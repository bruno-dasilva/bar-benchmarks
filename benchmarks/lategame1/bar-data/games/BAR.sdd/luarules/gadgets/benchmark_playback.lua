function gadget:GetInfo()
	return {
		name    = "Benchmark Playback",
		desc    = "Restores a captured late-game snapshot and replays births/deaths/orders for cross-version sim benchmarking. Trigger with `/luarules bench_start`.",
		author  = "",
		date    = "2026",
		license = "GPL v2 or later",
		layer   = 0,
		enabled = true,
	}
end

local SNAPSHOT_FILE      = "benchmark_snapshot.lua"
local PLAYBACK_WINDOW    = 5000   -- number of frames to replay after trigger
local ORIG_CAPTURE_FRAME = 81000  -- must match CAPTURE_FRAME in benchmark_capture.lua

-- Chunking protocol (unsynced → synced via SendLuaRulesMsg, since synced can't
-- read raw filesystem files without LuaDevMode, which breaks BAR).
--
-- Per-chunk framing is length-prefixed and checksummed end-to-end because the
-- transport is emphatically not opaque: under load (20MB / ~611 packets) the
-- server's bandwidth throttle queues packets and, in prior runs, delivered
-- a 98-byte surplus that broke loadstring mid-table. Length + checksum in
-- every chunk lets us reject torn/duplicated deliveries deterministically.
--   "bsc:<idx>:<len>:<cksum>:<data>"   per-chunk, idx+len+cksum are decimal
--   "bsd:<count>:<totalLen>"           end marker with whole-snapshot length
-- LUAMSG packet ceiling is ~65KB (BaseNetProtocol.cpp:444); use 32KB payload.
local CHUNK_SIZE = 32 * 1024


if gadgetHandler:IsSyncedCode() then
	-- SYNCED half: receive snapshot chunks, spawn + measure on bench_start.

	local Game_maxUnits = Game.maxUnits
	local data              -- populated once all chunks arrive
	local unitMap    = {}   -- origID -> newID
	local featureMap = {}   -- origID -> newID
	local state      = "waiting_chunks"  -- waiting_chunks | ready | running | done
	local benchStartFrame = nil
	local chunks = {}
	local benchStartPending = false

	local function isFactory(uDefID)
		local ud = UnitDefs[uDefID]
		return ud and ud.isFactory
	end

	local function isCommander(uDefID)
		local ud = UnitDefs[uDefID]
		if not ud then return false end
		if ud.customParams and ud.customParams.iscommander then return true end
		local n = ud.name or ""
		return n:sub(1,6) == "armcom" or n:sub(1,6) == "corcom" or n:sub(1,6) == "legcom"
	end

	local commanderCount = {}
	local commanderUnits = {}

	local function totalCommanders()
		local t = 0
		for _, c in pairs(commanderCount) do t = t + c end
		return t
	end

	local function remapUnitParams(params)
		if not params then return params end
		local out = {}
		for i, v in ipairs(params) do out[i] = v end
		if #out >= 1 and #out < 3 then
			local target = out[1]
			if type(target) == "number" then
				if target >= Game_maxUnits then
					local mapped = featureMap[target - Game_maxUnits]
					if mapped then out[1] = mapped + Game_maxUnits
					else return nil end
				else
					local mapped = unitMap[target]
					if mapped then out[1] = mapped
					else return nil end
				end
			end
		end
		return out
	end

	local function spawnFeature(f)
		local newID = Spring.CreateFeature(f.defID, f.pos[1], f.pos[2], f.pos[3], f.heading or 0, f.team)
		if not newID then return end
		featureMap[f.origID] = newID
		if f.health and f.maxHealth and f.maxHealth > 0 then
			Spring.SetFeatureHealth(newID, f.health)
			Spring.SetFeatureMaxHealth(newID, f.maxHealth)
		end
		if f.resources then
			Spring.SetFeatureResources(newID, f.resources.metal or 0, f.resources.energy or 0)
		end
		if f.rotation then
			Spring.SetFeatureRotation(newID, f.rotation[1], f.rotation[2], f.rotation[3])
		end
		if f.rulesParams then
			for k, v in pairs(f.rulesParams) do Spring.SetFeatureRulesParam(newID, k, v) end
		end
	end

	local function spawnUnit(u)
		local newID = Spring.CreateUnit(u.defID, u.pos[1], u.pos[2], u.pos[3], u.heading or 0, u.team, u.beingBuilt or false)
		if not newID then
			if isCommander(u.defID) then
				Spring.Echo(string.format("[BenchmarkPlayback] SPAWN FAILED for commander: team=%d defID=%d origID=%d pos=(%.0f,%.0f,%.0f)",
					u.team, u.defID, u.origID, u.pos[1], u.pos[2], u.pos[3]))
			end
			return
		end
		unitMap[u.origID] = newID
		if isCommander(u.defID) then
			commanderCount[u.team] = (commanderCount[u.team] or 0) + 1
			commanderUnits[newID] = true
		end
		if u.health and u.maxHealth then
			Spring.SetUnitHealth(newID, { health = u.health, build = u.buildProgress or 1, paralyze = u.paralyzeDmg or 0 })
			Spring.SetUnitMaxHealth(newID, u.maxHealth)
		end
		if u.experience and u.experience > 0 then Spring.SetUnitExperience(newID, u.experience) end
		if u.velocity then Spring.SetUnitVelocity(newID, u.velocity[1], u.velocity[2], u.velocity[3]) end
		if u.cloak then Spring.SetUnitCloak(newID, true) end
		if u.stockpile and u.stockpile.count and u.stockpile.count > 0 then
			Spring.SetUnitStockpile(newID, u.stockpile.count, u.stockpile.buildPct or 0)
		end
		if u.shields then
			for i, s in pairs(u.shields) do
				Spring.SetUnitShieldState(newID, i, s.enabled, s.power)
			end
		end
		if u.rulesParams then
			for k, v in pairs(u.rulesParams) do Spring.SetUnitRulesParam(newID, k, v) end
		end
		if u.commands and not isFactory(u.defID) then
			for _, cmd in ipairs(u.commands) do
				local mapped = remapUnitParams(cmd.params)
				if mapped then
					Spring.GiveOrderToUnit(newID, cmd.id, mapped, cmd.options or 0)
				end
			end
		end
	end

	local function applyEventsFor(origFrame)
		local e = data.events[origFrame]
		if not e then return end
		if e.births then
			for _, b in ipairs(e.births) do
				local newID = Spring.CreateUnit(b.defID, b.pos[1], b.pos[2], b.pos[3], b.heading or 0, b.team, false)
				if newID then unitMap[b.origID] = newID end
			end
		end
		if e.deaths then
			for _, d in ipairs(e.deaths) do
				local newID = unitMap[d.origID]
				if newID and Spring.ValidUnitID(newID) then
					if commanderUnits[newID] then
						local relFrame = Spring.GetGameFrame() - benchStartFrame
						Spring.Echo(string.format(
							"[BenchmarkPlayback] rel=%d origFrame=%d scripted death of COMMANDER unitID=%d origID=%d",
							relFrame, origFrame, newID, d.origID))
					end
					Spring.DestroyUnit(newID, false, false)
				end
			end
		end
		if e.orders then
			for _, o in ipairs(e.orders) do
				local newID = unitMap[o.origID]
				if newID and Spring.ValidUnitID(newID) and not isFactory(Spring.GetUnitDefID(newID)) then
					local mapped = remapUnitParams(o.params)
					if mapped then
						Spring.GiveOrderToUnit(newID, o.cmdID, mapped, o.options or 0)
					end
				end
			end
		end
	end

	local function validateEnv()
		local env = data.snapshot.env
		if not env then return true end
		if env.mapName and env.mapName ~= Game.mapName then
			Spring.Echo(string.format("[BenchmarkPlayback] WARNING: snapshot map=%q but current map=%q",
				env.mapName, Game.mapName))
		end
		if env.gameName and env.gameName ~= Game.gameName then
			Spring.Echo(string.format("[BenchmarkPlayback] WARNING: snapshot mod=%q but current mod=%q",
				env.gameName, Game.gameName))
		end
		local haveTeams = {}
		for _, tID in ipairs(Spring.GetTeamList()) do haveTeams[tID] = true end
		local missing = {}
		for _, t in ipairs(env.teams or {}) do
			if not haveTeams[t.teamID] then missing[#missing + 1] = t.teamID end
		end
		if #missing > 0 then
			Spring.Echo("[BenchmarkPlayback] ERROR: playback script is missing teams: " ..
				table.concat(missing, ","))
			return false
		end
		return true
	end

	local function startBenchmark()
		if state == "waiting_chunks" then
			Spring.Echo("[BenchmarkPlayback] bench_start received before snapshot finished loading; deferring.")
			benchStartPending = true
			return
		end
		if state ~= "ready" then
			Spring.Echo("[BenchmarkPlayback] bench_start ignored; state=" .. state)
			return
		end
		state = "running"
		benchStartFrame = Spring.GetGameFrame()

		-- IMPORTANT ordering: spawn snapshot FIRST, destroy pre-existing AFTER.
		-- If we destroy BAR's initial commanders first, the game-end gadget marks
		-- each allyteam dead the moment its last commander dies, and then
		-- CreateUnit rejects every subsequent spawn on those teams
		-- (LuaSyncedCtrl.cpp:1871, unitHandler.CanBuildUnit returns false on a
		-- dead team). By spawning the snapshot commanders first, each team
		-- always has at least 1 commander alive while we're clearing.
		local preUnits    = Spring.GetAllUnits()
		local preFeatures = Spring.GetAllFeatures()
		Spring.Echo(string.format("[BenchmarkPlayback] Starting at frame %d: spawning %d units, %d features (pre-existing: %d units, %d features will be cleared after)",
			benchStartFrame, #data.snapshot.units, #data.snapshot.features,
			#preUnits, #preFeatures))
		for _, feat in ipairs(data.snapshot.features) do spawnFeature(feat) end
		for _, u    in ipairs(data.snapshot.units)    do spawnUnit(u)      end

		-- Now clear the pre-existing BAR initial-spawn units/features. Snapshot
		-- commanders are already alive so allyteams keep at least one commander.
		for _, uID in ipairs(preUnits)    do Spring.DestroyUnit(uID, false, true) end
		for _, fID in ipairs(preFeatures) do Spring.DestroyFeature(fID)           end
		Spring.Echo(string.format("[BenchmarkPlayback] Cleared %d pre-existing units, %d pre-existing features",
			#preUnits, #preFeatures))

		-- Spring.GetTimer is unsynced-only (LuaUnsyncedRead.cpp:218), so delegate
		-- wall-clock timing to the unsynced half.
		SendToUnsynced("benchmarkStart")
		local lines = {}
		for team, count in pairs(commanderCount) do
			lines[#lines + 1] = "team=" .. team .. ":" .. count
		end
		Spring.Echo(string.format("[BenchmarkPlayback] Spawn done. Commanders=%d (%s). Replaying %d frames.",
			totalCommanders(), table.concat(lines, " "), PLAYBACK_WINDOW))
	end

	local function cheapChecksum(s)
		-- sum-of-bytes mod 2^32; cheap, not cryptographic, but catches any
		-- transport-corruption byte flip or truncation.
		local sum = 0
		for i = 1, #s do sum = (sum + s:byte(i)) % 4294967296 end
		return sum
	end

	local chunkIssues = 0
	local function reportChunkIssue(fmt, ...)
		-- cap the noise: the first few bad chunks tell you the whole story.
		chunkIssues = chunkIssues + 1
		if chunkIssues <= 8 then
			Spring.Echo("[BenchmarkPlayback] " .. string.format(fmt, ...))
		elseif chunkIssues == 9 then
			Spring.Echo("[BenchmarkPlayback] (further chunk-issue logs suppressed)")
		end
	end

	local function finalizeSnapshot(expectedCount, expectedTotalLen)
		Spring.Echo(string.format("[BenchmarkPlayback] finalizeSnapshot: assembling %d chunks (issues so far=%d)",
			expectedCount, chunkIssues))

		-- Before table.concat: audit the chunks table so we can attribute any
		-- length surplus to specific idx values.
		local sumInRange, inRange = 0, 0
		local oddSized = {}
		for i = 0, expectedCount - 1 do
			local c = chunks[i]
			if c then
				inRange = inRange + 1
				sumInRange = sumInRange + #c
				-- Every chunk except the last should be exactly CHUNK_SIZE bytes.
				if (i < expectedCount - 1 and #c ~= CHUNK_SIZE) or (i == expectedCount - 1 and #c > CHUNK_SIZE) then
					oddSized[#oddSized + 1] = string.format("idx=%d len=%d", i, #c)
				end
			end
		end
		local extraIdx, extraBytes = 0, 0
		for k, v in pairs(chunks) do
			if type(k) == "number" and (k < 0 or k >= expectedCount) then
				extraIdx = extraIdx + 1
				extraBytes = extraBytes + #v
				if extraIdx <= 5 then
					Spring.Echo(string.format("[BenchmarkPlayback] WARN: chunk with out-of-range idx=%s len=%d",
						tostring(k), #v))
				end
			end
		end
		Spring.Echo(string.format(
			"[BenchmarkPlayback] chunk audit: inRange=%d sumInRange=%d outOfRange=%d outOfRangeBytes=%d",
			inRange, sumInRange, extraIdx, extraBytes))
		if #oddSized > 0 then
			local sample = table.concat(oddSized, ", ", 1, math.min(10, #oddSized))
			Spring.Echo(string.format("[BenchmarkPlayback] odd-sized chunks (first 10): %s%s",
				sample, (#oddSized > 10) and (" (+" .. (#oddSized - 10) .. " more)") or ""))
		end

		-- Cross-check the chunk sum against sender's declared total. If the
		-- chunks already don't add up we refuse to parse, since stitching them
		-- together would silently hide the loss.
		if expectedTotalLen and sumInRange ~= expectedTotalLen then
			Spring.Echo(string.format("[BenchmarkPlayback] ERROR: chunk-sum mismatch: %d vs expected %d (%+d)",
				sumInRange, expectedTotalLen, sumInRange - expectedTotalLen))
			return
		end

		-- Why not `table.concat(pieces)`: a flat 638-entry concat totaling 20MB
		-- produced a string 124 bytes longer than the sum of the piece lengths,
		-- with the odd-sized shift pattern starting at chunk 512 — which is
		-- byte 16MB = 2^24 exactly. That's the luaL_Buffer boundary: Lua 5.1's
		-- auxiliary buffer-on-stack grows dynamically but has historically
		-- mishandled pieces that cross the 16MB mark when used with table.concat.
		-- So we reduce pair-wise using the `..` operator (OP_CONCAT in the VM,
		-- plain alloc+memcpy — doesn't go through luaL_Buffer), halving the piece
		-- count every pass. log2(638) ≈ 10 rounds, ~6380 total concats.
		local pieces = {}
		local sumPieces = 0
		for i = 0, expectedCount - 1 do
			pieces[i + 1] = chunks[i]
			sumPieces = sumPieces + #chunks[i]
		end
		chunks = nil
		Spring.Echo(string.format(
			"[BenchmarkPlayback] pair-wise concat: %d pieces summing to %d bytes",
			#pieces, sumPieces))
		while #pieces > 1 do
			local next = {}
			for i = 1, #pieces, 2 do
				if pieces[i + 1] then
					next[#next + 1] = pieces[i] .. pieces[i + 1]
				else
					next[#next + 1] = pieces[i]
				end
			end
			pieces = next
		end
		local content = pieces[1]
		if #content ~= sumPieces then
			Spring.Echo(string.format(
				"[BenchmarkPlayback] ERROR: pair-concat length mismatch: got %d, expected %d (%+d)",
				#content, sumPieces, #content - sumPieces))
			return
		end
		Spring.Echo(string.format(
			"[BenchmarkPlayback] All %d chunks assembled (%d bytes; issues=%d). Parsing...",
			expectedCount, #content, chunkIssues))
		local loader, err = loadstring(content, "benchmark_snapshot")
		if not loader then
			Spring.Echo("[BenchmarkPlayback] ERROR: loadstring failed: " .. tostring(err))
			return
		end
		data = loader()
		Spring.Echo(string.format(
			"[BenchmarkPlayback] Snapshot ready: %d units, %d features.",
			#data.snapshot.units, #data.snapshot.features))
		if not validateEnv() then return end
		state = "ready"
		if benchStartPending then
			benchStartPending = false
			startBenchmark()
		end
	end

	function gadget:Initialize()
		Spring.Echo("[BenchmarkPlayback] (synced) Initialize; awaiting snapshot chunks.")
	end

	local chunksLogged = 0
	local function noteChunkArrival(idx)
		chunksLogged = chunksLogged + 1
		if chunksLogged == 1 or chunksLogged % 100 == 0 then
			Spring.Echo(string.format("[BenchmarkPlayback] (synced) recv chunk %d (total accepted=%d)",
				idx, chunksLogged))
		end
	end

	local function splitColon(s, n)
		-- Split into up to n parts separated by ':'. Last part keeps any
		-- remaining colons verbatim (important: chunk data may contain ':').
		local out = {}
		local start = 1
		for _ = 1, n - 1 do
			local c = s:find(":", start, true)
			if not c then return nil end
			out[#out + 1] = s:sub(start, c - 1)
			start = c + 1
		end
		out[#out + 1] = s:sub(start)
		return out
	end

	function gadget:RecvLuaMsg(msg, playerID)
		-- SendLuaRulesMsg from our unsynced half lands here (snapshot chunks).
		if msg:sub(1,4) == "bsc:" then
			-- Format: bsc:<idx>:<len>:<cksum>:<data>
			local parts = splitColon(msg:sub(5), 4)
			if not parts then
				reportChunkIssue("ERROR: malformed bsc header: %q", msg:sub(1, 64))
				return
			end
			local idx    = tonumber(parts[1])
			local expLen = tonumber(parts[2])
			local expCk  = tonumber(parts[3])
			local data   = parts[4]
			if not idx or not expLen or not expCk or not data then
				reportChunkIssue("ERROR: malformed bsc numerics: idx=%s len=%s cksum=%s",
					tostring(parts[1]), tostring(parts[2]), tostring(parts[3]))
				return
			end
			if #data ~= expLen then
				reportChunkIssue("ERROR: chunk %d length %d != expected %d (msg total=%d, dropping)",
					idx, #data, expLen, #msg)
				return
			end
			local actualCk = cheapChecksum(data)
			if actualCk ~= expCk then
				reportChunkIssue("ERROR: chunk %d checksum %d != expected %d (dropping)",
					idx, actualCk, expCk)
				return
			end
			if chunks[idx] then
				if chunks[idx] == data then
					reportChunkIssue("WARN: duplicate chunk %d (identical payload)", idx)
				else
					reportChunkIssue("ERROR: duplicate chunk %d with DIFFERENT payload (len %d vs %d)",
						idx, #chunks[idx], #data)
				end
				-- Already verified payload is valid; keep first-write semantics so
				-- we don't get whipsawed by a later corrupted duplicate.
				return
			end
			chunks[idx] = data
			noteChunkArrival(idx)
			return
		end
		if msg:sub(1,4) == "bsd:" then
			Spring.Echo("[BenchmarkPlayback] (synced) bsd end marker received")
			-- Format: bsd:<count>:<totalLen>
			local parts = splitColon(msg:sub(5), 2)
			if not parts then
				Spring.Echo("[BenchmarkPlayback] ERROR: malformed bsd: " .. msg)
				return
			end
			local count = tonumber(parts[1])
			local tlen  = tonumber(parts[2])
			if count then finalizeSnapshot(count, tlen) end
			return
		end
	end

	-- `/luarules bench_start` routes through GotChatMsg, not RecvLuaMsg
	-- (see rts/Game/SyncedGameCommands.cpp:397 LuaRulesActionExecutor).
	function gadget:GotChatMsg(msg, playerID)
		if msg == "bench_start" then startBenchmark() end
	end

	function gadget:GameFrame(f)
		if state ~= "running" then return end
		local rel = f - benchStartFrame
		if rel < 1 then return end
		applyEventsFor(ORIG_CAPTURE_FRAME + rel)
		if rel >= PLAYBACK_WINDOW then
			state = "done"
			Spring.Echo(string.format(
				"[BenchmarkPlayback] Replay window done: %d sim frames.", PLAYBACK_WINDOW))
			SendToUnsynced("benchmarkPlaybackDone", PLAYBACK_WINDOW)
		end
	end

	function gadget:UnitDestroyed(uID, uDefID, uTeam, attackerID, attackerDefID, weaponDefID)
		if commanderUnits[uID] then
			commanderUnits[uID] = nil
			commanderCount[uTeam] = (commanderCount[uTeam] or 1) - 1
			local f = Spring.GetGameFrame()
			local rel = benchStartFrame and (f - benchStartFrame) or -1
			Spring.Echo(string.format(
				"[BenchmarkPlayback] frame=%d rel=%d commander DIED: team=%d defID=%d unitID=%d attackerID=%s attackerDefID=%s weaponDefID=%s (remaining team=%d total=%d)",
				f, rel, uTeam, uDefID, uID,
				tostring(attackerID), tostring(attackerDefID), tostring(weaponDefID),
				commanderCount[uTeam] or 0, totalCommanders()))
		end
	end

	function gadget:GameOver(winners)
		local f = Spring.GetGameFrame()
		local rel = benchStartFrame and (f - benchStartFrame) or -1
		local winStr = (winners and #winners > 0) and table.concat(winners, ",") or "(none)"
		Spring.Echo(string.format(
			"[BenchmarkPlayback] GameOver frame=%d rel=%d state=%s winners=[%s] commandersRemaining=%d",
			f, rel, state, winStr, totalCommanders()))
	end

else
	-- UNSYNCED half: read the snapshot file and stream it to synced via chunks.

	local function cheapChecksum(s)
		local sum = 0
		for i = 1, #s do sum = (sum + s:byte(i)) % 4294967296 end
		return sum
	end

	-- Per-frame timing (BAR dbg_benchmark state machine, slightly simplified).
	-- A tick walks: [>=0 sim frames] -> Update -> DrawGenesis -> DrawScreenPost.
	-- Sim boundary: GameFrame (start) to next Update OR next GameFrame (end).
	-- Update phase: end-of-Update to DrawGenesis. Draw phase: DrawGenesis to
	-- DrawScreenPost. Headless never fires Draw*, so only the Sim stream
	-- gets populated there — empty streams are omitted from the final report.
	local timingActive = false
	local simFrameTimes, drawFrameTimes, updateFrameTimes = {}, {}, {}
	local lastSimTimerUS, lastDrawTimerUS, lastUpdateTimerUS
	local lastFrameType = 'draw' -- 'draw' | 'sim' | 'update'

	-- returnMs=true, fromMicroSecs=true: our timers come from GetTimerMicros,
	-- and we want ms-scale numbers in the results.
	local function diffMs(now, prev)
		return Spring.DiffTimers(now, prev, true, true)
	end

	function gadget:Update()
		if not timingActive then return end
		local now = Spring.GetTimerMicros()
		if lastFrameType == 'sim' then
			simFrameTimes[#simFrameTimes + 1] = diffMs(now, lastSimTimerUS)
			-- Flip so a subsequent GameFrame before Draw* won't re-measure this
			-- same interval (see BAR dbg_benchmark comment on multi-sim ticks).
			lastFrameType = 'update'
		end
		lastUpdateTimerUS = now
	end

	function gadget:GameFrame(n)
		if not timingActive then return end
		local now = Spring.GetTimerMicros()
		if lastFrameType == 'sim' then
			-- back-to-back sim frames (headless / fast-forward): close the previous
			-- sim segment here instead of in Update.
			simFrameTimes[#simFrameTimes + 1] = diffMs(now, lastSimTimerUS)
		end
		lastSimTimerUS = now
		lastFrameType = 'sim'
	end

	function gadget:DrawGenesis()
		if not timingActive then return end
		local now = Spring.GetTimerMicros()
		if lastUpdateTimerUS then
			updateFrameTimes[#updateFrameTimes + 1] = diffMs(now, lastUpdateTimerUS)
		end
		lastDrawTimerUS = now
	end

	function gadget:DrawScreenPost()
		if not timingActive or not lastDrawTimerUS then return end
		drawFrameTimes[#drawFrameTimes + 1] = diffMs(Spring.GetTimerMicros(), lastDrawTimerUS)
		lastFrameType = 'draw'
	end

	local PERCENTILES = { 0, 1, 2, 5, 10, 20, 35, 50, 65, 80, 90, 95, 98, 99, 100 }

	local function computeStats(samples)
		if #samples == 0 then return nil end
		-- Discard first 10% as warmup (matches BAR dbg_benchmark).
		local sorted, count, total = {}, 0, 0
		local warmup = math.floor(#samples * 0.1)
		for i, v in ipairs(samples) do
			if i > warmup then
				count = count + 1
				sorted[count] = v
				total = total + v
			end
		end
		if count == 0 then return nil end
		local mean = total / count
		table.sort(sorted)
		local spread = 0
		for _, v in ipairs(sorted) do spread = spread + math.abs(v - mean) end
		spread = spread / count
		local pct = {}
		for _, p in ipairs(PERCENTILES) do
			pct[p] = sorted[math.min(count, 1 + math.floor(p * 0.01 * count))]
		end
		return { count = count, total = total, mean = mean, spread = spread, pct = pct }
	end

	local function appendTimingStats(lines)
		local streams = {
			{ name = "Sim",    samples = simFrameTimes    },
			{ name = "Update", samples = updateFrameTimes },
			{ name = "Draw",   samples = drawFrameTimes   },
		}
		for _, s in ipairs(streams) do
			local ms = computeStats(s.samples)
			if ms then
				lines[#lines + 1] = ""
				lines[#lines + 1] = string.format(
					"%s: %d frames, mean=%.3fms, spread=%.3fms, total=%.3fs",
					s.name, ms.count, ms.mean, ms.spread, ms.total / 1000)
				for _, p in ipairs(PERCENTILES) do
					lines[#lines + 1] = string.format("  p%d = %.3fms", p, ms.pct[p] or 0)
				end
				Spring.Echo(string.format(
					"[BenchmarkPlayback] %s: n=%d mean=%.3fms p50=%.3fms p95=%.3fms p99=%.3fms",
					s.name, ms.count, ms.mean, ms.pct[50] or 0, ms.pct[95] or 0, ms.pct[99] or 0))
			end
		end
	end

	function gadget:Initialize()
		Spring.Echo("[BenchmarkPlayback] (unsynced) Initialize: opening snapshot")
		-- Binary mode: avoids Windows "r" mode's CRLF→LF stripping, which would
		-- make the in-memory content size differ from the on-disk file size and
		-- complicate any byte-for-byte diagnostics. Lua's loadstring happily
		-- parses either line ending, so we just ship the file verbatim.
		local fh = io.open(SNAPSHOT_FILE, "rb")
		if not fh then
			Spring.Echo("[BenchmarkPlayback] (unsynced) ERROR: cannot open " .. SNAPSHOT_FILE)
			return
		end

		-- Stream the file one CHUNK_SIZE block at a time rather than slurping it
		-- into memory and slicing with content:sub. Spring's Lua uses
		-- single-precision floats, so Lua-side indices lose 1 bit of precision
		-- per power-of-two past 2^24 (16MB). A 40MB snapshot crosses 2^24 *and*
		-- 2^25, so any `i * CHUNK_SIZE + 1` style indexing has to juggle multiple
		-- 16MB boundaries. fh:read(CHUNK_SIZE) takes a small constant (< 2^24)
		-- and lets the C side handle the file offset in size_t, so no Lua-side
		-- integer ever grows past CHUNK_SIZE. Totals (numChunks, totalLen) do
		-- grow but only via small-increment adds where each addend is a
		-- multiple of 8 (CHUNK_SIZE = 32K), which stays exact past 2^26.
		local PROGRESS_EVERY = 100
		local numChunks = 0
		local totalLen  = 0
		while true do
			local data = fh:read(CHUNK_SIZE)
			if not data or #data == 0 then break end
			Spring.SendLuaRulesMsg(string.format("bsc:%d:%d:%d:%s",
				numChunks, #data, cheapChecksum(data), data))
			numChunks = numChunks + 1
			totalLen  = totalLen + #data
			if numChunks % PROGRESS_EVERY == 0 then
				Spring.Echo(string.format("[BenchmarkPlayback] (unsynced) sent %d chunks (%d bytes)",
					numChunks, totalLen))
			end
		end
		fh:close()
		Spring.SendLuaRulesMsg(string.format("bsd:%d:%d", numChunks, totalLen))
		Spring.Echo(string.format(
			"[BenchmarkPlayback] (unsynced) all chunks queued + bsd sent (%d chunks, %d bytes)",
			numChunks, totalLen))

		-- Wall-clock timing lives here because Spring.GetTimer/DiffTimers are
		-- unsynced-only. Synced signals start and end; we measure the delta.
		local startTimer
		gadgetHandler:AddSyncAction("benchmarkStart", function()
			startTimer = Spring.GetTimer()
			simFrameTimes, drawFrameTimes, updateFrameTimes = {}, {}, {}
			lastSimTimerUS, lastDrawTimerUS, lastUpdateTimerUS = nil, nil, nil
			lastFrameType = 'draw'
			timingActive = true
			Spring.Echo("[BenchmarkPlayback] (unsynced) wall timer + sim/update/draw timing started.")
		end)
		gadgetHandler:AddSyncAction("benchmarkPlaybackDone", function(_, simFrames)
			timingActive = false
			if not startTimer then
				Spring.Echo("[BenchmarkPlayback] (unsynced) ERROR: Done signal but no start timer recorded.")
				Spring.SendCommands("quitforce")
				return
			end
			local wall = Spring.DiffTimers(Spring.GetTimer(), startTimer)
			Spring.Echo(string.format(
				"[BenchmarkPlayback] DONE. %d sim frames in %.3fs wall = %.1f frames/s.",
				simFrames, wall, simFrames / wall))
			local lines = {
				string.format("Benchmark results %s", os.date("%Y-%m-%d %H:%M:%S")),
				string.format("Map: %s", tostring(Game.mapName)),
				string.format("Game: %s %s", tostring(Game.gameName), tostring(Game.gameVersion)),
				string.format("Engine: %s", tostring(Engine.versionFull)),
				string.format("frames=%d wall_s=%.6f fps=%.3f", simFrames, wall, simFrames / wall),
			}
			appendTimingStats(lines)
			local path = string.format("benchmark-%s.txt", os.date("%Y%m%d_%H%M%S"))
			local out = io.open(path, "w")
			if out then
				out:write(table.concat(lines, "\n"))
				out:write("\n")
				out:close()
				Spring.Echo("[BenchmarkPlayback] wrote results to " .. path)
			end
			Spring.SendCommands("quitforce")
		end)
	end
end
