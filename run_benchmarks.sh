#!/bin/bash

uv run bar-bench run --engine recoil-5c157c8-perf-wins --bar-content bar-test-29871-90f4bc1 --map starwatcher-v1.0 --scenario fightertest-bots --count 8 --machine-type c2d-standard-8
uv run bar-bench run --engine recoil-5c157c8-perf-wins --bar-content bar-test-29871-90f4bc1 --map starwatcher-v1.0 --scenario fightertest-aircraft --count 8 --machine-type c2d-standard-8
uv run bar-bench run --engine recoil-5c157c8-perf-wins --bar-content bar-test-29871-90f4bc1 --map starwatcher-v1.0 --scenario fightertest-tanks --count 8 --machine-type c2d-standard-8
uv run bar-bench run --engine recoil-5c157c8-perf-wins --bar-content bar-test-29871-90f4bc1 --map starwatcher-v1.0 --scenario fightertest-pathfinding --count 8 --machine-type c2d-standard-8
uv run bar-bench run --engine recoil-5c157c8-perf-wins --bar-content bar-test-29871-90f4bc1 --map hellas-basin-v1.4 --scenario lategame1 --count 16 --machine-type c2d-standard-8