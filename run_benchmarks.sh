#!/bin/bash

uv run bar-bench run --engine recoil-892ff9e-master --description "baseline run 1.0" --bar-content bar-test-29871-90f4bc1 --map starwatcher-v1.0 --scenario fightertest-bots --count 10 --machine-type c2d-standard-16
uv run bar-bench run --engine recoil-892ff9e-master --description "baseline run 1.0" --bar-content bar-test-29871-90f4bc1 --map starwatcher-v1.0 --scenario fightertest-aircraft --count 10 --machine-type c2d-standard-16
uv run bar-bench run --engine recoil-892ff9e-master --description "baseline run 1.0" --bar-content bar-test-29871-90f4bc1 --map starwatcher-v1.0 --scenario fightertest-tanks --count 10 --machine-type c2d-standard-16
uv run bar-bench run --engine recoil-892ff9e-master --description "baseline run 1.0" --bar-content bar-test-29871-90f4bc1 --map starwatcher-v1.0 --scenario fightertest-pathfinding --count 10 --machine-type c2d-standard-16
uv run bar-bench run --engine recoil-892ff9e-master --description "baseline run 1.0" --bar-content bar-test-29871-90f4bc1 --map hellas-basin-v1.4 --scenario lategame1 --count 16 --machine-type c2d-standard-16
