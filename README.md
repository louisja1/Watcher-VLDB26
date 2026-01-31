# Watcher-VLDB26
the github repository of Watcher for VLDB 2026 reviewer only

## Python Package Requirement
```
torch=2.1.0+cu121
tqdm==4.66.4
numpy==1.26.4
psycopg2==2.9.9 (dt dec pq3 ext lo64)
plyvel==1.5.1
```
and see JGMP's requirements in `src/jgmp/requirements.txt`

## Path setup
There are some paths/usernames/dbnames/ports to manually setup for connecting PostgreSQL and LevelDB in `pg_hint_utility.py`, `connection.py`, `db_wrapper.py`, and `collaborate_watchers_multid.py`, which are set as ANONYMOUS for now.

## Usage
See the details in `src/run.sh`
```
sh run.sh
```

## Example Usage to Run Q013a of Six-batch Setting
`USER` below denotes the db user for connection.
`PORT` below denotes the port for connection.

```
echo "1 ---------"
dropdb -p PORT watcherdb
createdb -p PORT -U USER watcherdb
python3 base_main1d.py ../input_configs/dsb_sf2_no-retrain_no-cache_config_Q013a-6B.json # for running Baseline
echo "2 ---------"
dropdb -p PORT watcherdb
createdb -p PORT -U USER watcherdb
python3 truecard_main1d.py ../input_configs/dsb_sf2_true-card_config_Q013a-6B.json # for running True
echo "3 ---------"
dropdb -p PORT watcherdb
createdb -p PORT -U USER watcherdb
python3 twotier_main1d.py ../input_configs/dsb_sf2_twotier_config_Q013a-6B_correctjoin2_trial1.json # for running SWatcher+
echo "4 ---------"
dropdb -p PORT watcherdb
createdb -p PORT -U USER watcherdb
python3 baseline1d.py ../input_configs/dsb_sf2_baseline_config_Q013a-6B_rt3.json # for running Periodic
echo "5 ---------"
dropdb -p PORT watcherdb
createdb -p PORT -U USER watcherdb
python3 main1d.py ../input_configs/dsb_sf2_config_Q013a-6B.json # for running Watcher1D
echo "6 ---------"
dropdb -p PORT watcherdb
createdb -p PORT -U USER watcherdb
python3 leo_main1d.py ../input_configs/dsb_sf2_leo_config_Q013a-6B.json # for running LEO
```

Please also see the details in the json files in `input_configs/`. We include all the experiments setup we run in that folder.

## Datasets and Workloads
`data/` is large and zipped.