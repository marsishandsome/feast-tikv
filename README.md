# Feast TiKV Online Store

### Overview

TiKV is not included in current [Feast](https://github.com/feast-dev/feast) roadmap, this project intends to add TiKV support for Online Store.  
For more details, can check [this Feast issue](https://github.com/feast-dev/feast/issues/1782).

### Testing the custom online store in this repository

Run the following commands to test the custom online store ([TiKVOnlineStore](https://github.com/marsishandsome/feast-tikv/blob/master/feast_custom_online_store/tikv.py))

```bash
pip install -r requirements.txt
```

```
pytest test_custom_online_store.py
```

It is also possible to run a Feast CLI command, which interacts with the online store. It may be necessary to add the 
`PYTHONPATH` to the path where your online store module is stored.
```
PYTHONPATH=$PYTHONPATH:/$(pwd) feast -c basic_feature_repo apply

```
```
Registered entity driver_id
Registered feature view driver_hourly_stats
Deploying infrastructure for driver_hourly_stats
```

```
$ PYTHONPATH=$PYTHONPATH:/$(pwd) feast -c feature_repo materialize-incremental 2021-08-19T22:29:28
```
```Materializing 1 feature views to 2021-08-19 15:29:28-07:00 into the feast_custom_online_store.tikv.TiKVOnlineStore online store.

driver_hourly_stats from 2020-08-24 05:23:49-07:00 to 2021-08-19 15:29:28-07:00:
100%|████████████████████████████████████████████████████████████████| 5/5 [00:00<00:00, 120.59it/s]
```
