from datetime import datetime
from typing import Sequence, Union, List, Optional, Tuple, Dict, Callable, Any

import pytz
from feast import Entity, FeatureTable, utils
from google.protobuf.timestamp_pb2 import Timestamp
from feast import RepoConfig, FeatureTable, FeatureView, Entity
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto


from feast.repo_config import FeastConfigBaseModel
from pydantic import StrictStr
from pydantic.typing import Literal

# TODO: release https://github.com/tikv/client-py
from tikv_client import RawClient


class TiKVOnlineStoreConfig(FeastConfigBaseModel):
    """
    Configuration for the TiKV online store.
    NOTE: The class *must* end with the `OnlineStoreConfig` suffix.
    """
    type: Literal["tikv",
                  "feast_custom_online_store.tikv.TiKVOnlineStore"] \
        = "feast_custom_online_store.tikv.TiKVOnlineStore"

    mode: Optional[StrictStr] = None
    pd_addresses: Optional[StrictStr] = None

class TiKVOnlineStore(OnlineStore):
    """
    An online store implementation that uses TiKV.
    NOTE: The class *must* end with the `OnlineStore` suffix.
    """

    _rawkv_client: Optional[RawClient] = None

    @staticmethod
    def _is_rawkv(online_store_config: TiKVOnlineStoreConfig) -> bool:
        return online_store_config.mode.lower() == "rawkv"

    @staticmethod
    def _encode_tikv_key(
        project: str, entity_key: EntityKeyProto, feature_view: str, feature_name: str
    ):
        entity_key_bin = serialize_entity_key(entity_key)
        tikv_key = (
            f"{project}:".encode("utf8")
            + entity_key_bin
            + f":{feature_view}:{feature_name}".encode("utf8")
        )
        return tikv_key

    def _get_rawkv_client(self, online_store_config: TiKVOnlineStoreConfig):
        if not self._rawkv_client:
            _rawkv_client = RawClient.connect(online_store_config.pd_addresses)
        return _rawkv_client

    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[Union[FeatureTable, FeatureView]],
        tables_to_keep: Sequence[Union[FeatureTable, FeatureView]],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        """
        There's currently no setup done for TiKV.
        """
        pass

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[Union[FeatureTable, FeatureView]],
        entities: Sequence[Entity],
    ):
        """
        There's currently no teardown done for TiKV.
        """
        pass

    def online_write_batch(
        self,
        config: RepoConfig,
        table: Union[FeatureTable, FeatureView],
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        online_store_config = config.online_store
        assert isinstance(online_store_config, TiKVOnlineStoreConfig)
        assert self._is_rawkv(online_store_config)

        client = self._get_rawkv_client(online_store_config)
        project = config.project
        feature_view = table.name

        for entity_key, values, timestamp, created_ts in data:
            ts = Timestamp()
            ts.seconds = int(utils.make_tzaware(timestamp).timestamp())

            if created_ts is not None:
                ex = Timestamp()
                ex.seconds = int(utils.make_tzaware(created_ts).timestamp())
            else:
                ex = ts

            ts_key = self._encode_tikv_key(project, entity_key, feature_view, "_ts")
            ts_value = ts.SerializeToString()
            client.put(ts_key, ts_value)

            ex_key = self._encode_tikv_key(project, entity_key, feature_view, "_ex")
            ex_value = ex.SerializeToString()
            client.put(ex_key, ex_value)

            for feature_name, val in values.items():
                tikv_key = self._encode_tikv_key(
                    project, entity_key, feature_view, feature_name
                )
                tikv_value = val.SerializeToString()
                client.put(tikv_key, tikv_value)
            if progress:
                progress(1)

    def online_read(
        self,
        config: RepoConfig,
        table: Union[FeatureTable, FeatureView],
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        online_store_config = config.online_store
        assert isinstance(online_store_config, TiKVOnlineStoreConfig)
        assert self._is_rawkv(online_store_config)

        client = self._get_rawkv_client(online_store_config)
        feature_view = table.name
        project = config.project

        result: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []

        if not requested_features:
            requested_features = [f.name for f in table.features]

        for entity_key in entity_keys:
            res = {}
            for feature_name in requested_features:
                tikv_key = self._encode_tikv_key(
                    project, entity_key, feature_view, feature_name
                )
                val_bin = client.get(tikv_key)
                val = ValueProto()
                if val_bin:
                    val.ParseFromString(val_bin)
                res[feature_name] = val

            ts_key = self._encode_tikv_key(project, entity_key, feature_view, "_ts")
            ts_value = client.get(ts_key)
            res_ts = Timestamp()
            if ts_value:
                res_ts.ParseFromString(ts_value)

            if not res:
                result.append((None, None))
            else:
                timestamp = datetime.fromtimestamp(res_ts.seconds)
                result.append((timestamp, res))
                print(result)
        return result

def _table_id(project: str, table: Union[FeatureTable, FeatureView]) -> str:
    return f"{project}_{table.name}"


def _to_naive_utc(ts: datetime):
    if ts.tzinfo is None:
        return ts
    else:
        return ts.astimezone(pytz.utc).replace(tzinfo=None)
