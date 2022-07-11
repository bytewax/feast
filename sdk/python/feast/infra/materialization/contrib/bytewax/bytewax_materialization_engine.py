from datetime import datetime, timedelta
from typing import List, Sequence, Union, Callable

from feast import RepoConfig, FeatureView
from feast.batch_feature_view import BatchFeatureView
from feast.stream_feature_view import StreamFeatureView
from feast.entity import Entity
from feast.registry import BaseRegistry
from feast.infra.offline_stores.offline_store import OfflineStore
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.materialization import BatchMaterializationEngine

from feast.utils import _get_column_names

from tqdm import tqdm

from .bytewax_materializataion_task import BytewaxMaterializationTask
from .bytewax_materialization_job import BytewaxMaterializationJob


class BytewaxMaterializationEngine(BatchMaterializationEngine):
    def __init__(
        self,
        repo_config: RepoConfig,
        offline_store: OfflineStore,
        online_store: OnlineStore,
    ):
        self.repo_config = repo_config
        self.offline_store = offline_store
        self.online_store = online_store

    def update(
        self,
        project: str,
        views_to_delete: Sequence[
            Union[BatchFeatureView, StreamFeatureView, FeatureView]
        ],
        views_to_keep: Sequence[
            Union[BatchFeatureView, StreamFeatureView, FeatureView]
        ],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
    ):
        """This method ensures that any necessary infrastructure or resources needed by the
        engine are set up ahead of materialization."""
        pass

    def teardown_infra(
        self,
        project: str,
        fvs: Sequence[Union[BatchFeatureView, StreamFeatureView, FeatureView]],
        entities: Sequence[Entity],
    ):
        """This method ensures that any infrastructure or resources set up by ``update()``are torn down."""
        pass

    def materialize(
        self,
        registry: BaseRegistry,
        tasks: List[BytewaxMaterializationTask],
    ) -> List[BytewaxMaterializationJob]:
        return [
            self._materialize_one(
                registry,
                task.feature_view,
                task.start_time,
                task.end_time,
                task.project,
                task.tqdm_builder,
            )
            for task in tasks
        ]

    def _materialize_one(
        self,
        registry: BaseRegistry,
        feature_view: Union[BatchFeatureView, StreamFeatureView, FeatureView],
        start_date: datetime,
        end_date: datetime,
        project: str,
        tqdm_builder: Callable[[int], tqdm],
    ):
        entities = []
        for entity_name in feature_view.entities:
            entities.append(registry.get_entity(entity_name, project))

        (
            join_key_columns,
            feature_name_columns,
            timestamp_field,
            created_timestamp_column,
        ) = _get_column_names(feature_view, entities)

        job = self.offline_store.pull_latest_from_table_or_query(
            config=self.repo_config,
            data_source=feature_view.batch_source,
            join_key_columns=join_key_columns,
            feature_name_columns=feature_name_columns,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            start_date=start_date,
            end_date=end_date,
        )

        paths = job.to_remote_storage()
        return BytewaxMaterializationJob(self.repo_config, paths)
