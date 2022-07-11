import yaml

import kubernetes
import kubernetes.client

from kubernetes import utils, config, client

from typing import List, Union

from feast import RepoConfig, FeatureView
from feast.infra.materialization import MaterializationJob
from feast.batch_feature_view import BatchFeatureView
from feast.stream_feature_view import StreamFeatureView


class BytewaxMaterializationJob(MaterializationJob):
    def __init__(
        self,
        config: RepoConfig,
        feature_view: Union[BatchFeatureView, StreamFeatureView, FeatureView],
        paths: List[str],
    ):
        self.config = config
        self.paths = paths
        self.feature_view = feature_view

        self.create_kubernetes_job()

    def status(self):
        pass

    def should_be_retried(self):
        pass

    def job_id(self):
        pass

    def url(self):
        pass

    def create_kubernetes_job(self):
        # Kubernetes setup
        # TODO: Pipe configuration to this point
        config.load_kube_config()
        v1 = client.CoreV1Api()
        # TODO: Maybe this should be a method on the RepoConfig object?
        # Taken from https://github.com/feast-dev/feast/blob/master/sdk/python/feast/repo_config.py#L342
        feature_store_configuration = yaml.dump(
            yaml.safe_load(
                self.config.json(
                    exclude={"repo_path"},
                    exclude_unset=True,
                )
            )
        )

        materialization_config = yaml.dump(
            {"paths": self.paths, "feature_view": self.feature_view.name}
        )

        configmap_manifest = {
            "kind": "ConfigMap",
            "apiVersion": "v1",
            "metadata": {
                "name": "feast",
            },
            "data": {
                "feature_store.yaml": feature_store_configuration,
                "bytewax_materialization_config.yaml": materialization_config,
            },
        }
        # TODO: Create or update
        v1.patch_namespaced_config_map(
            name="feast",
            namespace="default",
            body=configmap_manifest,
        )

        # Create the k8s job to run the dataflow
        k8s_client = client.api_client.ApiClient()
        yaml_file = "bytewax_dataflow.yaml"
        utils.create_from_yaml(k8s_client, yaml_file, verbose=True)
