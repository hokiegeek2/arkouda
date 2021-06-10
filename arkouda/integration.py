import os
from typing import List
from kubernetes import client # type: ignore
from kubernetes.client.rest import ApiException # type: ignore
from kubernetes.client import V1PodList, V1Pod # type: ignore


class DaoError(Exception):
    pass

class KubernetesDao():    
    
    __slots__ = ('core_client', 'apps_client')
    
    core_client: client.CoreV1Api
    apps_client: client.AppsV1Api
    
    def __init__(self):
        config = client.Configuration()

        try:
            config.cert_file =  os.environ['CERT_FILE']
            config.key_file =  os.environ['KEY_FILE']
            config.host = os.environ['K8S_HOST']
        except KeyError:
            raise EnvironmentError(('The CERT_FILE, KEY_FILE, and K8S_HOST ' +
                                   'env variables must be set'))
        config.verify_ssl = False

        api_client = client.ApiClient(configuration=config)
        self.core_client = client.CoreV1Api(api_client=api_client)
        self.apps_client = client.AppsV1Api(api_client)
    
    def get_pods(self, namespace : str, app_name : str=None) -> V1PodList:
        try:
            if namespace:
                pods = self.core_client.list_namespaced_pod(namespace=namespace)
                if app_name:
                    filteredPods = []
                    for pod in pods.items:
                        if self._is_named_pod(pod, app_name):
                            filteredPods.append(pod)
                    pods = filteredPods
                return pods
            else:
                return self.core_client.list_pod_for_all_namespaces()
        except ApiException as e:
            raise DaoError(e)
    
    def _is_named_pod(self, pod : V1Pod, app_name : str) -> V1Pod:
        return pod.metadata.labels.get('app') == app_name
     
    def get_pod_ips(self, namespace : str, app_name : str=None) -> List[str]:
        return [pod.status.pod_ip for pod in self.get_pods(namespace, app_name)]
