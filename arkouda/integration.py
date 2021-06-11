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
        # used to disable non-verified cert warnings
        import urllib3
        urllib3.disable_warnings()

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
     
    def get_pod_ips(self, namespace : str, app_name : str=None, pretty_print=False) -> List[str]:
        ips = [pod.status.pod_ip for pod in self.get_pods(namespace, app_name)]
        if pretty_print:
            return str(ips).strip('[]').replace(',','')
        else:
            return ips

def main():
    import sys
    dao = KubernetesDao()
    namespace=sys.argv[1]
    app_name=sys.argv[2]
    print(dao.get_pod_ips(namespace=namespace, app_name=app_name, pretty_print=True))

if __name__ == "__main__":
    main()

