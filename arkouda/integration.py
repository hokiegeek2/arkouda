import os
from typing import List, Optional, Union
from kubernetes import client # type: ignore
from kubernetes.client.rest import ApiException # type: ignore
from kubernetes.client import V1Pod # type: ignore


class DaoError(Exception):
    pass

'''
The KubernetesDao class encapsulates metadata and methods used to retrieve
objects such as pods and services from Kubernetes.
'''
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
        import urllib3 # type: ignore
        urllib3.disable_warnings()

        try: 
            api_client = client.ApiClient(configuration=config)
            self.core_client = client.CoreV1Api(api_client=api_client)
            self.apps_client = client.AppsV1Api(api_client)
        except Exception as e:
            raise DaoError(e)
    
    def get_pods(self, namespace : str, 
                               app_name : Optional[str]=None) -> List[V1Pod]:
        '''
        Retrieves a list of V1Pod objects corresponding to pods within a 
        namespace. An app name is optionally provided to narrow the scope
        of pods returned.

        :param str namespace: namespace to be queried
        :param Optional[str] app_name: name of app corresponding to the pods
        :return: a list of pods
        :rtype: List[V1Pod]
        :raises: DaoError if there is an error in retrieving pods
        '''
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
    
    def _is_named_pod(self, pod : V1Pod, app_name : str) -> bool:
        '''
        Indicates where the pod has an app name.
        
        :return: boolean indicating if the pod has an app name
        :rtype: bool
        '''
        return pod.metadata.labels.get('app') == app_name
     
    def get_pod_ips(self, namespace : str, app_name : str=None,
                                pretty_print=False) -> Union[List[str],str]:
        '''
        Retrieves the overlay network ip addresses for the pods within a 
        namespace. An app name is optionally provided to narrow the scope
        of ip addresses returned.
        
        :param str namespace: namespace to be queried
        :param str app_name: name of the app corresponding to the pods
        :return: a list of ip addresses
        :rtype: Union[List[str],str]
        :raises: DaoError if there is an error in retrieving pods
        '''
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

