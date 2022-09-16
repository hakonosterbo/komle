import os
import requests
import sys
import pyxb
from enum import Enum
from suds.client import Client
from suds.transport.http import HttpAuthenticated
from suds.transport import Reply
from typing import Union, List, Dict, Any

if 'komle.bindings.v1411.write' in sys.modules:
    # Witsml uses the same namespace for each schema
    # So for now check what binding is in use
    from komle.bindings.v1411.write import witsml
else:
    # Default to import read_bindings
    from komle.bindings.v1411.read import witsml

class RequestsTransport(HttpAuthenticated):
    def __init__(self, **kwargs):
        self.verify = kwargs.pop('verify', None)
        self.auth = (kwargs.pop('username', None), kwargs.pop('password', None))
        HttpAuthenticated.__init__(self, **kwargs)

    def send(self, request):
        resp = requests.post(
            request.url,
            data=request.message,
            headers=request.headers,
            verify=self.verify,
            auth=self.auth,
        )
        resp.raise_for_status()
        result = Reply(resp.status_code, resp.headers, resp.content)
        return result

def simple_client(service_url: str, username: str, password: str,
                  agent_name: str='komle', verify: Union[bool,str]=True) -> Client:
    '''Create a simple soap client using Suds, 
    
    This initializes the client with a local version of WMLS.WSDL 1.4 from energistics.

    Args:
        service_url (str): url giving the location of the Store
        username (str): username on the service
        password (str): password on the service
        agent_name (str): User-Agent name to pass in header 
        verify (bool|str): Whether to verify TLS certificates, or path to a cacerts file

    Returns:
        client (Client): A suds client with wsdl
    '''

    transport = RequestsTransport(username=username,
                                  password=password,
                                  verify=verify)

    client = Client(f'file:{os.path.join(os.path.dirname(__file__), "WMLS.WSDL")}', 
                    location=service_url)

    client.set_options(transport=transport, headers={'User-Agent':agent_name})

    return client

class StoreException(Exception):
    def __init__(self, reply, base_message):
        super().__init__(f'{reply.Result} : {base_message} - {reply.SuppMsgOut}')
        self.reply = reply
        self.message = base_message

def _to_envelope(objects):
    try:
        object_type = type(objects[0])
        _, _, typename = object_type._Name().rpartition('obj_')
        envelope_type = getattr(witsml, f'{typename}s')
    except Exception as e:
        raise TypeError(str(e))
    q_objs = envelope_type(*objects, version=witsml.__version__)
    return typename, q_objs.toxml()

class BaseClient:
    def __init__(self, service_url: str, username: str, password: str,
                 agent_name: str='komle', verify: Union[bool,str]=True):
        '''Create a GetFromStore client, 
        
        This initializes the client with a local version of WMLS.WSDL 1.4 from energistics.
    
        Args:
            service_url (str): url giving the location of the Store
            username (str): username on the service
            password (str): password on the service
            agent_name (str): User-Agent name to pass in header
            verify (bool|str): Whether to verify TLS certificates, or path to a cacerts file
        '''
    
        self.soap_client = simple_client(service_url,
                                         username,
                                         password,
                                         agent_name,
                                         verify)
    def _parse_reply(self, reply):
        if reply.Result <= 0:
            try:
                message = self.soap_client.service.WMLS_GetBaseMsg(reply.Result)
            except:
                message = 'Could not parse error code'
            raise StoreException(reply, message)
        if hasattr(reply, 'XMLout'):
            return witsml.CreateFromDocument(reply.XMLout)

class StoreClient (BaseClient):
  
    def get_bhaRuns(self, 
                    q_bha: witsml.obj_bhaRun,
                    returnElements: str='id-only') -> witsml.bhaRuns:
        '''Get bhaRuns from a witsml store server
    
        The default is only to return id-only, change to all when you know what bhaRun to get.
    
    
        Args:
            q_bha (witsml.obj_bhaRun): A query bhaRun specifing objects to return, can be an empty bhaRun
            returnElements (str): String describing data to get on of [all, id-only, header-only, data-only, station-location-only
                                                                       latest-change-only, requested]
        Returns:
            witsml.bhaRuns: bhaRuns a collection of bhaRun
        
        Raises:
            StoreException: If the soap server replies with an error
            pyxb.exception: If the reply is empty or the document fails to validate a pyxb exception is raised
        '''
    
        q_bhas = witsml.bhaRuns(version=witsml.__version__)
    
        q_bhas.append(q_bha)
    
        reply_bhas = self.soap_client.service.WMLS_GetFromStore('bhaRun',
                                                                q_bhas.toxml(),
                                                                OptionsIn=f'returnElements={returnElements}'
                                                               )
    
        return self._parse_reply(reply_bhas)


    def get_logs(self, 
                 q_log: witsml.obj_log,
                 returnElements: str='id-only') -> witsml.logs:
        '''Get logs from a witsml store server
    
        The default is to return id-only, change to all when you know what log to get.
        Pass an empty log with returnElements id-only to get all by id.
    
    
        Args:
            q_log (witsml.obj_log): A query log specifing objects to return, for example uidWell, uidWellbore or an empty log
            returnElements (str): String describing data to get on of [all, id-only, header-only, data-only, station-location-only
                                                                       latest-change-only, requested]
        Returns:
            witsml.logs: logs a collection of log
        
        Raises:
            StoreException: If the soap server replies with an error
            pyxb.exception: If the reply is empty or the document fails to validate a pyxb exception is raised
        '''
    
        q_logs = witsml.logs(version=witsml.__version__)
    
        q_logs.append(q_log)
    
        reply_logs = self.soap_client.service.WMLS_GetFromStore('log',
                                                                q_logs.toxml(),
                                                                OptionsIn=f'returnElements={returnElements}'
                                                               )

        return self._parse_reply(reply_logs)

    def get_mudLogs(self, 
                    q_mudlog: witsml.obj_mudLog,
                    returnElements: str='id-only') -> witsml.mudLogs:
        '''Get mudLogs from a witsml store server
    
        The default is only to return id-only, change to all when you know what mudLog to get.
        Pass an empty mudLog with returnElements id-only to get all by id.
    
    
        Args:
            q_mudlog (witsml.obj_mudLog): A query mudLog specifing objects to return, can be empty
            returnElements (str): String describing data to get on of [all, id-only, header-only, data-only, station-location-only
                                                                       latest-change-only, requested]
        Returns:
            witsml.mudLogs: mudLogs, a collection of mudLog
        
        Raises:
            StoreException: If the soap server replies with an error
            pyxb.exception: If the reply is empty or the document fails to validate a pyxb exception is raised
        '''
    
        q_mudlogs = witsml.mudLogs(version=witsml.__version__)
    
        q_mudlogs.append(q_mudlog)
    
        reply_mudlogs = self.soap_client.service.WMLS_GetFromStore('mudLog',
                                                                   q_mudlogs.toxml(),
                                                                   OptionsIn=f'returnElements={returnElements}'
                                                                  )
    
        return self._parse_reply(reply_mudlogs)

    def get_trajectorys(self, 
                        q_traj: witsml.obj_trajectory,
                        returnElements: str='id-only') -> witsml.trajectorys:
        '''Get trajectorys from a witsml store server
    
        The default is only to return id-only, change to all when you know what trajectory to get.
        Pass an empty trajectory with returnElements id-only to get all by id.
    
        Args:
            q_traj (witsml.obj_trajectory): A query trajectory specifing objects to return
            returnElements (str): String describing data to get on of [all, id-only, header-only, data-only, station-location-only
                                                                       latest-change-only, requested]
        Returns:
            witsml.trajectorys: trajectorys, a collection of trajectory
        
        Raises:
            StoreException: If the soap server replies with an error
            pyxb.exception: If the reply is empty or the document fails to validate a pyxb exception is raised
        '''
    
        q_trajs = witsml.trajectorys(version=witsml.__version__)
    
        q_trajs.append(q_traj)
    
        reply_traj = self.soap_client.service.WMLS_GetFromStore('trajectory',
                                                                q_trajs.toxml(),
                                                                OptionsIn=f'returnElements={returnElements}'
                                                               )
    
        return self._parse_reply(reply_traj)

    def get_wellbores(self, 
                      q_wellbore: witsml.obj_wellbore,
                      returnElements: str='id-only') -> witsml.wellbores:
        '''Get wellbores from a witsml store server
    
        The default is only to return id-only, change to all when you know what wellbore to get.
    
    
        Args:
            q_wellbore (witsml.obj_wellbore): A query wellbore specifing objects to return, can be an empty wellbore
            returnElements (str): String describing data to get on of [all, id-only, header-only, data-only, station-location-only
                                                                       latest-change-only, requested]
        Returns:
            witsml.wellbores: wellbores
        
        Raises:
            StoreException: If the soap server replies with an error
            pyxb.exception: If the reply is empty or the document fails to validate a pyxb exception is raised
        '''
    
        q_wellbores = witsml.wellbores(version=witsml.__version__)
    
        q_wellbores.append(q_wellbore)
    
        reply_wellbores = self.soap_client.service.WMLS_GetFromStore('wellbore',
                                                                     q_wellbores.toxml(),
                                                                     OptionsIn=f'returnElements={returnElements}'
                                                                    )
    
        return self._parse_reply(reply_wellbores)

class ReturnElements(str, Enum):
    All                 = 'all'
    IdOnly              = 'id-only'
    HeaderOnly          = 'header-only'
    DataOnly            = 'data-only'
    StationLocationOnly = 'station-location-only'
    LatestChangeOnly    = 'latest-change-only'
    Requested           = 'requested'

class StoreGenericClient(BaseClient):
    witsml = witsml # can be used instead of import by caller

    def list(self, object_type: Union[str,type],
             returnElements: Union[ReturnElements,str]=ReturnElements.IdOnly,
             **selector):
        '''List objects from a witsml store server

        The default is to return id-only for all objects

        Args:
            object_type (str|type): The name or type of the witsml type to query (e.g., 'wellbore'
                                                                          or witsml.obj_wellbore)
            returnElements (ReturnElements|str): Which data to get
            selector (kwargs): Filter for objects, if no kwargs are specified then all objects
                               are returned

        Returns:
            pyxb.binding.content._PluralBinding: The returned objects of the given type

        Raises:
            TypeError: Object type is not a queryable WITSML type
            TypeError: Selector contains field(s) not appropriate for type
            ValueError: Invalid returnElements specification
            StoreException: If the soap server replies with an error
            pyxb.exception: If the reply is empty or the document fails to validate a pyxb exception is raised

        Examples:
            >>> wb = client.list('wellbore')  # or witsml.obj_wellbore
            >>> ku.plural_dict(wb)
            {'name': ['wellb1', 'wellb2', 'wellb3', ...]}
            >>> wb = client.list('wellbore', operator='Tigergutt')
            >>> ku.plural_dict(wb)
            {'name': ['wellb1', 'wellb3']}
            >>> wb = client.list('wellbore', 'all', operator='Tigergutt')
            >>> ku.plural_dict(wb)
            {'name': ['wellb1', 'wellb3'], 'operator': ['Tigergutt', 'Tigergutt'], ...}
        '''
        return self.get(object_type, [selector], returnElements=returnElements)


    def get(self, object_type: Union[str,type],
            selectors: List[Dict[str,Any]],
            returnElements: Union[ReturnElements,str]=ReturnElements.All):
        '''Get selected objects from a witsml store server

        The default is to return all data for the selected objects.

        Args:
            object_type (str|type): The name or type of the witsml type to query (e.g., 'wellbore'
                                                                          or witsml.obj_wellbore)
            selectors (list of dicts): Arguments to the witsml type, specifying which objects to query,
            returnElements (ReturnElements|str): Which data to get
        Returns:
            pyxb.binding.content._PluralBinding: The returned objects of the given type

        Raises:
            TypeError: Object type is not a queryable WITSML type
            TypeError: Selectors list is empty, or contain field(s) not appropriate for type
            ValueError: Invalid returnElements specification
            StoreException: If the soap server replies with an error
            pyxb.exception: If the reply is empty or the document fails to validate a pyxb exception is raised

        Examples:
            >>> wb = client.get('wellbore',  # or witsml.obj_wellbore
                                selectors=[dict(name='wellb1'), dict(name=wellb2)])
            >>> ku.plural_dict(wb)
            {'name': ['wellb1', 'wellb2'], 'operator': ['Tigergutt', 'Brumm'], ...}
        '''
        try:
            if isinstance(object_type, str):
                object_type = getattr(witsml, f'obj_{object_type}')
            objects = [object_type(**selector) for selector in selectors]
        except Exception as e:
            raise TypeError(str(e))
        return self.get_objects(objects, returnElements=returnElements)


    def get_objects(self, objects: List[pyxb.binding.basis.complexTypeDefinition],
                    returnElements: Union[ReturnElements,str]=ReturnElements.All):
        '''Get selected objects from a witsml store server

        The default is to return all data for the selected objects.

        Args:
            objects (list of objects): Objects to query, must be all of the same type
            returnElements (ReturnElements|str): Which data to get
        Returns:
            pyxb.binding.content._PluralBinding: The returned objects of the given type

        Raises:
            TypeError: Objects list is empty, or not a queryable type
            pyxb.MixedContentError: Objects are not all of same type
            ValueError: Invalid returnElements specification
            StoreException: If the soap server replies with an error
            pyxb.exception: If the reply is empty or the document fails to validate a pyxb exception is raised

        Examples:
            >>> wb = client.get_objects([witsml.obj_wellbore(name='wellb1'),
                                         witsml.obj_wellbore(name='wellb2')])
            >>> ku.plural_dict(wb)
            {'name': ['wellb1', 'wellb2'], 'operator': ['Tigergutt', 'Brumm'], ...}
        '''
        typename, xml = _to_envelope(objects)
        options = f'returnElements={ReturnElements(returnElements)}'
        reply = self.soap_client.service.WMLS_GetFromStore(typename, xml, OptionsIn=options)
        return getattr(self._parse_reply(reply), typename)

    def add_objects(self, objects: List[pyxb.binding.basis.complexTypeDefinition]):
        typename, xml = _to_envelope(objects)
        reply = self.soap_client.service.WMLS_AddToStore(typename, xml)
        self._parse_reply(reply)

    def delete(self, object_type: Union[str,type], **selector):
        try:
            if isinstance(object_type, str):
                object_type = getattr(witsml, f'obj_{object_type}')
            objects = [object_type(**selector)]
        except Exception as e:
            raise TypeError(str(e))
        self.delete_objects(objects)

    def delete_objects(self, objects: List[pyxb.binding.basis.complexTypeDefinition]):
        typename, xml = _to_envelope(objects)
        reply = self.soap_client.service.WMLS_DeleteFromStore(typename, xml)
        self._parse_reply(reply)

    def update_objects(self, objects:List[pyxb.binding.basis.complexTypeDefinition]):
        typename, xml = _to_envelope(objects)
        reply = self.soap_client.service.WMLS_UpdateInStore(typename, xml)
        self._parse_reply(reply)
