import sys
from typing import Dict, List, Union, Any
from collections import OrderedDict
import pyxb

if 'komle.bindings.v20.witsml' in sys.modules:
    #Due to namespace collision we can't import bindings at the same time
    from komle.bindings.v20._eml import TimeStamp
    timestamp = TimeStamp

elif 'komle.bindings.v1411.write.witsml' in sys.modules:
    # Default to import read_bindings
    from komle.bindings.v1411.write.witsml import timestamp
    timestamp = timestamp
else:
    from komle.bindings.v1411.read.witsml import timestamp
    timestamp = timestamp



LOG_PRIM_TYPES = {'byte': bytes,
                  'date time': timestamp,
                  'double': float,
                  'float': float,
                  'int': int,
                  'long': int,
                  'short': int,
                  'string': str,
                  'string40': str,
                  'string16': str,
                  'unknown': str
                 }

def pretty_save(element, file_path:str):
    with open(file_path, 'w') as xml_file:
        xml_file.write(element.toDOM().toprettyxml())

def logdata_dict(log: 'witsml.obj_log', fill_missing: bool=True) -> Dict[str, List[Union[str, int, float, bytes]]]:
    '''Convert logData from a witsml log to a dict frame

    This is a dict where each mnemonic is the key to a list of data values.

    Args:
        log(witsml.obj_log): The log to extract logData from
        fill_missing(bool): If True set missing data to None, else ignore 

    Returns:
       Dict[str, List[witsml.LogDataType]]: A dict with each mnemonic as key and a list as value,
                                            where values are logData.data for the given mnemonic
    '''
    
    mnem_cast_map = {c.mnemonic.value(): LOG_PRIM_TYPES[c.typeLogData] for c in log.logCurveInfo}
    
    # The default delimiter is , 
    delimiter = ',' if log.dataDelimiter is None else log.dataDelimiter
    
    data_list = [(mnem, mnem_cast_map[mnem], []) for mnem in log.logData[0].mnemonicList.split(delimiter)]

    for data_str in log.logData[0].data:
        for i, point_str in enumerate(data_str.split(delimiter)):
            if point_str:
                value_cast = data_list[i][1]
                data_list[i][2].append(value_cast(point_str))

            elif fill_missing:
                data_list[i][2].append(None)
    
    return {mnem:values for mnem, _, values in data_list}

def to_plain(obj: 'witsml',
            include_attr: bool=False,
            prefix_attr: str='',
            delimiter: str='.',
            transpose: bool=False) -> Union[List,Dict[str, Any]]:
    '''Create a nested dict or list of plain python datatypes from a witsml object

    Args:
        obj(witsml.obj_...): A singular or plural witsml object
        include_attr(bool): Also take attributes
        prefix_attr(str): Give attributes a prefix for example @, default no prefix
        delimiter(str): Delimiter between object name and attribute name
        transpose (bool): Create dict-of-lists instead of lists-of-dicts for plurals

    Returns:
        dict|list: A dict or list representation of the witsml object(s), where values are plain datatypes
    '''

    def any_to_plain(obj):
        if isinstance(obj, (pyxb.binding.content._PluralBinding, list)):
            return plural_to_dict_of_lists(obj) if transpose else plural_to_list_of_plains(obj)
        elif obj.__class__.__name__ == 'obj_log':
            return logdata_dict(obj)
        else:
            return singular_to_plain(obj)

    def singular_to_plain(obj):
        if not isinstance(obj, pyxb.binding.basis.complexTypeDefinition):
            return obj;
        elif obj._ContentTypeTag in (obj._CT_EMPTY, obj._CT_SIMPLE):
            return obj.value()
        else:
            as_dict = {}
            if include_attr and hasattr(obj, '_AttributeMap'):
                for attr_use in obj._AttributeMap.values():
                    as_dict[prefix_attr+attr_use.id()] = attr_use.value(obj)
            names = [cont.elementDeclaration.id() for cont in obj.orderedContent() if cont.elementDeclaration is not None]
            # orderedContent() returns the PluralBindings repeatedly, because they may have ordering
            # interleaved with other elements. We disregard that possibility, and process the
            # PluralBinding once in order of first occurrence. (regular dicts are ordered in CPython3.6+, Python3.7+)
            for next_path in dict.fromkeys(names):
                    next_obj = getattr(obj, next_path)
                    if include_attr and hasattr(next_obj, '_AttributeMap'):
                        for attr_use in next_obj._AttributeMap.values():
                            as_dict[next_path+delimiter+prefix_attr+attr_use.id()] = attr_use.value(next_obj)
                    as_dict[next_path] = any_to_plain(next_obj)
            return as_dict

    def plural_to_list_of_plains(plural_obj):
        return [any_to_plain(obj) for obj in plural_obj]

    def plural_to_dict_of_lists(plural_obj):
        frame_list = []
        all_keys = {}
        for obj in plural_obj:
            as_dict = any_to_plain(obj)
            frame_list.append(as_dict)
            all_keys.update(as_dict) # values don't matter, we only care about the keys
        return {key: [frame.get(key, None) for frame in frame_list] for key in all_keys}

    return any_to_plain(obj)


def obj_dict(obj: 'witsml', 
             include_attr: bool=False,
             prefix_attr: str='',
             delimiter: str='.', 
             start_idx: int=0) -> Dict[str, 'witsml.datatype']:
    '''Flatten a witsml object into a flat dict

    Args:
        obj(witsml.obj_...): A singular witsml object for example obj_trajectory, obj_mudLog etc
        include_attr(bool): Also take attributes
        prefix_attr(str): Give attributes a prefix for example @, default no prefix
        delimiter(str): Deplimiter for nested elements, default . as in python
        start_idx(int): Start index for items in list, default 0

    Returns:
        flat_dict(dict): A flatten dict representation of the witsml obj, 
                         where values are witsml datatypes
    '''

    obj_list = [('', obj)]

    flatten_witsml = OrderedDict()

    while obj_list:

        base_name, obj = obj_list.pop()

        if isinstance(obj, pyxb.binding.content._PluralBinding):

            for i, item in enumerate(obj):
                obj_list.append((f'{base_name}[{start_idx+i}]', item))
            
            obj = None

        elif include_attr:

            for attr_use in obj._AttributeMap.values():
                next_path = delimiter.join(filter(None, [base_name, prefix_attr+attr_use.id()]))
                flatten_witsml[next_path] = attr_use.value(obj)
        
        if isinstance(obj, pyxb.binding.basis.complexTypeDefinition) and obj._ContentTypeTag not in (obj._CT_EMPTY, obj._CT_SIMPLE):

            for cont in obj.orderedContent():

                if cont.elementDeclaration is not None:

                    next_path = delimiter.join(filter(None, [base_name, cont.elementDeclaration.id()]))
                    next_obj = obj._ElementMap[cont.elementDeclaration.name()].value(obj)
                    obj_list.append((next_path, next_obj))
        
        elif obj is not None:
            value = obj
            if isinstance(obj, pyxb.binding.basis.complexTypeDefinition):
                value = value.value()
    
            flatten_witsml[base_name] = value


    return flatten_witsml

def plural_dict(plural_obj: pyxb.binding.content._PluralBinding, 
                 include_attr: bool=False, 
                 fill_missing: bool=True,
                 prefix_attr: str='',
                 delimiter: str='.', 
                 start_idx: int=0) -> Dict[str, List['witsml.datatype']]:
    '''Convert a plural witsml obj into a dict frame

    This is a dict where each leaf node path is the key to a list of data values.

    Args:
        plural_obj(pyxb.binding.content._PluralBinding): A witsml plural object for example,
                                                         trajectoryStation in trajectory, geologyInterval in mudLog,
                                                         mudLog in mudLogs etc
        include_attr(bool):Also take attributes
        prefix_attr(str): Give attributes a prefix for example @, default no prefix
        fill_missing(bool): If True set missing data to None, else ignore
        delimiter(str): Deplimiter for nested elements, default . as in python
        start_idx(int): Start index for items in list, default 0

    Returns:
        frame_dict(dict): A flatten dict representation of the witsml obj, 
                          where values are lists with witsml datatype items
    '''

    frame_list = []
    existing_keys = []
    for obj in plural_obj:

        flatten_witsml = obj_dict(obj, include_attr, prefix_attr, delimiter, start_idx)
        frame_list.append(flatten_witsml)
        existing_keys.extend([k for k in flatten_witsml if k not in existing_keys])

    
    if fill_missing:
            
        frame_dict = {key:len(plural_obj)*[None] for key in existing_keys}

        for idx, fw in enumerate(frame_list):
            for key in fw:
                if key in frame_dict:
                    frame_dict[key][idx] = fw[key]

        return frame_dict

    else:
    
        frame_dict = {key:[] for key in existing_keys}

        for fw in frame_list:
            for key in fw:
                frame_dict[key].append(fw[key])
        
        return frame_dict


        



