from __future__ import unicode_literals
from lxml.etree import tostring
from lxml.builder import E
import random
import string
import uuid

SYMBOLS_POPULATION = " " + string.ascii_letters + string.digits + string.punctuation

def get_uuid_string():
    return uuid.uuid1(random.getrandbits(48)).hex


def get_random_string():
    len = random.randrange(1, 100)
    return ''.join((random.choice(SYMBOLS_POPULATION) for x in xrange(len)))


def generate_file_name(extension=None):

    if extension:
        return '{}.{}'.format(get_uuid_string(), extension)
    else:
        return get_uuid_string()


class ManagerId(object):

    def __init__(self):
        self.uniq_id_store = set()
        self.max_retry = 5

    def generate_uniq_id(self):
        result = None
        flag_no_uniq = True
        retry = 0
        while flag_no_uniq:
            uniq_id_str = get_uuid_string()
            if uniq_id_str not in self.uniq_id_store:
                flag_no_uniq = False
                result = uniq_id_str
            else:
                retry += 1
                if retry >= self.max_retry:
                    raise Exception('ManagerId retry in generate exceed max')
        return result


def create_random_xml_string(manager_id):
    uniq_id_str = manager_id.generate_uniq_id()
    value_str = '{}'.format(random.randrange(1, 100))
    count_objects = random.randrange(1, 10)

    objects = tuple(E.object(name='{} : {}'.format(x, get_random_string()))
                             for x in xrange(count_objects)
                             )
    result = tostring(E.root(E.var(name='id', value=uniq_id_str),
                             E.var(name='level', value=value_str),
                             E.objects(*objects)
                             ),
                      pretty_print=True,
                      xml_declaration=False,
                      encoding='UTF-16')
    return result


def generate_zip_files(path_to='.', count=50, objects_in_zip=100):
    import os
    import zipfile
    print(os.path.abspath(path_to))
    if not os.path.isdir(path_to):
        os.makedirs(path_to)

    manager_id = ManagerId()

    for i in xrange(count):
        name = 'zip_with_xml_{:03}_{}.zip'.format(i, uuid.uuid1().hex)
        full_path = os.path.join(path_to, name)
        with zipfile.ZipFile(full_path, 'w') as z:
            for x in xrange(objects_in_zip):
                data = create_random_xml_string(manager_id)
                xml_name = 'xml_{:04}_{}.xml'.format(x, uuid.uuid1().hex)
                z.writestr(xml_name, data)

if __name__ == '__main__':
    generate_zip_files(path_to='!!')

