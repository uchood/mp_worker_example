from __future__ import unicode_literals
from multiprocessing import Manager, Process
from multiprocessing.queues import Empty
import uuid
import time
import logging
import traceback
import os
import csv

import zipfile

from lxml import etree


def prepare_xml(xml_as_string=None):
    """extract id,level, objects_names"""
    try:
        root  = etree.fromstring(xml_as_string)    
        var_id = root.find("var[@name='id']")
        var_level = root.find("var[@name='level']")
        objects = root.find("objects")
        var_id.attrib['value']
        var_level.attrib['value']
        result = {
            'id': var_id.attrib['value'],
            'level': var_level.attrib['value'],
            'objects_names': [x.attrib['name'] for x in objects]
        }
        return result
    except Exception as e:
        raise


def prepare_zip_with_xml(file_name):
    """extract xml"""
    list_xml = []
    list_xml_data = []
    with zipfile.ZipFile(file_name) as zf:
        for x in zf.infolist():
            if x.filename.endswith('.xml'):
                list_xml.append(x.filename)
                list_xml_data.append(prepare_xml(zf.read(x.filename)))
    return list_xml_data


def worker(queue_in, queue_out, queue_errors, worker_id):
    loglevel = logging.INFO
    logging.basicConfig(filename='log_{}.log'.format(worker_id),
                        level=loglevel,
                        format='%(levelname)-8s %(asctime)-16s %(message)s')
    logging.info('worker {} Started '.format(worker_id))
    error_count = 0
    errors = []
    counter = 0
    while True:
        try:
            zip_file_name = None
            zip_file_name = queue_in.get_nowait()
            if zip_file_name is StopIteration:
                logging.info('{}: Quitting time!'.format(worker_id))
                queue_out.put(StopIteration)
                break
            counter += 1
            logging.debug('{}: {} : {}'.format(
                worker_id, counter, zip_file_name))
            list_xml_data = prepare_zip_with_xml(zip_file_name)
            for x in list_xml_data:
                logging.debug('{}: {}'.format(worker_id, x))
                queue_out.put(x)
        except Empty:
            time.sleep(0.01)
            logging.debug("{}: empty".format(worker_id, ))
            pass
        except Exception as e:
            logging.exception("{}: {}".format(worker_id, e))
            logging.exception(traceback.format_exc())
            error_message = {
                "worker": worker_id,
                "file": zip_file_name,
                "error": '{}'.format(e)
            }
            queue_errors.put(error_message)
            errors.append(error_message)
            error_count += 1
            pass
    logging.info('{} : worker_in_pool stopped : error count = {}'.
                 format(worker_id, error_count))
    if error_count > 0:
        logging.error("{}: error: {}".format(worker_id, error_count))
    logging.debug('worker {} stopped '.format(worker_id))
    # queue_in.close()
    # queue_out.close()
    # queue_errors.close()


def zip_iterator(root="."):
    extension = ".zip"
    for root, dirs, files in os.walk(root):
        for x in files:
            if x.endswith(extension):
                yield os.path.join(root, x)


def prepare_extract_xml_from_zips(root='.'):
    manager = Manager()
    q_zip_files = manager.Queue()
    q_result = manager.Queue()
    q_errors = manager.Queue()
    count_processes = 2
    pool_processes = []
    for x in xrange(count_processes):
        pool_processes.append(Process(target=worker,
                                      args=(q_zip_files,
                                            q_result,
                                            q_errors,
                                            'worker_{}'.format(x)
                              )))
    for x in pool_processes:
        x.start()
    print("started [{}] processes".format(count_processes))
    counter = 0
    for x in zip_iterator():
        q_zip_files.put(x)
        counter += 1
    q_zip_files.put(StopIteration)
    q_zip_files.put(StopIteration)
    print("[{}] zip files in queue".format(counter))
    count_stop_iterations = 0
    counter = 0
    with open('id_level.csv', 'wb') as csvfile_levels:
        levels_writer = csv.DictWriter(csvfile_levels,
                                       fieldnames=['id', 'level'])
        levels_writer.writeheader()
        with open('id_object.csv', 'wb') as csvfile_objects:
            objects_writer = csv.DictWriter(csvfile_objects,
                                            fieldnames=['id', 'object'], 
                                            quoting=csv.QUOTE_MINIMAL)
            objects_writer.writeheader()
            while True:
                try:
                    extracted_data = q_result.get_nowait()
                    if extracted_data is StopIteration:
                        count_stop_iterations += 1
                        if count_stop_iterations >= count_processes:
                            logging.debug('q_result: Quitting time!')
                            break
                        else:
                            continue
                    counter += 1
                    levels_writer.writerow({"id": extracted_data['id'],
                                            "level": extracted_data['level']
                                            })
                    for x in extracted_data['objects_names']:
                        objects_writer.writerow({"id": extracted_data['id'],
                                                 "object": x.encode("utf-8")
                                                 })
                    logging.debug('{}: {} : {}'.format(
                        extracted_data['id'],
                        extracted_data['level'],
                        extracted_data['objects_names']
                    ))
                except Empty:
                    time.sleep(0.01)
                    logging.debug("q_result: empty")
                    pass
                except Exception as e:
                    logging.exception("{}: {}".format('q_result', e))
                    logging.exception(traceback.format_exc())
                    pass
    print("[{}] xml files success prepared".format(counter))
    for x in pool_processes:
        x.join()
    count_errors = 0
    if q_errors.qsize() > 0:
        while True:
            try:
                error = q_errors.get_nowait()
                count_errors += 1
                logging.error('{}: {} : {}'.format(error['file'],
                                                   error['worker'],
                                                   error['error']
                                                   )
                              )
            except Empty:
                time.sleep(0.01)
                logging.debug("q_errors: empty")
                if q_errors.empty():
                    break
                pass
            except Exception as e:
                logging.exception("{}: {}".format('q_result', e))
                logging.exception(traceback.format_exc())
                pass
    if count_errors > 0:
        print("Found [{}] errors in prepare files".format(count_errors))
    else:
        print("all done")



if __name__ == '__main__':
    loglevel = logging.INFO
    logging.basicConfig(filename='log.log', level=loglevel,
                        format='%(levelname)-8s %(asctime)-16s %(message)s')
    logging.info("start")
    prepare_extract_xml_from_zips(root='.')
    logging.info("end")    