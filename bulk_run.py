from __future__ import unicode_literals
import logging
import multiprocessing

from create_work_set import generate_zip_files
from mp_worker_queue import prepare_extract_xml_from_zips

if __name__ == '__main__':
    loglevel = logging.INFO
    path_for_zips = "place_for_zips"
    count_workers = multiprocessing.cpu_count()

    logging.basicConfig(filename='log.log', level=loglevel,
                        format='%(levelname)-8s %(asctime)-16s %(message)s')
    logging.info("{}: start".format(__name__))
    logging.info("start create zips")
    generate_zip_files(path_to=path_for_zips)
    logging.info("create zips: done")
    logging.info("start prepare zips")
    prepare_extract_xml_from_zips(root=path_for_zips,
                                  count_workers=count_workers)
    logging.info("end")
