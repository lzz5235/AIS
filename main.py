# -*- coding: utf8

from mysqlmgr import MysqlMgr
from elasticsearchmgr import esmgr
from ais_parser import parse
import os
import threading

def import_from_xml(db):
    par = parse("", "/AIS/data/normal")
    DY_gen = par.get_data_DY_gen()
    ST_gen = par.get_data_ST_gen()

    for dy in DY_gen:
        for d in dy:
            db.enqueue_DY(d)

    for st in ST_gen:
        for s in st:
            db.enqueue_ST(s)

def import_from_txt(db):
    par = parse("", "/media/xxx/1FE010E3268C15CA/elane_ais/JP_US/")
    dygen = par.read_data_txt_dy_gen()
    for node in dygen:
        # db.enqueue_DY(node)
        print node

    stgen = par.read_data_txt_st_gen()
    for node in stgen:
        # db.enqueue_ST(node)
        print node


def import_from_xml_bulk(db, folder):
    par = parse("", folder)
    DY_gen = par.get_data_DY_gen()
    ST_gen = par.get_data_ST_gen()

    for dy in DY_gen:
        db.import_dy_bulk(dy)

    for st in ST_gen:
        db.import_st_bulk(st)

def import_from_xml_bulk_multi(db,folder):
    def loop_dy(db,DY_gen):
        for dy in DY_gen:
            db.import_dy_bulk(dy)

    def loop_st(db,ST_gen):
        for st in ST_gen:
            db.import_st_bulk(st)

    par = parse("", folder)
    DY_gen = par.get_data_DY_gen()
    ST_gen = par.get_data_ST_gen()

    dy = threading.Thread(target=loop_dy,args=(db,DY_gen,))
    st = threading.Thread(target=loop_st,args=(db,ST_gen,))

    dy.start()
    st.start()
    dy.join()
    st.join()

def import_from_xml_parallel_bulk(db, folder):
    par = parse("", folder)
    # DY_gen = par.get_data_DY_gen()
    DY_gen = par.get_data_DY_cxw_gen()
    ST_gen = par.get_data_ST_gen()

    for dy in DY_gen:
        db.import_dy_parallel_bulk(dy)

    # for st in ST_gen:
    #     db.import_st_parallel_bulk(st)

if __name__ == "__main__":
    # db = MysqlMgr(10)
    db = esmgr()
    # import_from_xml_bulk(db,"/AIS/data/normal")
    # import_from_xml_bulk(db, "/AIS/data/satellite")
    # import_from_xml_parallel_bulk(db,"/AIS/data/unuseful")
    # import_from_xml_parallel_bulk(db, "/AIS/cxw-ais")
    # db.fetch_DY(310621000)
    # db.fetch_ST(310621000)
    db.get_remove_duplicate()
