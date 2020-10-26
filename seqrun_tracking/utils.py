"""Set of utility functions shared amongst the Stanford/JAXGM OSDF script suite
"""

__author__ = 'Benjamin Leopold (cometsong)'
__email__ = 'benjamin(at)cometsong(dot)net'


import os
import sys
import csv
import re
import logging
import time
import importlib

"""Subprocess module wrappers"""
from subprocess import call, check_output
from subprocess import STDOUT
from subprocess import CalledProcessError as SubprocCallError

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Functional ~~~~~
# Log It!
def log_it(logname=os.path.basename(__file__)):
    """log_it setup"""
    curtime = time.strftime("%Y%m%d-%H%M")
    logfile = '.'.join([curtime, logname,'log'])

    loglevel = logging.DEBUG
    logFormat = \
        "%(asctime)s %(levelname)5s: %(name)15s %(funcName)s: %(message)s"
    formatter = logging.Formatter(logFormat)

    logging.basicConfig(format=logFormat)
    l = logging.getLogger(logname)

    root = logging.getLogger()

    fh = logging.FileHandler(logfile, mode='a')
    fh.setFormatter(formatter)

    root.addHandler(fh)
    l.addHandler(fh)

    l.setLevel(loglevel)

    return l

log = log_it()
# log.setLevel(logging.INFO)

def get_output(cmd_string, stderr=STDOUT, shell=True,
        universal_newlines=True, **kwargs):
    """wrapper for subprocess.call; takes single or list as arg"""
    return check_output(cmd_string, stderr=stderr, shell=shell,
                        universal_newlines=universal_newlines, **kwargs)

# dump_args decorator
# orig from: https://wiki.python.org/moin/PythonDecoratorLibrary#Easy_Dump_of_Function_Arguments
def dump_args(func):
    "This decorator dumps out the arguments passed to a function before calling it"
    argnames = func.func_code.co_varnames[:func.func_code.co_argcount]
    fname = func.func_name

    def func_args(*args,**kwargs):
        log.debug("'{}' args: {}".format(
            fname, ', '.join('%s=%r' % entry
                for entry in zip(argnames,args) + kwargs.items())) )
            # "'"+fname+"' args: "+', '.join(
            # '%s=%r' % entry
            # for entry in zip(argnames,args) + kwargs.items()))
        return func(*args, **kwargs)

    return func_args

# use example:
# @dump_args
# def f1(a,b,c):
#     print a + b + c
#
# f1(1, 2, 3)

def get_field_header(csv_file):
    """returns first row of csv file as list of fieldnames"""
    log.info('Loading fields from {}'.format(csv_file))
    with open(csv_file) as csvfh:
        try:
            reader = csv.DictReader(csvfh)
            return reader.fieldnames
        except csv.Error as e:
            log.exception('Reading CSV file %s, line %d: %s',
                    csv_file, reader.line_num, e)


def load_records(csv_file):
    """yield row dicts from csv_file using DictReader
    """
    log.info('Loading rows from {}'.format(csv_file))
    with open(csv_file) as csvfh:
        reader = csv.DictReader(csvfh)
        # log.debug('csv dictreader opened')
        try:
            for row in reader:
                # log.debug(row)
                yield row
        except csv.Error as e:
            log.exception('Reading CSV file %s, line %d: %s',
                    csv_file, reader.line_num, e)


def csv_type_sniff(csv_file):
    """find the lne/ending type using csv.sniffer"""
    try:
        with open(csv_file, 'rb') as f:
            dialect = csv.Sniffer().sniff(f.read(1024))
            return dialect
    except Exception as e:
        raise e


def write_out_csv(csv_file,fieldnames,values=[]):
    """write all values in csv format to outfile.
    Values is list of dicts w/ keys matching fieldnames.
    To write header to file, omit `values`
    """
    log.info('Writing csv to {}'.format(csv_file))
    try:
        with open(csv_file, 'a') as csvout:
            writer = csv.DictWriter(csvout, fieldnames)
            if values:
                try:
                    for row in values:
                        if isinstance(row, dict):
                            log.debug(row)
                            writer.writerow(row)
                except Exception as e:
                    log.exception('Writing CSV file %s, %s', csv_file, str(e))
                    raise e
            else:
                log.info('Writing header of fieldnames to {}'.format(csv_file))
                writer.writeheader()
    except IOError as e:
        raise e


def values_to_node_dict(values,keynames):
    """pass list of lists of values and list of keys of desired dict
       This converts to list of dicts
    """
    from collections import OrderedDict
    log.debug('In values_to_node_dict')
    final_list = []

    key_dict = OrderedDict()
    for key in keynames:
        key_dict[key] = ''

    for vals in values:
        l = vals
        d = key_dict.copy()
        k = d.keys()
        for x in range(len(d)):
            lx = l[x] if len(l) > x and l[x] is not None else ''
            d[k[x]] = lx
        # log.debug(d)
        final_list.append(d)

    return final_list

# values_to_node_dict([['foo','bar','1'],['spam','eggs','2','spoof','filled']])


def run_tests():
    log = log_it('Testing functions')
    tests = 0
    failures = 0

    from tempfile import mkstemp

    # test csv read/write/headers:
    tests += 1
    (csvfh1, csv_file1) = mkstemp('test','test',text=True)
    field_names1 = ['one','two','three']
    # print('field_names1: '+str(field_names1))
    csv_values1 = [{'one':'a', 'two':'b', 'three':'c'},
                   {'one':'d', 'two':'e', 'three':'f'}]
    write_out_csv(csv_file1,field_names1) #write headers
    write_out_csv(csv_file1,field_names1,csv_values1)
    field_names_read1 = get_field_header(csv_file1)
    # print('field_names_read1: '+str(field_names_read1))

    (csvfh2, csv_file2) = mkstemp('test2','test',text=True)
    values2 = [['g', 'h', 'i'], ['j', 'k', 'l']]
    csv_values2 = values_to_node_dict(values2,field_names_read1)
    write_out_csv(csv_file2,field_names1) #write headers
    write_out_csv(csv_file2,field_names_read1,csv_values2)
    field_names_read2 = get_field_header(csv_file2)
    # print('field_names_read2: '+str(field_names_read2))

    failures += 1 if field_names1 != field_names_read2 else 0
    log.warning('Test: fieldnames equal: %s', 'Y' if failures==0 else 'N')

    log.warn('Tests run: %s', tests)
    log.warn('Test failures: %s', failures)


if __name__ == '__main__':
    run_tests()
    pass
