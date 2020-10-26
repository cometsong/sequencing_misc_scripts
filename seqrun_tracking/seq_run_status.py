#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""seq_run_status.py - pass a TSV list of sequencing runs with headers including 'FlowcellID'
    e.g.: RunStartDate,RunFinishDate,Location,Instrument,FlowcellID,RunFolder,Projects,SampleType, ...
Expects project folder nomenclature:
    proj/seqtype/raw/fastq/FlowcellID/*.fastq.gz
    proj/seqtype/processed/clean/FlowcellID/*.fast[qa](.gz)?
Writes out csv file of each flowcell, its sequencing type, and the corresponding list of project
    names that are demuxed and are cleaned.
"""

__author__ = 'Benjamin Leopold (cometsong)'
__email__ = 'benjamin(at)cometsong(dot)net'


import os
import sys
import re
import csv
import logging
import time
from pathlib import Path

"""Subprocess module wrappers"""
from subprocess import call, check_output
from subprocess import STDOUT
from subprocess import CalledProcessError as SubprocCallError

from utils import log_it, load_records, write_out_csv, get_field_header

#TODO: refactor for more than just ILMN run/proc/etc patterns

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Constants ~~~~~
COOLNESS = True

log = log_it('seqrun_check')

class defaults:
    class path:
        base = '/data/labname'
        projects = '/data/labname/projects'
    seq_types = ('16S', 'rnaseq', 'mwgs', 'virome', 'wgs')

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Classy + Funky ~~~~~
class Project(object):
    """manage projects, list, seq types, processed runs
       Requires: pathlib
    """
    def __init__(self, basepath='.'):
        # super(Project, self).__init__()
        # self.arg = arg
        self.basepath = Path(basepath)
        self.unwanted = ['PROT', 'TEST', 'WLPT']

    def iter_paths(self):
        bp = self.basepath
        for proj in bp.glob('*[A-Z0-9]_*'):
            if proj.is_dir() and proj.name[0:4] not in self.unwanted:
                yield proj

    def list_names(self):
        return [proj.name for proj in self.iter_paths()]

    def list_paths(self):
        return [proj for proj in self.iter_paths()]


def seqruns(run_list):
    """loads all seqrun names from csv file"""
    runs = []
    for r in load_records(run_list):
        runs.append(r)
    return runs


def check_runs(projects, runid_list, path_glob):
    """check runids in projects within the specified path_glob
       return dict of found status and list of projects found
    """
    runs_found = {runid : {'found': False, 'seqtype': None, 'projects': []}
                  for runid in runid_list}
    # log.debug('runs_found count: %i', len(runs_found))
    # log.debug('runs_found id: %s', [id for id in runs_found])

    for runid in runid_list:
        proj_found = []
        for proj in projects.iter_paths():
            for seq in defaults.seq_types:
                proj_folder = proj / seq
                if proj_folder.exists():
                    subfolds = proj_folder.glob(path_glob+'*/*')
                    for sub in subfolds:
                        if sub.is_dir() and re.search(sub.name[1:-1], runid):
                            # check if in list, or if missing first letter in list, as
                            # some NextSeq runs inexplicably have done
                            log.info('Found proj: {}, runID {}, seq: {}'.format(
                                proj.name[0:4], sub.name, seq))
                            proj_found.insert(0, proj.name[0:4])
                            runs_found[runid] = {
                                'found': True,
                                'seqtype': seq,
                                'projects': list(set(proj_found))
                                }
                            log.debug('runid_match proj: %s', runs_found[runid]['projects'])
    return runs_found


def check_runs_processed(projects, runid_list, folder_glob='clean'):
    """iterate list of projects, check each for runID in each seqtype's
       processed/glob folder structure
    """
    return check_runs(projects, runid_list, path_glob='processed/'+folder_glob)


def check_runs_demuxed(projects, runid_list, folder_glob='fastq'):
    """iterate list of projects, check each for runID in each seqtype's
       raw/glob folder structure
    """
    return check_runs(projects, runid_list, path_glob='raw/'+folder_glob)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Make it Happen! ~~~~~
def main(args):
    """make it happen"""
    # args = parse_args(args)

    projects = Project(defaults.path.projects)
    # proj_list = projects.list_paths()
    proj_names = projects.list_names()
    # print('Project objects: {}'.format(proj_list))
    # log.info('Project names: {}'.format(proj_names))
    log.info('Projects: ({})'.format(len(proj_names)))

    ### extract runIDs, remove any duplicates (multi-lanes, etc)
    runs = seqruns(args.run_list_file)
    runid_list = sorted(set([r['FlowcellID'] for r in runs]))
    log.info('Run IDs ({}): {}'.format(len(runid_list), runid_list))


    ### find, count all (un)demuxed runs from projects 'raw' folders
    runs_raw_check = check_runs_demuxed(projects, runid_list, 'fastq')
    log.info("Runs checked: %i", len(runs_raw_check))
    log.info("Runs checked: (%i): %s", len(runs_raw_check), runs_raw_check)

    runs_demuxed = {runid:vals for runid, vals in runs_raw_check.items()
                                if vals['found'] == True}
    log.info("Runs ARE demuxed (%i): %s",
             len(runs_demuxed), sorted(runs_demuxed))
    runs_undemux = {runid:vals for runid, vals in runs_raw_check.items()
                                if vals['found'] == False}
    log.info("Runs NOT demuxed  (%i): %s",
             len(runs_undemux), sorted(runs_undemux))


    ### find, count all (un)cleaned runs from projects 'processed' folders
    runs_clean_check = check_runs_processed(projects, runs_demuxed, 'cleaned')
    log.info("Runs checked: %i", len(runs_clean_check))

    runs_cleaned = {runid:vals for runid, vals in runs_clean_check.items()
                                if vals['found'] == True}
    log.info("Runs ARE cleaned (%i): %s",
             len(runs_cleaned), sorted(runs_cleaned))
    runs_unclean = {runid:vals for runid, vals in runs_clean_check.items()
                                if vals['found'] == False}
    log.info("Runs NOT cleaned (%i): %s",
             len(runs_unclean), sorted(runs_unclean))

    ### create map of runid to status: demuxed? cleaned?, write to outfile.csv
    run_status_map = {}
    status_fieldnames = ['runid', 'seqtype', 'demuxed', 'cleaned']
    status_outfile = args.run_list_file[:-4]+'_statuses.csv'
    # write headers to file
    write_out_csv(status_outfile, status_fieldnames, [])
    for runid in runid_list:
        if runid in runs_demuxed and len(runs_demuxed[runid]['projects']) > 0:
            projects_demux = ','.join(runs_demuxed[runid]['projects'])
            log.debug('runid %s projects demuxed: %s', runid, projects_demux)
            seq = runs_demuxed[runid]['seqtype']
            undemux = False
        else:
            projects_demux = 'None'
            seq = 'unk'
            undemux = True

        if runid in runs_cleaned and len(runs_cleaned[runid]['projects']) > 0:
            projects_clean = ','.join(runs_cleaned[runid]['projects'])
            log.debug('runid %s projects cleaned: %s', runid, projects_clean)
            unclean = False
        else:
            projects_clean = 'None'
            unclean = True

        runid_stat = {
                    'seqtype': seq,
                    'demuxed': projects_demux,
                    'cleaned': projects_clean,
                    }
        run_status_map.update(runid_stat)

        write_out_csv(status_outfile, status_fieldnames,
                    [{'runid': runid,
                      'seqtype': runid_stat.get('seqtype'),
                      'demuxed': runid_stat.get('demuxed'),
                      'cleaned': runid_stat.get('cleaned'),
                    }])

    return run_status_map

if __name__ == '__main__':
    # args = sys.argv[1:]
    class args:
        run_list_file = 'Sequencer_Runs_List.csv'

    sys.exit(main(args))
