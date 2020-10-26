#!/usr/bin/env python3

from pathlib import Path
import sys
import pandas as pd

VERSION = (0, 0, 8)
RELEASE_DATE = '2018-07-11'
__author__ = 'Benjamin Leopold'
__contact__ = 'bleopold@jax.org'
__license__ = 'GPL v3+'

### column names:
#  qseqid 
#  sseqid 
#  pident 
#  nident 
#  evalue 
#  bitscore 
#  align_length 
#  qlen 
#  slen 

PCT_FULL_LENGTH = 98.5

def get_highest_value(df, id_column:str, max_column:str):
    """return df with higest value of column dfs (e.g. max pident of qseqid)"""
    df_max = {}
    for val in df[id_column].unique():
        dv = df.loc[df[id_column]==val]
        max_idx = dv[max_column].idxmax()
        # print(f'get_highest_value: max for {val} is index {max_idx}')
        df_max[max_idx] = dv.loc[max_idx]
    highest1 = pd.DataFrame.from_records(df_max) #, columns=df.columns)
    # print(f'df_max len: {len(df_max)}')
    # print(f'highest1 head: {highest1.head(10)!s}')
    return highest1.transpose()

def parse_file(filename):
    """Parse and filter passed tsv file of blast
       results from checks of OTU sequences
       against themselves and each other.
    """
    try:
        df = pd.read_table(filename)

        # naming
        filepath = Path(filename)
        filestem = filepath.stem
        filesuff = filepath.suffix

        # remove qseqid==sseqid records (self-blasted)
        df = df.loc[df['qseqid'] != df['sseqid']]

        # add column of pct alignment of length
        df = df.assign(length_pct=lambda f:f['length']/f['qlen']*100)

        # add column of boolean full_length, then modify based on length_pct
        df = df.assign(full_length=False)
        df.loc[df['length_pct']>PCT_FULL_LENGTH, 'full_length'] = True

        # make df of each qseqid, using filter based on full_length

        print(f'df: {df.count(0)}')
        tsv_file = filestem + '_all.tsv'
        df.to_csv(tsv_file, sep='\t', index=False)

        df_best = get_highest_value(df_fulls, 'qseqid', 'pident')
        print(f'df_best: {df_best.count(0)}')
        tsv_file = filestem + '_all_best.tsv'
        df_fulls_best.to_csv(tsv_file, sep='\t', index=False)


        df_fulls = df.loc[df['full_length']==True]
        print(f'df_fulls: {df_fulls.count(0)}')
        tsv_file = filestem + '_fulls.tsv'
        df_fulls.to_csv(tsv_file, sep='\t', index=False)

        df_fulls_best = get_highest_value(df_fulls, 'qseqid', 'pident')
        print(f'df_fulls_best: {df_fulls_best.count(0)}')
        # print(f'df_fulls_best: {df_fulls_best.head(10)}')
        tsv_file = filestem + '_fulls_best.tsv'
        df_fulls_best.to_csv(tsv_file, sep='\t', index=False)
        # xls_file = filestem + '_fulls.xlsx'
        # df_fulls_best.to_excel(xl_file, sheet_name='full_best_match')

        df_short = df.loc[df['full_length']==False]
        tsv_file = filestem + '_shorts.tsv'
        df_short.to_csv(tsv_file, sep='\t', index=False)

        # df_short_best = get_highest_value(df_short, 'qseqid', 'pident')
        # print(f'df_short_best: {df_short_best.count(0)}')
        # print(f'df_short_best: {df_short_best.head(10)}')

        # xls_file = filestem + '_shorts.xlsx'
        # df_short_best.to_excel(xls_file, sheet_name='short_best_match')


        # write outputs to files
        with open(filestem+"_full_length.tsv") as out_full:
            # filter and write df out to files
            pass

        with open(filestem+"_shorter.tsv") as out_short:
            # filter and write df out to files
            pass
    except FileNotFoundError as e:
        # exit(f"File can't be located with path: '{e.args!s}'")
        exit(e.args[0])
    except Exception as e:
        raise e
    finally:
        pass


if __name__ == '__main__':
    if len(sys.argv) < 2:
        exit(f"Usage: {sys.argv[0]} <tsv filename of otu blast results>")
    parse_file(sys.argv[1])
