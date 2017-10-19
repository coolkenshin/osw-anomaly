#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import re
import subprocess
import calendar
import datetime
import time
import collections
import plotly.plotly as py
import plotly.graph_objs as go

#from certifi import __main__

__version__ = "1.0.0.170928"

"""
    NAME
      oswdata.py

    DESCRIPTION
      OSW data related methods.

    NOTES

    MODIFIED   (MM/DD/YY)
    luhwang     09/28/17 - created the initial OSWData class template
 
"""

import os
import sys
import re
import pandas as pd

"""
Category List
"""
PS = "ps"
TOP = "top"


"""Global Variables"""


"""Runtime Attributes"""

# -----------------------------------------------------------------------
# OSWData class


class OSWData(object):
    """OSWData related methods
    
    Parameters
    ----------
    path : string file path, default None
        path of the osw archive file

    category : string, default None
        type of the osw data, e.g. meminfo 
        
    Examples
    --------
    >>> path='/crash/cores/26855880/cffar13120402/domu/1506166186/var/log/ops/oswatcher/archive/oswmeminfo/cffar13120402.usdc2.oraclecloud.com_meminfo_17.09.23.0600.dat'
    >>> osw= OSWData(path=path,category='meminfo')
    >>> osw.df
    >>> osw.oswmem_free_memory(min=100,category='meminfo')
    >>>     False
    
    
     index, timestamp, category, sub_category, key, value
     0, Sat Sep 23 06:01:00 UTC 2017, meminfo, '', MemFree, 127332
         
    See also
    --------
    
    """

    def __init__(self, path=None, category=None):
        """OSWData class, init"""
        if path is None:
            raise ValueError("The 'path' arg is invalid!")

        if category is None:
            raise ValueError("The 'category' arg is invalid!")

        self.path = path
        self.category = category

        self.ps_dict = {}
        self.dat_pattern = re.compile(".*.dat$")
        self.gz_pattern = re.compile(".*(.gz|.gzip)$")
        self.bz2_pattern = re.compile(".*.bz2$")

#         if self.category == "top":
#             do_topfile(path=self.path, category=self.category)
#         elif self.category == "ps":
#             self._traverse_dir()
#         else:
#             pass
#             osw_dict = self._split_by_block(
#                 path=self.path, category=self.category)
#             osw_dict = _split_by_line(osw_dict)
#             osw_dict = _split_by_keypair(osw_dict)
#             self.df = _listofdict_to_df(osw_dict)

#     def __del__(self):
#         pass
# 
#     def _split_by_block(self, path=None, category='meminfo'):
#         """split by block
#         extract 1st level data (timestamp, and raw data of block) to dict
# 
#         Parameters
#         ----------
#         path : string file path, default None
#             path of the osw archive file
# 
#         Returns
#         -------
#         block_dict : list of dict
#             format: 
#             [{'timestamp':timestamp , 'category': category, 'sub_category': sub_category,'key': key,'value': value,'key_value2': key_value2,}
#              {'timestamp':timestamp , 'category': category, 'sub_category': sub_category,'key': key,'value': value,}]
#              
#              more: key_value1,key_value2
#              
#             Icmp:
#                 222595 ICMP messages received
#                 6 input ICMP message failed.
#                 ICMP input histogram:
#                     destination unreachable: 9191
#                     timeout in transit: 7
#                     echo requests: 213378
#                     echo replies: 19
#                     
#              --> category -> netstat
#                  sub_category: Icmp
#                  key_value1: ICMP input histogram:
#                  key_value2: 
#                  key: destination unreachable: 
#                  value: 9191
#             
#         Example of the output:
#             [{  'category': '',
#                 'sub_category': '',
#                 'timestamp': Sat Sep 23 06:00:30 UTC 2017',
#                 'key': 'raw_block',
#                 'value': '\nMemTotal:        1486148 kB\nMemFree:           78376 kB\nBuffers:            2376 kB\n
#                 },
#             {   'category': '',
#                 'sub_category': '',
#                 'timestamp': 'Sat Sep 23 06:01:00 UTC 2017',
#                 'key': 'raw_block',
#                 'value': '\nMemTotal:        1486148 kB\nMemFree:          127332 kB\nBuffers:            2500 kB\n
#                 }
#             ]
#             
#         """
# 
#         with open(path, "r") as f:
#             text = f.read()
# 
#         lst = re.split('zzz', text, flags=re.DOTALL)  # to list based on time
#         lst = [x for x in lst if x]  # remove empty strings
#         """
#         Python 2.x
#         lst = map(lambda v: re.split('(\s\W{1,}\w{3}\s\w{3}\s\w{2,3}\s\d{2}:\d{2}:\d{2}\s\w{3}\s\d{4})', v), lst)
#         """
#         lst = [re.split(
#             '(\s\W{1,}\w{3}\s\w{3}\s\w{2,3}\s\d{2}:\d{2}:\d{2}\s\w{3}\s\d{4})', v) for v in lst]
#         block_dict = []
#         for v in lst:
#             timestamp = v[1]
#             value = v[2]
#             _d = [{'timestamp': timestamp,
#                    'category': category,
#                    'sub_category': '',
#                    'key': 'raw_block',
#                    'value': value}]
#             block_dict.extend(_d)
# 
#         return block_dict
# 
#     def _split_by_line(self, osw_dict={}):
#         """split by line
#         extract 2nd level data to dict as key:value pair
# 
#         Parameters
#         ----------
#         osw_dict : dict, default {}
#             the output from _split_by_block. value is multiline contents from osw data file.
# 
#         Returns
#         -------
#         line_dict : list of dict
#             format: 
#             [{'timestamp':timestamp , 'category': category, 'sub_category': sub_category,'key': key,'value': value,}
#              {'timestamp':timestamp , 'category': category, 'sub_category': sub_category,'key': key,'value': value,}]
#             
#         Example of the output:
#             [{  'category': '', 
#                 'sub_category': '', 
#                 'timestamp': 'Sat Sep 23 06:00:30 UTC 2017',
#                 'key': 'raw_line', 
#                 'value': 'MemTotal:        1486148 kB'
#                 }
#              {'  category': '',          
#                  'sub_category': '', 
#                  'timestamp': 'Sat Sep 23 06:00:30 UTC 2017',
#                  'key': 'raw_line', 
#                  'value': 'MemFree:           78376 kB'
#                  }
#             ]
#             
#         """
#         lst = osw_dict
#         line_dict = []
#         for d in lst:
#             if d['key'] == 'raw_block':
#                 line_lst = re.split(r'\n', d['value'])
#                 for liner in line_lst:
#                     _d = [{'timestamp': d['timestamp'],
#                            'category': d['category'],
#                            'sub_category': d['sub_category'],
#                            'key': 'raw_line',
#                            'value': liner}]
#                     line_dict.extend(_d)
# 
#         return line_dict
# 
#     def _split_by_keypair(self, osw_dict={}):
#         """split by keypair element within a line
#         extract line level data to dict as key:value pair
# 
#         Parameters
#         ----------
#         osw_dict : dict, default {}
#             the output from _split_by_block. value is multiline contents from osw data file.
# 
#         Returns
#         -------
#         keypair_dict : list of dict
#             format: 
#             [{'timestamp':timestamp , 'category': category, 'sub_category': sub_category,'key': key,'value': value,}
#              {'timestamp':timestamp , 'category': category, 'sub_category': sub_category,'key': key,'value': value,}]
#             
#         Example of the output:
#             [{  'category': '', 
#                 'sub_category': '', 
#                 'timestamp': 'Sat Sep 23 06:00:30 UTC 2017',
#                  'key': 'MemTotal_kB', 
#                 'value': '1486148'
#                 }
#              {  'category': '', 
#                  'sub_category': '', 
#                  'timestamp': 'Sat Sep 23 06:00:30 UTC 2017',
#                  'key': 'MemFree_kB', 
#                  'value': '78376'
#                  }
#             ]
#             
#         """
#         lst = osw_dict
#         keypair_dict = []
#         for d in lst:
#             if d['key'] == 'raw_line':
#                 keypair_lst = re.split(r',', d['value'])
# 
#                 for k, v in keypair_lst:
#                     _d = [{'timestamp': d['timestamp'],
#                            'category': d['category'],
#                            'sub_category': d['sub_category'],
#                            'key': k,
#                            'value': v}]
#                     keypair_dict.extend(_d)
# 
#         return keypair_dict

    def _readfile(self, filepath):
        with open(filepath, "r") as f:
            return f.readlines()

    def _pipefile(self, cmd, file):
        p = subprocess.Popen([cmd, file], stdout=subprocess.PIPE)
        return p.stdout.readlines()

    def osw_foreach(self):
        dat_list = os.listdir(self.dir)
        dat_list.sort()
        for f in dat_list:
            if self.gz_pattern.match(f):
                self._pipefile('zcat', f)
            elif self.bz2_pattern.match(f):
                self._pipefile('bzcat', f)
            elif self.dat_pattern.match(f):
                self._readfile(f)

    def _month_to_number_str(self, string):
        #month_dict = dict((v,k) for k,v in enumerate(calendar.month_abbr))
        #month = month_dict[zzz[11:14]]
        m = {
            'jan': '01',
            'feb': '02',
            'mar': '03',
            'apr': '04',
            'may': '05',
            'jun': '06',
            'jul': '07',
            'aug': '08',
            'sep': '09',
            'oct': '10',
            'nov': '11',
            'dec': '12'
            }
        s = string.strip()[:3].lower()
    
        try:
            out = m[s]
            return out
        except:
            raise ValueError('Not a month' + string)
    
    def _convert_zzz_to_timestamp(self, zzz):
        month = self._month_to_number_str(zzz[11:14])
        day = zzz[15:17]
        hms = zzz[18:26]
        year= zzz[-4:]
        result = '-'.join([year, month, day]) + ' ' + hms
        return result
        
    def _analyse_ps_data(self, lines):
        # Ignore several leading lines without zzz
        line_num = -1
        for l in lines:
            line_num += 1
            l = l.strip().lstrip().rstrip()
            if l == "":
                continue
            if 'zzz' in l:
                break
                
        cur_key = ""
        for l in lines[line_num:]:
            l = l.strip().lstrip().rstrip()
            if l == "":
                continue
            if 'PPID' in l:
                continue
            if 'zzz' in l:
                """ Sometimes zzz is not at the beginning """
                zzz_start = l.index('zzz')
                if zzz_start != 0:
                    self.ps_dict[cur_key] += 1
                    
                """ Switch to new zzz """
                cur_key = self._convert_zzz_to_timestamp(l[zzz_start:])
                self.ps_dict[cur_key] = 0
            else:
                self.ps_dict[cur_key] += 1

    def _analyse_data_from_one_file(self, filepath):
        if self.gz_pattern.match(filepath):
            lines = self._pipefile('zcat', filepath)
        elif self.bz2_pattern.match(filepath):
            lines = self._pipefile('bzcat', filepath)
        elif self.dat_pattern.match(filepath):
            lines = self._readfile(filepath)

        if self.category == PS:
            #print("====== " + filepath + " ======")
            self._analyse_ps_data(lines)

    def traverse_dir(self):
        file_list = os.listdir(self.path)
        for file in file_list:
            filepath = os.path.join('%s/%s' % (self.path, file))
            self._analyse_data_from_one_file(filepath)
    
    def get_ps_dict(self):
        return self.ps_dict

def plot_diagram(keys, values, filename):
    trace = go.Scatter(
        x = keys,
        y = values
    )

    data = [trace]
    py.iplot(data, filename=filename)

def main():
    path = 'oswps'
    #path = 'oswps'
    osw = OSWData(path=path, category=PS)
    osw.traverse_dir()
    ps_dict_unsorted = osw.get_ps_dict()
    ps_od = collections.OrderedDict(sorted(ps_dict_unsorted.items()))
    keys, values = zip(*ps_od.items())
    plot_diagram(keys, values, 'ps_count_crond_problems')
    

if __name__ == '__main__':
    main()
