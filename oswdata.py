#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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

    def __init__(self,path=None,category=None):
        """OSWData class, init"""      
        if path is None:
            raise ValueError("The 'path' arg is invalid!")
            
        if category is None:
            raise ValueError("The 'category' arg is invalid!")
           
        self.path = path
        self.category = category        
        if self.category == "top":
            do_topfile(path=self.path,category=self.category)
        else:
            osw_dict = _split_by_block(path=self.path,category=self.category)
            osw_dict = _split_by_line(osw_dict)
            osw_dict = _split_by_keypair(osw_dict)
            self.df = _listofdict_to_df(osw_dict)
 
    def __del__(self):
        pass
 
     
    def _split_by_block(self, path=None,category='meminfo'):
        """split by block
        extract 1st level data (timestamp, and raw data of block) to dict

        Parameters
        ----------
        path : string file path, default None
            path of the osw archive file

        Returns
        -------
        block_dict : list of dict
            format: 
            [{'timestamp':timestamp , 'category': category, 'sub_category': sub_category,'key': key,'value': value,'key_value2': key_value2,}
             {'timestamp':timestamp , 'category': category, 'sub_category': sub_category,'key': key,'value': value,}]
             
             more: key_value1,key_value2
             
            Icmp:
                222595 ICMP messages received
                6 input ICMP message failed.
                ICMP input histogram:
                    destination unreachable: 9191
                    timeout in transit: 7
                    echo requests: 213378
                    echo replies: 19
                    
             --> category -> netstat
                 sub_category: Icmp
                 key_value1: ICMP input histogram:
                 key_value2: 
                 key: destination unreachable: 
                 value: 9191
            
        Example of the output:
            [{  'category': '',
                'sub_category': '',
                'timestamp': Sat Sep 23 06:00:30 UTC 2017',
                'key': 'raw_block',
                'value': '\nMemTotal:        1486148 kB\nMemFree:           78376 kB\nBuffers:            2376 kB\n
                },
            {   'category': '',
                'sub_category': '',
                'timestamp': 'Sat Sep 23 06:01:00 UTC 2017',
                'key': 'raw_block',
                'value': '\nMemTotal:        1486148 kB\nMemFree:          127332 kB\nBuffers:            2500 kB\n
                }
            ]
            
        """
        
        with open(path, "r") as f:     
            text = f.read()
            
        lst = re.split('zzz', text, flags=re.DOTALL) # to list based on time
        lst = [x for x in lst if x] # remove empty strings
        """
        Python 2.x
        lst = map(lambda v: re.split('(\s\W{1,}\w{3}\s\w{3}\s\w{2,3}\s\d{2}:\d{2}:\d{2}\s\w{3}\s\d{4})', v), lst)
        """
        lst = [re.split('(\s\W{1,}\w{3}\s\w{3}\s\w{2,3}\s\d{2}:\d{2}:\d{2}\s\w{3}\s\d{4})', v) for v in lst]
        block_dict = []
        for v in lst:
            timestamp=v[1]
            value=v[2]
            _d = [{'timestamp':timestamp, 
                   'category': category, 
                   'sub_category': '',
                   'key': 'raw_block',
                   'value': value}]
            block_dict.extend(_d)
            
        return block_dict
        

    def _split_by_line(self, osw_dict={}):
        """split by line
        extract 2nd level data to dict as key:value pair

        Parameters
        ----------
        osw_dict : dict, default {}
            the output from _split_by_block. value is multiline contents from osw data file.

        Returns
        -------
        line_dict : list of dict
            format: 
            [{'timestamp':timestamp , 'category': category, 'sub_category': sub_category,'key': key,'value': value,}
             {'timestamp':timestamp , 'category': category, 'sub_category': sub_category,'key': key,'value': value,}]
            
        Example of the output:
            [{  'category': '', 
                'sub_category': '', 
                'timestamp': 'Sat Sep 23 06:00:30 UTC 2017',
                'key': 'raw_line', 
                'value': 'MemTotal:        1486148 kB'
                }
             {'  category': '',          
                 'sub_category': '', 
                 'timestamp': 'Sat Sep 23 06:00:30 UTC 2017',
                 'key': 'raw_line', 
                 'value': 'MemFree:           78376 kB'
                 }
            ]
            
        """
        lst = osw_dict
        line_dict = []
        for d in lst:
            if d['key'] == 'raw_block':
                line_lst = re.split(r'\n',d['value'])
                for liner in line_lst:
                    _d = [{'timestamp':d['timestamp'] , 
                           'category': d['category'], 
                           'sub_category': d['sub_category'], 
                           'key': 'raw_line', 
                           'value': liner}]
                    line_dict.extend(_d)
                    
        return line_dict

    def _split_by_keypair(self, osw_dict={}):
        """split by keypair element within a line
        extract line level data to dict as key:value pair

        Parameters
        ----------
        osw_dict : dict, default {}
            the output from _split_by_block. value is multiline contents from osw data file.

        Returns
        -------
        keypair_dict : list of dict
            format: 
            [{'timestamp':timestamp , 'category': category, 'sub_category': sub_category,'key': key,'value': value,}
             {'timestamp':timestamp , 'category': category, 'sub_category': sub_category,'key': key,'value': value,}]
            
        Example of the output:
            [{  'category': '', 
                'sub_category': '', 
                'timestamp': 'Sat Sep 23 06:00:30 UTC 2017',
                 'key': 'MemTotal_kB', 
                'value': '1486148'
                }
             {  'category': '', 
                 'sub_category': '', 
                 'timestamp': 'Sat Sep 23 06:00:30 UTC 2017',
                 'key': 'MemFree_kB', 
                 'value': '78376'
                 }
            ]
            
        """    
        lst = osw_dict
        keypair_dict = []
        for d in lst:
            if d['key'] == 'raw_line':
                keypair_lst = re.split(r',',d['value'])
                
                for k,v in keypair_lst:
                    _d = [{'timestamp':d['timestamp'] , 
                           'category': d['category'], 
                           'sub_category': d['sub_category'], 
                           'key': k, 
                           'value': v}]
                    keypair_dict.extend(_d)
                    
        return keypair_dict


    def _listofdict_to_df(self, osw_dict=None): 
        """Read dict into a DataFrame.    

        Parameters
        ----------
        osw_dict : dict, default None
            the output from _split_by_keypair.

        Returns
        -------
        frame : DataFrame
        """ 
        if type(osw_dict) is not dict:
            raise ValueError("The 'osw_dict' arg is invalid!")
            
        frame = pd.DataFrame.from_dict(osw_dict, orient='columns')
        
        return frame
        
    def string_to_keypair(self, data):
        """Read dict into a DataFrame.    

        Parameters
        ----------
        data : string
            e.g. 'Quick ack mode was activated 39377 time'
                'TCPHystartDelayDetect: 157'
        Returns
        -------
        keypair_lst : list
            [[k,v],[k,v],[k,v],[k,v],[k,v]]
            e.g. 
            [[Quick ack mode was activated time,39377],[TCPHystartDelayDetect,157]]
        """        
        return keypair_lst


    
    def oswmem_free_memory(self,min=0): 
        """analyze oswmem free memory 
        check if free mmemory <= min memory

        Parameters
        ----------
        min : num, default 0
            free memeory

        Returns
        -------
        result : boolean. False means free mmemory <= min memory.
        """ 
        result = self.df[self.df['free mmemory'] > min].all        
        return result

    def do_topfile(self, path=None,category='top'):
        """Output oswtop file as a Data Frame

        Parameters
        ----------
        path : string file path, default None
            path of the oswtop file

        Returns
        -------
        data: DataFrame
        """
        s = StringIO()
        with open(self.path) as f:
            for line in f:
                if line.startswith('top') or line.startswith('Cpu') or \
                   line.startswith('Tasks') or line.startswith('Mem') or \
                   line.startswith('Swap'):
                    s.write(line)
        s.seek(0)
        oswdata=pd.read_csv(file, comment="L", sep="\n", names='a')
        raw=oswdata[oswdata.iloc[:,0].str.startswith("top")].dropna(axis=1)
        raw['a']=raw['a'].str.replace('days, ','days ')
        raw['a']=raw['a'].str.replace('top - ','')
        raw['a']=raw['a'].str.replace(' up ',',')
        raw['a']=raw['a'].str.replace('users','')
        raw['a']=raw['a'].str.replace('load average: ','')
        top=raw['a'].str.split(',', 5, expand=True).rename(columns={0:'ts', 1:'uptime', 2:'users', 3:'load1', 4:'load10', 5:'load15'})
        top=top.reset_index().rename(columns={'index': 'pos'})
        data=top.copy()
        #print top
    
        raw=oswdata[oswdata.iloc[:,0].str.startswith("Tasks")].dropna(axis=1)
        raw['a']=raw['a'].str.replace('Tasks:','')
        raw['a']=raw['a'].str.replace(' total,',',')
        raw['a']=raw['a'].str.replace(' running,',',')
        raw['a']=raw['a'].str.replace(' sleeping,',',')
        raw['a']=raw['a'].str.replace(' stopped,',',')
        raw['a']=raw['a'].str.replace(' zombie','')
        task=raw['a'].str.split(',', 4, expand=True).rename(columns={0:'tot', 1:'run', 2:'sleep', 3:'stop', 4:'zom'})
        task=task.reset_index().rename(columns={'index': 'pos'})
        task['pos']=task['pos'].apply(lambda d: int(d)-1)
        data=pd.merge(data,task,how='outer',on='pos')
        #print task
    
        raw=oswdata[oswdata.iloc[:,0].str.startswith("Cpu")].dropna(axis=1)
        raw['a']=raw['a'].str.replace('Cpu\(s\):','')
        raw['a']=raw['a'].str.replace('us,',',')
        raw['a']=raw['a'].str.replace('sy,',',')
        raw['a']=raw['a'].str.replace('ni,',',')
        raw['a']=raw['a'].str.replace('id,',',')
        raw['a']=raw['a'].str.replace('wa,',',')
        raw['a']=raw['a'].str.replace('hi,',',')
        raw['a']=raw['a'].str.replace('si,',',')
        raw['a']=raw['a'].str.replace('st','')
        cpu=raw['a'].str.split(',', 7, expand=True).rename(columns={0:'us', 1:'sy', 2:'ni', 3:'id', 4:'wa', 5:'hi', 6:'si', 7:'st'})
        cpu=cpu.reset_index().rename(columns={'index': 'pos'})
        cpu['pos']=cpu['pos'].apply(lambda d: int(d)-2)
        data=pd.merge(data,cpu,how='outer',on='pos')
        #print cpu
    
    
        raw=oswdata[oswdata.iloc[:,0].str.startswith("Mem:")].dropna(axis=1)
        raw['a']=raw['a'].str.replace('Mem:','')
        raw['a']=raw['a'].str.replace('k total,',',')
        raw['a']=raw['a'].str.replace('k used,',',')
        raw['a']=raw['a'].str.replace('k free,',',')
        raw['a']=raw['a'].str.replace('k buffers','')
        mem=raw['a'].str.split(',', 3, expand=True).rename(columns={0:'Memtot', 1:'Memused', 2:'Memfree', 3:'Membuf'})
        mem=mem.reset_index().rename(columns={'index': 'pos'})
        mem['pos']=mem['pos'].apply(lambda d: int(d)-3)
        data=pd.merge(data,mem,how='outer',on='pos')
        #print mem
    
        raw=oswdata[oswdata.iloc[:,0].str.startswith("Swap:")].dropna(axis=1)
        raw['a']=raw['a'].str.replace('Swap:','')
        raw['a']=raw['a'].str.replace('k total,',',')
        raw['a']=raw['a'].str.replace('k used,',',')
        raw['a']=raw['a'].str.replace('k free,',',')
        raw['a']=raw['a'].str.replace('k cached','')
        swap=raw['a'].str.split(',', 3, expand=True).rename(columns={0:'Swaptot', 1:'Swapused', 2:'Swapfree', 3:'cache'})
        swap=swap.reset_index().rename(columns={'index': 'pos'})
        swap['pos']=swap['pos'].apply(lambda d: int(d)-4)
        #print swap
        data=pd.merge(data,swap,how='outer',on='pos')
        print(data)
        return data

 
#
# ----------------------------------------------------------------------
# Add plotting methods to OSWData

 
