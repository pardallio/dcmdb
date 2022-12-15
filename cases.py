#!/usr/bin/env python3

from datetime import datetime
import os
import sys
#import time
import yaml
import json
import re
import subprocess

class Case:

    def __init__(self,cases=None,path=None,printlev=None,host=None):

        self.path = path if path is not None else 'cases'
        self.printlev = printlev if printlev is not None else 1
        self.host = host if host is not None else self.get_hostname()

        if cases is None:
          self.cases = []
        if isinstance(cases,str):
          self.cases = [cases]
        else:
          self.cases = cases

        self.known_keys = { '@YYYY@': 4,  # Year
                            '@MM@': 2,    # Month
                            '@DD@': 2,    # Day
                            '@HH@': 2,    # Hour
                            '@mm@': 2,    # Minute
                            '@LLL@': 3,   # Leadtime
                            '@LLLL@': 4,  # Leadtime
                            '*': 0,       # Wildcard
                          }

        self.case_content = self.load_cases()

#########################################################################
    def reconstruct(self,path_template,file_template,dtg,leadtime):

        path = path_template+file_template
        re_map = { '@YYYY@': '{:04d}'.format(dtg.year),
                   '@MM@': '{:02d}'.format(dtg.month),
                   '@DD@': '{:02d}'.format(dtg.day),
                   '@HH@': '{:02d}'.format(dtg.hour),
                   '@mm@': '{:02d}'.format(dtg.minute),
                   '@ss@': '{:02d}'.format(dtg.second),
                   '@LLL@': leadtime,
                   '@LLLL@': leadtime
                 }
        for k,v in re_map.items():
          path = path.replace(k,str(v))

        return path

#########################################################################
    def get_hostname(self):

        import socket

        host = socket.gethostname()
        if re.search(r'^a(a|b|c|d)',host):
            return 'atos'

        return None
#########################################################################
    def scan(self):

      def subsub(path,subdirs,replace_keys):
        
            def pdir(x,replace_keys):
                y = x 
                for k,v in replace_keys.items():
                    if k in y :
                        y = y.replace(k,v)
                return y
        
            result=[]
            content = ecfs_scan(path)
            for cc in content:
              pp = pdir(subdirs[0],replace_keys)
              mm = [m.start() for m in re.finditer(pp,cc)]
              if len(mm) > 0:
                  if len(subdirs) > 1 :
                   subresult = subsub(path+cc,subdirs[1:],replace_keys)
                  else:
                   subresult = [path+cc]
                  result.extend(subresult)
        
            return result


      for case in self.case_content:
          print('Scan:',case)
          findings = {}
          for run,body in self.case_content[case].items():
            findings[run] = {}
            path_template = body[self.host]['path_template']
            file_templates = body['file_templates']
    
            print('  search for {}:{} files named {} in {}'.format(case,run,file_templates,path_template))
    
            i = path_template.find('@')
    
            base_path = path_template[:i] if i > -1 else path_template
            part_path = path_template[i:] if i > -1 else ''
    
            if re.match('^ec',base_path):
                subdirs = path_template[i:].split('/')
                if subdirs[-1] == '':
                    subdirs.pop()
                x,mk,replace_keys = self.check_template(part_path)
                dirs = subsub(base_path,subdirs,replace_keys)
                content = [d[i:]+cc for d in dirs for cc in ecfs_scan(d)]
            else:
                content = find_files(base_path)
    
            for file_template in file_templates:
              tmp = {}
              x,mk,replace_keys = self.check_template(part_path+file_template)
              for cc in content:
                 zz = re.findall(r""+x+'$',cc)
                 if len(zz) > 0:
                  dtg,l = self.set_timestamp(mk,zz[0])
                  if dtg not in tmp:
                      tmp[dtg] = []
                  tmp[dtg].append(l)
                    
              for k in tmp:
                 tmp[k].sort()
            
              findings[run][file_template] = tmp
    
          final = {}
          final[self.host] = {}
          final[self.host]['path_template'] = path_template
          for run in findings:
            final[self.host][run] = findings[run]
    
          filename= self.path+'/'+case+'/data.yaml'
          with open(filename,"w") as outfile:
              print('  write to:',filename)
              json.dump(final,outfile,indent=1)
              outfile.close()

#########################################################################
    def set_timestamp(self,mk,z):
      list_keys = ('@YYYY@','@MM@','@DD@','@HH@','@mm@','@ss@')
      res = ['0']*6
    
      for j,l in enumerate(list_keys):
          for i,k in enumerate(mk): 
              if l == k :
                 res[j] = z[i]
      dtg = datetime.strptime(':'.join(res),'%Y:%m:%d:%H:%M:%S')
    
      leadtime = None
      for k in mk:
          if k in ('@LLL@','@LLLL@'):
                   leadtime = z[-1]
    
      return str(dtg),leadtime             
#########################################################################
    def check_template(self,x):

      mapped_keys = {}
      replace_keys = {}
      y=x
      for k,v in self.known_keys.items():
         kk = k
         if '@' in k:
           s=f'(\\d{{{v}}})'
         if '*' in k:
           s=f'(.*)'
           kk='\*'
         if '+' in k:
           s=f'\+'
         
         mm = [m.start() for m in re.finditer(kk,x)]
         y = y.replace(k,s)
         if len(mm) > 0:
             mapped_keys[k]= mm[0]
             replace_keys[k]= s
    
      mk = dict(sorted(mapped_keys.items(), key=lambda item: item[1]))
    
      y = y.replace('+','\+')
    
      return y,mk,replace_keys

#########################################################################
    def load_cases(self):

        def intersection(lst1, lst2):
            lst3 = [value for value in lst1 if value in lst2]
            return lst3
    
        meta = 'meta.yaml'
        case_list = [x.split('/')[0] for x in find_files(self.path,meta)]
        if len(self.cases) != 0 :
          case_list = intersection(case_list,self.cases)
    
        res = {}
        for x in case_list :
            p ='{}/{}/{}'.format(self.path,x,meta)
            m = yaml.safe_load(open(p))
            res[x] = m
    
        print("Loaded:",case_list)
        return res

#########################################################################
    def show(self):

      for case,body in self.case_content.items():
          print('\nCase:',case)
          print('   ',body)

#########################################################################
    def print(self,printlev=None):

        if printlev is None:
            printlev=self.printlev
    
        for case in self.case_content:
          print('\nCase:',case)
          filename= self.path+'/'+case+'/data.yaml'
          with open(filename, "r") as infile:
           data = json.load(infile)
           infile.close()
          for key,val in data.items():
             print(' Host:',key)
             print(' Path:',val['path_template'])
             path_template = val.pop('path_template', None)  
             if printlev > 0:
              for exp,body in val.items():
               print('   Exp:',exp)
               for fname,content in body.items():
                #dates = [datetime.strptime(d,'%Y-%m-%d %H:%M:%S') for d in content]
                dates = [d for d in content]
                print('   File:',fname)
                print('    Dates:',dates[0],'-',dates[-1])
                if printlev > 1:
                   print('    Leadtimes:',content[dates[0]])
                else:
                   print('    Leadtimes:',content[dates[0]][0],'...',content[dates[0]][-1])
                if printlev > 2:
                  dtg=datetime.strptime(dates[0],'%Y-%m-%d %H:%M:%S')
                  example = self.reconstruct(path_template,fname,dtg,content[dates[0]][-1])
                  print('    Example:',example)

#########################################################################
def ecfs_scan(path):

 glcmd = ['els',path]
 cmd = subprocess.Popen(glcmd, stdout=subprocess.PIPE)
 cmd_out, cmd_err = cmd.communicate()

 # Decode and filter output
 res = [line.decode("utf-8") for line in cmd_out.splitlines()]
 return res

#########################################################################

def find_files(path,prefix=''):

  # Scan given path and subdirs and return files matching the pattern
  result = []
  try :
    it = os.scandir(path)
  except :
    it = []
  for entry in it:
      if not entry.name.startswith('.') and entry.is_file():
          if re.search(prefix,entry.name) :
            result.append(entry.name)
      if not entry.name.startswith('.') and entry.is_dir():
          subresult = find_files(os.path.join(path,entry.name),prefix)
          subresult = [entry.name + "/" + e for e in subresult]
          result.extend(subresult)
  return result

