#!/usr/bin/env python3

from datetime import datetime
from datetime import timedelta as dt
import os
import sys
import yaml
import json
import re
import subprocess

class Cases:

    def __init__(self,names=None,path=None,printlev=None,host=None):

        self.path = path if path is not None else 'cases'
        self.printlev = printlev if printlev is not None else 1
        self.host = host if host is not None else self.get_hostname()

        if names is None:
         self.names = []
        if isinstance(names,str):
          self.names = [names]
        else:
          self.names = names

        self.known_keys = { '%Y': 4,  # Year
                            '%m': 2,    # Month
                            '%d': 2,    # Day
                            '%H': 2,    # Hour
                            '%M': 2,    # Minute
                            '%LLL': 3,   # Leadtime
                            '%LLLL': 4,  # Leadtime
                            '*': 0,       # Wildcard
                          }

        self.cases,self.names = self.load_cases()

#########################################################################
    def get_hostname(self):

        import socket

        host = socket.gethostname()
        if re.search(r'^a(a|b|c|d)',host):
            return 'atos'

        return 'atos'
#########################################################################
    def scan(self):
      for case in self.cases:
        self.cases[case].scan()

#########################################################################
    def scanold(self):

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


      for case in self.cases:
          print('Scan:',case)
          findings = {}
          for run,body in self.cases[case].items():
            findings[run] = {}
            path_template = body[self.host]['path_template']
            file_templates = body['file_templates']
    
            print('  Search for {}:{} files named {} in {}'.format(case,run,file_templates,path_template))
    
            i = path_template.find('%')
    
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
      list_keys = ('%Y','%m','%d','%H','%M','%S')
      res = ['0']*6
    
      for j,l in enumerate(list_keys):
          for i,k in enumerate(mk): 
              if l == k :
                 res[j] = z[i]
      dtg = datetime.strptime(':'.join(res),'%Y:%m:%d:%H:%M:%S')
    
      leadtime = None
      for k in mk:
          if k in ('%LLL','%LLLL'):
                   leadtime = dt(hours=z[-1])
    
      return str(dtg),leadtime             
#########################################################################
    def check_template(self,x):

      mapped_keys = {}
      replace_keys = {}
      y=x
      for k,v in self.known_keys.items():
         kk = k
         if '%' in k:
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
        if self.names is not None:
          case_list = intersection(case_list,self.names)
    
        res = {}
        for x in case_list :
            p ='{}/{}/{}'.format(self.path,x,meta)
            m = yaml.safe_load(open(p))
            res[x] = Case(self.host, self.path, self.printlev, m, x)
    
        print("Loaded:",case_list)
        if len(res) > 1 :
          return res,case_list
        else:
          return res[x],case_list

#########################################################################
    def show(self):

      for case,body in self.cases.items():
          print('\nCase:',case)
          print('   ',body)

#########################################################################
    def print(self,printlev=None):

        if printlev is not None:
            self.printlev = printlev
    
        if isinstance(self.cases,dict):
         for name,case in self.cases.items():
          print('\nCase:',name)
          case.print(self.printlev)
        else:
          self.cases.print(self.printlev)

    def reconstruct(self,dtg=None,leadtime=None,file_template=None):
    
        res =[]
        if isinstance(self.cases,dict):
         for name,case in self.cases.items():
          res.extend(case.reconstruct(dtg, leadtime, file_template))
        else:
          res.extend(self.cases.reconstruct(dtg, leadtime, file_template))

        return res
#########################################################################
#class Case(Cases):
class Case():

    def __init__(self, host, path, printlev, props, case):

        self.host,self.path,self.printlev = host,path,printlev
        self.case = case
        self.printlev = printlev

        self.data = self.load()
        self.runs = {}
        if len(props) > 1 :
         for exp,val in props.items():
          if exp in self.data[host]:
           self.runs[exp]= Exp(exp, host, printlev, val, self.data[host][exp])
        else:
         for exp,val in props.items():
          if exp in self.data[host]:
           self.runs= Exp(exp, host, printlev, val, self.data[host][exp])
        self.names= [x for x in props]
          
    def print(self,printlev=None):
        if printlev is not None:
            self.printlev = printlev
        if isinstance(self.runs,dict):
          for run,exp in self.runs.items():
           exp.print(self.printlev)
        else:
           self.runs.print(self.printlev)

    def load(self):
        filename= self.path+'/'+self.case+'/data.yaml'
        with open(filename, "r") as infile:
           data = json.load(infile)
           infile.close()
        return data

    def scan(self):
          findings = {}
          for name,exp in self.runs.items():
            result, signal = exp.scan()
            if signal:
              self.data[self.host][name] = result
            else:
              print("  no data found for",name)
          self.dump() 

    def dump(self):
          filename= self.path+'/'+self.case+'/data.yaml'
          with open(filename,"w") as outfile:
              print('  write to:',filename)
              json.dump(self.data,outfile,indent=1)
              outfile.close()

    def reconstruct(self,dtg=None,leadtime=None,file_template=None):
        res = []
        if isinstance(self.runs,dict):
          for run,exp in self.runs.items():
           res.extend(exp.reconstruct(dtg,leadtime,file_template))
        else:
           res.extend(self.runs.reconstruct(dtg,leadtime,file_template))
        return res
#########################################################################
#class Exp(Case):
class Exp():

    def __init__(self, name, host, printlev, val, data):

        self.name = name
        self.host = host
        self.printlev = printlev
        self.file_templates = val['file_templates']
        self.path_template = val[host]['path_template']
        self.domain = val['domain']
        self.data = data

        self.known_keys = { '%Y': 4,    # Year
                            '%m': 2,    # Month
                            '%d': 2,    # Day
                            '%H': 2,    # Hour
                            '%M': 2,    # Minute
                            '%LLL': 3,  # Leadtime
                            '%LLLL': 4, # Leadtime
                            '*': 0,     # Wildcard
                          }


#########################################################################
    def reconstruct(self,dtg=None,leadtime=None,file_template=None):

        def sub(p,dtgs,leadtime):

           dtg = datetime.strptime(dtgs,'%Y-%m-%d %H:%M:%S')
           if isinstance(leadtime,str):
             l = dt(hours=int(leadtime))
           elif isinstance(leadtime,int):
             l = dt(hours=leadtime)
           else:
             l = leadtime

           path = p

           re_map = { '%Y': '{:04d}'.format(dtg.year),
                   '%m': '{:02d}'.format(dtg.month),
                   '%d': '{:02d}'.format(dtg.day),
                   '%H': '{:02d}'.format(dtg.hour),
                   '%M': '{:02d}'.format(dtg.minute),
                   '%S': '{:02d}'.format(dtg.second),
                   '%LLL': '{:03d}'.format(int(l.seconds/3600)),
                   '%LLLL': '{:04d}'.format(int(l.seconds/3600)),
                 }
           for k,v in re_map.items():
             path = path.replace(k,str(v))
 
           return path

        if file_template is None:
          files = self.file_templates
        else:
          files = [file_template]

        result = []
        for file in files:
         if file in self.data:
          if dtg is None:
             dtgs = self.data[file]
          else:
             dtgs = [dtg]

          for ddd in dtgs:
             if leadtime is None:
               leadtimes = self.data[file][ddd]
             else:
               if isinstance(leadtime,list):
                 leadtimes = leadtime
               else:
                 leadtimes = [leadtime]

          result.extend([sub(self.path_template+file,ddd,l) for l in leadtimes])

        return result

    def print(self, printlev):
        if printlev is not None:
            self.printlev = printlev
        print(' ',self.name)
        print('   File templates:',self.file_templates)
        print('   Path template :',self.path_template)
        print('   Domain:',self.domain)
        for fname in self.file_templates:
           if fname in self.data:
            content = self.data[fname]
            dates = [d for d in content]
            print('   File:',fname)
            print('    Dates:',dates[0],'-',dates[-1])
            if self.printlev > 1:
                   print('    Leadtimes:',content[dates[0]])
            else:
                   print('    Leadtimes:',content[dates[0]][0],'...',content[dates[0]][-1])
            if self.printlev > 1:
                example = self.reconstruct(dates[0],content[dates[0]][-1],fname)
                print('    Example:',example)

    def load(self):
        filename= self.path+'/'+case+'/data.yaml'
        with open(filename, "r") as infile:
           data = json.load(infile)
           infile.close()

#########################################################################
    def scan(self):

      print(" scan:",self.name)
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


      print('  Search for files named {} in {}'.format(self.file_templates,self.path_template))
    
      i = self.path_template.find('%')
    
      base_path = self.path_template[:i] if i > -1 else path_template
      part_path = self.path_template[i:] if i > -1 else ''
    
      if re.match('^ec',base_path):
                subdirs = self.path_template[i:].split('/')
                if subdirs[-1] == '':
                    subdirs.pop()
                x,mk,replace_keys = self.check_template(part_path)
                dirs = subsub(base_path,subdirs,replace_keys)
                content = [d[i:]+cc for d in dirs for cc in ecfs_scan(d)]
      else:
                content = find_files(base_path)
    
      findings = {}
      signal = True
      for file_template in self.file_templates:
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
            
              signal = signal and bool(tmp)
              findings[file_template] = tmp

      return findings,signal
    
    
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

