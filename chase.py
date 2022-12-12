#!/usr/bin/env python3

import datetime
import os
import sys
import time
import argparse
import yaml
import json
import signal
import re
import subprocess


def get_hostname():

    import socket

    host = socket.gethostname()
    if re.search(r'^a(a|b|c|d)',host):
        return 'atos'

    return None

#########################################################################


#########################################################################

def load_cases(cp,case):

    def intersection(lst1, lst2):
        lst3 = [value for value in lst1 if value in lst2]
        return lst3

    meta = 'meta.yaml'
    case_list = [x.split('/')[0] for x in find_files(cp,meta)]
    if len(case) != 0 :
      case_list = intersection(case_list,case)

    res = {}
    for x in case_list :
        p ='{}/{}/{}'.format(cp,x,meta)
        m = yaml.safe_load(open(p))
        res[x] = m

    print("Loaded:",case_list)
    return res

#########################################################################

def digest_template(cfg,case_content):

  known_keys = { '@YYYY@': 4, 
                 '@DD@': 2, 
                 '@HH@': 2, 
                 '@MM@': 2, 
                 '@mm@': 2, 
                 '@LLL@': 3, 
                 '@LLLL@': 4, 
                 '@LLL:mm@': '3:2', 
                 '@LLLL:mm@': '4:2', 
                 #'@mbr@': 3,
                 '*': 0,
               }

  for case in case_content:
      print('Scan:',case)
      findings = {}
      for run,body in case_content[case].items():
        findings[run] = {}
        path_template = body[cfg['host']]['path_template']
        file_templates = body['file_templates']

        print('  search for {}:{} files named {} in {}'.format(case,run,file_templates,path_template))

        i = path_template.find('@')

        base_path = path_template[:i] if i > -1 else path_template
        part_path = path_template[i:] if i > -1 else ''

        if re.match('^ec',base_path):
            subdirs = path_template[i:].split('/')
            if subdirs[-1] == '':
                subdirs.pop()
            x,mk,replace_keys = check_template(part_path,known_keys)
            dirs = subsub(base_path,subdirs,replace_keys)
            content = [d[i:]+cc for d in dirs for cc in ecfs_scan(d)]
        else:
            content = find_files(base_path)

        for file_template in file_templates:
          tmp = {}
          x,mk,replace_keys = check_template(part_path+file_template,known_keys)
          for cc in content:
             zz = re.findall(r""+x+'$',cc)
             if len(zz) > 0:
              ttt = set_timestamp(mk,zz[0])
              for k,v in ttt.items():
               if k not in tmp:
                    tmp[k] = []
               tmp[k].extend(v)
                
          for k in tmp:
             tmp[k].sort()
        
          findings[run][file_template] = tmp

      final = {}
      final[cfg['host']] = {}
      final[cfg['host']]['path_template'] = path_template
      for run in findings:
        final[cfg['host']][run] = findings[run]

      filename= cfg['case_path']+'/'+case+'/data.yaml'
      with open(filename,"w") as outfile:
          print('  write to:',filename)
          json.dump(final,outfile,indent=1)
          outfile.close()

#########################################################################

def set_timestamp(mk,z):
  list_keys = ('@YYYY@','@MM@','@DD@','@HH@','@mm@')

  tmp = {}

  for j,l in enumerate(list_keys):
           for i,k in enumerate(mk): 
              if j == 0 and l == k :
                 timestamp = z[i]
              if j == 1 and l == k :
                 timestamp += z[i]
              if j == 2 and l == k :
                 timestamp += z[i]
              if j == 3 and l == k :
                 timestamp += 'T'+z[i]
  if timestamp not in tmp :
            tmp[timestamp] = []
  for i,k in enumerate(mk):
             if k in ('@LLL@','@LLLL@'):
               tmp[timestamp].append(z[i])

  return tmp             
                
def check_template(x,known_keys):

  mapped_keys = {}
  replace_keys = {}
  y=x
  for k,v in known_keys.items():
     kk = k
     if '@' in k:
       s=f'(\\d{{{v}}})'
     if '*' in k:
       s=f'(.*)'
       kk='\*'
     if '+' in k:
       s=f'\+'
     
     mm = [m.start() for m in re.finditer(kk,x)]
     if len(mm) > 1 :
       print("Only provide one match for "+k)
       sys.exit(1)
     y = y.replace(k,s)
     if len(mm) > 0 :
       mapped_keys[k] = mm[0]
       replace_keys[k] = s

  mk = dict(sorted(mapped_keys.items(), key=lambda item: item[1]))

  y = y.replace('+','\+')

  return y,mk,replace_keys


#########################################################################

def pdir(x,replace_keys):

     y = x 
     for k,v in replace_keys.items():
         if k in y :
            y = y.replace(k,v)
     
     return y

#########################################################################

def subsub(path,subdirs,replace_keys):

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


def find_config_files(path,prefix):

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

def reconstruct(path_template,file_template,date,leadtime):


    path = path_template+file_template
    YYYY,MM,DD,HH = re.findall(r'(\d{4})(\d{2})(\d{2})T(\d{2})',date)[0]
    re_map = { '@YYYY@': YYYY,
               '@MM@': MM,
               '@DD@': DD,
               '@HH@': HH,
               '@LLL@': leadtime,
               '@LLLL@': leadtime}
    for k,v in re_map.items():
      path = path.replace(k,v)

    return path

def list_case(cfg,case_content):

  for case in case_content:
      print('\nCase:',case)
      filename= cfg['case_path']+'/'+case+'/data.yaml'
      with open(filename, "r") as infile:
       data = json.load(infile)
       infile.close()
      for key,val in data.items():
         print(' Host:',key)
         print(' Path:',val['path_template'])
         path_template = val.pop('path_template', None)  
         for exp,body in val.items():
           print('   Exp:',exp)
           for fname,content in body.items():
            dates = [d for d in content]
            print('    Dates:',dates[0],'-',dates[-1])
            if len(content[dates[0]]) > 5:
               print('    Leadtimes:',content[dates[0]][0],'...',content[dates[0]][-1])
            else:
               print('    Leadtimes:',content[dates[0]])
            example = reconstruct(path_template,fname,dates[0],content[dates[0]][-1])
            print('    Example:',example)
            cmd='ls -l'
            if re.match('^ec',example):
                cmd = 'els -l'
            cmd += ' '+example
            print(cmd)
            os.system(cmd)

def show_case(cfg,case_content):

  for case,body in case_content.items():
      print('\nCase:',case)
      print('   ',body)

def main(argv) :

  parser = argparse.ArgumentParser(description='DE_330 case data base handler at your service')
  parser.add_argument('-c',dest="config_file",help='Config file',required=False,default=None)
  parser.add_argument('-case',dest="case",required=False,default=None,
                      help='Specify name of current host, overrides settings in config file ')
  parser.add_argument('-host',dest="host",help='Set host to check, default is current',required=False,default=None)
  parser.add_argument('-scan',action="store_true",help='Scan case for data',required=False,default=False)
  parser.add_argument('-list',action="store_true",help='List content of a case',required=False,default=False)
  parser.add_argument('-show',action="store_true",help='Show available cases',required=False,default=False)
  #parser.add_argument('-v', action='append_const', const=int, help='Increase verbosity')
  #parser.add_argument('-s', action='append_const', const=int, help='Decrease verbosity')


  if len(argv) == 1 :
     parser.print_help()
     sys.exit(1)

  args = parser.parse_args()

  if args.config_file is None :
    config = {}
    config['case_path'] = 'cases'
    config['host'] = get_hostname()
  else:
    # Read config file
    if not os.path.isfile(args.config_file) :
     print("Could not find config file:",args.config)
     sys.exit(1)
    config = yaml.safe_load(open(args.config_file))

  config['case'] = args.case.split(':') if args.case is not None else []
 
  case_content = load_cases(config['case_path'],config['case'])

  if args.scan :
    digest_template(config,case_content)
  elif args.list :
    list_case(config,case_content)
  elif args.show :
    show_case(config,case_content)

if __name__ == "__main__":
    sys.exit(main(sys.argv))


