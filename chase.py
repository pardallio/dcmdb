#!/usr/bin/env python3

#from datetime import datetime
import os
import sys
#import time
import argparse
#import yaml
#import json
#import re
#import subprocess
import cases

#########################################################################
def set_verbosity(a) :
  p = 1
  if a.v != None :
   p += len(a.v)

  if a.s != None :
   p -= len(a.s)
  return p

#########################################################################
def main(argv) :

  parser = argparse.ArgumentParser(description='DE_330 case data base handler at your service')
  parser.add_argument('-c',dest="config_file",help='Config file',required=False,default=None)
  parser.add_argument('-case',dest="case",required=False,default=None,
                      help='Specify name of current host, overrides settings in config file ')
  parser.add_argument('-host',dest="host",help='Set host to check, default is current',required=False,default=None)
  parser.add_argument('-scan',action="store_true",help='Scan case for data',required=False,default=False)
  parser.add_argument('-copy',action="store_true",help='Copy case',required=False,default=False)
  parser.add_argument('-list',action="store_true",help='List content of a case',required=False,default=False)
  parser.add_argument('-show',action="store_true",help='Show available cases',required=False,default=False)
  parser.add_argument('-v', action='append_const', const=int, help='Increase verbosity for list command in particular')
  parser.add_argument('-s', action='append_const', const=int, help='Decrease verbosity for list command in particular')


  if len(argv) == 1 :
     parser.print_help()
     sys.exit(1)

  args = parser.parse_args()
  
  if args.config_file is None :
    config = {}
    config['case_path'] = 'cases'
  else:
    # Read config file
    if not os.path.isfile(args.config_file) :
     print("Could not find config file:",args.config)
     sys.exit(1)
    config = yaml.safe_load(open(args.config_file))

  config['case'] = args.case.split(':') if args.case is not None else []
  config['printlev'] = set_verbosity(args)

  myc = cases.Cases(names=config['case'],printlev=config['printlev'])
 
  if args.scan :
    myc.scan()
  elif args.list :
    myc.print()
  elif args.show :
    myc.show()
  elif args.copy :
    myc.show()

if __name__ == "__main__":
    sys.exit(main(sys.argv))


