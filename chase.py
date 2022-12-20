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
  parser.add_argument('-case',dest="case",required=False,default=None,
                      help='Specify name of case(s) to work with. Use as -case case1[:case2:...:caseN ')
  parser.add_argument('-exp',dest="exp",required=False,default=None,
                      help='Specify name of exp(s) to work with within a case. Use as -exp exp1[:exp2:...:expN ')
  parser.add_argument('-host',dest="host",help='Set host to check, default is current',required=False,default=None)
  parser.add_argument('-scan',action="store_true",help='Scan case for data',required=False,default=False)
  parser.add_argument('-copy',action="store_true",help='Copy case',required=False,default=False)
  parser.add_argument('-list',action="store_true",help='List content of given case(s)',required=False,default=False)
  parser.add_argument('-show',action="store_true",help='Show available cases',required=False,default=False)
  parser.add_argument('-v', action='append_const', const=int, help='Increase verbosity for list command in particular')
  parser.add_argument('-s', action='append_const', const=int, help='Decrease verbosity for list command in particular')


  if len(argv) == 1 :
     parser.print_help()
     sys.exit(1)

  args = parser.parse_args()
  
  config = {}
  config['case_path'] = 'cases'
  config['case'] = args.case.split(':') if args.case is not None else None
  config['exp']  = args.exp.split(':') if args.exp is not None else None

  if args.exp is not None:
       if len(config['case']) > 1 :
           print('Only give one case if exp is given')
           sys.exit(1)
       selection={k:config['exp'] for k in config['case']}
  elif config['case'] is not None:
       selection={k:[] for k in config['case']}
  else:
       selection={}

  
  config['printlev'] = set_verbosity(args)

  myc = cases.Cases(selection=selection,printlev=config['printlev'])
 
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


