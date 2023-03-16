#!/usr/bin/env python3

import argparse
from cases import Cases
import sys

#########################################################################
def set_verbosity(a) :
  p = 0
  if a.v != None :
   p += len(a.v)

  if a.s != None :
   p -= len(a.s)
  return p

#########################################################################
def main(argv) :

  parser = argparse.ArgumentParser(description='DE_330 Case Meta DataBase handler at your service')
  parser.add_argument('-case',dest="case",required=False,default=None,
                      help='Specify name of case(s) to work with. Use as -case case1[:case2:...:caseN]')
  parser.add_argument('-exp',dest="exp",required=False,default=None,
                      help='Specify name of exp(s) to work with within a case. Use as -exp exp1[:exp2:...:expN]')
  parser.add_argument('-host',dest="host",help='Set host to check, default is current',required=False,default=None)
  parser.add_argument('-list',action="store_true",help='List content of given case(s)',required=False,default=False)
  parser.add_argument('-scan',action="store_true",help='Scan case for data',required=False,default=False)
  parser.add_argument('-toc',action="store_true",help='Create TOC for give filetypes',required=False,default=False)
  parser.add_argument('-path',dest="path",help='Path to directory with cases',required=False,default='cases')
  parser.add_argument('-v', action='append_const', const=int, help='Increase verbosity for list command in particular')
  parser.add_argument('-s', action='append_const', const=int, help='Decrease verbosity for list command in particular')


  # Handle arguments
  if len(argv) == 1 :
     parser.print_help()
     sys.exit(1)

  args = parser.parse_args()
  case = args.case.split(':') if args.case is not None else None

  if args.exp is not None and case is not None:
       if len(case) > 1 :
           print('Only give one case if exp is given')
           sys.exit(1)
       selection={k:args.exp.split(':') for k in case}
  elif case is not None:
       selection={k:[] for k in case}
  else:
       selection=[]

  # Construct the case structure
  myc = Cases(selection= selection,
              printlev= set_verbosity(args),
              path= args.path,
              host= args.host)
 

  # Run the actions
  if args.scan :
    myc.scan()
  elif args.list :
    myc.print()
  elif args.toc :
    myc.toc()

if __name__ == "__main__":
    sys.exit(main(sys.argv))
