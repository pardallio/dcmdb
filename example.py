#!/usr/bin/env python3

# Example on how to access case metadata information

from cases import Cases

# Select one case and one experiment
selection = {}
# or 
selection = []
# or 
selection = None
# or 
selection = [ 'finland_2017' ]
# or 
#selection = { 'finland_2017' : ['cy43_deode_ref_fin'] }

# Load the data 
example = Cases(selection=selection)

# print, try with different printlevels
example.print(printlev=2)

# Specify a selection of dates, leadtimes and file patterns
dates=[]
dates=['2017-08-10 12:00:00',
       '2017-08-11 00:00:00']
leadtimes =[]
leadtimes =[0,1,2]
file_template = None
file_template = 'fc(.*)'

# Generate the file paths and names
files = example.reconstruct(dtg=dates,leadtime=leadtimes)

# Print the result
print()
[print(f) for f in files]

