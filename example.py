#!/usr/bin/env python3

#
# Example on how to access case metadata information and reconstruct file names
#
# Ulf Andrae, SMHI, 2022
#

from cases import Cases

if True:

    # Select cases and experiments, specify as a list or a dictionary with cases and runs within cases
    # Select all
    selection = {}
    selection = []
    selection = None
    # Select one case
    selection = ["finland_2017"]
    # Select one case and one run from this case
    selection = {"finland_2017": ["cy43_deode_ref_fin"]}

    # Load the data
    example = Cases(selection=selection)

    # print, try with different printlevels
    example.print(printlev=1)

    # Specify a selection of dates, leadtimes and file patterns
    dates = []
    dates = ["2017-08-10 12:00:00", "2017-08-11 00:00:00"]
    leadtimes = []
    leadtimes = [0, 1, 2]
    file_template = None
    file_template = "fc(.*)fp"
    file_template = "fc(.*)"

    # Generate the file paths and names
    files = example.reconstruct(
        dtg=dates, leadtime=leadtimes, file_template=file_template
    )

    # Print the result
    print("\nFilenames:")
    [print(" ", f) for f in files]


#
# Example 2
#

if True:
    selection = ["finland_2017", "gavle_2021"]
    example2 = Cases(selection=selection, printlev=0)
    example2.print(printlev=0)

    case = "gavle_2021"
    print("\nCase:", case)

    # Access one case from the cases class
    files = example2.cases[case].reconstruct(leadtime=[0], file_template="(.*)fp")
    print()
    [print(f) for f in files]

    case = "finland_2017"
    dates = ["2017-08-10 12:00:00", "2017-08-11 00:00:00"]

    print("\nCase:", case)
    for run in example2.cases[case].runs:
        # Access one run from the Exp class ( called runs here )
        files = (
            example2.cases[case]
            .runs[run]
            .reconstruct(leadtime=[0], file_template="(.*)fp", dtg=dates)
        )
        print(" Run:", run)
        [print("  ", f) for f in files]

    # Print the metadata for this case and run
    print("\n--- metadata ---")
    [
        print("    {:<20}: {}".format(k, v))
        for k, v in example2.cases[case].runs[run].__dict__.items()
        if k != "data"
    ]
