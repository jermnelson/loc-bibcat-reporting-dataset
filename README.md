# Library of Congress BIBCAT Reporting Module Test Dataset
This command-line utility takes a phrase, searches the Library
of Congress catalog using Z39.50, extracts MARC XML records,
transforms the record to BIBFRAME graph using 
[marc2bibframe](https://github.com/lcnetdev/marc2bibframe) with the
[BIBFRAME Socket](https://github.com/jermnelson/bibframe-socket) server
running on port 8089.

## Installing

1.  Clone this repository with git:
    `git clone https://github.com/jermnelson/loc-bibcat-reporting-dataset.git`

1.  Clone the BIBFRAME Socket with git:
    `git clone https://github.com/jermnelson/bibframe-socket`

1.  Initalize and update submodules in both projects
    `cd loc-bibcat-reporting-dataset`
    `git submodule init`
    `git submodule update`
    `cd lib/bibframe_datastore`
    `git submodule init`
    `git submodule update`
    `cd ../../../bibframe-socket`
    `git submodule init`
    `git submodule update` 


## Running
To run this ingester, you'll first need to start BIBFRAME Socket using 
[jython](http://www.jython.org/) in its own terminal window:

    `jython server.py 0.0.0.0 8089`

After the BIBFRAME socket server is running, you can then run the ingester
from the command:

    `python ingester.py load {term}`
    
Running the command with *sample* will load the sample data set using the
terms "Mark Twain" and "bible":

    `python ingester load sample`
