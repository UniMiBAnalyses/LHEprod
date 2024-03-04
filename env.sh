#!/bin/bash

# in order to use ccrab client from <https://twiki.cern.ch/twiki/bin/view/CMSPublic/CMSCrabClient>
source /cvmfs/cms.cern.ch/common/crab-setup.sh

cmsenv 

# this is the python with crab in the current arch
py="#!"$(which python3)
submit=$CMSSW_BASE/src/LHEprod/Dumpers/LHEDumper/scripts/submit.py

# add the correct python exec to shebang in submit script


line=$(head -n 1 $submit)

if [[ $line != \#* ]]
then
	sed -i "1 i\\${py}" $submit
fi
