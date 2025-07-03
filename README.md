# Generate ROOT Ntuples of LHE events starting from a gridpack

Suppose you have a gridpack. You would like to plot some physical observable to check that the generation is reliable before proceeding for full production. 
However you quickly find that LHE as XML files are not really easy to understand and to transform into the more user-friendly ROOT NTuples.

This little cmssw-compatible framework helps in generating LHE events starting from a gridpack and producing nanoAOD-like root NTuples.

To run the generation you first need to have a gridpack. You'll also need to have acces to a machine that supports the CMS software environment CMSSW (such as lxplus or others).

```
# cmsrel <release_name>
cmsrel CMSSW_13_3_1; cd  CMSSW_13_3_1/src; cmsenv         # get cmssw version and activate env
git clone git@github.com:UniMiBAnalyses/LHEprod.git                       # clone this repo     
source env.sh                                                             # crab and script setup
scram b -j 8                                                              # compile the plugins
```

Once the environment is ready you can geenrate events starting from the gridpack. The output will be a .root file
For generating events locally one can simply run:
```
cd Dumpers/LHEDumper
cmsRun LHEDumperRunner.py input=<PATH_TO_GRIDPACK(default=gridpack.tar.xz)> \
                          nevents=<NUMBER_OF_EVENTS(default=10)> \
                          seed=<STARTING_SEED(default=10)>
                          jobNum=<JOB_NUMBER>
```

A script is also provided to submit the generation in batch mode and on crab:
```
usage: submit.py [-h] -gp GRIDPACK -o OUTPUT [-ne NEVENTS] [-nj NJOBS] [-nt NTHREADS] [-t TIER] [-q QUEUE] [--prefix LHE_PREFIX] [--conf CONF] [--crabconf CRABCONF] [--datasetname DATASETNAME]
                 [--requestname REQUESTNAME] [--datasettag DATASETTAG] [--maxmemory MAXMEMORY]

optional arguments:
  -h, --help            show this help message and exit
  -gp GRIDPACK, --gridpack GRIDPACK
                        Path to the gridpack you want to generate events with. [REQUIRED]
  -o OUTPUT, --output OUTPUT
                        Output folder where .root files will be stored. If using crab something like /store/user/<username>/... [REQUIRED]
  -ne NEVENTS, --nevents NEVENTS
                        Number of events per job requested (def=1000)
  -nj NJOBS, --njobs NJOBS
                        Number of jobs requested (def=1)
  -nt NTHREADS, --nthreads NTHREADS
                        Number of threads x job (def=1)
  -t TIER, --tier TIER  Tier for production. can be [afs, eos, crab]. Afs will submit jobs with HTcondor and save root files on afs while eos option will xrdcp to eos. crab instead will save on the
                        specified tier in the crab config file
  -q QUEUE, --queue QUEUE
                        Condor queue (def=longlunch)
  --prefix LHE_PREFIX, --prefix LHE_PREFIX
                        The prefix of the output LHE files, by default written in conf.json
  --conf CONF           Load configuration file (default=configuration/conf.json)
  --crabconf CRABCONF   Crab config json file (default=configuration/crabconf.json)
  --datasetname DATASETNAME
                        Crab dataset name under config.Data.outputPrimaryDataset. Can also specify in crabconfig
  --requestname REQUESTNAME
                        Name of the crab request under config.General.requestName
  --datasettag DATASETTAG
                        Name of the crab dataset tag under config.Data.outputDatasetTag
  --maxmemory MAXMEMORY
                        Max memory of the crab request under config.JobType.maxMemoryMB
```

Some real life examples

```
# crab submission
submit.py -gp /eos/user/g/gboldrin/gridpacks/zee/zee_slc7_amd64_gcc700_CMSSW_10_6_19_tarball.tar.xz -t crab -o /store/user/gboldrin/PrivateMC/RunIISummer20UL18NanoAODv9_nanoLHE/

# according to the current files in the configuration folder, the root files will be saved in
# <args.output>/<crabconf.outputPrimaryDataset>/<crabconf.outputDatasetTag>/<date_time>/<run_number>/nanoAOD_LHE_<jobNumber>.root
# /store/user/gboldrin/PrivateMC/RunIISummer20UL18NanoAODv9_nanoLHE/nAOD_LHE/Nanov12LHEOnly/240215_164509/0000/nanoAOD_LHE_1.root

# afs submission on condor with afs output folder
submit.py -gp /eos/user/g/gboldrin/gridpacks/zee/zee_slc7_amd64_gcc700_CMSSW_10_6_19_tarball.tar.xz -t afs -o $PWD/rootfiles

# afs submission on condor with eos output folder
submit.py -gp /eos/user/g/gboldrin/gridpacks/zee/zee_slc7_amd64_gcc700_CMSSW_10_6_19_tarball.tar.xz -t eos -o /eos/user/g/gboldrin/delete/
```


The structure of the output root file is the same as what can be seen in central nanoAOD files. In the ``Èvents``` TTree of the file one can see the 
following branches, comprising both initial/intermediate/final state kinematics but also theory uncertainties for renormalization and factorization scales, 
PDF and the reweighting weights if any.

```
['run', 'luminosityBlock', 'event', 'bunchCrossing', 'LHE_HT', 'LHE_HTIncoming', 'LHE_Vpt', 'LHE_AlphaS',
'LHE_Njets', 'LHE_Nb', 'LHE_Nc', 'LHE_Nuds', 'LHE_Nglu', 'LHE_NpNLO', 'LHE_NpLO', 'nLHEPart', 'LHEPart_pt',
'LHEPart_eta', 'LHEPart_phi', 'LHEPart_mass', 'LHEPart_incomingpz', 'LHEPart_pdgId', 'LHEPart_status',
'LHEPart_spin', 'LHEWeight_originalXWGTUP', 'nLHEPdfWeight', 'LHEPdfWeight', 'nLHEReweightingWeight',
'LHEReweightingWeight', 'nLHEScaleWeight', 'LHEScaleWeight']
```

# Problems

For some gridpacks it can be necessary to change the LHE code plugin by substituting [this line](https://github.com/UniMiBAnalyses/LHEprod/blob/bb547d94a2c933d10548b80bf77ad8dc48d0c099/LHEDumper/plugins/LHEWeightsTableProducer.cc#L427) with 

```
std::regex weightgroupRwgt("<weightgroup\\s+(?:name)=\"(.*)\"\\s+(?:weight_name_strategy)=\"(.*)\"\\s*>");
```
