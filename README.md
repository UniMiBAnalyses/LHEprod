# Generate ROOT Ntuples starting from a gridpack

Suppose you have a gridpack. You would like to plot some physical observable to check that the generation is reliable before proceeding for full production. 
However you quickly find that LHE as XML files are not really easy to understand and to transform into the more user-friendly ROOT NTuples.

This little cmssw-compatible framework helps in generating LHE events starting from a gridpack and producing nanoAOD-like root NTuples.

To run the generation you first need to have a gridpack. You'll also need to have acces to a machine that supports the CMS software environment CMSSW (such as lxplus or others).

```
# cmsrel <release_name>
cmsrel CMSSW_12_4_11_patch3; cd  CMSSW_12_4_11_patch3/src; cmsenv
# clone this repo
git clone git@github.com:UniMiBAnalyses/LHEprod.git
# compile the plugins
scram b -j 8
```

Once the environment is ready you can geenrate events starting from the gridpack. The output will be a .root file:

```
cd Dumpers/LHEDumper
cmsRun LHEDumperRunner.py input=<PATH_TO_GRIDPACK(default=gridpack.tar.xz)> \
                          output=<OUTPUT_ROOT_FILE(default=output.root)> \
                          nevents=<NUMBER_OF_EVENTS(default=10)> \
                          seed=<STARTING_SEED(default=10)> 
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
