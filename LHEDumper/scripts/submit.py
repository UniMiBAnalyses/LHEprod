#!/cvmfs/cms.cern.ch/el8_amd64_gcc10/cms/cmssw-patch/CMSSW_12_4_11_patch3/external/el8_amd64_gcc10/bin/python3
import os, sys 
import stat
import argparse
import tempfile
import json
from glob import glob
import multiprocessing  as mp
import numpy as np
import ROOT
import shutil

def swConsistency__(scram, cmssw):
    current_scram_ = os.environ["SCRAM_ARCH"]
    # set new scram
    os.environ["SCRAM_ARCH"] = scram 
    try:
        cmssw_list = ["CMSSW" + i.strip().split("CMSSW")[2] for i in os.popen("scram list").read().split("\n") if i.startswith("  CMSSW")]
        if cmssw not in cmssw_list:
            raise KeyError(f"The requested CMSSW version {cmssw} is not found for the requested SCRAM version {scram}")
        
    except:
        raise RuntimeError("General error happened in module swConsistency__, check CMSSW and SCRAM versions")
    
    return True

def submitCondor__(condorpath_):

    os.chdir(condorpath_)
    # submit 
    os.system("condor_submit submit.jdl")

    return
    

def createCondor__(args):

    # retrieve the CMSSW base 
    base_ = os.environ["CMSSW_BASE"]
    runner_ = os.path.join(base_, "src", "LHEprod", "LHEDumper", "LHEDumperRunner.py")
    pluginsfolder_ = os.path.join(base_, "src", "LHEprod", "LHEDumper", "plugins")
    pyfolder_ = os.path.join(base_, "src", "LHEprod", "LHEDumper", "python")

    path_ = os.path.join(base_, "src", "LHEprod", "LHEDumper", ".condorsub")
    condor_log_ = "condor_log"

    full_path_ = os.path.join(path_, condor_log_)

    # load general config
    d = json.load(open(args.conf, "r"))

    
    if not os.path.isdir(path_):  os.mkdir(path_)
    if not os.path.isdir(full_path_): os.mkdir(full_path_)

    print("--> Info: condor logs will be saved at " + full_path_)
    exe_ = "submit.sh"
    sub_ = "submit.jdl"

    scram_ = d["SCRAM"]
    cmssw_ = d["CMSSW"]
    lhe_prefix_ = "nAOD_LHE"

    if swConsistency__(scram_, cmssw_):
        print(f"--> Warning setup condor jobs with SCRAM={scram_} and CMSSW={cmssw_}")

    # check if gridpack is on afs or on eos
    eosgp = args.gridpack.startswith("/eos")

    # make the submission file
    # Need to check that input and output paths agree with tier
    # if afs, we should transfer input files specifying in the .jdl
    # if eos, we should xrdcp from inside the .sh script
    
    
    with open(os.path.join(path_, sub_), 'w') as condorSub:
        condorSub.write(f'executable = {exe_}\n')
        condorSub.write('universe = vanilla\n')
        for step__ in ["output", "error", "log"]:
            condorSub.write(f'{step__}                = {full_path_}/job.$(ClusterId).$(ProcId).$(Step).{step__[:3]}\n')
        condorSub.write('on_exit_hold = (ExitBySignal == True) || (ExitCode != 0)\n')
        condorSub.write('periodic_release =  (NumJobStarts < 3) && ((CurrentTime - EnteredCurrentStatus) > (60*3))\n')
        condorSub.write('request_cpus = 1\n')
        condorSub.write(f'MY.WantOS = "{scram_[:3]}"\n')
        condorSub.write("\n\n")
        condorSub.write('request_cpus = 1\n')
        condorSub.write(f'gridpack = {args.gridpack}\n') # we do not care if the gridpack is on eos or afs here
        condorSub.write(f'nthreads = {args.nthreads}\n')
        condorSub.write(f'nevents = {args.nevents}\n')
        condorSub.write("\n\n")
        condorSub.write('arguments = $(Step) $(gridpack) $(nthreads) $(nevents)\n')
        condorSub.write('should_transfer_files = YES\n')
        condorSub.write('transfer_input_files = {}, {}, {} {}'.format(runner_, pluginsfolder_, pyfolder_, ", " + args.gridpack + "\n" if not eosgp else "\n" ))
        condorSub.write(f'+JobFlavour = "{args.queue}"\n')
        condorSub.write('\n\n')
        if args.tier == "afs":
            condorSub.write(f'transfer_output_remaps = "{lhe_prefix_}.root = {args.output}/{lhe_prefix_}_$(Step).root"\n')
            condorSub.write('when_to_transfer_output = ON_EXIT\n')
            condorSub.write('\n\n')

        condorSub.write(f'queue {args.njobs}\n')

    with open(os.path.join(path_, exe_), 'w') as condorExe:
        condorExe.write('#!/bin/bash\n')
        condorExe.write(f'export EOS_MGM_URL={d["EOS_MGM_URL"]}\n')
        condorExe.write('echo "Starting job on " `date` #Date/time of start of job\n')
        condorExe.write('echo "Running on: `uname -a`" #Condor job is running on this node\n')
        condorExe.write('echo "System software: `cat /etc/redhat-release`" #Operating System on that node\n')
        condorExe.write('echo "System software: `cat /etc/redhat-release`" #Operating System on that node\n')
        condorExe.write('source /cvmfs/cms.cern.ch/cmsset_default.sh\n')
        condorExe.write('\n\n')
        condorExe.write(f'export SCRAM_ARCH={scram_}\n')
        condorExe.write(f'cmsrel {cmssw_}\n')
        condorExe.write('\n\n')
        condorExe.write(f'cd {cmssw_}/src; eval `scram runtime -sh`; cd -\n')
        condorExe.write(f'mkdir $CMSSW_BASE/src/LHEprod; mkdir $CMSSW_BASE/src/LHEprod/LHEDumper\n')
        condorExe.write('cp -r plugins $CMSSW_BASE/src/LHEprod/LHEDumper\n')
        condorExe.write('cp -r python $CMSSW_BASE/src/LHEprod/LHEDumper \n')
        condorExe.write('cp LHEDumperRunner.py $CMSSW_BASE/src/LHEprod/LHEDumper\n')
        condorExe.write('\n\n')
        if eosgp: condorExe.write(f'xrdcp {d["EOS_MGM_URL"]}/"${2}" .\n')
        condorExe.write('splits=($(echo $2 | tr "/" " "))\n')
        condorExe.write('gp_here=${splits[-1]} # this is the name of the gridpack in local\n')
        condorExe.write('cp $gp_here $CMSSW_BASE/src/LHEprod/LHEDumper\n')
        condorExe.write('\n\n')
        condorExe.write('cd $CMSSW_BASE/src/LHEprod/LHEDumper\n')
        condorExe.write('scram b -j 8\n')

        condorExe.write('echo "cmsRun -e -j FrameworkJobReport.xml LHEDumperRunner.py jobNum="$1" seed="$1" nthreads="$3" nevents="$4" input="${PWD}/${gp_here}""\n')
        condorExe.write('cmsRun -e -j FrameworkJobReport.xml LHEDumperRunner.py jobNum="$1" seed="$1" nthreads="$3" nevents="$4" input="${PWD}/${gp_here}"\n')
        condorExe.write('\n\n')
        # if the output stage is eos, xrdcp into it
        if args.tier == "eos": condorExe.write(f'xrdcp {lhe_prefix_}.root "$EOS_MGM_URL"/{args.output}/{lhe_prefix_}_"$1".root\n')
        else:
            condorExe.write(f'mv {lhe_prefix_}.root $CMSSW_BASE; cd $CMSSW_BASE\n')
            condorExe.write(f'mv {lhe_prefix_}.root ..; cd ..\n')
            condorExe.write('ls -lrth \n')
            condorExe.write('echo "Done"\n')

    # make it executable chmod +x
    st = os.stat(os.path.join(path_, exe_))
    os.chmod(os.path.join(path_, exe_), st.st_mode | stat.S_IEXEC)

    return path_

def createCrab__(args):

    from CRABClient.UserUtilities import config
    import CRABClient

    if not args.crabconf: raise RuntimeError("In order to submit via crab you need to provide a crab configuration file")
    eosgp = args.gridpack.startswith("/eos")
    if not eosgp: raise RuntimeError("The gridpack should be saved on eos becuase crab cannot handle heavy input file \
     sandbox. Path should start with /eos")
    if not args.output.startswith("/store"): raise RuntimeError("Provide an output directory compatible with crab standards")

    # load crab config
    dc = json.load(open(args.crabconf, "r"))
    d = json.load(open(args.conf, "r"))

    # retrieve the CMSSW base 
    base_ = os.environ["CMSSW_BASE"]
    runner_ = os.path.join(base_, "src", "LHEprod", "LHEDumper", "LHEDumperRunner.py")
    runner_name_ = "LHEDumperRunner.py"

    lhe_prefix = "nAOD_LHE"

    # first build the crab script

    exe_ = os.path.join(base_, "src", "LHEprod", "LHEDumper", "scripts", "crabsubmit.sh")
    with open(exe_, 'w') as crabExe:
        crabExe.write('#!/bin/bash\n')
        crabExe.write('bash\n')
        crabExe.write('set -e\n')
        crabExe.write(f'export EOS_MGM_URL={d["EOS_MGM_URL"]}\n')
        crabExe.write('BASE=$PWD\n')
        crabExe.write('RELEASE_BASE=$CMSSW_BASE\n')
        crabExe.write(f'export SCRAM_ARCH={os.environ["SCRAM_ARCH"]}\n')
        crabExe.write('source /cvmfs/cms.cern.ch/cmsset_default.sh\n')
        crabExe.write('\n\n')
        crabExe.write('cd $RELEASE_BASE\n')
        crabExe.write('eval `scram runtime -sh`\n')
        crabExe.write('cd $BASE\n')
        crabExe.write('\n\n')
        crabExe.write('splits=($(echo $2 | tr "=" " "))\n')
        crabExe.write('gp=${splits[1]}\n')
        crabExe.write(f'xrdcp {d["EOS_MGM_URL"]}/"$gp" .\n')
        crabExe.write('\n\n')
        crabExe.write('splits2=($(echo $gp | tr "/" " "))\n')
        crabExe.write('gp_here=${splits2[-1]} # this is the name of the gridpack in local\n')
        crabExe.write('\n\n')
        crabExe.write(f'echo "cmsRun -e -j FrameworkJobReport.xml {runner_name_} ' + 'jobNum="$1" \
                        seed="$1" "$3" "$4" input="${PWD}/${gp_here}""\n')
        crabExe.write(f'cmsRun -e -j FrameworkJobReport.xml {runner_name_} ' + 'jobNum="$1" \
                        seed="$1" "$3" "$4" input="${PWD}/${gp_here}"\n')
        crabExe.write('\n\n')


    # make it executable chmod +x
    st = os.stat(exe_)
    os.chmod(exe_, st.st_mode | stat.S_IEXEC)

    ## parameters 
    nThreads = args.nthreads
    datasetname = args.datasetname if args.datasetname else dc["outputPrimaryDataset"]
    datasettag =  args.datasettag if args.datasettag else dc["outputDatasetTag"]
    requestname = args.requestname if args.requestname else dc["requestName"]

    requestname += "_{}".format(len(glob("crab_" + requestname + "*")))

    configlhe=runner_
    outputName = f'{lhe_prefix}.root'
    gp = args.gridpack
    nEvents = args.nevents
    nEventsTotal = int(args.nevents * args.njobs)

    ## config file
    config = config()
    ## General settings
    config.General.requestName = requestname
    config.General.transferOutputs = True
    config.General.transferLogs = False
    ## PrivateMC type with a fake miniAOD step to circunvent crab requests (official data-tier for PrivateMC)
    config.JobType.pluginName  = 'PrivateMC'
    config.JobType.psetName    = configlhe
    # config.JobType.pyCfgParams = ['nThreads='+str(nThreads)]
    ## To be executed on node with Arguments
    config.JobType.scriptExe   = exe_
    config.JobType.scriptArgs  = ['input='+gp, 'nthreads='+str(nThreads), 'nevents='+str(nEvents)]
    config.JobType.inputFiles  = [configlhe]
    ## Output file to be collected
    config.JobType.outputFiles = [outputName]
    config.JobType.disableAutomaticOutputCollection = False
    ## Memory, cores, cmssw
    config.JobType.allowUndistributedCMSSW = True
    config.JobType.maxMemoryMB = args.maxmemory if args.maxmemory else dc["maxmemory"]
    config.JobType.numCores    = nThreads
    ## Data
    config.Data.splitting   = 'EventBased'
    config.Data.unitsPerJob = nEvents
    config.Data.totalUnits  = nEventsTotal
    config.Data.outLFNDirBase = args.output
    config.Data.publication   = True
    config.Data.outputPrimaryDataset = datasetname
    config.Data.outputDatasetTag = datasettag
    ## Site
    config.Site.storageSite = dc["storageSite"]

    print(config)
    from CRABAPI.RawCommand import crabCommand
    crabCommand('submit', config = config)

def computeXsec__(file_):
    print(f'processing file {file_}')
    df = ROOT.RDataFrame("Events", file_)
    xsec = df.Sum("LHEWeight_originalXWGTUP").GetValue()
    nev = df.Count().GetValue()
    
    return [xsec, nev]

def mtCrossSectionWeight__(args):

    # load general config
    d = json.load(open(args.conf, "r"))

    # retrieve all files
    lhe_prefix = "nAOD_LHE"
    inputFiles_ = f'{args.output}/{lhe_prefix}'
    retrieve_ = glob(inputFiles_ + '*.root')
    if len(retrieve_) == 0:
        print(f'Info did not found any file at path {args.output}, check job status')

    # compute the cross section weight for the generation
    # Need to scan each file separately
    pool = mp.Pool(8)
    results = pool.map(computeXsec__, retrieve_)
    xsecs = np.array([i[0] for i in results])
    xsec = xsecs.mean()
    err = xsecs.std()
    nev = np.array([i[1] for i in results]).sum()
    
    # We can use this value to fill histograms and obtain the right normalization straight away
    # Will save as fb in order to normalize with integrated lumi at LHC
    # sum of weights = xsec !!!
    baseW =  1000.* xsec/nev

    print(f"Final xsec = {xsec} +- {err} pb for NEvents {nev} (per-event weight: {baseW} fb)")

    return baseW

def mtbaseWUtil__(arg):
    # arg = {"path": '', "baseW": ''}
    opts = ROOT.RDF.RSnapshotOptions()
    opts.fMode = "update"
    opts.fOverwriteIfExists = True

    df = ROOT.RDataFrame("Events", arg['path'])

    if "baseW" in df.GetColumnNames(): return 

    tmp__ = tempfile.NamedTemporaryFile( suffix='.root' ) 
    os.remove(tmp__.name)
    
    df.Define("baseW", str(arg['baseW'])).Snapshot("Events", tmp__.name, "", opts)
    shutil.copyfile(tmp__.name, arg['path'])

def appendBaseW__(files, baseW):

    # expects files as a list of string and baseW as a str / number
    nt_ = 8 if len(files) > 8 else len(files)
    args = [{"path": i, "baseW": baseW} for i in files]
    
    pool = mp.Pool(nt_)
    pool.map(mtbaseWUtil__, args)
    

def addCrossSectionWeight__(args):
    # load general config
    d = json.load(open(args.conf, "r"))
    lhe_prefix = "nAOD_LHE"

    # if working on afs or eos tier then we can collect all files simply with glob

    if args.tier in ["afs", "eos"]:
        inputFiles_ = f'{args.output}/{lhe_prefix}'
        retrieve_ = glob(inputFiles_ + '*.root')
        baseW = mtCrossSectionWeight__(args)
        appendBaseW__(retrieve_, baseW)

def merge__(args):

    # load general config
    d = json.load(open(args.conf, "r"))

    outFile_ = args.merge
    lhe_prefix = "nAOD_LHE"
    baseW = mtCrossSectionWeight__(args)
    inputFiles_ = f'{args.output}/{lhe_prefix}'

    # if working on afs or eos tier then we can collect all files simply with glob
    if args.tier in ["afs", "eos"]:
        
        retrieve_ = glob(inputFiles_ + '*.root')
        os.system('hadd -j 8 {} {}'.format(outFile_,  " ".join(i for i in retrieve_)))

        # Append the baseW column 
        appendBaseW__([outFile_] , baseW)

    # If it is crab things are a little bit more complicated, 
    # files will be stored on the tier as 
    #    <args.output>/<crabconf.outputPrimaryDataset>/<crabconf.outputDatasetTag>/<date_time>/<run_number>/<lhe_prefix>_<jobNumber>.root
    
    else:
        if not args.crabmerge:
            raise RuntimeError('If you want to use merge on crab production, you need to also specify --crabmerge \
                               giving the path to the crab diretory on your local afs crab_<requestName>_<number>')
        
        # retrieve the dataset 
        from CRABAPI.RawCommand import crabCommand
        res = crabCommand('status', d = args.crabmerge)

        # for some reason it is a str of a list
        od = res["outdatasets"][2:-2]
        st = res["status"]
        dbst = res["dbStatus"]

        if st != "COMPLETED":
            raise RuntimeError("The submission is not yet completed. Control with crab status -d <dir_name>")
        
        # gather files

        print(f'dasgoclient --query="file dataset={od} instance=prod/phys03"')
        files_ = os.popen(f'dasgoclient --query="file dataset={od} instance=prod/phys03"').read().split('\n')

        print(files_)

# def merge__(args):

#     # load general config
#     d = json.load(open(args.conf, "r"))

#     outFile_ = args.merge

#     # if working on afs or eos tier then we can collect all files simply with glob

#     if args.tier in ["afs", "eos"]:
#         lhe_prefix = "nAOD_LHE"
#         inputFiles_ = f'{args.output}/{lhe_prefix}'
#         retrieve_ = glob(inputFiles_ + '*.root')
#         if len(retrieve_) == 0:
#             print(f'Info did not found any file at path {args.output}, check job status')


#         # compute the cross section weight for the overall file
#         # Need to scan each file separately
#         print("Computing cross section for the merged file")
#         pool = mp.Pool(8)
#         results = pool.map(computeXsec__, retrieve_)
#         xsecs = np.array([i[0] for i in results])
#         xsec = xsecs.mean()
#         err = xsecs.std()
#         nev = np.array([i[1] for i in results]).sum()
        
#         # We can use this value to fill histograms and obtain the right normalization straight away
#         # Will save as fb in order to normalize with integrated lumi at LHC
#         baseW =  1000.* xsec/nev

#         print(f"Final xsec = {xsec} +- {err} pb for NEvents {nev} (per-event weight: {baseW} fb)")

#         # Actually merge the files
#         tmp__ = tempfile.NamedTemporaryFile( suffix='.root' ) 
#         os.remove(tmp__.name)

#         os.system('hadd -j 8 {} {}'.format(tmp__.name,  " ".join(i for i in retrieve_)))

#         # Append the baseW column 
#         # This is NanoAOD so we will have Events TTree
#         df = ROOT.RDataFrame("Events", tmp__.name)
#         df.Define("baseW", str(baseW)).Snapshot("Events", outFile_)



#     # If it is crab things are a little bit more complicated, 
#     # files will be stored on the tier as 
#     #    <args.output>/<crabconf.outputPrimaryDataset>/<crabconf.outputDatasetTag>/<date_time>/<run_number>/<lhe_prefix>_<jobNumber>.root
    
#     else:
#         if not args.crabmerge:
#             raise RuntimeError('If you want to use merge on crab production, you need to also specify --crabmerge \
#                                giving the path to the crab diretory on your local afs crab_<requestName>_<number>')
        
#         # retrieve the dataset 
#         from CRABAPI.RawCommand import crabCommand
#         res = crabCommand('status', d = args.crabmerge)

#         # for some reason it is a str of a list
#         od = res["outdatasets"][2:-2]
#         st = res["status"]
#         dbst = res["dbStatus"]

#         if st != "COMPLETED":
#             raise RuntimeError("The submission is not yet completed. Control with crab status -d <dir_name>")
        
#         # gather files

#         print(f'dasgoclient --query="file dataset={od} instance=prod/phys03"')
#         files_ = os.popen(f'dasgoclient --query="file dataset={od} instance=prod/phys03"').read().split('\n')

#         print(files_)



if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-gp',  '--gridpack',   dest='gridpack',     help='Path to the gridpack you want to generate events with. [REQUIRED]', required = False, type=str)
    parser.add_argument('-o',  '--output',   dest='output',     help='Output folder where .root files will be stored. If using crab something like /store/user/<username>/... [REQUIRED]', required = False, type=str)
    parser.add_argument('-ne',  '--nevents',   dest='nevents',     help='Number of events per job requested (def=1000)', required = False, default=1000, type=int)
    parser.add_argument('-nj',  '--njobs',   dest='njobs',     help='Number of jobs requested (def=1)', required = False, default=1, type=int)
    parser.add_argument('-nt',  '--nthreads',   dest='nthreads',     help='Number of threads x job (def=1)', required = False, default=1, type=int)
    parser.add_argument('-t',  '--tier',   dest='tier',     help='Tier for production. can be [afs, eos, crab]. Afs will submit jobs with HTcondor and save root files on afs while eos option will xrdcp to eos. crab instead will save on the specified tier in the crab config file', required = False, default="afs", type=str)
    parser.add_argument('-q',  '--queue',   dest='queue',     help='Condor queue (def=longlunch)', required = False, default="longlunch", type=str)
    parser.add_argument('-m',  '--merge',   dest='merge',     help='Collect output root files and merge them. Provide an output path at -m (default=None)', required = False, default=None, type=str)
    parser.add_argument('-cm',  '--crabmerge',   dest='crabmerge',     help='For crab merge, you need to also specify the crab direcotry in your local afs so we can gather the necessary information of the published dataset', required = False, default=None, type=str)
    parser.add_argument('--conf',   dest='conf',     help='Load configuration file (default=configuration/conf.json)', required = False, default = os.path.join(os.environ["CMSSW_BASE"], "src", "LHEprod", "LHEDumper", "configuration", "conf.json"))
    # Appen base w
    parser.add_argument('--basew',          dest='basew',     help='Add the cross section weight gathering all the files from the output folders', default=False, action='store_true')
    # crab specific settings
    parser.add_argument('--crabconf',       dest='crabconf',        help='Crab config json file (default=configuration/crabconf.json)', required = False, default = os.path.join(os.environ["CMSSW_BASE"], "src", "LHEprod", "LHEDumper", "configuration", "crabconf.json"))
    parser.add_argument('--datasetname',    dest='datasetname',     help='Crab dataset name under config.Data.outputPrimaryDataset. Can also specify in crabconfig', required = False, default = None)
    parser.add_argument('--requestname',    dest='requestname',     help='Name of the crab request under config.General.requestName', required = False, default = None)
    parser.add_argument('--datasettag',     dest='datasettag',      help='Name of the crab dataset tag under config.Data.outputDatasetTag', required = False, default = None)
    parser.add_argument('--maxmemory',      dest='maxmemory',       help='Max memory of the crab request under config.JobType.maxMemoryMB', required = False, default = None)

    args = parser.parse_args()

    if not args.tier in ["afs", "eos", "crab"]: raise KeyError(f"Tier argument {args.tier} is not supported is not in afs eos crab")
    
    if not args.merge and not args.basew:
        if args.tier == "eos":
            if not args.output.startswith("/eos"): 
                raise ValueError("You specified eos Tier but the ooutput path is not a eos directory. Specify an output path starting with /eos")
            
        if args.tier == "afs":
            if args.output.startswith("/afs"): pass 
            elif args.output.startswith("/eos"): 
                print("--> Warning output stage on eos requested, will change to eos tier")
                args.tier = "eos" 
            else: args.output = os.path.join(os.environ["PWD"], args.output)
            if not os.path.isdir(args.output): os.mkdir(args.output)

    
        if args.tier in ["afs", "eos"] : 
            condorpath_ = createCondor__(args)
            submitCondor__(condorpath_)
        else:
            print("CRAB")
            createCrab__(args)

    elif args.merge:
        merge__(args)

    elif args.basew:
        addCrossSectionWeight__(args)

