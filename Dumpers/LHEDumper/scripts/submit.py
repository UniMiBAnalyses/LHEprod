#!/cvmfs/cms.cern.ch/el8_amd64_gcc10/cms/cmssw-patch/CMSSW_12_4_11_patch3/external/el8_amd64_gcc10/bin/python3
import os, sys 
import stat
import argparse
import tempfile
import json
from glob import glob

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

    path_ = tempfile.mkdtemp()
    
    print("--> Info: condor logs will be saved at " + path_)

    # load general config
    d = json.load(open(args.conf, "r"))

    condor_log_ = "condor_log"
    if not os.path.isdir(path_):  os.mkdir(path_)
    if not os.path.isdir(os.path.join(path_, condor_log_)): os.mkdir(os.path.join(path_, condor_log_))

    print("--> Info: condor logs will be saved at " + os.path.join(path_, condor_log_))
    exe_ = "submit.sh"
    sub_ = "submit.jdl"

    scram_ = d["SCRAM"]
    cmssw_ = d["CMSSW"]
    lhe_prefix_ = args.lhe_prefix if args.lhe_prefix else d["LHE_PREFIX"]

    # retrieve the CMSSW base 
    base_ = os.environ["CMSSW_BASE"]
    runner_ = os.path.join(base_, "src", "Dumpers", "LHEDumper", "LHEDumperRunner.py")
    pluginsfolder_ = os.path.join(base_, "src", "Dumpers", "LHEDumper", "plugins")
    pyfolder_ = os.path.join(base_, "src", "Dumpers", "LHEDumper", "python")

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
            condorSub.write(f'{step__}                = condor_log/job.$(ClusterId).$(ProcId).$(Step).{step__[:3]}\n')
        condorSub.write('on_exit_hold = (ExitBySignal == True) || (ExitCode != 0)\n')
        condorSub.write('periodic_release =  (NumJobStarts < 3) && ((CurrentTime - EnteredCurrentStatus) > (60*3))\n')
        condorSub.write('request_cpus = 1\n')
        condorSub.write(f'MY.WantOS = "{scram_[:3]}"\n')
        condorSub.write("\n\n")
        condorSub.write('request_cpus = 1\n')
        condorSub.write(f'gridpack = {args.gridpack}\n') # we do not care if the gridpack is on eos or afs here
        condorSub.write('nAOD_output = nanoAOD_LHE.root\n')
        condorSub.write(f'nthreads = {args.nthreads}\n')
        condorSub.write(f'nevents = {args.nevents}\n')
        condorSub.write("\n\n")
        condorSub.write('arguments = $(Step) $(gridpack) $(nAOD_output) $(nthreads) $(nevents)\n')
        condorSub.write('should_transfer_files = YES\n')
        condorSub.write('transfer_input_files = {}, {}, {} {}'.format(runner_, pluginsfolder_, pyfolder_, ", " + args.gridpack + "\n" if not eosgp else "\n" ))
        condorSub.write(f'+JobFlavour = "{args.queue}"\n')
        condorSub.write('\n\n')
        if args.tier == "afs":
            condorSub.write(f'transfer_output_remaps = "$(nAOD_output) = {args.output}/{lhe_prefix_}_$(Step).root"\n')
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
        condorExe.write(f'mkdir $CMSSW_BASE/src/Dumpers; mkdir $CMSSW_BASE/src/Dumpers/LHEDumper\n')
        condorExe.write('cp -r plugins $CMSSW_BASE/src/Dumpers/LHEDumper\n')
        condorExe.write('cp -r python $CMSSW_BASE/src/Dumpers/LHEDumper \n')
        condorExe.write('cp LHEDumperRunner.py $CMSSW_BASE/src/Dumpers/LHEDumper\n')
        condorExe.write('cd $CMSSW_BASE/src/Dumpers/LHEDumper\n')
        condorExe.write('\n\n')
        condorExe.write('scram b -j 8\n')
        if eosgp: condorExe.write(f'xrdcp {d["EOS_MGM_URL"]}/"${2}" .\n')
        condorExe.write('splits=($(echo $2 | tr "/" " "))\n')
        condorExe.write('gp_here=${splits[-1]} # this is the name of the gridpack in local\n')
        condorExe.write('\n\n')
        condorExe.write('echo "cmsRun -e -j FrameworkJobReport.xml LHEDumperRunner.py jobNum="$1" seed="$1" output="$3" nthreads="$4" nevents="$5" input="${PWD}/${gp_here}""\n')
        condorExe.write('cmsRun -e -j FrameworkJobReport.xml LHEDumperRunner.py jobNum="$1" seed="$1" output="$3" nthreads="$4" nevents="$5" input="${PWD}/${gp_here}"\n')
        condorExe.write('\n\n')
        # if the output stage is eos, xrdcp into it
        if args.tier == "eos": condorExe.write(f'xrdcp "$3" "$EOS_MGM_URL"/{args.output}/{lhe_prefix_}_"$1".root\n')
        else:
            condorExe.write('mv $3 $CMSSW_BASE; cd $CMSSW_BASE\n')
            condorExe.write('mv $3 ..; cd ..\n')
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
    runner_ = os.path.join(base_, "src", "Dumpers", "LHEDumper", "LHEDumperRunner.py")
    runner_name_ = "LHEDumperRunner.py"

    # first build the crab script

    exe_ = os.path.join(base_, "src", "Dumpers", "LHEDumper", "scripts", "crabsubmit.sh")
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
                        seed="$1" "$3" "$4" "$5" input="${PWD}/${gp_here}""\n')
        crabExe.write(f'cmsRun -e -j FrameworkJobReport.xml {runner_name_} ' + 'jobNum="$1" \
                        seed="$1" "$3" "$4" "$5" input="${PWD}/${gp_here}"\n')

    # make it executable chmod +x
    st = os.stat(exe_)
    os.chmod(exe_, st.st_mode | stat.S_IEXEC)

    ## parameters 
    nThreads = args.nthreads
    datasetname = args.datasetname if args.datasetname else dc["outputPrimaryDataset"]
    datasettag =  args.datasettag if args.datasettag else dc["outputDatasetTag"]
    requestname = args.requestname if args.requestname else dc["requestName"]
    lhe_prefix = args.lhe_prefix if args.lhe_prefix else d["LHE_PREFIX"]

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
    config.JobType.scriptArgs  = ['input='+gp,'output='+outputName, 'nthreads='+str(nThreads), 'nevents='+str(nEvents)]
    config.JobType.inputFiles  = [configlhe]
    ## Output file to be collected
    config.JobType.outputFiles = [outputName]
    config.JobType.disableAutomaticOutputCollection = True
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


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-gp',  '--gridpack',   dest='gridpack',     help='Path to the gridpack you want to generate events with. [REQUIRED]', required = True, type=str)
    parser.add_argument('-o',  '--output',   dest='output',     help='Output folder where .root files will be stored. If using crab something like /store/user/<username>/... [REQUIRED]', required = True, type=str)
    parser.add_argument('-ne',  '--nevents',   dest='nevents',     help='Number of events per job requested (def=1000)', required = False, default=1000, type=int)
    parser.add_argument('-nj',  '--njobs',   dest='njobs',     help='Number of jobs requested (def=1)', required = False, default=1, type=int)
    parser.add_argument('-nt',  '--nthreads',   dest='nthreads',     help='Number of threads x job (def=1)', required = False, default=1, type=int)
    parser.add_argument('-t',  '--tier',   dest='tier',     help='Tier for production. can be [afs, eos, crab]. Afs will submit jobs with HTcondor and save root files on afs while eos option will xrdcp to eos. crab instead will save on the specified tier in the crab config file', required = False, default="afs", type=str)
    parser.add_argument('-q',  '--queue',   dest='queue',     help='Condor queue (def=longlunch)', required = False, default="longlunch", type=str)
    parser.add_argument('--prefix',  '--prefix',   dest='lhe_prefix',     help='The prefix of the output LHE files, by default written in conf.json', required = False, default=None, type=str)
    parser.add_argument('--conf',   dest='conf',     help='Load configuration file (default=configuration/conf.json)', required = False, default = os.path.join(os.environ["CMSSW_BASE"], "src", "Dumpers", "LHEDumper", "configuration", "conf.json"))
    # crab specific settings
    parser.add_argument('--crabconf',   dest='crabconf',     help='Crab config json file (default=configuration/crabconf.json)', required = False, default = os.path.join(os.environ["CMSSW_BASE"], "src", "Dumpers", "LHEDumper", "configuration", "crabconf.json"))
    parser.add_argument('--datasetname',   dest='datasetname',     help='Crab dataset name under config.Data.outputPrimaryDataset. Can also specify in crabconfig', required = False, default = None)
    parser.add_argument('--requestname',   dest='requestname',     help='Name of the crab request under config.General.requestName', required = False, default = None)
    parser.add_argument('--datasettag',   dest='datasettag',     help='Name of the crab dataset tag under config.Data.outputDatasetTag', required = False, default = None)
    parser.add_argument('--maxmemory',   dest='maxmemory',     help='Max memory of the crab request under config.JobType.maxMemoryMB', required = False, default = None)

    args = parser.parse_args()

    if not args.tier in ["afs", "eos", "crab"]: raise KeyError(f"Tier argument {args.tier} is not supported is not in afs eos crab")
    
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
        createCrab__(args)