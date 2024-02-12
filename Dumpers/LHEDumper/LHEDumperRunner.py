#!/usr/bin/env cmsRun
import FWCore.ParameterSet.Config as cms
import FWCore.ParameterSet.VarParsing as VarParsing

# Paring command line arguments 
options = VarParsing.VarParsing('analysis')

options.register('input',
                    'gridpack.tar.xz',
                    VarParsing.VarParsing.multiplicity.singleton,
                    VarParsing.VarParsing.varType.string,
                    "input gridpack path (without file: prefix)")

options.register('output',
                    'output.root',
                    VarParsing.VarParsing.multiplicity.singleton,
                    VarParsing.VarParsing.varType.string,
                    "output file name (without file: prefix)")

options.register('nevents',
                    10,
                    VarParsing.VarParsing.multiplicity.singleton,
                    VarParsing.VarParsing.varType.int,
                    "Number of events to generate")

options.register('seed',
                    10,
                    VarParsing.VarParsing.multiplicity.singleton,
                    VarParsing.VarParsing.varType.int,
                    "Random number used as initial seed for this generation")

options.parseArguments()


# defining the process

process = cms.Process("LHE")

process.load('PhysicsTools.NanoAOD.nanogen_cff')
process.load('Configuration.StandardSequences.EndOfProcess_cff')
process.load('Configuration.StandardSequences.Services_cff')

# Changing the random number used for the generation
process.RandomNumberGeneratorService.externalLHEProducer.initialSeed = options.seed

# Define the source file as empty as we will be starting from scratch with our gridpack
process.source = cms.Source("EmptySource")


# Defining the external LHE producer, to generate the LHE

process.maxEvents = cms.untracked.PSet(
    input = cms.untracked.int32(options.nevents)
)
process.options = cms.untracked.PSet(

)

process.externalLHEProducer = cms.EDProducer("ExternalLHEProducer",
    args = cms.vstring(options.input),
    generateConcurrently = cms.untracked.bool(True),
    nEvents = cms.untracked.uint32(options.nevents),
    numberOfParameters = cms.uint32(1),
    outputFile = cms.string('cmsgrid_final.lhe'),
    scriptName = cms.FileInPath('GeneratorInterface/LHEInterface/data/run_generic_tarball_cvmfs.sh')
)

process.configurationMetadata = cms.untracked.PSet(
	version = cms.untracked.string('alpha'),
	name = cms.untracked.string('LHEF input'),
	annotation = cms.untracked.string('user file')
)

from Dumpers.LHEDumper.lheWeightTable_cfi import lheWeightsTable
from PhysicsTools.NanoAOD.taus_cff import *
from PhysicsTools.NanoAOD.jetMC_cff import *
from PhysicsTools.NanoAOD.globals_cff import genTable,genFilterTable
from PhysicsTools.NanoAOD.met_cff import metMCTable
from PhysicsTools.NanoAOD.genparticles_cff import *
from PhysicsTools.NanoAOD.particlelevel_cff import *
from PhysicsTools.NanoAOD.genWeightsTable_cfi import *
from PhysicsTools.NanoAOD.genVertex_cff import *
from PhysicsTools.NanoAOD.common_cff import Var,CandVars


process.lheInfoTable = cms.EDProducer("LHETablesProducer",
     lheInfo = cms.VInputTag(cms.InputTag("externalLHEProducer"), cms.InputTag("source")),
     precision = cms.int32(23),
     storeLHEParticles = cms.bool(True)
 )

process.lheWeightTable = lheWeightsTable

NanoAODEDMEventContent = cms.PSet(
    outputCommands = cms.untracked.vstring(
        'drop *',
        "keep nanoaodFlatTable_*Table_*_*",     # event data
        "keep edmTriggerResults_*_*_*",  # event data
        "keep String_*_genModel_*",  # generator model data
        "keep nanoaodMergeableCounterTable_*Table_*_*", # accumulated per/run or per/lumi data
        "keep nanoaodUniqueString_nanoMetadata_*_*",   # basic metadata
    )
)

process.NANOAODSIMEventContent = NanoAODEDMEventContent.clone(
    compressionLevel = cms.untracked.int32(9),
    compressionAlgorithm = cms.untracked.string("LZMA"),
)

process.NANOAODSIMoutput = cms.OutputModule("NanoAODOutputModule",

    compressionAlgorithm = cms.untracked.string('LZMA'),
    compressionLevel = cms.untracked.int32(9),
    dataset = cms.untracked.PSet(
        dataTier = cms.untracked.string('NANOAODSIM'),
        filterName = cms.untracked.string('')
    ),
    fileName = cms.untracked.string('file:' + options.output),
    outputCommands = process.NANOAODSIMEventContent.outputCommands
)



# Path and EndPath definitions
process.lhe_step = cms.Path(process.externalLHEProducer)
process.lhe_particles = cms.Path(process.lheInfoTable)
process.lhe_weights = cms.Path(process.lheWeightTable)

process.endjob_step = cms.EndPath(process.endOfProcess)
process.NANOAODSIMoutput_step = cms.EndPath(process.NANOAODSIMoutput)

process.schedule = cms.Schedule(process.lhe_step, process.lhe_particles, process.lhe_weights, process.endjob_step, process.NANOAODSIMoutput_step)

