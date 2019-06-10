#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py2-v3.0.1/icetray-start
#METAPROJECT /data/user/mhuennefeld/software/icecube/py2-v3.0.1/combo_trunk/build
import os

import click
import yaml

from utils import get_run_folder

from I3Tray import I3Tray
from icecube import icetray, dataio, dataclasses, hdfwriter, phys_services

from ic3_labels.labels import modules
# from utils.mask_pulses import get_valid_pulse_map
# from icecube.DNN_reco.deep_learning_reco import DeepLearningReco

@click.command()
@click.argument('cfg', type=click.Path(exists=True))
@click.argument('run_number', type=int)
@click.option('--scratch/--no-scratch', default=True)
def main(cfg, run_number, scratch):
    with open(cfg, 'r') as stream:
        cfg = yaml.load(stream, Loader=yaml.Loader)
    cfg['run_number'] = run_number
    cfg['run_folder'] = get_run_folder(run_number)

    infile = cfg['infile_pattern'].format(**cfg)
    infile = infile.replace(' ', '0')

    if scratch:
        outfile = cfg['scratchfile_pattern'].format(**cfg)
    else:
        outfile = cfg['outfile_pattern'].format(**cfg)
    outfile = outfile.replace(' ', '0')


    tray = I3Tray()
    """The main script"""
    tray.AddModule('I3Reader',
                   'i3 reader',
                   FilenameList=[cfg['gcd'], infile])

    #--------------------------------------------------
    # Add MC Labels mirco trained his DNN on
    #--------------------------------------------------
    add_cascade_labels = False
    if add_cascade_labels:
        tray.AddModule(modules.MCLabelsCascadeParameters, 'MCLabelsCascadeParameters',
                       PulseMapString='InIcePulses',
                       PrimaryKey='MCPrimary1',
                       OutputKey='LabelsDeepLearning')
    else:
        tray.AddModule(modules.MCLabelsDeepLearning, 'MCLabelsDeepLearning',
                       PulseMapString='InIcePulses',
                       PrimaryKey='MCPrimary1',
                       OutputKey='LabelsDeepLearning',
                       IsMuonGun=True)


    # #--------------------------------------------------
    # # Mask Pulses
    # #--------------------------------------------------
    # tray.AddModule(get_valid_pulse_map,'get_valid_pulse_map',
    #                pulse_key=cfg['DNN_pulse_key'],
    #                excluded_doms=cfg['DNN_excluded_doms'],
    #                partial_exclusion=cfg['DNN_partial_exclusion'],
    #                verbose=True,
    #                If = lambda f: cfg['DNN_pulse_key'] in f )

    # #--------------------------------------------------
    # # Apply DNN_reco
    # #--------------------------------------------------
    # tray.AddModule(DeepLearningReco,'DeepLearningReco',
    #                 PulseMapString=cfg['DNN_pulse_key'] + '_masked',
    #                 OutputBaseName=cfg['DNN_output_base_name'],
    #                 DNNModel=cfg['DNN_model'],
    #                 DNNModelDirectory=cfg['DNN_model_directory'],
    #                 MeasureTime=cfg['DNN_measure_time'],
    #                 ParallelismThreads=cfg['resources']['cpus'],
    #                 )


    tray.AddModule("I3Writer", "EventWriter",
                   filename=outfile,
                   Streams=[icetray.I3Frame.DAQ,
                            icetray.I3Frame.Physics,
                            icetray.I3Frame.TrayInfo,
                            icetray.I3Frame.Simulation],
                   DropOrphanStreams=[icetray.I3Frame.DAQ])

    tray.Execute()

    del tray


if __name__ == '__main__':
    main()
