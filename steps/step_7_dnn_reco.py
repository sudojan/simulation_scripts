#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py2-v3.0.1/icetray-start
#METAPROJECT /data/user/mhuennefeld/software/icecube/py2-v3.0.1/combo_trunk/build
import os

import click
import yaml

from utils import get_run_folder

from I3Tray import I3Tray
from icecube import icetray, dataio, dataclasses, hdfwriter, phys_services

from icecube.DNN_reco.deep_learning_reco import DeepLearningReco


def get_valid_pulse_map(frame, pulse_key, excluded_doms, 
                        partial_exclusion, verbose=False):
    # -------------------------------
    #   Todo: Only work on 
    #         I3RecoPulseSeriesMapMask
    # -------------------------------
    pulses = frame[pulse_key]

    if excluded_doms:
        if isinstance(pulses, dataclasses.I3RecoPulseSeriesMapMask) or \
           isinstance(pulses, dataclasses.I3RecoPulseSeriesMapUnion):
            pulses = pulses.apply(frame)

        pulses = dict(pulses)
        length_before = len(pulses)
        num_rm_pulses = 0

        for exclusion_key in excluded_doms:

            if exclusion_key in frame:

                #-------------------------
                # List of OMkeys to ignore
                #-------------------------
                if isinstance(frame[exclusion_key], 
                            dataclasses.I3VectorOMKey) or \
                   isinstance(frame[exclusion_key],list):

                    for key in frame[exclusion_key]:
                        pulses.pop(key, None)

                #-------------------------
                # I3TimeWindowSeriesMap
                #-------------------------
                elif isinstance(frame[exclusion_key], 
                            dataclasses.I3TimeWindowSeriesMap):

                    if partial_exclusion:
                        # remove Pulses in exluded time window
                        for key in frame[exclusion_key].keys():

                            # skip this key if it does
                            # not exist in reco pulse map
                            if not key in pulses:
                                continue

                            valid_hits = []
                            
                            # go through each reco pulse
                            for hit in pulses[key]:

                                # assume hit is valid
                                hit_is_valid = True

                                # go through every time window
                                for time_window in frame[exclusion_key][key]:
                                    if hit.time >= time_window.start and \
                                       hit.time <= time_window.stop:

                                       # reco pulse is in exclusion
                                       # time window and therefore
                                       # not valid
                                       hit_is_valid = False
                                       break

                                # append to valid hits   
                                if hit_is_valid:
                                    valid_hits.append(hit)
                            
                            # replace old hit
                            num_rm_pulses += len(pulses[key]) - len(valid_hits)
                            pulses.pop(key, None)
                            if valid_hits:
                                pulses[key] = dataclasses.vector_I3RecoPulse(valid_hits)

                    else:
                        # remove whole DOM
                        for key in frame[exclusion_key].keys():
                            pulses.pop(key, None)
                else:
                    msg = 'Unknown exclusion type {} of key {}'
                    raise ValueError(msg.format( type(frame[exclusion_key]),
                                                             exclusion_key))

        pulses = dataclasses.I3RecoPulseSeriesMap(pulses)

        if verbose:
            num_removed = length_before - len(pulses)
            msg = '[DNN_reco] Removed {} DOMs and {} additional pulses from {}'
            # ToDo: use logging
            print(  msg.format( num_removed,
                            num_rm_pulses,
                            pulse_key ))

    frame[pulse_key+'_masked'] = pulses
    # -------------------------------


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
    # Mask Pulses
    #--------------------------------------------------
    tray.AddModule(get_valid_pulse_map,'get_valid_pulse_map',
                   pulse_key=cfg['DNN_pulse_key'],
                   excluded_doms=cfg['DNN_excluded_doms'],
                   partial_exclusion=cfg['DNN_partial_exclusion'],
                   verbose=True,
                   If = lambda f: cfg['DNN_pulse_key'] in f )

    #--------------------------------------------------
    # Apply DNN_reco
    #--------------------------------------------------
    tray.AddModule(DeepLearningReco,'DeepLearningReco',
                    PulseMapString=cfg['DNN_pulse_key'] + '_masked',
                    OutputBaseName=cfg['DNN_output_base_name'],
                    DNNModel=cfg['DNN_model'],
                    DNNModelDirectory=cfg['DNN_model_directory'],
                    MeasureTime=cfg['DNN_measure_time'],
                    ParallelismThreads=cfg['resources']['cpus'][cfg['step']],
                    )


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
