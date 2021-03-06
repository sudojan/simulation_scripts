#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py2-v3.1.1/icetray-start
#METAPROJECT /data/user/jsoedingrekso/ic_software/combo_173346/build
import os

import click
import yaml

from icecube.simprod import segments
from I3Tray import I3Tray
from icecube import icetray, dataclasses, dataio, phys_services
from utils import create_random_services, get_run_folder


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

    tray.context['I3FileStager'] = dataio.get_stagers()

    random_services, run_id = create_random_services(
        dataset_number=cfg['dataset_number'],
        run_number=cfg['run_number'],
        seed=cfg['seed'],
        n_services=1)
    random_service = random_services[0]
    tray.context['I3RandomService'] = random_service

    tray.Add('I3Reader', FilenameList=[cfg['gcd'], infile])

    if run_number < cfg['det_pass2_keep_all_upto']:
        cfg['det_keep_mc_hits'] = True
        cfg['det_keep_propagated_mc_tree'] = True
        cfg['det_keep_mc_pulses'] = True

    tray.AddSegment(segments.DetectorSim, "Detector5Sim",
        RandomService='I3RandomService',
        RunID=run_id,
        GCDFile=cfg['gcd'],
        KeepMCHits=cfg['det_keep_mc_hits'],
        KeepPropagatedMCTree=cfg['det_keep_propagated_mc_tree'],
        KeepMCPulses=cfg['det_keep_mc_pulses'],
        SkipNoiseGenerator=cfg['det_skip_noise_generation'],
        LowMem=cfg['det_low_mem'],
        InputPESeriesMapName=cfg['mcpe_series_map'],
        BeaconLaunches=cfg['det_add_beacon_launches'],
        FilterTrigger=cfg['det_filter_trigger'])
    tray.AddModule("I3Writer", "EventWriter",
                   filename=outfile,
                   Streams=[icetray.I3Frame.DAQ,
                            icetray.I3Frame.Physics,
                            icetray.I3Frame.TrayInfo,
                            icetray.I3Frame.Simulation])

    tray.Execute()

    del tray


if __name__ == '__main__':
    main()
