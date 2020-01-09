#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py2-v3.1.1/icetray-start
#METAPROJECT /data/user/jsoedingrekso/ic_software/combo_173346/build
import click
import yaml

import numpy as np

from icecube.simprod import segments

from I3Tray import I3Tray
from icecube import icetray, dataclasses
from icecube import sim_services

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

    tray.Add('I3Reader', FilenameList=[cfg['gcd'], infile])

    # random_services, _ = create_random_services(
    #     dataset_number=cfg['dataset_number'],
    #     run_number=cfg['run_number'],
    #     seed=cfg['seed'],
    #     n_services=2)

    # random_service, random_service_prop = random_services
    # tray.context['I3RandomService'] = random_service

    # --------------------------------------
    # Propagate Muons
    # --------------------------------------
    tray.AddSegment(
        segments.PropagateMuons,
        "PropagateMuons",
        RandomService=random_service_prop,
        **cfg['muon_propagation_config'])


    click.echo('Output: {}'.format(outfile))
    tray.AddModule("I3Writer", "writer",
                   Filename=outfile,
                   Streams=[icetray.I3Frame.DAQ,
                            icetray.I3Frame.Physics,
                            icetray.I3Frame.Stream('S'),
                            icetray.I3Frame.Stream('M')])

    click.echo('Scratch: {}'.format(scratch))
    tray.Execute()


if __name__ == '__main__':
    main()
