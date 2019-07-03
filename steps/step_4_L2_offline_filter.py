#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py2-v3.1.1/icetray-start
#METAPROJECT /data/user/jsoedingrekso/ic_software/combo_173346/build
import click
import yaml

import os

from I3Tray import I3Tray
from icecube import icetray, dataclasses, dataio
from icecube.icetray import I3PacketModule, I3Units

from icecube.filterscripts.offlineL2.level2_all_filters import OfflineFilter
from icecube.filterscripts.offlineL2 import SpecialWriter

from utils import get_run_folder


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
    """The main L2 script"""
    tray.AddModule('I3Reader',
                   'i3 reader',
                   FilenameList=[cfg['gcd'], infile])

    tray.AddSegment(OfflineFilter,
                    "OfflineFilter",
                    dstfile=None,
                    mc=True,
                    doNotQify=True,
                    photonicsdir=cfg['photon_tables_dir'])

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
