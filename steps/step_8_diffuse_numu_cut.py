#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py2-v3.1.1/icetray-start
#METAPROJECT /data/user/jsoedingrekso/ic_software/combo_173730/build
import click
import yaml

import os

from I3Tray import I3Tray
from icecube import icetray, dataclasses, dataio
from icecube.icetray import I3PacketModule, I3Units

from icecube.finallevel_filter_diffusenumu import level4, level5
from ConfigParser import ConfigParser

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

    parser = ConfigParser()
    parser.readfp(open(os.path.join(os.path.expandvars('$I3_BUILD'),
                                    'lib/icecube/finallevel_filter_diffusenumu',
                                    'paths.cfg')))
    paths = dict(parser.items("main"))

    # Check if somebody messed with the tables
    ret = os.system("md5sum -c {}".format(
        os.path.join(os.path.expandvars('$I3_BUILD'),
                     'lib/icecube/finallevel_filter_diffusenumu',
                     'checksums')))
    if ret != 0:
        raise RuntimeError("Tables are corrupt")

    tray = I3Tray()

    tray.Add(level4.IC12L4,
             gcdfile=cfg['gcd'],
             infiles=infile,
             table_paths=paths,
             is_numu=cfg['aachen_diffuse_numu_isNuMu'])

    tray.Add(level5.segments.Scorer, "doLevel5",
        CutFunc=level5.segments.CutFunc,
        CascCut=0.5)

    tray.Add(level5.segments.millipede_segment, "MillipedeLosses", table_paths=paths)
    tray.Add(level5.segments.paraboloid_segment, "Paraboloid", table_paths=paths)

    # write output
    tray.AddModule("I3Writer", "EventWriter",
                   filename=outfile,
                   Streams=[icetray.I3Frame.DAQ,
                            icetray.I3Frame.Physics,
                            icetray.I3Frame.TrayInfo,
                            icetray.I3Frame.Simulation],
                   DropOrphanStreams=[icetray.I3Frame.DAQ])

    tray.Execute()

    usagemap = tray.Usage()

    for mod in usagemap:
        print(mod)



if __name__ == '__main__':
    main()
