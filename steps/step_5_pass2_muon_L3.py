#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py2-v3.1.1/icetray-start
#METAPROJECT /data/user/jsoedingrekso/ic_software/combo_173346/build
import os

import click
import yaml

from utils import get_run_folder

from I3Tray import I3Tray
from icecube import icetray, dataio, dataclasses, hdfwriter, phys_services

from icecube.level3_filter_muon.MuonL3TraySegment import MuonL3


@click.command()
@click.argument('cfg', type=click.Path(exists=True))
@click.argument('run_number', type=int)
@click.option('--scratch/--no-scratch', default=True)
def main(cfg, run_number, scratch):
    with open(cfg, 'r') as stream:
        cfg = yaml.load(stream, Loader=yaml.Loader)
    icetray.logging.set_level("WARN")
    cfg['run_number'] = run_number
    cfg['run_folder'] = get_run_folder(run_number)
    infile = cfg['infile_pattern'].format(**cfg)
    infile = infile.replace(' ', '0')

    if scratch:
        outfile = cfg['scratchfile_pattern'].format(**cfg)
    else:
        outfile = cfg['outfile_pattern'].format(**cfg)
    outfile = outfile.replace(' ', '0')

    photonics_dir = os.path.join(cfg['photon_tables_dir'], 'SPICEMie')
    photonics_driver_dir = os.path.join(photonics_dir, 'driverfiles')


    tray = I3Tray()
    """The main L3 script"""
    tray.AddSegment(
        MuonL3,
        gcdfile=cfg['gcd'],
        infiles=infile,
        output_i3=outfile,
        output_hd5="",
        output_root="",
        photonicsdir=photonics_dir,
        photonicsdriverdir=photonics_driver_dir,
        photonicsdriverfile='mu_photorec.list',
        infmuonampsplinepath=os.path.join(cfg['spline_table_dir'], cfg['mu_amplitude_spline_table']),
        infmuonprobsplinepath=os.path.join(cfg['spline_table_dir'], cfg['mu_timing_spline_table']),
        cascadeampsplinepath=os.path.join(cfg['spline_table_dir'], cfg['cascade_amplitude_spline_table']),
        cascadeprobsplinepath=os.path.join(cfg['spline_table_dir'], cfg['cascade_timing_spline_table']),
        restore_timewindow_forMC=True)

    tray.Execute()

    del tray

if __name__ == '__main__':
    main()
