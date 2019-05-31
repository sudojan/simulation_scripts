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
    infile = infile.replace('Level0.{}'.format(cfg['previous_step']),
                            'Level2')
    infile = infile.replace('2012_pass2', 'pass2')

    if scratch:
        outfile = cfg['scratchfile_pattern'].format(**cfg)
    else:
        outfile = cfg['outfile_pattern'].format(**cfg)
    outfile = outfile.replace('Level0.{}'.format(cfg['step']),
                            'Level3')
    outfile = outfile.replace(' ', '0')
    outfile = outfile.replace('2012_pass2', 'pass2')
    print('Outfile != $FINAL_OUT clean up for crashed scripts not possible!')

    tray = I3Tray()

    photonics_dir = os.path.join(cfg['photon_tables_dir'], 'SPICEMie')
    photonics_driver_dir = os.path.join(photonics_dir, 'driverfiles')

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


    tray.AddModule("TrashCan", "Bye")
    tray.Execute()
    tray.Finish()

if __name__ == '__main__':
    main()
