#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py2-v3.1.1/icetray-start
#METAPROJECT /data/user/jsoedingrekso/ic_software/combo_173346/build

import os

import click
import yaml

from utils import get_run_folder

from I3Tray import I3Tray
from icecube import icetray, dataio, dataclasses, hdfwriter, phys_services
from icecube import MuonGun
from ic3_labels.labels import modules


# this module is a copy from kkrings
# ---Tray module---------------------------------------------------------------
class MuonRemoveChildren(icetray.I3ConditionalModule):
    r"""Remove children from MC tree.

    This tray module removes the children that are outside of the given
    detector volume of all muon tracks that are saved in
    the ``MMCTrackList`` from the ``I3MCTree``.

    """
    @property
    def detector(self):
        r"""SamplingSurface: Detector volume"""
        return self._detector

    @property
    def output(self):
        r"""str: Name for cleaned MC tree"""
        return self._output

    def __init__(self, context):
        super(type(self), self).__init__(context)

        self._detector = None
        self.AddParameter(
            "Detector",
            type(self).detector.__doc__,
            self._detector)

        self._output = "I3MCTree_cleaned"
        self.AddParameter(
            "Output",
            type(self).output.__doc__,
            self._output)

        self.AddOutBox("OutBox")

    def Configure(self):
        self._detector = self.GetParameter("Detector")
        self._output = self.GetParameter("Output")

    def DAQ(self, frame):
        mctree = frame["I3MCTree"]
        tracks = frame["MMCTrackList"]

        for track in tracks:
            parent = mctree.get_particle(track.particle)
            mctree = self.remove(mctree, track, parent, self._detector)

        frame.Delete('I3MCTree')
        frame[self._output] = mctree

        self.PushFrame(frame)

    @staticmethod
    def remove(mctree, track, parent, detector):
        r"""Remove children from MC tree.

        Remove the children of the given parent particle from the given
        MC tree that are outside of the given detector volume.

        Parameters
        ----------
        mctree : I3MCTree
            MC tree
        track : MMCTrack
            MMC Track to retrieve entry/exit time
        parent : I3Particle
            Parent particle
        detector : SamplingSurface
            Detector volume

        Returns
        -------
        I3MCTree
            Cleaned MC tree

        """
        cleanedtree = dataclasses.I3MCTree(mctree)

        daughters = mctree.get_daughters(parent)
        for daughter in daughters:
            #intersections = detector.intersection(daughter.pos, parent.dir)

            #if intersections.first * intersections.second > 0.:
            if daughter.time < track.ti or daughter.time > track.tf:
                cleanedtree.erase(daughter)

        return cleanedtree


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


    detectorsurface = MuonGun.Cylinder(
        length=1600.*icetray.I3Units.m,
        radius=800.*icetray.I3Units.m,
        center=dataclasses.I3Position(0.*icetray.I3Units.m,
                                      0.*icetray.I3Units.m,
                                      0.*icetray.I3Units.m,))

    # filter secondaries that are not in detector volume
    tray.AddModule(MuonRemoveChildren, 'MuonRemoveChildren',
                   Detector=detectorsurface,
                   Output='I3MCTree')


    #--------------------------------------------------
    # Add MC Labels mirco trained his DNN on
    #--------------------------------------------------
    add_cascade_labels = False
    if add_cascade_labels:
        tray.AddModule(modules.MCLabelsCascades, 'MCLabelsCascade',
                       PulseMapString='InIcePulses',
                       PrimaryKey='MCPrimary1',
                       OutputKey='LabelsDeepLearning')
    else:
        tray.AddModule(modules.MCLabelsDeepLearning, 'MCLabelsDeepLearning',
                       PulseMapString='InIcePulses',
                       PrimaryKey='MCPrimary1',
                       MCPESeriesMapName=cfg['mcpe_series_map'],
                       OutputKey='LabelsDeepLearning',
                       IsMuonGun=True)


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
