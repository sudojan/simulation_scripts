#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py2-v3.1.1/icetray-start
#METAPROJECT /data/user/jsoedingrekso/ic_software/combo_173346/build
import click
import yaml

import numpy as np
from scipy.spatial import ConvexHull

from icecube.simprod import segments

from I3Tray import I3Tray, I3Units
from icecube import icetray, dataclasses
from icecube import sim_services, MuonGun
import os
import sys
file_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(file_dir + '/..')
from utils import create_random_services, get_run_folder
from dom_distance_cut import OversizeSplitterNSplits, generate_stream_object
from resources.geometry import get_intersections


def create_muon(azimuth_range=[0, 360],
                zenith_range=[0, 180],
                energy_range=[10000, 10000],
                anchor_time_range=[9000, 12000],
                anchor_x_range=[-400, 400],
                anchor_y_range=[-400, 400],
                anchor_z_range=[-400, 400],
                length_to_go_back=2000,
                convex_hull=None,
                extend_past_hull=0.,
                random_service=None,
                ):
    '''
    Generates muon where energy, direction and position is
    uniformly sampled within given range.

    First samples direction and anchor point. Then calculates
    the vertex by going back along the track from the anchor
    point. The distance how far to go back along the track is
    given by length_to_go_back.

    If a convex_hull is given, length_to_go_back is ignored
    and instead, the intersection point with the convex hull
    will be used as a vertex. Optionally, the vertex can be
    moved further out. This is defined by extend_past_hull.

    azimuth_range: [min, max]
                   in degree

    zenith_range: [min, max]
                   in degree

    energy_range: [min, max]
                   in GeV

    anchor_time_range: [min, max]
                   in ns
                Approximate time when
                muon is in detector at
                the simulated anchor
                point

    anchor_i_range: [min, max]
                    in m
                The anchor point coordinate i.
                Anchor point must be inside
                convex hull if given.

    length_to_go_back: float
                in m
                Length to go back along track from
                anchor point, e.g. how far away
                to set the vertex of the point

    convex_hull : scipy.spatial.ConvexHull
                 defining the desired convex volume

    extend_past_hull: float
                in m
                Length to extend past convex hull

    random_service: random number service
    '''

    # ------
    # sample direction and energy
    # ------
    azimuth = random_service.uniform(*azimuth_range) * I3Units.deg
    zenith = random_service.uniform(*zenith_range) * I3Units.deg
    energy = random_service.uniform(*energy_range) * I3Units.GeV

    # create particle
    muon = dataclasses.I3Particle()
    muon.speed = dataclasses.I3Constants.c
    muon.location_type = dataclasses.I3Particle.LocationType.InIce
    muon.type = dataclasses.I3Particle.ParticleType.MuMinus
    muon.dir = dataclasses.I3Direction(zenith, azimuth)
    muon.energy = energy * I3Units.GeV

    # ------
    # get anchor point and time in detector
    # ------
    anchor_x = random_service.uniform(*anchor_x_range)
    anchor_y = random_service.uniform(*anchor_y_range)
    anchor_z = random_service.uniform(*anchor_z_range)

    anchor = dataclasses.I3Position(
                    anchor_x * I3Units.m,
                    anchor_y * I3Units.m,
                    anchor_z * I3Units.m)

    anchor_time = random_service.uniform(*anchor_time_range) * I3Units.ns

    # ------
    # calculate vertex
    # ------
    if convex_hull is not None:
        t_s = get_intersections(convex_hull,
                                v_pos=(anchor_x,
                                       anchor_y,
                                       anchor_z),
                                v_dir=(muon.dir.x,
                                       muon.dir.y,
                                       muon.dir.z),
                                eps=1e-4)

        length_to_go_back = - t_s[t_s <= 0.0]
        assert len(length_to_go_back) == 1, \
            'Is anchor point within convex_hull?'

        length_to_go_back = length_to_go_back[0]

        # extend past convex hull
        length_to_go_back += extend_past_hull

    vertex = anchor - length_to_go_back*I3Units.m * muon.dir
    travel_time = length_to_go_back * I3Units.m / muon.speed
    vertex_time = anchor_time - travel_time * I3Units.ns

    muon.pos = vertex
    muon.time = vertex_time * I3Units.ns

    return muon


class ParticleMultiplier(icetray.I3ConditionalModule):
    def __init__(self, context):
        icetray.I3ConditionalModule.__init__(self, context)
        self.AddParameter('num_events', '', None)
        self.AddParameter('primary', '', None)

    def Configure(self):
        self.num_events = self.GetParameter('num_events')
        self.primary = self.GetParameter('primary')

        self.events_done = 0

    def DAQ(self, frame):

        # Fill primary into an MCTree
        mctree = dataclasses.I3MCTree()
        mctree.add_primary(self.primary)

        frame["I3MCTree_preMuonProp"] = mctree
        self.PushFrame(frame)

        self.events_done += 1
        if self.events_done >= self.num_events:
            self.RequestSuspension()


@click.command()
@click.argument('cfg', type=click.Path(exists=True))
@click.argument('run_number', type=int)
@click.option('--scratch/--no-scratch', default=True)
def main(cfg, run_number, scratch):
    with open(cfg, 'r') as stream:
        cfg = yaml.load(stream, Loader=yaml.Loader)
    cfg['run_number'] = run_number
    cfg['run_folder'] = get_run_folder(run_number)

    if scratch:
        outfile = cfg['scratchfile_pattern'].format(**cfg)
    else:
        outfile = cfg['outfile_pattern'].format(**cfg)
    outfile = outfile.replace(' ', '0')

    if cfg['distance_splits'] is not None:
        click.echo('SplittingDistances: {}'.format(cfg['distance_splits']))
        click.echo('Oversizefactors: {}'.format(cfg['oversize_factors']))
    click.echo('NEvents: {}'.format(cfg['n_events_per_run']))
    click.echo('EMin: {}'.format(cfg['e_min']))
    click.echo('EMax: {}'.format(cfg['e_max']))
    click.echo('EBreak: {}'.format(cfg['muongun_e_break']))
    click.echo('Gamma: {}'.format(cfg['gamma']))
    click.echo('ZenithMin: {}'.format(cfg['zenith_min']))
    click.echo('ZenithMax: {}'.format(cfg['zenith_max']))


    # create convex hull
    if 'use_convex_hull' in cfg and cfg['use_convex_hull']:

        # hardcode icecube corner points
        # ToDo: read from geometry file
        points = [
           [-570.90002441, -125.13999939, 501],  # string 31
           [-256.14001465, -521.08001709, 501],  # string 1
           [ 361.        , -422.82998657, 501],  # string 6
           [ 576.36999512,  170.91999817, 501],  # string 50
           [ 338.44000244,  463.72000122, 501],  # string 74
           [ 101.04000092,  412.79000854, 501],  # string 72
           [  22.11000061,  509.5       , 501],  # string 78
           [-347.88000488,  451.51998901, 501],  # string 75

           [-570.90002441, -125.13999939, -502],  # string 31
           [-256.14001465, -521.08001709, -502],  # string 1
           [ 361.        , -422.82998657, -502],  # string 6
           [ 576.36999512,  170.91999817, -502],  # string 50
           [ 338.44000244,  463.72000122, -502],  # string 74
           [ 101.04000092,  412.79000854, -502],  # string 72
           [  22.11000061,  509.5       , -502],  # string 78
           [-347.88000488,  451.51998901, -502],  # string 75
            ]
        convex_hull = ConvexHull(points)
    else:
        convex_hull = None

    if 'extend_past_hull' not in cfg:
        cfg['extend_past_hull'] = 0.0

    random_services, _ = create_random_services(
        dataset_number=cfg['dataset_number'],
        run_number=cfg['run_number'],
        seed=cfg['seed'],
        n_services=2)

    random_service, random_service_prop = random_services

    # create muon
    muon = create_muon(
            azimuth_range=[cfg['azimuth_min'], cfg['azimuth_max']],
            zenith_range=[cfg['zenith_min'], cfg['zenith_max']],
            energy_range=[cfg['e_min'], cfg['e_max']],
            anchor_time_range=cfg['anchor_time_range'],
            anchor_x_range=cfg['anchor_x_range'],
            anchor_y_range=cfg['anchor_y_range'],
            anchor_z_range=cfg['anchor_z_range'],
            length_to_go_back=cfg['length_to_go_back'],
            convex_hull=convex_hull,
            extend_past_hull=cfg['extend_past_hull'],
            random_service=random_services[0],
            )


    tray = I3Tray()

    tray.context['I3RandomService'] = random_service

    tray.AddModule("I3InfiniteSource",
                   "TheSource",
                   Prefix=cfg['gcd'],
                   Stream=icetray.I3Frame.DAQ)

    if cfg['MuonGenerator'] == 'MuonGunSinglemuons':
        tray.AddSegment(
            segments.GenerateSingleMuons,
            "GenerateCosmicRayMuons",
            NumEvents=cfg['n_events_per_run'],
            FromEnergy=cfg['e_min'] * icetray.I3Units.GeV,
            ToEnergy=cfg['e_max'] * icetray.I3Units.GeV,
            BreakEnergy=cfg['muongun_e_break'] * icetray.I3Units.GeV,
            GammaIndex=cfg['gamma'],
            ZenithRange=[cfg['zenith_min'] * icetray.I3Units.deg,
                         cfg['zenith_max'] * icetray.I3Units.deg])

    elif cfg['MuonGenerator'] == 'MuonGunGeneral':
        model = MuonGun.load_model(cfg['muongun_model'])
        model.flux.min_multiplicity = cfg['muongun_min_multiplicity']
        model.flux.max_multiplicity = cfg['muongun_max_multiplicity']
        spectrum = MuonGun.OffsetPowerLaw(cfg['gamma'],
                                          cfg['e_min']*icetray.I3Units.GeV,
                                          cfg['e_min']*icetray.I3Units.GeV,
                                          cfg['e_max']*icetray.I3Units.GeV)
        surface = MuonGun.Cylinder(1600, 800,
                                   dataclasses.I3Position(31.25, 19.64, 0))

        if cfg['muongun_generator'] == 'energy':
            scale = MuonGun.BasicSurfaceScalingFunction()
            scale.SetSideScaling(4., 17266, 3.41, 1.74)
            scale.SetCapScaling(4., 23710, 3.40, 1.88)
            generator = MuonGun.EnergyDependentSurfaceInjector(surface,
                                                               model.flux,
                                                               spectrum,
                                                               model.radius,
                                                               scale)
        elif cfg['muongun_generator'] == 'static':
            generator = MuonGun.StaticSurfaceInjector(surface,
                                                      model.flux,
                                                      spectrum,
                                                      model.radius)
        elif cfg['muongun_generator'] == 'floodlight':
            generator = MuonGun.Floodlight(
                                    surface=surface,
                                    energyGenerator=spectrum,
                                    cosMin=cfg['muongun_floodlight_min_cos'],
                                    cosMax=cfg['muongun_floodlight_max_cos'],
                                    )
        else:
            err_msg = 'MuonGun generator {} is not known.'
            err_msg += " Must be 'energy','static' or 'floodlight"
            raise ValueError(err_msg.format(cfg['muongun_generator']))

        tray.Add(MuonGun.segments.GenerateBundles, 'MuonGenerator',
                 Generator=generator,
                 NEvents=cfg['n_events_per_run'],
                 GCDFile=cfg['gcd'])

        tray.Add("Rename", keys=["I3MCTree", "I3MCTree_preMuonProp"])

    elif cfg['MuonGenerator'] == 'MuonResimulation':
        tray.AddModule(ParticleMultiplier,
                       'make_particles',
                       num_events=cfg['n_events_per_run'],
                       primary=muon)
    else:
        err_msg = 'MuonGenerator {} is not known.'
        err_msg += " Must be 'MuonGunSinglemuons','MuonGunGeneral' or 'MuonResimulation"
        raise ValueError(err_msg.format(cfg['MuonGenerator']))


    # --------------------------------------
    # Propagate Muons
    # --------------------------------------
    tray.AddSegment(
        segments.PropagateMuons,
        "PropagateMuons",
        RandomService=random_service_prop,
        **cfg['muon_propagation_config'])


    # --------------------------------------
    # Distance Splits
    # --------------------------------------
    if cfg['distance_splits'] is not None:
        click.echo('SplittingDistance: {}'.format(
            cfg['distance_splits']))
        distance_splits = np.atleast_1d(cfg['distance_splits'])
        dom_limits = np.atleast_1d(cfg['threshold_doms'])
        if len(dom_limits) == 1:
            dom_limits = np.ones_like(distance_splits) * cfg['threshold_doms']
        oversize_factors = np.atleast_1d(cfg['oversize_factors'])
        order = np.argsort(distance_splits)

        distance_splits = distance_splits[order]
        dom_limits = dom_limits[order]
        oversize_factors = oversize_factors[order]

        stream_objects = generate_stream_object(distance_splits,
                                                dom_limits,
                                                oversize_factors)
        tray.AddModule(OversizeSplitterNSplits,
                       "OversizeSplitterNSplits",
                       thresholds=distance_splits,
                       thresholds_doms=dom_limits,
                       oversize_factors=oversize_factors)
        for stream_i in stream_objects:
            outfile_i = stream_i.transform_filepath(outfile)
            tray.AddModule("I3Writer",
                           "writer_{}".format(stream_i.stream_name),
                           Filename=outfile_i,
                           Streams=[icetray.I3Frame.DAQ,
                                    icetray.I3Frame.Physics,
                                    icetray.I3Frame.Stream('S'),
                                    icetray.I3Frame.Stream('M')],
                           If=stream_i)
            click.echo('Output ({}): {}'.format(stream_i.stream_name,
                                                outfile_i))
    else:
        click.echo('Output: {}'.format(outfile))
        tray.AddModule("I3Writer", "writer",
                       Filename=outfile,
                       Streams=[icetray.I3Frame.DAQ,
                                icetray.I3Frame.Physics,
                                icetray.I3Frame.Stream('S'),
                                icetray.I3Frame.Stream('M')])

    click.echo('Scratch: {}'.format(scratch))

    tray.Execute()

    del tray


if __name__ == '__main__':
    main()
