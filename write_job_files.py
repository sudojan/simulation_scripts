import os
import stat

import click
import yaml
import getpass

from batch_processing import create_pbs_files, create_dagman_files


DATASET_FOLDER = '{base_dir}/{generator}/{dataset_number}'
STEP_FOLDER = DATASET_FOLDER + '/{step}'
JOB_FOLDER = DATASET_FOLDER + '/jobs/{step}'
SCRIPT_FOLDER = os.path.dirname(os.path.abspath(__file__))

step_enum = {-1: None,
              0: '0_after_proposal',
              1: '0_after_clsim',
              2: '2',
              3: '3'}

def create_filename(cfg, input=False):
    if input:
        filename = ('Level{previous_step}.IC86.YEAR.{generator}.' +
            '{dataset_number:6d}.{run_number}.i3.bz2').format(**cfg)
    else:
        filename = ('Level{step}.IC86.YEAR.{generator}.' +
            '{dataset_number:6d}.{run_number}.i3.bz2').format(**cfg)
    full_path = os.path.join(cfg['output_folder'], filename)
    full_path = full_path.replace(' ', '0')
    return full_path

def write_job_files(config, step):
    with open(os.path.join(SCRIPT_FOLDER, 'job_template.sh')) as f:
        template = f.read()
    fill_dict = {'job_file_folder': config['job_file_folder'],
                 'step_number': config['step_number'],
                 'script_folder': config['script_folder'],
                 'PBS_JOBID': '{PBS_JOBID}',
                 'CLUSTER': '{CLUSTER}',
                 'yaml_copy': config['yaml_copy']}
    output_base = os.path.join(config['job_file_folder'], 'jobs')

    if not os.path.isdir(output_base):
        os.makedirs(output_base)
    log_dir = os.path.join(config['job_file_folder'], 'logs')
    if not os.path.isdir(log_dir):
        os.makedirs(log_dir)
    scripts = []
    for i in range(config['n_runs']):
        final_out = config['outfile_pattern'].format(run_number=i)
        final_out = final_out.replace(' ', '0')
        fill_dict['final_out'] = final_out
        scratch_out = config['scratchfile_pattern'].format(run_number=i)
        scratch_out = scratch_out.replace(' ', '0')
        fill_dict['scratch_out'] = scratch_out
        fill_dict['run_number'] = i
        file_config = template.format(**fill_dict)
        scipt_name = 'step_{step_number}_run_{run_number}.sh'.format(
            **fill_dict)
        script_path = os.path.join(output_base, scipt_name)
        with open(script_path, 'w') as f:
            f.write(file_config)
        st = os.stat(script_path)
        os.chmod(script_path, st.st_mode | stat.S_IEXEC)

        scripts.append(script_path)
    return scripts


@click.command()
@click.argument('config_file', click.Path(exists=True))
@click.option('--base_dir', '-b', default=None,
              help='folder were all files should be placed')
@click.option('--dagman_scratch', '-d', default=None,
              help='Folder for the DAGMAN Files')
@click.option('--dagman/--no-dagman', default=True)
@click.option('--pbs/--no-pbs', default=True)
@click.option('--step', '-s', default=1,
              help='0=upto clsim\n1 = clsim\n2 =upto L2')
def main(base_dir, config_file, step, pbs, dagman, dagman_scratch):
    if base_dir is None:
        default = '/data/user/{}/simulation_scripts/'.format(getpass.getuser())
        base_dir = click.prompt(
            'Please enter the dir were the files should be stored',
            default=default)
        if base_dir.endswith('/'):
            base_dir = base_dir[:-1]

    config_file = click.format_filename(config_file)
    with open(config_file, 'r') as stream:
        config = yaml.load(stream)
    config.update({'step': step_enum[step],
                   'base_dir': str(base_dir)})
    config['e_min'] = float(config['e_min'])
    config['e_max'] = float(config['e_max'])
    config['muongun_e_break'] = float(config['muongun_e_break'])
    config['n_events_per_run'] = int(config['n_events_per_run'])
    config['output_folder'] = STEP_FOLDER.format(**config)
    config['dataset_folder'] = DATASET_FOLDER.format(**config)
    config['job_file_folder'] = JOB_FOLDER.format(**config)
    config['previous_step'] = step_enum[step - 1]
    config['run_number'] = '{run_number:6d}'
    config['infile_pattern'] = create_filename(config, input=True)
    config['outfile_pattern'] = create_filename(config)
    config['scratchfile_pattern'] = os.path.basename(config['outfile_pattern'])
    config['step_number'] = step
    config['script_folder'] = SCRIPT_FOLDER
    scratch_subfolder = '{dataset_number}_level{step}'.format(**config)

    if dagman and dagman_scratch is None:
        default = '/scratch/{}/simulation_scripts'.format(getpass.getuser())
        dagman_scratch = click.prompt(
                    'Please enter the dir were the files should be stored',
                    default=default)
    if dagman:
        config['dagman_scratch'] = os.path.join(dagman_scratch,
                                                scratch_subfolder)
        if not os.path.isdir(config['dagman_scratch']):
            os.makedirs(config['dagman_scratch'])
    outfile = os.path.basename(os.path.join(config_file))
    raw_filename = os.path.splitext(outfile)[0]
    filled_yaml = '{}_{}.yaml'.format(raw_filename, config['step'])
    if not os.path.isdir(config['output_folder']):
        os.makedirs(config['output_folder'])
    if not os.path.isdir(config['job_file_folder']):
        os.makedirs(config['job_file_folder'])
    filled_yaml = os.path.join(config['job_file_folder'], filled_yaml)
    with open(filled_yaml, 'w') as yaml_copy:
        yaml.dump(config, yaml_copy, default_flow_style=False)
    config['yaml_copy'] = filled_yaml
    script_files = write_job_files(config, step)
    if dagman:
        create_dagman_files(config, script_files)
    if pbs:
        create_pbs_files(config,
                         script_files)

if __name__ == '__main__':
    main()
