import os
import stat
import string

import click
import yaml
import getpass

from batch_processing import create_pbs_files, create_dagman_files
from steps.utils import get_run_folder

#from batch_processing import adjust_resources


DATASET_FOLDER = '{data_folder}/{dataset_number}'
STEP_FOLDER = DATASET_FOLDER + '/{step_name}'
PREVIOUS_STEP_FOLDER = DATASET_FOLDER + '/{previous_step_name}'
PROCESSING_FOLDER = DATASET_FOLDER + '/processing/{step_name}'
SCRIPT_FOLDER = os.path.dirname(os.path.abspath(__file__))


class SafeDict(dict):
    def __missing__(self, key):
        return '{' + key + '}'


def get_attribute_from_step(config, step, attibute_map):
    if attibute_map in config:
        if step in config[attibute_map]:
            return config[attibute_map][step]
        elif 'default' in config[attibute_map]:
            return config[attibute_map]['default'].format(step=step)
        else:
            return KeyError('No step {} or default defined in {}'.format(step, attibute_map))

    return KeyError('{} not defined in config'.format(attibute_map))

def create_filename(cfg, input=False):
    if input:
        step_name = cfg['step_name']
        step = cfg['step']
        step_level_name = cfg['step_level_name']
        cfg['step_name'] = cfg['previous_step_name']
        cfg['step'] = cfg['previous_step']
        cfg['step_level_name'] = get_attribute_from_step(cfg,
                                                         cfg['previous_step'],
                                                         'step_to_level_map')
        filename = cfg['output_pattern'].format(**cfg)
        full_path = os.path.join(cfg['input_folder'], filename)
        cfg['step_name'] = step_name
        cfg['step'] = step
        cfg['step_level_name'] = step_level_name
    else:
        filename = cfg['output_pattern'].format(**cfg)
        full_path = os.path.join(cfg['output_folder'], filename)
    full_path = full_path.replace(' ', '0')
    return full_path


def write_job_files(config, step, check_existing=False,
                    run_start=None, run_stop=None):
    with open(config['job_template']) as f:
        template = f.read()
    output_base = os.path.join(config['processing_folder'], 'jobs')
    if 'name_addition' not in config.keys():
        config['name_addition'] = ''
    if not os.path.isdir(output_base):
        os.makedirs(output_base)
    log_dir = os.path.join(config['processing_folder'], 'logs')
    if not os.path.isdir(log_dir):
        os.makedirs(log_dir)
    scripts = []
    run_numbers = []

    if run_start is None:
        run_start = 0
    else:
        if run_start < 0 or run_start >= config['n_runs']:
            raise ValueError('run_start is out of range: {!r}'.format(
                                                                    run_start))
    if run_stop is None:
        run_stop = config['n_runs']
    else:
        if run_start >= run_stop or run_stop > config['n_runs']:
            raise ValueError('run_stop is out of range: {!r}'.format(run_stop))

    for i in range(run_start, run_stop):
        config['run_number'] = i
        config['run_folder'] = get_run_folder(i)
        final_out = config['outfile_pattern'].format(**config)
        final_out = final_out.replace(' ', '0')
        # change the following line if you want resume
        #final_out = final_out.replace('Level0.{}'.format(config['step']), 'Level3')
        config['final_out'] = final_out
        if check_existing:
            if os.path.isfile(config['final_out']):
                continue
        output_folder = os.path.dirname(final_out)
        if not os.path.isdir(output_folder):
            os.makedirs(output_folder)
        config['output_folder'] = output_folder
        file_config = string.Formatter().vformat(template, (), config)
        script_name = string.Formatter().vformat(
            config['script_name'], (), config)
        script_path = os.path.join(output_base, script_name)
        with open(script_path, 'w') as f:
            f.write(file_config)
        st = os.stat(script_path)
        os.chmod(script_path, st.st_mode | stat.S_IEXEC)
        scripts.append(script_path)
        run_numbers.append(i)
    return scripts, run_numbers


def build_config(data_folder, custom_settings):
    if data_folder is None:
        default = '/data/user/{}/simulation_scripts/'.format(getpass.getuser())
        data_folder = click.prompt(
            'Please enter the dir were the files should be stored:',
            default=default)
    data_folder = os.path.abspath(data_folder)
    if data_folder.endswith('/'):
        data_folder = data_folder[:-1]
    with open(custom_settings['default_config'], 'r') as stream:
        config = SafeDict(yaml.load(stream, Loader=yaml.Loader))
    config.update(custom_settings)

    config.update({'data_folder': data_folder,
                   'run_number': '{run_number:6d}',
                   'run_folder': '{run_folder}'})

    config['input_folder'] = PREVIOUS_STEP_FOLDER.format(**config)
    config['output_folder'] = STEP_FOLDER.format(**config)
    config['dataset_folder'] = DATASET_FOLDER.format(**config)
    config['processing_folder'] = PROCESSING_FOLDER.format(**config)
    config['script_folder'] = SCRIPT_FOLDER
    if not os.path.isdir(config['output_folder']):
        os.makedirs(config['output_folder'])
    if not os.path.isdir(config['processing_folder']):
        os.makedirs(config['processing_folder'])
    return config


@click.command()
@click.argument('config_file', type=click.Path(exists=True))
@click.option('--data_folder', '-d', default=None,
              help='folder were all files should be placed')
@click.option('--processing_scratch', '-p', default=None,
              help='Folder for the DAGMAN Files')
@click.option('--dagman/--no-dagman', default=False,
              help='Write/Not write files to start dagman process.')
@click.option('--pbs/--no-pbs', default=False,
              help='Write/Not write files to start processing on a pbs system')
@click.option('--step', '-s', default=1,
              help='0=upto clsim\n1 = clsim\n2 =upto L2')
@click.option('--resume/--no-resume', default=False,
              help='Resume processing -> check for existing output')
@click.option('--run_start', default=None, type=int,
              help='Only process runs starting with this number.')
@click.option('--run_stop', default=None, type=int,
              help='Only process runs up to this number.')
def main(data_folder,
         config_file,
         processing_scratch,
         step,
         pbs,
         dagman,
         resume,
         run_start,
         run_stop):
    config_file = click.format_filename(config_file)
    with open(config_file, 'r') as stream:
        custom_settings = SafeDict(yaml.load(stream, Loader=yaml.Loader))
    chain_name = custom_settings['chain_name']
    click.echo('Initialized {} chain!'.format(chain_name))
    custom_settings.update({
        'step': step,
        'step_name': get_attribute_from_step(custom_settings,
                                             step,
                                             'step_name_map'),
        'step_level_name': get_attribute_from_step(custom_settings,
                                                   step,
                                                   'step_to_level_map'),
        'previous_step_name': get_attribute_from_step(custom_settings,
                                                      step - 1,
                                                      'step_name_map'),
        'previous_step': step - 1})

    if 'outfile_pattern' in custom_settings.keys():
        click.echo('Building config for next step based on provided config!')
        config = custom_settings
        config['infile_pattern'] = config['outfile_pattern']
        step = config['step'] + 1
        config.update({
            'step': step,
            'step_name': get_attribute_from_step(custom_settings,
                                                 step,
                                                 'step_name_map'),
            'previous_step_name': get_attribute_from_step(custom_settings,
                                                          step - 1,
                                                          'step_name_map')})
        if 'processing_scratch' in config.keys():
            processing_scratch = config['processing_scratch']
    else:
        click.echo('Building config from scratch!')
        custom_settings['default_config'] = config_file
        job_template = get_attribute_from_step(custom_settings, step, 'job_template_map')
        if not os.path.isabs(job_template):
            job_template = os.path.join(SCRIPT_FOLDER, 'job_templates', job_template)
        custom_settings['job_template'] = job_template
        config = build_config(data_folder, custom_settings)
        config['infile_pattern'] = create_filename(config, input=True)

    config['processing_folder'] = PROCESSING_FOLDER.format(**config)
    config['outfile_pattern'] = create_filename(config)
    config['scratchfile_pattern'] = os.path.basename(config['outfile_pattern'])
    config['script_name'] = '{step_name}{name_addition}_{run_number}.sh'
    if not os.path.isdir(config['processing_folder']):
        os.makedirs(config['processing_folder'])

    outfile = os.path.basename(os.path.join(config_file))
    filled_yaml = os.path.join(config['processing_folder'], outfile)
    config['yaml_copy'] = filled_yaml
    with open(config['yaml_copy'], 'w') as yaml_copy:
        yaml.dump(dict(config), yaml_copy, default_flow_style=False)

    if dagman or pbs:
        if processing_scratch is None:
            default = '/scratch/{}/simulation_scripts'.format(
                getpass.getuser())
            processing_scratch = click.prompt(
                'Please enter a processing scrath:',
                default=default)
        config['processing_scratch'] = os.path.abspath(processing_scratch)

    script_files, run_numbers = write_job_files(config, step,
                                                check_existing=resume,
                                                run_start=run_start,
                                                run_stop=run_stop)

    if dagman or pbs:
        scratch_subfolder = '{dataset_number}_{step_name}'.format(**config)
        scratch_folder = os.path.join(config['processing_scratch'],
                                      scratch_subfolder)
        if not os.path.isdir(scratch_folder):
            os.makedirs(scratch_folder)
        if dagman:
            create_dagman_files(config,
                                script_files,
                                run_numbers,
                                scratch_folder)
        if pbs:
            create_pbs_files(config,
                             script_files,
                             run_numbers,
                             scratch_folder)


if __name__ == '__main__':
    main()
