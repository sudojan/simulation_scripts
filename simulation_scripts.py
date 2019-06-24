import os
import stat
import string
from glob import glob

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

def update_config_from_input_dir(cfg, input_dir):
    input_dir = os.path.abspath(input_dir)
    if not os.path.isdir(input_dir):
        raise NameError('input folder {} is not a directory'.format(input_dir))
    if input_dir.endswith('/'):
        input_dir = input_dir[:-1]
    cfg['input_folder'] = input_dir

    file_list = glob(os.path.join(input_dir, '*.i3.bz2'))
    nfiles = len(file_list)
    if nfiles == 0:
        raise ValueError('input directory ha no i3 files')
    filename = file_list[0]
    base_file_name = os.path.basename(filename)
    dirname = os.path.dirname(filename)

    # split again for dataset number and run number
    split_base_name = base_file_name.split('.')
    numbers_list = [stmp for stmp in split_base_name if stmp.isdigit() and len(stmp) == 6]
    if len(numbers_list) != 2:
        raise NameError('problems splitting file name {}'.format(tmp))
    dataset_number = int(numbers_list[0])
    # run_number = int(numbers_list[1])
    idx_list = [split_base_name.index(tmp_num) for tmp_num in numbers_list]
    # split_base_name[idx_list[0]] = '{dataset_number:6d}'
    split_base_name[idx_list[1]] = '{run_number}'
    base_file_name = '.'.join(split_base_name).replace(' ', '0')

    run_numbers = []
    # get run numbers
    for idx, file_name in enumerate(file_list):
        base_name = os.path.basename(file_name)
        split_base_name = base_name.split('.')
        run_numbers.append(int(split_base_name[idx_list[1]]))

    #split base name to change the Level
    split_base_name = base_file_name.split('_')
    for idx, split_name in enumerate(split_base_name):
        if split_name.startswith('Level'):
            previous_step_level_name = split_base_name[idx]
            split_base_name[idx] = '{step_level_name}'
            break
        else:
            if idx == len(split_base_name)-1:
                #reached end of file, no processing level in name
                split_base_name = ['{step_level_name}'] + split_base_name
    base_file_name = '_'.join(split_base_name)

    # # extract run folder
    # split_dir_name = dirname.split(os.sep)
    # for dir_folder in split_dir_name[::-1]:
    #     tmp_split = dir_folder.split('-')
    #     if len(tmp_split) == 2:
    #         if len(tmp_split[0]) == 5 and len(tmp_split[1]) == 5:
    #             if tmp_split[0].isdigit() and tmp_split[1].isdigit():
    #                 run_folder = dir_folder
    #                 break

    cfg.update({
        'dataset_number': dataset_number,
        'run_numbers': run_numbers,
        # 'run_folder': run_folder,
        })

    # cfg['output_folder'] = STEP_FOLDER.format(**config)
    # cfg['dataset_folder'] = DATASET_FOLDER.format(**config)
    cfg['output_pattern'] = str(os.path.join('{run_folder}', base_file_name))
    # config['outfile_pattern'] = os.path.join(cfg['output_folder'], cfg['output_pattern'])

    step_level_name = cfg['step_level_name']
    cfg['step_level_name'] = previous_step_level_name
    full_input_file = os.path.join(dirname, base_file_name).format(**cfg)
    cfg['step_level_name'] = step_level_name

    cfg.update({
        'infile_pattern': str(full_input_file),
        })

    return cfg



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
                    run_start=None, run_stop=None,
                    run_numbers_to_check=None):
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

    if run_numbers_to_check is None:
        if run_start is None:
            run_start = 0
        else:
            if run_start < 0 or run_start >= config['n_runs']:
                raise ValueError('run_start is out of range: {!r}'.format(run_start))
        if run_stop is None:
            run_stop = config['n_runs']
        else:
            if run_start >= run_stop or run_stop > config['n_runs']:
                raise ValueError('run_stop is out of range: {!r}'.format(run_stop))

        run_numbers_to_check = range(run_start, run_stop)

    for i in run_numbers_to_check:
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


def build_config(data_folder, custom_settings, input_folder_name=None):
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

    if input_folder_name is None:
        config['input_folder'] = PREVIOUS_STEP_FOLDER.format(**config)
    else:
        config = update_config_from_input_dir(config, input_folder_name)
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
@click.option('--input_folder_name', '-i', default=None,
              help='folder were all files are read from')
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
         input_folder_name,
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
    click.echo('Initialized processing chain!')
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
        config = build_config(data_folder, custom_settings, input_folder_name)
        if 'infile_pattern' not in config.keys():
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

    prev_run_numbers = None
    if 'run_numbers' in config.keys():
        prev_run_numbers = config['run_numbers']
    script_files, run_numbers = write_job_files(config, step,
                                                check_existing=resume,
                                                run_start=run_start,
                                                run_stop=run_stop,
                                                run_numbers_to_check=prev_run_numbers)

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
