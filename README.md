# simulation_scripts

## Usage

```
$ python simulation_scripts.py -s "step" "path_to_config_yaml"
```

or if installed with pip:
```
$ simulation_scripts_write -s "step" "path_to_config_yaml"
```
check --help for options.

The steps till 5 are the default IceCube processing for muons.
The further steps are analysis specific, here adding Deep Learning attributes.

### Example
```
simulation_scripts_write -s 0 ~/simulation_scripts/configs/11300.yaml 
```

### now to generate the MCs for the muongun cross section

#### before simulating

- build your own combo in `/data/user` (don't forget to remove steamshovel before building combo) and link to it in the shebang lines in the first lines of the scripts in steps. Be careful and use the same software version (e.g. py2-v3.1.1) during building also in the shebang lines and the job_template
- create your own `PYTHONPATH` and `PYTHONUSERBASE` and install click, because this is not in the software packages of `pys-v3.1.1` (in case you're not using py2-v3.1.1, but an older version, there is no pyyaml preinstalled, so this also has to be done)

```
pip install --install-option="--prefix=${HOME}/software/python_libs" click
```

- get the simulation scripts (clone this repo) and make the scripts in 
first make the scripts in `steps/` executable for others

```
chmod -R ugo+rwx steps/
```

#### test the simulation scripts

to test the script first create the bash scripts executed if there would be a job send to the cluster
(of course everything in `/scratch` ;)
```
python simulation_scripts.py configs/muongun_singlemuons.yaml -s 0 -d /scratch/jsoedingrekso/tmp_out
```
and execute a jobfile script, eg.
```
/scratch/jsoedingrekso/tmp_out/1904/processing/step_0_general_muongun/jobs/step_general_muongun.sh
```
for further steps, just change the step number and adapt the path for the step.

There is one exception, that is the photon propagation.
Because on the cluster one should use GPUs, but there are no GPUs in __cobald__, one should change this to false, just for testing.

The rest is just changing the step numbers and paths.

#### run simulation on cluster

to submit jobs first go to 
```
ssh submitter
```
again create the job files, now with __dagman__ (and of course write log files to `/scratch` ;)
```
python simulation_scripts.py configs/muongun_singlemuons.yaml -s 0 -d /data/user/jsoedingrekso/muongun_crosssections/ -p /scratch/jsoedingrekso/muongun_crosssections --dagman
```
and send the jobs to the cluster, combined into a single submitted job
```
/scratch/jsoedingrekso/muongun_crosssections/1904_step_0_muongun_singlemuons_py3v4/start_dagman.sh
```
thats it.

#### test simulation scripts on cluster

Before sending the files to the cluster, one might just test a small subset, if everything is fine, eg. the first 5 files and see if things run. Therefore just change the `dagman.options` file.
```
cd /scratch/jsoedingrekso/muongun_crosssections/1904_step_0_muongun_singlemuons_py3v4/
cp dagman.options dagman.options_test
vim dagman.options_test
10jdG:wq
vim start_dagman.sh
A_test:wq
./start_dagman.sh
```

#### resume processing for crashed jobs

If files crashed during processing on the cluster, there is an option to `--resume` the processing and just process the files, that have been crashed.
So ( after fixing the bug ;) just type
```
python simulation_scripts.py configs/muongun_singlemuons.yaml -s 0 -d /data/user/jsoedingrekso/muongun_crosssections/ -p /scratch/jsoedingrekso/muongun_crosssections_resume --dagman --resume
```
the processing folder should be different to the previous processed one.

#### additional steps, not IceCube default

Every step after 5 is analysis specific.
For these steps install the python package `ic3-labels`
```
git clone https://github.com/mhuen/ic3-labels.git
cd ic3-labels
pip install --prefix=${HOME}/software/python_libs -e .
```
