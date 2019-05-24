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

### Example
```
simulation_scripts_write -s 0 ~/simulation_scripts/configs/11300.yaml 
```

### now to generat the MCs for the muongun cross section

##### before simulating

- build your own combo in `/data/user` and link to it in the shebang lines of the scripts in steps. Be careful and use the same software version (e.g. py2-v3.1.1) during building also in the shebang lines and the job_template
- create your own PYTHONPATH/PYTHONUSERBASE and install click, because this is not in the software packages of `pys-v3.1.1` (in case you're not using py2-v3.1.1, but an older version, there is no pyyaml preinstalled, so this also has to be done)

```
pip install --install-option="--prefix=${HOME}/software/python_libs" click
```

- get the simulation scripts (clone this repo) and make the scripts in 
first make the scripts in `steps/` executable for others
```
chmod -R ugo+rwx steps/
```

to test the script
```
python simulation_scripts.py configs/muongun_singlemuons.yaml -s 0 -d /scratch/jsoedingrekso/tmp_out
```

to finally generate muons and propagate them
```
python simulation_scripts.py configs/muongun_singlemuons.yaml -s 0 -d /data/user/jsoedingrekso/muongun_crosssections/ -p /scratch/jsoedingrekso/muongun_crosssections --dagman
```