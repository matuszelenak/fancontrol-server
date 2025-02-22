import json
import os
import re
import subprocess
import time
from typing import Literal
import logging

logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')


def gather_pwm_paths():
    for hwmon in os.listdir('/sys/class/hwmon'):
        for file_name in os.listdir(os.path.join('/sys/class/hwmon', hwmon)):
            if re.match(r'^pwm[0-9]+$', file_name):
                pth = os.path.join('/sys/class/hwmon', hwmon, file_name)
                logger.warning(f'Found PWM fan at {pth}')
                yield pth


def load_fan_configuration():
    with open('/root/fancontrol-server/fan_config.json', 'r') as f:
        fan_configuration = json.load(f)

    fan_pwm_paths = list(gather_pwm_paths())
    for pwm_regex, cfg in fan_configuration.items():
        matches = (pwm_path for pwm_path in fan_pwm_paths if re.search(re.compile(pwm_regex), pwm_path))
        matching_pwm_path = next(matches, None)

        if matching_pwm_path:
            # Fill up the curve
            cfg['curve'] = {int(k): v for k,v in cfg['curve'].items()}
            sorted_temp_keys = sorted([int(x) for x in cfg['curve'].keys()])
            full_curve = {}
            for temp_from, temp_to in zip(sorted_temp_keys, sorted_temp_keys[1:]):

                m = (cfg['curve'][temp_to] - cfg['curve'][temp_from]) / (temp_to - temp_from)
                b = cfg['curve'][temp_from] - m * temp_from

                for temp in range(temp_from, temp_to):
                    full_curve[temp] = int(m * temp + b)

            cfg['curve'] = full_curve
            cfg['pwm_path'] = matching_pwm_path

            yield cfg


def set_fan_speed(hwmon_path: str, speed: int):
    with open(f'{hwmon_path}_enable', 'w') as f:
        f.write('1')
    with open(hwmon_path, 'r') as f:
        current_speed = int(f.read().strip())

    if current_speed != speed:
        logger.warning(f'Setting {hwmon_path} to {speed}')
        with open(hwmon_path, 'w') as f:
            f.write(f'{speed}')


def get_gpu_parameter(vm_id, parameter: Literal['temperature', 'fan_speed']):
    if parameter == 'temperature':
        query = 'temperature.gpu'
    elif parameter == 'fan_speed':
        query = 'fan.speed'
    else:
        raise ValueError('Unsupported parameter')
    qm_guest_exec_command = f"qm guest exec {vm_id} nvidia-smi -- --query-gpu={query} --format=csv,noheader,nounits"

    try:
        result = subprocess.run(qm_guest_exec_command, shell=True, check=True, text=True, capture_output=True)
        gpu_temperature = json.loads(result.stdout.strip())['out-data'].strip().split('\n')

        return gpu_temperature

    except subprocess.CalledProcessError as e:
        logger.warning(f"An error occurred: {e.stderr}")
        return None


def get_pci_identifiers():
    result = subprocess.run(f'lspci | grep "VGA.*NVIDIA" | grep -o "^[0-9\:]\+"', shell=True, check=True, text=True, capture_output=True)
    result = result.stdout.strip()

    return result.strip().split('\n')


def find_vm_binding_pci_dev(pci_dev: str):
    try:
        result = subprocess.run(f'qm list | grep running | grep -o "^[ ]\+[0-9]\+" | grep -o "[0-9]\+"', shell=True, check=True, text=True, capture_output=True)
        result = result.stdout.strip()

        for vm_id in result.strip().split('\n'):
            cmd = f'qm config {vm_id} | grep "{pci_dev}"'
            result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, text=True)
            result = result.stdout.strip()
            if result:
                return int(vm_id)

    except subprocess.CalledProcessError as e:
        logger.warning(f"An error occurred: {e.stderr}")


def main():
    fan_configs = list(load_fan_configuration())

    while True:
        vm_ids_with_nvidia_gpus = set()
        for nvidia_pci in get_pci_identifiers():
            vm_id = find_vm_binding_pci_dev(nvidia_pci)
            if vm_id:
                logger.warning(f'GPU {nvidia_pci} is assigned to running VM {vm_id}')
                vm_ids_with_nvidia_gpus.add(vm_id)

        for _ in range(30):
            all_gpu_temps = [0]
            for vm_id in vm_ids_with_nvidia_gpus:
                gpu_temps = [int(x) for x in get_gpu_parameter(vm_id, 'temperature')]
                all_gpu_temps.extend(gpu_temps)
                logger.warning(f'GPU temps are {gpu_temps}')

            max_gpu_temp = max(all_gpu_temps)
            for cfg in fan_configs:
                pwm_value = cfg['curve'][max_gpu_temp]
                set_fan_speed(cfg['pwm_path'], pwm_value)

            time.sleep(3)


if __name__ == '__main__':
    main()
