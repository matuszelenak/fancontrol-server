import asyncio
import logging
from asyncio import Queue

import uvicorn
from fastapi import FastAPI

from fan import load_fan_configuration, get_pci_identifiers, find_vm_binding_pci_dev, get_gpu_parameter, set_fan_speed

logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')


async def lifespan(ap):
    ap.fan_configs = list(load_fan_configuration())

    pwm_task = asyncio.create_task(pwm_application_task())
    update_task = asyncio.create_task(update_ticker_task())
    yield
    pwm_task.cancel()
    update_task.cancel()


app = FastAPI(lifespan=lifespan)

update_queue = Queue()


async def update_ticker_task():
    while True:
        await update_queue.put(42)
        await asyncio.sleep(3)


async def pwm_application_task():
    try:
        while True:
            vm_ids_with_nvidia_gpus = set()
            for nvidia_pci in get_pci_identifiers():
                vm_id = find_vm_binding_pci_dev(nvidia_pci)
                if vm_id:
                    logger.warning(f'GPU {nvidia_pci} is assigned to running VM {vm_id}')
                    vm_ids_with_nvidia_gpus.add(vm_id)

            for _ in range(30):
                await update_queue.get()
                logger.warning('Updating ...')
                all_gpu_temps = [0]
                for vm_id in vm_ids_with_nvidia_gpus:
                    gpu_temps = [int(x) for x in get_gpu_parameter(vm_id, 'temperature')]
                    all_gpu_temps.extend(gpu_temps)
                    logger.warning(f'GPU temps are {gpu_temps}')

                max_gpu_temp = max(all_gpu_temps)
                for cfg in app.fan_configs:
                    pwm_value = cfg['curve'][max_gpu_temp]
                    set_fan_speed(cfg['pwm_path'], pwm_value)
    except Exception as e:
        logger.error('Exception in main', exc_info=True)
        logger.error(str(e))


@app.get('/fans')
async def get_fan_data():
    return {}


@app.post('/curve')
async def update_curve():
    await update_queue.put(47)

    return {}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8047)
