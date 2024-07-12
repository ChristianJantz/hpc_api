from configparser import ConfigParser
import datetime
import os
from typing import List
from azure.batch.models import BatchErrorException

SAMPLE_CONFIG_FILE_NAME = "configuration.cfg"


def print_batch_exception(batch_exception: BatchErrorException):
    """
    Prints the contents of the specified Batch exception.
    :param batch_exception:
    """
    print('-------------------------------------------')
    print('Exception encountered:')
    if batch_exception.error and \
            batch_exception.error.message and \
            batch_exception.error.message.value:
        print(batch_exception.error.message.value)
        if batch_exception.error.values:
            print()
            for mesg in batch_exception.error.values:
                print(f'{mesg.key}:\t{mesg.value}')
    print('-------------------------------------------')



def print_configuration(config: ConfigParser):
    """print the Configuration

    Args:
        config (ConfigParser): the Configuration
    """
    configuration_dict = {s: dict(config.items(s))
                          for s in config.sections()}
    print("----------------------")
    print(configuration_dict)


def generate_unique_resource_name(resource_prefix: str) -> str:
    """create a unique resource name by appending time

    Args:
        resource_prefix (str): prefix of the resource

    Returns:
        str: a speciffied resource name
    """
    return resource_prefix + "-" + datetime.datetime.utcnow().strftime("%Y%m%d-%H%M%S%f")


def wrap_commands_in_shell(ostype: str, commands: List[str]) -> str:
    """_summary_

    Args:
        os_type (str): _description_
        commands (list): _description_

    Returns:
        str: _description_
    """
    if ostype.lower() == "linux":
        return '/bin/bash -c \'set -e; set -o pipfail; {}; wait\''.format(';'.join(commands))
    elif ostype.lower() == "windows":
        return f'cmd.exe /c "{"&".join(commands)}"'
    else:
        raise ValueError(f'unkown os type: {ostype}')
