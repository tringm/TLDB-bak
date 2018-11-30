from pathlib import Path


def root_path():
    return Path(__file__)


def data_path():
    return Path(__file__).parent / 'data'
