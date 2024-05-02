from datetime import datetime, timezone
from os import makedirs
from typing import Dict, Tuple
from tempfile import mkdtemp

def get_utc_timestamp() -> str:
    """
    Returns the current UTC timestamp in the format "%Y-%m-%dT%H:%M:%SZ".
    
    Returns:
        str: The current UTC timestamp.
    Examples:
        >>> get_utc_timestamp()
        '2024-03-18T20:01:54Z'
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def make_workspace_dirs() -> Tuple[str, str]:
    """
    Creates a local directory structure for storing raw and staged data.

    Returns:1
        A tuple containing the paths to the raw and staged directories.
    
    Example:
        >>> make_local_dirs()
        ('./tmp/data-eh5dbkmn/raw', './tmp/data-eh5dbkmn/stg')
    """
    if True:
        dir = "tmp/"
        makedirs(dir,exist_ok=True)
    else:
        dir = None
    data = mkdtemp(prefix="data-", dir=dir)
    raw_dir = f"{data}/raw"
    stg_dir = f"{data}/stg"
    makedirs(raw_dir)
    makedirs(stg_dir)
    return raw_dir, stg_dir


def extract_keys_from_path(path:str) -> Dict[str,str]:
    """
    Extracts the keys and values from a partition path.

    Args:
        path (str): The partition path.

    Returns:
        Dict[str,str]: A dictionary containing the keys and values from the partition path.

    Examples:
        >>> extract_keys_from_path("my_folder/year=2022/month=01/day=01")
        {'year': '2022', 'month': '01', 'day': '01'}
    """
    parts = [part.split('=') for part in path.split("/") if '=' in part]
    return {k: value for k, value in parts}



def extract_key_from_path(path: str, key: str) -> str:
    """
    Extracts the value of a given key from a partition path.

    Args:
        path (str): The partition path.
        key (str): The key to extract the value for.

    Returns:
        str: The value of the key if found, otherwise an empty string.

    Examples:
        >>> extract_key_from_path("my_folder/year=2022/month=01/day=01", "year")
        '2022'

        >>> extract_key_from_path("my_folder/year=2022/month=01/day=01", "hour")
        ''
    """
    keys = extract_keys_from_path(path)
    return keys.get(key, None)
