import pandas as pd
import numpy as np
from random import choices
from faker import Faker

def generate_dummy_dataframe(num_rows:int) -> pd.DataFrame:
    """
    Generate a dummy dataframe with random data.

    Parameters:
    num_rows (int): The number of rows to generate in the dataframe.

    Returns:
    pd.DataFrame: The generated dummy dataframe.
    
    Example:
        >>> generate_dummy_dataframe(10)
                 full_name   birthdate  gender         state  income
        0   Terri Thompson  1983-01-01    Male       Arizona  113184
        1   Lindsay Glover  1947-09-13    Male    California   64034
        2   Kayla Mcmillan  1941-06-04  Female          Utah   66975
        3   Steven Shannon  1960-03-11  Female      Nebraska   84299
        4   Richard Harmon  1940-03-25    Male  Pennsylvania   90880
        5     Pamela Stone  2005-08-27  Female      Arkansas   69602
        6   Anthony Strong  1976-01-08  Female     Wisconsin  110118
        7        Lee Evans  1997-05-22    Male      Colorado   50748
        8  Kayla Wilkerson  1992-03-30  Female    New Jersey   40805
        9  Ashley Schwartz  1976-12-01    Male    New Mexico   63029
    """
    fake = Faker()
    cols = ["full_name", "birthdate", "gender", "state", "income"]
    
    data = {
        "full_name": [fake.name() for _ in range(num_rows)],
        "birthdate": [fake.date_of_birth(minimum_age=18, maximum_age=90) for _ in range(num_rows)],
        "gender": choices(["Male", "Female"], k=num_rows),
        "state": [fake.state() for _ in range(num_rows)],
        "income": np.random.randint(30000, 120000, num_rows)
    }
    
    return pd.DataFrame(data, columns=cols)

