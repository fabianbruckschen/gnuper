"""Contains functions specifically for bandicoot processing."""

import datetime  # for date calculations
import bandicoot as bc  # MIT toolkit for creating bandicoot indicators
from tqdm import tqdm  # progress bar for large tasks
from multiprocessing import Pool  # enables multiprocessing
from functools import partial  # multiprocessing with several arguments


def load_and_compute(user_id, attributes):
    """
    Bandicoot helper function with inputs based on the predefined attributes
    in the attributes class.

    Inputs
    ------
    user_id : ID of user on which bandicoot features are being calculated.
    attributes : Attributes class with specific options for current run.

    Output
    ------
    Dictionary of calculated bandicoot indicators.
    """
    try:
        # create user object &
        # ignore massive warnings output for better speed
        B = bc.read_csv(user_id=user_id,
                        records_path=attributes.bandicoot_path,
                        antennas_path=attributes.bandicoot_path+'antennas.csv',
                        describe=False,
                        warnings=False)
        # change weekend days and nocturnal time
        B.weekend = attributes.weekend_days
        B.night_start = datetime.time(attributes.noct_time['begin'])
        B.night_end = datetime.time(attributes.noct_time['end'])
        # group by month (aka insert user data in chunks of months)
        # & calculate for weekend and workweek separately
        # & calculate for day and night separately:
        metrics_dic = bc.utils.all(B,
                                   groupby=None,
                                   split_week=True,
                                   split_day=True,
                                   summary='extended')
    except Exception as e:
        metrics_dic = {'name': user_id, 'error': True}
    return metrics_dic


def bc_batch(users, attributes):
    """
    Bandicoot batch function off of single user files.

    Inputs
    ------
    users : Array of user ids on which bandicoot features are being calculated.
    attributes : Attributes class with specific options for current run.

    Output
    ------
    Dictionary of calculated bandicoot indicators for all users in input array.
    """
    # define empty results list
    indicators = []

    if attributes.mp_flag:
        # extract asynchronously and collect results
        pool = Pool(attributes.n_processors)  # multiprocessing
        for _ in tqdm(pool.imap_unordered(partial(load_and_compute,
                                                  attributes=attributes),
                                          users),
                      total=len(users), desc='User Features', leave=True):
            indicators.append(_)
            pass

        pool.close()
        pool.join()

    else:
        # initial progress bar
        pbar = tqdm(total=len(users), desc='User Features', leave=True)
        # looping and appending to list (saves memory)
        for u in users:
            ubc = load_and_compute(u, attributes=attributes)
            indicators.append(ubc)
            pbar.update(1)

    return indicators
