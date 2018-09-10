"""Contains tools for processing files and (spark) data frames."""

import os  # operating system functions like renaming files and directories
import glob  # pattern matching for paths
import datetime  # for date calculations
import bandicoot as bc  # MIT toolkit for creating bandicoot indicators
from tqdm import tqdm  # progress bar for large tasks
from multiprocessing import Pool  # enables multiprocessing
from multiprocessing.pool import ThreadPool  # enables spark multithreading
from functools import partial  # multiprocessing with several arguments


def read_as_sdf(file, sparksession, header=True, inferSchema=True,
                colnames=None, query=None):
    """
    Read in csv file as spark data frame (sdf).
    Optionally change columns and/or run sql query on it.

    Inputs
    ------
    file : path to the file to be read in
    sparksession : sparksession object defined by pyspark
    header : does the first row of the csv file represent the header
    inferSchema : should the filetype for each column be inferred (=True) or
                  simply be kept as string (=False)
    colnames : array of new column names for the file
    query : string of query which can be executed on read sdf

    Output
    ------
    Spark data frame.
    """
    try:
        sdf = sparksession.read.csv(file,
                                    header=header,
                                    inferSchema=inferSchema)
        if not header:
            if colnames is None:
                print('No header and no column names given... Loading sdf with\
                generic column names (c1, c2, etc.).')
            else:
                sdf = sdf.toDF(*colnames)

        if query is not None:  # execute query if given
            table_name = 'table_'+os.path.splitext(os.path.basename(file))[0]
            sdf.createOrReplaceTempView(table_name)
            sdf = sparksession.sql(query % {'table_name': table_name})

    except Exception as e:
        print('Oops! File:%s cannot be read!' % file)
        sdf = []
    return sdf


def read_alter_save(file, sparksession, read_header=True, inferSchema=True,
                    colnames=None, query=None,
                    save_path=None, save_format='csv', save_header=True,
                    save_mode='append', save_partition=None):
    """
    Read in a file and potentially alter it with a query.
    Save altered file to a new location with potential paritioning.

    Inputs
    ------
    file : path to the file to be read in
    sparksession : sparksession object defined by pyspark
    read_header : does the first row of the csv file represent the header
    inferSchema : should the filetype for each column be inferred (=True) or
                  simply be kept as string (=False)
    colnames : array of new column names for the file
    query : string of query which can be executed on read sdf
    save_path : location where altered sdf should be saved to
    save_format : format in which the sdf should be saved in
    save_header : Boolean if header should be saved as well
    save_mode : 'append' tries to save to same location if not existing yet
                'overwrite' deletes present files before saving
    save_partition : column which to partition by

    Output
    ------
    Simple TRUE statement after successful execution.
    """
    # read in and run potential query on it
    sdf = read_as_sdf(file=file, sparksession=sparksession,
                      header=read_header, inferSchema=inferSchema,
                      colnames=colnames, query=query)
    # save as desired file split by columns if desired
    sdf.write.save(path=save_path, format=save_format,
                   header=save_header, mode=save_mode,
                   partitionBy=save_partition)
    return True


def union_all(df_list):
    """
    Recursive unioning function.
    Stacking list of sdfs together into 1 single sdf.

    Inputs
    ------
    df_list : list of spark data frames with same schema

    Output
    ------
    Single sdf which unioned all elements of the input list.
    """
    if len(df_list) > 1:
        return df_list[0].union(union_all(df_list[1:]))
    else:
        return df_list[0]


def sdf_from_folder(folder, attributes, sparksession, file_pattern='*.csv',
                    recursive=False, header=True, inferSchema=True,
                    colnames=None, query=None,
                    save_path=None, save_format='csv', save_header=True,
                    save_mode='append', save_partition=None,
                    action='union'):
    """
    Read several files from a folder.
    Afterwards 'union' them, 'save' as altered csvs or 'both'.

    Inputs
    ------
    folder : directory of files to be read in
    attributes : attributes class with specific options for current run
    sparksession : sparksession object defined by pyspark
    file_pattern : pattern of files which should be included
    recursive : if TRUE subdirectories will be included as well
    header : does the first row of the csv files represent the header
    inferSchema : should the filetype for each column be inferred (=True) or
                  simply be kept as string (=False)
    colnames : array of new column names for the file
    query : string of query which can be executed on read sdf
    save_path : location where altered sdf should be saved to
    save_format : format in which the sdf should be saved in
    save_header : Boolean if header should be saved as well
    save_mode : 'append' tries to save to same location if not existing yet
                'overwrite' deletes present files before saving
    save_partition : column which to partition by
    action : One of ('union', 'save', 'both') which defines the further
             handling of the files after being read

    Output
    ------
    Depending on the input action either a spark data frame ('union' or 'both')
    or a TRUE statement ('save').
    """
    if action not in ('union', 'save', 'both'):
        raise ValueError("Action is not in 'union', 'save' or 'both'... \
        Aborting")

    # get file names
    file_names = glob.glob(folder+file_pattern)
    if recursive:
        subdirs = next(os.walk(folder))[1]
        for d in subdirs:
            file_names.append(glob.glob(folder+d+'/'+file_pattern)[0])

    # if files should be read, altered and saved
    if action in ('save', 'both'):
        # if attributes.mp_flag:
        #     # assuming 2 threads per processor
        #     pool = ThreadPool(attributes.n_processors*2)
        #     for _ in tqdm(pool
        #                   .imap_unordered(
        #                     partial(read_alter_save,
        #                             sparksession=sparksession,
        #                             read_header=header,
        #                             inferSchema=inferSchema,
        #                             colnames=colnames,
        #                             query=query,
        #                             save_path=save_path,
        #                             save_format=save_format,
        #                             save_header=save_header,
        #                             save_mode=save_mode,
        #                             save_partition=save_partition),
        #                     file_names),
        #                   total=len(file_names),
        #                   desc='Read, Alter & Save Files',
        #                   leave=True):
        #         pass
        #     pool.close()
        # else:
        pbar = tqdm(total=len(file_names), desc='Read, Alter & Save Files',
                    leave=True)
        for file in file_names:
            read_alter_save(file, sparksession=sparksession,
                            read_header=header,
                            inferSchema=inferSchema,
                            colnames=colnames,
                            query=query, save_path=save_path,
                            save_format=save_format,
                            save_header=save_header,
                            save_mode=save_mode,
                            save_partition=save_partition)
            pbar.update(1)
        print('Files in folder which match pattern have been read,\
              altered & saved!')

    if action in ('union', 'both'):
        # read in files
        raw_df_list = []
        if attributes.mp_flag:
            # assuming 2 threads per processor
            pool = ThreadPool(attributes.n_processors*2)
            for _ in tqdm(pool
                          .imap_unordered(partial(read_as_sdf,
                                                  sparksession=sparksession,
                                                  header=header,
                                                  inferSchema=inferSchema,
                                                  colnames=colnames,
                                                  query=query),
                                          file_names),
                          total=len(file_names), desc='Read Files',
                          leave=True):
                raw_df_list.append(_)
                pass
            pool.close()
        else:
            pbar = tqdm(total=len(file_names), desc='Read Files', leave=True)
            for file in file_names:
                raw_df_list.append(read_as_sdf(file, sparksession=sparksession,
                                               header=header,
                                               inferSchema=inferSchema,
                                               colnames=colnames,
                                               query=query))
                pbar.update(1)

        # unite them
        raw_df = union_all(raw_df_list)
        print('Files have been read and unioned!')
        return raw_df
    return True


def load_and_compute(user_id, attributes):
    """
    Bandicoot helper function with inputs based on the predefined attributes.

    Inputs
    ------
    user_id : id of user on which bandicoot features are being calculated
    attributes : attributes class with specific options for current run

    Output
    ------
    Dictionary of calculated bandicoot indicators.
    """
    try:
        # create user object &
        # ignore massive warnings output for better speed
        B = bc.read_csv(user_id=user_id,
                        records_path=attributes.bandicoot_path,
                        antennas_path=attributes.antennas_file,
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
    users : array of user ids on which bandicoot features are being calculated
    attributes : attributes class with specific options for current run

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


def aggregate_chunks(feature_type, attributes, sparksession, unit='antenna_id',
                     create_table=True, cache_table=False, query=None):
    """
    Function to aggregate files from chunked folders and run aggregation query.

    Inputs
    ------
    feature_type : type of feature which should be aggregated (e.g. 'hour')
    attributes : attributes class with specific options for current run
    sparksession : sparksession object defined by pyspark
    unit : column which represents the unit of aggregation
    create_table : if set, table will be created for further querying
    cache_table : if set, table will be cached (increases speed if table is
                  small enough and is being queried several times)
    query : string of query which can be executed on read sdf

    Output
    ------
    Aggregated spark data frame for inserted feature type.
    """
    # load in all items
    sdf = sdf_from_folder(folder=attributes.antenna_features_path +
                          feature_type+'/',
                          attributes=attributes, sparksession=sparksession,
                          recursive=True)

    # create table
    sdf.createOrReplaceTempView('table_antenna_metrics_'+feature_type+'_df')

    # aggregate
    sdf = sparksession.sql(query)

    if create_table:
        # create aggregated table
        sdf.createOrReplaceTempView('table_'+feature_type)
        # and cache if it will be used more than once
        if cache_table:
            sparksession.catalog.cacheTable('table_'+feature_type)

    print('Aggregated chunks for '+feature_type+'!')

    return sdf
