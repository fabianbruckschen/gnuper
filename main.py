"""
main.py

This script serves as a one line execution via the command line for the whole
preprocessing process. Several options can be set when started:

mp_flag : If set, several parts of the script try to spawn as many processes or
          threads as possible to speed up execution.
bc_flag : If set, the time intensive bandicoot indicators are being calculated
          as well.
hdfs_flag : If set, the data is saved in an HDFS (Depending on location of raw
            CDRs).
verbose : If set, no additional info about data structure or size are being
          printed in order to save time.
clean_up : If set, intermediate directories and files are being deleted after
           they have been used.
raw_data_path : Can be set to an alternate path (e.g. ../test_data/), default
                is 'data' folder in the parent directory (../data/).
sparkmaster : On what filesystem Spark is supposed to run (local, yarn, ...).
parts_to_run : Process is split into three parts which can be executed
               separately.
"""

import os  # operating system functions like renaming files and directories
import shutil  # recursive file and directory operations
import glob  # pattern matching for paths
import argparse  # entering flags from the cmd line
import bandicoot as bc  # MIT toolkit for creating bandicoot indicators
import gnuper as gn  # gnuper package for creating cdr features
import pyarrow as pa  # restructuring in HDFS
from pyspark.sql import SparkSession  # using spark context for big data files
from pyspark.sql.functions import col  # needed for function over each column

# Preparations
if __name__ == "__main__":
    # command line options
    parser = argparse.ArgumentParser(description='Options which can be entered\
     via cmd.')
    parser.add_argument("--mp_flag", action='store_true',
                        help="Enabling Multiprocessing where feasible",
                        default=False)
    parser.add_argument("--bc_flag", action='store_true',
                        help="Bandicoot indicators will be calculated as well",
                        default=False)
    parser.add_argument("--hdfs_flag", action='store_true',
                        help="Data is stored in an HDFS",
                        default=False)
    parser.add_argument("--verbose", action='store_true',
                        help="Additional info is being printed",
                        default=False)
    parser.add_argument("--clean_up", action='store_true',
                        help="Intermediate folders and files will be deleted\
                        after a successful run",
                        default=False)
    parser.add_argument("--raw_data_path",
                        help="Location of raw data, end with /")
    parser.add_argument("--sparkmaster",
                        help="If data stored locally 'local[*]',\
                         if hdfs 'yarn'.")
    parser.add_argument("--parts_to_run",
                        help="Define parts of the script which should be run")
    parsed_args = parser.parse_args()

    if parsed_args.raw_data_path is None:
        raw_data_path = 'data/'
    else:
        raw_data_path = parsed_args.raw_data_path

    if parsed_args.sparkmaster is None:
        sparkmaster = 'local[*]'
    else:
        sparkmaster = parsed_args.sparkmaster

    if parsed_args.parts_to_run is None:
        parts_to_run = 'all'
    else:
        parts_to_run = parsed_args.parts_to_run

# print info
print('3 parts will be run inside this script:\n\
Part 1: Chunking by user ids\n\
Part 2: Looping over those chunks to create antenna features\n\
Part 3: Uniting features from all sources to get 1 single output file')

# # define attributes for this session
att = gn.Attributes(mp_flag=parsed_args.mp_flag,
                    bc_flag=parsed_args.bc_flag,
                    hdfs_flag=parsed_args.hdfs_flag,
                    verbose=parsed_args.verbose,
                    clean_up=parsed_args.clean_up,
                    raw_data_path=raw_data_path,
                    sparkmaster=sparkmaster
                    # insert case specific attributes e.g.:
                    # raw_locations='MS_LOCATION.csv',
                    # cap_coords=[15.500654, 32.559899],  # capital gps
                    # weekend_days=[5, 6],
                    # holidays=['2018-10-16', '2018-10-17', '2018-10-18'],
                    # max_chunksize=25000
                    )

if parts_to_run in ('all', '1'):
    # --- Part 1 --- (Preprocessing/Unifying of raw files & chunking by users)
    spark = SparkSession.builder.master(att.sparkmaster)\
        .appName('cdr_extraction_part1').getOrCreate()
    print('Spark environment for Part 1 created!')

    # raw locations:
    # read cell and antenna locations into a spark dataframe (sdf)
    raw_locations = gn.read_as_sdf(file=att.raw_locations,
                                   sparksession=spark, header=False,
                                   colnames=['antenna_id', 'cell_id',
                                             'longitude', 'latitude'],
                                   query=gn.queries.general
                                   .raw_locations_query())
    # create raw table to query and cache it as we will query it for every day
    raw_locations.createOrReplaceTempView('table_raw_locations')
    spark.catalog.cacheTable('table_raw_locations')

    # antennas:
    # solely antenna locations as sdf (= ignore cell_id)
    # FILE: save as 1 csv next to the raw data
    raw_locations.selectExpr('antenna_id', 'longitude', 'latitude')\
        .dropDuplicates().coalesce(1).write.csv(att.antennas_path,
                                                mode='overwrite', header=True)
    print('Raw Locations SDF & table created!')

    # Preprocessing
    # **Level 0**: General preprocessing of raw call detail records
    # Storing daily files in a unified dataframe
    print('Starting with Level 0: General preprocessing of raw CDRs.')

    # CDRs info:
    if att.verbose:
        dates = gn.files_in_folder(folder=att.raw_data_path,
                                   file_pattern='20*.csv',
                                   hdfs_flag=att.hdfs_flag)
        dates = [os.path.basename(d).replace('.csv', '') for d in dates]
        print('Available CDR Dates: '+str(dates))  # doublechecking

    # Read unique Users:
    # order of the raw columns
    raw_colnames = ['direction', 'caller_id', 'datetime',
                    'interaction', 'cell_id', 'national',
                    'correspondent_id', 'tac_code', 'call_duration']
    # reading in user_ids
    users = gn.sdf_from_folder(folder=att.raw_data_path, attributes=att,
                               sparksession=spark, file_pattern='20*.csv',
                               header=False, colnames=raw_colnames,
                               query=gn.queries.general.get_user_ids_query(),
                               action='union')
    # sum up interactions per user_id over all files
    users = users.groupBy('caller_id').sum('n_interactions')\
                 .withColumnRenamed('sum(n_interactions)', 'n_interactions')
    # create table to query
    users.createOrReplaceTempView('table_user_ids')

    # Define Chunk ids:
    # create chunk id for every user,
    # based on max number of interactions per chunk
    users_w_chunk = spark.sql(gn.queries.general
                              .chunking_query_by_n_interactions(
                                  table_name='table_user_ids',
                                  max_chunksize=att.max_chunksize))
    users_w_chunk.createOrReplaceTempView('table_chunk_ids')
    # cache this table as well due to the need of querying it for every day
    spark.catalog.cacheTable('table_chunk_ids')

    # Save in Chunks:
    # preprocess every single file and save it split by chunk id
    gn.sdf_from_folder(folder=att.raw_data_path, attributes=att,
                       sparksession=spark, file_pattern='20*.csv',
                       header=False,
                       colnames=raw_colnames,
                       query=gn.queries.level0.raw_preprocessing_query(
                               cump=att.call_unit_multiplicator),
                       save_path=att.chunking_path, save_format='csv',
                       save_header=True, save_mode='append',
                       save_partition='chunk_id', action='save')
    print('CSV files created for user chunks.')

    # stop spark session
    spark.stop()
    print('DONE with Part 1! User chunks saved to chunking folder.')

if parts_to_run in ('all', '2'):
    # --- Part 2 --- (create antenna indicators in a loop over the chunks)
    spark = SparkSession.builder.master(att.sparkmaster)\
        .appName('cdr_extraction_part2').getOrCreate()
    print('Spark environment for Part 2 created!')

    # antenna locations:
    # read in table which was created in part 1
    antennas_locations = spark.read.csv(att.antennas_path,
                                        header=True,
                                        inferSchema=True)
    # create raw table to query and cache it for several queries
    antennas_locations.createOrReplaceTempView('table_antennas_locations')
    spark.catalog.cacheTable('table_antennas_locations')

    # user chunks info:
    if att.hdfs_flag:
        (ret, out, err) = gn.run_cmd(['hdfs', 'dfs', '-ls', '-R',
                                     att.chunking_path,
                                     "| grep drwx | awk -F'/' '{print $NF}'"],
                                     shell=True)
        chunks = out.decode('utf-8').split()
    else:
        chunks = sorted(next(os.walk(att.chunking_path))[1])
    # save length for loop
    n = len(chunks)
    print('Number of available user chunks: '+str(n))
    # calculate optimum number of files per save
    if att.mp_flag:
        n_files = min(10, att.n_processors)
    else:
        n_files = 10

    # LOOP
    # --- LOOP START ---
    for chunk in chunks:
        i = chunks.index(chunk)+1
        print('Starting with '+chunk+', iteration '+str(i)+' out of '+str(n))

        # read data:
        # read in chunk and filter out machines and multi-users
        raw_df = gn.sdf_from_folder(att.chunking_path+chunk+'/', att, spark,
                                    header=True, inferSchema=False,
                                    file_pattern='*.csv',
                                    query=gn.queries.level0
                                    .filter_machines_query(
                                        max_weekly_interactions=att.
                                        max_weekly_interactions))
        # quick look
        if att.verbose and i == 1:
            # double check file format
            print('Raw aggregated SDF structure:')
            raw_df.show(10)
        # and register as table
        raw_df.createOrReplaceTempView('table_raw_df')

        # home antennas:
        user_home_antenna_df = spark.sql(gn.queries.level1
                                         .user_home_antenna_query(
                                            noct_time=att.noct_time,
                                            table_name='table_raw_df'))
        # save as table and cache it
        user_home_antenna_df\
            .createOrReplaceTempView('table_user_home_antenna_df')
        spark.catalog.cacheTable('table_user_home_antenna_df')
        # save as file
        user_home_antenna_df.coalesce(n_files)\
            .write.csv(att.home_antennas_path+str(i), header=True,
                       mode='overwrite')

        # chunk info:
        # unique users in this chunk (doublechecking)
        if att.verbose:
            n_users = raw_df.select('caller_id').distinct().count()
            print('Number of users in this chunk: '+str(n_users))
            n_users_w_ha = user_home_antenna_df.select('user_id').distinct()\
                                               .count()
            print('Ratio of users in this chunk with home antenna: ' +
                  str(n_users_w_ha/n_users))

        # **Level 1**: Intermediate tables on user level (user_id visible)

        # Datasets created in first iteration with columns in brackets:
        # + **user_metrics**: metrics aggregated per user_id, day and hour
        #  *(user_id, day, hour, og_calls, ic_calls, og_sms, ic_sms,
        #    og_vol, ic_vol)*
        # + **user_home_antenna**: monthly estimate of antenna closest to home
        #  location per user *(user_id, antenna_id, month)*
        # + **user_bandicoot_features**: bandicoot interactions on user level
        #  per month *(user_id, month, ...)*
        print('Starting with Level 1: Intermediate tables on user level \
        (user_id still visible).')

        # user_metrics:
        user_metrics_df = spark.sql(gn.queries.level1.user_metrics_query(
                                    table_name='table_raw_df'))
        user_metrics_df.createOrReplaceTempView('table_user_metrics_df')

        # bandicoot_metrics:
        if att.bc_flag:
            # read in:
            bc_metrics_df = spark.sql(gn.queries.level1.bc_metrics_query(
                                      table_name='table_raw_df'))

            # restructure:
            # define unique users
            users = [str(u.caller_id) for u in bc_metrics_df
                     .select('caller_id').dropDuplicates().collect()]
            # save into single user folders
            bc_metrics_df.coalesce(1).write.save(
                        path=att.bandicoot_path,
                        format='csv',
                        header=True,
                        mode='overwrite',
                        partitionBy='caller_id')
            # save antenna file in the same directory
            antennas_locations.coalesce(1).write.save(
                path=att.bandicoot_path+'antennas',
                format='csv',
                header=True,
                mode='append')
            # proper file naming and directory structure
            if att.hdfs_flag:
                # get all file names
                bc_list = gn.ls_hdfs(att.bandicoot_path, '*.csv',
                                     recursive=True)
                # connect via API
                fs = pa.hdfs.connect()
                # rename and move up 1 folder
                for file in bc_list:
                    fs.rename(file, os.path.dirname(file).replace('caller_id=',
                              '')+'.csv')
                # create local directory
                target_dir = os.getcwd()+'/user_bandicoot/'
                if not os.path.exists(target_dir):
                    os.makedirs(target_dir)
                # copy to local in order to use bandicoot functions
                (ret, out, err) = gn.run_cmd(['hdfs', 'dfs', '-copyToLocal',
                                              '-f', att.bandicoot_path+'*.csv',
                                              target_dir])
            else:
                # get all subdirectories in bandicoot folder
                subdirs = next(os.walk(att.bandicoot_path))[1]
                for d in subdirs:
                    u = d.replace('caller_id=', '')  # extract user id
                    # rename csv to user id and move up
                    os.rename(glob.glob(att.bandicoot_path+d+'/*.csv')[0],
                              att.bandicoot_path+u+'.csv')
                    shutil.rmtree(att.bandicoot_path+d)  # delete obsolete dir
                print('Single User Files for bandicoot created for chunk ' +
                      str(i))

            # calculate BC:
            # execute bandicoot calculation as batch
            indicators = gn.bc_batch(users, att)
            # save as csv
            bc.io.to_csv(indicators, att.bandicoot_path +
                         'bandicoot_indicators_' + str(i) + '.csv')
            print('Bandicoot files csvs created for chunk ' + str(i))
            # copy back to hdfs for proper reading into sdf
            if att.hdfs_flag:
                (ret, out, err) = gn.run_cmd(['hdfs', 'dfs', '-copyFromLocal',
                                              att.bandicoot_path +
                                              'bandicoot_indicators_' +
                                              str(i) + '.csv',
                                              att.bandicoot_path])
            # re-read as single sdf
            bc_metrics_df = spark.read.csv(att.bandicoot_path +
                                           'bandicoot_indicators_' + str(i) +
                                           '.csv', header=True,
                                           inferSchema=True)

            # cleaning:
            bc_metrics_df = bc_metrics_df\
                .drop('reporting__antennas_path',
                      'reporting__attributes_path',
                      'reporting__recharges_path',
                      'reporting__version',
                      'reporting__code_signature',
                      'reporting__groupby',
                      'reporting__split_week',
                      'reporting__split_day',
                      'reporting__start_time',
                      'reporting__end_time',
                      'reporting__night_start',
                      'reporting__night_end',
                      'reporting__weekend',
                      'reporting__number_of_antennas',
                      'reporting__bins',
                      'reporting__bins_with_data',
                      'reporting__bins_without_data',
                      'reporting__has_call',
                      'reporting__has_text',
                      'reporting__has_home',
                      'reporting__has_recharges',
                      'reporting__has_attributes',
                      'reporting__has_network',
                      'reporting__number_of_recharges',
                      'reporting__percent_records_missing_location',
                      'reporting__antennas_missing_locations',
                      'reporting__percent_outofnetwork_calls',
                      'reporting__percent_outofnetwork_texts',
                      'reporting__percent_outofnetwork_contacts',
                      'reporting__percent_outofnetwork_call_durations',
                      'reporting__ignored_records__all',
                      'reporting__ignored_records__interaction',
                      'reporting__ignored_records__direction',
                      'reporting__ignored_records__correspondent_id',
                      'reporting__ignored_records__datetime',
                      'reporting__ignored_records__call_duration',
                      'reporting__ignored_records__location')

        # **Level 2**: Intermediate tables on antenna level
        # (user_id NOT visible anymore)

        # Datasets created in second iteration with columns in brackets:
        # + **antenna_interactions_generic**: alltime interactions between
        #  antennas without allocation of home antenna locations but generic
        #  activity
        #  *(og_antenna_id, ic_antenna_id, sms_count, calls_count, vol_sum)*
        # + **antenna_metrics_week**: metrics aggregated per home antenna of
        #  individual users, week and part of the week
        #  *(antenna_id, week_part, week_number, og_calls, ic_calls, og_sms,
        #  ic_sms, og_vol, ic_vol)*
        # + **antenna_metrics_hourly**: metrics aggregated per home antenna of
        #  individual users and hour
        #  *(antenna_id, hour, og_calls, ic_calls, og_sms, ic_sms, og_vol,
        #  ic_vol)*
        # + **antenna_interactions**: alltime interactions between antennas
        #  based on the users' behavior to which a certain antenna is the
        #  homebase
        #  *(antenna_id1, antenna_id2, sms_count, calls_count, vol_sum)*
        # + **antenna_bandicoot**: alltime averaged bandicoot interactions on
        #  antenna level *(antenna_id, ...)*
        #
        # A further explanation of the single features can be found
        #  here[http://bandicoot.mit.edu/docs/reference/bandicoot.individual.html]
        print('Starting with Level 2: Intermediate tables on antenna level\
        (user_id NOT visible anymore).')

        # antenna_metrics_week
        antenna_metrics_week_df = spark\
            .sql(gn.queries.level2.antenna_metrics_week_query(
                table_name='table_user_metrics_df',
                weekend_days=tuple(att.weekend_days),
                holidays=tuple(att.holidays)))

        # antenna_metrics_hourly
        antenna_metrics_hourly_df = spark\
            .sql(gn.queries.level2.antenna_metrics_hourly_query(
                table_name='table_user_metrics_df'))

        # antenna_interactions
        antenna_interactions_df = spark\
            .sql(gn.queries.level2.antenna_interactions_query(
                table_name='table_raw_df'))

        # uncache for memory
        spark.catalog.clearCache()

        # bandicoot_metrics
        if att.bc_flag:
            # join home_antenna
            join_cond = [bc_metrics_df.name == user_home_antenna_df.user_id]
            bc_metrics_df = bc_metrics_df\
                .join(user_home_antenna_df, join_cond, 'inner')\
                .drop('user_id', 'name', 'month')
            # keep weight as number of users adding to each antenna
            antenna_weight = bc_metrics_df.groupBy('antenna_id').count()
            # calculate antenna means
            antenna_bandicoot_features_df = bc_metrics_df\
                .groupBy('antenna_id').mean().drop('avg(antenna_id)')
            # mean drops out "delay" columns, because they are entirely empty
            # renaming the columns for better readability
            clean_cols = [c.replace('avg(', '').replace(')', '') for c in
                          antenna_bandicoot_features_df.columns]
            antenna_bandicoot_features_df = antenna_bandicoot_features_df\
                .toDF(*clean_cols)
            # add user weight
            antenna_bandicoot_features_df = antenna_bandicoot_features_df\
                .join(antenna_weight, 'antenna_id', 'inner')

        # save final outputs (not too many files):
        antenna_metrics_week_df.coalesce(n_files)\
            .write.csv(att.antenna_features_path+'week/'+str(i),
                       mode='overwrite', header=True)

        antenna_metrics_hourly_df.coalesce(n_files)\
            .write.csv(att.antenna_features_path+'hourly/'+str(i),
                       mode='overwrite', header=True)

        antenna_interactions_df.coalesce(n_files)\
            .write.csv(att.antenna_features_path+'interactions/'+str(i),
                       mode='overwrite', header=True)

        if att.bc_flag:
            antenna_bandicoot_features_df.coalesce(n_files)\
                .write.csv(att.antenna_features_path+'bc/'+str(i),
                           mode='overwrite', header=True)

        # info if done:
        if i == n:
            print('Done with all user chunks!')

    # --- LOOP END ---
    # stop spark session
    spark.stop()

    # delete chunking files if clean up flag is set
    # also bandicoot if set
    if att.clean_up:
        if att.hdfs_flag:
            (ret, out, err) = gn.run_cmd(['hdfs', 'dfs', '-rm', '-R',
                                          att.chunking_path])
        else:
            shutil.rmtree(att.chunking_path)
            if att.bc_flag:
                shutil.rmtree(att.bandicoot_path)
    print('DONE! Antenna features files saved to antenna_features folder.')

if parts_to_run in ('all', '3'):

    # --- Part 3 --- (Weighted unification of antenna features & create single
    # output file: final_features.csv )
    spark = SparkSession.builder.master(att.sparkmaster)\
        .appName('cdr_extraction_part3').getOrCreate()
    print('Spark environment for Part 3 created!')

    # antennas:
    # read in table
    antennas_locations = spark.read.csv(att.antennas_path,
                                        header=True,
                                        inferSchema=True)
    # create raw table to query & cache
    antennas_locations.createOrReplaceTempView('table_antennas_locations')
    spark.catalog.cacheTable('table_antennas_locations')

    # home antennas:
    home_antennas = gn.sdf_from_folder(att.home_antennas_path, att, spark,
                                       recursive=True,
                                       header=True,
                                       inferSchema=True)
    # keep number of antennas
    n_home_antennas = home_antennas.select('antenna_id').distinct().count()

    # **Level 2**: Intermediate tables on antenna level
    # (user_id NOT visible anymore)

    # Datasets created in second iteration with columns in brackets:
    # + **antenna_metrics_week**: metrics aggregated per home antenna of
    #  individual users, week and part of the week *(antenna_id, week_part,
    #  week_number, og_calls, ic_calls, og_sms, ic_sms, og_vol, ic_vol)*
    # + **antenna_metrics_hourly**: metrics aggregated per home antenna of
    #  individual users and hour
    #  *(antenna_id, hour, og_calls, ic_calls, og_sms, ic_sms, og_vol, ic_vol)*
    # + **antenna_interactions**: alltime interactions between antennas based
    #  on the users' behavior to which a certain antenna is the homebase
    #  *(antenna_id1, antenna_id2, sms_count, calls_count, vol_sum)*
    # + **antenna_bandicoot**: alltime averaged bandicoot interactions on
    #  antenna level *(antenna_id, ...)*
    #
    # A further explanation of the single features can be found
    #  here[http://bandicoot.mit.edu/docs/reference/bandicoot.individual.html]
    print('Starting with Level 2: Intermediate tables on antenna level\
     (user_id NOT visible anymore).')

    # antenna_metrics_week
    antenna_metrics_week_df = gn\
        .aggregate_chunks(feature_type='week', attributes=att,
                          sparksession=spark, cache_table=False,
                          query=gn.queries.level2.antenna_metrics_agg_query(
                            table_name='table_antenna_metrics_week_df',
                            columns='week_part,week_number'))

    # antenna_metrics_hourly
    antenna_metrics_hourly_df = gn\
        .aggregate_chunks(feature_type='hourly', attributes=att,
                          sparksession=spark, cache_table=False,
                          query=gn.queries.level2.antenna_metrics_agg_query(
                            table_name='table_antenna_metrics_hourly_df',
                            columns='hour'))

    # antenna_interactions
    antenna_interactions_df = gn\
        .aggregate_chunks(feature_type='interactions', attributes=att,
                          sparksession=spark, cache_table=False,
                          query=gn.queries.level2
                          .antenna_interactions_agg_query(
                           table_name='table_antenna_metrics_interactions_df'))

    # bandicoot_features
    if att.bc_flag and not att.hdfs_flag:
        # load in all items
        antenna_bandicoot_df = gn\
            .sdf_from_folder(att.antenna_features_path + 'bc/', attributes=att,
                             sparksession=spark, recursive=True)
        # get total sum of weights (should be equal to total number of users!)
        sum_weights = antenna_bandicoot_df.select('count').groupBy().sum()\
            .collect()[0][0]
        # apply weighing (multiply with weight and divide by all weights)
        antenna_bandicoot_df = antenna_bandicoot_df\
            .select('antenna_id', *[(col(col_name)*col('count')/sum_weights)
                                    .alias(col_name) for col_name in
                                    antenna_bandicoot_df.columns[1:]])
        # sum up per antenna_id
        antenna_bandicoot_df = antenna_bandicoot_df.groupBy('antenna_id').\
            sum().drop('sum(antenna_id)', 'sum(count)')

        # renaming to more readable column names
        clean_cols = [c.replace('sum(', '').replace(')', '')
                      for c in antenna_bandicoot_df.columns]
        antenna_bandicoot_df = antenna_bandicoot_df.toDF(*clean_cols)

        print('Aggregated chunks for bandicoot!')

    # **Level 3**: Intermediate feature tables
    #
    # Datasets holding different features/variables with columns in brackets:
    # + **alltime_features**: *(antenna_id, calls_ratio, sms_ratio, vol_ratio,
    #  sms2calls_ratio)*
    # + **active_users_features**: *(antenna_id, active_users)*
    # + **interaction_features**: *(antenna_id, calls_dist_mean, sms_dist_mean,
    #  calls_isolation, sms_isolation, calls_entropy, sms_entropy, dist2c)*
    # + **variance_features**: *(antenna_id, calls_ratio_var, sms_ratio_var,
    #  vol_ratio_var)*
    # + **daily_features**: *(antenna_id, og_calls_week_ratio,
    #  og_sms_week_ratio, og_vol_week_ratio, ic_calls_week_ratio,
    #  ic_sms_week_ratio, ic_vol_week_ratio)*
    # + **hourly_features**: *(antenna_id, og_calls_work_ratio,
    #  og_sms_work_ratio, og_vol_work_ratio, ic_calls_work_ratio,
    #  ic_sms_work_ratio, ic_vol_work_ratio, og_calls_peak_ratio,
    #  og_sms_peak_ratio, og_vol_peak_ratio, ic_calls_peak_ratio,
    #  ic_sms_peak_ratio, ic_vol_peak_ratio)*
    # + **antenna_bandicoot_features**: alltime averaged bandicoot interactions
    #  on antenna level *(antenna_id, ...)*
    #
    # A further explanation of the single features can be found
    #  here[http://bandicoot.mit.edu/docs/reference/bandicoot.individual.html]
    print('Starting with Level 3: Intermediate feature tables.')

    # alltime_features
    alltime_features_df = spark\
        .sql(gn.queries.level3.alltime_features_query(
            table_name='table_antenna_metrics_week_df'))

    # active_users_features
    active_users_features_df = home_antennas.select('antenna_id', 'user_id')\
        .distinct().groupBy('antenna_id').count()\
        .selectExpr('antenna_id', 'count as active_users')

    # interaction_features
    interaction_features_df = spark\
        .sql(gn.queries.level3.interaction_features_query(
            table_name='table_antenna_metrics_interactions_df',
            c_coord=att.c_coord,
            n_home_antennas=n_home_antennas))

    # variance_features
    variance_features_df = spark\
        .sql(gn.queries.level3.variance_features_query(
            table_name='table_antenna_metrics_week_df'))

    # daily_features
    daily_features_df = spark\
        .sql(gn.queries.level3.daily_features_query(
            table_name='table_antenna_metrics_week_df'))

    # hourly_features
    hourly_features_df = spark\
        .sql(gn.queries.level3.hourly_features_query(
            table_name='table_antenna_metrics_hourly_df',
            work_day=att.work_day,
            early_peak=att.early_peak,
            late_peak=att.late_peak))

    # **Level 4**: Final feature table
    print('Starting with Level 4: Final feature table.')

    final_features_df = antennas_locations\
        .join(alltime_features_df, 'antenna_id', 'left')\
        .join(active_users_features_df, 'antenna_id', 'left')\
        .join(interaction_features_df, 'antenna_id', 'left')\
        .join(variance_features_df, 'antenna_id', 'left')\
        .join(daily_features_df, 'antenna_id', 'left')\
        .join(hourly_features_df, 'antenna_id', 'left')

    if att.bc_flag and not att.hdfs_flag:
        final_features_df = final_features_df.join(antenna_bandicoot_df,
                                                   'antenna_id', 'left')

    # save final output
    final_features_df.coalesce(1).write.csv('final_features', mode='overwrite',
                                            header=True)
    # rename final file if local
    if not att.hdfs_flag:
        os.rename(glob.glob('final_features/*.csv')[0], 'final_features.csv')
        shutil.rmtree('final_features')

    spark.stop()
    if att.clean_up:
        if att.hdfs_flag:
                (ret, out, err) = gn.run_cmd(['hdfs', 'dfs', '-rm', '-R',
                                              att.antenna_features_path])
                (ret, out, err) = gn.run_cmd(['hdfs', 'dfs', '-rm', '-R',
                                              att.home_antennas_path])
        else:
            shutil.rmtree(att.antenna_features_path)
            shutil.rmtree(att.home_antennas_path)

    print('DONE! Features file saved to final_features/.')
