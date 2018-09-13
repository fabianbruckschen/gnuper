"""
preprocessing_script.py

This script serves as a one line execution via the command line for the whole
process. Several options can be set when started:

mp_flag : If set, several parts of the script try to spawn as many processes or
          threads as possible to speed up execution.
bc_flag : If set, the time intensive bandicoot indicators are being calculated
          as well.
no_info : If set, no additional info about data structure or size are being
          printed in order to save time.
clean_up : If set, intermediate directories and files are being deleted after
           they have been used.
raw_data_path : Can be set to an alternate path (e.g. ../test_data/), default
                is 'data' folder in the parent directory (../data/).
"""
import os  # operating system functions like renaming files and directories
import shutil  # recursive file and directory operations
import glob  # pattern matching for paths
import pandas as pd  # data mangling and transforming
import bandicoot as bc  # MIT toolkit for creating bandicoot indicators
import argparse  # entering flags from the cmd line
from pyspark.sql import SparkSession  # using spark context for big data files
from pyspark.sql.functions import col  # needed for functions over each column
from attributes_classes import Attributes  # module intern attributes
from queries import Queries  # module intern queries
from preprocessing_functions import (read_as_sdf, sdf_from_folder, union_all,
                                     bc_batch, aggregate_chunks)  # functions

# # Preparations
if __name__ == "__main__":
    # command line options
    parser = argparse.ArgumentParser()
    parser.add_argument("--mp_flag", action='store_true',
                        help="Should multiprocessing be enabled?",
                        default=False)
    parser.add_argument("--bc_flag", action='store_true',
                        help="Should bandicoot indicators be calculated?",
                        default=False)
    parser.add_argument("--no_info", action='store_true',
                        help="Should additional information be calculated?",
                        default=False)
    parser.add_argument("--clean_up", action='store_true',
                        help="Should intermediate folders and files be deleted\
                        after successful run?",
                        default=False)
    parser.add_argument("--raw_data_path", help="Location of raw data")
    parser.add_argument("--parts_to_run",
                        help="if only parts of the script should be run")
    parsed_args = parser.parse_args()

    if parsed_args.raw_data_path is None:
        raw_data_path = '../data/'
    else:
        raw_data_path = parsed_args.raw_data_path

    if parsed_args.parts_to_run is None:
        parts_to_run = 'all'
    else:
        parts_to_run = parsed_args.parts_to_run

# print info
print('3 parts will be run inside this script:\n\
Part 1: Chunking by user ids\n\
Part 2: Looping over those chunks to create antenna features\n\
Part 3: Uniting features from all sources to get 1 single output file')

# define attributes for this session
attributes = Attributes(mp_flag=parsed_args.mp_flag,
                        bc_flag=parsed_args.bc_flag,
                        no_info=parsed_args.no_info,
                        clean_up=parsed_args.clean_up,
                        raw_data_path=raw_data_path,
                        cap_lat=15.500654, cap_long=32.559899,  # capital gps
                        weekend_days=[5, 6], call_unit_multiplicator=60,
                        sparkmaster='local[3]')
# initiate queries class
q = Queries()

# # --- Part 1 --- (Preprocessing of raw files and saving by user)
spark = SparkSession.builder.master(attributes.sparkmaster)\
    .appName('cdr_extraction_part1').getOrCreate()
print('Spark environment for Part 1 created!')

# ## antennas datasets
# read cell and antenna locations into a spark dataframe (sdf)
raw_locations = read_as_sdf(file=attributes.raw_locations,
                            sparksession=spark, header=False,
                            colnames=['cell_id', 'antenna_id',
                                      'longitude', 'latitude'],
                            query=q.raw_locations_query())
# create raw table to query and cache it as we will query it for every day
raw_locations.createOrReplaceTempView('table_raw_locations')
spark.catalog.cacheTable('table_raw_locations')

# solely antenna locations as sdf (= ignore cell_id)
# FILE: save as 1 csv next to the raw data as we will need it later on
raw_locations.selectExpr('antenna_id', 'longitude', 'latitude')\
    .dropDuplicates().toPandas()\
    .to_csv(attributes.antennas_file, index=False)
print('Antenna SDF & table created!')

# ## Preprocessing
# **Level 0**: General preprocessing of raw call detail records
# Storing daily files in a unified dataframe
print('Starting with Level 0: General preprocessing of raw CDRs.')

# ### CDR datasets
if not attributes.no_info:
    # get all CDR filenames without the pre- and suffix
    raw_file_names = sorted([os.path.basename(f).replace('.csv', '')
                             for f in glob.glob(attributes.raw_data_path +
                                                '20*.csv')])
    print('Available CDR Dates: '+str(raw_file_names))  # doublechecking

# order of the raw columns
raw_colnames = ['CALL_RECORD_TYPE', 'CALLER_MSISDN', 'CALL_DATE',
                'BASIC_SERVICE', 'MS_LOCATION', 'CALL_PARTNER_IDENTITY_TYPE',
                'CALL_PARTNER_IDENTITY', 'TAC_CODE', 'CALL_DURATION']

# reading in user_ids
users = sdf_from_folder(folder=attributes.raw_data_path, attributes=attributes,
                        sparksession=spark, file_pattern='20*.csv',
                        header=False, colnames=raw_colnames,
                        query=q.get_user_ids_query(), action='union')
# drop duplicate ids and create table to query
users = users.dropDuplicates()
users.createOrReplaceTempView('table_user_ids')
# create chunk id for every user, based on max users per chunk
users_w_chunk = spark.sql(q.chunking_query(
                        table_name='table_user_ids',
                        max_chunksize=attributes.max_chunksize))
users_w_chunk.createOrReplaceTempView('table_chunk_ids')
# cache this table as well due to querying it for every day
spark.catalog.cacheTable('table_chunk_ids')

# preprocess every single file and save it split by chunk id
sdf_from_folder(folder=attributes.raw_data_path, attributes=attributes,
                sparksession=spark, file_pattern='20*.csv',
                header=False,
                colnames=raw_colnames,
                query=q.raw_preprocessing_query(
                        cump=attributes.call_unit_multiplicator),
                save_path=attributes.chunking_path, save_format='csv',
                save_header=True, save_mode='append',
                save_partition='chunk_id', action='save')
print('CSV files created for user chunks.')

# clear cached tables immediately
spark.catalog.clearCache()

# proper file naming and directory structure
# get all subdirectories in the chunking folder
subdirs = next(os.walk(attributes.chunking_path))[1]

for d in subdirs:
    c = d.replace('chunk_id=', '')  # extract chunk id
    filenames = glob.glob(attributes.chunking_path+d+'/*.csv')  # get all files
    combined_csv = pd.concat([pd.read_csv(f) for f in filenames])  # unite them
    # save as new file with chunk id
    combined_csv.to_csv(attributes.chunking_path+c+'.csv', index=False)
    shutil.rmtree(attributes.chunking_path+d)  # delete obsolete dir
print('CSV files united and renamed for user chunks.')

spark.stop()
print('DONE with Part 1! User chunks saved to chunking folder.')

# # --- Part 2 --- (create antenna indicators in a loop over the chunks)
spark = SparkSession.builder.master(attributes.sparkmaster)\
    .appName('cdr_extraction_part2').getOrCreate()
print('Spark environment for Part 2 created!')

# ## antenna locations
# read in table which was created in part 1
antennas_locations = spark.read.csv(attributes.antennas_file,
                                    header=True,
                                    inferSchema=True)

# create raw table to query and cache it for several queries
antennas_locations.createOrReplaceTempView('table_antennas_locations')
spark.catalog.cacheTable('table_antennas_locations')

# ## User chunks
# get all chunk filenames without the pre- and suffix
chunk_file_names = sorted(glob.glob(attributes.chunking_path+'/*.csv'))
print('Available user chunks: '+str(chunk_file_names))
# save length for loop
n = len(chunk_file_names)

# collect home antennas per chunk
home_antennas_list = []

# ## LOOP
# ### --- LOOP START ---
for chunk in chunk_file_names:
    i = chunk_file_names.index(chunk)+1
    print('Starting with '+chunk+', iteration '+str(i)+' out of '+str(n))

    # read in chunk and filter out machines and multi-users
    raw_df = read_as_sdf(chunk, spark, header=True, inferSchema=False,
                         query=q.filter_machines_query(
                            max_weekly_interactions=attributes.
                            max_weekly_interactions))

    # quick look
    if not attributes.no_info and i == 1:
        # double check file format
        print('Raw aggregated SDF structure:')
        raw_df.show(10)

    # and register as table
    raw_df.createOrReplaceTempView('table_raw_df')

    # ## home antennas
    user_home_antenna_df = spark.sql(q.user_home_antenna_query(
                            noct_time=attributes.noct_time,
                            table_name='table_raw_df'))
    # save as table
    user_home_antenna_df.createOrReplaceTempView('table_user_home_antenna_df')
    # and append to ongoing list
    home_antennas_list.append(user_home_antenna_df)

    # unique users in this chunk (doublechecking)
    n_users = raw_df.select('caller_id').distinct().count()
    print('Number of users in this chunk: '+str(n_users))

    # repartitioning by user
    raw_df = raw_df.repartition(n_users, 'caller_id')

    # **Level 1**: Intermediate tables on user level (user_id still visible)

    # Datasets created in first iteration with columns in brackets:
    # + **user_metrics**: metrics aggregated per user_id, day and hour
    #  *(user_id, day, hour, og_calls, ic_calls, og_sms, ic_sms,
    #    og_vol, ic_vol)*
    # + **user_home_antenna**: monthly estimate of antenna closest to home
    #  location per user *(user_id, antenna_id, month)*
    # + **user_bandicoot_features**: bandicoot interactions on user level per
    #  month *(user_id, month, ...)*

    print('Starting with Level 1: Intermediate tables on user level \
    (user_id still visible).')

    # #### user_metrics
    user_metrics_df = spark.sql(q.user_metrics_query(
                                table_name='table_raw_df'))
    user_metrics_df.createOrReplaceTempView('table_user_metrics_df')

    # #### bandicoot_metrics
    if attributes.bc_flag:
        # remove files from potential previous run
        if os.path.exists(attributes.bandicoot_path):
            shutil.rmtree(attributes.bandicoot_path)

        bc_metrics_df = spark.sql(q.bc_metrics_query(
                                  table_name='table_raw_df'))

        # define unique users
        users = [str(u.caller_id) for u in bc_metrics_df.select('caller_id')
                 .dropDuplicates().collect()]

        # save single user files
        bc_metrics_df.coalesce(1).write.save(
                    path=attributes.bandicoot_path,
                    format='csv',
                    header=True,
                    mode='overwrite',
                    partitionBy='caller_id')

        # proper file naming and directory structure
        # get all subdirectories in bandicoot folder
        subdirs = next(os.walk(attributes.bandicoot_path))[1]

        for d in subdirs:
            u = d.replace('caller_id=', '')  # extract user id
            # rename csv to user id and move up
            os.rename(glob.glob(attributes.bandicoot_path+d+'/*.csv')[0],
                      attributes.bandicoot_path+u+'.csv')
            shutil.rmtree(attributes.bandicoot_path+d)  # delete obsolete dir
        print('Single User Files for bandicoot created for chunk '+str(i))

        # execute bandicoot calculation as batch
        indicators = bc_batch(users, attributes)

        # save as csv
        bc.io.to_csv(indicators, '../bandicoot_indicators_'+str(i)+'.csv')
        print('Bandicoot files csvs created for chunk '+str(i))

        # re-read as single sdf
        bc_metrics_df = spark.read.csv('../bandicoot_indicators_' + str(i) +
                                       '.csv', header=True, inferSchema=True)

        # cleaning
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

        # clean up to save space
        if attributes.clean_up:
            shutil.rmtree(attributes.bandicoot_path)

    # **Level 2**: Intermediate tables on antenna level
    # (user_id NOT visible anymore)

    # Datasets created in second iteration with columns in brackets:
    # + **antenna_interactions_generic**: alltime interactions between antennas
    #  without allocation of home antenna locations but generic activity
    #  *(og_antenna_id, ic_antenna_id, sms_count, calls_count, vol_sum)*
    # + **antenna_metrics_week**: metrics aggregated per home antenna of
    #  individual users, week and part of the week
    #  *(antenna_id, week_part, week_number, og_calls, ic_calls, og_sms,
    #  ic_sms, og_vol, ic_vol)*
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

    # #### antenna_metrics_week
    antenna_metrics_week_df = spark.sql(q.antenna_metrics_week_query(
                                        weekend_days=tuple(attributes.
                                                           weekend_days),
                                        table_name='table_user_metrics_df'))

    # #### antenna_metrics_hourly
    antenna_metrics_hourly_df = spark.sql(q.antenna_metrics_hourly_query(
                                          table_name='table_user_metrics_df'))

    # #### antenna_interactions
    antenna_interactions_df = spark.sql(q.antenna_interactions_query(
                                        table_name='table_raw_df'))

    # uncache for memory
    spark.catalog.clearCache()

    # #### bandicoot_metrics
    if attributes.bc_flag:
        # join home_antenna
        join_cond = [bc_metrics_df.name == user_home_antenna_df.user_id]
        bc_metrics_df = bc_metrics_df\
            .join(user_home_antenna_df, join_cond, 'inner')\
            .drop('user_id', 'name', 'month')

        # keep weight as number of users adding to each antenna
        antenna_weight = bc_metrics_df.groupBy('antenna_id').count()

        # calculate antenna means
        antenna_bandicoot_features_df = bc_metrics_df.groupBy('antenna_id')\
            .mean().drop('avg(antenna_id)')
        # averaging drops out "delay" columns, because they are entirely empty

        # renaming the columns for better readability
        clean_cols = [c.replace('avg(', '').replace(')', '') for c in
                      antenna_bandicoot_features_df.columns]
        antenna_bandicoot_features_df = antenna_bandicoot_features_df\
            .toDF(*clean_cols)

        # add user weight
        antenna_bandicoot_features_df = antenna_bandicoot_features_df\
            .join(antenna_weight, 'antenna_id', 'inner')

    # #### save final outputs
    antenna_metrics_week_df.coalesce(1)\
        .write.csv(attributes.antenna_features_path+'week/'+str(i),
                   mode='overwrite', header=True)

    antenna_metrics_hourly_df.coalesce(1)\
        .write.csv(attributes.antenna_features_path+'hourly/'+str(i),
                   mode='overwrite', header=True)

    antenna_interactions_df.coalesce(1)\
        .write.csv(attributes.antenna_features_path+'interactions/'+str(i),
                   mode='overwrite', header=True)

    if attributes.bc_flag:
        antenna_bandicoot_features_df.coalesce(1)\
            .write.csv(attributes.antenna_features_path+'bc/'+str(i),
                       mode='overwrite', header=True)
        # remove temporary bandicoot file on user level
        os.remove('../bandicoot_indicators_'+str(i)+'.csv')

# ### --- LOOP END ---

# collect home antennas for all users
home_antennas = union_all(home_antennas_list)

# save home antennas for later
home_antennas.coalesce(1).\
    write.csv('home_antennas', header=True, mode='overwrite')
os.rename(glob.glob('home_antennas/*.csv')[0], attributes.home_antennas_file)
shutil.rmtree('home_antennas')

spark.stop()
# delete chunking files if clean up flag is set
if attributes.clean_up:
    shutil.rmtree(attributes.chunking_path)
print('DONE! Antenna features files saved to antenna_features folder.')

# # --- Part 3 --- (Weighted unification of antenna features & create single
# # output file: final_features.csv )
spark = SparkSession.builder.master(attributes.sparkmaster)\
    .appName('cdr_extraction_part3').getOrCreate()
print('Spark environment for Part 3 created!')

# ## antenna locations
# read in table
antennas_locations = spark.read.csv(attributes.antennas_file,
                                    header=True,
                                    inferSchema=True)

# create raw table to query & cache
antennas_locations.createOrReplaceTempView('table_antennas_locations')
spark.catalog.cacheTable('table_antennas_locations')

# ## home antennas
home_antennas = spark.read.csv(attributes.home_antennas_file,
                               header=True,
                               inferSchema=True)
# keep number of antennas
n_home_antennas = home_antennas.select('antenna_id').distinct().count()

# **Level 2**: Intermediate tables on antenna level
# (user_id NOT visible anymore)

# Datasets created in second iteration with columns in brackets:
# + **antenna_metrics_week**: metrics aggregated per home antenna of individual
#  users, week and part of the week *(antenna_id, week_part, week_number,
#  og_calls, ic_calls, og_sms, ic_sms, og_vol, ic_vol)*
# + **antenna_metrics_hourly**: metrics aggregated per home antenna of
#  individual users and hour
#  *(antenna_id, hour, og_calls, ic_calls, og_sms, ic_sms, og_vol, ic_vol)*
# + **antenna_interactions**: alltime interactions between antennas based on
#  the users' behavior to which a certain antenna is the homebase
#  *(antenna_id1, antenna_id2, sms_count, calls_count, vol_sum)*
# + **antenna_bandicoot**: alltime averaged bandicoot interactions on
#  antenna level *(antenna_id, ...)*
#
# A further explanation of the single features can be found
#  here[http://bandicoot.mit.edu/docs/reference/bandicoot.individual.html]
print('Starting with Level 2: Intermediate tables on antenna level\
 (user_id NOT visible anymore).')

# ### antenna_metrics_week
antenna_metrics_week_df = aggregate_chunks(
                            feature_type='week', attributes=attributes,
                            sparksession=spark, cache_table=False,
                            query=q.antenna_metrics_agg_query(
                                    table_name='table_antenna_metrics_week_df',
                                    columns='week_part,week_number'))

# ### antenna_metrics_hourly
antenna_metrics_hourly_df = aggregate_chunks(
                                feature_type='hourly', attributes=attributes,
                                sparksession=spark, cache_table=False,
                                query=q.antenna_metrics_agg_query(
                                    table_name='\
                                    table_antenna_metrics_hourly_df',
                                    columns='hour'))

# ### antenna_interactions
antenna_interactions_df = aggregate_chunks(
                            feature_type='interactions', attributes=attributes,
                            sparksession=spark, cache_table=False,
                            query=q.antenna_interactions_agg_query(
                                    table_name='\
                                    table_antenna_metrics_interactions_df'))

# ### bandicoot_features
if attributes.bc_flag:
    # load in all items
    antenna_bandicoot_df = sdf_from_folder(attributes.
                                           antenna_features_path+'bc/',
                                           attributes=attributes,
                                           sparksession=spark,
                                           recursive=True)

    # get total sum of weights (should be equal to total number of users!)
    sum_weights = antenna_bandicoot_df.select('count').groupBy().sum()\
        .collect()[0][0]

    # apply weighing (multiply with weight and divide by all weights)
    antenna_bandicoot_df = antenna_bandicoot_df\
        .select('antenna_id', *[(col(col_name)*col('count')/sum_weights)
                                .alias(col_name) for col_name in
                                antenna_bandicoot_df.columns[1:]])

    antenna_bandicoot_df = antenna_bandicoot_df.groupBy('antenna_id').\
        sum().drop('sum(antenna_id)', 'sum(count)')  # sum up per antenna_id

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
# + **daily_features**: *(antenna_id, og_calls_week_ratio, og_sms_week_ratio,
#  og_vol_week_ratio, ic_calls_week_ratio, ic_sms_week_ratio,
#  ic_vol_week_ratio)*
# + **hourly_features**: *(antenna_id, og_calls_work_ratio, og_sms_work_ratio,
#  og_vol_work_ratio, ic_calls_work_ratio, ic_sms_work_ratio,
#  ic_vol_work_ratio, og_calls_peak_ratio, og_sms_peak_ratio,
#  og_vol_peak_ratio, ic_calls_peak_ratio, ic_sms_peak_ratio,
#  ic_vol_peak_ratio)*
# + **antenna_bandicoot_features**: alltime averaged bandicoot interactions on
#  antenna level *(antenna_id, ...)*
#
# A further explanation of the single features can be found
#  here[http://bandicoot.mit.edu/docs/reference/bandicoot.individual.html]
print('Starting with Level 3: Intermediate feature tables.')

# ### alltime_features
alltime_features_df = spark.sql(q.alltime_features_query(
                                table_name='table_antenna_metrics_week_df'))

# ### active_users_features
active_users_features_df = home_antennas.select('antenna_id', 'user_id')\
    .distinct().groupBy('antenna_id').count()\
    .selectExpr('antenna_id', 'count as active_users')

# ### interaction_features
interaction_features_df = spark.sql(
                            q.interaction_features_query(
                                table_name='\
                                table_antenna_metrics_interactions_df',
                                c_coord=attributes.c_coord,
                                n_home_antennas=n_home_antennas))

# ### variance_features
variance_features_df = spark.sql(q.variance_features_query(
                                table_name='table_antenna_metrics_week_df'))

# ### daily_features
daily_features_df = spark.sql(q.daily_features_query(
                             table_name='table_antenna_metrics_week_df'))

# ### hourly_features
hourly_features_df = spark.sql(q.hourly_features_query(
                                table_name='table_antenna_metrics_hourly_df',
                                work_day=attributes.work_day,
                                early_peak=attributes.early_peak,
                                late_peak=attributes.late_peak))

# **Level 4**: Final feature table
print('Starting with Level 4: Final feature table.')

final_features_df = antennas_locations\
    .join(alltime_features_df, 'antenna_id', 'left')\
    .join(active_users_features_df, 'antenna_id', 'left')\
    .join(interaction_features_df, 'antenna_id', 'left')\
    .join(variance_features_df, 'antenna_id', 'left')\
    .join(daily_features_df, 'antenna_id', 'left')\
    .join(hourly_features_df, 'antenna_id', 'left')

if attributes.bc_flag:
    final_features_df = final_features_df.join(antenna_bandicoot_df,
                                               'antenna_id', 'left')

# ## save final output
final_features_df.coalesce(1).write.csv('../final_features', mode='overwrite',
                                        header=True)
os.rename(glob.glob('../final_features/*.csv')[0], '../final_features.csv')
shutil.rmtree('../final_features')

spark.stop()
if attributes.clean_up:
    shutil.rmtree(attributes.antenna_features_path)

print('DONE! Features file saved to final_features.csv.')
