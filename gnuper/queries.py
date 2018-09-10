
class Queries:
    """Queries needed for preprocessing script as a class."""

    # General queries
    def raw_locations_query(self, table_name='%(table_name)s'):
        """
        Extract raw locations of cells and antennas while getting rid of
        potential duplicates.

        Inputs
        ------
        table_name : name of the table the query is supposed to run on,
                     defaulting to '%(table_name)s', i.e. no substitution

        Output
        ------
        GPS information of availables cells and antennas.
        """
        query = """
            SELECT
              cell_id,
              antenna_id,
              MEAN(longitude) as longitude,
              MEAN(latitude) as latitude
            FROM
            (
                SELECT
                  cell_id,
                  antenna_id,
                  ROUND(longitude, 6) as longitude, -- 6 digits are sufficient
                  ROUND(latitude, 6) as latitude -- to identify humans
                FROM %(table_name)s
            )
            GROUP BY cell_id, antenna_id -- kick duplicates
        """
        return query % {'table_name': table_name}

    def get_user_ids_query(self, table_name='%(table_name)s'):
        """
        Extract unique available user ids per raw data file.

        Inputs
        ------
        table_name : name of the table the query is supposed to run on,
                     defaulting to '%(table_name)s', i.e. no substitution

        Output
        ------
        Unique caller_ids.
        """
        query = """
            SELECT
              CALLER_MSISDN as caller_id
            FROM %(table_name)s
            GROUP BY CALLER_MSISDN -- only keep uniques
        """
        return query % {'table_name': table_name}

    def chunking_query(self, max_chunksize, table_name='%(table_name)s'):
        """
        Create a chunk_id which is based on the unique users (caller_id) and
        the maximum size of users per chunk defined in attributes.

        Inputs
        ------
        table_name : name of the table the query is supposed to run on,
                     defaulting to '%(table_name)s', i.e. no substitution
        max_chunksize : maximum number of users per chunk

        Output
        ------
        Crosswalk between caller_id and chunk_id.
        """
        query = """
            SELECT
              caller_id,
              -- as soon as chunk limit is reached the id increases by 1
              SMALLINT((counter-1) / %(max_chunksize)s)+1 as chunk_id
            FROM
            (
              SELECT
                caller_id,
                ROW_NUMBER() OVER (ORDER BY caller_id) as counter -- no ties
              FROM %(table_name)s
              GROUP BY caller_id
            )
        """
        return query % {'table_name': table_name,
                        'max_chunksize': max_chunksize}

    # Level 0 queries
    def raw_preprocessing_query(self, cump, table_name='%(table_name)s'):
        """
        Preprocess initial raw data by making columns more readable and
        transform data into unified format (timestamp, seconds as duration,
        etc.).
        Only keep relevant information as well, e.g. no cell_id but
        solely antenna_id information and no TAC code.
        Finally add the chunk_id per event based on caller_id.

        Inputs
        ------
        table_name : name of the table the query is supposed to run on,
                     defaulting to '%(table_name)s', i.e. no substitution
        cump : stands for *C*all-*U*nit-*M*ulti*P*lier which is 60 if the input
               is minutes and 1 for seconds

        Output
        ------
        Readable dataframe of raw events, keeping bare minimum of
        information needed for further processing.
        """
        query = """
            SELECT
              CALLER_MSISDN as caller_id,
              CASE BASIC_SERVICE WHEN 2 THEN 'text' ELSE 'call' END
               as interaction,
              CASE CALL_RECORD_TYPE WHEN 1 THEN 'in' ELSE 'out' END
               as direction,
              CALL_PARTNER_IDENTITY_TYPE as national,
              CALL_PARTNER_IDENTITY as correspondent_id,
              CAST(UNIX_TIMESTAMP(CALL_DATE, 'dd MMM yyyy HH:mm:ss')
               AS TIMESTAMP) as datetime, -- transform to proper timestamp
              INT(ROUND(CALL_DURATION*%(cump)s))
               as call_duration, -- calculate duration in seconds
              ms.antenna_id as antenna_id,
              ci.chunk_id as chunk_id
            FROM %(table_name)s cd
            JOIN table_raw_locations ms
            ON cd.MS_LOCATION = ms.cell_id
            JOIN table_chunk_ids ci
            ON cd.CALLER_MSISDN = ci.caller_id
            WHERE CALL_RECORD_TYPE IN (1,2)
              AND BASIC_SERVICE IN (1,2)
            """
        return query % {'table_name': table_name,
                        'cump': cump}

    def filter_machines_query(self, max_weekly_interactions,
                              table_name='%(table_name)s'):
        """
        Filter out user ids which have more weekly interactions than the
        defined threshold in the attributes class. This query's purpose is
        to ignore machines and multi-user phones from the following analysis.

        Inputs
        ------
        table_name : name of the table the query is supposed to run on,
                     defaulting to '%(table_name)s', i.e. no substitution
        max_weekly_interactions : maximum number of weekly interactions to
                                  still count as a single user phone
                                  (not machine or multi-user)

        Output
        ------
        Filtered preprocessed dataframe which holds only user_ids
        which are relevant for the feature creation.
        """
        query = """
            SELECT
              raw.caller_id as caller_id,
              interaction,
              direction,
              national,
              correspondent_id,
              datetime,
              call_duration,
              antenna_id
            FROM %(table_name)s raw
            LEFT JOIN
            (
                SELECT
                  caller_id,
                  TRUE as is_machine -- only keep ids which should drop out
                FROM
                (
                    SELECT
                      caller_id,
                      DATE_FORMAT(DATE(datetime), 'w') as week_number,
                      COUNT(1) as n_interactions
                    FROM %(table_name)s
                    -- count interactions per week
                    GROUP BY caller_id, week_number
                )
                WHERE n_interactions > %(max_weekly_interactions)s
                GROUP BY caller_id
            ) filter -- identified machines or multiuser phones
            ON raw.caller_id = filter.caller_id
            WHERE is_machine IS NULL -- only keep non-machines (aka no match)
        """
        return query % {'table_name': table_name,
                        'max_weekly_interactions': max_weekly_interactions}

    # Level 1 queries
    def user_home_antenna_query(self, noct_time, table_name='%(table_name)s'):
        """
        Define a monthly home antenna per user, i.e. where does a user have
        the most interactions during that month during the defined
        (attributes class) night time window.

        Inputs
        ------
        table_name : name of the table the query is supposed to run on,
                     defaulting to '%(table_name)s', i.e. no substitution
        noct_time : window of beginning and end of nocturnal time as dict with
                    entries 'begin' and 'end'

        Output
        ------
        Crosswalk of user_ids and predicted home antenna per month.
        """
        query = """
            SELECT
              user_id,
              antenna_id
            FROM
            (
              SELECT
                user_id,
                antenna_id,
                ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY events DESC)
                 as antenna_rank -- ROW_NUMBER instead of RANK (no ties)
              FROM
              (
                SELECT
                  user_id,
                  antenna_id,
                  COUNT(*) as events -- count events per user and antenna
                FROM
                (
                  SELECT
                    caller_id as user_id,
                    antenna_id
                  FROM %(table_name)s
                  -- infuse nocturnal beginning
                  WHERE HOUR(datetime) BETWEEN %(noct_begin)s AND 23
                    OR HOUR(datetime) < %(noct_end)s -- infuse nocturnal end
                ) -- get active antennas per user during night time
                GROUP BY user_id, antenna_id
              ) nocturnal
            )
            -- select only 1 antenna per user, the one with the most events
            WHERE antenna_rank = 1
        """
        return query % {'table_name': table_name,
                        'noct_begin': noct_time['begin'],
                        'noct_end': noct_time['end']}

    def user_metrics_query(self, table_name='%(table_name)s'):
        """
        Create aggregated features per user, day and hour. E.g. count all
        outgoing sms to create an indicator for that time period called
        og_sms. Incoming events are abbreviated with 'ic'.

        Inputs
        ------
        table_name : name of the table the query is supposed to run on,
                     defaulting to '%(table_name)s', i.e. no substitution

        Output
        ------
        Aggregated event counts per user, day and hour.
        """
        query = """
            SELECT
              user_id,
              day,
              hour,
              SUM(IF(direction = 'out' AND interaction = 'text', value, NULL))
               AS og_sms,
              SUM(IF(direction = 'in' AND interaction = 'text', value, NULL))
               AS ic_sms,
              SUM(IF(direction = 'out' AND interaction = 'call', value, NULL))
               AS og_calls,
              SUM(IF(direction = 'in' AND interaction = 'call', value, NULL))
               AS ic_calls,
              SUM(IF(direction = 'out' AND interaction = 'call', vol, NULL))
               AS og_vol,
              SUM(IF(direction = 'in' AND interaction = 'call', vol, NULL))
               AS ic_vol
            FROM
            (
              SELECT
                caller_id as user_id,
                DATE(datetime) as day,
                HOUR(datetime) as hour,
                interaction,
                direction,
                COUNT(*) as value,
                SUM(call_duration) as vol
              FROM %(table_name)s
              GROUP BY caller_id, day, hour, interaction, direction
            )
            GROUP BY user_id, day, hour
        """
        return query % {'table_name': table_name}

    def bc_metrics_query(self, table_name='%(table_name)s'):
        """
        Further preprocess raw dataframe such that the format fits the bandi-
        coot expectations. That means timestamps as string and no NULL values
        for call durations. Only keep national events and add the antenna
        coordinates.

        Inputs
        ------
        table_name : name of the table the query is supposed to run on,
                     defaulting to '%(table_name)s', i.e. no substitution

        Output
        ------
        Dataframe ready for processing by bandicoot module.
        """
        query = """
            SELECT
              caller_id,
              interaction,
              direction,
              correspondent_id,
              CAST(CAST(datetime AS TIMESTAMP) as STRING) as datetime,
              IFNULL(CAST(call_duration as STRING), '') as call_duration,
              cd.antenna_id as antenna_id,
              latitude,
              longitude
            FROM %(table_name)s cd
            JOIN table_antennas_locations al
            ON cd.antenna_id = al.antenna_id
            WHERE national = 1
        """
        return query % {'table_name': table_name}

    # Level 2 queries
    def antenna_metrics_week_query(self, weekend_days,
                                   table_name='%(table_name)s'):
        """
        Further aggregate user event counts by part of the week (i.e. weekend
        or week days) as well as the week of the year itself, therefore
        ignoring the hour. Most importantly aggregate up to the antenna level
        for all users who are predicted to live inside an antennas coverage
        (home antenna).

        Inputs
        ------
        table_name : name of the table the query is supposed to run on,
                     defaulting to '%(table_name)s', i.e. no substitution
        weekend_days : days of the weeks which represent the weekend, input by
                       numbers of the days of the week (Monday = 1, etc.) in an
                       array, e.g. [6,7]

        Output
        ------
        Half-weekly (weekend and week days) antenna level aggregates of event
        counts, no more user ids.
        """
        query = """
            SELECT
              antenna_id,
              week_part,
              week_number,
              SUM(og_sms) as og_sms,
              SUM(ic_sms) as ic_sms,
              SUM(og_calls) as og_calls,
              SUM(ic_calls) as ic_calls,
              SUM(og_vol) as og_vol,
              SUM(ic_vol) as ic_vol
            FROM
            (
              SELECT
                antenna_id,
                day,
                IF(DATE_FORMAT(day, 'u') IN %(weekend_days)s,
                 'weekend', 'workday') as week_part, -- identify weekend days
                DATE_FORMAT(day, 'w') as week_number, -- identify exact week
                og_sms,
                ic_sms,
                og_calls,
                ic_calls,
                og_vol,
                ic_vol
              FROM %(table_name)s um
              JOIN table_user_home_antenna_df uha
              ON um.user_id = uha.user_id
            )
            GROUP BY antenna_id, week_part, week_number
        """
        return query % {'table_name': table_name,
                        'weekend_days': weekend_days}

    def antenna_metrics_hourly_query(self, table_name='%(table_name)s'):
        """
        Further aggregate user event counts by the hour of the day, therefore
        ignoring the day and week. Most importantly aggregate up to the
        antenna level for all users who are predicted to live inside an
        antennas coverage (home antenna).

        Inputs
        ------
        table_name : name of the table the query is supposed to run on,
                     defaulting to '%(table_name)s', i.e. no substitution

        Output
        ------
        Hourly antenna level aggregates of event counts,
        no more user ids.
        """
        query = """
            SELECT
              antenna_id,
              hour,
              SUM(og_sms) as og_sms,
              SUM(ic_sms) as ic_sms,
              SUM(og_calls) as og_calls,
              SUM(ic_calls) as ic_calls,
              SUM(og_vol) as og_vol,
              SUM(ic_vol) as ic_vol
            FROM %(table_name)s um
            JOIN table_user_home_antenna_df uha
            ON um.user_id = uha.user_id
            GROUP BY antenna_id, hour
        """
        return query % {'table_name': table_name}

    def antenna_interactions_query(self, table_name='%(table_name)s'):
        """
        Per each antenna pair which had at least one event occuring, three
        features are being created. Counts of all sms and calls as well as
        the summed up seconds of all calls.

        Inputs
        ------
        table_name : name of the table the query is supposed to run on,
                     defaulting to '%(table_name)s', i.e. no substitution

        Output
        ------
        Dataframe which holds information about interactions between
        each pair of antennas. No more user ids.
        """
        query = """
            SELECT
              antenna_id1,
              antenna_id2,
              SUM(IF(interaction = 'text', 1, NULL)) as sms_count,
              SUM(IF(interaction = 'call', 1, NULL)) as call_count,
              SUM(call_duration) as vol_sum
            FROM
            (
              SELECT
                caller_id,
                correspondent_id,
                uha1.antenna_id as antenna_id1,
                uha2.antenna_id as antenna_id2,
                interaction,
                call_duration
              FROM %(table_name)s rd
              JOIN table_user_home_antenna_df uha1
              -- get home antenna antenna for caller
              ON rd.caller_id = uha1.user_id
              JOIN table_user_home_antenna_df uha2
              -- get home antenna for recipient
              ON rd.correspondent_id = uha2.user_id
            )
            GROUP BY antenna_id1, antenna_id2
        """
        return query % {'table_name': table_name}

    def antenna_metrics_agg_query(self, columns, table_name='%(table_name)s'):
        """
        Aggregated dataframe of antenna metrics over all chunks by given column
        names.

        Inputs
        ------
        table_name : name of the table the query is supposed to run on,
                     defaulting to '%(table_name)s', i.e. no substitution
        columns : names of columns to aggregate by, e.g. 'hour' or
                  'week_part, week_number'

        Output
        ------
        Aggregated dataframe of antenna metrics over all chunks by given column
        names.
        """
        query = """
            SELECT
              antenna_id,
              %(columns)s,
              SUM(og_sms) as og_sms,
              SUM(ic_sms) as ic_sms,
              SUM(og_calls) as og_calls,
              SUM(ic_calls) as ic_calls,
              SUM(og_vol) as og_vol,
              SUM(ic_vol) as ic_vol
            FROM %(table_name)s
            GROUP BY antenna_id, %(columns)s
        """
        return query % {'table_name': table_name,
                        'columns': columns}

    def antenna_interactions_agg_query(self, table_name='%(table_name)s'):
        """
        Aggregated dataframe of antenna interactions over all chunks.

        Inputs
        ------
        table_name : name of the table the query is supposed to run on,
                     defaulting to '%(table_name)s', i.e. no substitution

        Output
        ------
        Aggregated dataframe of antenna interactions over all chunks.
        """
        query = """
            SELECT
              antenna_id1,
              antenna_id2,
              SUM(sms_count) as sms_count,
              SUM(call_count) as call_count,
              SUM(vol_sum) as vol_sum
            FROM %(table_name)s
            GROUP BY antenna_id1, antenna_id2
        """
        return query % {'table_name': table_name}

    # Level 3 queries
    def alltime_features_query(self, table_name='%(table_name)s'):
        """
        Create alltime features which are part of the final feature set.
        All features are either independent of total magnitude or scaled.
        X_ratio: Feature calculates the ratio of outgoing OVER incoming
                 events over the whole time period.
        scaled_X: Feature scales event or volume to be bound between 0 and 1
                  relative to all other antennas.

        Inputs
        ------
        table_name : name of the table the query is supposed to run on,
                     defaulting to '%(table_name)s', i.e. no substitution

        Output
        ------
        Partial final features dataframe which holds features
        calculated over the whole time period.
        """
        query = """
            SELECT
              antenna_id,
              og_calls/ic_calls as calls_ratio,
              og_sms/ic_sms as sms_ratio,
              og_vol/ic_vol as vol_ratio,
              (og_sms + ic_sms)/(og_calls + ic_calls) as sms2calls_ratio,
              (og_calls - MIN(og_calls) OVER())/(MAX(og_calls) OVER() -
               MIN(og_calls) OVER()) as scaled_og_calls,
              (og_sms - MIN(og_sms) OVER())/(MAX(og_sms) OVER() -
               MIN(og_sms) OVER()) as scaled_og_sms,
              (og_vol - MIN(og_vol) OVER())/(MAX(og_vol) OVER() -
               MIN(og_vol) OVER()) as scaled_og_vol,
              (ic_calls - MIN(ic_calls) OVER())/(MAX(ic_calls) OVER() -
               MIN(ic_calls) OVER()) as scaled_ic_calls,
              (ic_sms - MIN(ic_sms) OVER())/(MAX(ic_sms) OVER() -
               MIN(ic_sms) OVER()) as scaled_ic_sms,
              (ic_vol - MIN(ic_vol) OVER())/(MAX(ic_vol) OVER() -
               MIN(ic_vol) OVER()) as scaled_ic_vol
            FROM
            (
              SELECT
                antenna_id,
                SUM(og_sms) as og_sms,
                SUM(ic_sms) as ic_sms,
                SUM(og_calls) as og_calls,
                SUM(ic_calls) as ic_calls,
                SUM(og_vol) as og_vol,
                SUM(ic_vol) as ic_vol
              FROM %(table_name)s
              GROUP BY antenna_id
            )
        """
        return query % {'table_name': table_name}

    def variance_features_query(self, table_name='%(table_name)s'):
        """
        Create variance features which are part of the final feature set.
        For all ratio variables (outgoing over incoming of calls, sms and
        call volume in seconds) the variance over all weeks is being
        calculated. This feature tries to capture the volatility of the
        relation between outgoing and incoming traffic over time.

        Inputs
        ------
        table_name : name of the table the query is supposed to run on,
                     defaulting to '%(table_name)s', i.e. no substitution

        Output
        ------
        Partial final features dataframe which holds features
        calculated over the variance over weekly ratios.
        """
        query = """
            SELECT
              antenna_id,
              -- variance over these ratios
              VARIANCE(calls_ratio) as calls_ratio_var,
              VARIANCE(sms_ratio) as sms_ratio_var,
              VARIANCE(vol_ratio) as vol_ratio_var
            FROM
            (
              SELECT
                antenna_id,
                week_number,
                -- ratios per week
                SUM(og_calls)/SUM(ic_calls) as calls_ratio,
                SUM(og_sms)/SUM(ic_sms) as sms_ratio,
                SUM(og_vol)/SUM(ic_vol) as vol_ratio
              FROM %(table_name)s
              GROUP BY antenna_id, week_number
            )
            GROUP BY antenna_id
        """
        return query % {'table_name': table_name}

    def daily_features_query(self, table_name='%(table_name)s'):
        """
        Create daily features which are part of the final feature set.
        For outgoing and incoming events separately, the percentages of
        number of events (or total duration) happening on the weekend are
        being calculated. These features are bound between 0 and 1.

        Inputs
        ------
        table_name : name of the table the query is supposed to run on,
                     defaulting to '%(table_name)s', i.e. no substitution

        Output
        ------
        Partial final features dataframe which holds features
        calculated on the weekends over the whole weeks.
        """
        query = """
            SELECT
              antenna_id,
              -- og week ratios (weekend over all week)
              SUM(IF(week_part = 'weekend', og_calls, NULL))/SUM(og_calls)
               as og_calls_week_ratio,
              SUM(IF(week_part = 'weekend', og_sms, NULL))/SUM(og_sms)
               as og_sms_week_ratio,
              SUM(IF(week_part = 'weekend', og_vol, NULL))/SUM(og_vol)
               as og_vol_week_ratio,
              -- ic week ratios (weekend over all week)
              SUM(IF(week_part = 'weekend', ic_calls, NULL))/SUM(ic_calls)
               as ic_calls_week_ratio,
              SUM(IF(week_part = 'weekend', ic_sms, NULL))/SUM(ic_sms)
               as ic_sms_week_ratio,
              SUM(IF(week_part = 'weekend', ic_vol, NULL))/SUM(ic_vol)
               as ic_vol_week_ratio
            FROM %(table_name)s
            GROUP BY antenna_id
        """
        return query % {'table_name': table_name}

    def hourly_features_query(self, work_day, early_peak, late_peak,
                              table_name='%(table_name)s'):
        """
        Create hourly features which are part of the final feature set.
        For outgoing and incoming events separately, the percentages of
        number of events (or total duration) happening during several
        predefined periods (attributes class) are being calculated. There are
        three periods:
        workday (wd): Defaults to 9 am to 5 pm. General working hours.
        early peak (ep): Defaults to 3 am to 5 am. Tries to catch blue collar
                         or rural activity.
        late peak (lp): Defaults to 10 am to 12 pm. Tries to catch late
                        risers and white collar activity.
        All these features are bound between 0 and 1.

        Inputs
        ------
        table_name : name of the table the query is supposed to run on,
                     defaulting to '%(table_name)s', i.e. no substitution

        Output
        ------
        Partial final features dataframe which holds features
        calculated on the different time periods during the day.
        """
        query = """
            SELECT
              antenna_id,
              -- og work ratios (workday over whole day)
              SUM(IF(hour BETWEEN %(wd_begin)s AND %(wd_end)s, og_calls, NULL))
               /SUM(og_calls) as og_calls_work_ratio,
              SUM(IF(hour BETWEEN %(wd_begin)s AND %(wd_end)s, og_sms, NULL))
               /SUM(og_sms) as og_sms_work_ratio,
              SUM(IF(hour BETWEEN %(wd_begin)s AND %(wd_end)s, og_vol, NULL))
               /SUM(og_vol) as og_vol_work_ratio,
              -- ic work ratios (workday over whole day)
              SUM(IF(hour BETWEEN %(wd_begin)s AND %(wd_end)s, ic_calls, NULL))
               /SUM(ic_calls) as ic_calls_work_ratio,
              SUM(IF(hour BETWEEN %(wd_begin)s AND %(wd_end)s, ic_sms, NULL))
               /SUM(ic_sms) as ic_sms_work_ratio,
              SUM(IF(hour BETWEEN %(wd_begin)s AND %(wd_end)s, ic_vol, NULL))
               /SUM(ic_vol) as ic_vol_work_ratio,
              -- og peak ratios (early & late peak respectively over whole day)
              SUM(IF(hour BETWEEN %(ep_begin)s AND %(ep_end)s, og_calls,
               NULL))/
                SUM(IF(hour BETWEEN %(lp_begin)s AND %(lp_end)s, og_calls,
                 NULL)) as og_calls_peak_ratio,
              SUM(IF(hour BETWEEN %(ep_begin)s AND %(ep_end)s, og_sms, NULL))/
                SUM(IF(hour BETWEEN %(lp_begin)s AND %(lp_end)s, og_sms, NULL))
                as og_sms_peak_ratio,
              SUM(IF(hour BETWEEN %(ep_begin)s AND %(ep_end)s, og_vol, NULL))/
                SUM(IF(hour BETWEEN %(lp_begin)s AND %(lp_end)s, og_vol, NULL))
                 as og_vol_peak_ratio,
              -- ic peak ratios (early & late peak respectively over whole day)
              SUM(IF(hour BETWEEN %(ep_begin)s AND %(ep_end)s, ic_calls,
               NULL))/
                SUM(IF(hour BETWEEN %(lp_begin)s AND %(lp_end)s, ic_calls,
                 NULL)) as ic_calls_peak_ratio,
              SUM(IF(hour BETWEEN %(ep_begin)s AND %(ep_end)s, ic_sms, NULL))/
                SUM(IF(hour BETWEEN %(lp_begin)s AND %(lp_end)s, ic_sms,
                 NULL)) as ic_sms_peak_ratio,
              SUM(IF(hour BETWEEN %(ep_begin)s AND %(ep_end)s, ic_vol, NULL))/
                SUM(IF(hour BETWEEN %(lp_begin)s AND %(lp_end)s, ic_vol,
                 NULL)) as ic_vol_peak_ratio

            FROM %(table_name)s
            GROUP BY antenna_id
        """
        return query % {'table_name': table_name,
                        'wd_begin': work_day['begin'],
                        'wd_end': work_day['end'],
                        'ep_begin': early_peak['begin'],
                        'ep_end': early_peak['end'],
                        'lp_begin': late_peak['begin'],
                        'lp_end': late_peak['end']}

    def interaction_features_query(self, c_coord, n_home_antennas,
                                   table_name='%(table_name)s'):
        """
        Create interaction features which are part of the final feature set.
        For both event types (call, sms) three features are being created.
        X_dist_mean: The average distance in km of an event where the antenna
                     was part of. This feature tries to capture an indication
                     of reach per antenna.
        X_isolation: Simply the percentage of how many unique antennas an
                     antenna has had interactions with. This feature is bound
                     between 0 and 1.
        X_entropy: Entropy means the informative content of an antenna based
                   on the interactions of all antennas. It's being calculated
                   with the basic formula for entropy,
                   -sum(probability*ln(probability)), where probability is
                   based on the percentage of events happening between each
                   pair of antennas over all events.
        Additionally the feature dist2c tracks the distance of an antenna to
        the GPS coordinates of a capital as defined in the attributes class.

        Inputs
        ------
        table_name : name of the table the query is supposed to run on,
                     defaulting to '%(table_name)s', i.e. no substitution

        Output
        ------
        Partial final features dataframe which holds features
        calculated over interactions of antenna pairs.
        """
        query = """
            SELECT
              antenna_id1 as antenna_id,
              -- distance
              SUM(call_count*dist_km)/SUM(call_count) as calls_dist_mean,
              SUM(sms_count*dist_km)/SUM(sms_count) as sms_dist_mean,
              -- isolation
              COUNT(DISTINCT IF(NOT ISNULL(call_count), antenna_id2, NULL))/
               %(n_home_antennas)s as calls_isolation,
              COUNT(DISTINCT IF(NOT ISNULL(sms_count), antenna_id2, NULL))/
               %(n_home_antennas)s as sms_isolation,
              -- entropy
              -SUM(calls_proportion*LN(calls_proportion)) as calls_entropy,
              -SUM(sms_proportion*LN(sms_proportion)) as sms_entropy,
              dist2c
            FROM
            (
              SELECT
                antenna_id1,
                antenna_id2,
                sms_count,
                call_count,
                sms_count/all_sms as sms_proportion,
                call_count/all_calls as calls_proportion,
                -- haversine distance formula
                2 * asin(
                     sqrt(
                       cos(radians(lat1)) *
                       cos(radians(lat2)) *
                       pow(sin(radians((lon1 - lon2)/2)), 2)
                           +
                       pow(sin(radians((lat1 - lat2)/2)), 2)

                     )
                   ) * 6371 dist_km,
                 2 * asin(
                      sqrt(
                        cos(radians(lat1)) *
                        cos(radians(%(lat_c)s)) *
                        pow(sin(radians((lon1 - %(lon_c)s)/2)), 2)
                            +
                        pow(sin(radians((lat1 - %(lat_c)s)/2)), 2)

                      )
                    ) * 6371 dist2c
              FROM
              (
                SELECT
                  antenna_id1,
                  al1.latitude as lat1,
                  al1.longitude as lon1,
                  antenna_id2,
                  al2.latitude as lat2,
                  al2.longitude as lon2,
                  sms_count,
                  call_count,
                  SUM(sms_count) OVER() AS all_sms,
                  SUM(call_count) OVER() AS all_calls
                FROM %(table_name)s ai
                JOIN table_antennas_locations al1
                ON ai.antenna_id1 = al1.antenna_id
                JOIN table_antennas_locations al2
                ON ai.antenna_id2 = al2.antenna_id
              )
            )
            GROUP BY antenna_id, dist2c
        """
        return query % {'table_name': table_name,
                        'lat_c': c_coord['latitude'],
                        'lon_c': c_coord['longitude'],
                        'n_home_antennas': n_home_antennas}
