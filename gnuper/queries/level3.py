"""Queries as functions with SQL Syntax taking in an origin table name -
Level 3."""


def alltime_features_query(table_name='%(table_name)s'):
    """
    Create alltime features which are part of the final feature set.
    All features are either independent of total magnitude or scaled.

    X_ratio: This feature category calculates the ratio of outgoing OVER
             incoming events over the whole time period.
    scaled_X: This feature category scales event or volume to be bound between
              0 and 1 relative to all other antennas.

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


def variance_features_query(table_name='%(table_name)s'):
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


def daily_features_query(table_name='%(table_name)s'):
    """
    Create daily features which are part of the final feature set.
    For outgoing and incoming events separately, the percentages of
    number of events (or total duration) happening on the weekend or holidays
    are being calculated. These features are bound between 0 and 1.

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
          -- og week ratios (weekend/holidays over all week)
          SUM(IF(week_part IN ('weekend', 'holiday'), og_calls, NULL))/
            SUM(og_calls) as og_calls_week_ratio,
          SUM(IF(week_part IN ('weekend', 'holiday'), og_sms, NULL))/
            SUM(og_sms) as og_sms_week_ratio,
          SUM(IF(week_part IN ('weekend', 'holiday'), og_vol, NULL))/
            SUM(og_vol) as og_vol_week_ratio,
          -- ic week ratios (weekend/holidays over all week)
          SUM(IF(week_part IN ('weekend', 'holiday'), ic_calls, NULL))/
            SUM(ic_calls) as ic_calls_week_ratio,
          SUM(IF(week_part IN ('weekend', 'holiday'), ic_sms, NULL))/
            SUM(ic_sms) as ic_sms_week_ratio,
          SUM(IF(week_part IN ('weekend', 'holiday'), ic_vol, NULL))/
            SUM(ic_vol) as ic_vol_week_ratio
        FROM %(table_name)s
        GROUP BY antenna_id
    """
    return query % {'table_name': table_name}


def hourly_features_query(table_name='%(table_name)s',
                          work_day={'begin': 9, 'end': 17},
                          early_peak={'begin': 3, 'end': 5},
                          late_peak={'begin': 10, 'end': 12}):
    """
    Create hourly features which are part of the final feature set.
    For outgoing and incoming events separately, the percentages of
    number of events (or total duration) happening during several
    predefined periods (input as dictionary with begin and end keys) are being
    calculated. There are three periods:
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


def interaction_features_query(table_name='%(table_name)s',
                               c_coord={'latitude': 52.52437,
                                        'longitude': 13.41053},
                               n_home_antennas=1):
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
               with the formula for the Shannon Entropy,
               -sum(probability*log(2, probability)), where probability is
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
          -SUM(calls_proportion*LOG(2, calls_proportion)) as calls_entropy,
          -SUM(sms_proportion*LOG(2, sms_proportion)) as sms_entropy,
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
