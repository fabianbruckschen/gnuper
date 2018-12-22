"""Queries as functions with SQL Syntax taking in an origin table name -
Level 2."""


def antenna_metrics_week_query(table_name='%(table_name)s',
                               weekend_days=(6,7),
                               holidays=('')):
    """
    Further aggregate user event counts by part of the week (i.e. weekend,
    holiday or workday) as well as the week of the year therefore
    ignoring the hour. Most importantly aggregate up to the antenna level
    for all users who are predicted to live inside an antennas coverage
    (home antenna).

    Inputs
    ------
    table_name : name of the table the query is supposed to run on,
                 defaulting to '%(table_name)s', i.e. no substitution
    weekend_days : days of the weeks which represent the weekend, input by
                   numbers of the days of the week (Monday = 1, etc.) in a
                   tuple, e.g. (6,7)
    holidays : days during the timespan of interest which are national holidays,
               in a tuple, e.g. ('2018-12-15', '2018-12-26')

    Output
    ------
    Part-weekly (weekend, holiday and working days) antenna level aggregates of
    event counts, no more user ids.
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
            IF(DATE_FORMAT(day, 'u') IN %(weekend_days)s, 'weekend',
                IF(day IN %(holidays)s, 'holiday',
                    'workday')) as week_part, -- identify weekend and holidays
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
                    'weekend_days': weekend_days,
                    'holidays': holidays}


def antenna_metrics_hourly_query(table_name='%(table_name)s'):
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


def antenna_interactions_query(table_name='%(table_name)s'):
    """
    Per each antenna pair which had at least one event occurring, three
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


def antenna_metrics_agg_query(columns, table_name='%(table_name)s'):
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


def antenna_interactions_agg_query(table_name='%(table_name)s'):
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
