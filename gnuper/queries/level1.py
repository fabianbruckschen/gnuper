"""Queries as functions with SQL Syntax taking in an origin table name -
Level 1."""


def user_home_antenna_query(table_name='%(table_name)s',
                            noct_time={'begin': 19, 'end': 7}):
    """
    Define a monthly home antenna per user, i.e. where does a user have
    the most interactions during that month during a defined
    night time window.

    Inputs
    ------
    table_name : Name of the table the query is supposed to run on,
                 defaulting to '%(table_name)s', i.e. no substitution.
    noct_time : Dictionary holding beginning and end of nocturnal time with
                entries 'begin' and 'end'.

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


def user_metrics_query(table_name='%(table_name)s'):
    """
    Create aggregated features per user, day and hour. E.g. count all
    outgoing sms to create an indicator for that time period called
    og_sms. Incoming events are abbreviated with 'ic'.

    Inputs
    ------
    table_name : Name of the table the query is supposed to run on,
                 defaulting to '%(table_name)s', i.e. no substitution.

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


def bc_metrics_query(table_name='%(table_name)s'):
    """
    Further preprocess raw dataframe such that the format fits the bandi-
    coot expectations. That means timestamps as string and no NULL values
    for call durations. Only keep national events and add the antenna
    coordinates.

    Inputs
    ------
    table_name : Name of the table the query is supposed to run on,
                 defaulting to '%(table_name)s', i.e. no substitution.

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
