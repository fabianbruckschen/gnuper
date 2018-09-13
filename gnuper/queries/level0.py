"""Queries as functions with SQL Syntax taking in an origin table name -
Level 0."""


def raw_preprocessing_query(cump, table_name='%(table_name)s'):
    """
    Preprocess initial raw data by making columns more readable and
    transform data into unified format (timestamp, seconds as duration,
    etc.).
    Only keep relevant information as well, e.g. no cell_id but
    solely antenna_id information and no TAC code.
    Finally add the chunk_id per event based on caller_id.

    Inputs
    ------
    table_name : Name of the table the query is supposed to run on,
                 defaulting to '%(table_name)s', i.e. no substitution.
    cump : Stands for *C*all-*U*nit-*M*ulti*P*lier which is 60 if the input
           is minutes and 1 for seconds.

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


def filter_machines_query(max_weekly_interactions,
                          table_name='%(table_name)s'):
    """
    Filter out user ids which have more weekly interactions than the
    defined threshold in the attributes class. This query's purpose is
    to ignore machines and multi-user phones from the following analysis.

    Inputs
    ------
    table_name : Name of the table the query is supposed to run on,
                 defaulting to '%(table_name)s', i.e. no substitution.
    max_weekly_interactions : Maximum number of weekly interactions to
        still count as a single user phone (not machine or multi-user).

    Output
    ------
    Filtered preprocessed dataframe which holds only user_ids which are
    relevant for the feature creation.
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
