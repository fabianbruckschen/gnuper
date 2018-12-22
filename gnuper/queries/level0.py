"""Queries as functions with SQL Syntax taking in an origin table name -
Level 0."""


def raw_preprocessing_query(table_name='%(table_name)s', cump=1,
                            timestamp_format='dd MMM yyyy HH:mm:ss',
                            user_cname='caller_id',
                            service_cname='interaction',
                            record_type_cname='direction',
                            partner_cname='correspondent_id',
                            partner_type_cname='national',
                            date_cname='datetime',
                            duration_cname='call_duration',
                            cell_cname='cell_id'
                            ):
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
    timestamp_format : Format of original date or timestamp, which is needed
                        to interpret into right timestamp format.
    user_cname : Name of the column which holds the ids of users.
    service_cname : Name of the column which holds the type of event (2 = text
                    & 1 = call).
    record_type_cname : Name of the column which holds the direction of an
                        event (1 = incoming & 2 = outgoing).
    partner_cname : Name of the column which holds the ids of users on the
                    other end.
    partner_type_cname : Name of the column which holds the indicators for an
                         event being national (=1) or international (=2).
    date_cname : Name of the column which holds the timestamps for the events.
    duration_cname : Name of the column which holds the duration of calls.
    cell_cname : Name of the column which holds the ids of single cells/bts.

    Output
    ------
    Readable dataframe of raw events, keeping bare minimum of
    information needed for further processing.
    """
    query = """
        SELECT
          cd.%(user_cname)s as caller_id,
          CASE %(service_cname)s WHEN 2 THEN 'text' ELSE 'call' END
           as interaction,
          CASE %(record_type_cname)s WHEN 1 THEN 'in' ELSE 'out' END
           as direction,
          %(partner_type_cname)s as national,
          %(partner_cname)s as correspondent_id,
          CAST(UNIX_TIMESTAMP(%(date_cname)s, '%(timestamp_format)s')
           AS TIMESTAMP) as datetime, -- transform to proper timestamp
          INT(ROUND(%(duration_cname)s*%(cump)s))
           as call_duration, -- calculate duration in seconds
          ms.antenna_id as antenna_id,
          ci.chunk_id as chunk_id
        FROM %(table_name)s cd
        JOIN table_raw_locations ms
        ON cd.%(cell_cname)s = ms.cell_id
        JOIN table_chunk_ids ci
        ON cd.%(user_cname)s = ci.caller_id
        WHERE %(record_type_cname)s IN (1,2)
          AND %(service_cname)s IN (1,2)
        """
    return query % {'table_name': table_name,
                    'cump': cump,
                    'timestamp_format': timestamp_format,
                    'user_cname': user_cname,
                    'service_cname': service_cname,
                    'record_type_cname': record_type_cname,
                    'partner_cname': partner_cname,
                    'partner_type_cname': partner_type_cname,
                    'date_cname': date_cname,
                    'duration_cname': duration_cname,
                    'cell_cname': cell_cname}


def filter_machines_query(max_weekly_interactions,
                          table_name='%(table_name)s'):
    """
    Filter out user ids which have more weekly interactions on average than the
    defined threshold in the attributes class. This query's purpose is to
    ignore machines and multi-user phones from the following analysis.

    Inputs
    ------
    table_name : Name of the table the query is supposed to run on,
                 defaulting to '%(table_name)s', i.e. no substitution.
    max_weekly_interactions : Maximum number of average weekly interactions to
                              still count as a single user phone (not machine
                              or multi-user).

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
                  AVG(n_interactions) as avg_w_n_interactions -- calculate avg
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
                GROUP BY caller_id
            )
            WHERE avg_w_n_interactions > %(max_weekly_interactions)s
        ) filter -- identified machines or multiuser phones
        ON raw.caller_id = filter.caller_id
        WHERE is_machine IS NULL -- only keep non-machines (aka no match)
    """
    return query % {'table_name': table_name,
                    'max_weekly_interactions': max_weekly_interactions}
