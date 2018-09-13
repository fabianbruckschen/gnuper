"""Queries as functions with SQL Syntax taking in an origin table name -
General Queries."""


def raw_locations_query(table_name='%(table_name)s'):
    """
    Extract raw locations of cells and antennas while getting rid of
    potential duplicates.

    Inputs
    ------
    table_name : Name of the table the query is supposed to run on,
                 defaulting to '%(table_name)s', i.e. no substitution.

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


def get_user_ids_query(table_name='%(table_name)s'):
    """
    Extract unique available user ids per raw data file.

    Inputs
    ------
    table_name : Name of the table the query is supposed to run on,
                 defaulting to '%(table_name)s', i.e. no substitution.

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


def chunking_query(max_chunksize, table_name='%(table_name)s'):
    """
    Create a chunk_id which is based on the unique users (caller_id) and
    the maximum size of users per chunk defined in attributes.

    Inputs
    ------
    table_name : Name of the table the query is supposed to run on,
                 defaulting to '%(table_name)s', i.e. no substitution.
    max_chunksize : Maximum number of users per chunk.

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
