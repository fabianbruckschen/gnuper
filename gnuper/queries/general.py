"""Queries as functions with SQL Syntax taking in an origin table name -
General Queries."""


def raw_locations_query(table_name='%(table_name)s',
                        cell_cname='cell_id',
                        antenna_cname='antenna_id',
                        long_cname='longitude',
                        lat_cname='latitude'):
    """
    Extract raw locations of cells and antennas while getting rid of
    potential duplicates.

    Inputs
    ------
    table_name : Name of the table the query is supposed to run on,
                 defaulting to '%(table_name)s', i.e. no substitution.
    cell_cname : Name of the column which holds the ids of single cells/bts.
    antenna_cname : Name of the column which holds the ids of the
                    antennas/towers (i.e. unique GPS coordinates per id).
    long_cname : Name of column which holds the longitude coordinate.
    lat_cname : Name of column which holds the latitude coordinate.

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
              %(cell_cname)s as cell_id,
              %(antenna_cname)s as antenna_id,
              ROUND(%(long_cname)s, 6) as longitude, -- 6 digits are sufficient
              ROUND(%(lat_cname)s, 6) as latitude -- to identify humans
            FROM %(table_name)s
        )
        GROUP BY cell_id, antenna_id -- kick duplicates
    """
    return query % {'table_name': table_name,
                    'cell_cname': cell_cname, 'antenna_cname': antenna_cname,
                    'long_cname': long_cname, 'lat_cname': lat_cname}


def get_user_ids_query(table_name='%(table_name)s',
                       user_cname='caller_id'):
    """
    Extract unique available user ids per raw data file.

    Inputs
    ------
    table_name : Name of the table the query is supposed to run on,
                 defaulting to '%(table_name)s', i.e. no substitution.
    user_cname : Name of column which holds the ids unique per user.

    Output
    ------
    Unique caller_ids.
    """
    query = """
        SELECT
          %(user_cname)s as caller_id,
          COUNT(1) as n_interactions
        FROM %(table_name)s
        GROUP BY %(user_cname)s -- only keep uniques
    """
    return query % {'table_name': table_name, 'user_cname': user_cname}


def chunking_query_by_n_interactions(table_name='%(table_name)s',
                                     max_chunksize=50000):
    """
    Create a chunk_id per unique user which is based on the maximum number of
    interactions (=rows) per chunk defined in attributes.

    Inputs
    ------
    table_name : Name of the table the query is supposed to run on,
                 defaulting to '%(table_name)s', i.e. no substitution.
    max_chunksize : Maximum number of total interactions per chunk.

    Output
    ------
    Crosswalk between caller_id and chunk_id.
    """
    query = """
        SELECT
          caller_id,
          -- as soon as chunk limit is reached the id increases by 1
          SMALLINT((cumsum-1) / %(max_chunksize)s)+1 as chunk_id
        FROM
        (
          SELECT
            caller_id,
            SUM(n_interactions) OVER (ORDER BY caller_id) as cumsum
          FROM %(table_name)s
        )
    """
    return query % {'table_name': table_name,
                    'max_chunksize': max_chunksize}


def chunking_query_by_n_users(table_name='%(table_name)s',
                              max_chunksize=50000):
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
