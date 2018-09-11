
from multiprocessing import cpu_count  # enables multiprocessing
import pandas as pd  # data mangling and transforming


class Attributes:
    """Attributes class for the gnuper preprocessing."""

    def __init__(self,
                 mp_flag=None, bc_flag=None, no_info=None, clean_up=None,
                 cap_lat=None, cap_long=None,  # adjust
                 raw_data_path='../data/',  # adjust
                 chunking_path='../user_chunks/',
                 bandicoot_path='../user_bandicoot/',
                 antenna_features_path='../antenna_features/',
                 raw_locations='MS_LOCATION.csv',  # adjust
                 antennas_file='antennas.csv',
                 home_antennas_file='home_antennas.csv',
                 noct_begin=19, noct_end=7,  # adjust
                 work_begin=9, work_end=17,  # adjust
                 ep_begin=3, ep_end=5, lp_begin=10, lp_end=12,  # adjust
                 weekend_days=[6, 7],  # adjust
                 call_unit_multiplicator=1,  # multiplicator to get seconds
                 max_chunksize=50000,  # adjust
                 max_weekly_interactions=1000,  # adjust
                 sparkmaster='local'
                 ):
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

        # flags
        self.mp_flag = mp_flag  # multiprocessing
        self.bc_flag = bc_flag  # bandicoot execution
        self.no_info = no_info  # additional info printing
        self.clean_up = clean_up  # clean up files
        # define coordinates of the capital
        self.c_coord = {'latitude': cap_lat, 'longitude': cap_long}
        # paths
        self.raw_data_path = raw_data_path  # insert path to raw data files
        self.chunking_path = chunking_path  # temporary folder for user chunks
        self.antenna_features_path = antenna_features_path  # feature collect
        if bc_flag:
            self.bandicoot_path = bandicoot_path  # folder for bc files
        # file names
        self.raw_locations = raw_data_path+raw_locations
        self.antennas_file = raw_data_path+antennas_file
        self.home_antennas_file = raw_data_path+home_antennas_file
        # multiprocessing
        if mp_flag:
            self.n_processors = cpu_count() - 2  # leave 2 cpu unused
        # define time windows in 24h format
        self.noct_time = {'begin': noct_begin, 'end': noct_end}  # nighttime
        self.work_day = {'begin': work_begin, 'end': work_end}  # working day
        self.early_peak = {'begin': ep_begin, 'end': ep_end}  # early riser
        self.late_peak = {'begin': lp_begin, 'end': lp_end}  # late riser
        # define weekend (by number of weekday)
        self.weekend_days = weekend_days
        # multiplicator to get call units to the level of seconds
        # (e.g. for minutes = 60)
        self.call_unit_multiplicator = call_unit_multiplicator
        # maximum chunksize for users
        self.max_chunksize = max_chunksize
        self.max_weekly_interactions = max_weekly_interactions
        # spark variables
        self.sparkmaster = sparkmaster


class MockupAttributes:
    """Attributes class for creating mock up data."""

    def __init__(self, output_path='../sample_data/', n_antennas=1000,
                 n_cells_p_antenna=3, n_users=100000, n_avg_daily_events=25,
                 cell_cname='CELL_ID', antenna_cname='SITE_ID',
                 long_cname='X', lat_cname='Y',
                 type_cname='CALL_RECORD_TYPE',
                 msisdn_cname='CALLER_MSISDN',
                 date_cname='CALL_DATE',
                 service_cname='BASIC_SERVICE',
                 location_cname='MS_LOCATION',
                 partner_type_cname='CALL_PARTNER_IDENTITY_TYPE',
                 partner_cname='CALL_PARTNER_IDENTITY',
                 tac_cname='TAC_CODE',
                 duration_cname='CALL_DURATION',
                 long_range=[28, 38], lat_range=[11, 16],
                 max_call_duration=7000,
                 date_window=['2018-01-01', '2018-12-31'],
                 date_format=None,
                 loc_file_name='MS_LOCATION.csv',
                 raw_header=True, location_header=True):
        """
        Holds several attributes for creating synthetic CDRs according to one's
        wishes.

        Inputs
        ------

        General:
        --
        output_path : Folder in which the output files should be stored.
        max_call_duration : The maximum duration for a call
            (irrespective of the unit, e.g. seconds or minutes).
        date_format : The format in which the timestamp of an event should be
            saved.
        loc_file_name : File name of the locations file.
        raw_header : Flag for daily files, if the header is saved.
        location_header : Flag for location file, if the header is saved.
        long_range : Array which holds the minimum and maximum longitude
            coordinate for the antenna area.
        lat_range : Array which holds the minimum and maximum latitude
            coordinate for the antenna area.

        Size:
        --
        n_antennas : Number of different antennas.
        n_cells_p_antenna : Number of cells per antenna.
        n_users= : Number of users.
        n_avg_daily_events : Number of average daily events (calls & sms)
            per user.
        date_window : Array which holds begin and end date of the CDR span.

        Locations CSV:
        --
        cell_cname : Column name of the cell id.
        antenna_cname : Column name of the antenna id.
        long_cname : Column name of the longitude coordinate.
        lat_cname : Column name of the latitude coordinate.

        CDR CSV:
        --
        type_cname : Column name of the flag that defines the direction of an
            event.
        msisdn_cname : Column name of the user's MSISDN.
        date_cname : Column name of the date column.
        service_cname : Column name of the flag that defines the type of event.
        location_cname : Column name of the cell id.
        partner_type_cname : Column name of the partner type (national or
            international).
        partner_cname : Column name of the partner id.
        tac_cname : Column name of the tac code.
        duration_cname : Column name of the duration field.

        Output
        ------
        Class with all relevant information for creating Mockup data.
        """
        # outputpath
        self.output_path = output_path

        # number of generated antennas
        self.n_antennas = n_antennas
        # number of cells per antenna
        self.n_cells_p_antenna = n_cells_p_antenna
        # number of desired user_ids
        self.n_users = n_users
        # generating n of total events
        # multiply users, daily events and days, then divide by 2 to
        # control for outgoing and incoming side of events
        n_days = len(pd.date_range(date_window[0], date_window[1]))
        self.n_total_events = round(self.n_users*n_avg_daily_events*n_days/2)

        # locations column names
        self.loc_column_names = {'cell': cell_cname,
                                 'antenna': antenna_cname,
                                 'long': long_cname,
                                 'lat': lat_cname}

        # raw data column names
        self.raw_column_names = {'type': type_cname,
                                 'msisdn': msisdn_cname,
                                 'date': date_cname,
                                 'service': service_cname,
                                 'location': location_cname,
                                 'partner_type': partner_type_cname,
                                 'partner': partner_cname,
                                 'tac': tac_cname,
                                 'duration': duration_cname}

        # coordinates ranges
        self.long_range = long_range
        self.lat_range = lat_range

        # call specific attributes
        self.max_call_duration = max_call_duration  # maxi duration of a call

        # date timestamp specifics
        self.date_window = date_window  # start and end date of raw metadata
        self.date_format = date_format  # output format of date timestamp

        # name of the file which holds the cell location coordinates
        self.loc_file_name = loc_file_name
        # keep headers of output files
        self.raw_header = raw_header
        self.location_header = location_header
