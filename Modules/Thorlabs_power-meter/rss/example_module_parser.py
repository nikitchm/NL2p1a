"""
Tools for processing the entries made into the MC log file by the syringe injector module.
"""

from logger_parsing import MC_log_df
import re
import pandas as pd
import numpy as np
import os
from typing import List, Dict
import warnings

class PiezoNPointC400Log(MC_log_df):
    class DfColumnNames(MC_log_df.DfColumnNames):
        depth = 'depth'
    class MCModuleNames(MC_log_df.MCModuleNames):
        this_module = 'piezo'
    class LogTags(MC_log_df.LogTags):
        first_read_position = r'first read position : (-?\d+\.\d+)'                 # first read position : 35.983
        moving_pattern = r'moving to (-?\d+\.\d+) um : (\d{2}:\d{2}:\d{2}.\d+)'     # moving to -105.000 um : 15:10:36.302
        moving_time_format = '%H:%M:%S.%f'
        peristaltic_switch_re = r'state changed to (\w+) at\s+(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}.\d+)'
        peristaltic_switch_timestamp_re = '%Y/%m/%d %H:%M:%S.%f'
        paristaltic_switch_on = 'TRUE'
        paristaltic_switch_off = 'FALSE'

    def __init__(self):
        super().__init__()
        self.colnames = self.DfColumnNames()
        self.modulename = self.MCModuleNames()
        self.logtags = self.LogTags()

        pd.options.mode.chained_assignment = None  # default='warn'
    
    def first_read_position(self):
        """
        When the piezo controller starts, it logs it's current position. This method parses such a message and
        records the reported value in the depth column. Also, since the message doesn't contain a timestamp, the 
        timestamp of a log is copied into the module timestamp column, so that the further processing can proceed normally.
        Since such a read happens during the initialization step, a small descrepancy between the time when the measurement
        was done and the logged shouldn't make much of differece.

        Note: These messages become obsolete in later versions of the software, replaced with the reports of the current position
        and timestamp.

        Output: updated 'depth' and 'module_tstamp' columns in self.mdf
        """
        warnings.filterwarnings("ignore", 'This pattern is interpreted as a regular expression, and has match groups')
        # Identify rows that match the pattern
        mask = self.mdf[self.colnames.message].str.contains(self.logtags.first_read_position)#, regex=False)
        # Extract number and timestamp for matched rows and assign to the DataFrame
        extracted_df = self.mdf.loc[mask, self.colnames.message].str.extract(self.logtags.first_read_position)
        self.mdf.loc[mask, self.colnames.depth] = extracted_df[0]
        self.mdf.loc[mask, self.colnames.module_tstamp] = self.mdf.loc[mask, self.colnames.log_tstamp]

    def parse_move_messages(self):
        """
        These messages are generated when a command to move the piezo to a new position is received. They contain information on
        the position (depth) where the piezo is moving and the timestamp for when the move was initiated.

        Output: updated 'depth' and 'module_tstamp' columns in self.mdf 
        """
        warnings.filterwarnings("ignore", 'This pattern is interpreted as a regular expression, and has match groups')
        # Identify rows that match the pattern
        mask = self.mdf[self.colnames.message].str.contains(self.logtags.moving_pattern)#, regex=False)
        # Extract number and timestamp for matched rows and assign to the DataFrame
        extracted_df = self.mdf.loc[mask, self.colnames.message].str.extract(self.logtags.moving_pattern)
        self.mdf.loc[mask, self.colnames.depth] = extracted_df[0]
        self.mdf.loc[mask, self.colnames.module_tstamp] = pd.to_datetime(extracted_df[1], format=self.logtags.moving_time_format)
        # PiezoNPointC400Log.replace_date_in_colt_with_cols(df=self.mdf, colt='module_tstamp', cols='log_tstamp')
        self.mdf = self.replace_date_in_colt_with_cols2(df=self.mdf, col_sec='module_tstamp', col_date='log_tstamp',\
                                                        tstamp_div='minute', check_for_discrepancy=True)
