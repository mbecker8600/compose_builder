import pandas as pd
import numpy as np
from datetime import timedelta
import uuid
from abc import ABC, abstractmethod


class ComposeBuilder(ABC):

    def __init__(self):
        self.current_table = None

    @abstractmethod
    def initialize_compose_layer(self):
        pass

    @abstractmethod
    def ingest_cdc(self, cdc):
        pass

    @abstractmethod
    def persist_updates(self, output_dir, file):
        pass

    def generate_surrogate_keys(self, n_keys=1):
        sks = []
        for _ in range(n_keys):
            sks.append(uuid.uuid1())
        return sks

    def swap_to_natural_key(self, df):
        df.reset_index(inplace=True)  # reset index
        df.set_index(self.natural_key, inplace=True)  # make sk new index

    def swap_to_surrogate_key(self, df):
        df.reset_index(inplace=True)  # reset index
        df.set_index(self.surrogate_key, inplace=True)  # make sk new index

    def split_cdc_records(self, cdc):
        existing_records = cdc[cdc.index.isin(self.current_table.index)]
        new_records = cdc[~cdc.index.isin(self.current_table.index)]
        # TODO: do we need deleted records?

        return existing_records, new_records


class BasicComposeBuilder(ComposeBuilder):

    def __init__(self, init_load, primary_key):
        self.natural_key = primary_key
        self.surrogate_key = 'address_sk'
        self.init_load = init_load
        self.initialize_compose_layer()

    def initialize_compose_layer(self):
        self.current_table = self.init_load.copy()

    def ingest_cdc(self, cdc):
        existing_records, new_records = self.split_cdc_records(cdc)
        self.__ingest_existing_records__(existing_records)
        self.__ingest_new_records__(new_records)

    def persist_updates(self, output_dir, file):
        self.current_table.to_csv(output_dir + 'current/' + file)

    def __ingest_existing_records__(self, cdc):
        self.current_table.ix[cdc.index] = cdc

    def __ingest_new_records__(self, cdc):
        self.current_table = self.current_table.append(cdc)


class ComposeBuilderDecorator(ComposeBuilder):

    def __init__(self, builder):
        self.builder = builder
        self.current_table = builder.current_table
        self.natural_key = builder.natural_key
        self.surrogate_key = builder.surrogate_key
        self.init_load = builder.init_load

    def initialize_compose_layer(self):
        self.builder.initialize_compose_layer()

    def ingest_cdc(self, cdc):
        self.builder.ingest_cdc(cdc)

    def persist_updates(self, output_dir, file):
        self.builder.persist_updates(output_dir, file)


class TrackHistory(ComposeBuilderDecorator):

    def __init__(self, builder):
        super().__init__(builder)
        self.historical_table = None
        self.initialize_compose_layer()

    def initialize_compose_layer(self):
        super().initialize_compose_layer()

        self.historical_table = init_load.copy()
        self.historical_table['address_sk'] = self.generate_surrogate_keys(
            n_keys=len(init_load))  # create surrogate key
        self.swap_to_surrogate_key(self.historical_table)
        self.historical_table['effective_start'] = self.historical_table[
            'last_updated']  # initialize the begin and end dates
        self.historical_table['effective_end'] = np.nan

    def ingest_cdc(self, cdc):
        super().ingest_cdc(cdc)

        existing_records, new_records = self.split_cdc_records(cdc)
        self.__ingest_existing_records__(existing_records)
        self.__ingest_new_records__(new_records)

    def persist_updates(self, output_dir, file):
        super().persist_updates(output_dir, file)
        self.historical_table.to_csv(output_dir + 'historical/' + file)

    def __ingest_existing_records__(self, cdc):
        # get only the current records of the historical view
        historical_current_view = self.historical_table[self.historical_table['effective_end'].isnull()]

        # now it is safe to swap to the natural key since we only have current data
        self.swap_to_natural_key(historical_current_view)

        # set effective end date to previous time
        historical_current_view.loc[cdc.index, 'effective_end'] = cdc['last_updated'] - timedelta(days=1)
        historical_current_view['effective_end'] = pd.to_datetime(historical_current_view['effective_end'])
        self.swap_to_surrogate_key(historical_current_view)
        self.historical_table.ix[historical_current_view.index] = historical_current_view
        self.__add_current_historical_records__(cdc)

    def __ingest_new_records__(self, cdc):
        self.__add_current_historical_records__(cdc)

    def __add_current_historical_records__(self, cdc):
        cdc['address_sk'] = self.generate_surrogate_keys(n_keys=len(cdc))
        self.swap_to_surrogate_key(cdc)
        cdc['effective_start'] = cdc['last_updated']
        cdc['effective_end'] = np.nan
        self.historical_table = self.historical_table.append(cdc)


class TrackSchema(ComposeBuilderDecorator):
    pass


if __name__ == '__main__':

    # test slow changing dimension with history
    # initialize current/historical tables from init file
    init_load = pd.read_csv('raw-landed/scd/init-load/address.csv', index_col=0, parse_dates=['last_updated'])
    compose_builder = TrackHistory(BasicComposeBuilder(init_load=init_load, primary_key='address_key'))

    # cdc changes
    cdc = pd.read_csv('raw-landed/scd/cdc/basic-cdc.csv', index_col=0, parse_dates=['last_updated'])
    compose_builder.ingest_cdc(cdc)
    compose_builder.persist_updates(output_dir='raw-composed/scd/', file='address.csv')

    ## test fact with no history
    # initialize current/historical tables from init file
    init_load = pd.read_csv('raw-landed/fact/init-load/transactions.csv', index_col=0, parse_dates=['transaction_date'])
    compose_builder = BasicComposeBuilder(init_load=init_load, primary_key='transaction_key')

    # cdc changes
    cdc = pd.read_csv('raw-landed/fact/cdc/basic-cdc.csv', index_col=0, parse_dates=['transaction_date'])
    compose_builder.ingest_cdc(cdc)
    compose_builder.persist_updates(output_dir='raw-composed/fact/', file='transactions.csv')

