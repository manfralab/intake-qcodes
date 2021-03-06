from pathlib import Path
import json
from intake.source.base import DataSource, Schema
from qcodes.dataset.sqlite.database import connect
from qcodes.dataset.data_set import DataSet
from qcodes.dataset.sqlite.queries import get_runid_from_guid, get_guid_from_run_id, get_run_description
from qcodes.dataset.sqlite.query_helpers import select_one_where
from qcodes.dataset.descriptions.versioning.serialization import to_dict_for_storage
from intake_qcodes.datasets import get_parameter_data, datadict_to_dataframe, parameters_from_description, datadict_to_xarray
from intake_qcodes.plots import make_default_plots

class QCodesBase(DataSource):
    # add sample name and experiment name properties

    version = '0.0.1'
    partition_access = True

    def __init__(self, db_path, guid=None, run_id=None, metadata=None):

        self._db_path = Path(db_path).absolute()
        self._guid = guid
        self._run_id = run_id
        self._experiment_id = None
        self._sample = ''
        self._experiment = ''
        self._connection = None
        self._qcodes_dataset = None
        self._datadict = {}
        self._run_description = {}
        self._table_name = ''
        self._length = None
        self._snapshot = {}

        super().__init__(metadata=metadata)

        if 'plots' not in self.metadata:
            self.metadata['plots'] = make_default_plots(self.run_description)

    def _read_data(self, columns=()):

        if not columns:
            columns, _ = parameters_from_description(self.run_description)

        in_memory = tuple(self._datadict.keys())
        to_read = list(set(columns).difference(in_memory))

        data = get_parameter_data(
            self._conn,
            self._run_table_name,
            self.run_description,
            columns = to_read,
        )

        for key, val in data.items():
            self._datadict[key] = val

        return {col: self._datadict[col] for col in columns}

    def _get_schema(self):
        """
        return instance of Schema
        should take a roughly constant amount of time regardless of contents of dataset
        """

        self._qcodes_dataset = DataSet(run_id=self.run_id, conn=self._conn)
        dep_params, indep_params = parameters_from_description(self.run_description)

        return Schema(
            datashape=None,
            dtype=None,
            shape=(self._dataset.number_of_results,), # not sure what else to do here
            npartitions= len(dep_params),
            extra_metadata={
                'dataset_metadata': self._dataset.metadata,
            }
        )

    def __len__(self):
        return self._dataset.number_of_results

    @property
    def _conn(self):
        """ database connection """
        if not self._connection:
            self._connection = connect(self._db_path)
        return self._connection

    @property
    def _dataset(self):
        """ qcodes.DataSet """
        if not self._qcodes_dataset:
            self._qcodes_dataset = DataSet(run_id=self.run_id, conn=self._conn)
        return self._qcodes_dataset

    @property
    def _run_table_name(self):
        if not self._table_name:
            self._table_name = self._dataset.table_name
        return self._table_name

    def canonical(self):
        """ return qcodes.DataSet """
        return self._dataset

    def as_dict(self):
        return self._read_data()

    @property
    def guid(self):
        if not self._guid:
            self._guid = get_guid_from_run_id(self._conn, self._run_id)
        return self._guid

    @property
    def run_id(self):
        if not self._run_id:
            self._run_id = get_runid_from_guid(self._conn, self._guid)
        return self._run_id

    @property
    def snapshot(self):
        if not self._snapshot:
            self._snapshot = self._dataset.snapshot
        return self._snapshot

    @property
    def run_description(self):
        if not self._run_description:
            rd = self._dataset.description
            self._run_description = to_dict_for_storage(rd)
        return self._run_description

    @property
    def sample(self):
        if not self._sample:
            self._sample = self._dataset.sample_name
        return self._sample

    @property
    def experiment(self):
        if not self._experiment:
            self._experiment = self._dataset.exp_name
        return self._experiment

class QCodesDataFrame(QCodesBase):
    # should still be useful if not called from catalog

    name = 'qcodes_dataframe'
    container = 'dataframe'

    def __init__(self, db_path, guid=None, run_id=None, metadata=None):

        self._init_args = {
            'db_path': db_path,
            'guid': guid,
            'run_id': run_id,
            'metadata': metadata,
        }

        self._dataframe = None
        super().__init__(**self._init_args)

    def _get_partition(self, param):
        """Subclasses should return a container object for this partition
        This function will never be called with an out-of-range value.
        """
        datadict = self._read_data(columns=[param])
        return datadict_to_dataframe(datadict)

    def read(self):
        """Load entire dataset into a container and return it"""
        datadict = self._read_data()
        return datadict_to_dataframe(datadict)

    def read_chunked(self):
        """Return iterator over container fragments of data source"""
        dep_params, _ = parameters_from_description(self.run_description)
        for i in range(len(dep_params)):
            yield self._get_partition(i)

    def read_partition(self, idx):
        """Return a part of the data corresponding to i-th partition.
        By default, assumes i should be an integer between zero and npartitions;
        override for more complex indexing schemes.
        """
        dep_params, _ = parameters_from_description(self.run_description)

        if isinstance(idx, str):
            param = idx
        elif isinstance(idx, int):
            param = dep_params[idx]
        else:
            raise ValueError('Partition index should be an integer or parameter name')

        return self._get_partition(param)

    def to_dask(self):
        """Return a dask container for this data source"""
        raise NotImplementedError

class QCodesXArray(QCodesBase):
    # should still be useful if not called from catalog

    name = 'qcodes_xarray'
    container = 'xarray'

    def __init__(self, db_path, guid=None, run_id=None, metadata=None):

        self._init_args = {
            'db_path': db_path,
            'guid': guid,
            'run_id': run_id,
            'metadata': metadata,
        }

        self._dataframe = None
        super().__init__(**self._init_args)

    def _get_partition(self, param):
        """Subclasses should return a container object for this partition
        This function will never be called with an out-of-range value.
        """
        datadict = self._read_data(columns=[param])
        return datadict_to_xarray(datadict)

    def read(self):
        """Load entire dataset into a container and return it"""
        datadict = self._read_data()
        return datadict_to_xarray(datadict)

    def read_chunked(self):
        """Return iterator over container fragments of data source"""
        dep_params, _ = parameters_from_description(self.run_description)
        for i in range(len(dep_params)):
            yield self._get_partition(i)

    def read_partition(self, idx):
        """Return a part of the data corresponding to i-th partition.
        By default, assumes i should be an integer between zero and npartitions;
        override for more complex indexing schemes.
        """
        dep_params, _ = parameters_from_description(self.run_description)

        if isinstance(idx, str):
            param = idx
        elif isinstance(idx, int):
            param = dep_params[idx]
        else:
            raise ValueError('Partition index should be an integer or parameter name')

        return self._get_partition(param)

    def to_dask(self):
        """Return a dask container for this data source"""
        raise NotImplementedError
