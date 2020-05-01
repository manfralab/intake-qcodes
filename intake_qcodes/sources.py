from pathlib import Path
from intake.source.base import DataSource, Schema
from qcodes.dataset.sqlite.database import connect
from qcodes.dataset.data_set import DataSet
from qcodes.dataset.sqlite.queries import get_runid_from_guid
from intake_qcodes.datasets import get_parameter_data, datadict_to_dataframe

class QCodesBase(DataSource):

    version = '0.0.1'
    partition_access = True

    def __init__(self, db_path, guid, metadata=None):

        self._db_path = Path(db_path).absolute()
        self._guid = guid
        self._run_id = None
        self._connection = None
        self._qcodes_dataset = None
        self._datadict = {}

        super().__init__(metadata=metadata)

    @property
    def conn(self):
        if not self._connection:
            self._connection = connect(self._db_path)
        return self._connection

    def _read_data(self, columns=()):

        if not columns:
            columns = self.metadata['dependent_parameters']

        in_memory = tuple(self._datadict.keys())
        to_read = list(set(columns).difference(in_memory))

        data = get_parameter_data(
            self.conn,
            self.metadata['table_name'],
            self.description,
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
        if not self._run_id:
            self._run_id = get_runid_from_guid(self.conn, self._guid)

        # already have a few useful things
        n_dependent_params = len(self.metadata['dependent_parameters'])
        n_independent_params = len(self.metadata['independent_parameters'])

        # use the dataset to access remaining schema information
        self._qcodes_dataset = DataSet(run_id=self._run_id, conn=self.conn)
        dataset_length = self._qcodes_dataset.number_of_results

        return Schema(
            datashape=None,
            dtype=None,
            shape=(dataset_length,), # not sure what else to do here
            npartitions=n_dependent_params,
            extra_metadata={
                'dataset_metadata': self._qcodes_dataset.metadata,
                'number_of_records': dataset_length,
                'sweep_dimensions': n_independent_params,
                'snapshot': self._qcodes_dataset.snapshot
            }
        )


    def to_dask(self):
        """Return a dask container for this data source"""
        raise NotImplementedError

    def canonical(self):
        """ return qcodes.DataSet """
        self._load_metadata() # loads schema/dataset and metadata
        return self._qcodes_dataset

    @property
    def dataset(self):
        self._load_metadata() # loads schema/dataset and metadata
        return self._qcodes_dataset

    @property
    def snapshot(self):
        self._load_metadata() # loads schema/dataset and metadata
        return self.metadata['snapshot']


class QCodesDataFrame(QCodesBase):

    name = 'qcodes_dataframe'
    container = 'dataframe'

    def __init__(self, db_path, guid, metadata=None):

        self._init_args = {
            'db_path': db_path,
            'guid': guid,
            'metadata': metadata,
        }

        self._dataframe = None
        super().__init__(**self._init_args)

    def _get_partition(self, idx):
        """Subclasses should return a container object for this partition
        This function will never be called with an out-of-range value for i.
        """
        if isinstance(idx, int):
            param = self.metadata['dependent_parameters'][0]
        elif isinstance(idx, str):
            param = idx
        else:
            raise ValueError('Partition index should be an integer or parameter name')

        return self._read_data(columns=[param])

    def read(self):
        """Load entire dataset into a container and return it"""
        datadict = self._read_data()
        return datadict_to_dataframe(datadict)

    def read_chunked(self):
        """Return iterator over container fragments of data source"""
        npartitions = len(self.metadata['dependent_parameters'])
        for i in range(npartitions):
            yield self._get_partition(i)

    def read_partition(self, i):
        """Return a part of the data corresponding to i-th partition.
        By default, assumes i should be an integer between zero and npartitions;
        override for more complex indexing schemes.
        """
        npartitions = len(self.metadata['dependent_parameters'])
        if i < 0 or i >= npartitions:
            raise IndexError('%d is out of range' % i)

        return self._get_partition(i)
