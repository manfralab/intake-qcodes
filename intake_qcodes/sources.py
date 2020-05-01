from pathlib import Path
import json
from intake.source.base import DataSource, Schema
from qcodes.dataset.sqlite.database import connect
from qcodes.dataset.data_set import DataSet
from qcodes.dataset.sqlite.queries import get_runid_from_guid, get_guid_from_run_id, get_run_description
from qcodes.dataset.sqlite.query_helpers import select_one_where
from qcodes.dataset.descriptions.versioning.serialization import to_dict_for_storage
from intake_qcodes.datasets import get_parameter_data, datadict_to_dataframe

class QCodesBase(DataSource):

    version = '0.0.1'
    partition_access = True

    def __init__(self, db_path, guid=None, run_id=None, metadata=None):

        self._db_path = Path(db_path).absolute()
        self._guid = guid
        self._run_id = run_id
        self._connection = None
        self._qcodes_dataset = None
        self._datadict = {}
        self._run_description = {}
        self._table_name = ''
        self._length = None

        super().__init__(metadata=metadata)

    def _read_data(self, columns=()):

        if not columns:
            columns = self.metadata['dependent_parameters']

        in_memory = tuple(self._datadict.keys())
        to_read = list(set(columns).difference(in_memory))

        data = get_parameter_data(
            self.conn,
            self.run_table_name,
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

        # use the dataset to access remaining schema information
        self._qcodes_dataset = DataSet(run_id=self.run_id, conn=self.conn)
        self._length = self._qcodes_dataset.number_of_results

        return Schema(
            datashape=None,
            dtype=None,
            shape=(dataset_length,), # not sure what else to do here
            npartitions=n_dependent_params,
            extra_metadata={
                'run_description': self.run_description,
                'run_table_name': self.run_table_name,
                'dataset_metadata': self._qcodes_dataset.metadata,
                'number_of_records': self._length,
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
    def conn(self):
        if not self._connection:
            self._connection = connect(self._db_path)
        return self._connection

    @property
    def guid(self):
        if not self._guid:
            self._guid = get_guid_from_run_id(self.conn, self._run_id)
        return self._guid

    @property
    def run_id(self):
        if not self._run_id:
            self._run_id = get_runid_from_guid(self.conn, self._guid)
        return self._run_id

    @property
    def dataset(self):
        self._load_metadata() # loads schema/dataset and metadata
        return self._qcodes_dataset

    @property
    def snapshot(self):
        self._load_metadata() # loads schema/dataset and metadata
        return self.metadata['snapshot']

    @property
    def run_description(self):
        if not self._run_description:
            if self._qcodes_dataset:
                rd = self._qcodes_dataset.description
                self._run_description = to_dict_for_storage(rd)
            else:
                self._run_description = json.loads(get_run_description(self.conn, self.run_id))
        return self._run_description

    @property
    def run_table_name(self):
        if not self._table_name:
            if self._qcodes_dataset:
                self._table_name = self._qcodes_dataset.table_name
            else:
                select_one_where(self.conn, "runs",
                                "result_table_name", "run_id", self.run_id)
                pass


class QCodesDataFrame(QCodesBase):
    # should still be useful if not called from catalog
    # don't rely on metadata keys
    # make guid and run_id kwargs where one is required
    # things grabbed from meta that are used fo schema/loading should be args/kwargs to __init__

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
