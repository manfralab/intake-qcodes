import json
from pathlib import Path
from intake.catalog import Catalog
from intake.catalog.local import LocalCatalogEntry
from qcodes.dataset.sqlite.database import connect
from qcodes.dataset.sqlite.query_helpers import select_many_where
from qcodes.dataset.sqlite.connection import ConnectionPlus, transaction, atomic
from qcodes.dataset.guids import validate_guid_format

from intake_qcodes.sources import QCodesDataFrame


def _get_runs(conn):
    """ Get a list of runs.
    Args:
        conn:   database connection
        exp_id: id of the experiment to look inside.
                if None all experiments will be included
    Returns:
        list of rows
    """

    table_columns = [
        "run_id", "guid", "exp_id", "run_description", "run_timestamp", "completed_timestamp", "result_table_name",
    ]

    table_columns_str = ', '.join(table_columns)

    with atomic(conn) as conn:
        sql = f"SELECT {table_columns_str} FROM runs"
        c = transaction(conn, sql)

    return c.fetchall()


def _get_names_from_experiment_id(conn, exp_id):

    return select_many_where(
        conn, "experiments", "name", "sample_name",
        where_column="exp_id", where_value=exp_id
    )


def _parameters_from_description(desc):

    dependent_parameters = []
    independent_parameters = []
    for param_spec in desc['interdependencies']['paramspecs']:
        if param_spec['depends_on']:
            dependent_parameters.append(param_spec['name'])
        else:
            independent_parameters.append(param_spec['name'])
    return dependent_parameters, independent_parameters


known_types = {
    'dataframe': 'intake_qcodes.sources.QCodesDataFrame',
    'xarray': 'intake_qcodes.sources.Qcodes.XArray'
}


class QCodesCatalog(Catalog):

    name = "qcodes_catalog"
    version = '0.0.1'

    def __init__(self, path, dtype='dataframe', **kwargs):
        """
        kwargs go to Catalog.__init__
        """

        if dtype in known_types:
            self._dtype = dtype
            self._source_driver = known_types[dtype]
        else:
            raise ValueError(f'{dtype} is not a known datatype for QCodesCatalog entries')

        self._db_path = Path(path).absolute()
        self._db_path = Path(self._db_path.resolve())
        self._connection = None # database connection
        self._guid_lookup = {} # run_id: guid pairs
        self.guids = []
        self.run_ids = []
        self.experiments = []
        self.samples = []

        super().__init__(**kwargs)

    @property
    def conn(self):
        if not self._connection:
            self._connection = connect(self._db_path)
        return self._connection

    def _load(self):
        """ load entries into catalog """

        self._entries = {}

        exps = set()
        samples = set()
        for row in _get_runs(self.conn):

            run_description = json.loads(row['run_description'])

            # move these functions so they can be loaded elsewhere
            exp_name, sample_name = _get_names_from_experiment_id(self.conn, row['exp_id'])
            dependent_parameters, independent_parameters = _parameters_from_description(run_description)

            self._entries[row['guid']] = LocalCatalogEntry(
                name=row['guid'],
                description='run {} at {}'.format(row['guid'], str(self._db_path)),
                driver=self._source_driver,
                direct_access='forbid',
                args={
                    'db_path': str(self._db_path),
                    'guid': row['guid'],
                },
                cache=None,
                parameters=[],
                metadata={
                    "start_time": row['run_timestamp'],
                    "stop_time": row['completed_timestamp'],
                    "dependent_parameters": dependent_parameters,
                    "independent_parameters": independent_parameters,
                    "experiment_name": exp_name,
                    "sample_name": sample_name,
                    "table_name": row['result_table_name'],
                },
                catalog_dir=str(self._db_path),
                getenv=False,
                getshell=False,
                catalog=self,
            )

            self._guid_lookup[row['run_id']] = row['guid']
            self.guids.append(row['guid'])
            self.run_ids.append(row['run_id'])
            exps.add(exp_name)
            samples.add(sample_name)

        self.experiments = list(exps)
        self.samples = list(samples)

    # def search(self, query):
        ### TODO: add some functionality to select only some subset of the datasets
        # pass

    def __getitem__(self, identifier):
        """ identifier can be a guid (str) or run_id (int)"""

        if isinstance(identifier, int):
            try:
                guid = self._guid_lookup[identifier]
            except KeyError as e:
                raise KeyError(f'{identifier} is not a valid run_id in this database.') from e
        elif isinstance(identifier, str):
            guid = identifier
            validate_guid_format(guid)
        else:
            raise ValueError(f'{identifier} should be a run_id (int) or guid (str)')

        return self._entries[guid]
