import json
from pathlib import Path
from intake.catalog import Catalog
from intake.catalog.local import LocalCatalogEntry
from qcodes.dataset.sqlite.database import connect
from qcodes.dataset.sqlite.connection import ConnectionPlus
from qcodes.dataset.guids import validate_guid_format
from intake_qcodes.sources import QCodesDataFrame
from intake_qcodes.datasets import get_runs, get_names_from_experiment_id, parameters_from_description


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
        self._guid_lookup = {} # {run_id: guid} pairs
        self._run_id_lookup = {} # {guid: run_id} pairs
        self._experiments = []
        self._samples = []

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
        for row in get_runs(self.conn):

            run_description = json.loads(row['run_description'])

            # move these functions so they can be loaded elsewhere
            exp_name, sample_name = get_names_from_experiment_id(self.conn, row['exp_id'])
            dependent_parameters, independent_parameters = parameters_from_description(run_description)

            self._entries[row['guid']] = LocalCatalogEntry(
                name='run {}'.format(row['run_id']),
                description='run {} at {} with guid {}'.format(row['run_id'], str(self._db_path), row['guid']),
                driver=self._source_driver,
                direct_access='forbid',
                args={
                    'db_path': str(self._db_path),
                    'guid': row['guid'],
                    'run_id': row['run_id']
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
            exps.add(exp_name)
            samples.add(sample_name)

        self._experiments = list(exps)
        self._samples = list(samples)
        self._run_id_lookup = {val: key for key, val in self._guid_lookup.items()}

    def search(self, query: dict):
        ## TODO: add some functionality to select only some subset of the datasets
        query_keys = [
            'experiment', 'sample', 'has_parameter',
        ]
        if not all(key in query_keys for key in query):
            raise KeyError(f'Not sure what to do with {query}. This is not a good search function.')
        if len(query)>1:
            raise ValueError(f'This thing only handles one search key at a time')

        if 'experiment' in query:
            def _func(entry):
                if entry.metadata['experiment_name']==query['experiment']:
                    return True
                return False
            return self.filter(_func)

        if 'sample' in query:
            def _func(entry):
                if entry.metadata['sample_name']==query['sample']:
                    return True
                return False
            return self.filter(_func)

        if 'has_parameter' in query:
            def _func(entry):
                if (query['has_parameter'] in entry.metadata['dependent_parameters']) or (query['has_parameter'] in entry.metadata['independent_parameters']):
                    return True
                return False
            return self.filter(_func)


    def __getitem__(self, identifier):
        """ identifier can be a guid (str) or run_id (int)"""

        if isinstance(identifier, int):
            guid = self.guid_from_run_id(identifier)
        elif isinstance(identifier, slice):
            # make a best effort to slice this catalog by run_id
            id_list = list(range(min(self.run_ids), max(self.run_ids)))
            sliced_id_list = id_list[identifier]
            def _func(entry):
                if entry.run_id in sliced_id_list:
                    return True
                return False
            return self.filter(_func)

        elif isinstance(identifier, str):
            guid = identifier
        else:
            raise ValueError(f'{identifier} should be a run_id (int) or guid (str)')

        return self._entries[guid]

    def serialize(self):
        """
        Hacked version of this. May want to use .yaml() instead to roll my own.

        See here for updates: https://github.com/intake/intake/issues/492
        """
        import yaml
        output = {"metadata": self.metadata, "sources": {},
                  "name": self.name}
        for key, entry in self.items():
            # hack to fix serializing the name of the custom catalog
            kw = entry._captured_init_kwargs.copy()
            kw.pop('catalog')

            if isinstance(kw['parameters'], list):
                if not kw['parameters']:
                    kw['parameters'] = {}
                else:
                    raise ValueError('not sure how to serialize this correctly')
            if not kw['cache']:
                kw['cache'] = [] # default value of None raises an error when read back

            output["sources"][key] = kw

        return yaml.dump(output)

    def guid_from_run_id(self, run_id):
        if self._guid_lookup:
            return self._guid_lookup[run_id]

        raise ValueError('Catalog not initilized. No run information loaded')

    def run_id_from_guid(self, guid):
        if self._run_id_lookup:
            return self._run_id_lookup[guid]

        raise ValueError('Catalog not initilized. No run information loaded')

    @property
    def run_ids(self):
        return list(self._guid_lookup.keys())

    @property
    def guids(self):
        return list(self._guid_lookup.values())

    @property
    def samples(self):
        if self._samples:
            return self._samples

        raise ValueError('Catalog not initilized. No sample information loaded')

    @property
    def experiments(self):
        if self._experiments:
            return self._experiments

        raise ValueError('Catalog not initilized. No experiment information loaded')
