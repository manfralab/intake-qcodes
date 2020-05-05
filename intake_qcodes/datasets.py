from typing import Optional, List, Union, Sequence
from warnings import warn
import numpy as np
import pandas as pd
from pandas import DataFrame
import xarray as xr
from xarray import DataArray, Dataset
from qcodes.dataset.data_set import DataSet
from qcodes.dataset.sqlite.connection import ConnectionPlus, transaction, atomic
from qcodes.dataset.sqlite.query_helpers import select_many_where
from qcodes.dataset.sqlite.queries import get_parameter_tree_values
from qcodes.dataset.descriptions.versioning.serialization import from_dict_to_current


def get_runs(conn):
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


def get_names_from_experiment_id(conn, exp_id):

    return select_many_where(
        conn, "experiments", "name", "sample_name",
        where_column="exp_id", where_value=exp_id
    )

    @property
    def exp_id(self) -> int:
        return select_one_where(self.conn, "runs",
                                "exp_id", "run_id", self.run_id)


def parameters_from_description(desc):

    dependent_parameters = []
    independent_parameters = []
    for param_spec in desc['interdependencies']['paramspecs']:
        if param_spec['depends_on']:
            dependent_parameters.append(param_spec['name'])
        else:
            independent_parameters.append(param_spec['name'])
    return dependent_parameters, independent_parameters


def get_parameter_data(
    conn: ConnectionPlus,
    run_table_name: str,
    run_description: dict,
    columns: Sequence[str] = ()
):
    """
    Get data for one or more parameters and its dependencies. The data
    is returned as numpy arrays within 2 layers of nested dicts. The keys of
    the outermost dict are the requested parameters and the keys of the second
    level are the loaded parameters (requested parameter followed by its
    dependencies).

    Start and End allows one to specify a range of rows to
    be returned (1-based indexing, both ends are included). The range filter
    is applied AFTER the NULL values have been filtered out.
    Be aware that different parameters that are independent of each other
    may return a different number of rows.

    Note that this assumes that all array type parameters have the same length.
    This should always be the case for a parameter and its dependencies.
    Note that all numeric data will at the moment be returned as floating point
    values.

    Args:
        conn: database connection
        table_name: name of the table
        columns: list of columns. If no columns are provided, all parameters
            are returned.
    """


    rd = from_dict_to_current(run_description)
    interdeps = rd.interdeps

    datadict = {}
    if len(columns) == 0:
        columns = [ps.name for ps in interdeps.non_dependencies]

    # loop over all the requested parameters
    for param in columns:
        param_spec = interdeps._id_to_paramspec[param]
        # find all the dependencies of this param
        paramspecs = [param_spec] \
                   + list(interdeps.dependencies.get(param_spec, ()))
        param_names = [param.name for param in paramspecs]
        types = [param.type for param in paramspecs]

        results = get_parameter_tree_values(conn,
                                        run_table_name,
                                        param,
                                        *param_names[1:])

        # if we have array type parameters expand all other parameters
        # to arrays
        if 'array' in types and ('numeric' in types or 'text' in types
                                 or 'complex' in types):
            first_array_element = types.index('array')
            numeric_elms = [i for i, x in enumerate(types)
                            if x == "numeric"]
            complex_elms = [i for i, x in enumerate(types)
                            if x == 'complex']
            text_elms = [i for i, x in enumerate(types)
                         if x == "text"]
            for row in results:
                for element in numeric_elms:
                    row[element] = np.full_like(row[first_array_element],
                                                row[element],
                                                dtype=np.float)

                for element in complex_elms:
                    row[element] = np.full_like(row[first_array_element],
                                                row[element],
                                                dtype=np.complex)
                for element in text_elms:
                    strlen = len(row[element])
                    row[element] = np.full_like(row[first_array_element],
                                                row[element],
                                                dtype=f'U{strlen}')

        results_t = map(list, zip(*results))

        datadict[param] = {
            name: np.array(column_data)
            for name, column_data
            in zip(param_names, results_t)
        }

    return datadict


def datadict_to_dataframe(datadict):

    dataframe_dict = {}
    for name, subdict in datadict.items():
        keys = list(subdict.keys())
        if len(keys) == 0:
            dataframe_dict[name] = pd.DataFrame()
            continue
        if len(keys) == 1:
            index = None
        elif len(keys) == 2:
            index = pd.Index(subdict[keys[1]].ravel(), name=keys[1])
        else:
            indexdata = tuple(np.concatenate(subdict[key])
                              if subdict[key].dtype == np.dtype('O')
                              else subdict[key].ravel()
                              for key in keys[1:])
            index = pd.MultiIndex.from_arrays(
                indexdata,
                names=keys[1:])

        if subdict[keys[0]].dtype == np.dtype('O'):
            # ravel will not fully unpack a numpy array of arrays
            # which are of "object" dtype. This can happen if a variable
            # length array is stored in the db. We use concatenate to
            # flatten these
            try:
                mydata = numpy.concatenate(subdict[keys[0]])
            except ValueError:
                # not all objects are nested arrays
                # i ended up here when a dmm was overloaded for the whole measurement
                mydata = subdict[keys[0]].ravel()
        else:
            mydata = subdict[keys[0]].ravel()
        df = pd.DataFrame(mydata, index=index,
                          columns=[keys[0]])
        dataframe_dict[name] = df

    return pd.concat(list(dataframe_dict.values()), axis=1)


def datadict_to_xarray(datadict: dict) -> Dataset:
    """
    convert dictionary of numpy arrays to xarray
    """

    # TODO: do this without first creating a dataframe?
    # it isn't trivial to handle cases where the data is not on a regular grid
    # the pandas -> xarray flow handles this really nicely

    df = datadict_to_dataframe(datadict)
    return Dataset.from_dataframe(df, sparse=False)


def dataframe_to_xarray(df: DataFrame) -> Dataset:
    """
    Convert pandas DataFrame with MultiIndex to an xarray DataSet.
    """

    len_old_df = len(df)
    df = df[~df.index.duplicated()]
    len_new_df = len(df)

    # if len_new_df < len_old_df:
    #     warn("Duplicate values removed from DataFrame. This dataset is weird.")

    return df.to_xarray()


def xarray_to_dataframe(xr: Dataset) -> DataFrame:
    """
    Convert xarray to DataFrame with appropriate index
    """
    pass
