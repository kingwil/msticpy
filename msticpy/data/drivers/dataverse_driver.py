# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------
"""MDATP OData Driver class."""
from typing import Any, Union

import pandas as pd

from ..._version import VERSION
from ...auth.azure_auth_core import AzureCloudConfig
from ...common.utility import export
from .odata_driver import OData, QuerySource, _get_driver_settings

__version__ = VERSION
__author__ = "Will King"


@export
class DataverseDriver(OData):
    """OData driver class to retreive data from MS Dataverse APIs."""

    CONFIG_NAME = "Dataverse"
    _ALT_CONFIG_NAMES = ["DataverseApp"]

    def __init__(self, connection_str: str = None, **kwargs):
        """
        Instantiate MSGraph driver and optionally connect.

        Parameters
        ----------
        connection_str : str, optional
            Connection string

        """
        super().__init__(**kwargs)
        cs_dict = _get_driver_settings(
            self.CONFIG_NAME, self._ALT_CONFIG_NAMES
        )
        self.dataverse_instance_url = cs_dict["InstanceUrl"]
        azure_cloud = AzureCloudConfig()

        self.req_body = {
            "client_id": None,
            "client_secret": None,
            "grant_type": "client_credentials",
            "scope": f"{self.dataverse_instance_url}.default",
        }
        self.oauth_url = (
            f"{azure_cloud.endpoints.active_directory}/{{tenantId}}/oauth2/v2.0/token"
        )
        self.api_root = f"{self.dataverse_instance_url}api/data/"
        self.api_ver = "v9.2"
        if connection_str:
            self.current_connection = connection_str
            self.connect(connection_str)

    def query(
        self, query: str, query_source: QuerySource = None, **kwargs
    ) -> Union[pd.DataFrame, Any]:
        """
        Execute query string and return DataFrame of results.

        Parameters
        ----------
        query : str
            The query to execute
        query_source : QuerySource
            The query definition object

        Returns
        -------
        Union[pd.DataFrame, results.ResultSet]
            A DataFrame (if successfull) or
            the underlying provider result if an error.

        """
        del query_source, kwargs
        #response = self.query_with_results(query, body=False)[1])["value"]
        results = self.query_with_results(query, body=False)
        if isinstance(results[1], int):
            return results[1]
        return pd.json_normalize(results[1]["value"])
