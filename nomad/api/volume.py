"""Nomad volumes: https://developer.hashicorp.com/nomad/api-docs/volumes"""

from nomad.api.base import Requester
from time import sleep


class Volume(Requester):
    """
    The endpoint manage single volume

    https://developer.hashicorp.com/nomad/api-docs/volumes
    """

    ENDPOINT = "volume"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def __str__(self):
        return f"{self.__dict__}"

    def __repr__(self):
        return f"{self.__dict__}"

    def __getattr__(self, item):
        raise AttributeError

    def create_csi_volume(
        self,
        id_,
        volume_config,
        override_sentinel_policies=False,
        wait_until_schedulable = True
    ):
        """
        This endpoint creates or updates a volume.
        https://developer.hashicorp.com/nomad/api-docs/volumes#create-csi-volume

        arguments:
          - id_ :(str), volume ID
          - volume_config :(dict), Single volume configuration. Example:
            https://developer.hashicorp.com/nomad/api-docs/volumes#sample-payload-1
          - override_sentinel_policies :(bool) optional, If set, will ignore Sentinel
            soft policies
          - wait_until_schedulable: (bool) optional, wait until volume is schedulable
        returns: dict
        raises:
          - nomad.api.exceptions.BaseNomadException
          - nomad.api.exceptions.URLNotFoundNomadException
        """
        data = self.request(
            "csi",
            id_,
            "create",
            json={
                "PolicyOverride": override_sentinel_policies,
                "Volumes": [volume_config]
            },
            method="put"
        ).json()["Volumes"][0]
        while not wait_until_schedulable or not data["Schedulable"]:
            data = self.get_csi_volume(id_, volume_config["Namespace"])
            sleep(2)
        return data

    def get_csi_volume(self, id_, namespace=None):
        """This endpoint reads information about a specific volume by ID.

        https://developer.hashicorp.com/nomad/api-docs/volumes#read-csi-volume

        arguments:
          - id_ :(str), volume ID
          - namespace:(str) optional, namespace
        returns: dict
        raises:
          - nomad.api.exceptions.BaseNomadException
          - nomad.api.exceptions.URLNotFoundNomadException
        """
        params = {}
        if namespace:
            params["namespace"] = namespace
        return self.request("csi", id_, params=params, method="get").json()
