Objective
==========
To provide classes to 
- access Oracle Object Stores
- enumerate the contents of such stores
- download data from them
- upload data to them
- delete files in them

## Background

Oracle provide a [SDK](https://oracle-cloud-infrastructure-python-sdk.readthedocs.io/en/latest/) for their infrastructure.

It requires [authentication](https://docs.oracle.com/en-us/iaas/Content/API/Concepts/sdkconfig.htm).  
Examples of use in python are [provided](https://oracle-cloud-infrastructure-python-sdk.readthedocs.io/en/latest/configuration.html)

The SDK includes an [ObjectStorageClient](https://oracle-cloud-infrastructure-python-sdk.readthedocs.io/en/latest/api/object_storage/client/oci.object_storage.ObjectStorageClient.html) which can perform the objectives.


## Prerequisites
[OCI Python SDK](https://oracle-cloud-infrastructure-python-sdk.readthedocs.io/en/latest/installation.html) which can be installed via PyPI

