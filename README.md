# DM2T

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python](https://img.shields.io/badge/python-3.10-red)](https://www.python.org/)
[![Scala](https://img.shields.io/badge/Scala-2.12.12-red)](https://www.scala-lang.org/)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)][contributing]

DM2T is an open source project that provides some tools to copy a set of Azure container content to another Azure container and validate the data transfer.
This tool is composed of two parts:
- A Python notebook that uses the `azcopy` tool to copy the data from one container to another.
- A Scala notebook that validates the data transfer by comparing the delta tables between the source and destination containers.

## Features
- Copy several Azure container data to others.
- Execute multiple incremental copies while preventing duplicate entries.
- Validate the data transfer by comparing the delta tables between the source and destination containers.
    - Compare the total number of rows between source and destination.
    - Verify the number of rows per grouping key.
    - Compare the delta table history.

## Getting Started

### `Datamesh2Datamesh-transfer.ipynb`

This notebook is used to copy data from one data mesh container to another within the same storage account. The same directory will be used on the destination.

This notebook has been tested on Azure AI Machine Learning studio which includes by default AzCopy.

#### Pre-requisites:
- Ensure [AzCopy](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azcopy-v10?tabs=dnf) is installed on the VMs.
- Ensure the necessary permissions to access the storage accounts and containers are set.

#### Usage:
1. Copy `Datamesh2Datamesh-transfer.ipynb` and `azcopy-script.sh` to your notebook directory.
2. Define the parameters in the notebook (see Parameters).
3. Run the notebook to initiate the data transfer.
4. Monitor the output for status updates and logs.

#### Parameters:
The following parameters have to be defined in the notebook section `Define parameters`:

| Parameter             | Description                                                       |
|-----------------------|-------------------------------------------------------------------|
| `log_level`           | The logging level (e.g., "ERROR").                                |
| `src_storage_account` | The default source storage account.                               |
| `dst_storage_account` | The default destination storage account.                          |
| `containers_to_copy`  | A list of `ContainerCopyParam` objects defining the data to copy. |

#### ContainerCopyParam:
The `ContainerCopyParam` object defines the parameters for copying data from one container to another.

| Member                | Optional | Description                                                                  |
|-----------------------|----------|------------------------------------------------------------------------------|
| `src_container`       | No       | The source container name.                                                   |
| `dst_container`       | No       | The destination container name.                                              |
| `src_path`            | No       | The source path within the container.                                        |
| `dst_path`            | Yes      | The destination path within the container (default: same as source path).    |
| `src_storage_account` | Yes      | The source storage account (default: `src_storage_account`).                 |
| `dst_storage_account` | Yes      | The destination storage account (default: `dst_storage_account`).            |


### `transferValidation.scala`

This Scala notebook is used to validate the data transfer.

This notebook has been tested and designed on Databricks workspace.

#### Pre-requisites:
- Ensure the necessary permissions to access the storage accounts and containers.
- Ensure the dataset is stored in delta format.

#### Usage:
1. Copy `transferValidation.scala` to your notebook directory.
2. Define the parameters in the notebook (see Parameters).
3. Run the notebook to initiate the data validation.
4. Monitor the output for validation results.

#### Parameters:
The following parameters have to be defined in the first section:

| Parameter                        | Description                              |
|----------------------------------|------------------------------------------|
| defaultSourceStorageAccount      | The default source storage account.      |
| defaultDestinationStorageAccount | The default destination storage account. |

#### Container Validation Parameters:
The set of `ContainerValidationParam` have to be defined in the notebook section `Define containers to check`:

| Member                      | Optional | Description                                                                               |
|-----------------------------|----------|-------------------------------------------------------------------------------------------|
| `sourceContainer`           | No       | The source container name.                                                                |
| `destinationContainer`      | Yes      | The destination container name (default: same as source path).                            |
| `sourceDirectory`           | No       | The source path of the dataset within the container.                                      |
| `destinationDirectory`      | Yes      | The destination path  of the dataset within the container (default: same as source path). |
| `sourceStorageAccount`      | Yes      | The source storage account (default: `defaultSourceStorageAccount`).                      |
| `destinationStorageAccount` | Yes      | The destination storage account (default: `defaultDestinationStorageAccount`).            |
| `groupColumn`               | Yes      | the column used to group data for detailed check.                                         |


#### Validation KPIs:
- Total number of rows between source and destination.
- Number of rows per group between source and destination.
- Compares the delta table history.

## How to Use

1. Clone the repository.
2. Follow the instructions provided for each notebook.
3. Review the outputs for status updates, logs, and validation results.

For more detailed information on each notebook or script, refer to the comments and documentation within the code.

## Issues and Support
If you encounter any issues or require support, please create a new issue on the [GitHub repository][issues].

## Contribution
Contributions to DM2T are welcome! To contribute, please follow the guidelines outlined in [our contribution guide][contributing].

## License
This project is licensed under the Apache License 2.0 license. See the [LICENSE][license] file for more information.

[contributing]: CONTRIBUTING.md
[codeofconduct]: CODE_OF_CONDUCT.md
[license]: LICENSE
[repository]: https://github.com/AmadeusITGroup/dm2t
[issues]: https://github.com/AmadeusITGroup/dm2t/issues
