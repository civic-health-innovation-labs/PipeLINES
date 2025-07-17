# PipeLINES
Author: David Salac, University of Liverpool

This is the main repository containing pipelines. To deploy designed pipelines, use the repository named HeifER.

## What is good to know
There are a couple of things which is good to know before using this repository:
1. The HeifER project deploys these pipelines here in Azure.
2. Azure Data Factory (ADF) from where you can run pipelines.
3. Databricks and PySpark that perform the run.
4. Various technologies that are used for development
   (like Python, Pulumi, DevContainers, and MakeFiles).

## Developing in the local stack
All main tools are encapsulated in the Docker container to help
you run tests locally. It uses `.devcontainer` logic for the
Visual Studio Code, see:
 - [Devcontainers tutorial](https://code.visualstudio.com/docs/devcontainers/tutorial).

### How to run tests on your local machine
Use the following steps:
1. First, make sure your Docker is installed and running.
2. Then, open the project in the Dev container in VS Code.
3. Open the terminal and run:
```shell
make test
```

**Note:** 
Unit-tests are defined in the repositories `./PIPELINE_NAME/tests/`.

## Integration with HeifER
HeifER is the open-source infrastructure provisioning software
designed for deploying pipelines in this form.

First, you need to configure a series of environmental variables in the HeifER configuration section. There are the following `.env` files that are helpful:

1. `env-heifer.example.env`: The most important environmental variables related to HeifER infrastructure.
2. `env-dataset-provisioning.example.env`: Configuration of the dataset provisioning pipeline.
3. `env-bak-unzip-pipeline-example.env`: Configuration of the unzipping pipeline (that unloads zipped bak file into the Managed SQL Server Instance in Azure).
4. `env-rio.example.env`: Configuration of the RiO pipeline.

Then, HeifER and Pipelines need to synchronize `linkedServiceName` in the `pipelines.json` file. This is usually OK unless you change it.

Ultimately, libraries in `pipelines.json` need to be updated to match the concrete location in HeifER's `__STORAGE_ACCOUNT_NAME__`. Following the logic:
```
abfss://__CONTAINER_NAME__@__STORAGE_ACCOUNT_NAME__.dfs.core.windows.net/...
```
where `__CONTAINER_NAME__` is usually equal to `libraries`.

Before deploying libraries, you must build artifacts in the way that is described in the HeifER's main documentation (hint: just run `make artifacts` in the right folder).

Also, if your software requires external Python's dependencies, specify them in the `pyproject.toml` file in the section `dependencies` (chapter `[project]`).

## How to deploy pipelines into Azure?
Use the manual in the `README.md` file of the HeifER repository.

## How to run a single testcase?
```shell
pytest -s pipelines/rio_pipeline/src/rio_pipeline/tests/stages/test_end_to_end.py -m test_monitoring
```
The above command must be run in the terminal and it shows how the testcase is run and evaluated (due to the -s flag). 'test_monitoring' is the name of the custom marker of the testcase. It is helpful to debug any errors. 

## Local linting
Linter matching to the one in GitHub can be run locally from inside dev container using the command:
```shell
pre-commit run --all-files
```
