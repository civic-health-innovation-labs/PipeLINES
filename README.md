# PipeLINES
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

First, you need to configure the `SPARK_CONFIG` environmental variable in the HeifER configuration section in the following way:
```python
SPARK_CONFIG: Optional[dict[str, Any]] = {
     # A) MANDATORY: link to TRE workspace for Dataset provisioning (TRE-related)
     "spark.secret.workspace-tenant-id": "TODO_FILL",
     "spark.secret.workspace-app-id": "TODO_FILL",
     "spark.secret.workspace-app-secret": "TODO_FILL",
     # B) MANDATORY: Enable change data feed
     "spark.databricks.delta.properties.defaults.enableChangeDataFeed": True,
     # C) MANDATORY: Configuration of the SQL Server Connection
     "spark.secret.database-fqdn": "TODO_FILL",
     "spark.secret.database-database": "TODO_FILL",
     "spark.secret.database-username": "TODO_FILL",
     "spark.secret.database-password": "TODO_FILL",
     "spark.secret.database-use-aad-service-principal-auth": False,
     "spark.secret.database-trust-server-certificate": True,
 }
```
Also, you need to specify authentication in HeifER, in the definition of the `heifer_link_adf_databricks` use the following (or any modification based on your logic):
```python
spark_config=HeiferClusterConfiguration.SPARK_CONFIG | {
   # A) MANDATORY: Connection to Data lake
   f"fs.azure.account.auth.type.{HeiferConfig.STORAGE_ACCOUNT_NAME}.dfs.core.windows.net": "OAuth",
   f"fs.azure.account.oauth.provider.type.{HeiferConfig.STORAGE_ACCOUNT_NAME}.dfs.core.windows.net": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
   f"fs.azure.account.oauth2.client.id.{HeiferConfig.STORAGE_ACCOUNT_NAME}.dfs.core.windows.net": heifer_service_principal_for_databricks_storage_account.client_id.apply(lambda _client_id: _client_id),
   f"fs.azure.account.oauth2.client.secret.{HeiferConfig.STORAGE_ACCOUNT_NAME}.dfs.core.windows.net": heifer_app_for_databricks_storage_account_password.value.apply(lambda _value: _value),
   f"fs.azure.account.oauth2.client.endpoint.{HeiferConfig.STORAGE_ACCOUNT_NAME}.dfs.core.windows.net": f"https://login.microsoftonline.com/{CURRENT_CLIENT.tenant_id}/oauth2/token",
   "spark.secret.datalake-uri": f"{HeiferConfig.STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
},
```

Then, HeifER and Pipelines need to synchronize `linkedServiceName` in the `pipelines.json` file. 

Ultimately, libraries in `pipelines.json` need to be updated to match the concrete location in HeifER's `STORAGE_ACCOUNT_NAME`. Following the logic:
```
abfss://BLOB_NAME@STORAGE_ACCOUNT_NAME.dfs.core.windows.net/...
```
where `BLOB_NAME` is usually equal to `libraries`.

Before deploying libraries, you must build artifacts in the way that is described in the HeifER's main documentation.

Also, if your software requires external Python's dependencies, specify them in the `pyproject.toml` file in the section `dependencies` (chapter `[project]`).

## How to deploy pipelines into Azure?
Use the manual in the `README.md` file of the HeifER repository.
