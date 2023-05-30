# DevOps-Utilities
Azure Function App scripts created that assist in DevOps tasks. Some of these scripts are called via POST request and require a JSON payload as input. Examples for these inputs can be found in the "Example-Payloads" folder. If there is no example in the folder, it is because the function is either timer based, or only uses local.settings.json config data.

General Overview of each script:

CustomAlertEmail: This script uses SMTP information (Host, User, Password, Port, SSL) to test if emails can be sent successfully. If not, errors thrown can be used for debugging and find the root cause of the issue.

DBMigration: An Azure Function to migrate a Cosmos DB from one account to another. It accepts a JSON payload as input where you specify the source and destination accounts/DB names.

DBMigrationDataChange: Similar to the DBMigration script, but after the migration, an Azure Storage Table is accessed (stated in the input JSON) where certain information is pulled and swapped out in the newly migrated Cosmos DB. More information on this script is located in the README for the OneClick repo.

ProductionCosmosBackup: An Azure Timer Function that triggers every night. This function accesses a table which contains information for all tenant Cosmos DB Accounts. These connection strings are fetched used to connect to a tenant's Cosmos DB. Vital information from these DBs are then fetched and stored into a seperate Azure Storage account as backups (JSON files). The hierarchy for these backups organize them by POD (region), tenant ID, and DEV/PRD.

ProductionStorageAccountBackup: Similar to the above function, but for a tenant's Azure Storage Acocunt. Backups will be taken for the Blob Containers and Tables. The hierarchy is similar to the Cosmos DB backups for the sake of organization.
