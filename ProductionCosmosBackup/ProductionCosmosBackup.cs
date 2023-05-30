using System;
using System.Collections.Generic;
using CommonUtilityCode;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using VSI.CloudPlatform.Core.Telemetry;
using VSI.CloudPlatform.Core.Functions;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Auth;
using Microsoft.Azure.Storage.Blob;
using VSI.CloudPlaform.Core.Db;
using Azure.Storage.Blobs;
using System.Data.Common;
using System.Linq;

namespace ProductionCosmosBackup
{
    public class ProductionCosmosBackup
    {


        #region Telemetry Objects
        //grabs variable values from the configuration tab within Azure. these variables are used for the app insights/telemetry
        private static readonly string _key = Environment.GetEnvironmentVariable("APPINSIGHTS_INSTRUMENTATIONKEY");
        private static bool _excludeDependency = FunctionUtilities.GetBoolValue(Environment.GetEnvironmentVariable("ExcludeDependency"), false);

        //below lines create telemetry client, this can be sent into different funcitons to track/monitor things
        IOperationHolder<RequestTelemetry> operation = null;
        TelemetryClient telemetryClient = null;

        #endregion

        #region Variables used in Code
        private static string[] ContainerNamesForUpload = { "process_flow", "partnership", "configuration_data", "cache" };
        private static string[] query = { "SELECT * FROM c where c.entity_name in ('adapters','flow')", "SELECT * FROM c", "SELECT * FROM c where c.entity_name not like '%alert_history%' and c.entity_name not like '%TransactionFileHistory%'", "SELECT * FROM c where c.entity_type in ('company', 'customer', 'cross_reference_table')" };
        List<string> codeList = new List<string> { "All" };
        List<string> podList = new List<string> { "POD-dev1", "POD01", "POD02", "POD03", "POD04", "POD05" };
        //List<string> podList = new List<string> { "POD04", "POD05" };
        private static int queryTracker; //keeps track of position within query array

        private List<TenantConfigModel> tennats = null;

        #endregion

        #region Storage Account Objects
        private static readonly string blobStorageConnectionString = Environment.GetEnvironmentVariable("blobStorageConnectionString");
        private static readonly string blobStorageDirectoryName = Environment.GetEnvironmentVariable("blobStorageDirectoryName");


        StorageCredentials credentials;
        CloudStorageAccount storageAccount;
        CloudBlobClient blobClient1;
        CloudBlobContainer blobContainer;

        #endregion


        /// <summary>
        /// Main part of the function. Iterates through all tenants fetched in the list. Fetches their POD number, takes a backup of the tenantCatalog object, and DB containers
        /// </summary>
        public void BaseFunction()
        {
            try
            {
                var blobClient = new BlobContainerClient(blobStorageConnectionString, blobStorageDirectoryName);

                //this fetches the number of the current day. It will be use to create/select the directory in which the current backups will be stored
                string day = getCurrentDay(DateTime.Today.Day);

                foreach (var tenant in tennats)
                {
                    try
                    {
                        queryTracker = 0;//new tenent, so we are starting over in the query array
                        var databaseManipulation = new DatabaseManipulation(tenant.CosmosConnection); //creates a new DB manipulation obj given the tenants connection string

                        //fetches account name from service bus CS. we will pull POD number from this.
                        //its needed for placing backups in correct directory
                        #region GETTING POD NAME FOR CURRENT TENANT
                        var builder = new DbConnectionStringBuilder { ConnectionString = tenant.ServicebusConnectionString };
                        builder.TryGetValue("Endpoint", out dynamic accountName);

                        int subStringStart = accountName.IndexOf("0", 0);
                        string podName;
                        if (subStringStart == -1) //this handles the specical POD case for WERNER 'POD-Dev1'
                        {
                            subStringStart = accountName.IndexOf("-dev", 0);
                            podName = "POD" + accountName.Substring(subStringStart, 5);
                        }
                        else
                        {
                            podName = "POD" + accountName.Substring(subStringStart, 2);
                        }
                        #endregion


                        //backup tenant obj first
                        #region BACKING UP TENANT OBJ
                        Console.WriteLine($"SAVING {tenant.TenantCode} TENANT OBJECT...");
                        CloudBlockBlob blobBlockForTenantTable = blobContainer.GetBlockBlobReference($"Tenants/{podName}/PROD/TenantTable/{tenant.TenantCode}.json"); //tenant obj will be stored here
                        dynamic tester = JsonConvert.SerializeObject(tenant, Formatting.Indented);
                        blobBlockForTenantTable.UploadTextAsync(JsonConvert.SerializeObject(tenant, Formatting.Indented)).GetAwaiter().GetResult();
                        Console.WriteLine($"SAVING COMPLETE");
                        #endregion



                        //backup process. either append or replace
                        #region ENTIRE BACKUP PROCESS

                        foreach (var arg in ContainerNamesForUpload)
                        {

                            queryArrayValidation();

                            CloudBlockBlob blobBlock = blobContainer.GetBlockBlobReference($"Tenants/{podName}/PROD/{tenant.TenantCode}/Cosmos-Backups/{day}/{arg}.json"); //file will be stored here
                            var container = databaseManipulation.GetDatabaseContainerObject(arg).Result;

                            Console.WriteLine("\nBEGINNING REPLACE OPERAITON...");
                            List<object> listOfContainerEntities = databaseManipulation.GetDBContainerEntities(container, query[queryTracker]).Result;
                            var blob = blobClient.GetBlobClient(arg + ".json");
                            blobBlock.UploadTextAsync(JsonConvert.SerializeObject(listOfContainerEntities, Formatting.Indented)).GetAwaiter().GetResult();//<<<<<<<<<<<<< original upload
                            Console.WriteLine("Replace Backup Completed for: " + tenant.TenantCode + " - " + arg);

                            queryTracker++; //increment the tracker to access next query
                            
                        }

                        #endregion

                        Console.WriteLine("\n\nUpload Completed");
                        Console.WriteLine("'" + tenant.TenantCode + "' successfully uploaded!");
                        databaseManipulation.CloseConnection();
                    }
                    catch (Exception ex) //this catch checks if the exception is that the host doest exist. if thats the case, the tenant doesnt exist. and code continues
                    {

                        if (telemetryClient != null)
                        {
                            telemetryClient.TrackException(ex);
                        }

                        ErrorHandling.throwErrorNormal(ex);
                    }
                }

                //close connection to specific file destination
                blobClient = null;

            }
            catch (Exception ex)
            {
                if (telemetryClient != null)
                {
                    telemetryClient.TrackException(ex);
                }

                ErrorHandling.throwErrorNormal(ex);

            }
        }

        /// <summary>
        /// Checks if the current query using the query tracker is null, if it is, subtract one to prevent Out of Bounds exception.
        /// This means if there are less queries than containers, all extra containers will use the last query in the query list.
        /// </summary>
        public void queryArrayValidation()
        {
            if (queryTracker >= query.Length)
            {
                Console.WriteLine("End of Query List has been reached, using final query for rest of tables...");
                queryTracker--;
            }
            else
            {
                Console.WriteLine("Current Query Exists");
            }

        }

        public string getCurrentDay(int dayNumber)
        {
            string day = string.Empty;

            switch (dayNumber % 10)
            {
                case 1:
                    day = dayNumber + "st";
                    break;
                case 2:
                    day = dayNumber + "nd";
                    break;
                case 3:
                    day = dayNumber + "rd";
                    break;
                default:
                    day = dayNumber + "th";
                    break;
            }


            return day;
        }

        public void fetchTenants()
        {
            try
            {

                if (codeList != null && codeList[0] == "All")
                {
                    if (tennats == null)
                        tennats = new List<TenantConfigModel>();

                    //the nested foreach loop below will iterate through the list of PODs, and grab all tenenant within all PODs
                    foreach (var pod in podList)
                    {
                        try
                        {
                            //backs up the tenant catalog table of the entire POD as a single file
                            backupTenantTable(pod);

                            //changes the connection string depending on the POD in the podList
                            string changingStorageConnectionString = Environment.GetEnvironmentVariable("CommonStorageConnetionString_" + pod);
                            Console.WriteLine($"Fetching tenants for {pod}...");
                            //gets list of tenants
                            var test = TenantTableHelper.GetTenantModelList(changingStorageConnectionString);
                            //goes through list of tenant items to add into tenant list
                            foreach (var item in test)
                            {
                                tennats.Add(item);
                            }

                            Console.WriteLine(" - Fetched");
                        }
                        catch(Exception ex)
                        {
                            if (telemetryClient != null)
                            {
                                telemetryClient.TrackException(ex);
                            }
                            ErrorHandling.throwErrorNormal(ex);
                        }
                        
                    }


                }
                else if (codeList != null)
                {
                    if (tennats == null)
                        tennats = new List<TenantConfigModel>();

                    foreach (var pod in podList)
                    {
                        //create connection string for given pod in the list
                        string changingStorageConnectionString = Environment.GetEnvironmentVariable("CommonStorageConnetionString_" + pod);
                        //fetching list of tennants in specific pod

                        var tenanatConfigs = TenantTableHelper.GetTenantModelList(changingStorageConnectionString);

                        foreach (var item2 in tenanatConfigs)
                        {
                            if (codeList.Contains(item2.TenantCode))
                            {
                                tennats.Add(item2);
                            }
                        }
                    }


                } //!!! if for whatever reason the pod/code list changes, it will go here to fetch those values

            }
            catch (Exception ex)
            {

                if (telemetryClient != null)
                {
                    telemetryClient.TrackException(ex);
                }
                ErrorHandling.throwErrorNormal(ex);
            }
        }

        public void backupTenantTable(string podName)
        {
            try 
            {
                List<TenantConfigModel> tenantList = new List<TenantConfigModel>();
                dynamic entireTenantTableFile = string.Empty;

                //changes the connection string depending on the POD in the podList
                string changingStorageConnectionString = Environment.GetEnvironmentVariable("CommonStorageConnetionString_" + podName);

                //gets list of tenants
                var fetchedTenantList = TenantTableHelper.GetTenantModelList(changingStorageConnectionString);

                var last = fetchedTenantList.Last();
                //goes through list of tenant items to add into tenant list
                foreach (var tenant in fetchedTenantList)
                {
                    string serializedTenant = JsonConvert.SerializeObject(tenant, Formatting.Indented);

                    if (tenant.Equals(last))
                    {
                        entireTenantTableFile = entireTenantTableFile + serializedTenant;
                    }
                    else
                    {
                        entireTenantTableFile = entireTenantTableFile + serializedTenant + ",";
                    }

                }

                entireTenantTableFile = "[\n" + entireTenantTableFile + "\n]";

                CloudBlockBlob blobBlockForTenantTable = blobContainer.GetBlockBlobReference($"Tenants/{podName}/PROD/TenantTable/SingleFile/TenantCatalog.json"); //tenant obj will be stored here
                blobBlockForTenantTable.UploadTextAsync(entireTenantTableFile).GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                if (telemetryClient != null)
                {
                    telemetryClient.TrackException(ex);
                }
                ErrorHandling.throwErrorNormal(ex);
            }
            

        }

        
        public void cloudConnections()
        {
            #region Strg Act Connection
            var bobTheBuilder = new DbConnectionStringBuilder { ConnectionString = blobStorageConnectionString };
            bobTheBuilder.TryGetValue("AccountName", out dynamic accountName);
            bobTheBuilder.TryGetValue("AccountKey", out dynamic accountKey);

            credentials = new StorageCredentials(accountName, accountKey);
            storageAccount = new CloudStorageAccount(credentials, true);
            blobClient1 = storageAccount.CreateCloudBlobClient();
            blobContainer = blobClient1.GetContainerReference(blobStorageDirectoryName);
            #endregion
        }



        [FunctionName("ProductionCosmosBackup")]
        public void Run([TimerTrigger("0 3 * * *")]TimerInfo myTimer, ILogger log)
        {

            telemetryClient = TelemetryFactory.GetInstance("ProductionCosmosBackup", _key, _excludeDependency); //creates an instance of a telemetry and connects it to the function given its name/key
            operation = telemetryClient.StartOperation<RequestTelemetry>("ProductionCosmosBackup", Guid.NewGuid().ToString());

            cloudConnections();
            fetchTenants();
            BaseFunction();

            //close connection to storage account (destination)
            storageAccount = null;
            operation.Dispose();
            telemetryClient.Flush();
        }
    }
}
