using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.ApplicationInsights;
using VSI.CloudPlaform.Core.Db;
using VSI.CloudPlatform.Core.Functions;
using CommonUtilityCode;
using Azure.Storage.Blobs;
using Azure;
using Azure.Storage.Blobs.Models;
using Microsoft.Azure.Storage.Blob;
using Dasync.Collections;
using VSI.CloudPlatform.Core.Telemetry;

namespace ProductionStorageAccountBackup
{
    public class ProductionStorageAccountBackup
    {



        #region Telemetry Objects
        //grabs variable values from the configuration tab within Azure. these variables are used for the app insights/telemetry
        private static readonly string _key = Environment.GetEnvironmentVariable("APPINSIGHTS_INSTRUMENTATIONKEY");
        private static bool _excludeDependency = FunctionUtilities.GetBoolValue(Environment.GetEnvironmentVariable("ExcludeDependency"), false);

        //below lines create telemetry client, this can be sent into different funcitons to track/monitor things
        IOperationHolder<RequestTelemetry> operation = null;
        TelemetryClient telemetryClient = null;

        #endregion

        #region List of Containers and Tables to be Saved

        List<string> containersToBeUploaded = new List<string> { "partnerlink", "mapping" };


        List<string> tablesToBeUploaded = new List<string>{
            "adapterCacheSteps",
            "adapterTransactionSteps",
            "alertConditions",
            "alertOperators",
            "apiReceiveUtilityMapping",
            "apiReceiveUtilityXmlFilesConfig",
            "apiReceiveXmlResponse",
            "apiReceiveXmlValidateConfig",
            "apiXMLReceiveUtilityConfig",
            "as2LogsConfig",
            "as2MdnConfig",
            "as2plcertsconfig",
            "as2Server",
            "cacheEntity",
            "cacheEntityJob",
            "cacheEntityType",
            "cacheTransactionService",
            "certificateInfo",
            "clientErpSetting",
            "controlNumConfig",
            "csvreport997config",
            "datamigrationconfig",
            "dataRemovalConfig",
            "defaultJobCrone",
            "defaultMap",
            "defaultSchema",
            "demo",
            "devOpsConfigs",
            "devOpsPublishProfiles",
            "document",
            "ediVersions",
            "emailConfig",
            "EmailSubjectConfig",
            "errorCode",
            "existedPartnership",
            "failedTransactionsOnDBUpdate",
            "fileNameMapping",
            "FlowTuningConfig",
            "ftpReceiveUtilityConfig",
            "ftpReceiveUtilityMapping",
            "ftpReceiveUtilityXmlFilesConfig",
            "httpUrlParamsConfig",
            "identifiercode",
            "ignorealertsdescription",
            "inbound997CommonlocationConfig",
            "insightsSettings",
            "InvoiceReportConfig",
            "keyDataMappings",
            "mappingDateConversions",
            "mappingTemplate",
            "mdnStatusEmailConfig",
            "partnershiptype",
            "powerBIConfig",
            "processesToBeSynced",
            "publishHistory",
            "registerInvalidChars",
            "registerTransactionsInvalidChar",
            "reportsConfiguration",
            "resubmitConfig",
            "segmentRename",
            "sftpReceiveUtilityConfig",
            "sftpReceiveUtilityMapping",
            "sftpReceiveUtilityXmlFilesConfig",
            "SourceStorageConnection",
            "stageDataPartitionKey",
            "storagereceiveutilityconfig",
            "TargetStorageConnection",
            "transaction",
            "transactiondocument",
            "transactionsegment",
            "transactionset",
            "transactionsteps",
            "transactionTemplates",
            "UsersCatalog"
        };


        #endregion

        #region Objects for Destination
        private static readonly string blobStorageConnectionString = Environment.GetEnvironmentVariable("blobStorageConnectionString");
        private static readonly string blobStorageDirectoryName = Environment.GetEnvironmentVariable("blobStorageDirectoryName");


        Microsoft.Azure.Storage.Auth.StorageCredentials credentials;
        Microsoft.Azure.Storage.CloudStorageAccount storageAccount;
        Microsoft.Azure.Storage.Blob.CloudBlobClient blobClient1;
        Microsoft.Azure.Storage.Blob.CloudBlobContainer blobContainer;


        #endregion

        #region Objects to fetch Content in Source StrAcct

        Microsoft.Azure.Storage.Auth.StorageCredentials credentialsSource;
        Microsoft.Azure.Storage.CloudStorageAccount storageAccountSource;
        Microsoft.Azure.Storage.Blob.CloudBlobClient blobClient1Source;
        Microsoft.Azure.Storage.Blob.CloudBlobContainer blobContainerSource;

        private int CPUthreads;
        #endregion

        //modify these lists to check specific tenants for errors
        #region Objects for Tenants
        List<string> codeList = new List<string> { "All" };
        List<string> podList = new List<string> { "POD01", "POD02", "POD03", "POD04", "POD05" };

        //List<string> codeList = new List<string> { "All" };
        //List<string> podList = new List<string> { "POD04"};

        private List<TenantConfigModel> tennats = null;

        #endregion



        public async void BaseFunction()
        {

            //this fetches the number of the current day. It will be use to create/select the directory in which the current backups will be stored
            string day = getCurrentDay(DateTime.Today.Day);

            CPUthreads = Convert.ToInt32(Math.Ceiling((Environment.ProcessorCount * 0.75) * 1.0));
            appInsightLog("Number of Threads to be used in Parallel Upload: " + CPUthreads);

            foreach (var tenant in tennats)
            {

                try
                {

                    #region Print Tenant Name
                    Console.ForegroundColor = ConsoleColor.Magenta;
                    appInsightLog(tenant.TenantCode);
                    Console.ResetColor();
                    #endregion

                    var databaseManipulation = new DatabaseManipulation(tenant.CosmosConnection); //creates a new DB manipulation obj given the tenants connection string

                    //fetches account name from service bus CS. we will pull POD number from this.
                    //its needed for placing backups in correct directory
                    #region GETTING POD NAME FOR CURRENT TENANT
                    var builder = new DbConnectionStringBuilder { ConnectionString = tenant.ServicebusConnectionString };
                    builder.TryGetValue("Endpoint", out dynamic accountName);

                    int subStringStart = accountName.IndexOf("0", 0);
                    string podName = "POD" + accountName.Substring(subStringStart, 2);
                    #endregion

                    #region Getting all container names in current tennant
                    //gets all containers in current tenant
                    BlobServiceClient acct = new BlobServiceClient(tenant.BlobStore);
                    //AsyncPageable<BlobContainerItem> listOfContainers = acct.GetBlobContainersAsync();
                    var listOfContainers = acct.GetBlobContainersAsync(BlobContainerTraits.Metadata, default).AsPages(default, 10);

                    #endregion

                    #region Getting all table names in current tennant
                    CloudStorageAccount storageAccount = CloudStorageAccount.Parse(tenant.BlobStore);
                    CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
                    TableContinuationToken token = new TableContinuationToken();
                    List<dynamic> finalListOfTables = new List<dynamic>();
                    do
                    {
                        var resulSegment = await tableClient.ListTablesSegmentedAsync(null, token);

                        token = resulSegment.ContinuationToken;
                        foreach (var table in resulSegment)
                        {
                            finalListOfTables.Add(table);
                        }
                    }
                    while (token != null);


                    #endregion

                    #region Connecting and Getting all Container Content

                    var bobTheBuilder = new DbConnectionStringBuilder { ConnectionString = tenant.BlobStore };
                    bobTheBuilder.TryGetValue("AccountName", out dynamic accountNameSource);
                    bobTheBuilder.TryGetValue("AccountKey", out dynamic accountKeySource);

                    credentialsSource = new Microsoft.Azure.Storage.Auth.StorageCredentials(accountNameSource, accountKeySource);
                    storageAccountSource = new Microsoft.Azure.Storage.CloudStorageAccount(credentialsSource, true);
                    blobClient1Source = storageAccountSource.CreateCloudBlobClient();
                    blobContainerSource = blobClient1Source.GetContainerReference(blobStorageDirectoryName);

                    #endregion


                    int skipCounter = 0;
                    int counter = 0;
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    appInsightLog($" - Saving Containers for {tenant.TenantCode}...");
                    Console.ResetColor();

                    appInsightLog("Saving Containers for: " + tenant.TenantCode);

                    await foreach (Page<BlobContainerItem> containerPage in listOfContainers)
                    {

                        foreach (var container in containerPage.Values)
                        {
                            var containerName = container.Name;
                            

                            #region Getting Blob names & Uploading
                            
                            if (containersToBeUploaded.Contains(containerName) || containerName.Contains("mapping")) //use containers in list or any container related to 'mapping'
                            {
                                blobContainerSource = blobClient1Source.GetContainerReference(containerName);
                                BlobContainerClient sourceClient = new BlobContainerClient(tenant.BlobStore, containerName);
                                AsyncPageable<BlobHierarchyItem> blobHi = sourceClient.GetBlobsByHierarchyAsync(BlobTraits.None, BlobStates.None, "/");

                                //iterate through main blobs linerally, one-step into the container.
                                //There is uaully large data within these main blob Dirs, so we will go through those in parrallel
                                await foreach (var blob in blobHi)
                                {


                                    //need to add upload here
                                    if (blob.IsBlob)                                      
                                    {
                                        var name = blob.Blob.Name;
                                        await BlobUpload(name, podName, tenant.TenantCode, containerName, day);

                                    }
                                    else if (blob.IsPrefix && blob.Prefix != "") //checks to make sure its a directory and that it has a name
                                    {
                                        await mainContainerDirectory(blob.Prefix, podName, tenant.TenantCode, sourceClient, containerName, day);
                                    }

                                }
                                


                                counter++; //increments container counter
                            }
                            else
                                skipCounter++; //increments skipped container counter
                        #endregion



                        Console.ForegroundColor = ConsoleColor.Green;
                        Console.Write("\r   - Containers Skipped: {0} || Containers backed up: {1}", skipCounter, counter);
                        Console.ResetColor();
                        }

                    }

                    appInsightLog("Saving Tables for: " + tenant.TenantCode);
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine($"\n - Saving Tables for {tenant.TenantCode}...");
                    Console.ResetColor();

                    #region Table Saving and Uploading
                    var tableCounter = 0;
                    var skippedTableCounter = 0;
                    foreach (var table in finalListOfTables)
                    {

                        //only format data and upload if it is a desired table in the list of tables to be uploaded. 
                        if (tablesToBeUploaded.Contains(table.Name))
                        {
                            var targetTable = tableClient.GetTableReference(table.Name); //get table 
                            var results = new JArray();
                            var query = new TableQuery();
                            var data = targetTable.ExecuteQuerySegmentedAsync(query, null).Result;

                            //get all data within table
                            if (data.Results.Count > 0)
                            {
                                foreach (var item in data)
                                {
                                    var obj = new JObject();

                                    obj.Add("PartitionKey", item.PartitionKey);
                                    obj.Add("RowKey", item.RowKey);

                                    foreach (var p in item.Properties)
                                    {
                                        obj.Add(p.Key, JToken.FromObject(p.Value.PropertyAsObject));
                                    }

                                    results.Add(obj);
                                }
                            }

                            var fileForUpload = JsonConvert.SerializeObject(results, Formatting.Indented);

                            Microsoft.Azure.Storage.Blob.CloudBlockBlob destination = blobContainer.GetBlockBlobReference($"Tenants/{podName}/PROD/{tenant.TenantCode}/StorageAccount/Tables/{day}/{table.Name}.json"); //file will be stored here

                            destination.UploadTextAsync(fileForUpload).GetAwaiter();
                            tableCounter++;
                        }
                        else
                            skippedTableCounter++;

                        Console.ForegroundColor = ConsoleColor.Green;
                        Console.Write("\r  - Tables Skipped: {0} || Tables backed up: {1}", skippedTableCounter, tableCounter);
                        Console.ResetColor();

                    }

                    #endregion


                    #region Print Tenant Name
                    Console.ForegroundColor = ConsoleColor.Cyan;
                    appInsightLog($"\nBackup Complete for {tenant.TenantCode}\n");
                    Console.ResetColor();
                    #endregion
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

        }

        /// <summary>
        /// This funciton is entered when iterating through the first set of directories within a container. Any subsequent Dirs are processed through ContainerRecursion() function
        /// </summary>
        /// <param name="prefix"></param>
        /// <param name="podName"></param>
        /// <param name="tenantCode"></param>
        /// <param name="sourceClient"></param>
        /// <param name="containerName"></param>
        /// <param name="day"></param>
        /// <returns></returns>
        public async Task mainContainerDirectory(string prefix, string podName, string tenantCode, BlobContainerClient sourceClient, string containerName, string day)
        {
            try
            {
                //fetch new set of blobs within this directory
                AsyncPageable<BlobHierarchyItem> innerblobHi = sourceClient.GetBlobsByHierarchyAsync(BlobTraits.None, BlobStates.None, "/", prefix);

                await innerblobHi.ParallelForEachAsync(async blob =>
                {
                    //upload file
                    if (blob.IsBlob && blob.Blob.Name.Contains(prefix))
                    {
                        var name = blob.Blob.Name;
                        await BlobUpload(name, podName, tenantCode, containerName, day);

                    }
                    else if (blob.Prefix != null)                        
                    {
                        var newPrefix = blob.Prefix;
                        await ContainerRecursion(newPrefix, podName, tenantCode, sourceClient, containerName, day);
                    }

                }, maxDegreeOfParallelism: 0); //0 = default amount based on processor count. CPUThreads = 75% of CPU

            }
            catch (Exception ex)
            {
                appInsightLog("EXCEPTION IN CONTAINER RECURSION: " + ex.Message);
                telemetryClient.TrackException(ex);
                ErrorHandling.throwErrorNormal(ex);
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

        /// <summary>
        /// Creates a TrackTace to send a message for app insights within Azure. Also prints it out normally for local testing.
        /// </summary>
        /// <param name="telem"></param>
        /// <param name="msg"></param>
        public void appInsightLog(string msg)
        {
            telemetryClient.TrackTrace("storageaccount-prod-backups: " + msg);
            telemetryClient.TrackEvent(msg);
            telemetryClient.Flush();
            Console.WriteLine(msg);
        }

        public async Task ContainerRecursion(string prefix, string podName, string tenantCode, BlobContainerClient sourceClient, string containerName, string day)
        {
            try
            {
                //fetch new set of blobs within this directory
                AsyncPageable<BlobHierarchyItem> innerblobHi = sourceClient.GetBlobsByHierarchyAsync(BlobTraits.None, BlobStates.None, "/", prefix);
                List<BlobHierarchyItem> blobList = await innerblobHi.ToListAsync();


                foreach (var blob in blobList)
                {
                    //upload file
                    if (blob.IsBlob && blob.Blob.Name.Contains(prefix))
                    {
                        var name = blob.Blob.Name;
                        await BlobUpload(name, podName, tenantCode, containerName, day);

                    }
                    else if (blob.Prefix != null)
                    {
                        var newPrefix = blob.Prefix;
                        await ContainerRecursion(newPrefix, podName, tenantCode, sourceClient, containerName, day);
                    }
                }

            }
            catch (Exception ex)
            {
                appInsightLog("EXCEPTION IN CONTAINER RECURSION: " + ex.Message);
                telemetryClient.TrackException(ex);
                ErrorHandling.throwErrorNormal(ex);
            }

        }

        public async Task BlobUpload(string name, string podName, string tenantCode, string containerName, string day)
        {
            Microsoft.Azure.Storage.Blob.CloudBlockBlob destination = blobContainer.GetBlockBlobReference($"Tenants/{podName}/PROD/{tenantCode}/StorageAccount/BlobContainers/{day}/{containerName}/{name}"); //file will be stored here
            Microsoft.Azure.Storage.Blob.CloudBlockBlob sourceFile = blobContainerSource.GetBlockBlobReference(name);
            //string file = sourceFile.DownloadTextAsync().GetAwaiter().GetResult();

            try
            {
                if (sourceFile.BlobType == Microsoft.Azure.Storage.Blob.BlobType.AppendBlob)
                {
                    var snapshot = await sourceFile.CreateSnapshotAsync();
                    var text = await snapshot.DownloadTextAsync();
                    await destination.UploadTextAsync(text);
                }
                else
                {
                    using (var destStream = await destination.OpenWriteAsync())
                    {
                        using (var sourceStream = await sourceFile.OpenReadAsync())
                        {
                            await sourceStream.CopyToAsync(destStream);
                        }
                    }
                }
            }
            catch(Exception ex)
            {
                if (telemetryClient != null)
                {
                    telemetryClient.TrackException(ex);
                }
                appInsightLog("ERROR IN BLOB UPLOAD: " + ex.Message);
            }
            



        }

        public void fetchTenants()
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
                        catch (Exception ex)
                        {

                            if (telemetryClient != null)
                            {
                                telemetryClient.TrackException(ex);
                                appInsightLog(ex.Message);
                            }

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

        public void CloudConnections()
        {
            #region Strg Act Connection
            var bobTheBuilder = new DbConnectionStringBuilder { ConnectionString = blobStorageConnectionString };
            bobTheBuilder.TryGetValue("AccountName", out dynamic accountName);
            bobTheBuilder.TryGetValue("AccountKey", out dynamic accountKey);

            credentials = new Microsoft.Azure.Storage.Auth.StorageCredentials(accountName, accountKey);
            storageAccount = new Microsoft.Azure.Storage.CloudStorageAccount(credentials, true);
            blobClient1 = storageAccount.CreateCloudBlobClient();
            blobContainer = blobClient1.GetContainerReference(blobStorageDirectoryName);
            #endregion
        }

        
        [FunctionName("ProductionStorageAccountBackup")]
        public void Run([TimerTrigger("0 3 * * *")]TimerInfo myTimer, ILogger log)
        {
            telemetryClient = TelemetryFactory.GetInstance("ProductionStorageAccountBackup", _key, _excludeDependency); //creates an instance of a telemetry and connects it to the function given its name/key
            operation = telemetryClient.StartOperation<RequestTelemetry>("ProductionStorageAccountBackup", Guid.NewGuid().ToString());

            fetchTenants();
            CloudConnections();
            BaseFunction();
        }
    }
}
