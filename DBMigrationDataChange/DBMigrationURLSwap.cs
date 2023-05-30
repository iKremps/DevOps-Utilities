using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Routing;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using VSI.CloudPlatform.Core.Telemetry;
using Microsoft.Extensions.Configuration;
using VSI.CloudPlatform.Core.Functions;
using Microsoft.Azure.Cosmos;
using System.Data.Common;
using Newtonsoft.Json.Linq;
using Microsoft.Azure.Cosmos.Table;

namespace DBMigrationDataChange
{
    internal class DBMigrationURLSwap
    {

        IOperationHolder<RequestTelemetry> operation = null;
        TelemetryClient telemetryClient = null;
        private static List<dynamic> tableEntities;
        private readonly string _key;
        private readonly string _tableConnectionString;
        private readonly bool _excludeDependency;
        private readonly IConfiguration _config;

        public DBMigrationURLSwap(IConfiguration configuration)
        {
            _config = configuration;
            _key = configuration.GetValue<string>("APPINSIGHTS_INSTRUMENTATIONKEY");
            _excludeDependency = FunctionUtilities.GetBoolValue(configuration.GetValue<string>("ExcludeDependency"), false);
            _tableConnectionString = Environment.GetEnvironmentVariable("tableConnectionString");
        }

        [Timeout("05:00:00")]
        [FunctionName("DBMigrationURLSwap")]
        public async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            try
            {

                var requestBody = await req.ReadAsStringAsync();
                var migrationInput = JsonConvert.DeserializeObject<InputModel>(requestBody);

                telemetryClient = TelemetryFactory.GetInstance("DBMigrationURLSwap", _key, _excludeDependency); //creates an instance of a telemetry and connects it to the function given its name/key
                operation = telemetryClient.StartOperation<RequestTelemetry>("DBMigrationURLSwap", Guid.NewGuid().ToString());

                var cosmosDbClient = DbConnections(migrationInput);
                MigrateDatabase(migrationInput, cosmosDbClient);

            }
            catch (Exception ex)
            {
                if (telemetryClient != null)
                {
                    appInsightLog(ex.Message);
                    telemetryClient.TrackException(ex);
                    telemetryClient.StopOperation(operation);
                }
            }


            return new OkResult();
        }


        public void MigrateDatabase(InputModel input, CosmosDbClients dbClient)
        {
            try
            {
                #region Get all Containers from Source
                Database sourceDB = dbClient.SourceClient.GetDatabase(input.sourceDBName);

                FeedIterator<ContainerProperties> iterator = sourceDB.GetContainerQueryIterator<ContainerProperties>();
                FeedResponse<ContainerProperties> containers = iterator.ReadNextAsync().ConfigureAwait(false).GetAwaiter().GetResult();
                #endregion

                #region Create Destination DB
                Database destinationDB = dbClient.DestinationClient.GetDatabase(input.destinationDBName.ToString());
                var destinationDbResponse = dbClient.DestinationClient.CreateDatabaseIfNotExistsAsync(input.destinationDBName.ToString()).GetAwaiter().GetResult();

                if (destinationDbResponse.StatusCode == System.Net.HttpStatusCode.OK) //if db exists (it should)
                {
                    appInsightLog("Destination DB Exists, modifying...");
                }
                else
                {
                    appInsightLog("Destination DB Created");
                }
                #endregion


                //the following containers will have their data migrated. If not included, the container is just created
                string[] ContainersForDataMigration = input.containersForMigration;

                foreach (var container in containers)
                {
                    #region Create Container
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    appInsightLog("- Creating DB Container (" + container.Id + ")...");
                    ContainerCreator(destinationDB, container.Id, container.PartitionKeyPath);
                    #endregion

                    #region Fill Data In Destination
                    if (ContainersForDataMigration.Contains(container.Id) || ContainersForDataMigration[0].Equals("all", StringComparison.CurrentCultureIgnoreCase))
                    {
                        Container con = sourceDB.GetContainer(container.Id);
                        Container destinationContainer = destinationDB.GetContainer(container.Id);
                        List<string> result = new List<string>();
                        result = Query(con).GetAwaiter().GetResult();

                        #region process_flow container adapter url swap
                        if (con.Id == "process_flow")
                        {
                            TableConnections(input);
                            string processFlowObj = ("[\n" + result[0] + "\n]");
                            dynamic editedJson = JsonConvert.DeserializeObject<dynamic>(processFlowObj);

                            #region FOREACH THAT DOES SWAPPING
                            appInsightLog("Beginning 'process_flow' iteration for URL replace...");
                            foreach (dynamic item in editedJson)
                            {
                                appInsightLog("- New Item. Making comparison...");
                                string functionName = item["Name"];
                                string functionDirection = item["Direction"];
                                dynamic childAdapterList = item["ChildAdapterList"];

                                if (item["ChildAdapterList"] != null)
                                {
                                    Console.WriteLine("- Child Adapter List found, emptying...");
                                    item["ChildAdapterList"].Clear();
                                    Console.WriteLine(" - Child List Emptied");
                                }

                                //goes through each url in table. breaks when url is matched
                                foreach (dynamic tableItem in tableEntities)
                                {

                                    //these variables will be filled with table entity properties
                                    string entityName = "";
                                    string entityDirection = "";
                                    string entityDevURL = "";
                                    string entityProdURL = "";
                                    string entityDRURL = ""; //DR URL is new and will be placed into the table

                                    //this for each loop goes through the list of table objects and places all values into variables.
                                    foreach (dynamic property in tableItem.Properties)
                                    {
                                        if (property.Key == "Name")
                                        {
                                            entityName = property.Value.StringValue;
                                        }
                                        if (property.Key == "Direction")
                                        {
                                            entityDirection = property.Value.StringValue;
                                        }
                                        if (property.Key == "DevUrl")
                                        {
                                            entityDevURL = property.Value.StringValue;
                                        }
                                        if (property.Key == "ProdUrl")
                                        {
                                            entityProdURL = property.Value.StringValue;
                                        }
                                        if (property.Key == "DrURL")
                                        {
                                            entityDRURL = property.Value.StringValue;
                                        }
                                    }

                                    if (entityName.Equals(functionName) && entityDirection.Equals(functionDirection))
                                    {
                                        appInsightLog(" - Function Match Found...");
                                        appInsightLog("  - Prepareing to replace URL...");

                                        if (input.EnvName == "DEV")
                                        {
                                            item["Url"] = entityDevURL;
                                            appInsightLog("   - URL changed successfully!");
                                        }
                                        else if (input.EnvName == "PROD" || input.EnvName == "PRD")
                                        {
                                            item["Url"] = entityProdURL;
                                            appInsightLog("   - URL changed successfully!");
                                        }
                                        else
                                        {
                                            throw new Exception("DBMigrationURLSwap ERROR: the Enviroment name givin in the input is invalid. Enter either DEV or PROD");
                                        }

                                        break;
                                    }
                                    else //if not a match, wipe URL to string.Empty
                                    {
                                        item["Url"] = string.Empty; //wipes URL
                                    }

                                    //end of table item loop
                                }


                                //end of 'process_flow' entity
                            }
                            #endregion //comment if you want to skip this part


                            string finalChangedJSON = JsonConvert.SerializeObject(editedJson, Formatting.Indented);
                            finalChangedJSON = finalChangedJSON.Substring(1, finalChangedJSON.Length - 2);
                            result[0] = finalChangedJSON;


                        }
                        #endregion

                        #region cache container info swap
                        if (con.Id == "cache")
                        {
                            if (result[0] == "" || result == null)
                            {
                                Console.WriteLine("cache is empty, skipping...");
                            }
                            else
                            {
                                foreach (var test in result.ToList())
                                {
                                    var testForEdit = JsonConvert.DeserializeObject<dynamic>(test);
                                    //change company attributes before saving to blob storage
                                    testForEdit["Company_Id"] = input.Company_id;
                                    testForEdit["Company_Code"] = input.Company_code;

                                    //dynamic finalTest = "[\n" + testForEdit + "\n]";
                                    result[0] = JsonConvert.SerializeObject(testForEdit, Formatting.Indented);

                                }
                            }

                        }
                        #endregion

                        #region Fill Container Data
                        if (result.Count > 0 && result[0] != "")
                        {
                            appInsightLog("  - Creating Items...");
                            foreach (var test in result)
                            {
                                //format json form of container
                                dynamic finalTest = "[\n" + test + "\n]";
                                dynamic fileJsonObj = JsonConvert.DeserializeObject<object>(finalTest);

                                if (fileJsonObj != null)
                                {
                                    
                                    foreach (object entry in fileJsonObj)
                                    {
                                        try
                                        {
                                            var keyValue = GetPropValue(entry, container.PartitionKeyPath);
                                            dynamic partitionKeyValue = Convert.ToString(keyValue); //change back to var

                                            int partKeyAsInt;
                                            bool isParsable = Int32.TryParse(partitionKeyValue, out partKeyAsInt);
                                            if (isParsable) //if partitionKey is supposed to be an int, it is converted to an int
                                            {
                                                partitionKeyValue = partKeyAsInt;
                                            }

                                            if (keyValue != null)
                                            {
                                                //con.CreateItemAsync(entry, new PartitionKey(partitionKeyValue)).Wait();
                                                destinationContainer.UpsertItemAsync(entry, new PartitionKey(partitionKeyValue)).Wait(); //this will replace existing item if id matches
                                            }
                                            else //IF PARTITION KEY IS NULL, DO THIS
                                            {
                                                Console.WriteLine("   - NULL PARTITION KEY FOUND");
                                                var newObj = new JArray();
                                                var newEnt = new JObject();
                                                //have to make this obj into a Jobj. get all entities in obj
                                                foreach (var item in (dynamic)entry)
                                                {
                                                    newEnt.Add(item.Name, item.Value);
                                                }

                                                var newName = container.PartitionKeyPath.Replace('/', ' ').Trim();

                                                if (isParsable)
                                                {
                                                    newEnt.Add(newName, 0);
                                                }
                                                else
                                                {
                                                    newEnt.Add(newName, "null");
                                                }

                                                newObj.Add(newEnt);

                                                object finalObj = newEnt;

                                                dynamic finalKeyValue = GetPropValue(finalObj, container.PartitionKeyPath);

                                                var response = destinationContainer.UpsertItemAsync(finalObj, new PartitionKey(finalKeyValue)).GetAwaiter().GetResult();
                                            }
                                        }
                                        catch (Exception ex)
                                        {
                                            Console.WriteLine($"Error While creating items: {ex.Message}");
                                        }



                                    }
                                }
                                else
                                {
                                    appInsightLog($"No information for {container.Id}");
                                }
                                break;
                            }
                        }
                        else
                        {
                            appInsightLog($"  - No Items to Migrate...");
                        }
                        #endregion

                        Console.ForegroundColor = ConsoleColor.DarkGreen;
                        appInsightLog("          - Done!");
                        Console.ResetColor();
                        //end of creating process for new container
                    }
                    #endregion
                    //end of data migration if statement

                }


            }
            catch (Exception ex)
            {
                appInsightLog(ex.Message);
                telemetryClient.TrackException(ex);
                telemetryClient.StopOperation(operation);
            }
        }

        public void TableConnections(InputModel input)
        {
            try
            {
                appInsightLog("Creating Connections to Table for URL fetch...");
                CloudStorageAccount account = CloudStorageAccount.Parse(_tableConnectionString); //table connection string is a config/local variable.
                CloudTableClient client = account.CreateCloudTableClient(new TableClientConfiguration());
                CloudTable table = client.GetTableReference(input.PodName + "FunctionUrls");
                if (!table.Exists())
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    appInsightLog("ERROR: Target table storage does not exists. Check if table name is correct.");
                    Console.ResetColor();
                    throw new Exception("ERROR: Target table storage does not exists. Check if table name is correct.");
                }
                appInsightLog(" - Conncetion Made.");

                tableEntities = new List<dynamic>();

                TableQuery query = new TableQuery(); //responsible for fetching all data in table
                foreach (dynamic entity in table.ExecuteQuery(query))
                {
                    //table items stored in list. Each item has 4 properties, we must use
                    //these properties to compare and replace function URLs
                    tableEntities.Add(entity);

                }
            }
            catch (Exception ex)
            {
                if (telemetryClient != null)
                {
                    telemetryClient.TrackException(ex);
                    telemetryClient.StopOperation(operation);
                }
            }


        }
        public static object GetPropValue(object src, string propName)
        {
            propName = propName.Replace('/', ' ').Trim(); //takes partition key of each container and removes the '/' character that is included within the JSON
            var obj = JObject.Parse(src.ToString()); //'.Parse' loads a JObject from a string that contains JSON 
            var objectValue = obj.Properties().Where(x => x.Name == propName).FirstOrDefault();
            if (objectValue != null)
            {
                return ((JValue)(obj.Properties().Where(x => x.Name == propName).FirstOrDefault().Value)).Value;
            }

            return null;
        }
        public async Task<List<string>> Query(Container container)
        {
            try
            {

                QueryDefinition query;

                if (container.Id == "cache")
                {
                    query = new QueryDefinition("SELECT * FROM c WHERE c.entity_type = 'company'"); //creates query
                }
                else
                {
                    query = new QueryDefinition("SELECT * FROM c"); //creates query
                }

                //QueryDefinition query = new QueryDefinition("SELECT * FROM c"); //creates query
                //appInsightLog($"Query Made: {query.QueryText}");

                List<object> list = new List<object>(); //creates list that will store all responses

                using (FeedIterator<object> resultSetIterator = container.GetItemQueryIterator<object>(queryDefinition: query)) //uses query to fetch items in DB
                {
                    while (resultSetIterator.HasMoreResults) //keep looping while there are remaining results
                    {
                        //Stream iterator returns response with status code
                        FeedResponse<object> response = await resultSetIterator.ReadNextAsync(); //reads result

                        appInsightLog($"\nNumber of Entities: {response.LongCount()} "); //displays amount of results in response


                        list.AddRange(response); //adds element to list, keeps looping until all results are in



                    }

                }



                List<string> result = new List<string>();
                result = jsonConverter(list); //funciton to create json files out of response
                return result;
            }
            catch (Exception ex)
            {
                if (telemetryClient != null)
                {
                    telemetryClient.TrackException(ex);
                    telemetryClient.StopOperation(operation);
                }
                throw;
            }

        }
        public static List<string> jsonConverter(List<object> list)
        {
            List<string> placeHolder = new List<string>();
            List<string> finalList = new List<string>();

            foreach (object item in list)
            {
                var result = JsonConvert.SerializeObject(item, Formatting.Indented); //converts list into json and formats it to be presentable
                placeHolder.Add(result);
            }

            var test2 = string.Join(",\n", placeHolder);
            finalList.Add(test2);

            return finalList;


        }
        public CosmosDbClients DbConnections(InputModel inputModel)
        {

            #region Source Connections
            var builder = new DbConnectionStringBuilder { ConnectionString = inputModel.sourceConnectionString };

            dynamic sourceKey;
            dynamic sourceUrl;
            builder.TryGetValue("AccountKey", out sourceKey);
            builder.TryGetValue("AccountEndpoint", out sourceUrl);

            var sourceClient = new CosmosClient(sourceUrl, sourceKey);

            #endregion

            #region Destination Connections

            var builder2 = new DbConnectionStringBuilder { ConnectionString = inputModel.destinationConnectionString };
            dynamic destinationKey;
            dynamic destinationUrl;
            builder2.TryGetValue("AccountKey", out destinationKey);
            builder2.TryGetValue("AccountEndpoint", out destinationUrl);


            var destinationClient = new CosmosClient(destinationUrl, destinationKey);
            #endregion

            var config = new CosmosDbClients()
            {
                SourceClient = sourceClient,
                DestinationClient = destinationClient,
            };

            return config;

        }
        public void ContainerCreator(Database database, string ConName, string key)
        {
            try
            {
                ContainerProperties prop = new ContainerProperties()
                {
                    Id = ConName,
                    PartitionKeyPath = key
                };
                Container container = database.CreateContainerIfNotExistsAsync(prop).Result;
                appInsightLog(" - Container Created");
            }
            catch (Exception ex)
            {
                if (telemetryClient != null)
                {
                    telemetryClient.TrackException(ex);
                    telemetryClient.StopOperation(operation);
                }
            }

        }
        public void appInsightLog(string msg)
        {
            telemetryClient.TrackTrace("db-MigrationURLSwap: " + msg);
            telemetryClient.Flush();
            Console.WriteLine(msg);
        }

    }

}
