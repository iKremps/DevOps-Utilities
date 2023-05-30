using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.ApplicationInsights;
using VSI.CloudPlatform.Core.Functions;
using VSI.CloudPlatform.Core.Telemetry;
using System.Data.Common;
using Microsoft.Azure.Cosmos;
using Azure.Storage.Blobs;
using SetupCosmosDB.DtoModels;
using System.Linq;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using Microsoft.Extensions.Configuration;
using System.Configuration;
using System.Runtime.CompilerServices;
using VSI.CloudPlaform.Core.Db;
using VSI.CloudPlatform.Core.Db;
using Quartz.Simpl;

namespace DBMigration
{

    public class DBMigration
    {
        private readonly string _key;
        private readonly bool _excludeDependency;
        private readonly BlobServiceClient _blobServiceClient;
        private readonly IConfiguration _config;
        public DBMigration(IConfiguration configuration)
        {
            _config = configuration;
            _key = configuration.GetValue<string>("APPINSIGHTS_INSTRUMENTATIONKEY");
            _excludeDependency = FunctionUtilities.GetBoolValue(configuration.GetValue<string>("ExcludeDependency"), false);
        }

        [Timeout("05:00:00")]
        [FunctionName("DBMigration")]
        public async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            try
            {

                var requestBody = await req.ReadAsStringAsync();
                var migrationInput = JsonConvert.DeserializeObject<MigrationModel>(requestBody);

                telemetryClient = TelemetryFactory.GetInstance("DBMigration", _key, _excludeDependency); //creates an instance of a telemetry and connects it to the function given its name/key
                operation = telemetryClient.StartOperation<RequestTelemetry>("DBMigration", Guid.NewGuid().ToString());

                appInsightLog("Creating Connections");
                var cosmosDbClient = DbConnections(migrationInput);
                appInsightLog("Migration started....");
                MigrateDatabase(migrationInput, cosmosDbClient);
                appInsightLog("Migration completed....");

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

        #region Telemetry Objects/Variables

        IOperationHolder<RequestTelemetry> operation = null;
        TelemetryClient telemetryClient = null;
        #endregion 

        public void MigrateDatabase(MigrationModel input,CosmosDbClient dbClient)
        {
            try
            {
                #region Get all Containers from Source
                Database sourceDB = dbClient.SourceClient.GetDatabase(input.SourceDBName);

                FeedIterator<ContainerProperties> iterator = sourceDB.GetContainerQueryIterator<ContainerProperties>();
                FeedResponse<ContainerProperties> containers = iterator.ReadNextAsync().ConfigureAwait(false).GetAwaiter().GetResult();
                #endregion

                #region Create Destination DB
                Database destinationDB = dbClient.DestinationClient.GetDatabase(input.DestinationDBName.ToString());
                var destinationDbResponse = dbClient.DestinationClient.CreateDatabaseIfNotExistsAsync(input.DestinationDBName.ToString()).GetAwaiter().GetResult();

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
                string[] ContainersForDataMigration = input.ContainersForDataMigration;

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
                        result = Query(con,input.Process_flow_adaptersOnly).GetAwaiter().GetResult();

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


        /// <summary>
        /// Gets the partitionkey for each object in a container
        /// </summary>
        /// <param name="src"></param>
        /// <param name="propName"></param>
        /// <returns></returns>
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

        public async Task<List<string>> Query(Container container,bool adaptersOnly)
        {
            try
            {

                QueryDefinition query;

                if (container.Id == "process_flow")
                {
                    if (adaptersOnly)
                    {
                        query = new QueryDefinition("SELECT * FROM c WHERE c.entity_name = 'adapters'"); //creates query
                    }
                    else
                    {
                        query = new QueryDefinition("SELECT * FROM c"); //creates query
                    }

                }
                else if (container.Id == "cache")
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

        /// <summary>
        /// Converts all entities in list of Query responses to JSON format
        /// </summary>
        /// <param name="list"></param>
        /// <returns></returns>
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

        /// <summary>
        /// Creates a container for a DB object. given its name and a key for the partition key
        /// </summary>
        /// <param name="ConName"></param>
        /// <param name="key"></param>
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
            telemetryClient.TrackTrace("db-migrate: " + msg);
            telemetryClient.Flush();
            Console.WriteLine(msg);
        }

        public CosmosDbClient DbConnections(MigrationModel migrationModel)
        {

            #region Source Connections
            var builder = new DbConnectionStringBuilder { ConnectionString = migrationModel.SourceDBConnectionString };

            dynamic sourceKey;
            dynamic sourceUrl;
            builder.TryGetValue("AccountKey", out sourceKey);
            builder.TryGetValue("AccountEndpoint", out sourceUrl);

            var sourceClient = new CosmosClient(sourceUrl, sourceKey);

            #endregion

            #region Destination Connections

            var builder2 = new DbConnectionStringBuilder { ConnectionString = migrationModel.DestinationDBConnectionString };
            dynamic destinationKey;
            dynamic destinationUrl;
            builder2.TryGetValue("AccountKey", out destinationKey);
            builder2.TryGetValue("AccountEndpoint", out destinationUrl);

             
            var destinationClient = new CosmosClient(destinationUrl, destinationKey);
            #endregion

            var config = new CosmosDbClient()
            {
                SourceClient = sourceClient,
                DestinationClient = destinationClient,
            };

            return config;

        }

    }

}
