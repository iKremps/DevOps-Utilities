using Microsoft.Azure.Cosmos;

namespace DBMigrationDataChange
{
    public class CosmosDbClients
    {
        public CosmosClient DestinationClient { get; set; }
        public CosmosClient SourceClient { get; set; }
    }




}
