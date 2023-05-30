using Microsoft.Azure.Cosmos;

namespace DBMigration
{
    public class CosmosDbClient
    { 
        public CosmosClient DestinationClient { get; set; }
        public CosmosClient SourceClient { get; set; } 
    }

}
