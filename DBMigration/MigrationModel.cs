namespace DBMigration
{
    public class MigrationModel
    {
        public string SourceDBConnectionString { get; set; }
        public string SourceDBName { get; set; }
        public string DestinationDBConnectionString { get; set; }
        public string DestinationDBName { get; set; }
        public bool Process_flow_adaptersOnly { get; set; }
        public string[] ContainersForDataMigration { get; set; }
    }

}
