namespace DBMigrationDataChange
{
    public class InputModel
    {
        public string Company_id { get; set; }
        public string Company_code { get; set; }
        public string PodName { get; set; }
        public string newDBName { get; set; }
        public string EnvName { get; set; }
        public string destinationConnectionString { get; set; }
        public string sourceConnectionString { get; set; }
        public string sourceDBName { get; set; }
        public string destinationDBName { get; set; }
        public string[] containersForMigration { get; set; }
    }
}
