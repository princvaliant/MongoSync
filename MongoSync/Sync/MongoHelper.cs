using MongoDB.Driver;
using System;
using System.Configuration;

namespace kaiam.MongoSync.Sync
{
    public class MongoHelper<T> where T : class
    {
        public MongoCollection<T> Collection { get; private set; }

        public MongoHelper()
        {
            MongoServerSettings settings = new MongoServerSettings();
            settings.Server = new MongoServerAddress(
                ConfigurationManager.AppSettings["mongoConnection"], 
                Int32.Parse( ConfigurationManager.AppSettings["mongoPort"]));
            var client = new MongoServer(settings);
            var database = client.GetDatabase(ConfigurationManager.AppSettings["mongoDB"]);
            Collection = database.GetCollection<T>(typeof(T).Name.ToLower());
;        }
    }
}
