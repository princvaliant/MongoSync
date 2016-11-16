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
            var client = new MongoClient("mongodb://Miljenko:ka1amc00lc10ud@cloud.kaiamcorp.com/KaiamApp?ssl=true&sslVerifyCertificate=false");
            var database = client.GetServer().GetDatabase(ConfigurationManager.AppSettings["mongoDB"]);
            Collection = database.GetCollection<T>(typeof(T).Name.ToLower());
;        }
    }

    public class MongoViewHelper
    {
        public MongoCollection Collection { get; private set; }

        public MongoViewHelper(String viewName)
        {
            var client = new MongoClient("mongodb://Miljenko:ka1amc00lc10ud@cloud.kaiamcorp.com/KaiamApp?ssl=true&sslVerifyCertificate=false");
            var database = client.GetServer().GetDatabase(ConfigurationManager.AppSettings["mongoDB"]);
            Collection = database.GetCollection(viewName);
        }
    }
}
