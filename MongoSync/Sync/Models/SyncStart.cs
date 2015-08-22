using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;

namespace kaiam.MongoSync.Sync.Models
{
    public class SyncStart
    {
        [BsonId]
        public ObjectId id { get; set; }

        // domain string
        public string domain { get; set; }

        // Timestamp defining the start timestamp for sync 
        public DateTime start { get; set; }
    }

}
