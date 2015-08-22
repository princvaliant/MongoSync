using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;

namespace kaiam.MongoSync.Sync.Models
{
    public class SyncLog
    {
        [BsonId]
        public ObjectId id { get; set; }

        // Timestamp start 
        public DateTime df { get; set; }

        // Timestamp at increment of 12 hours from dt
        public DateTime dt { get; set; }

        // domain string
        public string domain { get; set; }

        // total synced if succesful
        public int total { get; set; }
        
        // Status 0 -> not synced, 1 -> succesfully synced, 2 -> error occured  
        public int status { get; set; }

        public IList<String> errors { get; set; }
    }

}
