using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;

namespace kaiam.MongoSync.Sync.Models
{
    public class TestData
    {
        [BsonId]
        public String id { get; set; }
    }

}
