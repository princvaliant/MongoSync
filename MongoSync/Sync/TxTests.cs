using kaiam.MongoSync.Sync.Models;
using KAIAM.DataAccess;
using MongoDB.Bson;
using System;
using System.Data.SqlTypes;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.Entity.Infrastructure;
using System.Data.Entity.SqlServer;
using System.Data.Entity.Validation;
using System.Linq;
using System.Linq.Expressions;
using System.Windows.Forms;

namespace kaiam.MongoSync.Sync
{

    class TxTests : SyncBase
    {
        private readonly string[] excludeData = new string[] { "Id", "TimeStamp", "TxTestData", "TxTestDataId","Result" };
        private readonly string[] excludeStatus = new string[] { "Id", "TxTestId", "TxTestChannelDatas", "TxTest", "Result" };

        public TxTests()
        {

        }
        public override void copyDataToMongo()
        {
            int count = 0;

            DateTime start = getSyncStart();
            while (start < DateTime.Now)
            {
                DateTime end = start.AddHours(BATCH_PERIOD_IN_HOURS);

                MongoHelper<TestData> mongoDoc = new MongoHelper<TestData>();

                using (KAIAMDataTestContainer db = new KAIAMDataTestContainer(Program.connString))
                {
                    // Retrieve data updated after timestamp
                    var tcdQ = from tcd in db.TxTestChannelDatas
                               where tcd.TimeStamp >= start && tcd.TimeStamp < end
                               select tcd;
                    foreach (TxTestChannelData tcd in tcdQ)
                    {
                        BsonDocument doc = buildMongoObject(tcd);
                        if (doc != null)
                            mongoDoc.Collection.Save(doc);
                        Application.DoEvents();
                        count++;
                    }
                }
                Program.log(domain() + " synced: " + start.ToString() + "-" + end.ToString() + " total:" + count.ToString());

                start = end;
            }
        }

        private BsonDocument buildMongoObject(TxTestChannelData tcd)
        {
            // Initialize all related objects from SQL
            TxTestData txTestData = tcd.TxTestData;
            Measurement measurement = null;
            Device device = null;
            if (tcd.TxTestData.TxTest != null)
            {
                measurement = tcd.TxTestData.TxTest.Measurement;
                if (measurement.DeviceId.HasValue)
                {
                    device = SyncManager.Instance.getDevice(measurement.DeviceId.Value);
                }
            }
            if (device == null)
                return null;

            // Initialize object that will be stored under 'data' in mongo document
            BsonDocument nestedData = new BsonDocument { };
            Dictionary<string, object> dictData = new Dictionary<string, object>();
            foreach (var prop in tcd.GetType().GetProperties())
            {
                if (!excludeData.Contains(prop.Name))
                    dictData.Add(prop.Name, prop.GetValue(tcd, null));
            }
            nestedData.AddRange(dictData);

            // Initialize object that will be stored under 'status' in mongo document
            BsonDocument nestedStatus = new BsonDocument { };
            Dictionary<string, object> dictStatus = new Dictionary<string, object>();
            foreach (var prop in txTestData.GetType().GetProperties())
            {
                if (!excludeStatus.Contains(prop.Name))
                    dictStatus.Add(prop.Name, prop.GetValue(txTestData, null));
            }
            dictStatus.Add("StartDateTime", measurement.StartDateTime);
            dictStatus.Add("EndDateTime", measurement.EndDateTime);
            dictStatus.Add("User", measurement.User.Login);
            nestedStatus.AddRange(dictStatus);

            // Initialize device object
            BsonDocument nestedDevice = new BsonDocument {
                 { "SerialNumber", device.SerialNumber }
            };

            // Initialize object in root of the document
            BsonDocument rootDoc = new BsonDocument {
                 { "_id",  tcd.Id.ToString()},
                 { "timestamp", tcd.TimeStamp},
                 { "type", domain() }
            };
            rootDoc.Add("device", nestedDevice);
            rootDoc.Add("status", nestedStatus);
            rootDoc.Add("data", nestedData);

            return rootDoc;
        }
    }
}
