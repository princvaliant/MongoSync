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
using FastMember;

namespace kaiam.MongoSync.Sync
{

    class TxTests : SyncBase
    {
        private readonly string[] excludeDataFields = new string[] { "_entityWrapper", "Id", "TimeStamp", "TxTestData", "KAIAM.DataAccess.TxTestData", "TxTestDataId", "Result" };

        public override int processTestData(DateTime start, DateTime end)
        {
            int count = 0;
            using (KAIAMDataTestContainer db = new KAIAMDataTestContainer(Program.connString))
            {
                // Retrieve data updated after timestamp
                var tcdQ = from tcd in db.TxTestChannelDatas
                           where tcd.TimeStamp >= start && tcd.TimeStamp < end
                           select tcd;
                foreach (TxTestChannelData tcd in tcdQ)
                {
                    BsonDocument doc = buildMongoObject(tcd);
                    saveDoc(doc);
                    Application.DoEvents();
                    count++;
                }
            }
            return count;
        }

        private BsonDocument buildMongoObject(TxTestChannelData tcd)
        {
            // Initialize all related objects from SQL
            Measurement measurement = null;

            // Initialize device object
            BsonDocument nestedDevice = null;

            // System.Diagnostics.Trace.WriteLine("A " + DateTime.Now.ToString("MM/dd/yyyy hh:mm:ss.fff tt"));

            if (tcd.TxTestData != null && tcd.TxTestData.TxTest != null)
            {
                try
                {
                    measurement = tcd.TxTestData.TxTest.Measurement;
                }
                catch (Exception exc)
                {
                    Program.log("ERROR TXTESTS: " + tcd.Id + " - " + exc.Message);
                }
                finally
                {
                    nestedDevice = SyncManager.Instance.getNestedDevice(measurement);
                }
            }
            if (nestedDevice == null)
                return null;

            // Initialize object that will be stored under 'meta' in mongo document
            BsonDocument nestedMeta = SyncManager.Instance.getNestedMeta(measurement);
            if (nestedMeta == null)
                return null;

            // Initialize object that will be stored under 'data' in mongo document
            BsonDocument nestedData = SyncManager.Instance.getNestedData(tcd, excludeDataFields);

            nestedMeta.Add("SetTemperature_C", tcd.TxTestData.SetTemperature_C);
            nestedMeta.Add("Channel", nestedData["Channel"]);
            nestedData.Remove("Channel");
            nestedMeta.Add("SetVoltage", nestedData["SetVoltage"]);
            nestedData.Remove("SetVoltage");

            //System.Diagnostics.Trace.WriteLine("D " + DateTime.Now.ToString("MM/dd/yyyy hh:mm:ss.fff tt"));

            // Initialize object in root of the document
            BsonDocument rootDoc = new BsonDocument {
                 { "_id",  tcd.Id.ToString()},
                 { "mid", measurement.Id.ToString()  },
                 { "timestamp", tcd.TimeStamp},
                 { "type", domain() },
                 { "subtype", "channeldata" },
                 { "status", getStatus(measurement.Result)  }
            };
            if (tcd.TxTestData.Passed)
            {
                rootDoc.Add("result", OK_STRING);
                rootDoc.Add("downstatus", "P");
            } else
            {
                rootDoc.Add("result", ERROR_STRING);
                rootDoc.Add("downstatus", "F");
            }
           
            rootDoc.Add("device", nestedDevice);
            rootDoc.Add("meta", nestedMeta);
            rootDoc.Add("data", nestedData);

            return rootDoc;
        }
    }
}
