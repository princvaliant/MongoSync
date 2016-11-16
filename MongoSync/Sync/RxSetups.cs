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

    class RxSetups : SyncBase
    {
        private readonly string[] excludeDataFields = new string[] { "_entityWrapper", "Id", "TimeStamp", "RxSetup", "KAIAM.DataAccess.RxSetup", "RxSetupId" };

        public override int processTestData(DateTime start, DateTime end)
        {
            int count = 0;
            using (KAIAMDataTestContainer db = new KAIAMDataTestContainer(Program.connString))
            {
                // Retrieve data updated after timestamp  for losssetup
                var tcdQ4 = from tcd in db.LOSSetups
                            where tcd.TimeStamp >= start && tcd.TimeStamp < end
                            select tcd;
                foreach (LOSSetup tcd in tcdQ4)
                {
                    BsonDocument doc = buildLossSetup(tcd);
                    saveDoc(doc);
                    Application.DoEvents();
                    count++;
                }

                // Retrieve data updated after timestamp  for sensitivities
                var tcdQ = from tcd in db.RSSISetups
                           where tcd.TimeStamp >= start && tcd.TimeStamp < end
                           select tcd;
                foreach (RSSISetup tcd in tcdQ)
                {
                    BsonDocument doc = buildRssiSetup(tcd);
                    saveDoc(doc);
                    Application.DoEvents();
                    count++;
                }
            }
            return count;
        }

        private BsonDocument buildLossSetup(LOSSetup tcd)
        {
            Measurement measurement = null;

            // Initialize object that will be stored under 'device' in mongo document
            BsonDocument nestedDevice = null;
            if (tcd.RxSetup != null)
            {
                try
                {
                    measurement = tcd.RxSetup.Measurement;
                }
                catch (Exception exc)
                {
                    Program.log("ERROR LOS SETUP: " + tcd.Id + " - " + exc.Message);
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

            // Custom code to move some set fields to meta object
            nestedMeta.Add("SetTemperature_C", nestedData["SetTemperature"]);
            nestedData.Remove("SetTemperature");
            nestedMeta.Add("SetVoltage", nestedData["SetVoltage"]);
            nestedData.Remove("SetVoltage");

            //System.Diagnostics.Trace.WriteLine("D " + DateTime.Now.ToString("MM/dd/yyyy hh:mm:ss.fff tt"));

            // Initialize object in root of the document
            BsonDocument rootDoc = new BsonDocument {
                 { "_id",  tcd.Id.ToString()},
                 { "mid", measurement.Id.ToString()  },
                 { "timestamp", tcd.TimeStamp},
                 { "type", domain() },
                 { "subtype",  "losssetup" },
                 { "result", getResult( nestedData["Result"].ToString()) },
                 { "downstatus", getDownStatus( nestedData["Result"].ToString()) },
                 { "status", getStatus(measurement.Result) }
            };
            nestedData.Remove("Result");

            rootDoc.Add("device", nestedDevice);
            rootDoc.Add("meta", nestedMeta);
            rootDoc.Add("data", nestedData);

            return rootDoc;
        }

        private BsonDocument buildRssiSetup(RSSISetup tcd)
        {
            Measurement measurement = null;

            // Initialize object that will be stored under 'device' in mongo document
            BsonDocument nestedDevice = null;
            if (tcd.RxSetup != null)
            {
                try
                {
                    measurement = tcd.RxSetup.Measurement;
                }
                catch (Exception exc)
                {
                    Program.log("ERROR RssiSetup: " + tcd.Id + " - " + exc.Message);
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

            // Custom code to move some set fields to meta object
            nestedMeta.Add("SetTemperature_C", nestedData["SetTemperature"]);
            nestedData.Remove("SetTemperature");
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
                 { "subtype",  "rssisetup" },
                 { "result", getResult( nestedData["Result"].ToString()) },
                 { "downstatus", getDownStatus( nestedData["Result"].ToString()) },
                 { "status", getStatus(measurement.Result) }
            };
            nestedData.Remove("Result");

            rootDoc.Add("device", nestedDevice);
            rootDoc.Add("meta", nestedMeta);
            rootDoc.Add("data", nestedData);

            return rootDoc;
        }
    }
}
