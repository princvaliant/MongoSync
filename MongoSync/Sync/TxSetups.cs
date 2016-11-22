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

    class TxSetups : SyncBase
    {
        private readonly string[] excludeDataFields = new string[] { "_entityWrapper", "Id", "TimeStamp", "TxSetup", "KAIAM.DataAccess.TxSetup", "TxSetupId", "TxSetupData", "TxSetupDataId", "KAIAM.DataAccess.TxSetupData" };

        public override int processTestData(DateTime start, DateTime end)
        {
            int count = 0;
            using (KAIAMDataTestContainer db = new KAIAMDataTestContainer(Program.connString))
            {
                // Retrieve data updated after timestamp  for channel data
                var tcdQ4 = from tcd in db.TxSetupChannelDatas
                            where tcd.TimeStamp >= start && tcd.TimeStamp < end
                            select tcd;
                foreach (TxSetupChannelData tcd in tcdQ4)
                {
                    BsonDocument doc = buildSetupChannelData(tcd);
                    saveDoc(doc);
                    Application.DoEvents();
                    count++;
                }

                // Retrieve data updated after timestamp  for matrix
                var tcdQ = from tcd in db.TxMatrices
                           where tcd.TimeStamp >= start && tcd.TimeStamp < end
                           select tcd;
                foreach (TxMatrix tcd in tcdQ)
                {
                    BsonDocument doc = buildMatrix(tcd);
                    saveDoc(doc);
                    Application.DoEvents();
                    count++;
                }
            }
            return count;
        }

        private BsonDocument buildSetupChannelData(TxSetupChannelData tcd)
        {
            Measurement measurement = null;

            // Initialize object that will be stored under 'device' in mongo document
            BsonDocument nestedDevice = null;
            if (tcd.TxSetupData != null)
            {
                try
                {
                    measurement = tcd.TxSetupData.TxSetup.Measurement;
                }
                catch (Exception exc)
                {
                    Program.log("ERROR TX SETUP CHANNEL DATA: " + tcd.Id + " - " + exc.Message);
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
            nestedMeta.Add("SetTemperature_C", nestedData["SetTemperature_in_C"]);
            nestedData.Remove("SetTemperature_in_C");
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
                 { "subtype",  "channeldata" },
                 { "status", getStatus(measurement.Result)  }
            };
            if (tcd.TxSetupData.Passed)
            {
                rootDoc.Add("result", OK_STRING);
                rootDoc.Add("downstatus", "P");
            }
            else
            {
                rootDoc.Add("result", ERROR_STRING);
                rootDoc.Add("downstatus", "F");
            }
            nestedData.Remove("Result");

            rootDoc.Add("device", nestedDevice);
            rootDoc.Add("meta", nestedMeta);
            rootDoc.Add("data", nestedData);

            return rootDoc;
        }

        private BsonDocument buildMatrix(TxMatrix tcd)
        {
            Measurement measurement = null;

            // Initialize object that will be stored under 'device' in mongo document
            BsonDocument nestedDevice = null;
            if (tcd.TxSetup != null)
            {
                try
                {
                    measurement = tcd.TxSetup.Measurement;
                }
                catch (Exception exc)
                {
                    Program.log("ERROR TxSetup Matrix: " + tcd.Id + " - " + exc.Message);
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
            nestedMeta.Add("SetTemperature_C", "");
            nestedMeta.Add("Channel", nestedData["Channel"]);
            nestedData.Remove("Channel");
            nestedMeta.Add("SetVoltage", "");

            //System.Diagnostics.Trace.WriteLine("D " + DateTime.Now.ToString("MM/dd/yyyy hh:mm:ss.fff tt"));

            // Initialize object in root of the document
            BsonDocument rootDoc = new BsonDocument {
                 { "_id",  tcd.Id.ToString()},
                 { "mid", measurement.Id.ToString()},
                 { "timestamp", tcd.TimeStamp},
                 { "type", domain() },
                 { "subtype",  "matrix" },
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
