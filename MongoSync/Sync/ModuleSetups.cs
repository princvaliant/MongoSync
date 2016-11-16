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

    class ModuleSetups : SyncBase
    {
        private readonly string[] excludeDataFields = new string[] { "_entityWrapper", "Id", "TimeStamp", "ModuleSetup", "KAIAM.DataAccess.ModuleSetup", "ModuleSetupId" };

        public override int processTestData(DateTime start, DateTime end)
        {
            int count = 0;
            using (KAIAMDataTestContainer db = new KAIAMDataTestContainer(Program.connString))
            {
                // Retrieve data updated after timestamp module VCC setup
                var tcdQ4 = from tcd in db.VCCSetups
                            where tcd.TimeStamp >= start && tcd.TimeStamp < end
                            select tcd;
                foreach (VCCSetup tcd in tcdQ4)
                {
                    BsonDocument doc = buildVccSetup(tcd);
                    saveDoc(doc);
                    Application.DoEvents();
                    count++;
                }

                // Retrieve data updated after timestamp module power consumption test
                var tcdQ = from tcd in db.TempSetups
                           where tcd.TimeStamp >= start && tcd.TimeStamp < end
                           select tcd;
                foreach (TempSetup tcd in tcdQ)
                {
                    BsonDocument doc = buildTempSetup(tcd);
                    saveDoc(doc);
                    Application.DoEvents();
                    count++;
                }
            }
            return count;
        }

        private BsonDocument buildVccSetup(VCCSetup tcd)
        {
            Measurement measurement = null;

            // Initialize object that will be stored under 'device' in mongo document
            BsonDocument nestedDevice = null;
            if (tcd.ModuleSetup != null)
            {
                try
                {
                    measurement = tcd.ModuleSetup.Measurement;
                }
                catch (Exception exc)
                {
                    Program.log("ERROR MODULE SETUP VCC: " + tcd.Id + " - " + exc.Message);
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
                 { "subtype",  "vccsetup" },
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

        private BsonDocument buildTempSetup(TempSetup tcd)
        {
            Measurement measurement = null;

            // Initialize object that will be stored under 'device' in mongo document
            BsonDocument nestedDevice = null;
            if (tcd.ModuleSetup != null)
            {
                try
                {
                    measurement = tcd.ModuleSetup.Measurement;
                }
                catch (Exception exc)
                {
                    Program.log("ERROR ModuleSetup temperature: " + tcd.Id + " - " + exc.Message);
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
                 { "subtype",  "tempsetup" },
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
