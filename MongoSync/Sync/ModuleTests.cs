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

    class ModuleTests: SyncBase
    {
        private readonly string[] excludeDataFields = new string[] { "_entityWrapper", "Id", "TimeStamp", "ModuleTest", "KAIAM.DataAccess.ModuleTest", "ModuleTestId" };

        public override int processTestData(DateTime start, DateTime end)
        {
            int count = 0;
            using (KAIAMDataTestContainer db = new KAIAMDataTestContainer(Program.connString))
            {
                // Retrieve data updated after timestamp module VCC test
                var tcdQ4 = from tcd in db.ModuleVccTests
                            where tcd.TimeStamp >= start && tcd.TimeStamp < end
                            select tcd;
                foreach (ModuleVccTest tcd in tcdQ4)
                {
                    BsonDocument doc = buildVccTest(tcd);
                    saveDoc(doc);
                    Application.DoEvents();
                    count++;
                }

                // Retrieve data updated after timestamp module power consumption test
                var tcdQ = from tcd in db.ModulePowerConsumptionTests
                           where tcd.TimeStamp >= start && tcd.TimeStamp < end
                           select tcd;
                foreach (ModulePowerConsumptionTest tcd in tcdQ)
                {
                    BsonDocument doc = buildPowerConsumption(tcd);
                    saveDoc(doc);
                    Application.DoEvents();
                    count++;
                }

                // Retrieve data updated after timestamp module mSA temperature test
                var tcdQ2 = from tcd in db.ModuleMSATemperatureTests
                           where tcd.TimeStamp >= start && tcd.TimeStamp < end
                           select tcd;
                foreach (ModuleMSATemperatureTest tcd in tcdQ2)
                {
                    BsonDocument doc = buildMSATemperature(tcd);
                    saveDoc(doc);
                    Application.DoEvents();
                    count++;
                }
            }
            return count;
        }

        private BsonDocument buildVccTest(ModuleVccTest tcd)
        {
            Measurement measurement = null;

            // Initialize object that will be stored under 'device' in mongo document
            BsonDocument nestedDevice = null;
            if (tcd.ModuleTest != null)
            {
                try
                {
                    measurement = tcd.ModuleTest.Measurement;
                }
                catch (Exception exc)
                {
                    Program.log("ERROR MODULE TEST VCC: " + tcd.Id + " - " + exc.Message);
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
            nestedMeta.Add("SetTemperature_C", nestedData["SetTemperature_C"]);
            nestedData.Remove("SetTemperature_C");
            nestedMeta.Add("SetVoltage", nestedData["VccSetPoint"]);
            nestedData.Remove("VccSetPoint");

            //System.Diagnostics.Trace.WriteLine("D " + DateTime.Now.ToString("MM/dd/yyyy hh:mm:ss.fff tt"));

            // Initialize object in root of the document
            BsonDocument rootDoc = new BsonDocument {
                 { "_id",  tcd.Id.ToString()},
                 { "mid", measurement.Id.ToString()  },
                 { "timestamp", tcd.TimeStamp},
                 { "type", domain() },
                 { "subtype",  "vcctest" },
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

        private BsonDocument buildPowerConsumption(ModulePowerConsumptionTest tcd)
        {
            Measurement measurement = null;

            // Initialize object that will be stored under 'device' in mongo document
            BsonDocument nestedDevice = null;
            if (tcd.ModuleTest != null)
            {
                try
                {
                    measurement = tcd.ModuleTest.Measurement;
                }
                catch (Exception exc)
                {
                    Program.log("ERROR ModuleTest Power consumption: " + tcd.Id + " - " + exc.Message);
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
            nestedMeta.Add("SetTemperature_C", nestedData["SetTemperature_C"]);
            nestedData.Remove("SetTemperature_C");
            nestedMeta.Add("SetVoltage", nestedData["VccSetPoint"]);
            nestedData.Remove("VccSetPoint");

            //System.Diagnostics.Trace.WriteLine("D " + DateTime.Now.ToString("MM/dd/yyyy hh:mm:ss.fff tt"));

            // Initialize object in root of the document
            BsonDocument rootDoc = new BsonDocument {
                 { "_id",  tcd.Id.ToString()},
                 { "mid", measurement.Id.ToString()  },
                 { "timestamp", tcd.TimeStamp},
                 { "type", domain() },
                 { "subtype",  "power" },
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

        private BsonDocument buildMSATemperature(ModuleMSATemperatureTest tcd)
        {
            Measurement measurement = null;

            // Initialize object that will be stored under 'device' in mongo document
            BsonDocument nestedDevice = null;
            if (tcd.ModuleTest != null)
            {
                try
                {
                    measurement = tcd.ModuleTest.Measurement;
                }
                catch (Exception exc)
                {
                    Program.log("ERROR Module MSA test: " + tcd.Id + " - " + exc.Message);
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
            nestedMeta.Add("SetTemperature_C", nestedData["SetTemperature_C"]);
            nestedData.Remove("SetTemperature_C");
            nestedMeta.Add("SetVoltage", nestedData["VccSetPoint"]);
            nestedData.Remove("VccSetPoint");

            //System.Diagnostics.Trace.WriteLine("D " + DateTime.Now.ToString("MM/dd/yyyy hh:mm:ss.fff tt"));

            // Initialize object in root of the document
            BsonDocument rootDoc = new BsonDocument {
                 { "_id",  tcd.Id.ToString()},
                 { "mid", measurement.Id.ToString()},
                 { "timestamp", tcd.TimeStamp},
                 { "type", domain() },
                 { "subtype",  "msatemp" },
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
