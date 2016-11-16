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

    class RxTests : SyncBase
    {
        private readonly string[] excludeDataFields = new string[] { "_entityWrapper", "Id", "TimeStamp", "RxTest", "KAIAM.DataAccess.RxTest", "RxTestId",  "SensitivityDatas" };

        public override int processTestData(DateTime start, DateTime end)
        {
            int count = 0;
            using (KAIAMDataTestContainer db = new KAIAMDataTestContainer(Program.connString))
            {
                // Retrieve data updated after timestamp  for rxAmplitudeTest
                var tcdQ4 = from tcd in db.RxAmplitudeTests
                            where tcd.TimeStamp >= start && tcd.TimeStamp < end
                            select tcd;
                foreach (RxAmplitudeTest tcd in tcdQ4)
                {
                    BsonDocument doc = buildRxAmplitudeTest(tcd);
                    saveDoc(doc);
                    Application.DoEvents();
                    count++;
                }

                // Retrieve data updated after timestamp  for sensitivities
                var tcdQ = from tcd in db.Sensitivities
                           where tcd.TimeStamp >= start && tcd.TimeStamp < end
                           select tcd;
                foreach (Sensitivity tcd in tcdQ)
                {
                    BsonDocument doc = buildSensitivity(tcd);
                    saveDoc(doc);
                    Application.DoEvents();
                    count++;
                }

                // Retrieve data updated after timestamp  for rssitests
                var tcdQ2 = from tcd in db.RSSITests
                           where tcd.TimeStamp >= start && tcd.TimeStamp < end
                           select tcd;
                foreach (RSSITest tcd in tcdQ2)
                {
                    BsonDocument doc = buildRSSITest(tcd);
                    saveDoc(doc);
                    Application.DoEvents();
                    count++;
                }

                // Retrieve data updated after timestamp  for lostest
                var tcdQ3 = from tcd in db.LOSTests
                            where tcd.TimeStamp >= start && tcd.TimeStamp < end
                            select tcd;
                foreach (LOSTest tcd in tcdQ3)
                {
                    BsonDocument doc = buildLOSTest(tcd);
                    saveDoc(doc);
                    Application.DoEvents();
                    count++;
                }

               
            }
            return count;
        }

        private BsonDocument buildSensitivity(Sensitivity tcd)
        {
            Measurement measurement = null;

            // Initialize object that will be stored under 'device' in mongo document
            BsonDocument nestedDevice = null;
            if (tcd.RxTest != null)
            {
                try {
                    measurement = tcd.RxTest.Measurement;
                }  catch (Exception exc) {
                    Program.log("ERROR SENSITIVITY: " + tcd.Id + " - " + exc.Message);
                 } finally {
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
                 { "subtype",  "sensitivity" },
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

        private BsonDocument buildRSSITest(RSSITest tcd)
        {
            Measurement measurement = null;

            // Initialize object that will be stored under 'device' in mongo document
            BsonDocument nestedDevice = null;
            if (tcd.RxTest != null)
            {
                try
                {
                    measurement = tcd.RxTest.Measurement;
                }
                catch (Exception exc)
                {
                    Program.log("ERROR RSSITEST: " + tcd.Id + " - " + exc.Message);
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
                 { "subtype",  "rssitest" },
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

        private BsonDocument buildLOSTest(LOSTest tcd)
        {
            Measurement measurement = null;

            // Initialize object that will be stored under 'device' in mongo document
            BsonDocument nestedDevice = null;
            if (tcd.RxTest != null)
            {
                try
                {
                    measurement = tcd.RxTest.Measurement;
                }
                catch (Exception exc)
                {
                    Program.log("ERROR LOSTEST: " + tcd.Id + " - " + exc.Message);
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
                 { "subtype",  "lostest" },
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

        private BsonDocument buildRxAmplitudeTest(RxAmplitudeTest tcd)
        {
            Measurement measurement = null;

            // Initialize object that will be stored under 'device' in mongo document
            BsonDocument nestedDevice = null;
            if (tcd.RxTest != null)
            {
                try
                {
                    measurement = tcd.RxTest.Measurement;
                }
                catch (Exception exc)
                {
                    Program.log("ERROR RXAMPLITUDE: " + tcd.Id + " - " + exc.Message);
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
            String st = "";
            String[] racks = { "DL_ST", "QSFP-DEV-PC2", "Production-4"};
            if (racks.Contains(nestedMeta["Rack"].ToString()))
            {
                st = "rxfunctionality";
            }
            String[] racks2 = { "Rack_1", "Rack_3", "Rack_4", "Rack_5", "Rack_6", "Rack_7", "Rack_8", "Rack_9" };
            if (racks2.Contains(nestedMeta["Rack"].ToString()))
            {
                st = "rxamplitude";
            }

            if (st == "")
            {
                return null;
            }

            // Initialize object in root of the document
            BsonDocument rootDoc = new BsonDocument {
                 { "_id",  tcd.Id.ToString()},
                  { "mid", measurement.Id.ToString()  },
                 { "timestamp", tcd.TimeStamp},
                 { "type", domain() },
                 { "subtype",  st},
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
