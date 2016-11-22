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


    class Packout : SyncBase
    {
        private readonly string[] excludeDataFields = new string[] { "_entityWrapper", "Id", "Measurement", "TimeStamp", "Result" };


        public override int processTestData(DateTime start, DateTime end)
        {
            int count = 0;
            using (KAIAMDataTestContainer db = new KAIAMDataTestContainer(Program.connString))
            {
                // Retrieve data updated after timestamp
                var tcdQ = from tcd in db.Packouts
                           where tcd.TimeStamp >= start && tcd.TimeStamp < end
                           select tcd;
                foreach (KAIAM.DataAccess.Packout tcd in tcdQ)
                {
                    BsonDocument doc = buildMongoObject(tcd);
                    saveDoc(doc);
                    Application.DoEvents();
                    count++;
                }
            }
            return count;
        }

        private BsonDocument buildMongoObject(KAIAM.DataAccess.Packout tcd)
        {
            // Initialize all related objects from SQL
            Measurement measurement = null;

            // Initialize device object
            BsonDocument nestedDevice = null;
            try
            {
                if (tcd.Measurement != null)
                {
                    measurement = tcd.Measurement;
                }
            }
            catch (Exception exc)
            {
                Program.log("ERROR PACKOUT: " + tcd.Id + " - " + exc.Message);
            }
            finally
            {
                nestedDevice = SyncManager.Instance.getNestedDevice(measurement);
            }
            if (nestedDevice == null)
                return null;

            // Initialize object that will be stored under "meta" in mongo document
            BsonDocument nestedMeta = SyncManager.Instance.getNestedMeta(measurement);
            if (nestedMeta == null)
                return null;

            // Initialize object that will be stored under "data" in mongo document
            BsonDocument nestedData = SyncManager.Instance.getNestedData(tcd, excludeDataFields);

            // Initialize object in root of the document
            BsonDocument rootDoc = new BsonDocument {
                 { "_id",  tcd.Id.ToString()},
                 { "mid", measurement.Id.ToString()  },
                 { "timestamp", tcd.TimeStamp},
                 { "type", domain() },
                 { "subtype", "" },
                 { "result", !ecns.Contains(nestedDevice["SerialNumber"].ToString()) ? getResult( tcd.Result.ToString()) : "OK" },
                 { "downstatus", !ecns.Contains(nestedDevice["SerialNumber"].ToString()) ? getDownStatus( tcd.Result.ToString()) : "P" },
                 { "status", !ecns.Contains(nestedDevice["SerialNumber"].ToString()) ? getStatus(measurement.Result) : "P" }
            };

            rootDoc.Add("device", nestedDevice);
            rootDoc.Add("meta", nestedMeta);
            rootDoc.Add("data", nestedData);

            return rootDoc;
        }

        private String[] ecns =
        {
            "KD51105181",
            "KD51105180",
            "KD51105144",
            "KD51105138",
            "KD51105177",
            "KD51105187",
            "KD51105184",
            "KD51105158",
            "KD51105149",
            "KD51105151",
            "KD51105165",
            "KD51105154",
            "KD51105167",
            "KD51105140",
            "KD51105131",
            "KD51105186",
            "KD51105172",
            "KD51105129",
            "KD51105141",
            "KD51105152",
            "KD51105135",
            "KD51105150",
            "KD51105174",
            "KD51105126",
            "KD51105160",
            "KD51105134",
            "KD51105130",
            "KD51105156",
            "KD51105183",
            "KD51105162",
            "KD51105170",
            "KD51105171",
            "KD51105124",
            "KD51105163",
            "KQ51102085",
            "KQ51102015",
            "KQ51102044",
            "KQ51102032",
            "KQ51102046",
            "KQ51102070",
            "KQ51102041",
            "KQ51102030",
            "KQ51102062",
            "KQ51102063",
            "KQ51102033",
            "KQ51102056",
            "KQ51102048",
            "KQ51102054",
            "KQ51102036",
            "KQ51102037",
            "KQ51102197",
            "KQ51102196",
            "KQ51102194",
            "KQ51102124",
            "KQ51102123",
            "KQ51102122",
            "KQ51102113",
            "KQ51102117",
            "KQ51102114",
            "KQ51102108",
            "KQ51102110",
            "KQ51102107",
            "KQ51102119",
            "KQ51102120",
            "KQ51102126",
            "KQ51102193",
            "KQ51102199",
            "KQ51102200",
            "KQ51102208",
            "KQ51102202",
            "KQ51102205",
            "KQ51102192",
            "KQ51102248",
            "KQ51102223",
            "KQ51102111",
            "KQ51102184",
            "KQ51102174",
            "KQ51102183",
            "KQ51102179",
            "KQ51102186",
            "KQ51102214",
            "KQ51102100",
            "KQ51102175",
            "KQ51102101",
            "KQ51102240",
            "KQ51102235",
            "KQ51102155",
            "KQ51102137",
            "KQ51102135",
            "KQ51102210",
            "KQ51102140",
            "KQ51102224",
            "KQ51102220",
            "KQ51102144",
            "KQ51102176",
            "KQ51102148",
            "KQ51102170",
            "KQ51102162",
            "KQ51102157",
            "KQ51102229",
            "KQ51102151",
            "KQ51102217",
            "KQ51102141",
            "KQ51102143",
            "KQ51102149",
            "KQ51102244",
            "KQ51102222",
            "KQ51102130",
            "KQ51102185",
            "KQ51102189",
            "KQ51102219",
            "KQ51102221",
            "KQ51102190",
            "KQ51102136",
            "KQ51102147"
        };
    }
}
