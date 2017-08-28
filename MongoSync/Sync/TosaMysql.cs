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
using MySql.Data.MySqlClient;
using System.Data;
using System.Text.RegularExpressions;
using Oracle.ManagedDataAccess.Client;

namespace kaiam.MongoSync.Sync
{

    class TosaMysql : SyncBase
    {
        private readonly string[] excludeDataFields = new string[] {  "id", "version", "spectrum_amplitudes", "spectrum_wavelengths",
                    "dut_type", "n_temps", "operator_number", "product_num", "dut_number", "test_duration", "i_track_lot_id"};

        public override int processTestData(DateTime start, DateTime end)
        {
            int cnt = 0;
            // Import TOSA data from Scotland database (mySQL
            try
            {
                MongoViewHelper mvh = new MongoViewHelper("testdata");
                String connstr = "SERVER=liv-svr-mysql3;DATABASE=xosa;UID=newark;PASSWORD=GFS54ad:)4dfH;Connection Timeout=7000";
                MySqlConnection connection = new MySqlConnection(connstr);
                connection.Open();
                string query = "SELECT * FROM osa_test,osa_sub_test,stripe,osa_sub_test_osa_stripe " +
                    "WHERE test_date >= '" + start.ToString("yyyy-MM-dd HH:mm:ss") + "' AND test_date < '" + end.ToString("yyyy-MM-dd HH:mm:ss") + "' " +
                    "AND osa_test.id = osa_sub_test.test_id " +
                    "AND stripe.id = osa_sub_test_osa_stripe.osa_stripe_id " +
                    "AND osa_sub_test.id = osa_sub_test_osa_stripe.osa_sub_test_stripes_id";
                //Create Command
                MySqlCommand cmd = new MySqlCommand(query, connection);
                cmd.CommandTimeout = 7200;
                //Create a data reader and Execute the command
                MySqlDataReader dataReader = cmd.ExecuteReader();
                DataTable schemaTable = dataReader.GetSchemaTable();
                string[] lists = { "liv_current_ma", "liv_power_mw", "liv_voltagev", "liv_mpd_ua",
                    "mpd_current_ua_stripe0", "mpd_current_ua_stripe1", "mpd_current_ua_stripe2", "mpd_current_ua_stripe3",
                    "mpd_ratio_db_stripe0", "mpd_ratio_db_stripe1", "mpd_ratio_db_stripe2", "mpd_ratio_db_stripe3"};
                char[] sep = { ' ' };

                // ----- 2017-08-25: Adding connection to Livingston I-Track DB -----
                string oradb = "Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=172.24.17.62)(PORT=1521)))(CONNECT_DATA=(SID=ITRK02)(SERVER=DEDICATED)));User Id=READ_ONLY_KAIAM;Password=I-TrackLLC123;";
                OracleConnection oraConn = new OracleConnection(oradb);
                try
                {
                    oraConn.Open();
                }
                catch (Exception OraEx)
                {
                    Program.log("TOSA I-Track DB Connection ERROR: " + OraEx.Message + "\n" + OraEx.StackTrace);
                }

                // ------------------------------------------------------------------

                while (dataReader.Read())
                {
                    cnt++;
                    BsonDocument bson = new BsonDocument();
                    foreach (DataRow row in schemaTable.Rows)
                    {
                        String col = row["ColumnName"].ToString();
                        if (!excludeDataFields.Contains(col))
                        {
                            Dictionary<string, object> dictData = new Dictionary<string, object>();

                            if (dataReader[col].GetType().ToString() == "System.TimeSpan")
                            {
                                int secs = 0;
                                System.TimeSpan ts = (System.TimeSpan)dataReader[col];
                                secs += ts.Seconds;
                                secs += ts.Minutes * 60;
                                secs += ts.Hours * 3600;
                                secs += ts.Days * 86400;
                                dictData.Add(col, secs);
                            }
                            else
                            {
                                var s = dataReader[col].GetType().ToString();
                                if (s != "System.DBNull")
                                {
                                    if (!lists.Contains(col))
                                    {
                                        dictData.Add(col, dataReader[col]);
                                    }
                                    else
                                    {
                                        String sl = dataReader[col].ToString();
                                        string[] sa = sl.Split(sep, StringSplitOptions.RemoveEmptyEntries);
                                        dictData.Add(col, Array.ConvertAll(sa, i => float.Parse(i)));
                                    }
                                }
                            }
                            if (!bson.Contains(col))
                            {
                                bson.AddRange(dictData);
                            }
                        }
                    }

                    BsonDocument rootDoc = new BsonDocument {
                         { "_id", "TOSA-" + bson["osa_stripe_id"]},
                         { "mid",  "TOSAMID-" + bson["osa_sub_test_stripes_id"] },
                         { "timestamp", bson["test_date"]},
                         { "type", "tosa" },
                         { "subtype", "dc" },
                         { "result", bson["pass"] == 1 ? "P" : "F" },
                         { "measstatus", bson["pass"] == 1 ? "P" : "F"},
                         { "status", bson["pass"] == 1 ? "P" : "F" }
                    };

                    rootDoc.Add("meta", new BsonDocument {
                         { "StartDateTime",  bson["test_date"]},
                         { "EndDateTime",  bson["test_date"]},
                         { "Channel",  bson["stripe_number"]}
                    });

                    if (bson["tosa_serial_number"] != null)
                    {
                        string[] tsn = bson["tosa_serial_number"].ToString().Split('_');
                        bson["tsn"] = tsn[0];
                        bson.Add("laser_pn", new BsonArray());
                        for (int i = 1; i < tsn.Length; i++)
                        {
                            ((BsonArray)bson["laser_pn"]).Add(tsn[i]);
                        }
                        bson.Remove("tosa_serial_number");

                        string tosaType = bson["tsn"].ToString().Substring(0, 3);
                        string serNum = bson["tsn"].ToString().Substring(3);

                        // ----- 2017-08-25: Adding connection to Livingston I-Track DB -----
                        string oraQuery = "SELECT DEVICE_ID, ROUTE, DESCRIPTION FROM (SELECT UNIQUE ROUTE, DEVICE_ID FROM (SELECT DEVICE_ID, LOT_ID AS LID FROM KAIAM.DEVICE_LOT_LOOKUP WHERE DEVICE_ID = '" + serNum + "') CROSS JOIN KAIAM.ASSEMBLE_PROC WHERE LID = KAIAM.ASSEMBLE_PROC.LOT_ID) CROSS JOIN KAIAM.ROUTE_OBJ WHERE ROUTE = ROUTE_ID";
                        OracleCommand oraCmd = new OracleCommand(oraQuery, oraConn);
                        oraCmd.CommandType = CommandType.Text;
                        oraCmd.CommandTimeout = 7200;
                        OracleDataReader dr = oraCmd.ExecuteReader();

                        try
                        {
                            dr.Read();
                        }
                        catch (Exception executeException)
                        {
                            Program.log("TOSA I-Track DB query ERROR: " + executeException.Message + "\n" + executeException.StackTrace);
                        }

                        var UKDeviceType = "Not Found";
                        var UKPartNumber = "Not Found";
                        var UKDescription = "Not Found";

                        try
                        {
                            var route = dr["Route"].ToString().Split('_');
                            UKDeviceType = route[1];
                            UKPartNumber = route[2];
                            UKDescription = dr["DESCRIPTION"].ToString();
                        }
                        catch (Exception e)
                        {
                            Program.log("TOSA I-Track DB data output ERROR: " + e.Message + "\n" + e.StackTrace);
                        }
                        // ------------------------------------------------------------------

                        rootDoc.Add("device", new BsonDocument {
                             { "SerialNumber", serNum},
                             { "TosaType", tosaType},
                             // ----- 2017-08-25: Adding connection to Livingston I-Track DB -----
                             { "UKDeviceType", UKDeviceType},
                             { "UKDevicePartNumber", UKPartNumber},
                             { "UKDeviceDescription", UKDescription}
                             // ------------------------------------------------------------------
                        });

                        bson.Remove("osa_stripe_id");
                        bson.Remove("osa_sub_test_stripes_id");
                        bson.Remove("test_date");
                        bson.Remove("stripe_number");
                        bson.Remove("pass");
                        rootDoc.Add("data", bson);

                        mvh.Collection.Save(rootDoc);
                    }
                }

                //close Data Reader and connection
                dataReader.Close();
                connection.Close();
                // ----- 2017-08-25: Adding connection to Livingston I-Track DB -----
                oraConn.Close();
                // ------------------------------------------------------------------
            }
            catch (Exception exc)
            {
                Program.log("TOSA import ERROR: " + exc.Message + "\n" + exc.StackTrace);
            }
            return cnt;
        }
    }
}
