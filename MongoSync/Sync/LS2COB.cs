using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using MongoDB.Bson;
using MySql.Data.MySqlClient;
using Oracle.ManagedDataAccess.Client;

namespace kaiam.MongoSync.Sync
{
    class Ls2Cob : SyncBase
    {
        private readonly string[] excludeDataFields = new string[] {  "id", "version", "comment", "spectrum_pwr_dbm", "operator", "ecn", "fw_num" };

        public override int processTestData(DateTime start, DateTime end)
        {
            int cnt = 0;
            // Import LS2 data from Scotland database (mySQL, OracleDb)
            try
            {
                MongoViewHelper mvh = new MongoViewHelper("testdata");
                String connstr = "SERVER=liv-svr-mysql3;DATABASE=xosa;UID=newark;PASSWORD=GFS54ad:)4dfH;Connection Timeout=7000";
                MySqlConnection connection = new MySqlConnection(connstr);
                connection.Open();
                string query = "SELECT *, cob_dc_channel_test.id as test_id FROM cob_dc_test,cob_dc_channel_test " +
                    "WHERE test_date >= '" + start.ToString("yyyy-MM-dd HH:mm:ss") + "' AND test_date < '" + end.ToString("yyyy-MM-dd HH:mm:ss") + "' " +
                    "AND cob_dc_test.id = cob_dc_channel_test.cob_test_id ";

                //Create Command
                MySqlCommand cmd = new MySqlCommand(query, connection);
                cmd.CommandTimeout = 7200;
                //Create a data reader and Execute the command
                MySqlDataReader dataReader = cmd.ExecuteReader();
                DataTable schemaTable = dataReader.GetSchemaTable();
                string[] lists = { "rx_cal_values", "tx_cal_values", "li_current", "li_power", "mpd_crosstalk", "rx_mpd_leakage", "spectrum_pwr_dbm", "spectrum_wl_nm" };
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
                    Program.log("LS2 I-Track DB Connection ERROR: " + OraEx.Message + "\n" + OraEx.StackTrace);
                    return -1;
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
                         { "_id", "COB-" + bson["test_id"]},
                         { "mid",  "COBMID-" + bson["cob_test_id"] },
                         { "timestamp", bson["test_date"]},
                         { "step", "download" },
                         { "type", bson["dut_type"] },
                         { "subtype", "dc" },
                         { "result", bson["fail_code"].ToString().Equals("Pass", StringComparison.Ordinal) ? "P" : "F" },
                         { "measstatus", bson["pass_fail"].ToString().Equals("Pass", StringComparison.Ordinal) ? "P" : "F"},
                         { "status", bson["pass_fail"].ToString().Equals("Pass", StringComparison.Ordinal) ? "P" : "F" }
                    };

                    rootDoc.Add("meta", new BsonDocument {
                         { "StartDateTime",  bson["test_date"]},
                         { "EndDateTime",  bson["test_date"]},
                         { "Channel",  bson["channel"]},
                         { "FirmwareVer", bson["fw_version"] },
                         {"SWVer", bson["sw_version"] },
                         {"TestStation", bson["test_station"] }
                    });

                    if (bson["serial_number"] != null)
                    {
                        bson["sn"] = bson["serial_number"];
                        bson.Add("laser_pn", new BsonArray());
                        bson.Remove("serial_number");

                        string partNum = bson["part_number"].ToString();
                        if (partNum.Contains("REFUNIT"))
                        {
                            continue;
                        }
                        string serNum = bson["sn"].ToString();
                        string pcbSerNum = bson["pcb_serial_number"].ToString();

                        var UKDeviceType = "Not Found";
                        var UKPartNumber = "Not Found";
                        var UKDescription = "Not Found";
                        var UKPartRevision = "Not Found";
                        var hasRows = false;
                        string oraQuery;
                        OracleCommand oraCmd;
                        OracleDataReader dr = null;

                        try {
                            if (!pcbSerNum.Equals("", StringComparison.Ordinal))
                            {
                                // ----- 2017-08-25: Adding connection to Livingston I-Track DB -----
                                oraQuery = "SELECT DISTINCT DEVICE_OBJ.DEVICE_ID, ROUTE, ROUTE_OBJ.DESCRIPTION FROM KAIAM.DEVICE_OBJ, KAIAM.ASSEMBLE_PROC, KAIAM.ASSEMBLE_DATA, KAIAM.ROUTE_OBJ WHERE ASSEMBLE_PROC.ASSEMBLE_PROC_UID = ASSEMBLE_DATA.ASSEMBLE_PROC_UID AND ASSEMBLE_DATA.DEVICE_OBJ_UID = DEVICE_OBJ.DEVICE_OBJ_UID AND DEVICE_OBJ.DEVICE_ID = '" + pcbSerNum + "' AND ROUTE = ROUTE_ID ORDER BY ROUTE_SEQ DESC FETCH FIRST 1 ROWS ONLY";
                                oraCmd = new OracleCommand(oraQuery, oraConn);
                                oraCmd.CommandType = CommandType.Text;
                                oraCmd.CommandTimeout = 7200;
                                dr = oraCmd.ExecuteReader();
                                hasRows = dr.Read();
                            }
                            if (!hasRows)
                            {
                                oraQuery = "SELECT DISTINCT DEVICE_OBJ.DEVICE_ID, ROUTE, ROUTE_OBJ.DESCRIPTION FROM KAIAM.DEVICE_OBJ, KAIAM.ASSEMBLE_PROC, KAIAM.ASSEMBLE_DATA, KAIAM.ROUTE_OBJ WHERE ASSEMBLE_PROC.ASSEMBLE_PROC_UID = ASSEMBLE_DATA.ASSEMBLE_PROC_UID AND ASSEMBLE_DATA.DEVICE_OBJ_UID = DEVICE_OBJ.DEVICE_OBJ_UID AND DEVICE_OBJ.DEVICE_ID = '" + serNum + "' AND ROUTE = ROUTE_ID ORDER BY ROUTE_SEQ DESC FETCH FIRST 1 ROWS ONLY";
                                oraCmd = new OracleCommand(oraQuery, oraConn);
                                oraCmd.CommandType = CommandType.Text;
                                oraCmd.CommandTimeout = 7200;
                                dr = oraCmd.ExecuteReader();
                                hasRows = dr.Read();
                            }
                        }
                        catch (Exception executeException)
                        {
                            Program.log("LS2 I-Track DB query ERROR: " + executeException.Message + "\n" + executeException.StackTrace);
                            return -1;
                        }

                        try
                        {
                            if (hasRows)
                            {
                                var route = dr["Route"].ToString().Split('_');
                                UKDeviceType = route[1] + route[2];
                                UKPartNumber = route[3];
                                UKPartRevision = route[4];
                                UKDescription = dr["DESCRIPTION"].ToString();
                            }
                        }

                        catch (Exception e)
                        {
                            Program.log("LS2 I-Track DB data output ERROR: " + e.Message + " (" + serNum + ")");
                        }
                        
                        // ------------------------------------------------------------------

                        rootDoc.Add("device", new BsonDocument {
                             { "SerialNumber", serNum},
                             { "PartNumber", partNum},
                             { "PCBSerialNumber", bson["pcb_serial_number"] },
                             // ----- 2017-08-25: Adding connection to Livingston I-Track DB -----
                             { "UKDeviceType", UKDeviceType},
                             { "UKDevicePartNumber", UKPartNumber},
                             { "UKDeviceDescription", UKDescription},
                             { "UKDeviceRevision", UKPartRevision}
                             // ------------------------------------------------------------------
                        });

                        bson.Remove("test_id");
                        bson.Remove("cob_test_id");
                        bson.Remove("test_date");
                        bson.Remove("channel");
                        bson.Remove("fail_code");
                        bson.Remove("pass_fail");
                        bson.Remove("dut_type");
                        bson.Remove("fw_version");
                        bson.Remove("part_number");
                        bson.Remove("sw_version");
                        bson.Remove("test_station");
                        bson.Remove("pcb_serial_number");
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
                Program.log("LS2 import ERROR: " + exc.Message + "\n" + exc.StackTrace);
                return -1;
            }
            return cnt;
        }
    }
}
