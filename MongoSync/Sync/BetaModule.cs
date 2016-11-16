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
using System.IO;
using GemBox.Spreadsheet;
using System.Dynamic;
using System.Text.RegularExpressions;
using System.Globalization;
using MongoDB.Bson.IO;
using System.Collections;

namespace kaiam.MongoSync.Sync
{

    class BetaModule : SyncBase
    {
        private readonly string[] excludeDataFields = new string[] { "_entityWrapper", "Id", "TimeStamp", "Result", "Measurement", "MeasurementId", "FirmwareFile", "ConstantsFile", "SettingsFile", "PersonalityFile", "ScriptFile" };
        private readonly string dir = "\\\\F1\\Public\\Development\\QSFP28\\Test_and_measurements\\Beta_modules";
        private readonly BsonDocument txmap = new BsonDocument {
                 { "ER dB", "Er_in_dB" },
                 {"Ave_Power dBm", "Pavg_in_dBm" },
                { "OMA dBm", "OMA_in_dBm" },
                 {"Jitter_RMS psec", "RMSJitter_in_picoSeconds" },
                { "Jitter_PP psec", "PPJitter_in_picoSeconds" },
               {  "Crossing %", "Crossing_in_Percent" }
            };

        public override int processTestData(DateTime start, DateTime end)
        {
            SpreadsheetInfo.SetLicense("FREE-LIMITED-KEY");
            // Filter all directories that have _FINAL in it
            string[] subdirectoryEntries = Directory.GetDirectories(dir);
            foreach (string subdirectory in subdirectoryEntries)
            {
                if ((subdirectory.ToUpper().Contains("_FINAL") || subdirectory.ToUpper().Contains("_RUN")) && !subdirectory.ToUpper().Contains("FAIL"))
                {
                    FileInfo fi = new FileInfo(subdirectory);
                    if (fi.CreationTime >= start && fi.CreationTime <= end)
                    {
                        try {
                            this.processItem(fi);
                        } catch(Exception exc)
                        {
                            Program.log("ERROR BETA MODULE" + " "  + fi.Name + " " + exc.Message);
                        }
                    }
                }
            }
        
            return 0;
        }

        private void processItem(FileInfo fi)
        {
            string serial = fi.Name.Split('_')[0];
            processTxTest(serial, fi, "txtests", "channeldata", "OK", "P");
            processRxTest(serial, fi, "rxtests", "sensitivity", "OK", "P");
            processDcTest(serial, fi, "dctests", "", "OK", "P");
        }

        private void processRxTest(string serial, FileInfo fi, string type, string subtype, string res, string stat )
        {
            // Process subdirectories containing temperature and voltage specific data
            string[] subdirs = Directory.GetDirectories(fi.FullName);
            foreach (string subdirectory in subdirs)
            {
                BsonDocument bson = initBson(serial, fi, type, subtype, res, stat);
                populateTemperatureVoltage(bson, subdirectory);
                ExcelWorksheet ws = readExcel(new FileInfo(subdirectory + "\\DUT_TX_DUT_RX_BER.xlsx"));
                importRxTestData(ws, bson);
            }
        }

        private void processTxTest(string serial, FileInfo fi, string type, string subtype, string res, string stat)
        {
            // Process subdirectories containing temperatue and voltage specific data
            string[] subdirs = Directory.GetDirectories(fi.FullName);
            foreach (string subdirectory in subdirs)
            {
                BsonDocument bson = initBson(serial, fi, type, subtype, res, stat);
                populateTemperatureVoltage(bson, subdirectory);
                ExcelWorksheet ws = readExcel(new FileInfo(subdirectory + "\\eye_data.xlsx"));
                importTxTestData(ws, bson);
            }
        }

        private void processDcTest(string serial, FileInfo fi, string type, string subtype, string res, string stat)
        {
            BsonDocument bson = initBson(serial, fi, type, subtype, res, stat);
            ExcelWorksheet ws = readExcel(new FileInfo(fi.FullName + "\\DC_data.xlsx"));
            importDcTestData(ws, bson);
        }

        private ExcelWorksheet readExcel(FileInfo fi)
        {
            if (!fi.Exists)
                return null;
            ExcelFile ef = ExcelFile.Load(fi.FullName);
            return ef.Worksheets[0];
        }

        private void populateTemperatureVoltage(BsonDocument obj, string subdir)
        {
            DirectoryInfo fi = new DirectoryInfo(subdir);
            if (obj != null && fi.Exists)
            {
                String pattern = @"[-+]?[0-9]*\.?[0-9]*";
                MatchCollection m = Regex.Matches(fi.Name, pattern);
                if (m[0].Value != "")
                    obj["meta"]["SetTemperature_C"] = long.Parse(m[0].Value);
                if (m[3].Value != "")
                    obj["meta"]["SetVoltage"] = float.Parse(m[3].Value);

            }
        }

        private BsonDocument initBson(string serial, FileInfo fi, string type, string subtype, string res, string stat)
        {
            BsonDocument rootDoc = new BsonDocument {
                  { "mid" ,  fi.Name},
                 { "timestamp", fi.CreationTime},
                 { "type", type },
                 { "subtype",  subtype },
                 { "result", res },
                 { "status", stat }
            };
            rootDoc.Add("device", new BsonDocument {
                { "SerialNumber", serial },
                { "ContractManufacturer", "Kaiam" },
                { "PartNumber", "XQX4000" },
                { "PartRevision", 1 },
                { "PartType", "Production" },
                { "PartFamily", "XQX4000" }
            });
            rootDoc.Add("meta", new BsonDocument {
                { "StartDateTime", fi.CreationTime },
                { "EndDateTime", fi.CreationTime },
                { "User", "" },
                { "DUT", "" },
                { "Rack", "" },
                { "SetTemperature_C", -1},
                { "SetVoltage", -1 },
                { "Channel", -1 }
            });
            rootDoc.Add("data", new BsonDocument());
            return rootDoc;
        }

        private void importRxTestData(ExcelWorksheet ws, BsonDocument bson)
        {
            if (ws == null || bson == null)
                return; 
            BsonDocument[] bsons = new BsonDocument[4];
            // Loop through all cells in 3. row to determine channels
            Console.WriteLine(bson["mid"]);
            for (int c = 1; c < 30; c++)
            {
                if (ws.Rows[2].Cells[c].Value != null && ws.Rows[3].Cells[c].Value != null)
                {
                    int channel = int.Parse(ws.Rows[2].Cells[c].Value.ToString().Replace("Ch", ""));
                    if (bsons[channel] == null)
                    {
                        bsons[channel] = (BsonDocument)bson.DeepClone();
                        bsons[channel].Add(new BsonElement("_id", Guid.NewGuid().ToString()));
                        bsons[channel]["meta"]["Channel"] = channel;
                    }
                    string varname = ws.Rows[3].Cells[c].Value.ToString().Replace(".", "").Trim();
                    if (varname == "Q0" || varname == "Q1" || varname == "Q2" || varname == "Q3")
                    {
                        varname = "Q";
                    }
                    BsonArray barray = new BsonArray();
                    for (int r = 4; r < 20; r++)
                    {
                        if (ws.Rows[r].Cells[c].Value != null && ws.Rows[r].Cells[c].Value.ToString() != "#N/A")
                        {
                            Console.WriteLine(ws.Rows[r].Cells[c].Value);
                            decimal d;
                            if (decimal.TryParse(ws.Rows[r].Cells[c].Value.ToString(), NumberStyles.AllowExponent | NumberStyles.Float, CultureInfo.InvariantCulture, out d))
                            {
                                Console.WriteLine(d);
                                barray.Add(decimal.ToDouble(d));
                            } else
                            {
                                barray.Add(0);
                            }
                        } else
                        {
                            barray.Add(0);
                        }
                    }
                    bsons[channel]["data"][varname] = barray; 
                }
            }
            for (int i = 0; i < 4; i++)
            {
                if (ws.Rows[26 + i].Cells[2].Value != null)
                {
                    decimal d;
                    if (decimal.TryParse(ws.Rows[26 + i].Cells[2].Value.ToString(), out d)) { 
                        bsons[i]["data"]["CWDM4 sensitivity dBm"] = decimal.ToDouble(d);
                    }
                }
                if (ws.Rows[26 + i].Cells[6].Value != null)
                {
                    decimal d;
                    if (decimal.TryParse(ws.Rows[26 + i].Cells[6].Value.ToString(), out d))
                    {
                        bsons[i]["data"]["CLR4 sensitivity dBm"] = decimal.ToDouble(d);
                    }
                }
                //Console.WriteLine(bsons[i].ToJson(new JsonWriterSettings { Indent = true }));
                saveDoc(bsons[i]);
            }
        }

        private void importTxTestData(ExcelWorksheet ws, BsonDocument bson)
        {
            if (ws == null || bson == null)
                return;
            BsonDocument[] bsons = new BsonDocument[4];
            // Loop through all cells in 3. row to determine channels
            for (int c = 1; c <= 12; c++)
            {
                if (ws.Rows[2].Cells[c].Value != null && ws.Rows[3].Cells[c].Value != null)
                {
                    string varname = ws.Rows[2].Cells[c].Value.ToString().Replace(".", "").Trim() + " " + 
                        ws.Rows[3].Cells[c].Value.ToString().Replace(".", "").Trim();
                    BsonArray barray = new BsonArray();
                    for (int r = 4; r <= 7; r++)
                    {
                        if (bsons[r - 4] == null)
                        {
                            bsons[r - 4] = (BsonDocument)bson.DeepClone();
                            bsons[r - 4].Add(new BsonElement("_id", Guid.NewGuid().ToString()));
                            bsons[r - 4]["meta"]["Channel"] = r - 4;
                        }
                        if (ws.Rows[r].Cells[c].Value != null && ws.Rows[r].Cells[c].Value.ToString() != "#N/A")
                        {
                            decimal d;
                            if (decimal.TryParse(ws.Rows[r].Cells[c].Value.ToString(), out d)) {
                                if (txmap.Contains(varname))
                                {
                                    varname = txmap[varname].ToString();
                                }
                                bsons[r - 4]["data"][varname] = decimal.ToDouble(d);
                            }
                        }
                    }
                   
                }
            }
            for (int i = 0; i < 4; i++)
            {
                // Console.WriteLine(bsons[i].ToJson(new JsonWriterSettings { Indent = true }));
                saveDoc(bsons[i]);
            }
        }

        private void importDcTestData(ExcelWorksheet ws, BsonDocument bson)
        {
            if (ws == null || bson == null)
                return;
            int lastIndex = 0;
            float f = 0;
            var list = new ArrayList();
            String pattern = @"[-+]?[0-9]*\.?[0-9]*";
            // Loop through all cells in 1. and 2. column to determine channels
            int r = 1;
            do
            {
                Console.WriteLine(bson["mid"]);
                if (ws.Rows != null && ws.Rows[r].Cells[0].Value != null)
                {
                    string val0 = ws.Rows[r].Cells[0].Value.ToString().Trim();
                    object val1 = ws.Rows[r].Cells[1].Value;
                    if (val0.IndexOf("Condition:") >= 0)
                    {
                        var dict = new Dictionary<string, BsonDocument[]>
                        {
                            {"c",  new BsonDocument[4]}, {"m", new BsonDocument[1]}
                        };
                        list.Add(dict);
                        lastIndex = list.Count - 1;

                        for (int i = 0; i < 4; i++)
                        {
                            ((Dictionary<string, BsonDocument[]>)list[lastIndex])["c"][i] = (BsonDocument)bson.DeepClone();
                        }
                        ((Dictionary<string, BsonDocument[]>)list[lastIndex])["m"][0] = (BsonDocument)bson.DeepClone();

                        for (int i = 0; i < 4; i++)
                        {
                            ((Dictionary<string, BsonDocument[]>)list[lastIndex])["c"][i].Add(new BsonElement("_id", Guid.NewGuid().ToString()));
                            ((Dictionary<string, BsonDocument[]>)list[lastIndex])["c"][i]["subtype"] = "channel";
                            ((Dictionary<string, BsonDocument[]>)list[lastIndex])["c"][i]["meta"]["Channel"] = i;
                        }
                        ((Dictionary<string, BsonDocument[]>)list[lastIndex])["m"][0].Add(new BsonElement("_id", Guid.NewGuid().ToString()));
                        ((Dictionary<string, BsonDocument[]>)list[lastIndex])["m"][0]["subtype"] = "module";

                        MatchCollection m = Regex.Matches(val0, pattern);
                        if (m[11].Value != "")
                        {
                            for (int i = 0; i < 4; i++)
                            {
                                ((Dictionary<string, BsonDocument[]>)list[lastIndex])["c"][i]["meta"]["SetTemperature_C"] = long.Parse(m[11].Value);
                            }
                            ((Dictionary<string, BsonDocument[]>)list[lastIndex])["m"][0]["meta"]["SetTemperature_C"] = long.Parse(m[11].Value);
                        }
                        if (m[14].Value != "")
                        {
                            for (int i = 0; i < 4; i++)
                            {
                                ((Dictionary<string, BsonDocument[]>)list[lastIndex])["c"][i]["meta"]["SetVoltage"] = float.Parse(m[14].Value);
                            }
                            ((Dictionary<string, BsonDocument[]>)list[lastIndex])["m"][0]["meta"]["SetVoltage"] = float.Parse(m[14].Value);
                        }

                    }
                    else if (val0 == "Supply current(A)" && val1 != null)
                    {
                        for (int i = 0; i < 4; i++)
                        {
                            ((Dictionary<string, BsonDocument[]>)list[lastIndex])["c"][i]["data"]["Current (A)"] = float.Parse(val1.ToString());
                        }
                        ((Dictionary<string, BsonDocument[]>)list[lastIndex])["m"][0]["data"]["Current (A)"] = float.Parse(val1.ToString());
                    }
                    else if (val0 == "Supply voltage(V)" && val1 != null)
                    {

                    }
                    else if (val0.IndexOf("ch.") >= 0 && val1 != null)
                    {
                        MatchCollection m = Regex.Matches(val0, pattern);
                        long i = long.Parse(m[9].Value.Replace(".", ""));
                        if (float.TryParse(val1.ToString(), out f))
                        {
                            ((Dictionary<string, BsonDocument[]>)list[lastIndex])["c"][i]["data"][val0.Replace(" ch." + i.ToString(), "")] = f;
                        }
                        else
                        {
                            ((Dictionary<string, BsonDocument[]>)list[lastIndex])["c"][i]["data"][val0.Replace(" ch." + i.ToString(), "")] = val1.ToString();
                        }
                    }
                    else if (val1 != null)
                    {
                        if (float.TryParse(val1.ToString(), out f))
                        {
                            ((Dictionary<string, BsonDocument[]>)list[lastIndex])["m"][0]["data"][val0] = f;
                        }
                        else
                        {
                            ((Dictionary<string, BsonDocument[]>)list[lastIndex])["m"][0]["data"][val0] = val1.ToString();
                        }
                    }

                    r++;
                }
            } while (ws.Rows[r].Cells[0].Value != null);

            foreach (var item in list)
            {
                var dict = (Dictionary<string, BsonDocument[]>)item;
                foreach (var bc in dict["c"])
                {
                    //Console.WriteLine(bc.ToJson(new JsonWriterSettings { Indent = true }));
                    saveDoc(bc);
                }
                foreach (var bm in dict["m"])
                {
                    //Console.WriteLine(bm.ToJson(new JsonWriterSettings { Indent = true }));
                    saveDoc(bm);
                }
            }
        }
    }
}
