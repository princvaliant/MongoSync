using kaiam.MongoSync.Sync.Models;
using MongoDB.Driver.Builders;
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
using KAIAM.DataAccess;
using kaiam.MongoSync.Sync;
using System.Text.RegularExpressions;
using MongoDB.Bson;
using System.IO;
using System.Net;
using System.Text;

namespace kaiam.MongoSync.Sync
{
    abstract class SyncBase
    {
        protected const string HOST = "http://localhost:3000/";
        protected const string TOKEN = "?token=tlIu3naNJQ-zMrTbDmSczUB15P0DKtThlbzS4sA_Hkv&";
        protected const int BATCH_PERIOD_IN_HOURS = 3;
        protected const int BATCH_PERIOD_IN_MINUTES = 5;
        protected const string OK_STRING = "OK";
        protected const string ERROR_STRING = "ERR";

        protected MongoHelper<SyncLog> mhSyncLog = new MongoHelper<SyncLog>();
        protected MongoHelper<SyncStart> mhSyncStart = new MongoHelper<SyncStart>();
        protected MongoHelper<TestData> mongoDoc = new MongoHelper<TestData>();

        public void toMongoTestData(int lag)
        {
            DateTime start = getSyncStart();
            while (start < DateTime.Now)
            {
                DateTime startLag = start.AddMinutes(lag);
                DateTime end = start.AddMinutes(BATCH_PERIOD_IN_MINUTES);

                int count = processTestData(startLag, end);

                if ((this.GetType() == typeof(Ls2Cob) && count == - 1) || (this.GetType() == typeof(TosaMysql) && count == -1))
                {
                    Program.log(domain() + " ERROR: something when wrong trying to sync " + startLag.ToString() + "-" + end.ToString());
                    continue;
                }

                Program.log(domain() + " synced: " + startLag.ToString() + "-" + end.ToString() + " total:" + count.ToString());

                upsertSyncStart(start);
                start = end;
            }
        }

        public abstract int processTestData(DateTime start, DateTime end);

        public void upsertSyncStart(DateTime timestamp)
        {
            //String dstr = getData("updateSyncStart", 
            //    "domain=" + domain() + 
            //    "&start=" + String.Format("{0:s}", timestamp));

            // Commented direct mongo insertion
            SyncStart syncStart = mhSyncStart.Collection.FindOne(Query.EQ("domain", domain()));
            if (syncStart == null)
            {
                syncStart = new SyncStart();
                syncStart.domain = domain();
            }
            syncStart.start = timestamp;
            mhSyncStart.Collection.Save(syncStart);
        }

        public DateTime getSyncStart()
        {
            //String dstr = getData("getSyncStart", "domain=" + domain());
            //DateTime dt = DateTime.Parse(dstr.Replace("\"","")).ToLocalTime();
            //return dt;
            // Commented direct mongo insertion
            SyncStart syncStart = mhSyncStart.Collection.FindOne(Query.EQ("domain", domain()));
            if (syncStart == null)
                return new DateTime(2015, 10, 28);
            else
                return syncStart.start.ToLocalTime();
        }

        protected String domain()
        {
            string domain = this.GetType().ToString().ToLower();
            return domain.Substring(domain.LastIndexOf(".") + 1);
        }

        protected String getResult(String result)
        {
            Regex regex = new Regex(@"\bPASS\b|\bPASSED\b");
            Match match = regex.Match(result);
            if (match.Success)
                return OK_STRING;
            else
                return ERROR_STRING;
        }

        protected String getDownStatus(String status)
        {
            if (status.Contains("PASS"))
            {
                return "P";
            }
            else if (status.Contains("FAIL"))
            {
                return "F";
            }
            else if (status.Contains("ERROR") || status.Contains("CANCEL"))
            {
                return "E";
            }
            else
            {
                return "P";
            }
        }

        protected String getStatus(String status)
        {
            if (status == "FAIL")
            {
                return "F";
            } else if (status == "PASS")
            {
                return "P";
            } else 
            {
                return "E";
            }
        }

        protected void saveDoc(BsonDocument doc)
        {
            if (doc != null)
            {
                doc.Add("site", "NW");
                mongoDoc.Collection.Save(doc);
                // sendData(doc.ToString(), "uploadDataSync");
            }
        }

        protected void sendData(String json, String url)
        {
            // Create a request using a URL that can receive a post. 
            WebRequest request = WebRequest.Create(HOST + url + TOKEN);
            // Set the Method property of the request to POST.
            request.Method = "POST";
            // Create POST data and convert it to a byte array.
            byte[] byteArray = Encoding.UTF8.GetBytes(json);
            // Set the ContentType property of the WebRequest.
            request.ContentType = "application/json; charset=utf-8";
            // Set the ContentLength property of the WebRequest.
            request.ContentLength = byteArray.Length;
            // Get the request stream.
            Stream dataStream = request.GetRequestStream();
            // Write the data to the request stream.
            dataStream.Write(byteArray, 0, byteArray.Length);
            // Close the Stream object.
            dataStream.Close();
        }

        protected String getData(String url, String param)
        {
            // Create a request using a URL that can receive a post. 
            WebRequest request = WebRequest.Create(HOST + url + 
               TOKEN + param);
            // Get the response.
            WebResponse response = request.GetResponse();
            // Display the status.
            Console.WriteLine(((HttpWebResponse)response).StatusDescription);
            // Get the stream containing content returned by the server.
            Stream dataStream = response.GetResponseStream();
            // Open the stream using a StreamReader for easy access.
            StreamReader reader = new StreamReader(dataStream);
            // Read the content.
            string responseFromServer = reader.ReadToEnd();
            // Display the content.
            Console.WriteLine(responseFromServer);
            // Clean up the streams and the response.
            reader.Close();
            response.Close();
            return responseFromServer;
        }
    }
}
