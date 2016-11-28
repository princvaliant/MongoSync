using KAIAM.DataAccess;
using System;
using System.Data.SqlTypes;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.Entity.Infrastructure;
using System.Data.Entity.SqlServer;
using System.Data.Entity.Validation;
using System.Linq;
using System.Linq.Expressions;
using System.Collections;
using MongoDB.Driver.Builders;
using MongoDB.Bson;
using FastMember;
using System.Data.SqlClient;
using System.Configuration;
using System.Data;


namespace kaiam.MongoSync.Sync
{
    class SyncManager
    {
        private static SyncManager instance;
        private string[] domains = { "TosaMysql", "Packout", "RxTests", "Download", "PowerCheckAssembly", "PowerCalBeforeTx", "ModuleTests","ModuleSetups", "TxSetups" , "RxSetups", "TxTests" };
        private string[] dbviews = { "vTestSpeed" };
        private Dictionary<Guid, Device> devices = new Dictionary<Guid, Device>();
        private Dictionary<Guid, Part> parts = new Dictionary<Guid, Part>();
        private Dictionary<Guid, OutputPart> outputParts = new Dictionary<Guid, OutputPart>();
        private Dictionary<Guid, PartFamily> partFamilies = new Dictionary<Guid, PartFamily>();
        private Dictionary<Guid, String> contrManufs = new Dictionary<Guid, String>();
        private Dictionary<Guid, List<_BatchRequest>> batchRequests = new Dictionary<Guid, List<_BatchRequest>>();
        private Dictionary<Guid, Rack> racks = new Dictionary<Guid, Rack>();
        private Dictionary<Guid, DUT> DUTs = new Dictionary<Guid, DUT>();
        private Dictionary<Guid, Dictionary<String, String>> tosarosas = new Dictionary<Guid, Dictionary<String, String>>();

        private SyncManager() { }

        class _BatchRequest
        {
            public DateTime date { get; set; }
            public string name { get; set; }
        }

        // Singleton implementation
        public static SyncManager Instance
        {
            get
            {
                if (instance == null)
                {
                    instance = new SyncManager();
                }
                return instance;
            }
        }

        // Loops through domains and executing sync for each domain
        public void startSync()
        { 
            // Import data from SQL server
            foreach (var domain in domains)
            {
                try
                {
                    Type type = Type.GetType("kaiam.MongoSync.Sync." + domain);
                    if (type != null)
                    {
                        SyncBase sync = (SyncBase)Activator.CreateInstance(type);
                        sync.toMongoTestData(-15);
                    }
                }
                catch (Exception exc)
                {
                    Program.log(domain + " ERROR: " + exc.Message + "\n" + exc.StackTrace);
                }
            }

            // Import measurement records without any down

            // Import beta module data from excel files
            try
            {
                Type typeBm = Type.GetType("kaiam.MongoSync.Sync.BetaModule");
                SyncBase syncBm = (SyncBase)Activator.CreateInstance(typeBm);
                syncBm.toMongoTestData(0);
            }
            catch (Exception exc)
            {
                Program.log("BetaModule ERROR: " + exc.Message + "\n" + exc.StackTrace);
            }

            // Import views from SQL server every night at 2 AM
            DateTime date1 = DateTime.Now;
            String hour = date1.ToString("%htt");
            if (hour != "2AM")
            {
                return;
            }
            try
            {
                String connstr = "data source=dbs1.kaiam.local;initial catalog=KAIAM.Data.Test.Production49;user id=KAIAM.TestUser;password=5525Iamkaiam!";
                SqlConnection remoteConnection = new SqlConnection(connstr);
                remoteConnection.Open();
                foreach (var dbview in dbviews)
                {
                    MongoViewHelper mvh = new MongoViewHelper(dbview);
                    mvh.Collection.RemoveAll();
                    SqlCommand myCommand = new SqlCommand("select * from " + dbview, remoteConnection);
                    SqlDataReader myReader = myCommand.ExecuteReader();
                    DataTable schemaTable = myReader.GetSchemaTable();
                    while (myReader.Read())
                    {
                        BsonDocument bson = new BsonDocument();
                        foreach (DataRow row in schemaTable.Rows)
                        {
                            Dictionary<string, object> dictData = new Dictionary<string, object>();
                            if (myReader[row["ColumnName"].ToString()].GetType().ToString() == "System.TimeSpan")
                            {
                                int secs = 0;
                                System.TimeSpan ts = (System.TimeSpan)myReader[row["ColumnName"].ToString()];
                                secs += ts.Seconds;
                                secs += ts.Minutes * 60;
                                secs += ts.Hours * 3600;
                                secs += ts.Days * 86400;
                                dictData.Add(row["ColumnName"].ToString(), secs);
                            }
                            else
                            {
                                dictData.Add(row["ColumnName"].ToString(), myReader[row["ColumnName"].ToString()]);
                            }
                            bson.AddRange(dictData);
                        }
                        mvh.Collection.Save(bson);
                    }
                    myReader.Close();
                }
                remoteConnection.Close();


            }
            catch (Exception exc) {
                Program.log("VIEW import ERROR: " + exc.Message + "\n" + exc.StackTrace);
            }
        }

        // Helper function to speed up device retrieval by caching it in the local dictionary
        private Device getDevice(Guid deviceId, OutputPart outputPart)
        {
            if (devices.ContainsKey(deviceId))
            {
                return devices[deviceId];
            }
            else
            {
                using (KAIAMDataTestContainer db = new KAIAMDataTestContainer(Program.connString))
                {
                    var device = (from d in db.Devices where d.Id == deviceId select d);
                    if (device.Any())
                    {
                        Device dev = device.First();

                        if (outputPart != null && !outputParts.ContainsKey(outputPart.Id))
                        {
                            outputParts.Add(outputPart.Id, outputPart);
                        }
                        devices.Add(deviceId, device.First());

                        String cm = "Kaiam";
                        if (device.First().ContractManufacturer != null)
                        {
                            cm = device.First().ContractManufacturer.Name;
                        }
                        contrManufs.Add(deviceId, cm);

                        foreach (var br in device.First().BatchRequest.OrderBy(it => it.TimeStampRequested))
                        {
                            if (!batchRequests.ContainsKey(deviceId))
                            {
                                batchRequests.Add(deviceId, new List<_BatchRequest>());
                            }
                            batchRequests[deviceId].Add(new _BatchRequest { date = br.TimeStampRequested, name = br.Name });
                        }

                        // Deterimine tosa, rosa, pbda
                        try
                        {
                            if (device.First().AssemblyRecords.Any())
                            {
                                foreach (var assrec in device.First().AssemblyRecords)
                                {
                                    if (assrec.AssemblyItems.Any())
                                    {
                                        foreach (var assitm in assrec.AssemblyItems)
                                        {
                                            if (assitm != null && assitm.BOMItem != null && assitm.BOMItem.InfeedPart != null)
                                            {
                                                if (!tosarosas.ContainsKey(deviceId))
                                                {
                                                    tosarosas.Add(deviceId, new Dictionary<String, String>());
                                                }
                                                var asstype = assitm.BOMItem.InfeedPart.Name;
                                                var assnumber = assitm.SerialNumber;
                                                tosarosas[deviceId].Add(asstype, assnumber);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        catch (Exception exc)
                        {
                            Program.log("ERROR: TOSA ROSA " + exc.Message);
                        }

                        return device.First();
                    }
                }
            }
            return null;
        }





        // Helper function to speed up part retrieval by caching it in the local dictionary
        private Part getPart(Guid partId)
        {
            if (parts.ContainsKey(partId))
            {
                return parts[partId];
            }
            else
            {
                using (KAIAMDataTestContainer db = new KAIAMDataTestContainer(Program.connString))
                {
                    var part = (from d in db.Parts where d.Id == partId select d);
                    if (part.Any())
                    {
                        parts.Add(partId, part.First());
                        return part.First();
                    }
                }
            }
            return null;
        }


        // Helper function to speed up part family retrieval by caching it in the local dictionary
        private PartFamily getPartFamily(Guid partFamilyId)
        {
            if (partFamilies.ContainsKey(partFamilyId))
            {
                return partFamilies[partFamilyId];
            }
            else
            {
                using (KAIAMDataTestContainer db = new KAIAMDataTestContainer(Program.connString))
                {
                    var partFamily = (from d in db.PartFamilySet where d.Id == partFamilyId select d);
                    if (partFamily.Any())
                    {
                        partFamilies.Add(partFamilyId, partFamily.First());
                        return partFamily.First();
                    }
                }
            }
            return null;
        }

        // Helper function to speed up rack retrieval by caching it in the local dictionary
        public Rack getRack(Guid rackId)
        {
            if (racks.ContainsKey(rackId))
            {
                return racks[rackId];
            }
            else
            {
                using (KAIAMDataTestContainer db = new KAIAMDataTestContainer(Program.connString))
                {
                    var rack = (from d in db.Racks where d.Id == rackId select d);
                    if (rack.Any())
                    {
                        racks.Add(rackId, rack.First());
                        return rack.First();
                    }
                }
            }
            return null;
        }

        // Helper function to speed up DUT retrieval by caching it in the local dictionary
        public DUT getDUT(Guid dutId)
        {
            if (DUTs.ContainsKey(dutId))
            {
                return DUTs[dutId];
            }
            else
            {
                using (KAIAMDataTestContainer db = new KAIAMDataTestContainer(Program.connString))
                {
                    var dut = (from d in db.DUTs where d.Id == dutId select d);
                    if (dut.Any())
                    {
                        DUTs.Add(dutId, dut.First());
                        return dut.First();
                    }
                }
            }
            return null;
        }

        public BsonDocument getNestedDevice(Measurement measurement)
        {
            BsonDocument bson = new BsonDocument();
            Device device = null;
            if (measurement != null && measurement.DeviceId.HasValue)
            {
                 device = getDevice(measurement.DeviceId.Value, measurement.PartSpec.OutputPart);
            }
            if (device == null)
            {
                return null;
            }

            if (device.SerialNumber == null)
                return null;
            bson.Add("SerialNumber", device.SerialNumber);
            bson.Add("ContractManufacturer", contrManufs[device.Id]);

            if (measurement.PartSpec == null || measurement.PartSpec.OutputPart == null)
                return null;

            Part part = getPart(measurement.PartSpec.OutputPart.Id);
            if (part == null)
            {
                Program.log("ERROR: No Part ID " + measurement.PartSpec.OutputPart.Id);
                return null;
            }

            // Determine batch requests and initialize their proper count  
            if (batchRequests.ContainsKey(device.Id)) {
                int brcount = 0;
                foreach (var br in batchRequests[device.Id])
                {
                    brcount += 1;
                    if (measurement.StartDateTime >= br.date && brcount == 1)
                    {
                        bson.Set("brDate", br.date);
                        bson.Set("brName", br.name);
                    }
                }
                bson.Set("brCount", brcount);
            }

            // Add tosa rosas
            if (tosarosas.ContainsKey(device.Id))
            {
                foreach (var br in tosarosas[device.Id])
                {
                    bson.Add(br.Key, br.Value);
                }
            }

            // Initialize part numbers
            bson.Add("PartNumber", part.PartNumber);
            bson.Add("PartRevision", part.Revision);
            bson.Add("PartType", part.Type);

            if (outputParts.ContainsKey(device.PartId))
            {
                PartFamily partFamily = getPartFamily(outputParts[device.PartId].PartFamilyId.Value);
                if (partFamily == null)
                {
                    Program.log("ERROR: No Part Family ID " + outputParts[device.PartId].PartFamilyId.ToString());
                    return null;
                }
                bson.Add("PartFamily", partFamily.Name);
            }

            return bson;
        }

        public BsonDocument getNestedMeta(Measurement measurement)
        {
            // Initialize object that will be stored under 'meta' in mongo document
            BsonDocument bson = new BsonDocument { };

            bson.Add("StartDateTime", measurement.StartDateTime);
            bson.Add("EndDateTime", measurement.EndDateTime);
            if (measurement.User != null)
                bson.Add("User", measurement.User.Login);
            var dut = SyncManager.Instance.getDUT(measurement.DUTId);
            if (dut != null)
            {
                bson.Add("DUT", dut.Name);
                var rack = SyncManager.Instance.getRack(dut.RackId);
                if (rack != null)
                {
                    bson.Add("Rack", rack.Name);
                }
            }

            return bson;
        }

        public BsonDocument getNestedData(Object tcd, string[] excludeDataFields)
        {
            BsonDocument nestedData = new BsonDocument { };
            Dictionary<string, object> dictData = new Dictionary<string, object>();

            var accessor2 = TypeAccessor.Create(tcd.GetType());
            foreach (var prop in accessor2.GetMembers())
            {
                if (!excludeDataFields.Contains(prop.Name))
                {
                    dictData.Add(prop.Name, accessor2[tcd, prop.Name]);
                }
            }
            nestedData.AddRange(dictData);
            return nestedData;
        }

    }
}
