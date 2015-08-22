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

namespace kaiam.MongoSync.Sync
{
    class SyncManager
    {
        private static SyncManager instance;
        private string[] domains = { "RxSetups", "RxTests", "TxSetups", "TxTests", "ModuleSetups", "ModuleTests" };
        private Dictionary<Guid, Device> devices = new Dictionary<Guid, Device>();
 
        private SyncManager() { }

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
            foreach (var domain in domains)
            {
                Type type = Type.GetType("kaiam.MongoSync.Sync." + domain);
                if (type != null)
                {
                    SyncBase sync = (SyncBase)Activator.CreateInstance(type);
                    sync.upsertSyncStart(new DateTime(2015, 4, 15));
                    sync.copyDataToMongo();
                }
            }
        }

        // Helper function to speed up device retrieval by caching it in the local dictionary
        public Device getDevice(Guid deviceId)
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
                        devices.Add(deviceId, device.First());
                        return device.First();
                    }
                }
            }
            return null;
        }
    }
}
