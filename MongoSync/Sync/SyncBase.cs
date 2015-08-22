using kaiam.MongoSync.Sync.Models;
using MongoDB.Driver.Builders;
using System;

namespace kaiam.MongoSync.Sync
{
    abstract class SyncBase
    {
        protected const int BATCH_PERIOD_IN_HOURS = 12;
        protected MongoHelper<SyncLog> mhSyncLog = new MongoHelper<SyncLog>();
        protected MongoHelper<SyncStart> mhSyncStart= new MongoHelper<SyncStart>();
        
        public abstract void copyDataToMongo();

        public void upsertSyncStart(DateTime timestamp)
        {
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
            SyncStart syncStart = mhSyncStart.Collection.FindOne(Query.EQ("domain", domain()));
            return syncStart.start;
        }

        protected String domain ()
        {
            string domain = this.GetType().ToString().ToLower();
            return domain.Substring(domain.LastIndexOf(".") + 1);
        }
    }
}
