using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.IO;

namespace kaiam.MongoSync
{
    static class Program
    {
        public static MainForm mainForm;
        public static String connString = "KAIAMDataTestRemoteContainer";

        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [STAThread]
        static void Main()
        {
            // Application.EnableVisualStyles();
            // Application.SetCompatibleTextRenderingDefault(false);				
            //using (ProcessIcon pi = new ProcessIcon())
            //{
            //    pi.Display();
            //    mainForm = new MainForm();
            //    mainForm.ProcessIcon = pi;
            //    Application.Run(mainForm);
            //}

            Sync.SyncManager.Instance.startSync();
        }

        public static void log(String str) {

            Console.WriteLine(str);

            DateTime date = DateTime.Now;
            using (StreamWriter w = File.AppendText("c:\\users\\jdominguez\\My Documents\\log" + date.ToString("yy-MM-dd") + ".txt"))
            {
                w.WriteLine( str);
                w.Close();
                w.Dispose();
            }


        }
    }
}
