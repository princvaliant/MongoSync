using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Forms;

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
            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);

            // Show the system tray icon.					
            using (ProcessIcon pi = new ProcessIcon())
            {
                pi.Display();
                mainForm = new MainForm();
                mainForm.ProcessIcon = pi;
                Application.Run(mainForm);
            }
        }

        public static void log(String str) {
            mainForm.log(str);
        }
    }
}
