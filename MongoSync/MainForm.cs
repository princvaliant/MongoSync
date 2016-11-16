using System;
using System.Collections;
using System.Drawing;
using System.Windows.Forms;
using System.Collections.Generic;

using System.Data.SqlTypes;
using System.Data.Entity;
using System.Linq;
using KAIAM.DataAccess;


namespace kaiam.MongoSync
{
    public partial class MainForm : Form
    {
        ProcessIcon processIcon;
        internal ProcessIcon ProcessIcon
        {
            get
            {
                return processIcon;
            }

            set
            {
                processIcon = value;
            }
        }

        public MainForm()
        {
            InitializeComponent();
        }
        
        public void log(String str)
        {
            String strDate = DateTime.Now.ToString("MM/dd/yy hh:mm:ss fff | ");
            logbox.Items.Insert(0, strDate + " " + str);
            Application.DoEvents();
        }     
         
        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void MainForm_Load(object sender, EventArgs e)
        {
            FormBorderStyle = FormBorderStyle.FixedDialog;
            ShowInTaskbar = false; // Remove from taskbar.
        }
        
        private void start_Click(object sender, EventArgs e)
        {
            Sync.SyncManager.Instance.startSync();
        }

        private void MainForm_Resize(object sender, EventArgs e)
        {
            if (WindowState == FormWindowState.Minimized)
            {
                this.Visible = false;
            }
        }
    }
}
