using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;

namespace KonikaGlo
{
    class FileHash
    {
        public static String fileName = "c:\\temp\\hash.bin";
        public static void save (Hashtable hash)
        {
            BinaryFormatter bfw = new BinaryFormatter();
            FileStream file = new FileStream(fileName, FileMode.OpenOrCreate);
            StreamWriter ws = new StreamWriter(file);
            bfw.Serialize(ws.BaseStream, hash);
        }

        public static Hashtable read()
        {
            FileStream file = new FileStream(fileName, FileMode.Open);
            StreamReader readMap = new StreamReader(file);
            BinaryFormatter bf = new BinaryFormatter();
            Hashtable hash = (Hashtable)bf.Deserialize(readMap.BaseStream);
            return hash;
        }

    }
}
