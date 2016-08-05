using System;

namespace Quantum_QFOR.Models
{
    public class ActivityLog
    {
        public Int16 ChkOnLoad { get; set; }
        public Int16 SearchFlag { get; set; }
        public Int16 CurrentPage { get; set; }
        public Int16 TotalPage { get; set; }
        public Int16 RecType { get; set; }
        public Int16 Category { get; set; }
        public Int16 UpdateType { get; set; }
        public String frmDt { get; set; }
        public String ToDt { get; set; }
        public String trnType { get; set; }
        public String RefNr { get; set; }
        public String Desc { get; set; }
        public Int32 ExportFlg { get; set; }
    }
}