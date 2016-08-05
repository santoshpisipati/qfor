using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace Quantum_QFOR.Models
{
    public class AuditTrailLog
    {
        public Int32 CurrentPage { get; set; }
        public Int32 TotalPage { get; set; }
        public Int64 locPk { get; set; }
        public Int64 UserPk { get; set; }
        public Int64 modulefk { get; set; }
        public Int64 menu_fk { get; set; }
        public string fromDt { get; set; }
        public string toDt { get; set; }
        public Int16 PostBackFlag { get; set; }
        public Int32 flag { get; set; }
    }
}

 