using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace Quantum_QFOR.Models
{
    public class AuditTrail
    {
        public Int64 UserID { get; set; }
        public Int64 Modules { get; set; }
        public String Activity { get; set; }
        public String SortBy { get; set; }
        public String FromDate { get; set; }
        public String ToDate { get; set; }
        public String location { get; set; }
        public Int32 CurrentPage { get; set; }
        public Int32 TotalPage { get; set; }
    }
}

