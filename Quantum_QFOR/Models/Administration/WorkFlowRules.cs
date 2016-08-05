using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace Quantum_QFOR.Models
{
    

    public class WorkFlowRules
    {
        public Int32 Activity { get; set; }
        public String Rule { get; set; }
        public String fdate { get; set; }
        public String tdate { get; set; }
        public Int16 chk { get; set; }
        public bool IsActive { get; set; }
        public string SearchType { get; set; }
        public int flag { get; set; }
        public string biz_type { get; set; }
    }
}