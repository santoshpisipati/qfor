using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace Quantum_QFOR.Models
{
    public class AirlineSchedule
    {
        public String Carrier_FK { get; set; }
        public String strFlightNo { get; set; }
        public Int16 excludeExp { get; set; }
        public String SortColumn { get; set; }
        public Int32 CurrentPage { get; set; }
        public Int32 TotalPage { get; set; }
        public Int64 usrLocFK { get; set; }
    }
}

