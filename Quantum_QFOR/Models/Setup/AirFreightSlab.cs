using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace Quantum_QFOR.Models
{
    public class AirFreightSlab
    {
        public string strBreakPointId { get; set; }
        public string strDescription { get; set; }
        public string SearchType { get; set; }
        public string strColumnName { get; set; }
        public Int32 CurrentPage { get; set; }
        public Int32 TotalPage { get; set; }
        public Int16 SortCol { get; set; }
        public Int16 IsActive { get; set; }
        public bool blnSortAscending { get; set; }
        public Int32 flag { get; set; }
    }
}

