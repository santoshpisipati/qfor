using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace Quantum_QFOR.Models
{
    public class MasterJobCardSea
    {
        public string MSTJCRefNo { get; set; }
        public bool ActiveOnly { get; set; }
        public string POLPk { get; set; }
        public string PODPk { get; set; }
        
        public string AgentPk { get; set; }
        public string LinePk { get; set; }
        public string SearchType { get; set; }
        public Int32 CurrentPage { get; set; }
        public Int32 TotalPage { get; set; }
        public string POLID { get; set; }
        public string PODId { get; set; }
        public string POLName { get; set; }
        public string PODName { get; set; }
        public Int64 lngUsrLocFk { get; set; }
        public string strColumnName { get; set; }
        public bool blnSortAscending { get; set; }
        public Int32 flag { get; set; }
        public string VesselName { get; set; }
    }
}

