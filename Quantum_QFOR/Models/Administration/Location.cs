using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace Quantum_QFOR.Models
{
    public class Location
    {
        public Int64 P_Location_Mst_Pk { get; set; }
        public string P_Location_Id { get; set; }
        public string P_Location_Name { get; set; }
        public Int64 P_LocationType { get; set; }
        public string P_Rep_Location_Id { get; set; }
        public string P_Rep_Location_Name { get; set; }
        public string P_COUNTRY_Id { get; set; }
        public string P_COUNTRY_Name { get; set; }
        public string P_OFFICE_NAME { get; set; }
        public string SearchType { get; set; }
        public string strColumnName { get; set; }
        public Int32 CurrentPage { get; set; }
        public Int32 TotalPage { get; set; }
        public Int32 isActive { get; set; }
        public bool isEFS { get; set; }
        public bool blnSortAscending { get; set; }
        public Int32 flag { get; set; }

        public Int32 FromFlag { get; set; }

    }
}

