using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace Quantum_QFOR.Models
{
    public class UserDefination
    {
        public string UserID { get; set; }
        public string UserName { get; set; }
        public string BranchID { get; set; }
        public string BranchName { get; set; }
        public bool ActiveOnly { get; set; }
        public string SearchType { get; set; }
        public string strColumnName { get; set; }
        public Int32 CurrentPage { get; set; }
        public Int32 TotalPage { get; set; }
        public bool blnSortAscending { get; set; }
        public Int32 flag { get; set; }
    }
}

