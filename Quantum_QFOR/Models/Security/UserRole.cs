﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace Quantum_QFOR.Models
{
    public class UserRole
    {
        public string Str { get; set; }
        public string strDesc { get; set; }
        public string Searchtype { get; set; }
        public string strColumnName { get; set; }
        public bool blnSortAscending { get; set; }
        public Int32 CurrentPage { get; set; }
        public Int32 TotalPage { get; set; }
        public Int32 flag { get; set; }
        public Int32 ActFlag { get; set; }
    }
}