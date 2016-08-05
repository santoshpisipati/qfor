using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace Quantum_QFOR.Models
{
    public class Employee
    {
        public string EmpId { get; set; }
        public string EmpName { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string LocId { get; set; }
        public string LocName { get; set; }
        public string DepartmentID { get; set; }
        public string DepartmentName { get; set; }
        public string DesignationID { get; set; }
        public string DesignationName { get; set; }
        public string StateId { get; set; }
        public string StateName { get; set; }
        public string Address1 { get; set; }
        public string Address2 { get; set; }
        public string City { get; set; }
        public string ZIP { get; set; }
        public string Phone { get; set; }
        public string Mobile { get; set; }
        public string EMail { get; set; }
        public string SearchType { get; set; }
        public string strColumnName { get; set; }
        public Int32 CurrentPage { get; set; }
        public Int32 TotalPage { get; set; }
        public Int32 intBusType { get; set; }
        public Int32 intUser { get; set; }
        public Int32 intActive { get; set; }
        public bool blnSortAscending { get; set; }
        public Int32 flag { get; set; }
    }
}


 