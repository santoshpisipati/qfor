using System;

namespace Quantum_QFOR.Models
{
    public class AppFinLoc
    {
        public Int32 FromFlag { get; set; }
        public String DBName { get; set; }
        public String ProductName { get; set; }
        public String LocationName { get; set; }
        public String Description { get; set; }
        public String fromDate { get; set; }
        public String Todate { get; set; }
        public Int32 Status { get; set; }
        public String SearchType { get; set; }
        public Int32 TotalPage { get; set; }
        public Int32 CurrentPage { get; set; }
    }
}