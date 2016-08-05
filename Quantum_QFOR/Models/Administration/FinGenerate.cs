using System;

namespace Quantum_QFOR.Models
{
    public class FinGenerate
    {
        public Int32 DocType { get; set; }
        public Int32 CustPK { get; set; }
        public Int16 CSTPK { get; set; }
        public Int32 RefPK { get; set; }
        public string FromDate { get; set; }
        public string ToDate { get; set; }
        public Int32 DocStatus { get; set; }
        public string Doc_Ref_Nr { get; set; }
        public string Cust_name { get; set; }
        public string biztype { get; set; }
        public string processtype { get; set; }
        public string cargotype { get; set; }
        public Int32 CurrentPage { get; set; }
        public Int32 TotalPage { get; set; }
        public Int32 Flag1 { get; set; }
        public string SearchType { get; set; }
    }
}