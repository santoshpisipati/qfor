using System;

namespace Quantum_QFOR.Models
{
    public class EcommGateWays
    {
        public Int32 CurrentPage { get; set; }
        public Int32 TotalPage { get; set; }
        public Int16 LoadFlg { get; set; }
        public String Company { get; set; }
        public string City { get; set; }
        public Int64 LocationFK { get; set; }
        public Int64 CountryFK { get; set; }
        public Int32 Status { get; set; }
        public String FromDate { get; set; }
        public string ToDate { get; set; }
    }
}