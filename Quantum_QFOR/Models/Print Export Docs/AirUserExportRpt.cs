using System;

namespace Quantum_QFOR.Models
{
    public class AirUserExportRpt
    {
        public String Flight { get; set; }
        public Int64 JobPk { get; set; }
        public Int64 PolPk { get; set; }
        public Int64 PodPk { get; set; }
        public Int64 CustPk { get; set; }
        public Int64 strLocPk { get; set; }
        public Int32 CurrentPage { get; set; }
        public Int32 TotalPage { get; set; }
        public Int32 flag { get; set; }
    }
}