using System;

namespace Quantum_QFOR.Models
{
    public class FMCSeaUserExport
    {
        public string VslVoy { get; set; }
        public Int64 JobPk { get; set; }
        public Int64 ShipperPk { get; set; }
        public Int64 HBLPk { get; set; }
        public Int32 CurrentPage { get; set; }
        public Int32 TotalPage { get; set; }
        public string depDate { get; set; }
        public Int32 flag { get; set; }
        public Int32 loc { get; set; }
    }
}