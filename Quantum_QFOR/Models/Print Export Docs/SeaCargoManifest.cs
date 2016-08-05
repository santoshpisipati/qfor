using System;

namespace Quantum_QFOR.Models
{
    public class SeaCargoManifest
    {
        public Int64 VesPK { get; set; }
        public String Ves_Flight { get; set; }
        public String Voyage { get; set; }
        public String POL { get; set; }
        public Int64 MBLPk { get; set; }
        public Int64 HBLPk { get; set; }
        public String POD { get; set; }
        public Int32 CurrentPage { get; set; }
        public Int32 TotalPage { get; set; }
        public String CargoType { get; set; }
        public Int64 CommodityType { get; set; }
        public String Status { get; set; }
        public Int32 nLocationFk { get; set; }
        public Int32 flag { get; set; }
        public String Customer { get; set; }
        public String Consignee { get; set; }
        public String DPAgent { get; set; }
    }
}