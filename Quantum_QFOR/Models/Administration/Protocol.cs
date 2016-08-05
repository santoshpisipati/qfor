using System;

namespace Quantum_QFOR.Models
{
    public class Protocol
    {
        public Int64 ProtocolPk { get; set; }
        public string ProtocolName { get; set; }
        public string ProtocolValue { get; set; }
        public string SearchType { get; set; }
        public string strSortColumn { get; set; }
        public string P_Un_No { get; set; }
        public int P_Protocol_Group_Mst_Fk { get; set; }
        public bool IsActive { get; set; }
        public int flag { get; set; }
    }
}