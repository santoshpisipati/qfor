using System;

namespace Quantum_QFOR.Models
{
    public class Commodity
    {
        public string P_Commodity_Id { get; set; }
        public string P_Commodity_Name { get; set; }
        public string P_Imdg_Class_Code { get; set; }
        public string P_Commodity_Group_Desc { get; set; }        
        public string P_Imdg_Code_Page { get; set; }
        public string P_Un_No { get; set; }
        public Int32 P_Commodity_Group_Mst_Fk { get; set; }
        public string SearchType { get; set; }
        public bool IsActive { get; set; }
        public Int32 flag { get; set; }
    }
}