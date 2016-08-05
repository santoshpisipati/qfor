using System;

namespace Quantum_QFOR.Models
{
    public class TaskAllotment
    {
        public Int32 RestrictionTypePK { get; set; }
        public Int32 ReferencePK { get; set; }
        public Int32 CustomerPK { get; set; }
        public Int16 Status { get; set; }        
        public string RestrictionMsg { get; set; }
        public string S_C { get; set; }
        public String FromDate { get; set; }
        public String ToDate { get; set; }
        public Int16 DataonLoad { get; set; }       
        public string SearchType { get; set; }
        public bool IsActive { get; set; }
        public Int32 flag { get; set; }
    }
}