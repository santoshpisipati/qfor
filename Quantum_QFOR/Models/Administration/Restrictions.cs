using System;

namespace Quantum_QFOR.Models
{
    public class Restrictions
    {
        public string RestrictionRefNo { get; set; }
        public string RestrictionDate { get; set; }
        public int Restrictionfk { get; set; }
        public int Commoditypk { get; set; }        
        public string RestrictionMessage { get; set; }
        public string IntBizType { get; set; }
        public Int32 RestrictType { get; set; }
        public Int16 RestrictAs { get; set; }
        public string SearchType { get; set; }
        public string IMDGCode { get; set; }
        public bool Active { get; set; }
        public Int32 flag { get; set; }
    }
}