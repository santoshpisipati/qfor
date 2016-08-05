using System;

namespace Quantum_QFOR.Models
{
    public class PaymentRequisition
    {
        

        public string JobNo { get; set; }
        public string Party { get; set; }
        public Int32 CurrentPage { get; set; }
        public Int32 TotalPage { get; set; }
        public Int32 flag { get; set; }
        public string invNr { get; set; }
        public double PageTotal { get; set; }
        public double GrandTotal { get; set; }
    }
}