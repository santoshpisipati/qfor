using System;

namespace Quantum_QFOR.Models
{
    public class Document
    {
        public string P_Document_Id { get; set; }
        public Int32 P_Document_Name { get; set; }
        public Int32 intDocTypeFk { get; set; }
        public Int32 intDocGroupFk { get; set; }
        public string SearchType { get; set; }
        public bool IsActive { get; set; }
        public Int32 flag { get; set; }

        public Int32 bizType { get; set; }
    }
}