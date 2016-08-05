using System;

namespace Quantum_QFOR.Models
{
    public class DocWorkFlowRules
    {
        public string strWFRulesId { get; set; }
        public String strDocId { get; set; }
        public String strEmpName { get; set; }
        public String intBusType { get; set; }
        public string intUser { get; set; }
        public bool IsActive { get; set; }
        public string SearchType { get; set; }
        public int flag { get; set; }
        public string biz_type { get; set; }
    }
}