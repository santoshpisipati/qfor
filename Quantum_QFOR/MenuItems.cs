using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace Quantum_QFOR
{
    public class MenuItems
    {
        public int Id { get; set; }
        public string MenuName { get; set; }

        public List<SubMenuItem> SubMenus { get; set; }
    }

    public class SubMenuItem
    {
        public int Id { get; set; }
        public string MenuName { get; set; }
    }
}