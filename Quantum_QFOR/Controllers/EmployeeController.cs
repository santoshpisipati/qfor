#region "Comments"

//'***************************************************************************************************************
//'*  Company Name            :
//'*  Project Title           :    QFOR
//'***************************************************************************************************************
//'*  Created By              :    Santosh on 10-May-16
//'*  Module/Project Leader   :    Santosh Pisipati
//'*  Description             :
//'*  Module/Form/Class Name  :
//'*  Configuration ID        :
//'***************************************************************************************************************
//'*  Revision History
//'***************************************************************************************************************
//'*  Modified DateTime(DD-MON-YYYY)              Modified By                             Remarks (Bugs Related)
//'*
//'*
//'***************************************************************************************************************

#endregion "Comments"

using Newtonsoft.Json;
using System.Web.Http;

namespace Quantum_QFOR.Controllers
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="System.Web.Http.ApiController" />
    public class EmployeeController : ApiController
    {
        [AcceptVerbs("GET", "POST")]
        [HttpGet]
        public object FetchLocationonly()
        {
            cls_Employee_Mst_Table cs = new cls_Employee_Mst_Table();
            string value = cs.FetchLocationonly();
            return JsonConvert.DeserializeObject(value);
        }

        [AcceptVerbs("GET", "POST")]
        [HttpGet]
        public object FetchDesigOnDepartment()
        {
            cls_Employee_Mst_Table cs = new cls_Employee_Mst_Table();
            string value = cs.FetchDesigOnDepartment();
            return JsonConvert.DeserializeObject(value);
        }

        [AcceptVerbs("GET", "POST")]
        [HttpGet]
        public object FetchDepartment()
        {
            cls_Employee_Mst_Table cs = new cls_Employee_Mst_Table();
            string value = cs.FetchDepartment();
            return JsonConvert.DeserializeObject(value);
        }

        [AcceptVerbs("GET", "POST")]
        [HttpGet]
        public object FetchAll()
        {
            cls_Employee_Mst_Table cs = new cls_Employee_Mst_Table();
            int strColumn = 1;
            int Current_Page = 0;
            string value = cs.FetchAll("", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "EMPLOYEE_ID", strColumn, Current_Page, 3, 3, 1, true, 1);
            return JsonConvert.DeserializeObject(value);
        }

        [AcceptVerbs("GET", "POST")]
        [HttpGet]
        public object FetchBusinessType_Removals()
        {
            cls_Employee_Mst_Table cs = new cls_Employee_Mst_Table();
            string value = cs.FetchBusinessType_Removals();
            return JsonConvert.DeserializeObject(value);
        }
    }
}