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
using Oracle.ManagedDataAccess.Client;
using Quantum_QFOR.Models;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Web.Http;

namespace Quantum_QFOR.Controllers
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="System.Web.Http.ApiController" />
    public class AdministratorController : ApiController
    {
        /// <summary>
        /// The menu items
        /// </summary>
        private MenuItems[] MenuItems = new MenuItems[]
       {
            new MenuItems { Id = 1, MenuName = "Administrator"},
            new MenuItems { Id = 2, MenuName = "Security"},
            new MenuItems { Id = 3, MenuName = "Set-up"},new MenuItems { Id = 4, MenuName = "Sales & Marketing"},
            new MenuItems { Id = 5, MenuName = "Customer Service"},new MenuItems { Id = 6, MenuName = "Supplier Management"},new MenuItems { Id = 7, MenuName = "Rating & Tarrif"}
            ,new MenuItems { Id = 8, MenuName = "Order Management"},new MenuItems { Id = 9, MenuName = "Export Docs"},new MenuItems { Id = 10, MenuName = "Import Docs"}
            ,new MenuItems { Id = 11, MenuName = "Recievables"},new MenuItems { Id = 12, MenuName = "Payables"},new MenuItems { Id = 13, MenuName = "Print Export Docs"}
            ,new MenuItems { Id = 14, MenuName = "Print Import Docs"},new MenuItems { Id = 15, MenuName = "Customs Brokerage"},new MenuItems { Id = 16, MenuName = "EDI"}
            ,new MenuItems { Id = 17, MenuName = "MIS"},new MenuItems { Id = 18, MenuName = "Reports"},new MenuItems { Id = 19, MenuName = "Utilities"}
            ,new MenuItems { Id = 20, MenuName = "User Activity"}
       };

        // GET: api/Administrator/GetMenuItems()
        /// <summary>
        /// Gets all menu items.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<MenuItems> GetAllMenuItems()
        {
            return MenuItems;
        }

        /// <summary>
        /// Gets the menu items.
        /// </summary>
        /// <param name="MenuId">The menu identifier.</param>
        /// <returns></returns>
        public IEnumerable<string> GetMenuItems(Int64 MenuId)
        {
            return new string[] { "AdminstratorSetup", "value2" };
        }

        /// <summary>
        /// Fetches all locations.
        /// </summary>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        [HttpGet]
        public object FetchAllLocations()
        {
            string strSQL = null;
            strSQL = "          SELECT                 ";
            strSQL += "      \tLOCATION_MST_PK\t      ,";
            strSQL += "      \tCORPORATE_MST_FK\t  ,";
            strSQL += "      \tLOCATION_ID\t          ,";
            strSQL += "      \tLOCATION_NAME\t      ,";
            strSQL += "      \tLOCATION_TYPE_FK\t  ,";
            strSQL += "      \tREPORTING_TO_FK\t      ,";
            strSQL += "      \tADDRESS_LINE1\t      ,";
            strSQL += "      \tADDRESS_LINE2\t      ,";
            strSQL += "      \tADDRESS_LINE3\t      ,";
            strSQL += "      \tZIP\t                  ,";
            strSQL += "      \tCITY\t              ,";
            strSQL += "      \tTELE_PHONE_NO\t      ,";
            strSQL += "      \tFAX_NO\t              ,";
            strSQL += "      \tE_MAIL_ID\t          ,";
            strSQL += "      \tREMARKS\t              ,";
            strSQL += "      \tTIME_ZONE\t          ,";
            strSQL += "      \tCOST_CENTER\t          ,";
            strSQL += "      \tCREATED_BY_FK\t      ,";
            strSQL += "      \tCREATED_DT\t          ,";
            strSQL += "      \tLAST_MODIFIED_BY_FK\t  ,";
            strSQL += "      \tLAST_MODIFIED_DT\t  ,";
            strSQL += "      \tVERSION_NO\t          ,";
            strSQL += "      \tCOUNTRY_MST_FK\t      ,";
            strSQL += "      \tACTIVE_FLAG\t          ,";
            strSQL += "      \tCOMP_LOCATION\t      ,";
            strSQL += "      \tOFFICE_NAME\t          ,";
            strSQL += "      \tLOGO_FILE_PATH\t      ,";
            strSQL += "      \tIATA_CODE\t          ,";
            strSQL += "      \tVAT_NO\t               ";
            strSQL += " FROM    LOCATION_MST_TBL WHERE ACTIVE_FLAG=1 ORDER BY LOCATION_ID";

            WorkFlow objWF = new WorkFlow();
            DataTable objDT = new DataTable();
            try
            {
                objDT = objWF.GetDataTable(strSQL);
                string value = JsonConvert.SerializeObject(objDT, Formatting.Indented);
                return JsonConvert.DeserializeObject(value);
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        /// <summary>
        /// Fetches the location.
        /// </summary>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        [HttpGet]
        public object FetchLocation()
        {
            clsLocation_Mst_Tbl clsLocation = new clsLocation_Mst_Tbl();
            string value = clsLocation.FetchLocation();
            return JsonConvert.DeserializeObject(value);
        }

        /// <summary>
        /// Fetches the ports.
        /// </summary>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        [HttpGet]
        public object FetchPorts()
        {
            clsLocation_Working_Ports_Trn clsPort = new clsLocation_Working_Ports_Trn();
            string value = clsPort.GetWorkingPortForLoc(-1);
            return JsonConvert.DeserializeObject(value);
        }

        /// <summary>
        /// Fetches the departments.
        /// </summary>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        [HttpGet]
        public object FetchDepartments()
        {
            cls_Location_Departments_Trn clsDepartment = new cls_Location_Departments_Trn();
            string value = clsDepartment.GetWorkingDept(-1);
            return JsonConvert.DeserializeObject(value);
        }

        /// <summary>
        /// Fetches the location banks.
        /// </summary>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        [HttpGet]
        public object FetchLocationBanks()
        {
            cls_Location_Bank_Trn clsLocationBank = new cls_Location_Bank_Trn();
            string value = clsLocationBank.FetchAll(-1);
            return JsonConvert.DeserializeObject(value);
        }

        /// <summary>
        /// Fetches the monthly cutoff.
        /// </summary>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        [HttpGet]
        public object FetchMonthlyCutoff()
        {
            clsLocation_Mst_Tbl clsLocation = new clsLocation_Mst_Tbl();
            int No_Months = default(Int32);
            if ((ConfigurationManager.AppSettings["No_of_Months"] != null))
            {
                No_Months = Convert.ToInt32(ConfigurationManager.AppSettings["No_of_Months"]);
            }
            else
            {
                No_Months = 12;
            }
            string value = clsLocation.FetchMonthlyCutoff(-1, No_Months);
            return JsonConvert.DeserializeObject(value);
        }

        /// <summary>
        /// Gets the country data.
        /// </summary>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        [HttpGet]
        public object GetCountryData()
        {
            CommonSearch cs = new CommonSearch();
            string value = cs.getSearchModule("COMMONSESADMIN", "COUNTRY", "L~~COUNTRY~1~0~0~~");
            string FillGrid = cs.FillGrid(value);
            return JsonConvert.DeserializeObject(FillGrid);
        }

        /// <summary>
        /// Gets the office name data.
        /// </summary>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        [HttpGet]
        public object GetOfficeNameData()
        {
            CommonSearch cs = new CommonSearch();
            string value = cs.getSearchModule("COMMONSESADMIN", "OFFICENAME", "L~~OFFICENAME~1~0~0~~");
            string FillGrid = cs.FillGrid(value);
            return JsonConvert.DeserializeObject(FillGrid);
        }

        /// <summary>
        /// Gets the shipping line data.
        /// </summary>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        [HttpGet]
        public object GetShippingLineData()
        {
            CommonSearch cs = new CommonSearch();
            string value = cs.getSearchModule("COMMONSESADMIN", "OPR", "L~~OPR~0~0~~~~~1");
            string FillGrid = cs.FillGrid(value);
            return JsonConvert.DeserializeObject(FillGrid);
        }

        /// <summary>
        /// Gets the pol data.
        /// </summary>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        [HttpGet]
        public object GetPOLData()
        {
            CommonSearch cs = new CommonSearch();
            string value = cs.getSearchModule("COMMONSESADMIN", "POL", "L~~POL~~0~~~~~1841~1");
            string FillGrid = cs.FillGrid(value);
            return JsonConvert.DeserializeObject(FillGrid);
        }

        /// <summary>
        /// Gets the pod data.
        /// </summary>
        /// <returns></returns>
        public object GetPODData()
        {
            CommonSearch cs = new CommonSearch();
            string value = cs.getSearchModule("COMMONSESADMIN", "POD", "L~~POD~~0~~~~~1");
            string FillGrid = cs.FillGrid(value);
            return JsonConvert.DeserializeObject(FillGrid);
        }

        /// <summary>
        /// Gets the line data.
        /// </summary>
        /// <returns></returns>
        public object GetLineData()
        {
            CommonSearch cs = new CommonSearch();
            string value = cs.getSearchModule("Common", "OPERATOR", "L~~1841");
            string FillGrid = cs.FillGrid(value);
            return JsonConvert.DeserializeObject(FillGrid);
        }

        /// <summary>
        /// Gets all pol data.
        /// </summary>
        /// <returns></returns>
        public object GetAllPOLData()
        {
            CommonSearch cs = new CommonSearch();
            string value = cs.getSearchModule("Common", "POL", "L~~~2");
            string FillGrid = cs.FillGrid(value);
            return JsonConvert.DeserializeObject(FillGrid);
        }

        /// <summary>
        /// Gets all pod data.
        /// </summary>
        /// <returns></returns>
        public object GetAllPODData()
        {
            CommonSearch cs = new CommonSearch();
            string value = cs.getSearchModule("Common", "POD", "L~~2");
            string FillGrid = cs.FillGrid(value);
            return JsonConvert.DeserializeObject(FillGrid);
        }

        /// <summary>
        /// Gets the exchange rates data.
        /// </summary>
        /// <returns></returns>
        public object GetExchangeRatesData()
        {
            CommonSearch cs = new CommonSearch();
            string value = cs.getSearchModule("COMMONSESADMIN", "EXCHANGE_RATE", "L~~EXCHANGE_RATE~1418~1~0~1~~~0");
            string FillGrid = cs.FillGrid(value);
            return JsonConvert.DeserializeObject(FillGrid);
        }

        /// <summary>
        /// Gets the customer data.
        /// </summary>
        /// <returns></returns>
        public object GetCustomerData()
        {
            CommonSearch cs = new CommonSearch();
            string value = cs.getSearchModule("COMMONSESADMIN", "SALESMARK_CUST", "L~~SALESMARK_CUST~1~~~0~0~0~3~~~1~~1841");
            string FillGrid = cs.FillGrid(value);
            return JsonConvert.DeserializeObject(FillGrid);
        }

        /// <summary>
        /// Gets the handling data.
        /// </summary>
        /// <returns></returns>
        public object GetHandlingData()
        {
            CommonSearch cs = new CommonSearch();
            string value = cs.getSearchModule("Common", "LOCATION_BY_NAME_FOR_COUNTRY", "L~~0");
            string FillGrid = cs.FillGrid(value);
            return JsonConvert.DeserializeObject(FillGrid);
        }

        /// <summary>
        /// Gets the protocol.
        /// </summary>
        /// <returns></returns>
        public object GetProtocol()
        {
            cls_Protocol_Mst_Tbl cs = new cls_Protocol_Mst_Tbl();
            string value = cs.FetchProtocol();
            return JsonConvert.DeserializeObject(value);
        }

        /// <summary>
        /// Gets the user roles.
        /// </summary>
        /// <returns></returns>
        public object GetUserRoles()
        {
            cls_Protocol_Mst_Tbl cs = new cls_Protocol_Mst_Tbl();
            string value = cs.FetchProtocol();
            return JsonConvert.DeserializeObject(value);
        }

        [AcceptVerbs("GET", "POST")]
        public object FetchCommodity([FromBody]Commodity commodity)
        {
            if (commodity == null) return "";
            string commodityId = commodity.P_Commodity_Id;
            string commodityName = commodity.P_Commodity_Name;
            string P_Imdg_Class_Code = commodity.P_Imdg_Class_Code;
            string P_Commodity_Group_Desc = commodity.P_Commodity_Group_Desc;
            string P_Imdg_Code_Page = commodity.P_Imdg_Code_Page;
            string P_Un_No = commodity.P_Un_No;
            string SearchType = commodity.SearchType;
            bool IsActive = commodity.IsActive;
            int flag = commodity.flag;
            cls_Commodity_Mst_Tbl cs = new cls_Commodity_Mst_Tbl();
            string value = cs.FetchAll(commodityId, commodityName, P_Imdg_Class_Code, P_Imdg_Code_Page, P_Un_No, 0, "C", "COMMODITY_ID", 0, 0, "COMMODITY_ID", 1, true, 1);
            return JsonConvert.DeserializeObject(value);
        }
        [AcceptVerbs("GET", "POST")]
        public object FetchAllCommodities()
        {
            cls_Commodity_Mst_Tbl cs = new cls_Commodity_Mst_Tbl();
            string value = cs.FetchAll("", "", "", "", "", 0, "C", "COMMODITY_ID", 0, 0, "COMMODITY_ID", 1, true, 1);
            return JsonConvert.DeserializeObject(value);
        }
    }
}