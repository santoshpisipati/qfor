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
            cls_Location_Mst_Tbl clsLocation = new cls_Location_Mst_Tbl();
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
            cls_Location_Mst_Tbl clsLocation = new cls_Location_Mst_Tbl();
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

        
        
        #region Commodity

        /// <summary>
        /// Fetches the commodity.
        /// </summary>
        /// <param name="commodity">The commodity.</param>
        /// <returns></returns>
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
        #endregion Commodity

        #region CommodityGroups
        /// <summary>
        /// Fetches the commodity groups.
        /// </summary>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchCommodityGroups()
        {
            cls_Commodity_Group_Mst_Tbl cs = new cls_Commodity_Group_Mst_Tbl();
            string value = cs.FetchAllcomodity();
            return JsonConvert.DeserializeObject(value);
        }

        /// <summary>
        /// Fetches all commodities.
        /// </summary>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchAllCommodities()
        {
            cls_Commodity_Mst_Tbl cs = new cls_Commodity_Mst_Tbl();
            string value = cs.FetchAll("", "", "", "", "", 0, "C", "COMMODITY_ID", 0, 0, "COMMODITY_ID", 1, true, 1);
            return JsonConvert.DeserializeObject(value);
        }
        #endregion CommodityGroups

        #region Adminstrator

        #region Corporate Profile Setup
        /// <summary>
        /// Fetches the key contacts.
        /// </summary>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        [HttpGet]
        public object FetchKeyContacts()
        {
            string json = string.Empty;
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            strSQL.Append("SELECT E.EMPLOYEE_NAME,");
            strSQL.Append("       RLMST.ROLE_DESCRIPTION,");
            strSQL.Append("       E.PHONE_NO,");
            strSQL.Append("       E.EMAIL_ID");
            strSQL.Append("  FROM ROLE_MST_TBL RLMST, USER_MST_TBL UMT, EMPLOYEE_MST_TBL E");
            strSQL.Append(" WHERE RLMST.ROLE_MST_TBL_PK(+) = UMT.ROLE_MST_FK");
            strSQL.Append("   AND UMT.EMPLOYEE_MST_FK = E.EMPLOYEE_MST_PK");
            strSQL.Append("   AND E.KEY_CONTACT = 1");
            strSQL.Append("   ORDER BY E.EMPLOYEE_NAME");

            WorkFlow objWF = new WorkFlow();

            try
            {
                DataSet ds = objWF.GetDataSet(strSQL.ToString());
                string value = JsonConvert.SerializeObject(ds, Formatting.Indented);
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
        /// Gets all currencies.
        /// </summary>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        [HttpGet]
        public object GetAllCurrencies()
        {
            string json = string.Empty;
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();
            try
            {
                var _with1 = objWF.MyCommand.Parameters;
                _with1.Add("CURR_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                ds = objWF.GetDataSet("EXCHANGE_SCHEDULER_SETUP_PKG", "GET_CURRENCY");
                string value = JsonConvert.SerializeObject(ds, Formatting.Indented);
                return JsonConvert.DeserializeObject(value);
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        #endregion Corporate Profile Setup

        #region Location & Resposibility
        /// <summary>
        /// Fetches the protocol.
        /// </summary>
        /// <param name="protocol">The protocol.</param>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchLocationsResp([FromBody]Location location)
        {
            if (location == null) return "";
            Int64 P_Location_Mst_Pk = location.P_Location_Mst_Pk;
        string P_Location_Id = location.P_Location_Id;
            string P_Location_Name = location.P_Location_Name;
            Int64 P_LocationType = location.P_LocationType;
            string P_Rep_Location_Id = location.P_Rep_Location_Id;
            string P_Rep_Location_Name = location.P_Rep_Location_Name;
            string P_COUNTRY_Id = location.P_COUNTRY_Id;
            string P_COUNTRY_Name = location.P_COUNTRY_Name;
            string P_OFFICE_NAME = location.P_OFFICE_NAME;
            string SearchType = location.SearchType;
            string strColumnName = location.strColumnName;
            Int32 CurrentPage = location.CurrentPage;
            Int32 TotalPage = location.TotalPage;
            Int32 isActive = location.isActive;
            bool isEFS = location.isEFS;
            bool blnSortAscending = location.blnSortAscending;
            Int32 flag = location.flag;
            Int32 FromFlag = location.FromFlag;       
           
            cls_Location_Mst_Tbl cs = new cls_Location_Mst_Tbl();
            string value = cs.FetchListing(P_Location_Mst_Pk,P_Location_Id,P_Location_Name,P_LocationType,P_Rep_Location_Id,P_Rep_Location_Name,P_COUNTRY_Id,P_COUNTRY_Name,P_OFFICE_NAME,SearchType,strColumnName,CurrentPage,TotalPage,isActive,isEFS,blnSortAscending,FromFlag);
            return JsonConvert.DeserializeObject(value);
        }

        /// <summary>
        /// Fetches all protocol.
        /// </summary>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchAllLocationResp()
        {
            cls_Location_Mst_Tbl cs = new cls_Location_Mst_Tbl();
            string value = cs.FetchListing(0, "", "", 0, "", "", "", "", "", "C", "Location_Name", 0, 0, 0, false, true, 1);
            return JsonConvert.DeserializeObject(value);
        }
        #endregion 

        #region Protocol

        /// <summary>
        /// Fetches the protocol.
        /// </summary>
        /// <param name="protocol">The protocol.</param>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchProtocol([FromBody]Protocol protocol)
        {
            if (protocol == null) return "";
            Int64 ProtocolId = protocol.ProtocolPk;
            string ProtocolName = protocol.ProtocolName;
            string ProtocolValue = protocol.ProtocolValue;
            string SearchType = protocol.SearchType;
            bool IsActive = protocol.IsActive;
            int flag = protocol.flag;
            cls_Protocol_Mst_Tbl cs = new cls_Protocol_Mst_Tbl();
            string value = cs.FetchAll(ProtocolId, ProtocolName, ProtocolValue, "", "SR_NO", 0, 0, false, 0, 0, 0, 0, 0);
            return JsonConvert.DeserializeObject(value);
        }

        /// <summary>
        /// Fetches all protocol.
        /// </summary>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchAllProtocol()
        {
            cls_Protocol_Mst_Tbl cs = new cls_Protocol_Mst_Tbl();
            string value = cs.FetchAll(0, "", "", "C", "PROTOCOL_NAME", 0, 0, true, 2, -1, 3, 3, 1);
            return JsonConvert.DeserializeObject(value);
        }

        #endregion Protocol

        #region Document

        /// <summary>
        /// Fetches the document.
        /// </summary>
        /// <param name="document">The document.</param>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchDocument([FromBody]Document document)
        {
            if (document == null) return "";
            string DocumentId = document.P_Document_Id;
            Int32 DocumentName = document.P_Document_Name;
            Int32 intDocGroupFk = document.intDocGroupFk;
            string SearchType = document.SearchType;
            bool IsActive = document.IsActive;
            Int32 bizType = document.bizType;

            int flag = document.flag;
            cls_DOCUMENT_MST_TBL cs = new cls_DOCUMENT_MST_TBL();

            string value = cs.FetchAll(DocumentId, DocumentName, bizType, intDocGroupFk, 0, 0, "", "SR_NO", 0, 0, false, 0);
            return JsonConvert.DeserializeObject(value);
        }

        /// <summary>
        /// Fetches all dcoument.
        /// </summary>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchAllDcoument()
        {
            cls_DOCUMENT_MST_TBL cs = new cls_DOCUMENT_MST_TBL();
            string value = cs.FetchAll("", 0, 1, 0, 0, 1, "C", "DOCUMENT_NAME", 0, 0, true, 1);
            return JsonConvert.DeserializeObject(value);
        }

        #endregion Document

        #region Document Workflow

        /// <summary>
        /// Fetches the document workflow.
        /// </summary>
        /// <param name="workFlowRule">The work flow rule.</param>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchDocumentWorkflow([FromBody]DocWorkFlowRules workFlowRule)
        {
            if (workFlowRule == null) return "";
            string strWFRulesId = workFlowRule.strWFRulesId;
            string strDocId = workFlowRule.strDocId;
            string strEmpName = workFlowRule.strEmpName;
            string SearchType = workFlowRule.SearchType;
            bool IsActive = workFlowRule.IsActive;
            int flag = workFlowRule.flag;
            clsWORKFLOW_RULES_TRN cs = new clsWORKFLOW_RULES_TRN();

            string value = cs.FetchListing(strWFRulesId, strDocId, strEmpName, SearchType, "", 0, 0, 0, false, 0, 0, 0, "");
            return JsonConvert.DeserializeObject(value);
        }

        /// <summary>
        /// Fetches all document workflow.
        /// </summary>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchAllDocumentWorkflow()
        {
            clsWORKFLOW_RULES_TRN cs = new clsWORKFLOW_RULES_TRN();
            string value = cs.FetchListing("", "", "", "C", "DOCUMENT_NAME", 0, 1, 0, true, 0, 0, 1, "1,2,3");
            return JsonConvert.DeserializeObject(value);
        }

        #endregion Document Workflow

        #region Restrictions

        /// <summary>
        /// Fetches the restrictions.
        /// </summary>
        /// <param name="restrictions">The restrictions.</param>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchRestrictions([FromBody]Restrictions restrictions)
        {
            if (restrictions == null) return "";
            string RestrictionRefNo = restrictions.RestrictionRefNo;
            string RestrictionDate = restrictions.RestrictionDate;
            int Restrictionfk = restrictions.Restrictionfk;
            int Commoditypk = restrictions.Commoditypk;
            string IMDGCode = restrictions.IMDGCode;
            string SearchType = restrictions.SearchType;
            bool IsActive = restrictions.Active;
            int flag = restrictions.flag;
            cls_Restriction cs = new cls_Restriction();
            string value = cs.FetchAll(RestrictionRefNo, RestrictionDate, Restrictionfk, Commoditypk, SearchType, IMDGCode, "C", "RESTRICTION_PK", 0, 0, 0, "DESC", 3, 1, 0, 0);
            return JsonConvert.DeserializeObject(value);
        }

        /// <summary>
        /// Fetches all restrictions.
        /// </summary>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchAllRestrictions()
        {
            cls_Restriction cs = new cls_Restriction();
            string value = cs.FetchAll("", "", 0, 0, "", "", "C", "RESTRICTION_PK", 1, 0, 1, "DESC", 3, 1, 0, 0);
            return JsonConvert.DeserializeObject(value);
        }

        #endregion Restrictions

        #region Restrictions Approval

        /// <summary>
        /// Fetches all restrictions approval.
        /// </summary>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchAllRestrictionsApproval()
        {
            cls_Restriction cs = new cls_Restriction();
            string value = cs.FetchRestrictionApproval(0, 0, 0, 0, "", "C", "", "", 1, 0, 1);
            return JsonConvert.DeserializeObject(value);
        }

        /// <summary>
        /// Fetches the restrictions approval.
        /// </summary>
        /// <param name="restrictionsApproval">The restrictions approval.</param>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchRestrictionsApproval([FromBody]RestrictionApprovals restrictionsApproval)
        {
            if (restrictionsApproval == null) return "";
            int RestrictionTypePK = restrictionsApproval.RestrictionTypePK;
            int RestrictionDate = restrictionsApproval.ReferencePK;
            int Restrictionfk = restrictionsApproval.CustomerPK;
            short Status = restrictionsApproval.Status;
            string RestrictionMsg = restrictionsApproval.RestrictionMsg;
            string S_C = restrictionsApproval.S_C;
            string FromDate = restrictionsApproval.FromDate;
            string ToDate = restrictionsApproval.ToDate;
            short DataonLoad = restrictionsApproval.DataonLoad;
            int flag = restrictionsApproval.flag;
            cls_Restriction cs = new cls_Restriction();
            string value = cs.FetchRestrictionApproval(RestrictionTypePK, RestrictionDate, Restrictionfk, Status, RestrictionMsg, S_C, FromDate, ToDate, 0, 0, DataonLoad);
            return JsonConvert.DeserializeObject(value);
        }

        #endregion Restrictions Approval

        #region Settings

        /// <summary>
        /// Fetches the sales executives.
        /// </summary>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchSalesExecutives()
        {
            clsDESIGNATION_MST_TBL cs = new clsDESIGNATION_MST_TBL();
            string value = cs.FetchDesig();
            return JsonConvert.DeserializeObject(value);
        }

        /// <summary>
        /// Fetches the freight mis.
        /// </summary>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchFreightMIS()
        {
            clsFREIGHT_ELEMENT_MST_TBL cs = new clsFREIGHT_ELEMENT_MST_TBL();
            string value = cs.FetchFreightMIS_Parameters();
            return JsonConvert.DeserializeObject(value);
        }

        /// <summary>
        /// Fetches the freight surcharge.
        /// </summary>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchFreightSurcharge()
        {
            clsFREIGHT_ELEMENT_MST_TBL cs = new clsFREIGHT_ELEMENT_MST_TBL();
            string value = cs.FetchFreightSurcharge_Parameters();
            return JsonConvert.DeserializeObject(value);
        }

        /// <summary>
        /// Fetches the demurage.
        /// </summary>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchDemurage()
        {
            clsFREIGHT_ELEMENT_MST_TBL cs = new clsFREIGHT_ELEMENT_MST_TBL();
            string value = cs.Fetch_Demurage();
            return JsonConvert.DeserializeObject(value);
        }

        /// <summary>
        /// Fetch_s the detention.
        /// </summary>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object Fetch_Detention()
        {
            clsFREIGHT_ELEMENT_MST_TBL cs = new clsFREIGHT_ELEMENT_MST_TBL();
            string value = cs.Fetch_Detention();
            return JsonConvert.DeserializeObject(value);
        }

        /// <summary>
        /// Fetches the freight_ parameters.
        /// </summary>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchFreight_Parameters()
        {
            clsFREIGHT_ELEMENT_MST_TBL cs = new clsFREIGHT_ELEMENT_MST_TBL();
            string value = cs.FetchFreight_Parameters();
            return JsonConvert.DeserializeObject(value);
        }

        #endregion Settings

        #region User Task List

        /// <summary>
        /// Fetches the user_ task_ list.
        /// </summary>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchUser_Task_List()
        {
            cls_user_task cs = new cls_user_task();
            string value = cs.Fetch(1,0,0);
            return JsonConvert.DeserializeObject(value);
        }

        #endregion User Task List

        #region Work Flow Rules

        /// <summary>
        /// Fetches all work flow rules.
        /// </summary>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchAllWorkFlowRules()
        {
            Cls_WorkflowRulesListing cs = new Cls_WorkflowRulesListing();
            string value = cs.FetchInternal(0, "", "", "", 1, 1, 0,1);
            return JsonConvert.DeserializeObject(value);
        }

        /// <summary>
        /// Fetches the work flow rules.
        /// </summary>
        /// <param name="workFlowRules">The work flow rules.</param>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchWorkFlowRules([FromBody]WorkFlowRules workFlowRules)
        {
            if (workFlowRules == null) return "";
            int activity = workFlowRules.Activity;
            string rule = workFlowRules.Rule;
            string fdate = workFlowRules.fdate;
            string tdate = workFlowRules.tdate;
            short chk = workFlowRules.chk;
            Cls_WorkflowRulesListing cs = new Cls_WorkflowRulesListing();
            string value = cs.FetchInternal(activity, rule, fdate, tdate, chk, 0, 0);
            return JsonConvert.DeserializeObject(value);
        }

        #endregion Work Flow Rules

        #region Work Flow Tasks

        /// <summary>
        /// Fetches all work flow task.
        /// </summary>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchAllWorkFlowTask()
        {
            Cls_Taskalotment cs = new Cls_Taskalotment();
            string value = cs.Fetch("", "", 1, 0, "", false, 0,1);
            return JsonConvert.DeserializeObject(value);
        }

        /// <summary>
        /// Fetches the work flow tasks.
        /// </summary>
        /// <param name="workFlowTasks">The work flow tasks.</param>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchWorkFlowTasks([FromBody]WorkFlowTasksAllotment workFlowTasks)
        {
            if (workFlowTasks == null) return "";
            string ValidFrom = workFlowTasks.Activity;
            string ValidTo = workFlowTasks.Rule;
            string User = workFlowTasks.fdate;
            bool Status = workFlowTasks.tdate;
            bool flag = workFlowTasks.flag;
            Cls_Taskalotment cs = new Cls_Taskalotment();
            string value = cs.Fetch(ValidFrom, ValidTo, 0, 0, User, flag, 0);
            return JsonConvert.DeserializeObject(value);
        }

        #endregion Work Flow Tasks

        #region Message Center

        /// <summary>
        /// Fetches all message center.
        /// </summary>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchAllMessageCenter()
        {
            cls_User_DocMessaging cs = new cls_User_DocMessaging();
            string value = cs.fn_Messaging_Grid("NEW", 0, 0, false, 1);
            return JsonConvert.DeserializeObject(value);
        }

        /// <summary>
        /// Fetches all message center.
        /// </summary>
        /// <param name="messageCenter">The message center.</param>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchAllMessageCenter([FromBody]MessageCenter messageCenter)
        {
            if (messageCenter == null) return "";
            Int64 lbl_TranPK = messageCenter.lbl_TranPK;
            Int64 lbl_CustomerPK = messageCenter.lbl_CustomerPK;
            cls_User_DocMessaging cs = new cls_User_DocMessaging();
            string value = cs.fn_Messaging_Grid("EDIT", lbl_TranPK, lbl_CustomerPK, true);
            return JsonConvert.DeserializeObject(value);
        }

        #endregion Message Center

        #region Announcements

        /// <summary>
        /// Fetches all announcements.
        /// </summary>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchAllAnnouncements()
        {
            cls_Announcement cs = new cls_Announcement();
            string value = cs.Fetch(0, 0, 0, 0, "", "", "", "", "", "", "", "", 0, 0, "", true, "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "");
            return JsonConvert.DeserializeObject(value);
        }

        /// <summary>
        /// Fetches the announcements.
        /// </summary>
        /// <param name="announcement">The announcement.</param>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchAnnouncements([FromBody]Announcement announcement)
        {
            if (announcement == null) return "";
            Int32 status = announcement.status;
            Int32 status2 = announcement.status2;
            Int32 check = announcement.check;
            Int32 AnnType = announcement.AnnType;
            String id = announcement.id;
            String idExt = announcement.idExt;
            String annfdate = announcement.annfdate;
            String anntdate = announcement.anntdate;
            String annsub = announcement.annsub;
            String des = announcement.des;
            String annsub2 = announcement.annsub2;
            String des2 = announcement.des2;
            Int32 CurrentPage = announcement.CurrentPage;
            Int32 TotalPage = announcement.TotalPage;
            String strsort = announcement.strsort;
            bool blnsortascending = announcement.blnsortascending;
            String annfDtExt = announcement.annfDtExt;
            String annToDtExt = announcement.annToDtExt;
            String LocPKs = announcement.LocPKs;
            String DeptPKs = announcement.DeptPKs;
            String DesigPKs = announcement.DesigPKs;
            String UserPKs = announcement.UserPKs;
            String MangementPKs = announcement.MangementPKs;
            String RegionPks = announcement.RegionPks;
            String TradePKs = announcement.TradePKs;
            String POLPks = announcement.POLPks;
            String AreaPKs = announcement.AreaPKs;
            String SectorPKs = announcement.SectorPKs;
            String CountryPKs = announcement.CountryPKs;
            String PODPks = announcement.PODPks;
            String AgentPKs = announcement.AgentPKs;
            String CustomerPKS = announcement.CustomerPKS;
            String PortGrp = announcement.PortGrp;
            String CommPKs = announcement.CommPKs;

            cls_Announcement cs = new cls_Announcement();
            string value = cs.Fetch(status, status2, check, AnnType,
            id, idExt, annfdate, anntdate, annsub,
            des, annsub2, des2, CurrentPage, TotalPage, strsort, blnsortascending, annfDtExt, annToDtExt, LocPKs, DeptPKs, DesigPKs, UserPKs, MangementPKs, RegionPks, TradePKs, POLPks, AreaPKs, SectorPKs, CountryPKs, PODPks, AgentPKs, CustomerPKS, PortGrp, CommPKs);
            return JsonConvert.DeserializeObject(value);
        }

        #endregion Announcements

        #region App Fin Loc Mapping

        /// <summary>
        /// Fetches all activity logs.
        /// </summary>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchAllAppFinLoc()
        {
            cls_FinMasters cs = new cls_FinMasters();
            string value = cs.GetData(0, "", "", "", "", "", "", 0, "", 0, 0);
            return JsonConvert.DeserializeObject(value);
        }

        /// <summary>
        /// Fetches the activity logs.
        /// </summary>
        /// <param name="activityLog">The activity log.</param>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchAppFinLoc([FromBody]AppFinLoc appFinLoc)
        {
            
            if (appFinLoc == null) return "";
            Int32 FromFlag = appFinLoc.FromFlag;
            string DBName = appFinLoc.DBName;
            string ProductName = appFinLoc.ProductName;
            string LocationName = appFinLoc.LocationName;
            string Description = appFinLoc.Description;
            string fromDate = appFinLoc.fromDate;
            string Todate = appFinLoc.Todate;
            Int32 Status = appFinLoc.Status;
            string SearchType = appFinLoc.SearchType;
            Int32 TotalPage = appFinLoc.TotalPage;
            Int32 CurrentPage = appFinLoc.CurrentPage;
            cls_FinMasters cs = new cls_FinMasters();
            string value = cs.GetData(FromFlag, DBName, ProductName, LocationName, Description, fromDate, Todate, Status, SearchType, TotalPage, CurrentPage);
            return JsonConvert.DeserializeObject(value);
        }

        #endregion App Fin Loc Mapping

        #region Activity Logs

        /// <summary>
        /// Fetches all activity logs.
        /// </summary>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchAllActivityLogs()
        {
            cls_Scheduler cs = new cls_Scheduler();
            string value = cs.FetchActivityLog(0, 0, 0, 0, 0, 0, 0, "", "", "", "", "", 0);
            return JsonConvert.DeserializeObject(value);
        }

        /// <summary>
        /// Fetches the activity logs.
        /// </summary>
        /// <param name="activityLog">The activity log.</param>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchActivityLogs([FromBody]ActivityLog activityLog)
        {
            if (activityLog == null) return "";
            Int16 ChkOnLoad = activityLog.ChkOnLoad;
            Int16 SearchFlag = activityLog.SearchFlag;
            Int16 CurrentPage = activityLog.CurrentPage;
            Int16 TotalPage = activityLog.TotalPage;
            Int16 RecType = activityLog.RecType;
            Int16 Category = activityLog.Category;
            Int16 UpdateType = activityLog.UpdateType;
            String frmDt = activityLog.frmDt;
            String ToDt = activityLog.ToDt;
            String trnType = activityLog.trnType;
            String RefNr = activityLog.RefNr;
            String Desc = activityLog.Desc;
            Int32 ExportFlg = activityLog.ExportFlg;
            cls_Scheduler cs = new cls_Scheduler();
            string value = cs.FetchActivityLog(ChkOnLoad, SearchFlag, CurrentPage, TotalPage, RecType, Category, UpdateType, frmDt, ToDt, trnType, RefNr, Desc, ExportFlg);
            return JsonConvert.DeserializeObject(value);
        }

        #endregion Activity Logs

        #region Ecomm GateWays

        /// <summary>
        /// Fetches all ecomm gate ways.
        /// </summary>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchAllEcommGateWays()
        {
            cls_EcommGateWay cs = new cls_EcommGateWay();
            string value = cs.FetchEcommDetails(0, 0, 0, "", "", 0, 0, 0, "");
            return JsonConvert.DeserializeObject(value);
        }

        /// <summary>
        /// Fetches the ecomm gate ways.
        /// </summary>
        /// <param name="ecommGateWay">The ecomm gate way.</param>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchEcommGateWays([FromBody]EcommGateWays ecommGateWay)
        {
            if (ecommGateWay == null) return "";
            Int32 CurrentPage = ecommGateWay.CurrentPage;
            Int32 TotalPage = ecommGateWay.TotalPage;
            Int16 LoadFlg = ecommGateWay.LoadFlg;
            string Company = ecommGateWay.Company;
            string City = ecommGateWay.City;
            Int64 LocationFK = ecommGateWay.LocationFK;
            Int64 CountryFK = ecommGateWay.CountryFK;
            Int32 Status = ecommGateWay.Status;
            string FromDate = ecommGateWay.FromDate;

            cls_EcommGateWay cs = new cls_EcommGateWay();
            string value = cs.FetchEcommDetails(CurrentPage, TotalPage, LoadFlg, Company, City, LocationFK, CountryFK, Status, FromDate);
            return JsonConvert.DeserializeObject(value);
        }

        #endregion Ecomm GateWays

        #region Document OverFlows

        /// <summary>
        /// Fetches the document over flows.
        /// </summary>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchDocumentOverFlows()
        {
            cls_DocumentFlowDefinition cs = new cls_DocumentFlowDefinition();
            string value = cs.GetDocumentFlowDescription();
            return JsonConvert.DeserializeObject(value);
        }

        #endregion Document OverFlows

        #region Fin Generation

        /// <summary>
        /// Fetches all fin generation.
        /// </summary>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchAllFinGeneration()
        {
            cls_FinGeneration cs = new cls_FinGeneration();
            string value = cs.FetchAll(0, 0, 0, 0, "", "", 0, "", "", "", "", "", 0, 0, 0, "");
            return JsonConvert.DeserializeObject(value);
        }

        /// <summary>
        /// Fetches the fin generation.
        /// </summary>
        /// <param name="finGenerate">The fin generate.</param>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchFinGeneration([FromBody]FinGenerate finGenerate)
        {
            if (finGenerate == null) return "";
            Int32 DocType = finGenerate.DocType;
            Int32 CustPK = finGenerate.CustPK;
            Int16 CSTPK = finGenerate.CSTPK;
            Int32 RefPK = finGenerate.RefPK;
            string FromDate = finGenerate.FromDate;
            string ToDate = finGenerate.ToDate;
            Int32 DocStatus = finGenerate.DocStatus;
            string Doc_Ref_Nr = finGenerate.Doc_Ref_Nr;
            string Cust_name = finGenerate.Cust_name;
            string biztype = finGenerate.biztype;
            string processtype = finGenerate.processtype;
            string cargotype = finGenerate.cargotype;
            Int32 CurrentPage = finGenerate.CurrentPage;
            Int32 TotalPage = finGenerate.TotalPage;
            Int32 Flag1 = finGenerate.Flag1;
            string SearchType = finGenerate.SearchType;

            cls_FinGeneration cs = new cls_FinGeneration();
            string value = cs.FetchAll(DocType, CustPK, CSTPK, RefPK, FromDate, ToDate, DocStatus, Doc_Ref_Nr, Cust_name, biztype, processtype, cargotype, CurrentPage, TotalPage, Flag1, SearchType);
            return JsonConvert.DeserializeObject(value);
        }

        #endregion Fin Generation

        #endregion Adminstrator

        #region Print ExportDocs

        #region Payment Requisition

        /// <summary>
        /// Fetches all fin generation.
        /// </summary>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchAllPaymentRequisitions()
        {
            Cls_Payment_Requisition cs = new Cls_Payment_Requisition();
            string value = cs.FetchBothExpPaymentGridData("", "", 1, 0, 1, "", 0, 0);
            return JsonConvert.DeserializeObject(value);
        }

        /// <summary>
        /// Fetches the fin generation.
        /// </summary>
        /// <param name="finGenerate">The fin generate.</param>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchPaymentRequisition([FromBody]PaymentRequisition paymentRequisition)
        {
            if (paymentRequisition == null) return "";
            string JobNo = paymentRequisition.JobNo;
            string Party = paymentRequisition.Party;
            Int32 CurrentPage = paymentRequisition.CurrentPage;
            Int32 TotalPage = paymentRequisition.TotalPage;
            Int32 flag = paymentRequisition.flag;
            string invNr = paymentRequisition.invNr;
            double PageTotal = paymentRequisition.PageTotal;
            double GrandTotal = paymentRequisition.GrandTotal;

            Cls_Payment_Requisition cs = new Cls_Payment_Requisition();
            string value = cs.FetchAirExpPaymentGridData(JobNo, Party, CurrentPage, TotalPage, flag, invNr, PageTotal, GrandTotal);
            return JsonConvert.DeserializeObject(value);
        }

        #endregion Payment Requisition

        #region FMCBL

        /// <summary>
        /// Fetches all fin generation.
        /// </summary>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchAllFMCSeaUserExport()
        {
            Cls_FMCBL cs = new Cls_FMCBL();
            string value = cs.FetchFMCSeaUserExport("", 0, 0, 0, 1, 0, "", 1, 1841);
            return JsonConvert.DeserializeObject(value);
        }

        /// <summary>
        /// Fetches the fin generation.
        /// </summary>
        /// <param name="finGenerate">The fin generate.</param>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchFMCSeaUserExport([FromBody]FMCSeaUserExport seaUserExport)
        {
            if (seaUserExport == null) return "";
            string VslVoy = seaUserExport.VslVoy;
            Int64 JobPk = seaUserExport.JobPk;
            Int64 ShipperPk = seaUserExport.ShipperPk;
            Int64 HBLPk = seaUserExport.HBLPk;
            Int32 CurrentPage = seaUserExport.CurrentPage;
            Int32 TotalPage = seaUserExport.TotalPage;
            string depDate = seaUserExport.depDate;
            Int32 flag = seaUserExport.flag;
            Int32 loc = seaUserExport.loc;

            Cls_FMCBL cs = new Cls_FMCBL();
            string value = cs.FetchFMCSeaUserExport(VslVoy, JobPk, ShipperPk, HBLPk, CurrentPage, TotalPage, depDate, flag, loc);
            return JsonConvert.DeserializeObject(value);
        }

        #endregion FMCBL

        #region Export FileCover Sheet

        /// <summary>
        /// Fetches all fin generation.
        /// </summary>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchAllAirUserExportFile()
        {
            clsMAWBListing cs = new clsMAWBListing();
            string value = cs.FetchAirUserExport("", 1, 0, 1841, 1);
            return JsonConvert.DeserializeObject(value);
        }

        /// <summary>
        /// Fetches the fin generation.
        /// </summary>
        /// <param name="finGenerate">The fin generate.</param>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchAirUserExportFile([FromBody]AirUserExport seaUserExport)
        {
            if (seaUserExport == null) return "";
            string MAWBRefNo = seaUserExport.MAWBRefNo;
            Int32 CurrentPage = seaUserExport.CurrentPage;
            Int32 TotalPage = seaUserExport.TotalPage;
            Int32 loc = seaUserExport.loc;
            Int32 flag = seaUserExport.flag;

            clsMAWBListing cs = new clsMAWBListing();
            string value = cs.FetchAirUserExport(MAWBRefNo, CurrentPage, TotalPage, loc, flag);
            return JsonConvert.DeserializeObject(value);
        }

        #endregion Export FileCover Sheet

        #region Cargo Manifest

        /// <summary>
        /// Fetches all fin generation.
        /// </summary>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchAllSeaCargoManifestDataNew()
        {
            Cls_SeaCargoManifest cs = new Cls_SeaCargoManifest();
            string value = cs.FetchSeaCargoManifestDataNew(0,"","","",0,0,"",0,0,"",0,"",0,0,"","","");
            return JsonConvert.DeserializeObject(value);
        }

        /// <summary>
        /// Fetches the fin generation.
        /// </summary>
        /// <param name="finGenerate">The fin generate.</param>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchSeaCargoManifestDataNew([FromBody]SeaCargoManifest seaCargoManifest)
        {
            if (seaCargoManifest == null) return "";
            Int64 VesPK = seaCargoManifest.VesPK;
            String Ves_Flight = seaCargoManifest.Ves_Flight;
            String Voyage = seaCargoManifest.Voyage;
            String POL = seaCargoManifest.POL;
            Int64 MBLPk = seaCargoManifest.MBLPk;
            Int64 HBLPk = seaCargoManifest.HBLPk;
            String POD = seaCargoManifest.POD;
            Int32 CurrentPage = seaCargoManifest.CurrentPage;
            Int32 TotalPage = seaCargoManifest.TotalPage;
            String CargoType = seaCargoManifest.CargoType;
            Int64 CommodityType = seaCargoManifest.CommodityType;
            String Status = seaCargoManifest.Status;
            Int32 nLocationFk = seaCargoManifest.nLocationFk;
            Int32 flag = seaCargoManifest.flag;
            String Customer = seaCargoManifest.Customer;
            String Consignee = seaCargoManifest.Consignee;
            String DPAgent = seaCargoManifest.DPAgent;
            Cls_SeaCargoManifest cs = new Cls_SeaCargoManifest();
            string value = cs.FetchSeaCargoManifestDataNew(VesPK, Ves_Flight, Voyage, POL, MBLPk, HBLPk, POD, CurrentPage, TotalPage, CargoType, CommodityType, Status, nLocationFk, flag, Customer, Consignee, DPAgent);
            return JsonConvert.DeserializeObject(value);
        }

        #endregion Cargo Manifest

        #region Airline Delivery Note

        /// <summary>
        /// Fetches all fin generation.
        /// </summary>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchAllAirUserExport()
        {
            Cls_Airline_Delivery_Note cs = new Cls_Airline_Delivery_Note();
            string value = cs.FetchAirUserExport("", 0, 0, 0, 0, 1841, 1, 0, 1);
            return JsonConvert.DeserializeObject(value);
        }

        /// <summary>
        /// Fetches the fin generation.
        /// </summary>
        /// <param name="finGenerate">The fin generate.</param>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        public object FetchAirUserExport([FromBody]AirUserExportRpt seaCargoManifest)
        {
            if (seaCargoManifest == null) return "";
            String Flight = seaCargoManifest.Flight;
            Int64 JobPk = seaCargoManifest.JobPk;
            Int64 PolPk = seaCargoManifest.PolPk;
            Int64 PodPk = seaCargoManifest.PodPk;
            Int64 CustPk = seaCargoManifest.CustPk;
            Int64 strLocPk = seaCargoManifest.strLocPk;
            Int32 CurrentPage = seaCargoManifest.CurrentPage;
            Int32 TotalPage = seaCargoManifest.TotalPage;
            Int32 flag = seaCargoManifest.flag;

            Cls_Airline_Delivery_Note cs = new Cls_Airline_Delivery_Note();
            string value = cs.FetchAirUserExport(Flight, JobPk, PolPk, PodPk, CustPk, strLocPk, CurrentPage, TotalPage, flag);
            return JsonConvert.DeserializeObject(value);
        }

        #endregion Airline Delivery Note

        #endregion Print ExportDocs
    }
}