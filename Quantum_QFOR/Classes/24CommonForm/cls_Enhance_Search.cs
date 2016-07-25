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
//'*  Modified Date(DD-MON-YYYY)              Modified By                             Remarks (Bugs Related)
//'*
//'*
//'***************************************************************************************************************

#endregion "Comments"

using Oracle.DataAccess.Client;
using Oracle.DataAccess.Types;
using System;
using System.Data;
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_Enhance_Search : CommonFeatures
    {
        #region "Property"

        /// <summary>
        /// The loc
        /// </summary>
        private double Loc;

        /// <summary>
        /// Gets or sets the location.
        /// </summary>
        /// <value>
        /// The location.
        /// </value>
        public double Location
        {
            get { return Loc; }
            set { Loc = value; }
        }

        #endregion "Property"

        #region "Module level Search"

        //Add the module name in the case statements if it is new
        /// <summary>
        /// Gets the search module.
        /// </summary>
        /// <param name="strSearchModule">The string search module.</param>
        /// <param name="strSearchFlag">The string search flag.</param>
        /// <param name="strCondition">The string condition.</param>
        /// <returns></returns>
        public string getSearchModule(string strSearchModule, string strSearchFlag, string strCondition)
        {
            switch (strSearchModule)
            {
                case "COMMONSES":
                    return getCommonSearchProcedure(strSearchFlag, strCondition);

                case "COMMONSESNEW":
                    return getCommonNewSearchProcedure(strSearchFlag, strCondition);

                case "COMMONSESNEW_EXD":
                    //new package created for export doc en search
                    return getCommonNewSearchProcedure(strSearchFlag, strCondition, "", true);

                case "COMMONSESNEWDATA":
                    return getCommonNewDataSearchProcedure(strSearchFlag, strCondition);

                case "Common":
                    return getCommonSearch(strSearchFlag, strCondition);

                case "BOOKING":
                    return getBookingSearch(strSearchFlag, strCondition);

                case "General":
                    return FetchGenCommonSearch(strSearchFlag, strCondition);

                case "CUSTOMSBROKERAGE":
                    return getCOMMONCustomsBrokerage(strSearchFlag, strCondition);

                case "RFQ":
                    return getRFQSearch(strSearchFlag, strCondition);

                case "SRR":
                    return getRatingAndTariff(strSearchFlag, strCondition);

                case "DOCUMENTATION":
                    return getDocumentationSearch(strSearchFlag, strCondition);

                case "COMMONSESEXPCTRTS":
                    return getCommonExpiredSearchProcedure(strSearchFlag, strCondition);

                case "MULTIPLEES":
                    return getMultipleES(strSearchFlag, strCondition);

                case "COMMONENHANCESEARCH":
                    return FetchEnCommonSearch(strCondition);

                case "MIS":
                    return getMISSearch(strSearchFlag, strCondition);

                case "COMMONSESDOCUMENT":
                    return getCommon_DOCUMENTProcedure(strSearchFlag, strCondition);

                case "COMMONSESADMIN":
                    return getCommonSearchProcedureAdmin(strSearchFlag, strCondition);

                case "COMMONSESRATINGNTARIFF":
                    return getCommonSearchRating(strSearchFlag, strCondition);

                case "COMMONSESCBJCREPORTS":
                    return getCommonSearchCBJCReports(strSearchFlag, strCondition);

                case "COMMONSESEDIREPORTS":
                    return getCommonSearchEDIReports(strSearchFlag, strCondition);

                case "COMMONSESMIS":
                    return getCommonSearchMIS(strSearchFlag, strCondition);

                case "COMMONSESSUPPLIERMGMT":
                    return getCommonSearchSUPPLIERMGMT(strSearchFlag, strCondition);

                case "COMMONSESSRECEIVABLES":
                    return getCommonSearchReceivables(strSearchFlag, strCondition);

                case "COMMONSESSPAYABLES":
                    return getCommonSearchPayables(strSearchFlag, strCondition);

                case "COMMONSESSPRINTEXPDOCS":
                    return getCommonSearchPrintExpDocs(strSearchFlag, strCondition);

                case "COMMONSESSPRINTIMPDOCS":
                    return getCommonSearchPrintImpDocs(strSearchFlag, strCondition);

                case "COMMONSESORDERMGMT":
                    return getCommonSearchOrderMgmt(strSearchFlag, strCondition);

                case "COMMONSESREPORT":
                    return getCommonSearchReports(strSearchFlag, strCondition);

                default:
                    return "";
            }
        }

        #endregion "Module level Search"

        #region " MIS"

        /// <summary>
        /// Gets the mis search.
        /// </summary>
        /// <param name="strSearchFlag">The string search flag.</param>
        /// <param name="strCondition">The string condition.</param>
        /// <returns></returns>
        public string getMISSearch(string strSearchFlag, string strCondition)
        {
            switch (strSearchFlag)
            {
                case "REFNO_TRACKNTRACE":
                    //Reference No Selection For Track N Trace Screen

                    return FetchRefNoForTrackNTrace(strCondition);

                case "VESSEL_VOYAGE":

                    return Fetchvesselvoyagebypolandpod(strCondition);

                default:
                    return "";
            }
        }

        #endregion " MIS"

        #region "Module Search Common"

        //Add the search flag in the case statements if it is new
        /// <summary>
        /// Gets the common search.
        /// </summary>
        /// <param name="strSearchFlag">The string search flag.</param>
        /// <param name="strCondition">The string condition.</param>
        /// <returns></returns>
        public string getCommonSearch(string strSearchFlag, string strCondition)
        {
            switch (strSearchFlag)
            {
                case "VESSELID_NAME":
                    return FetchVesselIDName(strCondition);

                case "TRANSPORTER":
                    return FetchTransporter(strCondition);

                case "CUSTOMER_GROUP":
                    return FetchGroupCustomer(strCondition);

                case "LOCATION_BY_NAME_FOR_COUNTRY":
                    return FetchLocationByNameForCountry(strCondition);

                case "LOCATION_BY_NAME":
                    return FetchLocationByName(strCondition);

                case "CUSTOMER_CONSIGNEE":
                    return FetchConsigneeLookUp(strCondition);

                case "TRANSPORTER_ZONE":
                    return FetchTransporterZone(strCondition);

                case "MBL_REF_NO_IMP_LIST":
                    return FetchForMblRefInImplist(strCondition);

                case "ReAssignVOYAGE":
                    return FetchVoyageReAssignRollOver(strCondition);

                case "COST_ELEMENT_ID":
                    return FetchCostElementID(strCondition);

                case "VOYAGE_BookingRollOver":
                    return FetchVoyageForBookingRollOver(strCondition);

                case "VslVoyRollOverListing":
                    return FetchVslVoyRollListing(strCondition);

                case "EMPLOYEE":
                    return FetchEmp(strCondition);

                case "EXECUTIVE":
                    return FetchExecutive_All(strCondition);

                case "CUSTBASEDEMPLOYEE":
                    return FetchCustBasedEmp(strCondition);

                case "EMPLOYEEID":
                    return FetchEmpID(strCondition);

                case "LOCATION":
                    return FetchLocation(strCondition);

                case "USER":
                    return FetchUser(strCondition);

                case "CREDITCUSTOMER":
                    return FetchCreditCustomer(strCondition);

                case "POL":
                    return FetchPOL(strCondition);

                case "SLPOL":
                    return FetchPOL(strCondition);

                case "POLIMP":
                    return FetchPOLIMP(strCondition);

                case "POD":
                    return FetchPOD(strCondition);

                case "PODTARIFF":
                    return FetchPODTARIFF(strCondition);

                case "SLPOD":
                    return FetchPOD(strCondition);

                case "PODIMP":
                    return FetchPODIMP(strCondition);

                case "POD_NOTWORKPORT":
                    return FetchPOD_NOTWORKPORT(strCondition);

                case "OPERATOR":
                    return FetchOperator(strCondition);

                case "VESSEL_VOYAGE_OPR":
                    return FetchVesselVoyageOpr(strCondition);

                case "LOCATION_BASED_CUSTOMER":
                    return FetchLocationBasedCust(strCondition);

                case "MBL_REF_NO_LIST":
                    return FetchForMblRefInMbllist(strCondition);

                case "MBL_REF_NO_LIST_IMP":
                    return FetchForMblRefInMblimplist(strCondition);

                case "MAWB_NR":
                    return FetchMawbNr(strCondition);

                case "AGENT":
                    return FetchAgent(strCondition);

                case "AGENT_INVOICE":
                    return FetchAgentInvoice(strCondition);

                case "JOB_REF_NO":
                    return FetchForJobRef(strCondition);

                case "JOBREFNO":
                    return FetchImpCertificateJob(strCondition);

                case "CURRENCY":
                    return FetchCurrency(strCondition);

                case "TEMP_CUSTOMER":
                    return FetchTempCustomer(strCondition);

                case "CUSTOMER":
                    return FetchCustomer(strCondition);

                case "CONTAINERTYPE":
                    return FetchContainerType(strCondition);

                case "OPERATOR_FOR_ENQUIRY":
                    return Fetch_Operator_For_Enquiry(strCondition);

                case "AIRLINE_FOR_ENQUIRY":
                    return Fetch_Airline_For_Enquiry(strCondition);

                case "POLENQUIRY":
                    return FetchPOLEnquiry(strCondition);

                case "PODENQUIRY":
                    return FetchPODEnquiry(strCondition);

                case "TRADE":
                    return FetchTrade(strCondition);

                case "AIRLINE":
                    return FetchAirline(strCondition);

                case "RFQ_FOR_AIRLINE_CUSTOMER":
                    return Fetch_RFQ_For_Airline_Customer(strCondition);

                case "SRR_FOR_AIRLINE_CUSTOMER":
                    return Fetch_SRR_For_Airline_Customer(strCondition);

                case "AIRLINE_FOR_CUSTOMER":
                    return Fetch_Airline_For_Customer(strCondition);

                case "CUSTOMER_CATEGORY":
                    return FetchForShipperAndConsignee(strCondition);

                case "POLPOD_BASED_CUSTOMER":
                    return FetchPOLPODBasedCust(strCondition);

                case "LPAGENT":
                    return FetchLpAgent(strCondition);

                case "DPAGENT":
                    return FetchDpAgent(strCondition);

                case "CHA_AGENT":
                    return FetchAllCHAAgent(strCondition);

                case "VENDOR":
                    return FetchVendor(strCondition);

                case "SUPPLIER_SECSERV":
                    return FetchSupplierSecondaryServ(strCondition);

                case "COUNTRY":

                    return FetchCountry(strCondition);

                case "AIRLINEPREFIX":

                    return FetchAirlineWithPrefix(strCondition);

                case "CONTRACT":

                    return FetchContract(strCondition);

                case "TARIFF_FOR_OPERATOR":

                    return FetchTariffForOperator(strCondition);

                case "OPERATOR_FOR_CUSTOMER":

                    return Fetch_Operator_For_Customer(strCondition);

                case "PORTCOUNTRYTRADE":
                    return FetchPortCountryTRADE(strCondition);

                case "TRADECOUNTRY":

                    return FetchTradeCountry(strCondition);

                case "TARIFF_FOR_AIRLINE":

                    return FetchTariffForAirline(strCondition);

                case "PercentageCommCheck":
                    return FecthFreightElement(strCondition);

                case "TCPORT":

                    return FetchTCPORT(strCondition);

                case "COMMODITY_FOR_GROUP":

                    return Fetch_Commodity_For_Group(strCondition);

                case "MASTER_JC_SEA":

                    return FetchMasterJobCardSea(strCondition);

                case "MASTER_JC_AIR":

                    return FetchMasterJobCardAir(strCondition);

                case "POL_WORKPORT":

                    return FetchPOL_WORKINGPORTS(strCondition);

                case "JOB_REF_PAYREQ":

                    return FETCH_JOB_REF_PAYREQ(strCondition);

                case "VENDOREXP_NAME":

                    return FetchExpVendorName(strCondition);

                case "FMCVESSELEXP":

                    return FetchForFMCVESSEL(strCondition);

                case "FMCHBLEXP":

                    return FetchForHblRefFMC(strCondition);

                case "MAWB_JOB_REF_NO":

                    return FetchForMAWBJobRef(strCondition);

                case "JOB_REF_NO_IMP_DOLIST":

                    return FetchJobRefNo_Imp_For_DOList(strCondition);

                case "JOB_REF_NO_IMP_DO":

                    return FetchJobRefNo_Imp_For_DO(strCondition);

                case "JOB_REF_NO_IMP_PM":

                    return FetchJobRefNo_Imp_For_PrintManager(strCondition);

                case "AIRLINE_FLIGHTNAME":

                    return FetchAIRLINEFLIGHT(strCondition);

                case "Depo_WISE":

                    return FetchDepo_Wise(strCondition);

                case "MBL_REF_NO_JOBCARD_IMP":

                    return FetchMBLForJobCardImp(strCondition);
                ///''''''''''''''''
                case "MBL_REF_NO_JOBCARD_INVOICE":

                    return FetchMBLForJobCardImpInvoice(strCondition);
                ///''''''''''''''''
                case "MBL_REF_NO_LIST_NEW":

                    return FetchForMblRefImp(strCondition);

                case "MBL_REF_NO_JOBCARD":

                    return FetchMBLForJobCard(strCondition);

                case "VESSEL_VOYAGE_INV_CB":

                    return FetchVesselVoyageForInvCB(strCondition);

                case "MASTERFORMS":

                    return FetchMastersForm(strCondition);

                case "VESSEL_VOYAGE_IMP_CB":

                    return FetchVesselVoyageImpCP(strCondition);

                case "INV_FLEIGHT":

                    return FetchFleightForInvDP(strCondition);

                case "VOYAGE_JOBCARD":

                    return FetchVoyageForJobCard(strCondition);

                case "VOYAGE_JOBCARD_IMP_NEW":

                    return FetchVoyageForJobCardNew(strCondition);

                case "VOYAGE_JOBCARD_IMP":

                    return FetchVoyageForJobCard(strCondition);

                case "AGENTCOMMON":

                    return FetchAllAgents(strCondition);

                case "EXPORTAR":

                    return FetchExportAR(strCondition);

                case "EXPORTAP":

                    return FetchExportAP(strCondition);

                case "VESSEL_VOYAGE":

                    return FetchVesselVoyage(strCondition);

                case "HBL_REF_NO_JOBCARDS":

                    return FetchHBLForJobCardS(strCondition);

                case "HBL_REF_NO":

                    return FetchForHblRef(strCondition);

                case "HBL_REF_NO_MAWB":

                    return FetchForHblRefMAWB(strCondition);

                case "GET_MBL_REF_JOBCARD_EXP":

                    return FetchMBLForJobCardExp(strCondition);

                case "NEW CURRENCY":

                    return FetchNewCurrency(strCondition);

                case "RFQ_FOR_OPERATOR_CUSTOMER":
                    return Fetch_RFQ_For_Operator_Customer(strCondition);

                case "JOB_VENDOR":

                    return FetchJobVendors(strCondition);

                case "VESSEL_FLIGHT":

                    return FetchVslFlight(strCondition);

                case "MBL_REF_NO_LIST_SUPPINV":

                    return FetchForMblRefInSuppInv(strCondition);

                case "OPERATOR_AIRLINE":

                    return FetchOperatorAirLine(strCondition);

                case "OPERATOR_WF":

                    return FetchOperator_WF(strCondition);

                case "HBL_REF_NO_IMP_LIST":

                    return FetchForHblRefInImplist(strCondition);

                case "SHP_REF_NO_IMP_LIST":

                    return FetchForSHPRefInImplist(strCondition);

                case "CNS_REF_NO_IMP_LIST":

                    return FetchForCNSRefInImplist(strCondition);

                case "AGENTCOMM":

                    return FetchAgentComm(strCondition);

                case "AIRLINE_FOR_EXP_MANIFEST":

                    return Fetch_Airline_For_Enquiry_EXP_Manifest(strCondition);

                case "VESSELSRPT":

                    return FetchVesselsRpt(strCondition);

                case "VOYAGE":

                    return FetchVoyage(strCondition);

                case "JOB_REF_FOR_MBL":

                    return FetchJobInMblEntry(strCondition);

                case "JOB_REF_FOR_BBMBL":

                    return FetchJobInBBMblEntry(strCondition);

                case "HBL_REF_FOR_MBL":

                    return FetchForHblRefInMbl(strCondition);

                case "MJC_REF_FOR_MBL":

                    return FetchMJCInMblEntry(strCondition);

                case "CUSTOMER_TEMP":

                    return FetchCustomer_Temp(strCondition);

                case "JOBCARD_CUSTOMS":

                    return FetchJobcard(strCondition);

                case "TRANSPORT_JC":

                    return FetchAllJobcard(strCondition);

                case "WORKFLOW_MANAGER":

                    return InsertionToUserTaskListTable(strCondition);

                case "NEWVENDOR":

                    return FetchNewVendor(strCondition);

                case "COMMODITY":

                    return FetchCommodity(strCondition);

                case "BBHBL_JOB_REF_NO":

                    return FetchForJobBBHblRef(strCondition);

                case "EMAILLOG_DOCREF":

                    return FetchDocRef(strCondition);

                case "EMAILLOG_DOCTYPE":

                    return FetchDocType(strCondition);

                case "EMAILLOG_SENDTO":

                    return FetchSendTo(strCondition);

                case "EMAILLOG_SENDFROM":

                    return FetchSendFrom(strCondition);

                default:
                    return "";
            }
        }

        #endregion "Module Search Common"

        #region "FetchFreightElement"

        /// <summary>
        /// Fecthes the freight element.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FecthFreightElement(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            DataSet objds = new DataSet();
            int Int_I = 0;
            string strReturn = "";
            try
            {
                objds = objWF.GetDataSet((strCond.Split('~'))[1]);
                if (objds.Tables[0].Rows.Count > 0)
                {
                    for (Int_I = 0; Int_I <= objds.Tables[0].Rows.Count - 1; Int_I++)
                    {
                        strReturn += objds.Tables[0].Rows[Int_I][0] + ",";
                    }
                }
                strReturn = strReturn.TrimEnd(Convert.ToChar(','));
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return strReturn;
        }

        #endregion "FetchFreightElement"

        #region "Supplier Management :- Common "

        /// <summary>
        /// Fetches the vendor.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchVendor(string strCond)
        {
            cls_VendorListing objClass = new cls_VendorListing();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchVendor(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the supplier secondary serv.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchSupplierSecondaryServ(string strCond)
        {
            cls_VendorListing objClass = new cls_VendorListing();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchSupplierSecondaryServ(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Insertions to user task list table.
        /// </summary>
        /// <param name="strcond">The strcond.</param>
        /// <returns></returns>
        public string InsertionToUserTaskListTable(string strcond)
        {
            Cls_Workflow_Mgr_Report objclass = new Cls_Workflow_Mgr_Report();
            string strreturn = null;
            try
            {
                strreturn = objclass.InsertToUserTaskListTable(strcond);
                return strreturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Supplier Management :- Common "

        #region " Documentation "

        /// <summary>
        /// Gets the documentation search.
        /// </summary>
        /// <param name="strSearchFlag">The string search flag.</param>
        /// <param name="strCondition">The string condition.</param>
        /// <returns></returns>
        public string getDocumentationSearch(string strSearchFlag, string strCondition)
        {
            switch (strSearchFlag)
            {
                case "JOB_CARD_MAWB":

                    return FetchForJobRefMAWB(strCondition);

                case "JOB_REF_NO":

                    return FetchForJobRefDetention(strSearchFlag, strCondition);

                case "DETENTION_CALC_REF":

                    return FetchDetCalcRef(strCondition);

                case "WARE_HOUSE":

                    return FetchWarehouse(strCondition);

                case "JOB_REF_NO_DET":

                    return FetchForJobRefDetention(strSearchFlag, strCondition);

                case "GET_ACT_JOB_FOR_EXP_INV_NEW":
                    //To Display Jobacard Date also
                    cls_JobCardSearch objClass = new cls_JobCardSearch();
                    return FetchActiveJobCardHBLMBLNew(strCondition);

                case "IMP_JOB_REF_NO_INV_LIST_NEW":
                    //'Fetching Jobacrds for Invoice agent Sea(Imp) Listing

                    return FetchActiveJobCardAgentImport(strCondition);

                case "JOB_REF_NO_ACTIVE_HBL_MBL":
                    //Priya-Active job cards

                    return FetchActiveJobCardHBLMBL(strCondition);

                case "IMP_JOB_REF_NO_ACTIVE":
                    //Vimlesh-imports Active job cards

                    return FetchActiveIMPJobCard(strCondition);

                case "JOB_REF_NO_INVOICE_AGENT_AIR":
                    //Booking Details

                    return FetchInvoiceAgentJCNo(strCondition);

                case "IMP_JOB_REF_NO_INV_LIST":

                    return FetchActiveJobCardForImport(strCondition);

                case "BATCHNO":

                    return FetchForBATCHNo(strCondition);

                case "JOB_REF_NO_AIR_CON":

                    return FetchMasterJobCardAirCON(strCondition);

                case "FLIGHTNO":
                    //Existing Flight No from HAWB

                    return FetchFlightNo(strCondition);

                case "BATCHNOAIR":

                    return FetchForBATCHNoAir(strCondition);

                case "CR_INV_AGENT_NO_IMP":
                    return FetchInvoiceNoForCredit(strCondition);

                case "CR_INV_AGENT_NO":
                    return FetchInvoiceNoForCredit(strCondition);

                case "COLLECTION_REF_NO":

                    return Fetch_Collection_RefNr(strCondition);

                default:
                    return "";
            }
        }

        #endregion " Documentation "

        #region "Documentation"

        /// <summary>
        /// Fetches the consignee look up.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchConsigneeLookUp(string strCond)
        {
            cls_JobCardSearch clsJcSearch = new cls_JobCardSearch();
            string strReturn = null;
            try
            {
                strReturn = clsJcSearch.FetchConsigneeLookUp(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches for shipper and consignee HBL.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchForShipperAndConsigneeHBL(string strCond)
        {
            cls_HBL_Entry clsHble = new cls_HBL_Entry();
            string strReturn = null;
            try
            {
                strReturn = clsHble.FetchForShipperAndConsigneeHBL(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches for shipper and consignee.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchForShipperAndConsignee(string strCond)
        {
            cls_JobCardSearch objClass = new cls_JobCardSearch();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchForShipperAndConsignee(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //Fetch Load Port Agent
        /// <summary>
        /// Fetches the lp agent.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchLpAgent(string strCond)
        {
            cls_Agent_Details objClass = new cls_Agent_Details();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchLpAgent(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the dp agent.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchDpAgent(string strCond)
        {
            cls_Agent_Details objClass = new cls_Agent_Details();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchDpAgent(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches for job reference detention.
        /// </summary>
        /// <param name="strSearchFlag">The string search flag.</param>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchForJobRefDetention(string strSearchFlag, string strCond)
        {
            cls_DetentionCalculation objClass = new cls_DetentionCalculation();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchForJobRefDetention(strSearchFlag, strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the det calculate reference.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchDetCalcRef(string strCond)
        {
            cls_DetentionCalculation objClass = new cls_DetentionCalculation();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchDetentionRef(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the warehouse.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchWarehouse(string strCond)
        {
            cls_DetentionCalculation objClass = new cls_DetentionCalculation();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchWarehouse(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the master job card air.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchMasterJobCardAir(string strCond)
        {
            cls_JobCard objClass = new cls_JobCard();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchMasterJobCardAir(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the master job card sea.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchMasterJobCardSea(string strCond)
        {
            cls_JobCard objClass = new cls_JobCard();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchMasterJobCardSea(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the po l_ workingports.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchPOL_WORKINGPORTS(string strCond)
        {
            clsPORT_MST_TBL objClass = new clsPORT_MST_TBL();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchPOL_WORKINGPORTS(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the depo_ wise.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchDepo_Wise(string strCond)
        {
            cls_ContainerOnHireListing objClass = new cls_ContainerOnHireListing();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchDepo(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the active job card agent import.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchActiveJobCardAgentImport(string strCond)
        {
            cls_InvoiceAgentSeaImpList objClass = new cls_InvoiceAgentSeaImpList();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchActiveJobAgentImp(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the active imp job card.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchActiveIMPJobCard(string strCond)
        {
            cls_InvoiceAgentSeaImpList objClass = new cls_InvoiceAgentSeaImpList();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchJobCardImportForInvoice(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the invoice agent jc no.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchInvoiceAgentJCNo(string strCond)
        {
            clsInvoiceAgentEntryAir objClass = new clsInvoiceAgentEntryAir();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchInvoiceAgentJCNo(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the active job card for import.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchActiveJobCardForImport(string strCond)
        {
            cls_InvoiceAgentSeaImpList objClass = new cls_InvoiceAgentSeaImpList();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchActiveJobCard(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches for batch no.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchForBATCHNo(string strCond)
        {
            cls_CSVInterfaceSAGESea objClass = new cls_CSVInterfaceSAGESea();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchForBATCHNo(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the master job card air con.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchMasterJobCardAirCON(string strCond)
        {
            cls_MSTJobCardAir objClass = new cls_MSTJobCardAir();
            //fetching for consolidation
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchMasterJobCardAirCON(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the flight no.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchFlightNo(string strCond)
        {
            cls_ExportPreAlertsAir objClass = new cls_ExportPreAlertsAir();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchFlightNo(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches for batch no air.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchForBATCHNoAir(string strCond)
        {
            cls_CSVInterfaceSAGEAir objClass = new cls_CSVInterfaceSAGEAir();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchForBATCHNoAir(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the invoice no for credit.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchInvoiceNoForCredit(string strCond)
        {
            cls_CreditNoteToAgentAirImpEntry objClass = new cls_CreditNoteToAgentAirImpEntry();
            string strReturn = null;
            try
            {
                //strReturn = objClass.FetchInvoiceNoForCredit(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetch_s the collection_ reference nr.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string Fetch_Collection_RefNr(string strCond)
        {
            cls_Collectionlist objClass = new cls_Collectionlist();
            string strReturn = null;
            try
            {
                strReturn = objClass.Fetch_Collection_RefNr(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the agent comm.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchAgentComm(string strCond)
        {
            cls_Agent_Details objClass = new cls_Agent_Details();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchAgentComm(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches for job bb HBL reference.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchForJobBBHblRef(string strCond)
        {
            cls_BBHBL_Entry objClass = new cls_BBHBL_Entry();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchForJobBBHblRef(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Documentation"

        #region " Booking "

        /// <summary>
        /// Gets the booking search.
        /// </summary>
        /// <param name="strSearchFlag">The string search flag.</param>
        /// <param name="strCondition">The string condition.</param>
        /// <returns></returns>
        public string getBookingSearch(string strSearchFlag, string strCondition)
        {
            switch (strSearchFlag)
            {
                case "CUSTOMER_CATEGORY_ADDRESS":

                    return FetchCustomerCategoryAddress(strCondition);

                case "CUSTOMSSTATUSCODE":

                    return FetchForCustomsSCode(strCondition);

                case "PACKTYPE":
                    //Fetch the pack types

                    return FetchForPackType(strCondition);

                case "PLACE":
                    //Place from the place master

                    return FetchForPlace(strCondition);

                case "ENQUIRY_BKG":

                    return FetchBkgEnqSearch(strCondition);

                case "QUOTATIONNO":

                    return FetchForQuotationNo(strCondition);

                case "VES_VOY_BOOKING":

                    return FetchVesVoyBooking(strCondition);

                case "ENQUIRY_SEA":
                    // Rajesh (25-Jan-2006) Used in Quotation Sea

                    return FetchEnquiryRefNo(strCondition);

                case "ENQUIRY_AIR":
                    // Rajesh (25-Jan-2006) Used in Quotation Sea

                    return FetchEnquiryRefNoAir(strCondition);

                case "VESSELVOYAGE":

                    return FetchVesVoySplitBooking(strCondition);

                case "VESVOY_BOOKING":

                    return FetchVesVoy_Booking(strCondition);

                case "PAYTYPE":

                    return FetchForIncoTermsPayType(strCondition);

                case "QUOTATIONNOEXPIRY":

                    return FetchForQuotationNoExpiry(strCondition);

                case "NEWQUOTATIONNO":

                    return FetchForNewQuotationNo(strCondition);

                case "BOOKINGNR":

                    return FetchBookingNrSplitBooking(strCondition);

                default:
                    return "";
            }
        }

        #endregion " Booking "

        #region "getCommonSearchProcedure"

        /// <summary>
        /// Gets the common search procedure.
        /// </summary>
        /// <param name="strSearchFlag">The string search flag.</param>
        /// <param name="strCondition">The string condition.</param>
        /// <param name="strChk">The string CHK.</param>
        /// <returns></returns>
        public string getCommonSearchProcedure(string strSearchFlag, string strCondition, string strChk = "")
        {
            return FetchCOMMONDATA(strCondition, strChk);
        }

        /// <summary>
        /// Fetches the commondata.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <param name="strChk">The string CHK.</param>
        /// <returns></returns>
        public string FetchCOMMONDATA(string strCond, string strChk = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string C = null;
            string C1 = null;
            string C3 = null;
            string C4 = null;
            string C5 = null;
            string C6 = null;
            string C7 = null;
            string C8 = null;
            string C9 = null;
            string C10 = null;
            string C11 = null;
            string C12 = null;
            string C13 = null;
            string C14 = null;
            string C15 = null;
            string User_pk = null;
            string strSearchFlag_IN = null;
            string strFromToType = "";
            var strNull = "";
            string SelectedLoc = null;
            string strLOOKUP_IN = null;
            arr = strCond.Split('~');
            // lOOK UP VALUE E OR L
            strLOOKUP_IN = Convert.ToString(arr.GetValue(0));
            //USER TYPE TEXT WHILE PRESSING THE KEY
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (strSERACH_IN.IndexOf("$") != -1)
            {
                strSERACH_IN = strSERACH_IN.Replace("$", "&");
            }
            strSERACH_IN = strSERACH_IN.TrimEnd(',');
            User_pk = Convert.ToString(HttpContext.Current.Session["USER_PK"]);
            //FLAG FOR ES FOR WHICH FETCHING THE RECORDS
            strSearchFlag_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
            {
                C3 = Convert.ToString(arr.GetValue(3));
            }
            if (arr.Length > 4)
            {
                C4 = Convert.ToString(arr.GetValue(4));
            }
            if (arr.Length > 5)
            {
                C5 = Convert.ToString(arr.GetValue(5));
            }
            if (arr.Length > 6)
            {
                C6 = Convert.ToString(arr.GetValue(6));
            }
            if (arr.Length > 7)
            {
                C7 = Convert.ToString(arr.GetValue(7));
            }
            if (arr.Length > 8)
            {
                C8 = Convert.ToString(arr.GetValue(8));
            }
            if (arr.Length > 9)
            {
                C9 = Convert.ToString(arr.GetValue(9));
            }
            if (arr.Length > 10)
            {
                C10 = Convert.ToString(arr.GetValue(10));
            }
            if (arr.Length > 11)
            {
                C11 = Convert.ToString(arr.GetValue(11));
            }
            if (arr.Length > 12)
            {
                C12 = Convert.ToString(arr.GetValue(12));
            }
            if (arr.Length > 13)
            {
                C13 = Convert.ToString(arr.GetValue(13));
            }
            if (arr.Length > 14)
            {
                C14 = Convert.ToString(arr.GetValue(14));
            }
            if (arr.Length > 15)
            {
                C15 = Convert.ToString(arr.GetValue(15));
            }

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".GET_EN_COMMON_PKG.GET_COMMON_NEW";

                var _with1 = selectCommand.Parameters;
                _with1.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with1.Add("LOOKUP_VALUE_IN", strLOOKUP_IN).Direction = ParameterDirection.Input;
                _with1.Add("SearchFlag", (string.IsNullOrEmpty(strSearchFlag_IN) ? strNull : strSearchFlag_IN)).Direction = ParameterDirection.Input;
                _with1.Add("USER_PK_IN", (string.IsNullOrEmpty(User_pk) ? strNull : User_pk)).Direction = ParameterDirection.Input;
                _with1.Add("PARAM3_IN", (string.IsNullOrEmpty(C3) ? strNull : C3)).Direction = ParameterDirection.Input;
                _with1.Add("PARAM4_IN", (string.IsNullOrEmpty(C4) ? strNull : C4)).Direction = ParameterDirection.Input;
                _with1.Add("PARAM5_IN", (string.IsNullOrEmpty(C5) ? strNull : C5)).Direction = ParameterDirection.Input;
                _with1.Add("PARAM6_IN", (string.IsNullOrEmpty(C6) ? strNull : C6)).Direction = ParameterDirection.Input;
                _with1.Add("PARAM7_IN", (string.IsNullOrEmpty(C7) ? strNull : C7)).Direction = ParameterDirection.Input;
                _with1.Add("PARAM8_IN", (string.IsNullOrEmpty(C8) ? strNull : C8)).Direction = ParameterDirection.Input;
                _with1.Add("PARAM9_IN", (string.IsNullOrEmpty(C9) ? strNull : C9)).Direction = ParameterDirection.Input;
                _with1.Add("PARAM10_IN", (string.IsNullOrEmpty(C10) ? strNull : C10)).Direction = ParameterDirection.Input;
                _with1.Add("PARAM11_IN", (string.IsNullOrEmpty(C11) ? strNull : C11)).Direction = ParameterDirection.Input;
                _with1.Add("PARAM12_IN", (string.IsNullOrEmpty(C12) ? strNull : C12)).Direction = ParameterDirection.Input;
                _with1.Add("PARAM13_IN", (string.IsNullOrEmpty(C13) ? strNull : C13)).Direction = ParameterDirection.Input;
                _with1.Add("PARAM14_IN", (string.IsNullOrEmpty(C14) ? strNull : C14)).Direction = ParameterDirection.Input;
                _with1.Add("PARAM15_IN", (string.IsNullOrEmpty(C15) ? strNull : C15)).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                OracleClob clob = null;
                clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        #endregion "getCommonSearchProcedure"

        #region "getCommonNewDataSearchProcedure"

        /// <summary>
        /// Gets the common new data search procedure.
        /// </summary>
        /// <param name="strSearchFlag">The string search flag.</param>
        /// <param name="strCondition">The string condition.</param>
        /// <param name="strChk">The string CHK.</param>
        /// <returns></returns>
        public string getCommonNewDataSearchProcedure(string strSearchFlag, string strCondition, string strChk = "")
        {
            return FetchCOMMONNEWDATA(strCondition, strChk);
        }

        /// <summary>
        /// Fetches the commonnewdata.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <param name="strChk">The string CHK.</param>
        /// <returns></returns>
        public string FetchCOMMONNEWDATA(string strCond, string strChk = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string C = null;
            string C1 = null;
            string C3 = null;
            string C4 = null;
            string C5 = null;
            string C6 = null;
            string C7 = null;
            string C8 = null;
            string C9 = null;
            string C10 = null;
            string C11 = null;
            string C12 = null;
            string C13 = null;
            string C14 = null;
            string C15 = null;
            string C16 = null;
            string C17 = null;
            string C18 = null;
            string C19 = null;
            string C20 = null;
            string C21 = null;
            string C22 = null;
            string C23 = null;
            string User_pk = null;
            string strSearchFlag_IN = null;
            string strFromToType = "";
            var strNull = "";
            string SelectedLoc = null;
            string strLOOKUP_IN = null;
            arr = strCond.Split('~');
            //***********************
            // lOOK UP VALUE E OR L
            strLOOKUP_IN = Convert.ToString(arr.GetValue(0));
            //*******************

            //*************************
            //USER TYPE TEXT WHILE PRESSING THE KEY
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (strSERACH_IN.IndexOf("$") != -1)
            {
                strSERACH_IN = strSERACH_IN.Replace("$", "&");
            }
            strSERACH_IN = strSERACH_IN.TrimEnd(',');
            //*************************
            //***********************
            User_pk = Convert.ToString(HttpContext.Current.Session["USER_PK"]);
            //FLAG FOR ES FOR WHICH FETCHING THE RECORDS
            strSearchFlag_IN = Convert.ToString(arr.GetValue(2));
            //*********************
            if (arr.Length > 3)
            {
                C3 = Convert.ToString(arr.GetValue(3));
            }
            if (arr.Length > 4)
            {
                C4 = Convert.ToString(arr.GetValue(4));
            }
            if (arr.Length > 5)
            {
                C5 = Convert.ToString(arr.GetValue(5));
            }
            if (arr.Length > 6)
            {
                C6 = Convert.ToString(arr.GetValue(6));
            }
            if (arr.Length > 7)
            {
                C7 = Convert.ToString(arr.GetValue(7));
            }
            if (arr.Length > 8)
            {
                C8 = Convert.ToString(arr.GetValue(8));
            }
            if (arr.Length > 9)
            {
                C9 = Convert.ToString(arr.GetValue(9));
            }
            if (arr.Length > 10)
            {
                C10 = Convert.ToString(arr.GetValue(10));
            }
            if (arr.Length > 11)
            {
                C11 = Convert.ToString(arr.GetValue(11));
            }
            if (arr.Length > 12)
            {
                C12 = Convert.ToString(arr.GetValue(12));
            }
            if (arr.Length > 13)
            {
                C13 = Convert.ToString(arr.GetValue(13));
            }
            if (arr.Length > 14)
            {
                C14 = Convert.ToString(arr.GetValue(14));
            }
            if (arr.Length > 15)
            {
                C15 = Convert.ToString(arr.GetValue(15));
            }
            if (arr.Length > 16)
            {
                C16 = Convert.ToString(arr.GetValue(16));
            }
            if (arr.Length > 17)
            {
                C17 = Convert.ToString(arr.GetValue(17));
            }
            if (arr.Length > 18)
            {
                C18 = Convert.ToString(arr.GetValue(18));
            }
            if (arr.Length > 19)
            {
                C19 = Convert.ToString(arr.GetValue(19));
            }
            if (arr.Length > 20)
            {
                C20 = Convert.ToString(arr.GetValue(20));
            }
            if (arr.Length > 21)
            {
                C21 = Convert.ToString(arr.GetValue(21));
            }
            if (arr.Length > 22)
            {
                C22 = Convert.ToString(arr.GetValue(22));
            }
            if (arr.Length > 23)
            {
                C23 = Convert.ToString(arr.GetValue(23));
            }
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".GET_EN_COMMON_PKG_NEW_DATA.GET_COMMON_NEW";

                var _with2 = selectCommand.Parameters;
                _with2.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with2.Add("LOOKUP_VALUE_IN", strLOOKUP_IN).Direction = ParameterDirection.Input;
                _with2.Add("SearchFlag", (string.IsNullOrEmpty(strSearchFlag_IN) ? strNull : strSearchFlag_IN)).Direction = ParameterDirection.Input;
                _with2.Add("USER_PK_IN", (string.IsNullOrEmpty(User_pk) ? strNull : User_pk)).Direction = ParameterDirection.Input;
                _with2.Add("PARAM3_IN", (string.IsNullOrEmpty(C3) ? strNull : C3)).Direction = ParameterDirection.Input;
                _with2.Add("PARAM4_IN", (string.IsNullOrEmpty(C4) ? strNull : C4)).Direction = ParameterDirection.Input;
                _with2.Add("PARAM5_IN", (string.IsNullOrEmpty(C5) ? strNull : C5)).Direction = ParameterDirection.Input;
                _with2.Add("PARAM6_IN", (string.IsNullOrEmpty(C6) ? strNull : C6)).Direction = ParameterDirection.Input;
                _with2.Add("PARAM7_IN", (string.IsNullOrEmpty(C7) ? strNull : C7)).Direction = ParameterDirection.Input;
                _with2.Add("PARAM8_IN", (string.IsNullOrEmpty(C8) ? strNull : C8)).Direction = ParameterDirection.Input;
                _with2.Add("PARAM9_IN", (string.IsNullOrEmpty(C9) ? strNull : C9)).Direction = ParameterDirection.Input;
                _with2.Add("PARAM10_IN", (string.IsNullOrEmpty(C10) ? strNull : C10)).Direction = ParameterDirection.Input;
                _with2.Add("PARAM11_IN", (string.IsNullOrEmpty(C11) ? strNull : C11)).Direction = ParameterDirection.Input;
                _with2.Add("PARAM12_IN", (string.IsNullOrEmpty(C12) ? strNull : C12)).Direction = ParameterDirection.Input;
                _with2.Add("PARAM13_IN", (string.IsNullOrEmpty(C13) ? strNull : C13)).Direction = ParameterDirection.Input;
                _with2.Add("PARAM14_IN", (string.IsNullOrEmpty(C14) ? strNull : C14)).Direction = ParameterDirection.Input;
                _with2.Add("PARAM15_IN", (string.IsNullOrEmpty(C15) ? strNull : C15)).Direction = ParameterDirection.Input;
                _with2.Add("PARAM16_IN", (string.IsNullOrEmpty(C16) ? strNull : C16)).Direction = ParameterDirection.Input;
                _with2.Add("PARAM17_IN", (string.IsNullOrEmpty(C17) ? strNull : C17)).Direction = ParameterDirection.Input;
                _with2.Add("PARAM18_IN", (string.IsNullOrEmpty(C18) ? strNull : C18)).Direction = ParameterDirection.Input;
                _with2.Add("PARAM19_IN", (string.IsNullOrEmpty(C19) ? strNull : C19)).Direction = ParameterDirection.Input;
                _with2.Add("PARAM20_IN", (string.IsNullOrEmpty(C20) ? strNull : C20)).Direction = ParameterDirection.Input;
                _with2.Add("PARAM21_IN", (string.IsNullOrEmpty(C21) ? strNull : C21)).Direction = ParameterDirection.Input;
                _with2.Add("PARAM22_IN", (string.IsNullOrEmpty(C22) ? strNull : C22)).Direction = ParameterDirection.Input;
                _with2.Add("PARAM23_IN", (string.IsNullOrEmpty(C23) ? strNull : C23)).Direction = ParameterDirection.Input;
                _with2.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                OracleClob clob = null;
                clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        //For Common Multiple Enhance Search : Before Changing kindly discuss
        /// <summary>
        /// Fetches the multiple es.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <param name="strChk">The string CHK.</param>
        /// <returns></returns>
        public string FetchMultipleES(string strCond, string strChk = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string C = null;
            string C1 = null;
            string C3 = null;
            string C4 = null;
            string C5 = null;
            string C6 = null;
            string C7 = null;
            string C8 = null;
            string C9 = null;
            string C10 = null;
            string C11 = null;
            string C12 = null;
            string User_pk = null;
            string strSearchFlag_IN = null;
            string strFromToType = "";
            var strNull = "";
            string SelectedLoc = null;
            string strLOOKUP_IN = null;
            arr = strCond.Split('~');
            //***********************
            // lOOK UP VALUE E OR L
            strLOOKUP_IN = Convert.ToString(arr.GetValue(0));
            //*******************

            //*************************
            //USER TYPE TEXT WHILE PRESSING THE KEY
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (strSERACH_IN.IndexOf("$") != -1)
            {
                strSERACH_IN = strSERACH_IN.Replace("$", "&");
            }
            strSERACH_IN = strSERACH_IN.TrimEnd(',');
            //*************************
            //***********************
            User_pk = Convert.ToString(HttpContext.Current.Session["USER_PK"]);
            //FLAG FOR ES FOR WHICH FETCHING THE RECORDS
            strSearchFlag_IN = Convert.ToString(arr.GetValue(2));
            //*********************
            if (arr.Length > 3)
            {
                C3 = Convert.ToString(arr.GetValue(3));
            }
            if (arr.Length > 4)
            {
                C4 = Convert.ToString(arr.GetValue(4));
            }
            if (arr.Length > 5)
            {
                C5 = Convert.ToString(arr.GetValue(5));
            }
            if (arr.Length > 6)
            {
                C6 = Convert.ToString(arr.GetValue(6));
            }
            if (arr.Length > 7)
            {
                C7 = Convert.ToString(arr.GetValue(7));
            }
            if (arr.Length > 8)
            {
                C8 = Convert.ToString(arr.GetValue(8));
            }
            if (arr.Length > 9)
            {
                C9 = Convert.ToString(arr.GetValue(9));
            }
            if (arr.Length > 10)
            {
                C10 = Convert.ToString(arr.GetValue(10));
            }
            if (arr.Length > 11)
            {
                C11 = Convert.ToString(arr.GetValue(11));
            }
            if (arr.Length > 12)
            {
                C12 = Convert.ToString(arr.GetValue(12));
            }

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".GET_EN_MULTIPLE_PKG.EN_MULTIPLE_PKG";
                var _with3 = selectCommand.Parameters;
                _with3.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with3.Add("LOOKUP_VALUE_IN", strLOOKUP_IN).Direction = ParameterDirection.Input;
                _with3.Add("SearchFlag", (string.IsNullOrEmpty(strSearchFlag_IN) ? strNull : strSearchFlag_IN)).Direction = ParameterDirection.Input;
                _with3.Add("USER_PK_IN", (string.IsNullOrEmpty(User_pk) ? strNull : User_pk)).Direction = ParameterDirection.Input;
                _with3.Add("PARAM3_IN", (string.IsNullOrEmpty(C3) ? strNull : C3)).Direction = ParameterDirection.Input;
                _with3.Add("PARAM4_IN", (string.IsNullOrEmpty(C4) ? strNull : C4)).Direction = ParameterDirection.Input;
                _with3.Add("PARAM5_IN", (string.IsNullOrEmpty(C5) ? strNull : C5)).Direction = ParameterDirection.Input;
                _with3.Add("PARAM6_IN", (string.IsNullOrEmpty(C6) ? strNull : C6)).Direction = ParameterDirection.Input;
                _with3.Add("PARAM7_IN", (string.IsNullOrEmpty(C7) ? strNull : C7)).Direction = ParameterDirection.Input;
                _with3.Add("PARAM8_IN", (string.IsNullOrEmpty(C8) ? strNull : C8)).Direction = ParameterDirection.Input;
                _with3.Add("PARAM9_IN", (string.IsNullOrEmpty(C9) ? strNull : C9)).Direction = ParameterDirection.Input;
                _with3.Add("PARAM10_IN", (string.IsNullOrEmpty(C10) ? strNull : C10)).Direction = ParameterDirection.Input;
                _with3.Add("PARAM11_IN", (string.IsNullOrEmpty(C11) ? strNull : C11)).Direction = ParameterDirection.Input;
                _with3.Add("PARAM12_IN", (string.IsNullOrEmpty(C12) ? strNull : C12)).Direction = ParameterDirection.Input;
                _with3.Add("RETURN_VALUE", OracleDbType.Varchar2, 32767, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString((string.IsNullOrEmpty(selectCommand.Parameters["RETURN_VALUE"].Value.ToString()) ? "" : selectCommand.Parameters["RETURN_VALUE"].Value));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        #endregion "getCommonNewDataSearchProcedure"

        #region "getCommonNewSearchProcedure"

        /// <summary>
        /// Gets the common new search procedure.
        /// </summary>
        /// <param name="strSearchFlag">The string search flag.</param>
        /// <param name="strCondition">The string condition.</param>
        /// <param name="strChk">The string CHK.</param>
        /// <param name="EXD">if set to <c>true</c> [exd].</param>
        /// <returns></returns>
        public string getCommonNewSearchProcedure(string strSearchFlag, string strCondition, string strChk = "", bool EXD = false)
        {
            return FetchCOMMONDATANew(strCondition, strChk, EXD);
        }

        /// <summary>
        /// Fetches the commondata new.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <param name="strChk">The string CHK.</param>
        /// <param name="EXD">if set to <c>true</c> [exd].</param>
        /// <returns></returns>
        public string FetchCOMMONDATANew(string strCond, string strChk = "", bool EXD = false)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string C = null;
            string C1 = null;
            string C3 = null;
            string C4 = null;
            string C5 = null;
            string C6 = null;
            string C7 = null;
            string C8 = null;
            string C9 = null;
            string C10 = null;
            string C11 = null;
            string C12 = null;
            string C13 = null;
            string C14 = null;
            string C15 = null;
            string User_pk = null;
            string strSearchFlag_IN = null;
            string strFromToType = "";
            var strNull = "";
            string SelectedLoc = null;
            string strLOOKUP_IN = null;
            arr = strCond.Split('~');
            // lOOK UP VALUE E OR L
            strLOOKUP_IN = Convert.ToString(arr.GetValue(0));
            //USER TYPE TEXT WHILE PRESSING THE KEY
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (strSERACH_IN.IndexOf("$") != -1)
            {
                strSERACH_IN = strSERACH_IN.Replace("$", "&");
            }
            strSERACH_IN = strSERACH_IN.TrimEnd(',');
            User_pk = Convert.ToString(HttpContext.Current.Session["USER_PK"]);
            //FLAG FOR ES FOR WHICH FETCHING THE RECORDS
            strSearchFlag_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
            {
                C3 = Convert.ToString(arr.GetValue(3));
            }
            if (arr.Length > 4)
            {
                C4 = Convert.ToString(arr.GetValue(4));
            }
            if (arr.Length > 5)
            {
                C5 = Convert.ToString(arr.GetValue(5));
            }
            if (arr.Length > 6)
            {
                C6 = Convert.ToString(arr.GetValue(6));
            }
            if (arr.Length > 7)
            {
                C7 = Convert.ToString(arr.GetValue(7));
            }
            if (arr.Length > 8)
            {
                C8 = Convert.ToString(arr.GetValue(8));
            }
            if (arr.Length > 9)
            {
                C9 = Convert.ToString(arr.GetValue(9));
            }
            if (arr.Length > 10)
            {
                C10 = Convert.ToString(arr.GetValue(10));
            }
            if (arr.Length > 11)
            {
                C11 = Convert.ToString(arr.GetValue(11));
            }
            if (arr.Length > 12)
            {
                C12 = Convert.ToString(arr.GetValue(12));
            }
            if (arr.Length > 13)
            {
                C13 = Convert.ToString(arr.GetValue(13));
            }
            if (arr.Length > 14)
            {
                C14 = Convert.ToString(arr.GetValue(14));
            }
            if (arr.Length > 15)
            {
                C15 = Convert.ToString(arr.GetValue(15));
            }

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                //export doc module enhance search
                if (EXD)
                {
                    selectCommand.CommandText = objWF.MyUserName + ".GET_EN_COMMON_EXP_DOC_PKG.GET_COMMON_NEW";
                }
                else
                {
                    selectCommand.CommandText = objWF.MyUserName + ".GET_EN_COMMON_PKG_NEW.GET_COMMON_NEW";
                }

                var _with4 = selectCommand.Parameters;
                _with4.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with4.Add("LOOKUP_VALUE_IN", strLOOKUP_IN).Direction = ParameterDirection.Input;
                _with4.Add("SearchFlag", (string.IsNullOrEmpty(strSearchFlag_IN) ? strNull : strSearchFlag_IN)).Direction = ParameterDirection.Input;
                _with4.Add("USER_PK_IN", (string.IsNullOrEmpty(User_pk) ? strNull : User_pk)).Direction = ParameterDirection.Input;
                _with4.Add("PARAM3_IN", (string.IsNullOrEmpty(C3) ? strNull : C3)).Direction = ParameterDirection.Input;
                _with4.Add("PARAM4_IN", (string.IsNullOrEmpty(C4) ? strNull : C4)).Direction = ParameterDirection.Input;
                _with4.Add("PARAM5_IN", (string.IsNullOrEmpty(C5) ? strNull : C5)).Direction = ParameterDirection.Input;
                _with4.Add("PARAM6_IN", (string.IsNullOrEmpty(C6) ? strNull : C6)).Direction = ParameterDirection.Input;
                _with4.Add("PARAM7_IN", (string.IsNullOrEmpty(C7) ? strNull : C7)).Direction = ParameterDirection.Input;
                _with4.Add("PARAM8_IN", (string.IsNullOrEmpty(C8) ? strNull : C8)).Direction = ParameterDirection.Input;
                _with4.Add("PARAM9_IN", (string.IsNullOrEmpty(C9) ? strNull : C9)).Direction = ParameterDirection.Input;
                _with4.Add("PARAM10_IN", (string.IsNullOrEmpty(C10) ? strNull : C10)).Direction = ParameterDirection.Input;
                _with4.Add("PARAM11_IN", (string.IsNullOrEmpty(C11) ? strNull : C11)).Direction = ParameterDirection.Input;
                _with4.Add("PARAM12_IN", (string.IsNullOrEmpty(C12) ? strNull : C12)).Direction = ParameterDirection.Input;
                _with4.Add("PARAM13_IN", (string.IsNullOrEmpty(C13) ? strNull : C13)).Direction = ParameterDirection.Input;
                _with4.Add("PARAM14_IN", (string.IsNullOrEmpty(C14) ? strNull : C14)).Direction = ParameterDirection.Input;
                //export doc module enhance search
                if (EXD)
                {
                    for (int _counter = 15; _counter <= 51; _counter++)
                    {
                        string _paramName = "PARAM" + _counter.ToString() + "_IN";
                        string _paramValue = "";
                        if (arr.Length > _counter)
                        {
                            _paramValue = Convert.ToString(arr.GetValue(_counter));
                            _with4.Add(_paramName, (string.IsNullOrEmpty(_paramValue) ? strNull : _paramValue)).Direction = ParameterDirection.Input;
                        }
                    }
                }
                _with4.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                OracleClob clob = null;
                clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        #endregion "getCommonNewSearchProcedure"

        #region "getCommonExpiredSearchProcedure"

        /// <summary>
        /// Gets the common expired search procedure.
        /// </summary>
        /// <param name="strSearchFlag">The string search flag.</param>
        /// <param name="strCondition">The string condition.</param>
        /// <param name="strChk">The string CHK.</param>
        /// <returns></returns>
        public string getCommonExpiredSearchProcedure(string strSearchFlag, string strCondition, string strChk = "")
        {
            return FetchCOMMONDATAExpiry(strCondition, strChk);
        }

        /// <summary>
        /// Gets the multiple es.
        /// </summary>
        /// <param name="strSearchFlag">The string search flag.</param>
        /// <param name="strCondition">The string condition.</param>
        /// <param name="strChk">The string CHK.</param>
        /// <returns></returns>
        public string getMultipleES(string strSearchFlag, string strCondition, string strChk = "")
        {
            return FetchMultipleES(strCondition, strChk);
        }

        /// <summary>
        /// Fetches the commondata expiry.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <param name="strChk">The string CHK.</param>
        /// <returns></returns>
        public string FetchCOMMONDATAExpiry(string strCond, string strChk = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string C = null;
            string C1 = null;
            string C3 = null;
            string C4 = null;
            string C5 = null;
            string C6 = null;
            string C7 = null;
            string C8 = null;
            string C9 = null;
            string C10 = null;
            string C11 = null;
            string C12 = null;
            string User_pk = null;
            string strSearchFlag_IN = null;
            string strFromToType = "";
            var strNull = "";
            string SelectedLoc = null;
            string strLOOKUP_IN = null;
            arr = strCond.Split('~');
            //***********************
            // lOOK UP VALUE E OR L
            strLOOKUP_IN = Convert.ToString(arr.GetValue(0));
            //*******************

            //*************************
            //USER TYPE TEXT WHILE PRESSING THE KEY
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (strSERACH_IN.IndexOf("$") != -1)
            {
                strSERACH_IN = strSERACH_IN.Replace("$", "&");
            }
            strSERACH_IN = strSERACH_IN.TrimEnd(',');
            //*************************
            //***********************
            User_pk = Convert.ToString(HttpContext.Current.Session["USER_PK"]);
            //FLAG FOR ES FOR WHICH FETCHING THE RECORDS
            strSearchFlag_IN = Convert.ToString(arr.GetValue(2));
            //*********************
            if (arr.Length > 3)
            {
                C3 = Convert.ToString(arr.GetValue(3));
            }
            if (arr.Length > 4)
            {
                C4 = Convert.ToString(arr.GetValue(4));
            }
            if (arr.Length > 5)
            {
                C5 = Convert.ToString(arr.GetValue(5));
            }
            if (arr.Length > 6)
            {
                C6 = Convert.ToString(arr.GetValue(6));
            }
            if (arr.Length > 7)
            {
                C7 = Convert.ToString(arr.GetValue(7));
            }
            if (arr.Length > 8)
            {
                C8 = Convert.ToString(arr.GetValue(8));
            }
            if (arr.Length > 9)
            {
                C9 = Convert.ToString(arr.GetValue(9));
            }
            if (arr.Length > 10)
            {
                C10 = Convert.ToString(arr.GetValue(10));
            }
            if (arr.Length > 11)
            {
                C11 = Convert.ToString(arr.GetValue(11));
            }
            if (arr.Length > 12)
            {
                C12 = Convert.ToString(arr.GetValue(12));
            }

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".GET_EN_COMMON_PKG_EXP_CTRTS.GET_COMMON_NEW";

                var _with5 = selectCommand.Parameters;
                _with5.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with5.Add("LOOKUP_VALUE_IN", strLOOKUP_IN).Direction = ParameterDirection.Input;
                _with5.Add("SearchFlag", (string.IsNullOrEmpty(strSearchFlag_IN) ? strNull : strSearchFlag_IN)).Direction = ParameterDirection.Input;
                _with5.Add("USER_PK_IN", (string.IsNullOrEmpty(User_pk) ? strNull : User_pk)).Direction = ParameterDirection.Input;
                _with5.Add("PARAM3_IN", (string.IsNullOrEmpty(C3) ? strNull : C3)).Direction = ParameterDirection.Input;
                _with5.Add("PARAM4_IN", (string.IsNullOrEmpty(C4) ? strNull : C4)).Direction = ParameterDirection.Input;
                _with5.Add("PARAM5_IN", (string.IsNullOrEmpty(C5) ? strNull : C5)).Direction = ParameterDirection.Input;
                _with5.Add("PARAM6_IN", (string.IsNullOrEmpty(C6) ? strNull : C6)).Direction = ParameterDirection.Input;
                _with5.Add("PARAM7_IN", (string.IsNullOrEmpty(C7) ? strNull : C7)).Direction = ParameterDirection.Input;
                _with5.Add("PARAM8_IN", (string.IsNullOrEmpty(C8) ? strNull : C8)).Direction = ParameterDirection.Input;
                _with5.Add("PARAM9_IN", (string.IsNullOrEmpty(C9) ? strNull : C9)).Direction = ParameterDirection.Input;
                _with5.Add("PARAM10_IN", (string.IsNullOrEmpty(C10) ? strNull : C10)).Direction = ParameterDirection.Input;
                _with5.Add("PARAM11_IN", (string.IsNullOrEmpty(C11) ? strNull : C11)).Direction = ParameterDirection.Input;
                _with5.Add("PARAM12_IN", (string.IsNullOrEmpty(C12) ? strNull : C12)).Direction = ParameterDirection.Input;
                _with5.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                OracleClob clob = null;
                clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        #endregion "getCommonExpiredSearchProcedure"

        #region "Fetch Gen Common Search Function"

        /// <summary>
        /// Fetches the gen common search.
        /// </summary>
        /// <param name="strSearchFlag">The string search flag.</param>
        /// <param name="strCond">The string cond.</param>
        /// <param name="strChk">The string CHK.</param>
        /// <returns></returns>
        public string FetchGenCommonSearch(string strSearchFlag, string strCond, string strChk = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string C = null;
            string C1 = null;
            string C2 = null;
            string C3 = null;
            string C4 = null;
            string C5 = null;
            string C6 = null;
            string C7 = null;
            string C8 = null;
            string C9 = null;
            string C10 = null;
            string User_pk = null;
            string strSearchFlag_IN = null;
            string strFromToType = "";
            var strNull = "";
            string SelectedLoc = null;
            string strLOOKUP_IN = null;
            arr = strCond.Split('~');
            // lOOK UP VALUE E OR L
            strLOOKUP_IN = Convert.ToString(arr.GetValue(0));
            //USER TYPE TEXT WHILE PRESSING THE KEY
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (strSERACH_IN.IndexOf("$") != -1)
            {
                strSERACH_IN = strSERACH_IN.Replace("$", "&");
            }
            strSERACH_IN = strSERACH_IN.TrimEnd(',');
            User_pk = Convert.ToString(HttpContext.Current.Session["USER_PK"]);
            //FLAG FOR ES FOR WHICH FETCHING THE RECORDS
            strSearchFlag_IN = strSearchFlag;
            if (arr.Length > 2)
            {
                C2 = Convert.ToString(arr.GetValue(2));
            }
            if (arr.Length > 3)
            {
                C3 = Convert.ToString(arr.GetValue(3));
            }
            if (arr.Length > 4)
            {
                C4 = Convert.ToString(arr.GetValue(4));
            }
            if (arr.Length > 5)
            {
                C5 = Convert.ToString(arr.GetValue(5));
            }
            if (arr.Length > 6)
            {
                C6 = Convert.ToString(arr.GetValue(6));
            }
            if (arr.Length > 7)
            {
                C7 = Convert.ToString(arr.GetValue(7));
            }
            if (arr.Length > 8)
            {
                C8 = Convert.ToString(arr.GetValue(8));
            }
            if (arr.Length > 9)
            {
                C9 = Convert.ToString(arr.GetValue(9));
            }
            if (arr.Length > 10)
            {
                C10 = Convert.ToString(arr.GetValue(10));
            }

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".GET_COMMON_PKG.GET_COMMON_EN";

                var _with6 = selectCommand.Parameters;
                _with6.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with6.Add("LOOKUP_VALUE_IN", strLOOKUP_IN).Direction = ParameterDirection.Input;
                _with6.Add("SearchFlag", (string.IsNullOrEmpty(strSearchFlag_IN) ? strNull : strSearchFlag_IN)).Direction = ParameterDirection.Input;
                _with6.Add("USER_PK_IN", (string.IsNullOrEmpty(User_pk) ? strNull : User_pk)).Direction = ParameterDirection.Input;
                _with6.Add("PARAM2_IN", (string.IsNullOrEmpty(C2) ? strNull : C2)).Direction = ParameterDirection.Input;
                _with6.Add("PARAM3_IN", (string.IsNullOrEmpty(C3) ? strNull : C3)).Direction = ParameterDirection.Input;
                _with6.Add("PARAM4_IN", (string.IsNullOrEmpty(C4) ? strNull : C4)).Direction = ParameterDirection.Input;
                _with6.Add("PARAM5_IN", (string.IsNullOrEmpty(C5) ? strNull : C5)).Direction = ParameterDirection.Input;
                _with6.Add("PARAM6_IN", (string.IsNullOrEmpty(C6) ? strNull : C6)).Direction = ParameterDirection.Input;
                _with6.Add("PARAM7_IN", (string.IsNullOrEmpty(C7) ? strNull : C7)).Direction = ParameterDirection.Input;
                _with6.Add("PARAM8_IN", (string.IsNullOrEmpty(C8) ? strNull : C8)).Direction = ParameterDirection.Input;
                _with6.Add("PARAM9_IN", (string.IsNullOrEmpty(C9) ? strNull : C9)).Direction = ParameterDirection.Input;
                _with6.Add("PARAM10_IN", (string.IsNullOrEmpty(C10) ? strNull : C10)).Direction = ParameterDirection.Input;
                _with6.Add("RETURN_VALUE", OracleDbType.Varchar2, 32767, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString((string.IsNullOrEmpty(selectCommand.Parameters["RETURN_VALUE"].Value.ToString()) ? "" : selectCommand.Parameters["RETURN_VALUE"].Value));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        #endregion "Fetch Gen Common Search Function"

        #region "getCommon_DOCUMENTPKGProcedure"

        /// <summary>
        /// Gets the common_ document procedure.
        /// </summary>
        /// <param name="strSearchFlag">The string search flag.</param>
        /// <param name="strCondition">The string condition.</param>
        /// <param name="strChk">The string CHK.</param>
        /// <returns></returns>
        public string getCommon_DOCUMENTProcedure(string strSearchFlag, string strCondition, string strChk = "")
        {
            return FetchGETENCOMMON_DOCUMENTPKG(strCondition, strChk);
        }

        /// <summary>
        /// Fetches the getencommo n_ documentpkg.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <param name="strChk">The string CHK.</param>
        /// <returns></returns>
        public string FetchGETENCOMMON_DOCUMENTPKG(string strCond, string strChk = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string C = null;
            string C1 = null;
            string C3 = null;
            string C4 = null;
            string C5 = null;
            string C6 = null;
            string C7 = null;
            string C8 = null;
            string C9 = null;
            string C10 = null;
            string C11 = null;
            string C12 = null;
            string User_pk = null;
            string strSearchFlag_IN = null;
            string strFromToType = "";
            var strNull = "";
            string SelectedLoc = null;
            string strLOOKUP_IN = null;
            arr = strCond.Split('~');
            //***********************
            // lOOK UP VALUE E OR L
            strLOOKUP_IN = Convert.ToString(arr.GetValue(0));
            //*******************

            //*************************
            //USER TYPE TEXT WHILE PRESSING THE KEY
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (strSERACH_IN.IndexOf("$") != -1)
            {
                strSERACH_IN = strSERACH_IN.Replace("$", "&");
            }
            strSERACH_IN = strSERACH_IN.TrimEnd(',');
            //*************************
            //***********************
            User_pk = Convert.ToString(HttpContext.Current.Session["USER_PK"]);
            //FLAG FOR ES FOR WHICH FETCHING THE RECORDS
            strSearchFlag_IN = Convert.ToString(arr.GetValue(2));
            //*********************
            if (arr.Length > 3)
            {
                C3 = Convert.ToString(arr.GetValue(3));
            }
            if (arr.Length > 4)
            {
                C4 = Convert.ToString(arr.GetValue(4));
            }
            if (arr.Length > 5)
            {
                C5 = Convert.ToString(arr.GetValue(5));
            }
            if (arr.Length > 6)
            {
                C6 = Convert.ToString(arr.GetValue(6));
            }
            if (arr.Length > 7)
            {
                C7 = Convert.ToString(arr.GetValue(7));
            }
            if (arr.Length > 8)
            {
                C8 = Convert.ToString(arr.GetValue(8));
            }
            if (arr.Length > 9)
            {
                C9 = Convert.ToString(arr.GetValue(9));
            }
            if (arr.Length > 10)
            {
                C10 = Convert.ToString(arr.GetValue(10));
            }

            if (arr.Length > 11)
            {
                C11 = Convert.ToString(arr.GetValue(11));
            }
            if (arr.Length > 12)
            {
                C12 = Convert.ToString(arr.GetValue(12));
            }

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".GET_EN_COMMON_DOCUMENT_PKG.GET_EN_COMMON_DOCUMENT";
                var _with7 = selectCommand.Parameters;
                _with7.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with7.Add("LOOKUP_VALUE_IN", strLOOKUP_IN).Direction = ParameterDirection.Input;
                _with7.Add("SearchFlag", (string.IsNullOrEmpty(strSearchFlag_IN) ? strNull : strSearchFlag_IN)).Direction = ParameterDirection.Input;
                _with7.Add("USER_PK_IN", (string.IsNullOrEmpty(User_pk) ? strNull : User_pk)).Direction = ParameterDirection.Input;
                _with7.Add("PARAM3_IN", (string.IsNullOrEmpty(C3) ? strNull : C3)).Direction = ParameterDirection.Input;
                _with7.Add("PARAM4_IN", (string.IsNullOrEmpty(C4) ? strNull : C4)).Direction = ParameterDirection.Input;
                _with7.Add("PARAM5_IN", (string.IsNullOrEmpty(C5) ? strNull : C5)).Direction = ParameterDirection.Input;
                _with7.Add("PARAM6_IN", (string.IsNullOrEmpty(C6) ? strNull : C6)).Direction = ParameterDirection.Input;
                _with7.Add("PARAM7_IN", (string.IsNullOrEmpty(C7) ? strNull : C7)).Direction = ParameterDirection.Input;
                _with7.Add("PARAM8_IN", (string.IsNullOrEmpty(C8) ? strNull : C8)).Direction = ParameterDirection.Input;
                _with7.Add("PARAM9_IN", (string.IsNullOrEmpty(C9) ? strNull : C9)).Direction = ParameterDirection.Input;
                _with7.Add("PARAM10_IN", (string.IsNullOrEmpty(C10) ? strNull : C10)).Direction = ParameterDirection.Input;
                _with7.Add("PARAM11_IN", (string.IsNullOrEmpty(C11) ? strNull : C11)).Direction = ParameterDirection.Input;
                _with7.Add("PARAM12_IN", (string.IsNullOrEmpty(C12) ? strNull : C12)).Direction = ParameterDirection.Input;
                _with7.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                OracleClob clob = null;
                clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        #endregion "getCommon_DOCUMENTPKGProcedure"

        #region " RFQ "

        /// <summary>
        /// Gets the RFQ search.
        /// </summary>
        /// <param name="strSearchFlag">The string search flag.</param>
        /// <param name="strCondition">The string condition.</param>
        /// <returns></returns>
        public string getRFQSearch(string strSearchFlag, string strCondition)
        {
            switch (strSearchFlag)
            {
                case "OPERATOR_RFQ_INSERT_STATUS":
                    return RFQSpotRateInsertStatus(strCondition);

                case "AIRLINE_RFQ_INSERT_STATUS":
                    return RFQSpotRateInsertStatus(strCondition);

                case "CONTRACT_NO_FOR_RFQ_AIR":
                    return ContractNoForAirRfq(strCondition);

                default:
                    return "";
            }
        }

        /// <summary>
        /// Fetches for quotation no expiry.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchForQuotationNoExpiry(string strCond)
        {
            Cls_BookingEntry objClass = new Cls_BookingEntry();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchForQuotationNoExpiry(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches for new quotation no.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchForNewQuotationNo(string strCond)
        {
            Cls_BookingEntry objClass = new Cls_BookingEntry();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchForNewQuotationNo(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion " RFQ "

        #region "CustBasebEmployee"

        /// <summary>
        /// Fetches the customer based emp.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchCustBasedEmp(string strCond)
        {
            cls_Employee_Mst_Table cls_EmpMstbl1 = new cls_Employee_Mst_Table();
            string strReturn = null;
            try
            {
                strReturn = cls_EmpMstbl1.FetchCustBasedEmp(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "CustBasebEmployee"

        #region " Rating And Tariff :- RFQ "

        /// <summary>
        /// Fetches the port country trade.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchPortCountryTRADE(string strCond)
        {
            cls_THC_PHC objClass = new cls_THC_PHC();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchPortCountryTRADE(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the trade country.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchTradeCountry(string strCond)
        {
            cls_THC_PHC objClass = new cls_THC_PHC();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchTradeCountry(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Gets the rating and tariff.
        /// </summary>
        /// <param name="strSearchFlag">The string search flag.</param>
        /// <param name="strCondition">The string condition.</param>
        /// <returns></returns>
        public string getRatingAndTariff(string strSearchFlag, string strCondition)
        {
            switch (strSearchFlag)
            {
                case "OPERATOR_TARIFF_SHOW":
                    return OperatorTariff(strCondition);

                case "AIR_TARIFF_SHOW":
                    return AirlineTariff(strCondition);

                case "CUSTOMER_CONTRACT_SHOW":
                    return FetchCustomerContract(strCondition);

                case "CUSTOMER_CONTRACT_AIR_SHOW":
                    return FetchCustomerContract_Air(strCondition);

                case "SPOT_RATE_SHOW":
                    return FetchSpotRate(strCondition);

                default:
                    return "";
            }
        }

        /// <summary>
        /// Operators the tariff.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string OperatorTariff(string strCond)
        {
            Cls_SRRSeaContract objClass = new Cls_SRRSeaContract();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchOperatorTariff(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Airlines the tariff.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string AirlineTariff(string strCond)
        {
            string strReturn = null;
            try
            {
                strReturn = Cls_SRRAirContract.FetchAirlineTariff(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// RFQs the spot rate insert status.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string RFQSpotRateInsertStatus(string strCond)
        {
            Cls_OperatorRFQSpotRate objClass = new Cls_OperatorRFQSpotRate();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchRFQSpotRateInsertStatus(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Contracts the no for air RFQ.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string ContractNoForAirRfq(string strCond)
        {
            Cls_AirlineRFQSpotRate objClass = new Cls_AirlineRFQSpotRate();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchContractNoForAirRfq(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion " Rating And Tariff :- RFQ "

        #region " Rating And Tariff :- Common "

        /// <summary>
        /// Fetches the customer contract.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchCustomerContract(string strCond)
        {
            Cls_SRRSeaContract objClass = new Cls_SRRSeaContract();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchCustomerContract(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the customer contract_ air.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchCustomerContract_Air(string strCond)
        {
            string strReturn = null;
            try
            {
                strReturn = Cls_SRRAirContract.Fetch_Customer_Contract_Air(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the spot rate.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchSpotRate(string strCond)
        {
            string strReturn = null;
            try
            {
                strReturn = Cls_SRRAirContract.Fetch_SpotRate(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        // Used in Operator Tariff Listing to fetch tariff of particular operator only
        /// <summary>
        /// Fetches the tariff for operator.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchTariffForOperator(string strCond)
        {
            Cls_OperatorTarif_Listing objClass = new Cls_OperatorTarif_Listing();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchTariffForOperator(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        // Used in Airline Tariff Listing to fetch tariff of particular airline only
        /// <summary>
        /// Fetches the tariff for airline.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchTariffForAirline(string strCond)
        {
            Cls_AirlineTarif_Listing objClass = new Cls_AirlineTarif_Listing();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchTariffForAirline(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetch_s the customer_ for_ category.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string Fetch_Customer_For_Category(string strCond)
        {
            cls_Customer_Mst_Tbl objClass = new cls_Customer_Mst_Tbl();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchCustomerForCategory(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        // Used in Operator RFQ Spot Rate Listing
        /// <summary>
        /// Fetch_s the customer_ for_ operator.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string Fetch_Customer_For_Operator(string strCond)
        {
            Cls_OperatorRFQSpotRate_Listing objClass = new Cls_OperatorRFQSpotRate_Listing();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchCustomerForOperator(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        // Used in Operator RFQ Spot Rate Listing
        /// <summary>
        /// Fetch_s the rf q_ for_ operator_ customer.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string Fetch_RFQ_For_Operator_Customer(string strCond)
        {
            Cls_OperatorRFQSpotRate_Listing objClass = new Cls_OperatorRFQSpotRate_Listing();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchRfqForOperatorCustomer(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetch_s the customer_ for_ airline.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string Fetch_Customer_For_Airline(string strCond)
        {
            Cls_AirlineRFQSpotRate_Listing objClass = new Cls_AirlineRFQSpotRate_Listing();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchCustomerForAirline(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        // Used in Airline RFQ Spot Rate Listing
        /// <summary>
        /// Fetch_s the sr r_ for_ operator_ customer.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string Fetch_SRR_For_Operator_Customer(string strCond)
        {
            clsSRRCustToOprListing objClass = new clsSRRCustToOprListing();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchSRRForOperatorCustomer(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetch_s the cont_ for_ operator_ customer.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string Fetch_Cont_For_Operator_Customer(string strCond)
        {
            cls_ContractSeaListing objClass = new cls_ContractSeaListing();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchContForOperatorCustomer(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetch_s the cont_ for_ airline_ customer.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string Fetch_Cont_For_Airline_Customer(string strCond)
        {
            cls_ContractAirListing objClass = new cls_ContractAirListing();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchCONTForAirlineCustomer(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        // Used in RFQ Spot Rate Entry
        /// <summary>
        /// Fetch_s the commodity_ for_ group.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string Fetch_Commodity_For_Group(string strCond)
        {
            cls_Commodity_Mst_Tbl objClass = new cls_Commodity_Mst_Tbl();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchCommodityForGroup(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        // Used in Enquiry for Rates
        /// <summary>
        /// Fetch_s the operator_ for_ enquiry.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string Fetch_Operator_For_Enquiry(string strCond)
        {
            Cls_Operator_Definition cls_OprtDef = new Cls_Operator_Definition();
            string strReturn = null;
            try
            {
                //strReturn = cls_OprtDef.FetchOperatorForEnquiry(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //Fetch_Airline_For_Enquiry
        /// <summary>
        /// Fetch_s the airline_ for_ enquiry_ ex p_ manifest.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string Fetch_Airline_For_Enquiry_EXP_Manifest(string strCond)
        {
            Cls_Airline_Definition cls_AirDef = new Cls_Airline_Definition();
            string strReturn = null;
            try
            {
                strReturn = cls_AirDef.FetchAirlineForEnquiryExportManifest(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //Added by Snigdharani to fetch customers based on the location of the POL or POD selected.
        /// <summary>
        /// Fetches the polpod based customer.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchPOLPODBasedCust(string strCond)
        {
            cls_Customer_Mst_Tbl objClass = new cls_Customer_Mst_Tbl();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchPOLPODBasedCust(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the group customer.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchGroupCustomer(string strCond)
        {
            cls_Customer_Mst_Tbl clsCmst = new cls_Customer_Mst_Tbl();
            string strReturn = null;
            try
            {
                strReturn = clsCmst.FetchGroupCustomer(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion " Rating And Tariff :- Common "

        #region "Fetch Common Customs Brokerage"

        /// <summary>
        /// Gets the common customs brokerage.
        /// </summary>
        /// <param name="strSearchFlag">The string search flag.</param>
        /// <param name="strCondition">The string condition.</param>
        /// <param name="strChk">The string CHK.</param>
        /// <returns></returns>
        public string getCOMMONCustomsBrokerage(string strSearchFlag, string strCondition, string strChk = "")
        {
            return FetchCOMMONCustomsBrokerage(strCondition, strChk);
        }

        /// <summary>
        /// Fetches the common customs brokerage.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <param name="strChk">The string CHK.</param>
        /// <returns></returns>
        public string FetchCOMMONCustomsBrokerage(string strCond, string strChk = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string C = null;
            string C1 = null;
            string C3 = null;
            string C4 = null;
            string C5 = null;
            string C6 = null;
            string C7 = null;
            string C8 = null;
            string C9 = null;
            string C10 = null;
            string C11 = null;
            string C12 = null;
            string C13 = null;
            string C14 = null;
            string C15 = null;
            string C16 = null;
            string C17 = null;
            string C18 = null;
            string C19 = null;
            string C20 = null;
            string User_pk = null;
            string strSearchFlag_IN = null;
            string strFromToType = "";
            var strNull = "";
            string SelectedLoc = null;
            string strLOOKUP_IN = null;
            arr = strCond.Split('~');
            strLOOKUP_IN = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (strSERACH_IN.IndexOf("$") != -1)
            {
                strSERACH_IN = strSERACH_IN.Replace("$", "&");
            }
            strSERACH_IN = strSERACH_IN.TrimEnd(',');
            User_pk = Convert.ToString(HttpContext.Current.Session["USER_PK"]);
            strSearchFlag_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
            {
                C3 = Convert.ToString(arr.GetValue(3));
            }
            if (arr.Length > 4)
            {
                C4 = Convert.ToString(arr.GetValue(4));
            }
            if (arr.Length > 5)
            {
                C5 = Convert.ToString(arr.GetValue(5));
            }
            if (arr.Length > 6)
            {
                C6 = Convert.ToString(arr.GetValue(6));
            }
            if (arr.Length > 7)
            {
                C7 = Convert.ToString(arr.GetValue(7));
            }
            if (arr.Length > 8)
            {
                C8 = Convert.ToString(arr.GetValue(8));
            }
            if (arr.Length > 9)
            {
                C9 = Convert.ToString(arr.GetValue(9));
            }
            if (arr.Length > 10)
            {
                C10 = Convert.ToString(arr.GetValue(10));
            }
            if (arr.Length > 11)
            {
                C11 = Convert.ToString(arr.GetValue(11));
            }
            if (arr.Length > 12)
            {
                C12 = Convert.ToString(arr.GetValue(12));
            }
            if (arr.Length > 13)
            {
                C13 = Convert.ToString(arr.GetValue(13));
            }
            if (arr.Length > 14)
            {
                C14 = Convert.ToString(arr.GetValue(14));
            }
            if (arr.Length > 15)
            {
                C15 = Convert.ToString(arr.GetValue(15));
            }
            if (arr.Length > 16)
            {
                C16 = Convert.ToString(arr.GetValue(16));
            }
            if (arr.Length > 17)
            {
                C17 = Convert.ToString(arr.GetValue(17));
            }
            if (arr.Length > 18)
            {
                C18 = Convert.ToString(arr.GetValue(18));
            }
            if (arr.Length > 19)
            {
                C19 = Convert.ToString(arr.GetValue(19));
            }
            if (arr.Length > 20)
            {
                C20 = Convert.ToString(arr.GetValue(20));
            }
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".GET_EN_CUSTOMSBROKERAGE_PKG.GET_CUSTOMSBROKERAGE";

                var _with8 = selectCommand.Parameters;
                _with8.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with8.Add("LOOKUP_VALUE_IN", strLOOKUP_IN).Direction = ParameterDirection.Input;
                _with8.Add("SearchFlag", (string.IsNullOrEmpty(strSearchFlag_IN) ? strNull : strSearchFlag_IN)).Direction = ParameterDirection.Input;
                _with8.Add("USER_PK_IN", (string.IsNullOrEmpty(User_pk) ? strNull : User_pk)).Direction = ParameterDirection.Input;
                _with8.Add("PARAM3_IN", (string.IsNullOrEmpty(C3.Trim()) ? strNull : C3)).Direction = ParameterDirection.Input;
                _with8.Add("PARAM4_IN", (string.IsNullOrEmpty(C4.Trim()) ? strNull : C4)).Direction = ParameterDirection.Input;
                _with8.Add("PARAM5_IN", (string.IsNullOrEmpty(C5.Trim()) ? strNull : C5)).Direction = ParameterDirection.Input;
                _with8.Add("PARAM6_IN", (string.IsNullOrEmpty(C6.Trim()) ? strNull : C6)).Direction = ParameterDirection.Input;
                _with8.Add("PARAM7_IN", (string.IsNullOrEmpty(C7.Trim()) ? strNull : C7)).Direction = ParameterDirection.Input;
                _with8.Add("PARAM8_IN", (string.IsNullOrEmpty(C8.Trim()) ? strNull : C8)).Direction = ParameterDirection.Input;
                _with8.Add("PARAM9_IN", (string.IsNullOrEmpty(C9.Trim()) ? strNull : C9)).Direction = ParameterDirection.Input;
                _with8.Add("PARAM10_IN", (string.IsNullOrEmpty(C10.Trim()) ? strNull : C10)).Direction = ParameterDirection.Input;
                _with8.Add("PARAM11_IN", (string.IsNullOrEmpty(C11.Trim()) ? strNull : C11)).Direction = ParameterDirection.Input;
                _with8.Add("PARAM12_IN", (string.IsNullOrEmpty(C12.Trim()) ? strNull : C12)).Direction = ParameterDirection.Input;
                _with8.Add("PARAM13_IN", (string.IsNullOrEmpty(C13.Trim()) ? strNull : C13)).Direction = ParameterDirection.Input;
                _with8.Add("PARAM14_IN", (string.IsNullOrEmpty(C14.Trim()) ? strNull : C14)).Direction = ParameterDirection.Input;
                _with8.Add("PARAM15_IN", (string.IsNullOrEmpty(C15.Trim()) ? strNull : C15)).Direction = ParameterDirection.Input;
                _with8.Add("PARAM16_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C16)).Direction = ParameterDirection.Input;
                _with8.Add("PARAM17_IN", (string.IsNullOrEmpty(C17.Trim()) ? strNull : C17)).Direction = ParameterDirection.Input;
                _with8.Add("PARAM18_IN", (string.IsNullOrEmpty(C18.Trim()) ? strNull : C18)).Direction = ParameterDirection.Input;
                _with8.Add("PARAM19_IN", (string.IsNullOrEmpty(C19.Trim()) ? strNull : C19)).Direction = ParameterDirection.Input;
                _with8.Add("PARAM20_IN", (string.IsNullOrEmpty(C20.Trim()) ? strNull : C20)).Direction = ParameterDirection.Input;
                _with8.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                OracleClob clob = null;
                clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        #endregion "Fetch Common Customs Brokerage"

        /// <summary>
        /// Fetches the po d_ notworkport.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchPOD_NOTWORKPORT(string strCond)
        {
            clsPORT_MST_TBL cls_PortMstTbl8 = new clsPORT_MST_TBL();
            string strReturn = null;
            try
            {
                strReturn = cls_PortMstTbl8.FetchPOD_NOTWORKPORT(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #region " Common Queries :- Customer,CustomerCategory,POL,POD,Depot,Trade,Operator "

        /// <summary>
        /// Fetches the name of the vessel identifier.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchVesselIDName(string strCond)
        {
            cls_Vessel_Voyage_Listing clsVvl = new cls_Vessel_Voyage_Listing();
            string strReturn = null;
            try
            {
                strReturn = clsVvl.FetchVesselIDName(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the new vendor.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchNewVendor(string strCond)
        {
            Cls_StatementOfAccountsVendor objClass = new Cls_StatementOfAccountsVendor();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchNewVendor(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the transporter.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchTransporter(string strCond)
        {
            Cls_Transporter_Contract clsTc = new Cls_Transporter_Contract();
            string strReturn = null;
            try
            {
                strReturn = clsTc.FetchTransporter(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the location by name for country.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchLocationByNameForCountry(string strCond)
        {
            clsRegionDetails clsRd = new clsRegionDetails();
            string strReturn = null;
            try
            {
                strReturn = clsRd.FetchLocationByNameForCountry(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the transporter zone.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchTransporterZone(string strCond)
        {
            Cls_Transport_Note clsTNote = new Cls_Transport_Note();
            string strReturn = null;
            try
            {
                strReturn = clsTNote.FetchTransporterZone(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the cost element identifier.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchCostElementID(string strCond)
        {
            Cls_Supplier_Invoice Cls_SupInv = new Cls_Supplier_Invoice();
            string strReturn = null;
            try
            {
                strReturn = Cls_SupInv.FetchCostElementID(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the job in MBL entry.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchJobInMblEntry(string strCond)
        {
            cls_MBL_Entry objClass = new cls_MBL_Entry();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchJobInMblEntry(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the job in bb MBL entry.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchJobInBBMblEntry(string strCond)
        {
            cls_BBMBL_Entry objClass = new cls_BBMBL_Entry();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchJobInBBMblEntry(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the MJC in MBL entry.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchMJCInMblEntry(string strCond)
        {
            cls_MBL_Entry objClass = new cls_MBL_Entry();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchMJCInMblEntry(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches for HBL reference in MBL.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchForHblRefInMbl(string strCond)
        {
            cls_MBL_Entry objClass = new cls_MBL_Entry();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchForHblRefInMbl(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches for MBL reference in implist.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchForMblRefInImplist(string strCond)
        {
            clsQuickEntry clsQe = new clsQuickEntry();
            string strReturn = null;
            try
            {
                strReturn = clsQe.FetchForMblRefInImplist(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches for HBL reference in implist.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchForHblRefInImplist(string strCond)
        {
            clsQuickEntry objClass = new clsQuickEntry();
            string strReturn = null;
            try
            {
               strReturn = objClass.FetchForHblRefInImplist(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches for SHP reference in implist.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchForSHPRefInImplist(string strCond)
        {
            clsQuickEntry objClass = new clsQuickEntry();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchForSHPRefInImplist(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches for CNS reference in implist.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchForCNSRefInImplist(string strCond)
        {
            clsQuickEntry objClass = new clsQuickEntry();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchForCNSRefInImplist(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the voyage re assign roll over.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchVoyageReAssignRollOver(string strCond)
        {
            cls_BookingRollOver objClass = new cls_BookingRollOver();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchVoyageReAssignRollOver(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the voyage for booking roll over.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchVoyageForBookingRollOver(string strCond)
        {
            cls_BookingRollOver cls_Bro = new cls_BookingRollOver();
            string strReturn = null;
            try
            {
                strReturn = cls_Bro.FetchVoyageForBookingRollOver(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the voyage for job card.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchVoyageForJobCard(string strCond)
        {
            cls_InvoiceListSea objClass = new cls_InvoiceListSea();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchVoyageForJobCard(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the VSL voy roll listing.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchVslVoyRollListing(string strCond)
        {
            cls_BookingRollOver cls_Bro1 = new cls_BookingRollOver();
            string strReturn = null;
            try
            {
                strReturn = cls_Bro1.FetchVslVoyRollListing(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the airline with prefix.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchAirlineWithPrefix(string strCond)
        {
            Cls_Airline_Definition objClass = new Cls_Airline_Definition();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchAirlineWithPrefix(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches all cha agent.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchAllCHAAgent(string strCond)
        {
            clsVendorDetails objClass = new clsVendorDetails();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchAllCHAAgent(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the tcport.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchTCPORT(string strCond)
        {
            clsPORT_MST_TBL objClass = new clsPORT_MST_TBL();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchTCPORT(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the mawb nr.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchMawbNr(string strCond)
        {
            clsAirwayBill cls_AirBill = new clsAirwayBill();
            string strReturn = null;
            try
            {
                strReturn = cls_AirBill.FetchMawbNr(strCond, Convert.ToInt32(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //FetchMawbNr( strCondition)
        /// <summary>
        /// Fetch_s the airline_ for_ customer.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string Fetch_Airline_For_Customer(string strCond)
        {
            Cls_AirlineRFQSpotRate_Listing objClass = new Cls_AirlineRFQSpotRate_Listing();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchAirlineForCustomer(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetch_s the sr r_ for_ airline_ customer.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string Fetch_SRR_For_Airline_Customer(string strCond)
        {
            cls_SRRCustToAirListing objClass = new cls_SRRCustToAirListing();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchSRRForAirlineCustomer(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        // Used in Airline RFQ Spot Rate Listing
        /// <summary>
        /// Fetches the location.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchLocation(string strCond)
        {
            clsRegionDetails cls_RgnDetail = new clsRegionDetails();
            string strReturn = null;
            try
            {
                strReturn = cls_RgnDetail.FetchLocation(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the active job card HBLMBL new.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchActiveJobCardHBLMBLNew(string strCond)
        {
            cls_JobCardSearch objClass = new cls_JobCardSearch();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchActiveJobCardHBLMBLNew(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the credit customer.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchCreditCustomer(string strCond)
        {
            cls_Customer_Mst_Tbl cls_CustMstTbl = new cls_Customer_Mst_Tbl();
            string strReturn = null;
            try
            {
                strReturn = cls_CustMstTbl.FetchCreditCustomer(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the airline.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchAirline(string strCond)
        {
            Cls_Airline_Definition cls_AirDef1 = new Cls_Airline_Definition();
            string strReturn = null;
            try
            {
                strReturn = cls_AirDef1.FetchAirline(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the pol.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchPOL(string strCond)
        {
            clsPORT_MST_TBL cls_PortMstTbl1 = new clsPORT_MST_TBL();
            string strReturn = null;
            try
            {
                strReturn = cls_PortMstTbl1.FetchPOL(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the polimp.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchPOLIMP(string strCond)
        {
            clsPORT_MST_TBL cls_PortMstTbl3 = new clsPORT_MST_TBL();
            string strReturn = null;
            try
            {
                strReturn = cls_PortMstTbl3.FetchPOLIMP(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the pod.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchPOD(string strCond)
        {
            clsPORT_MST_TBL objClass = new clsPORT_MST_TBL();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchPOD(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the podtariff.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchPODTARIFF(string strCond)
        {
            clsPORT_MST_TBL cls_PortMstTbl5 = new clsPORT_MST_TBL();
            string strReturn = null;
            try
            {
                //strReturn = cls_PortMstTbl5.FetchPODTARIFF(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the podimp.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchPODIMP(string strCond)
        {
            clsPORT_MST_TBL cls_PortMstTbl7 = new clsPORT_MST_TBL();
            string strReturn = null;
            try
            {
                strReturn = cls_PortMstTbl7.FetchPODIMP(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the operator.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchOperator(string strCond)
        {
            Cls_Operator_Definition cls_OprtrDef = new Cls_Operator_Definition();
            string strReturn = null;
            try
            {
                strReturn = cls_OprtrDef.FetchOperator(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the vessel voyage opr.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchVesselVoyageOpr(string strCond)
        {
            cls_Vessel_Voyage_Listing cls_Vvlist = new cls_Vessel_Voyage_Listing();
            string strReturn = null;
            try
            {
                strReturn = cls_Vvlist.FetchVesselVoyageOpr(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the voyage for job card new.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchVoyageForJobCardNew(string strCond)
        {
            cls_InvoiceListSeaImp objClass = new cls_InvoiceListSeaImp();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchVoyageForJobCardNew(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the location based customer.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchLocationBasedCust(string strCond)
        {
            cls_Customer_Mst_Tbl cls_CustMstTbl1 = new cls_Customer_Mst_Tbl();
            string strReturn = null;
            try
            {
                strReturn = cls_CustMstTbl1.FetchLocationBasedCust(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the job reference no_ imp_ for_ do list.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchJobRefNo_Imp_For_DOList(string strCond)
        {
            cls_PrintManager objClass = new cls_PrintManager();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchImportJobRef_DOList(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the job reference no_ imp_ for_ do.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchJobRefNo_Imp_For_DO(string strCond)
        {
            cls_PrintManager objClass = new cls_PrintManager();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchImportJobRef_DO(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches for MBL reference in mbllist.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchForMblRefInMbllist(string strCond)
        {
            cls_MBL_Listings cls_MblList = new cls_MBL_Listings();
            string strReturn = null;
            try
            {
                strReturn = cls_MblList.FetchForMblRefInMbllist(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the job vendors.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchJobVendors(string strCond)
        {
            cls_VendorListing objClass = new cls_VendorListing();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchJobVendors(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches for MBL reference in mblimplist.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchForMblRefInMblimplist(string strCond)
        {
            cls_MBL_Listings cls_MblList1 = new cls_MBL_Listings();
            string strReturn = null;
            try
            {
                strReturn = cls_MblList1.FetchForMblRefInMblimplist(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the agent.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchAgent(string strCond)
        {
            cls_Agent_Details cls_AgntDtls = new cls_Agent_Details();
            string strReturn = null;
            try
            {
                strReturn = cls_AgntDtls.FetchAgent(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the agent invoice.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchAgentInvoice(string strCond)
        {
            cls_Agent_Details cls_AgntDtls1 = new cls_Agent_Details();
            string strReturn = null;
            try
            {
                strReturn = cls_AgntDtls1.FetchAgentInvoice(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches for job reference.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchForJobRef(string strCond)
        {
            cls_JobCardSearch cls_JobCrdSearch = new cls_JobCardSearch();
            string strReturn = null;
            try
            {
                strReturn = cls_JobCrdSearch.FetchForJobRef(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the MBL for job card imp.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchMBLForJobCardImp(string strCond)
        {
            cls_MBL_List objClass = new cls_MBL_List();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchMBLForJobCardImp(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the MBL for job card imp invoice.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchMBLForJobCardImpInvoice(string strCond)
        {
            cls_MBL_List objClass = new cls_MBL_List();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchMBLForJobCardImpInvoice(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the imp certificate job.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchImpCertificateJob(string strCond)
        {
            Cls_Arrival_Notice cls_ArrvlNotice = new Cls_Arrival_Notice();
            string strReturn = null;
            try
            {
                strReturn = cls_ArrvlNotice.FetchImpCertificateJob(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the currency.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchCurrency(string strCond)
        {
            cls_THC_PHC cls_ThcPhc = new cls_THC_PHC();
            string strReturn = null;
            try
            {
                strReturn = cls_ThcPhc.FetchCurrency(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the temporary customer.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchTempCustomer(string strCond)
        {
            Cls_CustomerReconciliation cls_CstmrRecon = new Cls_CustomerReconciliation();
            string strReturn = null;
            try
            {
                strReturn = cls_CstmrRecon.E_FetchTempCustomer(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the type of the container.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchContainerType(string strCond)
        {
            cls_Customer_Mst_Tbl cls_CustMstTbl3 = new cls_Customer_Mst_Tbl();
            string strReturn = null;
            try
            {
                strReturn = cls_CustMstTbl3.FetchContainerType(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the customer.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchCustomer(string strCond)
        {
            cls_Customer_Mst_Tbl cls_CustMstTbl2 = new cls_Customer_Mst_Tbl();
            string strReturn = null;
            try
            {
                strReturn = cls_CustMstTbl2.FetchCustomer(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the pol enquiry.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchPOLEnquiry(string strCond)
        {
            clsPORT_MST_TBL cls_PortMstTbl9 = new clsPORT_MST_TBL();
            string strReturn = null;
            try
            {
                strReturn = cls_PortMstTbl9.FetchPOLEnquiry(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the pod enquiry.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchPODEnquiry(string strCond)
        {
            clsPORT_MST_TBL objClass = new clsPORT_MST_TBL();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchPODEnquiry(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the trade.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchTrade(string strCond)
        {
            clsTRADE_MST_TBL cls_TrdMstTbl = new clsTRADE_MST_TBL();
            string strReturn = null;
            try
            {
                strReturn = cls_TrdMstTbl.FetchTrade(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the emp.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchEmp(string strCond)
        {
            cls_Employee_Mst_Table cls_EmpMstbl = new cls_Employee_Mst_Table();
            string strReturn = null;
            try
            {
                strReturn = cls_EmpMstbl.FetchEmp(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the emp identifier.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchEmpID(string strCond)
        {
            cls_Employee_Mst_Table cls_EmpMstbl2 = new cls_Employee_Mst_Table();
            string strReturn = null;
            try
            {
                strReturn = cls_EmpMstbl2.FetchEmpID(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetch_s the airline_ for_ enquiry.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string Fetch_Airline_For_Enquiry(string strCond)
        {
            Cls_Airline_Definition objClass = new Cls_Airline_Definition();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchAirlineForEnquiry(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        // Used in Airline RFQ Spot Rate Listing
        /// <summary>
        /// Fetch_s the rf q_ for_ airline_ customer.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string Fetch_RFQ_For_Airline_Customer(string strCond)
        {
            Cls_AirlineRFQSpotRate_Listing objClass = new Cls_AirlineRFQSpotRate_Listing();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchRfqForAirlineCustomer(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the commodity.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchCommodity(string strCond)
        {
            cls_Restriction objClass = new cls_Restriction();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchCommodity(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the country.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchCountry(string strCond)
        {
            clsCountry_Mst_Tbl objClass = new clsCountry_Mst_Tbl();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchCountry(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the contract.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchContract(string strCond)
        {
            clsDetentionTariff objClass = new clsDetentionTariff();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchContract(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetch_s the operator_ for_ customer.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string Fetch_Operator_For_Customer(string strCond)
        {
            Cls_OperatorRFQSpotRate_Listing objClass = new Cls_OperatorRFQSpotRate_Listing();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchOperatorForCustomer(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetcs the h_ jo b_ re f_ payreq.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FETCH_JOB_REF_PAYREQ(string strCond)
        {
            cls_JobCardSearch objClass = new cls_JobCardSearch();
            string strReturn = null;
            try
            {
                strReturn = objClass.FETCH_JOB_REF_PAYREQ(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the name of the exp vendor.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchExpVendorName(string strCond)
        {
            Cls_Payment_Requisition objClass = new Cls_Payment_Requisition();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchVendorName(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches for fmcvessel.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchForFMCVESSEL(string strCond)
        {
            Cls_FMCBL objClass = new Cls_FMCBL();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchForVESSEL(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches for HBL reference FMC.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchForHblRefFMC(string strCond)
        {
            Cls_FMCBL objClass = new Cls_FMCBL();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchForHblRef(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches for mawb job reference.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchForMAWBJobRef(string strCond)
        {
            clsMAWBListing objClass = new clsMAWBListing();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchForMAWBJobRef(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the job reference no_ imp_ for_ print manager.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchJobRefNo_Imp_For_PrintManager(string strCond)
        {
            cls_PrintManager objClass = new cls_PrintManager();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchImportJobRef(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the airlineflight.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchAIRLINEFLIGHT(string strCond)
        {
            Cls_Airline_Delivery_Note objClass = new Cls_Airline_Delivery_Note();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchExpAirlineFlight(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the vessel voyage for inv cb.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchVesselVoyageForInvCB(string strCond)
        {
            cls_Vessel_Voyage_Listing objClass = new cls_Vessel_Voyage_Listing();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchVesselVoyageForInvCB(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the MBL for job card exp.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchMBLForJobCardExp(string strCond)
        {
            cls_MBL_List objClass = new cls_MBL_List();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchMBLForJobCardExp(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the MBL for job card.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchMBLForJobCard(string strCond)
        {
            cls_MBL_List objClass = new cls_MBL_List();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchMBLForJobCard(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the masters form.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchMastersForm(string strCond)
        {
            clsUtil_ExportWizard objClass = new clsUtil_ExportWizard();
            string strReturn = null;
            try
            {
                strReturn = (string)objClass.FetchMasterForms(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches for MBL reference imp.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchForMblRefImp(string strCond)
        {
            cls_MBL_Listings objClass = new cls_MBL_Listings();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchForMblRefImp(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the active job card HBLMBL.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchActiveJobCardHBLMBL(string strCond)
        {
            cls_JobCardSearch objClass = new cls_JobCardSearch();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchActiveJobCardHBLMBL(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the vessel voyage imp cp.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchVesselVoyageImpCP(string strCond)
        {
            cls_Vessel_Voyage_Listing objClass = new cls_Vessel_Voyage_Listing();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchVesselVoyageImpCP(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the fleight for inv dp.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchFleightForInvDP(string strCond)
        {
            cls_InvoiceListSea objClass = new cls_InvoiceListSea();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchFleightForInvDP(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches all agents.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchAllAgents(string strCond)
        {
            cls_Agent_Details objClass = new cls_Agent_Details();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchAllAgents(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the VSL flight.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchVslFlight(string strCond)
        {
            cls_Vessel_Voyage_Listing objClass = new cls_Vessel_Voyage_Listing();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchVslFlight(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the operator air line.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchOperatorAirLine(string strCond)
        {
            Cls_Operator_Definition objClass = new Cls_Operator_Definition();
            string strReturn = null;
            try
            {
                //strReturn = objClass.FetchOperatorAirLine(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the operator_ wf.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchOperator_WF(string strCond)
        {
            Cls_Operator_Definition objClass = new Cls_Operator_Definition();
            string strReturn = null;
            try
            {
                //strReturn = objClass.FetchOperator_WF(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion " Common Queries :- Customer,CustomerCategory,POL,POD,Depot,Trade,Operator "

        #region "MIS"

        /// <summary>
        /// Fetches the reference no for track n trace.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchRefNoForTrackNTrace(string strCond)
        {
            cls_TrackAndTrace objClass = new cls_TrackAndTrace();
            string strReturn = null;
            try
            {
                //strReturn = objClass.FetchRefNoForTrackNTrace(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetchvesselvoyagebypolandpods the specified string cond.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string Fetchvesselvoyagebypolandpod(string strCond)
        {
            cls_FrieghtOutstanding clfost = new cls_FrieghtOutstanding();
            string strReturn = null;
            try
            {
                strReturn = clfost.FetchVesselVoyageBYPOLandpod(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches for job reference mawb.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchForJobRefMAWB(string strCond)
        {
            cls_MAWBEntry objClass = new cls_MAWBEntry();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchForJobRefMAWB(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "MIS"

        #region " Common Queries :- EDI, Print "

        /// <summary>
        /// Fetches the HBL for job card s.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchHBLForJobCardS(string strCond)
        {
            cls_HBL_List objClass = new cls_HBL_List();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchHBLForJobCards(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the vessel voyage.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchVesselVoyage(string strCond)
        {
            cls_Vessel_Voyage_Listing objClass = new cls_Vessel_Voyage_Listing();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchVesselVoyage(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches for HBL reference.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchForHblRef(string strCond)
        {
            cls_HBL_Entry objClass = new cls_HBL_Entry();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchForHblRef(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches for HBL reference mawb.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchForHblRefMAWB(string strCond)
        {
            cls_HBL_Entry objClass = new cls_HBL_Entry();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchForHblRefMAWB(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the export ar.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchExportAR(string strCond)
        {
            cls_Util_ExportWizard_Transaction objClass = new cls_Util_ExportWizard_Transaction();
            string strReturn = null;
            try
            {
                //strReturn = objClass.Fetch_Export_AR(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the export ap.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchExportAP(string strCond)
        {
            cls_Util_ExportWizard_Transaction objClass = new cls_Util_ExportWizard_Transaction();
            string strReturn = null;
            try
            {
                //strReturn = objClass.Fetch_Export_AP(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the new currency.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchNewCurrency(string strCond)
        {
            cls_THC_PHC objClass = new cls_THC_PHC();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchNewCurrency(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches for MBL reference in supp inv.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchForMblRefInSuppInv(string strCond)
        {
            cls_MBL_Listings objClass = new cls_MBL_Listings();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchForMblRefInSuppInv(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the vessels RPT.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchVesselsRpt(string strCond)
        {
            cls_Vessel_Voyage_Listing objClass = new cls_Vessel_Voyage_Listing();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchVesselsRpt(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the voyage.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchVoyage(string strCond)
        {
            cls_Vessel_Voyage_Listing objClass = new cls_Vessel_Voyage_Listing();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchVoyage(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion " Common Queries :- EDI, Print "

        #region "User, Executive"

        /// <summary>
        /// Fetches the user.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchUser(string strCond)
        {
            clsUser_Mst_Tbl cls_UMstTbl = new clsUser_Mst_Tbl();
            string strReturn = null;
            try
            {
                strReturn = cls_UMstTbl.FetchUser(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the executive_ all.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchExecutive_All(string strCond)
        {
            cls_SALESPLAN cls_SalePlan = new cls_SALESPLAN();
            string strReturn = null;
            try
            {
                strReturn = cls_SalePlan.FetchExecutive_All(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the executive all.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchExecutiveAll(string strCond)
        {
            string strReturn = null;
            try
            {
                //strReturn = objClass.FetchExecutive_All(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the executive_ other.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchExecutive_Other(string strCond)
        {
            cls_SALESPLAN objClass = new cls_SALESPLAN();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchExecutive_Other(strCond, Convert.ToString(Loc));
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "User, Executive"

        #region "Booking"

        /// <summary>
        /// Fetches the customer category address.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchCustomerCategoryAddress(string strCond)
        {
            cls_AirBookingEntry objClass = new cls_AirBookingEntry();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchCustomerCategoryAddress(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the type of for inco terms pay.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchForIncoTermsPayType(string strCond)
        {
            cls_AirBookingEntry objClass = new cls_AirBookingEntry();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchForIncoTermsPayType(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches for customs s code.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchForCustomsSCode(string strCond)
        {
            cls_AirBookingEntry objClass = new cls_AirBookingEntry();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchForCustomsSCode(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the type of for pack.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchForPackType(string strCond)
        {
            Cls_BookingEntry objClass = new Cls_BookingEntry();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchForPackType(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the ves voy split booking.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchVesVoySplitBooking(string strCond)
        {
            cls_Merge_Split_Booking objClass = new cls_Merge_Split_Booking();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchVesVoySplitBooking(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches for place.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchForPlace(string strCond)
        {
            Cls_BookingEntry objClass = new Cls_BookingEntry();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchForPlace(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the BKG enq search.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchBkgEnqSearch(string strCond)
        {
            cls_BookingEnquiry objClass = new cls_BookingEnquiry();
            string strReturn = null;
            try
            {
                strReturn = objClass.GetBooking(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches for quotation no.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchForQuotationNo(string strCond)
        {
            Cls_BookingEntry objClass = new Cls_BookingEntry();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchForQuotationNo(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the ves voy booking.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchVesVoyBooking(string strCond)
        {
            Cls_BookingEntry objClass = new Cls_BookingEntry();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchVesVoyBooking(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the enquiry reference no.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchEnquiryRefNo(string strCond)
        {
            Cls_QuotationForBookingSea objClass = new Cls_QuotationForBookingSea();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchEnquiryRefNo(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the enquiry reference no air.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchEnquiryRefNoAir(string strCond)
        {
            Cls_QuotationForBookingAir objClass = new Cls_QuotationForBookingAir();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchEnquiryRefNo(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the ves voy_ booking.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchVesVoy_Booking(string strCond)
        {
            Cls_BookingEntry objClass = new Cls_BookingEntry();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchVesVoy_Booking(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the jobcard.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchJobcard(string strCond)
        {
            cls_CustomsBrokerage objClass = new cls_CustomsBrokerage();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchJobcard(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches all jobcard.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchAllJobcard(string strCond)
        {
            cls_TransporterNote objClass = new cls_TransporterNote();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchAllJobcard(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the name of the location by.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchLocationByName(string strCond)
        {
            clsRegionDetails clsRdtls = new clsRegionDetails();
            string strReturn = null;
            try
            {
                strReturn = clsRdtls.FetchLocationByName(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the customer_ temporary.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchCustomer_Temp(string strCond)
        {
            cls_Customer_Mst_Tbl objClass = new cls_Customer_Mst_Tbl();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchCustomer_Temp(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the booking nr split booking.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchBookingNrSplitBooking(string strCond)
        {
            cls_Merge_Split_Booking objClass = new cls_Merge_Split_Booking();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchBookingNrSplitBooking(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the document reference.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchDocRef(string strCond)
        {
            cls_EmailTracking objClass = new cls_EmailTracking();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchDocRef(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the type of the document.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchDocType(string strCond)
        {
            cls_EmailTracking objClass = new cls_EmailTracking();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchDocType(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the send to.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchSendTo(string strCond)
        {
            cls_EmailTracking objClass = new cls_EmailTracking();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchSendTo(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the send from.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchSendFrom(string strCond)
        {
            cls_EmailTracking objClass = new cls_EmailTracking();
            string strReturn = null;
            try
            {
                strReturn = objClass.FetchSendFrom(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Booking"

        #region "Fetch En Common Search Function"

        /// <summary>
        /// Fetches the en common search.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchEnCommonSearch(string strCond = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string strREGION_IN = null;
            string strAREA_IN = null;
            string strDATE_IN = null;
            string strSALEHDR_IN = null;
            string strLOOKUP_IN = null;
            string strActive_IN = null;
            string strMessageNo_IN = null;
            string strWherecond_IN = null;
            string strWherecond2_IN = null;
            string CONDITION_IN = null;
            string CONDITION2_IN = null;
            string strSearchFlag_IN = null;
            //Dim strFromToType As String = ""
            var strNull = "";
            string strTableName_IN = null;
            string strFieldID_IN = null;
            string strFieldName_IN = null;
            string strFieldPK_IN = null;
            arr = strCond.Split('~');
            strLOOKUP_IN = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (strSERACH_IN.IndexOf("$") != -1)
            {
                strSERACH_IN = strSERACH_IN.Replace("$", "&");
            }
            strSERACH_IN = strSERACH_IN.TrimEnd(',');
            strTableName_IN = Convert.ToString(arr.GetValue(2));
            strFieldPK_IN = Convert.ToString(arr.GetValue(3));
            strFieldID_IN = Convert.ToString(arr.GetValue(4));
            strFieldName_IN = Convert.ToString(arr.GetValue(5));
            strActive_IN = Convert.ToString(arr.GetValue(6));
            strWherecond_IN = Convert.ToString(arr.GetValue(7));
            if (arr.Length == 9)
            {
                CONDITION_IN = Convert.ToString(arr.GetValue(8));
            }
            if (arr.Length == 11)
            {
                CONDITION_IN = Convert.ToString(arr.GetValue(8));
                strWherecond2_IN = Convert.ToString(arr.GetValue(9));
                CONDITION2_IN = Convert.ToString(arr.GetValue(10));
            }

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;

                selectCommand.CommandText = objWF.MyUserName + ".EN_COMMON_SEARCH_PKG.GET_EN_COMMON_SEARCH";

                var _with9 = selectCommand.Parameters;
                _with9.Add("TABLE_NAME_IN", (string.IsNullOrEmpty(strTableName_IN) ? strNull : strTableName_IN)).Direction = ParameterDirection.Input;
                _with9.Add("LOOKUP_VALUE_IN", strLOOKUP_IN).Direction = ParameterDirection.Input;
                _with9.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with9.Add("FIELD_NAME_PK_IN", strFieldPK_IN).Direction = ParameterDirection.Input;
                _with9.Add("FIELD_NAME_ID_IN", (string.IsNullOrEmpty(strFieldID_IN) ? strNull : strFieldID_IN)).Direction = ParameterDirection.Input;
                _with9.Add("FIELD_NAME_NAME_IN", (string.IsNullOrEmpty(strFieldName_IN) ? strNull : strFieldName_IN)).Direction = ParameterDirection.Input;
                _with9.Add("WHERECONDITION_IN", (string.IsNullOrEmpty(strWherecond_IN) ? strNull : strWherecond_IN)).Direction = ParameterDirection.Input;
                _with9.Add("CONDITION_IN", (string.IsNullOrEmpty(CONDITION_IN) ? strNull : CONDITION_IN)).Direction = ParameterDirection.Input;

                _with9.Add("WHERECONDITION2_IN", (string.IsNullOrEmpty(strWherecond2_IN) ? strNull : strWherecond2_IN)).Direction = ParameterDirection.Input;
                _with9.Add("CONDITION2_IN", (string.IsNullOrEmpty(CONDITION2_IN) ? strNull : CONDITION2_IN)).Direction = ParameterDirection.Input;

                _with9.Add("ACTIVE_FILED_NAME_IN", (string.IsNullOrEmpty(strActive_IN) ? strNull : strActive_IN)).Direction = ParameterDirection.Input;
                _with9.Add("RETURN_VALUE", OracleDbType.Varchar2, 6000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        #endregion "Fetch En Common Search Function"

        #region "getCommonSearchProcedure"

        /// <summary>
        /// Gets the common search procedure admin.
        /// </summary>
        /// <param name="strSearchFlag">The string search flag.</param>
        /// <param name="strCondition">The string condition.</param>
        /// <param name="strChk">The string CHK.</param>
        /// <returns></returns>
        public string getCommonSearchProcedureAdmin(string strSearchFlag, string strCondition, string strChk = "")
        {
            return FetchCOMMONDATAAdminModule(strCondition, strChk);
        }

        /// <summary>
        /// Fetches the commondata admin module.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <param name="strChk">The string CHK.</param>
        /// <returns></returns>
        public string FetchCOMMONDATAAdminModule(string strCond, string strChk = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string C = null;
            string C1 = null;
            string C3 = null;
            string C4 = null;
            string C5 = null;
            string C6 = null;
            string C7 = null;
            string C8 = null;
            string C9 = null;
            string C10 = null;
            string C11 = null;
            string C12 = null;
            string C13 = null;
            string C14 = null;
            string C15 = null;
            string C16 = null;
            string C17 = null;
            string C18 = null;
            string C19 = null;
            string User_pk = null;
            string strSearchFlag_IN = null;
            string strFromToType = "";
            var strNull = "";
            string SelectedLoc = null;
            string strLOOKUP_IN = null;
            arr = strCond.Split('~');
            // lOOK UP VALUE E OR L
            strLOOKUP_IN = Convert.ToString(arr.GetValue(0));
            //USER TYPE TEXT WHILE PRESSING THE KEY
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (strSERACH_IN.IndexOf("$") != -1)
            {
                strSERACH_IN = strSERACH_IN.Replace("$", "&");
            }
            strSERACH_IN = strSERACH_IN.TrimEnd(',');
            User_pk = Convert.ToString(HttpContext.Current.Session["USER_PK"]);
            //FLAG FOR ES FOR WHICH FETCHING THE RECORDS
            strSearchFlag_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
            {
                C3 = Convert.ToString(arr.GetValue(3));
            }
            if (arr.Length > 4)
            {
                C4 = Convert.ToString(arr.GetValue(4));
            }
            if (arr.Length > 5)
            {
                C5 = Convert.ToString(arr.GetValue(5));
            }
            if (arr.Length > 6)
            {
                C6 = Convert.ToString(arr.GetValue(6));
            }
            if (arr.Length > 7)
            {
                C7 = Convert.ToString(arr.GetValue(7));
            }
            if (arr.Length > 8)
            {
                C8 = Convert.ToString(arr.GetValue(8));
            }
            if (arr.Length > 9)
            {
                C9 = Convert.ToString(arr.GetValue(9));
            }
            if (arr.Length > 10)
            {
                C10 = Convert.ToString(arr.GetValue(10));
            }
            if (arr.Length > 11)
            {
                C11 = Convert.ToString(arr.GetValue(11));
            }
            if (arr.Length > 12)
            {
                C12 = Convert.ToString(arr.GetValue(12));
            }
            if (arr.Length > 13)
            {
                C13 = Convert.ToString(arr.GetValue(13));
            }
            if (arr.Length > 14)
            {
                C14 = Convert.ToString(arr.GetValue(14));
            }
            if (arr.Length > 15)
            {
                C15 = Convert.ToString(arr.GetValue(15));
            }
            if (arr.Length > 16)
            {
                C16 = Convert.ToString(arr.GetValue(16));
            }
            if (arr.Length > 17)
            {
                C17 = Convert.ToString(arr.GetValue(17));
            }
            if (arr.Length > 18)
            {
                C18 = Convert.ToString(arr.GetValue(18));
            }
            if (arr.Length > 19)
            {
                C19 = Convert.ToString(arr.GetValue(19));
            }

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_ADMIN_ENHANCE_SEARCH_PKG.ADMIN_ENHANCE_SEARCH";

                var _with10 = selectCommand.Parameters;
                _with10.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with10.Add("LOOKUP_VALUE_IN", strLOOKUP_IN).Direction = ParameterDirection.Input;
                _with10.Add("SearchFlag", (string.IsNullOrEmpty(strSearchFlag_IN) ? strNull : strSearchFlag_IN)).Direction = ParameterDirection.Input;
                _with10.Add("USER_PK_IN", (string.IsNullOrEmpty(User_pk) ? strNull : User_pk)).Direction = ParameterDirection.Input;
                _with10.Add("PARAM3_IN", (string.IsNullOrEmpty(C3) ? strNull : C3)).Direction = ParameterDirection.Input;
                _with10.Add("PARAM4_IN", (string.IsNullOrEmpty(C4) ? strNull : C4)).Direction = ParameterDirection.Input;
                _with10.Add("PARAM5_IN", (string.IsNullOrEmpty(C5) ? strNull : C5)).Direction = ParameterDirection.Input;
                _with10.Add("PARAM6_IN", (string.IsNullOrEmpty(C6) ? strNull : C6)).Direction = ParameterDirection.Input;
                _with10.Add("PARAM7_IN", (string.IsNullOrEmpty(C7) ? strNull : C7)).Direction = ParameterDirection.Input;
                _with10.Add("PARAM8_IN", (string.IsNullOrEmpty(C8) ? strNull : C8)).Direction = ParameterDirection.Input;
                _with10.Add("PARAM9_IN", (string.IsNullOrEmpty(C9) ? strNull : C9)).Direction = ParameterDirection.Input;
                _with10.Add("PARAM10_IN", (string.IsNullOrEmpty(C10) ? strNull : C10)).Direction = ParameterDirection.Input;
                _with10.Add("PARAM11_IN", (string.IsNullOrEmpty(C11) ? strNull : C11)).Direction = ParameterDirection.Input;
                _with10.Add("PARAM12_IN", (string.IsNullOrEmpty(C12) ? strNull : C12)).Direction = ParameterDirection.Input;
                _with10.Add("PARAM13_IN", (string.IsNullOrEmpty(C13) ? strNull : C13)).Direction = ParameterDirection.Input;
                _with10.Add("PARAM14_IN", (string.IsNullOrEmpty(C14) ? strNull : C14)).Direction = ParameterDirection.Input;
                _with10.Add("PARAM15_IN", (string.IsNullOrEmpty(C15) ? strNull : C15)).Direction = ParameterDirection.Input;
                _with10.Add("PARAM16_IN", (string.IsNullOrEmpty(C16) ? strNull : C16)).Direction = ParameterDirection.Input;
                _with10.Add("PARAM17_IN", (string.IsNullOrEmpty(C17) ? strNull : C17)).Direction = ParameterDirection.Input;
                _with10.Add("PARAM18_IN", (string.IsNullOrEmpty(C18) ? strNull : C18)).Direction = ParameterDirection.Input;
                _with10.Add("PARAM19_IN", (string.IsNullOrEmpty(C19) ? strNull : C19)).Direction = ParameterDirection.Input;
                _with10.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                OracleClob clob = null;
                clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        #endregion "getCommonSearchProcedure"

        #region "Common Search Procedure for RatingandTariff Module"

        /// <summary>
        /// Gets the common search rating.
        /// </summary>
        /// <param name="strSearchFlag">The string search flag.</param>
        /// <param name="strCondition">The string condition.</param>
        /// <param name="strChk">The string CHK.</param>
        /// <returns></returns>
        public string getCommonSearchRating(string strSearchFlag, string strCondition, string strChk = "")
        {
            return FetchCommonData_Rating(strCondition, strChk);
        }

        /// <summary>
        /// Fetches the common data_ rating.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <param name="strChk">The string CHK.</param>
        /// <returns></returns>
        public string FetchCommonData_Rating(string strCond, string strChk = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string C = null;
            string C1 = null;
            string C3 = null;
            string C4 = null;
            string C5 = null;
            string C6 = null;
            string C7 = null;
            string C8 = null;
            string C9 = null;
            string C10 = null;
            string C11 = null;
            string C12 = null;
            string C13 = null;
            string C14 = null;
            string C15 = null;
            string C16 = null;
            string User_pk = null;
            string strSearchFlag_IN = null;
            string strFromToType = "";
            var strNull = "";
            string SelectedLoc = null;
            string strLOOKUP_IN = null;
            arr = strCond.Split('~');
            // lOOK UP VALUE E OR L
            strLOOKUP_IN = Convert.ToString(arr.GetValue(0));
            //USER TYPE TEXT WHILE PRESSING THE KEY
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (strSERACH_IN.IndexOf("$") != -1)
            {
                strSERACH_IN = strSERACH_IN.Replace("$", "&");
            }
            strSERACH_IN = strSERACH_IN.TrimEnd(',');
            User_pk = Convert.ToString(HttpContext.Current.Session["USER_PK"]);
            //FLAG FOR ES FOR WHICH FETCHING THE RECORDS
            strSearchFlag_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
            {
                C3 = Convert.ToString(arr.GetValue(3));
            }
            if (arr.Length > 4)
            {
                C4 = Convert.ToString(arr.GetValue(4));
            }
            if (arr.Length > 5)
            {
                C5 = Convert.ToString(arr.GetValue(5));
            }
            if (arr.Length > 6)
            {
                C6 = Convert.ToString(arr.GetValue(6));
            }
            if (arr.Length > 7)
            {
                C7 = Convert.ToString(arr.GetValue(7));
            }
            if (arr.Length > 8)
            {
                C8 = Convert.ToString(arr.GetValue(8));
            }
            if (arr.Length > 9)
            {
                C9 = Convert.ToString(arr.GetValue(9));
            }
            if (arr.Length > 10)
            {
                C10 = Convert.ToString(arr.GetValue(10));
            }
            if (arr.Length > 11)
            {
                C11 = Convert.ToString(arr.GetValue(11));
            }
            if (arr.Length > 12)
            {
                C12 = Convert.ToString(arr.GetValue(12));
            }
            if (arr.Length > 13)
            {
                C13 = Convert.ToString(arr.GetValue(13));
            }
            if (arr.Length > 14)
            {
                C14 = Convert.ToString(arr.GetValue(14));
            }
            if (arr.Length > 15)
            {
                C15 = Convert.ToString(arr.GetValue(15));
            }
            if (arr.Length > 16)
            {
                C16 = Convert.ToString(arr.GetValue(16));
            }

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".GET_EN_COMMON_TARIFF_PKG.GET_COMMON_TARIFF";

                var _with11 = selectCommand.Parameters;
                _with11.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with11.Add("LOOKUP_VALUE_IN", strLOOKUP_IN).Direction = ParameterDirection.Input;
                _with11.Add("SearchFlag", (string.IsNullOrEmpty(strSearchFlag_IN) ? strNull : strSearchFlag_IN)).Direction = ParameterDirection.Input;
                _with11.Add("USER_PK_IN", (string.IsNullOrEmpty(User_pk) ? strNull : User_pk)).Direction = ParameterDirection.Input;
                _with11.Add("PARAM3_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C3)).Direction = ParameterDirection.Input;
                _with11.Add("PARAM4_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C4)).Direction = ParameterDirection.Input;
                _with11.Add("PARAM5_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C5)).Direction = ParameterDirection.Input;
                _with11.Add("PARAM6_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C6)).Direction = ParameterDirection.Input;
                _with11.Add("PARAM7_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C7)).Direction = ParameterDirection.Input;
                _with11.Add("PARAM8_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C8)).Direction = ParameterDirection.Input;
                _with11.Add("PARAM9_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C9)).Direction = ParameterDirection.Input;
                _with11.Add("PARAM10_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C10)).Direction = ParameterDirection.Input;
                _with11.Add("PARAM11_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C11)).Direction = ParameterDirection.Input;
                _with11.Add("PARAM12_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C12)).Direction = ParameterDirection.Input;
                _with11.Add("PARAM13_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C13)).Direction = ParameterDirection.Input;
                _with11.Add("PARAM14_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C14)).Direction = ParameterDirection.Input;
                _with11.Add("PARAM15_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C15)).Direction = ParameterDirection.Input;
                _with11.Add("PARAM16_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C16)).Direction = ParameterDirection.Input;
                _with11.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                OracleClob clob = null;
                clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        #endregion "Common Search Procedure for RatingandTariff Module"

        #region "Common Search Procedure for Search CBJCReports Module"

        /// <summary>
        /// Gets the common search CBJC reports.
        /// </summary>
        /// <param name="strSearchFlag">The string search flag.</param>
        /// <param name="strCondition">The string condition.</param>
        /// <param name="strChk">The string CHK.</param>
        /// <returns></returns>
        public string getCommonSearchCBJCReports(string strSearchFlag, string strCondition, string strChk = "")
        {
            return FetchCommonCBJCReports(strCondition, strChk);
        }

        /// <summary>
        /// Fetches the common CBJC reports.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <param name="strChk">The string CHK.</param>
        /// <returns></returns>
        public string FetchCommonCBJCReports(string strCond, string strChk = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string C = null;
            string C1 = null;
            string C3 = null;
            string C4 = null;
            string C5 = null;
            string C6 = null;
            string C7 = null;
            string C8 = null;
            string C9 = null;
            string C10 = null;
            string C11 = null;
            string C12 = null;
            string C13 = null;
            string C14 = null;
            string C15 = null;
            string C16 = null;
            string C17 = null;
            string C18 = null;
            string C19 = null;
            string C20 = null;
            string User_pk = null;
            string strSearchFlag_IN = null;
            string strFromToType = "";
            var strNull = "";
            string SelectedLoc = null;
            string strLOOKUP_IN = null;
            arr = strCond.Split('~');
            strLOOKUP_IN = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (strSERACH_IN.IndexOf("$") != -1)
            {
                strSERACH_IN = strSERACH_IN.Replace("$", "&");
            }
            strSERACH_IN = strSERACH_IN.TrimEnd(',');
            User_pk = Convert.ToString(HttpContext.Current.Session["USER_PK"]);
            strSearchFlag_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
            {
                C3 = Convert.ToString(arr.GetValue(3));
            }
            if (arr.Length > 4)
            {
                C4 = Convert.ToString(arr.GetValue(4));
            }
            if (arr.Length > 5)
            {
                C5 = Convert.ToString(arr.GetValue(5));
            }
            if (arr.Length > 6)
            {
                C6 = Convert.ToString(arr.GetValue(6));
            }
            if (arr.Length > 7)
            {
                C7 = Convert.ToString(arr.GetValue(7));
            }
            if (arr.Length > 8)
            {
                C8 = Convert.ToString(arr.GetValue(8));
            }
            if (arr.Length > 9)
            {
                C9 = Convert.ToString(arr.GetValue(9));
            }
            if (arr.Length > 10)
            {
                C10 = Convert.ToString(arr.GetValue(10));
            }
            if (arr.Length > 11)
            {
                C11 = Convert.ToString(arr.GetValue(11));
            }
            if (arr.Length > 12)
            {
                C12 = Convert.ToString(arr.GetValue(12));
            }
            if (arr.Length > 13)
            {
                C13 = Convert.ToString(arr.GetValue(13));
            }
            if (arr.Length > 14)
            {
                C14 = Convert.ToString(arr.GetValue(14));
            }
            if (arr.Length > 15)
            {
                C15 = Convert.ToString(arr.GetValue(15));
            }
            if (arr.Length > 16)
            {
                C16 = Convert.ToString(arr.GetValue(16));
            }
            if (arr.Length > 17)
            {
                C17 = Convert.ToString(arr.GetValue(17));
            }
            if (arr.Length > 18)
            {
                C18 = Convert.ToString(arr.GetValue(18));
            }
            if (arr.Length > 19)
            {
                C19 = Convert.ToString(arr.GetValue(19));
            }
            if (arr.Length > 20)
            {
                C20 = Convert.ToString(arr.GetValue(20));
            }
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".GET_EN_CBJCREPORTS_PKG.GET_CBJC_REPORTS";

                var _with12 = selectCommand.Parameters;
                _with12.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with12.Add("LOOKUP_VALUE_IN", strLOOKUP_IN).Direction = ParameterDirection.Input;
                _with12.Add("SearchFlag", (string.IsNullOrEmpty(strSearchFlag_IN) ? strNull : strSearchFlag_IN)).Direction = ParameterDirection.Input;
                _with12.Add("USER_PK_IN", (string.IsNullOrEmpty(User_pk) ? strNull : User_pk)).Direction = ParameterDirection.Input;
                _with12.Add("PARAM3_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C3)).Direction = ParameterDirection.Input;
                _with12.Add("PARAM4_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C4)).Direction = ParameterDirection.Input;
                _with12.Add("PARAM5_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C5)).Direction = ParameterDirection.Input;
                _with12.Add("PARAM6_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C6)).Direction = ParameterDirection.Input;
                _with12.Add("PARAM7_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C7)).Direction = ParameterDirection.Input;
                _with12.Add("PARAM8_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C8)).Direction = ParameterDirection.Input;
                _with12.Add("PARAM9_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C9)).Direction = ParameterDirection.Input;
                _with12.Add("PARAM10_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C10)).Direction = ParameterDirection.Input;
                _with12.Add("PARAM11_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C11)).Direction = ParameterDirection.Input;
                _with12.Add("PARAM12_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C12)).Direction = ParameterDirection.Input;
                _with12.Add("PARAM13_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C13)).Direction = ParameterDirection.Input;
                _with12.Add("PARAM14_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C14)).Direction = ParameterDirection.Input;
                _with12.Add("PARAM15_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C15)).Direction = ParameterDirection.Input;
                _with12.Add("PARAM16_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C16)).Direction = ParameterDirection.Input;
                _with12.Add("PARAM17_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C17)).Direction = ParameterDirection.Input;
                _with12.Add("PARAM18_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C18)).Direction = ParameterDirection.Input;
                _with12.Add("PARAM19_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C19)).Direction = ParameterDirection.Input;
                _with12.Add("PARAM20_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C20)).Direction = ParameterDirection.Input;
                _with12.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                OracleClob clob = null;
                clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        #endregion "Common Search Procedure for Search CBJCReports Module"

        #region "Common Search Procedure for Search CBJCReports Module"

        /// <summary>
        /// Gets the common search edi reports.
        /// </summary>
        /// <param name="strSearchFlag">The string search flag.</param>
        /// <param name="strCondition">The string condition.</param>
        /// <param name="strChk">The string CHK.</param>
        /// <returns></returns>
        public string getCommonSearchEDIReports(string strSearchFlag, string strCondition, string strChk = "")
        {
            return FetchCommonEDIReports(strCondition, strChk);
        }

        /// <summary>
        /// Fetches the common edi reports.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <param name="strChk">The string CHK.</param>
        /// <returns></returns>
        public string FetchCommonEDIReports(string strCond, string strChk = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string C = null;
            string C1 = null;
            string C3 = null;
            string C4 = null;
            string C5 = null;
            string C6 = null;
            string C7 = null;
            string C8 = null;
            string C9 = null;
            string C10 = null;
            string C11 = null;
            string C12 = null;
            string C13 = null;
            string C14 = null;
            string C15 = null;
            string C16 = null;
            string C17 = null;
            string C18 = null;
            string C19 = null;
            string C20 = null;
            string User_pk = null;
            string strSearchFlag_IN = null;
            string strFromToType = "";
            var strNull = "";
            string SelectedLoc = null;
            string strLOOKUP_IN = null;
            arr = strCond.Split('~');
            strLOOKUP_IN = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (strSERACH_IN.IndexOf("$") != -1)
            {
                strSERACH_IN = strSERACH_IN.Replace("$", "&");
            }
            strSERACH_IN = strSERACH_IN.TrimEnd(',');
            User_pk = Convert.ToString(HttpContext.Current.Session["USER_PK"]);
            strSearchFlag_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
            {
                C3 = Convert.ToString(arr.GetValue(3));
            }
            if (arr.Length > 4)
            {
                C4 = Convert.ToString(arr.GetValue(4));
            }
            if (arr.Length > 5)
            {
                C5 = Convert.ToString(arr.GetValue(5));
            }
            if (arr.Length > 6)
            {
                C6 = Convert.ToString(arr.GetValue(6));
            }
            if (arr.Length > 7)
            {
                C7 = Convert.ToString(arr.GetValue(7));
            }
            if (arr.Length > 8)
            {
                C8 = Convert.ToString(arr.GetValue(8));
            }
            if (arr.Length > 9)
            {
                C9 = Convert.ToString(arr.GetValue(9));
            }
            if (arr.Length > 10)
            {
                C10 = Convert.ToString(arr.GetValue(10));
            }
            if (arr.Length > 11)
            {
                C11 = Convert.ToString(arr.GetValue(11));
            }
            if (arr.Length > 12)
            {
                C12 = Convert.ToString(arr.GetValue(12));
            }
            if (arr.Length > 13)
            {
                C13 = Convert.ToString(arr.GetValue(13));
            }
            if (arr.Length > 14)
            {
                C14 = Convert.ToString(arr.GetValue(14));
            }
            if (arr.Length > 15)
            {
                C15 = Convert.ToString(arr.GetValue(15));
            }
            if (arr.Length > 16)
            {
                C16 = Convert.ToString(arr.GetValue(16));
            }
            if (arr.Length > 17)
            {
                C17 = Convert.ToString(arr.GetValue(17));
            }
            if (arr.Length > 18)
            {
                C18 = Convert.ToString(arr.GetValue(18));
            }
            if (arr.Length > 19)
            {
                C19 = Convert.ToString(arr.GetValue(19));
            }
            if (arr.Length > 20)
            {
                C20 = Convert.ToString(arr.GetValue(20));
            }
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".GET_EN_COMMON_EDI_PKG.GET_EDI_REPORTS";

                var _with13 = selectCommand.Parameters;
                _with13.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with13.Add("LOOKUP_VALUE_IN", strLOOKUP_IN).Direction = ParameterDirection.Input;
                _with13.Add("SearchFlag", (string.IsNullOrEmpty(strSearchFlag_IN) ? strNull : strSearchFlag_IN)).Direction = ParameterDirection.Input;
                _with13.Add("USER_PK_IN", (string.IsNullOrEmpty(User_pk) ? strNull : User_pk)).Direction = ParameterDirection.Input;
                _with13.Add("PARAM3_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C3)).Direction = ParameterDirection.Input;
                _with13.Add("PARAM4_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C4)).Direction = ParameterDirection.Input;
                _with13.Add("PARAM5_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C5)).Direction = ParameterDirection.Input;
                _with13.Add("PARAM6_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C6)).Direction = ParameterDirection.Input;
                _with13.Add("PARAM7_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C7)).Direction = ParameterDirection.Input;
                _with13.Add("PARAM8_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C8)).Direction = ParameterDirection.Input;
                _with13.Add("PARAM9_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C9)).Direction = ParameterDirection.Input;
                _with13.Add("PARAM10_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C10)).Direction = ParameterDirection.Input;
                _with13.Add("PARAM11_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C11)).Direction = ParameterDirection.Input;
                _with13.Add("PARAM12_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C12)).Direction = ParameterDirection.Input;
                _with13.Add("PARAM13_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C13)).Direction = ParameterDirection.Input;
                _with13.Add("PARAM14_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C14)).Direction = ParameterDirection.Input;
                _with13.Add("PARAM15_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C15)).Direction = ParameterDirection.Input;
                _with13.Add("PARAM16_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C16)).Direction = ParameterDirection.Input;
                _with13.Add("PARAM17_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C17)).Direction = ParameterDirection.Input;
                _with13.Add("PARAM18_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C18)).Direction = ParameterDirection.Input;
                _with13.Add("PARAM19_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C19)).Direction = ParameterDirection.Input;
                _with13.Add("PARAM20_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C20)).Direction = ParameterDirection.Input;
                _with13.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                OracleClob clob = null;
                clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        #endregion "Common Search Procedure for Search CBJCReports Module"

        #region "Common Search Procedure for MIS Module"

        /// <summary>
        /// Gets the common search mis.
        /// </summary>
        /// <param name="strSearchFlag">The string search flag.</param>
        /// <param name="strCondition">The string condition.</param>
        /// <param name="strChk">The string CHK.</param>
        /// <returns></returns>
        public string getCommonSearchMIS(string strSearchFlag, string strCondition, string strChk = "")
        {
            return FetchCommonData_MIS(strCondition, strChk);
        }

        /// <summary>
        /// Fetches the common data_ mis.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <param name="strChk">The string CHK.</param>
        /// <returns></returns>
        public string FetchCommonData_MIS(string strCond, string strChk = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string C = null;
            string C1 = null;
            string C3 = null;
            string C4 = null;
            string C5 = null;
            string C6 = null;
            string C7 = null;
            string C8 = null;
            string C9 = null;
            string C10 = null;
            string C11 = null;
            string C12 = null;
            string C13 = null;
            string C14 = null;
            string C15 = null;
            string C16 = null;
            string C17 = null;
            string C18 = null;
            string C19 = null;
            string C20 = null;
            string C21 = null;
            string C22 = null;
            string C23 = null;
            string User_pk = null;
            string strSearchFlag_IN = null;
            string strFromToType = "";
            var strNull = "";
            string SelectedLoc = null;
            string strLOOKUP_IN = null;
            arr = strCond.Split('~');
            // lOOK UP VALUE E OR L
            strLOOKUP_IN = Convert.ToString(arr.GetValue(0)); ;
            //USER TYPE TEXT WHILE PRESSING THE KEY
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (strSERACH_IN.IndexOf("$") != -1)
            {
                strSERACH_IN = strSERACH_IN.Replace("$", "&");
            }
            strSERACH_IN = strSERACH_IN.TrimEnd(',');
            User_pk = Convert.ToString(HttpContext.Current.Session["USER_PK"]);
            //FLAG FOR ES FOR WHICH FETCHING THE RECORDS
            strSearchFlag_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
            {
                C3 = Convert.ToString(arr.GetValue(3));
            }
            if (arr.Length > 4)
            {
                C4 = Convert.ToString(arr.GetValue(4));
            }
            if (arr.Length > 5)
            {
                C5 = Convert.ToString(arr.GetValue(5));
            }
            if (arr.Length > 6)
            {
                C6 = Convert.ToString(arr.GetValue(6));
            }
            if (arr.Length > 7)
            {
                C7 = Convert.ToString(arr.GetValue(7));
            }
            if (arr.Length > 8)
            {
                C8 = Convert.ToString(arr.GetValue(8));
            }
            if (arr.Length > 9)
            {
                C9 = Convert.ToString(arr.GetValue(9));
            }
            if (arr.Length > 10)
            {
                C10 = Convert.ToString(arr.GetValue(10));
            }
            if (arr.Length > 11)
            {
                C11 = Convert.ToString(arr.GetValue(11));
            }
            if (arr.Length > 12)
            {
                C12 = Convert.ToString(arr.GetValue(12));
            }
            if (arr.Length > 13)
            {
                C13 = Convert.ToString(arr.GetValue(13));
            }
            if (arr.Length > 14)
            {
                C14 = Convert.ToString(arr.GetValue(14));
            }
            if (arr.Length > 15)
            {
                C15 = Convert.ToString(arr.GetValue(15));
            }
            if (arr.Length > 16)
            {
                C16 = Convert.ToString(arr.GetValue(16));
            }
            if (arr.Length > 17)
            {
                C17 = Convert.ToString(arr.GetValue(17));
            }
            if (arr.Length > 18)
            {
                C18 = Convert.ToString(arr.GetValue(18));
            }
            if (arr.Length > 19)
            {
                C19 = Convert.ToString(arr.GetValue(19));
            }
            if (arr.Length > 20)
            {
                C20 = Convert.ToString(arr.GetValue(20));
            }
            if (arr.Length > 21)
            {
                C21 = Convert.ToString(arr.GetValue(21));
            }
            if (arr.Length > 22)
            {
                C22 = Convert.ToString(arr.GetValue(22));
            }
            if (arr.Length > 23)
            {
                C23 = Convert.ToString(arr.GetValue(23));
            }
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".GET_EN_COMMON_MIS_PKG.GET_COMMON_MIS";

                var _with14 = selectCommand.Parameters;
                _with14.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with14.Add("LOOKUP_VALUE_IN", strLOOKUP_IN).Direction = ParameterDirection.Input;
                _with14.Add("SearchFlag", (string.IsNullOrEmpty(strSearchFlag_IN) ? strNull : strSearchFlag_IN)).Direction = ParameterDirection.Input;
                _with14.Add("USER_PK_IN", (string.IsNullOrEmpty(User_pk) ? strNull : User_pk)).Direction = ParameterDirection.Input;
                _with14.Add("PARAM3_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C3)).Direction = ParameterDirection.Input;
                _with14.Add("PARAM4_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C4)).Direction = ParameterDirection.Input;
                _with14.Add("PARAM5_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C5)).Direction = ParameterDirection.Input;
                _with14.Add("PARAM6_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C6)).Direction = ParameterDirection.Input;
                _with14.Add("PARAM7_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C7)).Direction = ParameterDirection.Input;
                _with14.Add("PARAM8_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C8)).Direction = ParameterDirection.Input;
                _with14.Add("PARAM9_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C9)).Direction = ParameterDirection.Input;
                _with14.Add("PARAM10_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C10)).Direction = ParameterDirection.Input;
                _with14.Add("PARAM11_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C11)).Direction = ParameterDirection.Input;
                _with14.Add("PARAM12_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C12)).Direction = ParameterDirection.Input;
                _with14.Add("PARAM13_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C13)).Direction = ParameterDirection.Input;
                _with14.Add("PARAM14_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C14)).Direction = ParameterDirection.Input;
                _with14.Add("PARAM15_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C15)).Direction = ParameterDirection.Input;
                _with14.Add("PARAM16_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C16)).Direction = ParameterDirection.Input;
                _with14.Add("PARAM17_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C17)).Direction = ParameterDirection.Input;
                _with14.Add("PARAM18_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C18)).Direction = ParameterDirection.Input;
                _with14.Add("PARAM19_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C19)).Direction = ParameterDirection.Input;
                _with14.Add("PARAM20_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C20)).Direction = ParameterDirection.Input;
                _with14.Add("PARAM21_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C21)).Direction = ParameterDirection.Input;
                _with14.Add("PARAM22_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C22)).Direction = ParameterDirection.Input;
                _with14.Add("PARAM23_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C23)).Direction = ParameterDirection.Input;
                _with14.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                OracleClob clob = null;
                clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        #endregion "Common Search Procedure for MIS Module"

        #region "Common Search Procedure for SUPPLIR MGMT Module"

        /// <summary>
        /// Gets the common search suppliermgmt.
        /// </summary>
        /// <param name="strSearchFlag">The string search flag.</param>
        /// <param name="strCondition">The string condition.</param>
        /// <param name="strChk">The string CHK.</param>
        /// <returns></returns>
        public string getCommonSearchSUPPLIERMGMT(string strSearchFlag, string strCondition, string strChk = "")
        {
            return FetchCommonData_SUPPLIERMGMT(strCondition, strChk);
        }

        /// <summary>
        /// Fetches the common data_ suppliermgmt.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <param name="strChk">The string CHK.</param>
        /// <returns></returns>
        public string FetchCommonData_SUPPLIERMGMT(string strCond, string strChk = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string C = null;
            string C1 = null;
            string C3 = null;
            string C4 = null;
            string C5 = null;
            string C6 = null;
            string C7 = null;
            string C8 = null;
            string C9 = null;
            string C10 = null;
            string C11 = null;
            string C12 = null;
            string C13 = null;
            string C14 = null;
            string C15 = null;
            string C16 = null;
            string User_pk = null;
            string strSearchFlag_IN = null;
            string strFromToType = "";
            var strNull = "";
            string SelectedLoc = null;
            string strLOOKUP_IN = null;
            arr = strCond.Split('~');
            // lOOK UP VALUE E OR L
            strLOOKUP_IN = Convert.ToString(arr.GetValue(0)); ;
            //USER TYPE TEXT WHILE PRESSING THE KEY
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (strSERACH_IN.IndexOf("$") != -1)
            {
                strSERACH_IN = strSERACH_IN.Replace("$", "&");
            }
            strSERACH_IN = strSERACH_IN.TrimEnd(',');
            User_pk = Convert.ToString(HttpContext.Current.Session["USER_PK"]);
            //FLAG FOR ES FOR WHICH FETCHING THE RECORDS
            strSearchFlag_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
            {
                C3 = Convert.ToString(arr.GetValue(3));
            }
            if (arr.Length > 4)
            {
                C4 = Convert.ToString(arr.GetValue(4));
            }
            if (arr.Length > 5)
            {
                C5 = Convert.ToString(arr.GetValue(5));
            }
            if (arr.Length > 6)
            {
                C6 = Convert.ToString(arr.GetValue(6));
            }
            if (arr.Length > 7)
            {
                C7 = Convert.ToString(arr.GetValue(7));
            }
            if (arr.Length > 8)
            {
                C8 = Convert.ToString(arr.GetValue(8));
            }
            if (arr.Length > 9)
            {
                C9 = Convert.ToString(arr.GetValue(9));
            }
            if (arr.Length > 10)
            {
                C10 = Convert.ToString(arr.GetValue(10));
            }
            if (arr.Length > 11)
            {
                C11 = Convert.ToString(arr.GetValue(11));
            }
            if (arr.Length > 12)
            {
                C12 = Convert.ToString(arr.GetValue(12));
            }
            if (arr.Length > 13)
            {
                C13 = Convert.ToString(arr.GetValue(13));
            }
            if (arr.Length > 14)
            {
                C14 = Convert.ToString(arr.GetValue(14));
            }
            if (arr.Length > 15)
            {
                C15 = Convert.ToString(arr.GetValue(15));
            }
            if (arr.Length > 16)
            {
                C16 = Convert.ToString(arr.GetValue(16));
            }
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".GET_EN_COMMON_SUPPLIERMGMT_PKG.GET_COMMON_SUPPLIERMGMT";

                var _with15 = selectCommand.Parameters;
                _with15.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with15.Add("LOOKUP_VALUE_IN", strLOOKUP_IN).Direction = ParameterDirection.Input;
                _with15.Add("SearchFlag", (string.IsNullOrEmpty(strSearchFlag_IN) ? strNull : strSearchFlag_IN)).Direction = ParameterDirection.Input;
                _with15.Add("USER_PK_IN", (string.IsNullOrEmpty(User_pk) ? strNull : User_pk)).Direction = ParameterDirection.Input;
                _with15.Add("PARAM3_IN", (string.IsNullOrEmpty(C13.Trim()) ? strNull : C3)).Direction = ParameterDirection.Input;
                _with15.Add("PARAM4_IN", (string.IsNullOrEmpty(C4.Trim()) ? strNull : C4)).Direction = ParameterDirection.Input;
                _with15.Add("PARAM5_IN", (string.IsNullOrEmpty(C5.Trim()) ? strNull : C5)).Direction = ParameterDirection.Input;
                _with15.Add("PARAM6_IN", (string.IsNullOrEmpty(C6.Trim()) ? strNull : C6)).Direction = ParameterDirection.Input;
                _with15.Add("PARAM7_IN", (string.IsNullOrEmpty(C7.Trim()) ? strNull : C7)).Direction = ParameterDirection.Input;
                _with15.Add("PARAM8_IN", (string.IsNullOrEmpty(C8.Trim()) ? strNull : C8)).Direction = ParameterDirection.Input;
                _with15.Add("PARAM9_IN", (string.IsNullOrEmpty(C9.Trim()) ? strNull : C9)).Direction = ParameterDirection.Input;
                _with15.Add("PARAM10_IN", (string.IsNullOrEmpty(C10.Trim()) ? strNull : C10)).Direction = ParameterDirection.Input;
                _with15.Add("PARAM11_IN", (string.IsNullOrEmpty(C11.Trim()) ? strNull : C11)).Direction = ParameterDirection.Input;
                _with15.Add("PARAM12_IN", (string.IsNullOrEmpty(C12.Trim()) ? strNull : C12)).Direction = ParameterDirection.Input;
                _with15.Add("PARAM13_IN", (string.IsNullOrEmpty(C13.Trim()) ? strNull : C13)).Direction = ParameterDirection.Input;
                _with15.Add("PARAM14_IN", (string.IsNullOrEmpty(C14.Trim()) ? strNull : C14)).Direction = ParameterDirection.Input;
                _with15.Add("PARAM15_IN", (string.IsNullOrEmpty(C15.Trim()) ? strNull : C15)).Direction = ParameterDirection.Input;
                _with15.Add("PARAM16_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C16)).Direction = ParameterDirection.Input;
                _with15.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                OracleClob clob = null;
                clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        #endregion "Common Search Procedure for SUPPLIR MGMT Module"

        #region "Common Search Procedure for Receivable Module"

        /// <summary>
        /// Gets the common search receivables.
        /// </summary>
        /// <param name="strSearchFlag">The string search flag.</param>
        /// <param name="strCondition">The string condition.</param>
        /// <param name="strChk">The string CHK.</param>
        /// <returns></returns>
        public string getCommonSearchReceivables(string strSearchFlag, string strCondition, string strChk = "")
        {
            return FetchCommonData_Receivables(strCondition, strChk);
        }

        /// <summary>
        /// Fetches the common data_ receivables.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <param name="strChk">The string CHK.</param>
        /// <returns></returns>
        public string FetchCommonData_Receivables(string strCond, string strChk = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string C = null;
            string C1 = null;
            string C3 = null;
            string C4 = null;
            string C5 = null;
            string C6 = null;
            string C7 = null;
            string C8 = null;
            string C9 = null;
            string C10 = null;
            string C11 = null;
            string C12 = null;
            string C13 = null;
            string C14 = null;
            string C15 = null;
            string C16 = null;
            string User_pk = null;
            string strSearchFlag_IN = null;
            string strFromToType = "";
            var strNull = "";
            string SelectedLoc = null;
            string strLOOKUP_IN = null;
            arr = strCond.Split('~');
            // lOOK UP VALUE E OR L
            strLOOKUP_IN = Convert.ToString(arr.GetValue(0)); ;
            //USER TYPE TEXT WHILE PRESSING THE KEY
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (strSERACH_IN.IndexOf("$") != -1)
            {
                strSERACH_IN = strSERACH_IN.Replace("$", "&");
            }
            strSERACH_IN = strSERACH_IN.TrimEnd(',');
            User_pk = Convert.ToString(HttpContext.Current.Session["USER_PK"]);
            //FLAG FOR ES FOR WHICH FETCHING THE RECORDS
            strSearchFlag_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
            {
                C3 = Convert.ToString(arr.GetValue(3));
            }
            if (arr.Length > 4)
            {
                C4 = Convert.ToString(arr.GetValue(4));
            }
            if (arr.Length > 5)
            {
                C5 = Convert.ToString(arr.GetValue(5));
            }
            if (arr.Length > 6)
            {
                C6 = Convert.ToString(arr.GetValue(6));
            }
            if (arr.Length > 7)
            {
                C7 = Convert.ToString(arr.GetValue(7));
            }
            if (arr.Length > 8)
            {
                C8 = Convert.ToString(arr.GetValue(8));
            }
            if (arr.Length > 9)
            {
                C9 = Convert.ToString(arr.GetValue(9));
            }
            if (arr.Length > 10)
            {
                C10 = Convert.ToString(arr.GetValue(10));
            }
            if (arr.Length > 11)
            {
                C11 = Convert.ToString(arr.GetValue(11));
            }
            if (arr.Length > 12)
            {
                C12 = Convert.ToString(arr.GetValue(12));
            }
            if (arr.Length > 13)
            {
                C13 = Convert.ToString(arr.GetValue(13));
            }
            if (arr.Length > 14)
            {
                C14 = Convert.ToString(arr.GetValue(14));
            }
            if (arr.Length > 15)
            {
                C15 = Convert.ToString(arr.GetValue(15));
            }
            if (arr.Length > 16)
            {
                C16 = Convert.ToString(arr.GetValue(16));
            }

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".GET_EN_COMMON_RECEIVABLES_PKG.GET_RECEIVABLES_ES";

                var _with16 = selectCommand.Parameters;
                _with16.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with16.Add("LOOKUP_VALUE_IN", strLOOKUP_IN).Direction = ParameterDirection.Input;
                _with16.Add("SearchFlag", (string.IsNullOrEmpty(strSearchFlag_IN) ? strNull : strSearchFlag_IN)).Direction = ParameterDirection.Input;
                _with16.Add("USER_PK_IN", (string.IsNullOrEmpty(User_pk) ? strNull : User_pk)).Direction = ParameterDirection.Input;
                //.Add("PARAM3_IN", IIf(Trim(C3) = "", strNull, C3)).Direction = ParameterDirection.Input
                //.Add("PARAM4_IN", IIf(Trim(C4) = "", strNull, C4)).Direction = ParameterDirection.Input
                //.Add("PARAM5_IN", IIf(Trim(C5) = "", strNull, C5)).Direction = ParameterDirection.Input
                //.Add("PARAM6_IN", IIf(Trim(C6) = "", strNull, C6)).Direction = ParameterDirection.Input
                //.Add("PARAM7_IN", IIf(Trim(C7) = "", strNull, C7)).Direction = ParameterDirection.Input
                //.Add("PARAM8_IN", IIf(Trim(C8) = "", strNull, C8)).Direction = ParameterDirection.Input
                //.Add("PARAM9_IN", IIf(Trim(C9) = "", strNull, C9)).Direction = ParameterDirection.Input
                //.Add("PARAM10_IN", IIf(Trim(C10) = "", strNull, C10)).Direction = ParameterDirection.Input
                //.Add("PARAM11_IN", IIf(Trim(C11) = "", strNull, C11)).Direction = ParameterDirection.Input
                //.Add("PARAM12_IN", IIf(Trim(C12) = "", strNull, C12)).Direction = ParameterDirection.Input
                //.Add("PARAM13_IN", IIf(Trim(C13) = "", strNull, C13)).Direction = ParameterDirection.Input
                //.Add("PARAM14_IN", IIf(Trim(C14) = "", strNull, C14)).Direction = ParameterDirection.Input
                //.Add("PARAM15_IN", IIf(Trim(C15) = "", strNull, C15)).Direction = ParameterDirection.Input
                //.Add("PARAM16_IN", IIf(Trim(C16) = "", strNull, C16)).Direction = ParameterDirection.Input
                for (int _counter = 3; _counter <= 30; _counter++)
                {
                    string _paramName = "PARAM" + _counter.ToString() + "_IN";
                    string _paramValue = "";
                    if (arr.Length > _counter)
                    {
                        _paramValue = Convert.ToString(arr.GetValue(_counter));
                        _with16.Add(_paramName, (string.IsNullOrEmpty(_paramValue.Trim()) ? strNull : _paramValue)).Direction = ParameterDirection.Input;
                    }
                }
                _with16.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                OracleClob clob = null;
                clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        #endregion "Common Search Procedure for Receivable Module"

        #region "Common Search Procedure for Payables Module"

        /// <summary>
        /// Gets the common search payables.
        /// </summary>
        /// <param name="strSearchFlag">The string search flag.</param>
        /// <param name="strCondition">The string condition.</param>
        /// <param name="strChk">The string CHK.</param>
        /// <returns></returns>
        public string getCommonSearchPayables(string strSearchFlag, string strCondition, string strChk = "")
        {
            return FetchCommonData_Payables(strCondition, strChk);
        }

        /// <summary>
        /// Fetches the common data_ payables.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <param name="strChk">The string CHK.</param>
        /// <returns></returns>
        public string FetchCommonData_Payables(string strCond, string strChk = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string C = null;
            string C1 = null;
            string C3 = null;
            string C4 = null;
            string C5 = null;
            string C6 = null;
            string C7 = null;
            string C8 = null;
            string C9 = null;
            string C10 = null;
            string C11 = null;
            string C12 = null;
            string C13 = null;
            string C14 = null;
            string C15 = null;
            string C16 = null;
            string User_pk = null;
            string strSearchFlag_IN = null;
            string strFromToType = "";
            var strNull = "";
            string SelectedLoc = null;
            string strLOOKUP_IN = null;
            arr = strCond.Split('~');
            // lOOK UP VALUE E OR L
            strLOOKUP_IN = Convert.ToString(arr.GetValue(0)); ;
            //USER TYPE TEXT WHILE PRESSING THE KEY
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (strSERACH_IN.IndexOf("$") != -1)
            {
                strSERACH_IN = strSERACH_IN.Replace("$", "&");
            }
            strSERACH_IN = strSERACH_IN.TrimEnd(',');
            User_pk = Convert.ToString(HttpContext.Current.Session["USER_PK"]);
            //FLAG FOR ES FOR WHICH FETCHING THE RECORDS
            strSearchFlag_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
            {
                C3 = Convert.ToString(arr.GetValue(3));
            }
            if (arr.Length > 4)
            {
                C4 = Convert.ToString(arr.GetValue(4));
            }
            if (arr.Length > 5)
            {
                C5 = Convert.ToString(arr.GetValue(5));
            }
            if (arr.Length > 6)
            {
                C6 = Convert.ToString(arr.GetValue(6));
            }
            if (arr.Length > 7)
            {
                C7 = Convert.ToString(arr.GetValue(7));
            }
            if (arr.Length > 8)
            {
                C8 = Convert.ToString(arr.GetValue(8));
            }
            if (arr.Length > 9)
            {
                C9 = Convert.ToString(arr.GetValue(9));
            }
            if (arr.Length > 10)
            {
                C10 = Convert.ToString(arr.GetValue(10));
            }
            if (arr.Length > 11)
            {
                C11 = Convert.ToString(arr.GetValue(11));
            }
            if (arr.Length > 12)
            {
                C12 = Convert.ToString(arr.GetValue(12));
            }
            if (arr.Length > 13)
            {
                C13 = Convert.ToString(arr.GetValue(13));
            }
            if (arr.Length > 14)
            {
                C14 = Convert.ToString(arr.GetValue(14));
            }
            if (arr.Length > 15)
            {
                C15 = Convert.ToString(arr.GetValue(15));
            }
            if (arr.Length > 16)
            {
                C16 = Convert.ToString(arr.GetValue(16));
            }

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".GET_EN_COMMON_PAYABLES_PKG.GET_PAYABLES_ES";

                var _with17 = selectCommand.Parameters;
                _with17.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with17.Add("LOOKUP_VALUE_IN", strLOOKUP_IN).Direction = ParameterDirection.Input;
                _with17.Add("SearchFlag", (string.IsNullOrEmpty(strSearchFlag_IN) ? strNull : strSearchFlag_IN)).Direction = ParameterDirection.Input;
                _with17.Add("USER_PK_IN", (string.IsNullOrEmpty(User_pk) ? strNull : User_pk)).Direction = ParameterDirection.Input;
                _with17.Add("PARAM3_IN", (string.IsNullOrEmpty(C13.Trim()) ? strNull : C3)).Direction = ParameterDirection.Input;
                _with17.Add("PARAM4_IN", (string.IsNullOrEmpty(C4.Trim()) ? strNull : C4)).Direction = ParameterDirection.Input;
                _with17.Add("PARAM5_IN", (string.IsNullOrEmpty(C5.Trim()) ? strNull : C5)).Direction = ParameterDirection.Input;
                _with17.Add("PARAM6_IN", (string.IsNullOrEmpty(C6.Trim()) ? strNull : C6)).Direction = ParameterDirection.Input;
                _with17.Add("PARAM7_IN", (string.IsNullOrEmpty(C7.Trim()) ? strNull : C7)).Direction = ParameterDirection.Input;
                _with17.Add("PARAM8_IN", (string.IsNullOrEmpty(C8.Trim()) ? strNull : C8)).Direction = ParameterDirection.Input;
                _with17.Add("PARAM9_IN", (string.IsNullOrEmpty(C9.Trim()) ? strNull : C9)).Direction = ParameterDirection.Input;
                _with17.Add("PARAM10_IN", (string.IsNullOrEmpty(C10.Trim()) ? strNull : C10)).Direction = ParameterDirection.Input;
                _with17.Add("PARAM11_IN", (string.IsNullOrEmpty(C11.Trim()) ? strNull : C11)).Direction = ParameterDirection.Input;
                _with17.Add("PARAM12_IN", (string.IsNullOrEmpty(C12.Trim()) ? strNull : C12)).Direction = ParameterDirection.Input;
                _with17.Add("PARAM13_IN", (string.IsNullOrEmpty(C13.Trim()) ? strNull : C13)).Direction = ParameterDirection.Input;
                _with17.Add("PARAM14_IN", (string.IsNullOrEmpty(C14.Trim()) ? strNull : C14)).Direction = ParameterDirection.Input;
                _with17.Add("PARAM15_IN", (string.IsNullOrEmpty(C15.Trim()) ? strNull : C15)).Direction = ParameterDirection.Input;
                _with17.Add("PARAM16_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C16)).Direction = ParameterDirection.Input;
                _with17.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                OracleClob clob = null;
                clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        #endregion "Common Search Procedure for Payables Module"

        #region "Common Search Procedure for Print Export Docs Module"

        /// <summary>
        /// Gets the common search print exp docs.
        /// </summary>
        /// <param name="strSearchFlag">The string search flag.</param>
        /// <param name="strCondition">The string condition.</param>
        /// <param name="strChk">The string CHK.</param>
        /// <returns></returns>
        public string getCommonSearchPrintExpDocs(string strSearchFlag, string strCondition, string strChk = "")
        {
            return FetchCommonData_PrintExpDocs(strCondition, strChk);
        }

        /// <summary>
        /// Fetches the common data_ print exp docs.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <param name="strChk">The string CHK.</param>
        /// <returns></returns>
        public string FetchCommonData_PrintExpDocs(string strCond, string strChk = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string C = null;
            string C1 = null;
            string C3 = null;
            string C4 = null;
            string C5 = null;
            string C6 = null;
            string C7 = null;
            string C8 = null;
            string C9 = null;
            string C10 = null;
            string C11 = null;
            string C12 = null;
            string C13 = null;
            string C14 = null;
            string C15 = null;
            string C16 = null;
            string User_pk = null;
            string strSearchFlag_IN = null;
            string strFromToType = "";
            var strNull = "";
            string SelectedLoc = null;
            string strLOOKUP_IN = null;
            arr = strCond.Split('~');
            // lOOK UP VALUE E OR L
            strLOOKUP_IN = Convert.ToString(arr.GetValue(0)); ;
            //USER TYPE TEXT WHILE PRESSING THE KEY
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (strSERACH_IN.IndexOf("$") != -1)
            {
                strSERACH_IN = strSERACH_IN.Replace("$", "&");
            }
            strSERACH_IN = strSERACH_IN.TrimEnd(',');
            User_pk = Convert.ToString(HttpContext.Current.Session["USER_PK"]);
            //FLAG FOR ES FOR WHICH FETCHING THE RECORDS
            strSearchFlag_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
            {
                C3 = Convert.ToString(arr.GetValue(3));
            }
            if (arr.Length > 4)
            {
                C4 = Convert.ToString(arr.GetValue(4));
            }
            if (arr.Length > 5)
            {
                C5 = Convert.ToString(arr.GetValue(5));
            }
            if (arr.Length > 6)
            {
                C6 = Convert.ToString(arr.GetValue(6));
            }
            if (arr.Length > 7)
            {
                C7 = Convert.ToString(arr.GetValue(7));
            }
            if (arr.Length > 8)
            {
                C8 = Convert.ToString(arr.GetValue(8));
            }
            if (arr.Length > 9)
            {
                C9 = Convert.ToString(arr.GetValue(9));
            }
            if (arr.Length > 10)
            {
                C10 = Convert.ToString(arr.GetValue(10));
            }
            if (arr.Length > 11)
            {
                C11 = Convert.ToString(arr.GetValue(11));
            }
            if (arr.Length > 12)
            {
                C12 = Convert.ToString(arr.GetValue(12));
            }
            if (arr.Length > 13)
            {
                C13 = Convert.ToString(arr.GetValue(13));
            }
            if (arr.Length > 14)
            {
                C14 = Convert.ToString(arr.GetValue(14));
            }
            if (arr.Length > 15)
            {
                C15 = Convert.ToString(arr.GetValue(15));
            }
            if (arr.Length > 16)
            {
                C16 = Convert.ToString(arr.GetValue(16));
            }

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".GET_EN_COMMON_PAYABLES_PKG.GET_PAYABLES_ES";

                var _with18 = selectCommand.Parameters;
                _with18.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with18.Add("LOOKUP_VALUE_IN", strLOOKUP_IN).Direction = ParameterDirection.Input;
                _with18.Add("SearchFlag", (string.IsNullOrEmpty(strSearchFlag_IN) ? strNull : strSearchFlag_IN)).Direction = ParameterDirection.Input;
                _with18.Add("USER_PK_IN", (string.IsNullOrEmpty(User_pk) ? strNull : User_pk)).Direction = ParameterDirection.Input;
                _with18.Add("PARAM3_IN", (string.IsNullOrEmpty(C13.Trim()) ? strNull : C3)).Direction = ParameterDirection.Input;
                _with18.Add("PARAM4_IN", (string.IsNullOrEmpty(C4.Trim()) ? strNull : C4)).Direction = ParameterDirection.Input;
                _with18.Add("PARAM5_IN", (string.IsNullOrEmpty(C5.Trim()) ? strNull : C5)).Direction = ParameterDirection.Input;
                _with18.Add("PARAM6_IN", (string.IsNullOrEmpty(C6.Trim()) ? strNull : C6)).Direction = ParameterDirection.Input;
                _with18.Add("PARAM7_IN", (string.IsNullOrEmpty(C7.Trim()) ? strNull : C7)).Direction = ParameterDirection.Input;
                _with18.Add("PARAM8_IN", (string.IsNullOrEmpty(C8.Trim()) ? strNull : C8)).Direction = ParameterDirection.Input;
                _with18.Add("PARAM9_IN", (string.IsNullOrEmpty(C9.Trim()) ? strNull : C9)).Direction = ParameterDirection.Input;
                _with18.Add("PARAM10_IN", (string.IsNullOrEmpty(C10.Trim()) ? strNull : C10)).Direction = ParameterDirection.Input;
                _with18.Add("PARAM11_IN", (string.IsNullOrEmpty(C11.Trim()) ? strNull : C11)).Direction = ParameterDirection.Input;
                _with18.Add("PARAM12_IN", (string.IsNullOrEmpty(C12.Trim()) ? strNull : C12)).Direction = ParameterDirection.Input;
                _with18.Add("PARAM13_IN", (string.IsNullOrEmpty(C13.Trim()) ? strNull : C13)).Direction = ParameterDirection.Input;
                _with18.Add("PARAM14_IN", (string.IsNullOrEmpty(C14.Trim()) ? strNull : C14)).Direction = ParameterDirection.Input;
                _with18.Add("PARAM15_IN", (string.IsNullOrEmpty(C15.Trim()) ? strNull : C15)).Direction = ParameterDirection.Input;
                _with18.Add("PARAM16_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C16)).Direction = ParameterDirection.Input;
                _with18.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                OracleClob clob = null;
                clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        #endregion "Common Search Procedure for Print Export Docs Module"

        #region "Common Search Procedure for Print Import Docs Module"

        /// <summary>
        /// Gets the common search print imp docs.
        /// </summary>
        /// <param name="strSearchFlag">The string search flag.</param>
        /// <param name="strCondition">The string condition.</param>
        /// <param name="strChk">The string CHK.</param>
        /// <returns></returns>
        public string getCommonSearchPrintImpDocs(string strSearchFlag, string strCondition, string strChk = "")
        {
            return FetchCommonData_PrintImpDocs(strCondition, strChk);
        }

        /// <summary>
        /// Fetches the common data_ print imp docs.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <param name="strChk">The string CHK.</param>
        /// <returns></returns>
        public string FetchCommonData_PrintImpDocs(string strCond, string strChk = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string C = null;
            string C1 = null;
            string C3 = null;
            string C4 = null;
            string C5 = null;
            string C6 = null;
            string C7 = null;
            string C8 = null;
            string C9 = null;
            string C10 = null;
            string C11 = null;
            string C12 = null;
            string C13 = null;
            string C14 = null;
            string C15 = null;
            string C16 = null;
            string User_pk = null;
            string strSearchFlag_IN = null;
            string strFromToType = "";
            var strNull = "";
            string SelectedLoc = null;
            string strLOOKUP_IN = null;
            arr = strCond.Split('~');
            // lOOK UP VALUE E OR L
            strLOOKUP_IN = Convert.ToString(arr.GetValue(0)); ;
            //USER TYPE TEXT WHILE PRESSING THE KEY
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (strSERACH_IN.IndexOf("$") != -1)
            {
                strSERACH_IN = strSERACH_IN.Replace("$", "&");
            }
            strSERACH_IN = strSERACH_IN.TrimEnd(',');
            User_pk = Convert.ToString(HttpContext.Current.Session["USER_PK"]);
            //FLAG FOR ES FOR WHICH FETCHING THE RECORDS
            strSearchFlag_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
            {
                C3 = Convert.ToString(arr.GetValue(3));
            }
            if (arr.Length > 4)
            {
                C4 = Convert.ToString(arr.GetValue(4));
            }
            if (arr.Length > 5)
            {
                C5 = Convert.ToString(arr.GetValue(5));
            }
            if (arr.Length > 6)
            {
                C6 = Convert.ToString(arr.GetValue(6));
            }
            if (arr.Length > 7)
            {
                C7 = Convert.ToString(arr.GetValue(7));
            }
            if (arr.Length > 8)
            {
                C8 = Convert.ToString(arr.GetValue(8));
            }
            if (arr.Length > 9)
            {
                C9 = Convert.ToString(arr.GetValue(9));
            }
            if (arr.Length > 10)
            {
                C10 = Convert.ToString(arr.GetValue(10));
            }
            if (arr.Length > 11)
            {
                C11 = Convert.ToString(arr.GetValue(11));
            }
            if (arr.Length > 12)
            {
                C12 = Convert.ToString(arr.GetValue(12));
            }
            if (arr.Length > 13)
            {
                C13 = Convert.ToString(arr.GetValue(13));
            }
            if (arr.Length > 14)
            {
                C14 = Convert.ToString(arr.GetValue(14));
            }
            if (arr.Length > 15)
            {
                C15 = Convert.ToString(arr.GetValue(15));
            }
            if (arr.Length > 16)
            {
                C16 = Convert.ToString(arr.GetValue(16));
            }

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".GET_EN_COMMON_PAYABLES_PKG.GET_PAYABLES_ES";

                var _with19 = selectCommand.Parameters;
                _with19.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with19.Add("LOOKUP_VALUE_IN", strLOOKUP_IN).Direction = ParameterDirection.Input;
                _with19.Add("SearchFlag", (string.IsNullOrEmpty(strSearchFlag_IN) ? strNull : strSearchFlag_IN)).Direction = ParameterDirection.Input;
                _with19.Add("USER_PK_IN", (string.IsNullOrEmpty(User_pk) ? strNull : User_pk)).Direction = ParameterDirection.Input;
                _with19.Add("PARAM3_IN", (string.IsNullOrEmpty(C13.Trim()) ? strNull : C3)).Direction = ParameterDirection.Input;
                _with19.Add("PARAM4_IN", (string.IsNullOrEmpty(C4.Trim()) ? strNull : C4)).Direction = ParameterDirection.Input;
                _with19.Add("PARAM5_IN", (string.IsNullOrEmpty(C5.Trim()) ? strNull : C5)).Direction = ParameterDirection.Input;
                _with19.Add("PARAM6_IN", (string.IsNullOrEmpty(C6.Trim()) ? strNull : C6)).Direction = ParameterDirection.Input;
                _with19.Add("PARAM7_IN", (string.IsNullOrEmpty(C7.Trim()) ? strNull : C7)).Direction = ParameterDirection.Input;
                _with19.Add("PARAM8_IN", (string.IsNullOrEmpty(C8.Trim()) ? strNull : C8)).Direction = ParameterDirection.Input;
                _with19.Add("PARAM9_IN", (string.IsNullOrEmpty(C9.Trim()) ? strNull : C9)).Direction = ParameterDirection.Input;
                _with19.Add("PARAM10_IN", (string.IsNullOrEmpty(C10.Trim()) ? strNull : C10)).Direction = ParameterDirection.Input;
                _with19.Add("PARAM11_IN", (string.IsNullOrEmpty(C11.Trim()) ? strNull : C11)).Direction = ParameterDirection.Input;
                _with19.Add("PARAM12_IN", (string.IsNullOrEmpty(C12.Trim()) ? strNull : C12)).Direction = ParameterDirection.Input;
                _with19.Add("PARAM13_IN", (string.IsNullOrEmpty(C13.Trim()) ? strNull : C13)).Direction = ParameterDirection.Input;
                _with19.Add("PARAM14_IN", (string.IsNullOrEmpty(C14.Trim()) ? strNull : C14)).Direction = ParameterDirection.Input;
                _with19.Add("PARAM15_IN", (string.IsNullOrEmpty(C15.Trim()) ? strNull : C15)).Direction = ParameterDirection.Input;
                _with19.Add("PARAM16_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C16)).Direction = ParameterDirection.Input;
                _with19.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                OracleClob clob = null;
                clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        #endregion "Common Search Procedure for Print Import Docs Module"

        #region "Common Search Procedure for Order Mgmt Module"

        /// <summary>
        /// Gets the common search order MGMT.
        /// </summary>
        /// <param name="strSearchFlag">The string search flag.</param>
        /// <param name="strCondition">The string condition.</param>
        /// <param name="strChk">The string CHK.</param>
        /// <returns></returns>
        public string getCommonSearchOrderMgmt(string strSearchFlag, string strCondition, string strChk = "")
        {
            return FetchCommonData_OrderMgmt(strCondition, strChk);
        }

        /// <summary>
        /// Fetches the common data_ order MGMT.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <param name="strChk">The string CHK.</param>
        /// <returns></returns>
        public string FetchCommonData_OrderMgmt(string strCond, string strChk = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string C = null;
            string C1 = null;
            string C3 = null;
            string C4 = null;
            string C5 = null;
            string C6 = null;
            string C7 = null;
            string C8 = null;
            string C9 = null;
            string C10 = null;
            string C11 = null;
            string C12 = null;
            string C13 = null;
            string C14 = null;
            string C15 = null;
            string C16 = null;
            string C17 = null;
            string C18 = null;
            string C19 = null;
            string C20 = null;
            string C21 = null;
            string C22 = null;
            string C23 = null;
            string C24 = null;
            string C25 = null;
            string User_pk = null;
            string strSearchFlag_IN = null;
            string strFromToType = "";
            var strNull = "";
            string SelectedLoc = null;
            string strLOOKUP_IN = null;
            arr = strCond.Split('~');
            // lOOK UP VALUE E OR L
            strLOOKUP_IN = Convert.ToString(arr.GetValue(0)); ;
            //USER TYPE TEXT WHILE PRESSING THE KEY
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (strSERACH_IN.IndexOf("$") != -1)
            {
                strSERACH_IN = strSERACH_IN.Replace("$", "&");
            }
            strSERACH_IN = strSERACH_IN.TrimEnd(',');
            User_pk = Convert.ToString(HttpContext.Current.Session["USER_PK"]);
            //FLAG FOR ES FOR WHICH FETCHING THE RECORDS
            strSearchFlag_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
            {
                C3 = Convert.ToString(arr.GetValue(3));
            }
            if (arr.Length > 4)
            {
                C4 = Convert.ToString(arr.GetValue(4));
            }
            if (arr.Length > 5)
            {
                C5 = Convert.ToString(arr.GetValue(5));
            }
            if (arr.Length > 6)
            {
                C6 = Convert.ToString(arr.GetValue(6));
            }
            if (arr.Length > 7)
            {
                C7 = Convert.ToString(arr.GetValue(7));
            }
            if (arr.Length > 8)
            {
                C8 = Convert.ToString(arr.GetValue(8));
            }
            if (arr.Length > 9)
            {
                C9 = Convert.ToString(arr.GetValue(9));
            }
            if (arr.Length > 10)
            {
                C10 = Convert.ToString(arr.GetValue(10));
            }
            if (arr.Length > 11)
            {
                C11 = Convert.ToString(arr.GetValue(11));
            }
            if (arr.Length > 12)
            {
                C12 = Convert.ToString(arr.GetValue(12));
            }
            if (arr.Length > 13)
            {
                C13 = Convert.ToString(arr.GetValue(13));
            }
            if (arr.Length > 14)
            {
                C14 = Convert.ToString(arr.GetValue(14));
            }
            if (arr.Length > 15)
            {
                C15 = Convert.ToString(arr.GetValue(15));
            }
            if (arr.Length > 16)
            {
                C16 = Convert.ToString(arr.GetValue(16));
            }
            if (arr.Length > 17)
            {
                C17 = Convert.ToString(arr.GetValue(17));
            }
            if (arr.Length > 18)
            {
                C18 = Convert.ToString(arr.GetValue(18));
            }
            if (arr.Length > 19)
            {
                C19 = Convert.ToString(arr.GetValue(19));
            }
            if (arr.Length > 20)
            {
                C20 = Convert.ToString(arr.GetValue(20));
            }
            if (arr.Length > 21)
            {
                C21 = Convert.ToString(arr.GetValue(21));
            }
            if (arr.Length > 22)
            {
                C22 = Convert.ToString(arr.GetValue(22));
            }
            if (arr.Length > 23)
            {
                C23 = Convert.ToString(arr.GetValue(23));
            }
            if (arr.Length > 24)
            {
                C24 = Convert.ToString(arr.GetValue(24));
            }
            if (arr.Length > 25)
            {
                C25 = Convert.ToString(arr.GetValue(25));
            }
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".GET_EN_COMMON_ORDERMGMT_PKG.GET_ORDERMGMT_ES";

                var _with20 = selectCommand.Parameters;
                _with20.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with20.Add("LOOKUP_VALUE_IN", strLOOKUP_IN).Direction = ParameterDirection.Input;
                _with20.Add("SearchFlag", (string.IsNullOrEmpty(strSearchFlag_IN) ? strNull : strSearchFlag_IN)).Direction = ParameterDirection.Input;
                _with20.Add("USER_PK_IN", (string.IsNullOrEmpty(User_pk) ? strNull : User_pk)).Direction = ParameterDirection.Input;
                _with20.Add("PARAM3_IN", (string.IsNullOrEmpty(C13.Trim()) ? strNull : C3)).Direction = ParameterDirection.Input;
                _with20.Add("PARAM4_IN", (string.IsNullOrEmpty(C4.Trim()) ? strNull : C4)).Direction = ParameterDirection.Input;
                _with20.Add("PARAM5_IN", (string.IsNullOrEmpty(C5.Trim()) ? strNull : C5)).Direction = ParameterDirection.Input;
                _with20.Add("PARAM6_IN", (string.IsNullOrEmpty(C6.Trim()) ? strNull : C6)).Direction = ParameterDirection.Input;
                _with20.Add("PARAM7_IN", (string.IsNullOrEmpty(C7.Trim()) ? strNull : C7)).Direction = ParameterDirection.Input;
                _with20.Add("PARAM8_IN", (string.IsNullOrEmpty(C8.Trim()) ? strNull : C8)).Direction = ParameterDirection.Input;
                _with20.Add("PARAM9_IN", (string.IsNullOrEmpty(C9.Trim()) ? strNull : C9)).Direction = ParameterDirection.Input;
                _with20.Add("PARAM10_IN", (string.IsNullOrEmpty(C10.Trim()) ? strNull : C10)).Direction = ParameterDirection.Input;
                _with20.Add("PARAM11_IN", (string.IsNullOrEmpty(C11.Trim()) ? strNull : C11)).Direction = ParameterDirection.Input;
                _with20.Add("PARAM12_IN", (string.IsNullOrEmpty(C12.Trim()) ? strNull : C12)).Direction = ParameterDirection.Input;
                _with20.Add("PARAM13_IN", (string.IsNullOrEmpty(C13.Trim()) ? strNull : C13)).Direction = ParameterDirection.Input;
                _with20.Add("PARAM14_IN", (string.IsNullOrEmpty(C14.Trim()) ? strNull : C14)).Direction = ParameterDirection.Input;
                _with20.Add("PARAM15_IN", (string.IsNullOrEmpty(C15.Trim()) ? strNull : C15)).Direction = ParameterDirection.Input;
                _with20.Add("PARAM16_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C16)).Direction = ParameterDirection.Input;
                _with20.Add("PARAM17_IN", (string.IsNullOrEmpty(C17.Trim()) ? strNull : C17)).Direction = ParameterDirection.Input;
                _with20.Add("PARAM18_IN", (string.IsNullOrEmpty(C18.Trim()) ? strNull : C18)).Direction = ParameterDirection.Input;
                _with20.Add("PARAM19_IN", (string.IsNullOrEmpty(C19.Trim()) ? strNull : C19)).Direction = ParameterDirection.Input;
                _with20.Add("PARAM20_IN", (string.IsNullOrEmpty(C20.Trim()) ? strNull : C20)).Direction = ParameterDirection.Input;
                _with20.Add("PARAM21_IN", (string.IsNullOrEmpty(C21.Trim()) ? strNull : C21)).Direction = ParameterDirection.Input;
                _with20.Add("PARAM22_IN", (string.IsNullOrEmpty(C22.Trim()) ? strNull : C22)).Direction = ParameterDirection.Input;
                _with20.Add("PARAM23_IN", (string.IsNullOrEmpty(C23.Trim()) ? strNull : C23)).Direction = ParameterDirection.Input;
                _with20.Add("PARAM24_IN", (string.IsNullOrEmpty(C24.Trim()) ? strNull : C24)).Direction = ParameterDirection.Input;
                _with20.Add("PARAM25_IN", (string.IsNullOrEmpty(C25.Trim()) ? strNull : C25)).Direction = ParameterDirection.Input;
                _with20.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                OracleClob clob = null;
                clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        #endregion "Common Search Procedure for Order Mgmt Module"

        #region "Common Search Procedure for Reports Module"

        /// <summary>
        /// Gets the common search reports.
        /// </summary>
        /// <param name="strSearchFlag">The string search flag.</param>
        /// <param name="strCondition">The string condition.</param>
        /// <param name="strChk">The string CHK.</param>
        /// <returns></returns>
        public string getCommonSearchReports(string strSearchFlag, string strCondition, string strChk = "")
        {
            return FetchCommonData_Reports(strCondition, strChk);
        }

        /// <summary>
        /// Fetches the common data_ reports.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <param name="strChk">The string CHK.</param>
        /// <returns></returns>
        public string FetchCommonData_Reports(string strCond, string strChk = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string C = null;
            string C1 = null;
            string C3 = null;
            string C4 = null;
            string C5 = null;
            string C6 = null;
            string C7 = null;
            string C8 = null;
            string C9 = null;
            string C10 = null;
            string C11 = null;
            string C12 = null;
            string C13 = null;
            string C14 = null;
            string C15 = null;
            string C16 = null;
            string C17 = null;
            string User_pk = null;
            string strSearchFlag_IN = null;
            string strFromToType = "";
            var strNull = "";
            string SelectedLoc = null;
            string strLOOKUP_IN = null;
            arr = strCond.Split('~');
            // lOOK UP VALUE E OR L
            strLOOKUP_IN = Convert.ToString(arr.GetValue(0)); ;
            //USER TYPE TEXT WHILE PRESSING THE KEY
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (strSERACH_IN.IndexOf("$") != -1)
            {
                strSERACH_IN = strSERACH_IN.Replace("$", "&");
            }
            strSERACH_IN = strSERACH_IN.TrimEnd(',');
            User_pk = Convert.ToString(HttpContext.Current.Session["USER_PK"]);
            //FLAG FOR ES FOR WHICH FETCHING THE RECORDS
            strSearchFlag_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
            {
                C3 = Convert.ToString(arr.GetValue(3));
            }
            if (arr.Length > 4)
            {
                C4 = Convert.ToString(arr.GetValue(4));
            }
            if (arr.Length > 5)
            {
                C5 = Convert.ToString(arr.GetValue(5));
            }
            if (arr.Length > 6)
            {
                C6 = Convert.ToString(arr.GetValue(6));
            }
            if (arr.Length > 7)
            {
                C7 = Convert.ToString(arr.GetValue(7));
            }
            if (arr.Length > 8)
            {
                C8 = Convert.ToString(arr.GetValue(8));
            }
            if (arr.Length > 9)
            {
                C9 = Convert.ToString(arr.GetValue(9));
            }
            if (arr.Length > 10)
            {
                C10 = Convert.ToString(arr.GetValue(10));
            }
            if (arr.Length > 11)
            {
                C11 = Convert.ToString(arr.GetValue(11));
            }
            if (arr.Length > 12)
            {
                C12 = Convert.ToString(arr.GetValue(12));
            }
            if (arr.Length > 13)
            {
                C13 = Convert.ToString(arr.GetValue(13));
            }
            if (arr.Length > 14)
            {
                C14 = Convert.ToString(arr.GetValue(14));
            }
            if (arr.Length > 15)
            {
                C15 = Convert.ToString(arr.GetValue(15));
            }
            if (arr.Length > 16)
            {
                C16 = Convert.ToString(arr.GetValue(16));
            }
            if (arr.Length > 17)
            {
                C17 = Convert.ToString(arr.GetValue(17));
            }
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".GET_EN_COMMON_REPORTS_PKG.GET_REPORTS_ES";

                var _with21 = selectCommand.Parameters;
                _with21.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with21.Add("LOOKUP_VALUE_IN", strLOOKUP_IN).Direction = ParameterDirection.Input;
                _with21.Add("SearchFlag", (string.IsNullOrEmpty(strSearchFlag_IN) ? strNull : strSearchFlag_IN)).Direction = ParameterDirection.Input;
                _with21.Add("USER_PK_IN", (string.IsNullOrEmpty(User_pk) ? strNull : User_pk)).Direction = ParameterDirection.Input;
                _with21.Add("PARAM3_IN", (string.IsNullOrEmpty(C13.Trim()) ? strNull : C3)).Direction = ParameterDirection.Input;
                _with21.Add("PARAM4_IN", (string.IsNullOrEmpty(C4.Trim()) ? strNull : C4)).Direction = ParameterDirection.Input;
                _with21.Add("PARAM5_IN", (string.IsNullOrEmpty(C5.Trim()) ? strNull : C5)).Direction = ParameterDirection.Input;
                _with21.Add("PARAM6_IN", (string.IsNullOrEmpty(C6.Trim()) ? strNull : C6)).Direction = ParameterDirection.Input;
                _with21.Add("PARAM7_IN", (string.IsNullOrEmpty(C7.Trim()) ? strNull : C7)).Direction = ParameterDirection.Input;
                _with21.Add("PARAM8_IN", (string.IsNullOrEmpty(C8.Trim()) ? strNull : C8)).Direction = ParameterDirection.Input;
                _with21.Add("PARAM9_IN", (string.IsNullOrEmpty(C9.Trim()) ? strNull : C9)).Direction = ParameterDirection.Input;
                _with21.Add("PARAM10_IN", (string.IsNullOrEmpty(C10.Trim()) ? strNull : C10)).Direction = ParameterDirection.Input;
                _with21.Add("PARAM11_IN", (string.IsNullOrEmpty(C11.Trim()) ? strNull : C11)).Direction = ParameterDirection.Input;
                _with21.Add("PARAM12_IN", (string.IsNullOrEmpty(C12.Trim()) ? strNull : C12)).Direction = ParameterDirection.Input;
                _with21.Add("PARAM13_IN", (string.IsNullOrEmpty(C13.Trim()) ? strNull : C13)).Direction = ParameterDirection.Input;
                _with21.Add("PARAM14_IN", (string.IsNullOrEmpty(C14.Trim()) ? strNull : C14)).Direction = ParameterDirection.Input;
                _with21.Add("PARAM15_IN", (string.IsNullOrEmpty(C15.Trim()) ? strNull : C15)).Direction = ParameterDirection.Input;
                _with21.Add("PARAM16_IN", (string.IsNullOrEmpty(C16.Trim()) ? strNull : C16)).Direction = ParameterDirection.Input;
                _with21.Add("PARAM17_IN", (string.IsNullOrEmpty(C17.Trim()) ? strNull : C17)).Direction = ParameterDirection.Input;
                _with21.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                OracleClob clob = null;
                clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        #endregion "Common Search Procedure for Reports Module"
    }
}