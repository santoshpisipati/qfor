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
using System;
using System.Data;
using System.Text;

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class CommonSearch : CommonFeatures
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

        /// <summary>
        /// Gets the CLS_ SRR air contract.
        /// </summary>
        /// <value>
        /// The CLS_ SRR air contract.
        /// </value>
        public object Cls_SRRAirContract { get; private set; }

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
                //case "COMMONSES":
                //    return getCommonSearchProcedure(strSearchFlag, strCondition);

                //case "COMMONSESNEW":
                //    return getCommonNewSearchProcedure(strSearchFlag, strCondition);

                //case "COMMONSESNEW_EXD":
                //    //new package created for export doc en search
                //    return getCommonNewSearchProcedure(strSearchFlag, strCondition, "", true);

                //case "COMMONSESNEWDATA":
                //    return getCommonNewDataSearchProcedure(strSearchFlag, strCondition);

                case "Common":
                    return getCommonSearch(strSearchFlag, strCondition);

                //case "BOOKING":
                //    return getBookingSearch(strSearchFlag, strCondition);

                //case "General":
                //    return FetchGenCommonSearch(strSearchFlag, strCondition);

                //case "CUSTOMSBROKERAGE":
                //    return getCOMMONCustomsBrokerage(strSearchFlag, strCondition);

                //case "RFQ":
                //    return getRFQSearch(strSearchFlag, strCondition);

                //case "SRR":
                //    return getRatingAndTariff(strSearchFlag, strCondition);

                //case "DOCUMENTATION":
                //    return getDocumentationSearch(strSearchFlag, strCondition);

                //case "COMMONSESEXPCTRTS":
                //    return getCommonExpiredSearchProcedure(strSearchFlag, strCondition);

                //case "MULTIPLEES":
                //    return getMultipleES(strSearchFlag, strCondition);

                //case "COMMONENHANCESEARCH":
                //    return FetchEnCommonSearch(strCondition);

                //case "MIS":
                //    return getMISSearch(strSearchFlag, strCondition);

                //case "COMMONSESDOCUMENT":
                //    return getCommon_DOCUMENTProcedure(strSearchFlag, strCondition);

                case "COMMONSESADMIN":
                    return getCommonSearchProcedureAdmin(strSearchFlag, strCondition);

                //case "COMMONSESRATINGNTARIFF":
                //    return getCommonSearchRating(strSearchFlag, strCondition);

                //case "COMMONSESCBJCREPORTS":
                //    return getCommonSearchCBJCReports(strSearchFlag, strCondition);

                //case "COMMONSESEDIREPORTS":
                //    return getCommonSearchEDIReports(strSearchFlag, strCondition);

                //case "COMMONSESMIS":
                //    return getCommonSearchMIS(strSearchFlag, strCondition);

                //case "COMMONSESSUPPLIERMGMT":
                //    return getCommonSearchSUPPLIERMGMT(strSearchFlag, strCondition);

                //case "COMMONSESSRECEIVABLES":
                //    return getCommonSearchReceivables(strSearchFlag, strCondition);

                //case "COMMONSESSPAYABLES":
                //    return getCommonSearchPayables(strSearchFlag, strCondition);

                //case "COMMONSESSPRINTEXPDOCS":
                //    return getCommonSearchPrintExpDocs(strSearchFlag, strCondition);

                //case "COMMONSESSPRINTIMPDOCS":
                //    return getCommonSearchPrintImpDocs(strSearchFlag, strCondition);

                //case "COMMONSESORDERMGMT":
                //    return getCommonSearchOrderMgmt(strSearchFlag, strCondition);

                //case "COMMONSESREPORT":
                //    return getCommonSearchReports(strSearchFlag, strCondition);

                default:
                    return null;
            }
        }

        /// <summary>
        /// Fills the grid.
        /// </summary>
        /// <param name="sSQL">The s SQL.</param>
        /// <returns></returns>
        public string FillGrid(string sSQL)
        {
            DataSet objDS = new DataSet();
            string strSqlSearch = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                sSQL = sSQL.Replace(", '*S' AS \"HIDDEN\"", "");
                sSQL = sSQL.Replace(", '*E' AS \"HIDDEN\"", "");
                string ssqlhidden = null;
                Int32 j = default(Int32);
                ssqlhidden = sSQL;
                j = 1;
                while (ssqlhidden.Length > 0)
                {
                    if (string.Compare(ssqlhidden.ToUpper(), "\"HIDDEN\"") > 0)
                    {
                        ssqlhidden = ssqlhidden.ToUpper().Replace("\"HIDDEN\"", "\"HIDDEN" + j + "\"");
                    }
                    else
                    {
                        break; // TODO: might not be correct. Was : Exit While
                    }
                    j = j + 1;
                }

                sSQL = ssqlhidden;
                strSqlSearch = "SELECT * FROM (SELECT QRY.*,ROWNUM AS HIDDENX  FROM ( " + sSQL + ") qry";
                strSqlSearch = strSqlSearch.ToUpper();
                strSqlSearch = strSqlSearch + " ) where HIDDENX Between " + 1 + " and " + 15;

                objDS = objWF.GetDataSet(strSqlSearch);
                return JsonConvert.SerializeObject(objDS, Formatting.Indented);
            }
            catch (Exception ex)
            {
            }
            return string.Empty;
        }

        #endregion "Module level Search"

        //#region " MIS"

        //public string getMISSearch(string strSearchFlag, string strCondition)
        //{
        //    switch (strSearchFlag)
        //    {
        //        case "REFNO_TRACKNTRACE":
        //            //Reference No Selection For Track N Trace Screen
        //            object objClass = new clsTrackNTrace();
        //            return FetchRefNoForTrackNTrace(objClass, strCondition);

        //        case "VESSEL_VOYAGE":
        //            objClass = new clsFrieghtOutstanding();
        //            return Fetchvesselvoyagebypolandpod(objClass, strCondition);

        //        default:
        //            return null;
        //    }
        //}

        //#endregion " MIS"

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
                //case "VESSELID_NAME":
                //    object clvl = new clsVessel_Voyage_Listing();
                //    return FetchVesselIDName(clvl, strCondition);

                //case "TRANSPORTER":
                //    object ctc = new Cls_Transporter_Contract();
                //    return FetchTransporter(ctc, strCondition);

                //case "CUSTOMER_GROUP":
                //    object ccmt = new clsCustomer_Mst_Tbl();
                //    return FetchGroupCustomer(ccmt, strCondition);

                case "LOCATION_BY_NAME_FOR_COUNTRY":
                    object crd = new clsRegionDetails();
                    return FetchLocationByNameForCountry(crd, strCondition);

                //case "LOCATION_BY_NAME":
                //    object crdl = new clsRegionDetails();
                //    return FetchLocationByName(crdl, strCondition);

                //case "CUSTOMER_CONSIGNEE":
                //    object cjcs = new clsJobCardSearch();
                //    return FetchConsigneeLookUp(cjcs, strCondition);

                //case "TRANSPORTER_ZONE":
                //    object ctn = new Cls_Transport_Note();
                //    return FetchTransporterZone(ctn, strCondition);

                //case "MBL_REF_NO_IMP_LIST":
                //    object cqe = new clsQuickEntry();
                //    return FetchForMblRefInImplist(cqe, strCondition);

                //case "ReAssignVOYAGE":
                //    object cbr = new cls_BookingRollOver();
                //    return FetchVoyageReAssignRollOver(cbr, strCondition);

                //case "COST_ELEMENT_ID":
                //    object csi = new Cls_Supplier_Invoice();
                //    return FetchCostElementID(csi, strCondition);

                //case "VOYAGE_BookingRollOver":
                //    object cbro = new cls_BookingRollOver();
                //    return FetchVoyageForBookingRollOver(cbro, strCondition);

                //case "VslVoyRollOverListing":
                //    object cbrol = new cls_BookingRollOver();
                //    return FetchVslVoyRollListing(cbrol, strCondition);

                //case "EMPLOYEE":
                //    object cemt = new cls_Employee_Mst_Table();
                //    return FetchEmp(cemt, strCondition);

                //case "EXECUTIVE":
                //    object csp = new clsSALESPLAN();
                //    return FetchExecutive_All(csp, strCondition);

                //case "CUSTBASEDEMPLOYEE":
                //    cls_Employee_Mst_Table cemtbl = new cls_Employee_Mst_Table();
                //    return FetchCustBasedEmp(cemtbl, strCondition);

                //case "EMPLOYEEID":
                //    cls_Employee_Mst_Table cemptbl = new cls_Employee_Mst_Table();
                //    return FetchEmpID(cemptbl, strCondition);

                //case "LOCATION":
                //    clsRegionDetails crde = new clsRegionDetails();
                //    return FetchLocation(crde, strCondition);

                //case "USER":
                //    clsUser_Mst_Tbl cumt = new clsUser_Mst_Tbl();
                //    return FetchUser(cumt, strCondition);

                //case "CREDITCUSTOMER":
                //    clsCustomer_Mst_Tbl cmtbl = new clsCustomer_Mst_Tbl();
                //    return FetchCreditCustomer(cmtbl, strCondition);

                case "POL":
                    object cpmt = new clsPORT_MST_TBL();
                    return FetchPOL(cpmt, strCondition);

                //case "SLPOL":
                //    clsPORT_MST_TBL cpmtb = new clsPORT_MST_TBL();
                //    return FetchPOL(cpmtb, strCondition);

                //case "POLIMP":
                //    clsPORT_MST_TBL cpmtbl = new clsPORT_MST_TBL();
                //    return FetchPOLIMP(cpmtbl, strCondition);

                case "POD":
                    object cpmtblp = new clsPORT_MST_TBL();
                    return FetchPOD(cpmtblp, strCondition);

                //case "PODTARIFF":
                //    clsPORT_MST_TBL cpmtbltaf = new clsPORT_MST_TBL();
                //    return FetchPODTARIFF(cpmtbltaf, strCondition);

                //case "SLPOD":
                //    clsPORT_MST_TBL cpmtblpod = new clsPORT_MST_TBL();
                //    return FetchPOD(cpmtblpod, strCondition);

                //case "PODIMP":
                //    clsPORT_MST_TBL cpmtblimp = new clsPORT_MST_TBL();
                //    return FetchPODIMP(cpmtblimp, strCondition);

                //case "POD_NOTWORKPORT":
                //    clsPORT_MST_TBL cpmtblpor = new clsPORT_MST_TBL();
                //    return FetchPOD_NOTWORKPORT(cpmtblpor, strCondition);

                case "OPERATOR":
                    object cod = new Cls_Operator_Definition();
                    return FetchOperator(cod, strCondition);

                //case "VESSEL_VOYAGE_OPR":
                //    clsVESSEL_VOYAGELISTING cvvl = new clsVESSEL_VOYAGELISTING();
                //    return FetchVesselVoyageOpr(cvvl, strCondition);

                //case "LOCATION_BASED_CUSTOMER":
                //    clsCustomer_Mst_Tbl ccmtb = new clsCustomer_Mst_Tbl();
                //    return FetchLocationBasedCust(ccmtb, strCondition);

                //case "MBL_REF_NO_LIST":
                //    cls_MBL_Listings ccml = new cls_MBL_Listings();
                //    return FetchForMblRefInMbllist(ccml, strCondition);

                //case "MBL_REF_NO_LIST_IMP":
                //    cls_MBL_Listings ccmli = new cls_MBL_Listings();
                //    return FetchForMblRefInMblimplist(ccmli, strCondition);

                //case "MAWB_NR":
                //    clsAirwayBill cab = new clsAirwayBill();
                //    return FetchMawbNr(cab, strCondition);

                //case "AGENT_INVOICE":
                //    cls_Agent_Details cad = new cls_Agent_Details();
                //    return FetchAgentInvoice(cad, strCondition);

                //case "JOB_REF_NO":
                //    clsJobCardSearch cjcse = new clsJobCardSearch();
                //    return FetchForJobRef(cjcse, strCondition);

                //case "JOBREFNO":
                //    Cls_Arrival_Notice can = new Cls_Arrival_Notice();
                //    return FetchImpCertificateJob(can, strCondition);

                //case "CURRENCY":
                //    cls_THC_PHC ctpc = new cls_THC_PHC();
                //    return FetchCurrency(ctpc, strCondition);

                //case "TEMP_CUSTOMER":
                //    Cls_CustomerReconciliation ccr = new Cls_CustomerReconciliation();
                //    return FetchTempCustomer(ccr, strCondition);

                //case "CUSTOMER":
                //    clsCustomer_Mst_Tbl ccusmtb = new clsCustomer_Mst_Tbl();
                //    return FetchCustomer(ccusmtb, strCondition);

                //case "CONTAINERTYPE":
                //    clsCustomer_Mst_Tbl ccusmstb = new clsCustomer_Mst_Tbl();
                //    return FetchContainerType(ccusmstb, strCondition);

                //case "OPERATOR_FOR_ENQUIRY":
                //    Cls_Operator_Definition code = new Cls_Operator_Definition();
                //    return Fetch_Operator_For_Enquiry(code, strCondition);

                //case "POLENQUIRY":
                //    clsPORT_MST_TBL cpmtbllenq = new clsPORT_MST_TBL();
                //    return FetchPOLEnquiry(cpmtbllenq, strCondition);

                //case "PODENQUIRY":
                //    clsPORT_MST_TBL cpmtbldenq = new clsPORT_MST_TBL();
                //    return FetchPODEnquiry(cpmtbldenq, strCondition);

                //case "TRADE":
                //    clsTRADE_MST_TBL cpmtblt = new clsTRADE_MST_TBL();
                //    return FetchTrade(cpmtblt, strCondition);

                //case "AIRLINE":
                //    Cls_Airline_Definition cade = new Cls_Airline_Definition();
                //    return FetchAirline(cade, strCondition);

                //case "AIRLINE_FOR_ENQUIRY":
                //    Cls_Airline_Definition objClass = new Cls_Airline_Definition();
                //    return Fetch_Airline_For_Enquiry(objClass, strCondition);
                //case "RFQ_FOR_AIRLINE_CUSTOMER":
                //    Cls_AirlineRFQSpotRate_Listing objClass = new Cls_AirlineRFQSpotRate_Listing();
                //    return Fetch_RFQ_For_Airline_Customer(objClass, strCondition);
                //case "SRR_FOR_AIRLINE_CUSTOMER":
                //    clsSRRCustToAirListing objClass = new clsSRRCustToAirListing();
                //    return Fetch_SRR_For_Airline_Customer(objClass, strCondition);
                //case "AIRLINE_FOR_CUSTOMER":
                //    Cls_AirlineRFQSpotRate_Listing objClass = new Cls_AirlineRFQSpotRate_Listing();
                //    return Fetch_Airline_For_Customer(objClass, strCondition);
                //case "CUSTOMER_CATEGORY":
                //    clsJobCardSearch objClass = new clsJobCardSearch();
                //    return FetchForShipperAndConsignee(objClass, strCondition);
                //case "POLPOD_BASED_CUSTOMER":
                //    clsCustomer_Mst_Tbl objClass = new clsCustomer_Mst_Tbl();
                //    return FetchPOLPODBasedCust(objClass, strCondition);
                //case "LPAGENT":
                //    cls_Agent_Details objClass = new cls_Agent_Details();
                //    return FetchLpAgent(objClass, strCondition);
                //case "DPAGENT":
                //    cls_Agent_Details objClass = new cls_Agent_Details();
                //    return FetchDpAgent(objClass, strCondition);
                //case "AGENT":
                //    cls_Agent_Details objClass = new cls_Agent_Details();
                //    return FetchAgent(objClass, strCondition);
                //case "CHA_AGENT":
                //    clsVendorDetails objClass = new clsVendorDetails();
                //    return FetchAllCHAAgent(objClass, strCondition);
                //case "VENDOR":
                //    clsVendorListing objClass = new clsVendorListing();
                //    return FetchVendor(objClass, strCondition);
                //case "SUPPLIER_SECSERV":
                //    //'Added for JC Secondary Services Supplier Popup
                //    clsVendorListing objClass = new clsVendorListing();
                //    return FetchSupplierSecondaryServ(objClass, strCondition);
                //case "COUNTRY":
                //    clsCountry_Mst_Tbl objClass = new clsCountry_Mst_Tbl();
                //    return FetchCountry(objClass, strCondition);
                //case "AIRLINEPREFIX":
                //    Cls_Airline_Definition objClass = new Cls_Airline_Definition();
                //    return FetchAirlineWithPrefix(objClass, strCondition);
                //case "CONTRACT":
                //    clsDetentionTariff objClass = new clsDetentionTariff();
                //    return FetchContract(objClass, strCondition);
                //case "TARIFF_FOR_OPERATOR":
                //    Cls_OperatorTarif_Listing objClass = new Cls_OperatorTarif_Listing();
                //    return FetchTariffForOperator(objClass, strCondition);
                //case "OPERATOR_FOR_CUSTOMER":
                //    Cls_OperatorRFQSpotRate_Listing objClass = new Cls_OperatorRFQSpotRate_Listing();
                //    return Fetch_Operator_For_Customer(objClass, strCondition);
                //case "PORTCOUNTRYTRADE":
                //    cls_THC_PHC objClass = new cls_THC_PHC();
                //    return FetchPortCountryTRADE(objClass, strCondition);
                //case "TRADECOUNTRY":
                //    cls_THC_PHC objClass = new cls_THC_PHC();
                //    return FetchTradeCountry(objClass, strCondition);
                //case "TARIFF_FOR_AIRLINE":
                //    Cls_AirlineTarif_Listing objClass = new Cls_AirlineTarif_Listing();
                //    return FetchTariffForAirline(objClass, strCondition);
                //case "PercentageCommCheck":
                //    return FecthFreightElement(strCondition);
                //case "TCPORT":
                //    clsPORT_MST_TBL objClass = new clsPORT_MST_TBL();
                //    return FetchTCPORT(objClass, strCondition);
                //case "COMMODITY_FOR_GROUP":
                //    clsCOMMODITY_MST_TBL objClass = new clsCOMMODITY_MST_TBL();
                //    return Fetch_Commodity_For_Group(objClass, strCondition);
                //case "MASTER_JC_SEA":
                //    clsJobCard objClass = new clsJobCard();
                //    return FetchMasterJobCardSea(objClass, strCondition);
                //case "MASTER_JC_AIR":
                //    clsJobCard objClass = new clsJobCard();
                //    return FetchMasterJobCardAir(objClass, strCondition);
                //case "POL_WORKPORT":
                //    clsPort_Mst_Tbl objClass = new clsPort_Mst_Tbl();
                //    return FetchPOL_WORKINGPORTS(objClass, strCondition);
                //case "JOB_REF_PAYREQ":
                //    clsJobCardSearch objClass = new clsJobCardSearch();
                //    return FETCH_JOB_REF_PAYREQ(objClass, strCondition);
                //case "VENDOREXP_NAME":
                //    Cls_Payment_Requisition objClass = new Cls_Payment_Requisition();
                //    return FetchExpVendorName(objClass, strCondition);
                //case "FMCVESSELEXP":
                //    Cls_FMCBL objClass = new Cls_FMCBL();
                //    return FetchForFMCVESSEL(objClass, strCondition);
                //case "FMCHBLEXP":
                //    Cls_FMCBL objClass = new Cls_FMCBL();
                //    return FetchForHblRefFMC(objClass, strCondition);
                //case "MAWB_JOB_REF_NO":
                //    clsMAWBListing objClass = new clsMAWBListing();
                //    return FetchForMAWBJobRef(objClass, strCondition);
                //case "JOB_REF_NO_IMP_DOLIST":
                //    cls_PrintManager objClass = new cls_PrintManager();
                //    return FetchJobRefNo_Imp_For_DOList(objClass, strCondition);
                //case "JOB_REF_NO_IMP_DO":
                //    cls_PrintManager objClass = new cls_PrintManager();
                //    return FetchJobRefNo_Imp_For_DO(objClass, strCondition);
                //case "JOB_REF_NO_IMP_PM":
                //    cls_PrintManager objClass = new cls_PrintManager();
                //    return FetchJobRefNo_Imp_For_PrintManager(objClass, strCondition);
                //case "AIRLINE_FLIGHTNAME":
                //    Cls_Airline_Delivery_Note objClass = new Cls_Airline_Delivery_Note();
                //    return FetchAIRLINEFLIGHT(objClass, strCondition);
                //case "Depo_WISE":
                //    cls_ContainerOnHireListing objClass = new cls_ContainerOnHireListing();
                //    return FetchDepo_Wise(objClass, strCondition);
                //case "MBL_REF_NO_JOBCARD_IMP":
                //    cls_MBL_List objClass = new cls_MBL_List();
                //    return FetchMBLForJobCardImp(objClass, strCondition);
                //case "MBL_REF_NO_JOBCARD_INVOICE":
                //    cls_MBL_List objClass = new cls_MBL_List();
                //    return FetchMBLForJobCardImpInvoice(objClass, strCondition);
                //case "MBL_REF_NO_LIST_NEW":
                //    cls_MBL_Listings objClass = new cls_MBL_Listings();
                //    return FetchForMblRefImp(objClass, strCondition);
                //case "MBL_REF_NO_JOBCARD":
                //    cls_MBL_List objClass = new cls_MBL_List();
                //    return FetchMBLForJobCard(objClass, strCondition);
                //case "VESSEL_VOYAGE_INV_CB":
                //    clsVESSEL_VOYAGELISTING objClass = new clsVESSEL_VOYAGELISTING();
                //    return FetchVesselVoyageForInvCB(objClass, strCondition);
                //case "MASTERFORMS":
                //    clsUtil_ExportWizard objClass = new clsUtil_ExportWizard();
                //    return FetchMastersForm(objClass, strCondition);
                //case "VESSEL_VOYAGE_IMP_CB":
                //    clsVESSEL_VOYAGELISTING objClass = new clsVESSEL_VOYAGELISTING();
                //    return FetchVesselVoyageImpCP(objClass, strCondition);
                //case "INV_FLEIGHT":
                //    clsInvoiceListSea objClass = new clsInvoiceListSea();
                //    return FetchFleightForInvDP(objClass, strCondition);
                //case "VOYAGE_JOBCARD":
                //    clsInvoiceListSea objClass = new clsInvoiceListSea();
                //    return FetchVoyageForJobCard(objClass, strCondition);
                //case "VOYAGE_JOBCARD_IMP_NEW":
                //    clsInvoiceListSeaImp objClass = new clsInvoiceListSeaImp();
                //    return FetchVoyageForJobCardNew(objClass, strCondition);
                //case "VOYAGE_JOBCARD_IMP":
                //    clsInvoiceListSeaImp objClass = new clsInvoiceListSeaImp();
                //    return FetchVoyageForJobCard(objClass, strCondition);
                //case "AGENTCOMMON":
                //    cls_Agent_Details objClass = new cls_Agent_Details();
                //    return FetchAllAgents(objClass, strCondition);
                //case "EXPORTAR":
                //    clsUtil_ExportWizard_Transaction objClass = new clsUtil_ExportWizard_Transaction();
                //    return FetchExportAR(objClass, strCondition);
                //case "EXPORTAP":
                //    clsUtil_ExportTransaction_AP objClass = new clsUtil_ExportTransaction_AP();
                //    return FetchExportAP(objClass, strCondition);
                //case "VESSEL_VOYAGE":
                //    clsVESSEL_VOYAGELISTING objClass = new clsVESSEL_VOYAGELISTING();
                //    return FetchVesselVoyage(objClass, strCondition);
                //case "HBL_REF_NO_JOBCARDS":
                //    cls_HBL_List objClass = new cls_HBL_List();
                //    return FetchHBLForJobCardS(objClass, strCondition);
                //case "GET_MBL_REF_JOBCARD_EXP":
                //    cls_MBL_List objClass = new cls_MBL_List();
                //    return FetchMBLForJobCardExp(objClass, strCondition);
                //case "HBL_REF_NO":
                //    cls_HBL_Entry objClass = new cls_HBL_Entry();
                //    return FetchForHblRef(objClass, strCondition);
                //case "HBL_REF_NO_MAWB":
                //    cls_HBL_Entry objClass = new cls_HBL_Entry();
                //    return FetchForHblRefMAWB(objClass, strCondition);
                //case "NEW CURRENCY":
                //    cls_THC_PHC objClass = new cls_THC_PHC();
                //    return FetchNewCurrency(objClass, strCondition);
                //case "RFQ_FOR_OPERATOR_CUSTOMER":
                //    // Rajesh - for Operator RFQ Spot Rate Listing (06-Dec-2005)
                //    Cls_OperatorRFQSpotRate_Listing objClass = new Cls_OperatorRFQSpotRate_Listing();
                //    return Fetch_RFQ_For_Operator_Customer(objClass, strCondition);
                //case "JOB_VENDOR":
                //    clsVendorListing objClass = new clsVendorListing();
                //    return FetchJobVendors(objClass, strCondition);
                //case "VESSEL_FLIGHT":
                //    clsVESSEL_VOYAGELISTING objClass = new clsVESSEL_VOYAGELISTING();
                //    return FetchVslFlight(objClass, strCondition);
                //case "MBL_REF_NO_LIST_SUPPINV":
                //    cls_MBL_Listings objClass = new cls_MBL_Listings();
                //    return FetchForMblRefInSuppInv(objClass, strCondition);
                //case "OPERATOR_AIRLINE":
                //    Cls_Operator_Definition objClass = new Cls_Operator_Definition();
                //    return FetchOperatorAirLine(objClass, strCondition);
                //case "OPERATOR_WF":
                //    Cls_Operator_Definition objClass = new Cls_Operator_Definition();
                //    return FetchOperator_WF(objClass, strCondition);
                //case "HBL_REF_NO_IMP_LIST":
                //    clsQuickEntry objClass = new clsQuickEntry();
                //    return FetchForHblRefInImplist(objClass, strCondition);
                //case "SHP_REF_NO_IMP_LIST":
                //    clsQuickEntry objClass = new clsQuickEntry();
                //    return FetchForSHPRefInImplist(objClass, strCondition);
                //case "CNS_REF_NO_IMP_LIST":
                //    clsQuickEntry objClass = new clsQuickEntry();
                //    return FetchForCNSRefInImplist(objClass, strCondition);
                //case "AGENTCOMM":
                //    cls_Agent_Details objClass = new cls_Agent_Details();
                //    return FetchAgentComm(objClass, strCondition);
                //case "AIRLINE_FOR_EXP_MANIFEST":
                //    Cls_Airline_Definition objClass = new Cls_Airline_Definition();
                //    return Fetch_Airline_For_Enquiry_EXP_Manifest(objClass, strCondition);
                //case "VESSELSRPT":
                //    clsVESSEL_VOYAGELISTING objClass = new clsVESSEL_VOYAGELISTING();
                //    return FetchVesselsRpt(objClass, strCondition);
                //case "VOYAGE":
                //    clsVESSEL_VOYAGELISTING objClass = new clsVESSEL_VOYAGELISTING();
                //    return FetchVoyage(objClass, strCondition);
                //case "JOB_REF_FOR_MBL":
                //    cls_MBL_Entry objClass = new cls_MBL_Entry();
                //    return FetchJobInMblEntry(objClass, strCondition);
                //case "JOB_REF_FOR_BBMBL":
                //    cls_BBMBL_Entry objClass = new cls_BBMBL_Entry();
                //    return FetchJobInBBMblEntry(objClass, strCondition);
                //case "HBL_REF_FOR_MBL":
                //    cls_MBL_Entry objClass = new cls_MBL_Entry();
                //    return FetchForHblRefInMbl(objClass, strCondition);
                //case "MJC_REF_FOR_MBL":
                //    cls_MBL_Entry objClass = new cls_MBL_Entry();
                //    return FetchMJCInMblEntry(objClass, strCondition);
                //case "CUSTOMER_TEMP":
                //    clsCustomer_Mst_Tbl objClass = new clsCustomer_Mst_Tbl();
                //    return FetchCustomer_Temp(objClass, strCondition);
                //case "JOBCARD_CUSTOMS":
                //    cls_CustomsBrokerage objClass = new cls_CustomsBrokerage();
                //    return FetchJobcard(objClass, strCondition);
                //case "TRANSPORT_JC":
                //    cls_TransporterNote objClass = new cls_TransporterNote();
                //    return FetchAllJobcard(objClass, strCondition);
                //case "WORKFLOW_MANAGER":
                //    Cls_Workflow_Mgr_Report objclass = new Cls_Workflow_Mgr_Report();
                //    return InsertionToUserTaskListTable(objclass, strCondition);

                //case "NEWVENDOR":
                //    Cls_StatementOfAccountsVendor objClass = new Cls_StatementOfAccountsVendor();
                //    return FetchNewVendor(objClass, strCondition);
                //case "COMMODITY":
                //    clsRestriction objClass = new clsRestriction();
                //    return FetchCommodity(objClass, strCondition);
                //case "BBHBL_JOB_REF_NO":
                //    cls_BBHBL_Entry objClass = new cls_BBHBL_Entry();
                //    return FetchForJobBBHblRef(objClass, strCondition);
                //case "EMAILLOG_DOCREF":
                //    cls_EmailTracking objClass = new cls_EmailTracking();
                //    return FetchDocRef(objClass, strCondition);
                //case "EMAILLOG_DOCTYPE":
                //    cls_EmailTracking objClass = new cls_EmailTracking();
                //    return FetchDocType(objClass, strCondition);

                //case "EMAILLOG_SENDTO":
                //    cls_EmailTracking objClass = new cls_EmailTracking();
                //    return FetchSendTo(objClass, strCondition);
                //case "EMAILLOG_SENDFROM":
                //    cls_EmailTracking objClass = new cls_EmailTracking();
                //    return FetchSendFrom(objClass, strCondition);
                default:
                    return null;
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
            Oracle.ManagedDataAccess.Client.OracleCommand selectCommand = new Oracle.ManagedDataAccess.Client.OracleCommand();
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
                        strReturn += "";
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

        //public string FetchVendor(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchVendor(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchSupplierSecondaryServ(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchSupplierSecondaryServ(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string InsertionToUserTaskListTable(object objclass, string strcond)
        //{
        //    string strreturn = null;
        //    try
        //    {
        //        strreturn = objclass.InsertToUserTaskListTable(strcond);
        //        return strreturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        #endregion "Supplier Management :- Common "

        #region " Rating And Tariff :- Common "

        //public string FetchPortCountryTRADE(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchPortCountryTRADE(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchTradeCountry(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchTradeCountry(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        #endregion " Rating And Tariff :- Common "

        #region " Documentation "

        //public string getDocumentationSearch(string strSearchFlag, string strCondition)
        //{
        //    switch (strSearchFlag)
        //    {
        //        case "JOB_CARD_MAWB":
        //            object cme = new clsMAWBEntry();
        //            return FetchForJobRefMAWB(cme, strCondition);

        //        case "JOB_REF_NO":
        //            object cdc = new clsDetentionCalculation();
        //            return FetchForJobRefDetention(strSearchFlag, cdc, strCondition);

        //        case "DETENTION_CALC_REF":
        //            object cdcd = new clsDetentionCalculation();
        //            return FetchDetCalcRef(cdcd, strCondition);

        //        case "WARE_HOUSE":
        //            object cdcw = new clsDetentionCalculation();
        //            return FetchWarehouse(cdcw, strCondition);

        //        case "JOB_REF_NO_DET":
        //            object cdcj = new clsDetentionCalculation();
        //            return FetchForJobRefDetention(strSearchFlag, cdcj, strCondition);

        //        case "GET_ACT_JOB_FOR_EXP_INV_NEW":
        //            //To Display Jobacard DateTime also
        //            object cjcs = new clsJobCardSearch();
        //            return FetchActiveJobCardHBLMBLNew(cjcs, strCondition);

        //        case "IMP_JOB_REF_NO_INV_LIST_NEW":
        //            //'Fetching Jobacrds for Invoice agent Sea(Imp) Listing
        //            object ciasi = new clsInvoiceAgentSeaImpList();
        //            return FetchActiveJobCardAgentImport(ciasi, strCondition);

        //        case "JOB_REF_NO_ACTIVE_HBL_MBL":
        //            //Active job cards
        //            object cjcsa = new clsJobCardSearch();
        //            return FetchActiveJobCardHBLMBL(cjcsa, strCondition);

        //        case "IMP_JOB_REF_NO_ACTIVE":
        //            //imports Active job cards
        //            object objClass = new clsInvoiceAgentSeaImpList();
        //            return FetchActiveIMPJobCard(objClass, strCondition);

        //        case "JOB_REF_NO_INVOICE_AGENT_AIR":
        //            //Booking Details
        //            object ciaer = new clsInvoiceAgentEntryAir();
        //            return FetchInvoiceAgentJCNo(ciaer, strCondition);

        //        case "IMP_JOB_REF_NO_INV_LIST":
        //            object ciasil = new clsInvoiceAgentSeaImpList();
        //            return FetchActiveJobCardForImport(ciasil, strCondition);

        //        case "BATCHNO":
        //            object ccsvis = new cls_CSVInterfaceSAGESea();
        //            return FetchForBATCHNo(ccsvis, strCondition);

        //        case "JOB_REF_NO_AIR_CON":
        //            object cmjca = new clsMSTJobCardAir();
        //            return FetchMasterJobCardAirCON(cmjca, strCondition);

        //        case "FLIGHTNO":
        //            //Existing Flight No from HAWB
        //            object cepaa = new clsExportPreAlertsAir();
        //            return FetchFlightNo(cepaa, strCondition);

        //        case "BATCHNOAIR":
        //            object clscis = new cls_CSVInterfaceSAGEAir();
        //            return FetchForBATCHNoAir(clscis, strCondition);

        //        case "CR_INV_AGENT_NO_IMP":
        //            object clcnt = new clsCreditNoteToAgentAirImpEntry();
        //            return FetchInvoiceNoForCredit(clcnt, strCondition);

        //        case "CR_INV_AGENT_NO":
        //            object clcnta = new clsCreditNoteToAgentEntry();
        //            return FetchInvoiceNoForCredit(clcnta, strCondition);

        //        case "COLLECTION_REF_NO":
        //            object ccl = new clsCollectionlist();
        //            return Fetch_Collection_RefNr(ccl, strCondition);
        //        default:
        //            return null;
        //    }
        //}

        //#endregion " Documentation "

        //#region "Documentation"

        //public string FetchConsigneeLookUp(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchConsigneeLookUp(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchForShipperAndConsigneeHBL(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchForShipperAndConsigneeHBL(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchForShipperAndConsignee(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchForShipperAndConsignee(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        ////Fetch Load Port Agent
        //public string FetchLpAgent(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchLpAgent(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchDpAgent(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchDpAgent(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchForJobRefDetention(string strSearchFlag, object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchForJobRefDetention(strSearchFlag, strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchDetCalcRef(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchDetentionRef(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchWarehouse(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchWarehouse(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchMasterJobCardAir(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchMasterJobCardAir(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchMasterJobCardSea(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchMasterJobCardSea(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchPOL_WORKINGPORTS(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchPOL_WORKINGPORTS(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchDepo_Wise(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchDepo(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchActiveJobCardAgentImport(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchActiveJobAgentImp(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchActiveIMPJobCard(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchJobCardImportForInvoice(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchInvoiceAgentJCNo(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchInvoiceAgentJCNo(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchActiveJobCardForImport(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchActiveJobCard(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchForBATCHNo(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchForBATCHNo(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchMasterJobCardAirCON(object objClass, string strCond)
        //{
        //    //fetching for consolidation
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchMasterJobCardAirCON(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchFlightNo(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchFlightNo(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchForBATCHNoAir(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchForBATCHNoAir(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchInvoiceNoForCredit(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchInvoiceNoForCredit(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string Fetch_Collection_RefNr(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.Fetch_Collection_RefNr(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        ////public string FetchAgentComm(object objClass, string strCond)
        ////{
        ////    string strReturn = null;
        ////    try
        ////    {
        ////        strReturn = objClass.FetchAgentComm(strCond, Loc);
        ////        return strReturn;
        ////    }
        ////    catch (Exception ex)
        ////    {
        ////        throw ex;
        ////    }
        ////}

        ////public string FetchForJobBBHblRef(object objClass, string strCond)
        ////{
        ////    string strReturn = null;
        ////    try
        ////    {
        ////        strReturn = objClass.FetchForJobBBHblRef(strCond);
        ////        return strReturn;
        ////    }
        ////    catch (Exception ex)
        ////    {
        ////        throw ex;
        ////    }
        ////}

        //#endregion "Documentation"

        //#region " Booking "

        ////public string getBookingSearch(string strSearchFlag, string strCondition)
        ////{
        ////    switch (strSearchFlag)
        ////    {
        ////        case "CUSTOMER_CATEGORY_ADDRESS":
        ////            cls_AirBookingEntry objClass = new cls_AirBookingEntry();
        ////            return FetchCustomerCategoryAddress(objClass, strCondition);

        ////        case "CUSTOMSSTATUSCODE":
        ////            cls_AirBookingEntry objClass = new cls_AirBookingEntry();
        ////            return FetchForCustomsSCode(objClass, strCondition);

        ////        case "PACKTYPE":
        ////            //Fetch the pack types
        ////            Cls_BookingEntry objClass = new Cls_BookingEntry();
        ////            return FetchForPackType(objClass, strCondition);

        ////        case "PLACE":
        ////            //Place from the place master
        ////            Cls_BookingEntry objClass = new Cls_BookingEntry();
        ////            return FetchForPlace(objClass, strCondition);

        ////        case "ENQUIRY_BKG":
        ////            clsBookingEnquiry objClass = new clsBookingEnquiry();
        ////            return FetchBkgEnqSearch(objClass, strCondition);

        ////        case "QUOTATIONNO":
        ////            Cls_BookingEntry objClass = new Cls_BookingEntry();
        ////            return FetchForQuotationNo(objClass, strCondition);

        ////        case "VES_VOY_BOOKING":
        ////            Cls_BookingEntry objClass = new Cls_BookingEntry();
        ////            return FetchVesVoyBooking(objClass, strCondition);

        ////        case "ENQUIRY_SEA":
        ////            // Rajesh (25-Jan-2006) Used in Quotation Sea
        ////            Cls_QuotationForBookingSea objClass = new Cls_QuotationForBookingSea();
        ////            return FetchEnquiryRefNo(objClass, strCondition);

        ////        case "ENQUIRY_AIR":
        ////            // Rajesh (25-Jan-2006) Used in Quotation Sea
        ////            Cls_QuotationForBookingAir objClass = new Cls_QuotationForBookingAir();
        ////            return FetchEnquiryRefNoAir(objClass, strCondition);

        ////        case "VESSELVOYAGE":
        ////            cls_Merge_Split_Booking objClass = new cls_Merge_Split_Booking();
        ////            return FetchVesVoySplitBooking(objClass, strCondition);

        ////        case "VESVOY_BOOKING":
        ////            Cls_BookingEntry objClass = new Cls_BookingEntry();
        ////            return FetchVesVoy_Booking(objClass, strCondition);

        ////        case "PAYTYPE":
        ////            cls_AirBookingEntry objClass = new cls_AirBookingEntry();
        ////            return FetchForIncoTermsPayType(objClass, strCondition);

        ////        case "QUOTATIONNOEXPIRY":
        ////            Cls_BookingEntry objClass = new Cls_BookingEntry();
        ////            return FetchForQuotationNoExpiry(objClass, strCondition);

        ////        case "NEWQUOTATIONNO":
        ////            Cls_BookingEntry objClass = new Cls_BookingEntry();
        ////            return FetchForNewQuotationNo(objClass, strCondition);

        ////        case "BOOKINGNR":
        ////            cls_Merge_Split_Booking objClass = new cls_Merge_Split_Booking();
        ////            return FetchBookingNrSplitBooking(objClass, strCondition);

        ////        default:
        ////            return null;
        ////    }
        ////}

        //#endregion " Booking "

        //#region "getCommonSearchProcedure"

        //public string getCommonSearchProcedure(string strSearchFlag, string strCondition, string strChk = "")
        //{
        //    return FetchCOMMONDATA(strCondition, strChk);
        //}

        //public string FetchCOMMONDATA(string strCond, string strChk = "")
        //{
        //    WorkFlow objWF = new WorkFlow();
        //    Oracle.ManagedDataAccess.Client.OracleCommand selectCommand = new Oracle.ManagedDataAccess.Client.OracleCommand();
        //    string strReturn = null;
        //    Array arr = null;
        //    string strSERACH_IN = null;
        //    string C = null;
        //    string C1 = null;
        //    string C3 = null;
        //    string C4 = null;
        //    string C5 = null;
        //    string C6 = null;
        //    string C7 = null;
        //    string C8 = null;
        //    string C9 = null;
        //    string C10 = null;
        //    string C11 = null;
        //    string C12 = null;
        //    string C13 = null;
        //    string C14 = null;
        //    string C15 = null;
        //    string User_pk = null;
        //    string strSearchFlag_IN = null;
        //    string strFromToType = "";
        //    dynamic strNull = "";
        //    string SelectedLoc = null;
        //    string strLOOKUP_IN = null;
        //    arr = strCond.Split('~');
        //    // lOOK UP VALUE E OR L
        //    strLOOKUP_IN = arr(0);
        //    //USER TYPE TEXT WHILE PRESSING THE KEY
        //    strSERACH_IN = arr(1);
        //    if (strSERACH_IN.IndexOf("$") != -1)
        //    {
        //        strSERACH_IN = strSERACH_IN.Replace("$", "&");
        //    }
        //    strSERACH_IN = strSERACH_IN.TrimEnd(',');
        //    User_pk = Convert.ToString(HttpContext.Current.Session["USER_PK"]);
        //    //FLAG FOR ES FOR WHICH FETCHING THE RECORDS
        //    strSearchFlag_IN = arr(2);
        //    if (arr.Length > 3)
        //    {
        //        C3 = arr(3);
        //    }
        //    if (arr.Length > 4)
        //    {
        //        C4 = arr(4);
        //    }
        //    if (arr.Length > 5)
        //    {
        //        C5 = arr(5);
        //    }
        //    if (arr.Length > 6)
        //    {
        //        C6 = arr(6);
        //    }
        //    if (arr.Length > 7)
        //    {
        //        C7 = arr(7);
        //    }
        //    if (arr.Length > 8)
        //    {
        //        C8 = arr(8);
        //    }
        //    if (arr.Length > 9)
        //    {
        //        C9 = arr(9);
        //    }
        //    if (arr.Length > 10)
        //    {
        //        C10 = arr(10);
        //    }
        //    if (arr.Length > 11)
        //    {
        //        C11 = arr(11);
        //    }
        //    if (arr.Length > 12)
        //    {
        //        C12 = arr(12);
        //    }
        //    if (arr.Length > 13)
        //    {
        //        C13 = arr(13);
        //    }
        //    if (arr.Length > 14)
        //    {
        //        C14 = arr(14);
        //    }
        //    if (arr.Length > 15)
        //    {
        //        C15 = arr(15);
        //    }

        //    try
        //    {
        //        objWF.OpenConnection();
        //        selectCommand.Connection = objWF.MyConnection;
        //        selectCommand.CommandType = CommandType.StoredProcedure;
        //        selectCommand.CommandText = objWF.MyUserName + ".GET_EN_COMMON_PKG.GET_COMMON_NEW";

        //        var _with1 = selectCommand.Parameters;
        //        _with1.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
        //        _with1.Add("LOOKUP_VALUE_IN", strLOOKUP_IN).Direction = ParameterDirection.Input;
        //        _with1.Add("SearchFlag", (string.IsNullOrEmpty(strSearchFlag_IN) ? strNull : strSearchFlag_IN)).Direction = ParameterDirection.Input;
        //        _with1.Add("USER_PK_IN", (string.IsNullOrEmpty(User_pk) ? strNull : User_pk)).Direction = ParameterDirection.Input;
        //        _with1.Add("PARAM3_IN", (string.IsNullOrEmpty(C3) ? strNull : C3)).Direction = ParameterDirection.Input;
        //        _with1.Add("PARAM4_IN", (string.IsNullOrEmpty(C4) ? strNull : C4)).Direction = ParameterDirection.Input;
        //        _with1.Add("PARAM5_IN", (string.IsNullOrEmpty(C5) ? strNull : C5)).Direction = ParameterDirection.Input;
        //        _with1.Add("PARAM6_IN", (string.IsNullOrEmpty(C6) ? strNull : C6)).Direction = ParameterDirection.Input;
        //        _with1.Add("PARAM7_IN", (string.IsNullOrEmpty(C7) ? strNull : C7)).Direction = ParameterDirection.Input;
        //        _with1.Add("PARAM8_IN", (string.IsNullOrEmpty(C8) ? strNull : C8)).Direction = ParameterDirection.Input;
        //        _with1.Add("PARAM9_IN", (string.IsNullOrEmpty(C9) ? strNull : C9)).Direction = ParameterDirection.Input;
        //        _with1.Add("PARAM10_IN", (string.IsNullOrEmpty(C10) ? strNull : C10)).Direction = ParameterDirection.Input;
        //        _with1.Add("PARAM11_IN", (string.IsNullOrEmpty(C11) ? strNull : C11)).Direction = ParameterDirection.Input;
        //        _with1.Add("PARAM12_IN", (string.IsNullOrEmpty(C12) ? strNull : C12)).Direction = ParameterDirection.Input;
        //        _with1.Add("PARAM13_IN", (string.IsNullOrEmpty(C13) ? strNull : C13)).Direction = ParameterDirection.Input;
        //        _with1.Add("PARAM14_IN", (string.IsNullOrEmpty(C14) ? strNull : C14)).Direction = ParameterDirection.Input;
        //        _with1.Add("PARAM15_IN", (string.IsNullOrEmpty(C15) ? strNull : C15)).Direction = ParameterDirection.Input;
        //        _with1.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
        //        selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
        //        selectCommand.ExecuteNonQuery();
        //        OracleClob clob = default(OracleClob);
        //        clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
        //        System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
        //        strReturn = strReader.ReadToEnd();
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //    finally
        //    {
        //        selectCommand.Connection.Close();
        //    }
        //}

        //#endregion "getCommonSearchProcedure"

        //#region "getCommonNewDataSearchProcedure"

        //public string getCommonNewDataSearchProcedure(string strSearchFlag, string strCondition, string strChk = "")
        //{
        //    return FetchCOMMONNEWDATA(strCondition, strChk);
        //}

        //public string FetchCOMMONNEWDATA(string strCond, string strChk = "")
        //{
        //    WorkFlow objWF = new WorkFlow();
        //    Oracle.ManagedDataAccess.Client.OracleCommand selectCommand = new Oracle.ManagedDataAccess.Client.OracleCommand();
        //    string strReturn = null;
        //    Array arr = null;
        //    string strSERACH_IN = null;
        //    string C = null;
        //    string C1 = null;
        //    string C3 = null;
        //    string C4 = null;
        //    string C5 = null;
        //    string C6 = null;
        //    string C7 = null;
        //    string C8 = null;
        //    string C9 = null;
        //    string C10 = null;
        //    string C11 = null;
        //    string C12 = null;
        //    string C13 = null;
        //    string C14 = null;
        //    string C15 = null;
        //    string C16 = null;
        //    string C17 = null;
        //    string C18 = null;
        //    string C19 = null;
        //    string C20 = null;
        //    string C21 = null;
        //    string C22 = null;
        //    string C23 = null;
        //    string User_pk = null;
        //    string strSearchFlag_IN = null;
        //    string strFromToType = "";
        //    dynamic strNull = "";
        //    string SelectedLoc = null;
        //    string strLOOKUP_IN = null;
        //    arr = strCond.Split('~');
        //    //***********************
        //    // lOOK UP VALUE E OR L
        //    strLOOKUP_IN = arr(0);
        //    //*******************

        //    //*************************
        //    //USER TYPE TEXT WHILE PRESSING THE KEY
        //    strSERACH_IN = arr(1);
        //    if (strSERACH_IN.IndexOf("$") != -1)
        //    {
        //        strSERACH_IN = strSERACH_IN.Replace("$", "&");
        //    }
        //    strSERACH_IN = strSERACH_IN.TrimEnd(',');
        //    //*************************
        //    //***********************
        //    User_pk = Convert.ToString(HttpContext.Current.Session["USER_PK"]);
        //    //FLAG FOR ES FOR WHICH FETCHING THE RECORDS
        //    strSearchFlag_IN = arr(2);
        //    //*********************
        //    if (arr.Length > 3)
        //    {
        //        C3 = arr(3);
        //    }
        //    if (arr.Length > 4)
        //    {
        //        C4 = arr(4);
        //    }
        //    if (arr.Length > 5)
        //    {
        //        C5 = arr(5);
        //    }
        //    if (arr.Length > 6)
        //    {
        //        C6 = arr(6);
        //    }
        //    if (arr.Length > 7)
        //    {
        //        C7 = arr(7);
        //    }
        //    if (arr.Length > 8)
        //    {
        //        C8 = arr(8);
        //    }
        //    if (arr.Length > 9)
        //    {
        //        C9 = arr(9);
        //    }
        //    if (arr.Length > 10)
        //    {
        //        C10 = arr(10);
        //    }
        //    if (arr.Length > 11)
        //    {
        //        C11 = arr(11);
        //    }
        //    if (arr.Length > 12)
        //    {
        //        C12 = arr(12);
        //    }
        //    if (arr.Length > 13)
        //    {
        //        C13 = arr(13);
        //    }
        //    if (arr.Length > 14)
        //    {
        //        C14 = arr(14);
        //    }
        //    if (arr.Length > 15)
        //    {
        //        C15 = arr(15);
        //    }
        //    if (arr.Length > 16)
        //    {
        //        C16 = arr(16);
        //    }
        //    if (arr.Length > 17)
        //    {
        //        C17 = arr(17);
        //    }
        //    if (arr.Length > 18)
        //    {
        //        C18 = arr(18);
        //    }
        //    if (arr.Length > 19)
        //    {
        //        C19 = arr(19);
        //    }
        //    if (arr.Length > 20)
        //    {
        //        C20 = arr(20);
        //    }
        //    if (arr.Length > 21)
        //    {
        //        C21 = arr(21);
        //    }
        //    if (arr.Length > 22)
        //    {
        //        C22 = arr(22);
        //    }
        //    if (arr.Length > 23)
        //    {
        //        C23 = arr(23);
        //    }
        //    try
        //    {
        //        objWF.OpenConnection();
        //        selectCommand.Connection = objWF.MyConnection;
        //        selectCommand.CommandType = CommandType.StoredProcedure;
        //        selectCommand.CommandText = objWF.MyUserName + ".GET_EN_COMMON_PKG_NEW_DATA.GET_COMMON_NEW";

        //        var _with2 = selectCommand.Parameters;
        //        _with2.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
        //        _with2.Add("LOOKUP_VALUE_IN", strLOOKUP_IN).Direction = ParameterDirection.Input;
        //        _with2.Add("SearchFlag", (string.IsNullOrEmpty(strSearchFlag_IN) ? strNull : strSearchFlag_IN)).Direction = ParameterDirection.Input;
        //        _with2.Add("USER_PK_IN", (string.IsNullOrEmpty(User_pk) ? strNull : User_pk)).Direction = ParameterDirection.Input;
        //        _with2.Add("PARAM3_IN", (string.IsNullOrEmpty(C3) ? strNull : C3)).Direction = ParameterDirection.Input;
        //        _with2.Add("PARAM4_IN", (string.IsNullOrEmpty(C4) ? strNull : C4)).Direction = ParameterDirection.Input;
        //        _with2.Add("PARAM5_IN", (string.IsNullOrEmpty(C5) ? strNull : C5)).Direction = ParameterDirection.Input;
        //        _with2.Add("PARAM6_IN", (string.IsNullOrEmpty(C6) ? strNull : C6)).Direction = ParameterDirection.Input;
        //        _with2.Add("PARAM7_IN", (string.IsNullOrEmpty(C7) ? strNull : C7)).Direction = ParameterDirection.Input;
        //        _with2.Add("PARAM8_IN", (string.IsNullOrEmpty(C8) ? strNull : C8)).Direction = ParameterDirection.Input;
        //        _with2.Add("PARAM9_IN", (string.IsNullOrEmpty(C9) ? strNull : C9)).Direction = ParameterDirection.Input;
        //        _with2.Add("PARAM10_IN", (string.IsNullOrEmpty(C10) ? strNull : C10)).Direction = ParameterDirection.Input;
        //        _with2.Add("PARAM11_IN", (string.IsNullOrEmpty(C11) ? strNull : C11)).Direction = ParameterDirection.Input;
        //        _with2.Add("PARAM12_IN", (string.IsNullOrEmpty(C12) ? strNull : C12)).Direction = ParameterDirection.Input;
        //        _with2.Add("PARAM13_IN", (string.IsNullOrEmpty(C13) ? strNull : C13)).Direction = ParameterDirection.Input;
        //        _with2.Add("PARAM14_IN", (string.IsNullOrEmpty(C14) ? strNull : C14)).Direction = ParameterDirection.Input;
        //        _with2.Add("PARAM15_IN", (string.IsNullOrEmpty(C15) ? strNull : C15)).Direction = ParameterDirection.Input;
        //        _with2.Add("PARAM16_IN", (string.IsNullOrEmpty(C16) ? strNull : C16)).Direction = ParameterDirection.Input;
        //        _with2.Add("PARAM17_IN", (string.IsNullOrEmpty(C17) ? strNull : C17)).Direction = ParameterDirection.Input;
        //        _with2.Add("PARAM18_IN", (string.IsNullOrEmpty(C18) ? strNull : C18)).Direction = ParameterDirection.Input;
        //        _with2.Add("PARAM19_IN", (string.IsNullOrEmpty(C19) ? strNull : C19)).Direction = ParameterDirection.Input;
        //        _with2.Add("PARAM20_IN", (string.IsNullOrEmpty(C20) ? strNull : C20)).Direction = ParameterDirection.Input;
        //        _with2.Add("PARAM21_IN", (string.IsNullOrEmpty(C21) ? strNull : C21)).Direction = ParameterDirection.Input;
        //        _with2.Add("PARAM22_IN", (string.IsNullOrEmpty(C22) ? strNull : C22)).Direction = ParameterDirection.Input;
        //        _with2.Add("PARAM23_IN", (string.IsNullOrEmpty(C23) ? strNull : C23)).Direction = ParameterDirection.Input;
        //        _with2.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
        //        selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
        //        selectCommand.ExecuteNonQuery();
        //        OracleClob clob = default(OracleClob);
        //        clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
        //        System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
        //        strReturn = strReader.ReadToEnd();
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //    finally
        //    {
        //        selectCommand.Connection.Close();
        //    }
        //}

        ////For Common Multiple Enhance Search : Before Changing kindly discuss
        //public string FetchMultipleES(string strCond, string strChk = "")
        //{
        //    WorkFlow objWF = new WorkFlow();
        //    Oracle.ManagedDataAccess.Client.OracleCommand selectCommand = new Oracle.ManagedDataAccess.Client.OracleCommand();
        //    string strReturn = null;
        //    Array arr = null;
        //    string strSERACH_IN = null;
        //    string C = null;
        //    string C1 = null;
        //    string C3 = null;
        //    string C4 = null;
        //    string C5 = null;
        //    string C6 = null;
        //    string C7 = null;
        //    string C8 = null;
        //    string C9 = null;
        //    string C10 = null;
        //    string C11 = null;
        //    string C12 = null;
        //    string User_pk = null;
        //    string strSearchFlag_IN = null;
        //    string strFromToType = "";
        //    dynamic strNull = "";
        //    string SelectedLoc = null;
        //    string strLOOKUP_IN = null;
        //    arr = strCond.Split('~');
        //    //***********************
        //    // lOOK UP VALUE E OR L
        //    strLOOKUP_IN = arr(0);
        //    //*******************

        //    //*************************
        //    //USER TYPE TEXT WHILE PRESSING THE KEY
        //    strSERACH_IN = arr(1);
        //    if (strSERACH_IN.IndexOf("$") != -1)
        //    {
        //        strSERACH_IN = strSERACH_IN.Replace("$", "&");
        //    }
        //    strSERACH_IN = strSERACH_IN.TrimEnd(',');
        //    //*************************
        //    //***********************
        //    User_pk = Convert.ToString(HttpContext.Current.Session["USER_PK"]);
        //    //FLAG FOR ES FOR WHICH FETCHING THE RECORDS
        //    strSearchFlag_IN = arr(2);
        //    //*********************
        //    if (arr.Length > 3)
        //    {
        //        C3 = arr(3);
        //    }
        //    if (arr.Length > 4)
        //    {
        //        C4 = arr(4);
        //    }
        //    if (arr.Length > 5)
        //    {
        //        C5 = arr(5);
        //    }
        //    if (arr.Length > 6)
        //    {
        //        C6 = arr(6);
        //    }
        //    if (arr.Length > 7)
        //    {
        //        C7 = arr(7);
        //    }
        //    if (arr.Length > 8)
        //    {
        //        C8 = arr(8);
        //    }
        //    if (arr.Length > 9)
        //    {
        //        C9 = arr(9);
        //    }
        //    if (arr.Length > 10)
        //    {
        //        C10 = arr(10);
        //    }
        //    if (arr.Length > 11)
        //    {
        //        C11 = arr(11);
        //    }
        //    if (arr.Length > 12)
        //    {
        //        C12 = arr(12);
        //    }

        //    try
        //    {
        //        objWF.OpenConnection();
        //        selectCommand.Connection = objWF.MyConnection;
        //        selectCommand.CommandType = CommandType.StoredProcedure;
        //        selectCommand.CommandText = objWF.MyUserName + ".GET_EN_MULTIPLE_PKG.EN_MULTIPLE_PKG";
        //        var _with3 = selectCommand.Parameters;
        //        _with3.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
        //        _with3.Add("LOOKUP_VALUE_IN", strLOOKUP_IN).Direction = ParameterDirection.Input;
        //        _with3.Add("SearchFlag", (string.IsNullOrEmpty(strSearchFlag_IN) ? strNull : strSearchFlag_IN)).Direction = ParameterDirection.Input;
        //        _with3.Add("USER_PK_IN", (string.IsNullOrEmpty(User_pk) ? strNull : User_pk)).Direction = ParameterDirection.Input;
        //        _with3.Add("PARAM3_IN", (string.IsNullOrEmpty(C3) ? strNull : C3)).Direction = ParameterDirection.Input;
        //        _with3.Add("PARAM4_IN", (string.IsNullOrEmpty(C4) ? strNull : C4)).Direction = ParameterDirection.Input;
        //        _with3.Add("PARAM5_IN", (string.IsNullOrEmpty(C5) ? strNull : C5)).Direction = ParameterDirection.Input;
        //        _with3.Add("PARAM6_IN", (string.IsNullOrEmpty(C6) ? strNull : C6)).Direction = ParameterDirection.Input;
        //        _with3.Add("PARAM7_IN", (string.IsNullOrEmpty(C7) ? strNull : C7)).Direction = ParameterDirection.Input;
        //        _with3.Add("PARAM8_IN", (string.IsNullOrEmpty(C8) ? strNull : C8)).Direction = ParameterDirection.Input;
        //        _with3.Add("PARAM9_IN", (string.IsNullOrEmpty(C9) ? strNull : C9)).Direction = ParameterDirection.Input;
        //        _with3.Add("PARAM10_IN", (string.IsNullOrEmpty(C10) ? strNull : C10)).Direction = ParameterDirection.Input;
        //        _with3.Add("PARAM11_IN", (string.IsNullOrEmpty(C11) ? strNull : C11)).Direction = ParameterDirection.Input;
        //        _with3.Add("PARAM12_IN", (string.IsNullOrEmpty(C12) ? strNull : C12)).Direction = ParameterDirection.Input;
        //        _with3.Add("RETURN_VALUE", OracleDbType.NVarchar2, 32767, "RETURN_VALUE").Direction = ParameterDirection.Output;
        //        selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
        //        selectCommand.ExecuteNonQuery();
        //        strReturn = Convert.ToString((string.IsNullOrEmpty(selectCommand.Parameters["RETURN_VALUE"].Value) ? "" : selectCommand.Parameters["RETURN_VALUE"].Value));
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //    finally
        //    {
        //        selectCommand.Connection.Close();
        //    }
        //}

        //#endregion "getCommonNewDataSearchProcedure"

        //#region "getCommonNewSearchProcedure"

        //public string getCommonNewSearchProcedure(string strSearchFlag, string strCondition, string strChk = "", bool EXD = false)
        //{
        //    return FetchCOMMONDATANew(strCondition, strChk, EXD);
        //}

        //public string FetchCOMMONDATANew(string strCond, string strChk = "", bool EXD = false)
        //{
        //    WorkFlow objWF = new WorkFlow();
        //    Oracle.ManagedDataAccess.Client.OracleCommand selectCommand = new Oracle.ManagedDataAccess.Client.OracleCommand();
        //    string strReturn = null;
        //    Array arr = null;
        //    string strSERACH_IN = null;
        //    string C = null;
        //    string C1 = null;
        //    string C3 = null;
        //    string C4 = null;
        //    string C5 = null;
        //    string C6 = null;
        //    string C7 = null;
        //    string C8 = null;
        //    string C9 = null;
        //    string C10 = null;
        //    string C11 = null;
        //    string C12 = null;
        //    string C13 = null;
        //    string C14 = null;
        //    string C15 = null;
        //    string User_pk = null;
        //    string strSearchFlag_IN = null;
        //    string strFromToType = "";
        //    dynamic strNull = "";
        //    string SelectedLoc = null;
        //    string strLOOKUP_IN = null;
        //    arr = strCond.Split('~');
        //    // lOOK UP VALUE E OR L
        //    strLOOKUP_IN = arr(0);
        //    //USER TYPE TEXT WHILE PRESSING THE KEY
        //    strSERACH_IN = arr(1);
        //    if (strSERACH_IN.IndexOf("$") != -1)
        //    {
        //        strSERACH_IN = strSERACH_IN.Replace("$", "&");
        //    }
        //    strSERACH_IN = strSERACH_IN.TrimEnd(',');
        //    User_pk = Convert.ToString(HttpContext.Current.Session["USER_PK"]);
        //    //FLAG FOR ES FOR WHICH FETCHING THE RECORDS
        //    strSearchFlag_IN = arr(2);
        //    if (arr.Length > 3)
        //    {
        //        C3 = arr(3);
        //    }
        //    if (arr.Length > 4)
        //    {
        //        C4 = arr(4);
        //    }
        //    if (arr.Length > 5)
        //    {
        //        C5 = arr(5);
        //    }
        //    if (arr.Length > 6)
        //    {
        //        C6 = arr(6);
        //    }
        //    if (arr.Length > 7)
        //    {
        //        C7 = arr(7);
        //    }
        //    if (arr.Length > 8)
        //    {
        //        C8 = arr(8);
        //    }
        //    if (arr.Length > 9)
        //    {
        //        C9 = arr(9);
        //    }
        //    if (arr.Length > 10)
        //    {
        //        C10 = arr(10);
        //    }
        //    if (arr.Length > 11)
        //    {
        //        C11 = arr(11);
        //    }
        //    if (arr.Length > 12)
        //    {
        //        C12 = arr(12);
        //    }
        //    if (arr.Length > 13)
        //    {
        //        C13 = arr(13);
        //    }
        //    if (arr.Length > 14)
        //    {
        //        C14 = arr(14);
        //    }
        //    if (arr.Length > 15)
        //    {
        //        C15 = arr(15);
        //    }

        //    try
        //    {
        //        objWF.OpenConnection();
        //        selectCommand.Connection = objWF.MyConnection;
        //        selectCommand.CommandType = CommandType.StoredProcedure;
        //        //export doc module enhance search
        //        if (EXD)
        //        {
        //            selectCommand.CommandText = objWF.MyUserName + ".GET_EN_COMMON_EXP_DOC_PKG.GET_COMMON_NEW";
        //        }
        //        else
        //        {
        //            selectCommand.CommandText = objWF.MyUserName + ".GET_EN_COMMON_PKG_NEW.GET_COMMON_NEW";
        //        }

        //        var _with4 = selectCommand.Parameters;
        //        _with4.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
        //        _with4.Add("LOOKUP_VALUE_IN", strLOOKUP_IN).Direction = ParameterDirection.Input;
        //        _with4.Add("SearchFlag", (string.IsNullOrEmpty(strSearchFlag_IN) ? strNull : strSearchFlag_IN)).Direction = ParameterDirection.Input;
        //        _with4.Add("USER_PK_IN", (string.IsNullOrEmpty(User_pk) ? strNull : User_pk)).Direction = ParameterDirection.Input;
        //        _with4.Add("PARAM3_IN", (string.IsNullOrEmpty(C3) ? strNull : C3)).Direction = ParameterDirection.Input;
        //        _with4.Add("PARAM4_IN", (string.IsNullOrEmpty(C4) ? strNull : C4)).Direction = ParameterDirection.Input;
        //        _with4.Add("PARAM5_IN", (string.IsNullOrEmpty(C5) ? strNull : C5)).Direction = ParameterDirection.Input;
        //        _with4.Add("PARAM6_IN", (string.IsNullOrEmpty(C6) ? strNull : C6)).Direction = ParameterDirection.Input;
        //        _with4.Add("PARAM7_IN", (string.IsNullOrEmpty(C7) ? strNull : C7)).Direction = ParameterDirection.Input;
        //        _with4.Add("PARAM8_IN", (string.IsNullOrEmpty(C8) ? strNull : C8)).Direction = ParameterDirection.Input;
        //        _with4.Add("PARAM9_IN", (string.IsNullOrEmpty(C9) ? strNull : C9)).Direction = ParameterDirection.Input;
        //        _with4.Add("PARAM10_IN", (string.IsNullOrEmpty(C10) ? strNull : C10)).Direction = ParameterDirection.Input;
        //        _with4.Add("PARAM11_IN", (string.IsNullOrEmpty(C11) ? strNull : C11)).Direction = ParameterDirection.Input;
        //        _with4.Add("PARAM12_IN", (string.IsNullOrEmpty(C12) ? strNull : C12)).Direction = ParameterDirection.Input;
        //        _with4.Add("PARAM13_IN", (string.IsNullOrEmpty(C13) ? strNull : C13)).Direction = ParameterDirection.Input;
        //        _with4.Add("PARAM14_IN", (string.IsNullOrEmpty(C14) ? strNull : C14)).Direction = ParameterDirection.Input;
        //        //export doc module enhance search
        //        if (EXD)
        //        {
        //            for (int _counter = 15; _counter <= 51; _counter++)
        //            {
        //                string _paramName = "PARAM" + _counter.ToString() + "_IN";
        //                string _paramValue = "";
        //                if (arr.Length > _counter)
        //                {
        //                    _paramValue = arr(_counter);
        //                    _with4.Add(_paramName, (string.IsNullOrEmpty(_paramValue) ? strNull : _paramValue)).Direction = ParameterDirection.Input;
        //                }
        //            }
        //        }
        //        _with4.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
        //        selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
        //        selectCommand.ExecuteNonQuery();
        //        OracleClob clob = default(OracleClob);
        //        clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
        //        System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
        //        strReturn = strReader.ReadToEnd();
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //    finally
        //    {
        //        selectCommand.Connection.Close();
        //    }
        //}

        //#endregion "getCommonNewSearchProcedure"

        //#region "getCommonExpiredSearchProcedure"

        //public string getCommonExpiredSearchProcedure(string strSearchFlag, string strCondition, string strChk = "")
        //{
        //    return FetchCOMMONDATAExpiry(strCondition, strChk);
        //}

        //public string getMultipleES(string strSearchFlag, string strCondition, string strChk = "")
        //{
        //    return FetchMultipleES(strCondition, strChk);
        //}

        //public string FetchCOMMONDATAExpiry(string strCond, string strChk = "")
        //{
        //    WorkFlow objWF = new WorkFlow();
        //    Oracle.ManagedDataAccess.Client.OracleCommand selectCommand = new Oracle.ManagedDataAccess.Client.OracleCommand();
        //    string strReturn = null;
        //    Array arr = null;
        //    string strSERACH_IN = null;
        //    string C = null;
        //    string C1 = null;
        //    string C3 = null;
        //    string C4 = null;
        //    string C5 = null;
        //    string C6 = null;
        //    string C7 = null;
        //    string C8 = null;
        //    string C9 = null;
        //    string C10 = null;
        //    string C11 = null;
        //    string C12 = null;
        //    string User_pk = null;
        //    string strSearchFlag_IN = null;
        //    string strFromToType = "";
        //    dynamic strNull = "";
        //    string SelectedLoc = null;
        //    string strLOOKUP_IN = null;
        //    arr = strCond.Split('~');
        //    //***********************
        //    // lOOK UP VALUE E OR L
        //    strLOOKUP_IN = arr(0);
        //    //*******************

        //    //*************************
        //    //USER TYPE TEXT WHILE PRESSING THE KEY
        //    strSERACH_IN = arr(1);
        //    if (strSERACH_IN.IndexOf("$") != -1)
        //    {
        //        strSERACH_IN = strSERACH_IN.Replace("$", "&");
        //    }
        //    strSERACH_IN = strSERACH_IN.TrimEnd(',');
        //    //*************************
        //    //***********************
        //    User_pk = Convert.ToString(HttpContext.Current.Session["USER_PK"]);
        //    //FLAG FOR ES FOR WHICH FETCHING THE RECORDS
        //    strSearchFlag_IN = arr(2);
        //    //*********************
        //    if (arr.Length > 3)
        //    {
        //        C3 = arr(3);
        //    }
        //    if (arr.Length > 4)
        //    {
        //        C4 = arr(4);
        //    }
        //    if (arr.Length > 5)
        //    {
        //        C5 = arr(5);
        //    }
        //    if (arr.Length > 6)
        //    {
        //        C6 = arr(6);
        //    }
        //    if (arr.Length > 7)
        //    {
        //        C7 = arr(7);
        //    }
        //    if (arr.Length > 8)
        //    {
        //        C8 = arr(8);
        //    }
        //    if (arr.Length > 9)
        //    {
        //        C9 = arr(9);
        //    }
        //    if (arr.Length > 10)
        //    {
        //        C10 = arr(10);
        //    }
        //    if (arr.Length > 11)
        //    {
        //        C11 = arr(11);
        //    }
        //    if (arr.Length > 12)
        //    {
        //        C12 = arr(12);
        //    }

        //    try
        //    {
        //        objWF.OpenConnection();
        //        selectCommand.Connection = objWF.MyConnection;
        //        selectCommand.CommandType = CommandType.StoredProcedure;
        //        selectCommand.CommandText = objWF.MyUserName + ".GET_EN_COMMON_PKG_EXP_CTRTS.GET_COMMON_NEW";

        //        var _with5 = selectCommand.Parameters;
        //        _with5.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
        //        _with5.Add("LOOKUP_VALUE_IN", strLOOKUP_IN).Direction = ParameterDirection.Input;
        //        _with5.Add("SearchFlag", (string.IsNullOrEmpty(strSearchFlag_IN) ? strNull : strSearchFlag_IN)).Direction = ParameterDirection.Input;
        //        _with5.Add("USER_PK_IN", (string.IsNullOrEmpty(User_pk) ? strNull : User_pk)).Direction = ParameterDirection.Input;
        //        _with5.Add("PARAM3_IN", (string.IsNullOrEmpty(C3) ? strNull : C3)).Direction = ParameterDirection.Input;
        //        _with5.Add("PARAM4_IN", (string.IsNullOrEmpty(C4) ? strNull : C4)).Direction = ParameterDirection.Input;
        //        _with5.Add("PARAM5_IN", (string.IsNullOrEmpty(C5) ? strNull : C5)).Direction = ParameterDirection.Input;
        //        _with5.Add("PARAM6_IN", (string.IsNullOrEmpty(C6) ? strNull : C6)).Direction = ParameterDirection.Input;
        //        _with5.Add("PARAM7_IN", (string.IsNullOrEmpty(C7) ? strNull : C7)).Direction = ParameterDirection.Input;
        //        _with5.Add("PARAM8_IN", (string.IsNullOrEmpty(C8) ? strNull : C8)).Direction = ParameterDirection.Input;
        //        _with5.Add("PARAM9_IN", (string.IsNullOrEmpty(C9) ? strNull : C9)).Direction = ParameterDirection.Input;
        //        _with5.Add("PARAM10_IN", (string.IsNullOrEmpty(C10) ? strNull : C10)).Direction = ParameterDirection.Input;
        //        _with5.Add("PARAM11_IN", (string.IsNullOrEmpty(C11) ? strNull : C11)).Direction = ParameterDirection.Input;
        //        _with5.Add("PARAM12_IN", (string.IsNullOrEmpty(C12) ? strNull : C12)).Direction = ParameterDirection.Input;
        //        _with5.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
        //        selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
        //        selectCommand.ExecuteNonQuery();
        //        OracleClob clob = default(OracleClob);
        //        clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
        //        System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
        //        strReturn = strReader.ReadToEnd();
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //    finally
        //    {
        //        selectCommand.Connection.Close();
        //    }
        //}

        //#endregion "getCommonExpiredSearchProcedure"

        //#region "Fetch Gen Common Search Function"

        //public string FetchGenCommonSearch(string strSearchFlag, string strCond, string strChk = "")
        //{
        //    WorkFlow objWF = new WorkFlow();
        //    Oracle.ManagedDataAccess.Client.OracleCommand selectCommand = new Oracle.ManagedDataAccess.Client.OracleCommand();
        //    string strReturn = null;
        //    Array arr = null;
        //    string strSERACH_IN = null;
        //    string C = null;
        //    string C1 = null;
        //    string C2 = null;
        //    string C3 = null;
        //    string C4 = null;
        //    string C5 = null;
        //    string C6 = null;
        //    string C7 = null;
        //    string C8 = null;
        //    string C9 = null;
        //    string C10 = null;
        //    string User_pk = null;
        //    string strSearchFlag_IN = null;
        //    string strFromToType = "";
        //    dynamic strNull = "";
        //    string SelectedLoc = null;
        //    string strLOOKUP_IN = null;
        //    arr = strCond.Split('~');
        //    // lOOK UP VALUE E OR L
        //    strLOOKUP_IN = arr(0);
        //    //USER TYPE TEXT WHILE PRESSING THE KEY
        //    strSERACH_IN = arr(1);
        //    if (strSERACH_IN.IndexOf("$") != -1)
        //    {
        //        strSERACH_IN = strSERACH_IN.Replace("$", "&");
        //    }
        //    strSERACH_IN = strSERACH_IN.TrimEnd(',');
        //    User_pk = Convert.ToString(HttpContext.Current.Session["USER_PK"]);
        //    //FLAG FOR ES FOR WHICH FETCHING THE RECORDS
        //    strSearchFlag_IN = strSearchFlag;
        //    if (arr.Length > 2)
        //    {
        //        C2 = arr(2);
        //    }
        //    if (arr.Length > 3)
        //    {
        //        C3 = arr(3);
        //    }
        //    if (arr.Length > 4)
        //    {
        //        C4 = arr(4);
        //    }
        //    if (arr.Length > 5)
        //    {
        //        C5 = arr(5);
        //    }
        //    if (arr.Length > 6)
        //    {
        //        C6 = arr(6);
        //    }
        //    if (arr.Length > 7)
        //    {
        //        C7 = arr(7);
        //    }
        //    if (arr.Length > 8)
        //    {
        //        C8 = arr(8);
        //    }
        //    if (arr.Length > 9)
        //    {
        //        C9 = arr(9);
        //    }
        //    if (arr.Length > 10)
        //    {
        //        C10 = arr(10);
        //    }

        //    try
        //    {
        //        objWF.OpenConnection();
        //        selectCommand.Connection = objWF.MyConnection;
        //        selectCommand.CommandType = CommandType.StoredProcedure;
        //        selectCommand.CommandText = objWF.MyUserName + ".GET_COMMON_PKG.GET_COMMON_EN";

        //        var _with6 = selectCommand.Parameters;
        //        _with6.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
        //        _with6.Add("LOOKUP_VALUE_IN", strLOOKUP_IN).Direction = ParameterDirection.Input;
        //        _with6.Add("SearchFlag", (string.IsNullOrEmpty(strSearchFlag_IN) ? strNull : strSearchFlag_IN)).Direction = ParameterDirection.Input;
        //        _with6.Add("USER_PK_IN", (string.IsNullOrEmpty(User_pk) ? strNull : User_pk)).Direction = ParameterDirection.Input;
        //        _with6.Add("PARAM2_IN", (string.IsNullOrEmpty(C2) ? strNull : C2)).Direction = ParameterDirection.Input;
        //        _with6.Add("PARAM3_IN", (string.IsNullOrEmpty(C3) ? strNull : C3)).Direction = ParameterDirection.Input;
        //        _with6.Add("PARAM4_IN", (string.IsNullOrEmpty(C4) ? strNull : C4)).Direction = ParameterDirection.Input;
        //        _with6.Add("PARAM5_IN", (string.IsNullOrEmpty(C5) ? strNull : C5)).Direction = ParameterDirection.Input;
        //        _with6.Add("PARAM6_IN", (string.IsNullOrEmpty(C6) ? strNull : C6)).Direction = ParameterDirection.Input;
        //        _with6.Add("PARAM7_IN", (string.IsNullOrEmpty(C7) ? strNull : C7)).Direction = ParameterDirection.Input;
        //        _with6.Add("PARAM8_IN", (string.IsNullOrEmpty(C8) ? strNull : C8)).Direction = ParameterDirection.Input;
        //        _with6.Add("PARAM9_IN", (string.IsNullOrEmpty(C9) ? strNull : C9)).Direction = ParameterDirection.Input;
        //        _with6.Add("PARAM10_IN", (string.IsNullOrEmpty(C10) ? strNull : C10)).Direction = ParameterDirection.Input;
        //        _with6.Add("RETURN_VALUE", OracleDbType.NVarchar2, 32767, "RETURN_VALUE").Direction = ParameterDirection.Output;
        //        selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
        //        selectCommand.ExecuteNonQuery();
        //        strReturn = Convert.ToString((string.IsNullOrEmpty(selectCommand.Parameters["RETURN_VALUE"].Value) ? "" : selectCommand.Parameters["RETURN_VALUE"].Value));
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //    finally
        //    {
        //        selectCommand.Connection.Close();
        //    }
        //}

        //#endregion "Fetch Gen Common Search Function"

        //#region "getCommon_DOCUMENTPKGProcedure"

        //public string getCommon_DOCUMENTProcedure(string strSearchFlag, string strCondition, string strChk = "")
        //{
        //    return FetchGETENCOMMON_DOCUMENTPKG(strCondition, strChk);
        //}

        //public string FetchGETENCOMMON_DOCUMENTPKG(string strCond, string strChk = "")
        //{
        //    WorkFlow objWF = new WorkFlow();
        //    Oracle.ManagedDataAccess.Client.OracleCommand selectCommand = new Oracle.ManagedDataAccess.Client.OracleCommand();
        //    string strReturn = null;
        //    Array arr = null;
        //    string strSERACH_IN = null;
        //    string C = null;
        //    string C1 = null;
        //    string C3 = null;
        //    string C4 = null;
        //    string C5 = null;
        //    string C6 = null;
        //    string C7 = null;
        //    string C8 = null;
        //    string C9 = null;
        //    string C10 = null;
        //    string C11 = null;
        //    string C12 = null;
        //    string User_pk = null;
        //    string strSearchFlag_IN = null;
        //    string strFromToType = "";
        //    dynamic strNull = "";
        //    string SelectedLoc = null;
        //    string strLOOKUP_IN = null;
        //    arr = strCond.Split('~');
        //    //***********************
        //    // lOOK UP VALUE E OR L
        //    strLOOKUP_IN = arr(0);
        //    //*******************

        //    //*************************
        //    //USER TYPE TEXT WHILE PRESSING THE KEY
        //    strSERACH_IN = arr(1);
        //    if (strSERACH_IN.IndexOf("$") != -1)
        //    {
        //        strSERACH_IN = strSERACH_IN.Replace("$", "&");
        //    }
        //    strSERACH_IN = strSERACH_IN.TrimEnd(',');
        //    //*************************
        //    //***********************
        //    User_pk = Convert.ToString(HttpContext.Current.Session["USER_PK"]);
        //    //FLAG FOR ES FOR WHICH FETCHING THE RECORDS
        //    strSearchFlag_IN = arr(2);
        //    //*********************
        //    if (arr.Length > 3)
        //    {
        //        C3 = arr(3);
        //    }
        //    if (arr.Length > 4)
        //    {
        //        C4 = arr(4);
        //    }
        //    if (arr.Length > 5)
        //    {
        //        C5 = arr(5);
        //    }
        //    if (arr.Length > 6)
        //    {
        //        C6 = arr(6);
        //    }
        //    if (arr.Length > 7)
        //    {
        //        C7 = arr(7);
        //    }
        //    if (arr.Length > 8)
        //    {
        //        C8 = arr(8);
        //    }
        //    if (arr.Length > 9)
        //    {
        //        C9 = arr(9);
        //    }
        //    if (arr.Length > 10)
        //    {
        //        C10 = arr(10);
        //    }
        //    if (arr.Length > 11)
        //    {
        //        C11 = arr(11);
        //    }
        //    if (arr.Length > 12)
        //    {
        //        C12 = arr(12);
        //    }

        //    try
        //    {
        //        objWF.OpenConnection();
        //        selectCommand.Connection = objWF.MyConnection;
        //        selectCommand.CommandType = CommandType.StoredProcedure;
        //        selectCommand.CommandText = objWF.MyUserName + ".GET_EN_COMMON_DOCUMENT_PKG.GET_EN_COMMON_DOCUMENT";
        //        var _with7 = selectCommand.Parameters;
        //        _with7.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
        //        _with7.Add("LOOKUP_VALUE_IN", strLOOKUP_IN).Direction = ParameterDirection.Input;
        //        _with7.Add("SearchFlag", (string.IsNullOrEmpty(strSearchFlag_IN) ? strNull : strSearchFlag_IN)).Direction = ParameterDirection.Input;
        //        _with7.Add("USER_PK_IN", (string.IsNullOrEmpty(User_pk) ? strNull : User_pk)).Direction = ParameterDirection.Input;
        //        _with7.Add("PARAM3_IN", (string.IsNullOrEmpty(C3) ? strNull : C3)).Direction = ParameterDirection.Input;
        //        _with7.Add("PARAM4_IN", (string.IsNullOrEmpty(C4) ? strNull : C4)).Direction = ParameterDirection.Input;
        //        _with7.Add("PARAM5_IN", (string.IsNullOrEmpty(C5) ? strNull : C5)).Direction = ParameterDirection.Input;
        //        _with7.Add("PARAM6_IN", (string.IsNullOrEmpty(C6) ? strNull : C6)).Direction = ParameterDirection.Input;
        //        _with7.Add("PARAM7_IN", (string.IsNullOrEmpty(C7) ? strNull : C7)).Direction = ParameterDirection.Input;
        //        _with7.Add("PARAM8_IN", (string.IsNullOrEmpty(C8) ? strNull : C8)).Direction = ParameterDirection.Input;
        //        _with7.Add("PARAM9_IN", (string.IsNullOrEmpty(C9) ? strNull : C9)).Direction = ParameterDirection.Input;
        //        _with7.Add("PARAM10_IN", (string.IsNullOrEmpty(C10) ? strNull : C10)).Direction = ParameterDirection.Input;
        //        _with7.Add("PARAM11_IN", (string.IsNullOrEmpty(C11) ? strNull : C11)).Direction = ParameterDirection.Input;
        //        _with7.Add("PARAM12_IN", (string.IsNullOrEmpty(C12) ? strNull : C12)).Direction = ParameterDirection.Input;
        //        _with7.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
        //        selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
        //        selectCommand.ExecuteNonQuery();
        //        OracleClob clob = default(OracleClob);
        //        clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
        //        System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
        //        strReturn = strReader.ReadToEnd();
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //    finally
        //    {
        //        selectCommand.Connection.Close();
        //    }
        //}

        //#endregion "getCommon_DOCUMENTPKGProcedure"

        //#region " RFQ "

        //public string getRFQSearch(string strSearchFlag, string strCondition)
        //{
        //    switch (strSearchFlag)
        //    {
        //        case "OPERATOR_RFQ_INSERT_STATUS":
        //            object corsro = new Cls_OperatorRFQSpotRate();
        //            return RFQSpotRateInsertStatus(corsro, strCondition);

        //        case "AIRLINE_RFQ_INSERT_STATUS":
        //            object corsra = new Cls_AirlineRFQSpotRate();
        //            return RFQSpotRateInsertStatus(corsra, strCondition);

        //        case "CONTRACT_NO_FOR_RFQ_AIR":
        //            object corsrc = new Cls_AirlineRFQSpotRate();
        //            return ContractNoForAirRfq(corsrc, strCondition);

        //        default:
        //            return null;
        //    }
        //}

        //public string FetchForQuotationNoExpiry(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchForQuotationNoExpiry(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchForNewQuotationNo(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchForNewQuotationNo(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //#endregion " RFQ "

        //#region "CustBasebEmployee"

        //public string FetchCustBasedEmp(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchCustBasedEmp(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //#endregion "CustBasebEmployee"

        //#region " Rating And Tariff :- RFQ "

        //public string getRatingAndTariff(string strSearchFlag, string strCondition)
        //{
        //    switch (strSearchFlag)
        //    {
        //        case "OPERATOR_TARIFF_SHOW":
        //            object csc = new Cls_SRRSeaContract();
        //            return OperatorTariff(csc, strCondition);

        //        case "AIR_TARIFF_SHOW":
        //            return AirlineTariff(strCondition);

        //        case "CUSTOMER_CONTRACT_SHOW":
        //            object cssc = new Cls_SRRSeaContract();
        //            return FetchCustomerContract(cssc, strCondition);

        //        case "CUSTOMER_CONTRACT_AIR_SHOW":
        //            return FetchCustomerContract_Air(strCondition);

        //        case "SPOT_RATE_SHOW":
        //            return FetchSpotRate(strCondition);
        //        default:
        //            return null;
        //    }
        //}

        //public string OperatorTariff(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchOperatorTariff(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string AirlineTariff(string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = Cls_SRRAirContract.FetchAirlineTariff(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string RFQSpotRateInsertStatus(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchRFQSpotRateInsertStatus(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string ContractNoForAirRfq(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchContractNoForAirRfq(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //#endregion " Rating And Tariff :- RFQ "

        //#region " Rating And Tariff :- Common "

        //public string FetchCustomerContract(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchCustomerContract(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchCustomerContract_Air(string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = Cls_SRRAirContract.Fetch_Customer_Contract_Air(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchSpotRate(string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = Cls_SRRAirContract.Fetch_SpotRate(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //// Used in Operator Tariff Listing to fetch tariff of particular operator only
        //public string FetchTariffForOperator(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchTariffForOperator(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //// Used in Airline Tariff Listing to fetch tariff of particular airline only
        //public string FetchTariffForAirline(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchTariffForAirline(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string Fetch_Customer_For_Category(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchCustomerForCategory(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //// Used in Operator RFQ Spot Rate Listing
        //public string Fetch_Customer_For_Operator(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchCustomerForOperator(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //// Used in Operator RFQ Spot Rate Listing
        //public string Fetch_RFQ_For_Operator_Customer(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchRfqForOperatorCustomer(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string Fetch_Customer_For_Airline(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchCustomerForAirline(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //// Used in Airline RFQ Spot Rate Listing
        //public string Fetch_SRR_For_Operator_Customer(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchSRRForOperatorCustomer(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string Fetch_Cont_For_Operator_Customer(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchContForOperatorCustomer(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string Fetch_Cont_For_Airline_Customer(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchCONTForAirlineCustomer(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //// Used in RFQ Spot Rate Entry
        //public string Fetch_Commodity_For_Group(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchCommodityForGroup(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //// Used in Enquiry for Rates
        //public string Fetch_Operator_For_Enquiry(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchOperatorForEnquiry(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        ////Fetch_Airline_For_Enquiry
        //public string Fetch_Airline_For_Enquiry_EXP_Manifest(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchAirlineForEnquiryExportManifest(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        ////Added by Snigdharani to fetch customers based on the location of the POL or POD selected.
        //public string FetchPOLPODBasedCust(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchPOLPODBasedCust(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchGroupCustomer(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchGroupCustomer(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //#endregion " Rating And Tariff :- Common "

        //#region "Fetch Common Customs Brokerage"

        //public string getCOMMONCustomsBrokerage(string strSearchFlag, string strCondition, string strChk = "")
        //{
        //    return FetchCOMMONCustomsBrokerage(strCondition, strChk);
        //}

        //public string FetchCOMMONCustomsBrokerage(string strCond, string strChk = "")
        //{
        //    WorkFlow objWF = new WorkFlow();
        //    Oracle.ManagedDataAccess.Client.OracleCommand selectCommand = new Oracle.ManagedDataAccess.Client.OracleCommand();
        //    string strReturn = null;
        //    Array arr = null;
        //    string strSERACH_IN = null;
        //    string C = null;
        //    string C1 = null;
        //    string C3 = null;
        //    string C4 = null;
        //    string C5 = null;
        //    string C6 = null;
        //    string C7 = null;
        //    string C8 = null;
        //    string C9 = null;
        //    string C10 = null;
        //    string C11 = null;
        //    string C12 = null;
        //    string C13 = null;
        //    string C14 = null;
        //    string C15 = null;
        //    string C16 = null;
        //    string C17 = null;
        //    string C18 = null;
        //    string C19 = null;
        //    string C20 = null;
        //    string User_pk = null;
        //    string strSearchFlag_IN = null;
        //    string strFromToType = "";
        //    dynamic strNull = "";
        //    string SelectedLoc = null;
        //    string strLOOKUP_IN = null;
        //    arr = strCond.Split('~');
        //    strLOOKUP_IN = arr(0);
        //    strSERACH_IN = arr(1);
        //    if (strSERACH_IN.IndexOf("$") != -1)
        //    {
        //        strSERACH_IN = strSERACH_IN.Replace("$", "&");
        //    }
        //    strSERACH_IN = strSERACH_IN.TrimEnd(',');
        //    User_pk = Convert.ToString(HttpContext.Current.Session["USER_PK"]);
        //    strSearchFlag_IN = arr(2);
        //    if (arr.Length > 3)
        //    {
        //        C3 = arr[3];
        //    }
        //    if (arr.Length > 4)
        //    {
        //        C4 = arr(4);
        //    }
        //    if (arr.Length > 5)
        //    {
        //        C5 = arr(5);
        //    }
        //    if (arr.Length > 6)
        //    {
        //        C6 = arr(6);
        //    }
        //    if (arr.Length > 7)
        //    {
        //        C7 = arr(7);
        //    }
        //    if (arr.Length > 8)
        //    {
        //        C8 = arr(8);
        //    }
        //    if (arr.Length > 9)
        //    {
        //        C9 = arr(9);
        //    }
        //    if (arr.Length > 10)
        //    {
        //        C10 = arr(10);
        //    }
        //    if (arr.Length > 11)
        //    {
        //        C11 = arr(11);
        //    }
        //    if (arr.Length > 12)
        //    {
        //        C12 = arr(12);
        //    }
        //    if (arr.Length > 13)
        //    {
        //        C13 = arr(13);
        //    }
        //    if (arr.Length > 14)
        //    {
        //        C14 = arr(14);
        //    }
        //    if (arr.Length > 15)
        //    {
        //        C15 = arr(15);
        //    }
        //    if (arr.Length > 16)
        //    {
        //        C16 = arr(16);
        //    }
        //    if (arr.Length > 17)
        //    {
        //        C17 = arr(17);
        //    }
        //    if (arr.Length > 18)
        //    {
        //        C18 = arr(18);
        //    }
        //    if (arr.Length > 19)
        //    {
        //        C19 = arr(19);
        //    }
        //    if (arr.Length > 20)
        //    {
        //        C20 = arr(20);
        //    }
        //    try
        //    {
        //        objWF.OpenConnection();
        //        selectCommand.Connection = objWF.MyConnection;
        //        selectCommand.CommandType = CommandType.StoredProcedure;
        //        selectCommand.CommandText = objWF.MyUserName + ".GET_EN_CUSTOMSBROKERAGE_PKG.GET_CUSTOMSBROKERAGE";

        //        var _with8 = selectCommand.Parameters;
        //        _with8.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
        //        _with8.Add("LOOKUP_VALUE_IN", strLOOKUP_IN).Direction = ParameterDirection.Input;
        //        _with8.Add("SearchFlag", (string.IsNullOrEmpty(strSearchFlag_IN) ? strNull : strSearchFlag_IN)).Direction = ParameterDirection.Input;
        //        _with8.Add("USER_PK_IN", (string.IsNullOrEmpty(User_pk) ? strNull : User_pk)).Direction = ParameterDirection.Input;
        //        _with8.Add("PARAM3_IN", (string.IsNullOrEmpty(Strings.Trim(C3)) ? strNull : C3)).Direction = ParameterDirection.Input;
        //        _with8.Add("PARAM4_IN", (string.IsNullOrEmpty(Strings.Trim(C4)) ? strNull : C4)).Direction = ParameterDirection.Input;
        //        _with8.Add("PARAM5_IN", (string.IsNullOrEmpty(Strings.Trim(C5)) ? strNull : C5)).Direction = ParameterDirection.Input;
        //        _with8.Add("PARAM6_IN", (string.IsNullOrEmpty(Strings.Trim(C6)) ? strNull : C6)).Direction = ParameterDirection.Input;
        //        _with8.Add("PARAM7_IN", (string.IsNullOrEmpty(Strings.Trim(C7)) ? strNull : C7)).Direction = ParameterDirection.Input;
        //        _with8.Add("PARAM8_IN", (string.IsNullOrEmpty(Strings.Trim(C8)) ? strNull : C8)).Direction = ParameterDirection.Input;
        //        _with8.Add("PARAM9_IN", (string.IsNullOrEmpty(Strings.Trim(C9)) ? strNull : C9)).Direction = ParameterDirection.Input;
        //        _with8.Add("PARAM10_IN", (string.IsNullOrEmpty(Strings.Trim(C10)) ? strNull : C10)).Direction = ParameterDirection.Input;
        //        _with8.Add("PARAM11_IN", (string.IsNullOrEmpty(Strings.Trim(C11)) ? strNull : C11)).Direction = ParameterDirection.Input;
        //        _with8.Add("PARAM12_IN", (string.IsNullOrEmpty(Strings.Trim(C12)) ? strNull : C12)).Direction = ParameterDirection.Input;
        //        _with8.Add("PARAM13_IN", (string.IsNullOrEmpty(Strings.Trim(C13)) ? strNull : C13)).Direction = ParameterDirection.Input;
        //        _with8.Add("PARAM14_IN", (string.IsNullOrEmpty(Strings.Trim(C14)) ? strNull : C14)).Direction = ParameterDirection.Input;
        //        _with8.Add("PARAM15_IN", (string.IsNullOrEmpty(Strings.Trim(C15)) ? strNull : C15)).Direction = ParameterDirection.Input;
        //        _with8.Add("PARAM16_IN", (string.IsNullOrEmpty(Strings.Trim(C16)) ? strNull : C16)).Direction = ParameterDirection.Input;
        //        _with8.Add("PARAM17_IN", (string.IsNullOrEmpty(Strings.Trim(C17)) ? strNull : C17)).Direction = ParameterDirection.Input;
        //        _with8.Add("PARAM18_IN", (string.IsNullOrEmpty(Strings.Trim(C18)) ? strNull : C18)).Direction = ParameterDirection.Input;
        //        _with8.Add("PARAM19_IN", (string.IsNullOrEmpty(Strings.Trim(C19)) ? strNull : C19)).Direction = ParameterDirection.Input;
        //        _with8.Add("PARAM20_IN", (string.IsNullOrEmpty(Strings.Trim(C20)) ? strNull : C20)).Direction = ParameterDirection.Input;
        //        _with8.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
        //        selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
        //        selectCommand.ExecuteNonQuery();
        //        OracleClob clob = default(OracleClob);
        //        clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
        //        System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
        //        strReturn = strReader.ReadToEnd();
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //    finally
        //    {
        //        selectCommand.Connection.Close();
        //    }
        //}

        //#endregion "Fetch Common Customs Brokerage"

        //public string FetchPOD_NOTWORKPORT(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchPOD_NOTWORKPORT(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //#region " Common Queries :- Customer,CustomerCategory,POL,POD,Depot,Trade,Operator "

        //public string FetchVesselIDName(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchVesselIDName(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchNewVendor(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchNewVendor(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchTransporter(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchTransporter(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        /// <summary>
        /// Fetches the location by name for country.
        /// </summary>
        /// <param name="objClass">The object class.</param>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchLocationByNameForCountry(object objClass, string strCond)
        {
            string strReturn = null;
            try
            {
                clsRegionDetails cod = new clsRegionDetails();
                strReturn = cod.FetchLocationByNameForCountry(strCond);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //public string FetchTransporterZone(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchTransporterZone(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchCostElementID(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchCostElementID(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchJobInMblEntry(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchJobInMblEntry(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchJobInBBMblEntry(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchJobInBBMblEntry(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchMJCInMblEntry(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchMJCInMblEntry(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchForHblRefInMbl(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchForHblRefInMbl(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchForMblRefInImplist(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchForMblRefInImplist(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchForHblRefInImplist(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchForHblRefInImplist(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchForSHPRefInImplist(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchForSHPRefInImplist(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchForCNSRefInImplist(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchForCNSRefInImplist(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchVoyageReAssignRollOver(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchVoyageReAssignRollOver(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchVoyageForBookingRollOver(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchVoyageForBookingRollOver(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchVoyageForJobCard(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchVoyageForJobCard(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchVslVoyRollListing(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchVslVoyRollListing(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchAirlineWithPrefix(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchAirlineWithPrefix(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchAllCHAAgent(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchAllCHAAgent(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchTCPORT(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchTCPORT(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchMawbNr(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchMawbNr(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        ////FetchMawbNr(objClass, strCondition)
        //public string Fetch_Airline_For_Customer(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchAirlineForCustomer(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string Fetch_SRR_For_Airline_Customer(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchSRRForAirlineCustomer(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //// Used in Airline RFQ Spot Rate Listing
        //public string FetchLocation(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchLocation(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchActiveJobCardHBLMBLNew(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchActiveJobCardHBLMBLNew(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchCreditCustomer(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchCreditCustomer(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchAirline(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchAirline(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        /// <summary>
        /// Fetches the pol.
        /// </summary>
        /// <param name="objClass">The object class.</param>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchPOL(object objClass, string strCond)
        {
            string strReturn = null;
            try
            {
                clsPORT_MST_TBL cod = new clsPORT_MST_TBL();
                return cod.FetchPOL(strCond);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //public string FetchPOLIMP(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchPOLIMP(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        /// <summary>
        /// Fetches the pod.
        /// </summary>
        /// <param name="objClass">The object class.</param>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchPOD(object objClass, string strCond)
        {
            string strReturn = null;
            try
            {
                clsPORT_MST_TBL cod = new clsPORT_MST_TBL();
                return cod.FetchPOD(strCond);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //public string FetchPODTARIFF(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchPODTARIFF(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchPODIMP(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchPODIMP(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        /// <summary>
        /// Fetches the operator.
        /// </summary>
        /// <param name="objClass">The object class.</param>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchOperator(object objClass, string strCond)
        {
            string strReturn = null;
            try
            {
                Cls_Operator_Definition cod = new Cls_Operator_Definition();
                return cod.FetchOperator(strCond);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //public string FetchVesselVoyageOpr(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchVesselVoyageOpr(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchVoyageForJobCardNew(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchVoyageForJobCardNew(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchLocationBasedCust(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchLocationBasedCust(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchJobRefNo_Imp_For_DOList(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchImportJobRef_DOList(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchJobRefNo_Imp_For_DO(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchImportJobRef_DO(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchForMblRefInMbllist(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchForMblRefInMbllist(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchJobVendors(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchJobVendors(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchForMblRefInMblimplist(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchForMblRefInMblimplist(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchAgent(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchAgent(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchAgentInvoice(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchAgentInvoice(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchForJobRef(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchForJobRef(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        ////FetchMBLForJobCardImpInvoice
        //public string FetchMBLForJobCardImp(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchMBLForJobCardImp(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchMBLForJobCardImpInvoice(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchMBLForJobCardImpInvoice(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchImpCertificateJob(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchImpCertificateJob(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchCurrency(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchCurrency(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchTempCustomer(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.E_FetchTempCustomer(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchContainerType(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchContainerType(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchCustomer(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchCustomer(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchPOLEnquiry(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchPOLEnquiry(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchPODEnquiry(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchPODEnquiry(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchTrade(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchTrade(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchEmp(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchEmp(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchEmpID(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchEmpID(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string Fetch_Airline_For_Enquiry(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchAirlineForEnquiry(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //// Used in Airline RFQ Spot Rate Listing
        //public string Fetch_RFQ_For_Airline_Customer(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchRfqForAirlineCustomer(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchCommodity(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchCommodity(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchCountry(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchCountry(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchContract(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchContract(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string Fetch_Operator_For_Customer(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchOperatorForCustomer(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FETCH_JOB_REF_PAYREQ(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FETCH_JOB_REF_PAYREQ(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchExpVendorName(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchVendorName(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchForFMCVESSEL(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchForVESSEL(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchForHblRefFMC(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchForHblRef(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchForMAWBJobRef(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchForMAWBJobRef(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchJobRefNo_Imp_For_PrintManager(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchImportJobRef(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchAIRLINEFLIGHT(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchExpAirlineFlight(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchVesselVoyageForInvCB(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchVesselVoyageForInvCB(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchMBLForJobCardExp(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchMBLForJobCardExp(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchMBLForJobCard(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchMBLForJobCard(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchMastersForm(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchMasterForms(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchForMblRefImp(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchForMblRefImp(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchActiveJobCardHBLMBL(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchActiveJobCardHBLMBL(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchVesselVoyageImpCP(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchVesselVoyageImpCP(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchFleightForInvDP(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchFleightForInvDP(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchAllAgents(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchAllAgents(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchVslFlight(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchVslFlight(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchOperatorAirLine(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchOperatorAirLine(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchOperator_WF(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchOperator_WF(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //#endregion " Common Queries :- Customer,CustomerCategory,POL,POD,Depot,Trade,Operator "

        //#region "MIS"

        //public string FetchRefNoForTrackNTrace(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchRefNoForTrackNTrace(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string Fetchvesselvoyagebypolandpod(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchVesselVoyageBYPOLandpod(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchForJobRefMAWB(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchForJobRefMAWB(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //#endregion "MIS"

        //#region " Common Queries :- EDI, Print "

        //public string FetchHBLForJobCardS(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchHBLForJobCards(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchVesselVoyage(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchVesselVoyage(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchForHblRef(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchForHblRef(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchForHblRefMAWB(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchForHblRefMAWB(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchExportAR(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.Fetch_Export_AR(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchExportAP(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.Fetch_Export_AP(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchNewCurrency(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchNewCurrency(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchForMblRefInSuppInv(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchForMblRefInSuppInv(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchVesselsRpt(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchVesselsRpt(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchVoyage(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchVoyage(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //#endregion " Common Queries :- EDI, Print "

        //#region "User, Executive"

        //public string FetchUser(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchUser(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchExecutive_All(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchExecutive_All(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchExecutiveAll(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchExecutive_All(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchExecutive_Other(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchExecutive_Other(strCond, Loc);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //#endregion "User, Executive"

        //#region "Booking"

        //public string FetchCustomerCategoryAddress(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchCustomerCategoryAddress(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchForIncoTermsPayType(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchForIncoTermsPayType(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchForCustomsSCode(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchForCustomsSCode(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchForPackType(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchForPackType(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchVesVoySplitBooking(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchVesVoySplitBooking(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchForPlace(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchForPlace(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchBkgEnqSearch(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.GetBooking(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchForQuotationNo(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchForQuotationNo(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchVesVoyBooking(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchVesVoyBooking(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchEnquiryRefNo(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchEnquiryRefNo(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchEnquiryRefNoAir(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchEnquiryRefNo(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchVesVoy_Booking(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchVesVoy_Booking(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchJobcard(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchJobcard(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchAllJobcard(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchAllJobcard(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchLocationByName(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchLocationByName(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchCustomer_Temp(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchCustomer_Temp(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchBookingNrSplitBooking(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchBookingNrSplitBooking(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchDocRef(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchDocRef(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchDocType(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchDocType(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchSendTo(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchSendTo(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string FetchSendFrom(object objClass, string strCond)
        //{
        //    string strReturn = null;
        //    try
        //    {
        //        strReturn = objClass.FetchSendFrom(strCond);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //#endregion "Booking"

        //#region "Fetch En Common Search Function"

        //public string FetchEnCommonSearch(string strCond = "")
        //{
        //    WorkFlow objWF = new WorkFlow();
        //    Oracle.ManagedDataAccess.Client.OracleCommand selectCommand = new Oracle.ManagedDataAccess.Client.OracleCommand();
        //    string strReturn = null;
        //    Array arr = null;
        //    string strSERACH_IN = null;
        //    string strREGION_IN = null;
        //    string strAREA_IN = null;
        //    string strDATE_IN = null;
        //    string strSALEHDR_IN = null;
        //    string strLOOKUP_IN = null;
        //    string strActive_IN = null;
        //    string strMessageNo_IN = null;
        //    string strWherecond_IN = null;
        //    string strWherecond2_IN = null;
        //    string CONDITION_IN = null;
        //    string CONDITION2_IN = null;
        //    string strSearchFlag_IN = null;
        //    //Dim strFromToType As String = ""
        //    dynamic strNull = "";
        //    string strTableName_IN = null;
        //    string strFieldID_IN = null;
        //    string strFieldName_IN = null;
        //    string strFieldPK_IN = null;
        //    arr = strCond.Split('~');
        //    strLOOKUP_IN = arr(0);
        //    strSERACH_IN = arr(1);
        //    if (strSERACH_IN.IndexOf("$") != -1)
        //    {
        //        strSERACH_IN = strSERACH_IN.Replace("$", "&");
        //    }
        //    strSERACH_IN = strSERACH_IN.TrimEnd(',');
        //    strTableName_IN = arr(2);
        //    strFieldPK_IN = arr(3);
        //    strFieldID_IN = arr(4);
        //    strFieldName_IN = arr(5);
        //    strActive_IN = arr(6);
        //    strWherecond_IN = arr(7);
        //    if (arr.Length == 9)
        //    {
        //        CONDITION_IN = arr(8);
        //    }
        //    if (arr.Length == 11)
        //    {
        //        CONDITION_IN = arr(8);
        //        strWherecond2_IN = arr(9);
        //        CONDITION2_IN = arr(10);
        //    }

        //    try
        //    {
        //        objWF.OpenConnection();
        //        selectCommand.Connection = objWF.MyConnection;
        //        selectCommand.CommandType = CommandType.StoredProcedure;

        //        selectCommand.CommandText = objWF.MyUserName + ".EN_COMMON_SEARCH_PKG.GET_EN_COMMON_SEARCH";

        //        var _with9 = selectCommand.Parameters;
        //        _with9.Add("TABLE_NAME_IN", (string.IsNullOrEmpty(strTableName_IN) ? strNull : strTableName_IN)).Direction = ParameterDirection.Input;
        //        _with9.Add("LOOKUP_VALUE_IN", strLOOKUP_IN).Direction = ParameterDirection.Input;
        //        _with9.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
        //        _with9.Add("FIELD_NAME_PK_IN", strFieldPK_IN).Direction = ParameterDirection.Input;
        //        _with9.Add("FIELD_NAME_ID_IN", (string.IsNullOrEmpty(strFieldID_IN) ? strNull : strFieldID_IN)).Direction = ParameterDirection.Input;
        //        _with9.Add("FIELD_NAME_NAME_IN", (string.IsNullOrEmpty(strFieldName_IN) ? strNull : strFieldName_IN)).Direction = ParameterDirection.Input;
        //        _with9.Add("WHERECONDITION_IN", (string.IsNullOrEmpty(strWherecond_IN) ? strNull : strWherecond_IN)).Direction = ParameterDirection.Input;
        //        _with9.Add("CONDITION_IN", (string.IsNullOrEmpty(CONDITION_IN) ? strNull : CONDITION_IN)).Direction = ParameterDirection.Input;

        //        _with9.Add("WHERECONDITION2_IN", (string.IsNullOrEmpty(strWherecond2_IN) ? strNull : strWherecond2_IN)).Direction = ParameterDirection.Input;
        //        _with9.Add("CONDITION2_IN", (string.IsNullOrEmpty(CONDITION2_IN) ? strNull : CONDITION2_IN)).Direction = ParameterDirection.Input;

        //        _with9.Add("ACTIVE_FILED_NAME_IN", (string.IsNullOrEmpty(strActive_IN) ? strNull : strActive_IN)).Direction = ParameterDirection.Input;
        //        _with9.Add("RETURN_VALUE", OracleDbType.NVarchar2, 6000, "RETURN_VALUE").Direction = ParameterDirection.Output;
        //        selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

        //        selectCommand.ExecuteNonQuery();
        //        strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //    finally
        //    {
        //        selectCommand.Connection.Close();
        //    }
        //}

        #endregion " Documentation "

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
            Oracle.ManagedDataAccess.Client.OracleCommand selectCommand = new Oracle.ManagedDataAccess.Client.OracleCommand();
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
            dynamic strNull = "";
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
            User_pk = Convert.ToString(3);
            //FLAG FOR ES FOR WHICH FETCHING THE RECORDS
            strSearchFlag_IN = Convert.ToString(arr.GetValue(2)); ;
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
                Oracle.ManagedDataAccess.Types.OracleClob clob = default(Oracle.ManagedDataAccess.Types.OracleClob);
                clob = (Oracle.ManagedDataAccess.Types.OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
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

        //public string getCommonSearchRating(string strSearchFlag, string strCondition, string strChk = "")
        //{
        //    return FetchCommonData_Rating(strCondition, strChk);
        //}

        //public string FetchCommonData_Rating(string strCond, string strChk = "")
        //{
        //    WorkFlow objWF = new WorkFlow();
        //    Oracle.ManagedDataAccess.Client.OracleCommand selectCommand = new Oracle.ManagedDataAccess.Client.OracleCommand();
        //    string strReturn = null;
        //    Array arr = null;
        //    string strSERACH_IN = null;
        //    string C = null;
        //    string C1 = null;
        //    string C3 = null;
        //    string C4 = null;
        //    string C5 = null;
        //    string C6 = null;
        //    string C7 = null;
        //    string C8 = null;
        //    string C9 = null;
        //    string C10 = null;
        //    string C11 = null;
        //    string C12 = null;
        //    string C13 = null;
        //    string C14 = null;
        //    string C15 = null;
        //    string C16 = null;
        //    string User_pk = null;
        //    string strSearchFlag_IN = null;
        //    string strFromToType = "";
        //    dynamic strNull = "";
        //    string SelectedLoc = null;
        //    string strLOOKUP_IN = null;
        //    arr = strCond.Split('~');
        //    // lOOK UP VALUE E OR L
        //    strLOOKUP_IN = arr(0);
        //    //USER TYPE TEXT WHILE PRESSING THE KEY
        //    strSERACH_IN = arr(1);
        //    if (strSERACH_IN.IndexOf("$") != -1)
        //    {
        //        strSERACH_IN = strSERACH_IN.Replace("$", "&");
        //    }
        //    strSERACH_IN = strSERACH_IN.TrimEnd(',');
        //    User_pk = Convert.ToString(HttpContext.Current.Session["USER_PK"]);
        //    //FLAG FOR ES FOR WHICH FETCHING THE RECORDS
        //    strSearchFlag_IN = arr(2);
        //    if (arr.Length > 3)
        //    {
        //        C3 = arr(3);
        //    }
        //    if (arr.Length > 4)
        //    {
        //        C4 = arr(4);
        //    }
        //    if (arr.Length > 5)
        //    {
        //        C5 = arr(5);
        //    }
        //    if (arr.Length > 6)
        //    {
        //        C6 = arr(6);
        //    }
        //    if (arr.Length > 7)
        //    {
        //        C7 = arr(7);
        //    }
        //    if (arr.Length > 8)
        //    {
        //        C8 = arr(8);
        //    }
        //    if (arr.Length > 9)
        //    {
        //        C9 = arr(9);
        //    }
        //    if (arr.Length > 10)
        //    {
        //        C10 = arr(10);
        //    }
        //    if (arr.Length > 11)
        //    {
        //        C11 = arr(11);
        //    }
        //    if (arr.Length > 12)
        //    {
        //        C12 = arr(12);
        //    }
        //    if (arr.Length > 13)
        //    {
        //        C13 = arr(13);
        //    }
        //    if (arr.Length > 14)
        //    {
        //        C14 = arr(14);
        //    }
        //    if (arr.Length > 15)
        //    {
        //        C15 = arr(15);
        //    }
        //    if (arr.Length > 16)
        //    {
        //        C16 = arr(16);
        //    }

        //    try
        //    {
        //        objWF.OpenConnection();
        //        selectCommand.Connection = objWF.MyConnection;
        //        selectCommand.CommandType = CommandType.StoredProcedure;
        //        selectCommand.CommandText = objWF.MyUserName + ".GET_EN_COMMON_TARIFF_PKG.GET_COMMON_TARIFF";

        //        var _with11 = selectCommand.Parameters;
        //        _with11.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
        //        _with11.Add("LOOKUP_VALUE_IN", strLOOKUP_IN).Direction = ParameterDirection.Input;
        //        _with11.Add("SearchFlag", (string.IsNullOrEmpty(strSearchFlag_IN) ? strNull : strSearchFlag_IN)).Direction = ParameterDirection.Input;
        //        _with11.Add("USER_PK_IN", (string.IsNullOrEmpty(User_pk) ? strNull : User_pk)).Direction = ParameterDirection.Input;
        //        _with11.Add("PARAM3_IN", (string.IsNullOrEmpty(Strings.Trim(C3)) ? strNull : C3)).Direction = ParameterDirection.Input;
        //        _with11.Add("PARAM4_IN", (string.IsNullOrEmpty(Strings.Trim(C4)) ? strNull : C4)).Direction = ParameterDirection.Input;
        //        _with11.Add("PARAM5_IN", (string.IsNullOrEmpty(Strings.Trim(C5)) ? strNull : C5)).Direction = ParameterDirection.Input;
        //        _with11.Add("PARAM6_IN", (string.IsNullOrEmpty(Strings.Trim(C6)) ? strNull : C6)).Direction = ParameterDirection.Input;
        //        _with11.Add("PARAM7_IN", (string.IsNullOrEmpty(Strings.Trim(C7)) ? strNull : C7)).Direction = ParameterDirection.Input;
        //        _with11.Add("PARAM8_IN", (string.IsNullOrEmpty(Strings.Trim(C8)) ? strNull : C8)).Direction = ParameterDirection.Input;
        //        _with11.Add("PARAM9_IN", (string.IsNullOrEmpty(Strings.Trim(C9)) ? strNull : C9)).Direction = ParameterDirection.Input;
        //        _with11.Add("PARAM10_IN", (string.IsNullOrEmpty(Strings.Trim(C10)) ? strNull : C10)).Direction = ParameterDirection.Input;
        //        _with11.Add("PARAM11_IN", (string.IsNullOrEmpty(Strings.Trim(C11)) ? strNull : C11)).Direction = ParameterDirection.Input;
        //        _with11.Add("PARAM12_IN", (string.IsNullOrEmpty(Strings.Trim(C12)) ? strNull : C12)).Direction = ParameterDirection.Input;
        //        _with11.Add("PARAM13_IN", (string.IsNullOrEmpty(Strings.Trim(C13)) ? strNull : C13)).Direction = ParameterDirection.Input;
        //        _with11.Add("PARAM14_IN", (string.IsNullOrEmpty(Strings.Trim(C14)) ? strNull : C14)).Direction = ParameterDirection.Input;
        //        _with11.Add("PARAM15_IN", (string.IsNullOrEmpty(Strings.Trim(C15)) ? strNull : C15)).Direction = ParameterDirection.Input;
        //        _with11.Add("PARAM16_IN", (string.IsNullOrEmpty(Strings.Trim(C16)) ? strNull : C16)).Direction = ParameterDirection.Input;
        //        _with11.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
        //        selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
        //        selectCommand.ExecuteNonQuery();
        //        OracleClob clob = default(OracleClob);
        //        clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
        //        System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
        //        strReturn = strReader.ReadToEnd();
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //    finally
        //    {
        //        selectCommand.Connection.Close();
        //    }
        //}

        //#endregion "Common Search Procedure for RatingandTariff Module"

        //#region "Common Search Procedure for Search CBJCReports Module"

        //public string getCommonSearchCBJCReports(string strSearchFlag, string strCondition, string strChk = "")
        //{
        //    return FetchCommonCBJCReports(strCondition, strChk);
        //}

        //public string FetchCommonCBJCReports(string strCond, string strChk = "")
        //{
        //    WorkFlow objWF = new WorkFlow();
        //    Oracle.ManagedDataAccess.Client.OracleCommand selectCommand = new Oracle.ManagedDataAccess.Client.OracleCommand();
        //    string strReturn = null;
        //    Array arr = null;
        //    string strSERACH_IN = null;
        //    string C = null;
        //    string C1 = null;
        //    string C3 = null;
        //    string C4 = null;
        //    string C5 = null;
        //    string C6 = null;
        //    string C7 = null;
        //    string C8 = null;
        //    string C9 = null;
        //    string C10 = null;
        //    string C11 = null;
        //    string C12 = null;
        //    string C13 = null;
        //    string C14 = null;
        //    string C15 = null;
        //    string C16 = null;
        //    string C17 = null;
        //    string C18 = null;
        //    string C19 = null;
        //    string C20 = null;
        //    string User_pk = null;
        //    string strSearchFlag_IN = null;
        //    string strFromToType = "";
        //    dynamic strNull = "";
        //    string SelectedLoc = null;
        //    string strLOOKUP_IN = null;
        //    arr = strCond.Split('~');
        //    strLOOKUP_IN = arr(0);
        //    strSERACH_IN = arr(1);
        //    if (strSERACH_IN.IndexOf("$") != -1)
        //    {
        //        strSERACH_IN = strSERACH_IN.Replace("$", "&");
        //    }
        //    strSERACH_IN = strSERACH_IN.TrimEnd(',');
        //    User_pk = Convert.ToString(HttpContext.Current.Session["USER_PK"]);
        //    strSearchFlag_IN = arr(2);
        //    if (arr.Length > 3)
        //    {
        //        C3 = arr(3);
        //    }
        //    if (arr.Length > 4)
        //    {
        //        C4 = arr(4);
        //    }
        //    if (arr.Length > 5)
        //    {
        //        C5 = arr(5);
        //    }
        //    if (arr.Length > 6)
        //    {
        //        C6 = arr(6);
        //    }
        //    if (arr.Length > 7)
        //    {
        //        C7 = arr(7);
        //    }
        //    if (arr.Length > 8)
        //    {
        //        C8 = arr(8);
        //    }
        //    if (arr.Length > 9)
        //    {
        //        C9 = arr(9);
        //    }
        //    if (arr.Length > 10)
        //    {
        //        C10 = arr(10);
        //    }
        //    if (arr.Length > 11)
        //    {
        //        C11 = arr(11);
        //    }
        //    if (arr.Length > 12)
        //    {
        //        C12 = arr(12);
        //    }
        //    if (arr.Length > 13)
        //    {
        //        C13 = arr(13);
        //    }
        //    if (arr.Length > 14)
        //    {
        //        C14 = arr(14);
        //    }
        //    if (arr.Length > 15)
        //    {
        //        C15 = arr(15);
        //    }
        //    if (arr.Length > 16)
        //    {
        //        C16 = arr(16);
        //    }
        //    if (arr.Length > 17)
        //    {
        //        C17 = arr(17);
        //    }
        //    if (arr.Length > 18)
        //    {
        //        C18 = arr(18);
        //    }
        //    if (arr.Length > 19)
        //    {
        //        C19 = arr(19);
        //    }
        //    if (arr.Length > 20)
        //    {
        //        C20 = arr(20);
        //    }
        //    try
        //    {
        //        objWF.OpenConnection();
        //        selectCommand.Connection = objWF.MyConnection;
        //        selectCommand.CommandType = CommandType.StoredProcedure;
        //        selectCommand.CommandText = objWF.MyUserName + ".GET_EN_CBJCREPORTS_PKG.GET_CBJC_REPORTS";

        //        var _with12 = selectCommand.Parameters;
        //        _with12.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
        //        _with12.Add("LOOKUP_VALUE_IN", strLOOKUP_IN).Direction = ParameterDirection.Input;
        //        _with12.Add("SearchFlag", (string.IsNullOrEmpty(strSearchFlag_IN) ? strNull : strSearchFlag_IN)).Direction = ParameterDirection.Input;
        //        _with12.Add("USER_PK_IN", (string.IsNullOrEmpty(User_pk) ? strNull : User_pk)).Direction = ParameterDirection.Input;
        //        _with12.Add("PARAM3_IN", (string.IsNullOrEmpty(Strings.Trim(C3)) ? strNull : C3)).Direction = ParameterDirection.Input;
        //        _with12.Add("PARAM4_IN", (string.IsNullOrEmpty(Strings.Trim(C4)) ? strNull : C4)).Direction = ParameterDirection.Input;
        //        _with12.Add("PARAM5_IN", (string.IsNullOrEmpty(Strings.Trim(C5)) ? strNull : C5)).Direction = ParameterDirection.Input;
        //        _with12.Add("PARAM6_IN", (string.IsNullOrEmpty(Strings.Trim(C6)) ? strNull : C6)).Direction = ParameterDirection.Input;
        //        _with12.Add("PARAM7_IN", (string.IsNullOrEmpty(Strings.Trim(C7)) ? strNull : C7)).Direction = ParameterDirection.Input;
        //        _with12.Add("PARAM8_IN", (string.IsNullOrEmpty(Strings.Trim(C8)) ? strNull : C8)).Direction = ParameterDirection.Input;
        //        _with12.Add("PARAM9_IN", (string.IsNullOrEmpty(Strings.Trim(C9)) ? strNull : C9)).Direction = ParameterDirection.Input;
        //        _with12.Add("PARAM10_IN", (string.IsNullOrEmpty(Strings.Trim(C10)) ? strNull : C10)).Direction = ParameterDirection.Input;
        //        _with12.Add("PARAM11_IN", (string.IsNullOrEmpty(Strings.Trim(C11)) ? strNull : C11)).Direction = ParameterDirection.Input;
        //        _with12.Add("PARAM12_IN", (string.IsNullOrEmpty(Strings.Trim(C12)) ? strNull : C12)).Direction = ParameterDirection.Input;
        //        _with12.Add("PARAM13_IN", (string.IsNullOrEmpty(Strings.Trim(C13)) ? strNull : C13)).Direction = ParameterDirection.Input;
        //        _with12.Add("PARAM14_IN", (string.IsNullOrEmpty(Strings.Trim(C14)) ? strNull : C14)).Direction = ParameterDirection.Input;
        //        _with12.Add("PARAM15_IN", (string.IsNullOrEmpty(Strings.Trim(C15)) ? strNull : C15)).Direction = ParameterDirection.Input;
        //        _with12.Add("PARAM16_IN", (string.IsNullOrEmpty(Strings.Trim(C16)) ? strNull : C16)).Direction = ParameterDirection.Input;
        //        _with12.Add("PARAM17_IN", (string.IsNullOrEmpty(Strings.Trim(C17)) ? strNull : C17)).Direction = ParameterDirection.Input;
        //        _with12.Add("PARAM18_IN", (string.IsNullOrEmpty(Strings.Trim(C18)) ? strNull : C18)).Direction = ParameterDirection.Input;
        //        _with12.Add("PARAM19_IN", (string.IsNullOrEmpty(Strings.Trim(C19)) ? strNull : C19)).Direction = ParameterDirection.Input;
        //        _with12.Add("PARAM20_IN", (string.IsNullOrEmpty(Strings.Trim(C20)) ? strNull : C20)).Direction = ParameterDirection.Input;
        //        _with12.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
        //        selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
        //        selectCommand.ExecuteNonQuery();
        //        OracleClob clob = default(OracleClob);
        //        clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
        //        System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
        //        strReturn = strReader.ReadToEnd();
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //    finally
        //    {
        //        selectCommand.Connection.Close();
        //    }
        //}

        //#endregion "Common Search Procedure for Search CBJCReports Module"

        //#region "Common Search Procedure for Search CBJCReports Module"

        //public string getCommonSearchEDIReports(string strSearchFlag, string strCondition, string strChk = "")
        //{
        //    return FetchCommonEDIReports(strCondition, strChk);
        //}

        //public string FetchCommonEDIReports(string strCond, string strChk = "")
        //{
        //    WorkFlow objWF = new WorkFlow();
        //    Oracle.ManagedDataAccess.Client.OracleCommand selectCommand = new Oracle.ManagedDataAccess.Client.OracleCommand();
        //    string strReturn = null;
        //    Array arr = null;
        //    string strSERACH_IN = null;
        //    string C = null;
        //    string C1 = null;
        //    string C3 = null;
        //    string C4 = null;
        //    string C5 = null;
        //    string C6 = null;
        //    string C7 = null;
        //    string C8 = null;
        //    string C9 = null;
        //    string C10 = null;
        //    string C11 = null;
        //    string C12 = null;
        //    string C13 = null;
        //    string C14 = null;
        //    string C15 = null;
        //    string C16 = null;
        //    string C17 = null;
        //    string C18 = null;
        //    string C19 = null;
        //    string C20 = null;
        //    string User_pk = null;
        //    string strSearchFlag_IN = null;
        //    string strFromToType = "";
        //    dynamic strNull = "";
        //    string SelectedLoc = null;
        //    string strLOOKUP_IN = null;
        //    arr = strCond.Split('~');
        //    strLOOKUP_IN = arr[0];
        //    strSERACH_IN = arr(1);
        //    if (strSERACH_IN.IndexOf("$") != -1)
        //    {
        //        strSERACH_IN = strSERACH_IN.Replace("$", "&");
        //    }
        //    strSERACH_IN = strSERACH_IN.TrimEnd(',');
        //    User_pk = Convert.ToString(HttpContext.Current.Session["USER_PK"]);
        //    strSearchFlag_IN = arr(2);
        //    if (arr.Length > 3)
        //    {
        //        C3 = arr(3);
        //    }
        //    if (arr.Length > 4)
        //    {
        //        C4 = arr(4);
        //    }
        //    if (arr.Length > 5)
        //    {
        //        C5 = arr(5);
        //    }
        //    if (arr.Length > 6)
        //    {
        //        C6 = arr(6);
        //    }
        //    if (arr.Length > 7)
        //    {
        //        C7 = arr(7);
        //    }
        //    if (arr.Length > 8)
        //    {
        //        C8 = arr(8);
        //    }
        //    if (arr.Length > 9)
        //    {
        //        C9 = arr(9);
        //    }
        //    if (arr.Length > 10)
        //    {
        //        C10 = arr(10);
        //    }
        //    if (arr.Length > 11)
        //    {
        //        C11 = arr(11);
        //    }
        //    if (arr.Length > 12)
        //    {
        //        C12 = arr(12);
        //    }
        //    if (arr.Length > 13)
        //    {
        //        C13 = arr(13);
        //    }
        //    if (arr.Length > 14)
        //    {
        //        C14 = arr(14);
        //    }
        //    if (arr.Length > 15)
        //    {
        //        C15 = arr(15);
        //    }
        //    if (arr.Length > 16)
        //    {
        //        C16 = arr(16);
        //    }
        //    if (arr.Length > 17)
        //    {
        //        C17 = arr(17);
        //    }
        //    if (arr.Length > 18)
        //    {
        //        C18 = arr(18);
        //    }
        //    if (arr.Length > 19)
        //    {
        //        C19 = arr(19);
        //    }
        //    if (arr.Length > 20)
        //    {
        //        C20 = arr(20);
        //    }
        //    try
        //    {
        //        objWF.OpenConnection();
        //        selectCommand.Connection = objWF.MyConnection;
        //        selectCommand.CommandType = CommandType.StoredProcedure;
        //        selectCommand.CommandText = objWF.MyUserName + ".GET_EN_COMMON_EDI_PKG.GET_EDI_REPORTS";

        //        var _with13 = selectCommand.Parameters;
        //        _with13.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
        //        _with13.Add("LOOKUP_VALUE_IN", strLOOKUP_IN).Direction = ParameterDirection.Input;
        //        _with13.Add("SearchFlag", (string.IsNullOrEmpty(strSearchFlag_IN) ? strNull : strSearchFlag_IN)).Direction = ParameterDirection.Input;
        //        _with13.Add("USER_PK_IN", (string.IsNullOrEmpty(User_pk) ? strNull : User_pk)).Direction = ParameterDirection.Input;
        //        _with13.Add("PARAM3_IN", (string.IsNullOrEmpty(Strings.Trim(C3)) ? strNull : C3)).Direction = ParameterDirection.Input;
        //        _with13.Add("PARAM4_IN", (string.IsNullOrEmpty(Strings.Trim(C4)) ? strNull : C4)).Direction = ParameterDirection.Input;
        //        _with13.Add("PARAM5_IN", (string.IsNullOrEmpty(Strings.Trim(C5)) ? strNull : C5)).Direction = ParameterDirection.Input;
        //        _with13.Add("PARAM6_IN", (string.IsNullOrEmpty(Strings.Trim(C6)) ? strNull : C6)).Direction = ParameterDirection.Input;
        //        _with13.Add("PARAM7_IN", (string.IsNullOrEmpty(Strings.Trim(C7)) ? strNull : C7)).Direction = ParameterDirection.Input;
        //        _with13.Add("PARAM8_IN", (string.IsNullOrEmpty(Strings.Trim(C8)) ? strNull : C8)).Direction = ParameterDirection.Input;
        //        _with13.Add("PARAM9_IN", (string.IsNullOrEmpty(Strings.Trim(C9)) ? strNull : C9)).Direction = ParameterDirection.Input;
        //        _with13.Add("PARAM10_IN", (string.IsNullOrEmpty(Strings.Trim(C10)) ? strNull : C10)).Direction = ParameterDirection.Input;
        //        _with13.Add("PARAM11_IN", (string.IsNullOrEmpty(Strings.Trim(C11)) ? strNull : C11)).Direction = ParameterDirection.Input;
        //        _with13.Add("PARAM12_IN", (string.IsNullOrEmpty(Strings.Trim(C12)) ? strNull : C12)).Direction = ParameterDirection.Input;
        //        _with13.Add("PARAM13_IN", (string.IsNullOrEmpty(Strings.Trim(C13)) ? strNull : C13)).Direction = ParameterDirection.Input;
        //        _with13.Add("PARAM14_IN", (string.IsNullOrEmpty(Strings.Trim(C14)) ? strNull : C14)).Direction = ParameterDirection.Input;
        //        _with13.Add("PARAM15_IN", (string.IsNullOrEmpty(Strings.Trim(C15)) ? strNull : C15)).Direction = ParameterDirection.Input;
        //        _with13.Add("PARAM16_IN", (string.IsNullOrEmpty(Strings.Trim(C16)) ? strNull : C16)).Direction = ParameterDirection.Input;
        //        _with13.Add("PARAM17_IN", (string.IsNullOrEmpty(Strings.Trim(C17)) ? strNull : C17)).Direction = ParameterDirection.Input;
        //        _with13.Add("PARAM18_IN", (string.IsNullOrEmpty(Strings.Trim(C18)) ? strNull : C18)).Direction = ParameterDirection.Input;
        //        _with13.Add("PARAM19_IN", (string.IsNullOrEmpty(Strings.Trim(C19)) ? strNull : C19)).Direction = ParameterDirection.Input;
        //        _with13.Add("PARAM20_IN", (string.IsNullOrEmpty(Strings.Trim(C20)) ? strNull : C20)).Direction = ParameterDirection.Input;
        //        _with13.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
        //        selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
        //        selectCommand.ExecuteNonQuery();
        //        OracleClob clob = default(OracleClob);
        //        clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
        //        System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
        //        strReturn = strReader.ReadToEnd();
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //    finally
        //    {
        //        selectCommand.Connection.Close();
        //    }
        //}

        //#endregion "Common Search Procedure for Search CBJCReports Module"

        //#region "Common Search Procedure for MIS Module"

        //public string getCommonSearchMIS(string strSearchFlag, string strCondition, string strChk = "")
        //{
        //    return FetchCommonData_MIS(strCondition, strChk);
        //}

        //public string FetchCommonData_MIS(string strCond, string strChk = "")
        //{
        //    WorkFlow objWF = new WorkFlow();
        //    Oracle.ManagedDataAccess.Client.OracleCommand selectCommand = new Oracle.ManagedDataAccess.Client.OracleCommand();
        //    string strReturn = null;
        //    Array arr = null;
        //    string strSERACH_IN = null;
        //    string C = null;
        //    string C1 = null;
        //    string C3 = null;
        //    string C4 = null;
        //    string C5 = null;
        //    string C6 = null;
        //    string C7 = null;
        //    string C8 = null;
        //    string C9 = null;
        //    string C10 = null;
        //    string C11 = null;
        //    string C12 = null;
        //    string C13 = null;
        //    string C14 = null;
        //    string C15 = null;
        //    string C16 = null;
        //    string C17 = null;
        //    string C18 = null;
        //    string C19 = null;
        //    string C20 = null;
        //    string C21 = null;
        //    string C22 = null;
        //    string C23 = null;
        //    string User_pk = null;
        //    string strSearchFlag_IN = null;
        //    string strFromToType = "";
        //    dynamic strNull = "";
        //    string SelectedLoc = null;
        //    string strLOOKUP_IN = null;
        //    arr = strCond.Split('~');
        //    // lOOK UP VALUE E OR L
        //    strLOOKUP_IN = arr(0);
        //    //USER TYPE TEXT WHILE PRESSING THE KEY
        //    strSERACH_IN = arr(1);
        //    if (strSERACH_IN.IndexOf("$") != -1)
        //    {
        //        strSERACH_IN = strSERACH_IN.Replace("$", "&");
        //    }
        //    strSERACH_IN = strSERACH_IN.TrimEnd(',');
        //    User_pk = Convert.ToString(HttpContext.Current.Session["USER_PK"]);
        //    //FLAG FOR ES FOR WHICH FETCHING THE RECORDS
        //    strSearchFlag_IN = arr(2);
        //    if (arr.Length > 3)
        //    {
        //        C3 = arr(3);
        //    }
        //    if (arr.Length > 4)
        //    {
        //        C4 = arr(4);
        //    }
        //    if (arr.Length > 5)
        //    {
        //        C5 = arr(5);
        //    }
        //    if (arr.Length > 6)
        //    {
        //        C6 = arr(6);
        //    }
        //    if (arr.Length > 7)
        //    {
        //        C7 = arr(7);
        //    }
        //    if (arr.Length > 8)
        //    {
        //        C8 = arr(8);
        //    }
        //    if (arr.Length > 9)
        //    {
        //        C9 = arr(9);
        //    }
        //    if (arr.Length > 10)
        //    {
        //        C10 = arr(10);
        //    }
        //    if (arr.Length > 11)
        //    {
        //        C11 = arr(11);
        //    }
        //    if (arr.Length > 12)
        //    {
        //        C12 = arr(12);
        //    }
        //    if (arr.Length > 13)
        //    {
        //        C13 = arr(13);
        //    }
        //    if (arr.Length > 14)
        //    {
        //        C14 = arr(14);
        //    }
        //    if (arr.Length > 15)
        //    {
        //        C15 = arr(15);
        //    }
        //    if (arr.Length > 16)
        //    {
        //        C16 = arr(16);
        //    }
        //    if (arr.Length > 17)
        //    {
        //        C17 = arr(17);
        //    }
        //    if (arr.Length > 18)
        //    {
        //        C18 = arr(18);
        //    }
        //    if (arr.Length > 19)
        //    {
        //        C19 = arr(19);
        //    }
        //    if (arr.Length > 20)
        //    {
        //        C20 = arr(20);
        //    }
        //    if (arr.Length > 21)
        //    {
        //        C21 = arr(21);
        //    }
        //    if (arr.Length > 22)
        //    {
        //        C22 = arr(22);
        //    }
        //    if (arr.Length > 23)
        //    {
        //        C23 = arr(23);
        //    }
        //    try
        //    {
        //        objWF.OpenConnection();
        //        selectCommand.Connection = objWF.MyConnection;
        //        selectCommand.CommandType = CommandType.StoredProcedure;
        //        selectCommand.CommandText = objWF.MyUserName + ".GET_EN_COMMON_MIS_PKG.GET_COMMON_MIS";

        //        var _with14 = selectCommand.Parameters;
        //        _with14.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
        //        _with14.Add("LOOKUP_VALUE_IN", strLOOKUP_IN).Direction = ParameterDirection.Input;
        //        _with14.Add("SearchFlag", (string.IsNullOrEmpty(strSearchFlag_IN) ? strNull : strSearchFlag_IN)).Direction = ParameterDirection.Input;
        //        _with14.Add("USER_PK_IN", (string.IsNullOrEmpty(User_pk) ? strNull : User_pk)).Direction = ParameterDirection.Input;
        //        _with14.Add("PARAM3_IN", (string.IsNullOrEmpty(Strings.Trim(C3)) ? strNull : C3)).Direction = ParameterDirection.Input;
        //        _with14.Add("PARAM4_IN", (string.IsNullOrEmpty(Strings.Trim(C4)) ? strNull : C4)).Direction = ParameterDirection.Input;
        //        _with14.Add("PARAM5_IN", (string.IsNullOrEmpty(Strings.Trim(C5)) ? strNull : C5)).Direction = ParameterDirection.Input;
        //        _with14.Add("PARAM6_IN", (string.IsNullOrEmpty(Strings.Trim(C6)) ? strNull : C6)).Direction = ParameterDirection.Input;
        //        _with14.Add("PARAM7_IN", (string.IsNullOrEmpty(Strings.Trim(C7)) ? strNull : C7)).Direction = ParameterDirection.Input;
        //        _with14.Add("PARAM8_IN", (string.IsNullOrEmpty(Strings.Trim(C8)) ? strNull : C8)).Direction = ParameterDirection.Input;
        //        _with14.Add("PARAM9_IN", (string.IsNullOrEmpty(Strings.Trim(C9)) ? strNull : C9)).Direction = ParameterDirection.Input;
        //        _with14.Add("PARAM10_IN", (string.IsNullOrEmpty(Strings.Trim(C10)) ? strNull : C10)).Direction = ParameterDirection.Input;
        //        _with14.Add("PARAM11_IN", (string.IsNullOrEmpty(Strings.Trim(C11)) ? strNull : C11)).Direction = ParameterDirection.Input;
        //        _with14.Add("PARAM12_IN", (string.IsNullOrEmpty(Strings.Trim(C12)) ? strNull : C12)).Direction = ParameterDirection.Input;
        //        _with14.Add("PARAM13_IN", (string.IsNullOrEmpty(Strings.Trim(C13)) ? strNull : C13)).Direction = ParameterDirection.Input;
        //        _with14.Add("PARAM14_IN", (string.IsNullOrEmpty(Strings.Trim(C14)) ? strNull : C14)).Direction = ParameterDirection.Input;
        //        _with14.Add("PARAM15_IN", (string.IsNullOrEmpty(Strings.Trim(C15)) ? strNull : C15)).Direction = ParameterDirection.Input;
        //        _with14.Add("PARAM16_IN", (string.IsNullOrEmpty(Strings.Trim(C16)) ? strNull : C16)).Direction = ParameterDirection.Input;
        //        _with14.Add("PARAM17_IN", (string.IsNullOrEmpty(Strings.Trim(C17)) ? strNull : C17)).Direction = ParameterDirection.Input;
        //        _with14.Add("PARAM18_IN", (string.IsNullOrEmpty(Strings.Trim(C18)) ? strNull : C18)).Direction = ParameterDirection.Input;
        //        _with14.Add("PARAM19_IN", (string.IsNullOrEmpty(Strings.Trim(C19)) ? strNull : C19)).Direction = ParameterDirection.Input;
        //        _with14.Add("PARAM20_IN", (string.IsNullOrEmpty(Strings.Trim(C20)) ? strNull : C20)).Direction = ParameterDirection.Input;
        //        _with14.Add("PARAM21_IN", (string.IsNullOrEmpty(Strings.Trim(C21)) ? strNull : C21)).Direction = ParameterDirection.Input;
        //        _with14.Add("PARAM22_IN", (string.IsNullOrEmpty(Strings.Trim(C22)) ? strNull : C22)).Direction = ParameterDirection.Input;
        //        _with14.Add("PARAM23_IN", (string.IsNullOrEmpty(Strings.Trim(C23)) ? strNull : C23)).Direction = ParameterDirection.Input;
        //        _with14.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
        //        selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
        //        selectCommand.ExecuteNonQuery();
        //        OracleClob clob = default(OracleClob);
        //        clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
        //        System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
        //        strReturn = strReader.ReadToEnd();
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //    finally
        //    {
        //        selectCommand.Connection.Close();
        //    }
        //}

        //#endregion "Common Search Procedure for MIS Module"

        //#region "Common Search Procedure for SUPPLIR MGMT Module"

        //public string getCommonSearchSUPPLIERMGMT(string strSearchFlag, string strCondition, string strChk = "")
        //{
        //    return FetchCommonData_SUPPLIERMGMT(strCondition, strChk);
        //}

        //public string FetchCommonData_SUPPLIERMGMT(string strCond, string strChk = "")
        //{
        //    WorkFlow objWF = new WorkFlow();
        //    Oracle.ManagedDataAccess.Client.OracleCommand selectCommand = new Oracle.ManagedDataAccess.Client.OracleCommand();
        //    string strReturn = null;
        //    Array arr = null;
        //    string strSERACH_IN = null;
        //    string C = null;
        //    string C1 = null;
        //    string C3 = null;
        //    string C4 = null;
        //    string C5 = null;
        //    string C6 = null;
        //    string C7 = null;
        //    string C8 = null;
        //    string C9 = null;
        //    string C10 = null;
        //    string C11 = null;
        //    string C12 = null;
        //    string C13 = null;
        //    string C14 = null;
        //    string C15 = null;
        //    string C16 = null;
        //    string User_pk = null;
        //    string strSearchFlag_IN = null;
        //    string strFromToType = "";
        //    dynamic strNull = "";
        //    string SelectedLoc = null;
        //    string strLOOKUP_IN = null;
        //    arr = strCond.Split('~');
        //    // lOOK UP VALUE E OR L
        //    strLOOKUP_IN = arr(0);
        //    //USER TYPE TEXT WHILE PRESSING THE KEY
        //    strSERACH_IN = arr(1);
        //    if (strSERACH_IN.IndexOf("$") != -1)
        //    {
        //        strSERACH_IN = strSERACH_IN.Replace("$", "&");
        //    }
        //    strSERACH_IN = strSERACH_IN.TrimEnd(',');
        //    User_pk = Convert.ToString(HttpContext.Current.Session["USER_PK"]);
        //    //FLAG FOR ES FOR WHICH FETCHING THE RECORDS
        //    strSearchFlag_IN = arr(2);
        //    if (arr.Length > 3)
        //    {
        //        C3 = arr(3);
        //    }
        //    if (arr.Length > 4)
        //    {
        //        C4 = arr(4);
        //    }
        //    if (arr.Length > 5)
        //    {
        //        C5 = arr(5);
        //    }
        //    if (arr.Length > 6)
        //    {
        //        C6 = arr(6);
        //    }
        //    if (arr.Length > 7)
        //    {
        //        C7 = arr(7);
        //    }
        //    if (arr.Length > 8)
        //    {
        //        C8 = arr(8);
        //    }
        //    if (arr.Length > 9)
        //    {
        //        C9 = arr(9);
        //    }
        //    if (arr.Length > 10)
        //    {
        //        C10 = arr(10);
        //    }
        //    if (arr.Length > 11)
        //    {
        //        C11 = arr(11);
        //    }
        //    if (arr.Length > 12)
        //    {
        //        C12 = arr(12);
        //    }
        //    if (arr.Length > 13)
        //    {
        //        C13 = arr(13);
        //    }
        //    if (arr.Length > 14)
        //    {
        //        C14 = arr(14);
        //    }
        //    if (arr.Length > 15)
        //    {
        //        C15 = arr(15);
        //    }
        //    if (arr.Length > 16)
        //    {
        //        C16 = arr(16);
        //    }

        //    try
        //    {
        //        objWF.OpenConnection();
        //        selectCommand.Connection = objWF.MyConnection;
        //        selectCommand.CommandType = CommandType.StoredProcedure;
        //        selectCommand.CommandText = objWF.MyUserName + ".GET_EN_COMMON_SUPPLIERMGMT_PKG.GET_COMMON_SUPPLIERMGMT";

        //        var _with15 = selectCommand.Parameters;
        //        _with15.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
        //        _with15.Add("LOOKUP_VALUE_IN", strLOOKUP_IN).Direction = ParameterDirection.Input;
        //        _with15.Add("SearchFlag", (string.IsNullOrEmpty(strSearchFlag_IN) ? strNull : strSearchFlag_IN)).Direction = ParameterDirection.Input;
        //        _with15.Add("USER_PK_IN", (string.IsNullOrEmpty(User_pk) ? strNull : User_pk)).Direction = ParameterDirection.Input;
        //        _with15.Add("PARAM3_IN", (string.IsNullOrEmpty(Strings.Trim(C3)) ? strNull : C3)).Direction = ParameterDirection.Input;
        //        _with15.Add("PARAM4_IN", (string.IsNullOrEmpty(Strings.Trim(C4)) ? strNull : C4)).Direction = ParameterDirection.Input;
        //        _with15.Add("PARAM5_IN", (string.IsNullOrEmpty(Strings.Trim(C5)) ? strNull : C5)).Direction = ParameterDirection.Input;
        //        _with15.Add("PARAM6_IN", (string.IsNullOrEmpty(Strings.Trim(C6)) ? strNull : C6)).Direction = ParameterDirection.Input;
        //        _with15.Add("PARAM7_IN", (string.IsNullOrEmpty(Strings.Trim(C7)) ? strNull : C7)).Direction = ParameterDirection.Input;
        //        _with15.Add("PARAM8_IN", (string.IsNullOrEmpty(Strings.Trim(C8)) ? strNull : C8)).Direction = ParameterDirection.Input;
        //        _with15.Add("PARAM9_IN", (string.IsNullOrEmpty(Strings.Trim(C9)) ? strNull : C9)).Direction = ParameterDirection.Input;
        //        _with15.Add("PARAM10_IN", (string.IsNullOrEmpty(Strings.Trim(C10)) ? strNull : C10)).Direction = ParameterDirection.Input;
        //        _with15.Add("PARAM11_IN", (string.IsNullOrEmpty(Strings.Trim(C11)) ? strNull : C11)).Direction = ParameterDirection.Input;
        //        _with15.Add("PARAM12_IN", (string.IsNullOrEmpty(Strings.Trim(C12)) ? strNull : C12)).Direction = ParameterDirection.Input;
        //        _with15.Add("PARAM13_IN", (string.IsNullOrEmpty(Strings.Trim(C13)) ? strNull : C13)).Direction = ParameterDirection.Input;
        //        _with15.Add("PARAM14_IN", (string.IsNullOrEmpty(Strings.Trim(C14)) ? strNull : C14)).Direction = ParameterDirection.Input;
        //        _with15.Add("PARAM15_IN", (string.IsNullOrEmpty(Strings.Trim(C15)) ? strNull : C15)).Direction = ParameterDirection.Input;
        //        _with15.Add("PARAM16_IN", (string.IsNullOrEmpty(Strings.Trim(C16)) ? strNull : C16)).Direction = ParameterDirection.Input;
        //        _with15.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
        //        selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
        //        selectCommand.ExecuteNonQuery();
        //        OracleClob clob = default(OracleClob);
        //        clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
        //        System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
        //        strReturn = strReader.ReadToEnd();
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //    finally
        //    {
        //        selectCommand.Connection.Close();
        //    }
        //}

        //#endregion "Common Search Procedure for SUPPLIR MGMT Module"

        //#region "Common Search Procedure for Receivable Module"

        //public string getCommonSearchReceivables(string strSearchFlag, string strCondition, string strChk = "")
        //{
        //    return FetchCommonData_Receivables(strCondition, strChk);
        //}

        //public string FetchCommonData_Receivables(string strCond, string strChk = "")
        //{
        //    WorkFlow objWF = new WorkFlow();
        //    Oracle.ManagedDataAccess.Client.OracleCommand selectCommand = new Oracle.ManagedDataAccess.Client.OracleCommand();
        //    string strReturn = null;
        //    Array arr = null;
        //    string strSERACH_IN = null;
        //    string C = null;
        //    string C1 = null;
        //    string C3 = null;
        //    string C4 = null;
        //    string C5 = null;
        //    string C6 = null;
        //    string C7 = null;
        //    string C8 = null;
        //    string C9 = null;
        //    string C10 = null;
        //    string C11 = null;
        //    string C12 = null;
        //    string C13 = null;
        //    string C14 = null;
        //    string C15 = null;
        //    string C16 = null;
        //    string User_pk = null;
        //    string strSearchFlag_IN = null;
        //    string strFromToType = "";
        //    dynamic strNull = "";
        //    string SelectedLoc = null;
        //    string strLOOKUP_IN = null;
        //    arr = strCond.Split('~');
        //    // lOOK UP VALUE E OR L
        //    strLOOKUP_IN = arr(0);
        //    //USER TYPE TEXT WHILE PRESSING THE KEY
        //    strSERACH_IN = arr(1);
        //    if (strSERACH_IN.IndexOf("$") != -1)
        //    {
        //        strSERACH_IN = strSERACH_IN.Replace("$", "&");
        //    }
        //    strSERACH_IN = strSERACH_IN.TrimEnd(',');
        //    User_pk = Convert.ToString(HttpContext.Current.Session["USER_PK"]);
        //    //FLAG FOR ES FOR WHICH FETCHING THE RECORDS
        //    strSearchFlag_IN = arr(2);
        //    if (arr.Length > 3)
        //    {
        //        C3 = arr(3);
        //    }
        //    if (arr.Length > 4)
        //    {
        //        C4 = arr(4);
        //    }
        //    if (arr.Length > 5)
        //    {
        //        C5 = arr(5);
        //    }
        //    if (arr.Length > 6)
        //    {
        //        C6 = arr(6);
        //    }
        //    if (arr.Length > 7)
        //    {
        //        C7 = arr(7);
        //    }
        //    if (arr.Length > 8)
        //    {
        //        C8 = arr(8);
        //    }
        //    if (arr.Length > 9)
        //    {
        //        C9 = arr(9);
        //    }
        //    if (arr.Length > 10)
        //    {
        //        C10 = arr(10);
        //    }
        //    if (arr.Length > 11)
        //    {
        //        C11 = arr(11);
        //    }
        //    if (arr.Length > 12)
        //    {
        //        C12 = arr(12);
        //    }
        //    if (arr.Length > 13)
        //    {
        //        C13 = arr(13);
        //    }
        //    if (arr.Length > 14)
        //    {
        //        C14 = arr(14);
        //    }
        //    if (arr.Length > 15)
        //    {
        //        C15 = arr(15);
        //    }
        //    if (arr.Length > 16)
        //    {
        //        C16 = arr(16);
        //    }

        //    try
        //    {
        //        objWF.OpenConnection();
        //        selectCommand.Connection = objWF.MyConnection;
        //        selectCommand.CommandType = CommandType.StoredProcedure;
        //        selectCommand.CommandText = objWF.MyUserName + ".GET_EN_COMMON_RECEIVABLES_PKG.GET_RECEIVABLES_ES";

        //        var _with16 = selectCommand.Parameters;
        //        _with16.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
        //        _with16.Add("LOOKUP_VALUE_IN", strLOOKUP_IN).Direction = ParameterDirection.Input;
        //        _with16.Add("SearchFlag", (string.IsNullOrEmpty(strSearchFlag_IN) ? strNull : strSearchFlag_IN)).Direction = ParameterDirection.Input;
        //        _with16.Add("USER_PK_IN", (string.IsNullOrEmpty(User_pk) ? strNull : User_pk)).Direction = ParameterDirection.Input;
        //        //.Add("PARAM3_IN", IIf(Trim(C3) = "", strNull, C3)).Direction = ParameterDirection.Input
        //        //.Add("PARAM4_IN", IIf(Trim(C4) = "", strNull, C4)).Direction = ParameterDirection.Input
        //        //.Add("PARAM5_IN", IIf(Trim(C5) = "", strNull, C5)).Direction = ParameterDirection.Input
        //        //.Add("PARAM6_IN", IIf(Trim(C6) = "", strNull, C6)).Direction = ParameterDirection.Input
        //        //.Add("PARAM7_IN", IIf(Trim(C7) = "", strNull, C7)).Direction = ParameterDirection.Input
        //        //.Add("PARAM8_IN", IIf(Trim(C8) = "", strNull, C8)).Direction = ParameterDirection.Input
        //        //.Add("PARAM9_IN", IIf(Trim(C9) = "", strNull, C9)).Direction = ParameterDirection.Input
        //        //.Add("PARAM10_IN", IIf(Trim(C10) = "", strNull, C10)).Direction = ParameterDirection.Input
        //        //.Add("PARAM11_IN", IIf(Trim(C11) = "", strNull, C11)).Direction = ParameterDirection.Input
        //        //.Add("PARAM12_IN", IIf(Trim(C12) = "", strNull, C12)).Direction = ParameterDirection.Input
        //        //.Add("PARAM13_IN", IIf(Trim(C13) = "", strNull, C13)).Direction = ParameterDirection.Input
        //        //.Add("PARAM14_IN", IIf(Trim(C14) = "", strNull, C14)).Direction = ParameterDirection.Input
        //        //.Add("PARAM15_IN", IIf(Trim(C15) = "", strNull, C15)).Direction = ParameterDirection.Input
        //        //.Add("PARAM16_IN", IIf(Trim(C16) = "", strNull, C16)).Direction = ParameterDirection.Input
        //        for (int _counter = 3; _counter <= 30; _counter++)
        //        {
        //            string _paramName = "PARAM" + _counter.ToString() + "_IN";
        //            string _paramValue = "";
        //            if (arr.Length > _counter)
        //            {
        //                _paramValue = arr(_counter);
        //                _with16.Add(_paramName, (string.IsNullOrEmpty(_paramValue.Trim()) ? strNull : _paramValue)).Direction = ParameterDirection.Input;
        //            }
        //        }
        //        _with16.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
        //        selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
        //        selectCommand.ExecuteNonQuery();
        //        OracleClob clob = default(OracleClob);
        //        clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
        //        System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
        //        strReturn = strReader.ReadToEnd();
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //    finally
        //    {
        //        selectCommand.Connection.Close();
        //    }
        //}

        //#endregion "Common Search Procedure for Receivable Module"

        //#region "Common Search Procedure for Payables Module"

        //public string getCommonSearchPayables(string strSearchFlag, string strCondition, string strChk = "")
        //{
        //    return FetchCommonData_Payables(strCondition, strChk);
        //}

        //public string FetchCommonData_Payables(string strCond, string strChk = "")
        //{
        //    WorkFlow objWF = new WorkFlow();
        //    Oracle.ManagedDataAccess.Client.OracleCommand selectCommand = new Oracle.ManagedDataAccess.Client.OracleCommand();
        //    string strReturn = null;
        //    Array arr = null;
        //    string strSERACH_IN = null;
        //    string C = null;
        //    string C1 = null;
        //    string C3 = null;
        //    string C4 = null;
        //    string C5 = null;
        //    string C6 = null;
        //    string C7 = null;
        //    string C8 = null;
        //    string C9 = null;
        //    string C10 = null;
        //    string C11 = null;
        //    string C12 = null;
        //    string C13 = null;
        //    string C14 = null;
        //    string C15 = null;
        //    string C16 = null;
        //    string User_pk = null;
        //    string strSearchFlag_IN = null;
        //    string strFromToType = "";
        //    dynamic strNull = "";
        //    string SelectedLoc = null;
        //    string strLOOKUP_IN = null;
        //    arr = strCond.Split('~');
        //    // lOOK UP VALUE E OR L
        //    strLOOKUP_IN = arr(0);
        //    //USER TYPE TEXT WHILE PRESSING THE KEY
        //    strSERACH_IN = arr(1);
        //    if (strSERACH_IN.IndexOf("$") != -1)
        //    {
        //        strSERACH_IN = strSERACH_IN.Replace("$", "&");
        //    }
        //    strSERACH_IN = strSERACH_IN.TrimEnd(',');
        //    User_pk = Convert.ToString(HttpContext.Current.Session["USER_PK"]);
        //    //FLAG FOR ES FOR WHICH FETCHING THE RECORDS
        //    strSearchFlag_IN = arr(2);
        //    if (arr.Length > 3)
        //    {
        //        C3 = arr(3);
        //    }
        //    if (arr.Length > 4)
        //    {
        //        C4 = arr(4);
        //    }
        //    if (arr.Length > 5)
        //    {
        //        C5 = arr(5);
        //    }
        //    if (arr.Length > 6)
        //    {
        //        C6 = arr(6);
        //    }
        //    if (arr.Length > 7)
        //    {
        //        C7 = arr(7);
        //    }
        //    if (arr.Length > 8)
        //    {
        //        C8 = arr(8);
        //    }
        //    if (arr.Length > 9)
        //    {
        //        C9 = arr(9);
        //    }
        //    if (arr.Length > 10)
        //    {
        //        C10 = arr(10);
        //    }
        //    if (arr.Length > 11)
        //    {
        //        C11 = arr(11);
        //    }
        //    if (arr.Length > 12)
        //    {
        //        C12 = arr(12);
        //    }
        //    if (arr.Length > 13)
        //    {
        //        C13 = arr(13);
        //    }
        //    if (arr.Length > 14)
        //    {
        //        C14 = arr(14);
        //    }
        //    if (arr.Length > 15)
        //    {
        //        C15 = arr(15);
        //    }
        //    if (arr.Length > 16)
        //    {
        //        C16 = arr(16);
        //    }

        //    try
        //    {
        //        objWF.OpenConnection();
        //        selectCommand.Connection = objWF.MyConnection;
        //        selectCommand.CommandType = CommandType.StoredProcedure;
        //        selectCommand.CommandText = objWF.MyUserName + ".GET_EN_COMMON_PAYABLES_PKG.GET_PAYABLES_ES";

        //        var _with17 = selectCommand.Parameters;
        //        _with17.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
        //        _with17.Add("LOOKUP_VALUE_IN", strLOOKUP_IN).Direction = ParameterDirection.Input;
        //        _with17.Add("SearchFlag", (string.IsNullOrEmpty(strSearchFlag_IN) ? strNull : strSearchFlag_IN)).Direction = ParameterDirection.Input;
        //        _with17.Add("USER_PK_IN", (string.IsNullOrEmpty(User_pk) ? strNull : User_pk)).Direction = ParameterDirection.Input;
        //        _with17.Add("PARAM3_IN", (string.IsNullOrEmpty(Strings.Trim(C3)) ? strNull : C3)).Direction = ParameterDirection.Input;
        //        _with17.Add("PARAM4_IN", (string.IsNullOrEmpty(Strings.Trim(C4)) ? strNull : C4)).Direction = ParameterDirection.Input;
        //        _with17.Add("PARAM5_IN", (string.IsNullOrEmpty(Strings.Trim(C5)) ? strNull : C5)).Direction = ParameterDirection.Input;
        //        _with17.Add("PARAM6_IN", (string.IsNullOrEmpty(Strings.Trim(C6)) ? strNull : C6)).Direction = ParameterDirection.Input;
        //        _with17.Add("PARAM7_IN", (string.IsNullOrEmpty(Strings.Trim(C7)) ? strNull : C7)).Direction = ParameterDirection.Input;
        //        _with17.Add("PARAM8_IN", (string.IsNullOrEmpty(Strings.Trim(C8)) ? strNull : C8)).Direction = ParameterDirection.Input;
        //        _with17.Add("PARAM9_IN", (string.IsNullOrEmpty(Strings.Trim(C9)) ? strNull : C9)).Direction = ParameterDirection.Input;
        //        _with17.Add("PARAM10_IN", (string.IsNullOrEmpty(Strings.Trim(C10)) ? strNull : C10)).Direction = ParameterDirection.Input;
        //        _with17.Add("PARAM11_IN", (string.IsNullOrEmpty(Strings.Trim(C11)) ? strNull : C11)).Direction = ParameterDirection.Input;
        //        _with17.Add("PARAM12_IN", (string.IsNullOrEmpty(Strings.Trim(C12)) ? strNull : C12)).Direction = ParameterDirection.Input;
        //        _with17.Add("PARAM13_IN", (string.IsNullOrEmpty(Strings.Trim(C13)) ? strNull : C13)).Direction = ParameterDirection.Input;
        //        _with17.Add("PARAM14_IN", (string.IsNullOrEmpty(Strings.Trim(C14)) ? strNull : C14)).Direction = ParameterDirection.Input;
        //        _with17.Add("PARAM15_IN", (string.IsNullOrEmpty(Strings.Trim(C15)) ? strNull : C15)).Direction = ParameterDirection.Input;
        //        _with17.Add("PARAM16_IN", (string.IsNullOrEmpty(Strings.Trim(C16)) ? strNull : C16)).Direction = ParameterDirection.Input;
        //        _with17.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
        //        selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
        //        selectCommand.ExecuteNonQuery();
        //        OracleClob clob = default(OracleClob);
        //        clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
        //        System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
        //        strReturn = strReader.ReadToEnd();
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //    finally
        //    {
        //        selectCommand.Connection.Close();
        //    }
        //}

        //#endregion "Common Search Procedure for Payables Module"

        //#region "Common Search Procedure for Print Export Docs Module"

        //public string getCommonSearchPrintExpDocs(string strSearchFlag, string strCondition, string strChk = "")
        //{
        //    return FetchCommonData_PrintExpDocs(strCondition, strChk);
        //}

        //public string FetchCommonData_PrintExpDocs(string strCond, string strChk = "")
        //{
        //    WorkFlow objWF = new WorkFlow();
        //    Oracle.ManagedDataAccess.Client.OracleCommand selectCommand = new Oracle.ManagedDataAccess.Client.OracleCommand();
        //    string strReturn = null;
        //    Array arr = null;
        //    string strSERACH_IN = null;
        //    string C = null;
        //    string C1 = null;
        //    string C3 = null;
        //    string C4 = null;
        //    string C5 = null;
        //    string C6 = null;
        //    string C7 = null;
        //    string C8 = null;
        //    string C9 = null;
        //    string C10 = null;
        //    string C11 = null;
        //    string C12 = null;
        //    string C13 = null;
        //    string C14 = null;
        //    string C15 = null;
        //    string C16 = null;
        //    string User_pk = null;
        //    string strSearchFlag_IN = null;
        //    string strFromToType = "";
        //    dynamic strNull = "";
        //    string SelectedLoc = null;
        //    string strLOOKUP_IN = null;
        //    arr = strCond.Split('~');
        //    // lOOK UP VALUE E OR L
        //    strLOOKUP_IN = arr(0);
        //    //USER TYPE TEXT WHILE PRESSING THE KEY
        //    strSERACH_IN = arr(1);
        //    if (strSERACH_IN.IndexOf("$") != -1)
        //    {
        //        strSERACH_IN = strSERACH_IN.Replace("$", "&");
        //    }
        //    strSERACH_IN = strSERACH_IN.TrimEnd(',');
        //    User_pk = Convert.ToString(HttpContext.Current.Session["USER_PK"]);
        //    //FLAG FOR ES FOR WHICH FETCHING THE RECORDS
        //    strSearchFlag_IN = arr(2);
        //    if (arr.Length > 3)
        //    {
        //        C3 = arr(3);
        //    }
        //    if (arr.Length > 4)
        //    {
        //        C4 = arr(4);
        //    }
        //    if (arr.Length > 5)
        //    {
        //        C5 = arr(5);
        //    }
        //    if (arr.Length > 6)
        //    {
        //        C6 = arr(6);
        //    }
        //    if (arr.Length > 7)
        //    {
        //        C7 = arr(7);
        //    }
        //    if (arr.Length > 8)
        //    {
        //        C8 = arr(8);
        //    }
        //    if (arr.Length > 9)
        //    {
        //        C9 = arr(9);
        //    }
        //    if (arr.Length > 10)
        //    {
        //        C10 = arr(10);
        //    }
        //    if (arr.Length > 11)
        //    {
        //        C11 = arr(11);
        //    }
        //    if (arr.Length > 12)
        //    {
        //        C12 = arr(12);
        //    }
        //    if (arr.Length > 13)
        //    {
        //        C13 = arr(13);
        //    }
        //    if (arr.Length > 14)
        //    {
        //        C14 = arr(14);
        //    }
        //    if (arr.Length > 15)
        //    {
        //        C15 = arr(15);
        //    }
        //    if (arr.Length > 16)
        //    {
        //        C16 = arr(16);
        //    }

        //    try
        //    {
        //        objWF.OpenConnection();
        //        selectCommand.Connection = objWF.MyConnection;
        //        selectCommand.CommandType = CommandType.StoredProcedure;
        //        selectCommand.CommandText = objWF.MyUserName + ".GET_EN_COMMON_PAYABLES_PKG.GET_PAYABLES_ES";

        //        var _with18 = selectCommand.Parameters;
        //        _with18.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
        //        _with18.Add("LOOKUP_VALUE_IN", strLOOKUP_IN).Direction = ParameterDirection.Input;
        //        _with18.Add("SearchFlag", (string.IsNullOrEmpty(strSearchFlag_IN) ? strNull : strSearchFlag_IN)).Direction = ParameterDirection.Input;
        //        _with18.Add("USER_PK_IN", (string.IsNullOrEmpty(User_pk) ? strNull : User_pk)).Direction = ParameterDirection.Input;
        //        _with18.Add("PARAM3_IN", (string.IsNullOrEmpty(Strings.Trim(C3)) ? strNull : C3)).Direction = ParameterDirection.Input;
        //        _with18.Add("PARAM4_IN", (string.IsNullOrEmpty(Strings.Trim(C4)) ? strNull : C4)).Direction = ParameterDirection.Input;
        //        _with18.Add("PARAM5_IN", (string.IsNullOrEmpty(Strings.Trim(C5)) ? strNull : C5)).Direction = ParameterDirection.Input;
        //        _with18.Add("PARAM6_IN", (string.IsNullOrEmpty(Strings.Trim(C6)) ? strNull : C6)).Direction = ParameterDirection.Input;
        //        _with18.Add("PARAM7_IN", (string.IsNullOrEmpty(Strings.Trim(C7)) ? strNull : C7)).Direction = ParameterDirection.Input;
        //        _with18.Add("PARAM8_IN", (string.IsNullOrEmpty(Strings.Trim(C8)) ? strNull : C8)).Direction = ParameterDirection.Input;
        //        _with18.Add("PARAM9_IN", (string.IsNullOrEmpty(Strings.Trim(C9)) ? strNull : C9)).Direction = ParameterDirection.Input;
        //        _with18.Add("PARAM10_IN", (string.IsNullOrEmpty(Strings.Trim(C10)) ? strNull : C10)).Direction = ParameterDirection.Input;
        //        _with18.Add("PARAM11_IN", (string.IsNullOrEmpty(Strings.Trim(C11)) ? strNull : C11)).Direction = ParameterDirection.Input;
        //        _with18.Add("PARAM12_IN", (string.IsNullOrEmpty(Strings.Trim(C12)) ? strNull : C12)).Direction = ParameterDirection.Input;
        //        _with18.Add("PARAM13_IN", (string.IsNullOrEmpty(Strings.Trim(C13)) ? strNull : C13)).Direction = ParameterDirection.Input;
        //        _with18.Add("PARAM14_IN", (string.IsNullOrEmpty(Strings.Trim(C14)) ? strNull : C14)).Direction = ParameterDirection.Input;
        //        _with18.Add("PARAM15_IN", (string.IsNullOrEmpty(Strings.Trim(C15)) ? strNull : C15)).Direction = ParameterDirection.Input;
        //        _with18.Add("PARAM16_IN", (string.IsNullOrEmpty(Strings.Trim(C16)) ? strNull : C16)).Direction = ParameterDirection.Input;
        //        _with18.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
        //        selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
        //        selectCommand.ExecuteNonQuery();
        //        OracleClob clob = default(OracleClob);
        //        clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
        //        System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
        //        strReturn = strReader.ReadToEnd();
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //    finally
        //    {
        //        selectCommand.Connection.Close();
        //    }
        //}

        //#endregion "Common Search Procedure for Print Export Docs Module"

        //#region "Common Search Procedure for Print Import Docs Module"

        //public string getCommonSearchPrintImpDocs(string strSearchFlag, string strCondition, string strChk = "")
        //{
        //    return FetchCommonData_PrintImpDocs(strCondition, strChk);
        //}

        //public string FetchCommonData_PrintImpDocs(string strCond, string strChk = "")
        //{
        //    WorkFlow objWF = new WorkFlow();
        //    Oracle.ManagedDataAccess.Client.OracleCommand selectCommand = new Oracle.ManagedDataAccess.Client.OracleCommand();
        //    string strReturn = null;
        //    Array arr = null;
        //    string strSERACH_IN = null;
        //    string C = null;
        //    string C1 = null;
        //    string C3 = null;
        //    string C4 = null;
        //    string C5 = null;
        //    string C6 = null;
        //    string C7 = null;
        //    string C8 = null;
        //    string C9 = null;
        //    string C10 = null;
        //    string C11 = null;
        //    string C12 = null;
        //    string C13 = null;
        //    string C14 = null;
        //    string C15 = null;
        //    string C16 = null;
        //    string User_pk = null;
        //    string strSearchFlag_IN = null;
        //    string strFromToType = "";
        //    dynamic strNull = "";
        //    string SelectedLoc = null;
        //    string strLOOKUP_IN = null;
        //    arr = strCond.Split('~');
        //    // lOOK UP VALUE E OR L
        //    strLOOKUP_IN = arr(0);
        //    //USER TYPE TEXT WHILE PRESSING THE KEY
        //    strSERACH_IN = arr(1);
        //    if (strSERACH_IN.IndexOf("$") != -1)
        //    {
        //        strSERACH_IN = strSERACH_IN.Replace("$", "&");
        //    }
        //    strSERACH_IN = strSERACH_IN.TrimEnd(',');
        //    User_pk = Convert.ToString(HttpContext.Current.Session["USER_PK"]);
        //    //FLAG FOR ES FOR WHICH FETCHING THE RECORDS
        //    strSearchFlag_IN = arr(2);
        //    if (arr.Length > 3)
        //    {
        //        C3 = arr(3);
        //    }
        //    if (arr.Length > 4)
        //    {
        //        C4 = arr(4);
        //    }
        //    if (arr.Length > 5)
        //    {
        //        C5 = arr(5);
        //    }
        //    if (arr.Length > 6)
        //    {
        //        C6 = arr(6);
        //    }
        //    if (arr.Length > 7)
        //    {
        //        C7 = arr(7);
        //    }
        //    if (arr.Length > 8)
        //    {
        //        C8 = arr(8);
        //    }
        //    if (arr.Length > 9)
        //    {
        //        C9 = arr(9);
        //    }
        //    if (arr.Length > 10)
        //    {
        //        C10 = arr(10);
        //    }
        //    if (arr.Length > 11)
        //    {
        //        C11 = arr(11);
        //    }
        //    if (arr.Length > 12)
        //    {
        //        C12 = arr(12);
        //    }
        //    if (arr.Length > 13)
        //    {
        //        C13 = arr(13);
        //    }
        //    if (arr.Length > 14)
        //    {
        //        C14 = arr(14);
        //    }
        //    if (arr.Length > 15)
        //    {
        //        C15 = arr(15);
        //    }
        //    if (arr.Length > 16)
        //    {
        //        C16 = arr(16);
        //    }

        //    try
        //    {
        //        objWF.OpenConnection();
        //        selectCommand.Connection = objWF.MyConnection;
        //        selectCommand.CommandType = CommandType.StoredProcedure;
        //        selectCommand.CommandText = objWF.MyUserName + ".GET_EN_COMMON_PAYABLES_PKG.GET_PAYABLES_ES";

        //        var _with19 = selectCommand.Parameters;
        //        _with19.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
        //        _with19.Add("LOOKUP_VALUE_IN", strLOOKUP_IN).Direction = ParameterDirection.Input;
        //        _with19.Add("SearchFlag", (string.IsNullOrEmpty(strSearchFlag_IN) ? strNull : strSearchFlag_IN)).Direction = ParameterDirection.Input;
        //        _with19.Add("USER_PK_IN", (string.IsNullOrEmpty(User_pk) ? strNull : User_pk)).Direction = ParameterDirection.Input;
        //        _with19.Add("PARAM3_IN", (string.IsNullOrEmpty(Strings.Trim(C3)) ? strNull : C3)).Direction = ParameterDirection.Input;
        //        _with19.Add("PARAM4_IN", (string.IsNullOrEmpty(Strings.Trim(C4)) ? strNull : C4)).Direction = ParameterDirection.Input;
        //        _with19.Add("PARAM5_IN", (string.IsNullOrEmpty(Strings.Trim(C5)) ? strNull : C5)).Direction = ParameterDirection.Input;
        //        _with19.Add("PARAM6_IN", (string.IsNullOrEmpty(Strings.Trim(C6)) ? strNull : C6)).Direction = ParameterDirection.Input;
        //        _with19.Add("PARAM7_IN", (string.IsNullOrEmpty(Strings.Trim(C7)) ? strNull : C7)).Direction = ParameterDirection.Input;
        //        _with19.Add("PARAM8_IN", (string.IsNullOrEmpty(Strings.Trim(C8)) ? strNull : C8)).Direction = ParameterDirection.Input;
        //        _with19.Add("PARAM9_IN", (string.IsNullOrEmpty(Strings.Trim(C9)) ? strNull : C9)).Direction = ParameterDirection.Input;
        //        _with19.Add("PARAM10_IN", (string.IsNullOrEmpty(Strings.Trim(C10)) ? strNull : C10)).Direction = ParameterDirection.Input;
        //        _with19.Add("PARAM11_IN", (string.IsNullOrEmpty(Strings.Trim(C11)) ? strNull : C11)).Direction = ParameterDirection.Input;
        //        _with19.Add("PARAM12_IN", (string.IsNullOrEmpty(Strings.Trim(C12)) ? strNull : C12)).Direction = ParameterDirection.Input;
        //        _with19.Add("PARAM13_IN", (string.IsNullOrEmpty(Strings.Trim(C13)) ? strNull : C13)).Direction = ParameterDirection.Input;
        //        _with19.Add("PARAM14_IN", (string.IsNullOrEmpty(Strings.Trim(C14)) ? strNull : C14)).Direction = ParameterDirection.Input;
        //        _with19.Add("PARAM15_IN", (string.IsNullOrEmpty(Strings.Trim(C15)) ? strNull : C15)).Direction = ParameterDirection.Input;
        //        _with19.Add("PARAM16_IN", (string.IsNullOrEmpty(Strings.Trim(C16)) ? strNull : C16)).Direction = ParameterDirection.Input;
        //        _with19.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
        //        selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
        //        selectCommand.ExecuteNonQuery();
        //        OracleClob clob = default(OracleClob);
        //        clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
        //        System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
        //        strReturn = strReader.ReadToEnd();
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //    finally
        //    {
        //        selectCommand.Connection.Close();
        //    }
        //}

        //#endregion "Common Search Procedure for Print Import Docs Module"

        //#region "Common Search Procedure for Order Mgmt Module"

        //public string getCommonSearchOrderMgmt(string strSearchFlag, string strCondition, string strChk = "")
        //{
        //    return FetchCommonData_OrderMgmt(strCondition, strChk);
        //}

        //public string FetchCommonData_OrderMgmt(string strCond, string strChk = "")
        //{
        //    WorkFlow objWF = new WorkFlow();
        //    Oracle.ManagedDataAccess.Client.OracleCommand selectCommand = new Oracle.ManagedDataAccess.Client.OracleCommand();
        //    string strReturn = null;
        //    Array arr = null;
        //    string strSERACH_IN = null;
        //    string C = null;
        //    string C1 = null;
        //    string C3 = null;
        //    string C4 = null;
        //    string C5 = null;
        //    string C6 = null;
        //    string C7 = null;
        //    string C8 = null;
        //    string C9 = null;
        //    string C10 = null;
        //    string C11 = null;
        //    string C12 = null;
        //    string C13 = null;
        //    string C14 = null;
        //    string C15 = null;
        //    string C16 = null;
        //    string C17 = null;
        //    string C18 = null;
        //    string C19 = null;
        //    string C20 = null;
        //    string C21 = null;
        //    string C22 = null;
        //    string C23 = null;
        //    string C24 = null;
        //    string C25 = null;
        //    string User_pk = null;
        //    string strSearchFlag_IN = null;
        //    string strFromToType = "";
        //    dynamic strNull = "";
        //    string SelectedLoc = null;
        //    string strLOOKUP_IN = null;
        //    arr = strCond.Split('~');
        //    // lOOK UP VALUE E OR L
        //    strLOOKUP_IN = arr(0);
        //    //USER TYPE TEXT WHILE PRESSING THE KEY
        //    strSERACH_IN = arr(1);
        //    if (strSERACH_IN.IndexOf("$") != -1)
        //    {
        //        strSERACH_IN = strSERACH_IN.Replace("$", "&");
        //    }
        //    strSERACH_IN = strSERACH_IN.TrimEnd(',');
        //    User_pk = Convert.ToString(HttpContext.Current.Session["USER_PK"]);
        //    //FLAG FOR ES FOR WHICH FETCHING THE RECORDS
        //    strSearchFlag_IN = arr(2);
        //    if (arr.Length > 3)
        //    {
        //        C3 = arr(3);
        //    }
        //    if (arr.Length > 4)
        //    {
        //        C4 = arr(4);
        //    }
        //    if (arr.Length > 5)
        //    {
        //        C5 = arr(5);
        //    }
        //    if (arr.Length > 6)
        //    {
        //        C6 = arr(6);
        //    }
        //    if (arr.Length > 7)
        //    {
        //        C7 = arr(7);
        //    }
        //    if (arr.Length > 8)
        //    {
        //        C8 = arr(8);
        //    }
        //    if (arr.Length > 9)
        //    {
        //        C9 = arr(9);
        //    }
        //    if (arr.Length > 10)
        //    {
        //        C10 = arr(10);
        //    }
        //    if (arr.Length > 11)
        //    {
        //        C11 = arr(11);
        //    }
        //    if (arr.Length > 12)
        //    {
        //        C12 = arr(12);
        //    }
        //    if (arr.Length > 13)
        //    {
        //        C13 = arr(13);
        //    }
        //    if (arr.Length > 14)
        //    {
        //        C14 = arr(14);
        //    }
        //    if (arr.Length > 15)
        //    {
        //        C15 = arr(15);
        //    }
        //    if (arr.Length > 16)
        //    {
        //        C16 = arr(16);
        //    }
        //    if (arr.Length > 17)
        //    {
        //        C17 = arr(17);
        //    }
        //    if (arr.Length > 18)
        //    {
        //        C18 = arr(18);
        //    }
        //    if (arr.Length > 19)
        //    {
        //        C19 = arr(19);
        //    }
        //    if (arr.Length > 20)
        //    {
        //        C20 = arr(20);
        //    }
        //    if (arr.Length > 21)
        //    {
        //        C21 = arr(21);
        //    }
        //    if (arr.Length > 22)
        //    {
        //        C22 = arr(22);
        //    }
        //    if (arr.Length > 23)
        //    {
        //        C23 = arr(23);
        //    }
        //    if (arr.Length > 24)
        //    {
        //        C24 = arr(24);
        //    }
        //    if (arr.Length > 25)
        //    {
        //        C25 = arr(25);
        //    }
        //    try
        //    {
        //        objWF.OpenConnection();
        //        selectCommand.Connection = objWF.MyConnection;
        //        selectCommand.CommandType = CommandType.StoredProcedure;
        //        selectCommand.CommandText = objWF.MyUserName + ".GET_EN_COMMON_ORDERMGMT_PKG.GET_ORDERMGMT_ES";

        //        var _with20 = selectCommand.Parameters;
        //        _with20.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
        //        _with20.Add("LOOKUP_VALUE_IN", strLOOKUP_IN).Direction = ParameterDirection.Input;
        //        _with20.Add("SearchFlag", (string.IsNullOrEmpty(strSearchFlag_IN) ? strNull : strSearchFlag_IN)).Direction = ParameterDirection.Input;
        //        _with20.Add("USER_PK_IN", (string.IsNullOrEmpty(User_pk) ? strNull : User_pk)).Direction = ParameterDirection.Input;
        //        _with20.Add("PARAM3_IN", (string.IsNullOrEmpty(Strings.Trim(C3)) ? strNull : C3)).Direction = ParameterDirection.Input;
        //        _with20.Add("PARAM4_IN", (string.IsNullOrEmpty(Strings.Trim(C4)) ? strNull : C4)).Direction = ParameterDirection.Input;
        //        _with20.Add("PARAM5_IN", (string.IsNullOrEmpty(Strings.Trim(C5)) ? strNull : C5)).Direction = ParameterDirection.Input;
        //        _with20.Add("PARAM6_IN", (string.IsNullOrEmpty(Strings.Trim(C6)) ? strNull : C6)).Direction = ParameterDirection.Input;
        //        _with20.Add("PARAM7_IN", (string.IsNullOrEmpty(Strings.Trim(C7)) ? strNull : C7)).Direction = ParameterDirection.Input;
        //        _with20.Add("PARAM8_IN", (string.IsNullOrEmpty(Strings.Trim(C8)) ? strNull : C8)).Direction = ParameterDirection.Input;
        //        _with20.Add("PARAM9_IN", (string.IsNullOrEmpty(Strings.Trim(C9)) ? strNull : C9)).Direction = ParameterDirection.Input;
        //        _with20.Add("PARAM10_IN", (string.IsNullOrEmpty(Strings.Trim(C10)) ? strNull : C10)).Direction = ParameterDirection.Input;
        //        _with20.Add("PARAM11_IN", (string.IsNullOrEmpty(Strings.Trim(C11)) ? strNull : C11)).Direction = ParameterDirection.Input;
        //        _with20.Add("PARAM12_IN", (string.IsNullOrEmpty(Strings.Trim(C12)) ? strNull : C12)).Direction = ParameterDirection.Input;
        //        _with20.Add("PARAM13_IN", (string.IsNullOrEmpty(Strings.Trim(C13)) ? strNull : C13)).Direction = ParameterDirection.Input;
        //        _with20.Add("PARAM14_IN", (string.IsNullOrEmpty(Strings.Trim(C14)) ? strNull : C14)).Direction = ParameterDirection.Input;
        //        _with20.Add("PARAM15_IN", (string.IsNullOrEmpty(Strings.Trim(C15)) ? strNull : C15)).Direction = ParameterDirection.Input;
        //        _with20.Add("PARAM16_IN", (string.IsNullOrEmpty(Strings.Trim(C16)) ? strNull : C16)).Direction = ParameterDirection.Input;
        //        _with20.Add("PARAM17_IN", (string.IsNullOrEmpty(Strings.Trim(C17)) ? strNull : C17)).Direction = ParameterDirection.Input;
        //        _with20.Add("PARAM18_IN", (string.IsNullOrEmpty(Strings.Trim(C18)) ? strNull : C18)).Direction = ParameterDirection.Input;
        //        _with20.Add("PARAM19_IN", (string.IsNullOrEmpty(Strings.Trim(C19)) ? strNull : C19)).Direction = ParameterDirection.Input;
        //        _with20.Add("PARAM20_IN", (string.IsNullOrEmpty(Strings.Trim(C20)) ? strNull : C20)).Direction = ParameterDirection.Input;
        //        _with20.Add("PARAM21_IN", (string.IsNullOrEmpty(Strings.Trim(C21)) ? strNull : C21)).Direction = ParameterDirection.Input;
        //        _with20.Add("PARAM22_IN", (string.IsNullOrEmpty(Strings.Trim(C22)) ? strNull : C22)).Direction = ParameterDirection.Input;
        //        _with20.Add("PARAM23_IN", (string.IsNullOrEmpty(Strings.Trim(C23)) ? strNull : C23)).Direction = ParameterDirection.Input;
        //        _with20.Add("PARAM24_IN", (string.IsNullOrEmpty(Strings.Trim(C24)) ? strNull : C24)).Direction = ParameterDirection.Input;
        //        _with20.Add("PARAM25_IN", (string.IsNullOrEmpty(Strings.Trim(C25)) ? strNull : C25)).Direction = ParameterDirection.Input;
        //        _with20.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
        //        selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
        //        selectCommand.ExecuteNonQuery();
        //        OracleClob clob = default(OracleClob);
        //        clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
        //        System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
        //        strReturn = strReader.ReadToEnd();
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //    finally
        //    {
        //        selectCommand.Connection.Close();
        //    }
        //}

        //#endregion "Common Search Procedure for Order Mgmt Module"

        //#region "Common Search Procedure for Reports Module"

        //public string getCommonSearchReports(string strSearchFlag, string strCondition, string strChk = "")
        //{
        //    return FetchCommonData_Reports(strCondition, strChk);
        //}

        //public string FetchCommonData_Reports(string strCond, string strChk = "")
        //{
        //    WorkFlow objWF = new WorkFlow();
        //    Oracle.ManagedDataAccess.Client.OracleCommand selectCommand = new Oracle.ManagedDataAccess.Client.OracleCommand();
        //    string strReturn = null;
        //    Array arr = null;
        //    string strSERACH_IN = null;
        //    string C = null;
        //    string C1 = null;
        //    string C3 = null;
        //    string C4 = null;
        //    string C5 = null;
        //    string C6 = null;
        //    string C7 = null;
        //    string C8 = null;
        //    string C9 = null;
        //    string C10 = null;
        //    string C11 = null;
        //    string C12 = null;
        //    string C13 = null;
        //    string C14 = null;
        //    string C15 = null;
        //    string C16 = null;
        //    string C17 = null;
        //    string User_pk = null;
        //    string strSearchFlag_IN = null;
        //    string strFromToType = "";
        //    dynamic strNull = "";
        //    string SelectedLoc = null;
        //    string strLOOKUP_IN = null;
        //    arr = strCond.Split('~');
        //    // lOOK UP VALUE E OR L
        //    strLOOKUP_IN = arr(0);
        //    //USER TYPE TEXT WHILE PRESSING THE KEY
        //    strSERACH_IN = arr(1);
        //    if (strSERACH_IN.IndexOf("$") != -1)
        //    {
        //        strSERACH_IN = strSERACH_IN.Replace("$", "&");
        //    }
        //    strSERACH_IN = strSERACH_IN.TrimEnd(',');
        //    User_pk = Convert.ToString(HttpContext.Current.Session["USER_PK"]);
        //    //FLAG FOR ES FOR WHICH FETCHING THE RECORDS
        //    strSearchFlag_IN = arr(2);
        //    if (arr.Length > 3)
        //    {
        //        C3 = arr(3);
        //    }
        //    if (arr.Length > 4)
        //    {
        //        C4 = arr(4);
        //    }
        //    if (arr.Length > 5)
        //    {
        //        C5 = arr(5);
        //    }
        //    if (arr.Length > 6)
        //    {
        //        C6 = arr(6);
        //    }
        //    if (arr.Length > 7)
        //    {
        //        C7 = arr(7);
        //    }
        //    if (arr.Length > 8)
        //    {
        //        C8 = arr(8);
        //    }
        //    if (arr.Length > 9)
        //    {
        //        C9 = arr(9);
        //    }
        //    if (arr.Length > 10)
        //    {
        //        C10 = arr(10);
        //    }
        //    if (arr.Length > 11)
        //    {
        //        C11 = arr(11);
        //    }
        //    if (arr.Length > 12)
        //    {
        //        C12 = arr(12);
        //    }
        //    if (arr.Length > 13)
        //    {
        //        C13 = arr(13);
        //    }
        //    if (arr.Length > 14)
        //    {
        //        C14 = arr(14);
        //    }
        //    if (arr.Length > 15)
        //    {
        //        C15 = arr(15);
        //    }
        //    if (arr.Length > 16)
        //    {
        //        C16 = arr(16);
        //    }
        //    if (arr.Length > 17)
        //    {
        //        C17 = arr(17);
        //    }
        //    try
        //    {
        //        objWF.OpenConnection();
        //        selectCommand.Connection = objWF.MyConnection;
        //        selectCommand.CommandType = CommandType.StoredProcedure;
        //        selectCommand.CommandText = objWF.MyUserName + ".GET_EN_COMMON_REPORTS_PKG.GET_REPORTS_ES";

        //        var _with21 = selectCommand.Parameters;
        //        _with21.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
        //        _with21.Add("LOOKUP_VALUE_IN", strLOOKUP_IN).Direction = ParameterDirection.Input;
        //        _with21.Add("SearchFlag", (string.IsNullOrEmpty(strSearchFlag_IN) ? strNull : strSearchFlag_IN)).Direction = ParameterDirection.Input;
        //        _with21.Add("USER_PK_IN", (string.IsNullOrEmpty(User_pk) ? strNull : User_pk)).Direction = ParameterDirection.Input;
        //        _with21.Add("PARAM3_IN", (string.IsNullOrEmpty(Strings.Trim(C3)) ? strNull : C3)).Direction = ParameterDirection.Input;
        //        _with21.Add("PARAM4_IN", (string.IsNullOrEmpty(Strings.Trim(C4)) ? strNull : C4)).Direction = ParameterDirection.Input;
        //        _with21.Add("PARAM5_IN", (string.IsNullOrEmpty(Strings.Trim(C5)) ? strNull : C5)).Direction = ParameterDirection.Input;
        //        _with21.Add("PARAM6_IN", (string.IsNullOrEmpty(Strings.Trim(C6)) ? strNull : C6)).Direction = ParameterDirection.Input;
        //        _with21.Add("PARAM7_IN", (string.IsNullOrEmpty(Strings.Trim(C7)) ? strNull : C7)).Direction = ParameterDirection.Input;
        //        _with21.Add("PARAM8_IN", (string.IsNullOrEmpty(Strings.Trim(C8)) ? strNull : C8)).Direction = ParameterDirection.Input;
        //        _with21.Add("PARAM9_IN", (string.IsNullOrEmpty(Strings.Trim(C9)) ? strNull : C9)).Direction = ParameterDirection.Input;
        //        _with21.Add("PARAM10_IN", (string.IsNullOrEmpty(Strings.Trim(C10)) ? strNull : C10)).Direction = ParameterDirection.Input;
        //        _with21.Add("PARAM11_IN", (string.IsNullOrEmpty(Strings.Trim(C11)) ? strNull : C11)).Direction = ParameterDirection.Input;
        //        _with21.Add("PARAM12_IN", (string.IsNullOrEmpty(Strings.Trim(C12)) ? strNull : C12)).Direction = ParameterDirection.Input;
        //        _with21.Add("PARAM13_IN", (string.IsNullOrEmpty(Strings.Trim(C13)) ? strNull : C13)).Direction = ParameterDirection.Input;
        //        _with21.Add("PARAM14_IN", (string.IsNullOrEmpty(Strings.Trim(C14)) ? strNull : C14)).Direction = ParameterDirection.Input;
        //        _with21.Add("PARAM15_IN", (string.IsNullOrEmpty(Strings.Trim(C15)) ? strNull : C15)).Direction = ParameterDirection.Input;
        //        _with21.Add("PARAM16_IN", (string.IsNullOrEmpty(Strings.Trim(C16)) ? strNull : C16)).Direction = ParameterDirection.Input;
        //        _with21.Add("PARAM17_IN", (string.IsNullOrEmpty(Strings.Trim(C17)) ? strNull : C17)).Direction = ParameterDirection.Input;
        //        _with21.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
        //        selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
        //        selectCommand.ExecuteNonQuery();
        //        OracleClob clob = default(OracleClob);
        //        clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
        //        System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
        //        strReturn = strReader.ReadToEnd();
        //        return strReturn;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //    finally
        //    {
        //        selectCommand.Connection.Close();
        //    }
        //}

        #endregion "Common Search Procedure for RatingandTariff Module"
    }
}