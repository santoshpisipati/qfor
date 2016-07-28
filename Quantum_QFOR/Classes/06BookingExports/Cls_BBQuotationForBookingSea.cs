#region "Comments"

//'***************************************************************************************************************
//'*  Company Name            :
//'*  Project Title           :    QFOR
//'***************************************************************************************************************
//'*  Created By              :    Santosh on 31-May-16
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

using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;
using System;
using System.Collections;
using System.Data;
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class Cls_BBQuotationForBookingSea : CommonFeatures
    {
        #region "variables"
        private long _PkValue;
        #endregion
        public Int32 errors = 0;

        #region " Enum and SRC function "

        private enum SourceType
        {
            DefaultValue = 0,
            SpotRate = 1,
            CustomerContract = 2,
            Quotation = 3,
            Enquiry = 4,
            OperatorTariff = 5,
            GeneralTariff = 6,
            Manual = 7
        }

        private string SRC(SourceType SType)
        {
            switch (SType)
            {
                case SourceType.DefaultValue:
                    return "'GenTar'";
                case SourceType.SpotRate:
                    return "'SpRate'";
                case SourceType.CustomerContract:
                    return "'Cont'";
                case SourceType.Quotation:
                    return "'Quote'";
                case SourceType.Enquiry:
                    return "'ENQ'";
                case SourceType.OperatorTariff:
                    return "'SLTar'";
                case SourceType.GeneralTariff:
                    return "'GenTar'";
                case SourceType.Manual:
                    return "'Manual'";
                default:
                    return "";
            }
        }

        private SourceType SourceEnum(string SType)
        {
            switch (SType)
            {
                case "2":
                    return SourceType.CustomerContract;
                case "1":
                    return SourceType.SpotRate;
                case "3":
                    return SourceType.Quotation;
                case "4":
                    return SourceType.Enquiry;
                case "5":
                    return SourceType.OperatorTariff;
                case "6":
                    return SourceType.GeneralTariff;
                case "7":
                    return SourceType.Manual;
                default:
                    return SourceType.DefaultValue;
            }
        }

        #endregion

        #region " Fetch UWG2 Entry grid. "
        #region "common variables"
        public Int32 cargo = 0;
        public Int32 cargotypes;
        #endregion
        public Int32 basis_cont = 0;

        #region " Enquiry Detail "
        // 4 DataBase Call
        private void GetEnqDetail(string EnqNo = "", string CargoType = "2", string CustomerPK = "", string CustomerID = "", string CustomerCategory = "", string AgentPK = "", string AgentID = "", string CommodityGroup = "", string SectorContainers = "", DataSet EnqDS = null,
        string QuoteNo = "", Int32 Version = 0, object QuotationStatus = null, DataTable OthDt = null, object ValidFor = null, object QuoteDate = null, Int16 CustomerType = 0, object ShipDate = null, Int32 CreditLimit = 0, Int32 CreditDays = 0,
        string remarks = "", string CargoMoveCode = "", object BaseCurrencyId = null, Int32 INCOTerms = 0, Int32 PYMTType = 0, int BBFlag = 0, string CommodityPks = "")
        {
            string strSQL = null;
            string strSQLFreight = null;
            if (!string.IsNullOrEmpty(QuoteNo))
            {
                strSQL = GetQuoteQuery(QuoteNo, CargoType, Version, QuotationStatus, OthDt, ValidFor, QuoteDate, ShipDate, CreditDays, CreditLimit,
                remarks, CargoMoveCode, BaseCurrencyId, INCOTerms, PYMTType);
                strSQLFreight = GetQuoteQueryFreights(QuoteNo, (cargo > 0 ? Convert.ToString(cargo) : CargoType));
            }
            else
            {
                strSQL = GetEnquiryQuery(EnqNo, CargoType);
                strSQLFreight = GetEnquiryQueryFreights(EnqNo, CargoType, BBFlag);
            }
            DataSet ds = null;
            WorkFlow objWF = new WorkFlow();
            DataTable DT = null;
            DataRow DR = null;
            try
            {
                EnqDS = objWF.GetDataSet(strSQL);
                EnqDS.Tables.Add(objWF.GetDataTable(strSQLFreight));
                DataRelation REL = null;
                if (Convert.ToInt32(CargoType) == 1 | cargo == 1)
                {
                    REL = new DataRelation("EnqRelation", new DataColumn[] {
                        EnqDS.Tables[0].Columns["REF_NO"],
                        EnqDS.Tables[0].Columns["POL_PK"],
                        EnqDS.Tables[0].Columns["POD_PK"],
                        EnqDS.Tables[0].Columns["CNTR_PK"]
                    }, new DataColumn[] {
                        EnqDS.Tables[1].Columns["REF_NO"],
                        EnqDS.Tables[1].Columns["POL_PK"],
                        EnqDS.Tables[1].Columns["POD_PK"],
                        EnqDS.Tables[1].Columns["CNTR_PK"]
                    });
                }
                else
                {
                    if (BBFlag == 1)
                    {
                        REL = new DataRelation("EnqRelation", new DataColumn[] {
                            EnqDS.Tables[0].Columns["REF_NO"],
                            EnqDS.Tables[0].Columns["POL_PK"],
                            EnqDS.Tables[0].Columns["POD_PK"],
                            EnqDS.Tables[0].Columns["LCL_BASIS"],
                            EnqDS.Tables[0].Columns["COMM_PK"]
                        }, new DataColumn[] {
                            EnqDS.Tables[1].Columns["REF_NO"],
                            EnqDS.Tables[1].Columns["POL_PK"],
                            EnqDS.Tables[1].Columns["POD_PK"],
                            EnqDS.Tables[1].Columns["LCLBASIS"],
                            EnqDS.Tables[1].Columns["COMM_PK"]
                        });
                    }
                    else
                    {
                        REL = new DataRelation("EnqRelation", new DataColumn[] {
                            EnqDS.Tables[0].Columns["REF_NO"],
                            EnqDS.Tables[0].Columns["POL_PK"],
                            EnqDS.Tables[0].Columns["POD_PK"],
                            EnqDS.Tables[0].Columns["LCL_BASIS"]
                        }, new DataColumn[] {
                            EnqDS.Tables[1].Columns["REF_NO"],
                            EnqDS.Tables[1].Columns["POL_PK"],
                            EnqDS.Tables[1].Columns["POD_PK"],
                            EnqDS.Tables[1].Columns["LCLBASIS"]
                        });
                    }
                }
                EnqDS.Relations.Add(REL);
                
                Int16 ColCnt = default(Int16);

                if (string.IsNullOrEmpty(Convert.ToString(EnqNo).Trim()) & string.IsNullOrEmpty(Convert.ToString(QuoteNo).Trim()))
                {
                    if (Convert.ToInt32(CargoType) == 1)
                    {
                        strSQL = " SELECT POL.PORT_MST_PK POLPK, POL.PORT_ID POLID, " +  " POD.PORT_MST_PK PODPK, POD.PORT_ID PODID, " +  " CNTR.CONTAINER_TYPE_MST_PK CNTPK, CNTR.CONTAINER_TYPE_MST_ID CNTID " +  " FROM PORT_MST_TBL POL, PORT_MST_TBL POD, CONTAINER_TYPE_MST_TBL CNTR " +  " WHERE (POL.PORT_MST_PK, POD.PORT_MST_PK, CNTR.CONTAINER_TYPE_MST_PK) " +  " IN (" + SectorContainers + ") Order By POLID,PODID,CNTID ";
                    }
                    else
                    {
                        if (BBFlag == 1)
                        {
                            strSQL = " SELECT POL.PORT_MST_PK POLPK, POL.PORT_ID POLID, " +  " POD.PORT_MST_PK PODPK, POD.PORT_ID PODID, " +  " DIM.DIMENTION_UNIT_MST_PK DIMPK, DIM.DIMENTION_ID DIMID,CMT.COMMODITY_MST_PK,CMT.COMMODITY_ID " +  " FROM PORT_MST_TBL POL, PORT_MST_TBL POD, DIMENTION_UNIT_MST_TBL DIM,COMMODITY_MST_TBL CMT " +  " WHERE CMT.COMMODITY_MST_PK IN (" + CommodityPks + ") " +  " AND (POL.PORT_MST_PK, POD.PORT_MST_PK, DIM.DIMENTION_UNIT_MST_PK ) " +  " IN (" + SectorContainers + ") Order By POLID,PODID,DIMID ";
                        }
                        else
                        {
                            strSQL = " SELECT POL.PORT_MST_PK POLPK, POL.PORT_ID POLID, " +  " POD.PORT_MST_PK PODPK, POD.PORT_ID PODID, " +  " DIM.DIMENTION_UNIT_MST_PK DIMPK, DIM.DIMENTION_ID DIMID " +  " FROM PORT_MST_TBL POL, PORT_MST_TBL POD, DIMENTION_UNIT_MST_TBL DIM " +  " WHERE (POL.PORT_MST_PK, POD.PORT_MST_PK, DIM.DIMENTION_UNIT_MST_PK ) " +  " IN (" + SectorContainers + ") Order By POLID,PODID,DIMID ";
                        }
                    }
                    DT = objWF.GetDataTable(strSQL);
                    for (int RowCnt = 0; RowCnt <= DT.Rows.Count - 1; RowCnt++)
                    {
                        var _with1 = DT.Rows[RowCnt];
                        DR = EnqDS.Tables[0].NewRow();
                        for (ColCnt = 0; ColCnt <= EnqDS.Tables[0].Columns.Count - 1; ColCnt++)
                        {
                            DR[ColCnt] = DBNull.Value;
                        }
                        if (Convert.ToInt32(CargoType) == 1)
                        {
                            // DR["PK") = 0 : DR["TYPE") = "" : DR["REF_NO") = "0" : DR["SHIP_DATE") = ""
                            DR["POL_PK"] = _with1["POLPK"];
                            DR["POL_ID"] = _with1["POLID"];
                            DR["POD_PK"] = _with1["PODPK"];
                            DR["POD_ID"] = _with1["PODID"];
                            // DR["OPER_PK") = "" : DR["OPER_ID") = "" : DR["OPER_NAME") = "" 
                            DR["CNTR_PK"] = _with1["CNTPK"];
                            DR["CNTR_ID"] = _with1["CNTID"];
                            // DR["QUANTITY") = "" : DR["COMM_PK") = "" : DR["COMM_ID") = ""
                            // DR["ALL_IN_TARIFF") = "" : DR["ALL_IN_QUOTE") = "" : DR["TARIFF") = "" : DR["NET") = ""
                            DR["CUSTOMER_PK"] = CustomerPK;
                            DR["CUSTOMER_CATPK"] = CustomerCategory;
                            DR["COMM_GRPPK"] = CommodityGroup;
                        }
                        else
                        {
                            DR["POL_PK"] = _with1["POLPK"];
                            DR["POL_ID"] = _with1["POLID"];
                            DR["POD_PK"] = _with1["PODPK"];
                            DR["POD_ID"] = _with1["PODID"];
                            if (BBFlag == 1)
                            {
                                DR["COMM_PK"] = _with1["COMMODITY_MST_PK"];
                                DR["COMM_ID"] = _with1["COMMODITY_ID"];
                            }
                            DR["LCL_BASIS"] = _with1["DIMPK"];
                            DR["DIMENTION_ID"] = _with1["DIMID"];
                            DR["CUSTOMER_PK"] = CustomerPK;
                            DR["CUSTOMER_CATPK"] = CustomerCategory;
                            DR["COMM_GRPPK"] = CommodityGroup;
                        }
                        EnqDS.Tables[0].Rows.Add(DR);
                    }
                }
                else
                {
                    SectorContainers = "";
                    for (int RowCnt = 0; RowCnt <= EnqDS.Tables[0].Rows.Count - 1; RowCnt++)
                    {
                        var _with2 = EnqDS.Tables[0].Rows[RowCnt];
                        if (Convert.ToInt32(CargoType) == 1 | cargo == 1)
                        {
                            SectorContainers += "(" + _with2["POL_PK"] + "," + _with2["POD_PK"] + "," + _with2["CNTR_PK"] + "),";
                        }
                        else
                        {
                            SectorContainers += "(" + _with2["POL_PK"] + "," + _with2["POD_PK"] + "," + _with2["LCL_BASIS"] + "),";
                        }
                    }
                    if (EnqDS.Tables[0].Rows.Count > 0)
                    {
                        SectorContainers = Convert.ToString(SectorContainers).TrimEnd(',');
                        CustomerPK = Convert.ToString(EnqDS.Tables[0].Rows[0]["CUSTOMER_PK"]);
                        CustomerCategory = Convert.ToString(EnqDS.Tables[0].Rows[0]["CUSTOMER_CATPK"]);
                        CommodityGroup = Convert.ToString(EnqDS.Tables[0].Rows[0]["COMM_GRPPK"]);
                        ShipDate = EnqDS.Tables[0].Rows[0]["SHIP_DATE"];
                        // Getting Customer ID
                        CustomerType = Convert.ToInt16(getDefault(EnqDS.Tables[0].Rows[0]["CUST_TYPE"], 0));
                        if (CustomerType == 0)
                        {
                            CustomerID = objWF.ExecuteScaler(" Select CUSTOMER_ID from CUSTOMER_MST_TBL where CUSTOMER_MST_PK = " + CustomerPK);
                        }
                        else
                        {
                            CustomerID = objWF.ExecuteScaler(" Select CUSTOMER_ID from TEMP_CUSTOMER_TBL where CUSTOMER_MST_PK = " + CustomerPK);
                        }
                    }
                }
                if (string.IsNullOrEmpty(Convert.ToString(QuoteNo).Trim()))
                    AddFreights(EnqDS, Convert.ToInt32(CargoType), BBFlag, CommodityPks);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception eX)
            {
                throw eX;
            }
        }

        #endregion

        #region " Add Freights "
        // 1 DataBase Call
        private void AddFreights(DataSet DS, int CargoType, int BBFlag = 0, string CommodityPks = "")
        {
            DataTable frtDt = null;
            string strSQL = null;
            //Added by rabbani on 24/3/07 ,To display BOF as first element in Freight Element Column using UNION ALL.
            //modified by thiyagarajan on 20/11/08 for location based currency task
            //Snigdharani - 30/12/2008 - & " AND FRM.BY_DEFAULT = 1 " & vbCrLf _ ,& " AND FRM.CHARGE_BASIS <> 2 " & vbCrLf _Commented
            try
            {
                if (BBFlag == 1)
                {
                    strSQL = " Select " +  " FRM.FREIGHT_ELEMENT_MST_PK, FRM.FREIGHT_ELEMENT_ID, FRM.FREIGHT_ELEMENT_NAME, " +  " CURR.CURRENCY_MST_PK, CURR.CURRENCY_ID,CMT.COMMODITY_MST_PK COMM_PK " +  " from FREIGHT_ELEMENT_MST_TBL FRM, CURRENCY_TYPE_MST_TBL CURR,COMMODITY_MST_TBL CMT " +  " where FRM.ACTIVE_FLAG = 1 " +  " AND CURR.CURRENCY_MST_PK = " + HttpContext.Current.Session["CURRENCY_MST_PK"] +  " AND CMT.COMMODITY_MST_PK IN (" + CommodityPks + ") " +  " AND FRM.BUSINESS_TYPE in (2,3) " +  " AND FRM.CHARGE_TYPE <> 3 " +  " ORDER BY FRM.PREFERENCE";
                }
                else
                {
                    strSQL = " Select " +  " FRM.FREIGHT_ELEMENT_MST_PK, FRM.FREIGHT_ELEMENT_ID, FRM.FREIGHT_ELEMENT_NAME, " +  " CURR.CURRENCY_MST_PK, CURR.CURRENCY_ID " +  " from FREIGHT_ELEMENT_MST_TBL FRM, CURRENCY_TYPE_MST_TBL CURR" +  " where FRM.ACTIVE_FLAG = 1 " +  " AND CURR.CURRENCY_MST_PK = " +HttpContext.Current.Session["CURRENCY_MST_PK"] +  " AND FRM.BUSINESS_TYPE in (2,3) " +  " AND FRM.CHARGE_TYPE <> 3 " +  " ORDER BY FRM.PREFERENCE";
                }

                //Change made by purnanand for freight element should come in preference order(09/01/08)

                //& " AND FRM.FREIGHT_ELEMENT_MST_PK IN (SELECT PARM.FRT_BOF_FK  FROM PARAMETERS_TBL PARM) " & vbCrLf _
                // & " UNION ALL " & vbCrLf _
                //& " Select " & vbCrLf _
                //& " FRM.FREIGHT_ELEMENT_MST_PK, FRM.FREIGHT_ELEMENT_ID, FRM.FREIGHT_ELEMENT_NAME, " & vbCrLf _
                //& " CURR.CURRENCY_MST_PK, CURR.CURRENCY_ID " & vbCrLf _
                //& " from FREIGHT_ELEMENT_MST_TBL FRM, CURRENCY_TYPE_MST_TBL CURR " & vbCrLf _
                //& " where FRM.ACTIVE_FLAG = 1 " & vbCrLf _
                //& " AND CURR.CURRENCY_MST_PK = (Select CURRENCY_MST_FK  from CORPORATE_MST_TBL) " & vbCrLf _
                //& " AND FRM.BUSINESS_TYPE in (2,3) " & vbCrLf _
                //& " AND FRM.BY_DEFAULT = 1 " & vbCrLf _
                //& " AND FRM.CHARGE_BASIS <> 2 " & vbCrLf _
                //& " AND FRM.FREIGHT_ELEMENT_MST_PK NOT IN (SELECT PARM.FRT_BOF_FK  FROM PARAMETERS_TBL PARM) "
                //Change made by purnanand for freight element should come in preference order(09/01/08)

                frtDt = (new WorkFlow()).GetDataTable(strSQL);
                Int16 RowCnt = default(Int16);
                // REF_NO, POL_PK, POD_PK, CNTR_PK, FRT_PK, FRT_ID, FRT_NAME 
                // SELECTED, CURR_PK, CURR_ID, Rate, QUOTERATE, PYTYPE
                // Setting Unique Constraints for adding Remaining Freights
                UniqueConstraint UC = null;

                if (Convert.ToInt32(CargoType) == 1)
                {
                    UC = new UniqueConstraint("UkFreights", new DataColumn[] {
                        DS.Tables[1].Columns["REF_NO"],
                        DS.Tables[1].Columns["POL_PK"],
                        DS.Tables[1].Columns["POD_PK"],
                        DS.Tables[1].Columns["CNTR_PK"],
                        DS.Tables[1].Columns["FRT_PK"]
                    });
                }
                else
                {
                    if (BBFlag == 1)
                    {
                        UC = new UniqueConstraint("UkFreights", new DataColumn[] {
                            DS.Tables[1].Columns["REF_NO"],
                            DS.Tables[1].Columns["POL_PK"],
                            DS.Tables[1].Columns["POD_PK"],
                            DS.Tables[1].Columns["LCLBASIS"],
                            DS.Tables[1].Columns["FRT_PK"],
                            DS.Tables[1].Columns["COMM_PK"]
                        });
                    }
                    else
                    {
                        UC = new UniqueConstraint("UkFreights", new DataColumn[] {
                            DS.Tables[1].Columns["REF_NO"],
                            DS.Tables[1].Columns["POL_PK"],
                            DS.Tables[1].Columns["POD_PK"],
                            DS.Tables[1].Columns["LCLBASIS"],
                            DS.Tables[1].Columns["FRT_PK"]
                        });
                    }
                }
                try
                {
                    DS.Tables[1].Constraints.Add(UC);
                }
                catch (System.Exception eX)
                {
                    throw new Exception(" Duplicate Freights ", eX);
                }
                DataRow R = null;
                DataRow FR = null;
                foreach (DataRow R_loopVariable in DS.Tables[0].Rows)
                {
                    R = R_loopVariable;
                    for (RowCnt = 0; RowCnt <= frtDt.Rows.Count - 1; RowCnt++)
                    {
                        try
                        {
                            FR = DS.Tables[1].NewRow();
                            if (Convert.ToInt32(CargoType) == 1)
                            {
                                FR["REF_NO"] = R["REF_NO"];
                                FR["POL_PK"] = R["POL_PK"];
                                FR["POD_PK"] = R["POD_PK"];
                                FR["CNTR_PK"] = R["CNTR_PK"];
                                FR["FRT_PK"] = frtDt.Rows[RowCnt]["FREIGHT_ELEMENT_MST_PK"];
                                FR["FRT_ID"] = frtDt.Rows[RowCnt]["FREIGHT_ELEMENT_ID"];
                                FR["FRT_NAME"] = frtDt.Rows[RowCnt]["FREIGHT_ELEMENT_NAME"];
                                FR["SELECTED"] = 0;
                                FR["CURR_PK"] = frtDt.Rows[RowCnt]["CURRENCY_MST_PK"];
                                FR["CURR_ID"] = frtDt.Rows[RowCnt]["CURRENCY_ID"];
                                FR["Rate"] = 0;
                                FR["QUOTERATE"] = 0;
                                FR["PYTPE"] = 1;
                                FR["PYTYPE"] = "PrePaid";
                            }
                            else
                            {
                                FR["REF_NO"] = R["REF_NO"];
                                FR["POL_PK"] = R["POL_PK"];
                                FR["POD_PK"] = R["POD_PK"];
                                FR["LCLBASIS"] = R["LCL_BASIS"];
                                FR["FRT_PK"] = frtDt.Rows[RowCnt]["FREIGHT_ELEMENT_MST_PK"];
                                FR["FRT_ID"] = frtDt.Rows[RowCnt]["FREIGHT_ELEMENT_ID"];
                                FR["FRT_NAME"] = frtDt.Rows[RowCnt]["FREIGHT_ELEMENT_NAME"];
                                FR["SELECTED"] = 0;
                                FR["CURR_PK"] = frtDt.Rows[RowCnt]["CURRENCY_MST_PK"];
                                FR["CURR_ID"] = frtDt.Rows[RowCnt]["CURRENCY_ID"];
                                if (BBFlag == 1)
                                {
                                    FR["COMM_PK"] = frtDt.Rows[RowCnt]["COMM_PK"];
                                }
                                FR["Rate"] = 0;
                                FR["QUOTE_MIN_RATE"] = 0;
                                //Added by rabbani reason USS Gap,introduced New Column Min.Qte.Rate
                                FR["QUOTERATE"] = 0;
                                FR["PYTPE"] = 1;
                                FR["PYTYPE"] = "PrePaid";
                            }
                            DS.Tables[1].Rows.Add(FR);
                        }
                        catch (Exception ex)
                        {
                            throw ex;
                        }
                    }
                }
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion

        #region " Enquiry Query For (EnqNo.) "
        // 1 DataBase Call
        private string GetEnquiryQuery(string EnquiryNo, string CargoType = "1")
        {
            try
            {
                string strSQL = null;
                if (!string.IsNullOrEmpty(Convert.ToString(EnquiryNo).Trim()))
                {
                    strSQL = " Select CARGO_TYPE from ENQUIRY_BKG_SEA_TBL where ENQUIRY_REF_NO = '" + EnquiryNo + "'";
                    CargoType = (new WorkFlow()).ExecuteScaler(strSQL);
                }
                else
                {
                    EnquiryNo = "0";
                }
                // PK,TYPE,REF_NO,REFNO,SHIP_DATE,POL_PK,POL_ID,POD_PK,POD_ID,OPER_PK,OPER_ID,OPER_NAME,
                // CNTR_PK,CNTR_ID,QUANTITY,COMM_PK,COMM_ID,ALL_IN_TARIFF,ALL_IN_QUOTE,TARIFF,NET,SELECTED,
                // CUSTOMER_PK,CUSTOMER_CATPK,COMM_GRPPK,TRAN_REF_NO2
                // Getting Query for Enquiry Detail
                if (CargoType == "1")
                {
                    strSQL = "    Select   DISTINCT           " +  "     main4.ENQUIRY_BKG_SEA_PK                   PK,                  " +  "  " + SRC(SourceType.Enquiry) + "               TYPE,                " +  "     main4.ENQUIRY_REF_NO                       REF_NO,              " +  "     main4.ENQUIRY_REF_NO                       REFNO,               " +  "     TO_CHAR(tran4.EXPECTED_SHIPMENT,'" + dateFormat + "') SHIP_DATE,   " +  "     tran4.PORT_MST_POL_FK                      POL_PK,              " +  "     portpol4.PORT_ID                           POL_ID,              " +  "     tran4.PORT_MST_POD_FK                      POD_PK,              " +  "     portpod4.PORT_ID                           POD_ID,              " +  "     tran4.OPERATOR_MST_FK                      OPER_PK,             " +  "     opr4.OPERATOR_ID                           OPER_ID,             " +  "     opr4.OPERATOR_NAME                         OPER_NAME,           " +  "     tran4.CONTAINER_TYPE_MST_FK                CNTR_PK,             " +  "     cntr4.CONTAINER_TYPE_MST_ID                CNTR_ID,             " +  "     tran4.EXPECTED_BOXES                       QUANTITY,            " +  "     tran4.COMMODITY_MST_FK                     COMM_PK,             " +  "     cmdt4.COMMODITY_NAME                       COMM_ID,             " +  "     tran4.ALL_IN_TARIFF                        ALL_IN_TARIFF,       " +  "     tran4.ALL_IN_TARIFF                        ALL_IN_QUOTE,        " +  "     0                                          OPERATOR_RATE,       " +  "     NULL                                       NET,                 " +  "     main4.CUSTOMER_MST_FK                      CUSTOMER_PK,         " +  "     main4.CUSTOMER_CATEGORY_FK                 CUSTOMER_CATPK,      " +  "     tran4.COMMODITY_GROUP_FK                   COMM_GRPPK,          " +  "     NULL                                       REF_NO2,             " +  "     NULL                                       TYPE2,               " +  "     ''                                         OTH_BTN,             " +  "     ''                                         OTH_DTL,             " +  "     tran4.ENQUIRY_TRN_SEA_PK                   FK,                   " +  "     nvl( CUST_TYPE,1)                          CUST_TYPE            " +  "    from                                                             " +  "     ENQUIRY_BKG_SEA_TBL            main4,                           " +  "     ENQUIRY_TRN_SEA_FCL_LCL        tran4,                           " +  "     PORT_MST_TBL                   portpol4,                        " +  "     PORT_MST_TBL                   portpod4,                        " +  "     OPERATOR_MST_TBL               opr4,                            " +  "     COMMODITY_MST_TBL              cmdt4,                           " +  "     CONTAINER_TYPE_MST_TBL         cntr4                            " +  "    where                                                                " +  "            main4.ENQUIRY_BKG_SEA_PK    = tran4.ENQUIRY_MAIN_SEA_FK            " +  "     AND    main4.CARGO_TYPE            = 1                                    " +  "     AND    tran4.PORT_MST_POL_FK       = portpol4.PORT_MST_PK(+)              " +  "     AND    tran4.PORT_MST_POD_FK       = portpod4.PORT_MST_PK(+)              " +  "     AND    tran4.OPERATOR_MST_FK       = opr4.OPERATOR_MST_PK(+)              " +  "     AND    tran4.COMMODITY_MST_FK      = cmdt4.COMMODITY_MST_PK(+)            " +  "     AND    tran4.CONTAINER_TYPE_MST_FK = cntr4.CONTAINER_TYPE_MST_PK(+)       " +  "     AND    main4.ENQUIRY_REF_NO        = '" + EnquiryNo + "'";
                }
                else
                {
                    // PK, TYPE, REF_NO, SHIP_DATE, POL_PK, POL_ID, POD_PK, POD_ID, OPER_PK, OPER_ID, OPER_NAME
                    // COMM_PK, COMM_ID, LCL_BASIS, DIMENTION_ID, WEIGHT, VOLUME, ALL_IN_TARIFF, ALL_IN_QUOTE 
                    // TARIFF, NET, REF_NO2, TYPE2, OTH_BTN(26), OTH_DTL(27), FK(28), CUST_TYPE(29)

                    strSQL = "    Select        DISTINCT                                  " +  "     main4.ENQUIRY_BKG_SEA_PK                   PK,                  " +  "  " + SRC(SourceType.Enquiry) + "               TYPE,                " +  "     main4.ENQUIRY_REF_NO                       REF_NO,              " +  "     TO_CHAR(tran4.EXPECTED_SHIPMENT,'" + dateFormat + "') SHIP_DATE,        " +  "     tran4.PORT_MST_POL_FK                      POL_PK,              " +  "     portpol4.PORT_ID                           POL_ID,              " +  "     tran4.PORT_MST_POD_FK                      POD_PK,              " +  "     portpod4.PORT_ID                           POD_ID,              " +  "     tran4.OPERATOR_MST_FK                      OPER_PK,             " +  "     opr4.OPERATOR_ID                           OPER_ID,             " +  "     opr4.OPERATOR_NAME                         OPER_NAME,           " +  "     tran4.COMMODITY_MST_FK                     COMM_PK,             " +  "     cmdt4.COMMODITY_ID                         COMM_ID,             " +  "     tran4.BASIS                                LCL_BASIS,           " +  "     NVL(dim4.DIMENTION_ID,'')                  DIMENTION_ID,        " +  "     tran4.EXPECTED_WEIGHT                      WEIGHT,              " +  "     tran4.EXPECTED_VOLUME                      VOLUME,              " +  "     tran4.ALL_IN_TARIFF                        ALL_IN_TARIFF,       " +  "     tran4.ALL_IN_TARIFF                        ALL_IN_QUOTE,        " +  "     0                                          OPERATOR_RATE,       " +  "     NULL                                       NET,                 " +  "     NULL                                       REF_NO2,             " +  "     NULL                                       TYPE2,               " +  "     main4.CUSTOMER_MST_FK                      CUSTOMER_PK,         " +  "     main4.CUSTOMER_CATEGORY_FK                 CUSTOMER_CATPK,      " +  "     tran4.COMMODITY_GROUP_FK                   COMM_GRPPK,          " +  "     ''                                         OTH_BTN,             " +  "     ''                                         OTH_DTL,             " +  "     tran4.ENQUIRY_TRN_SEA_PK                   FK,                  " +  "     nvl( CUST_TYPE,1)                          CUST_TYPE            " +  "    from                                                             " +  "     ENQUIRY_BKG_SEA_TBL            main4,                           " +  "     ENQUIRY_TRN_SEA_FCL_LCL        tran4,                           " +  "     PORT_MST_TBL                   portpol4,                        " +  "     PORT_MST_TBL                   portpod4,                        " +  "     OPERATOR_MST_TBL               opr4,                            " +  "     COMMODITY_MST_TBL              cmdt4,                           " +  "     DIMENTION_UNIT_MST_TBL         dim4                             " +  "    where                                                                " +  "            main4.ENQUIRY_BKG_SEA_PK    = tran4.ENQUIRY_MAIN_SEA_FK            " +  "     AND    main4.CARGO_TYPE            = 2                                    " +  "     AND    tran4.PORT_MST_POL_FK       = portpol4.PORT_MST_PK(+)              " +  "     AND    tran4.PORT_MST_POD_FK       = portpod4.PORT_MST_PK(+)              " +  "     AND    tran4.OPERATOR_MST_FK       = opr4.OPERATOR_MST_PK(+)              " +  "     AND    tran4.COMMODITY_MST_FK      = cmdt4.COMMODITY_MST_PK(+)          " +  "     AND    tran4.BASIS                 = dim4.DIMENTION_UNIT_MST_PK(+)        " +  "     AND    main4.ENQUIRY_REF_NO        = '" + EnquiryNo + "'";
                }
                strSQL = strSQL.Replace("   ", " ");
                strSQL = strSQL.Replace("  ", " ");
                return strSQL;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion

        #region " Enquiry Query For Freights (EnqNo.) "

        private string GetEnquiryQueryFreights(string EnquiryNo, string CargoType = "1", int BBFlag = 0)
        {
            try
            {
                if (string.IsNullOrEmpty(Convert.ToString(EnquiryNo).Trim()))
                    EnquiryNo = "0";
                string strSQL = null;
                // REF_NO,POL_PK,POD_PK,CNTR_PK,FRT_PK,FRT_ID,FRT_NAME,SELECTED,CURR_PK,CURR_ID,RATE,QUOTERATE,PYTYPE
                if (CargoType == "1")
                {
                    strSQL = "    Select            " +  "     main4.ENQUIRY_REF_NO                       REF_NO,              " +  "     tran4.PORT_MST_POL_FK                      POL_PK,              " +  "     tran4.PORT_MST_POD_FK                      POD_PK,              " +  "     tran4.CONTAINER_TYPE_MST_FK                CNTR_PK,             " +  "     frtd4.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt4.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt4.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frtd4.CHECK_FOR_ALL_IN_RT, 1,'true','false') SELECTED,   " +  "     frtd4.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr4.CURRENCY_ID                          CURR_ID,             " +  "     frtd4.TARIFF_RATE                          RATE,                " +  "     frtd4.TARIFF_RATE                          QUOTERATE,           " +  "     1                                       PYTPE,               " +  "     'PrePaid'                                       PYTYPE               " +  "    from                                                             " +  "     ENQUIRY_BKG_SEA_TBL            main4,                           " +  "     ENQUIRY_TRN_SEA_FCL_LCL        tran4,                           " +  "     ENQUIRY_TRN_SEA_FRT_DTLS       frtd4,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt4,                            " +  "     CURRENCY_TYPE_MST_TBL          curr4                            " +  "    where                                                               " +  "            main4.ENQUIRY_BKG_SEA_PK    = tran4.ENQUIRY_MAIN_SEA_FK        " +  "     AND    tran4.ENQUIRY_TRN_SEA_PK    = frtd4.ENQUIRY_TRN_SEA_FK         " +  "     AND    frtd4.FREIGHT_ELEMENT_MST_FK = frt4.FREIGHT_ELEMENT_MST_PK(+)  " +  "     AND    frtd4.CURRENCY_MST_FK       = curr4.CURRENCY_MST_PK(+)         " +  "     AND    main4.CARGO_TYPE            = 1                                " +  "     AND    main4.ENQUIRY_REF_NO        = '" + EnquiryNo + "'              " +  "          order by frt4.preference";
                    //Added "preference order " By Prakash Chandra on 29/4/08 

                    //Added by rabbani reason USS Gap,introduced new column as "QUOTE_MIN_RATE"
                }
                else
                {
                    if (BBFlag == 1)
                    {
                        strSQL = "    Select            " +  "     main4.ENQUIRY_REF_NO                       REF_NO,              " +  "     tran4.PORT_MST_POL_FK                      POL_PK,              " +  "     tran4.PORT_MST_POD_FK                      POD_PK,              " +  "     tran4.BASIS                                LCLBASIS,            " +  "     frtd4.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt4.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt4.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frtd4.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     frtd4.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr4.CURRENCY_ID                          CURR_ID,             " +  "     frtd4.TARIFF_RATE                          RATE,                " +  "     frtd4.TARIFF_MIN_RATE                      QUOTE_MIN_RATE,      " +  "     frtd4.TARIFF_RATE                          QUOTERATE,           " +  "     1                                       PYTPE,               " +  "     'PrePaid'    PYTYPE ,TRAN4.COMMODITY_MST_FK COMM_PK             " +  "    from                                                             " +  "     ENQUIRY_BKG_SEA_TBL            main4,                           " +  "     ENQUIRY_TRN_SEA_FCL_LCL        tran4,                           " +  "     ENQUIRY_TRN_SEA_FRT_DTLS       frtd4,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt4,                            " +  "     CURRENCY_TYPE_MST_TBL          curr4                            " +  "    where                                                                " +  "            main4.ENQUIRY_BKG_SEA_PK    = tran4.ENQUIRY_MAIN_SEA_FK           " +  "     AND    tran4.ENQUIRY_TRN_SEA_PK    = frtd4.ENQUIRY_TRN_SEA_FK            " +  "     AND    frtd4.FREIGHT_ELEMENT_MST_FK = frt4.FREIGHT_ELEMENT_MST_PK(+)     " +  "     --AND    frt4.CHARGE_BASIS           <> 2                                  " +  "     AND    frtd4.CURRENCY_MST_FK       = curr4.CURRENCY_MST_PK(+)            " +  "     AND    main4.CARGO_TYPE            = 2                                   " +  "     AND    main4.ENQUIRY_REF_NO        = '" + EnquiryNo + "'                 " +  "          order by frt4.preference";
                        //Added "preference order " By Prakash Chandra on 29/4/08 
                    }
                    else
                    {
                        strSQL = "    Select            " +  "     main4.ENQUIRY_REF_NO                       REF_NO,              " +  "     tran4.PORT_MST_POL_FK                      POL_PK,              " +  "     tran4.PORT_MST_POD_FK                      POD_PK,              " +  "     tran4.BASIS                                LCLBASIS,            " +  "     frtd4.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt4.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt4.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frtd4.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     frtd4.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr4.CURRENCY_ID                          CURR_ID,             " +  "     frtd4.TARIFF_RATE                          RATE,                " +  "     frtd4.TARIFF_MIN_RATE                      QUOTE_MIN_RATE,      " +  "     frtd4.TARIFF_RATE                          QUOTERATE,           " +  "     1                                       PYTPE,               " +  "     'PrePaid'                               PYTYPE               " +  "    from                                                             " +  "     ENQUIRY_BKG_SEA_TBL            main4,                           " +  "     ENQUIRY_TRN_SEA_FCL_LCL        tran4,                           " +  "     ENQUIRY_TRN_SEA_FRT_DTLS       frtd4,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt4,                            " +  "     CURRENCY_TYPE_MST_TBL          curr4                            " +  "    where                                                                " +  "            main4.ENQUIRY_BKG_SEA_PK    = tran4.ENQUIRY_MAIN_SEA_FK           " +  "     AND    tran4.ENQUIRY_TRN_SEA_PK    = frtd4.ENQUIRY_TRN_SEA_FK            " +  "     AND    frtd4.FREIGHT_ELEMENT_MST_FK = frt4.FREIGHT_ELEMENT_MST_PK(+)     " +  "     --AND    frt4.CHARGE_BASIS           <> 2                                  " +  "     AND    frtd4.CURRENCY_MST_FK       = curr4.CURRENCY_MST_PK(+)            " +  "     AND    main4.CARGO_TYPE            = 2                                   " +  "     AND    main4.ENQUIRY_REF_NO        = '" + EnquiryNo + "'                 " +  "          order by frt4.preference";
                        //Added "preference order " By Prakash Chandra on 29/4/08 
                    }

                }
                strSQL = strSQL.Replace("   ", " ");
                strSQL = strSQL.Replace("  ", " ");
                return strSQL;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region " Quotation Query For (QuotePk.) "

        private string GetQuoteQuery(string QuotationPK, string CargoType = "1", long Version = 0, object QuotationStatus = null, DataTable OthDt = null, object ValidFor = null, object QuoteDate = null, object ShipDate = null, Int32 CREDIT_DAYS = 0, int CREDIT_Limit = 0,
        string remarks = "", string CargoMoveCode = "", object BaseCurrencyId = null, int INCOTerms = 0, int PYMTType = 0)
        {
            try
            {
                string strSQL = null;
                string cargos = null;
                DataTable scalerDT = null;
                if (!string.IsNullOrEmpty(Convert.ToString(QuotationPK).Trim()))
                {
                    strSQL = " Select nvl(CARGO_TYPE,1), nvl(QUOTATION_SEA_TBL.VERSION_NO, 0) VERSION_NO, nvl(STATUS,1), nvl(VALID_FOR,1), " + "        to_char(QUOTATION_DATE,'" + dateFormat + "'), " + "        to_char(EXPECTED_SHIPMENT_DT,'" + dateFormat + "'), " + "        CREDIT_DAYS, " + "        CREDIT_LIMIT," + "        remarks," + "        cargo_move_fk," + "        CURR.CURRENCY_MST_PK BASE_CURRENCY_FK," + "        CURR.CURRENCY_ID, shipping_terms_mst_pk, pymt_type " + "  from  QUOTATION_SEA_TBL,  CURRENCY_TYPE_MST_TBL CURR " + " where CURR.CURRENCY_MST_PK(+) = QUOTATION_SEA_TBL.BASE_CURRENCY_FK" + " AND QUOTATION_SEA_PK = " + QuotationPK;
                    scalerDT = (new WorkFlow()).GetDataTable(strSQL);
                    if (scalerDT.Rows.Count > 0)
                    {
                        CargoType = Convert.ToString(removeDBNull(scalerDT.Rows[0][0]));
                        Version = Convert.ToInt32(removeDBNull(scalerDT.Rows[0][1]));
                        QuotationStatus = removeDBNull(scalerDT.Rows[0][2]);
                        ValidFor = removeDBNull(scalerDT.Rows[0][3]);
                        QuoteDate = removeDBNull(scalerDT.Rows[0][4]);
                        ShipDate = removeDBNull(scalerDT.Rows[0][5]);
                        CREDIT_DAYS = Convert.ToInt32(removeDBNull(scalerDT.Rows[0][6]));
                        CREDIT_Limit = Convert.ToInt32(removeDBNull(scalerDT.Rows[0][7]));
                        remarks = Convert.ToString(removeDBNull(scalerDT.Rows[0][8]));
                        CargoMoveCode = Convert.ToString(removeDBNull(scalerDT.Rows[0][9]));
                        BaseCurrencyId = Convert.ToString(removeDBNull(scalerDT.Rows[0]["CURRENCY_ID"]));
                        INCOTerms = Convert.ToInt32(removeDBNull(scalerDT.Rows[0]["shipping_terms_mst_pk"]));
                        PYMTType = Convert.ToInt32(removeDBNull(scalerDT.Rows[0]["pymt_type"]));
                    }
                    strSQL = " Select FREIGHT_ELEMENT_MST_FK, CURRENCY_MST_FK, AMOUNT, QUOTATION_TRN_SEA_FK ,FREIGHT_TYPE PYMT_TYPE " + " from QUOTATION_TRN_SEA_OTH_CHRG where QUOTATION_TRN_SEA_FK IN " + "      ( Select QUOTE_TRN_SEA_PK from QUOTATION_TRN_SEA_FCL_LCL where " + "               QUOTATION_SEA_FK = " + QuotationPK + " ) ";
                    OthDt = (new WorkFlow()).GetDataTable(strSQL);
                }
                else
                {
                    QuotationPK = "0";
                }

                //modified by Thiyagarajan on 28/5/08 for fcl and lcl combination
                if (Convert.ToInt32(CargoType) == 3)
                {
                    if (cargo == 0)
                    {
                        cargo = 1;
                        basis_cont = 1;
                    }
                }
                if (basis_cont > 0)
                {
                    if (cargo == 2)
                    {
                        cargos = " and tran3.container_type_mst_fk is null ";
                    }
                    else if (cargo == 1)
                    {
                        cargos = " and tran3.basis is null ";
                    }
                }
                else
                {
                    cargos = "";
                }
                //end
                //Snigdharani - & "     NULL   NET," & vbCrLf _ Net is changed both for FCL and LCL.
                // Getting Query for Quotation Detail
                if (CargoType == "1" | cargo == 1)
                {
                    strSQL = "    Select     DISTINCT                                    " +  "     main3.QUOTATION_SEA_PK                     PK,                  " +  "     Decode(tran3.TRANS_REFERED_FROM," + SourceType.SpotRate + "," + SRC(SourceType.SpotRate) + "," + " " + SourceType.Quotation + "," + SRC(SourceType.Quotation) + "," + " " + SourceType.Enquiry + "," + SRC(SourceType.Enquiry) + "," + " " + SourceType.OperatorTariff + "," + SRC(SourceType.OperatorTariff) + "," + " " + SourceType.CustomerContract + "," + SRC(SourceType.CustomerContract) + "," + " " + SourceType.GeneralTariff + "," + SRC(SourceType.GeneralTariff) + "," + " " + SourceType.Manual + "," + SRC(SourceType.Manual) + ")          TYPE,                " +  "     tran3.TRANS_REF_NO                         REF_NO,              " +  "     tran3.TRANS_REF_NO                         REFNO,               " +  "     TO_CHAR(main3.EXPECTED_SHIPMENT_DT,'" + dateFormat + "')  SHIP_DATE,    " +  "     tran3.PORT_MST_POL_FK                      POL_PK,              " +  "     portpol3.PORT_ID                           POL_ID,              " +  "     tran3.PORT_MST_POD_FK                      POD_PK,              " +  "     portpod3.PORT_ID                           POD_ID,              " +  "     tran3.OPERATOR_MST_FK                      OPER_PK,             " +  "     opr3.OPERATOR_ID                           OPER_ID,             " +  "     opr3.OPERATOR_NAME                         OPER_NAME,           " +  "     tran3.CONTAINER_TYPE_MST_FK                CNTR_PK,             " +  "     cntr3.CONTAINER_TYPE_MST_ID                CNTR_ID,             " +  "     tran3.EXPECTED_BOXES                       QUANTITY,            " +  "     tran3.COMMODITY_MST_FK                     COMM_PK,             " +  "     cmdt3.COMMODITY_NAME                       COMM_ID,             " +  "     tran3.ALL_IN_TARIFF                        ALL_IN_TARIFF,       " +  "     tran3.ALL_IN_QUOTED_TARIFF                 ALL_IN_QUOTE,        " +  "     nvl(tran3.BUYING_RATE,0)                   OPERATOR_RATE,       " +  "     case                                                            " +  "     when (nvl(tran3.BUYING_RATE, 0) > 0) then                       " +  "     nvl(tran3.ALL_IN_QUOTED_TARIFF -                                " +  "     nvl(tran3.BUYING_RATE, 0),                                      " +  "     0)                                                              " +  "     Else                                                            " +  "     NULL                                                            " +  "     end NET,                                                        " +  "     main3.CUSTOMER_MST_FK                      CUSTOMER_PK,         " +  "     main3.CUSTOMER_CATEGORY_MST_FK             CUSTOMER_CATPK,      " +  "     tran3.COMMODITY_GROUP_FK                   COMM_GRPPK,          " +  "     tran3.TRAN_REF_NO2                         REF_NO2,             " +  "     tran3.REF_TYPE2                            TYPE2,               " +  "     ''                                         OTH_BTN,             " +  "     ''                                         OTH_DTL,             " +  "     tran3.QUOTE_TRN_SEA_PK                     FK,                  " +  "     nvl(main3.CUST_TYPE,1)                          CUST_TYPE            " +  "    from                                                             " +  "     QUOTATION_SEA_TBL              main3,                           " +  "     QUOTATION_TRN_SEA_FCL_LCL      tran3,                           " +  "     PORT_MST_TBL                   portpol3,                        " +  "     PORT_MST_TBL                   portpod3,                        " +  "     OPERATOR_MST_TBL               opr3,                            " +  "     COMMODITY_MST_TBL              cmdt3,                           " +  "     CONTAINER_TYPE_MST_TBL         cntr3                            " +  "   where    main3.QUOTATION_SEA_PK      = tran3.QUOTATION_SEA_FK             " +  "     AND    tran3.PORT_MST_POL_FK       = portpol3.PORT_MST_PK(+)            " +  "     AND    tran3.PORT_MST_POD_FK       = portpod3.PORT_MST_PK(+)            " +  "     AND    tran3.OPERATOR_MST_FK       = opr3.OPERATOR_MST_PK(+)            " +  "     AND    tran3.CONTAINER_TYPE_MST_FK = cntr3.CONTAINER_TYPE_MST_PK(+)     " +  "     AND    tran3.COMMODITY_MST_FK      = cmdt3.COMMODITY_MST_PK(+)          " +  "     AND    main3.QUOTATION_SEA_PK =  " + QuotationPK +  cargos;
                }
                else
                {
                    strSQL = "    Select     DISTINCT                                    " +  "     main3.QUOTATION_SEA_PK                     PK,                  " +  "     Decode(tran3.TRANS_REFERED_FROM," + SourceType.SpotRate + "," + SRC(SourceType.SpotRate) + "," + " " + SourceType.Quotation + "," + SRC(SourceType.Quotation) + "," + " " + SourceType.Enquiry + "," + SRC(SourceType.Enquiry) + "," + " " + SourceType.OperatorTariff + "," + SRC(SourceType.OperatorTariff) + "," + " " + SourceType.CustomerContract + "," + SRC(SourceType.CustomerContract) + "," + " " + SourceType.GeneralTariff + "," + SRC(SourceType.GeneralTariff) + "," + " " + SourceType.Manual + "," + SRC(SourceType.Manual) + ")          TYPE,                " +  "     tran3.TRANS_REF_NO                         REF_NO,              " +  "     TO_CHAR(main3.EXPECTED_SHIPMENT_DT,'" + dateFormat + "')  SHIP_DATE, " +  "     tran3.PORT_MST_POL_FK                      POL_PK,              " +  "     portpol3.PORT_ID                           POL_ID,              " +  "     tran3.PORT_MST_POD_FK                      POD_PK,              " +  "     portpod3.PORT_ID                           POD_ID,              " +  "     tran3.OPERATOR_MST_FK                      OPER_PK,             " +  "     opr3.OPERATOR_ID                           OPER_ID,             " +  "     opr3.OPERATOR_NAME                         OPER_NAME,           " +  "     tran3.COMMODITY_MST_FK                     COMM_PK,             " +  "     cmdt3.COMMODITY_NAME                       COMM_ID,             " +  "     tran3.BASIS                                LCL_BASIS,           " +  "     NVL(dim3.DIMENTION_ID,'')                  DIMENTION_ID,        " +  "     tran3.EXPECTED_WEIGHT                      WEIGHT,              " +  "     tran3.EXPECTED_VOLUME                      VOLUME,              " +  "     tran3.ALL_IN_TARIFF                        ALL_IN_TARIFF,       " +  "     tran3.ALL_IN_QUOTED_TARIFF                 ALL_IN_QUOTE,        " +  "     nvl(tran3.BUYING_RATE,0)                   OPERATOR_RATE,       " +  "     case                                                            " +  "     when (nvl(tran3.BUYING_RATE, 0) > 0) then                       " +  "     nvl(tran3.ALL_IN_QUOTED_TARIFF -                                " +  "     nvl(tran3.BUYING_RATE, 0),                                      " +  "     0)                                                              " +  "     Else                                                            " +  "     NULL                                                            " +  "     end NET,                                                        " +  "     tran3.TRAN_REF_NO2                         REF_NO2,             " +  "     tran3.REF_TYPE2                            TYPE2,               " +  "     main3.CUSTOMER_MST_FK                      CUSTOMER_PK,         " +  "     main3.CUSTOMER_CATEGORY_MST_FK             CUSTOMER_CATPK,      " +  "     tran3.COMMODITY_GROUP_FK                   COMM_GRPPK,          " +  "     ''                                         OTH_BTN,             " +  "     ''                                         OTH_DTL,             " +  "     tran3.QUOTE_TRN_SEA_PK                     FK,                  " +  "     nvl(main3.CUST_TYPE,1)                     CUST_TYPE            " +  "    from                                                             " +  "     QUOTATION_SEA_TBL              main3,                           " +  "     QUOTATION_TRN_SEA_FCL_LCL      tran3,                           " +  "     PORT_MST_TBL                   portpol3,                        " +  "     PORT_MST_TBL                   portpod3,                        " +  "     OPERATOR_MST_TBL               opr3,                            " +  "     COMMODITY_MST_TBL              cmdt3,                           " +  "     DIMENTION_UNIT_MST_TBL         dim3                             " +  "    where                                                                " +  "            main3.QUOTATION_SEA_PK      = tran3.QUOTATION_SEA_FK             " +  "     AND    tran3.PORT_MST_POL_FK       = portpol3.PORT_MST_PK(+)            " +  "     AND    tran3.PORT_MST_POD_FK       = portpod3.PORT_MST_PK(+)            " +  "     AND    tran3.OPERATOR_MST_FK       = opr3.OPERATOR_MST_PK(+)            " +  "     AND    tran3.BASIS                 = dim3.DIMENTION_UNIT_MST_PK(+)      " +  "     AND    tran3.COMMODITY_MST_FK      = cmdt3.COMMODITY_MST_PK(+)          " +  "     AND    main3.QUOTATION_SEA_PK =  " + QuotationPK +  cargos;
                }
                strSQL = strSQL.Replace("   ", " ");
                strSQL = strSQL.Replace("  ", " ");
                return strSQL;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion

        #region " Quotation Query For Freights (QuotePk.) "
        private string GetQuoteQueryFreights(string QuotationPK, string CargoType = "1")
        {
            try
            {
                if (string.IsNullOrEmpty(Convert.ToString(QuotationPK).Trim()))
                    QuotationPK = "0";
                string strSQL = null;
                string strSQL1 = null;
                string cargos = null;

                //adding by Thiyagarajan 0n 28/5/08 for fcl and lcl combination
                if (basis_cont > 0)
                {
                    if (cargo == 2)
                    {
                        cargos = " and tran3.container_type_mst_fk is null ";
                    }
                    else if (cargo == 1)
                    {
                        cargos = " and tran3.basis is null ";
                    }
                }
                else
                {
                    cargos = "";
                }
                //end

                if (CargoType == "1")
                {
                    strSQL = "    (Select            " +  "     tran3.TRANS_REF_NO                         REF_NO,              " +  "     tran3.PORT_MST_POL_FK                      POL_PK,              " +  "     tran3.PORT_MST_POD_FK                      POD_PK,              " +  "     tran3.CONTAINER_TYPE_MST_FK                CNTR_PK,             " +  "     frtd3.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt3.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt3.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frtd3.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     frtd3.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr3.CURRENCY_ID                          CURR_ID,             " +  "     frtd3.TARIFF_RATE                          RATE,                " +  "     frtd3.QUOTED_RATE                          QUOTERATE,           " +  "     frtd3.PYMT_TYPE                            PYTPE,               " +  "     decode(frtd3.PYMT_TYPE,1,'PrePaid','Collect') PYTYPE,           " +  "     frt3.preference preference                                      " +  "    from                                                             " +  "     QUOTATION_SEA_TBL              main3,                           " +  "     QUOTATION_TRN_SEA_FCL_LCL      tran3,                           " +  "     QUOTATION_TRN_SEA_FRT_DTLS     frtd3,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt3,                            " +  "     CURRENCY_TYPE_MST_TBL          curr3                            " +  "    where                                                               " +  "            main3.QUOTATION_SEA_PK      = tran3.QUOTATION_SEA_FK           " +  "     AND    tran3.QUOTE_TRN_SEA_PK      = frtd3.QUOTE_TRN_SEA_FK           " +  "     AND    frtd3.FREIGHT_ELEMENT_MST_FK = frt3.FREIGHT_ELEMENT_MST_PK(+)  " +  "     AND    frt3.CHARGE_BASIS           <> 2                               " +  "     AND    frtd3.CURRENCY_MST_FK       = curr3.CURRENCY_MST_PK(+)         " +  "     AND    main3.QUOTATION_SEA_PK =  " + QuotationPK +  cargos + " ) ";
                    //& " order by frt3.preference"  'Added " Preference Order" By Prakash Chandra on 29/4/08

                    //Added by rabbani reason USS Gap,introduced new field as "QUOTE_MIN_RATE"
                }
                else
                {
                    strSQL = "    Select            " +  "     tran3.TRANS_REF_NO                         REF_NO,              " +  "     tran3.PORT_MST_POL_FK                      POL_PK,              " +  "     tran3.PORT_MST_POD_FK                      POD_PK,              " +  "     tran3.BASIS                                LCLBASIS,            " +  "     frtd3.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt3.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt3.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frtd3.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     frtd3.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr3.CURRENCY_ID                          CURR_ID,             " +  "     frtd3.TARIFF_RATE                          RATE,                " +  "     frtd3.QUOTED_MIN_RATE                      QUOTE_MIN_RATE,      " +  "     frtd3.QUOTED_RATE                          QUOTERATE,           " +  "     frtd3.PYMT_TYPE                            PYTPE,               " +  "     decode(frtd3.PYMT_TYPE,1,'PrePaid','Collect') PYTYPE,frt3.preference preference " +  "    from                                                             " +  "     QUOTATION_SEA_TBL              main3,                           " +  "     QUOTATION_TRN_SEA_FCL_LCL      tran3,                           " +  "     QUOTATION_TRN_SEA_FRT_DTLS     frtd3,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt3,                            " +  "     CURRENCY_TYPE_MST_TBL          curr3                            " +  "    where                                                                " +  "            main3.QUOTATION_SEA_PK      = tran3.QUOTATION_SEA_FK           " +  "     AND    tran3.QUOTE_TRN_SEA_PK      = frtd3.QUOTE_TRN_SEA_FK           " +  "     AND    frtd3.FREIGHT_ELEMENT_MST_FK = frt3.FREIGHT_ELEMENT_MST_PK(+)  " +  "     AND    frt3.CHARGE_BASIS           <> 2                               " +  "     AND    frtd3.CURRENCY_MST_FK       = curr3.CURRENCY_MST_PK(+)         " +  "     AND    main3.QUOTATION_SEA_PK =  " + QuotationPK + "                     " +  cargos;
                    //& "            order by frt3.preference"  'Added " Preference Order" By Prakash Chandra on 29/4/08

                }
                //Manoharan 10July2007: to show all the freight elements when Quotation is active
                strSQL1 = "  union  (Select tran3.TRANS_REF_NO REF_NO, tran3.PORT_MST_POL_FK POL_PK, tran3.PORT_MST_POD_FK POD_PK,  ";
                if (CargoType == "1")
                {
                    strSQL1 += " tran3.CONTAINER_TYPE_MST_FK CNTR_PK, ";
                }
                else
                {
                    strSQL1 += " tran3.BASIS LCLBASIS, ";
                }

                strSQL1 += "  FRM.FREIGHT_ELEMENT_MST_PK  FRT_PK, FRM.FREIGHT_ELEMENT_ID      FRT_ID,  " +  "  FRM.FREIGHT_ELEMENT_NAME    FRT_NAME, 'false'   SELECTED,  CURR.CURRENCY_MST_PK  CURR_PK, " +  "  CURR.CURRENCY_ID  CURR_ID,  null   RATE, ";

                if (CargoType == "2")
                {
                    strSQL1 += " null    QUOTE_MIN_RATE, ";
                }
                //modified by thiyagarajan on 2/12/08 for location based curr. task
                //Snigdharani - 30/12/2008 -  AND FRM.BY_DEFAULT = 1 - Removed
                strSQL1 += " null    QUOTERATE, 1    PYTPE, " +  "  'PrePaid'  PYTYPE,FRM.Preference preference  from FREIGHT_ELEMENT_MST_TBL FRM, CURRENCY_TYPE_MST_TBL CURR, " +  "  QUOTATION_SEA_TBL   main3,  QUOTATION_TRN_SEA_FCL_LCL tran3, QUOTATION_TRN_SEA_FRT_DTLS  frtd3  " +  "  where FRM.ACTIVE_FLAG = 1 AND CURR.CURRENCY_MST_PK = " + HttpContext.Current.Session["currency_mst_pk"] +  "  AND FRM.BUSINESS_TYPE in (2,3) AND FRM.CHARGE_BASIS <> 2 " +  "  AND FRM.FREIGHT_ELEMENT_MST_PK NOT IN (SELECT PARM.FRT_BOF_FK  FROM PARAMETERS_TBL PARM) " +  "  and frm.freight_element_mst_pk not in (select  frtd3.FREIGHT_ELEMENT_MST_FK  from " +  "  QUOTATION_SEA_TBL   main3,  QUOTATION_TRN_SEA_FCL_LCL tran3, QUOTATION_TRN_SEA_FRT_DTLS  frtd3 " +  "  where  main3.QUOTATION_SEA_PK = tran3.QUOTATION_SEA_FK  AND tran3.QUOTE_TRN_SEA_PK = frtd3.QUOTE_TRN_SEA_FK " +  "  AND main3.QUOTATION_SEA_PK = " + QuotationPK +  cargos;
                strSQL1 += " ) and   main3.QUOTATION_SEA_PK = tran3.QUOTATION_SEA_FK   " +  "  and main3.Status=1 AND tran3.QUOTE_TRN_SEA_PK = frtd3.QUOTE_TRN_SEA_FK AND main3.QUOTATION_SEA_PK = " + QuotationPK +  cargos +  ") order by preference";
                strSQL = strSQL + strSQL1;
                //End    Manoharan 10July2007:
                strSQL = strSQL.Replace("   ", " ");
                strSQL = strSQL.Replace("  ", " ");
                return strSQL;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion
        public void UpdateTempCus(int CustomerPK)
        {
            try
            {
                WorkFlow objWF = new WorkFlow();
                bool Result = false;
                Result = objWF.ExecuteCommands("update temp_customer_tbl set transaction_type=1 where customer_mst_pk=' " + CustomerPK + "'");
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Fetch Barcode Manager Pk"
        public int FetchBarCodeManagerPk(string Configid)
        {
            string StrSql = null;
            DataSet DsBarManager = null;
            int strReturn = 0;
            try
            {
                WorkFlow objWF = new WorkFlow();
                //StrSql = "select bdmt.bcd_mst_pk,bdmt.field_name from  barcode_data_mst_tbl bdmt where bdmt.config_id_fk='" & Configid & " '"

                StrSql = "select bdmt.bcd_mst_pk,bdmt.field_name from  barcode_data_mst_tbl bdmt" +  "where bdmt.config_id_fk= '" + Configid + "'";
                DsBarManager = objWF.GetDataSet(StrSql);
                if (DsBarManager.Tables[0].Rows.Count > 0)
                {
                    var _with3 = DsBarManager.Tables[0].Rows[0];
                    strReturn = Convert.ToInt32(removeDBNull(_with3["bcd_mst_pk"]));
                }
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion


        #region "Fetch Barcode Type"
        public DataSet FetchBarCodeField(int BarCodeManagerPk)
        {
            string StrSql = null;
            DataSet DsBarManager = null;
            int strReturn = 0;
            WorkFlow objWF = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();
            try
            {
                strQuery.Append("select bdmt.bcd_mst_pk, bdmt.field_name, bdmt.default_value" );
                strQuery.Append("  from barcode_data_mst_tbl bdmt, barcode_doc_data_tbl bddt" );
                strQuery.Append(" where bdmt.bcd_mst_pk = bddt.bcd_mst_fk" );
                strQuery.Append("   and bdmt.BCD_MST_FK= " + BarCodeManagerPk);
                strQuery.Append(" ORDER BY default_value desc" );

                // StrSql = "select bdmt.bcd_mst_pk, bdmt.field_name ,bdmt.default_value from barcode_data_mst_tbl bdmt, barcode_doc_data_tbl bddt where bdmt.bcd_mst_pk=bddt.bcd_mst_fk and bdmt.BCD_MST_FK=" & BarCodeManagerPk
                DsBarManager = objWF.GetDataSet(strQuery.ToString());
                return DsBarManager;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region " Main Fetch Query "
        // this is modified by Thiyagarajan 28/5/08 for fcl and lcl
        public DataSet Fetch_Header(int Iquotationpk = 0)
        {

            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT QTS.PORT_MST_POL_FK,");
            sb.Append("       POL.PORT_ID,");
            sb.Append("       POL.PORT_NAME,");
            sb.Append("       QTS.Port_Mst_Pod_Fk podpk,");
            sb.Append("       POD.PORT_ID podid,");
            sb.Append("       POD.PORT_NAME podNAME,");
            sb.Append("       QST.CUSTOMER_MST_FK,");
            sb.Append("       CMT.CUSTOMER_ID,");
            sb.Append("       CMT.CUSTOMER_NAME,");
            sb.Append("       QST.CARGO_TYPE,");
            sb.Append("       QST.CUSTOMER_CATEGORY_MST_FK,");
            sb.Append("       QST.QUOTED_BY,");
            sb.Append("       QST.STATUS,");
            sb.Append("       QST.CREATED_DT,");
            sb.Append("       QST.QUOTATION_DATE,");
            sb.Append("       QST.EXPECTED_SHIPMENT_DT,");
            sb.Append("       QST.REMARKS,");
            sb.Append("       QST.CREDIT_LIMIT,");
            sb.Append("       QST.CREDIT_DAYS,");
            sb.Append("        QST.VALID_FOR,");
            sb.Append("        QST.CUST_TYPE,");
            sb.Append("       QST.PYMT_TYPE,");
            sb.Append("       QST.SHIPPING_TERMS_MST_PK,");
            sb.Append("       QST.CARGO_MOVE_FK");
            sb.Append("");
            sb.Append("    FROM QUOTATION_SEA_TBL         QST,");
            sb.Append("       QUOTATION_TRN_SEA_FCL_LCL QTS,");
            sb.Append("       CUSTOMER_MST_TBL          CMT,");
            sb.Append("       PORT_MST_TBL              POL,");
            sb.Append("       PORT_MST_TBL              POD,");
            sb.Append("       CUSTOMER_CATEGORY_MST_TBL CCMT");
            sb.Append("");
            sb.Append("   WHERE QST.QUOTATION_SEA_PK = QTS.QUOTATION_SEA_FK(+)");
            sb.Append("   AND POL.PORT_MST_PK = QTS.PORT_MST_POL_FK");
            sb.Append("   AND POD.PORT_MST_PK = QTS.PORT_MST_POD_FK");
            sb.Append("   AND CMT.CUSTOMER_MST_PK = QST.CUSTOMER_MST_FK");
            sb.Append("   AND CCMT.CUSTOMER_CATEGORY_MST_PK = QST.CUSTOMER_CATEGORY_MST_FK");
            if (Iquotationpk > 0)
            {
                sb.Append("   AND QST.QUOTATION_SEA_PK =  " + Iquotationpk + "");
            }

            try
            {
                return (objWF.GetDataSet(sb.ToString()));

            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }

        public DataSet Fetch_OtherCharges(int Iquotationpk = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);


            if (Iquotationpk > 0)
            {
                sb.Append("SELECT Q.FREIGHT_ELEMENT_MST_FK || '~' || Q.CURRENCY_MST_FK || '~' ||");
                sb.Append("       Q.AMOUNT || '~' || Q.FREIGHT_TYPE AS Freight");
                sb.Append("  FROM QUOTATION_TRN_SEA_OTH_CHRG Q WHERE q.quotation_sea_fk=  " + Iquotationpk + "");

            }

            try
            {
                return (objWF.GetDataSet(sb.ToString()));

            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }
        public DataSet Fetch_FreightCharges(int Iquotationpk = 0)
        {

            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);


            if (Iquotationpk > 0)
            {
                sb.Append("SELECT SUM(NVL(FRTCHG,0))AS  Frt FROM ");
                sb.Append(" (SELECT QT.BUYING_RATE FRTCHG, QT.QUOTATION_SEA_FK");
                sb.Append("       FROM QUOTATION_TRN_SEA_FCL_LCL QT");
                sb.Append("   WHERE QT.QUOTATION_SEA_FK =  " + Iquotationpk + " ) q  GROUP BY QUOTATION_SEA_FK");

            }

            try
            {
                return (objWF.GetDataSet(sb.ToString()));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public DataSet Fetch_Freight_Other_Charges(int Iquotationpk = 0)
        {

            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            if (Iquotationpk > 0)
            {
                sb.Append("SELECT sum(Nvl(Q.AMOUNT,0)) Amount");
                sb.Append("  FROM QUOTATION_TRN_SEA_OTH_CHRG Q");
                sb.Append("   WHERE  Q.Quotation_Sea_Fk = " + Iquotationpk + "");
                sb.Append("   GROUP BY Q.Quotation_Sea_Fk");
            }
            try
            {
                return (objWF.GetDataSet(sb.ToString()));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }


        public void FetchOne(DataSet GridDS = null, DataSet EnqDS = null, string EnqNo = "", string QuoteNo = "", string CustNo = "", string CustID = "", string CustCategory = "", string AgentNo = "", string AgentID = "", string CargoType = "2",
        string SectorContainers = "", string CommodityGroup = "", string ShipDate = "", string QuoteDate = "", object Options = null, int Version = 0, object QuotationStatus = null, DataTable OthDt = null, object ValidFor = null, Int16 CustomerType = 0,
        int CreditDays = 0, int CreditLimit = 0, string remarks = "", string CargoMoveCode = "", int fcllcl = 0, object BaseCurrencyId = null, int INCOTerms = 0, int PYMTType = 0, int BBFlag = 0, string CommodityPks = "",
        int PostBackFlag = 0, int From_Flag = 0)
        {
            try
            {
                DataRow DR = null;
                DataTable ExChTable = null;
                decimal Amount = default(decimal);
                string[] cargo1 = null;
                bool forFCL = false;

                System.Text.StringBuilder MasterQuery = new System.Text.StringBuilder();
                System.Text.StringBuilder FreightQuery = new System.Text.StringBuilder();
                //added by vimlesh kumar for manual quote
                if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[0].Selected)
                {
                    if (MasterQuery.Length > 0)
                    {
                        MasterQuery.Append( " UNION " );
                        FreightQuery.Append( " UNION " );
                    }
                    MasterQuery.Append(ManualQuoteQuery(forFCL, CustNo, SectorContainers, CommodityPks, ShipDate, PostBackFlag, Convert.ToInt32(QuoteNo)));
                    FreightQuery.Append(ManualQuoteFreightQuery(forFCL, CustNo, SectorContainers, CommodityGroup, ShipDate, CommodityPks, PostBackFlag, Convert.ToInt32(QuoteNo), Convert.ToInt32(QuotationStatus), From_Flag));
                }
                WorkFlow objWF = new WorkFlow();
                try
                {
                    GridDS = objWF.GetDataSet(MasterQuery.ToString());
                    GridDS.Tables.Add(objWF.GetDataTable(FreightQuery.ToString()));

                    DataRelation REL = null;
                    REL = new DataRelation("RFQRelation", new DataColumn[] { GridDS.Tables[0].Columns["COMM_PK"] }, new DataColumn[] { GridDS.Tables[1].Columns["COMM_PK"] });
                    REL.Nested = true;
                    GridDS.Relations.Add(REL);


                }
                catch (Exception eX)
                {
                    throw eX;
                }
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        private DataSet FetchOneLCL(DataSet GridDS, string EnqNo = "", string CustNo = "", string CargoType = "2", string SectorContainers = "", string CommodityGroup = "", string ShipDate = "")
        {
            try
            {
                string strSQL = null;
                string strCondition = null;
                WorkFlow objWF = new WorkFlow();
                System.Text.StringBuilder buildQuery = new System.Text.StringBuilder();
                System.Text.StringBuilder buildCondition = new System.Text.StringBuilder();
                DataSet ds = null;
                GridDS.Tables.Clear();
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return new DataSet();
        }

        public int FetchPayType(int ShipPK)
        {
            try
            {
                string strSQL = null;
                WorkFlow objWF = new WorkFlow();

                strSQL = "SELECT SH.FREIGHT_TYPE FROM SHIPPING_TERMS_MST_TBL SH WHERE SH.SHIPPING_TERMS_MST_PK = " + ShipPK;
                return Convert.ToInt32(objWF.ExecuteScaler(strSQL));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion

        #region " Fetch UWG1 Option Grid "

        #region " Header Query "

        #region " Quote Query."

        private string QuoteQuery(bool forFCL = true, string CustNo = "", string SectorContainers = "", string CommodityGroup = "", string ShipDate = "")
        {
            try
            {
                string strSQL = null;
                string cargos = "";
                //modifided by Thiygarajan on 28/5/08 for fcl and lcl combination
                if (cargotypes == 2)
                {
                    cargos = " and tran3.CONTAINER_TYPE_MST_FK is null";
                }
                else if (cargotypes == 1)
                {
                    cargos = " and tran3.basis is null ";
                }
                //end

                if (forFCL)
                {
                    strSQL = "    Select     DISTINCT                                    " +  "     main3.QUOTATION_SEA_PK                     PK,                  " +  "  " + SRC(SourceType.Quotation) + "             TYPE,                " +  "     main3.QUOTATION_REF_NO                     REF_NO,              " +  "     main3.QUOTATION_REF_NO                     REFNO,               " +  "     TO_CHAR(main3.EXPECTED_SHIPMENT_DT,'" + dateFormat + "')  SHIP_DATE,    " +  "     tran3.PORT_MST_POL_FK                      POL_PK,              " +  "     portpol3.PORT_ID                           POL_ID,              " +  "     tran3.PORT_MST_POD_FK                      POD_PK,              " +  "     portpod3.PORT_ID                           POD_ID,              " +  "     tran3.OPERATOR_MST_FK                      OPER_PK,             " +  "     opr3.OPERATOR_ID                           OPER_ID,             " +  "     opr3.OPERATOR_NAME                         OPER_NAME,           " +  "     tran3.CONTAINER_TYPE_MST_FK                CNTR_PK,             " +  "     cntr3.CONTAINER_TYPE_MST_ID                CNTR_ID,             " +  "     tran3.EXPECTED_BOXES                       QUANTITY,            " +  "     tran3.COMMODITY_MST_FK                     COMM_PK,             " +  "     cmdt3.COMMODITY_ID                         COMM_ID,             " +  "     tran3.ALL_IN_QUOTED_TARIFF                 ALL_IN_TARIFF,       " +  "     0                                          OPERATOR_RATE,       " +  "     NULL                                       TARIFF,              " +  "     NULL                                       NET,                 " +  "     'false'                                    SELECTED,            " +  "   " + SourceType.Quotation + "                 PRIORITYORDER        " +  "    from                                                             " +  "     QUOTATION_SEA_TBL              main3,                           " +  "     QUOTATION_TRN_SEA_FCL_LCL      tran3,                           " +  "     PORT_MST_TBL                   portpol3,                        " +  "     PORT_MST_TBL                   portpod3,                        " +  "     COMMODITY_MST_TBL              cmdt3,                           " +  "     OPERATOR_MST_TBL               opr3,                            " +  "     CONTAINER_TYPE_MST_TBL         cntr3                            " +  "    where                                                                " +  "            main3.QUOTATION_SEA_PK      = tran3.QUOTATION_SEA_FK             " +  "     AND    main3.CARGO_TYPE            in ( 1,3)                             " +  "     AND    tran3.PORT_MST_POL_FK       = portpol3.PORT_MST_PK(+)            " +  "     AND    tran3.PORT_MST_POD_FK       = portpod3.PORT_MST_PK(+)            " +  "     AND    tran3.OPERATOR_MST_FK       = opr3.OPERATOR_MST_PK(+)            " +  "     AND    tran3.CONTAINER_TYPE_MST_FK = cntr3.CONTAINER_TYPE_MST_PK(+)     " +  "     AND    tran3.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "       " +  "     AND    tran3.COMMODITY_MST_FK      = cmdt3.COMMODITY_MST_PK(+)          " +  "     AND    main3.CUSTOMER_MST_FK       = " + Convert.ToString(CustNo) + "               " +  "     AND    (tran3.PORT_MST_POL_FK, tran3.PORT_MST_POD_FK,                   " +  "                                   tran3.CONTAINER_TYPE_MST_FK )             " +  "              in ( " + Convert.ToString(SectorContainers) + " )                          " +  cargos +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5)  BETWEEN       " +  "            main3.QUOTATION_DATE AND (main3.QUOTATION_DATE + main3.VALID_FOR)";
                }
                else
                {
                    //  0   1      2         3        4       5       6       7        8        9        10
                    // PK, TYPE, REF_NO, SHIP_DATE, POL_PK, POL_ID, POD_PK, POD_ID, OPER_PK, OPER_ID, OPER_NAME
                    //  11         12       13          14          15      16         17           18   
                    // COMM_PK, COMM_ID, LCL_BASIS, DIMENTION_ID, WEIGHT, VOLUME, ALL_IN_TARIFF, SELECTED
                    strSQL = "    Select         DISTINCT                                 " +  "     main3.QUOTATION_SEA_PK                     PK,                 " +  "  " + SRC(SourceType.Quotation) + "             TYPE,                " +  "     main3.QUOTATION_REF_NO                     REF_NO,             " +  "     TO_CHAR(main3.EXPECTED_SHIPMENT_DT,'" + dateFormat + "')  SHIP_DATE,   " +  "     tran3.PORT_MST_POL_FK                      POL_PK,             " +  "     portpol3.PORT_ID                           POL_ID,             " +  "     tran3.PORT_MST_POD_FK                      POD_PK,             " +  "     portpod3.PORT_ID                           POD_ID,             " +  "     tran3.OPERATOR_MST_FK                      OPER_PK,            " +  "     opr3.OPERATOR_ID                           OPER_ID,            " +  "     ''                                         OPER_NAME,          " +  "     tran3.COMMODITY_MST_FK                     COMM_PK,             " +  "     cmdt3.COMMODITY_ID                         COMM_ID,             " +  "     tran3.BASIS                                LCL_BASIS,          " +  "     NVL(dim3.DIMENTION_ID,'')                  DIMENTION_ID,       " +  "     tran3.EXPECTED_WEIGHT                      WEIGHT,             " +  "     tran3.EXPECTED_VOLUME                      VOLUME,             " +  "     tran3.ALL_IN_QUOTED_TARIFF                 ALL_IN_TARIFF,      " +  "     'false'                                    SELECTED,           " +  "     0                                          OPERATOR_RATE,      " +  "     " + SourceType.Quotation + "               PRIORITYORDER       " +  "    from                                                            " +  "     QUOTATION_SEA_TBL              main3,                          " +  "     QUOTATION_TRN_SEA_FCL_LCL      tran3,                          " +  "     PORT_MST_TBL                   portpol3,                       " +  "     PORT_MST_TBL                   portpod3,                       " +  "     COMMODITY_MST_TBL              cmdt3,                           " +  "     OPERATOR_MST_TBL               opr3,                           " +  "     DIMENTION_UNIT_MST_TBL         dim3                            " +  "    where   1=2                                                             " +  "     AND    main3.QUOTATION_SEA_PK      = tran3.QUOTATION_SEA_FK              " +  "     AND    main3.CARGO_TYPE    in ( 2 ,3)                                     " +  "     AND    tran3.PORT_MST_POL_FK       = portpol3.PORT_MST_PK(+)             " +  "     AND    tran3.PORT_MST_POD_FK       = portpod3.PORT_MST_PK(+)             " +  "     AND    tran3.OPERATOR_MST_FK       = opr3.OPERATOR_MST_PK(+)             " +  "     AND    tran3.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "        " +  "     AND    tran3.COMMODITY_MST_FK      = cmdt3.COMMODITY_MST_PK(+)          " +  "     AND    tran3.BASIS                 = dim3.DIMENTION_UNIT_MST_PK(+)       " +  "     AND    (tran3.PORT_MST_POL_FK, tran3.PORT_MST_POD_FK,tran3.BASIS )       " +  "              in ( " + Convert.ToString(SectorContainers) + " )                           " +  "     AND    main3.CUSTOMER_MST_FK       = " + Convert.ToString(CustNo) + "                " +  cargos +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5)  BETWEEN        " +  "            main3.QUOTATION_DATE AND (main3.QUOTATION_DATE + main3.VALID_FOR) ";
                }
                strSQL = strSQL.Replace("   ", " ");
                strSQL = strSQL.Replace("  ", " ");
                return strSQL;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion

        #region " Enquiry Query."

        private string EnquiryQuery(bool forFCL = true, string CustNo = "", string SectorContainers = "", string CommodityGroup = "", string QuoteDate = "")
        {
            try
            {
                string strSQL = null;
                if (forFCL)
                {
                    strSQL = "    Select       DISTINCT                                   " +  "     main4.ENQUIRY_BKG_SEA_PK                   PK,                  " +  "  " + SRC(SourceType.Enquiry) + "               TYPE,                " +  "     main4.ENQUIRY_REF_NO                       REF_NO,              " +  "     main4.ENQUIRY_REF_NO                       REFNO,               " +  "     TO_CHAR(tran4.EXPECTED_SHIPMENT,'" + dateFormat + "') SHIP_DATE,        " +  "     tran4.PORT_MST_POL_FK                      POL_PK,              " +  "     portpol4.PORT_ID                           POL_ID,              " +  "     tran4.PORT_MST_POD_FK                      POD_PK,              " +  "     portpod4.PORT_ID                           POD_ID,              " +  "     tran4.OPERATOR_MST_FK                      OPER_PK,             " +  "     opr4.OPERATOR_ID                           OPER_ID,             " +  "     opr4.OPERATOR_NAME                         OPER_NAME,           " +  "     tran4.CONTAINER_TYPE_MST_FK                CNTR_PK,             " +  "     cntr4.CONTAINER_TYPE_MST_ID                CNTR_ID,             " +  "     tran4.EXPECTED_BOXES                       QUANTITY,            " +  "     tran4.COMMODITY_MST_FK                     COMM_PK,             " +  "     cmdt4.COMMODITY_ID                         COMM_ID,             " +  "     tran4.ALL_IN_TARIFF                        ALL_IN_TARIFF,       " +  "     0                                          OPERATOR_RATE,       " +  "     NULL                                       TARIFF,              " +  "     NULL                                       NET,                 " +  "    'false'                                     SELECTED,            " +  "   " + SourceType.Enquiry + "                   PRIORITYORDER        " +  "    from                                                             " +  "     ENQUIRY_BKG_SEA_TBL            main4,                           " +  "     ENQUIRY_TRN_SEA_FCL_LCL        tran4,                           " +  "     PORT_MST_TBL                   portpol4,                        " +  "     PORT_MST_TBL                   portpod4,                        " +  "     COMMODITY_MST_TBL              cmdt4,                           " +  "     OPERATOR_MST_TBL               opr4,                            " +  "     CONTAINER_TYPE_MST_TBL         cntr4                            " +  "    where                                                                " +  "            main4.ENQUIRY_BKG_SEA_PK    = tran4.ENQUIRY_MAIN_SEA_FK           " +  "     AND    main4.CARGO_TYPE            = 1                                   " +  "     AND    tran4.PORT_MST_POL_FK       = portpol4.PORT_MST_PK(+)             " +  "     AND    tran4.PORT_MST_POD_FK       = portpod4.PORT_MST_PK(+)             " +  "     AND    tran4.OPERATOR_MST_FK       = opr4.OPERATOR_MST_PK(+)             " +  "     AND    tran4.CONTAINER_TYPE_MST_FK = cntr4.CONTAINER_TYPE_MST_PK(+)      " +  "     AND    tran4.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "        " +  "     AND    tran4.COMMODITY_MST_FK      = cmdt4.COMMODITY_MST_PK(+)           " +  "     AND    main4.CUSTOMER_MST_FK       = " + Convert.ToString(CustNo) + "                " +  "     AND    (tran4.PORT_MST_POL_FK, tran4.PORT_MST_POD_FK,                    " +  "                                   tran4.CONTAINER_TYPE_MST_FK )              " +  "              in ( " + Convert.ToString(SectorContainers) + " )                           " +  "     AND    ROUND(TO_DATE('" + QuoteDate + "','" + dateFormat + "')-0.5)  <=          " +  "            tran4.EXPECTED_SHIPMENT                                           ";


                }
                else
                {
                    strSQL = "    Select        DISTINCT                                  " +  "     main4.ENQUIRY_SEA_PK                       PK,                  " +  "  " + SRC(SourceType.Enquiry) + "               TYPE,                " +  "     main4.ENQUIRY_REF_NO                       REF_NO,              " +  "     TO_CHAR(main4.EXPECTED_SHIPMENT,'" + dateFormat + "')  SHIP_DATE,       " +  "     tran4.PORT_MST_POL_FK                      POL_PK,              " +  "     portpol4.PORT_ID                           POL_ID,              " +  "     tran4.PORT_MST_POD_FK                      POD_PK,              " +  "     portpod4.PORT_ID                           POD_ID,              " +  "     tran4.OPERATOR_MST_FK                      OPER_PK,             " +  "     opr4.OPERATOR_ID                           OPER_ID,             " +  "     opr4.OPERATOR_NAME                         OPER_NAME,           " +  "     tran4.COMMODITY_MST_FK                     COMM_PK,             " +  "     cmdt4.COMMODITY_ID                         COMM_ID,             " +  "     tran4.BASIS                                LCL_BASIS,           " +  "     NVL(dim4.DIMENTION_ID,'')                  DIMENTION_ID,        " +  "     tran4.EXPECTED_WEIGHT                      WEIGHT,              " +  "     tran4.EXPECTED_VOLUME                      VOLUME,              " +  "     tran4.ALL_IN_TARIFF                        ALL_IN_TARIFF,       " +  "     'false'                                    SELECTED,            " +  "     0                                          OPERATOR_RATE,       " +  "     " + SourceType.Enquiry + "                 PRIORITYORDER        " +  "    from                                                             " +  "     ENQUIRY_BKG_SEA_TBL            main4,                           " +  "     ENQUIRY_TRN_SEA_FCL_LCL        tran4,                           " +  "     PORT_MST_TBL                   portpol4,                        " +  "     PORT_MST_TBL                   portpod4,                        " +  "     COMMODITY_MST_TBL              cmdt4,                           " +  "     OPERATOR_MST_TBL               opr4,                            " +  "     DIMENTION_UNIT_MST_TBL         dim4                             " +  "    where                                                                " +  "            main4.ENQUIRY_BKG_SEA_PK    = tran4.ENQUIRY_MAIN_SEA_FK           " +  "     AND    main4.CARGO_TYPE            = 2                                   " +  "     AND    tran4.PORT_MST_POL_FK       = portpol4.PORT_MST_PK(+)             " +  "     AND    tran4.PORT_MST_POD_FK       = portpod4.PORT_MST_PK(+)             " +  "     AND    tran4.OPERATOR_MST_FK       = opr4.OPERATOR_MST_PK(+)             " +  "     AND    tran4.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "        " +  "     AND    tran4.COMMODITY_MST_FK      = cmdt4.COMMODITY_MST_PK(+)           " +  "     AND    tran4.BASIS                 = dim4.DIMENTION_UNIT_MST_PK(+)       " +  "     AND    (tran4.PORT_MST_POL_FK, tran4.PORT_MST_POD_FK,                    " +  "                                   tran4.LCL_BASIS )                          " +  "              in ( " + Convert.ToString(SectorContainers) + " )                           " +  "     AND    main4.CUSTOMER_MST_FK       = " + Convert.ToString(CustNo) + "                " +  "     AND    ROUND(TO_DATE('" + QuoteDate + "','" + dateFormat + "')-0.5) <=             " +  "            tran4.EXPECTED_SHIPMENT                                           ";
                }
                strSQL = strSQL.Replace("   ", " ");
                strSQL = strSQL.Replace("  ", " ");
                return strSQL;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion

        #region " Spot Rate Query."

        private string SpotRateQuery(bool forFCL = true, string CustNo = "", string SectorContainers = "", string CommodityGroup = "", string ShipDate = "")
        {
            string strSQL = null;
            string exchQueryFCL = null;
            string exchQueryLCL = null;
            try
            {
                if (forFCL)
                {
                    exchQueryFCL = " ( Select nvl(Sum(NVL(FCL_APP_RATE * EXCHANGE_RATE,0)),0)          " +  "   from  RFQ_SPOT_TRN_SEA_FCL_LCL t1, V_EXCHANGE_RATE v1,            ";
                    // & vbCrLf & _
                    //Snigdharani - Removing v-array - 04/11/2008
                    //"TABLE(t1.CONTAINER_DTL_FCL) (+) c1,                         " & vbCrLf & _
                    exchQueryFCL = exchQueryFCL + "RFQ_SPOT_TRN_SEA_CONT_DET c1,                         " +  "         FREIGHT_ELEMENT_MST_TBL f1                                  " +  "   where t1.RFQ_SPOT_SEA_FK = main1.RFQ_SPOT_SEA_PK and v1.exch_rate_type_fk = 1 and              " +  "         c1.CONTAINER_TYPE_MST_FK = cont1.CONTAINER_TYPE_MST_FK  AND " +  "         C1.RFQ_SPOT_SEA_TRN_FK = t1.RFQ_SPOT_SEA_TRN_PK         AND " +  "         t1.CHECK_FOR_ALL_IN_RT   = 1                            AND " +  "         t1.CURRENCY_MST_FK =  v1.CURRENCY_MST_FK                AND " +  "         f1.FREIGHT_ELEMENT_MST_PK =  t1.FREIGHT_ELEMENT_MST_FK  AND " +  "         f1.CHARGE_BASIS <> 2                                    AND " +  "         ROUND(sysdate-0.5) between v1.FROM_DATE and v1.TO_DATE )      ";
                }

                if (!forFCL)
                {
                    exchQueryLCL = "( Select nvl(Sum(NVL(LCL_APPROVED_RATE * EXCHANGE_RATE,0)),0)      " +  "   from  RFQ_SPOT_TRN_SEA_FCL_LCL t1, V_EXCHANGE_RATE v1,            " +  "         FREIGHT_ELEMENT_MST_TBL f1                                  " +  "   where t1.RFQ_SPOT_SEA_FK = main1.RFQ_SPOT_SEA_PK and v1.exch_rate_type_fk = 1 and              " +  "         t1.CHECK_FOR_ALL_IN_RT = 1 AND                              " +  "         t1.CURRENCY_MST_FK =  v1.CURRENCY_MST_FK AND                " +  "         f1.FREIGHT_ELEMENT_MST_PK =  t1.FREIGHT_ELEMENT_MST_FK  AND " +  "         f1.CHARGE_BASIS <> 2                                    AND " +  "         ROUND(sysdate-0.5) between v1.FROM_DATE and v1.TO_DATE )    ";
                }
                //Snigdharani - 28/11/2008 - The query is modified by Snigdharani for comparing with commodity _group 
                //table and commodity table differently, as commodity group is mandatory but commodity is not mandatory.
                if (forFCL)
                {
                    strSQL = "    Select       DISTINCT                                   " +  "     main1.RFQ_SPOT_SEA_PK                      PK,                  " +  "  " + SRC(SourceType.SpotRate) + "             TYPE,                " +  "     main1.RFQ_REF_NO                           REF_NO,              " +  "     main1.RFQ_REF_NO                           REFNO,               " +  "     TO_CHAR(main1.VALID_TO,'" + dateFormat + "')       SHIP_DATE,           " +  "     tran1.PORT_MST_POL_FK                      POL_PK,              " +  "     portpol1.PORT_ID                           POL_ID,              " +  "     tran1.PORT_MST_POD_FK                      POD_PK,              " +  "     portpod1.PORT_ID                           POD_ID,              " +  "     main1.OPERATOR_MST_FK                      OPER_PK,             " +  "     opr1.OPERATOR_ID                           OPER_ID,             " +  "     opr1.OPERATOR_NAME                         OPER_NAME,           " +  "     cont1.CONTAINER_TYPE_MST_FK                CNTR_PK,             " +  "     cntr1.CONTAINER_TYPE_MST_ID                CNTR_ID,             " +  "     main1.TEUS_VOL                             QUANTITY,            " +  "     cmdt2.COMMODITY_MST_pK                     COMM_PK,             " +  "     cmdt2.COMMODITY_ID                         COMM_ID,             " +  "     NVL(" + exchQueryFCL + ",0)                ALL_IN_TARIFF,       " +  "     NVL(" + exchQueryFCL + ",0)                OPERATOR_RATE,       " +  "     NULL                                       TARIFF,              " +  "     NULL                                       NET,                 " +  "     'false'                                    SELECTED,            " +  "   " + SourceType.SpotRate + "                  PRIORITYORDER        " +  "    from                                                             " +  "     RFQ_SPOT_RATE_SEA_TBL          main1,                           " +  "     RFQ_SPOT_TRN_SEA_FCL_LCL       tran1,                           " +  "     PORT_MST_TBL                   portpol1,                        " +  "     PORT_MST_TBL                   portpod1,                        " +  "     OPERATOR_MST_TBL               opr1,                            ";
                    // & vbCrLf _
                    //Snigdharani - 04/11/2008 - Removing v-array
                    //& "     TABLE(tran1.CONTAINER_DTL_FCL) (+) cont1,                        " & vbCrLf _
                    strSQL = strSQL + "     RFQ_SPOT_TRN_SEA_CONT_DET cont1,                        " +  "     CONTAINER_TYPE_MST_TBL         cntr1,                           " +  "     commodity_group_mst_tbl              cmdt1, COMMODITY_MST_TBL         cmdt2" +  "    where                                                                " +  "            main1.RFQ_SPOT_SEA_PK       = tran1.RFQ_SPOT_SEA_FK               " +  "     AND    main1.CARGO_TYPE            = 1                                   " +  "     AND    main1.ACTIVE                = 1                                   " +  "     AND    main1.APPROVED              = 1                                   " +  "     AND    CONT1.RFQ_SPOT_SEA_TRN_FK   = tran1.RFQ_SPOT_SEA_TRN_PK           " +  "     AND    tran1.PORT_MST_POL_FK       = portpol1.PORT_MST_PK(+)             " +  "     AND    tran1.PORT_MST_POD_FK       = portpod1.PORT_MST_PK(+)             " +  "     AND    main1.OPERATOR_MST_FK       = opr1.OPERATOR_MST_PK(+)             " +  "     AND    cont1.CONTAINER_TYPE_MST_FK = cntr1.CONTAINER_TYPE_MST_PK(+)      " +  "     AND    main1.commodity_group_fk      = cmdt1.commodity_group_pk           " +  "     AND    cmdt1.COMMODITY_GROUP_PK    = " + Convert.ToString(CommodityGroup) + "        " +  "     and main1.commodity_mst_fk = cmdt2.commodity_mst_pk(+)       " +  "     AND    (main1.CUSTOMER_MST_FK       = " + Convert.ToString(CustNo) + " OR main1.CUSTOMER_MST_FK IS NULL) " +  "     AND    (tran1.PORT_MST_POL_FK, tran1.PORT_MST_POD_FK,                    " +  "                                   cont1.CONTAINER_TYPE_MST_FK )              " +  "              in ( " + Convert.ToString(SectorContainers) + " )                           " +  "     AND    TO_DATE('" + ShipDate + "','" + dateFormat + "') BETWEEN                  " +  "            main1.VALID_FROM   AND   main1.VALID_TO                           ";
                }
                else
                {
                    strSQL = "    Select         DISTINCT                                 " +  "     main1.RFQ_SPOT_SEA_PK                      PK,                  " +  "  " + SRC(SourceType.SpotRate) + "              TYPE,                " +  "     main1.RFQ_REF_NO                           REF_NO,              " +  "     TO_CHAR(main1.VALID_TO,'" + dateFormat + "')       SHIP_DATE,           " +  "     tran1.PORT_MST_POL_FK                      POL_PK,              " +  "     portpol1.PORT_ID                           POL_ID,              " +  "     tran1.PORT_MST_POD_FK                      POD_PK,              " +  "     portpod1.PORT_ID                           POD_ID,              " +  "     main1.OPERATOR_MST_FK                      OPER_PK,             " +  "     opr1.OPERATOR_ID                           OPER_ID,             " +  "     opr1.OPERATOR_NAME                         OPER_NAME,           " +  "     main1.COMMODITY_MST_FK                     COMM_PK,             " +  "     cmdt2.COMMODITY_ID                         COMM_ID,             " +  "     tran1.LCL_BASIS                            LCL_BASIS,           " +  "     NVL(dim1.DIMENTION_ID,'')                  DIMENTION_ID,        " +  "     0                                          WEIGHT,              " +  "     0                                          VOLUME,              " +  "     NVL(" + exchQueryLCL + ",0)                ALL_IN_TARIFF,       " +  "     'false'                                    SELECTED,            " +  "     NVL(" + exchQueryLCL + ",0)                OPERATOR_RATE,       " +  "     " + SourceType.SpotRate + "                PRIORITYORDER        " +  "    from                                                             " +  "     RFQ_SPOT_RATE_SEA_TBL          main1,                           " +  "     RFQ_SPOT_TRN_SEA_FCL_LCL       tran1,                           " +  "     PORT_MST_TBL                   portpol1,                        " +  "     PORT_MST_TBL                   portpod1,                        " +  "     OPERATOR_MST_TBL               opr1,                            " +  "     commodity_group_mst_tbl        cmdt1, COMMODITY_MST_TBL         cmdt2," +  "     DIMENTION_UNIT_MST_TBL         dim1                             " +  "    where                                                                " +  "            main1.RFQ_SPOT_SEA_PK       = tran1.RFQ_SPOT_SEA_FK              " +  "     AND    main1.CARGO_TYPE            = 2                                  " +  "     AND    main1.ACTIVE                = 1                                  " +  "     AND    main1.APPROVED              = 1                                  " +  "     AND    tran1.PORT_MST_POL_FK       = portpol1.PORT_MST_PK(+)            " +  "     AND    tran1.PORT_MST_POD_FK       = portpod1.PORT_MST_PK(+)            " +  "     AND    main1.OPERATOR_MST_FK       = opr1.OPERATOR_MST_PK(+)            " +  "     AND    main1.commodity_group_fk      = cmdt1.commodity_group_pk           " +  "     and    main1.commodity_mst_fk = cmdt2.commodity_mst_pk(+)       " +  "     AND    cmdt1.COMMODITY_GROUP_PK    = " + Convert.ToString(CommodityGroup) + "       " +  "     AND    (main1.CUSTOMER_MST_FK       = " + Convert.ToString(CustNo) + " OR main1.CUSTOMER_MST_FK IS NULL) " +  "     AND    tran1.LCL_BASIS             = dim1.DIMENTION_UNIT_MST_PK(+)      " +  "     AND    (tran1.PORT_MST_POL_FK, tran1.PORT_MST_POD_FK,tran1.LCL_BASIS )  " +  "              in ( " + Convert.ToString(SectorContainers) + " )                          " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN      " +  "            main1.VALID_FROM   AND   main1.VALID_TO                         ";
                }
                strSQL = strSQL.Replace("   ", " ");
                strSQL = strSQL.Replace("  ", " ");
                return strSQL;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion

        #region " Customer Contract Query."

        private string CustContQuery(bool forFCL = true, string CustNo = "", string SectorContainers = "", string CommodityGroup = "", string ShipDate = "")
        {
            try
            {
                string strSQL = null;
                string exchQueryFCL = null;
                string exchQueryLCL = null;
                string OperatorRate = null;
                //The following queries are changed by Snigdharani on 28/11/2008 avoid error due to exchange rate
                //and v2.exch_rate_type_fk = 1
                if (forFCL)
                {
                    exchQueryFCL = " NVL(( Case When NVL(tran2.APPROVED_ALL_IN_RATE,0) > 0 Then     " + "            tran2.CURRENT_BOF_RATE                     else        " + "            tran2.APPROVED_BOF_RATE End                            " + "         )* get_ex_rate(tran2.currency_mst_fk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",sysdate) ,0 ) + " + " ( Select nvl(Sum(NVL(APP_SURCHARGE_AMT * get_ex_rate(tran2.currency_mst_fk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",sysdate),0)),0)" +  "       from  CONT_CUST_TRN_SEA_TBL t2,                     " +  "               CONT_SUR_CHRG_SEA_TBL f2                                        " +  "       where t2.CONT_CUST_TRN_SEA_PK  = tran2.CONT_CUST_TRN_SEA_PK  and         " +  "               t2.CONT_CUST_TRN_SEA_PK  = f2.CONT_CUST_TRN_SEA_FK and          " +  "               f2.CHECK_FOR_ALL_IN_RT   = 1                                 ";
                    //"               f2.CURRENCY_MST_FK =  v2.CURRENCY_MST_FK AND                    " & vbCrLf & _
                    //"               ROUND(sysdate-0.5) between v2.FROM_DATE and v2.TO_DATE          " & vbCrLf & _
                    // vv5.exch_rate_type_fk = 1 AND
                    exchQueryFCL = exchQueryFCL + "    )    +                                                                     " +  " ( Select NVL(Sum(NVL(FCL_REQ_RATE * get_ex_rate(tran2.currency_mst_fk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",sysdate),0 ) ),0) from               " +  "         TARIFF_TRN_SEA_FCL_LCL tt5,                      " +  "         TARIFF_MAIN_SEA_TBL mm5,";
                    // V_EXCHANGE_RATE vv5,TABLE(tt5.CONTAINER_DTL_FCL) (+) cc5
                    //Snigdharani - 29/10/2008 - Removing V-Array
                    //Deleted following line - Snigdharani
                    //tt5.CURRENCY_MST_FK       = vv5.CURRENCY_MST_FK          AND
                    //ROUND(sysdate-0.5) between vv5.FROM_DATE and vv5.TO_DATE AND
                    exchQueryFCL = exchQueryFCL + " TARIFF_TRN_SEA_CONT_DTL cc5         " +  "   where cc5.CONTAINER_TYPE_MST_FK = tran2.CONTAINER_TYPE_MST_FK and          " +  "         mm5.TARIFF_MAIN_SEA_PK    = tt5.TARIFF_MAIN_SEA_FK       AND          " +  "         cc5.TARIFF_TRN_SEA_FK     = tt5.TARIFF_TRN_SEA_PK        AND          " +  "         tt5.PORT_MST_POL_FK       = tran2.PORT_MST_POL_FK        AND          " +  "         tt5.PORT_MST_POD_FK       = tran2.PORT_MST_POD_FK        AND          " +  "                   " +  "         tt5.CHECK_FOR_ALL_IN_RT   = 1                            AND          " +  "                   " +  "         tran2.SUBJECT_TO_SURCHG_CHG = 1                          AND          " +  "         mm5.OPERATOR_MST_FK       = main2.OPERATOR_MST_FK        AND          " +  "         mm5.CARGO_TYPE            = 1                            AND          " +  "         mm5.ACTIVE                = 1                            AND          " +  "         mm5.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + " AND          " +  "         ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN           " +  "         tt5.VALID_FROM   AND   NVL(tt5.VALID_TO,NULL_DATE_FORMAT)   AND          " +  "         tt5.FREIGHT_ELEMENT_MST_FK not in                                     " +  "           ( Select FREIGHT_ELEMENT_MST_FK                                     " +  "              from CONT_CUST_TRN_SEA_TBL tt2, CONT_SUR_CHRG_SEA_TBL ff2 where  " +  "                   tt2.CONT_CUST_TRN_SEA_PK  = tran2.CONT_CUST_TRN_SEA_PK and  " +  "                   tt2.CONT_CUST_TRN_SEA_PK  = ff2.CONT_CUST_TRN_SEA_FK and    " +  "                   ff2.CHECK_FOR_ALL_IN_RT   = 1                               " +  "           )                                                                   " +  "  )                                                                            ";

                    OperatorRate = " ( Select nvl(Sum(NVL(FCL_APP_RATE * get_ex_rate(tran2.currency_mst_fk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",sysdate),0)),0)               " +  "   from  CONT_MAIN_SEA_TBL mx, CONT_TRN_SEA_FCL_LCL tx,";
                    // & vbCrLf & _ V_EXCHANGE_RATE vx,vx.exch_rate_type_fk = 1  AND 
                    //Snigdharani - 05/11/2008 - Removing v-array
                    //"         TABLE(tx.CONTAINER_DTL_FCL) (+) cx                                " & vbCrLf & _
                    OperatorRate = OperatorRate + "         CONT_TRN_SEA_FCL_RATES cx                       " +  "   where mx.ACTIVE                     = 1   and                         " +  "         mx.CONT_APPROVED              = 1     AND                         " +  "         mx.CARGO_TYPE                 = 1     AND                         " +  "         tx.CONT_TRN_SEA_PK = cx.CONT_TRN_SEA_FK AND                       " +  "         mx.OPERATOR_MST_FK            = main2.OPERATOR_MST_FK AND         " +  "         mx.COMMODITY_GROUP_FK         =  " + Convert.ToString(CommodityGroup) + " AND " +  "         ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "         tx.VALID_FROM   AND   NVL(tx.VALID_TO,NULL_DATE_FORMAT)          AND " +  "         tx.CONT_MAIN_SEA_FK           = mx.CONT_MAIN_SEA_PK           AND " +  "         cx.CONTAINER_TYPE_MST_FK      = tran2.CONTAINER_TYPE_MST_FK   AND " +  "         tx.PORT_MST_POL_FK            = tran2.PORT_MST_POL_FK         AND " +  "         tx.PORT_MST_POD_FK            = tran2.PORT_MST_POD_FK         AND " +  "         tx.CHECK_FOR_ALL_IN_RT        = 1 )                             ";
                    //& vbCrLf & _
                    //                            "        AND tx.CURRENCY_MST_FK            = vx.CURRENCY_MST_FK AND            " & vbCrLf & _
                    //                            "         ROUND(sysdate-0.5) between vx.FROM_DATE and vx.TO_DATE )            "
                    //Following lines are deleted - Snigdharani - 28/11/2008
                    //V_EXCHANGE_RATE v2,
                    //"               f2.CURRENCY_MST_FK =  v2.CURRENCY_MST_FK AND                    " & vbCrLf & _
                    //            "               ROUND(sysdate-0.5) between v2.FROM_DATE and v2.TO_DATE          " & vbCrLf & _
                    //, V_EXCHANGE_RATE vv5
                    //  and vv5.exch_rate_type_fk = 1
                    //"         tt5.CURRENCY_MST_FK       = vv5.CURRENCY_MST_FK          AND  and v2.exch_rate_type_fk = 1         " & vbCrLf & _
                    //            "         ROUND(sysdate-0.5) between vv5.FROM_DATE and vv5.TO_DATE AND          " & vbCrLf & _
                    //, V_EXCHANGE_RATE vx
                    //and vx.exch_rate_type_fk = 1  
                    //"         ROUND(TO_DATE('" & ShipDate & "','" & dateFormat & "')-0.5) BETWEEN       " & vbCrLf & _
                    //            "           tx.VALID_FROM   AND   NVL(tx.VALID_TO,NULL_DATE_FORMAT)        AND " & vbCrLf & _
                    //"         tx.CURRENCY_MST_FK            = vx.CURRENCY_MST_FK AND            " & vbCrLf & _
                    //          "         ROUND(sysdate-0.5) between vx.FROM_DATE and vx.TO_DATE )            "
                }
                else
                {
                    exchQueryLCL = "   ( Case When NVL(tran2.APPROVED_ALL_IN_RATE,0) > 0 Then       " + "            tran2.CURRENT_BOF_RATE                     else        " + "            tran2.APPROVED_BOF_RATE End                            " + "      ) +                                                          " + " ( Select nvl(Sum(NVL(APP_SURCHARGE_AMT * get_ex_rate(tran2.currency_mst_fk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",sysdate),0)),0)              " +  "       from  CONT_CUST_TRN_SEA_TBL t2,                     " +  "               CONT_SUR_CHRG_SEA_TBL f2                                        " +  "       where t2.CONT_CUST_TRN_SEA_PK  = tran2.CONT_CUST_TRN_SEA_PK and         " +  "               t2.CONT_CUST_TRN_SEA_PK  = f2.CONT_CUST_TRN_SEA_FK and          " +  "               f2.CHECK_FOR_ALL_IN_RT   = 1                                 " +  "    )    +                                                                     " +  " ( Select NVL(Sum(NVL(LCL_TARIFF_RATE * get_ex_rate(tran2.currency_mst_fk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",sysdate),0 ) ),0) from            " +  "         TARIFF_TRN_SEA_FCL_LCL tt5,                      " +  "         TARIFF_MAIN_SEA_TBL mm5                                               " +  "   where                                                                       " +  "         mm5.TARIFF_MAIN_SEA_PK    = tt5.TARIFF_MAIN_SEA_FK     AND          " +  "         tt5.PORT_MST_POL_FK       = tran2.PORT_MST_POL_FK        AND          " +  "         tt5.PORT_MST_POD_FK       = tran2.PORT_MST_POD_FK        AND          " +  "         tt5.CHECK_FOR_ALL_IN_RT   = 1                            AND          " +  "         tran2.SUBJECT_TO_SURCHG_CHG = 1                          AND          " +  "         mm5.OPERATOR_MST_FK       = main2.OPERATOR_MST_FK        AND          " +  "         mm5.CARGO_TYPE            = 2                            AND          " +  "         mm5.ACTIVE                = 1                            AND          " +  "         mm5.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + " AND          " +  "         ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN           " +  "         tt5.VALID_FROM   AND   NVL(tt5.VALID_TO,NULL_DATE_FORMAT)   AND          " +  "         tt5.FREIGHT_ELEMENT_MST_FK not in                                     " +  "           ( Select FREIGHT_ELEMENT_MST_FK                                     " +  "              from CONT_CUST_TRN_SEA_TBL tt2, CONT_SUR_CHRG_SEA_TBL ff2 where  " +  "                   tt2.CONT_CUST_TRN_SEA_PK  = tran2.CONT_CUST_TRN_SEA_PK and  " +  "                   tt2.CONT_CUST_TRN_SEA_PK  = ff2.CONT_CUST_TRN_SEA_FK and    " +  "                   ff2.CHECK_FOR_ALL_IN_RT   = 1                               " +  "           )                                                                   " +  "  )                                                                            ";

                    OperatorRate = " ( Select nvl(Sum(NVL(LCL_APPROVED_RATE * get_ex_rate(tran2.currency_mst_fk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",sysdate),0)),0)          " +  "   from  CONT_MAIN_SEA_TBL mx, CONT_TRN_SEA_FCL_LCL tx " +  "   where mx.ACTIVE                     = 1   AND                         " +  "         mx.CONT_APPROVED              = 1     AND                         " +  "         mx.CARGO_TYPE                 = 2     AND                         " +  "         mx.OPERATOR_MST_FK            = main2.OPERATOR_MST_FK AND         " +  "         mx.COMMODITY_GROUP_FK         =  " + Convert.ToString(CommodityGroup) + " AND " +  "         tx.CONT_MAIN_SEA_FK           = mx.CONT_MAIN_SEA_PK           AND " +  "         tx.LCL_BASIS                  = tran2.LCL_BASIS               AND " +  "         tx.PORT_MST_POL_FK            = tran2.PORT_MST_POL_FK         AND " +  "         tx.PORT_MST_POD_FK            = tran2.PORT_MST_POD_FK         AND " +  "         tx.CHECK_FOR_ALL_IN_RT        = 1 ) ";
                }
                //Following lines are deleted - Snigdharani - 28/11/2008
                //& "     V_EXCHANGE_RATE                Vex2                            " & vbCrLf _
                //  and vex2.exch_rate_type_fk = 1
                //& "     AND    tran2.CURRENCY_MST_FK       = Vex2.CURRENCY_MST_FK                " & vbCrLf _
                //            & "     AND    ROUND(sysdate-0.5) between Vex2.FROM_DATE and Vex2.TO_DATE        " & vbCrLf _
                //& vbCrLf _
                //  & "     AND    ROUND(TO_DATE('" & ShipDate & "','" & dateFormat & "')-0.5) BETWEEN       " & vbCrLf _
                // & "            tran2.VALID_FROM   AND   NVL(tran2.VALID_TO,NULL_DATE_FORMAT)        "
                if (forFCL)
                {
                    strSQL = "    Select      DISTINCT                                             " +  "     main2.CONT_CUST_SEA_PK                     PK,                 " +  "  " + SRC(SourceType.CustomerContract) + "      TYPE,               " +  "     main2.CONT_REF_NO                          REF_NO,             " +  "     main2.CONT_REF_NO                          REFNO,              " +  "     TO_CHAR(tran2.VALID_TO,'" + dateFormat + "')       SHIP_DATE,          " +  "     tran2.PORT_MST_POL_FK                      POL_PK,             " +  "     portpol2.PORT_ID                           POL_ID,             " +  "     tran2.PORT_MST_POD_FK                      POD_PK,             " +  "     portpod2.PORT_ID                           POD_ID,             " +  "     main2.OPERATOR_MST_FK                      OPER_PK,            " +  "     opr2.OPERATOR_ID                           OPER_ID,            " +  "     opr2.OPERATOR_NAME                         OPER_NAME,          " +  "     tran2.CONTAINER_TYPE_MST_FK                CNTR_PK,            " +  "     cntr2.CONTAINER_TYPE_MST_ID                CNTR_ID,            " +  "     tran2.EXPECTED_VOLUME                      QUANTITY,           " +  "     main2.COMMODITY_MST_FK                     COMM_PK,            " +  "     NVL(cmdt2.COMMODITY_ID,'')                 COMM_ID,            " +  "     NVL(" + exchQueryFCL + ",0)                ALL_IN_TARIFF,      " +  "     NVL(" + OperatorRate + ",0)                OPERATOR_RATE,      " +  "     NULL                                       TARIFF,             " +  "     NULL                                       NET,                " +  "     'false'                                    SELECTED,           " +  "   " + SourceType.CustomerContract + "          PRIORITYORDER       " +  "    from                                                            " +  "     CONT_CUST_SEA_TBL              main2,                          " +  "     CONT_CUST_TRN_SEA_TBL          tran2,                          " +  "     PORT_MST_TBL                   portpol2,                       " +  "     PORT_MST_TBL                   portpod2,                       " +  "     OPERATOR_MST_TBL               opr2,                           " +  "     CONTAINER_TYPE_MST_TBL         cntr2,                          " +  "     COMMODITY_MST_TBL              cmdt2                          " +  "    where                                                                " +  "            main2.CONT_CUST_SEA_PK      = tran2.CONT_CUST_SEA_FK              " +  "     AND    main2.CARGO_TYPE            = 1                                 " +  "     AND    tran2.PORT_MST_POL_FK       = portpol2.PORT_MST_PK(+)             " +  "     AND    tran2.PORT_MST_POD_FK       = portpod2.PORT_MST_PK(+)             " +  "     AND    main2.OPERATOR_MST_FK       = opr2.OPERATOR_MST_PK(+)             " +  "     AND    tran2.CONTAINER_TYPE_MST_FK = cntr2.CONTAINER_TYPE_MST_PK(+)      " +  "     AND    main2.COMMODITY_MST_FK      = cmdt2.COMMODITY_MST_PK(+)           " +  "     AND ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN           " +  "         tran2.VALID_FROM AND NVL(tran2.VALID_TO, NULL_DATE_FORMAT)           " +  "     AND    main2.COMMODITY_GROUP_MST_FK = " + Convert.ToString(CommodityGroup) + "       " +  "     AND    main2.CUSTOMER_MST_FK       =  " + Convert.ToString(CustNo) + "               " +  "     AND    (tran2.PORT_MST_POL_FK, tran2.PORT_MST_POD_FK,                    " +  "                                   tran2.CONTAINER_TYPE_MST_FK )              " +  "              in ( " + Convert.ToString(SectorContainers) + " )                           ";

                    //The following lines are delete - Snigdharani - 28/11/2008
                    //& "     V_EXCHANGE_RATE                Vex2                             " & vbCrLf _
                    //  and vex2.exch_rate_type_fk = 1
                    //& "     AND    tran2.CURRENCY_MST_FK       = Vex2.CURRENCY_MST_FK                 " & vbCrLf _
                    //       & "     AND    ROUND(sysdate-0.5) between Vex2.FROM_DATE and Vex2.TO_DATE         " & vbCrLf _

                }
                else
                {
                    strSQL = "    Select     DISTINCT                                 " +  "     main2.CONT_CUST_SEA_PK                     PK,                  " +  "  " + SRC(SourceType.CustomerContract) + "      TYPE,                " +  "     main2.CONT_REF_NO                          REF_NO,              " +  "     TO_CHAR(tran2.VALID_TO,'" + dateFormat + "')       SHIP_DATE,           " +  "     tran2.PORT_MST_POL_FK                      POL_PK,              " +  "     portpol2.PORT_ID                           POL_ID,              " +  "     tran2.PORT_MST_POD_FK                      POD_PK,              " +  "     portpod2.PORT_ID                           POD_ID,              " +  "     main2.OPERATOR_MST_FK                      OPER_PK,             " +  "     opr2.OPERATOR_ID                           OPER_ID,             " +  "     opr2.OPERATOR_NAME                         OPER_NAME,           " +  "     tran2.COMMODITY_MST_FK                     COMM_PK,             " +  "     NVL(cmdt2.COMMODITY_ID,'')                 COMM_ID,             " +  "     tran2.LCL_BASIS                            LCL_BASIS,           " +  "     NVL(dim2.DIMENTION_ID,'')                  DIMENTION_ID,        " +  "     0                                          WEIGHT,              " +  "     0                                          VOLUME,              " +  "     NVL(" + exchQueryLCL + ",0)                ALL_IN_TARIFF,       " +  "     'false'                                    SELECTED,            " +  "     NVL(" + OperatorRate + ",0)                OPERATOR_RATE,       " +  "     " + SourceType.CustomerContract + "        PRIORITYORDER        " +  "    from                                                             " +  "     CONT_CUST_SEA_TBL              main2,                           " +  "     CONT_CUST_TRN_SEA_TBL          tran2,                           " +  "     PORT_MST_TBL                   portpol2,                        " +  "     PORT_MST_TBL                   portpod2,                        " +  "     OPERATOR_MST_TBL               opr2,                            " +  "     COMMODITY_MST_TBL              cmdt2,                           " +  "     DIMENTION_UNIT_MST_TBL         dim2                            " +  "    where                                                                " +  "            main2.CONT_CUST_SEA_PK      = tran2.CONT_CUST_SEA_FK             " +  "     AND    main2.CARGO_TYPE            = 2                                    " +  "     AND    tran2.PORT_MST_POL_FK       = portpol2.PORT_MST_PK(+)              " +  "     AND    tran2.PORT_MST_POD_FK       = portpod2.PORT_MST_PK(+)              " +  "     AND    main2.OPERATOR_MST_FK       = opr2.OPERATOR_MST_PK(+)              " +  "     AND    main2.COMMODITY_MST_FK      = cmdt2.COMMODITY_MST_PK(+)            " +  "     AND ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN           " +  "         tran2.VALID_FROM AND NVL(tran2.VALID_TO, NULL_DATE_FORMAT)           " +  "     AND    main2.COMMODITY_GROUP_MST_FK = " + Convert.ToString(CommodityGroup) + "        " +  "     AND    tran2.LCL_BASIS             = dim2.DIMENTION_UNIT_MST_PK(+)        " +  "     AND    (tran2.PORT_MST_POL_FK, tran2.PORT_MST_POD_FK,                     " +  "                                   tran2.LCL_BASIS )                           " +  "              in ( " + Convert.ToString(SectorContainers) + " )                            " +  "     AND    main2.CUSTOMER_MST_FK       = " + Convert.ToString(CustNo) + "                 " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN        " +  "            tran2.VALID_FROM   AND   NVL(tran2.VALID_TO,NULL_DATE_FORMAT)         ";
                }
                strSQL = strSQL.Replace("   ", " ");
                strSQL = strSQL.Replace("  ", " ");
                return strSQL;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion

        #region " Operator Tariff Query."

        private string OprTariffQuery(bool forFCL = true, string CustNo = "", string SectorContainers = "", string CommodityGroup = "", string ShipDate = "", int BBFlag = 0, string CommodityPks = "")
        {
            string strSQL = null;
            string exchQueryFCL = null;
            string exchQueryLCL = null;
            string OperatorRate = null;
            try
            {
                if (forFCL)
                {
                    exchQueryFCL = " ( Select nvl(Sum(NVL(FCL_REQ_RATE * EXCHANGE_RATE,0)),0)               " +  "   from  TARIFF_TRN_SEA_FCL_LCL t5, V_EXCHANGE_RATE v5,";
                    //TABLE(t5.CONTAINER_DTL_FCL) (+) c5 " & vbCrLf & _
                    //Modified by Snigdharani - 29/10/2008 - Removing v-array
                    exchQueryFCL = exchQueryFCL + " TARIFF_TRN_SEA_CONT_DTL c5,                             " +  "         FREIGHT_ELEMENT_MST_TBL f5                                       " +  "   where t5.TARIFF_MAIN_SEA_FK = main5.TARIFF_MAIN_SEA_PK and v5.exch_rate_type_fk = 1 and             " +  "         c5.CONTAINER_TYPE_MST_FK = cont5.CONTAINER_TYPE_MST_FK  AND      " +  "         c5.TARIFF_TRN_SEA_FK = t5.TARIFF_TRN_SEA_PK  AND                 " +  "         t5.PORT_MST_POL_FK = tran5.PORT_MST_POL_FK AND                   " +  "         t5.PORT_MST_POD_FK = tran5.PORT_MST_POD_FK AND                   " +  "         t5.CHECK_FOR_ALL_IN_RT   = 1 AND v5.CURRENCY_MST_BASE_FK = main5.base_currency_fk " +  "         and t5.CURRENCY_MST_FK =  v5.CURRENCY_MST_FK AND                     " +  "         f5.FREIGHT_ELEMENT_MST_PK =  t5.FREIGHT_ELEMENT_MST_FK  AND      " +  "         f5.CHARGE_BASIS <> 2                                    AND      " +  "         ROUND(sysdate-0.5) between v5.FROM_DATE and v5.TO_DATE )         ";

                    OperatorRate = " ( Select nvl(Sum(NVL(FCL_APP_RATE * EXCHANGE_RATE,0)),0)               " +  "   from  CONT_MAIN_SEA_TBL m, CONT_TRN_SEA_FCL_LCL t, V_EXCHANGE_RATE v,  ";
                    // & vbCrLf & _
                    //Snigdharani - 05/11/2008 - Removing v-array
                    //"         TABLE(t.CONTAINER_DTL_FCL) (+) c                                " & vbCrLf & _
                    OperatorRate = OperatorRate + "CONT_TRN_SEA_FCL_RATES c                                 " +  "   where m.ACTIVE                     = 1   and v.exch_rate_type_fk = 1  AND                         " +  "         m.CONT_APPROVED              = 1     AND                         " +  "         m.CARGO_TYPE                 = 1     AND                         " +  "         m.OPERATOR_MST_FK            = main5.OPERATOR_MST_FK AND         " +  "         t.CONT_TRN_SEA_PK = C.CONT_TRN_SEA_FK AND                        " +  "         m.COMMODITY_GROUP_FK         =  " + Convert.ToString(CommodityGroup) + " AND " +  "         ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN      " +  "           v.FROM_DATE and v.TO_DATE                                          AND " +  "         ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN      " +  "           t.VALID_FROM   AND   NVL(t.VALID_TO,NULL_DATE_FORMAT)         AND " +  "         t.CONT_MAIN_SEA_FK           = m.CONT_MAIN_SEA_PK            AND " +  "         c.CONTAINER_TYPE_MST_FK      = cont5.CONTAINER_TYPE_MST_FK   AND " +  "         t.PORT_MST_POL_FK            = tran5.PORT_MST_POL_FK         AND " +  "         t.PORT_MST_POD_FK            = tran5.PORT_MST_POD_FK         AND " +  "         t.CHECK_FOR_ALL_IN_RT        = 1                             AND " +  "         t.CURRENCY_MST_FK            = v.CURRENCY_MST_FK AND             " +  "  v.CURRENCY_MST_BASE_FK = m.base_currency_fk) ";
                    //"         ROUND(sysdate-0.5) between v.FROM_DATE and v.TO_DATE )           "

                }

                if (!forFCL)
                {
                    if (BBFlag == 1)
                    {
                        exchQueryLCL = "( Select nvl(Sum(NVL(LCL_TARIFF_RATE * EXCHANGE_RATE,0)),0)             " +  "   from  TARIFF_TRN_SEA_FCL_LCL t5, V_EXCHANGE_RATE v5,                   " +  "         FREIGHT_ELEMENT_MST_TBL f5                                       " +  "   where t5.TARIFF_MAIN_SEA_FK = main5.TARIFF_MAIN_SEA_PK and v5.exch_rate_type_fk = 1 and             " +  "         t5.PORT_MST_POL_FK = tran5.PORT_MST_POL_FK AND                   " +  "         t5.PORT_MST_POD_FK = tran5.PORT_MST_POD_FK AND                   " +  "         t5.CHECK_FOR_ALL_IN_RT   = 1 AND v5.CURRENCY_MST_BASE_FK = main5.base_currency_fk " +  "         and T5.COMMODITY_MST_FK=CMT.COMMODITY_MST_PK  AND CMT.COMMODITY_MST_PK IN (" + CommodityPks + ") AND t5.CURRENCY_MST_FK =  v5.CURRENCY_MST_FK AND                     " +  "         f5.FREIGHT_ELEMENT_MST_PK =  t5.FREIGHT_ELEMENT_MST_FK  AND      " +  "         f5.CHARGE_BASIS <> 2                                    AND      " +  "         ROUND(sysdate-0.5) between v5.FROM_DATE and v5.TO_DATE )         ";
                    }
                    else
                    {
                        exchQueryLCL = "( Select nvl(Sum(NVL(LCL_TARIFF_RATE * EXCHANGE_RATE,0)),0)             " +  "   from  TARIFF_TRN_SEA_FCL_LCL t5, V_EXCHANGE_RATE v5,                   " +  "         FREIGHT_ELEMENT_MST_TBL f5                                       " +  "   where t5.TARIFF_MAIN_SEA_FK = main5.TARIFF_MAIN_SEA_PK and v5.exch_rate_type_fk = 1 and             " +  "         t5.PORT_MST_POL_FK = tran5.PORT_MST_POL_FK AND                   " +  "         t5.PORT_MST_POD_FK = tran5.PORT_MST_POD_FK AND                   " +  "         t5.CHECK_FOR_ALL_IN_RT   = 1 AND v5.CURRENCY_MST_BASE_FK = main5.base_currency_fk " +  "         and t5.CURRENCY_MST_FK =  v5.CURRENCY_MST_FK AND                     " +  "         f5.FREIGHT_ELEMENT_MST_PK =  t5.FREIGHT_ELEMENT_MST_FK  AND      " +  "         f5.CHARGE_BASIS <> 2                                    AND      " +  "         ROUND(sysdate-0.5) between v5.FROM_DATE and v5.TO_DATE )         ";
                    }

                    OperatorRate = " ( Select nvl(Sum(NVL(LCL_APPROVED_RATE * EXCHANGE_RATE,0)),0)          " +  "   from  CONT_MAIN_SEA_TBL m, CONT_TRN_SEA_FCL_LCL t, V_EXCHANGE_RATE v   " +  "   where m.ACTIVE                     = 1  and v.exch_rate_type_fk = 1   AND                         " +  "         m.CONT_APPROVED              = 1     AND                         " +  "         m.CARGO_TYPE                 = 2     AND                         " +  "         m.OPERATOR_MST_FK            = main5.OPERATOR_MST_FK AND         " +  "         m.COMMODITY_GROUP_FK         =  " + Convert.ToString(CommodityGroup) + " AND " +  "         ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN      " +  "           v.FROM_DATE and v.TO_DATE                                          AND " +  "         ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN      " +  "           t.VALID_FROM   AND   NVL(t.VALID_TO,NULL_DATE_FORMAT)         AND " +  "         t.CONT_MAIN_SEA_FK           = m.CONT_MAIN_SEA_PK            AND " +  "         t.LCL_BASIS                  = tran5.LCL_BASIS               AND " +  "         t.PORT_MST_POL_FK            = tran5.PORT_MST_POL_FK         AND " +  "         t.PORT_MST_POD_FK            = tran5.PORT_MST_POD_FK         AND " +  "         t.CHECK_FOR_ALL_IN_RT        = 1                             AND " +  "         t.CURRENCY_MST_FK            = v.CURRENCY_MST_FK AND             " +  "  v.CURRENCY_MST_BASE_FK = m.base_currency_fk) ";
                    //"         ROUND(sysdate-0.5) between v.FROM_DATE and v.TO_DATE )           "
                    //Operator rate query(for FCL and LCL) is Changed by Snigdharani - 04/06/2009

                }

                if (forFCL)
                {
                    strSQL = "    Select  DISTINCT                             " +  "     main5.TARIFF_MAIN_SEA_PK                   PK,                  " +  "  " + SRC(SourceType.OperatorTariff) + "        TYPE,                " +  "     main5.TARIFF_REF_NO                        REF_NO,              " +  "     main5.TARIFF_REF_NO                        REFNO,               " +  "     TO_CHAR(tran5.VALID_TO,'" + dateFormat + "')       SHIP_DATE,           " +  "     tran5.PORT_MST_POL_FK                      POL_PK,              " +  "     portpol5.PORT_ID                           POL_ID,              " +  "     tran5.PORT_MST_POD_FK                      POD_PK,              " +  "     portpod5.PORT_ID                           POD_ID,              " +  "     main5.OPERATOR_MST_FK                      OPER_PK,             " +  "     opr5.OPERATOR_ID                           OPER_ID,             " +  "     opr5.OPERATOR_NAME                         OPER_NAME,           " +  "     cont5.CONTAINER_TYPE_MST_FK                CNTR_PK,             " +  "     cntr5.CONTAINER_TYPE_MST_ID                CNTR_ID,             " +  "     NULL                                       QUANTITY,            " +  "     0                                          COMM_PK,             " +  "     ''                                         COMM_ID,             " +  "     NVL(" + exchQueryFCL + ",0)                ALL_IN_TARIFF,       " +  "     NVL(" + OperatorRate + ",0)                OPERATOR_RATE,       " +  "     NULL                                       TARIFF,              " +  "     NULL                                       NET,                 " +  "     'false'                                    SELECTED,            " +  "   " + SourceType.OperatorTariff + "            PRIORITYORDER        " +  "    from                                                             " +  "     TARIFF_MAIN_SEA_TBL            main5,                           " +  "     TARIFF_TRN_SEA_FCL_LCL         tran5,                           " +  "     PORT_MST_TBL                   portpol5,                        " +  "     PORT_MST_TBL                   portpod5,                        " +  "     OPERATOR_MST_TBL               opr5,                            ";
                    //& vbCrLf _
                    //Modified by Snigdharani - 29/10/2008 - Removing v-array
                    strSQL = strSQL + " TARIFF_TRN_SEA_CONT_DTL    cont5,                           " +  "     CONTAINER_TYPE_MST_TBL         cntr5                            " +  "     where                                                                    " +  "            main5.TARIFF_MAIN_SEA_PK    = tran5.TARIFF_MAIN_SEA_FK            " +  "     AND    main5.CARGO_TYPE            = 1                                   " +  "     AND    cont5.TARIFF_TRN_SEA_FK     = tran5.TARIFF_TRN_SEA_PK             " +  "     AND    main5.ACTIVE                = 1                                   " +  "     AND    main5.TARIFF_TYPE           = 1                                   " +  "     AND    tran5.PORT_MST_POL_FK       = portpol5.PORT_MST_PK(+)             " +  "     AND    tran5.PORT_MST_POD_FK       = portpod5.PORT_MST_PK(+)             " +  "     AND    main5.OPERATOR_MST_FK       = opr5.OPERATOR_MST_PK(+)             " +  "     AND    cont5.CONTAINER_TYPE_MST_FK = cntr5.CONTAINER_TYPE_MST_PK(+)      " +  "     AND    main5.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "        " +  "     AND    (tran5.PORT_MST_POL_FK, tran5.PORT_MST_POD_FK,                    " +  "                                   cont5.CONTAINER_TYPE_MST_FK )              " +  "              in ( " + Convert.ToString(SectorContainers) + " )                           " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            tran5.VALID_FROM   AND   NVL(tran5.VALID_TO,NULL_DATE_FORMAT)        ";
                }
                else
                {
                    if (BBFlag == 1)
                    {
                        strSQL = "    Select    DISTINCT                                  " +  "     main5.TARIFF_MAIN_SEA_PK                   PK,                  " +  "  " + SRC(SourceType.OperatorTariff) + "        TYPE,                " +  "     main5.TARIFF_REF_NO                        REF_NO,              " +  "     TO_CHAR(tran5.VALID_TO,'" + dateFormat + "')       SHIP_DATE,           " +  "     tran5.PORT_MST_POL_FK                      POL_PK,              " +  "     portpol5.PORT_ID                           POL_ID,              " +  "     tran5.PORT_MST_POD_FK                      POD_PK,              " +  "     portpod5.PORT_ID                           POD_ID,              " +  "     main5.OPERATOR_MST_FK                      OPER_PK,             " +  "     opr5.OPERATOR_ID                           OPER_ID,             " +  "     opr5.OPERATOR_NAME                         OPER_NAME,           " +  "     CMT.COMMODITY_MST_PK                       COMM_PK,             " +  "     CMT.COMMODITY_ID                           COMM_ID,             " +  "     tran5.LCL_BASIS                             LCL_BASIS,           " +  "     NVL(dim5.DIMENTION_ID,'')                  DIMENTION_ID,        " +  "     NULL                                       WEIGHT,              " +  "     0                                          VOLUME,              " +  "     NVL(" + exchQueryLCL + ",0)                ALL_IN_TARIFF,       " +  "     'false'                                    SELECTED,            " +  "     NVL(" + OperatorRate + ",0)                OPERATOR_RATE,       " +  "     " + SourceType.OperatorTariff + "          PRIORITYORDER        " +  "    from                                                             " +  "     TARIFF_MAIN_SEA_TBL            main5,    COMMODITY_MST_TBL CMT, " +  "     TARIFF_TRN_SEA_FCL_LCL         tran5,                           " +  "     PORT_MST_TBL                   portpol5,                        " +  "     PORT_MST_TBL                   portpod5,                        " +  "     OPERATOR_MST_TBL               opr5,                            " +  "     DIMENTION_UNIT_MST_TBL         dim5                             " +  "    where                                                                " +  "            main5.TARIFF_MAIN_SEA_PK    = tran5.TARIFF_MAIN_SEA_FK           " +  "     AND    main5.CARGO_TYPE            = 2                                  " +  "     AND    main5.ACTIVE                = 1                                  " +  "     AND    main5.TARIFF_TYPE           = 1                                  " +  "     AND    tran5.PORT_MST_POL_FK       = portpol5.PORT_MST_PK(+)            " +  "    AND    TRAN5.COMMODITY_MST_FK      = CMT.COMMODITY_MST_PK  AND CMT.COMMODITY_MST_PK IN (" + CommodityPks + ")              " +  "     AND    tran5.PORT_MST_POD_FK       = portpod5.PORT_MST_PK(+)            " +  "     AND    main5.OPERATOR_MST_FK       = opr5.OPERATOR_MST_PK(+)            " +  "     AND    main5.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "       " +  "     AND    tran5.LCL_BASIS             = dim5.DIMENTION_UNIT_MST_PK(+)      " +  "     AND    (tran5.PORT_MST_POL_FK, tran5.PORT_MST_POD_FK,tran5.LCL_BASIS )  " +  "              in ( " + Convert.ToString(SectorContainers) + " )                          " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN      " +  "            tran5.VALID_FROM   AND   NVL(tran5.VALID_TO,NULL_DATE_FORMAT)      ";
                    }
                    else
                    {
                        strSQL = "    Select    DISTINCT                                  " +  "     main5.TARIFF_MAIN_SEA_PK                   PK,                  " +  "  " + SRC(SourceType.OperatorTariff) + "        TYPE,                " +  "     main5.TARIFF_REF_NO                        REF_NO,              " +  "     TO_CHAR(tran5.VALID_TO,'" + dateFormat + "')       SHIP_DATE,           " +  "     tran5.PORT_MST_POL_FK                      POL_PK,              " +  "     portpol5.PORT_ID                           POL_ID,              " +  "     tran5.PORT_MST_POD_FK                      POD_PK,              " +  "     portpod5.PORT_ID                           POD_ID,              " +  "     main5.OPERATOR_MST_FK                      OPER_PK,             " +  "     opr5.OPERATOR_ID                           OPER_ID,             " +  "     opr5.OPERATOR_NAME                         OPER_NAME,           " +  "     0                                          COMM_PK,             " +  "     ''                                         COMM_ID,             " +  "     tran5.LCL_BASIS                            LCL_BASIS,           " +  "     NVL(dim5.DIMENTION_ID,'')                  DIMENTION_ID,        " +  "     NULL                                       WEIGHT,              " +  "     0                                          VOLUME,              " +  "     NVL(" + exchQueryLCL + ",0)                ALL_IN_TARIFF,       " +  "     'false'                                    SELECTED,            " +  "     NVL(" + OperatorRate + ",0)                OPERATOR_RATE,       " +  "     " + SourceType.OperatorTariff + "          PRIORITYORDER        " +  "    from                                                             " +  "     TARIFF_MAIN_SEA_TBL            main5,                           " +  "     TARIFF_TRN_SEA_FCL_LCL         tran5,                           " +  "     PORT_MST_TBL                   portpol5,                        " +  "     PORT_MST_TBL                   portpod5,                        " +  "     OPERATOR_MST_TBL               opr5,                            " +  "     DIMENTION_UNIT_MST_TBL         dim5                             " +  "    where                                                                " +  "            main5.TARIFF_MAIN_SEA_PK    = tran5.TARIFF_MAIN_SEA_FK           " +  "     AND    main5.CARGO_TYPE            = 2                                  " +  "     AND    main5.ACTIVE                = 1                                  " +  "     AND    main5.TARIFF_TYPE           = 1                                  " +  "     AND    tran5.PORT_MST_POL_FK       = portpol5.PORT_MST_PK(+)            " +  "     AND    tran5.PORT_MST_POD_FK       = portpod5.PORT_MST_PK(+)            " +  "     AND    main5.OPERATOR_MST_FK       = opr5.OPERATOR_MST_PK(+)            " +  "     AND    main5.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "       " +  "     AND    tran5.LCL_BASIS             = dim5.DIMENTION_UNIT_MST_PK(+)      " +  "     AND    (tran5.PORT_MST_POL_FK, tran5.PORT_MST_POD_FK,tran5.LCL_BASIS )  " +  "              in ( " + Convert.ToString(SectorContainers) + " )                          " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN      " +  "            tran5.VALID_FROM   AND   NVL(tran5.VALID_TO,NULL_DATE_FORMAT)      ";
                    }
                }
                strSQL = strSQL.Replace("   ", " ");
                strSQL = strSQL.Replace("  ", " ");
                return strSQL;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion

        #region " General Tariff Query."

        private string funGenTariffQuery(bool forFCL = true, string CustNo = "", string SectorContainers = "", string CommodityGroup = "", string ShipDate = "")
        {
            string strSQL = null;
            string exchQueryFCL = null;
            string exchQueryLCL = null;
            try
            {
                if (forFCL)
                {
                    exchQueryFCL = " ( Select nvl(Sum(NVL(FCL_REQ_RATE * EXCHANGE_RATE,0)),0)               " +  "   from  TARIFF_TRN_SEA_FCL_LCL t5, V_EXCHANGE_RATE v5,                   ";
                    //& vbCrLf & _
                    //Modified by Snigdharani - 29/10/2008 - Removing v-array
                    //TABLE(t5.CONTAINER_DTL_FCL) (+) c5
                    exchQueryFCL = exchQueryFCL + " TARIFF_TRN_SEA_CONT_DTL c5,                             " +  "         FREIGHT_ELEMENT_MST_TBL f5                                       " +  "   where t5.TARIFF_MAIN_SEA_FK = main5.TARIFF_MAIN_SEA_PK and v5.exch_rate_type_fk = 1 and             " +  "         c5.CONTAINER_TYPE_MST_FK = cont5.CONTAINER_TYPE_MST_FK  AND      " +  "         c5.TARIFF_TRN_SEA_FK = t5.TARIFF_TRN_SEA_PK  AND                 " +  "         t5.PORT_MST_POL_FK = tran5.PORT_MST_POL_FK AND                   " +  "         t5.PORT_MST_POD_FK = tran5.PORT_MST_POD_FK AND                   " +  "         t5.CHECK_FOR_ALL_IN_RT   = 1 AND                                 " +  "         t5.CURRENCY_MST_FK =  v5.CURRENCY_MST_FK AND                     " +  "         f5.FREIGHT_ELEMENT_MST_PK =  t5.FREIGHT_ELEMENT_MST_FK  AND      " +  "         f5.CHARGE_BASIS <> 2                                    AND      " +  "         ROUND(sysdate-0.5) between v5.FROM_DATE and v5.TO_DATE )         ";

                }

                if (!forFCL)
                {
                    exchQueryLCL = "( Select nvl(Sum(NVL(LCL_TARIFF_RATE * EXCHANGE_RATE,0)),0)             " +  "   from  TARIFF_TRN_SEA_FCL_LCL t5, V_EXCHANGE_RATE v5,                   " +  "         FREIGHT_ELEMENT_MST_TBL f5                                       " +  "   where t5.TARIFF_MAIN_SEA_FK = main5.TARIFF_MAIN_SEA_PK and v5.exch_rate_type_fk = 1 and             " +  "         t5.PORT_MST_POL_FK = tran5.PORT_MST_POL_FK AND                   " +  "         t5.PORT_MST_POD_FK = tran5.PORT_MST_POD_FK AND                   " +  "         t5.CHECK_FOR_ALL_IN_RT   = 1 AND                                 " +  "         t5.CURRENCY_MST_FK =  v5.CURRENCY_MST_FK AND                     " +  "         f5.FREIGHT_ELEMENT_MST_PK =  t5.FREIGHT_ELEMENT_MST_FK  AND      " +  "         f5.CHARGE_BASIS <> 2                                    AND      " +  "         ROUND(sysdate-0.5) between v5.FROM_DATE and v5.TO_DATE )         ";

                }

                if (forFCL)
                {
                    strSQL = "    Select  DISTINCT                             " +  "     main5.TARIFF_MAIN_SEA_PK                   PK,                  " +  "  " + SRC(SourceType.GeneralTariff) + "        TYPE,                " +  "     main5.TARIFF_REF_NO                        REF_NO,              " +  "     main5.TARIFF_REF_NO                        REFNO,               " +  "     TO_CHAR(tran5.VALID_TO,'" + dateFormat + "')       SHIP_DATE,           " +  "     tran5.PORT_MST_POL_FK                      POL_PK,              " +  "     portpol5.PORT_ID                           POL_ID,              " +  "     tran5.PORT_MST_POD_FK                      POD_PK,              " +  "     portpod5.PORT_ID                           POD_ID,              " +  "     main5.OPERATOR_MST_FK                      OPER_PK,             " +  "     opr5.OPERATOR_ID                           OPER_ID,             " +  "     opr5.OPERATOR_NAME                         OPER_NAME,           " +  "     cont5.CONTAINER_TYPE_MST_FK                CNTR_PK,             " +  "     cntr5.CONTAINER_TYPE_MST_ID                CNTR_ID,             " +  "     NULL                                       QUANTITY,            " +  "     0                                          COMM_PK,             " +  "     ''                                         COMM_ID,             " +  "     NVL(" + exchQueryFCL + ",0)                ALL_IN_TARIFF,       " +  "     NULL                                       OPERATOR_RATE,       " +  "     NULL                                       TARIFF,              " +  "     NULL                                       NET,                 " +  "     'false'                                    SELECTED,            " +  "   " + SourceType.GeneralTariff + "             PRIORITYORDER        " +  "    from                                                             " +  "     TARIFF_MAIN_SEA_TBL            main5,                           " +  "     TARIFF_TRN_SEA_FCL_LCL         tran5,                           " +  "     PORT_MST_TBL                   portpol5,                        " +  "     PORT_MST_TBL                   portpod5,                        " +  "     OPERATOR_MST_TBL               opr5,                            ";
                    //& vbCrLf _
                    //Modified by Snigdharani -29/10/2008 - Removin v-array
                    //TABLE(tran5.CONTAINER_DTL_FCL) (+) cont5,
                    strSQL = strSQL + "TARIFF_TRN_SEA_CONT_DTL cont5,                            " +  "     CONTAINER_TYPE_MST_TBL         cntr5                            " +  "    where                                                                " +  "            main5.TARIFF_MAIN_SEA_PK    = tran5.TARIFF_MAIN_SEA_FK            " +  "     AND    main5.CARGO_TYPE            = 1                                   " +  "     AND    main5.ACTIVE                = 1                                   " +  "     AND    main5.TARIFF_TYPE           = 2                                   " +  "     AND    cont5.TARIFF_TRN_SEA_FK = tran5.TARIFF_TRN_SEA_PK                 " +  "     AND    tran5.PORT_MST_POL_FK       = portpol5.PORT_MST_PK(+)             " +  "     AND    tran5.PORT_MST_POD_FK       = portpod5.PORT_MST_PK(+)             " +  "     AND    main5.OPERATOR_MST_FK       = opr5.OPERATOR_MST_PK(+)             " +  "     AND    cont5.CONTAINER_TYPE_MST_FK = cntr5.CONTAINER_TYPE_MST_PK(+)      " +  "     AND    main5.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "        " +  "     AND    (tran5.PORT_MST_POL_FK, tran5.PORT_MST_POD_FK,                    " +  "                                   cont5.CONTAINER_TYPE_MST_FK )              " +  "              in ( " + Convert.ToString(SectorContainers) + " )                           " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            tran5.VALID_FROM   AND   NVL(tran5.VALID_TO,NULL_DATE_FORMAT)        ";
                }
                else
                {
                    strSQL = "    Select    DISTINCT                                  " +  "     main5.TARIFF_MAIN_SEA_PK                   PK,                  " +  "  " + SRC(SourceType.GeneralTariff) + "        TYPE,                " +  "     main5.TARIFF_REF_NO                        REF_NO,              " +  "     TO_CHAR(tran5.VALID_TO,'" + dateFormat + "')       SHIP_DATE,           " +  "     tran5.PORT_MST_POL_FK                      POL_PK,              " +  "     portpol5.PORT_ID                           POL_ID,              " +  "     tran5.PORT_MST_POD_FK                      POD_PK,              " +  "     portpod5.PORT_ID                           POD_ID,              " +  "     main5.OPERATOR_MST_FK                      OPER_PK,             " +  "     opr5.OPERATOR_ID                           OPER_ID,             " +  "     opr5.OPERATOR_NAME                         OPER_NAME,           " +  "     0                                          COMM_PK,             " +  "     ''                                         COMM_ID,             " +  "     tran5.LCL_BASIS                            LCL_BASIS,           " +  "     NVL(dim5.DIMENTION_ID,'')                  DIMENTION_ID,        " +  "     NULL                                       WEIGHT,              " +  "     0                                          VOLUME,              " +  "     NVL(" + exchQueryLCL + ",0)                ALL_IN_TARIFF,       " +  "     'false'                                    SELECTED,            " +  "     NULL                                       OPERATOR_RATE,       " +  "     " + SourceType.GeneralTariff + "           PRIORITYORDER        " +  "    from                                                             " +  "     TARIFF_MAIN_SEA_TBL            main5,                           " +  "     TARIFF_TRN_SEA_FCL_LCL         tran5,                           " +  "     PORT_MST_TBL                   portpol5,                        " +  "     PORT_MST_TBL                   portpod5,                        " +  "     OPERATOR_MST_TBL               opr5,                            " +  "     DIMENTION_UNIT_MST_TBL         dim5                             " +  "    where                                                                " +  "            main5.TARIFF_MAIN_SEA_PK    = tran5.TARIFF_MAIN_SEA_FK           " +  "     AND    main5.CARGO_TYPE            = 2                                  " +  "     AND    main5.ACTIVE                = 1                                  " +  "     AND    main5.TARIFF_TYPE           = 2                                  " +  "     AND    tran5.PORT_MST_POL_FK       = portpol5.PORT_MST_PK(+)            " +  "     AND    tran5.PORT_MST_POD_FK       = portpod5.PORT_MST_PK(+)            " +  "     AND    main5.OPERATOR_MST_FK       = opr5.OPERATOR_MST_PK(+)            " +  "     AND    main5.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "       " +  "     AND    tran5.LCL_BASIS             = dim5.DIMENTION_UNIT_MST_PK(+)      " +  "     AND    (tran5.PORT_MST_POL_FK, tran5.PORT_MST_POD_FK,tran5.LCL_BASIS )  " +  "              in ( " + Convert.ToString(SectorContainers) + " )                          " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN      " +  "            tran5.VALID_FROM   AND   NVL(tran5.VALID_TO,NULL_DATE_FORMAT)      ";
                }
                strSQL = strSQL.Replace("   ", " ");
                strSQL = strSQL.Replace("  ", " ");
                return strSQL;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion

        #region " Manual Query."

        private string ManualQuoteQuery(bool forFCL = true, string CustNo = "", string SectorContainers = "", string CommodityPks = "", string ShipDate = "", int PostBackFlag = 0, int QuotationPk = 0)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                int i = 0;

                if (QuotationPk > 0)
                {
                    if (string.IsNullOrEmpty(CommodityPks))
                    {
                        DataSet ds = null;
                        CommodityPks = "0";
                        WorkFlow objWF = new WorkFlow();
                        sb.Append(" SELECT TRN.COMMODITY_MST_FK");
                        sb.Append("  FROM QUOTATION_SEA_TBL MAIN, QUOTATION_TRN_SEA_FCL_LCL TRN");
                        sb.Append(" WHERE MAIN.QUOTATION_SEA_PK = " + QuotationPk + "");
                        sb.Append("   AND MAIN.QUOTATION_SEA_PK = TRN.QUOTATION_SEA_FK");
                        sb.Append("");
                        ds = objWF.GetDataSet(sb.ToString());
                        if (ds.Tables[0].Rows.Count > 0)
                        {
                            for (i = 0; i <= ds.Tables[0].Rows.Count - 1; i++)
                            {
                                CommodityPks = CommodityPks + "," + ds.Tables[0].Rows[i]["COMMODITY_MST_FK"];
                            }

                        }
                        sb.Remove(0, sb.Length);
                    }

                    //sb.Append(" SELECT DISTINCT QTS.QUOTE_TRN_SEA_PK PK,")
                    //sb.Append("                'Manual' AS TYPE,")
                    //sb.Append("                QTS.Trans_Ref_No REFNO,")
                    //sb.Append("                SYSDATE SHIP_DATE,")
                    //sb.Append("                QTS.OPERATOR_MST_FK OPER_PK,")
                    //sb.Append("                OMT.OPERATOR_ID OPER_ID,")
                    //sb.Append("                QTS.COMMODITY_MST_FK COMM_PK,")
                    //sb.Append("                CMT.COMMODITY_ID COMM_ID,")
                    //sb.Append("                QTS.PACK_TYPE_FK BASIS_PK,")
                    //sb.Append("                PCT.PACK_TYPE_ID PACK_TYPE,")
                    //sb.Append("                QTS.BASIS BASIS_PK,")
                    //sb.Append("                DMT.DIMENTION_ID BASIS_ID,")
                    //sb.Append("                QTS.EXPECTED_BOXES QUANTITY,")
                    //sb.Append("                QTS.EXPECTED_WEIGHT CARGO_WT,")
                    //sb.Append("                QTS.EXPECTED_VOLUME CARGO_VOL,")
                    //sb.Append("                '' CARGO_CALC,")
                    //sb.Append("                NULL ALL_IN_TARIFF,")
                    //sb.Append("                'false' SELECTED,")
                    //sb.Append("                QTS.BUYING_RATE OPERATOR_RATE,")
                    //sb.Append("                3 PRIORITYORDER,")
                    //sb.Append("                ROWNUM SLNO,")
                    //sb.Append("                QTSO.AMOUNT OTHER_CHARGE,")
                    //sb.Append("                '' OTH_DTL")
                    //sb.Append("")
                    //sb.Append("       FROM QUOTATION_SEA_TBL         QST,")
                    //sb.Append("       QUOTATION_TRN_SEA_FCL_LCL QTS,")
                    //sb.Append("       ")
                    //sb.Append("       QUOTATION_TRN_SEA_OTH_CHRG QTSO,")
                    //sb.Append("       OPERATOR_MST_TBL           OMT,")
                    //sb.Append("       DIMENTION_UNIT_MST_TBL     DMT,")
                    //sb.Append("       COMMODITY_MST_TBL          CMT,")
                    //sb.Append("       PACK_TYPE_MST_TBL          PCT")
                    //sb.Append("")
                    //sb.Append("   WHERE QST.QUOTATION_SEA_PK = QTS.QUOTATION_SEA_FK(+)")
                    //sb.Append("   AND QTS.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK")
                    //sb.Append("   AND QTS.PACK_TYPE_FK = PCT.PACK_TYPE_MST_PK(+)")
                    //sb.Append("   AND OMT.OPERATOR_MST_PK = QTS.OPERATOR_MST_FK")
                    //sb.Append("   AND QTS.BASIS = DMT.DIMENTION_UNIT_MST_PK")
                    //sb.Append("   AND QTS.QUOTE_TRN_SEA_PK = QTSO.QUOTATION_TRN_SEA_FK(+)")
                    //sb.Append("   AND QST.QUOTATION_SEA_PK = " & QuotationPk & " ")

                    sb.Append(" SELECT Q.*,ROWNUM SLNR FROM (SELECT NULL AS PK,");
                    sb.Append("       'Manual' AS TYPE,");
                    sb.Append("       NULL AS REFNO,");
                    sb.Append("       SYSDATE SHIP_DATE,");
                    sb.Append("       NULL OPER_PK,");
                    sb.Append("       '' OPER_ID,");
                    sb.Append("       CMT.COMMODITY_MST_PK COMM_PK,");
                    sb.Append("       CMT.COMMODITY_ID COMM_ID,");
                    sb.Append("       0 PACK_PK,");
                    sb.Append("       '' PACK_TYPE,");
                    sb.Append("       NULL BASIS_PK,");
                    sb.Append("       '' BASIS_ID,");
                    sb.Append("       NULL QUANTITY,");
                    sb.Append("       NULL CARGO_WT,");
                    sb.Append("       NULL CARGO_VOL,");
                    sb.Append("       '' CARGO_CALC,");
                    sb.Append("       NULL ALL_IN_TARIFF,");
                    sb.Append("       'false' SELECTED,");
                    sb.Append("       0 OPERATOR_RATE,");
                    sb.Append("       3 PRIORITYORDER,");
                    sb.Append("       ROWNUM SLNO,");
                    sb.Append("       NULL OTHER_CHARGE,");
                    sb.Append("       '' OTH_DTL,");
                    sb.Append("       0 TOTAL_RATE");
                    sb.Append("");
                    sb.Append("  FROM COMMODITY_MST_TBL CMT");
                    sb.Append("  WHERE CMT.COMMODITY_MST_PK IN ( " + CommodityPks + " )");
                    sb.Append("   AND CMT.COMMODITY_MST_PK NOT IN");
                    sb.Append("       (SELECT QTS.COMMODITY_MST_FK");
                    sb.Append("          FROM QUOTATION_SEA_TBL          QST,");
                    sb.Append("               QUOTATION_TRN_SEA_FCL_LCL  QTS,");
                    sb.Append("               QUOTATION_TRN_SEA_OTH_CHRG QTSO,");
                    sb.Append("               OPERATOR_MST_TBL           OMT,");
                    sb.Append("               DIMENTION_UNIT_MST_TBL     DMT,");
                    sb.Append("               COMMODITY_MST_TBL          CMT,");
                    sb.Append("               PACK_TYPE_MST_TBL          PCT");
                    sb.Append("          WHERE QST.QUOTATION_SEA_PK = QTS.QUOTATION_SEA_FK(+)");
                    sb.Append("           AND QTS.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK");
                    sb.Append("           AND QTS.PACK_TYPE_FK = PCT.PACK_TYPE_MST_PK(+)");
                    sb.Append("           AND OMT.OPERATOR_MST_PK(+) = QTS.OPERATOR_MST_FK");
                    sb.Append("           AND QTS.BASIS = DMT.DIMENTION_UNIT_MST_PK");
                    sb.Append("           AND QTS.QUOTE_TRN_SEA_PK = QTSO.QUOTATION_TRN_SEA_FK(+)");
                    sb.Append("           AND QST.QUOTATION_SEA_PK =  " + QuotationPk + ")");
                    sb.Append("");
                    sb.Append("  UNION");
                    sb.Append("  SELECT DISTINCT QTS.QUOTE_TRN_SEA_PK PK,");
                    sb.Append("                'Manual' AS TYPE,");
                    sb.Append("                QTS.TRANS_REF_NO REFNO,");
                    sb.Append("                SYSDATE SHIP_DATE,");
                    sb.Append("                QTS.OPERATOR_MST_FK OPER_PK,");
                    sb.Append("                OMT.OPERATOR_ID OPER_ID,");
                    sb.Append("                QTS.COMMODITY_MST_FK COMM_PK,");
                    sb.Append("                CMT.COMMODITY_ID COMM_ID,");
                    sb.Append("                QTS.PACK_TYPE_FK BASIS_PK,");
                    sb.Append("                PCT.PACK_TYPE_ID PACK_TYPE,");
                    sb.Append("                QTS.BASIS BASIS_PK,");
                    sb.Append("                DMT.DIMENTION_ID BASIS_ID,");
                    sb.Append("                QTS.EXPECTED_BOXES QUANTITY,");
                    sb.Append("                QTS.EXPECTED_WEIGHT CARGO_WT,");
                    sb.Append("                QTS.EXPECTED_VOLUME CARGO_VOL,");
                    sb.Append("                '' CARGO_CALC,");
                    sb.Append("                NULL ALL_IN_TARIFF,");
                    sb.Append("                'false' SELECTED,");
                    sb.Append("                QTS.BUYING_RATE OPERATOR_RATE,");
                    sb.Append("                3 PRIORITYORDER,");
                    sb.Append("                ROWNUM SLNO,");
                    sb.Append("                QTSO.AMOUNT OTHER_CHARGE,");
                    sb.Append("                '' OTH_DTL,");
                    //sb.Append("                QTS.BUYING_RATE TOTAL_RATE")
                    sb.Append(" (SELECT (SUM(F.RATE) * (CASE");
                    sb.Append("                                  WHEN DMT.DIMENTION_ID = 'W/M' THEN");
                    sb.Append("                                   CASE");
                    sb.Append("                                     WHEN QTS.EXPECTED_WEIGHT >");
                    sb.Append("                                          QTS.EXPECTED_VOLUME THEN");
                    sb.Append("                                      QTS.EXPECTED_WEIGHT");
                    sb.Append("                                     ELSE");
                    sb.Append("                                      QTS.EXPECTED_VOLUME");
                    sb.Append("                                   END");
                    sb.Append("                                  WHEN DMT.DIMENTION_ID = 'UNIT' THEN");
                    sb.Append("                                   QTS.EXPECTED_BOXES");
                    sb.Append("                                  WHEN DMT.DIMENTION_ID = 'MT' THEN");
                    sb.Append("                                   QTS.EXPECTED_WEIGHT");
                    sb.Append("                                  WHEN DMT.DIMENTION_ID = 'CBM' THEN");
                    sb.Append("                                   QTS.EXPECTED_VOLUME");
                    sb.Append("                                  ELSE");
                    sb.Append("                                   QTS.EXPECTED_BOXES");
                    sb.Append("                                END))");
                    sb.Append("                           FROM QUOTATION_TRN_SEA_FRT_DTLS F");
                    sb.Append("                          WHERE F.QUOTE_TRN_SEA_FK = QTS.QUOTE_TRN_SEA_PK) TOTAL_RATE");
                    sb.Append("   FROM QUOTATION_SEA_TBL          QST,");
                    sb.Append("       QUOTATION_TRN_SEA_FCL_LCL  QTS,");
                    sb.Append("       QUOTATION_TRN_SEA_OTH_CHRG QTSO,");
                    sb.Append("       OPERATOR_MST_TBL           OMT,");
                    sb.Append("       DIMENTION_UNIT_MST_TBL     DMT,");
                    sb.Append("       COMMODITY_MST_TBL          CMT,");
                    sb.Append("       PACK_TYPE_MST_TBL          PCT");
                    sb.Append("   WHERE QST.QUOTATION_SEA_PK = QTS.QUOTATION_SEA_FK(+)");
                    sb.Append("   AND QTS.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK");
                    sb.Append("   AND QTS.PACK_TYPE_FK = PCT.PACK_TYPE_MST_PK(+)");
                    sb.Append("   AND OMT.OPERATOR_MST_PK(+)= QTS.OPERATOR_MST_FK");
                    sb.Append("   AND QTS.BASIS = DMT.DIMENTION_UNIT_MST_PK");
                    sb.Append("   AND QTS.QUOTE_TRN_SEA_PK = QTSO.QUOTATION_TRN_SEA_FK(+)");
                    sb.Append("        AND QST.QUOTATION_SEA_PK =  " + QuotationPk + "");
                    sb.Append(")Q");



                }
                else
                {
                    sb.Append("SELECT NULL AS PK,");
                    sb.Append("       'Manual' AS TYPE,");
                    sb.Append("       NULL AS REFNO,");
                    sb.Append("       SYSDATE SHIP_DATE,");
                    sb.Append("       NULL OPER_PK,");
                    sb.Append("       '' OPER_ID,");
                    sb.Append("       CMT.COMMODITY_MST_PK COMM_PK,");
                    sb.Append("       CMT.COMMODITY_ID COMM_ID,");
                    sb.Append("       0 PACK_PK,");
                    sb.Append("       '' PACK_TYPE,");
                    sb.Append("       NULL BASIS_PK,");
                    sb.Append("       '' BASIS_ID,");
                    sb.Append("       '' QUANTITY,");
                    sb.Append("       '' CARGO_WT,");
                    sb.Append("       '' CARGO_VOL,");
                    sb.Append("       '' CARGO_CALC,");
                    sb.Append("       NULL ALL_IN_TARIFF,");
                    sb.Append("       'false' SELECTED,");
                    sb.Append("       '0.00' OPERATOR_RATE,");
                    sb.Append("       3 PRIORITYORDER,");
                    sb.Append("       ROWNUM SLNO,");
                    sb.Append("       '' OTHER_CHARGE,");
                    sb.Append("       '' OTH_DTL,");
                    sb.Append("       '0.00' TOTAL_RATE,");
                    sb.Append("       ROWNUM SLNR");
                    sb.Append("  FROM COMMODITY_MST_TBL CMT");
                    sb.Append(" WHERE ");
                    if (string.IsNullOrEmpty(CommodityPks))
                    {
                        CommodityPks = " 0";
                    }

                    sb.Append("  CMT.COMMODITY_MST_PK IN ( " + CommodityPks + " )");
                    if (PostBackFlag == 0)
                    {
                        sb.Append(" AND 1=2 ");
                    }
                    else
                    {
                        sb.Append(" AND 1=1");
                    }
                }

                return sb.ToString();
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }


        private string ManualQuoteFreightQuery(bool forFCL = true, string CustNo = "", string SectorContainers = "", string CommodityGroup = "", string ShipDate = "", string CommodityPks = "", int PostBackFlag = 0, int QuotationPk = 0, int QuotStatus = 0, int From_Flag = 0)
        {

            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                int i = 0;
                WorkFlow objWF = new WorkFlow();
                int QtStatus = 0;
                if (QuotationPk > 0)
                {
                    if (string.IsNullOrEmpty(CommodityPks))
                    {
                        DataSet ds = null;
                        CommodityPks = "0";
                        sb.Append(" SELECT TRN.COMMODITY_MST_FK");
                        sb.Append("  FROM QUOTATION_SEA_TBL MAIN, QUOTATION_TRN_SEA_FCL_LCL TRN");
                        sb.Append(" WHERE MAIN.QUOTATION_SEA_PK = " + QuotationPk + "");
                        sb.Append("   AND MAIN.QUOTATION_SEA_PK = TRN.QUOTATION_SEA_FK");
                        sb.Append("");
                        ds = objWF.GetDataSet(sb.ToString());
                        if (ds.Tables[0].Rows.Count > 0)
                        {
                            for (i = 0; i <= ds.Tables[0].Rows.Count - 1; i++)
                            {
                                CommodityPks = CommodityPks + "," + ds.Tables[0].Rows[i]["COMMODITY_MST_FK"];
                            }

                        }
                        sb.Remove(0, sb.Length);
                    }
                    //Added by Faheem
                    //Dim strStatus As String = "SELECT Q.STATUS FROM QUOTATION_SEA_TBL Q WHERE Q.QUOTATION_SEA_PK= " & QuotationPk
                    //QtStatus = CType(objWF.ExecuteScaler(strStatus), Int32)
                    //End
                    sb.Append(" SELECT QTS.TRANS_REF_NO REFNO,");
                    sb.Append("       QTS.BASIS BASIS_PK,");
                    sb.Append("       ('AYZ'|| '$$;' || OFEMT.CREDIT) COMMODITY,");
                    sb.Append("       QTSD.FREIGHT_ELEMENT_MST_FK FREIGHT_ELEMENT_MST_PK,");
                    sb.Append("       OFEMT.FREIGHT_ELEMENT_ID FREIGHT_ELEMENT_ID,");
                    sb.Append("       DECODE(OFEMT.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit') CHARGE_BASIS,");
                    //sb.Append("      QTSD.CHECK_FOR_ALL_IN_RT SEL,")
                    sb.Append("       DECODE (QTSD.CHECK_FOR_ALL_IN_RT,1,'true',0,'false') SEL,");
                    sb.Append("       DECODE (QTSD.CHECK_ADVATOS,1,'true',0,'false') ADVATOS,");
                    //'for Advatos
                    sb.Append("       QTSD.CURRENCY_MST_FK CURRENCY_MST_PK,");
                    sb.Append("       OCUMT.CURRENCY_ID CURRENCY_ID,");
                    sb.Append("       OCUMT.CURRENCY_NAME CURRENCY_NAME,");
                    sb.Append("       QTSD.QUOTED_MIN_RATE QUOTED_MIN_RATE,");
                    sb.Append("       QTSD.QUOTED_RATE RATE,");
                    sb.Append("    ROUND(QTSD.ROE, 6) EXCHANGE_RATE,");
                    //sb.Append("        NVL(NVL(QTSD.ROE, 1) * NVL(QTSD.RATE, 0), 0) AMOUNT,")
                    //sb.Append(" NVL(NVL(QTSD.ROE, 1) * NVL(CASE")
                    sb.Append(" NVL(NVL(CASE");
                    sb.Append("                                     WHEN nvl(QTSD.QUOTED_MIN_RATE,0) > nvl(QTSD.QUOTED_RATE,0) THEN");
                    // sb.Append("                                      QTSD.QUOTED_MIN_RATE")
                    sb.Append("          (QTSD.QUOTED_MIN_RATE * NVL(DECODE(OFEMT.CREDIT, 0, -1, 1, 1), 1))");
                    sb.Append("                                     ELSE");
                    // sb.Append("                                      QTSD.QUOTED_RATE")
                    sb.Append("                                       (QTSD.QUOTED_RATE * NVL(DECODE(OFEMT.CREDIT,NULL,1, 0, -1, 1, 1), 1))");

                    sb.Append("                                   END,");
                    sb.Append("                                   0),");
                    sb.Append("            0) AMOUNT,");
                    sb.Append(" NVL(NVL(CASE");
                    sb.Append("                                     WHEN nvl(QTSD.QUOTED_MIN_RATE,0) > nvl(QTSD.QUOTED_RATE,0) THEN");
                    sb.Append("                                      QTSD.RATE");
                    sb.Append("                                     ELSE");
                    sb.Append("                                      QTSD.RATE");
                    sb.Append("                                   END,");
                    sb.Append("                                   0),");
                    sb.Append("            0) FINAL_AMOUNT,");
                    //sb.Append("            0 FINAL_AMOUNT,")
                    sb.Append("       QTSD.QUOTE_TRN_SEA_FRT_PK BASISPK,");

                    //'sb.Append("       QTSD.PYMT_TYPE,")
                    sb.Append("   CASE");
                    sb.Append("         WHEN QTSD.PYMT_TYPE = 1 THEN");
                    sb.Append("          'PrePaid'");
                    sb.Append("         ELSE");
                    sb.Append("          'Collect'");
                    sb.Append("       END PYMT_TYPE,");

                    sb.Append("       QTS.COMMODITY_MST_FK COMM_PK");
                    sb.Append("   FROM QUOTATION_SEA_TBL          QST,");
                    sb.Append("       QUOTATION_TRN_SEA_FCL_LCL  QTS,");
                    sb.Append("       QUOTATION_TRN_SEA_FRT_DTLS QTSD,");
                    sb.Append("       OPERATOR_MST_TBL           OMT,");
                    sb.Append("       FREIGHT_ELEMENT_MST_TBL    OFEMT,");
                    sb.Append("       CURRENCY_TYPE_MST_TBL      OCUMT");
                    sb.Append("    WHERE QST.QUOTATION_SEA_PK(+) = QTS.QUOTATION_SEA_FK");
                    sb.Append("   AND OMT.OPERATOR_MST_PK(+) = QTS.OPERATOR_MST_FK");
                    sb.Append("   AND QTS.QUOTE_TRN_SEA_PK = QTSD.QUOTE_TRN_SEA_FK");
                    sb.Append("   AND QTSD.FREIGHT_ELEMENT_MST_FK(+) = OFEMT.FREIGHT_ELEMENT_MST_PK");
                    sb.Append("   AND QTSD.CURRENCY_MST_FK = OCUMT.CURRENCY_MST_PK");
                    sb.Append("   AND QST.QUOTATION_SEA_PK= " + QuotationPk + " ");
                    if (QuotStatus != 2)
                    {
                        sb.Append("  UNION ");
                        sb.Append(" SELECT NULL AS REFNO,");
                        sb.Append("                NULL AS BASIS,");
                        sb.Append("                (CMT.COMMODITY_NAME|| '$$;' || OFEMT.CREDIT) AS COMMODITY,");
                        sb.Append("                OFEMT.FREIGHT_ELEMENT_MST_PK,");
                        sb.Append("                OFEMT.FREIGHT_ELEMENT_ID,");
                        sb.Append("       DECODE(OFEMT.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit') CHARGE_BASIS,");
                        sb.Append("                NULL SEL,");
                        sb.Append("                NULL ADVATOS,");
                        sb.Append("                OCUMT.CURRENCY_MST_PK,");
                        sb.Append("                OCUMT.CURRENCY_ID,");
                        sb.Append("                ''CURRENCY_NAME,");
                        // sb.Append("                ''EXCHANGE_RATE,")
                        sb.Append("                null AS QUOTED_MIN_RATE,");
                        sb.Append("                null AS RATE,");
                        sb.Append("    1.000000 EXCHANGE_RATE,");
                        sb.Append("     0.00 AMOUNT,");
                        sb.Append("     0.00 FINAL_AMOUNT,");
                        sb.Append("                NULL AS BASISPK,");
                        sb.Append("                'PrePaid' PYMT_TYPE,");
                        sb.Append("                CMT.COMMODITY_MST_PK COMM_PK");
                        sb.Append("   FROM    FREIGHT_ELEMENT_MST_TBL OFEMT,");
                        sb.Append("       CURRENCY_TYPE_MST_TBL   OCUMT,");
                        sb.Append("       COMMODITY_MST_TBL CMT");
                        sb.Append(" WHERE ");

                        sb.Append("   OFEMT.BUSINESS_TYPE IN (2, 3)");
                        sb.Append("   AND OCUMT.CURRENCY_MST_PK = " + HttpContext.Current.Session["CURRENCY_MST_PK"]);
                        sb.Append("   AND OFEMT.ACTIVE_FLAG = 1");
                        //'NOT IN
                        sb.Append(" AND  OFEMT.FREIGHT_ELEMENT_MST_PK NOT IN (SELECT  qtsD.Freight_Element_Mst_Fk ");

                        sb.Append("  FROM QUOTATION_SEA_TBL          QST,");
                        sb.Append("       QUOTATION_TRN_SEA_FCL_LCL  QTS,");
                        sb.Append("       QUOTATION_TRN_SEA_FRT_DTLS QTSD");
                        sb.Append(" WHERE QST.QUOTATION_SEA_PK(+) = QTS.QUOTATION_SEA_FK");
                        sb.Append("   AND QTS.QUOTE_TRN_SEA_PK = QTSD.QUOTE_TRN_SEA_FK(+)");
                        sb.Append("   AND QTS.Commodity_Mst_Fk=CMT.COMMODITY_MST_PK");
                        sb.Append("   AND QST.QUOTATION_SEA_PK = " + QuotationPk + ") ");

                        sb.Append("   AND nvl(OFEMT.CHARGE_TYPE,0) <> 3 ");
                        if (string.IsNullOrEmpty(CommodityPks))
                        {
                            CommodityPks = " 0";
                        }
                        sb.Append("   AND CMT.COMMODITY_MST_PK IN (" + CommodityPks + ")");
                        sb.Append(" AND 1=1");
                    }




                }
                else
                {
                    sb.Append(" SELECT NULL AS REFNO,");
                    sb.Append("                NULL AS BASIS,");
                    //'changed for cr/dr sb.Append("                CMT.COMMODITY_NAME AS COMMODITY,")
                    sb.Append(" (CMT.COMMODITY_NAME || '$$;' || OFEMT.CREDIT) AS COMMODITY,");
                    sb.Append("                OFEMT.FREIGHT_ELEMENT_MST_PK,");
                    sb.Append("                OFEMT.FREIGHT_ELEMENT_ID,");
                    sb.Append("       DECODE(OFEMT.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit') CHARGE_BASIS,");
                    sb.Append("                DECODE(0, 1, 'true', 'false') SEL,");
                    sb.Append("                DECODE(0, 1, 'true', 'false') ADVATOS,");
                    sb.Append("                OCUMT.CURRENCY_MST_PK,");
                    sb.Append("                OCUMT.CURRENCY_ID,");
                    sb.Append("                ''CURRENCY_NAME,");
                    sb.Append("                0.00 AS QUOTED_MIN_RATE,");
                    sb.Append("                0.00 AS RATE,");
                    sb.Append("                1.000000 EXCHANGE_RATE,");
                    sb.Append("                0.00 AMOUNT,");
                    sb.Append("                0.00 FINAL_AMOUNT,");
                    sb.Append("                NULL AS BASISPK,");
                    //'Export
                    if (From_Flag == 0)
                    {
                        sb.Append("                'PrePaid' AS PYMT_TYPE,");
                        //'Import
                    }
                    else
                    {
                        sb.Append("                'Collect' AS PYMT_TYPE,");
                    }
                    sb.Append("                CMT.COMMODITY_MST_PK COMM_PK");
                    sb.Append("   FROM    FREIGHT_ELEMENT_MST_TBL OFEMT,");
                    sb.Append("       CURRENCY_TYPE_MST_TBL   OCUMT,");
                    sb.Append("       COMMODITY_MST_TBL CMT");
                    sb.Append(" WHERE ");

                    sb.Append("   OFEMT.BUSINESS_TYPE IN (2, 3)");
                    sb.Append("   AND OCUMT.CURRENCY_MST_PK = " + HttpContext.Current.Session["CURRENCY_MST_PK"]);
                    sb.Append("   AND OFEMT.ACTIVE_FLAG = 1");
                    sb.Append("   AND nvl(OFEMT.CHARGE_TYPE,0) <> 3 ");
                    if (string.IsNullOrEmpty(CommodityPks))
                    {
                        CommodityPks = " 0";
                    }
                    sb.Append("   AND CMT.COMMODITY_MST_PK IN (" + CommodityPks + ")");
                    if (PostBackFlag == 0)
                    {
                        sb.Append(" AND 1=2 ");
                    }
                    else
                    {
                        sb.Append(" AND 1=1");
                    }
                    sb.Append("   ORDER BY OFEMT.preference");

                }

                return sb.ToString();
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion

        #endregion

        #region " Freight Level Query "

        #region " Quote Query "

        private string QuoteFreightQuery(bool forFCL = true, string CustNo = "", string SectorContainers = "", string CommodityGroup = "", string ShipDate = "")
        {

            try
            {
                string strSQL = null;
                string cargos = "";
                //This procedure modified by Thiyagarajan on 29/5/08 for fcl and lcl combination
                //adding by Thiyagarajan
                if (cargotypes == 2)
                {
                    cargos = " and tran3.CONTAINER_TYPE_MST_FK is null";
                }
                else if (cargotypes == 1)
                {
                    cargos = " and tran3.basis is null ";
                }
                //endif
                if (forFCL)
                {
                    strSQL = "    SELECT Q.* FROM (Select            " +  "     main3.QUOTATION_REF_NO                     REF_NO,              " +  "     tran3.PORT_MST_POL_FK                      POL_PK,              " +  "     tran3.PORT_MST_POD_FK                      POD_PK,              " +  "     tran3.CONTAINER_TYPE_MST_FK                CNTR_PK,             " +  "     frtd3.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt3.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt3.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frtd3.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     frtd3.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr3.CURRENCY_ID                          CURR_ID,             " +  "     frtd3.QUOTED_RATE                          RATE,                " +  "     frtd3.QUOTED_RATE                          QUOTERATE,           " +  "     frtd3.PYMT_TYPE                            PYTYPE               " +  "    from                                                             " +  "     QUOTATION_SEA_TBL              main3,                           " +  "     QUOTATION_TRN_SEA_FCL_LCL      tran3,                           " +  "     QUOTATION_TRN_SEA_FRT_DTLS     frtd3,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt3,                            " +  "     CURRENCY_TYPE_MST_TBL          curr3                            " +  "    where                                                                " +  "            main3.QUOTATION_SEA_PK      = tran3.QUOTATION_SEA_FK               " +  "     AND    tran3.QUOTE_TRN_SEA_PK      = frtd3.QUOTE_TRN_SEA_FK               " +  "     AND    frtd3.FREIGHT_ELEMENT_MST_FK = frt3.FREIGHT_ELEMENT_MST_PK(+)      " +  "     AND    frt3.CHARGE_BASIS           <> 2                                   " +  "     AND    frtd3.CURRENCY_MST_FK       = curr3.CURRENCY_MST_PK(+)             " +  "     AND    main3.CARGO_TYPE            in ( 1 ,3 )                            " +  "     AND    tran3.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "         " +  "     AND    main3.CUSTOMER_MST_FK       = " + Convert.ToString(CustNo) + "                 " +  "     AND    (tran3.PORT_MST_POL_FK, tran3.PORT_MST_POD_FK,                     " +  "                                   tran3.CONTAINER_TYPE_MST_FK )               " +  "              in ( " + Convert.ToString(SectorContainers) + " )                            " +  cargos +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5)  BETWEEN       " +  "            main3.QUOTATION_DATE AND (main3.QUOTATION_DATE + main3.VALID_FOR)) Q, " +  "            FREIGHT_ELEMENT_MST_TBL FRT " +  "            WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FRT_PK " +  "            ORDER BY FRT.PREFERENCE ";
                    //Added by Rabbani raeson USS Gap,introduced New column as "QUOTED_MIN_RATE"
                }
                else
                {
                    //    0       1       2        3        4      5         6        7         8       9       10
                    // REF_NO, POL_PK, POD_PK, LCLBASIS, FRT_PK, FRT_ID, FRT_NAME, SELECTED, CURR_PK, CURR_ID, RATE
                    //Added by rabbani on 24/3/07 ,To display BOF as first element in Freight Element Column using UNION ALL.
                    strSQL = "    SELECT Q.* FROM (Select            " +  "     main3.QUOTATION_REF_NO                     REF_NO,              " +  "     tran3.PORT_MST_POL_FK                      POL_PK,              " +  "     tran3.PORT_MST_POD_FK                      POD_PK,              " +  "     tran3.BASIS                                LCLBASIS,            " +  "     frtd3.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt3.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt3.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frtd3.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     frtd3.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr3.CURRENCY_ID                          CURR_ID,             " +  "     frtd3.QUOTED_MIN_RATE                      QUOTED_MIN_RATE,     " +  "     frtd3.QUOTED_RATE  RATE,COMM.COMMODITY_MST_PK COMM_PK                 " +  "    from                                                             " +  "     QUOTATION_SEA_TBL              main3,                           " +  "     QUOTATION_TRN_SEA_FCL_LCL      tran3,                           " +  "     QUOTATION_TRN_SEA_FRT_DTLS     frtd3,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt3,                            " +  "     CURRENCY_TYPE_MST_TBL          curr3,COMMODITY_MST_TBL COMM                            " +  "    where   1=2                                                             " +  "     AND     main3.QUOTATION_SEA_PK      = tran3.QUOTATION_SEA_FK               " +  "     AND    tran3.QUOTE_TRN_SEA_PK      = frtd3.QUOTE_TRN_SEA_FK               " +  "     AND    frtd3.FREIGHT_ELEMENT_MST_FK = frt3.FREIGHT_ELEMENT_MST_PK(+)      " +  "     AND    COMM.COMMODITY_MST_PK(+)=TRAN3.COMMODITY_MST_FK      " +  "     AND    frt3.CHARGE_BASIS           <> 2                                  " +  "     AND    frtd3.CURRENCY_MST_FK       = curr3.CURRENCY_MST_PK(+)             " +  "     AND    main3.CARGO_TYPE           in ( 2 , 3)                              " +  "     AND    tran3.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "         " +  "     AND    main3.CUSTOMER_MST_FK       = " + Convert.ToString(CustNo) + "                 " +  "     AND    (tran3.PORT_MST_POL_FK, tran3.PORT_MST_POD_FK,                     " +  "                                   tran3.BASIS )                               " +  "              in ( " + Convert.ToString(SectorContainers) + " )                            " +  cargos +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5)  BETWEEN       " +  "            main3.QUOTATION_DATE AND (main3.QUOTATION_DATE + main3.VALID_FOR)  " +  "     AND    frt3.FREIGHT_ELEMENT_MST_PK IN (SELECT PARM.FRT_BOF_FK  FROM PARAMETERS_TBL PARM) " +  "     UNION ALL " +  "     Select            " +  "     main3.QUOTATION_REF_NO                     REF_NO,              " +  "     tran3.PORT_MST_POL_FK                      POL_PK,              " +  "     tran3.PORT_MST_POD_FK                      POD_PK,              " +  "     tran3.BASIS                                LCLBASIS,            " +  "     frtd3.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt3.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt3.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frtd3.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     frtd3.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr3.CURRENCY_ID                          CURR_ID,             " +  "     frtd3.QUOTED_MIN_RATE                      QUOTED_MIN_RATE,     " +  "     frtd3.QUOTED_RATE RATE   ,COMM.COMMODITY_MST_PK COMM_PK              " +  "     from                                                             " +  "     QUOTATION_SEA_TBL              main3,                           " +  "     QUOTATION_TRN_SEA_FCL_LCL      tran3,                           " +  "     QUOTATION_TRN_SEA_FRT_DTLS     frtd3,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt3,                            " +  "     CURRENCY_TYPE_MST_TBL          curr3  ,COMMODITY_MST_TBL COMM                          " +  "    where    1=2                                                            " +  "     AND    main3.QUOTATION_SEA_PK      = tran3.QUOTATION_SEA_FK               " +  "     AND    tran3.QUOTE_TRN_SEA_PK      = frtd3.QUOTE_TRN_SEA_FK               " +  "     AND    frtd3.FREIGHT_ELEMENT_MST_FK = frt3.FREIGHT_ELEMENT_MST_PK(+)      " +  "     AND    frt3.CHARGE_BASIS           <> 2                                  " +  "     AND   COMM.COMMODITY_MST_PK(+)=TRAN3.COMMODITY_MST_FK            " +  "     AND    frtd3.CURRENCY_MST_FK       = curr3.CURRENCY_MST_PK(+)             " +  "     AND    main3.CARGO_TYPE            in(2,3)                                " +  "     AND    tran3.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "         " +  "     AND    main3.CUSTOMER_MST_FK       = " + Convert.ToString(CustNo) + "                 " +  cargos +  "     AND    (tran3.PORT_MST_POL_FK, tran3.PORT_MST_POD_FK,                     " +  "                                   tran3.BASIS )                               " +  "              in ( " + Convert.ToString(SectorContainers) + " )                            " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5)  BETWEEN       " +  "            main3.QUOTATION_DATE AND (main3.QUOTATION_DATE + main3.VALID_FOR)  " +  "     AND    frt3.FREIGHT_ELEMENT_MST_PK NOT IN (SELECT PARM.FRT_BOF_FK  FROM PARAMETERS_TBL PARM)) Q, " +  "            FREIGHT_ELEMENT_MST_TBL FRT " +  "            WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FRT_PK " +  "            ORDER BY FRT.PREFERENCE ";
                }
                strSQL = strSQL.Replace("   ", " ");
                strSQL = strSQL.Replace("  ", " ");
                return strSQL;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion

        #region " Enquiry Query "

        private string EnquiryFreightQuery(bool forFCL = true, string CustNo = "", string SectorContainers = "", string CommodityGroup = "", string QuoteDate = "")
        {
            try
            {
                string strSQL = null;
                if (forFCL)
                {
                    strSQL = "    Select            " +  "     main4.ENQUIRY_REF_NO                       REF_NO,              " +  "     tran4.PORT_MST_POL_FK                      POL_PK,              " +  "     tran4.PORT_MST_POD_FK                      POD_PK,              " +  "     tran4.CONTAINER_TYPE_MST_FK                CNTR_PK,             " +  "     frtd4.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt4.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt4.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frtd4.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     frtd4.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr4.CURRENCY_ID                          CURR_ID,             " +  "     frtd4.TARIFF_RATE                          RATE,                " +  "     NULL                                       QUOTERATE,           " +  "     NULL                                       PYTYPE               " +  "    from                                                             " +  "     ENQUIRY_BKG_SEA_TBL            main4,                           " +  "     ENQUIRY_TRN_SEA_FCL_LCL        tran4,                           " +  "     ENQUIRY_TRN_SEA_FRT_DTLS       frtd4,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt4,                            " +  "     CURRENCY_TYPE_MST_TBL          curr4                            " +  "    where                                                                " +  "            main4.ENQUIRY_BKG_SEA_PK    = tran4.ENQUIRY_MAIN_SEA_FK            " +  "     AND    tran4.ENQUIRY_TRN_SEA_PK    = frtd4.ENQUIRY_TRN_SEA_FK             " +  "     AND    frtd4.FREIGHT_ELEMENT_MST_FK = frt4.FREIGHT_ELEMENT_MST_PK(+)      " +  "     AND    frt4.CHARGE_BASIS           <> 2                                   " +  "     AND    frtd4.CURRENCY_MST_FK       = curr4.CURRENCY_MST_PK(+)             " +  "     AND    main4.CARGO_TYPE            = 1                                    " +  "     AND    tran4.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "         " +  "     AND    main4.CUSTOMER_MST_FK       = " + Convert.ToString(CustNo) + "                 " +  "     AND    (tran4.PORT_MST_POL_FK, tran4.PORT_MST_POD_FK,                     " +  "                                   tran4.CONTAINER_TYPE_MST_FK )               " +  "              in ( " + Convert.ToString(SectorContainers) + " )                            " +  "     AND    ROUND(TO_DATE('" + QuoteDate + "','" + dateFormat + "')-0.5)  <=           " +  "            tran4.EXPECTED_SHIPMENT                                           ";
                    //Added by rabbani on 24/3/07 ,To display BOF as first element in Freight Element Column using UNION ALL.
                }
                else
                {
                    strSQL = "    Select            " +  "     main4.ENQUIRY_REF_NO                       REF_NO,              " +  "     tran4.PORT_MST_POL_FK                      POL_PK,              " +  "     tran4.PORT_MST_POD_FK                      POD_PK,              " +  "     tran4.BASIS                                LCLBASIS,            " +  "     frtd4.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt4.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt4.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frtd4.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     frtd4.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr4.CURRENCY_ID                          CURR_ID,             " +  "     frtd4.TARIFF_RATE                          RATE                 " +  "    from                                                             " +  "     ENQUIRY_BKG_SEA_TBL            main4,                           " +  "     ENQUIRY_TRN_SEA_FCL_LCL        tran4,                           " +  "     ENQUIRY_TRN_SEA_FRT_DTLS       frtd4,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt4,                            " +  "     CURRENCY_TYPE_MST_TBL          curr4                            " +  "    where                                                                " +  "            main4.ENQUIRY_BKG_SEA_PK    = tran4.ENQUIRY_MAIN_SEA_FK          " +  "     AND    tran4.ENQUIRY_TRN_SEA_PK    = frtd4.ENQUIRY_TRN_SEA_FK           " +  "     AND    frtd4.FREIGHT_ELEMENT_MST_FK = frt4.FREIGHT_ELEMENT_MST_PK(+)    " +  "     AND    frt4.CHARGE_BASIS           <> 2                                 " +  "     AND    frtd4.CURRENCY_MST_FK       = curr4.CURRENCY_MST_PK(+)           " +  "     AND    main4.CARGO_TYPE            = 2                                  " +  "     AND    tran4.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "       " +  "     AND    main4.CUSTOMER_MST_FK       = " + Convert.ToString(CustNo) + "               " +  "     AND    (tran4.PORT_MST_POL_FK, tran4.PORT_MST_POD_FK,                   " +  "                                   tran4.LCL_BASIS )                         " +  "              in ( " + Convert.ToString(SectorContainers) + " )                          " +  "     AND    ROUND(TO_DATE('" + QuoteDate + "','" + dateFormat + "')-0.5)  <=         " +  "            tran4.EXPECTED_SHIPMENT                                          " +  "     AND    frt4.FREIGHT_ELEMENT_MST_PK IN (SELECT PARM.FRT_BOF_FK  FROM PARAMETERS_TBL PARM)" +  "     UNION ALL " +  "    Select            " +  "     main4.ENQUIRY_REF_NO                       REF_NO,              " +  "     tran4.PORT_MST_POL_FK                      POL_PK,              " +  "     tran4.PORT_MST_POD_FK                      POD_PK,              " +  "     tran4.BASIS                                LCLBASIS,            " +  "     frtd4.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt4.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt4.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frtd4.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     frtd4.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr4.CURRENCY_ID                          CURR_ID,             " +  "     frtd4.TARIFF_RATE                          RATE                 " +  "    from                                                             " +  "     ENQUIRY_BKG_SEA_TBL            main4,                           " +  "     ENQUIRY_TRN_SEA_FCL_LCL        tran4,                           " +  "     ENQUIRY_TRN_SEA_FRT_DTLS       frtd4,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt4,                            " +  "     CURRENCY_TYPE_MST_TBL          curr4                            " +  "    where                                                                " +  "            main4.ENQUIRY_BKG_SEA_PK    = tran4.ENQUIRY_MAIN_SEA_FK          " +  "     AND    tran4.ENQUIRY_TRN_SEA_PK    = frtd4.ENQUIRY_TRN_SEA_FK           " +  "     AND    frtd4.FREIGHT_ELEMENT_MST_FK = frt4.FREIGHT_ELEMENT_MST_PK(+)    " +  "     AND    frt4.CHARGE_BASIS           <> 2                                 " +  "     AND    frtd4.CURRENCY_MST_FK       = curr4.CURRENCY_MST_PK(+)           " +  "     AND    main4.CARGO_TYPE            = 2                                  " +  "     AND    tran4.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "       " +  "     AND    main4.CUSTOMER_MST_FK       = " + Convert.ToString(CustNo) + "               " +  "     AND    (tran4.PORT_MST_POL_FK, tran4.PORT_MST_POD_FK,                   " +  "                                   tran4.LCL_BASIS )                         " +  "              in ( " + Convert.ToString(SectorContainers) + " )                          " +  "     AND    ROUND(TO_DATE('" + QuoteDate + "','" + dateFormat + "')-0.5)  <=         " +  "            tran4.EXPECTED_SHIPMENT                                          " +  "     AND    frt4.FREIGHT_ELEMENT_MST_PK NOT IN (SELECT PARM.FRT_BOF_FK  FROM PARAMETERS_TBL PARM)";
                }
                strSQL = strSQL.Replace("   ", " ");
                strSQL = strSQL.Replace("  ", " ");
                return strSQL;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion

        #region " Spot Rate Query "

        private string SpotRateFreightQuery(bool forFCL = true, string CustNo = "", string SectorContainers = "", string CommodityGroup = "", string ShipDate = "")
        {
            try
            {
                string strSQL = null;
                //The following query is modified by Snigdharani on 28/11/2008 as commodity group is mandatory where as commodity is not mandatory.
                if (forFCL)
                {
                    strSQL = "    SELECT Q.* FROM(Select      " +  "     main1.RFQ_REF_NO                           REF_NO,              " +  "     tran1.PORT_MST_POL_FK                      POL_PK,              " +  "     tran1.PORT_MST_POD_FK                      POD_PK,              " +  "     cont1.CONTAINER_TYPE_MST_FK                CNTR_PK,             " +  "     tran1.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt1.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt1.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(tran1.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     tran1.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr1.CURRENCY_ID                          CURR_ID,             " +  "     cont1.FCL_APP_RATE                         RATE,                " +  "     NULL                                       QUOTERATE,           " +  "     NULL                                       PYTYPE               " +  "    from                                                             " +  "     RFQ_SPOT_RATE_SEA_TBL          main1,                           " +  "     RFQ_SPOT_TRN_SEA_FCL_LCL       tran1,                           ";
                    // & vbCrLf _
                    //Snigdharani - Removing v-array - 04/11/2008
                    //& "TABLE(tran1.CONTAINER_DTL_FCL) (+) cont1,                    " & vbCrLf _
                    strSQL = strSQL + "RFQ_SPOT_TRN_SEA_CONT_DET   cont1,                    " +  "     commodity_group_mst_tbl              cmdt1,COMMODITY_MST_TBL         cmdt2," +  "     FREIGHT_ELEMENT_MST_TBL        frt1,                            " +  "     CURRENCY_TYPE_MST_TBL          curr1                            " +  "    where                                                                " +  "            main1.RFQ_SPOT_SEA_PK       = tran1.RFQ_SPOT_SEA_FK               " +  "     AND    main1.CARGO_TYPE            = 1                                   " +  "     AND    CONT1.RFQ_SPOT_SEA_TRN_FK=tran1.RFQ_SPOT_SEA_TRN_PK               " +  "     AND    main1.ACTIVE                = 1                                   " +  "     AND    main1.APPROVED              = 1                                   " +  "     AND    tran1.FREIGHT_ELEMENT_MST_FK = frt1.FREIGHT_ELEMENT_MST_PK(+)     " +  "     AND    frt1.CHARGE_BASIS           <> 2                                  " +  "     AND    tran1.CURRENCY_MST_FK       = curr1.CURRENCY_MST_PK(+)            " +  "     AND    main1.commodity_group_fk = cmdt1.commodity_group_pk           " +  "     and    main1.commodity_mst_fk = cmdt2.commodity_mst_pk(+)           " +  "     AND    cmdt1.COMMODITY_GROUP_pK    = " + Convert.ToString(CommodityGroup) + "        " +  "     AND    (main1.CUSTOMER_MST_FK       = " + Convert.ToString(CustNo) + " OR main1.CUSTOMER_MST_FK IS NULL) " +  "     AND    (tran1.PORT_MST_POL_FK, tran1.PORT_MST_POD_FK,                    " +  "                                   cont1.CONTAINER_TYPE_MST_FK )              " +  "              in ( " + Convert.ToString(SectorContainers) + " )                           " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            main1.VALID_FROM   AND   main1.VALID_TO) Q,                       " +  "            FREIGHT_ELEMENT_MST_TBL FRT                           " +  "            WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FRT_PK                       " +  "            ORDER BY FRT.PREFERENCE                           ";
                    //Added by Rabbani raeson USS Gap,introduced New column as "Min.Rate"
                }
                else
                {
                    //Added by rabbani on 24/3/07 ,To display BOF as first element in Freight Element Column using UNION ALL.
                    strSQL = "    SELECT Q.* FROM(Select            " +  "     main1.RFQ_REF_NO                           REF_NO,              " +  "     tran1.PORT_MST_POL_FK                      POL_PK,              " +  "     tran1.PORT_MST_POD_FK                      POD_PK,              " +  "     tran1.LCL_BASIS                            LCLBASIS,            " +  "     tran1.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt1.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt1.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(tran1.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     tran1.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr1.CURRENCY_ID                          CURR_ID,             " +  "     tran1.LCL_APPROVED_MIN_RATE                MIN_RATE,            " +  "     tran1.LCL_APPROVED_RATE                    RATE                 " +  "    from                                                             " +  "     RFQ_SPOT_RATE_SEA_TBL          main1,                           " +  "     RFQ_SPOT_TRN_SEA_FCL_LCL       tran1,                           " +  "     commodity_group_mst_tbl        cmdt1,COMMODITY_MST_TBL         cmdt2," +  "     FREIGHT_ELEMENT_MST_TBL        frt1,                            " +  "     CURRENCY_TYPE_MST_TBL          curr1                            " +  "    where                                                                " +  "            main1.RFQ_SPOT_SEA_PK       = tran1.RFQ_SPOT_SEA_FK               " +  "     AND    main1.CARGO_TYPE            = 2                                   " +  "     AND    main1.ACTIVE                = 1                                   " +  "     AND    main1.APPROVED              = 1                                   " +  "     AND    tran1.FREIGHT_ELEMENT_MST_FK = frt1.FREIGHT_ELEMENT_MST_PK(+)     " +  "     AND    frt1.CHARGE_BASIS           <> 2                                  " +  "     AND    tran1.CURRENCY_MST_FK       = curr1.CURRENCY_MST_PK(+)            " +  "     AND    main1.commodity_group_fk = cmdt1.commodity_group_pk           " +  "     and    main1.commodity_mst_fk = cmdt2.commodity_mst_pk(+)           " +  "     AND    cmdt1.COMMODITY_GROUP_pK    = " + Convert.ToString(CommodityGroup) + "        " +  "     AND    (main1.CUSTOMER_MST_FK       = " + Convert.ToString(CustNo) + " OR main1.CUSTOMER_MST_FK IS NULL) " +  "     AND    (tran1.PORT_MST_POL_FK, tran1.PORT_MST_POD_FK,                    " +  "                                   tran1.LCL_BASIS )                          " +  "              in ( " + Convert.ToString(SectorContainers) + " )                           " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            main1.VALID_FROM   AND   main1.VALID_TO                           " +  "     AND    frt1.FREIGHT_ELEMENT_MST_PK IN (SELECT PARM.FRT_BOF_FK  FROM PARAMETERS_TBL PARM)" +  "     UNION ALL " +  "     Select            " +  "     main1.RFQ_REF_NO                           REF_NO,              " +  "     tran1.PORT_MST_POL_FK                      POL_PK,              " +  "     tran1.PORT_MST_POD_FK                      POD_PK,              " +  "     tran1.LCL_BASIS                            LCLBASIS,            " +  "     tran1.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt1.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt1.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(tran1.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     tran1.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr1.CURRENCY_ID                          CURR_ID,             " +  "     tran1.LCL_APPROVED_MIN_RATE                MIN_RATE,            " +  "     tran1.LCL_APPROVED_RATE                    RATE                 " +  "    from                                                             " +  "     RFQ_SPOT_RATE_SEA_TBL          main1,                           " +  "     RFQ_SPOT_TRN_SEA_FCL_LCL       tran1,                           " +  "     commodity_group_mst_tbl        cmdt1,COMMODITY_MST_TBL         cmdt2," +  "     FREIGHT_ELEMENT_MST_TBL        frt1,                            " +  "     CURRENCY_TYPE_MST_TBL          curr1                            " +  "    where                                                                " +  "            main1.RFQ_SPOT_SEA_PK       = tran1.RFQ_SPOT_SEA_FK               " +  "     AND    main1.CARGO_TYPE            = 2                                   " +  "     AND    main1.ACTIVE                = 1                                   " +  "     AND    main1.APPROVED              = 1                                   " +  "     AND    tran1.FREIGHT_ELEMENT_MST_FK = frt1.FREIGHT_ELEMENT_MST_PK(+)     " +  "     AND    frt1.CHARGE_BASIS           <> 2                                  " +  "     AND    tran1.CURRENCY_MST_FK       = curr1.CURRENCY_MST_PK(+)            " +  "     AND    main1.commodity_group_fk = cmdt1.commodity_group_pk           " +  "     and    main1.commodity_mst_fk = cmdt2.commodity_mst_pk(+)           " +  "     AND    cmdt1.COMMODITY_GROUP_pK    = " + Convert.ToString(CommodityGroup) + "        " +  "     AND    (main1.CUSTOMER_MST_FK       = " + Convert.ToString(CustNo) + " OR main1.CUSTOMER_MST_FK IS NULL) " +  "     AND    (tran1.PORT_MST_POL_FK, tran1.PORT_MST_POD_FK,                    " +  "                                   tran1.LCL_BASIS )                          " +  "              in ( " + Convert.ToString(SectorContainers) + " )                           " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            main1.VALID_FROM   AND   main1.VALID_TO                           " +  "     AND    frt1.FREIGHT_ELEMENT_MST_PK NOT IN (SELECT PARM.FRT_BOF_FK  FROM PARAMETERS_TBL PARM)) Q," +  "            FREIGHT_ELEMENT_MST_TBL FRT                           " +  "            WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FRT_PK                       " +  "            ORDER BY FRT.PREFERENCE                           ";
                }
                strSQL = strSQL.Replace("   ", " ");
                strSQL = strSQL.Replace("  ", " ");
                return strSQL;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion

        #region " Customer Contract Query "

        private string CustContFreightQuery(bool forFCL = true, string CustNo = "", string SectorContainers = "", string CommodityGroup = "", string ShipDate = "")
        {
            string strSQL = null;
            //Dim CargoType As String = "2"
            //If forFCL = True Then
            //    CargoType = "1"
            //End If

            try
            {
                string strContRefNo = null;
                string strFreightElements = null;
                string strSurcharge = null;
                string strContSectors = null;
                string strContNoLCL = null;
                string strFreightsLCL = null;
                string strSurchargeLCL = null;
                string strBasisSectors = null;

                strContRefNo = " (   Select    DISTINCT  CONT_REF_NO " +  "    from                                                             " +  "     CONT_CUST_SEA_TBL              main7,                           " +  "     CONT_CUST_TRN_SEA_TBL          tran7                            " +  "    where                                                                " +  "            main7.CONT_CUST_SEA_PK      = tran7.CONT_CUST_SEA_FK              " +  "     AND    main7.CARGO_TYPE            = 1                                   " +  "     AND    main7.COMMODITY_GROUP_MST_FK = " + Convert.ToString(CommodityGroup) + "       " +  "     AND    main7.CUSTOMER_MST_FK       =  " + Convert.ToString(CustNo) + "               " +  "     AND    tran7.PORT_MST_POL_FK       =  tran6.PORT_MST_POL_FK              " +  "     AND    tran7.PORT_MST_POD_FK       =  tran6.PORT_MST_POD_FK              " +  "     AND    tran7.CONTAINER_TYPE_MST_FK =  cont6.CONTAINER_TYPE_MST_FK        " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            tran7.VALID_FROM   AND   NVL(tran7.VALID_TO,NULL_DATE_FORMAT)        " +  "     AND    main7.OPERATOR_MST_FK =  main6.OPERATOR_MST_FK  )    ";

                strContNoLCL = " (   Select    DISTINCT  CONT_REF_NO " +  "    from                                                             " +  "     CONT_CUST_SEA_TBL              main7,                           " +  "     CONT_CUST_TRN_SEA_TBL          tran7                            " +  "    where                                                                " +  "            main7.CONT_CUST_SEA_PK      = tran7.CONT_CUST_SEA_FK              " +  "     AND    main7.CARGO_TYPE            = 2                                   " +  "     AND    main7.COMMODITY_GROUP_MST_FK = " + Convert.ToString(CommodityGroup) + "       " +  "     AND    main7.CUSTOMER_MST_FK       =  " + Convert.ToString(CustNo) + "               " +  "     AND    tran7.PORT_MST_POL_FK       =  tran6.PORT_MST_POL_FK              " +  "     AND    tran7.PORT_MST_POD_FK       =  tran6.PORT_MST_POD_FK              " +  "     AND    tran7.LCL_BASIS             =  tran6.LCL_BASIS                    " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            tran7.VALID_FROM   AND   NVL(tran7.VALID_TO,NULL_DATE_FORMAT)        " +  "     AND    main7.OPERATOR_MST_FK =  main6.OPERATOR_MST_FK  )    ";

                strFreightElements = " (   Select   DISTINCT  frtd8.FREIGHT_ELEMENT_MST_FK " +  "    from                                                             " +  "     CONT_CUST_SEA_TBL              main8,                           " +  "     CONT_CUST_TRN_SEA_TBL          tran8,                           " +  "     CONT_SUR_CHRG_SEA_TBL          frtd8                            " +  "    where                                                                " +  "            main8.CONT_CUST_SEA_PK      = tran8.CONT_CUST_SEA_FK              " +  "     AND    tran8.CONT_CUST_TRN_SEA_PK  = frtd8.CONT_CUST_TRN_SEA_FK          " +  "     AND    main8.CARGO_TYPE            = 1                                   " +  "     AND    main8.COMMODITY_GROUP_MST_FK = " + Convert.ToString(CommodityGroup) + "       " +  "     AND    main8.CUSTOMER_MST_FK       =  " + Convert.ToString(CustNo) + "               " +  "     AND    tran8.PORT_MST_POL_FK       = tran6.PORT_MST_POL_FK               " +  "     AND    tran8.PORT_MST_POD_FK       = tran6.PORT_MST_POD_FK               " +  "     AND    tran8.CONTAINER_TYPE_MST_FK = cont6.CONTAINER_TYPE_MST_FK         " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            tran8.VALID_FROM   AND   NVL(tran8.VALID_TO,NULL_DATE_FORMAT)        " +  "     AND    main8.OPERATOR_MST_FK       =  main6.OPERATOR_MST_FK          )   ";

                strFreightsLCL = " (   Select   DISTINCT  frtd8.FREIGHT_ELEMENT_MST_FK " +  "    from                                                             " +  "     CONT_CUST_SEA_TBL              main8,                           " +  "     CONT_CUST_TRN_SEA_TBL          tran8,                           " +  "     CONT_SUR_CHRG_SEA_TBL          frtd8                            " +  "    where                                                                " +  "            main8.CONT_CUST_SEA_PK      = tran8.CONT_CUST_SEA_FK              " +  "     AND    tran8.CONT_CUST_TRN_SEA_PK  = frtd8.CONT_CUST_TRN_SEA_FK          " +  "     AND    main8.CARGO_TYPE            = 2                                   " +  "     AND    main8.COMMODITY_GROUP_MST_FK = " + Convert.ToString(CommodityGroup) + "       " +  "     AND    main8.CUSTOMER_MST_FK       =  " + Convert.ToString(CustNo) + "               " +  "     AND    tran8.PORT_MST_POL_FK       = tran6.PORT_MST_POL_FK               " +  "     AND    tran8.PORT_MST_POD_FK       = tran6.PORT_MST_POD_FK               " +  "     AND    tran8.LCL_BASIS             = tran6.LCL_BASIS                     " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            tran8.VALID_FROM   AND   NVL(tran8.VALID_TO,NULL_DATE_FORMAT)        " +  "     AND    main8.OPERATOR_MST_FK       =  main6.OPERATOR_MST_FK          )   ";


                strSurcharge = " ( Select  DISTINCT  SUBJECT_TO_SURCHG_CHG  " +  "    from                                                             " +  "     CONT_CUST_SEA_TBL              main9,                           " +  "     CONT_CUST_TRN_SEA_TBL          tran9                            " +  "    where                                                                " +  "            main9.CONT_CUST_SEA_PK      = tran9.CONT_CUST_SEA_FK              " +  "     AND    main9.CARGO_TYPE            = 1                                   " +  "     AND    main9.COMMODITY_GROUP_MST_FK = " + Convert.ToString(CommodityGroup) + "       " +  "     AND    main9.CUSTOMER_MST_FK       =  " + Convert.ToString(CustNo) + "               " +  "     AND    tran9.PORT_MST_POL_FK       =  tran6.PORT_MST_POL_FK              " +  "     AND    tran9.PORT_MST_POD_FK       =  tran6.PORT_MST_POD_FK              " +  "     AND    tran9.CONTAINER_TYPE_MST_FK =  cont6.CONTAINER_TYPE_MST_FK        " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            tran9.VALID_FROM   AND   NVL(tran9.VALID_TO,NULL_DATE_FORMAT)        " +  "     AND    main9.OPERATOR_MST_FK       =  main6.OPERATOR_MST_FK          )   ";

                strSurchargeLCL = " ( Select  DISTINCT  SUBJECT_TO_SURCHG_CHG  " +  "    from                                                             " +  "     CONT_CUST_SEA_TBL              main9,                           " +  "     CONT_CUST_TRN_SEA_TBL          tran9                            " +  "    where                                                                " +  "            main9.CONT_CUST_SEA_PK      = tran9.CONT_CUST_SEA_FK              " +  "     AND    main9.CARGO_TYPE            = 2                                   " +  "     AND    main9.COMMODITY_GROUP_MST_FK = " + Convert.ToString(CommodityGroup) + "       " +  "     AND    main9.CUSTOMER_MST_FK       =  " + Convert.ToString(CustNo) + "               " +  "     AND    tran9.PORT_MST_POL_FK       =  tran6.PORT_MST_POL_FK              " +  "     AND    tran9.PORT_MST_POD_FK       =  tran6.PORT_MST_POD_FK              " +  "     AND    tran9.LCL_BASIS             =  tran6.LCL_BASIS                    " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            tran9.VALID_FROM   AND   NVL(tran9.VALID_TO,NULL_DATE_FORMAT)        " +  "     AND    main9.OPERATOR_MST_FK       =  main6.OPERATOR_MST_FK          )   ";

                strContSectors = "  Select  DISTINCT  tran10.PORT_MST_POL_FK,                               " +  "          tran10.PORT_MST_POD_FK, tran10.CONTAINER_TYPE_MST_FK                " +  "    from                                                                      " +  "     CONT_CUST_SEA_TBL              main10,                                   " +  "     CONT_CUST_TRN_SEA_TBL          tran10                                    " +  "    where                                                                     " +  "            main10.CONT_CUST_SEA_PK      = tran10.CONT_CUST_SEA_FK            " +  "     AND    main10.CARGO_TYPE            = 1                                  " +  "     AND    main10.COMMODITY_GROUP_MST_FK = " + Convert.ToString(CommodityGroup) + "      " +  "     AND    main10.CUSTOMER_MST_FK       =  " + Convert.ToString(CustNo) + "              " +  "     AND    (tran10.PORT_MST_POL_FK, tran10.PORT_MST_POD_FK,                  " +  "                                   tran10.CONTAINER_TYPE_MST_FK )             " +  "              in ( " + Convert.ToString(SectorContainers) + " )                           " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            tran10.VALID_FROM   AND   NVL(tran10.VALID_TO,NULL_DATE_FORMAT)      " +  "     AND    main10.OPERATOR_MST_FK       =  main6.OPERATOR_MST_FK            ";

                strBasisSectors = "  Select  DISTINCT  tran10.PORT_MST_POL_FK,                               " +  "          tran10.PORT_MST_POD_FK, tran10.LCL_BASIS                            " +  "    from                                                                      " +  "     CONT_CUST_SEA_TBL              main10,                                   " +  "     CONT_CUST_TRN_SEA_TBL          tran10                                    " +  "    where                                                                     " +  "            main10.CONT_CUST_SEA_PK      = tran10.CONT_CUST_SEA_FK            " +  "     AND    main10.CARGO_TYPE            = 2                                  " +  "     AND    main10.COMMODITY_GROUP_MST_FK = " + Convert.ToString(CommodityGroup) + "      " +  "     AND    main10.CUSTOMER_MST_FK       =  " + Convert.ToString(CustNo) + "              " +  "     AND    (tran10.PORT_MST_POL_FK, tran10.PORT_MST_POD_FK,                  " +  "                                   tran10.LCL_BASIS )                         " +  "              in ( " + Convert.ToString(SectorContainers) + " )                           " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            tran10.VALID_FROM   AND   NVL(tran10.VALID_TO,NULL_DATE_FORMAT)      " +  "     AND    main10.OPERATOR_MST_FK       =  main6.OPERATOR_MST_FK            ";

                //IF "APPROVED_ALL_IN_RATE" > 0 then
                //(1) Elements from child table having "CONT_SUR_CHRG_SEA_TBL.CHECK_FOR ALL_IN_RT"=1 with approved rates 
                //(2) BOF with "CONT_CUST_TRN_SEA_TBL.CURRENT_BOF_RATE"
                //ELSE
                //(1) All Elements from child table with approved rates 
                //(2) BOF with "CONT_CUST_TRN_SEA_TBL.APPROVED_BOF_RATE"
                if (forFCL)
                {
                    strSQL = "    SELECT Q.* FROM (Select     " +  "     main22.CONT_REF_NO                          REF_NO,              " +  "     tran22.PORT_MST_POL_FK                      POL_PK,              " +  "     tran22.PORT_MST_POD_FK                      POD_PK,              " +  "     tran22.CONTAINER_TYPE_MST_FK                CNTR_PK,             " +  "     frt22.FREIGHT_ELEMENT_MST_PK                FRT_PK,              " +  "     frt22.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt22.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     'true'                                      SELECTED,            " +  "     tran22.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr22.CURRENCY_ID                          CURR_ID,             " +  "   ( Case When NVL(tran22.APPROVED_ALL_IN_RATE,0) > 0 Then    " + "            tran22.CURRENT_BOF_RATE                     else    " + "            tran22.APPROVED_BOF_RATE End )         RATE,                " +  "     NULL                                        QUOTERATE,           " +  "     NULL                                        PYTYPE               " +  "    from                                                              " +  "     CONT_CUST_SEA_TBL              main22,                           " +  "     CONT_CUST_TRN_SEA_TBL          tran22,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt22,                            " +  "     CURRENCY_TYPE_MST_TBL          curr22                            " +  "    where                                                                  " +  "            main22.CONT_CUST_SEA_PK      = tran22.CONT_CUST_SEA_FK              " +  "     AND    frt22.FREIGHT_ELEMENT_MST_PK = " + BofPk + "                        " +  "     AND    tran22.CURRENCY_MST_FK       = curr22.CURRENCY_MST_PK(+)            " +  "     AND    main22.CARGO_TYPE            = 1                                    " +  "     AND    main22.COMMODITY_GROUP_MST_FK = " + Convert.ToString(CommodityGroup) + "        " +  "     AND    main22.CUSTOMER_MST_FK       =  " + Convert.ToString(CustNo) + "                " +  "     AND    (tran22.PORT_MST_POL_FK, tran22.PORT_MST_POD_FK,                    " +  "                                   tran22.CONTAINER_TYPE_MST_FK )               " +  "              in ( " + Convert.ToString(SectorContainers) + " )                             " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN " +  "            tran22.VALID_FROM   AND   NVL(tran22.VALID_TO,NULL_DATE_FORMAT)        " +  "    UNION " ;
                }
                else
                {
                    strSQL = "    SELECT Q.* FROM (Select     " +  "     main22.CONT_REF_NO                          REF_NO,              " +  "     tran22.PORT_MST_POL_FK                      POL_PK,              " +  "     tran22.PORT_MST_POD_FK                      POD_PK,              " +  "     tran22.LCL_BASIS                            LCLBASIS,            " +  "     frt22.FREIGHT_ELEMENT_MST_PK                FRT_PK,              " +  "     frt22.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt22.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     'true'                                      SELECTED,            " +  "     tran22.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr22.CURRENCY_ID                          CURR_ID,             " +  "     NULL  MIN_RATE,             " +  "   ( Case When NVL(tran22.APPROVED_ALL_IN_RATE,0) > 0 Then    " + "            tran22.CURRENT_BOF_RATE                     else    " + "            tran22.APPROVED_BOF_RATE End )         RATE ,COMM.COMMODITY_MST_PK COMM_PK                " +  "    from                                                              " +  "     CONT_CUST_SEA_TBL              main22,COMMODITY_MST_TBL COMM,   " +  "     CONT_CUST_TRN_SEA_TBL          tran22,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt22,                            " +  "     CURRENCY_TYPE_MST_TBL          curr22                            " +  "    where                                                                  " +  "            main22.CONT_CUST_SEA_PK      = tran22.CONT_CUST_SEA_FK              " +  "           AND TRAN22.COMMODITY_MST_FK=COMM.COMMODITY_MST_PK                   " +  "     AND    frt22.FREIGHT_ELEMENT_MST_PK = " + BofPk + "                        " +  "     AND    tran22.CURRENCY_MST_FK       = curr22.CURRENCY_MST_PK(+)            " +  "     AND    main22.CARGO_TYPE            = 2                                    " +  "     AND    main22.COMMODITY_GROUP_MST_FK = " + Convert.ToString(CommodityGroup) + "        " +  "     AND    main22.CUSTOMER_MST_FK       =  " + Convert.ToString(CustNo) + "                " +  "     AND    (tran22.PORT_MST_POL_FK, tran22.PORT_MST_POD_FK,                    " +  "                                   tran22.LCL_BASIS )                           " +  "              in ( " + Convert.ToString(SectorContainers) + " )                             " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN " +  "            tran22.VALID_FROM   AND   NVL(tran22.VALID_TO,NULL_DATE_FORMAT)        " +  "    UNION " ;
                }
                if (forFCL)
                {
                    strSQL += "    Select     " +  "     main2.CONT_REF_NO                          REF_NO,              " +  "     tran2.PORT_MST_POL_FK                      POL_PK,              " +  "     tran2.PORT_MST_POD_FK                      POD_PK,              " +  "     tran2.CONTAINER_TYPE_MST_FK                CNTR_PK,             " +  "     frtd2.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt2.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt2.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "   ( Case When NVL(tran2.APPROVED_ALL_IN_RATE,0) > 0        Then " + "        DECODE(frtd2.CHECK_FOR_ALL_IN_RT, 1,'true','false')   else " + "        'true'        End )                       SELECTED,            " +  "     frtd2.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr2.CURRENCY_ID                          CURR_ID,             " +  "     frtd2.APP_SURCHARGE_AMT                    RATE,                " +  "     NULL                                       QUOTERATE,           " +  "     NULL                                       PYTYPE               " +  "    from                                                             " +  "     CONT_CUST_SEA_TBL              main2,                           " +  "     CONT_CUST_TRN_SEA_TBL          tran2,                           " +  "     CONT_SUR_CHRG_SEA_TBL          frtd2,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt2,                            " +  "     CURRENCY_TYPE_MST_TBL          curr2                            " +  "    where                                                                " +  "            main2.CONT_CUST_SEA_PK      = tran2.CONT_CUST_SEA_FK              " +  "     AND    tran2.CONT_CUST_TRN_SEA_PK  = frtd2.CONT_CUST_TRN_SEA_FK          " +  "     AND    frtd2.FREIGHT_ELEMENT_MST_FK = frt2.FREIGHT_ELEMENT_MST_PK(+)     " +  "     AND    frt2.CHARGE_BASIS           <> 2                                  " +  "     AND    frtd2.CURRENCY_MST_FK       = curr2.CURRENCY_MST_PK(+)            " +  "     AND    main2.CARGO_TYPE            = 1                                   " +  "     AND    main2.COMMODITY_GROUP_MST_FK = " + Convert.ToString(CommodityGroup) + "       " +  "     AND    main2.CUSTOMER_MST_FK       =  " + Convert.ToString(CustNo) + "               " +  "     AND    (tran2.PORT_MST_POL_FK, tran2.PORT_MST_POD_FK,                    " +  "                                   tran2.CONTAINER_TYPE_MST_FK )              " +  "              in ( " + Convert.ToString(SectorContainers) + " )                           " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            tran2.VALID_FROM   AND   NVL(tran2.VALID_TO,NULL_DATE_FORMAT)        " +  "    UNION  " +  "   Select            " +  "     " + strContRefNo + "                       REF_NO,              " +  "     tran6.PORT_MST_POL_FK                      POL_PK,              " +  "     tran6.PORT_MST_POD_FK                      POD_PK,              " +  "     cont6.CONTAINER_TYPE_MST_FK                CNTR_PK,             " +  "     tran6.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt6.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt6.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(tran6.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     tran6.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr6.CURRENCY_ID                          CURR_ID,             " +  "     cont6.FCL_REQ_RATE                         RATE,                " +  "     NULL                                       QUOTERATE,           " +  "     NULL                                       PYTYPE               " +  "    from                                                             " +  "     TARIFF_MAIN_SEA_TBL            main6,                           " +  "     TARIFF_TRN_SEA_FCL_LCL         tran6,                           ";
                    //& vbCrLf _
                    //Modified by Snigdharani - 29/10/2008 - Removing v-array
                    //TABLE(tran6.CONTAINER_DTL_FCL) (+) cont6
                    strSQL = strSQL + "TARIFF_TRN_SEA_CONT_DTL cont6,                       " +  "     FREIGHT_ELEMENT_MST_TBL        frt6,                            " +  "     CURRENCY_TYPE_MST_TBL          curr6                            " +  "    where " + strContRefNo + " IS NOT NULL AND                       " +  "            main6.TARIFF_MAIN_SEA_PK    = tran6.TARIFF_MAIN_SEA_FK           " +  "     AND    main6.CARGO_TYPE            = 1                                  " +  "     AND    main6.ACTIVE                = 1                                  " +  "     AND    tran6.FREIGHT_ELEMENT_MST_FK = frt6.FREIGHT_ELEMENT_MST_PK(+)    " +  "     AND    cont6.TARIFF_TRN_SEA_FK = tran6.TARIFF_TRN_SEA_PK                " +  "     AND    frt6.CHARGE_BASIS           <> 2                                 " +  "     AND    tran6.CURRENCY_MST_FK       = curr6.CURRENCY_MST_PK(+)           " +  "     AND    main6.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "       " +  "     AND    (tran6.PORT_MST_POL_FK, tran6.PORT_MST_POD_FK,                   " +  "                                   cont6.CONTAINER_TYPE_MST_FK )             " +  "              in ( " + strContSectors + " )                                  " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN      " +  "            tran6.VALID_FROM   AND   NVL(tran6.VALID_TO,NULL_DATE_FORMAT)       " +  "     AND    tran6.FREIGHT_ELEMENT_MST_FK NOT IN (" + strFreightElements + ") " +  "     AND  " + strSurcharge + " = 1) Q,                  " +  "     FREIGHT_ELEMENT_MST_TBL FRT                  " +  "     WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FRT_PK         " +  "     ORDER BY FRT.PREFERENCE                ";
                }
                else
                {
                    strSQL += "    Select            " +  "     main2.CONT_REF_NO                          REF_NO,              " +  "     tran2.PORT_MST_POL_FK                      POL_PK,              " +  "     tran2.PORT_MST_POD_FK                      POD_PK,              " +  "     tran2.LCL_BASIS                            LCLBASIS,            " +  "     frtd2.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt2.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt2.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "   ( Case When NVL(tran2.APPROVED_ALL_IN_RATE,0) > 0        Then " + "        DECODE(frtd2.CHECK_FOR_ALL_IN_RT, 1,'true','false')   else " + "        'true'        End )                       SELECTED,            " +  "     frtd2.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr2.CURRENCY_ID                          CURR_ID,             " +  "     NULL  MIN_RATE,                 " +  "     frtd2.APP_SURCHARGE_AMT                    RATE,COMM.COMMODITY_MST_PK COMM_PK                 " +  "    from                                                             " +  "     CONT_CUST_SEA_TBL              main2,COMMODITY_MST_TBL COMM,                           " +  "     CONT_CUST_TRN_SEA_TBL          tran2,                           " +  "     CONT_SUR_CHRG_SEA_TBL          frtd2,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt2,                            " +  "     CURRENCY_TYPE_MST_TBL          curr2                            " +  "    where                                                                " +  "            main2.CONT_CUST_SEA_PK      = tran2.CONT_CUST_SEA_FK             " +  "     AND    tran2.CONT_CUST_TRN_SEA_PK  = frtd2.CONT_CUST_TRN_SEA_FK         " +  "   AND    TRAN2.COMMODITY_MST_FK=COMM.COMMODITY_MST_PK         " +  "     AND    frtd2.FREIGHT_ELEMENT_MST_FK = frt2.FREIGHT_ELEMENT_MST_PK(+)    " +  "     AND    frt2.CHARGE_BASIS           <> 2                                 " +  "     AND    frtd2.CURRENCY_MST_FK       = curr2.CURRENCY_MST_PK(+)           " +  "     AND    main2.CARGO_TYPE            = 2                                  " +  "     AND    main2.COMMODITY_GROUP_MST_FK = " + Convert.ToString(CommodityGroup) + "      " +  "     AND    main2.CUSTOMER_MST_FK       =  " + Convert.ToString(CustNo) + "              " +  "     AND    (tran2.PORT_MST_POL_FK, tran2.PORT_MST_POD_FK,                   " +  "                                   tran2.LCL_BASIS )                         " +  "              in ( " + Convert.ToString(SectorContainers) + " )                          " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN      " +  "            tran2.VALID_FROM   AND   NVL(tran2.VALID_TO,NULL_DATE_FORMAT)       " +  "    UNION  " +  "   Select            " +  "     " + strContNoLCL + "                       REF_NO,              " +  "     tran6.PORT_MST_POL_FK                      POL_PK,              " +  "     tran6.PORT_MST_POD_FK                      POD_PK,              " +  "     tran6.LCL_BASIS                            LCLBASIS,            " +  "     tran6.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt6.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt6.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(tran6.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     tran6.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr6.CURRENCY_ID                          CURR_ID,             " +  "     NULL  MIN_RATE,                 " +  "     tran6.LCL_TARIFF_RATE                      RATE ,NULL COMM_PK     " +  "    from                                                             " +  "     TARIFF_MAIN_SEA_TBL            main6,                           " +  "     TARIFF_TRN_SEA_FCL_LCL         tran6,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt6,                            " +  "     CURRENCY_TYPE_MST_TBL          curr6                            " +  "    where " + strContNoLCL + " IS NOT NULL AND                       " +  "            main6.TARIFF_MAIN_SEA_PK    = tran6.TARIFF_MAIN_SEA_FK           " +  "     AND    main6.CARGO_TYPE            = 2                                  " +  "     AND    main6.ACTIVE                = 1                                  " +  "     AND    tran6.FREIGHT_ELEMENT_MST_FK = frt6.FREIGHT_ELEMENT_MST_PK(+)    " +  "     AND    frt6.CHARGE_BASIS           <> 2                                  " +  "     AND    tran6.CURRENCY_MST_FK       = curr6.CURRENCY_MST_PK(+)           " +  "     AND    main6.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "       " +  "     AND    (tran6.PORT_MST_POL_FK, tran6.PORT_MST_POD_FK,                   " +  "                                   tran6.LCL_BASIS )                         " +  "              in ( " + strBasisSectors + " )                                 " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN      " +  "            tran6.VALID_FROM   AND   NVL(tran6.VALID_TO,NULL_DATE_FORMAT)       " +  "     AND    tran6.FREIGHT_ELEMENT_MST_FK NOT IN (" + strFreightsLCL + ")     " +  "     AND  " + strSurchargeLCL + " = 1) Q,               " +  "     FREIGHT_ELEMENT_MST_TBL FRT                  " +  "     WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FRT_PK         " +  "     ORDER BY FRT.PREFERENCE                ";
                }
                strSQL = strSQL.Replace("   ", " ");
                strSQL = strSQL.Replace("  ", " ");
                return strSQL;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion

        #region " Operator Tariff Query "

        private string OprTariffFreightQuery(bool forFCL = true, string CustNo = "", string SectorContainers = "", string CommodityGroup = "", string ShipDate = "", int BBFlag = 0, string CommodityPks = "")
        {
            try
            {
                string strSQL = null;
                string exchQueryFCL = null;
                string exchQueryLCL = null;

                if (forFCL)
                {
                    strSQL = "    SELECT Q.* FROM (Select            " +  "     main5.TARIFF_REF_NO                        REF_NO,              " +  "     tran5.PORT_MST_POL_FK                      POL_PK,              " +  "     tran5.PORT_MST_POD_FK                      POD_PK,              " +  "     cont5.CONTAINER_TYPE_MST_FK                CNTR_PK,             " +  "     tran5.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt5.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt5.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(tran5.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     tran5.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr5.CURRENCY_ID                          CURR_ID,             " +  "     cont5.FCL_REQ_RATE                         RATE,                " +  "     NULL                                       QUOTERATE,           " +  "     NULL                                       PYTYPE               " +  "    from                                                             " +  "     TARIFF_MAIN_SEA_TBL            main5,                           " +  "     TARIFF_TRN_SEA_FCL_LCL         tran5,                           ";
                    //& vbCrLf _
                    //Modified by Snigdharani - 29/10/2008 - Removing v-array
                    //TABLE(tran5.CONTAINER_DTL_FCL) (+) cont5
                    strSQL = strSQL + "     TARIFF_TRN_SEA_CONT_DTL cont5,                       " +  "     FREIGHT_ELEMENT_MST_TBL        frt5,                            " +  "     CURRENCY_TYPE_MST_TBL          curr5                            " +  "    where                                                                " +  "            main5.TARIFF_MAIN_SEA_PK    = tran5.TARIFF_MAIN_SEA_FK            " +  "     AND    main5.CARGO_TYPE            = 1                                   " +  "     AND    main5.ACTIVE                = 1                                   " +  "     AND    main5.TARIFF_TYPE           = 1                                   " +  "     AND    tran5.FREIGHT_ELEMENT_MST_FK = frt5.FREIGHT_ELEMENT_MST_PK(+)     " +  "     AND    cont5.TARIFF_TRN_SEA_FK = tran5.TARIFF_TRN_SEA_PK                 " +  "     AND    frt5.CHARGE_BASIS           <> 2                                  " +  "     AND    tran5.CURRENCY_MST_FK       = curr5.CURRENCY_MST_PK(+)            " +  "     AND    main5.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "        " +  "     AND    (tran5.PORT_MST_POL_FK, tran5.PORT_MST_POD_FK,                    " +  "                                   cont5.CONTAINER_TYPE_MST_FK )              " +  "              in ( " + Convert.ToString(SectorContainers) + " )                           " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            tran5.VALID_FROM   AND   NVL(tran5.VALID_TO,NULL_DATE_FORMAT)) Q," +  "            FREIGHT_ELEMENT_MST_TBL FRT        " +  "            WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FRT_PK       " +  "            ORDER BY FRT.PREFERENCE        ";
                    //Added by rabbani reason USS Gap,introduced New Column as "QUOTE_MIN_RATE"
                }
                else
                {
                    //Added by rabbani on 24/3/07 ,To display BOF as first element in Freight Element Column using UNION ALL.
                    if (BBFlag == 1)
                    {
                        strSQL = "    SELECT Q.* FROM (Select            " +  "     main5.TARIFF_REF_NO                        REF_NO,              " +  "     tran5.PORT_MST_POL_FK                      POL_PK,              " +  "     tran5.PORT_MST_POD_FK                      POD_PK,              " +  "     tran5.LCL_BASIS                            LCLBASIS,            " +  "     tran5.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt5.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt5.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(tran5.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     tran5.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr5.CURRENCY_ID                          CURR_ID,             " +  "     tran5.LCL_TARIFF_MIN_RATE                  QUOTE_MIN_RATE,      " +  "     tran5.LCL_TARIFF_RATE    RATE ,CMT.COMMODITY_MST_PK  COMM_PK    " +  "    from                                                             " +  "     TARIFF_MAIN_SEA_TBL            main5, COMMODITY_MST_TBL CMT ,  " +  "     TARIFF_TRN_SEA_FCL_LCL         tran5,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt5,                            " +  "     CURRENCY_TYPE_MST_TBL          curr5                            " +  "    where                                                                " +  "            main5.TARIFF_MAIN_SEA_PK    = tran5.TARIFF_MAIN_SEA_FK            " +  "     AND    main5.CARGO_TYPE            = 2                                   " +  "     AND    main5.ACTIVE                = 1                                   " +  "     AND    main5.TARIFF_TYPE           = 1                                   " +  "     AND    tran5.FREIGHT_ELEMENT_MST_FK = frt5.FREIGHT_ELEMENT_MST_PK(+)     " +  "    AND    TRAN5.COMMODITY_MST_FK        = CMT.COMMODITY_MST_PK    AND CMT.COMMODITY_MST_PK IN (" + Convert.ToString(CommodityPks) + ")            " +  "     AND    frt5.CHARGE_BASIS           <> 2                                  " +  "     AND    tran5.CURRENCY_MST_FK       = curr5.CURRENCY_MST_PK(+)            " +  "     AND    main5.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "        " +  "     AND    (tran5.PORT_MST_POL_FK, tran5.PORT_MST_POD_FK,                    " +  "                                   tran5.LCL_BASIS )                          " +  "              in ( " + Convert.ToString(SectorContainers) + " )                           " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            tran5.VALID_FROM   AND   NVL(tran5.VALID_TO,NULL_DATE_FORMAT)       " +  "     AND    frt5.FREIGHT_ELEMENT_MST_PK IN (SELECT PARM.FRT_BOF_FK  FROM PARAMETERS_TBL PARM) " +  "     UNION ALL " +  "     Select            " +  "     main5.TARIFF_REF_NO                        REF_NO,              " +  "     tran5.PORT_MST_POL_FK                      POL_PK,              " +  "     tran5.PORT_MST_POD_FK                      POD_PK,              " +  "     tran5.LCL_BASIS                            LCLBASIS,            " +  "     tran5.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt5.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt5.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(tran5.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     tran5.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr5.CURRENCY_ID                          CURR_ID,             " +  "     tran5.LCL_TARIFF_MIN_RATE                  QUOTE_MIN_RATE,      " +  "     tran5.LCL_TARIFF_RATE RATE ,CMT.COMMODITY_MST_PK  COMM_PK       " +  "    from                                                             " +  "     TARIFF_MAIN_SEA_TBL   main5,COMMODITY_MST_TBL CMT,              " +  "     TARIFF_TRN_SEA_FCL_LCL         tran5,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt5,                            " +  "     CURRENCY_TYPE_MST_TBL          curr5                            " +  "    where                                                                " +  "            main5.TARIFF_MAIN_SEA_PK    = tran5.TARIFF_MAIN_SEA_FK            " +  "     AND    main5.CARGO_TYPE            = 2                                   " +  "     AND    main5.ACTIVE                = 1                                   " +  "     AND    main5.TARIFF_TYPE           = 1                                   " +  "     AND    tran5.FREIGHT_ELEMENT_MST_FK = frt5.FREIGHT_ELEMENT_MST_PK(+)     " +  "    AND   TRAN5.COMMODITY_MST_FK=CMT.COMMODITY_MST_PK  AND CMT.COMMODITY_MST_PK IN (" + Convert.ToString(CommodityPks) + ")   " +  "     AND    frt5.CHARGE_BASIS           <> 2                                  " +  "     AND    tran5.CURRENCY_MST_FK       = curr5.CURRENCY_MST_PK(+)            " +  "     AND    main5.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "        " +  "     AND    (tran5.PORT_MST_POL_FK, tran5.PORT_MST_POD_FK,                    " +  "                                   tran5.LCL_BASIS )                          " +  "              in ( " + Convert.ToString(SectorContainers) + " )                           " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            tran5.VALID_FROM   AND   NVL(tran5.VALID_TO,NULL_DATE_FORMAT)       " +  "     AND    frt5.FREIGHT_ELEMENT_MST_PK NOT IN (SELECT PARM.FRT_BOF_FK  FROM PARAMETERS_TBL PARM)) Q, " +  "            FREIGHT_ELEMENT_MST_TBL FRT        " +  "            WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FRT_PK       " +  "            ORDER BY FRT.PREFERENCE        ";
                    }
                    else
                    {
                        strSQL = "    SELECT Q.* FROM (Select            " +  "     main5.TARIFF_REF_NO                        REF_NO,              " +  "     tran5.PORT_MST_POL_FK                      POL_PK,              " +  "     tran5.PORT_MST_POD_FK                      POD_PK,              " +  "     tran5.LCL_BASIS                            LCLBASIS,            " +  "     tran5.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt5.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt5.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(tran5.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     tran5.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr5.CURRENCY_ID                          CURR_ID,             " +  "     tran5.LCL_TARIFF_MIN_RATE                  QUOTE_MIN_RATE,      " +  "     tran5.LCL_TARIFF_RATE                      RATE                 " +  "    from                                                             " +  "     TARIFF_MAIN_SEA_TBL            main5,                           " +  "     TARIFF_TRN_SEA_FCL_LCL         tran5,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt5,                            " +  "     CURRENCY_TYPE_MST_TBL          curr5                            " +  "    where                                                                " +  "            main5.TARIFF_MAIN_SEA_PK    = tran5.TARIFF_MAIN_SEA_FK            " +  "     AND    main5.CARGO_TYPE            = 2                                   " +  "     AND    main5.ACTIVE                = 1                                   " +  "     AND    main5.TARIFF_TYPE           = 1                                   " +  "     AND    tran5.FREIGHT_ELEMENT_MST_FK = frt5.FREIGHT_ELEMENT_MST_PK(+)     " +  "     AND    frt5.CHARGE_BASIS           <> 2                                  " +  "     AND    tran5.CURRENCY_MST_FK       = curr5.CURRENCY_MST_PK(+)            " +  "     AND    main5.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "        " +  "     AND    (tran5.PORT_MST_POL_FK, tran5.PORT_MST_POD_FK,                    " +  "                                   tran5.LCL_BASIS )                          " +  "              in ( " + Convert.ToString(SectorContainers) + " )                           " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            tran5.VALID_FROM   AND   NVL(tran5.VALID_TO,NULL_DATE_FORMAT)       " +  "     AND    frt5.FREIGHT_ELEMENT_MST_PK IN (SELECT PARM.FRT_BOF_FK  FROM PARAMETERS_TBL PARM) " +  "     UNION ALL " +  "     Select            " +  "     main5.TARIFF_REF_NO                        REF_NO,              " +  "     tran5.PORT_MST_POL_FK                      POL_PK,              " +  "     tran5.PORT_MST_POD_FK                      POD_PK,              " +  "     tran5.LCL_BASIS                            LCLBASIS,            " +  "     tran5.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt5.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt5.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(tran5.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     tran5.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr5.CURRENCY_ID                          CURR_ID,             " +  "     tran5.LCL_TARIFF_MIN_RATE                  QUOTE_MIN_RATE,      " +  "     tran5.LCL_TARIFF_RATE                      RATE                 " +  "    from                                                             " +  "     TARIFF_MAIN_SEA_TBL            main5,                           " +  "     TARIFF_TRN_SEA_FCL_LCL         tran5,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt5,                            " +  "     CURRENCY_TYPE_MST_TBL          curr5                            " +  "    where                                                                " +  "            main5.TARIFF_MAIN_SEA_PK    = tran5.TARIFF_MAIN_SEA_FK            " +  "     AND    main5.CARGO_TYPE            = 2                                   " +  "     AND    main5.ACTIVE                = 1                                   " +  "     AND    main5.TARIFF_TYPE           = 1                                   " +  "     AND    tran5.FREIGHT_ELEMENT_MST_FK = frt5.FREIGHT_ELEMENT_MST_PK(+)     " +  "     AND    frt5.CHARGE_BASIS           <> 2                                  " +  "     AND    tran5.CURRENCY_MST_FK       = curr5.CURRENCY_MST_PK(+)            " +  "     AND    main5.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "        " +  "     AND    (tran5.PORT_MST_POL_FK, tran5.PORT_MST_POD_FK,                    " +  "                                   tran5.LCL_BASIS )                          " +  "              in ( " + Convert.ToString(SectorContainers) + " )                           " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            tran5.VALID_FROM   AND   NVL(tran5.VALID_TO,NULL_DATE_FORMAT)       " +  "     AND    frt5.FREIGHT_ELEMENT_MST_PK NOT IN (SELECT PARM.FRT_BOF_FK  FROM PARAMETERS_TBL PARM)) Q, " +  "            FREIGHT_ELEMENT_MST_TBL FRT        " +  "            WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FRT_PK       " +  "            ORDER BY FRT.PREFERENCE        ";
                    }
                }
                strSQL = strSQL.Replace("   ", " ");
                strSQL = strSQL.Replace("  ", " ");
                return strSQL;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion

        #region " General Tariff Query "

        private string funGenTariffFreightQuery(bool forFCL = true, string CustNo = "", string SectorContainers = "", string CommodityGroup = "", string ShipDate = "")
        {
            try
            {
                string strSQL = null;
                string exchQueryFCL = null;
                string exchQueryLCL = null;

                if (forFCL)
                {
                    strSQL = "    SELECT Q.* FROM (Select            " +  "     main5.TARIFF_REF_NO                        REF_NO,              " +  "     tran5.PORT_MST_POL_FK                      POL_PK,              " +  "     tran5.PORT_MST_POD_FK                      POD_PK,              " +  "     cont5.CONTAINER_TYPE_MST_FK                CNTR_PK,             " +  "     tran5.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt5.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt5.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(tran5.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     tran5.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr5.CURRENCY_ID                          CURR_ID,             " +  "     cont5.FCL_REQ_RATE                         RATE,                " +  "     NULL                                       QUOTERATE,           " +  "     NULL                                       PYTYPE               " +  "    from                                                             " +  "     TARIFF_MAIN_SEA_TBL            main5,                           " +  "     TARIFF_TRN_SEA_FCL_LCL         tran5,                           ";
                    //& vbCrLf _
                    //Modified by Snigdharani - 29/10/2008 - Removing v-array
                    //TABLE(tran5.CONTAINER_DTL_FCL) (+) cont5
                    strSQL = strSQL + "     TARIFF_TRN_SEA_CONT_DTL cont5,                       " +  "     FREIGHT_ELEMENT_MST_TBL        frt5,                            " +  "     CURRENCY_TYPE_MST_TBL          curr5                            " +  "    where                                                                " +  "            main5.TARIFF_MAIN_SEA_PK    = tran5.TARIFF_MAIN_SEA_FK            " +  "     AND    main5.CARGO_TYPE            = 1                                   " +  "     AND    main5.ACTIVE                = 1                                   " +  "     AND    main5.TARIFF_TYPE           = 2                                   " +  "     AND    tran5.FREIGHT_ELEMENT_MST_FK = frt5.FREIGHT_ELEMENT_MST_PK(+)     " +  "     AND    cont5.TARIFF_TRN_SEA_FK = tran5.TARIFF_TRN_SEA_PK     " +  "     AND    frt5.CHARGE_BASIS           <> 2                                  " +  "     AND    tran5.CURRENCY_MST_FK       = curr5.CURRENCY_MST_PK(+)            " +  "     AND    main5.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "        " +  "     AND    (tran5.PORT_MST_POL_FK, tran5.PORT_MST_POD_FK,                    " +  "                                   cont5.CONTAINER_TYPE_MST_FK )              " +  "              in ( " + Convert.ToString(SectorContainers) + " )                           " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            tran5.VALID_FROM   AND   NVL(tran5.VALID_TO,NULL_DATE_FORMAT)) Q,        " +  "            FREIGHT_ELEMENT_MST_TBL FRT        " +  "            WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FRT_PK       " +  "            ORDER BY FRT.PREFERENCE        ";
                    //Added by Rabbani raeson USS Gap,introduced New column as "MIN_RATE"
                }
                else
                {
                    //Added by rabbani on 24/3/07 ,To display BOF as first element in Freight Element Column using UNION ALL.
                    strSQL = "    SELECT Q.* FROM (Select            " +  "     main5.TARIFF_REF_NO                        REF_NO,              " +  "     tran5.PORT_MST_POL_FK                      POL_PK,              " +  "     tran5.PORT_MST_POD_FK                      POD_PK,              " +  "     tran5.LCL_BASIS                            LCLBASIS,            " +  "     tran5.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt5.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt5.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(tran5.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     tran5.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr5.CURRENCY_ID                          CURR_ID,             " +  "     tran5.LCL_TARIFF_MIN_RATE                  MIN_RATE,            " +  "     tran5.LCL_TARIFF_RATE                      RATE                 " +  "    from                                                             " +  "     TARIFF_MAIN_SEA_TBL            main5,                           " +  "     TARIFF_TRN_SEA_FCL_LCL         tran5,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt5,                            " +  "     CURRENCY_TYPE_MST_TBL          curr5                            " +  "    where                                                                " +  "            main5.TARIFF_MAIN_SEA_PK    = tran5.TARIFF_MAIN_SEA_FK            " +  "     AND    main5.CARGO_TYPE            = 2                                   " +  "     AND    main5.ACTIVE                = 1                                   " +  "     AND    main5.TARIFF_TYPE           = 2                                   " +  "     AND    tran5.FREIGHT_ELEMENT_MST_FK = frt5.FREIGHT_ELEMENT_MST_PK(+)     " +  "     AND    frt5.CHARGE_BASIS           <> 2                                  " +  "     AND    tran5.CURRENCY_MST_FK       = curr5.CURRENCY_MST_PK(+)            " +  "     AND    main5.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "        " +  "     AND    (tran5.PORT_MST_POL_FK, tran5.PORT_MST_POD_FK,                    " +  "                                   tran5.LCL_BASIS )                          " +  "              in ( " + Convert.ToString(SectorContainers) + " )                           " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            tran5.VALID_FROM   AND   NVL(tran5.VALID_TO,NULL_DATE_FORMAT)       " +  "     AND    frt5.FREIGHT_ELEMENT_MST_PK IN (SELECT PARM.FRT_BOF_FK  FROM PARAMETERS_TBL PARM)" +  "     UNION ALL " +  "    Select            " +  "     main5.TARIFF_REF_NO                        REF_NO,              " +  "     tran5.PORT_MST_POL_FK                      POL_PK,              " +  "     tran5.PORT_MST_POD_FK                      POD_PK,              " +  "     tran5.LCL_BASIS                            LCLBASIS,            " +  "     tran5.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt5.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt5.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(tran5.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     tran5.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr5.CURRENCY_ID                          CURR_ID,             " +  "     tran5.LCL_TARIFF_MIN_RATE                  MIN_RATE,            " +  "     tran5.LCL_TARIFF_RATE                      RATE                 " +  "    from                                                             " +  "     TARIFF_MAIN_SEA_TBL            main5,                           " +  "     TARIFF_TRN_SEA_FCL_LCL         tran5,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt5,                            " +  "     CURRENCY_TYPE_MST_TBL          curr5                            " +  "    where                                                                " +  "            main5.TARIFF_MAIN_SEA_PK    = tran5.TARIFF_MAIN_SEA_FK            " +  "     AND    main5.CARGO_TYPE            = 2                                   " +  "     AND    main5.ACTIVE                = 1                                   " +  "     AND    main5.TARIFF_TYPE           = 2                                   " +  "     AND    tran5.FREIGHT_ELEMENT_MST_FK = frt5.FREIGHT_ELEMENT_MST_PK(+)     " +  "     AND    frt5.CHARGE_BASIS           <> 2                                  " +  "     AND    tran5.CURRENCY_MST_FK       = curr5.CURRENCY_MST_PK(+)            " +  "     AND    main5.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "        " +  "     AND    (tran5.PORT_MST_POL_FK, tran5.PORT_MST_POD_FK,                    " +  "                                   tran5.LCL_BASIS )                          " +  "              in ( " + Convert.ToString(SectorContainers) + " )                           " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            tran5.VALID_FROM   AND   NVL(tran5.VALID_TO,NULL_DATE_FORMAT)       " +  "     AND    frt5.FREIGHT_ELEMENT_MST_PK NOT IN (SELECT PARM.FRT_BOF_FK  FROM PARAMETERS_TBL PARM)) Q," +  "            FREIGHT_ELEMENT_MST_TBL FRT        " +  "            WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FRT_PK       " +  "            ORDER BY FRT.PREFERENCE        ";
                }
                strSQL = strSQL.Replace("   ", " ");
                strSQL = strSQL.Replace("  ", " ");
                return strSQL;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion

        #endregion

        #region " Supporting Methods "

        #region " Enhance Search Function for Enquiry "

        public string FetchEnquiryRefNo(string strCond)
        {

            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string CARGO = "";
            string QUOTEDATE = "";
            string Loc = "";
            string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                CARGO = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                QUOTEDATE = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                Loc = Convert.ToString(arr.GetValue(4));
            //Manoharan 14Sep07:Enquiry popup should display location based
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_BOOKING_PKG.GET_BOOKING_ENQUIRY_SEA";
                var _with4 = SCM.Parameters;
                _with4.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with4.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with4.Add("CARGO_TYPE_IN", CARGO).Direction = ParameterDirection.Input;
                _with4.Add("QUOTEDATE_IN", QUOTEDATE).Direction = ParameterDirection.Input;
                //Manoharan 14Sep07:Enquiry popup should display location based
                _with4.Add("LogLoc_IN", Loc).Direction = ParameterDirection.Input;
                //.Add("RETURN_VALUE", OracleDbType.NVarChar, 2000, "RETURN_VALUE").Direction = ParameterDirection.Output
                _with4.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                //Manoharan 14Sep07:Enquiry popup should display location based
                // strReturn = CStr(SCM.Parameters["RETURN_VALUE"].Value).Trim
                OracleClob clob = null;
                clob = (OracleClob)SCM.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn.Trim();
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                SCM.Connection.Close();
            }

        }

        #endregion

        public DataTable fetchCustomerCategory()
        {
            string strSQL = null;
            strSQL = " Select CUSTOMER_CATEGORY_MST_PK, CUSTOMER_CATEGORY_ID " + " from CUSTOMER_CATEGORY_MST_TBL where ACTIVE_FLAG = 1 order by CUSTOMER_CATEGORY_ID ";
            try
            {
                return (new WorkFlow()).GetDataTable(strSQL);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception eX)
            {
                throw eX;
            }
        }

        private new object removeDBNull(object col)
        {
            if (col == null)
            {
                return "";
            }
            else if (object.ReferenceEquals(col, DBNull.Value))
            {
                return "";
            }
            return col;
        }

        private new object ifDBNull(object col)
        {
            if (col == null)
            {
                return DBNull.Value;
            }
            else if (object.ReferenceEquals(col, DBNull.Value))
            {
                return DBNull.Value;
            }
            else if (Convert.ToString(col).Length == 0)
            {
                return DBNull.Value;
            }
            else
            {
                return col;
            }
        }

        private new object ifDBZero(object col, Int16 Zero = 0)
        {
            if (Convert.ToInt32(col) == Zero)
            {
                return DBNull.Value;
            }
            else
            {
                return col;
            }
        }

        #endregion

        #endregion

        #region "FETCH CREDTDAYS AND CREDIT LIMIT"
        public int fetchCredit(object CreditDays, object CreditLimit, string Pk = "0", int type = 0, int CustPk = 0)
        {
            //type
            //1--Enquiry
            //2--Quotation
            //3--CustomerContract
            try
            {
                System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
                System.Text.StringBuilder strCustQuery = new System.Text.StringBuilder();
                strCustQuery.Append("SELECT C.SEA_CREDIT_DAYS," );
                strCustQuery.Append(" C.SEA_CREDIT_LIMIT" );
                strCustQuery.Append("FROM customer_mst_tbl c" );
                strCustQuery.Append("WHERE c.customer_mst_pk=" + CustPk );
                OracleDataReader dr = null;
                if (type == 1)
                {
                    strQuery.Append("SELECT C.CREDIT_PERIOD" );
                    strQuery.Append("  FROM ENQUIRY_BKG_SEA_TBL     EB," );
                    strQuery.Append("       ENQUIRY_TRN_SEA_FCL_LCL ET," );
                    strQuery.Append("       CONT_CUST_SEA_TBL       C" );
                    strQuery.Append("       WHERE" );
                    strQuery.Append("       ET.TRANS_REFERED_FROM=2" );
                    strQuery.Append("       AND Eb.ENQUIRY_REF_NO='" + Pk + "'" );
                    strQuery.Append("       AND EB.ENQUIRY_BKG_SEA_PK=ET.ENQUIRY_MAIN_SEA_FK" );
                    strQuery.Append("       AND ET.TRANS_REF_NO=C.CONT_REF_NO" );
                    strQuery.Append("     AND C.CUSTOMER_MST_FK=" + CustPk );
                }
                else if (type == 2)
                {
                    strQuery.Append("SELECT Q.CREDIT_DAYS" );
                    //strQuery.Append("     Q.CREDIT_LIMIT " & vbCrLf)
                    strQuery.Append("     FROM QUOTATION_SEA_TBL Q" );
                    strQuery.Append("     WHERE Q.QUOTATION_SEA_PK=" + Pk );
                    strQuery.Append("     AND Q.CUSTOMER_MST_FK=" + CustPk );
                }
                else if (type == 3)
                {
                    strQuery.Append("SELECT C.CREDIT_PERIOD FROM cont_cust_sea_tbl C  " );
                    strQuery.Append("WHERE C.CONT_CUST_SEA_PK=" + Pk );
                }
                else
                {
                    strQuery = strCustQuery;
                }
                DataTable dt = null;
                dt = (new WorkFlow()).GetDataTable(strQuery.ToString());
                if (dt.Rows.Count > 0)
                {
                    CreditDays = getDefault(dt.Rows[0][0], "");
                    if (dt.Columns.Count > 1)
                        CreditLimit = getDefault(dt.Rows[0][1], "");
                }
                else
                {
                    dt = (new WorkFlow()).GetDataTable(strCustQuery.ToString());
                    CreditDays = getDefault(dt.Rows[0][0], "");
                    if (dt.Columns.Count > 1)
                        CreditLimit = getDefault(dt.Rows[0][1], "");
                }
                dr = (new WorkFlow()).GetDataReader(strCustQuery.ToString());
                while (dr.Read())
                {
                    if (string.IsNullOrEmpty(Convert.ToString(CreditDays)))
                        CreditDays = getDefault(dr[0], "");
                    if (string.IsNullOrEmpty(Convert.ToString(CreditLimit)))
                        CreditLimit = getDefault(dr[1], "");
                }
                dr.Close();
                if (!string.IsNullOrEmpty(Convert.ToString(CreditDays)))
                {
                    return 1;
                }
                else
                {
                    return 0;
                }
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                return 0;
            }
        }
        #endregion

        #region " Save Quotation "

        #region " Header Part [ Save Quotation ] "
        //this is modified by Thiyagrajan on 29/5/08 for fcl and lcl quotation    
        public ArrayList SaveQuotation(DataTable HDT, DataTable PDT, DataTable CDT, DataSet DSCalculator, DataTable OthDT, object QuoteNo, object QuotePk, long nLocationId, long nEmpId, Int16 CargoType,
        string remarks = "", string CargoMoveCode = "", string Header = "", string Footer = "", Int16 cargo = 1, Int32 PYMTType = 0, int INCOTerms = 0, int PolPk = 0, int Podpk = 0, bool AmendFlg = false,
        int From_Flag = 0, bool Customer_Approved = false)
        {

            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            bool chkFlag = false;
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            arrMessage.Clear();
            objWK.MyCommand.Transaction = TRAN;
            string PrevQuotPK = null;
            try
            {
                CargoType = 4;
                if (AmendFlg == true)
                {
                    PrevQuotPK = Convert.ToString(QuotePk);
                    QuotePk = "";
                }
                if (string.IsNullOrEmpty(Convert.ToString(QuoteNo)))
                {
                    QuoteNo = GenerateQuoteNo(nLocationId, nEmpId, M_CREATED_BY_FK, objWK, From_Flag);
                    if (QuoteNo == "Protocol Not Defined.")
                    {
                        arrMessage.Add("Protocol Not Defined.");
                        QuoteNo = "";
                        return arrMessage;
                    }
                    chkFlag = true;
                }

                //by thiyagarajan
                if (!string.IsNullOrEmpty(Convert.ToString(QuotePk)))
                {
                    _PkValue = Convert.ToInt32(QuotePk);
                }
                //end

                if (string.IsNullOrEmpty(Convert.ToString(QuotePk)))
                {
                    var _with5 = objWK.MyCommand;
                    _with5.CommandType = CommandType.StoredProcedure;
                    _with5.CommandText = objWK.MyUserName + ".QUOT_BB_TBL_PKG.QUOT_BB_SEA_TBL_INS";
                    _with5.Parameters.Clear();

                    _with5.Parameters.Add("QUOTATION_REF_NO_IN", Convert.ToString(QuoteNo)).Direction = ParameterDirection.Input;
                    _with5.Parameters["QUOTATION_REF_NO_IN"].SourceVersion = DataRowVersion.Current;

                    _with5.Parameters.Add("QUOTATION_DATE_IN", Convert.ToDateTime(HDT.Rows[0]["QUOTATION_DATE"])).Direction = ParameterDirection.Input;
                    _with5.Parameters["QUOTATION_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    _with5.Parameters.Add("VALID_FOR_IN", HDT.Rows[0]["VALID_FOR"]).Direction = ParameterDirection.Input;
                    _with5.Parameters["VALID_FOR_IN"].SourceVersion = DataRowVersion.Current;

                    _with5.Parameters.Add("CARGO_TYPE_IN", 4).Direction = ParameterDirection.Input;
                    _with5.Parameters["CARGO_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with5.Parameters.Add("PYMT_TYPE_IN", PYMTType).Direction = ParameterDirection.Input;
                    _with5.Parameters["PYMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with5.Parameters.Add("QUOTED_BY_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                    _with5.Parameters["QUOTED_BY_IN"].SourceVersion = DataRowVersion.Current;

                    _with5.Parameters.Add("COL_PLACE_MST_FK_IN", HDT.Rows[0]["COL_PLACE_MST_FK"]).Direction = ParameterDirection.Input;
                    _with5.Parameters["COL_PLACE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with5.Parameters.Add("COL_ADDRESS_IN", getDefault(HDT.Rows[0]["COL_ADDRESS"], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with5.Parameters["COL_ADDRESS_IN"].SourceVersion = DataRowVersion.Current;

                    _with5.Parameters.Add("DEL_PLACE_MST_FK_IN", HDT.Rows[0]["DEL_PLACE_MST_FK"]).Direction = ParameterDirection.Input;
                    _with5.Parameters["DEL_PLACE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with5.Parameters.Add("DEL_ADDRESS_IN", getDefault(HDT.Rows[0]["DEL_ADDRESS"], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with5.Parameters["DEL_ADDRESS_IN"].SourceVersion = DataRowVersion.Current;

                    _with5.Parameters.Add("AGENT_MST_FK_IN", HDT.Rows[0]["AGENT_MST_FK"]).Direction = ParameterDirection.Input;
                    _with5.Parameters["AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with5.Parameters.Add("STATUS_IN", HDT.Rows[0]["STATUS"]).Direction = ParameterDirection.Input;
                    _with5.Parameters["STATUS_IN"].SourceVersion = DataRowVersion.Current;

                    ///'
                    _with5.Parameters.Add("EXPECTED_SHIPMENT_DT_IN", Convert.ToDateTime(HDT.Rows[0]["EXPECTED_SHIPMENT_DT"])).Direction = ParameterDirection.Input;
                    _with5.Parameters["EXPECTED_SHIPMENT_DT_IN"].SourceVersion = DataRowVersion.Current;

                    _with5.Parameters.Add("CUSTOMER_MST_FK_IN", HDT.Rows[0]["CUSTOMER_MST_FK"]).Direction = ParameterDirection.Input;
                    _with5.Parameters["CUSTOMER_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with5.Parameters.Add("CUSTOMER_CATEGORY_MST_FK_IN", HDT.Rows[0]["CUSTOMER_CATEGORY_MST_FK"]).Direction = ParameterDirection.Input;
                    _with5.Parameters["CUSTOMER_CATEGORY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with5.Parameters.Add("SPECIAL_INSTRUCTIONS_IN", getDefault(HDT.Rows[0]["SPECIAL_INSTRUCTIONS"], 0)).Direction = ParameterDirection.Input;
                    _with5.Parameters["SPECIAL_INSTRUCTIONS_IN"].SourceVersion = DataRowVersion.Current;

                    _with5.Parameters.Add("CUST_TYPE_IN", getDefault(HDT.Rows[0]["CUST_TYPE"], 0)).Direction = ParameterDirection.Input;
                    _with5.Parameters["CUST_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with5.Parameters.Add("CREDIT_LIMIT_IN", HDT.Rows[0]["CREDIT_LIMIT"]).Direction = ParameterDirection.Input;
                    _with5.Parameters["CREDIT_LIMIT_IN"].SourceVersion = DataRowVersion.Current;

                    _with5.Parameters.Add("CREDIT_DAYS_IN", HDT.Rows[0]["CREDIT_DAYS"]).Direction = ParameterDirection.Input;
                    _with5.Parameters["CREDIT_DAYS_IN"].SourceVersion = DataRowVersion.Current;


                    _with5.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                    _with5.Parameters["CREATED_BY_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with5.Parameters.Add("CONFIG_PK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                    _with5.Parameters["CONFIG_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with5.Parameters.Add("REMARKS_IN", getDefault(remarks, DBNull.Value)).Direction = ParameterDirection.Input;
                    _with5.Parameters["REMARKS_IN"].SourceVersion = DataRowVersion.Current;

                    _with5.Parameters.Add("CARGO_MOVE_CODE_IN", getDefault(CargoMoveCode, DBNull.Value)).Direction = ParameterDirection.Input;
                    _with5.Parameters["CARGO_MOVE_CODE_IN"].SourceVersion = DataRowVersion.Current;

                    _with5.Parameters.Add("Header_IN", getDefault(Header, DBNull.Value)).Direction = ParameterDirection.Input;
                    _with5.Parameters["Header_IN"].SourceVersion = DataRowVersion.Current;

                    _with5.Parameters.Add("Footer_IN", getDefault(Footer, DBNull.Value)).Direction = ParameterDirection.Input;
                    _with5.Parameters["Footer_IN"].SourceVersion = DataRowVersion.Current;

                    _with5.Parameters.Add("BASE_CURRENCY_FK_IN", getDefault(HDT.Rows[0]["BASE_CURRENCY_FK"], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with5.Parameters["BASE_CURRENCY_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with5.Parameters.Add("FROM_FLAG_IN", getDefault(From_Flag, 0)).Direction = ParameterDirection.Input;
                    _with5.Parameters["FROM_FLAG_IN"].SourceVersion = DataRowVersion.Current;

                    _with5.Parameters.Add("CUSTOMER_APPROVED_IN", (Customer_Approved ? 1 : 0)).Direction = ParameterDirection.Input;

                    _with5.Parameters.Add("SHIPPING_TERMS_MST_PK_IN", INCOTerms).Direction = ParameterDirection.Input;
                    _with5.Parameters["SHIPPING_TERMS_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with5.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;

                    _with5.ExecuteNonQuery();
                    _PkValue = Convert.ToInt64(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);
                }
                HDT.Rows[0]["QUOTATION_SEA_PK"] = _PkValue;
                //by thiyagarajan
                if (CargoType == 3 & cargo == 2)
                {
                    CargoType = 2;
                }
                if (CargoType == 3 & cargo == 1)
                {
                    CargoType = 1;
                }
                //end

                arrMessage = SaveTransaction(PDT, CDT, DSCalculator, OthDT, _PkValue, objWK.MyCommand, objWK.MyUserName, CargoType, Convert.ToInt16(PolPk), Convert.ToInt16(Podpk),
                From_Flag);

                arrMessage = SaveOTHFreights(OthDT, _PkValue, objWK.MyCommand, objWK.MyUserName);

                // If arrMessage.Count > 0 Then
                //     If InStr(CStr(arrMessage(0)).ToUpper, "SAVED") = 0 Then
                //         TRAN.Rollback()
                //         Return arrMessage
                //     End If
                // End If

                // arrMessage = SaveOTHFreights(OTHDT, _PkValue, objWK.MyCommand, objWK.MyUserName)
                //'
                if (AmendFlg == true)
                {
                    string str = null;
                    Int32 intIns = default(Int32);
                    Int32 ValidFor = default(Int32);
                    TimeSpan Span = default(TimeSpan);
                    OracleCommand updCmdUser = new OracleCommand();
                    updCmdUser.Transaction = TRAN;
                    System.DateTime QuotDate = default(System.DateTime);
                    QuotDate = GetQuotDt(PrevQuotPK);
                    if (QuotDate > DateTime.Today.Date)
                    {
                        str = " update QUOTATION_SEA_TBL QT SET QT.STATUS = 3 ,";
                        str += " QT.AMEND_FLAG = 1 ";
                        str += " WHERE QT.QUOTATION_SEA_PK=" + PrevQuotPK;
                    }
                    else
                    {
                        Span = DateTime.Today.Subtract(QuotDate);
                        ValidFor = Span.Days;
                        str = " update QUOTATION_SEA_TBL QT SET QT.EXPECTED_SHIPMENT_DT = SYSDATE ,";
                        str += " QT.AMEND_FLAG = 1 ,";
                        str += " QT.VALID_FOR = " + ValidFor;
                        str += " WHERE QT.QUOTATION_SEA_PK=" + PrevQuotPK;
                    }
                    var _with6 = updCmdUser;
                    _with6.Connection = objWK.MyConnection;
                    _with6.Transaction = TRAN;
                    _with6.CommandType = CommandType.Text;
                    _with6.CommandText = str;
                    intIns = _with6.ExecuteNonQuery();

                }
                //'
                if (arrMessage.Count > 0)
                {
                    if (string.Compare(Convert.ToString(arrMessage[0]).ToUpper(), "SAVED") > 0)
                    {
                        arrMessage.Add("All data Saved successfully");
                        TRAN.Commit();
                        QuotePk = _PkValue;
                        return arrMessage;
                    }
                    else
                    {
                        TRAN.Rollback();
                        //added by surya prasad for implementinf protocol roll back on 18-02-2009
                        if (chkFlag)
                        {
                            RollbackProtocolKey("QUOTATION (SEA)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), Convert.ToString(QuoteNo), System.DateTime.Now);
                            chkFlag = false;
                        }
                        errors = 1;
                        return arrMessage;
                    }
                }

            }
            catch (OracleException oraexp)
            {
                TRAN.Rollback();
                //added by surya prasad for implementinf protocol roll back on 18-02-2009
                if (chkFlag)
                {
                    RollbackProtocolKey("QUOTATION (SEA)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), Convert.ToString(QuoteNo), System.DateTime.Now);
                    chkFlag = false;
                }
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                TRAN.Rollback();
                //added by surya prasad for implementinf protocol roll back on 18-02-2009
                if (chkFlag)
                {
                    RollbackProtocolKey("QUOTATION (SEA)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), Convert.ToString(QuoteNo), System.DateTime.Now);
                    chkFlag = false;
                }
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
            finally
            {
                objWK.MyCommand.Connection.Close();
            }
            return new ArrayList();
        }

        #endregion

        #region " Save Other Freights "

        private ArrayList SaveOTHFreights(DataTable OTHDT, long PkValue, OracleCommand SCM, string UserName)
        {
            Int32 nRowCnt = default(Int32);
            DataRow DR = null;
            long FreightPk = 0;
            arrMessage.Clear();
            try
            {
                var _with7 = SCM;
                for (nRowCnt = 0; nRowCnt <= OTHDT.Rows.Count - 1; nRowCnt++)
                {
                    DR = OTHDT.Rows[nRowCnt];
                    //  If DR[3) = OldPk Then
                    _with7.CommandType = CommandType.StoredProcedure;
                    _with7.CommandText = UserName + ".QUOT_BB_TBL_PKG.QUOT_BB_TRN_SEA_OTH_CHRG_INS";
                    var _with8 = _with7.Parameters;
                    _with8.Clear();
                    _with8.Add("QUOTATION_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    _with8.Add("FREIGHT_ELEMENT_MST_FK_IN", DR["FREIGHT_ELEMENT_MST_FK"]).Direction = ParameterDirection.Input;
                    _with8.Add("CURRENCY_MST_FK_IN", DR["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                    _with8.Add("AMOUNT_IN", getDefault(Convert.ToDouble(DR["AMOUNT"]), 0)).Direction = ParameterDirection.Input;
                    _with8.Add("FREIGHT_TYPE_IN", getDefault(DR["PYMT_TYPE"], 1)).Direction = ParameterDirection.Input;
                    _with8.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with7.ExecuteNonQuery();
                    FreightPk = Convert.ToInt64(_with7.Parameters["RETURN_VALUE"].Value);
                    //  End If
                }
                arrMessage.Add("All data saved successfully");
                return arrMessage;
            }
            catch (OracleException oraexp)
            {
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
        }

        #endregion

        #region " Save Transaction FCL/LCL "
        private ArrayList SaveTransaction(DataTable PDT, DataTable CDT, DataSet DSCalculator, DataTable OthDT, long PkValue, OracleCommand SCM, string UserName, Int16 CargoType, Int16 PolPk, Int16 Podpk,
        int From_Flag)
        {
            Int32 nRowCnt = default(Int32);
            Int32 nRowCnt1 = default(Int32);
            DataRow DR = null;
            Int16 AllInRate = default(Int16);
            Int16 BasisType = default(Int16);
            long TransactionPK = 0;
            long OldPk = 0;
            arrMessage.Clear();
            try
            {
                nRowCnt1 = 1;
                var _with9 = SCM;
                for (nRowCnt = 0; nRowCnt <= PDT.Rows.Count - 1; nRowCnt++)
                {
                    _with9.CommandType = CommandType.StoredProcedure;
                    _with9.CommandText = UserName + ".QUOT_BB_TBL_PKG.QUOT_BB_TRN_SEA_FCL_LCL_ins";
                    DR = PDT.Rows[nRowCnt];
                    var _with10 = _with9.Parameters;
                    _with10.Clear();
                    _with10.Add("QUOTATION_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    _with10.Add("PORT_MST_POL_FK_IN", Convert.ToInt32(PolPk)).Direction = ParameterDirection.Input;
                    _with10.Add("PORT_MST_POD_FK_IN", Convert.ToInt32(Podpk)).Direction = ParameterDirection.Input;
                    //'6 for manual
                    _with10.Add("TRANS_REFERED_FROM_IN", 6).Direction = ParameterDirection.Input;
                    _with10.Add("TRANS_REF_NO_IN", DR["REFNO"]).Direction = ParameterDirection.Input;
                    _with10.Add("OPERATOR_MST_FK_IN", getDefault(DR["OPER_PK"], DBNull.Value)).Direction = ParameterDirection.Input;
                    if (CargoType == 1 | CargoType == 4)
                    {
                        //.Add("CONTAINER_TYPE_MST_FK_IN", DR["CNTR_PK")).Direction = ParameterDirection.Input
                        _with10.Add("EXPECTED_BOXES_IN", DR["QUANTITY"]).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        //  .Add("CONTAINER_TYPE_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input
                        _with10.Add("EXPECTED_BOXES_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    if (CargoType == 2 | CargoType == 4)
                    {
                        _with10.Add("BASIS_IN", DR["BASIS_PK"]).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with10.Add("BASIS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }

                    // .Add("COMMODITY_GROUP_FK_IN", DR["COMM_GRPPK")).Direction = ParameterDirection.Input
                    _with10.Add("COMMODITY_MST_FK_IN", getDefault(DR["COMM_PK"], DBNull.Value)).Direction = ParameterDirection.Input;
                    //.Add("ALL_IN_TARIFF_IN", DR["ALL_IN_TARIFF")).Direction = ParameterDirection.Input
                    //.Add("ALL_IN_QUOTED_TARIFF_IN", DR["ALL_IN_QUOTE")).Direction = ParameterDirection.Input
                    _with10.Add("PACK_TYPE_FK_IN", Convert.ToInt32(DR["PACK_PK"])).Direction = ParameterDirection.Input;

                    if (CargoType == 2 | CargoType == 4)
                    {
                        _with10.Add("EXPECTED_VOLUME_IN", Convert.ToDouble(DR["CARGO_VOL"])).Direction = ParameterDirection.Input;
                        _with10.Add("EXPECTED_WEIGHT_IN", Convert.ToDouble(DR["CARGO_WT"])).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with10.Add("EXPECTED_VOLUME_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        _with10.Add("EXPECTED_WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }

                    //.Add("TRAN_REF_NO2_IN", DR["REF_NO2")).Direction = ParameterDirection.Input
                    //If getDefault(DR["TYPE2"), "0") = "0" Then
                    //    .Add("REF_TYPE2_IN", DR["TYPE2")).Direction = ParameterDirection.Input
                    //Else
                    //    .Add("REF_TYPE2_IN", SourceEnum(DR["TYPE2"))).Direction = ParameterDirection.Input
                    //End If
                    _with10.Add("FROM_FLAG_IN", From_Flag).Direction = ParameterDirection.Input;
                    _with10.Add("BUYING_RATE_IN", Convert.ToDouble(DR["OPERATOR_RATE"])).Direction = ParameterDirection.Input;
                    _with10.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with9.ExecuteNonQuery();
                    TransactionPK = Convert.ToInt64(_with9.Parameters["RETURN_VALUE"].Value);
                    OldPk = Convert.ToInt64(getDefault(DR["PK"], -1));

                    arrMessage = SaveFreights(DR, CDT, TransactionPK, SCM, UserName, CargoType, Convert.ToInt16(nRowCnt1), Convert.ToInt16(PDT.Rows.Count));

                    if ((DSCalculator != null))
                    {
                        SaveBBCargoCalculator(SCM, DSCalculator, TransactionPK, UserName, Convert.ToInt32(DR["SLNO"]));
                    }

                    if (arrMessage.Count > 0)
                    {
                        if (string.Compare(Convert.ToString(arrMessage[0]).ToUpper(), "SAVED") == 0)
                        {
                            return arrMessage;
                        }
                    }

                    // arrMessage = SaveOTHFreights(OthDT, TransactionPK, OldPk, SCM, UserName)
                    if (arrMessage.Count > 0)
                    {
                        if (string.Compare(Convert.ToString(arrMessage[0]).ToUpper(), "SAVED") == 0)
                        {
                            return arrMessage;
                        }
                    }
                    //'comented by subhransu
                    //If DR["COMM_GRPPK") = HAZARDOUS Then
                    //    arrMessage = SaveTransactionHZSpcl(SCM, UserName, getDefault(DR["strSpclReq"), ""), TransactionPK)
                    //ElseIf DR["COMM_GRPPK") = REEFER Then
                    //    arrMessage = SaveTransactionReefer(SCM, UserName, getDefault(DR["strSpclReq"), ""), TransactionPK)
                    //ElseIf DR["COMM_GRPPK") = ODC Then
                    //    arrMessage = SaveTransactionODC(SCM, UserName, getDefault(DR["strSpclReq"), ""), TransactionPK)
                    //End If

                    if (arrMessage.Count > 0)
                    {
                        if (string.Compare(Convert.ToString(arrMessage[0]).ToUpper(), "SAVED") == 0)
                        {
                            return arrMessage;
                        }
                    }
                    nRowCnt1 = nRowCnt1 + 1;
                }
                arrMessage.Add("All data saved successfully");
                return arrMessage;
            }
            catch (OracleException oraexp)
            {
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                return arrMessage;
            }

        }

        //Added by subhransu  for Break Bulk Cargo Calculator
        #region "Cargo Calculator"
        public int SaveBBCargoCalculator(OracleCommand SelectCommand, DataSet M_DataSet, Int64 quotTranPK, string BkgCargoPK, int slno = 0)
        {
            WorkFlow objWK = new WorkFlow();
            long i = 0;
            var strNull = DBNull.Value;
            double LtoMT = 0.98421;
            double FTOM = 0.3048;
            try
            {
                if (M_DataSet.Tables[0].Rows.Count > 0)
                {
                    for (int RowCnt = 0; RowCnt <= M_DataSet.Tables[0].Rows.Count - 1; RowCnt++)
                    {
                        if (Convert.ToInt32(M_DataSet.Tables[0].Rows[RowCnt]["CARGO_CALL_PK"]) <= 0)
                        {
                            if (Convert.ToInt32(M_DataSet.Tables[0].Rows[RowCnt]["QUOTREF"]) == slno)
                            {
                                var _with11 = SelectCommand;
                                _with11.CommandType = CommandType.StoredProcedure;
                                _with11.Parameters.Clear();
                                _with11.CommandText = objWK.MyUserName + ".QUOT_BB_TBL_PKG.QUOT_BB_SEA_CARGO_CALC_INS";
                                _with11.Parameters.Clear();

                                _with11.Parameters.Add("QUOTE_TRN_SEA_FK", Convert.ToInt32(quotTranPK)).Direction = ParameterDirection.Input;
                                _with11.Parameters.Add("NO_OF_PIECES_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["NOP"].ToString()) ? "" : M_DataSet.Tables[0].Rows[RowCnt]["NOP"])).Direction = ParameterDirection.Input;

                                if (Convert.ToInt32(M_DataSet.Tables[0].Rows[RowCnt]["Measurement"]) == 1)
                                {
                                    _with11.Parameters.Add("LENGTH_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["LENGTH"].ToString()) ? 0 : (Convert.ToDouble(M_DataSet.Tables[0].Rows[RowCnt]["LENGTH"]) * FTOM))).Direction = ParameterDirection.Input;
                                    _with11.Parameters.Add("WTUNIT_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["WTUNIT"].ToString()) ? 0 : (Convert.ToDouble(M_DataSet.Tables[0].Rows[RowCnt]["WTUNIT"] )* LtoMT))).Direction = ParameterDirection.Input;
                                    _with11.Parameters.Add("WIDTH_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["Width"].ToString()) ? 0 : (Convert.ToDouble(M_DataSet.Tables[0].Rows[RowCnt]["Width"]) * FTOM))).Direction = ParameterDirection.Input;
                                    _with11.Parameters.Add("HEIGHT_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["Height"].ToString()) ? 0 : (Convert.ToDouble(M_DataSet.Tables[0].Rows[RowCnt]["Height"]) * FTOM))).Direction = ParameterDirection.Input;
                                    _with11.Parameters.Add("CBM_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["Uom"].ToString()) ? 0 : (Convert.ToDouble(M_DataSet.Tables[0].Rows[RowCnt]["Cube"]) * LtoMT))).Direction = ParameterDirection.Input;
                                    _with11.Parameters.Add("WEIGHT_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["Cube"].ToString()) ? 0 : (Convert.ToDouble(M_DataSet.Tables[0].Rows[RowCnt]["Uom"]) * LtoMT))).Direction = ParameterDirection.Input;
                                }
                                else
                                {
                                    _with11.Parameters.Add("LENGTH_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["LENGTH"].ToString()) ? "" : M_DataSet.Tables[0].Rows[RowCnt]["LENGTH"])).Direction = ParameterDirection.Input;
                                    _with11.Parameters.Add("WTUNIT_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["WTUNIT"].ToString()) ? "" : M_DataSet.Tables[0].Rows[RowCnt]["WTUNIT"])).Direction = ParameterDirection.Input;
                                    _with11.Parameters.Add("WIDTH_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["Width"].ToString()) ? "" : M_DataSet.Tables[0].Rows[RowCnt]["Width"])).Direction = ParameterDirection.Input;
                                    _with11.Parameters.Add("HEIGHT_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["Height"].ToString()) ? "" : M_DataSet.Tables[0].Rows[RowCnt]["Height"])).Direction = ParameterDirection.Input;
                                    _with11.Parameters.Add("CBM_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["Uom"].ToString()) ? "" : M_DataSet.Tables[0].Rows[RowCnt]["Cube"])).Direction = ParameterDirection.Input;
                                    _with11.Parameters.Add("WEIGHT_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["Cube"].ToString()) ? "" : M_DataSet.Tables[0].Rows[RowCnt]["Uom"])).Direction = ParameterDirection.Input;
                                }

                                _with11.Parameters.Add("Measurement_In", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["Measurement"].ToString()) ? 0 : M_DataSet.Tables[0].Rows[RowCnt]["Measurement"])).Direction = ParameterDirection.Input;
                                _with11.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                                _with11.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                                _with11.ExecuteNonQuery();
                            }
                        }
                        else
                        {
                            if (Convert.ToInt32(M_DataSet.Tables[0].Rows[RowCnt]["QUOTREF"]) == slno)
                            {
                                //' for update
                                var _with12 = SelectCommand;
                                _with12.CommandText = objWK.MyUserName + ".QUOT_BB_TBL_PKG.QUOT_BB_SEA_CARGO_CALC_UPD";
                                _with12.Parameters.Clear();
                                _with12.Parameters.Add("QUOTATION_SEA_CARGO_CALC_PK_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["CARGO_CALL_PK"].ToString()) ? "" : M_DataSet.Tables[0].Rows[RowCnt]["CARGO_CALL_PK"])).Direction = ParameterDirection.Input;
                                _with12.Parameters.Add("QUOTE_TRN_SEA_FK_IN", quotTranPK).Direction = ParameterDirection.Input;
                                _with12.Parameters.Add("NO_OF_PIECES_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["NOP"].ToString()) ? "" : M_DataSet.Tables[0].Rows[RowCnt]["NOP"])).Direction = ParameterDirection.Input;

                                if (Convert.ToInt32(M_DataSet.Tables[0].Rows[RowCnt]["Measurement"]) == 1)
                                {
                                    _with12.Parameters.Add("LENGTH_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["LENGTH"].ToString()) ? 0 : (Convert.ToDouble(M_DataSet.Tables[0].Rows[RowCnt]["LENGTH"]) * FTOM))).Direction = ParameterDirection.Input;
                                    _with12.Parameters.Add("WTUNIT_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["WTUNIT"].ToString()) ? 0 : (Convert.ToDouble(M_DataSet.Tables[0].Rows[RowCnt]["WTUNIT"]) * LtoMT))).Direction = ParameterDirection.Input;
                                    _with12.Parameters.Add("WIDTH_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["Width"].ToString()) ? 0 : (Convert.ToDouble(M_DataSet.Tables[0].Rows[RowCnt]["Width"]) * FTOM))).Direction = ParameterDirection.Input;
                                    _with12.Parameters.Add("HEIGHT_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["Height"].ToString()) ? 0 : (Convert.ToDouble(M_DataSet.Tables[0].Rows[RowCnt]["Height"]) * FTOM))).Direction = ParameterDirection.Input;
                                    _with12.Parameters.Add("CBM_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["Uom"].ToString()) ? 0 : (Convert.ToDouble(M_DataSet.Tables[0].Rows[RowCnt]["Cube"]) * LtoMT))).Direction = ParameterDirection.Input;
                                    _with12.Parameters.Add("WEIGHT_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["Cube"].ToString()) ? 0 : (Convert.ToDouble(M_DataSet.Tables[0].Rows[RowCnt]["Uom"]) * LtoMT))).Direction = ParameterDirection.Input;
                                }
                                else
                                {
                                    _with12.Parameters.Add("LENGTH_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["LENGTH"].ToString()) ? "" : M_DataSet.Tables[0].Rows[RowCnt]["LENGTH"])).Direction = ParameterDirection.Input;
                                    _with12.Parameters.Add("WTUNIT_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["LENGTH"].ToString()) ? "" : M_DataSet.Tables[0].Rows[RowCnt]["WTUNIT"])).Direction = ParameterDirection.Input;
                                    _with12.Parameters.Add("WIDTH_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["Width"].ToString()) ? "" : M_DataSet.Tables[0].Rows[RowCnt]["Width"])).Direction = ParameterDirection.Input;
                                    _with12.Parameters.Add("HEIGHT_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["Height"].ToString()) ? "" : M_DataSet.Tables[0].Rows[RowCnt]["Height"])).Direction = ParameterDirection.Input;
                                    _with12.Parameters.Add("CBM_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["Cube"].ToString()) ? "" : M_DataSet.Tables[0].Rows[RowCnt]["Cube"])).Direction = ParameterDirection.Input;
                                    _with12.Parameters.Add("WEIGHT_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["Uom"].ToString()) ? "" : M_DataSet.Tables[0].Rows[RowCnt]["Uom"])).Direction = ParameterDirection.Input;
                                }
                                _with12.Parameters.Add("Measurement_in", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["Measurement"].ToString()) ? 0 : M_DataSet.Tables[0].Rows[RowCnt]["Measurement"])).Direction = ParameterDirection.Input;
                                //.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input
                                //.Parameters.Add("LAST_MODIFIED_BY_IN", Session("USER_PK")).Direction = ParameterDirection.Input
                                _with12.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                                _with12.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                                _with12.ExecuteNonQuery();
                            }
                        }
                    }
                }
                if (arrMessage.Count > 0)
                {
                    return 1;
                }
                else
                {
                    return 0;
                }

            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                arrMessage.Add(oraexp.Message);
            }
            catch (Exception ex)
            {
                throw ex;
                arrMessage.Add(ex.Message);
            }
        }
        #endregion
        //'cargo calculator

        private void ModifyOTH(DataRow DR)
        {
            try
            {
                Array arrSTR = null;
                Array arrELE = null;
                string othSTR = Convert.ToString(getDefault(DR["OTH_DTL"], "")).TrimEnd('^');
                long PK = Convert.ToInt64(DR["FK"]);
                string strELE = null;
                string nELE = null;
                string nSTR = null;
                arrSTR = othSTR.Split('^');
                Int16 i = default(Int16);
                Int16 j = default(Int16);

                for (i = 0; i <= arrSTR.Length - 1; i++)
                {
                    strELE = Convert.ToString(arrSTR.GetValue(i));
                    arrELE = strELE.Split('~');
                    nSTR = "";
                    for (j = 0; j <= arrELE.Length - 1; j++)
                    {
                        nSTR += arrELE.GetValue(j) + "~";
                    }
                }
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        private ArrayList Update_Transaction(DataTable PDT, DataTable CDT, DataTable OthDT, DataSet DSCalculator, long PkValue, OracleCommand SCM, string UserName, Int16 CargoType, Int16 PolPk, Int16 Podpk,
        int From_Flag)
        {
            Int32 nRowCnt = default(Int32);
            DataRow DR = null;
            Int16 AllInRate = default(Int16);
            Int16 BasisType = default(Int16);
            long TransactionPK = 0;
            long OldPk = 0;
            arrMessage.Clear();
            CargoType = 4;
            try
            {
                var _with13 = SCM;
                for (nRowCnt = 0; nRowCnt <= PDT.Rows.Count - 1; nRowCnt++)
                {
                    DR = PDT.Rows[nRowCnt];
                    if ((!object.ReferenceEquals(DR["PK"], DBNull.Value)))
                    {
                        _with13.CommandType = CommandType.StoredProcedure;
                        _with13.CommandText = UserName + ".QUOT_BB_TBL_PKG.QUOT_BB_TRN_SEA_FCL_LCL_UPD";

                        var _with14 = _with13.Parameters;
                        _with14.Clear();
                        _with14.Add("QUOTATION_SEA_PK_IN", DR["PK"]).Direction = ParameterDirection.Input;
                        _with14.Add("OPERATOR_MST_FK_IN", getDefault(DR["OPER_PK"], DBNull.Value)).Direction = ParameterDirection.Input;
                        _with14.Add("EXPECTED_BOXES_IN", DR["QUANTITY"]).Direction = ParameterDirection.Input;
                        _with14.Add("BASIS_IN", DR["BASIS_PK"]).Direction = ParameterDirection.Input;
                        _with14.Add("COMMODITY_MST_FK_IN", getDefault(DR["COMM_PK"], DBNull.Value)).Direction = ParameterDirection.Input;
                        _with14.Add("EXPECTED_VOLUME_IN", Convert.ToDouble(DR["CARGO_VOL"])).Direction = ParameterDirection.Input;
                        _with14.Add("EXPECTED_WEIGHT_IN", Convert.ToDouble(DR["CARGO_WT"])).Direction = ParameterDirection.Input;
                        _with14.Add("BUYING_RATE_IN", Convert.ToDouble(DR["OPERATOR_RATE"])).Direction = ParameterDirection.Input;
                        _with14.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        _with13.ExecuteNonQuery();
                        TransactionPK = Convert.ToInt64(_with13.Parameters["RETURN_VALUE"].Value);
                    }
                    else
                    {
                        //'Added for Newly Added Record
                        _with13.CommandType = CommandType.StoredProcedure;
                        _with13.CommandText = UserName + ".QUOT_BB_TBL_PKG.QUOT_BB_TRN_SEA_FCL_LCL_ins";
                        var _with15 = _with13.Parameters;
                        _with15.Clear();
                        _with15.Add("QUOTATION_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input;
                        _with15.Add("PORT_MST_POL_FK_IN", Convert.ToInt32(PolPk)).Direction = ParameterDirection.Input;
                        _with15.Add("PORT_MST_POD_FK_IN", Convert.ToInt32(Podpk)).Direction = ParameterDirection.Input;
                        //'6 for manual
                        _with15.Add("TRANS_REFERED_FROM_IN", 6).Direction = ParameterDirection.Input;
                        _with15.Add("TRANS_REF_NO_IN", DR["REFNO"]).Direction = ParameterDirection.Input;
                        _with15.Add("OPERATOR_MST_FK_IN", getDefault(DR["OPER_PK"], DBNull.Value)).Direction = ParameterDirection.Input;
                        if (CargoType == 1 | CargoType == 4)
                        {
                            _with15.Add("EXPECTED_BOXES_IN", DR["QUANTITY"]).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with15.Add("EXPECTED_BOXES_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        if (CargoType == 2 | CargoType == 4)
                        {
                            _with15.Add("BASIS_IN", DR["BASIS_PK"]).Direction = ParameterDirection.Input;

                        }
                        else
                        {
                            _with15.Add("BASIS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        _with15.Add("COMMODITY_MST_FK_IN", getDefault(DR["COMM_PK"], DBNull.Value)).Direction = ParameterDirection.Input;
                        _with15.Add("PACK_TYPE_FK_IN", Convert.ToInt32(DR["PACK_PK"])).Direction = ParameterDirection.Input;

                        if (CargoType == 2 | CargoType == 4)
                        {
                            _with15.Add("EXPECTED_VOLUME_IN", Convert.ToDouble(DR["CARGO_VOL"])).Direction = ParameterDirection.Input;
                            _with15.Add("EXPECTED_WEIGHT_IN", Convert.ToDouble(DR["CARGO_WT"])).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with15.Add("EXPECTED_VOLUME_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            _with15.Add("EXPECTED_WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        _with15.Add("BUYING_RATE_IN", Convert.ToDouble(DR["OPERATOR_RATE"])).Direction = ParameterDirection.Input;
                        _with15.Add("FROM_FLAG_IN", From_Flag).Direction = ParameterDirection.Input;
                        _with15.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        _with13.ExecuteNonQuery();
                        TransactionPK = Convert.ToInt64(_with13.Parameters["RETURN_VALUE"].Value);
                    }
                    //'Updating the Freight
                    arrMessage = Update_Freights(DR, CDT, TransactionPK, SCM, UserName, CargoType);

                    if ((DSCalculator != null))
                    {
                        //SaveBBCargoCalculator(SCM, DSCalculator, TransactionPK, UserName, DR["SLNO"))
                        SaveBBCargoCalculator(SCM, DSCalculator, TransactionPK, UserName, Convert.ToInt32(DR["SLNR"]));
                    }

                }
                arrMessage.Add("All data saved successfully");
                return arrMessage;
            }
            catch (OracleException oraexp)
            {
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
        }
        #endregion
        #region "Spacial Request"

        private ArrayList SaveTransactionHZSpcl(OracleCommand SCM, string UserName, string strSpclRequest, long PkValue)
        {
            if (!string.IsNullOrEmpty(strSpclRequest))
            {
                arrMessage.Clear();
                string[] strParam = null;
                strParam = strSpclRequest.Split('~');
                try
                {
                    var _with16 = SCM;
                    _with16.CommandType = CommandType.StoredProcedure;
                    _with16.CommandText = UserName + ".QUOT_TRN_SEA_HAZ_SPL_REQ_PKG.QUOT_TRN_SEA_HAZ_SPL_REQ_INS";
                    var _with17 = _with16.Parameters;
                    _with17.Clear();
                    //QUOTE_TRN_SEA_FK_IN()
                    _with17.Add("QUOTE_TRN_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    //OUTER_PACK_TYPE_MST_FK_IN()
                    _with17.Add("OUTER_PACK_TYPE_MST_FK_IN", getDefault(strParam[0], DBNull.Value)).Direction = ParameterDirection.Input;
                    //INNER_PACK_TYPE_MST_FK_IN()
                    _with17.Add("INNER_PACK_TYPE_MST_FK_IN", getDefault(strParam[1], DBNull.Value)).Direction = ParameterDirection.Input;
                    //MIN_TEMP_IN()
                    _with17.Add("MIN_TEMP_IN", getDefault(strParam[2], DBNull.Value)).Direction = ParameterDirection.Input;
                    //MIN_TEMP_UOM_IN()
                    _with17.Add("MIN_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[2]) ? 0 : Convert.ToInt32(strParam[3])), 0)).Direction = ParameterDirection.Input;
                    //MAX_TEMP_IN()
                    _with17.Add("MAX_TEMP_IN", getDefault(strParam[4], DBNull.Value)).Direction = ParameterDirection.Input;
                    //MAX_TEMP_UOM_IN()
                    _with17.Add("MAX_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[4]) ? 0 : Convert.ToInt32(strParam[5])), 0)).Direction = ParameterDirection.Input;
                    //FLASH_PNT_TEMP_IN()
                    _with17.Add("FLASH_PNT_TEMP_IN", getDefault(strParam[6], DBNull.Value)).Direction = ParameterDirection.Input;
                    //FLASH_PNT_TEMP_UOM_IN()
                    _with17.Add("FLASH_PNT_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[6]) ? 0 : Convert.ToInt32(strParam[7])), 0)).Direction = ParameterDirection.Input;
                    //IMDG_CLASS_CODE_IN()
                    _with17.Add("IMDG_CLASS_CODE_IN", getDefault(strParam[8], DBNull.Value)).Direction = ParameterDirection.Input;
                    //UN_NO_IN()
                    _with17.Add("UN_NO_IN", getDefault(strParam[9], DBNull.Value)).Direction = ParameterDirection.Input;
                    //IMO_SURCHARGE_IN()
                    _with17.Add("IMO_SURCHARGE_IN", getDefault(strParam[10], 0)).Direction = ParameterDirection.Input;
                    //SURCHARGE_AMT_IN()
                    _with17.Add("SURCHARGE_AMT_IN", getDefault(strParam[11], 0)).Direction = ParameterDirection.Input;
                    //IS_MARINE_POLLUTANT_IN()
                    _with17.Add("IS_MARINE_POLLUTANT_IN", getDefault(strParam[12], 0)).Direction = ParameterDirection.Input;
                    //EMS_NUMBER_IN()
                    _with17.Add("EMS_NUMBER_IN", getDefault(strParam[13], DBNull.Value)).Direction = ParameterDirection.Input;
                    //RETURN_VALUE()
                    _with17.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with16.ExecuteNonQuery();
                    arrMessage.Add("All data saved successfully");
                    return arrMessage;
                }
                catch (OracleException oraexp)
                {
                    arrMessage.Add(oraexp.Message);
                    return arrMessage;
                }
                catch (Exception ex)
                {
                    arrMessage.Add(ex.Message);
                    return arrMessage;
                }
            }
            arrMessage.Add("All data saved successfully");
            return arrMessage;
        }
        public DataTable fetchSpclReq(string strPK, string strRef)
        {
            try
            {
                if (!string.IsNullOrEmpty(strPK))
                {
                    System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
                    strQuery.Append("SELECT QUOTE_TRN_SEA_HAZ_SPL_PK," );
                    strQuery.Append("QUOTE_TRN_SEA_FK," );
                    strQuery.Append("OUTER_PACK_TYPE_MST_FK," );
                    strQuery.Append("INNER_PACK_TYPE_MST_FK," );
                    strQuery.Append("MIN_TEMP," );
                    strQuery.Append("MIN_TEMP_UOM," );
                    strQuery.Append("MAX_TEMP," );
                    strQuery.Append("MAX_TEMP_UOM," );
                    strQuery.Append("FLASH_PNT_TEMP," );
                    strQuery.Append("FLASH_PNT_TEMP_UOM," );
                    strQuery.Append("IMDG_CLASS_CODE," );
                    strQuery.Append("UN_NO," );
                    strQuery.Append("IMO_SURCHARGE," );
                    strQuery.Append("SURCHARGE_AMT," );
                    strQuery.Append("IS_MARINE_POLLUTANT," );
                    strQuery.Append("EMS_NUMBER FROM QUOTATION_TRN_SEA_HAZ_SPL_REQ Q" );
                    //,QUOTATION_TRN_SEA_FCL_LCL QT,QUOTATION_SEA_TBL QM" & vbCrLf)
                    strQuery.Append("WHERE " );
                    strQuery.Append("Q.QUOTE_TRN_SEA_FK=" + strPK);
                    //strQuery.Append("QT.QUOTATION_SEA_FK=QM.QUOTATION_SEA_PK" & vbCrLf)
                    //strQuery.Append("AND QT.TRANS_REF_NO='" & strRef & "'" & vbCrLf)
                    //strQuery.Append("AND QM.QUOTATION_SEA_PK=" & strPK)
                    return (new WorkFlow()).GetDataTable(strQuery.ToString());
                }
                else
                {
                    return null;
                }
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public int FetchIMOUno(string COM, string IMO, string Unr)
        {
            OracleDataReader dr = null;
            string UNnr = null;
            string strQuery = "SELECT CO.IMDG_CLASS_CODE,CO.UN_NO,CO.COMMODITY_NAME FROM commodity_mst_tbl co WHERE co.commodity_id='" + COM + "'";
            try
            {
                dr = (new WorkFlow()).GetDataReader(strQuery);
                while (dr.Read())
                {
                    IMO = Convert.ToString(getDefault(dr["IMDG_CLASS_CODE"], ""));
                    Unr = Convert.ToString(getDefault(dr["UN_NO"], ""));
                    COM = Convert.ToString(dr["COMMODITY_NAME"]);
                }
                return 1;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                return 0;
            }
            finally
            {
                dr.Close();
            }
        }
        private ArrayList SaveTransactionReefer(OracleCommand SCM, string UserName, string strSpclRequest, long PkValue)
        {

            if (!string.IsNullOrEmpty(strSpclRequest))
            {
                arrMessage.Clear();
                string[] strParam = null;
                strParam = strSpclRequest.Split('~');
                try
                {
                    var _with18 = SCM;
                    _with18.CommandType = CommandType.StoredProcedure;
                    _with18.CommandText = UserName + ".QUOTE_SEA_REF_SPL_REQ_PKG.QUOTE_SEA_REF_SPL_REQ_INS";
                    var _with19 = _with18.Parameters;
                    _with19.Clear();
                    //QUOTE_TRN_SEA_FK_IN()
                    _with19.Add("QUOTE_TRN_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    //VENTILATION_IN()
                    _with19.Add("VENTILATION_IN", strParam[0]).Direction = ParameterDirection.Input;
                    //AIR_COOL_METHOD_IN()
                    _with19.Add("AIR_COOL_METHOD_IN", strParam[1]).Direction = ParameterDirection.Input;
                    //HUMIDITY_FACTOR_IN()
                    _with19.Add("HUMIDITY_FACTOR_IN", getDefault(strParam[2], DBNull.Value)).Direction = ParameterDirection.Input;
                    //IS_PERISHABLE_GOODS_IN()
                    _with19.Add("IS_PERISHABLE_GOODS_IN", strParam[3]).Direction = ParameterDirection.Input;
                    //MIN_TEMP_IN()
                    _with19.Add("MIN_TEMP_IN", getDefault(strParam[4], DBNull.Value)).Direction = ParameterDirection.Input;
                    //MIN_TEMP_UOM_IN()
                    _with19.Add("MIN_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[4]) ? 0 : Convert.ToInt32(strParam[5])), 0)).Direction = ParameterDirection.Input;
                    //MAX_TEMP_IN()
                    _with19.Add("MAX_TEMP_IN", getDefault(strParam[6], DBNull.Value)).Direction = ParameterDirection.Input;
                    //MAX_TEMP_UOM_IN()
                    _with19.Add("MAX_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[6]) ? 0 : Convert.ToInt32(strParam[7])), 0)).Direction = ParameterDirection.Input;
                    //PACK_TYPE_MST_FK_IN()
                    _with19.Add("PACK_TYPE_MST_FK_IN", getDefault(strParam[8], DBNull.Value)).Direction = ParameterDirection.Input;
                    //PACK_COUNT_IN()
                    _with19.Add("PACK_COUNT_IN", getDefault(strParam[9], 0)).Direction = ParameterDirection.Input;
                    //RETURN_VALUE()
                    _with19.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with18.ExecuteNonQuery();
                    arrMessage.Add("All data saved successfully");
                    return arrMessage;
                }
                catch (OracleException oraexp)
                {
                    arrMessage.Add(oraexp.Message);
                    return arrMessage;
                }
                catch (Exception ex)
                {
                    arrMessage.Add(ex.Message);
                    return arrMessage;
                }
            }
            arrMessage.Add("All data saved successfully");
            return arrMessage;
        }
        public DataTable fetchSpclReqReefer(string strPK, string strRef)
        {
            try
            {
                if (!string.IsNullOrEmpty(strPK))
                {
                    System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
                    strQuery.Append("SELECT QUOTE_SEA_REF_SPL_REQ_PK," );
                    strQuery.Append("QUOTE_TRN_SEA_FK," );
                    strQuery.Append("VENTILATION," );
                    strQuery.Append("AIR_COOL_METHOD," );
                    strQuery.Append("HUMIDITY_FACTOR," );
                    strQuery.Append("IS_PERISHABLE_GOODS," );
                    strQuery.Append("MIN_TEMP," );
                    strQuery.Append("MIN_TEMP_UOM," );
                    strQuery.Append("MAX_TEMP," );
                    strQuery.Append("MAX_TEMP_UOM," );
                    strQuery.Append("PACK_TYPE_MST_FK," );
                    strQuery.Append("Q.PACK_COUNT " );
                    strQuery.Append("FROM QUOTE_SEA_REF_SPL_REQ Q" );
                    //,QUOTATION_TRN_SEA_FCL_LCL QT,QUOTATION_SEA_TBL QM" & vbCrLf)
                    strQuery.Append("WHERE " );
                    strQuery.Append("Q.QUOTE_TRN_SEA_FK=" + strPK);
                    //strQuery.Append("QUOTATION_SEA_TBL QM," & vbCrLf)
                    //strQuery.Append("QUOTATION_TRN_SEA_FCL_LCL QT" & vbCrLf)
                    //strQuery.Append("WHERE " & vbCrLf)
                    //strQuery.Append("Q.QUOTE_TRN_SEA_FK=QT.QUOTE_TRN_SEA_PK" & vbCrLf)
                    //strQuery.Append("AND QT.QUOTATION_SEA_FK=QM.QUOTATION_SEA_PK" & vbCrLf)
                    //strQuery.Append("AND QT.TRANS_REF_NO='" & strRef & "'" & vbCrLf)
                    //strQuery.Append("AND QM.QUOTATION_SEA_PK=" & strPK)
                    return (new WorkFlow()).GetDataTable(strQuery.ToString());
                }
                else
                {
                    return null;
                }
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public DataTable fetchSpclReqODC(string strPK, string strRef)
        {
            try
            {
                if (!string.IsNullOrEmpty(strPK))
                {
                    System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
                    strQuery.Append("SELECT " );
                    strQuery.Append("QUOTE_SEA_ODC_SPL_REQ_PK," );
                    strQuery.Append("QUOTE_TRN_SEA_FK," );
                    strQuery.Append("LENGTH," );
                    strQuery.Append("LENGTH_UOM_MST_FK," );
                    strQuery.Append("HEIGHT," );
                    strQuery.Append("HEIGHT_UOM_MST_FK," );
                    strQuery.Append("WIDTH," );
                    strQuery.Append("WIDTH_UOM_MST_FK," );
                    strQuery.Append("WEIGHT," );
                    strQuery.Append("WEIGHT_UOM_MST_FK," );
                    strQuery.Append("VOLUME," );
                    strQuery.Append("VOLUME_UOM_MST_FK," );
                    strQuery.Append("SLOT_LOSS," );
                    strQuery.Append("LOSS_QUANTITY," );
                    strQuery.Append("APPR_REQ " );
                    strQuery.Append("FROM QUOTE_SEA_ODC_SPL_REQ Q" );
                    //,QUOTATION_TRN_SEA_FCL_LCL QT,QUOTATION_SEA_TBL QM" & vbCrLf)
                    strQuery.Append("WHERE " );
                    strQuery.Append("Q.QUOTE_TRN_SEA_FK=" + strPK);
                    //strQuery.Append("QUOTATION_SEA_TBL QM," & vbCrLf)
                    //strQuery.Append("QUOTATION_TRN_SEA_FCL_LCL QT" & vbCrLf)
                    //strQuery.Append("WHERE " & vbCrLf)
                    //strQuery.Append("Q.QUOTE_TRN_SEA_FK=QT.QUOTE_TRN_SEA_PK" & vbCrLf)
                    //strQuery.Append("AND QT.QUOTATION_SEA_FK=QM.QUOTATION_SEA_PK" & vbCrLf)
                    //strQuery.Append("AND QT.TRANS_REF_NO='" & strRef & "'" & vbCrLf)
                    //strQuery.Append("AND QM.QUOTATION_SEA_PK=" & strPK)
                    return (new WorkFlow()).GetDataTable(strQuery.ToString());
                }
                else
                {
                    return null;
                }
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        private ArrayList SaveTransactionODC(OracleCommand SCM, string UserName, string strSpclRequest, long PkValue)
        {
            if (!string.IsNullOrEmpty(strSpclRequest))
            {
                arrMessage.Clear();
                string[] strParam = null;
                strParam = strSpclRequest.Split('~');
                try
                {
                    var _with20 = SCM;
                    _with20.CommandType = CommandType.StoredProcedure;
                    _with20.CommandText = UserName + ".QUOTE_SEA_ODC_SPL_REQ_PKG.QUOTE_SEA_ODC_SPL_REQ_INS";
                    var _with21 = _with20.Parameters;
                    _with21.Clear();
                    //QUOTE_TRN_SEA_FK_IN()
                    _with21.Add("QUOTE_TRN_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    //LENGTH_IN()
                    _with21.Add("LENGTH_IN", getDefault(strParam[0], DBNull.Value)).Direction = ParameterDirection.Input;
                    //LENGTH_UOM_MST_FK_IN()
                    _with21.Add("LENGTH_UOM_MST_FK_IN", getDefault((string.IsNullOrEmpty(strParam[0]) ? 0 : Convert.ToInt32(strParam[1])), 0)).Direction = ParameterDirection.Input;
                    //HEIGHT_IN()
                    _with21.Add("HEIGHT_IN", getDefault(strParam[2], DBNull.Value)).Direction = ParameterDirection.Input;
                    //HEIGHT_UOM_MST_FK_IN()
                    _with21.Add("HEIGHT_UOM_MST_FK_IN", getDefault((string.IsNullOrEmpty(strParam[2]) ? 0 : Convert.ToInt32(strParam[3])), 0)).Direction = ParameterDirection.Input;
                    //WIDTH_IN()
                    _with21.Add("WIDTH_IN", getDefault(strParam[4], 0)).Direction = ParameterDirection.Input;
                    //WIDTH_UOM_MST_FK_IN()
                    _with21.Add("WIDTH_UOM_MST_FK_IN", getDefault((string.IsNullOrEmpty(strParam[4]) ? 0 : Convert.ToInt32(strParam[5])), 0)).Direction = ParameterDirection.Input;
                    //WEIGHT_IN()
                    _with21.Add("WEIGHT_IN", getDefault(strParam[6], DBNull.Value)).Direction = ParameterDirection.Input;
                    //WEIGHT_UOM_MST_FK_IN()
                    _with21.Add("WEIGHT_UOM_MST_FK_IN", getDefault(strParam[7], 0)).Direction = ParameterDirection.Input;
                    //VOLUME_IN()
                    _with21.Add("VOLUME_IN", getDefault(strParam[8], DBNull.Value)).Direction = ParameterDirection.Input;
                    //VOLUME_UOM_MST_FK_IN()
                    _with21.Add("VOLUME_UOM_MST_FK_IN", getDefault(strParam[9], 0)).Direction = ParameterDirection.Input;
                    //SLOT_LOSS_IN()
                    _with21.Add("SLOT_LOSS_IN", getDefault(strParam[10], 0)).Direction = ParameterDirection.Input;
                    //LOSS_QUANTITY_IN()
                    _with21.Add("LOSS_QUANTITY_IN", getDefault(strParam[11], DBNull.Value)).Direction = ParameterDirection.Input;
                    //APPR_REQ_IN()
                    _with21.Add("APPR_REQ_IN", getDefault(strParam[12], 0)).Direction = ParameterDirection.Input;
                    //RETURN_VALUE()
                    _with21.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with20.ExecuteNonQuery();
                    arrMessage.Add("All data saved successfully");
                    return arrMessage;
                }
                catch (OracleException oraexp)
                {
                    arrMessage.Add(oraexp.Message);
                    return arrMessage;
                }
                catch (Exception ex)
                {
                    arrMessage.Add(ex.Message);
                    return arrMessage;
                }
            }
            arrMessage.Add("All data saved successfully");
            return arrMessage;
        }
        #endregion

        #region "Delete"
        public ArrayList Delete(object UWGCOMMODITY, int Commpk, int Quotationpk)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            OracleCommand delCommand = new OracleCommand();
            Int16 Count = default(Int16);
            Int32 RecAfct = default(Int32);

            try
            {
                //Ask Query
                //for (int Count = 0; Count <= UWGCOMMODITY.rows.count() - 1; Count++)
                //{

                //    if ((UWGCOMMODITY.rows(Count).cells(7).value == "true" | UWGCOMMODITY.rows(Count).cells(7).value == true) & Commpk == UWGCOMMODITY.rows(Count).cells(3).value)
                //    {
                //        var _with22 = delCommand;
                //        _with22.Connection = objWK.MyConnection;
                //        _with22.CommandType = CommandType.StoredProcedure;
                //        _with22.CommandText = objWK.MyUserName + ".QUOT_BB_TBL_PKG.QUOT_BB_SEA_TBL_DEL";
                //        var _with23 = _with22.Parameters;
                //        _with23.Add("QUOTATION_SEA_FK_IN", Convert.ToInt32(Quotationpk)).Direction = ParameterDirection.Input;
                //        _with23.Add("COMMODITY_MST_FK_IN", Convert.ToInt32(Commpk)).Direction = ParameterDirection.Input;
                //        _with23.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                //        _with23.Add("DELETED_BY_FK_IN", LAST_MODIFIED_BY).Direction = ParameterDirection.Input;
                //        _with23.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                //        var _with24 = objWK.MyDataAdapter;
                //        _with24.DeleteCommand = delCommand;
                //        _with24.DeleteCommand.Transaction = TRAN;
                //        RecAfct = _with24.DeleteCommand.ExecuteNonQuery();
                //    }
                //    //'end of if statement
                //}

                if (RecAfct > 0)
                {
                    TRAN.Commit();
                    arrMessage.Add("Deleted");
                    return arrMessage;
                }
                else
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWK.MyConnection.Close();
            }

        }
        #endregion

        #region " Freight Elemenmts "

        private ArrayList SaveFreights(DataRow PDR, DataTable CDT, long TrnPKValue, OracleCommand SCM, string UserName, Int16 CargoType, Int16 nRowCnt1, Int16 Rcnt)
        {
            Int32 nRowCnt = default(Int32);
            DataRow DR = null;
            bool Flag = false;
            Int16 AllInRate = default(Int16);
            Int16 ChkAdvatos = default(Int16);
            long FreightPk = 0;
            arrMessage.Clear();
            Int16 NRow = default(Int16);
            try
            {
                NRow = 0;

                var _with25 = SCM;

                for (nRowCnt = 0; nRowCnt <= CDT.Rows.Count - 1; nRowCnt++)
                {
                    _with25.CommandType = CommandType.StoredProcedure;
                    _with25.CommandText = UserName + ".QUOT_BB_TBL_PKG.QUOT_BB_TRN_SEA_FRT_DTLS_INS";
                    DR = CDT.Rows[nRowCnt];
                    if (DR["COMM_PK"] == PDR["COMM_PK"])
                    {
                        Flag = false;
                        if (CargoType == 1)
                        {
                            if (DR["REF_NO"] == PDR["REF_NO"] & DR["POL_PK"] == PDR["POL_PK"] & DR["POD_PK"] == PDR["POD_PK"] & DR["CNTR_PK"] == PDR["CNTR_PK"])
                            {
                                Flag = true;
                            }
                        }
                        else
                        {
                            //If DR["REF_NO") = PDR["REF_NO") And _
                            //        DR["POL_PK") = PDR["POL_PK") And _
                            //        DR["POD_PK") = PDR["POD_PK") And _
                            //        DR["LCLBASIS") = PDR["LCL_BASIS") Then
                            //    Flag = True
                            //End If COMENTED BY SUBHRANSU
                            if (DR["REFNO"] == PDR["REFNO"])
                            {
                                Flag = true;
                            }
                        }
                        int payment = 0;
                        payment = 1;
                        if (Flag == true)
                        {
                            var _with26 = _with25.Parameters;
                            _with26.Clear();
                            AllInRate = Convert.ToInt16(DR["SEL"] == "true" ? 1 : 0);
                            ChkAdvatos = Convert.ToInt16(DR["ADVATOS"] == "true" ? 1 : 0);
                            _with26.Add("QUOTE_TRN_SEA_FK_IN", TrnPKValue).Direction = ParameterDirection.Input;
                            _with26.Add("FREIGHT_ELEMENT_MST_FK_IN", DR["FREIGHT_ELEMENT_MST_PK"]).Direction = ParameterDirection.Input;
                            _with26.Add("CHECK_FOR_ALL_IN_RT_IN", AllInRate).Direction = ParameterDirection.Input;
                            _with26.Add("CURRENCY_MST_FK_IN", DR["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
                            //  .Add("TARIFF_RATE_IN", DR["RATE")).Direction = ParameterDirection.Input
                            //.Add("QUOTED_RATE_IN", DR["RATE")).Direction = ParameterDirection.Input
                            _with26.Add("QUOTED_RATE_IN", (string.IsNullOrEmpty(DR["RATE"].ToString()) ? 0 : Convert.ToDouble(DR["RATE"]))).Direction = ParameterDirection.Input;
                            //.Add("FINAL_AMOUNT_IN", DR["FINAL_AMOUNT")).Direction = ParameterDirection.Input
                            _with26.Add("FINAL_AMOUNT_IN", (string.IsNullOrEmpty(DR["FINAL_AMOUNT"].ToString()) ? 0 : Convert.ToDouble(DR["FINAL_AMOUNT"]))).Direction = ParameterDirection.Input;
                            if (DR["PYMT_TYPE"] == "Collect" | DR["PYMT_TYPE"] == "2")
                            {
                                payment = 2;
                            }
                            else
                            {
                                payment = 1;
                            }

                            _with26.Add("PYMT_TYPE_IN", payment).Direction = ParameterDirection.Input;
                            _with26.Add("ROE_IN", Convert.ToDouble(DR["EXCHANGE_RATE"])).Direction = ParameterDirection.Input;
                            _with26.Add("SURCHARGE_IN", DR["SURCHARGE"]).Direction = ParameterDirection.Input;
                            //'for surcharge
                            _with26.Add("CHECK_ADVATOS_IN", ChkAdvatos).Direction = ParameterDirection.Input;
                            //'Added By Koteshwari for Advatos
                            if (CargoType == 1)
                            {
                                _with26.Add("QUOTED_MIN_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                                //Added by rabbani reason USS Gap,introduced New Column Min.Qte.Rate
                            }
                            else
                            {
                                //.Add("QUOTED_MIN_RATE_IN", DR["QUOTED_MIN_RATE")).Direction = ParameterDirection.Input 'Added by rabbani reason USS Gap,introduced New Column Min.Qte.Rate
                                _with26.Add("QUOTED_MIN_RATE_IN", (string.IsNullOrEmpty(DR["QUOTED_MIN_RATE"].ToString()) ? 0 : Convert.ToDouble(DR["QUOTED_MIN_RATE"]))).Direction = ParameterDirection.Input;
                                //Added by rabbani reason USS Gap,introduced New Column Min.Qte.Rate
                            }
                            _with26.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                            _with25.ExecuteNonQuery();
                            FreightPk = Convert.ToInt64(_with25.Parameters["RETURN_VALUE"].Value);
                        }
                    }
                    //'end if 
                }
                arrMessage.Add("All data saved successfully");
                return arrMessage;
            }
            catch (OracleException oraexp)
            {
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
        }

        private ArrayList Update_Freights(DataRow PDR, DataTable CDT, long TrnPKValue, OracleCommand SCM, string UserName, Int16 CargoType)
        {
            Int32 nRowCnt = default(Int32);
            DataRow DR = null;
            bool Flag = false;
            Int16 AllInRate = default(Int16);
            Int16 ChkAdvatos = default(Int16);
            long FreightPk = 0;
            Int16 payment = default(Int16);
            arrMessage.Clear();
            try
            {
                var _with27 = SCM;
                for (nRowCnt = 0; nRowCnt <= CDT.Rows.Count - 1; nRowCnt++)
                {
                    DR = CDT.Rows[nRowCnt];
                    Flag = false;
                    if ((!object.ReferenceEquals(DR["BASISPK"], DBNull.Value)))
                    {
                        //'For Update
                        _with27.CommandType = CommandType.StoredProcedure;
                        _with27.CommandText = UserName + ".QUOT_BB_TBL_PKG.QUOT_BB_TRN_SEA_FRT_DTLS_UPD";

                        if (DR["COMM_PK"] == PDR["COMM_PK"])
                        {
                            Flag = true;
                        }
                        else
                        {
                            Flag = false;
                        }

                        payment = 1;
                        if (Flag == true)
                        {
                            var _with28 = _with27.Parameters;
                            _with28.Clear();
                            AllInRate = Convert.ToInt16(DR["SEL"].ToString() == "true" ? 1 : 0);
                            ChkAdvatos = Convert.ToInt16(DR["ADVATOS"] == "true" ? 1 : 0);
                            _with28.Add("QUOTE_TRN_SEA_FRT_PK_IN", DR["BASISPK"]).Direction = ParameterDirection.Input;
                            _with28.Add("QUOTE_TRN_SEA_FK_IN", TrnPKValue).Direction = ParameterDirection.Input;
                            _with28.Add("FREIGHT_ELEMENT_MST_FK_IN", DR["FREIGHT_ELEMENT_MST_PK"]).Direction = ParameterDirection.Input;
                            _with28.Add("CHECK_FOR_ALL_IN_RT_IN", AllInRate).Direction = ParameterDirection.Input;
                            _with28.Add("CURRENCY_MST_FK_IN", DR["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
                            //.Add("QUOTED_RATE_IN", DR["RATE")).Direction = ParameterDirection.Input
                            _with28.Add("QUOTED_RATE_IN", (string.IsNullOrEmpty(DR["RATE"].ToString()) ? 0 : Convert.ToDouble(DR["RATE"]))).Direction = ParameterDirection.Input;
                            //.Add("FINAL_AMOUNT_IN", DR["FINAL_AMOUNT")).Direction = ParameterDirection.Input
                            _with28.Add("FINAL_AMOUNT_IN", (string.IsNullOrEmpty(DR["FINAL_AMOUNT"].ToString()) ? 0 : Convert.ToDouble(DR["FINAL_AMOUNT"]))).Direction = ParameterDirection.Input;
                            if (DR["PYMT_TYPE"] == "Collect" | DR["PYMT_TYPE"] == "2")
                            {
                                payment = 2;
                            }
                            else
                            {
                                payment = 1;
                            }

                            _with28.Add("PYMT_TYPE_IN", payment).Direction = ParameterDirection.Input;
                            _with28.Add("ROE_IN", Convert.ToDouble(DR["EXCHANGE_RATE"])).Direction = ParameterDirection.Input;
                            _with28.Add("SURCHARGE_IN", DR["SURCHARGE"]).Direction = ParameterDirection.Input;
                            //'for surcharge
                            _with28.Add("CHECK_ADVATOS_IN", ChkAdvatos).Direction = ParameterDirection.Input;
                            //'Added By Koteshwari for Advatos
                            if (CargoType == 1)
                            {
                                _with28.Add("QUOTED_MIN_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                                //Added by rabbani reason USS Gap,introduced New Column Min.Qte.Rate
                            }
                            else
                            {
                                //.Add("QUOTED_MIN_RATE_IN", DR["QUOTED_MIN_RATE")).Direction = ParameterDirection.Input 'Added by rabbani reason USS Gap,introduced New Column Min.Qte.Rate
                                _with28.Add("QUOTED_MIN_RATE_IN", (string.IsNullOrEmpty(DR["QUOTED_MIN_RATE"].ToString()) ? 0 : Convert.ToDouble(DR["QUOTED_MIN_RATE"]))).Direction = ParameterDirection.Input;
                                //Added by rabbani reason USS Gap,introduced New Column Min.Qte.Rate
                            }
                            _with28.Add("RETURN_VALUE", OracleDbType.Int32, 20, "RETURN_VALUE").Direction = ParameterDirection.Output;
                            _with27.ExecuteNonQuery();
                            FreightPk = Convert.ToInt64(_with27.Parameters["RETURN_VALUE"].Value);
                        }
                        //'else of main if
                    }
                    else
                    {
                        //'For Insert the Freight Details
                        _with27.CommandType = CommandType.StoredProcedure;
                        _with27.CommandText = UserName + ".QUOT_BB_TBL_PKG.QUOT_BB_TRN_SEA_FRT_DTLS_INS";
                        if (DR["COMM_PK"] == PDR["COMM_PK"])
                        {
                            Flag = true;
                            payment = 1;
                            if (Flag == true)
                            {
                                var _with29 = _with27.Parameters;
                                _with29.Clear();
                                AllInRate = Convert.ToInt16(DR["SEL"].ToString() == "true" ? 1 : 0);
                                ChkAdvatos = Convert.ToInt16(DR["ADVATOS"] == "true" ? 1 : 0);
                                _with29.Add("QUOTE_TRN_SEA_FK_IN", TrnPKValue).Direction = ParameterDirection.Input;
                                _with29.Add("FREIGHT_ELEMENT_MST_FK_IN", DR["FREIGHT_ELEMENT_MST_PK"]).Direction = ParameterDirection.Input;
                                _with29.Add("CHECK_FOR_ALL_IN_RT_IN", AllInRate).Direction = ParameterDirection.Input;
                                _with29.Add("CURRENCY_MST_FK_IN", DR["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
                                //.Add("QUOTED_RATE_IN", DR["RATE")).Direction = ParameterDirection.Input
                                _with29.Add("QUOTED_RATE_IN", (string.IsNullOrEmpty(DR["RATE"].ToString()) ? 0 : Convert.ToDouble(DR["RATE"]))).Direction = ParameterDirection.Input;
                                //.Add("FINAL_AMOUNT_IN", DR["FINAL_AMOUNT")).Direction = ParameterDirection.Input
                                _with29.Add("FINAL_AMOUNT_IN", (string.IsNullOrEmpty(DR["FINAL_AMOUNT"].ToString()) ? 0 : Convert.ToDouble(DR["FINAL_AMOUNT"]))).Direction = ParameterDirection.Input;
                                if (DR["PYMT_TYPE"] == "Collect")
                                {
                                    payment = 2;
                                }
                                else
                                {
                                    payment = 1;
                                }

                                _with29.Add("PYMT_TYPE_IN", payment).Direction = ParameterDirection.Input;
                                _with29.Add("ROE_IN", Convert.ToDouble(DR["EXCHANGE_RATE"])).Direction = ParameterDirection.Input;
                                _with29.Add("SURCHARGE_IN", DR["SURCHARGE"]).Direction = ParameterDirection.Input;
                                //'for surcharge
                                _with29.Add("CHECK_ADVATOS_IN", ChkAdvatos).Direction = ParameterDirection.Input;
                                if (CargoType == 1)
                                {
                                    _with29.Add("QUOTED_MIN_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                                    //Added by rabbani reason USS Gap,introduced New Column Min.Qte.Rate
                                }
                                else
                                {
                                    //.Add("QUOTED_MIN_RATE_IN", DR["QUOTED_MIN_RATE")).Direction = ParameterDirection.Input 'Added by rabbani reason USS Gap,introduced New Column Min.Qte.Rate
                                    _with29.Add("QUOTED_MIN_RATE_IN", (string.IsNullOrEmpty(DR["QUOTED_MIN_RATE"].ToString()) ? 0 : Convert.ToDouble(DR["QUOTED_MIN_RATE"]))).Direction = ParameterDirection.Input;
                                    //Added by rabbani reason USS Gap,introduced New Column Min.Qte.Rate
                                }
                                _with29.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                                _with27.ExecuteNonQuery();
                                FreightPk = Convert.ToInt64(_with27.Parameters["RETURN_VALUE"].Value);
                            }
                        }
                        //'end if 

                    }
                }
                arrMessage.Add("All data saved successfully");
                return arrMessage;
            }
            catch (OracleException oraexp)
            {
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
        }

        #endregion

        #region " Protocol No Generation "

        public new string GenerateQuoteNo(long nLocationId, long nEmployeeId, long nCreatedBy, WorkFlow ObjWK = null, int From_Flag = 0)
        {
            string functionReturnValue = null;
            //'Export
            if (From_Flag == 0)
            {
                functionReturnValue = GenerateProtocolKey("QUOTATION (SEA)", nLocationId, nEmployeeId, DateTime.Now, "", "", "", nCreatedBy, ObjWK);
            }
            else
            {
                functionReturnValue = GenerateProtocolKey("IMPORT QUOTATION SEA", nLocationId, nEmployeeId, DateTime.Now, "", "","" , nCreatedBy, ObjWK);
            }
            return functionReturnValue;
        }

        #endregion

        #endregion

        #region " Get Address of Quotation "

        public DataTable GetAddressOfQuotation(long QuotationPK)
        {
            string strSQL = null;
            strSQL = " Select COL_PLACE_MST_FK, col.PLACE_CODE colplace, CMT.COL_ADDRESS," + " DEL_PLACE_MST_FK, del.PLACE_CODE delplace,CMT.DEL_ADDRESS " + " from PLACE_MST_TBL col, PLACE_MST_TBL del, QUOTATION_SEA_TBL q ,CUSTOMER_MST_TBL CMT" + " where q.COL_PLACE_MST_FK = col.PLACE_PK(+) and " + "       q.DEL_PLACE_MST_FK = del.PLACE_PK(+) and " + "       Q.CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK and " + "       QUOTATION_SEA_PK = " + QuotationPK;
            try
            {
                return (new WorkFlow()).GetDataTable(strSQL);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public DataTable GetAddressOfCustomer(long CustomerPK, Int16 CustomerType)
        {
            string strSQL = null;
            strSQL = " Select '' COL_PLACE_MST_FK, '' colplace, COL_ADDRESS, " + " '' DEL_PLACE_MST_FK, '' delplace, DEL_ADDRESS " + " from V_ALL_CUSTOMER cm " + " where cm.CUSTOMER_MST_PK = " + CustomerPK + "   and cm.CUSTOMER_TYPE = " + CustomerType;
            try
            {
                return (new WorkFlow()).GetDataTable(strSQL);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion

        #region " Update Quotation [ Status ] "

        public ArrayList UpdateQuotation(DataTable HDT, DataTable PDT, DataTable CDT, DataSet DSCalculator, DataTable OthDT, string QuotationPk, string ValidFor, Int16 CargoType, string Status, string ExpectedShipmentDate,
        string remarks, string CargoMoveCode, string Header = "", string Footer = "", Int32 updation = 0, int PYMTType = 0, int INCOTerms = 0, int PolPk = 0, int Podpk = 0, int From_Flag = 0,
        bool Customer_Approved = false)
        {


            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            Int32 prvstatus = default(Int32);
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            arrMessage.Clear();
            objWK.MyCommand.Transaction = TRAN;
            try
            {
                //modified by Thiyagarajan on 1/7/08 for fcl and lcl quotation task
                if (updation <= 1)
                {
                    var _with30 = objWK.MyCommand;
                    _with30.CommandType = CommandType.StoredProcedure;
                    _with30.CommandText = objWK.MyUserName + ".QUOT_BB_TBL_PKG.QUOT_BB_SEA_TBL_UPD";
                    var _with31 = _with30.Parameters;
                    //.Clear()
                    // OracleDbType.Int32, 10
                    _with31.Add("QUOTATION_SEA_PK_IN", QuotationPk).Direction = ParameterDirection.Input;
                    _with31.Add("VALID_FOR_IN", ValidFor).Direction = ParameterDirection.Input;
                    _with31.Add("STATUS_IN", Status).Direction = ParameterDirection.Input;
                    _with31.Add("EXPECTED_SHIPMENT_DT_IN", ExpectedShipmentDate).Direction = ParameterDirection.Input;
                    _with31.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                    _with31.Add("VERSION_NO_IN", Version_No).Direction = ParameterDirection.Input;
                    _with31.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                    _with31.Add("COL_PLACE_MST_FK_IN", OracleDbType.Int32, 10).Direction = ParameterDirection.Input;
                    _with31.Add("COL_ADDRESS_IN", OracleDbType.Varchar2, 200).Direction = ParameterDirection.Input;
                    _with31.Add("DEL_PLACE_MST_FK_IN", OracleDbType.Int32, 10).Direction = ParameterDirection.Input;
                    _with31.Add("DEL_ADDRESS_IN", OracleDbType.Varchar2, 200).Direction = ParameterDirection.Input;
                    //Added by Venkata on 14/12/06
                    _with31.Add("REMARKS_IN", OracleDbType.Varchar2, 250).Direction = ParameterDirection.Input;
                    _with31.Add("CARGO_MOVE_CODE_IN", OracleDbType.Int32, 10).Direction = ParameterDirection.Input;
                    //End
                    //added by gopi
                    _with31.Add("Header_IN", OracleDbType.Varchar2, 2000).Direction = ParameterDirection.Input;
                    _with31.Add("Footer_IN", OracleDbType.Varchar2, 2000).Direction = ParameterDirection.Input;
                    //end
                    _with31.Add("SHIPPING_TERMS_MST_PK_IN", INCOTerms).Direction = ParameterDirection.Input;
                    _with31.Add("PYMTType_IN", PYMTType).Direction = ParameterDirection.Input;
                    _with31.Add("FROM_FLAG_IN", getDefault(From_Flag, 0)).Direction = ParameterDirection.Input;
                    _with31.Add("CUSTOMER_APPROVED_IN", (Customer_Approved ? 1 : 0)).Direction = ParameterDirection.Input;
                    _with31.Add("RETURN_VALUE", OracleDbType.Varchar2, 20, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with30.Parameters["COL_PLACE_MST_FK_IN"].Value = getDefault(HDT.Rows[0]["COL_PLACE_MST_FK"], DBNull.Value);
                    _with30.Parameters["COL_ADDRESS_IN"].Value = getDefault(HDT.Rows[0]["COL_ADDRESS"], DBNull.Value);
                    // = "", DBNull.Value, HDT.Rows(0)["COL_ADDRESS") = "")
                    _with30.Parameters["DEL_PLACE_MST_FK_IN"].Value = getDefault(HDT.Rows[0]["DEL_PLACE_MST_FK"], DBNull.Value);
                    _with30.Parameters["DEL_ADDRESS_IN"].Value = getDefault(HDT.Rows[0]["DEL_ADDRESS"], DBNull.Value);
                    //Added by Venkata 14/12/06
                    _with30.Parameters["REMARKS_IN"].Value = getDefault(remarks, DBNull.Value);
                    //IIf(remarks = "", DBNull.Value, remarks)
                    _with30.Parameters["CARGO_MOVE_CODE_IN"].Value = getDefault(CargoMoveCode, DBNull.Value);
                    //end
                    //added by gopi
                    _with30.Parameters["Header_IN"].Value = getDefault(Header, DBNull.Value);
                    _with30.Parameters["Footer_IN"].Value = getDefault(Footer, DBNull.Value);

                    _with30.ExecuteNonQuery();
                    _PkValue = Convert.ToInt64(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);
                }
                //modified by Thiyagarajan on 1/7/08 for fcl and lcl quotation task
                if (updation > 1)
                {
                    _PkValue = Convert.ToInt64(QuotationPk);
                }
                arrMessage = Update_Transaction(PDT, CDT, OthDT, DSCalculator, _PkValue, objWK.MyCommand, objWK.MyUserName, CargoType, Convert.ToInt16(PolPk), Convert.ToInt16(Podpk),From_Flag);

                if (prvstatus != 2 & prvstatus != 3)
                {
                    updateOthCharge(PDT, OthDT, objWK.MyCommand, objWK.MyUserName, Convert.ToInt32(_PkValue));
                }

                if (arrMessage.Count > 0)
                {
                    if (string.Compare(arrMessage[0].ToString(), "saved")>0)
                    {
                        TRAN.Commit();
                        arrMessage.Add("All data saved successfully");
                    }
                    else
                    {
                        TRAN.Rollback();
                        errors = 1;
                    }
                }
                else
                {
                    TRAN.Commit();
                    arrMessage.Add("All data saved successfully");
                }
                return arrMessage;
            }
            catch (OracleException oraexp)
            {
                TRAN.Rollback();
                errors = 1;
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                TRAN.Rollback();
                errors = 1;
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
            finally
            {
                objWK.MyCommand.Connection.Close();
            }
        }
        public int getStatus(string pk)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            OracleDataReader dr = null;
            try
            {
                strQuery.Append("SELECT STATUS" );
                strQuery.Append("FROM QUOTATION_SEA_TBL AIR" );
                strQuery.Append("WHERE AIR.QUOTATION_SEA_PK =" );
                strQuery.Append(pk);
                dr = (new WorkFlow()).GetDataReader(strQuery.ToString());
                while (dr.Read())
                {
                    return Convert.ToInt32(dr[0]);
                }
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                return 0;
            }
            finally
            {
                dr.Close();
            }
            return 0;
        }
        private ArrayList updateOthCharge(DataTable PDT, DataTable OthDT, OracleCommand SCM, string UserName, int Quotpk)
        {
            DataRow Pdr = null;
            DataRow Odr = null;
            int delFlage = 0;
            int RowCnot = 0;
            int nRowCnot = 0;

            try
            {

                // For Each Pdr In PDT.Rows comented by subhransu
                delFlage = 1;
                //end
                // For Each Odr In OthDT.Rows
                //If OthDT.Rows.Count > 0 Then
                //    If OthDT.Rows.Count > 2 Then
                //        nRowCnot = (OthDT.Rows.Count / 2)
                //    Else
                //        nRowCnot = OthDT.Rows.Count
                //    End If
                //End If


                for (RowCnot = 0; RowCnot <= OthDT.Rows.Count - 1; RowCnot++)
                {
                    Odr = OthDT.Rows[RowCnot];
                    SCM.CommandType = CommandType.StoredProcedure;
                    SCM.CommandText = UserName + ".QUOT_BB_TBL_PKG.QUOT_BB_TRN_SEA_OTH_CHRG_UPD";
                    // If Odr["QUOTATION_TRN_SEA_FK") = Pdr["FK") Then
                    //  If Odr["QUOTATION_TRN_SEA_FK") = Quotpk Then
                    var _with32 = SCM.Parameters;
                    _with32.Clear();
                    _with32.Add("DEL_FLAG", delFlage);
                    _with32.Add("QUOTATION_SEA_FK_IN", Quotpk);
                    _with32.Add("FREIGHT_ELEMENT_MST_FK_IN", Odr["FREIGHT_ELEMENT_MST_FK"]);
                    _with32.Add("CURRENCY_MST_FK_IN", Odr["CURRENCY_MST_FK"]);
                    _with32.Add("AMOUNT_IN", getDefault(Odr["AMOUNT"], 0));
                    _with32.Add("FREIGHT_TYPE_IN", getDefault(Odr["PYMT_TYPE"], 1)).Direction = ParameterDirection.Input;
                    _with32.Add("RETURN_VALUE", OracleDbType.Varchar2, 20, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    delFlage += SCM.ExecuteNonQuery();
                    //  End If
                    //  Next
                    //If Pdr["COMM_GRPPK") = HAZARDOUS Then
                    //    arrMessage = SaveTransactionHZSpcl(SCM, UserName, getDefault(Pdr["strSpclReq"), ""), Pdr["FK"))
                    //ElseIf Pdr["COMM_GRPPK") = REEFER Then
                    //    arrMessage = SaveTransactionReefer(SCM, UserName, getDefault(Pdr["strSpclReq"), ""), Pdr["FK"))
                    //ElseIf Pdr["COMM_GRPPK") = ODC Then
                    //    arrMessage = SaveTransactionODC(SCM, UserName, getDefault(Pdr["strSpclReq"), ""), Pdr["FK"))
                    //End If
                    if (arrMessage.Count > 0)
                    {
                        if (string.Compare(Convert.ToString(arrMessage[0]).ToUpper(), "SAVED") == 0)
                        {
                            return arrMessage;
                        }
                    }
                }

                arrMessage.Add("saved");
                return arrMessage;
                //QUOTATION_SEA_FK_IN()
                //QUOTATION_TRN_REF()
                //FREIGHT_ELEMENT_MST_FK_IN()
                //CURRENCY_MST_FK_IN()
                //AMOUNT_IN()
                //RETURN_VALUE()
            }
            catch (OracleException oraexp)
            {
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
        }

        #endregion

        #region " Header - Info. "

        public DataTable HeaderInfo(string QuotationPK, bool AmendFlag = false)
        {
            if (AmendFlag == true)
            {
                QuotationPK = "";
            }
            if (string.IsNullOrEmpty(QuotationPK))
                QuotationPK = "-1";
            string strSQL = null;
            strSQL = " Select QUOTATION_SEA_PK,     " +  "       QUOTATION_REF_NO,                                                   " +  "       to_char(QUOTATION_DATE,'" + dateFormat + "')        QUOTATION_DATE,         " +  "       VALID_FOR,                                                          " +  "       CARGO_TYPE,                                                         " +  "       PYMT_TYPE,                                                          " +  "       QUOTED_BY,                                                          " +  "       COL_PLACE_MST_FK,                                                   " +  "       COL_ADDRESS,                                                        " +  "       DEL_PLACE_MST_FK,                                                   " +  "       DEL_ADDRESS,                                                        " +  "       AGENT_MST_FK,                                                       " +  "       STATUS,                                                             " +  "       CREATED_BY_FK,                                                      " +  "       to_char(CREATED_DT,'" + dateFormat + "')            CREATED_DT,             " +  "       LAST_MODIFIED_BY_FK,                                                " +  "       to_char(LAST_MODIFIED_DT,'" + dateFormat + "')      LAST_MODIFIED_DT,       " +  "       Version_No,                                                         " +  "       to_char(EXPECTED_SHIPMENT_DT,'" + dateFormat + "')  EXPECTED_SHIPMENT_DT,   " +  "       CUSTOMER_MST_FK,                                                    " +  "       CUSTOMER_CATEGORY_MST_FK,                                           " +  "       SPECIAL_INSTRUCTIONS,                                               " +  "       CUST_TYPE ,                                                        " +  "       CREDIT_DAYS,                                                       " +  "       CREDIT_LIMIT,BASE_CURRENCY_FK                                      " +  "  from QUOTATION_SEA_TBL                                                 " +  "  Where  QUOTATION_SEA_PK = " + QuotationPK;
            strSQL = strSQL.Replace("   ", " ");
            strSQL = strSQL.Replace("  ", " ");
            try
            {
                return (new WorkFlow()).GetDataTable(strSQL);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion

        #region "Fetch_Fcl_Lcl_Header"
        public DataSet Header_Fcl_Lcl(string QuotationPk)
        {
            try
            {
                WorkFlow ObjWk = new WorkFlow();
                StringBuilder strquery = new StringBuilder();

                //strquery.Append("SELECT     DISTINCT                                    " & vbCrLf)
                //strquery.Append("    tran3.quote_trn_sea_pk                     PK," & vbCrLf)
                //strquery.Append("     OPR3.OPERATOR_NAME                         OPER_NAME,    " & vbCrLf)
                //strquery.Append("     DECODE(MAIN3.PYMT_TYPE,1,'PREPAID','COLLECT') P_TYPE_ID,                 " & vbCrLf)
                //strquery.Append("     PORTPOL3.PORT_ID                           POL_ID,              " & vbCrLf)
                //strquery.Append("     PORTPOD3.PORT_ID                           POD_ID, " & vbCrLf)
                //strquery.Append("     MAIN3.QUOTATION_DATE                       VALID_FROM," & vbCrLf)
                //strquery.Append("     CNTR3.CONTAINER_TYPE_MST_ID                CNTR_ID,             " & vbCrLf)
                //strquery.Append("     DECODE(MAIN3.CARGO_TYPE,1,'FCL','LCL')  CARGO_TYPE," & vbCrLf)
                //strquery.Append("      DUMT.DIMENTION_ID                                LCLBASIS " & vbCrLf)
                //strquery.Append(" FROM                                                             " & vbCrLf)
                //strquery.Append("     QUOTATION_SEA_TBL              MAIN3,                           " & vbCrLf)
                //strquery.Append("     QUOTATION_TRN_SEA_FCL_LCL      TRAN3,                           " & vbCrLf)
                //strquery.Append("     PORT_MST_TBL                   PORTPOL3,                        " & vbCrLf)
                //strquery.Append("     PORT_MST_TBL                   PORTPOD3,                        " & vbCrLf)
                //strquery.Append("     OPERATOR_MST_TBL               OPR3,                            " & vbCrLf)
                //strquery.Append("     CONTAINER_TYPE_MST_TBL         CNTR3," & vbCrLf)
                //strquery.Append("     DIMENTION_UNIT_MST_TBL          DUMT                            " & vbCrLf)
                //strquery.Append("   WHERE    MAIN3.QUOTATION_SEA_PK      = TRAN3.QUOTATION_SEA_FK             " & vbCrLf)
                //strquery.Append("     AND    TRAN3.PORT_MST_POL_FK       = PORTPOL3.PORT_MST_PK(+)            " & vbCrLf)
                //strquery.Append("     AND    TRAN3.PORT_MST_POD_FK       = PORTPOD3.PORT_MST_PK(+)            " & vbCrLf)
                //strquery.Append("     AND    TRAN3.OPERATOR_MST_FK       = OPR3.OPERATOR_MST_PK(+)            " & vbCrLf)
                //strquery.Append("     AND    TRAN3.CONTAINER_TYPE_MST_FK = CNTR3.CONTAINER_TYPE_MST_PK(+)     " & vbCrLf)
                //strquery.Append("     AND    TRAN3.BASIS = DUMT.DIMENTION_UNIT_MST_PK(+)" & vbCrLf)
                //strquery.Append("     AND    MAIN3.QUOTATION_SEA_PK =  " & QuotationPk)
                strquery.Append(" SELECT     DISTINCT                                    " );
                strquery.Append("     MAIN3.QUOTATION_SEA_PK                     PK," );
                strquery.Append("     tran3.quote_trn_sea_pk                     Fk," );
                strquery.Append("     OPR3.OPERATOR_NAME                         OPER_NAME,    " );
                strquery.Append("     DECODE(MAIN3.PYMT_TYPE,1,'PREPAID','COLLECT') P_TYPE_ID,                 " );
                strquery.Append("     PORTPOL3.PORT_NAME                           POL_ID,              " );
                strquery.Append("     PORTPOD3.PORT_NAME                           POD_ID, " );
                strquery.Append("     MAIN3.QUOTATION_DATE                       VALID_FROM," );
                strquery.Append("     MAIN3.HEADER_CONTENT  HEADER, " );
                strquery.Append("     MAIN3.FOOTER_CONTENT  FOOTER, " );
                //'comented by subhransu strquery.Append("     CNTR3.CONTAINER_TYPE_MST_ID                CNTR_ID,             " & vbCrLf)
                strquery.Append("     CASE WHEN MAIN3.CARGO_TYPE = 4 THEN  TO_CHAR(DUMT.DIMENTION_ID) " );
                strquery.Append("     ELSE  TO_CHAR(CNTR3.CONTAINER_TYPE_MST_ID) END CNTR_ID," );
                //modified by Thiyagarjan on 30/6/08 for fcl and lcl quotation
                //strquery.Append("     DECODE(MAIN3.CARGO_TYPE,1,'FCL','LCL')  CARGO_TYPE," & vbCrLf)
                //'Modified by Shubranshu for BreakBulk Cargo Type
                //strquery.Append("    ( case when TRAN3.container_type_mst_fk is not null then 'FCL' else 'LCL' end )  CARGO_TYPE," & vbCrLf)
                strquery.Append("    DECODE(MAIN3.CARGO_TYPE,1,'FCL',2,'LCL',4,'BBC')CARGO_TYPE," );
                //end
                strquery.Append("      DUMT.DIMENTION_ID      LCLBASIS " );
                strquery.Append(" FROM                                                             " );
                strquery.Append("     QUOTATION_SEA_TBL              MAIN3,                           " );
                strquery.Append("     QUOTATION_TRN_SEA_FCL_LCL      TRAN3,                           " );
                strquery.Append("     PORT_MST_TBL                   PORTPOL3,                        " );
                strquery.Append("     PORT_MST_TBL                   PORTPOD3,                        " );
                strquery.Append("     OPERATOR_MST_TBL               OPR3,                            " );
                strquery.Append("     CONTAINER_TYPE_MST_TBL         CNTR3," );
                strquery.Append("     DIMENTION_UNIT_MST_TBL          DUMT                            " );
                strquery.Append("   WHERE    MAIN3.QUOTATION_SEA_PK      = TRAN3.QUOTATION_SEA_FK             " );
                strquery.Append("     AND    TRAN3.PORT_MST_POL_FK       = PORTPOL3.PORT_MST_PK(+)            " );
                strquery.Append("     AND    TRAN3.PORT_MST_POD_FK       = PORTPOD3.PORT_MST_PK(+)            " );
                strquery.Append("     AND    TRAN3.OPERATOR_MST_FK       = OPR3.OPERATOR_MST_PK(+)            " );
                strquery.Append("     AND    TRAN3.CONTAINER_TYPE_MST_FK = CNTR3.CONTAINER_TYPE_MST_PK(+)     " );
                strquery.Append("     AND    TRAN3.BASIS = DUMT.DIMENTION_UNIT_MST_PK(+)" );
                strquery.Append("     AND    MAIN3.QUOTATION_SEA_PK =  " + QuotationPk);
                return ObjWk.GetDataSet(strquery.ToString());
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Fetch_Fcl_Lcl_Main"
        public DataSet Main_Fcl_Lcl(string QuotationPk)
        {
            try
            {
                WorkFlow ObjWk = new WorkFlow();
                StringBuilder strquery = new StringBuilder();
                strquery.Append("SELECT DISTINCT TRAN3.QUOTATION_SEA_FK PK," );
                strquery.Append("                TRAN3.QUOTE_TRN_SEA_PK," );
                strquery.Append("       CASE WHEN  NVL(TRAN3.EXPECTED_BOXES,0)=0 THEN 'NA' " );
                strquery.Append("        ELSE TRAN3.EXPECTED_BOXES||''  END QUANTITY, " );
                strquery.Append("        " );
                //  strquery.Append("                CMDT4.COMMODITY_GROUP_CODE  COMM_ID," & vbCrLf)
                //Added by subhransu
                strquery.Append("  CMDT3.COMMODITY_NAME COMM_ID," );

                strquery.Append("                TRAN3.Expected_Weight      GROSSWEIGTH," );
                strquery.Append("                TRAN3.Expected_Volume      VOLUME," );
                strquery.Append("                cmt.cargo_move_code," );
                strquery.Append("                main3.cargo_type" );
                strquery.Append("  FROM QUOTATION_SEA_TBL         MAIN3," );
                strquery.Append("       QUOTATION_TRN_SEA_FCL_LCL TRAN3," );
                strquery.Append("       COMMODITY_MST_TBL         CMDT3," );
                strquery.Append("       COMMODITY_GROUP_MST_TBL CMDT4 ," );
                strquery.Append("       cargo_move_mst_tbl      cmt" );
                strquery.Append(" WHERE MAIN3.QUOTATION_SEA_PK = TRAN3.QUOTATION_SEA_FK" );
                //'added by subhransu
                strquery.Append("   AND tran3.commodity_mst_fk = cmdt3.commodity_mst_pk(+)" );

                strquery.Append("    AND TRAN3.COMMODITY_GROUP_FK = CMDT4.COMMODITY_GROUP_PK(+)" );
                strquery.Append("    and main3.cargo_move_fk=cmt.cargo_move_pk(+)" );
                strquery.Append("    AND MAIN3.QUOTATION_SEA_PK = " + QuotationPk);
                return ObjWk.GetDataSet(strquery.ToString());
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Fetch_Fcl_Lcl_Charges_Description"
        public DataSet Charges_Description(string QuotationPk, System.DateTime QuotationDate, Int64 baseCurrency)
        {
            try
            {
                WorkFlow ObjWk = new WorkFlow();
                StringBuilder strquery = new StringBuilder();
                QuotationDate = QuotationDate.Date;
                strquery.Append("SELECT TRAN3.QUOTATION_SEA_FK PK," );
                strquery.Append("       TRAN3.QUOTE_TRN_SEA_PK," );
                //'comented for surcharge strquery.Append("       FRT3.FREIGHT_ELEMENT_NAME FRT_NAME," & vbCrLf)            
                strquery.Append("       CASE WHEN FRTD3.SURCHARGE IS NULL THEN  " );
                strquery.Append("       FRT3.FREIGHT_ELEMENT_NAME  ELSE (FRT3.FREIGHT_ELEMENT_NAME ||' ( '||FRTD3.Surcharge||' ) '||'')  END FRT_NAME," );

                strquery.Append("       CURR3.CURRENCY_ID CURR_ID," );
                //'comented by subhransu strquery.Append("       FRTD3.QUOTED_RATE QUOTERATE1," & vbCrLf)
                strquery.Append("       CASE WHEN FRTD3.QUOTED_RATE > FRTD3.QUOTED_MIN_RATE THEN  FRTD3.QUOTED_RATE  ELSE FRTD3.QUOTED_MIN_RATE END QUOTERATE1," );

                strquery.Append("       ROUND((SELECT GET_EX_RATE(FRTD3.CURRENCY_MST_FK, " + baseCurrency + ",TO_DATE('" + QuotationDate + "', DATEFORMAT)) FROM DUAL), 6) ROE," );
                //comented by subhransu strquery.Append("       ROUND(FRTD3.QUOTED_RATE * (SELECT GET_EX_RATE(FRTD3.CURRENCY_MST_FK, " & baseCurrency & ",TO_DATE('" & QuotationDate & "', DATEFORMAT))FROM DUAL), 2) QUOTERATE," & vbCrLf)
                //'comented by subhransu strquery.Append("       ROUND(NVL(CASE WHEN FRTD3.QUOTED_RATE > FRTD3.QUOTED_MIN_RATE THEN  FRTD3.QUOTED_RATE  ELSE FRTD3.QUOTED_MIN_RATE END,0) * (SELECT GET_EX_RATE(FRTD3.CURRENCY_MST_FK, " & baseCurrency & ",TO_DATE('" & QuotationDate & "', DATEFORMAT))FROM DUAL), 2) QUOTERATE," & vbCrLf)
                strquery.Append("       FRTD3.Rate QUOTERATE," );
                //'added by subhransu
                strquery.Append("        CASE WHEN DUMT.DIMENTION_ID = 'CBM' THEN (NVL(TRAN3.EXPECTED_VOLUME, 0) * NVL(FRTD3.RATE, 0))" );
                strquery.Append("        WHEN DUMT.DIMENTION_ID = 'MT' THEN  (NVL(TRAN3.EXPECTED_WEIGHT, 0) * NVL(FRTD3.RATE, 0))" );
                strquery.Append("        WHEN DUMT.DIMENTION_ID = 'UNIT' THEN (NVL(TRAN3.EXPECTED_BOXES, 0) * NVL(FRTD3.RATE, 0))" );
                strquery.Append("        WHEN DUMT.DIMENTION_ID = 'W/M' THEN   CASE  WHEN (NVL(TRAN3.EXPECTED_VOLUME, 0)) >  (NVL(TRAN3.EXPECTED_WEIGHT, 0)) THEN" );
                strquery.Append("        (NVL(TRAN3.EXPECTED_VOLUME, 0) * NVL(FRTD3.RATE, 0)) ELSE (NVL(TRAN3.EXPECTED_WEIGHT, 0) * NVL(FRTD3.RATE, 0)) END END QUOTFINALRATE, " );

                //'added by subhransu
                strquery.Append("       DUMT.DIMENTION_ID" );

                strquery.Append("  FROM QUOTATION_SEA_TBL          MAIN3," );
                strquery.Append("       QUOTATION_TRN_SEA_FCL_LCL  TRAN3," );
                strquery.Append("       QUOTATION_TRN_SEA_FRT_DTLS FRTD3," );
                strquery.Append("       FREIGHT_ELEMENT_MST_TBL    FRT3," );
                strquery.Append("       CURRENCY_TYPE_MST_TBL      CURR3," );
                strquery.Append("       DIMENTION_UNIT_MST_TBL  DUMT" );
                strquery.Append("       " );
                strquery.Append(" WHERE MAIN3.QUOTATION_SEA_PK = TRAN3.QUOTATION_SEA_FK" );
                strquery.Append("   AND TRAN3.QUOTE_TRN_SEA_PK = FRTD3.QUOTE_TRN_SEA_FK" );
                strquery.Append("   AND FRTD3.FREIGHT_ELEMENT_MST_FK = FRT3.FREIGHT_ELEMENT_MST_PK(+)" );
                strquery.Append("   AND TRAN3.BASIS=DUMT.DIMENTION_UNIT_MST_PK(+)" );
                //strquery.Append("   AND FRT3.CHARGE_BASIS <> 2" & vbCrLf)
                strquery.Append("   AND FRTD3.CURRENCY_MST_FK = CURR3.CURRENCY_MST_PK(+)" );
                strquery.Append("   AND MAIN3.QUOTATION_SEA_PK = " + QuotationPk);
                strquery.Append("   order by preference" );
                return ObjWk.GetDataSet(strquery.ToString());
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Fetch_Fcl_Lcl_Other_Charges"
        public DataSet Other_Charges(string QuotationPk)
        {
            try
            {
                WorkFlow ObjWk = new WorkFlow();
                StringBuilder strquery = new StringBuilder();

                //strquery.Append("SELECT fcl_lcl.quote_trn_sea_pk," & vbCrLf)
                //strquery.Append("       FRT3.FREIGHT_ELEMENT_NAME," & vbCrLf)
                //strquery.Append("       CURR3.CURRENCY_ID," & vbCrLf)
                //strquery.Append("       QUOT_OTHER.AMOUNT" & vbCrLf)
                //strquery.Append("  FROM QUOTATION_SEA_TBL          MAIN1," & vbCrLf)
                //strquery.Append("       QUOTATION_TRN_SEA_FCL_LCL  FCL_LCL," & vbCrLf)
                //strquery.Append("       QUOTATION_TRN_SEA_OTH_CHRG QUOT_OTHER," & vbCrLf)
                //strquery.Append("       FREIGHT_ELEMENT_MST_TBL    FRT3," & vbCrLf)
                //strquery.Append("       CURRENCY_TYPE_MST_TBL      CURR3" & vbCrLf)
                //strquery.Append(" WHERE FCL_LCL.QUOTATION_SEA_FK = MAIN1.QUOTATION_SEA_PK" & vbCrLf)
                //strquery.Append("   AND QUOT_OTHER.QUOTATION_TRN_SEA_FK = FCL_LCL.QUOTE_TRN_SEA_PK" & vbCrLf)
                //strquery.Append("   AND QUOT_OTHER.FREIGHT_ELEMENT_MST_FK = FRT3.FREIGHT_ELEMENT_MST_PK" & vbCrLf)
                //strquery.Append("   AND QUOT_OTHER.CURRENCY_MST_FK = CURR3.CURRENCY_MST_PK" & vbCrLf)
                //strquery.Append("   AND MAIN1.QUOTATION_SEA_PK = " & QuotationPk)

                strquery.Append("SELECT MAIN1.QUOTATION_SEA_PK," );
                strquery.Append("       fcl_lcl.quote_trn_sea_pk," );
                strquery.Append("       FRT3.FREIGHT_ELEMENT_NAME," );
                strquery.Append("       CURR3.CURRENCY_ID," );
                strquery.Append("       GET_EX_RATE(QUOT_OTHER.CURRENCY_MST_FK," +HttpContext.Current.Session["CURRENCY_MST_PK"] + ", SYSDATE) ROE," );
                strquery.Append("       QUOT_OTHER.AMOUNT," );
                strquery.Append("       (GET_EX_RATE(QUOT_OTHER.CURRENCY_MST_FK," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ", SYSDATE) * QUOT_OTHER.AMOUNT) QUOTEAMOUNT" );

                strquery.Append("  FROM QUOTATION_SEA_TBL          MAIN1," );
                strquery.Append("       QUOTATION_TRN_SEA_FCL_LCL  FCL_LCL," );
                strquery.Append("       QUOTATION_TRN_SEA_OTH_CHRG QUOT_OTHER," );
                strquery.Append("       FREIGHT_ELEMENT_MST_TBL    FRT3," );
                strquery.Append("       CURRENCY_TYPE_MST_TBL      CURR3" );
                strquery.Append(" WHERE FCL_LCL.QUOTATION_SEA_FK = MAIN1.QUOTATION_SEA_PK" );
                //strquery.Append("   AND QUOT_OTHER.QUOTATION_TRN_SEA_FK = FCL_LCL.QUOTE_TRN_SEA_PK" & vbCrLf)
                strquery.Append("   AND QUOT_OTHER.QUOTATION_SEA_FK =  MAIN1.QUOTATION_SEA_PK" );
                strquery.Append("   AND QUOT_OTHER.FREIGHT_ELEMENT_MST_FK = FRT3.FREIGHT_ELEMENT_MST_PK" );
                strquery.Append("   AND QUOT_OTHER.CURRENCY_MST_FK = CURR3.CURRENCY_MST_PK" );
                strquery.Append("   AND MAIN1.QUOTATION_SEA_PK =  " + QuotationPk);
                return ObjWk.GetDataSet(strquery.ToString());
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Fetch Location"
        public DataSet FetchLocation(long Loc)
        {

            WorkFlow ObjWk = new WorkFlow();
            StringBuilder StrSqlBuilder = new StringBuilder();
            StrSqlBuilder.Append("  SELECT L.Office_Name CORPORATE_NAME,");
            StrSqlBuilder.Append("  COP.GST_NO,COP.COMPANY_REG_NO,COP.HOME_PAGE URL, ");
            StrSqlBuilder.Append("  L.LOCATION_ID , L.LOCATION_NAME, ");
            StrSqlBuilder.Append("  L.ADDRESS_LINE1,L.ADDRESS_LINE2,L.ADDRESS_LINE3,L.TELE_PHONE_NO,L.FAX_NO,L.E_MAIL_ID,");
            StrSqlBuilder.Append("  L.CITY,CMST.COUNTRY_NAME COUNTRY,L.ZIP, L.LOCATION_MST_PK");
            StrSqlBuilder.Append("  FROM CORPORATE_MST_TBL COP,LOCATION_MST_TBL L,COUNTRY_MST_TBL CMST");
            StrSqlBuilder.Append("  WHERE CMST.COUNTRY_MST_PK(+)=L.COUNTRY_MST_FK AND L.LOCATION_MST_PK = " + Loc + "");

            try
            {
                return ObjWk.GetDataSet(StrSqlBuilder.ToString());
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Fetch Custumer "
        public DataSet Fetch_Custumer(int CustPk, string CustId = "")
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strQuery.Append("  " );
                strQuery.Append("         SELECT Q.CUSTOMER_NAME," );
                strQuery.Append("         Q.ADM_ADDRESS_1," );
                strQuery.Append("         case when Q.ADM_ADDRESS_2 is null then ' ' else Q.ADM_ADDRESS_2 end ADM_ADDRESS_2," );
                strQuery.Append("         case when Q.ADM_ADDRESS_3 is null then ' ' else Q.ADM_ADDRESS_3 end ADM_ADDRESS_3," );
                strQuery.Append("         case when Q.ADM_ZIP_CODE is null then ' ' else Q.ADM_ZIP_CODE end ADM_ZIP_CODE," );
                strQuery.Append("         case when Q.ADM_CITY is null then ' ' else Q.ADM_CITY end ADM_CITY," );
                strQuery.Append("         case when Q.COUNTRY_NAME is null then ' ' else Q.COUNTRY_NAME end COUNTRY_NAME" );
                strQuery.Append("         FROM  (");
                strQuery.Append("  SELECT CUST.CUSTOMER_NAME," );
                strQuery.Append("         CC.ADM_ADDRESS_1," );
                strQuery.Append("         CC.ADM_ADDRESS_2," );
                strQuery.Append("         CC.ADM_ADDRESS_3," );
                strQuery.Append("         CC.ADM_ZIP_CODE," );
                strQuery.Append("         CC.ADM_CITY," );
                strQuery.Append("         CCC.COUNTRY_NAME" );
                strQuery.Append("    FROM CUSTOMER_MST_TBL      CUST," );
                strQuery.Append("         CUSTOMER_CONTACT_DTLS CC," );
                strQuery.Append("         COUNTRY_MST_TBL       CCC" );
                strQuery.Append("   WHERE CC.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK" );
                strQuery.Append("     AND CC.ADM_COUNTRY_MST_FK = CCC.COUNTRY_MST_PK" );
                strQuery.Append("     AND CUST.CUSTOMER_MST_PK =" + CustPk);
                if (CustId.Trim().Length > 0)
                    strQuery.Append("     AND CUST.CUSTOMER_ID = '" + CustId + "'");
                strQuery.Append("" );
                strQuery.Append(" UNION " );
                strQuery.Append("" );
                strQuery.Append("  SELECT CUST.CUSTOMER_NAME," );
                strQuery.Append("         CC.ADM_ADDRESS_1," );
                strQuery.Append("         CC.ADM_ADDRESS_2," );
                strQuery.Append("         CC.ADM_ADDRESS_3," );
                strQuery.Append("         CC.ADM_ZIP_CODE," );
                strQuery.Append("         CC.ADM_CITY," );
                strQuery.Append("         (select ccc.country_name from country_mst_tbl ccc, location_mst_tbl l ");
                strQuery.Append("         where l.country_mst_fk = ccc.country_mst_pk ");
                strQuery.Append("         and l.location_mst_pk = cc.adm_location_mst_fk)  country_name" );
                strQuery.Append("    FROM TEMP_CUSTOMER_TBL      CUST," );
                strQuery.Append("         TEMP_CUSTOMER_CONTACT_DTLS CC" );
                strQuery.Append("   WHERE CC.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK" );
                strQuery.Append("     AND CUST.CUSTOMER_MST_PK =" + CustPk);
                if (CustId.Trim().Length > 0)
                    strQuery.Append("     AND CUST.CUSTOMER_ID = '" + CustId + "'");
                strQuery.Append("     )Q" );

                return objWF.GetDataSet(strQuery.ToString());
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "COmmodity Grid Query"
        public DataSet Fetch_Commodity(int QuotPk)
        {
            try
            {
                WorkFlow objWF = new WorkFlow();
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                if (QuotPk > 0)
                {
                    sb.Append(" SELECT ROWNUM SLNO,");
                    sb.Append("       CG.COMMODITY_GROUP_PK,");
                    sb.Append("       CG.COMMODITY_GROUP_CODE,");
                    sb.Append("       CMT.COMMODITY_MST_PK,");
                    sb.Append("       CMT.COMMODITY_ID,");
                    sb.Append("       CMT.COMMODITY_NAME,");
                    sb.Append("       '1' SEL,");
                    sb.Append("        '" + QuotPk + "' QUOTPK,");
                    sb.Append("       '' DEL");
                    sb.Append("  FROM QUOTATION_TRN_SEA_FCL_LCL QT,");
                    sb.Append("       COMMODITY_GROUP_MST_TBL   CG,");
                    sb.Append("       COMMODITY_MST_TBL         CMT");
                    sb.Append(" WHERE QT.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK");
                    sb.Append("   AND CG.COMMODITY_GROUP_PK(+) =CMT.COMMODITY_GROUP_FK");
                    sb.Append("   AND QT.QUOTATION_SEA_FK = " + QuotPk);
                }
                else
                {
                    sb.Append("SELECT ROWNUM SLNO,CG.COMMODITY_GROUP_PK,");
                    sb.Append("       CG.COMMODITY_GROUP_CODE,");
                    sb.Append("       CMT.COMMODITY_MST_PK,");
                    sb.Append("       CMT.COMMODITY_ID,");
                    sb.Append("       CMT.COMMODITY_NAME,");
                    sb.Append("       '' SEL,");
                    sb.Append("       '' QUOTPK,");
                    sb.Append("       '' DEL");
                    sb.Append("  FROM COMMODITY_GROUP_MST_TBL CG, COMMODITY_MST_TBL CMT");
                    sb.Append(" WHERE CG.COMMODITY_GROUP_PK = CMT.COMMODITY_GROUP_FK AND 1=2");
                }
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Get Quotation Status"
        public int GetQuotStatus(string QuotationPK)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append("   SELECT COUNT(Q.QUOTATION_SEA_PK)");
            sb.Append("   FROM QUOTATION_SEA_TBL Q");
            sb.Append("   WHERE Q.STATUS <> 2");
            sb.Append("   AND Q.STATUS <> 4");
            sb.Append("   AND Q.QUOTATION_SEA_PK = " + QuotationPK);
            try
            {
                return Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
            }
            catch (OracleException sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw;
            }
        }
        #endregion
        public System.DateTime GetQuotDt(string QuotPK)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();

            sb.Append("   SELECT Q.QUOTATION_DATE ");
            sb.Append("   FROM QUOTATION_SEA_TBL Q");
            sb.Append("   WHERE Q.QUOTATION_SEA_PK = " + QuotPK);
            try
            {
                return Convert.ToDateTime(objWF.ExecuteScaler(sb.ToString()));
            }
            catch (OracleException sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw;
            }
        }
        public int GetQuotAmendSt(string QuotPK)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append("   SELECT Q.AMEND_FLAG ");
            sb.Append("   FROM QUOTATION_SEA_TBL Q");
            sb.Append("   WHERE Q.QUOTATION_SEA_PK = " + QuotPK);
            try
            {
                return Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
            }
            catch (OracleException sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw;
            }
        }
    }
}