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
using System.Collections;
using System.Data;
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    public class Cls_QuotationForBookingSea : CommonFeatures
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
                case "0":
                    return SourceType.OperatorTariff;
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
        string QuoteNo = "", int Version = 0, object QuotationStatus = null, DataTable OthDt = null, object ValidFor = null, object QuoteDate = null, Int16 CustomerType = 0, object ShipDate = null, int CreditLimit = 0, int CreditDays = 0,
        string remarks = "", string CargoMoveCode = "", object BaseCurrencyId = null, int INCOTerms = 0, int PYMTType = 0, Int16 Group = 0, bool AmendFlg = false, int From_Flag = 0)
        {
            string strSQL = null;
            string strSQLFreight = null;
            DataSet EnqDSNew = new DataSet();
            int i = 0;
            WorkFlow objWF = new WorkFlow();
            if (!string.IsNullOrEmpty(QuoteNo))
            {
                strSQL = GetQuoteQuery(QuoteNo, CargoType, Version, QuotationStatus, OthDt, ValidFor, QuoteDate, ShipDate, CreditDays, CreditLimit,
                remarks, CargoMoveCode, BaseCurrencyId, INCOTerms, PYMTType, Group);
                if (AmendFlg == true)
                {
                    EnqDS = objWF.GetDataSet(strSQL);
                    SectorContainers = "";
                    for (i = 0; i <= EnqDS.Tables[0].Rows.Count - 1; i++)
                    {
                        var _with1 = EnqDS.Tables[0].Rows[i];
                        if (Convert.ToInt32(CargoType) == 1 | cargo == 1)
                        {
                            SectorContainers += "(" + _with1["POL_PK"] + "," + _with1["POD_PK"] + "," + _with1["CNTR_PK"] + "),";
                        }
                        else
                        {
                            SectorContainers += "(" + _with1["POL_PK"] + "," + _with1["POD_PK"] + "," + _with1["LCL_BASIS"] + "),";
                        }
                    }
                    SectorContainers = Convert.ToString(SectorContainers).TrimEnd(',');
                }
                strSQLFreight = GetQuoteQueryFreights(QuoteNo, (cargo > 0 ? Convert.ToString(cargo) : CargoType), Group, AmendFlg, SectorContainers);
            }
            else
            {
                strSQL = GetEnquiryQuery(EnqNo, CargoType);
                strSQLFreight = GetEnquiryQueryFreights(EnqNo, CargoType);
            }
            DataSet ds = null;
            DataTable DT = null;
            DataTable DT1 = null;
            DataRow DR = null;
            DataRow DR1 = null;
            try
            {
                if (AmendFlg == false)
                {
                    EnqDS = objWF.GetDataSet(strSQL);
                }
                EnqDS.Tables.Add(objWF.GetDataTable(strSQLFreight));
                DataRelation REL = null;
                if (Convert.ToInt32(CargoType) == 1 | cargo == 1)
                {
                    if (AmendFlg == false)
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
                        SectorContainers = "";
                        REL = new DataRelation("EnqRelation", new DataColumn[] {
                        EnqDS.Tables[0].Columns["POL_PK"],
                        EnqDS.Tables[0].Columns["POD_PK"],
                        EnqDS.Tables[0].Columns["CNTR_PK"]
                    }, new DataColumn[] {
                        EnqDS.Tables[1].Columns["POL_PK"],
                        EnqDS.Tables[1].Columns["POD_PK"],
                        EnqDS.Tables[1].Columns["CNTR_PK"]
                    });
                    }
                }
                else
                {
                    if (AmendFlg == false)
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
                    else
                    {
                        SectorContainers = "";
                        REL = new DataRelation("EnqRelation", new DataColumn[] {
                        EnqDS.Tables[0].Columns["POL_PK"],
                        EnqDS.Tables[0].Columns["POD_PK"],
                        EnqDS.Tables[0].Columns["LCL_BASIS"]
                    }, new DataColumn[] {
                        EnqDS.Tables[1].Columns["POL_PK"],
                        EnqDS.Tables[1].Columns["POD_PK"],
                        EnqDS.Tables[1].Columns["LCLBASIS"]
                    });
                    }

                }
                EnqDS.Relations.Add(REL);

                Int16 RowCnt = default(Int16);
                Int16 ColCnt = default(Int16);

                if (string.IsNullOrEmpty(Convert.ToString(EnqNo).Trim()) & string.IsNullOrEmpty(Convert.ToString(QuoteNo).Trim()))
                {
                    if (Group == 1 | Group == 2)
                    {
                        if (Convert.ToInt32(CargoType) == 1)
                        {
                            strSQL = " SELECT POLPK, POLID, PODPK, PODID, CNTPK, CNTID FROM (SELECT  DISTINCT PGL.PORT_GRP_MST_PK POLPK,PGL.PORT_GRP_ID POLID, " +  " PGD.PORT_GRP_MST_PK PODPK, PGD.PORT_GRP_ID PODID, " +  " CNTR.CONTAINER_TYPE_MST_PK CNTPK, CNTR.CONTAINER_TYPE_MST_ID CNTID,CNTR.PREFERENCES " +  " FROM PORT_MST_TBL POL, PORT_MST_TBL POD, PORT_GRP_MST_TBL PGL, PORT_GRP_MST_TBL PGD,PORT_GRP_TRN_TBL PGTL, PORT_GRP_TRN_TBL PGTD, CONTAINER_TYPE_MST_TBL CNTR " +  "  WHERE PGL.PORT_GRP_MST_PK = PGTL.PORT_GRP_MST_FK " +  "  AND PGTL.PORT_MST_FK = POL.PORT_MST_PK " +  "  AND PGD.PORT_GRP_MST_PK = PGTD.PORT_GRP_MST_FK " +  "  AND PGTD.PORT_MST_FK = POD.PORT_MST_PK " +  "  AND (PGL.PORT_GRP_MST_PK, PGD.PORT_GRP_MST_PK, CNTR.CONTAINER_TYPE_MST_PK) " +  " IN (" + SectorContainers + ") ) Order By POLID,PODID,PREFERENCES ";

                        }
                        else
                        {
                            strSQL = " SELECT DISTINCT PGL.PORT_GRP_MST_PK POLPK,PGL.PORT_GRP_ID POLID, " +  " PGD.PORT_GRP_MST_PK PODPK, PGD.PORT_GRP_ID PODID, " +  " DIM.DIMENTION_UNIT_MST_PK DIMPK, DIM.DIMENTION_ID DIMID " +  " FROM PORT_MST_TBL POL, PORT_MST_TBL POD, PORT_GRP_MST_TBL PGL, PORT_GRP_MST_TBL PGD,PORT_GRP_TRN_TBL PGTL, PORT_GRP_TRN_TBL PGTD, DIMENTION_UNIT_MST_TBL DIM " +  "  WHERE PGL.PORT_GRP_MST_PK = PGTL.PORT_GRP_MST_FK " +  "  AND PGTL.PORT_MST_FK = POL.PORT_MST_PK " +  "  AND PGD.PORT_GRP_MST_PK = PGTD.PORT_GRP_MST_FK " +  "  AND PGTD.PORT_MST_FK = POD.PORT_MST_PK " +  "  AND (PGL.PORT_GRP_MST_PK, PGD.PORT_GRP_MST_PK, DIM.DIMENTION_UNIT_MST_PK) " +  " IN (" + SectorContainers + ") Order By POLID,PODID,DIMID ";

                        }
                    }
                    else
                    {
                        if (Convert.ToInt32(CargoType) == 1)
                        {
                            strSQL = " SELECT POL.PORT_MST_PK POLPK, POL.PORT_ID POLID, " +  " POD.PORT_MST_PK PODPK, POD.PORT_ID PODID, " +  " CNTR.CONTAINER_TYPE_MST_PK CNTPK, CNTR.CONTAINER_TYPE_MST_ID CNTID " +  " FROM PORT_MST_TBL POL, PORT_MST_TBL POD, CONTAINER_TYPE_MST_TBL CNTR " +  " WHERE (POL.PORT_MST_PK, POD.PORT_MST_PK, CNTR.CONTAINER_TYPE_MST_PK) " +  " IN (" + SectorContainers + ") Order By POLID,PODID,CNTR.PREFERENCES ";
                        }
                        else
                        {
                            strSQL = " SELECT POL.PORT_MST_PK POLPK, POL.PORT_ID POLID, " +  " POD.PORT_MST_PK PODPK, POD.PORT_ID PODID, " +  " DIM.DIMENTION_UNIT_MST_PK DIMPK, DIM.DIMENTION_ID DIMID " +  " FROM PORT_MST_TBL POL, PORT_MST_TBL POD, DIMENTION_UNIT_MST_TBL DIM " +  " WHERE (POL.PORT_MST_PK, POD.PORT_MST_PK, DIM.DIMENTION_UNIT_MST_PK ) " +  " IN (" + SectorContainers + ") Order By POLID,PODID,DIMID ";
                        }
                    }

                    DT = objWF.GetDataTable(strSQL);
                    for (RowCnt = 0; RowCnt <= DT.Rows.Count - 1; RowCnt++)
                    {
                        var _with2 = DT.Rows[RowCnt];
                        DR = EnqDS.Tables[0].NewRow();
                        for (ColCnt = 0; ColCnt <= EnqDS.Tables[0].Columns.Count - 1; ColCnt++)
                        {
                            DR[ColCnt] = DBNull.Value;
                        }
                        if (Convert.ToInt32(CargoType) == 1)
                        {
                            // DR["PK") = 0 : DR["TYPE") = "" : DR["REF_NO") = "0" : DR["SHIP_DATE") = ""
                            DR["POL_PK"] = _with2["POLPK"];
                            DR["POL_ID"] = _with2["POLID"];
                            DR["POD_PK"] = _with2["PODPK"];
                            DR["POD_ID"] = _with2["PODID"];
                            // DR["OPER_PK") = "" : DR["OPER_ID") = "" : DR["OPER_NAME") = "" 
                            DR["CNTR_PK"] = _with2["CNTPK"];
                            DR["CNTR_ID"] = _with2["CNTID"];
                            // DR["QUANTITY") = "" : DR["COMM_PK") = "" : DR["COMM_ID") = ""
                            // DR["ALL_IN_TARIFF") = "" : DR["ALL_IN_QUOTE") = "" : DR["TARIFF") = "" : DR["NET") = ""
                            DR["CUSTOMER_PK"] = CustomerPK;
                            DR["CUSTOMER_CATPK"] = CustomerCategory;
                            DR["COMM_GRPPK"] = CommodityGroup;
                        }
                        else
                        {
                            DR["POL_PK"] = _with2["POLPK"];
                            DR["POL_ID"] = _with2["POLID"];
                            DR["POD_PK"] = _with2["PODPK"];
                            DR["POD_ID"] = _with2["PODID"];
                            DR["LCL_BASIS"] = _with2["DIMPK"];
                            DR["DIMENTION_ID"] = _with2["DIMID"];
                            DR["CUSTOMER_PK"] = CustomerPK;
                            DR["CUSTOMER_CATPK"] = CustomerCategory;
                            DR["COMM_GRPPK"] = CommodityGroup;
                        }
                        EnqDS.Tables[0].Rows.Add(DR);
                    }
                }
                else
                {
                    if (!string.IsNullOrEmpty(QuoteNo) & !string.IsNullOrEmpty(SectorContainers))
                    {
                        EnqDSNew = EnqDS.Clone();
                        if (Group == 1 | Group == 2)
                        {
                            if (Convert.ToInt32(CargoType) == 1)
                            {
                                strSQL = " SELECT  PGL.PORT_GRP_MST_PK POLPK,PGL.PORT_GRP_ID POLID, " +  " PGD.PORT_GRP_MST_PK PODPK, PGD.PORT_GRP_ID PODID, " +  " CNTR.CONTAINER_TYPE_MST_PK CNTPK, CNTR.CONTAINER_TYPE_MST_ID CNTID" +  " FROM PORT_MST_TBL POL, PORT_MST_TBL POD, PORT_GRP_MST_TBL PGL, PORT_GRP_MST_TBL PGD,PORT_GRP_TRN_TBL PGTL, PORT_GRP_TRN_TBL PGTD, CONTAINER_TYPE_MST_TBL CNTR " +  "  WHERE PGL.PORT_GRP_MST_PK = PGTL.PORT_GRP_MST_FK " +  "  AND PGTL.PORT_MST_FK = POL.PORT_MST_PK " +  "  AND PGD.PORT_GRP_MST_PK = PGTD.PORT_GRP_MST_FK " +  "  AND PGTD.PORT_MST_FK = POD.PORT_MST_PK " +  "  AND (PGL.PORT_GRP_MST_PK, PGD.PORT_GRP_MST_PK, CNTR.CONTAINER_TYPE_MST_PK) " +  " IN (" + SectorContainers + ") Order By POLID,PODID,CNTR.PREFERENCES ";
                            }
                            else
                            {
                                strSQL = " SELECT POL.PORT_MST_PK POLPK, POL.PORT_ID POLID, " +  " POD.PORT_MST_PK PODPK, POD.PORT_ID PODID, " +  " DIM.DIMENTION_UNIT_MST_PK DIMPK, DIM.DIMENTION_ID DIMID " +  " FROM PORT_MST_TBL POL, PORT_MST_TBL POD, DIMENTION_UNIT_MST_TBL DIM " +  " WHERE (POL.PORT_MST_PK, POD.PORT_MST_PK, DIM.DIMENTION_UNIT_MST_PK ) " +  " IN (" + SectorContainers + ") Order By POLID,PODID,DIMID ";
                            }
                        }
                        else
                        {
                            if (Convert.ToInt32(CargoType) == 1)
                            {
                                strSQL = " SELECT POL.PORT_MST_PK POLPK, POL.PORT_ID POLID, " +  " POD.PORT_MST_PK PODPK, POD.PORT_ID PODID, " +  " CNTR.CONTAINER_TYPE_MST_PK CNTPK, CNTR.CONTAINER_TYPE_MST_ID CNTID " +  " FROM PORT_MST_TBL POL, PORT_MST_TBL POD, CONTAINER_TYPE_MST_TBL CNTR " +  " WHERE (POL.PORT_MST_PK, POD.PORT_MST_PK, CNTR.CONTAINER_TYPE_MST_PK) " +  " IN (" + SectorContainers + ") Order By POLID,PODID,CNTR.PREFERENCES ";
                            }
                            else
                            {
                                strSQL = " SELECT POL.PORT_MST_PK POLPK, POL.PORT_ID POLID, " +  " POD.PORT_MST_PK PODPK, POD.PORT_ID PODID, " +  " DIM.DIMENTION_UNIT_MST_PK DIMPK, DIM.DIMENTION_ID DIMID " +  " FROM PORT_MST_TBL POL, PORT_MST_TBL POD, DIMENTION_UNIT_MST_TBL DIM " +  " WHERE (POL.PORT_MST_PK, POD.PORT_MST_PK, DIM.DIMENTION_UNIT_MST_PK ) " +  " IN (" + SectorContainers + ") Order By POLID,PODID,DIMID ";
                            }
                        }


                        DT1 = objWF.GetDataTable(strSQL);
                        for (RowCnt = 0; RowCnt <= DT1.Rows.Count - 1; RowCnt++)
                        {
                            var _with3 = DT1.Rows[RowCnt];
                            DR1 = EnqDSNew.Tables[0].NewRow();
                            for (ColCnt = 0; ColCnt <= EnqDSNew.Tables[0].Columns.Count - 1; ColCnt++)
                            {
                                DR1[ColCnt] = DBNull.Value;
                            }
                            if (Convert.ToInt32(CargoType) == 1)
                            {
                                DR1["POL_PK"] = _with3["POLPK"];
                                DR1["POL_ID"] = _with3["POLID"];
                                DR1["POD_PK"] = _with3["PODPK"];
                                DR1["POD_ID"] = _with3["PODID"];
                                DR1["CNTR_PK"] = _with3["CNTPK"];
                                DR1["CNTR_ID"] = _with3["CNTID"];
                                DR1["CUSTOMER_PK"] = CustomerPK;
                                DR1["CUSTOMER_CATPK"] = CustomerCategory;
                                DR1["COMM_GRPPK"] = CommodityGroup;
                            }
                            else
                            {
                                DR1["POL_PK"] = _with3["POLPK"];
                                DR1["POL_ID"] = _with3["POLID"];
                                DR1["POD_PK"] = _with3["PODPK"];
                                DR1["POD_ID"] = _with3["PODID"];
                                DR1["LCL_BASIS"] = _with3["DIMPK"];
                                DR1["DIMENTION_ID"] = _with3["DIMID"];
                                DR1["CUSTOMER_PK"] = CustomerPK;
                                DR1["CUSTOMER_CATPK"] = CustomerCategory;
                                DR1["COMM_GRPPK"] = CommodityGroup;
                            }
                            EnqDSNew.Tables[0].Rows.Add(DR1);
                        }
                        AddFreights(EnqDSNew, CargoType);
                        EnqDS.Merge(EnqDSNew);
                    }
                    SectorContainers = "";
                    for (RowCnt = 0; RowCnt <= EnqDS.Tables[0].Rows.Count - 1; RowCnt++)
                    {
                        var _with4 = EnqDS.Tables[0].Rows[RowCnt];
                        if (Convert.ToInt32(CargoType) == 1 | cargo == 1)
                        {
                            SectorContainers += "(" + _with4["POL_PK"] + "," + _with4["POD_PK"] + "," + _with4["CNTR_PK"] + "),";
                        }
                        else
                        {
                            SectorContainers += "(" + _with4["POL_PK"] + "," + _with4["POD_PK"] + "," + _with4["LCL_BASIS"] + "),";
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
                            CustomerID = objWF.ExecuteScaler(" Select CUSTOMER_NAME from CUSTOMER_MST_TBL where CUSTOMER_MST_PK = " + CustomerPK);
                        }
                        else
                        {
                            CustomerID = objWF.ExecuteScaler(" Select CUSTOMER_NAME from TEMP_CUSTOMER_TBL where CUSTOMER_MST_PK = " + CustomerPK);
                        }
                    }
                }
                if (string.IsNullOrEmpty(Convert.ToString(QuoteNo).Trim()))
                {
                    if (string.IsNullOrEmpty(Convert.ToString(EnqNo).Trim()))
                    {
                        AddFreights(EnqDS, CargoType, From_Flag);
                    }
                }
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception eX)
            {
                throw eX;
            }
        }

        #endregion

        #region " Add Freights "
        // 1 DataBase Call
        private void AddFreights(DataSet DS, object CargoType, int From_Flag = 0)
        {
            DataTable frtDt = null;
            //Added by rabbani on 24/3/07 ,To display BOF as first element in Freight Element Column using UNION ALL.
            //modified by thiyagarajan on 20/11/08 for location based currency task
            //Snigdharani - 30/12/2008 - & " AND FRM.BY_DEFAULT = 1 " & vbCrLf _ ,& " AND FRM.CHARGE_BASIS <> 2 " & vbCrLf _Commented
            string strSQL = " Select " +  " FRM.FREIGHT_ELEMENT_MST_PK, FRM.FREIGHT_ELEMENT_ID, FRM.FREIGHT_ELEMENT_NAME, " +  " DECODE(FRM.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS , " +  " CURR.CURRENCY_MST_PK, CURR.CURRENCY_ID,FRM.Credit " +  " from FREIGHT_ELEMENT_MST_TBL FRM, CURRENCY_TYPE_MST_TBL CURR" +  " where FRM.ACTIVE_FLAG = 1 " +  " AND CURR.CURRENCY_MST_PK = " + HttpContext.Current.Session["CURRENCY_MST_PK"] +  " AND FRM.BUSINESS_TYPE in (2,3) " +  " AND FRM.CHARGE_TYPE <> 3 " +  " ORDER BY FRM.PREFERENCE";
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
                UC = new UniqueConstraint("UkFreights", new DataColumn[] {
                DS.Tables[1].Columns["REF_NO"],
                DS.Tables[1].Columns["POL_PK"],
                DS.Tables[1].Columns["POD_PK"],
                DS.Tables[1].Columns["LCLBASIS"],
                DS.Tables[1].Columns["FRT_PK"]
            });
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
                            FR["CHARGE_BASIS"] = frtDt.Rows[RowCnt]["CHARGE_BASIS"];
                            FR["SELECTED"] = 0;
                            FR["CURR_PK"] = frtDt.Rows[RowCnt]["CURRENCY_MST_PK"];
                            FR["CURR_ID"] = frtDt.Rows[RowCnt]["CURRENCY_ID"];
                            FR["QUOTERATE"] = 0;
                            FR["FINAL_RATE"] = 0;
                            //'Export
                            if (From_Flag == 0)
                            {
                                FR["PYTPE"] = 1;
                                FR["PYTYPE"] = "PrePaid";
                                //'Import
                            }
                            else
                            {
                                FR["PYTPE"] = 2;
                                FR["PYTYPE"] = "Collect";
                            }
                            FR["CREDIT"] = frtDt.Rows[RowCnt]["CREDIT"];
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
                            FR["CHARGE_BASIS"] = frtDt.Rows[RowCnt]["CHARGE_BASIS"];
                            FR["SELECTED"] = 0;
                            FR["CURR_PK"] = frtDt.Rows[RowCnt]["CURRENCY_MST_PK"];
                            FR["CURR_ID"] = frtDt.Rows[RowCnt]["CURRENCY_ID"];
                            FR["QUOTE_MIN_RATE"] = 0;
                            FR["QUOTERATE"] = 0;
                            FR["FINAL_RATE"] = 0;
                            //'Export
                            if (From_Flag == 0)
                            {
                                FR["PYTPE"] = 1;
                                FR["PYTYPE"] = "PrePaid";
                                //'Import
                            }
                            else
                            {
                                FR["PYTPE"] = 2;
                                FR["PYTYPE"] = "Collect";
                            }
                            FR["CREDIT"] = frtDt.Rows[RowCnt]["CREDIT"];
                        }
                        DS.Tables[1].Rows.Add(FR);
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
                    EnquiryNo = Convert.ToString(0);
                }
                // PK,TYPE,REF_NO,REFNO,SHIP_DATE,POL_PK,POL_ID,POD_PK,POD_ID,OPER_PK,OPER_ID,OPER_NAME,
                // CNTR_PK,CNTR_ID,QUANTITY,COMM_PK,COMM_ID,ALL_IN_TARIFF,ALL_IN_QUOTE,TARIFF,NET,SELECTED,
                // CUSTOMER_PK,CUSTOMER_CATPK,COMM_GRPPK,TRAN_REF_NO2
                // Getting Query for Enquiry Detail
                if (CargoType == "1")
                {
                    strSQL = "  SELECT Q.PK," +  "     Q.TYPE, " +  "     Q.REF_NO, " +  "     Q.REFNO, " +  "     Q.SHIP_DATE, " +  "     Q.POL_PK, " +  "     Q.POL_ID, " +  "     Q.POD_PK, " +  "     Q.POD_ID, " +  "     Q.OPER_PK, " +  "     Q.OPER_ID, " +  "     Q.OPER_NAME, " +  "     Q.CNTR_PK, " +  "     Q.CNTR_ID, " +  "     Q.QUANTITY, " +  "     Q.COMM_PK, " +  "     Q.COMM_ID, " +  "     Q.ALL_IN_TARIFF, " +  "     Q.ALL_IN_QUOTE, " +  "     Q.OPERATOR_RATE, " +  "     Q.NET, " +  "     Q.CUSTOMER_PK, " +  "     Q.CUSTOMER_CATPK,  " +  "     Q.COMM_GRPPK,Q.REF_NO2,Q.TYPE2,Q.OTH_BTN,Q.OTH_DTL,  " +  "     Q.FK,Q.CUST_TYPE," +  "     Q.COPY,Q.COMMODITY_MST_FKS,Q.POL_GRP_FK, Q.POD_GRP_FK, Q.TARIFF_GRP_FK,Q.AIF " +  "   FROM (Select   DISTINCT           " +  "     main4.ENQUIRY_BKG_SEA_PK                   PK,                  " +  "  " + SRC(SourceType.Enquiry) + "               TYPE,                " +  "     main4.ENQUIRY_REF_NO                       REF_NO,              " +  "     main4.ENQUIRY_REF_NO                       REFNO,               " +  "     TO_CHAR(tran4.EXPECTED_SHIPMENT,'" + dateFormat + "') SHIP_DATE,   " +  "     tran4.PORT_MST_POL_FK                      POL_PK,              " +  "     portpol4.PORT_ID                           POL_ID,              " +  "     tran4.PORT_MST_POD_FK                      POD_PK,              " +  "     portpod4.PORT_ID                           POD_ID,              " +  "     tran4.OPERATOR_MST_FK                      OPER_PK,             " +  "     opr4.OPERATOR_ID                           OPER_ID,             " +  "     opr4.OPERATOR_NAME                         OPER_NAME,           " +  "     tran4.CONTAINER_TYPE_MST_FK                CNTR_PK,             " +  "     cntr4.CONTAINER_TYPE_MST_ID                CNTR_ID,             " +  "     tran4.EXPECTED_BOXES                       QUANTITY,            " +  "     tran4.COMMODITY_MST_FK                     COMM_PK,             " +  "     cmdt4.COMMODITY_NAME                       COMM_ID,             " +  "     tran4.ALL_IN_TARIFF                        ALL_IN_TARIFF,       " +  "     tran4.ALL_IN_TARIFF                        ALL_IN_QUOTE,        " +  "     0                                          OPERATOR_RATE,       " +  "     NULL                                       NET,                 " +  "     main4.CUSTOMER_MST_FK                      CUSTOMER_PK,         " +  "     main4.CUSTOMER_CATEGORY_FK                 CUSTOMER_CATPK,      " +  "     tran4.COMMODITY_GROUP_FK                   COMM_GRPPK,          " +  "     NULL                                       REF_NO2,             " +  "     NULL                                       TYPE2,               " +  "     ''                                         OTH_BTN,             " +  "     ''                                         OTH_DTL,             " +  "     tran4.ENQUIRY_TRN_SEA_PK                   FK,                   " +  "     nvl( CUST_TYPE,1)                          CUST_TYPE , '' COPY, '' COMMODITY_MST_FKS,    " +  "     '' POL_GRP_FK, '' POD_GRP_FK, '' TARIFF_GRP_FK,cntr4.PREFERENCES,0 AIF         " +  "    from                                                             " +  "     ENQUIRY_BKG_SEA_TBL            main4,                           " +  "     ENQUIRY_TRN_SEA_FCL_LCL        tran4,                           " +  "     PORT_MST_TBL                   portpol4,                        " +  "     PORT_MST_TBL                   portpod4,                        " +  "     OPERATOR_MST_TBL               opr4,                            " +  "     COMMODITY_MST_TBL              cmdt4,                           " +  "     CONTAINER_TYPE_MST_TBL         cntr4                            " +  "    where                                                                " +  "            main4.ENQUIRY_BKG_SEA_PK    = tran4.ENQUIRY_MAIN_SEA_FK            " +  "     AND    main4.CARGO_TYPE            = 1                                    " +  "     AND    tran4.PORT_MST_POL_FK       = portpol4.PORT_MST_PK(+)              " +  "     AND    tran4.PORT_MST_POD_FK       = portpod4.PORT_MST_PK(+)              " +  "     AND    tran4.OPERATOR_MST_FK       = opr4.OPERATOR_MST_PK(+)              " +  "     AND    tran4.COMMODITY_MST_FK      = cmdt4.COMMODITY_MST_PK(+)            " +  "     AND    tran4.CONTAINER_TYPE_MST_FK = cntr4.CONTAINER_TYPE_MST_PK(+)       " +  "     AND    main4.ENQUIRY_REF_NO        = '" + EnquiryNo + "'" +  "      )Q" +  "   ORDER BY Q.PREFERENCES";
                }
                else
                {
                    // PK, TYPE, REF_NO, SHIP_DATE, POL_PK, POL_ID, POD_PK, POD_ID, OPER_PK, OPER_ID, OPER_NAME
                    // COMM_PK, COMM_ID, LCL_BASIS, DIMENTION_ID, WEIGHT, VOLUME, ALL_IN_TARIFF, ALL_IN_QUOTE 
                    // TARIFF, NET, REF_NO2, TYPE2, OTH_BTN(26), OTH_DTL(27), FK(28), CUST_TYPE(29)

                    strSQL = "    Select        DISTINCT                                  " +  "     main4.ENQUIRY_BKG_SEA_PK                   PK,                  " +  "  " + SRC(SourceType.Enquiry) + "               TYPE,                " +  "     main4.ENQUIRY_REF_NO                       REF_NO,              " +  "     TO_CHAR(tran4.EXPECTED_SHIPMENT,'" + dateFormat + "') SHIP_DATE,        " +  "     tran4.PORT_MST_POL_FK                      POL_PK,              " +  "     portpol4.PORT_ID                           POL_ID,              " +  "     tran4.PORT_MST_POD_FK                      POD_PK,              " +  "     portpod4.PORT_ID                           POD_ID,              " +  "     tran4.OPERATOR_MST_FK                      OPER_PK,             " +  "     opr4.OPERATOR_ID                           OPER_ID,             " +  "     opr4.OPERATOR_NAME                         OPER_NAME,           " +  "     tran4.COMMODITY_MST_FK                     COMM_PK,             " +  "     cmdt4.COMMODITY_ID                         COMM_ID,             " +  "     tran4.BASIS                                LCL_BASIS,           " +  "     NVL(dim4.DIMENTION_ID,'')                  DIMENTION_ID,        " +  "     tran4.EXPECTED_WEIGHT                      WEIGHT,              " +  "     tran4.EXPECTED_VOLUME                      VOLUME,              " +  "     tran4.ALL_IN_TARIFF                        ALL_IN_TARIFF,       " +  "     tran4.ALL_IN_TARIFF                        ALL_IN_QUOTE,        " +  "     0                                          OPERATOR_RATE,       " +  "     NULL                                       NET,                 " +  "     NULL                                       REF_NO2,             " +  "     NULL                                       TYPE2,               " +  "     main4.CUSTOMER_MST_FK                      CUSTOMER_PK,         " +  "     main4.CUSTOMER_CATEGORY_FK                 CUSTOMER_CATPK,      " +  "     tran4.COMMODITY_GROUP_FK                   COMM_GRPPK,          " +  "     ''                                         OTH_BTN,             " +  "     ''                                         OTH_DTL,             " +  "     tran4.ENQUIRY_TRN_SEA_PK                   FK,                  " +  "     nvl( CUST_TYPE,1)     CUST_TYPE,'' COPY,'' COMMODITY_MST_FKS,   " +  "     '' POL_GRP_FK, '' POD_GRP_FK, '' TARIFF_GRP_FK,0 AIF         " +  "    from                                                             " +  "     ENQUIRY_BKG_SEA_TBL            main4,                           " +  "     ENQUIRY_TRN_SEA_FCL_LCL        tran4,                           " +  "     PORT_MST_TBL                   portpol4,                        " +  "     PORT_MST_TBL                   portpod4,                        " +  "     OPERATOR_MST_TBL               opr4,                            " +  "     COMMODITY_MST_TBL              cmdt4,                           " +  "     DIMENTION_UNIT_MST_TBL         dim4                             " +  "    where                                                                " +  "            main4.ENQUIRY_BKG_SEA_PK    = tran4.ENQUIRY_MAIN_SEA_FK            " +  "     AND    main4.CARGO_TYPE            = 2                                    " +  "     AND    tran4.PORT_MST_POL_FK       = portpol4.PORT_MST_PK(+)              " +  "     AND    tran4.PORT_MST_POD_FK       = portpod4.PORT_MST_PK(+)              " +  "     AND    tran4.OPERATOR_MST_FK       = opr4.OPERATOR_MST_PK(+)              " +  "     AND    tran4.COMMODITY_MST_FK      = cmdt4.COMMODITY_MST_PK(+)          " +  "     AND    tran4.BASIS                 = dim4.DIMENTION_UNIT_MST_PK(+)        " +  "     AND    main4.ENQUIRY_REF_NO        = '" + EnquiryNo + "'";
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

        private string GetEnquiryQueryFreights(string EnquiryNo, string CargoType = "1")
        {
            try
            {
                if (string.IsNullOrEmpty(Convert.ToString(EnquiryNo).Trim()))
                    EnquiryNo = Convert.ToString(0);
                string strSQL = null;
                // REF_NO,POL_PK,POD_PK,CNTR_PK,FRT_PK,FRT_ID,FRT_NAME,SELECTED,CURR_PK,CURR_ID,RATE,QUOTERATE,PYTYPE
                if (CargoType == "1")
                {
                    strSQL = "    Select            " + 
                         "     main4.ENQUIRY_REF_NO                       REF_NO,              " + 
                         "     tran4.PORT_MST_POL_FK                      POL_PK,              " + 
                         "     tran4.PORT_MST_POD_FK                      POD_PK,              " +
                         "     tran4.CONTAINER_TYPE_MST_FK                CNTR_PK,             " +
                         "     frtd4.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " + 
                         "     frt4.FREIGHT_ELEMENT_ID                    FRT_ID,              " + 
                         "     frt4.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " + 
                         "     DECODE(frt4.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS,  " + 
                         "     DECODE(frtd4.CHECK_FOR_ALL_IN_RT, 1,'true','false') SELECTED,   " + 
                         "     DECODE(frtd4.CHECK_ADVATOS, 1,'true','false') ADVATOS,          " + 
                         "     frtd4.CURRENCY_MST_FK                      CURR_PK,             " + 
                         "     curr4.CURRENCY_ID                          CURR_ID,             " + 
                         "     frtd4.TARIFF_RATE                          QUOTERATE,           " + 
                         "     frtd4.TARIFF_RATE                          FINAL_RATE,                " + 
                         "     1                                       PYTPE,               " + 
                         "     'PrePaid'                                       PYTYPE,frt4.CREDIT,'' AIFType               " +
                         "    from                                                             " + 
                         "     ENQUIRY_BKG_SEA_TBL            main4,                           " + 
                         "     ENQUIRY_TRN_SEA_FCL_LCL        tran4,                           " +
                         "     ENQUIRY_TRN_SEA_FRT_DTLS       frtd4,                           " + 
                         "     FREIGHT_ELEMENT_MST_TBL        frt4,                            " + 
                         "     CURRENCY_TYPE_MST_TBL          curr4                            " + 
                         "    where                                                               " + 
                         "            main4.ENQUIRY_BKG_SEA_PK    = tran4.ENQUIRY_MAIN_SEA_FK        " + 
                         "     AND    tran4.ENQUIRY_TRN_SEA_PK    = frtd4.ENQUIRY_TRN_SEA_FK         " + 
                         "     AND    frtd4.FREIGHT_ELEMENT_MST_FK = frt4.FREIGHT_ELEMENT_MST_PK(+)  " + 
                         "     AND    frtd4.CURRENCY_MST_FK       = curr4.CURRENCY_MST_PK(+)         " + 
                         "     AND    main4.CARGO_TYPE            = 1                                " + 
                         "     AND    main4.ENQUIRY_REF_NO        = '" + EnquiryNo + "'              " + 
                         "          order by frt4.preference";
                    //Added "preference order " By Prakash Chandra on 29/4/08 

                    //Added by rabbani reason USS Gap,introduced new column as "QUOTE_MIN_RATE"
                }
                else
                {
                    strSQL = "    Select            " +  "     main4.ENQUIRY_REF_NO                       REF_NO,              " +  "     tran4.PORT_MST_POL_FK                      POL_PK,              " +  "     tran4.PORT_MST_POD_FK                      POD_PK,              " +  "     tran4.BASIS                                LCLBASIS,            " +  "     frtd4.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt4.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt4.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frt4.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS,  " +  "     DECODE(frtd4.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     DECODE(frtd4.CHECK_ADVATOS, 1,'true','false') ADVATOS,          " +  "     frtd4.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr4.CURRENCY_ID                          CURR_ID,             " +  "     frtd4.TARIFF_MIN_RATE                      QUOTE_MIN_RATE,      " +  "     frtd4.TARIFF_RATE                          QUOTERATE,                " +  "     CASE WHEN frtd4.TARIFF_MIN_RATE > frtd4.TARIFF_RATE THEN frtd4.TARIFF_MIN_RATE ELSE frtd4.TARIFF_RATE END                          FINAL_RATE,           " +  "     1                                       PYTPE,               " +  "     'PrePaid'                               PYTYPE ,frt4.CREDIT,'' AIFType              " +  "    from                                                             " +  "     ENQUIRY_BKG_SEA_TBL            main4,                           " +  "     ENQUIRY_TRN_SEA_FCL_LCL        tran4,                           " +  "     ENQUIRY_TRN_SEA_FRT_DTLS       frtd4,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt4,                            " +  "     CURRENCY_TYPE_MST_TBL          curr4                            " +  "    where                                                                " +  "            main4.ENQUIRY_BKG_SEA_PK    = tran4.ENQUIRY_MAIN_SEA_FK           " +  "     AND    tran4.ENQUIRY_TRN_SEA_PK    = frtd4.ENQUIRY_TRN_SEA_FK            " +  "     AND    frtd4.FREIGHT_ELEMENT_MST_FK = frt4.FREIGHT_ELEMENT_MST_PK(+)     " +  "     --AND    frt4.CHARGE_BASIS           <> 2                                  " +  "     AND    frtd4.CURRENCY_MST_FK       = curr4.CURRENCY_MST_PK(+)            " +  "     AND    main4.CARGO_TYPE            = 2                                   " +  "     AND    main4.ENQUIRY_REF_NO        = '" + EnquiryNo + "'                 " +  "          order by frt4.preference";
                    //Added "preference order " By Prakash Chandra on 29/4/08 

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

        private string GetQuoteQuery(string QuotationPK, string CargoType = "1", long Version = 0, object QuotationStatus = null, DataTable OthDt = null, object ValidFor = null, object QuoteDate = null, object ShipDate = null, int CREDIT_DAYS = 0, int CREDIT_Limit = 0,
        string remarks = "", string CargoMoveCode = "", object BaseCurrencyId = null, int INCOTerms = 0, int PYMTType = 0, int Group = 0)
        {
            try
            {
                string strSQL = null;
                string cargos = null;
                DataTable scalerDT = null;
                if (!string.IsNullOrEmpty(Convert.ToString(QuotationPK).Trim()))
                {
                    strSQL = " Select nvl(CARGO_TYPE,1), nvl(QUOTATION_MST_TBL.VERSION_NO, 0) VERSION_NO, nvl(STATUS,1), nvl(VALID_FOR,1), " + "        to_char(QUOTATION_DATE,'" + dateFormat + "'), " + "        to_char(EXPECTED_SHIPMENT_DT,'" + dateFormat + "'), " + "        CREDIT_DAYS, " + "        CREDIT_LIMIT," + "        remarks," + "        cargo_move_fk," + "        CURR.CURRENCY_MST_PK BASE_CURRENCY_FK," + "        CURR.CURRENCY_ID, shipping_terms_mst_pk, pymt_type " + "  from  QUOTATION_MST_TBL,  CURRENCY_TYPE_MST_TBL CURR " + " where CURR.CURRENCY_MST_PK(+) = QUOTATION_MST_TBL.BASE_CURRENCY_FK" + " AND QUOTATION_MST_PK = " + QuotationPK;
                    scalerDT = (new WorkFlow()).GetDataTable(strSQL);
                    if (scalerDT.Rows.Count > 0)
                    {
                        CargoType = Convert.ToString(removeDBNull(scalerDT.Rows[0][0]));
                        Version = Convert.ToInt64(removeDBNull(scalerDT.Rows[0][1]));
                        QuotationStatus = removeDBNull(scalerDT.Rows[0][2]);
                        ValidFor = removeDBNull(scalerDT.Rows[0][3]);
                        QuoteDate = removeDBNull(scalerDT.Rows[0][4]);
                        ShipDate = removeDBNull(scalerDT.Rows[0][5]);
                        CREDIT_DAYS = Convert.ToInt32(removeDBNull(scalerDT.Rows[0][6]));
                        CREDIT_Limit = Convert.ToInt32(removeDBNull(scalerDT.Rows[0][7]));
                        remarks = Convert.ToString(removeDBNull(scalerDT.Rows[0][8]));
                        CargoMoveCode = Convert.ToString(removeDBNull(scalerDT.Rows[0][9]));
                        BaseCurrencyId = removeDBNull(scalerDT.Rows[0]["CURRENCY_ID"]);
                        INCOTerms = Convert.ToInt32(removeDBNull(scalerDT.Rows[0]["shipping_terms_mst_pk"]));
                        PYMTType = Convert.ToInt32(removeDBNull(scalerDT.Rows[0]["pymt_type"]));
                    }
                    strSQL = " Select FREIGHT_ELEMENT_MST_FK, CURRENCY_MST_FK, AMOUNT, QUOTATION_TRN_SEA_FK ,FREIGHT_TYPE PYMT_TYPE " + " from QUOTATION_TRN_SEA_OTH_CHRG where QUOTATION_TRN_SEA_FK IN " + "      ( Select QUOTE_TRN_SEA_PK from QUOTATION_DTL_TBL where " + "               QUOTATION_MST_FK = " + QuotationPK + " ) ";
                    OthDt = (new WorkFlow()).GetDataTable(strSQL);
                }
                else
                {
                    QuotationPK = "0";
                }

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
                if (Group == 1 | Group == 2)
                {
                    if (CargoType == "1" | cargo == 1)
                    {
                        strSQL = "  SELECT Q.PK," +  "     Q.TYPE, " +  "     Q.REF_NO, " +  "     Q.REFNO, " +  "     Q.SHIP_DATE, " +  "     Q.POL_PK, " +  "     Q.POL_ID, " +  "     Q.POD_PK, " +  "     Q.POD_ID, " +  "     Q.OPER_PK, " +  "     Q.OPER_ID, " +  "     Q.OPER_NAME, " +  "     Q.CNTR_PK, " +  "     Q.CNTR_ID, " +  "     Q.QUANTITY, " +  "     Q.COMM_PK, " +  "     Q.COMM_ID, " +  "     Q.ALL_IN_TARIFF, " +  "     Q.ALL_IN_QUOTE, " +  "     Q.OPERATOR_RATE, " +  "     Q.NET, " +  "     Q.CUSTOMER_PK, " +  "     Q.CUSTOMER_CATPK,  " +  "     Q.COMM_GRPPK,Q.REF_NO2,Q.TYPE2,Q.OTH_BTN,Q.OTH_DTL,  " +  "     Q.FK,Q.CUST_TYPE," +  "     Q.COPY,Q.COMMODITY_MST_FKS,Q.POL_GRP_FK, Q.POD_GRP_FK, Q.TARIFF_GRP_FK,Q.AIF" +  "   FROM(Select     DISTINCT                                    " +  "     main3.QUOTATION_MST_PK                     PK,                  " +  "     Decode(tran3.TRANS_REFERED_FROM," + SourceType.SpotRate + "," + SRC(SourceType.SpotRate) + "," + " " + SourceType.Quotation + "," + SRC(SourceType.Quotation) + "," + " " + SourceType.Enquiry + "," + SRC(SourceType.Enquiry) + "," + " " + SourceType.OperatorTariff + "," + SRC(SourceType.OperatorTariff) + "," + " " + SourceType.CustomerContract + "," + SRC(SourceType.CustomerContract) + "," + " " + SourceType.GeneralTariff + "," + SRC(SourceType.GeneralTariff) + "," + " " + SourceType.Manual + "," + SRC(SourceType.Manual) + ")          TYPE,                " +  "     tran3.TRANS_REF_NO                         REF_NO,              " +  "     tran3.TRANS_REF_NO                         REFNO,               " +  "     TO_CHAR(main3.EXPECTED_SHIPMENT_DT,'" + dateFormat + "')  SHIP_DATE,    " +  "     PGL.PORT_GRP_MST_PK POL_PK,                                     " +  "     PGL.PORT_GRP_ID POL_ID,                                       " +  "     PGD.PORT_GRP_MST_PK POD_PK,                                     " +  "     PGD.PORT_GRP_ID POD_ID,                                       " +  "     tran3.OPERATOR_MST_FK                      OPER_PK,             " +  "     opr3.OPERATOR_ID                           OPER_ID,             " +  "     opr3.OPERATOR_NAME                         OPER_NAME,           " +  "     tran3.CONTAINER_TYPE_MST_FK                CNTR_PK,             " +  "     cntr3.CONTAINER_TYPE_MST_ID                CNTR_ID,             " +  "     tran3.EXPECTED_BOXES                       QUANTITY,            " +  "     tran3.COMMODITY_MST_FK                     COMM_PK,             " +  "     cmdt3.COMMODITY_NAME                       COMM_ID,             " +  "     tran3.ALL_IN_TARIFF                        ALL_IN_TARIFF,       " +  "     tran3.ALL_IN_QUOTED_TARIFF                 ALL_IN_QUOTE,        " +  "     nvl(tran3.BUYING_RATE,0)                   OPERATOR_RATE,       " +  "     case                                                            " +  "     when (nvl(tran3.BUYING_RATE, 0) > 0) then                       " +  "     nvl(tran3.ALL_IN_QUOTED_TARIFF -                                " +  "     nvl(tran3.BUYING_RATE, 0),                                      " +  "     0)                                                              " +  "     Else                                                            " +  "     NULL                                                            " +  "     end NET,                                                        " +  "     main3.CUSTOMER_MST_FK                      CUSTOMER_PK,         " +  "     main3.CUSTOMER_CATEGORY_MST_FK             CUSTOMER_CATPK,      " +  "     tran3.COMMODITY_GROUP_FK                   COMM_GRPPK,          " +  "     tran3.TRAN_REF_NO2                         REF_NO2,             " +  "     tran3.REF_TYPE2                            TYPE2,               " +  "     ''                                         OTH_BTN,             " +  "     ''                                         OTH_DTL,             " +  "     0                     FK,                  " +  "     nvl(main3.CUST_TYPE,1) CUST_TYPE , '' COPY, tran3.COMMODITY_MST_FKS, " +  "     TRAN3.POL_GRP_FK, TRAN3.POD_GRP_FK, TRAN3.TARIFF_GRP_FK,cntr3.PREFERENCES,0 AIF         " +  "    from                                                             " +  "     QUOTATION_MST_TBL              main3,                           " +  "     QUOTATION_DTL_TBL      tran3,                           " +  "     PORT_MST_TBL                   portpol3,                        " +  "     PORT_MST_TBL                   portpod3,                        " +  "     PORT_GRP_MST_TBL        PGL,                                  " +  "     PORT_GRP_MST_TBL        PGD,                                  " +  "     OPERATOR_MST_TBL               opr3,                            " +  "     COMMODITY_MST_TBL              cmdt3,                           " +  "     CONTAINER_TYPE_MST_TBL         cntr3                            " +  "   where    main3.QUOTATION_MST_PK      = tran3.QUOTATION_MST_FK             " +  "     AND    tran3.PORT_MST_POL_FK       = portpol3.PORT_MST_PK(+)            " +  "     AND    tran3.PORT_MST_POD_FK       = portpod3.PORT_MST_PK(+)            " +  "     AND    PGL.PORT_GRP_MST_PK(+)      = TRAN3.POL_GRP_FK                   " +  "     AND    PGD.PORT_GRP_MST_PK(+)      = TRAN3.POD_GRP_FK                   " +  "     AND    tran3.OPERATOR_MST_FK       = opr3.OPERATOR_MST_PK(+)            " +  "     AND    tran3.CONTAINER_TYPE_MST_FK = cntr3.CONTAINER_TYPE_MST_PK(+)     " +  "     AND    tran3.COMMODITY_MST_FK      = cmdt3.COMMODITY_MST_PK(+)          " +  "     AND    main3.QUOTATION_MST_PK =  " + QuotationPK +  " " + cargos + " )Q" +  " ORDER BY Q.PREFERENCES";

                    }
                    else
                    {
                        strSQL = "    Select     DISTINCT                                    " +  "     main3.QUOTATION_MST_PK                     PK,                  " +  "     Decode(tran3.TRANS_REFERED_FROM," + SourceType.SpotRate + "," + SRC(SourceType.SpotRate) + "," + " " + SourceType.Quotation + "," + SRC(SourceType.Quotation) + "," + " " + SourceType.Enquiry + "," + SRC(SourceType.Enquiry) + "," + " " + SourceType.OperatorTariff + "," + SRC(SourceType.OperatorTariff) + "," + " " + SourceType.CustomerContract + "," + SRC(SourceType.CustomerContract) + "," + " " + SourceType.GeneralTariff + "," + SRC(SourceType.GeneralTariff) + "," + " " + SourceType.Manual + "," + SRC(SourceType.Manual) + ")          TYPE,                " +  "     tran3.TRANS_REF_NO                         REF_NO,              " +  "     TO_CHAR(main3.EXPECTED_SHIPMENT_DT,'" + dateFormat + "')  SHIP_DATE, " +  "     PGL.PORT_GRP_MST_PK POL_PK,                                     " +  "     PGL.PORT_GRP_ID POL_ID,                                         " +  "     PGD.PORT_GRP_MST_PK POD_PK,                                     " +  "     PGD.PORT_GRP_ID POD_ID,                                         " +  "     tran3.OPERATOR_MST_FK                      OPER_PK,             " +  "     opr3.OPERATOR_ID                           OPER_ID,             " +  "     opr3.OPERATOR_NAME                         OPER_NAME,           " +  "     tran3.COMMODITY_MST_FK                     COMM_PK,             " +  "     cmdt3.COMMODITY_NAME                       COMM_ID,             " +  "     tran3.BASIS                                LCL_BASIS,           " +  "     NVL(dim3.DIMENTION_ID,'')                  DIMENTION_ID,        " +  "     tran3.EXPECTED_WEIGHT                      WEIGHT,              " +  "     tran3.EXPECTED_VOLUME                      VOLUME,              " +  "     tran3.ALL_IN_TARIFF                        ALL_IN_TARIFF,       " +  "     tran3.ALL_IN_QUOTED_TARIFF                 ALL_IN_QUOTE,        " +  "     nvl(tran3.BUYING_RATE,0)                   OPERATOR_RATE,       " +  "     case                                                            " +  "     when (nvl(tran3.BUYING_RATE, 0) > 0) then                       " +  "     nvl(tran3.ALL_IN_QUOTED_TARIFF -                                " +  "     nvl(tran3.BUYING_RATE, 0),                                      " +  "     0)                                                              " +  "     Else                                                            " +  "     NULL                                                            " +  "     end NET,                                                        " +  "     tran3.TRAN_REF_NO2                         REF_NO2,             " +  "     tran3.REF_TYPE2                            TYPE2,               " +  "     main3.CUSTOMER_MST_FK                      CUSTOMER_PK,         " +  "     main3.CUSTOMER_CATEGORY_MST_FK             CUSTOMER_CATPK,      " +  "     tran3.COMMODITY_GROUP_FK                   COMM_GRPPK,          " +  "     ''                                         OTH_BTN,             " +  "     ''                                         OTH_DTL,             " +  "     0                     FK,                  " +  "     nvl(main3.CUST_TYPE,1)                     CUST_TYPE , '' COPY, tran3.COMMODITY_MST_FKS, " +  "     TRAN3.POL_GRP_FK, TRAN3.POD_GRP_FK, TRAN3.TARIFF_GRP_FK,0 AIF         " +  "    FROM                                                             " +  "     QUOTATION_MST_TBL              main3,                           " +  "     QUOTATION_DTL_TBL      tran3,                           " +  "     PORT_MST_TBL                   portpol3,                        " +  "     PORT_MST_TBL                   portpod3,                        " +  "     PORT_GRP_MST_TBL               PGL,                             " +  "     PORT_GRP_MST_TBL               PGD,                             " +  "     OPERATOR_MST_TBL               opr3,                            " +  "     COMMODITY_MST_TBL              cmdt3,                           " +  "     DIMENTION_UNIT_MST_TBL         dim3                             " +  "    where                                                                " +  "            main3.QUOTATION_MST_PK      = tran3.QUOTATION_MST_FK             " +  "     AND    tran3.PORT_MST_POL_FK       = portpol3.PORT_MST_PK(+)            " +  "     AND    tran3.PORT_MST_POD_FK       = portpod3.PORT_MST_PK(+)            " +  "     AND    PGL.PORT_GRP_MST_PK(+)      = TRAN3.POL_GRP_FK                   " +  "     AND    PGD.PORT_GRP_MST_PK(+)      = TRAN3.POD_GRP_FK                   " +  "     AND    tran3.OPERATOR_MST_FK       = opr3.OPERATOR_MST_PK(+)            " +  "     AND    tran3.BASIS                 = dim3.DIMENTION_UNIT_MST_PK(+)      " +  "     AND    tran3.COMMODITY_MST_FK      = cmdt3.COMMODITY_MST_PK(+)          " +  "     AND    main3.QUOTATION_MST_PK =  " + QuotationPK +  cargos;
                    }
                }
                else
                {
                    if (CargoType == "1" | cargo == 1)
                    {
                        strSQL = "  SELECT Q.PK," +  "     Q.TYPE, " +  "     Q.REF_NO, " +  "     Q.REFNO, " +  "     Q.SHIP_DATE, " +  "     Q.POL_PK, " +  "     Q.POL_ID, " +  "     Q.POD_PK, " +  "     Q.POD_ID, " +  "     Q.OPER_PK, " +  "     Q.OPER_ID, " +  "     Q.OPER_NAME, " +  "     Q.CNTR_PK, " +  "     Q.CNTR_ID, " +  "     Q.QUANTITY, " +  "     Q.COMM_PK, " +  "     Q.COMM_ID, " +  "     Q.ALL_IN_TARIFF, " +  "     Q.ALL_IN_QUOTE, " +  "     Q.OPERATOR_RATE, " +  "     Q.NET, " +  "     Q.CUSTOMER_PK, " +  "     Q.CUSTOMER_CATPK,  " +  "     Q.COMM_GRPPK,Q.REF_NO2,Q.TYPE2,Q.OTH_BTN,Q.OTH_DTL,  " +  "     Q.FK,Q.CUST_TYPE," +  "     Q.COPY,Q.COMMODITY_MST_FKS,Q.POL_GRP_FK, Q.POD_GRP_FK, Q.TARIFF_GRP_FK,Q.AIF " +  "    FROM(Select     DISTINCT                                    " +  "     main3.QUOTATION_MST_PK                     PK,                  " +  "     Decode(tran3.TRANS_REFERED_FROM," + SourceType.SpotRate + "," + SRC(SourceType.SpotRate) + "," + " " + SourceType.Quotation + "," + SRC(SourceType.Quotation) + "," + " " + SourceType.Enquiry + "," + SRC(SourceType.Enquiry) + "," + " " + SourceType.OperatorTariff + "," + SRC(SourceType.OperatorTariff) + "," + " " + SourceType.CustomerContract + "," + SRC(SourceType.CustomerContract) + "," + " " + SourceType.GeneralTariff + "," + SRC(SourceType.GeneralTariff) + "," + " " + SourceType.Manual + "," + SRC(SourceType.Manual) + ")          TYPE,                " +  "     tran3.TRANS_REF_NO                         REF_NO,              " +  "     tran3.TRANS_REF_NO                         REFNO,               " +  "     TO_CHAR(main3.EXPECTED_SHIPMENT_DT,'" + dateFormat + "')  SHIP_DATE,    " +  "     tran3.PORT_MST_POL_FK                      POL_PK,              " +  "     portpol3.PORT_ID                           POL_ID,              " +  "     tran3.PORT_MST_POD_FK                      POD_PK,              " +  "     portpod3.PORT_ID                           POD_ID,              " +  "     tran3.OPERATOR_MST_FK                      OPER_PK,             " +  "     opr3.OPERATOR_ID                           OPER_ID,             " +  "     opr3.OPERATOR_NAME                         OPER_NAME,           " +  "     tran3.CONTAINER_TYPE_MST_FK                CNTR_PK,             " +  "     cntr3.CONTAINER_TYPE_MST_ID                CNTR_ID,             " +  "     tran3.EXPECTED_BOXES                       QUANTITY,            " +  "     tran3.COMMODITY_MST_FK                     COMM_PK,             " +  "     cmdt3.COMMODITY_NAME                       COMM_ID,             " +  "     tran3.ALL_IN_TARIFF                        ALL_IN_TARIFF,       " +  "     tran3.ALL_IN_QUOTED_TARIFF                 ALL_IN_QUOTE,        " +  "     nvl(tran3.BUYING_RATE,0)                   OPERATOR_RATE,       " +  "     case                                                            " +  "     when (nvl(tran3.BUYING_RATE, 0) > 0) then                       " +  "     nvl(tran3.ALL_IN_QUOTED_TARIFF -                                " +  "     nvl(tran3.BUYING_RATE, 0),                                      " +  "     0)                                                              " +  "     Else                                                            " +  "     NULL                                                            " +  "     end NET,                                                        " +  "     main3.CUSTOMER_MST_FK                      CUSTOMER_PK,         " +  "     main3.CUSTOMER_CATEGORY_MST_FK             CUSTOMER_CATPK,      " +  "     tran3.COMMODITY_GROUP_FK                   COMM_GRPPK,          " +  "     tran3.TRAN_REF_NO2                         REF_NO2,             " +  "     tran3.REF_TYPE2                            TYPE2,               " +  "     ''                                         OTH_BTN,             " +  "     ''                                         OTH_DTL,             " +  "     tran3.QUOTE_TRN_SEA_PK                     FK,                  " +  "     nvl(main3.CUST_TYPE,1)                      CUST_TYPE , '' COPY, tran3.COMMODITY_MST_FKS, " +  "     TRAN3.POL_GRP_FK, TRAN3.POD_GRP_FK, TRAN3.TARIFF_GRP_FK,cntr3.PREFERENCES,0 AIF          " +  "    from                                                             " +  "     QUOTATION_MST_TBL              main3,                           " +  "     QUOTATION_DTL_TBL      tran3,                           " +  "     PORT_MST_TBL                   portpol3,                        " +  "     PORT_MST_TBL                   portpod3,                        " +  "     OPERATOR_MST_TBL               opr3,                            " +  "     COMMODITY_MST_TBL              cmdt3,                           " +  "     CONTAINER_TYPE_MST_TBL         cntr3                            " +  "   where    main3.QUOTATION_MST_PK      = tran3.QUOTATION_MST_FK             " +  "     AND    tran3.PORT_MST_POL_FK       = portpol3.PORT_MST_PK(+)            " +  "     AND    tran3.PORT_MST_POD_FK       = portpod3.PORT_MST_PK(+)            " +  "     AND    tran3.OPERATOR_MST_FK       = opr3.OPERATOR_MST_PK(+)            " +  "     AND    tran3.CONTAINER_TYPE_MST_FK = cntr3.CONTAINER_TYPE_MST_PK(+)     " +  "     AND    tran3.COMMODITY_MST_FK      = cmdt3.COMMODITY_MST_PK(+)          " +  "     AND    main3.QUOTATION_MST_PK =  " + QuotationPK +  " " + cargos + ")Q" +  " ORDER BY Q.PREFERENCES";
                    }
                    else
                    {
                        strSQL = "    Select     DISTINCT                                    " +  "     main3.QUOTATION_MST_PK                     PK,                  " +  "     Decode(tran3.TRANS_REFERED_FROM," + SourceType.SpotRate + "," + SRC(SourceType.SpotRate) + "," + " " + SourceType.Quotation + "," + SRC(SourceType.Quotation) + "," + " " + SourceType.Enquiry + "," + SRC(SourceType.Enquiry) + "," + " " + SourceType.OperatorTariff + "," + SRC(SourceType.OperatorTariff) + "," + " " + SourceType.CustomerContract + "," + SRC(SourceType.CustomerContract) + "," + " " + SourceType.GeneralTariff + "," + SRC(SourceType.GeneralTariff) + "," + " " + SourceType.Manual + "," + SRC(SourceType.Manual) + ")          TYPE,                " +  "     tran3.TRANS_REF_NO                         REF_NO,              " +  "     TO_CHAR(main3.EXPECTED_SHIPMENT_DT,'" + dateFormat + "')  SHIP_DATE, " +  "     tran3.PORT_MST_POL_FK                      POL_PK,              " +  "     portpol3.PORT_ID                           POL_ID,              " +  "     tran3.PORT_MST_POD_FK                      POD_PK,              " +  "     portpod3.PORT_ID                           POD_ID,              " +  "     tran3.OPERATOR_MST_FK                      OPER_PK,             " +  "     opr3.OPERATOR_ID                           OPER_ID,             " +  "     opr3.OPERATOR_NAME                         OPER_NAME,           " +  "     tran3.COMMODITY_MST_FK                     COMM_PK,             " +  "     cmdt3.COMMODITY_NAME                       COMM_ID,             " +  "     tran3.BASIS                                LCL_BASIS,           " +  "     NVL(dim3.DIMENTION_ID,'')                  DIMENTION_ID,        " +  "     tran3.EXPECTED_WEIGHT                      WEIGHT,              " +  "     tran3.EXPECTED_VOLUME                      VOLUME,              " +  "     tran3.ALL_IN_TARIFF                        ALL_IN_TARIFF,       " +  "     tran3.ALL_IN_QUOTED_TARIFF                 ALL_IN_QUOTE,        " +  "     nvl(tran3.BUYING_RATE,0)                   OPERATOR_RATE,       " +  "     case                                                            " +  "     when (nvl(tran3.BUYING_RATE, 0) > 0) then                       " +  "     nvl(tran3.ALL_IN_QUOTED_TARIFF -                                " +  "     nvl(tran3.BUYING_RATE, 0),                                      " +  "     0)                                                              " +  "     Else                                                            " +  "     NULL                                                            " +  "     end NET,                                                        " +  "     tran3.TRAN_REF_NO2                         REF_NO2,             " +  "     tran3.REF_TYPE2                            TYPE2,               " +  "     main3.CUSTOMER_MST_FK                      CUSTOMER_PK,         " +  "     main3.CUSTOMER_CATEGORY_MST_FK             CUSTOMER_CATPK,      " +  "     tran3.COMMODITY_GROUP_FK                   COMM_GRPPK,          " +  "     ''                                         OTH_BTN,             " +  "     ''                                         OTH_DTL,             " +  "     tran3.QUOTE_TRN_SEA_PK                     FK,                  " +  "     nvl(main3.CUST_TYPE,1)                     CUST_TYPE , '' COPY, tran3.COMMODITY_MST_FKS," +  "     TRAN3.POL_GRP_FK, TRAN3.POD_GRP_FK, TRAN3.TARIFF_GRP_FK,0 AIF   " +  "    FROM                                                             " +  "     QUOTATION_MST_TBL              main3,                           " +  "     QUOTATION_DTL_TBL      tran3,                           " +  "     PORT_MST_TBL                   portpol3,                        " +  "     PORT_MST_TBL                   portpod3,                        " +  "     OPERATOR_MST_TBL               opr3,                            " +  "     COMMODITY_MST_TBL              cmdt3,                           " +  "     DIMENTION_UNIT_MST_TBL         dim3                             " +  "    where                                                                " +  "            main3.QUOTATION_MST_PK      = tran3.QUOTATION_MST_FK             " +  "     AND    tran3.PORT_MST_POL_FK       = portpol3.PORT_MST_PK(+)            " +  "     AND    tran3.PORT_MST_POD_FK       = portpod3.PORT_MST_PK(+)            " +  "     AND    tran3.OPERATOR_MST_FK       = opr3.OPERATOR_MST_PK(+)            " +  "     AND    tran3.BASIS                 = dim3.DIMENTION_UNIT_MST_PK(+)      " +  "     AND    tran3.COMMODITY_MST_FK      = cmdt3.COMMODITY_MST_PK(+)          " +  "     AND    main3.QUOTATION_MST_PK =  " + QuotationPK +  cargos;
                    }
                }
                //If CargoType = "1" Or cargo = 1 Then
                //    strSQL = "    Select     DISTINCT                                    " & vbCrLf _
                //            & "     main3.QUOTATION_MST_PK                     PK,                  " & vbCrLf _
                //            & "     Decode(tran3.TRANS_REFERED_FROM," & SourceType.SpotRate & "," & SRC(SourceType.SpotRate) & "," _
                //            & " " & SourceType.Quotation & "," & SRC(SourceType.Quotation) & "," _
                //            & " " & SourceType.Enquiry & "," & SRC(SourceType.Enquiry) & "," _
                //            & " " & SourceType.OperatorTariff & "," & SRC(SourceType.OperatorTariff) & "," _
                //            & " " & SourceType.CustomerContract & "," & SRC(SourceType.CustomerContract) & "," _
                //            & " " & SourceType.GeneralTariff & "," & SRC(SourceType.GeneralTariff) & "," _
                //            & " " & SourceType.Manual & "," & SRC(SourceType.Manual) & ")          TYPE,                " & vbCrLf _
                //            & "     tran3.TRANS_REF_NO                         REF_NO,              " & vbCrLf _
                //            & "     tran3.TRANS_REF_NO                         REFNO,               " & vbCrLf _
                //            & "     TO_CHAR(main3.EXPECTED_SHIPMENT_DT,'" & dateFormat & "')  SHIP_DATE,    " & vbCrLf _
                //            & "     tran3.PORT_MST_POL_FK                      POL_PK,              " & vbCrLf _
                //            & "     portpol3.PORT_ID                           POL_ID,              " & vbCrLf _
                //            & "     tran3.PORT_MST_POD_FK                      POD_PK,              " & vbCrLf _
                //            & "     portpod3.PORT_ID                           POD_ID,              " & vbCrLf _
                //            & "     tran3.OPERATOR_MST_FK                      OPER_PK,             " & vbCrLf _
                //            & "     opr3.OPERATOR_ID                           OPER_ID,             " & vbCrLf _
                //            & "     opr3.OPERATOR_NAME                         OPER_NAME,           " & vbCrLf _
                //            & "     tran3.CONTAINER_TYPE_MST_FK                CNTR_PK,             " & vbCrLf _
                //            & "     cntr3.CONTAINER_TYPE_MST_ID                CNTR_ID,             " & vbCrLf _
                //            & "     tran3.EXPECTED_BOXES                       QUANTITY,            " & vbCrLf _
                //            & "     tran3.COMMODITY_MST_FK                     COMM_PK,             " & vbCrLf _
                //            & "     cmdt3.COMMODITY_NAME                       COMM_ID,             " & vbCrLf _
                //            & "     tran3.ALL_IN_TARIFF                        ALL_IN_TARIFF,       " & vbCrLf _
                //            & "     tran3.ALL_IN_QUOTED_TARIFF                 ALL_IN_QUOTE,        " & vbCrLf _
                //            & "     nvl(tran3.BUYING_RATE,0)                   OPERATOR_RATE,       " & vbCrLf _
                //            & "     case                                                            " & vbCrLf _
                //            & "     when (nvl(tran3.BUYING_RATE, 0) > 0) then                       " & vbCrLf _
                //            & "     nvl(tran3.ALL_IN_QUOTED_TARIFF -                                " & vbCrLf _
                //            & "     nvl(tran3.BUYING_RATE, 0),                                      " & vbCrLf _
                //            & "     0)                                                              " & vbCrLf _
                //            & "     Else                                                            " & vbCrLf _
                //            & "     NULL                                                            " & vbCrLf _
                //            & "     end NET,                                                        " & vbCrLf _
                //            & "     main3.CUSTOMER_MST_FK                      CUSTOMER_PK,         " & vbCrLf _
                //            & "     main3.CUSTOMER_CATEGORY_MST_FK             CUSTOMER_CATPK,      " & vbCrLf _
                //            & "     tran3.COMMODITY_GROUP_FK                   COMM_GRPPK,          " & vbCrLf _
                //            & "     tran3.TRAN_REF_NO2                         REF_NO2,             " & vbCrLf _
                //            & "     tran3.REF_TYPE2                            TYPE2,               " & vbCrLf _
                //            & "     ''                                         OTH_BTN,             " & vbCrLf _
                //            & "     ''                                         OTH_DTL,             " & vbCrLf _
                //            & "     tran3.QUOTE_TRN_SEA_PK                     FK,                  " & vbCrLf _
                //            & "     nvl(main3.CUST_TYPE,1)                      CUST_TYPE , '' COPY, tran3.COMMODITY_MST_FKS " & vbCrLf _
                //            & "    from                                                             " & vbCrLf _
                //            & "     QUOTATION_MST_TBL              main3,                           " & vbCrLf _
                //            & "     QUOTATION_DTL_TBL      tran3,                           " & vbCrLf _
                //            & "     PORT_MST_TBL                   portpol3,                        " & vbCrLf _
                //            & "     PORT_MST_TBL                   portpod3,                        " & vbCrLf _
                //            & "     OPERATOR_MST_TBL               opr3,                            " & vbCrLf _
                //            & "     COMMODITY_MST_TBL              cmdt3,                           " & vbCrLf _
                //            & "     CONTAINER_TYPE_MST_TBL         cntr3                            " & vbCrLf _
                //            & "   where    main3.QUOTATION_MST_PK      = tran3.QUOTATION_MST_FK             " & vbCrLf _
                //            & "     AND    tran3.PORT_MST_POL_FK       = portpol3.PORT_MST_PK(+)            " & vbCrLf _
                //            & "     AND    tran3.PORT_MST_POD_FK       = portpod3.PORT_MST_PK(+)            " & vbCrLf _
                //            & "     AND    tran3.OPERATOR_MST_FK       = opr3.OPERATOR_MST_PK(+)            " & vbCrLf _
                //            & "     AND    tran3.CONTAINER_TYPE_MST_FK = cntr3.CONTAINER_TYPE_MST_PK(+)     " & vbCrLf _
                //            & "     AND    tran3.COMMODITY_MST_FK      = cmdt3.COMMODITY_MST_PK(+)          " & vbCrLf _
                //            & "     AND    main3.QUOTATION_MST_PK =  " & QuotationPK & vbCrLf _
                //            & cargos
                //Else
                //    strSQL = "    Select     DISTINCT                                    " & vbCrLf _
                //            & "     main3.QUOTATION_MST_PK                     PK,                  " & vbCrLf _
                //            & "     Decode(tran3.TRANS_REFERED_FROM," & SourceType.SpotRate & "," & SRC(SourceType.SpotRate) & "," _
                //            & " " & SourceType.Quotation & "," & SRC(SourceType.Quotation) & "," _
                //            & " " & SourceType.Enquiry & "," & SRC(SourceType.Enquiry) & "," _
                //            & " " & SourceType.OperatorTariff & "," & SRC(SourceType.OperatorTariff) & "," _
                //            & " " & SourceType.CustomerContract & "," & SRC(SourceType.CustomerContract) & "," _
                //            & " " & SourceType.GeneralTariff & "," & SRC(SourceType.GeneralTariff) & "," _
                //            & " " & SourceType.Manual & "," & SRC(SourceType.Manual) & ")          TYPE,                " & vbCrLf _
                //            & "     tran3.TRANS_REF_NO                         REF_NO,              " & vbCrLf _
                //            & "     TO_CHAR(main3.EXPECTED_SHIPMENT_DT,'" & dateFormat & "')  SHIP_DATE, " & vbCrLf _
                //            & "     tran3.PORT_MST_POL_FK                      POL_PK,              " & vbCrLf _
                //            & "     portpol3.PORT_ID                           POL_ID,              " & vbCrLf _
                //            & "     tran3.PORT_MST_POD_FK                      POD_PK,              " & vbCrLf _
                //            & "     portpod3.PORT_ID                           POD_ID,              " & vbCrLf _
                //            & "     tran3.OPERATOR_MST_FK                      OPER_PK,             " & vbCrLf _
                //            & "     opr3.OPERATOR_ID                           OPER_ID,             " & vbCrLf _
                //            & "     opr3.OPERATOR_NAME                         OPER_NAME,           " & vbCrLf _
                //            & "     tran3.COMMODITY_MST_FK                     COMM_PK,             " & vbCrLf _
                //            & "     cmdt3.COMMODITY_NAME                       COMM_ID,             " & vbCrLf _
                //            & "     tran3.BASIS                                LCL_BASIS,           " & vbCrLf _
                //            & "     NVL(dim3.DIMENTION_ID,'')                  DIMENTION_ID,        " & vbCrLf _
                //            & "     tran3.EXPECTED_WEIGHT                      WEIGHT,              " & vbCrLf _
                //            & "     tran3.EXPECTED_VOLUME                      VOLUME,              " & vbCrLf _
                //            & "     tran3.ALL_IN_TARIFF                        ALL_IN_TARIFF,       " & vbCrLf _
                //            & "     tran3.ALL_IN_QUOTED_TARIFF                 ALL_IN_QUOTE,        " & vbCrLf _
                //            & "     nvl(tran3.BUYING_RATE,0)                   OPERATOR_RATE,       " & vbCrLf _
                //            & "     case                                                            " & vbCrLf _
                //            & "     when (nvl(tran3.BUYING_RATE, 0) > 0) then                       " & vbCrLf _
                //            & "     nvl(tran3.ALL_IN_QUOTED_TARIFF -                                " & vbCrLf _
                //            & "     nvl(tran3.BUYING_RATE, 0),                                      " & vbCrLf _
                //            & "     0)                                                              " & vbCrLf _
                //            & "     Else                                                            " & vbCrLf _
                //            & "     NULL                                                            " & vbCrLf _
                //            & "     end NET,                                                        " & vbCrLf _
                //            & "     tran3.TRAN_REF_NO2                         REF_NO2,             " & vbCrLf _
                //            & "     tran3.REF_TYPE2                            TYPE2,               " & vbCrLf _
                //            & "     main3.CUSTOMER_MST_FK                      CUSTOMER_PK,         " & vbCrLf _
                //            & "     main3.CUSTOMER_CATEGORY_MST_FK             CUSTOMER_CATPK,      " & vbCrLf _
                //            & "     tran3.COMMODITY_GROUP_FK                   COMM_GRPPK,          " & vbCrLf _
                //            & "     ''                                         OTH_BTN,             " & vbCrLf _
                //            & "     ''                                         OTH_DTL,             " & vbCrLf _
                //            & "     tran3.QUOTE_TRN_SEA_PK                     FK,                  " & vbCrLf _
                //            & "     nvl(main3.CUST_TYPE,1)                     CUST_TYPE , '' COPY, tran3.COMMODITY_MST_FKS          " & vbCrLf _
                //            & "    from                                                             " & vbCrLf _
                //            & "     QUOTATION_MST_TBL              main3,                           " & vbCrLf _
                //            & "     QUOTATION_DTL_TBL      tran3,                           " & vbCrLf _
                //            & "     PORT_MST_TBL                   portpol3,                        " & vbCrLf _
                //            & "     PORT_MST_TBL                   portpod3,                        " & vbCrLf _
                //            & "     OPERATOR_MST_TBL               opr3,                            " & vbCrLf _
                //            & "     COMMODITY_MST_TBL              cmdt3,                           " & vbCrLf _
                //            & "     DIMENTION_UNIT_MST_TBL         dim3                             " & vbCrLf _
                //            & "    where                                                                " & vbCrLf _
                //            & "            main3.QUOTATION_MST_PK      = tran3.QUOTATION_MST_FK             " & vbCrLf _
                //            & "     AND    tran3.PORT_MST_POL_FK       = portpol3.PORT_MST_PK(+)            " & vbCrLf _
                //            & "     AND    tran3.PORT_MST_POD_FK       = portpod3.PORT_MST_PK(+)            " & vbCrLf _
                //            & "     AND    tran3.OPERATOR_MST_FK       = opr3.OPERATOR_MST_PK(+)            " & vbCrLf _
                //            & "     AND    tran3.BASIS                 = dim3.DIMENTION_UNIT_MST_PK(+)      " & vbCrLf _
                //            & "     AND    tran3.COMMODITY_MST_FK      = cmdt3.COMMODITY_MST_PK(+)          " & vbCrLf _
                //            & "     AND    main3.QUOTATION_MST_PK =  " & QuotationPK & vbCrLf _
                //            & cargos
                //End If
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
        private string GetQuoteQueryFreights(string QuotationPK, string CargoType = "1", int Group = 0, bool AmendFlg = false, string SectorContainers = "")
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
                if (Group == 1 | Group == 2)
                {
                    if (CargoType == "1")
                    {
                        strSQL = "    (Select    DISTINCT        " +  "     tran3.TRANS_REF_NO                         REF_NO,              " +  "     PGL.PORT_GRP_MST_PK                        POL_PK,              " +  "     PGD.PORT_GRP_MST_PK                        POD_PK,              " +  "     tran3.CONTAINER_TYPE_MST_FK                CNTR_PK,             " +  "     frtd3.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt3.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt3.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frt3.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, " +  "     DECODE(frtd3.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     DECODE(frtd3.CHECK_ADVATOS, 1,'true','false') ADVATOS,         " +  "     frtd3.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr3.CURRENCY_ID                          CURR_ID,             " +  "     frtd3.QUOTED_RATE                          QUOTERATE,                " +  "     frtd3.TARIFF_RATE                          FINAL_RATE,           " +  "     frtd3.PYMT_TYPE                            PYTPE,               " +  "     decode(frtd3.PYMT_TYPE,1,'PrePaid','Collect') PYTYPE,           " +  "     frt3.preference preference,                                     " +  "     frt3.credit,'' AIFType                                          " +  "    from                                                             " +  "     QUOTATION_MST_TBL              main3,                           " +  "     QUOTATION_DTL_TBL      tran3,                           " +  "     PORT_MST_TBL              POL," +  "     PORT_MST_TBL              POD," +  "     PORT_GRP_MST_TBL          PGL," +  "     PORT_GRP_MST_TBL          PGD," +  "     QUOTATION_TRN_SEA_FRT_DTLS     frtd3,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt3,                            " +  "     CURRENCY_TYPE_MST_TBL          curr3                            " +  "    where                                                               " +  "             main3.QUOTATION_MST_PK      = tran3.QUOTATION_MST_FK           " +  "     AND     tran3.QUOTE_TRN_SEA_PK      = frtd3.QUOTE_TRN_SEA_FK           " +  "     And     TRAN3.PORT_MST_POL_FK       = POL.PORT_MST_PK" +  "     AND     TRAN3.PORT_MST_POD_FK       = POD.PORT_MST_PK" +  "     AND     TRAN3.POL_GRP_FK = PGL.PORT_GRP_MST_PK(+)" +  "     AND     TRAN3.POD_GRP_FK = PGD.PORT_GRP_MST_PK(+)" +  "     AND     frtd3.FREIGHT_ELEMENT_MST_FK = frt3.FREIGHT_ELEMENT_MST_PK(+)  " +  "     --AND    frt3.CHARGE_BASIS           <> 2                               " +  "     AND    frtd3.CURRENCY_MST_FK       = curr3.CURRENCY_MST_PK(+)         " +  "     AND    main3.QUOTATION_MST_PK =  " + QuotationPK +  cargos + " ) ";
                        //& " order by frt3.preference"  'Added " Preference Order" By Prakash Chandra on 29/4/08

                    }
                    else
                    {
                        strSQL = "    Select    DISTINCT        " +  "     tran3.TRANS_REF_NO                         REF_NO,              " +  "     PGL.PORT_GRP_MST_PK                        POL_PK,              " +  "     PGD.PORT_GRP_MST_PK                        POD_PK,              " +  "     tran3.BASIS                                LCLBASIS,            " +  "     frtd3.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt3.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt3.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frt3.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, " +  "     DECODE(frtd3.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     DECODE(frtd3.CHECK_ADVATOS, 1,'true','false') ADVATOS,          " +  "     frtd3.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr3.CURRENCY_ID                          CURR_ID,             " +  "     frtd3.QUOTED_MIN_RATE                      QUOTE_MIN_RATE,      " +  "     frtd3.QUOTED_RATE                          QUOTERATE,           " +  "     frtd3.TARIFF_RATE                          FINAL_RATE,                " +  "     frtd3.PYMT_TYPE                            PYTPE,               " +  "     decode(frtd3.PYMT_TYPE,1,'PrePaid','Collect') PYTYPE,frt3.preference preference,frt3.credit,'' AIFType " +  "    from                                                             " +  "     QUOTATION_MST_TBL              main3,                           " +  "     QUOTATION_DTL_TBL      tran3,                           " +  "     PORT_MST_TBL              POL," +  "     PORT_MST_TBL              POD," +  "     PORT_GRP_MST_TBL          PGL," +  "     PORT_GRP_MST_TBL          PGD," +  "     QUOTATION_TRN_SEA_FRT_DTLS     frtd3,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt3,                            " +  "     CURRENCY_TYPE_MST_TBL          curr3                            " +  "    where                                                                " +  "            main3.QUOTATION_MST_PK      = tran3.QUOTATION_MST_FK           " +  "     AND    tran3.QUOTE_TRN_SEA_PK      = frtd3.QUOTE_TRN_SEA_FK           " +  "     And     TRAN3.PORT_MST_POL_FK      = POL.PORT_MST_PK" +  "     AND     TRAN3.PORT_MST_POD_FK      = POD.PORT_MST_PK" +  "     AND     TRAN3.POL_GRP_FK           = PGL.PORT_GRP_MST_PK(+)" +  "     AND     TRAN3.POD_GRP_FK           = PGD.PORT_GRP_MST_PK(+)" +  "     AND    frtd3.FREIGHT_ELEMENT_MST_FK = frt3.FREIGHT_ELEMENT_MST_PK(+)  " +  "     --AND    frt3.CHARGE_BASIS           <> 2                               " +  "     AND    frtd3.CURRENCY_MST_FK       = curr3.CURRENCY_MST_PK(+)         " +  "     AND    main3.QUOTATION_MST_PK =  " + QuotationPK + "                     " +  cargos;
                        //& "            order by frt3.preference"  'Added " Preference Order" By Prakash Chandra on 29/4/08

                    }
                    //Manoharan 10July2007: to show all the freight elements when Quotation is active
                    strSQL1 = "  union  (Select DISTINCT tran3.TRANS_REF_NO REF_NO, PGL.PORT_GRP_MST_PK POL_PK, PGD.PORT_GRP_MST_PK POD_PK,  ";
                    if (CargoType == "1")
                    {
                        strSQL1 += " tran3.CONTAINER_TYPE_MST_FK CNTR_PK, ";
                    }
                    else
                    {
                        strSQL1 += " tran3.BASIS LCLBASIS, ";
                    }

                    strSQL1 += "  FRM.FREIGHT_ELEMENT_MST_PK  FRT_PK, FRM.FREIGHT_ELEMENT_ID      FRT_ID,  " +  "  FRM.FREIGHT_ELEMENT_NAME    FRT_NAME, DECODE(FRM.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, 'false'   SELECTED,'false' ADVATOS , CURR.CURRENCY_MST_PK  CURR_PK, " +  "  CURR.CURRENCY_ID  CURR_ID,  null   RATE, ";

                    if (CargoType == "2")
                    {
                        strSQL1 += " null    QUOTE_MIN_RATE, ";
                    }
                    //modified by thiyagarajan on 2/12/08 for location based curr. task
                    //Snigdharani - 30/12/2008 -  AND FRM.BY_DEFAULT = 1 - Removed
                    strSQL1 += " null    QUOTERATE, 1    PYTPE, " +  "  'PrePaid'  PYTYPE,FRM.Preference preference,FRM.credit,'' AIFType from FREIGHT_ELEMENT_MST_TBL FRM, CURRENCY_TYPE_MST_TBL CURR, " +  "  QUOTATION_MST_TBL   main3,  QUOTATION_DTL_TBL tran3, PORT_MST_TBL  POL, PORT_MST_TBL  POD, PORT_GRP_MST_TBL PGL, PORT_GRP_MST_TBL PGD, QUOTATION_TRN_SEA_FRT_DTLS  frtd3  " +  "  where FRM.ACTIVE_FLAG = 1 AND CURR.CURRENCY_MST_PK = " + HttpContext.Current.Session["currency_mst_pk"] +  "  AND FRM.BUSINESS_TYPE in (2,3) " +  "  And TRAN3.PORT_MST_POL_FK = POL.PORT_MST_PK" +  "  AND TRAN3.PORT_MST_POD_FK = POD.PORT_MST_PK" +  "  AND TRAN3.POL_GRP_FK           = PGL.PORT_GRP_MST_PK(+)" +  "  AND TRAN3.POD_GRP_FK           = PGD.PORT_GRP_MST_PK(+)" +  "  --AND FRM.CHARGE_BASIS <> 2 " +  "  AND FRM.FREIGHT_ELEMENT_MST_PK NOT IN (SELECT PARM.FRT_BOF_FK  FROM PARAMETERS_TBL PARM) " +  "  and frm.freight_element_mst_pk not in (select  frtd3.FREIGHT_ELEMENT_MST_FK  from " +  "  QUOTATION_MST_TBL   main3,  QUOTATION_DTL_TBL tran3, QUOTATION_TRN_SEA_FRT_DTLS  frtd3 " +  "  where  main3.QUOTATION_MST_PK = tran3.QUOTATION_MST_FK  AND tran3.QUOTE_TRN_SEA_PK = frtd3.QUOTE_TRN_SEA_FK " +  "  AND main3.QUOTATION_MST_PK = " + QuotationPK +  cargos;
                    strSQL1 += " ) and   main3.QUOTATION_MST_PK = tran3.QUOTATION_MST_FK   " +  "  and main3.Status=1 AND tran3.QUOTE_TRN_SEA_PK = frtd3.QUOTE_TRN_SEA_FK AND main3.QUOTATION_MST_PK = " + QuotationPK +  cargos +  ") order by preference";
                    strSQL = strSQL + strSQL1;

                }
                else
                {
                    if (CargoType == "1")
                    {
                        if (AmendFlg == false)
                        {
                            strSQL = "    (Select            " +  "     tran3.TRANS_REF_NO                         REF_NO,              " +  "     tran3.PORT_MST_POL_FK                      POL_PK,              " +  "     tran3.PORT_MST_POD_FK                      POD_PK,              " +  "     tran3.CONTAINER_TYPE_MST_FK                CNTR_PK,             " +  "     frtd3.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt3.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt3.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frt3.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, " +  "     DECODE(frtd3.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     DECODE(frtd3.CHECK_ADVATOS, 1,'true','false') ADVATOS,         " +  "     frtd3.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr3.CURRENCY_ID                          CURR_ID,             " +  "     frtd3.QUOTED_RATE                          QUOTERATE,                " +  "     frtd3.TARIFF_RATE                          FINAL_RATE,           " +  "     frtd3.PYMT_TYPE                            PYTPE,               " +  "     decode(frtd3.PYMT_TYPE,1,'PrePaid','Collect') PYTYPE,           " +  "     frt3.preference preference,                                     " +  "     frt3.credit,'' AIFType                                          " +  "    from                                                             " +  "     QUOTATION_MST_TBL              main3,                           " +  "     QUOTATION_DTL_TBL      tran3,                           " +  "     QUOTATION_TRN_SEA_FRT_DTLS     frtd3,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt3,                            " +  "     CURRENCY_TYPE_MST_TBL          curr3                            " +  "    where                                                               " +  "            main3.QUOTATION_MST_PK      = tran3.QUOTATION_MST_FK           " +  "     AND    tran3.QUOTE_TRN_SEA_PK      = frtd3.QUOTE_TRN_SEA_FK           " +  "     AND    frtd3.FREIGHT_ELEMENT_MST_FK = frt3.FREIGHT_ELEMENT_MST_PK(+)  " +  "     --AND    frt3.CHARGE_BASIS           <> 2                               " +  "     AND    frtd3.CURRENCY_MST_FK       = curr3.CURRENCY_MST_PK(+)         " +  "     AND    main3.QUOTATION_MST_PK =  " + QuotationPK +  cargos + " ) ";
                            //& " order by frt3.preference"  'Added " Preference Order" By Prakash Chandra on 29/4/08
                        }
                        else
                        {
                            strSQL = " SELECT TRAN3.TRANS_REF_NO REF_NO," +  "   TRAN3.PORT_MST_POL_FK POL_PK," +  "       TRAN3.PORT_MST_POD_FK POD_PK," +  "        TRAN3.CONTAINER_TYPE_MST_FK CNTR_PK," +  "        FRTD3.FREIGHT_ELEMENT_MST_FK FRT_PK," +  "        FRT3.FREIGHT_ELEMENT_ID FRT_ID," +  "        FRT3.FREIGHT_ELEMENT_NAME FRT_NAME," +  "       DECODE(FRT3.CHARGE_BASIS," +  "               '0'," +  "               ''," +  "              '1'," +  "               '%'," +  "               '2'," +  "               'Flat Rate'," +  "               '3'," +  "               'Kgs'," +  "              '4'," +  "               'Unit') CHARGE_BASIS," +  "      DECODE(FRTD3.CHECK_FOR_ALL_IN_RT, 1, 'true', 'false') SELECTED," +  "       DECODE(FRTD3.CHECK_ADVATOS, 1, 'true', 'false') ADVATOS," +  "        FRTD3.CURRENCY_MST_FK CURR_PK," +  "        CURR3.CURRENCY_ID CURR_ID," +  "        FRTD3.QUOTED_RATE QUOTERATE," +  "       FRTD3.TARIFF_RATE FINAL_RATE," +  "       FRTD3.PYMT_TYPE PYTPE," +  "        DECODE(FRTD3.PYMT_TYPE, 1, 'PrePaid', 'Collect') PYTYPE," +  "        FRT3.PREFERENCE PREFERENCE," +  "        FRT3.CREDIT,'' AIFType " +  "   FROM QUOTATION_MST_TBL          MAIN3," +  "        QUOTATION_DTL_TBL  TRAN3," +  "        QUOTATION_TRN_SEA_FRT_DTLS FRTD3," +  "        FREIGHT_ELEMENT_MST_TBL    FRT3," +  "        CURRENCY_TYPE_MST_TBL      CURR3" +  "  WHERE MAIN3.QUOTATION_MST_PK = TRAN3.QUOTATION_MST_FK" +  "   AND TRAN3.QUOTE_TRN_SEA_PK = FRTD3.QUOTE_TRN_SEA_FK" +  "   AND FRTD3.FREIGHT_ELEMENT_MST_FK = FRT3.FREIGHT_ELEMENT_MST_PK(+)" +  "   AND FRTD3.CURRENCY_MST_FK = CURR3.CURRENCY_MST_PK(+)" +  "    AND MAIN3.QUOTATION_MST_PK =  " + QuotationPK +  cargos + " " +  "    UNION (SELECT '' REF_NO," +  "               POL.PORT_MST_PK POL_PK," +  "               POD.PORT_MST_PK POD_PK," +  "               CNTR.CONTAINER_TYPE_MST_PK CNTR_PK," +  "               FRM.FREIGHT_ELEMENT_MST_PK," +  "              FRM.FREIGHT_ELEMENT_ID," +  "              FRM.FREIGHT_ELEMENT_NAME," +  "               DECODE(FRM.CHARGE_BASIS," +  "                      '0'," +  "                      ''," +  "                     '1'," +  "                     '%'," +  "                      '2'," +  "                      'Flat Rate'," +  "                     '3'," +  "                    'Kgs'," +  "                     '4'," +  "                     'Unit') CHARGE_BASIS," +  "               'false' SELECTED," +  "             'false' ADVATOS," +  "              CURR.CURRENCY_MST_PK," +  "              CURR.CURRENCY_ID," +  "               NULL RATE," +  "              NULL QUOTERATE," +  "              1 PYTPE," +  "             'PrePaid' PYTYPE," +  "              FRM.PREFERENCE PREFERENCE," +  "              FRM.CREDIT,'' AIFType " +  "          FROM FREIGHT_ELEMENT_MST_TBL FRM," +  "             CURRENCY_TYPE_MST_TBL   CURR," +  "              PORT_MST_TBL            POL," +  "             PORT_MST_TBL            POD," +  "               CONTAINER_TYPE_MST_TBL  CNTR" +  "        WHERE FRM.ACTIVE_FLAG = 1" +  "           AND CURR.CURRENCY_MST_PK = " + HttpContext.Current.Session["currency_mst_pk"] +  "         AND FRM.BUSINESS_TYPE IN (2, 3)" +  "         AND FRM.CHARGE_TYPE <> 3" +  "           AND (POL.PORT_MST_PK, POD.PORT_MST_PK, CNTR.CONTAINER_TYPE_MST_PK) IN " +  "               (" + Convert.ToString(SectorContainers) + ")" +  "           AND FRM.FREIGHT_ELEMENT_MST_PK NOT IN" +  "               (SELECT FRTD3.FREIGHT_ELEMENT_MST_FK" +  "                  FROM QUOTATION_MST_TBL          MAIN3," +  "                       QUOTATION_DTL_TBL  TRAN3," +  "                      QUOTATION_TRN_SEA_FRT_DTLS FRTD3," +  "                       FREIGHT_ELEMENT_MST_TBL    FRT3," +  "                       CURRENCY_TYPE_MST_TBL      CURR3" +  "                 WHERE MAIN3.QUOTATION_MST_PK = TRAN3.QUOTATION_MST_FK" +  "                   AND TRAN3.QUOTE_TRN_SEA_PK = FRTD3.QUOTE_TRN_SEA_FK" +  "                   AND FRTD3.FREIGHT_ELEMENT_MST_FK =" +  "                       FRT3.FREIGHT_ELEMENT_MST_PK(+)" +  "                   AND FRTD3.CURRENCY_MST_FK = CURR3.CURRENCY_MST_PK(+)" +  "                   AND MAIN3.QUOTATION_MST_PK =  " + QuotationPK + "))" +  cargos + " ";
                            // & "  ORDER BY PREFERENCE "
                        }

                        //Added by rabbani reason USS Gap,introduced new field as "QUOTE_MIN_RATE"
                    }
                    else
                    {
                        if (AmendFlg == false)
                        {
                            strSQL = "    Select            " +  "     tran3.TRANS_REF_NO                         REF_NO,              " +  "     tran3.PORT_MST_POL_FK                      POL_PK,              " +  "     tran3.PORT_MST_POD_FK                      POD_PK,              " +  "     tran3.BASIS                                LCLBASIS,            " +  "     frtd3.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt3.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt3.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frt3.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, " +  "     DECODE(frtd3.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     DECODE(frtd3.CHECK_ADVATOS, 1,'true','false') ADVATOS,          " +  "     frtd3.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr3.CURRENCY_ID                          CURR_ID,             " +  "     frtd3.QUOTED_MIN_RATE                      QUOTE_MIN_RATE,      " +  "     frtd3.QUOTED_RATE                          QUOTERATE,           " +  "     frtd3.TARIFF_RATE                          FINAL_RATE,                " +  "     frtd3.PYMT_TYPE                            PYTPE,               " +  "     decode(frtd3.PYMT_TYPE,1,'PrePaid','Collect') PYTYPE,frt3.preference preference,frt3.credit,'' AIFType " +  "    from                                                             " +  "     QUOTATION_MST_TBL              main3,                           " +  "     QUOTATION_DTL_TBL      tran3,                           " +  "     QUOTATION_TRN_SEA_FRT_DTLS     frtd3,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt3,                            " +  "     CURRENCY_TYPE_MST_TBL          curr3                            " +  "    where                                                                " +  "            main3.QUOTATION_MST_PK      = tran3.QUOTATION_MST_FK           " +  "     AND    tran3.QUOTE_TRN_SEA_PK      = frtd3.QUOTE_TRN_SEA_FK           " +  "     AND    frtd3.FREIGHT_ELEMENT_MST_FK = frt3.FREIGHT_ELEMENT_MST_PK(+)  " +  "     --AND    frt3.CHARGE_BASIS           <> 2                               " +  "     AND    frtd3.CURRENCY_MST_FK       = curr3.CURRENCY_MST_PK(+)         " +  "     AND    main3.QUOTATION_MST_PK =  " + QuotationPK + "                     " +  cargos;
                            //& "            order by frt3.preference"  'Added " Preference Order" By Prakash Chandra on 29/4/08
                        }
                        else
                        {
                            strSQL = "    Select            " +  "     tran3.TRANS_REF_NO                         REF_NO,              " +  "     tran3.PORT_MST_POL_FK                      POL_PK,              " +  "     tran3.PORT_MST_POD_FK                      POD_PK,              " +  "     tran3.BASIS                                LCLBASIS,            " +  "     frtd3.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt3.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt3.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frt3.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, " +  "     DECODE(frtd3.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     DECODE(frtd3.CHECK_ADVATOS, 1,'true','false') ADVATOS,          " +  "     frtd3.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr3.CURRENCY_ID                          CURR_ID,             " +  "     frtd3.QUOTED_MIN_RATE                      QUOTE_MIN_RATE,      " +  "     frtd3.QUOTED_RATE                          QUOTERATE,           " +  "     frtd3.TARIFF_RATE                          FINAL_RATE,                " +  "     frtd3.PYMT_TYPE                            PYTPE,               " +  "     decode(frtd3.PYMT_TYPE,1,'PrePaid','Collect') PYTYPE,frt3.preference preference,frt3.credit,'' AIFType " +  "    from                                                             " +  "     QUOTATION_MST_TBL              main3,                           " +  "     QUOTATION_DTL_TBL      tran3,                           " +  "     QUOTATION_TRN_SEA_FRT_DTLS     frtd3,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt3,                            " +  "     CURRENCY_TYPE_MST_TBL          curr3                            " +  "    where                                                                " +  "            main3.QUOTATION_MST_PK      = tran3.QUOTATION_MST_FK           " +  "     AND    tran3.QUOTE_TRN_SEA_PK      = frtd3.QUOTE_TRN_SEA_FK           " +  "     AND    frtd3.FREIGHT_ELEMENT_MST_FK = frt3.FREIGHT_ELEMENT_MST_PK(+)  " +  "     AND    frtd3.CURRENCY_MST_FK       = curr3.CURRENCY_MST_PK(+)         " +  "     AND    main3.QUOTATION_MST_PK =  " + QuotationPK + "                     " +  cargos + " " +  "  UNION (SELECT '' REF_NO, " +  "              POL.PORT_MST_PK POL_PK, " +  "              POD.PORT_MST_PK POD_PK, " +  "             DIM.DIMENTION_UNIT_MST_PK LCLBASIS, " +  "              FRM.FREIGHT_ELEMENT_MST_PK FRT_PK, " +  "             FRM.FREIGHT_ELEMENT_ID FRT_ID, " +  "             FRM.FREIGHT_ELEMENT_NAME FRT_NAME, " +  "              DECODE(FRM.CHARGE_BASIS, " +  "                       '0', " +  "                        '', " +  "                        '1', " +  "                       '%', " +  "                       '2', " +  "                       'Flat Rate', " +  "                       '3', " +  "                       'Kgs', " +  "                        '4', " +  "                      'Unit') CHARGE_BASIS, " +  "                'false' SELECTED, " +  "                 'false' ADVATOS, " +  "                 CURR.CURRENCY_MST_PK CURR_PK, " +  "                 CURR.CURRENCY_ID CURR_ID, " +  "                 NULL QUOTE_MIN_RATE, " +  "                NULL QUOTERATE, " +  "                 NULL FINAL_RATE, " +  "                1 PYTPE, " +  "                 'PrePaid' PYTYPE, " +  "                FRM.PREFERENCE PREFERENCE, " +  "                 FRM.CREDIT,'' AIFType " +  "            FROM FREIGHT_ELEMENT_MST_TBL FRM, " +  "                 CURRENCY_TYPE_MST_TBL   CURR, " +  "                PORT_MST_TBL            POL, " +  "                PORT_MST_TBL            POD, " +  "                 DIMENTION_UNIT_MST_TBL  DIM " +  "          WHERE FRM.ACTIVE_FLAG = 1 " +  "             AND CURR.CURRENCY_MST_PK = 1388 " +  "            AND FRM.BUSINESS_TYPE IN (2, 3) " +  "             AND FRM.CHARGE_TYPE <> 3 " +  "           AND (POL.PORT_MST_PK, POD.PORT_MST_PK, DIM.DIMENTION_UNIT_MST_PK) IN " +  "                (" + Convert.ToString(SectorContainers) + ") " +  "            AND FRM.FREIGHT_ELEMENT_MST_PK NOT IN " +  "                (SELECT FRTD3.FREIGHT_ELEMENT_MST_FK " +  "                    FROM QUOTATION_MST_TBL          MAIN3, " +  "                        QUOTATION_DTL_TBL  TRAN3, " +  "                        QUOTATION_TRN_SEA_FRT_DTLS FRTD3, " +  "                       FREIGHT_ELEMENT_MST_TBL    FRT3, " +  "                     CURRENCY_TYPE_MST_TBL      CURR3 " +  "                WHERE MAIN3.QUOTATION_MST_PK = TRAN3.QUOTATION_MST_FK " +  "                    AND TRAN3.QUOTE_TRN_SEA_PK = FRTD3.QUOTE_TRN_SEA_FK " +  "                     AND FRTD3.FREIGHT_ELEMENT_MST_FK = " +  "                        FRT3.FREIGHT_ELEMENT_MST_PK(+) " +  "                   AND FRTD3.CURRENCY_MST_FK = CURR3.CURRENCY_MST_PK(+) " +  "                    AND MAIN3.QUOTATION_MST_PK =" + QuotationPK + ") " +  cargos + ") ";
                        }

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

                    strSQL1 += "  FRM.FREIGHT_ELEMENT_MST_PK  FRT_PK, FRM.FREIGHT_ELEMENT_ID      FRT_ID,  " +  "  FRM.FREIGHT_ELEMENT_NAME    FRT_NAME, DECODE(FRM.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, 'false'   SELECTED,'false' ADVATOS , CURR.CURRENCY_MST_PK  CURR_PK, " +  "  CURR.CURRENCY_ID  CURR_ID,  null   RATE, ";

                    if (CargoType == "2")
                    {
                        strSQL1 += " null    QUOTE_MIN_RATE, ";
                    }
                    //modified by thiyagarajan on 2/12/08 for location based curr. task
                    //Snigdharani - 30/12/2008 -  AND FRM.BY_DEFAULT = 1 - Removed
                    strSQL1 += " null    QUOTERATE, 1    PYTPE, " +  "  'PrePaid'  PYTYPE,FRM.Preference preference,FRM.credit,'' AIFType from FREIGHT_ELEMENT_MST_TBL FRM, CURRENCY_TYPE_MST_TBL CURR, " +  "  QUOTATION_MST_TBL   main3,  QUOTATION_DTL_TBL tran3, QUOTATION_TRN_SEA_FRT_DTLS  frtd3  " +  "  where FRM.ACTIVE_FLAG = 1 AND CURR.CURRENCY_MST_PK = " + HttpContext.Current.Session["currency_mst_pk"] +  "  AND FRM.BUSINESS_TYPE in (2,3) " +  "  --AND FRM.CHARGE_BASIS <> 2 " +  "  AND FRM.FREIGHT_ELEMENT_MST_PK NOT IN (SELECT PARM.FRT_BOF_FK  FROM PARAMETERS_TBL PARM) " +  "  and frm.freight_element_mst_pk not in (select  frtd3.FREIGHT_ELEMENT_MST_FK  from " +  "  QUOTATION_MST_TBL   main3,  QUOTATION_DTL_TBL tran3, QUOTATION_TRN_SEA_FRT_DTLS  frtd3 " +  "  where  main3.QUOTATION_MST_PK = tran3.QUOTATION_MST_FK  AND tran3.QUOTE_TRN_SEA_PK = frtd3.QUOTE_TRN_SEA_FK " +  "  AND main3.QUOTATION_MST_PK = " + QuotationPK +  cargos;
                    strSQL1 += " ) and   main3.QUOTATION_MST_PK = tran3.QUOTATION_MST_FK   " +  "  and main3.Status=1 AND tran3.QUOTE_TRN_SEA_PK = frtd3.QUOTE_TRN_SEA_FK AND main3.QUOTATION_MST_PK = " + QuotationPK +  cargos +  ") order by preference";
                    strSQL = strSQL + strSQL1;
                }
                //If CargoType = "1" Then
                //    strSQL = "    (Select            " & vbCrLf _
                //            & "     tran3.TRANS_REF_NO                         REF_NO,              " & vbCrLf _
                //            & "     tran3.PORT_MST_POL_FK                      POL_PK,              " & vbCrLf _
                //            & "     tran3.PORT_MST_POD_FK                      POD_PK,              " & vbCrLf _
                //            & "     tran3.CONTAINER_TYPE_MST_FK                CNTR_PK,             " & vbCrLf _
                //            & "     frtd3.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " & vbCrLf _
                //            & "     frt3.FREIGHT_ELEMENT_ID                    FRT_ID,              " & vbCrLf _
                //            & "     frt3.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " & vbCrLf _
                //            & "     DECODE(frt3.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, " & vbCrLf _
                //            & "     DECODE(frtd3.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " & vbCrLf _
                //            & "     DECODE(frtd3.CHECK_ADVATOS, 1,'true','false') ADVATOS,         " & vbCrLf _
                //            & "     frtd3.CURRENCY_MST_FK                      CURR_PK,             " & vbCrLf _
                //            & "     curr3.CURRENCY_ID                          CURR_ID,             " & vbCrLf _
                //            & "     frtd3.QUOTED_RATE                          QUOTERATE,                " & vbCrLf _
                //            & "     frtd3.TARIFF_RATE                          FINAL_RATE,           " & vbCrLf _
                //            & "     frtd3.PYMT_TYPE                            PYTPE,               " & vbCrLf _
                //            & "     decode(frtd3.PYMT_TYPE,1,'PrePaid','Collect') PYTYPE,           " & vbCrLf _
                //            & "     frt3.preference preference,                                     " & vbCrLf _
                //            & "     frt3.credit                                                     " & vbCrLf _
                //            & "    from                                                             " & vbCrLf _
                //            & "     QUOTATION_MST_TBL              main3,                           " & vbCrLf _
                //            & "     QUOTATION_DTL_TBL      tran3,                           " & vbCrLf _
                //            & "     QUOTATION_TRN_SEA_FRT_DTLS     frtd3,                           " & vbCrLf _
                //            & "     FREIGHT_ELEMENT_MST_TBL        frt3,                            " & vbCrLf _
                //            & "     CURRENCY_TYPE_MST_TBL          curr3                            " & vbCrLf _
                //            & "    where                                                               " & vbCrLf _
                //            & "            main3.QUOTATION_MST_PK      = tran3.QUOTATION_MST_FK           " & vbCrLf _
                //            & "     AND    tran3.QUOTE_TRN_SEA_PK      = frtd3.QUOTE_TRN_SEA_FK           " & vbCrLf _
                //            & "     AND    frtd3.FREIGHT_ELEMENT_MST_FK = frt3.FREIGHT_ELEMENT_MST_PK(+)  " & vbCrLf _
                //            & "     --AND    frt3.CHARGE_BASIS           <> 2                               " & vbCrLf _
                //            & "     AND    frtd3.CURRENCY_MST_FK       = curr3.CURRENCY_MST_PK(+)         " & vbCrLf _
                //            & "     AND    main3.QUOTATION_MST_PK =  " & QuotationPK & vbCrLf _
                //            & cargos & " ) "
                //    '& " order by frt3.preference"  'Added " Preference Order" By Prakash Chandra on 29/4/08

                //Else 'Added by rabbani reason USS Gap,introduced new field as "QUOTE_MIN_RATE"
                //    strSQL = "    Select            " & vbCrLf _
                //            & "     tran3.TRANS_REF_NO                         REF_NO,              " & vbCrLf _
                //            & "     tran3.PORT_MST_POL_FK                      POL_PK,              " & vbCrLf _
                //            & "     tran3.PORT_MST_POD_FK                      POD_PK,              " & vbCrLf _
                //            & "     tran3.BASIS                                LCLBASIS,            " & vbCrLf _
                //            & "     frtd3.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " & vbCrLf _
                //            & "     frt3.FREIGHT_ELEMENT_ID                    FRT_ID,              " & vbCrLf _
                //            & "     frt3.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " & vbCrLf _
                //            & "     DECODE(frt3.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, " & vbCrLf _
                //            & "     DECODE(frtd3.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " & vbCrLf _
                //            & "     DECODE(frtd3.CHECK_ADVATOS, 1,'true','false') ADVATOS,          " & vbCrLf _
                //            & "     frtd3.CURRENCY_MST_FK                      CURR_PK,             " & vbCrLf _
                //            & "     curr3.CURRENCY_ID                          CURR_ID,             " & vbCrLf _
                //            & "     frtd3.QUOTED_MIN_RATE                      QUOTE_MIN_RATE,      " & vbCrLf _
                //            & "     frtd3.QUOTED_RATE                          QUOTERATE,           " & vbCrLf _
                //            & "     frtd3.TARIFF_RATE                          FINAL_RATE,                " & vbCrLf _
                //            & "     frtd3.PYMT_TYPE                            PYTPE,               " & vbCrLf _
                //            & "     decode(frtd3.PYMT_TYPE,1,'PrePaid','Collect') PYTYPE,frt3.preference preference,frt3.credit " & vbCrLf _
                //            & "    from                                                             " & vbCrLf _
                //            & "     QUOTATION_MST_TBL              main3,                           " & vbCrLf _
                //            & "     QUOTATION_DTL_TBL      tran3,                           " & vbCrLf _
                //            & "     QUOTATION_TRN_SEA_FRT_DTLS     frtd3,                           " & vbCrLf _
                //            & "     FREIGHT_ELEMENT_MST_TBL        frt3,                            " & vbCrLf _
                //            & "     CURRENCY_TYPE_MST_TBL          curr3                            " & vbCrLf _
                //            & "    where                                                                " & vbCrLf _
                //            & "            main3.QUOTATION_MST_PK      = tran3.QUOTATION_MST_FK           " & vbCrLf _
                //            & "     AND    tran3.QUOTE_TRN_SEA_PK      = frtd3.QUOTE_TRN_SEA_FK           " & vbCrLf _
                //            & "     AND    frtd3.FREIGHT_ELEMENT_MST_FK = frt3.FREIGHT_ELEMENT_MST_PK(+)  " & vbCrLf _
                //            & "     --AND    frt3.CHARGE_BASIS           <> 2                               " & vbCrLf _
                //            & "     AND    frtd3.CURRENCY_MST_FK       = curr3.CURRENCY_MST_PK(+)         " & vbCrLf _
                //            & "     AND    main3.QUOTATION_MST_PK =  " & QuotationPK & "                     " & vbCrLf _
                //            & cargos
                //    '& "            order by frt3.preference"  'Added " Preference Order" By Prakash Chandra on 29/4/08

                //End If
                //'Manoharan 10July2007: to show all the freight elements when Quotation is active
                //strSQL1 = "  union  (Select tran3.TRANS_REF_NO REF_NO, tran3.PORT_MST_POL_FK POL_PK, tran3.PORT_MST_POD_FK POD_PK,  "
                //If CargoType = "1" Then
                //    strSQL1 &= " tran3.CONTAINER_TYPE_MST_FK CNTR_PK, "
                //Else
                //    strSQL1 &= " tran3.BASIS LCLBASIS, "
                //End If

                //strSQL1 &= "  FRM.FREIGHT_ELEMENT_MST_PK  FRT_PK, FRM.FREIGHT_ELEMENT_ID      FRT_ID,  " & vbCrLf _
                //        & "  FRM.FREIGHT_ELEMENT_NAME    FRT_NAME, DECODE(FRM.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, 'false'   SELECTED,'false' ADVATOS , CURR.CURRENCY_MST_PK  CURR_PK, " & vbCrLf _
                //        & "  CURR.CURRENCY_ID  CURR_ID,  null   RATE, "

                //If CargoType = "2" Then
                //    strSQL1 &= " null    QUOTE_MIN_RATE, "
                //End If
                //'modified by thiyagarajan on 2/12/08 for location based curr. task
                //'Snigdharani - 30/12/2008 -  AND FRM.BY_DEFAULT = 1 - Removed
                //strSQL1 &= " null    QUOTERATE, 1    PYTPE, " & vbCrLf _
                //        & "  'PrePaid'  PYTYPE,FRM.Preference preference,FRM.credit from FREIGHT_ELEMENT_MST_TBL FRM, CURRENCY_TYPE_MST_TBL CURR, " & vbCrLf _
                //        & "  QUOTATION_MST_TBL   main3,  QUOTATION_DTL_TBL tran3, QUOTATION_TRN_SEA_FRT_DTLS  frtd3  " & vbCrLf _
                //        & "  where FRM.ACTIVE_FLAG = 1 AND CURR.CURRENCY_MST_PK = " & HttpContext.Current.Session["currency_mst_pk"] & vbCrLf _
                //        & "  AND FRM.BUSINESS_TYPE in (2,3) " & vbCrLf _
                //        & "  --AND FRM.CHARGE_BASIS <> 2 " & vbCrLf _
                //        & "  AND FRM.FREIGHT_ELEMENT_MST_PK NOT IN (SELECT PARM.FRT_BOF_FK  FROM PARAMETERS_TBL PARM) " & vbCrLf _
                //        & "  and frm.freight_element_mst_pk not in (select  frtd3.FREIGHT_ELEMENT_MST_FK  from " & vbCrLf _
                //        & "  QUOTATION_MST_TBL   main3,  QUOTATION_DTL_TBL tran3, QUOTATION_TRN_SEA_FRT_DTLS  frtd3 " & vbCrLf _
                //        & "  where  main3.QUOTATION_MST_PK = tran3.QUOTATION_MST_FK  AND tran3.QUOTE_TRN_SEA_PK = frtd3.QUOTE_TRN_SEA_FK " & vbCrLf _
                //        & "  AND main3.QUOTATION_MST_PK = " & QuotationPK & vbCrLf _
                //        & cargos
                //strSQL1 &= " ) and   main3.QUOTATION_MST_PK = tran3.QUOTATION_MST_FK   " & vbCrLf _
                //        & "  and main3.Status=1 AND tran3.QUOTE_TRN_SEA_PK = frtd3.QUOTE_TRN_SEA_FK AND main3.QUOTATION_MST_PK = " & QuotationPK & vbCrLf _
                //        & cargos & vbCrLf _
                //        & ") order by preference"
                //strSQL = strSQL & strSQL1
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
            try
            {
                string StrSql = null;
                DataSet DsBarManager = null;
                int strReturn = 0;

                WorkFlow objWF = new WorkFlow();
                //StrSql = "select bdmt.bcd_mst_pk,bdmt.field_name from  barcode_data_mst_tbl bdmt where bdmt.config_id_fk='" & Configid & " '"

                StrSql = "select bdmt.bcd_mst_pk,bdmt.field_name from  barcode_data_mst_tbl bdmt" +  "where bdmt.config_id_fk= '" + Configid + "'";
                DsBarManager = objWF.GetDataSet(StrSql);
                if (DsBarManager.Tables[0].Rows.Count > 0)
                {
                    var _with5 = DsBarManager.Tables[0].Rows[0];
                    strReturn = Convert.ToInt32(removeDBNull(_with5["bcd_mst_pk"]));
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
            try
            {
                string StrSql = null;
                DataSet DsBarManager = null;
                int strReturn = 0;
                WorkFlow objWF = new WorkFlow();
                StringBuilder strQuery = new StringBuilder();

                strQuery.Append("select bdmt.bcd_mst_pk, bdmt.field_name, bdmt.default_value");
                strQuery.Append("  from barcode_data_mst_tbl bdmt, barcode_doc_data_tbl bddt");
                strQuery.Append(" where bdmt.bcd_mst_pk = bddt.bcd_mst_fk");
                strQuery.Append("   and bdmt.BCD_MST_FK= " + BarCodeManagerPk);
                strQuery.Append(" ORDER BY default_value desc");

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
        public void FetchOne(DataSet GridDS = null, DataSet EnqDS = null, string EnqNo = "", string QuoteNo = "", string CustNo = "", string CustID = "", string CustCategory = "", string AgentNo = "", string AgentID = "", string CargoType = "2",
        string SectorContainers = "", string CommodityGroup = "", string ShipDate = "", string QuoteDate = "", object Options = null, int Version = 0, object QuotationStatus = null, DataTable OthDt = null, object ValidFor = null, Int16 CustomerType = 0,
        int CreditDays = 0, int CreditLimit = 0, string remarks = "", string CargoMoveCode = "", int fcllcl = 0, object BaseCurrencyId = null, int INCOTerms = 0, int PYMTType = 0, Int16 Group = 0, bool AmendFlg = false,
        int From_Flag = 0)
        {
            try
            {
                DataRow DR = null;
                DataTable ExChTable = null;
                decimal Amount = default(decimal);
                string[] cargo1 = null;

                bool forFCL = false;
                //modifided by Thiygarajan on 28/5/08 for fcl and lcl combination
                if (string.IsNullOrEmpty(EnqNo) & string.IsNullOrEmpty(QuoteNo))
                {
                    if (fcllcl == 0)
                    {
                        MakeConditionString(SectorContainers);
                    }
                    else
                    {
                        MakeConditionStrings(SectorContainers);
                        cargo1 = SectorContainers.Split('~');
                        if (fcllcl == 1)
                        {
                            SectorContainers = cargo1[0];
                            CargoType = "1";
                            cargotypes = 1;
                        }
                        if (fcllcl == 2)
                        {
                            SectorContainers = cargo1[1];
                            CargoType = "2";
                            cargotypes = 2;
                        }
                    }
                }
                else if (!string.IsNullOrEmpty(SectorContainers))
                {
                    if (fcllcl == 0)
                    {
                        MakeConditionString(SectorContainers);
                    }
                    else
                    {
                        MakeConditionStrings(SectorContainers);
                        cargo1 = SectorContainers.Split('~');
                        if (fcllcl == 1)
                        {
                            SectorContainers = cargo1[0];
                            CargoType = "1";
                            cargotypes = 1;
                        }
                        if (fcllcl == 2)
                        {
                            SectorContainers = cargo1[1];
                            CargoType = "2";
                            cargotypes = 2;
                        }
                    }
                }
                //end

                GetEnqDetail(EnqNo, CargoType, CustNo, CustID, CustCategory, AgentNo, AgentID, CommodityGroup, SectorContainers, EnqDS,
                QuoteNo, Version, QuotationStatus, OthDt, ValidFor, QuoteDate, CustomerType, ShipDate, CreditDays, CreditLimit,
                remarks, CargoMoveCode, BaseCurrencyId, INCOTerms, PYMTType, Group, AmendFlg, From_Flag);

                if (!string.IsNullOrEmpty(Convert.ToString(QuoteNo).Trim()))
                {
                    foreach (DataRow DR_loopVariable in EnqDS.Tables[0].Rows)
                    {
                        DR = DR_loopVariable;
                        DR["OTH_DTL"] = Cls_FlatRateFreights.GetOTHstring(OthDt, 0, 1, 2, (string.IsNullOrEmpty(DR["FK"].ToString()) ? Convert.ToInt16(0) : Convert.ToInt16(DR["FK"].ToString())), 4, "", 0, 0, new DataTable(), 0, 0);
                    }
                    return;
                }

                if (Convert.ToInt32(CargoType) == 1)
                {
                    forFCL = true;
                }
                else
                {
                    forFCL = false;
                }

                if (Convert.ToInt32(CargoType) == 3 & (fcllcl == 1 | cargo == 1))
                {
                    forFCL = true;
                    cargotypes = 1;
                }
                else if (Convert.ToInt32(CargoType) == 3 & (fcllcl == 2 | cargo == 2))
                {
                    forFCL = false;
                    cargotypes = 2;
                }

                System.Text.StringBuilder MasterQuery = new System.Text.StringBuilder();
                System.Text.StringBuilder FreightQuery = new System.Text.StringBuilder();

                if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[0].Selected)
                {
                    MasterQuery.Append(SpotRateQuery(forFCL, CustNo, SectorContainers, CommodityGroup, ShipDate, Convert.ToString(HttpContext.Current.Session["CURRENCY_MST_PK"])));
                    FreightQuery.Append(SpotRateFreightQuery(forFCL, CustNo, SectorContainers, CommodityGroup, ShipDate));
                }

                if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[1].Selected)
                {
                    if (MasterQuery.Length > 0)
                    {
                        MasterQuery.Append( " UNION ");
                        FreightQuery.Append( " UNION ");
                    }
                    MasterQuery.Append(CustContQuery(forFCL, CustNo, SectorContainers, CommodityGroup, ShipDate));
                    FreightQuery.Append(CustContFreightQuery(forFCL, CustNo, SectorContainers, CommodityGroup, ShipDate));
                }

                if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[2].Selected)
                {
                    if (MasterQuery.Length > 0)
                    {
                        MasterQuery.Append( " UNION ");
                        FreightQuery.Append( " UNION ");
                    }
                    MasterQuery.Append(QuoteQuery(forFCL, CustNo, SectorContainers, CommodityGroup, ShipDate, Group));
                    FreightQuery.Append(QuoteFreightQuery(forFCL, CustNo, SectorContainers, CommodityGroup, ShipDate, Group));
                }

                if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[3].Selected)
                {
                    if (MasterQuery.Length > 0)
                    {
                        MasterQuery.Append( " UNION ");
                        FreightQuery.Append( " UNION ");
                    }
                    MasterQuery.Append(OprTariffQuery(forFCL, CustNo, SectorContainers, CommodityGroup, ShipDate, Convert.ToString(HttpContext.Current.Session["CURRENCY_MST_PK"])));
                    FreightQuery.Append(OprTariffFreightQuery(forFCL, CustNo, SectorContainers, CommodityGroup, ShipDate));
                }

                if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[4].Selected)
                {
                    if (MasterQuery.Length > 0)
                    {
                        MasterQuery.Append( " UNION ");
                        FreightQuery.Append( " UNION ");
                    }
                    MasterQuery.Append(funGenTariffQuery(forFCL, CustNo, SectorContainers, CommodityGroup, ShipDate, Convert.ToString(HttpContext.Current.Session["CURRENCY_MST_PK"]), Group));
                    FreightQuery.Append(funGenTariffFreightQuery(forFCL, CustNo, SectorContainers, CommodityGroup, ShipDate, Group));
                }
                //added by vimlesh kumar for manual quote
                if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[5].Selected)
                {
                    if (MasterQuery.Length > 0)
                    {
                        MasterQuery.Append( " UNION ");
                        FreightQuery.Append( " UNION ");
                    }
                    MasterQuery.Append(QuoteQuery(forFCL, CustNo, SectorContainers, CommodityGroup, ShipDate, Group));
                    FreightQuery.Append(QuoteFreightQuery(forFCL, CustNo, SectorContainers, CommodityGroup, ShipDate, Group));
                }
                WorkFlow objWF = new WorkFlow();
                try
                {
                    GridDS = objWF.GetDataSet(MasterQuery.ToString());
                    GridDS.Tables.Add(objWF.GetDataTable(FreightQuery.ToString()));
                    DataRelation REL = null;
                    if (Convert.ToInt32(CargoType) == 1)
                    {
                        REL = new DataRelation("RFQRelation", new DataColumn[] {
                        GridDS.Tables[0].Columns["REF_NO"],
                        GridDS.Tables[0].Columns["POL_PK"],
                        GridDS.Tables[0].Columns["POD_PK"],
                        GridDS.Tables[0].Columns["CNTR_PK"]
                    }, new DataColumn[] {
                        GridDS.Tables[1].Columns["REF_NO"],
                        GridDS.Tables[1].Columns["POL_PK"],
                        GridDS.Tables[1].Columns["POD_PK"],
                        GridDS.Tables[1].Columns["CNTR_PK"]
                    });
                    }
                    else
                    {
                        REL = new DataRelation("RFQRelation", new DataColumn[] {
                        GridDS.Tables[0].Columns["REF_NO"],
                        GridDS.Tables[0].Columns["POL_PK"],
                        GridDS.Tables[0].Columns["POD_PK"],
                        GridDS.Tables[0].Columns["LCL_BASIS"]
                    }, new DataColumn[] {
                        GridDS.Tables[1].Columns["REF_NO"],
                        GridDS.Tables[1].Columns["POL_PK"],
                        GridDS.Tables[1].Columns["POD_PK"],
                        GridDS.Tables[1].Columns["LCLBASIS"]
                    });
                    }
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

        public bool IsCustomerApproved(int QuotationSeaPk)
        {
            WorkFlow objWF = new WorkFlow();
            try
            {
                int status = Convert.ToInt32(objWF.ExecuteScaler("SELECT NVL(Q.CUSTOMER_APPROVED,0) FROM QUOTATION_MST_TBL Q WHERE Q.QUOTATION_MST_PK=" + QuotationSeaPk));
                if (status == 1)
                    return true;
                else
                    return false;
            }
            catch (Exception ex)
            {
            }
            return false;
        }
        #endregion

        #region "Get Quotation Status"
        public int GetQuotStatus(string QuotationPK)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append("   SELECT COUNT(Q.QUOTATION_MST_PK)");
            sb.Append("   FROM QUOTATION_MST_TBL Q");
            sb.Append("   WHERE Q.STATUS <> 2");
            sb.Append("   AND Q.STATUS <> 4");
            sb.Append("   AND Q.QUOTATION_MST_PK = " + QuotationPK);
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

        #region " Fetch UWG1 Option Grid "

        #region " Header Query "

        #region " Quote Query."

        private string QuoteQuery(bool forFCL = true, string CustNo = "", string SectorContainers = "", string CommodityGroup = "", string ShipDate = "", int Group = 0)
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

                if (Group == 1 | Group == 2)
                {
                    if (forFCL)
                    {
                        strSQL = "  SELECT Q.PK," +  "     Q.TYPE, " +  "     Q.REF_NO, " +  "     Q.REFNO, " +  "     Q.SHIP_DATE, " +  "     Q.POL_PK, " +  "     Q.POL_ID, " +  "     Q.POD_PK, " +  "     Q.POD_ID, " +  "     Q.OPER_PK, " +  "     Q.OPER_ID, " +  "     Q.OPER_NAME, " +  "     Q.CNTR_PK, " +  "     Q.CNTR_ID, " +  "     Q.QUANTITY, " +  "     Q.COMM_PK, " +  "     Q.COMM_ID, " +  "     Q.ALL_IN_TARIFF, " +  "     Q.OPERATOR_RATE, " +  "     Q.TARIFF, " +  "     Q.NET, " +  "     Q.SELECTED, " +  "     Q.PRIORITYORDER  " +  "   FROM (Select     DISTINCT                                    " +  "     main3.QUOTATION_MST_PK                     PK,                  " +  "  " + SRC(SourceType.Quotation) + "             TYPE,                " +  "     main3.QUOTATION_REF_NO                     REF_NO,              " +  "     main3.QUOTATION_REF_NO                     REFNO,               " +  "     TO_CHAR(main3.EXPECTED_SHIPMENT_DT,'" + dateFormat + "')  SHIP_DATE,    " +  "     TRAN3.POL_GRP_FK                           POL_PK,              " +  "     PGL.PORT_GRP_ID                            POL_ID,              " +  "     TRAN3.POD_GRP_FK                           POD_PK,              " +  "     PGD.PORT_GRP_ID                            POD_ID,              " +  "     tran3.OPERATOR_MST_FK                      OPER_PK,             " +  "     opr3.OPERATOR_ID                           OPER_ID,             " +  "     opr3.OPERATOR_NAME                         OPER_NAME,           " +  "     tran3.CONTAINER_TYPE_MST_FK                CNTR_PK,             " +  "     cntr3.CONTAINER_TYPE_MST_ID                CNTR_ID,             " +  "     tran3.EXPECTED_BOXES                       QUANTITY,            " +  "     tran3.COMMODITY_MST_FK                     COMM_PK,             " +  "     cmdt3.COMMODITY_ID                         COMM_ID,             " +  "     tran3.ALL_IN_QUOTED_TARIFF                 ALL_IN_TARIFF,       " +  "     0                                          OPERATOR_RATE,       " +  "     NULL                                       TARIFF,              " +  "     NULL                                       NET,                 " +  "     'false'                                    SELECTED,            " +  "   " + SourceType.Quotation + "                 PRIORITYORDER,cntr3.PREFERENCES        " +  "    from                                                             " +  "     QUOTATION_MST_TBL              main3,                           " +  "     QUOTATION_DTL_TBL      tran3,                           " +  "     PORT_GRP_MST_TBL               PGL,                             " +  "     PORT_GRP_MST_TBL               PGD,                             " +  "     COMMODITY_MST_TBL              cmdt3,                           " +  "     OPERATOR_MST_TBL               opr3,                            " +  "     CONTAINER_TYPE_MST_TBL         cntr3                            " +  "    where                                                                " +  "            main3.QUOTATION_MST_PK      = tran3.QUOTATION_MST_FK             " +  "     AND    main3.CARGO_TYPE            in ( 1,3)                             " +  "     AND    TRAN3.POL_GRP_FK = PGL.PORT_GRP_MST_PK(+)                " +  "     AND    TRAN3.POD_GRP_FK = PGD.PORT_GRP_MST_PK(+)                " +  "     AND    tran3.OPERATOR_MST_FK       = opr3.OPERATOR_MST_PK(+)            " +  "     AND    tran3.CONTAINER_TYPE_MST_FK = cntr3.CONTAINER_TYPE_MST_PK(+)     " +  "     AND    tran3.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "       " +  "     AND    tran3.COMMODITY_MST_FK      = cmdt3.COMMODITY_MST_PK(+)          " +  "     AND    main3.CUSTOMER_MST_FK       = " + Convert.ToString(CustNo) + "               " +  "     AND   (TRAN3.POL_GRP_FK, TRAN3.POD_GRP_FK,                              " +  "                                   TRAN3.CONTAINER_TYPE_MST_FK)              " +  "              IN ( " + Convert.ToString(SectorContainers) + " )                          " +  cargos +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5)  BETWEEN       " +  "            main3.QUOTATION_DATE AND (main3.QUOTATION_DATE + main3.VALID_FOR))Q " +  "   ORDER BY Q.PK DESC,Q.PREFERENCES  ";

                    }
                    else
                    {
                        //  0   1      2         3        4       5       6       7        8        9        10
                        // PK, TYPE, REF_NO, SHIP_DATE, POL_PK, POL_ID, POD_PK, POD_ID, OPER_PK, OPER_ID, OPER_NAME
                        //  11         12       13          14          15      16         17           18   
                        // COMM_PK, COMM_ID, LCL_BASIS, DIMENTION_ID, WEIGHT, VOLUME, ALL_IN_TARIFF, SELECTED
                        strSQL = "    Select         DISTINCT                                 " +  "     main3.QUOTATION_MST_PK                     PK,                 " +  "  " + SRC(SourceType.Quotation) + "             TYPE,                " +  "     main3.QUOTATION_REF_NO                     REF_NO,             " +  "     TO_CHAR(main3.EXPECTED_SHIPMENT_DT,'" + dateFormat + "')  SHIP_DATE,   " +  "     TRAN3.POL_GRP_FK                           POL_PK,              " +  "     PGL.PORT_GRP_ID                            POL_ID,              " +  "     TRAN3.POD_GRP_FK                           POD_PK,              " +  "     PGD.PORT_GRP_ID                            POD_ID,              " +  "     tran3.OPERATOR_MST_FK                      OPER_PK,            " +  "     opr3.OPERATOR_ID                           OPER_ID,            " +  "     ''                                         OPER_NAME,          " +  "     tran3.COMMODITY_MST_FK                     COMM_PK,             " +  "     cmdt3.COMMODITY_ID                         COMM_ID,             " +  "     tran3.BASIS                                LCL_BASIS,          " +  "     NVL(dim3.DIMENTION_ID,'')                  DIMENTION_ID,       " +  "     tran3.EXPECTED_WEIGHT                      WEIGHT,             " +  "     tran3.EXPECTED_VOLUME                      VOLUME,             " +  "     tran3.ALL_IN_QUOTED_TARIFF                 ALL_IN_TARIFF,      " +  "     'false'                                    SELECTED,           " +  "     0                                          OPERATOR_RATE,      " +  "     " + SourceType.Quotation + "               PRIORITYORDER       " +  "    from                                                            " +  "     QUOTATION_MST_TBL              main3,                          " +  "     QUOTATION_DTL_TBL      tran3,                          " +  "     PORT_GRP_MST_TBL               PGL,                             " +  "     PORT_GRP_MST_TBL               PGD,                             " +  "     COMMODITY_MST_TBL              cmdt3,                           " +  "     OPERATOR_MST_TBL               opr3,                           " +  "     DIMENTION_UNIT_MST_TBL         dim3                            " +  "    where                                                                " +  "            main3.QUOTATION_MST_PK      = tran3.QUOTATION_MST_FK              " +  "     AND    main3.CARGO_TYPE    in ( 2 ,3)                                     " +  "     AND    TRAN3.POL_GRP_FK = PGL.PORT_GRP_MST_PK(+)                " +  "     AND    TRAN3.POD_GRP_FK = PGD.PORT_GRP_MST_PK(+)                " +  "     AND    tran3.OPERATOR_MST_FK       = opr3.OPERATOR_MST_PK(+)             " +  "     AND    tran3.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "        " +  "     AND    tran3.COMMODITY_MST_FK      = cmdt3.COMMODITY_MST_PK(+)           " +  "     AND    tran3.BASIS                 = dim3.DIMENTION_UNIT_MST_PK(+)       " +  "     AND    (TRAN3.POL_GRP_FK, TRAN3.POD_GRP_FK, TRAN3.BASIS )                " +  "              IN ( " + Convert.ToString(SectorContainers) + " )                           " +  "     AND    main3.CUSTOMER_MST_FK       = " + Convert.ToString(CustNo) + "                " +  cargos +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5)  BETWEEN        " +  "            main3.QUOTATION_DATE AND (main3.QUOTATION_DATE + main3.VALID_FOR) " +  "   ORDER BY PK DESC  ";
                    }
                }
                else
                {
                    if (forFCL)
                    {
                        strSQL = "  SELECT Q.PK," +  "     Q.TYPE, " +  "     Q.REF_NO, " +  "     Q.REFNO, " +  "     Q.SHIP_DATE, " +  "     Q.POL_PK, " +  "     Q.POL_ID, " +  "     Q.POD_PK, " +  "     Q.POD_ID, " +  "     Q.OPER_PK, " +  "     Q.OPER_ID, " +  "     Q.OPER_NAME," +  "     Q.CNTR_PK, " +  "     Q.CNTR_ID, " +  "     Q.QUANTITY, " +  "     Q.COMM_PK, " +  "     Q.COMM_ID, " +  "     Q.ALL_IN_TARIFF, " +  "     Q.OPERATOR_RATE, " +  "     Q.TARIFF, " +  "     Q.NET, " +  "     Q.SELECTED, " +  "     Q.PRIORITYORDER  " +  "     FROM(Select     DISTINCT                                    " +  "     main3.QUOTATION_MST_PK                     PK,                  " +  "  " + SRC(SourceType.Quotation) + "             TYPE,                " +  "     main3.QUOTATION_REF_NO                     REF_NO,              " +  "     main3.QUOTATION_REF_NO                     REFNO,               " +  "     TO_CHAR(main3.EXPECTED_SHIPMENT_DT,'" + dateFormat + "')  SHIP_DATE,    " +  "     tran3.PORT_MST_POL_FK                      POL_PK,              " +  "     portpol3.PORT_ID                           POL_ID,              " +  "     tran3.PORT_MST_POD_FK                      POD_PK,              " +  "     portpod3.PORT_ID                           POD_ID,              " +  "     tran3.OPERATOR_MST_FK                      OPER_PK,             " +  "     opr3.OPERATOR_ID                           OPER_ID,             " +  "     opr3.OPERATOR_NAME                         OPER_NAME,           " +  "     tran3.CONTAINER_TYPE_MST_FK                CNTR_PK,             " +  "     cntr3.CONTAINER_TYPE_MST_ID                CNTR_ID,             " +  "     tran3.EXPECTED_BOXES                       QUANTITY,            " +  "     tran3.COMMODITY_MST_FK                     COMM_PK,             " +  "     cmdt3.COMMODITY_ID                         COMM_ID,             " +  "     tran3.ALL_IN_QUOTED_TARIFF                 ALL_IN_TARIFF,       " +  "     0                                          OPERATOR_RATE,       " +  "     NULL                                       TARIFF,              " +  "     NULL                                       NET,                 " +  "     'false'                                    SELECTED,            " +  "   " + SourceType.Quotation + "                 PRIORITYORDER,cntr3.PREFERENCES        " +  "    from                                                             " +  "     QUOTATION_MST_TBL              main3,                           " +  "     QUOTATION_DTL_TBL      tran3,                           " +  "     PORT_MST_TBL                   portpol3,                        " +  "     PORT_MST_TBL                   portpod3,                        " +  "     COMMODITY_MST_TBL              cmdt3,                           " +  "     OPERATOR_MST_TBL               opr3,                            " +  "     CONTAINER_TYPE_MST_TBL         cntr3                            " +  "    where                                                                " +  "            main3.QUOTATION_MST_PK      = tran3.QUOTATION_MST_FK             " +  "     AND    main3.CARGO_TYPE            in ( 1,3)                             " +  "     AND    tran3.PORT_MST_POL_FK       = portpol3.PORT_MST_PK(+)            " +  "     AND    tran3.PORT_MST_POD_FK       = portpod3.PORT_MST_PK(+)            " +  "     AND    tran3.OPERATOR_MST_FK       = opr3.OPERATOR_MST_PK(+)            " +  "     AND    tran3.CONTAINER_TYPE_MST_FK = cntr3.CONTAINER_TYPE_MST_PK(+)     " +  "     AND    tran3.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "       " +  "     AND    tran3.COMMODITY_MST_FK      = cmdt3.COMMODITY_MST_PK(+)          " +  "     AND    main3.CUSTOMER_MST_FK       = " + Convert.ToString(CustNo) + "               " +  "     AND    (tran3.PORT_MST_POL_FK, tran3.PORT_MST_POD_FK,                   " +  "                                   tran3.CONTAINER_TYPE_MST_FK )             " +  "              in ( " + Convert.ToString(SectorContainers) + " )                          " +  cargos +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5)  BETWEEN       " +  "            main3.QUOTATION_DATE AND (main3.QUOTATION_DATE + main3.VALID_FOR))Q " +  "   ORDER BY Q.PK DESC,Q.PREFERENCES  ";
                    }
                    else
                    {
                        //  0   1      2         3        4       5       6       7        8        9        10
                        // PK, TYPE, REF_NO, SHIP_DATE, POL_PK, POL_ID, POD_PK, POD_ID, OPER_PK, OPER_ID, OPER_NAME
                        //  11         12       13          14          15      16         17           18   
                        // COMM_PK, COMM_ID, LCL_BASIS, DIMENTION_ID, WEIGHT, VOLUME, ALL_IN_TARIFF, SELECTED
                        strSQL = "    Select         DISTINCT                                 " +  "     main3.QUOTATION_MST_PK                     PK,                 " +  "  " + SRC(SourceType.Quotation) + "             TYPE,                " +  "     main3.QUOTATION_REF_NO                     REF_NO,             " +  "     TO_CHAR(main3.EXPECTED_SHIPMENT_DT,'" + dateFormat + "')  SHIP_DATE,   " +  "     tran3.PORT_MST_POL_FK                      POL_PK,             " +  "     portpol3.PORT_ID                           POL_ID,             " +  "     tran3.PORT_MST_POD_FK                      POD_PK,             " +  "     portpod3.PORT_ID                           POD_ID,             " +  "     tran3.OPERATOR_MST_FK                      OPER_PK,            " +  "     opr3.OPERATOR_ID                           OPER_ID,            " +  "     ''                                         OPER_NAME,          " +  "     tran3.COMMODITY_MST_FK                     COMM_PK,             " +  "     cmdt3.COMMODITY_ID                         COMM_ID,             " +  "     tran3.BASIS                                LCL_BASIS,          " +  "     NVL(dim3.DIMENTION_ID,'')                  DIMENTION_ID,       " +  "     tran3.EXPECTED_WEIGHT                      WEIGHT,             " +  "     tran3.EXPECTED_VOLUME                      VOLUME,             " +  "     tran3.ALL_IN_QUOTED_TARIFF                 ALL_IN_TARIFF,      " +  "     'false'                                    SELECTED,           " +  "     0                                          OPERATOR_RATE,      " +  "     " + SourceType.Quotation + "               PRIORITYORDER       " +  "    from                                                            " +  "     QUOTATION_MST_TBL              main3,                          " +  "     QUOTATION_DTL_TBL      tran3,                          " +  "     PORT_MST_TBL                   portpol3,                       " +  "     PORT_MST_TBL                   portpod3,                       " +  "     COMMODITY_MST_TBL              cmdt3,                           " +  "     OPERATOR_MST_TBL               opr3,                           " +  "     DIMENTION_UNIT_MST_TBL         dim3                            " +  "    where                                                                " +  "            main3.QUOTATION_MST_PK      = tran3.QUOTATION_MST_FK              " +  "     AND    main3.CARGO_TYPE    in ( 2 ,3)                                     " +  "     AND    tran3.PORT_MST_POL_FK       = portpol3.PORT_MST_PK(+)             " +  "     AND    tran3.PORT_MST_POD_FK       = portpod3.PORT_MST_PK(+)             " +  "     AND    tran3.OPERATOR_MST_FK       = opr3.OPERATOR_MST_PK(+)             " +  "     AND    tran3.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "        " +  "     AND    tran3.COMMODITY_MST_FK      = cmdt3.COMMODITY_MST_PK(+)           " +  "     AND    tran3.BASIS                 = dim3.DIMENTION_UNIT_MST_PK(+)       " +  "     AND    (tran3.PORT_MST_POL_FK, tran3.PORT_MST_POD_FK,tran3.BASIS )       " +  "              in ( " + Convert.ToString(SectorContainers) + " )                           " +  "     AND    main3.CUSTOMER_MST_FK       = " + Convert.ToString(CustNo) + "                " +  cargos +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5)  BETWEEN        " +  "            main3.QUOTATION_DATE AND (main3.QUOTATION_DATE + main3.VALID_FOR) " +  "   ORDER BY PK DESC  ";
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

        private string SpotRateQuery(bool forFCL = true, string CustNo = "", string SectorContainers = "", string CommodityGroup = "", string ShipDate = "", string CurrPK = "")
        {
            try
            {
                string strSQL = null;
                string exchQueryFCL = null;
                string exchQueryLCL = null;

                if (forFCL)
                {
                    exchQueryFCL = " ( Select nvl(Sum(NVL(FCL_APP_RATE * EXCHANGE_RATE,0)),0)          " +  "   from  RFQ_SPOT_TRN_SEA_FCL_LCL t1, V_EXCHANGE_RATE v1,            ";
                    // & vbCrLf & _
                    //Snigdharani - Removing v-array - 04/11/2008
                    //"TABLE(t1.CONTAINER_DTL_FCL) (+) c1,                         " & vbCrLf & _
                    exchQueryFCL = exchQueryFCL + "RFQ_SPOT_TRN_SEA_CONT_DET c1,                         " +  "         FREIGHT_ELEMENT_MST_TBL f1                                  " +  "   where t1.RFQ_SPOT_SEA_FK = main1.RFQ_SPOT_SEA_PK and v1.exch_rate_type_fk = 1 and              " +  "         c1.CONTAINER_TYPE_MST_FK = cont1.CONTAINER_TYPE_MST_FK  AND " +  "         C1.RFQ_SPOT_SEA_TRN_FK = t1.RFQ_SPOT_SEA_TRN_PK         AND " +  "         t1.CHECK_FOR_ALL_IN_RT   = 1                            AND " +  "         t1.CURRENCY_MST_FK =  v1.CURRENCY_MST_FK                AND " +  "         V1.CURRENCY_MST_BASE_FK= " + CurrPK + "                 AND " +  "         f1.FREIGHT_ELEMENT_MST_PK =  t1.FREIGHT_ELEMENT_MST_FK  AND " +  "         --f1.CHARGE_BASIS <> 2                                    AND " +  "         ROUND(sysdate-0.5) between v1.FROM_DATE and v1.TO_DATE )      ";
                }

                if (!forFCL)
                {
                    exchQueryLCL = "( Select nvl(Sum(NVL(LCL_APPROVED_RATE * EXCHANGE_RATE,0)),0)      " +  "   from  RFQ_SPOT_TRN_SEA_FCL_LCL t1, V_EXCHANGE_RATE v1,            " +  "         FREIGHT_ELEMENT_MST_TBL f1                                  " +  "   where t1.RFQ_SPOT_SEA_FK = main1.RFQ_SPOT_SEA_PK and v1.exch_rate_type_fk = 1 and              " +  "         t1.CHECK_FOR_ALL_IN_RT = 1 AND                              " +  "         t1.CURRENCY_MST_FK =  v1.CURRENCY_MST_FK AND                " +  "         V1.CURRENCY_MST_BASE_FK= " + CurrPK + "                 AND " +  "         f1.FREIGHT_ELEMENT_MST_PK =  t1.FREIGHT_ELEMENT_MST_FK  AND " +  "         --f1.CHARGE_BASIS <> 2                                    AND " +  "         ROUND(sysdate-0.5) between v1.FROM_DATE and v1.TO_DATE )    ";
                }
                //Snigdharani - 28/11/2008 - The query is modified by Snigdharani for comparing with commodity _group 
                //table and commodity table differently, as commodity group is mandatory but commodity is not mandatory.
                if (forFCL)
                {
                    strSQL = "  SELECT Q.PK," +  "     Q.TYPE, " +  "     Q.REF_NO, " +  "     Q.REFNO, " +  "     Q.SHIP_DATE, " +  "     Q.POL_PK, " +  "     Q.POL_ID, " +  "     Q.POD_PK, " +  "     Q.POD_ID, " +  "     Q.OPER_PK, " +  "     Q.OPER_ID, " +  "     Q.OPER_NAME, " +  "     Q.CNTR_PK, " +  "     Q.CNTR_ID, " +  "     Q.QUANTITY, " +  "     Q.COMM_PK, " +  "     Q.COMM_ID, " +  "     Q.ALL_IN_TARIFF, " +  "     Q.OPERATOR_RATE, " +  "     Q.TARIFF, " +  "     Q.NET, " +  "     Q.SELECTED, " +  "     Q.PRIORITYORDER  " +  "    FROM(Select       DISTINCT                                   " +  "     main1.RFQ_SPOT_SEA_PK                      PK,                  " +  "  " + SRC(SourceType.SpotRate) + "             TYPE,                " +  "     main1.RFQ_REF_NO                           REF_NO,              " +  "     main1.RFQ_REF_NO                           REFNO,               " +  "     TO_CHAR(main1.VALID_TO,'" + dateFormat + "')       SHIP_DATE,           " +  "     tran1.PORT_MST_POL_FK                      POL_PK,              " +  "     portpol1.PORT_ID                           POL_ID,              " +  "     tran1.PORT_MST_POD_FK                      POD_PK,              " +  "     portpod1.PORT_ID                           POD_ID,              " +  "     main1.OPERATOR_MST_FK                      OPER_PK,             " +  "     opr1.OPERATOR_ID                           OPER_ID,             " +  "     opr1.OPERATOR_NAME                         OPER_NAME,           " +  "     cont1.CONTAINER_TYPE_MST_FK                CNTR_PK,             " +  "     cntr1.CONTAINER_TYPE_MST_ID                CNTR_ID,             " +  "     main1.TEUS_VOL                             QUANTITY,            " +  "     cmdt2.COMMODITY_MST_pK                     COMM_PK,             " +  "     cmdt2.COMMODITY_ID                         COMM_ID,             " +  "     NVL(" + exchQueryFCL + ",0)                ALL_IN_TARIFF,       " +  "     NVL(" + exchQueryFCL + ",0)                OPERATOR_RATE,       " +  "     NULL                                       TARIFF,              " +  "     NULL                                       NET,                 " +  "     'false'                                    SELECTED,            " +  "   " + SourceType.SpotRate + "                  PRIORITYORDER,cntr1.PREFERENCES        " +  "    from                                                             " +  "     RFQ_SPOT_RATE_SEA_TBL          main1,                           " +  "     RFQ_SPOT_TRN_SEA_FCL_LCL       tran1,                           " +  "     PORT_MST_TBL                   portpol1,                        " +  "     PORT_MST_TBL                   portpod1,                        " +  "     OPERATOR_MST_TBL               opr1,                            ";
                    // & vbCrLf _
                    //Snigdharani - 04/11/2008 - Removing v-array
                    //& "     TABLE(tran1.CONTAINER_DTL_FCL) (+) cont1,                        " & vbCrLf _
                    strSQL = strSQL + "     RFQ_SPOT_TRN_SEA_CONT_DET cont1,                        " +  "     CONTAINER_TYPE_MST_TBL         cntr1,                           " +  "     commodity_group_mst_tbl              cmdt1, COMMODITY_MST_TBL         cmdt2" +  "    where                                                                " +  "            main1.RFQ_SPOT_SEA_PK       = tran1.RFQ_SPOT_SEA_FK               " +  "     AND    main1.CARGO_TYPE            = 1                                   " +  "     AND    main1.ACTIVE                = 1                                   " +  "     AND    main1.APPROVED              = 1                                   " +  "     AND    CONT1.RFQ_SPOT_SEA_TRN_FK   = tran1.RFQ_SPOT_SEA_TRN_PK           " +  "     AND    tran1.PORT_MST_POL_FK       = portpol1.PORT_MST_PK(+)             " +  "     AND    tran1.PORT_MST_POD_FK       = portpod1.PORT_MST_PK(+)             " +  "     AND    main1.OPERATOR_MST_FK       = opr1.OPERATOR_MST_PK(+)             " +  "     AND    cont1.CONTAINER_TYPE_MST_FK = cntr1.CONTAINER_TYPE_MST_PK(+)      " +  "     AND    main1.commodity_group_fk      = cmdt1.commodity_group_pk           " +  "     AND    cmdt1.COMMODITY_GROUP_PK    = " + Convert.ToString(CommodityGroup) + "        " +  "     and main1.commodity_mst_fk = cmdt2.commodity_mst_pk(+)       " +  "     AND    (main1.CUSTOMER_MST_FK       = " + Convert.ToString(CustNo) + " OR main1.CUSTOMER_MST_FK IS NULL) " +  "     AND    (tran1.PORT_MST_POL_FK, tran1.PORT_MST_POD_FK,                    " +  "                                   cont1.CONTAINER_TYPE_MST_FK )              " +  "              in ( " + Convert.ToString(SectorContainers) + " )                           " +  "     AND    TO_DATE('" + ShipDate + "','" + dateFormat + "') BETWEEN                  " +  "            main1.VALID_FROM   AND   main1.VALID_TO  )Q                         " +  "   ORDER BY Q.PK DESC,Q.PREFERENCES ";
                }
                else
                {
                    strSQL = "    Select         DISTINCT                                 " +  "     main1.RFQ_SPOT_SEA_PK                      PK,                  " +  "  " + SRC(SourceType.SpotRate) + "              TYPE,                " +  "     main1.RFQ_REF_NO                           REF_NO,              " +  "     TO_CHAR(main1.VALID_TO,'" + dateFormat + "')       SHIP_DATE,           " +  "     tran1.PORT_MST_POL_FK                      POL_PK,              " +  "     portpol1.PORT_ID                           POL_ID,              " +  "     tran1.PORT_MST_POD_FK                      POD_PK,              " +  "     portpod1.PORT_ID                           POD_ID,              " +  "     main1.OPERATOR_MST_FK                      OPER_PK,             " +  "     opr1.OPERATOR_ID                           OPER_ID,             " +  "     opr1.OPERATOR_NAME                         OPER_NAME,           " +  "     main1.COMMODITY_MST_FK                     COMM_PK,             " +  "     cmdt2.COMMODITY_ID                         COMM_ID,             " +  "     tran1.LCL_BASIS                            LCL_BASIS,           " +  "     NVL(dim1.DIMENTION_ID,'')                  DIMENTION_ID,        " +  "     0                                          WEIGHT,              " +  "     0                                          VOLUME,              " +  "     NVL(" + exchQueryLCL + ",0)                ALL_IN_TARIFF,       " +  "     'false'                                    SELECTED,            " +  "     NVL(" + exchQueryLCL + ",0)                OPERATOR_RATE,       " +  "     " + SourceType.SpotRate + "                PRIORITYORDER        " +  "    from                                                             " +  "     RFQ_SPOT_RATE_SEA_TBL          main1,                           " +  "     RFQ_SPOT_TRN_SEA_FCL_LCL       tran1,                           " +  "     PORT_MST_TBL                   portpol1,                        " +  "     PORT_MST_TBL                   portpod1,                        " +  "     OPERATOR_MST_TBL               opr1,                            " +  "     commodity_group_mst_tbl        cmdt1, COMMODITY_MST_TBL         cmdt2," +  "     DIMENTION_UNIT_MST_TBL         dim1                             " +  "    where                                                                " +  "            main1.RFQ_SPOT_SEA_PK       = tran1.RFQ_SPOT_SEA_FK              " +  "     AND    main1.CARGO_TYPE            = 2                                  " +  "     AND    main1.ACTIVE                = 1                                  " +  "     AND    main1.APPROVED              = 1                                  " +  "     AND    tran1.PORT_MST_POL_FK       = portpol1.PORT_MST_PK(+)            " +  "     AND    tran1.PORT_MST_POD_FK       = portpod1.PORT_MST_PK(+)            " +  "     AND    main1.OPERATOR_MST_FK       = opr1.OPERATOR_MST_PK(+)            " +  "     AND    main1.commodity_group_fk      = cmdt1.commodity_group_pk           " +  "     and    main1.commodity_mst_fk = cmdt2.commodity_mst_pk(+)       " +  "     AND    cmdt1.COMMODITY_GROUP_PK    = " + Convert.ToString(CommodityGroup) + "       " +  "     AND    (main1.CUSTOMER_MST_FK       = " + Convert.ToString(CustNo) + " OR main1.CUSTOMER_MST_FK IS NULL) " +  "     AND    tran1.LCL_BASIS             = dim1.DIMENTION_UNIT_MST_PK(+)      " +  "     AND    (tran1.PORT_MST_POL_FK, tran1.PORT_MST_POD_FK,tran1.LCL_BASIS )  " +  "              in ( " + Convert.ToString(SectorContainers) + " )                          " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN      " +  "            main1.VALID_FROM   AND   main1.VALID_TO                         " +  "   ORDER BY PK DESC ";
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
                    exchQueryFCL = " NVL(( Case WHEN MAIN2.STATUS <> 2 Then     " + "            tran2.CURRENT_BOF_RATE                     else        " + "            tran2.APPROVED_BOF_RATE End                            " + "         )* get_ex_rate(tran2.currency_mst_fk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",sysdate) ,0 ) + " + " ( Select nvl(Sum(NVL(APP_SURCHARGE_AMT * get_ex_rate(tran2.currency_mst_fk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",sysdate),0)),0)" +  "       from  CONT_CUST_TRN_SEA_TBL t2,                     " +  "               CONT_SUR_CHRG_SEA_TBL f2                                        " +  "       where t2.CONT_CUST_TRN_SEA_PK  = tran2.CONT_CUST_TRN_SEA_PK  and         " +  "               t2.CONT_CUST_TRN_SEA_PK  = f2.CONT_CUST_TRN_SEA_FK and          " +  "               f2.CHECK_FOR_ALL_IN_RT   = 1                                 ";
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
                    exchQueryLCL = "   ( Case  WHEN MAIN2.STATUS <> 2 Then       " + "            tran2.CURRENT_BOF_RATE                     else        " + "            tran2.APPROVED_BOF_RATE End                            " + "      ) +                                                          " + " ( Select nvl(Sum(NVL(APP_SURCHARGE_AMT * get_ex_rate(tran2.currency_mst_fk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",sysdate),0)),0)              " +  "       from  CONT_CUST_TRN_SEA_TBL t2,                     " +  "               CONT_SUR_CHRG_SEA_TBL f2                                        " +  "       where t2.CONT_CUST_TRN_SEA_PK  = tran2.CONT_CUST_TRN_SEA_PK and         " +  "               t2.CONT_CUST_TRN_SEA_PK  = f2.CONT_CUST_TRN_SEA_FK and          " +  "               f2.CHECK_FOR_ALL_IN_RT   = 1                                 " +  "    )    +                                                                     " +  " ( Select NVL(Sum(NVL(LCL_TARIFF_RATE * get_ex_rate(tran2.currency_mst_fk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",sysdate),0 ) ),0) from            " +  "         TARIFF_TRN_SEA_FCL_LCL tt5,                      " +  "         TARIFF_MAIN_SEA_TBL mm5                                               " +  "   where                                                                       " +  "         mm5.TARIFF_MAIN_SEA_PK    = tt5.TARIFF_MAIN_SEA_FK     AND          " +  "         tt5.PORT_MST_POL_FK       = tran2.PORT_MST_POL_FK        AND          " +  "         tt5.PORT_MST_POD_FK       = tran2.PORT_MST_POD_FK        AND          " +  "         tt5.CHECK_FOR_ALL_IN_RT   = 1                            AND          " +  "         tran2.SUBJECT_TO_SURCHG_CHG = 1                          AND          " +  "         mm5.OPERATOR_MST_FK       = main2.OPERATOR_MST_FK        AND          " +  "         mm5.CARGO_TYPE            = 2                            AND          " +  "         mm5.ACTIVE                = 1                            AND          " +  "         mm5.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + " AND          " +  "         ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN           " +  "         tt5.VALID_FROM   AND   NVL(tt5.VALID_TO,NULL_DATE_FORMAT)   AND          " +  "         tt5.FREIGHT_ELEMENT_MST_FK not in                                     " +  "           ( Select FREIGHT_ELEMENT_MST_FK                                     " +  "              from CONT_CUST_TRN_SEA_TBL tt2, CONT_SUR_CHRG_SEA_TBL ff2 where  " +  "                   tt2.CONT_CUST_TRN_SEA_PK  = tran2.CONT_CUST_TRN_SEA_PK and  " +  "                   tt2.CONT_CUST_TRN_SEA_PK  = ff2.CONT_CUST_TRN_SEA_FK and    " +  "                   ff2.CHECK_FOR_ALL_IN_RT   = 1                               " +  "           )                                                                   " +  "  )                                                                            ";

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
                    strSQL = "  SELECT Q.PK," +  "     Q.TYPE, " +  "     Q.REF_NO, " +  "     Q.REFNO, " +  "     Q.SHIP_DATE, " +  "     Q.POL_PK, " +  "     Q.POL_ID, " +  "     Q.POD_PK, " +  "     Q.POD_ID, " +  "     Q.OPER_PK, " +  "     Q.OPER_ID, " +  "     Q.OPER_NAME, " +  "     Q.CNTR_PK, " +  "     Q.CNTR_ID, " +  "     Q.QUANTITY, " +  "     Q.COMM_PK, " +  "     Q.COMM_ID, " +  "     Q.ALL_IN_TARIFF, " +  "     Q.OPERATOR_RATE, " +  "     Q.TARIFF, " +  "     Q.NET, " +  "     Q.SELECTED, " +  "     Q.PRIORITYORDER  " +  "     FROM(Select      DISTINCT                                             " +  "     main2.CONT_CUST_SEA_PK                     PK,                 " +  "  " + SRC(SourceType.CustomerContract) + "      TYPE,               " +  "     main2.CONT_REF_NO                          REF_NO,             " +  "     main2.CONT_REF_NO                          REFNO,              " +  "     TO_CHAR(tran2.VALID_TO,'" + dateFormat + "')       SHIP_DATE,          " +  "     tran2.PORT_MST_POL_FK                      POL_PK,             " +  "     portpol2.PORT_ID                           POL_ID,             " +  "     tran2.PORT_MST_POD_FK                      POD_PK,             " +  "     portpod2.PORT_ID                           POD_ID,             " +  "     main2.OPERATOR_MST_FK                      OPER_PK,            " +  "     opr2.OPERATOR_ID                           OPER_ID,            " +  "     opr2.OPERATOR_NAME                         OPER_NAME,          " +  "     tran2.CONTAINER_TYPE_MST_FK                CNTR_PK,            " +  "     cntr2.CONTAINER_TYPE_MST_ID                CNTR_ID,            " +  "     tran2.EXPECTED_VOLUME                      QUANTITY,           " +  "     main2.COMMODITY_MST_FK                     COMM_PK,            " +  "     NVL(cmdt2.COMMODITY_ID,'')                 COMM_ID,            " +  "     NVL(" + exchQueryFCL + ",0)                ALL_IN_TARIFF,      " +  "     NVL(" + OperatorRate + ",0)                OPERATOR_RATE,      " +  "     NULL                                       TARIFF,             " +  "     NULL                                       NET,                " +  "     'false'                                    SELECTED,           " +  "   " + SourceType.CustomerContract + "          PRIORITYORDER,cntr2.PREFERENCES       " +  "    from                                                            " +  "     CONT_CUST_SEA_TBL              main2,                          " +  "     CONT_CUST_TRN_SEA_TBL          tran2,                          " +  "     PORT_MST_TBL                   portpol2,                       " +  "     PORT_MST_TBL                   portpod2,                       " +  "     OPERATOR_MST_TBL               opr2,                           " +  "     CONTAINER_TYPE_MST_TBL         cntr2,                          " +  "     COMMODITY_MST_TBL              cmdt2                          " +  "    where                                                                " +  "            main2.CONT_CUST_SEA_PK      = tran2.CONT_CUST_SEA_FK              " +  "     AND    main2.CARGO_TYPE            = 1  and  main2.active=1             " +  "     AND    tran2.PORT_MST_POL_FK       = portpol2.PORT_MST_PK(+)             " +  "     AND    tran2.PORT_MST_POD_FK       = portpod2.PORT_MST_PK(+)             " +  "     AND    main2.OPERATOR_MST_FK       = opr2.OPERATOR_MST_PK(+)             " +  "     AND    tran2.CONTAINER_TYPE_MST_FK = cntr2.CONTAINER_TYPE_MST_PK(+)      " +  "     AND    main2.COMMODITY_MST_FK      = cmdt2.COMMODITY_MST_PK(+)           " +  "     AND ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN           " +  "         tran2.VALID_FROM AND NVL(tran2.VALID_TO, NULL_DATE_FORMAT)           " +  "     AND    main2.COMMODITY_GROUP_MST_FK = " + Convert.ToString(CommodityGroup) + "       " +  "     AND    main2.CUSTOMER_MST_FK       =  " + Convert.ToString(CustNo) + "               " +  "     AND    (tran2.PORT_MST_POL_FK, tran2.PORT_MST_POD_FK,                    " +  "                                   tran2.CONTAINER_TYPE_MST_FK )              " +  "              in ( " + Convert.ToString(SectorContainers) + " ))Q                        " +  "   ORDER BY Q.PK DESC,Q.PREFERENCES ";

                    //The following lines are delete - Snigdharani - 28/11/2008
                    //& "     V_EXCHANGE_RATE                Vex2                             " & vbCrLf _
                    //  and vex2.exch_rate_type_fk = 1
                    //& "     AND    tran2.CURRENCY_MST_FK       = Vex2.CURRENCY_MST_FK                 " & vbCrLf _
                    //       & "     AND    ROUND(sysdate-0.5) between Vex2.FROM_DATE and Vex2.TO_DATE         " & vbCrLf _

                }
                else
                {
                    strSQL = "    Select     DISTINCT                                 " +  "     main2.CONT_CUST_SEA_PK                     PK,                  " +  "  " + SRC(SourceType.CustomerContract) + "      TYPE,                " +  "     main2.CONT_REF_NO                          REF_NO,              " +  "     TO_CHAR(tran2.VALID_TO,'" + dateFormat + "')       SHIP_DATE,           " +  "     tran2.PORT_MST_POL_FK                      POL_PK,              " +  "     portpol2.PORT_ID                           POL_ID,              " +  "     tran2.PORT_MST_POD_FK                      POD_PK,              " +  "     portpod2.PORT_ID                           POD_ID,              " +  "     main2.OPERATOR_MST_FK                      OPER_PK,             " +  "     opr2.OPERATOR_ID                           OPER_ID,             " +  "     opr2.OPERATOR_NAME                         OPER_NAME,           " +  "     main2.COMMODITY_MST_FK                     COMM_PK,             " +  "     NVL(cmdt2.COMMODITY_ID,'')                 COMM_ID,             " +  "     tran2.LCL_BASIS                            LCL_BASIS,           " +  "     NVL(dim2.DIMENTION_ID,'')                  DIMENTION_ID,        " +  "     0                                          WEIGHT,              " +  "     0                                          VOLUME,              " +  "     NVL(" + exchQueryLCL + ",0)                ALL_IN_TARIFF,       " +  "     'false'                                    SELECTED,            " +  "     NVL(" + OperatorRate + ",0)                OPERATOR_RATE,       " +  "     " + SourceType.CustomerContract + "        PRIORITYORDER        " +  "    from                                                             " +  "     CONT_CUST_SEA_TBL              main2,                           " +  "     CONT_CUST_TRN_SEA_TBL          tran2,                           " +  "     PORT_MST_TBL                   portpol2,                        " +  "     PORT_MST_TBL                   portpod2,                        " +  "     OPERATOR_MST_TBL               opr2,                            " +  "     COMMODITY_MST_TBL              cmdt2,                           " +  "     DIMENTION_UNIT_MST_TBL         dim2                            " +  "    where                                                                " +  "            main2.CONT_CUST_SEA_PK      = tran2.CONT_CUST_SEA_FK             " +  "     AND    main2.CARGO_TYPE            = 2                                    " +  "     AND    tran2.PORT_MST_POL_FK       = portpol2.PORT_MST_PK(+)              " +  "     AND    tran2.PORT_MST_POD_FK       = portpod2.PORT_MST_PK(+)              " +  "     AND    main2.OPERATOR_MST_FK       = opr2.OPERATOR_MST_PK(+)              " +  "     AND    main2.COMMODITY_MST_FK      = cmdt2.COMMODITY_MST_PK(+)            " +  "     AND ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN           " +  "         tran2.VALID_FROM AND NVL(tran2.VALID_TO, NULL_DATE_FORMAT)           " +  "     AND    main2.COMMODITY_GROUP_MST_FK = " + Convert.ToString(CommodityGroup) + "        " +  "     AND    tran2.LCL_BASIS             = dim2.DIMENTION_UNIT_MST_PK(+)        " +  "     AND    (tran2.PORT_MST_POL_FK, tran2.PORT_MST_POD_FK,                     " +  "                                   tran2.LCL_BASIS )                           " +  "              in ( " + Convert.ToString(SectorContainers) + " )                            " +  "     AND    main2.CUSTOMER_MST_FK       = " + Convert.ToString(CustNo) + "                 " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN        " +  "            tran2.VALID_FROM   AND   NVL(tran2.VALID_TO,NULL_DATE_FORMAT)         " +  "   ORDER BY PK DESC ";
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

        private string OprTariffQuery(bool forFCL = true, string CustNo = "", string SectorContainers = "", string CommodityGroup = "", string ShipDate = "", string CurrPK = "")
        {
            try
            {
                string strSQL = null;
                string exchQueryFCL = null;
                string exchQueryLCL = null;
                string OperatorRate = null;

                if (forFCL)
                {
                    exchQueryFCL = " ( Select nvl(Sum(NVL(FCL_REQ_RATE * EXCHANGE_RATE,0)),0)               " +  "   from  TARIFF_TRN_SEA_FCL_LCL t5, V_EXCHANGE_RATE v5,";
                    //TABLE(t5.CONTAINER_DTL_FCL) (+) c5 " & vbCrLf & _
                    //Modified by Snigdharani - 29/10/2008 - Removing v-array
                    exchQueryFCL = exchQueryFCL + " TARIFF_TRN_SEA_CONT_DTL c5,                             " +  "         FREIGHT_ELEMENT_MST_TBL f5                                       " +  "   where t5.TARIFF_MAIN_SEA_FK = main5.TARIFF_MAIN_SEA_PK and v5.exch_rate_type_fk = 1 and             " +  "         c5.CONTAINER_TYPE_MST_FK = cont5.CONTAINER_TYPE_MST_FK  AND      " +  "         c5.TARIFF_TRN_SEA_FK = t5.TARIFF_TRN_SEA_PK  AND                 " +  "         t5.PORT_MST_POL_FK = tran5.PORT_MST_POL_FK AND                   " +  "         t5.PORT_MST_POD_FK = tran5.PORT_MST_POD_FK AND                   " +  "         t5.CHECK_FOR_ALL_IN_RT   = 1 AND v5.CURRENCY_MST_BASE_FK = main5.base_currency_fk " +  "         and t5.CURRENCY_MST_FK =  v5.CURRENCY_MST_FK AND                     " +  "         f5.FREIGHT_ELEMENT_MST_PK =  t5.FREIGHT_ELEMENT_MST_FK  AND      " +  "         --f5.CHARGE_BASIS <> 2                                    AND      " +  "         ROUND(sysdate-0.5) between v5.FROM_DATE and v5.TO_DATE )         ";

                    OperatorRate = " ( Select nvl(Sum(NVL(FCL_APP_RATE * EXCHANGE_RATE,0)),0)               " +  "   from  CONT_MAIN_SEA_TBL m, CONT_TRN_SEA_FCL_LCL t, V_EXCHANGE_RATE v,  ";
                    // & vbCrLf & _
                    //Snigdharani - 05/11/2008 - Removing v-array
                    //"         TABLE(t.CONTAINER_DTL_FCL) (+) c                                " & vbCrLf & _
                    OperatorRate = OperatorRate + "CONT_TRN_SEA_FCL_RATES c                                 " +  "   where m.ACTIVE                     = 1   and v.exch_rate_type_fk = 1  AND                         " +  "         m.CONT_APPROVED              = 1     AND                         " +  "         m.CARGO_TYPE                 = 1     AND                         " +  "         m.OPERATOR_MST_FK            = main5.OPERATOR_MST_FK AND         " +  "         t.CONT_TRN_SEA_PK = C.CONT_TRN_SEA_FK AND                        " +  "         m.COMMODITY_GROUP_FK         =  " + Convert.ToString(CommodityGroup) + " AND " +  "         ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN      " +  "           v.FROM_DATE and v.TO_DATE                                          AND " +  "         ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN      " +  "           t.VALID_FROM   AND   NVL(t.VALID_TO,NULL_DATE_FORMAT)         AND " +  "         t.CONT_MAIN_SEA_FK           = m.CONT_MAIN_SEA_PK            AND " +  "         c.CONTAINER_TYPE_MST_FK      = cont5.CONTAINER_TYPE_MST_FK   AND " +  "         t.PORT_MST_POL_FK            = tran5.PORT_MST_POL_FK         AND " +  "         t.PORT_MST_POD_FK            = tran5.PORT_MST_POD_FK         AND " +  "         t.CHECK_FOR_ALL_IN_RT        = 1                             AND " +  "         t.CURRENCY_MST_FK            = v.CURRENCY_MST_FK AND             " +  "  v.CURRENCY_MST_BASE_FK = m.base_currency_fk) ";
                    //"         ROUND(sysdate-0.5) between v.FROM_DATE and v.TO_DATE )           "

                }

                if (!forFCL)
                {
                    exchQueryLCL = "( Select nvl(Sum(NVL(LCL_TARIFF_RATE * EXCHANGE_RATE,0)),0)             " +  "   from  TARIFF_TRN_SEA_FCL_LCL t5, V_EXCHANGE_RATE v5,                   " +  "         FREIGHT_ELEMENT_MST_TBL f5                                       " +  "   where t5.TARIFF_MAIN_SEA_FK = main5.TARIFF_MAIN_SEA_PK and v5.exch_rate_type_fk = 1 and             " +  "         t5.PORT_MST_POL_FK = tran5.PORT_MST_POL_FK AND                   " +  "         t5.PORT_MST_POD_FK = tran5.PORT_MST_POD_FK AND                   " +  "         t5.CHECK_FOR_ALL_IN_RT   = 1 AND v5.CURRENCY_MST_BASE_FK = main5.base_currency_fk " +  "         and t5.CURRENCY_MST_FK =  v5.CURRENCY_MST_FK AND                     " +  "         f5.FREIGHT_ELEMENT_MST_PK =  t5.FREIGHT_ELEMENT_MST_FK  AND      " +  "         --f5.CHARGE_BASIS <> 2                                    AND      " +  "         ROUND(sysdate-0.5) between v5.FROM_DATE and v5.TO_DATE )         ";

                    OperatorRate = " ( Select nvl(Sum(NVL(LCL_APPROVED_RATE * EXCHANGE_RATE,0)),0)          " +  "   from  CONT_MAIN_SEA_TBL m, CONT_TRN_SEA_FCL_LCL t, V_EXCHANGE_RATE v   " +  "   where m.ACTIVE                     = 1  and v.exch_rate_type_fk = 1   AND                         " +  "         m.CONT_APPROVED              = 1     AND                         " +  "         m.CARGO_TYPE                 = 2     AND                         " +  "         m.OPERATOR_MST_FK            = main5.OPERATOR_MST_FK AND         " +  "         m.COMMODITY_GROUP_FK         =  " + Convert.ToString(CommodityGroup) + " AND " +  "         ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN      " +  "           v.FROM_DATE and v.TO_DATE                                          AND " +  "         ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN      " +  "           t.VALID_FROM   AND   NVL(t.VALID_TO,NULL_DATE_FORMAT)         AND " +  "         t.CONT_MAIN_SEA_FK           = m.CONT_MAIN_SEA_PK            AND " +  "         t.LCL_BASIS                  = tran5.LCL_BASIS               AND " +  "         t.PORT_MST_POL_FK            = tran5.PORT_MST_POL_FK         AND " +  "         t.PORT_MST_POD_FK            = tran5.PORT_MST_POD_FK         AND " +  "         t.CHECK_FOR_ALL_IN_RT        = 1                             AND " +  "         t.CURRENCY_MST_FK            = v.CURRENCY_MST_FK AND             " +  "  v.CURRENCY_MST_BASE_FK = m.base_currency_fk) ";
                    //"         ROUND(sysdate-0.5) between v.FROM_DATE and v.TO_DATE )           "
                    //Operator rate query(for FCL and LCL) is Changed by Snigdharani - 04/06/2009

                }

                if (forFCL)
                {
                    strSQL = "  SELECT Q.PK," +  "     Q.TYPE, " +  "     Q.REF_NO, " +  "     Q.REFNO, " +  "     Q.SHIP_DATE, " +  "     Q.POL_PK, " +  "     Q.POL_ID, " +  "     Q.POD_PK, " +  "     Q.POD_ID, " +  "     Q.OPER_PK, " +  "     Q.OPER_ID, " +  "     Q.OPER_NAME, " +  "     Q.CNTR_PK, " +  "     Q.CNTR_ID, " +  "     Q.QUANTITY, " +  "     Q.COMM_PK, " +  "     Q.COMM_ID, " +  "     Q.ALL_IN_TARIFF, " +  "     Q.OPERATOR_RATE, " +  "     Q.TARIFF, " +  "     Q.NET, " +  "     Q.SELECTED, " +  "     Q.PRIORITYORDER  " +  "    FROM (Select  DISTINCT                             " +  "     main5.TARIFF_MAIN_SEA_PK                   PK,                  " +  "  " + SRC(SourceType.OperatorTariff) + "        TYPE,                " +  "     main5.TARIFF_REF_NO                        REF_NO,              " +  "     main5.TARIFF_REF_NO                        REFNO,               " +  "     TO_CHAR(tran5.VALID_TO,'" + dateFormat + "')       SHIP_DATE,           " +  "     tran5.PORT_MST_POL_FK                      POL_PK,              " +  "     portpol5.PORT_ID                           POL_ID,              " +  "     tran5.PORT_MST_POD_FK                      POD_PK,              " +  "     portpod5.PORT_ID                           POD_ID,              " +  "     main5.OPERATOR_MST_FK                      OPER_PK,             " +  "     opr5.OPERATOR_ID                           OPER_ID,             " +  "     opr5.OPERATOR_NAME                         OPER_NAME,           " +  "     cont5.CONTAINER_TYPE_MST_FK                CNTR_PK,             " +  "     cntr5.CONTAINER_TYPE_MST_ID                CNTR_ID,             " +  "     NULL                                       QUANTITY,            " +  "     0                                          COMM_PK,             " +  "     ''                                         COMM_ID,             " +  "     NVL(" + exchQueryFCL + ",0)                ALL_IN_TARIFF,       " +  "     NVL(" + OperatorRate + ",0)                OPERATOR_RATE,       " +  "     NULL                                       TARIFF,              " +  "     NULL                                       NET,                 " +  "     'false'                                    SELECTED,            " +  "   " + SourceType.OperatorTariff + "            PRIORITYORDER,        " +  "   CNTR5.PREFERENCES                                               " +  "    from                                                             " +  "     TARIFF_MAIN_SEA_TBL            main5,                           " +  "     TARIFF_TRN_SEA_FCL_LCL         tran5,                           " +  "     PORT_MST_TBL                   portpol5,                        " +  "     PORT_MST_TBL                   portpod5,                        " +  "     OPERATOR_MST_TBL               opr5,                            ";
                    //& vbCrLf _
                    //Modified by Snigdharani - 29/10/2008 - Removing v-array
                    strSQL = strSQL + " TARIFF_TRN_SEA_CONT_DTL    cont5,                           " +  "     CONTAINER_TYPE_MST_TBL         cntr5                            " +  "     where                                                                    " +  "            main5.TARIFF_MAIN_SEA_PK    = tran5.TARIFF_MAIN_SEA_FK            " +  "     AND    main5.CARGO_TYPE            = 1                                   " +  "     AND    cont5.TARIFF_TRN_SEA_FK     = tran5.TARIFF_TRN_SEA_PK             " +  "     AND    main5.ACTIVE                = 1                                   " +  "     AND    main5.TARIFF_TYPE           = 1                                   " +  "     AND    tran5.PORT_MST_POL_FK       = portpol5.PORT_MST_PK(+)             " +  "     AND    tran5.PORT_MST_POD_FK       = portpod5.PORT_MST_PK(+)             " +  "     AND    main5.OPERATOR_MST_FK       = opr5.OPERATOR_MST_PK(+)             " +  "     AND    cont5.CONTAINER_TYPE_MST_FK = cntr5.CONTAINER_TYPE_MST_PK(+)      " +  "     AND    main5.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "        " +  "     AND    (tran5.PORT_MST_POL_FK, tran5.PORT_MST_POD_FK,                    " +  "                                   cont5.CONTAINER_TYPE_MST_FK )              " +  "              in ( " + Convert.ToString(SectorContainers) + " )                           " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            tran5.VALID_FROM   AND   NVL(tran5.VALID_TO,NULL_DATE_FORMAT))Q       " +  "     ORDER BY Q.PK DESC,Q.PREFERENCES  ";
                }
                else
                {
                    strSQL = "  Select    DISTINCT                                  " +  "     main5.TARIFF_MAIN_SEA_PK                   PK,                  " +  "  " + SRC(SourceType.OperatorTariff) + "        TYPE,                " +  "     main5.TARIFF_REF_NO                        REF_NO,              " +  "     TO_CHAR(tran5.VALID_TO,'" + dateFormat + "')       SHIP_DATE,           " +  "     tran5.PORT_MST_POL_FK                      POL_PK,              " +  "     portpol5.PORT_ID                           POL_ID,              " +  "     tran5.PORT_MST_POD_FK                      POD_PK,              " +  "     portpod5.PORT_ID                           POD_ID,              " +  "     main5.OPERATOR_MST_FK                      OPER_PK,             " +  "     opr5.OPERATOR_ID                           OPER_ID,             " +  "     opr5.OPERATOR_NAME                         OPER_NAME,           " +  "     0                                          COMM_PK,             " +  "     ''                                         COMM_ID,             " +  "     tran5.LCL_BASIS                            LCL_BASIS,           " +  "     NVL(dim5.DIMENTION_ID,'')                  DIMENTION_ID,        " +  "     NULL                                       WEIGHT,              " +  "     0                                          VOLUME,              " +  "     NVL(" + exchQueryLCL + ",0)                ALL_IN_TARIFF,       " +  "     'false'                                    SELECTED,            " +  "     NVL(" + OperatorRate + ",0)                OPERATOR_RATE,       " +  "     " + SourceType.OperatorTariff + "          PRIORITYORDER       " +  "    from                                                             " +  "     TARIFF_MAIN_SEA_TBL            main5,                           " +  "     TARIFF_TRN_SEA_FCL_LCL         tran5,                           " +  "     PORT_MST_TBL                   portpol5,                        " +  "     PORT_MST_TBL                   portpod5,                        " +  "     OPERATOR_MST_TBL               opr5,                            " +  "     DIMENTION_UNIT_MST_TBL         dim5                             " +  "    where                                                                " +  "            main5.TARIFF_MAIN_SEA_PK    = tran5.TARIFF_MAIN_SEA_FK           " +  "     AND    main5.CARGO_TYPE            = 2                                  " +  "     AND    main5.ACTIVE                = 1                                  " +  "     AND    main5.TARIFF_TYPE           = 1                                  " +  "     AND    tran5.PORT_MST_POL_FK       = portpol5.PORT_MST_PK(+)            " +  "     AND    tran5.PORT_MST_POD_FK       = portpod5.PORT_MST_PK(+)            " +  "     AND    main5.OPERATOR_MST_FK       = opr5.OPERATOR_MST_PK(+)            " +  "     AND    main5.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "       " +  "     AND    tran5.LCL_BASIS             = dim5.DIMENTION_UNIT_MST_PK(+)      " +  "     AND    (tran5.PORT_MST_POL_FK, tran5.PORT_MST_POD_FK,tran5.LCL_BASIS )  " +  "              in ( " + Convert.ToString(SectorContainers) + " )                          " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN      " +  "            tran5.VALID_FROM   AND   NVL(tran5.VALID_TO,NULL_DATE_FORMAT)     " +  "     ORDER BY PK DESC ";
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

        private string funGenTariffQuery(bool forFCL = true, string CustNo = "", string SectorContainers = "", string CommodityGroup = "", string ShipDate = "", string CurrPK = "", int Group = 0)
        {
            try
            {
                string strSQL = null;
                string exchQueryFCL = null;
                string exchQueryLCL = null;
                if (Group == 1 | Group == 2)
                {
                    if (forFCL)
                    {
                        exchQueryFCL = " ( SELECT SUM(DISTINCT NVL(FCL_REQ_RATE * EXCHANGE_RATE,0))               " +  "   from  TARIFF_TRN_SEA_FCL_LCL t5, V_EXCHANGE_RATE v5,                   ";
                        //& vbCrLf & _
                        //Modified by Snigdharani - 29/10/2008 - Removing v-array
                        //TABLE(t5.CONTAINER_DTL_FCL) (+) c5
                        exchQueryFCL = exchQueryFCL + " TARIFF_TRN_SEA_CONT_DTL c5,                             " +  "         FREIGHT_ELEMENT_MST_TBL f5                                       " +  "   where t5.TARIFF_MAIN_SEA_FK = main5.TARIFF_MAIN_SEA_PK and v5.exch_rate_type_fk = 1 and             " +  "         c5.CONTAINER_TYPE_MST_FK = cont5.CONTAINER_TYPE_MST_FK  AND      " +  "         c5.TARIFF_TRN_SEA_FK = t5.TARIFF_TRN_SEA_PK  AND                 " +  "         T5.POL_GRP_FK = TRAN5.POL_GRP_FK AND                             " +  "         T5.POD_GRP_FK = TRAN5.POD_GRP_FK AND                             " +  "         t5.CHECK_FOR_ALL_IN_RT   = 1 AND                                 " +  "         t5.CURRENCY_MST_FK =  v5.CURRENCY_MST_FK AND                     " +  "         V5.CURRENCY_MST_BASE_FK= " + CurrPK + "                 AND " +  "         f5.FREIGHT_ELEMENT_MST_PK =  t5.FREIGHT_ELEMENT_MST_FK  AND      " +  "         --f5.CHARGE_BASIS <> 2                                    AND      " +  "         ROUND(sysdate-0.5) between v5.FROM_DATE and v5.TO_DATE )         ";

                    }

                    if (!forFCL)
                    {
                        exchQueryLCL = "( Select Sum(DISTINCT NVL(LCL_TARIFF_RATE * EXCHANGE_RATE,0))            " +  "   from  TARIFF_TRN_SEA_FCL_LCL t5, V_EXCHANGE_RATE v5,                   " +  "         FREIGHT_ELEMENT_MST_TBL f5                                       " +  "   where t5.TARIFF_MAIN_SEA_FK = main5.TARIFF_MAIN_SEA_PK and v5.exch_rate_type_fk = 1 and             " +  "         T5.POL_GRP_FK = TRAN5.POL_GRP_FK AND                             " +  "         T5.POD_GRP_FK = TRAN5.POD_GRP_FK AND                   " +  "         t5.CHECK_FOR_ALL_IN_RT   = 1 AND                                 " +  "         t5.CURRENCY_MST_FK =  v5.CURRENCY_MST_FK AND                     " +  "         V5.CURRENCY_MST_BASE_FK= " + CurrPK + "                 AND " +  "         f5.FREIGHT_ELEMENT_MST_PK =  t5.FREIGHT_ELEMENT_MST_FK  AND      " +  "         --f5.CHARGE_BASIS <> 2                                    AND      " +  "         ROUND(sysdate-0.5) between v5.FROM_DATE and v5.TO_DATE )         ";

                    }

                    if (forFCL)
                    {
                        strSQL = "  SELECT Q.PK," +  "     Q.TYPE, " +  "     Q.REF_NO, " +  "     Q.REFNO, " +  "     Q.SHIP_DATE, " +  "     Q.POL_PK, " +  "     Q.POL_ID, " +  "     Q.POD_PK, " +  "     Q.POD_ID, " +  "     Q.OPER_PK, " +  "     Q.OPER_ID, " +  "     Q.OPER_NAME, " +  "     Q.CNTR_PK, " +  "     Q.CNTR_ID, " +  "     Q.QUANTITY, " +  "     Q.COMM_PK, " +  "     Q.COMM_ID, " +  "     Q.ALL_IN_TARIFF, " +  "     Q.OPERATOR_RATE, " +  "     Q.TARIFF, " +  "     Q.NET, " +  "     Q.SELECTED, " +  "     Q.PRIORITYORDER  " +  "   FROM (Select  DISTINCT                             " +  "     main5.TARIFF_MAIN_SEA_PK                   PK,                  " +  "  " + SRC(SourceType.GeneralTariff) + "        TYPE,                " +  "     main5.TARIFF_REF_NO                        REF_NO,              " +  "     main5.TARIFF_REF_NO                        REFNO,               " +  "     TO_CHAR(tran5.VALID_TO,'" + dateFormat + "')       SHIP_DATE,           " +  "     TRAN5.POL_GRP_FK                           POL_PK,              " +  "     PGL.PORT_GRP_ID                            POL_ID,              " +  "     TRAN5.POD_GRP_FK                           POD_PK,              " +  "     PGD.PORT_GRP_ID                            POD_ID,              " +  "     main5.OPERATOR_MST_FK                      OPER_PK,             " +  "     opr5.OPERATOR_ID                           OPER_ID,             " +  "     opr5.OPERATOR_NAME                         OPER_NAME,           " +  "     cont5.CONTAINER_TYPE_MST_FK                CNTR_PK,             " +  "     cntr5.CONTAINER_TYPE_MST_ID                CNTR_ID,             " +  "     NULL                                       QUANTITY,            " +  "     0                                          COMM_PK,             " +  "     ''                                         COMM_ID,             " +  "     NVL(" + exchQueryFCL + ",0)                ALL_IN_TARIFF,       " +  "     NULL                                       OPERATOR_RATE,       " +  "     NULL                                       TARIFF,              " +  "     NULL                                       NET,                 " +  "     'false'                                    SELECTED,            " +  "   " + SourceType.GeneralTariff + "             PRIORITYORDER,        " +  "   CNTR5.PREFERENCES                                               " +  "    from                                                             " +  "     TARIFF_MAIN_SEA_TBL            main5,                           " +  "     TARIFF_TRN_SEA_FCL_LCL         tran5,                           " +  "     PORT_GRP_MST_TBL               PGL,                             " +  "     PORT_GRP_MST_TBL               PGD,                             " +  "     OPERATOR_MST_TBL               opr5,                            ";
                        //& vbCrLf _
                        //Modified by Snigdharani -29/10/2008 - Removin v-array
                        //TABLE(tran5.CONTAINER_DTL_FCL) (+) cont5,
                        strSQL = strSQL + "TARIFF_TRN_SEA_CONT_DTL cont5,                            " +  "     CONTAINER_TYPE_MST_TBL         cntr5                            " +  "    where                                                                " +  "            main5.TARIFF_MAIN_SEA_PK    = tran5.TARIFF_MAIN_SEA_FK            " +  "     AND    main5.CARGO_TYPE            = 1                                   " +  "     AND    main5.ACTIVE                = 1                                   " +  "     AND    main5.TARIFF_TYPE           = 2                                   " +  "     AND    cont5.TARIFF_TRN_SEA_FK = tran5.TARIFF_TRN_SEA_PK                 " +  "     AND    TRAN5.POL_GRP_FK            = PGL.PORT_GRP_MST_PK(+)              " +  "     AND    TRAN5.POD_GRP_FK            = PGD.PORT_GRP_MST_PK(+)              " +  "     AND    main5.OPERATOR_MST_FK       = opr5.OPERATOR_MST_PK(+)             " +  "     AND    cont5.CONTAINER_TYPE_MST_FK = cntr5.CONTAINER_TYPE_MST_PK(+)      " +  "     AND    main5.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "        " +  "     AND    (TRAN5.POL_GRP_FK, TRAN5.POD_GRP_FK,                              " +  "                                   cont5.CONTAINER_TYPE_MST_FK )              " +  "              in ( " + Convert.ToString(SectorContainers) + " )                           " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            tran5.VALID_FROM   AND   NVL(tran5.VALID_TO,NULL_DATE_FORMAT))Q        " +  "     ORDER BY Q.PK DESC,Q.PREFERENCES";

                    }
                    else
                    {
                        strSQL = "Select    DISTINCT                                  " +  "     main5.TARIFF_MAIN_SEA_PK                   PK,                  " +  "  " + SRC(SourceType.GeneralTariff) + "        TYPE,                " +  "     main5.TARIFF_REF_NO                        REF_NO,              " +  "     TO_CHAR(tran5.VALID_TO,'" + dateFormat + "')       SHIP_DATE,           " +  "     TRAN5.POL_GRP_FK                           POL_PK,              " +  "     PGL.PORT_GRP_ID                            POL_ID,              " +  "     TRAN5.POD_GRP_FK                           POD_PK,              " +  "     PGD.PORT_GRP_ID                            POD_ID,              " +  "     main5.OPERATOR_MST_FK                      OPER_PK,             " +  "     opr5.OPERATOR_ID                           OPER_ID,             " +  "     opr5.OPERATOR_NAME                         OPER_NAME,           " +  "     0                                          COMM_PK,             " +  "     ''                                         COMM_ID,             " +  "     tran5.LCL_BASIS                            LCL_BASIS,           " +  "     NVL(dim5.DIMENTION_ID,'')                  DIMENTION_ID,        " +  "     NULL                                       WEIGHT,              " +  "     0                                          VOLUME,              " +  "     NVL(" + exchQueryLCL + ",0)                ALL_IN_TARIFF,       " +  "     'false'                                    SELECTED,            " +  "     NULL                                       OPERATOR_RATE,       " +  "     " + SourceType.GeneralTariff + "           PRIORITYORDER        " +  "    from                                                             " +  "     TARIFF_MAIN_SEA_TBL            main5,                           " +  "     TARIFF_TRN_SEA_FCL_LCL         tran5,                           " +  "     PORT_GRP_MST_TBL               PGL,                             " +  "     PORT_GRP_MST_TBL               PGD,                             " +  "     OPERATOR_MST_TBL               opr5,                            " +  "     DIMENTION_UNIT_MST_TBL         dim5                             " +  "    where                                                                " +  "            main5.TARIFF_MAIN_SEA_PK    = tran5.TARIFF_MAIN_SEA_FK           " +  "     AND    main5.CARGO_TYPE            = 2                                  " +  "     AND    main5.ACTIVE                = 1                                  " +  "     AND    main5.TARIFF_TYPE           = 2                                  " +  "     AND    TRAN5.POL_GRP_FK            = PGL.PORT_GRP_MST_PK(+)              " +  "     AND    TRAN5.POD_GRP_FK            = PGD.PORT_GRP_MST_PK(+)              " +  "     AND    main5.OPERATOR_MST_FK       = opr5.OPERATOR_MST_PK(+)            " +  "     AND    main5.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "       " +  "     AND    tran5.LCL_BASIS             = dim5.DIMENTION_UNIT_MST_PK(+)      " +  "     AND    (TRAN5.POL_GRP_FK, TRAN5.POD_GRP_FK, tran5.LCL_BASIS )  " +  "              in ( " + Convert.ToString(SectorContainers) + " )                          " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN      " +  "            tran5.VALID_FROM   AND   NVL(tran5.VALID_TO,NULL_DATE_FORMAT)      " +  "     ORDER BY PK DESC ";
                    }

                }
                else
                {
                    if (forFCL)
                    {
                        exchQueryFCL = " ( Select nvl(Sum(NVL(FCL_REQ_RATE * EXCHANGE_RATE,0)),0)               " +  "   from  TARIFF_TRN_SEA_FCL_LCL t5, V_EXCHANGE_RATE v5,                   ";
                        //& vbCrLf & _
                        //Modified by Snigdharani - 29/10/2008 - Removing v-array
                        //TABLE(t5.CONTAINER_DTL_FCL) (+) c5
                        exchQueryFCL = exchQueryFCL + " TARIFF_TRN_SEA_CONT_DTL c5,                             " +  "         FREIGHT_ELEMENT_MST_TBL f5                                       " +  "   where t5.TARIFF_MAIN_SEA_FK = main5.TARIFF_MAIN_SEA_PK and v5.exch_rate_type_fk = 1 and             " +  "         c5.CONTAINER_TYPE_MST_FK = cont5.CONTAINER_TYPE_MST_FK  AND      " +  "         c5.TARIFF_TRN_SEA_FK = t5.TARIFF_TRN_SEA_PK  AND                 " +  "         t5.PORT_MST_POL_FK = tran5.PORT_MST_POL_FK AND                   " +  "         t5.PORT_MST_POD_FK = tran5.PORT_MST_POD_FK AND                   " +  "         t5.CHECK_FOR_ALL_IN_RT   = 1 AND                                 " +  "         t5.CURRENCY_MST_FK =  v5.CURRENCY_MST_FK AND                     " +  "         V5.CURRENCY_MST_BASE_FK= " + CurrPK + "                 AND " +  "         f5.FREIGHT_ELEMENT_MST_PK =  t5.FREIGHT_ELEMENT_MST_FK  AND      " +  "         --f5.CHARGE_BASIS <> 2                                    AND      " +  "         ROUND(sysdate-0.5) between v5.FROM_DATE and v5.TO_DATE )         ";

                    }

                    if (!forFCL)
                    {
                        exchQueryLCL = "( Select nvl(Sum(NVL(LCL_TARIFF_RATE * EXCHANGE_RATE,0)),0)             " +  "   from  TARIFF_TRN_SEA_FCL_LCL t5, V_EXCHANGE_RATE v5,                   " +  "         FREIGHT_ELEMENT_MST_TBL f5                                       " +  "   where t5.TARIFF_MAIN_SEA_FK = main5.TARIFF_MAIN_SEA_PK and v5.exch_rate_type_fk = 1 and             " +  "         t5.PORT_MST_POL_FK = tran5.PORT_MST_POL_FK AND                   " +  "         t5.PORT_MST_POD_FK = tran5.PORT_MST_POD_FK AND                   " +  "         t5.CHECK_FOR_ALL_IN_RT   = 1 AND                                 " +  "         t5.CURRENCY_MST_FK =  v5.CURRENCY_MST_FK AND                     " +  "         V5.CURRENCY_MST_BASE_FK= " + CurrPK + "                 AND " +  "         f5.FREIGHT_ELEMENT_MST_PK =  t5.FREIGHT_ELEMENT_MST_FK  AND      " +  "         --f5.CHARGE_BASIS <> 2                                    AND      " +  "         ROUND(sysdate-0.5) between v5.FROM_DATE and v5.TO_DATE )         ";

                    }

                    if (forFCL)
                    {
                        strSQL = "  SELECT Q.PK," +  "     Q.TYPE, " +  "     Q.REF_NO, " +  "     Q.REFNO, " +  "     Q.SHIP_DATE, " +  "     Q.POL_PK, " +  "     Q.POL_ID, " +  "     Q.POD_PK, " +  "     Q.POD_ID, " +  "     Q.OPER_PK, " +  "     Q.OPER_ID, " +  "     Q.OPER_NAME, " +  "     Q.CNTR_PK, " +  "     Q.CNTR_ID, " +  "     Q.QUANTITY, " +  "     Q.COMM_PK, " +  "     Q.COMM_ID, " +  "     Q.ALL_IN_TARIFF, " +  "     Q.OPERATOR_RATE, " +  "     Q.TARIFF, " +  "     Q.NET, " +  "     Q.SELECTED, " +  "     Q.PRIORITYORDER  " +  " FROM(Select  DISTINCT                             " +  "     main5.TARIFF_MAIN_SEA_PK                   PK,                  " +  "  " + SRC(SourceType.GeneralTariff) + "        TYPE,                " +  "     main5.TARIFF_REF_NO                        REF_NO,              " +  "     main5.TARIFF_REF_NO                        REFNO,               " +  "     TO_CHAR(tran5.VALID_TO,'" + dateFormat + "')       SHIP_DATE,           " +  "     tran5.PORT_MST_POL_FK                      POL_PK,              " +  "     portpol5.PORT_ID                           POL_ID,              " +  "     tran5.PORT_MST_POD_FK                      POD_PK,              " +  "     portpod5.PORT_ID                           POD_ID,              " +  "     main5.OPERATOR_MST_FK                      OPER_PK,             " +  "     opr5.OPERATOR_ID                           OPER_ID,             " +  "     opr5.OPERATOR_NAME                         OPER_NAME,           " +  "     cont5.CONTAINER_TYPE_MST_FK                CNTR_PK,             " +  "     cntr5.CONTAINER_TYPE_MST_ID                CNTR_ID,             " +  "     NULL                                       QUANTITY,            " +  "     0                                          COMM_PK,             " +  "     ''                                         COMM_ID,             " +  "     NVL(" + exchQueryFCL + ",0)                ALL_IN_TARIFF,       " +  "     NULL                                       OPERATOR_RATE,       " +  "     NULL                                       TARIFF,              " +  "     NULL                                       NET,                 " +  "     'false'                                    SELECTED,            " +  "   " + SourceType.GeneralTariff + "             PRIORITYORDER,        " +  "   CNTR5.PREFERENCES                                               " +  "    from                                                             " +  "     TARIFF_MAIN_SEA_TBL            main5,                           " +  "     TARIFF_TRN_SEA_FCL_LCL         tran5,                           " +  "     PORT_MST_TBL                   portpol5,                        " +  "     PORT_MST_TBL                   portpod5,                        " +  "     OPERATOR_MST_TBL               opr5,                            ";
                        //& vbCrLf _
                        //Modified by Snigdharani -29/10/2008 - Removin v-array
                        //TABLE(tran5.CONTAINER_DTL_FCL) (+) cont5,
                        strSQL = strSQL + "TARIFF_TRN_SEA_CONT_DTL cont5,                            " +  "     CONTAINER_TYPE_MST_TBL         cntr5                            " +  "    where                                                                " +  "            main5.TARIFF_MAIN_SEA_PK    = tran5.TARIFF_MAIN_SEA_FK            " +  "     AND    main5.CARGO_TYPE            = 1                                   " +  "     AND    main5.ACTIVE                = 1                                   " +  "     AND    main5.TARIFF_TYPE           = 2                                   " +  "     AND    cont5.TARIFF_TRN_SEA_FK = tran5.TARIFF_TRN_SEA_PK                 " +  "     AND    tran5.PORT_MST_POL_FK       = portpol5.PORT_MST_PK(+)             " +  "     AND    tran5.PORT_MST_POD_FK       = portpod5.PORT_MST_PK(+)             " +  "     AND    main5.OPERATOR_MST_FK       = opr5.OPERATOR_MST_PK(+)             " +  "     AND    cont5.CONTAINER_TYPE_MST_FK = cntr5.CONTAINER_TYPE_MST_PK(+)      " +  "     AND    main5.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "        " +  "     AND    (tran5.PORT_MST_POL_FK, tran5.PORT_MST_POD_FK,                    " +  "                                   cont5.CONTAINER_TYPE_MST_FK )              " +  "              in ( " + Convert.ToString(SectorContainers) + " )                           " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            tran5.VALID_FROM   AND   NVL(tran5.VALID_TO,NULL_DATE_FORMAT))Q        " +  "     ORDER BY PK DESC,Q.PREFERENCES ";
                    }
                    else
                    {
                        strSQL = "    Select    DISTINCT                                  " +  "     main5.TARIFF_MAIN_SEA_PK                   PK,                  " +  "  " + SRC(SourceType.GeneralTariff) + "        TYPE,                " +  "     main5.TARIFF_REF_NO                        REF_NO,              " +  "     TO_CHAR(tran5.VALID_TO,'" + dateFormat + "')       SHIP_DATE,           " +  "     tran5.PORT_MST_POL_FK                      POL_PK,              " +  "     portpol5.PORT_ID                           POL_ID,              " +  "     tran5.PORT_MST_POD_FK                      POD_PK,              " +  "     portpod5.PORT_ID                           POD_ID,              " +  "     main5.OPERATOR_MST_FK                      OPER_PK,             " +  "     opr5.OPERATOR_ID                           OPER_ID,             " +  "     opr5.OPERATOR_NAME                         OPER_NAME,           " +  "     0                                          COMM_PK,             " +  "     ''                                         COMM_ID,             " +  "     tran5.LCL_BASIS                            LCL_BASIS,           " +  "     NVL(dim5.DIMENTION_ID,'')                  DIMENTION_ID,        " +  "     NULL                                       WEIGHT,              " +  "     0                                          VOLUME,              " +  "     NVL(" + exchQueryLCL + ",0)                ALL_IN_TARIFF,       " +  "     'false'                                    SELECTED,            " +  "     NULL                                       OPERATOR_RATE,       " +  "     " + SourceType.GeneralTariff + "           PRIORITYORDER        " +  "    from                                                             " +  "     TARIFF_MAIN_SEA_TBL            main5,                           " +  "     TARIFF_TRN_SEA_FCL_LCL         tran5,                           " +  "     PORT_MST_TBL                   portpol5,                        " +  "     PORT_MST_TBL                   portpod5,                        " +  "     OPERATOR_MST_TBL               opr5,                            " +  "     DIMENTION_UNIT_MST_TBL         dim5                             " +  "    where                                                                " +  "            main5.TARIFF_MAIN_SEA_PK    = tran5.TARIFF_MAIN_SEA_FK           " +  "     AND    main5.CARGO_TYPE            = 2                                  " +  "     AND    main5.ACTIVE                = 1                                  " +  "     AND    main5.TARIFF_TYPE           = 2                                  " +  "     AND    tran5.PORT_MST_POL_FK       = portpol5.PORT_MST_PK(+)            " +  "     AND    tran5.PORT_MST_POD_FK       = portpod5.PORT_MST_PK(+)            " +  "     AND    main5.OPERATOR_MST_FK       = opr5.OPERATOR_MST_PK(+)            " +  "     AND    main5.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "       " +  "     AND    tran5.LCL_BASIS             = dim5.DIMENTION_UNIT_MST_PK(+)      " +  "     AND    (tran5.PORT_MST_POL_FK, tran5.PORT_MST_POD_FK,tran5.LCL_BASIS )  " +  "              in ( " + Convert.ToString(SectorContainers) + " )                          " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN      " +  "            tran5.VALID_FROM   AND   NVL(tran5.VALID_TO,NULL_DATE_FORMAT)      " +  "     ORDER BY PK DESC ";
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

        #endregion

        #region " Freight Level Query "

        #region " Quote Query "

        private string QuoteFreightQuery(bool forFCL = true, string CustNo = "", string SectorContainers = "", string CommodityGroup = "", string ShipDate = "", int Group = 0)
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
                if (Group == 1 | Group == 2)
                {
                    if (forFCL)
                    {
                        strSQL = "    SELECT Q.* FROM (Select            " +  "     main3.QUOTATION_REF_NO                     REF_NO,              " +  "     TRAN3.POL_GRP_FK                           POL_PK,              " +  "     TRAN3.POD_GRP_FK                           POD_PK,              " +  "     tran3.CONTAINER_TYPE_MST_FK                CNTR_PK,             " +  "     frtd3.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt3.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt3.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frt3.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, " +  "     DECODE(frtd3.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     DECODE(frtd3.CHECK_ADVATOS, 1,'true','false') ADVATOS,          " +  "     frtd3.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr3.CURRENCY_ID                          CURR_ID,             " +  "     frtd3.QUOTED_RATE                          QUOTERATE,           " +  "     frtd3.QUOTED_RATE                          FINAL_RATE,          " +  "     frtd3.PYMT_TYPE                            PYTYPE   " +  "    from                                                             " +  "     QUOTATION_MST_TBL              main3,                           " +  "     QUOTATION_DTL_TBL      tran3,                           " +  "     QUOTATION_TRN_SEA_FRT_DTLS     frtd3,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt3,                            " +  "     CURRENCY_TYPE_MST_TBL          curr3                            " +  "    where                                                                " +  "            main3.QUOTATION_MST_PK      = tran3.QUOTATION_MST_FK               " +  "     AND    tran3.QUOTE_TRN_SEA_PK      = frtd3.QUOTE_TRN_SEA_FK               " +  "     AND    frtd3.FREIGHT_ELEMENT_MST_FK = frt3.FREIGHT_ELEMENT_MST_PK(+)      " +  "     --AND    frt3.CHARGE_BASIS           <> 2                                   " +  "     AND    frtd3.CURRENCY_MST_FK       = curr3.CURRENCY_MST_PK(+)             " +  "     AND    main3.CARGO_TYPE            in ( 1 ,3 )                            " +  "     AND    tran3.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "         " +  "     AND    main3.CUSTOMER_MST_FK       = " + Convert.ToString(CustNo) + "                 " +  "     AND    (TRAN3.POL_GRP_FK, TRAN3.POD_GRP_FK ,                     " +  "                                   tran3.CONTAINER_TYPE_MST_FK )               " +  "              in ( " + Convert.ToString(SectorContainers) + " )                            " +  cargos +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5)  BETWEEN       " +  "            main3.QUOTATION_DATE AND (main3.QUOTATION_DATE + main3.VALID_FOR)) Q, " +  "            FREIGHT_ELEMENT_MST_TBL FRT " +  "            WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FRT_PK " +  "            ORDER BY FRT.PREFERENCE ";
                        //Added by Rabbani raeson USS Gap,introduced New column as "QUOTED_MIN_RATE"
                    }
                    else
                    {
                        //    0       1       2        3        4      5         6        7         8       9       10
                        // REF_NO, POL_PK, POD_PK, LCLBASIS, FRT_PK, FRT_ID, FRT_NAME, SELECTED, CURR_PK, CURR_ID, RATE
                        //Added by rabbani on 24/3/07 ,To display BOF as first element in Freight Element Column using UNION ALL.
                        strSQL = "    SELECT Q.* FROM (Select            " +  "     main3.QUOTATION_REF_NO                     REF_NO,              " +  "     TRAN3.POL_GRP_FK                           POL_PK,              " +  "     TRAN3.POD_GRP_FK                           POD_PK,              " +  "     tran3.BASIS                                LCLBASIS,            " +  "     frtd3.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt3.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt3.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frt3.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, " +  "     DECODE(frtd3.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     DECODE(frtd3.CHECK_ADVATOS, 1,'true','false') ADVATOS,          " +  "     frtd3.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr3.CURRENCY_ID                          CURR_ID,             " +  "     frtd3.QUOTED_MIN_RATE                      QUOTED_MIN_RATE,     " +  "     frtd3.QUOTED_RATE                          QUOTERATE  ,               " +  "     (CASE WHEN frtd3.QUOTED_RATE > frtd3.QUOTED_MIN_RATE THEN frtd3.QUOTED_RATE ELSE frtd3.QUOTED_MIN_RATE END)  FINAL_RATE,frt3.Credit  " +  "    from                                                             " +  "     QUOTATION_MST_TBL              main3,                           " +  "     QUOTATION_DTL_TBL      tran3,                           " +  "     QUOTATION_TRN_SEA_FRT_DTLS     frtd3,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt3,                            " +  "     CURRENCY_TYPE_MST_TBL          curr3                            " +  "    where                                                                " +  "            main3.QUOTATION_MST_PK      = tran3.QUOTATION_MST_FK               " +  "     AND    tran3.QUOTE_TRN_SEA_PK      = frtd3.QUOTE_TRN_SEA_FK               " +  "     AND    frtd3.FREIGHT_ELEMENT_MST_FK = frt3.FREIGHT_ELEMENT_MST_PK(+)      " +  "     --AND    frt3.CHARGE_BASIS           <> 2                                  " +  "     AND    frtd3.CURRENCY_MST_FK       = curr3.CURRENCY_MST_PK(+)             " +  "     AND    main3.CARGO_TYPE           in ( 2 , 3)                              " +  "     AND    tran3.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "         " +  "     AND    main3.CUSTOMER_MST_FK       = " + Convert.ToString(CustNo) + "                 " +  "     AND    (TRAN3.POL_GRP_FK, TRAN3.POD_GRP_FK ,                              " +  "                                   tran3.BASIS )                               " +  "              in ( " + Convert.ToString(SectorContainers) + " )                            " +  cargos +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5)  BETWEEN       " +  "            main3.QUOTATION_DATE AND (main3.QUOTATION_DATE + main3.VALID_FOR)  " +  "     AND    frt3.FREIGHT_ELEMENT_MST_PK IN (SELECT PARM.FRT_BOF_FK  FROM PARAMETERS_TBL PARM) " +  "     UNION ALL " +  "     Select            " +  "     main3.QUOTATION_REF_NO                     REF_NO,              " +  "     TRAN3.POL_GRP_FK                           POL_PK,              " +  "     TRAN3.POD_GRP_FK                           POD_PK,              " +  "     tran3.BASIS                                LCLBASIS,            " +  "     frtd3.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt3.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt3.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frt3.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, " +  "     DECODE(frtd3.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     DECODE(frtd3.CHECK_ADVATOS, 1,'true','false') ADVATOS, " +  "     frtd3.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr3.CURRENCY_ID                          CURR_ID,             " +  "     frtd3.QUOTED_MIN_RATE                      QUOTED_MIN_RATE,     " +  "     frtd3.QUOTED_RATE                          QUOTERATE,                 " +  "     (CASE WHEN frtd3.QUOTED_RATE > frtd3.QUOTED_MIN_RATE THEN frtd3.QUOTED_RATE ELSE frtd3.QUOTED_MIN_RATE END)  FINAL_RATE,frt3.Credit " +  "     from                                                             " +  "     QUOTATION_MST_TBL              main3,                           " +  "     QUOTATION_DTL_TBL      tran3,                           " +  "     QUOTATION_TRN_SEA_FRT_DTLS     frtd3,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt3,                            " +  "     CURRENCY_TYPE_MST_TBL          curr3                            " +  "    where                                                                " +  "            main3.QUOTATION_MST_PK      = tran3.QUOTATION_MST_FK               " +  "     AND    tran3.QUOTE_TRN_SEA_PK      = frtd3.QUOTE_TRN_SEA_FK               " +  "     AND    frtd3.FREIGHT_ELEMENT_MST_FK = frt3.FREIGHT_ELEMENT_MST_PK(+)      " +  "     --AND    frt3.CHARGE_BASIS           <> 2                                  " +  "     AND    frtd3.CURRENCY_MST_FK       = curr3.CURRENCY_MST_PK(+)             " +  "     AND    main3.CARGO_TYPE            in(2,3)                                " +  "     AND    tran3.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "         " +  "     AND    main3.CUSTOMER_MST_FK       = " + Convert.ToString(CustNo) + "                 " +  cargos +  "     AND    (TRAN3.POL_GRP_FK, TRAN3.POD_GRP_FK ,                              " +  "                                   tran3.BASIS )                               " +  "              in ( " + Convert.ToString(SectorContainers) + " )                            " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5)  BETWEEN       " +  "            main3.QUOTATION_DATE AND (main3.QUOTATION_DATE + main3.VALID_FOR)  " +  "     AND    frt3.FREIGHT_ELEMENT_MST_PK NOT IN (SELECT PARM.FRT_BOF_FK  FROM PARAMETERS_TBL PARM)) Q, " +  "            FREIGHT_ELEMENT_MST_TBL FRT " +  "            WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FRT_PK " +  "            ORDER BY FRT.PREFERENCE ";
                    }
                }
                else
                {
                    if (forFCL)
                    {
                        strSQL = "    SELECT Q.* FROM (Select            " +  "     main3.QUOTATION_REF_NO                     REF_NO,              " +  "     tran3.PORT_MST_POL_FK                      POL_PK,              " +  "     tran3.PORT_MST_POD_FK                      POD_PK,              " +  "     tran3.CONTAINER_TYPE_MST_FK                CNTR_PK,             " +  "     frtd3.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt3.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt3.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frt3.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, " +  "     DECODE(frtd3.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     DECODE(frtd3.CHECK_ADVATOS, 1,'true','false') ADVATOS,          " +  "     frtd3.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr3.CURRENCY_ID                          CURR_ID,             " +  "     frtd3.QUOTED_RATE                          QUOTERATE,           " +  "     frtd3.QUOTED_RATE                          FINAL_RATE,          " +  "     frtd3.PYMT_TYPE                            PYTYPE   " +  "    from                                                             " +  "     QUOTATION_MST_TBL              main3,                           " +  "     QUOTATION_DTL_TBL      tran3,                           " +  "     QUOTATION_TRN_SEA_FRT_DTLS     frtd3,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt3,                            " +  "     CURRENCY_TYPE_MST_TBL          curr3                            " +  "    where                                                                " +  "            main3.QUOTATION_MST_PK      = tran3.QUOTATION_MST_FK               " +  "     AND    tran3.QUOTE_TRN_SEA_PK      = frtd3.QUOTE_TRN_SEA_FK               " +  "     AND    frtd3.FREIGHT_ELEMENT_MST_FK = frt3.FREIGHT_ELEMENT_MST_PK(+)      " +  "     --AND    frt3.CHARGE_BASIS           <> 2                                   " +  "     AND    frtd3.CURRENCY_MST_FK       = curr3.CURRENCY_MST_PK(+)             " +  "     AND    main3.CARGO_TYPE            in ( 1 ,3 )                            " +  "     AND    tran3.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "         " +  "     AND    main3.CUSTOMER_MST_FK       = " + Convert.ToString(CustNo) + "                 " +  "     AND    (tran3.PORT_MST_POL_FK, tran3.PORT_MST_POD_FK,                     " +  "                                   tran3.CONTAINER_TYPE_MST_FK )               " +  "              in ( " + Convert.ToString(SectorContainers) + " )                            " +  cargos +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5)  BETWEEN       " +  "            main3.QUOTATION_DATE AND (main3.QUOTATION_DATE + main3.VALID_FOR)) Q, " +  "            FREIGHT_ELEMENT_MST_TBL FRT " +  "            WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FRT_PK " +  "            ORDER BY FRT.PREFERENCE ";
                        //Added by Rabbani raeson USS Gap,introduced New column as "QUOTED_MIN_RATE"
                    }
                    else
                    {
                        //    0       1       2        3        4      5         6        7         8       9       10
                        // REF_NO, POL_PK, POD_PK, LCLBASIS, FRT_PK, FRT_ID, FRT_NAME, SELECTED, CURR_PK, CURR_ID, RATE
                        //Added by rabbani on 24/3/07 ,To display BOF as first element in Freight Element Column using UNION ALL.
                        strSQL = "    SELECT Q.* FROM (Select            " +  "     main3.QUOTATION_REF_NO                     REF_NO,              " +  "     tran3.PORT_MST_POL_FK                      POL_PK,              " +  "     tran3.PORT_MST_POD_FK                      POD_PK,              " +  "     tran3.BASIS                                LCLBASIS,            " +  "     frtd3.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt3.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt3.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frt3.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, " +  "     DECODE(frtd3.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     DECODE(frtd3.CHECK_ADVATOS, 1,'true','false') ADVATOS,          " +  "     frtd3.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr3.CURRENCY_ID                          CURR_ID,             " +  "     frtd3.QUOTED_MIN_RATE                      QUOTED_MIN_RATE,     " +  "     frtd3.QUOTED_RATE                          QUOTERATE  ,               " +  "     (CASE WHEN frtd3.QUOTED_RATE > frtd3.QUOTED_MIN_RATE THEN frtd3.QUOTED_RATE ELSE frtd3.QUOTED_MIN_RATE END)  FINAL_RATE,frt3.Credit  " +  "    from                                                             " +  "     QUOTATION_MST_TBL              main3,                           " +  "     QUOTATION_DTL_TBL      tran3,                           " +  "     QUOTATION_TRN_SEA_FRT_DTLS     frtd3,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt3,                            " +  "     CURRENCY_TYPE_MST_TBL          curr3                            " +  "    where                                                                " +  "            main3.QUOTATION_MST_PK      = tran3.QUOTATION_MST_FK               " +  "     AND    tran3.QUOTE_TRN_SEA_PK      = frtd3.QUOTE_TRN_SEA_FK               " +  "     AND    frtd3.FREIGHT_ELEMENT_MST_FK = frt3.FREIGHT_ELEMENT_MST_PK(+)      " +  "     --AND    frt3.CHARGE_BASIS           <> 2                                  " +  "     AND    frtd3.CURRENCY_MST_FK       = curr3.CURRENCY_MST_PK(+)             " +  "     AND    main3.CARGO_TYPE           in ( 2 , 3)                              " +  "     AND    tran3.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "         " +  "     AND    main3.CUSTOMER_MST_FK       = " + Convert.ToString(CustNo) + "                 " +  "     AND    (tran3.PORT_MST_POL_FK, tran3.PORT_MST_POD_FK,                     " +  "                                   tran3.BASIS )                               " +  "              in ( " + Convert.ToString(SectorContainers) + " )                            " +  cargos +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5)  BETWEEN       " +  "            main3.QUOTATION_DATE AND (main3.QUOTATION_DATE + main3.VALID_FOR)  " +  "     AND    frt3.FREIGHT_ELEMENT_MST_PK IN (SELECT PARM.FRT_BOF_FK  FROM PARAMETERS_TBL PARM) " +  "     UNION ALL " +  "     Select            " +  "     main3.QUOTATION_REF_NO                     REF_NO,              " +  "     tran3.PORT_MST_POL_FK                      POL_PK,              " +  "     tran3.PORT_MST_POD_FK                      POD_PK,              " +  "     tran3.BASIS                                LCLBASIS,            " +  "     frtd3.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt3.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt3.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frt3.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, " +  "     DECODE(frtd3.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     DECODE(frtd3.CHECK_ADVATOS, 1,'true','false') ADVATOS, " +  "     frtd3.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr3.CURRENCY_ID                          CURR_ID,             " +  "     frtd3.QUOTED_MIN_RATE                      QUOTED_MIN_RATE,     " +  "     frtd3.QUOTED_RATE                          QUOTERATE,                 " +  "     (CASE WHEN frtd3.QUOTED_RATE > frtd3.QUOTED_MIN_RATE THEN frtd3.QUOTED_RATE ELSE frtd3.QUOTED_MIN_RATE END)  FINAL_RATE,frt3.Credit " +  "     from                                                             " +  "     QUOTATION_MST_TBL              main3,                           " +  "     QUOTATION_DTL_TBL      tran3,                           " +  "     QUOTATION_TRN_SEA_FRT_DTLS     frtd3,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt3,                            " +  "     CURRENCY_TYPE_MST_TBL          curr3                            " +  "    where                                                                " +  "            main3.QUOTATION_MST_PK      = tran3.QUOTATION_MST_FK               " +  "     AND    tran3.QUOTE_TRN_SEA_PK      = frtd3.QUOTE_TRN_SEA_FK               " +  "     AND    frtd3.FREIGHT_ELEMENT_MST_FK = frt3.FREIGHT_ELEMENT_MST_PK(+)      " +  "     --AND    frt3.CHARGE_BASIS           <> 2                                  " +  "     AND    frtd3.CURRENCY_MST_FK       = curr3.CURRENCY_MST_PK(+)             " +  "     AND    main3.CARGO_TYPE            in(2,3)                                " +  "     AND    tran3.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "         " +  "     AND    main3.CUSTOMER_MST_FK       = " + Convert.ToString(CustNo) + "                 " +  cargos +  "     AND    (tran3.PORT_MST_POL_FK, tran3.PORT_MST_POD_FK,                     " +  "                                   tran3.BASIS )                               " +  "              in ( " + Convert.ToString(SectorContainers) + " )                            " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5)  BETWEEN       " +  "            main3.QUOTATION_DATE AND (main3.QUOTATION_DATE + main3.VALID_FOR)  " +  "     AND    frt3.FREIGHT_ELEMENT_MST_PK NOT IN (SELECT PARM.FRT_BOF_FK  FROM PARAMETERS_TBL PARM)) Q, " +  "            FREIGHT_ELEMENT_MST_TBL FRT " +  "            WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FRT_PK " +  "            ORDER BY FRT.PREFERENCE ";
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

        #region " Enquiry Query "

        private string EnquiryFreightQuery(bool forFCL = true, string CustNo = "", string SectorContainers = "", string CommodityGroup = "", string QuoteDate = "")
        {
            try
            {
                string strSQL = null;
                if (forFCL)
                {
                    strSQL = "    Select            " +  "     main4.ENQUIRY_REF_NO                       REF_NO,              " +  "     tran4.PORT_MST_POL_FK                      POL_PK,              " +  "     tran4.PORT_MST_POD_FK                      POD_PK,              " +  "     tran4.CONTAINER_TYPE_MST_FK                CNTR_PK,             " +  "     frtd4.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt4.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt4.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frtd4.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     'false'  ADVATOS,                                               " +  "     frtd4.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr4.CURRENCY_ID                          CURR_ID,             " +  "     frtd4.TARIFF_RATE                          RATE,                " +  "     NULL                                       QUOTERATE,           " +  "     NULL                                       PYTYPE               " +  "    from                                                             " +  "     ENQUIRY_BKG_SEA_TBL            main4,                           " +  "     ENQUIRY_TRN_SEA_FCL_LCL        tran4,                           " +  "     ENQUIRY_TRN_SEA_FRT_DTLS       frtd4,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt4,                            " +  "     CURRENCY_TYPE_MST_TBL          curr4                            " +  "    where                                                                " +  "            main4.ENQUIRY_BKG_SEA_PK    = tran4.ENQUIRY_MAIN_SEA_FK            " +  "     AND    tran4.ENQUIRY_TRN_SEA_PK    = frtd4.ENQUIRY_TRN_SEA_FK             " +  "     AND    frtd4.FREIGHT_ELEMENT_MST_FK = frt4.FREIGHT_ELEMENT_MST_PK(+)      " +  "     --AND    frt4.CHARGE_BASIS           <> 2                                   " +  "     AND    frtd4.CURRENCY_MST_FK       = curr4.CURRENCY_MST_PK(+)             " +  "     AND    main4.CARGO_TYPE            = 1                                    " +  "     AND    tran4.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "         " +  "     AND    main4.CUSTOMER_MST_FK       = " + Convert.ToString(CustNo) + "                 " +  "     AND    (tran4.PORT_MST_POL_FK, tran4.PORT_MST_POD_FK,                     " +  "                                   tran4.CONTAINER_TYPE_MST_FK )               " +  "              in ( " + Convert.ToString(SectorContainers) + " )                            " +  "     AND    ROUND(TO_DATE('" + QuoteDate + "','" + dateFormat + "')-0.5)  <=           " +  "            tran4.EXPECTED_SHIPMENT                                           ";
                    //Added by rabbani on 24/3/07 ,To display BOF as first element in Freight Element Column using UNION ALL.
                }
                else
                {
                    strSQL = "    Select            " +  "     main4.ENQUIRY_REF_NO                       REF_NO,              " +  "     tran4.PORT_MST_POL_FK                      POL_PK,              " +  "     tran4.PORT_MST_POD_FK                      POD_PK,              " +  "     tran4.BASIS                                LCLBASIS,            " +  "     frtd4.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt4.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt4.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frtd4.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     'false'  ADVATOS,                                               " +  "     frtd4.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr4.CURRENCY_ID                          CURR_ID,             " +  "     frtd4.TARIFF_RATE                          RATE                 " +  "    from                                                             " +  "     ENQUIRY_BKG_SEA_TBL            main4,                           " +  "     ENQUIRY_TRN_SEA_FCL_LCL        tran4,                           " +  "     ENQUIRY_TRN_SEA_FRT_DTLS       frtd4,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt4,                            " +  "     CURRENCY_TYPE_MST_TBL          curr4                            " +  "    where                                                                " +  "            main4.ENQUIRY_BKG_SEA_PK    = tran4.ENQUIRY_MAIN_SEA_FK          " +  "     AND    tran4.ENQUIRY_TRN_SEA_PK    = frtd4.ENQUIRY_TRN_SEA_FK           " +  "     AND    frtd4.FREIGHT_ELEMENT_MST_FK = frt4.FREIGHT_ELEMENT_MST_PK(+)    " +  "     --AND    frt4.CHARGE_BASIS           <> 2                                 " +  "     AND    frtd4.CURRENCY_MST_FK       = curr4.CURRENCY_MST_PK(+)           " +  "     AND    main4.CARGO_TYPE            = 2                                  " +  "     AND    tran4.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "       " +  "     AND    main4.CUSTOMER_MST_FK       = " + Convert.ToString(CustNo) + "               " +  "     AND    (tran4.PORT_MST_POL_FK, tran4.PORT_MST_POD_FK,                   " +  "                                   tran4.LCL_BASIS )                         " +  "              in ( " + Convert.ToString(SectorContainers) + " )                          " +  "     AND    ROUND(TO_DATE('" + QuoteDate + "','" + dateFormat + "')-0.5)  <=         " +  "            tran4.EXPECTED_SHIPMENT                                          " +  "     AND    frt4.FREIGHT_ELEMENT_MST_PK IN (SELECT PARM.FRT_BOF_FK  FROM PARAMETERS_TBL PARM)" +  "     UNION ALL " +  "    Select            " +  "     main4.ENQUIRY_REF_NO                       REF_NO,              " +  "     tran4.PORT_MST_POL_FK                      POL_PK,              " +  "     tran4.PORT_MST_POD_FK                      POD_PK,              " +  "     tran4.BASIS                                LCLBASIS,            " +  "     frtd4.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt4.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt4.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frtd4.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     'false'  ADVATOS,                                               " +  "     frtd4.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr4.CURRENCY_ID                          CURR_ID,             " +  "     frtd4.TARIFF_RATE                          RATE                 " +  "    from                                                             " +  "     ENQUIRY_BKG_SEA_TBL            main4,                           " +  "     ENQUIRY_TRN_SEA_FCL_LCL        tran4,                           " +  "     ENQUIRY_TRN_SEA_FRT_DTLS       frtd4,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt4,                            " +  "     CURRENCY_TYPE_MST_TBL          curr4                            " +  "    where                                                                " +  "            main4.ENQUIRY_BKG_SEA_PK    = tran4.ENQUIRY_MAIN_SEA_FK          " +  "     AND    tran4.ENQUIRY_TRN_SEA_PK    = frtd4.ENQUIRY_TRN_SEA_FK           " +  "     AND    frtd4.FREIGHT_ELEMENT_MST_FK = frt4.FREIGHT_ELEMENT_MST_PK(+)    " +  "     --AND    frt4.CHARGE_BASIS           <> 2                                 " +  "     AND    frtd4.CURRENCY_MST_FK       = curr4.CURRENCY_MST_PK(+)           " +  "     AND    main4.CARGO_TYPE            = 2                                  " +  "     AND    tran4.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "       " +  "     AND    main4.CUSTOMER_MST_FK       = " + Convert.ToString(CustNo) + "               " +  "     AND    (tran4.PORT_MST_POL_FK, tran4.PORT_MST_POD_FK,                   " +  "                                   tran4.LCL_BASIS )                         " +  "              in ( " + Convert.ToString(SectorContainers) + " )                          " +  "     AND    ROUND(TO_DATE('" + QuoteDate + "','" + dateFormat + "')-0.5)  <=         " +  "            tran4.EXPECTED_SHIPMENT                                          " +  "     AND    frt4.FREIGHT_ELEMENT_MST_PK NOT IN (SELECT PARM.FRT_BOF_FK  FROM PARAMETERS_TBL PARM)";
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
                    strSQL = "    SELECT Q.* FROM(Select      " +  "     main1.RFQ_REF_NO                           REF_NO,              " +  "     tran1.PORT_MST_POL_FK                      POL_PK,              " +  "     tran1.PORT_MST_POD_FK                      POD_PK,              " +  "     cont1.CONTAINER_TYPE_MST_FK                CNTR_PK,             " +  "     tran1.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt1.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt1.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frt1.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, " +  "     DECODE(tran1.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     'false'  ADVATOS,                                               " +  "     tran1.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr1.CURRENCY_ID                          CURR_ID,             " +  "     cont1.FCL_APP_RATE                         QUOTERATE,                " +  "     NULL                                       FINAL_RATE,           " +  "     NULL                                       PYTYPE               " +  "    from                                                             " +  "     RFQ_SPOT_RATE_SEA_TBL          main1,                           " +  "     RFQ_SPOT_TRN_SEA_FCL_LCL       tran1,                           ";
                    // & vbCrLf _
                    //Snigdharani - Removing v-array - 04/11/2008
                    //& "TABLE(tran1.CONTAINER_DTL_FCL) (+) cont1,                    " & vbCrLf _
                    strSQL = strSQL + "RFQ_SPOT_TRN_SEA_CONT_DET   cont1,                    " +  "     commodity_group_mst_tbl              cmdt1,COMMODITY_MST_TBL         cmdt2," +  "     FREIGHT_ELEMENT_MST_TBL        frt1,                            " +  "     CURRENCY_TYPE_MST_TBL          curr1                            " +  "    where                                                                " +  "            main1.RFQ_SPOT_SEA_PK       = tran1.RFQ_SPOT_SEA_FK               " +  "     AND    main1.CARGO_TYPE            = 1                                   " +  "     AND    CONT1.RFQ_SPOT_SEA_TRN_FK=tran1.RFQ_SPOT_SEA_TRN_PK               " +  "     AND    main1.ACTIVE                = 1                                   " +  "     AND    main1.APPROVED              = 1                                   " +  "     AND    tran1.FREIGHT_ELEMENT_MST_FK = frt1.FREIGHT_ELEMENT_MST_PK(+)     " +  "     --AND    frt1.CHARGE_BASIS           <> 2                                  " +  "     AND    tran1.CURRENCY_MST_FK       = curr1.CURRENCY_MST_PK(+)            " +  "     AND    main1.commodity_group_fk = cmdt1.commodity_group_pk           " +  "     and    main1.commodity_mst_fk = cmdt2.commodity_mst_pk(+)           " +  "     AND    cmdt1.COMMODITY_GROUP_pK    = " + Convert.ToString(CommodityGroup) + "        " +  "     AND    (main1.CUSTOMER_MST_FK       = " + Convert.ToString(CustNo) + " OR main1.CUSTOMER_MST_FK IS NULL) " +  "     AND    (tran1.PORT_MST_POL_FK, tran1.PORT_MST_POD_FK,                    " +  "                                   cont1.CONTAINER_TYPE_MST_FK )              " +  "              in ( " + Convert.ToString(SectorContainers) + " )                           " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            tran1.VALID_FROM   AND   tran1.VALID_TO) Q,                       " +  "            FREIGHT_ELEMENT_MST_TBL FRT                           " +  "            WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FRT_PK                       " +  "            ORDER BY FRT.PREFERENCE                           ";
                    //Added by Rabbani raeson USS Gap,introduced New column as "Min.Rate"
                }
                else
                {
                    //Added by rabbani on 24/3/07 ,To display BOF as first element in Freight Element Column using UNION ALL.
                    strSQL = "    SELECT Q.* FROM(Select            " +  "     main1.RFQ_REF_NO                           REF_NO,              " +  "     tran1.PORT_MST_POL_FK                      POL_PK,              " +  "     tran1.PORT_MST_POD_FK                      POD_PK,              " +  "     tran1.LCL_BASIS                            LCLBASIS,            " +  "     tran1.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt1.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt1.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frt1.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, " +  "     DECODE(tran1.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     'false'  ADVATOS,                                               " +  "     tran1.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr1.CURRENCY_ID                          CURR_ID,             " +  "     tran1.LCL_APPROVED_MIN_RATE                QUOTE_MIN_RATE,            " +  "     tran1.LCL_APPROVED_RATE                    QUOTERATE,                 " +  "     tran1.LCL_APPROVED_RATE                    FINAL_RATE           " +  "    from                                                             " +  "     RFQ_SPOT_RATE_SEA_TBL          main1,                           " +  "     RFQ_SPOT_TRN_SEA_FCL_LCL       tran1,                           " +  "     commodity_group_mst_tbl        cmdt1,COMMODITY_MST_TBL         cmdt2," +  "     FREIGHT_ELEMENT_MST_TBL        frt1,                            " +  "     CURRENCY_TYPE_MST_TBL          curr1                            " +  "    where                                                                " +  "            main1.RFQ_SPOT_SEA_PK       = tran1.RFQ_SPOT_SEA_FK               " +  "     AND    main1.CARGO_TYPE            = 2                                   " +  "     AND    main1.ACTIVE                = 1                                   " +  "     AND    main1.APPROVED              = 1                                   " +  "     AND    tran1.FREIGHT_ELEMENT_MST_FK = frt1.FREIGHT_ELEMENT_MST_PK(+)     " +  "     --AND    frt1.CHARGE_BASIS           <> 2                                  " +  "     AND    tran1.CURRENCY_MST_FK       = curr1.CURRENCY_MST_PK(+)            " +  "     AND    main1.commodity_group_fk = cmdt1.commodity_group_pk           " +  "     and    main1.commodity_mst_fk = cmdt2.commodity_mst_pk(+)           " +  "     AND    cmdt1.COMMODITY_GROUP_pK    = " + Convert.ToString(CommodityGroup) + "        " +  "     AND    (main1.CUSTOMER_MST_FK       = " + Convert.ToString(CustNo) + " OR main1.CUSTOMER_MST_FK IS NULL) " +  "     AND    (tran1.PORT_MST_POL_FK, tran1.PORT_MST_POD_FK,                    " +  "                                   tran1.LCL_BASIS )                          " +  "              in ( " + Convert.ToString(SectorContainers) + " )                           " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            tran1.VALID_FROM   AND   tran1.VALID_TO                           " +  "     AND    frt1.FREIGHT_ELEMENT_MST_PK IN (SELECT PARM.FRT_BOF_FK  FROM PARAMETERS_TBL PARM)" +  "     UNION ALL " +  "     Select            " +  "     main1.RFQ_REF_NO                           REF_NO,              " +  "     tran1.PORT_MST_POL_FK                      POL_PK,              " +  "     tran1.PORT_MST_POD_FK                      POD_PK,              " +  "     tran1.LCL_BASIS                            LCLBASIS,            " +  "     tran1.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt1.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt1.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frt1.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, " +  "     DECODE(tran1.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     'false'  ADVATOS,                                                " +  "     tran1.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr1.CURRENCY_ID                          CURR_ID,             " +  "     tran1.LCL_APPROVED_MIN_RATE                QUOTE_MIN_RATE,            " +  "     tran1.LCL_APPROVED_RATE                    QUOTERATE ,                " +  "     tran1.LCL_APPROVED_RATE                    FINAL_RATE                 " +  "    from                                                             " +  "     RFQ_SPOT_RATE_SEA_TBL          main1,                           " +  "     RFQ_SPOT_TRN_SEA_FCL_LCL       tran1,                           " +  "     commodity_group_mst_tbl        cmdt1,COMMODITY_MST_TBL         cmdt2," +  "     FREIGHT_ELEMENT_MST_TBL        frt1,                            " +  "     CURRENCY_TYPE_MST_TBL          curr1                            " +  "    where                                                                " +  "            main1.RFQ_SPOT_SEA_PK       = tran1.RFQ_SPOT_SEA_FK               " +  "     AND    main1.CARGO_TYPE            = 2                                   " +  "     AND    main1.ACTIVE                = 1                                   " +  "     AND    main1.APPROVED              = 1                                   " +  "     AND    tran1.FREIGHT_ELEMENT_MST_FK = frt1.FREIGHT_ELEMENT_MST_PK(+)     " +  "     --AND    frt1.CHARGE_BASIS           <> 2                                  " +  "     AND    tran1.CURRENCY_MST_FK       = curr1.CURRENCY_MST_PK(+)            " +  "     AND    main1.commodity_group_fk = cmdt1.commodity_group_pk           " +  "     and    main1.commodity_mst_fk = cmdt2.commodity_mst_pk(+)           " +  "     AND    cmdt1.COMMODITY_GROUP_pK    = " + Convert.ToString(CommodityGroup) + "        " +  "     AND    (main1.CUSTOMER_MST_FK       = " + Convert.ToString(CustNo) + " OR main1.CUSTOMER_MST_FK IS NULL) " +  "     AND    (tran1.PORT_MST_POL_FK, tran1.PORT_MST_POD_FK,                    " +  "                                   tran1.LCL_BASIS )                          " +  "              in ( " + Convert.ToString(SectorContainers) + " )                           " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            tran1.VALID_FROM   AND   tran1.VALID_TO                           " +  "     AND    frt1.FREIGHT_ELEMENT_MST_PK NOT IN (SELECT PARM.FRT_BOF_FK  FROM PARAMETERS_TBL PARM)) Q," +  "            FREIGHT_ELEMENT_MST_TBL FRT                           " +  "            WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FRT_PK                       " +  "            ORDER BY FRT.PREFERENCE                           ";
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

                strContRefNo = " (   Select    DISTINCT  CONT_REF_NO " +  "    from                                                             " +  "     CONT_CUST_SEA_TBL              main7,                           " +  "     CONT_CUST_TRN_SEA_TBL          tran7                            " +  "    where                                                                " +  "            main7.CONT_CUST_SEA_PK      = tran7.CONT_CUST_SEA_FK              " +  "     AND    main7.CARGO_TYPE            = 1   AND main7.active=1              " +  "     AND    main7.COMMODITY_GROUP_MST_FK = " + Convert.ToString(CommodityGroup) + "       " +  "     AND    main7.CUSTOMER_MST_FK       =  " + Convert.ToString(CustNo) + "               " +  "     AND    tran7.PORT_MST_POL_FK       =  tran6.PORT_MST_POL_FK              " +  "     AND    tran7.PORT_MST_POD_FK       =  tran6.PORT_MST_POD_FK              " +  "     AND    tran7.CONTAINER_TYPE_MST_FK =  cont6.CONTAINER_TYPE_MST_FK        " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            tran7.VALID_FROM   AND   NVL(tran7.VALID_TO,NULL_DATE_FORMAT)        " +  "     AND    main7.OPERATOR_MST_FK =  main6.OPERATOR_MST_FK  )    ";

                strContNoLCL = " (   Select    DISTINCT  CONT_REF_NO " +  "    from                                                             " +  "     CONT_CUST_SEA_TBL              main7,                           " +  "     CONT_CUST_TRN_SEA_TBL          tran7                            " +  "    where                                                                " +  "            main7.CONT_CUST_SEA_PK      = tran7.CONT_CUST_SEA_FK              " +  "     AND    main7.CARGO_TYPE            = 2                                   " +  "     AND    main7.COMMODITY_GROUP_MST_FK = " + Convert.ToString(CommodityGroup) + "       " +  "     AND    main7.CUSTOMER_MST_FK       =  " + Convert.ToString(CustNo) + "               " +  "     AND    tran7.PORT_MST_POL_FK       =  tran6.PORT_MST_POL_FK              " +  "     AND    tran7.PORT_MST_POD_FK       =  tran6.PORT_MST_POD_FK              " +  "     AND    tran7.LCL_BASIS             =  tran6.LCL_BASIS                    " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            tran7.VALID_FROM   AND   NVL(tran7.VALID_TO,NULL_DATE_FORMAT)        " +  "     AND    main7.OPERATOR_MST_FK =  main6.OPERATOR_MST_FK  )    ";

                strFreightElements = " (   Select   DISTINCT  frtd8.FREIGHT_ELEMENT_MST_FK " +  "    from                                                             " +  "     CONT_CUST_SEA_TBL              main8,                           " +  "     CONT_CUST_TRN_SEA_TBL          tran8,                           " +  "     CONT_SUR_CHRG_SEA_TBL          frtd8                            " +  "    where                                                                " +  "            main8.CONT_CUST_SEA_PK      = tran8.CONT_CUST_SEA_FK              " +  "     AND    tran8.CONT_CUST_TRN_SEA_PK  = frtd8.CONT_CUST_TRN_SEA_FK          " +  "     AND    main8.CARGO_TYPE            = 1  AND MAIN8.Active=1              " +  "     AND    main8.COMMODITY_GROUP_MST_FK = " + Convert.ToString(CommodityGroup) + "       " +  "     AND    main8.CUSTOMER_MST_FK       =  " + Convert.ToString(CustNo) + "               " +  "     AND    tran8.PORT_MST_POL_FK       = tran6.PORT_MST_POL_FK               " +  "     AND    tran8.PORT_MST_POD_FK       = tran6.PORT_MST_POD_FK               " +  "     AND    tran8.CONTAINER_TYPE_MST_FK = cont6.CONTAINER_TYPE_MST_FK         " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            tran8.VALID_FROM   AND   NVL(tran8.VALID_TO,NULL_DATE_FORMAT)        " +  "     AND    main8.OPERATOR_MST_FK       =  main6.OPERATOR_MST_FK          )   ";

                strFreightsLCL = " (   Select   DISTINCT  frtd8.FREIGHT_ELEMENT_MST_FK " +  "    from                                                             " +  "     CONT_CUST_SEA_TBL              main8,                           " +  "     CONT_CUST_TRN_SEA_TBL          tran8,                           " +  "     CONT_SUR_CHRG_SEA_TBL          frtd8                            " +  "    where                                                                " +  "            main8.CONT_CUST_SEA_PK      = tran8.CONT_CUST_SEA_FK              " +  "     AND    tran8.CONT_CUST_TRN_SEA_PK  = frtd8.CONT_CUST_TRN_SEA_FK          " +  "     AND    main8.CARGO_TYPE            = 2                                   " +  "     AND    main8.COMMODITY_GROUP_MST_FK = " + Convert.ToString(CommodityGroup) + "       " +  "     AND    main8.CUSTOMER_MST_FK       =  " + Convert.ToString(CustNo) + "               " +  "     AND    tran8.PORT_MST_POL_FK       = tran6.PORT_MST_POL_FK               " +  "     AND    tran8.PORT_MST_POD_FK       = tran6.PORT_MST_POD_FK               " +  "     AND    tran8.LCL_BASIS             = tran6.LCL_BASIS                     " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            tran8.VALID_FROM   AND   NVL(tran8.VALID_TO,NULL_DATE_FORMAT)        " +  "     AND    main8.OPERATOR_MST_FK       =  main6.OPERATOR_MST_FK          )   ";


                strSurcharge = " ( Select  DISTINCT  SUBJECT_TO_SURCHG_CHG  " +  "    from                                                             " +  "     CONT_CUST_SEA_TBL              main9,                           " +  "     CONT_CUST_TRN_SEA_TBL          tran9                            " +  "    where                                                                " +  "            main9.CONT_CUST_SEA_PK      = tran9.CONT_CUST_SEA_FK              " +  "     AND    main9.CARGO_TYPE            = 1 AND MAIN9.Active=1               " +  "     AND    main9.COMMODITY_GROUP_MST_FK = " + Convert.ToString(CommodityGroup) + "       " +  "     AND    main9.CUSTOMER_MST_FK       =  " + Convert.ToString(CustNo) + "               " +  "     AND    tran9.PORT_MST_POL_FK       =  tran6.PORT_MST_POL_FK              " +  "     AND    tran9.PORT_MST_POD_FK       =  tran6.PORT_MST_POD_FK              " +  "     AND    tran9.CONTAINER_TYPE_MST_FK =  cont6.CONTAINER_TYPE_MST_FK        " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            tran9.VALID_FROM   AND   NVL(tran9.VALID_TO,NULL_DATE_FORMAT)        " +  "     AND    main9.OPERATOR_MST_FK       =  main6.OPERATOR_MST_FK          )   ";

                strSurchargeLCL = " ( Select  DISTINCT  SUBJECT_TO_SURCHG_CHG  " +  "    from                                                             " +  "     CONT_CUST_SEA_TBL              main9,                           " +  "     CONT_CUST_TRN_SEA_TBL          tran9                            " +  "    where                                                                " +  "            main9.CONT_CUST_SEA_PK      = tran9.CONT_CUST_SEA_FK              " +  "     AND    main9.CARGO_TYPE            = 2                                   " +  "     AND    main9.COMMODITY_GROUP_MST_FK = " + Convert.ToString(CommodityGroup) + "       " +  "     AND    main9.CUSTOMER_MST_FK       =  " + Convert.ToString(CustNo) + "               " +  "     AND    tran9.PORT_MST_POL_FK       =  tran6.PORT_MST_POL_FK              " +  "     AND    tran9.PORT_MST_POD_FK       =  tran6.PORT_MST_POD_FK              " +  "     AND    tran9.LCL_BASIS             =  tran6.LCL_BASIS                    " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            tran9.VALID_FROM   AND   NVL(tran9.VALID_TO,NULL_DATE_FORMAT)        " +  "     AND    main9.OPERATOR_MST_FK       =  main6.OPERATOR_MST_FK          )   ";

                strContSectors = "  Select  DISTINCT  tran10.PORT_MST_POL_FK,                               " +  "          tran10.PORT_MST_POD_FK, tran10.CONTAINER_TYPE_MST_FK                " +  "    from                                                                      " +  "     CONT_CUST_SEA_TBL              main10,                                   " +  "     CONT_CUST_TRN_SEA_TBL          tran10                                    " +  "    where                                                                     " +  "            main10.CONT_CUST_SEA_PK      = tran10.CONT_CUST_SEA_FK            " +  "     AND    main10.CARGO_TYPE            = 1  AND MAIN10.Active=1             " +  "     AND    main10.COMMODITY_GROUP_MST_FK = " + Convert.ToString(CommodityGroup) + "      " +  "     AND    main10.CUSTOMER_MST_FK       =  " + Convert.ToString(CustNo) + "              " +  "     AND    (tran10.PORT_MST_POL_FK, tran10.PORT_MST_POD_FK,                  " +  "                                   tran10.CONTAINER_TYPE_MST_FK )             " +  "              in ( " + Convert.ToString(SectorContainers) + " )                           " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            tran10.VALID_FROM   AND   NVL(tran10.VALID_TO,NULL_DATE_FORMAT)      " +  "     AND    main10.OPERATOR_MST_FK       =  main6.OPERATOR_MST_FK            ";

                strBasisSectors = "  Select  DISTINCT  tran10.PORT_MST_POL_FK,                               " +  "          tran10.PORT_MST_POD_FK, tran10.LCL_BASIS                            " +  "    from                                                                      " +  "     CONT_CUST_SEA_TBL              main10,                                   " +  "     CONT_CUST_TRN_SEA_TBL          tran10                                    " +  "    where                                                                     " +  "            main10.CONT_CUST_SEA_PK      = tran10.CONT_CUST_SEA_FK            " +  "     AND    main10.CARGO_TYPE            = 2                                  " +  "     AND    main10.COMMODITY_GROUP_MST_FK = " + Convert.ToString(CommodityGroup) + "      " +  "     AND    main10.CUSTOMER_MST_FK       =  " + Convert.ToString(CustNo) + "              " +  "     AND    (tran10.PORT_MST_POL_FK, tran10.PORT_MST_POD_FK,                  " +  "                                   tran10.LCL_BASIS )                         " +  "              in ( " + Convert.ToString(SectorContainers) + " )                           " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            tran10.VALID_FROM   AND   NVL(tran10.VALID_TO,NULL_DATE_FORMAT)      " +  "     AND    main10.OPERATOR_MST_FK       =  main6.OPERATOR_MST_FK            ";

                //IF "APPROVED_ALL_IN_RATE" > 0 then
                //(1) Elements from child table having "CONT_SUR_CHRG_SEA_TBL.CHECK_FOR ALL_IN_RT"=1 with approved rates 
                //(2) BOF with "CONT_CUST_TRN_SEA_TBL.CURRENT_BOF_RATE"
                //ELSE
                //(1) All Elements from child table with approved rates 
                //(2) BOF with "CONT_CUST_TRN_SEA_TBL.APPROVED_BOF_RATE"
                if (forFCL)
                {
                    strSQL = "    SELECT Q.* FROM (Select     " +  "     main22.CONT_REF_NO                          REF_NO,              " +  "     tran22.PORT_MST_POL_FK                      POL_PK,              " +  "     tran22.PORT_MST_POD_FK                      POD_PK,              " +  "     tran22.CONTAINER_TYPE_MST_FK                CNTR_PK,             " +  "     frt22.FREIGHT_ELEMENT_MST_PK                FRT_PK,              " +  "     frt22.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt22.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frt22.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, " +  "     'true'                                      SELECTED,            " +  "     'false'  ADVATOS,                                               " +  "     tran22.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr22.CURRENCY_ID                          CURR_ID,             " +  "   ( Case When MAIN22.STATUS <> 2 Then    " + "            tran22.CURRENT_BOF_RATE                     else    " + "            tran22.APPROVED_BOF_RATE End )         QUOTERATE,                " +  "     NULL                                        FINAL_RATE,           " +  "     NULL                                        PYTYPE               " +  "    from                                                              " +  "     CONT_CUST_SEA_TBL              main22,                           " +  "     CONT_CUST_TRN_SEA_TBL          tran22,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt22,                            " +  "     CURRENCY_TYPE_MST_TBL          curr22                            " +  "    where                                                                  " +  "            main22.CONT_CUST_SEA_PK      = tran22.CONT_CUST_SEA_FK              " +  "     AND    frt22.FREIGHT_ELEMENT_MST_PK = " + BofPk + "                        " +  "     AND    tran22.CURRENCY_MST_FK       = curr22.CURRENCY_MST_PK(+)            " +  "     AND    main22.CARGO_TYPE            = 1  AND main22.active=1               " +  "     AND    main22.COMMODITY_GROUP_MST_FK = " + Convert.ToString(CommodityGroup) + "        " +  "     AND    main22.CUSTOMER_MST_FK       =  " + Convert.ToString(CustNo) + "                " +  "     AND    (tran22.PORT_MST_POL_FK, tran22.PORT_MST_POD_FK,                    " +  "                                   tran22.CONTAINER_TYPE_MST_FK )               " +  "              in ( " + Convert.ToString(SectorContainers) + " )                             " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN " +  "            tran22.VALID_FROM   AND   NVL(tran22.VALID_TO,NULL_DATE_FORMAT)        " +  "    UNION ";
                }
                else
                {
                    strSQL = "    SELECT Q.* FROM (Select     " +  "     main22.CONT_REF_NO                          REF_NO,              " +  "     tran22.PORT_MST_POL_FK                      POL_PK,              " +  "     tran22.PORT_MST_POD_FK                      POD_PK,              " +  "     tran22.LCL_BASIS                            LCLBASIS,            " +  "     frt22.FREIGHT_ELEMENT_MST_PK                FRT_PK,              " +  "     frt22.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt22.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frt22.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, " +  "     'true'                                      SELECTED,            " +  "     'false'  ADVATOS,                                               " +  "     tran22.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr22.CURRENCY_ID                          CURR_ID,             " +  "     NULL  QUOTE_MIN_RATE,             " +  "   ( Case When MAIN22.STATUS <> 2 Then    " + "            tran22.CURRENT_BOF_RATE                     else    " + "            tran22.APPROVED_BOF_RATE End )         QUOTERATE ,                " +  "   ( Case When MAIN22.STATUS <> 2 Then    " + "            tran22.CURRENT_BOF_RATE                     else    " + "            tran22.APPROVED_BOF_RATE End )         FINAL_RATE                 " +  "    from                                                              " +  "     CONT_CUST_SEA_TBL              main22,                           " +  "     CONT_CUST_TRN_SEA_TBL          tran22,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt22,                            " +  "     CURRENCY_TYPE_MST_TBL          curr22                            " +  "    where                                                                  " +  "            main22.CONT_CUST_SEA_PK      = tran22.CONT_CUST_SEA_FK              " +  "     AND    frt22.FREIGHT_ELEMENT_MST_PK = " + BofPk + "                        " +  "     AND    tran22.CURRENCY_MST_FK       = curr22.CURRENCY_MST_PK(+)            " +  "     AND    main22.CARGO_TYPE            = 2                                    " +  "     AND    main22.COMMODITY_GROUP_MST_FK = " + Convert.ToString(CommodityGroup) + "        " +  "     AND    main22.CUSTOMER_MST_FK       =  " + Convert.ToString(CustNo) + "                " +  "     AND    (tran22.PORT_MST_POL_FK, tran22.PORT_MST_POD_FK,                    " +  "                                   tran22.LCL_BASIS )                           " +  "              in ( " + Convert.ToString(SectorContainers) + " )                             " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN " +  "            tran22.VALID_FROM   AND   NVL(tran22.VALID_TO,NULL_DATE_FORMAT)        " +  "    UNION ";
                }
                if (forFCL)
                {
                    strSQL += "    Select     " +  "     main2.CONT_REF_NO                          REF_NO,              " +  "     tran2.PORT_MST_POL_FK                      POL_PK,              " +  "     tran2.PORT_MST_POD_FK                      POD_PK,              " +  "     tran2.CONTAINER_TYPE_MST_FK                CNTR_PK,             " +  "     frtd2.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt2.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt2.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frt2.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, " +  "   ( Case When NVL(tran2.APPROVED_ALL_IN_RATE,0) > 0        Then " + "        DECODE(frtd2.CHECK_FOR_ALL_IN_RT, 1,'true','false')   else " + "        'true'        End )                       SELECTED,            " +  "     'false'  ADVATOS,                                               " +  "     frtd2.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr2.CURRENCY_ID                          CURR_ID,             " +  "     frtd2.APP_SURCHARGE_AMT                    QUOTERATE,                " +  "     NULL                                       FINAL_RATE,           " +  "     NULL                                       PYTYPE               " +  "    from                                                             " +  "     CONT_CUST_SEA_TBL              main2,                           " +  "     CONT_CUST_TRN_SEA_TBL          tran2,                           " +  "     CONT_SUR_CHRG_SEA_TBL          frtd2,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt2,                            " +  "     CURRENCY_TYPE_MST_TBL          curr2                            " +  "    where                                                                " +  "            main2.CONT_CUST_SEA_PK      = tran2.CONT_CUST_SEA_FK              " +  "     AND    tran2.CONT_CUST_TRN_SEA_PK  = frtd2.CONT_CUST_TRN_SEA_FK          " +  "     AND    frtd2.FREIGHT_ELEMENT_MST_FK = frt2.FREIGHT_ELEMENT_MST_PK(+)     " +  "     --AND    frt2.CHARGE_BASIS           <> 2                                  " +  "     AND    frtd2.CURRENCY_MST_FK       = curr2.CURRENCY_MST_PK(+)            " +  "     AND    main2.CARGO_TYPE            = 1   AND main2.active=1              " +  "     AND    main2.COMMODITY_GROUP_MST_FK = " + Convert.ToString(CommodityGroup) + "       " +  "     AND    main2.CUSTOMER_MST_FK       =  " + Convert.ToString(CustNo) + "               " +  "     AND    (tran2.PORT_MST_POL_FK, tran2.PORT_MST_POD_FK,                    " +  "                                   tran2.CONTAINER_TYPE_MST_FK )              " +  "              in ( " + Convert.ToString(SectorContainers) + " )                           " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            tran2.VALID_FROM   AND   NVL(tran2.VALID_TO,NULL_DATE_FORMAT)        " +  "    UNION  " +  "   Select            " +  "     " + strContRefNo + "                       REF_NO,              " +  "     tran6.PORT_MST_POL_FK                      POL_PK,              " +  "     tran6.PORT_MST_POD_FK                      POD_PK,              " +  "     cont6.CONTAINER_TYPE_MST_FK                CNTR_PK,             " +  "     tran6.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt6.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt6.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frt6.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, " +  "     DECODE(tran6.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     'false'  ADVATOS,                                               " +  "     tran6.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr6.CURRENCY_ID                          CURR_ID,             " +  "     cont6.FCL_REQ_RATE                         RATE,                " +  "     NULL                                       QUOTERATE,           " +  "     NULL                                       PYTYPE               " +  "    from                                                             " +  "     TARIFF_MAIN_SEA_TBL            main6,                           " +  "     TARIFF_TRN_SEA_FCL_LCL         tran6,                           ";
                    //& vbCrLf _
                    //Modified by Snigdharani - 29/10/2008 - Removing v-array
                    //TABLE(tran6.CONTAINER_DTL_FCL) (+) cont6
                    strSQL = strSQL + "TARIFF_TRN_SEA_CONT_DTL cont6,                       " +  "     FREIGHT_ELEMENT_MST_TBL        frt6,                            " +  "     CURRENCY_TYPE_MST_TBL          curr6                            " +  "    where " + strContRefNo + " IS NOT NULL AND                       " +  "            main6.TARIFF_MAIN_SEA_PK    = tran6.TARIFF_MAIN_SEA_FK           " +  "     AND    main6.CARGO_TYPE            = 1  AND MAIN6.ACTIVE = 1            " +  "     AND    main6.ACTIVE                = 1                                  " +  "     AND    tran6.FREIGHT_ELEMENT_MST_FK = frt6.FREIGHT_ELEMENT_MST_PK(+)    " +  "     AND    cont6.TARIFF_TRN_SEA_FK = tran6.TARIFF_TRN_SEA_PK                " +  "     --AND    frt6.CHARGE_BASIS           <> 2                                 " +  "     AND    tran6.CURRENCY_MST_FK       = curr6.CURRENCY_MST_PK(+)           " +  "     AND    main6.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "       " +  "     AND    (tran6.PORT_MST_POL_FK, tran6.PORT_MST_POD_FK,                   " +  "                                   cont6.CONTAINER_TYPE_MST_FK )             " +  "              in ( " + strContSectors + " )                                  " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN      " +  "            tran6.VALID_FROM   AND   NVL(tran6.VALID_TO,NULL_DATE_FORMAT)       " +  "     AND    tran6.FREIGHT_ELEMENT_MST_FK NOT IN (" + strFreightElements + ") " +  "     AND  " + strSurcharge + " = 1) Q,                  " +  "     FREIGHT_ELEMENT_MST_TBL FRT                  " +  "     WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FRT_PK         " +  "     ORDER BY FRT.PREFERENCE                ";
                }
                else
                {
                    strSQL += "    Select            " +  "     main2.CONT_REF_NO                          REF_NO,              " +  "     tran2.PORT_MST_POL_FK                      POL_PK,              " +  "     tran2.PORT_MST_POD_FK                      POD_PK,              " +  "     tran2.LCL_BASIS                            LCLBASIS,            " +  "     frtd2.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt2.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt2.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frt2.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, " +  "   ( Case When NVL(tran2.APPROVED_ALL_IN_RATE,0) > 0        Then " + "        DECODE(frtd2.CHECK_FOR_ALL_IN_RT, 1,'true','false')   else " + "        'true'        End )                       SELECTED,            " +  "     'false'  ADVATOS,                                               " +  "     frtd2.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr2.CURRENCY_ID                          CURR_ID,             " +  "     NULL  MIN_RATE,                 " +  "     frtd2.APP_SURCHARGE_AMT                    RATE  ,               " +  "     frtd2.APP_SURCHARGE_AMT                    FINAL_RATE                 " +  "    from                                                             " +  "     CONT_CUST_SEA_TBL              main2,                           " +  "     CONT_CUST_TRN_SEA_TBL          tran2,                           " +  "     CONT_SUR_CHRG_SEA_TBL          frtd2,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt2,                            " +  "     CURRENCY_TYPE_MST_TBL          curr2                            " +  "    where                                                                " +  "            main2.CONT_CUST_SEA_PK      = tran2.CONT_CUST_SEA_FK             " +  "     AND    tran2.CONT_CUST_TRN_SEA_PK  = frtd2.CONT_CUST_TRN_SEA_FK         " +  "     AND    frtd2.FREIGHT_ELEMENT_MST_FK = frt2.FREIGHT_ELEMENT_MST_PK(+)    " +  "     --AND    frt2.CHARGE_BASIS           <> 2                                 " +  "     AND    frtd2.CURRENCY_MST_FK       = curr2.CURRENCY_MST_PK(+)           " +  "     AND    main2.CARGO_TYPE            = 2                                  " +  "     AND    main2.COMMODITY_GROUP_MST_FK = " + Convert.ToString(CommodityGroup) + "      " +  "     AND    main2.CUSTOMER_MST_FK       =  " + Convert.ToString(CustNo) + "              " +  "     AND    (tran2.PORT_MST_POL_FK, tran2.PORT_MST_POD_FK,                   " +  "                                   tran2.LCL_BASIS )                         " +  "              in ( " + Convert.ToString(SectorContainers) + " )                          " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN      " +  "            tran2.VALID_FROM   AND   NVL(tran2.VALID_TO,NULL_DATE_FORMAT)       " +  "    UNION  " +  "   Select            " +  "     " + strContNoLCL + "                       REF_NO,              " +  "     tran6.PORT_MST_POL_FK                      POL_PK,              " +  "     tran6.PORT_MST_POD_FK                      POD_PK,              " +  "     tran6.LCL_BASIS                            LCLBASIS,            " +  "     tran6.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt6.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt6.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frt6.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, " +  "     DECODE(tran6.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     'false'  ADVATOS,                                               " +  "     tran6.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr6.CURRENCY_ID                          CURR_ID,             " +  "     NULL  MIN_RATE,                 " +  "     tran6.LCL_TARIFF_RATE                      RATE  ,               " +  "     tran6.LCL_TARIFF_RATE                      FINAL_RATE                 " +  "    from                                                             " +  "     TARIFF_MAIN_SEA_TBL            main6,                           " +  "     TARIFF_TRN_SEA_FCL_LCL         tran6,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt6,                            " +  "     CURRENCY_TYPE_MST_TBL          curr6                            " +  "    where " + strContNoLCL + " IS NOT NULL AND                       " +  "            main6.TARIFF_MAIN_SEA_PK    = tran6.TARIFF_MAIN_SEA_FK           " +  "     AND    main6.CARGO_TYPE            = 2                                  " +  "     AND    main6.ACTIVE                = 1                                  " +  "     AND    tran6.FREIGHT_ELEMENT_MST_FK = frt6.FREIGHT_ELEMENT_MST_PK(+)    " +  "     --AND    frt6.CHARGE_BASIS           <> 2                                  " +  "     AND    tran6.CURRENCY_MST_FK       = curr6.CURRENCY_MST_PK(+)           " +  "     AND    main6.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "       " +  "     AND    (tran6.PORT_MST_POL_FK, tran6.PORT_MST_POD_FK,                   " +  "                                   tran6.LCL_BASIS )                         " +  "              in ( " + strBasisSectors + " )                                 " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN      " +  "            tran6.VALID_FROM   AND   NVL(tran6.VALID_TO,NULL_DATE_FORMAT)       " +  "     AND    tran6.FREIGHT_ELEMENT_MST_FK NOT IN (" + strFreightsLCL + ")     " +  "     AND  " + strSurchargeLCL + " = 1) Q,               " +  "     FREIGHT_ELEMENT_MST_TBL FRT                  " +  "     WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FRT_PK         " +  "     ORDER BY FRT.PREFERENCE                ";
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

        private string OprTariffFreightQuery(bool forFCL = true, string CustNo = "", string SectorContainers = "", string CommodityGroup = "", string ShipDate = "")
        {
            try
            {
                string strSQL = null;
                string exchQueryFCL = null;
                string exchQueryLCL = null;

                if (forFCL)
                {
                    strSQL = "    SELECT Q.* FROM (Select            " +  "     main5.TARIFF_REF_NO                        REF_NO,              " +  "     tran5.PORT_MST_POL_FK                      POL_PK,              " +  "     tran5.PORT_MST_POD_FK                      POD_PK,              " +  "     cont5.CONTAINER_TYPE_MST_FK                CNTR_PK,             " +  "     tran5.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt5.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt5.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frt5.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, " +  "     DECODE(tran5.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     'false'  ADVATOS,                                               " +  "     tran5.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr5.CURRENCY_ID                          CURR_ID,             " +  "     cont5.FCL_REQ_RATE                         QUOTERATE,                " +  "     NULL                                       FINAL_RATE,           " +  "     NULL                                       PYTYPE               " +  "    from                                                             " +  "     TARIFF_MAIN_SEA_TBL            main5,                           " +  "     TARIFF_TRN_SEA_FCL_LCL         tran5,                           ";
                    //& vbCrLf _
                    //Modified by Snigdharani - 29/10/2008 - Removing v-array
                    //TABLE(tran5.CONTAINER_DTL_FCL) (+) cont5
                    strSQL = strSQL + "     TARIFF_TRN_SEA_CONT_DTL cont5,                       " +  "     FREIGHT_ELEMENT_MST_TBL        frt5,                            " +  "     CURRENCY_TYPE_MST_TBL          curr5                            " +  "    where                                                                " +  "            main5.TARIFF_MAIN_SEA_PK    = tran5.TARIFF_MAIN_SEA_FK            " +  "     AND    main5.CARGO_TYPE            = 1                                   " +  "     AND    main5.ACTIVE                = 1                                   " +  "     AND    main5.TARIFF_TYPE           = 1                                   " +  "     AND    tran5.FREIGHT_ELEMENT_MST_FK = frt5.FREIGHT_ELEMENT_MST_PK(+)     " +  "     AND    cont5.TARIFF_TRN_SEA_FK = tran5.TARIFF_TRN_SEA_PK                 " +  "     --AND    frt5.CHARGE_BASIS           <> 2                                  " +  "     AND    tran5.CURRENCY_MST_FK       = curr5.CURRENCY_MST_PK(+)            " +  "     AND    main5.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "        " +  "     AND    (tran5.PORT_MST_POL_FK, tran5.PORT_MST_POD_FK,                    " +  "                                   cont5.CONTAINER_TYPE_MST_FK )              " +  "              in ( " + Convert.ToString(SectorContainers) + " )                           " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            tran5.VALID_FROM   AND   NVL(tran5.VALID_TO,NULL_DATE_FORMAT)) Q," +  "            FREIGHT_ELEMENT_MST_TBL FRT        " +  "            WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FRT_PK       " +  "            ORDER BY FRT.PREFERENCE        ";
                    //Added by rabbani reason USS Gap,introduced New Column as "QUOTE_MIN_RATE"
                }
                else
                {
                    //Added by rabbani on 24/3/07 ,To display BOF as first element in Freight Element Column using UNION ALL.
                    strSQL = "    SELECT Q.* FROM (Select            " +  "     main5.TARIFF_REF_NO                        REF_NO,              " +  "     tran5.PORT_MST_POL_FK                      POL_PK,              " +  "     tran5.PORT_MST_POD_FK                      POD_PK,              " +  "     tran5.LCL_BASIS                            LCLBASIS,            " +  "     tran5.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt5.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt5.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frt5.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, " +  "     DECODE(tran5.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     'false'  ADVATOS,                                               " +  "     tran5.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr5.CURRENCY_ID                          CURR_ID,             " +  "     tran5.LCL_TARIFF_MIN_RATE                  QUOTE_MIN_RATE,      " +  "     tran5.LCL_TARIFF_RATE                      QUOTERATE ,                " +  "     tran5.LCL_TARIFF_RATE                      FINAL_RATE                 " +  "    from                                                             " +  "     TARIFF_MAIN_SEA_TBL            main5,                           " +  "     TARIFF_TRN_SEA_FCL_LCL         tran5,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt5,                            " +  "     CURRENCY_TYPE_MST_TBL          curr5                            " +  "    where                                                                " +  "            main5.TARIFF_MAIN_SEA_PK    = tran5.TARIFF_MAIN_SEA_FK            " +  "     AND    main5.CARGO_TYPE            = 2                                   " +  "     AND    main5.ACTIVE                = 1                                   " +  "     AND    main5.TARIFF_TYPE           = 1                                   " +  "     AND    tran5.FREIGHT_ELEMENT_MST_FK = frt5.FREIGHT_ELEMENT_MST_PK(+)     " +  "     --AND    frt5.CHARGE_BASIS           <> 2                                  " +  "     AND    tran5.CURRENCY_MST_FK       = curr5.CURRENCY_MST_PK(+)            " +  "     AND    main5.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "        " +  "     AND    (tran5.PORT_MST_POL_FK, tran5.PORT_MST_POD_FK,                    " +  "                                   tran5.LCL_BASIS )                          " +  "              in ( " + Convert.ToString(SectorContainers) + " )                           " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            tran5.VALID_FROM   AND   NVL(tran5.VALID_TO,NULL_DATE_FORMAT)       " +  "     AND    frt5.FREIGHT_ELEMENT_MST_PK IN (SELECT PARM.FRT_BOF_FK  FROM PARAMETERS_TBL PARM) " +  "     UNION ALL " +  "     Select            " +  "     main5.TARIFF_REF_NO                        REF_NO,              " +  "     tran5.PORT_MST_POL_FK                      POL_PK,              " +  "     tran5.PORT_MST_POD_FK                      POD_PK,              " +  "     tran5.LCL_BASIS                            LCLBASIS,            " +  "     tran5.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt5.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt5.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frt5.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, " +  "     DECODE(tran5.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     'false'  ADVATOS,                                               " +  "     tran5.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr5.CURRENCY_ID                          CURR_ID,             " +  "     tran5.LCL_TARIFF_MIN_RATE                  QUOTE_MIN_RATE,      " +  "     tran5.LCL_TARIFF_RATE                      RATE,                 " +  "     tran5.LCL_TARIFF_RATE                      FINAL_RATE                 " +  "    from                                                             " +  "     TARIFF_MAIN_SEA_TBL            main5,                           " +  "     TARIFF_TRN_SEA_FCL_LCL         tran5,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt5,                            " +  "     CURRENCY_TYPE_MST_TBL          curr5                            " +  "    where                                                                " +  "            main5.TARIFF_MAIN_SEA_PK    = tran5.TARIFF_MAIN_SEA_FK            " +  "     AND    main5.CARGO_TYPE            = 2                                   " +  "     AND    main5.ACTIVE                = 1                                   " +  "     AND    main5.TARIFF_TYPE           = 1                                   " +  "     AND    tran5.FREIGHT_ELEMENT_MST_FK = frt5.FREIGHT_ELEMENT_MST_PK(+)     " +  "     --AND    frt5.CHARGE_BASIS           <> 2                                  " +  "     AND    tran5.CURRENCY_MST_FK       = curr5.CURRENCY_MST_PK(+)            " +  "     AND    main5.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "        " +  "     AND    (tran5.PORT_MST_POL_FK, tran5.PORT_MST_POD_FK,                    " +  "                                   tran5.LCL_BASIS )                          " +  "              in ( " + Convert.ToString(SectorContainers) + " )                           " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            tran5.VALID_FROM   AND   NVL(tran5.VALID_TO,NULL_DATE_FORMAT)       " +  "     AND    frt5.FREIGHT_ELEMENT_MST_PK NOT IN (SELECT PARM.FRT_BOF_FK  FROM PARAMETERS_TBL PARM)) Q, " +  "            FREIGHT_ELEMENT_MST_TBL FRT        " +  "            WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FRT_PK       " +  "            ORDER BY FRT.PREFERENCE        ";
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

        private string funGenTariffFreightQuery(bool forFCL = true, string CustNo = "", string SectorContainers = "", string CommodityGroup = "", string ShipDate = "", int Group = 0)
        {
            try
            {
                string strSQL = null;
                string exchQueryFCL = null;
                string exchQueryLCL = null;
                if (Group == 1 | Group == 2)
                {
                    if (forFCL)
                    {
                        strSQL = "    SELECT Q.* FROM (SELECT DISTINCT           " +  "     main5.TARIFF_REF_NO                        REF_NO,              " +  "     tran5.POL_GRP_FK                      POL_PK,              " +  "     tran5.POD_GRP_FK                      POD_PK,              " +  "     cont5.CONTAINER_TYPE_MST_FK                CNTR_PK,             " +  "     tran5.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt5.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt5.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frt5.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, " +  "     DECODE(tran5.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     'false'  ADVATOS,                                               " +  "     tran5.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr5.CURRENCY_ID                          CURR_ID,             " +  "     cont5.FCL_REQ_RATE                         QUOTERATE,                " +  "     NULL                                       FINAL_RATE,           " +  "     NULL                                       PYTYPE               " +  "    FROM                                                             " +  "     TARIFF_MAIN_SEA_TBL            main5,                           " +  "     TARIFF_TRN_SEA_FCL_LCL         tran5,                           " +  "     TARIFF_TRN_SEA_CONT_DTL        cont5,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt5,                            " +  "     CURRENCY_TYPE_MST_TBL          curr5                            " +  "    WHERE                                                                " +  "            main5.TARIFF_MAIN_SEA_PK    = tran5.TARIFF_MAIN_SEA_FK            " +  "     AND    main5.CARGO_TYPE            = 1                                   " +  "     AND    main5.ACTIVE                = 1                                   " +  "     AND    main5.TARIFF_TYPE           = 2                                   " +  "     AND    tran5.FREIGHT_ELEMENT_MST_FK = frt5.FREIGHT_ELEMENT_MST_PK(+)     " +  "     AND    cont5.TARIFF_TRN_SEA_FK = tran5.TARIFF_TRN_SEA_PK     " +  "     --AND    frt5.CHARGE_BASIS           <> 2                                  " +  "     AND    tran5.CURRENCY_MST_FK       = curr5.CURRENCY_MST_PK(+)            " +  "     AND    main5.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "        " +  "     AND    (tran5.POL_GRP_FK, tran5.POD_GRP_FK,                              " +  "                                   cont5.CONTAINER_TYPE_MST_FK )              " +  "              in ( " + Convert.ToString(SectorContainers) + " )                           " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            tran5.VALID_FROM   AND   NVL(tran5.VALID_TO,NULL_DATE_FORMAT)) Q,        " +  "            FREIGHT_ELEMENT_MST_TBL FRT        " +  "            WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FRT_PK       " +  "            ORDER BY FRT.PREFERENCE        ";
                    }
                    else
                    {
                        strSQL = "    SELECT Q.* FROM (Select DISTINCT           " +  "     main5.TARIFF_REF_NO                        REF_NO,              " +  "     tran5.POL_GRP_FK                           POL_PK,              " +  "     tran5.POD_GRP_FK                           POD_PK,              " +  "     tran5.LCL_BASIS                            LCLBASIS,            " +  "     tran5.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt5.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt5.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frt5.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, " +  "     DECODE(tran5.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     'false'  ADVATOS,                                               " +  "     tran5.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr5.CURRENCY_ID                          CURR_ID,             " +  "     tran5.LCL_TARIFF_MIN_RATE                  MIN_RATE,            " +  "     tran5.LCL_TARIFF_RATE                      QUOTERATE,                 " +  "     tran5.LCL_TARIFF_RATE                      FINAL_RATE                 " +  "    from                                                             " +  "     TARIFF_MAIN_SEA_TBL            main5,                           " +  "     TARIFF_TRN_SEA_FCL_LCL         tran5,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt5,                            " +  "     CURRENCY_TYPE_MST_TBL          curr5                            " +  "    where                                                                " +  "            main5.TARIFF_MAIN_SEA_PK    = tran5.TARIFF_MAIN_SEA_FK            " +  "     AND    main5.CARGO_TYPE            = 2                                   " +  "     AND    main5.ACTIVE                = 1                                   " +  "     AND    main5.TARIFF_TYPE           = 2                                   " +  "     AND    tran5.FREIGHT_ELEMENT_MST_FK = frt5.FREIGHT_ELEMENT_MST_PK(+)     " +  "     --AND    frt5.CHARGE_BASIS           <> 2                                  " +  "     AND    tran5.CURRENCY_MST_FK       = curr5.CURRENCY_MST_PK(+)            " +  "     AND    main5.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "        " +  "     AND    (tran5.POL_GRP_FK, tran5.POD_GRP_FK,                              " +  "                                   tran5.LCL_BASIS )                          " +  "              in ( " + Convert.ToString(SectorContainers) + " )                           " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            tran5.VALID_FROM   AND   NVL(tran5.VALID_TO,NULL_DATE_FORMAT)       " +  "     AND    frt5.FREIGHT_ELEMENT_MST_PK IN (SELECT PARM.FRT_BOF_FK  FROM PARAMETERS_TBL PARM)" +  "     UNION ALL " +  "    Select            " +  "     main5.TARIFF_REF_NO                        REF_NO,              " +  "     tran5.POL_GRP_FK                           POL_PK,              " +  "     tran5.POD_GRP_FK                           POD_PK,              " +  "     tran5.LCL_BASIS                            LCLBASIS,            " +  "     tran5.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt5.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt5.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frt5.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, " +  "     DECODE(tran5.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     'false'  ADVATOS,                                               " +  "     tran5.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr5.CURRENCY_ID                          CURR_ID,             " +  "     tran5.LCL_TARIFF_MIN_RATE                  MIN_RATE,            " +  "     tran5.LCL_TARIFF_RATE                      RATE,                 " +  "     tran5.LCL_TARIFF_RATE                      FINAL_RATE                 " +  "    from                                                             " +  "     TARIFF_MAIN_SEA_TBL            main5,                           " +  "     TARIFF_TRN_SEA_FCL_LCL         tran5,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt5,                            " +  "     CURRENCY_TYPE_MST_TBL          curr5                            " +  "    where                                                                " +  "            main5.TARIFF_MAIN_SEA_PK    = tran5.TARIFF_MAIN_SEA_FK            " +  "     AND    main5.CARGO_TYPE            = 2                                   " +  "     AND    main5.ACTIVE                = 1                                   " +  "     AND    main5.TARIFF_TYPE           = 2                                   " +  "     AND    tran5.FREIGHT_ELEMENT_MST_FK = frt5.FREIGHT_ELEMENT_MST_PK(+)     " +  "     --AND    frt5.CHARGE_BASIS           <> 2                                  " +  "     AND    tran5.CURRENCY_MST_FK       = curr5.CURRENCY_MST_PK(+)            " +  "     AND    main5.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "        " +  "     AND    (tran5.POL_GRP_FK, tran5.POD_GRP_FK,                              " +  "                                   tran5.LCL_BASIS )                          " +  "              in ( " + Convert.ToString(SectorContainers) + " )                           " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            tran5.VALID_FROM   AND   NVL(tran5.VALID_TO,NULL_DATE_FORMAT)       " +  "     AND    frt5.FREIGHT_ELEMENT_MST_PK NOT IN (SELECT PARM.FRT_BOF_FK  FROM PARAMETERS_TBL PARM)) Q," +  "            FREIGHT_ELEMENT_MST_TBL FRT        " +  "            WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FRT_PK       " +  "            ORDER BY FRT.PREFERENCE        ";
                    }
                }
                else
                {
                    if (forFCL)
                    {
                        strSQL = "    SELECT Q.* FROM (Select            " +  "     main5.TARIFF_REF_NO                        REF_NO,              " +  "     tran5.PORT_MST_POL_FK                      POL_PK,              " +  "     tran5.PORT_MST_POD_FK                      POD_PK,              " +  "     cont5.CONTAINER_TYPE_MST_FK                CNTR_PK,             " +  "     tran5.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt5.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt5.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frt5.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, " +  "     DECODE(tran5.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     'false'  ADVATOS,                                               " +  "     tran5.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr5.CURRENCY_ID                          CURR_ID,             " +  "     cont5.FCL_REQ_RATE                         QUOTERATE,                " +  "     NULL                                       FINAL_RATE,           " +  "     NULL                                       PYTYPE               " +  "    from                                                             " +  "     TARIFF_MAIN_SEA_TBL            main5,                           " +  "     TARIFF_TRN_SEA_FCL_LCL         tran5,                           ";
                        //& vbCrLf _
                        //Modified by Snigdharani - 29/10/2008 - Removing v-array
                        //TABLE(tran5.CONTAINER_DTL_FCL) (+) cont5
                        strSQL = strSQL + "     TARIFF_TRN_SEA_CONT_DTL cont5,                       " +  "     FREIGHT_ELEMENT_MST_TBL        frt5,                            " +  "     CURRENCY_TYPE_MST_TBL          curr5                            " +  "    where                                                                " +  "            main5.TARIFF_MAIN_SEA_PK    = tran5.TARIFF_MAIN_SEA_FK            " +  "     AND    main5.CARGO_TYPE            = 1                                   " +  "     AND    main5.ACTIVE                = 1                                   " +  "     AND    main5.TARIFF_TYPE           = 2                                   " +  "     AND    tran5.FREIGHT_ELEMENT_MST_FK = frt5.FREIGHT_ELEMENT_MST_PK(+)     " +  "     AND    cont5.TARIFF_TRN_SEA_FK = tran5.TARIFF_TRN_SEA_PK     " +  "     --AND    frt5.CHARGE_BASIS           <> 2                                  " +  "     AND    tran5.CURRENCY_MST_FK       = curr5.CURRENCY_MST_PK(+)            " +  "     AND    main5.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "        " +  "     AND    (tran5.PORT_MST_POL_FK, tran5.PORT_MST_POD_FK,                    " +  "                                   cont5.CONTAINER_TYPE_MST_FK )              " +  "              in ( " + Convert.ToString(SectorContainers) + " )                           " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            tran5.VALID_FROM   AND   NVL(tran5.VALID_TO,NULL_DATE_FORMAT)) Q,        " +  "            FREIGHT_ELEMENT_MST_TBL FRT        " +  "            WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FRT_PK       " +  "            ORDER BY FRT.PREFERENCE        ";
                        //Added by Rabbani raeson USS Gap,introduced New column as "MIN_RATE"
                    }
                    else
                    {
                        //Added by rabbani on 24/3/07 ,To display BOF as first element in Freight Element Column using UNION ALL.
                        strSQL = "    SELECT Q.* FROM (Select            " +  "     main5.TARIFF_REF_NO                        REF_NO,              " +  "     tran5.PORT_MST_POL_FK                      POL_PK,              " +  "     tran5.PORT_MST_POD_FK                      POD_PK,              " +  "     tran5.LCL_BASIS                            LCLBASIS,            " +  "     tran5.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt5.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt5.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frt5.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, " +  "     DECODE(tran5.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     'false'  ADVATOS,                                               " +  "     tran5.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr5.CURRENCY_ID                          CURR_ID,             " +  "     tran5.LCL_TARIFF_MIN_RATE                  MIN_RATE,            " +  "     tran5.LCL_TARIFF_RATE                      QUOTERATE,                 " +  "     tran5.LCL_TARIFF_RATE                      FINAL_RATE                 " +  "    from                                                             " +  "     TARIFF_MAIN_SEA_TBL            main5,                           " +  "     TARIFF_TRN_SEA_FCL_LCL         tran5,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt5,                            " +  "     CURRENCY_TYPE_MST_TBL          curr5                            " +  "    where                                                                " +  "            main5.TARIFF_MAIN_SEA_PK    = tran5.TARIFF_MAIN_SEA_FK            " +  "     AND    main5.CARGO_TYPE            = 2                                   " +  "     AND    main5.ACTIVE                = 1                                   " +  "     AND    main5.TARIFF_TYPE           = 2                                   " +  "     AND    tran5.FREIGHT_ELEMENT_MST_FK = frt5.FREIGHT_ELEMENT_MST_PK(+)     " +  "     --AND    frt5.CHARGE_BASIS           <> 2                                  " +  "     AND    tran5.CURRENCY_MST_FK       = curr5.CURRENCY_MST_PK(+)            " +  "     AND    main5.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "        " +  "     AND    (tran5.PORT_MST_POL_FK, tran5.PORT_MST_POD_FK,                    " +  "                                   tran5.LCL_BASIS )                          " +  "              in ( " + Convert.ToString(SectorContainers) + " )                           " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            tran5.VALID_FROM   AND   NVL(tran5.VALID_TO,NULL_DATE_FORMAT)       " +  "     AND    frt5.FREIGHT_ELEMENT_MST_PK IN (SELECT PARM.FRT_BOF_FK  FROM PARAMETERS_TBL PARM)" +  "     UNION ALL " +  "    Select            " +  "     main5.TARIFF_REF_NO                        REF_NO,              " +  "     tran5.PORT_MST_POL_FK                      POL_PK,              " +  "     tran5.PORT_MST_POD_FK                      POD_PK,              " +  "     tran5.LCL_BASIS                            LCLBASIS,            " +  "     tran5.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " +  "     frt5.FREIGHT_ELEMENT_ID                    FRT_ID,              " +  "     frt5.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " +  "     DECODE(frt5.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, " +  "     DECODE(tran5.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " +  "     'false'  ADVATOS,                                               " +  "     tran5.CURRENCY_MST_FK                      CURR_PK,             " +  "     curr5.CURRENCY_ID                          CURR_ID,             " +  "     tran5.LCL_TARIFF_MIN_RATE                  MIN_RATE,            " +  "     tran5.LCL_TARIFF_RATE                      RATE,                 " +  "     tran5.LCL_TARIFF_RATE                      FINAL_RATE                 " +  "    from                                                             " +  "     TARIFF_MAIN_SEA_TBL            main5,                           " +  "     TARIFF_TRN_SEA_FCL_LCL         tran5,                           " +  "     FREIGHT_ELEMENT_MST_TBL        frt5,                            " +  "     CURRENCY_TYPE_MST_TBL          curr5                            " +  "    where                                                                " +  "            main5.TARIFF_MAIN_SEA_PK    = tran5.TARIFF_MAIN_SEA_FK            " +  "     AND    main5.CARGO_TYPE            = 2                                   " +  "     AND    main5.ACTIVE                = 1                                   " +  "     AND    main5.TARIFF_TYPE           = 2                                   " +  "     AND    tran5.FREIGHT_ELEMENT_MST_FK = frt5.FREIGHT_ELEMENT_MST_PK(+)     " +  "     --AND    frt5.CHARGE_BASIS           <> 2                                  " +  "     AND    tran5.CURRENCY_MST_FK       = curr5.CURRENCY_MST_PK(+)            " +  "     AND    main5.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup) + "        " +  "     AND    (tran5.PORT_MST_POL_FK, tran5.PORT_MST_POD_FK,                    " +  "                                   tran5.LCL_BASIS )                          " +  "              in ( " + Convert.ToString(SectorContainers) + " )                           " +  "     AND    ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5) BETWEEN       " +  "            tran5.VALID_FROM   AND   NVL(tran5.VALID_TO,NULL_DATE_FORMAT)       " +  "     AND    frt5.FREIGHT_ELEMENT_MST_PK NOT IN (SELECT PARM.FRT_BOF_FK  FROM PARAMETERS_TBL PARM)) Q," +  "            FREIGHT_ELEMENT_MST_TBL FRT        " +  "            WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FRT_PK       " +  "            ORDER BY FRT.PREFERENCE        ";
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
            string CustPK = "";
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
            if (arr.Length > 5)
                CustPK = Convert.ToString(arr.GetValue(5));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_BOOKING_PKG.GET_BOOKING_ENQUIRY_SEA";
                var _with6 = SCM.Parameters;
                _with6.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with6.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with6.Add("CARGO_TYPE_IN", CARGO).Direction = ParameterDirection.Input;
                _with6.Add("QUOTEDATE_IN", QUOTEDATE).Direction = ParameterDirection.Input;
                //Manoharan 14Sep07:Enquiry popup should display location based
                _with6.Add("LogLoc_IN", Loc).Direction = ParameterDirection.Input;
                _with6.Add("CUSTOMER_PK_IN", (string.IsNullOrEmpty(CustPK) ? "" : CustPK)).Direction = ParameterDirection.Input;
                //.Add("RETURN_VALUE", OracleDbType.NVarChar, 2000, "RETURN_VALUE").Direction = ParameterDirection.Output
                _with6.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                //Manoharan 14Sep07:Enquiry popup should display location based
                // strReturn = CStr(SCM.Parameters["RETURN_VALUE").Value).Trim
                OracleClob clob = default(OracleClob);
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
                //strCustQuery.Append("SELECT c.CREDIT_DAYS," & vbCrLf)
                //strCustQuery.Append(" c.CREDIT_LIMIT" & vbCrLf)
                strCustQuery.Append("SELECT C.SEA_CREDIT_DAYS,");
                strCustQuery.Append(" C.SEA_CREDIT_LIMIT");
                strCustQuery.Append("FROM customer_mst_tbl c");
                strCustQuery.Append("WHERE c.customer_mst_pk=" + CustPk);
                OracleDataReader dr = default(OracleDataReader);
                if (type == 1)
                {
                    strQuery.Append("SELECT C.CREDIT_PERIOD");
                    strQuery.Append("  FROM ENQUIRY_BKG_SEA_TBL     EB,");
                    strQuery.Append("       ENQUIRY_TRN_SEA_FCL_LCL ET,");
                    strQuery.Append("       CONT_CUST_SEA_TBL       C");
                    strQuery.Append("       WHERE");
                    strQuery.Append("       ET.TRANS_REFERED_FROM=2");
                    strQuery.Append("       AND Eb.ENQUIRY_REF_NO='" + Pk + "'");
                    strQuery.Append("       AND EB.ENQUIRY_BKG_SEA_PK=ET.ENQUIRY_MAIN_SEA_FK");
                    strQuery.Append("       AND ET.TRANS_REF_NO=C.CONT_REF_NO");
                    strQuery.Append("     AND C.CUSTOMER_MST_FK=" + CustPk);
                }
                else if (type == 2)
                {
                    strQuery.Append("SELECT Q.CREDIT_DAYS");
                    //strQuery.Append("     Q.CREDIT_LIMIT " & vbCrLf)
                    strQuery.Append("     FROM QUOTATION_MST_TBL Q");
                    strQuery.Append("     WHERE Q.QUOTATION_MST_PK=" + Pk);
                    strQuery.Append("     AND Q.CUSTOMER_MST_FK=" + CustPk);
                }
                else if (type == 3)
                {
                    strQuery.Append("SELECT C.CREDIT_PERIOD FROM cont_cust_sea_tbl C  ");
                    strQuery.Append("WHERE C.CONT_CUST_SEA_PK=" + Pk);
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
        public ArrayList SaveQuotation(DataTable HDT, DataTable PDT, DataTable CDT, DataTable OthDT, string QuoteNo, string QuotePk, long nLocationId, long nEmpId, Int16 CargoType, string remarks = "",
        string CargoMoveCode = "", string Header = "", string Footer = "", Int16 cargo = 1, int PYMTType = 0, int INCOTerms = 0, Int16 Group = 0, bool AmendFlg = false, Int16 chkAIF = 0, int From_Flag = 0,
        bool Customer_Approved = false)
        {

            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = default(OracleTransaction);
            //Dim TRAN1 As OracleTransaction
            bool chkFlag = false;
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            //TRAN1 = objWK.MyConnection.BeginTransaction()
            arrMessage.Clear();
            objWK.MyCommand.Transaction = TRAN;
            string strExtendedQTN = null;
            Int16 nIndex = default(Int16);
            string PrevQuotPK = null;
            try
            {
                if (AmendFlg == true)
                {
                    PrevQuotPK = QuotePk;
                    QuotePk = "";
                }
                if (string.IsNullOrEmpty(QuoteNo))
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
                if (!string.IsNullOrEmpty(QuotePk))
                {
                    _PkValue = Convert.ToInt32(QuotePk);
                }
                //end
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
                        str = " update QUOTATION_MST_TBL QT SET QT.STATUS = 3 ,";
                        str += " QT.AMEND_FLAG = 1 ";
                        str += " WHERE QT.QUOTATION_MST_PK=" + PrevQuotPK;
                    }
                    else
                    {
                        Span = DateTime.Today.Subtract(QuotDate);
                        ValidFor = Span.Days;
                        str = " update QUOTATION_MST_TBL QT SET QT.EXPECTED_SHIPMENT_DT = SYSDATE ,";
                        str += " QT.AMEND_FLAG = 1 ,";
                        str += " QT.VALID_FOR = " + ValidFor;
                        str += " WHERE QT.QUOTATION_MST_PK=" + PrevQuotPK;
                    }
                    var _with7 = updCmdUser;
                    _with7.Connection = objWK.MyConnection;
                    _with7.Transaction = TRAN;
                    _with7.CommandType = CommandType.Text;
                    _with7.CommandText = str;
                    intIns = _with7.ExecuteNonQuery();
                }
                //'
                if (string.IsNullOrEmpty(QuotePk))
                {
                    var _with8 = objWK.MyCommand;
                    _with8.CommandType = CommandType.StoredProcedure;
                    _with8.CommandText = objWK.MyUserName + ".QUOTATION_SEA_TBL_PKG.QUOTATION_SEA_TBL_INS";
                    _with8.Parameters.Clear();

                    _with8.Parameters.Add("QUOTATION_REF_NO_IN", Convert.ToString(QuoteNo)).Direction = ParameterDirection.Input;
                    _with8.Parameters["QUOTATION_REF_NO_IN"].SourceVersion = DataRowVersion.Current;

                    _with8.Parameters.Add("QUOTATION_DATE_IN", Convert.ToDateTime(HDT.Rows[0]["QUOTATION_DATE"])).Direction = ParameterDirection.Input;
                    _with8.Parameters["QUOTATION_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    _with8.Parameters.Add("VALID_FOR_IN", HDT.Rows[0]["VALID_FOR"]).Direction = ParameterDirection.Input;
                    _with8.Parameters["VALID_FOR_IN"].SourceVersion = DataRowVersion.Current;

                    _with8.Parameters.Add("CARGO_TYPE_IN", HDT.Rows[0]["CARGO_TYPE"]).Direction = ParameterDirection.Input;
                    _with8.Parameters["CARGO_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with8.Parameters.Add("PYMT_TYPE_IN", PYMTType).Direction = ParameterDirection.Input;
                    _with8.Parameters["PYMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with8.Parameters.Add("QUOTED_BY_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                    _with8.Parameters["QUOTED_BY_IN"].SourceVersion = DataRowVersion.Current;

                    _with8.Parameters.Add("COL_PLACE_MST_FK_IN", HDT.Rows[0]["COL_PLACE_MST_FK"]).Direction = ParameterDirection.Input;
                    _with8.Parameters["COL_PLACE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with8.Parameters.Add("COL_ADDRESS_IN", getDefault(HDT.Rows[0]["COL_ADDRESS"], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with8.Parameters["COL_ADDRESS_IN"].SourceVersion = DataRowVersion.Current;

                    _with8.Parameters.Add("DEL_PLACE_MST_FK_IN", HDT.Rows[0]["DEL_PLACE_MST_FK"]).Direction = ParameterDirection.Input;
                    _with8.Parameters["DEL_PLACE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with8.Parameters.Add("DEL_ADDRESS_IN", getDefault(HDT.Rows[0]["DEL_ADDRESS"], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with8.Parameters["DEL_ADDRESS_IN"].SourceVersion = DataRowVersion.Current;

                    _with8.Parameters.Add("AGENT_MST_FK_IN", HDT.Rows[0]["AGENT_MST_FK"]).Direction = ParameterDirection.Input;
                    _with8.Parameters["AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with8.Parameters.Add("STATUS_IN", HDT.Rows[0]["STATUS"]).Direction = ParameterDirection.Input;
                    _with8.Parameters["STATUS_IN"].SourceVersion = DataRowVersion.Current;

                    ///'
                    _with8.Parameters.Add("EXPECTED_SHIPMENT_DT_IN", Convert.ToDateTime(HDT.Rows[0]["EXPECTED_SHIPMENT_DT"])).Direction = ParameterDirection.Input;
                    _with8.Parameters["EXPECTED_SHIPMENT_DT_IN"].SourceVersion = DataRowVersion.Current;

                    _with8.Parameters.Add("CUSTOMER_MST_FK_IN", HDT.Rows[0]["CUSTOMER_MST_FK"]).Direction = ParameterDirection.Input;
                    _with8.Parameters["CUSTOMER_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with8.Parameters.Add("CUSTOMER_CATEGORY_MST_FK_IN", HDT.Rows[0]["CUSTOMER_CATEGORY_MST_FK"]).Direction = ParameterDirection.Input;
                    _with8.Parameters["CUSTOMER_CATEGORY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with8.Parameters.Add("SPECIAL_INSTRUCTIONS_IN", getDefault(HDT.Rows[0]["SPECIAL_INSTRUCTIONS"], 0)).Direction = ParameterDirection.Input;
                    _with8.Parameters["SPECIAL_INSTRUCTIONS_IN"].SourceVersion = DataRowVersion.Current;

                    _with8.Parameters.Add("CUST_TYPE_IN", getDefault(HDT.Rows[0]["CUST_TYPE"], 0)).Direction = ParameterDirection.Input;
                    _with8.Parameters["CUST_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with8.Parameters.Add("CREDIT_LIMIT_IN", HDT.Rows[0]["CREDIT_LIMIT"]).Direction = ParameterDirection.Input;
                    _with8.Parameters["CREDIT_LIMIT_IN"].SourceVersion = DataRowVersion.Current;

                    _with8.Parameters.Add("CREDIT_DAYS_IN", HDT.Rows[0]["CREDIT_DAYS"]).Direction = ParameterDirection.Input;
                    _with8.Parameters["CREDIT_DAYS_IN"].SourceVersion = DataRowVersion.Current;


                    _with8.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                    _with8.Parameters["CREATED_BY_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with8.Parameters.Add("CONFIG_PK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                    _with8.Parameters["CONFIG_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with8.Parameters.Add("REMARKS_IN", getDefault(remarks, DBNull.Value)).Direction = ParameterDirection.Input;
                    _with8.Parameters["REMARKS_IN"].SourceVersion = DataRowVersion.Current;

                    _with8.Parameters.Add("CARGO_MOVE_CODE_IN", getDefault(CargoMoveCode, DBNull.Value)).Direction = ParameterDirection.Input;
                    _with8.Parameters["CARGO_MOVE_CODE_IN"].SourceVersion = DataRowVersion.Current;

                    _with8.Parameters.Add("Header_IN", getDefault(Header, DBNull.Value)).Direction = ParameterDirection.Input;
                    _with8.Parameters["Header_IN"].SourceVersion = DataRowVersion.Current;

                    _with8.Parameters.Add("Footer_IN", getDefault(Footer, DBNull.Value)).Direction = ParameterDirection.Input;
                    _with8.Parameters["Footer_IN"].SourceVersion = DataRowVersion.Current;

                    _with8.Parameters.Add("BASE_CURRENCY_FK_IN", getDefault(HDT.Rows[0]["BASE_CURRENCY_FK"], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with8.Parameters["BASE_CURRENCY_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with8.Parameters.Add("PORT_GROUP_IN", getDefault(HDT.Rows[0]["PORT_GROUP"], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with8.Parameters["PORT_GROUP_IN"].SourceVersion = DataRowVersion.Current;

                    _with8.Parameters.Add("AIF_FLAG_IN", getDefault(chkAIF, 0)).Direction = ParameterDirection.Input;
                    _with8.Parameters["AIF_FLAG_IN"].SourceVersion = DataRowVersion.Current;

                    _with8.Parameters.Add("FROM_FLAG_IN", getDefault(From_Flag, 0)).Direction = ParameterDirection.Input;
                    _with8.Parameters["FROM_FLAG_IN"].SourceVersion = DataRowVersion.Current;

                    _with8.Parameters.Add("CUSTOMER_APPROVED_IN", (Customer_Approved ? 1 : 0)).Direction = ParameterDirection.Input;

                    _with8.Parameters.Add("SHIPPING_TERMS_MST_PK_IN", INCOTerms).Direction = ParameterDirection.Input;
                    _with8.Parameters["SHIPPING_TERMS_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with8.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;

                    _with8.ExecuteNonQuery();
                    _PkValue = Convert.ToInt64(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);
                }
                HDT.Rows[0]["QUOTATION_MST_PK"] = _PkValue;
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

                arrMessage = SaveTransaction(PDT, CDT, OthDT, _PkValue, objWK.MyCommand, objWK.MyUserName, CargoType, From_Flag);
                // If arrMessage.Count > 0 Then
                //     If InStr(CStr(arrMessage(0)).ToUpper, "SAVED") = 0 Then
                //         TRAN.Rollback()
                //         Return arrMessage
                //     End If
                // End If
                // arrMessage = SaveOTHFreights(OTHDT, _PkValue, objWK.MyCommand, objWK.MyUserName)

                if (arrMessage.Count > 0)
                {
                    if (string.Compare(Convert.ToString(arrMessage[0].ToString()).ToUpper(), "SAVED") > 0)
                    {
                        arrMessage.Add("All data Saved successfully");
                        TRAN.Commit();
                        QuotePk = Convert.ToString(_PkValue);
                        return arrMessage;
                    }
                    else
                    {
                        TRAN.Rollback();
                        //added by surya prasad for implementinf protocol roll back on 18-02-2009
                        if (chkFlag)
                        {
                            RollbackProtocolKey("QUOTATION (SEA)", Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt64(HttpContext.Current.Session["EMP_PK"]), QuoteNo, System.DateTime.Now);
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
                    RollbackProtocolKey("QUOTATION (SEA)", Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt64(HttpContext.Current.Session["EMP_PK"]), QuoteNo, System.DateTime.Now);
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
                    RollbackProtocolKey("QUOTATION (SEA)", Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt64(HttpContext.Current.Session["EMP_PK"]), QuoteNo, System.DateTime.Now);
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

        private ArrayList SaveOTHFreights(DataTable OTHDT, long PkValue, long OldPk, OracleCommand SCM, string UserName)
        {
            Int32 nRowCnt = default(Int32);
            DataRow DR = null;
            long FreightPk = 0;
            arrMessage.Clear();
            try
            {
                var _with9 = SCM;
                for (nRowCnt = 0; nRowCnt <= OTHDT.Rows.Count - 1; nRowCnt++)
                {
                    DR = OTHDT.Rows[nRowCnt];
                    if (Convert.ToInt64(DR[3]) == OldPk)
                    {
                        _with9.CommandType = CommandType.StoredProcedure;
                        _with9.CommandText = UserName + ".QUOTATION_SEA_TBL_PKG.QUOTATION_TRN_SEA_OTH_CHRG_INS";
                        var _with10 = _with9.Parameters;
                        _with10.Clear();
                        _with10.Add("QUOTATION_TRN_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input;
                        _with10.Add("FREIGHT_ELEMENT_MST_FK_IN", DR["FREIGHT_ELEMENT_MST_FK"]).Direction = ParameterDirection.Input;
                        _with10.Add("CURRENCY_MST_FK_IN", DR["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                        _with10.Add("AMOUNT_IN", getDefault(DR["AMOUNT"], 0)).Direction = ParameterDirection.Input;
                        _with10.Add("FREIGHT_TYPE_IN", getDefault(DR["PYMT_TYPE"], 1)).Direction = ParameterDirection.Input;
                        _with10.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        _with9.ExecuteNonQuery();
                        FreightPk = Convert.ToInt64(_with9.Parameters["RETURN_VALUE"].Value);
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

        #region " Save Transaction FCL/LCL "
        private ArrayList SaveTransaction(DataTable PDT, DataTable CDT, DataTable OthDT, long PkValue, OracleCommand SCM, string UserName, Int16 CargoType, int From_Flag = 0)
        {
            Int32 nRowCnt = default(Int32);
            DataRow DR = null;
            Int16 AllInRate = default(Int16);
            Int16 BasisType = default(Int16);
            long TransactionPK = 0;
            long OldPk = 0;
            string REF_NO = null;
            WorkFlow objWF = new WorkFlow();
            arrMessage.Clear();
            try
            {
                var _with11 = SCM;
                for (nRowCnt = 0; nRowCnt <= PDT.Rows.Count - 1; nRowCnt++)
                {
                    _with11.CommandType = CommandType.StoredProcedure;
                    _with11.CommandText = UserName + ".QUOTATION_SEA_TBL_PKG.QUOTATION_TRN_SEA_FCL_LCL_INS";
                    if (PDT.Columns.Contains("REFNO"))
                    {
                        if (!string.IsNullOrEmpty(PDT.Rows[nRowCnt]["REFNO"].ToString()) & (PDT.Rows[nRowCnt]["REFNO"] != null))
                        {
                            PDT.Rows[nRowCnt]["REF_NO"] = PDT.Rows[nRowCnt]["REFNO"];
                        }
                    }
                    DR = PDT.Rows[nRowCnt];

                    var _with12 = _with11.Parameters;
                    _with12.Clear();
                    _with12.Add("QUOTATION_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    _with12.Add("PORT_MST_POL_FK_IN", DR["POL_PK"]).Direction = ParameterDirection.Input;
                    _with12.Add("PORT_MST_POD_FK_IN", DR["POD_PK"]).Direction = ParameterDirection.Input;
                    _with12.Add("TRANS_REFERED_FROM_IN", Convert.ToInt32(SourceEnum("'" + DR["TYPE"] + "'"))).Direction = ParameterDirection.Input;
                    _with12.Add("TRANS_REF_NO_IN", DR["REF_NO"]).Direction = ParameterDirection.Input;
                    _with12.Add("OPERATOR_MST_FK_IN", getDefault(DR["OPER_PK"], DBNull.Value)).Direction = ParameterDirection.Input;
                    if (CargoType == 1)
                    {
                        _with12.Add("CONTAINER_TYPE_MST_FK_IN", DR["CNTR_PK"]).Direction = ParameterDirection.Input;
                        _with12.Add("EXPECTED_BOXES_IN", DR["QUANTITY"]).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with12.Add("CONTAINER_TYPE_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        _with12.Add("EXPECTED_BOXES_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    if (CargoType == 2)
                    {
                        _with12.Add("BASIS_IN", DR["LCL_BASIS"]).Direction = ParameterDirection.Input;
                        // BasisType = (New  WorkFlow).ExecuteScaler( _
                        //            " select DIMENTION_TYPE from dimension_unit_mst_tbl " & _
                        //            "  where DIMENTION_UNIT_MST_PK = " & DR["LCL_BASIS"))
                    }
                    else
                    {
                        _with12.Add("BASIS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }

                    _with12.Add("COMMODITY_GROUP_FK_IN", DR["COMM_GRPPK"]).Direction = ParameterDirection.Input;
                    _with12.Add("COMMODITY_MST_FK_IN", getDefault(DR["COMM_PK"], DBNull.Value)).Direction = ParameterDirection.Input;
                    //Added by Faheem
                    _with12.Add("COMMODITY_MST_FKS_IN", getDefault(DR["COMMODITY_MST_FKS"], DBNull.Value)).Direction = ParameterDirection.Input;
                    //End
                    _with12.Add("ALL_IN_TARIFF_IN", DR["ALL_IN_TARIFF"]).Direction = ParameterDirection.Input;
                    _with12.Add("ALL_IN_QUOTED_TARIFF_IN", DR["ALL_IN_QUOTE"]).Direction = ParameterDirection.Input;
                    if (CargoType == 2)
                    {
                        //If BasisType = 1 Then
                        _with12.Add("EXPECTED_VOLUME_IN", DR["VOLUME"]).Direction = ParameterDirection.Input;
                        _with12.Add("EXPECTED_WEIGHT_IN", DR["WEIGHT"]).Direction = ParameterDirection.Input;
                        //Else
                        //    .Add("EXPECTED_VOLUME_IN", DBNull.Value).Direction = ParameterDirection.Input
                        //    .Add("EXPECTED_WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input
                        //End If
                    }
                    else
                    {
                        _with12.Add("EXPECTED_VOLUME_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        _with12.Add("EXPECTED_WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }

                    _with12.Add("TRAN_REF_NO2_IN", DR["REF_NO2"]).Direction = ParameterDirection.Input;
                    if (getDefault(DR["TYPE2"], "0") == "0")
                    {
                        _with12.Add("REF_TYPE2_IN", DR["TYPE2"]).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with12.Add("REF_TYPE2_IN", SourceEnum(DR["TYPE2"].ToString())).Direction = ParameterDirection.Input;
                    }
                    _with12.Add("BUYING_RATE_IN", DR["OPERATOR_RATE"]).Direction = ParameterDirection.Input;
                    _with12.Add("POL_GRP_FK_IN", (string.IsNullOrEmpty(DR["POL_GRP_FK"].ToString()) ? DBNull.Value : DR["POL_GRP_FK"])).Direction = ParameterDirection.Input;
                    _with12.Add("POD_GRP_FK_IN", (string.IsNullOrEmpty(DR["POD_GRP_FK"].ToString()) ? DBNull.Value : DR["POD_GRP_FK"])).Direction = ParameterDirection.Input;
                    _with12.Add("TARIFF_GRP_FK_IN", (string.IsNullOrEmpty(DR["TARIFF_GRP_FK"].ToString()) ? DBNull.Value : DR["TARIFF_GRP_FK"])).Direction = ParameterDirection.Input;
                    _with12.Add("FROM_FLAG_IN", From_Flag).Direction = ParameterDirection.Input;
                    _with12.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with11.ExecuteNonQuery();
                    TransactionPK = Convert.ToInt64(_with11.Parameters["RETURN_VALUE"].Value);
                    OldPk = Convert.ToInt64(getDefault(DR["FK"], -1));
                    // = TransactionPK
                    arrMessage = SaveFreights(DR, CDT, TransactionPK, SCM, UserName, CargoType);
                    if (arrMessage.Count > 0)
                    {
                        if (string.Compare(Convert.ToString(arrMessage[0].ToString()).ToUpper(), "SAVED") == 0)
                        {
                            return arrMessage;
                        }
                    }
                    arrMessage = SaveOTHFreights(OthDT, TransactionPK, OldPk, SCM, UserName);
                    if (arrMessage.Count > 0)
                    {
                        if (string.Compare(Convert.ToString(arrMessage[0].ToString()).ToUpper(), "SAVED") == 0)
                        {
                            return arrMessage;
                        }
                    }
                    //added by vimlesh kumar 26/08/2006 for adding special request
                    string CntrType = null;
                    if (CargoType == 2)
                    {
                        CntrType = (string.IsNullOrEmpty(DR.ItemArray[12].ToString()) ? "0" : DR.ItemArray[12].ToString());
                    }
                    else
                    {
                        CntrType = DR.ItemArray[13].ToString();
                    }
                    int i = 0;
                    string strSql = null;
                    string drCntKind = null;
                    strSql = "SELECT C.CONTAINER_KIND FROM CONTAINER_TYPE_MST_TBL C WHERE C.CONTAINER_TYPE_MST_ID= '" + CntrType + "'";
                    drCntKind = objWF.ExecuteScaler(strSql);
                    if (Convert.ToInt64(DR["COMM_GRPPK"]) == HAZARDOUS)
                    {
                        if (drCntKind == "3" | drCntKind == "4" | drCntKind == "5")
                        {
                            arrMessage = SaveTransactionODC(SCM, UserName, Convert.ToString(getDefault(DR["strSpclReq"], "")), TransactionPK);
                        }
                        else
                        {
                            arrMessage = SaveTransactionHZSpcl(SCM, UserName, Convert.ToString(getDefault(DR["strSpclReq"], "")), TransactionPK);
                        }
                    }
                    else if (Convert.ToInt64(DR["COMM_GRPPK"]) == REEFER)
                    {
                        if (drCntKind == "3" | drCntKind == "4" | drCntKind == "5")
                        {
                            arrMessage = SaveTransactionODC(SCM, UserName, Convert.ToString(getDefault(DR["strSpclReq"], "")), TransactionPK);
                        }
                        else
                        {
                            arrMessage = SaveTransactionReefer(SCM, UserName, Convert.ToString(getDefault(DR["strSpclReq"], "")), TransactionPK);
                        }
                    }
                    else if (Convert.ToInt64(DR["COMM_GRPPK"]) == ODC)
                    {
                        if (drCntKind == "3" | drCntKind == "4" | drCntKind == "5")
                        {
                            arrMessage = SaveTransactionODC(SCM, UserName, Convert.ToString(getDefault(DR["strSpclReq"], "")), TransactionPK);
                        }
                    }
                    else
                    {
                        if (drCntKind == "3" | drCntKind == "4" | drCntKind == "5")
                        {
                            arrMessage = SaveTransactionODC(SCM, UserName, Convert.ToString(getDefault(DR["strSpclReq"], "")), TransactionPK);
                        }
                    }
                    if (arrMessage.Count > 0)
                    {
                        if (string.Compare(Convert.ToString(arrMessage[0].ToString()).ToUpper(), "SAVED") == 0)
                        {
                            return arrMessage;
                        }
                    }
                    //end
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

        private void ModifyOTH(DataRow DR)
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
                    nSTR += Convert.ToInt64(arrELE.GetValue(j)) + "~";
                }
            }
        }
        private ArrayList Update_Transaction(DataTable PDT, DataTable CDT, DataTable OthDT, long PkValue, OracleCommand SCM, string UserName, Int16 CargoType, int From_Flag = 0)
        {
            Int32 nRowCnt = default(Int32);
            DataRow DR = null;
            Int16 AllInRate = default(Int16);
            Int16 BasisType = default(Int16);
            long TransactionPK = 0;
            long OldPk = 0;
            WorkFlow objWF = new WorkFlow();
            arrMessage.Clear();
            try
            {
                var _with13 = SCM;
                for (nRowCnt = 0; nRowCnt <= PDT.Rows.Count - 1; nRowCnt++)
                {
                    _with13.CommandType = CommandType.StoredProcedure;
                    _with13.CommandText = UserName + ".QUOTATION_SEA_TBL_PKG.QUOTATION_TRN_SEA_FCL_LCL_UPD";
                    DR = PDT.Rows[nRowCnt];
                    var _with14 = _with13.Parameters;
                    _with14.Clear();
                    //.Add("QUOTE_TRN_SEA_PK_IN", DR["PK")).Direction = ParameterDirection.Input
                    _with14.Add("QUOTATION_SEA_PK_IN", PkValue).Direction = ParameterDirection.Input;
                    _with14.Add("PORT_MST_POL_FK_IN", DR["POL_PK"]).Direction = ParameterDirection.Input;
                    _with14.Add("PORT_MST_POD_FK_IN", DR["POD_PK"]).Direction = ParameterDirection.Input;
                    //'UnCommened By Koteshwari PTS ID:OCT-010
                    _with14.Add("TRANS_REFERED_FROM_IN", Convert.ToInt32(SourceEnum("'" + DR["TYPE"] + "'"))).Direction = ParameterDirection.Input;
                    _with14.Add("TRANS_REF_NO_IN", DR["REF_NO"]).Direction = ParameterDirection.Input;
                    //'End
                    _with14.Add("OPERATOR_MST_FK_IN", getDefault(DR["OPER_PK"], DBNull.Value)).Direction = ParameterDirection.Input;
                    if (CargoType == 1)
                    {
                        _with14.Add("CONTAINER_TYPE_MST_FK_IN", DR["CNTR_PK"]).Direction = ParameterDirection.Input;
                        _with14.Add("EXPECTED_BOXES_IN", DR["QUANTITY"]).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with14.Add("CONTAINER_TYPE_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        _with14.Add("EXPECTED_BOXES_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    if (CargoType == 2)
                    {
                        _with14.Add("BASIS_IN", DR["LCL_BASIS"]).Direction = ParameterDirection.Input;
                        // BasisType = (New  WorkFlow).ExecuteScaler( _
                        //            " select DIMENTION_TYPE from dimension_unit_mst_tbl " & _
                        //            "  where DIMENTION_UNIT_MST_PK = " & DR["LCL_BASIS"))
                    }
                    else
                    {
                        _with14.Add("BASIS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    _with14.Add("COMMODITY_GROUP_FK_IN", DR["COMM_GRPPK"]).Direction = ParameterDirection.Input;
                    _with14.Add("COMMODITY_MST_FK_IN", getDefault(DR["COMM_PK"], DBNull.Value)).Direction = ParameterDirection.Input;
                    //Added by Faheem
                    _with14.Add("COMMODITY_MST_FKS_IN", getDefault(DR["COMMODITY_MST_FKS"], DBNull.Value)).Direction = ParameterDirection.Input;
                    //End
                    _with14.Add("ALL_IN_TARIFF_IN", DR["ALL_IN_TARIFF"]).Direction = ParameterDirection.Input;
                    _with14.Add("ALL_IN_QUOTED_TARIFF_IN", DR["ALL_IN_QUOTE"]).Direction = ParameterDirection.Input;
                    if (CargoType == 2)
                    {
                        //If BasisType = 1 Then
                        _with14.Add("EXPECTED_VOLUME_IN", DR["VOLUME"]).Direction = ParameterDirection.Input;
                        _with14.Add("EXPECTED_WEIGHT_IN", DR["WEIGHT"]).Direction = ParameterDirection.Input;
                        //Else
                        //    .Add("EXPECTED_VOLUME_IN", DBNull.Value).Direction = ParameterDirection.Input
                        //    .Add("EXPECTED_WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input
                        //End If
                    }
                    else
                    {
                        _with14.Add("EXPECTED_VOLUME_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        _with14.Add("EXPECTED_WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    //'Uncommented By Koteshwari PTS ID:OCT-010
                    _with14.Add("TRAN_REF_NO2_IN", DR["REF_NO2"]).Direction = ParameterDirection.Input;
                    if (getDefault(DR["TYPE2"], "0") == "0")
                    {
                        _with14.Add("REF_TYPE2_IN", DR["TYPE2"]).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with14.Add("REF_TYPE2_IN", SourceEnum(DR["TYPE2"].ToString())).Direction = ParameterDirection.Input;
                    }
                    //'End
                    _with14.Add("BUYING_RATE_IN", DR["OPERATOR_RATE"]).Direction = ParameterDirection.Input;
                    _with14.Add("POL_GRP_FK_IN", (string.IsNullOrEmpty(DR["POL_GRP_FK"].ToString()) ? DBNull.Value : DR["POL_GRP_FK"])).Direction = ParameterDirection.Input;
                    _with14.Add("POD_GRP_FK_IN", (string.IsNullOrEmpty(DR["POD_GRP_FK"].ToString()) ? DBNull.Value : DR["POD_GRP_FK"])).Direction = ParameterDirection.Input;
                    _with14.Add("TARIFF_GRP_FK_IN", (string.IsNullOrEmpty(DR["TARIFF_GRP_FK"].ToString()) ? DBNull.Value : DR["TARIFF_GRP_FK"])).Direction = ParameterDirection.Input;
                    _with14.Add("FROM_FLAG_IN", From_Flag).Direction = ParameterDirection.Input;
                    _with14.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with13.ExecuteNonQuery();
                    TransactionPK = Convert.ToInt64(_with13.Parameters["RETURN_VALUE"].Value);
                    arrMessage = Update_Freights(DR, CDT, TransactionPK, SCM, UserName, CargoType);
                    string CntrType = null;
                    if (CargoType == 2)
                    {
                        CntrType = (string.IsNullOrEmpty(DR.ItemArray[12].ToString()) ? "0" : DR.ItemArray[12].ToString());
                    }
                    else
                    {
                        CntrType = DR.ItemArray[13].ToString();
                    }
                    int i = 0;
                    string strSql = null;
                    string drCntKind = null;
                    strSql = "SELECT C.CONTAINER_KIND FROM CONTAINER_TYPE_MST_TBL C WHERE C.CONTAINER_TYPE_MST_ID= '" + CntrType + "'";
                    drCntKind = objWF.ExecuteScaler(strSql);
                    if (Convert.ToInt64(DR["COMM_GRPPK"] )== HAZARDOUS)
                    {
                        if (drCntKind == "3" | drCntKind == "4" | drCntKind == "5")
                        {
                            arrMessage = SaveTransactionODC(SCM, UserName, Convert.ToString(getDefault(DR["strSpclReq"], "")), TransactionPK);
                        }
                        else
                        {
                            arrMessage = SaveTransactionHZSpcl(SCM, UserName, Convert.ToString(getDefault(DR["strSpclReq"], "")), TransactionPK);
                        }
                    }
                    else if (Convert.ToInt64(DR["COMM_GRPPK"]) == REEFER)
                    {
                        if (drCntKind == "3" | drCntKind == "4" | drCntKind == "5")
                        {
                            arrMessage = SaveTransactionODC(SCM, UserName, Convert.ToString(getDefault(DR["strSpclReq"], "")), TransactionPK);
                        }
                        else
                        {
                            arrMessage = SaveTransactionReefer(SCM, UserName, Convert.ToString(getDefault(DR["strSpclReq"], "")), TransactionPK);
                        }
                    }
                    else if (Convert.ToInt64(DR["COMM_GRPPK"] )== ODC)
                    {
                        if (drCntKind == "3" | drCntKind == "4" | drCntKind == "5")
                        {
                            arrMessage = SaveTransactionODC(SCM, UserName, Convert.ToString(getDefault(DR["strSpclReq"], "")), TransactionPK);
                        }
                    }
                    else
                    {
                        if (drCntKind == "3" | drCntKind == "4" | drCntKind == "5")
                        {
                            arrMessage = SaveTransactionODC(SCM, UserName, Convert.ToString(getDefault(DR["strSpclReq"], "")), TransactionPK);
                        }
                    }
                    if (arrMessage.Count > 0)
                    {
                        if (string.Compare(Convert.ToString(arrMessage[0].ToString()).ToUpper(), "SAVED") == 0)
                        {
                            return arrMessage;
                        }
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
                    var _with15 = SCM;
                    _with15.CommandType = CommandType.StoredProcedure;
                    _with15.CommandText = UserName + ".QUOT_TRN_SEA_HAZ_SPL_REQ_PKG.QUOT_TRN_SEA_HAZ_SPL_REQ_INS";
                    var _with16 = _with15.Parameters;
                    _with16.Clear();
                    //QUOTE_TRN_SEA_FK_IN()
                    _with16.Add("QUOTE_TRN_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    //OUTER_PACK_TYPE_MST_FK_IN()
                    _with16.Add("OUTER_PACK_TYPE_MST_FK_IN", getDefault(strParam[0], DBNull.Value)).Direction = ParameterDirection.Input;
                    //INNER_PACK_TYPE_MST_FK_IN()
                    _with16.Add("INNER_PACK_TYPE_MST_FK_IN", getDefault(strParam[1], DBNull.Value)).Direction = ParameterDirection.Input;
                    //MIN_TEMP_IN()
                    _with16.Add("MIN_TEMP_IN", getDefault(strParam[2], DBNull.Value)).Direction = ParameterDirection.Input;
                    //MIN_TEMP_UOM_IN()
                    _with16.Add("MIN_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[2]) ? "0" : strParam[3]), 0)).Direction = ParameterDirection.Input;
                    //MAX_TEMP_IN()
                    _with16.Add("MAX_TEMP_IN", getDefault(strParam[4], DBNull.Value)).Direction = ParameterDirection.Input;
                    //MAX_TEMP_UOM_IN()
                    _with16.Add("MAX_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[4]) ? "0" : strParam[5]), 0)).Direction = ParameterDirection.Input;
                    //FLASH_PNT_TEMP_IN()
                    _with16.Add("FLASH_PNT_TEMP_IN", getDefault(strParam[6], DBNull.Value)).Direction = ParameterDirection.Input;
                    //FLASH_PNT_TEMP_UOM_IN()
                    _with16.Add("FLASH_PNT_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[6]) ? "0" : strParam[7]), 0)).Direction = ParameterDirection.Input;
                    //IMDG_CLASS_CODE_IN()
                    _with16.Add("IMDG_CLASS_CODE_IN", getDefault(strParam[8], DBNull.Value)).Direction = ParameterDirection.Input;
                    //UN_NO_IN()
                    _with16.Add("UN_NO_IN", getDefault(strParam[9], DBNull.Value)).Direction = ParameterDirection.Input;
                    //IMO_SURCHARGE_IN()
                    _with16.Add("IMO_SURCHARGE_IN", getDefault(strParam[10], 0)).Direction = ParameterDirection.Input;
                    //SURCHARGE_AMT_IN()
                    _with16.Add("SURCHARGE_AMT_IN", getDefault(strParam[11], 0)).Direction = ParameterDirection.Input;
                    //IS_MARINE_POLLUTANT_IN()
                    _with16.Add("IS_MARINE_POLLUTANT_IN", getDefault(strParam[12], 0)).Direction = ParameterDirection.Input;
                    //EMS_NUMBER_IN()
                    _with16.Add("EMS_NUMBER_IN", getDefault(strParam[13], DBNull.Value)).Direction = ParameterDirection.Input;
                    //RETURN_VALUE()
                    _with16.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with15.ExecuteNonQuery();
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
                    strQuery.Append("SELECT QUOTE_TRN_SEA_HAZ_SPL_PK,");
                    strQuery.Append("QUOTE_TRN_SEA_FK,");
                    strQuery.Append("OUTER_PACK_TYPE_MST_FK,");
                    strQuery.Append("INNER_PACK_TYPE_MST_FK,");
                    strQuery.Append("MIN_TEMP,");
                    strQuery.Append("MIN_TEMP_UOM,");
                    strQuery.Append("MAX_TEMP,");
                    strQuery.Append("MAX_TEMP_UOM,");
                    strQuery.Append("FLASH_PNT_TEMP,");
                    strQuery.Append("FLASH_PNT_TEMP_UOM,");
                    strQuery.Append("IMDG_CLASS_CODE,");
                    strQuery.Append("UN_NO,");
                    strQuery.Append("IMO_SURCHARGE,");
                    strQuery.Append("SURCHARGE_AMT,");
                    strQuery.Append("IS_MARINE_POLLUTANT,");
                    strQuery.Append("EMS_NUMBER FROM QUOTATION_TRN_SEA_HAZ_SPL_REQ Q");
                    //,QUOTATION_DTL_TBL QT,QUOTATION_MST_TBL QM" & vbCrLf)
                    strQuery.Append("WHERE ");
                    strQuery.Append("Q.QUOTE_TRN_SEA_FK=" + strPK);
                    //strQuery.Append("QT.QUOTATION_MST_FK=QM.QUOTATION_MST_PK" & vbCrLf)
                    //strQuery.Append("AND QT.TRANS_REF_NO='" & strRef & "'" & vbCrLf)
                    //strQuery.Append("AND QM.QUOTATION_MST_PK=" & strPK)
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
            OracleDataReader dr = default(OracleDataReader);
            string UNnr = null;
            //job_trn_spl_req()
            //Dim strQuery As String = "SELECT CO.IMDG_CLASS_CODE,CO.UN_NO,CO.COMMODITY_NAME FROM  job_trn_spl_req co WHERE co.COMMODITY_NAME='" & COM & "'"
            string strQuery = "SELECT CO.IMDG_CLASS_CODE,CO.UN_NO,CO.COMMODITY_NAME FROM commodity_mst_tbl co WHERE co.COMMODITY_NAME='" + COM + "'";
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
                    var _with17 = SCM;
                    _with17.CommandType = CommandType.StoredProcedure;
                    _with17.CommandText = UserName + ".QUOTE_SEA_REF_SPL_REQ_PKG.QUOTE_SEA_REF_SPL_REQ_INS";
                    var _with18 = _with17.Parameters;
                    _with18.Clear();
                    //QUOTE_TRN_SEA_FK_IN()
                    _with18.Add("QUOTE_TRN_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    //VENTILATION_IN()
                    _with18.Add("VENTILATION_IN", strParam[0]).Direction = ParameterDirection.Input;
                    //AIR_COOL_METHOD_IN()
                    _with18.Add("AIR_COOL_METHOD_IN", strParam[1]).Direction = ParameterDirection.Input;
                    //HUMIDITY_FACTOR_IN()
                    _with18.Add("HUMIDITY_FACTOR_IN", getDefault(strParam[2], DBNull.Value)).Direction = ParameterDirection.Input;
                    //IS_PERISHABLE_GOODS_IN()
                    _with18.Add("IS_PERISHABLE_GOODS_IN", strParam[3]).Direction = ParameterDirection.Input;
                    //MIN_TEMP_IN()
                    _with18.Add("MIN_TEMP_IN", getDefault(strParam[4], DBNull.Value)).Direction = ParameterDirection.Input;
                    //MIN_TEMP_UOM_IN()
                    _with18.Add("MIN_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[4]) ? "0" : strParam[5]), 0)).Direction = ParameterDirection.Input;
                    //MAX_TEMP_IN()
                    _with18.Add("MAX_TEMP_IN", getDefault(strParam[6], DBNull.Value)).Direction = ParameterDirection.Input;
                    //MAX_TEMP_UOM_IN()
                    _with18.Add("MAX_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[6]) ? "0" : strParam[7]), 0)).Direction = ParameterDirection.Input;
                    //PACK_TYPE_MST_FK_IN()
                    _with18.Add("PACK_TYPE_MST_FK_IN", getDefault(strParam[8], DBNull.Value)).Direction = ParameterDirection.Input;
                    //PACK_COUNT_IN()
                    _with18.Add("PACK_COUNT_IN", getDefault(strParam[9], 0)).Direction = ParameterDirection.Input;
                    //RETURN_VALUE()
                    _with18.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with17.ExecuteNonQuery();
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
                    strQuery.Append("SELECT QUOTE_SEA_REF_SPL_REQ_PK,");
                    strQuery.Append("QUOTE_TRN_SEA_FK,");
                    strQuery.Append("VENTILATION,");
                    strQuery.Append("AIR_COOL_METHOD,");
                    strQuery.Append("HUMIDITY_FACTOR,");
                    strQuery.Append("IS_PERISHABLE_GOODS,");
                    strQuery.Append("MIN_TEMP,");
                    strQuery.Append("MIN_TEMP_UOM,");
                    strQuery.Append("MAX_TEMP,");
                    strQuery.Append("MAX_TEMP_UOM,");
                    strQuery.Append("PACK_TYPE_MST_FK,");
                    strQuery.Append("Q.PACK_COUNT ");
                    strQuery.Append("FROM QUOTE_SEA_REF_SPL_REQ Q");
                    //,QUOTATION_DTL_TBL QT,QUOTATION_MST_TBL QM" & vbCrLf)
                    strQuery.Append("WHERE ");
                    strQuery.Append("Q.QUOTE_TRN_SEA_FK=" + strPK);
                    //strQuery.Append("QUOTATION_MST_TBL QM," & vbCrLf)
                    //strQuery.Append("QUOTATION_DTL_TBL QT" & vbCrLf)
                    //strQuery.Append("WHERE " & vbCrLf)
                    //strQuery.Append("Q.QUOTE_TRN_SEA_FK=QT.QUOTE_TRN_SEA_PK" & vbCrLf)
                    //strQuery.Append("AND QT.QUOTATION_MST_FK=QM.QUOTATION_MST_PK" & vbCrLf)
                    //strQuery.Append("AND QT.TRANS_REF_NO='" & strRef & "'" & vbCrLf)
                    //strQuery.Append("AND QM.QUOTATION_MST_PK=" & strPK)
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
                    strQuery.Append("SELECT ");
                    strQuery.Append("QUOTE_SEA_ODC_SPL_REQ_PK,");
                    strQuery.Append("QUOTE_TRN_SEA_FK,");
                    strQuery.Append("LENGTH,");
                    strQuery.Append("LENGTH_UOM_MST_FK,");
                    strQuery.Append("HEIGHT,");
                    strQuery.Append("HEIGHT_UOM_MST_FK,");
                    strQuery.Append("WIDTH,");
                    strQuery.Append("WIDTH_UOM_MST_FK,");
                    strQuery.Append("WEIGHT,");
                    strQuery.Append("WEIGHT_UOM_MST_FK,");
                    strQuery.Append("VOLUME,");
                    strQuery.Append("VOLUME_UOM_MST_FK,");
                    strQuery.Append("SLOT_LOSS,");
                    strQuery.Append("LOSS_QUANTITY,");
                    strQuery.Append("APPR_REQ, ");
                    strQuery.Append("STOWAGE, ");
                    strQuery.Append("HANDLING_INSTR, ");
                    strQuery.Append("LASHING_INSTR ");
                    strQuery.Append("FROM QUOTE_SEA_ODC_SPL_REQ Q");
                    //,QUOTATION_DTL_TBL QT,QUOTATION_MST_TBL QM" & vbCrLf)
                    strQuery.Append("WHERE ");
                    strQuery.Append("Q.QUOTE_TRN_SEA_FK=" + strPK);
                    //strQuery.Append("QUOTATION_MST_TBL QM," & vbCrLf)
                    //strQuery.Append("QUOTATION_DTL_TBL QT" & vbCrLf)
                    //strQuery.Append("WHERE " & vbCrLf)
                    //strQuery.Append("Q.QUOTE_TRN_SEA_FK=QT.QUOTE_TRN_SEA_PK" & vbCrLf)
                    //strQuery.Append("AND QT.QUOTATION_MST_FK=QM.QUOTATION_MST_PK" & vbCrLf)
                    //strQuery.Append("AND QT.TRANS_REF_NO='" & strRef & "'" & vbCrLf)
                    //strQuery.Append("AND QM.QUOTATION_MST_PK=" & strPK)
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
                    var _with19 = SCM;
                    _with19.CommandType = CommandType.StoredProcedure;
                    _with19.CommandText = UserName + ".QUOTE_SEA_ODC_SPL_REQ_PKG.QUOTE_SEA_ODC_SPL_REQ_INS";
                    var _with20 = _with19.Parameters;
                    _with20.Clear();
                    //QUOTE_TRN_SEA_FK_IN()
                    _with20.Add("QUOTE_TRN_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    //LENGTH_IN()
                    _with20.Add("LENGTH_IN", getDefault(strParam[0], DBNull.Value)).Direction = ParameterDirection.Input;
                    //LENGTH_UOM_MST_FK_IN()
                    _with20.Add("LENGTH_UOM_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    //HEIGHT_IN()
                    _with20.Add("HEIGHT_IN", getDefault(strParam[2], DBNull.Value)).Direction = ParameterDirection.Input;
                    //HEIGHT_UOM_MST_FK_IN()
                    _with20.Add("HEIGHT_UOM_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    //WIDTH_IN()
                    _with20.Add("WIDTH_IN", getDefault(strParam[1], 0)).Direction = ParameterDirection.Input;
                    //WIDTH_UOM_MST_FK_IN()
                    _with20.Add("WIDTH_UOM_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    //WEIGHT_IN()
                    _with20.Add("WEIGHT_IN", getDefault(strParam[3], DBNull.Value)).Direction = ParameterDirection.Input;
                    //WEIGHT_UOM_MST_FK_IN()
                    _with20.Add("WEIGHT_UOM_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    //VOLUME_IN()
                    _with20.Add("VOLUME_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    //VOLUME_UOM_MST_FK_IN()
                    _with20.Add("VOLUME_UOM_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    //SLOT_LOSS_IN()
                    _with20.Add("SLOT_LOSS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    //LOSS_QUANTITY_IN()
                    _with20.Add("LOSS_QUANTITY_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    //APPR_REQ_IN()
                    _with20.Add("APPR_REQ_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    if (Convert.ToBoolean(strParam[4]) == true)
                    {
                        _with20.Add("STOWAGE_IN", 1).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with20.Add("STOWAGE_IN", 2).Direction = ParameterDirection.Input;
                    }
                    _with20.Add("HAND_INST_IN", (string.IsNullOrEmpty(strParam[6]) ? "" : strParam[6])).Direction = ParameterDirection.Input;
                    _with20.Add("LASH_INST_IN", (string.IsNullOrEmpty(strParam[7]) ? "" : strParam[7])).Direction = ParameterDirection.Input;
                    //RETURN_VALUE()
                    _with20.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with19.ExecuteNonQuery();
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

        #region " Freight Elemenmts "

        private ArrayList SaveFreights(DataRow PDR, DataTable CDT, long TrnPKValue, OracleCommand SCM, string UserName, Int16 CargoType)
        {
            Int32 nRowCnt = default(Int32);
            DataRow DR = null;
            bool Flag = false;
            Int16 AllInRate = default(Int16);
            Int16 ChkAdvatos = default(Int16);
            long FreightPk = 0;
            arrMessage.Clear();
            try
            {
                var _with21 = SCM;
                for (nRowCnt = 0; nRowCnt <= CDT.Rows.Count - 1; nRowCnt++)
                {
                    _with21.CommandType = CommandType.StoredProcedure;
                    _with21.CommandText = UserName + ".QUOTATION_SEA_TBL_PKG.QUOTATION_TRN_SEA_FRT_DTLS_INS";
                    DR = CDT.Rows[nRowCnt];
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
                        if (DR["REF_NO"] == PDR["REF_NO"] & DR["POL_PK"] == PDR["POL_PK"] & DR["POD_PK"] == PDR["POD_PK"] & DR["LCLBASIS"] == PDR["LCL_BASIS"])
                        {
                            Flag = true;
                        }
                    }
                    //adding by thiyagarajan on 26/5/08 for combined LCL and FCL 
                    //If CargoType = 3 Then
                    //    If DR["REF_NO") = PDR["REF_NO") And _
                    //                DR["POL_PK") = PDR["POL_PK") And _
                    //                DR["POD_PK") = PDR["POD_PK") And _
                    //                DR["CNTR_PK") = PDR["CNTR_PK") _
                    //        Then
                    //        Flag = True
                    //    End If
                    //    If DR["REF_NO") = PDR["REF_NO") And _
                    //            DR["POL_PK") = PDR["POL_PK") And _
                    //            DR["POD_PK") = PDR["POD_PK") And _
                    //            DR["LCLBASIS") = PDR["LCL_BASIS") Then
                    //        Flag = True
                    //    End If
                    //End If
                    //END

                    if (Flag == true)
                    {
                        var _with22 = _with21.Parameters;
                        _with22.Clear();
                        AllInRate = Convert.ToInt16((DR["SELECTED"] == "true" ? 1 : 0));
                        ChkAdvatos = Convert.ToInt16(((string.IsNullOrEmpty(DR["ADVATOS"].ToString()) ? "false" : DR["ADVATOS"]) == "true" ? 1 : 0));
                        _with22.Add("QUOTE_TRN_SEA_FK_IN", TrnPKValue).Direction = ParameterDirection.Input;
                        _with22.Add("FREIGHT_ELEMENT_MST_FK_IN", DR["FRT_PK"]).Direction = ParameterDirection.Input;
                        _with22.Add("CHECK_FOR_ALL_IN_RT_IN", AllInRate).Direction = ParameterDirection.Input;
                        _with22.Add("CURRENCY_MST_FK_IN", DR["CURR_PK"]).Direction = ParameterDirection.Input;
                        //.Add("TARIFF_RATE_IN", DR["FINAL_RATE")).Direction = ParameterDirection.Input
                        _with22.Add("TARIFF_RATE_IN", (string.IsNullOrEmpty(DR["FINAL_RATE"].ToString()) ? 0 : DR["FINAL_RATE"])).Direction = ParameterDirection.Input;
                        //.Add("QUOTED_RATE_IN", DR["QUOTERATE")).Direction = ParameterDirection.Input
                        _with22.Add("QUOTED_RATE_IN", (string.IsNullOrEmpty(DR["QUOTERATE"].ToString()) ? 0 : DR["QUOTERATE"])).Direction = ParameterDirection.Input;
                        _with22.Add("PYMT_TYPE_IN", DR["PYTPE"]).Direction = ParameterDirection.Input;
                        _with22.Add("SURCHARGE_IN", DR["SURCHARGE"]).Direction = ParameterDirection.Input;
                        _with22.Add("CHECK_ADVATOS_IN", ChkAdvatos).Direction = ParameterDirection.Input;
                        //'Added By Koteshwari
                        if (CargoType == 1)
                        {
                            _with22.Add("QUOTED_MIN_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            //Added by rabbani reason USS Gap,introduced New Column Min.Qte.Rate
                        }
                        else
                        {
                            _with22.Add("QUOTED_MIN_RATE_IN", (string.IsNullOrEmpty(DR["QUOTE_MIN_RATE"].ToString()) ? 0 : DR["QUOTE_MIN_RATE"])).Direction = ParameterDirection.Input;
                            //Added by rabbani reason USS Gap,introduced New Column Min.Qte.Rate
                        }
                        _with22.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        _with21.ExecuteNonQuery();
                        FreightPk = Convert.ToInt64(_with21.Parameters["RETURN_VALUE"].Value);
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

        private ArrayList Update_Freights(DataRow PDR, DataTable CDT, long TrnPKValue, OracleCommand SCM, string UserName, Int16 CargoType)
        {
            Int32 nRowCnt = default(Int32);
            DataRow DR = null;
            bool Flag = false;
            Int16 AllInRate = default(Int16);
            Int16 ChkAdvatos = default(Int16);
            long FreightPk = 0;

            arrMessage.Clear();
            try
            {
                var _with23 = SCM;
                for (nRowCnt = 0; nRowCnt <= CDT.Rows.Count - 1; nRowCnt++)
                {
                    _with23.CommandType = CommandType.StoredProcedure;
                    _with23.CommandText = UserName + ".QUOTATION_SEA_TBL_PKG.QUOTATION_TRN_SEA_FRT_DTLS_UPD";
                    DR = CDT.Rows[nRowCnt];
                    Flag = false;
                    if (CargoType == 1)
                    {
                        if (!string.IsNullOrEmpty(DR["REF_NO"].ToString()))
                        {
                            if (DR["REF_NO"] == PDR["REF_NO"] & DR["POL_PK"] == PDR["POL_PK"] & DR["POD_PK"] == PDR["POD_PK"] & DR["CNTR_PK"] == PDR["CNTR_PK"])
                            {
                                Flag = true;
                            }
                        }
                        else
                        {
                            Flag = true;
                        }
                    }
                    else
                    {
                        if (!string.IsNullOrEmpty(DR["REF_NO"].ToString()))
                        {
                            if (DR["REF_NO"] == PDR["REF_NO"] & DR["POL_PK"] == PDR["POL_PK"] & DR["POD_PK"] == PDR["POD_PK"] & DR["LCLBASIS"] == PDR["LCL_BASIS"])
                            {
                                Flag = true;
                            }
                        }
                        else
                        {
                            Flag = true;
                        }
                    }
                    if (Flag == true)
                    {
                        var _with24 = _with23.Parameters;
                        _with24.Clear();
                        AllInRate = Convert.ToInt16((DR["SELECTED"] == "true" ? 1 : 0));
                        ChkAdvatos = Convert.ToInt16(((string.IsNullOrEmpty(DR["ADVATOS"].ToString()) ? "false" : DR["ADVATOS"]) == "true" ? 1 : 0));
                        _with24.Add("QUOTE_TRN_SEA_FK_IN", TrnPKValue).Direction = ParameterDirection.Input;
                        _with24.Add("FREIGHT_ELEMENT_MST_FK_IN", DR["FRT_PK"]).Direction = ParameterDirection.Input;
                        _with24.Add("CHECK_FOR_ALL_IN_RT_IN", AllInRate).Direction = ParameterDirection.Input;
                        _with24.Add("CURRENCY_MST_FK_IN", DR["CURR_PK"]).Direction = ParameterDirection.Input;
                        //.Add("TARIFF_RATE_IN", DR["FINAL_RATE")).Direction = ParameterDirection.Input
                        _with24.Add("TARIFF_RATE_IN", (string.IsNullOrEmpty(DR["FINAL_RATE"].ToString()) ? 0 : DR["FINAL_RATE"])).Direction = ParameterDirection.Input;
                        //.Add("QUOTED_RATE_IN", DR["QUOTERATE")).Direction = ParameterDirection.Input
                        _with24.Add("QUOTED_RATE_IN", (string.IsNullOrEmpty(DR["QUOTERATE"].ToString()) ? 0 : DR["QUOTERATE"])).Direction = ParameterDirection.Input;
                        _with24.Add("PYMT_TYPE_IN", DR["PYTPE"]).Direction = ParameterDirection.Input;
                        _with24.Add("SURCHARGE_IN", DR["SURCHARGE"]).Direction = ParameterDirection.Input;
                        _with24.Add("CHECK_ADVATOS_IN", ChkAdvatos).Direction = ParameterDirection.Input;
                        //'Added By Koteshwari
                        if (CargoType == 1)
                        {
                            //.Add("QUOTED_MIN_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input 'Added by rabbani reason USS Gap,introduced New Column Min.Qte.Rate
                            _with24.Add("QUOTED_MIN_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            //Added by rabbani reason USS Gap,introduced New Column Min.Qte.Rate
                        }
                        else
                        {
                            _with24.Add("QUOTED_MIN_RATE_IN", (string.IsNullOrEmpty(DR["QUOTE_MIN_RATE"].ToString()) ? 0 : DR["QUOTE_MIN_RATE"])).Direction = ParameterDirection.Input;
                            //Added by rabbani reason USS Gap,introduced New Column Min.Qte.Rate
                        }
                        _with24.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        _with23.ExecuteNonQuery();
                        FreightPk = Convert.ToInt64(_with23.Parameters["RETURN_VALUE"].Value);
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
                functionReturnValue = GenerateProtocolKey("QUOTATION (SEA)", nLocationId, nEmployeeId, DateTime.Now,"" ,"" ,"" , nCreatedBy, ObjWK);
                //'Import
            }
            else
            {
                functionReturnValue = GenerateProtocolKey("IMPORT QUOTATION SEA", nLocationId, nEmployeeId, DateTime.Now,"" ,"" ,"" , nCreatedBy, ObjWK);
            }
            return functionReturnValue;
        }

        #endregion

        #endregion

        #region " Get Address of Quotation "

        public DataTable GetAddressOfQuotation(long QuotationPK)
        {
            string strSQL = null;
            strSQL = " Select COL_PLACE_MST_FK, col.PLACE_CODE colplace, CMT.COL_ADDRESS," + " DEL_PLACE_MST_FK, del.PLACE_CODE delplace,CMT.DEL_ADDRESS " + " from PLACE_MST_TBL col, PLACE_MST_TBL del, QUOTATION_MST_TBL q ,CUSTOMER_MST_TBL CMT" + " where q.COL_PLACE_MST_FK = col.PLACE_PK(+) and " + "       q.DEL_PLACE_MST_FK = del.PLACE_PK(+) and " + "       Q.CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK and " + "       QUOTATION_MST_PK = " + QuotationPK;
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
        public ArrayList UpdateQuotation(DataTable HDT, DataTable PDT, DataTable CDT, DataTable OthDT, string QuotationPk, string ValidFor, Int16 CargoType, string Status, string ExpectedShipmentDate, string remarks,
        string CargoMoveCode, string Header = "", string Footer = "", Int32 updation = 0, int PYMTType = 0, int INCOTerms = 0, Int32 chkAIF = 0, int From_Flag = 0, bool Customer_Approved = false)
        {


            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = default(OracleTransaction);
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
                    var _with25 = objWK.MyCommand;
                    _with25.CommandType = CommandType.StoredProcedure;
                    _with25.CommandText = objWK.MyUserName + ".QUOTATION_SEA_TBL_PKG.QUOTATION_SEA_TBL_UPD";
                    var _with26 = _with25.Parameters;
                    //.Clear()
                    // OracleDbType.Int32, 10
                    _with26.Add("QUOTATION_SEA_PK_IN", QuotationPk).Direction = ParameterDirection.Input;
                    _with26.Add("VALID_FOR_IN", ValidFor).Direction = ParameterDirection.Input;
                    _with26.Add("STATUS_IN", Status).Direction = ParameterDirection.Input;
                    _with26.Add("EXPECTED_SHIPMENT_DT_IN", ExpectedShipmentDate).Direction = ParameterDirection.Input;
                    _with26.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                    _with26.Add("VERSION_NO_IN", Version_No).Direction = ParameterDirection.Input;
                    _with26.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                    _with26.Add("COL_PLACE_MST_FK_IN", OracleDbType.Int32, 10).Direction = ParameterDirection.Input;
                    _with26.Add("COL_ADDRESS_IN", OracleDbType.Varchar2, 200).Direction = ParameterDirection.Input;
                    _with26.Add("DEL_PLACE_MST_FK_IN", OracleDbType.Int32, 10).Direction = ParameterDirection.Input;
                    _with26.Add("DEL_ADDRESS_IN", OracleDbType.Varchar2, 200).Direction = ParameterDirection.Input;
                    //Added by Venkata on 14/12/06
                    _with26.Add("REMARKS_IN", OracleDbType.Varchar2, 250).Direction = ParameterDirection.Input;
                    _with26.Add("CARGO_MOVE_CODE_IN", OracleDbType.Int32, 10).Direction = ParameterDirection.Input;
                    //End
                    //added by gopi
                    _with26.Add("Header_IN", OracleDbType.Varchar2, 2000).Direction = ParameterDirection.Input;
                    _with26.Add("Footer_IN", OracleDbType.Varchar2, 2000).Direction = ParameterDirection.Input;
                    //end
                    _with26.Add("SHIPPING_TERMS_MST_PK_IN", INCOTerms).Direction = ParameterDirection.Input;
                    _with26.Add("AIF_FLAG_IN", chkAIF).Direction = ParameterDirection.Input;
                    _with26.Add("PYMTType_IN", PYMTType).Direction = ParameterDirection.Input;
                    _with26.Add("FROM_FLAG_IN", getDefault(From_Flag, 0)).Direction = ParameterDirection.Input;
                    _with26.Add("CUSTOMER_APPROVED_IN", (Customer_Approved ? 1 : 0)).Direction = ParameterDirection.Input;
                    _with26.Add("RETURN_VALUE", OracleDbType.Varchar2, 20, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with25.Parameters["COL_PLACE_MST_FK_IN"].Value = getDefault(HDT.Rows[0]["COL_PLACE_MST_FK"], DBNull.Value);
                    _with25.Parameters["COL_ADDRESS_IN"].Value = getDefault(HDT.Rows[0]["COL_ADDRESS"], DBNull.Value);
                    // = "", DBNull.Value, HDT.Rows(0)["COL_ADDRESS") = "")
                    _with25.Parameters["DEL_PLACE_MST_FK_IN"].Value = getDefault(HDT.Rows[0]["DEL_PLACE_MST_FK"], DBNull.Value);
                    _with25.Parameters["DEL_ADDRESS_IN"].Value = getDefault(HDT.Rows[0]["DEL_ADDRESS"], DBNull.Value);
                    _with25.Parameters["REMARKS_IN"].Value = getDefault(remarks, DBNull.Value);
                    //IIf(remarks = "", DBNull.Value, remarks)
                    _with25.Parameters["CARGO_MOVE_CODE_IN"].Value = getDefault(CargoMoveCode, DBNull.Value);
                    _with25.Parameters["Header_IN"].Value = getDefault(Header, DBNull.Value);
                    _with25.Parameters["Footer_IN"].Value = getDefault(Footer, DBNull.Value);

                    _with25.ExecuteNonQuery();
                    _PkValue = Convert.ToInt64(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);
                }
                //modified by Thiyagarajan on 1/7/08 for fcl and lcl quotation task
                if (updation > 1)
                {
                    _PkValue = Convert.ToInt64(QuotationPk);
                }
                arrMessage = Update_Transaction(PDT, CDT, OthDT, _PkValue, objWK.MyCommand, objWK.MyUserName, CargoType, From_Flag);

                if (prvstatus != 2 & prvstatus != 3)
                {
                    updateOthCharge(PDT, OthDT, objWK.MyCommand, objWK.MyUserName);
                }
                if (arrMessage.Count > 0)
                {
                    if (string.Compare(arrMessage[0].ToString(), "saved") > 0)
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
            OracleDataReader dr = default(OracleDataReader);
            try
            {
                strQuery.Append("SELECT STATUS");
                strQuery.Append("FROM QUOTATION_MST_TBL AIR");
                strQuery.Append("WHERE AIR.QUOTATION_MST_PK =");
                strQuery.Append(pk);
                dr = (new WorkFlow()).GetDataReader(strQuery.ToString());
                while (dr.Read())
                {
                    return Convert.ToInt16(dr[0]);
                }
                return 0;
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
        private ArrayList updateOthCharge(DataTable PDT, DataTable OthDT, OracleCommand SCM, string UserName)
        {
            DataRow Pdr = null;
            DataRow Odr = null;
            int delFlage = 0;

            try
            {

                foreach (DataRow Pdr_loopVariable in PDT.Rows)
                {
                    Pdr = Pdr_loopVariable;
                    delFlage = 1;
                    //added by vimlesh kumar 26/08/2006 for adding special request

                    //end
                    foreach (DataRow Odr_loopVariable in OthDT.Rows)
                    {
                        Odr = Odr_loopVariable;
                        SCM.CommandType = CommandType.StoredProcedure;
                        SCM.CommandText = UserName + ".QUOTATION_SEA_TBL_PKG.QUOTATION_TRN_SEA_OTH_CHRG_UPD";
                        if (Odr["QUOTATION_TRN_SEA_FK"] == Pdr["FK"])
                        {
                            var _with27 = SCM.Parameters;
                            _with27.Clear();
                            _with27.Add("DEL_FLAG", delFlage);
                            _with27.Add("QUOTATION_SEA_FK_IN", Odr["QUOTATION_TRN_SEA_FK"]);
                            _with27.Add("FREIGHT_ELEMENT_MST_FK_IN", Odr["FREIGHT_ELEMENT_MST_FK"]);
                            _with27.Add("CURRENCY_MST_FK_IN", Odr["CURRENCY_MST_FK"]);
                            _with27.Add("AMOUNT_IN", getDefault(Odr["AMOUNT"], 0));
                            _with27.Add("FREIGHT_TYPE_IN", getDefault(Odr["PYMT_TYPE"], 1)).Direction = ParameterDirection.Input;
                            _with27.Add("RETURN_VALUE", OracleDbType.Varchar2, 20, "RETURN_VALUE").Direction = ParameterDirection.Output;
                            delFlage += SCM.ExecuteNonQuery();
                        }
                    }
                    if (Convert.ToInt32(Pdr["COMM_GRPPK"]) == HAZARDOUS)
                    {
                        arrMessage = SaveTransactionHZSpcl(SCM, UserName, Convert.ToString(getDefault(Pdr["strSpclReq"], "")), Convert.ToInt64(Pdr["FK"].ToString()));
                    }
                    else if (Convert.ToInt32(Pdr["COMM_GRPPK"]) == REEFER)
                    {
                        arrMessage = SaveTransactionReefer(SCM, UserName, Convert.ToString(getDefault(Pdr["strSpclReq"], "")), Convert.ToInt64(Pdr["FK"].ToString()));
                    }
                    else if (Convert.ToInt32(Pdr["COMM_GRPPK"]) == ODC)
                    {
                        arrMessage = SaveTransactionODC(SCM, UserName, Convert.ToString(getDefault(Pdr["strSpclReq"], "")), Convert.ToInt64(Pdr["FK"].ToString()));
                    }
                    if (arrMessage.Count > 0)
                    {
                        if (string.Compare(Convert.ToString(arrMessage[0].ToString()).ToUpper(), "SAVED") == 0)
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

        public DataTable HeaderInfo(string QuotationPK, bool AmendFlg = false)
        {
            if (AmendFlg == true)
            {
                QuotationPK = "";
            }
            if (string.IsNullOrEmpty(QuotationPK))
                QuotationPK = "-1";
            string strSQL = null;
            strSQL = " Select QUOTATION_MST_PK,     " +  "       QUOTATION_REF_NO,                                                   " +  "       to_char(QUOTATION_DATE,'" + dateFormat + "')        QUOTATION_DATE,         " +  "       VALID_FOR,                                                          " +  "       CARGO_TYPE,                                                         " +  "       PYMT_TYPE,                                                          " +  "       QUOTED_BY,                                                          " +  "       COL_PLACE_MST_FK,                                                   " +  "       COL_ADDRESS,                                                        " +  "       DEL_PLACE_MST_FK,                                                   " +  "       DEL_ADDRESS,                                                        " +  "       AGENT_MST_FK,                                                       " +  "       STATUS,                                                             " +  "       CREATED_BY_FK,                                                      " +  "       to_char(CREATED_DT,'" + dateFormat + "')            CREATED_DT,             " +  "       LAST_MODIFIED_BY_FK,                                                " +  "       to_char(LAST_MODIFIED_DT,'" + dateFormat + "')      LAST_MODIFIED_DT,       " +  "       Version_No,                                                         " +  "       to_char(EXPECTED_SHIPMENT_DT,'" + dateFormat + "')  EXPECTED_SHIPMENT_DT,   " +  "       CUSTOMER_MST_FK,                                                    " +  "       CUSTOMER_CATEGORY_MST_FK,                                           " +  "       SPECIAL_INSTRUCTIONS,                                               " +  "       CUST_TYPE ,                                                        " +  "       CREDIT_DAYS,                                                       " +  "       CREDIT_LIMIT,BASE_CURRENCY_FK,PORT_GROUP,HEADER_CONTENT,FOOTER_CONTENT                          " +  "  from QUOTATION_MST_TBL                                                 " +  "  Where  QUOTATION_MST_PK = " + QuotationPK;
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
        //Public Function Header_Fcl_Lcl(ByVal QuotationPk As String) As DataSet
        //    Try
        //        Dim ObjWk As New  WorkFlow
        //        Dim strquery As New StringBuilder

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
        //strquery.Append("     QUOTATION_MST_TBL              MAIN3,                           " & vbCrLf)
        //strquery.Append("     QUOTATION_DTL_TBL      TRAN3,                           " & vbCrLf)
        //strquery.Append("     PORT_MST_TBL                   PORTPOL3,                        " & vbCrLf)
        //strquery.Append("     PORT_MST_TBL                   PORTPOD3,                        " & vbCrLf)
        //strquery.Append("     OPERATOR_MST_TBL               OPR3,                            " & vbCrLf)
        //strquery.Append("     CONTAINER_TYPE_MST_TBL         CNTR3," & vbCrLf)
        //strquery.Append("     DIMENTION_UNIT_MST_TBL          DUMT                            " & vbCrLf)
        //strquery.Append("   WHERE    MAIN3.QUOTATION_MST_PK      = TRAN3.QUOTATION_MST_FK             " & vbCrLf)
        //strquery.Append("     AND    TRAN3.PORT_MST_POL_FK       = PORTPOL3.PORT_MST_PK(+)            " & vbCrLf)
        //strquery.Append("     AND    TRAN3.PORT_MST_POD_FK       = PORTPOD3.PORT_MST_PK(+)            " & vbCrLf)
        //strquery.Append("     AND    TRAN3.OPERATOR_MST_FK       = OPR3.OPERATOR_MST_PK(+)            " & vbCrLf)
        //strquery.Append("     AND    TRAN3.CONTAINER_TYPE_MST_FK = CNTR3.CONTAINER_TYPE_MST_PK(+)     " & vbCrLf)
        //strquery.Append("     AND    TRAN3.BASIS = DUMT.DIMENTION_UNIT_MST_PK(+)" & vbCrLf)
        //strquery.Append("     AND    MAIN3.QUOTATION_MST_PK =  " & QuotationPk)
        //        strquery.Append(" SELECT     DISTINCT                                    " & vbCrLf)
        //        strquery.Append("     MAIN3.QUOTATION_MST_PK                     PK," & vbCrLf)
        //        strquery.Append("     0                     Fk," & vbCrLf)
        //        strquery.Append("     OPR3.OPERATOR_NAME                         OPER_NAME,    " & vbCrLf)
        //        strquery.Append("     DECODE(MAIN3.PYMT_TYPE,1,'PREPAID','COLLECT') P_TYPE_ID,                 " & vbCrLf)
        //        'strquery.Append("     PORTPOL3.PORT_ID                           POL_ID,              " & vbCrLf)
        //        'strquery.Append("     PORTPOD3.PORT_ID                           POD_ID, " & vbCrLf)
        //        strquery.Append("     PLG.PORT_GRP_CODE POL_ID                           ,              " & vbCrLf)
        //        strquery.Append("     PDG.PORT_GRP_CODE POD_ID                           , " & vbCrLf)

        //        strquery.Append("     MAIN3.QUOTATION_DATE                       VALID_FROM," & vbCrLf)
        //        strquery.Append("     MAIN3.HEADER_CONTENT  HEADER, " & vbCrLf)
        //        strquery.Append("     MAIN3.FOOTER_CONTENT  FOOTER, " & vbCrLf)
        //        strquery.Append("     ' '                CNTR_ID,             " & vbCrLf)
        //        'modified by Thiyagarjan on 30/6/08 for fcl and lcl quotation
        //        'strquery.Append("     DECODE(MAIN3.CARGO_TYPE,1,'FCL','LCL')  CARGO_TYPE," & vbCrLf)
        //        strquery.Append("    ( case when TRAN3.container_type_mst_fk is not null then 'FCL' else 'LCL' end )  CARGO_TYPE," & vbCrLf)
        //        'end
        //        strquery.Append("      ' '      LCLBASIS, " & vbCrLf)
        //        strquery.Append("      CMMT.CARGO_MOVE_CODE, " & vbCrLf)
        //        strquery.Append("      ' ' COMMODITY_GROUP_CODE, " & vbCrLf)
        //        strquery.Append("      SUM(NVL(TRAN3.EXPECTED_BOXES,0)) BOXES, " & vbCrLf)
        //        strquery.Append("      SUM(NVL(TRAN3.EXPECTED_WEIGHT,0)) WEIGHT, " & vbCrLf)
        //        strquery.Append("      SUM(NVL(TRAN3.EXPECTED_VOLUME,0)) VOLUME " & vbCrLf)
        //        strquery.Append(" FROM                                                             " & vbCrLf)
        //        strquery.Append("     QUOTATION_MST_TBL              MAIN3,                           " & vbCrLf)
        //        strquery.Append("     QUOTATION_DTL_TBL      TRAN3,                           " & vbCrLf)
        //        strquery.Append("     PORT_MST_TBL                   PORTPOL3,                        " & vbCrLf)
        //        strquery.Append("     PORT_MST_TBL                   PORTPOD3,                        " & vbCrLf)
        //        strquery.Append("     OPERATOR_MST_TBL               OPR3,                            " & vbCrLf)
        //        strquery.Append("     CONTAINER_TYPE_MST_TBL         CNTR3," & vbCrLf)
        //        strquery.Append("     DIMENTION_UNIT_MST_TBL          DUMT, PORT_GROUP_MST_TBL        PLG,PORT_GROUP_MST_TBL        PDG,CARGO_MOVE_MST_TBL        CMMT                            " & vbCrLf)
        //        strquery.Append("   WHERE    MAIN3.QUOTATION_MST_PK      = TRAN3.QUOTATION_MST_FK             " & vbCrLf)
        //        strquery.Append("     AND    TRAN3.PORT_MST_POL_FK       = PORTPOL3.PORT_MST_PK(+)            " & vbCrLf)
        //        strquery.Append("     AND    TRAN3.PORT_MST_POD_FK       = PORTPOD3.PORT_MST_PK(+)            " & vbCrLf)
        //        strquery.Append("     AND    TRAN3.OPERATOR_MST_FK       = OPR3.OPERATOR_MST_PK(+)            " & vbCrLf)
        //        strquery.Append("     AND    TRAN3.CONTAINER_TYPE_MST_FK = CNTR3.CONTAINER_TYPE_MST_PK(+)     " & vbCrLf)
        //        strquery.Append("     AND MAIN3.CUSTOMER_MST_FK = CMMT.CARGO_MOVE_PK(+) " & vbCrLf)
        //        strquery.Append("     AND    TRAN3.BASIS = DUMT.DIMENTION_UNIT_MST_PK(+)  AND PLG.PORT_GRP_MST_PK = PORTPOL3.PORT_GRP_MST_FK  AND PDG.PORT_GRP_MST_PK = PORTPOD3.PORT_GRP_MST_FK " & vbCrLf)
        //        strquery.Append("     AND    MAIN3.QUOTATION_MST_PK =  " & QuotationPk)
        //        strquery.Append("     GROUP BY MAIN3.QUOTATION_MST_PK ," & vbCrLf)
        //        strquery.Append("     OPR3.OPERATOR_NAME," & vbCrLf)
        //        strquery.Append("     MAIN3.PYMT_TYPE," & vbCrLf)
        //        strquery.Append("     PLG.PORT_GRP_CODE," & vbCrLf)
        //        strquery.Append("     PDG.PORT_GRP_CODE," & vbCrLf)
        //        strquery.Append("     MAIN3.QUOTATION_DATE," & vbCrLf)
        //        strquery.Append("     MAIN3.HEADER_CONTENT," & vbCrLf)
        //        strquery.Append("     MAIN3.FOOTER_CONTENT ," & vbCrLf)
        //        strquery.Append("     TRAN3.CONTAINER_TYPE_MST_FK ," & vbCrLf)
        //        strquery.Append("     CMMT.CARGO_MOVE_CODE " & vbCrLf)

        //    Return ObjWk.GetDataSet(strquery.ToString)
        //    Catch Oraexp As OracleException
        //        Throw Oraexp        ''Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
        //    Catch ex As Exception
        //        Throw ex
        //    End Try
        //End Function
        #endregion

        #region "Fetch_Fcl_Lcl_Header"
        public DataSet Header_Fcl_Lcl(string QuotationPk)
        {
            try
            {
                WorkFlow ObjWk = new WorkFlow();
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("select PK,");
                sb.Append("       fk,");
                sb.Append("       OPER_NAME,");
                sb.Append("       P_TYPE_ID,");
                sb.Append("       POL_ID,");
                sb.Append("       POD_ID,");
                sb.Append("       VALID_FROM,");
                sb.Append("       HEADER,");
                sb.Append("       FOOTER,");
                sb.Append("       CNTR_ID,");
                sb.Append("       CARGO_TYPE,");
                sb.Append("       LCLBASIS,");
                sb.Append("       CARGO_MOVE_CODE,");
                sb.Append("       COMMODITY_GROUP_CODE,");
                sb.Append("       sum(BOXES) BOXES,");
                sb.Append("       sum(WEIGHT) WEIGHT,");
                sb.Append("       sum(VOLUME) VOLUME");
                sb.Append("  from (SELECT DISTINCT MAIN3.QUOTATION_MST_PK PK,");
                sb.Append("                        0 Fk,");
                sb.Append("                        OPR3.OPERATOR_NAME OPER_NAME,");
                sb.Append("                        DECODE(MAIN3.PYMT_TYPE, 1, 'PREPAID', 'COLLECT') P_TYPE_ID,");
                sb.Append("                        CASE WHEN NVL(MAIN3.PORT_GROUP, 0) = 1 THEN PLG.PORT_GRP_NAME ELSE PORTPOL3.PORT_NAME END POL_ID,");
                sb.Append("                        CASE WHEN NVL(MAIN3.PORT_GROUP, 0) = 1 THEN PDG.PORT_GRP_NAME ELSE PORTPOD3.PORT_NAME END POD_ID,");
                sb.Append("                        MAIN3.QUOTATION_DATE VALID_FROM,");
                sb.Append("                        MAIN3.HEADER_CONTENT HEADER,");
                sb.Append("                        MAIN3.FOOTER_CONTENT FOOTER,");
                sb.Append("                        TRAN3.container_type_mst_fk,");
                sb.Append("                        ' ' CNTR_ID,");
                sb.Append("                        (case");
                sb.Append("                          when TRAN3.container_type_mst_fk is not null then");
                sb.Append("                           'FCL'");
                sb.Append("                          else");
                sb.Append("                           'LCL'");
                sb.Append("                        end) CARGO_TYPE,");
                sb.Append("                        ' ' LCLBASIS,");
                sb.Append("                        CMMT.CARGO_MOVE_CODE,");
                sb.Append("                        CGMT.COMMODITY_GROUP_CODE COMMODITY_GROUP_CODE,");
                sb.Append("                        NVL(TRAN3.EXPECTED_BOXES, 0) BOXES,");
                sb.Append("                        NVL(TRAN3.EXPECTED_WEIGHT, 0) WEIGHT,");
                sb.Append("                        NVL(TRAN3.EXPECTED_VOLUME, 0) VOLUME");
                sb.Append("          FROM QUOTATION_MST_TBL         MAIN3,");
                sb.Append("               QUOTATION_DTL_TBL TRAN3,");
                sb.Append("               PORT_MST_TBL              PORTPOL3,");
                sb.Append("               PORT_MST_TBL              PORTPOD3,");
                sb.Append("               OPERATOR_MST_TBL          OPR3,");
                sb.Append("               CONTAINER_TYPE_MST_TBL    CNTR3,");
                sb.Append("               DIMENTION_UNIT_MST_TBL    DUMT,");
                sb.Append("               PORT_GRP_MST_TBL        PLG,");
                sb.Append("               PORT_GRP_MST_TBL        PDG,");
                sb.Append("               CARGO_MOVE_MST_TBL        CMMT, COMMODITY_GROUP_MST_TBL   CGMT ");
                sb.Append("         WHERE MAIN3.QUOTATION_MST_PK = TRAN3.QUOTATION_MST_FK");
                sb.Append("           AND TRAN3.PORT_MST_POL_FK = PORTPOL3.PORT_MST_PK(+)");
                sb.Append("           AND TRAN3.PORT_MST_POD_FK = PORTPOD3.PORT_MST_PK(+)");
                sb.Append("           AND TRAN3.CARRIER_MST_FK = OPR3.OPERATOR_MST_PK(+)");
                sb.Append("           AND TRAN3.CONTAINER_TYPE_MST_FK = CNTR3.CONTAINER_TYPE_MST_PK(+)");
                sb.Append("           AND MAIN3.CARGO_MOVE_FK = CMMT.CARGO_MOVE_PK(+)");
                sb.Append("           AND TRAN3.BASIS = DUMT.DIMENTION_UNIT_MST_PK(+)");
                sb.Append("           AND PLG.PORT_GRP_MST_PK(+) = TRAN3.POL_GRP_FK");
                sb.Append("           AND PDG.PORT_GRP_MST_PK(+) = TRAN3.POD_GRP_FK AND CGMT.COMMODITY_GROUP_PK = TRAN3.COMMODITY_GROUP_FK ");
                sb.Append("           AND MAIN3.QUOTATION_MST_PK = " + QuotationPk);
                sb.Append("         GROUP BY MAIN3.QUOTATION_MST_PK,");
                sb.Append("                  OPR3.OPERATOR_NAME,");
                sb.Append("                  MAIN3.PYMT_TYPE,");
                sb.Append("                  PLG.PORT_GRP_NAME,");
                sb.Append("                  PDG.PORT_GRP_NAME,");
                sb.Append("                  MAIN3.QUOTATION_DATE,");
                sb.Append("                  MAIN3.HEADER_CONTENT,");
                sb.Append("                  MAIN3.FOOTER_CONTENT,");
                sb.Append("                  TRAN3.CONTAINER_TYPE_MST_FK,");
                sb.Append("                  CMMT.CARGO_MOVE_CODE,");
                sb.Append("                  TRAN3.EXPECTED_BOXES,");
                sb.Append("                  TRAN3.EXPECTED_WEIGHT,");
                sb.Append("                  TRAN3.EXPECTED_VOLUME, PORTPOL3.PORT_NAME, PORTPOD3.PORT_NAME, CGMT.COMMODITY_GROUP_CODE, MAIN3.PORT_GROUP)");
                sb.Append("       group by PK,");
                sb.Append("       fk,");
                sb.Append("       OPER_NAME,");
                sb.Append("       P_TYPE_ID,");
                sb.Append("       POL_ID,");
                sb.Append("       POD_ID,");
                sb.Append("       VALID_FROM,");
                sb.Append("       HEADER,");
                sb.Append("       FOOTER,");
                sb.Append("       CNTR_ID,");
                sb.Append("       CARGO_TYPE,");
                sb.Append("       LCLBASIS,");
                sb.Append("       CARGO_MOVE_CODE,");
                sb.Append("       COMMODITY_GROUP_CODE");
                return ObjWk.GetDataSet(sb.ToString());
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
                strquery.Append("SELECT DISTINCT TRAN3.QUOTATION_MST_FK PK,");
                strquery.Append("                TRAN3.QUOTE_DTL_PK QUOTE_TRN_SEA_PK,");
                strquery.Append("       CASE WHEN  NVL(TRAN3.EXPECTED_BOXES,0)=0 THEN 'NA' ");
                strquery.Append("        ELSE TRAN3.EXPECTED_BOXES||''  END QUANTITY, ");
                strquery.Append("        ");
                strquery.Append("                CMDT4.COMMODITY_GROUP_CODE  COMM_ID,");
                strquery.Append("                TRAN3.Expected_Weight      GROSSWEIGTH,");
                strquery.Append("                TRAN3.Expected_Volume      VOLUME,");
                strquery.Append("                cmt.cargo_move_code,");
                strquery.Append("                main3.cargo_type");
                strquery.Append("  FROM QUOTATION_MST_TBL         MAIN3,");
                strquery.Append("       QUOTATION_DTL_TBL TRAN3,");
                strquery.Append("       COMMODITY_MST_TBL         CMDT3,");
                strquery.Append("       COMMODITY_GROUP_MST_TBL CMDT4 ,");
                strquery.Append("       cargo_move_mst_tbl      cmt");
                strquery.Append(" WHERE MAIN3.QUOTATION_MST_PK = TRAN3.QUOTATION_MST_FK");
                strquery.Append("    AND TRAN3.COMMODITY_GROUP_FK = CMDT4.COMMODITY_GROUP_PK(+)");
                strquery.Append("    and main3.cargo_move_fk=cmt.cargo_move_pk(+)");
                strquery.Append("    AND MAIN3.QUOTATION_MST_PK = " + QuotationPk);


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

                strquery.Append("SELECT TRAN3.QUOTATION_MST_FK PK,");
                strquery.Append("       TRAN3.QUOTE_TRN_SEA_PK,");
                //'changed for surcharge 
                //'strquery.Append("       FRT3.FREIGHT_ELEMENT_NAME FRT_NAME," & vbCrLf)
                strquery.Append("       CASE WHEN FRTD3.SURCHARGE IS NULL THEN  FRT3.FREIGHT_ELEMENT_NAME");
                strquery.Append("       ELSE (FRT3.FREIGHT_ELEMENT_NAME || ' ( ' ||  FRTD3.SURCHARGE  || ' ) ' || '')");
                strquery.Append("       END FRT_NAME,");


                strquery.Append("       CURR3.CURRENCY_ID CURR_ID,");
                strquery.Append("       FRTD3.QUOTED_RATE QUOTERATE,");
                strquery.Append("       ROUND((SELECT GET_EX_RATE(FRTD3.CURRENCY_MST_FK, " + baseCurrency + ",TO_DATE('" + QuotationDate + "', DATEFORMAT)) FROM DUAL), 6) ROE,");
                strquery.Append("       ROUND(ABS(FRTD3.QUOTED_RATE)*NVL(DECODE(FRT3.CREDIT,NULL,1,0,-1,1,1),1)* (SELECT GET_EX_RATE(FRTD3.CURRENCY_MST_FK, " + baseCurrency + ",TO_DATE('" + QuotationDate + "', DATEFORMAT))FROM DUAL), 2) QUOTERATE,");
                strquery.Append("       DUMT.DIMENTION_ID");
                //strquery.Append("   (case when DUMT.DIMENTION_ID is null then '1' else DUMT.DIMENTION_ID end) DIMENTION_ID " & vbCrLf)
                strquery.Append("  FROM QUOTATION_MST_TBL          MAIN3,");
                strquery.Append("       QUOTATION_DTL_TBL  TRAN3,");
                strquery.Append("       QUOTATION_TRN_SEA_FRT_DTLS FRTD3,");
                strquery.Append("       FREIGHT_ELEMENT_MST_TBL    FRT3,");
                strquery.Append("       CURRENCY_TYPE_MST_TBL      CURR3,");
                strquery.Append("       DIMENTION_UNIT_MST_TBL  DUMT");
                strquery.Append("       ");
                strquery.Append(" WHERE MAIN3.QUOTATION_MST_PK = TRAN3.QUOTATION_MST_FK");
                strquery.Append("   AND TRAN3.QUOTE_TRN_SEA_PK = FRTD3.QUOTE_TRN_SEA_FK");
                strquery.Append("   AND FRTD3.FREIGHT_ELEMENT_MST_FK = FRT3.FREIGHT_ELEMENT_MST_PK(+)");
                strquery.Append("   AND TRAN3.BASIS=DUMT.DIMENTION_UNIT_MST_PK(+)");
                //strquery.Append("   AND FRT3.CHARGE_BASIS <> 2" & vbCrLf) ''Commeneted by Koteshwari on 05/07/2011
                strquery.Append("   AND FRTD3.CURRENCY_MST_FK = CURR3.CURRENCY_MST_PK(+)");
                strquery.Append("   AND MAIN3.QUOTATION_MST_PK = " + QuotationPk);
                strquery.Append("   order by preference");

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
        public DataSet Charges_Description_New(string QuotationPk, System.DateTime QuotationDate, Int64 baseCurrency, int FCL = 0)
        {
            try
            {
                WorkFlow ObjWk = new WorkFlow();
                StringBuilder strquery = new StringBuilder();
                strquery.Append("SELECT PORT_GRP_CODE,");
                strquery.Append("PORT_GRP_CODE1,");
                strquery.Append("POL,");
                strquery.Append("POD,");
                strquery.Append("CARRIER,");
                strquery.Append("FRT_NAME,");
                if (FCL == 1)
                {
                    strquery.Append("CONTAINER_TYPE_MST_ID || ' (' || CURR_ID || ')' CONTAINER_TYPE_MST_ID,");
                }
                else
                {
                    strquery.Append("CONTAINER_TYPE_MST_ID || ' (' || CURR_ID || ')' CONTAINER_TYPE_MST_ID,");
                }
                strquery.Append("CURR_ID,");
                strquery.Append("QUOTERATE,");
                strquery.Append("ROE,");
                strquery.Append("QUOTERATE1 from ");
                strquery.Append("(SELECT distinct PGL.PORT_GRP_NAME PORT_GRP_CODE,");
                strquery.Append("       PGD.PORT_GRP_NAME PORT_GRP_CODE1,");
                strquery.Append("       '' POL,");
                strquery.Append("       '' POD,");
                strquery.Append("       OMT.OPERATOR_NAME Carrier,");
                //'changed for surcharge 
                strquery.Append("       FRT3.FREIGHT_ELEMENT_NAME FRT_NAME,");
                //strquery.Append("       CASE WHEN FRTD3.SURCHARGE IS NULL THEN  FRT3.FREIGHT_ELEMENT_NAME" & vbCrLf)
                //strquery.Append("       ELSE (FRT3.FREIGHT_ELEMENT_NAME || ' ( ' ||  FRTD3.SURCHARGE  || ' ) ' || '')" & vbCrLf)
                //strquery.Append("       END FRT_NAME," & vbCrLf)
                if (FCL == 1)
                {
                    strquery.Append("       CTMT.CONTAINER_TYPE_MST_ID,");
                }
                else
                {
                    strquery.Append("       DUMT.DIMENTION_ID CONTAINER_TYPE_MST_ID,");
                }

                strquery.Append("       CURR3.CURRENCY_ID CURR_ID,");
                strquery.Append("       FRTD3.QUOTED_RATE QUOTERATE,");
                strquery.Append("       ROUND((SELECT GET_EX_RATE(FRTD3.CURRENCY_MST_FK, " + baseCurrency + ",TO_DATE('" + QuotationDate + "', DATEFORMAT)) FROM DUAL), 6) ROE,");
                strquery.Append("       CASE WHEN MAIN3.CARGO_TYPE=2 THEN");
                strquery.Append("       ROUND(ABS(FRTD3.QUOTED_RATE)*NVL(DECODE(FRT3.CREDIT,NULL,1,0,-1,1,1),1)* (SELECT GET_EX_RATE(FRTD3.CURRENCY_MST_FK, " + baseCurrency + ",TO_DATE('" + QuotationDate + "', DATEFORMAT))FROM DUAL), 2) ELSE");
                strquery.Append("       ROUND(ABS(FRTD3.QUOTED_RATE)*NVL(DECODE(FRT3.CREDIT,NULL,1,0,-1,1,1),1), 2) END QUOTERATE1");
                //strquery.Append("       DUMT.DIMENTION_ID" & vbCrLf)
                //strquery.Append("   (case when DUMT.DIMENTION_ID is null then '1' else DUMT.DIMENTION_ID end) DIMENTION_ID " & vbCrLf)
                strquery.Append("  FROM QUOTATION_MST_TBL          MAIN3,");
                strquery.Append("       QUOTATION_DTL_TBL  TRAN3,");
                strquery.Append("       QUOTATION_TRN_SEA_FRT_DTLS FRTD3,");
                strquery.Append("       FREIGHT_ELEMENT_MST_TBL    FRT3,");
                if (FCL == 1)
                {
                    strquery.Append("       CONTAINER_TYPE_MST_TBL     CTMT,");
                }
                else
                {
                    strquery.Append("       DIMENTION_UNIT_MST_TBL       DUMT,");
                }

                strquery.Append("       CURRENCY_TYPE_MST_TBL      CURR3,");
                strquery.Append("       PORT_MST_TBL               POL,");
                strquery.Append("       PORT_MST_TBL               POD,");
                strquery.Append("       PORT_GROUP_MST_TBL         PGL,");
                strquery.Append("       PORT_GROUP_MST_TBL PGD, OPERATOR_MST_TBL           OMT        ");
                //strquery.Append("       DIMENTION_UNIT_MST_TBL  DUMT" & vbCrLf)
                strquery.Append("       ");
                strquery.Append(" WHERE MAIN3.QUOTATION_MST_PK = TRAN3.QUOTATION_MST_FK");
                strquery.Append("   AND TRAN3.QUOTE_TRN_SEA_PK = FRTD3.QUOTE_TRN_SEA_FK");
                strquery.Append("   AND FRTD3.FREIGHT_ELEMENT_MST_FK = FRT3.FREIGHT_ELEMENT_MST_PK(+)");
                //strquery.Append("   AND TRAN3.BASIS=DUMT.DIMENTION_UNIT_MST_PK(+)" & vbCrLf)
                //strquery.Append("   AND FRT3.CHARGE_BASIS <> 2" & vbCrLf) ''Commeneted by Koteshwari on 05/07/2011
                strquery.Append("   AND FRTD3.CURRENCY_MST_FK = CURR3.CURRENCY_MST_PK(+)");
                if (FCL == 1)
                {
                    strquery.Append("   AND CTMT.CONTAINER_TYPE_MST_PK(+) = TRAN3.CONTAINER_TYPE_MST_FK");
                }
                else
                {
                    strquery.Append("   AND DUMT.DIMENTION_UNIT_MST_PK(+) = TRAN3.BASIS");
                }

                strquery.Append("   AND TRAN3.PORT_MST_POL_FK  = POL.PORT_MST_PK");
                strquery.Append("   AND TRAN3.PORT_MST_POD_FK  = POD.PORT_MST_PK");
                strquery.Append("   AND PGL.PORT_GRP_MST_PK(+) = POL.PORT_GRP_MST_FK");
                strquery.Append("   AND PGD.PORT_GRP_MST_PK(+) = POD.PORT_GRP_MST_FK");
                strquery.Append("   AND OMT.OPERATOR_MST_PK(+) = TRAN3.OPERATOR_MST_FK");
                strquery.Append("   AND MAIN3.QUOTATION_MST_PK = " + QuotationPk);
                //strquery.Append("   order by preference) Q" & vbCrLf)
                strquery.Append("   ) Q");
                //strquery.Append("   WHERE Q.QUOTERATE1 <> 0" & vbCrLf)

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
        public DataSet Charges_Description_Vatos(string QuotationPk, System.DateTime QuotationDate, Int64 baseCurrency)
        {
            try
            {
                WorkFlow ObjWk = new WorkFlow();
                StringBuilder strquery = new StringBuilder();
                strquery.Append("SELECT PORT_GRP_CODE,");
                strquery.Append("PORT_GRP_CODE1,");
                strquery.Append("POL,");
                strquery.Append("POD,");
                strquery.Append("CARRIER,");
                strquery.Append("CHECK_ADVATOS,");
                strquery.Append("FRT_NAME,");
                strquery.Append("CONTAINER_TYPE_MST_ID,");
                strquery.Append("CURR_ID,");
                strquery.Append("QUOTERATE,");
                strquery.Append("ROE,");
                strquery.Append("QUOTERATE1 from ");
                strquery.Append("(SELECT PGL.PORT_GRP_NAME PORT_GRP_CODE,");
                strquery.Append("       PGD.PORT_GRP_NAME PORT_GRP_CODE1,");
                strquery.Append("       POL.PORT_NAME POL,");
                strquery.Append("       POD.PORT_NAME POD,");
                strquery.Append("       OMT.OPERATOR_ID Carrier,");
                strquery.Append("       FRTD3.CHECK_ADVATOS,");
                //'changed for surcharge 
                strquery.Append("       FRT3.FREIGHT_ELEMENT_NAME FRT_NAME,");
                //strquery.Append("       CASE WHEN FRTD3.SURCHARGE IS NULL THEN  FRT3.FREIGHT_ELEMENT_NAME" & vbCrLf)
                //strquery.Append("       ELSE (FRT3.FREIGHT_ELEMENT_NAME || ' ( ' ||  FRTD3.SURCHARGE  || ' ) ' || '')" & vbCrLf)
                //strquery.Append("       END FRT_NAME," & vbCrLf)
                strquery.Append("       CTMT.CONTAINER_TYPE_MST_ID,");
                strquery.Append("       CURR3.CURRENCY_ID CURR_ID,");
                strquery.Append("       FRTD3.QUOTED_RATE QUOTERATE,");
                strquery.Append("       ROUND((SELECT GET_EX_RATE(FRTD3.CURRENCY_MST_FK, " + baseCurrency + ",TO_DATE('" + QuotationDate + "', DATEFORMAT)) FROM DUAL), 6) ROE,");
                strquery.Append("       ROUND(ABS(FRTD3.QUOTED_RATE)*NVL(DECODE(FRT3.CREDIT,NULL,1,0,-1,1,1),1)* (SELECT GET_EX_RATE(FRTD3.CURRENCY_MST_FK, " + baseCurrency + ",TO_DATE('" + QuotationDate + "', DATEFORMAT))FROM DUAL), 2) QUOTERATE1");
                //strquery.Append("       DUMT.DIMENTION_ID" & vbCrLf)
                //strquery.Append("   (case when DUMT.DIMENTION_ID is null then '1' else DUMT.DIMENTION_ID end) DIMENTION_ID " & vbCrLf)
                strquery.Append("  FROM QUOTATION_MST_TBL          MAIN3,");
                strquery.Append("       QUOTATION_DTL_TBL  TRAN3,");
                strquery.Append("       Quotation_Freight_Trn FRTD3,");
                strquery.Append("       FREIGHT_ELEMENT_MST_TBL    FRT3,");
                strquery.Append("       CONTAINER_TYPE_MST_TBL     CTMT,");
                strquery.Append("       CURRENCY_TYPE_MST_TBL      CURR3,");
                strquery.Append("       PORT_MST_TBL               POL,");
                strquery.Append("       PORT_MST_TBL               POD,");
                strquery.Append("       PORT_GROUP_MST_TBL         PGL,");
                strquery.Append("       PORT_GROUP_MST_TBL PGD, OPERATOR_MST_TBL           OMT        ");
                //strquery.Append("       DIMENTION_UNIT_MST_TBL  DUMT" & vbCrLf)
                strquery.Append("       ");
                strquery.Append(" WHERE MAIN3.QUOTATION_MST_PK = TRAN3.QUOTATION_MST_FK");
                strquery.Append("   AND TRAN3.QUOTE_DTL_PK = FRTD3.Quotation_Dtl_Fk");
                strquery.Append("   AND FRTD3.FREIGHT_ELEMENT_MST_FK = FRT3.FREIGHT_ELEMENT_MST_PK(+)");
                //strquery.Append("   AND TRAN3.BASIS=DUMT.DIMENTION_UNIT_MST_PK(+)" & vbCrLf)
                //strquery.Append("   AND FRT3.CHARGE_BASIS <> 2" & vbCrLf) ''Commeneted by Koteshwari on 05/07/2011
                strquery.Append("   AND FRTD3.CURRENCY_MST_FK = CURR3.CURRENCY_MST_PK(+)");
                strquery.Append("   AND CTMT.CONTAINER_TYPE_MST_PK(+) = TRAN3.CONTAINER_TYPE_MST_FK");
                strquery.Append("   AND TRAN3.PORT_MST_POL_FK  = POL.PORT_MST_PK");
                strquery.Append("   AND TRAN3.PORT_MST_POD_FK  = POD.PORT_MST_PK");
                strquery.Append("   AND PGL.PORT_GRP_MST_PK(+) = POL.PORT_GRP_MST_FK");
                strquery.Append("   AND PGD.PORT_GRP_MST_PK(+) = POD.PORT_GRP_MST_FK");
                strquery.Append("   AND OMT.OPERATOR_MST_PK(+) = TRAN3.Carrier_Mst_Fk");
                strquery.Append("   AND MAIN3.QUOTATION_MST_PK = " + QuotationPk);
                strquery.Append("   order by preference) Q");
                strquery.Append("   WHERE (Q.QUOTERATE1 = 0 OR Q.CHECK_ADVATOS=1)");

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
        //'added by subhransu for the quotation report 09/aug/2011

        public DataSet Fetch_Freight(string QuotationPk, System.DateTime QuotationDate, Int64 baseCurrency)
        {

            WorkFlow ObjWk = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("       SELECT DISTINCT FRT3.FREIGHT_ELEMENT_MST_PK,");
            sb.Append("       FRT3.FREIGHT_ELEMENT_NAME FRT_NAME,");
            sb.Append("       '" + HttpContext.Current.Session["CURRENCY_ID"] + "' CURRENCY_ID,");
            sb.Append("       '" + QuotationDate + "' QUOTERATE,");
            sb.Append("       PLG.PORT_GRP_NAME PGC,");
            sb.Append("       PDG.PORT_GRP_NAME PDC");
            sb.Append("");
            sb.Append("       FROM QUOTATION_MST_TBL          MAIN3,");
            sb.Append("       QUOTATION_DTL_TBL  TRAN3,");
            sb.Append("       QUOTATION_TRN_SEA_FRT_DTLS FRTD3,");
            sb.Append("       FREIGHT_ELEMENT_MST_TBL    FRT3,");
            sb.Append("       PORT_MST_TBL               POL,");
            sb.Append("       PORT_MST_TBL               POD,");
            sb.Append("       PORT_GROUP_MST_TBL         PLG,");
            sb.Append("       PORT_GROUP_MST_TBL         PDG");
            sb.Append("");
            sb.Append("   WHERE MAIN3.QUOTATION_MST_PK = TRAN3.QUOTATION_MST_FK");
            sb.Append("   AND TRAN3.QUOTE_TRN_SEA_PK = FRTD3.QUOTE_TRN_SEA_FK");
            sb.Append("   AND FRTD3.FREIGHT_ELEMENT_MST_FK = FRT3.FREIGHT_ELEMENT_MST_PK(+)");
            sb.Append("   AND MAIN3.QUOTATION_MST_PK = " + QuotationPk);
            sb.Append("   AND TRAN3.PORT_MST_POL_FK = POL.PORT_MST_PK");
            sb.Append("   AND TRAN3.PORT_MST_POD_FK = POD.PORT_MST_PK");
            sb.Append("   AND POL.PORT_GRP_MST_FK = PLG.PORT_GRP_MST_PK");
            sb.Append("   AND POD.PORT_GRP_MST_FK = PDG.PORT_GRP_MST_PK");
            sb.Append("   AND FRTD3.QUOTED_RATE <> 0");
            sb.Append("");
            sb.Append("          GROUP BY FRT3.FREIGHT_ELEMENT_NAME,");
            sb.Append("          FRTD3.CURRENCY_MST_FK,");
            sb.Append("          FRT3.CREDIT,");
            sb.Append("          FRT3.FREIGHT_ELEMENT_MST_PK,");
            sb.Append("          PLG.PORT_GRP_NAME,");
            sb.Append("          PDG.PORT_GRP_NAME");
            sb.Append("");

            try
            {
                return ObjWk.GetDataSet(sb.ToString());
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
        public DataSet Fetch_Container(string QuotationPk, System.DateTime QuotationDate, Int64 baseCurrency)
        {

            WorkFlow ObjWk = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

            sb.Append("  SELECT DISTINCT CNT.CONTAINER_TYPE_MST_PK, CNT.CONTAINER_TYPE_MST_ID");
            sb.Append("");
            sb.Append("  FROM QUOTATION_MST_TBL         MAIN3,");
            sb.Append("       QUOTATION_DTL_TBL TRAN3,");
            sb.Append("       CONTAINER_TYPE_MST_TBL    CNT");
            sb.Append("");
            sb.Append("   WHERE MAIN3.QUOTATION_MST_PK = TRAN3.QUOTATION_MST_FK");
            sb.Append("   AND CNT.CONTAINER_TYPE_MST_PK = TRAN3.CONTAINER_TYPE_MST_FK");
            sb.Append("   AND MAIN3.QUOTATION_MST_PK =" + QuotationPk);
            try
            {
                return ObjWk.GetDataSet(sb.ToString());
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
        public DataSet Fetch_Total_Freight(string QuotationPk, System.DateTime QuotationDate, Int64 baseCurrency)
        {

            WorkFlow ObjWk = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("      SELECT TRAN3.QUOTATION_MST_FK PK,");
            sb.Append("        FRT3.FREIGHT_ELEMENT_MST_PK,TRAN3.QUOTE_TRN_SEA_PK,");
            sb.Append("       FRT3.FREIGHT_ELEMENT_NAME,");
            sb.Append("       CASE");
            sb.Append("         WHEN FRTD3.SURCHARGE IS NULL THEN");
            sb.Append("          FRT3.FREIGHT_ELEMENT_NAME");
            sb.Append("         ELSE");
            sb.Append("          (FRT3.FREIGHT_ELEMENT_NAME || ' ( ' || FRTD3.SURCHARGE || ' ) ' || '')");
            sb.Append("       END FRT_NAME,");
            sb.Append("       CURR3.CURRENCY_ID CURR_ID,");
            sb.Append("       FRTD3.QUOTED_RATE QUOTERATE,");
            sb.Append("       ROUND((SELECT GET_EX_RATE(FRTD3.CURRENCY_MST_FK,");
            sb.Append("                                173,");
            sb.Append("                                TO_DATE('07/08/2011', DATEFORMAT))");
            sb.Append("               FROM DUAL),");
            sb.Append("             6) ROE,");
            sb.Append("       ROUND(ABS(FRTD3.QUOTED_RATE) *");
            sb.Append("             NVL(DECODE(FRT3.CREDIT, NULL, 1, 0, -1, 1, 1), 1) *");
            sb.Append("             (SELECT GET_EX_RATE(FRTD3.CURRENCY_MST_FK,");
            sb.Append("                                 173,");
            sb.Append("                                 TO_DATE('07/08/2011', DATEFORMAT))");
            sb.Append("                FROM DUAL),");
            sb.Append("             2) QUOTERATE1,");
            sb.Append("       DUMT.DIMENTION_ID,");
            sb.Append("       CNT.CONTAINER_TYPE_MST_ID CONT1");
            sb.Append("");
            sb.Append("     FROM QUOTATION_MST_TBL          MAIN3,");
            sb.Append("       QUOTATION_DTL_TBL  TRAN3,");
            sb.Append("       QUOTATION_TRN_SEA_FRT_DTLS FRTD3,");
            sb.Append("       FREIGHT_ELEMENT_MST_TBL    FRT3,");
            sb.Append("       CURRENCY_TYPE_MST_TBL      CURR3,");
            sb.Append("       DIMENTION_UNIT_MST_TBL     DUMT,");
            sb.Append("       CONTAINER_TYPE_MST_TBL     CNT");
            sb.Append("   WHERE MAIN3.QUOTATION_MST_PK = TRAN3.QUOTATION_MST_FK");
            sb.Append("   AND TRAN3.QUOTE_TRN_SEA_PK = FRTD3.QUOTE_TRN_SEA_FK");
            sb.Append("   AND FRTD3.FREIGHT_ELEMENT_MST_FK = FRT3.FREIGHT_ELEMENT_MST_PK(+)");
            sb.Append("   AND TRAN3.BASIS = DUMT.DIMENTION_UNIT_MST_PK(+)");
            sb.Append("   AND FRTD3.CURRENCY_MST_FK = CURR3.CURRENCY_MST_PK(+)");
            sb.Append("   AND CNT.CONTAINER_TYPE_MST_PK = TRAN3.CONTAINER_TYPE_MST_FK");
            sb.Append("   AND MAIN3.QUOTATION_MST_PK =" + QuotationPk);
            sb.Append("");
            sb.Append(" ORDER BY PREFERENCE");
            sb.Append("");
            try
            {
                return ObjWk.GetDataSet(sb.ToString());
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

        //'added by subhransu for the quotation report 09/aug/2011

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
                //strquery.Append("  FROM QUOTATION_MST_TBL          MAIN1," & vbCrLf)
                //strquery.Append("       QUOTATION_DTL_TBL  FCL_LCL," & vbCrLf)
                //strquery.Append("       QUOTATION_TRN_SEA_OTH_CHRG QUOT_OTHER," & vbCrLf)
                //strquery.Append("       FREIGHT_ELEMENT_MST_TBL    FRT3," & vbCrLf)
                //strquery.Append("       CURRENCY_TYPE_MST_TBL      CURR3" & vbCrLf)
                //strquery.Append(" WHERE FCL_LCL.QUOTATION_MST_FK = MAIN1.QUOTATION_MST_PK" & vbCrLf)
                //strquery.Append("   AND QUOT_OTHER.QUOTATION_TRN_SEA_FK = FCL_LCL.QUOTE_TRN_SEA_PK" & vbCrLf)
                //strquery.Append("   AND QUOT_OTHER.FREIGHT_ELEMENT_MST_FK = FRT3.FREIGHT_ELEMENT_MST_PK" & vbCrLf)
                //strquery.Append("   AND QUOT_OTHER.CURRENCY_MST_FK = CURR3.CURRENCY_MST_PK" & vbCrLf)
                //strquery.Append("   AND MAIN1.QUOTATION_MST_PK = " & QuotationPk)

                strquery.Append("SELECT MAIN1.QUOTATION_MST_PK,");
                strquery.Append("       fcl_lcl.QUOTE_DTL_PK quote_trn_sea_pk,");
                strquery.Append("       FRT3.FREIGHT_ELEMENT_NAME,");
                strquery.Append("       CURR3.CURRENCY_ID,");
                strquery.Append("       QUOT_OTHER.AMOUNT, CTMT.CONTAINER_TYPE_MST_ID ");
                strquery.Append("  FROM QUOTATION_MST_TBL          MAIN1,");
                strquery.Append("       QUOTATION_DTL_TBL  FCL_LCL,");
                strquery.Append("       QUOTATION_OTHER_FREIGHT_TRN QUOT_OTHER,");
                strquery.Append("       FREIGHT_ELEMENT_MST_TBL    FRT3,");
                strquery.Append("       CURRENCY_TYPE_MST_TBL      CURR3, CONTAINER_TYPE_MST_TBL     CTMT ");
                strquery.Append(" WHERE FCL_LCL.QUOTATION_MST_FK = MAIN1.QUOTATION_MST_PK");
                strquery.Append("   AND QUOT_OTHER.QUOTATION_DTL_FK = FCL_LCL.QUOTE_DTL_PK");
                strquery.Append("   AND QUOT_OTHER.FREIGHT_ELEMENT_MST_FK = FRT3.FREIGHT_ELEMENT_MST_PK");
                strquery.Append("   AND QUOT_OTHER.CURRENCY_MST_FK = CURR3.CURRENCY_MST_PK AND CTMT.CONTAINER_TYPE_MST_PK = FCL_LCL.CONTAINER_TYPE_MST_FK ");
                strquery.Append("   AND MAIN1.QUOTATION_MST_PK =  " + QuotationPk);

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
        public DataSet PortGrpPOL(string QuotationPk)
        {
            try
            {
                WorkFlow ObjWk = new WorkFlow();
                StringBuilder strquery = new StringBuilder();
                strquery.Append("SELECT DISTINCT CASE WHEN QT.PORT_GROUP = 1 THEN  PGM.PORT_GRP_NAME || ' - ' || substr(CONCATINATE_PORT_GROUP_POL(qt.QUOTATION_MST_PK),3,LENGTH(CONCATINATE_PORT_GROUP_POD(qt.QUOTATION_MST_PK))) ELSE NULL END PORT_GRP_CODE");
                strquery.Append("FROM QUOTATION_MST_TBL         QT,");
                strquery.Append("QUOTATION_DTL_TBL TRN,");
                strquery.Append("PORT_MST_TBL              POL,");
                strquery.Append("PORT_MST_TBL              POD,");
                strquery.Append("PORT_GRP_MST_TBL PGM ");
                strquery.Append("WHERE QT.QUOTATION_MST_PK = TRN.QUOTATION_MST_FK");
                strquery.Append("AND TRN.PORT_MST_POL_FK = POL.PORT_MST_PK");
                strquery.Append("AND TRN.PORT_MST_POD_FK = POD.PORT_MST_PK");
                strquery.Append("AND TRN.POL_GRP_FK= PGM.PORT_GRP_MST_PK(+)");
                strquery.Append("   AND QT.QUOTATION_MST_PK =  " + QuotationPk);
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
        public DataSet PortGrpPOD(string QuotationPk)
        {
            try
            {
                WorkFlow ObjWk = new WorkFlow();
                StringBuilder strquery = new StringBuilder();
                strquery.Append("SELECT DISTINCT CASE WHEN QT.PORT_GROUP = 1 THEN  PGM.PORT_GRP_NAME || ' - ' || substr(CONCATINATE_PORT_GROUP_POD(qt.QUOTATION_MST_PK),3,LENGTH(CONCATINATE_PORT_GROUP_POD(qt.QUOTATION_MST_PK))) ELSE NULL END PORT_GRP_CODE");
                strquery.Append("FROM QUOTATION_MST_TBL         QT,");
                strquery.Append("QUOTATION_DTL_TBL TRN,");
                strquery.Append("PORT_MST_TBL              POL,");
                strquery.Append("PORT_MST_TBL              POD,");
                strquery.Append("PORT_GRP_MST_TBL PGM ");
                strquery.Append("WHERE QT.QUOTATION_MST_PK = TRN.QUOTATION_MST_FK");
                strquery.Append("AND TRN.PORT_MST_POL_FK = POL.PORT_MST_PK");
                strquery.Append("AND TRN.PORT_MST_POD_FK = POD.PORT_MST_PK");
                strquery.Append("AND TRN.POD_GRP_FK = PGM.PORT_GRP_MST_PK(+)");
                strquery.Append("AND QT.QUOTATION_MST_PK =  " + QuotationPk);
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
                strQuery.Append("  ");
                strQuery.Append("         SELECT Q.CUSTOMER_NAME,");
                strQuery.Append("         Q.ADM_ADDRESS_1,");
                strQuery.Append("         case when Q.ADM_ADDRESS_2 is null then ' ' else Q.ADM_ADDRESS_2 end ADM_ADDRESS_2,");
                strQuery.Append("         case when Q.ADM_ADDRESS_3 is null then ' ' else Q.ADM_ADDRESS_3 end ADM_ADDRESS_3,");
                strQuery.Append("         case when Q.ADM_ZIP_CODE is null then ' ' else Q.ADM_ZIP_CODE end ADM_ZIP_CODE,");
                strQuery.Append("         case when Q.ADM_CITY is null then ' ' else Q.ADM_CITY end ADM_CITY,");
                strQuery.Append("         case when Q.COUNTRY_NAME is null then ' ' else Q.COUNTRY_NAME end COUNTRY_NAME");
                strQuery.Append("         FROM  (");
                strQuery.Append("  SELECT CUST.CUSTOMER_NAME,");
                strQuery.Append("         CC.ADM_ADDRESS_1,");
                strQuery.Append("         CC.ADM_ADDRESS_2,");
                strQuery.Append("         CC.ADM_ADDRESS_3,");
                strQuery.Append("         CC.ADM_ZIP_CODE,");
                strQuery.Append("         CC.ADM_CITY,");
                strQuery.Append("         CCC.COUNTRY_NAME");
                strQuery.Append("    FROM CUSTOMER_MST_TBL      CUST,");
                strQuery.Append("         CUSTOMER_CONTACT_DTLS CC,");
                strQuery.Append("         COUNTRY_MST_TBL       CCC");
                strQuery.Append("   WHERE CC.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK");
                strQuery.Append("     AND CC.ADM_COUNTRY_MST_FK = CCC.COUNTRY_MST_PK");
                strQuery.Append("     AND CUST.CUSTOMER_MST_PK =" + CustPk);
                if (CustId.Trim().Length > 0)
                    strQuery.Append("     AND CUST.CUSTOMER_NAME = '" + CustId + "'");
                strQuery.Append("");
                strQuery.Append(" UNION ");
                strQuery.Append("");
                strQuery.Append("  SELECT CUST.CUSTOMER_NAME,");
                strQuery.Append("         CC.ADM_ADDRESS_1,");
                strQuery.Append("         CC.ADM_ADDRESS_2,");
                strQuery.Append("         CC.ADM_ADDRESS_3,");
                strQuery.Append("         CC.ADM_ZIP_CODE,");
                strQuery.Append("         CC.ADM_CITY,");
                strQuery.Append("         (select ccc.country_name from country_mst_tbl ccc, location_mst_tbl l ");
                strQuery.Append("         where l.country_mst_fk = ccc.country_mst_pk ");
                strQuery.Append("         and l.location_mst_pk = cc.adm_location_mst_fk)  country_name");
                strQuery.Append("    FROM TEMP_CUSTOMER_TBL      CUST,");
                strQuery.Append("         TEMP_CUSTOMER_CONTACT_DTLS CC");
                strQuery.Append("   WHERE CC.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK");
                strQuery.Append("     AND CUST.CUSTOMER_MST_PK =" + CustPk);
                if (CustId.Trim().Length > 0)
                    strQuery.Append("     AND CUST.CUSTOMER_NAME = '" + CustId + "'");
                strQuery.Append("     )Q");

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

        #region "Fetch Quotation Int32"
        public string FetchQuotNr(int QuotPK)
        {
            try
            {
                string strSQL = null;
                WorkFlow objWF = new WorkFlow();
                strSQL = " SELECT Q.QUOTATION_REF_NO FROM QUOTATION_MST_TBL Q WHERE Q.QUOTATION_MST_PK = " + QuotPK;
                return objWF.ExecuteScaler(strSQL);
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
        public string FetchPrtGroup(int QuotPK)
        {
            try
            {
                string strSQL = null;
                WorkFlow objWF = new WorkFlow();
                strSQL = " SELECT NVL(Q.PORT_GROUP,0) PORT_GROUP FROM QUOTATION_MST_TBL Q WHERE Q.QUOTATION_MST_PK = " + QuotPK;
                return objWF.ExecuteScaler(strSQL);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public DataSet FetchApprovalDtl(int QuotPK)
        {
            try
            {
                string strSQL = null;
                WorkFlow objWF = new WorkFlow();
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("SELECT U.USER_ID, TO_CHAR(Q.APP_DATE, DATEFORMAT) APP_DATE,Q.STATUS");
                sb.Append("  FROM QUOTATION_MST_TBL Q, USER_MST_TBL U");
                sb.Append(" WHERE Q.APP_BY = U.USER_MST_PK");
                sb.Append("   AND Q.QUOTATION_MST_PK = " + QuotPK);
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

        #region " Customer Contract Query "

        //Private Function CustContQuery(Optional ByVal forFCL As Boolean = True, _
        //                                Optional ByRef CustNo As Object = "", _
        //                                Optional ByRef SectorContainers As Object = "", _
        //                                Optional ByRef CommodityGroup As Object = "", _
        //                                Optional ByRef ShipDate As Object = "" _
        //                              ) As String
        //    Dim strSQL As String
        //    Dim exchQueryFCL, exchQueryLCL, OperatorRate As String
        //    If forFCL Then
        //        exchQueryFCL = " ( Select nvl(Sum(NVL(APP_SURCHARGE_AMT * EXCHANGE_RATE,0)),0)              " & vbCrLf & _
        //                    "       from  CONT_CUST_TRN_SEA_TBL t2, V_EXCHANGE_RATE v2,                     " & vbCrLf & _
        //                    "               CONT_SUR_CHRG_SEA_TBL f2                                        " & vbCrLf & _
        //                    "       where t2.CONT_CUST_TRN_SEA_PK  = tran2.CONT_CUST_TRN_SEA_PK and         " & vbCrLf & _
        //                    "               t2.CONT_CUST_TRN_SEA_PK  = f2.CONT_CUST_TRN_SEA_FK and          " & vbCrLf & _
        //                    "               f2.CHECK_FOR_ALL_IN_RT   = 1 AND                                " & vbCrLf & _
        //                    "               f2.CURRENCY_MST_FK =  v2.CURRENCY_MST_FK AND                    " & vbCrLf & _
        //                    "               ROUND(sysdate-0.5) between v2.FROM_DATE and v2.TO_DATE          " & vbCrLf & _
        //                    "    )    +                                                                     " & vbCrLf & _
        //                    " ( Select NVL(Sum(NVL(FCL_REQ_RATE * EXCHANGE_RATE,0 ) ),0) from               " & vbCrLf & _
        //                    "         TARIFF_TRN_SEA_FCL_LCL tt5, V_EXCHANGE_RATE vv5,                      " & vbCrLf & _
        //                    "         TARIFF_MAIN_SEA_TBL mm5, TABLE(tt5.CONTAINER_DTL_FCL) (+) cc5         " & vbCrLf & _
        //                    "   where cc5.CONTAINER_TYPE_MST_FK = tran2.CONTAINER_TYPE_MST_FK  AND          " & vbCrLf & _
        //                    "         mm5.TARIFF_MAIN_SEA_PK    = tt5.TARIFF_MAIN_SEA_FK       AND          " & vbCrLf & _
        //                    "         tt5.PORT_MST_POL_FK       = tran2.PORT_MST_POL_FK        AND          " & vbCrLf & _
        //                    "         tt5.PORT_MST_POD_FK       = tran2.PORT_MST_POD_FK        AND          " & vbCrLf & _
        //                    "         tt5.CURRENCY_MST_FK       = vv5.CURRENCY_MST_FK          AND          " & vbCrLf & _
        //                    "         tt5.CHECK_FOR_ALL_IN_RT   = 1                            AND          " & vbCrLf & _
        //                    "         ROUND(sysdate-0.5) between vv5.FROM_DATE and vv5.TO_DATE AND          " & vbCrLf & _
        //                    "         tran2.SUBJECT_TO_SURCHG_CHG = 1                          AND          " & vbCrLf & _
        //                    "         mm5.OPERATOR_MST_FK       = main2.OPERATOR_MST_FK        AND          " & vbCrLf & _
        //                    "         mm5.CARGO_TYPE            = 1                            AND          " & vbCrLf & _
        //                    "         mm5.ACTIVE                = 1                            AND          " & vbCrLf & _
        //                    "         mm5.COMMODITY_GROUP_FK    = " & CStr(CommodityGroup) & " AND          " & vbCrLf & _
        //                    "         ROUND(TO_DATE('" & ShipDate & "','" & dateFormat & "')-0.5) BETWEEN           " & vbCrLf & _
        //                    "         tt5.VALID_FROM   AND   NVL(tt5.VALID_TO,NULL_DATE_FORMAT)   AND          " & vbCrLf & _
        //                    "         tt5.FREIGHT_ELEMENT_MST_FK not in                                     " & vbCrLf & _
        //                    "           ( Select FREIGHT_ELEMENT_MST_FK                                     " & vbCrLf & _
        //                    "              from CONT_CUST_TRN_SEA_TBL tt2, CONT_SUR_CHRG_SEA_TBL ff2 where  " & vbCrLf & _
        //                    "                   tt2.CONT_CUST_TRN_SEA_PK  = tran2.CONT_CUST_TRN_SEA_PK and  " & vbCrLf & _
        //                    "                   tt2.CONT_CUST_TRN_SEA_PK  = ff2.CONT_CUST_TRN_SEA_FK and    " & vbCrLf & _
        //                    "                   ff2.CHECK_FOR_ALL_IN_RT   = 1                               " & vbCrLf & _
        //                    "           )                                                                   " & vbCrLf & _
        //                    "  )                                                                            "

        //        OperatorRate = " ( Select nvl(Sum(NVL(FCL_APP_RATE * EXCHANGE_RATE,0)),0)               " & vbCrLf & _
        //                    "   from  CONT_MAIN_SEA_TBL mx, CONT_TRN_SEA_FCL_LCL tx, V_EXCHANGE_RATE vx," & vbCrLf & _
        //                    "         TABLE(tx.CONTAINER_DTL_FCL) (+) cx                                " & vbCrLf & _
        //                    "   where mx.ACTIVE                     = 1     AND                         " & vbCrLf & _
        //                    "         mx.CONT_APPROVED              = 1     AND                         " & vbCrLf & _
        //                    "         mx.CARGO_TYPE                 = 1     AND                         " & vbCrLf & _
        //                    "         mx.OPERATOR_MST_FK            = main2.OPERATOR_MST_FK AND         " & vbCrLf & _
        //                    "         mx.COMMODITY_GROUP_FK         =  " & CStr(CommodityGroup) & " AND " & vbCrLf & _
        //                    "         ROUND(TO_DATE('" & ShipDate & "','" & dateFormat & "')-0.5) BETWEEN       " & vbCrLf & _
        //                    "         tx.VALID_FROM   AND   NVL(tx.VALID_TO,NULL_DATE_FORMAT)          AND " & vbCrLf & _
        //                    "         tx.CONT_MAIN_SEA_FK           = mx.CONT_MAIN_SEA_PK           AND " & vbCrLf & _
        //                    "         cx.CONTAINER_TYPE_MST_FK      = tran2.CONTAINER_TYPE_MST_FK   AND " & vbCrLf & _
        //                    "         tx.PORT_MST_POL_FK            = tran2.PORT_MST_POL_FK         AND " & vbCrLf & _
        //                    "         tx.PORT_MST_POD_FK            = tran2.PORT_MST_POD_FK         AND " & vbCrLf & _
        //                    "         tx.CHECK_FOR_ALL_IN_RT        = 1                             AND " & vbCrLf & _
        //                    "         tx.CURRENCY_MST_FK            = vx.CURRENCY_MST_FK AND            " & vbCrLf & _
        //                    "         ROUND(sysdate-0.5) between vx.FROM_DATE and vx.TO_DATE )            "

        //    Else
        //        exchQueryLCL = " ( Select nvl(Sum(NVL(APP_SURCHARGE_AMT * EXCHANGE_RATE,0)),0)              " & vbCrLf & _
        //                    "       from  CONT_CUST_TRN_SEA_TBL t2, V_EXCHANGE_RATE v2,                     " & vbCrLf & _
        //                    "               CONT_SUR_CHRG_SEA_TBL f2                                        " & vbCrLf & _
        //                    "       where t2.CONT_CUST_TRN_SEA_PK  = tran2.CONT_CUST_TRN_SEA_PK and         " & vbCrLf & _
        //                    "               t2.CONT_CUST_TRN_SEA_PK  = f2.CONT_CUST_TRN_SEA_FK and          " & vbCrLf & _
        //                    "               f2.CHECK_FOR_ALL_IN_RT   = 1 AND                                " & vbCrLf & _
        //                    "               f2.CURRENCY_MST_FK =  v2.CURRENCY_MST_FK AND                    " & vbCrLf & _
        //                    "               ROUND(sysdate-0.5) between v2.FROM_DATE and v2.TO_DATE          " & vbCrLf & _
        //                    "    )    +                                                                     " & vbCrLf & _
        //                    " ( Select NVL(Sum(NVL(LCL_TARIFF_RATE * EXCHANGE_RATE,0 ) ),0) from            " & vbCrLf & _
        //                    "         TARIFF_TRN_SEA_FCL_LCL tt5, V_EXCHANGE_RATE vv5,                      " & vbCrLf & _
        //                    "         TARIFF_MAIN_SEA_TBL mm5                                               " & vbCrLf & _
        //                    "   where                                                                       " & vbCrLf & _
        //                    "         mm5.TARIFF_MAIN_SEA_PK    = tt5.TARIFF_MAIN_SEA_FK       AND          " & vbCrLf & _
        //                    "         tt5.PORT_MST_POL_FK       = tran2.PORT_MST_POL_FK        AND          " & vbCrLf & _
        //                    "         tt5.PORT_MST_POD_FK       = tran2.PORT_MST_POD_FK        AND          " & vbCrLf & _
        //                    "         tt5.CURRENCY_MST_FK       = vv5.CURRENCY_MST_FK          AND          " & vbCrLf & _
        //                    "         tt5.CHECK_FOR_ALL_IN_RT   = 1                            AND          " & vbCrLf & _
        //                    "         ROUND(sysdate-0.5) between vv5.FROM_DATE and vv5.TO_DATE AND          " & vbCrLf & _
        //                    "         tran2.SUBJECT_TO_SURCHG_CHG = 1                          AND          " & vbCrLf & _
        //                    "         mm5.OPERATOR_MST_FK       = main2.OPERATOR_MST_FK        AND          " & vbCrLf & _
        //                    "         mm5.CARGO_TYPE            = 2                            AND          " & vbCrLf & _
        //                    "         mm5.ACTIVE                = 1                            AND          " & vbCrLf & _
        //                    "         mm5.COMMODITY_GROUP_FK    = " & CStr(CommodityGroup) & " AND          " & vbCrLf & _
        //                    "         ROUND(TO_DATE('" & ShipDate & "','" & dateFormat & "')-0.5) BETWEEN           " & vbCrLf & _
        //                    "         tt5.VALID_FROM   AND   NVL(tt5.VALID_TO,NULL_DATE_FORMAT)   AND          " & vbCrLf & _
        //                    "         tt5.FREIGHT_ELEMENT_MST_FK not in                                     " & vbCrLf & _
        //                    "           ( Select FREIGHT_ELEMENT_MST_FK                                     " & vbCrLf & _
        //                    "              from CONT_CUST_TRN_SEA_TBL tt2, CONT_SUR_CHRG_SEA_TBL ff2 where  " & vbCrLf & _
        //                    "                   tt2.CONT_CUST_TRN_SEA_PK  = tran2.CONT_CUST_TRN_SEA_PK and  " & vbCrLf & _
        //                    "                   tt2.CONT_CUST_TRN_SEA_PK  = ff2.CONT_CUST_TRN_SEA_FK and    " & vbCrLf & _
        //                    "                   ff2.CHECK_FOR_ALL_IN_RT   = 1                               " & vbCrLf & _
        //                    "           )                                                                   " & vbCrLf & _
        //                    "  )                                                                            "

        //        OperatorRate = " ( Select nvl(Sum(NVL(LCL_APPROVED_RATE * EXCHANGE_RATE,0)),0)          " & vbCrLf & _
        //                    "   from  CONT_MAIN_SEA_TBL mx, CONT_TRN_SEA_FCL_LCL tx, V_EXCHANGE_RATE vx " & vbCrLf & _
        //                    "   where mx.ACTIVE                     = 1     AND                         " & vbCrLf & _
        //                    "         mx.CONT_APPROVED              = 1     AND                         " & vbCrLf & _
        //                    "         mx.CARGO_TYPE                 = 2     AND                         " & vbCrLf & _
        //                    "         mx.OPERATOR_MST_FK            = main2.OPERATOR_MST_FK AND         " & vbCrLf & _
        //                    "         mx.COMMODITY_GROUP_FK         =  " & CStr(CommodityGroup) & " AND " & vbCrLf & _
        //                    "         ROUND(TO_DATE('" & ShipDate & "','" & dateFormat & "')-0.5) BETWEEN       " & vbCrLf & _
        //                    "           tx.VALID_FROM   AND   NVL(tx.VALID_TO,NULL_DATE_FORMAT)        AND " & vbCrLf & _
        //                    "         tx.CONT_MAIN_SEA_FK           = mx.CONT_MAIN_SEA_PK           AND " & vbCrLf & _
        //                    "         tx.LCL_BASIS                  = tran2.LCL_BASIS               AND " & vbCrLf & _
        //                    "         tx.PORT_MST_POL_FK            = tran2.PORT_MST_POL_FK         AND " & vbCrLf & _
        //                    "         tx.PORT_MST_POD_FK            = tran2.PORT_MST_POD_FK         AND " & vbCrLf & _
        //                    "         tx.CHECK_FOR_ALL_IN_RT        = 1                             AND " & vbCrLf & _
        //                    "         tx.CURRENCY_MST_FK            = vx.CURRENCY_MST_FK AND            " & vbCrLf & _
        //                    "         ROUND(sysdate-0.5) between vx.FROM_DATE and vx.TO_DATE )            "
        //    End If

        //    If forFCL Then
        //        strSQL = "    Select      DISTINCT                                             " & vbCrLf _
        //                & "     main2.CONT_CUST_SEA_PK                     PK,                 " & vbCrLf _
        //                & "  " & SRC(SourceType.CustomerContract) & "      TYPE,               " & vbCrLf _
        //                & "     main2.CONT_REF_NO                          REF_NO,             " & vbCrLf _
        //                & "     main2.CONT_REF_NO                          REFNO,              " & vbCrLf _
        //                & "     TO_CHAR(tran2.VALID_TO,'" & dateFormat & "')       SHIP_DATE,          " & vbCrLf _
        //                & "     tran2.PORT_MST_POL_FK                      POL_PK,             " & vbCrLf _
        //                & "     portpol2.PORT_ID                           POL_ID,             " & vbCrLf _
        //                & "     tran2.PORT_MST_POD_FK                      POD_PK,             " & vbCrLf _
        //                & "     portpod2.PORT_ID                           POD_ID,             " & vbCrLf _
        //                & "     main2.OPERATOR_MST_FK                      OPER_PK,            " & vbCrLf _
        //                & "     opr2.OPERATOR_ID                           OPER_ID,            " & vbCrLf _
        //                & "     opr2.OPERATOR_NAME                         OPER_NAME,          " & vbCrLf _
        //                & "     tran2.CONTAINER_TYPE_MST_FK                CNTR_PK,            " & vbCrLf _
        //                & "     cntr2.CONTAINER_TYPE_MST_ID                CNTR_ID,            " & vbCrLf _
        //                & "     tran2.EXPECTED_VOLUME                      QUANTITY,           " & vbCrLf _
        //                & "     main2.COMMODITY_MST_FK                     COMM_PK,            " & vbCrLf _
        //                & "     NVL(cmdt2.COMMODITY_ID,'')                 COMM_ID,            " & vbCrLf _
        //                & "     NVL(" & exchQueryFCL & ",0)                ALL_IN_TARIFF,      " & vbCrLf _
        //                & "     NVL(" & OperatorRate & ",0)                OPERATOR_RATE,      " & vbCrLf _
        //                & "     NULL                                       TARIFF,             " & vbCrLf _
        //                & "     NULL                                       NET,                " & vbCrLf _
        //                & "     'false'                                    SELECTED,           " & vbCrLf _
        //                & "   " & SourceType.CustomerContract & "          PRIORITYORDER       " & vbCrLf _
        //                & "    from                                                            " & vbCrLf _
        //                & "     CONT_CUST_SEA_TBL              main2,                          " & vbCrLf _
        //                & "     CONT_CUST_TRN_SEA_TBL          tran2,                          " & vbCrLf _
        //                & "     PORT_MST_TBL                   portpol2,                       " & vbCrLf _
        //                & "     PORT_MST_TBL                   portpod2,                       " & vbCrLf _
        //                & "     OPERATOR_MST_TBL               opr2,                           " & vbCrLf _
        //                & "     CONTAINER_TYPE_MST_TBL         cntr2,                          " & vbCrLf _
        //                & "     COMMODITY_MST_TBL              cmdt2                           " & vbCrLf _
        //                & "    where                                                                " & vbCrLf _
        //                & "            main2.CONT_CUST_SEA_PK      = tran2.CONT_CUST_SEA_FK              " & vbCrLf _
        //                & "     AND    main2.CARGO_TYPE            = 1                                   " & vbCrLf _
        //                & "     AND    tran2.PORT_MST_POL_FK       = portpol2.PORT_MST_PK(+)             " & vbCrLf _
        //                & "     AND    tran2.PORT_MST_POD_FK       = portpod2.PORT_MST_PK(+)             " & vbCrLf _
        //                & "     AND    main2.OPERATOR_MST_FK       = opr2.OPERATOR_MST_PK(+)             " & vbCrLf _
        //                & "     AND    tran2.CONTAINER_TYPE_MST_FK = cntr2.CONTAINER_TYPE_MST_PK(+)      " & vbCrLf _
        //                & "     AND    main2.COMMODITY_MST_FK      = cmdt2.COMMODITY_MST_PK(+)           " & vbCrLf _
        //                & "     AND    main2.COMMODITY_GROUP_MST_FK = " & CStr(CommodityGroup) & "       " & vbCrLf _
        //                & "     AND    main2.CUSTOMER_MST_FK       =  " & CStr(CustNo) & "               " & vbCrLf _
        //                & "     AND    (tran2.PORT_MST_POL_FK, tran2.PORT_MST_POD_FK,                    " & vbCrLf _
        //                & "                                   tran2.CONTAINER_TYPE_MST_FK )              " & vbCrLf _
        //                & "              in ( " & CStr(SectorContainers) & " )                           " & vbCrLf _
        //                & "     AND    ROUND(TO_DATE('" & ShipDate & "','" & dateFormat & "')-0.5) BETWEEN       " & vbCrLf _
        //                & "            tran2.VALID_FROM   AND   NVL(tran2.VALID_TO,NULL_DATE_FORMAT)        "


        //    Else
        //        strSQL = "    Select     DISTINCT                                 " & vbCrLf _
        //                & "     main2.CONT_CUST_SEA_PK                     PK,                  " & vbCrLf _
        //                & "  " & SRC(SourceType.CustomerContract) & "      TYPE,                " & vbCrLf _
        //                & "     main2.CONT_REF_NO                          REF_NO,              " & vbCrLf _
        //                & "     TO_CHAR(tran2.VALID_TO,'" & dateFormat & "')       SHIP_DATE,           " & vbCrLf _
        //                & "     tran2.PORT_MST_POL_FK                      POL_PK,              " & vbCrLf _
        //                & "     portpol2.PORT_ID                           POL_ID,              " & vbCrLf _
        //                & "     tran2.PORT_MST_POD_FK                      POD_PK,              " & vbCrLf _
        //                & "     portpod2.PORT_ID                           POD_ID,              " & vbCrLf _
        //                & "     main2.OPERATOR_MST_FK                      OPER_PK,             " & vbCrLf _
        //                & "     opr2.OPERATOR_ID                           OPER_ID,             " & vbCrLf _
        //                & "     opr2.OPERATOR_NAME                         OPER_NAME,           " & vbCrLf _
        //                & "     main2.COMMODITY_MST_FK                     COMM_PK,             " & vbCrLf _
        //                & "     NVL(cmdt2.COMMODITY_ID,'')                 COMM_ID,             " & vbCrLf _
        //                & "     tran2.LCL_BASIS                            LCL_BASIS,           " & vbCrLf _
        //                & "     NVL(dim2.DIMENTION_ID,'')                  DIMENTION_ID,        " & vbCrLf _
        //                & "     0                                          WEIGHT,              " & vbCrLf _
        //                & "     0                                          VOLUME,              " & vbCrLf _
        //                & "     NVL(" & exchQueryLCL & ",0)                ALL_IN_TARIFF,       " & vbCrLf _
        //                & "     'false'                                    SELECTED,            " & vbCrLf _
        //                & "     NVL(" & OperatorRate & ",0)                OPERATOR_RATE,       " & vbCrLf _
        //                & "     " & SourceType.CustomerContract & "        PRIORITYORDER        " & vbCrLf _
        //                & "    from                                                             " & vbCrLf _
        //                & "     CONT_CUST_SEA_TBL              main2,                           " & vbCrLf _
        //                & "     CONT_CUST_TRN_SEA_TBL          tran2,                           " & vbCrLf _
        //                & "     PORT_MST_TBL                   portpol2,                        " & vbCrLf _
        //                & "     PORT_MST_TBL                   portpod2,                        " & vbCrLf _
        //                & "     OPERATOR_MST_TBL               opr2,                            " & vbCrLf _
        //                & "     COMMODITY_MST_TBL              cmdt2,                           " & vbCrLf _
        //                & "     DIMENTION_UNIT_MST_TBL         dim2                             " & vbCrLf _
        //                & "    where                                                                " & vbCrLf _
        //                & "            main2.CONT_CUST_SEA_PK      = tran2.CONT_CUST_SEA_FK               " & vbCrLf _
        //                & "     AND    main2.CARGO_TYPE            = 2                                    " & vbCrLf _
        //                & "     AND    tran2.PORT_MST_POL_FK       = portpol2.PORT_MST_PK(+)              " & vbCrLf _
        //                & "     AND    tran2.PORT_MST_POD_FK       = portpod2.PORT_MST_PK(+)              " & vbCrLf _
        //                & "     AND    main2.OPERATOR_MST_FK       = opr2.OPERATOR_MST_PK(+)              " & vbCrLf _
        //                & "     AND    main2.COMMODITY_MST_FK      = cmdt2.COMMODITY_MST_PK(+)            " & vbCrLf _
        //                & "     AND    main2.COMMODITY_GROUP_MST_FK = " & CStr(CommodityGroup) & "        " & vbCrLf _
        //                & "     AND    tran2.LCL_BASIS             = dim2.DIMENTION_UNIT_MST_PK(+)        " & vbCrLf _
        //                & "     AND    (tran2.PORT_MST_POL_FK, tran2.PORT_MST_POD_FK,                     " & vbCrLf _
        //                & "                                   tran2.LCL_BASIS )                           " & vbCrLf _
        //                & "              in ( " & CStr(SectorContainers) & " )                            " & vbCrLf _
        //                & "     AND    main2.CUSTOMER_MST_FK       = " & CStr(CustNo) & "                 " & vbCrLf _
        //                & "     AND    ROUND(TO_DATE('" & ShipDate & "','" & dateFormat & "')-0.5) BETWEEN        " & vbCrLf _
        //                & "            tran2.VALID_FROM   AND   NVL(tran2.VALID_TO,NULL_DATE_FORMAT)         "
        //    End If
        //    strSQL = strSQL.Replace("   ", " ")
        //    strSQL = strSQL.Replace("  ", " ")
        //    Return strSQL
        //End Function


        //Private Function CustContFreightQuery(Optional ByVal forFCL As Boolean = True, _
        //                                        Optional ByRef CustNo As Object = "", _
        //                                        Optional ByRef SectorContainers As Object = "", _
        //                                        Optional ByRef CommodityGroup As Object = "", _
        //                                        Optional ByRef ShipDate As Object = "" _
        //                                     ) As String
        //    Dim strSQL As String
        //    Dim CargoType As String = "2"
        //    If forFCL = True Then
        //        CargoType = "1"
        //    End If

        //    Dim strContRefNo, strFreightElements, strSurcharge, strContSectors As String
        //    Dim strContNoLCL, strFreightsLCL, strSurchargeLCL, strBasisSectors As String

        //    strContRefNo = " (   Select    DISTINCT  CONT_REF_NO " & vbCrLf _
        //                & "    from                                                             " & vbCrLf _
        //                & "     CONT_CUST_SEA_TBL              main7,                           " & vbCrLf _
        //                & "     CONT_CUST_TRN_SEA_TBL          tran7                            " & vbCrLf _
        //                & "    where                                                                " & vbCrLf _
        //                & "            main7.CONT_CUST_SEA_PK      = tran7.CONT_CUST_SEA_FK              " & vbCrLf _
        //                & "     AND    main7.CARGO_TYPE            = 1                                   " & vbCrLf _
        //                & "     AND    main7.COMMODITY_GROUP_MST_FK = " & CStr(CommodityGroup) & "       " & vbCrLf _
        //                & "     AND    main7.CUSTOMER_MST_FK       =  " & CStr(CustNo) & "               " & vbCrLf _
        //                & "     AND    tran7.PORT_MST_POL_FK       =  tran6.PORT_MST_POL_FK              " & vbCrLf _
        //                & "     AND    tran7.PORT_MST_POD_FK       =  tran6.PORT_MST_POD_FK              " & vbCrLf _
        //                & "     AND    tran7.CONTAINER_TYPE_MST_FK =  cont6.CONTAINER_TYPE_MST_FK        " & vbCrLf _
        //                & "     AND    ROUND(TO_DATE('" & ShipDate & "','" & dateFormat & "')-0.5) BETWEEN       " & vbCrLf _
        //                & "            tran7.VALID_FROM   AND   NVL(tran7.VALID_TO,NULL_DATE_FORMAT)        " & vbCrLf _
        //                & "     AND    main7.OPERATOR_MST_FK =  main6.OPERATOR_MST_FK  )    "

        //    strContNoLCL = " (   Select    DISTINCT  CONT_REF_NO " & vbCrLf _
        //                & "    from                                                             " & vbCrLf _
        //                & "     CONT_CUST_SEA_TBL              main7,                           " & vbCrLf _
        //                & "     CONT_CUST_TRN_SEA_TBL          tran7                            " & vbCrLf _
        //                & "    where                                                                " & vbCrLf _
        //                & "            main7.CONT_CUST_SEA_PK      = tran7.CONT_CUST_SEA_FK              " & vbCrLf _
        //                & "     AND    main7.CARGO_TYPE            = 2                                   " & vbCrLf _
        //                & "     AND    main7.COMMODITY_GROUP_MST_FK = " & CStr(CommodityGroup) & "       " & vbCrLf _
        //                & "     AND    main7.CUSTOMER_MST_FK       =  " & CStr(CustNo) & "               " & vbCrLf _
        //                & "     AND    tran7.PORT_MST_POL_FK       =  tran6.PORT_MST_POL_FK              " & vbCrLf _
        //                & "     AND    tran7.PORT_MST_POD_FK       =  tran6.PORT_MST_POD_FK              " & vbCrLf _
        //                & "     AND    tran7.LCL_BASIS             =  tran6.LCL_BASIS                    " & vbCrLf _
        //                & "     AND    ROUND(TO_DATE('" & ShipDate & "','" & dateFormat & "')-0.5) BETWEEN       " & vbCrLf _
        //                & "            tran7.VALID_FROM   AND   NVL(tran7.VALID_TO,NULL_DATE_FORMAT)        " & vbCrLf _
        //                & "     AND    main7.OPERATOR_MST_FK =  main6.OPERATOR_MST_FK  )    "

        //    strFreightElements = " (   Select   DISTINCT  frtd8.FREIGHT_ELEMENT_MST_FK " & vbCrLf _
        //                & "    from                                                             " & vbCrLf _
        //                & "     CONT_CUST_SEA_TBL              main8,                           " & vbCrLf _
        //                & "     CONT_CUST_TRN_SEA_TBL          tran8,                           " & vbCrLf _
        //                & "     CONT_SUR_CHRG_SEA_TBL          frtd8                            " & vbCrLf _
        //                & "    where                                                                " & vbCrLf _
        //                & "            main8.CONT_CUST_SEA_PK      = tran8.CONT_CUST_SEA_FK              " & vbCrLf _
        //                & "     AND    tran8.CONT_CUST_TRN_SEA_PK  = frtd8.CONT_CUST_TRN_SEA_FK          " & vbCrLf _
        //                & "     AND    main8.CARGO_TYPE            = 1                                   " & vbCrLf _
        //                & "     AND    main8.COMMODITY_GROUP_MST_FK = " & CStr(CommodityGroup) & "       " & vbCrLf _
        //                & "     AND    main8.CUSTOMER_MST_FK       =  " & CStr(CustNo) & "               " & vbCrLf _
        //                & "     AND    tran8.PORT_MST_POL_FK       = tran6.PORT_MST_POL_FK               " & vbCrLf _
        //                & "     AND    tran8.PORT_MST_POD_FK       = tran6.PORT_MST_POD_FK               " & vbCrLf _
        //                & "     AND    tran8.CONTAINER_TYPE_MST_FK = cont6.CONTAINER_TYPE_MST_FK         " & vbCrLf _
        //                & "     AND    ROUND(TO_DATE('" & ShipDate & "','" & dateFormat & "')-0.5) BETWEEN       " & vbCrLf _
        //                & "            tran8.VALID_FROM   AND   NVL(tran8.VALID_TO,NULL_DATE_FORMAT)        " & vbCrLf _
        //                & "     AND    main8.OPERATOR_MST_FK       =  main6.OPERATOR_MST_FK          )   "

        //    strFreightsLCL = " (   Select   DISTINCT  frtd8.FREIGHT_ELEMENT_MST_FK " & vbCrLf _
        //                & "    from                                                             " & vbCrLf _
        //                & "     CONT_CUST_SEA_TBL              main8,                           " & vbCrLf _
        //                & "     CONT_CUST_TRN_SEA_TBL          tran8,                           " & vbCrLf _
        //                & "     CONT_SUR_CHRG_SEA_TBL          frtd8                            " & vbCrLf _
        //                & "    where                                                                " & vbCrLf _
        //                & "            main8.CONT_CUST_SEA_PK      = tran8.CONT_CUST_SEA_FK              " & vbCrLf _
        //                & "     AND    tran8.CONT_CUST_TRN_SEA_PK  = frtd8.CONT_CUST_TRN_SEA_FK          " & vbCrLf _
        //                & "     AND    main8.CARGO_TYPE            = 2                                   " & vbCrLf _
        //                & "     AND    main8.COMMODITY_GROUP_MST_FK = " & CStr(CommodityGroup) & "       " & vbCrLf _
        //                & "     AND    main8.CUSTOMER_MST_FK       =  " & CStr(CustNo) & "               " & vbCrLf _
        //                & "     AND    tran8.PORT_MST_POL_FK       = tran6.PORT_MST_POL_FK               " & vbCrLf _
        //                & "     AND    tran8.PORT_MST_POD_FK       = tran6.PORT_MST_POD_FK               " & vbCrLf _
        //                & "     AND    tran8.LCL_BASIS             = tran6.LCL_BASIS                     " & vbCrLf _
        //                & "     AND    ROUND(TO_DATE('" & ShipDate & "','" & dateFormat & "')-0.5) BETWEEN       " & vbCrLf _
        //                & "            tran8.VALID_FROM   AND   NVL(tran8.VALID_TO,NULL_DATE_FORMAT)        " & vbCrLf _
        //                & "     AND    main8.OPERATOR_MST_FK       =  main6.OPERATOR_MST_FK          )   "


        //    strSurcharge = " ( Select  DISTINCT  SUBJECT_TO_SURCHG_CHG  " & vbCrLf _
        //                & "    from                                                             " & vbCrLf _
        //                & "     CONT_CUST_SEA_TBL              main9,                           " & vbCrLf _
        //                & "     CONT_CUST_TRN_SEA_TBL          tran9                            " & vbCrLf _
        //                & "    where                                                                " & vbCrLf _
        //                & "            main9.CONT_CUST_SEA_PK      = tran9.CONT_CUST_SEA_FK              " & vbCrLf _
        //                & "     AND    main9.CARGO_TYPE            = 1                                   " & vbCrLf _
        //                & "     AND    main9.COMMODITY_GROUP_MST_FK = " & CStr(CommodityGroup) & "       " & vbCrLf _
        //                & "     AND    main9.CUSTOMER_MST_FK       =  " & CStr(CustNo) & "               " & vbCrLf _
        //                & "     AND    tran9.PORT_MST_POL_FK       =  tran6.PORT_MST_POL_FK              " & vbCrLf _
        //                & "     AND    tran9.PORT_MST_POD_FK       =  tran6.PORT_MST_POD_FK              " & vbCrLf _
        //                & "     AND    tran9.CONTAINER_TYPE_MST_FK =  cont6.CONTAINER_TYPE_MST_FK        " & vbCrLf _
        //                & "     AND    ROUND(TO_DATE('" & ShipDate & "','" & dateFormat & "')-0.5) BETWEEN       " & vbCrLf _
        //                & "            tran9.VALID_FROM   AND   NVL(tran9.VALID_TO,NULL_DATE_FORMAT)        " & vbCrLf _
        //                & "     AND    main9.OPERATOR_MST_FK       =  main6.OPERATOR_MST_FK          )   "

        //    strSurchargeLCL = " ( Select  DISTINCT  SUBJECT_TO_SURCHG_CHG  " & vbCrLf _
        //                & "    from                                                             " & vbCrLf _
        //                & "     CONT_CUST_SEA_TBL              main9,                           " & vbCrLf _
        //                & "     CONT_CUST_TRN_SEA_TBL          tran9                            " & vbCrLf _
        //                & "    where                                                                " & vbCrLf _
        //                & "            main9.CONT_CUST_SEA_PK      = tran9.CONT_CUST_SEA_FK              " & vbCrLf _
        //                & "     AND    main9.CARGO_TYPE            = 2                                   " & vbCrLf _
        //                & "     AND    main9.COMMODITY_GROUP_MST_FK = " & CStr(CommodityGroup) & "       " & vbCrLf _
        //                & "     AND    main9.CUSTOMER_MST_FK       =  " & CStr(CustNo) & "               " & vbCrLf _
        //                & "     AND    tran9.PORT_MST_POL_FK       =  tran6.PORT_MST_POL_FK              " & vbCrLf _
        //                & "     AND    tran9.PORT_MST_POD_FK       =  tran6.PORT_MST_POD_FK              " & vbCrLf _
        //                & "     AND    tran9.LCL_BASIS             =  tran6.LCL_BASIS                    " & vbCrLf _
        //                & "     AND    ROUND(TO_DATE('" & ShipDate & "','" & dateFormat & "')-0.5) BETWEEN       " & vbCrLf _
        //                & "            tran9.VALID_FROM   AND   NVL(tran9.VALID_TO,NULL_DATE_FORMAT)        " & vbCrLf _
        //                & "     AND    main9.OPERATOR_MST_FK       =  main6.OPERATOR_MST_FK          )   "

        //    strContSectors = "  Select  DISTINCT  tran10.PORT_MST_POL_FK,                               " & vbCrLf _
        //                & "          tran10.PORT_MST_POD_FK, tran10.CONTAINER_TYPE_MST_FK                " & vbCrLf _
        //                & "    from                                                                      " & vbCrLf _
        //                & "     CONT_CUST_SEA_TBL              main10,                                   " & vbCrLf _
        //                & "     CONT_CUST_TRN_SEA_TBL          tran10                                    " & vbCrLf _
        //                & "    where                                                                     " & vbCrLf _
        //                & "            main10.CONT_CUST_SEA_PK      = tran10.CONT_CUST_SEA_FK            " & vbCrLf _
        //                & "     AND    main10.CARGO_TYPE            = 1                                  " & vbCrLf _
        //                & "     AND    main10.COMMODITY_GROUP_MST_FK = " & CStr(CommodityGroup) & "      " & vbCrLf _
        //                & "     AND    main10.CUSTOMER_MST_FK       =  " & CStr(CustNo) & "              " & vbCrLf _
        //                & "     AND    (tran10.PORT_MST_POL_FK, tran10.PORT_MST_POD_FK,                  " & vbCrLf _
        //                & "                                   tran10.CONTAINER_TYPE_MST_FK )             " & vbCrLf _
        //                & "              in ( " & CStr(SectorContainers) & " )                           " & vbCrLf _
        //                & "     AND    ROUND(TO_DATE('" & ShipDate & "','" & dateFormat & "')-0.5) BETWEEN       " & vbCrLf _
        //                & "            tran10.VALID_FROM   AND   NVL(tran10.VALID_TO,NULL_DATE_FORMAT)      " & vbCrLf _
        //                & "     AND    main10.OPERATOR_MST_FK       =  main6.OPERATOR_MST_FK            "

        //    strBasisSectors = "  Select  DISTINCT  tran10.PORT_MST_POL_FK,                               " & vbCrLf _
        //                & "          tran10.PORT_MST_POD_FK, tran10.LCL_BASIS                            " & vbCrLf _
        //                & "    from                                                                      " & vbCrLf _
        //                & "     CONT_CUST_SEA_TBL              main10,                                   " & vbCrLf _
        //                & "     CONT_CUST_TRN_SEA_TBL          tran10                                    " & vbCrLf _
        //                & "    where                                                                     " & vbCrLf _
        //                & "            main10.CONT_CUST_SEA_PK      = tran10.CONT_CUST_SEA_FK            " & vbCrLf _
        //                & "     AND    main10.CARGO_TYPE            = 2                                  " & vbCrLf _
        //                & "     AND    main10.COMMODITY_GROUP_MST_FK = " & CStr(CommodityGroup) & "      " & vbCrLf _
        //                & "     AND    main10.CUSTOMER_MST_FK       =  " & CStr(CustNo) & "              " & vbCrLf _
        //                & "     AND    (tran10.PORT_MST_POL_FK, tran10.PORT_MST_POD_FK,                  " & vbCrLf _
        //                & "                                   tran10.LCL_BASIS )                         " & vbCrLf _
        //                & "              in ( " & CStr(SectorContainers) & " )                           " & vbCrLf _
        //                & "     AND    ROUND(TO_DATE('" & ShipDate & "','" & dateFormat & "')-0.5) BETWEEN       " & vbCrLf _
        //                & "            tran10.VALID_FROM   AND   NVL(tran10.VALID_TO,NULL_DATE_FORMAT)      " & vbCrLf _
        //                & "     AND    main10.OPERATOR_MST_FK       =  main6.OPERATOR_MST_FK            "

        //    'IF "APPROVED_ALL_IN_RATE" > 0 then
        //    '(1) Elements from child table having "CONT_SUR_CHRG_SEA_TBL.CHECK_FOR ALL_IN_RT"=1 with approved rates 
        //    '(2) BOF with "CONT_CUST_TRN_SEA_TBL.CURRENT_BOF_RATE"
        //    'ELSE
        //    '(1) All Elements from child table with approved rates 
        //    '(2) BOF with "CONT_CUST_TRN_SEA_TBL.APPROVED_BOF_RATE"

        //    strSQL = "    Select     " & vbCrLf _
        //                & "     main22.CONT_REF_NO                          REF_NO,              " & vbCrLf _
        //                & "     tran22.PORT_MST_POL_FK                      POL_PK,              " & vbCrLf _
        //                & "     tran22.PORT_MST_POD_FK                      POD_PK,              " & vbCrLf _
        //                & "     tran22.CONTAINER_TYPE_MST_FK                CNTR_PK,             " & vbCrLf _
        //                & "     frt22.FREIGHT_ELEMENT_MST_PK                FRT_PK,              " & vbCrLf _
        //                & "     frt22.FREIGHT_ELEMENT_ID                    FRT_ID,              " & vbCrLf _
        //                & "     frt22.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " & vbCrLf _
        //                & "     'true'                                      SELECTED,            " & vbCrLf _
        //                & "     frtd22.CURRENCY_MST_FK                      CURR_PK,             " & vbCrLf _
        //                & "     curr22.CURRENCY_ID                          CURR_ID,             " & vbCrLf _
        //                & "   ( Case When NVL(tran22.APPROVED_ALL_IN_RATE,0) > 0 Then    " & _
        //                "            tran22.CURRENT_BOF_RATE                     else    " & _
        //                "            tran22.APPROVED_BOF_RATE End )         RATE,                " & vbCrLf _
        //                & "     NULL                                        QUOTERATE,           " & vbCrLf _
        //                & "     NULL                                        PYTYPE               " & vbCrLf _
        //                & "    from                                                              " & vbCrLf _
        //                & "     CONT_CUST_SEA_TBL              main22,                           " & vbCrLf _
        //                & "     CONT_CUST_TRN_SEA_TBL          tran22,                           " & vbCrLf _
        //                & "     FREIGHT_ELEMENT_MST_TBL        frt22,                            " & vbCrLf _
        //                & "     CURRENCY_TYPE_MST_TBL          curr22                            " & vbCrLf _
        //                & "    where                                                                  " & vbCrLf _
        //                & "            main22.CONT_CUST_SEA_PK      = tran22.CONT_CUST_SEA_FK              " & vbCrLf _
        //                & "     AND    tran22.CONT_CUST_TRN_SEA_PK  = frtd22.CONT_CUST_TRN_SEA_FK          " & vbCrLf _
        //                & "     AND    frt22.FREIGHT_ELEMENT_MST_PK = " & BofPk & "                        " & vbCrLf _
        //                & "     AND    tran22.CURRENCY_MST_FK       = curr22.CURRENCY_MST_PK(+)            " & vbCrLf _
        //                & "     AND    main22.CARGO_TYPE            = " & CargoType & "                    " & vbCrLf _
        //                & "     AND    main22.COMMODITY_GROUP_MST_FK = " & CStr(CommodityGroup) & "        " & vbCrLf _
        //                & "     AND    main22.CUSTOMER_MST_FK       =  " & CStr(CustNo) & "                " & vbCrLf _
        //                & "     AND    (tran22.PORT_MST_POL_FK, tran22.PORT_MST_POD_FK,                    " & vbCrLf _
        //                & "                                   tran22.CONTAINER_TYPE_MST_FK )               " & vbCrLf _
        //                & "              in ( " & CStr(SectorContainers) & " )                             " & vbCrLf _
        //                & "     AND    ROUND(TO_DATE('" & ShipDate & "','" & dateFormat & "')-0.5) BETWEEN " & vbCrLf _
        //                & "            tran22.VALID_FROM   AND   NVL(tran22.VALID_TO,NULL_DATE_FORMAT)        " & vbCrLf _
        //                & "    UNION " & vbCrLf

        //    If forFCL Then
        //        strSQL = "    Select     " & vbCrLf _
        //                & "     main2.CONT_REF_NO                          REF_NO,              " & vbCrLf _
        //                & "     tran2.PORT_MST_POL_FK                      POL_PK,              " & vbCrLf _
        //                & "     tran2.PORT_MST_POD_FK                      POD_PK,              " & vbCrLf _
        //                & "     tran2.CONTAINER_TYPE_MST_FK                CNTR_PK,             " & vbCrLf _
        //                & "     frtd2.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " & vbCrLf _
        //                & "     frt2.FREIGHT_ELEMENT_ID                    FRT_ID,              " & vbCrLf _
        //                & "     frt2.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " & vbCrLf _
        //                & "     DECODE(frtd2.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " & vbCrLf _
        //                & "     frtd2.CURRENCY_MST_FK                      CURR_PK,             " & vbCrLf _
        //                & "     curr2.CURRENCY_ID                          CURR_ID,             " & vbCrLf _
        //                & "     frtd2.APP_SURCHARGE_AMT                    RATE,                " & vbCrLf _
        //                & "     NULL                                       QUOTERATE,           " & vbCrLf _
        //                & "     NULL                                       PYTYPE               " & vbCrLf _
        //                & "    from                                                             " & vbCrLf _
        //                & "     CONT_CUST_SEA_TBL              main2,                           " & vbCrLf _
        //                & "     CONT_CUST_TRN_SEA_TBL          tran2,                           " & vbCrLf _
        //                & "     CONT_SUR_CHRG_SEA_TBL          frtd2,                           " & vbCrLf _
        //                & "     FREIGHT_ELEMENT_MST_TBL        frt2,                            " & vbCrLf _
        //                & "     CURRENCY_TYPE_MST_TBL          curr2                            " & vbCrLf _
        //                & "    where                                                                " & vbCrLf _
        //                & "            main2.CONT_CUST_SEA_PK      = tran2.CONT_CUST_SEA_FK              " & vbCrLf _
        //                & "     AND    tran2.CONT_CUST_TRN_SEA_PK  = frtd2.CONT_CUST_TRN_SEA_FK          " & vbCrLf _
        //                & "     AND    frtd2.FREIGHT_ELEMENT_MST_FK = frt2.FREIGHT_ELEMENT_MST_PK(+)     " & vbCrLf _
        //                & "     AND    frt2.CHARGE_BASIS           <> 2                                  " & vbCrLf _
        //                & "     AND    frtd2.CURRENCY_MST_FK       = curr2.CURRENCY_MST_PK(+)            " & vbCrLf _
        //                & "     AND    main2.CARGO_TYPE            = 1                                   " & vbCrLf _
        //                & "     AND    main2.COMMODITY_GROUP_MST_FK = " & CStr(CommodityGroup) & "       " & vbCrLf _
        //                & "     AND    main2.CUSTOMER_MST_FK       =  " & CStr(CustNo) & "               " & vbCrLf _
        //                & "     AND    (tran2.PORT_MST_POL_FK, tran2.PORT_MST_POD_FK,                    " & vbCrLf _
        //                & "                                   tran2.CONTAINER_TYPE_MST_FK )              " & vbCrLf _
        //                & "              in ( " & CStr(SectorContainers) & " )                           " & vbCrLf _
        //                & "     AND    ROUND(TO_DATE('" & ShipDate & "','" & dateFormat & "')-0.5) BETWEEN       " & vbCrLf _
        //                & "            tran2.VALID_FROM   AND   NVL(tran2.VALID_TO,NULL_DATE_FORMAT)        " & vbCrLf _
        //                & "    UNION  " & vbCrLf _
        //                & "   Select            " & vbCrLf _
        //                & "     " & strContRefNo & "                       REF_NO,              " & vbCrLf _
        //                & "     tran6.PORT_MST_POL_FK                      POL_PK,              " & vbCrLf _
        //                & "     tran6.PORT_MST_POD_FK                      POD_PK,              " & vbCrLf _
        //                & "     cont6.CONTAINER_TYPE_MST_FK                CNTR_PK,             " & vbCrLf _
        //                & "     tran6.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " & vbCrLf _
        //                & "     frt6.FREIGHT_ELEMENT_ID                    FRT_ID,              " & vbCrLf _
        //                & "     frt6.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " & vbCrLf _
        //                & "     DECODE(tran6.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " & vbCrLf _
        //                & "     tran6.CURRENCY_MST_FK                      CURR_PK,             " & vbCrLf _
        //                & "     curr6.CURRENCY_ID                          CURR_ID,             " & vbCrLf _
        //                & "     cont6.FCL_REQ_RATE                         RATE,                " & vbCrLf _
        //                & "     NULL                                       QUOTERATE,           " & vbCrLf _
        //                & "     NULL                                       PYTYPE               " & vbCrLf _
        //                & "    from                                                             " & vbCrLf _
        //                & "     TARIFF_MAIN_SEA_TBL            main6,                           " & vbCrLf _
        //                & "     TARIFF_TRN_SEA_FCL_LCL         tran6,                           " & vbCrLf _
        //                & "     TABLE(tran6.CONTAINER_DTL_FCL) (+) cont6,                       " & vbCrLf _
        //                & "     FREIGHT_ELEMENT_MST_TBL        frt6,                            " & vbCrLf _
        //                & "     CURRENCY_TYPE_MST_TBL          curr6                            " & vbCrLf _
        //                & "    where " & strContRefNo & " IS NOT NULL AND                       " & vbCrLf _
        //                & "            main6.TARIFF_MAIN_SEA_PK    = tran6.TARIFF_MAIN_SEA_FK           " & vbCrLf _
        //                & "     AND    main6.CARGO_TYPE            = 1                                  " & vbCrLf _
        //                & "     AND    main6.ACTIVE                = 1                                  " & vbCrLf _
        //                & "     AND    tran6.FREIGHT_ELEMENT_MST_FK = frt6.FREIGHT_ELEMENT_MST_PK(+)    " & vbCrLf _
        //                & "     AND    frt6.CHARGE_BASIS           <> 2                                 " & vbCrLf _
        //                & "     AND    tran6.CURRENCY_MST_FK       = curr6.CURRENCY_MST_PK(+)           " & vbCrLf _
        //                & "     AND    main6.COMMODITY_GROUP_FK    = " & CStr(CommodityGroup) & "       " & vbCrLf _
        //                & "     AND    (tran6.PORT_MST_POL_FK, tran6.PORT_MST_POD_FK,                   " & vbCrLf _
        //                & "                                   cont6.CONTAINER_TYPE_MST_FK )             " & vbCrLf _
        //                & "              in ( " & strContSectors & " )                                  " & vbCrLf _
        //                & "     AND    ROUND(TO_DATE('" & ShipDate & "','" & dateFormat & "')-0.5) BETWEEN      " & vbCrLf _
        //                & "            tran6.VALID_FROM   AND   NVL(tran6.VALID_TO,NULL_DATE_FORMAT)       " & vbCrLf _
        //                & "     AND    tran6.FREIGHT_ELEMENT_MST_FK NOT IN (" & strFreightElements & ") " & vbCrLf _
        //                & "     AND  " & strSurcharge & " = 1                  " & vbCrLf
        //    Else
        //        strSQL = "    Select            " & vbCrLf _
        //                & "     main2.CONT_REF_NO                          REF_NO,              " & vbCrLf _
        //                & "     tran2.PORT_MST_POL_FK                      POL_PK,              " & vbCrLf _
        //                & "     tran2.PORT_MST_POD_FK                      POD_PK,              " & vbCrLf _
        //                & "     tran2.LCL_BASIS                            LCLBASIS,            " & vbCrLf _
        //                & "     frtd2.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " & vbCrLf _
        //                & "     frt2.FREIGHT_ELEMENT_ID                    FRT_ID,              " & vbCrLf _
        //                & "     frt2.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " & vbCrLf _
        //                & "     DECODE(frtd2.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " & vbCrLf _
        //                & "     frtd2.CURRENCY_MST_FK                      CURR_PK,             " & vbCrLf _
        //                & "     curr2.CURRENCY_ID                          CURR_ID,             " & vbCrLf _
        //                & "     frtd2.APP_SURCHARGE_AMT                    RATE                 " & vbCrLf _
        //                & "    from                                                             " & vbCrLf _
        //                & "     CONT_CUST_SEA_TBL              main2,                           " & vbCrLf _
        //                & "     CONT_CUST_TRN_SEA_TBL          tran2,                           " & vbCrLf _
        //                & "     CONT_SUR_CHRG_SEA_TBL          frtd2,                           " & vbCrLf _
        //                & "     FREIGHT_ELEMENT_MST_TBL        frt2,                            " & vbCrLf _
        //                & "     CURRENCY_TYPE_MST_TBL          curr2                            " & vbCrLf _
        //                & "    where                                                                " & vbCrLf _
        //                & "            main2.CONT_CUST_SEA_PK      = tran2.CONT_CUST_SEA_FK             " & vbCrLf _
        //                & "     AND    tran2.CONT_CUST_TRN_SEA_PK  = frtd2.CONT_CUST_TRN_SEA_FK         " & vbCrLf _
        //                & "     AND    frtd2.FREIGHT_ELEMENT_MST_FK = frt2.FREIGHT_ELEMENT_MST_PK(+)    " & vbCrLf _
        //                & "     AND    frt2.CHARGE_BASIS           <> 2                                 " & vbCrLf _
        //                & "     AND    frtd2.CURRENCY_MST_FK       = curr2.CURRENCY_MST_PK(+)           " & vbCrLf _
        //                & "     AND    main2.CARGO_TYPE            = 2                                  " & vbCrLf _
        //                & "     AND    main2.COMMODITY_GROUP_MST_FK = " & CStr(CommodityGroup) & "      " & vbCrLf _
        //                & "     AND    main2.CUSTOMER_MST_FK       =  " & CStr(CustNo) & "              " & vbCrLf _
        //                & "     AND    (tran2.PORT_MST_POL_FK, tran2.PORT_MST_POD_FK,                   " & vbCrLf _
        //                & "                                   tran2.LCL_BASIS )                         " & vbCrLf _
        //                & "              in ( " & CStr(SectorContainers) & " )                          " & vbCrLf _
        //                & "     AND    ROUND(TO_DATE('" & ShipDate & "','" & dateFormat & "')-0.5) BETWEEN      " & vbCrLf _
        //                & "            tran2.VALID_FROM   AND   NVL(tran2.VALID_TO,NULL_DATE_FORMAT)       " & vbCrLf _
        //                & "    UNION  " & vbCrLf _
        //                & "   Select            " & vbCrLf _
        //                & "     " & strContNoLCL & "                       REF_NO,              " & vbCrLf _
        //                & "     tran6.PORT_MST_POL_FK                      POL_PK,              " & vbCrLf _
        //                & "     tran6.PORT_MST_POD_FK                      POD_PK,              " & vbCrLf _
        //                & "     tran6.LCL_BASIS                            LCLBASIS,            " & vbCrLf _
        //                & "     tran6.FREIGHT_ELEMENT_MST_FK               FRT_PK,              " & vbCrLf _
        //                & "     frt6.FREIGHT_ELEMENT_ID                    FRT_ID,              " & vbCrLf _
        //                & "     frt6.FREIGHT_ELEMENT_NAME                  FRT_NAME,            " & vbCrLf _
        //                & "     DECODE(tran6.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SELECTED,  " & vbCrLf _
        //                & "     tran6.CURRENCY_MST_FK                      CURR_PK,             " & vbCrLf _
        //                & "     curr6.CURRENCY_ID                          CURR_ID,             " & vbCrLf _
        //                & "     tran6.LCL_TARIFF_RATE                      RATE                 " & vbCrLf _
        //                & "    from                                                             " & vbCrLf _
        //                & "     TARIFF_MAIN_SEA_TBL            main6,                           " & vbCrLf _
        //                & "     TARIFF_TRN_SEA_FCL_LCL         tran6,                           " & vbCrLf _
        //                & "     FREIGHT_ELEMENT_MST_TBL        frt6,                            " & vbCrLf _
        //                & "     CURRENCY_TYPE_MST_TBL          curr6                            " & vbCrLf _
        //                & "    where " & strContNoLCL & " IS NOT NULL AND                       " & vbCrLf _
        //                & "            main6.TARIFF_MAIN_SEA_PK    = tran6.TARIFF_MAIN_SEA_FK           " & vbCrLf _
        //                & "     AND    main6.CARGO_TYPE            = 2                                  " & vbCrLf _
        //                & "     AND    main6.ACTIVE                = 1                                  " & vbCrLf _
        //                & "     AND    tran6.FREIGHT_ELEMENT_MST_FK = frt6.FREIGHT_ELEMENT_MST_PK(+)    " & vbCrLf _
        //                & "     AND    frt6.CHARGE_BASIS           <> 2                                  " & vbCrLf _
        //                & "     AND    tran6.CURRENCY_MST_FK       = curr6.CURRENCY_MST_PK(+)           " & vbCrLf _
        //                & "     AND    main6.COMMODITY_GROUP_FK    = " & CStr(CommodityGroup) & "       " & vbCrLf _
        //                & "     AND    (tran6.PORT_MST_POL_FK, tran6.PORT_MST_POD_FK,                   " & vbCrLf _
        //                & "                                   tran6.LCL_BASIS )                         " & vbCrLf _
        //                & "              in ( " & strBasisSectors & " )                                 " & vbCrLf _
        //                & "     AND    ROUND(TO_DATE('" & ShipDate & "','" & dateFormat & "')-0.5) BETWEEN      " & vbCrLf _
        //                & "            tran6.VALID_FROM   AND   NVL(tran6.VALID_TO,NULL_DATE_FORMAT)       " & vbCrLf _
        //                & "     AND    tran6.FREIGHT_ELEMENT_MST_FK NOT IN (" & strFreightsLCL & ")     " & vbCrLf _
        //                & "     AND  " & strSurchargeLCL & " = 1                  " & vbCrLf
        //    End If
        //    strSQL = strSQL.Replace("   ", " ")
        //    strSQL = strSQL.Replace("  ", " ")
        //    Return strSQL
        //End Function

        #endregion

        #region "Fetch Profitability data"
        public ArrayList SaveProfitability(DataSet DsProfitability, int RefFK, int Biz_Type, int CargoType, int RefFlag, int IncTeaStatus, long POLPK, long PODPK, long OperatorPK, DataSet DSProfitability_Local = null)
        {

            int RowCnt = 0;
            int ColCnt = 0;
            int ProfitabilityPK = 0;
            int RecAft = 0;
            int i = 0;
            string ProfPK_ToDel = "0";
            WorkFlow objWF = new WorkFlow();
            OracleTransaction TRAN = default(OracleTransaction);
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();

            arrMessage.Clear();

            for (RowCnt = 0; RowCnt <= DsProfitability.Tables[0].Rows.Count - 1; RowCnt++)
            {
                for (ColCnt = 7; ColCnt <= DsProfitability.Tables[0].Columns.Count - 1; ColCnt++)
                {
                    if (DsProfitability.Tables[0].Columns[ColCnt].Caption.PadLeft(2) == "PK")
                    {
                        if (!string.IsNullOrEmpty(DsProfitability.Tables[0].Rows[RowCnt][ColCnt + 2].ToString()))
                        {
                            if ((Convert.ToInt32(DsProfitability.Tables[0].Rows[RowCnt][ColCnt + 2]) > 0))
                            {
                                if (!string.IsNullOrEmpty(DsProfitability.Tables[0].Rows[RowCnt][ColCnt].ToString()))
                                {
                                    ProfPK_ToDel = ProfPK_ToDel + ", " + DsProfitability.Tables[0].Rows[RowCnt][ColCnt];
                                }
                            }
                        }
                    }
                }
            }

            objWF.OpenConnection();
            TRAN = objWF.MyConnection.BeginTransaction();
            objWF.MyCommand = new OracleCommand();
            objWF.MyCommand.Parameters.Clear();
            objWF.MyCommand.Transaction = TRAN;
            objWF.MyCommand.Connection = objWF.MyConnection;
            try
            {
                if (!string.IsNullOrEmpty(ProfPK_ToDel))
                {
                    var _with28 = objWF.MyCommand;
                    _with28.CommandType = CommandType.Text;
                    _with28.CommandText = "DELETE FROM QUOTATION_PROFITABILITY_TBL QFT WHERE QFT.QUOTATION_FK = " + RefFK + " AND QFT.POL_FK = " + POLPK + " AND QFT.POD_FK = " + PODPK + " AND NVL(QFT.CARRIER_MST_FK,0) = " + OperatorPK + " AND QFT.REF_FLAG = " + RefFlag + " AND QFT.QUOTATION_PROFITABILITY_PK NOT IN (" + ProfPK_ToDel + ") AND QFT.OTHER_CHARGE = 0 ";
                    _with28.ExecuteNonQuery();
                    //objWF.ExecuteCommands("DELETE FROM QUOTATION_PROFITABILITY_TBL QFT WHERE QFT.QUOTATION_FK = " & RefFK & " AND QFT.POL_FK = " & POLPK & " AND QFT.POD_FK = " _
                    //                      & PODPK & " AND QFT.CARRIER_MST_FK = " & OperatorPK & " AND QFT.REF_FLAG = " & RefFlag _
                    //                      & " AND QFT.QUOTATION_PROFITABILITY_PK NOT IN (" & ProfPK_ToDel & ")")
                }

                //-----------------------------------------------------------
                for (RowCnt = 0; RowCnt <= DsProfitability.Tables[0].Rows.Count - 2; RowCnt++)
                {
                    for (ColCnt = 7; ColCnt <= DsProfitability.Tables[0].Columns.Count - 1; ColCnt++)
                    {

                        if (DsProfitability.Tables[0].Columns[ColCnt].Caption.PadLeft(2) == "PK")
                        {
                            if (string.IsNullOrEmpty(DsProfitability.Tables[0].Rows[RowCnt][ColCnt].ToString()))
                            {
                                ProfitabilityPK = 0;
                            }
                            else
                            {
                                ProfitabilityPK = Convert.ToInt32(DsProfitability.Tables[0].Rows[RowCnt][ColCnt]);
                            }

                            if (!string.IsNullOrEmpty(DsProfitability.Tables[0].Rows[RowCnt][ColCnt + 2].ToString()))
                            {
                                if (Convert.ToInt32(DsProfitability.Tables[0].Rows[RowCnt][ColCnt + 2]) > 0)
                                {
                                    var _with29 = insCommand;
                                    insCommand.Parameters.Clear();
                                    _with29.Connection = objWF.MyConnection;
                                    _with29.CommandType = CommandType.StoredProcedure;
                                    _with29.CommandText = objWF.MyUserName + ".QUOTATION_PROFITABILITY_PKG.QUOTATION_PROFITABILITYRPT_INS";

                                    var _with30 = _with29.Parameters;
                                    _with30.Add("QUOTATION_FK_IN", RefFK).Direction = ParameterDirection.Input;
                                    _with30.Add("COST_ELEMENT_MST_FK_IN", (string.IsNullOrEmpty(DsProfitability.Tables[0].Rows[RowCnt]["COST_ELEMENT_MST_PK"].ToString()) ? DBNull.Value : DsProfitability.Tables[0].Rows[RowCnt]["COST_ELEMENT_MST_PK"])).Direction = ParameterDirection.Input;
                                    _with30.Add("CURRENCY_TYPE_MST_FK_IN", (string.IsNullOrEmpty(DsProfitability.Tables[0].Rows[RowCnt]["CURRENCY_MST_PK"].ToString()) ? HttpContext.Current.Session["CURRENCY_MST_PK"] : DsProfitability.Tables[0].Rows[RowCnt]["CURRENCY_MST_PK"])).Direction = ParameterDirection.Input;
                                    if (CargoType == 1 | CargoType == -1)
                                    {
                                        _with30.Add("CONTAINER_TYPE_FK_IN", DsProfitability.Tables[0].Columns[ColCnt + 1].Caption.PadRight(Convert.ToInt32(DsProfitability.Tables[0].Columns[ColCnt + 1].Caption.Length) - 7)).Direction = ParameterDirection.Input;
                                    }
                                    else if (CargoType == 2)
                                    {
                                        _with30.Add("CONTAINER_TYPE_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                                    }
                                    _with30.Add("PROFITABILITY_RATE_IN", (string.IsNullOrEmpty(DsProfitability.Tables[0].Rows[RowCnt][ColCnt + 2].ToString()) ? DBNull.Value : DsProfitability.Tables[0].Rows[RowCnt][ColCnt + 2])).Direction = ParameterDirection.Input;
                                    _with30.Add("ROE_IN", (string.IsNullOrEmpty(DsProfitability.Tables[0].Rows[RowCnt]["ROE"].ToString()) ? DBNull.Value : DsProfitability.Tables[0].Rows[RowCnt]["ROE"])).Direction = ParameterDirection.Input;
                                    _with30.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                                    _with30.Add("CARGO_TYPE_IN", CargoType).Direction = ParameterDirection.Input;
                                    _with30.Add("REF_FLAG_IN", RefFlag).Direction = ParameterDirection.Input;
                                    _with30.Add("VOLUME_IN", (string.IsNullOrEmpty(DsProfitability.Tables[0].Rows[DsProfitability.Tables[0].Rows.Count - 1][ColCnt + 2].ToString()) ? 1 : DsProfitability.Tables[0].Rows[DsProfitability.Tables[0].Rows.Count - 1][ColCnt + 2])).Direction = ParameterDirection.Input;
                                    _with30.Add("INC_TEA_STATUS_IN", IncTeaStatus).Direction = ParameterDirection.Input;

                                    _with30.Add("POL_FK_IN", POLPK).Direction = ParameterDirection.Input;
                                    _with30.Add("POD_FK_IN", PODPK).Direction = ParameterDirection.Input;
                                    _with30.Add("CARRIER_MST_FK_IN", OperatorPK).Direction = ParameterDirection.Input;

                                    _with30.Add("CREATED_BY_FK_IN", CREATED_BY).Direction = ParameterDirection.Input;
                                    _with30.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                                    _with30.Add("OTHER_CHARGE_IN", 0).Direction = ParameterDirection.Input;
                                    _with30.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;

                                    var _with31 = updCommand;
                                    updCommand.Parameters.Clear();
                                    _with31.Connection = objWF.MyConnection;
                                    _with31.CommandType = CommandType.StoredProcedure;
                                    _with31.CommandText = objWF.MyUserName + ".QUOTATION_PROFITABILITY_PKG.QUOTATION_PROFITABILITYRPT_UPD";

                                    var _with32 = _with31.Parameters;
                                    _with32.Add("QUOTATION_PROFITABILITY_PK_IN", ProfitabilityPK).Direction = ParameterDirection.Input;
                                    _with32.Add("QUOTATION_FK_IN", RefFK).Direction = ParameterDirection.Input;
                                    _with32.Add("COST_ELEMENT_MST_FK_IN", (string.IsNullOrEmpty(DsProfitability.Tables[0].Rows[RowCnt]["COST_ELEMENT_MST_PK"].ToString()) ? DBNull.Value : DsProfitability.Tables[0].Rows[RowCnt]["COST_ELEMENT_MST_PK"])).Direction = ParameterDirection.Input;
                                    _with32.Add("CURRENCY_TYPE_MST_FK_IN", (string.IsNullOrEmpty(DsProfitability.Tables[0].Rows[RowCnt]["CURRENCY_MST_PK"].ToString()) ? HttpContext.Current.Session["CURRENCY_MST_PK"] : DsProfitability.Tables[0].Rows[RowCnt]["CURRENCY_MST_PK"])).Direction = ParameterDirection.Input;
                                    if (CargoType == 1 | CargoType == -1)
                                    {
                                        
                                        _with32.Add("CONTAINER_TYPE_FK_IN", DsProfitability.Tables[0].Columns[ColCnt + 1].Caption.PadRight(Convert.ToInt32(DsProfitability.Tables[0].Columns[ColCnt + 1].Caption.Length) - 7)).Direction = ParameterDirection.Input;
                                    }
                                    else if (CargoType == 2)
                                    {
                                        _with32.Add("CONTAINER_TYPE_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                                    }
                                    _with32.Add("PROFITABILITY_RATE_IN", (string.IsNullOrEmpty(DsProfitability.Tables[0].Rows[RowCnt][ColCnt + 2].ToString()) ? DBNull.Value : DsProfitability.Tables[0].Rows[RowCnt][ColCnt + 2])).Direction = ParameterDirection.Input;
                                    _with32.Add("ROE_IN", (string.IsNullOrEmpty(DsProfitability.Tables[0].Rows[RowCnt]["ROE"].ToString()) ? DBNull.Value : DsProfitability.Tables[0].Rows[RowCnt]["ROE"])).Direction = ParameterDirection.Input;
                                    _with32.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                                    _with32.Add("CARGO_TYPE_IN", CargoType).Direction = ParameterDirection.Input;
                                    _with32.Add("REF_FLAG_IN", RefFlag).Direction = ParameterDirection.Input;
                                    _with32.Add("VOLUME_IN", (string.IsNullOrEmpty(DsProfitability.Tables[0].Rows[DsProfitability.Tables[0].Rows.Count - 1][ColCnt + 2].ToString()) ? 1 : DsProfitability.Tables[0].Rows[DsProfitability.Tables[0].Rows.Count - 1][ColCnt + 2])).Direction = ParameterDirection.Input;
                                    _with32.Add("INC_TEA_STATUS_IN", IncTeaStatus).Direction = ParameterDirection.Input;

                                    _with32.Add("POL_FK_IN", POLPK).Direction = ParameterDirection.Input;
                                    _with32.Add("POD_FK_IN", PODPK).Direction = ParameterDirection.Input;
                                    _with32.Add("CARRIER_MST_FK_IN", OperatorPK).Direction = ParameterDirection.Input;

                                    _with32.Add("LAST_MODIFIED_BY_FK_IN", CREATED_BY).Direction = ParameterDirection.Input;
                                    _with32.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                                    _with32.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;

                                    if (ProfitabilityPK <= 0)
                                    {
                                        var _with33 = objWF.MyDataAdapter;
                                        _with33.InsertCommand = insCommand;
                                        _with33.InsertCommand.Transaction = TRAN;
                                        RecAft = RecAft + _with33.InsertCommand.ExecuteNonQuery();
                                    }
                                    else
                                    {
                                        var _with34 = objWF.MyDataAdapter;
                                        _with34.UpdateCommand = updCommand;
                                        _with34.UpdateCommand.Transaction = TRAN;
                                        RecAft = RecAft + _with34.UpdateCommand.ExecuteNonQuery();
                                    }
                                }
                            }
                        }
                    }
                }

                //--------------------insert/update local/other charges---------------------
                ProfPK_ToDel = "";
                if ((DSProfitability_Local != null))
                {
                    objWF.MyCommand = new OracleCommand();
                    foreach (DataRow _row in DSProfitability_Local.Tables[0].Rows)
                    {
                        ProfitabilityPK = 0;
                        if (!string.IsNullOrEmpty(_row["QUOTATION_PROFITABILITY_PK"].ToString()))
                        {
                            ProfitabilityPK = Convert.ToInt32(_row["QUOTATION_PROFITABILITY_PK"]);
                        }
                        if (Convert.ToInt32(_row["DELETE_FLAG"]) == 1)
                        {
                            if (string.IsNullOrEmpty(ProfPK_ToDel))
                            {
                                ProfPK_ToDel = Convert.ToString(ProfitabilityPK);
                            }
                            else
                            {
                                ProfPK_ToDel += "," + ProfitabilityPK;
                            }
                        }
                        else
                        {
                            var _with35 = objWF.MyCommand;
                            _with35.Parameters.Clear();
                            _with35.Transaction = TRAN;
                            _with35.Connection = objWF.MyConnection;
                            _with35.CommandType = CommandType.StoredProcedure;
                            if (ProfitabilityPK > 0)
                            {
                                _with35.CommandText = objWF.MyUserName + ".QUOTATION_PROFITABILITY_PKG.QUOTATION_PROFITABILITYRPT_UPD";
                                _with35.Parameters.Add("QUOTATION_PROFITABILITY_PK_IN", ProfitabilityPK).Direction = ParameterDirection.Input;
                                _with35.Parameters.Add("LAST_MODIFIED_BY_FK_IN", CREATED_BY).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with35.CommandText = objWF.MyUserName + ".QUOTATION_PROFITABILITY_PKG.QUOTATION_PROFITABILITYRPT_INS";
                                _with35.Parameters.Add("CREATED_BY_FK_IN", CREATED_BY).Direction = ParameterDirection.Input;
                                _with35.Parameters.Add("OTHER_CHARGE_IN", 1).Direction = ParameterDirection.Input;
                            }

                            var _with36 = _with35.Parameters;
                            _with36.Add("QUOTATION_FK_IN", RefFK).Direction = ParameterDirection.Input;
                            _with36.Add("COST_ELEMENT_MST_FK_IN", (string.IsNullOrEmpty(_row["COST_ELEMENT_MST_PK"].ToString()) ? DBNull.Value : _row["COST_ELEMENT_MST_PK"])).Direction = ParameterDirection.Input;
                            _with36.Add("CURRENCY_TYPE_MST_FK_IN", (string.IsNullOrEmpty(_row["CURRENCY_MST_PK"].ToString()) ? HttpContext.Current.Session["CURRENCY_MST_PK"] : _row["CURRENCY_MST_PK"])).Direction = ParameterDirection.Input;
                            _with36.Add("CONTAINER_TYPE_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            _with36.Add("PROFITABILITY_RATE_IN", (string.IsNullOrEmpty(_row["AMOUNT"].ToString()) ? DBNull.Value : _row["AMOUNT"])).Direction = ParameterDirection.Input;
                            _with36.Add("ROE_IN", (string.IsNullOrEmpty(_row["ROE"].ToString()) ? DBNull.Value : _row["ROE"])).Direction = ParameterDirection.Input;
                            _with36.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                            _with36.Add("CARGO_TYPE_IN", CargoType).Direction = ParameterDirection.Input;
                            _with36.Add("REF_FLAG_IN", RefFlag).Direction = ParameterDirection.Input;
                            _with36.Add("VOLUME_IN", 1).Direction = ParameterDirection.Input;
                            _with36.Add("INC_TEA_STATUS_IN", IncTeaStatus).Direction = ParameterDirection.Input;

                            _with36.Add("POL_FK_IN", POLPK).Direction = ParameterDirection.Input;
                            _with36.Add("POD_FK_IN", PODPK).Direction = ParameterDirection.Input;
                            _with36.Add("CARRIER_MST_FK_IN", OperatorPK).Direction = ParameterDirection.Input;

                            _with36.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                            _with36.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;

                            RecAft = RecAft + _with35.ExecuteNonQuery();
                        }
                    }
                    //--------------------delete local/other charges---------------------
                    if (!string.IsNullOrEmpty(ProfPK_ToDel))
                    {
                        var _with37 = objWF.MyCommand;
                        _with37.Parameters.Clear();
                        _with37.Transaction = TRAN;
                        _with37.Connection = objWF.MyConnection;
                        _with37.CommandType = CommandType.Text;
                        _with37.CommandText = "DELETE FROM QUOTATION_PROFITABILITY_TBL QFT WHERE QFT.QUOTATION_PROFITABILITY_PK IN (" + ProfPK_ToDel + ") ";
                        _with37.ExecuteNonQuery();
                    }
                }
                //----------------------------------------------------------------------
                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    objWF.CloseConnection();
                }
                else
                {

                    string cmdText = "";
                    if (RefFlag == 1)
                    {
                        cmdText = " UPDATE BOOKING_MST_TBL BST SET BST.PROFITABILITY_FLAG = 1 " + " WHERE BST.BOOKING_MST_PK = " + RefFK;
                    }
                    else if (RefFlag == 0)
                    {
                        cmdText = " UPDATE QUOTATION_MST_TBL QST SET QST.PROFITABILITY_FLAG = 1 " + " WHERE QST.QUOTATION_MST_PK = " + RefFK;
                    }
                    else if (RefFlag == 2)
                    {
                        cmdText = " UPDATE SRR_SEA_TBL SST SET SST.PROFITABILITY_FLAG = 1 " + " WHERE SST.SRR_SEA_PK = " + RefFK;
                        /// 3 - Quot Air
                    }
                    else if (RefFlag == 3)
                    {
                        cmdText = " UPDATE QUOTATION_MST_TBL SST SET SST.PROFITABILITY_FLAG = 1 " + " WHERE SST.QUOTATION_MST_PK = " + RefFK;
                        //' 4 - SRR Air
                    }
                    else if (RefFlag == 4)
                    {
                        cmdText = " UPDATE SRR_AIR_TBL SST SET SST.PROFITABILITY_FLAG = 1 " + " WHERE SST.SRR_AIR_PK = " + RefFK;
                        //' 5 - Booking Air
                    }
                    else if (RefFlag == 5)
                    {
                        cmdText = " UPDATE BOOKING_MST_TBL SST SET SST.PROFITABILITY_FLAG = 1 " + " WHERE SST.BOOKING_MST_PK = " + RefFK;
                    }

                    if (!string.IsNullOrEmpty(cmdText))
                    {
                        objWF.MyCommand.Parameters.Clear();
                        objWF.MyCommand.Transaction = TRAN;
                        objWF.MyCommand.Connection = objWF.MyConnection;
                        objWF.MyCommand.CommandText = cmdText;
                        objWF.MyCommand.ExecuteNonQuery();
                    }
                    TRAN.Commit();
                    arrMessage.Add("saved");
                }

                return arrMessage;
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWF.CloseConnection();
            }
        }
        public void SaveProfitDetails(short RefFlag, int RefPk)
        {
            WorkFlow objWF = new WorkFlow();
            OracleTransaction TRAN = default(OracleTransaction);

            objWF.OpenConnection();
            TRAN = objWF.MyConnection.BeginTransaction();
            objWF.MyCommand = new OracleCommand();

            try
            {
                var _with38 = objWF.MyCommand;
                _with38.Transaction = TRAN;
                _with38.Connection = objWF.MyConnection;
                _with38.CommandType = CommandType.StoredProcedure;
                _with38.CommandText = objWF.MyUserName + ".QUOTATION_PROFITABILITY_PKG.UPDATE_PROFIT_DETAILS";
                _with38.Parameters.Clear();

                _with38.Parameters.Add("REF_FLAG_IN", RefFlag).Direction = ParameterDirection.Input;
                _with38.Parameters.Add("REF_PK_IN", RefPk).Direction = ParameterDirection.Input;
                _with38.Parameters.Add("PROFIT_AMOUNT_IN", getDefault(HttpContext.Current.Session["PROFIT_AMOUNT"], 0)).Direction = ParameterDirection.Input;
                _with38.Parameters.Add("PROFIT_PERCENTAGE_IN", getDefault(HttpContext.Current.Session["PROFIT_PERCENTAGE"], 0)).Direction = ParameterDirection.Input;
                _with38.ExecuteNonQuery();
                TRAN.Commit();
            }
            catch (Exception ex)
            {
                TRAN.Rollback();
            }
            finally
            {
                objWF.CloseConnection();
            }
        }

        public DataSet Fetch_Revenue(int Quotationpk = 0)
        {
            //'fetching the estimated cost and revenue cost
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                sb.Append("   SELECT");
                sb.Append("");
                sb.Append("   (SELECT ROUND(SUM(NVL(FD.QUOTED_RATE*get_ex_rate(fd.currency_mst_fk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",main.quotation_date), 0)), 2)");
                sb.Append("    FROM QUOTATION_MST_TBL          MAIN,");
                sb.Append("         QUOTATION_DTL_TBL  TRN,");
                sb.Append("         QUOTATION_TRN_SEA_FRT_DTLS FD");
                sb.Append("  ");
                sb.Append("      WHERE MAIN.QUOTATION_MST_PK = TRN.QUOTATION_MST_FK");
                sb.Append("      AND TRN.QUOTE_TRN_SEA_PK = FD.QUOTE_TRN_SEA_FK");
                sb.Append("      AND MAIN.QUOTATION_MST_PK =" + Quotationpk);
                sb.Append(" ");
                sb.Append("        ) ESTREVENUE ,ROUND((SELECT SUM(NVL(QPT.ROE, 0) * NVL(QPT.PROFITABILITY_RATE, 0))");
                sb.Append("         FROM QUOTATION_PROFITABILITY_TBL QPT");
                sb.Append("        WHERE QPT.QUOTATION_MST_FK=" + Quotationpk);
                sb.Append("        ),2) ESTCOST");
                sb.Append("");
                sb.Append("    FROM DUAL");

                return (objWF.GetDataSet(sb.ToString()));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public DataSet Fetch_Record(int Quotationpk = 0, int BizType = 0)
        {
            DataSet M_GridDs = null;
            DataSet GrdDs = null;
            DataSet M_Datset = null;
            DataSet M_GridDatset = null;
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);


            try
            {
                //'check if profitability already saved in QUOTATION_PROFITABILITY_TBL 
                sb.Append("  SELECT ROWNUM");
                sb.Append("  FROM QUOTATION_PROFITABILITY_TBL QP");
                sb.Append("  WHERE 1=1");
                if (Quotationpk > 0)
                {
                    sb.Append("  and  QP.QUOTATION_MST_FK= " + Quotationpk);
                }
                else
                {
                    sb.Append("  and  QP.QUOTATION_MST_FK =-1");
                }
                GrdDs = objWF.GetDataSet(sb.ToString());

                //'check if record exhist in QUOTATION_PROFITABILITY_TBL 
                if (GrdDs.Tables[0].Rows.Count > 0)
                {
                    //'fetch the data from QUOTATION_PROFITABILITY_TBL  and return
                    sb.Remove(0, sb.Length);
                    sb.Append("       SELECT ROWNUM SLNR,QP.QUOTATION_PROFITABILITY_PK PROFITABILITYPK,");
                    sb.Append("       QP.QUOTATION_MST_FK,");
                    sb.Append("       QP.COST_ELEMENT_MST_FK COSTELEMENTPK,");
                    sb.Append("       CMT.COST_ELEMENT_ID COSTELEMENTID,");
                    sb.Append("       CMT.COST_ELEMENT_NAME COSTELEMENTDESC,");
                    sb.Append("       QP.CURRENCY_TYPE_MST_FK CURRENCYFK,");
                    sb.Append("       CTMT.CURRENCY_ID CURRENCYID,");
                    sb.Append("       ROUND(NVL(QP.PROFITABILITY_RATE, 0), 2) RATE,");
                    sb.Append("       ROUND(NVL(QP.ROE, 0), 6) ROE,");
                    sb.Append("       ROUND(NVL(QP.PROFITABILITY_RATE, 0) * NVL(QP.ROE, 0), 2) AMOUNT,QP.BIZ_TYPE BIZTYPE");
                    sb.Append("");
                    sb.Append("       FROM QUOTATION_PROFITABILITY_TBL QP,");
                    sb.Append("       COST_ELEMENT_MST_TBL        CMT,");
                    sb.Append("       CURRENCY_TYPE_MST_TBL       CTMT");
                    sb.Append("");
                    sb.Append("   WHERE 1 = 1");
                    sb.Append("   AND QP.COST_ELEMENT_MST_FK = CMT.COST_ELEMENT_MST_PK");
                    sb.Append("   AND QP.CURRENCY_TYPE_MST_FK = CTMT.CURRENCY_MST_PK");
                    sb.Append("   AND QP.QUOTATION_MST_FK= " + Quotationpk);

                    M_GridDatset = objWF.GetDataSet(sb.ToString());
                    return M_GridDatset;
                    //' fetch the deafult operator cost from the operator master
                }
                else
                {
                    //'for the sea
                    if (BizType == 2)
                    {
                        //'find the tariff and spot  reference number from quotation
                        sb.Remove(0, sb.Length);
                        sb.Append("   SELECT QT.TRANS_REFERED_FROM, QT.TRANS_REF_NO,QST.CARGO_TYPE");
                        sb.Append("   FROM QUOTATION_DTL_TBL QT, QUOTATION_MST_TBL QST");
                        sb.Append("   WHERE QST.QUOTATION_MST_PK = QT.QUOTATION_MST_FK");
                        if (Quotationpk > 0)
                        {
                            sb.Append("   AND QT.QUOTATION_MST_FK= " + Quotationpk);
                        }
                        else
                        {
                            sb.Append("   AND 1=2");
                        }
                        M_GridDs = objWF.GetDataSet(sb.ToString());
                        if (M_GridDs.Tables[0].Rows.Count > 0)
                        {
                            M_Datset = GetOperatorCost(Convert.ToInt32(M_GridDs.Tables[0].Rows[0][0]), Convert.ToString(M_GridDs.Tables[0].Rows[0][1]), Convert.ToInt32(M_GridDs.Tables[0].Rows[0][2]), Quotationpk, BizType);
                        }
                        else
                        {
                            M_Datset = GetOperatorCost(0, "", 0, Quotationpk);
                        }
                        //'for the air cargo 
                    }
                    else
                    {
                        sb.Remove(0, sb.Length);
                        sb.Append("     SELECT TRN.TRANS_REFERED_FROM, TRN.TRANS_REF_NO");
                        sb.Append("     FROM QUOTATION_MST_TBL MAIN, QUOTATION_TRN_AIR TRN");
                        sb.Append("     WHERE MAIN.QUOTATION_MST_PK = TRN.QUOTATION_MST_FK");
                        if (Quotationpk > 0)
                        {
                            sb.Append("   AND MAIN.QUOTATION_MST_PK= " + Quotationpk);
                        }
                        else
                        {
                            sb.Append("   AND 1=2");
                        }
                        M_GridDs = objWF.GetDataSet(sb.ToString());

                        if (M_GridDs.Tables[0].Rows.Count > 0)
                        {
                            M_Datset = GetOperatorCost(Convert.ToInt32(M_GridDs.Tables[0].Rows[0][0]), Convert.ToString(M_GridDs.Tables[0].Rows[0][1]), 0, Quotationpk);
                        }
                        else
                        {
                            M_Datset = GetOperatorCost(0, "", 0, Quotationpk);
                        }

                    }

                    return M_Datset;
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

        private DataSet GetOperatorCost(int TRANS_REFERED_FROM = 0, string TRANS_REF_NO = "", int CARGO_TYPE = 0, int Quotationpk = 0, int BizType = 0)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                WorkFlow objWF = new WorkFlow();
                DataSet DsOperatorCost = null;

                //'for the air cargo
                if (BizType == 1)
                {
                    //'spot rate based
                    if (TRANS_REFERED_FROM == 1)
                    {
                        sb.Append("       SELECT ROWNUM SLNR,");
                        sb.Append("       '' PROFITABILITYPK,");
                        sb.Append("       '" + Quotationpk + "' QUOTATION_MST_FK,");
                        sb.Append("       CMT.COST_ELEMENT_MST_PK COSTELEMENTPK,");
                        sb.Append("       CMT.COST_ELEMENT_ID COSTELEMENTID,");
                        sb.Append("       CMT.COST_ELEMENT_NAME COSTELEMENTDESC,");
                        sb.Append("       '" + HttpContext.Current.Session["CURRENCY_MST_PK"] + "' CURRENCYFK,");
                        sb.Append("       '" + HttpContext.Current.Session["CURRENCY_ID"] + "' CURRENCYID,");
                        sb.Append("       (SELECT SUM(NVL(BPNT.APPROVED_RATE, 0))");
                        sb.Append("          FROM RFQ_SPOT_RATE_AIR_TBL        MAIN,");
                        sb.Append("               RFQ_SPOT_AIR_TRN_FREIGHT_TBL TRAN,");
                        sb.Append("               RFQ_SPOT_AIR_BREAKPOINTS     BPNT");
                        sb.Append("        ");
                        sb.Append("         WHERE BPNT.RFQ_SPOT_AIR_FRT_FK = TRAN.RFQ_SPOT_TRN_FREIGHT_PK");
                        sb.Append("           AND MAIN.RFQ_SPOT_AIR_PK = TRAN.RFQ_SPOT_TRN_AIR_FK");
                        sb.Append("           AND MAIN.RFQ_REF_NO = '" + TRANS_REF_NO + "') RATE,");
                        sb.Append("       '1.000000' ROE,");
                        sb.Append("       ");
                        sb.Append("       (SELECT SUM(NVL(GET_EX_RATE(TRAN.CURRENCY_MST_FK, '" + HttpContext.Current.Session["CURRENCY_MST_PK"] + "', SYSDATE), 0) *");
                        sb.Append("                   NVL(BPNT.APPROVED_RATE, 0))");
                        sb.Append("          FROM RFQ_SPOT_RATE_AIR_TBL        MAIN,");
                        sb.Append("               RFQ_SPOT_AIR_TRN_FREIGHT_TBL TRAN,");
                        sb.Append("               RFQ_SPOT_AIR_BREAKPOINTS     BPNT");
                        sb.Append("        ");
                        sb.Append("         WHERE BPNT.RFQ_SPOT_AIR_FRT_FK = TRAN.RFQ_SPOT_TRN_FREIGHT_PK");
                        sb.Append("           AND MAIN.RFQ_SPOT_AIR_PK = TRAN.RFQ_SPOT_TRN_AIR_FK");
                        sb.Append("           AND MAIN.RFQ_REF_NO = '" + TRANS_REF_NO + "') AMOUNT,'1' BIZTYPE ");
                        sb.Append("    FROM COST_ELEMENT_MST_TBL CMT");
                        sb.Append("    WHERE CMT.COST_ELEMENT_ID = 'OPC'");
                        //'tarif based
                    }
                    else if (TRANS_REFERED_FROM == 5)
                    {
                        sb.Append("       SELECT ROWNUM SLNR,");
                        sb.Append("       '' PROFITABILITYPK,");
                        sb.Append("       '" + Quotationpk + "' QUOTATION_MST_FK,");
                        sb.Append("       CMT.COST_ELEMENT_MST_PK COSTELEMENTPK,");
                        sb.Append("       CMT.COST_ELEMENT_ID COSTELEMENTID,");
                        sb.Append("       CMT.COST_ELEMENT_NAME COSTELEMENTDESC,");
                        sb.Append("       '" + HttpContext.Current.Session["CURRENCY_MST_PK"] + "' CURRENCYFK,");
                        sb.Append("       '" + HttpContext.Current.Session["CURRENCY_ID"] + "' CURRENCYID,");
                        sb.Append("       (SELECT SUM(NVL(SURCH.TARIFF_RATE, 0))");
                        sb.Append("          FROM TARIFF_MAIN_AIR_TBL      MAIN,");
                        sb.Append("               TARIFF_TRN_AIR_TBL       TRAN,");
                        sb.Append("               TARIFF_TRN_AIR_SURCHARGE SURCH");
                        sb.Append("        ");
                        sb.Append("         WHERE MAIN.TARIFF_MAIN_AIR_PK = TRAN.TARIFF_MAIN_AIR_FK");
                        sb.Append("           AND TRAN.TARIFF_TRN_AIR_PK = SURCH.TARIFF_TRN_AIR_FK");
                        sb.Append("           AND MAIN.TARIFF_REF_NO = '" + TRANS_REF_NO + "') RATE,");
                        sb.Append("       '1.000000' ROE,");
                        sb.Append("       ");
                        sb.Append("       (SELECT SUM(NVL(GET_EX_RATE(SURCH.CURRENCY_MST_FK, '" + HttpContext.Current.Session["CURRENCY_MST_PK"] + "', SYSDATE), 0) *");
                        sb.Append("                   NVL(SURCH.TARIFF_RATE, 0))");
                        sb.Append("          FROM TARIFF_MAIN_AIR_TBL      MAIN,");
                        sb.Append("               TARIFF_TRN_AIR_TBL       TRAN,");
                        sb.Append("               TARIFF_TRN_AIR_SURCHARGE SURCH");
                        sb.Append("        ");
                        sb.Append("         WHERE MAIN.TARIFF_MAIN_AIR_PK = TRAN.TARIFF_MAIN_AIR_FK");
                        sb.Append("           AND TRAN.TARIFF_TRN_AIR_PK = SURCH.TARIFF_TRN_AIR_FK");
                        sb.Append("           AND MAIN.TARIFF_REF_NO = '" + TRANS_REF_NO + "') AMOUNT,");
                        sb.Append("       '1' BIZTYPE");
                        sb.Append("");
                        sb.Append("  FROM COST_ELEMENT_MST_TBL CMT");
                        sb.Append(" WHERE CMT.COST_ELEMENT_ID = 'OPC'");
                        sb.Append("");
                        sb.Append("");


                    }
                    else
                    {
                        sb.Append("       SELECT '' SLNR,");
                        sb.Append("       '' PROFITABILITYPK,");
                        sb.Append("       '' QUOTATION_MST_FK,");
                        sb.Append("       '' COSTELEMENTPK,");
                        sb.Append("       '' COSTELEMENTID,");
                        sb.Append("       '' COSTELEMENTDESC,");
                        sb.Append("       '' CURRENCYFK,");
                        sb.Append("       '' CURRENCYID,");
                        sb.Append("       '' RATE,");
                        sb.Append("       '' ROE,");
                        sb.Append("       '' AMOUNT,''BIZTYPE");
                        sb.Append("  FROM DUAL");
                        sb.Append("  WHERE 1 = 2");
                    }
                }
                else if (BizType == 2)
                {
                    //'for the sea cargo 
                    //'spot rate based
                    if (TRANS_REFERED_FROM == 1)
                    {
                        //'for fcl cargo
                        if (CARGO_TYPE == 1)
                        {
                            sb.Append("       SELECT ROWNUM SLNR,");
                            sb.Append("       '' PROFITABILITYPK,");
                            sb.Append("       '" + Quotationpk + "' QUOTATION_MST_FK,");
                            sb.Append("       CMT.COST_ELEMENT_MST_PK COSTELEMENTPK,");
                            sb.Append("       CMT.COST_ELEMENT_ID COSTELEMENTID,");
                            sb.Append("       CMT.COST_ELEMENT_NAME COSTELEMENTDESC,");
                            sb.Append("       '" + HttpContext.Current.Session["CURRENCY_MST_PK"] + "' CURRENCYFK,");
                            sb.Append("       '" + HttpContext.Current.Session["CURRENCY_ID"] + "' CURRENCYID,");
                            sb.Append("       (SELECT SUM(NVL(CONT1.FCL_APP_RATE, 0)) QUOTERATE");
                            sb.Append("        ");
                            sb.Append("          FROM RFQ_SPOT_RATE_SEA_TBL     MAIN1,");
                            sb.Append("               RFQ_SPOT_TRN_SEA_FCL_LCL  TRAN1,");
                            sb.Append("               RFQ_SPOT_TRN_SEA_CONT_DET CONT1");
                            sb.Append("         WHERE MAIN1.RFQ_SPOT_SEA_PK = TRAN1.RFQ_SPOT_SEA_FK");
                            sb.Append("           AND CONT1.RFQ_SPOT_SEA_TRN_FK = TRAN1.RFQ_SPOT_SEA_TRN_PK");
                            sb.Append("           AND MAIN1.RFQ_REF_NO = '" + TRANS_REF_NO + "') RATE,");
                            sb.Append("       '1.00000' ROE,");
                            sb.Append("       (SELECT SUM(NVL(CONT1.FCL_APP_RATE, 0)*NVL(GET_EX_RATE(TRAN1.CURRENCY_MST_FK, '" + HttpContext.Current.Session["CURRENCY_MST_PK"] + "', SYSDATE), 0)) QUOTERATE");
                            sb.Append("        ");
                            sb.Append("          FROM RFQ_SPOT_RATE_SEA_TBL     MAIN1,");
                            sb.Append("               RFQ_SPOT_TRN_SEA_FCL_LCL  TRAN1,");
                            sb.Append("               RFQ_SPOT_TRN_SEA_CONT_DET CONT1");
                            sb.Append("         WHERE MAIN1.RFQ_SPOT_SEA_PK = TRAN1.RFQ_SPOT_SEA_FK");
                            sb.Append("           AND CONT1.RFQ_SPOT_SEA_TRN_FK = TRAN1.RFQ_SPOT_SEA_TRN_PK");
                            sb.Append("           AND MAIN1.RFQ_REF_NO = '" + TRANS_REF_NO + "') AMOUNT,");
                            sb.Append("         '2' BIZTYPE");
                            sb.Append("");
                            sb.Append("       FROM COST_ELEMENT_MST_TBL CMT");
                            sb.Append("       WHERE CMT.COST_ELEMENT_ID = 'OPC'");
                            //'for lcl cargo
                        }
                        else
                        {
                            sb.Append("       SELECT ROWNUM SLNR,");
                            sb.Append("       '' PROFITABILITYPK,");
                            sb.Append("       '" + Quotationpk + "' QUOTATION_MST_FK,");
                            sb.Append("       CMT.COST_ELEMENT_MST_PK COSTELEMENTPK,");
                            sb.Append("       CMT.COST_ELEMENT_ID COSTELEMENTID,");
                            sb.Append("       CMT.COST_ELEMENT_NAME COSTELEMENTDESC,");
                            sb.Append("       '" + HttpContext.Current.Session["CURRENCY_MST_PK"] + "' CURRENCYFK,");
                            sb.Append("       '" + HttpContext.Current.Session["CURRENCY_ID"] + "' CURRENCYID,");
                            sb.Append("       (SELECT SUM(CASE");
                            sb.Append("                     WHEN (NVL(TRAN1.LCL_APPROVED_MIN_RATE, 0) =");
                            sb.Append("                          NVL(TRAN1.LCL_APPROVED_RATE, 0)) THEN");
                            sb.Append("                      NVL(TRAN1.LCL_APPROVED_MIN_RATE, 0)");
                            sb.Append("                   ");
                            sb.Append("                     ELSE");
                            sb.Append("                      NVL(TRAN1.LCL_APPROVED_RATE, 0)");
                            sb.Append("                   END)");
                            sb.Append("        ");
                            sb.Append("          FROM RFQ_SPOT_RATE_SEA_TBL MAIN1, RFQ_SPOT_TRN_SEA_FCL_LCL TRAN1");
                            sb.Append("         WHERE MAIN1.RFQ_SPOT_SEA_PK = TRAN1.RFQ_SPOT_SEA_FK");
                            sb.Append("           AND MAIN1.RFQ_REF_NO ='" + TRANS_REF_NO + "') RATE,");
                            sb.Append("       '1.00000' ROE,");
                            sb.Append("       (SELECT SUM(NVL(GET_EX_RATE(TRAN1.CURRENCY_MST_FK, '" + HttpContext.Current.Session["CURRENCY_MST_PK"] + "', SYSDATE), 0) *");
                            sb.Append("                   (CASE");
                            sb.Append("                      WHEN (NVL(TRAN1.LCL_APPROVED_MIN_RATE, 0) =");
                            sb.Append("                           NVL(TRAN1.LCL_APPROVED_RATE, 0)) THEN");
                            sb.Append("                       NVL(TRAN1.LCL_APPROVED_MIN_RATE, 0)");
                            sb.Append("                    ");
                            sb.Append("                      ELSE");
                            sb.Append("                       NVL(TRAN1.LCL_APPROVED_RATE, 0)");
                            sb.Append("                    END))");
                            sb.Append("        ");
                            sb.Append("          FROM RFQ_SPOT_RATE_SEA_TBL MAIN1, RFQ_SPOT_TRN_SEA_FCL_LCL TRAN1");
                            sb.Append("         WHERE MAIN1.RFQ_SPOT_SEA_PK = TRAN1.RFQ_SPOT_SEA_FK");
                            sb.Append("           AND MAIN1.RFQ_REF_NO = '" + TRANS_REF_NO + "') AMOUNT,");
                            sb.Append("       ");
                            sb.Append("       '2' BIZTYPE");
                            sb.Append("");
                            sb.Append("  FROM COST_ELEMENT_MST_TBL CMT");
                            sb.Append(" WHERE CMT.COST_ELEMENT_ID = 'OPC'");
                            sb.Append("");
                            sb.Append("");
                        }
                        //'tariff base
                    }
                    else if (TRANS_REFERED_FROM == 5)
                    {
                        //'for lcl cargo
                        if (CARGO_TYPE == 2)
                        {
                            sb.Append("       SELECT  ROWNUM SLNR,'' PROFITABILITYPK,  '" + Quotationpk + "'QUOTATION_MST_FK,");
                            sb.Append("       CMT.COST_ELEMENT_MST_PK COSTELEMENTPK,");
                            sb.Append("       CMT.COST_ELEMENT_ID COSTELEMENTID,");
                            sb.Append("       CMT.COST_ELEMENT_NAME COSTELEMENTDESC,");
                            sb.Append("       '" + HttpContext.Current.Session["CURRENCY_MST_PK"] + "' CURRENCYFK,");
                            sb.Append("        '" + HttpContext.Current.Session["CURRENCY_ID"] + "'CURRENCYID,");
                            //'&HttpContext.Current.Session["CURRENCY_ID"]&'
                            sb.Append("       ROUND(NVL((SELECT SUM(CASE");
                            sb.Append("                              WHEN NVL(TT.LCL_TARIFF_RATE, 0) = 0 THEN");
                            sb.Append("                               CASE");
                            sb.Append("                                 WHEN NVL(TT.LCL_TARIFF_MIN_RATE, 0) = 0 THEN");
                            sb.Append("                                  CASE");
                            sb.Append("                                    WHEN NVL(TT.LCL_CURRENT_RATE, 0) = 0 THEN");
                            sb.Append("                                     ROUND(NVL(TT.LCL_CURRENT_MIN_RATE, 0), 2)");
                            sb.Append("                                    ELSE");
                            sb.Append("                                     ROUND(NVL(TT.LCL_CURRENT_RATE, 0), 2)");
                            sb.Append("                                  END");
                            sb.Append("                                 ELSE");
                            sb.Append("                                  ROUND(NVL(TT.LCL_TARIFF_MIN_RATE, 0), 2)");
                            sb.Append("                               END");
                            sb.Append("                              ELSE");
                            sb.Append("                               ROUND(NVL(TT.LCL_TARIFF_RATE, 0), 2)");
                            sb.Append("                            END)");
                            sb.Append("                 ");
                            sb.Append("                   FROM TARIFF_MAIN_SEA_TBL TM, TARIFF_TRN_SEA_FCL_LCL TT");
                            sb.Append("                  WHERE TM.TARIFF_MAIN_SEA_PK = TT.TARIFF_MAIN_SEA_FK");
                            sb.Append("                    AND TM.TARIFF_REF_NO ='" + TRANS_REF_NO + "'),");
                            sb.Append("                 0),");
                            sb.Append("             2) RATE,");
                            sb.Append("       '1.000000' ROE,");
                            sb.Append("       ROUND(NVL((SELECT SUM(CASE");
                            sb.Append("                              WHEN NVL(TT.LCL_TARIFF_RATE, 0) = 0 THEN");
                            sb.Append("                               CASE");
                            sb.Append("                                 WHEN NVL(TT.LCL_TARIFF_MIN_RATE, 0) = 0 THEN");
                            sb.Append("                                  CASE");
                            sb.Append("                                    WHEN NVL(TT.LCL_CURRENT_RATE, 0) = 0 THEN");
                            sb.Append("                                    ");
                            sb.Append("                                     ROUND(NVL(GET_EX_RATE(TT.CURRENCY_MST_FK,");
                            sb.Append("                                                           '" + HttpContext.Current.Session["CURRENCY_MST_PK"] + "',");
                            sb.Append("                                                           SYSDATE),");
                            sb.Append("                                               0) * NVL(TT.LCL_CURRENT_MIN_RATE, 0),");
                            sb.Append("                                           2)");
                            sb.Append("                                    ELSE");
                            sb.Append("                                    ");
                            sb.Append("                                     ROUND(NVL(GET_EX_RATE(TT.CURRENCY_MST_FK,");
                            sb.Append("                                                           '" + HttpContext.Current.Session["CURRENCY_MST_PK"] + "',");
                            sb.Append("                                                           SYSDATE),");
                            sb.Append("                                               0) * NVL(TT.LCL_CURRENT_RATE, 0),");
                            sb.Append("                                           2)");
                            sb.Append("                                  END");
                            sb.Append("                                 ELSE");
                            sb.Append("                                 ");
                            sb.Append("                                  ROUND(NVL(GET_EX_RATE(TT.CURRENCY_MST_FK,");
                            sb.Append("                                                        '" + HttpContext.Current.Session["CURRENCY_MST_PK"] + "',");
                            sb.Append("                                                        SYSDATE),");
                            sb.Append("                                            0) * NVL(TT.LCL_TARIFF_MIN_RATE, 0),");
                            sb.Append("                                        2)");
                            sb.Append("                               END");
                            sb.Append("                              ELSE");
                            sb.Append("                               ROUND(NVL(GET_EX_RATE(TT.CURRENCY_MST_FK,");
                            sb.Append("                                                     '" + HttpContext.Current.Session["CURRENCY_MST_PK"] + "',");
                            sb.Append("                                                     SYSDATE),");
                            sb.Append("                                         0) * NVL(TT.LCL_TARIFF_RATE, 0),");
                            sb.Append("                                     2)");
                            sb.Append("                            END)");
                            sb.Append("                 ");
                            sb.Append("                   FROM TARIFF_MAIN_SEA_TBL TM, TARIFF_TRN_SEA_FCL_LCL TT");
                            sb.Append("                  WHERE TM.TARIFF_MAIN_SEA_PK = TT.TARIFF_MAIN_SEA_FK");
                            sb.Append("                  AND TM.TARIFF_REF_NO ='" + TRANS_REF_NO + "'),");
                            sb.Append("                 0),");
                            sb.Append("             2) AMOUNT,'2' BIZTYPE");
                            sb.Append("");
                            sb.Append("   FROM COST_ELEMENT_MST_TBL CMT");
                            sb.Append("   WHERE CMT.COST_ELEMENT_ID = 'OPC'");
                            //'for fcl type cargo
                        }
                        else
                        {

                            sb.Append("       SELECT ROWNUM SLNR,");
                            sb.Append("       '' PROFITABILITYPK,");
                            sb.Append("       '" + Quotationpk + "' QUOTATION_MST_FK,");
                            sb.Append("       CMT.COST_ELEMENT_MST_PK COSTELEMENTPK,");
                            sb.Append("       CMT.COST_ELEMENT_ID COSTELEMENTID,");
                            sb.Append("       CMT.COST_ELEMENT_NAME COSTELEMENTDESC,");
                            sb.Append("       '" + HttpContext.Current.Session["CURRENCY_MST_PK"] + "' CURRENCYFK,");
                            sb.Append("       '" + HttpContext.Current.Session["CURRENCY_ID"] + "' CURRENCYID,");
                            sb.Append("       ROUND((SELECT SUM(NVL(CONT.FCL_REQ_RATE, 0)) RATE");
                            sb.Append("               FROM TARIFF_TRN_SEA_CONT_DTL CONT,");
                            sb.Append("                    TARIFF_TRN_SEA_FCL_LCL  TRN,");
                            sb.Append("                    TARIFF_MAIN_SEA_TBL     MAIN");
                            sb.Append("              WHERE CONT.TARIFF_TRN_SEA_FK = TRN.TARIFF_TRN_SEA_PK");
                            sb.Append("                AND MAIN.TARIFF_MAIN_SEA_PK = TRN.TARIFF_MAIN_SEA_FK");
                            sb.Append("                AND MAIN.TARIFF_REF_NO = '" + TRANS_REF_NO + "'),");
                            sb.Append("             2) RATE,");
                            sb.Append("       '1.00000' ROE,");
                            sb.Append("       ");
                            sb.Append("       ROUND((SELECT SUM(NVL(GET_EX_RATE(TRN.CURRENCY_MST_FK, 173, SYSDATE),");
                            sb.Append("                            0) * NVL(CONT.FCL_REQ_RATE, 0)) AMOUNT");
                            sb.Append("               FROM TARIFF_TRN_SEA_CONT_DTL CONT,");
                            sb.Append("                    TARIFF_TRN_SEA_FCL_LCL  TRN,");
                            sb.Append("                    TARIFF_MAIN_SEA_TBL     MAIN");
                            sb.Append("              WHERE CONT.TARIFF_TRN_SEA_FK = TRN.TARIFF_TRN_SEA_PK");
                            sb.Append("                AND MAIN.TARIFF_MAIN_SEA_PK = TRN.TARIFF_MAIN_SEA_FK");
                            sb.Append("                AND MAIN.TARIFF_REF_NO = '" + TRANS_REF_NO + "'),");
                            sb.Append("             2) AMOUNT,");
                            sb.Append("       '2' BIZTYPE");
                            sb.Append("");
                            sb.Append("  FROM COST_ELEMENT_MST_TBL CMT");
                            sb.Append(" WHERE CMT.COST_ELEMENT_ID = 'OPC'");
                        }
                        //'if not from sl contract and spot rate
                    }
                    else
                    {
                        sb.Append("       SELECT '' SLNR,");
                        sb.Append("       '' PROFITABILITYPK,");
                        sb.Append("       '' QUOTATION_MST_FK,");
                        sb.Append("       '' COSTELEMENTPK,");
                        sb.Append("       '' COSTELEMENTID,");
                        sb.Append("       '' COSTELEMENTDESC,");
                        sb.Append("       '' CURRENCYFK,");
                        sb.Append("       '' CURRENCYID,");
                        sb.Append("       '' RATE,");
                        sb.Append("       '' ROE,");
                        sb.Append("       '' AMOUNT,''BIZTYPE");
                        sb.Append("  FROM DUAL");
                        sb.Append("  WHERE 1 = 2");
                    }

                    //'if bizness type=0
                }
                else
                {
                    sb.Append("       SELECT '' SLNR,");
                    sb.Append("       '' PROFITABILITYPK,");
                    sb.Append("       '' QUOTATION_MST_FK,");
                    sb.Append("       '' COSTELEMENTPK,");
                    sb.Append("       '' COSTELEMENTID,");
                    sb.Append("       '' COSTELEMENTDESC,");
                    sb.Append("       '' CURRENCYFK,");
                    sb.Append("       '' CURRENCYID,");
                    sb.Append("       '' RATE,");
                    sb.Append("       '' ROE,");
                    sb.Append("       '' AMOUNT,''BIZTYPE");
                    sb.Append("  FROM DUAL");
                    sb.Append("  WHERE 1 = 2");
                }

                DsOperatorCost = objWF.GetDataSet(sb.ToString());
                return DsOperatorCost;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #region "PortGroup"
        public DataSet FetchFromPortGroup(int QuotPK = 0, int PortGrpPK = 0, string POLPK = "0")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (QuotPK != 0)
                {
                    sb.Append(" SELECT P.PORT_MST_PK, P.PORT_ID, T.POL_GRP_FK FROM QUOTATION_DTL_TBL T,PORT_MST_TBL P WHERE T.PORT_MST_POL_FK = P.PORT_MST_PK AND T.POL_GRP_FK = " + PortGrpPK);
                    sb.Append(" AND T.QUOTATION_MST_FK =" + QuotPK);
                }
                else
                {
                    sb.Append(" SELECT P.PORT_MST_PK, P.PORT_ID, PGT.PORT_GRP_MST_FK POL_GRP_FK FROM PORT_MST_TBL P, PORT_GRP_TRN_TBL PGT WHERE P.PORT_MST_PK = PGT.PORT_MST_FK AND PGT.PORT_GRP_MST_FK =" + PortGrpPK);
                    sb.Append(" AND P.BUSINESS_TYPE = 2 AND P.ACTIVE_FLAG = 1 ");
                    if (POLPK != "0")
                    {
                        sb.Append(" AND PGT.PORT_MST_FK IN (" + POLPK + ") ");
                    }
                }
                //sb.Append("SELECT P.PORT_MST_PK, P.PORT_ID FROM PORT_MST_TBL P WHERE P.PORT_GRP_MST_FK=" & Quotationpk)
                //sb.Append("AND P.BUSINESS_TYPE=2 AND P.ACTIVE_FLAG=1")

                return (objWF.GetDataSet(sb.ToString()));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public DataSet FetchToPortGroup(int QuotPK = 0, int PortGrpPK = 0, string PODPK = "0")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (QuotPK != 0)
                {
                    sb.Append(" SELECT P.PORT_MST_PK, P.PORT_ID, T.POD_GRP_FK FROM QUOTATION_DTL_TBL T, PORT_MST_TBL P WHERE T.PORT_MST_POD_FK = P.PORT_MST_PK AND T.POD_GRP_FK = " + PortGrpPK);
                    sb.Append(" AND T.QUOTATION_MST_FK =" + QuotPK);
                }
                else
                {
                    sb.Append(" SELECT P.PORT_MST_PK, P.PORT_ID, PGT.PORT_GRP_MST_FK POD_GRP_FK FROM PORT_MST_TBL P, PORT_GRP_TRN_TBL PGT WHERE P.PORT_MST_PK = PGT.PORT_MST_FK AND PGT.PORT_GRP_MST_FK =" + PortGrpPK);
                    sb.Append(" AND P.BUSINESS_TYPE = 2 AND P.ACTIVE_FLAG = 1");
                    if (PODPK != "0")
                    {
                        sb.Append(" AND PGT.PORT_MST_FK IN (" + PODPK + ") ");
                    }
                }
                //sb.Append("SELECT P.PORT_MST_PK, P.PORT_ID FROM PORT_MST_TBL P WHERE P.PORT_GRP_MST_FK=" & Quotationpk)
                //sb.Append("AND P.BUSINESS_TYPE=2 AND P.ACTIVE_FLAG=1")

                return (objWF.GetDataSet(sb.ToString()));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public DataSet FetchTariffGrp(int QuotPK = 0, int PortGrpPK = 0, string TariffPK = "0")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                sb.Append(" SELECT DISTINCT * FROM (");
                sb.Append(" SELECT POL.PORT_MST_PK   POL_PK,");
                sb.Append("       POL.PORT_ID       POL_ID,");
                sb.Append("       POD.PORT_MST_PK   POD_PK,");
                sb.Append("       POD.PORT_ID       POD_ID,");
                sb.Append("       T.POL_GRP_FK      POL_GRP_FK,");
                sb.Append("       T.PORT_MST_POD_FK POD_GRP_FK,");
                sb.Append("       T.TARIFF_GRP_FK   TARIFF_GRP_MST_PK");
                sb.Append("  FROM PORT_MST_TBL POL, PORT_MST_TBL POD, QUOTATION_DTL_TBL T");
                sb.Append(" WHERE T.PORT_MST_POD_FK = POL.PORT_MST_PK");
                sb.Append("   AND T.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND T.QUOTATION_MST_FK =" + QuotPK);
                sb.Append("   UNION");
                sb.Append(" SELECT POL.PORT_MST_PK       POL_PK,");
                sb.Append("       POL.PORT_ID           POL_ID,");
                sb.Append("       POD.PORT_MST_PK       POD_PK,");
                sb.Append("       POD.PORT_ID           POD_ID,");
                sb.Append("       TGM.POL_GRP_MST_FK    POL_GRP_FK,");
                sb.Append("       TGM.POD_GRP_MST_FK    POD_GRP_FK,");
                sb.Append("       TGM.TARIFF_GRP_MST_PK");
                sb.Append("  FROM PORT_MST_TBL       POL,");
                sb.Append("       PORT_MST_TBL       POD,");
                sb.Append("       TARIFF_GRP_TRN_TBL TGT,");
                sb.Append("       TARIFF_GRP_MST_TBL TGM");
                sb.Append(" WHERE TGM.TARIFF_GRP_MST_PK = TGT.TARIFF_GRP_MST_FK");
                sb.Append("   AND POL.PORT_MST_PK = TGT.POL_MST_FK");
                sb.Append("   AND POD.PORT_MST_PK = TGT.POD_MST_FK");
                sb.Append("   AND TGM.TARIFF_GRP_MST_PK =" + TariffPK);
                sb.Append("   AND POL.BUSINESS_TYPE = 2");
                sb.Append("   AND POL.ACTIVE_FLAG = 1");
                sb.Append("   )");

                //'Comeented if we are fetching from tariff screen records not displaying
                //If QuotPK <> 0 Then
                //    sb.Append(" SELECT POL.PORT_MST_PK   POL_PK,")
                //    sb.Append("       POL.PORT_ID       POL_ID,")
                //    sb.Append("       POD.PORT_MST_PK   POD_PK,")
                //    sb.Append("       POD.PORT_ID       POD_ID,")
                //    sb.Append("       T.POL_GRP_FK      POL_GRP_FK,")
                //    sb.Append("       T.PORT_MST_POD_FK POD_GRP_FK,")
                //    sb.Append("       T.TARIFF_GRP_FK   TARIFF_GRP_MST_PK")
                //    sb.Append("  FROM PORT_MST_TBL POL, PORT_MST_TBL POD, QUOTATION_DTL_TBL T")
                //    sb.Append(" WHERE T.PORT_MST_POD_FK = POL.PORT_MST_PK")
                //    sb.Append("   AND T.PORT_MST_POD_FK = POD.PORT_MST_PK")
                //    sb.Append("   AND T.QUOTATION_MST_FK =" & QuotPK)
                //Else
                //    sb.Append(" SELECT POL.PORT_MST_PK       POL_PK,")
                //    sb.Append("       POL.PORT_ID           POL_ID,")
                //    sb.Append("       POD.PORT_MST_PK       POD_PK,")
                //    sb.Append("       POD.PORT_ID           POD_ID,")
                //    sb.Append("       TGM.POL_GRP_MST_FK    POL_GRP_FK,")
                //    sb.Append("       TGM.POD_GRP_MST_FK    POD_GRP_FK,")
                //    sb.Append("       TGM.TARIFF_GRP_MST_PK")
                //    sb.Append("  FROM PORT_MST_TBL       POL,")
                //    sb.Append("       PORT_MST_TBL       POD,")
                //    sb.Append("       TARIFF_GRP_TRN_TBL TGT,")
                //    sb.Append("       TARIFF_GRP_MST_TBL TGM")
                //    sb.Append(" WHERE TGM.TARIFF_GRP_MST_PK = TGT.TARIFF_GRP_MST_FK")
                //    sb.Append("   AND POL.PORT_MST_PK = TGT.POL_MST_FK")
                //    sb.Append("   AND POD.PORT_MST_PK = TGT.POD_MST_FK")
                //    sb.Append("   AND TGM.TARIFF_GRP_MST_PK =" & TariffPK)
                //    sb.Append("   AND POL.BUSINESS_TYPE = 2")
                //    sb.Append("   AND POL.ACTIVE_FLAG = 1")
                //End If


                return (objWF.GetDataSet(sb.ToString()));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public DataSet FetchTariffPODGrp(int QuotPK = 0, int PortGrpPK = 0, string TariffPK = "0")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (QuotPK != 0)
                {
                    sb.Append(" SELECT P.PORT_MST_PK, P.PORT_ID, T.POD_GRP_FK, T.TARIFF_GRP_FK FROM QUOTATION_DTL_TBL T, PORT_MST_TBL P WHERE T.PORT_MST_POD_FK = P.PORT_MST_PK AND T.TARIFF_GRP_FK = " + TariffPK);
                    sb.Append(" AND T.QUOTATION_MST_FK =" + QuotPK);
                }
                else
                {
                    sb.Append(" SELECT P.PORT_MST_PK,");
                    sb.Append("        P.PORT_ID,");
                    sb.Append("        TGM.POD_GRP_MST_FK POD_GRP_FK,");
                    sb.Append("        TGM.TARIFF_GRP_MST_PK");
                    sb.Append("  FROM PORT_MST_TBL P, TARIFF_GRP_TRN_TBL TGT, TARIFF_GRP_MST_TBL TGM");
                    sb.Append(" WHERE TGM.TARIFF_GRP_MST_PK = TGT.TARIFF_GRP_MST_FK");
                    sb.Append("   AND P.PORT_MST_PK = TGT.POD_MST_FK");
                    sb.Append("   AND TGM.TARIFF_GRP_MST_PK =" + TariffPK);
                    sb.Append("   AND P.BUSINESS_TYPE = 2");
                    sb.Append("   AND P.ACTIVE_FLAG = 1");
                }
                //sb.Append("SELECT P.PORT_MST_PK, P.PORT_ID FROM PORT_MST_TBL P WHERE P.PORT_GRP_MST_FK=" & Quotationpk)
                //sb.Append("AND P.BUSINESS_TYPE=2 AND P.ACTIVE_FLAG=1")

                return (objWF.GetDataSet(sb.ToString()));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        public DataSet FetchExpanses(int RefPK, int BizType, int BaseCurPK, int CargoType, int RefFlag, int PortGroup, long POLPK, long PODPK, long OperatorPK)
        {

            WorkFlow objWF = new WorkFlow();


            try
            {
                objWF.MyCommand.Parameters.Clear();

                var _with39 = objWF.MyCommand.Parameters;
                _with39.Add("QUOTATION_FK_IN", RefPK).Direction = ParameterDirection.Input;
                _with39.Add("BIZTYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with39.Add("BASE_CURRENCY_MST_PK_IN", BaseCurPK).Direction = ParameterDirection.Input;
                _with39.Add("PORT_GROUP_IN", PortGroup).Direction = ParameterDirection.Input;
                _with39.Add("POL_FK_IN", POLPK).Direction = ParameterDirection.Input;
                _with39.Add("POD_FK_IN", PODPK).Direction = ParameterDirection.Input;
                _with39.Add("OPERATOR_FK_IN", OperatorPK).Direction = ParameterDirection.Input;
                _with39.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                /// Quotation Sea
                if (RefFlag == 0)
                {
                    if (CargoType == 1)
                    {
                        return objWF.GetDataSet("FETCH_PROFITABILITY_PKG", "FETCH_QUOT_EXPENSE_FCL");
                    }
                    else if (CargoType == 2)
                    {
                        return objWF.GetDataSet("FETCH_PROFITABILITY_PKG", "FETCH_QUOT_EXPENSE_LCL");
                    }
                    /// Booking
                }
                else if (RefFlag == 1)
                {
                    if (CargoType == 1)
                    {
                        return objWF.GetDataSet("FETCH_PROFITABILITY_PKG", "FETCH_BKG_EXPENSE_FCL");
                    }
                    else if (CargoType == 2)
                    {
                        return objWF.GetDataSet("FETCH_PROFITABILITY_PKG", "FETCH_BKG_EXPENSE_LCL");
                    }
                    /// SRR
                }
                else if (RefFlag == 2)
                {
                    if (CargoType == 1)
                    {
                        return objWF.GetDataSet("FETCH_PROFITABILITY_PKG", "FETCH_SRR_EXPENSE_FCL");
                    }
                    else if (CargoType == 2)
                    {
                        return objWF.GetDataSet("FETCH_PROFITABILITY_PKG", "FETCH_SRR_EXPENSE_LCL");
                    }
                    /// Quotation Air
                }
                else if (RefFlag == 3)
                {
                    return objWF.GetDataSet("FETCH_PROFITABILITY_PKG", "FETCH_QUOT_EXPENSE_AIR");
                    /// SRR Air
                }
                else if (RefFlag == 4)
                {
                    return objWF.GetDataSet("FETCH_PROFITABILITY_PKG", "FETCH_SRR_EXPENSE_AIR");
                    /// Booking Air
                }
                else if (RefFlag == 5)
                {
                    return objWF.GetDataSet("FETCH_PROFITABILITY_PKG", "FETCH_BKG_EXPENSE_AIR");
                    ///' Quotation Air ( Specific )
                }
                else if (RefFlag == 6)
                {
                    return objWF.GetDataSet("FETCH_PROFITABILITY_PKG", "FETCH_QUOT_EXPENSE_SPC_AIR");
                }

            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
            return new DataSet();
        }

        public DataSet FetchHeader(int RefPK, int RefFlag, int BaseCurPK)
        {

            WorkFlow objWF = new WorkFlow();


            try
            {
                objWF.MyCommand.Parameters.Clear();

                var _with40 = objWF.MyCommand.Parameters;
                _with40.Add("QUOTATION_FK_IN", RefPK).Direction = ParameterDirection.Input;
                _with40.Add("REF_FLAG_IN", RefFlag).Direction = ParameterDirection.Input;
                _with40.Add("BASE_CURRENCY_MST_PK_IN", BaseCurPK).Direction = ParameterDirection.Input;
                _with40.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWF.GetDataSet("FETCH_PROFITABILITY_PKG", "FETCH_HEADER");

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

        public DataSet checkApprover(int RefPK, int RefFlag)
        {

            WorkFlow objWF = new WorkFlow();


            try
            {
                objWF.MyCommand.Parameters.Clear();

                var _with41 = objWF.MyCommand.Parameters;
                _with41.Add("QUOTATION_FK_IN", RefPK).Direction = ParameterDirection.Input;
                _with41.Add("REF_FLAG_IN", RefFlag).Direction = ParameterDirection.Input;
                _with41.Add("LOGGED_LOC_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                _with41.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWF.GetDataSet("FETCH_PROFITABILITY_PKG", "FETCH_CHECK_APPROVER");

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

        public DataSet FetchPOLDropDown(int RefPK, int RefFlag, int PortGroup)
        {
            WorkFlow objWF = new WorkFlow();

            try
            {
                objWF.MyCommand.Parameters.Clear();

                var _with42 = objWF.MyCommand.Parameters;
                _with42.Add("QUOTATION_FK_IN", RefPK).Direction = ParameterDirection.Input;
                _with42.Add("REF_FLAG_IN", RefFlag).Direction = ParameterDirection.Input;
                _with42.Add("PORT_GROUP_IN", PortGroup).Direction = ParameterDirection.Input;
                _with42.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWF.GetDataSet("FETCH_PROFITABILITY_PKG", "FETCH_POL_DROP_DOWN");
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

        public DataSet FetchPODDropDown(int RefPK, int RefFlag, int PortGroup)
        {
            WorkFlow objWF = new WorkFlow();

            try
            {
                objWF.MyCommand.Parameters.Clear();

                var _with43 = objWF.MyCommand.Parameters;
                _with43.Add("QUOTATION_FK_IN", RefPK).Direction = ParameterDirection.Input;
                _with43.Add("REF_FLAG_IN", RefFlag).Direction = ParameterDirection.Input;
                _with43.Add("PORT_GROUP_IN", PortGroup).Direction = ParameterDirection.Input;
                _with43.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWF.GetDataSet("FETCH_PROFITABILITY_PKG", "FETCH_POD_DROP_DOWN");
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

        public DataSet FillOperatorDropDown(int RefPK, int RefFlag, int PortGroup, int POLPK, int PODPK)
        {
            WorkFlow objWF = new WorkFlow();

            try
            {
                objWF.MyCommand.Parameters.Clear();

                var _with44 = objWF.MyCommand.Parameters;
                _with44.Add("QUOTATION_FK_IN", RefPK).Direction = ParameterDirection.Input;
                _with44.Add("REF_FLAG_IN", RefFlag).Direction = ParameterDirection.Input;
                _with44.Add("PORT_GROUP_IN", PortGroup).Direction = ParameterDirection.Input;
                _with44.Add("POL_FK_IN", POLPK).Direction = ParameterDirection.Input;
                _with44.Add("POD_FK_IN", PODPK).Direction = ParameterDirection.Input;
                _with44.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWF.GetDataSet("FETCH_PROFITABILITY_PKG", "FETCH_OPERATOR_DROP_DOWN");
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

        public DataSet FetchDocumentDetails(int RefPK, int RefFlag, int DocFlag)
        {

            WorkFlow objWF = new WorkFlow();


            try
            {
                objWF.MyCommand.Parameters.Clear();

                var _with45 = objWF.MyCommand.Parameters;
                _with45.Add("QUOTATION_FK_IN", RefPK).Direction = ParameterDirection.Input;
                _with45.Add("REF_FLAG_IN", RefFlag).Direction = ParameterDirection.Input;
                _with45.Add("DOC_FLAG_IN", DocFlag).Direction = ParameterDirection.Input;
                _with45.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWF.GetDataSet("FETCH_PROFITABILITY_PKG", "FETCH_DOCUMENT_DETAILS");

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

        public DataSet FetchIncome(long RefPK, int BizType, int BaseCurPK, int CargoType, int RefFlag, int PortGroup, long POLPK, long PODPK, long OperatorPK)
        {

            WorkFlow objWF = new WorkFlow();


            try
            {
                objWF.MyCommand.Parameters.Clear();

                var _with46 = objWF.MyCommand.Parameters;
                _with46.Add("QUOTATION_FK_IN", RefPK).Direction = ParameterDirection.Input;
                _with46.Add("BIZTYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with46.Add("BASE_CURRENCY_MST_PK_IN", BaseCurPK).Direction = ParameterDirection.Input;
                _with46.Add("PORT_GROUP_IN", PortGroup).Direction = ParameterDirection.Input;
                _with46.Add("POL_FK_IN", POLPK).Direction = ParameterDirection.Input;
                _with46.Add("POD_FK_IN", PODPK).Direction = ParameterDirection.Input;
                _with46.Add("OPERATOR_FK_IN", OperatorPK).Direction = ParameterDirection.Input;
                _with46.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                //If RefFlag = 3 Then
                //    RefFlag = 2
                //End If
                ///' Quotation Sea
                if (RefFlag == 0)
                {
                    if (CargoType == 1)
                    {
                        return objWF.GetDataSet("FETCH_PROFITABILITY_PKG", "FETCH_QUOT_INCOME_FCL");
                    }
                    else if (CargoType == 2)
                    {
                        return objWF.GetDataSet("FETCH_PROFITABILITY_PKG", "FETCH_QUOT_INCOME_LCL");
                    }
                    ///' Booking
                }
                else if (RefFlag == 1)
                {
                    if (CargoType == 1)
                    {
                        return objWF.GetDataSet("FETCH_PROFITABILITY_PKG", "FETCH_BKG_INCOME_FCL");
                    }
                    else if (CargoType == 2)
                    {
                        return objWF.GetDataSet("FETCH_PROFITABILITY_PKG", "FETCH_BKG_INCOME_LCL");
                    }
                    ///' SRR
                }
                else if (RefFlag == 2)
                {
                    if (CargoType == 1)
                    {
                        return objWF.GetDataSet("FETCH_PROFITABILITY_PKG", "FETCH_SRR_INCOME_FCL");
                    }
                    else if (CargoType == 2)
                    {
                        return objWF.GetDataSet("FETCH_PROFITABILITY_PKG", "FETCH_SRR_INCOME_LCL");
                    }
                    ///' Quotation Air
                }
                else if (RefFlag == 3)
                {
                    return objWF.GetDataSet("FETCH_PROFITABILITY_PKG", "FETCH_QUOT_INCOME_AIR");
                    ///' SRR Air
                }
                else if (RefFlag == 4)
                {
                    return objWF.GetDataSet("FETCH_PROFITABILITY_PKG", "FETCH_SRR_INCOME_AIR");
                    ///' Booking Air
                }
                else if (RefFlag == 5)
                {
                    return objWF.GetDataSet("FETCH_PROFITABILITY_PKG", "FETCH_BKG_INCOME_AIR");
                    ///' Quotation Air ( Specific )
                }
                else if (RefFlag == 6)
                {
                    return objWF.GetDataSet("FETCH_PROFITABILITY_PKG", "FETCH_QUOT_INCOME_SPC_AIR");
                }

            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
            return new DataSet();
        }

        public DataSet FetchProfit(int RefPK, int BizType, int CargoType, int RefFlag)
        {

            WorkFlow objWF = new WorkFlow();


            try
            {
                objWF.MyCommand.Parameters.Clear();

                var _with47 = objWF.MyCommand.Parameters;
                _with47.Add("QUOTATION_FK_IN", RefPK).Direction = ParameterDirection.Input;
                _with47.Add("BIZTYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with47.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                /// Quotation Sea
                if (RefFlag == 0)
                {
                    if (CargoType == 1)
                    {
                        return objWF.GetDataSet("FETCH_PROFITABILITY_PKG", "FETCH_QUOT_PROFIT_FCL");
                    }
                    else if (CargoType == 2)
                    {
                        return objWF.GetDataSet("FETCH_PROFITABILITY_PKG", "FETCH_QUOT_PROFIT_LCL");
                    }
                    /// Booking
                }
                else if (RefFlag == 1)
                {
                    if (CargoType == 1)
                    {
                        return objWF.GetDataSet("FETCH_PROFITABILITY_PKG", "FETCH_BKG_PROFIT_FCL");
                    }
                    else if (CargoType == 2)
                    {
                        return objWF.GetDataSet("FETCH_PROFITABILITY_PKG", "FETCH_BKG_PROFIT_LCL");
                    }
                    /// SRR
                }
                else if (RefFlag == 2)
                {
                    if (CargoType == 1)
                    {
                        return objWF.GetDataSet("FETCH_PROFITABILITY_PKG", "FETCH_SRR_PROFIT_FCL");
                    }
                    else if (CargoType == 2)
                    {
                        return objWF.GetDataSet("FETCH_PROFITABILITY_PKG", "FETCH_SRR_PROFIT_LCL");
                    }
                    /// Quotation Air
                }
                else if (RefFlag == 3)
                {
                    return objWF.GetDataSet("FETCH_PROFITABILITY_PKG", "FETCH_QUOT_PROFIT_AIR");
                    /// SRR Air
                }
                else if (RefFlag == 4)
                {
                    return objWF.GetDataSet("FETCH_PROFITABILITY_PKG", "FETCH_SRR_PROFIT_AIR");
                    /// Booking Air
                }
                else if (RefFlag == 5)
                {
                    return objWF.GetDataSet("FETCH_PROFITABILITY_PKG", "FETCH_BKG_PROFIT_AIR");
                    ///' Quotation Air ( Specific )
                }
                else if (RefFlag == 6)
                {
                    return objWF.GetDataSet("FETCH_PROFITABILITY_PKG", "FETCH_QUOT_PROFIT_SPC_AIR");
                }

            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
            return new DataSet();
        }
        /// End
        public int FetchQuotFrom(int QuotPK)
        {
            try
            {
                string strSQL = null;
                WorkFlow objWF = new WorkFlow();
                strSQL = " SELECT DISTINCT T.TRANS_REFERED_FROM FROM QUOTATION_DTL_TBL T WHERE T.QUOTATION_MST_FK = " + QuotPK;
                return Convert.ToInt32(objWF.ExecuteScaler(strSQL));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public string GetPolName(string PolPK)
        {
            try
            {
                string strSQL = null;
                WorkFlow objWF = new WorkFlow();
                strSQL = "  SELECT POL.PORT_NAME FROM PORT_MST_TBL POL WHERE POL.PORT_MST_PK= " + PolPK;
                return (objWF.ExecuteScaler(strSQL));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public string GetPodName(string PolPK)
        {
            try
            {
                string strSQL = null;
                WorkFlow objWF = new WorkFlow();
                strSQL = "  SELECT POL.PORT_NAME FROM PORT_MST_TBL POL WHERE POL.PORT_MST_PK= " + PolPK;
                return (objWF.ExecuteScaler(strSQL));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public System.DateTime GetQuotDt(string QuotPK)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append("   SELECT Q.QUOTATION_DATE ");
            sb.Append("   FROM QUOTATION_MST_TBL Q");
            sb.Append("   WHERE Q.QUOTATION_MST_PK = " + QuotPK);
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
            sb.Append("   FROM QUOTATION_MST_TBL Q");
            sb.Append("   WHERE Q.QUOTATION_MST_PK = " + QuotPK);
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
        public int GetAutoQuote(string QuotPK)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append("   SELECT Q.AUTO_CREATE ");
            sb.Append("   FROM QUOTATION_MST_TBL Q");
            sb.Append("   WHERE Q.QUOTATION_MST_PK = " + QuotPK);
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
        public DataSet GetAIFCharges()
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append("   SELECT ROWTOCOL('SELECT F.FREIGHT_ELEMENT_MST_PK || F.CHARGE_BASIS ");
            sb.Append("   FROM FREIGHT_ELEMENT_MST_TBL F WHERE F.FREIGHT_ELEMENT_MST_PK IN (' || ");
            sb.Append("   NVL(FEMT.AIF_FREIGHT_FKS,0) || ')') FROM FREIGHT_ELEMENT_MST_TBL FEMT, PARAMETERS_TBL PT ");
            sb.Append("   WHERE FEMT.FREIGHT_ELEMENT_MST_PK = PT.FRT_AIF_FK ");
            try
            {
                return objWF.GetDataSet(sb.ToString());
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
        public DataSet GetAIFStatus(string QuotPK)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append("   SELECT Q.AIF_FLAG FROM QUOTATION_MST_TBL Q WHERE Q.QUOTATION_MST_PK=" + QuotPK);
            try
            {
                return objWF.GetDataSet(sb.ToString());
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
        public DataSet FetchIMOCode(int ExpContPK)
        {
            //Add by Raghavendra
            WorkFlow objWF = new WorkFlow();
            string UNnr = null;
            //job_trn_spl_req()
            string strQuery = "SELECT CO.IMDG_CLASS_CODE,CO.UN_NO,CO.PROPER_SHIPPING_NAME FROM  job_trn_spl_req co WHERE co.job_trn_sea_exp_cont_fk=" + ExpContPK;
            try
            {
                return objWF.GetDataSet(strQuery);

            }
            catch (Exception ex)
            {
                throw ex;

            }
            finally
            {
            }
        }

        #region "Fetch Local Charges Income & Expense"
        public DataSet FetchProfitabilityLocal(int RefPK, int RefFlag, int BaseCurPK, bool IncomeFlag = true)
        {

            WorkFlow objWF = new WorkFlow();
            DataSet dsLocal = null;
            //If RefFlag = 2 Then
            //    RefFlag = 3
            //End If
            try
            {
                objWF.MyCommand.Parameters.Clear();
                var _with48 = objWF.MyCommand.Parameters;
                _with48.Add("QUOTATION_FK_IN", RefPK).Direction = ParameterDirection.Input;
                _with48.Add("REF_FLAG_IN", RefFlag).Direction = ParameterDirection.Input;
                _with48.Add("BASE_CURRENCY_FK_IN", BaseCurPK).Direction = ParameterDirection.Input;
                _with48.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                if (IncomeFlag)
                {
                    dsLocal = objWF.GetDataSet("FETCH_PROFITABILITY_PKG", "FETCH_LOCAL_CHARGES_INCOME");
                }
                else
                {
                    dsLocal = objWF.GetDataSet("FETCH_PROFITABILITY_PKG", "FETCH_LOCAL_CHARGES_EXPENSE");
                }
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
            return dsLocal;
        }

        public DataSet FetchLocalCharges(short QtnOrBkg, int QtnOrBkgPk, int BaseCurrencyFk, bool ForExpense = false)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            DataSet dsLocal = null;

            sb.Append("SELECT QL.QUOTATION_LOCAL_PK,");
            sb.Append("       QL.QUOTATION_MST_FK,");
            sb.Append("       QL.TARIFF_TRN_FK,");
            sb.Append("       QL.FROM_FLAG,");
            if (!ForExpense)
            {
                sb.Append("       FEMT.FREIGHT_ELEMENT_MST_PK ELEMENT_PK,");
                sb.Append("       FEMT.FREIGHT_ELEMENT_ID ELEMENT_ID,");
                sb.Append("       FEMT.FREIGHT_ELEMENT_NAME ELEMENT_NAME,");
            }
            else
            {
                sb.Append("       CEMT.COST_ELEMENT_MST_PK ELEMENT_PK,");
                sb.Append("       CEMT.COST_ELEMENT_ID ELEMENT_ID,");
                sb.Append("       CEMT.COST_ELEMENT_NAME ELEMENT_NAME,");
            }
            sb.Append("           (CASE");
            sb.Append("                         WHEN NVL(QL.FROM_FLAG,1) = 1 THEN");
            sb.Append("                          (SELECT Q.BASE_CURRENCY_FK");
            sb.Append("                             FROM QUOTATION_MST_TBL Q");
            sb.Append("                            WHERE Q.QUOTATION_MST_PK = QL.QUOTATION_MST_FK)");
            sb.Append("                         WHEN NVL(QL.FROM_FLAG, 2) = 2 THEN ");
            sb.Append("                          (SELECT B.BASE_CURRENCY_FK");
            sb.Append("                             FROM BOOKING_MST_TBL B");
            sb.Append("                            WHERE B.BOOKING_MST_PK = QL.QUOTATION_MST_FK)");
            sb.Append("                         WHEN NVL(QL.FROM_FLAG, 3) = 3 THEN ");
            sb.Append("                          (SELECT S.BASE_CURRENCY_FK");
            sb.Append("                             FROM SRR_SEA_TBL S");
            sb.Append("                            WHERE S.SRR_SEA_PK = QL.QUOTATION_MST_FK)");
            sb.Append("                         ELSE ");
            sb.Append(HttpContext.Current.Session["currency_mst_pk"]);
            sb.Append("                       END) CURRENCY_MST_PK,");
            sb.Append("                       (CASE");
            sb.Append("                         WHEN NVL(QL.FROM_FLAG,1) = 1 THEN");
            sb.Append("                          (SELECT CT.CURRENCY_ID");
            sb.Append("                             FROM QUOTATION_MST_TBL Q, CURRENCY_TYPE_MST_TBL CT");
            sb.Append("                            WHERE Q.QUOTATION_MST_PK = QL.QUOTATION_MST_FK");
            sb.Append("                              AND Q.BASE_CURRENCY_FK = CT.CURRENCY_MST_PK)");
            sb.Append("                         WHEN NVL(QL.FROM_FLAG,2) = 2 THEN ");
            sb.Append("                          (SELECT CT.CURRENCY_ID");
            sb.Append("                             FROM BOOKING_MST_TBL B, CURRENCY_TYPE_MST_TBL CT");
            sb.Append("                            WHERE B.BOOKING_MST_PK = QL.QUOTATION_MST_FK");
            sb.Append("                              AND B.BASE_CURRENCY_FK = CT.CURRENCY_MST_PK)");
            sb.Append("                        WHEN NVL(QL.FROM_FLAG,3) = 3 THEN ");
            sb.Append("                          (SELECT CT.CURRENCY_ID");
            sb.Append("                             FROM SRR_SEA_TBL S, CURRENCY_TYPE_MST_TBL CT");
            sb.Append("                            WHERE S.SRR_SEA_PK = QL.QUOTATION_MST_FK");
            sb.Append("                              AND S.BASE_CURRENCY_FK = CT.CURRENCY_MST_PK)");
            sb.Append("                        ELSE ");
            sb.Append("                          '" + HttpContext.Current.Session["CURRENCY_ID"] + "' ");
            sb.Append("                       END) CURRENCY_ID,");
            sb.Append("       ROUND(QL.CONT_20_AMT, 2) \"20\",");
            sb.Append("       ROUND(QL.CONT_40_AMT, 2) \"40\",");
            sb.Append("       ROUND(NVL(QL.SHIPMENT_AMT, QL.MINIMUM_AMT), 2) SHP_AMT,");
            sb.Append("       1 ROE, ");
            sb.Append("       FEMT.CHARGE_BASIS, ");
            sb.Append("       (SELECT QD.DD_ID FROM QFOR_DROP_DOWN_TBL QD ");
            sb.Append("       WHERE QD.CONFIG_ID = 'QFOR4458' AND QD.DD_FLAG = 'BASIS' ");
            sb.Append("       AND QD.DD_VALUE = FEMT.CHARGE_BASIS) BASIS ");
            sb.Append("  FROM QUOTATION_LOCAL_TRN QL,");
            sb.Append("       TARIFF_TRN TT,");
            sb.Append("       FREIGHT_ELEMENT_MST_TBL FEMT,");
            sb.Append("       COST_ELEMENT_MST_TBL CEMT,");
            sb.Append("       CURRENCY_TYPE_MST_TBL CTMT");
            sb.Append(" WHERE QL.TARIFF_TRN_FK = TT.TARIFF_PK");
            sb.Append("   AND TT.FREIGHT_ELEMENT_MST_FK = FEMT.FREIGHT_ELEMENT_MST_PK");
            sb.Append("   AND UPPER(FEMT.FREIGHT_ELEMENT_ID) = UPPER(CEMT.COST_ELEMENT_ID)");
            sb.Append("   AND TT.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK");
            if (QtnOrBkg == 0)
            {
                QtnOrBkg = 3;
            }
            sb.Append("   AND QL.FROM_FLAG = " + QtnOrBkg);
            sb.Append("   AND QL.QUOTATION_MST_FK = " + QtnOrBkgPk);
            if (!ForExpense)
            {
                sb.Append("   AND FEMT.CHARGE_BASIS IN (3, 4, 6, 8, 9) ");
                sb.Append(" ORDER BY FEMT.PREFERENCE ");
            }
            else
            {
                sb.Append("   AND CEMT.CHARGE_BASIS IN (3, 4, 6, 8, 9) ");
                sb.Append(" ORDER BY CEMT.PREFERENCE ");
            }

            try
            {
                dsLocal = objWF.GetDataSet(sb.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return dsLocal;
        }
        #endregion
    }
}