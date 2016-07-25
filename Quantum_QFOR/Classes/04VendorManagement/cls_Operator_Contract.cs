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
using System;
using System.Collections;
using System.Data;
using System.Web;

namespace Quantum_QFOR
{
    public class cls_Operator_Contract : CommonFeatures
    {
        //Public WithEvents Message1 As New QForDev.WorkFlow.Message
        #region "Private Variables"
        #endregion
        private long _PkValue;

        #region "Property"
        public long ContractPk
        {
            get { return _PkValue; }
        }
        #endregion

        #region "FetchHeader Function- Main"
        public DataTable FetchHeader(string strPolPk, string strPodPk, string strContId, bool IsLCL, string ValidFrom, string ValidTo, string Mode, int SLContractPk, int RFQContractPk)
        {
            string str = null;
            WorkFlow objWF = new WorkFlow();
            DataTable dtMain = new DataTable();
            DataTable dtContainerType = new DataTable();
            DataColumn dcCol = null;
            Int16 i = default(Int16);
            Array arrPolPk = null;
            Array arrPodPk = null;
            string strCondition = "";
            string strNewModeCondition = "";
            arrPolPk = strPolPk.Split(',');
            arrPodPk = strPodPk.Split(',');
            for (i = 0; i <= arrPolPk.Length - 1; i++)
            {
                if (string.IsNullOrEmpty(strCondition))
                {
                    strCondition = " (POL.PORT_MST_PK =" + arrPolPk.GetValue(i) + " AND POD.PORT_MST_PK =" + arrPodPk.GetValue(i) + ")";
                }
                else
                {
                    strCondition += " OR (POL.PORT_MST_PK =" + arrPolPk.GetValue(i) + " AND POD.PORT_MST_PK =" + arrPodPk.GetValue(i) + ")";
                }
            }
            if (!(Mode == "EDIT"))
            {
                strNewModeCondition = " AND POL.BUSINESS_TYPE = 2";
                strNewModeCondition += " AND POD.BUSINESS_TYPE = 2";
            }
            if (SLContractPk > 0)
            {
                str = " SELECT DISTINCT POL.PORT_MST_PK AS POLPK,POL.PORT_ID AS POL,";
                str += " POD.PORT_MST_PK AS PODPK,POD.PORT_ID AS POD , ";
                str += " TO_CHAR(CTS.VALID_FROM,'DD/mm/yyyy') VALID_FROM,  TO_CHAR(CTS.VALID_TO,'DD/mm/yyyy') VALID_TO , CTS.SHIP_TERM_MST_FK";
                str += "FROM CONT_TRN_SEA_FCL_LCL CTS,PORT_MST_TBL POL, PORT_MST_TBL POD";
                str += " WHERE CTS.PORT_MST_POL_FK = POL.PORT_MST_PK ";
                str += " AND CTS.PORT_MST_POD_FK = POD.PORT_MST_PK";
                str += " AND CTS.CONT_MAIN_SEA_FK = " + SLContractPk;
                str += " AND (";
                str += strCondition + ")";
                str += strNewModeCondition;
                str += " GROUP BY POL.PORT_MST_PK,POL.PORT_ID,POD.PORT_ID,POD.PORT_MST_PK, CTS.SHIP_TERM_MST_FK, CTS.VALID_FROM,CTS.VALID_TO ";
                str += " HAVING POL.PORT_ID<>POD.PORT_ID";
                str += " ORDER BY POL.PORT_ID";
            }
            else if (RFQContractPk > 0)
            {
                str = " SELECT DISTINCT POL.PORT_MST_PK AS POLPK,POL.PORT_ID AS POL,";
                str += " POD.PORT_MST_PK AS PODPK,POD.PORT_ID AS POD , ";
                str += " TO_CHAR(RTS.VALID_FROM,'DD/mm/yyyy') VALID_FROM, TO_CHAR(RTS.VALID_TO,'DD/mm/yyyy') VALID_TO, RTS.SHIP_TERM_MST_FK";
                str += " FROM RFQ_TRN_SEA_FCL_LCL RTS,PORT_MST_TBL POL, PORT_MST_TBL POD";
                str += " WHERE RTS.PORT_MST_POL_FK = POL.PORT_MST_PK ";
                str += " AND RTS.PORT_MST_POD_FK = POD.PORT_MST_PK";
                str += " AND RTS.RFQ_MAIN_SEA_FK = " + RFQContractPk;
                str += " AND (";
                str += strCondition + ")";
                str += strNewModeCondition;
                str += " GROUP BY POL.PORT_MST_PK,POL.PORT_ID,POD.PORT_ID,POD.PORT_MST_PK, RTS.SHIP_TERM_MST_FK, RTS.VALID_FROM,RTS.VALID_TO";
                str += " HAVING POL.PORT_ID<>POD.PORT_ID";
                str += " ORDER BY POL.PORT_ID";
            }
            else
            {
                str = " SELECT POL.PORT_MST_PK AS \"POLPK\",POL.PORT_ID AS \"POL\",";
                str += " POD.PORT_MST_PK AS \"PODPK\",POD.PORT_ID AS \"POD\" , ";
                str += " TO_DATE('" + ValidFrom + "','DD/MM/YYYY') AS VALID_FROM, TO_DATE('" + ValidTo + "','DD/MM/YYYY') AS VALID_TO, NULL SHIP_TERM_MST_FK ";
                str += " FROM PORT_MST_TBL POL, PORT_MST_TBL POD";
                str += " WHERE (1=1)";
                str += " AND (";
                str += strCondition + ")";
                str += strNewModeCondition;
                str += " GROUP BY POL.PORT_MST_PK,POL.PORT_ID,POD.PORT_ID,POD.PORT_MST_PK";
                str += " HAVING POL.PORT_ID<>POD.PORT_ID";
                str += " ORDER BY POL.PORT_ID";
            }

            try
            {
                dtMain = objWF.GetDataTable(str);
                if (!IsLCL)
                {
                    dtContainerType = FetchActiveCont(strContId);
                    for (i = 0; i <= dtContainerType.Rows.Count - 1; i++)
                    {
                        dcCol = new DataColumn(Convert.ToString(dtContainerType.Rows[i][0]), typeof(decimal));
                        dtMain.Columns.Add(dcCol);

                        dcCol = new DataColumn(Convert.ToString(dtContainerType.Rows[i][1]), typeof(decimal));
                        dtMain.Columns.Add(dcCol);

                        dcCol = new DataColumn(Convert.ToString(dtContainerType.Rows[i][2]), typeof(decimal));
                        dtMain.Columns.Add(dcCol);
                    }
                }
                else
                {
                    dtMain.Columns.Add("Basis");
                    dtMain.Columns.Add("Curr", typeof(decimal));
                    dtMain.Columns.Add("Req", typeof(decimal));
                    dtMain.Columns.Add("BasisPK");
                    dtMain.Columns.Add("App", typeof(decimal));
                }
                dtMain.Columns.Add("Trade");
                dtMain.Columns.Add("Routing");
                return dtMain;
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "FetchActiveCont"
        public DataTable FetchActiveCont(string strContId = "")
        {
            string str = null;
            WorkFlow objWF = new WorkFlow();
            if (!string.IsNullOrEmpty(strContId))
            {
                strContId = " AND CTMT.CONTAINER_TYPE_MST_ID IN (" + strContId + ") ";
            }
            str = " SELECT 'C-' || CTMT.CONTAINER_TYPE_MST_ID,'R-' || CTMT.CONTAINER_TYPE_MST_ID,CTMT.CONTAINER_TYPE_MST_ID,CTMT.CONTAINER_TYPE_NAME" + " FROM CONTAINER_TYPE_MST_TBL CTMT WHERE CTMT.ACTIVE_FLAG=1" + strContId + " ORDER BY CTMT.PREFERENCES";
            try
            {
                return objWF.GetDataTable(str);
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "FetchFreight"
        public DataTable FetchFreight(string strPolPk, string strPodPk, string strContId, bool IsLCL, string Mode, Int16 Chk = 0, Int16 ChkThc = 0, string FromDt = "", string ToDt = "", long ContractPK = 0,
        string RFQ_REFNO = "")
        {
            string str = null;
            WorkFlow objWF = new WorkFlow();
            DataTable dtMain = new DataTable();
            DataTable dtContainerType = new DataTable();
            DataColumn dcCol = null;
            Int16 i = default(Int16);
            Array arrPolPk = null;
            Array arrPodPk = null;
            string strCondition = "";
            string strNewModeCondition = "";
            string strThlThdPk = null;
            strThlThdPk = " select f.freight_element_mst_pk from freight_element_mst_tbl f where f.freight_element_id like 'THL' UNION";
            strThlThdPk += " select f.freight_element_mst_pk from freight_element_mst_tbl f where f.freight_element_id like 'THD'";
            arrPolPk = strPolPk.Split(',');
            arrPodPk = strPodPk.Split(',');
            for (i = 0; i <= arrPolPk.Length - 1; i++)
            {
                if (string.IsNullOrEmpty(strCondition))
                {
                    strCondition = " (POL.PORT_MST_PK =" + arrPolPk.GetValue(i) + " AND POD.PORT_MST_PK =" + arrPodPk.GetValue(i) + ")";
                }
                else
                {
                    strCondition += " OR (POL.PORT_MST_PK =" + arrPolPk.GetValue(i) + " AND POD.PORT_MST_PK =" + arrPodPk.GetValue(i) + ")";
                }
            }

            if (!(Mode == "EDIT"))
            {
                strNewModeCondition = " AND POL.BUSINESS_TYPE = 2";
                strNewModeCondition += " AND POD.BUSINESS_TYPE = 2";
                strNewModeCondition += " AND FMT.ACTIVE_FLAG = 1";
                strNewModeCondition += " AND FMT.BUSINESS_TYPE IN (2,3)";
            }

            if (Mode == "NEW")
            {
            }
            str = " select qry.POLPK PORT_MST_PK, qry.POL POL,";
            str += " qry.PODPK PORT_MST_PK, qry.POD POD,";
            str += " qry.FREIGHT_ELEMENT_MST_PK,";
            str += " qry.FREIGHT_ELEMENT_ID, QRY.FRTFORMULA1 FRTFORMULA, qry.chk CHK,";
            str += " qry.CURRENCY_MST_PK, qry.CURRENCY_ID, QRY.BASIS from (";

            str += " SELECT DISTINCT POL.PORT_MST_PK POLPK,POL.PORT_ID AS \"POL\",";
            str += " POD.PORT_MST_PK PODPK,POD.PORT_ID AS \"POD\", ";
            str += " FMT.FREIGHT_ELEMENT_MST_PK,FMT.FREIGHT_ELEMENT_ID,";
            str += "(CASE WHEN FMT.FREIGHT_ELEMENT_MST_PK = Q.FRT_ELEMENT_MST_FK THEN";
            str += "1";
            str += " ELSE";
            str += "0";
            str += " END) FRTFORMULA1,";
            str += " TO_CHAR('0') AS \"CHK\",CURR.CURRENCY_MST_PK,CURR.CURRENCY_ID,";
            if (!(Mode == "NEW"))
            {
                if (!IsLCL)
                {
                    str += " NULL BASIS";
                }
                else
                {
                    str += " DUMT.DIMENTION_ID BASIS,";
                    str += " CNTTRN.LCL_BASIS";
                }
            }
            else
            {
                str += " NULL BASIS";
            }

            str += " FROM FREIGHT_ELEMENT_MST_TBL FMT, PORT_MST_TBL POL, PORT_MST_TBL POD,";
            str += " CURRENCY_TYPE_MST_TBL CURR, TRADE_SUR_TBL   TST, ";
            if (Mode == "FROMRFQ")
            {
                str += " RFQ_TRN_SEA_FCL_LCL CNTTRN, DIMENTION_UNIT_MST_TBL  DUMT,";
            }
            else
            {
                str += " DIMENTION_UNIT_MST_TBL  DUMT,";
            }
            if (Mode != "NEW" & Mode != "FROMRFQ")
            {
                str += " CONT_TRN_SEA_FCL_LCL CNTTRN, ";
            }
            str += "   (SELECT TST.FRT_ELEMENT_MST_FK";
            str += "      FROM TRADE_SUR_TBL TST";
            str += "     WHERE TST.TRADE_MST_FK IN";
            str += "  (select mst.trade_mst_pk";
            str += "      from trade_mst_tbl mst, trade_mst_trn trn";
            str += " where(mst.trade_mst_pk = trn.trade_mst_fk)";
            str += "      and trn.port_mst_fk in( " + strPolPk + ")";
            str += "       and mst.trade_mst_pk in";
            str += "          (select mst.trade_mst_pk";
            str += "            from trade_mst_tbl mst, trade_mst_trn trn";
            str += " where(mst.trade_mst_pk = trn.trade_mst_fk)";
            str += "               and trn.port_mst_fk in( " + strPodPk + ")))) Q";

            str += " WHERE (1=1)";
            str += " AND FMT.FREIGHT_ELEMENT_MST_PK = TST.FRT_ELEMENT_MST_FK(+)";
            str += " AND (";
            str += strCondition + ")";
            str += strNewModeCondition;
            str += " AND CURR.CURRENCY_MST_PK = " + HttpContext.Current.Session["CURRENCY_MST_PK"];

            str += " AND FMT.CHARGE_TYPE <> 3 ";
            if (!(Mode == "NEW"))
            {
                str += " AND (CNTTRN.PORT_MST_POL_FK = POL.PORT_MST_PK AND ";
                str += " CNTTRN.PORT_MST_POD_FK = POD.PORT_MST_PK) ";
                if (string.IsNullOrEmpty(ToDt))
                {
                    str += " AND (CNTTRN.VALID_FROM >= TO_DATE('" + FromDt + "' , 'DD/MM/YYYY')) ";
                }
                else
                {
                    str += " AND (CNTTRN.VALID_FROM >= TO_DATE('" + FromDt + "' , 'DD/MM/YYYY') AND ";
                    str += " CNTTRN.VALID_TO <= TO_DATE('" + ToDt + "' , 'DD/MM/YYYY')) ";
                }
                str += " AND FMT.FREIGHT_ELEMENT_MST_PK(+) = CNTTRN.FREIGHT_ELEMENT_MST_FK ";
                if (!IsLCL)
                {
                    str += " AND DUMT.DIMENTION_UNIT_MST_PK(+) = CNTTRN.LCL_BASIS ";
                }
                else
                {
                    str += " AND DUMT.DIMENTION_UNIT_MST_PK = CNTTRN.LCL_BASIS ";
                }
                //'
                if (ContractPK != 0)
                {
                    str += "AND CNTTRN.CONT_MAIN_SEA_FK =" + ContractPK;
                }
            }
            str += " AND FMT.FREIGHT_ELEMENT_MST_PK IN(SELECT PARM.frt_bof_fk FROM PARAMETERS_TBL PARM)";
            str += "  and FMT.FREIGHT_ELEMENT_MST_PK = Q.FRT_ELEMENT_MST_FK(+)";
            str += " GROUP BY POL.PORT_MST_PK,POL.PORT_ID,POD.PORT_MST_PK,POD.PORT_ID,";
            str += " FMT.FREIGHT_ELEMENT_MST_PK,TST.FRT_ELEMENT_MST_FK,FMT.FREIGHT_ELEMENT_ID,";
            str += " CURR.CURRENCY_MST_PK,CURR.CURRENCY_ID,DUMT.DIMENTION_ID,";

            if (!(Mode == "NEW") & Mode != "FROMRFQ")
            {
                str += "  CNTTRN.LCL_BASIS,";
            }
            else if (Mode == "FROMRFQ")
            {
                str += "  CNTTRN.LCL_BASIS,";
            }
            str += "  Q.FRT_ELEMENT_MST_FK ";
            str += " HAVING POL.PORT_ID<>POD.PORT_ID";
            str += " UNION ALL";
            str += " SELECT DISTINCT POL.PORT_MST_PK POLPK,POL.PORT_ID AS \"POL\",";
            str += " POD.PORT_MST_PK PODPK,POD.PORT_ID AS \"POD\", ";
            str += " FMT.FREIGHT_ELEMENT_MST_PK,FMT.FREIGHT_ELEMENT_ID,";
            str += "(CASE WHEN FMT.FREIGHT_ELEMENT_MST_PK = Q.FRT_ELEMENT_MST_FK THEN";
            str += "1";
            str += " ELSE";
            str += "0";
            str += " END) FRTFORMULA1,";
            str += " TO_CHAR('0') AS \"CHK\",CURR.CURRENCY_MST_PK,CURR.CURRENCY_ID,";
            if (!(Mode == "NEW"))
            {
                if (!IsLCL)
                {
                    str += " NULL BASIS";
                }
                else
                {
                    str += " DUMT.DIMENTION_ID BASIS,";
                    str += " CNTTRN.LCL_BASIS ";
                }
            }
            else
            {
                str += " NULL BASIS";
            }

            str += " FROM FREIGHT_ELEMENT_MST_TBL FMT, PORT_MST_TBL POL, PORT_MST_TBL POD,";
            str += " CURRENCY_TYPE_MST_TBL CURR, TRADE_SUR_TBL   TST, ";
            if (Mode == "FROMRFQ")
            {
                str += " RFQ_TRN_SEA_FCL_LCL CNTTRN,";
            }
            else if (!(Mode == "NEW"))
            {
                str += " CONT_TRN_SEA_FCL_LCL CNTTRN, ";
            }
            str += " DIMENTION_UNIT_MST_TBL  DUMT, ";

            str += "   (SELECT TST.FRT_ELEMENT_MST_FK";
            str += "      FROM TRADE_SUR_TBL TST";
            str += "     WHERE TST.TRADE_MST_FK IN";
            str += "  (select mst.trade_mst_pk";
            str += "      from trade_mst_tbl mst, trade_mst_trn trn";
            str += " where(mst.trade_mst_pk = trn.trade_mst_fk)";
            str += "      and trn.port_mst_fk in( " + strPolPk + ")";
            str += "       and mst.trade_mst_pk in";
            str += "          (select mst.trade_mst_pk";
            str += "            from trade_mst_tbl mst, trade_mst_trn trn";
            str += " where(mst.trade_mst_pk = trn.trade_mst_fk)";
            str += "               and trn.port_mst_fk in( " + strPodPk + ")))) Q";

            str += " WHERE (1=1)";
            str += " AND FMT.FREIGHT_ELEMENT_MST_PK = TST.FRT_ELEMENT_MST_FK(+)";
            str += " AND (";
            str += strCondition + ")";
            str += strNewModeCondition;
            str += " AND CURR.CURRENCY_MST_PK = " + HttpContext.Current.Session["CURRENCY_MST_PK"];
            str += " AND FMT.CHARGE_TYPE <> 3 ";
            if (!(Mode == "NEW"))
            {
                str += " AND (CNTTRN.PORT_MST_POL_FK = POL.PORT_MST_PK AND ";
                str += " CNTTRN.PORT_MST_POD_FK = POD.PORT_MST_PK) ";
                if (string.IsNullOrEmpty(ToDt))
                {
                    str += " AND (CNTTRN.VALID_FROM >= TO_DATE('" + FromDt + "' , 'DD/MM/YYYY')) ";
                }
                else
                {
                    str += " AND (CNTTRN.VALID_FROM >= TO_DATE('" + FromDt + "' , 'DD/MM/YYYY') AND ";
                    str += " CNTTRN.VALID_TO <= TO_DATE('" + ToDt + "' , 'DD/MM/YYYY')) ";
                }
                if (Chk == 1)
                {
                    str += " AND FMT.FREIGHT_ELEMENT_MST_PK(+) = CNTTRN.FREIGHT_ELEMENT_MST_FK ";
                }
                if (!IsLCL)
                {
                    str += " AND DUMT.DIMENTION_UNIT_MST_PK(+) = CNTTRN.LCL_BASIS ";
                }
                else
                {
                    str += " AND DUMT.DIMENTION_UNIT_MST_PK = CNTTRN.LCL_BASIS ";
                }
                //'
                if (ContractPK != 0)
                {
                    str += "AND CNTTRN.CONT_MAIN_SEA_FK =" + ContractPK;
                }
            }

            str += " AND FMT.FREIGHT_ELEMENT_MST_PK NOT IN(SELECT PARM.frt_bof_fk FROM PARAMETERS_TBL PARM)";
            str += "  and FMT.FREIGHT_ELEMENT_MST_PK = Q.FRT_ELEMENT_MST_FK(+)";
            str += " GROUP BY POL.PORT_MST_PK,POL.PORT_ID,POD.PORT_MST_PK,POD.PORT_ID,";
            str += " FMT.FREIGHT_ELEMENT_MST_PK,TST.FRT_ELEMENT_MST_FK,FMT.FREIGHT_ELEMENT_ID,";
            str += " CURR.CURRENCY_MST_PK,CURR.CURRENCY_ID, DUMT.DIMENTION_ID,";
            if (!(Mode == "NEW"))
            {
                str += " CNTTRN.LCL_BASIS, ";
            }
            else if (Mode == "FROMRFQ")
            {
                str += " CNTTRN.LCL_BASIS, ";
            }
            str += "  Q.FRT_ELEMENT_MST_FK ";
            str += " HAVING POL.PORT_ID<>POD.PORT_ID";
            str += " ) qry,";
            str += " FREIGHT_ELEMENT_MST_TBL FRT";
            str += " WHERE QRY.FREIGHT_ELEMENT_MST_PK = FRT.FREIGHT_ELEMENT_MST_PK";
            if (IsLCL)
            {
                if (Mode == "FROMRFQ" & !string.IsNullOrEmpty(RFQ_REFNO))
                {
                    str += " AND QRY.LCL_BASIS IN (SELECT RTS.LCL_BASIS FROM RFQ_MAIN_SEA_TBL RM,RFQ_TRN_SEA_FCL_LCL RTS ";
                    str += " WHERE RM.RFQ_MAIN_SEA_PK = RTS.RFQ_MAIN_SEA_FK ";
                    str += " AND RM.RFQ_REF_NO='" + RFQ_REFNO + "')";
                }
            }
            str += " ORDER BY FRT.PREFERENCE";
            try
            {
                dtMain = objWF.GetDataTable(str);
                if (!IsLCL)
                {
                    dtContainerType = FetchActiveCont(strContId);
                    for (i = 0; i <= dtContainerType.Rows.Count - 1; i++)
                    {
                        dcCol = new DataColumn(Convert.ToString(dtContainerType.Rows[i][0]));
                        dtMain.Columns.Add(dcCol);
                        dcCol = new DataColumn(Convert.ToString(dtContainerType.Rows[i][1]));
                        dtMain.Columns.Add(dcCol);

                        dcCol = new DataColumn(Convert.ToString(dtContainerType.Rows[i][2]));
                        dtMain.Columns.Add(dcCol);
                    }
                }
                else
                {
                    dtMain.Columns.Add("CurrMin");
                    dtMain.Columns.Add("Curr");
                    dtMain.Columns.Add("ReqMin");
                    dtMain.Columns.Add("Req");
                    dtMain.Columns.Add("BasisPK");
                    dtMain.Columns.Add("AppMin");
                    dtMain.Columns.Add("App");
                }
                return dtMain;
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Fetch Business type"
        public DataSet FetchShipmentTerms()
        {

            string strSQL = null;
            strSQL = "SELECT M.MOVECODE_PK, M.MOVECODE_ID FROM MOVECODE_MST_TBL M WHERE M.ACTIVE_FLAG = 1 AND (M.BUSINESS_TYPE = 2 OR M.BUSINESS_TYPE = 3)";
            WorkFlow objWF = new WorkFlow();
            try
            {
                return objWF.GetDataSet(strSQL);
            }
            catch (OracleException sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }
        #endregion

        #region "To Fetch the Thc Rates"
        public DataSet FetchThcRates(string strPolPk, string strPodPk, string strContId, bool IsLCL, Int16 ChkThc = 0, string ValidFrom = "", string ValidTo = "")
        {
            string str = "";
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();
            Int16 i = default(Int16);
            Array arrPolPk = null;
            Array arrPodPk = null;
            string strCondition = "";
            strContId.Replace(" ' ", " ");
            if ((strContId != null))
            {
                strContId = strContId;
            }
            else
            {
                strContId = "0";
            }
            arrPolPk = strPolPk.Split(',');
            arrPodPk = strPodPk.Split(',');
            for (i = 0; i <= arrPolPk.Length - 1; i++)
            {
                if (string.IsNullOrEmpty(strCondition))
                {
                    strCondition = " (POL.PORT_MST_PK =" + arrPolPk.GetValue(i) + " AND POD.PORT_MST_PK =" + arrPodPk.GetValue(i) + ")";
                }
                else
                {
                    strCondition += " OR (POL.PORT_MST_PK =" + arrPolPk.GetValue(i) + " AND POD.PORT_MST_PK =" + arrPodPk.GetValue(i) + ")";
                }
            }
            str += "SELECT POL.PORT_MST_PK,";
            str += "  POL.PORT_ID AS \"POL\",";
            str += "  POD.PORT_MST_PK,";
            str += "  POD.PORT_ID AS \"POD\",";
            str += " FMT.FREIGHT_ELEMENT_MST_PK,";
            str += " FMT.FREIGHT_ELEMENT_ID,'0' FRTFORMULA,";
            str += " TO_CHAR('0') AS \"CHK\",";
            str += "CURR.CURRENCY_MST_PK,";
            str += "CURR.CURRENCY_ID,'' BASIS,";
            str += "ctm.container_type_mst_id,";
            str += "cont.Container_Rate";
            str += " FROM PORT_THC_RATES_TRN      PTRT,";
            str += " PORT_MST_TBL            POL,";
            str += " PORT_MST_TBL            POD,";
            str += " FREIGHT_ELEMENT_MST_TBL FMT,";
            str += " CURRENCY_TYPE_MST_TBL   CURR,";
            str += " PORT_THC_CONT_DTL CONT,";
            str += " CONTAINER_TYPE_MST_TBL CTM";
            str += " WHERE   PTRT.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK";
            if (ChkThc == 1)
            {
                str += "  AND PTRT.TRADE_MST_FK in";
                str += " (SELECT (TRPOL.TRADE_MST_FK)";
                str += " FROM TRADE_MST_TRN TRPOL, TRADE_MST_TRN TRPOD";
                str += " WHERE TRPOL.TRADE_MST_FK = TRPOD.TRADE_MST_FK";
                str += " AND TRPOL.PORT_MST_FK in(" + strPolPk + "))";
            }
            else
            {
                str += "  AND PTRT.TRADE_MST_FK IS NULL";
            }
            str += " AND CONT.THC_RATES_MST_FK = ptrt.THC_RATES_MST_PK";
            str += " AND POD.PORT_MST_PK in(" + strPodPk + ")";
            str += " AND (PTRT.FREIGHT_ELEMENT_MST_FK in(select f.freight_element_mst_pk from freight_element_mst_tbl f where f.freight_element_id like 'THL')";
            str += "    AND PTRT.PORT_MST_FK in(" + strPolPk + "))";
            str += "  AND PTRT.PORT_MST_FK = POL.PORT_MST_PK";
            str += " AND PTRT.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK(+)";

            str += "AND CTM.Container_Type_Mst_Pk=CONT.container_type_mst_fk";
            str += " AND PTRT.VALID_FROM <= to_date('" + ValidFrom + "','" + dateFormat + "')";
            if (!string.IsNullOrEmpty(ValidTo))
            {
                str += " AND NVL(PTRT.Valid_To, NULL_DATE_FORMAT) >=  NVL(TO_DATE('" + ValidTo + "','" + dateFormat + "'), NULL_DATE_FORMAT) ";
            }
            if (string.IsNullOrEmpty(strContId))
            {
                str += "AND ROWNUM <=10  ";
            }
            else
            {
                str += "AND ctm.container_type_mst_id in(" + strContId + ")";
            }

            str += "  UNION";

            str += "SELECT POL.PORT_MST_PK,";
            str += "  POL.PORT_ID AS \"POL\",";
            str += "  POD.PORT_MST_PK,";
            str += "  POD.PORT_ID AS \"POD\",";
            str += " FMT.FREIGHT_ELEMENT_MST_PK,'0' FRTFORMULA,";
            str += " FMT.FREIGHT_ELEMENT_ID,";
            str += " TO_CHAR('0') AS \"CHK\",";
            str += "CURR.CURRENCY_MST_PK,";
            str += "CURR.CURRENCY_ID,'' BASIS,";
            str += "ctm.container_type_mst_id,";
            str += "cont.Container_Rate";
            str += " FROM PORT_THC_RATES_TRN      PTRT,";
            str += " PORT_MST_TBL            POL,";
            str += " PORT_MST_TBL            POD,";
            str += " FREIGHT_ELEMENT_MST_TBL FMT,";
            str += " CURRENCY_TYPE_MST_TBL   CURR,";
            str += " PORT_THC_CONT_DTL CONT,";
            str += " CONTAINER_TYPE_MST_TBL CTM";
            str += "WHERE PTRT.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK";
            if (ChkThc == 1)
            {
                str += " AND PTRT.TRADE_MST_FK in";
                str += " (SELECT (TRPOL.TRADE_MST_FK)";
                str += " FROM TRADE_MST_TRN TRPOL, TRADE_MST_TRN TRPOD";
                str += " WHERE TRPOL.TRADE_MST_FK = TRPOD.TRADE_MST_FK";
                str += " AND TRPOD.PORT_MST_FK in(" + strPodPk + "))";
            }
            else
            {
                str += "  AND PTRT.TRADE_MST_FK IS NULL";
            }
            str += " AND CONT.THC_RATES_MST_FK = ptrt.THC_RATES_MST_PK";
            //Snigdharani
            str += " AND POL.PORT_MST_PK in(" + strPolPk + ")";
            str += " AND (PTRT.FREIGHT_ELEMENT_MST_FK in(select f.freight_element_mst_pk from freight_element_mst_tbl f where f.freight_element_id like 'THD')";
            str += " AND PTRT.PORT_MST_FK in(" + strPodPk + "))";
            str += "  AND PTRT.PORT_MST_FK = POD.PORT_MST_PK";
            str += " AND PTRT.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK(+)";

            str += "AND CTM.Container_Type_Mst_Pk=CONT.container_type_mst_fk";
            str += " AND PTRT.VALID_FROM <= to_date('" + ValidFrom + "','" + dateFormat + "')";
            if (!string.IsNullOrEmpty(ValidTo))
            {
                str += " AND NVL(PTRT.Valid_To, NULL_DATE_FORMAT) >=  NVL(TO_DATE('" + ValidTo + "','" + dateFormat + "'), NULL_DATE_FORMAT) ";
            }

            if (string.IsNullOrEmpty(strContId))
            {
                str += "AND ROWNUM <=10  ";
            }
            else
            {
                str += "AND ctm.container_type_mst_id in(" + strContId + ")";
            }
            try
            {
                ds = objWF.GetDataSet(str);
                return ds;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }
        #endregion

        #region "Fetch  Container/Sector"
        public DataTable ActiveSector(string strPOLPk, string strPODPk)
        {
            string strSQL = "";
            WorkFlow objWF = new WorkFlow();
            Int16 i = default(Int16);
            Array arrPolPk = null;
            Array arrPodPk = null;
            string strCondition = "";
            arrPolPk = strPOLPk.Split(',');
            arrPodPk = strPODPk.Split(',');

            for (i = 0; i <= arrPolPk.Length - 1; i++)
            {
                if (string.IsNullOrEmpty(strCondition))
                {
                    strCondition = " (POL.PORT_MST_PK =" + arrPolPk.GetValue(i) + " AND POD.PORT_MST_PK =" + arrPodPk.GetValue(i) + ")";
                }
                else
                {
                    strCondition += " OR (POL.PORT_MST_PK =" + arrPolPk.GetValue(i) + " AND POD.PORT_MST_PK =" + arrPodPk.GetValue(i) + ")";
                }
            }
            strSQL = "";
            strSQL = "SELECT POL.PORT_MST_PK ,POL.PORT_ID AS \"POL\", " + "POD.PORT_MST_PK,POD.PORT_ID,'1' CHK " + "FROM PORT_MST_TBL POL, PORT_MST_TBL POD  " + "WHERE POL.PORT_MST_PK <> POD.PORT_MST_PK  " + "AND POL.BUSINESS_TYPE = 2 " + "AND POD.BUSINESS_TYPE = 2 " + "AND ( " + strCondition + " ) " + "UNION " + "SELECT POL.PORT_MST_PK AS POLPK, POL.PORT_ID AS POL, " + "POD.PORT_MST_PK AS PODPK, POD.PORT_ID POD, " + "(CASE WHEN (" + strCondition + ") " + "THEN '1' ELSE '0' END ) CHK " + "FROM SECTOR_MST_TBL SMT, " + "PORT_MST_TBL POL, " + "PORT_MST_TBL POD " + "WHERE SMT.FROM_PORT_FK = POL.PORT_MST_PK " + "AND   POL.BUSINESS_TYPE = 2 " + "AND   POD.BUSINESS_TYPE = 2 " + "AND   SMT.TO_PORT_FK = POD.PORT_MST_PK " + "AND   SMT.ACTIVE = 1 " + "ORDER BY CHK DESC,POL";
            try
            {
                return objWF.GetDataTable(strSQL);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "ActiveContainers"
        public DataTable ActiveContainers()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            strSQL = "";
            strSQL = "SELECT " + "CMT.CONTAINER_TYPE_MST_PK, CMT.CONTAINER_TYPE_MST_ID, " + "(CASE WHEN ROWNUM<=10 THEN '1' ELSE '0' END) CHK " + "FROM CONTAINER_TYPE_MST_TBL CMT " + "WHERE CMT.ACTIVE_FLAG=1  " + "ORDER BY CHK DESC,CMT.PREFERENCES";
            try
            {
                return objWF.GetDataTable(strSQL);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "UpDate Main"
        public ArrayList UpdateContract(DataSet dsMain, long ContractPk, short intApproved, string ContractNo, bool isRFQ = false, string strPolPk = "", string strPodPk = "", string strFreightPk = "", short IntCargo = 0, string strContainerTypes = "",
        short IntActive = 1)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleCommand UpdateCmd = new OracleCommand();
            string UserName = objWK.MyUserName;
            OracleTransaction TRAN = default(OracleTransaction);
            TRAN = objWK.MyConnection.BeginTransaction();
            M_Configuration_PK = 3010;
            try
            {
                var _with1 = objWK.MyCommand;
                _with1.Connection = objWK.MyConnection;
                _with1.Transaction = TRAN;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".CONT_MAIN_SEA_TBL_PKG.CONT_MAIN_SEA_TBL_UPD";
                _with1.Parameters.Clear();
                _with1.Parameters.Add("CONT_MAIN_SEA_PK_IN", Convert.ToInt64(ContractPk)).Direction = ParameterDirection.Input;
                _with1.Parameters["CONT_MAIN_SEA_PK_IN"].SourceVersion = DataRowVersion.Current;
                _with1.Parameters.Add("OPERATOR_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["OPERATOR_MST_FK"])).Direction = ParameterDirection.Input;
                _with1.Parameters["OPERATOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with1.Parameters.Add("RFQ_MAIN_TBL_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                _with1.Parameters["RFQ_MAIN_TBL_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with1.Parameters.Add("CONTRACT_NO_IN", ContractNo).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("CONTRACT_DATE_IN", dsMain.Tables["tblMaster"].Rows[0]["CONTRACT_DATE"]).Direction = ParameterDirection.Input;
                _with1.Parameters["CONTRACT_DATE_IN"].SourceVersion = DataRowVersion.Current;
                _with1.Parameters.Add("CONT_APPROVED_IN", intApproved).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("VALID_FROM_IN", dsMain.Tables["tblMaster"].Rows[0]["VALID_FROM"]).Direction = ParameterDirection.Input;
                _with1.Parameters["VALID_FROM_IN"].SourceVersion = DataRowVersion.Current;
                _with1.Parameters.Add("VALID_TO_IN", dsMain.Tables["tblMaster"].Rows[0]["VALID_TO"]).Direction = ParameterDirection.Input;
                _with1.Parameters["VALID_TO_IN"].SourceVersion = DataRowVersion.Current;
                _with1.Parameters.Add("ACTIVE_IN", IntActive).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("CARGO_TYPE_IN", Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["CARGO_TYPE"])).Direction = ParameterDirection.Input;
                _with1.Parameters["CARGO_TYPE_IN"].SourceVersion = DataRowVersion.Current;
                if (isRFQ == true)
                {
                    _with1.Parameters.Add("COMMODITY_GROUP_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["COMMODITY_MST_FK"])).Direction = ParameterDirection.Input;
                    _with1.Parameters["COMMODITY_GROUP_FK_IN"].SourceVersion = DataRowVersion.Current;
                }
                else
                {
                    _with1.Parameters.Add("COMMODITY_GROUP_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["COMMODITY_GROUP_FK"])).Direction = ParameterDirection.Input;
                    _with1.Parameters["COMMODITY_GROUP_FK_IN"].SourceVersion = DataRowVersion.Current;
                }
                _with1.Parameters.Add("POLPK_IN", strPolPk).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("PODPK_IN", strPodPk).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("FREIGHTPK_IN", strFreightPk).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("CARGO_IN", IntCargo).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("CONTAINER_TYPE_IN", strContainerTypes).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("VERSION_NO_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["VERSION_NO"])).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("CONFIG_PK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with1.ExecuteNonQuery();
                if (string.Compare(Convert.ToString(objWK.MyCommand.Parameters["RETURN_VALUE"].Value), "contract") > 0)
                {
                    arrMessage.Add(Convert.ToString(objWK.MyCommand.Parameters["RETURN_VALUE"].Value));
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    _PkValue = Convert.ToInt64(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);
                }
                arrMessage = UpdateTransaction(dsMain, ContractPk, objWK.MyCommand);
                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                }
                else
                {
                    TRAN.Commit();
                    arrMessage.Add("All Data Saved Successfully");
                    return arrMessage;
                }
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
            finally
            {
                objWK.MyCommand.Connection.Close();
            }
            return new ArrayList();
        }
        #endregion

        #region "Update Transaction"
        public ArrayList UpdateTransaction(DataSet dsMain, long pkValue, OracleCommand UpdateCmd)
        {
            Int32 nRowCnt = default(Int32);
            string retVal = null;
            Int32 RecAfct = default(Int32);
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            string UserName = objWK.MyUserName;
            arrMessage.Clear();
            try
            {
                var _with2 = UpdateCmd;
                _with2.CommandType = CommandType.StoredProcedure;
                _with2.CommandText = objWK.MyUserName + ".CONT_MAIN_SEA_TBL_PKG.CONT_TRN_SEA_FCL_LCL_UPD";
                for (nRowCnt = 0; nRowCnt <= dsMain.Tables["tblTransaction"].Rows.Count - 1; nRowCnt++)
                {
                    UpdateCmd.Parameters.Clear();
                    _with2.Parameters.Add("CONT_MAIN_SEA_FK_IN", Convert.ToInt64(pkValue)).Direction = ParameterDirection.Input;
                    _with2.Parameters.Add("PORT_MST_POL_FK_IN", Convert.ToInt64(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["PORT_MST_POL_FK"])).Direction = ParameterDirection.Input;
                    _with2.Parameters["PORT_MST_POL_FK_IN"].SourceVersion = DataRowVersion.Current;
                    _with2.Parameters.Add("PORT_MST_POD_FK_IN", Convert.ToInt64(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["PORT_MST_POD_FK"])).Direction = ParameterDirection.Input;
                    _with2.Parameters["PORT_MST_POD_FK_IN"].SourceVersion = DataRowVersion.Current;
                    _with2.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["FREIGHT_ELEMENT_MST_FK"])).Direction = ParameterDirection.Input;
                    _with2.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                    _with2.Parameters.Add("CHECK_FOR_ALL_IN_RT_IN", Convert.ToInt32(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CHECK_FOR_ALL_IN_RT"])).Direction = ParameterDirection.Input;
                    _with2.Parameters["CHECK_FOR_ALL_IN_RT_IN"].SourceVersion = DataRowVersion.Current;
                    _with2.Parameters.Add("CURRENCY_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CURRENCY_MST_FK"])).Direction = ParameterDirection.Input;
                    _with2.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                    if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_BASIS"].ToString()))
                    {
                        _with2.Parameters.Add("LCL_BASIS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with2.Parameters.Add("LCL_BASIS_IN", Convert.ToInt64(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_BASIS"])).Direction = ParameterDirection.Input;
                    }
                    _with2.Parameters["LCL_BASIS_IN"].SourceVersion = DataRowVersion.Current;
                    if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_CURRENT_RATE"].ToString()))
                    {
                        _with2.Parameters.Add("LCL_CURRENT_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with2.Parameters.Add("LCL_CURRENT_RATE_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_CURRENT_RATE"])).Direction = ParameterDirection.Input;
                    }
                    _with2.Parameters["LCL_CURRENT_RATE_IN"].SourceVersion = DataRowVersion.Current;
                    if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_REQUEST_RATE"].ToString()))
                    {
                        _with2.Parameters.Add("LCL_REQUEST_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with2.Parameters.Add("LCL_REQUEST_RATE_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_REQUEST_RATE"])).Direction = ParameterDirection.Input;
                    }
                    _with2.Parameters["LCL_REQUEST_RATE_IN"].SourceVersion = DataRowVersion.Current;
                    if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_APPROVED_RATE"].ToString()))
                    {
                        _with2.Parameters.Add("LCL_APPROVED_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with2.Parameters.Add("LCL_APPROVED_RATE_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_APPROVED_RATE"])).Direction = ParameterDirection.Input;
                    }
                    _with2.Parameters["LCL_APPROVED_RATE_IN"].SourceVersion = DataRowVersion.Current;
                    if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CONTAINER_DTL_FCL"].ToString()))
                    {
                        _with2.Parameters.Add("CONTAINER_DTL_FCL_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with2.Parameters.Add("CONTAINER_DTL_FCL_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CONTAINER_DTL_FCL"]).Direction = ParameterDirection.Input;
                    }
                    _with2.Parameters["CONTAINER_DTL_FCL_IN"].SourceVersion = DataRowVersion.Current;
                    _with2.Parameters.Add("VALID_FROM_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["VALID_FROM"]).Direction = ParameterDirection.Input;
                    if (!string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["VALID_TO"].ToString()))
                    {
                        _with2.Parameters.Add("VALID_TO_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["VALID_TO"]).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with2.Parameters.Add("VALID_TO_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    _with2.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with2.ExecuteNonQuery();
                }
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
            finally
            {
                objWK.MyCommand.Connection.Close();
            }

        }
        #endregion

        #region "FetchVoucher"
        public DataSet fetchDocumentWorkFlow(long pk)
        {
            string strSQL = null;
            strSQL = "SELECT COUNT(*)";
            strSQL += " From DOCUMENT_PREF_LOC_MST_TBL D, DOCUMENT_PREFERENCE_MST_TBL DP ";
            strSQL += "WHERE " ;
            strSQL += " D.LOCATION_MST_FK = " + pk + " " ;
            strSQL += "AND D.DOC_PREFERENCE_FK = DP.DOCUMENT_PREFERENCE_MST_PK" ;
            strSQL += "AND  DP.DOCUMENT_PREFERENCE_NAME='SL Contract'";

            WorkFlow objWF = new WorkFlow();
            DataSet objDS = null;
            try
            {
                objDS = objWF.GetDataSet(strSQL);
                return objDS;
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "SaveContract Main"
        public ArrayList SaveContract(DataSet dsMain, object txtContractNo, long nLocationId, long nEmpId, string strLclBasis = "", string strContrID = "", Int16 Restricted = 0, string sid = "", string polid = "", string podid = "")
        {

            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = default(OracleTransaction);
            string ContractNo = null;
            bool IsUpdate = false;
            M_Configuration_PK = 3010;
            cls_RFQ_Others objRFQ = new cls_RFQ_Others();
            long ContPK = 0;

            if (!string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["CONT_MAIN_SEA_PK"].ToString()))
            {
                if (Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["CONT_MAIN_SEA_PK"]) == 0)
                {
                    ContPK = 0;
                }
                else
                {
                    ContPK = Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["CONT_MAIN_SEA_PK"].ToString());
                }
            }
            else
            {
                ContPK = 0;
            }

            try
            {
                objWK.OpenConnection();
                TRAN = objWK.MyConnection.BeginTransaction();
                objWK.MyCommand.Transaction = TRAN;

                if (ContPK == 0)
                {
                    if (string.IsNullOrEmpty(txtContractNo.ToString()))
                    {
                        ContractNo = GenerateContractNo(nLocationId, nEmpId, M_CREATED_BY_FK, objWK, sid, polid, podid);
                        if (ContractNo == "Protocol Not Defined.")
                        {
                            arrMessage.Add("Protocol Not Defined.");
                            return arrMessage;
                        }
                    }
                    else
                    {
                        ContractNo = txtContractNo.ToString();
                    }
                    var _with3 = objWK.MyCommand;
                    _with3.Connection = objWK.MyConnection;
                    _with3.CommandType = CommandType.StoredProcedure;
                    _with3.CommandText = objWK.MyUserName + ".CONT_MAIN_SEA_TBL_PKG.CONT_MAIN_SEA_TBL_INS";
                    _with3.Parameters.Clear();

                    _with3.Parameters.Add("OPERATOR_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["OPERATOR_MST_FK"])).Direction = ParameterDirection.Input;

                    if (!string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["RFQ_MAIN_TBL_FK"].ToString()))
                    {
                        _with3.Parameters.Add("RFQ_MAIN_TBL_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["RFQ_MAIN_TBL_FK"])).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with3.Parameters.Add("RFQ_MAIN_TBL_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }

                    _with3.Parameters.Add("CONTRACT_NO_IN", ContractNo).Direction = ParameterDirection.Input;

                    _with3.Parameters.Add("CONTRACT_DATE_IN", dsMain.Tables["tblMaster"].Rows[0]["CONTRACT_DATE"]).Direction = ParameterDirection.Input;

                    _with3.Parameters.Add("CONT_APPROVED_IN", Convert.ToInt32(dsMain.Tables[0].Rows[0]["CONT_APPROVED"])).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("TRADE_CHK_IN", Convert.ToInt32(dsMain.Tables[0].Rows[0]["TRADE_CHK"])).Direction = ParameterDirection.Input;

                    _with3.Parameters.Add("TRADETHC_CHK_IN", Convert.ToInt32(dsMain.Tables[0].Rows[0]["TRADETHC_CHK"])).Direction = ParameterDirection.Input;

                    _with3.Parameters.Add("WORKFLOW_STATUS_IN", Convert.ToInt32(dsMain.Tables[0].Rows[0]["WORKFLOW_STATUS"])).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("APPROVEDT_IN", (dsMain.Tables[0].Rows[0]["APPROVEDT"] == "0" ? DBNull.Value : (dsMain.Tables[0].Rows[0]["APPROVEDT"]))).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("APPROVEBY_IN", (dsMain.Tables[0].Rows[0]["APPROVEBY"] == "0" ? DBNull.Value : (dsMain.Tables[0].Rows[0]["APPROVEBY"]))).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("RESTRICTED_IN", Restricted).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("ACTIVE_IN", Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["ACTIVE"])).Direction = ParameterDirection.Input;

                    _with3.Parameters.Add("VALID_FROM_IN", dsMain.Tables["tblMaster"].Rows[0]["VALID_FROM"]).Direction = ParameterDirection.Input;


                    _with3.Parameters.Add("VALID_TO_IN", dsMain.Tables["tblMaster"].Rows[0]["VALID_TO"]).Direction = ParameterDirection.Input;

                    _with3.Parameters.Add("CARGO_TYPE_IN", Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["CARGO_TYPE"])).Direction = ParameterDirection.Input;

                    _with3.Parameters.Add("COMMODITY_GROUP_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["COMMODITY_GROUP_FK"])).Direction = ParameterDirection.Input;

                    _with3.Parameters.Add("POLPK_IN", dsMain.Tables["tblMaster"].Rows[0]["POL_PK"]).Direction = ParameterDirection.Input;

                    _with3.Parameters.Add("PODPK_IN", dsMain.Tables["tblMaster"].Rows[0]["POD_PK"]).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("LCL_BASIS_IN", getDefault(strLclBasis, 0)).Direction = ParameterDirection.Input;

                    _with3.Parameters.Add("CONTAINER_TYPE_IN", dsMain.Tables["tblMaster"].Rows[0]["CONTAINER_ID"]).Direction = ParameterDirection.Input;

                    _with3.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("CONFIG_PK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("BASE_CURRENCY_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["BASE_CURRENCY_FK"])).Direction = ParameterDirection.Input;

                    _with3.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with3.ExecuteNonQuery();
                }
                else
                {
                    IsUpdate = true;
                    var _with4 = objWK.MyCommand;
                    _with4.Connection = objWK.MyConnection;
                    _with4.CommandType = CommandType.StoredProcedure;
                    _with4.CommandText = objWK.MyUserName + ".CONT_MAIN_SEA_TBL_PKG.CONT_MAIN_SEA_TBL_UPD";
                    _with4.Parameters.Clear();

                    _with4.Parameters.Add("CONT_MAIN_SEA_PK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["CONT_MAIN_SEA_PK"])).Direction = ParameterDirection.Input;

                    _with4.Parameters.Add("OPERATOR_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["OPERATOR_MST_FK"])).Direction = ParameterDirection.Input;

                    _with4.Parameters.Add("CONT_APPROVED_IN", Convert.ToInt32(dsMain.Tables[0].Rows[0]["CONT_APPROVED"])).Direction = ParameterDirection.Input;

                    _with4.Parameters.Add("TRADE_CHK_IN", Convert.ToInt32(dsMain.Tables[0].Rows[0]["TRADE_CHK"])).Direction = ParameterDirection.Input;

                    _with4.Parameters.Add("TRADETHC_CHK_IN", Convert.ToInt32(dsMain.Tables[0].Rows[0]["TRADETHC_CHK"])).Direction = ParameterDirection.Input;

                    _with4.Parameters.Add("WORKFLOW_STATUS_IN", Convert.ToInt32(dsMain.Tables[0].Rows[0]["WORKFLOW_STATUS"])).Direction = ParameterDirection.Input;
                    _with4.Parameters.Add("APPROVEDT_IN", (dsMain.Tables[0].Rows[0]["APPROVEDT"] == "0" ? DBNull.Value : (dsMain.Tables[0].Rows[0]["APPROVEDT"]))).Direction = ParameterDirection.Input;
                    _with4.Parameters.Add("APPROVEBY_IN", (dsMain.Tables[0].Rows[0]["APPROVEBY"] == "0" ? DBNull.Value : (dsMain.Tables[0].Rows[0]["APPROVEBY"]))).Direction = ParameterDirection.Input;
                    _with4.Parameters.Add("RESTRICTED_IN", Restricted).Direction = ParameterDirection.Input;
                    _with4.Parameters.Add("ACTIVE_IN", Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["ACTIVE"])).Direction = ParameterDirection.Input;

                    _with4.Parameters.Add("VALID_FROM_IN", dsMain.Tables["tblMaster"].Rows[0]["VALID_FROM"]).Direction = ParameterDirection.Input;


                    _with4.Parameters.Add("VALID_TO_IN", dsMain.Tables["tblMaster"].Rows[0]["VALID_TO"]).Direction = ParameterDirection.Input;

                    _with4.Parameters.Add("CARGO_TYPE_IN", Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["CARGO_TYPE"])).Direction = ParameterDirection.Input;

                    _with4.Parameters.Add("COMMODITY_GROUP_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["COMMODITY_GROUP_FK"])).Direction = ParameterDirection.Input;

                    _with4.Parameters.Add("POLPK_IN", dsMain.Tables["tblMaster"].Rows[0]["POL_PK"]).Direction = ParameterDirection.Input;

                    _with4.Parameters.Add("PODPK_IN", dsMain.Tables["tblMaster"].Rows[0]["POD_PK"]).Direction = ParameterDirection.Input;

                    _with4.Parameters.Add("LCL_BASIS_IN", getDefault(strLclBasis, 0)).Direction = ParameterDirection.Input;

                    _with4.Parameters.Add("CONTAINER_TYPE_IN", dsMain.Tables["tblMaster"].Rows[0]["CONTAINER_ID"]).Direction = ParameterDirection.Input;

                    _with4.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["VERSION_NO"].ToString()))
                    {
                        _with4.Parameters.Add("VERSION_NO_IN", 0).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with4.Parameters.Add("VERSION_NO_IN", Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["VERSION_NO"])).Direction = ParameterDirection.Input;
                    }

                    _with4.Parameters.Add("CONFIG_PK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                    _with4.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 500, "RETURN_VALUE").Direction = ParameterDirection.Output;

                    _with4.ExecuteNonQuery();
                }

                string returnValue = Convert.ToString(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);
                if (string.Compare(returnValue, "contract")>0)
                {
                    arrMessage.Add(Convert.ToString(objWK.MyCommand.Parameters["RETURN_VALUE"].Value));
                    //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                    if (!IsUpdate)
                    {
                        RollbackProtocolKey("OPERATOR CONTRACT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), ContractNo, System.DateTime.Now);
                    }
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    _PkValue = Convert.ToInt64(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);
                }

                arrMessage = SaveContractTran(dsMain, _PkValue, objWK, IsUpdate, strContrID);
                //'
                string CurrFKs = "0";
                System.DateTime ContractDt = default(System.DateTime);
                ContractDt = Convert.ToDateTime(dsMain.Tables["tblMaster"].Rows[0]["CONTRACT_DATE"]);
                for (int nRowCnt = 0; nRowCnt <= dsMain.Tables["tblTransaction"].Rows.Count - 1; nRowCnt++)
                {
                    if (dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CHECK_FOR_ALL_IN_RT"] == "1")
                    {
                        CurrFKs += "," + dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CURRENCY_MST_FK"];
                    }
                }
                UpdateTempExRate(_PkValue, objWK, CurrFKs, ContractDt, "SLCONTRACT");
                if (arrMessage.Count > 0)
                {
                    if (string.Compare(arrMessage[0].ToString(), "Saved")>0)
                    {
                        arrMessage.Clear();
                        arrMessage.Add("All data saved successfully");
                        TRAN.Commit();
                        txtContractNo = ContractNo;
                        return arrMessage;
                    }
                    else
                    {
                        if (!IsUpdate)
                        {
                            RollbackProtocolKey("OPERATOR CONTRACT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), ContractNo, System.DateTime.Now);
                            //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                        }
                        TRAN.Rollback();
                        return arrMessage;
                    }
                }
            }
            catch (OracleException oraexp)
            {
                if (!IsUpdate)
                {
                    RollbackProtocolKey("OPERATOR CONTRACT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), ContractNo, System.DateTime.Now);
                    //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                }
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                if (!IsUpdate)
                {
                    RollbackProtocolKey("OPERATOR CONTRACT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), ContractNo, System.DateTime.Now);
                    //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
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

        #region "Update Temp ExchangeRate"
        public ArrayList UpdateTempExRate(long PkValue, WorkFlow objWK, string CurrFks, System.DateTime FromDt, string FromFlag = "")
        {
            try
            {
                arrMessage.Clear();
                var _with5 = objWK.MyCommand;
                _with5.CommandType = CommandType.StoredProcedure;
                _with5.CommandText = objWK.MyUserName + ".CONT_MAIN_SEA_TBL_PKG.TEMP_EX_RATE_TRN_UPD";
                _with5.Parameters.Clear();
                _with5.Parameters.Add("REF_FK_IN", PkValue).Direction = ParameterDirection.Input;
                _with5.Parameters["REF_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with5.Parameters.Add("BASE_CURRENCY_FK_IN", HttpContext.Current.Session["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
                _with5.Parameters["BASE_CURRENCY_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with5.Parameters.Add("CURRENCY_FKS_IN", CurrFks).Direction = ParameterDirection.Input;
                _with5.Parameters["CURRENCY_FKS_IN"].SourceVersion = DataRowVersion.Current;
                _with5.Parameters.Add("FROM_DATE_IN", FromDt).Direction = ParameterDirection.Input;
                _with5.Parameters["FROM_DATE_IN"].SourceVersion = DataRowVersion.Current;
                _with5.Parameters.Add("FROM_FLAG_IN", FromFlag).Direction = ParameterDirection.Input;
                _with5.Parameters["FROM_FLAG_IN"].SourceVersion = DataRowVersion.Current;
                _with5.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with5.ExecuteNonQuery();
                arrMessage.Add("All Data Saved Successfully");
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

        #region "Deactivate"
        public ArrayList Deactivate(long ContractPk)
        {
            WorkFlow objWK = new WorkFlow();
            string strSQL = null;
            OracleTransaction TRAN = default(OracleTransaction);
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            arrMessage.Clear();
            objWK.MyCommand.Transaction = TRAN;

            strSQL = "UPDATE CONT_MAIN_SEA_TBL T " + "SET T.ACTIVE = 0," + "T.LAST_MODIFIED_BY_FK =" + M_CREATED_BY_FK + "," + "T.LAST_MODIFIED_DT = SYSDATE," + "T.VERSION_NO = T.VERSION_NO + 1" + "WHERE T.CONT_MAIN_SEA_PK =" + ContractPk;
            objWK.MyCommand.CommandType = CommandType.Text;
            objWK.MyCommand.CommandText = strSQL;
            try
            {
                objWK.MyCommand.ExecuteScalar();
                _PkValue = ContractPk;
                arrMessage.Add("All data saved successfully");
                TRAN.Commit();
                return arrMessage;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                TRAN.Rollback();
                return arrMessage;
            }
            finally
            {
                objWK.MyConnection.Close();
            }
        }
        #endregion

        #region "Save Child"
        public ArrayList SaveContractTran(DataSet dsMain, long PkValue, WorkFlow objWK, bool IsUpdate, string strContrID = "")
        {

            Int32 nRowCnt = default(Int32);
            string retVal = null;
            Int32 RecAfct = default(Int32);
            arrMessage.Clear();


            try
            {
                if (!IsUpdate)
                {
                    var _with6 = objWK.MyCommand;
                    _with6.CommandType = CommandType.StoredProcedure;
                    _with6.CommandText = objWK.MyUserName + ".CONT_MAIN_SEA_TBL_PKG.CONT_TRN_SEA_FCL_LCL_INS";
                    for (nRowCnt = 0; nRowCnt <= dsMain.Tables["tblTransaction"].Rows.Count - 1; nRowCnt++)
                    {
                        //'Added by Mayur PTS Task, for Saving only Selected Records and Not all records for LCL Cargo Type
                        if (dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CHECK_FOR_ALL_IN_RT"] == "1")
                        {
                            _with6.Parameters.Clear();
                            _with6.Parameters.Add("CONT_MAIN_SEA_FK_IN", Convert.ToInt64(PkValue)).Direction = ParameterDirection.Input;

                            _with6.Parameters.Add("PORT_MST_POL_FK_IN", Convert.ToInt64(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["PORT_MST_POL_FK"])).Direction = ParameterDirection.Input;

                            _with6.Parameters.Add("PORT_MST_POD_FK_IN", Convert.ToInt64(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["PORT_MST_POD_FK"])).Direction = ParameterDirection.Input;

                            _with6.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["FREIGHT_ELEMENT_MST_FK"])).Direction = ParameterDirection.Input;

                            _with6.Parameters.Add("CHECK_FOR_ALL_IN_RT_IN", Convert.ToInt32(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CHECK_FOR_ALL_IN_RT"])).Direction = ParameterDirection.Input;

                            _with6.Parameters.Add("CURRENCY_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CURRENCY_MST_FK"])).Direction = ParameterDirection.Input;

                            if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_BASIS"].ToString()))
                            {
                                _with6.Parameters.Add("LCL_BASIS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with6.Parameters.Add("LCL_BASIS_IN", Convert.ToInt64(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_BASIS"])).Direction = ParameterDirection.Input;
                            }

                            if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["SURCHARGE"].ToString()))
                            {
                                _with6.Parameters.Add("SURCHARGE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with6.Parameters.Add("SURCHARGE_IN", (dsMain.Tables["tblTransaction"].Rows[nRowCnt]["SURCHARGE"])).Direction = ParameterDirection.Input;
                            }

                            if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_CURRENT_RATE"].ToString()))
                            {
                                _with6.Parameters.Add("LCL_CURRENT_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with6.Parameters.Add("LCL_CURRENT_RATE_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_CURRENT_RATE"])).Direction = ParameterDirection.Input;
                            }

                            if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_REQUEST_RATE"].ToString()))
                            {
                                _with6.Parameters.Add("LCL_REQUEST_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with6.Parameters.Add("LCL_REQUEST_RATE_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_REQUEST_RATE"])).Direction = ParameterDirection.Input;
                            }


                            if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_APPROVED_RATE"].ToString()))
                            {
                                _with6.Parameters.Add("LCL_APPROVED_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with6.Parameters.Add("LCL_APPROVED_RATE_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_APPROVED_RATE"])).Direction = ParameterDirection.Input;
                            }


                            if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CONTAINER_DTL_FCL"].ToString()))
                            {
                                _with6.Parameters.Add("CONTAINER_DTL_FCL_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with6.Parameters.Add("CONTAINER_DTL_FCL_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CONTAINER_DTL_FCL"]).Direction = ParameterDirection.Input;
                            }
                            _with6.Parameters.Add("VALID_FROM_IN", Convert.ToString(Convert.ToDateTime(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["VALID_FROM"]))).Direction = ParameterDirection.Input;

                            if (!string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["VALID_TO"].ToString()))
                            {
                                _with6.Parameters.Add("VALID_TO_IN", Convert.ToString(Convert.ToDateTime(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["VALID_TO"]))).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with6.Parameters.Add("VALID_TO_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }

                            _with6.Parameters.Add("CONTAINER_TYPE_IN", dsMain.Tables["tblMaster"].Rows[0]["CONTAINER_ID"]).Direction = ParameterDirection.Input;



                            if (!string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_CURRENT_MIN_RATE"].ToString()))
                            {
                                _with6.Parameters.Add("LCL_CURRENT_MIN_RATE_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_CURRENT_MIN_RATE"])).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with6.Parameters.Add("LCL_CURRENT_MIN_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }


                            if (!string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_REQUEST_MIN_RATE"].ToString()))
                            {
                                _with6.Parameters.Add("LCL_REQUEST_MIN_RATE_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_REQUEST_MIN_RATE"])).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with6.Parameters.Add("LCL_REQUEST_MIN_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }


                            if (!string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_APPROVED_MIN_RATE"].ToString()))
                            {
                                _with6.Parameters.Add("LCL_APPROVED_MIN_RATE_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_APPROVED_MIN_RATE"])).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with6.Parameters.Add("LCL_APPROVED_MIN_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }
                            /// Added By Sushama

                            if (!string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["SHIP_TERM_MST_PK"].ToString()))
                            {
                                _with6.Parameters.Add("SHIP_TERM_MST_PK_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["SHIP_TERM_MST_PK"])).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with6.Parameters.Add("SHIP_TERM_MST_PK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }
                            /// END
                            _with6.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                            _with6.ExecuteNonQuery();

                            if (int.Parse(_with6.Parameters["RETURN_VALUE"].Value.ToString()) > 0)
                            {
                                arrMessage.Add(_with6.Parameters["RETURN_VALUE"].Value);
                                return arrMessage;
                            }
                        }
                    }
                }
                else
                {
                    var _with7 = objWK.MyCommand;
                    _with7.CommandType = CommandType.StoredProcedure;
                    _with7.CommandText = objWK.MyUserName + ".CONT_MAIN_SEA_TBL_PKG.CONT_TRN_SEA_FCL_LCL_UPD";
                    for (nRowCnt = 0; nRowCnt <= dsMain.Tables["tblTransaction"].Rows.Count - 1; nRowCnt++)
                    {
                        //'Added by Mayur PTS Task, for Saving only Selected Records and Not all records for LCL Cargo Type
                        if (dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CHECK_FOR_ALL_IN_RT"] == "1")
                        {
                            _with7.Parameters.Clear();
                            _with7.Parameters.Add("CONT_MAIN_SEA_FK_IN", Convert.ToInt64(PkValue)).Direction = ParameterDirection.Input;

                            _with7.Parameters.Add("PORT_MST_POL_FK_IN", Convert.ToInt64(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["PORT_MST_POL_FK"])).Direction = ParameterDirection.Input;

                            _with7.Parameters.Add("PORT_MST_POD_FK_IN", Convert.ToInt64(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["PORT_MST_POD_FK"])).Direction = ParameterDirection.Input;

                            _with7.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["FREIGHT_ELEMENT_MST_FK"])).Direction = ParameterDirection.Input;

                            _with7.Parameters.Add("CHECK_FOR_ALL_IN_RT_IN", Convert.ToInt32(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CHECK_FOR_ALL_IN_RT"])).Direction = ParameterDirection.Input;

                            _with7.Parameters.Add("CURRENCY_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CURRENCY_MST_FK"])).Direction = ParameterDirection.Input;

                            if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_BASIS"].ToString()))
                            {
                                _with7.Parameters.Add("LCL_BASIS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with7.Parameters.Add("LCL_BASIS_IN", Convert.ToInt64(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_BASIS"])).Direction = ParameterDirection.Input;
                            }

                            if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["SURCHARGE"].ToString()))
                            {
                                _with7.Parameters.Add("SURCHARGE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with7.Parameters.Add("SURCHARGE_IN", (dsMain.Tables["tblTransaction"].Rows[nRowCnt]["SURCHARGE"])).Direction = ParameterDirection.Input;
                            }
                            if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_CURRENT_RATE"].ToString()))
                            {
                                _with7.Parameters.Add("LCL_CURRENT_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with7.Parameters.Add("LCL_CURRENT_RATE_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_CURRENT_RATE"])).Direction = ParameterDirection.Input;
                            }

                            if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_REQUEST_RATE"].ToString()))
                            {
                                _with7.Parameters.Add("LCL_REQUEST_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with7.Parameters.Add("LCL_REQUEST_RATE_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_REQUEST_RATE"])).Direction = ParameterDirection.Input;
                            }


                            if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_APPROVED_RATE"].ToString()))
                            {
                                _with7.Parameters.Add("LCL_APPROVED_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with7.Parameters.Add("LCL_APPROVED_RATE_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_APPROVED_RATE"])).Direction = ParameterDirection.Input;
                            }


                            if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CONTAINER_DTL_FCL"].ToString()))
                            {
                                _with7.Parameters.Add("CONTAINER_DTL_FCL_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with7.Parameters.Add("CONTAINER_DTL_FCL_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CONTAINER_DTL_FCL"]).Direction = ParameterDirection.Input;
                            }

                            _with7.Parameters.Add("VALID_FROM_IN", Convert.ToString(Convert.ToDateTime(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["VALID_FROM"]))).Direction = ParameterDirection.Input;

                            if (!string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["VALID_TO"].ToString()))
                            {
                                _with7.Parameters.Add("VALID_TO_IN", Convert.ToString(Convert.ToDateTime(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["VALID_TO"]))).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with7.Parameters.Add("VALID_TO_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }
                            _with7.Parameters.Add("CONTAINER_TYPE_IN", dsMain.Tables["tblMaster"].Rows[0]["CONTAINER_ID"]).Direction = ParameterDirection.Input;


                            if (!string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_CURRENT_MIN_RATE"].ToString()))
                            {
                                _with7.Parameters.Add("LCL_CURRENT_MIN_RATE_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_CURRENT_MIN_RATE"])).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with7.Parameters.Add("LCL_CURRENT_MIN_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }


                            if (!string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_REQUEST_MIN_RATE"].ToString()))
                            {
                                _with7.Parameters.Add("LCL_REQUEST_MIN_RATE_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_REQUEST_MIN_RATE"])).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with7.Parameters.Add("LCL_REQUEST_MIN_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }


                            if (!string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_APPROVED_MIN_RATE"].ToString()))
                            {
                                _with7.Parameters.Add("LCL_APPROVED_MIN_RATE_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_APPROVED_MIN_RATE"])).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with7.Parameters.Add("LCL_APPROVED_MIN_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }
                            /// Added By Sushama

                            if (!string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["SHIP_TERM_MST_PK"].ToString()))
                            {
                                _with7.Parameters.Add("SHIP_TERM_MST_PK_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["SHIP_TERM_MST_PK"])).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with7.Parameters.Add("SHIP_TERM_MST_PK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }
                            /// END
                            _with7.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                            _with7.ExecuteNonQuery();
                        }
                    }
                }
                arrMessage.Add("All Data Saved Successfully");
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

        #region "Generate Protocol Value- Contract No"

        public string GenerateContractNo(long nLocationId, long nEmployeeId, long nCreatedBy, WorkFlow objwk, string SLID = "", string POLID = "", string PODID = "")
        {
            string functionReturnValue = null;

            try
            {
                functionReturnValue = GenerateProtocolKey("OPERATOR CONTRACT", nLocationId, nEmployeeId, DateTime.Now, "","" , POLID, nCreatedBy, objwk, SLID,
                PODID);
                return functionReturnValue;
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return functionReturnValue;
        }
        #endregion

        #region "fetch from  Previous Contract"
        public DataSet fetchPreviousContract(long nOprPk, string strPOL, string strPOD, Int16 CargoType, Int16 CommodityPk, string ValidFrom, string ValidTo)
        {
            try
            {
                System.Text.StringBuilder buildQuery = new System.Text.StringBuilder();
                string strSQL = null;
                buildQuery.Append("    Select ");
                buildQuery.Append("    OPR.OPERATOR_MST_PK,OPR.OPERATOR_ID,OPR.OPERATOR_NAME, ");
                buildQuery.Append("    CONTTRN.PORT_MST_POL_FK,CONTTRN.PORT_MST_POD_FK, ");
                buildQuery.Append("    CONTTRN.CHECK_FOR_ALL_IN_RT, ");
                buildQuery.Append("    CONTTRN.FREIGHT_ELEMENT_MST_FK,CONTTRN.CURRENCY_MST_FK,CURR.CURRENCY_ID, ");
                buildQuery.Append("   '" + ValidFrom + "' AS P_VALID_FROM,");
                buildQuery.Append("    '" + ValidTo + "'  AS P_VALID_TO,");
                buildQuery.Append("    CONTTRN.LCL_BASIS,CONTTRN.LCL_CURRENT_RATE, ");
                buildQuery.Append("    CONTTRN.LCL_REQUEST_RATE,LCL_APPROVED_RATE,DMT.DIMENTION_ID  ");
                buildQuery.Append("    FROM OPERATOR_MST_TBL OPR,");
                buildQuery.Append("    CONT_TRN_SEA_FCL_LCL CONTTRN,CONT_MAIN_SEA_TBL CONTHDR, ");
                buildQuery.Append("    CURRENCY_TYPE_MST_TBL CURR,DIMENTION_UNIT_MST_TBL DMT");
                buildQuery.Append("    WHERE CONTHDR.CONT_MAIN_SEA_PK = CONTTRN.CONT_MAIN_SEA_FK  ");
                buildQuery.Append("    AND OPR.OPERATOR_MST_PK = CONTHDR.OPERATOR_MST_FK ");
                buildQuery.Append("    AND CURR.CURRENCY_MST_PK = CONTTRN.CURRENCY_MST_FK ");
                buildQuery.Append("    AND DMT.DIMENTION_UNIT_MST_PK = CONTTRN.LCL_BASIS ");
                buildQuery.Append("    AND CONTTRN.PORT_MST_POL_FK in (" + strPOL + ")");
                buildQuery.Append("    AND CONTTRN.PORT_MST_POD_FK in (" + strPOD + ")");
                buildQuery.Append("    AND CONTHDR.CARGO_TYPE = " + CargoType + " ");
                buildQuery.Append("    AND CONTHDR.COMMODITY_GROUP_FK = " + CommodityPk + "");
                buildQuery.Append("    AND CONTHDR.CONT_APPROVED=1");
                buildQuery.Append("    AND CONTHDR.OPERATOR_MST_FK = " + nOprPk + " ");
                buildQuery.Append("    AND CONTHDR.ACTIVE=1");
                buildQuery.Append("    AND CONTHDR.VALID_TO = (SELECT MAX(CM.VALID_TO) FROM CONT_MAIN_SEA_TBL CM,");
                buildQuery.Append("    CONT_TRN_SEA_FCL_LCL CT ");
                buildQuery.Append("    WHERE CM.OPERATOR_MST_FK = " + nOprPk + "  ");
                buildQuery.Append("    AND CM.COMMODITY_GROUP_FK = " + CommodityPk + "");
                buildQuery.Append("    AND CM.CONT_APPROVED=1");
                buildQuery.Append("    AND CM.ACTIVE=1");
                buildQuery.Append("    AND CM.CARGO_TYPE = " + CargoType + "");
                buildQuery.Append("    AND CT.PORT_MST_POL_FK in (" + strPOL + ")");
                buildQuery.Append("    AND CT.PORT_MST_POD_FK in (" + strPOD + ")");
                buildQuery.Append("    AND CM.CONT_MAIN_SEA_PK = CT.CONT_MAIN_SEA_FK");
                buildQuery.Append(" )");
                return (new WorkFlow()).GetDataSet(buildQuery.ToString());
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "fetchPreviousContractFCL"
        public DataSet fetchPreviousContractFCL(long nOprPk, string strPOL, string strPOD, Int16 CargoType, Int16 CommodityPk, string ValidFrom, string ValidTo)
        {
            try
            {
                System.Text.StringBuilder buildQuery = new System.Text.StringBuilder();
                string strSQL = null;
                buildQuery.Append("    Select ");
                buildQuery.Append("  OPR.OPERATOR_MST_PK,OPR.OPERATOR_ID,OPR.OPERATOR_NAME,  ");
                buildQuery.Append("  CONTHDR.CONTRACT_NO,CONTHDR.CONTRACT_DATE,  ");
                buildQuery.Append("  CONTHDR.CARGO_TYPE, CONTHDR.COMMODITY_GROUP_FK,CONTHDR.RFQ_MAIN_TBL_FK,CONTHDR.VALID_FROM,  ");
                buildQuery.Append("  CONTHDR.VALID_TO,CONTHDR.ACTIVE,CONTHDR.CONT_APPROVED,CONTHDR.VERSION_NO,  ");
                buildQuery.Append("  CONTTRN.PORT_MST_POL_FK,CONTTRN.PORT_MST_POD_FK,CONTTRN.CHECK_FOR_ALL_IN_RT, ");
                buildQuery.Append("  CONTTRN.FREIGHT_ELEMENT_MST_FK,CONTTRN.CURRENCY_MST_FK,CURR.CURRENCY_ID,  ");
                buildQuery.Append("  '" + ValidFrom + "' AS P_VALID_FROM, ");
                buildQuery.Append("  '" + ValidTo + "' AS P_VALID_TO, ");
                buildQuery.Append("  CMT.CONTAINER_TYPE_MST_ID,CONT.FCL_CURRENT_RATE,CONT.FCL_REQ_RATE,CONT.FCL_APP_RATE ");
                buildQuery.Append("  FROM OPERATOR_MST_TBL OPR,");
                buildQuery.Append("  CONT_TRN_SEA_FCL_LCL CONTTRN,");
                buildQuery.Append("  CONT_TRN_SEA_FCL_RATES CONT,   ");
                buildQuery.Append("  CONTAINER_TYPE_MST_TBL CMT,  ");
                buildQuery.Append("  CONT_MAIN_SEA_TBL CONTHDR,CURRENCY_TYPE_MST_TBL CURR    ");
                buildQuery.Append("  WHERE CONTHDR.CONT_MAIN_SEA_PK = CONTTRN.CONT_MAIN_SEA_FK ");
                buildQuery.Append("  AND OPR.OPERATOR_MST_PK = CONTHDR.OPERATOR_MST_FK ");
                buildQuery.Append("  AND CMT.CONTAINER_TYPE_MST_PK = CONT.CONTAINER_TYPE_MST_FK ");
                buildQuery.Append("  AND CONTTRN.CONT_TRN_SEA_PK = CONT.CONT_TRN_SEA_FK ");
                buildQuery.Append("  AND CURR.CURRENCY_MST_PK = CONTTRN.CURRENCY_MST_FK ");
                buildQuery.Append("  AND CONTTRN.PORT_MST_POL_FK in (" + strPOL + ")");
                buildQuery.Append("  AND CONTTRN.PORT_MST_POD_FK in (" + strPOD + ")");
                buildQuery.Append("  AND CONTHDR.CARGO_TYPE = " + CargoType + " ");
                buildQuery.Append("  AND CONTHDR.COMMODITY_GROUP_FK = " + CommodityPk + "");
                buildQuery.Append("  AND CONTHDR.CONT_APPROVED=1");
                buildQuery.Append("  AND CONTHDR.OPERATOR_MST_FK = " + nOprPk + " ");
                buildQuery.Append("  AND CONTHDR.ACTIVE=1");
                buildQuery.Append("  AND CONTHDR.VALID_TO = (SELECT MAX(CM.VALID_TO) FROM CONT_MAIN_SEA_TBL CM,");
                buildQuery.Append("  CONT_TRN_SEA_FCL_LCL CT ");
                buildQuery.Append("  WHERE CM.OPERATOR_MST_FK = " + nOprPk + "  ");
                buildQuery.Append("  AND CM.COMMODITY_GROUP_FK = " + CommodityPk + "");
                buildQuery.Append("  AND CM.CONT_APPROVED=1");
                buildQuery.Append("  AND CM.ACTIVE=1");
                buildQuery.Append("  AND CM.CARGO_TYPE = " + CargoType + "");
                buildQuery.Append("  AND CT.PORT_MST_POL_FK in (" + strPOL + ")");
                buildQuery.Append("  AND CT.PORT_MST_POD_FK in (" + strPOD + ")");
                buildQuery.Append("  AND CM.CONT_MAIN_SEA_PK = CT.CONT_MAIN_SEA_FK");
                buildQuery.Append(" )");
                return (new WorkFlow()).GetDataSet(buildQuery.ToString());
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "FetchContractFromRFQ"
        public DataSet FetchContractFromRFQ(long nRFQPk, Int16 nIsLCL)
        {
            try
            {
                string strSQL = null;
                if (nIsLCL == 1)
                {
                    strSQL = "SELECT OPR.OPERATOR_MST_PK,OPR.OPERATOR_ID,OPR.OPERATOR_NAME, " + "RFQHDR.RFQ_REF_NO,RFQHDR.RFQ_DATE, " + "RFQHDR.CARGO_TYPE, RFQHDR.COMMODITY_MST_FK, RFQHDR.VALID_FROM, " + "RFQHDR.VALID_TO,RFQHDR.VERSION_NO, " + "RFQTRN.PORT_MST_POL_FK,RFQTRN.PORT_MST_POD_FK,RFQTRN.CHECK_FOR_ALL_IN_RT, " + "RFQTRN.FREIGHT_ELEMENT_MST_FK,RFQTRN.CURRENCY_MST_FK,CURR.CURRENCY_ID, " + "POL.PORT_MST_PK POL_PK, POL.PORT_ID POL_ID, POL.PORT_NAME POL_NAME, POD.PORT_MST_PK POD_PK, POD.PORT_ID POD_ID, POD.PORT_NAME POD_NAME, " + "TO_CHAR(RFQTRN.VALID_FROM,'" + dateFormat + "') AS \"P_VALID_FROM\", " + "TO_CHAR(RFQTRN.VALID_TO,'" + dateFormat + "') AS \"P_VALID_TO\", " + "CMT.CONTAINER_TYPE_MST_ID, CONT.FCL_CURRENT_RATE,CONT.FCL_REQ_RATE,0 FCL_APP_RATE, " + "BCUR.CURRENCY_MST_PK BASE_CURRENCY_FK,BCUR.CURRENCY_ID BASE_CURRENCY_ID , RFQTRN.SHIP_TERM_MST_FK " + "FROM OPERATOR_MST_TBL OPR, " + "RFQ_TRN_SEA_FCL_LCL RFQTRN,  ";

                    strSQL = strSQL + "RFQ_TRN_SEA_CONT_DTL CONT,  " + "CONTAINER_TYPE_MST_TBL CMT, " + "RFQ_MAIN_SEA_TBL RFQHDR,CURRENCY_TYPE_MST_TBL CURR,CURRENCY_TYPE_MST_TBL  BCUR, PORT_MST_TBL POL, PORT_MST_TBL POD  " + "WHERE RFQHDR.RFQ_MAIN_SEA_PK = RFQTRN.RFQ_MAIN_SEA_FK " + "AND OPR.OPERATOR_MST_PK = RFQHDR.OPERATOR_MST_FK " + "AND CONT.RFQ_TRN_SEA_FK = RFQTRN.RFQ_TRN_SEA_PK" + "AND CURR.CURRENCY_MST_PK = RFQTRN.CURRENCY_MST_FK " + "AND CMT.CONTAINER_TYPE_MST_PK = CONT.CONTAINER_TYPE_MST_FK " + "AND BCUR.CURRENCY_MST_PK(+) = RFQHDR.BASE_CURRENCY_FK " + "AND POL.PORT_MST_PK = RFQTRN.PORT_MST_POL_FK " + "AND POD.PORT_MST_PK = RFQTRN.PORT_MST_POD_FK " + "AND RFQHDR.RFQ_MAIN_SEA_PK = " + nRFQPk;
                }
                else
                {
                    strSQL = "SELECT OPR.OPERATOR_MST_PK,OPR.OPERATOR_ID,OPR.OPERATOR_NAME, " + "RFQHDR.RFQ_REF_NO,RFQHDR.RFQ_DATE, " + "RFQHDR.CARGO_TYPE, RFQHDR.COMMODITY_MST_FK, RFQHDR.VALID_FROM, " + "RFQHDR.VALID_TO,RFQHDR.VERSION_NO, " + "RFQTRN.PORT_MST_POL_FK,RFQTRN.PORT_MST_POD_FK,RFQTRN.CHECK_FOR_ALL_IN_RT, " + "RFQTRN.FREIGHT_ELEMENT_MST_FK,RFQTRN.CURRENCY_MST_FK,CURR.CURRENCY_ID, " + "POL.PORT_MST_PK POL_PK, POL.PORT_ID POL_ID, POL.PORT_NAME POL_NAME, POD.PORT_MST_PK POD_PK, POD.PORT_ID POD_ID, POD.PORT_NAME POD_NAME, " + "TO_CHAR(RFQTRN.VALID_FROM,'" + dateFormat + "') AS P_VALID_FROM, " + "TO_CHAR(RFQTRN.VALID_TO,'" + dateFormat + "') AS P_VALID_TO, " + "RFQTRN.LCL_BASIS,RFQTRN.LCL_CURRENT_MIN_RATE,RFQTRN.LCL_CURRENT_RATE, " + "RFQTRN.LCL_REQUEST_MIN_RATE,RFQTRN.LCL_REQUEST_RATE,DMT.DIMENTION_ID, " + "BCUR.CURRENCY_MST_PK BASE_CURRENCY_FK,BCUR.CURRENCY_ID BASE_CURRENCY_ID,RFQTRN.SHIP_TERM_MST_FK " + "FROM OPERATOR_MST_TBL OPR, " + "RFQ_TRN_SEA_FCL_LCL RFQTRN,RFQ_MAIN_SEA_TBL RFQHDR, " + "CURRENCY_TYPE_MST_TBL CURR,DIMENTION_UNIT_MST_TBL DMT,CURRENCY_TYPE_MST_TBL  BCUR, PORT_MST_TBL POL, PORT_MST_TBL POD " + "WHERE RFQHDR.RFQ_MAIN_SEA_PK = RFQTRN.RFQ_MAIN_SEA_FK " + "AND OPR.OPERATOR_MST_PK = RFQHDR.OPERATOR_MST_FK " + "AND CURR.CURRENCY_MST_PK = RFQTRN.CURRENCY_MST_FK " + "AND DMT.DIMENTION_UNIT_MST_PK = RFQTRN.LCL_BASIS " + "AND BCUR.CURRENCY_MST_PK(+) = RFQHDR.BASE_CURRENCY_FK " + "AND POL.PORT_MST_PK = RFQTRN.PORT_MST_POL_FK " + "AND POD.PORT_MST_PK = RFQTRN.PORT_MST_POD_FK " + "AND RFQHDR.RFQ_MAIN_SEA_PK =" + nRFQPk;
                }
                return (new WorkFlow()).GetDataSet(strSQL);
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Fetch Contract"
        public DataSet FetchContract(long nContractPk, Int16 nIsLCL)
        {
            try
            {
                string strSQL = null;
                if (nIsLCL == 1)
                {
                    strSQL = "SELECT OPR.OPERATOR_MST_PK,OPR.OPERATOR_ID,OPR.OPERATOR_NAME, " + "CONTHDR.CONTRACT_NO,CONTHDR.CONTRACT_DATE, " + "CONTHDR.CARGO_TYPE, CONTHDR.COMMODITY_GROUP_FK,CONTHDR.RFQ_MAIN_TBL_FK,CONTHDR.VALID_FROM, " + "CONTHDR.VALID_TO,CONTHDR.ACTIVE,CONTHDR.CONT_APPROVED,CONTHDR.TRADE_CHK,CONTHDR.TRADETHC_CHK,CONTHDR.VERSION_NO, " + "CONTTRN.PORT_MST_POL_FK,CONTTRN.PORT_MST_POD_FK,CONTTRN.CHECK_FOR_ALL_IN_RT, " + "CONTTRN.FREIGHT_ELEMENT_MST_FK,CONTTRN.CURRENCY_MST_FK,CURR.CURRENCY_ID, " + "POL.PORT_MST_PK POL_PK, POL.PORT_ID POL_ID, POL.PORT_NAME POL_NAME, POD.PORT_MST_PK POD_PK, POD.PORT_ID POD_ID, POD.PORT_NAME POD_NAME, " + "TO_CHAR(CONTTRN.VALID_FROM,'" + dateFormat + "') AS \"P_VALID_FROM\", " + "TO_CHAR(CONTTRN.VALID_TO,'" + dateFormat + "') AS \"P_VALID_TO\", " + "CMT.CONTAINER_TYPE_MST_ID,CONT.FCL_CURRENT_RATE,CONT.FCL_REQ_RATE,CONT.FCL_APP_RATE, " + "CURRS.CURRENCY_MST_PK BASE_CURRENCY_FK,CURRS.CURRENCY_ID BASE_CURRENCY_ID,CONTHDR.WORKFLOW_STATUS,UMT.USER_ID APPROVEBY,CONTHDR.APPROVEDT,CONTTRN.SHIP_TERM_MST_FK " + "FROM OPERATOR_MST_TBL OPR, " + "CONT_TRN_SEA_FCL_LCL CONTTRN,  ";

                    strSQL = strSQL + "CONT_TRN_SEA_FCL_RATES CONT,  " + "CONTAINER_TYPE_MST_TBL CMT, " + "CONT_MAIN_SEA_TBL CONTHDR,CURRENCY_TYPE_MST_TBL CURR,  " + "CURRENCY_TYPE_MST_TBL  CURRS, PORT_MST_TBL POL, PORT_MST_TBL POD, USER_MST_TBL UMT " + "WHERE CONTHDR.CONT_MAIN_SEA_PK = CONTTRN.CONT_MAIN_SEA_FK " + "AND CONTTRN.CONT_TRN_SEA_PK = CONT.CONT_TRN_SEA_FK " + "AND OPR.OPERATOR_MST_PK = CONTHDR.OPERATOR_MST_FK " + "AND CURR.CURRENCY_MST_PK = CONTTRN.CURRENCY_MST_FK " + "AND CMT.CONTAINER_TYPE_MST_PK = CONT.CONTAINER_TYPE_MST_FK " + "AND CURRS.CURRENCY_MST_PK(+) = CONTHDR.BASE_CURRENCY_FK " + "AND UMT.USER_MST_PK(+) = CONTHDR.APPROVEBY " + "AND POL.PORT_MST_PK = CONTTRN.PORT_MST_POL_FK " + "AND POD.PORT_MST_PK = CONTTRN.PORT_MST_POD_FK " + "AND CONTHDR.CONT_MAIN_SEA_PK = " + nContractPk;
                }
                else
                {
                    strSQL = "SELECT OPR.OPERATOR_MST_PK,OPR.OPERATOR_ID,OPR.OPERATOR_NAME, " + "CONTHDR.CONTRACT_NO,CONTHDR.CONTRACT_DATE, " + "CONTHDR.CARGO_TYPE,CONTHDR.ACTIVE,CONTHDR.CONT_APPROVED,CONTHDR.TRADE_CHK,CONTHDR.TRADETHC_CHK,CONTHDR.COMMODITY_GROUP_FK,CONTHDR.RFQ_MAIN_TBL_FK,CONTHDR.VALID_FROM, " + "CONTHDR.VALID_TO,CONTHDR.VERSION_NO, " + "CONTTRN.PORT_MST_POL_FK,CONTTRN.PORT_MST_POD_FK,CONTTRN.CHECK_FOR_ALL_IN_RT, " + "CONTTRN.FREIGHT_ELEMENT_MST_FK,CONTTRN.CURRENCY_MST_FK,CURR.CURRENCY_ID, " + "POL.PORT_MST_PK POL_PK, POL.PORT_ID POL_ID, POL.PORT_NAME POL_NAME, POD.PORT_MST_PK POD_PK, POD.PORT_ID POD_ID, POD.PORT_NAME POD_NAME, " + "TO_CHAR(CONTTRN.VALID_FROM,'" + dateFormat + "') AS P_VALID_FROM, " + "TO_CHAR(CONTTRN.VALID_TO,'" + dateFormat + "') AS P_VALID_TO, " + "CONTTRN.LCL_BASIS,CONTTRN.LCL_CURRENT_MIN_RATE,CONTTRN.LCL_CURRENT_RATE,CONTTRN.LCL_REQUEST_MIN_RATE, " + "CONTTRN.LCL_REQUEST_RATE,CONTTRN.LCL_APPROVED_MIN_RATE,LCL_APPROVED_RATE,DMT.DIMENTION_ID, " + "CURRS.CURRENCY_MST_PK BASE_CURRENCY_FK,CURRS.CURRENCY_ID BASE_CURRENCY_ID,CONTHDR.WORKFLOW_STATUS,UMT.USER_ID APPROVEBY,CONTHDR.APPROVEDT,CONTTRN.SHIP_TERM_MST_FK " + "FROM OPERATOR_MST_TBL OPR, PORT_MST_TBL POL, PORT_MST_TBL POD, " + "CONT_TRN_SEA_FCL_LCL CONTTRN,CONT_MAIN_SEA_TBL CONTHDR, " + "CURRENCY_TYPE_MST_TBL CURR,DIMENTION_UNIT_MST_TBL DMT, " + "CURRENCY_TYPE_MST_TBL  CURRS,USER_MST_TBL UMT " + "WHERE CONTHDR.CONT_MAIN_SEA_PK = CONTTRN.CONT_MAIN_SEA_FK " + "AND OPR.OPERATOR_MST_PK = CONTHDR.OPERATOR_MST_FK " + "AND CURR.CURRENCY_MST_PK = CONTTRN.CURRENCY_MST_FK " + "AND DMT.DIMENTION_UNIT_MST_PK = CONTTRN.LCL_BASIS " + "AND CURRS.CURRENCY_MST_PK(+) = CONTHDR.BASE_CURRENCY_FK " + "AND UMT.USER_MST_PK(+) = CONTHDR.APPROVEBY " + "AND POL.PORT_MST_PK = CONTTRN.PORT_MST_POL_FK " + "AND POD.PORT_MST_PK = CONTTRN.PORT_MST_POD_FK " + "AND CONTHDR.CONT_MAIN_SEA_PK =" + nContractPk;
                }
                return (new WorkFlow()).GetDataSet(strSQL);
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public string FetchRFQREFNO(int RfqPk)
        {
            try
            {
                string strSQL = null;
                WorkFlow objWF = new WorkFlow();
                strSQL = "select RFQ_REF_NO from RFQ_MAIN_SEA_TBL where RFQ_MAIN_SEA_PK = " + RfqPk + " ";
                string RfqRefNo = null;
                RfqRefNo = objWF.ExecuteScaler(strSQL);
                return RfqRefNo;
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Fetch Max Contract No."
        public string FetchContractNo(string strContractNo)
        {
            try
            {
                string strSQL = null;
                strSQL = "SELECT NVL(MAX(T.CONTRACT_NO),0) FROM CONT_MAIN_SEA_TBL T " + "WHERE T.CONTRACT_NO LIKE '" + strContractNo + "/%' " + "ORDER BY T.CONTRACT_NO";
                return (new WorkFlow()).ExecuteScaler(strSQL);
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Update File Name"
        public bool UpdateFileName(long OpContractPk, string strFileName, Int16 Flag)
        {
            if (strFileName.Trim().Length > 0)
            {
                string RemQuery = null;
                WorkFlow objwk = new WorkFlow();
                if (Flag == 1)
                {
                    RemQuery = " UPDATE CONT_MAIN_SEA_TBL CMST SET CMST.ATTACHED_FILE_NAME='" + strFileName + "'";
                    RemQuery += " WHERE CMST.CONT_MAIN_SEA_PK = " + OpContractPk;
                    try
                    {
                        objwk.OpenConnection();
                        objwk.ExecuteCommands(RemQuery);
                        return true;
                    }
                    catch (OracleException OraExp)
                    {
                        throw OraExp;
                    }
                    catch (Exception ex)
                    {
                        throw ex;
                        return false;
                    }
                    finally
                    {
                        objwk.MyCommand.Connection.Close();
                    }
                }
                else if (Flag == 2)
                {
                    RemQuery = " UPDATE CONT_MAIN_SEA_TBL CMST SET CMST.ATTACHED_FILE_NAME='" + DBNull.Value + "'";
                    RemQuery += " WHERE CMST.CONT_MAIN_SEA_PK = " + OpContractPk;
                    try
                    {
                        objwk.OpenConnection();
                        objwk.ExecuteCommands(RemQuery);
                        return true;
                    }
                    catch (OracleException OraExp)
                    {
                        throw OraExp;
                    }
                    catch (Exception ex)
                    {
                        throw ex;
                        return false;
                    }
                    finally
                    {
                        objwk.MyCommand.Connection.Close();
                    }
                }
            }
            return false;
        }

        public string FetchFileName(long OpContractPk)
        {
            string strSQL = null;
            string strUpdFileName = null;
            WorkFlow objWF = new WorkFlow();
            strSQL = " SELECT ";
            strSQL += " CMST.ATTACHED_FILE_NAME FROM CONT_MAIN_SEA_TBL CMST WHERE CMST.CONT_MAIN_SEA_PK = " + OpContractPk;
            try
            {
                DataSet ds = null;
                strUpdFileName = objWF.ExecuteScaler(strSQL);
                return strUpdFileName;
            }
            catch (OracleException sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }
        #endregion

        #region "Fetch Trade Surcharge"
        public DataSet FetchExistingTrade()
        {
            string strSQL = null;
            strSQL = "SELECT TSUR.FRT_ELEMENT_MST_FK, " + "FRM.FREIGHT_ELEMENT_ID SURCHARGE," + "TSUR.FRT_APPLICABLE_ON_FK," + "FRM1.FREIGHT_ELEMENT_ID APPLICABLE," + "TSUR.PERC_APPLICABLE PERCENTAGE," + "(FRM1.Basis_Value*TSUR.PERC_APPLICABLE)/100 FINALSURCHARGE" + "FROM TRADE_SUR_TBL TSUR," + "FREIGHT_ELEMENT_MST_TBL FRM," + "FREIGHT_ELEMENT_MST_TBL FRM1" + "WHERE TSUR.FRT_ELEMENT_MST_FK = FRM.FREIGHT_ELEMENT_MST_PK(+)" + "AND TSUR.FRT_APPLICABLE_ON_FK = FRM1.FREIGHT_ELEMENT_MST_PK(+)" + "ORDER BY SURCHARGE";

            try
            {
                WorkFlow objWF = new WorkFlow();
                return objWF.GetDataSet(strSQL);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion

        #region "Fetch Basic of BOF"
        public double FetchBasicBOF()
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            strSQL = "SELECT FRM.BASIS_VALUE" + "FROM FREIGHT_ELEMENT_MST_TBL FRM" + "WHERE FRM.FREIGHT_ELEMENT_MST_PK=1145";
            try
            {
                return Convert.ToDouble(objWF.ExecuteScaler(strSQL));
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion

        #region "To Add THL THD Elements"
        public object GetThlThd(DataSet dsGrid, string strPolPk, string strPodPk, string strContId, bool IsLCL, string Mode, Int16 Chk = 0, Int16 ChkThc = 0)
        {
            string str = null;
            WorkFlow objWF = new WorkFlow();
            DataTable dtMainThc = new DataTable();
            DataTable dtContainerType = new DataTable();
            DataColumn dcCol = null;
            Int16 i = default(Int16);
            Array arrPolPk = null;
            Array arrPodPk = null;
            string strCondition = null;
            string strNewModeCondition = null;
            string strTradeCondition = null;
            string strThlThdPk = null;

            strThlThdPk = " select f.freight_element_mst_pk from freight_element_mst_tbl f where f.freight_element_id like 'THL' UNION";
            strThlThdPk += " select f.freight_element_mst_pk from freight_element_mst_tbl f where f.freight_element_id like 'THD'";


            arrPolPk = strPolPk.Split(',');
            arrPodPk = strPodPk.Split(',');

            for (i = 0; i <= arrPolPk.Length - 1; i++)
            {
                if (string.IsNullOrEmpty(strCondition))
                {
                    strCondition = " (POL.PORT_MST_PK =" + arrPolPk.GetValue(i) + " AND POD.PORT_MST_PK =" + arrPodPk.GetValue(i) + ")";
                }
                else
                {
                    strCondition += " OR (POL.PORT_MST_PK =" + arrPolPk.GetValue(i) + " AND POD.PORT_MST_PK =" + arrPodPk.GetValue(i) + ")";
                }
            }

            if (!(Mode == "EDIT"))
            {
                strNewModeCondition = " AND POL.BUSINESS_TYPE = 2";
                strNewModeCondition += " AND POD.BUSINESS_TYPE = 2";
                strNewModeCondition += " AND FMT.ACTIVE_FLAG = 1";
                strNewModeCondition += " AND FMT.BUSINESS_TYPE IN (2,3)";
            }

            str = " SELECT POL.PORT_MST_PK,POL.PORT_ID AS \"POL\",";
            str += " POD.PORT_MST_PK,POD.PORT_ID AS \"POD\", ";
            str += " FMT.FREIGHT_ELEMENT_MST_PK,FMT.FREIGHT_ELEMENT_ID,'0' FRTFORMULA,";
            str += " TO_CHAR('0') AS \"CHK\",CURR.CURRENCY_MST_PK,CURR.CURRENCY_ID,'' BASIS";

            str += " FROM FREIGHT_ELEMENT_MST_TBL FMT, PORT_MST_TBL POL, PORT_MST_TBL POD,";
            str += " CURRENCY_TYPE_MST_TBL CURR ";

            str += " WHERE (1=1)";
            str += " AND (";
            str += strCondition + ")";
            str += strNewModeCondition;
            str += " AND CURR.CURRENCY_MST_PK = " + HttpContext.Current.Session["CURRENCY_MST_PK"];
            str += " AND FMT.freight_element_mst_pk in (" + strThlThdPk + ")";
            str += " GROUP BY POL.PORT_MST_PK,POL.PORT_ID,POD.PORT_MST_PK,POD.PORT_ID,";
            str += " FMT.FREIGHT_ELEMENT_MST_PK,FMT.FREIGHT_ELEMENT_ID,";
            str += " CURR.CURRENCY_MST_PK,CURR.CURRENCY_ID";

            str += " HAVING POL.PORT_ID<>POD.PORT_ID";
            str += " ORDER BY FMT.FREIGHT_ELEMENT_ID";

            try
            {
                dtMainThc = objWF.GetDataTable(str);
                dtContainerType = FetchActiveCont(strContId);
                for (i = 0; i <= dtContainerType.Rows.Count - 1; i++)
                {
                    dcCol = new DataColumn(Convert.ToString(dtContainerType.Rows[i][0]));
                    dtMainThc.Columns.Add(dcCol);
                    dcCol = new DataColumn(Convert.ToString(dtContainerType.Rows[i][1]));
                    dtMainThc.Columns.Add(dcCol);

                    dcCol = new DataColumn(Convert.ToString(dtContainerType.Rows[i][2]));
                    dtMainThc.Columns.Add(dcCol);
                }
                return dtMainThc;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }
        #endregion

        #region "Fetch BOF pk"
        public string Fetch_BOF_pk()
        {
            string strSQL = null;
            strSQL = "select bof.frt_bof_fk from parameters_tbl bof";
            try
            {
                return (new WorkFlow()).ExecuteScaler(strSQL);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "FetchFreightName"
        public string FetchFreightName(string BOF_pk)
        {
            string strSQL = null;
            strSQL = "select frt.freight_element_id from freight_element_mst_tbl frt where frt.freight_element_mst_pk=" + BOF_pk;
            try
            {
                return (new WorkFlow()).ExecuteScaler(strSQL);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "FetchFormulaForFreight"
        public DataSet GetFormulaForFrt(int FrtEle, string POL = "", string POD = "")
        {
            string strSQL = null;
            strSQL = "SELECT TST.FRT_APPLICABLE_ON_FK,TST.PERC_APPLICABLE ";
            strSQL += " FROM TRADE_SUR_TBL TST, FREIGHT_ELEMENT_MST_TBL FRT";
            strSQL += " WHERE FRT.FREIGHT_ELEMENT_MST_PK = TST.FRT_ELEMENT_MST_FK ";
            strSQL += " AND FRT.FREIGHT_ELEMENT_MST_PK = " + FrtEle;
            strSQL += " AND TST.TRADE_MST_FK IN ";
            strSQL += " (select mst.trade_mst_pk ";
            strSQL += " from trade_mst_tbl mst, trade_mst_trn trn ";
            strSQL += " where mst.trade_mst_pk = trn.trade_mst_fk ";
            strSQL += " and trn.port_mst_fk in (" + POL + ")";
            strSQL += " and mst.trade_mst_pk in ";
            strSQL += " (select mst.trade_mst_pk ";
            strSQL += " from trade_mst_tbl mst, trade_mst_trn trn ";
            strSQL += " where mst.trade_mst_pk = trn.trade_mst_fk ";
            strSQL += " and trn.port_mst_fk IN (" + POD + ")))";
            try
            {
                return (new WorkFlow()).GetDataSet(strSQL);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "FetchBasispk"
        public Int32 Fetch(string Basis)
        {
            try
            {
                string strSQL = null;
                WorkFlow objWF = new WorkFlow();
                strSQL = "select dumt.dimention_unit_mst_pk from dimention_unit_mst_tbl dumt where dumt.dimention_id= '" + Basis + "' ";
                Int32 pk = default(Int32);
                pk = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
                return pk;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Fetch Report Details"
        public DataSet FetchReportDetails(string CustContractNr)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                WorkFlow objWK = new WorkFlow();
                sb.Append("select OMT.OPERATOR_NAME,");
                sb.Append("       OCD.ADM_ADDRESS_1,");
                sb.Append("       OCD.ADM_ADDRESS_2,");
                sb.Append("       OCD.ADM_ADDRESS_3,");
                sb.Append("       OCD.ADM_CITY,");
                sb.Append("       OCD.ADM_ZIP_CODE,");
                sb.Append("       CMT.COUNTRY_NAME,");
                sb.Append("       'SEA' BIZ_TYPE,");
                sb.Append("       DECODE(CMST.CARGO_TYPE, 1, 'FCL', 2, 'LCL') CARGO_TYPE,");
                sb.Append("       DECODE(CMST.CONT_APPROVED, 1, 'APPROVED', 0, 'PENDING',2,'REJECTED') CONT_APPROVED,");
                sb.Append("       DECODE(CMST.cont_approved,");
                sb.Append("              0,");
                sb.Append("              CUMT.USER_NAME,");
                sb.Append("              1,");
                sb.Append("              LUMT.USER_NAME,");
                sb.Append("              2,");
                sb.Append("              LUMT.USER_NAME) USER_ID,");
                sb.Append("       CGMT.COMMODITY_GROUP_DESC,");
                sb.Append("       RMST.RFQ_REF_NO,");
                sb.Append("       CMST.CONTRACT_NO,");
                sb.Append("       CMST.CONTRACT_DATE,");
                sb.Append("       CMST.VALID_FROM,");
                sb.Append("       CMST.VALID_TO");
                sb.Append("  FROM CONT_MAIN_SEA_TBL       CMST,");
                sb.Append("       OPERATOR_MST_TBL        OMT,");
                sb.Append("       OPERATOR_CONTACT_DTLS   OCD,");
                sb.Append("       COUNTRY_MST_TBL         CMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT,");
                sb.Append("       USER_MST_TBL            CUMT,");
                sb.Append("       USER_MST_TBL            LUMT,");
                sb.Append("       RFQ_MAIN_SEA_TBL        RMST");
                sb.Append(" WHERE CMST.OPERATOR_MST_FK = OMT.OPERATOR_MST_PK");
                sb.Append("   AND OCD.OPERATOR_MST_FK = OMT.OPERATOR_MST_PK");
                sb.Append("   AND OCD.ADM_COUNTRY_MST_FK = CMT.COUNTRY_MST_PK");
                sb.Append("   AND CMST.COMMODITY_GROUP_FK = CGMT.COMMODITY_GROUP_PK(+)");
                sb.Append("   AND CMST.RFQ_MAIN_TBL_FK = RMST.RFQ_MAIN_SEA_PK(+)");
                sb.Append("   AND CMST.CREATED_BY_FK = CUMT.USER_MST_PK");
                sb.Append("   AND CMST.LAST_MODIFIED_BY_FK = LUMT.USER_MST_PK(+)");
                sb.Append("   AND CMST.CONTRACT_NO = '" + CustContractNr + "'");
                return objWK.GetDataSet(sb.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Fetch Report Details"
        public DataSet FetchFreightDetails(string CustContractNr)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                WorkFlow objWK = new WorkFlow();
                sb.Append("SELECT DISTINCT CTSF.CONT_TRN_SEA_PK,");
                sb.Append("       POL.PORT_NAME PORT_ID,");
                sb.Append("       POD.PORT_NAME PORT_ID,");
                sb.Append("       CTSF.VALID_FROM,");
                sb.Append("       CTSF.VALID_TO,");
                sb.Append("       CONTAINER_TYPE_MST_ID || ' (' || CTM.CURRENCY_ID || ')' CONTAINER_TYPE_MST_ID,");
                //sb.Append("       CTMT.CONTAINER_TYPE_MST_ID CONTAINER_TYPE_MST_ID,")
                sb.Append("       FEMT.FREIGHT_ELEMENT_ID FREIGHT_ELEMENT_ID,");
                sb.Append("       FEMT.FREIGHT_ELEMENT_NAME,");
                sb.Append("       CTM.CURRENCY_ID,");
                sb.Append("       CTSR.FCL_CURRENT_RATE,");
                sb.Append("       CTSR.FCL_REQ_RATE,");
                sb.Append("       CTSR.FCL_APP_RATE,");
                sb.Append("       FEMT.PREFERENCE,SHIPTERM.MOVECODE_ID, CTMT.PREFERENCES, FEMT.PREFERENCE ");
                sb.Append("   FROM CONT_MAIN_SEA_TBL      CMST,");
                sb.Append("       CONT_TRN_SEA_FCL_LCL   CTSF,");
                sb.Append("       PORT_MST_TBL           POL,");
                sb.Append("       PORT_MST_TBL           POD,");
                sb.Append("       CONTAINER_TYPE_MST_TBL CTMT,");
                sb.Append("       FREIGHT_ELEMENT_MST_TBL FEMT,");
                sb.Append("       CURRENCY_TYPE_MST_TBL   CTM,");
                sb.Append("       CONT_TRN_SEA_FCL_RATES CTSR, MOVECODE_MST_TBL SHIPTERM");
                sb.Append(" WHERE CMST.CONT_MAIN_SEA_PK = CTSF.CONT_MAIN_SEA_FK");
                sb.Append("   AND CTSF.CONT_TRN_SEA_PK = CTSR.CONT_TRN_SEA_FK");
                sb.Append("   AND CTSF.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND CTSF.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND CTSR.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK ");
                sb.Append("   AND FEMT.FREIGHT_ELEMENT_MST_PK = CTSF.FREIGHT_ELEMENT_MST_FK");
                sb.Append("   AND CTM.CURRENCY_MST_PK = CTSF.CURRENCY_MST_FK AND SHIPTERM.MOVECODE_PK(+) = CTSF.SHIP_TERM_MST_FK ");
                sb.Append("   AND CMST.CONTRACT_NO= '" + CustContractNr + "'");
                sb.Append("   ORDER BY CTMT.PREFERENCES, FEMT.PREFERENCE ");
                return objWK.GetDataSet(sb.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "LCL Freight Details"
        public DataSet FetchLCLFreightDetails(string CustContractNr)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                WorkFlow objWK = new WorkFlow();
                sb.Append("SELECT CTSF.CONT_TRN_SEA_PK,");
                sb.Append("       POL.PORT_NAME PORT_ID,");
                sb.Append("       POD.PORT_NAME PORT_ID,");
                sb.Append("       CTSF.VALID_FROM,");
                sb.Append("       CTSF.VALID_TO,");
                sb.Append("       FEMT.FREIGHT_ELEMENT_ID FREIGHT_ELEMENT_ID,");
                sb.Append("       FEMT.FREIGHT_ELEMENT_NAME,");
                sb.Append("       CTM.CURRENCY_ID,");
                sb.Append("       DUMT.DIMENTION_ID,");
                sb.Append("       CTSF.LCL_CURRENT_MIN_RATE,");
                sb.Append("       CTSF.LCL_CURRENT_RATE,");
                sb.Append("       CTSF.LCL_REQUEST_MIN_RATE,");
                sb.Append("       CTSF.LCL_REQUEST_RATE,");
                sb.Append("       CTSF.LCL_APPROVED_MIN_RATE, ");
                sb.Append("       CTSF.LCL_APPROVED_RATE,SHIPTERM.MOVECODE_ID");
                sb.Append("");
                sb.Append("  FROM CONT_MAIN_SEA_TBL       CMST,");
                sb.Append("       CONT_TRN_SEA_FCL_LCL    CTSF,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       DIMENTION_UNIT_MST_TBL  DUMT,");
                sb.Append("       FREIGHT_ELEMENT_MST_TBL FEMT,");
                sb.Append("       CURRENCY_TYPE_MST_TBL   CTM, MOVECODE_MST_TBL SHIPTERM ");
                sb.Append(" WHERE CMST.CONT_MAIN_SEA_PK = CTSF.CONT_MAIN_SEA_FK");
                sb.Append("   AND CTSF.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND CTSF.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND DUMT.DIMENTION_UNIT_MST_PK = CTSF.LCL_BASIS");
                sb.Append("   AND FEMT.FREIGHT_ELEMENT_MST_PK = CTSF.FREIGHT_ELEMENT_MST_FK");
                sb.Append("   AND CTM.CURRENCY_MST_PK = CTSF.CURRENCY_MST_FK AND SHIPTERM.MOVECODE_PK(+) = CTSF.SHIP_TERM_MST_FK ");
                sb.Append("   AND CTSF.CHECK_FOR_ALL_IN_RT=1 ");
                sb.Append("   AND CMST.CONTRACT_NO = '" + CustContractNr + "'");
                sb.Append("ORDER BY PREFERENCE");
                return objWK.GetDataSet(sb.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Added Value"
        public DataSet FetchLCLFreightDetailsNew(string CustContractNr)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                WorkFlow objWK = new WorkFlow();
                sb.Append("SELECT nvl(SUM(NVL(CUR_RATE, 0)*ROE),0) CUR_RATE,");
                sb.Append("       nvl(SUM(NVL(REQ_RATE, 0)*ROE),0) REQ_RATE,");
                sb.Append("       nvl(SUM(NVL(APP_RATE, 0)*ROE),0) APP_RATE FROM (");
                sb.Append("SELECT CTSF.CONT_TRN_SEA_PK,");
                sb.Append("       POL.PORT_NAME PORT_ID,");
                sb.Append("       POD.PORT_NAME PORT_ID,");
                sb.Append("       CTSF.VALID_FROM,");
                sb.Append("       CTSF.VALID_TO,");
                sb.Append("       FEMT.FREIGHT_ELEMENT_ID FREIGHT_ELEMENT_ID,");
                sb.Append("       FEMT.FREIGHT_ELEMENT_NAME,");
                sb.Append("       CTM.CURRENCY_ID,");
                sb.Append("       DUMT.DIMENTION_ID,");
                sb.Append("       CTSF.LCL_CURRENT_MIN_RATE,");
                sb.Append("       CTSF.LCL_CURRENT_RATE CUR_RATE,");
                sb.Append("       CTSF.LCL_REQUEST_MIN_RATE,");
                sb.Append("       CTSF.LCL_REQUEST_RATE REQ_RATE,");
                sb.Append("       CTSF.LCL_APPROVED_MIN_RATE, ");
                sb.Append("       CTSF.LCL_APPROVED_RATE APP_RATE,");
                sb.Append("       GET_EX_RATE(CTSF.CURRENCY_MST_FK, CMST.BASE_CURRENCY_FK,SYSDATE) ROE ");
                sb.Append("  FROM CONT_MAIN_SEA_TBL       CMST,");
                sb.Append("       CONT_TRN_SEA_FCL_LCL    CTSF,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       DIMENTION_UNIT_MST_TBL  DUMT,");
                sb.Append("       FREIGHT_ELEMENT_MST_TBL FEMT,");
                sb.Append("       CURRENCY_TYPE_MST_TBL   CTM");
                sb.Append(" WHERE CMST.CONT_MAIN_SEA_PK = CTSF.CONT_MAIN_SEA_FK");
                sb.Append("   AND CTSF.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND CTSF.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND DUMT.DIMENTION_UNIT_MST_PK = CTSF.LCL_BASIS");
                sb.Append("   AND FEMT.FREIGHT_ELEMENT_MST_PK = CTSF.FREIGHT_ELEMENT_MST_FK");
                sb.Append("   AND CTM.CURRENCY_MST_PK = CTSF.CURRENCY_MST_FK");
                sb.Append("   AND CTSF.CHECK_FOR_ALL_IN_RT=1 ");
                sb.Append("   AND CMST.CONTRACT_NO = '" + CustContractNr + "'");
                sb.Append("ORDER BY FREIGHT_ELEMENT_ID)");
                return objWK.GetDataSet(sb.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Fetch Contract Status Whether it is approved or not"
        public long Fetch_Contract_Status(long PK)
        {
            string strSQL = null;
            strSQL = "SELECT C.CONT_APPROVED FROM CONT_MAIN_SEA_TBL C WHERE C.CONT_MAIN_SEA_PK=" + PK;
            try
            {
                return Convert.ToInt64((new WorkFlow()).ExecuteScaler(strSQL));
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "For Fetching All Freight Elements while Amending"
        public DataTable FetchAllFreight(string strPolPk, string strPodPk, string strContId, bool IsLCL, string Mode, string ContPK, Int16 Chk = 0, Int16 ChkThc = 0, string FromDt = "", string ToDt = "")
        {
            string str = null;
            WorkFlow objWF = new WorkFlow();
            DataTable dtMain = new DataTable();
            DataTable dtContainerType = new DataTable();
            //This datatable contains the active containers
            DataColumn dcCol = null;
            Int16 i = default(Int16);
            Array arrPolPk = null;
            Array arrPodPk = null;
            string strCondition = null;
            string strNewModeCondition = null;
            string strTradeCondition = null;
            //Snigdharani - 26/11/2008 - In New Mode, THL and THD freight elements need not be fetched.
            string strThlThdPk = null;
            strThlThdPk = " select f.freight_element_mst_pk from freight_element_mst_tbl f where f.freight_element_id like 'THL' UNION";
            strThlThdPk += " select f.freight_element_mst_pk from freight_element_mst_tbl f where f.freight_element_id like 'THD'";
            arrPolPk = strPolPk.Split(',');
            arrPodPk = strPodPk.Split(',');
            for (i = 0; i <= arrPolPk.Length - 1; i++)
            {
                if (string.IsNullOrEmpty(strCondition))
                {
                    strCondition = " (POL.PORT_MST_PK =" + arrPolPk.GetValue(i) + " AND POD.PORT_MST_PK =" + arrPodPk.GetValue(i) + ")";
                }
                else
                {
                    strCondition += " OR (POL.PORT_MST_PK =" + arrPolPk.GetValue(i) + " AND POD.PORT_MST_PK =" + arrPodPk.GetValue(i) + ")";
                }
            }

            //Snigdharani - 26/11/2008 - In New Mode, THL and THD freight elements need not be fetched.

            str = " SELECT Q.* FROM (select qry.POLPK PORT_MST_PK, qry.POL POL,";
            str += " qry.PODPK PORT_MST_PK1, qry.POD POD,";
            str += " qry.FREIGHT_ELEMENT_MST_PK,";
            //str &= vbCrLf & " qry.FREIGHT_ELEMENT_ID, QRY.FRTFORMULA FRTFORMULA, qry.chk CHK,"
            str += " qry.FREIGHT_ELEMENT_ID, QRY.FRTFORMULA1 FRTFORMULA, qry.chk CHK,";
            str += " qry.CURRENCY_MST_PK, qry.CURRENCY_ID, QRY.BASIS from (";

            str += " SELECT DISTINCT POL.PORT_MST_PK POLPK,POL.PORT_ID AS \"POL\",";
            str += " POD.PORT_MST_PK PODPK,POD.PORT_ID AS \"POD\", ";
            str += " FMT.FREIGHT_ELEMENT_MST_PK,FMT.FREIGHT_ELEMENT_ID,";
            //commented and modified by surya prasad as if it takes multiple pol,pod(s) IsSurchargePresent function fails
            //str &= vbCrLf & " IsSurchargePresent(" & strPolPk & "," & strPodPk & ",2,fmt.freight_element_mst_pk) FRTFORMULA,"
            str += "(CASE WHEN FMT.FREIGHT_ELEMENT_MST_PK = Q.FRT_ELEMENT_MST_FK THEN";
            str += "1";
            str += " ELSE";
            str += "0";
            str += " END) FRTFORMULA1,";
            //end 
            str += " TO_CHAR('0') AS \"CHK\",CURR.CURRENCY_MST_PK,CURR.CURRENCY_ID,";
            //Modified by Snigdharani
            if (!IsLCL)
            {
                str += " '' BASIS";
            }
            else
            {
                //ElseIf Mode = "FRORFQ" Then 'added by purnanand
                //    str &= vbCrLf & " DUMT.DIMENTION_ID BASIS"
                //Else
                str += " DUMT.DIMENTION_ID BASIS";
            }

            str += " FROM FREIGHT_ELEMENT_MST_TBL FMT, PORT_MST_TBL POL, PORT_MST_TBL POD,";
            str += " CURRENCY_TYPE_MST_TBL CURR, TRADE_SUR_TBL   TST, ";
            //removed "Corporate mst. table"  by thiyagarajan on 18/11/08 for loc. based curr.
            if (Mode == "FROMRFQ")
            {
                str += " RFQ_TRN_SEA_FCL_LCL CNTTRN, DIMENTION_UNIT_MST_TBL  DUMT,";
                //Snigdharani
            }
            else
            {
                str += " DIMENTION_UNIT_MST_TBL  DUMT,";
                //Snigdharani
            }
            if (Mode != "FROMRFQ")
            {
                str += " CONT_TRN_SEA_FCL_LCL CNTTRN, ";
            }

            str += "   (SELECT TST.FRT_ELEMENT_MST_FK";
            str += "      FROM TRADE_SUR_TBL TST";
            str += "     WHERE TST.TRADE_MST_FK IN";
            str += "  (select mst.trade_mst_pk";
            str += "      from trade_mst_tbl mst, trade_mst_trn trn";
            str += " where(mst.trade_mst_pk = trn.trade_mst_fk)";
            str += "      and trn.port_mst_fk in( " + strPolPk + ")";
            str += "       and mst.trade_mst_pk in";
            str += "          (select mst.trade_mst_pk";
            str += "            from trade_mst_tbl mst, trade_mst_trn trn";
            str += " where(mst.trade_mst_pk = trn.trade_mst_fk)";
            str += "               and trn.port_mst_fk in( " + strPodPk + ")))) Q";
            //end

            str += " WHERE (1=1)";
            str += " AND FMT.FREIGHT_ELEMENT_MST_PK = TST.FRT_ELEMENT_MST_FK(+)";
            str += " AND (";
            str += strCondition + ")";
            str += strNewModeCondition;

            //adding  "Session("CURRENCY_MST_PK")"  by thiyagarajan on 18/11/08 for loc. based curr.
            str += " AND CURR.CURRENCY_MST_PK = " + HttpContext.Current.Session["CURRENCY_MST_PK"];

            //modified
            //str &= vbCrLf & " AND FMT.FREIGHT_TYPE <> 0 " 'and charge_basis <> 2"
            str += " AND FMT.CHARGE_TYPE <> 3 ";
            //and charge_basis <> 2"

            str += " AND (CNTTRN.PORT_MST_POL_FK = POL.PORT_MST_PK AND ";
            str += " CNTTRN.PORT_MST_POD_FK = POD.PORT_MST_PK) ";
            if (string.IsNullOrEmpty(ToDt))
            {
                str += " AND (CNTTRN.VALID_FROM = TO_DATE('" + FromDt + "' , 'DD/MM/YYYY')) ";
            }
            else
            {
                str += " AND (CNTTRN.VALID_FROM = TO_DATE('" + FromDt + "' , 'DD/MM/YYYY') AND ";
                str += " CNTTRN.VALID_TO = TO_DATE('" + ToDt + "' , 'DD/MM/YYYY')) ";
            }
            str += " AND FMT.FREIGHT_ELEMENT_MST_PK(+) = CNTTRN.FREIGHT_ELEMENT_MST_FK ";
            if (!IsLCL)
            {
                str += " AND DUMT.DIMENTION_UNIT_MST_PK(+) = CNTTRN.LCL_BASIS ";
            }
            else
            {
                str += " AND DUMT.DIMENTION_UNIT_MST_PK = CNTTRN.LCL_BASIS ";
            }
            //str &= vbCrLf & " AND DUMT.DIMENTION_UNIT_MST_PK(+) = CNTTRN.LCL_BASIS "

            //comented by purnanand as freight element coming as preference
            str += " AND FMT.FREIGHT_ELEMENT_MST_PK IN(SELECT PARM.frt_bof_fk FROM PARAMETERS_TBL PARM)";
            //Added by rabbani on 24/3/07 ,To display BOF as first element in Freight Element Column.

            //added by surya prasad on 11-mar-2009
            str += "  and FMT.FREIGHT_ELEMENT_MST_PK = Q.FRT_ELEMENT_MST_FK(+)";
            //end

            str += " GROUP BY POL.PORT_MST_PK,POL.PORT_ID,POD.PORT_MST_PK,POD.PORT_ID,";
            str += " FMT.FREIGHT_ELEMENT_MST_PK,TST.FRT_ELEMENT_MST_FK,FMT.FREIGHT_ELEMENT_ID,";
            str += " CURR.CURRENCY_MST_PK,CURR.CURRENCY_ID,DUMT.DIMENTION_ID,";
            str += "  CNTTRN.LCL_BASIS,";

            //added by surya prasad on 11-mar-2009
            str += "  Q.FRT_ELEMENT_MST_FK ";
            str += " HAVING POL.PORT_ID<>POD.PORT_ID";
            //str &= vbCrLf & " ORDER BY FMT.FREIGHT_ELEMENT_ID"

            str += " UNION ALL";
            //Added by rabbani on 24/3/07 ,To display BOF as first element in Freight Element Column.
            str += " SELECT DISTINCT POL.PORT_MST_PK POLPK,POL.PORT_ID AS \"POL\",";
            str += " POD.PORT_MST_PK PODPK,POD.PORT_ID AS \"POD\", ";
            str += " FMT.FREIGHT_ELEMENT_MST_PK,FMT.FREIGHT_ELEMENT_ID,";
            //commented and modified by surya prasad as if it takes multiple pol,pod(s) IsSurchargePresent function fails
            // str &= vbCrLf & " IsSurchargePresent(" & strPolPk & "," & strPodPk & ",2,fmt.freight_element_mst_pk) FRTFORMULA,"
            str += "(CASE WHEN FMT.FREIGHT_ELEMENT_MST_PK = Q.FRT_ELEMENT_MST_FK THEN";
            str += "1";
            str += " ELSE";
            str += "0";
            str += " END) FRTFORMULA1,";
            //end 
            //Modified by Snigdharani - 23/09/2008
            str += " TO_CHAR('0') AS \"CHK\",CURR.CURRENCY_MST_PK,CURR.CURRENCY_ID,";
            if (!IsLCL)
            {
                str += " '' BASIS";
            }
            else
            {
                str += " DUMT.DIMENTION_ID BASIS";
            }
            //If Chk = 1 Then
            //    str &= vbCrLf & ",TSUR.PERC_APPLICABLE"
            //End If
            //,TSUR.PERC_APPLICABLE"
            str += " FROM FREIGHT_ELEMENT_MST_TBL FMT, PORT_MST_TBL POL, PORT_MST_TBL POD,";
            str += " CURRENCY_TYPE_MST_TBL CURR, TRADE_SUR_TBL   TST, ";
            //removed "Corporate mst. table"  by thiyagarajan on 18/11/08 for loc. based curr.
            str += " CONT_TRN_SEA_FCL_LCL CNTTRN, ";
            str += " DIMENTION_UNIT_MST_TBL  DUMT, ";
            //Snigdharani

            str += "   (SELECT TST.FRT_ELEMENT_MST_FK";
            str += "      FROM TRADE_SUR_TBL TST";
            str += "     WHERE TST.TRADE_MST_FK IN";
            str += "  (select mst.trade_mst_pk";
            str += "      from trade_mst_tbl mst, trade_mst_trn trn";
            str += " where(mst.trade_mst_pk = trn.trade_mst_fk)";
            str += "      and trn.port_mst_fk in( " + strPolPk + ")";
            str += "       and mst.trade_mst_pk in";
            str += "          (select mst.trade_mst_pk";
            str += "            from trade_mst_tbl mst, trade_mst_trn trn";
            str += " where(mst.trade_mst_pk = trn.trade_mst_fk)";
            str += "               and trn.port_mst_fk in( " + strPodPk + ")))) Q";
            //end

            str += " WHERE (1=1)";
            str += " AND FMT.FREIGHT_ELEMENT_MST_PK = TST.FRT_ELEMENT_MST_FK(+)";
            str += " AND (";
            str += strCondition + ")";
            str += strNewModeCondition;
            //adding  "Session("CURRENCY_MST_PK")"  by thiyagarajan on 18/11/08 for loc. based curr.
            str += " AND CURR.CURRENCY_MST_PK = " + HttpContext.Current.Session["CURRENCY_MST_PK"];
            //modified
            //str &= vbCrLf & " AND FMT.FREIGHT_TYPE <> 0 " 'and charge_basis <> 2"
            str += " AND FMT.CHARGE_TYPE <> 3 ";
            //and charge_basis <> 2"

            str += " AND (CNTTRN.PORT_MST_POL_FK = POL.PORT_MST_PK AND ";
            str += " CNTTRN.PORT_MST_POD_FK = POD.PORT_MST_PK) ";
            if (string.IsNullOrEmpty(ToDt))
            {
                str += " AND (CNTTRN.VALID_FROM = TO_DATE('" + FromDt + "' , 'DD/MM/YYYY')) ";
            }
            else
            {
                str += " AND (CNTTRN.VALID_FROM = TO_DATE('" + FromDt + "' , 'DD/MM/YYYY') AND ";
                str += " CNTTRN.VALID_TO = TO_DATE('" + ToDt + "' , 'DD/MM/YYYY')) ";
            }
            //'Modified by Mayur for showing all freight element until approved

            str += " AND FMT.FREIGHT_ELEMENT_MST_PK(+) = CNTTRN.FREIGHT_ELEMENT_MST_FK ";
            //'Ended by Mayur
            if (!IsLCL)
            {
                str += " AND DUMT.DIMENTION_UNIT_MST_PK(+) = CNTTRN.LCL_BASIS ";
            }
            else
            {
                str += " AND DUMT.DIMENTION_UNIT_MST_PK = CNTTRN.LCL_BASIS ";
            }
            //str &= vbCrLf & " AND DUMT.DIMENTION_UNIT_MST_PK(+) = CNTTRN.LCL_BASIS "

            str += " AND FMT.FREIGHT_ELEMENT_MST_PK NOT IN(SELECT PARM.frt_bof_fk FROM PARAMETERS_TBL PARM)";

            //added by surya prasad on 11-mar-2009
            str += "  and FMT.FREIGHT_ELEMENT_MST_PK = Q.FRT_ELEMENT_MST_FK(+)";
            //end

            str += " GROUP BY POL.PORT_MST_PK,POL.PORT_ID,POD.PORT_MST_PK,POD.PORT_ID,";
            str += " FMT.FREIGHT_ELEMENT_MST_PK,TST.FRT_ELEMENT_MST_FK,FMT.FREIGHT_ELEMENT_ID,";
            str += " CURR.CURRENCY_MST_PK,CURR.CURRENCY_ID, DUMT.DIMENTION_ID,";
            str += " CNTTRN.LCL_BASIS, ";
            //added by surya prasad on 11-mar-2009
            str += "  Q.FRT_ELEMENT_MST_FK ";
            //end

            //If Chk = 1 Then
            //    str &= vbCrLf & ",TSUR.PERC_APPLICABLE"
            //End If
            //,TSUR.PERC_APPLICABLE
            str += " HAVING POL.PORT_ID<>POD.PORT_ID";
            //Snigdharani

            str += " ) qry,";
            str += " FREIGHT_ELEMENT_MST_TBL FRT";
            str += " WHERE QRY.FREIGHT_ELEMENT_MST_PK = FRT.FREIGHT_ELEMENT_MST_PK";

            str += " UNION ";

            strNewModeCondition = " AND POL.BUSINESS_TYPE = 2";
            //BUSINESS_TYPE = 2 :- Is the business type for SEA
            strNewModeCondition += " AND POD.BUSINESS_TYPE = 2";
            //BUSINESS_TYPE = 2 :- Is the business type for SEA
            strNewModeCondition += " AND FMT.ACTIVE_FLAG = 1";
            strNewModeCondition += " AND FMT.BUSINESS_TYPE IN (2,3)";
            //strNewModeCondition &= vbCrLf & " AND FMT.BY_DEFAULT IN(1) " 'Modified By Anand to check only  default set to true

            str += " select qry.POLPK PORT_MST_PK, qry.POL POL,";
            str += " qry.PODPK PORT_MST_PK, qry.POD POD,";
            str += " qry.FREIGHT_ELEMENT_MST_PK,";
            //str &= vbCrLf & " qry.FREIGHT_ELEMENT_ID, QRY.FRTFORMULA FRTFORMULA, qry.chk CHK,"
            str += " qry.FREIGHT_ELEMENT_ID, QRY.FRTFORMULA1 FRTFORMULA, qry.chk CHK,";
            str += " qry.CURRENCY_MST_PK, qry.CURRENCY_ID, QRY.BASIS from (";

            str += " SELECT DISTINCT POL.PORT_MST_PK POLPK,POL.PORT_ID AS \"POL\",";
            str += " POD.PORT_MST_PK PODPK,POD.PORT_ID AS \"POD\", ";
            str += " FMT.FREIGHT_ELEMENT_MST_PK,FMT.FREIGHT_ELEMENT_ID,";

            str += "(CASE WHEN FMT.FREIGHT_ELEMENT_MST_PK = Q.FRT_ELEMENT_MST_FK THEN";
            str += "1";
            str += " ELSE";
            str += "0";
            str += " END) FRTFORMULA1,";
            //end 
            str += " TO_CHAR('0') AS \"CHK\",CURR.CURRENCY_MST_PK,CURR.CURRENCY_ID,";
            //If Not IsLCL Then
            str += " '' BASIS";
            //Else
            //    str &= vbCrLf & " DUMT.DIMENTION_ID BASIS"
            //End If
            str += " FROM FREIGHT_ELEMENT_MST_TBL FMT, PORT_MST_TBL POL, PORT_MST_TBL POD,";
            str += " CURRENCY_TYPE_MST_TBL CURR, TRADE_SUR_TBL   TST, ";
            //removed "Corporate mst. table"  by thiyagarajan on 18/11/08 for loc. based curr.
            str += " DIMENTION_UNIT_MST_TBL  DUMT,";
            //Snigdharani            

            str += "   (SELECT TST.FRT_ELEMENT_MST_FK";
            str += "      FROM TRADE_SUR_TBL TST";
            str += "     WHERE TST.TRADE_MST_FK IN";
            str += "  (select mst.trade_mst_pk";
            str += "      from trade_mst_tbl mst, trade_mst_trn trn";
            str += " where(mst.trade_mst_pk = trn.trade_mst_fk)";
            str += "      and trn.port_mst_fk in( " + strPolPk + ")";
            str += "       and mst.trade_mst_pk in";
            str += "          (select mst.trade_mst_pk";
            str += "            from trade_mst_tbl mst, trade_mst_trn trn";
            str += " where(mst.trade_mst_pk = trn.trade_mst_fk)";
            str += "               and trn.port_mst_fk in( " + strPodPk + ")))) Q";
            //end

            str += " WHERE (1=1)";
            str += " AND FMT.FREIGHT_ELEMENT_MST_PK = TST.FRT_ELEMENT_MST_FK(+)";
            str += " AND (";
            str += strCondition + ")";
            str += strNewModeCondition;
            str += " AND CURR.CURRENCY_MST_PK = " + HttpContext.Current.Session["CURRENCY_MST_PK"];
            str += " AND FMT.CHARGE_TYPE <> 3 ";
            //and charge_basis <> 2"                  


            str += " AND FMT.FREIGHT_ELEMENT_MST_PK IN(SELECT PARM.frt_bof_fk FROM PARAMETERS_TBL PARM)";
            //Added by rabbani on 24/3/07 ,To display BOF as first element in Freight Element Column.

            //added by surya prasad on 11-mar-2009
            str += "  and FMT.FREIGHT_ELEMENT_MST_PK = Q.FRT_ELEMENT_MST_FK(+)";
            //end
            //'
            str += "  AND FMT.FREIGHT_ELEMENT_MST_PK NOT IN (SELECT ";
            str += "  FTMT.FREIGHT_ELEMENT_MST_PK FROM FREIGHT_ELEMENT_MST_TBL FTMT,CONT_TRN_SEA_FCL_LCL    CTFL";
            str += "  WHERE FTMT.FREIGHT_ELEMENT_MST_PK = CTFL.FREIGHT_ELEMENT_MST_FK AND CTFL.CONT_MAIN_SEA_FK =" + ContPK;


            str += " ) GROUP BY POL.PORT_MST_PK,POL.PORT_ID,POD.PORT_MST_PK,POD.PORT_ID,";
            str += " FMT.FREIGHT_ELEMENT_MST_PK,TST.FRT_ELEMENT_MST_FK,FMT.FREIGHT_ELEMENT_ID,";
            str += " CURR.CURRENCY_MST_PK,CURR.CURRENCY_ID,DUMT.DIMENTION_ID,";
            str += "  Q.FRT_ELEMENT_MST_FK ";
            str += " HAVING POL.PORT_ID<>POD.PORT_ID";
            str += " UNION ALL";
            str += " SELECT DISTINCT POL.PORT_MST_PK POLPK,POL.PORT_ID AS \"POL\",";
            str += " POD.PORT_MST_PK PODPK,POD.PORT_ID AS \"POD\", ";
            str += " FMT.FREIGHT_ELEMENT_MST_PK,FMT.FREIGHT_ELEMENT_ID,";
            str += "(CASE WHEN FMT.FREIGHT_ELEMENT_MST_PK = Q.FRT_ELEMENT_MST_FK THEN";
            str += "1";
            str += " ELSE";
            str += "0";
            str += " END) FRTFORMULA1,";
            //end 
            //Modified by Snigdharani - 23/09/2008
            str += " TO_CHAR('0') AS \"CHK\",CURR.CURRENCY_MST_PK,CURR.CURRENCY_ID,";
            //If Not IsLCL Then
            str += " '' BASIS";
            //Else
            //    str &= vbCrLf & " DUMT.DIMENTION_ID BASIS"
            //End If

            str += " FROM FREIGHT_ELEMENT_MST_TBL FMT, PORT_MST_TBL POL, PORT_MST_TBL POD,";
            str += " CURRENCY_TYPE_MST_TBL CURR, TRADE_SUR_TBL   TST, ";
            //removed "Corporate mst. table"  by thiyagarajan on 18/11/08 for loc. based curr.
            str += " DIMENTION_UNIT_MST_TBL  DUMT, ";
            //Snigdharani

            str += "   (SELECT TST.FRT_ELEMENT_MST_FK";
            str += "      FROM TRADE_SUR_TBL TST";
            str += "     WHERE TST.TRADE_MST_FK IN";
            str += "  (select mst.trade_mst_pk";
            str += "      from trade_mst_tbl mst, trade_mst_trn trn";
            str += " where(mst.trade_mst_pk = trn.trade_mst_fk)";
            str += "      and trn.port_mst_fk in( " + strPolPk + ")";
            str += "       and mst.trade_mst_pk in";
            str += "          (select mst.trade_mst_pk";
            str += "            from trade_mst_tbl mst, trade_mst_trn trn";
            str += " where(mst.trade_mst_pk = trn.trade_mst_fk)";
            str += "               and trn.port_mst_fk in( " + strPodPk + ")))) Q";
            //end

            str += " WHERE (1=1)";
            str += " AND FMT.FREIGHT_ELEMENT_MST_PK = TST.FRT_ELEMENT_MST_FK(+)";
            str += " AND (";
            str += strCondition + ")";
            str += strNewModeCondition;
            str += " AND CURR.CURRENCY_MST_PK = " + HttpContext.Current.Session["CURRENCY_MST_PK"];
            str += " AND FMT.CHARGE_TYPE <> 3 ";
            str += " AND FMT.FREIGHT_ELEMENT_MST_PK NOT IN(SELECT PARM.frt_bof_fk FROM PARAMETERS_TBL PARM)";

            str += "  and FMT.FREIGHT_ELEMENT_MST_PK = Q.FRT_ELEMENT_MST_FK(+)";

            str += "  AND FMT.FREIGHT_ELEMENT_MST_PK NOT IN (SELECT ";
            str += "  FTMT.FREIGHT_ELEMENT_MST_PK FROM FREIGHT_ELEMENT_MST_TBL FTMT,CONT_TRN_SEA_FCL_LCL    CTFL";
            str += "  WHERE FTMT.FREIGHT_ELEMENT_MST_PK = CTFL.FREIGHT_ELEMENT_MST_FK AND CTFL.CONT_MAIN_SEA_FK =" + ContPK;

            str += " ) GROUP BY POL.PORT_MST_PK,POL.PORT_ID,POD.PORT_MST_PK,POD.PORT_ID,";
            str += " FMT.FREIGHT_ELEMENT_MST_PK,TST.FRT_ELEMENT_MST_FK,FMT.FREIGHT_ELEMENT_ID,";
            str += " CURR.CURRENCY_MST_PK,CURR.CURRENCY_ID, DUMT.DIMENTION_ID,";
            str += "  Q.FRT_ELEMENT_MST_FK ";
            str += " HAVING POL.PORT_ID<>POD.PORT_ID";
            str += " ) qry,";
            str += " FREIGHT_ELEMENT_MST_TBL FRT";
            str += " WHERE QRY.FREIGHT_ELEMENT_MST_PK = FRT.FREIGHT_ELEMENT_MST_PK";
            str += " ) Q, FREIGHT_ELEMENT_MST_TBL OFRT ";
            str += " WHERE Q.FREIGHT_ELEMENT_MST_PK = OFRT.FREIGHT_ELEMENT_MST_PK ";
            str += "  ORDER BY OFRT.PREFERENCE ";
            try
            {
                dtMain = objWF.GetDataTable(str);
                if (!IsLCL)
                {
                    dtContainerType = FetchActiveCont(strContId);
                    for (i = 0; i <= dtContainerType.Rows.Count - 1; i++)
                    {
                        dcCol = new DataColumn(Convert.ToString(dtContainerType.Rows[i][0]));
                        dtMain.Columns.Add(dcCol);
                        dcCol = new DataColumn(Convert.ToString(dtContainerType.Rows[i][1]));
                        dtMain.Columns.Add(dcCol);

                        dcCol = new DataColumn(Convert.ToString(dtContainerType.Rows[i][2]));
                        dtMain.Columns.Add(dcCol);
                    }
                }
                else
                {
                    //dtMain.Columns.Add("Basis")
                    dtMain.Columns.Add("CurrMin");
                    //Added by Rabbani raeson USS Gap,introduced New column as "Curr.Min.Rate"
                    dtMain.Columns.Add("Curr");
                    dtMain.Columns.Add("ReqMin");
                    //Added by Rabbani raeson USS Gap,introduced New column as "Req.Min.Rate"
                    dtMain.Columns.Add("Req");
                    dtMain.Columns.Add("BasisPK");
                    dtMain.Columns.Add("AppMin");
                    //Added by Rabbani raeson USS Gap,introduced New column as "App.Min.Rate"
                    dtMain.Columns.Add("App");
                }
                return dtMain;
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 13/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "FECTH CURRENCY"
        public DataSet GetCurrenyDetails(string CurrFK, string FromFlag)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with8 = objWK.MyCommand.Parameters;
                _with8.Add("CURRENCY_FK_IN", CurrFK).Direction = ParameterDirection.Input;
                _with8.Add("LOCATION_MST_FK_IN", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"])).Direction = ParameterDirection.Input;
                _with8.Add("FROM_FLG_IN", FromFlag).Direction = ParameterDirection.Input;
                _with8.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("CONT_MAIN_SEA_TBL_PKG", "FETCH_CURR_DETAILS");
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public ArrayList SaveExchangeRate(DataSet DS, long BaseCurrFk, string FromDt = "", string FromFlg = "", string RefFK = "")
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = default(OracleTransaction);
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            objWK.MyCommand.Transaction = TRAN;
            arrMessage.Clear();
            try
            {
                if (DS.Tables[0].Rows.Count > 0)
                {
                    for (int Rcnt = 0; Rcnt <= DS.Tables[0].Rows.Count - 1; Rcnt++)
                    {
                        var _with9 = objWK.MyCommand;
                        _with9.CommandType = CommandType.StoredProcedure;
                        _with9.CommandText = objWK.MyUserName + ".CONT_MAIN_SEA_TBL_PKG.TEMP_EX_RATE_TRN_INS";
                        var _with10 = _with9.Parameters;
                        _with10.Clear();
                        _with10.Add("BASE_CURRENCY_FK_IN", BaseCurrFk);
                        _with10.Add("TO_CURRENCY_FK_IN", DS.Tables[0].Rows[Rcnt]["CURRENCY_MST_PK"]);
                        _with10.Add("EXCHANGE_RATE_IN", DS.Tables[0].Rows[Rcnt]["ROE"]);
                        if (!string.IsNullOrEmpty(FromDt))
                        {
                            _with10.Add("FROM_DATE_IN", Convert.ToDateTime(FromDt));
                        }
                        else
                        {
                            _with10.Add("FROM_DATE_IN", System.DateTime.Now.Date);
                        }
                        _with10.Add("FROM_FLAG_IN", FromFlg);
                        _with10.Add("REF_FK_IN", (string.IsNullOrEmpty(RefFK) ? "" : RefFK));
                        _with10.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        _with9.ExecuteNonQuery();
                    }
                    TRAN.Commit();
                    arrMessage.Add("All data saved successfully");
                    return arrMessage;
                }
            }
            catch (OracleException oraexp)
            {
                arrMessage.Add(oraexp.Message);
                TRAN.Rollback();
                return arrMessage;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
            return new ArrayList();
        }
        #endregion
    }
}