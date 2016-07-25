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
    public class cls_Operator_Tariff_Entry : CommonFeatures
    {
        #region "Property"

        private long _PkValue;

        public long TariffPk
        {
            get { return _PkValue; }
        }

        #endregion "Property"

        #region "Fetch Function- Main"

        public DataTable FetchHeader(string strPolPk, string strPodPk, string strContId, bool IsLCL, string strTFDate, string strTTDate, int Group = 0)
        {
            string str = null;
            WorkFlow objWF = new WorkFlow();
            DataTable dtMain = new DataTable();
            DataTable dtContainerType = new DataTable();
            DataColumn dcCol = null;
            Int16 i = default(Int16);
            Array arrPolPk = null;
            Array arrPodPk = null;
            string strCondition = null;
            arrPolPk = strPolPk.Split(',');
            arrPodPk = strPodPk.Split(',');
            if (Group == 1 | Group == 2)
            {
                str = " SELECT POLGP.PORT_GRP_MST_PK AS \"POLPK\",";
                str += " POLGP.PORT_GRP_ID AS \"POL\",";
                str += " PODGP.PORT_GRP_MST_PK AS \"PODPK\",";
                str += " PODGP.PORT_GRP_ID AS \"POD\",";
                str += " '" + strTFDate + "' AS \"VALID_FROM\",'" + strTTDate + "' AS \"VALID_TO\"";
                str += " FROM PORT_GRP_MST_TBL POLGP, PORT_GRP_MST_TBL PODGP";
                str += " WHERE(1 = 1)";
                str += " AND POLGP.PORT_GRP_MST_PK IN (" + strPolPk + ")";
                str += " AND PODGP.PORT_GRP_MST_PK IN (" + strPodPk + ")";
                str += " AND POLGP.PORT_GRP_MST_PK <> PODGP.PORT_GRP_MST_PK";
                str += " ORDER BY POLGP.PORT_GRP_ID";
            }
            else
            {
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
                str = " SELECT POL.PORT_MST_PK AS \"POLPK\",POL.PORT_ID AS \"POL\",";
                str += " POD.PORT_MST_PK AS \"PODPK\",POD.PORT_ID AS \"POD\",";
                str += " '" + strTFDate + "' AS \"VALID_FROM\",'" + strTTDate + "' AS \"VALID_TO\"";
                str += " FROM PORT_MST_TBL POL, PORT_MST_TBL POD";
                str += " WHERE (1=1)";
                str += " AND (";
                str += strCondition;
                str += " ) AND POL.BUSINESS_TYPE = 2";
                str += " AND POD.BUSINESS_TYPE = 2";
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
                        dcCol = new DataColumn(dtContainerType.Rows[i][0].ToString(), typeof(double));
                        dtMain.Columns.Add(dcCol);
                        dcCol = new DataColumn(Convert.ToString(dtContainerType.Rows[i][1]));
                        dtMain.Columns.Add(dcCol);
                    }
                }
                else
                {
                    dtMain.Columns.Add("Basis");
                    dtMain.Columns.Add("Curr");
                    dtMain.Columns.Add("Req", typeof(double));
                    dtMain.Columns.Add("BasisPK");
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

        #endregion "Fetch Function- Main"

        #region "Fetch Active Containers"

        public DataTable FetchActiveCont(string strContId = "")
        {
            string str = null;
            WorkFlow objWF = new WorkFlow();
            if (string.IsNullOrEmpty(strContId))
            {
                strContId = " AND ROWNUM <=10 ";
            }
            else
            {
                strContId = " AND CTMT.CONTAINER_TYPE_MST_ID IN (" + strContId + ") ";
            }

            str = " SELECT 'C-' || CTMT.CONTAINER_TYPE_MST_ID,CTMT.CONTAINER_TYPE_MST_ID" + " FROM CONTAINER_TYPE_MST_TBL CTMT WHERE CTMT.ACTIVE_FLAG=1" + strContId + " ORDER BY CTMT.PREFERENCES";
            try
            {
                return objWF.GetDataTable(str);
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

        #endregion "Fetch Active Containers"

        #region "Fetch Freight"

        public DataTable FetchFreight(string strPolPk, string strPodPk, string strContId, bool IsLCL, string Mode, string FromDt = "", string ToDt = "", int LCLBasis = 0, Int64 MainPk = 0, int Group = 0)
        {
            string str = null;
            WorkFlow objWF = new WorkFlow();
            DataTable dtMain = new DataTable();
            DataTable dtContainerType = new DataTable();
            DataColumn dcCol = null;
            Int16 i = default(Int16);
            Array arrPolPk = null;
            Array arrPodPk = null;
            string strNewModeCondition = null;
            string strCondition = null;
            string strThlThdPk = null;
            strThlThdPk = " select f.freight_element_mst_pk from freight_element_mst_tbl f where f.freight_element_id like 'THL' UNION";
            strThlThdPk += " select f.freight_element_mst_pk from freight_element_mst_tbl f where f.freight_element_id like 'THD'";

            arrPolPk = strPolPk.Split(',');
            arrPodPk = strPodPk.Split(',');
            if (Group == 1 | Group == 2)
            {
                //'Begin Group Block----------------------------------------------------------
                if (string.IsNullOrEmpty(strCondition))
                {
                    strCondition = " (PGL.PORT_GRP_MST_PK IN (" + strPolPk + ") AND PGD.PORT_GRP_MST_PK IN (" + strPodPk + "))";
                }
                else
                {
                    strCondition += " OR (PGL.PORT_GRP_MST_PK IN (" + strPolPk + ")AND PGD.PORT_GRP_MST_PK IN(" + strPodPk + "))";
                }
                if (!(Mode == "EDIT"))
                {
                    strNewModeCondition = " AND PGL.BIZ_TYPE = 2";
                    strNewModeCondition += " AND PGD.BIZ_TYPE = 2";
                    strNewModeCondition += " AND FMT.ACTIVE_FLAG = 1";
                    strNewModeCondition += " AND FMT.BUSINESS_TYPE IN (2,3)";
                }
                if (Mode == "NEW" | Mode == "FROMCONTRACT")
                {
                    strNewModeCondition += strNewModeCondition + " AND FMT.FREIGHT_ELEMENT_MST_PK NOT IN(" + strThlThdPk + ")";
                }
                str = " select qry.POLPK PORT_MST_PK, qry.POL POL,";
                str += " qry.PODPK PORT_MST_PK, qry.POD POD,";
                str += " qry.FREIGHT_ELEMENT_MST_PK,";
                str += " qry.FREIGHT_ELEMENT_ID, QRY.FRTFORMULA FRTFORMULA, QRY.CHARGE_BASIS,qry.chk CHK,";
                str += " QRY.CURRENCY_MST_PK, QRY.CURRENCY_ID, QRY.BASIS FROM (";

                str += " SELECT DISTINCT PGL.PORT_GRP_MST_PK POLPK, PGL.PORT_GRP_ID AS \"POL\",";
                str += " PGD.PORT_GRP_MST_PK PODPK,PGD.PORT_GRP_ID AS \"POD\", ";
                str += " FMT.FREIGHT_ELEMENT_MST_PK,FMT.FREIGHT_ELEMENT_ID,";
                str += "(CASE WHEN FMT.FREIGHT_ELEMENT_MST_PK = Q.FRT_ELEMENT_MST_FK THEN";
                str += "1";
                str += " ELSE";
                str += "0";
                str += " END) FRTFORMULA,";
                str += " DECODE(FMT.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit') CHARGE_BASIS,";
                str += " 'false' AS \"CHK\",CURR.CURRENCY_MST_PK,CURR.CURRENCY_ID,";
                if (!(Mode == "NEW"))
                {
                    if (!IsLCL)
                    {
                        str += " '' BASIS";
                    }
                    else
                    {
                        str += " DUMT.DIMENTION_ID BASIS";
                    }
                }
                else
                {
                    str += " '' BASIS";
                }
                str += " FROM FREIGHT_ELEMENT_MST_TBL FMT, ";
                str += " CURRENCY_TYPE_MST_TBL CURR,DIMENTION_UNIT_MST_TBL  DUMT,";
                //, TARIFF_TRN_SEA_FCL_LCL  TRF"
                if (MainPk == 0)
                {
                    str += " FREIGHT_CONFIG_TRN_TBL FCT,SECTOR_MST_TBL SMT, ";
                }
                str += "   (SELECT TST.FRT_ELEMENT_MST_FK";
                str += "      FROM TRADE_SUR_TBL TST";
                str += "     WHERE TST.TRADE_MST_FK IN";
                str += "  (select mst.trade_mst_pk";
                str += "      from trade_mst_tbl mst, trade_mst_trn trn";
                str += " where(mst.trade_mst_pk = trn.trade_mst_fk)";
                str += "      and trn.port_mst_fk in( SELECT PGT.PORT_MST_FK FROM PORT_GRP_TRN_TBL PGT WHERE PGT.PORT_GRP_MST_FK IN ( " + strPolPk + ") )";
                str += "       and mst.trade_mst_pk in";
                str += "          (select mst.trade_mst_pk";
                str += "            from trade_mst_tbl mst, trade_mst_trn trn";
                str += " where(mst.trade_mst_pk = trn.trade_mst_fk)";
                str += "       and trn.port_mst_fk in(SELECT PGT.PORT_MST_FK FROM PORT_GRP_TRN_TBL PGT WHERE PGT.PORT_GRP_MST_FK IN ( " + strPolPk + "))))) Q,";

                str += "  TARIFF_TRN_SEA_FCL_LCL  TRF,";
                str += "  PORT_GRP_MST_TBL       PGL,";
                str += "  PORT_GRP_MST_TBL       PGD";

                str += " WHERE (1=1)";
                str += " AND FMT.FREIGHT_ELEMENT_MST_PK = Q.FRT_ELEMENT_MST_FK(+)";
                str += " AND TRF.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK(+) ";
                if (MainPk == 0)
                {
                    str += "  AND FMT.FREIGHT_ELEMENT_MST_PK = FCT.FREIGHT_ELEMENT_FK ";
                    str += "  AND CURR.CURRENCY_MST_PK = FCT.CURRENCY_MST_FK ";
                    str += "  AND SMT.SECTOR_MST_PK = FCT.SECTOR_MST_FK ";
                }
                else
                {
                    str += " AND CURR.CURRENCY_MST_PK = " + HttpContext.Current.Session["CURRENCY_MST_PK"];
                }
                str += " AND (";
                str += strCondition + ")";
                str += strNewModeCondition;

                str += " AND FMT.CHARGE_TYPE <> 3 ";

                if (!(Mode == "NEW"))
                {
                    str += " AND (PGL.PORT_GRP_MST_PK(+) = TRF.POL_GRP_FK AND ";
                    str += " PGD.PORT_GRP_MST_PK(+) = TRF.POD_GRP_FK) ";
                    if (string.IsNullOrEmpty(ToDt))
                    {
                        str += " AND (TRF.VALID_FROM <= TO_DATE('" + FromDt + "' , '" + dateFormat + "')) ";
                    }
                    else
                    {
                        str += " AND (TRF.VALID_FROM <= TO_DATE('" + FromDt + "' , '" + dateFormat + "') AND ";
                        str += " (TRF.VALID_TO >= TO_DATE('" + ToDt + "' , '" + dateFormat + "') OR TRF.VALID_TO IS NULL)) ";
                    }
                    if (!IsLCL)
                    {
                        str += " AND DUMT.DIMENTION_UNIT_MST_PK(+) = TRF.LCL_BASIS ";
                    }
                    else
                    {
                        str += " AND DUMT.DIMENTION_UNIT_MST_PK = TRF.LCL_BASIS ";
                        str += " AND DUMT.DIMENTION_ID IN ('KGS')";
                        if (LCLBasis > 0)
                        {
                            str += " AND DUMT.DIMENTION_UNIT_MST_PK = " + LCLBasis;
                        }
                    }
                }
                str += " AND FMT.FREIGHT_ELEMENT_MST_PK IN(SELECT PARM.frt_bof_fk FROM PARAMETERS_TBL PARM)";
                str += " GROUP BY PGL.PORT_GRP_MST_PK, PGL.PORT_GRP_ID,PGD.PORT_GRP_MST_PK, PGD.PORT_GRP_ID,";
                str += " FMT.FREIGHT_ELEMENT_MST_PK,FMT.FREIGHT_ELEMENT_ID,FMT.CHARGE_BASIS,";
                str += " CURR.CURRENCY_MST_PK,CURR.CURRENCY_ID,DUMT.DIMENTION_ID,";
                str += " Q.FRT_ELEMENT_MST_FK ";
                str += " HAVING PGL.PORT_GRP_ID <> PGD.PORT_GRP_ID";

                str += "UNION ALL";
                str += " SELECT DISTINCT PGL.PORT_GRP_MST_PK POLPK, PGL.PORT_GRP_ID AS \"POL\",";
                str += " PGD.PORT_GRP_MST_PK PODPK, PGD.PORT_GRP_ID AS \"POD\", ";
                str += " FMT.FREIGHT_ELEMENT_MST_PK,FMT.FREIGHT_ELEMENT_ID,";
                str += "(CASE WHEN FMT.FREIGHT_ELEMENT_MST_PK = Q.FRT_ELEMENT_MST_FK THEN";
                str += "1";
                str += " ELSE";
                str += "0";
                str += " END) FRTFORMULA,";
                if (MainPk > 0)
                {
                    str += " DECODE(TRF.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit') CHARGE_BASIS,";
                }
                else
                {
                    str += " DECODE(FMT.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit') CHARGE_BASIS,";
                }
                str += " 'false' AS \"CHK\",CURR.CURRENCY_MST_PK,CURR.CURRENCY_ID,";
                if (!(Mode == "NEW"))
                {
                    if (!IsLCL)
                    {
                        str += " '' BASIS";
                    }
                    else
                    {
                        str += " DUMT.DIMENTION_ID BASIS";
                    }
                }
                else
                {
                    str += " '' BASIS";
                }
                str += " FROM FREIGHT_ELEMENT_MST_TBL FMT, ";
                str += " CURRENCY_TYPE_MST_TBL CURR, DIMENTION_UNIT_MST_TBL  DUMT,";
                if (MainPk == 0)
                {
                    str += "  FREIGHT_CONFIG_TRN_TBL FCT, SECTOR_MST_TBL SMT,";
                }
                str += "   (SELECT TST.FRT_ELEMENT_MST_FK";
                str += "      FROM TRADE_SUR_TBL TST";
                str += "     WHERE TST.TRADE_MST_FK IN";
                str += "  (SELECT MST.TRADE_MST_PK";
                str += "      FROM TRADE_MST_TBL MST, TRADE_MST_TRN TRN";
                str += " WHERE(MST.TRADE_MST_PK = TRN.TRADE_MST_FK)";
                str += "      AND TRN.PORT_MST_FK IN( SELECT PGT.PORT_MST_FK FROM PORT_GRP_TRN_TBL PGT WHERE PGT.PORT_GRP_MST_FK IN ( " + strPolPk + "))";
                str += "       AND MST.TRADE_MST_PK IN";
                str += "          (SELECT MST.TRADE_MST_PK";
                str += "            FROM TRADE_MST_TBL MST, TRADE_MST_TRN TRN";
                str += " WHERE(MST.TRADE_MST_PK = TRN.TRADE_MST_FK)";
                str += "      AND TRN.PORT_MST_FK IN( SELECT PGT.PORT_MST_FK FROM PORT_GRP_TRN_TBL PGT WHERE PGT.PORT_GRP_MST_FK IN ( " + strPodPk + "))))) Q,";

                str += "  TARIFF_TRN_SEA_FCL_LCL  TRF,";
                str += "  PORT_GRP_MST_TBL        PGL,";
                str += "  PORT_GRP_MST_TBL        PGD";

                str += " WHERE (1=1)";
                str += " AND FMT.FREIGHT_ELEMENT_MST_PK = Q.FRT_ELEMENT_MST_FK(+)";
                str += " AND TRF.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK(+)";
                if (MainPk == 0)
                {
                    str += "  AND FMT.FREIGHT_ELEMENT_MST_PK = FCT.FREIGHT_ELEMENT_FK ";
                    str += "  AND CURR.CURRENCY_MST_PK = FCT.CURRENCY_MST_FK ";
                    str += "  AND SMT.SECTOR_MST_PK = FCT.SECTOR_MST_FK ";
                }
                else
                {
                    str += " AND CURR.CURRENCY_MST_PK = " + HttpContext.Current.Session["CURRENCY_MST_PK"];
                }

                str += " AND (";
                str += strCondition + ")";
                str += strNewModeCondition;
                if (MainPk > 0)
                {
                    str += " AND TRF.TARIFF_MAIN_SEA_FK = " + MainPk;
                }
                str += " AND FMT.CHARGE_TYPE <> 3 ";
                if (!(Mode == "NEW"))
                {
                    str += " AND (PGL.PORT_GRP_MST_PK(+) = TRF.POL_GRP_FK AND ";
                    str += " PGD.PORT_GRP_MST_PK(+) = TRF.POD_GRP_FK) ";

                    if (string.IsNullOrEmpty(ToDt))
                    {
                        str += " AND (TRF.VALID_FROM <= TO_DATE('" + FromDt + "' , '" + dateFormat + "')) ";
                    }
                    else
                    {
                        str += " AND (TRF.VALID_FROM <= TO_DATE('" + FromDt + "' , '" + dateFormat + "') AND ";
                        str += " (TRF.VALID_TO >= TO_DATE('" + ToDt + "' , '" + dateFormat + "') OR TRF.VALID_TO IS NULL)) ";
                    }
                    if (!IsLCL)
                    {
                        str += " AND DUMT.DIMENTION_UNIT_MST_PK(+) = TRF.LCL_BASIS ";
                    }
                    else
                    {
                        str += " AND DUMT.DIMENTION_UNIT_MST_PK = TRF.LCL_BASIS ";
                        if (LCLBasis > 0)
                        {
                            str += " AND DUMT.DIMENTION_UNIT_MST_PK = " + LCLBasis;
                        }
                    }
                }
                str += " AND FMT.FREIGHT_ELEMENT_MST_PK NOT IN(SELECT PARM.frt_bof_fk FROM PARAMETERS_TBL PARM)";
                str += " GROUP BY PGL.PORT_GRP_MST_PK, PGL.PORT_GRP_ID, PGD.PORT_GRP_MST_PK, PGD.PORT_GRP_ID,";
                if (MainPk > 0)
                {
                    str += " TRF.CHARGE_BASIS,";
                }
                else
                {
                    str += " FMT.CHARGE_BASIS,";
                }
                str += " FMT.FREIGHT_ELEMENT_MST_PK,FMT.FREIGHT_ELEMENT_ID,";
                str += " CURR.CURRENCY_MST_PK,CURR.CURRENCY_ID,DUMT.DIMENTION_ID";
                str += " , Q.FRT_ELEMENT_MST_FK ";
                str += " HAVING PGL.PORT_GRP_ID <> PGD.PORT_GRP_ID";
                str += " ) qry,";
                str += " FREIGHT_ELEMENT_MST_TBL FRT";
                str += " WHERE QRY.FREIGHT_ELEMENT_MST_PK = FRT.FREIGHT_ELEMENT_MST_PK";
                //If Mode = "NEW" Then
                //    str &= vbCrLf & " AND FRT.FREIGHT_ELEMENT_MST_PK IN (SELECT FMT.FREIGHT_ELEMENT_MST_PK "
                //    str &= vbCrLf & " FROM FREIGHT_ELEMENT_MST_TBL FMT, FREIGHT_CONFIG_TRN_TBL  FCT,SECTOR_MST_TBL SMT "
                //    str &= vbCrLf & " WHERE FMT.FREIGHT_ELEMENT_MST_PK = FCT.FREIGHT_ELEMENT_FK "
                //    str &= vbCrLf & " AND SMT.SECTOR_MST_PK = FCT.SECTOR_MST_FK "
                //    str &= vbCrLf & " AND FCT.CHARGE_TYPE <> 3 "
                //    str &= vbCrLf & " ) "
                //End If
                if (Mode == "NEW")
                {
                    str += " AND FRT.FREIGHT_ELEMENT_MST_PK IN (SELECT FMT.FREIGHT_ELEMENT_MST_PK ";
                    str += " FROM FREIGHT_ELEMENT_MST_TBL FMT, FREIGHT_CONFIG_TRN_TBL  FCT ";
                    str += " WHERE FMT.FREIGHT_ELEMENT_MST_PK = FCT.FREIGHT_ELEMENT_FK ";
                    str += " ) ";
                }
                //'End Group---------------------------------------------
            }
            else
            {
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
                    //BUSINESS_TYPE = 2 :- Is the business type for SEA
                    strNewModeCondition += " AND POD.BUSINESS_TYPE = 2";
                    //BUSINESS_TYPE = 2 :- Is the business type for SEA
                    strNewModeCondition += " AND FMT.ACTIVE_FLAG = 1";
                    strNewModeCondition += " AND FMT.BUSINESS_TYPE IN (2,3)";
                }
                if (Mode == "NEW" | Mode == "FROMCONTRACT")
                {
                    strNewModeCondition += strNewModeCondition + " AND FMT.FREIGHT_ELEMENT_MST_PK NOT IN(" + strThlThdPk + ")";
                }
                str = " select qry.POLPK PORT_MST_PK, qry.POL POL,";
                str += " qry.PODPK PORT_MST_PK, qry.POD POD,";
                str += " qry.FREIGHT_ELEMENT_MST_PK,";
                str += " qry.FREIGHT_ELEMENT_ID, QRY.FRTFORMULA FRTFORMULA, QRY.CHARGE_BASIS,qry.chk CHK,";
                str += " qry.CURRENCY_MST_PK, qry.CURRENCY_ID, QRY.BASIS from (";

                str += " SELECT DISTINCT POL.PORT_MST_PK POLPK,POL.PORT_ID AS \"POL\",";
                str += " POD.PORT_MST_PK PODPK,POD.PORT_ID AS \"POD\", ";
                str += " FMT.FREIGHT_ELEMENT_MST_PK,FMT.FREIGHT_ELEMENT_ID,";
                str += "(CASE WHEN FMT.FREIGHT_ELEMENT_MST_PK = Q.FRT_ELEMENT_MST_FK THEN";
                str += "1";
                str += " ELSE";
                str += "0";
                str += " END) FRTFORMULA,";
                str += " DECODE(FMT.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit') CHARGE_BASIS,";
                str += " 'false' AS \"CHK\",CURR.CURRENCY_MST_PK,CURR.CURRENCY_ID,";
                if (!(Mode == "NEW"))
                {
                    if (!IsLCL)
                    {
                        str += " '' BASIS";
                    }
                    else
                    {
                        str += " DUMT.DIMENTION_ID BASIS";
                    }
                }
                else
                {
                    str += " '' BASIS";
                }
                str += " FROM FREIGHT_ELEMENT_MST_TBL FMT, PORT_MST_TBL POL, PORT_MST_TBL POD,";
                str += " CURRENCY_TYPE_MST_TBL CURR,DIMENTION_UNIT_MST_TBL  DUMT,";
                if (MainPk == 0)
                {
                    str += " FREIGHT_CONFIG_TRN_TBL FCT,SECTOR_MST_TBL SMT, ";
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

                if (Mode == "FROMCONTRACT")
                {
                    str += " , cont_trn_sea_fcl_lcl  TRF";
                }
                else if (!(Mode == "NEW"))
                {
                    str += " , TARIFF_TRN_SEA_FCL_LCL  TRF";
                }

                str += " WHERE (1=1)";
                str += " AND FMT.FREIGHT_ELEMENT_MST_PK = Q.FRT_ELEMENT_MST_FK(+)";
                if (MainPk == 0)
                {
                    str += "  AND FMT.FREIGHT_ELEMENT_MST_PK = FCT.FREIGHT_ELEMENT_FK ";
                    str += "  AND CURR.CURRENCY_MST_PK = FCT.CURRENCY_MST_FK ";
                    str += "  AND SMT.SECTOR_MST_PK = FCT.SECTOR_MST_FK ";
                    str += "  AND POL.PORT_MST_PK = SMT.FROM_PORT_FK ";
                    str += "  AND POD.PORT_MST_PK=SMT.TO_PORT_FK ";
                }
                else
                {
                    str += " AND CURR.CURRENCY_MST_PK = " + HttpContext.Current.Session["CURRENCY_MST_PK"];
                }
                str += " AND (";
                str += strCondition + ")";
                str += strNewModeCondition;

                str += " AND FMT.CHARGE_TYPE <> 3 ";

                if (!(Mode == "NEW"))
                {
                    str += " AND (TRF.PORT_MST_POL_FK = POL.PORT_MST_PK AND ";
                    str += " TRF.PORT_MST_POD_FK = POD.PORT_MST_PK) ";
                    if (string.IsNullOrEmpty(ToDt))
                    {
                        // str &= vbCrLf & " AND (TRF.VALID_FROM <= TO_DATE('" & FromDt & "' , '" & dateFormat & "')) "
                        str += " AND (TRF.VALID_FROM <= TO_DATE('" + FromDt + "' , '" + dateFormat + "') AND (TRF.VALID_TO >= TO_DATE(SYSDATE, '" + dateFormat + "') OR TRF.VALID_TO IS NULL)) ";
                    }
                    else
                    {
                        str += " AND (TRF.VALID_FROM <= TO_DATE('" + FromDt + "' , '" + dateFormat + "') AND ";
                        // str &= vbCrLf & " TRF.VALID_TO >= TO_DATE('" & ToDt & "' , '" & dateFormat & "') OR TRF.VALID_TO IS NULL) "
                        str += " (TRF.VALID_TO >= TO_DATE('" + ToDt + "' , '" + dateFormat + "') OR TRF.VALID_TO IS NULL) ) ";
                    }
                    str += " AND FMT.FREIGHT_ELEMENT_MST_PK(+) = TRF.FREIGHT_ELEMENT_MST_FK ";
                    if (!IsLCL)
                    {
                        str += " AND DUMT.DIMENTION_UNIT_MST_PK(+) = TRF.LCL_BASIS ";
                    }
                    else
                    {
                        str += " AND DUMT.DIMENTION_UNIT_MST_PK = TRF.LCL_BASIS ";
                        if (Mode == "FROMCONTRACT")
                        {
                            if (strContId.Trim().Length > 0)
                            {
                                str += " AND DUMT.DIMENTION_ID IN (" + strContId + ")";
                            }
                        }
                        if (LCLBasis > 0)
                        {
                            str += " AND DUMT.DIMENTION_UNIT_MST_PK = " + LCLBasis;
                        }
                    }
                }
                str += " AND FMT.FREIGHT_ELEMENT_MST_PK IN(SELECT PARM.frt_bof_fk FROM PARAMETERS_TBL PARM)";
                str += " GROUP BY POL.PORT_MST_PK,POL.PORT_ID,POD.PORT_MST_PK,POD.PORT_ID,";
                str += " FMT.FREIGHT_ELEMENT_MST_PK,FMT.FREIGHT_ELEMENT_ID,FMT.CHARGE_BASIS,";
                str += " CURR.CURRENCY_MST_PK,CURR.CURRENCY_ID,DUMT.DIMENTION_ID";
                str += " , Q.FRT_ELEMENT_MST_FK ";
                str += " HAVING POL.PORT_ID<>POD.PORT_ID";

                str += "UNION ALL";
                str += " SELECT DISTINCT POL.PORT_MST_PK POLPK,POL.PORT_ID AS \"POL\",";
                str += " POD.PORT_MST_PK PODPK,POD.PORT_ID AS \"POD\", ";
                str += " FMT.FREIGHT_ELEMENT_MST_PK,FMT.FREIGHT_ELEMENT_ID,";
                str += "(CASE WHEN FMT.FREIGHT_ELEMENT_MST_PK = Q.FRT_ELEMENT_MST_FK THEN";
                str += "1";
                str += " ELSE";
                str += "0";
                str += " END) FRTFORMULA,";
                if (MainPk > 0)
                {
                    str += " DECODE(TRF.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit') CHARGE_BASIS,";
                }
                else
                {
                    str += " DECODE(FMT.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit') CHARGE_BASIS,";
                }
                str += " 'false' AS \"CHK\",CURR.CURRENCY_MST_PK,CURR.CURRENCY_ID,";
                if (!(Mode == "NEW"))
                {
                    if (!IsLCL)
                    {
                        str += " '' BASIS";
                    }
                    else
                    {
                        str += " DUMT.DIMENTION_ID BASIS";
                    }
                }
                else
                {
                    str += " '' BASIS";
                }
                str += " FROM FREIGHT_ELEMENT_MST_TBL FMT, PORT_MST_TBL POL, PORT_MST_TBL POD,";
                str += " CURRENCY_TYPE_MST_TBL CURR, DIMENTION_UNIT_MST_TBL  DUMT,";
                //, TARIFF_TRN_SEA_FCL_LCL  TRF"
                if (MainPk == 0)
                {
                    str += "  FREIGHT_CONFIG_TRN_TBL FCT, SECTOR_MST_TBL SMT,";
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

                if (Mode == "FROMCONTRACT")
                {
                    str += " , cont_trn_sea_fcl_lcl  TRF";
                }
                else if (!(Mode == "NEW"))
                {
                    str += " , TARIFF_TRN_SEA_FCL_LCL  TRF";
                }

                str += " WHERE (1=1)";
                str += " AND FMT.FREIGHT_ELEMENT_MST_PK = Q.FRT_ELEMENT_MST_FK(+)";
                if (MainPk == 0)
                {
                    str += "  AND FMT.FREIGHT_ELEMENT_MST_PK = FCT.FREIGHT_ELEMENT_FK ";
                    str += "  AND CURR.CURRENCY_MST_PK = FCT.CURRENCY_MST_FK ";
                    str += "  AND SMT.SECTOR_MST_PK = FCT.SECTOR_MST_FK ";
                    str += "  AND POL.PORT_MST_PK = SMT.FROM_PORT_FK ";
                    str += "  AND POD.PORT_MST_PK=SMT.TO_PORT_FK ";
                }
                else
                {
                    str += " AND CURR.CURRENCY_MST_PK = " + HttpContext.Current.Session["CURRENCY_MST_PK"];
                }

                str += " AND (";
                str += strCondition + ")";
                str += strNewModeCondition;
                if (MainPk > 0)
                {
                    str += " AND TRF.TARIFF_MAIN_SEA_FK = " + MainPk;
                }
                str += " AND FMT.CHARGE_TYPE <> 3 ";
                if (!(Mode == "NEW"))
                {
                    str += " AND (TRF.PORT_MST_POL_FK = POL.PORT_MST_PK AND ";
                    str += " TRF.PORT_MST_POD_FK = POD.PORT_MST_PK) ";
                    if (string.IsNullOrEmpty(ToDt))
                    {
                        // str &= vbCrLf & " AND (TRF.VALID_FROM <= TO_DATE('" & FromDt & "' , '" & dateFormat & "')) "
                        str += " AND (TRF.VALID_FROM <= TO_DATE('" + FromDt + "' , '" + dateFormat + "') AND (TRF.VALID_TO >= TO_DATE(SYSDATE, '" + dateFormat + "') OR TRF.VALID_TO IS NULL)) ";
                    }
                    else
                    {
                        str += " AND (TRF.VALID_FROM <= TO_DATE('" + FromDt + "' , '" + dateFormat + "') AND ";
                        //str &= vbCrLf & " TRF.VALID_TO >= TO_DATE('" & ToDt & "' , '" & dateFormat & "') OR TRF.VALID_TO IS NULL) "
                        str += " (TRF.VALID_TO >= TO_DATE('" + ToDt + "' , '" + dateFormat + "') OR TRF.VALID_TO IS NULL) ) ";
                    }
                    str += " AND FMT.FREIGHT_ELEMENT_MST_PK(+) = TRF.FREIGHT_ELEMENT_MST_FK ";
                    if (!IsLCL)
                    {
                        str += " AND DUMT.DIMENTION_UNIT_MST_PK(+) = TRF.LCL_BASIS ";
                    }
                    else
                    {
                        str += " AND DUMT.DIMENTION_UNIT_MST_PK = TRF.LCL_BASIS ";
                        if (Mode == "FROMCONTRACT")
                        {
                            if (strContId.Trim().Length > 0)
                            {
                                str += " AND DUMT.DIMENTION_ID IN (" + strContId + ")";
                            }
                        }
                        if (LCLBasis > 0)
                        {
                            str += " AND DUMT.DIMENTION_UNIT_MST_PK = " + LCLBasis;
                        }
                    }
                }
                str += " AND FMT.FREIGHT_ELEMENT_MST_PK NOT IN(SELECT PARM.frt_bof_fk FROM PARAMETERS_TBL PARM)";
                str += " GROUP BY POL.PORT_MST_PK,POL.PORT_ID,POD.PORT_MST_PK,POD.PORT_ID,";
                if (MainPk > 0)
                {
                    str += " TRF.CHARGE_BASIS,";
                }
                else
                {
                    str += " FMT.CHARGE_BASIS,";
                }
                str += " FMT.FREIGHT_ELEMENT_MST_PK,FMT.FREIGHT_ELEMENT_ID,";
                str += " CURR.CURRENCY_MST_PK,CURR.CURRENCY_ID,DUMT.DIMENTION_ID";
                str += " , Q.FRT_ELEMENT_MST_FK ";
                str += " HAVING POL.PORT_ID<>POD.PORT_ID";
                str += " ) qry,";
                str += " FREIGHT_ELEMENT_MST_TBL FRT";
                str += " WHERE QRY.FREIGHT_ELEMENT_MST_PK = FRT.FREIGHT_ELEMENT_MST_PK";
                if (Mode == "NEW")
                {
                    str += " AND FRT.FREIGHT_ELEMENT_MST_PK IN (SELECT FMT.FREIGHT_ELEMENT_MST_PK ";
                    str += " FROM FREIGHT_ELEMENT_MST_TBL FMT, FREIGHT_CONFIG_TRN_TBL  FCT,SECTOR_MST_TBL SMT ";
                    str += " WHERE FMT.FREIGHT_ELEMENT_MST_PK = FCT.FREIGHT_ELEMENT_FK ";
                    str += " AND SMT.SECTOR_MST_PK = FCT.SECTOR_MST_FK ";
                    str += " AND FMT.CHARGE_TYPE <> 3 ";
                    str += " AND SMT.SECTOR_MST_PK IN (SELECT S.SECTOR_MST_PK FROM SECTOR_MST_TBL S ";
                    str += " WHERE S.FROM_PORT_FK IN ('" + strPolPk + "') ";
                    str += " AND S.TO_PORT_FK IN ('" + strPodPk + "'))) ";
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
                        dcCol = new DataColumn((dtContainerType.Rows[i][0].ToString()), typeof(double));
                        dtMain.Columns.Add(dcCol);
                        dcCol = new DataColumn(Convert.ToString(dtContainerType.Rows[i][1]), typeof(double));
                        dtMain.Columns.Add(dcCol);
                    }
                }
                else
                {
                    dtMain.Columns.Add("Curr Min", typeof(double));
                    dtMain.Columns.Add("Curr", typeof(double));

                    dtMain.Columns.Add("Req Min", typeof(double));
                    dtMain.Columns.Add("Req", typeof(double));
                    dtMain.Columns.Add("BasisPK");
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

        #endregion "Fetch Freight"

        #region "To Fetch the Thc Rates"

        public string CheckExchangeRates(DataTable dtMain)
        {
            int RwCnt = 0;
            double ExchFlag = 0;
            WorkFlow objWF = new WorkFlow();
            string str = null;
            string strMsg = null;
            strMsg = "";
            try
            {
                if (dtMain.Rows.Count > 0)
                {
                    for (RwCnt = 0; RwCnt <= dtMain.Rows.Count - 1; RwCnt++)
                    {
                        str = "SELECT GET_EX_RATE(" + dtMain.Rows[RwCnt]["CURRENCY_MST_PK"] + "," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",to_date(SYSDATE)) FROM DUAL";
                        ExchFlag = Convert.ToDouble(objWF.ExecuteScaler(str.ToString()));
                        if (ExchFlag == 0)
                        {
                            strMsg = "Exchange Rate not defined for : " + dtMain.Rows[RwCnt]["CURRENCY_ID"];
                            break; // TODO: might not be correct. Was : Exit For
                        }
                    }
                }
                return strMsg;
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

        #endregion "To Fetch the Thc Rates"

        #region "To Fetch the Thc Rates"

        public DataSet FetchThcRates(string strPolPk, string strPodPk, string strContId, bool IsLCL, Int16 ChkThc = 0, string ValidFrom = "", string ValidTo = "", int Group = 0)
        {
            string str = null;
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();
            Int16 i = default(Int16);
            Array arrPolPk = null;
            Array arrPodPk = null;
            string strCondition = null;

            arrPolPk = strPolPk.Split(',');
            arrPodPk = strPodPk.Split(',');

            if (Group == 1 | Group == 2)
            {
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
                str += " FMT.FREIGHT_ELEMENT_ID, '0' FRTFORMULA,";
                str += " DECODE(FMT.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit') CHARGE_BASIS,";

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
                if (!string.IsNullOrEmpty(ValidTo))
                {
                    str += " AND TO_DATE('" + ValidFrom + "','" + dateFormat + "')  between PTRT.VALID_FROM and PTRT.VALID_TO";
                    str += " AND  TO_DATE('" + ValidTo + "','" + dateFormat + "')  between PTRT.VALID_FROM and PTRT.VALID_TO";
                }
                else
                {
                    str += " AND  PTRT.VALID_FROM < =   to_date('" + ValidFrom + "','" + dateFormat + "')";
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
                str += " FMT.FREIGHT_ELEMENT_MST_PK,";
                str += " FMT.FREIGHT_ELEMENT_ID, '0' FRTFORMULA,";
                str += " DECODE(FMT.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit') CHARGE_BASIS,";
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
                str += " AND POL.PORT_MST_PK in(" + strPolPk + ")";
                str += " AND (PTRT.FREIGHT_ELEMENT_MST_FK in(select f.freight_element_mst_pk from freight_element_mst_tbl f where f.freight_element_id like 'THD')";
                str += " AND PTRT.PORT_MST_FK in(" + strPodPk + "))";
                str += "  AND PTRT.PORT_MST_FK = POD.PORT_MST_PK";
                str += " AND PTRT.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK(+)";

                str += "AND CTM.Container_Type_Mst_Pk=CONT.container_type_mst_fk";
                if (!string.IsNullOrEmpty(ValidTo))
                {
                    str += " AND TO_DATE('" + ValidFrom + "','" + dateFormat + "')  between PTRT.VALID_FROM and PTRT.VALID_TO";
                    str += " AND  TO_DATE('" + ValidTo + "','" + dateFormat + "')  between PTRT.VALID_FROM and PTRT.VALID_TO";
                }
                else
                {
                    str += " AND  PTRT.VALID_FROM < =   to_date('" + ValidFrom + "','" + dateFormat + "')";
                }
                if (string.IsNullOrEmpty(strContId))
                {
                    str += "AND ROWNUM <=10";
                }
                else
                {
                    str += "AND ctm.container_type_mst_id in(" + strContId + ")";
                }
            }
            else
            {
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
                str += " FMT.FREIGHT_ELEMENT_ID, '0' FRTFORMULA,";
                str += " DECODE(FMT.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit') CHARGE_BASIS,";

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
                if (!string.IsNullOrEmpty(ValidTo))
                {
                    str += " AND TO_DATE('" + ValidFrom + "','" + dateFormat + "')  between PTRT.VALID_FROM and PTRT.VALID_TO";
                    str += " AND  TO_DATE('" + ValidTo + "','" + dateFormat + "')  between PTRT.VALID_FROM and PTRT.VALID_TO";
                }
                else
                {
                    str += " AND  PTRT.VALID_FROM < =   to_date('" + ValidFrom + "','" + dateFormat + "')";
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
                str += " FMT.FREIGHT_ELEMENT_MST_PK,";
                str += " FMT.FREIGHT_ELEMENT_ID, '0' FRTFORMULA,";
                str += " DECODE(FMT.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit') CHARGE_BASIS,";
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
                str += " AND POL.PORT_MST_PK in(" + strPolPk + ")";
                str += " AND (PTRT.FREIGHT_ELEMENT_MST_FK in(select f.freight_element_mst_pk from freight_element_mst_tbl f where f.freight_element_id like 'THD')";
                str += " AND PTRT.PORT_MST_FK in(" + strPodPk + "))";
                str += "  AND PTRT.PORT_MST_FK = POD.PORT_MST_PK";
                str += " AND PTRT.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK(+)";

                str += "AND CTM.Container_Type_Mst_Pk=CONT.container_type_mst_fk";
                if (!string.IsNullOrEmpty(ValidTo))
                {
                    str += " AND TO_DATE('" + ValidFrom + "','" + dateFormat + "')  between PTRT.VALID_FROM and PTRT.VALID_TO";
                    str += " AND  TO_DATE('" + ValidTo + "','" + dateFormat + "')  between PTRT.VALID_FROM and PTRT.VALID_TO";
                }
                else
                {
                    str += " AND  PTRT.VALID_FROM < =   to_date('" + ValidFrom + "','" + dateFormat + "')";
                }
                if (string.IsNullOrEmpty(strContId))
                {
                    str += "AND ROWNUM <=10";
                }
                else
                {
                    str += "AND ctm.container_type_mst_id in(" + strContId + ")";
                }
            }

            try
            {
                ds = objWF.GetDataSet(str);
                return ds;
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

        #endregion "To Fetch the Thc Rates"

        #region "To Add THL THD Elements"

        public object GetThlThd(string strPolPk, string strPodPk, string strContId, bool IsLCL, int Group = 0)
        {
            //Dim str As String
            //Dim objWF As New WorkFlow
            //Dim dtMain As New DataTable
            //Dim dtContainerType As New DataTable 'This datatable contains the active containers
            //Dim dcCol As DataColumn
            //Dim i As Int16
            //Dim arrPolPk As Array
            //Dim arrPodPk As Array
            //Dim strCondition As String
            //Dim strThlThdPk As String

            //strThlThdPk = vbCrLf & " select f.freight_element_mst_pk from freight_element_mst_tbl f where f.freight_element_id like 'THL' UNION"
            //strThlThdPk &= vbCrLf & " select f.freight_element_mst_pk from freight_element_mst_tbl f where f.freight_element_id like 'THD'"

            //arrPolPk = strPolPk.Split(',')
            //arrPodPk = strPodPk.Split(',')
            //For i = 0 To arrPolPk.Length - 1
            //    If strCondition = "" Then
            //        strCondition = " (POL.PORT_MST_PK =" & arrPolPk(i) & _
            //                        " AND POD.PORT_MST_PK =" & arrPodPk(i) & ")"
            //    Else
            //        strCondition &= " OR (POL.PORT_MST_PK =" & arrPolPk(i) & _
            //                        " AND POD.PORT_MST_PK =" & arrPodPk(i) & ")"
            //    End If
            //Next
            //str = str.Empty & " SELECT POL.PORT_MST_PK,POL.PORT_ID AS ""POL"","
            //str &= vbCrLf & " POD.PORT_MST_PK,POD.PORT_ID AS ""POD"", "
            //str &= vbCrLf & " FMT.FREIGHT_ELEMENT_MST_PK,FMT.FREIGHT_ELEMENT_ID,"
            //str &= vbCrLf & " 'true' AS ""CHK"",CURR.CURRENCY_MST_PK,CURR.CURRENCY_ID"
            //str &= vbCrLf & " FROM FREIGHT_ELEMENT_MST_TBL FMT, PORT_MST_TBL POL, PORT_MST_TBL POD,"
            //str &= vbCrLf & " CURRENCY_TYPE_MST_TBL CURR, CORPORATE_MST_TBL CMT"
            //str &= vbCrLf & " WHERE (1=1)"
            //str &= vbCrLf & " AND ("
            //str &= vbCrLf & strCondition
            //str &= vbCrLf & " ) AND POL.BUSINESS_TYPE = 2"
            //str &= vbCrLf & " AND POD.BUSINESS_TYPE = 2"
            //str &= vbCrLf & " AND (FMT.BUSINESS_TYPE = 3 OR FMT.BUSINESS_TYPE = 2) "
            //str &= vbCrLf & " AND FMT.BY_DEFAULT  = 1"
            //' str &= vbCrLf & " AND FMT.CHARGE_BASIS <> 2"
            //str &= vbCrLf & " AND FMT.ACTIVE_FLAG = 1 "
            //str &= vbCrLf & " AND CURR.CURRENCY_MST_PK = CMT.CURRENCY_MST_FK"
            //str &= vbCrLf & " AND FMT.freight_element_mst_pk in (" & strThlThdPk & ")"
            //'modified
            //str &= vbCrLf & " AND FMT.FREIGHT_TYPE <> 0 "
            ///
            //str &= vbCrLf & " GROUP BY POL.PORT_MST_PK,POL.PORT_ID,POD.PORT_MST_PK,POD.PORT_ID,"
            //str &= vbCrLf & " FMT.FREIGHT_ELEMENT_MST_PK,FMT.FREIGHT_ELEMENT_ID,"
            //str &= vbCrLf & " CURR.CURRENCY_MST_PK,CURR.CURRENCY_ID"
            //str &= vbCrLf & " HAVING POL.PORT_ID<>POD.PORT_ID"
            //str &= vbCrLf & " ORDER BY FMT.FREIGHT_ELEMENT_ID"
            //Try
            //    dtMain = objWF.GetDataTable(str)
            //    dtContainerType = FetchActiveCont(strContId)
            //    For i = 0 To dtContainerType.Rows.Count - 1
            //        dcCol = New DataColumn((dtContainerType.Rows(i).Item(0)), GetType(Double))
            //        dtMain.Columns.Add(dcCol)
            //        dcCol = New DataColumn(CStr(dtContainerType.Rows(i).Item(1)), GetType(Double))
            //        dtMain.Columns.Add(dcCol)
            //    Next

            //    Return dtMain
            //Catch SQLEX As Exception
            //    Throw SQLEX
            //Catch ex As Exception
            //    Throw ex
            //End Try

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
            string strThlThdPk = null;

            strThlThdPk = " select f.freight_element_mst_pk from freight_element_mst_tbl f where f.freight_element_id like 'THL' UNION";
            strThlThdPk += " select f.freight_element_mst_pk from freight_element_mst_tbl f where f.freight_element_id like 'THD'";

            arrPolPk = strPolPk.Split(',');
            arrPodPk = strPodPk.Split(',');

            if (Group == 1 | Group == 2)
            {
                if (string.IsNullOrEmpty(strCondition))
                {
                    strCondition = " (PGL.PORT_GRP_MST_PK IN (" + strPolPk + ") AND PGD.PORT_GRP_MST_PK IN (" + strPodPk + "))";
                }
                else
                {
                    strCondition += " OR (PGL.PORT_GRP_MST_PK IN (" + strPolPk + ") AND PGD.PORT_GRP_MST_PK IN (" + strPodPk + "))";
                }

                str = " SELECT PGL.PORT_GRP_MST_PK PORT_MST_PK,PGL.PORT_GRP_ID AS \"POL\",";
                str += " PGD.PORT_GRP_MST_PK PORT_MST_PK, PGD.PORT_GRP_ID AS \"POD\", ";
                str += " FMT.FREIGHT_ELEMENT_MST_PK,FMT.FREIGHT_ELEMENT_ID, '0' FRTFORMULA,";
                str += " DECODE(FMT.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit') CHARGE_BASIS,";
                str += " 'true' AS \"CHK\",CURR.CURRENCY_MST_PK,CURR.CURRENCY_ID,'' BASIS, FMT.PREFERENCE ";

                str += " FROM FREIGHT_ELEMENT_MST_TBL FMT, PORT_MST_TBL POL, PORT_MST_TBL POD, PORT_GRP_MST_TBL PGL, PORT_GRP_MST_TBL PGD,";
                str += " CURRENCY_TYPE_MST_TBL CURR ";

                str += " WHERE (1=1)";

                str += " AND (";
                str += strCondition + ")";
                str += strNewModeCondition;
                str += " AND CURR.CURRENCY_MST_PK = " + HttpContext.Current.Session["CURRENCY_MST_PK"];
                str += " AND FMT.freight_element_mst_pk in (" + strThlThdPk + ")";
                str += " GROUP BY PGL.PORT_GRP_MST_PK, PGL.PORT_GRP_ID, PGD.PORT_GRP_MST_PK, PGD.PORT_GRP_ID,";
                str += " FMT.FREIGHT_ELEMENT_MST_PK,FMT.FREIGHT_ELEMENT_ID,FMT.CHARGE_BASIS,";
                str += " CURR.CURRENCY_MST_PK, CURR.CURRENCY_ID, FMT.PREFERENCE";

                str += " HAVING PGL.PORT_GRP_ID <> PGD.PORT_GRP_ID";
                str += " ORDER BY FMT.PREFERENCE";
            }
            else
            {
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
                str = " SELECT POL.PORT_MST_PK,POL.PORT_ID AS \"POL\",";
                str += " POD.PORT_MST_PK,POD.PORT_ID AS \"POD\", ";
                str += " FMT.FREIGHT_ELEMENT_MST_PK,FMT.FREIGHT_ELEMENT_ID, '0' FRTFORMULA,";
                str += " DECODE(FMT.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit') CHARGE_BASIS,";
                str += " 'true' AS \"CHK\",CURR.CURRENCY_MST_PK,CURR.CURRENCY_ID,'' BASIS, FMT.PREFERENCE ";

                str += " FROM FREIGHT_ELEMENT_MST_TBL FMT, PORT_MST_TBL POL, PORT_MST_TBL POD,";
                str += " CURRENCY_TYPE_MST_TBL CURR ";

                str += " WHERE (1=1)";
                str += " AND (";
                str += strCondition + ")";
                str += strNewModeCondition;
                str += " AND CURR.CURRENCY_MST_PK = " + HttpContext.Current.Session["CURRENCY_MST_PK"];
                str += " AND FMT.freight_element_mst_pk in (" + strThlThdPk + ")";
                str += " GROUP BY POL.PORT_MST_PK,POL.PORT_ID,POD.PORT_MST_PK,POD.PORT_ID,";
                str += " FMT.FREIGHT_ELEMENT_MST_PK,FMT.FREIGHT_ELEMENT_ID,FMT.CHARGE_BASIS,";
                str += " CURR.CURRENCY_MST_PK,CURR.CURRENCY_ID, FMT.PREFERENCE ";

                str += " HAVING POL.PORT_ID<>POD.PORT_ID";
                str += " ORDER BY FMT.PREFERENCE ";
            }
            try
            {
                dtMain = objWF.GetDataTable(str);
                dtContainerType = FetchActiveCont(strContId);
                for (i = 0; i <= dtContainerType.Rows.Count - 1; i++)
                {
                    dcCol = new DataColumn((dtContainerType.Rows[i][0].ToString()), typeof(double));
                    dtMain.Columns.Add(dcCol);
                    dcCol = new DataColumn(Convert.ToString(dtContainerType.Rows[i][1]), typeof(double));
                    dtMain.Columns.Add(dcCol);
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

        #endregion "To Add THL THD Elements"

        #region "Fetch  Container/Sector"

        public DataTable ActiveSector(string strPOLPk, string strPODPk)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            Int16 i = default(Int16);
            Array arrPolPk = null;
            Array arrPodPk = null;
            string strCondition = null;
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

        #endregion "Fetch  Container/Sector"

        #region "Fetch Tariff"

        //This function fetch the Contract from the database against the supplied Contract Pk and selected container type
        public DataSet FetchTariff(long nTariffPk, Int16 nIsLCL, int TrfType)
        {
            try
            {
                string strSQL = null;
                // FOR FCL
                if (nIsLCL == 1)
                {
                    strSQL = " SELECT DISTINCT";
                    //If TrfType = 1 Then
                    strSQL = strSQL + " OPR.OPERATOR_MST_PK,OPR.OPERATOR_ID,OPR.OPERATOR_NAME, ";
                    //End If
                    //If TrfType = 3 Or TrfType = 4 Then
                    strSQL = strSQL + " AGT.AGENT_MST_PK,AGT.AGENT_ID,AGT.AGENT_NAME, ";
                    //End If
                    strSQL = strSQL + "TRFHDR.TARIFF_REF_NO,TRFHDR.TARIFF_TYPE,TRFHDR.CONT_MAIN_SEA_FK,CONT.CONTRACT_NO,TRFHDR.TARIFF_DATE, ";
                    strSQL = strSQL + "TO_CHAR(CONT.VALID_FROM,'" + dateFormat + "') AS C_VALID_FROM, ";
                    strSQL = strSQL + "TO_CHAR(CONT.VALID_TO,'" + dateFormat + "') AS C_VALID_TO, ";
                    strSQL = strSQL + "TRFHDR.CARGO_TYPE, TRFHDR.COMMODITY_GROUP_FK, TRFHDR.VALID_FROM, ";
                    strSQL = strSQL + "TRFHDR.VALID_TO,TRFHDR.VERSION_NO,TRFHDR.ACTIVE, ";

                    strSQL = strSQL + " CASE WHEN TRFHDR.PORT_GROUP <> 0 THEN TRFTRN.POL_GRP_FK ";
                    strSQL = strSQL + " ELSE TRFTRN.PORT_MST_POL_FK";
                    strSQL = strSQL + " END PORT_MST_POL_FK,";
                    strSQL = strSQL + " CASE WHEN TRFHDR.PORT_GROUP <> 0 THEN TRFTRN.POD_GRP_FK";
                    strSQL = strSQL + " ELSE TRFTRN.PORT_MST_POD_FK";

                    strSQL = strSQL + " END PORT_MST_POD_FK,";
                    strSQL = strSQL + " TRFTRN.TARIFF_GRP_FK,";
                    strSQL = strSQL + " NVL(TRFHDR.PORT_GROUP, 0) PORT_GROUP,";
                    strSQL = strSQL + "TRFTRN.CHECK_FOR_ALL_IN_RT, TRFTRN.FREIGHT_ELEMENT_MST_FK,TRFTRN.CURRENCY_MST_FK,CURR.CURRENCY_ID, ";
                    strSQL = strSQL + "TO_CHAR(TRFTRN.VALID_FROM,'" + dateFormat + "') AS \"P_VALID_FROM\", ";
                    strSQL = strSQL + "TO_CHAR(TRFTRN.VALID_TO,'" + dateFormat + "') AS \"P_VALID_TO\", ";
                    strSQL = strSQL + "CURRS.CURRENCY_MST_PK BASE_CURRENCY_FK,CURRS.CURRENCY_ID BASE_CURRENCY_ID,TRFHDR.REMARKS, ";
                    strSQL = strSQL + "CMT.CONTAINER_TYPE_MST_ID, TRF.FCL_CURRENT_RATE,TRF.FCL_REQ_RATE,TRFHDR.TRADE_CHK, TRFHDR.TRADETHC_CHK, TRFHDR.STATUS ";
                    //If TrfType = 1 Then
                    strSQL = strSQL + "FROM OPERATOR_MST_TBL OPR, ";
                    //End If
                    //If TrfType = 3 Or TrfType = 4 Then
                    strSQL = strSQL + " AGENT_MST_TBL AGT, ";
                    //End If
                    strSQL = strSQL + " TARIFF_TRN_SEA_FCL_LCL TRFTRN,  ";
                    strSQL = strSQL + " CONT_MAIN_SEA_TBL CONT,  ";

                    strSQL = strSQL + " TARIFF_TRN_SEA_CONT_DTL TRF,  ";
                    strSQL = strSQL + " CONTAINER_TYPE_MST_TBL CMT, ";
                    strSQL = strSQL + " TARIFF_MAIN_SEA_TBL TRFHDR,CURRENCY_TYPE_MST_TBL CURR,  ";
                    strSQL = strSQL + " CURRENCY_TYPE_MST_TBL  CURRS  ";
                    strSQL = strSQL + " WHERE TRFHDR.TARIFF_MAIN_SEA_PK = TRFTRN.TARIFF_MAIN_SEA_FK ";
                    //If TrfType = 1 Then
                    strSQL = strSQL + "AND TRFHDR.OPERATOR_MST_FK=OPR.OPERATOR_MST_PK(+) ";
                    //End If
                    //If TrfType = 3 Or TrfType = 4 Then
                    strSQL = strSQL + "AND TRFHDR.AGENT_MST_FK=AGT.AGENT_MST_PK(+) ";
                    //End If
                    strSQL = strSQL + " AND TRFHDR.CONT_MAIN_SEA_FK=CONT.CONT_MAIN_SEA_PK(+)  ";
                    strSQL = strSQL + " AND CURR.CURRENCY_MST_PK = TRFTRN.CURRENCY_MST_FK ";
                    strSQL = strSQL + " AND TRF.TARIFF_TRN_SEA_FK(+) = TRFTRN.TARIFF_TRN_SEA_PK ";
                    strSQL = strSQL + " AND CMT.CONTAINER_TYPE_MST_PK(+) = TRF.CONTAINER_TYPE_MST_FK ";
                    //(+)
                    strSQL = strSQL + " AND CURRS.CURRENCY_MST_PK(+) = TRFHDR.BASE_CURRENCY_FK ";
                    //(+)
                    strSQL = strSQL + " AND TRFHDR.TARIFF_MAIN_SEA_PK = " + nTariffPk;
                }
                else
                {
                    strSQL = " SELECT DISTINCT";
                    //If TrfType = 1 Then
                    strSQL = strSQL + " OPR.OPERATOR_MST_PK,OPR.OPERATOR_ID,OPR.OPERATOR_NAME, ";
                    //End If
                    //If TrfType = 3 Or TrfType = 4 Then
                    strSQL = strSQL + " AGT.AGENT_MST_PK,AGT.AGENT_ID,AGT.AGENT_NAME, ";
                    //End If
                    strSQL = strSQL + "TRFHDR.TARIFF_REF_NO,TRFHDR.TARIFF_TYPE,TRFHDR.CONT_MAIN_SEA_FK,CONT.CONTRACT_NO,TRFHDR.TARIFF_DATE, ";
                    strSQL = strSQL + "TO_CHAR(CONT.VALID_FROM,'" + dateFormat + "') AS C_VALID_FROM, ";
                    strSQL = strSQL + "TO_CHAR(CONT.VALID_TO,'" + dateFormat + "') AS C_VALID_TO, ";
                    strSQL = strSQL + "TRFHDR.CARGO_TYPE, TRFHDR.COMMODITY_GROUP_FK, TRFHDR.VALID_FROM, ";
                    strSQL = strSQL + "TRFHDR.VALID_TO,TRFHDR.VERSION_NO,TRFHDR.ACTIVE, ";

                    strSQL = strSQL + " CASE WHEN TRFHDR.PORT_GROUP <> 0 THEN TRFTRN.POL_GRP_FK ";
                    strSQL = strSQL + " ELSE TRFTRN.PORT_MST_POL_FK";
                    strSQL = strSQL + " END PORT_MST_POL_FK,";
                    strSQL = strSQL + " CASE WHEN TRFHDR.PORT_GROUP <> 0 THEN TRFTRN.POD_GRP_FK";
                    strSQL = strSQL + " ELSE TRFTRN.PORT_MST_POD_FK";
                    strSQL = strSQL + " END PORT_MST_POD_FK,";
                    strSQL = strSQL + " TRFTRN.TARIFF_GRP_FK,";
                    strSQL = strSQL + " NVL(TRFHDR.PORT_GROUP, 0) PORT_GROUP,";
                    strSQL = strSQL + "TRFTRN.CHECK_FOR_ALL_IN_RT, TRFTRN.FREIGHT_ELEMENT_MST_FK,TRFTRN.CURRENCY_MST_FK,CURR.CURRENCY_ID, ";
                    strSQL = strSQL + "TO_CHAR(TRFTRN.VALID_FROM,'" + dateFormat + "') AS P_VALID_FROM, ";
                    strSQL = strSQL + "TO_CHAR(TRFTRN.VALID_TO,'" + dateFormat + "') AS P_VALID_TO, ";
                    strSQL = strSQL + "TRFTRN.LCL_BASIS,TRFTRN.Lcl_Current_Min_Rate,TRFTRN.LCL_CURRENT_RATE LCL_CURRENT_RATE,TRFHDR.REMARKS, ";
                    strSQL = strSQL + "TRFTRN.Lcl_Tariff_Min_Rate,TRFTRN.LCL_TARIFF_RATE,DMT.DIMENTION_ID,TRFHDR.TRADE_CHK, TRFHDR.TRADETHC_CHK,CURRS.CURRENCY_MST_PK BASE_CURRENCY_FK, CURRS.CURRENCY_ID BASE_CURRENCY_ID, TRFHDR.STATUS FROM ";
                    //Added by Rabbani raeson USS Gap,introduced New column as "Tariff.Min.Rate"
                    //If TrfType = 1 Then
                    strSQL = strSQL + " OPERATOR_MST_TBL OPR, ";
                    //End If
                    //If TrfType = 3 Or TrfType = 4 Then
                    strSQL = strSQL + " AGENT_MST_TBL AGT, ";
                    //End If
                    strSQL = strSQL + "TARIFF_TRN_SEA_FCL_LCL TRFTRN,TARIFF_MAIN_SEA_TBL TRFHDR, ";
                    strSQL = strSQL + "CURRENCY_TYPE_MST_TBL CURR,DIMENTION_UNIT_MST_TBL DMT,CONT_MAIN_SEA_TBL CONT, ";
                    strSQL = strSQL + "CURRENCY_TYPE_MST_TBL  CURRS ";
                    strSQL = strSQL + "WHERE TRFHDR.TARIFF_MAIN_SEA_PK = TRFTRN.TARIFF_MAIN_SEA_FK ";
                    //If TrfType = 1 Then
                    strSQL = strSQL + "AND TRFHDR.OPERATOR_MST_FK=OPR.OPERATOR_MST_PK(+) ";
                    //End If
                    //If TrfType = 3 Or TrfType = 4 Then
                    strSQL = strSQL + "AND TRFHDR.AGENT_MST_FK=AGT.AGENT_MST_PK(+) ";
                    //End If
                    strSQL = strSQL + "AND TRFHDR.CONT_MAIN_SEA_FK=CONT.CONT_MAIN_SEA_PK(+)  ";
                    strSQL = strSQL + "AND CURR.CURRENCY_MST_PK = TRFTRN.CURRENCY_MST_FK ";
                    strSQL = strSQL + "AND DMT.DIMENTION_UNIT_MST_PK(+) = TRFTRN.LCL_BASIS ";
                    strSQL = strSQL + "AND CURRS.CURRENCY_MST_PK(+) = TRFHDR.BASE_CURRENCY_FK  ";
                    strSQL = strSQL + "AND TRFHDR.TARIFF_MAIN_SEA_PK =" + nTariffPk;
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

        #endregion "Fetch Tariff"

        #region "Find wheather an active contract exists for as particular Operator In the given period for a given commodity"

        public object FetchFromContract(string strPOL, string strPOD, string StrCont, int OprPk, int CommodityPk, int CargoType, string strFDate, string strTDate, int TariffType, int Group = 0)
        {
            try
            {
                System.Text.StringBuilder sbCont = new System.Text.StringBuilder(5000);
                if (StrCont.Trim().Length <= 0)
                {
                    StrCont = "'0'";
                }
                string strSQL = null;
                string strCondition = null;
                if (!string.IsNullOrEmpty(strTDate))
                {
                    strCondition += "  AND TO_DATE('" + strTDate + "' , '" + dateFormat + "')";
                    strCondition += " BETWEEN TRFHDR.VALID_FROM AND DECODE(TRFHDR.VALID_TO, NULL,NULL_DATE_FORMAT,TRFHDR.VALID_TO) ";
                }
                if (CargoType == 2)
                {
                    if (StrCont == "'0'")
                    {
                    }
                    else
                    {
                        strCondition += " AND DMT.DIMENTION_ID IN ( " + StrCont + ") ";
                    }
                }

                if (CargoType == 1)
                {
                    sbCont.AppendLine("SELECT OPR.OPERATOR_MST_PK,OPR.OPERATOR_ID,OPR.OPERATOR_NAME, ");
                    sbCont.AppendLine("TRFHDR.CONTRACT_NO,TRFHDR.CONT_MAIN_SEA_PK,TRFHDR.CONTRACT_DATE, ");
                    sbCont.AppendLine("TRFHDR.CARGO_TYPE, TRFHDR.COMMODITY_GROUP_FK, TRFHDR.VALID_FROM, ");
                    sbCont.AppendLine("TRFHDR.VALID_TO, ");
                    sbCont.AppendLine("TRFTRN.PORT_MST_POL_FK,TRFTRN.PORT_MST_POD_FK,TRFTRN.CHECK_FOR_ALL_IN_RT, ");
                    sbCont.AppendLine("TRFTRN.FREIGHT_ELEMENT_MST_FK,TRFTRN.CURRENCY_MST_FK,CURR.CURRENCY_ID, ");
                    sbCont.AppendLine("'" + strFDate + "' AS \"P_VALID_FROM\", ");
                    sbCont.AppendLine("'" + strTDate + "' AS \"P_VALID_TO\", ");
                    sbCont.AppendLine("CMT.CONTAINER_TYPE_MST_ID, TRF.FCL_APP_RATE,TRF.FCL_APP_RATE");

                    sbCont.AppendLine("FROM OPERATOR_MST_TBL OPR, ");
                    sbCont.AppendLine("CONT_TRN_SEA_FCL_LCL TRFTRN,  ");
                    sbCont.AppendLine("CONT_TRN_SEA_FCL_RATES TRF,  ");
                    sbCont.AppendLine("CONTAINER_TYPE_MST_TBL CMT, ");
                    sbCont.AppendLine("CONT_MAIN_SEA_TBL TRFHDR,CURRENCY_TYPE_MST_TBL CURR  ");
                    sbCont.AppendLine("WHERE TRFHDR.CONT_MAIN_SEA_PK = TRFTRN.CONT_MAIN_SEA_FK ");
                    sbCont.AppendLine("AND TRFTRN.CONT_TRN_SEA_PK = TRF.CONT_TRN_SEA_FK ");
                    sbCont.AppendLine("AND TRFHDR.OPERATOR_MST_FK=OPR.OPERATOR_MST_PK");
                    sbCont.AppendLine("AND CURR.CURRENCY_MST_PK = TRFTRN.CURRENCY_MST_FK ");
                    sbCont.AppendLine("AND CMT.CONTAINER_TYPE_MST_PK = TRF.CONTAINER_TYPE_MST_FK ");
                    sbCont.AppendLine("AND TRFTRN.PORT_MST_POL_FK in (" + strPOL + ")");
                    sbCont.AppendLine("AND TRFTRN.PORT_MST_POD_FK in (" + strPOD + ")");
                    sbCont.AppendLine("AND TRFHDR.CARGO_TYPE = " + CargoType + " ");
                    sbCont.AppendLine("AND TRFHDR.COMMODITY_GROUP_FK = " + CommodityPk + " ");
                    sbCont.AppendLine("AND TRFHDR.CONT_APPROVED=1");
                    if (OprPk > 0)
                    {
                        sbCont.AppendLine("AND TRFHDR.OPERATOR_MST_FK = " + OprPk);
                    }
                    sbCont.AppendLine("AND TRFHDR.ACTIVE=1");
                    if (!string.IsNullOrEmpty(StrCont))
                    {
                        sbCont.AppendLine(" AND CMT.CONTAINER_TYPE_MST_ID IN ( " + StrCont + ") ");
                    }
                    sbCont.AppendLine("AND TO_DATE('" + strFDate + "','" + dateFormat + "') BETWEEN TRFHDR.VALID_FROM ");
                    sbCont.AppendLine("AND DECODE(TRFHDR.VALID_TO, NULL,NULL_DATE_FORMAT,TRFHDR.VALID_TO) ");
                    sbCont.AppendLine("" + strCondition + "");
                }
                else
                {
                    sbCont.AppendLine("SELECT OPR.OPERATOR_MST_PK,OPR.OPERATOR_ID,OPR.OPERATOR_NAME, ");
                    sbCont.AppendLine("TRFHDR.CONTRACT_NO,TRFHDR.CONT_MAIN_SEA_PK,TRFHDR.CONTRACT_DATE, ");
                    sbCont.AppendLine("TRFHDR.CARGO_TYPE, TRFHDR.COMMODITY_GROUP_FK, TRFHDR.VALID_FROM, ");
                    sbCont.AppendLine("TRFHDR.VALID_TO,TRFHDR.VERSION_NO,TRFHDR.ACTIVE, ");
                    sbCont.AppendLine("TRFTRN.PORT_MST_POL_FK,TRFTRN.PORT_MST_POD_FK,TRFTRN.CHECK_FOR_ALL_IN_RT, ");
                    sbCont.AppendLine("TRFTRN.FREIGHT_ELEMENT_MST_FK,TRFTRN.CURRENCY_MST_FK,CURR.CURRENCY_ID, ");
                    sbCont.AppendLine("'" + strFDate + "' AS P_VALID_FROM, ");
                    sbCont.AppendLine("'" + strTDate + "' AS  P_VALID_TO, ");
                    sbCont.AppendLine("TRFTRN.LCL_BASIS,TRFTRN.lcl_approved_min_rate,TRFTRN.LCL_APPROVED_RATE, ");
                    sbCont.AppendLine("TRFTRN.lcl_approved_min_rate,TRFTRN.LCL_APPROVED_RATE,DMT.DIMENTION_ID ");
                    sbCont.AppendLine("FROM OPERATOR_MST_TBL OPR, ");
                    sbCont.AppendLine("CONT_TRN_SEA_FCL_LCL TRFTRN,CONT_MAIN_SEA_TBL TRFHDR, ");
                    sbCont.AppendLine("CURRENCY_TYPE_MST_TBL CURR,DIMENTION_UNIT_MST_TBL DMT ");
                    sbCont.AppendLine("WHERE TRFHDR.CONT_MAIN_SEA_PK = TRFTRN.CONT_MAIN_SEA_FK ");
                    sbCont.AppendLine("AND OPR.OPERATOR_MST_PK = TRFHDR.OPERATOR_MST_FK ");
                    sbCont.AppendLine("AND CURR.CURRENCY_MST_PK = TRFTRN.CURRENCY_MST_FK ");
                    sbCont.AppendLine("AND DMT.DIMENTION_UNIT_MST_PK = TRFTRN.LCL_BASIS ");
                    sbCont.AppendLine("AND TRFTRN.PORT_MST_POL_FK in (" + strPOL + ")");
                    sbCont.AppendLine("AND TRFTRN.PORT_MST_POD_FK in (" + strPOD + ")");
                    sbCont.AppendLine("AND TRFHDR.CARGO_TYPE = " + CargoType + " ");
                    sbCont.AppendLine("AND TRFHDR.COMMODITY_GROUP_FK = " + CommodityPk + " ");
                    sbCont.AppendLine("AND TRFHDR.CONT_APPROVED=1");
                    if (OprPk > 0)
                    {
                        sbCont.AppendLine("AND TRFHDR.OPERATOR_MST_FK = " + OprPk);
                    }
                    sbCont.AppendLine("AND TRFHDR.ACTIVE=1  AND TRFTRN.LCL_APPROVED_RATE IS NOT NULL ");
                    sbCont.AppendLine("AND TO_DATE('" + strFDate);
                    sbCont.AppendLine("','" + dateFormat + "') BETWEEN TRFHDR.VALID_FROM ");
                    sbCont.AppendLine("AND DECODE(TRFHDR.VALID_TO, NULL,NULL_DATE_FORMAT,TRFHDR.VALID_TO)");
                    sbCont.AppendLine("" + strCondition + "");
                }

                //If TariffType <> 1 Then
                //    sbCont.AppendLine(" AND 1=2")
                //End If
                return (new WorkFlow()).GetDataSet(sbCont.ToString());
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

        #endregion "Find wheather an active contract exists for as particular Operator In the given period for a given commodity"

        #region "Check Contract"

        public Int32 CheckAvailableContracts(string ValidFrom = "", string ValidTo = "", Int32 Commodityfk = 0, short CargoType = 1)
        {
            string strSQl = null;
            string strCondition = null;
            WorkFlow objWF = new WorkFlow();
            int TotalRecords = 0;
            int ContractPk = 0;
            try
            {
                strCondition = "WHERE 1=1";

                if (ValidFrom.Length > 0)
                {
                    strCondition += " AND CONT.VALID_FROM >= TO_DATE('" + ValidFrom + "' , '" + dateFormat + "')";
                }
                if (ValidTo.Length > 0)
                {
                    strCondition += " AND CONT.VALID_TO <= TO_DATE('" + ValidTo + "' , '" + dateFormat + "')";
                }
                if ((Commodityfk == 1))
                {
                    strCondition += " AND CONT.COMMODITY_GROUP_FK = " + Commodityfk + "";
                }
                if ((CargoType == 1))
                {
                    strCondition += " AND CONT.CARGO_TYPE = " + CargoType + "";
                }
                strCondition += " AND CONT.ACTIVE = 1";
                strSQl = "SELECT COUNT(*) from CONT_MAIN_SEA_TBL CONT  ";
                strSQl += strCondition;
                TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQl));
                strSQl = "";
                if (TotalRecords > 0)
                {
                    strSQl = "SELECT CONT.CONT_MAIN_SEA_PK FROM CONT_MAIN_SEA_TBL";
                    strSQl += strCondition;
                    ContractPk = Convert.ToInt32(objWF.ExecuteScaler(strSQl));
                }
                else
                {
                    ContractPk = 0;
                }
                return ContractPk;
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

        #endregion "Check Contract"

        #region "UpDate Main"

        public ArrayList UpdateTariff(DataSet dsMain, long TariffPk, string TariffNo, string strPolPk = "", string strPodPk = "", string strLclBasis = "", string strFreightPk = "", short IntCargo = 0, string strContainerTypes = "", short IntActive = 1,
        string Remarks = "")
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleCommand UpdateCmd = new OracleCommand();
            string Update_Proc = null;
            string UserName = objWK.MyUserName;
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            M_Configuration_PK = 3023;
            try
            {
                var _with1 = objWK.MyCommand;
                _with1.Connection = objWK.MyConnection;
                _with1.Transaction = TRAN;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".TARIFF_MAIN_SEA_TBL_PKG.TARIFF_MAIN_SEA_TBL_UPD";
                _with1.Parameters.Clear();
                _with1.Parameters.Add("TARIFF_MAIN_SEA_PK_IN", Convert.ToInt64(TariffPk)).Direction = ParameterDirection.Input;
                _with1.Parameters["TARIFF_MAIN_SEA_PK_IN"].SourceVersion = DataRowVersion.Current;
                _with1.Parameters.Add("OPERATOR_MST_FK_IN", (Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["OPERATOR_MST_FK"]) == 0 ? 0 : Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["OPERATOR_MST_FK"]))).Direction = ParameterDirection.Input;
                _with1.Parameters["OPERATOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with1.Parameters.Add("AGENT_MST_FK_IN", (Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["AGENT_MST_FK"]) == 0 ? 0 : Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["AGENT_MST_FK"]))).Direction = ParameterDirection.Input;
                _with1.Parameters["AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with1.Parameters.Add("TARIFF_TYPE_IN", Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["TARIFF_TYPE"])).Direction = ParameterDirection.Input;
                _with1.Parameters["TARIFF_TYPE_IN"].SourceVersion = DataRowVersion.Current;
                _with1.Parameters.Add("TARIFF_REF_NO_IN", TariffNo).Direction = ParameterDirection.Input;
                //.Parameters.Add("TARIFF_REF_NO_IN", IIf(TariffNo = "", "", TariffNo)).Direction = ParameterDirection.Input
                _with1.Parameters.Add("TARIFF_DATE_IN", dsMain.Tables["tblMaster"].Rows[0]["TARIFF_DATE"]).Direction = ParameterDirection.Input;
                _with1.Parameters["TARIFF_DATE_IN"].SourceVersion = DataRowVersion.Current;
                _with1.Parameters.Add("VALID_FROM_IN", dsMain.Tables["tblMaster"].Rows[0]["VALID_FROM"]).Direction = ParameterDirection.Input;
                _with1.Parameters["VALID_FROM_IN"].SourceVersion = DataRowVersion.Current;
                _with1.Parameters.Add("VALID_TO_IN", dsMain.Tables["tblMaster"].Rows[0]["VALID_TO"]).Direction = ParameterDirection.Input;
                _with1.Parameters["VALID_TO_IN"].SourceVersion = DataRowVersion.Current;
                _with1.Parameters.Add("ACTIVE_IN", IntActive).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("CARGO_TYPE_IN", Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["CARGO_TYPE"])).Direction = ParameterDirection.Input;
                _with1.Parameters["CARGO_TYPE_IN"].SourceVersion = DataRowVersion.Current;
                _with1.Parameters.Add("COMMODITY_GROUP_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["COMMODITY_GROUP_FK"])).Direction = ParameterDirection.Input;
                _with1.Parameters["COMMODITY_GROUP_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with1.Parameters.Add("CONT_MAIN_SEA_FK_IN", (dsMain.Tables[0].Rows[0]["CONT_MAIN_SEA_FK"] == "0" ? "" : dsMain.Tables[0].Rows[0]["CONT_MAIN_SEA_FK"])).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("POLPK_IN", strPolPk).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("PODPK_IN", strPodPk).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("LCL_BASIS_IN", getDefault(strLclBasis, 0)).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("FREIGHTPK_IN", strFreightPk).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("CARGO_IN", IntCargo).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("REMARKS_IN", (string.IsNullOrEmpty(Remarks) ? "" : Remarks)).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("CONTAINER_TYPE_IN", strContainerTypes).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("PORT_GROUP_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["PORT_GROUP"])).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("STATUS_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["STATUS"])).Direction = ParameterDirection.Input;
                _with1.Parameters["STATUS_IN"].SourceVersion = DataRowVersion.Current;
                _with1.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("VERSION_NO_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["VERSION_NO"])).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("CONFIG_PK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 200, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with1.ExecuteNonQuery();
                if (string.Compare(Convert.ToString(objWK.MyCommand.Parameters["RETURN_VALUE"].Value), "tariff") > 0)
                {
                    arrMessage.Add(Convert.ToString(objWK.MyCommand.Parameters["RETURN_VALUE"].Value));
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    _PkValue = Convert.ToInt64(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);
                }
                arrMessage = UpdateTransaction(dsMain, TariffPk, objWK.MyCommand);
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
                //Added by sivachandran - To close the connection after Transaction
            }
            return new ArrayList();
        }

        #endregion "UpDate Main"

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
                _with2.CommandText = objWK.MyUserName + ".TARIFF_MAIN_SEA_TBL_PKG.TARIFF_TRN_SEA_FCL_LCL_UPD";
                for (nRowCnt = 0; nRowCnt <= dsMain.Tables["tblTransaction"].Rows.Count - 1; nRowCnt++)
                {
                    UpdateCmd.Parameters.Clear();
                    _with2.Parameters.Add("TARIFF_MAIN_SEA_PK_IN", Convert.ToInt64(pkValue)).Direction = ParameterDirection.Input;
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
                        _with2.Parameters.Add("LCL_BASIS_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with2.Parameters.Add("LCL_BASIS_IN", Convert.ToInt64(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_BASIS"])).Direction = ParameterDirection.Input;
                    }
                    _with2.Parameters["LCL_BASIS_IN"].SourceVersion = DataRowVersion.Current;
                    if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_CURRENT_RATE"].ToString()))
                    {
                        _with2.Parameters.Add("LCL_CURRENT_RATE_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with2.Parameters.Add("LCL_CURRENT_RATE_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_CURRENT_RATE"])).Direction = ParameterDirection.Input;
                    }
                    _with2.Parameters["LCL_CURRENT_RATE_IN"].SourceVersion = DataRowVersion.Current;
                    if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_TARIFF_RATE"].ToString()))
                    {
                        _with2.Parameters.Add("LCL_TARIFF_RATE_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with2.Parameters.Add("LCL_TARIFF_RATE_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_TARIFF_RATE"])).Direction = ParameterDirection.Input;
                    }
                    _with2.Parameters["LCL_TARIFF_RATE_IN"].SourceVersion = DataRowVersion.Current;
                    if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CONTAINER_DTL_FCL"].ToString()))
                    {
                        _with2.Parameters.Add("CONTAINER_DTL_FCL_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with2.Parameters.Add("CONTAINER_DTL_FCL_IN", (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CONTAINER_DTL_FCL"].ToString()) ? "" : dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CONTAINER_DTL_FCL"])).Direction = ParameterDirection.Input;
                    }
                    _with2.Parameters["CONTAINER_DTL_FCL_IN"].SourceVersion = DataRowVersion.Current;
                    _with2.Parameters.Add("VALID_FROM_IN", (dsMain.Tables["tblTransaction"].Rows[nRowCnt]["VALID_FROM"].ToString())).Direction = ParameterDirection.Input;
                    if (!string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["VALID_TO"].ToString()))
                    {
                        _with2.Parameters.Add("VALID_TO_IN", (dsMain.Tables["tblTransaction"].Rows[nRowCnt]["VALID_TO"])).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with2.Parameters.Add("VALID_TO_IN", "").Direction = ParameterDirection.Input;
                    }

                    //Added by Rabbani raeson USS Gap,introduced New column as "Curr.Min.Rate"
                    if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_CURRENT_MIN_RATE"].ToString()))
                    {
                        _with2.Parameters.Add("LCL_CURRENT_MIN_RATE_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with2.Parameters.Add("LCL_CURRENT_MIN_RATE_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_CURRENT_MIN_RATE"])).Direction = ParameterDirection.Input;
                    }
                    _with2.Parameters["LCL_CURRENT_MIN_RATE_IN"].SourceVersion = DataRowVersion.Current;

                    //Added by Rabbani raeson USS Gap,introduced New column as "Tariff.Min.Rate"
                    if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_TARIFF_MIN_RATE"].ToString()))
                    {
                        _with2.Parameters.Add("LCL_TARIFF_MIN_RATE_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with2.Parameters.Add("LCL_TARIFF_MIN_RATE_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_TARIFF_MIN_RATE"])).Direction = ParameterDirection.Input;
                    }
                    _with2.Parameters["LCL_TARIFF_MIN_RATE_IN"].SourceVersion = DataRowVersion.Current;

                    _with2.Parameters.Add("CHARGE_BASIS_IN", Convert.ToInt64(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CHARGE_BASIS"])).Direction = ParameterDirection.Input;
                    _with2.Parameters["CHARGE_BASIS_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["SURCHARGE"].ToString()))
                    {
                        _with2.Parameters.Add("SURCHARGE_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with2.Parameters.Add("SURCHARGE_IN", (dsMain.Tables["tblTransaction"].Rows[nRowCnt]["SURCHARGE"])).Direction = ParameterDirection.Input;
                    }

                    _with2.Parameters.Add("POL_GRP_FK_IN", (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["POL_GRP_FK"].ToString()) ? "" : dsMain.Tables["tblTransaction"].Rows[nRowCnt]["POL_GRP_FK"])).Direction = ParameterDirection.Input;
                    _with2.Parameters["POL_GRP_FK_IN"].SourceVersion = DataRowVersion.Current;
                    _with2.Parameters.Add("POD_GRP_FK_IN", (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["POD_GRP_FK"].ToString()) ? "" : dsMain.Tables["tblTransaction"].Rows[nRowCnt]["POD_GRP_FK"])).Direction = ParameterDirection.Input;
                    _with2.Parameters["POD_GRP_FK_IN"].SourceVersion = DataRowVersion.Current;
                    _with2.Parameters.Add("TARIFF_GRP_FK_IN", (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TARIFF_GRP_FK"].ToString()) ? "" : dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TARIFF_GRP_FK"])).Direction = ParameterDirection.Input;
                    _with2.Parameters["TARIFF_GRP_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with2.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 200, "RETURN_VALUE").Direction = ParameterDirection.Output;
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
                //'Exception Handling Added by Gangadhar on 13/09/2011, PTS ID: SEP-01
            }
            finally
            {
                objWK.MyCommand.Connection.Close();
            }
        }

        #endregion "Update Transaction"

        #region "Save Main"

        public ArrayList SaveTariff(DataSet dsMain, object txtTariffNo, long nPrevContractPk, long nLocationId, long nEmpId, string strPodPk = "", string strPolPk = "", string strLclBasis = "", string strFreightpk = "", string strContainerTypes = "",
        short IntCargo = 0, string Remarks = "", string AmendFlg = "0", string PreviousPK = "0", string sid = "", string polid = "", string podid = "")
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            string Insert_Proc = null;
            string UserName = objWK.MyUserName;
            string TariffNo = null;
            M_Configuration_PK = 3023;
            try
            {
                if (string.IsNullOrEmpty(txtTariffNo.ToString()))
                {
                    TariffNo = GenerateTariffNo(nLocationId, nEmpId, M_CREATED_BY_FK, sid, polid, podid);
                    if (TariffNo == "Protocol Not Defined.")
                    {
                        arrMessage.Add("Protocol Not Defined.");
                        return arrMessage;
                    }
                    else
                    {
                        txtTariffNo = TariffNo;
                    }
                }
                else
                {
                    TariffNo = txtTariffNo.ToString();
                }
                if (Convert.ToInt32(AmendFlg) == 1)
                {
                    string str = null;
                    System.DateTime ValidTo = default(System.DateTime);
                    Int32 intIns = default(Int32);
                    OracleCommand updCmdUser = new OracleCommand();
                    updCmdUser.Transaction = TRAN;
                    if (Convert.ToInt32(PreviousPK) > -1)
                    {
                        ValidTo = DateTime.Today.Date;
                        str = " UPDATE TARIFF_MAIN_SEA_TBL T SET T.VALID_TO = '" + ValidTo + "'";
                        str += " WHERE T.TARIFF_MAIN_SEA_PK=" + PreviousPK;
                        var _with3 = updCmdUser;
                        _with3.Connection = objWK.MyConnection;
                        _with3.Transaction = TRAN;
                        _with3.CommandType = CommandType.Text;
                        _with3.CommandText = str;
                        intIns = _with3.ExecuteNonQuery();
                        str = "";
                        str = " UPDATE TARIFF_TRN_SEA_FCL_LCL F SET F.VALID_TO = '" + ValidTo + "'";
                        str += " WHERE F.TARIFF_MAIN_SEA_FK=" + PreviousPK;
                        var _with4 = updCmdUser;
                        _with4.Connection = objWK.MyConnection;
                        _with4.Transaction = TRAN;
                        _with4.CommandType = CommandType.Text;
                        _with4.CommandText = str;
                        intIns = _with4.ExecuteNonQuery();
                    }
                }
                var _with5 = objWK.MyCommand;
                _with5.Connection = objWK.MyConnection;
                _with5.CommandType = CommandType.StoredProcedure;
                _with5.Transaction = TRAN;
                _with5.CommandText = objWK.MyUserName + ".TARIFF_MAIN_SEA_TBL_PKG.TARIFF_MAIN_SEA_TBL_INS";
                _with5.Parameters.Clear();

                _with5.Parameters.Add("OPERATOR_MST_FK_IN", (Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["OPERATOR_MST_FK"]) == 0 ? 0 : Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["OPERATOR_MST_FK"]))).Direction = ParameterDirection.Input;
                _with5.Parameters["OPERATOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with5.Parameters.Add("AGENT_MST_FK_IN", (Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["AGENT_MST_FK"]) == 0 ? 0 : Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["AGENT_MST_FK"]))).Direction = ParameterDirection.Input;
                _with5.Parameters["AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with5.Parameters.Add("TARIFF_TYPE_IN", Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["TARIFF_TYPE"])).Direction = ParameterDirection.Input;
                _with5.Parameters["TARIFF_TYPE_IN"].SourceVersion = DataRowVersion.Current;
                _with5.Parameters.Add("TARIFF_REF_NO_IN", TariffNo).Direction = ParameterDirection.Input;
                _with5.Parameters.Add("TARIFF_DATE_IN", dsMain.Tables["tblMaster"].Rows[0]["TARIFF_DATE"]).Direction = ParameterDirection.Input;
                _with5.Parameters["TARIFF_DATE_IN"].SourceVersion = DataRowVersion.Current;
                _with5.Parameters.Add("VALID_FROM_IN", dsMain.Tables["tblMaster"].Rows[0]["VALID_FROM"]).Direction = ParameterDirection.Input;
                _with5.Parameters["VALID_FROM_IN"].SourceVersion = DataRowVersion.Current;
                _with5.Parameters.Add("VALID_TO_IN", dsMain.Tables["tblMaster"].Rows[0]["VALID_TO"]).Direction = ParameterDirection.Input;
                _with5.Parameters["VALID_TO_IN"].SourceVersion = DataRowVersion.Current;
                _with5.Parameters.Add("ACTIVE_IN", Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["ACTIVE"]));
                _with5.Parameters.Add("CARGO_TYPE_IN", Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["CARGO_TYPE"])).Direction = ParameterDirection.Input;
                _with5.Parameters["CARGO_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                _with5.Parameters.Add("TRADE_CHK_IN", Convert.ToInt32(dsMain.Tables[0].Rows[0]["TRADE_CHK"])).Direction = ParameterDirection.Input;
                _with5.Parameters["TRADE_CHK_IN"].SourceVersion = DataRowVersion.Current;
                _with5.Parameters.Add("TRADETHC_CHK_IN", Convert.ToInt32(dsMain.Tables[0].Rows[0]["TRADETHC_CHK"])).Direction = ParameterDirection.Input;
                _with5.Parameters["TRADETHC_CHK_IN"].SourceVersion = DataRowVersion.Current;

                _with5.Parameters.Add("COMMODITY_GROUP_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["COMMODITY_GROUP_FK"])).Direction = ParameterDirection.Input;
                _with5.Parameters["COMMODITY_GROUP_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with5.Parameters.Add("CONT_MAIN_SEA_FK_IN", (dsMain.Tables[0].Rows[0]["CONT_MAIN_SEA_FK"] == "0" ? "" : dsMain.Tables[0].Rows[0]["CONT_MAIN_SEA_FK"])).Direction = ParameterDirection.Input;
                _with5.Parameters.Add("POLPK_IN", strPolPk).Direction = ParameterDirection.Input;
                _with5.Parameters.Add("PODPK_IN", strPodPk).Direction = ParameterDirection.Input;

                _with5.Parameters.Add("LCL_BASIS_IN", getDefault(strLclBasis, 0)).Direction = ParameterDirection.Input;
                _with5.Parameters.Add("FREIGHTPK_IN", strFreightpk).Direction = ParameterDirection.Input;
                _with5.Parameters.Add("CARGO_IN", IntCargo).Direction = ParameterDirection.Input;
                _with5.Parameters.Add("CONTAINER_TYPE_IN", strContainerTypes).Direction = ParameterDirection.Input;

                _with5.Parameters.Add("BASE_CURRENCY_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["BASE_CURRENCY_FK"])).Direction = ParameterDirection.Input;
                _with5.Parameters["BASE_CURRENCY_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with5.Parameters.Add("REMARKS_IN", (string.IsNullOrEmpty(Remarks) ? "" : Remarks)).Direction = ParameterDirection.Input;
                _with5.Parameters.Add("PORT_GROUP_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["PORT_GROUP"])).Direction = ParameterDirection.Input;
                _with5.Parameters.Add("STATUS_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["STATUS"])).Direction = ParameterDirection.Input;
                _with5.Parameters["STATUS_IN"].SourceVersion = DataRowVersion.Current;

                _with5.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                _with5.Parameters.Add("CONFIG_PK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                _with5.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 200, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with5.ExecuteNonQuery();
                if (string.Compare(Convert.ToString(objWK.MyCommand.Parameters["RETURN_VALUE"].Value), "tariff") > 0)
                {
                    arrMessage.Add(Convert.ToString(objWK.MyCommand.Parameters["RETURN_VALUE"].Value));
                    RollbackProtocolKey("OPERATOR TARIFF", Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt64(HttpContext.Current.Session["EMP_PK"]), TariffNo, System.DateTime.Now);
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    _PkValue = Convert.ToInt64(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);
                }
                arrMessage = SaveTariffTran(dsMain, _PkValue, objWK.MyCommand);
                //'
                string CurrFKs = "0";
                System.DateTime ContractDt = default(System.DateTime);
                cls_Operator_Contract objContract = new cls_Operator_Contract();
                ContractDt = Convert.ToDateTime(dsMain.Tables["tblMaster"].Rows[0]["TARIFF_DATE"]);
                for (int nRowCnt = 0; nRowCnt <= dsMain.Tables["tblTransaction"].Rows.Count - 1; nRowCnt++)
                {
                    if (dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CHECK_FOR_ALL_IN_RT"] == "1")
                    {
                        CurrFKs += "," + dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CURRENCY_MST_FK"];
                    }
                }
                objContract.UpdateTempExRate(_PkValue, objWK, CurrFKs, ContractDt, "SLTARIFF");
                //'
                if (arrMessage.Count > 0)
                {
                    RollbackProtocolKey("OPERATOR TARIFF", Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt64(HttpContext.Current.Session["EMP_PK"]), TariffNo, System.DateTime.Now);
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
                RollbackProtocolKey("OPERATOR TARIFF", Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt64(HttpContext.Current.Session["EMP_PK"]), TariffNo, System.DateTime.Now);
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                RollbackProtocolKey("OPERATOR TARIFF", Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt64(HttpContext.Current.Session["EMP_PK"]), TariffNo, System.DateTime.Now);
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
            finally
            {
                objWK.MyCommand.Connection.Close();
            }
            return new ArrayList();
        }

        #endregion "Save Main"

        #region "Deactivate"

        public ArrayList Deactivate(long TariffPk)
        {
            WorkFlow objWK = new WorkFlow();
            string strSQL = null;
            OracleTransaction TRAN = null;
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            arrMessage.Clear();
            objWK.MyCommand.Transaction = TRAN;
            strSQL = "UPDATE TARIFF_MAIN_SEA_TBL T " + " SET T.ACTIVE = 0," + " T.LAST_MODIFIED_BY_FK =" + M_CREATED_BY_FK + "," + " T.VERSION_NO = T.VERSION_NO + 1," + " T.LAST_MODIFIED_DT = SYSDATE " + " WHERE T.TARIFF_MAIN_SEA_PK = " + TariffPk;

            objWK.MyCommand.CommandType = CommandType.Text;
            objWK.MyCommand.CommandText = strSQL;
            try
            {
                objWK.MyCommand.ExecuteScalar();
                _PkValue = TariffPk;
                arrMessage.Add("All data Saved successfully");
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

        #endregion "Deactivate"

        #region "Save Child"

        public ArrayList SaveTariffTran(DataSet dsMain, long PkValue, OracleCommand InsertCmd)
        {
            Int32 nRowCnt = default(Int32);
            string retVal = null;
            Int32 RecAfct = default(Int32);
            WorkFlow objWK = new WorkFlow();
            string UserName = objWK.MyUserName;
            arrMessage.Clear();

            try
            {
                var _with6 = InsertCmd;
                _with6.CommandType = CommandType.StoredProcedure;
                _with6.CommandText = objWK.MyUserName + ".TARIFF_MAIN_SEA_TBL_PKG.TARIFF_TRN_SEA_FCL_LCL_INS";
                for (nRowCnt = 0; nRowCnt <= dsMain.Tables["tblTransaction"].Rows.Count - 1; nRowCnt++)
                {
                    InsertCmd.Parameters.Clear();
                    _with6.Parameters.Add("TARIFF_MAIN_SEA_FK_IN", Convert.ToInt64(PkValue)).Direction = ParameterDirection.Input;
                    _with6.Parameters.Add("PORT_MST_POL_FK_IN", Convert.ToInt64(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["PORT_MST_POL_FK"])).Direction = ParameterDirection.Input;
                    _with6.Parameters["PORT_MST_POL_FK_IN"].SourceVersion = DataRowVersion.Current;
                    _with6.Parameters.Add("PORT_MST_POD_FK_IN", Convert.ToInt64(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["PORT_MST_POD_FK"])).Direction = ParameterDirection.Input;
                    _with6.Parameters["PORT_MST_POD_FK_IN"].SourceVersion = DataRowVersion.Current;
                    _with6.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["FREIGHT_ELEMENT_MST_FK"])).Direction = ParameterDirection.Input;
                    _with6.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                    _with6.Parameters.Add("CHECK_FOR_ALL_IN_RT_IN", Convert.ToInt32(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CHECK_FOR_ALL_IN_RT"])).Direction = ParameterDirection.Input;
                    _with6.Parameters["CHECK_FOR_ALL_IN_RT_IN"].SourceVersion = DataRowVersion.Current;
                    _with6.Parameters.Add("CURRENCY_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CURRENCY_MST_FK"])).Direction = ParameterDirection.Input;
                    _with6.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                    if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_BASIS"].ToString()))
                    {
                        _with6.Parameters.Add("LCL_BASIS_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with6.Parameters.Add("LCL_BASIS_IN", Convert.ToInt64(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_BASIS"])).Direction = ParameterDirection.Input;
                    }
                    _with6.Parameters["LCL_BASIS_IN"].SourceVersion = DataRowVersion.Current;
                    if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_CURRENT_RATE"].ToString()))
                    {
                        _with6.Parameters.Add("LCL_CURRENT_RATE_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with6.Parameters.Add("LCL_CURRENT_RATE_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_CURRENT_RATE"])).Direction = ParameterDirection.Input;
                    }
                    _with6.Parameters["LCL_CURRENT_RATE_IN"].SourceVersion = DataRowVersion.Current;
                    if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_TARIFF_RATE"].ToString()))
                    {
                        _with6.Parameters.Add("LCL_TARIFF_RATE_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with6.Parameters.Add("LCL_TARIFF_RATE_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_TARIFF_RATE"])).Direction = ParameterDirection.Input;
                    }
                    _with6.Parameters["LCL_TARIFF_RATE_IN"].SourceVersion = DataRowVersion.Current;
                    if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CONTAINER_DTL_FCL"].ToString()))
                    {
                        _with6.Parameters.Add("CONTAINER_DTL_FCL_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with6.Parameters.Add("CONTAINER_DTL_FCL_IN", (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CONTAINER_DTL_FCL"].ToString()) ? "" : dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CONTAINER_DTL_FCL"])).Direction = ParameterDirection.Input;
                    }
                    _with6.Parameters["CONTAINER_DTL_FCL_IN"].SourceVersion = DataRowVersion.Current;
                    _with6.Parameters.Add("VALID_FROM_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["VALID_FROM"]).Direction = ParameterDirection.Input;
                    if (!string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["VALID_TO"].ToString()))
                    {
                        _with6.Parameters.Add("VALID_TO_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["VALID_TO"]).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with6.Parameters.Add("VALID_TO_IN", "").Direction = ParameterDirection.Input;
                    }

                    //Added by Rabbani raeson USS Gap,introduced New column as "Curr.Min.Rate"
                    if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_CURRENT_MIN_RATE"].ToString()))
                    {
                        _with6.Parameters.Add("LCL_CURRENT_MIN_RATE_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with6.Parameters.Add("LCL_CURRENT_MIN_RATE_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_CURRENT_MIN_RATE"])).Direction = ParameterDirection.Input;
                    }
                    _with6.Parameters["LCL_CURRENT_MIN_RATE_IN"].SourceVersion = DataRowVersion.Current;

                    //Added by Rabbani raeson USS Gap,introduced New column as "Tariff.Min.Rate"
                    if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_TARIFF_MIN_RATE"].ToString()))
                    {
                        _with6.Parameters.Add("LCL_TARIFF_MIN_RATE_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with6.Parameters.Add("LCL_TARIFF_MIN_RATE_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_TARIFF_MIN_RATE"])).Direction = ParameterDirection.Input;
                    }
                    _with6.Parameters["LCL_TARIFF_MIN_RATE_IN"].SourceVersion = DataRowVersion.Current;
                    _with6.Parameters.Add("CHARGE_BASIS_IN", Convert.ToInt64(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CHARGE_BASIS"])).Direction = ParameterDirection.Input;
                    _with6.Parameters["CHARGE_BASIS_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["SURCHARGE"].ToString()))
                    {
                        _with6.Parameters.Add("SURCHARGE_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with6.Parameters.Add("SURCHARGE_IN", (dsMain.Tables["tblTransaction"].Rows[nRowCnt]["SURCHARGE"])).Direction = ParameterDirection.Input;
                    }
                    _with6.Parameters.Add("POL_GRP_FK_IN", (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["POL_GRP_FK"].ToString()) ? "" : dsMain.Tables["tblTransaction"].Rows[nRowCnt]["POL_GRP_FK"])).Direction = ParameterDirection.Input;
                    _with6.Parameters["POL_GRP_FK_IN"].SourceVersion = DataRowVersion.Current;
                    _with6.Parameters.Add("POD_GRP_FK_IN", (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["POD_GRP_FK"].ToString()) ? "" : dsMain.Tables["tblTransaction"].Rows[nRowCnt]["POD_GRP_FK"])).Direction = ParameterDirection.Input;
                    _with6.Parameters["POD_GRP_FK_IN"].SourceVersion = DataRowVersion.Current;
                    _with6.Parameters.Add("TARIFF_GRP_FK_IN", (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TARIFF_GRP_FK"].ToString()) ? "" : dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TARIFF_GRP_FK"])).Direction = ParameterDirection.Input;
                    _with6.Parameters["TARIFF_GRP_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with6.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 200, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with6.ExecuteNonQuery();
                    //tariffTRNPK = CLng(objWK.MyCommand.Parameters["RETURN_VALUE"].Value)
                    //arrmessage = SaveContDet(objWK.MyCommand, contString, tariffTRNPK)
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
        }

        #endregion "Save Child"

        #region "Generate Protocol Value- Tariff No"

        public string GenerateTariffNo(long nLocationId, long nEmployeeId, long nCreatedBy, string SID = "", string POLID = "", string PODID = "")
        {
            string functionReturnValue = null;
            try
            {
                functionReturnValue = GenerateProtocolKey("OPERATOR TARIFF", nLocationId, nEmployeeId, DateTime.Now, "", "", POLID, nCreatedBy, new WorkFlow(), SID,
                PODID);
                return functionReturnValue;
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
            return functionReturnValue;
        }

        #endregion "Generate Protocol Value- Tariff No"

        #region "Function to retrive Port ID"

        public string retPID(string strPortID)
        {
            try
            {
                DataSet ds = null;
                Array arrPID = null;
                string strPOLPK = null;
                string strPODPK = null;
                Array arrPOLPK = null;
                Array arrPODPK = null;
                Int32 i = default(Int32);
                string strSql = "";
                string strCondition = "";
                string strPortsID = "";
                WorkFlow objWF = new WorkFlow();
                arrPID = strPortID.Split('~');
                strPOLPK = Convert.ToString(arrPID.GetValue(0));
                strPODPK = Convert.ToString(arrPID.GetValue(1));
                arrPOLPK = strPOLPK.Split(',');
                arrPODPK = strPODPK.Split(',');
                for (i = 0; i <= arrPOLPK.Length - 1; i++)
                {
                    if (string.IsNullOrEmpty(strCondition))
                    {
                        strCondition = " (POL.PORT_MST_PK =" + arrPOLPK.GetValue(i) + " AND POD.PORT_MST_PK =" + arrPODPK.GetValue(i) + ")";
                    }
                    else
                    {
                        strCondition += " OR (POL.PORT_MST_PK =" + arrPOLPK.GetValue(i) + " AND POD.PORT_MST_PK =" + arrPODPK.GetValue(i) + ")";
                    }
                }
                strSql = "SELECT POL.PORT_ID AS POL,POD.PORT_ID AS POD FROM " + "PORT_MST_TBL POl,PORT_MST_TBL POD WHERE " + strCondition;
                ds = objWF.GetDataSet(strSql);
                Int32 intRCnt = default(Int32);
                for (intRCnt = 0; intRCnt <= ds.Tables[0].Rows.Count - 1; intRCnt++)
                {
                    if (intRCnt == 0)
                    {
                        strPortsID = strPortsID + ds.Tables[0].Rows[intRCnt]["POL"] + "-";
                        strPortsID = strPortsID + ds.Tables[0].Rows[intRCnt]["POD"];
                    }
                    else
                    {
                        strPortsID = strPortsID + "," + ds.Tables[0].Rows[intRCnt]["POL"] + "-";
                        strPortsID = strPortsID + ds.Tables[0].Rows[intRCnt]["POD"];
                    }
                }
                return strPortsID;
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

        #endregion "Function to retrive Port ID"

        #region "Enhance Search"

        public string FetchAgentType(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strLOC_MST_IN = "";
            string strReq = null;
            string businessType = null;
            string strAgentType = "";
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                businessType = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                strAgentType = Convert.ToString(arr.GetValue(4));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_AGENT_PKG.GETAGENT_TYPE";
                var _with7 = SCM.Parameters;
                _with7.Add("SEARCH_IN", getDefault(strSERACH_IN, "")).Direction = ParameterDirection.Input;
                _with7.Add("LOCATION_MST_FK_IN", getDefault(strLOC_MST_IN, "")).Direction = ParameterDirection.Input;
                _with7.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with7.Add("BUSINESS_TYPE_IN", businessType).Direction = ParameterDirection.Input;
                _with7.Add("AGENT_TYPE_IN", strAgentType).Direction = ParameterDirection.Input;
                _with7.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
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

        #endregion "Enhance Search"

        #region "Fetch Contract"

        //Added by rabbani on 11/12/06
        public DataSet FetchContract(long nContractPk, Int16 nIsLCL)
        {
            try
            {
                string strSQL = null;
                if (nIsLCL == 1)
                {
                    strSQL = "SELECT OPR.OPERATOR_MST_PK,OPR.OPERATOR_ID,OPR.OPERATOR_NAME, " + "CONTHDR.CONTRACT_NO,CONTHDR.CONTRACT_DATE, " + "CONTHDR.CARGO_TYPE, CONTHDR.COMMODITY_GROUP_FK,CONTHDR.RFQ_MAIN_TBL_FK,CONTHDR.VALID_FROM, " + "CONTHDR.VALID_TO,CONTHDR.ACTIVE,CONTHDR.CONT_APPROVED,CONTHDR.VERSION_NO, " + "CONTTRN.PORT_MST_POL_FK,CONTTRN.PORT_MST_POD_FK,CONTTRN.CHECK_FOR_ALL_IN_RT, " + "CONTTRN.FREIGHT_ELEMENT_MST_FK,CONTTRN.CURRENCY_MST_FK,CURR.CURRENCY_ID, " + "TO_CHAR(CONTTRN.VALID_FROM,'" + dateFormat + "') AS \"P_VALID_FROM\", " + "TO_CHAR(CONTTRN.VALID_TO,'" + dateFormat + "') AS \"P_VALID_TO\", " + "CMT.CONTAINER_TYPE_MST_ID,CONT.FCL_CURRENT_RATE,CONT.FCL_REQ_RATE,CONT.FCL_APP_RATE,CONTHDR.TRADE_CHK, CONTHDR.TRADETHC_CHK,BCUR.CURRENCY_MST_PK BASE_CURRENCY_FK, BCUR.CURRENCY_ID BASE_CURRENCY_ID ,'' REMARKS, 0 STATUS" + "FROM OPERATOR_MST_TBL OPR, " + "CONT_TRN_SEA_FCL_LCL CONTTRN,  ";
                    // & vbCrLf & _
                    //Snigdharani - 05/11/2008 - Removing v-array
                    strSQL = strSQL + "CONT_TRN_SEA_FCL_RATES CONT,  " + "CONTAINER_TYPE_MST_TBL CMT, " + "CONT_MAIN_SEA_TBL CONTHDR,CURRENCY_TYPE_MST_TBL CURR,CURRENCY_TYPE_MST_TBL  BCUR  " + "WHERE CONTHDR.CONT_MAIN_SEA_PK = CONTTRN.CONT_MAIN_SEA_FK " + "AND OPR.OPERATOR_MST_PK = CONTHDR.OPERATOR_MST_FK " + "AND CONTTRN.CONT_TRN_SEA_PK = CONT.CONT_TRN_SEA_FK " + "AND CURR.CURRENCY_MST_PK = CONTTRN.CURRENCY_MST_FK " + "AND CMT.CONTAINER_TYPE_MST_PK = CONT.CONTAINER_TYPE_MST_FK " + "AND BCUR.CURRENCY_MST_PK(+) = CONTHDR.BASE_CURRENCY_FK " + "AND CONTHDR.CONT_MAIN_SEA_PK = " + nContractPk;
                    //Added by Rabbani raeson USS Gap,introduced New column as "App.Min.Rate"
                }
                else
                {
                    strSQL = "SELECT OPR.OPERATOR_MST_PK,OPR.OPERATOR_ID,OPR.OPERATOR_NAME, " + "CONTHDR.CONTRACT_NO,CONTHDR.CONTRACT_DATE, " + "CONTHDR.CARGO_TYPE,CONTHDR.ACTIVE,CONTHDR.CONT_APPROVED,CONTHDR.COMMODITY_GROUP_FK,CONTHDR.RFQ_MAIN_TBL_FK,CONTHDR.VALID_FROM, " + "CONTHDR.VALID_TO,CONTHDR.VERSION_NO, " + "CONTTRN.PORT_MST_POL_FK,CONTTRN.PORT_MST_POD_FK,CONTTRN.CHECK_FOR_ALL_IN_RT, " + "CONTTRN.FREIGHT_ELEMENT_MST_FK,CONTTRN.CURRENCY_MST_FK,CURR.CURRENCY_ID, " + "TO_CHAR(CONTTRN.VALID_FROM,'" + dateFormat + "') AS P_VALID_FROM, " + "TO_CHAR(CONTTRN.VALID_TO,'" + dateFormat + "') AS P_VALID_TO, " + "CONTTRN.LCL_BASIS,CONTTRN.Lcl_Approved_Min_Rate,TO_NUMBER(CONTTRN.LCL_APPROVED_RATE) LCL_APPROVED_RATE, " + "CONTTRN.Lcl_Approved_Min_Rate,CONTTRN.LCL_REQUEST_RATE,LCL_APPROVED_RATE,DMT.DIMENTION_ID,CONTHDR.TRADE_CHK, CONTHDR.TRADETHC_CHK, BCUR.CURRENCY_MST_PK BASE_CURRENCY_FK,BCUR.CURRENCY_ID BASE_CURRENCY_ID,'' REMARKS, 0 STATUS " + "FROM OPERATOR_MST_TBL OPR, " + "CONT_TRN_SEA_FCL_LCL CONTTRN,CONT_MAIN_SEA_TBL CONTHDR, " + "CURRENCY_TYPE_MST_TBL CURR,DIMENTION_UNIT_MST_TBL DMT, CURRENCY_TYPE_MST_TBL  BCUR " + "WHERE CONTHDR.CONT_MAIN_SEA_PK = CONTTRN.CONT_MAIN_SEA_FK " + "AND OPR.OPERATOR_MST_PK = CONTHDR.OPERATOR_MST_FK " + "AND CURR.CURRENCY_MST_PK = CONTTRN.CURRENCY_MST_FK " + "AND DMT.DIMENTION_UNIT_MST_PK = CONTTRN.LCL_BASIS " + "AND BCUR.CURRENCY_MST_PK(+) = CONTHDR.BASE_CURRENCY_FK " + "AND CONTHDR.CONT_MAIN_SEA_PK =" + nContractPk;
                }
                return (new WorkFlow()).GetDataSet(strSQL);
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

        //Ended by rabbani on 11/12/06

        #endregion "Fetch Contract"

        //=============================== Fetch For Pop up ===========================================

        #region "Fetch Active Containers"

        public DataTable FetchActiveCont_Popup(string strContId = "")
        {
            string str = null;
            WorkFlow objWF = new WorkFlow();
            if (string.IsNullOrEmpty(strContId))
            {
                strContId = " AND ROWNUM <=10 ";
            }
            else
            {
                strContId = " AND CTMT.CONTAINER_TYPE_MST_ID IN (" + strContId + ") ";
            }

            str = " SELECT distinct ctmt.teu_factor " + " FROM CONTAINER_TYPE_MST_TBL CTMT WHERE CTMT.ACTIVE_FLAG=1" + strContId + " ORDER BY CTMT.Teu_Factor";
            try
            {
                return objWF.GetDataTable(str);
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

        #endregion "Fetch Active Containers"

        #region "Fetch Freight"

        public DataTable FetchFreight_popup(string strPolPk, string strPodPk, string strContId, bool IsLCL)
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
            str = " SELECT";
            // str &= vbCrLf & " POD.PORT_MST_PK,POD.PORT_ID AS ""POD"", "
            str += " FMT.FREIGHT_ELEMENT_MST_PK,FMT.FREIGHT_ELEMENT_ID,";
            str += " CURR.CURRENCY_MST_PK,CURR.CURRENCY_ID";
            str += " FROM FREIGHT_ELEMENT_MST_TBL FMT, PORT_MST_TBL POL, PORT_MST_TBL POD,";
            str += " CURRENCY_TYPE_MST_TBL CURR, CORPORATE_MST_TBL CMT";
            str += " WHERE (1=1)";
            str += " AND (";
            str += strCondition;
            str += " ) AND POL.BUSINESS_TYPE = 2";
            str += " AND POD.BUSINESS_TYPE = 2";
            str += " AND (FMT.BUSINESS_TYPE = 3 OR FMT.BUSINESS_TYPE = 2) ";
            str += " AND FMT.BY_DEFAULT  = 1";
            str += " AND FMT.CHARGE_BASIS <> 2";
            str += " AND FMT.ACTIVE_FLAG = 1 ";
            str += " AND CURR.CURRENCY_MST_PK = CMT.CURRENCY_MST_FK";
            //modified
            str += " AND FMT.CHARGE_TYPE <> 3 ";
            //'
            str += " GROUP BY POL.PORT_MST_PK,POL.PORT_ID,POD.PORT_MST_PK,POD.PORT_ID,";
            str += " FMT.FREIGHT_ELEMENT_MST_PK,FMT.FREIGHT_ELEMENT_ID,";
            str += " CURR.CURRENCY_MST_PK,CURR.CURRENCY_ID";
            str += " HAVING POL.PORT_ID<>POD.PORT_ID";
            str += " ORDER BY FMT.FREIGHT_ELEMENT_ID";
            try
            {
                dtMain = objWF.GetDataTable(str);
                if (!IsLCL)
                {
                    dtContainerType = FetchActiveCont_Popup(strContId);
                    for (i = 0; i <= dtContainerType.Rows.Count - 1; i++)
                    {
                        dcCol = new DataColumn((dtContainerType.Rows[i][0].ToString()), typeof(double));
                        dtMain.Columns.Add(dcCol);
                        //dcCol = New DataColumn(CStr(dtContainerType.Rows(i).Item(1)), GetType(Double))
                        //dtMain.Columns.Add(dcCol)
                    }
                }
                else
                {
                    dtMain.Columns.Add("Basis");
                    dtMain.Columns.Add("Curr");
                    dtMain.Columns.Add("Req", typeof(double));
                    dtMain.Columns.Add("BasisPK");
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

        #endregion "Fetch Freight"

        #region "Fetch Data For Teus"

        public DataSet FetchTeus()
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            //If strContId = "" Then
            //    strContId = " AND ROWNUM <=10 "
            //Else
            //    strContId = " AND CTMT.CONTAINER_TYPE_MST_ID IN (" & strContId & ") "
            //End If

            strQuery.Append("SELECT ctmt.container_type_mst_id as CONT_TYPE, ctmt.teu_factor ");
            strQuery.Append("  FROM CONTAINER_TYPE_MST_TBL CTMT");
            strQuery.Append(" WHERE CTMT.ACTIVE_FLAG = 1");
            strQuery.Append(" ORDER BY CTMT.PREFERENCES");
            strQuery.Append("");

            try
            {
                return objWF.GetDataSet(strQuery.ToString());
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

        #endregion "Fetch Data For Teus"

        #region "Fetch BOF pk"

        public string Fetch_BOF_pk()
        {
            string strSQL = null;
            strSQL = "select bof.frt_bof_fk from parameters_tbl bof";
            try
            {
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

        #endregion "Fetch BOF pk"

        #region "FetchFreightName"

        public string FetchFreightName(string BOF_pk)
        {
            string strSQL = null;
            strSQL = "select frt.freight_element_id from freight_element_mst_tbl frt where frt.freight_element_mst_pk=" + BOF_pk;
            try
            {
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

        #endregion "FetchFreightName"

        #region "FetchFormulaForFreight" 'Snigdharani> To Fetch Surcharge Percentages

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

        #endregion "FetchFormulaForFreight" 'Snigdharani> To Fetch Surcharge Percentages

        #region "FetchBasisPk"

        //Snigdharani -- To fetch the basis pk
        public int fetchBasisPk(int BasisID)
        {
            try
            {
                string sql = null;
                sql = "select dumt.dimention_unit_mst_pk from dimention_unit_mst_tbl dumt where dumt.dimention_id = '" + BasisID + "'";
                WorkFlow objWF = new WorkFlow();
                return Convert.ToInt32(objWF.ExecuteScaler(sql));
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

        #endregion "FetchBasisPk"

        #region "INSERT CONTAINER DETAILS FOR FREIGHT ELEMENT"

        //SNIGDHARANI - 28/10/2008 - REMOVING V-ARRAY TASK....
        public ArrayList SaveContDet(OracleCommand InsertCmd, string ContString = "", int TariffTRnPK = 0)
        {
            try
            {
                WorkFlow objWK = new WorkFlow();
                var _with8 = InsertCmd;
                _with8.CommandType = CommandType.StoredProcedure;
                _with8.CommandText = objWK.MyUserName + ".TARIFF_MAIN_SEA_TBL_PKG.TARIFF_TRN_SEA_FCL_LCL_INS";
                InsertCmd.Parameters.Clear();
                if (string.IsNullOrEmpty(ContString))
                {
                    _with8.Parameters.Add("CONTAINER_STRING", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with8.Parameters.Add("CONTAINER_STRING", ContString).Direction = ParameterDirection.Input;
                }
                _with8.Parameters.Add("TARIFF_TRN_SEA_FK", TariffTRnPK).Direction = ParameterDirection.Input;
                _with8.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 200, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with8.ExecuteNonQuery();
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

        #endregion "INSERT CONTAINER DETAILS FOR FREIGHT ELEMENT"

        #region "FetchPrtGroup"

        public string FetchPrtGroup(int TariffMstPK = 0)
        {
            try
            {
                string strSQL = null;
                WorkFlow objWF = new WorkFlow();
                strSQL = " SELECT NVL(T.PORT_GROUP,0) PORT_GROUP FROM TARIFF_MAIN_SEA_TBL T WHERE T.TARIFF_MAIN_SEA_PK = " + TariffMstPK;
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

        #endregion "FetchPrtGroup"

        #region "PortGroup"

        public DataSet FetchFromPortGroup(int TariffMstPK = 0, int PortGrpPK = 0, string POLPK = "0")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (TariffMstPK != 0)
                {
                    sb.Append("SELECT P.PORT_MST_PK, P.PORT_ID, T.POL_GRP_FK POL_GRP_FK");
                    sb.Append("  FROM PORT_MST_TBL P, TARIFF_TRN_SEA_FCL_LCL T");
                    sb.Append(" WHERE P.PORT_MST_PK = T.PORT_MST_POL_FK");
                    sb.Append("   AND T.TARIFF_MAIN_SEA_FK = " + TariffMstPK);
                }
                else
                {
                    sb.Append(" SELECT P.PORT_MST_PK, P.PORT_ID, PGT.PORT_GRP_MST_FK POL_GRP_FK FROM PORT_MST_TBL P, PORT_GRP_TRN_TBL PGT WHERE P.PORT_MST_PK = PGT.PORT_MST_FK AND PGT.PORT_GRP_MST_FK =" + PortGrpPK);
                    sb.Append(" AND P.BUSINESS_TYPE = 2 AND P.ACTIVE_FLAG = 1 ");
                    if (POLPK != "0" & !string.IsNullOrEmpty(POLPK))
                    {
                        sb.Append(" AND PGT.PORT_MST_FK IN (" + POLPK + ") ");
                    }
                }

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

        public DataSet FetchToPortGroup(int TariffMstPK = 0, int PortGrpPK = 0, string PODPK = "0")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (TariffMstPK != 0)
                {
                    sb.Append("SELECT P.PORT_MST_PK, P.PORT_ID, T.POD_GRP_FK POD_GRP_FK ");
                    sb.Append("  FROM PORT_MST_TBL P, TARIFF_TRN_SEA_FCL_LCL T");
                    sb.Append(" WHERE P.PORT_MST_PK = T.PORT_MST_POD_FK");
                    sb.Append("   AND T.TARIFF_MAIN_SEA_FK = " + TariffMstPK);
                }
                else
                {
                    sb.Append(" SELECT P.PORT_MST_PK, P.PORT_ID, PGT.PORT_GRP_MST_FK POD_GRP_FK FROM PORT_MST_TBL P, PORT_GRP_TRN_TBL PGT WHERE P.PORT_MST_PK = PGT.PORT_MST_FK AND PGT.PORT_GRP_MST_FK =" + PortGrpPK);
                    sb.Append(" AND P.BUSINESS_TYPE = 2 AND P.ACTIVE_FLAG = 1");
                    if (PODPK != "0" & !string.IsNullOrEmpty(PODPK))
                    {
                        sb.Append(" AND PGT.PORT_MST_FK IN (" + PODPK + ") ");
                    }
                }
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

        public DataSet FetchTariffGrp(int TariffMstPK = 0, int PortPOLGrpPK = 0, int PortPODGrpPK = 0, string TariffPK = "0")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (TariffMstPK != 0)
                {
                    sb.Append(" SELECT POL.PORT_MST_PK   POL_PK,");
                    sb.Append("       POL.PORT_ID       POL_ID,");
                    sb.Append("       POD.PORT_MST_PK   POD_PK,");
                    sb.Append("       POD.PORT_ID       POD_ID,");
                    sb.Append("       T.POL_GRP_FK      POL_GRP_FK,");
                    sb.Append("       T.PORT_MST_POD_FK POD_GRP_FK,");
                    sb.Append("       T.TARIFF_GRP_FK   TARIFF_GRP_MST_PK");
                    sb.Append("  FROM PORT_MST_TBL POL, PORT_MST_TBL POD, TARIFF_TRN_SEA_FCL_LCL T");
                    sb.Append(" WHERE T.PORT_MST_POL_FK = POL.PORT_MST_PK");
                    sb.Append("   AND T.PORT_MST_POD_FK = POD.PORT_MST_PK");
                    sb.Append("   AND T.TARIFF_MAIN_SEA_FK =" + TariffMstPK);
                }
                else
                {
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
                }
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

        public DataSet FetchSavedGroup(int TariffMstPK = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();

            try
            {
                sb.Append("SELECT POL.PORT_MST_PK POL_PK,");
                sb.Append("       POL.PORT_ID     POD_ID,");
                sb.Append("       T.POL_GRP_FK,");
                sb.Append("       POd.PORT_MST_PK POD_PK,");
                sb.Append("       POD.PORT_ID     POD_ID,");
                sb.Append("       T.POD_GRP_FK,");
                sb.Append("       T.TARIFF_GRP_FK");
                sb.Append("  FROM PORT_MST_TBL POL, PORT_MST_TBL POD, TARIFF_TRN_SEA_FCL_LCL T");
                sb.Append(" WHERE POL.PORT_MST_PK = T.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK = T.PORT_MST_POD_FK");
                sb.Append("   AND T.TARIFF_MAIN_SEA_FK = " + TariffMstPK);

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

        #endregion "PortGroup"

        #region "Fetch Max Tariff Nr"

        public string FetchTariffNr(string strTariffNo)
        {
            try
            {
                string strSQL = null;
                strSQL = "SELECT NVL(MAX(T.TARIFF_REF_NO),0) FROM TARIFF_MAIN_SEA_TBL T " + "WHERE T.TARIFF_REF_NO LIKE '" + strTariffNo + "/%' " + "ORDER BY T.TARIFF_REF_NO";
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

        #endregion "Fetch Max Tariff Nr"
    }
}