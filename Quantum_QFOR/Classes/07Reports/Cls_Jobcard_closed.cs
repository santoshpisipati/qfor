#region "Comments"

//'***************************************************************************************************************
//'*  Company Name:
//'*  Project Title           :    QFOR
//'***************************************************************************************************************
//'*  Created By  :    Santosh on 31-May-16
//'*  Module/Project Leader   :    Santosh Pisipati
//'*  Description :
//'*  Module/Form/Class Name  :
//'*  Configuration ID        :
//'***************************************************************************************************************
//'*  Revision History
//'***************************************************************************************************************
//'*  Modified DateTime(DD-MON-YYYY)  Modified By     Remarks (Bugs Related)
//'*
//'*
//'***************************************************************************************************************

#endregion "Comments"

using Oracle.DataAccess.Client;
using System;
using System.Data;

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class Cls_Jobcard_closed : CommonFeatures
    {
        #region "To Fetch Jocard For Closed Status"

        /// <summary>
        /// Gets the query for job card closed.
        /// </summary>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <param name="Customerpk">The customerpk.</param>
        /// <param name="Jocardpk">The jocardpk.</param>
        /// <param name="POLPk">The pol pk.</param>
        /// <param name="shippinglinepk">The shippinglinepk.</param>
        /// <param name="Voyagepk">The voyagepk.</param>
        /// <param name="PODPk">The pod pk.</param>
        /// <param name="VoyNo">The voy no.</param>
        /// <param name="lngUsrLocFk">The LNG usr loc fk.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <returns></returns>
        public string GetQueryForJobCardClosed(int BizType = 0, int Process = 0, string Customerpk = "", string Jocardpk = "", string POLPk = "", string shippinglinepk = "", string Voyagepk = "", string PODPk = "", string VoyNo = "", long lngUsrLocFk = 0,
        string FromDt = "", string ToDt = "")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("select DISTINCT JOB.JOB_CARD_TRN_PK  JOBREF_PK, ");
            sb.Append("        JOB.JOBCARD_REF_NO JOBREF_NR,");
            sb.Append("        JOB.JOBCARD_DATE JOBCARD_DATE,");
            sb.Append("                (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.DD_FLAG = 'BIZ_TYPE' AND DD.CONFIG_ID = 'QFORCOMMON' AND DD.DD_VALUE = " + BizType + ") BIZ_TYPE,");
            sb.Append("                (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.DD_FLAG = 'PROCESS_TYPE' AND DD.CONFIG_ID = 'QFORCOMMON' AND DD.DD_VALUE =" + Process + ") PROCESS_TYPE,");
            sb.Append("       CUST.CUSTOMER_MST_PK,");
            sb.Append("       CUST.CUSTOMER_NAME,");
            sb.Append("       CONInv.Invoice_Date,");
            sb.Append("       TO_CHAR(P.PAYMENT_DATE,dateFormat)PAYMENT_DATE,");
            sb.Append("       TO_CHAR(C.COLLECTIONS_DATE,dateFormat)COLLECTIONS_DATE,");
            sb.Append("       TO_CHAR(JOB.JOB_CARD_CLOSED_ON,dateFormat)JOB_CARD_CLOSED_ON,");
            sb.Append("       POL.PORT_MST_PK POLPK,");
            sb.Append("       POL.PORT_NAME POL_NAME,");
            sb.Append("       POD.PORT_MST_PK PODPK,");
            sb.Append("       POD.PORT_NAME POD_NAME,");
            if (BizType == 2)
            {
                sb.Append("       OPR.OPERATOR_MST_PK OPERATORPK,");
                sb.Append("       OPR.OPERATOR_NAME OPERATOR,");
            }
            else
            {
                sb.Append("       OPR.AIRLINE_MST_PK OPERATORPK,");
                sb.Append("       OPR.AIRLINE_NAME OPERATOR,");
            }
            if (BizType == 2)
            {
                sb.Append("       VVT.VOYAGE_TRN_PK,");
                sb.Append("       V.VESSEL_ID,");
                sb.Append("       V.VESSEL_NAME,");
                sb.Append("       VVT.VOYAGE,");
                sb.Append("  DECODE(JOB.CARGO_TYPE, '1','FCL','2','LCL','4','BBC' ) CARGOTYPE");
            }
            else
            {
                sb.Append("       0 VOYAGE_TRN_PK,");
                sb.Append("       '' VESSEL_ID,");
                sb.Append("       '' VESSEL_NAME,");
                sb.Append("       JOB.VOYAGE_FLIGHT_NO VOYAGE,");
                sb.Append("         '' CARGOTYPE");
            }
            sb.Append("   from collections_tbl   C,");
            sb.Append("       COLLECTIONS_TRN_TBL    CIT,");
            sb.Append("       JOB_CARD_TRN   JOB,");
            sb.Append("       CONSOL_INVOICE_TBL     CONInv,");
            sb.Append("       CONSOL_INVOICE_TRN_TBL INVT,");
            sb.Append("       PAYMENT_TRN_TBL        PAY,");
            sb.Append("       PAYMENTS_TBL           P,");
            sb.Append("       INV_SUPPLIER_TBL       INV,");
            sb.Append("       INV_SUPPLIER_TRN_TBL   INVTR,");
            sb.Append("       PAYMENTS_MODE_TRN_TBL  Pm,");
            sb.Append("       JOB_TRN_COST   JTC,");
            sb.Append("       customer_mst_tbl       CUST,");
            sb.Append("       USER_MST_TBL         UMT,");
            sb.Append("       port_mst_tbl           POL,");
            sb.Append("       port_mst_tbl           POD,");
            if (BizType == 2)
            {
                sb.Append("       operator_mst_tbl       OPR,");
                sb.Append("       VESSEL_VOYAGE_TBL      V,");
                sb.Append("       VESSEL_VOYAGE_TRN      VVT");
            }
            else
            {
                sb.Append("       airline_mst_tbl        OPR");
            }

            sb.Append(" where C.COLLECTIONS_TBL_PK = CIT.COLLECTIONS_TBL_FK");
            sb.Append("   AND CONInv.CONSOL_INVOICE_PK = INVT.CONSOL_INVOICE_FK");
            sb.Append("   AND CIT.INVOICE_REF_NR = CONInv.INVOICE_REF_NO");
            sb.Append("   AND INVT.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK");
            sb.Append("   AND P.PAYMENT_TBL_PK = PAY.PAYMENTS_TBL_FK");
            sb.Append("   AND PAY.INV_SUPPLIER_TBL_FK = INV.INV_SUPPLIER_PK");
            sb.Append("   AND PM.PAYMENTS_TBL_FK = P.PAYMENT_TBL_PK");
            sb.Append("   AND INV.INV_SUPPLIER_PK = INVTR.INV_SUPPLIER_TBL_FK");
            sb.Append("   AND JTC.JOB_TRN_COST_PK = INVTR.JOB_TRN_EST_FK");
            sb.Append("   AND JOB.JOB_CARD_TRN_PK = JTC.JOB_CARD_TRN_FK");

            sb.Append("  AND JOB.PORT_MST_POL_FK = POL.PORT_MST_PK");
            sb.Append(" AND JOB.PORT_MST_POD_FK = POD.PORT_MST_PK");
            sb.Append(" AND JOB.CUST_CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK");
            if (BizType == 2)
            {
                sb.Append(" AND JOB.CARRIER_MST_FK = OPR.OPERATOR_MST_PK(+)");
            }
            else
            {
                sb.Append(" AND JOB.CARRIER_MST_FK = OPR.AIRLINE_MST_PK(+)");
            }

            if (BizType == 2)
            {
                sb.Append("   AND VVT.VESSEL_VOYAGE_TBL_FK = V.VESSEL_VOYAGE_TBL_PK(+)");
                sb.Append("   AND JOB.VOYAGE_TRN_FK = VVT.VOYAGE_TRN_PK(+)");
            }
            sb.Append("  AND UMT.USER_MST_PK = JOB.CREATED_BY_FK");
            if (Process == 1)
            {
                sb.Append("  AND UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk + " ");
            }
            else
            {
                sb.Append(" AND ((UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk + ") OR (UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk + " and JOB.JC_AUTO_MANUAL = 0) ");
                sb.Append("  OR (JOB.PORT_MST_POD_FK ");
                sb.Append(" IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK= " + lngUsrLocFk + ")  and JOB.JC_AUTO_MANUAL = 1)) ");
            }

            sb.Append(" AND JOB.JOB_CARD_STATUS=2");

            if (!string.IsNullOrEmpty(Customerpk) & Customerpk != "0")
            {
                sb.Append("      AND CUST.CUSTOMER_MST_PK IN(" + Customerpk + ")");
            }

            if (!string.IsNullOrEmpty(Jocardpk) & Jocardpk != "0")
            {
                sb.Append("     AND JOB.JOB_CARD_TRN_PK IN(" + Jocardpk + ")");
            }

            if (!string.IsNullOrEmpty(POLPk) & POLPk != "0")
            {
                sb.Append("   AND  POL.PORT_MST_PK IN(" + POLPk + ")");
            }

            if (!string.IsNullOrEmpty(shippinglinepk) & shippinglinepk != "0")
            {
                if (BizType == 2)
                {
                    sb.Append("   AND OPR.OPERATOR_MST_PK =" + shippinglinepk);
                }
                else
                {
                    sb.Append("   AND OPR.AIRLINE_MST_PK =" + shippinglinepk);
                }
            }

            if (!string.IsNullOrEmpty(PODPk) & PODPk != "0")
            {
                sb.Append("    AND POD.PORT_MST_PK IN(" + PODPk + ")");
            }

            if (!((FromDt == null | string.IsNullOrEmpty(FromDt)) & (ToDt == null | string.IsNullOrEmpty(ToDt))))
            {
                sb.Append("   AND JOB.JOBCARD_DATE BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)");
            }
            else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
            {
                sb.Append("   AND JOB.JOBCARD_DATE >= TO_DATE('" + FromDt + "',dateformat) ");
            }
            else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
            {
                sb.Append("   AND JOB.JOBCARD_DATE <= TO_DATE('" + ToDt + "',dateformat) ");
            }

            if (BizType == 2)
            {
                if (!string.IsNullOrEmpty(Voyagepk) & Voyagepk != "0")
                {
                    sb.Append("    AND VVT.VOYAGE_TRN_PK IN(" + Voyagepk + ")");
                }
            }
            else
            {
                if (!string.IsNullOrEmpty(VoyNo))
                {
                    sb.Append(" AND UPPER(JOB.VOYAGE_FLIGHT_NO) LIKE '" + VoyNo.Trim().ToUpper().Replace("'", "''") + "'");
                }
            }
            if (BizType > 0)
            {
                sb.Append("  AND JOB.BUSINESS_TYPE= " + BizType + " ");
            }
            if (Process > 0)
            {
                sb.Append("   AND JOB.PROCESS_TYPE = " + Process + " ");
            }
            return sb.ToString();
        }

        /// <summary>
        /// Fetches the jobcard closed.
        /// </summary>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="ExportExcel">The export excel.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <param name="Customerpk">The customerpk.</param>
        /// <param name="Jocardpk">The jocardpk.</param>
        /// <param name="POLPk">The pol pk.</param>
        /// <param name="shippinglinepk">The shippinglinepk.</param>
        /// <param name="Voyagepk">The voyagepk.</param>
        /// <param name="PODPk">The pod pk.</param>
        /// <param name="VoyNo">The voy no.</param>
        /// <param name="lngUsrLocFk">The LNG usr loc fk.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <returns></returns>
        public DataSet FetchJobcardClosed(Int32 TotalPage = 0, Int32 CurrentPage = 0, Int32 ExportExcel = 0, int BizType = 0, int Process = 0, string Customerpk = "", string Jocardpk = "", string POLPk = "", string shippinglinepk = "", string Voyagepk = "",
        string PODPk = "", string VoyNo = "", long lngUsrLocFk = 0, string FromDt = "", string ToDt = "")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            Int32 last = 0;
            Int32 start = 0;
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            string strQuery = "";
            try
            {
                if ((BizType == 0))
                {
                    strQuery = GetQueryForJobCardClosed(2, Process, Customerpk, Jocardpk, POLPk, shippinglinepk, Voyagepk, PODPk, VoyNo, lngUsrLocFk,
                    FromDt, ToDt);
                    sb.Append(strQuery);
                    sb.Append(" UNION ");
                    strQuery = "";
                    strQuery = GetQueryForJobCardClosed(1, Process, Customerpk, Jocardpk, POLPk, shippinglinepk, Voyagepk, PODPk, VoyNo, lngUsrLocFk,
                    FromDt, ToDt);
                    sb.Append(strQuery);
                }
                else
                {
                    strQuery = GetQueryForJobCardClosed(BizType, Process, Customerpk, Jocardpk, POLPk, shippinglinepk, Voyagepk, PODPk, VoyNo, lngUsrLocFk,
                    FromDt, ToDt);
                    sb.Append(strQuery);
                }
                strSQL = " select count(*) from (";
                strSQL += sb.ToString() + ")";

                TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
                TotalPage = TotalRecords / RecordsPerPage;
                if (TotalRecords % RecordsPerPage != 0)
                {
                    TotalPage += 1;
                }
                if (CurrentPage > TotalPage)
                {
                    CurrentPage = 1;
                }
                if (TotalRecords == 0)
                {
                    CurrentPage = 0;
                }
                last = CurrentPage * RecordsPerPage;
                start = (CurrentPage - 1) * RecordsPerPage + 1;

                strSQL = " SELECT * FROM (SELECT ROWNUM SR_NO, q.* FROM(";
                strSQL += sb.ToString();
                if (ExportExcel == 0)
                {
                    strSQL += " ORDER BY JOBCARD_DATE DESC )q ) WHERE SR_NO Between " + start + " and " + last;
                }
                else
                {
                    strSQL += " ORDER BY JOBCARD_DATE DESC)q ) ";
                }

                return objWF.GetDataSet(strSQL);
            }
            catch (OracleException SqlExp)
            {
                throw SqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion "To Fetch Jocard For Closed Status"

        #region "To Fetch Jobcard in open Status"

        /// <summary>
        /// Fetches the jobcard open.
        /// </summary>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="ExportExcel">The export excel.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <param name="Customerpk">The customerpk.</param>
        /// <param name="Jocardpk">The jocardpk.</param>
        /// <param name="POLPk">The pol pk.</param>
        /// <param name="shippinglinepk">The shippinglinepk.</param>
        /// <param name="Voyagepk">The voyagepk.</param>
        /// <param name="PODPk">The pod pk.</param>
        /// <param name="VoyNo">The voy no.</param>
        /// <param name="lngUsrLocFk">The LNG usr loc fk.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <returns></returns>
        public DataSet FetchJobcardOpen(Int32 TotalPage = 0, Int32 CurrentPage = 0, Int32 ExportExcel = 0, int BizType = 0, int Process = 0, string Customerpk = "", string Jocardpk = "", string POLPk = "", string shippinglinepk = "", string Voyagepk = "",
        string PODPk = "", string VoyNo = "", long lngUsrLocFk = 0, string FromDt = "", string ToDt = "")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            Int32 last = 0;
            Int32 start = 0;
            string strSQL = null;
            string strQuery = "";
            Int32 TotalRecords = default(Int32);
            try
            {
                if ((BizType == 0))
                {
                    strQuery = GetQueryForJobCardOpen(2, Process, Customerpk, Jocardpk, POLPk, shippinglinepk, Voyagepk, PODPk, VoyNo, lngUsrLocFk,
                    FromDt, ToDt);
                    sb.Append(strQuery);
                    sb.Append(" UNION ");
                    strQuery = "";
                    strQuery = GetQueryForJobCardOpen(1, Process, Customerpk, Jocardpk, POLPk, shippinglinepk, Voyagepk, PODPk, VoyNo, lngUsrLocFk,
                    FromDt, ToDt);
                    sb.Append(strQuery);
                }
                else
                {
                    strQuery = GetQueryForJobCardOpen(BizType, Process, Customerpk, Jocardpk, POLPk, shippinglinepk, Voyagepk, PODPk, VoyNo, lngUsrLocFk,
                    FromDt, ToDt);
                    sb.Append(strQuery);
                }
                strSQL = " select count(*) from (";
                strSQL += sb.ToString() + ")";
                TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
                TotalPage = TotalRecords / RecordsPerPage;
                if (TotalRecords % RecordsPerPage != 0)
                {
                    TotalPage += 1;
                }
                if (CurrentPage > TotalPage)
                {
                    CurrentPage = 1;
                }
                if (TotalRecords == 0)
                {
                    CurrentPage = 0;
                }
                last = CurrentPage * RecordsPerPage;
                start = (CurrentPage - 1) * RecordsPerPage + 1;

                strSQL = " SELECT * FROM (SELECT ROWNUM SR_NO, q.* FROM(";
                strSQL += sb.ToString();
                if (ExportExcel == 0)
                {
                    strSQL += " ORDER BY JOBCARD_DATE DESC )q ) WHERE SR_NO Between " + start + " and " + last;
                }
                else
                {
                    strSQL += " ORDER BY JOBCARD_DATE DESC )q ) ";
                }
                return objWF.GetDataSet(strSQL);
            }
            catch (OracleException SqlExp)
            {
                throw SqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        /// <summary>
        /// Gets the query for job card open.
        /// </summary>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <param name="Customerpk">The customerpk.</param>
        /// <param name="Jocardpk">The jocardpk.</param>
        /// <param name="POLPk">The pol pk.</param>
        /// <param name="shippinglinepk">The shippinglinepk.</param>
        /// <param name="Voyagepk">The voyagepk.</param>
        /// <param name="PODPk">The pod pk.</param>
        /// <param name="VoyNo">The voy no.</param>
        /// <param name="lngUsrLocFk">The LNG usr loc fk.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <returns></returns>
        public string GetQueryForJobCardOpen(int BizType = 0, int Process = 0, string Customerpk = "", string Jocardpk = "", string POLPk = "", string shippinglinepk = "", string Voyagepk = "", string PODPk = "", string VoyNo = "", long lngUsrLocFk = 0,
        string FromDt = "", string ToDt = "")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT DISTINCT JOB.JOB_CARD_TRN_PK JOBREF_PK,");
            sb.Append("                JOB.JOBCARD_REF_NO JOBREF_NR,");
            sb.Append("                JOB.JOBCARD_DATE JOBCARD_DATE,");
            sb.Append("                DECODE(JOB.BUSINESS_TYPE, 1, 'AIR', 2, 'SEA') BIZ_TYPE,");
            sb.Append("                DECODE(JOB.PROCESS_TYPE, 1, 'EXPORT', 2, 'IMPORT') PROCESS_TYPE,");
            sb.Append("                CUST.CUSTOMER_MST_PK,");
            sb.Append("                CUST.CUSTOMER_NAME,");
            sb.Append("                (SELECT TO_CHAR(CONINV.INVOICE_DATE, DATEFORMAT)");
            sb.Append("                   FROM CONSOL_INVOICE_TBL     CONINV,");
            sb.Append("                        CONSOL_INVOICE_TRN_TBL INVT");
            sb.Append("                  WHERE CONINV.CONSOL_INVOICE_PK = INVT.CONSOL_INVOICE_FK");
            sb.Append("                    AND INVT.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK");
            sb.Append("                    AND ROWNUM = 1) INVOICE_DATE,");
            sb.Append("                (SELECT TO_CHAR(P.PAYMENT_DATE, DATEFORMAT)");
            sb.Append("                   FROM PAYMENTS_TBL         P,");
            sb.Append("                        PAYMENT_TRN_TBL      PAY,");
            sb.Append("                        INV_SUPPLIER_TBL     INV,");
            sb.Append("                        INV_SUPPLIER_TRN_TBL INVTRN,");
            sb.Append("                        JOB_TRN_COST         JTC");
            sb.Append("                  WHERE P.PAYMENT_TBL_PK = PAY.PAYMENTS_TBL_FK");
            sb.Append("                    AND PAY.INV_SUPPLIER_TBL_FK = INV.INV_SUPPLIER_PK");
            sb.Append("                    AND INV.INV_SUPPLIER_PK = INVTRN.INV_SUPPLIER_TBL_FK");
            sb.Append("                    AND JOB.JOB_CARD_TRN_PK = JTC.JOB_CARD_TRN_FK");
            sb.Append("                    AND JTC.JOB_TRN_COST_PK = INVTRN.JOB_TRN_EST_FK");
            sb.Append("                    AND ROWNUM = 1) PAYMENT_DATE,");
            sb.Append("                (SELECT TO_CHAR(C.COLLECTIONS_DATE, DATEFORMAT)");
            sb.Append("                   FROM COLLECTIONS_TBL        C,");
            sb.Append("                        COLLECTIONS_TRN_TBL    CIT,");
            sb.Append("                        CONSOL_INVOICE_TRN_TBL INVT,");
            sb.Append("                        CONSOL_INVOICE_TBL     CONINV");
            sb.Append("                  WHERE C.COLLECTIONS_TBL_PK = CIT.COLLECTIONS_TBL_FK");
            sb.Append("                    AND CONINV.CONSOL_INVOICE_PK = INVT.CONSOL_INVOICE_FK");
            sb.Append("                    AND INVT.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK");
            sb.Append("                    AND ROWNUM = 1) COLLECTIONS_DATE,");
            sb.Append("                TO_CHAR(JOB.JOB_CARD_CLOSED_ON, DATEFORMAT) JOB_CARD_CLOSED_ON,");
            sb.Append("                POL.PORT_MST_PK POLPK,");
            sb.Append("                POL.PORT_NAME POL_NAME,");
            sb.Append("                POD.PORT_MST_PK PODPK,");
            sb.Append("                POD.PORT_NAME POD_NAME,");
            sb.Append("                JOB.CARRIER_MST_FK OPERATORPK,");
            sb.Append("                CASE");
            sb.Append("                  WHEN JOB.BUSINESS_TYPE = 2 THEN");
            sb.Append("                   OPR.OPERATOR_NAME");
            sb.Append("                  ELSE");
            sb.Append("                   AMT.AIRLINE_NAME");
            sb.Append("                END OPERATOR,");
            sb.Append("                NVL(JOB.VOYAGE_TRN_FK, 0) VOYAGE_TRN_PK,");
            sb.Append("                NVL(V.VESSEL_ID, '') VESSEL_ID,");
            sb.Append("                NVL(V.VESSEL_NAME, '') VESSEL_NAME,");
            sb.Append("                JOB.VOYAGE_FLIGHT_NO,");
            sb.Append("                DECODE(JOB.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGOTYPE");
            sb.Append("  FROM JOB_CARD_TRN      JOB,");
            sb.Append("       CUSTOMER_MST_TBL  CUST,");
            sb.Append("       USER_MST_TBL      UMT,");
            sb.Append("       PORT_MST_TBL      POL,");
            sb.Append("       PORT_MST_TBL      POD,");
            sb.Append("       VESSEL_VOYAGE_TBL V,");
            sb.Append("       VESSEL_VOYAGE_TRN VVT,");
            sb.Append("       OPERATOR_MST_TBL  OPR,");
            sb.Append("       AIRLINE_MST_TBL   AMT");
            sb.Append(" WHERE JOB.PORT_MST_POL_FK = POL.PORT_MST_PK");
            sb.Append("   AND JOB.PORT_MST_POD_FK = POD.PORT_MST_PK");
            sb.Append("   AND JOB.CUST_CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK");
            sb.Append("   AND JOB.CARRIER_MST_FK = OPR.OPERATOR_MST_PK(+)");
            sb.Append("   AND JOB.CARRIER_MST_FK = AMT.AIRLINE_MST_PK(+)");
            sb.Append("   AND VVT.VESSEL_VOYAGE_TBL_FK = V.VESSEL_VOYAGE_TBL_PK(+)");
            sb.Append("   AND JOB.VOYAGE_TRN_FK = VVT.VOYAGE_TRN_PK(+)");
            sb.Append("   AND UMT.USER_MST_PK = JOB.CREATED_BY_FK");
            sb.Append("   AND JOB.JOB_CARD_STATUS = 1");
            if (BizType > 0)
            {
                sb.Append("  AND JOB.BUSINESS_TYPE= " + BizType + " ");
            }
            if (Process > 0)
            {
                sb.Append("   AND JOB.PROCESS_TYPE = " + Process + " ");
            }
            if (Process == 1)
            {
                sb.Append("  AND UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk + " ");
            }
            else if (Process == 2)
            {
                sb.Append(" AND ((UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk + ") OR (UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk + " and JOB.JC_AUTO_MANUAL = 0) ");
                sb.Append("  OR (JOB.PORT_MST_POD_FK ");
                sb.Append(" IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK= " + lngUsrLocFk + ")  and JOB.JC_AUTO_MANUAL = 1)) ");
            }
            else
            {
                sb.Append(" AND ((UMT.DEFAULT_LOCATION_FK =" + lngUsrLocFk + ") OR");
                sb.Append(" (UMT.DEFAULT_LOCATION_FK =" + lngUsrLocFk + " AND JOB.JC_AUTO_MANUAL=0) OR");
                sb.Append(" (JOB.PORT_MST_POD_FK IN");
                sb.Append(" (SELECT T.PORT_MST_FK");
                sb.Append("  FROM LOC_PORT_MAPPING_TRN T");
                sb.Append("  WHERE T.LOCATION_MST_FK =" + lngUsrLocFk + ") AND");
                sb.Append(" JOB.JC_AUTO_MANUAL = 1 ))");
            }
            if (!string.IsNullOrEmpty(Customerpk) & Customerpk != "0")
            {
                sb.Append("      AND CUST.CUSTOMER_MST_PK IN(" + Customerpk + ")");
            }

            if (!string.IsNullOrEmpty(Jocardpk) & Jocardpk != "0")
            {
                sb.Append("     AND JOB.JOB_CARD_TRN_PK IN(" + Jocardpk + ")");
            }

            if (!string.IsNullOrEmpty(POLPk) & POLPk != "0")
            {
                sb.Append("   AND  POL.PORT_MST_PK IN(" + POLPk + ")");
            }

            if (!string.IsNullOrEmpty(shippinglinepk) & shippinglinepk != "0")
            {
                if (BizType == 2)
                {
                    sb.Append("   AND OPR.OPERATOR_MST_PK =" + shippinglinepk);
                }
                else if (BizType == 1)
                {
                    sb.Append("   AND AMT.AIRLINE_MST_PK =" + shippinglinepk);
                }
                else
                {
                    sb.Append("   AND (OPR.OPERATOR_MST_PK =" + shippinglinepk + " OR AMT.AIRLINE_MST_PK =" + shippinglinepk + ") ");
                }
            }
            if (!string.IsNullOrEmpty(PODPk) & PODPk != "0")
            {
                sb.Append("    AND POD.PORT_MST_PK IN(" + PODPk + ")");
            }
            if (!((FromDt == null | string.IsNullOrEmpty(FromDt)) & (ToDt == null | string.IsNullOrEmpty(ToDt))))
            {
                sb.Append("   AND JOB.JOBCARD_DATE BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)");
            }
            else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
            {
                sb.Append("   AND JOB.JOBCARD_DATE >= TO_DATE('" + FromDt + "',dateformat) ");
            }
            else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
            {
                sb.Append("   AND JOB.JOBCARD_DATE <= TO_DATE('" + ToDt + "',dateformat) ");
            }
            if (BizType == 2)
            {
                if (!string.IsNullOrEmpty(Voyagepk) & Voyagepk != "0")
                {
                    sb.Append("    AND VVT.VOYAGE_TRN_PK IN(" + Voyagepk + ")");
                }
            }
            else
            {
                if (!string.IsNullOrEmpty(VoyNo))
                {
                    sb.Append(" AND UPPER(JOB.VOYAGE_FLIGHT_NO) LIKE '" + VoyNo.Trim().ToUpper().Replace("'", "''") + "'");
                }
            }
            sb.Append("");
            return sb.ToString();
        }

        #endregion "To Fetch Jobcard in open Status"

        #region "To Fetch Jobcard in Closed Status Report"

        /// <summary>
        /// Gets the query for close status report.
        /// </summary>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <param name="Customerpk">The customerpk.</param>
        /// <param name="Jocardpk">The jocardpk.</param>
        /// <param name="POLPk">The pol pk.</param>
        /// <param name="shippinglinepk">The shippinglinepk.</param>
        /// <param name="Voyagepk">The voyagepk.</param>
        /// <param name="PODPk">The pod pk.</param>
        /// <param name="VoyNo">The voy no.</param>
        /// <param name="lngUsrLocFk">The LNG usr loc fk.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <returns></returns>
        public string GetQueryForCloseStatusReport(int BizType = 0, int Process = 0, string Customerpk = "", string Jocardpk = "", string POLPk = "", string shippinglinepk = "", string Voyagepk = "", string PODPk = "", string VoyNo = "", long lngUsrLocFk = 0,
        string FromDt = "", string ToDt = "")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("select DISTINCT JOB.JOB_CARD_TRN_PK, ");
            sb.Append("        JOB.JOBCARD_REF_NO,");
            sb.Append("       JOB.JOBCARD_DATE,");
            sb.Append("                (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.DD_FLAG = 'BIZ_TYPE' AND DD.CONFIG_ID = 'QFORCOMMON' AND DD.DD_VALUE = " + BizType + ") BIZ_TYPE,");
            sb.Append("                (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.DD_FLAG = 'PROCESS_TYPE' AND DD.CONFIG_ID = 'QFORCOMMON' AND DD.DD_VALUE =" + Process + ") PROCESS_TYPE,");
            sb.Append("      CUST.CUSTOMER_MST_PK,");
            sb.Append("       CUST.CUSTOMER_NAME,");
            sb.Append("       CONInv.Invoice_Date,");
            sb.Append("       TO_CHAR(P.PAYMENT_DATE,dateFormat)PAYMENT_DATE,");
            sb.Append("       TO_CHAR(C.COLLECTIONS_DATE,dateFormat)COLLECTIONS_DATE,");
            sb.Append("       TO_CHAR(JOB.JOB_CARD_CLOSED_ON,dateFormat)JOB_CARD_CLOSED_ON,");
            sb.Append("       POL.PORT_MST_PK POLPK,");
            sb.Append("       POL.PORT_NAME POL_NAME,");
            sb.Append("       POD.PORT_MST_PK PODPK,");
            sb.Append("       POD.PORT_NAME POD_NAME,");
            if (BizType == 2)
            {
                sb.Append("       OPR.OPERATOR_MST_PK,");
                sb.Append("       OPR.OPERATOR_NAME,");
            }
            else
            {
                sb.Append("       OPR.AIRLINE_MST_PK AS OPERATOR_MST_PK,");
                sb.Append("       OPR.AIRLINE_NAME AS OPERATOR_NAME,");
            }
            if (BizType == 2)
            {
                sb.Append("       VVT.VOYAGE_TRN_PK,");
                sb.Append("       V.VESSEL_ID,");
                sb.Append("       V.VESSEL_NAME,");
                sb.Append("       VVT.VOYAGE");
            }
            else
            {
                sb.Append("       0 VOYAGE_TRN_PK,");
                sb.Append("       '' VESSEL_ID,");
                sb.Append("       '' VESSEL_NAME,");
                sb.Append("       JOB.VOYAGE_FLIGHT_NO VOYAGE");
            }
            sb.Append("   from collections_tbl   C,");
            sb.Append("       COLLECTIONS_TRN_TBL    CIT,");
            sb.Append("       JOB_CARD_TRN   JOB,");
            sb.Append("       CONSOL_INVOICE_TBL     CONInv,");
            sb.Append("       CONSOL_INVOICE_TRN_TBL INVT,");
            sb.Append("       PAYMENT_TRN_TBL        PAY,");
            sb.Append("       PAYMENTS_TBL           P,");
            sb.Append("       INV_SUPPLIER_TBL       INV,");
            sb.Append("       INV_SUPPLIER_TRN_TBL   INVTR,");
            sb.Append("       USER_MST_TBL         UMT,");
            sb.Append("       PAYMENTS_MODE_TRN_TBL  Pm,");
            sb.Append("       JOB_TRN_COST          JTC,");
            sb.Append("       customer_mst_tbl       CUST,");
            sb.Append("       port_mst_tbl           POL,");
            sb.Append("       port_mst_tbl           POD,");
            if (BizType == 2)
            {
                sb.Append("       operator_mst_tbl       OPR,");
                sb.Append("       VESSEL_VOYAGE_TBL      V,");
                sb.Append("       VESSEL_VOYAGE_TRN      VVT");
            }
            else
            {
                sb.Append("       airline_mst_tbl        OPR");
            }

            sb.Append(" where C.COLLECTIONS_TBL_PK = CIT.COLLECTIONS_TBL_FK");
            sb.Append("   AND CONInv.CONSOL_INVOICE_PK = INVT.CONSOL_INVOICE_FK");
            sb.Append("   AND CIT.INVOICE_REF_NR = CONInv.INVOICE_REF_NO");
            sb.Append("   AND INVT.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK");
            sb.Append("   AND P.PAYMENT_TBL_PK = PAY.PAYMENTS_TBL_FK");
            sb.Append("   AND PAY.INV_SUPPLIER_TBL_FK = INV.INV_SUPPLIER_PK");
            sb.Append("   AND PM.PAYMENTS_TBL_FK = P.PAYMENT_TBL_PK");
            sb.Append("   AND INV.INV_SUPPLIER_PK = INVTR.INV_SUPPLIER_TBL_FK");
            sb.Append("   AND JTC.JOB_TRN_COST_PK = INVTR.JOB_TRN_EST_FK");
            sb.Append("   AND JOB.JOB_CARD_TRN_PK = JTC.JOB_CARD_TRN_FK");
            sb.Append("  AND UMT.USER_MST_PK = JOB.CREATED_BY_FK");

            sb.Append("  AND JOB.PORT_MST_POL_FK = POL.PORT_MST_PK");
            sb.Append("  AND JOB.PORT_MST_POD_FK = POD.PORT_MST_PK");
            sb.Append("  AND JOB.CUST_CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK");
            if (BizType == 2)
            {
                sb.Append("  AND JOB.CARRIER_MST_FK=OPR.OPERATOR_MST_PK(+)");
            }
            else
            {
                sb.Append("  AND JOB.CARRIER_MST_FK = OPR.AIRLINE_MST_PK(+)");
            }

            if (BizType == 2)
            {
                sb.Append("   AND VVT.VESSEL_VOYAGE_TBL_FK = V.VESSEL_VOYAGE_TBL_PK(+)");
                sb.Append("   AND JOB.VOYAGE_TRN_FK = VVT.VOYAGE_TRN_PK(+)");
            }
            if (Process == 1)
            {
                sb.Append("  AND UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk + " ");
            }
            else if (Process == 2)
            {
                sb.Append(" AND ((UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk + ") OR (UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk + " and JOB.JC_AUTO_MANUAL = 0) ");
                sb.Append("  OR (JOB.PORT_MST_POD_FK ");
                sb.Append(" IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK= " + lngUsrLocFk + ")  and JOB.JC_AUTO_MANUAL = 1)) ");
            }
            else
            {
                sb.Append(" AND ((UMT.DEFAULT_LOCATION_FK =" + lngUsrLocFk + " ) OR");
                sb.Append(" (UMT.DEFAULT_LOCATION_FK =" + lngUsrLocFk + " AND JOB.JC_AUTO_MANUAL=0) OR");
                sb.Append(" (JOB.PORT_MST_POD_FK IN");
                sb.Append(" (SELECT T.PORT_MST_FK");
                sb.Append("  FROM LOC_PORT_MAPPING_TRN T");
                sb.Append("  WHERE T.LOCATION_MST_FK =" + lngUsrLocFk + ") AND");
                sb.Append(" JOB.JC_AUTO_MANUAL = 1 ))");
            }
            if (BizType > 0)
            {
                sb.Append("  AND JOB.BUSINESS_TYPE= " + BizType + " ");
            }
            if (Process > 0)
            {
                sb.Append("   AND JOB.PROCESS_TYPE = " + Process + " ");
            }
            sb.Append(" AND JOB.JOB_CARD_STATUS=2");

            if (!string.IsNullOrEmpty(Customerpk) & Customerpk != "0")
            {
                sb.Append("      AND CUST.CUSTOMER_MST_PK IN(" + Customerpk + ")");
            }

            if (!string.IsNullOrEmpty(Jocardpk) & Jocardpk != "0")
            {
                sb.Append("     AND JOB.JOB_CARD_TRN_PK IN(" + Jocardpk + ")");
            }

            if (!string.IsNullOrEmpty(POLPk) & POLPk != "0")
            {
                sb.Append("   AND  POL.PORT_MST_PK IN(" + POLPk + ")");
            }

            if (!string.IsNullOrEmpty(shippinglinepk) & shippinglinepk != "0")
            {
                if (BizType == 2)
                {
                    sb.Append("   AND OPR.OPERATOR_MST_PK =" + shippinglinepk);
                }
                else
                {
                    sb.Append("   AND OPR.AIRLINE_MST_PK =" + shippinglinepk);
                }
            }

            if (!string.IsNullOrEmpty(PODPk) & PODPk != "0")
            {
                sb.Append("    AND POD.PORT_MST_PK IN(" + PODPk + ")");
            }

            if (!((FromDt == null | string.IsNullOrEmpty(FromDt)) & (ToDt == null | string.IsNullOrEmpty(ToDt))))
            {
                sb.Append("   AND JOB.JOBCARD_DATE BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)");
            }
            else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
            {
                sb.Append("   AND JOB.JOBCARD_DATE >= TO_DATE('" + FromDt + "',dateformat) ");
            }
            else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
            {
                sb.Append("   AND JOB.JOBCARD_DATE <= TO_DATE('" + ToDt + "',dateformat) ");
            }

            if (BizType == 2)
            {
                if (!string.IsNullOrEmpty(Voyagepk) & Voyagepk != "0")
                {
                    sb.Append("    AND VVT.VOYAGE_TRN_PK IN(" + Voyagepk + ")");
                }
            }
            else
            {
                if (!string.IsNullOrEmpty(VoyNo))
                {
                    sb.Append(" AND UPPER(JOB.VOYAGE_FLIGHT_NO) LIKE '%" + VoyNo.Trim().ToUpper().Replace("'", "''") + "%'");
                }
            }
            if (BizType == 1)
            {
                sb.Replace("SEA", "AIR");
            }
            if (Process == 2)
            {
                sb.Replace("EXP", "IMP");
            }
            return sb.ToString();
        }

        /// <summary>
        /// Fetches the close status report.
        /// </summary>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <param name="Customerpk">The customerpk.</param>
        /// <param name="Jocardpk">The jocardpk.</param>
        /// <param name="POLPk">The pol pk.</param>
        /// <param name="shippinglinepk">The shippinglinepk.</param>
        /// <param name="Voyagepk">The voyagepk.</param>
        /// <param name="PODPk">The pod pk.</param>
        /// <param name="VoyNo">The voy no.</param>
        /// <param name="lngUsrLocFk">The LNG usr loc fk.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <returns></returns>
        public DataSet FetchCloseStatusReport(int BizType = 0, int Process = 0, string Customerpk = "", string Jocardpk = "", string POLPk = "", string shippinglinepk = "", string Voyagepk = "", string PODPk = "", string VoyNo = "", long lngUsrLocFk = 0,
        string FromDt = "", string ToDt = "")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            Int32 last = 0;
            Int32 start = 0;
            string strSQL = null;
            string strCondition = null;
            string strQuery = "";
            Int32 TotalRecords = default(Int32);
            try
            {
                if ((BizType == 0))
                {
                    strQuery = GetQueryForCloseStatusReport(2, Process, Customerpk, Jocardpk, POLPk, shippinglinepk, Voyagepk, PODPk, VoyNo, lngUsrLocFk,
                    FromDt, ToDt);
                    sb.Append(strQuery);
                    sb.Append(" UNION ");
                    strQuery = "";
                    strQuery = GetQueryForCloseStatusReport(1, Process, Customerpk, Jocardpk, POLPk, shippinglinepk, Voyagepk, PODPk, VoyNo, lngUsrLocFk,
                    FromDt, ToDt);
                    sb.Append(strQuery);
                }
                else
                {
                    strQuery = GetQueryForCloseStatusReport(BizType, Process, Customerpk, Jocardpk, POLPk, shippinglinepk, Voyagepk, PODPk, VoyNo, lngUsrLocFk,
                    FromDt, ToDt);
                    sb.Append(strQuery);
                }
                return objWF.GetDataSet("SELECT * FROM (" + (sb.ToString()) + ") ORDER BY JOBCARD_DATE DESC ");
            }
            catch (OracleException SqlExp)
            {
                throw SqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion "To Fetch Jobcard in Closed Status Report"

        #region "To Fetch Jobcard in open Status Report"

        /// <summary>
        /// Gets the query for open status report.
        /// </summary>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <param name="Customerpk">The customerpk.</param>
        /// <param name="Jocardpk">The jocardpk.</param>
        /// <param name="POLPk">The pol pk.</param>
        /// <param name="shippinglinepk">The shippinglinepk.</param>
        /// <param name="Voyagepk">The voyagepk.</param>
        /// <param name="PODPk">The pod pk.</param>
        /// <param name="VoyNo">The voy no.</param>
        /// <param name="lngUsrLocFk">The LNG usr loc fk.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <returns></returns>
        public string GetQueryForOpenStatusReport(int BizType = 0, int Process = 0, string Customerpk = "", string Jocardpk = "", string POLPk = "", string shippinglinepk = "", string Voyagepk = "", string PODPk = "", string VoyNo = "", long lngUsrLocFk = 0,
        string FromDt = "", string ToDt = "")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT DISTINCT JOB.JOB_CARD_TRN_PK,");
            sb.Append("                JOB.JOBCARD_REF_NO,");
            sb.Append("                JOB.JOBCARD_DATE,");
            sb.Append("                (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.DD_FLAG = 'BIZ_TYPE' AND DD.CONFIG_ID = 'QFORCOMMON' AND DD.DD_VALUE = " + BizType + ") BIZ_TYPE,");
            sb.Append("                (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.DD_FLAG = 'PROCESS_TYPE' AND DD.CONFIG_ID = 'QFORCOMMON' AND DD.DD_VALUE =" + Process + ") PROCESS_TYPE,");
            sb.Append("                CUST.CUSTOMER_MST_PK,");
            sb.Append("                CUST.CUSTOMER_NAME,");
            sb.Append("                (SELECT CONINV.INVOICE_DATE");
            sb.Append("                   FROM CONSOL_INVOICE_TBL     CONINV,");
            sb.Append("                        CONSOL_INVOICE_TRN_TBL INVT");
            sb.Append("                  WHERE CONINV.CONSOL_INVOICE_PK = INVT.CONSOL_INVOICE_FK");
            sb.Append("                    and invt.job_card_fk = job.JOB_CARD_TRN_PK");
            sb.Append("                    and rownum = 1) Invoice_Date,");
            sb.Append("                (SELECT p.payment_date");
            sb.Append("                   FROM PAYMENTS_TBL         P,");
            sb.Append("                        PAYMENT_TRN_TBL      PAY,");
            sb.Append("                        INV_SUPPLIER_TBL     INV,");
            sb.Append("                        INV_SUPPLIER_TRN_TBL INVTRN,");
            sb.Append("                        JOB_TRN_COST JTC");
            sb.Append("                  WHERE P.PAYMENT_TBL_PK = PAY.PAYMENTS_TBL_FK");
            sb.Append("                    AND PAY.INV_SUPPLIER_TBL_FK = INV.INV_SUPPLIER_PK");
            sb.Append("                    AND JOB.JOB_CARD_TRN_PK = JTC.JOB_CARD_TRN_FK");
            sb.Append("                    AND INV.INV_SUPPLIER_PK = INVTRN.INV_SUPPLIER_TBL_FK");
            sb.Append("                    AND JTC.JOB_TRN_COST_PK = INVTRN.JOB_TRN_EST_FK");
            sb.Append("                    and rownum = 1) PAYMENT_DATE,");
            sb.Append("                (SELECT C.COLLECTIONS_DATE");
            sb.Append("                   FROM COLLECTIONS_TBL        C,");
            sb.Append("                        COLLECTIONS_TRN_TBL    CIT,");
            sb.Append("                        CONSOL_INVOICE_TRN_TBL INVT,");
            sb.Append("                        CONSOL_INVOICE_TBL     CONINV");
            sb.Append("                  WHERE C.COLLECTIONS_TBL_PK = CIT.COLLECTIONS_TBL_FK");
            sb.Append("                    AND CONINV.CONSOL_INVOICE_PK = INVT.CONSOL_INVOICE_FK");
            sb.Append("                    AND INVT.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK");
            sb.Append("                    AND rownum = 1) COLLECTIONS_DATE,");
            sb.Append("                JOB.JOB_CARD_CLOSED_ON,");
            sb.Append("       POL.PORT_MST_PK POLPK,");
            sb.Append("       POL.PORT_NAME POL_NAME,");
            sb.Append("       POD.PORT_MST_PK PODPK,");
            sb.Append("       POD.PORT_NAME POD_NAME,");
            if (BizType == 2)
            {
                sb.Append("       OPR.OPERATOR_MST_PK,");
                sb.Append("       OPR.OPERATOR_NAME,");
            }
            else
            {
                sb.Append("       OPR.AIRLINE_MST_PK AS OPERATOR_MST_PK,");
                sb.Append("       OPR.AIRLINE_NAME AS OPERATOR_NAME,");
            }
            if (BizType == 2)
            {
                sb.Append("       VVT.VOYAGE_TRN_PK,");
                sb.Append("       V.VESSEL_ID,");
                sb.Append("       V.VESSEL_NAME,");
                sb.Append("       VVT.VOYAGE");
            }
            else
            {
                sb.Append("       0 VOYAGE_TRN_PK,");
                sb.Append("       '' VESSEL_ID,");
                sb.Append("       '' VESSEL_NAME,");
                sb.Append("       JOB.VOYAGE_FLIGHT_NO VOYAGE");
            }
            sb.Append("    FROM JOB_CARD_TRN JOB,");
            sb.Append("       CUSTOMER_MST_TBL     CUST,");
            if (Process == 1)
            {
                sb.Append("       BOOKING_MST_TBL      BKNG,");
            }
            sb.Append("       PORT_MST_TBL         POL,");
            sb.Append("       PORT_MST_TBL         POD,");
            sb.Append("       USER_MST_TBL         UMT,");
            if (BizType == 2)
            {
                sb.Append("       operator_mst_tbl       OPR,");
                sb.Append("       VESSEL_VOYAGE_TBL      V,");
                sb.Append("       VESSEL_VOYAGE_TRN      VVT");
            }
            else
            {
                sb.Append("       airline_mst_tbl        OPR");
            }
            sb.Append("  WHERE");

            sb.Append("   JOB.PORT_MST_POL_FK = POL.PORT_MST_PK");
            sb.Append("  AND JOB.PORT_MST_POD_FK = POD.PORT_MST_PK");
            sb.Append("  AND JOB.CUST_CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK");
            if (BizType == 2)
            {
                sb.Append("  AND JOB.CARRIER_MST_FK = OPR.OPERATOR_MST_PK(+) ");
            }
            else
            {
                sb.Append("  AND JOB.CARRIER_MST_FK = OPR.AIRLINE_MST_PK(+) ");
            }

            if (BizType == 2)
            {
                sb.Append("   AND VVT.VESSEL_VOYAGE_TBL_FK = V.VESSEL_VOYAGE_TBL_PK(+)");
                sb.Append("   AND JOB.VOYAGE_TRN_FK = VVT.VOYAGE_TRN_PK(+)");
            }
            sb.Append("   AND JOB.JOB_CARD_STATUS = 1");
            sb.Append("  AND UMT.USER_MST_PK = JOB.CREATED_BY_FK");
            if (Process == 1)
            {
                sb.Append("  AND UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk + " ");
            }
            else
            {
                sb.Append(" AND ((UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk + ") OR (UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk + " and JOB.JC_AUTO_MANUAL = 0) ");
                sb.Append("  OR (JOB.PORT_MST_POD_FK ");
                sb.Append(" IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK= " + lngUsrLocFk + ")  and JOB.JC_AUTO_MANUAL = 1)) ");
            }
            if (!string.IsNullOrEmpty(Customerpk) & Customerpk != "0")
            {
                sb.Append("      AND CUST.CUSTOMER_MST_PK IN(" + Customerpk + ")");
            }

            if (!string.IsNullOrEmpty(Jocardpk) & Jocardpk != "0")
            {
                sb.Append("     AND JOB.JOB_CARD_TRN_PK IN(" + Jocardpk + ")");
            }

            if (!string.IsNullOrEmpty(POLPk) & POLPk != "0")
            {
                sb.Append("   AND  POL.PORT_MST_PK IN(" + POLPk + ")");
            }

            if (!string.IsNullOrEmpty(shippinglinepk) & shippinglinepk != "0")
            {
                if (BizType == 2)
                {
                    sb.Append("   AND OPR.OPERATOR_MST_PK =" + shippinglinepk);
                }
                else
                {
                    sb.Append("   AND OPR.AIRLINE_MST_PK =" + shippinglinepk);
                }
            }

            if (!string.IsNullOrEmpty(PODPk) & PODPk != "0")
            {
                sb.Append("    AND POD.PORT_MST_PK IN(" + PODPk + ")");
            }

            if (!((FromDt == null | string.IsNullOrEmpty(FromDt)) & (ToDt == null | string.IsNullOrEmpty(ToDt))))
            {
                sb.Append("   AND JOB.JOBCARD_DATE BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)");
            }
            else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
            {
                sb.Append("   AND JOB.JOBCARD_DATE >= TO_DATE('" + FromDt + "',dateformat) ");
            }
            else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
            {
                sb.Append("   AND JOB.JOBCARD_DATE <= TO_DATE('" + ToDt + "',dateformat) ");
            }

            if (BizType == 2)
            {
                if (!string.IsNullOrEmpty(Voyagepk) & Voyagepk != "0")
                {
                    sb.Append("    AND VVT.VOYAGE_TRN_PK IN(" + Voyagepk + ")");
                }
            }
            else
            {
                if (!string.IsNullOrEmpty(VoyNo))
                {
                    sb.Append(" AND UPPER(JOB.VOYAGE_FLIGHT_NO) LIKE '%" + VoyNo.Trim().ToUpper().Replace("'", "''") + "%'");
                }
            }
            if (BizType > 0)
            {
                sb.Append("  AND JOB.BUSINESS_TYPE= " + BizType + " ");
            }
            if (Process > 0)
            {
                sb.Append("   AND JOB.PROCESS_TYPE = " + Process + " ");
            }
            if (BizType == 1)
            {
                sb.Replace("SEA", "AIR");
            }
            if (Process == 2)
            {
                sb.Replace("EXP", "IMP");
            }
            return sb.ToString();
        }

        /// <summary>
        /// Fetches the open status report.
        /// </summary>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <param name="Customerpk">The customerpk.</param>
        /// <param name="Jocardpk">The jocardpk.</param>
        /// <param name="POLPk">The pol pk.</param>
        /// <param name="shippinglinepk">The shippinglinepk.</param>
        /// <param name="Voyagepk">The voyagepk.</param>
        /// <param name="PODPk">The pod pk.</param>
        /// <param name="VoyNo">The voy no.</param>
        /// <param name="lngUsrLocFk">The LNG usr loc fk.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <returns></returns>
        public DataSet FetchOpenStatusReport(int BizType = 0, int Process = 0, string Customerpk = "", string Jocardpk = "", string POLPk = "", string shippinglinepk = "", string Voyagepk = "", string PODPk = "", string VoyNo = "", long lngUsrLocFk = 0,
        string FromDt = "", string ToDt = "")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            Int32 last = 0;
            Int32 start = 0;
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            string strQuery = "";
            try
            {
                if ((BizType == 0))
                {
                    strQuery = GetQueryForOpenStatusReport(2, Process, Customerpk, Jocardpk, POLPk, shippinglinepk, Voyagepk, PODPk, VoyNo, lngUsrLocFk,
                    FromDt, ToDt);
                    sb.Append(strQuery);
                    sb.Append(" UNION ");
                    strQuery = "";
                    strQuery = GetQueryForOpenStatusReport(1, Process, Customerpk, Jocardpk, POLPk, shippinglinepk, Voyagepk, PODPk, VoyNo, lngUsrLocFk,
                    FromDt, ToDt);
                    sb.Append(strQuery);
                }
                else
                {
                    strQuery = GetQueryForOpenStatusReport(BizType, Process, Customerpk, Jocardpk, POLPk, shippinglinepk, Voyagepk, PODPk, VoyNo, lngUsrLocFk,
                    FromDt, ToDt);
                    sb.Append(strQuery);
                }
                return objWF.GetDataSet("SELECT * FROM (" + (sb.ToString()) + ") ORDER BY JOBCARD_DATE DESC ");
            }
            catch (OracleException SqlExp)
            {
                throw SqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion "To Fetch Jobcard in open Status Report"
    }
}