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
    public class Cls_JCinvoiceGenerationReport : CommonFeatures
    {
        #region "Fetch Job Card for Sea-Export"

        /// <summary>
        /// Fetches the jc sea export.
        /// </summary>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="ETADt">The eta dt.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="Excel">The excel.</param>
        /// <returns></returns>
        public DataSet FetchJCSeaExport(Int32 LocFk = 0, string FromDt = "", string ToDt = "", string CustName = "", string ETDDt = "", string ETADt = "", Int32 flag = 0, Int32 TotalPage = 0, Int32 CurrentPage = 0, Int32 Excel = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strCondition = null;
            string strSQL = null;
            Int32 last = 0;
            Int32 start = 0;
            Int32 TotalRecords = default(Int32);
            try
            {
                if (!(FromDt == null | string.IsNullOrEmpty(FromDt)))
                {
                    strCondition = strCondition + " And TO_DATE(JCSET.JOBCARD_DATE,DATEFORMAT) >= TO_DATE('" + FromDt + "',dateformat)";
                }
                if (!(ToDt == null | string.IsNullOrEmpty(ToDt)))
                {
                    strCondition = strCondition + " And TO_DATE(JCSET.JOBCARD_DATE,DATEFORMAT) <= TO_DATE('" + ToDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition = strCondition + " And TO_DATE(JCSET.ETD_DATE,dateformat) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ETADt))
                {
                    strCondition = strCondition + " And TO_DATE(JCSET.ETA_DATE,dateformat) = TO_DATE('" + ETADt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    strCondition = strCondition + " And SHP.CUSTOMER_NAME = '" + CustName + "'";
                }
                strCondition = strCondition + " AND JCSET.BUSINESS_TYPE = 2 ";
                strCondition = strCondition + " AND JCSET.PROCESS_TYPE = 1 ";

                sb.Append("SELECT JCSET.JOB_CARD_TRN_PK JOBPK,");
                sb.Append("    MAX(JCSET.JOBCARD_REF_NO) JOBREFNR,");
                sb.Append("   TO_DATE(MAX(JCSET.JOBCARD_DATE), FROMDATE) JOBDATE,");
                sb.Append("   MAX(SHP.CUSTOMER_NAME) SHIPPER,");
                sb.Append("    MAX(CONS.CUSTOMER_NAME) CONSIGNEE,");
                sb.Append("      MAX(FRTPYR.CUSTOMER_MST_PK) CUST_PK,");
                sb.Append("    MAX(FRTPYR.CUSTOMER_NAME) CUST,");
                sb.Append("    'Sea' BUSINESS_TYPE,");
                sb.Append("    'Export' PROCESS_TYPE,");
                sb.Append("    MAX(POL.PORT_ID) POL,");
                sb.Append("    MAX(POD.PORT_ID) POD,");
                sb.Append("    MAX(JCSET.ETD_DATE) ETD_DATE,");
                sb.Append("    MAX(JCSET.ETA_DATE) ETA,");
                sb.Append("    SUM(NVL(JTSEF.FREIGHT_AMT, 0) * JTSEF.EXCHANGE_RATE) FRAMT,");
                sb.Append("     '' SEL");
                sb.Append("    FROM BOOKING_MST_TBL BST,");
                sb.Append("    JOB_CARD_TRN         JCSET,");
                sb.Append("    JOB_TRN_FD           JTSEF, ");
                sb.Append("    CUSTOMER_MST_TBL     SHP,");
                sb.Append("    CUSTOMER_MST_TBL     CONS,");
                sb.Append("    CUSTOMER_MST_TBL     FRTPYR,");
                sb.Append("    PORT_MST_TBL         POL,");
                sb.Append("    PORT_MST_TBL         POD");

                sb.Append("    WHERE(BST.BOOKING_MST_PK = JCSET.BOOKING_MST_FK)");
                sb.Append("    AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
                sb.Append("    AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
                sb.Append("    AND SHP.CUSTOMER_MST_PK = JCSET.SHIPPER_CUST_MST_FK");
                sb.Append("    AND CONS.CUSTOMER_MST_PK(+) = JCSET.CONSIGNEE_CUST_MST_FK");
                sb.Append("    AND FRTPYR.CUSTOMER_MST_PK(+) = JTSEF.FRTPAYER_CUST_MST_FK");
                sb.Append("    AND JTSEF.CONSOL_INVOICE_TRN_FK IS NULL");
                sb.Append("    AND ABS((NVL(JTSEF.FREIGHT_AMT, 0) * JTSEF.EXCHANGE_RATE)) > 0");
                sb.Append("    AND BST.STATUS <> 3");
                sb.Append("    AND JCSET.JOB_CARD_STATUS <> 2");
                sb.Append("    AND JTSEF.FREIGHT_TYPE = 1");
                sb.Append("    AND JTSEF.JOB_CARD_TRN_FK = JCSET.JOB_CARD_TRN_PK");
                sb.Append("    AND POL.LOCATION_MST_FK =(SELECT L.LOCATION_MST_PK");
                sb.Append("    FROM LOCATION_MST_TBL L");
                sb.Append("    WHERE L.LOCATION_MST_PK =" + LocFk + ")");

                sb.Append(strCondition);
                sb.Append("    GROUP BY JCSET.JOB_CARD_TRN_PK ");
                sb.Append("    UNION ALL");
                sb.Append("    SELECT JCSET.JOB_CARD_TRN_PK JOBPK,");
                sb.Append("    MAX(JCSET.JOBCARD_REF_NO) JOBREFNR,");
                sb.Append("    TO_DATEMAX(JCSET.JOBCARD_DATE), FROMDATE) JOBDATE,");
                sb.Append("    MAX(SHP.CUSTOMER_NAME) SHIPPER,");
                sb.Append("    MAX(CONS.CUSTOMER_NAME) CONSIGNEE,");
                sb.Append("    MAX(FRTPYR.CUSTOMER_MST_PK) CUST_PK,");
                sb.Append("    MAX(FRTPYR.CUSTOMER_NAME) CUST,");
                sb.Append("    'Sea' BUSINESS_TYPE,");
                sb.Append("    'Export' PROCESS_TYPE,");
                sb.Append("    MAX(POL.PORT_ID) POL,");
                sb.Append("    MAX(POD.PORT_ID) POD,");
                sb.Append("    MAX(JCSET.ETD_DATE) ETD_DATE,");
                sb.Append("    MAX(JCSET.ETA_DATE) ETA,");
                sb.Append("    SUM(NVL(JOTSEF.AMOUNT, 0) * JOTSEF.EXCHANGE_RATE) FRAMT,");
                sb.Append("     '' SEL");
                sb.Append("    FROM BOOKING_MST_TBL      BST,");
                sb.Append("    JOB_CARD_TRN              JCSET,");
                sb.Append("    JOB_TRN_OTH_CHRG         JOTSEF,");
                sb.Append("    CUSTOMER_MST_TBL     SHP,");
                sb.Append("    CUSTOMER_MST_TBL     CONS,");
                sb.Append("    CUSTOMER_MST_TBL     FRTPYR,");
                sb.Append("    PORT_MST_TBL         POL,");
                sb.Append("    PORT_MST_TBL         POD");

                sb.Append("    WHERE(BST.BOOKING_MST_PK = JCSET.BOOKING_MST_FK)");
                sb.Append("    AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
                sb.Append("    AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
                sb.Append("    AND SHP.CUSTOMER_MST_PK = JCSET.SHIPPER_CUST_MST_FK");
                sb.Append("    AND CONS.CUSTOMER_MST_PK(+) = JCSET.CONSIGNEE_CUST_MST_FK");
                sb.Append("    AND FRTPYR.CUSTOMER_MST_PK(+) = JOTSEF.FRTPAYER_CUST_MST_FK");
                sb.Append("    AND JOTSEF.CONSOL_INVOICE_TRN_FK IS NULL");
                sb.Append("    AND ABS((NVL(JOTSEF.AMOUNT, 0) * JOTSEF.EXCHANGE_RATE)) > 0");
                sb.Append("    AND BST.STATUS <> 3");
                sb.Append("    AND JCSET.JOB_CARD_STATUS <> 2");
                sb.Append("    AND JOTSEF.FREIGHT_TYPE = 1");
                sb.Append("    AND JOTSEF.JOB_CARD_TRN_FK = JCSET.JOB_CARD_TRN_PK");
                sb.Append("    AND POL.LOCATION_MST_FK =(SELECT L.LOCATION_MST_PK");
                sb.Append("    FROM LOCATION_MST_TBL L");
                sb.Append("    WHERE L.LOCATION_MST_PK =" + LocFk + ")");
                sb.Append(strCondition);
                sb.Append("    GROUP BY JCSET.JOB_CARD_TRN_PK");
                sb.Append("    ORDER BY JOBDATE DESC, JOBREFNR DESC");

                strSQL = "select count(*) from (";
                strSQL += sb.ToString() + ")";
                TotalRecords = (Int32)objWF.GetDataSet(strSQL).Tables[0].Rows[0][0];
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
                if (Excel == 1)
                {
                    strSQL += " )q ) ";
                }
                else
                {
                    strSQL += " )q ) WHERE SR_NO Between " + start + " and " + last;
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

        #endregion "Fetch Job Card for Sea-Export"

        #region "Fetch Job Card for Air-Export"

        /// <summary>
        /// Fetches the jc air export.
        /// </summary>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="ETADt">The eta dt.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="Excel">The excel.</param>
        /// <returns></returns>
        public DataSet FetchJCAirExport(Int32 LocFk = 0, string FromDt = "", string ToDt = "", string CustName = "", string ETDDt = "", string ETADt = "", Int32 flag = 0, Int32 TotalPage = 0, Int32 CurrentPage = 0, Int32 Excel = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strCondition = null;
            string strSQL = null;
            Int32 last = 0;
            Int32 start = 0;
            Int32 TotalRecords = default(Int32);
            try
            {
                if (!(FromDt == null | string.IsNullOrEmpty(FromDt)))
                {
                    strCondition = strCondition + " And TO_DATE(JCSET.JOBCARD_DATE,DATEFORMAT) >= TO_DATE('" + FromDt + "',dateformat)";
                }
                if (!(ToDt == null | string.IsNullOrEmpty(ToDt)))
                {
                    strCondition = strCondition + " And TO_DATE(JCSET.JOBCARD_DATE,DATEFORMAT) <= TO_DATE('" + ToDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition = strCondition + " And TO_DATE(JCSET.ETD_DATE,dateformat) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ETADt))
                {
                    strCondition = strCondition + " And TO_DATE(JCSET.ETA_DATE,dateformat) = TO_DATE('" + ETADt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    strCondition = strCondition + " And SHP.CUSTOMER_NAME = '" + CustName + "'";
                }
                strCondition = strCondition + " AND JCSET.BUSINESS_TYPE = 1 ";
                strCondition = strCondition + " AND JCSET.PROCESS_TYPE = 1 ";

                sb.Append("SELECT JCSET.JOB_CARD_TRN_PK JOBPK,");
                sb.Append("  MAX(JCSET.JOBCARD_REF_NO) JOBREFNR,");
                sb.Append("  TO_DATE(MAX(JCSET.JOBCARD_DATE), DATEFORMAT) JOBDATE,");
                sb.Append("  MAX(SHP.CUSTOMER_NAME) SHIPPER,");
                sb.Append("  MAX(CONS.CUSTOMER_NAME) CONSIGNEE,");
                sb.Append("  MAX(FRTPYR.CUSTOMER_MST_PK) CUST_PK,");
                sb.Append("  MAX(FRTPYR.CUSTOMER_NAME) CUST,");
                sb.Append("    'Air' BUSINESS_TYPE,");
                sb.Append("    'Export' PROCESS_TYPE,");
                sb.Append("  MAX(POL.PORT_ID) POL,");
                sb.Append("  MAX(POD.PORT_ID) POD,");
                sb.Append("    MAX(JCSET.ETD_DATE) ETD_DATE,");
                sb.Append("    MAX(JCSET.ETA_DATE) ETA,");
                sb.Append("  SUM(NVL(JTSEF.FREIGHT_AMT, 0) * JTSEF.EXCHANGE_RATE) FRAMT,");
                sb.Append("     '' SEL");
                sb.Append("       FROM BOOKING_MST_TBL      BST,");
                sb.Append("       JOB_CARD_TRN JCSET,");
                sb.Append("       JOB_TRN_FD   JTSEF,");
                sb.Append("       CUSTOMER_MST_TBL     SHP,");
                sb.Append("       CUSTOMER_MST_TBL     CONS,");
                sb.Append("       CUSTOMER_MST_TBL     FRTPYR,");
                sb.Append("       PORT_MST_TBL         POL,");
                sb.Append("       PORT_MST_TBL          POD");

                sb.Append("       WHERE(BST.BOOKING_MST_PK = JCSET.BOOKING_MST_FK)");
                sb.Append("       AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
                sb.Append("       AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
                sb.Append("       AND SHP.CUSTOMER_MST_PK = JCSET.SHIPPER_CUST_MST_FK");
                sb.Append("       AND CONS.CUSTOMER_MST_PK(+) = JCSET.CONSIGNEE_CUST_MST_FK");
                sb.Append("       AND FRTPYR.CUSTOMER_MST_PK(+) = JTSEF.FRTPAYER_CUST_MST_FK");
                sb.Append("       AND JTSEF.CONSOL_INVOICE_TRN_FK IS NULL");
                sb.Append("       AND ABS((NVL(JTSEF.FREIGHT_AMT, 0) * JTSEF.EXCHANGE_RATE)) > 0");
                sb.Append("       AND BST.STATUS <> 3");
                sb.Append("       AND JCSET.JOB_CARD_STATUS <> 2");
                sb.Append("       AND JTSEF.FREIGHT_TYPE = 1");
                sb.Append("       AND JTSEF.JOB_CARD_TRN_FK = JCSET.JOB_CARD_TRN_PK");
                sb.Append("       AND POL.LOCATION_MST_FK = (SELECT L.LOCATION_MST_PK");
                sb.Append("       FROM LOCATION_MST_TBL L");
                sb.Append("    WHERE L.LOCATION_MST_PK =" + LocFk + ")");
                sb.Append(strCondition);
                sb.Append("       GROUP BY JCSET.JOB_CARD_TRN_PK");
                sb.Append("                     UNION ALL");
                sb.Append("       SELECT JCSET.JOB_CARD_TRN_PK JOBPK,");
                sb.Append("        MAX(JCSET.JOBCARD_REF_NO) JOBREFNR,");
                sb.Append("        MAX(JCSET.JOBCARD_DATE) JOBDATE,");
                sb.Append("       MAX(SHP.CUSTOMER_NAME) SHIPPER,");
                sb.Append("       MAX(CONS.CUSTOMER_NAME) CONSIGNEE,");
                sb.Append("       MAX(FRTPYR.CUSTOMER_MST_PK) CUST_PK,");
                sb.Append("       MAX(FRTPYR.CUSTOMER_NAME) CUST,");
                sb.Append("    'Air' BUSINESS_TYPE,");
                sb.Append("    'Export' PROCESS_TYPE,");
                sb.Append("       MAX(POL.PORT_ID) POL,");
                sb.Append("       MAX(POD.PORT_ID) POD,");
                sb.Append("       MAX(JCSET.ETD_DATE) ETD_DATE,");
                sb.Append("       MAX(JCSET.ETA_DATE) ETA,");
                sb.Append("       SUM(NVL(JOTSEF.AMOUNT, 0) * JOTSEF.EXCHANGE_RATE) FRAMT,");
                sb.Append("     '' SEL");
                sb.Append("        FROM BOOKING_MST_TBL      BST,");
                sb.Append("        JOB_CARD_TRN JCSET,");
                sb.Append("        JOB_TRN_OTH_CHRG   JOTSEF,");
                sb.Append("        CUSTOMER_MST_TBL     SHP,");
                sb.Append("        CUSTOMER_MST_TBL     CONS,");
                sb.Append("        CUSTOMER_MST_TBL     FRTPYR,");
                sb.Append("       PORT_MST_TBL         POL,");
                sb.Append("       PORT_MST_TBL          POD");

                sb.Append("        WHERE(BST.BOOKING_MST_PK = JCSET.BOOKING_MST_FK)");
                sb.Append("       AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
                sb.Append("       AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
                sb.Append("        AND SHP.CUSTOMER_MST_PK = JCSET.SHIPPER_CUST_MST_FK");
                sb.Append("       AND CONS.CUSTOMER_MST_PK(+) = JCSET.CONSIGNEE_CUST_MST_FK");
                sb.Append("       AND FRTPYR.CUSTOMER_MST_PK(+) = JOTSEF.FRTPAYER_CUST_MST_FK");
                sb.Append("        AND JOTSEF.CONSOL_INVOICE_TRN_FK IS NULL");
                sb.Append("        AND ABS((NVL(JOTSEF.AMOUNT, 0) * JOTSEF.EXCHANGE_RATE)) > 0");
                sb.Append("        AND BST.STATUS <> 3");
                sb.Append("       AND JCSET.JOB_CARD_STATUS <> 2");
                sb.Append("        AND JOTSEF.FREIGHT_TYPE = 1");
                sb.Append("       AND JOTSEF.JOB_CARD_TRN_FK = JCSET.JOB_CARD_TRN_PK");
                sb.Append("     AND POL.LOCATION_MST_FK =(SELECT L.LOCATION_MST_PK");
                sb.Append("     FROM LOCATION_MST_TBL L");
                sb.Append("     WHERE L.LOCATION_MST_PK =" + LocFk + ")");
                sb.Append(strCondition);
                sb.Append("       GROUP BY JCSET.JOB_CARD_TRN_PK");
                sb.Append("       ORDER BY JOBDATE DESC, JOBREFNR DESC");

                strSQL = " select count(*) from (";
                strSQL += sb.ToString() + ")";
                TotalRecords = (Int32)objWF.GetDataSet(strSQL).Tables[0].Rows[0][0];
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
                if (Excel == 1)
                {
                    strSQL += " )q ) ";
                }
                else
                {
                    strSQL += " )q ) WHERE SR_NO Between " + start + " and " + last;
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

        #endregion "Fetch Job Card for Air-Export"

        #region "Fetch Job Card for Sea-Import"

        /// <summary>
        /// Fetches the jc sea import.
        /// </summary>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="ETADt">The eta dt.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="Excel">The excel.</param>
        /// <returns></returns>
        public DataSet FetchJCSeaImport(Int32 LocFk = 0, string FromDt = "", string ToDt = "", string CustName = "", string ETDDt = "", string ETADt = "", Int32 flag = 0, Int32 TotalPage = 0, Int32 CurrentPage = 0, Int32 Excel = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strCondition = null;
            string strSQL = null;
            Int32 last = 0;
            Int32 start = 0;
            Int32 TotalRecords = default(Int32);
            try
            {
                if (!(FromDt == null | string.IsNullOrEmpty(FromDt)))
                {
                    strCondition = strCondition + " And TO_DATE(JCSET.JOBCARD_DATE,DATEFORMAT) >= TO_DATE('" + FromDt + "',dateformat)";
                }
                if (!(ToDt == null | string.IsNullOrEmpty(ToDt)))
                {
                    strCondition = strCondition + " And TO_DATE(JCSET.JOBCARD_DATE,DATEFORMAT) <= TO_DATE('" + ToDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition = strCondition + " And TO_DATE(JCSET.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ETADt))
                {
                    strCondition = strCondition + " And TO_DATE(JCSET.ETA_DATE,dateformat) = TO_DATE('" + ETADt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    strCondition = strCondition + " And CONS.CUSTOMER_NAME = '" + CustName + "'";
                }
                strCondition = strCondition + " AND JCSET.BUSINESS_TYPE = 2 ";
                strCondition = strCondition + " AND JCSET.PROCESS_TYPE = 2 ";

                sb.Append("SELECT JCSET.JOB_CARD_TRN_PK JOBPK,");
                sb.Append("       MAX(JCSET.JOBCARD_REF_NO) JOBREFNR,");
                sb.Append("       TO_DATE(MAX(JCSET.JOBCARD_DATE), DATEFORMAT) JOBDATE,");
                sb.Append("       MAX(SHP.CUSTOMER_NAME) SHIPPER,");
                sb.Append("       MAX(CONS.CUSTOMER_NAME) CONSIGNEE,");
                sb.Append("       MAX(FRTPYR.CUSTOMER_MST_PK) CUST_PK,");
                sb.Append("       MAX(FRTPYR.CUSTOMER_NAME) CUST,");
                sb.Append("    'Sea' BUSINESS_TYPE,");
                sb.Append("    'Import' PROCESS_TYPE,");
                sb.Append("    MAX(POL.PORT_ID) POL,");
                sb.Append("       MAX(POD.PORT_ID) POD,");
                sb.Append("       MAX(JCSET.ETD_DATE) ETD_DATE,");
                sb.Append("       MAX(JCSET.ETA_DATE) ETA,");
                sb.Append("       SUM(NVL(JTSEF.FREIGHT_AMT, 0) * JTSEF.EXCHANGE_RATE) FRAMT,");
                sb.Append("     '' SEL");
                sb.Append("        FROM JOB_CARD_TRN   JCSET,");
                sb.Append("       JOB_TRN_FD   JTSEF,");
                sb.Append("       CUSTOMER_MST_TBL     SHP,");
                sb.Append("       CUSTOMER_MST_TBL     CONS,");
                sb.Append("       CUSTOMER_MST_TBL     FRTPYR,");
                sb.Append("       PORT_MST_TBL         POL,");
                sb.Append("       PORT_MST_TBL          POD");

                sb.Append("       WHERE(POD.PORT_MST_PK = JCSET.PORT_MST_POD_FK)");
                sb.Append("       AND POL.PORT_MST_PK = JCSET.PORT_MST_POL_FK");
                sb.Append("       AND SHP.CUSTOMER_MST_PK = JCSET.SHIPPER_CUST_MST_FK");
                sb.Append("       AND CONS.CUSTOMER_MST_PK(+) = JCSET.CONSIGNEE_CUST_MST_FK");
                sb.Append("       AND FRTPYR.CUSTOMER_MST_PK(+) = JTSEF.FRTPAYER_CUST_MST_FK");
                sb.Append("       AND JTSEF.CONSOL_INVOICE_TRN_FK IS NULL");
                sb.Append("       AND ABS((NVL(JTSEF.FREIGHT_AMT, 0) * JTSEF.EXCHANGE_RATE)) > 0");
                sb.Append("       AND JCSET.JOB_CARD_STATUS <> 2");
                sb.Append("       AND JTSEF.FREIGHT_TYPE = 2");
                sb.Append("       AND JTSEF.JOB_CARD_TRN_FK = JCSET.JOB_CARD_TRN_PK");
                sb.Append("       AND POD.LOCATION_MST_FK = ");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("       FROM LOCATION_MST_TBL L");
                sb.Append("       WHERE L.LOCATION_MST_PK =" + LocFk + ")");

                sb.Append(strCondition);
                sb.Append("       GROUP BY JCSET.JOB_CARD_TRN_PK");
                sb.Append("        UNION ALL");
                sb.Append("       SELECT JCSET.JOB_CARD_TRN_PK JOBPK,");
                sb.Append("       MAX(JCSET.JOBCARD_REF_NO) JOBREFNR,");
                sb.Append("      TO_DATE(MAX(JCSET.JOBCARD_DATE), DATEFORMAT) JOBDATE,");
                sb.Append("       MAX(SHP.CUSTOMER_NAME) SHIPPER,");
                sb.Append("       MAX(CONS.CUSTOMER_NAME) CONSIGNEE,");
                sb.Append("       MAX(FRTPYR.CUSTOMER_MST_PK) CUST_PK,");
                sb.Append("       MAX(FRTPYR.CUSTOMER_NAME) CUST,");
                sb.Append("    'Sea' BUSINESS_TYPE,");
                sb.Append("    'Import' PROCESS_TYPE,");
                sb.Append("    MAX(POL.PORT_ID) POL,");
                sb.Append("       MAX(POD.PORT_ID) POD,");
                sb.Append("       MAX(JCSET.ETD_DATE) ETD_DATE,");
                sb.Append("       MAX(JCSET.ETA_DATE) ETA,");
                sb.Append("       SUM(NVL(JOTSEF.AMOUNT, 0) * JOTSEF.EXCHANGE_RATE) FRAMT,");
                sb.Append("     '' SEL");
                sb.Append("       FROM JOB_CARD_TRN JCSET,");
                sb.Append("       JOB_TRN_OTH_CHRG   JOTSEF,");
                sb.Append("       CUSTOMER_MST_TBL     SHP,");
                sb.Append("       CUSTOMER_MST_TBL     CONS,");
                sb.Append("       CUSTOMER_MST_TBL     FRTPYR,");
                sb.Append("       PORT_MST_TBL         POL,");
                sb.Append("       PORT_MST_TBL     POD");

                sb.Append("       WHERE(POD.PORT_MST_PK = JCSET.PORT_MST_POD_FK)");
                sb.Append("       AND POL.PORT_MST_PK = JCSET.PORT_MST_POL_FK");
                sb.Append("       AND SHP.CUSTOMER_MST_PK = JCSET.SHIPPER_CUST_MST_FK");
                sb.Append("       AND CONS.CUSTOMER_MST_PK(+) = JCSET.CONSIGNEE_CUST_MST_FK");
                sb.Append("       AND FRTPYR.CUSTOMER_MST_PK(+) = JOTSEF.FRTPAYER_CUST_MST_FK");
                sb.Append("       AND JOTSEF.CONSOL_INVOICE_TRN_FK IS NULL");
                sb.Append("       AND ABS((NVL(JOTSEF.AMOUNT, 0) * JOTSEF.EXCHANGE_RATE)) > 0");
                sb.Append("       AND JCSET.JOB_CARD_STATUS <> 2");
                sb.Append("       AND JOTSEF.FREIGHT_TYPE = 2");
                sb.Append("       AND JOTSEF.JOB_CARD_TRN_FK = JCSET.JOB_CARD_TRN_PK");
                sb.Append("       AND POD.LOCATION_MST_FK = ");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("       FROM LOCATION_MST_TBL L");
                sb.Append("       WHERE L.LOCATION_MST_PK =" + LocFk + ")");
                sb.Append(strCondition);
                sb.Append("       GROUP BY JCSET.JOB_CARD_TRN_PK");
                sb.Append("       ORDER BY JOBDATE DESC, JOBREFNR DESC");
                strSQL = " select count(*) from (";
                strSQL += sb.ToString() + ")";

                TotalRecords = (Int32)objWF.GetDataSet(strSQL).Tables[0].Rows[0][0];
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
                if (Excel == 1)
                {
                    strSQL += " )q ) ";
                }
                else
                {
                    strSQL += " )q ) WHERE SR_NO Between " + start + " and " + last;
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

        #endregion "Fetch Job Card for Sea-Import"

        #region "Fetch Job Card for Air-Import"

        /// <summary>
        /// Fetches the jc air import.
        /// </summary>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="ETADt">The eta dt.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="Excel">The excel.</param>
        /// <returns></returns>
        public DataSet FetchJCAirImport(Int32 LocFk = 0, string FromDt = "", string ToDt = "", string CustName = "", string ETDDt = "", string ETADt = "", Int32 flag = 0, Int32 TotalPage = 0, Int32 CurrentPage = 0, Int32 Excel = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strCondition = null;
            string strSQL = null;
            Int32 last = 0;
            Int32 start = 0;
            Int32 TotalRecords = default(Int32);
            try
            {
                if (!(FromDt == null | string.IsNullOrEmpty(FromDt)))
                {
                    strCondition = strCondition + " And TO_DATE(JCSET.JOBCARD_DATE,DATEFORMAT) >= TO_DATE('" + FromDt + "',dateformat)";
                }
                if (!(ToDt == null | string.IsNullOrEmpty(ToDt)))
                {
                    strCondition = strCondition + " And TO_DATE(JCSET.JOBCARD_DATE,DATEFORMAT) <= TO_DATE('" + ToDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition = strCondition + " And TO_DATE(JCSET.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ETADt))
                {
                    strCondition = strCondition + " And TO_DATE(JCSET.ETA_DATE,dateformat) = TO_DATE('" + ETADt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    strCondition = strCondition + " And CONS.CUSTOMER_NAME = '" + CustName + "'";
                }
                strCondition = strCondition + " AND JCSET.BUSINESS_TYPE = 1 ";
                strCondition = strCondition + " AND JCSET.PROCESS_TYPE = 2 ";

                sb.Append("SELECT JCSET.JOB_CARD_TRN_PK JOBPK,");
                sb.Append("       MAX(JCSET.JOBCARD_REF_NO) JOBREFNR,");
                sb.Append("       TO_DATE(MAX(JCSET.JOBCARD_DATE), DATEFORMAT) JOBDATE,");
                sb.Append("       MAX(SHP.CUSTOMER_NAME) SHIPPER,");
                sb.Append("       MAX(CONS.CUSTOMER_NAME) CONSIGNEE,");
                sb.Append("       MAX(FRTPYR.CUSTOMER_MST_PK) CUST_PK,");
                sb.Append("       MAX(FRTPYR.CUSTOMER_NAME) CUST,");
                sb.Append("    'Air' BUSINESS_TYPE,");
                sb.Append("    'Import' PROCESS_TYPE,");
                sb.Append("    MAX(POL.PORT_ID) POL,");
                sb.Append("       MAX(POD.PORT_ID) POD,");
                sb.Append("       MAX(JCSET.ETD_DATE) ETD_DATE,");
                sb.Append("       MAX(JCSET.ETA_DATE) ETA,");
                sb.Append("       SUM(NVL(JTSEF.FREIGHT_AMT, 0) * JTSEF.EXCHANGE_RATE) FRAMT,");
                sb.Append("     '' SEL");
                sb.Append("       FROM JOB_CARD_TRN JCSET,");
                sb.Append("       JOB_TRN_FD   JTSEF,");
                sb.Append("       CUSTOMER_MST_TBL     SHP,");
                sb.Append("       CUSTOMER_MST_TBL     CONS,");
                sb.Append("       CUSTOMER_MST_TBL     FRTPYR,");
                sb.Append("       PORT_MST_TBL         POL,");
                sb.Append("       PORT_MST_TBL      POD");

                sb.Append("       WHERE(POD.PORT_MST_PK = JCSET.PORT_MST_POD_FK)");
                sb.Append("       AND POL.PORT_MST_PK = JCSET.PORT_MST_POL_FK");
                sb.Append("       AND SHP.CUSTOMER_MST_PK = JCSET.SHIPPER_CUST_MST_FK");
                sb.Append("       AND CONS.CUSTOMER_MST_PK(+) = JCSET.CONSIGNEE_CUST_MST_FK");
                sb.Append("       AND FRTPYR.CUSTOMER_MST_PK(+) = JTSEF.FRTPAYER_CUST_MST_FK");
                sb.Append("       AND JTSEF.CONSOL_INVOICE_TRN_FK IS NULL");
                sb.Append("       AND ABS((NVL(JTSEF.FREIGHT_AMT, 0) * JTSEF.EXCHANGE_RATE)) > 0");
                sb.Append("       AND JCSET.JOB_CARD_STATUS <> 2");
                sb.Append("       AND JTSEF.FREIGHT_TYPE = 2");
                sb.Append("       AND JTSEF.JOB_CARD_TRN_FK = JCSET.JOB_CARD_TRN_PK");
                sb.Append("       AND POD.LOCATION_MST_FK = ");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("       FROM LOCATION_MST_TBL L");
                sb.Append("       WHERE L.LOCATION_MST_PK =" + LocFk + ")");
                sb.Append(strCondition);
                sb.Append("       GROUP BY JCSET.JOB_CARD_TRN_PK");
                sb.Append("        UNION ALL");
                sb.Append("       SELECT JCSET.JOB_CARD_TRN_PK JOBPK,");
                sb.Append("        MAX(JCSET.JOBCARD_REF_NO) JOBREFNR,");
                sb.Append("       TO_DATE(MAX(JCSET.JOBCARD_DATE), DATEFORMAT) JOBDATE,");
                sb.Append("       MAX(SHP.CUSTOMER_NAME) SHIPPER,");
                sb.Append("       MAX(CONS.CUSTOMER_NAME) CONSIGNEE,");
                sb.Append("       MAX(FRTPYR.CUSTOMER_MST_PK) CUST_PK,");
                sb.Append("       MAX(FRTPYR.CUSTOMER_NAME) CUST,");
                sb.Append("    'Air' BUSINESS_TYPE,");
                sb.Append("    'Import' PROCESS_TYPE,");
                sb.Append("    MAX(POL.PORT_ID) POL,");
                sb.Append("       MAX(POD.PORT_ID) POD,");
                sb.Append("       MAX(JCSET.ETD_DATE) ETD_DATE,");
                sb.Append("    MAX(JCSET.ETA_DATE) ETA,");
                sb.Append("       SUM(NVL(JOTSEF.AMOUNT, 0) * JOTSEF.EXCHANGE_RATE) FRAMT,");
                sb.Append("     '' SEL");
                sb.Append("       FROM JOB_CARD_TRN JCSET,");
                sb.Append("       JOB_TRN_OTH_CHRG   JOTSEF,");
                sb.Append("       CUSTOMER_MST_TBL     SHP,");
                sb.Append("       CUSTOMER_MST_TBL     CONS,");
                sb.Append("       CUSTOMER_MST_TBL     FRTPYR,");
                sb.Append("       PORT_MST_TBL         POL,");
                sb.Append("       PORT_MST_TBL      POD");

                sb.Append("       WHERE(POD.PORT_MST_PK = JCSET.PORT_MST_POD_FK)");
                sb.Append("       AND POL.PORT_MST_PK = JCSET.PORT_MST_POL_FK");
                sb.Append("       AND SHP.CUSTOMER_MST_PK = JCSET.SHIPPER_CUST_MST_FK");
                sb.Append("       AND CONS.CUSTOMER_MST_PK(+) = JCSET.CONSIGNEE_CUST_MST_FK");
                sb.Append("       AND FRTPYR.CUSTOMER_MST_PK(+) = JOTSEF.FRTPAYER_CUST_MST_FK");
                sb.Append("       AND JOTSEF.CONSOL_INVOICE_TRN_FK IS NULL");
                sb.Append("       AND ABS((NVL(JOTSEF.AMOUNT, 0) * JOTSEF.EXCHANGE_RATE)) > 0");
                sb.Append("       AND JCSET.JOB_CARD_STATUS <> 2");
                sb.Append("       AND JOTSEF.FREIGHT_TYPE = 2");
                sb.Append("       AND JOTSEF.JOB_CARD_TRN_FK = JCSET.JOB_CARD_TRN_PK");
                sb.Append("       AND POD.LOCATION_MST_FK = ");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("       FROM LOCATION_MST_TBL L");
                sb.Append("       WHERE L.LOCATION_MST_PK =" + LocFk + ")");
                sb.Append(strCondition);
                sb.Append("       GROUP BY JCSET.JOB_CARD_TRN_PK");
                sb.Append("       ORDER BY JOBDATE DESC, JOBREFNR DESC");
                strSQL = " select count(*) from (";
                strSQL += sb.ToString() + ")";

                TotalRecords = (Int32)objWF.GetDataSet(strSQL).Tables[0].Rows[0][0];
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
                if (Excel == 1)
                {
                    strSQL += " )q ) ";
                }
                else
                {
                    strSQL += " )q ) WHERE SR_NO Between " + start + " and " + last;
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

        #endregion "Fetch Job Card for Air-Import"

        #region "Fetch All"

        /// <summary>
        /// Fetches the jc all.
        /// </summary>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="ETADt">The eta dt.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <param name="Excel">The excel.</param>
        /// <returns></returns>
        public DataSet FetchJCAll(Int32 LocFk = 0, string FromDt = "", string ToDt = "", string CustName = "", string ETDDt = "", string ETADt = "", Int32 flag = 0, Int32 TotalPage = 0, Int32 CurrentPage = 0, string BizType = "Both",
        string ProcessType = "All", int Excel = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strSQL = null;
            string strSQL1 = null;
            Int32 last = 0;
            Int32 start = 0;
            Int32 TotalRecords = default(Int32);
            try
            {
                sb.Append("SELECT JCSET.JOB_CARD_TRN_PK JOBPK,");
                sb.Append("    MAX(JCSET.JOBCARD_REF_NO) JOBREFNR,");
                sb.Append("   TO_DATE(MAX(JCSET.JOBCARD_DATE),DATEFORMAT) JOBDATE,");
                sb.Append("   MAX(SHP.CUSTOMER_NAME) SHIPPER,");
                sb.Append("    MAX(CONS.CUSTOMER_NAME) CONSIGNEE,");
                sb.Append("      MAX(FRTPYR.CUSTOMER_MST_PK) CUST_PK,");
                sb.Append("    MAX(FRTPYR.CUSTOMER_NAME) CUST,");
                sb.Append("    'Sea' BUSINESS_TYPE,");
                sb.Append("    'Export' PROCESS_TYPE,");
                sb.Append("    MAX(POL.PORT_ID) POL,");
                sb.Append("    MAX(POD.PORT_ID) POD,");
                sb.Append("    MAX(JCSET.ETD_DATE) ETD_DATE,");
                sb.Append("    MAX(JCSET.ETA_DATE) ETA,");
                sb.Append("    SUM(NVL(JTSEF.FREIGHT_AMT, 0) * JTSEF.EXCHANGE_RATE) FRAMT,");
                sb.Append("     '' SEL");
                sb.Append("    FROM BOOKING_MST_TBL      BST,");
                sb.Append("    JOB_CARD_TRN JCSET,");
                sb.Append("    JOB_TRN_FD   JTSEF, ");
                sb.Append("    CUSTOMER_MST_TBL     SHP,");
                sb.Append("    CUSTOMER_MST_TBL     CONS,");
                sb.Append("    CUSTOMER_MST_TBL     FRTPYR,");
                sb.Append("    PORT_MST_TBL         POL,");
                sb.Append("    PORT_MST_TBL         POD");
                sb.Append("    WHERE(BST.BOOKING_MST_PK = JCSET.BOOKING_MST_FK)");
                sb.Append("    AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
                sb.Append("    AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
                sb.Append("    AND SHP.CUSTOMER_MST_PK = JCSET.SHIPPER_CUST_MST_FK");
                sb.Append("    AND CONS.CUSTOMER_MST_PK(+) = JCSET.CONSIGNEE_CUST_MST_FK");
                sb.Append("    AND FRTPYR.CUSTOMER_MST_PK(+) = JTSEF.FRTPAYER_CUST_MST_FK");
                sb.Append("    AND JTSEF.CONSOL_INVOICE_TRN_FK IS NULL");
                sb.Append("    AND ABS((NVL(JTSEF.FREIGHT_AMT, 0) * JTSEF.EXCHANGE_RATE)) > 0");
                sb.Append("    AND BST.STATUS <> 3");
                sb.Append("    AND JCSET.BUSINESS_TYPE =  2 ");
                sb.Append("    AND JCSET.PROCESS_TYPE =  1 ");
                sb.Append("    AND JCSET.JOB_CARD_STATUS <> 2");
                sb.Append("    AND JTSEF.FREIGHT_TYPE = 1");
                sb.Append("    AND JTSEF.JOB_CARD_TRN_FK = JCSET.JOB_CARD_TRN_PK");
                sb.Append("    AND POL.LOCATION_MST_FK =(SELECT L.LOCATION_MST_PK");
                sb.Append("    FROM LOCATION_MST_TBL L");
                sb.Append("    WHERE L.LOCATION_MST_PK =" + LocFk + ")");

                if (!(FromDt == null | string.IsNullOrEmpty(FromDt)))
                {
                    sb.Append(" And TO_DATE(JCSET.JOBCARD_DATE,DATEFORMAT) >= TO_DATE('" + FromDt + "',dateformat)");
                }
                if (!(ToDt == null | string.IsNullOrEmpty(ToDt)))
                {
                    sb.Append(" And TO_DATE(JCSET.JOBCARD_DATE,DATEFORMAT) <= TO_DATE('" + ToDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append(" And TO_DATE(JCSET.ETD_DATE,dateformat) = TO_DATE('" + ETDDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(ETADt))
                {
                    sb.Append(" And TO_DATE(JCSET.ETA_DATE,dateformat) = TO_DATE('" + ETADt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    sb.Append(" And FRTPYR.CUSTOMER_NAME = '" + CustName + "'");
                }
                sb.Append("    GROUP BY JCSET.JOB_CARD_TRN_PK");
                sb.Append("    UNION ALL");
                sb.Append("    SELECT JCSET.JOB_CARD_TRN_PK JOBPK,");
                sb.Append("    MAX(JCSET.JOBCARD_REF_NO) JOBREFNR,");
                sb.Append("    TO_DATE(MAX(JCSET.JOBCARD_DATE),DATEFORMAT) JOBDATE,");
                sb.Append("    MAX(SHP.CUSTOMER_NAME) SHIPPER,");
                sb.Append("    MAX(CONS.CUSTOMER_NAME) CONSIGNEE,");
                sb.Append("    MAX(FRTPYR.CUSTOMER_MST_PK) CUST_PK,");
                sb.Append("    MAX(FRTPYR.CUSTOMER_NAME) CUST,");
                sb.Append("    'Sea' BUSINESS_TYPE,");
                sb.Append("    'Export' PROCESS_TYPE,");
                sb.Append("    MAX(POL.PORT_ID) POL,");
                sb.Append("    MAX(POD.PORT_ID) POD,");
                sb.Append("    MAX(JCSET.ETD_DATE) ETD_DATE,");
                sb.Append("    MAX(JCSET.ETA_DATE) ETA,");
                sb.Append("    SUM(NVL(JOTSEF.AMOUNT, 0) * JOTSEF.EXCHANGE_RATE) FRAMT,");
                sb.Append("     '' SEL");
                sb.Append("    FROM BOOKING_MST_TBL      BST,");
                sb.Append("    JOB_CARD_TRN JCSET,");
                sb.Append("    JOB_TRN_OTH_CHRG   JOTSEF,");
                sb.Append("    CUSTOMER_MST_TBL     SHP,");
                sb.Append("    CUSTOMER_MST_TBL     CONS,");
                sb.Append("    CUSTOMER_MST_TBL     FRTPYR,");
                sb.Append("    PORT_MST_TBL         POL,");
                sb.Append("    PORT_MST_TBL         POD");
                sb.Append("    WHERE(BST.BOOKING_MST_PK = JCSET.BOOKING_MST_FK)");
                sb.Append("    AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
                sb.Append("    AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
                sb.Append("    AND SHP.CUSTOMER_MST_PK = JCSET.SHIPPER_CUST_MST_FK");
                sb.Append("    AND CONS.CUSTOMER_MST_PK(+) = JCSET.CONSIGNEE_CUST_MST_FK");
                sb.Append("    AND FRTPYR.CUSTOMER_MST_PK(+) = JOTSEF.FRTPAYER_CUST_MST_FK");
                sb.Append("    AND JOTSEF.CONSOL_INVOICE_TRN_FK IS NULL");
                sb.Append("    AND ABS((NVL(JOTSEF.AMOUNT, 0) * JOTSEF.EXCHANGE_RATE)) > 0");
                sb.Append("    AND BST.STATUS <> 3");
                sb.Append("    AND JCSET.BUSINESS_TYPE =  2 ");
                sb.Append("    AND JCSET.PROCESS_TYPE =  1 ");
                sb.Append("    AND JCSET.JOB_CARD_STATUS <> 2");
                sb.Append("    AND JOTSEF.FREIGHT_TYPE = 1");
                sb.Append("    AND JOTSEF.JOB_CARD_TRN_FK = JCSET.JOB_CARD_TRN_PK");
                sb.Append("    AND POL.LOCATION_MST_FK =(SELECT L.LOCATION_MST_PK");
                sb.Append("    FROM LOCATION_MST_TBL L");
                sb.Append("    WHERE L.LOCATION_MST_PK =" + LocFk + ")");
                if (!(FromDt == null | string.IsNullOrEmpty(FromDt)))
                {
                    sb.Append(" And TO_DATE(JCSET.JOBCARD_DATE,DATEFORMAT) >= TO_DATE('" + FromDt + "',dateformat)");
                }
                if (!(ToDt == null | string.IsNullOrEmpty(ToDt)))
                {
                    sb.Append(" And TO_DATE(JCSET.JOBCARD_DATE,DATEFORMAT) <= TO_DATE('" + ToDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append(" And TO_DATE(JCSET.ETD_DATE,dateformat) = TO_DATE('" + ETDDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(ETADt))
                {
                    sb.Append(" And TO_DATE(JCSET.ETA_DATE,dateformat) = TO_DATE('" + ETADt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    sb.Append(" And FRTPYR.CUSTOMER_NAME = '" + CustName + "'");
                }
                sb.Append("    GROUP BY JCSET.JOB_CARD_TRN_PK");

                sb.Append(" UNION ");

                sb.Append(" SELECT JCSET.JOB_CARD_TRN_PK JOBPK,");
                sb.Append("  MAX(JCSET.JOBCARD_REF_NO) JOBREFNR,");
                sb.Append("  TO_DATE(MAX(JCSET.JOBCARD_DATE),DATEFORMAT) JOBDATE,");
                sb.Append("  MAX(SHP.CUSTOMER_NAME) SHIPPER,");
                sb.Append("  MAX(CONS.CUSTOMER_NAME) CONSIGNEE,");
                sb.Append("  MAX(FRTPYR.CUSTOMER_MST_PK) CUST_PK,");
                sb.Append("  MAX(FRTPYR.CUSTOMER_NAME) CUST,");
                sb.Append("    'Air' BUSINESS_TYPE,");
                sb.Append("    'Export' PROCESS_TYPE,");
                sb.Append("  MAX(POL.PORT_ID) POL,");
                sb.Append("  MAX(POD.PORT_ID) POD,");
                sb.Append("    MAX(JCSET.ETD_DATE) ETD_DATE,");
                sb.Append("    MAX(JCSET.ETA_DATE) ETA,");
                sb.Append("  SUM(NVL(JTSEF.FREIGHT_AMT, 0) * JTSEF.EXCHANGE_RATE) FRAMT,");
                sb.Append("     '' SEL");
                sb.Append("       FROM BOOKING_MST_TBL      BST,");
                sb.Append("       JOB_CARD_TRN JCSET,");
                sb.Append("       JOB_TRN_FD   JTSEF,");
                sb.Append("       CUSTOMER_MST_TBL     SHP,");
                sb.Append("       CUSTOMER_MST_TBL     CONS,");
                sb.Append("       CUSTOMER_MST_TBL     FRTPYR,");
                sb.Append("        PORT_MST_TBL         POL,");
                sb.Append("       PORT_MST_TBL          POD");

                sb.Append("       WHERE(BST.BOOKING_MST_PK = JCSET.BOOKING_MST_FK)");
                sb.Append("       AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
                sb.Append("       AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
                sb.Append("       AND SHP.CUSTOMER_MST_PK = JCSET.SHIPPER_CUST_MST_FK");
                sb.Append("       AND CONS.CUSTOMER_MST_PK(+) = JCSET.CONSIGNEE_CUST_MST_FK");
                sb.Append("       AND FRTPYR.CUSTOMER_MST_PK(+) = JTSEF.FRTPAYER_CUST_MST_FK");
                sb.Append("       AND JTSEF.CONSOL_INVOICE_TRN_FK IS NULL");
                sb.Append("       AND ABS((NVL(JTSEF.FREIGHT_AMT, 0) * JTSEF.EXCHANGE_RATE)) > 0");
                sb.Append("       AND BST.STATUS <> 3");
                sb.Append("       AND JCSET.BUSINESS_TYPE =  1 ");
                sb.Append("       AND JCSET.PROCESS_TYPE =  1 ");
                sb.Append("       AND JCSET.JOB_CARD_STATUS <> 2");
                sb.Append("       AND JTSEF.FREIGHT_TYPE = 1");
                sb.Append("       AND JTSEF.JOB_CARD_TRN_FK = JCSET.JOB_CARD_TRN_PK");
                sb.Append("       AND POL.LOCATION_MST_FK = (SELECT L.LOCATION_MST_PK");
                sb.Append("       FROM LOCATION_MST_TBL L");
                sb.Append("    WHERE L.LOCATION_MST_PK =" + LocFk + ")");
                if (!(FromDt == null | string.IsNullOrEmpty(FromDt)))
                {
                    sb.Append(" And TO_DATE(JCSET.JOBCARD_DATE,DATEFORMAT) >= TO_DATE('" + FromDt + "',dateformat)");
                }
                if (!(ToDt == null | string.IsNullOrEmpty(ToDt)))
                {
                    sb.Append(" And TO_DATE(JCSET.JOBCARD_DATE,DATEFORMAT) <= TO_DATE('" + ToDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append(" And TO_DATE(JCSET.ETD_DATE,dateformat) = TO_DATE('" + ETDDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(ETADt))
                {
                    sb.Append(" And TO_DATE(JCSET.ETA_DATE,dateformat) = TO_DATE('" + ETADt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    sb.Append(" And FRTPYR.CUSTOMER_NAME = '" + CustName + "'");
                }
                sb.Append("       GROUP BY JCSET.JOB_CARD_TRN_PK");
                sb.Append("                     UNION ALL");
                sb.Append("       SELECT JCSET.JOB_CARD_TRN_PK JOBPK,");
                sb.Append("        MAX(JCSET.JOBCARD_REF_NO) JOBREFNR,");
                sb.Append("        TO_DATE(MAX(JCSET.JOBCARD_DATE),DATEFORMAT) JOBDATE,");
                sb.Append("       MAX(SHP.CUSTOMER_NAME) SHIPPER,");
                sb.Append("       MAX(CONS.CUSTOMER_NAME) CONSIGNEE,");
                sb.Append("       MAX(FRTPYR.CUSTOMER_MST_PK) CUST_PK,");
                sb.Append("       MAX(FRTPYR.CUSTOMER_NAME) CUST,");
                sb.Append("    'Air' BUSINESS_TYPE,");
                sb.Append("    'Export' PROCESS_TYPE,");
                sb.Append("       MAX(POL.PORT_ID) POL,");
                sb.Append("       MAX(POD.PORT_ID) POD,");
                sb.Append("       MAX(JCSET.ETD_DATE) ETD_DATE,");
                sb.Append("       MAX(JCSET.ETA_DATE) ETA,");
                sb.Append("       SUM(NVL(JOTSEF.AMOUNT, 0) * JOTSEF.EXCHANGE_RATE) FRAMT,");
                sb.Append("     '' SEL");
                sb.Append("        FROM BOOKING_MST_TBL      BST,");
                sb.Append("        JOB_CARD_TRN JCSET,");
                sb.Append("        JOB_TRN_OTH_CHRG   JOTSEF,");
                sb.Append("        CUSTOMER_MST_TBL     SHP,");
                sb.Append("        CUSTOMER_MST_TBL     CONS,");
                sb.Append("        CUSTOMER_MST_TBL     FRTPYR,");
                sb.Append("       PORT_MST_TBL         POL,");
                sb.Append("       PORT_MST_TBL          POD");

                sb.Append("        WHERE(BST.BOOKING_MST_PK = JCSET.BOOKING_MST_FK)");
                sb.Append("       AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
                sb.Append("       AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
                sb.Append("        AND SHP.CUSTOMER_MST_PK = JCSET.SHIPPER_CUST_MST_FK");
                sb.Append("       AND CONS.CUSTOMER_MST_PK(+) = JCSET.CONSIGNEE_CUST_MST_FK");
                sb.Append("       AND FRTPYR.CUSTOMER_MST_PK(+) = JOTSEF.FRTPAYER_CUST_MST_FK");
                sb.Append("        AND JOTSEF.CONSOL_INVOICE_TRN_FK IS NULL");
                sb.Append("        AND ABS((NVL(JOTSEF.AMOUNT, 0) * JOTSEF.EXCHANGE_RATE)) > 0");
                sb.Append("        AND BST.STATUS <> 3");
                sb.Append("       AND JCSET.BUSINESS_TYPE =  1 ");
                sb.Append("       AND JCSET.PROCESS_TYPE =  1 ");
                sb.Append("       AND JCSET.JOB_CARD_STATUS <> 2");
                sb.Append("        AND JOTSEF.FREIGHT_TYPE = 1");
                sb.Append("       AND JOTSEF.JOB_CARD_TRN_FK = JCSET.JOB_CARD_TRN_PK");
                sb.Append("     AND POL.LOCATION_MST_FK =(SELECT L.LOCATION_MST_PK");
                sb.Append("     FROM LOCATION_MST_TBL L");
                sb.Append("     WHERE L.LOCATION_MST_PK =" + LocFk + ")");
                if (!(FromDt == null | string.IsNullOrEmpty(FromDt)))
                {
                    sb.Append(" And TO_DATE(JCSET.JOBCARD_DATE,DATEFORMAT) >= TO_DATE('" + FromDt + "',dateformat)");
                }
                if (!(ToDt == null | string.IsNullOrEmpty(ToDt)))
                {
                    sb.Append(" And TO_DATE(JCSET.JOBCARD_DATE,DATEFORMAT) <= TO_DATE('" + ToDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append(" And TO_DATE(JCSET.ETD_DATE,dateformat) = TO_DATE('" + ETDDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(ETADt))
                {
                    sb.Append(" And TO_DATE(JCSET.ETA_DATE,dateformat) = TO_DATE('" + ETADt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    sb.Append(" And FRTPYR.CUSTOMER_NAME = '" + CustName + "'");
                }
                sb.Append("       GROUP BY JCSET.JOB_CARD_TRN_PK");

                sb.Append(" UNION ");

                sb.Append("SELECT JCSET.JOB_CARD_TRN_PK JOBPK,");
                sb.Append("       MAX(JCSET.JOBCARD_REF_NO) JOBREFNR,");
                sb.Append("       TO_DATE(MAX(JCSET.JOBCARD_DATE),DATEFORMAT) JOBDATE,");
                sb.Append("       MAX(SHP.CUSTOMER_NAME) SHIPPER,");
                sb.Append("       MAX(CONS.CUSTOMER_NAME) CONSIGNEE,");
                sb.Append("       MAX(FRTPYR.CUSTOMER_MST_PK) CUST_PK,");
                sb.Append("       MAX(FRTPYR.CUSTOMER_NAME) CUST,");
                sb.Append("    'Sea' BUSINESS_TYPE,");
                sb.Append("    'Import' PROCESS_TYPE,");
                sb.Append("    MAX(POL.PORT_ID) POL,");
                sb.Append("       MAX(POD.PORT_ID) POD,");
                sb.Append("       MAX(JCSET.ETD_DATE) ETD_DATE,");
                sb.Append("       MAX(JCSET.ETA_DATE) ETA,");
                sb.Append("       SUM(NVL(JTSEF.FREIGHT_AMT, 0) * JTSEF.EXCHANGE_RATE) FRAMT,");
                sb.Append("     '' SEL");
                sb.Append("        FROM JOB_CARD_TRN JCSET,");
                sb.Append("       JOB_TRN_FD   JTSEF,");
                sb.Append("       CUSTOMER_MST_TBL     SHP,");
                sb.Append("       CUSTOMER_MST_TBL     CONS,");
                sb.Append("       CUSTOMER_MST_TBL     FRTPYR,");
                sb.Append("       PORT_MST_TBL         POL,");
                sb.Append("       PORT_MST_TBL          POD");

                sb.Append("       WHERE(POD.PORT_MST_PK = JCSET.PORT_MST_POD_FK)");
                sb.Append("       AND POL.PORT_MST_PK = JCSET.PORT_MST_POL_FK");
                sb.Append("       AND SHP.CUSTOMER_MST_PK = JCSET.SHIPPER_CUST_MST_FK");
                sb.Append("       AND CONS.CUSTOMER_MST_PK(+) = JCSET.CONSIGNEE_CUST_MST_FK");
                sb.Append("       AND FRTPYR.CUSTOMER_MST_PK(+) = JTSEF.FRTPAYER_CUST_MST_FK");
                sb.Append("       AND JTSEF.CONSOL_INVOICE_TRN_FK IS NULL");
                sb.Append("       AND ABS((NVL(JTSEF.FREIGHT_AMT, 0) * JTSEF.EXCHANGE_RATE)) > 0");
                sb.Append("       AND JCSET.JOB_CARD_STATUS <> 2");
                sb.Append("       AND JTSEF.FREIGHT_TYPE = 2");
                sb.Append("       AND JCSET.BUSINESS_TYPE =  2 ");
                sb.Append("       AND JCSET.PROCESS_TYPE =  2 ");
                sb.Append("       AND JTSEF.JOB_CARD_TRN_FK = JCSET.JOB_CARD_TRN_PK");
                sb.Append("       AND POD.LOCATION_MST_FK = ");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("       FROM LOCATION_MST_TBL L");
                sb.Append("       WHERE L.LOCATION_MST_PK =" + LocFk + ")");

                if (!(FromDt == null | string.IsNullOrEmpty(FromDt)))
                {
                    sb.Append(" And TO_DATE(JCSET.JOBCARD_DATE,DATEFORMAT) >= TO_DATE('" + FromDt + "',dateformat)");
                }
                if (!(ToDt == null | string.IsNullOrEmpty(ToDt)))
                {
                    sb.Append(" And TO_DATE(JCSET.JOBCARD_DATE,DATEFORMAT) <= TO_DATE('" + ToDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append(" And TO_DATE(JCSET.ETD_DATE,dateformat) = TO_DATE('" + ETDDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(ETADt))
                {
                    sb.Append(" And TO_DATE(JCSET.ETA_DATE,dateformat) = TO_DATE('" + ETADt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    sb.Append(" And FRTPYR.CUSTOMER_NAME = '" + CustName + "'");
                }
                sb.Append("       GROUP BY JCSET.JOB_CARD_TRN_PK");
                sb.Append("        UNION ALL");
                sb.Append("       SELECT JCSET.JOB_CARD_TRN_PK JOBPK,");
                sb.Append("       MAX(JCSET.JOBCARD_REF_NO) JOBREFNR,");
                sb.Append("       TO_DATE(MAX(JCSET.JOBCARD_DATE),DATEFORMAT) JOBDATE,");
                sb.Append("       MAX(SHP.CUSTOMER_NAME) SHIPPER,");
                sb.Append("       MAX(CONS.CUSTOMER_NAME) CONSIGNEE,");
                sb.Append("       MAX(FRTPYR.CUSTOMER_MST_PK) CUST_PK,");
                sb.Append("       MAX(FRTPYR.CUSTOMER_NAME) CUST,");
                sb.Append("    'Sea' BUSINESS_TYPE,");
                sb.Append("    'Import' PROCESS_TYPE,");
                sb.Append("    MAX(POL.PORT_ID) POL,");
                sb.Append("       MAX(POD.PORT_ID) POD,");
                sb.Append("       MAX(JCSET.ETD_DATE) ETD_DATE,");
                sb.Append("       MAX(JCSET.ETA_DATE) ETA,");
                sb.Append("       SUM(NVL(JOTSEF.AMOUNT, 0) * JOTSEF.EXCHANGE_RATE) FRAMT,");
                sb.Append("     '' SEL");
                sb.Append("       FROM JOB_CARD_TRN JCSET,");
                sb.Append("       JOB_TRN_OTH_CHRG   JOTSEF,");
                sb.Append("       CUSTOMER_MST_TBL     SHP,");
                sb.Append("       CUSTOMER_MST_TBL     CONS,");
                sb.Append("       CUSTOMER_MST_TBL     FRTPYR,");
                sb.Append("       PORT_MST_TBL         POL,");
                sb.Append("        PORT_MST_TBL     POD");

                sb.Append("       WHERE(POD.PORT_MST_PK = JCSET.PORT_MST_POD_FK)");
                sb.Append("       AND POL.PORT_MST_PK = JCSET.PORT_MST_POL_FK");
                sb.Append("       AND SHP.CUSTOMER_MST_PK = JCSET.SHIPPER_CUST_MST_FK");
                sb.Append("       AND CONS.CUSTOMER_MST_PK(+) = JCSET.CONSIGNEE_CUST_MST_FK");
                sb.Append("       AND FRTPYR.CUSTOMER_MST_PK(+) = JOTSEF.FRTPAYER_CUST_MST_FK");
                sb.Append("       AND JOTSEF.CONSOL_INVOICE_TRN_FK IS NULL");
                sb.Append("       AND ABS((NVL(JOTSEF.AMOUNT, 0) * JOTSEF.EXCHANGE_RATE)) > 0");
                sb.Append("       AND JCSET.JOB_CARD_STATUS <> 2");
                sb.Append("       AND JCSET.BUSINESS_TYPE =  2 ");
                sb.Append("       AND JCSET.PROCESS_TYPE =  2 ");
                sb.Append("       AND JOTSEF.FREIGHT_TYPE = 2");
                sb.Append("       AND JOTSEF.JOB_CARD_TRN_FK = JCSET.JOB_CARD_TRN_PK");
                sb.Append("       AND POD.LOCATION_MST_FK = ");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("       FROM LOCATION_MST_TBL L");
                sb.Append("       WHERE L.LOCATION_MST_PK =" + LocFk + ")");
                if (!(FromDt == null | string.IsNullOrEmpty(FromDt)))
                {
                    sb.Append(" And TO_DATE(JCSET.JOBCARD_DATE,DATEFORMAT) >= TO_DATE('" + FromDt + "',dateformat)");
                }
                if (!(ToDt == null | string.IsNullOrEmpty(ToDt)))
                {
                    sb.Append(" And TO_DATE(JCSET.JOBCARD_DATE,DATEFORMAT) <= TO_DATE('" + ToDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append(" And TO_DATE(JCSET.ETD_DATE,dateformat) = TO_DATE('" + ETDDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(ETADt))
                {
                    sb.Append(" And TO_DATE(JCSET.ETA_DATE,dateformat) = TO_DATE('" + ETADt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    sb.Append(" And FRTPYR.CUSTOMER_NAME = '" + CustName + "'");
                }
                sb.Append("       GROUP BY JCSET.JOB_CARD_TRN_PK");

                sb.Append(" UNION ");

                sb.Append(" SELECT JCSET.JOB_CARD_TRN_PK JOBPK,");
                sb.Append("       MAX(JCSET.JOBCARD_REF_NO) JOBREFNR,");
                sb.Append("       TO_DATE(MAX(JCSET.JOBCARD_DATE),DATEFORMAT) JOBDATE,");
                sb.Append("       MAX(SHP.CUSTOMER_NAME) SHIPPER,");
                sb.Append("       MAX(CONS.CUSTOMER_NAME) CONSIGNEE,");
                sb.Append("       MAX(FRTPYR.CUSTOMER_MST_PK) CUST_PK,");
                sb.Append("       MAX(FRTPYR.CUSTOMER_NAME) CUST,");
                sb.Append("    'Air' BUSINESS_TYPE,");
                sb.Append("    'Import' PROCESS_TYPE,");
                sb.Append("    MAX(POL.PORT_ID) POL,");
                sb.Append("       MAX(POD.PORT_ID) POD,");
                sb.Append("       MAX(JCSET.ETD_DATE) ETD_DATE,");
                sb.Append("       MAX(JCSET.ETA_DATE) ETA,");
                sb.Append("       SUM(NVL(JTSEF.FREIGHT_AMT, 0) * JTSEF.EXCHANGE_RATE) FRAMT,");
                sb.Append("     '' SEL");
                sb.Append("       FROM JOB_CARD_TRN JCSET,");
                sb.Append("       JOB_TRN_FD   JTSEF,");
                sb.Append("       CUSTOMER_MST_TBL     SHP,");
                sb.Append("       CUSTOMER_MST_TBL     CONS,");
                sb.Append("       CUSTOMER_MST_TBL     FRTPYR,");
                sb.Append("       PORT_MST_TBL         POL,");
                sb.Append("       PORT_MST_TBL      POD");

                sb.Append("       WHERE(POD.PORT_MST_PK = JCSET.PORT_MST_POD_FK)");
                sb.Append("       AND POL.PORT_MST_PK = JCSET.PORT_MST_POL_FK");
                sb.Append("       AND SHP.CUSTOMER_MST_PK = JCSET.SHIPPER_CUST_MST_FK");
                sb.Append("       AND CONS.CUSTOMER_MST_PK(+) = JCSET.CONSIGNEE_CUST_MST_FK");
                sb.Append("       AND FRTPYR.CUSTOMER_MST_PK(+) = JTSEF.FRTPAYER_CUST_MST_FK");
                sb.Append("       AND JTSEF.CONSOL_INVOICE_TRN_FK IS NULL");
                sb.Append("       AND ABS((NVL(JTSEF.FREIGHT_AMT, 0) * JTSEF.EXCHANGE_RATE)) > 0");
                sb.Append("       AND JCSET.JOB_CARD_STATUS <> 2");
                sb.Append("       AND JCSET.BUSINESS_TYPE =  1 ");
                sb.Append("       AND JCSET.PROCESS_TYPE =  2 ");
                sb.Append("       AND JTSEF.FREIGHT_TYPE = 2");
                sb.Append("       AND JTSEF.JOB_CARD_TRN_FK = JCSET.JOB_CARD_TRN_PK");
                sb.Append("       AND POD.LOCATION_MST_FK = ");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("       FROM LOCATION_MST_TBL L");
                sb.Append("       WHERE L.LOCATION_MST_PK =" + LocFk + ")");
                if (!(FromDt == null | string.IsNullOrEmpty(FromDt)))
                {
                    sb.Append(" And TO_DATE(JCSET.JOBCARD_DATE,DATEFORMAT) >= TO_DATE('" + FromDt + "',dateformat)");
                }
                if (!(ToDt == null | string.IsNullOrEmpty(ToDt)))
                {
                    sb.Append(" And TO_DATE(JCSET.JOBCARD_DATE,DATEFORMAT) <= TO_DATE('" + ToDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append(" And TO_DATE(JCSET.ETD_DATE,dateformat) = TO_DATE('" + ETDDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(ETADt))
                {
                    sb.Append(" And TO_DATE(JCSET.ETA_DATE,dateformat) = TO_DATE('" + ETADt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    sb.Append(" And FRTPYR.CUSTOMER_NAME = '" + CustName + "'");
                }
                sb.Append("       GROUP BY JCSET.JOB_CARD_TRN_PK");
                sb.Append("        UNION ALL");
                sb.Append("       SELECT JCSET.JOB_CARD_TRN_PK JOBPK,");
                sb.Append("        MAX(JCSET.JOBCARD_REF_NO) JOBREFNR,");
                sb.Append("        TO_DATE(MAX(JCSET.JOBCARD_DATE),DATEFORMAT) JOBDATE,");
                sb.Append("       MAX(SHP.CUSTOMER_NAME) SHIPPER,");
                sb.Append("       MAX(CONS.CUSTOMER_NAME) CONSIGNEE,");
                sb.Append("       MAX(FRTPYR.CUSTOMER_MST_PK) CUST_PK,");
                sb.Append("       MAX(FRTPYR.CUSTOMER_NAME) CUST,");
                sb.Append("    'Air' BUSINESS_TYPE,");
                sb.Append("    'Import' PROCESS_TYPE,");
                sb.Append("    MAX(POL.PORT_ID) POL,");
                sb.Append("       MAX(POD.PORT_ID) POD,");
                sb.Append("       MAX(JCSET.ETD_DATE) ETD_DATE,");
                sb.Append("       MAX(JCSET.ETA_DATE) ETA,");
                sb.Append("       SUM(NVL(JOTSEF.AMOUNT, 0) * JOTSEF.EXCHANGE_RATE) FRAMT,");
                sb.Append("     '' SEL");
                sb.Append("       FROM JOB_CARD_TRN JCSET,");
                sb.Append("       JOB_TRN_OTH_CHRG   JOTSEF,");
                sb.Append("       CUSTOMER_MST_TBL     SHP,");
                sb.Append("       CUSTOMER_MST_TBL     CONS,");
                sb.Append("       CUSTOMER_MST_TBL     FRTPYR,");
                sb.Append("       PORT_MST_TBL         POL,");
                sb.Append("       PORT_MST_TBL      POD");

                sb.Append("       WHERE(POD.PORT_MST_PK = JCSET.PORT_MST_POD_FK)");
                sb.Append("       AND POL.PORT_MST_PK = JCSET.PORT_MST_POL_FK");
                sb.Append("       AND SHP.CUSTOMER_MST_PK = JCSET.SHIPPER_CUST_MST_FK");
                sb.Append("       AND CONS.CUSTOMER_MST_PK(+) = JCSET.CONSIGNEE_CUST_MST_FK");
                sb.Append("       AND FRTPYR.CUSTOMER_MST_PK(+) = JOTSEF.FRTPAYER_CUST_MST_FK");
                sb.Append("       AND JOTSEF.CONSOL_INVOICE_TRN_FK IS NULL");
                sb.Append("       AND ABS((NVL(JOTSEF.AMOUNT, 0) * JOTSEF.EXCHANGE_RATE)) > 0");
                sb.Append("       AND JCSET.JOB_CARD_STATUS <> 2");
                sb.Append("       AND JCSET.BUSINESS_TYPE =  1 ");
                sb.Append("       AND JCSET.PROCESS_TYPE =  2 ");
                sb.Append("       AND JOTSEF.FREIGHT_TYPE = 2");
                sb.Append("       AND JOTSEF.JOB_CARD_TRN_FK = JCSET.JOB_CARD_TRN_PK");
                sb.Append("       AND POD.LOCATION_MST_FK = ");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("       FROM LOCATION_MST_TBL L");
                sb.Append("       WHERE L.LOCATION_MST_PK =" + LocFk + ")");
                if (!(FromDt == null | string.IsNullOrEmpty(FromDt)))
                {
                    sb.Append(" And TO_DATE(JCSET.JOBCARD_DATE,DATEFORMAT) >= TO_DATE('" + FromDt + "',dateformat)");
                }
                if (!(ToDt == null | string.IsNullOrEmpty(ToDt)))
                {
                    sb.Append(" And TO_DATE(JCSET.JOBCARD_DATE,DATEFORMAT) <= TO_DATE('" + ToDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append(" And TO_DATE(JCSET.ETD_DATE,dateformat) = TO_DATE('" + ETDDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(ETADt))
                {
                    sb.Append(" And TO_DATE(JCSET.ETA_DATE,dateformat) = TO_DATE('" + ETADt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    sb.Append(" And FRTPYR.CUSTOMER_NAME = '" + CustName + "'");
                }
                sb.Append("       GROUP BY JCSET.JOB_CARD_TRN_PK");

                strSQL = " SELECT * FROM (SELECT ROWNUM SR_NO, q.* FROM( SELECT * FROM ( ";
                strSQL += sb.ToString();
                strSQL += " ) ORDER BY JOBDATE DESC, JOBREFNR DESC )q ";
                if (BizType != "Both")
                {
                    strSQL += " WHERE BUSINESS_TYPE='" + BizType + "'";
                }
                if (BizType != "Both" & ProcessType != "All")
                {
                    strSQL += " AND PROCESS_TYPE='" + ProcessType + "'";
                }
                else if (ProcessType != "All")
                {
                    strSQL += " WHERE PROCESS_TYPE='" + ProcessType + "'";
                }
                strSQL += " ) ";

                strSQL1 = "select count(*) from (";
                strSQL1 += strSQL + ")";
                TotalRecords = (Int32)objWF.GetDataSet(strSQL1).Tables[0].Rows[0][0];
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
                if (Excel == 0)
                {
                    strSQL += "WHERE SR_NO Between " + start + " and " + last;
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

        #endregion "Fetch All"

        #region "Fetch Job Card for Sea-Export Report"

        /// <summary>
        /// Fetches the jc sea export report.
        /// </summary>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public DataSet FetchJCSeaExportReport(Int32 LocFk = 0, string FromDt = "", string ToDt = "", string CustName = "", string ETDDt = "", Int32 flag = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strCondition = null;
            string strSQL = null;
            try
            {
                if (!(FromDt == null | string.IsNullOrEmpty(FromDt)))
                {
                    strCondition = strCondition + " And JCSET.JOBCARD_DATE >= TO_DATE('" + FromDt + "',dateformat)";
                }
                if (!(ToDt == null | string.IsNullOrEmpty(ToDt)))
                {
                    strCondition = strCondition + " And JCSET.JOBCARD_DATE <= TO_DATE('" + ToDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition = strCondition + " And TO_DATE(JCSET.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    strCondition = strCondition + " And SHP.CUSTOMER_NAME = '" + CustName + "'";
                }
                strCondition = strCondition + " AND JCSET.BUSINESS_TYPE = 2 ";
                strCondition = strCondition + " AND JCSET.PROCESS_TYPE = 1 ";

                sb.Append("SELECT JCSET.JOB_CARD_TRN_PK JOBPK,");
                sb.Append("    MAX(JCSET.JOBCARD_REF_NO) JOBREFNR,");
                sb.Append("   MAX(JCSET.JOBCARD_DATE) JOBDATE,");
                sb.Append("   MAX(SHP.CUSTOMER_NAME) SHIPPER,");
                sb.Append("    MAX(CONS.CUSTOMER_NAME) CONSIGNEE,");
                sb.Append("    MAX(POD.PORT_ID) POD,");
                sb.Append("    MAX(TO_CHAR(JCSET.ETD_DATE, DATETIMEFORMAT24)) ETD_DATE,");
                sb.Append("    SUM(NVL(JTSEF.FREIGHT_AMT, 0) * JTSEF.EXCHANGE_RATE) FRAMT,");
                sb.Append("      MAX(FRTPYR.CUSTOMER_MST_PK) CUST_PK,");
                sb.Append("    MAX(FRTPYR.CUSTOMER_NAME) CUST,");
                sb.Append("     '' SEL");
                sb.Append("    FROM BOOKING_MST_TBL      BST,");
                sb.Append("    JOB_CARD_TRN JCSET,");
                sb.Append("    JOB_TRN_FD   JTSEF, ");
                sb.Append("    CUSTOMER_MST_TBL     SHP,");
                sb.Append("    CUSTOMER_MST_TBL     CONS,");
                sb.Append("    CUSTOMER_MST_TBL     FRTPYR,");
                sb.Append("    PORT_MST_TBL         POL,");
                sb.Append("    PORT_MST_TBL         POD");

                sb.Append("    WHERE(BST.BOOKING_MST_PK = JCSET.BOOKING_MST_FK)");
                sb.Append("    AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
                sb.Append("    AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
                sb.Append("    AND SHP.CUSTOMER_MST_PK = JCSET.SHIPPER_CUST_MST_FK");
                sb.Append("    AND CONS.CUSTOMER_MST_PK(+) = JCSET.CONSIGNEE_CUST_MST_FK");
                sb.Append("    AND FRTPYR.CUSTOMER_MST_PK(+) = JTSEF.FRTPAYER_CUST_MST_FK");
                sb.Append("    AND JTSEF.CONSOL_INVOICE_TRN_FK IS NULL");
                sb.Append("    AND ABS((NVL(JTSEF.FREIGHT_AMT, 0) * JTSEF.EXCHANGE_RATE)) > 0");
                sb.Append("    AND BST.STATUS <> 3");
                sb.Append("    AND JCSET.JOB_CARD_STATUS <> 2");
                sb.Append("    AND JTSEF.FREIGHT_TYPE = 1");
                sb.Append("    AND JTSEF.JOB_CARD_TRN_FK = JCSET.JOB_CARD_TRN_PK");
                sb.Append("    AND POL.LOCATION_MST_FK =(SELECT L.LOCATION_MST_PK");
                sb.Append("    FROM LOCATION_MST_TBL L");
                sb.Append("    WHERE L.LOCATION_MST_PK =" + LocFk + ")");

                sb.Append(strCondition);
                sb.Append("    GROUP BY JCSET.JOB_CARD_TRN_PK");
                sb.Append("    UNION ALL");
                sb.Append("    SELECT JCSET.JOB_CARD_TRN_PK JOBPK,");
                sb.Append("    MAX(JCSET.JOBCARD_REF_NO) JOBREFNR,");
                sb.Append("    MAX(JCSET.JOBCARD_DATE) JOBDATE,");
                sb.Append("    MAX(SHP.CUSTOMER_NAME) SHIPPER,");
                sb.Append("    MAX(CONS.CUSTOMER_NAME) CONSIGNEE,");
                sb.Append("    MAX(POD.PORT_ID) POD,");
                sb.Append("    MAX(TO_CHAR(JCSET.ETD_DATE, DATETIMEFORMAT24)) ETD_DATE,");
                sb.Append("    SUM(NVL(JOTSEF.AMOUNT, 0) * JOTSEF.EXCHANGE_RATE) FRAMT,");
                sb.Append("    MAX(FRTPYR.CUSTOMER_MST_PK) CUST_PK,");
                sb.Append("    MAX(FRTPYR.CUSTOMER_NAME) CUST,");
                sb.Append("     '' SEL");
                sb.Append("    FROM BOOKING_MST_TBL      BST,");
                sb.Append("    JOB_CARD_TRN JCSET,");
                sb.Append("    JOB_TRN_OTH_CHRG   JOTSEF,");
                sb.Append("    CUSTOMER_MST_TBL     SHP,");
                sb.Append("    CUSTOMER_MST_TBL     CONS,");
                sb.Append("    CUSTOMER_MST_TBL     FRTPYR,");
                sb.Append("    PORT_MST_TBL         POL,");
                sb.Append("    PORT_MST_TBL         POD");

                sb.Append("    WHERE(BST.BOOKING_MST_PK = JCSET.BOOKING_MST_FK)");
                sb.Append("    AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
                sb.Append("    AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
                sb.Append("    AND SHP.CUSTOMER_MST_PK = JCSET.SHIPPER_CUST_MST_FK");
                sb.Append("    AND CONS.CUSTOMER_MST_PK(+) = JCSET.CONSIGNEE_CUST_MST_FK");
                sb.Append("    AND FRTPYR.CUSTOMER_MST_PK(+) = JOTSEF.FRTPAYER_CUST_MST_FK");
                sb.Append("    AND JOTSEF.CONSOL_INVOICE_TRN_FK IS NULL");
                sb.Append("    AND ABS((NVL(JOTSEF.AMOUNT, 0) * JOTSEF.EXCHANGE_RATE)) > 0");
                sb.Append("    AND BST.STATUS <> 3");
                sb.Append("    AND JCSET.JOB_CARD_STATUS <> 2");
                sb.Append("    AND JOTSEF.FREIGHT_TYPE = 1");
                sb.Append("    AND JOTSEF.JOB_CARD_TRN_FK = JCSET.JOB_CARD_TRN_PK");
                sb.Append("    AND POL.LOCATION_MST_FK =(SELECT L.LOCATION_MST_PK");
                sb.Append("    FROM LOCATION_MST_TBL L");
                sb.Append("    WHERE L.LOCATION_MST_PK =" + LocFk + ")");
                sb.Append(strCondition);
                sb.Append("    GROUP BY JCSET.JOB_CARD_TRN_PK");
                sb.Append("    ORDER BY JOBDATE DESC, JOBREFNR DESC");
                strSQL = " SELECT * FROM (SELECT ROWNUM SR_NO, q.* FROM(";
                strSQL += sb.ToString();
                strSQL += " )q )";
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

        #endregion "Fetch Job Card for Sea-Export Report"

        #region "Fetch Job Card for Air-Export Report"

        /// <summary>
        /// Fetches the jc air export report.
        /// </summary>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public DataSet FetchJCAirExportReport(Int32 LocFk = 0, string FromDt = "", string ToDt = "", string CustName = "", string ETDDt = "", Int32 flag = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strCondition = null;
            string strSQL = null;
            try
            {
                if (!(FromDt == null | string.IsNullOrEmpty(FromDt)))
                {
                    strCondition = strCondition + " And JCSET.JOBCARD_DATE >= TO_DATE('" + FromDt + "',dateformat)";
                }
                if (!(ToDt == null | string.IsNullOrEmpty(ToDt)))
                {
                    strCondition = strCondition + " And JCSET.JOBCARD_DATE <= TO_DATE('" + ToDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition = strCondition + " And TO_DATE(JCSET.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    strCondition = strCondition + " And SHP.CUSTOMER_NAME = '" + CustName + "'";
                }
                strCondition = strCondition + " AND JCSET.BUSINESS_TYPE = 1 ";
                strCondition = strCondition + " AND JCSET.PROCESS_TYPE = 1 ";

                sb.Append("SELECT JCSET.JOB_CARD_TRN_PK JOBPK,");
                sb.Append("  MAX(JCSET.JOBCARD_REF_NO) JOBREFNR,");
                sb.Append("  MAX(JCSET.JOBCARD_DATE) JOBDATE,");
                sb.Append("  MAX(SHP.CUSTOMER_NAME) SHIPPER,");
                sb.Append("  MAX(CONS.CUSTOMER_NAME) CONSIGNEE,");
                sb.Append("  MAX(POD.PORT_ID) POD,");
                sb.Append("    MAX(TO_CHAR(JCSET.ETD_DATE, DATETIMEFORMAT24)) ETD_DATE,");
                sb.Append("  SUM(NVL(JTSEF.FREIGHT_AMT, 0) * JTSEF.EXCHANGE_RATE) FRAMT,");
                sb.Append("  MAX(FRTPYR.CUSTOMER_MST_PK) CUST_PK,");
                sb.Append("  MAX(FRTPYR.CUSTOMER_NAME) CUST,");
                sb.Append("     '' SEL");

                sb.Append("       FROM BOOKING_MST_TBL      BST,");
                sb.Append("       JOB_CARD_TRN JCSET,");
                sb.Append("       JOB_TRN_FD   JTSEF,");
                sb.Append("       CUSTOMER_MST_TBL     SHP,");
                sb.Append("       CUSTOMER_MST_TBL     CONS,");
                sb.Append("       CUSTOMER_MST_TBL     FRTPYR,");
                sb.Append("        PORT_MST_TBL         POL,");
                sb.Append("       PORT_MST_TBL      POD");

                sb.Append("       WHERE(BST.BOOKING_MST_PK = JCSET.BOOKING_MST_FK)");
                sb.Append("       AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
                sb.Append("       AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
                sb.Append("       AND SHP.CUSTOMER_MST_PK = JCSET.SHIPPER_CUST_MST_FK");
                sb.Append("       AND CONS.CUSTOMER_MST_PK(+) = JCSET.CONSIGNEE_CUST_MST_FK");
                sb.Append("       AND FRTPYR.CUSTOMER_MST_PK(+) = JTSEF.FRTPAYER_CUST_MST_FK");
                sb.Append("       AND JTSEF.CONSOL_INVOICE_TRN_FK IS NULL");
                sb.Append("       AND ABS((NVL(JTSEF.FREIGHT_AMT, 0) * JTSEF.EXCHANGE_RATE)) > 0");
                sb.Append("       AND BST.STATUS <> 3");
                sb.Append("       AND JCSET.JOB_CARD_STATUS <> 2");
                sb.Append("       AND JTSEF.FREIGHT_TYPE = 1");
                sb.Append("       AND JTSEF.JOB_CARD_TRN_FK = JCSET.JOB_CARD_TRN_PK");
                sb.Append("       AND POL.LOCATION_MST_FK = (SELECT L.LOCATION_MST_PK");
                sb.Append("       FROM LOCATION_MST_TBL L");
                sb.Append("    WHERE L.LOCATION_MST_PK =" + LocFk + ")");
                sb.Append(strCondition);
                sb.Append("       GROUP BY JCSET.JOB_CARD_TRN_PK");
                sb.Append("                     UNION ALL");
                sb.Append("       SELECT JCSET.JOB_CARD_TRN_PK JOBPK,");
                sb.Append("        MAX(JCSET.JOBCARD_REF_NO) JOBREFNR,");
                sb.Append("        MAX(JCSET.JOBCARD_DATE) JOBDATE,");
                sb.Append("       MAX(SHP.CUSTOMER_NAME) SHIPPER,");
                sb.Append("       MAX(CONS.CUSTOMER_NAME) CONSIGNEE,");
                sb.Append("       MAX(POD.PORT_ID) POD,");
                sb.Append("       MAX(TO_CHAR(JCSET.ETD_DATE, DATETIMEFORMAT24)) ETD_DATE,");
                sb.Append("       SUM(NVL(JOTSEF.AMOUNT, 0) * JOTSEF.EXCHANGE_RATE) FRAMT,");
                sb.Append("       MAX(FRTPYR.CUSTOMER_MST_PK) CUST_PK,");
                sb.Append("       MAX(FRTPYR.CUSTOMER_NAME) CUST,");
                sb.Append("     '' SEL");
                sb.Append("        FROM BOOKING_MST_TBL      BST,");
                sb.Append("        JOB_CARD_TRN JCSET,");
                sb.Append("        JOB_TRN_OTH_CHRG   JOTSEF,");
                sb.Append("        CUSTOMER_MST_TBL     SHP,");
                sb.Append("        CUSTOMER_MST_TBL     CONS,");
                sb.Append("        CUSTOMER_MST_TBL     FRTPYR,");
                sb.Append("       PORT_MST_TBL         POL,");
                sb.Append("       PORT_MST_TBL      POD");

                sb.Append("        WHERE(BST.BOOKING_MST_PK = JCSET.BOOKING_MST_FK)");
                sb.Append("       AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
                sb.Append("       AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
                sb.Append("        AND SHP.CUSTOMER_MST_PK = JCSET.SHIPPER_CUST_MST_FK");
                sb.Append("       AND CONS.CUSTOMER_MST_PK(+) = JCSET.CONSIGNEE_CUST_MST_FK");
                sb.Append("       AND FRTPYR.CUSTOMER_MST_PK(+) = JOTSEF.FRTPAYER_CUST_MST_FK");
                sb.Append("        AND JOTSEF.CONSOL_INVOICE_TRN_FK IS NULL");
                sb.Append("        AND ABS((NVL(JOTSEF.AMOUNT, 0) * JOTSEF.EXCHANGE_RATE)) > 0");
                sb.Append("        AND BST.STATUS <> 3");
                sb.Append("       AND JCSET.JOB_CARD_STATUS <> 2");
                sb.Append("        AND JOTSEF.FREIGHT_TYPE = 1");
                sb.Append("       AND JOTSEF.JOB_CARD_TRN_FK = JCSET.JOB_CARD_TRN_PK");
                sb.Append("     AND POL.LOCATION_MST_FK =(SELECT L.LOCATION_MST_PK");
                sb.Append("     FROM LOCATION_MST_TBL L");
                sb.Append("     WHERE L.LOCATION_MST_PK =" + LocFk + ")");
                sb.Append(strCondition);
                sb.Append("       GROUP BY JCSET.JOB_CARD_TRN_PK");
                sb.Append("       ORDER BY JOBDATE DESC, JOBREFNR DESC");
                strSQL = " SELECT * FROM (SELECT ROWNUM SR_NO, q.* FROM(";
                strSQL += sb.ToString();
                strSQL += " )q )";
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

        #endregion "Fetch Job Card for Air-Export Report"

        #region "Fetch Job Card for Sea-Import Report"

        /// <summary>
        /// Fetches the jc sea import report.
        /// </summary>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public DataSet FetchJCSeaImportReport(Int32 LocFk = 0, string FromDt = "", string ToDt = "", string CustName = "", string ETDDt = "", Int32 flag = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strCondition = null;
            string strSQL = null;
            try
            {
                if (!(FromDt == null | string.IsNullOrEmpty(FromDt)))
                {
                    strCondition = strCondition + " And JCSET.JOBCARD_DATE >= TO_DATE('" + FromDt + "',dateformat)";
                }
                if (!(ToDt == null | string.IsNullOrEmpty(ToDt)))
                {
                    strCondition = strCondition + " And JCSET.JOBCARD_DATE <= TO_DATE('" + ToDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition = strCondition + " And TO_DATE(JCSET.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    strCondition = strCondition + " And CONS.CUSTOMER_NAME = '" + CustName + "'";
                }

                strCondition = strCondition + " AND JCSET.BUSINESS_TYPE = 2 ";
                strCondition = strCondition + " AND JCSET.PROCESS_TYPE = 2 ";

                sb.Append("SELECT JCSET.JOB_CARD_TRN_PK JOBPK,");
                sb.Append("       MAX(JCSET.JOBCARD_REF_NO) JOBREFNR,");
                sb.Append("       MAX(JCSET.JOBCARD_DATE) JOBDATE,");
                sb.Append("       MAX(SHP.CUSTOMER_NAME) SHIPPER,");
                sb.Append("       MAX(CONS.CUSTOMER_NAME) CONSIGNEE,");
                sb.Append("       MAX(POD.PORT_ID) POD,");
                sb.Append("       MAX(TO_CHAR(JCSET.ETD_DATE, DATETIMEFORMAT24)) ETD_DATE,");
                sb.Append("       SUM(NVL(JTSEF.FREIGHT_AMT, 0) * JTSEF.EXCHANGE_RATE) FRAMT,");
                sb.Append("       MAX(FRTPYR.CUSTOMER_MST_PK) CUST_PK,");
                sb.Append("       MAX(FRTPYR.CUSTOMER_NAME) CUST,");
                sb.Append("     '' SEL");
                sb.Append("        FROM JOB_CARD_TRN JCSET,");
                sb.Append("       JOB_TRN_FD   JTSEF,");
                sb.Append("       CUSTOMER_MST_TBL     SHP,");
                sb.Append("       CUSTOMER_MST_TBL     CONS,");
                sb.Append("       CUSTOMER_MST_TBL     FRTPYR,");
                sb.Append("       PORT_MST_TBL         POL,");
                sb.Append("       PORT_MST_TBL          POD");

                sb.Append("       WHERE(POD.PORT_MST_PK = JCSET.PORT_MST_POD_FK)");
                sb.Append("       AND POL.PORT_MST_PK = JCSET.PORT_MST_POL_FK");
                sb.Append("       AND SHP.CUSTOMER_MST_PK = JCSET.SHIPPER_CUST_MST_FK");
                sb.Append("       AND CONS.CUSTOMER_MST_PK(+) = JCSET.CONSIGNEE_CUST_MST_FK");
                sb.Append("       AND FRTPYR.CUSTOMER_MST_PK(+) = JTSEF.FRTPAYER_CUST_MST_FK");
                sb.Append("       AND JTSEF.CONSOL_INVOICE_TRN_FK IS NULL");
                sb.Append("       AND ABS((NVL(JTSEF.FREIGHT_AMT, 0) * JTSEF.EXCHANGE_RATE)) > 0");
                sb.Append("       AND JCSET.JOB_CARD_STATUS <> 2");
                sb.Append("       AND JTSEF.FREIGHT_TYPE = 2");
                sb.Append("       AND JTSEF.JOB_CARD_TRN_FK = JCSET.JOB_CARD_TRN_PK");
                sb.Append("       AND POD.LOCATION_MST_FK = ");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("       FROM LOCATION_MST_TBL L");
                sb.Append("       WHERE L.LOCATION_MST_PK =" + LocFk + ")");

                sb.Append(strCondition);
                sb.Append("       GROUP BY JCSET.JOB_CARD_TRN_PK");
                sb.Append("        UNION ALL");
                sb.Append("       SELECT JCSET.JOB_CARD_TRN_PK JOBPK,");
                sb.Append("       MAX(JCSET.JOBCARD_REF_NO) JOBREFNR,");
                sb.Append("       MAX(JCSET.JOBCARD_DATE) JOBDATE,");
                sb.Append("       MAX(SHP.CUSTOMER_NAME) SHIPPER,");
                sb.Append("       MAX(CONS.CUSTOMER_NAME) CONSIGNEE,");
                sb.Append("       MAX(POD.PORT_ID) POD,");
                sb.Append("       MAX(TO_CHAR(JCSET.ETD_DATE, DATETIMEFORMAT24)) ETD_DATE,");
                sb.Append("       SUM(NVL(JOTSEF.AMOUNT, 0) * JOTSEF.EXCHANGE_RATE) FRAMT,");
                sb.Append("       MAX(FRTPYR.CUSTOMER_MST_PK) CUST_PK,");
                sb.Append("       MAX(FRTPYR.CUSTOMER_NAME) CUST,");
                sb.Append("     '' SEL");
                sb.Append("       FROM JOB_CARD_TRN JCSET,");
                sb.Append("       JOB_TRN_OTH_CHRG   JOTSEF,");
                sb.Append("       CUSTOMER_MST_TBL     SHP,");
                sb.Append("       CUSTOMER_MST_TBL     CONS,");
                sb.Append("       CUSTOMER_MST_TBL     FRTPYR,");
                sb.Append("       PORT_MST_TBL         POL,");
                sb.Append("        PORT_MST_TBL         POD");

                sb.Append("       WHERE(POD.PORT_MST_PK = JCSET.PORT_MST_POD_FK)");
                sb.Append("       AND POL.PORT_MST_PK = JCSET.PORT_MST_POL_FK");
                sb.Append("       AND SHP.CUSTOMER_MST_PK = JCSET.SHIPPER_CUST_MST_FK");
                sb.Append("       AND CONS.CUSTOMER_MST_PK(+) = JCSET.CONSIGNEE_CUST_MST_FK");
                sb.Append("       AND FRTPYR.CUSTOMER_MST_PK(+) = JOTSEF.FRTPAYER_CUST_MST_FK");
                sb.Append("       AND JOTSEF.CONSOL_INVOICE_TRN_FK IS NULL");
                sb.Append("       AND ABS((NVL(JOTSEF.AMOUNT, 0) * JOTSEF.EXCHANGE_RATE)) > 0");
                sb.Append("       AND JCSET.JOB_CARD_STATUS <> 2");
                sb.Append("       AND JOTSEF.FREIGHT_TYPE = 2");
                sb.Append("       AND JOTSEF.JOB_CARD_TRN_FK = JCSET.JOB_CARD_TRN_PK");
                sb.Append("       AND POD.LOCATION_MST_FK = ");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("       FROM LOCATION_MST_TBL L");
                sb.Append("       WHERE L.LOCATION_MST_PK =" + LocFk + ")");
                sb.Append(strCondition);
                sb.Append("       GROUP BY JCSET.JOB_CARD_TRN_PK");
                sb.Append("       ORDER BY JOBDATE DESC, JOBREFNR DESC");
                strSQL = " SELECT * FROM (SELECT ROWNUM SR_NO, q.* FROM(";
                strSQL += sb.ToString();
                strSQL += " )q )";
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

        #endregion "Fetch Job Card for Sea-Import Report"

        #region "Fetch Job Card for Air-Import Report"

        /// <summary>
        /// Fetches the jc air import report.
        /// </summary>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public DataSet FetchJCAirImportReport(Int32 LocFk = 0, string FromDt = "", string ToDt = "", string CustName = "", string ETDDt = "", Int32 flag = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strCondition = null;
            string strSQL = null;

            try
            {
                if (!(FromDt == null | string.IsNullOrEmpty(FromDt)))
                {
                    strCondition = strCondition + " And JCSET.JOBCARD_DATE >= TO_DATE('" + FromDt + "',dateformat)";
                }
                if (!(ToDt == null | string.IsNullOrEmpty(ToDt)))
                {
                    strCondition = strCondition + " And JCSET.JOBCARD_DATE <= TO_DATE('" + ToDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition = strCondition + " And TO_DATE(JCSET.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    strCondition = strCondition + " And CONS.CUSTOMER_NAME = '" + CustName + "'";
                }
                strCondition = strCondition + " AND JCSET.BUSINESS_TYPE = 1 ";
                strCondition = strCondition + " AND JCSET.PROCESS_TYPE = 2 ";

                sb.Append("SELECT JCSET.JOB_CARD_TRN_PK JOBPK,");
                sb.Append("       MAX(JCSET.JOBCARD_REF_NO) JOBREFNR,");
                sb.Append("       MAX(JCSET.JOBCARD_DATE) JOBDATE,");
                sb.Append("       MAX(SHP.CUSTOMER_NAME) SHIPPER,");
                sb.Append("       MAX(CONS.CUSTOMER_NAME) CONSIGNEE,");
                sb.Append("       MAX(POD.PORT_ID) POD,");
                sb.Append("       MAX(TO_CHAR(JCSET.ETD_DATE, DATETIMEFORMAT24)) ETD_DATE,");
                sb.Append("       SUM(NVL(JTSEF.FREIGHT_AMT, 0) * JTSEF.EXCHANGE_RATE) FRAMT,");
                sb.Append("       MAX(FRTPYR.CUSTOMER_MST_PK) CUST_PK,");
                sb.Append("       MAX(FRTPYR.CUSTOMER_NAME) CUST,");
                sb.Append("     '' SEL");
                sb.Append("       FROM JOB_CARD_TRN JCSET,");
                sb.Append("       JOB_TRN_FD   JTSEF,");
                sb.Append("       CUSTOMER_MST_TBL     SHP,");
                sb.Append("       CUSTOMER_MST_TBL     CONS,");
                sb.Append("       CUSTOMER_MST_TBL     FRTPYR,");
                sb.Append("       PORT_MST_TBL         POL,");
                sb.Append("       PORT_MST_TBL          POD");

                sb.Append("       WHERE(POD.PORT_MST_PK = JCSET.PORT_MST_POD_FK)");
                sb.Append("       AND POL.PORT_MST_PK = JCSET.PORT_MST_POL_FK");
                sb.Append("       AND SHP.CUSTOMER_MST_PK = JCSET.SHIPPER_CUST_MST_FK");
                sb.Append("       AND CONS.CUSTOMER_MST_PK(+) = JCSET.CONSIGNEE_CUST_MST_FK");
                sb.Append("       AND FRTPYR.CUSTOMER_MST_PK(+) = JTSEF.FRTPAYER_CUST_MST_FK");
                sb.Append("       AND JTSEF.CONSOL_INVOICE_TRN_FK IS NULL");
                sb.Append("       AND ABS((NVL(JTSEF.FREIGHT_AMT, 0) * JTSEF.EXCHANGE_RATE)) > 0");
                sb.Append("       AND JCSET.JOB_CARD_STATUS <> 2");
                sb.Append("       AND JTSEF.FREIGHT_TYPE = 2");
                sb.Append("       AND JTSEF.JOB_CARD_TRN_FK = JCSET.JOB_CARD_TRN_PK");
                sb.Append("       AND POD.LOCATION_MST_FK = ");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("       FROM LOCATION_MST_TBL L");
                sb.Append("       WHERE L.LOCATION_MST_PK =" + LocFk + ")");
                sb.Append(strCondition);
                sb.Append("       GROUP BY JCSET.JOB_CARD_TRN_PK");
                sb.Append("        UNION ALL");
                sb.Append("       SELECT JCSET.JOB_CARD_TRN_PK JOBPK,");
                sb.Append("        MAX(JCSET.JOBCARD_REF_NO) JOBREFNR,");
                sb.Append("       MAX(JCSET.JOBCARD_DATE) JOBDATE,");
                sb.Append("       MAX(SHP.CUSTOMER_NAME) SHIPPER,");
                sb.Append("       MAX(CONS.CUSTOMER_NAME) CONSIGNEE,");
                sb.Append("       MAX(POD.PORT_ID) POD,");
                sb.Append("       MAX(TO_CHAR(JCSET.ETD_DATE, DATETIMEFORMAT24)) ETD_DATE,");
                sb.Append("       SUM(NVL(JOTSEF.AMOUNT, 0) * JOTSEF.EXCHANGE_RATE) FRAMT,");
                sb.Append("       MAX(FRTPYR.CUSTOMER_MST_PK) CUST_PK,");
                sb.Append("       MAX(FRTPYR.CUSTOMER_NAME) CUST,");
                sb.Append("     '' SEL");
                sb.Append("       FROM JOB_CARD_TRN JCSET,");
                sb.Append("       JOB_TRN_OTH_CHRG   JOTSEF,");
                sb.Append("       CUSTOMER_MST_TBL     SHP,");
                sb.Append("       CUSTOMER_MST_TBL     CONS,");
                sb.Append("       CUSTOMER_MST_TBL     FRTPYR,");
                sb.Append("       PORT_MST_TBL         POL,");
                sb.Append("       PORT_MST_TBL          POD");
                sb.Append("       WHERE(POD.PORT_MST_PK = JCSET.PORT_MST_POD_FK)");
                sb.Append("       AND POL.PORT_MST_PK = JCSET.PORT_MST_POL_FK");
                sb.Append("       AND SHP.CUSTOMER_MST_PK = JCSET.SHIPPER_CUST_MST_FK");
                sb.Append("       AND CONS.CUSTOMER_MST_PK(+) = JCSET.CONSIGNEE_CUST_MST_FK");
                sb.Append("       AND FRTPYR.CUSTOMER_MST_PK(+) = JOTSEF.FRTPAYER_CUST_MST_FK");
                sb.Append("       AND JOTSEF.CONSOL_INVOICE_TRN_FK IS NULL");
                sb.Append("       AND ABS((NVL(JOTSEF.AMOUNT, 0) * JOTSEF.EXCHANGE_RATE)) > 0");
                sb.Append("       AND JCSET.JOB_CARD_STATUS <> 2");
                sb.Append("       AND JOTSEF.FREIGHT_TYPE = 2");
                sb.Append("       AND JOTSEF.JOB_CARD_TRN_FK = JCSET.JOB_CARD_TRN_PK");
                sb.Append("       AND POD.LOCATION_MST_FK = ");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("       FROM LOCATION_MST_TBL L");
                sb.Append("       WHERE L.LOCATION_MST_PK =" + LocFk + ")");
                sb.Append(strCondition);
                sb.Append("       GROUP BY JCSET.JOB_CARD_TRN_PK");
                sb.Append("       ORDER BY JOBDATE DESC, JOBREFNR DESC");

                strSQL = " SELECT * FROM (SELECT ROWNUM SR_NO, q.* FROM(";
                strSQL += sb.ToString();
                strSQL += " )q )";
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

        #endregion "Fetch Job Card for Air-Import Report"
    }
}