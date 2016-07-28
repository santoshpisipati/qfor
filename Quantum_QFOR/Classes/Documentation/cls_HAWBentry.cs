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

using Oracle.ManagedDataAccess.Client;
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
    public class clsHAWBentry : CommonFeatures
    {
        /// <summary>
        /// The object wf
        /// </summary>
        private WorkFlow objWF = new WorkFlow();
        /// <summary>
        /// The object track n trace
        /// </summary>
        private cls_TrackAndTrace objTrackNTrace = new cls_TrackAndTrace();

        /// <summary>
        /// </summary>
        private string strSql;

        #region "Class Variables"

        /// <summary>
        /// The m_ ship data set
        /// </summary>
        private static DataSet M_ShipDataSet = new DataSet();
        /// <summary>
        /// The m_ freight terms dataset
        /// </summary>
        private static DataSet M_FreightTermsDataset = new DataSet();

        #endregion "Class Variables"

        /// <summary>
        /// The m_ move code dataset
        /// </summary>
        private static DataSet M_MoveCodeDataset = new DataSet();

        #region "Properties"

        /// <summary>
        /// Gets the ship data set.
        /// </summary>
        /// <value>
        /// The ship data set.
        /// </value>
        public static DataSet ShipDataSet
        {
            get { return M_ShipDataSet; }
        }

        /// <summary>
        /// Gets the freight data set.
        /// </summary>
        /// <value>
        /// The freight data set.
        /// </value>
        public static DataSet FreightDataSet
        {
            get { return M_FreightTermsDataset; }
        }

        /// <summary>
        /// Gets the move code data set.
        /// </summary>
        /// <value>
        /// The move code data set.
        /// </value>
        public static DataSet MoveCodeDataSet
        {
            get { return M_MoveCodeDataset; }
        }

        #endregion "Properties"

        #region "Constructor"

        /// <summary>
        /// Initializes a new instance of the <see cref="clsHAWBentry"/> class.
        /// </summary>
        public clsHAWBentry()
        {
            string strShipSQL = null;
            string strFreightSQL = null;
            string strMoveCodeSQL = null;
            strShipSQL = "SELECT 0 SHIPPING_TERMS_MST_PK, ' ' INCO_CODE  FROM  DUAL UNION ";
            strShipSQL += " SELECT SHIPPING_TERMS_MST_PK,INCO_CODE  FROM SHIPPING_TERMS_MST_TBL WHERE ACTIVE_FLAG = 1";
            strFreightSQL = "SELECT FREIGHT_TERMS_MST_PK,FRIEGHT_TERMS  FROM FREIGHT_TERMS_MST_TBL ";
            strMoveCodeSQL = "SELECT 0 CARGO_MOVE_PK,'' CARGO_MOVE_CODE FROM DUAL UNION ";
            strMoveCodeSQL += "SELECT CARGO_MOVE_PK,CARGO_MOVE_CODE FROM CARGO_MOVE_MST_TBL WHERE ACTIVE_FLAG = 1";
            try
            {
                M_ShipDataSet = (new WorkFlow()).GetDataSet(strShipSQL);
                M_FreightTermsDataset = (new WorkFlow()).GetDataSet(strFreightSQL);
                M_MoveCodeDataset = (new WorkFlow()).GetDataSet(strMoveCodeSQL);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Constructor"

        #region " Fetch On Click Event"

        #region " Fetch Airline Details"

        /// <summary>
        /// Fetches the airline details.
        /// </summary>
        /// <param name="JOBPk">The job pk.</param>
        /// <returns></returns>
        public DataSet FetchAirlineDetails(string JOBPk = "0")
        {
            StringBuilder strSqlBuilder = new StringBuilder();

            //
            strSqlBuilder.Append("SELECT ");
            strSqlBuilder.Append(" CUST.CUSTOMER_ID AS CUSTOMER,     ");
            //Customer
            strSqlBuilder.Append(" PLD.PLACE_NAME AS DELPLACE ,    ");
            //PLD
            strSqlBuilder.Append(" PLR.PLACE_NAME AS RECPLACE ,    ");
            //PLR
            strSqlBuilder.Append(" AO.PORT_NAME AS AOO ,    ");
            //AOO
            strSqlBuilder.Append(" AD.PORT_NAME AS AOD ,    ");
            //AOD

            strSqlBuilder.Append(" AO.PORT_MST_PK AS POL_PK ,    ");
            strSqlBuilder.Append(" AD.PORT_MST_PK AS POD_PK ,    ");
            strSqlBuilder.Append(" AIR.AIRLINE_ID,    ");
            //Airline ID
            strSqlBuilder.Append(" AIR.AIRLINE_NAME AS AIRLINE,    ");
            //Airline Name
            strSqlBuilder.Append(" JOB.VOYAGE_FLIGHT_NO AS FLIGHTNO,");
            //Flight Nr.
            strSqlBuilder.Append(" JOB.SEC_FLIGHT_NO AS SEC_FLIGHT_NO,    ");
            //Sec Flight Nr.            'ETA TP1
            strSqlBuilder.Append(" JOB.ETA_DATE  AS ETA_DATE,   ");
            //ETAAOD
            strSqlBuilder.Append(" JOB.ETD_DATE AS ETD_DATE,    ");
            //ETDAOO
            strSqlBuilder.Append(" JOB.ARRIVAL_DATE AS ARRIVAL_DATE,    ");
            //Arrival Date
            strSqlBuilder.Append(" JOB.DEPARTURE_DATE AS DEPARTURE_DATE,     ");
            //Departure Date
            strSqlBuilder.Append(" JOB.SEC_ETD_DATE AS SEC_ETD_DATE,    ");
            //ETD TP1
            strSqlBuilder.Append(" JOB.SEC_ETA_DATE AS SEC_ETA_DATE,    ");
            //ETA TP1
            strSqlBuilder.Append(" JOB.GOODS_DESCRIPTION AS GDESC,     ");
            //Goods Desc
            strSqlBuilder.Append(" JOB.MARKS_NUMBERS AS MNUMBERS,     ");
            //Marks Numbers
            //ADDED BY GOPI
            strSqlBuilder.Append(" SHT.inco_code, ");
            strSqlBuilder.Append(" mov.cargo_move_code, ");
            strSqlBuilder.Append(" JOB.PYMT_TYPE, ");
            strSqlBuilder.Append(" job.job_card_status, ");
            strSqlBuilder.Append("JOB.jobcard_ref_no AS JOBREF,");
            strSqlBuilder.Append("JOB.CB_AGENT_MST_FK,");
            strSqlBuilder.Append(" JOB.DP_AGENT_MST_FK,");
            strSqlBuilder.Append(" JOB.CL_AGENT_MST_FK,");
            strSqlBuilder.Append(" JOB.SHIPPER_CUST_MST_FK, ");

            strSqlBuilder.Append("       TEMP_CONS.CUSTOMER_MST_PK AS CONSIGNEEPK_TEMP,");
            strSqlBuilder.Append("   TMPNOTIFY1.CUSTOMER_MST_PK AS NOTIFY1PK_TEMP,");
            strSqlBuilder.Append("     TMPNOTIFY2.CUSTOMER_MST_PK AS NOTIFY2PK_TEMP,");
            strSqlBuilder.Append("    nvl( JOB.CONSIGNEE_CUST_MST_FK,TEMP_CONS.CUSTOMER_MST_PK)CONSIGNEE_CUST_MST_FK,");
            strSqlBuilder.Append("  CASE WHEN JOB.SAC_N1 = 0 then  nvl(  JOB.NOTIFY1_CUST_MST_FK,TMPNOTIFY1.CUSTOMER_MST_PK) else null end AS NOTIFY1_CUST_MST_FK,");

            strSqlBuilder.Append("  CASE WHEN JOB.SAC_N2 = 0 then nvl(   JOB.NOTIFY2_CUST_MST_FK,TMPNOTIFY2.CUSTOMER_MST_PK) else null end AS NOTIFY2_CUST_MST_FK,");

            //strSqlBuilder.Append(" JOB.CONSIGNEE_CUST_MST_FK, ")
            //strSqlBuilder.Append("JOB.NOTIFY1_CUST_MST_FK,")
            //strSqlBuilder.Append(" JOB.NOTIFY2_CUST_MST_FK, ")
            strSqlBuilder.Append("SH.customer_name AS SHIPPER,");
            strSqlBuilder.Append("  nvl( CO.customer_name,TEMP_CONS.CUSTOMER_NAME) AS CONSIGNEE, ");
            strSqlBuilder.Append("  CASE WHEN JOB.SAC_N1=0 then nvl( N1.customer_name,TMPNOTIFY1.CUSTOMER_NAME) ");
            strSqlBuilder.Append(" Else  'Same As Consignee' END  NOTIFY1, ");

            strSqlBuilder.Append("     CASE WHEN job.SAC_N2=0  THEN  nvl(N2.customer_name,TMPNOTIFY2.CUSTOMER_NAME) ");
            strSqlBuilder.Append("    Else  'Same As Consignee' END  NOTIFY2,");
            // strSqlBuilder.Append(" N1.customer_name AS NOTIFY1,")
            //  strSqlBuilder.Append("  N2.customer_name AS NOTIFY2, ")
            strSqlBuilder.Append("   CB.agent_name as CBAGENT,");
            strSqlBuilder.Append(" DP.agent_name AS DPAGENT,");
            strSqlBuilder.Append("  CL.agent_name AS CLAGENT, ");
            strSqlBuilder.Append("       JOB.LC_SHIPMENT, (SELECT LMT.LOCATION_NAME FROM LOCATION_MST_TBL LMT WHERE LMT.LOCATION_MST_PK = " + Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]) + ") PLACE_ISSUE ");
            strSqlBuilder.Append(" from JOB_CARD_TRN JOB, ");

            strSqlBuilder.Append("    TEMP_CUSTOMER_TBL TMPNOTIFY2,");
            strSqlBuilder.Append("  TEMP_CUSTOMER_TBL TMPNOTIFY1,");
            strSqlBuilder.Append("  TEMP_CUSTOMER_TBL TEMP_CONS,");

            strSqlBuilder.Append(" CUSTOMER_MST_TBL     SH,");
            strSqlBuilder.Append(" CUSTOMER_MST_TBL     CO,");
            strSqlBuilder.Append(" CUSTOMER_MST_TBL     N1,");
            strSqlBuilder.Append(" CUSTOMER_MST_TBL     N2,");
            strSqlBuilder.Append(" AGENT_MST_TBL        CB,");
            strSqlBuilder.Append(" AGENT_MST_TBL        DP,");
            strSqlBuilder.Append(" AGENT_MST_TBL  CL, ");
            strSqlBuilder.Append(" AIRLINE_MST_TBL AIR, ");
            strSqlBuilder.Append(" PORT_MST_TBL AO, ");
            strSqlBuilder.Append(" PORT_MST_TBL AD, ");
            strSqlBuilder.Append(" PLACE_MST_TBL PLR, ");
            strSqlBuilder.Append(" PLACE_MST_TBL PLD,");
            strSqlBuilder.Append(" CUSTOMER_MST_TBL CUST, ");
            strSqlBuilder.Append("  BOOKING_MST_TBL BOOK, ");
            strSqlBuilder.Append(" CARGO_MOVE_MST_TBL MOV,");
            strSqlBuilder.Append(" SHIPPING_TERMS_MST_TBL SHT");
            strSqlBuilder.Append(" WHERE ");
            strSqlBuilder.Append(" JOB.JOB_CARD_TRN_PK=" + JOBPk);
            strSqlBuilder.Append(" AND SH.CUSTOMER_MST_PK(+) = JOB.SHIPPER_CUST_MST_FK");
            strSqlBuilder.Append(" AND CO.CUSTOMER_MST_PK(+) = JOB.CONSIGNEE_CUST_MST_FK");
            strSqlBuilder.Append(" AND N1.CUSTOMER_MST_PK(+) = JOB.NOTIFY1_CUST_MST_FK");
            strSqlBuilder.Append(" AND N2.CUSTOMER_MST_PK(+) = JOB.NOTIFY2_CUST_MST_FK");
            strSqlBuilder.Append(" AND CB.AGENT_MST_PK(+) = JOB.CB_AGENT_MST_FK");
            strSqlBuilder.Append(" AND DP.AGENT_MST_PK(+) = JOB.DP_AGENT_MST_FK");
            strSqlBuilder.Append(" AND CL.AGENT_MST_PK(+) = JOB.CL_AGENT_MST_FK");
            strSqlBuilder.Append(" AND MOV.CARGO_MOVE_PK(+) = JOB.CARGO_MOVE_FK  ");
            strSqlBuilder.Append(" AND SHT.SHIPPING_TERMS_MST_PK(+)=JOB.SHIPPING_TERMS_MST_FK ");
            strSqlBuilder.Append(" AND cust.customer_mst_pk(+) =  Book.Cust_Customer_Mst_Fk ");
            strSqlBuilder.Append(" and book.port_mst_pod_fk(+) = ad.PORT_MST_PK ");
            strSqlBuilder.Append(" and book.port_mst_pol_fk = ao.port_mst_pk ");
            strSqlBuilder.Append(" and book.col_place_mst_fk = plr.place_pk(+) ");
            strSqlBuilder.Append(" and Book.del_place_mst_fk = pld.place_pk(+) ");
            strSqlBuilder.Append(" and book.CARRIER_MST_FK = air.airline_mst_pk(+)");
            strSqlBuilder.Append(" and job.BOOKING_MST_FK = book.BOOKING_MST_PK ");
            strSqlBuilder.Append("   AND JOB.CONSIGNEE_CUST_MST_FK = TEMP_CONS.CUSTOMER_MST_PK(+)");
            strSqlBuilder.Append(" AND JOB.NOTIFY1_CUST_MST_FK=TMPNOTIFY1.CUSTOMER_MST_PK(+)");
            strSqlBuilder.Append("  AND JOB.NOTIFY2_CUST_MST_FK=TMPNOTIFY2.CUSTOMER_MST_PK(+)");
            try
            {
                return (objWF.GetDataSet(strSqlBuilder.ToString()));
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

        /// <summary>
        /// Fetches the coll information.
        /// </summary>
        /// <param name="JOBPk">The job pk.</param>
        /// <param name="locpk">The locpk.</param>
        /// <returns></returns>
        public DataTable FetchCollInfo(string JOBPk, string locpk)
        {
            StringBuilder strSqlBuilder = new StringBuilder();

            strSqlBuilder.Append(" SELECT QRY.* ");
            strSqlBuilder.Append(" FROM (SELECT  T.* ");
            strSqlBuilder.Append(" FROM (SELECT INV.CONSOL_INVOICE_PK PK, ");
            strSqlBuilder.Append("             INV.INVOICE_REF_NO, ");
            strSqlBuilder.Append("             CMT.CUSTOMER_NAME, ");
            strSqlBuilder.Append("             INV.INVOICE_DATE, ");
            strSqlBuilder.Append("             INV.NET_RECEIVABLE, ");
            strSqlBuilder.Append("             NVL((select sum(ctrn.recd_amount_hdr_curr) ");
            strSqlBuilder.Append("                   from collections_trn_tbl ctrn ");
            strSqlBuilder.Append("                  where ctrn.invoice_ref_nr like inv.invoice_ref_no), ");
            strSqlBuilder.Append("                 0) Recieved, ");
            strSqlBuilder.Append("             NVL((INV.NET_RECEIVABLE            ");
            strSqlBuilder.Append("                       - NVL((select sum(ctrn.recd_amount_hdr_curr) ");
            strSqlBuilder.Append("                                     from collections_trn_tbl ctrn ");
            strSqlBuilder.Append("                                    where ctrn.invoice_ref_nr like ");
            strSqlBuilder.Append("                                          inv.invoice_ref_no), ");
            strSqlBuilder.Append("                                   0.00)), ");
            strSqlBuilder.Append("                 0) Balance, ");
            strSqlBuilder.Append("             CUMT.CURRENCY_ID, ");
            strSqlBuilder.Append("             INV.INV_UNIQUE_REF_NR ");
            strSqlBuilder.Append("        FROM CONSOL_INVOICE_TBL     INV, ");
            strSqlBuilder.Append("             CONSOL_INVOICE_TRN_TBL INVTRN, ");
            strSqlBuilder.Append("             JOB_CARD_TRN   JOB, ");
            strSqlBuilder.Append("             HAWB_EXP_TBL           HAWB, ");
            strSqlBuilder.Append("             CUSTOMER_MST_TBL       CMT, ");
            strSqlBuilder.Append("             CURRENCY_TYPE_MST_TBL  CUMT, ");
            strSqlBuilder.Append("             USER_MST_TBL           UMT ");
            strSqlBuilder.Append("       WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+) ");
            strSqlBuilder.Append("         AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+) ");
            strSqlBuilder.Append("         AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+) ");
            strSqlBuilder.Append("         AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+) ");
            strSqlBuilder.Append("         AND UMT.DEFAULT_LOCATION_FK =" + locpk + " ");
            strSqlBuilder.Append("         AND INV.CREATED_BY_FK = UMT.USER_MST_PK ");
            strSqlBuilder.Append("         AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK(+) ");
            strSqlBuilder.Append("         AND INV.PROCESS_TYPE = '1' ");
            strSqlBuilder.Append("         AND INV.BUSINESS_TYPE = '1' ");
            strSqlBuilder.Append("         AND JOB.JOB_CARD_TRN_PK = " + JOBPk + " ");
            strSqlBuilder.Append("       GROUP BY INV.CONSOL_INVOICE_PK, ");
            strSqlBuilder.Append("                INV.INVOICE_REF_NO, ");
            strSqlBuilder.Append("                INV.INVOICE_DATE, ");
            strSqlBuilder.Append("                CUMT.CURRENCY_ID, ");
            strSqlBuilder.Append("                CMT.CUSTOMER_NAME, ");
            strSqlBuilder.Append("                INV.NET_RECEIVABLE, ");
            strSqlBuilder.Append("                INV.CREATED_DT, ");
            strSqlBuilder.Append("                INV.INV_UNIQUE_REF_NR ");
            strSqlBuilder.Append("       ORDER BY INV.CREATED_DT DESC) T) QRY ");

            try
            {
                return (objWF.GetDataTable(strSqlBuilder.ToString()));
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

        /// <summary>
        /// Fetches the invoice information.
        /// </summary>
        /// <param name="JOBPk">The job pk.</param>
        /// <param name="locpk">The locpk.</param>
        /// <returns></returns>
        public DataTable FetchInvoiceInfo(string JOBPk, string locpk)
        {
            StringBuilder strSqlBuilder = new StringBuilder();

            strSqlBuilder.Append("                 select distinct jcf.freight_element_mst_fk, ");
            strSqlBuilder.Append("                     ft.freight_element_id, ");
            strSqlBuilder.Append("                     jc.jobcard_ref_no ");
            strSqlBuilder.Append("       from JOB_CARD_TRN    jc, ");
            strSqlBuilder.Append("            JOB_TRN_FD      jcf, ");
            strSqlBuilder.Append("            freight_element_mst_tbl ft ");
            strSqlBuilder.Append("       where jc.JOB_CARD_TRN_PK = jcf.JOB_CARD_TRN_FK ");
            strSqlBuilder.Append("        and ft.freight_element_mst_pk = jcf.freight_element_mst_fk ");
            strSqlBuilder.Append("        and jcf.freight_element_mst_fk not in ");
            strSqlBuilder.Append("            (select jfrt.freight_element_mst_fk ");
            strSqlBuilder.Append("               from JOB_CARD_TRN    jcexp, ");
            strSqlBuilder.Append("                    JOB_TRN_FD      jfrt, ");
            strSqlBuilder.Append("                    consol_invoice_tbl      cinv, ");
            strSqlBuilder.Append("                    consol_invoice_trn_tbl  cintrn, ");
            strSqlBuilder.Append("                    freight_element_mst_tbl frt ");
            strSqlBuilder.Append("              where jcexp.JOB_CARD_TRN_PK = jfrt.JOB_CARD_TRN_FK ");
            strSqlBuilder.Append("                and cinv.consol_invoice_pk = cintrn.consol_invoice_fk ");
            strSqlBuilder.Append("                and cintrn.job_card_fk = jcexp.JOB_CARD_TRN_PK ");
            strSqlBuilder.Append("                and cintrn.frt_oth_element_fk = jfrt.freight_element_mst_fk ");
            strSqlBuilder.Append("                and cinv.process_type = 1 ");
            strSqlBuilder.Append("                and cinv.business_type = 1 ");
            strSqlBuilder.Append("                and frt.freight_element_mst_pk = jfrt.freight_element_mst_fk ");
            strSqlBuilder.Append("                and jcexp.JOB_CARD_TRN_PK = jc.JOB_CARD_TRN_PK) ");
            strSqlBuilder.Append("        and jc.JOB_CARD_TRN_PK = " + JOBPk + " ");
            try
            {
                return (objWF.GetDataTable(strSqlBuilder.ToString()));
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

        /// <summary>
        /// Fetches the size of the pallete.
        /// </summary>
        /// <param name="jobPk">The job pk.</param>
        /// <returns></returns>
        public DataSet FetchPalleteSize(string jobPk = "0")
        {
            string strSqlpl = null;
            strSqlpl = "select JT.JOB_CARD_TRN_FK, ";
            strSqlpl += " JT.PALETTE_SIZE from JOB_TRN_CONT JT WHERE JT.JOB_CARD_TRN_FK=" + jobPk;
            try
            {
                return (objWF.GetDataSet(strSqlpl));
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

        /// <summary>
        /// Fetches the vol weight.
        /// </summary>
        /// <param name="jobPk">The job pk.</param>
        /// <returns></returns>
        public DataSet FetchVolWeight(string jobPk = "0")
        {
            string strSqlpl = null;
            strSqlpl = "select SUM(VOLUME_IN_CBM) as VOLUME,SUM(GROSS_WEIGHT) AS GWEIGHT";
            strSqlpl += ", SUM(CHARGEABLE_WEIGHT) AS CWEIGHT,SUM(PACK_COUNT) AS PACKCOUNT FROM JOB_TRN_CONT JT WHERE JT.JOB_CARD_TRN_FK=" + jobPk;
            try
            {
                return (objWF.GetDataSet(strSqlpl));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion " Fetch Airline Details"

        #endregion " Fetch On Click Event"

        #region " Fetch All On Edit"

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="HAWBPk">The hawb pk.</param>
        /// <param name="Status">The status.</param>
        /// <returns></returns>
        public DataSet FetchAll(string HAWBPk = "0", int Status = 0)
        {
            if (string.IsNullOrEmpty(HAWBPk))
            {
                HAWBPk = "0";
            }
            strSql = "SELECT ";
            strSql += " H.HAWB_EXP_TBL_PK,";
            strSql += " J.JOB_CARD_TRN_PK JOB_CARD_AIR_EXP_FK,";
            strSql += " J.JOBCARD_REF_NO,";
            strSql += " H.HAWB_REF_NO,";
            strSql += " H.HAWB_DATE,";
            strSql += " H.FLIGHT_NO,";
            strSql += " H.ETA_DATE,";
            strSql += " H.ETD_DATE,";
            strSql += " H.ARRIVAL_DATE,";
            strSql += " H.DEPARTURE_DATE,";
            strSql += " AIR.AIRLINE_ID, ";
            strSql += " AIR.AIRLINE_NAME, ";
            strSql += " j.SEC_FLIGHT_NO,";
            strSql += " j.SEC_ETA_DATE,";
            strSql += " j.SEC_ETD_DATE,";

            strSql += " H.SHIPPER_CUST_MST_FK,";
            strSql += " SH.customer_name AS SHIPPER,";

            strSql += "    nvl( H.CONSIGNEE_CUST_MST_FK,TEMP_CONS.CUSTOMER_MST_PK)as CONSIGNEE_CUST_MST_FK,";
            //  strSql &= " H.CONSIGNEE_CUST_MST_FK," & vbCrLf
            strSql += " (case when H.IS_TO_ORDER = '0' then nvl ( CO.customer_name,TEMP_CONS.CUSTOMER_NAME) else 'To Order' end) AS CONSIGNEE_NAME,";
            strSql += "  nvl ( h.consignee_name,TEMP_CONS.CUSTOMER_NAME) AS CONSIGNEE,";
            strSql += "   nvl  (H.consignee_name,TEMP_CONS.CUSTOMER_NAME) AS CONSIGNEE_TOORDER,";
            strSql += " H.IS_TO_ORDER, ";

            strSql += " nvl( H.NOTIFY1_CUST_MST_FK,TMPNOTIFY1.CUSTOMER_MST_PK) as NOTIFY1_CUST_MST_FK ,";

            // strSql &= " H.NOTIFY1_CUST_MST_FK," & vbCrLf
            strSql += "(case  when NVL( H.sac_n1,0) = '0' then    nvl(N1.customer_name,TMPNOTIFY1.CUSTOMER_NAME)  else 'Same As Consignee' end) as NOTIFY1,";
            strSql += " nvl( H.NOTIFY2_CUST_MST_FK, TMPNOTIFY2.CUSTOMER_MST_PK) as  NOTIFY2_CUST_MST_FK,";
            strSql += "       (case when NVL(H.sac_n2,0) = '0' then  nvl(N2.customer_name,TMPNOTIFY2.CUSTOMER_NAME) else 'Same As Consignee' end) as NOTIFY2,";
            //   strSql &= " N1.customer_name AS NOTIFY1," & vbCrLf
            //  strSql &= " H.NOTIFY2_CUST_MST_FK," & vbCrLf
            // strSql &= " N2.customer_name AS NOTIFY2," & vbCrLf
            strSql += " H.CB_AGENT_MST_FK,";
            strSql += " CB.agent_name as CBAGENT,";
            strSql += " H.DP_AGENT_MST_FK,";
            strSql += " DP.agent_name AS DPAGENT,";
            strSql += " H.CL_AGENT_MST_FK,";
            strSql += " CL.agent_name AS CLAGENT,";
            strSql += " POL.PORT_NAME AS POL,";
            strSql += " POD.PORT_NAME AS POD,";
            strSql += " POL.PORT_MST_PK AS POL_PK,";
            strSql += " POD.PORT_MST_PK AS POD_PK,";
            strSql += " COLP.PLACE_NAME AS RECP,";
            strSql += " DELP.PLACE_NAME AS DELP,";
            strSql += " H.GOODS_DESCRIPTION,";
            strSql += " SHT.inco_code,";
            strSql += " mov.cargo_move_code,";
            strSql += " J.PYMT_TYPE,";

            strSql += " H.TOTAL_PACK_COUNT, ";
            strSql += " H.DECL_VAL_FOR_CUSTOMS, ";
            strSql += " H.DEL_VAL_FOR_CARRIAGE, ";
            strSql += " H.SHIPPER_ADDRESS,";
            strSql += " H.CONSIGNEE_ADDRESS,";
            // strSql &= " (case when NVL(H.sac_n1,0) = '0' then  H.NOTIFY1_ADDRESS  else  'Same As Consignee' end) as NOTIFY1_ADDRESS," & vbCrLf

            // strSql &= "  (case when NVL(H.sac_n2,0) = '0' then H.NOTIFY2_ADDRESS else 'Same As Consignee' end) as NOTIFY2_ADDRESS," & vbCrLf
            strSql += " H.NOTIFY1_ADDRESS,";
            strSql += " H.NOTIFY2_ADDRESS,";
            strSql += " H.CB_AGENT_ADDRESS,";
            strSql += " H.CL_AGENT_ADDRESS,";
            strSql += " H.DP_AGENT_ADDRESS,";

            strSql += " H.MARKS_NUMBERS, ";
            strSql += " CUST.CUSTOMER_NAME, ";
            strSql += " H.HAWB_STATUS, ";
            strSql += " j.job_card_status, ";
            strSql += " H.LETTER_OF_CREDIT, ";
            strSql += " H.TOTAL_VOLUME, ";
            strSql += " H.TOTAL_CHARGE_WEIGHT,";
            strSql += " H.TOTAL_GROSS_WEIGHT,";
            strSql += " H.VERSION_NO,";
            strSql += "  H.LC_NUMBER,";
            strSql += "  H.LC_DATE,";
            strSql += "  H.LC_EXPIRES_ON, H.PLACE_ISSUE, ";
            strSql += " H.SURRENDER_DT ";

            strSql += " FROM HAWB_EXP_TBL H,";
            strSql += "   TEMP_CUSTOMER_TBL TMPNOTIFY2,";
            strSql += "   TEMP_CUSTOMER_TBL TMPNOTIFY1,";
            strSql += "  TEMP_CUSTOMER_TBL TEMP_CONS,";
            strSql += " JOB_CARD_TRN J,";
            strSql += " BOOKING_MST_TBL B,";
            strSql += " CUSTOMER_MST_TBL SH,";
            strSql += " CUSTOMER_MST_TBL CO,";
            strSql += " CUSTOMER_MST_TBL N1,";
            strSql += " CUSTOMER_MST_TBL N2,";
            strSql += " AGENT_MST_TBL CB,";
            strSql += " AGENT_MST_TBL DP,";
            strSql += " AGENT_MST_TBL CL,";
            strSql += " PORT_MST_TBL POL,";
            strSql += " PORT_MST_TBL POD,";
            strSql += " AIRLINE_MST_TBL AIR,";
            strSql += " CUSTOMER_MST_TBL CUST,";
            strSql += " PLACE_MST_TBL COLP,";
            strSql += "PLACE_MST_TBL DELP,";
            strSql += "CARGO_MOVE_MST_TBL MOV,";
            strSql += "SHIPPING_TERMS_MST_TBL SHT";

            strSql += " WHERE";
            strSql += " J.BOOKING_MST_FK = B.BOOKING_MST_PK " + " AND b.CARRIER_MST_FK = air.airline_mst_pk(+)  ";
            if (Status == 3)
            {
                strSql += " AND ((J.JOB_CARD_TRN_PK = H.JOB_CARD_AIR_EXP_FK)";
                strSql += "   OR (J.JOB_CARD_TRN_PK = H.NEW_JOB_CARD_AIR_EXP_FK))";
            }
            else
            {
                strSql += " AND J.JOB_CARD_TRN_PK=H.JOB_CARD_AIR_EXP_FK ";
            }
            strSql += "AND SH.CUSTOMER_MST_PK(+)=H.SHIPPER_CUST_MST_FK";
            strSql += "AND CO.CUSTOMER_MST_PK(+)=H.CONSIGNEE_CUST_MST_FK ";
            strSql += "AND N1.CUSTOMER_MST_PK(+)=H.NOTIFY1_CUST_MST_FK";
            strSql += "AND N2.CUSTOMER_MST_PK(+)=H.NOTIFY2_CUST_MST_FK";
            strSql += "AND CB.AGENT_MST_PK(+)=H.CB_AGENT_MST_FK";
            strSql += "AND DP.AGENT_MST_PK(+)=H.DP_AGENT_MST_FK";
            strSql += "AND CL.AGENT_MST_PK(+)=H.CL_AGENT_MST_FK";
            strSql += "AND POL.PORT_MST_PK(+)=B.PORT_MST_POL_FK";
            strSql += "AND POD.PORT_MST_PK(+)=B.PORT_MST_POD_FK";

            strSql += "AND MOV.CARGO_MOVE_PK(+) = j.CARGO_MOVE_FK";
            strSql += "AND SHT.SHIPPING_TERMS_MST_PK(+)=j.SHIPPING_TERMS_MST_FK";

            strSql += "AND COLP.PLACE_PK(+)=B.COL_PLACE_MST_FK";
            strSql += "AND DELP.PLACE_PK(+)=B.DEL_PLACE_MST_FK";
            strSql += "AND CUST.CUSTOMER_MST_PK(+)=B.CUST_CUSTOMER_MST_FK";
            strSql += "AND H.HAWB_EXP_TBL_PK= " + HAWBPk;
            strSql += "      AND J.CONSIGNEE_CUST_MST_FK = TEMP_CONS.CUSTOMER_MST_PK(+)";
            strSql += "  AND J.NOTIFY1_CUST_MST_FK=TMPNOTIFY1.CUSTOMER_MST_PK(+)";
            strSql += "  AND J.NOTIFY2_CUST_MST_FK=TMPNOTIFY2.CUSTOMER_MST_PK(+)";
            try
            {
                return (objWF.GetDataSet(strSql));
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

        #endregion " Fetch All On Edit"

        #region "Fetch Temp Customer"

        /// <summary>
        /// Fetches the temporary customer.
        /// </summary>
        /// <param name="jobNr">The job nr.</param>
        /// <param name="JobcardPk">The jobcard pk.</param>
        /// <returns></returns>
        public DataSet fetchTempCust(string jobNr = "", string JobcardPk = "0")
        {
            string strSqlpl = null;
            strSqlpl = "select t.cust_customer_mst_fk from BOOKING_MST_TBL t ";
            strSqlpl += " where t.BOOKING_MST_PK =";
            strSqlpl += " (select j.BOOKING_MST_FK from JOB_CARD_TRN j where j.jobcard_ref_no = '" + jobNr + "'";
            strSqlpl += " and j.JOB_CARD_TRN_PK = " + JobcardPk + ")";

            try
            {
                return (objWF.GetDataSet(strSqlpl));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Temp Customer"

        #region " Save"

        /// <summary>
        /// Genrates the hawb no.
        /// </summary>
        /// <param name="Location">The location.</param>
        /// <param name="emp">The emp.</param>
        /// <param name="obj">The object.</param>
        /// <param name="SID">The sid.</param>
        /// <param name="POLID">The polid.</param>
        /// <param name="PODID">The podid.</param>
        /// <returns></returns>
        public string genrateHAWBNo(long Location, long emp, object obj, string SID = "", string POLID = "", string PODID = "")
        {
            return GenerateProtocolKey("HAWB EXPORTS", Location, emp, DateTime.Today, "", "", POLID, LAST_MODIFIED_BY, new WorkFlow(), SID,
            PODID);
        }

        #region "To Fetch JobcardHBLreleased"

        /// <summary>
        /// Fetches the hawb released.
        /// </summary>
        /// <param name="JobPk">The job pk.</param>
        /// <returns></returns>
        public object FetchHAWBReleased(string JobPk)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                sb.Append("select DISTINCT J.JOB_CARD_TRN_PK,J.JOBCARD_REF_NO ");
                sb.Append(" from HAWB_EXP_TBL HAW,JOB_CARD_TRN J ");
                sb.Append("where HAW.JOB_CARD_AIR_EXP_FK = J.JOB_CARD_TRN_PK ");
                sb.Append(" AND HAW.JOB_CARD_AIR_EXP_FK='" + JobPk + "'");
                sb.Append(" AND HAW.HAWB_STATUS= 1");
                return objWF.GetDataSet(sb.ToString());
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

        /// <summary>
        /// Updates the hawb status.
        /// </summary>
        /// <param name="JobPk">The job pk.</param>
        /// <returns></returns>
        public ArrayList UpdateHAWBStatus(string JobPk)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            OracleCommand updCmdUser = new OracleCommand();
            string str = null;
            Int16 intIns = default(Int16);
            try
            {
                updCmdUser.Transaction = TRAN;

                str = "UPDATE JOB_CARD_TRN  j SET ";
                str += "   j.HBL_RELEASED_STATUS = 1";
                str += " WHERE j.JOB_CARD_TRN_PK=" + JobPk;

                var _with1 = updCmdUser;
                _with1.Connection = objWK.MyConnection;
                _with1.Transaction = TRAN;
                _with1.CommandType = CommandType.Text;
                _with1.CommandText = str;
                intIns = Convert.ToInt16(_with1.ExecuteNonQuery());
                if (intIns > 0)
                {
                    TRAN.Commit();
                    arrMessage.Add("Protocol Generated Succesfully");
                    return arrMessage;
                }
            }
            catch (OracleException OraEx)
            {
                TRAN.Rollback();
                arrMessage.Add(OraEx.Message);
                return arrMessage;
            }
            finally
            {
                objWK.CloseConnection();
            }
            return new ArrayList();
        }

        /// <summary>
        /// Checkstatuses the specified job pk.
        /// </summary>
        /// <param name="JobPk">The job pk.</param>
        /// <param name="dsAgtCollection">The ds agt collection.</param>
        /// <returns></returns>
        public object checkstatus(string JobPk, DataSet dsAgtCollection)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            Int32 i = default(Int32);
            bool IsDPAgent = false;
            bool IsCBAgent = false;
            try
            {
                if (dsAgtCollection.Tables.Count > 0)
                {
                    if (dsAgtCollection.Tables[0].Rows.Count > 0)
                    {
                        for (i = 0; i <= dsAgtCollection.Tables[0].Rows.Count - 1; i++)
                        {
                            if (!string.IsNullOrEmpty(dsAgtCollection.Tables[0].Rows[i]["AGENTTYPE"].ToString()))
                            {
                                if (Convert.ToInt32(dsAgtCollection.Tables[0].Rows[i]["AGENTTYPE"]) == 2)
                                {
                                    IsDPAgent = true;
                                }
                                else if (Convert.ToInt32(dsAgtCollection.Tables[0].Rows[i]["AGENTTYPE"]) == 1)
                                {
                                    IsCBAgent = true;
                                }
                            }
                        }
                    }
                }

                if (IsDPAgent == true & IsCBAgent == true)
                {
                    sb.Append("select * FROM  JOB_CARD_TRN JOB");
                    sb.Append(" where JOB.JOB_CARD_TRN_PK='" + JobPk + "'");
                    sb.Append(" AND JOB.COLLECTION_STATUS=1");
                    sb.Append(" AND JOB.PAYEMENT_STATUS=1");
                    sb.Append(" AND JOB.HBL_RELEASED_STATUS=1");
                    sb.Append("  AND JOB.DPAGENT_STATUS=1");
                    sb.Append("  AND JOB.CBAGENT_STATUS=1");
                }
                else if (IsDPAgent == true)
                {
                    sb.Append("select * FROM  JOB_CARD_TRN JOB");
                    sb.Append(" where JOB.JOB_CARD_TRN_PK='" + JobPk + "'");
                    sb.Append(" AND JOB.COLLECTION_STATUS=1");
                    sb.Append(" AND JOB.PAYEMENT_STATUS=1");
                    sb.Append(" AND JOB.HBL_RELEASED_STATUS=1");
                    sb.Append("  AND JOB.DPAGENT_STATUS=1");
                }
                else if (IsCBAgent == true)
                {
                    sb.Append("select * FROM  JOB_CARD_TRN JOB");
                    sb.Append(" where JOB.JOB_CARD_TRN_PK='" + JobPk + "'");
                    sb.Append(" AND JOB.COLLECTION_STATUS=1");
                    sb.Append(" AND JOB.PAYEMENT_STATUS=1");
                    sb.Append(" AND JOB.HBL_RELEASED_STATUS=1");
                    sb.Append("  AND JOB.CBAGENT_STATUS=1");
                }
                else
                {
                    sb.Append("select * FROM  JOB_CARD_TRN JOB");
                    sb.Append(" where JOB.JOB_CARD_TRN_PK='" + JobPk + "'");
                    sb.Append(" AND JOB.COLLECTION_STATUS=1");
                    sb.Append(" AND JOB.PAYEMENT_STATUS=1");
                    sb.Append(" AND JOB.HBL_RELEASED_STATUS=1");
                }
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Updatejobcarddates the specified job pk.
        /// </summary>
        /// <param name="JobPk">The job pk.</param>
        /// <returns></returns>
        public ArrayList updatejobcarddate(string JobPk)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            OracleCommand updCmdUser = new OracleCommand();
            string str = null;
            Int16 intIns = default(Int16);
            try
            {
                updCmdUser.Transaction = TRAN;

                str = "UPDATE JOB_CARD_TRN  j SET ";
                str += "   j.JOB_CARD_STATUS = 2, j.JOB_CARD_CLOSED_ON = SYSDATE";
                str += " WHERE j.JOB_CARD_TRN_PK=" + JobPk;
                var _with2 = updCmdUser;
                _with2.Connection = objWK.MyConnection;
                _with2.Transaction = TRAN;
                _with2.CommandType = CommandType.Text;
                _with2.CommandText = str;
                intIns = Convert.ToInt16(_with2.ExecuteNonQuery());
                if (intIns > 0)
                {
                    TRAN.Commit();
                    arrMessage.Add("Protocol Generated Succesfully");
                    return arrMessage;
                }
            }
            catch (OracleException OraEx)
            {
                TRAN.Rollback();
                arrMessage.Add(OraEx.Message);
                return arrMessage;
            }
            finally
            {
                objWK.CloseConnection();
            }
            return new ArrayList();
        }

        #endregion "To Fetch JobcardHBLreleased"

        #endregion " Save"

        #region " Enhance Search for Hawb Job Ref No"

        /// <summary>
        /// Fetches for hawb job reference.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchForHAWBJobRef(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strReq = null;
            string strLOCATION_IN = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strLOCATION_IN = Convert.ToString(arr.GetValue(2));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_JOB_HAWB_REF_NO_PKG.GET_JOB_HAWB_REF_COMMON";
                var _with6 = SCM.Parameters;
                _with6.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with6.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with6.Add("LOCATION_IN", (!string.IsNullOrEmpty(strLOCATION_IN) ? strLOCATION_IN : "")).Direction = ParameterDirection.Input;
                _with6.Add("RETURN_VALUE", OracleDbType.NVarchar2, 4000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
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

        /// <summary>
        /// Ifs the database null.
        /// </summary>
        /// <param name="col">The col.</param>
        /// <returns></returns>
        private object ifDBNull(object col)
        {
            if (Convert.ToString(col).Length == 0)
            {
                return "";
            }
            else
            {
                return col;
            }
        }

        /// <summary>
        /// Ifs the database zero.
        /// </summary>
        /// <param name="col">The col.</param>
        /// <returns></returns>
        private object ifDBZero(object col)
        {
            if (Convert.ToString(col).Length == 0)
            {
                return 0;
            }
            else
            {
                return col;
            }
        }

        /// <summary>
        /// Ifs the date null.
        /// </summary>
        /// <param name="col">The col.</param>
        /// <returns></returns>
        private object ifDateNull(object col)
        {
            if (Convert.ToString(col).Length == 0)
            {
                return "";
            }
            else
            {
                return Convert.ToDateTime(col);
            }
        }

        #endregion " Enhance Search for Hawb Job Ref No"

        #region " Function to Find Hawb Pk"

        /// <summary>
        /// Finds the hawb pk.
        /// </summary>
        /// <param name="RefNo">The reference no.</param>
        /// <returns></returns>
        public int FindHawbPk(string RefNo)
        {
            try
            {
                string strSQL = null;
                WorkFlow objWF = new WorkFlow();
                strSQL = "select h.hawb_exp_tbl_pk from hawb_exp_tbl h where h.hawb_ref_no= '" + RefNo + "' ";
                string HBLPK = null;
                HBLPK = objWF.ExecuteScaler(strSQL);
                return Convert.ToInt32(HBLPK);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Finds the hawb version no.
        /// </summary>
        /// <param name="RefNo">The reference no.</param>
        /// <returns></returns>
        public int FindHawbVersionNo(string RefNo)
        {
            try
            {
                string strSQL = null;
                WorkFlow objWF = new WorkFlow();
                strSQL = "select max(h.version_no) from hawb_exp_tbl h where h.hawb_ref_no = '" + RefNo + "' ";
                string HBLPK = null;
                HBLPK = objWF.ExecuteScaler(strSQL);
                return Convert.ToInt32(HBLPK);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion " Function to Find Hawb Pk"

        #region " Hawb Printing"

        /// <summary>
        /// Fetches the hawb main.
        /// </summary>
        /// <param name="HAWBPk">The hawb pk.</param>
        /// <returns></returns>
        public DataSet FetchHAWBMain(Int32 HAWBPk)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();

            Strsql = " SELECT H.HAWB_EXP_TBL_PK AS HAWBPK,H.HAWB_REF_NO AS HAWBREF,M.MAWB_EXP_TBL_PK AS MAWBPK,M.MAWB_REF_NO AS MAWBREF,";
            Strsql += " JA.JOB_CARD_TRN_PK AS JOBPK,JA.JOBCARD_REF_NO AS JOBREF,CTMST.CUSTOMS_STATUS_CODE CUST_CODE,SHP.CUSTOMER_NAME AS SHIPPER,";
            Strsql += " SHPDET.ADM_ADDRESS_1 AS SHPADD1,SHPDET.ADM_ADDRESS_2 AS SHPADD2,SHPDET.ADM_ADDRESS_2 AS SHPADD3,SHPDET.ADM_ZIP_CODE AS SHPZIP,";
            Strsql += " CON.CUSTOMER_NAME AS CONSIGNEE,CONDET.ADM_ADDRESS_1 AS CONADD1,CONDET.ADM_ADDRESS_2 AS CONADD2,";
            Strsql += " CONDET.ADM_ADDRESS_3 AS CONADD3,CONDET.ADM_ZIP_CODE AS CONZIP,COP.CORPORATE_NAME AS CORPORATE,";
            Strsql += " COP.ADDRESS_LINE1 AS CORPADD1,COP.ADDRESS_LINE2 AS CORPADD2,COP.ADDRESS_LINE3 AS COPADD3,COP.CITY AS CORPCITY,";
            Strsql += " COU.COUNTRY_NAME AS CORPCOUNTRY,COP.PHONE AS COPTEL,COP.FAX AS COPFAX,COP.EMAIL AS COPEMAIL,COP.FMC_NO AS COPFMC,";
            Strsql += " CY.CURRENCY_ID AS BASECURID,CY.CURRENCY_NAME AS BASECUR,H.MARKS_NUMBERS AS MARKS,H.GOODS_DESCRIPTION AS GOODS,";
            Strsql += " PL.PORT_ID AS POLID,PL.PORT_NAME AS POL,PD.PORT_ID AS PODID,PD.PORT_NAME AS POD,AM.AIRLINE_ID,AM.AIRLINE_NAME,";
            Strsql += " NVL(H.DECL_VAL_FOR_CUSTOMS,0) AS DECLVALUE,H.FLIGHT_NO AS FLIGHTNO,H.ETA_DATE AS ETA,H.ETD_DATE AS ETD,";
            Strsql += " H.ARRIVAL_DATE AS ARRDATE,H.DEPARTURE_DATE AS DEPDATE,JA.INSURANCE_AMT AS AMTINSURANCE,";
            Strsql += " H.TOTAL_PACK_COUNT AS NOOFPACKAGES,H.TOTAL_GROSS_WEIGHT AS TOTGRWT,H.TOTAL_CHARGE_WEIGHT AS TOTCHRGEWT,";
            Strsql += " (SELECT SUM(JF.FREIGHT_AMT*JF.EXCHANGE_RATE) FROM JOB_TRN_FD JF WHERE ";
            Strsql += "   JF.JOB_CARD_TRN_FK=JA.JOB_CARD_TRN_PK) AS TOTFAMT";
            Strsql += " FROM JOB_CARD_TRN JA,customs_status_mst_tbl CTMST,BOOKING_MST_TBL BA,HAWB_EXP_TBL H,MAWB_EXP_TBL M,CUSTOMER_MST_TBL SHP,";
            Strsql += " CUSTOMER_CONTACT_DTLS SHPDET,CUSTOMER_MST_TBL CON,CUSTOMER_CONTACT_DTLS CONDET,CORPORATE_MST_TBL COP,";
            Strsql += " COUNTRY_MST_TBL COU,CURRENCY_TYPE_MST_TBL CY,PORT_MST_TBL PL,PORT_MST_TBL PD,AIRLINE_MST_TBL AM";
            Strsql += " WHERE H.HAWB_EXP_TBL_PK=" + HAWBPk;
            Strsql += " AND CTMST.CUSTOMS_CODE_MST_PK(+)=BA.CUSTOMS_CODE_MST_FK";
            Strsql += " AND H.JOB_CARD_AIR_EXP_FK=JA.JOB_CARD_TRN_PK";
            Strsql += " AND H.MAWB_EXP_TBL_FK=M.MAWB_EXP_TBL_PK(+)";
            Strsql += " AND JA.BOOKING_MST_FK=BA.BOOKING_MST_PK";
            Strsql += " AND SHP.CUSTOMER_MST_PK(+)=H.SHIPPER_CUST_MST_FK";
            Strsql += " AND SHP.CUSTOMER_MST_PK=SHPDET.CUSTOMER_MST_FK(+)";
            Strsql += " AND CON.CUSTOMER_MST_PK(+)=H.CONSIGNEE_CUST_MST_FK";
            Strsql += " AND CON.CUSTOMER_MST_PK=CONDET.CUSTOMER_MST_FK(+)";
            Strsql += " AND COP.COUNTRY_MST_FK=COU.COUNTRY_MST_PK";
            Strsql += " AND CY.CURRENCY_MST_PK=COP.CURRENCY_MST_FK";
            Strsql += " AND PL.PORT_MST_PK=BA.PORT_MST_POL_FK";
            Strsql += " AND PD.PORT_MST_PK=BA.PORT_MST_POD_FK";
            Strsql += " AND BA.CARRIER_MST_FK=AM.AIRLINE_MST_PK(+)";

            try
            {
                return Objwk.GetDataSet(Strsql);
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

        /// <summary>
        /// Fetches the hawb freight.
        /// </summary>
        /// <param name="HAWBPk">The hawb pk.</param>
        /// <returns></returns>
        public DataSet FetchHAWBFreight(Int32 HAWBPk)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();

            Strsql = " SELECT HH.HAWB_EXP_TBL_PK AS HAWBPK,JFD.JOB_CARD_TRN_FK AS JOBPK,JFD.FREIGHT_TYPE,JFD.FREIGHT_AMT";
            Strsql += " FROM HAWB_EXP_TBL HH,JOB_TRN_FD JFD";
            Strsql += " WHERE HH.JOB_CARD_AIR_EXP_FK=JFD.JOB_CARD_TRN_FK";
            Strsql += " AND HH.HAWB_EXP_TBL_PK=" + HAWBPk;

            try
            {
                return Objwk.GetDataSet(Strsql);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the transhipment details.
        /// </summary>
        /// <param name="HAWBPk">The hawb pk.</param>
        /// <returns></returns>
        public DataSet FetchTranshipmentDetails(Int32 HAWBPk)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();

            Strsql = " SELECT H.HAWB_EXP_TBL_PK AS HAWBPK,JT.JOB_CARD_AIR_EXP_FK AS JOBPK,PMT.PORT_ID,PMT.PORT_NAME,";
            Strsql += " AA.AIRLINE_ID,AA.AIRLINE_NAME";
            Strsql += " FROM HAWB_EXP_TBL H, JOB_TRN_AIR_EXP_TP JT,AIRLINE_MST_TBL AA,PORT_MST_TBL PMT";
            Strsql += " WHERE H.HAWB_EXP_TBL_PK=" + HAWBPk;
            Strsql += " AND H.JOB_CARD_AIR_EXP_FK=JT.JOB_CARD_AIR_EXP_FK";
            Strsql += " AND JT.AIRLINE_MST_FK=AA.AIRLINE_MST_PK";
            Strsql += " AND JT.PORT_MST_FK=PMT.PORT_MST_PK";

            try
            {
                return Objwk.GetDataSet(Strsql);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //Akhilesh ... Start [18-May-06] [EFS]
        //Reason: HAWB Printing
        /// <summary>
        /// Haws the b_ print.
        /// </summary>
        /// <param name="HAWB_PK">The haw b_ pk.</param>
        /// <param name="Loged_In_Loc">The loged_ in_ loc.</param>
        /// <param name="HAWBRate">The hawb rate.</param>
        /// <returns></returns>
        public DataSet HAWB_Print(string HAWB_PK, long Loged_In_Loc, string HAWBRate = "0")
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with7 = objWK.MyCommand.Parameters;
                _with7.Add("HAWB_PK_IN", HAWB_PK).Direction = ParameterDirection.Input;
                _with7.Add("LOGGED_IN_LOC_FK", Loged_In_Loc).Direction = ParameterDirection.Input;
                //.Add("HAWBRATE_IN", HAWBRate).Direction = ParameterDirection.Input
                _with7.Add("HAWB_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                if (HAWBRate == "Yes")
                {
                    return objWK.GetDataSet("HAWB_MAWB_PRINT_PKG", "HAWB_PRINT");
                }
                else
                {
                    return objWK.GetDataSet("HAWB_MAWB_PRINT_PKG", "HAWB_PRINT_NOT_RATED");
                }
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Haws the b_ FRT.
        /// </summary>
        /// <param name="HAWB_PK">The haw b_ pk.</param>
        /// <param name="HAWBRate">The hawb rate.</param>
        /// <returns></returns>
        public DataSet HAWB_FRT(string HAWB_PK, string HAWBRate = "0")
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with8 = objWK.MyCommand.Parameters;
                _with8.Add("HAWB_PK_IN", HAWB_PK).Direction = ParameterDirection.Input;
                _with8.Add("HAWB_ITEM", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                if (HAWBRate == "Yes")
                {
                    return objWK.GetDataSet("HAWB_MAWB_PRINT_PKG", "HAWB_ITEMWISE_DTLS");
                }
                else
                {
                    return objWK.GetDataSet("HAWB_MAWB_PRINT_PKG", "HAWB_ITEMWISE_DTLS_NONRATED");
                }
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Job_card_s the remarks.
        /// </summary>
        /// <param name="strJobCardRefNo">The string job card reference no.</param>
        /// <returns></returns>
        public string Job_card_Remarks(string strJobCardRefNo)
        {
            string strSQL = null;
            WorkFlow objWK = new WorkFlow();
            OracleDataReader oracleReader = null;
            try
            {
                strSQL = "SELECT UPPER(T.REMARKS) FROM JOB_CARD_TRN T WHERE T.JOBCARD_REF_NO ='" + strJobCardRefNo + "'";
                objWK.MyCommand.CommandType = CommandType.Text;
                objWK.MyCommand.CommandText = strSQL;

                oracleReader = objWK.GetDataReader(strSQL);
                strSQL = "";

                if (oracleReader.Read())
                {
                    strSQL = oracleReader[0].ToString();
                }

                return strSQL;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                oracleReader.Close();
                oracleReader.Dispose();
            }
        }

        //Akhilesh ... End

        #endregion " Hawb Printing"

        #region "Check for MJC"

        /// <summary>
        /// </summary>
        /// <param name="JcPk">The jc pk.</param>
        /// <returns></returns>
        public bool ChkMJC(string JcPk)
        {
            string str = null;
            string ret = null;
            str = "select j.MASTER_JC_FK from JOB_CARD_TRN j where j.JOB_CARD_TRN_PK=" + JcPk;

            try
            {
                ret = objWF.ExecuteScaler(str);

                if (ret.Length > 0)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Check for MJC"

        #region " Hbl Report "

        //#Region "Fetch HBL Report Data"
        //        'Public Function FetchHAWBHeaderDocumentData(ByVal HBLPK As Integer) As DataSet
        //        '    Dim objWF As New WorkFlow
        //        '    Dim strSQL As String
        //        '    strSQL = "select JAE.JOB_CARD_TRN_PK JOBPK,"
        //        '    strSQL &= vbCrLf & "HAWB.HAWB_EXP_TBL_PK HBPK,"
        //        '    strSQL &= vbCrLf & "JAE.JOBCARD_REF_NO JOBNO,"
        //        '    strSQL &= vbCrLf & "JAE.UCR_NO UCRNO,"
        //        '    strSQL &= vbCrLf & "HAWB.HAWB_REF_NO HBNO,"
        //        '    strSQL &= vbCrLf & "HAWB.FLIGHT_NO VES_FLIGHT,"
        //        '    strSQL &= vbCrLf & "'' voyage,"
        //        '    strSQL &= vbCrLf & "POL.PORT_NAME POL,"
        //        '    strSQL &= vbCrLf & "POD.PORT_NAME POD,"
        //        '    strSQL &= vbCrLf & "PMT.PLACE_NAME PLD,"
        //        '    strSQL &= vbCrLf & "CustMstShipper.Customer_Name Shipper,"
        //        '    strSQL &= vbCrLf & "CustShipperDtls.Adm_Address_1 ShiAddress1,"
        //        '    strSQL &= vbCrLf & "CustShipperDtls.Adm_Address_2 ShiAddress2,"
        //        '    strSQL &= vbCrLf & "CustShipperDtls.Adm_Address_3 ShiAddress3,"
        //        '    strSQL &= vbCrLf & "CustShipperDtls.Adm_City ShiCity,"
        //        '    strSQL &= vbCrLf & "CustMstConsignee.Customer_Name Consignee,"
        //        '    strSQL &= vbCrLf & "CustConsigneeDtls.Adm_Address_1 ConsiAddress1,"
        //        '    strSQL &= vbCrLf & "CustConsigneeDtls.Adm_Address_2 ConsiAddress2,"
        //        '    strSQL &= vbCrLf & "CustConsigneeDtls.Adm_Address_3 ConsiAddress3,"
        //        '    strSQL &= vbCrLf & "CustConsigneeDtls.Adm_City ConsiCity,"
        //        '    strSQL &= vbCrLf & "AgentMst.Agent_Name,"
        //        '    strSQL &= vbCrLf & "AgentDtls.Adm_Address_1 AgtAddress1,"
        //        '    strSQL &= vbCrLf & "AgentDtls.Adm_Address_2 AgtAddress2,"
        //        '    strSQL &= vbCrLf & "AgentDtls.Adm_Address_3 AgtAddress3,"
        //        '    strSQL &= vbCrLf & "AgentDtls.Adm_City AgtCity,"
        //        '    strSQL &= vbCrLf & "HAWB.GOODS_DESCRIPTION"
        //        '    strSQL &= vbCrLf & "from JOB_CARD_TRN JAE,"
        //        '    strSQL &= vbCrLf & "HAWB_EXP_TBL HAWB,"
        //        '    strSQL &= vbCrLf & "  Booking_AIR_Tbl BAT,"
        //        '    strSQL &= vbCrLf & " Port_Mst_Tbl POL,"
        //        '    strSQL &= vbCrLf & " Port_Mst_Tbl POD,"
        //        '    strSQL &= vbCrLf & " Place_Mst_Tbl PMT,"
        //        '    strSQL &= vbCrLf & "Customer_Mst_Tbl CustMstShipper,"
        //        '    strSQL &= vbCrLf & "Customer_Mst_Tbl CustMstConsignee,"
        //        '    strSQL &= vbCrLf & " Agent_Mst_Tbl AgentMst,"
        //        '    strSQL &= vbCrLf & "Customer_Contact_Dtls CustShipperDtls,"
        //        '    strSQL &= vbCrLf & " Customer_Contact_Dtls CustConsigneeDtls,"
        //        '    strSQL &= vbCrLf & "  Agent_Contact_Dtls AgentDtls"
        //        '    strSQL &= vbCrLf & " where JAE.JOB_CARD_TRN_PK = HAWB.JOB_CARD_AIR_EXP_FK"
        //        '    strSQL &= vbCrLf & "and   JAE.BOOKING_AIR_FK=BAT.BOOKING_AIR_PK"
        //        '    strSQL &= vbCrLf & "and   POL.PORT_MST_PK(+)=BAT.Port_Mst_Pol_Fk"
        //        '    strSQL &= vbCrLf & "and   POD.PORT_MST_PK(+)=BAT.Port_Mst_Pod_Fk"
        //        '    strSQL &= vbCrLf & "and   PMT.PLACE_PK(+)=BAT.Del_Place_Mst_Fk"
        //        '    strSQL &= vbCrLf & "and   HAWB.Shipper_Cust_Mst_Fk=CustMstShipper.Customer_Mst_Pk(+)"
        //        '    strSQL &= vbCrLf & "and   HAWB.Consignee_Cust_Mst_Fk=CustMstConsignee.Customer_Mst_Pk(+)"
        //        '    strSQL &= vbCrLf & "and   HAWB.Dp_Agent_Mst_Fk=AgentMst.Agent_Mst_Pk(+)"
        //        '    strSQL &= vbCrLf & "and   CustMstShipper.Customer_Mst_Pk=CustShipperDtls.Customer_Mst_Fk(+)"
        //        '    strSQL &= vbCrLf & "and   CustMstConsignee.Customer_Mst_Pk=CustConsigneeDtls.Customer_Mst_Fk(+)"
        //        '    strSQL &= vbCrLf & "and   AgentMst.Agent_Mst_Pk=AgentDtls.Agent_Mst_Fk(+)"
        //        '    strSQL &= vbCrLf & "and   HAWB.HAWB_EXP_TBL_PK=" & HBLPK

        //        '    Try
        //        '        Return (objWF.GetDataSet(strSQL))
        //        '    Catch sqlExp As Exception
        //        '        ErrorMessage = sqlExp.Message
        //        '        Throw sqlExp
        //        '    End Try
        //        'End Function
        //#End Region

        #endregion " Hbl Report "

        #region "Get Master JobCard Pk"

        /// <summary>
        /// Gets the MJCPK.
        /// </summary>
        /// <param name="PKVal">The pk value.</param>
        /// <returns></returns>
        public Int32 GetMJCPK(long PKVal)
        {
            try
            {
                System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
                string MJCPK = null;
                WorkFlow objwf = new WorkFlow();
                //strSQL = " "
                strSQL.Append(" SELECT NVL(JC.MASTER_JC_FK,0) MJCPK ");
                strSQL.Append("  FROM JOB_CARD_TRN JC ");
                strSQL.Append("  WHERE JC.JOB_CARD_TRN_PK =" + PKVal);

                MJCPK = objwf.ExecuteScaler(strSQL.ToString());
                if (!string.IsNullOrEmpty(MJCPK))
                {
                    return Convert.ToInt32(MJCPK);
                }
                else
                {
                    return 0;
                }
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Get Master JobCard Pk"

        #region "Fetching JobPk based on HBL"

        /// <summary>
        /// Fetches the jobpk.
        /// </summary>
        /// <param name="hawbPk">The hawb pk.</param>
        /// <returns></returns>
        public object FetchJobpk(string hawbPk = "0")
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder(5000);
            WorkFlow objwf = new WorkFlow();
            strSQL.Append(" SELECT H.HAWB_EXP_TBL_PK,");
            strSQL.Append(" JOB.JOB_CARD_TRN_PK");
            strSQL.Append(" FROM HAWB_EXP_TBL H,");
            strSQL.Append("  JOB_CARD_TRN JOB");
            strSQL.Append("  WHERE H.HAWB_EXP_TBL_PK = JOB.HBL_HAWB_FK");
            strSQL.Append("  AND H.HAWB_EXP_TBL_PK =" + hawbPk);
            try
            {
                return (objwf.GetDataSet(strSQL.ToString()));
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

        #endregion "Fetching JobPk based on HBL"

        #region "Credit Control"

        /// <summary>
        /// Fetches the credit details.
        /// </summary>
        /// <param name="JOBPk">The job pk.</param>
        /// <returns></returns>
        public DataSet FetchCreditDetails(string JOBPk = "0")
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder(5000);
            WorkFlow objwf = new WorkFlow();

            strSQL.Append(" SELECT JOB.JOB_CARD_TRN_PK,");
            strSQL.Append(" JOB.SHIPPER_CUST_MST_FK,");
            strSQL.Append(" CMT.SEA_CREDIT_LIMIT,");
            strSQL.Append(" JOB.JOBCARD_REF_NO,");
            strSQL.Append(" SUM(NVL(JFD.EXCHANGE_RATE * JFD.FREIGHT_AMT, 0)) AS TOTAL");
            strSQL.Append(" FROM JOB_TRN_FD JFD, ");
            strSQL.Append(" JOB_CARD_TRN JOB,");
            strSQL.Append(" CUSTOMER_MST_TBL     CMT");
            strSQL.Append(" WHERE JOB.JOB_CARD_TRN_PK = JFD.JOB_CARD_TRN_FK");
            strSQL.Append(" AND JOB.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
            strSQL.Append(" AND JOB.JOB_CARD_TRN_PK = " + JOBPk);
            strSQL.Append(" GROUP BY JOB_CARD_TRN_PK,");
            strSQL.Append("  SHIPPER_CUST_MST_FK,SEA_CREDIT_LIMIT,JOB.JOBCARD_REF_NO");

            try
            {
                return (objwf.GetDataSet(strSQL.ToString()));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
            }
            return new DataSet();
        }

        #endregion "Credit Control"

        #region "Fetching CreditPolicy Details based on Shipper"

        /// <summary>
        /// Fetches the credit policy.
        /// </summary>
        /// <param name="ShipperPK">The shipper pk.</param>
        /// <returns></returns>
        public object FetchCreditPolicy(string ShipperPK = "0")
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            strSQL.Append(" SELECT CMT.CREDIT_LIMIT,");
            strSQL.Append(" cmt.customer_name,");
            strSQL.Append(" CMT.CREDIT_DAYS,");
            strSQL.Append(" CMT.AIR_APP_BOOKING,");
            strSQL.Append(" CMT.AIR_APP_BL_RELEASE,");
            strSQL.Append(" CMT.AIR_APP_RELEASE_ODR");
            strSQL.Append(" FROM CUSTOMER_MST_TBL CMT");
            strSQL.Append(" WHERE CMT.CUSTOMER_MST_PK = " + ShipperPK);

            try
            {
                return (objWF.GetDataSet(strSQL.ToString()));
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

        #endregion "Fetching CreditPolicy Details based on Shipper"

        #region "HAWB Standard Print Main"

        /// <summary>
        /// Haws the b_ main print.
        /// </summary>
        /// <param name="HAWBPk">The hawb pk.</param>
        /// <param name="LocPk">The loc pk.</param>
        /// <returns></returns>
        public object HAWB_MainPrint(string HAWBPk = "0", int LocPk = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT DISTINCT H.HAWB_EXP_TBL_PK,");
            sb.Append("       SH.CUSTOMER_NAME AS SHIPPER,");
            sb.Append("       H.SHIPPER_ADDRESS,");
            sb.Append("       CO.CUSTOMER_NAME AS CONSIGNEE,");
            sb.Append("       H.CONSIGNEE_ADDRESS,");
            sb.Append(" (case  when NVL( H.sac_n1,0) = '0' then    N1.customer_name  else 'Same As Consignee' end) as NOTIFY1,");
            // sb.Append("       N1.CUSTOMER_NAME AS NOTIFY1,")
            sb.Append("       H.NOTIFY1_ADDRESS,");
            //  sb.Append("       N2.CUSTOMER_NAME AS NOTIFY2,")
            sb.Append("   (case when NVL(H.sac_n2,0) = '0' then N2.customer_name else 'Same As Consignee' end) as NOTIFY2,");
            sb.Append("       H.NOTIFY2_ADDRESS,");
            sb.Append("       J.JOBCARD_REF_NO,");
            sb.Append("       B.BOOKING_REF_NO,");
            sb.Append("       H.HAWB_REF_NO,");
            sb.Append("       H.HAWB_DATE,");
            sb.Append("       H.FLIGHT_NO,");
            sb.Append("       AIR.AIRLINE_NAME,");
            sb.Append("       J.UCR_NO,");
            sb.Append("       SHT.INCO_CODE,");
            sb.Append("       MOV.CARGO_MOVE_CODE,");
            sb.Append("       DECODE(J.PYMT_TYPE, 1, 'Prepaid', 2, 'Collect') PYMT_TYPE,");
            sb.Append("       H.DECL_VAL_FOR_CUSTOMS,");
            sb.Append("       H.DEL_VAL_FOR_CARRIAGE,");
            sb.Append("       B.PORT_MST_POL_FK,");
            sb.Append("       B.PORT_MST_POD_FK,  ");
            sb.Append("       UPPER(POL.PORT_ID || ', ' || POL.PORT_NAME) AS AOO,");
            sb.Append("       UPPER(POD.PORT_ID || ', ' || POD.PORT_NAME) AS AOD,");
            sb.Append("       CASE WHEN TRIM(COLP.PLACE_NAME) IS NULL THEN COLP.PLACE_CODE ELSE UPPER(COLP.PLACE_CODE || ', ' || COLP.PLACE_NAME) END AS RECP, ");
            sb.Append("       CASE WHEN TRIM(DELP.PLACE_NAME) IS NULL THEN DELP.PLACE_CODE ELSE UPPER(DELP.PLACE_CODE || ', ' || DELP.PLACE_NAME) END AS DELP, ");
            //sb.Append("       UPPER(COLP.PLACE_CODE || ', ' || COLP.PLACE_NAME) AS RECP,")
            //sb.Append("       UPPER(DELP.PLACE_CODE || ', ' || DELP.PLACE_NAME) AS DELP,")
            sb.Append("       H.MARKS_NUMBERS,");
            sb.Append("       H.GOODS_DESCRIPTION,");
            sb.Append("       '' PALETTE_SIZE,");
            sb.Append("       PT.PACK_TYPE_DESC PACKTYPE,");
            sb.Append("       H.TOTAL_PACK_COUNT,");
            sb.Append("       H.TOTAL_CHARGE_WEIGHT,");
            sb.Append("       H.TOTAL_GROSS_WEIGHT,");
            sb.Append("       H.TOTAL_VOLUME");
            sb.Append("  FROM HAWB_EXP_TBL           H,");
            sb.Append("       JOB_CARD_TRN   J,");
            sb.Append("       BOOKING_MST_TBL        B,");
            sb.Append("       JOB_TRN_CONT   JEC,");
            sb.Append("       CUSTOMER_MST_TBL       SH,");
            sb.Append("       CUSTOMER_MST_TBL       CO,");
            sb.Append("       CUSTOMER_MST_TBL       N1,");
            sb.Append("       CUSTOMER_MST_TBL       N2,");
            sb.Append("       PORT_MST_TBL           POL,");
            sb.Append("       PORT_MST_TBL           POD,");
            sb.Append("       AIRLINE_MST_TBL        AIR,");
            sb.Append("       PLACE_MST_TBL          COLP,");
            sb.Append("       PLACE_MST_TBL          DELP,");
            sb.Append("       CARGO_MOVE_MST_TBL     MOV,");
            sb.Append("       SHIPPING_TERMS_MST_TBL SHT,");
            sb.Append("       PACK_TYPE_MST_TBL     PT,");
            sb.Append("       LOCATION_MST_TBL       LOC ");
            sb.Append("  WHERE J.BOOKING_MST_FK = B.BOOKING_MST_PK");
            sb.Append("   AND B.CARRIER_MST_FK = AIR.AIRLINE_MST_PK(+)");
            sb.Append("   AND ((J.JOB_CARD_TRN_PK = H.JOB_CARD_AIR_EXP_FK)");
            sb.Append("   OR (J.JOB_CARD_TRN_PK = H.NEW_JOB_CARD_AIR_EXP_FK))");
            sb.Append("   AND J.JOB_CARD_TRN_PK = JEC.JOB_CARD_TRN_FK");
            sb.Append("   AND SH.CUSTOMER_MST_PK(+) = H.SHIPPER_CUST_MST_FK");
            sb.Append("   AND CO.CUSTOMER_MST_PK(+) = H.CONSIGNEE_CUST_MST_FK");
            sb.Append("   AND N1.CUSTOMER_MST_PK(+) = H.NOTIFY1_CUST_MST_FK");
            sb.Append("   AND N2.CUSTOMER_MST_PK(+) = H.NOTIFY2_CUST_MST_FK");
            sb.Append("   AND POL.PORT_MST_PK(+) = B.PORT_MST_POL_FK");
            sb.Append("   AND POD.PORT_MST_PK(+) = B.PORT_MST_POD_FK");
            sb.Append("   AND MOV.CARGO_MOVE_PK(+) = J.CARGO_MOVE_FK");
            sb.Append("   AND JEC.PACK_TYPE_MST_FK=PT.PACK_TYPE_MST_PK(+)");
            sb.Append("   AND SHT.SHIPPING_TERMS_MST_PK(+) = J.SHIPPING_TERMS_MST_FK");
            sb.Append("   AND COLP.PLACE_PK(+) = B.COL_PLACE_MST_FK");
            sb.Append("   AND DELP.PLACE_PK(+) = B.DEL_PLACE_MST_FK");
            sb.Append("   AND LOC.LOCATION_MST_PK =" + LocPk);
            sb.Append("   AND H.HAWB_EXP_TBL_PK = " + HAWBPk);
            try
            {
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

        #endregion "HAWB Standard Print Main"

        #region "HAWB Standard Print Freight"

        /// <summary>
        /// Haws the b_ freight.
        /// </summary>
        /// <param name="HAWBPk">The hawb pk.</param>
        /// <returns></returns>
        public object HAWB_FREIGHT(string HAWBPk = "0")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT FEMT.FREIGHT_ELEMENT_NAME,");
            sb.Append("       CTMT.CURRENCY_ID,");
            sb.Append("       FEMT.PREFERENCE,");
            sb.Append("       CASE WHEN JFD.FREIGHT_TYPE=1 THEN");
            sb.Append("       SUM(JFD.FREIGHT_AMT) ");
            sb.Append("       END PREPAID,");
            sb.Append("       CASE WHEN JFD.FREIGHT_TYPE=2 THEN");
            sb.Append("       SUM(JFD.FREIGHT_AMT) ");
            sb.Append("       END COLLECT");
            sb.Append(" FROM HAWB_EXP_TBL         H,");
            sb.Append("       JOB_CARD_TRN J,");
            sb.Append("       JOB_TRN_FD JFD,");
            sb.Append("       FREIGHT_ELEMENT_MST_TBL FEMT,");
            sb.Append("       CURRENCY_TYPE_MST_TBL   CTMT");
            sb.Append(" WHERE J.JOB_CARD_TRN_PK = H.JOB_CARD_AIR_EXP_FK");
            sb.Append("   AND J.JOB_CARD_TRN_PK = JFD.JOB_CARD_TRN_FK");
            sb.Append("   AND FEMT.FREIGHT_ELEMENT_MST_PK = JFD.FREIGHT_ELEMENT_MST_FK");
            sb.Append("   AND CTMT.CURRENCY_MST_PK = JFD.CURRENCY_MST_FK");
            sb.Append("   AND H.HAWB_EXP_TBL_PK = " + HAWBPk);
            sb.Append(" GROUP BY FEMT.FREIGHT_ELEMENT_NAME,");
            sb.Append("       CTMT.CURRENCY_ID,");
            sb.Append("       JFD.FREIGHT_TYPE,");
            sb.Append("       FEMT.PREFERENCE");
            sb.Append(" ORDER BY FEMT.PREFERENCE");

            try
            {
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

        #endregion "HAWB Standard Print Freight"

        #region "HAWB PALETTE"

        /// <summary>
        /// Haws the b_ palette.
        /// </summary>
        /// <param name="HAWBPk">The hawb pk.</param>
        /// <returns></returns>
        public object HAWB_PALETTE(string HAWBPk = "0")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT DISTINCT JEC.PALETTE_SIZE");
            sb.Append("  FROM HAWB_EXP_TBL H, ");
            sb.Append("  JOB_CARD_TRN J, ");
            sb.Append("  JOB_TRN_CONT JEC");
            sb.Append(" WHERE J.JOB_CARD_TRN_PK = H.JOB_CARD_AIR_EXP_FK");
            sb.Append("   AND J.JOB_CARD_TRN_PK = JEC.JOB_CARD_TRN_FK");
            sb.Append("   AND H.HAWB_EXP_TBL_PK = " + HAWBPk);
            try
            {
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

        #endregion "HAWB PALETTE"

        #region "Get Import Job Card Pks "

        /// <summary>
        /// Gets the import JCP ks.
        /// </summary>
        /// <param name="HBLNr">The HBL nr.</param>
        /// <returns></returns>
        public string GetImportJCPKs(string HBLNr)
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL.Append("SELECT ROWTOCOL('");
                strSQL.Append(" SELECT 0 AS JCPK FROM DUAL");
                strSQL.Append(" UNION");
                strSQL.Append(" SELECT JCAIT.JOB_CARD_TRN_PK AS JCPK FROM JOB_CARD_TRN JCAIT");
                strSQL.Append(" WHERE JCAIT.Hbl_Hawb_Ref_No=''" + HBLNr + "'' AND JCAIT.JC_AUTO_MANUAL=1')");
                strSQL.Append(" FROM DUAL");

                return objWF.ExecuteScaler(strSQL.ToString());
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

        #endregion "Get Import Job Card Pks "
    }
}