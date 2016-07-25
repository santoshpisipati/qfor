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
using System.Data;
using System.Web;

namespace Quantum_QFOR
{
    public class clsSalesReport : CommonFeatures
    {
        private cls_TrackAndTrace objTrackNTrace = new cls_TrackAndTrace();

        #region "GetMainBookingData"

        //PPD value .. if prepaid then value is one else two
        //this block modifing by thiyagarajan on 16/4/09 for introducing grid functionality
        public DataSet GetSalesReport(Int32 pageload, string polpk, string podpk, string emppk, string custpk, int reportType, string Loc, string LocFK, string fromDate, string toDate,
        int isPPD = 0, int isSPL = 0, Int32 Report = 2, string Process = "")
        {
            WorkFlow objWF = new WorkFlow();
            string SQL = null;
            string deprtmnt = null;
            string deprtmnts = null;
            string strCondition = " ";
            if (isPPD == 1 | isPPD == 2)
            {
                strCondition = strCondition + " AND J.PYMT_TYPE = " + isPPD;
            }
            strCondition = strCondition + " ORDER BY J.JOBCARD_DATE DESC ";
            if (isSPL == 1)
            {
                strCondition = strCondition + " , SALES_REPORTER";
            }
            if (Report == 2 | Report == 3)
            {
                deprtmnt = "select  ";
                deprtmnts = " '" + Process + "' AS DEPARTMENT, ";
            }
            else
            {
                deprtmnt = "select '" + Process + "' AS DEPARTMENT,";
                deprtmnts = " ";
            }
            // SeaExport
            if (reportType == 1)
            {
                SQL = deprtmnt +
                    "       TO_CHAR(J.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH," +
                    "       TO_CHAR(J.JOBCARD_DATE, dateformat) JOBCARD_DATE," +
                    "       J.JOBCARD_REF_NO REF_NUMBER," + deprtmnts +
                    "       SHIPPER.CUSTOMER_NAME SHIPPER_NAME," +
                    "       CONSIGNEE.CUSTOMER_NAME CONSIGNEE_NAME," +
                    "       DECODE(J.PYMT_TYPE, 1, 'PPD', 2, 'CLT') PAYMENT_TYPE," +
                    "       POL.PORT_ID POL," +
                    "       POD.PORT_ID POD," +
                    "       (CASE" +
                    "         WHEN BOOK.CARGO_TYPE = 1 THEN" +
                    "          (SELECT SUM(NVL(CONT.NET_WEIGHT, 0))" +
                    "             FROM JOB_TRN_CONT CONT" +
                    "            WHERE CONT.job_card_trn_fk = J.job_card_trn_pk)" +
                    "         ELSE" +
                    "          (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0))" +
                    "             FROM JOB_TRN_CONT CONT" +
                    "            WHERE CONT.job_card_trn_fk = J.job_card_trn_pk)" +
                    "       END) WEIGHT_VOLUME,";
                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4)) FREIGHT, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";

                SQL += "       EMP.EMPLOYEE_NAME SALES_REPORTER" +
                     "  from JOB_CARD_TRN J," +
                     "       BOOKING_MST_TBL      BOOK," +
                     "       CUSTOMER_MST_TBL     SHIPPER," +
                     "       CUSTOMER_MST_TBL     CONSIGNEE," +
                     "       PORT_MST_TBL         POL," +
                     "       PORT_MST_TBL         POD," +
                     "       EMPLOYEE_MST_TBL     EMP";
                SQL = SQL + "       ,USER_MST_TBL         USR ,location_mst_tbl l";
                SQL = SQL + " WHERE J.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)" +
                     "   AND J.BOOKING_MST_FK = BOOK.BOOKING_MST_PK" +
                     "   AND J.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)" +
                     "   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK" +
                     "   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK" +
                     "   AND POL.location_mst_fk = l.location_mst_pk and l.location_mst_pk=usr.default_location_fk " + "   AND SHIPPER.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+) ";
                SQL += "   AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))";
                SQL = SQL + "   AND j.created_by_fk = usr.user_mst_pk ";
                if (Report == 4)
                {
                    SQL += " and J.PYMT_TYPE=1";
                }
                else if (Report == 5)
                {
                    SQL += " and J.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Convert.ToInt32(Loc)) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(Convert.ToInt32(custpk)) > 0)
                {
                    SQL += " and J.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(Convert.ToInt32(emppk)) > 0)
                {
                    SQL += " and SHIPPER.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(Convert.ToInt32(polpk)) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(Convert.ToInt32(podpk)) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POD_FK=" + podpk;
                }
                SQL = SQL + " order by J.JOBCARD_DATE DESC , J.JOBCARD_REF_NO DESC";
                //end
                //Sea Import
            }
            else if (reportType == 2)
            {
                SQL = deprtmnt + "TO_CHAR(Jc.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH," + "TO_CHAR(Jc.JOBCARD_DATE, dateformat) JOBCARD_DATE," + "Jc.JOBCARD_REF_NO REF_NUMBER," + deprtmnts + "sh.CUSTOMER_NAME SHIPPER_NAME," + "co.CUSTOMER_NAME CONSIGNEE_NAME," + "DECODE(Jc.PYMT_TYPE, 1, 'PPD', 2, 'CLT') PAYMENT_TYPE," + "POL.PORT_ID POL," + "POD.PORT_ID POD," + "(CASE WHEN jc.CARGO_TYPE = 1 THEN (SELECT SUM(NVL(CONT.NET_WEIGHT, 0)) FROM JOB_TRN_CONT CONT WHERE CONT.job_card_trn_fk = Jc.job_card_trn_pk) ELSE (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0)) FROM JOB_TRN_CONT CONT WHERE CONT.job_card_trn_fk = Jc.job_card_trn_pk) END) WEIGHT_VOLUME,";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4)) FREIGHT, " + "       /*FETCH_FREIGHT_IMPSEA(jc.job_card_trn_pk) FETCH_FREIGHT_IMPSEA_NEW(jc.job_card_trn_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ") FREIGHT,*/ " + "       EMP.EMPLOYEE_NAME SALES_REPORTER " + "       from JOB_CARD_TRN jC," + "       /*JOB_TRN_CONT cont,*/" + "       CUSTOMER_MST_TBL     SH," + "       CUSTOMER_MST_TBL     CO," + "       PORT_MST_TBL         POL," + "       PORT_MST_TBL         POD," + "      AGENT_MST_TBL        POLA," + "       EMPLOYEE_MST_TBL     EMP,location_mst_tbl l," + "       USER_MST_TBL UMT" + "where JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)" + " /* AND jc.job_card_trn_pk = cont.job_card_trn_fk(+) */" + "AND sh.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)" + "AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)" + "AND JC.PORT_MST_POL_FK = POL.PORT_MST_PK" + "AND JC.PORT_MST_POD_FK = POD.PORT_MST_PK" + "AND JC.POL_AGENT_MST_FK = POLA.AGENT_MST_PK(+)";
                SQL += " AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS = 1)";
                SQL += "  AND jc.CREATED_BY_FK = UMT.USER_MST_PK and l.location_mst_pk=umt.default_location_fk ";
                SQL += "  AND ((UMT.DEFAULT_LOCATION_FK = l.location_mst_pk and JC.JC_AUTO_MANUAL = 0) OR (JC.PORT_MST_POD_FK  ";
                SQL += "  IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK= l.location_mst_pk)  and JC.JC_AUTO_MANUAL = 1)) ";

                SQL += "  AND SH.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)";
                SQL += " AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))"
                    ;

                if (Report == 4)
                {
                    SQL += " and JC.PYMT_TYPE=1";
                }
                else if (Report == 5)
                {
                    SQL += " and JC.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(Convert.ToInt32(custpk)) > 0)
                {
                    SQL += " and JC.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(Convert.ToInt32(emppk)) > 0)
                {
                    SQL += " and SH.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(Convert.ToInt32(polpk)) > 0)
                {
                    SQL += " and JC.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(Convert.ToInt32(podpk)) > 0)
                {
                    SQL += " and JC.PORT_MST_POD_FK=" + podpk;
                }
                SQL += " ORDER BY JOBCARD_DATE DESC, JOBCARD_REF_NO DESC";

                //Air Export
            }
            else if (reportType == 3)
            {
                SQL = deprtmnt + "       TO_CHAR(J.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH," + "       TO_CHAR(J.JOBCARD_DATE, dateformat) JOBCARD_DATE," + "       J.JOBCARD_REF_NO REF_NUMBER," + deprtmnts + "       SHIPPER.CUSTOMER_NAME SHIPPER_NAME," + "       CONSIGNEE.CUSTOMER_NAME CONSIGNEE_NAME," + "       DECODE(J.PYMT_TYPE, 1, 'Y', 2, 'N') PAYMENT_TYPE," + "       POL.PORT_ID POL," + "       POD.PORT_ID POD," + "       (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0))" + "             FROM JOB_TRN_CONT CONT" + "            WHERE CONT.job_card_trn_fk = J.job_card_trn_pk" + "       ) WEIGHT_VOLUME,";
                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4)) FREIGHT, " + "       /*FETCH_FREIGHT_EXPAIR(j.job_card_trn_pk)  FETCH_EST_COST_EXPAIR_NEW(j.job_card_trn_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + " )  FREIGHT,*/ " + "       EMP.EMPLOYEE_NAME SALES_REPORTER" + "  from JOB_CARD_TRN J," + "       BOOKING_MST_TBL      BOOK," + "       CUSTOMER_MST_TBL     SHIPPER," + "       CUSTOMER_MST_TBL     CONSIGNEE," + "       PORT_MST_TBL         POL," + "       PORT_MST_TBL         POD," + "       EMPLOYEE_MST_TBL     EMP";
                SQL = SQL + "       ,USER_MST_TBL  USR,location_mst_tbl l  ";
                SQL = SQL + " WHERE J.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)" + "   AND J.BOOKING_MST_FK = BOOK.BOOKING_MST_PK" + "   AND J.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)" + "   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK" + "   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK" + "   AND POL.location_mst_fk = l.location_mst_pk  and l.location_mst_pk=usr.default_location_fk " + "   AND SHIPPER.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+) " + "   AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))"
                    ;
                SQL = SQL + " AND j.created_by_fk = usr.user_mst_pk ";
                if (Report == 4)
                {
                    SQL += " and J.PYMT_TYPE=1";
                }
                else if (Report == 5)
                {
                    SQL += " and J.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and J.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SHIPPER.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POD_FK=" + podpk;
                }
                SQL = SQL + " order by J.JOBCARD_DATE DESC , J.JOBCARD_REF_NO DESC";
                //Air Import
            }
            else if (reportType == 4)
            {
                SQL = deprtmnt + " TO_CHAR(Jc.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH," + " TO_CHAR(Jc.JOBCARD_DATE, dateformat) JOBCARD_DATE," + " Jc.JOBCARD_REF_NO REF_NUMBER," + deprtmnts + " SH.CUSTOMER_NAME SHIPPER_NAME," + " CO.CUSTOMER_NAME CONSIGNEE_NAME," + " DECODE(Jc.PYMT_TYPE, 1, 'Y', 2, 'N') PAYMENT_TYPE," + " POL.PORT_ID POL," + " POD.PORT_ID POD," + " (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0))" + "    FROM JOB_TRN_CONT CONT" + "   WHERE CONT.job_card_trn_fk = Jc.job_card_trn_pk) WEIGHT_VOLUME,"
                    ;

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4)) FREIGHT, " + " /*FETCH_FREIGHT_IMPAIR(jc.job_card_trn_pk)  FETCH_FREIGHT_IMPAIR_NEW(jc.job_card_trn_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + " ) FREIGHT,*/" + "  EMP.EMPLOYEE_NAME SALES_REPORTER  " + "  from JOB_CARD_TRN JC," + "  CUSTOMER_MST_TBL     SH," + "  CUSTOMER_MST_TBL     CO," + "  PORT_MST_TBL         POL," + "  PORT_MST_TBL         POD," + "  AGENT_MST_TBL        POLA," + "  USER_MST_TBL         UMT,location_mst_tbl l, " + "  EMPLOYEE_MST_TBL     EMP" + "  where JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)" + "  AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)" + "  AND JC.PORT_MST_POL_FK = POL.PORT_MST_PK" + "  AND JC.PORT_MST_POD_FK = POD.PORT_MST_PK"
                    ;
                SQL += "  AND JC.POL_AGENT_MST_FK = POLA.AGENT_MST_PK(+)";
                SQL += " AND  jc.CREATED_BY_FK = UMT.USER_MST_PK   "
                    ;
                SQL += "  AND ((UMT.DEFAULT_LOCATION_FK = l.location_mst_pk and JC.JC_AUTO_MANUAL = 0) OR (JC.PORT_MST_POD_FK  ";
                SQL += "  IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK= l.location_mst_pk)  and JC.JC_AUTO_MANUAL = 1))  ";

                SQL += "  AND SH.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)"
                    ;
                SQL += " AND SH.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)"
                    ;
                SQL += " AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS = 1)"
                    ;
                SQL += " AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))"
                    ;
                SQL += "  and l.location_mst_pk = umt.default_location_fk ";

                if (Report == 4)
                {
                    SQL += " and JC.PYMT_TYPE=1";
                }
                else if (Report == 5)
                {
                    SQL += " and JC.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and JC.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SH.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POD_FK=" + podpk;
                }

                SQL += " ORDER BY JOBCARD_DATE DESC, JOBCARD_REF_NO DESC"
                    ;
                //Air ExportImport
            }
            else if (reportType == 5)
            {
                //Air Export
                SQL = deprtmnt + "       TO_CHAR(J.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH," + "       TO_CHAR(J.JOBCARD_DATE, dateformat) JOBCARD_DATE," + "       J.JOBCARD_REF_NO REF_NUMBER," + deprtmnts + "       SHIPPER.CUSTOMER_NAME SHIPPER_NAME," + "       CONSIGNEE.CUSTOMER_NAME CONSIGNEE_NAME," + "       DECODE(J.PYMT_TYPE, 1, 'Y', 2, 'N') PAYMENT_TYPE," + "       POL.PORT_ID POL," + "       POD.PORT_ID POD," + "       (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0))" + "             FROM JOB_TRN_CONT CONT" + "            WHERE CONT.job_card_trn_fk = J.job_card_trn_pk" + "       ) WEIGHT_VOLUME,"
                    ;

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4)) FREIGHT, " + "       /*FETCH_FREIGHT_EXPAIR(j.job_card_trn_pk)  FETCH_EST_COST_EXPAIR_NEW(j.job_card_trn_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + " )  FREIGHT,*/ " + "       EMP.EMPLOYEE_NAME SALES_REPORTER" + "  from JOB_CARD_TRN J," + "       BOOKING_MST_TBL      BOOK," + "       CUSTOMER_MST_TBL     SHIPPER," + "       CUSTOMER_MST_TBL     CONSIGNEE," + "       PORT_MST_TBL         POL," + "       PORT_MST_TBL         POD," + "       EMPLOYEE_MST_TBL     EMP"
                    ;
                SQL = SQL + "       ,USER_MST_TBL  USR,location_mst_tbl l  "
                    ;
                SQL = SQL + " WHERE J.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)" + "   AND J.BOOKING_MST_FK = BOOK.BOOKING_MST_PK" + "   AND J.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)" + "   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK" + "   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK" + "   AND POL.location_mst_fk = l.location_mst_pk  and l.location_mst_pk=usr.default_location_fk " + "   AND SHIPPER.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+) " + "   AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))"
                    ;
                SQL = SQL + " AND j.created_by_fk = usr.user_mst_pk ";
                if (Report == 4)
                {
                    SQL += " and J.PYMT_TYPE=1";
                }
                else if (Report == 5)
                {
                    SQL += " and J.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and J.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SHIPPER.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POD_FK=" + podpk;
                }

                ///'''''''''''''''''''''''''
                SQL += " UNION ";
                ///'''''''''''''''''''''''''''
                //Air Import

                SQL += deprtmnt + " TO_CHAR(Jc.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH," + " TO_CHAR(Jc.JOBCARD_DATE, dateformat) JOBCARD_DATE," + " Jc.JOBCARD_REF_NO REF_NUMBER," + deprtmnts + " SH.CUSTOMER_NAME SHIPPER_NAME," + " CO.CUSTOMER_NAME CONSIGNEE_NAME," + " DECODE(Jc.PYMT_TYPE, 1, 'Y', 2, 'N') PAYMENT_TYPE," + " POL.PORT_ID POL," + " POD.PORT_ID POD," + " (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0))" + "    FROM JOB_TRN_CONT CONT" + "   WHERE CONT.job_card_trn_fk = Jc.job_card_trn_pk) WEIGHT_VOLUME,"
                    ;

                //SQL &= " FETCH_EST_COST_IMPair(jc.job_card_trn_pk) ESTIMATED_REVENUE," & vbNewLine
                //SQL &= " FETCH_JOB_CARD_AIR_IMP_ACTREV(jc.job_card_trn_pk," & HttpContext.Current.Session("currency_mst_pk") & " ) ACTUAL_REVENUE," & vbNewLine
                //SQL &= " NVL(FETCH_ACT_COST_IMPAIR(jc.job_card_trn_pk), 0) ACTUAL_COST," & vbNewLine & _

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4)) FREIGHT, " + " /*FETCH_FREIGHT_IMPAIR(jc.job_card_trn_pk)  FETCH_FREIGHT_IMPAIR_NEW(jc.job_card_trn_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + " ) FREIGHT,*/" + "  EMP.EMPLOYEE_NAME SALES_REPORTER  " + "  from JOB_CARD_TRN JC," + "  CUSTOMER_MST_TBL     SH," + "  CUSTOMER_MST_TBL     CO," + "  PORT_MST_TBL         POL," + "  PORT_MST_TBL         POD," + "  AGENT_MST_TBL        POLA," + "  USER_MST_TBL         UMT,location_mst_tbl l, " + "  EMPLOYEE_MST_TBL     EMP" + "  where JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)" + "  AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)" + "  AND JC.PORT_MST_POL_FK = POL.PORT_MST_PK" + "  AND JC.PORT_MST_POD_FK = POD.PORT_MST_PK"
                    ;
                SQL += "  AND JC.POL_AGENT_MST_FK = POLA.AGENT_MST_PK(+)";
                //SQL &= " AND (JC.PORT_MST_POD_FK IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK = " & LocFK & ") and JC.JC_AUTO_MANUAL = 1)" & vbNewLine
                //SQL &= " AND jc.CREATED_BY_FK = UMT.USER_MST_PK" & vbNewLine
                SQL += " AND  jc.CREATED_BY_FK = UMT.USER_MST_PK   "
                    ;
                SQL += "  AND ((UMT.DEFAULT_LOCATION_FK = l.location_mst_pk and JC.JC_AUTO_MANUAL = 0) OR (JC.PORT_MST_POD_FK  ";
                SQL += "  IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK= l.location_mst_pk)  and JC.JC_AUTO_MANUAL = 1))  ";

                SQL += "  AND SH.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)"
                    ;
                SQL += " AND SH.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)"
                    ;
                //SQL &=    " AND POL.location_mst_fk ='" & Loc & "'" & vbNewLine
                SQL += " AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS = 1)"
                    ;
                SQL += " AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))"
                    ;
                SQL += "  and l.location_mst_pk = umt.default_location_fk ";

                if (Report == 4)
                {
                    SQL += " and JC.PYMT_TYPE=1";
                }
                else if (Report == 5)
                {
                    SQL += " and JC.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and JC.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SH.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POD_FK=" + podpk;
                }

                //Sea Export-Import
            }
            else if (reportType == 6)
            {
                //Sea Export
                SQL = deprtmnt + "       TO_CHAR(J.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH," + "       TO_CHAR(J.JOBCARD_DATE, dateformat) JOBCARD_DATE," + "       J.JOBCARD_REF_NO REF_NUMBER," + deprtmnts + "       SHIPPER.CUSTOMER_NAME SHIPPER_NAME," + "       CONSIGNEE.CUSTOMER_NAME CONSIGNEE_NAME," + "       DECODE(J.PYMT_TYPE, 1, 'PPD', 2, 'CLT') PAYMENT_TYPE," + "       POL.PORT_ID POL," + "       POD.PORT_ID POD," + "       (CASE" + "         WHEN BOOK.CARGO_TYPE = 1 THEN" + "          (SELECT SUM(NVL(CONT.NET_WEIGHT, 0))" + "             FROM JOB_TRN_CONT CONT" + "            WHERE CONT.job_card_trn_fk = J.job_card_trn_pk)" + "         ELSE" + "          (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0))" + "             FROM JOB_TRN_CONT CONT" + "            WHERE CONT.job_card_trn_fk = J.job_card_trn_pk)" + "       END) WEIGHT_VOLUME,"
                    ;
                //SQL &= "       FETCH_FREIGHT_EXPSEA(j.job_card_trn_pk) FREIGHT," & vbNewLine
                //SQL &= "       FETCH_FREIGHT_EXPSEA_NEW(j.job_card_trn_pk, " & HttpContext.Current.Session("CURRENCY_MST_PK") & ") FREIGHT,"
                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4)) FREIGHT, ";

                //SQL &= "       FETCH_JOB_CARD_SEA_EXP_ACTREV(j.job_card_trn_pk," & HttpContext.Current.Session("currency_mst_pk") & " ) ACTUAL_REVENUE, " & vbNewLine
                //SQL &= "       NVL(FETCH_ACT_COST_EXPSEA(j.job_card_trn_pk),0) ACTUAL_COST, " & vbNewLine
                //SQL &= "       FETCH_EST_COST_EXPSEA(j.job_card_trn_pk) ESTIMATED_REVENUE, " & vbNewLine

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";

                SQL += "       EMP.EMPLOYEE_NAME SALES_REPORTER" + "  from JOB_CARD_TRN J," + "       BOOKING_MST_TBL      BOOK," + "       CUSTOMER_MST_TBL     SHIPPER," + "       CUSTOMER_MST_TBL     CONSIGNEE," + "       PORT_MST_TBL         POL," + "       PORT_MST_TBL         POD," + "       EMPLOYEE_MST_TBL     EMP"
                    ;
                //If LocFK <> 0 Then ' 'Added by purnanand for PTS MIS-APR-001 for all report type
                SQL = SQL + "       ,USER_MST_TBL         USR ,location_mst_tbl l"
                    ;
                //End If
                SQL = SQL + " WHERE J.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)" + "   AND J.BOOKING_MST_FK = BOOK.BOOKING_MST_PK" + "   AND J.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)" + "   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK" + "   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK" + "   AND POL.location_mst_fk = l.location_mst_pk and l.location_mst_pk=usr.default_location_fk " + "   AND SHIPPER.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+) "
                    ;
                //adding by thiyagarajan on 8/11/08
                SQL += "   AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))";
                //If LocFK <> 0 Then
                SQL = SQL + "   AND j.created_by_fk = usr.user_mst_pk ";
                //End If

                if (Report == 4)
                {
                    SQL += " and J.PYMT_TYPE=1";
                }
                else if (Report == 5)
                {
                    SQL += " and J.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and J.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SHIPPER.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POD_FK=" + podpk;
                }

                ///''''''''''''''''''''
                SQL += " UNION ";
                ///'''''''''''''''''
                //Sea Import
                SQL += deprtmnt + "TO_CHAR(Jc.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH," + "TO_CHAR(Jc.JOBCARD_DATE, dateformat) JOBCARD_DATE," + "Jc.JOBCARD_REF_NO REF_NUMBER," + deprtmnts + "sh.CUSTOMER_NAME SHIPPER_NAME," + "co.CUSTOMER_NAME CONSIGNEE_NAME," + "DECODE(Jc.PYMT_TYPE, 1, 'PPD', 2, 'CLT') PAYMENT_TYPE," + "POL.PORT_ID POL," + "POD.PORT_ID POD," + "(CASE WHEN jc.CARGO_TYPE = 1 THEN (SELECT SUM(NVL(CONT.NET_WEIGHT, 0)) FROM JOB_TRN_CONT CONT WHERE CONT.job_card_trn_fk = Jc.job_card_trn_pk) ELSE (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0)) FROM JOB_TRN_CONT CONT WHERE CONT.job_card_trn_fk = Jc.job_card_trn_pk) END) WEIGHT_VOLUME,";
                //SQL &= "       FETCH_EST_COST_IMPSEA(jc.job_card_trn_pk) ESTIMATED_REVENUE," & vbNewLine
                //SQL &= "       FETCH_JOB_CARD_SEA_IMP_ACTREV(jc.job_card_trn_pk," & HttpContext.Current.Session("currency_mst_pk") & " ) ACTUAL_REVENUE, " & vbNewLine
                //sql &=   "       NVL(FETCH_ACT_COST_IMPSEA(jc.job_card_trn_pk),0) ACTUAL_COST, " & vbNewLine & _

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4)) FREIGHT, " + "       /*FETCH_FREIGHT_IMPSEA(jc.job_card_trn_pk) FETCH_FREIGHT_IMPSEA_NEW(jc.job_card_trn_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ") FREIGHT,*/ " + "       EMP.EMPLOYEE_NAME SALES_REPORTER " + "       from JOB_CARD_TRN jC," + "       /*JOB_TRN_CONT cont,*/" + "       CUSTOMER_MST_TBL     SH," + "       CUSTOMER_MST_TBL     CO," + "       PORT_MST_TBL         POL," + "       PORT_MST_TBL         POD," + "      AGENT_MST_TBL        POLA," + "       EMPLOYEE_MST_TBL     EMP,location_mst_tbl l," + "       USER_MST_TBL UMT" + "where JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)" + " /* AND jc.job_card_trn_pk = cont.job_card_trn_fk(+) */" + "AND sh.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)" + "AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)" + "AND JC.PORT_MST_POL_FK = POL.PORT_MST_PK" + "AND JC.PORT_MST_POD_FK = POD.PORT_MST_PK" + "AND JC.POL_AGENT_MST_FK = POLA.AGENT_MST_PK(+)"
                    ;
                // SQL &= "AND (JC.PORT_MST_POD_FK IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK =" & LocFK & ") and JC.JC_AUTO_MANUAL = 1)" & vbNewLine
                // SQL &= "AND jc.CREATED_BY_FK = UMT.USER_MST_PK" & vbNewLine & _
                SQL += " AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS = 1)"
                    ;
                //sql &=  "AND POL.location_mst_fk ='" & Loc & "'" & vbNewLine & _
                //sql &=  " AND JC.CARGO_TYPE = 1" & vbNewLine & _
                //sql &=  " AND POL.location_mst_fk ='" & Loc & "'" & vbNewLine & _
                //sql &=  "  AND JC.CARGO_TYPE = 1"
                SQL += "  AND jc.CREATED_BY_FK = UMT.USER_MST_PK and l.location_mst_pk=umt.default_location_fk "
                    ;
                SQL += "  AND ((UMT.DEFAULT_LOCATION_FK = l.location_mst_pk and JC.JC_AUTO_MANUAL = 0) OR (JC.PORT_MST_POD_FK  ";
                SQL += "  IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK= l.location_mst_pk)  and JC.JC_AUTO_MANUAL = 1)) ";

                SQL += "  AND SH.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)"
                    ;
                SQL += " AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))"
                    ;

                if (Report == 4)
                {
                    SQL += " and JC.PYMT_TYPE=1";
                }
                else if (Report == 5)
                {
                    SQL += " and JC.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and JC.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SH.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POD_FK=" + podpk;
                }
                //Air-Sea Export
            }
            else if (reportType == 7)
            {
                //Air Export
                SQL = deprtmnt + "       TO_CHAR(J.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH," + "       TO_CHAR(J.JOBCARD_DATE, dateformat) JOBCARD_DATE," + "       J.JOBCARD_REF_NO REF_NUMBER," + deprtmnts + "       SHIPPER.CUSTOMER_NAME SHIPPER_NAME," + "       CONSIGNEE.CUSTOMER_NAME CONSIGNEE_NAME," + "       DECODE(J.PYMT_TYPE, 1, 'Y', 2, 'N') PAYMENT_TYPE," + "       POL.PORT_ID POL," + "       POD.PORT_ID POD," + "       (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0))" + "             FROM JOB_TRN_CONT CONT" + "            WHERE CONT.job_card_trn_fk = J.job_card_trn_pk" + "       ) WEIGHT_VOLUME,"
                    ;
                //SQL &= "       FETCH_EST_COST_EXPair(j.job_card_trn_pk) ESTIMATED_REVENUE," & vbNewLine
                //SQL &= "       FETCH_JOB_CARD_AIR_EXP_ACTREV(j.job_card_trn_pk," & HttpContext.Current.Session("currency_mst_pk") & " )  ACTUAL_REVENUE, " & vbNewLine
                //SQL &= "       NVL(FETCH_ACT_COST_EXPAIR(j.job_card_trn_pk),0) ACTUAL_COST, " & vbNewLine & _

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4)) FREIGHT, " + "       /*FETCH_FREIGHT_EXPAIR(j.job_card_trn_pk)  FETCH_EST_COST_EXPAIR_NEW(j.job_card_trn_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + " )  FREIGHT,*/ " + "       EMP.EMPLOYEE_NAME SALES_REPORTER" + "  from JOB_CARD_TRN J," + "       BOOKING_MST_TBL      BOOK," + "       CUSTOMER_MST_TBL     SHIPPER," + "       CUSTOMER_MST_TBL     CONSIGNEE," + "       PORT_MST_TBL         POL," + "       PORT_MST_TBL         POD," + "       EMPLOYEE_MST_TBL     EMP"
                    ;
                //If LocFK <> 0 Then
                SQL = SQL + "       ,USER_MST_TBL  USR,location_mst_tbl l  "
                    ;
                //End If
                SQL = SQL + " WHERE J.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)" + "   AND J.BOOKING_MST_FK = BOOK.BOOKING_MST_PK" + "   AND J.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)" + "   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK" + "   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK" + "   AND POL.location_mst_fk = l.location_mst_pk  and l.location_mst_pk=usr.default_location_fk " + "   AND SHIPPER.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+) " + "   AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))"
                    ;
                //adding by thiyagarajan on 8/11/08
                //If LocFK <> 0 Then
                //    SQL &= " l.location_mst_pk=" & LocFK
                //End If
                SQL = SQL + " AND j.created_by_fk = usr.user_mst_pk ";
                //If pageload = 0 Then
                //    SQL &= " and 1=2 "
                //End If
                if (Report == 4)
                {
                    SQL += " and J.PYMT_TYPE=1";
                }
                else if (Report == 5)
                {
                    SQL += " and J.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and J.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SHIPPER.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POD_FK=" + podpk;
                }

                ///''''''''
                SQL += " UNION ";
                ///'''''''''''''''
                //Sea Export

                SQL += deprtmnt + "       TO_CHAR(J.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH," + "       TO_CHAR(J.JOBCARD_DATE, dateformat) JOBCARD_DATE," + "       J.JOBCARD_REF_NO REF_NUMBER," + deprtmnts + "       SHIPPER.CUSTOMER_NAME SHIPPER_NAME," + "       CONSIGNEE.CUSTOMER_NAME CONSIGNEE_NAME," + "       DECODE(J.PYMT_TYPE, 1, 'PPD', 2, 'CLT') PAYMENT_TYPE," + "       POL.PORT_ID POL," + "       POD.PORT_ID POD," + "       (CASE" + "         WHEN BOOK.CARGO_TYPE = 1 THEN" + "          (SELECT SUM(NVL(CONT.NET_WEIGHT, 0))" + "             FROM JOB_TRN_CONT CONT" + "            WHERE CONT.job_card_trn_fk = J.job_card_trn_pk)" + "         ELSE" + "          (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0))" + "             FROM JOB_TRN_CONT CONT" + "            WHERE CONT.job_card_trn_fk = J.job_card_trn_pk)" + "       END) WEIGHT_VOLUME,"
                    ;
                //SQL &= "       FETCH_FREIGHT_EXPSEA(j.job_card_trn_pk) FREIGHT," & vbNewLine
                //SQL &= "       FETCH_FREIGHT_EXPSEA_NEW(j.job_card_trn_pk, " & HttpContext.Current.Session("CURRENCY_MST_PK") & ") FREIGHT,"
                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4)) FREIGHT, ";

                //SQL &= "       FETCH_JOB_CARD_SEA_EXP_ACTREV(j.job_card_trn_pk," & HttpContext.Current.Session("currency_mst_pk") & " ) ACTUAL_REVENUE, " & vbNewLine
                //SQL &= "       NVL(FETCH_ACT_COST_EXPSEA(j.job_card_trn_pk),0) ACTUAL_COST, " & vbNewLine
                //SQL &= "       FETCH_EST_COST_EXPSEA(j.job_card_trn_pk) ESTIMATED_REVENUE, " & vbNewLine

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";

                SQL += "       EMP.EMPLOYEE_NAME SALES_REPORTER" + "  from JOB_CARD_TRN J," + "       BOOKING_MST_TBL      BOOK," + "       CUSTOMER_MST_TBL     SHIPPER," + "       CUSTOMER_MST_TBL     CONSIGNEE," + "       PORT_MST_TBL         POL," + "       PORT_MST_TBL         POD," + "       EMPLOYEE_MST_TBL     EMP"
                    ;
                //If LocFK <> 0 Then ' 'Added by purnanand for PTS MIS-APR-001 for all report type
                SQL = SQL + "       ,USER_MST_TBL         USR ,location_mst_tbl l"
                    ;
                //End If
                SQL = SQL + " WHERE J.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)" + "   AND J.BOOKING_MST_FK = BOOK.BOOKING_MST_PK" + "   AND J.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)" + "   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK" + "   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK" + "   AND POL.location_mst_fk = l.location_mst_pk and l.location_mst_pk=usr.default_location_fk " + "   AND SHIPPER.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+) "
                    ;
                //adding by thiyagarajan on 8/11/08
                SQL += "   AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))";
                //If LocFK <> 0 Then
                SQL = SQL + "   AND j.created_by_fk = usr.user_mst_pk ";
                //End If

                if (Report == 4)
                {
                    SQL += " and J.PYMT_TYPE=1";
                }
                else if (Report == 5)
                {
                    SQL += " and J.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and J.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SHIPPER.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POD_FK=" + podpk;
                }

                //Air-Sea Import
            }
            else if (reportType == 8)
            {
                //Air Import
                SQL = deprtmnt + " TO_CHAR(Jc.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH," + " TO_CHAR(Jc.JOBCARD_DATE, dateformat) JOBCARD_DATE," + " Jc.JOBCARD_REF_NO REF_NUMBER," + deprtmnts + " SH.CUSTOMER_NAME SHIPPER_NAME," + " CO.CUSTOMER_NAME CONSIGNEE_NAME," + " DECODE(Jc.PYMT_TYPE, 1, 'Y', 2, 'N') PAYMENT_TYPE," + " POL.PORT_ID POL," + " POD.PORT_ID POD," + " (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0))" + "    FROM JOB_TRN_CONT CONT" + "   WHERE CONT.job_card_trn_fk = Jc.job_card_trn_pk) WEIGHT_VOLUME,"
                    ;

                //SQL &= " FETCH_EST_COST_IMPair(jc.job_card_trn_pk) ESTIMATED_REVENUE," & vbNewLine
                //SQL &= " FETCH_JOB_CARD_AIR_IMP_ACTREV(jc.job_card_trn_pk," & HttpContext.Current.Session("currency_mst_pk") & " ) ACTUAL_REVENUE," & vbNewLine
                //SQL &= " NVL(FETCH_ACT_COST_IMPAIR(jc.job_card_trn_pk), 0) ACTUAL_COST," & vbNewLine & _

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4)) FREIGHT, " + " /*FETCH_FREIGHT_IMPAIR(jc.job_card_trn_pk)  FETCH_FREIGHT_IMPAIR_NEW(jc.job_card_trn_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + " ) FREIGHT,*/" + "  EMP.EMPLOYEE_NAME SALES_REPORTER  " + "  from JOB_CARD_TRN JC," + "  CUSTOMER_MST_TBL     SH," + "  CUSTOMER_MST_TBL     CO," + "  PORT_MST_TBL         POL," + "  PORT_MST_TBL         POD," + "  AGENT_MST_TBL        POLA," + "  USER_MST_TBL         UMT,location_mst_tbl l, " + "  EMPLOYEE_MST_TBL     EMP" + "  where JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)" + "  AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)" + "  AND JC.PORT_MST_POL_FK = POL.PORT_MST_PK" + "  AND JC.PORT_MST_POD_FK = POD.PORT_MST_PK"
                    ;
                SQL += "  AND JC.POL_AGENT_MST_FK = POLA.AGENT_MST_PK(+)";
                //SQL &= " AND (JC.PORT_MST_POD_FK IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK = " & LocFK & ") and JC.JC_AUTO_MANUAL = 1)" & vbNewLine
                //SQL &= " AND jc.CREATED_BY_FK = UMT.USER_MST_PK" & vbNewLine
                SQL += " AND  jc.CREATED_BY_FK = UMT.USER_MST_PK   "
                    ;
                SQL += "  AND ((UMT.DEFAULT_LOCATION_FK = l.location_mst_pk and JC.JC_AUTO_MANUAL = 0) OR (JC.PORT_MST_POD_FK  ";
                SQL += "  IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK= l.location_mst_pk)  and JC.JC_AUTO_MANUAL = 1))  ";

                SQL += "  AND SH.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)"
                    ;
                SQL += " AND SH.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)"
                    ;
                //SQL &=    " AND POL.location_mst_fk ='" & Loc & "'" & vbNewLine
                SQL += " AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS = 1)"
                    ;
                SQL += " AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))"
                    ;
                SQL += "  and l.location_mst_pk = umt.default_location_fk ";

                if (Report == 4)
                {
                    SQL += " and JC.PYMT_TYPE=1";
                }
                else if (Report == 5)
                {
                    SQL += " and JC.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and JC.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SH.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POD_FK=" + podpk;
                }
                ///'''''''''''''''''
                SQL += " UNION ";
                ///''''''''''''''''''
                //Sea Import

                SQL += deprtmnt + "TO_CHAR(Jc.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH," + "TO_CHAR(Jc.JOBCARD_DATE, dateformat) JOBCARD_DATE," + "Jc.JOBCARD_REF_NO REF_NUMBER," + deprtmnts + "sh.CUSTOMER_NAME SHIPPER_NAME," + "co.CUSTOMER_NAME CONSIGNEE_NAME," + "DECODE(Jc.PYMT_TYPE, 1, 'PPD', 2, 'CLT') PAYMENT_TYPE," + "POL.PORT_ID POL," + "POD.PORT_ID POD," + "(CASE WHEN jc.CARGO_TYPE = 1 THEN (SELECT SUM(NVL(CONT.NET_WEIGHT, 0)) FROM JOB_TRN_CONT CONT WHERE CONT.job_card_trn_fk = Jc.job_card_trn_pk) ELSE (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0)) FROM JOB_TRN_CONT CONT WHERE CONT.job_card_trn_fk = Jc.job_card_trn_pk) END) WEIGHT_VOLUME,"
                    ;
                //SQL &= "       FETCH_EST_COST_IMPSEA(jc.job_card_trn_pk) ESTIMATED_REVENUE," & vbNewLine
                //SQL &= "       FETCH_JOB_CARD_SEA_IMP_ACTREV(jc.job_card_trn_pk," & HttpContext.Current.Session("currency_mst_pk") & " ) ACTUAL_REVENUE, " & vbNewLine
                //sql &=   "       NVL(FETCH_ACT_COST_IMPSEA(jc.job_card_trn_pk),0) ACTUAL_COST, " & vbNewLine & _

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4)) FREIGHT, " + "       /*FETCH_FREIGHT_IMPSEA(jc.job_card_trn_pk) FETCH_FREIGHT_IMPSEA_NEW(jc.job_card_trn_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ") FREIGHT,*/ " + "       EMP.EMPLOYEE_NAME SALES_REPORTER " + "       from JOB_CARD_TRN jC," + "       /*JOB_TRN_CONT cont,*/" + "       CUSTOMER_MST_TBL     SH," + "       CUSTOMER_MST_TBL     CO," + "       PORT_MST_TBL         POL," + "       PORT_MST_TBL         POD," + "      AGENT_MST_TBL        POLA," + "       EMPLOYEE_MST_TBL     EMP,location_mst_tbl l," + "       USER_MST_TBL UMT" + "where JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)" + " /* AND jc.job_card_trn_pk = cont.job_card_trn_fk(+) */" + "AND sh.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)" + "AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)" + "AND JC.PORT_MST_POL_FK = POL.PORT_MST_PK" + "AND JC.PORT_MST_POD_FK = POD.PORT_MST_PK" + "AND JC.POL_AGENT_MST_FK = POLA.AGENT_MST_PK(+)"
                    ;
                // SQL &= "AND (JC.PORT_MST_POD_FK IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK =" & LocFK & ") and JC.JC_AUTO_MANUAL = 1)" & vbNewLine
                // SQL &= "AND jc.CREATED_BY_FK = UMT.USER_MST_PK" & vbNewLine & _
                SQL += " AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS = 1)"
                    ;
                //sql &=  "AND POL.location_mst_fk ='" & Loc & "'" & vbNewLine & _
                //sql &=  " AND JC.CARGO_TYPE = 1" & vbNewLine & _
                //sql &=  " AND POL.location_mst_fk ='" & Loc & "'" & vbNewLine & _
                //sql &=  "  AND JC.CARGO_TYPE = 1"
                SQL += "  AND jc.CREATED_BY_FK = UMT.USER_MST_PK and l.location_mst_pk=umt.default_location_fk "
                    ;
                SQL += "  AND ((UMT.DEFAULT_LOCATION_FK = l.location_mst_pk and JC.JC_AUTO_MANUAL = 0) OR (JC.PORT_MST_POD_FK  ";
                SQL += "  IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK= l.location_mst_pk)  and JC.JC_AUTO_MANUAL = 1)) ";

                SQL += "  AND SH.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)"
                    ;
                SQL += " AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))"
                    ;

                if (Report == 4)
                {
                    SQL += " and JC.PYMT_TYPE=1";
                }
                else if (Report == 5)
                {
                    SQL += " and JC.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and JC.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SH.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POD_FK=" + podpk;
                }
                //Air-Sea Export-Import
            }
            else if (reportType == 9)
            {
                //Air Export
                SQL = deprtmnt + "       TO_CHAR(J.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH," + "       TO_CHAR(J.JOBCARD_DATE, dateformat) JOBCARD_DATE," + "       J.JOBCARD_REF_NO REF_NUMBER," + deprtmnts + "       SHIPPER.CUSTOMER_NAME SHIPPER_NAME," + "       CONSIGNEE.CUSTOMER_NAME CONSIGNEE_NAME," + "       DECODE(J.PYMT_TYPE, 1, 'Y', 2, 'N') PAYMENT_TYPE," + "       POL.PORT_ID POL," + "       POD.PORT_ID POD," + "       (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0))" + "             FROM JOB_TRN_CONT CONT" + "            WHERE CONT.job_card_trn_fk = J.job_card_trn_pk" + "       ) WEIGHT_VOLUME,"
                    ;
                //SQL &= "       FETCH_EST_COST_EXPair(j.job_card_trn_pk) ESTIMATED_REVENUE," & vbNewLine
                //SQL &= "       FETCH_JOB_CARD_AIR_EXP_ACTREV(j.job_card_trn_pk," & HttpContext.Current.Session("currency_mst_pk") & " )  ACTUAL_REVENUE, " & vbNewLine
                //SQL &= "       NVL(FETCH_ACT_COST_EXPAIR(j.job_card_trn_pk),0) ACTUAL_COST, " & vbNewLine & _

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4)) FREIGHT, " + "       /*FETCH_FREIGHT_EXPAIR(j.job_card_trn_pk)  FETCH_EST_COST_EXPAIR_NEW(j.job_card_trn_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + " )  FREIGHT,*/ " + "       EMP.EMPLOYEE_NAME SALES_REPORTER" + "  from JOB_CARD_TRN J," + "       BOOKING_MST_TBL      BOOK," + "       CUSTOMER_MST_TBL     SHIPPER," + "       CUSTOMER_MST_TBL     CONSIGNEE," + "       PORT_MST_TBL         POL," + "       PORT_MST_TBL         POD," + "       EMPLOYEE_MST_TBL     EMP"
                    ;
                //If LocFK <> 0 Then
                SQL = SQL + "       ,USER_MST_TBL  USR,location_mst_tbl l  "
                    ;
                //End If
                SQL = SQL + " WHERE J.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)" + "   AND J.BOOKING_MST_FK = BOOK.BOOKING_MST_PK" + "   AND J.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)" + "   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK" + "   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK" + "   AND POL.location_mst_fk = l.location_mst_pk  and l.location_mst_pk=usr.default_location_fk " + "   AND SHIPPER.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+) " + "   AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))"
                    ;
                //adding by thiyagarajan on 8/11/08
                //If LocFK <> 0 Then
                //    SQL &= " l.location_mst_pk=" & LocFK
                //End If
                SQL = SQL + " AND j.created_by_fk = usr.user_mst_pk ";
                //If pageload = 0 Then
                //    SQL &= " and 1=2 "
                //End If
                if (Report == 4)
                {
                    SQL += " and J.PYMT_TYPE=1";
                }
                else if (Report == 5)
                {
                    SQL += " and J.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and J.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SHIPPER.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POD_FK=" + podpk;
                }
                ///
                SQL += " UNION ";
                //'
                //Air Import
                SQL += deprtmnt + " TO_CHAR(Jc.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH," + " TO_CHAR(Jc.JOBCARD_DATE, dateformat) JOBCARD_DATE," + " Jc.JOBCARD_REF_NO REF_NUMBER," + deprtmnts + " SH.CUSTOMER_NAME SHIPPER_NAME," + " CO.CUSTOMER_NAME CONSIGNEE_NAME," + " DECODE(Jc.PYMT_TYPE, 1, 'Y', 2, 'N') PAYMENT_TYPE," + " POL.PORT_ID POL," + " POD.PORT_ID POD," + " (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0))" + "    FROM JOB_TRN_CONT CONT" + "   WHERE CONT.job_card_trn_fk = Jc.job_card_trn_pk) WEIGHT_VOLUME,";

                //SQL &= " FETCH_EST_COST_IMPair(jc.job_card_trn_pk) ESTIMATED_REVENUE," & vbNewLine
                //SQL &= " FETCH_JOB_CARD_AIR_IMP_ACTREV(jc.job_card_trn_pk," & HttpContext.Current.Session("currency_mst_pk") & " ) ACTUAL_REVENUE," & vbNewLine
                //SQL &= " NVL(FETCH_ACT_COST_IMPAIR(jc.job_card_trn_pk), 0) ACTUAL_COST," & vbNewLine & _

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4)) FREIGHT, " + " /*FETCH_FREIGHT_IMPAIR(jc.job_card_trn_pk)  FETCH_FREIGHT_IMPAIR_NEW(jc.job_card_trn_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + " ) FREIGHT,*/" + "  EMP.EMPLOYEE_NAME SALES_REPORTER  " + "  from JOB_CARD_TRN JC," + "  CUSTOMER_MST_TBL     SH," + "  CUSTOMER_MST_TBL     CO," + "  PORT_MST_TBL         POL," + "  PORT_MST_TBL         POD," + "  AGENT_MST_TBL        POLA," + "  USER_MST_TBL         UMT,location_mst_tbl l, " + "  EMPLOYEE_MST_TBL     EMP" + "  where JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)" + "  AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)" + "  AND JC.PORT_MST_POL_FK = POL.PORT_MST_PK" + "  AND JC.PORT_MST_POD_FK = POD.PORT_MST_PK";
                SQL += "  AND JC.POL_AGENT_MST_FK = POLA.AGENT_MST_PK(+)";
                //SQL &= " AND (JC.PORT_MST_POD_FK IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK = " & LocFK & ") and JC.JC_AUTO_MANUAL = 1)" & vbNewLine
                //SQL &= " AND jc.CREATED_BY_FK = UMT.USER_MST_PK" & vbNewLine
                SQL += " AND  jc.CREATED_BY_FK = UMT.USER_MST_PK   ";
                SQL += "  AND ((UMT.DEFAULT_LOCATION_FK = l.location_mst_pk and JC.JC_AUTO_MANUAL = 0) OR (JC.PORT_MST_POD_FK  ";
                SQL += "  IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK= l.location_mst_pk)  and JC.JC_AUTO_MANUAL = 1))  ";

                SQL += "  AND SH.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)";
                SQL += " AND SH.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)";
                //SQL &=    " AND POL.location_mst_fk ='" & Loc & "'" & vbNewLine
                SQL += " AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS = 1)";
                SQL += " AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))";
                SQL += "  and l.location_mst_pk = umt.default_location_fk ";

                if (Report == 4)
                {
                    SQL += " and JC.PYMT_TYPE=1";
                }
                else if (Report == 5)
                {
                    SQL += " and JC.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and JC.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SH.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POD_FK=" + podpk;
                }
                ///''''''''''''
                SQL += " UNION ";

                ///''''''''''''
                // Sea Export
                SQL += deprtmnt + "       TO_CHAR(J.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH," + "       TO_CHAR(J.JOBCARD_DATE, dateformat) JOBCARD_DATE," + "       J.JOBCARD_REF_NO REF_NUMBER," + deprtmnts + "       SHIPPER.CUSTOMER_NAME SHIPPER_NAME," + "       CONSIGNEE.CUSTOMER_NAME CONSIGNEE_NAME," + "       DECODE(J.PYMT_TYPE, 1, 'PPD', 2, 'CLT') PAYMENT_TYPE," + "       POL.PORT_ID POL," + "       POD.PORT_ID POD," + "       (CASE" + "         WHEN BOOK.CARGO_TYPE = 1 THEN" + "          (SELECT SUM(NVL(CONT.NET_WEIGHT, 0))" + "             FROM JOB_TRN_CONT CONT" + "            WHERE CONT.job_card_trn_fk = J.job_card_trn_pk)" + "         ELSE" + "          (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0))" + "             FROM JOB_TRN_CONT CONT" + "            WHERE CONT.job_card_trn_fk = J.job_card_trn_pk)" + "       END) WEIGHT_VOLUME,";
                //SQL &= "       FETCH_FREIGHT_EXPSEA(j.job_card_trn_pk) FREIGHT," & vbNewLine
                //SQL &= "       FETCH_FREIGHT_EXPSEA_NEW(j.job_card_trn_pk, " & HttpContext.Current.Session("CURRENCY_MST_PK") & ") FREIGHT,"
                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4)) FREIGHT, ";

                //SQL &= "       FETCH_JOB_CARD_SEA_EXP_ACTREV(j.job_card_trn_pk," & HttpContext.Current.Session("currency_mst_pk") & " ) ACTUAL_REVENUE, " & vbNewLine
                //SQL &= "       NVL(FETCH_ACT_COST_EXPSEA(j.job_card_trn_pk),0) ACTUAL_COST, " & vbNewLine
                //SQL &= "       FETCH_EST_COST_EXPSEA(j.job_card_trn_pk) ESTIMATED_REVENUE, " & vbNewLine

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";

                SQL += "       EMP.EMPLOYEE_NAME SALES_REPORTER" + "  from JOB_CARD_TRN J," + "       BOOKING_MST_TBL      BOOK," + "       CUSTOMER_MST_TBL     SHIPPER," + "       CUSTOMER_MST_TBL     CONSIGNEE," + "       PORT_MST_TBL         POL," + "       PORT_MST_TBL         POD," + "       EMPLOYEE_MST_TBL     EMP";
                //If LocFK <> 0 Then ' 'Added by purnanand for PTS MIS-APR-001 for all report type
                SQL = SQL + "       ,USER_MST_TBL         USR ,location_mst_tbl l";
                //End If
                SQL = SQL + " WHERE J.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)" + "   AND J.BOOKING_MST_FK = BOOK.BOOKING_MST_PK" + "   AND J.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)" + "   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK" + "   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK" + "   AND POL.location_mst_fk = l.location_mst_pk and l.location_mst_pk=usr.default_location_fk " + "   AND SHIPPER.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+) ";
                //adding by thiyagarajan on 8/11/08
                SQL += "   AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))";
                //If LocFK <> 0 Then
                SQL = SQL + "   AND j.created_by_fk = usr.user_mst_pk ";
                //End If

                if (Report == 4)
                {
                    SQL += " and J.PYMT_TYPE=1";
                }
                else if (Report == 5)
                {
                    SQL += " and J.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and J.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SHIPPER.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POD_FK=" + podpk;
                }

                ///''''''''''
                SQL += " UNION ";
                ///'''''''''''''
                //Sea Import
                SQL += deprtmnt + "TO_CHAR(Jc.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH," + "TO_CHAR(Jc.JOBCARD_DATE, dateformat) JOBCARD_DATE," + "Jc.JOBCARD_REF_NO REF_NUMBER," + deprtmnts + "sh.CUSTOMER_NAME SHIPPER_NAME," + "co.CUSTOMER_NAME CONSIGNEE_NAME," + "DECODE(Jc.PYMT_TYPE, 1, 'PPD', 2, 'CLT') PAYMENT_TYPE," + "POL.PORT_ID POL," + "POD.PORT_ID POD," + "(CASE WHEN jc.CARGO_TYPE = 1 THEN (SELECT SUM(NVL(CONT.NET_WEIGHT, 0)) FROM JOB_TRN_CONT CONT WHERE CONT.job_card_trn_fk = Jc.job_card_trn_pk) ELSE (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0)) FROM JOB_TRN_CONT CONT WHERE CONT.job_card_trn_fk = Jc.job_card_trn_pk) END) WEIGHT_VOLUME,";
                //SQL &= "       FETCH_EST_COST_IMPSEA(jc.job_card_trn_pk) ESTIMATED_REVENUE," & vbNewLine
                //SQL &= "       FETCH_JOB_CARD_SEA_IMP_ACTREV(jc.job_card_trn_pk," & HttpContext.Current.Session("currency_mst_pk") & " ) ACTUAL_REVENUE, " & vbNewLine
                //sql &=   "       NVL(FETCH_ACT_COST_IMPSEA(jc.job_card_trn_pk),0) ACTUAL_COST, " & vbNewLine & _

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4)) FREIGHT, " + "       /*FETCH_FREIGHT_IMPSEA(jc.job_card_trn_pk) FETCH_FREIGHT_IMPSEA_NEW(jc.job_card_trn_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ") FREIGHT,*/ " + "       EMP.EMPLOYEE_NAME SALES_REPORTER " + "       from JOB_CARD_TRN jC," + "       /*JOB_TRN_CONT cont,*/" + "       CUSTOMER_MST_TBL     SH," + "       CUSTOMER_MST_TBL     CO," + "       PORT_MST_TBL         POL," + "       PORT_MST_TBL         POD," + "      AGENT_MST_TBL        POLA," + "       EMPLOYEE_MST_TBL     EMP,location_mst_tbl l," + "       USER_MST_TBL UMT" + "where JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)" + " /* AND jc.job_card_trn_pk = cont.job_card_trn_fk(+) */" + "AND sh.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)" + "AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)" + "AND JC.PORT_MST_POL_FK = POL.PORT_MST_PK" + "AND JC.PORT_MST_POD_FK = POD.PORT_MST_PK" + "AND JC.POL_AGENT_MST_FK = POLA.AGENT_MST_PK(+)";
                // SQL &= "AND (JC.PORT_MST_POD_FK IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK =" & LocFK & ") and JC.JC_AUTO_MANUAL = 1)" & vbNewLine
                // SQL &= "AND jc.CREATED_BY_FK = UMT.USER_MST_PK" & vbNewLine & _
                SQL += " AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS = 1)";
                //sql &=  "AND POL.location_mst_fk ='" & Loc & "'" & vbNewLine & _
                //sql &=  " AND JC.CARGO_TYPE = 1" & vbNewLine & _
                //sql &=  " AND POL.location_mst_fk ='" & Loc & "'" & vbNewLine & _
                //sql &=  "  AND JC.CARGO_TYPE = 1"
                SQL += "  AND jc.CREATED_BY_FK = UMT.USER_MST_PK and l.location_mst_pk=umt.default_location_fk ";
                SQL += "  AND ((UMT.DEFAULT_LOCATION_FK = l.location_mst_pk and JC.JC_AUTO_MANUAL = 0) OR (JC.PORT_MST_POD_FK  ";
                SQL += "  IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK= l.location_mst_pk)  and JC.JC_AUTO_MANUAL = 1)) ";

                SQL += "  AND SH.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)";
                SQL += " AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))";

                if (Report == 4)
                {
                    SQL += " and JC.PYMT_TYPE=1";
                }
                else if (Report == 5)
                {
                    SQL += " and JC.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and JC.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SH.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POD_FK=" + podpk;
                }
            }
            try
            {
                //Return objWF.GetDataSet(SQL & strCondition)

                return objWF.GetDataSet(SQL);
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

        public DataSet GetSalesReportNew(Int32 pageload, string polpk, string podpk, string emppk, string custpk, int reportType, string Loc, string LocFK, string fromDate, string toDate,
        int isPPD = 0, int isSPL = 0, Int32 Report = 2, string Process = "")
        {
            WorkFlow objWF = new WorkFlow();
            string SQL = null;
            string deprtmnt = null;
            string deprtmnts = null;
            string strCondition = " ";

            //commented by thiyagarajan on 8/11/08

            //If Not ((fromDate Is Nothing Or fromDate = "") And (toDate Is Nothing Or toDate = "")) Then
            //    strCondition = " AND J.JOBCARD_DATE BETWEEN TO_DATE('" & fromDate & "',dateformat)  AND TO_DATE('" & toDate & "',dateformat)  "
            //ElseIf Not (fromDate Is Nothing Or fromDate = "") Then
            //    strCondition = " AND J.JOBCARD_DATE >= TO_DATE('" & fromDate & "',dateformat) "
            //ElseIf Not (toDate Is Nothing Or toDate = "") Then
            //    strCondition = " AND J.JOBCARD_DATE >= TO_DATE('" & toDate & "',dateformat) "
            //End If
            //end

            //If LocFK <> 0 Then  'Added by purnanand for PTS MIS-APR-001
            //    strCondition = "AND usr.default_location_fk = '" & LocFK & " ' "
            //End If

            if (isPPD == 1 | isPPD == 2)
            {
                strCondition = strCondition + " AND J.PYMT_TYPE = " + isPPD;
            }
            strCondition = strCondition + " ORDER BY J.JOBCARD_DATE DESC ";
            //If Loc <> Nothing Or Loc <> "" Then     'Added By Prakash Chandra on 22/4/2008
            //    strCondition = strCondition & "  POL.PORT_ID ='" & Loc & " '"
            //End If
            if (isSPL == 1)
            {
                strCondition = strCondition + " , SALES_REPORTER";
            }

            //adding by thiyagarajan on 1/8/08 for introducing export to excel
            if (Report == 2 | Report == 3)
            {
                deprtmnt = "select  ";
                deprtmnts = " '" + Process + "' AS DEPARTMENT, ";
            }
            else
            {
                deprtmnt = "select '" + Process + "' AS DEPARTMENT,";
                deprtmnts = " ";
            }
            //end

            //add by latha
            //modified by thiyagarajan on 1/8/08 for introducing export to excel
            //modified by thiyagarajan on 16/12/08 for local currency task
            // SeaExport
            if (reportType == 1)
            {
                SQL = deprtmnt + "       TO_CHAR(J.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH," + "       TO_CHAR(J.JOBCARD_DATE, dateformat) JOBCARD_DATE," + "       J.JOBCARD_REF_NO REF_NUMBER," + deprtmnts + "       SHIPPER.CUSTOMER_NAME SHIPPER_NAME," + "       CONSIGNEE.CUSTOMER_NAME CONSIGNEE_NAME," + "       DECODE(J.PYMT_TYPE, 1, 'PPD', 2, 'CLT') PAYMENT_TYPE," + "       POL.PORT_ID POL," + "       POD.PORT_ID POD," + "        (SELECT SUM(NVL(CTMT.TEU_FACTOR, 0))" + "           FROM JOB_TRN_CONT  CONT, CONTAINER_TYPE_MST_TBL CTMT" + "       WHERE CONT.JOB_CARD_TRN_FK = J.JOB_CARD_TRN_PK " + "        AND CONT.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK(+)) TEU, " + "       (CASE" + "         WHEN BOOK.CARGO_TYPE = 1 THEN" + "          (SELECT SUM(NVL(CONT.NET_WEIGHT, 0))" + "             FROM JOB_TRN_CONT CONT" + "            WHERE CONT.JOB_CARD_TRN_FK = J.JOB_CARD_TRN_PK)" + "         ELSE" + "          (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0))" + "             FROM JOB_TRN_CONT CONT" + "            WHERE CONT.JOB_CARD_TRN_FK = J.JOB_CARD_TRN_PK)" + "       END) WEIGHT_VOLUME,";
                //SQL &= "       FETCH_FREIGHT_EXPSEA(j.JOB_CARD_TRN_PK) FREIGHT," & vbNewLine
                //SQL &= "       FETCH_FREIGHT_EXPSEA_NEW(j.JOB_CARD_TRN_PK, " & HttpContext.Current.Session("CURRENCY_MST_PK") & ") FREIGHT,"
                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4)) FREIGHT, ";

                //SQL &= "       FETCH_JOB_CARD_SEA_EXP_ACTREV(j.JOB_CARD_TRN_PK," & HttpContext.Current.Session("currency_mst_pk") & " ) ACTUAL_REVENUE, " & vbNewLine
                //SQL &= "       NVL(FETCH_ACT_COST_EXPSEA(j.JOB_CARD_TRN_PK),0) ACTUAL_COST, " & vbNewLine
                //SQL &= "       FETCH_EST_COST_EXPSEA(j.JOB_CARD_TRN_PK) ESTIMATED_REVENUE, " & vbNewLine

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";

                SQL += "       NVL(EMP.EMPLOYEE_NAME,'CSR') SALES_REPORTER" + "  from job_card_trn J," + "       BOOKING_MST_TBL      BOOK," + "       CUSTOMER_MST_TBL     SHIPPER," + "       CUSTOMER_MST_TBL     CONSIGNEE," + "       PORT_MST_TBL         POL," + "       PORT_MST_TBL         POD," + "       EMPLOYEE_MST_TBL     EMP";
                //If LocFK <> 0 Then ' 'Added by purnanand for PTS MIS-APR-001 for all report type
                SQL = SQL + "       ,USER_MST_TBL         USR ,location_mst_tbl l";
                //End If
                SQL = SQL + " WHERE J.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)" + "   AND J.BOOKING_MST_FK = BOOK.BOOKING_MST_PK" + "   AND J.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)" + "   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK" + "   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK" + "   AND POL.location_mst_fk = l.location_mst_pk and l.location_mst_pk=usr.default_location_fk " + "   AND SHIPPER.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+) ";
                //adding by thiyagarajan on 8/11/08
                SQL += "   AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))";
                //If LocFK <> 0 Then
                SQL = SQL + "   AND j.created_by_fk = usr.user_mst_pk ";
                //End If

                if (Report == 4)
                {
                    SQL += " and J.PYMT_TYPE=1";
                }
                else if (Report == 5)
                {
                    SQL += " and J.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and J.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SHIPPER.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POD_FK=" + podpk;
                }
                //adding by thiyagarajan on 1/8/08 to display the values by order
                SQL = SQL + " order by J.JOBCARD_DATE DESC , J.JOBCARD_REF_NO DESC";
                //end
                //Sea Import
            }
            else if (reportType == 2)
            {
                //LocFK = HttpContext.Current.Session("loged_in_loc_fk")
                SQL = deprtmnt + "TO_CHAR(Jc.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH," + "TO_CHAR(Jc.JOBCARD_DATE, dateformat) JOBCARD_DATE," + "Jc.JOBCARD_REF_NO REF_NUMBER," + deprtmnts + "sh.CUSTOMER_NAME SHIPPER_NAME," + "co.CUSTOMER_NAME CONSIGNEE_NAME," + "DECODE(Jc.PYMT_TYPE, 1, 'PPD', 2, 'CLT') PAYMENT_TYPE," + "POL.PORT_ID POL," + "POD.PORT_ID POD," + "(SELECT SUM(NVL(CTMT.TEU_FACTOR, 0))" + "FROM JOB_TRN_CONT  CONT, CONTAINER_TYPE_MST_TBL CTMT" + "WHERE CONT.JOB_CARD_sea_imp_FK = jC.job_card_trn_pk " + "AND CONT.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK(+)) TEU, " + "(CASE WHEN jc.CARGO_TYPE = 1 THEN (SELECT SUM(NVL(CONT.NET_WEIGHT, 0)) FROM JOB_TRN_CONT CONT WHERE CONT.JOB_CARD_SEA_IMP_FK = Jc.job_card_trn_pk) ELSE (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0)) FROM JOB_TRN_CONT CONT WHERE CONT.JOB_CARD_SEA_IMP_FK = Jc.job_card_trn_pk) END) WEIGHT_VOLUME,";
                //SQL &= "       FETCH_EST_COST_IMPSEA(jc.job_card_trn_pk) ESTIMATED_REVENUE," & vbNewLine
                //SQL &= "       FETCH_JOB_CARD_SEA_IMP_ACTREV(jc.job_card_trn_pk," & HttpContext.Current.Session("currency_mst_pk") & " ) ACTUAL_REVENUE, " & vbNewLine
                //sql &=   "       NVL(FETCH_ACT_COST_IMPSEA(jc.job_card_trn_pk),0) ACTUAL_COST, " & vbNewLine & _

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4)) FREIGHT, " + "       /*FETCH_FREIGHT_IMPSEA(jc.job_card_trn_pk) FETCH_FREIGHT_IMPSEA_NEW(jc.job_card_trn_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ") FREIGHT,*/ " + "      NVL(EMP.EMPLOYEE_NAME,'CSR') SALES_REPORTER " + "       from JOB_CARD_TRN jC," + "       /*JOB_TRN_CONT cont,*/" + "       CUSTOMER_MST_TBL     SH," + "       CUSTOMER_MST_TBL     CO," + "       PORT_MST_TBL         POL," + "       PORT_MST_TBL         POD," + "      AGENT_MST_TBL        POLA," + "       EMPLOYEE_MST_TBL     EMP,location_mst_tbl l," + "       USER_MST_TBL UMT" + "where JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)" + " /* AND jc.job_card_trn_pk = cont.job_card_sea_imp_fk(+) */" + "AND sh.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)" + "AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)" + "AND JC.PORT_MST_POL_FK = POL.PORT_MST_PK" + "AND JC.PORT_MST_POD_FK = POD.PORT_MST_PK" + "AND JC.POL_AGENT_MST_FK = POLA.AGENT_MST_PK(+)";
                // SQL &= "AND (JC.PORT_MST_POD_FK IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK =" & LocFK & ") and JC.JC_AUTO_MANUAL = 1)" & vbNewLine
                // SQL &= "AND jc.CREATED_BY_FK = UMT.USER_MST_PK" & vbNewLine & _
                SQL += " AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS = 1)";
                //sql &=  "AND POL.location_mst_fk ='" & Loc & "'" & vbNewLine & _
                //sql &=  " AND JC.CARGO_TYPE = 1" & vbNewLine & _
                //sql &=  " AND POL.location_mst_fk ='" & Loc & "'" & vbNewLine & _
                //sql &=  "  AND JC.CARGO_TYPE = 1"
                SQL += "  AND jc.CREATED_BY_FK = UMT.USER_MST_PK and l.location_mst_pk=umt.default_location_fk ";
                SQL += "  AND ((UMT.DEFAULT_LOCATION_FK = l.location_mst_pk and JC.JC_AUTO_MANUAL = 0) OR (JC.PORT_MST_POD_FK  ";
                SQL += "  IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK= l.location_mst_pk)  and JC.JC_AUTO_MANUAL = 1)) ";

                SQL += "  AND SH.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)";
                SQL += " AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))";

                if (Report == 4)
                {
                    SQL += " and JC.PYMT_TYPE=1";
                }
                else if (Report == 5)
                {
                    SQL += " and JC.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and JC.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SH.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POD_FK=" + podpk;
                }
                SQL += " ORDER BY JOBCARD_DATE DESC, JOBCARD_REF_NO DESC";
                //SQL = "select 'SEAIMP' AS DEPARTMENT," & vbNewLine & _
                //       "       TO_CHAR(J.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH," & vbNewLine & _
                //      "       TO_CHAR(J.JOBCARD_DATE, dateformat) JOBCARD_DATE," & vbNewLine & _
                //     "       J.JOBCARD_REF_NO REF_NUMBER," & vbNewLine & _
                //    "       SHIPPER.CUSTOMER_NAME SHIPPER_NAME," & vbNewLine & _
                //   "       CONSIGNEE.CUSTOMER_NAME CONSIGNEE_NAME," & vbNewLine & _
                //  "       DECODE(J.PYMT_TYPE, 1, 'Y', 2, 'N') PAYMENT_TYPE," & vbNewLine & _
                // "       POL.PORT_ID POL," & vbNewLine & _
                //        "       POD.PORT_ID POD," & vbNewLine & _
                //       "       (CASE" & vbNewLine & _
                //      "         WHEN J.CARGO_TYPE = 1 THEN" & vbNewLine & _
                //     "          (SELECT SUM(NVL(CONT.NET_WEIGHT, 0))" & vbNewLine & _
                //    "             FROM JOB_TRN_CONT CONT" & vbNewLine & _
                //   "            WHERE CONT.JOB_CARD_SEA_IMP_FK = J.job_card_trn_pk)" & vbNewLine & _
                //  "         ELSE" & vbNewLine & _
                // "          (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0))" & vbNewLine & _
                //"             FROM JOB_TRN_CONT CONT" & vbNewLine & _
                //"            WHERE CONT.JOB_CARD_SEA_IMP_FK = J.job_card_trn_pk)" & vbNewLine & _
                //      "       END) WEIGHT_VOLUME," & vbNewLine & _
                //     "       FETCH_EST_COST_IMPSEA(j.job_card_trn_pk) ESTIMATED_REVENUE," & vbNewLine & _
                //    "       FETCH_JOB_CARD_SEA_IMP_ACTREV(j.job_card_trn_pk) ACTUAL_REVENUE, " & vbNewLine & _
                //   "       NVL(FETCH_ACT_COST_IMPSEA(j.job_card_trn_pk),0) ACTUAL_COST, " & vbNewLine & _
                //  "       FETCH_FREIGHT_IMPSEA(j.job_card_trn_pk) FREIGHT, " & vbNewLine & _
                // "       EMP.EMPLOYEE_NAME SALES_REPORTER" & vbNewLine & _
                //        "  from JOB_CARD_TRN J," & vbNewLine & _
                //       "       CUSTOMER_MST_TBL     SHIPPER," & vbNewLine & _
                //      "       CUSTOMER_MST_TBL     CONSIGNEE," & vbNewLine & _
                //     "       PORT_MST_TBL         POL," & vbNewLine & _
                //    "       PORT_MST_TBL         POD," & vbNewLine & _
                //   "       EMPLOYEE_MST_TBL     EMP" & vbNewLine
                //If LocFK <> 0 Then
                //SQL = SQL & "       ,USER_MST_TBL         USR " & vbNewLine
                //End If
                //SQL = SQL & " WHERE J.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)" & vbNewLine & _
                // "   and (j.PORT_MST_POD_FK " & vbNewLine & _
                //"   IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK= " & LocFK & ")  and j.JC_AUTO_MANUAL = 1)" & vbNewLine & _
                //"   AND J.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)" & vbNewLine & _
                //"   AND J.PORT_MST_POD_FK = POD.PORT_MST_PK" & vbNewLine & _
                //"   AND J.PORT_MST_POL_FK = POL.PORT_MST_PK" & vbNewLine & _
                //"   AND SHIPPER.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+) " & vbNewLine
                //If LocFK <> 0 Then
                //    SQL = SQL & "   AND j.created_by_fk = usr.user_mst_pk "
                //End If

                //Air Export
            }
            else if (reportType == 3)
            {
                SQL = deprtmnt + "       TO_CHAR(J.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH," + "       TO_CHAR(J.JOBCARD_DATE, dateformat) JOBCARD_DATE," + "       J.JOBCARD_REF_NO REF_NUMBER," + deprtmnts + "       SHIPPER.CUSTOMER_NAME SHIPPER_NAME," + "       CONSIGNEE.CUSTOMER_NAME CONSIGNEE_NAME," + "       DECODE(J.PYMT_TYPE, 1, 'Y', 2, 'N') PAYMENT_TYPE," + "       POL.PORT_ID POL," + "       POD.PORT_ID POD," + "       0 TEU," + "       (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0))" + "             FROM JOB_TRN_air_EXP_CONT CONT" + "            WHERE CONT.JOB_CARD_air_EXP_FK = J.JOB_CARD_air_EXP_PK" + "       ) WEIGHT_VOLUME,";
                //SQL &= "       FETCH_EST_COST_EXPair(j.job_card_air_exp_pk) ESTIMATED_REVENUE," & vbNewLine
                //SQL &= "       FETCH_JOB_CARD_AIR_EXP_ACTREV(j.job_card_air_exp_pk," & HttpContext.Current.Session("currency_mst_pk") & " )  ACTUAL_REVENUE, " & vbNewLine
                //SQL &= "       NVL(FETCH_ACT_COST_EXPAIR(j.job_card_air_exp_pk),0) ACTUAL_COST, " & vbNewLine & _

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.job_card_air_exp_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.job_card_air_exp_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.job_card_air_exp_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.job_card_air_exp_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4)) FREIGHT, " + "       /*FETCH_FREIGHT_EXPAIR(j.job_card_air_exp_pk)  FETCH_EST_COST_EXPAIR_NEW(j.job_card_air_exp_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + " )  FREIGHT,*/ " + "      NVL(EMP.EMPLOYEE_NAME,'CSR') SALES_REPORTER" + "  from job_card_air_exp_tbl J," + "       BOOKING_air_TBL      BOOK," + "       CUSTOMER_MST_TBL     SHIPPER," + "       CUSTOMER_MST_TBL     CONSIGNEE," + "       PORT_MST_TBL         POL," + "       PORT_MST_TBL         POD," + "       EMPLOYEE_MST_TBL     EMP";
                //If LocFK <> 0 Then
                SQL = SQL + "       ,USER_MST_TBL  USR,location_mst_tbl l  ";
                //End If
                SQL = SQL + " WHERE J.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)" + "   AND J.BOOKING_air_FK = BOOK.BOOKING_air_PK" + "   AND J.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)" + "   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK" + "   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK" + "   AND POL.location_mst_fk = l.location_mst_pk  and l.location_mst_pk=usr.default_location_fk " + "   AND SHIPPER.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+) " + "   AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))";
                //adding by thiyagarajan on 8/11/08
                //If LocFK <> 0 Then
                //    SQL &= " l.location_mst_pk=" & LocFK
                //End If
                SQL = SQL + " AND j.created_by_fk = usr.user_mst_pk ";
                //If pageload = 0 Then
                //    SQL &= " and 1=2 "
                //End If
                if (Report == 4)
                {
                    SQL += " and J.PYMT_TYPE=1";
                }
                else if (Report == 5)
                {
                    SQL += " and J.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and J.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SHIPPER.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POD_FK=" + podpk;
                }
                //adding by thiyagarajan on 1/8/08 to display the values by order
                SQL = SQL + " order by J.JOBCARD_DATE DESC , J.JOBCARD_REF_NO DESC";
                //end
                //Air Import
            }
            else if (reportType == 4)
            {
                //LocFK = HttpContext.Current.Session("loged_in_loc_fk")
                SQL = deprtmnt + " TO_CHAR(Jc.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH," + " TO_CHAR(Jc.JOBCARD_DATE, dateformat) JOBCARD_DATE," + " Jc.JOBCARD_REF_NO REF_NUMBER," + deprtmnts + " SH.CUSTOMER_NAME SHIPPER_NAME," + " CO.CUSTOMER_NAME CONSIGNEE_NAME," + " DECODE(Jc.PYMT_TYPE, 1, 'Y', 2, 'N') PAYMENT_TYPE," + " POL.PORT_ID POL," + " POD.PORT_ID POD," + "       0 TEU," + " (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0))" + "    FROM JOB_TRN_air_IMP_CONT CONT" + "   WHERE CONT.JOB_CARD_air_IMP_FK = Jc.job_card_trn_pk) WEIGHT_VOLUME,";

                //SQL &= " FETCH_EST_COST_IMPair(jc.job_card_trn_pk) ESTIMATED_REVENUE," & vbNewLine
                //SQL &= " FETCH_JOB_CARD_AIR_IMP_ACTREV(jc.job_card_trn_pk," & HttpContext.Current.Session("currency_mst_pk") & " ) ACTUAL_REVENUE," & vbNewLine
                //SQL &= " NVL(FETCH_ACT_COST_IMPAIR(jc.job_card_trn_pk), 0) ACTUAL_COST," & vbNewLine & _

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4)) FREIGHT, " + " /*FETCH_FREIGHT_IMPAIR(jc.job_card_trn_pk)  FETCH_FREIGHT_IMPAIR_NEW(jc.job_card_trn_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + " ) FREIGHT,*/" + "  NVL(EMP.EMPLOYEE_NAME,'CSR') SALES_REPORTER  " + "  from JOB_CARD_AIR_IMP_TBL JC," + "  CUSTOMER_MST_TBL     SH," + "  CUSTOMER_MST_TBL     CO," + "  PORT_MST_TBL         POL," + "  PORT_MST_TBL         POD," + "  AGENT_MST_TBL        POLA," + "  USER_MST_TBL         UMT,location_mst_tbl l, " + "  EMPLOYEE_MST_TBL     EMP" + "  where JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)" + "  AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)" + "  AND JC.PORT_MST_POL_FK = POL.PORT_MST_PK" + "  AND JC.PORT_MST_POD_FK = POD.PORT_MST_PK";
                SQL += "  AND JC.POL_AGENT_MST_FK = POLA.AGENT_MST_PK(+)";
                //SQL &= " AND (JC.PORT_MST_POD_FK IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK = " & LocFK & ") and JC.JC_AUTO_MANUAL = 1)" & vbNewLine
                //SQL &= " AND jc.CREATED_BY_FK = UMT.USER_MST_PK" & vbNewLine
                SQL += " AND  jc.CREATED_BY_FK = UMT.USER_MST_PK   ";
                SQL += "  AND ((UMT.DEFAULT_LOCATION_FK = l.location_mst_pk and JC.JC_AUTO_MANUAL = 0) OR (JC.PORT_MST_POD_FK  ";
                SQL += "  IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK= l.location_mst_pk)  and JC.JC_AUTO_MANUAL = 1))  ";

                SQL += "  AND SH.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)";
                SQL += " AND SH.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)";
                //SQL &=    " AND POL.location_mst_fk ='" & Loc & "'" & vbNewLine
                SQL += " AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS = 1)";
                SQL += " AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))";
                SQL += "  and l.location_mst_pk = umt.default_location_fk ";

                if (Report == 4)
                {
                    SQL += " and JC.PYMT_TYPE=1";
                }
                else if (Report == 5)
                {
                    SQL += " and JC.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and JC.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SH.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POD_FK=" + podpk;
                }

                SQL += " ORDER BY JOBCARD_DATE DESC, JOBCARD_REF_NO DESC";
                //adding by thiyagarajan on 8/11/08

                //SQL = "select 'AIRIMP' AS DEPARTMENT," & vbNewLine & _
                //       "       TO_CHAR(J.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH," & vbNewLine & _
                //      "       TO_CHAR(J.JOBCARD_DATE, dateformat) JOBCARD_DATE," & vbNewLine & _
                //     "       J.JOBCARD_REF_NO REF_NUMBER," & vbNewLine & _
                //    "       SHIPPER.CUSTOMER_NAME SHIPPER_NAME," & vbNewLine & _
                //        "       CONSIGNEE.CUSTOMER_NAME CONSIGNEE_NAME," & vbNewLine & _
                //       "       DECODE(J.PYMT_TYPE, 1, 'Y', 2, 'N') PAYMENT_TYPE," & vbNewLine & _
                //      "       POL.PORT_ID POL," & vbNewLine & _
                //     "       POD.PORT_ID POD," & vbNewLine & _
                //    "       (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0))" & vbNewLine & _
                //   "             FROM JOB_TRN_air_IMP_CONT CONT" & vbNewLine & _
                //  "            WHERE CONT.JOB_CARD_air_IMP_FK = J.job_card_trn_pk" & vbNewLine & _
                // "       ) WEIGHT_VOLUME," & vbNewLine & _
                // "       FETCH_EST_COST_IMPair(j.job_card_trn_pk) ESTIMATED_REVENUE," & vbNewLine & _
                //"       FETCH_JOB_CARD_AIR_IMP_ACTREV(j.job_card_trn_pk) ACTUAL_REVENUE, " & vbNewLine & _
                //"       NVL(FETCH_ACT_COST_IMPAIR(j.job_card_trn_pk),0) ACTUAL_COST, " & vbNewLine & _
                //"       FETCH_FREIGHT_IMPAIR(j.job_card_trn_pk) FREIGHT, " & vbNewLine & _
                //"       EMP.EMPLOYEE_NAME SALES_REPORTER" & vbNewLine & _
                //"  from job_card_air_IMP_tbl J," & vbNewLine & _
                //"       CUSTOMER_MST_TBL     SHIPPER," & vbNewLine & _
                //"       CUSTOMER_MST_TBL     CONSIGNEE," & vbNewLine & _
                //"       PORT_MST_TBL         POL," & vbNewLine & _
                //"       PORT_MST_TBL         POD," & vbNewLine & _
                //"       EMPLOYEE_MST_TBL     EMP" & vbNewLine
                //If LocFK <> 0 Then
                //SQL = SQL & "       ,USER_MST_TBL         USR " & vbNewLine
                //End If
                //SQL = SQL & " WHERE J.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)" & vbNewLine & _
                //"   AND J.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)" & vbNewLine & _
                //"   and (j.PORT_MST_POD_FK " & vbNewLine & _
                //"   IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK= " & LocFK & ")  and j.JC_AUTO_MANUAL = 1)" & vbNewLine & _
                //"   AND J.PORT_MST_POD_FK = POD.PORT_MST_PK" & vbNewLine & _
                //"   AND J.PORT_MST_POL_FK = POL.PORT_MST_PK" & vbNewLine & _
                //"   AND SHIPPER.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+) " & vbNewLine
                //If LocFK <> 0 Then
                //   SQL = SQL & "   AND j.created_by_fk = usr.user_mst_pk "
                //End If
                //Air Export-Import
            }
            else if (reportType == 5)
            {
                //Air Export
                SQL = deprtmnt + "       TO_CHAR(J.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH," + "       TO_CHAR(J.JOBCARD_DATE, dateformat) JOBCARD_DATE," + "       J.JOBCARD_REF_NO REF_NUMBER," + deprtmnts + "       SHIPPER.CUSTOMER_NAME SHIPPER_NAME," + "       CONSIGNEE.CUSTOMER_NAME CONSIGNEE_NAME," + "       DECODE(J.PYMT_TYPE, 1, 'Y', 2, 'N') PAYMENT_TYPE," + "       POL.PORT_ID POL," + "       POD.PORT_ID POD," + "       0 TEU," + "       (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0))" + "             FROM JOB_TRN_air_EXP_CONT CONT" + "            WHERE CONT.JOB_CARD_air_EXP_FK = J.JOB_CARD_air_EXP_PK" + "       ) WEIGHT_VOLUME,";
                //SQL &= "       FETCH_EST_COST_EXPair(j.job_card_air_exp_pk) ESTIMATED_REVENUE," & vbNewLine
                //SQL &= "       FETCH_JOB_CARD_AIR_EXP_ACTREV(j.job_card_air_exp_pk," & HttpContext.Current.Session("currency_mst_pk") & " )  ACTUAL_REVENUE, " & vbNewLine
                //SQL &= "       NVL(FETCH_ACT_COST_EXPAIR(j.job_card_air_exp_pk),0) ACTUAL_COST, " & vbNewLine & _

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.job_card_air_exp_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.job_card_air_exp_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.job_card_air_exp_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.job_card_air_exp_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4)) FREIGHT, " + "       /*FETCH_FREIGHT_EXPAIR(j.job_card_air_exp_pk)  FETCH_EST_COST_EXPAIR_NEW(j.job_card_air_exp_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + " )  FREIGHT,*/ " + "      NVL(EMP.EMPLOYEE_NAME,'CSR') SALES_REPORTER" + "  from job_card_air_exp_tbl J," + "       BOOKING_air_TBL      BOOK," + "       CUSTOMER_MST_TBL     SHIPPER," + "       CUSTOMER_MST_TBL     CONSIGNEE," + "       PORT_MST_TBL         POL," + "       PORT_MST_TBL         POD," + "       EMPLOYEE_MST_TBL     EMP";
                //If LocFK <> 0 Then
                SQL = SQL + "       ,USER_MST_TBL  USR,location_mst_tbl l  ";
                //End If
                SQL = SQL + " WHERE J.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)" + "   AND J.BOOKING_air_FK = BOOK.BOOKING_air_PK" + "   AND J.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)" + "   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK" + "   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK" + "   AND POL.location_mst_fk = l.location_mst_pk  and l.location_mst_pk=usr.default_location_fk " + "   AND SHIPPER.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+) " + "   AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))";
                //adding by thiyagarajan on 8/11/08
                //If LocFK <> 0 Then
                //    SQL &= " l.location_mst_pk=" & LocFK
                //End If
                SQL = SQL + " AND j.created_by_fk = usr.user_mst_pk ";
                //If pageload = 0 Then
                //    SQL &= " and 1=2 "
                //End If
                if (Report == 4)
                {
                    SQL += " and J.PYMT_TYPE=1";
                }
                else if (Report == 5)
                {
                    SQL += " and J.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and J.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SHIPPER.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POD_FK=" + podpk;
                }

                ///'''''''''''''''''
                SQL += " UNION ";

                ///'''''''''''''''
                //Air Import

                SQL += deprtmnt + " TO_CHAR(Jc.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH," + " TO_CHAR(Jc.JOBCARD_DATE, dateformat) JOBCARD_DATE," + " Jc.JOBCARD_REF_NO REF_NUMBER," + deprtmnts + " SH.CUSTOMER_NAME SHIPPER_NAME," + " CO.CUSTOMER_NAME CONSIGNEE_NAME," + " DECODE(Jc.PYMT_TYPE, 1, 'Y', 2, 'N') PAYMENT_TYPE," + " POL.PORT_ID POL," + " POD.PORT_ID POD," + "       0 TEU," + " (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0))" + "    FROM JOB_TRN_air_IMP_CONT CONT" + "   WHERE CONT.JOB_CARD_air_IMP_FK = Jc.job_card_trn_pk) WEIGHT_VOLUME,";

                //SQL &= " FETCH_EST_COST_IMPair(jc.job_card_trn_pk) ESTIMATED_REVENUE," & vbNewLine
                //SQL &= " FETCH_JOB_CARD_AIR_IMP_ACTREV(jc.job_card_trn_pk," & HttpContext.Current.Session("currency_mst_pk") & " ) ACTUAL_REVENUE," & vbNewLine
                //SQL &= " NVL(FETCH_ACT_COST_IMPAIR(jc.job_card_trn_pk), 0) ACTUAL_COST," & vbNewLine & _

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4)) FREIGHT, " + " /*FETCH_FREIGHT_IMPAIR(jc.job_card_trn_pk)  FETCH_FREIGHT_IMPAIR_NEW(jc.job_card_trn_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + " ) FREIGHT,*/" + "  NVL(EMP.EMPLOYEE_NAME,'CSR') SALES_REPORTER  " + "  from JOB_CARD_AIR_IMP_TBL JC," + "  CUSTOMER_MST_TBL     SH," + "  CUSTOMER_MST_TBL     CO," + "  PORT_MST_TBL         POL," + "  PORT_MST_TBL         POD," + "  AGENT_MST_TBL        POLA," + "  USER_MST_TBL         UMT,location_mst_tbl l, " + "  EMPLOYEE_MST_TBL     EMP" + "  where JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)" + "  AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)" + "  AND JC.PORT_MST_POL_FK = POL.PORT_MST_PK" + "  AND JC.PORT_MST_POD_FK = POD.PORT_MST_PK";
                SQL += "  AND JC.POL_AGENT_MST_FK = POLA.AGENT_MST_PK(+)";
                //SQL &= " AND (JC.PORT_MST_POD_FK IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK = " & LocFK & ") and JC.JC_AUTO_MANUAL = 1)" & vbNewLine
                //SQL &= " AND jc.CREATED_BY_FK = UMT.USER_MST_PK" & vbNewLine
                SQL += " AND  jc.CREATED_BY_FK = UMT.USER_MST_PK   ";
                SQL += "  AND ((UMT.DEFAULT_LOCATION_FK = l.location_mst_pk and JC.JC_AUTO_MANUAL = 0) OR (JC.PORT_MST_POD_FK  ";
                SQL += "  IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK= l.location_mst_pk)  and JC.JC_AUTO_MANUAL = 1))  ";

                SQL += "  AND SH.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)";
                SQL += " AND SH.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)";
                //SQL &=    " AND POL.location_mst_fk ='" & Loc & "'" & vbNewLine
                SQL += " AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS = 1)";
                SQL += " AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))";
                SQL += "  and l.location_mst_pk = umt.default_location_fk ";

                if (Report == 4)
                {
                    SQL += " and JC.PYMT_TYPE=1";
                }
                else if (Report == 5)
                {
                    SQL += " and JC.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and JC.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SH.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POD_FK=" + podpk;
                }
                //Sea Export-Import
            }
            else if (reportType == 6)
            {
                SQL = deprtmnt + "       TO_CHAR(J.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH," + "       TO_CHAR(J.JOBCARD_DATE, dateformat) JOBCARD_DATE," + "       J.JOBCARD_REF_NO REF_NUMBER," + deprtmnts + "       SHIPPER.CUSTOMER_NAME SHIPPER_NAME," + "       CONSIGNEE.CUSTOMER_NAME CONSIGNEE_NAME," + "       DECODE(J.PYMT_TYPE, 1, 'PPD', 2, 'CLT') PAYMENT_TYPE," + "       POL.PORT_ID POL," + "       POD.PORT_ID POD," + "        (SELECT SUM(NVL(CTMT.TEU_FACTOR, 0))" + "           FROM JOB_TRN_CONT  CONT, CONTAINER_TYPE_MST_TBL CTMT" + "       WHERE CONT.JOB_CARD_TRN_FK = J.JOB_CARD_TRN_PK " + "        AND CONT.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK(+)) TEU, " + "       (CASE" + "         WHEN BOOK.CARGO_TYPE = 1 THEN" + "          (SELECT SUM(NVL(CONT.NET_WEIGHT, 0))" + "             FROM JOB_TRN_CONT CONT" + "            WHERE CONT.JOB_CARD_TRN_FK = J.JOB_CARD_TRN_PK)" + "         ELSE" + "          (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0))" + "             FROM JOB_TRN_CONT CONT" + "            WHERE CONT.JOB_CARD_TRN_FK = J.JOB_CARD_TRN_PK)" + "       END) WEIGHT_VOLUME,";
                //SQL &= "       FETCH_FREIGHT_EXPSEA(j.JOB_CARD_TRN_PK) FREIGHT," & vbNewLine
                //SQL &= "       FETCH_FREIGHT_EXPSEA_NEW(j.JOB_CARD_TRN_PK, " & HttpContext.Current.Session("CURRENCY_MST_PK") & ") FREIGHT,"
                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4)) FREIGHT, ";

                //SQL &= "       FETCH_JOB_CARD_SEA_EXP_ACTREV(j.JOB_CARD_TRN_PK," & HttpContext.Current.Session("currency_mst_pk") & " ) ACTUAL_REVENUE, " & vbNewLine
                //SQL &= "       NVL(FETCH_ACT_COST_EXPSEA(j.JOB_CARD_TRN_PK),0) ACTUAL_COST, " & vbNewLine
                //SQL &= "       FETCH_EST_COST_EXPSEA(j.JOB_CARD_TRN_PK) ESTIMATED_REVENUE, " & vbNewLine

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";

                SQL += "       NVL(EMP.EMPLOYEE_NAME,'CSR') SALES_REPORTER" + "  from job_card_trn J," + "       BOOKING_MST_TBL      BOOK," + "       CUSTOMER_MST_TBL     SHIPPER," + "       CUSTOMER_MST_TBL     CONSIGNEE," + "       PORT_MST_TBL         POL," + "       PORT_MST_TBL         POD," + "       EMPLOYEE_MST_TBL     EMP";
                //If LocFK <> 0 Then ' 'Added by purnanand for PTS MIS-APR-001 for all report type
                SQL = SQL + "       ,USER_MST_TBL         USR ,location_mst_tbl l";
                //End If
                SQL = SQL + " WHERE J.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)" + "   AND J.BOOKING_MST_FK = BOOK.BOOKING_MST_PK" + "   AND J.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)" + "   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK" + "   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK" + "   AND POL.location_mst_fk = l.location_mst_pk and l.location_mst_pk=usr.default_location_fk " + "   AND SHIPPER.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+) ";
                //adding by thiyagarajan on 8/11/08
                SQL += "   AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))";
                //If LocFK <> 0 Then
                SQL = SQL + "   AND j.created_by_fk = usr.user_mst_pk ";
                //End If

                if (Report == 4)
                {
                    SQL += " and J.PYMT_TYPE=1";
                }
                else if (Report == 5)
                {
                    SQL += " and J.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and J.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SHIPPER.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POD_FK=" + podpk;
                }

                ///''''''''''''
                SQL += " UNION ";

                ///'''''''''''
                //Sea Import
                SQL += deprtmnt + "TO_CHAR(Jc.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH," + "TO_CHAR(Jc.JOBCARD_DATE, dateformat) JOBCARD_DATE," + "Jc.JOBCARD_REF_NO REF_NUMBER," + deprtmnts + "sh.CUSTOMER_NAME SHIPPER_NAME," + "co.CUSTOMER_NAME CONSIGNEE_NAME," + "DECODE(Jc.PYMT_TYPE, 1, 'PPD', 2, 'CLT') PAYMENT_TYPE," + "POL.PORT_ID POL," + "POD.PORT_ID POD," + "(SELECT SUM(NVL(CTMT.TEU_FACTOR, 0))" + "FROM JOB_TRN_CONT  CONT, CONTAINER_TYPE_MST_TBL CTMT" + "WHERE CONT.JOB_CARD_sea_imp_FK = jC.job_card_trn_pk " + "AND CONT.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK(+)) TEU, " + "(CASE WHEN jc.CARGO_TYPE = 1 THEN (SELECT SUM(NVL(CONT.NET_WEIGHT, 0)) FROM JOB_TRN_CONT CONT WHERE CONT.JOB_CARD_SEA_IMP_FK = Jc.job_card_trn_pk) ELSE (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0)) FROM JOB_TRN_CONT CONT WHERE CONT.JOB_CARD_SEA_IMP_FK = Jc.job_card_trn_pk) END) WEIGHT_VOLUME,";
                //SQL &= "       FETCH_EST_COST_IMPSEA(jc.job_card_trn_pk) ESTIMATED_REVENUE," & vbNewLine
                //SQL &= "       FETCH_JOB_CARD_SEA_IMP_ACTREV(jc.job_card_trn_pk," & HttpContext.Current.Session("currency_mst_pk") & " ) ACTUAL_REVENUE, " & vbNewLine
                //sql &=   "       NVL(FETCH_ACT_COST_IMPSEA(jc.job_card_trn_pk),0) ACTUAL_COST, " & vbNewLine & _

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4)) FREIGHT, " + "       /*FETCH_FREIGHT_IMPSEA(jc.job_card_trn_pk) FETCH_FREIGHT_IMPSEA_NEW(jc.job_card_trn_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ") FREIGHT,*/ " + "      NVL(EMP.EMPLOYEE_NAME,'CSR') SALES_REPORTER " + "       from JOB_CARD_TRN jC," + "       /*JOB_TRN_CONT cont,*/" + "       CUSTOMER_MST_TBL     SH," + "       CUSTOMER_MST_TBL     CO," + "       PORT_MST_TBL         POL," + "       PORT_MST_TBL         POD," + "      AGENT_MST_TBL        POLA," + "       EMPLOYEE_MST_TBL     EMP,location_mst_tbl l," + "       USER_MST_TBL UMT" + "where JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)" + " /* AND jc.job_card_trn_pk = cont.job_card_sea_imp_fk(+) */" + "AND sh.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)" + "AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)" + "AND JC.PORT_MST_POL_FK = POL.PORT_MST_PK" + "AND JC.PORT_MST_POD_FK = POD.PORT_MST_PK" + "AND JC.POL_AGENT_MST_FK = POLA.AGENT_MST_PK(+)";
                // SQL &= "AND (JC.PORT_MST_POD_FK IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK =" & LocFK & ") and JC.JC_AUTO_MANUAL = 1)" & vbNewLine
                // SQL &= "AND jc.CREATED_BY_FK = UMT.USER_MST_PK" & vbNewLine & _
                SQL += " AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS = 1)";
                //sql &=  "AND POL.location_mst_fk ='" & Loc & "'" & vbNewLine & _
                //sql &=  " AND JC.CARGO_TYPE = 1" & vbNewLine & _
                //sql &=  " AND POL.location_mst_fk ='" & Loc & "'" & vbNewLine & _
                //sql &=  "  AND JC.CARGO_TYPE = 1"
                SQL += "  AND jc.CREATED_BY_FK = UMT.USER_MST_PK and l.location_mst_pk=umt.default_location_fk ";
                SQL += "  AND ((UMT.DEFAULT_LOCATION_FK = l.location_mst_pk and JC.JC_AUTO_MANUAL = 0) OR (JC.PORT_MST_POD_FK  ";
                SQL += "  IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK= l.location_mst_pk)  and JC.JC_AUTO_MANUAL = 1)) ";

                SQL += "  AND SH.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)";
                SQL += " AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))";

                if (Report == 4)
                {
                    SQL += " and JC.PYMT_TYPE=1";
                }
                else if (Report == 5)
                {
                    SQL += " and JC.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and JC.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SH.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POD_FK=" + podpk;
                }
                //Air-Sea Export
            }
            else if (reportType == 7)
            {
                //Air Export
                SQL = deprtmnt + "       TO_CHAR(J.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH," + "       TO_CHAR(J.JOBCARD_DATE, dateformat) JOBCARD_DATE," + "       J.JOBCARD_REF_NO REF_NUMBER," + deprtmnts + "       SHIPPER.CUSTOMER_NAME SHIPPER_NAME," + "       CONSIGNEE.CUSTOMER_NAME CONSIGNEE_NAME," + "       DECODE(J.PYMT_TYPE, 1, 'Y', 2, 'N') PAYMENT_TYPE," + "       POL.PORT_ID POL," + "       POD.PORT_ID POD," + "       0 TEU," + "       (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0))" + "             FROM JOB_TRN_air_EXP_CONT CONT" + "            WHERE CONT.JOB_CARD_air_EXP_FK = J.JOB_CARD_air_EXP_PK" + "       ) WEIGHT_VOLUME,";
                //SQL &= "       FETCH_EST_COST_EXPair(j.job_card_air_exp_pk) ESTIMATED_REVENUE," & vbNewLine
                //SQL &= "       FETCH_JOB_CARD_AIR_EXP_ACTREV(j.job_card_air_exp_pk," & HttpContext.Current.Session("currency_mst_pk") & " )  ACTUAL_REVENUE, " & vbNewLine
                //SQL &= "       NVL(FETCH_ACT_COST_EXPAIR(j.job_card_air_exp_pk),0) ACTUAL_COST, " & vbNewLine & _

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.job_card_air_exp_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.job_card_air_exp_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.job_card_air_exp_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.job_card_air_exp_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4)) FREIGHT, " + "       /*FETCH_FREIGHT_EXPAIR(j.job_card_air_exp_pk)  FETCH_EST_COST_EXPAIR_NEW(j.job_card_air_exp_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + " )  FREIGHT,*/ " + "      NVL(EMP.EMPLOYEE_NAME,'CSR') SALES_REPORTER" + "  from job_card_air_exp_tbl J," + "       BOOKING_air_TBL      BOOK," + "       CUSTOMER_MST_TBL     SHIPPER," + "       CUSTOMER_MST_TBL     CONSIGNEE," + "       PORT_MST_TBL         POL," + "       PORT_MST_TBL         POD," + "       EMPLOYEE_MST_TBL     EMP";
                //If LocFK <> 0 Then
                SQL = SQL + "       ,USER_MST_TBL  USR,location_mst_tbl l  ";
                //End If
                SQL = SQL + " WHERE J.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)" + "   AND J.BOOKING_air_FK = BOOK.BOOKING_air_PK" + "   AND J.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)" + "   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK" + "   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK" + "   AND POL.location_mst_fk = l.location_mst_pk  and l.location_mst_pk=usr.default_location_fk " + "   AND SHIPPER.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+) " + "   AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))";
                //adding by thiyagarajan on 8/11/08
                //If LocFK <> 0 Then
                //    SQL &= " l.location_mst_pk=" & LocFK
                //End If
                SQL = SQL + " AND j.created_by_fk = usr.user_mst_pk ";
                //If pageload = 0 Then
                //    SQL &= " and 1=2 "
                //End If
                if (Report == 4)
                {
                    SQL += " and J.PYMT_TYPE=1";
                }
                else if (Report == 5)
                {
                    SQL += " and J.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and J.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SHIPPER.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POD_FK=" + podpk;
                }

                ///'''''''
                SQL += " UNION ";

                //Sea Export
                SQL += deprtmnt + "       TO_CHAR(J.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH," + "       TO_CHAR(J.JOBCARD_DATE, dateformat) JOBCARD_DATE," + "       J.JOBCARD_REF_NO REF_NUMBER," + deprtmnts + "       SHIPPER.CUSTOMER_NAME SHIPPER_NAME," + "       CONSIGNEE.CUSTOMER_NAME CONSIGNEE_NAME," + "       DECODE(J.PYMT_TYPE, 1, 'PPD', 2, 'CLT') PAYMENT_TYPE," + "       POL.PORT_ID POL," + "       POD.PORT_ID POD," + "        (SELECT SUM(NVL(CTMT.TEU_FACTOR, 0))" + "           FROM JOB_TRN_CONT  CONT, CONTAINER_TYPE_MST_TBL CTMT" + "       WHERE CONT.JOB_CARD_TRN_FK = J.JOB_CARD_TRN_PK " + "        AND CONT.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK(+)) TEU, " + "       (CASE" + "         WHEN BOOK.CARGO_TYPE = 1 THEN" + "          (SELECT SUM(NVL(CONT.NET_WEIGHT, 0))" + "             FROM JOB_TRN_CONT CONT" + "            WHERE CONT.JOB_CARD_TRN_FK = J.JOB_CARD_TRN_PK)" + "         ELSE" + "          (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0))" + "             FROM JOB_TRN_CONT CONT" + "            WHERE CONT.JOB_CARD_TRN_FK = J.JOB_CARD_TRN_PK)" + "       END) WEIGHT_VOLUME,";
                //SQL &= "       FETCH_FREIGHT_EXPSEA(j.JOB_CARD_TRN_PK) FREIGHT," & vbNewLine
                //SQL &= "       FETCH_FREIGHT_EXPSEA_NEW(j.JOB_CARD_TRN_PK, " & HttpContext.Current.Session("CURRENCY_MST_PK") & ") FREIGHT,"
                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4)) FREIGHT, ";

                //SQL &= "       FETCH_JOB_CARD_SEA_EXP_ACTREV(j.JOB_CARD_TRN_PK," & HttpContext.Current.Session("currency_mst_pk") & " ) ACTUAL_REVENUE, " & vbNewLine
                //SQL &= "       NVL(FETCH_ACT_COST_EXPSEA(j.JOB_CARD_TRN_PK),0) ACTUAL_COST, " & vbNewLine
                //SQL &= "       FETCH_EST_COST_EXPSEA(j.JOB_CARD_TRN_PK) ESTIMATED_REVENUE, " & vbNewLine

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";

                SQL += "       NVL(EMP.EMPLOYEE_NAME,'CSR') SALES_REPORTER" + "  from job_card_trn J," + "       BOOKING_MST_TBL      BOOK," + "       CUSTOMER_MST_TBL     SHIPPER," + "       CUSTOMER_MST_TBL     CONSIGNEE," + "       PORT_MST_TBL         POL," + "       PORT_MST_TBL         POD," + "       EMPLOYEE_MST_TBL     EMP";
                //If LocFK <> 0 Then ' 'Added by purnanand for PTS MIS-APR-001 for all report type
                SQL = SQL + "       ,USER_MST_TBL         USR ,location_mst_tbl l";
                //End If
                SQL = SQL + " WHERE J.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)" + "   AND J.BOOKING_MST_FK = BOOK.BOOKING_MST_PK" + "   AND J.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)" + "   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK" + "   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK" + "   AND POL.location_mst_fk = l.location_mst_pk and l.location_mst_pk=usr.default_location_fk " + "   AND SHIPPER.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+) ";
                //adding by thiyagarajan on 8/11/08
                SQL += "   AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))";
                //If LocFK <> 0 Then
                SQL = SQL + "   AND j.created_by_fk = usr.user_mst_pk ";
                //End If

                if (Report == 4)
                {
                    SQL += " and J.PYMT_TYPE=1";
                }
                else if (Report == 5)
                {
                    SQL += " and J.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and J.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SHIPPER.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POD_FK=" + podpk;
                }
                //Air-Sea Import
            }
            else if (reportType == 8)
            {
                //Air Import
                SQL = deprtmnt + " TO_CHAR(Jc.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH," + " TO_CHAR(Jc.JOBCARD_DATE, dateformat) JOBCARD_DATE," + " Jc.JOBCARD_REF_NO REF_NUMBER," + deprtmnts + " SH.CUSTOMER_NAME SHIPPER_NAME," + " CO.CUSTOMER_NAME CONSIGNEE_NAME," + " DECODE(Jc.PYMT_TYPE, 1, 'Y', 2, 'N') PAYMENT_TYPE," + " POL.PORT_ID POL," + " POD.PORT_ID POD," + "       0 TEU," + " (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0))" + "    FROM JOB_TRN_air_IMP_CONT CONT" + "   WHERE CONT.JOB_CARD_air_IMP_FK = Jc.job_card_trn_pk) WEIGHT_VOLUME,";

                //SQL &= " FETCH_EST_COST_IMPair(jc.job_card_trn_pk) ESTIMATED_REVENUE," & vbNewLine
                //SQL &= " FETCH_JOB_CARD_AIR_IMP_ACTREV(jc.job_card_trn_pk," & HttpContext.Current.Session("currency_mst_pk") & " ) ACTUAL_REVENUE," & vbNewLine
                //SQL &= " NVL(FETCH_ACT_COST_IMPAIR(jc.job_card_trn_pk), 0) ACTUAL_COST," & vbNewLine & _

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4)) FREIGHT, " + " /*FETCH_FREIGHT_IMPAIR(jc.job_card_trn_pk)  FETCH_FREIGHT_IMPAIR_NEW(jc.job_card_trn_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + " ) FREIGHT,*/" + "  NVL(EMP.EMPLOYEE_NAME,'CSR') SALES_REPORTER  " + "  from JOB_CARD_AIR_IMP_TBL JC," + "  CUSTOMER_MST_TBL     SH," + "  CUSTOMER_MST_TBL     CO," + "  PORT_MST_TBL         POL," + "  PORT_MST_TBL         POD," + "  AGENT_MST_TBL        POLA," + "  USER_MST_TBL         UMT,location_mst_tbl l, " + "  EMPLOYEE_MST_TBL     EMP" + "  where JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)" + "  AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)" + "  AND JC.PORT_MST_POL_FK = POL.PORT_MST_PK" + "  AND JC.PORT_MST_POD_FK = POD.PORT_MST_PK";
                SQL += "  AND JC.POL_AGENT_MST_FK = POLA.AGENT_MST_PK(+)";
                //SQL &= " AND (JC.PORT_MST_POD_FK IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK = " & LocFK & ") and JC.JC_AUTO_MANUAL = 1)" & vbNewLine
                //SQL &= " AND jc.CREATED_BY_FK = UMT.USER_MST_PK" & vbNewLine
                SQL += " AND  jc.CREATED_BY_FK = UMT.USER_MST_PK   ";
                SQL += "  AND ((UMT.DEFAULT_LOCATION_FK = l.location_mst_pk and JC.JC_AUTO_MANUAL = 0) OR (JC.PORT_MST_POD_FK  ";
                SQL += "  IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK= l.location_mst_pk)  and JC.JC_AUTO_MANUAL = 1))  ";

                SQL += "  AND SH.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)";
                SQL += " AND SH.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)";
                //SQL &=    " AND POL.location_mst_fk ='" & Loc & "'" & vbNewLine
                SQL += " AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS = 1)";
                SQL += " AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))";
                SQL += "  and l.location_mst_pk = umt.default_location_fk ";

                if (Report == 4)
                {
                    SQL += " and JC.PYMT_TYPE=1";
                }
                else if (Report == 5)
                {
                    SQL += " and JC.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and JC.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SH.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POD_FK=" + podpk;
                }
                ///'''''''''''''''''''''''''''''''''''
                SQL += " UNION ";
                ///'''''''''''''''''''''''''''''''''''''
                //Sea Import
                SQL += deprtmnt + "TO_CHAR(Jc.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH," + "TO_CHAR(Jc.JOBCARD_DATE, dateformat) JOBCARD_DATE," + "Jc.JOBCARD_REF_NO REF_NUMBER," + deprtmnts + "sh.CUSTOMER_NAME SHIPPER_NAME," + "co.CUSTOMER_NAME CONSIGNEE_NAME," + "DECODE(Jc.PYMT_TYPE, 1, 'PPD', 2, 'CLT') PAYMENT_TYPE," + "POL.PORT_ID POL," + "POD.PORT_ID POD," + "(SELECT SUM(NVL(CTMT.TEU_FACTOR, 0))" + "FROM JOB_TRN_CONT  CONT, CONTAINER_TYPE_MST_TBL CTMT" + "WHERE CONT.JOB_CARD_sea_imp_FK = jC.job_card_trn_pk " + "AND CONT.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK(+)) TEU, " + "(CASE WHEN jc.CARGO_TYPE = 1 THEN (SELECT SUM(NVL(CONT.NET_WEIGHT, 0)) FROM JOB_TRN_CONT CONT WHERE CONT.JOB_CARD_SEA_IMP_FK = Jc.job_card_trn_pk) ELSE (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0)) FROM JOB_TRN_CONT CONT WHERE CONT.JOB_CARD_SEA_IMP_FK = Jc.job_card_trn_pk) END) WEIGHT_VOLUME,";
                //SQL &= "       FETCH_EST_COST_IMPSEA(jc.job_card_trn_pk) ESTIMATED_REVENUE," & vbNewLine
                //SQL &= "       FETCH_JOB_CARD_SEA_IMP_ACTREV(jc.job_card_trn_pk," & HttpContext.Current.Session("currency_mst_pk") & " ) ACTUAL_REVENUE, " & vbNewLine
                //sql &=   "       NVL(FETCH_ACT_COST_IMPSEA(jc.job_card_trn_pk),0) ACTUAL_COST, " & vbNewLine & _

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4)) FREIGHT, " + "       /*FETCH_FREIGHT_IMPSEA(jc.job_card_trn_pk) FETCH_FREIGHT_IMPSEA_NEW(jc.job_card_trn_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ") FREIGHT,*/ " + "      NVL(EMP.EMPLOYEE_NAME,'CSR') SALES_REPORTER " + "       from JOB_CARD_TRN jC," + "       /*JOB_TRN_CONT cont,*/" + "       CUSTOMER_MST_TBL     SH," + "       CUSTOMER_MST_TBL     CO," + "       PORT_MST_TBL         POL," + "       PORT_MST_TBL         POD," + "      AGENT_MST_TBL        POLA," + "       EMPLOYEE_MST_TBL     EMP,location_mst_tbl l," + "       USER_MST_TBL UMT" + "where JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)" + " /* AND jc.job_card_trn_pk = cont.job_card_sea_imp_fk(+) */" + "AND sh.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)" + "AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)" + "AND JC.PORT_MST_POL_FK = POL.PORT_MST_PK" + "AND JC.PORT_MST_POD_FK = POD.PORT_MST_PK" + "AND JC.POL_AGENT_MST_FK = POLA.AGENT_MST_PK(+)";
                // SQL &= "AND (JC.PORT_MST_POD_FK IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK =" & LocFK & ") and JC.JC_AUTO_MANUAL = 1)" & vbNewLine
                // SQL &= "AND jc.CREATED_BY_FK = UMT.USER_MST_PK" & vbNewLine & _
                SQL += " AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS = 1)";
                //sql &=  "AND POL.location_mst_fk ='" & Loc & "'" & vbNewLine & _
                //sql &=  " AND JC.CARGO_TYPE = 1" & vbNewLine & _
                //sql &=  " AND POL.location_mst_fk ='" & Loc & "'" & vbNewLine & _
                //sql &=  "  AND JC.CARGO_TYPE = 1"
                SQL += "  AND jc.CREATED_BY_FK = UMT.USER_MST_PK and l.location_mst_pk=umt.default_location_fk ";
                SQL += "  AND ((UMT.DEFAULT_LOCATION_FK = l.location_mst_pk and JC.JC_AUTO_MANUAL = 0) OR (JC.PORT_MST_POD_FK  ";
                SQL += "  IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK= l.location_mst_pk)  and JC.JC_AUTO_MANUAL = 1)) ";

                SQL += "  AND SH.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)";
                SQL += " AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))";

                if (Report == 4)
                {
                    SQL += " and JC.PYMT_TYPE=1";
                }
                else if (Report == 5)
                {
                    SQL += " and JC.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and JC.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SH.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POD_FK=" + podpk;
                }
                //Air-Sea Export-Import
            }
            else if (reportType == 9)
            {
                //Air Export
                SQL = deprtmnt + "       TO_CHAR(J.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH," + "       TO_CHAR(J.JOBCARD_DATE, dateformat) JOBCARD_DATE," + "       J.JOBCARD_REF_NO REF_NUMBER," + deprtmnts + "       SHIPPER.CUSTOMER_NAME SHIPPER_NAME," + "       CONSIGNEE.CUSTOMER_NAME CONSIGNEE_NAME," + "       DECODE(J.PYMT_TYPE, 1, 'Y', 2, 'N') PAYMENT_TYPE," + "       POL.PORT_ID POL," + "       POD.PORT_ID POD," + "       0 TEU," + "       (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0))" + "             FROM JOB_TRN_air_EXP_CONT CONT" + "            WHERE CONT.JOB_CARD_air_EXP_FK = J.JOB_CARD_air_EXP_PK" + "       ) WEIGHT_VOLUME,";
                //SQL &= "       FETCH_EST_COST_EXPair(j.job_card_air_exp_pk) ESTIMATED_REVENUE," & vbNewLine
                //SQL &= "       FETCH_JOB_CARD_AIR_EXP_ACTREV(j.job_card_air_exp_pk," & HttpContext.Current.Session("currency_mst_pk") & " )  ACTUAL_REVENUE, " & vbNewLine
                //SQL &= "       NVL(FETCH_ACT_COST_EXPAIR(j.job_card_air_exp_pk),0) ACTUAL_COST, " & vbNewLine & _

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.job_card_air_exp_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.job_card_air_exp_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.job_card_air_exp_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.job_card_air_exp_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4)) FREIGHT, " + "       /*FETCH_FREIGHT_EXPAIR(j.job_card_air_exp_pk)  FETCH_EST_COST_EXPAIR_NEW(j.job_card_air_exp_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + " )  FREIGHT,*/ " + "      NVL(EMP.EMPLOYEE_NAME,'CSR') SALES_REPORTER" + "  from job_card_air_exp_tbl J," + "       BOOKING_air_TBL      BOOK," + "       CUSTOMER_MST_TBL     SHIPPER," + "       CUSTOMER_MST_TBL     CONSIGNEE," + "       PORT_MST_TBL         POL," + "       PORT_MST_TBL         POD," + "       EMPLOYEE_MST_TBL     EMP";
                //If LocFK <> 0 Then
                SQL = SQL + "       ,USER_MST_TBL  USR,location_mst_tbl l  ";
                //End If
                SQL = SQL + " WHERE J.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)" + "   AND J.BOOKING_air_FK = BOOK.BOOKING_air_PK" + "   AND J.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)" + "   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK" + "   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK" + "   AND POL.location_mst_fk = l.location_mst_pk  and l.location_mst_pk=usr.default_location_fk " + "   AND SHIPPER.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+) " + "   AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))";
                //adding by thiyagarajan on 8/11/08
                //If LocFK <> 0 Then
                //    SQL &= " l.location_mst_pk=" & LocFK
                //End If
                SQL = SQL + " AND j.created_by_fk = usr.user_mst_pk ";
                //If pageload = 0 Then
                //    SQL &= " and 1=2 "
                //End If
                if (Report == 4)
                {
                    SQL += " and J.PYMT_TYPE=1";
                }
                else if (Report == 5)
                {
                    SQL += " and J.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and J.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SHIPPER.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POD_FK=" + podpk;
                }

                ///'''''''''
                SQL += " UNION ";
                ///'''''''''''
                //Air Import
                SQL += deprtmnt + " TO_CHAR(Jc.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH," + " TO_CHAR(Jc.JOBCARD_DATE, dateformat) JOBCARD_DATE," + " Jc.JOBCARD_REF_NO REF_NUMBER," + deprtmnts + " SH.CUSTOMER_NAME SHIPPER_NAME," + " CO.CUSTOMER_NAME CONSIGNEE_NAME," + " DECODE(Jc.PYMT_TYPE, 1, 'Y', 2, 'N') PAYMENT_TYPE," + " POL.PORT_ID POL," + " POD.PORT_ID POD," + "       0 TEU," + " (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0))" + "    FROM JOB_TRN_air_IMP_CONT CONT" + "   WHERE CONT.JOB_CARD_air_IMP_FK = Jc.job_card_trn_pk) WEIGHT_VOLUME,";

                //SQL &= " FETCH_EST_COST_IMPair(jc.job_card_trn_pk) ESTIMATED_REVENUE," & vbNewLine
                //SQL &= " FETCH_JOB_CARD_AIR_IMP_ACTREV(jc.job_card_trn_pk," & HttpContext.Current.Session("currency_mst_pk") & " ) ACTUAL_REVENUE," & vbNewLine
                //SQL &= " NVL(FETCH_ACT_COST_IMPAIR(jc.job_card_trn_pk), 0) ACTUAL_COST," & vbNewLine & _

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4)) FREIGHT, " + " /*FETCH_FREIGHT_IMPAIR(jc.job_card_trn_pk)  FETCH_FREIGHT_IMPAIR_NEW(jc.job_card_trn_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + " ) FREIGHT,*/" + "  NVL(EMP.EMPLOYEE_NAME,'CSR') SALES_REPORTER  " + "  from JOB_CARD_AIR_IMP_TBL JC," + "  CUSTOMER_MST_TBL     SH," + "  CUSTOMER_MST_TBL     CO," + "  PORT_MST_TBL         POL," + "  PORT_MST_TBL         POD," + "  AGENT_MST_TBL        POLA," + "  USER_MST_TBL         UMT,location_mst_tbl l, " + "  EMPLOYEE_MST_TBL     EMP" + "  where JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)" + "  AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)" + "  AND JC.PORT_MST_POL_FK = POL.PORT_MST_PK" + "  AND JC.PORT_MST_POD_FK = POD.PORT_MST_PK";
                SQL += "  AND JC.POL_AGENT_MST_FK = POLA.AGENT_MST_PK(+)";
                //SQL &= " AND (JC.PORT_MST_POD_FK IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK = " & LocFK & ") and JC.JC_AUTO_MANUAL = 1)" & vbNewLine
                //SQL &= " AND jc.CREATED_BY_FK = UMT.USER_MST_PK" & vbNewLine
                SQL += " AND  jc.CREATED_BY_FK = UMT.USER_MST_PK   ";
                SQL += "  AND ((UMT.DEFAULT_LOCATION_FK = l.location_mst_pk and JC.JC_AUTO_MANUAL = 0) OR (JC.PORT_MST_POD_FK  ";
                SQL += "  IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK= l.location_mst_pk)  and JC.JC_AUTO_MANUAL = 1))  ";

                SQL += "  AND SH.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)";
                SQL += " AND SH.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)";
                //SQL &=    " AND POL.location_mst_fk ='" & Loc & "'" & vbNewLine
                SQL += " AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS = 1)";
                SQL += " AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))";
                SQL += "  and l.location_mst_pk = umt.default_location_fk ";

                if (Report == 4)
                {
                    SQL += " and JC.PYMT_TYPE=1";
                }
                else if (Report == 5)
                {
                    SQL += " and JC.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and JC.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SH.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POD_FK=" + podpk;
                }

                ///''''''''
                SQL += " UNION ";
                ///''''''''''''''''
                //Sea Export
                SQL += deprtmnt + "       TO_CHAR(J.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH," + "       TO_CHAR(J.JOBCARD_DATE, dateformat) JOBCARD_DATE," + "       J.JOBCARD_REF_NO REF_NUMBER," + deprtmnts + "       SHIPPER.CUSTOMER_NAME SHIPPER_NAME," + "       CONSIGNEE.CUSTOMER_NAME CONSIGNEE_NAME," + "       DECODE(J.PYMT_TYPE, 1, 'PPD', 2, 'CLT') PAYMENT_TYPE," + "       POL.PORT_ID POL," + "       POD.PORT_ID POD," + "        (SELECT SUM(NVL(CTMT.TEU_FACTOR, 0))" + "           FROM JOB_TRN_CONT  CONT, CONTAINER_TYPE_MST_TBL CTMT" + "       WHERE CONT.JOB_CARD_TRN_FK = J.JOB_CARD_TRN_PK " + "        AND CONT.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK(+)) TEU, " + "       (CASE" + "         WHEN BOOK.CARGO_TYPE = 1 THEN" + "          (SELECT SUM(NVL(CONT.NET_WEIGHT, 0))" + "             FROM JOB_TRN_CONT CONT" + "            WHERE CONT.JOB_CARD_TRN_FK = J.JOB_CARD_TRN_PK)" + "         ELSE" + "          (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0))" + "             FROM JOB_TRN_CONT CONT" + "            WHERE CONT.JOB_CARD_TRN_FK = J.JOB_CARD_TRN_PK)" + "       END) WEIGHT_VOLUME,";
                //SQL &= "       FETCH_FREIGHT_EXPSEA(j.JOB_CARD_TRN_PK) FREIGHT," & vbNewLine
                //SQL &= "       FETCH_FREIGHT_EXPSEA_NEW(j.JOB_CARD_TRN_PK, " & HttpContext.Current.Session("CURRENCY_MST_PK") & ") FREIGHT,"
                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4)) FREIGHT, ";

                //SQL &= "       FETCH_JOB_CARD_SEA_EXP_ACTREV(j.JOB_CARD_TRN_PK," & HttpContext.Current.Session("currency_mst_pk") & " ) ACTUAL_REVENUE, " & vbNewLine
                //SQL &= "       NVL(FETCH_ACT_COST_EXPSEA(j.JOB_CARD_TRN_PK),0) ACTUAL_COST, " & vbNewLine
                //SQL &= "       FETCH_EST_COST_EXPSEA(j.JOB_CARD_TRN_PK) ESTIMATED_REVENUE, " & vbNewLine

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";

                SQL += "       NVL(EMP.EMPLOYEE_NAME,'CSR') SALES_REPORTER" + "  from job_card_trn J," + "       BOOKING_MST_TBL      BOOK," + "       CUSTOMER_MST_TBL     SHIPPER," + "       CUSTOMER_MST_TBL     CONSIGNEE," + "       PORT_MST_TBL         POL," + "       PORT_MST_TBL         POD," + "       EMPLOYEE_MST_TBL     EMP";
                //If LocFK <> 0 Then ' 'Added by purnanand for PTS MIS-APR-001 for all report type
                SQL = SQL + "       ,USER_MST_TBL         USR ,location_mst_tbl l";
                //End If
                SQL = SQL + " WHERE J.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)" + "   AND J.BOOKING_MST_FK = BOOK.BOOKING_MST_PK" + "   AND J.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)" + "   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK" + "   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK" + "   AND POL.location_mst_fk = l.location_mst_pk and l.location_mst_pk=usr.default_location_fk " + "   AND SHIPPER.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+) ";
                //adding by thiyagarajan on 8/11/08
                SQL += "   AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))";
                //If LocFK <> 0 Then
                SQL = SQL + "   AND j.created_by_fk = usr.user_mst_pk ";
                //End If

                if (Report == 4)
                {
                    SQL += " and J.PYMT_TYPE=1";
                }
                else if (Report == 5)
                {
                    SQL += " and J.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and J.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SHIPPER.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POD_FK=" + podpk;
                }

                ///'''''''''''''
                SQL += " UNION ";
                ///'''''''''''''
                //Sea Import
                SQL += deprtmnt + "TO_CHAR(Jc.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH," + "TO_CHAR(Jc.JOBCARD_DATE, dateformat) JOBCARD_DATE," + "Jc.JOBCARD_REF_NO REF_NUMBER," + deprtmnts + "sh.CUSTOMER_NAME SHIPPER_NAME," + "co.CUSTOMER_NAME CONSIGNEE_NAME," + "DECODE(Jc.PYMT_TYPE, 1, 'PPD', 2, 'CLT') PAYMENT_TYPE," + "POL.PORT_ID POL," + "POD.PORT_ID POD," + "(SELECT SUM(NVL(CTMT.TEU_FACTOR, 0))" + "FROM JOB_TRN_CONT  CONT, CONTAINER_TYPE_MST_TBL CTMT" + "WHERE CONT.JOB_CARD_sea_imp_FK = jC.job_card_trn_pk " + "AND CONT.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK(+)) TEU, " + "(CASE WHEN jc.CARGO_TYPE = 1 THEN (SELECT SUM(NVL(CONT.NET_WEIGHT, 0)) FROM JOB_TRN_CONT CONT WHERE CONT.JOB_CARD_SEA_IMP_FK = Jc.job_card_trn_pk) ELSE (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0)) FROM JOB_TRN_CONT CONT WHERE CONT.JOB_CARD_SEA_IMP_FK = Jc.job_card_trn_pk) END) WEIGHT_VOLUME,";
                //SQL &= "       FETCH_EST_COST_IMPSEA(jc.job_card_trn_pk) ESTIMATED_REVENUE," & vbNewLine
                //SQL &= "       FETCH_JOB_CARD_SEA_IMP_ACTREV(jc.job_card_trn_pk," & HttpContext.Current.Session("currency_mst_pk") & " ) ACTUAL_REVENUE, " & vbNewLine
                //sql &=   "       NVL(FETCH_ACT_COST_IMPSEA(jc.job_card_trn_pk),0) ACTUAL_COST, " & vbNewLine & _

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4)) FREIGHT, " + "       /*FETCH_FREIGHT_IMPSEA(jc.job_card_trn_pk) FETCH_FREIGHT_IMPSEA_NEW(jc.job_card_trn_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ") FREIGHT,*/ " + "      NVL(EMP.EMPLOYEE_NAME,'CSR') SALES_REPORTER " + "       from JOB_CARD_TRN jC," + "       /*JOB_TRN_CONT cont,*/" + "       CUSTOMER_MST_TBL     SH," + "       CUSTOMER_MST_TBL     CO," + "       PORT_MST_TBL         POL," + "       PORT_MST_TBL         POD," + "      AGENT_MST_TBL        POLA," + "       EMPLOYEE_MST_TBL     EMP,location_mst_tbl l," + "       USER_MST_TBL UMT" + "where JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)" + " /* AND jc.job_card_trn_pk = cont.job_card_sea_imp_fk(+) */" + "AND sh.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)" + "AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)" + "AND JC.PORT_MST_POL_FK = POL.PORT_MST_PK" + "AND JC.PORT_MST_POD_FK = POD.PORT_MST_PK" + "AND JC.POL_AGENT_MST_FK = POLA.AGENT_MST_PK(+)";
                // SQL &= "AND (JC.PORT_MST_POD_FK IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK =" & LocFK & ") and JC.JC_AUTO_MANUAL = 1)" & vbNewLine
                // SQL &= "AND jc.CREATED_BY_FK = UMT.USER_MST_PK" & vbNewLine & _
                SQL += " AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS = 1)";
                //sql &=  "AND POL.location_mst_fk ='" & Loc & "'" & vbNewLine & _
                //sql &=  " AND JC.CARGO_TYPE = 1" & vbNewLine & _
                //sql &=  " AND POL.location_mst_fk ='" & Loc & "'" & vbNewLine & _
                //sql &=  "  AND JC.CARGO_TYPE = 1"
                SQL += "  AND jc.CREATED_BY_FK = UMT.USER_MST_PK and l.location_mst_pk=umt.default_location_fk ";
                SQL += "  AND ((UMT.DEFAULT_LOCATION_FK = l.location_mst_pk and JC.JC_AUTO_MANUAL = 0) OR (JC.PORT_MST_POD_FK  ";
                SQL += "  IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK= l.location_mst_pk)  and JC.JC_AUTO_MANUAL = 1)) ";

                SQL += "  AND SH.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)";
                SQL += " AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))";

                if (Report == 4)
                {
                    SQL += " and JC.PYMT_TYPE=1";
                }
                else if (Report == 5)
                {
                    SQL += " and JC.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and JC.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SH.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POD_FK=" + podpk;
                }
            }
            try
            {
                //Return objWF.GetDataSet(SQL & strCondition)

                return objWF.GetDataSet(SQL);
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

        //created by thiyagarajan on 8/11/08 for Export to Excel
        //this block modifing by thiyagarajan on 16/4/09 for introducing grid functionality
        public DataSet GetSalesRptExcel(string polpk, string podpk, string emppk, string custpk, Int32 rptvalue, int reportType, string Loc, string LocFK, string fromDate, string toDate,
        int isPPD = 0, int isSPL = 0, Int32 Report = 2, string Process = "")
        {
            WorkFlow objWF = new WorkFlow();
            string SQL = null;
            string deprtmnt = null;
            string deprtmnts = null;
            string strCondition = " ";
            if (isPPD == 1 | isPPD == 2)
            {
                strCondition = strCondition + " AND J.PYMT_TYPE = " + isPPD;
            }
            strCondition = strCondition + " ORDER BY J.JOBCARD_DATE DESC ";
            if (isSPL == 1)
            {
                strCondition = strCondition + " , SALES_REPORTER";
            }
            if (Report == 2 | Report == 3)
            {
                deprtmnt = "select  ";
                deprtmnts = " '" + Process + "' AS DEPARTMENT, ";
            }
            else
            {
                deprtmnt = "select '" + Process + "' AS DEPARTMENT,";
                deprtmnts = " ";
            }
            // SeaExport
            if (reportType == 1)
            {
                SQL = deprtmnt;
                SQL += "  TO_CHAR(J.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH,";
                SQL += "  TO_CHAR(J.JOBCARD_DATE, dateformat) JOBCARD_DATE,";
                SQL += "       J.JOBCARD_REF_NO REF_NUMBER," + deprtmnts;
                SQL += "       SHIPPER.CUSTOMER_NAME SHIPPER_NAME,";
                SQL += "       CONSIGNEE.CUSTOMER_NAME CONSIGNEE_NAME,";
                SQL += "       DECODE(J.PYMT_TYPE, 1, 'Y', 2, 'N') PAYMENT_TYPE,";
                SQL += "       POL.PORT_ID POL,";
                SQL += "       POD.PORT_ID POD,";
                SQL += "       (CASE";
                SQL += "         WHEN BOOK.CARGO_TYPE = 1 THEN";
                SQL += "          (SELECT SUM(NVL(CONT.NET_WEIGHT, 0))";
                SQL += "             FROM JOB_TRN_CONT CONT";
                SQL += "            WHERE CONT.JOB_CARD_TRN_FK = J.JOB_CARD_TRN_PK)";
                SQL += "         ELSE";
                SQL += "          (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0))";
                SQL += "             FROM JOB_TRN_CONT CONT";
                SQL += "            WHERE CONT.JOB_CARD_TRN_FK = J.JOB_CARD_TRN_PK)";
                SQL += "       END) WEIGHT_VOLUME,";
                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + " ,4)) FREIGHT, ";

                if (rptvalue != 1 & rptvalue != 8)
                {
                    SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.JOB_CARD_TRN_PK, ";
                    SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                    SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + " ,5)) ESTIMATED_REVENUE, ";
                }
                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + " ,1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + " ,2)) ACTUAL_COST, ";
                if (rptvalue == 1 | rptvalue == 8)
                {
                    SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.JOB_CARD_TRN_PK, ";
                    SQL += "       /* (select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                    SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + " ,5)) ESTIMATED_REVENUE, ";
                }
                SQL += "       EMP.EMPLOYEE_NAME SALES_REPORTER";
                SQL += "  from job_card_trn J,";
                SQL += "       BOOKING_MST_TBL      BOOK,";
                SQL += "       CUSTOMER_MST_TBL     SHIPPER,";
                SQL += "       CUSTOMER_MST_TBL     CONSIGNEE,";
                SQL += "       PORT_MST_TBL         POL,";
                SQL += "       PORT_MST_TBL         POD,";
                SQL += "       EMPLOYEE_MST_TBL     EMP";
                SQL = SQL + "       ,USER_MST_TBL         USR ,location_mst_tbl l ";
                SQL = SQL + " WHERE J.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)";
                SQL += "   AND J.BOOKING_MST_FK = BOOK.BOOKING_MST_PK";
                SQL += "   AND J.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)";
                SQL += "   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK";
                SQL += "   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK";
                SQL += "   AND POL.location_mst_fk =l.location_mst_pk and l.location_mst_pk=usr.default_location_fk  ";
                SQL += "   AND SHIPPER.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+) ";
                SQL += "   AND J.BUSINESS_TYPE=2 AND J.PROCESS_TYPE=1 ";
                SQL += "   AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))";
                SQL = SQL + "   AND j.created_by_fk = usr.user_mst_pk ";
                if (rptvalue == 4)
                {
                    SQL += " and J.PYMT_TYPE=1";
                }
                else if (rptvalue == 5)
                {
                    SQL += " and J.PYMT_TYPE=2";
                }
                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and J.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SHIPPER.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POD_FK=" + podpk;
                }
                SQL = SQL + " order by J.JOBCARD_DATE DESC , J.JOBCARD_REF_NO DESC";
                //Sea Import
            }
            else if (reportType == 2)
            {
                SQL += deprtmnt;
                SQL += "TO_CHAR(Jc.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH,";
                SQL += "TO_CHAR(Jc.JOBCARD_DATE, dateformat) JOBCARD_DATE,";
                SQL += "Jc.JOBCARD_REF_NO REF_NUMBER," + deprtmnts;
                SQL += "sh.CUSTOMER_NAME SHIPPER_NAME,";
                SQL += "co.CUSTOMER_NAME CONSIGNEE_NAME,";
                SQL += "DECODE(Jc.PYMT_TYPE, 1, 'Y', 2, 'N') PAYMENT_TYPE,";
                SQL += "POL.PORT_ID POL,";
                SQL += "POD.PORT_ID POD,";
                SQL += "(CASE WHEN jc.CARGO_TYPE = 1 THEN (SELECT SUM(NVL(CONT.NET_WEIGHT, 0)) FROM JOB_TRN_CONT CONT WHERE CONT.JOB_CARD_TRN_FK = Jc.JOB_CARD_TRN_PK) ELSE (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0)) FROM JOB_TRN_CONT CONT WHERE CONT.JOB_CARD_TRN_FK = Jc.JOB_CARD_TRN_PK) END) WEIGHT_VOLUME,";
                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.JOB_CARD_TRN_PK, ";
                SQL += "      /* (select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + " ,4)) FREIGHT, ";
                if (rptvalue != 1 & rptvalue != 8)
                {
                    SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.JOB_CARD_TRN_PK, ";
                    SQL += "      /* (select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                    SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + " ,5)) ESTIMATED_REVENUE, ";
                }
                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + " ,1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST ";

                if (rptvalue == 1 | rptvalue == 8)
                {
                    SQL += "     ,  (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.JOB_CARD_TRN_PK, ";
                    SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                    SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE ";
                }

                SQL += "   , '' SALES_REPORTER ";
                SQL += "from JOB_CARD_TRN jC,";
                SQL += "       CUSTOMER_MST_TBL     CO,CUSTOMER_MST_TBL     SH,";
                SQL += "       PORT_MST_TBL         POL,";
                SQL += "       PORT_MST_TBL         POD,";
                SQL += "       AGENT_MST_TBL        POLA,";
                SQL += "       EMPLOYEE_MST_TBL     EMP,location_mst_tbl l,";
                SQL += "       USER_MST_TBL UMT";
                SQL += "where JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)";
                SQL += "AND sh.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)";
                SQL += "AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)";
                SQL += "AND JC.PORT_MST_POL_FK = POL.PORT_MST_PK";
                SQL += "AND JC.PORT_MST_POD_FK = POD.PORT_MST_PK";
                SQL += "AND JC.POL_AGENT_MST_FK = POLA.AGENT_MST_PK(+)";
                SQL += "AND JC.BUSINESS_TYPE=2 AND JC.PROCESS_TYPE=2";
                SQL += "  AND jc.CREATED_BY_FK = UMT.USER_MST_PK and l.location_mst_pk=umt.default_location_fk ";
                SQL += "  AND ((UMT.DEFAULT_LOCATION_FK = l.location_mst_pk and JC.JC_AUTO_MANUAL = 0) OR (JC.PORT_MST_POD_FK  ";
                SQL += "  IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK= l.location_mst_pk)  and JC.JC_AUTO_MANUAL = 1)) ";
                SQL += "AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS = 1)";
                SQL += "   AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))";

                if (rptvalue == 4)
                {
                    SQL += " and JC.PYMT_TYPE=1";
                }
                else if (rptvalue == 5)
                {
                    SQL += " and JC.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and JC.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SH.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POD_FK=" + podpk;
                }
                SQL += " ORDER BY JOBCARD_DATE DESC, JOBCARD_REF_NO DESC";
                //Air Export
            }
            else if (reportType == 3)
            {
                SQL = deprtmnt;
                SQL += "       TO_CHAR(J.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH,";
                SQL += "       TO_CHAR(J.JOBCARD_DATE, dateformat) JOBCARD_DATE,";
                SQL += "       J.JOBCARD_REF_NO REF_NUMBER," + deprtmnts;
                SQL += "       SHIPPER.CUSTOMER_NAME SHIPPER_NAME,";
                SQL += "       CONSIGNEE.CUSTOMER_NAME CONSIGNEE_NAME,";
                SQL += "       DECODE(J.PYMT_TYPE, 1, 'Y', 2, 'N') PAYMENT_TYPE,";
                SQL += "       POL.PORT_ID POL,";
                SQL += "       POD.PORT_ID POD,";
                SQL += "       (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0))";
                SQL += "             FROM JOB_TRN_CONT CONT";
                SQL += "            WHERE CONT.JOB_CARD_TRN_FK = J.JOB_CARD_TRN_PK";
                SQL += "       ) WEIGHT_VOLUME,";
                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4)) FREIGHT, ";
                if (rptvalue != 1 & rptvalue != 8)
                {
                    SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.JOB_CARD_TRN_PK, ";
                    SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                    SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";
                }
                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST, ";
                if (rptvalue == 1 | rptvalue == 8)
                {
                    SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.JOB_CARD_TRN_PK, ";
                    SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                    SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";
                }
                SQL += "       EMP.EMPLOYEE_NAME SALES_REPORTER";
                SQL += "  from JOB_CARD_TRN J,";
                SQL += "       BOOKING_MST_TBL      BOOK,";
                SQL += "       CUSTOMER_MST_TBL     SHIPPER,";
                SQL += "       CUSTOMER_MST_TBL     CONSIGNEE,";
                SQL += "       PORT_MST_TBL         POL,";
                SQL += "       PORT_MST_TBL         POD,";
                SQL += "       EMPLOYEE_MST_TBL     EMP";
                SQL = SQL + "       ,USER_MST_TBL USR,location_mst_tbl l  ";
                SQL = SQL + " WHERE J.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)";
                SQL += "   AND J.BOOKING_MST_FK = BOOK.BOOKING_MST_PK";
                SQL += "   AND J.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)";
                SQL += "   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK";
                SQL += "   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK";
                SQL += "   AND POL.location_mst_fk =l.location_mst_pk  and l.location_mst_pk=usr.default_location_fk ";
                SQL += "   AND SHIPPER.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+) ";
                SQL += "   AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))";
                SQL += "   AND J.BUSINESS_TYPE=1 AND J.PROCESS_TYPE=1";
                if (rptvalue == 4)
                {
                    SQL += " and J.PYMT_TYPE=1";
                }
                else if (rptvalue == 5)
                {
                    SQL += " and J.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and J.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SHIPPER.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POD_FK=" + podpk;
                }
                SQL = SQL + "   AND j.created_by_fk = usr.user_mst_pk ";
                SQL = SQL + " order by J.JOBCARD_DATE DESC , J.JOBCARD_REF_NO DESC";
                //Air Import
            }
            else if (reportType == 4)
            {
                SQL = deprtmnt;
                SQL += " TO_CHAR(Jc.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH,";
                SQL += " TO_CHAR(Jc.JOBCARD_DATE, dateformat) JOBCARD_DATE,";
                SQL += " Jc.JOBCARD_REF_NO REF_NUMBER," + deprtmnts;
                SQL += " SH.CUSTOMER_NAME SHIPPER_NAME,";
                SQL += " CO.CUSTOMER_NAME CONSIGNEE_NAME,";
                SQL += " DECODE(Jc.PYMT_TYPE, 1, 'Y', 2, 'N') PAYMENT_TYPE,";
                SQL += " POL.PORT_ID POL,";
                SQL += " POD.PORT_ID POD,";
                SQL += " (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0))";
                SQL += "    FROM JOB_TRN_CONT CONT";
                SQL += "   WHERE CONT.JOB_CARD_TRN_FK = Jc.JOB_CARD_TRN_PK) WEIGHT_VOLUME,";
                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4)) FREIGHT, ";
                if (rptvalue != 1 & rptvalue != 8)
                {
                    SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                    SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                    SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";
                }
                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST, ";

                if (rptvalue == 1 | rptvalue == 8)
                {
                    SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                    SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                    SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";
                }
                SQL += "  '' SALES_REPORTER ";
                SQL += "  from JOB_CARD_TRN JC,";
                SQL += "  CUSTOMER_MST_TBL     SH,";
                SQL += "  CUSTOMER_MST_TBL     CO,";
                SQL += "  PORT_MST_TBL         POL,";
                SQL += "  PORT_MST_TBL         POD,";
                SQL += "  AGENT_MST_TBL        POLA,";
                SQL += "  USER_MST_TBL         UMT,location_mst_tbl l,";
                SQL += "  EMPLOYEE_MST_TBL     EMP";
                SQL += "  where JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)";
                SQL += "  AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)";
                SQL += "  AND JC.PORT_MST_POL_FK = POL.PORT_MST_PK";
                SQL += "  AND JC.PORT_MST_POD_FK = POD.PORT_MST_PK";
                SQL += "  AND JC.POL_AGENT_MST_FK = POLA.AGENT_MST_PK(+)";
                SQL += " AND jc.CREATED_BY_FK = UMT.USER_MST_PK";
                SQL += " AND ((UMT.DEFAULT_LOCATION_FK = l.location_mst_pk and JC.JC_AUTO_MANUAL = 0) OR (JC.PORT_MST_POD_FK  ";
                SQL += " IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK= l.location_mst_pk)  and JC.JC_AUTO_MANUAL = 1)) ";
                SQL += " AND SH.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)";
                SQL += "AND SH.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)";
                SQL += "AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS = 1)";
                SQL += "  and l.location_mst_pk = umt.default_location_fk ";
                SQL += " AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))";
                SQL += "   AND JC.BUSINESS_TYPE=1 AND JC.PROCESS_TYPE=2";
                if (rptvalue == 4)
                {
                    SQL += " and JC.PYMT_TYPE=1";
                }
                else if (rptvalue == 5)
                {
                    SQL += " and JC.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and JC.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SH.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POD_FK=" + podpk;
                }
                SQL += " ORDER BY JOBCARD_DATE DESC, JOBCARD_REF_NO DESC";
                //Air Export-Import
            }
            else if (reportType == 5)
            {
                SQL = deprtmnt;
                SQL += "       TO_CHAR(J.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH,";
                SQL += "       TO_CHAR(J.JOBCARD_DATE, dateformat) JOBCARD_DATE,";
                SQL += "       J.JOBCARD_REF_NO REF_NUMBER," + deprtmnts;
                SQL += "       SHIPPER.CUSTOMER_NAME SHIPPER_NAME,";
                SQL += "       CONSIGNEE.CUSTOMER_NAME CONSIGNEE_NAME,";
                SQL += "       DECODE(J.PYMT_TYPE, 1, 'Y', 2, 'N') PAYMENT_TYPE,";
                SQL += "       POL.PORT_ID POL,";
                SQL += "       POD.PORT_ID POD,";
                SQL += "       (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0))";
                SQL += "             FROM JOB_TRN_CONT CONT";
                SQL += "            WHERE CONT.JOB_CARD_TRN_FK = J.JOB_CARD_TRN_PK";
                SQL += "       ) WEIGHT_VOLUME,";
                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4)) FREIGHT, ";
                if (rptvalue != 1 & rptvalue != 8)
                {
                    SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.JOB_CARD_TRN_PK, ";
                    SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                    SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";
                }
                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST, ";
                if (rptvalue == 1 | rptvalue == 8)
                {
                    SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.JOB_CARD_TRN_PK, ";
                    SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                    SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";
                }
                SQL += "       EMP.EMPLOYEE_NAME SALES_REPORTER";
                SQL += "  from JOB_CARD_TRN J,";
                SQL += "       BOOKING_MST_TBL      BOOK,";
                SQL += "       CUSTOMER_MST_TBL     SHIPPER,";
                SQL += "       CUSTOMER_MST_TBL     CONSIGNEE,";
                SQL += "       PORT_MST_TBL         POL,";
                SQL += "       PORT_MST_TBL         POD,";
                SQL += "       EMPLOYEE_MST_TBL     EMP";
                SQL = SQL + "       ,USER_MST_TBL USR,location_mst_tbl l  ";
                SQL = SQL + " WHERE J.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)";
                SQL += "   AND J.BOOKING_MST_FK = BOOK.BOOKING_MST_PK";
                SQL += "   AND J.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)";
                SQL += "   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK";
                SQL += "   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK";
                SQL += "   AND POL.location_mst_fk =l.location_mst_pk  and l.location_mst_pk=usr.default_location_fk ";
                SQL += "   AND SHIPPER.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+) ";
                SQL += "   AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))";
                SQL += "   AND J.BUSINESS_TYPE=1 AND J.PROCESS_TYPE=2";
                if (rptvalue == 4)
                {
                    SQL += " and J.PYMT_TYPE=1";
                }
                else if (rptvalue == 5)
                {
                    SQL += " and J.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and J.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SHIPPER.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POD_FK=" + podpk;
                }

                SQL = SQL + "   AND j.created_by_fk = usr.user_mst_pk ";
                ///''''''''''''''''
                SQL += " UNION ";
                ///''''''''''''''''
                //AIr Import
                SQL += deprtmnt;
                SQL += " TO_CHAR(Jc.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH,";
                SQL += " TO_CHAR(Jc.JOBCARD_DATE, dateformat) JOBCARD_DATE,";
                SQL += " Jc.JOBCARD_REF_NO REF_NUMBER," + deprtmnts;
                SQL += " SH.CUSTOMER_NAME SHIPPER_NAME,";
                SQL += " CO.CUSTOMER_NAME CONSIGNEE_NAME,";
                SQL += " DECODE(Jc.PYMT_TYPE, 1, 'Y', 2, 'N') PAYMENT_TYPE,";
                SQL += " POL.PORT_ID POL,";
                SQL += " POD.PORT_ID POD,";
                SQL += " (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0))";
                SQL += "    FROM JOB_TRN_CONT CONT";
                SQL += "   WHERE CONT.JOB_CARD_TRN_FK = Jc.job_card_trn_pk) WEIGHT_VOLUME,";
                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4)) FREIGHT, ";
                if (rptvalue != 1 & rptvalue != 8)
                {
                    SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                    SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                    SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";
                }
                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST, ";

                if (rptvalue == 1 | rptvalue == 8)
                {
                    SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                    SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                    SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";
                }
                SQL += "  '' SALES_REPORTER ";
                SQL += "  from JOB_CARD_TRN JC,";
                SQL += "  CUSTOMER_MST_TBL     SH,";
                SQL += "  CUSTOMER_MST_TBL     CO,";
                SQL += "  PORT_MST_TBL         POL,";
                SQL += "  PORT_MST_TBL         POD,";
                SQL += "  AGENT_MST_TBL        POLA,";
                SQL += "  USER_MST_TBL         UMT,location_mst_tbl l,";
                SQL += "  EMPLOYEE_MST_TBL     EMP";
                SQL += "  where JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)";
                SQL += "  AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)";
                SQL += "  AND JC.PORT_MST_POL_FK = POL.PORT_MST_PK";
                SQL += "  AND JC.PORT_MST_POD_FK = POD.PORT_MST_PK";
                SQL += "  AND JC.POL_AGENT_MST_FK = POLA.AGENT_MST_PK(+)";
                SQL += " AND jc.CREATED_BY_FK = UMT.USER_MST_PK";
                SQL += " AND ((UMT.DEFAULT_LOCATION_FK = l.location_mst_pk and JC.JC_AUTO_MANUAL = 0) OR (JC.PORT_MST_POD_FK  ";
                SQL += " IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK= l.location_mst_pk)  and JC.JC_AUTO_MANUAL = 1)) ";
                SQL += " AND SH.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)";
                SQL += "AND SH.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)";
                SQL += "AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS = 1)";
                SQL += "  and l.location_mst_pk = umt.default_location_fk ";
                SQL += " AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))";
                SQL += "   AND JC.BUSINESS_TYPE=1 AND JC.PROCESS_TYPE=2";
                if (rptvalue == 4)
                {
                    SQL += " and JC.PYMT_TYPE=1";
                }
                else if (rptvalue == 5)
                {
                    SQL += " and JC.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and JC.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SH.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POD_FK=" + podpk;
                }
                //Sea Export-Import
            }
            else if (reportType == 6)
            {
                //Sea Export
                SQL = deprtmnt;
                SQL += "  TO_CHAR(J.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH,";
                SQL += "  TO_CHAR(J.JOBCARD_DATE, dateformat) JOBCARD_DATE,";
                SQL += "       J.JOBCARD_REF_NO REF_NUMBER," + deprtmnts;
                SQL += "       SHIPPER.CUSTOMER_NAME SHIPPER_NAME,";
                SQL += "       CONSIGNEE.CUSTOMER_NAME CONSIGNEE_NAME,";
                SQL += "       DECODE(J.PYMT_TYPE, 1, 'Y', 2, 'N') PAYMENT_TYPE,";
                SQL += "       POL.PORT_ID POL,";
                SQL += "       POD.PORT_ID POD,";
                SQL += "       (CASE";
                SQL += "         WHEN BOOK.CARGO_TYPE = 1 THEN";
                SQL += "          (SELECT SUM(NVL(CONT.NET_WEIGHT, 0))";
                SQL += "             FROM JOB_TRN_CONT CONT";
                SQL += "            WHERE CONT.JOB_CARD_TRN_FK = J.JOB_CARD_TRN_PK)";
                SQL += "         ELSE";
                SQL += "          (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0))";
                SQL += "             FROM JOB_TRN_CONT CONT";
                SQL += "            WHERE CONT.JOB_CARD_TRN_FK = J.JOB_CARD_TRN_PK)";
                SQL += "       END) WEIGHT_VOLUME,";
                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + " ,4)) FREIGHT, ";

                if (rptvalue != 1 & rptvalue != 8)
                {
                    SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.JOB_CARD_TRN_PK, ";
                    SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                    SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + " ,5)) ESTIMATED_REVENUE, ";
                }
                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + " ,1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + " ,2)) ACTUAL_COST, ";
                if (rptvalue == 1 | rptvalue == 8)
                {
                    SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.JOB_CARD_TRN_PK, ";
                    SQL += "       /* (select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                    SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + " ,5)) ESTIMATED_REVENUE, ";
                }
                SQL += "       EMP.EMPLOYEE_NAME SALES_REPORTER";
                SQL += "  from job_card_trn J,";
                SQL += "       BOOKING_MST_TBL      BOOK,";
                SQL += "       CUSTOMER_MST_TBL     SHIPPER,";
                SQL += "       CUSTOMER_MST_TBL     CONSIGNEE,";
                SQL += "       PORT_MST_TBL         POL,";
                SQL += "       PORT_MST_TBL         POD,";
                SQL += "       EMPLOYEE_MST_TBL     EMP";
                SQL = SQL + "       ,USER_MST_TBL         USR ,location_mst_tbl l ";
                SQL = SQL + " WHERE J.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)";
                SQL += "   AND J.BOOKING_MST_FK = BOOK.BOOKING_MST_PK";
                SQL += "   AND J.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)";
                SQL += "   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK";
                SQL += "   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK";
                SQL += "   AND POL.location_mst_fk =l.location_mst_pk and l.location_mst_pk=usr.default_location_fk  ";
                SQL += "   AND SHIPPER.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+) ";
                SQL += "   AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))";
                SQL += "   AND J.BUSINESS_TYPE=2 AND J.PROCESS_TYPE=1";
                SQL = SQL + "   AND j.created_by_fk = usr.user_mst_pk ";
                if (rptvalue == 4)
                {
                    SQL += " and J.PYMT_TYPE=1";
                }
                else if (rptvalue == 5)
                {
                    SQL += " and J.PYMT_TYPE=2";
                }
                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and J.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SHIPPER.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POD_FK=" + podpk;
                }

                ///''''''''''''''''
                SQL += " UNION ";

                ///''''''''''''''''''''
                //Sea Import
                SQL += deprtmnt;
                SQL += "TO_CHAR(Jc.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH,";
                SQL += "TO_CHAR(Jc.JOBCARD_DATE, dateformat) JOBCARD_DATE,";
                SQL += "Jc.JOBCARD_REF_NO REF_NUMBER," + deprtmnts;
                SQL += "sh.CUSTOMER_NAME SHIPPER_NAME,";
                SQL += "co.CUSTOMER_NAME CONSIGNEE_NAME,";
                SQL += "DECODE(Jc.PYMT_TYPE, 1, 'Y', 2, 'N') PAYMENT_TYPE,";
                SQL += "POL.PORT_ID POL,";
                SQL += "POD.PORT_ID POD,";
                SQL += "(CASE WHEN jc.CARGO_TYPE = 1 THEN (SELECT SUM(NVL(CONT.NET_WEIGHT, 0)) FROM JOB_TRN_CONT CONT WHERE CONT.JOB_CARD_TRN_FK = Jc.JOB_CARD_TRN_PK) ELSE (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0)) FROM JOB_TRN_CONT CONT WHERE CONT.JOB_CARD_TRN_FK = Jc.JOB_CARD_TRN_PK) END) WEIGHT_VOLUME,";
                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.JOB_CARD_TRN_PK, ";
                SQL += "      /* (select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + " ,4)) FREIGHT, ";

                if (rptvalue != 1 & rptvalue != 8)
                {
                    SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.JOB_CARD_TRN_PK, ";
                    SQL += "      /* (select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                    SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + " ,5)) ESTIMATED_REVENUE, ";
                }
                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + " ,1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST ";

                if (rptvalue == 1 | rptvalue == 8)
                {
                    SQL += "     ,  (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.JOB_CARD_TRN_PK, ";
                    SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                    SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE ";
                }

                SQL += "   , '' SALES_REPORTER ";
                SQL += "from JOB_CARD_TRN jC,";
                SQL += "       CUSTOMER_MST_TBL     CO,CUSTOMER_MST_TBL     SH,";
                SQL += "       PORT_MST_TBL         POL,";
                SQL += "       PORT_MST_TBL         POD,";
                SQL += "       AGENT_MST_TBL        POLA,";
                SQL += "       EMPLOYEE_MST_TBL     EMP,location_mst_tbl l,";
                SQL += "       USER_MST_TBL UMT";
                SQL += "where JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)";
                SQL += "AND sh.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)";
                SQL += "AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)";
                SQL += "AND JC.PORT_MST_POL_FK = POL.PORT_MST_PK";
                SQL += "AND JC.PORT_MST_POD_FK = POD.PORT_MST_PK";
                SQL += "AND JC.POL_AGENT_MST_FK = POLA.AGENT_MST_PK(+)";
                SQL += "  AND jc.CREATED_BY_FK = UMT.USER_MST_PK and l.location_mst_pk=umt.default_location_fk ";
                SQL += "  AND ((UMT.DEFAULT_LOCATION_FK = l.location_mst_pk and JC.JC_AUTO_MANUAL = 0) OR (JC.PORT_MST_POD_FK  ";
                SQL += "  IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK= l.location_mst_pk)  and JC.JC_AUTO_MANUAL = 1)) ";

                SQL += "AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS = 1)";
                SQL += "   AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))";
                SQL += "   AND JC.BUSINESS_TYPE=2 AND JC.PROCESS_TYPE=2";
                if (rptvalue == 4)
                {
                    SQL += " and JC.PYMT_TYPE=1";
                }
                else if (rptvalue == 5)
                {
                    SQL += " and JC.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and JC.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SH.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POD_FK=" + podpk;
                }
                //Air-Sea Export
            }
            else if (reportType == 7)
            {
                //'Air Export
                SQL = deprtmnt;
                SQL += "       TO_CHAR(J.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH,";
                SQL += "       TO_CHAR(J.JOBCARD_DATE, dateformat) JOBCARD_DATE,";
                SQL += "       J.JOBCARD_REF_NO REF_NUMBER," + deprtmnts;
                SQL += "       SHIPPER.CUSTOMER_NAME SHIPPER_NAME,";
                SQL += "       CONSIGNEE.CUSTOMER_NAME CONSIGNEE_NAME,";
                SQL += "       DECODE(J.PYMT_TYPE, 1, 'Y', 2, 'N') PAYMENT_TYPE,";
                SQL += "       POL.PORT_ID POL,";
                SQL += "       POD.PORT_ID POD,";
                SQL += "       (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0))";
                SQL += "             FROM JOB_TRN_CONT CONT";
                SQL += "            WHERE CONT.JOB_CARD_TRN_FK = J.JOB_CARD_TRN_PK";
                SQL += "       ) WEIGHT_VOLUME,";
                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4)) FREIGHT, ";
                if (rptvalue != 1 & rptvalue != 8)
                {
                    SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.JOB_CARD_TRN_PK, ";
                    SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                    SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";
                }
                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST, ";
                if (rptvalue == 1 | rptvalue == 8)
                {
                    SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.JOB_CARD_TRN_PK, ";
                    SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                    SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";
                }
                SQL += "       EMP.EMPLOYEE_NAME SALES_REPORTER";
                SQL += "  from JOB_CARD_TRN J,";
                SQL += "       BOOKING_MST_TBL      BOOK,";
                SQL += "       CUSTOMER_MST_TBL     SHIPPER,";
                SQL += "       CUSTOMER_MST_TBL     CONSIGNEE,";
                SQL += "       PORT_MST_TBL         POL,";
                SQL += "       PORT_MST_TBL         POD,";
                SQL += "       EMPLOYEE_MST_TBL     EMP";
                SQL = SQL + "       ,USER_MST_TBL USR,location_mst_tbl l  ";
                SQL = SQL + " WHERE J.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)";
                SQL += "   AND J.BOOKING_MST_FK = BOOK.BOOKING_MST_PK";
                SQL += "   AND J.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)";
                SQL += "   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK";
                SQL += "   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK";
                SQL += "   AND POL.location_mst_fk =l.location_mst_pk  and l.location_mst_pk=usr.default_location_fk ";
                SQL += "   AND SHIPPER.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+) ";
                SQL += "   AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))";
                SQL += "   AND J.BUSINESS_TYPE=1 AND J.PROCESS_TYPE=1";
                if (rptvalue == 4)
                {
                    SQL += " and J.PYMT_TYPE=1";
                }
                else if (rptvalue == 5)
                {
                    SQL += " and J.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and J.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SHIPPER.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POD_FK=" + podpk;
                }

                SQL = SQL + "   AND j.created_by_fk = usr.user_mst_pk ";
                ///''
                SQL += " UNION ";
                ///
                //Sea Export

                SQL += deprtmnt;
                SQL += "  TO_CHAR(J.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH,";
                SQL += "  TO_CHAR(J.JOBCARD_DATE, dateformat) JOBCARD_DATE,";
                SQL += "       J.JOBCARD_REF_NO REF_NUMBER," + deprtmnts;
                SQL += "       SHIPPER.CUSTOMER_NAME SHIPPER_NAME,";
                SQL += "       CONSIGNEE.CUSTOMER_NAME CONSIGNEE_NAME,";
                SQL += "       DECODE(J.PYMT_TYPE, 1, 'Y', 2, 'N') PAYMENT_TYPE,";
                SQL += "       POL.PORT_ID POL,";
                SQL += "       POD.PORT_ID POD,";
                SQL += "       (CASE";
                SQL += "         WHEN BOOK.CARGO_TYPE = 1 THEN";
                SQL += "          (SELECT SUM(NVL(CONT.NET_WEIGHT, 0))";
                SQL += "             FROM JOB_TRN_CONT CONT";
                SQL += "            WHERE CONT.JOB_CARD_TRN_FK = J.JOB_CARD_TRN_PK)";
                SQL += "         ELSE";
                SQL += "          (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0))";
                SQL += "             FROM JOB_TRN_CONT CONT";
                SQL += "            WHERE CONT.JOB_CARD_TRN_FK = J.JOB_CARD_TRN_PK)";
                SQL += "       END) WEIGHT_VOLUME,";
                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + " ,4)) FREIGHT, ";

                if (rptvalue != 1 & rptvalue != 8)
                {
                    SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.JOB_CARD_TRN_PK, ";
                    SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                    SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + " ,5)) ESTIMATED_REVENUE, ";
                }

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + " ,1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + " ,2)) ACTUAL_COST, ";
                if (rptvalue == 1 | rptvalue == 8)
                {
                    SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.JOB_CARD_TRN_PK, ";
                    SQL += "       /* (select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                    SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + " ,5)) ESTIMATED_REVENUE, ";
                }
                SQL += "       EMP.EMPLOYEE_NAME SALES_REPORTER";
                SQL += "  from job_card_trn J,";
                SQL += "       BOOKING_MST_TBL      BOOK,";
                SQL += "       CUSTOMER_MST_TBL     SHIPPER,";
                SQL += "       CUSTOMER_MST_TBL     CONSIGNEE,";
                SQL += "       PORT_MST_TBL         POL,";
                SQL += "       PORT_MST_TBL         POD,";
                SQL += "       EMPLOYEE_MST_TBL     EMP";
                SQL = SQL + "       ,USER_MST_TBL         USR ,location_mst_tbl l ";
                SQL = SQL + " WHERE J.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)";
                SQL += "   AND J.BOOKING_MST_FK = BOOK.BOOKING_MST_PK";
                SQL += "   AND J.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)";
                SQL += "   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK";
                SQL += "   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK";
                SQL += "   AND POL.location_mst_fk =l.location_mst_pk and l.location_mst_pk=usr.default_location_fk  ";
                SQL += "   AND SHIPPER.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+) ";
                SQL += "   AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))";
                SQL += "   AND J.BUSINESS_TYPE=2 AND J.PROCESS_TYPE=1";

                SQL = SQL + "   AND j.created_by_fk = usr.user_mst_pk ";
                if (rptvalue == 4)
                {
                    SQL += " and J.PYMT_TYPE=1";
                }
                else if (rptvalue == 5)
                {
                    SQL += " and J.PYMT_TYPE=2";
                }
                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and J.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SHIPPER.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POD_FK=" + podpk;
                }
                //Air-Sea Import
            }
            else if (reportType == 8)
            {
                //Air Import
                SQL = deprtmnt;
                SQL += " TO_CHAR(Jc.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH,";
                SQL += " TO_CHAR(Jc.JOBCARD_DATE, dateformat) JOBCARD_DATE,";
                SQL += " Jc.JOBCARD_REF_NO REF_NUMBER," + deprtmnts;
                SQL += " SH.CUSTOMER_NAME SHIPPER_NAME,";
                SQL += " CO.CUSTOMER_NAME CONSIGNEE_NAME,";
                SQL += " DECODE(Jc.PYMT_TYPE, 1, 'Y', 2, 'N') PAYMENT_TYPE,";
                SQL += " POL.PORT_ID POL,";
                SQL += " POD.PORT_ID POD,";
                SQL += " (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0))";
                SQL += "    FROM JOB_TRN_CONT CONT";
                SQL += "   WHERE CONT.JOB_CARD_TRN_FK = Jc.job_card_trn_pk) WEIGHT_VOLUME,";
                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4)) FREIGHT, ";

                if (rptvalue != 1 & rptvalue != 8)
                {
                    SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                    SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                    SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";
                }
                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST, ";

                if (rptvalue == 1 | rptvalue == 8)
                {
                    SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                    SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                    SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";
                }
                SQL += "  '' SALES_REPORTER ";
                SQL += "  from JOB_CARD_TRN JC,";
                SQL += "  CUSTOMER_MST_TBL     SH,";
                SQL += "  CUSTOMER_MST_TBL     CO,";
                SQL += "  PORT_MST_TBL         POL,";
                SQL += "  PORT_MST_TBL         POD,";
                SQL += "  AGENT_MST_TBL        POLA,";
                SQL += "  USER_MST_TBL         UMT,location_mst_tbl l,";
                SQL += "  EMPLOYEE_MST_TBL     EMP";
                SQL += "  where JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)";
                SQL += "  AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)";
                SQL += "  AND JC.PORT_MST_POL_FK = POL.PORT_MST_PK";
                SQL += "  AND JC.PORT_MST_POD_FK = POD.PORT_MST_PK";
                SQL += "  AND JC.POL_AGENT_MST_FK = POLA.AGENT_MST_PK(+)";
                SQL += " AND jc.CREATED_BY_FK = UMT.USER_MST_PK";
                SQL += " AND ((UMT.DEFAULT_LOCATION_FK = l.location_mst_pk and JC.JC_AUTO_MANUAL = 0) OR (JC.PORT_MST_POD_FK  ";
                SQL += " IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK= l.location_mst_pk)  and JC.JC_AUTO_MANUAL = 1)) ";
                SQL += " AND SH.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)";

                SQL += "AND SH.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)";
                SQL += "AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS = 1)";
                SQL += "  and l.location_mst_pk = umt.default_location_fk ";
                SQL += " AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))";
                SQL += "   AND JC.BUSINESS_TYPE=1 AND JC.PROCESS_TYPE=2";

                if (rptvalue == 4)
                {
                    SQL += " and JC.PYMT_TYPE=1";
                }
                else if (rptvalue == 5)
                {
                    SQL += " and JC.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and JC.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SH.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POD_FK=" + podpk;
                }

                ///''''''''''
                SQL += " UNION ";
                ///'''''''''''''''
                //Sea Import

                SQL += deprtmnt;
                SQL += " TO_CHAR(Jc.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH,";
                SQL += " TO_CHAR(Jc.JOBCARD_DATE, dateformat) JOBCARD_DATE,";
                SQL += " Jc.JOBCARD_REF_NO REF_NUMBER," + deprtmnts;
                SQL += " SH.CUSTOMER_NAME SHIPPER_NAME,";
                SQL += " CO.CUSTOMER_NAME CONSIGNEE_NAME,";
                SQL += " DECODE(Jc.PYMT_TYPE, 1, 'Y', 2, 'N') PAYMENT_TYPE,";
                SQL += " POL.PORT_ID POL,";
                SQL += " POD.PORT_ID POD,";
                SQL += " (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0))";
                SQL += "    FROM JOB_TRN_CONT CONT";
                SQL += "   WHERE CONT.JOB_CARD_TRN_FK = Jc.job_card_trn_pk) WEIGHT_VOLUME,";
                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4)) FREIGHT, ";

                if (rptvalue != 1 & rptvalue != 8)
                {
                    SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                    SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                    SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";
                }
                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST, ";

                if (rptvalue == 1 | rptvalue == 8)
                {
                    SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                    SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                    SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";
                }
                SQL += "  '' SALES_REPORTER ";
                SQL += "  from JOB_CARD_TRN JC,";
                SQL += "  CUSTOMER_MST_TBL     SH,";
                SQL += "  CUSTOMER_MST_TBL     CO,";
                SQL += "  PORT_MST_TBL         POL,";
                SQL += "  PORT_MST_TBL         POD,";
                SQL += "  AGENT_MST_TBL        POLA,";
                SQL += "  USER_MST_TBL         UMT,location_mst_tbl l,";
                SQL += "  EMPLOYEE_MST_TBL     EMP";
                SQL += "  where JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)";
                SQL += "  AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)";
                SQL += "  AND JC.PORT_MST_POL_FK = POL.PORT_MST_PK";
                SQL += "  AND JC.PORT_MST_POD_FK = POD.PORT_MST_PK";
                SQL += "  AND JC.POL_AGENT_MST_FK = POLA.AGENT_MST_PK(+)";
                SQL += " AND jc.CREATED_BY_FK = UMT.USER_MST_PK";
                SQL += " AND ((UMT.DEFAULT_LOCATION_FK = l.location_mst_pk and JC.JC_AUTO_MANUAL = 0) OR (JC.PORT_MST_POD_FK  ";
                SQL += " IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK= l.location_mst_pk)  and JC.JC_AUTO_MANUAL = 1)) ";
                SQL += " AND SH.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)";

                SQL += "AND SH.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)";
                SQL += "AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS = 1)";
                SQL += "  and l.location_mst_pk = umt.default_location_fk ";
                SQL += " AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))";
                SQL += "   AND JC.BUSINESS_TYPE=1 AND JC.PROCESS_TYPE=2";

                if (rptvalue == 4)
                {
                    SQL += " and JC.PYMT_TYPE=1";
                }
                else if (rptvalue == 5)
                {
                    SQL += " and JC.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and JC.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SH.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POD_FK=" + podpk;
                }
                ///''''''''''
                //Air-Sea Export-Import
            }
            else if (reportType == 9)
            {
                //'------------------------------------------------------------------------------------------------
                SQL = deprtmnt;
                SQL += "       TO_CHAR(J.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH,";
                SQL += "       TO_CHAR(J.JOBCARD_DATE, dateformat) JOBCARD_DATE,";
                SQL += "       J.JOBCARD_REF_NO REF_NUMBER," + deprtmnts;
                SQL += "       SHIPPER.CUSTOMER_NAME SHIPPER_NAME,";
                SQL += "       CONSIGNEE.CUSTOMER_NAME CONSIGNEE_NAME,";
                SQL += "       DECODE(J.PYMT_TYPE, 1, 'Y', 2, 'N') PAYMENT_TYPE,";
                SQL += "       POL.PORT_ID POL,";
                SQL += "       POD.PORT_ID POD,";
                SQL += "       (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0))";
                SQL += "             FROM JOB_TRN_CONT CONT";
                SQL += "            WHERE CONT.JOB_CARD_TRN_FK = J.JOB_CARD_TRN_PK";
                SQL += "       ) WEIGHT_VOLUME,";
                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4)) FREIGHT, ";

                if (rptvalue != 1 & rptvalue != 8)
                {
                    SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.JOB_CARD_TRN_PK, ";
                    SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                    SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";
                }
                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST, ";

                if (rptvalue == 1 | rptvalue == 8)
                {
                    SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_Air(j.JOB_CARD_TRN_PK, ";
                    SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                    SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";
                }
                SQL += "       EMP.EMPLOYEE_NAME SALES_REPORTER";
                SQL += "  from JOB_CARD_TRN J,";
                SQL += "       BOOKING_MST_TBL      BOOK,";
                SQL += "       CUSTOMER_MST_TBL     SHIPPER,";
                SQL += "       CUSTOMER_MST_TBL     CONSIGNEE,";
                SQL += "       PORT_MST_TBL         POL,";
                SQL += "       PORT_MST_TBL         POD,";
                SQL += "       EMPLOYEE_MST_TBL     EMP";
                SQL = SQL + "       ,USER_MST_TBL USR,location_mst_tbl l  ";
                SQL = SQL + " WHERE J.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)";
                SQL += "   AND J.BOOKING_MST_FK = BOOK.BOOKING_MST_PK";
                SQL += "   AND J.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)";
                SQL += "   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK";
                SQL += "   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK";
                SQL += "  /*AND POL.location_mst_fk =l.location_mst_pk */  and l.location_mst_pk=usr.default_location_fk ";
                SQL += "   AND SHIPPER.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+) ";
                SQL += "   AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))";
                SQL += "   AND J.BUSINESS_TYPE=1 AND J.PROCESS_TYPE=1";

                if (rptvalue == 4)
                {
                    SQL += " and J.PYMT_TYPE=1";
                }
                else if (rptvalue == 5)
                {
                    SQL += " and J.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and J.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SHIPPER.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POD_FK=" + podpk;
                }

                SQL = SQL + "   AND j.created_by_fk = usr.user_mst_pk ";
                ///''''''''''''''''
                SQL += " UNION ";
                ///''''''''''''''''
                //AIr Import
                SQL += deprtmnt;
                SQL += " TO_CHAR(Jc.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH,";
                SQL += " TO_CHAR(Jc.JOBCARD_DATE, dateformat) JOBCARD_DATE,";
                SQL += " Jc.JOBCARD_REF_NO REF_NUMBER," + deprtmnts;
                SQL += " SH.CUSTOMER_NAME SHIPPER_NAME,";
                SQL += " CO.CUSTOMER_NAME CONSIGNEE_NAME,";
                SQL += " DECODE(Jc.PYMT_TYPE, 1, 'Y', 2, 'N') PAYMENT_TYPE,";
                SQL += " POL.PORT_ID POL,";
                SQL += " POD.PORT_ID POD,";
                SQL += " (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0))";
                SQL += "    FROM JOB_TRN_CONT CONT";
                SQL += "   WHERE CONT.JOB_CARD_TRN_FK = Jc.job_card_trn_pk) WEIGHT_VOLUME,";
                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4)) FREIGHT, ";

                if (rptvalue != 1 & rptvalue != 8)
                {
                    SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                    SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                    SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";
                }
                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST, ";

                if (rptvalue == 1 | rptvalue == 8)
                {
                    SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_Air(jc.job_card_trn_pk, ";
                    SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and  ";
                    SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE, ";
                }
                SQL += "  '' SALES_REPORTER ";
                SQL += "  from JOB_CARD_TRN JC, BOOKING_MST_TBL BOOK,";
                SQL += "  CUSTOMER_MST_TBL     SH,";
                SQL += "  CUSTOMER_MST_TBL     CO,";
                SQL += "  PORT_MST_TBL         POL,";
                SQL += "  PORT_MST_TBL         POD,";
                SQL += "  AGENT_MST_TBL        POLA,";
                SQL += "  USER_MST_TBL         UMT,location_mst_tbl l,";
                SQL += "  EMPLOYEE_MST_TBL     EMP";

                SQL = SQL + " WHERE JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)";
                SQL += "   AND JC.BOOKING_MST_FK = BOOK.BOOKING_MST_PK";
                SQL += "   AND JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)";
                SQL += "   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK";
                SQL += "   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK";
                SQL += "   /*AND POL.location_mst_fk =l.location_mst_pk */  and l.location_mst_pk=umt.default_location_fk ";
                SQL += "   AND SH.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+) ";
                SQL += "   AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))";
                SQL += "   AND JC.BUSINESS_TYPE=1 AND JC.PROCESS_TYPE=2";
                //'SQL &= "  where JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)" & vbNewLine
                //'SQL &= "  AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)" & vbNewLine
                //'SQL &= "  AND JC.PORT_MST_POL_FK = POL.PORT_MST_PK" & vbNewLine
                //'SQL &= "  AND JC.PORT_MST_POD_FK = POD.PORT_MST_PK" & vbNewLine
                //'SQL &= "  AND JC.POL_AGENT_MST_FK = POLA.AGENT_MST_PK(+)" & vbNewLine
                //'SQL &= " AND jc.CREATED_BY_FK = UMT.USER_MST_PK" & vbNewLine
                //'SQL &= " AND ((UMT.DEFAULT_LOCATION_FK = l.location_mst_pk and JC.JC_AUTO_MANUAL = 0) OR (JC.PORT_MST_POD_FK  "
                //'SQL &= " IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK= l.location_mst_pk)  and JC.JC_AUTO_MANUAL = 1)) "
                //'SQL &= " AND SH.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)" & vbNewLine
                //'SQL &= "AND SH.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)" & vbNewLine
                //'SQL &= "AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS = 1)" & vbNewLine
                //'SQL &= "  and l.location_mst_pk = umt.default_location_fk "
                //'SQL &= " AND (TO_DATE(JOBCARD_DATE,'" & dateFormat & "') BETWEEN TO_DATE('" & fromDate & "','" & dateFormat & "') AND TO_DATE('" & toDate & "','" & dateFormat & "'))"
                //'SQL &= "   AND JC.BUSINESS_TYPE=1 AND JC.PROCESS_TYPE=2" & vbNewLine

                if (rptvalue == 4)
                {
                    SQL += " and JC.PYMT_TYPE=1";
                }
                else if (rptvalue == 5)
                {
                    SQL += " and JC.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and JC.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SH.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POD_FK=" + podpk;
                }
                ///''''''''''''''''''
                SQL += " UNION ";
                ///''''''''''''''''''
                //Sea Export
                SQL += deprtmnt;
                SQL += "  TO_CHAR(J.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH,";
                SQL += "  TO_CHAR(J.JOBCARD_DATE, dateformat) JOBCARD_DATE,";
                SQL += "       J.JOBCARD_REF_NO REF_NUMBER," + deprtmnts;
                SQL += "       SHIPPER.CUSTOMER_NAME SHIPPER_NAME,";
                SQL += "       CONSIGNEE.CUSTOMER_NAME CONSIGNEE_NAME,";
                SQL += "       DECODE(J.PYMT_TYPE, 1, 'Y', 2, 'N') PAYMENT_TYPE,";
                SQL += "       POL.PORT_ID POL,";
                SQL += "       POD.PORT_ID POD,";
                SQL += "       (CASE";
                SQL += "         WHEN BOOK.CARGO_TYPE = 1 THEN";
                SQL += "          (SELECT SUM(NVL(CONT.NET_WEIGHT, 0))";
                SQL += "             FROM JOB_TRN_CONT CONT";
                SQL += "            WHERE CONT.JOB_CARD_TRN_FK = J.JOB_CARD_TRN_PK)";
                SQL += "         ELSE";
                SQL += "          (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0))";
                SQL += "             FROM JOB_TRN_CONT CONT";
                SQL += "            WHERE CONT.JOB_CARD_TRN_FK = J.JOB_CARD_TRN_PK)";
                SQL += "       END) WEIGHT_VOLUME,";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + " ,4)) FREIGHT, ";

                if (rptvalue != 1 & rptvalue != 8)
                {
                    SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.JOB_CARD_TRN_PK, ";
                    SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                    SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + " ,5)) ESTIMATED_REVENUE, ";
                }

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + " ,1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and  ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + " ,2)) ACTUAL_COST, ";
                if (rptvalue == 1 | rptvalue == 8)
                {
                    SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_sea(j.JOB_CARD_TRN_PK, ";
                    SQL += "       /* (select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk and ";
                    SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + " ,5)) ESTIMATED_REVENUE, ";
                }
                SQL += "       EMP.EMPLOYEE_NAME SALES_REPORTER";
                SQL += "  from job_card_trn J,";
                SQL += "       BOOKING_MST_TBL      BOOK,";
                SQL += "       CUSTOMER_MST_TBL     SHIPPER,";
                SQL += "       CUSTOMER_MST_TBL     CONSIGNEE,";
                SQL += "       PORT_MST_TBL         POL,";
                SQL += "       PORT_MST_TBL         POD,";
                SQL += "       EMPLOYEE_MST_TBL     EMP";
                SQL = SQL + "       ,USER_MST_TBL         USR ,location_mst_tbl l ";

                SQL = SQL + " WHERE J.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)";
                SQL += "   AND J.BOOKING_MST_FK = BOOK.BOOKING_MST_PK";
                SQL += "   AND J.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)";
                SQL += "   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK";
                SQL += "   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK";
                SQL += "   /*AND POL.location_mst_fk =l.location_mst_pk */ and l.location_mst_pk=usr.default_location_fk ";
                SQL += "   AND SHIPPER.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+) ";
                SQL += "   AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))";
                SQL += "   AND J.BUSINESS_TYPE=2 AND J.PROCESS_TYPE=1";
                // ''SQL = SQL & " WHERE J.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)" & vbNewLine
                // ''SQL &= "   AND J.BOOKING_MST_FK = BOOK.BOOKING_MST_PK" & vbNewLine
                // ''SQL &= "   AND J.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)" & vbNewLine
                // ''SQL &= "   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK" & vbNewLine
                // ''SQL &= "   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK" & vbNewLine
                // ''SQL &= "   AND POL.location_mst_fk =l.location_mst_pk and l.location_mst_pk=usr.default_location_fk  " & vbNewLine
                // ''SQL &= "   AND SHIPPER.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+) " & vbNewLine
                // ''SQL &= "   AND (TO_DATE(JOBCARD_DATE,'" & dateFormat & "') BETWEEN TO_DATE('" & fromDate & "','" & dateFormat & "') AND TO_DATE('" & toDate & "','" & dateFormat & "'))"
                // ''SQL &= "   AND J.BUSINESS_TYPE=2 AND J.PROCESS_TYPE=1" & vbNewLine

                SQL = SQL + "   AND j.created_by_fk = usr.user_mst_pk ";
                if (rptvalue == 4)
                {
                    SQL += " and J.PYMT_TYPE=1";
                }
                else if (rptvalue == 5)
                {
                    SQL += " and J.PYMT_TYPE=2";
                }
                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and J.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SHIPPER.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and BOOK.PORT_MST_POD_FK=" + podpk;
                }

                ///''''''''''''''''
                SQL += " UNION ";

                ///''''''''''''''''''''
                //Sea Import
                SQL += deprtmnt;
                SQL += "TO_CHAR(Jc.JOBCARD_DATE, 'MONTH YYYY') JOBCARD_MONTH,";
                SQL += "TO_CHAR(Jc.JOBCARD_DATE, dateformat) JOBCARD_DATE,";
                SQL += "Jc.JOBCARD_REF_NO REF_NUMBER," + deprtmnts;
                SQL += "sh.CUSTOMER_NAME SHIPPER_NAME,";
                SQL += "co.CUSTOMER_NAME CONSIGNEE_NAME,";
                SQL += "DECODE(Jc.PYMT_TYPE, 1, 'Y', 2, 'N') PAYMENT_TYPE,";
                SQL += "POL.PORT_ID POL,";
                SQL += "POD.PORT_ID POD,";
                SQL += "(CASE WHEN jc.CARGO_TYPE = 1 THEN (SELECT SUM(NVL(CONT.NET_WEIGHT, 0)) FROM JOB_TRN_CONT CONT WHERE CONT.JOB_CARD_TRN_FK = Jc.JOB_CARD_TRN_PK) ELSE (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0)) FROM JOB_TRN_CONT CONT WHERE CONT.JOB_CARD_TRN_FK = Jc.JOB_CARD_TRN_PK) END) WEIGHT_VOLUME,";
                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.JOB_CARD_TRN_PK, ";
                SQL += "      /* (select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + " ,4)) FREIGHT, ";

                if (rptvalue != 1 & rptvalue != 8)
                {
                    SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.JOB_CARD_TRN_PK, ";
                    SQL += "      /* (select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                    SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + " ,5)) ESTIMATED_REVENUE, ";
                }
                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/ " + HttpContext.Current.Session["CURRENCY_MST_PK"] + " ,1)) ACTUAL_REVENUE, ";

                SQL += "       (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.JOB_CARD_TRN_PK, ";
                SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) ACTUAL_COST ";

                if (rptvalue == 1 | rptvalue == 8)
                {
                    SQL += "     ,  (FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_sea(jc.JOB_CARD_TRN_PK, ";
                    SQL += "       /*(select c.currency_mst_fk from country_mst_tbl c,location_mst_tbl loc where loc.location_mst_pk=umt.default_location_fk and ";
                    SQL += "       loc.country_mst_fk=c.country_mst_pk)*/" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5)) ESTIMATED_REVENUE ";
                }

                SQL += "   , '' SALES_REPORTER ";
                SQL += "from JOB_CARD_TRN jC, BOOKING_MST_TBL BOOK,";
                SQL += "       CUSTOMER_MST_TBL     CO,CUSTOMER_MST_TBL     SH,";
                SQL += "       PORT_MST_TBL         POL,";
                SQL += "       PORT_MST_TBL         POD,";
                SQL += "       AGENT_MST_TBL        POLA,";
                SQL += "       EMPLOYEE_MST_TBL     EMP,location_mst_tbl l,";
                SQL += "       USER_MST_TBL UMT";

                SQL = SQL + " WHERE JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)";
                SQL += "   AND JC.BOOKING_MST_FK = BOOK.BOOKING_MST_PK";
                SQL += "   AND JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)";
                SQL += "   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK";
                SQL += "   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK";
                SQL += "   /*AND POL.location_mst_fk =l.location_mst_pk */ and l.location_mst_pk=umt.default_location_fk ";
                SQL += "   AND SH.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+) ";
                SQL += "   AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))";
                SQL += "   AND JC.BUSINESS_TYPE=2 AND JC.PROCESS_TYPE=2";

                // ''SQL &= "where JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)" & vbNewLine
                // ''SQL &= "AND sh.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)" & vbNewLine
                // ''SQL &= "AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)" & vbNewLine
                // ''SQL &= "AND JC.PORT_MST_POL_FK = POL.PORT_MST_PK" & vbNewLine
                // ''SQL &= "AND JC.PORT_MST_POD_FK = POD.PORT_MST_PK" & vbNewLine
                // ''SQL &= "AND JC.POL_AGENT_MST_FK = POLA.AGENT_MST_PK(+)" & vbNewLine
                // ''SQL &= "   AND JC.BUSINESS_TYPE=2 AND JC.PROCESS_TYPE=2" & vbNewLine

                // ''SQL &= "  AND jc.CREATED_BY_FK = UMT.USER_MST_PK and l.location_mst_pk=umt.default_location_fk " & vbNewLine
                // ''SQL &= "  AND ((UMT.DEFAULT_LOCATION_FK = l.location_mst_pk and JC.JC_AUTO_MANUAL = 0) OR (JC.PORT_MST_POD_FK  "
                // ''SQL &= "  IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK= l.location_mst_pk)  and JC.JC_AUTO_MANUAL = 1)) "

                // ''SQL &= "AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS = 1)" & vbNewLine
                // ''SQL &= "   AND (TO_DATE(JOBCARD_DATE,'" & dateFormat & "') BETWEEN TO_DATE('" & fromDate & "','" & dateFormat & "') AND TO_DATE('" & toDate & "','" & dateFormat & "'))"
                // ''SQL &= "   AND JC.BUSINESS_TYPE=2 AND JC.PROCESS_TYPE=2" & vbNewLine

                if (rptvalue == 4)
                {
                    SQL += " and JC.PYMT_TYPE=1";
                }
                else if (rptvalue == 5)
                {
                    SQL += " and JC.PYMT_TYPE=2";
                }

                if (Convert.ToInt32(Loc) > 0)
                {
                    SQL += " and l.location_mst_pk=" + Loc;
                }
                if (Convert.ToInt32(custpk) > 0)
                {
                    SQL += " and JC.SHIPPER_CUST_MST_FK= " + custpk;
                }
                if (Convert.ToInt32(emppk) > 0)
                {
                    SQL += " and SH.REP_EMP_MST_FK=" + emppk;
                }
                if (Convert.ToInt32(polpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POL_FK=" + polpk;
                }
                if (Convert.ToInt32(podpk) > 0)
                {
                    SQL += " and JC.PORT_MST_POD_FK=" + podpk;
                }
                //'------------------------------------------------------------------------------------------------
            }
            try
            {
                return objWF.GetDataSet(SQL);
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

        //this block creating by thiyagarajan on 16/4/09 for introducing grid functionality
        public DataSet GetSalesRptGrid(Int32 pageload, string SortColumn, string polpk, string podpk, string emppk, string custpk, Int32 rptvalue, int reportType, string Loc, string LocFK,
        string fromDate, string toDate, Int32 CurrentPage, Int32 TotalPage, int isPPD = 0, int isSPL = 0, Int32 Report = 2, Int16 Process = 0, Int32 CurrPk = 0, Int16 biztype = 0)
        {
            WorkFlow objWF = new WorkFlow();
            string SQL = null;
            string deprtmnt = null;
            string deprtmnts = null;
            string strCondition = " ";
            Int32 TotalRecords = default(Int32);
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            if (isPPD == 1 | isPPD == 2)
            {
                strCondition = strCondition + " AND J.PYMT_TYPE = " + isPPD;
            }
            strCondition = strCondition + " ORDER BY J.JOBCARD_DATE DESC ";
            if (isSPL == 1)
            {
                strCondition = strCondition + " , SALES_REPORTER ";
            }
            //******************
            SQL += "  SELECT J.JOBCARD_REF_NO REF_NUMBER,  ";
            SQL += "         TO_DATE(J.JOBCARD_DATE, 'DD/MM/YYYY') JOBCARD_DATE,  ";
            SQL += "         SHIPPER.CUSTOMER_NAME SHIPPER_NAME,  ";
            SQL += "         CONSIGNEE.CUSTOMER_NAME CONSIGNEE_NAME,  ";
            SQL += "         DECODE(J.PYMT_TYPE, 1, 'PPD', 2, 'CLT') PAYMENT_TYPE,  ";
            SQL += "         POL.PORT_ID POL,  ";
            SQL += "         POD.PORT_ID POD,  ";
            SQL += "         (SELECT SUM(NVL(CTMT.TEU_FACTOR, 0))  ";
            SQL += "            FROM JOB_TRN_CONT CONT, CONTAINER_TYPE_MST_TBL CTMT  ";
            SQL += "           WHERE (CONT.JOB_CARD_TRN_FK = J.JOB_CARD_TRN_PK)  ";
            SQL += "             AND CONT.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK(+)) TEU,  ";
            SQL += "         (SELECT SUM(NVL(CONT.GROSS_WEIGHT, 0))  ";
            SQL += "            FROM JOB_TRN_CONT CONT  ";
            SQL += "           WHERE CONT.JOB_CARD_TRN_FK = J.JOB_CARD_TRN_PK) WEIGHT,  ";
            SQL += "         (SELECT SUM(NVL(CONT.VOLUME_IN_CBM, 0))  ";
            SQL += "            FROM JOB_TRN_CONT CONT  ";
            SQL += "           WHERE CONT.JOB_CARD_TRN_FK = J.JOB_CARD_TRN_PK) VOLUME,  ";
            SQL += "         'EUR' CURRENCY,  ";
            SQL += "         NULL FREIGHT,  ";
            SQL += "         NULL ACTUAL_COST,  ";
            SQL += "         NULL ESTIMATED_REVENUE,  ";
            SQL += "         NULL ACTUAL_REVENUE,  ";
            SQL += "         NVL(EMP.EMPLOYEE_NAME, 'CSR') SALES_REPORTER,  ";
            SQL += "         (CASE  ";
            SQL += "           WHEN J.BUSINESS_TYPE = 1 AND J.PROCESS_TYPE = 1 THEN  ";
            SQL += "            'AIREXP'  ";
            SQL += "           WHEN J.BUSINESS_TYPE = 1 AND J.PROCESS_TYPE = 2 THEN  ";
            SQL += "            'AIRIMP'  ";
            SQL += "           WHEN J.BUSINESS_TYPE = 2 AND J.PROCESS_TYPE = 1 THEN  ";
            SQL += "            'SEAEXP'  ";
            SQL += "           WHEN J.BUSINESS_TYPE = 2 AND J.PROCESS_TYPE = 2 THEN  ";
            SQL += "            'SEAIMP'  ";
            SQL += "           ELSE  ";
            SQL += "            ''  ";
            SQL += "         END) AS DEPARTMENT,  ";
            SQL += "         J.JOB_CARD_TRN_PK AS JOBPK,  ";
            SQL += "         (CASE  ";
            SQL += "           WHEN J.BUSINESS_TYPE = 2 AND BOOK.CARGO_TYPE = 1 THEN  ";
            SQL += "            'FCL'  ";
            SQL += "           WHEN J.BUSINESS_TYPE = 2 AND BOOK.CARGO_TYPE = 2 THEN  ";
            SQL += "            'LCL'  ";
            SQL += "           WHEN J.BUSINESS_TYPE = 2 AND BOOK.CARGO_TYPE = 4 THEN  ";
            SQL += "            'BBC'  ";
            SQL += "           ELSE  ";
            SQL += "            ''  ";
            SQL += "         END) AS CARGOTYPE  ";
            SQL += "    FROM JOB_CARD_TRN     J,  ";
            SQL += "         BOOKING_MST_TBL  BOOK,  ";
            SQL += "         CUSTOMER_MST_TBL SHIPPER,  ";
            SQL += "         CUSTOMER_MST_TBL CONSIGNEE,  ";
            SQL += "         PORT_MST_TBL     POL,  ";
            SQL += "         PORT_MST_TBL     POD,  ";
            SQL += "         EMPLOYEE_MST_TBL EMP,  ";
            SQL += "         USER_MST_TBL     USR,  ";
            SQL += "         LOCATION_MST_TBL L  ";
            SQL += "   WHERE J.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)  ";
            SQL += "     AND J.BOOKING_MST_FK = BOOK.BOOKING_MST_PK  ";
            SQL += "     AND J.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)  ";
            SQL += "     AND J.PORT_MST_POD_FK = POD.PORT_MST_PK  ";
            SQL += "     AND J.PORT_MST_POL_FK = POL.PORT_MST_PK  ";
            SQL += "     AND L.LOCATION_MST_PK = USR.DEFAULT_LOCATION_FK  ";
            SQL += "     AND SHIPPER.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)  ";
            SQL += "     AND J.CREATED_BY_FK = USR.USER_MST_PK  ";
            SQL += "      ";

            if (Process > 0)
            {
                SQL += " AND J.PROCESS_TYPE = " + Process;
            }
            if (biztype > 0 & biztype != 3)
            {
                SQL += " AND J.BUSINESS_TYPE = " + biztype;
            }
            if (rptvalue == 4)
            {
                SQL += " and J.PYMT_TYPE=1";
            }
            else if (rptvalue == 5)
            {
                SQL += " and J.PYMT_TYPE=2";
            }

            if (Convert.ToInt32(Loc) > 0)
            {
                SQL += " and l.location_mst_pk=" + Loc;
            }
            if (Convert.ToInt32(custpk) > 0)
            {
                SQL += " and J.SHIPPER_CUST_MST_FK= " + custpk;
            }
            if (Convert.ToInt32(emppk) > 0)
            {
                SQL += " and SHIPPER.REP_EMP_MST_FK=" + emppk;
            }
            if (Convert.ToInt32(polpk) > 0)
            {
                SQL += " and J.PORT_MST_POL_FK=" + polpk;
            }
            if (Convert.ToInt32(podpk) > 0)
            {
                SQL += " and J.PORT_MST_POD_FK=" + podpk;
            }
            //If fromDate <> "" Then
            //    SQL &= " and J.JOBCARD_DATE>=TO_DATE('" & fromDate & "',DATEFORMAT)"
            //End If
            //If toDate <> "" Then
            //    SQL &= " and J.JOBCARD_DATE<=TO_DATE('" & toDate & "',DATEFORMAT)"
            //End If

            SQL += "   AND (TO_DATE(JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "') AND TO_DATE('" + toDate + "','" + dateFormat + "'))";
            //******************

            try
            {
                int SelectCount = 0;
                if (pageload == 0)
                {
                    if (reportType <= 4)
                    {
                        SQL += " where 1=2 ";
                    }
                }
                if (reportType > 4)
                {
                    if (pageload == 0)
                    {
                        TotalRecords = Convert.ToInt32(objWF.ExecuteScaler("SELECT count(*) FROM ( " + SQL + ") WHERE 1=2"));
                        // Getting No of satisfying records.
                    }
                    else
                    {
                        TotalRecords = Convert.ToInt32(objWF.ExecuteScaler("SELECT count(*) FROM ( " + SQL + ")"));
                        // Getting No of satisfying records.
                    }
                }
                else
                {
                    TotalRecords = Convert.ToInt32(objWF.ExecuteScaler("SELECT count(*) FROM ( " + SQL + ")"));
                    // Getting No of satisfying records.
                }
                TotalPage = TotalRecords / RecordsPerPage;
                if (TotalRecords % RecordsPerPage != 0)
                    TotalPage += 1;
                if (CurrentPage > TotalPage)
                    CurrentPage = 1;
                if (TotalRecords == 0)
                    CurrentPage = 0;
                last = CurrentPage * RecordsPerPage;
                start = (CurrentPage - 1) * RecordsPerPage + 1;
                string SORTBY = null;
                //Depart - 1, shipper -2, consi - 3, Prepaid - 4, Collect - 5, Dest - 6, Origin - 7, Perform - 8
                if (rptvalue == 1)
                {
                    SORTBY = " DEPARTMENT, JOBCARD_DATE ";
                }
                else if (rptvalue == 2)
                {
                    SORTBY = " SHIPPER_NAME, JOBCARD_DATE ";
                }
                else if (rptvalue == 3)
                {
                    SORTBY = " CONSIGNEE_NAME, JOBCARD_DATE ";
                }
                else if (rptvalue == 4)
                {
                    SORTBY = " PAYMENT_TYPE, JOBCARD_DATE ";
                }
                else if (rptvalue == 5)
                {
                    SORTBY = " PAYMENT_TYPE, JOBCARD_DATE ";
                }
                else if (rptvalue == 6)
                {
                    SORTBY = " POD, JOBCARD_DATE ";
                }
                else if (rptvalue == 7)
                {
                    SORTBY = " POL, JOBCARD_DATE ";
                }
                else if (rptvalue == 8)
                {
                    SORTBY = " ACTUAL_REVENUE Desc, JOBCARD_DATE ";
                }
                else
                {
                    SORTBY = " JOBCARD_DATE ";
                }
                DataSet dsRPT = new DataSet();
                if (reportType > 4)
                {
                    if (reportType == 9)
                    {
                        dsRPT = objWF.GetDataSet(" SELECT MAINQ.* FROM(SELECT ROWNUM SLNO ,Q.* FROM  ( SELECT * FROM (" + SQL + ") ORDER BY " + SortColumn + " ) Q ) MAINQ where SLNO between " + start + " and " + last);
                    }
                    else
                    {
                        dsRPT = objWF.GetDataSet(" SELECT MAINQ.* FROM(SELECT ROWNUM SLNO ,Q.* FROM  ( SELECT * FROM (" + SQL + ") ORDER BY " + SortColumn + " ) Q ) MAINQ where SLNO between " + start + " and " + last);
                    }
                }
                else
                {
                    dsRPT = objWF.GetDataSet(" SELECT MAINQ.* FROM(SELECT ROWNUM SLNO ,Q.* FROM  ( SELECT * FROM (" + SQL + ") ORDER BY " + SortColumn + " ) Q  ) MAINQ where SLNO between " + start + " and " + last);
                }

                foreach (DataRow row in dsRPT.Tables[0].Rows)
                {
                    //4 FREIGHT,2 ACTUAL_COST,5 ESTIMATED_REVENUE,0 ACTUAL_REVENUE
                    if (row["DEPARTMENT"] == "SEAEXP")
                    {
                        row["FREIGHT"] = objWF.ExecuteScaler("SELECT FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_SEA(" + row["JOBPK"] + ", " + CurrPk + ", 4) FROM DUAL");
                        row["ACTUAL_COST"] = objWF.ExecuteScaler("SELECT FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_SEA(" + row["JOBPK"] + ", " + CurrPk + ", 2) FROM DUAL");
                        row["ESTIMATED_REVENUE"] = objWF.ExecuteScaler("SELECT FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_SEA(" + row["JOBPK"] + ", " + CurrPk + ", 5) FROM DUAL");
                        row["ACTUAL_REVENUE"] = Convert.ToInt32(objWF.ExecuteScaler("SELECT FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_SEA(" + row["JOBPK"] + ", " + CurrPk + ", 1) FROM DUAL")) - Convert.ToInt32(row["ACTUAL_COST"]);
                    }
                    else if (row["DEPARTMENT"] == "SEAIMP")
                    {
                        row["FREIGHT"] = objWF.ExecuteScaler("SELECT FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_SEA(" + row["JOBPK"] + ", " + CurrPk + ", 4) FROM DUAL");
                        row["ACTUAL_COST"] = objWF.ExecuteScaler("SELECT FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_SEA(" + row["JOBPK"] + ", " + CurrPk + ", 2) FROM DUAL");
                        row["ESTIMATED_REVENUE"] = objWF.ExecuteScaler("SELECT FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_SEA(" + row["JOBPK"] + ", " + CurrPk + ", 5) FROM DUAL");
                        row["ACTUAL_REVENUE"] = Convert.ToInt32(objWF.ExecuteScaler("SELECT FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_SEA(" + row["JOBPK"] + ", " + CurrPk + ", 1) FROM DUAL")) - Convert.ToInt32(row["ACTUAL_COST"]);
                    }
                    else if (row["DEPARTMENT"] == "AIREXP")
                    {
                        row["FREIGHT"] = objWF.ExecuteScaler("SELECT FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_AIR(" + row["JOBPK"] + ", " + CurrPk + ", 4) FROM DUAL");
                        row["ACTUAL_COST"] = objWF.ExecuteScaler("SELECT FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_AIR(" + row["JOBPK"] + ", " + CurrPk + ", 2) FROM DUAL");
                        row["ESTIMATED_REVENUE"] = objWF.ExecuteScaler("SELECT FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_AIR(" + row["JOBPK"] + ", " + CurrPk + ", 5) FROM DUAL");
                        row["ACTUAL_REVENUE"] = Convert.ToInt32(objWF.ExecuteScaler("SELECT FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_SEA(" + row["JOBPK"] + ", " + CurrPk + ", 1) FROM DUAL")) - Convert.ToInt32(row["ACTUAL_COST"]);
                    }
                    else if (row["DEPARTMENT"] == "AIRIMP")
                    {
                        row["FREIGHT"] = objWF.ExecuteScaler("SELECT FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_AIR(" + row["JOBPK"] + ", " + CurrPk + ", 4) FROM DUAL");
                        row["ACTUAL_COST"] = objWF.ExecuteScaler("SELECT FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_AIR(" + row["JOBPK"] + ", " + CurrPk + ", 2) FROM DUAL");
                        row["ESTIMATED_REVENUE"] = objWF.ExecuteScaler("SELECT FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_AIR(" + row["JOBPK"] + ", " + CurrPk + ", 5) FROM DUAL");
                        row["ACTUAL_REVENUE"] = Convert.ToInt32(objWF.ExecuteScaler("SELECT FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_SEA(" + row["JOBPK"] + ", " + CurrPk + ", 1) FROM DUAL")) - Convert.ToInt32(row["ACTUAL_COST"]);
                    }
                    try
                    {
                        row["FREIGHT"] = Convert.ToDouble(row["FREIGHT"]).ToString("####0.00");
                        row["ACTUAL_COST"] = Convert.ToDouble(row["ACTUAL_COST"]).ToString("####0.00");
                        row["ESTIMATED_REVENUE"] = Convert.ToDouble(row["ESTIMATED_REVENUE"]).ToString("####0.00");
                        row["ACTUAL_REVENUE"] = Convert.ToDouble(row["ACTUAL_REVENUE"]).ToString("####0.00");
                    }
                    catch (Exception ex)
                    {
                    }
                }
                return dsRPT;
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

        #endregion "GetMainBookingData"
    }
}