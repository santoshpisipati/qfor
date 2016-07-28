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
using System;
using System.Collections;
using System.Data;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_Consolidation : CommonFeatures
    {

        WorkFlow objWF = new WorkFlow();
        #region "FetchJCPKAir"
        public DataSet FetchJCPKAir(string airline = "", string shipper = "", string pol = "", string pod = "", string commodity = "", string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " ASC ", string CurrBizType = "3")
        {
            string strSql = null;
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            string strCondition = "";
            string strGroupBy = "";
            string strMain = null;
            if (BlankGrid == 0)
            {
                strCondition += " AND 1=2 ";
            }
            if (pol.Length > 0)
            {
                strCondition += " AND B.PORT_MST_POL_FK= " + pol;
            }
            if (pod.Length > 0)
            {
                strCondition += " AND B.PORT_MST_POD_FK= " + pod;
            }
            if (shipper.Length > 0)
            {
                strCondition += " AND H.SHIPPER_CUST_MST_FK= " + shipper;
            }
            if (airline.Length > 0)
            {
                strCondition += " AND B.CARRIER_MST_FK= " + airline;
            }
            if (commodity.Length > 0)
            {
                strCondition += " AND COM.COMMODITY_GROUP_PK= " + commodity;
            }
            if (!string.IsNullOrEmpty(strCondition))
            {
                strCondition += " AND H.MAWB_EXP_TBL_FK IS NULL ";
            }

            strMain = "FROM " +  "BOOKING_MST_TBL B, " +  "HAWB_EXP_TBL H," +  "JOB_TRN_CONT J," +  "JOB_CARD_TRN JC," +  "AIRLINE_MST_TBL A," +  "PORT_MST_TBL POL," +  "PORT_MST_TBL POD," +  "CUSTOMER_MST_TBL SH," +  "CUSTOMER_MST_TBL CON," +  "COMMODITY_GROUP_MST_TBL COM" +  "WHERE" +  "B.BOOKING_MST_PK=JC.BOOKING_MST_FK AND" +  "JC.JOB_CARD_TRN_PK = H.JOB_CARD_AIR_EXP_FK And " +  "J.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK" +  "AND A.AIRLINE_MST_PK(+)= B.CARRIER_MST_FK" +  "AND SH.CUSTOMER_MST_PK(+)=H.SHIPPER_CUST_MST_FK" +  "AND CON.CUSTOMER_MST_PK(+)=H.CONSIGNEE_CUST_MST_FK" +  "and POL.PORT_MST_PK(+)=B.PORT_MST_POL_FK" +  "AND POD.PORT_MST_PK(+)=B.PORT_MST_POD_FK" +  "AND JC.JOBCARD_REF_NO NOT IN (SELECT A.REFERENCE_NO FROM AIRWAY_BILL_TRN A WHERE A.REFERENCE_NO IS NOT NULL)" +  "AND JC.COMMODITY_GROUP_FK = COM.COMMODITY_GROUP_PK";

            strGroupBy = " GROUP BY" +  "JC.MASTER_JC_FK," +  "JC.JOB_CARD_TRN_PK ";

            strSql = "SELECT JC.MASTER_JC_FK MJCPK, JC.JOB_CARD_TRN_PK JCPK" +  strMain;
            if (string.IsNullOrEmpty(strCondition))
            {
                strSql += " AND JOB_CARD_TRN_PK=0";
            }
            else
            {
                strSql += strCondition;
            }
            try
            {
                return objWF.GetDataSet(strSql);
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

        #region "fetch"
        public DataSet fetch(string airline = "", string shipper = "", string pol = "", string pod = "", string commodity = "", string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " ASC ", string JCPKS = "0",
        string CurrBizType = "3")
        {
            string strSql = null;
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            string strCondition = "";
            string strGroupBy = "";
            string strMain = null;
            if (BlankGrid == 0)
            {
                strCondition += " AND 1=2 ";
            }
            if (pol.Length > 0)
            {
                strCondition += " AND B.PORT_MST_POL_FK=" + pol;
            }
            if (pod.Length > 0)
            {
                strCondition += " AND B.PORT_MST_POD_FK=" + pod;
            }
            if (shipper.Length > 0)
            {
                strCondition += " AND H.SHIPPER_CUST_MST_FK=" + shipper;
            }
            if (airline.Length > 0)
            {
                strCondition += " AND B.CARRIER_MST_FK=" + airline;
            }
            if (commodity.Length > 0)
            {
                strCondition += " AND COM.COMMODITY_GROUP_PK=" + commodity;
            }
            if (!string.IsNullOrEmpty(strCondition))
            {
                strCondition += " AND H.MAWB_EXP_TBL_FK IS NULL ";
            }

            strMain = "FROM " +  "BOOKING_MST_TBL B, " +  "HAWB_EXP_TBL H," +  "JOB_TRN_CONT J," +  "JOB_CARD_TRN JC," +  "AIRLINE_MST_TBL A," +  "PORT_MST_TBL POL," +  "PORT_MST_TBL POD," +  "CUSTOMER_MST_TBL SH," +  "CUSTOMER_MST_TBL CON," +  "COMMODITY_GROUP_MST_TBL COM" +  "WHERE" +  "B.BOOKING_MST_PK=JC.BOOKING_MST_FK AND" +  "JC.JOB_CARD_TRN_PK = H.JOB_CARD_AIR_EXP_FK And " +  "J.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK" +  "AND A.AIRLINE_MST_PK(+)= B.CARRIER_MST_FK" +  "AND SH.CUSTOMER_MST_PK(+)=H.SHIPPER_CUST_MST_FK" +  "AND CON.CUSTOMER_MST_PK(+)=H.CONSIGNEE_CUST_MST_FK" +  "and POL.PORT_MST_PK(+)=B.PORT_MST_POL_FK" +  "AND POD.PORT_MST_PK(+)=B.PORT_MST_POD_FK" +  "AND JC.JOBCARD_REF_NO NOT IN (SELECT A.REFERENCE_NO FROM AIRWAY_BILL_TRN A WHERE A.REFERENCE_NO IS NOT NULL)" +  "AND JC.COMMODITY_GROUP_FK = COM.COMMODITY_GROUP_PK";

            strGroupBy = " GROUP BY" +  "B.CARRIER_MST_FK," +  "A.AIRLINE_ID," +  "H.HAWB_REF_NO," +  "H.SHIPPER_CUST_MST_FK," +  "SH.CUSTOMER_ID ," +  "H.CONSIGNEE_CUST_MST_FK," +  "CON.CUSTOMER_ID ," +  "B.PORT_MST_POL_FK, " +  "POL.PORT_ID ," +  "B.PORT_MST_POD_FK," +  "POD.PORT_ID ," +  "JC.COMMODITY_GROUP_FK," +  "H.TOTAL_VOLUME," +  "H.TOTAL_GROSS_WEIGHT ," +  "H.TOTAL_CHARGE_WEIGHT," +  "COM.COMMODITY_GROUP_CODE,H.HAWB_EXP_TBL_PK,nvl(JC.sb_number, ''), nvl(JC.sb_date, '') ";

            strSql = "Select count(*)" + strMain;
            if (string.IsNullOrEmpty(strCondition))
            {
                strSql += " AND JOB_CARD_TRN_PK=0";
            }
            else
            {
                strSql += strCondition;
            }
            TotalRecords = Convert.ToInt32( objWF.ExecuteScaler(strSql));
            TotalPage = TotalRecords / RecordsPerPage;
            if (TotalRecords % RecordsPerPage != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * RecordsPerPage;
            start = (CurrentPage - 1) * RecordsPerPage + 1;

            strSql = "Select * from ( Select ROWNUM SR_NO, q.* from (SELECT  " +  "B.CARRIER_MST_FK," +  "A.AIRLINE_ID," +  "H.HAWB_REF_NO," +  "H.SHIPPER_CUST_MST_FK," +  "SH.CUSTOMER_ID as Shipper," +  "H.CONSIGNEE_CUST_MST_FK," +  "CON.CUSTOMER_ID as CONSIGNEE," +  "B.PORT_MST_POL_FK, " +  "POL.PORT_ID as POL," +  "B.PORT_MST_POD_FK," +  "POD.PORT_ID as POD," +  "JC.COMMODITY_GROUP_FK," +  "COM.COMMODITY_GROUP_CODE," +  "H.HAWB_EXP_TBL_PK," +  "SUM(J.PACK_COUNT) PACK_COUNT," +  "H.TOTAL_VOLUME," +  "H.TOTAL_GROSS_WEIGHT," +  "H.TOTAL_CHARGE_WEIGHT," +  "'' ActRev," +  "'' EstRev," +  "'' ActCost," +  "'' EstCost," +  "'' ActProf," +  "'' EstProf, nvl(JC.sb_number, '') SBNo, nvl(JC.sb_date, '') SBDt,null CONTAINER_TYPE,null CONTAINER_NO,null SEAL_NO,null CONTAINER_PK, null TAREWEIGHT" +  strMain;
            if (string.IsNullOrEmpty(strCondition))
            {
                strSql += " AND JOB_CARD_TRN_PK=0";
            }
            else
            {
                strSql += strCondition;
            }
            strSql += strGroupBy +  ") q) where SR_NO between " + start + " and " + last;
            try
            {
                if (BlankGrid == 0)
                {
                    return objWF.GetDataSet(strSql);
                }
                else
                {
                    var _with1 = objWF.MyCommand.Parameters;
                    _with1.Add("JCPK", JCPKS).Direction = ParameterDirection.Input;
                    _with1.Add("START_RECORD", start).Direction = ParameterDirection.Input;
                    _with1.Add("LAST_RECORD", last).Direction = ParameterDirection.Input;
                    _with1.Add("VCONDITION", (!string.IsNullOrEmpty(strCondition) ? strCondition : "")).Direction = ParameterDirection.Input;
                    _with1.Add("CurrPk", HttpContext.Current.Session["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
                    _with1.Add("CONS_MJC_HAWB", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                    return objWF.GetDataSet("FETCH_COST_REVENUE_PROFIT", "CONS_FROM_HAWB");
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
        #endregion

        #region "FetchJCPKSea"
        public DataSet FetchJCPKSea(string OPERATOR1 = "", string shipper = "", string pol = "", string pod = "", string commodity = "", string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " ASC ", string VoyageFk = "",
        string CurrBizType = "3")
        {
            string strSql = null;
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            string strCondition = "";
            string strMain = "";
            string strGroupBy = "";
            if (BlankGrid == 0)
            {
                strCondition += " AND 1=2 ";
            }
            if (pol.Length > 0)
            {
                strCondition += " AND B.PORT_MST_POL_FK=" + pol;
            }
            if (pod.Length > 0)
            {
                strCondition += " AND B.PORT_MST_POD_FK=" + pod;
            }
            if (shipper.Length > 0)
            {
                strCondition += " AND H.SHIPPER_CUST_MST_FK=" + shipper;
            }
            if (OPERATOR1.Length > 0)
            {
                strCondition += " AND B.CARRIER_MST_FK=" + OPERATOR1;
            }
            if (commodity.Length > 0)
            {
                strCondition += " AND COM.COMMODITY_GROUP_PK=" + commodity;
            }

            if (VoyageFk.Length > 0)
            {
                strCondition += " AND JC.VOYAGE_TRN_FK = " + VoyageFk;
            }
            if (!string.IsNullOrEmpty(strCondition))
            {
                strCondition += " AND H.MBL_EXP_TBL_FK IS NULL AND jc.MBL_MAWB_FK is null AND B.CARGO_TYPE=2";
            }


            strMain = "FROM " +  "BOOKING_MST_TBL B, " +  "HBL_EXP_TBL H," +  "JOB_TRN_CONT J," +  "JOB_CARD_TRN JC," +  "OPERATOR_MST_TBL A," +  "PORT_MST_TBL POL," +  "PORT_MST_TBL POD," +  "CUSTOMER_MST_TBL SH," +  "CUSTOMER_MST_TBL CON," +  "COMMODITY_GROUP_MST_TBL COM" +  "WHERE" +  "B.BOOKING_MST_PK=JC.BOOKING_MST_FK AND" +  "JC.JOB_CARD_TRN_PK = H.JOB_CARD_SEA_EXP_FK And " +  "J.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK" +  "AND A.OPERATOR_MST_PK(+)= B.CARRIER_MST_FK" +  "AND SH.CUSTOMER_MST_PK(+)=H.SHIPPER_CUST_MST_FK" +  "AND CON.CUSTOMER_MST_PK(+)=H.CONSIGNEE_CUST_MST_FK" +  "and POL.PORT_MST_PK(+)=B.PORT_MST_POL_FK" +  "AND POD.PORT_MST_PK(+)=B.PORT_MST_POD_FK" +  "AND JC.COMMODITY_GROUP_FK = COM.COMMODITY_GROUP_PK";

            strGroupBy = " GROUP BY" +  "jc.MASTER_JC_FK,jc.JOB_CARD_TRN_PK";

            strSql = "select jc.MASTER_JC_FK MJCPK, " +  "jc.JOB_CARD_TRN_PK JCPK" +  strMain;

            if (string.IsNullOrEmpty(strCondition))
            {
                strSql += " AND JOB_CARD_TRN_PK=0";
            }
            else
            {
                strSql += strCondition;
            }
            try
            {
                return objWF.GetDataSet(strSql);
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

        #region "fetchforSEA"
        public DataSet fetchforSEA(string OPERATOR1 = "", string shipper = "", string pol = "", string pod = "", string commodity = "", string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " ASC ", string VoyageFk = "",
        string JCPKS = "", string CurrBizType = "3")
        {
            string strSql = null;
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            string strCondition = "";
            string strMain = "";
            string strGroupBy = "";
            int blnkgrd = 0;
            if (BlankGrid == 0)
            {
                strCondition += " AND 1=2 ";
            }
            if (pol.Length > 0)
            {
                strCondition += " AND B.PORT_MST_POL_FK=" + pol;
            }
            if (pod.Length > 0)
            {
                strCondition += " AND B.PORT_MST_POD_FK=" + pod;
            }
            if (shipper.Length > 0)
            {
                strCondition += " AND H.SHIPPER_CUST_MST_FK=" + shipper;
            }
            if (OPERATOR1.Length > 0)
            {
                strCondition += " AND B.CARRIER_MST_FK=" + OPERATOR1;
            }
            if (commodity.Length > 0)
            {
                strCondition += " AND COM.COMMODITY_GROUP_PK=" + commodity;
            }

            if (VoyageFk.Length > 0)
            {
                strCondition += " AND JC.VOYAGE_TRN_FK = " + VoyageFk;
            }
            if (!string.IsNullOrEmpty(strCondition))
            {
                strCondition += " AND H.MBL_EXP_TBL_FK IS NULL AND jc.MBL_MAWB_FK is null AND B.CARGO_TYPE=2 ";
            }


            strMain = "FROM " +  "BOOKING_MST_TBL B, " +  "HBL_EXP_TBL H," +  "JOB_TRN_CONT J," +  "JOB_CARD_TRN JC," +  "OPERATOR_MST_TBL A," +  "PORT_MST_TBL POL," +  "PORT_MST_TBL POD," +  "CUSTOMER_MST_TBL SH," +  "CUSTOMER_MST_TBL CON," +  "COMMODITY_GROUP_MST_TBL COM" +  "WHERE" +  "B.BOOKING_MST_PK=JC.BOOKING_MST_FK AND" +  "JC.JOB_CARD_TRN_PK = H.JOB_CARD_SEA_EXP_FK And " +  "J.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK" +  "AND A.OPERATOR_MST_PK(+)= B.CARRIER_MST_FK" +  "AND SH.CUSTOMER_MST_PK(+)=H.SHIPPER_CUST_MST_FK" +  "AND CON.CUSTOMER_MST_PK(+)=H.CONSIGNEE_CUST_MST_FK" +  "and POL.PORT_MST_PK(+)=B.PORT_MST_POL_FK" +  "AND POD.PORT_MST_PK(+)=B.PORT_MST_POD_FK" +  "AND JC.COMMODITY_GROUP_FK = COM.COMMODITY_GROUP_PK";

            strGroupBy = " GROUP BY" +  "B.CARRIER_MST_FK," +  "A.OPERATOR_ID," +  "H.HBL_REF_NO," +  "H.SHIPPER_CUST_MST_FK," +  "SH.CUSTOMER_ID ," +  "H.CONSIGNEE_CUST_MST_FK," +  "CON.CUSTOMER_ID ," +  "B.PORT_MST_POL_FK, " +  "POL.PORT_ID ," +  "B.PORT_MST_POD_FK," +  "POD.PORT_ID ," +  "JC.COMMODITY_GROUP_FK," +  "H.TOTAL_VOLUME," +  "H.TOTAL_GROSS_WEIGHT ," +  "H.TOTAL_CHARGE_WEIGHT," +  "COM.COMMODITY_GROUP_CODE,H.HBL_EXP_TBL_PK";

            strSql = "Select count(B.CARRIER_MST_FK) " + strMain;
            if (string.IsNullOrEmpty(strCondition))
            {
                strSql += " AND JOB_CARD_TRN_PK=0";
            }
            else
            {
                strSql += strCondition;
            }
            strSql += strGroupBy;

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSql));
            TotalPage = TotalRecords / RecordsPerPage;
            if (TotalRecords % RecordsPerPage != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * RecordsPerPage;
            start = (CurrentPage - 1) * RecordsPerPage + 1;

            strSql = "Select * from ( Select ROWNUM SR_NO, q.* from (SELECT  " +  "B.CARRIER_MST_FK," +  "A.OPERATOR_ID," +  "H.HBL_REF_NO," +  "H.SHIPPER_CUST_MST_FK," +  "SH.CUSTOMER_ID as Shipper," +  "H.CONSIGNEE_CUST_MST_FK," +  "CON.CUSTOMER_ID as CONSIGNEE," +  "B.PORT_MST_POL_FK, " +  "POL.PORT_ID as POL," +  "B.PORT_MST_POD_FK," +  "POD.PORT_ID as POD," +  "JC.COMMODITY_GROUP_FK," +  "COM.COMMODITY_GROUP_CODE," +  "H.HBL_EXP_TBL_PK," +  "SUM(J.PACK_COUNT) PACK_COUNT," +  "H.TOTAL_VOLUME," +  "H.TOTAL_GROSS_WEIGHT," +  "H.TOTAL_CHARGE_WEIGHT," +  "'' ActRev," +  "'' EstRev," +  "'' ActCost," +  "'' EstCost," +  "'' ActProf," +  "'' EstProf, nvl(JC.sb_number, '') SBNo, nvl(JC.sb_date, '') SBDt, '' container_type,'' container_no,'' seal_no,''container_pk, ''tareweight" +  strMain;
            if (string.IsNullOrEmpty(strCondition))
            {
                strSql += " AND JOB_CARD_TRN_PK=0";
            }
            else
            {
                strSql += strCondition;
            }
            strGroupBy +=  " ,JC.sb_number,JC.sb_date";

            strSql += strGroupBy +  ") q) where SR_NO between " + start + " and " + last;
            try
            {
                if (BlankGrid == 0)
                {
                    return objWF.GetDataSet(strSql);
                }
                else
                {
                    var _with2 = objWF.MyCommand.Parameters;
                    _with2.Add("JCPK", JCPKS).Direction = ParameterDirection.Input;
                    _with2.Add("START_RECORD", start).Direction = ParameterDirection.Input;
                    _with2.Add("LAST_RECORD", last).Direction = ParameterDirection.Input;
                    _with2.Add("VCONDITION", (!string.IsNullOrEmpty(strCondition) ? strCondition : "")).Direction = ParameterDirection.Input;
                    _with2.Add("CurrPk", HttpContext.Current.Session["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
                    _with2.Add("CONS_MJC_HBL", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                    return objWF.GetDataSet("FETCH_COST_REVENUE_PROFIT", "CONS_FROM_HBL");
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
        #endregion

        #region "CONTAINERTYPE"
        public DataSet CONTAINERTYPE()
        {
            string strSQL = null;
            strSQL = "SELECT C.CONTAINER_TYPE_MST_PK,";
            strSQL += "C.CONTAINER_TYPE_MST_ID,";
            strSQL += "C.CONTAINER_TYPE_MST_PK || '~' ||" + " C.CONTAINER_TAREWEIGHT_TONE || '~' ||" + "C.CONTAINER_MAX_CAPACITY_TONE AS CAPACITY  ";
            strSQL += " FROM CONTAINER_TYPE_MST_TBL C  ";
            strSQL += " WHERE C.ACTIVE_FLAG = 1 ";
            strSQL += " ORDER BY C.PREFERENCES";
            try
            {
                return (new WorkFlow()).GetDataSet(strSQL);
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

        #region "GetCargoType"
        public int GetCargoType(string MJCPK)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT M.CARGO_TYPE FROM MASTER_JC_SEA_EXP_TBL M WHERE M.MASTER_JC_SEA_EXP_PK=" + MJCPK);
            try
            {
                return Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
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

        #region "GetCommGrp"
        public int GetCommGrp(string MJCPK)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT M.COMMODITY_GROUP_FK FROM MASTER_JC_AIR_EXP_TBL M WHERE M.MASTER_JC_AIR_EXP_PK=" + MJCPK);
            try
            {
                return Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
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

        #region "FetchForSeaMasterJC"
        public DataSet FetchForSeaMasterJC(string masterJC = "0", string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " ASC ")
        {
            string strSql = null;
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            string strCondition = "";
            string strMain = "";
            string strGroupBy = "";

            if (string.IsNullOrEmpty(masterJC) | masterJC == "0")
            {
                masterJC = "1233";
            }

            strMain = "from JOB_CARD_TRN    job_exp," +  "master_jc_sea_exp_tbl   mst_job," +  "BOOKING_MST_TBL         book," +  "JOB_TRN_CONT    job_cont," +  "customer_mst_tbl        shipper," +  "customer_mst_tbl        consignee," +  "port_mst_tbl            pol," +  "port_mst_tbl            pod," +  "OPERATOR_MST_tbl        opr," +  "commodity_group_mst_tbl comm," +  "vessel_voyage_trn vv " +  "where mst_job.master_jc_sea_exp_pk = " + masterJC +  "and mst_job.master_jc_sea_exp_pk = job_exp.MASTER_JC_FK" +  "and book.BOOKING_MST_PK = job_exp.BOOKING_MST_FK" +  "and book.port_mst_pol_fk = pol.port_mst_pk" +  "and book.port_mst_pod_fk = pod.port_mst_pk" +  "and book.CARRIER_MST_FK = opr.OPERATOR_MST_pk(+)" +  "and job_exp.JOB_CARD_TRN_PK = job_cont.JOB_CARD_TRN_FK(+)" +  "and job_exp.shipper_cust_mst_fk = shipper.customer_mst_pk(+)" +  "and job_exp.consignee_cust_mst_fk = consignee.customer_mst_pk(+)" +  " and job_exp.commodity_group_fk = comm.commodity_group_pk " +  "  and mst_job.voyage_trn_fk = vv.voyage_trn_pk (+)" +  " and job_exp.MBL_MAWB_FK is null ";

            strSql = "Select count(JOB_CARD_TRN_PK) " + strMain;

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSql));
            TotalPage = TotalRecords / RecordsPerPage;
            if (TotalRecords % RecordsPerPage != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * RecordsPerPage;
            start = (CurrentPage - 1) * RecordsPerPage + 1;

            try
            {
                var _with3 = objWF.MyCommand.Parameters;
                _with3.Add("JCPK", masterJC).Direction = ParameterDirection.Input;
                _with3.Add("START_RECORD", start).Direction = ParameterDirection.Input;
                _with3.Add("LAST_RECORD", last).Direction = ParameterDirection.Input;
                _with3.Add("CurrPk", HttpContext.Current.Session["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
                _with3.Add("CONS_MJC_SEA", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                return objWF.GetDataSet("FETCH_COST_REVENUE_PROFIT", "CONS_FROM_MJCSEA");
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

        #region "FetchForAirMasterJC"
        public DataSet FetchForAirMasterJC(string masterJC = "", string airline = "", string commodity = "", string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " ASC ")
        {
            string strSql = null;
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            string strCondition = "";
            string strMain = "";
            string strGroupBy = "";
            dynamic ConAir = null;
            if (airline.Length != 0)
            {
                ConAir = "And opr.Airline_mst_pk=" + airline;
            }
            if (commodity.Length > 0)
            {
                ConAir += " AND job_exp.COMMODITY_GROUP_FK=" + commodity;
            }

            strMain = "from JOB_CARD_TRN    job_exp," +  "master_jc_Air_exp_tbl   mst_job," +  "BOOKING_MST_TBL         book," +  "JOB_TRN_CONT    job_cont," +  "customer_mst_tbl        shipper," +  "customer_mst_tbl        consignee," +  "port_mst_tbl            pol," +  "port_mst_tbl            pod," +  "Airline_mst_tbl        opr," +  "commodity_group_mst_tbl comm" +  "where mst_job.master_jc_Air_exp_pk = " + masterJC +  "and mst_job.master_jc_Air_exp_pk = job_exp.MASTER_JC_FK" +  "and book.BOOKING_MST_PK = job_exp.BOOKING_MST_FK" +  "and book.port_mst_pol_fk = pol.port_mst_pk" +  "and book.port_mst_pod_fk = pod.port_mst_pk" +  "and book.CARRIER_MST_FK = opr.Airline_mst_pk(+)" +  "and job_exp.JOB_CARD_TRN_PK = job_cont.JOB_CARD_TRN_FK(+)" +  "and job_exp.shipper_cust_mst_fk = shipper.customer_mst_pk(+)" +  "and job_exp.consignee_cust_mst_fk = consignee.customer_mst_pk(+)" +  "and job_exp.commodity_group_fk = comm.commodity_group_pk  " + "and job_exp.MBL_MAWB_FK is null  " + ConAir;

            strSql = "Select count(JOB_CARD_TRN_PK) " + strMain;

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSql));
            TotalPage = TotalRecords / RecordsPerPage;
            if (TotalRecords % RecordsPerPage != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * RecordsPerPage;
            start = (CurrentPage - 1) * RecordsPerPage + 1;

            try
            {
                var _with4 = objWF.MyCommand.Parameters;
                _with4.Add("JCPK", masterJC).Direction = ParameterDirection.Input;
                _with4.Add("START_RECORD", start).Direction = ParameterDirection.Input;
                _with4.Add("LAST_RECORD", last).Direction = ParameterDirection.Input;
                _with4.Add("AIRLINE", (!string.IsNullOrEmpty(airline) ? airline : "")).Direction = ParameterDirection.Input;
                _with4.Add("COMMODITY", (!string.IsNullOrEmpty(commodity) ? commodity : "")).Direction = ParameterDirection.Input;
                _with4.Add("CurrPk", HttpContext.Current.Session["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
                _with4.Add("CONS_MJC_AIR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                return objWF.GetDataSet("FETCH_COST_REVENUE_PROFIT", "CONS_FROM_MJCAIR");
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

        #region " Save Vessel/Voyage"
        public ArrayList SaveVesselVoyage(string VESSEL_ID, string FirstVoyageFk, string FirstVessel, string FirstVoyage, string OPERATORPk, string ETD_DATE, string POLPk, string PODPk)
        {
            WorkFlow objWK = new WorkFlow();
            cls_SeaBookingEntry objSBE = new cls_SeaBookingEntry();

            Int16 exe = default(Int16);
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            objWK.MyCommand.Transaction = TRAN;

            string strVoyagepk = "";
            strVoyagepk = FirstVoyageFk;

            try
            {
                if ((string.IsNullOrEmpty(FirstVoyageFk) || FirstVoyageFk == "0") & !string.IsNullOrEmpty(VESSEL_ID))
                {
                    FirstVoyageFk = "0";
                    objSBE.CREATED_BY = CREATED_BY;
                    objSBE.ConfigurationPK = ConfigurationPK;
                    if (!string.IsNullOrEmpty(FirstVessel) & !string.IsNullOrEmpty(VESSEL_ID) & !string.IsNullOrEmpty(FirstVoyage))
                    {
                        arrMessage = objSBE.SaveVesselMaster(Convert.ToInt32(FirstVoyageFk),
                            Convert.ToString(getDefault(FirstVessel, "")),
                            Convert.ToInt64(getDefault(OPERATORPk, 0)),
                            Convert.ToString(getDefault(VESSEL_ID, "")),
                            Convert.ToString(getDefault(FirstVoyage, "")), 
                            objWK.MyCommand,
                            Convert.ToInt32(getDefault(POLPk, 0)),
                            Convert.ToString(getDefault(PODPk, 0)), DateTime.MinValue,
                            Convert.ToDateTime(getDefault(ETD_DATE, null)),
                            DateTime.Now, 
                            DateTime.Now,
                            DateTime.Now,
                            DateTime.Now);
                        strVoyagepk = FirstVoyageFk;
                        if (!(string.Compare(arrMessage[0].ToString(), "saved") > 0))
                        {
                            return arrMessage;
                        }
                        else
                        {
                            TRAN.Commit();
                            arrMessage.Clear();
                        }
                    }
                }
            }
            catch (OracleException oraexp)
            {
                TRAN.Rollback();
                arrMessage.Add(oraexp.Message);
                return arrMessage;
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return new ArrayList();
        }
        #endregion

        #region "CONTAINERTYPE"
        public DataSet CONTAINERTYPECONSOLE(string CommGrp)
        {
            string strSQL = null;
            strSQL = "SELECT C.CONTAINER_TYPE_MST_PK,";
            strSQL += "C.CONTAINER_TYPE_MST_ID,";
            strSQL += "C.CONTAINER_TYPE_MST_PK || '~' ||" + " C.CONTAINER_TAREWEIGHT_TONE || '~' ||" + "C.CONTAINER_MAX_CAPACITY_TONE AS CAPACITY  ";
            strSQL += " FROM CONTAINER_TYPE_MST_TBL C  ";
            strSQL += " WHERE C.ACTIVE_FLAG = 1 AND 1 = 1";
            if (!string.IsNullOrEmpty(CommGrp))
            {
                if (CommGrp == "General")
                {
                    //strSQL &= " AND C.CONTAINER_KIND = 1 "
                }
                else if (CommGrp == "Reefer")
                {
                    strSQL += " AND C.CONTAINER_KIND = 2  ";
                }
            }
            strSQL += " ORDER BY C.PREFERENCES";
            try
            {
                return (new WorkFlow()).GetDataSet(strSQL);
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

    }
}