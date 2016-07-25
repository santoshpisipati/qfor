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
    public class Cls_Exp_Certificate : CommonFeatures
    {
        #region "Fetch For SEA/AIR Export"

        public DataSet FetchAirUserExport(string Ves_Flight = "", long JobPk = 0, long PolPk = 0, long PodPk = 0, long CustPk = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 flag = 0, string strFromDate = "")
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Int32 last = default(Int32);
            Int32 start = default(Int32);

            if (strFromDate.Trim().Length > 0)
            {
                strCondition = strCondition + " AND JA.DEPARTURE_DATE >= To_date('" + strFromDate + "', dateformat)";
            }

            if (Ves_Flight.Trim().Length > 0 & Ves_Flight != "0")
            {
                strCondition = strCondition + " AND air.airline_id = '" + Ves_Flight + "'";
            }
            if (flag == 0)
            {
                strCondition += " AND 1=2";
            }
            if (JobPk > 0)
            {
                strCondition = strCondition + " AND JA.JOB_CARD_TRN_PK = " + JobPk;
            }

            if (PolPk > 0)
            {
                strCondition = strCondition + " And BA.PORT_MST_POl_FK = " + PolPk;
            }

            if (PodPk > 0)
            {
                strCondition = strCondition + " And BA.PORT_MST_POD_FK = " + PodPk;
            }

            if (CustPk > 0)
            {
                strCondition = strCondition + " AND C.CUSTOMER_MST_PK=" + CustPk;
            }
            strCondition = strCondition + " AND JA.BUSINESS_TYPE = 1 ";
            strCondition = strCondition + " AND JA.PROCESS_TYPE = 1 ";

            Strsql = " SELECT Count(*) ";
            Strsql += " FROM JOB_CARD_TRN JA,HAWB_EXP_TBL HA,MAWB_EXP_TBL MA,";
            Strsql += " BOOKING_MST_TBL BA,PORT_MST_TBL POL,";
            Strsql += " PORT_MST_TBL POD,CUSTOMER_MST_TBL C,airline_mst_tbl   air, BOOKING_TRN bta";
            Strsql += " WHERE  JA.BOOKING_MST_FK(+)=BA.BOOKING_MST_PK";
            Strsql += " AND HA.JOB_CARD_AIR_EXP_FK(+)=JA.JOB_CARD_TRN_PK";
            Strsql += " AND HA.HAWB_EXP_TBL_PK(+)=JA.HBL_HAWB_FK";
            Strsql += " AND MA.MAWB_EXP_TBL_PK(+)=JA.MBL_MAWB_FK";
            Strsql += " AND POD.PORT_MST_PK(+)=BA.PORT_MST_POD_FK";
            Strsql += " AND POL.PORT_MST_PK(+)=BA.PORT_MST_POL_FK";
            Strsql += " AND C.CUSTOMER_MST_PK(+)=BA.CUST_CUSTOMER_MST_FK ";
            //'
            Strsql += " AND BA.booking_mst_pk=ja.booking_mst_fk  ";
            Strsql += " AND  bta.booking_mst_fk(+)=ba.booking_mst_pk ";
            Strsql += " AND  air.airline_mst_pk=ba.carrier_mst_fk ";
            //'
            Strsql += " AND JA.DEPARTURE_DATE IS NOT NULL  ";

            //If HttpContext.Current.Session("user_id") <> "admin" Then
            //    Strsql &= " and JA.created_by_fk= " & HttpContext.Current.Session("USER_PK")
            //End If
            Strsql += strCondition;
            //TotalRecords = (Int32)objWF.ExecuteScaler(Strsql);
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

            Strsql = " select  * from (";
            Strsql += " SELECT ROWNUM AS SLNO,Q.* FROM (";
            Strsql += " SELECT JA.JOB_CARD_TRN_PK AS JOBCARDPK ,";
            Strsql += " JA.BOOKING_MST_FK AS BOOKINGPK,JA.JOBCARD_REF_NO AS JOBREF, ";
            Strsql += " JA.VOYAGE_FLIGHT_NO AS VSL_FLIGHT,";
            Strsql += " HA.HAWB_EXP_TBL_PK AS HBL_HAWB_PK,HA.HAWB_REF_NO AS HBL_HAWB_REF,";
            Strsql += " MA.MAWB_EXP_TBL_PK AS MBL_MAWB_PK,MA.MAWB_REF_NO AS MBL_MAWB_REF,";
            Strsql += " TO_CHAR(JA.DEPARTURE_DATE,'" + dateFormat + "') AS DEP,POL.PORT_NAME AS POL,";
            Strsql += " POD.PORT_NAME AS POD,C.CUSTOMER_NAME AS CUSTOMER,'' AS SEL,1 BIZTYPE";
            Strsql += " FROM JOB_CARD_TRN JA,HAWB_EXP_TBL HA,MAWB_EXP_TBL MA,";
            Strsql += " BOOKING_MST_TBL BA,PORT_MST_TBL POL,";
            Strsql += " PORT_MST_TBL POD,CUSTOMER_MST_TBL C, airline_mst_tbl   air, BOOKING_TRN    bta";
            Strsql += " WHERE  JA.BOOKING_MST_FK(+)=BA.BOOKING_MST_PK";
            Strsql += " AND HA.JOB_CARD_AIR_EXP_FK(+)=JA.JOB_CARD_TRN_PK";
            Strsql += " AND HA.HAWB_EXP_TBL_PK(+)=JA.HBL_HAWB_FK";
            Strsql += " AND MA.MAWB_EXP_TBL_PK(+)=JA.MBL_MAWB_FK";
            Strsql += " AND POD.PORT_MST_PK(+)=BA.PORT_MST_POD_FK";
            Strsql += " AND POL.PORT_MST_PK(+)=BA.PORT_MST_POL_FK";
            Strsql += " AND C.CUSTOMER_MST_PK(+)=BA.CUST_CUSTOMER_MST_FK ";
            Strsql += " AND JA.DEPARTURE_DATE IS NOT NULL  ";
            Strsql += " AND BA.booking_mst_pk=ja.booking_mst_fk  ";
            Strsql += " AND  bta.booking_mst_fk(+)=ba.booking_mst_pk ";
            Strsql += " AND  air.airline_mst_pk=ba.carrier_mst_fk ";

            //If HttpContext.Current.Session("user_id") <> "admin" Then
            //    Strsql &= " and JA.created_by_fk= " & HttpContext.Current.Session("USER_PK")
            //End If

            Strsql += strCondition;
            Strsql += " ORDER BY JA.DEPARTURE_DATE DESC , JA.JOBCARD_REF_NO DESC";
            Strsql += " )q) WHERE SLNO  Between " + start + " and " + last;
            try
            {
                return Objwk.GetDataSet(Strsql);
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

        public DataSet FetchSeaUserExport(string Ves_Flight = "", long JobPk = 0, long PolPk = 0, long PodPk = 0, long CustPk = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 flag = 0, string strFromDate = "")
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Int32 last = default(Int32);
            Int32 start = default(Int32);

            if (strFromDate.Trim().Length > 0)
            {
                strCondition = strCondition + " AND JS.DEPARTURE_DATE >= To_date('" + strFromDate + "', dateformat)";
            }

            if (Ves_Flight.Trim().Length > 0 & Ves_Flight != "0")
            {
                strCondition = strCondition + " AND vvt.vessel_id = '" + Ves_Flight + "'";
            }

            if (flag == 0)
            {
                strCondition += " AND 1=2";
            }
            if (JobPk > 0)
            {
                strCondition = strCondition + " AND JS.JOB_CARD_TRN_PK = " + JobPk;
            }

            if (PolPk > 0)
            {
                strCondition = strCondition + " And BS.PORT_MST_POl_FK = " + PolPk;
            }

            if (PodPk > 0)
            {
                strCondition = strCondition + " And BS.PORT_MST_POD_FK = " + PodPk;
            }

            if (CustPk > 0)
            {
                strCondition = strCondition + " AND C.CUSTOMER_MST_PK=" + CustPk;
            }

            strCondition = strCondition + " AND JS.BUSINESS_TYPE = 2 ";
            strCondition = strCondition + " AND JS.PROCESS_TYPE = 1 ";

            Strsql = " SELECT Count(*) ";
            Strsql += " FROM JOB_CARD_TRN JS,HBL_EXP_TBL H,MBL_EXP_TBL M,";
            Strsql += " BOOKING_MST_TBL BS,PORT_MST_TBL POL,";
            Strsql += " PORT_MST_TBL POD,CUSTOMER_MST_TBL C,vessel_voyage_tbl  vvt,  vessel_voyage_trn  vtt";
            Strsql += " WHERE JS.BOOKING_MST_FK(+) = BS.BOOKING_MST_PK";
            Strsql += " AND H.JOB_CARD_SEA_EXP_FK(+)=JS.JOB_CARD_TRN_PK";
            Strsql += " AND H.HBL_EXP_TBL_PK(+)=JS.HBL_HAWB_FK";
            Strsql += " AND M.MBL_EXP_TBL_PK(+)=JS.MBL_MAWB_FK";
            Strsql += " AND POD.PORT_MST_PK(+)=BS.PORT_MST_POD_FK";
            Strsql += " AND POL.PORT_MST_PK(+)=BS.PORT_MST_POL_FK";
            Strsql += " AND C.CUSTOMER_MST_PK(+)=BS.CUST_CUSTOMER_MST_FK ";
            Strsql += "  AND vvt.vessel_voyage_tbl_pk(+)=vtt.vessel_voyage_tbl_fk";
            Strsql += " AND  js.voyage_trn_fk=vtt.voyage_trn_pk(+) ";
            Strsql += " AND JS.DEPARTURE_DATE IS NOT NULL  ";

            //If HttpContext.Current.Session("user_id") <> "admin" Then
            //    Strsql &= " and JS.created_by_fk= " & HttpContext.Current.Session("USER_PK")
            //End If

            Strsql += strCondition;
            //TotalRecords = (Int32)objWF.ExecuteScaler(Strsql);
            //TotalPage = TotalRecords / RecordsPerPage;
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

            Strsql = " select  * from (";
            Strsql += " SELECT ROWNUM AS SLNO,Q.* FROM (";
            Strsql += " SELECT JS.JOB_CARD_TRN_PK AS JOBCARDPK,";
            Strsql += " JS.BOOKING_MST_FK AS BOOKINGPK,JS.JOBCARD_REF_NO AS JOBREF, ";
            Strsql += " JS.VESSEL_NAME AS VSL_FLIGHT,";
            Strsql += " H.HBL_EXP_TBL_PK AS HBL_HAWB_PK,H.HBL_REF_NO AS HBL_HAWB_REF,";
            Strsql += " M.MBL_EXP_TBL_PK AS MBL_MAWB_PK,M.MBL_REF_NO AS MBL_MAWB_REF,";
            Strsql += " TO_CHAR(JS.DEPARTURE_DATE,'" + dateFormat + "') AS DEP,POL.PORT_NAME AS POL,";
            Strsql += " POD.PORT_NAME AS POD,C.CUSTOMER_NAME AS CUSTOMER ,'' AS SEL,2 BIZTYPE";
            Strsql += " FROM JOB_CARD_TRN JS,HBL_EXP_TBL H,MBL_EXP_TBL M,";
            Strsql += " BOOKING_MST_TBL BS,PORT_MST_TBL POL,";
            Strsql += " PORT_MST_TBL POD,CUSTOMER_MST_TBL C,vessel_voyage_tbl  vvt,  vessel_voyage_trn  vtt";
            Strsql += " WHERE JS.BOOKING_MST_FK(+) = BS.BOOKING_MST_PK";
            Strsql += " AND H.JOB_CARD_SEA_EXP_FK(+)=JS.JOB_CARD_TRN_PK";
            Strsql += " AND H.HBL_EXP_TBL_PK(+)=JS.HBL_HAWB_FK";
            Strsql += " AND M.MBL_EXP_TBL_PK(+)=JS.MBL_MAWB_FK";
            Strsql += " AND POD.PORT_MST_PK(+)=BS.PORT_MST_POD_FK";
            Strsql += " AND POL.PORT_MST_PK(+)=BS.PORT_MST_POL_FK";
            Strsql += " AND C.CUSTOMER_MST_PK(+)=BS.CUST_CUSTOMER_MST_FK ";
            Strsql += "  AND vvt.vessel_voyage_tbl_pk(+)=vtt.vessel_voyage_tbl_fk";
            Strsql += " AND  js.voyage_trn_fk=vtt.voyage_trn_pk(+) ";
            Strsql += " AND JS.DEPARTURE_DATE IS NOT NULL  ";

            //If HttpContext.Current.Session("user_id") <> "admin" Then
            //    Strsql &= " and JS.created_by_fk= " & HttpContext.Current.Session("USER_PK")
            //End If

            Strsql += strCondition;

            Strsql += " ORDER BY JS.DEPARTURE_DATE DESC, JS.JOBCARD_REF_NO DESC";

            Strsql += " )q) WHERE SLNO  Between " + start + " and " + last;
            try
            {
                return Objwk.GetDataSet(Strsql);
            }
            catch (Exception sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
        }

        public DataSet FetchSeaAirUserExport(string Ves_Flight = "", long JobPk = 0, long PolPk = 0, long PodPk = 0, long CustPk = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 flag = 0, string strFromDate = "")
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            string strCondition = null;
            string strCondition1 = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            if (strFromDate.Trim().Length > 0)
            {
                strCondition = strCondition + " AND JA.DEPARTURE_DATE >= To_date('" + strFromDate + "', dateformat)";
                strCondition1 = strCondition1 + " AND JS.DEPARTURE_DATE >= To_date('" + strFromDate + "', dateformat)";
            }

            if (Ves_Flight.Trim().Length > 0 & Ves_Flight != "0")
            {
                strCondition = strCondition + " AND air.airline_id = '" + Ves_Flight + "'";
                strCondition1 = strCondition1 + " AND vvt.vessel_id = '" + Ves_Flight + "'";
            }
            if (flag == 0)
            {
                strCondition += " AND 1=2";
                strCondition1 += " AND 1=2";
            }
            if (JobPk > 0)
            {
                strCondition = strCondition + " AND JA.JOB_CARD_TRN_PK = " + JobPk;
                strCondition1 = strCondition1 + " AND JS.JOB_CARD_TRN_PK = " + JobPk;
            }

            if (PolPk > 0)
            {
                strCondition = strCondition + " And BA.PORT_MST_POl_FK = " + PolPk;
                strCondition1 = strCondition1 + " And BS.PORT_MST_POl_FK = " + PolPk;
            }

            if (PodPk > 0)
            {
                strCondition = strCondition + " And BA.PORT_MST_POD_FK = " + PodPk;
                strCondition1 = strCondition1 + " And BS.PORT_MST_POD_FK = " + PodPk;
            }

            if (CustPk > 0)
            {
                strCondition = strCondition + " AND C.CUSTOMER_MST_PK=" + CustPk;
                strCondition1 = strCondition1 + " AND C.CUSTOMER_MST_PK=" + CustPk;
            }

            strCondition = strCondition + " AND JA.BUSINESS_TYPE = 1 ";
            strCondition = strCondition + " AND JA.PROCESS_TYPE = 1 ";

            strCondition1 = strCondition1 + " AND JS.BUSINESS_TYPE = 2 ";
            strCondition1 = strCondition1 + " AND JS.PROCESS_TYPE = 1 ";

            Strsql += " SELECT JA.JOB_CARD_TRN_PK AS JOBCARDPK ,";
            Strsql += " JA.BOOKING_MST_FK AS BOOKINGPK,JA.JOBCARD_REF_NO AS JOBREF, ";
            Strsql += " JA.VOYAGE_FLIGHT_NO AS VSL_FLIGHT,";
            Strsql += " HA.HAWB_EXP_TBL_PK AS HBL_HAWB_PK,HA.HAWB_REF_NO AS HBL_HAWB_REF,";
            Strsql += " MA.MAWB_EXP_TBL_PK AS MBL_MAWB_PK,MA.MAWB_REF_NO AS MBL_MAWB_REF,";
            Strsql += " JA.DEPARTURE_DATE AS DEP,POL.PORT_NAME AS POL,";
            Strsql += " POD.PORT_NAME AS POD,C.CUSTOMER_NAME AS CUSTOMER,'' AS SEL,1 BIZTYPE";
            Strsql += " FROM JOB_CARD_TRN JA,HAWB_EXP_TBL HA,MAWB_EXP_TBL MA,";
            Strsql += " BOOKING_MST_TBL BA,PORT_MST_TBL POL,";
            Strsql += "  PORT_MST_TBL POD,CUSTOMER_MST_TBL C, airline_mst_tbl air, BOOKING_TRN  bta";
            Strsql += " WHERE  JA.BOOKING_MST_FK(+)=BA.BOOKING_MST_PK";
            Strsql += " AND HA.JOB_CARD_AIR_EXP_FK(+)=JA.JOB_CARD_TRN_PK";
            Strsql += " AND HA.HAWB_EXP_TBL_PK(+)=JA.HBL_HAWB_FK";
            Strsql += " AND MA.MAWB_EXP_TBL_PK(+)=JA.MBL_MAWB_FK";
            Strsql += " AND POD.PORT_MST_PK(+)=BA.PORT_MST_POD_FK";
            Strsql += " AND POL.PORT_MST_PK(+)=BA.PORT_MST_POL_FK";

            Strsql += " AND BA.booking_mst_pk=ja.booking_mst_fk  ";
            Strsql += " AND  bta.booking_mst_fk(+)=ba.booking_mst_pk ";
            Strsql += " AND  air.airline_mst_pk=ba.carrier_mst_fk ";

            Strsql += " AND C.CUSTOMER_MST_PK(+)=BA.CUST_CUSTOMER_MST_FK ";

            Strsql += " AND JA.DEPARTURE_DATE IS NOT NULL  ";
            //If HttpContext.Current.Session("user_id") <> "admin" Then
            //    Strsql &= " and JA.created_by_fk= " & HttpContext.Current.Session("USER_PK")
            //End If
            Strsql += strCondition;
            Strsql += "  UNION";
            Strsql += " SELECT JS.JOB_CARD_TRN_PK AS JOBCARDPK,";
            Strsql += " JS.BOOKING_MST_FK AS BOOKINGPK,JS.JOBCARD_REF_NO AS JOBREF, ";
            Strsql += " JS.VESSEL_NAME AS VSL_FLIGHT,";
            Strsql += " H.HBL_EXP_TBL_PK AS HBL_HAWB_PK,H.HBL_REF_NO AS HBL_HAWB_REF,";
            Strsql += " M.MBL_EXP_TBL_PK AS MBL_MAWB_PK,M.MBL_REF_NO AS MBL_MAWB_REF,";
            Strsql += " JS.DEPARTURE_DATE AS DEP,POL.PORT_NAME AS POL,";
            Strsql += " POD.PORT_NAME AS POD,C.CUSTOMER_NAME AS CUSTOMER ,'' AS SEL,2 BIZTYPE";
            Strsql += " FROM JOB_CARD_TRN JS,HBL_EXP_TBL H,MBL_EXP_TBL M,";
            Strsql += " BOOKING_MST_TBL BS,PORT_MST_TBL POL,";
            Strsql += " PORT_MST_TBL POD,CUSTOMER_MST_TBL C, vessel_voyage_tbl  vvt,  vessel_voyage_trn  vtt";
            Strsql += " WHERE JS.BOOKING_MST_FK(+) = BS.BOOKING_MST_PK";
            Strsql += " AND H.JOB_CARD_SEA_EXP_FK(+)=JS.JOB_CARD_TRN_PK";
            Strsql += " AND H.HBL_EXP_TBL_PK(+)=JS.HBL_HAWB_FK";
            Strsql += " AND M.MBL_EXP_TBL_PK(+)=JS.MBL_MAWB_FK";
            Strsql += " AND POD.PORT_MST_PK(+)=BS.PORT_MST_POD_FK";
            Strsql += " AND POL.PORT_MST_PK(+)=BS.PORT_MST_POL_FK";
            Strsql += " AND C.CUSTOMER_MST_PK(+)=BS.CUST_CUSTOMER_MST_FK ";

            Strsql += "  AND vvt.vessel_voyage_tbl_pk(+)=vtt.vessel_voyage_tbl_fk";
            Strsql += " AND  js.voyage_trn_fk=vtt.voyage_trn_pk(+) ";

            Strsql += " AND JS.DEPARTURE_DATE IS NOT NULL  ";
            //If HttpContext.Current.Session("user_id") <> "admin" Then
            //    Strsql &= " and JS.created_by_fk= " & HttpContext.Current.Session("USER_PK")
            //End If
            Strsql += strCondition1;

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*)  from  ");
            strCount.Append((" (" + Strsql.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));
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
            strCount.Remove(0, strCount.Length);

            System.Text.StringBuilder sqlstr2 = new System.Text.StringBuilder();
            sqlstr2.Append(" SELECT q1.* FROM (SELECT   ROWNUM AS SLNO,Q.* FROM ( ");
            sqlstr2.Append(" SELECT * FROM ");
            sqlstr2.Append("  (" + Strsql.ToString() + " ");
            sqlstr2.Append("  ) ORDER BY 9 DESC,3 DESC ) Q) Q1 WHERE q1.slno  Between " + start + " and " + last + "");
            try
            {
                return Objwk.GetDataSet(sqlstr2.ToString());
            }
            catch (Exception sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
        }

        #endregion "Fetch For SEA/AIR Export"

        #region "Enhance Search & Lookup Search Block "

        public string FetchExpCertificateJob(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strLOC_MST_IN = null;
            string strSERACH_IN = null;
            string strBizType = null;
            string strReq = null;
            string strLocPK = null;
            var strNull = "";
            //arr = strCond.Split("~");
            //strReq = arr(0);
            //strSERACH_IN = arr(1);
            //strBizType = arr(2);
            //strLocPK = arr(3);
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_EXP_CERTIFICATE_JOB_PKG.GET_EXP_REP_JOB_REF_COMMON";

                var _with1 = selectCommand.Parameters;
                _with1.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with1.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with1.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with1.Add("LOCATION_MST_FK_IN", strLocPK).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
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

        public string FetchExpCertificateVesFlight(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string strBizType = null;
            string strReq = null;
            var strNull = "";
            //arr = strCond.Split("~");
            //strReq = arr(0);
            //strSERACH_IN = arr(1);
            //strBizType = arr(2);
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_EXP_VESFLIGHT_PKG.GET_VES_FLIGHT_COMMON";

                var _with2 = selectCommand.Parameters;
                _with2.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with2.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with2.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with2.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
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

        #endregion "Enhance Search & Lookup Search Block "

        #region "FetchContainers -- Main -- SEA User"

        public DataSet FetchSeaMain(string JobRefNos)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();

            Strsql = " SELECT JOBCARDPK,";
            Strsql += "JOBCARDNO,UCRNO,CONSIGNAME,CONSIGADD1,CONSIGADD2,";
            Strsql += "CONSIGADD3,CONSIGZIP,CONSIGPHONE,CONSIGCITY,CONFAX,";
            Strsql += "CONEMAIL,CONSCOUNTRY,SHIPPERNAME,SHIPPERADD1,SHIPPERADD2,";
            Strsql += "SHIPPERADD3,SHIPPERZIP,SHIPPERPHONE, SHIPPERCITY,SHIPPERFAX,";
            Strsql += "SHIPPEREMAIL,SHIPCOUNTRY, ";
            Strsql += "DPAGENT, DPADD1,DPADD2,DPADD3, DPCITY,DPZIP,";
            Strsql += "DPPHONE,DPFAX,DPEMAIL,DPCOUNTRY,";
            Strsql += "VES_VOY, CARGO_TYPE,COMMTYPE,REFNO, ";
            Strsql += "POLNAME,PODNAME,";
            Strsql += "DEL_PLACE_NAME,COL_PLACE_NAME,";
            Strsql += "GOODS,ETD,MARKS,TERMS,INSURANCE,PYMT_TYPE,";
            Strsql += "SUM(GROSSWEIGHT) GROSSWEIGHT,";
            Strsql += "SUM(NETWEIGHT) NETWEIGHT,";
            Strsql += " SUM(CHARWT) CHARWT,";
            Strsql += " SUM(VOLUME) VOLUME,";
            Strsql += "  MAX(ETA) ETA";
            Strsql += "FROM";
            Strsql += "(select JSE.JOB_CARD_TRN_PK JOBCARDPK,";
            Strsql += "JSE.JOBCARD_REF_NO JOBCARDNO,";
            Strsql += "JSE.UCR_NO         AS UCRNO,";
            Strsql += "CONSIGCUST.CUSTOMER_NAME       CONSIGNAME,";
            Strsql += "CONSIGCUSTDTLS.ADM_ADDRESS_1   CONSIGADD1,";
            Strsql += "CONSIGCUSTDTLS.ADM_ADDRESS_2   CONSIGADD2,";
            Strsql += "CONSIGCUSTDTLS.ADM_ADDRESS_3   CONSIGADD3,";
            Strsql += "CONSIGCUSTDTLS.ADM_ZIP_CODE    CONSIGZIP,";
            Strsql += "CONSIGCUSTDTLS.ADM_PHONE_NO_1  CONSIGPHONE,";
            Strsql += "CONSIGCUSTDTLS.ADM_CITY        CONSIGCITY,";
            Strsql += "CONSIGCUSTDTLS.ADM_FAX_NO      CONFAX,";
            Strsql += "CONSIGCUSTDTLS.ADM_EMAIL_ID    CONEMAIL,";
            Strsql += "CONSCOUNTRY.COUNTRY_NAME       CONSCOUNTRY,";
            Strsql += "SHIPPERCUST.CUSTOMER_NAME      SHIPPERNAME,";
            Strsql += "SHIPPERCUSTDTLS.ADM_ADDRESS_1  SHIPPERADD1,";
            Strsql += "SHIPPERCUSTDTLS.ADM_ADDRESS_2  SHIPPERADD2,";
            Strsql += "SHIPPERCUSTDTLS.ADM_ADDRESS_3  SHIPPERADD3,";
            Strsql += "SHIPPERCUSTDTLS.ADM_ZIP_CODE   SHIPPERZIP,";
            Strsql += "SHIPPERCUSTDTLS.ADM_PHONE_NO_1 SHIPPERPHONE,";
            Strsql += "SHIPPERCUSTDTLS.ADM_CITY       SHIPPERCITY,";
            Strsql += "SHIPPERCUSTDTLS.ADM_FAX_NO     SHIPPERFAX,";
            Strsql += "SHIPPERCUSTDTLS.ADM_EMAIL_ID   SHIPPEREMAIL,";
            Strsql += "SHIPCOUNTRY.COUNTRY_NAME       SHIPCOUNTRY, ";
            Strsql += "DPAGENTMST.AGENT_NAME          DPAGENT,";
            Strsql += "DPAGENTDTLS.ADM_ADDRESS_1      DPADD1,";
            Strsql += "DPAGENTDTLS.ADM_ADDRESS_2      DPADD2,";
            Strsql += "DPAGENTDTLS.ADM_ADDRESS_3      DPADD3,";
            Strsql += "DPAGENTDTLS.ADM_CITY           DPCITY,";
            Strsql += "DPAGENTDTLS.ADM_ZIP_CODE       DPZIP,";
            Strsql += "DPAGENTDTLS.ADM_PHONE_NO_1     DPPHONE,";
            Strsql += "DPAGENTDTLS.ADM_FAX_NO         DPFAX,";
            Strsql += "DPAGENTDTLS.ADM_EMAIL_ID       DPEMAIL,";
            Strsql += "DPAGENTCOUNTRY.COUNTRY_NAME    DPCOUNTRY,      ";
            Strsql += "(CASE";
            Strsql += "WHEN JSE.VOYAGE_FLIGHT_NO IS NOT NULL THEN";
            Strsql += "JSE.VESSEL_NAME || '/' || JSE.VOYAGE_FLIGHT_NO";
            Strsql += "ELSE";
            Strsql += "JSE.VESSEL_NAME";
            Strsql += "END) VES_VOY,";

            Strsql += "BST.CARGO_TYPE,";
            Strsql += "CTMST.COMMODITY_GROUP_DESC  COMMTYPE,";
            Strsql += "(CASE";
            Strsql += "WHEN JSE.HBL_HAWB_FK IS NOT NULL THEN";
            Strsql += "(SELECT HBL.HBL_REF_NO";
            Strsql += "FROM HBL_EXP_TBL HBL";
            Strsql += "WHERE JSE.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK)";
            Strsql += " ELSE";
            Strsql += "(SELECT MBL.MBL_REF_NO";
            Strsql += " FROM MBL_EXP_TBL MBL";
            Strsql += " WHERE JSE.MBL_MAWB_FK = MBL.MBL_EXP_TBL_PK)";
            Strsql += "END) REFNO,";
            Strsql += "POL.PORT_NAME POLNAME,";
            Strsql += "POD.PORT_NAME PODNAME,";
            Strsql += "DELMST.PLACE_NAME DEL_PLACE_NAME,";
            Strsql += "COLMST.PLACE_NAME COL_PLACE_NAME,";
            Strsql += "(CASE";
            Strsql += "WHEN JSE.HBL_HAWB_FK IS NOT NULL THEN";
            Strsql += "(SELECT HBL.GOODS_DESCRIPTION";
            Strsql += "FROM HBL_EXP_TBL HBL";
            Strsql += "WHERE JSE.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK)";
            Strsql += " ELSE";
            Strsql += "(SELECT MBL.GOODS_DESCRIPTION";
            Strsql += " FROM MBL_EXP_TBL MBL";
            Strsql += " WHERE JSE.MBL_MAWB_FK = MBL.MBL_EXP_TBL_PK)";
            Strsql += "END) GOODS,";

            Strsql += "JSE.ETD_DATE ETD,";
            Strsql += "(CASE";
            Strsql += "WHEN JSE.HBL_HAWB_FK IS NOT NULL THEN";
            Strsql += " (SELECT HBL.MARKS_NUMBERS";
            Strsql += " FROM HBL_EXP_TBL HBL";
            Strsql += " WHERE JSE.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK)";
            Strsql += " ELSE";
            Strsql += " (SELECT MBL.MARKS_NUMBERS";
            Strsql += "  FROM MBL_EXP_TBL MBL";
            Strsql += " WHERE JSE.MBL_MAWB_FK = MBL.MBL_EXP_TBL_PK)";
            Strsql += "END) MARKS,";
            Strsql += "STMST.INCO_CODE TERMS,";
            Strsql += "NVL(JSE.INSURANCE_AMT, 0) INSURANCE,";
            Strsql += "JSE.PYMT_TYPE PYMT_TYPE,";
            Strsql += "(JTSC.GROSS_WEIGHT) GROSSWEIGHT,";
            Strsql += "(JTSC.NET_WEIGHT) NETWEIGHT,";
            Strsql += "(JTSC.CHARGEABLE_WEIGHT) CHARWT,";
            Strsql += "(JTSC.VOLUME_IN_CBM) VOLUME,";
            Strsql += "JSE.ETA_DATE ETA ";
            Strsql += "from JOB_CARD_TRN   JSE,";
            Strsql += " BOOKING_MST_TBL        BST,";
            Strsql += " CUSTOMER_MST_TBL       CONSIGCUST,";
            Strsql += " CUSTOMER_CONTACT_DTLS  CONSIGCUSTDTLS,";
            Strsql += " CUSTOMER_CONTACT_DTLS  SHIPPERCUSTDTLS,";
            Strsql += " CUSTOMER_MST_TBL       SHIPPERCUST,";
            Strsql += " AGENT_MST_TBL          DPAGENTMST,";
            Strsql += " AGENT_CONTACT_DTLS     DPAGENTDTLS,";
            Strsql += " PORT_MST_TBL           POL,";
            Strsql += " PORT_MST_TBL           POD,";
            Strsql += " JOB_TRN_CONT           JTSC,";
            Strsql += " SHIPPING_TERMS_MST_TBL STMST,";
            Strsql += " COUNTRY_MST_TBL        SHIPCOUNTRY,";
            Strsql += " COUNTRY_MST_TBL        CONSCOUNTRY,";
            Strsql += " COUNTRY_MST_TBL        DPAGENTCOUNTRY,";
            Strsql += " PLACE_MST_TBL          DELMST,";
            Strsql += " COMMODITY_GROUP_MST_TBL CTMST,";
            Strsql += " PLACE_MST_TBL COLMST";
            Strsql += "WHERE CONSIGCUST.CUSTOMER_MST_PK(+) = JSE.CONSIGNEE_CUST_MST_FK";
            Strsql += "AND SHIPPERCUST.CUSTOMER_MST_PK(+) = JSE.SHIPPER_CUST_MST_FK";
            Strsql += " AND CONSIGCUSTDTLS.CUSTOMER_MST_FK(+) = CONSIGCUST.CUSTOMER_MST_PK";
            Strsql += " AND SHIPPERCUSTDTLS.CUSTOMER_MST_FK(+) = SHIPPERCUST.CUSTOMER_MST_PK";
            Strsql += "AND POL.PORT_MST_PK(+) = BST.PORT_MST_POL_FK";
            Strsql += "AND POD.PORT_MST_PK(+) = BST.PORT_MST_POD_FK";
            Strsql += "AND BST.BOOKING_MST_PK = JSE.BOOKING_MST_FK";
            Strsql += "AND JTSC.JOB_CARD_TRN_FK(+) = JSE.JOB_CARD_TRN_PK";
            Strsql += "AND STMST.SHIPPING_TERMS_MST_PK(+) = JSE.SHIPPING_TERMS_MST_FK";
            Strsql += "AND CONSCOUNTRY.COUNTRY_MST_PK(+) = CONSIGCUSTDTLS.ADM_COUNTRY_MST_FK";
            Strsql += "AND SHIPCOUNTRY.COUNTRY_MST_PK(+) = SHIPPERCUSTDTLS.ADM_COUNTRY_MST_FK";
            Strsql += "AND DELMST.PLACE_PK(+) = BST.DEL_PLACE_MST_FK";
            Strsql += "AND COLMST.PLACE_PK(+)=BST.COL_PLACE_MST_FK";
            Strsql += "AND DPAGENTMST.AGENT_MST_PK(+)=JSE.DP_AGENT_MST_FK";
            Strsql += "AND DPAGENTDTLS.AGENT_MST_FK(+)=DPAGENTMST.AGENT_MST_PK";
            Strsql += "AND DPAGENTCOUNTRY.COUNTRY_MST_PK(+)=DPAGENTDTLS.ADM_COUNTRY_MST_FK";
            Strsql += "AND CTMST.COMMODITY_GROUP_PK(+)=JSE.COMMODITY_GROUP_FK";
            //Strsql &= vbCrLf & " AND JSE.BUSINESS_TYPE = 2 "
            //Strsql &= vbCrLf & " AND JSE.PROCESS_TYPE = 1 "
            Strsql += "AND JSE.JOB_CARD_TRN_PK IN (" + JobRefNos + " ))";

            Strsql += "GROUP BY JOBCARDPK,";
            Strsql += " JOBCARDNO,UCRNO,";
            Strsql += " CONSIGNAME,CONSIGADD1,CONSIGADD2,CONSIGADD3,CONSIGZIP,CONSIGPHONE,";
            Strsql += "  CONSIGCITY,CONFAX,CONEMAIL,CONSCOUNTRY,";
            Strsql += "  SHIPPERNAME,SHIPPERADD1,SHIPPERADD2,SHIPPERADD3,SHIPPERZIP,";
            Strsql += "  SHIPPERPHONE,SHIPPERCITY,SHIPPERFAX,SHIPPEREMAIL,SHIPCOUNTRY, ";
            Strsql += "  DPAGENT,DPADD1,DPADD2,DPADD3, DPCITY,";
            Strsql += "  DPZIP,DPPHONE,DPFAX,DPEMAIL,DPCOUNTRY,      ";
            Strsql += "  VES_VOY,CARGO_TYPE,COMMTYPE,REFNO, POLNAME,PODNAME,";
            Strsql += "  DEL_PLACE_NAME,COL_PLACE_NAME,";
            Strsql += "  GOODS,ETD,MARKS,TERMS,INSURANCE,";
            Strsql += " PYMT_TYPE";

            try
            {
                return Objwk.GetDataSet(Strsql);
            }
            catch (Exception sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
        }

        public DataSet FetchSeaContainers(string JobRefNos)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = " SELECT JSE.JOB_CARD_TRN_FK JOBPK ,JSE.CONTAINER_NUMBER CONTAINER FROM JOB_TRN_CONT JSE ";
            Strsql += "WHERE JSE.JOB_CARD_TRN_FK IN(" + JobRefNos + ")";

            try
            {
                return Objwk.GetDataSet(Strsql);
            }
            catch (Exception sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
        }

        public DataSet FetchCollectChargesSeaDetails(string JobCardPK)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = "select JSFD.JOB_TRN_FD_PK JOBFDEXPPK,";
            Strsql += "JSFD.JOB_CARD_TRN_FK JOBEXPPK,";
            Strsql += "JSFD.FREIGHT_ELEMENT_MST_FK,";
            Strsql += "FREELE.FREIGHT_ELEMENT_ID,";
            Strsql += "FREELE.FREIGHT_ELEMENT_NAME,";
            Strsql += "JSFD.FREIGHT_AMT,";
            Strsql += "CURRTYPE.CURRENCY_ID,";
            Strsql += "CURRTYPE.CURRENCY_NAME";
            Strsql += "from JOB_TRN_FD JSFD,";
            Strsql += "JOB_CARD_TRN JSI,";
            Strsql += "FREIGHT_ELEMENT_MST_TBL FREELE,";
            Strsql += "CURRENCY_TYPE_MST_TBL CURRTYPE";
            Strsql += "WHERE FREELE.FREIGHT_ELEMENT_MST_PK = JSFD.FREIGHT_ELEMENT_MST_FK";
            Strsql += "AND    CURRTYPE.CURRENCY_MST_PK=JSFD.CURRENCY_MST_FK";
            Strsql += "AND    JSI.JOB_CARD_TRN_PK=JSFD.JOB_CARD_TRN_FK";
            Strsql += "and JSFD.Freight_Type = 2";
            //  Strsql &= vbCrLf & " AND JSI.BUSINESS_TYPE = 2 "
            //   Strsql &= vbCrLf & " AND JSI.PROCESS_TYPE = 1 "
            Strsql += "AND FREELE.FREIGHT_ELEMENT_ID LIKE 'BOF'";
            Strsql += "AND jsfd.job_card_TRN_fk IN (" + JobCardPK + ")";
            try
            {
                return Objwk.GetDataSet(Strsql);
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

        #endregion "FetchContainers -- Main -- SEA User"

        #region "Fetch For Air Users --- Main"

        public DataSet FetchAirFlightMain(string JobRefNos)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();

            Strsql = "SELECT JAE.JOB_CARD_TRN_PK JOBCARDPK," +
                     "JAE.JOBCARD_REF_NO JOBCARDNO," +
                     "SHIPPER.CUSTOMER_NAME SHIPPERNAME," +
                     "SHIPPERCUSTDTLS.ADM_ADDRESS_1 SHIPPERADD1," +
                     "SHIPPERCUSTDTLS.ADM_ADDRESS_2 SHIPPERADD2," +
                     "SHIPPERCUSTDTLS.ADM_ADDRESS_3 SHIPPERADD3," +
                     "SHIPPERCUSTDTLS.ADM_ZIP_CODE SHIPPERZIP," +
                     "SHIPPERCUSTDTLS.ADM_PHONE_NO_1 SHIPPERPHONE," +
                     "SHIPPERCUSTDTLS.ADM_CITY SHIPPERCITY," +
                     "SHIPPERCUSTDTLS.ADM_FAX_NO SHIPPERFAX," +
                     "SHIPPERCUSTDTLS.ADM_EMAIL_ID SHIPPEREMAIL," +
                     "SHIPCOUNTRY.COUNTRY_NAME SHIPCOUNTRY," +
                     "JAE.VOYAGE_FLIGHT_NO FLIGHT," +
                     "" +
                     "HAWB.HAWB_REF_NO HAWBREFNO," +
                     "MAWB.MAWB_REF_NO MAWBREFNO," +
                     "JAE.DEPARTURE_DATE ATDPOL," +
                     "SUM(JAEC.CHARGEABLE_WEIGHT) CHARWEIGHT," +
                     "SUM(JAEC.PACK_COUNT) PCES" +
                     "FROM" +
                     " JOB_CARD_TRN   JAE," +
                     " JOB_TRN_CONT JAEC," +
                     " BOOKING_MST_TBL BAT," +
                     "           CUSTOMER_CONTACT_DTLS  SHIPPERCUSTDTLS," +
                     " CUSTOMER_MST_TBL       SHIPPER," +
                     "          PORT_MST_TBL           POL," +
                     "          PORT_MST_TBL           POD," +
                     "COUNTRY_MST_TBL        SHIPCOUNTRY," +
                     "           HAWB_EXP_TBL HAWB," +
                     "           MAWB_EXP_TBL MAWB" +
                     "" +
                     " WHERE" +
                     " SHIPPER.CUSTOMER_MST_PK(+) = JAE.SHIPPER_CUST_MST_FK" +
                     "AND SHIPPERCUSTDTLS.CUSTOMER_MST_FK(+) = SHIPPER.CUSTOMER_MST_PK" +
                     "" +
                     "AND SHIPCOUNTRY.COUNTRY_MST_PK(+) = SHIPPERCUSTDTLS.ADM_COUNTRY_MST_FK" +
                     "AND HAWB.HAWB_EXP_TBL_PK(+)=JAE.HBL_HAWB_FK" +
                     "AND MAWB.MAWB_EXP_TBL_PK(+)=JAE.MBL_MAWB_FK" +
                     "and JAEC.JOB_CARD_TRN_FK(+)=JAE.JOB_CARD_TRN_PK" +
                     "AND BAT.PORT_MST_POL_FK = POL.PORT_MST_PK(+)" +
                     "AND BAT.PORT_MST_POD_FK = POD.PORT_MST_PK(+)" +
                     "AND JAE.BOOKING_MST_FK = BAT.BOOKING_MST_PK" +
                     "         AND JAE.JOB_CARD_TRN_PK IN (" + JobRefNos + ") AND JAE.BUSINESS_TYPE = 1 AND JAE.PROCESS_TYPE = 1 " +
                     "" +
                     "GROUP BY" +
                     "           JAE.JOB_CARD_TRN_PK ," +
                     "JAE.JOBCARD_REF_NO," +
                     "SHIPPER.CUSTOMER_NAME," +
                     "SHIPPERCUSTDTLS.ADM_ADDRESS_1 ," +
                     "SHIPPERCUSTDTLS.ADM_ADDRESS_2 ," +
                     "SHIPPERCUSTDTLS.ADM_ADDRESS_3 ," +
                     "SHIPPERCUSTDTLS.ADM_ZIP_CODE ," +
                     "SHIPPERCUSTDTLS.ADM_PHONE_NO_1," +
                     "SHIPPERCUSTDTLS.ADM_CITY ," +
                     "SHIPPERCUSTDTLS.ADM_FAX_NO," +
                     "SHIPPERCUSTDTLS.ADM_EMAIL_ID ," +
                     "SHIPCOUNTRY.COUNTRY_NAME ," +
                     "JAE.VOYAGE_FLIGHT_NO ," +
                     "HAWB.HAWB_REF_NO," +
                     "MAWB.MAWB_REF_NO," +
                     "JAE.DEPARTURE_DATE";

            try
            {
                return Objwk.GetDataSet(Strsql);
            }
            catch (Exception sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
        }

        public DataSet FetchAirInvoice(string JobRefNos)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = "SELECT JAE.JOB_CARD_TRN_PK JOBCARDPK," + "JAE.JOBCARD_REF_NO JOBCARDNO," + "INVEXP.INVOICE_REF_NO INVOICENO" + "FROM" + " JOB_CARD_TRN   JAE," + " INV_CUST_AIR_EXP_TBL INVEXP" + "" + " WHERE" + "INVEXP.JOB_CARD_AIR_EXP_FK=JAE.JOB_CARD_TRN_PK AND JAE.BUSINESS_TYPE = 1 AND JAE.PROCESS_TYPE = 1 " + " AND JAE.JOB_CARD_TRN_PK IN(" + JobRefNos + ")";

            try
            {
                return Objwk.GetDataSet(Strsql);
            }
            catch (Exception sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
        }

        public DataSet FetchCollectChargesAirDetails(string JobCardPK)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = "select JAFD.JOB_TRN_FD_PK JOBFDEXPPK,";
            Strsql += "JAFD.JOB_CARD_TRN_FK JOBEXPPK,";
            Strsql += "JAFD.FREIGHT_ELEMENT_MST_FK,";
            Strsql += "FREELE.FREIGHT_ELEMENT_ID,";
            Strsql += "FREELE.FREIGHT_ELEMENT_NAME,";
            Strsql += "JAFD.FREIGHT_AMT,";
            Strsql += "CURRTYPE.CURRENCY_ID,";
            Strsql += "CURRTYPE.CURRENCY_NAME";
            Strsql += "from   JOB_TRN_FD JAFD,";
            Strsql += "JOB_CARD_TRN JAI,";
            Strsql += "FREIGHT_ELEMENT_MST_TBL FREELE,";
            Strsql += "CURRENCY_TYPE_MST_TBL CURRTYPE";
            Strsql += "WHERE FREELE.FREIGHT_ELEMENT_MST_PK = JAFD.FREIGHT_ELEMENT_MST_FK";
            Strsql += "AND    CURRTYPE.CURRENCY_MST_PK=JAFD.CURRENCY_MST_FK";
            Strsql += "AND    JAI.JOB_CARD_TRN_PK=JAFD.JOB_CARD_TRN_FK";
            Strsql += "and JAFD.Freight_Type = 2 AND JAI.BUSINESS_TYPE = 1 AND JAI.PROCESS_TYPE = 1 ";
            Strsql += "AND FREELE.FREIGHT_ELEMENT_ID LIKE 'AFC'";
            Strsql += "AND jAfd.job_card_TRN_fk IN (" + JobCardPK + ")";

            try
            {
                return Objwk.GetDataSet(Strsql);
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

        #endregion "Fetch For Air Users --- Main"
    }
}