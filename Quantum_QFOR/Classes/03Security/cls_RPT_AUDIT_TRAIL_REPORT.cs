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

using Newtonsoft.Json;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Data;
namespace Quantum_QFOR
{
    public class cls_RPT_AUDIT_TRAIL_REPORT : CommonFeatures
	{

		#region "Fetch Function"

		public DataSet FetchAll(Int64 UserID = 0, Int64 Modules = 0, string Activity = "", string SortBy = "", string FromDate = "", string ToDate = "", string location = "")
		{


			string strSQL = null;
			string dt = null;


			//strSQL = "SELECT USER_NAME, CMT.CONFIG_NAME, ACTIVITY_DATE, ACTIVITY_TYPE, PARAMETERS,REMARKS "
			strSQL = "SELECT USER_NAME, CMT.CONFIG_NAME,";
			//If SortBy = "ACTIVITY_DATE" Then
			//    dt = "to_char(ual.activity_date,'dd-mon-yyyy') ACTIVITY_DATE "
			//Else
			//dt = " ACTIVITY_DATE "
			//End If
			//strSQL = strSQL + dt
			strSQL = strSQL + "ACTIVITY_DATE, ACTIVITY_TYPE, PARAMETERS,REMARKS  FROM USER_ACTIVITY_LOG UAL, USER_MST_TBL UMT,CONFIG_MST_TBL CMT ";
			strSQL = strSQL + " WHERE UMT.USER_MST_PK=UAL.USER_MST_FK  ";
			strSQL = strSQL + " AND UAL.CONFIG_MST_FK=CMT.CONFIG_MST_PK ";

			if (Modules > 0) {
				strSQL = strSQL + " and ual.config_mst_fk = " + Modules;
			}

			if (UserID > 0) {
				strSQL = strSQL + " and ual.user_mst_fk = " + UserID;
			}

			if (!string.IsNullOrEmpty(FromDate) & !string.IsNullOrEmpty(ToDate)) {
				string frmDt = System.String.Format("{0:dd-MMM-yyyy}", Convert.ToDateTime(FromDate));
				string toDt = System.String.Format("{0:dd-MMM-yyyy}", Convert.ToDateTime(FromDate));
                //strSQL = strSQL + " and activity_date between '" & frmDt.ToLower & "' and '" & toDt.ToLower & "'"
                //strSQL = strSQL + " and activity_date  between '" & frmDt.ToUpper & "' and '" & toDt.ToUpper & "'"
                strSQL = strSQL + " and activity_date  between '" + frmDt.ToUpper() + "' and to_date('" + toDt.ToUpper() + "','dd-mon-yyyy')+1";
			} else if (!string.IsNullOrEmpty(FromDate) & string.IsNullOrEmpty(ToDate)) {
				string frmDt = System.String.Format("{0:dd-MMM-yyyy}", Convert.ToDateTime(FromDate));
                strSQL = strSQL + " and ual.activity_date >= '" + frmDt.ToUpper() + "'";
				//added >
			} else if (string.IsNullOrEmpty(FromDate) & !string.IsNullOrEmpty(ToDate)) {
				string toDt = System.String.Format("{0:dd-MMM-yyyy}", Convert.ToDateTime(FromDate));
                strSQL = strSQL + " and ual.activity_date <= to_date('" + toDt.ToUpper() + "','dd-mon-yyyy')+1";
				//added <
			}

			if (!string.IsNullOrEmpty(Activity)) {
				strSQL = strSQL + " and activity_type='" + Activity + "'";
			}

			//commented on 22-Jul-2005
			//If location <> "" Then
			//    strSQL = strSQL + " and DEFAULT_LOCATION_FK='" & location & "'"
			//End If

			//If SortBy <> "" Then
			//commented and modified by latha to show report in order according to the date
			//strSQL = strSQL + " order by   " & SortBy & "  DESC "
			strSQL = strSQL + " order by ACTIVITY_DATE DESC ";
			//If SortBy = "ACTIVITY_DATE" Then
			//strS'QL = strSQL + " DESC "
			// End If
			//End If



			//strSQL = strSQL + "AND T.LOCATION_MST_FK=" & location & " "

			WorkFlow objWF = new WorkFlow();

			try {
				return objWF.GetDataSet(strSQL);
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}


		}
        public string FetchAllNew(Int64 UserID = 0, Int64 Modules = 0, string Activity = "", string SortBy = "", string FromDate = "", string ToDate = "", string location = "", Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {

            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            string dt = null;
            WorkFlow objWF = new WorkFlow();

            strSQL = "SELECT Count(*)FROM(SELECT * FROM(SELECT ROWNUM SLNO, Q.* from(SELECT USER_NAME UserName, CMT.CONFIG_NAME FormName,REMARKS  Remarks,";
            strSQL = strSQL + "ACTIVITY_TYPE Activity, PARAMETERS Parameters,ACTIVITY_DATE DateandTime  FROM USER_ACTIVITY_LOG UAL, USER_MST_TBL UMT,CONFIG_MST_TBL CMT ";
            strSQL = strSQL + " WHERE UMT.USER_MST_PK=UAL.USER_MST_FK  ";
            strSQL = strSQL + " AND UAL.CONFIG_MST_FK=CMT.CONFIG_MST_PK";

            if (Modules > 0)
            {
                strSQL = strSQL + " and ual.config_mst_fk = " + Modules;
            }

            if (UserID > 0)
            {
                strSQL = strSQL + " and ual.user_mst_fk = " + UserID;
            }

            if (!string.IsNullOrEmpty(FromDate) & !string.IsNullOrEmpty(ToDate))
            {
                string frmDt = System.String.Format("{0:dd-MMM-yyyy}", Convert.ToDateTime(FromDate));
                string toDt = System.String.Format("{0:dd-MMM-yyyy}", Convert.ToDateTime(FromDate));
                strSQL = strSQL + " and activity_date  between '" + frmDt.ToUpper() + "' and to_date('" + toDt.ToUpper() + "','dd-mon-yyyy')+1";
            }
            else if (!string.IsNullOrEmpty(FromDate) & string.IsNullOrEmpty(ToDate))
            {
                string frmDt = System.String.Format("{0:dd-MMM-yyyy}", Convert.ToDateTime(FromDate));
                strSQL = strSQL + " and ual.activity_date >= '" + frmDt.ToUpper() + "'";
                //added >
            }
            else if (string.IsNullOrEmpty(FromDate) & !string.IsNullOrEmpty(ToDate))
            {
                string toDt = System.String.Format("{0:dd-MMM-yyyy}", Convert.ToDateTime(FromDate));
                strSQL = strSQL + " and ual.activity_date <= to_date('" + toDt.ToUpper() + "','dd-mon-yyyy')+1";
                //added <
            }

            if (!string.IsNullOrEmpty(Activity))
            {
                strSQL = strSQL + " and activity_type='" + Activity + "'";
            }
            strSQL = strSQL + " order by ACTIVITY_DATE DESC) Q))";

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

            strSQL = "SELECT * FROM(SELECT ROWNUM SLNO, Q.* from(SELECT USER_NAME UserName, CMT.CONFIG_NAME FormName,REMARKS  Remarks,";
        
            strSQL = strSQL + "ACTIVITY_TYPE Activity, PARAMETERS Parameters,ACTIVITY_DATE DateandTime  FROM USER_ACTIVITY_LOG UAL, USER_MST_TBL UMT,CONFIG_MST_TBL CMT ";
            strSQL = strSQL + " WHERE UMT.USER_MST_PK=UAL.USER_MST_FK  ";
            strSQL = strSQL + " AND UAL.CONFIG_MST_FK=CMT.CONFIG_MST_PK";

            if (Modules > 0)
            {
                strSQL = strSQL + " and ual.config_mst_fk = " + Modules;
            }

            if (UserID > 0)
            {
                strSQL = strSQL + " and ual.user_mst_fk = " + UserID;
            }

            if (!string.IsNullOrEmpty(FromDate) & !string.IsNullOrEmpty(ToDate))
            {
                string frmDt = System.String.Format("{0:dd-MMM-yyyy}", Convert.ToDateTime(FromDate));
                string toDt = System.String.Format("{0:dd-MMM-yyyy}", Convert.ToDateTime(FromDate));
                strSQL = strSQL + " and activity_date  between '" + frmDt.ToUpper() + "' and to_date('" + toDt.ToUpper() + "','dd-mon-yyyy')+1";
            }
            else if (!string.IsNullOrEmpty(FromDate) & string.IsNullOrEmpty(ToDate))
            {
                string frmDt = System.String.Format("{0:dd-MMM-yyyy}", Convert.ToDateTime(FromDate));
                strSQL = strSQL + " and ual.activity_date >= '" + frmDt.ToUpper() + "'";
                //added >
            }
            else if (string.IsNullOrEmpty(FromDate) & !string.IsNullOrEmpty(ToDate))
            {
                string toDt = System.String.Format("{0:dd-MMM-yyyy}", Convert.ToDateTime(FromDate));
                strSQL = strSQL + " and ual.activity_date <= to_date('" + toDt.ToUpper() + "','dd-mon-yyyy')+1";
                //added <
            }

            if (!string.IsNullOrEmpty(Activity))
            {
                strSQL = strSQL + " and activity_type='" + Activity + "'";
            }

            strSQL = strSQL + " order by DateandTime desc) Q) where SLNO Between '" + start + "' and '" + last + "' ";
           





            try
            {
                DataSet Ds = objWF.GetDataSet(strSQL);
                return JsonConvert.SerializeObject(Ds, Formatting.Indented);
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

		public DataSet FetchLocation()
		{
			string strSQL = null;
			strSQL = "select location_mst_pk, location_name from location_mst_tbl";

			WorkFlow objWF = new WorkFlow();

			try {
				return objWF.GetDataSet(strSQL);
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}

		}

		#endregion
	}

}

