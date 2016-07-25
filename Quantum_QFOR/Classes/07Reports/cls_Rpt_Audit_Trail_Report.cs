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
//'*  Modified DateTime(DD-MON-YYYY)              Modified By                             Remarks (Bugs Related)
//'*
//'*
//'***************************************************************************************************************

#endregion "Comments"

using Oracle.DataAccess.Client;
using System;
using System.Data;

namespace Quantum_QFOR
{
    internal class cls_Rpt_Audit_Trail_Report : CommonFeatures
    {
        #region "Fetch Function"

        public DataSet FetchAll(Int64 UserID = 0, Int64 Modules = 0, string Activity = "", string SortBy = "", string FromDate = "", string ToDate = "", string location = "")
        {
            string strSQL = null;
            strSQL = "SELECT USER_NAME, CMT.CONFIG_NAME,";
            strSQL = strSQL + "ACTIVITY_DATE, ACTIVITY_TYPE, PARAMETERS,REMARKS  FROM USER_ACTIVITY_LOG UAL, USER_MST_TBL UMT,CONFIG_MST_TBL CMT ";
            strSQL = strSQL + " WHERE UMT.USER_MST_PK=UAL.USER_MST_FK  ";
            strSQL = strSQL + " AND UAL.CONFIG_MST_FK=CMT.CONFIG_MST_PK ";
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
                string frmDt = string.Format("{0:dd-MMM-yyyy}", Convert.ToDateTime(FromDate));
                string toDt = string.Format("{0:dd-MMM-yyyy}", Convert.ToDateTime(ToDate));
                strSQL = strSQL + " and activity_date  between '" + frmDt.ToUpper() + "' and to_date('" + toDt.ToUpper() + "','dd-mon-yyyy')+1";
            }
            else if (!string.IsNullOrEmpty(FromDate) & string.IsNullOrEmpty(ToDate))
            {
                string frmDt = System.String.Format("{0:dd-MMM-yyyy}", Convert.ToDateTime(FromDate));
                strSQL = strSQL + " and ual.activity_date >= '" + frmDt.ToUpper() + "'";
            }
            else if (string.IsNullOrEmpty(FromDate) & !string.IsNullOrEmpty(ToDate))
            {
                string toDt = System.String.Format("{0:dd-MMM-yyyy}", Convert.ToDateTime(ToDate));
                strSQL = strSQL + " and ual.activity_date <= to_date('" + toDt.ToUpper() + "','dd-mon-yyyy')+1";
            }

            if (!string.IsNullOrEmpty(Activity))
            {
                strSQL = strSQL + " and activity_type='" + Activity + "'";
            }

            strSQL = strSQL + " order by ACTIVITY_DATE DESC ";
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

        public DataSet FetchAllNew(Int64 UserID = 0, Int64 Modules = 0, string Activity = "", string SortBy = "", string FromDate = "", string ToDate = "", string location = "", Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            Int32 TotalRecords = default(Int32);
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
                string toDt = System.String.Format("{0:dd-MMM-yyyy}", Convert.ToDateTime(ToDate));
                strSQL = strSQL + " and activity_date  between '" + frmDt.ToUpper() + "' and to_date('" + toDt.ToUpper() + "','dd-mon-yyyy')+1";
            }
            else if (!string.IsNullOrEmpty(FromDate) & string.IsNullOrEmpty(ToDate))
            {
                string frmDt = System.String.Format("{0:dd-MMM-yyyy}", Convert.ToDateTime(FromDate));
                strSQL = strSQL + " and ual.activity_date >= '" + frmDt.ToUpper() + "'";
            }
            else if (string.IsNullOrEmpty(FromDate) & !string.IsNullOrEmpty(ToDate))
            {
                string toDt = System.String.Format("{0:dd-MMM-yyyy}", Convert.ToDateTime(ToDate));
                strSQL = strSQL + " and ual.activity_date <= to_date('" + toDt.ToUpper() + "','dd-mon-yyyy')+1";
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
                string toDt = System.String.Format("{0:dd-MMM-yyyy}", Convert.ToDateTime(ToDate));
                strSQL = strSQL + " and activity_date  between '" + frmDt.ToUpper() + "' and to_date('" + toDt.ToUpper() + "','dd-mon-yyyy')+1";
            }
            else if (!string.IsNullOrEmpty(FromDate) & string.IsNullOrEmpty(ToDate))
            {
                string frmDt = System.String.Format("{0:dd-MMM-yyyy}", Convert.ToDateTime(FromDate));
                strSQL = strSQL + " and ual.activity_date >= '" + frmDt.ToUpper() + "'";
            }
            else if (string.IsNullOrEmpty(FromDate) & !string.IsNullOrEmpty(ToDate))
            {
                string toDt = System.String.Format("{0:dd-MMM-yyyy}", Convert.ToDateTime(ToDate));
                strSQL = strSQL + " and ual.activity_date <= to_date('" + toDt.ToUpper() + "','dd-mon-yyyy')+1";
            }

            if (!string.IsNullOrEmpty(Activity))
            {
                strSQL = strSQL + " and activity_type='" + Activity + "'";
            }

            strSQL = strSQL + " order by DateandTime desc) Q) where SLNO Between '" + start + "' and '" + last + "' ";

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

        public DataSet FetchLocation()
        {
            string strSQL = null;
            strSQL = "select location_mst_pk, location_name from location_mst_tbl";

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

        #endregion "Fetch Function"
    }
}