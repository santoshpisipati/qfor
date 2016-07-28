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
using System.Data;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class clsCommission : CommonFeatures
    {
        #region "GetMainBookingData"

        /// <summary>
        /// Gets the sales report.
        /// </summary>
        /// <param name="reportType">Type of the report.</param>
        /// <param name="fromDate">From date.</param>
        /// <param name="toDate">To date.</param>
        /// <param name="Status">The status.</param>
        /// <returns></returns>
        public DataSet GetSalesReport(string reportType, string fromDate, string toDate, Int32 Status = 1)
        {
            WorkFlow objWF = new WorkFlow();
            string SQL = null;
            string Biz = null;
            string strSQL = null;
            string stStatus = null;
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            string strCondition = " ";
            string strStatus = " ";
            try
            {
                if (Status == 0)
                {
                    strStatus = " AND A.ACTIVE_FLAG=0";
                }
                else if (Status == 1)
                {
                    strStatus = " AND A.ACTIVE_FLAG=1";
                }

                if (reportType == "1" | reportType == "2")
                {
                    strCondition = " WHERE A.BUSINESS_TYPE IN (1,3)";
                }
                else if (reportType == "3" | reportType == "4")
                {
                    strCondition = " WHERE A.BUSINESS_TYPE IN (2,3)";
                }
                else if (reportType == "5" | reportType == "6")
                {
                    strCondition = " WHERE A.BUSINESS_TYPE IN (1,2,3)";
                }
                //If strCondition = " WHERE A.BUSINESS_TYPE = 1" Then
                //    Biz = "AIR"
                //ElseIf strCondition = " WHERE A.BUSINESS_TYPE = 2" Then
                //    Biz = "SEA"
                //ElseIf strCondition = " WHERE A.BUSINESS_TYPE = 3" Then
                //    Biz = "BOTH"
                //End If
                strCondition = strCondition + strStatus;
                if (!((fromDate == null | string.IsNullOrEmpty(fromDate)) & (toDate == null | string.IsNullOrEmpty(toDate))))
                {
                    strCondition = strCondition + " AND TO_DATE(A.CREATED_DT, DATEFORMAT) >= TO_DATE('" + fromDate + "',DATEFORMAT) AND TO_DATE(A.CREATED_DT, DATEFORMAT) <= TO_DATE('" + toDate + "',DATEFORMAT) ";
                }
                else if (((fromDate != null) | !string.IsNullOrEmpty(fromDate)) & ((toDate == null) | string.IsNullOrEmpty(toDate)))
                {
                    strCondition = strCondition + " AND A.CREATED_DT >= TO_DATE('" + fromDate + "',dateformat) ";
                }
                else if (((toDate != null) | !string.IsNullOrEmpty(toDate)) & ((fromDate == null) | string.IsNullOrEmpty(fromDate)))
                {
                    strCondition = strCondition + " AND A.CREATED_DT >= TO_DATE('" + toDate + "',dateformat) ";
                }
                if (reportType == "1" | reportType == "3" | reportType == "5")
                {
                    strCondition = strCondition + " ORDER BY A.IMP_COMM_PER DESC ";
                }
                else if (reportType == "2" | reportType == "4" | reportType == "6")
                {
                    strCondition = strCondition + " ORDER BY A.EXP_COMM_PER DESC";
                }
                SQL = "select A.AGENT_ID  AGENT_ID      A.AGENT_NAME AGENT_NAME      TO_CHAR(A.CREATED_DT, 'MONTH YYYY') CREATED_DATE      nvl(A.EXP_COMM_PER,0) EXPORT_COMMISSION      nvl(A.IMP_COMM_PER,0) IMPORT_COMMISSION      A.BUSINESS_TYPE BUSINESS_TYPE      DECODE(A.BUSINESS_TYPE,1,'AIR',2,'SEA','BOTH') BIZTYPE, " + "     TO_CHAR(A.CREATED_DT, 'MONTH YYYY') MONTHYEAR      CASE WHEN ACTIVE_FLAG=0 THEN 'INACTIVE' ELSE 'ACTIVE' END ACTIVE_FLAG  from agent_mst_tbl A";
                //"     '" & Biz & "' BIZTYPE & vbNewLine & _

                return objWF.GetDataSet(SQL + strCondition);
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

        /// <summary>
        /// Gets the sales report new.
        /// </summary>
        /// <param name="reportType">Type of the report.</param>
        /// <param name="fromDate">From date.</param>
        /// <param name="toDate">To date.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="Status">The status.</param>
        /// <returns></returns>
        public DataSet GetSalesReportNew(string reportType, string fromDate, string toDate, Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 Status = 1)
        {
            WorkFlow objWF = new WorkFlow();
            string SQL = null;
            string Biz = null;
            string strSQL = null;
            string stStatus = null;
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            string strCondition = " ";
            string strStatus = " ";
            try
            {
                if (Status == 0)
                {
                    strStatus = " AND A.ACTIVE_FLAG=0";
                }
                else if (Status == 1)
                {
                    strStatus = " AND A.ACTIVE_FLAG=1";
                }
                if (reportType == "1" | reportType == "2")
                {
                    strCondition = " WHERE A.BUSINESS_TYPE IN (1,3)";
                    Biz = "AIR";
                }
                else if (reportType == "3" | reportType == "4")
                {
                    strCondition = " WHERE A.BUSINESS_TYPE IN (2,3)";
                    Biz = "SEA";
                }
                else if (reportType == "5" | reportType == "6")
                {
                    strCondition = " WHERE A.BUSINESS_TYPE IN (1,2,3)";
                    Biz = "BOTH";
                }
                //If strCondition = " WHERE A.BUSINESS_TYPE = 1" Then
                //    Biz = "AIR"
                //ElseIf strCondition = " WHERE A.BUSINESS_TYPE = 2" Then
                //    Biz = "SEA"
                //ElseIf strCondition = " WHERE A.BUSINESS_TYPE IN (1,2,3)" Then
                //    Biz = "BOTH"
                //End If
                strCondition = strCondition + strStatus;
                if (!((fromDate == null | string.IsNullOrEmpty(fromDate)) & (toDate == null | string.IsNullOrEmpty(toDate))))
                {
                    strCondition = strCondition + " AND TO_DATE(A.CREATED_DT, DATEFORMAT) >= TO_DATE('" + fromDate + "',DATEFORMAT) AND TO_DATE(A.CREATED_DT, DATEFORMAT) <= TO_DATE('" + toDate + "',DATEFORMAT) ";
                }
                else if (((fromDate != null) | !string.IsNullOrEmpty(fromDate)) & ((toDate == null) | string.IsNullOrEmpty(toDate)))
                {
                    strCondition = strCondition + " AND A.CREATED_DT >= TO_DATE('" + fromDate + "',dateformat) ";
                }
                else if (((toDate != null) | !string.IsNullOrEmpty(toDate)) & ((fromDate == null) | string.IsNullOrEmpty(fromDate)))
                {
                    strCondition = strCondition + " AND A.CREATED_DT >= TO_DATE('" + toDate + "',dateformat) ";
                }
                if (reportType == "1" | reportType == "3" | reportType == "5")
                {
                    strCondition = strCondition + " ORDER BY A.CREATED_DT DESC ";
                }
                else if (reportType == "2" | reportType == "4" | reportType == "6")
                {
                    strCondition = strCondition + " ORDER BY A.CREATED_DT DESC";
                }
                if (reportType == "1" | reportType == "3" | reportType == "5")
                {
                    strSQL = "SELECT Count(*) from(select rownum SLNO,q.* from(select A.AGENT_ID  AGENTID      A.AGENT_NAME AGENTNAME      TO_CHAR(A.CREATED_DT, 'MONTH YYYY') MONTHYEAR      '" + stStatus + "' ACTIVE_FLAG      '" + Biz + "' BIZTYPE      NULL EXPORT_COMMISSION      NULL IMPORT_COMMISSION  from agent_mst_tbl A";
                    strSQL = (strSQL + strCondition);
                    strSQL = strSQL + ")q)  ";
                }
                else
                {
                    strSQL = "SELECT Count(*) from(select rownum SLNO,q.* from(select A.AGENT_ID  AGENTID      A.AGENT_NAME AGENTNAME      TO_CHAR(A.CREATED_DT, 'MONTH YYYY') MONTHYEAR      '" + stStatus + "' ACTIVE_FLAG      '" + Biz + "' BIZTYPE      NULL EXPORT_COMMISSION      NULL IMPORT_COMMISSION  from agent_mst_tbl A";
                    strSQL = (strSQL + strCondition);
                    strSQL = strSQL + ")q)  ";
                }

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

                if (reportType == "1" | reportType == "3" | reportType == "5")
                {
                    SQL = "SELECT * FROM(select rownum SLNO,q.* from(select A.AGENT_MST_PK, A.AGENT_ID  AGENTID      A.AGENT_NAME AGENTNAME      TO_CHAR(A.CREATED_DT, 'MONTH YYYY') MONTHYEAR      CASE WHEN ACTIVE_FLAG=0 THEN 'INACTIVE' ELSE 'ACTIVE' END ACTIVE_FLAG      '" + Biz + "' BIZTYPE      NULL EXPORT_COMMISSION      NULL IMPORT_COMMISSION  from agent_mst_tbl A";
                    SQL = (SQL + strCondition);
                    SQL = SQL + ")q) where SLNO Between '" + start + "' and '" + last + "' ";
                }
                else
                {
                    SQL = "SELECT * FROM(select rownum SLNO,q.* from(select A.AGENT_MST_PK, A.AGENT_ID  AGENTID      A.AGENT_NAME AGENTNAME      TO_CHAR(A.CREATED_DT, 'MONTH YYYY') MONTHYEAR      CASE WHEN ACTIVE_FLAG=0 THEN 'INACTIVE' ELSE 'ACTIVE' END ACTIVE_FLAG      '" + Biz + "' BIZTYPE      NULL EXPORT_COMMISSION      NULL IMPORT_COMMISSION  from agent_mst_tbl A";
                    SQL = (SQL + strCondition);
                    SQL = SQL + ")q) where SLNO Between '" + start + "' and '" + last + "'";
                }

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

        #endregion "GetMainBookingData"
    }
}