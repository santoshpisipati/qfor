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
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class clsPayableList : CommonFeatures
    {
        #region "Parent Table"

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="REFNo">The reference no.</param>
        /// <param name="InvNo">The inv no.</param>
        /// <param name="VENDOR">The vendor.</param>
        /// <param name="RefDate">The reference date.</param>
        /// <param name="FromDate">From date.</param>
        /// <param name="toDate">To date.</param>
        /// <param name="Active">The active.</param>
        /// <param name="SortColumn">The sort column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="SortType">Type of the sort.</param>
        /// <param name="lngUsrLocFk">The LNG usr loc fk.</param>
        /// <param name="bizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <param name="JobType">Type of the job.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="DueFromDate">The due from date.</param>
        /// <param name="DuetoDate">The dueto date.</param>
        /// <param name="IsGrand">The is grand.</param>
        /// <returns></returns>
        public object FetchAll(string REFNo = "", string InvNo = "", string VENDOR = "", string RefDate = "", string FromDate = "", string toDate = "", Int16 Active = 0, string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0,
        string SortType = " DESC ", long lngUsrLocFk = 0, int bizType = 3, int ProcessType = 1, int JobType = 0, Int32 flag = 0, string DueFromDate = "", string DuetoDate = "", int IsGrand = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            StringBuilder strSQLBuilder = new StringBuilder();
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            StringBuilder strCount = new StringBuilder();
            strSQLBuilder.Append(" SELECT Q.* FROM(");
            strSQLBuilder.Append(" SELECT ");
            strSQLBuilder.Append(" P.PAYMENT_TBL_PK PK,");
            strSQLBuilder.Append(" V.VENDOR_ID,");
            strSQLBuilder.Append(" DECODE(P.BUSINESS_TYPE, 1, 'Air', 2, 'Sea') BIZTYPE,");
            strSQLBuilder.Append(" DECODE(P.PROCESS_TYPE, 1, 'Export', 2, 'Import') PROCESSTYPE,");
            strSQLBuilder.Append(" P.PAYMENT_REF_NO,");
            strSQLBuilder.Append(" DECODE(P.JOB_TYPE, 1, 'Job Card', 2, 'Customs Brokerage', 3, 'Transport Note') JOB_TYPE,");
            strSQLBuilder.Append(" P.PAYMENT_DATE,");
            strSQLBuilder.Append(" CUR.CURRENCY_ID,");
            strSQLBuilder.Append(" sum(PTRN.PAID_AMOUNT_HDR_CURR) PAYAMT,");
            strSQLBuilder.Append(" Decode(P.APPROVED,1,'Approved',2,'Rejected',0,'Pending',3,'Cancelled') STATUS,");
            strSQLBuilder.Append(" NVL(GET_EX_RATE(P.CURRENCY_MST_FK," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ", P.PAYMENT_DATE),0) ROE,'' SEL ");
            strSQLBuilder.Append(" FROM PAYMENTS_TBL     P,");
            strSQLBuilder.Append(" PAYMENT_TRN_TBL       PTRN,");
            strSQLBuilder.Append(" CURRENCY_TYPE_MST_TBL CUR,");
            strSQLBuilder.Append(" USER_MST_TBL          UMT,");
            strSQLBuilder.Append(" INV_SUPPLIER_TBL      I,");
            strSQLBuilder.Append(" VENDOR_MST_TBL V");
            strSQLBuilder.Append(" WHERE ");
            strSQLBuilder.Append(" P.PAYMENT_TBL_PK = PTRN.PAYMENTS_TBL_FK ");
            strSQLBuilder.Append(" AND CUR.CURRENCY_MST_PK = P.CURRENCY_MST_FK");
            strSQLBuilder.Append(" AND P.CREATED_BY_FK = UMT.USER_MST_PK");
            strSQLBuilder.Append(" AND PTRN.INV_SUPPLIER_TBL_FK = I.INV_SUPPLIER_PK");
            strSQLBuilder.Append(" AND V.VENDOR_MST_PK = P.VENDOR_MST_FK");
            if (ProcessType != 0)
            {
                strSQLBuilder.Append(" AND P.PROCESS_TYPE ='" + ProcessType + "'");
            }
            if (bizType != 3)
            {
                strSQLBuilder.Append(" AND P.BUSINESS_TYPE ='" + bizType + "'");
            }
            if (JobType != 0)
            {
                strSQLBuilder.Append(" AND P.JOB_TYPE ='" + JobType + "'");
            }
            if (Active == 1)
            {
                Active = -1;
            }
            else if (Active == 2)
            {
                Active = 0;
            }
            else if (Active == 3)
            {
                Active = 1;
            }
            else if (Active == 4)
            {
                Active = 2;
            }
            else if (Active == 5)
            {
                Active = 3;
            }
            if (Active >= 0)
            {
                strSQLBuilder.Append(" AND P.APPROVED = " + Active);
            }
            if (flag == 0)
            {
                strSQLBuilder.Append(" AND 1=2 ");
            }
            if (!string.IsNullOrEmpty(VENDOR))
            {
                strSQLBuilder.Append(" AND P.VENDOR_MST_FK = '" + VENDOR + "' ");
            }

            if (!string.IsNullOrEmpty(getDefault(FromDate, "").ToString()) & !string.IsNullOrEmpty(getDefault(toDate, "").ToString()))
            {
                strSQLBuilder.Append(" AND I.SUPPLIER_INV_DT >=TO_DATE('" + FromDate + "' ,'" + dateFormat + "')");
                strSQLBuilder.Append(" AND I.SUPPLIER_INV_DT <=TO_DATE('" + toDate + "' ,'" + dateFormat + "')");
            }
            else if (!string.IsNullOrEmpty(getDefault(FromDate, "").ToString()))
            {
                strSQLBuilder.Append(" AND I.SUPPLIER_INV_DT >=TO_DATE('" + FromDate + "' ,'" + dateFormat + "')");
            }
            else if (!string.IsNullOrEmpty(getDefault(toDate, "").ToString()))
            {
                strSQLBuilder.Append(" AND I.SUPPLIER_INV_DT <=TO_DATE('" + toDate + "' ,'" + dateFormat + "')");
            }

            if (!string.IsNullOrEmpty(getDefault(DueFromDate, "").ToString()) & !string.IsNullOrEmpty(getDefault(DuetoDate, "").ToString()))
            {
                strSQLBuilder.Append(" AND I.SUPPLIER_DUE_DT >=TO_DATE('" + DueFromDate + "' ,'" + dateFormat + "')");
                strSQLBuilder.Append(" AND I.SUPPLIER_DUE_DT <=TO_DATE('" + DuetoDate + "' ,'" + dateFormat + "')");
            }
            else if (!string.IsNullOrEmpty(getDefault(DueFromDate, "").ToString()))
            {
                strSQLBuilder.Append(" AND I.SUPPLIER_DUE_DT >=TO_DATE('" + DueFromDate + "' ,'" + dateFormat + "')");
            }
            else if (!string.IsNullOrEmpty(getDefault(DuetoDate, "").ToString()))
            {
                strSQLBuilder.Append(" AND I.SUPPLIER_DUE_DT <=TO_DATE('" + DuetoDate + "' ,'" + dateFormat + "')");
            }

            //
            if (!string.IsNullOrEmpty(getDefault(RefDate, "").ToString()))
            {
                strSQLBuilder.Append(" AND TO_DATE(P.PAYMENT_DATE,'DD/MM/YYYY') =TO_DATE('" + RefDate + "' ,'" + dateFormat + "')");
            }

            strSQLBuilder.Append(" AND UMT.DEFAULT_LOCATION_FK ='" + lngUsrLocFk + "'");

            if (!string.IsNullOrEmpty(REFNo))
            {
                REFNo = REFNo.ToUpper();
                strSQLBuilder.Append(" AND UPPER(P.PAYMENT_REF_NO) LIKE '%" + REFNo.ToUpper().Replace("'", "''") + "%'");
                // strCondition = strCondition & " AND UPPER(Freight_Element_ID) LIKE '" & Freight_ElementID.ToUpper.Replace("'", "''") & "%'" & vbCrLf
            }
            if (!string.IsNullOrEmpty(InvNo))
            {
                //strSQLBuilder.Append(" AND UPPER(I.SUPPLIER_INV_NO) LIKE '%" & InvNo.Trim.ToUpper.Replace("'", "''") & "%' ")
                strSQLBuilder.Append(" AND UPPER(I.INVOICE_REF_NO) LIKE '%" + InvNo.Trim().ToUpper().Replace("'", "''") + "%' ");
            }
            strSQLBuilder.Append(" GROUP BY ");
            strSQLBuilder.Append(" P.PAYMENT_TBL_PK,V.VENDOR_ID,P.PAYMENT_REF_NO,P.PAYMENT_DATE,CUR.CURRENCY_ID,P.APPROVED,P.BUSINESS_TYPE,P.PROCESS_TYPE,P.JOB_TYPE,P.CURRENCY_MST_FK ORDER BY " + SortColumn + "  " + SortType + ")Q  ");
            strCount.Append(" SELECT COUNT(*)");
            strCount.Append(" FROM ");
            strCount.Append("(" + strSQLBuilder.ToString() + ")");

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

            strSQL.Append("SELECT QRY.* FROM ");
            strSQL.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            strSQL.Append("  (" + strSQLBuilder.ToString() + " ");
            strSQL.Append("  ) T) QRY ");

            if (IsGrand != 1)
            {
                strSQL.Append(" WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            }

            string sql = null;
            sql = strSQL.ToString();

            DataSet DS = null;
            try
            {
                DS = objWF.GetDataSet(sql);
                DataRelation CONTRel = null;
                DS.Tables.Add(Fetchchildlist(AllMasterPKs(DS), REFNo, InvNo, VENDOR, RefDate, FromDate, toDate, Active, lngUsrLocFk, bizType,
                ProcessType, JobType, DueFromDate, DuetoDate));
                CONTRel = new DataRelation("CONTRelation", DS.Tables[0].Columns["PK"], DS.Tables[1].Columns["PK"], true);
                CONTRel.Nested = true;
                DS.Relations.Add(CONTRel);
                return DS;
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

        #endregion "Parent Table"

        #region "PK Value"

        /// <summary>
        /// Alls the master p ks.
        /// </summary>
        /// <param name="ds">The ds.</param>
        /// <returns></returns>
        private string AllMasterPKs(DataSet ds)
        {
            try
            {
                Int16 RowCnt = default(Int16);
                Int16 ln = default(Int16);
                System.Text.StringBuilder strBuild = new System.Text.StringBuilder();
                strBuild.Append("-1,");
                for (RowCnt = 0; RowCnt <= ds.Tables[0].Rows.Count - 1; RowCnt++)
                {
                    strBuild.Append(Convert.ToString(ds.Tables[0].Rows[RowCnt]["PK"]).Trim() + ",");
                }
                strBuild.Remove(strBuild.Length - 1, 1);
                return strBuild.ToString();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "PK Value"

        #region "ChildTable"

        /// <summary>
        /// Fetchchildlists the specified cont spot p ks.
        /// </summary>
        /// <param name="CONTSpotPKs">The cont spot p ks.</param>
        /// <param name="REFNo">The reference no.</param>
        /// <param name="InvNo">The inv no.</param>
        /// <param name="VENDOR">The vendor.</param>
        /// <param name="RefDate">The reference date.</param>
        /// <param name="FromDate">From date.</param>
        /// <param name="toDate">To date.</param>
        /// <param name="Active">The active.</param>
        /// <param name="lngUsrLocFk">The LNG usr loc fk.</param>
        /// <param name="bizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <param name="JobType">Type of the job.</param>
        /// <param name="DueFromDate">The due from date.</param>
        /// <param name="DuetoDate">The dueto date.</param>
        /// <returns></returns>
        public DataTable Fetchchildlist(string CONTSpotPKs = "", string REFNo = "", string InvNo = "", string VENDOR = "", string RefDate = "", string FromDate = "", string toDate = "", Int16 Active = 0, long lngUsrLocFk = 0, int bizType = 3,
        int ProcessType = 1, int JobType = 0, string DueFromDate = "", string DuetoDate = "")
        {
            System.Text.StringBuilder buildQuery = new System.Text.StringBuilder();
            string strsql = null;
            WorkFlow objWF = new WorkFlow();
            DataTable dt = null;
            int RowCnt = 0;
            int Rno = 0;
            int pk = 0;

            buildQuery.Append(" SELECT ROWNUM \"SL.NR\", T.* ");
            buildQuery.Append(" FROM ");
            buildQuery.Append(" (SELECT P.PAYMENT_TBL_PK PK,");
            buildQuery.Append(" I.INVOICE_REF_NO,");
            buildQuery.Append(" I.INVOICE_DATE,");
            buildQuery.Append(" I.SUPPLIER_INV_NO,");
            buildQuery.Append(" I.SUPPLIER_INV_DT,");
            buildQuery.Append(" CUR.CURRENCY_ID,");
            buildQuery.Append(" PTRN.PAID_AMOUNT_HDR_CURR");
            buildQuery.Append(" , I.SUPPLIER_DUE_DT");
            buildQuery.Append(" FROM PAYMENTS_TBL P,");
            buildQuery.Append(" PAYMENT_TRN_TBL       PTRN,");
            buildQuery.Append(" CURRENCY_TYPE_MST_TBL CUR,");
            buildQuery.Append(" USER_MST_TBL          UMT,");
            buildQuery.Append(" INV_SUPPLIER_TBL      I,");
            buildQuery.Append(" VENDOR_MST_TBL V ");
            buildQuery.Append(" WHERE");
            buildQuery.Append(" P.PAYMENT_TBL_PK = PTRN.PAYMENTS_TBL_FK");
            buildQuery.Append(" AND CUR.CURRENCY_MST_PK = P.CURRENCY_MST_FK");
            buildQuery.Append(" AND V.VENDOR_MST_PK=P.VENDOR_MST_FK");
            buildQuery.Append(" AND P.CREATED_BY_FK = UMT.USER_MST_PK");
            buildQuery.Append(" AND PTRN.INV_SUPPLIER_TBL_FK = I.INV_SUPPLIER_PK");
            if (ProcessType != 0)
            {
                buildQuery.Append(" AND P.PROCESS_TYPE ='" + ProcessType + "'");
            }
            if (bizType != 3)
            {
                buildQuery.Append(" AND P.BUSINESS_TYPE ='" + bizType + "'");
            }
            if (JobType != 0)
            {
                buildQuery.Append(" AND P.JOB_TYPE ='" + JobType + "'");
            }
            if (Active > 0)
            {
                buildQuery.Append(" AND P.APPROVED = " + Active);
            }
            if (!string.IsNullOrEmpty(VENDOR))
            {
                buildQuery.Append(" AND P.VENDOR_MST_FK = '" + VENDOR + "' ");
            }
            buildQuery.Append(" AND UMT.DEFAULT_LOCATION_FK ='" + lngUsrLocFk + "'");

            if (!string.IsNullOrEmpty(REFNo))
            {
                REFNo = REFNo.ToUpper();
                buildQuery.Append(" AND UPPER(P.PAYMENT_REF_NO) LIKE '%" + REFNo.ToUpper().Replace("'", "''") + "%'");
                // strCondition = strCondition & " AND UPPER(Freight_Element_ID) LIKE '" & Freight_ElementID.ToUpper.Replace("'", "''") & "%'" & vbCrLf
            }

            if (!string.IsNullOrEmpty(getDefault(FromDate, "").ToString()) & !string.IsNullOrEmpty(getDefault(toDate, "").ToString()))
            {
                buildQuery.Append(" AND I.SUPPLIER_INV_DT >=TO_DATE('" + FromDate + "' ,'" + dateFormat + "')");
                buildQuery.Append(" AND I.SUPPLIER_INV_DT <=TO_DATE('" + toDate + "' ,'" + dateFormat + "')");
            }
            else if (!string.IsNullOrEmpty(getDefault(FromDate, "").ToString()))
            {
                buildQuery.Append(" AND I.SUPPLIER_INV_DT >=TO_DATE('" + FromDate + "' ,'" + dateFormat + "')");
            }
            else if (!string.IsNullOrEmpty(getDefault(toDate, "").ToString()))
            {
                buildQuery.Append(" AND I.SUPPLIER_INV_DT <=TO_DATE('" + toDate + "' ,'" + dateFormat + "')");
            }

            //
            if (!string.IsNullOrEmpty(getDefault(RefDate, "").ToString()))
            {
                buildQuery.Append(" AND P.PAYMENT_DATE = TO_DATE('" + RefDate + "' ,'" + dateFormat + "')");
            }

            if (!string.IsNullOrEmpty(getDefault(DueFromDate, "").ToString()) & !string.IsNullOrEmpty(getDefault(DuetoDate, "").ToString()))
            {
                buildQuery.Append(" AND I.SUPPLIER_DUE_DT >=TO_DATE('" + DueFromDate + "' ,'" + dateFormat + "')");
                buildQuery.Append(" AND I.SUPPLIER_DUE_DT <=TO_DATE('" + DuetoDate + "' ,'" + dateFormat + "')");
            }
            else if (!string.IsNullOrEmpty(getDefault(DueFromDate, "").ToString()))
            {
                buildQuery.Append(" AND I.SUPPLIER_DUE_DT >=TO_DATE('" + DueFromDate + "' ,'" + dateFormat + "')");
            }
            else if (!string.IsNullOrEmpty(getDefault(DuetoDate, "").ToString()))
            {
                buildQuery.Append(" AND I.SUPPLIER_DUE_DT <=TO_DATE('" + DuetoDate + "' ,'" + dateFormat + "')");
            }
            //end

            if (!string.IsNullOrEmpty(InvNo))
            {
                buildQuery.Append(" AND UPPER(I.SUPPLIER_INV_NO) LIKE '%" + InvNo.Trim().ToUpper().Replace("'", "''") + "%' ");
            }

            if (CONTSpotPKs.Trim().Length > 0)
            {
                buildQuery.Append(" AND P.PAYMENT_TBL_PK  IN (" + CONTSpotPKs + ") ");
            }
            buildQuery.Append(" GROUP BY P.PAYMENT_TBL_PK ,I.INVOICE_REF_NO,I.INVOICE_DATE,I.SUPPLIER_INV_NO,I.SUPPLIER_INV_DT,CUR.CURRENCY_ID,PTRN.PAID_AMOUNT_HDR_CURR, I.SUPPLIER_DUE_DT");
            buildQuery.Append(" ) T ");

            strsql = buildQuery.ToString();
            try
            {
                pk = -1;
                dt = objWF.GetDataTable(strsql);
                for (RowCnt = 0; RowCnt <= dt.Rows.Count - 1; RowCnt++)
                {
                    if (Convert.ToInt32(dt.Rows[RowCnt]["PK"]) != pk)
                    {
                        pk = Convert.ToInt32(dt.Rows[RowCnt]["PK"]);
                        Rno = 0;
                    }
                    Rno += 1;
                    dt.Rows[RowCnt]["SL.NR"] = Rno;
                }
                return dt;
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

        #endregion "ChildTable"

        #region "Fetching Currency"

        /// <summary>
        /// Fetches the payment current.
        /// </summary>
        /// <param name="PayRefPKs">The pay reference p ks.</param>
        /// <returns></returns>
        public DataSet FetchPaymentCur(string PayRefPKs)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            int CurrFK = 0;
            try
            {
                if (CurrFK == 0)
                {
                    CurrFK = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
                }
                if ((PayRefPKs != null))
                {
                    sb.Append(" SELECT DISTINCT CMT.CURRENCY_ID,(GET_EX_RATE(P.CURRENCY_MST_FK," + CurrFK + ",P.PAYMENT_DATE))ROE");
                    sb.Append("  FROM PAYMENTS_TBL P,CURRENCY_TYPE_MST_TBL CMT");
                    sb.Append(" WHERE P.CURRENCY_MST_FK = CMT.CURRENCY_MST_PK");
                    sb.Append(" AND P.PAYMENT_TBL_PK IN (" + PayRefPKs + ")");
                }
                return (objWF.GetDataSet(sb.ToString()));
            }
            catch (OracleException sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw;
            }
        }

        #endregion "Fetching Currency"

        #region "Fetch Status"

        /// <summary>
        /// Fetches the drop down values.
        /// </summary>
        /// <param name="Flag">The flag.</param>
        /// <param name="ConfigID">The configuration identifier.</param>
        /// <returns></returns>
        public DataSet FetchDropDownValues(string Flag, string ConfigID)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT T.DD_VALUE, T.DD_ID");
            sb.Append("  FROM QFOR_DROP_DOWN_TBL T");
            sb.Append(" WHERE T.DD_FLAG = '" + Flag + "'");
            sb.Append(" AND T.CONFIG_ID  = '" + ConfigID + "'");
            sb.Append(" ORDER BY T.DROPDOWN_PK ");
            try
            {
                return objWF.GetDataSet(sb.ToString());
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

        #endregion "Fetch Status"

        #region "UpdatePayStatus"

        /// <summary>
        /// Updates the payment status.
        /// </summary>
        /// <param name="PaymentPk">The payment pk.</param>
        /// <param name="remarks">The remarks.</param>
        /// <returns></returns>
        public string UpdatePaymentStatus(string PaymentPk, string remarks)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand updCmd = new OracleCommand();
            Int16 intIns = default(Int16);
            try
            {
                objWF.OpenConnection();
                var _with1 = updCmd;
                _with1.Connection = objWF.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWF.MyUserName + ".PAYMENT_CRN_CANCELLATION_PKG.CANCEL_PAYMENT_CRN";
                var _with2 = _with1.Parameters;
                updCmd.Parameters.Add("PK_IN", PaymentPk).Direction = ParameterDirection.Input;
                updCmd.Parameters.Add("REMARKS_IN", remarks).Direction = ParameterDirection.Input;
                updCmd.Parameters.Add("LAST_MODIFIED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                updCmd.Parameters.Add("TYPE_FLAG_IN", "PAYMENT").Direction = ParameterDirection.Input;
                updCmd.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 5000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                intIns = Convert.ToInt16(_with1.ExecuteNonQuery());
                return Convert.ToString(updCmd.Parameters["RETURN_VALUE"].Value);
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        #endregion "UpdatePayStatus"
    }
}