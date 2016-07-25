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

using Oracle.DataAccess.Client;
using System;
using System.Data;
using System.Text;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class clsAgentCollectionlist : CommonFeatures
    {
        #region "Fetch Function"

        /// <summary>
        /// Fetchlists the specified biztype.
        /// </summary>
        /// <param name="Biztype">The biztype.</param>
        /// <param name="Process">The process.</param>
        /// <param name="ColType">Type of the col.</param>
        /// <param name="fromDate">From date.</param>
        /// <param name="ToDate">To date.</param>
        /// <param name="Collectionrefno">The collectionrefno.</param>
        /// <param name="Invrefno">The invrefno.</param>
        /// <param name="Custpk">The custpk.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="usrLocFK">The usr loc fk.</param>
        /// <param name="SortColumn">The sort column.</param>
        /// <param name="SortType">Type of the sort.</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public object Fetchlist(long Biztype, long Process, long ColType, string fromDate = "", string ToDate = "", string Collectionrefno = "", string Invrefno = "", string Custpk = "", Int32 CurrentPage = 0, Int32 TotalPage = 0,
        long usrLocFK = 0, string SortColumn = "", string SortType = "DESC", Int32 flag = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            StringBuilder strSQLBuilder = new StringBuilder();
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            StringBuilder strCount = new StringBuilder();

            strSQLBuilder.Append(" SELECT COL.COLLECTIONS_TBL_PK PK, ");
            if (ColType == 1)
            {
                if (Biztype == 2)
                {
                    strSQLBuilder.Append(" VMT.OPERATOR_ID PARTY, ");
                }
                else
                {
                    strSQLBuilder.Append(" VMT.AIRLINE_ID PARTY, ");
                }
            }
            else
            {
                strSQLBuilder.Append(" AMT.AGENT_ID PARTY, ");
            }
            strSQLBuilder.Append(" COL.COLLECTIONS_REF_NO RECDREFNR, ");
            strSQLBuilder.Append(" COL.COLLECTIONS_DATE COLLECTIONDATE, ");
            strSQLBuilder.Append(" CUR.CURRENCY_ID CURRENCY, ");
            strSQLBuilder.Append(" SUM(CTRN.RECD_AMOUNT_HDR_CURR) RECDAMOUNT ");
            strSQLBuilder.Append(" FROM COLLECTIONS_TBL       COL, ");
            strSQLBuilder.Append(" COLLECTIONS_TRN_TBL   CTRN,USER_MST_TBL UMT, ");
            if (ColType == 1)
            {
                if (Biztype == 2)
                {
                    strSQLBuilder.Append(" OPERATOR_MST_TBL         VMT, ");
                }
                else
                {
                    strSQLBuilder.Append(" AIRLINE_MST_TBL         VMT, ");
                }
            }
            else
            {
                strSQLBuilder.Append(" AGENT_MST_TBL         AMT, ");
            }
            strSQLBuilder.Append(" CURRENCY_TYPE_MST_TBL CUR ");
            strSQLBuilder.Append(" WHERE COL.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK ");
            strSQLBuilder.Append(" AND COL.BUSINESS_TYPE = '" + Biztype + "'");
            strSQLBuilder.Append(" AND COL.PROCESS_TYPE = '" + Process + "'");
            strSQLBuilder.Append(" AND COL.COLLECTION_TYPE = '" + ColType + "'");

            if (ColType == 1)
            {
                if (Biztype == 2)
                {
                    strSQLBuilder.Append(" AND VMT.OPERATOR_MST_PK = COL.AGENT_MST_FK ");
                }
                else
                {
                    strSQLBuilder.Append(" AND VMT.AIRLINE_MST_PK = COL.AGENT_MST_FK ");
                }
            }
            else
            {
                strSQLBuilder.Append(" AND AMT.AGENT_MST_PK = COL.AGENT_MST_FK ");
            }
            strSQLBuilder.Append(" AND CUR.CURRENCY_MST_PK = COL.CURRENCY_MST_FK ");
            strSQLBuilder.Append(" AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK + " ");
            strSQLBuilder.Append(" AND COL.CREATED_BY_FK = UMT.USER_MST_PK ");
            strSQLBuilder.Append(" AND CTRN.FROM_INV_OR_CONSOL_INV = 3 ");
            if (flag == 0)
            {
                strSQLBuilder.Append(" AND 1=2 ");
            }
            if (!string.IsNullOrEmpty(Collectionrefno))
            {
                strSQLBuilder.Append(" AND UPPER(COL.COLLECTIONS_REF_NO) LIKE '%" + Collectionrefno.Trim().ToUpper().Replace("'", "''") + "%' ");
            }
            if (!string.IsNullOrEmpty(Custpk))
            {
                strSQLBuilder.Append(" AND COL.AGENT_MST_FK = '" + Custpk + "' ");
            }
            if (!string.IsNullOrEmpty(Invrefno))
            {
                strSQLBuilder.Append(" AND UPPER(CTRN.INVOICE_REF_NR) LIKE '%" + Invrefno.Trim().ToUpper().Replace("'", "''") + "%' ");
            }

            if (!string.IsNullOrEmpty(Convert.ToString(Convert.ToString(getDefault(fromDate, "")))) & !string.IsNullOrEmpty(Convert.ToString(getDefault(ToDate, ""))))
            {
                strSQLBuilder.Append(" AND TO_DATE(COL.COLLECTIONS_DATE) BETWEEN TO_DATE('" + fromDate + "', '" + dateFormat + "') AND TO_DATE('" + ToDate + "', '" + dateFormat + "')");
            }
            else if (!string.IsNullOrEmpty(Convert.ToString(Convert.ToString(getDefault(fromDate, "")))))
            {
                strSQLBuilder.Append(" AND COL.COLLECTIONS_DATE  >=TO_DATE('" + fromDate + "' ,'" + dateFormat + "')");
            }
            else if (!string.IsNullOrEmpty(Convert.ToString(getDefault(ToDate, ""))))
            {
                strSQLBuilder.Append(" AND COL.COLLECTIONS_DATE  <=TO_DATE('" + ToDate + "' ,'" + dateFormat + "')");
            }

            strSQLBuilder.Append(" GROUP BY COL.COLLECTIONS_TBL_PK, ");
            if (ColType == 1)
            {
                if (Biztype == 2)
                {
                    strSQLBuilder.Append(" VMT.OPERATOR_ID, ");
                }
                else
                {
                    strSQLBuilder.Append(" VMT.AIRLINE_ID, ");
                }
            }
            else
            {
                strSQLBuilder.Append(" AMT.AGENT_ID, ");
            }
            strSQLBuilder.Append(" COL.COLLECTIONS_REF_NO, ");
            strSQLBuilder.Append(" COL.COLLECTIONS_DATE, ");
            strSQLBuilder.Append(" CUR.CURRENCY_ID   ORDER BY " + SortColumn + "  " + SortType + "  ");

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
            strSQL.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            string sql = null;
            sql = strSQL.ToString();

            DataSet DS = null;
            try
            {
                DS = objWF.GetDataSet(sql);
                DataRelation CONTRel = null;
                DS.Tables.Add(Fetchchildlist(AllMasterPKs(DS), Biztype, Process, ColType, fromDate, ToDate, Collectionrefno, Invrefno, Custpk, usrLocFK,
                SortColumn, SortType));
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

        #endregion "Fetch Function"

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

        #region "Child Table"

        /// <summary>
        /// Fetchchildlists the specified cont spot p ks.
        /// </summary>
        /// <param name="CONTSpotPKs">The cont spot p ks.</param>
        /// <param name="Biztype">The biztype.</param>
        /// <param name="Process">The process.</param>
        /// <param name="Coltype">The coltype.</param>
        /// <param name="fromDate">From date.</param>
        /// <param name="ToDate">To date.</param>
        /// <param name="Collectionrefno">The collectionrefno.</param>
        /// <param name="Invrefno">The invrefno.</param>
        /// <param name="Custpk">The custpk.</param>
        /// <param name="usrLocFK">The usr loc fk.</param>
        /// <param name="SortColumn">The sort column.</param>
        /// <param name="SortType">Type of the sort.</param>
        /// <returns></returns>
        public DataTable Fetchchildlist(string CONTSpotPKs = "", long Biztype = 0, long Process = 0, long Coltype = 0, string fromDate = "", string ToDate = "", string Collectionrefno = "", string Invrefno = "", string Custpk = "", long usrLocFK = 0,
        string SortColumn = "", string SortType = "DESC")
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
            buildQuery.Append(" (SELECT COL.COLLECTIONS_TBL_PK PK, ");
            buildQuery.Append(" CTRN.INVOICE_REF_NR INVOICENR, ");
            buildQuery.Append(" CUR.CURRENCY_ID CURRENCY ,");
            buildQuery.Append(" CTRN.RECD_AMOUNT_HDR_CURR RECDAMOUNT ");
            buildQuery.Append(" FROM COLLECTIONS_TBL       COL, ");
            buildQuery.Append(" COLLECTIONS_TRN_TBL   CTRN, ");
            if (Coltype == 1)
            {
                if (Biztype == 2)
                {
                    buildQuery.Append(" OPERATOR_MST_TBL         AMT, ");
                }
                else
                {
                    buildQuery.Append(" AIRLINE_MST_TBL         AMT, ");
                }
            }
            else
            {
                buildQuery.Append(" AGENT_MST_TBL         AMT, ");
            }
            buildQuery.Append(" USER_MST_TBL UMT, ");
            buildQuery.Append(" CURRENCY_TYPE_MST_TBL CUR ");
            buildQuery.Append(" WHERE ");
            buildQuery.Append(" COL.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK ");
            buildQuery.Append(" AND COL.BUSINESS_TYPE  = '" + Biztype + "' ");
            buildQuery.Append(" AND COL.PROCESS_TYPE= '" + Process + "' ");
            buildQuery.Append(" AND COL.COLLECTION_TYPE= '" + Coltype + "' ");

            if (Coltype == 1)
            {
                if (Biztype == 2)
                {
                    buildQuery.Append(" AND AMT.OPERATOR_MST_PK = COL.AGENT_MST_FK ");
                }
                else
                {
                    buildQuery.Append(" AND AMT.AIRLINE_MST_PK = COL.AGENT_MST_FK ");
                }
            }
            else
            {
                buildQuery.Append(" AND AMT.AGENT_MST_PK = COL.AGENT_MST_FK ");
            }

            buildQuery.Append(" AND CUR.CURRENCY_MST_PK = COL.CURRENCY_MST_FK ");
            buildQuery.Append(" AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK + " ");
            buildQuery.Append(" AND COL.CREATED_BY_FK = UMT.USER_MST_PK ");
            buildQuery.Append(" AND CTRN.FROM_INV_OR_CONSOL_INV = 3 ");

            if (!string.IsNullOrEmpty(Custpk))
            {
                buildQuery.Append(" AND COL.AGENT_MST_FK = '" + Custpk + "' ");
            }

            if (!string.IsNullOrEmpty(Collectionrefno))
            {
                buildQuery.Append(" AND UPPER(COL.COLLECTIONS_REF_NO) LIKE '%" + Collectionrefno.Trim().ToUpper().Replace("'", "''") + "%' ");
            }

            if (!string.IsNullOrEmpty(Invrefno))
            {
                buildQuery.Append(" AND UPPER(CTRN.INVOICE_REF_NR) LIKE '%" + Invrefno.Trim().ToUpper().Replace("'", "''") + "%' ");
            }

            if (!string.IsNullOrEmpty(Convert.ToString(getDefault(fromDate, ""))) & !string.IsNullOrEmpty(Convert.ToString(getDefault(ToDate, ""))))
            {
                buildQuery.Append(" AND TO_DATE(COL.COLLECTIONS_DATE)  BETWEEN TO_DATE('" + fromDate + "', '" + dateFormat + "') AND TO_DATE('" + ToDate + "', '" + dateFormat + "')");
            }
            else if (!string.IsNullOrEmpty(Convert.ToString(getDefault(fromDate, ""))))
            {
                buildQuery.Append(" AND COL.COLLECTIONS_DATE  >=TO_DATE('" + fromDate + "' ,'" + dateFormat + "')");
            }
            else if (!string.IsNullOrEmpty(Convert.ToString(getDefault(ToDate, ""))))
            {
                buildQuery.Append(" AND COL.COLLECTIONS_DATE  <=TO_DATE('" + ToDate + "' ,'" + dateFormat + "')");
            }

            if (CONTSpotPKs.Trim().Length > 0 | CONTSpotPKs != "-1")
            {
                buildQuery.Append(" AND COL.COLLECTIONS_TBL_PK  in (" + CONTSpotPKs + ") ");
            }
            buildQuery.Append(")T ");

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

        #endregion "Child Table"

        #region "Fetch Invoice Nr."

        /// <summary>
        /// Fetches the inv nr.
        /// </summary>
        /// <param name="Biz">The biz.</param>
        /// <param name="Proc">The proc.</param>
        /// <param name="InvNr">The inv nr.</param>
        /// <returns></returns>
        public int FetchInvNr(short Biz, short Proc, string InvNr = "0")
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            if (Biz == 2 & Proc == 1)
            {
                strSQL = "SELECT count(*) FROM INV_CUST_SEA_EXP_TBL  INV" + "WHERE INV.INVOICE_REF_NO= '" + InvNr + "'";
            }
            else if (Biz == 1 & Proc == 1)
            {
                strSQL = "SELECT count(*) FROM INV_CUST_AIR_EXP_TBL  INV" + "WHERE INV.INVOICE_REF_NO= '" + InvNr + "'";
            }
            else if (Biz == 2 & Proc == 2)
            {
                strSQL = "SELECT count(*) FROM INV_CUST_SEA_IMP_TBL  INV" + "WHERE INV.INVOICE_REF_NO= '" + InvNr + "'";
            }
            else
            {
                strSQL = "SELECT count(*) FROM INV_CUST_AIR_IMP_TBL  INV" + "WHERE INV.INVOICE_REF_NO= '" + InvNr + "'";
            }
            try
            {
                return Convert.ToInt32(objWF.ExecuteScaler(strSQL));
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

        #endregion "Fetch Invoice Nr."
    }
}