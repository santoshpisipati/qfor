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
    public class clsRemovalsCollectionlist : CommonFeatures
    {
        #region "Fetch Function"

        public object Fetchlist(string fromDate = "", string ToDate = "", string Collectionrefno = "", string Invrefno = "", string Custpk = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, long usrLocFK = 0, string SortColumn = "", string SortType = "DESC",
        Int32 flag = 0)
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
            strSQLBuilder.Append(" CMT.CUSTOMER_ID PARTY, ");
            strSQLBuilder.Append(" COL.COLLECTIONS_REF_NO RECDREFNR, ");
            strSQLBuilder.Append(" COL.COLLECTIONS_DATE COLLECTIONDATE, ");
            strSQLBuilder.Append(" SUM(CTRN.RECD_AMOUNT_HDR_CURR) RECDAMOUNT, ");
            strSQLBuilder.Append(" CUR.CURRENCY_ID CURRENCY ");
            strSQLBuilder.Append(" FROM REM_M_COLLECTIONS_TBL       COL, ");
            strSQLBuilder.Append(" REM_T_COLLECTIONS_TRN_TBL   CTRN,USER_MST_TBL UMT, ");
            strSQLBuilder.Append(" CUSTOMER_MST_TBL      CMT, ");
            strSQLBuilder.Append(" CURRENCY_TYPE_MST_TBL CUR ");
            strSQLBuilder.Append(" WHERE COL.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK ");
            strSQLBuilder.Append(" AND CMT.CUSTOMER_MST_PK = COL.CUSTOMER_MST_FK ");
            strSQLBuilder.Append(" AND CUR.CURRENCY_MST_PK = COL.CURRENCY_MST_FK ");
            strSQLBuilder.Append(" AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK + " ");
            strSQLBuilder.Append(" AND COL.CREATED_BY_FK = UMT.USER_MST_PK ");
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
                strSQLBuilder.Append(" AND COL.CUSTOMER_MST_FK = '" + Custpk + "' ");
            }
            if (!string.IsNullOrEmpty(Invrefno))
            {
                strSQLBuilder.Append(" AND UPPER(CTRN.INVOICE_REF_NR) LIKE '%" + Invrefno.Trim().ToUpper().Replace("'", "''") + "%' ");
            }

            if (((fromDate != null) | !string.IsNullOrEmpty(fromDate)) & (ToDate == null | !string.IsNullOrEmpty(ToDate)))
            {
                strSQLBuilder.Append(" AND COL.COLLECTIONS_DATE  >=TO_DATE('" + fromDate + "' ,'" + dateFormat + "')");
            }

            if (((ToDate != null) | !string.IsNullOrEmpty(ToDate)) & (fromDate == null | !string.IsNullOrEmpty(fromDate)))
            {
                strSQLBuilder.Append(" AND COL.COLLECTIONS_DATE  <=TO_DATE('" + ToDate + "' ,'" + dateFormat + "')");
            }

            if (((fromDate != null) | !string.IsNullOrEmpty(fromDate)) & ((ToDate != null) | !string.IsNullOrEmpty(ToDate)))
            {
                strSQLBuilder.Append(" AND COL.COLLECTIONS_DATE  BETWEEN TO_DATE('" + fromDate + "', '" + dateFormat + "') AND TO_DATE('" + ToDate + "', '" + dateFormat + "')");
            }

            strSQLBuilder.Append(" GROUP BY COL.COLLECTIONS_TBL_PK, ");
            strSQLBuilder.Append(" CMT.CUSTOMER_ID, ");
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
                DS.Tables.Add(Fetchchildlist(AllMasterPKs(DS), fromDate, ToDate, Collectionrefno, Invrefno, Custpk, usrLocFK, SortColumn, SortType));
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

        private string AllMasterPKs(DataSet ds)
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

        #endregion "PK Value"

        //Child Table

        #region "Child Table"

        public DataTable Fetchchildlist(string CONTSpotPKs = "", string fromDate = "", string ToDate = "", string Collectionrefno = "", string Invrefno = "", string Custpk = "", long usrLocFK = 0, string SortColumn = "", string SortType = "DESC")
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
            buildQuery.Append(" CTRN.RECD_AMOUNT_HDR_CURR RECDAMOUNT, ");
            buildQuery.Append(" CUR.CURRENCY_ID CURRENCY ");
            buildQuery.Append(" FROM REM_M_COLLECTIONS_TBL       COL, ");
            buildQuery.Append(" REM_T_COLLECTIONS_TRN_TBL   CTRN, ");
            buildQuery.Append(" CUSTOMER_MST_TBL      CMT, USER_MST_TBL UMT,");
            buildQuery.Append(" CURRENCY_TYPE_MST_TBL CUR ");
            buildQuery.Append(" WHERE ");
            buildQuery.Append(" COL.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK ");
            buildQuery.Append(" AND CMT.CUSTOMER_MST_PK = COL.CUSTOMER_MST_FK ");
            buildQuery.Append(" AND CUR.CURRENCY_MST_PK = COL.CURRENCY_MST_FK ");
            buildQuery.Append(" AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK + " ");
            buildQuery.Append(" AND COL.CREATED_BY_FK = UMT.USER_MST_PK ");

            if (!string.IsNullOrEmpty(Custpk))
            {
                buildQuery.Append(" AND COL.CUSTOMER_MST_FK = '" + Custpk + "' ");
            }

            if (!string.IsNullOrEmpty(Collectionrefno))
            {
                buildQuery.Append(" AND UPPER(COL.COLLECTIONS_REF_NO) LIKE '%" + Collectionrefno.Trim().ToUpper().Replace("'", "''") + "%' ");
            }

            if (!string.IsNullOrEmpty(Invrefno))
            {
                buildQuery.Append(" AND UPPER(CTRN.INVOICE_REF_NR) LIKE '%" + Invrefno.Trim().ToUpper().Replace("'", "''") + "%' ");
            }

            if (((fromDate != null) | !string.IsNullOrEmpty(fromDate)) & (ToDate == null | !string.IsNullOrEmpty(ToDate)))
            {
                buildQuery.Append(" AND COL.COLLECTIONS_DATE  >=TO_DATE('" + fromDate + "' ,'" + dateFormat + "')");
            }

            if (((ToDate != null) | !string.IsNullOrEmpty(ToDate)) & (fromDate == null | !string.IsNullOrEmpty(fromDate)))
            {
                buildQuery.Append(" AND COL.COLLECTIONS_DATE  <=TO_DATE('" + ToDate + "' ,'" + dateFormat + "')");
            }

            if (((fromDate != null) | !string.IsNullOrEmpty(fromDate)) & ((ToDate != null) | !string.IsNullOrEmpty(ToDate)))
            {
                buildQuery.Append(" AND COL.COLLECTIONS_DATE  BETWEEN TO_DATE('" + fromDate + "', '" + dateFormat + "') AND TO_DATE('" + ToDate + "', '" + dateFormat + "')");
            }

            if (CONTSpotPKs.Trim().Length > 0 | CONTSpotPKs != "-1")
            {
                buildQuery.Append(" AND COL.COLLECTIONS_TBL_PK  in (" + CONTSpotPKs + ") ");
            }

            buildQuery.Append(")T ");
            buildQuery.Append(" order by pk ");
            strsql = buildQuery.ToString();
            try
            {
                pk = -1;
                dt = objWF.GetDataTable(strsql);
                for (RowCnt = 0; RowCnt <= dt.Rows.Count - 1; RowCnt++)
                {
                    if (Convert.ToInt32(dt.Rows[RowCnt]["PK"])!= pk)
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

        public int FetchInvNr(short Biz, short Proc, string InvNr = "0")
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            //Sea & Export
            if (Biz == 2 & Proc == 1)
            {
                strSQL = "SELECT count(*) FROM INV_CUST_SEA_EXP_TBL  INV" + "WHERE INV.INVOICE_REF_NO= '" + InvNr + "'";
                //Air Export
            }
            else if (Biz == 1 & Proc == 1)
            {
                strSQL = "SELECT count(*) FROM INV_CUST_AIR_EXP_TBL  INV" + "WHERE INV.INVOICE_REF_NO= '" + InvNr + "'";
                //Sea Import
            }
            else if (Biz == 2 & Proc == 2)
            {
                strSQL = "SELECT count(*) FROM INV_CUST_SEA_IMP_TBL  INV" + "WHERE INV.INVOICE_REF_NO= '" + InvNr + "'";
                //Air Import
            }
            else
            {
                strSQL = "SELECT count(*) FROM INV_CUST_AIR_IMP_TBL  INV" + "WHERE INV.INVOICE_REF_NO= '" + InvNr + "'";
            }
            try
            {
                return Convert.ToInt32(objWF.ExecuteScaler(strSQL));
                //'Manjunath  PTS ID:Sep-02  28/09/2011
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Invoice Nr."
    }
}