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
using System.Text;

namespace Quantum_QFOR
{
    public class clsRemovalConsolidatedlist : CommonFeatures
    {
        #region "Parent"

        public Int32 FetchBusinessType(Int32 userpk)
        {
            StringBuilder strsql = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strsql.Append("select users.business_type from user_mst_tbl users where users.user_mst_pk=" + userpk);
                return Convert.ToInt32(objWF.ExecuteScaler(strsql.ToString()));
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

        public DataSet FetchListData(string strInvRefNo = "", string strJobRefNo = "", string strCustomer = "", string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " DESC ", long usrLocFK = 0, string uniqueRefNr = "", Int32 flag = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            System.Text.StringBuilder strCondition = new System.Text.StringBuilder();
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            string a = null;
            string b = null;
            a = strCustomer;
            if (a == "0")
            {
                strCustomer = "";
            }
            else
            {
                strCustomer = strCustomer;
            }

            strCondition.Append(" SELECT   INV.REMOVALS_INVOICE_PK PK,");
            strCondition.Append(" INV.INVOICE_REF_NO,");
            strCondition.Append(" JOB.JOB_CARD_REF,");
            strCondition.Append(" CMT.CUSTOMER_NAME, ");
            strCondition.Append(" INV.INVOICE_DATE,");
            strCondition.Append(" CUMT.CURRENCY_ID,INV.NET_RECEIVABLE,");
            //strCondition.Append(" 0 Recieved ,")
            //strCondition.Append(" NVL((INV.NET_RECEIVABLE - 0),")
            //strCondition.Append("  0) Balance ")
            strCondition.Append("NVL((select sum(ctrn.recd_amount_hdr_curr)");
            strCondition.Append("   from rem_t_collections_trn_tbl ctrn");
            strCondition.Append("   where ctrn.invoice_ref_nr like inv.invoice_ref_no),");
            strCondition.Append("   0) Recieved,");
            strCondition.Append("   NVL(INV.NET_RECEIVABLE -");
            strCondition.Append("   NVL((select sum(ctrn.recd_amount_hdr_curr)");
            strCondition.Append("   from rem_t_collections_trn_tbl ctrn");
            strCondition.Append("   where ctrn.invoice_ref_nr like");
            strCondition.Append("   inv.invoice_ref_no),");
            strCondition.Append("   0.00),");
            strCondition.Append("   0) Balance");
            strCondition.Append(" FROM");
            strCondition.Append(" REM_M_INVOICE_TBL INV, ");
            strCondition.Append(" REM_INVOICE_TRN_TBL INVTRN,");
            strCondition.Append(" rem_m_job_card_mst_tbl   JOB,");
            strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
            strCondition.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
            strCondition.Append(" USER_MST_TBL           UMT");
            strCondition.Append(" WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_PK(+) ");
            strCondition.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+) ");
            strCondition.Append(" AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
            //strCondition.Append(" AND UMT.DEFAULT_LOCATION_FK = " & usrLocFK & " ")
            //strCondition.Append(" AND INV.CREATED_BY_FK = UMT.USER_MST_PK")
            strCondition.Append(" AND INV.REMOVALS_INVOICE_PK = INVTRN.REMOVALS_INVOICE_FK(+)");

            if (!string.IsNullOrEmpty(strJobRefNo))
            {
                strCondition.Append(" AND JOB.JOB_CARD_REF='" + strJobRefNo + "'");
            }

            if (!string.IsNullOrEmpty(strCustomer))
            {
                strCondition.Append(" AND CMT.CUSTOMER_MST_PK IN (" + strCustomer + ")");
            }

            if (!string.IsNullOrEmpty(strInvRefNo))
            {
                strCondition.Append(" AND INV.INVOICE_REF_NO = '" + strInvRefNo + "' ");
            }

            if (!string.IsNullOrEmpty(uniqueRefNr))
            {
                strCondition.Append(" AND INV.INV_UNIQUE_REF_NR LIKE '%" + uniqueRefNr.Replace("'", "''") + "%' ");
            }

            strCondition.Append(" GROUP BY");
            strCondition.Append(" INV.REMOVALS_INVOICE_PK,");
            strCondition.Append(" INV.INVOICE_REF_NO,");
            strCondition.Append(" JOB.JOB_CARD_REF,");
            strCondition.Append(" INV.INVOICE_DATE ,");
            strCondition.Append(" CUMT.CURRENCY_ID,CMT.CUSTOMER_NAME,");
            strCondition.Append(" INV.NET_RECEIVABLE,INV.CREATED_DT,INV.INV_UNIQUE_REF_NR  ORDER BY INV.CREATED_DT DESC");

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + strCondition.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

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

            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append("SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + strCondition.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);

            string sql = null;
            sql = sqlstr.ToString();
            DataSet DS = null;
            try
            {
                DS = objWF.GetDataSet(sql);

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

        #endregion "Parent"

        #region "PK Value"

        private string AllMasterPKs(DataSet ds)
        {
            Int16 RowCnt = default(Int16);
            Int16 ln = default(Int16);
            System.Text.StringBuilder strBuild = new System.Text.StringBuilder();
            strBuild.Append("-1,");
            for (RowCnt = 0; RowCnt <= Convert.ToInt16(ds.Tables[0].Rows.Count - 1); RowCnt++)
            {
                strBuild.Append(Convert.ToString(ds.Tables[0].Rows[RowCnt]["PK"]).Trim() + ",");
            }
            strBuild.Remove(strBuild.Length - 1, 1);
            return strBuild.ToString();
        }

        #endregion "PK Value"

        #region "PK Value"

        public string Fetchcustpk(string custid)
        {
            string strSQL = null;
            strSQL = "select c.customer_mst_pk from customer_mst_tbl c where c.customer_name = '" + custid + "'";
            try
            {
                return (new WorkFlow()).ExecuteScaler(strSQL);
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

        public string FetchEXpk(string PK)
        {
            string strSQL = null;
            strSQL = "select T.EXCH_RATE_TYPE_PK from REM_M_INVOICE_TBL  t  where t.REMOVALS_INVOICE_PK = " + PK;
            try
            {
                return (new WorkFlow()).ExecuteScaler(strSQL);
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

        #endregion "PK Value"

        #region "Take User Biz Type"

        public string FetchBizType(int UsrPk)
        {
            string strSQL = null;
            strSQL = "select usr.business_type from user_mst_tbl usr, role_mst_tbl r where r.role_mst_tbl_pk = usr.role_mst_fk and usr.user_mst_pk = " + UsrPk;
            try
            {
                return (new WorkFlow()).ExecuteScaler(strSQL);
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

        #endregion "Take User Biz Type"
    }
}