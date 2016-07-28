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
    public class Cls_QuotationAirListing : CommonFeatures
    {
        #region " Fetch All "

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="QuotationNo">The quotation no.</param>
        /// <param name="FromDate">From date.</param>
        /// <param name="ToDate">To date.</param>
        /// <param name="POLID">The polid.</param>
        /// <param name="POLName">Name of the pol.</param>
        /// <param name="PODID">The podid.</param>
        /// <param name="PODName">Name of the pod.</param>
        /// <param name="CustomerID">The customer identifier.</param>
        /// <param name="CustomerName">Name of the customer.</param>
        /// <param name="AgentID">The agent identifier.</param>
        /// <param name="AgentName">Name of the agent.</param>
        /// <param name="ApprovalStatus">The approval status.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="SortColumn">The sort column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="SortType">Type of the sort.</param>
        /// <param name="lngUserLocFk">The LNG user loc fk.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="fromPage">From page.</param>
        /// <param name="Priority">The priority.</param>
        /// <param name="Customer_Approved">if set to <c>true</c> [customer_ approved].</param>
        /// <returns></returns>
        public DataSet FetchAll(string QuotationNo = "", string FromDate = "", string ToDate = "", string POLID = "", string POLName = "", string PODID = "", string PODName = "", string CustomerID = "", string CustomerName = "", string AgentID = "",
        string AgentName = "", Int16 ApprovalStatus = 1, string SearchType = "", string SortColumn = " QUOTATION_REF_NO ", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " ASC ", long lngUserLocFk = 0, Int32 flag = 0, string fromPage = "",
        int Priority = 0, bool Customer_Approved = false)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);

            string strSQL = "";
            string strCondition = "";
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder buildCondition = new System.Text.StringBuilder();
            string SrOP = (SearchType == "C" ? "%" : "");
            buildCondition.Append(" FROM          ");
            buildCondition.Append("  QUOTATION_AIR_TBL   MAIN,     ");
            buildCondition.Append("  QUOTATION_TRN_AIR   TRAN,     ");
            buildCondition.Append("  V_ALL_CUSTOMER    CUST,     ");
            buildCondition.Append("  CUSTOMER_CATEGORY_MST_TBL CCAT,     ");
            buildCondition.Append("  AGENT_MST_TBL   AGNT,     ");
            buildCondition.Append("  PORT_MST_TBL    PORTPOL,    ");
            buildCondition.Append("  PORT_MST_TBL    PORTPOD,     ");
            buildCondition.Append("  USER_MST_TBL    UMT     ");
            buildCondition.Append(" WHERE ");
            buildCondition.Append(" MAIN.QUOTATION_AIR_PK  = TRAN.QUOTATION_AIR_FK   ");
            buildCondition.Append(" -- MAIN.QUOTATION_AIR_PK  = TRAN.QUOT_GEN_AIR_FK ");
            buildCondition.Append("  AND MAIN.CUSTOMER_MST_FK   = CUST.CUSTOMER_MST_PK(+)   ");
            buildCondition.Append("  AND MAIN.CUST_TYPE    = CUST.CUSTOMER_TYPE(+)   ");
            buildCondition.Append("  AND MAIN.CUSTOMER_CATEGORY_MST_FK = CCAT.CUSTOMER_CATEGORY_MST_PK(+) ");
            buildCondition.Append("  AND MAIN.AGENT_MST_FK   = AGNT.AGENT_MST_PK(+)   ");
            buildCondition.Append("  AND TRAN.PORT_MST_POL_FK   = PORTPOL.PORT_MST_PK   ");
            buildCondition.Append("  AND TRAN.PORT_MST_POD_FK   = PORTPOD.PORT_MST_PK   ");

            if (flag == 0)
            {
                buildCondition.Append(" AND 1=2 ");
            }
            if (QuotationNo.Length > 0)
            {
                buildCondition.Append(" AND UPPER(QUOTATION_REF_NO) LIKE '" + SrOP + QuotationNo.ToUpper().Replace("'", "''") + "%'");
            }
            if (FromDate.Length > 0)
            {
                buildCondition.Append(" AND (main.QUOTATION_DATE + main.VALID_FOR) >= TO_DATE('" + FromDate + "' , '" + dateFormat + "') ");
            }
            if (ToDate.Length > 0)
            {
                buildCondition.Append(" AND main.QUOTATION_DATE <= TO_DATE('" + ToDate + "' , '" + dateFormat + "') ");
            }
            if (POLID.Length > 0)
            {
                buildCondition.Append(" AND UPPER(portpol.PORT_ID) LIKE '" + SrOP + POLID.ToUpper().Replace("'", "''") + "%'");
            }
            if (POLName.Length > 0)
            {
                buildCondition.Append(" AND UPPER(portpol.PORT_NAME) LIKE '" + SrOP + POLName.ToUpper().Replace("'", "''") + "%'");
            }
            if (PODID.Length > 0)
            {
                buildCondition.Append(" AND UPPER(portpod.PORT_ID) LIKE '" + SrOP + PODID.ToUpper().Replace("'", "''") + "%'");
            }
            if (PODName.Length > 0)
            {
                buildCondition.Append(" AND UPPER(portpod.PORT_NAME) LIKE '" + SrOP + PODName.ToUpper().Replace("'", "''") + "%'");
            }

            if (CustomerID.Length > 0)
            {
                buildCondition.Append(" AND UPPER(CUSTOMER_ID) LIKE '" + SrOP + CustomerID.ToUpper().Replace("'", "''") + "%'");
            }
            if (CustomerName.Length > 0)
            {
                buildCondition.Append(" AND UPPER(CUSTOMER_NAME) LIKE '" + SrOP + CustomerName.ToUpper().Replace("'", "''") + "%'");
            }
            if (AgentID.Length > 0)
            {
                buildCondition.Append(" AND UPPER(AGENT_ID) LIKE '" + SrOP + AgentID.ToUpper().Replace("'", "''") + "%'");
            }
            if (AgentName.Length > 0)
            {
                buildCondition.Append(" AND UPPER(AGENT_NAME) LIKE '" + SrOP + AgentName.ToUpper().Replace("'", "''") + "%'");
            }
            if (!string.IsNullOrEmpty(fromPage))
            {
                if (fromPage == "simple" | fromPage == "approval")
                {
                    buildCondition.Append(" AND main.FROM_FLAG = 0");
                }
                else
                {
                    buildCondition.Append(" AND main.FROM_FLAG = 1");
                }
            }
            if (Priority > 0)
            {
                buildCondition.Append("  and cust.PRIORITY=" + Priority);
            }
            if (ApprovalStatus != 5)
            {
                buildCondition.Append(" AND main.STATUS = " + ApprovalStatus);
            }
            if (Customer_Approved)
            {
                buildCondition.Append(" AND main.CUSTOMER_APPROVED = 1");
            }
            buildCondition.Append(" AND UMT.DEFAULT_LOCATION_FK = " + lngUserLocFk + " ");
            buildCondition.Append(" AND main.CREATED_BY_FK = UMT.USER_MST_PK ");
            strCondition = buildCondition.ToString();

            strSQL = "SELECT COUNT(*) FROM  (Select DISTINCT QUOTATION_AIR_PK " + strCondition + "Union  Select DISTINCT QUOTATION_AIR_PK " + strCondition.Replace("QUOTATION_TRN_AIR", "QUOT_GEN_TRN_AIR_TBL").Replace("QUOTATION_AIR_FK", "QUOT_GEN_AIR_FK") + ")";

            strSQL = strSQL.Replace("   ", " ");
            strSQL = strSQL.Replace("  ", " ");

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));

            TotalPage = TotalRecords / RecordsPerPage;
            if (TotalRecords % RecordsPerPage != 0)
                TotalPage += 1;
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;
            last = CurrentPage * RecordsPerPage;
            start = (CurrentPage - 1) * RecordsPerPage + 1;

            System.Text.StringBuilder strSqlMain = new System.Text.StringBuilder();
            strSqlMain.Append("( SELECT");
            strSqlMain.Append(" QUOTATION_AIR_PK,        ");
            strSqlMain.Append(" QUOTATION_REF_NO,        ");
            strSqlMain.Append(" QUOTATION_DATE QUOTATION_DATE, ");
            strSqlMain.Append(" CUSTOMER_MST_FK,       ");
            strSqlMain.Append(" CUSTOMER_ID,        ");
            strSqlMain.Append(" CUSTOMER_NAME,        ");
            strSqlMain.Append(" PRIORITY,        ");
            strSqlMain.Append(" AGENT_MST_FK,        ");
            strSqlMain.Append(" AGENT_ID,         ");
            strSqlMain.Append(" AGENT_NAME,        ");
            strSqlMain.Append(" QTYPE,  ");
            strSqlMain.Append(" Status,CUSTOMER_APPROVED FROM(");
            System.Text.StringBuilder strSqlPart = new System.Text.StringBuilder();
            strSqlPart.Append("Select DISTINCT        ");
            strSqlPart.Append("  main.QUOTATION_AIR_PK,        ");
            strSqlPart.Append("  main.QUOTATION_REF_NO,        ");
            strSqlPart.Append("  main.QUOTATION_DATE QUOTATION_DATE, ");
            strSqlPart.Append("  main.CUSTOMER_MST_FK,       ");
            strSqlPart.Append("  cust.CUSTOMER_ID,        ");
            strSqlPart.Append("  cust.CUSTOMER_NAME,        ");
            strSqlPart.Append("  DECODE(cust.PRIORITY, 1, 'Priority 1', 2, 'Priority 2', 3, 'Priority 3',4, 'Priority 4',5, 'Priority 5') PRIORITY, ");
            strSqlPart.Append("  main.AGENT_MST_FK,        ");
            strSqlPart.Append("  agnt.AGENT_ID,         ");
            strSqlPart.Append("  agnt.AGENT_NAME,        ");
            strSqlPart.Append("  MAIN.QUOTATION_TYPE QTYPE,");
            strSqlPart.Append("  DECODE(MAIN.STATUS,1,'Active',2,'Confirm',3,'Cancelled',4,'Used') Status,");
            strSqlPart.Append("  main.CUSTOMER_APPROVED ");
            strSQL = " Select * from                                                         " + "  ( Select ROWNUM SR_NO, q.* from                                       " + strSqlMain.ToString() + strSqlPart.ToString() + strCondition + "UNION " + strSqlPart.ToString() + strCondition.Replace("QUOTATION_TRN_AIR", "QUOT_GEN_TRN_AIR_TBL").Replace("QUOTATION_AIR_FK", "QUOT_GEN_AIR_FK") + ")" + "     Order By " + SortColumn + SortType + "     ,QUOTATION_REF_NO DESC " + "  ) q " + "  )   " + " where  SR_NO between " + start + " and " + last;

            strSQL = strSQL.Replace("   ", " ");
            strSQL = strSQL.Replace("  ", " ");

            DataSet DS = null;
            try
            {
                DS = objWF.GetDataSet(strSQL);
                DS.Tables.Add(FetchChilds(AllMasterPKs(DS), POLID, POLName, PODID, PODName, SrOP));
                DataRelation rfqRel = new DataRelation("RFQRelation", DS.Tables[0].Columns["QUOTATION_AIR_PK"], DS.Tables[1].Columns["QUOTATION_AIR_PK"], true);
                DS.Relations.Add(rfqRel);
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

        #endregion " Fetch All "

        #region " All Master Quotation PKs "

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
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                strBuilder.Append("-1,");
                for (RowCnt = 0; RowCnt <= ds.Tables[0].Rows.Count - 1; RowCnt++)
                {
                    strBuilder.Append(Convert.ToString(ds.Tables[0].Rows[RowCnt]["QUOTATION_AIR_PK"]).Trim() + ",");
                }
                strBuilder.Remove(strBuilder.Length - 1, 1);
                return strBuilder.ToString();
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

        #endregion " All Master Quotation PKs "

        #region " Fetch Child Rows "

        /// <summary>
        /// Fetches the childs.
        /// </summary>
        /// <param name="QuotationPKs">The quotation p ks.</param>
        /// <param name="POLID">The polid.</param>
        /// <param name="POLName">Name of the pol.</param>
        /// <param name="PODID">The podid.</param>
        /// <param name="PODName">Name of the pod.</param>
        /// <param name="SrOp">The sr op.</param>
        /// <returns></returns>
        private DataTable FetchChilds(string QuotationPKs = "", string POLID = "", string POLName = "", string PODID = "", string PODName = "", string SrOp = "%")
        {
            string strSQL = "";
            string strCondition = "";
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder buildCondition = new System.Text.StringBuilder();
            buildCondition.Append("FROM QUOTATION_AIR_TBL       MAIN,");
            buildCondition.Append("       QUOTATION_TRN_AIR       TRAN,");
            buildCondition.Append("      -- QUOT_GEN_TRN_AIR_TBL       TRAN,");
            buildCondition.Append("       PORT_MST_TBL            PORTPOL,");
            buildCondition.Append("       PORT_MST_TBL            PORTPOD,");
            buildCondition.Append("       AIRLINE_MST_TBL         AIR,");
            buildCondition.Append("       COMMODITY_GROUP_MST_TBL CGRP");
            buildCondition.Append(" WHERE MAIN.QUOTATION_AIR_PK = TRAN.QUOTATION_AIR_FK");
            buildCondition.Append(" -- MAIN.QUOTATION_AIR_PK = TRAN.QUOT_GEN_AIR_FK");
            buildCondition.Append("   AND TRAN.PORT_MST_POL_FK = PORTPOL.PORT_MST_PK");
            buildCondition.Append("   AND TRAN.PORT_MST_POD_FK = PORTPOD.PORT_MST_PK");
            buildCondition.Append("   AND TRAN.AIRLINE_MST_FK = AIR.AIRLINE_MST_PK(+)");
            buildCondition.Append("  -- AND TRAN.COMMODITY_GROUP_FK = CGRP.COMMODITY_GROUP_PK");
            buildCondition.Append("      AND TRAN.COMMODITY_GROUP_FK = CGRP.COMMODITY_GROUP_PK");
            if (QuotationPKs.Trim().Length > 0)
            {
                buildCondition.Append(" and QUOTATION_AIR_PK in (" + QuotationPKs + ") ");
            }
            if (POLID.Trim().Length > 0)
            {
                buildCondition.Append(" and UPPER(portpol.PORT_ID) LIKE '" + SrOp + POLID.ToUpper().Replace("'", "''") + "%'");
            }
            if (POLName.Trim().Length > 0)
            {
                buildCondition.Append(" and UPPER(portpol.PORT_NAME) LIKE '" + SrOp + POLName.ToUpper().Replace("'", "''") + "%'");
            }
            if (PODID.Trim().Length > 0)
            {
                buildCondition.Append(" and UPPER(portpod.PORT_ID) LIKE '" + SrOp + PODID.ToUpper().Replace("'", "''") + "%'");
            }
            if (PODName.Trim().Length > 0)
            {
                buildCondition.Append(" and UPPER(portpod.PORT_NAME) LIKE '" + SrOp + PODName.ToUpper().Replace("'", "''") + "%'");
            }

            strCondition = buildCondition.ToString();
            System.Text.StringBuilder strMain = new System.Text.StringBuilder();
            strMain.Append("SELECT DISTINCT MAIN.QUOTATION_AIR_PK,");
            strMain.Append("                TRAN.PORT_MST_POL_FK POL,");
            strMain.Append("                PORTPOL.PORT_ID POLID,");
            strMain.Append("                PORTPOL.PORT_NAME POLNAME,");
            strMain.Append("                TRAN.PORT_MST_POD_FK POD,");
            strMain.Append("                PORTPOD.PORT_ID POID,");
            strMain.Append("                PORTPOD.PORT_NAME PODNAME,");
            strMain.Append("                TO_CHAR(MAIN.QUOTATION_DATE, '" + dateFormat + "') VALID_FROM,");
            strMain.Append("                TO_CHAR(MAIN.QUOTATION_DATE + MAIN.VALID_FOR, '" + dateFormat + "') VALID_TO,");
            strMain.Append("                TRAN.AIRLINE_MST_FK AIRLINE,");
            strMain.Append("                AIR.AIRLINE_ID AIRLINEID,");
            strMain.Append("                AIR.AIRLINE_NAME AIRLINENAME,");
            strMain.Append("            --  MAIN.COMMODITY_GROUP_MST_FK, ");
            strMain.Append("            TRAN.COMMODITY_GROUP_FK COMGRP,");
            strMain.Append("                CGRP.COMMODITY_GROUP_CODE COM,");
            strMain.Append("                DECODE(MAIN.STATUS,");
            strMain.Append("                       1,");
            strMain.Append("                       'ACTIVE',");
            strMain.Append("                       2,");
            strMain.Append("                       'CONFIRM',");
            strMain.Append("                       3,");
            strMain.Append("                       'CANCELLED',");
            strMain.Append("                       'USED') STATUS");

            strSQL = "  Select   * From ( " + strMain.ToString() + strCondition + "Union " + strMain.ToString().Replace("TRAN.COMMODITY_GROUP_FK", "MAIN.COMMODITY_GROUP_MST_FK") + strCondition.Replace("TRAN.COMMODITY_GROUP_FK", "MAIN.COMMODITY_GROUP_MST_FK").Replace("QUOTATION_TRN_AIR", "QUOT_GEN_TRN_AIR_TBL").Replace("QUOTATION_AIR_FK", "QUOT_GEN_AIR_FK") + " )  Order By QUOTATION_AIR_PK, POLNAME, PODNAME ASC ";

            try
            {
                return (new WorkFlow()).GetDataTable(strSQL);
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

        #endregion " Fetch Child Rows "

        #region "For entry screen enahance search"

        /// <summary>
        /// Fetches the p kref for quote.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchPKrefForQuote(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string INPUT_TYPE = "";
            string SECTOR = "";
            string DATE_IN = "";
            string CUSTOMER_IN = "";
            string COMMODITY_IN = "";
            string RETURN_PK = null;
            string businessType = null;
            arr = strCond.Split('~');
            INPUT_TYPE = Convert.ToString(arr.GetValue(0));
            if (arr.Length > 1)
                SECTOR = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                DATE_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                CUSTOMER_IN = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                COMMODITY_IN = Convert.ToString(arr.GetValue(4));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".QUOTATION_GEN_AIR_TBL_PKG.RETURNVALUES";
                var _with1 = SCM.Parameters;
                _with1.Add("INPUT_TYPE", getDefault(INPUT_TYPE, "")).Direction = ParameterDirection.Input;
                _with1.Add("SECTOR", getDefault(SECTOR, "(0,0)")).Direction = ParameterDirection.Input;
                _with1.Add("DATE_IN", DATE_IN).Direction = ParameterDirection.Input;
                _with1.Add("CUSTOMER_IN", CUSTOMER_IN).Direction = ParameterDirection.Input;
                _with1.Add("COMMODITY_IN", COMMODITY_IN).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_PK", OracleDbType.Int32, 10, "RETURN_PK").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_PK"].SourceVersion = DataRowVersion.Current;
                _with1.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                RETURN_PK = Convert.ToString(SCM.Parameters["RETURN_PK"].Value);
                if (Convert.ToInt32(RETURN_PK) == 0)
                {
                    strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                }
                else
                {
                    strReturn = RETURN_PK;
                }
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                SCM.Connection.Close();
            }
        }

        #endregion "For entry screen enahance search"
    }
}