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
    public class cls_WriteOff : CommonFeatures
    {
        /// <summary>
        /// The user pk value
        /// </summary>
        private long UserPKValue;

        #region "Property"

        /// <summary>
        /// Gets the user pk.
        /// </summary>
        /// <value>
        /// The user pk.
        /// </value>
        public long UserPK
        {
            get { return UserPKValue; }
        }

        #endregion "Property"

        #region "Currency Property"

        /// <summary>
        /// Gets the corp currency.
        /// </summary>
        /// <returns></returns>
        public DataSet GetCorpCurrency()
        {
            string strSQL = null;
            strSQL = "select CUMT.CURRENCY_MST_PK,CUMT.CURRENCY_ID from currency_type_mst_tbl CUMT";

            try
            {
                DataSet DS = null;
                DS = (new WorkFlow()).GetDataSet(strSQL);
                return DS;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Gets the currency.
        /// </summary>
        /// <param name="CurrPK">The curr pk.</param>
        /// <returns></returns>
        public DataSet GetCurrency(int CurrPK)
        {
            string strSQL = null;
            strSQL = "select CUMT.CURRENCY_MST_PK,CUMT.CURRENCY_ID from currency_type_mst_tbl CUMT where CUMT.CURRENCY_MST_PK = " + CurrPK;

            try
            {
                DataSet DS = null;
                DS = (new WorkFlow()).GetDataSet(strSQL);
                return DS;
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

        #endregion "Currency Property"

        #region "Fetch Function"

        /// <summary>
        /// Fetches the specified loc pk.
        /// </summary>
        /// <param name="LocPK">The loc pk.</param>
        /// <param name="UserPK">The user pk.</param>
        /// <returns></returns>
        public DataTable Fetch(int LocPK, int UserPK)
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;

            strSQL += " select WR.WRITE_OFF_PK,WR.WRITE_OFF_LOC_FK,";
            strSQL += "  WR.WRITE_OFF_LOC_NAME,WR.WRITE_OFF_USER_FK,";
            strSQL += "  WR.WRITE_OFF_USER_NAME,WR.WRITE_OFF_SEA_CR_DAYS,";
            strSQL += "  WR.WRITE_OFF_SEA_CURR_FK,WR.WRITE_OFF_AIR_CR_DAYS,";
            strSQL += "  WR.WRITE_OFF_AIR_CURR_FK,WR.SEA_BAL_AMT,";
            strSQL += "  WR.AIR_BAL_AMT,WR.APPLY_TO,WR.WRITE_OFF_MODE,NVL(WR.APPLY_TO_USERS,0),";
            strSQL += "  S_CMT.CURRENCY_ID CURR,";
            strSQL += "  A_CMT.CURRENCY_ID,";
            strSQL += "  WR.WRITE_OFF_SEA_LIMIT,";
            strSQL += "  WR.WRITE_OFF_AIR_LIMIT,";
            strSQL += "  WR.VALID_FROM,";
            strSQL += "  WR.VALID_TO,WR.WRITE_OFF_USER_FK,  NVL(WR.APPLY_TO_AGENTS,0) APPLY_TO_AGENTS , ";
            strSQL += "  NVL(WR.APPLY_TO_VENDORS,0) APPLY_TO_VENDORS ,";
            strSQL += "  NVL(WR.AGENTS_POLICY_TYPE,0) AGENTS_POLICY_TYPE,";
            strSQL += "  NVL(WR.VENDORS_POLICY_TYPE,0) VENDORS_POLICY_TYPE";
            strSQL += "  from WRITEOFF_MST_TBL WR,CURRENCY_TYPE_MST_TBL S_CMT,CURRENCY_TYPE_MST_TBL A_CMT";
            strSQL += "  where ";
            strSQL += "   WR.WRITE_OFF_USER_FK=" + UserPK;
            strSQL += "  AND S_CMT.CURRENCY_MST_PK(+)=WR.WRITE_OFF_SEA_CURR_FK";
            strSQL += "  AND A_CMT.CURRENCY_MST_PK(+)=WR.WRITE_OFF_AIR_CURR_FK";
            try
            {
                return objWF.GetDataTable(strSQL);
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

        #endregion "Fetch Function"

        #region "FetchLocation"

        /// <summary>
        /// Fetches the location.
        /// </summary>
        /// <param name="strLocPk">The string loc pk.</param>
        /// <param name="intUsrPk">The int usr pk.</param>
        /// <returns></returns>
        public DataSet FetchLocation(string strLocPk, int intUsrPk)
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            strSQL += "SELECT LOC.LOCATION_MST_PK,LOC.LOCATION_ID, ";
            strSQL += " LOC.LOCATION_NAME,USR.USER_NAME employee_name,usr.user_mst_pk employee_mst_pk";
            strSQL += " FROM LOCATION_MST_TBL LOC, USER_MST_TBL USR, employee_mst_tbl emp, WRITEOFF_MST_TBL WMT";
            strLocPk = (string.IsNullOrEmpty(strLocPk) ? "" : strLocPk);
            strSQL += " WHERE WMT.WRITE_OFF_USER_FK=USR.USER_MST_PK";
            strSQL += " and emp.employee_mst_pk = usr.employee_mst_fk";
            strSQL += " AND USR.DEFAULT_LOCATION_FK = LOC.LOCATION_MST_PK ";
            strSQL += " AND USR.USER_MST_PK =  " + intUsrPk + " ";
            try
            {
                return objWF.GetDataSet(strSQL);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception EX)
            {
                throw EX;
            }
        }

        #endregion "FetchLocation"

        #region "Save Function"

        /// <summary>
        /// Saves the specified ds.
        /// </summary>
        /// <param name="ds">The ds.</param>
        /// <returns></returns>
        public ArrayList Save(DataSet ds)
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction INSERT = null;
            OracleCommand Ins = new OracleCommand();
            objWK.OpenConnection();
            INSERT = objWK.MyConnection.BeginTransaction();
            try
            {
                var _with1 = Ins;
                _with1.Transaction = INSERT;
                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".WRITE_OFF_PKG.INS_WRITEOFF_SETUP";

                var _with2 = _with1.Parameters;
                _with2.Clear();
                //Ins.Parameters.Add("WRITE_OFF_PK_IN", ds.Tables(0).Rows(0).Item("WRITE_OFF_PK")).Direction = ParameterDirection.Input
                _with2.Add("WRITE_OFF_LOC_FK_IN", ds.Tables[0].Rows[0]["WRITE_OFF_LOC_FK"]).Direction = ParameterDirection.Input;
                _with2.Add("WRITE_OFF_LOC_NAME_IN", ds.Tables[0].Rows[0]["WRITE_OFF_LOC_NAME"]).Direction = ParameterDirection.Input;
                _with2.Add("WRITE_OFF_USER_FK_IN", ds.Tables[0].Rows[0]["WRITE_OFF_USER_FK"]).Direction = ParameterDirection.Input;
                _with2.Add("WRITE_OFF_USER_NAME_IN", ds.Tables[0].Rows[0]["WRITE_OFF_USER_NAME"]).Direction = ParameterDirection.Input;
                _with2.Add("WRITE_OFF_SEA_CR_DAYS_IN", getDefault(ds.Tables[0].Rows[0]["WRITE_OFF_SEA_CR_DAYS"], "")).Direction = ParameterDirection.Input;
                _with2.Add("WRITE_OFF_SEA_CURR_FK_IN", getDefault(ds.Tables[0].Rows[0]["WRITE_OFF_SEA_CURR_FK"], "")).Direction = ParameterDirection.Input;
                _with2.Add("WRITE_OFF_AIR_CR_DAYS_IN", getDefault(ds.Tables[0].Rows[0]["WRITE_OFF_AIR_CR_DAYS"], "")).Direction = ParameterDirection.Input;
                _with2.Add("WRITE_OFF_AIR_CURR_FK_IN", getDefault(ds.Tables[0].Rows[0]["WRITE_OFF_AIR_CURR_FK"], "")).Direction = ParameterDirection.Input;
                _with2.Add("SEA_BAL_AMT_IN", getDefault(ds.Tables[0].Rows[0]["SEA_BAL_AMT"], "")).Direction = ParameterDirection.Input;
                _with2.Add("AIR_BAL_AMT_IN", getDefault(ds.Tables[0].Rows[0]["AIR_BAL_AMT"], "")).Direction = ParameterDirection.Input;
                //'
                _with2.Add("WRITE_OFF_SEA_LIMIT_IN", getDefault(ds.Tables[0].Rows[0]["WRITE_OFF_SEA_LIMT"], "")).Direction = ParameterDirection.Input;
                _with2.Add("WRITE_OFF_AIR_LIMIT_IN", getDefault(ds.Tables[0].Rows[0]["WRITE_OFF_AIR_LIMT"], "")).Direction = ParameterDirection.Input;
                _with2.Add("VALID_FROM_IN", getDefault(ds.Tables[0].Rows[0]["VALID_FROM"], "")).Direction = ParameterDirection.Input;
                _with2.Add("VALID_TO_IN", getDefault(ds.Tables[0].Rows[0]["VALID_TO"], "")).Direction = ParameterDirection.Input;
                //'
                _with2.Add("APPLY_TO_IN", getDefault(ds.Tables[0].Rows[0]["APPLY_TO"], "")).Direction = ParameterDirection.Input;
                _with2.Add("AGENTS_POLICY_TYPE_IN", getDefault(ds.Tables[0].Rows[0]["AGENTS_POLICY_TYPE"], "")).Direction = ParameterDirection.Input;
                _with2.Add("VENDORS_POLICY_TYPE_IN", getDefault(ds.Tables[0].Rows[0]["VENDORS_POLICY_TYPE"], "")).Direction = ParameterDirection.Input;

                _with2.Add("WRITE_OFF_MODE_IN", ds.Tables[0].Rows[0]["WRITE_OFF_MODE"]).Direction = ParameterDirection.Input;
                _with2.Add("APPLY_TO_USERS_IN", getDefault(ds.Tables[0].Rows[0]["APPLY_TO_USERS"], 0)).Direction = ParameterDirection.Input;

                _with2.Add("APPLY_TO_AGENTS_IN", getDefault(ds.Tables[0].Rows[0]["APPLY_TO_AGENTS"], 0)).Direction = ParameterDirection.Input;
                _with2.Add("APPLY_TO_VENDORS_IN", getDefault(ds.Tables[0].Rows[0]["APPLY_TO_VENDORS"], 0)).Direction = ParameterDirection.Input;

                _with2.Add("CREATED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                _with2.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;

                _with1.ExecuteNonQuery();

                if (Convert.ToInt32(Ins.Parameters["RETURN_VALUE"].Value) > 0)
                {
                    UserPKValue = Convert.ToInt64(Ins.Parameters["WRITE_OFF_USER_FK_IN"].Value);
                    arrMessage = AutomaticWriteOff();
                    INSERT.Commit();
                    return arrMessage;
                }
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
                objWK.CloseConnection();
            }
            return new ArrayList();
        }

        /// <summary>
        /// Updates the specified ds.
        /// </summary>
        /// <param name="ds">The ds.</param>
        /// <returns></returns>
        public ArrayList Update(DataSet ds)
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction UPD = null;
            OracleCommand UP = new OracleCommand();
            objWK.OpenConnection();
            UPD = objWK.MyConnection.BeginTransaction();
            Int16 cnt = 0;
            try
            {
                var _with3 = UP;
                _with3.Transaction = UPD;
                _with3.Connection = objWK.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".WRITE_OFF_PKG.UP_WRITEOFF_SETUP";

                var _with4 = _with3.Parameters;
                _with4.Clear();
                _with4.Add("WRITE_OFF_PK_IN", ds.Tables[0].Rows[0]["WRITE_OFF_PK"]).Direction = ParameterDirection.Input;
                _with4.Add("WRITE_OFF_LOC_FK_IN", ds.Tables[0].Rows[0]["WRITE_OFF_LOC_FK"]).Direction = ParameterDirection.Input;
                _with4.Add("WRITE_OFF_LOC_NAME_IN", ds.Tables[0].Rows[0]["WRITE_OFF_LOC_NAME"]).Direction = ParameterDirection.Input;
                _with4.Add("WRITE_OFF_USER_FK_IN", ds.Tables[0].Rows[0]["WRITE_OFF_USER_FK"]).Direction = ParameterDirection.Input;
                _with4.Add("WRITE_OFF_USER_NAME_IN", ds.Tables[0].Rows[0]["WRITE_OFF_USER_NAME"]).Direction = ParameterDirection.Input;
                _with4.Add("WRITE_OFF_SEA_CR_DAYS_IN", ds.Tables[0].Rows[0]["WRITE_OFF_SEA_CR_DAYS"]).Direction = ParameterDirection.Input;
                _with4.Add("WRITE_OFF_SEA_CURR_FK_IN", ds.Tables[0].Rows[0]["WRITE_OFF_SEA_CURR_FK"]).Direction = ParameterDirection.Input;
                _with4.Add("WRITE_OFF_AIR_CR_DAYS_IN", ds.Tables[0].Rows[0]["WRITE_OFF_AIR_CR_DAYS"]).Direction = ParameterDirection.Input;
                _with4.Add("WRITE_OFF_AIR_CURR_FK_IN", ds.Tables[0].Rows[0]["WRITE_OFF_AIR_CURR_FK"]).Direction = ParameterDirection.Input;
                _with4.Add("SEA_BAL_AMT_IN", ds.Tables[0].Rows[0]["SEA_BAL_AMT"]).Direction = ParameterDirection.Input;
                _with4.Add("AIR_BAL_AMT_IN", ds.Tables[0].Rows[0]["AIR_BAL_AMT"]).Direction = ParameterDirection.Input;
                //'
                _with4.Add("WRITE_OFF_SEA_LIMIT_IN", getDefault(ds.Tables[0].Rows[0]["WRITE_OFF_SEA_LIMT"], "")).Direction = ParameterDirection.Input;
                _with4.Add("WRITE_OFF_AIR_LIMIT_IN", getDefault(ds.Tables[0].Rows[0]["WRITE_OFF_AIR_LIMT"], "")).Direction = ParameterDirection.Input;
                _with4.Add("VALID_FROM_IN", getDefault(ds.Tables[0].Rows[0]["VALID_FROM"], "")).Direction = ParameterDirection.Input;
                _with4.Add("VALID_TO_IN", getDefault(ds.Tables[0].Rows[0]["VALID_TO"], "")).Direction = ParameterDirection.Input;
                //'
                _with4.Add("APPLY_TO_IN", ds.Tables[0].Rows[0]["APPLY_TO"]).Direction = ParameterDirection.Input;

                _with4.Add("AGENTS_POLICY_TYPE_IN", ds.Tables[0].Rows[0]["AGENTS_POLICY_TYPE"]).Direction = ParameterDirection.Input;
                _with4.Add("VENDORS_POLICY_TYPE_IN", ds.Tables[0].Rows[0]["VENDORS_POLICY_TYPE"]).Direction = ParameterDirection.Input;

                _with4.Add("WRITE_OFF_MODE_IN", ds.Tables[0].Rows[0]["WRITE_OFF_MODE"]).Direction = ParameterDirection.Input;
                if (string.IsNullOrEmpty(ds.Tables[0].Rows[0]["APPLY_TO_USERS"].ToString()))
                {
                    _with4.Add("APPLY_TO_USERS_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with4.Add("APPLY_TO_USERS_IN", ds.Tables[0].Rows[0]["APPLY_TO_USERS"]).Direction = ParameterDirection.Input;
                }

                if (string.IsNullOrEmpty(ds.Tables[0].Rows[0]["APPLY_TO_AGENTS"].ToString()))
                {
                    _with4.Add("APPLY_TO_AGENTS_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with4.Add("APPLY_TO_AGENTS_IN", ds.Tables[0].Rows[0]["APPLY_TO_AGENTS"]).Direction = ParameterDirection.Input;
                }

                if (string.IsNullOrEmpty(ds.Tables[0].Rows[0]["APPLY_TO_VENDORS"].ToString()))
                {
                    _with4.Add("APPLY_TO_VENDORS_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with4.Add("APPLY_TO_VENDORS_IN", ds.Tables[0].Rows[0]["APPLY_TO_VENDORS"]).Direction = ParameterDirection.Input;
                }

                _with4.Add("LAST_MODIFIED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                cnt = Convert.ToInt16(UP.ExecuteNonQuery());

                if (cnt > 0)
                {
                    UserPKValue = Convert.ToInt64(UP.Parameters["WRITE_OFF_USER_FK_IN"].Value);
                    arrMessage = AutomaticWriteOff();
                    UPD.Commit();
                    return arrMessage;
                }
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
                objWK.CloseConnection();
            }
            return new ArrayList();
        }

        #endregion "Save Function"

        #region " Fetch Grid Data "

        /// <summary>
        /// Fetches the data.
        /// </summary>
        /// <param name="cust">The customer.</param>
        /// <param name="Customerpk">The customerpk.</param>
        /// <param name="usrLocFK">The usr loc fk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="process">The process.</param>
        /// <param name="fromDate">From date.</param>
        /// <param name="toDate">To date.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <returns></returns>
        public DataSet FetchData(string cust = "", long Customerpk = 0, long usrLocFK = 0, short BizType = 1, short process = 1, string fromDate = "", string toDate = "", Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {
            System.Text.StringBuilder strCondition = new System.Text.StringBuilder();
            System.Text.StringBuilder str = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string condition = null;
            string sqlA = null;
            Int32 Apply = default(Int32);
            Int32 TotalRecords = default(Int32);
            int BaseCurrFk = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);

            try
            {
                sqlA = " SELECT WMT.APPLY_TO FROM WRITEOFF_MST_TBL WMT ";
                Apply = Convert.ToInt32(objWF.ExecuteScaler(sqlA.ToString()));

                if (!((fromDate == null | string.IsNullOrEmpty(fromDate)) | (toDate == null | string.IsNullOrEmpty(toDate))))
                {
                    str.Append(" AND TO_DATE(INV.INVOICE_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "')  AND TO_DATE('" + toDate + "','" + dateFormat + "') ");
                }
                else if (!(fromDate == null | string.IsNullOrEmpty(fromDate)))
                {
                    str.Append(" AND TO_DATE(INV.INVOICE_DATE,'" + dateFormat + "') >= TO_DATE('" + fromDate + "',dateformat) ");
                }
                else if (!(toDate == null | string.IsNullOrEmpty(toDate)))
                {
                    str.Append(" AND TO_DATE(INV.INVOICE_DATE,'" + dateFormat + "') <= TO_DATE('" + toDate + "',dateformat) ");
                }
                condition = str.ToString();
                strCondition.Append("SELECT LOC.LOCATION_ID,");
                strCondition.Append("       INV.CONSOL_INVOICE_PK,");
                strCondition.Append("       INV.INVOICE_REF_NO,");
                strCondition.Append("       CMT.CUSTOMER_NAME,");
                strCondition.Append("       INV.INVOICE_DATE,");
                strCondition.Append("       NVL(INV.NET_RECEIVABLE,0)*GET_EX_RATE(INV.CURRENCY_MST_FK," + BaseCurrFk + ",INV.INVOICE_DATE) NET_RECEIVABLE,");

                strCondition.Append("       NVL((SELECT SUM(NVL(CTRN.RECD_AMOUNT_HDR_CURR, 0) * ");
                strCondition.Append("               GET_EX_RATE(CT.CURRENCY_MST_FK,");
                strCondition.Append("                          " + BaseCurrFk + ",");
                strCondition.Append("                           CT.COLLECTIONS_DATE))");
                strCondition.Append("      FROM COLLECTIONS_TRN_TBL CTRN, COLLECTIONS_TBL CT");
                strCondition.Append(" WHERE CTRN.INVOICE_REF_NR Like INV.INVOICE_REF_NO");
                strCondition.Append("       AND CT.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK),0.00)RECIEVED,");
                strCondition.Append("       MAX(NVL(WRM.WRITEOFF_AMOUNT,0)*");
                strCondition.Append("       GET_EX_RATE(INV.CURRENCY_MST_FK," + BaseCurrFk + ",INV.INVOICE_DATE)) WRITEOFF,");
                strCondition.Append("       ABS(NVL((NVL(INV.NET_RECEIVABLE,0)*GET_EX_RATE(INV.CURRENCY_MST_FK," + BaseCurrFk + ",INV.INVOICE_DATE) -");
                strCondition.Append("               NVL((SELECT SUM(NVL(WMT.WRITEOFF_AMOUNT,0)*");
                strCondition.Append("                       GET_EX_RATE(INV.CURRENCY_MST_FK," + BaseCurrFk + ",INV.INVOICE_DATE)) ");
                strCondition.Append("                      FROM WRITEOFF_MANUAL_TBL WMT");
                strCondition.Append("                     WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
                strCondition.Append("                    0.00) -");
                strCondition.Append("               NVL((SELECT SUM(NVL(CTRN.RECD_AMOUNT_HDR_CURR,0)*");
                strCondition.Append("                       GET_EX_RATE(CT.CURRENCY_MST_FK," + BaseCurrFk + ",CT.COLLECTIONS_DATE))");
                strCondition.Append("                      FROM COLLECTIONS_TRN_TBL CTRN, COLLECTIONS_TBL CT");
                strCondition.Append("                     WHERE CTRN.INVOICE_REF_NR LIKE INV.INVOICE_REF_NO");
                strCondition.Append("                       AND CT.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK),");
                strCondition.Append("                    0.00)),");
                strCondition.Append("               0)) BALANCE,");
                strCondition.Append("       DECODE(WRM.REMARKS, '0', '', '1', 'Excess Charge') REMARKS,");
                strCondition.Append("       UMT.USER_ID USERS,");
                strCondition.Append("       INV.BUSINESS_TYPE,");
                strCondition.Append("       INV.PROCESS_TYPE,");
                strCondition.Append("       INVTRN.JOB_CARD_FK ");

                strCondition.Append("  FROM COLLECTIONS_TRN_TBL    CTRN,");
                strCondition.Append("       CONSOL_INVOICE_TBL     INV,");
                strCondition.Append("       CONSOL_INVOICE_TRN_TBL INVTRN,");
                strCondition.Append("       WRITEOFF_MANUAL_TBL    WRM,");
                strCondition.Append("       LOCATION_MST_TBL       LOC,");

                if (Apply == 2)
                {
                    strCondition.Append(" WRITEOFF_MST_TBL WRM1, ");
                }

                strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition.Append(" USER_MST_TBL           UMT");
                strCondition.Append(" WHERE INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                strCondition.Append(" AND CTRN.INVOICE_REF_NR(+)  =INV.INVOICE_REF_NO");
                strCondition.Append("               " + condition);

                strCondition.Append(" AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
                strCondition.Append(" AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK + " ");
                strCondition.Append(" AND UMT.USER_MST_PK(+)=WRM.CREATED_BY");
                strCondition.Append(" AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK(+)");
                if (process == 1 | process == 2)
                {
                    strCondition.Append(" AND WRM.PROCESS ='" + process + "' ");
                }
                if (BizType == 1 | BizType == 2)
                {
                    strCondition.Append(" AND WRM.BIZTYPE ='" + BizType + "' ");
                }
                strCondition.Append(" AND WRM.CONSOL_INVOICE_FK=INV.CONSOL_INVOICE_PK(+)");
                strCondition.Append(" AND UMT.DEFAULT_LOCATION_FK = LOC.LOCATION_MST_PK");

                if (Apply == 2)
                {
                    strCondition.Append(" And WRM1.WRITE_OFF_LOC_FK = " + usrLocFK);
                    strCondition.Append(" And instr(',' || WRM1.apply_to_users || ',',',' || CMT.CUSTOMER_MST_PK || ',') > 0");
                }
                if (!string.IsNullOrEmpty(cust))
                {
                    strCondition.Append(" AND WRM.CREATED_BY=" + cust + " ");
                }
                if (Customerpk != 0)
                {
                    strCondition.Append(" AND CMT.CUSTOMER_MST_PK=" + Customerpk + " ");
                }
                strCondition.Append(" GROUP BY");
                strCondition.Append(" INV.CONSOL_INVOICE_PK,");
                strCondition.Append(" INV.INVOICE_REF_NO,");
                strCondition.Append(" INV.INVOICE_DATE ,");
                strCondition.Append(" INV.CURRENCY_MST_FK ,");
                strCondition.Append(" CUMT.CURRENCY_ID,");
                strCondition.Append(" CMT.CUSTOMER_NAME,");
                strCondition.Append(" INV.NET_RECEIVABLE,");
                strCondition.Append(" INV.CREATED_DT,");
                strCondition.Append(" WRM.WRITEOFF_AMOUNT,");
                strCondition.Append(" WRM.REMARKS,");
                strCondition.Append(" LOC.LOCATION_ID,");
                strCondition.Append(" UMT.USER_ID, ");
                strCondition.Append(" INV.BUSINESS_TYPE,");
                strCondition.Append(" INV.PROCESS_TYPE, ");
                strCondition.Append(" INVTRN.JOB_CARD_FK ");

                strCondition.Append(" ORDER BY INV.CREATED_DT DESC");

                System.Text.StringBuilder strCount = new System.Text.StringBuilder();
                strCount.Append(" SELECT COUNT(*)  from  ");
                strCount.Append((" (" + strCondition.ToString() + ")"));
                DataSet ds = null;
                ds = objWF.GetDataSet(strCount.ToString());
                TotalRecords = (Int32)ds.Tables[0].Rows[0][0];
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
                sqlstr2.Append(" SELECT * FROM ( ");
                sqlstr2.Append(" SELECT ROWNUM SLNO, Qry.* FROM ");
                sqlstr2.Append("  (" + strCondition.ToString() + " ");
                sqlstr2.Append("  ) Qry ) WHERE SLNO  Between " + start + " and " + last + "");

                return objWF.GetDataSet(sqlstr2.ToString());
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

        #endregion " Fetch Grid Data "

        #region "For Report"

        /// <summary>
        /// Fetches the report.
        /// </summary>
        /// <param name="cust">The customer.</param>
        /// <param name="usrLocFK">The usr loc fk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="process">The process.</param>
        /// <param name="fromDate">From date.</param>
        /// <param name="toDate">To date.</param>
        /// <returns></returns>
        public DataSet FetchReport(string cust = "", long usrLocFK = 0, short BizType = 1, short process = 1, string fromDate = "", string toDate = "")
        {
            System.Text.StringBuilder strCondition = new System.Text.StringBuilder();
            System.Text.StringBuilder str = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string condition = null;
            string sqlA = null;
            Int32 Apply = default(Int32);
            int BaseCurrFk = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);

            sqlA = "select WMT.APPLY_TO from writeoff_mst_tbl WMT";
            Apply = Convert.ToInt32(objWF.ExecuteScaler(sqlA.ToString()));

            if (!((fromDate == null | string.IsNullOrEmpty(fromDate)) | (toDate == null | string.IsNullOrEmpty(toDate))))
            {
                str.Append(" AND TO_DATE(wrm.writeoff_date,'" + dateFormat + "') BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "')  AND TO_DATE('" + toDate + "','" + dateFormat + "') ");
            }
            else if (!(fromDate == null | string.IsNullOrEmpty(fromDate)))
            {
                str.Append(" AND TO_DATE(wrm.writeoff_date,'" + dateFormat + "') >= TO_DATE('" + fromDate + "',dateformat) ");
            }
            else if (!(toDate == null | string.IsNullOrEmpty(toDate)))
            {
                str.Append(" AND TO_DATE(wrm.writeoff_date,'" + dateFormat + "') <= TO_DATE('" + toDate + "',dateformat) ");
            }
            condition = str.ToString();
            strCondition.Append(" SELECT  ");
            strCondition.Append(" INV.INVOICE_REF_NO,");
            strCondition.Append(" CMT.CUSTOMER_NAME, ");
            strCondition.Append(" INV.INVOICE_DATE,");
            strCondition.Append(" NVL(INV.NET_RECEIVABLE,0)*GET_EX_RATE(INV.CURRENCY_MST_FK," + BaseCurrFk + ",INV.INVOICE_DATE) NET_RECEIVABLE,");

            strCondition.Append("       NVL((SELECT SUM(NVL(CTRN.RECD_AMOUNT_HDR_CURR, 0) * ");
            strCondition.Append("               GET_EX_RATE(CT.CURRENCY_MST_FK,");
            strCondition.Append("                          " + BaseCurrFk + ",");
            strCondition.Append("                           CT.COLLECTIONS_DATE))");
            strCondition.Append("      FROM COLLECTIONS_TRN_TBL CTRN, COLLECTIONS_TBL CT");
            strCondition.Append(" WHERE CTRN.INVOICE_REF_NR Like INV.INVOICE_REF_NO");
            strCondition.Append("       AND CT.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK),0.00)RECIEVED,");

            strCondition.Append(" MAX(NVL(WRM.WRITEOFF_AMOUNT,0)*");
            strCondition.Append(" GET_EX_RATE(INV.CURRENCY_MST_FK," + BaseCurrFk + ",INV.INVOICE_DATE)) WRITEOFF,");
            //------------------------
            strCondition.Append("       ABS(NVL((NVL(INV.NET_RECEIVABLE,0)*GET_EX_RATE(INV.CURRENCY_MST_FK," + BaseCurrFk + ",INV.INVOICE_DATE) -");
            strCondition.Append("               NVL((SELECT SUM(NVL(WMT.WRITEOFF_AMOUNT,0)*");
            strCondition.Append("                       GET_EX_RATE(INV.CURRENCY_MST_FK," + BaseCurrFk + ",INV.INVOICE_DATE)) ");
            strCondition.Append("                      FROM WRITEOFF_MANUAL_TBL WMT");
            strCondition.Append("                     WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
            strCondition.Append("                    0.00) -");
            strCondition.Append("               NVL((SELECT SUM(NVL(CTRN.RECD_AMOUNT_HDR_CURR,0)*");
            strCondition.Append("                       GET_EX_RATE(CT.CURRENCY_MST_FK," + BaseCurrFk + ",CT.COLLECTIONS_DATE))");
            strCondition.Append("                      FROM COLLECTIONS_TRN_TBL CTRN, COLLECTIONS_TBL CT");
            strCondition.Append("                     WHERE CTRN.INVOICE_REF_NR LIKE INV.INVOICE_REF_NO");
            strCondition.Append("                       AND CT.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK),");
            strCondition.Append("                    0.00)),");
            strCondition.Append("               0)) BALANCE,");
            strCondition.Append("       DECODE(WRM.REMARKS, '0', '', '1', 'Excess Charge') REMARKS,");
            strCondition.Append("       UMT.USER_ID USERS");
            strCondition.Append("  FROM COLLECTIONS_TRN_TBL    CTRN,");
            strCondition.Append("       CONSOL_INVOICE_TBL     INV,");
            strCondition.Append("       CONSOL_INVOICE_TRN_TBL INVTRN,");
            strCondition.Append("       WRITEOFF_MANUAL_TBL    WRM,");

            if (Apply == 2)
            {
                strCondition.Append(" writeoff_mst_tbl WRM1, ");
            }

            strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
            strCondition.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
            strCondition.Append(" USER_MST_TBL           UMT");
            strCondition.Append(" WHERE INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
            strCondition.Append(" AND CTRN.INVOICE_REF_NR(+)  =INV.INVOICE_REF_NO");
            strCondition.Append("               " + condition);
            strCondition.Append(" AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
            strCondition.Append(" AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK + " ");
            strCondition.Append(" AND UMT.USER_MST_PK(+)=WRM.CREATED_BY");
            strCondition.Append(" AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK(+)");
            if (process == 1 | process == 2)
            {
                strCondition.Append(" AND WRM.PROCESS ='" + process + "' ");
            }
            if (BizType == 1 | BizType == 2)
            {
                strCondition.Append(" AND WRM.BIZTYPE ='" + BizType + "' ");
            }
            strCondition.Append(" AND wrm.consol_invoice_fk=INV.CONSOL_INVOICE_PK(+)");
            if (Apply == 2)
            {
                strCondition.Append(" And WRM1.WRITE_OFF_LOC_FK = " + usrLocFK);
                strCondition.Append(" And instr(',' || WRM1.apply_to_users || ',',',' || CMT.customer_mst_pk || ',') > 0");
            }
            if (!string.IsNullOrEmpty(cust))
            {
                strCondition.Append(" AND WRM.CREATED_BY=" + cust + " ");
            }
            strCondition.Append(" GROUP BY");
            strCondition.Append(" INV.CONSOL_INVOICE_PK,");
            strCondition.Append(" INV.INVOICE_REF_NO,");
            strCondition.Append(" INV.INVOICE_DATE ,");
            strCondition.Append(" INV.CURRENCY_MST_FK ,");
            strCondition.Append(" CUMT.CURRENCY_ID,CMT.CUSTOMER_NAME,");
            strCondition.Append(" INV.NET_RECEIVABLE,INV.CREATED_DT,WRM.WRITEOFF_AMOUNT,WRM.REMARKS, UMT.USER_ID  ORDER BY INV.CREATED_DT DESC");

            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append("SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + strCondition.ToString() + " ");
            sqlstr.Append("  ) T) QRY");
            string sql = null;
            sql = sqlstr.ToString();
            DataSet DS = null;
            try
            {
                return objWF.GetDataSet(sql);
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

        #endregion "For Report"

        #region "FOR WRITEOFF MANUAL"

        /// <summary>
        /// Fetches the list data.
        /// </summary>
        /// <param name="strInvRefNo">The string inv reference no.</param>
        /// <param name="strJobRefNo">The string job reference no.</param>
        /// <param name="strCustomer">The string customer.</param>
        /// <param name="strVessel">The string vessel.</param>
        /// <param name="SortColumn">The sort column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="SortType">Type of the sort.</param>
        /// <param name="usrLocFK">The usr loc fk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="process">The process.</param>
        /// <param name="Status">The status.</param>
        /// <param name="fromDate">From date.</param>
        /// <param name="toDate">To date.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="Party_Type">Type of the party_.</param>
        /// <param name="strVesselName">Name of the string vessel.</param>
        /// <returns></returns>
        public DataSet FetchListData(string strInvRefNo = "", string strJobRefNo = "", string strCustomer = "", string strVessel = "", string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " DESC ", long usrLocFK = 0, short BizType = 1,
        short process = 1, short Status = 1, string fromDate = "", string toDate = "", Int32 flag = 0, int Party_Type = 3, string strVesselName = "")
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            string condition = null;
            string condition1 = null;
            string sqlA = null;
            string ApplyStr = null;
            string BizText = null;
            string ProcessText = null;
            Int32 Apply = default(Int32);
            string ApplyUsersPK = null;
            ApplyStr = "select nvl(j.apply_to_users,0) from writeoff_mst_tbl j where j.write_off_loc_fk = " + usrLocFK + "and j.write_off_user_fk = " + HttpContext.Current.Session["USER_PK"];
            ApplyUsersPK = Convert.ToString(objWF.ExecuteScaler(ApplyStr.ToString()));
            if (ApplyUsersPK == null)
            {
                ApplyUsersPK = "0";
            }
            if (BizType == 1)
            {
                BizText = "Air";
            }
            else if (BizType == 2)
            {
                BizText = "Sea";
            }
            if (process == 1)
            {
                ProcessText = "Export";
            }
            else if (process == 2)
            {
                ProcessText = "Import";
            }
            sqlA = "select WMT.APPLY_TO from writeoff_mst_tbl WMT  WHERE  WMT.WRITE_OFF_USER_FK =" + HttpContext.Current.Session["USER_PK"] + "";
            Apply = Convert.ToInt32(objWF.ExecuteScaler(sqlA.ToString()));

            //'*************CUSTOMER**********
            if (BizType == 1)
            {
                sb.Append(" SELECT MAX(VJC.PARTY_NAME) CUSTOMER_NAME,");
                sb.Append(" DECODE(VJC.BUSINESS_TYPE, 1, 'Air', 2, 'Sea') BIZTYPE,");
                sb.Append(" DECODE(VJC.PROCESS_TYPE, 1, 'Export', 2, 'Import') PROCESS,");
                sb.Append(" '' LOCATION,");
                sb.Append(" MAX(VJC.CONSOL_INVOICE_PK) PK,");
                sb.Append(" VJC.INVOICE_REF_NO,");
                sb.Append(" MAX(VJC.INVOICE_DATE) INVOICE_DATE,");
                sb.Append(" MAX(VJC.NET_INV_AMT) NET_RECEIVABLE,");
                sb.Append(" (SELECT NVL(sum(c.recd_amount_hdr_curr), 0) ");
                sb.Append("                       FROM ");
                sb.Append("                             collections_trn_tbl c ");
                sb.Append("                       WHERE c.invoice_ref_nr=VJC.INVOICE_REF_NO)");
                sb.Append("                       ");
                sb.Append("                       Recieved,");
                sb.Append(" NVL((max(VJC.NET_INV_AMT) - ");
                sb.Append("   (SELECT NVL(sum(c.recd_amount_hdr_curr), 0) ");
                sb.Append("   FROM collections_trn_tbl c ");
                sb.Append("   WHERE c.invoice_ref_nr=VJC.INVOICE_REF_NO)");
                sb.Append(" -NVL(max(VJC.WRITEOFF_AMOUNT), '0.00')),'0.00') Balance,");
                sb.Append("  (TO_DATE(SYSDATE,'DD/MM/YYYY')-TO_DATE(VJC.INVOICE_DATE))OutStandingDays,");

                //'Pending
                if (Status == 1)
                {
                    sb.Append(" 0 amount,'' Idate,");
                    //'History
                }
                else
                {
                    sb.Append(" NVL(max(VJC.WRITEOFF_AMOUNT),'0.00') amount,NVL(max(TO_DATE(VJC.WRITEOFF_DATE,'DD/MM/YYYY')), '') Idate,");
                }
                sb.Append(" max(VJC.REMARKS) REMARKS,0 PARTY_TYPE,'' Sel");
                sb.Append(" FROM");
                sb.Append(" VIEW_WRITE_OFF_JOBCARD_POPUP VJC, CUSTOMER_MST_TBL CMT ");
                if (Apply == 2)
                {
                    sb.Append(" , WRITEOFF_MST_TBL WMT");
                }

                sb.Append("     WHERE VJC.PARTY_PK= CMT.CUSTOMER_MST_PK  /* AND VJC.PK IS NOT NULL */  AND VJC.PARTY_TYPE=0  AND VJC.CREATED_BY_FK IN");
                sb.Append("     (SELECT UMT.USER_MST_PK");
                sb.Append("     FROM USER_MST_TBL UMT");
                sb.Append("     WHERE UMT.DEFAULT_LOCATION_FK =");
                sb.Append("     (SELECT U.DEFAULT_LOCATION_FK");
                sb.Append("     FROM USER_MST_TBL U");
                sb.Append("     WHERE U.USER_MST_PK =" + HttpContext.Current.Session["USER_PK"] + "))");
                if (Apply == 2)
                {
                    sb.Append(" And instr(','||wmt.apply_to_users||',',','||CMT.customer_mst_pk||',') > 0");
                }
                if (!(fromDate == null | string.IsNullOrEmpty(fromDate)))
                {
                    sb.Append(" AND VJC.invoice_date >= TO_DATE('" + fromDate + "',dateformat) ");
                }
                else if (!(toDate == null | string.IsNullOrEmpty(toDate)))
                {
                    sb.Append(" AND VJC.invoice_date <= TO_DATE('" + toDate + "',dateformat) ");
                }
                sb.Append(" and ((select floor(sysdate-vjc.INVOICE_DATE) from dual) >= ");
                sb.Append("(select w.write_off_air_cr_days from writeoff_mst_tbl");
                sb.Append(" w where (w.apply_to_users = '0' or cmt.customer_mst_pk");
                sb.Append(" in (" + ApplyUsersPK + ")) ");

                if (usrLocFK > 0)
                {
                    sb.Append(" and w.write_off_loc_fk = " + usrLocFK + " ");
                }
                sb.Append(" and w.write_off_user_fk = " + HttpContext.Current.Session["USER_PK"] + " ");

                //sb.Append(" )) and NVL((vjc.NET_INV_AMT -NVL((SELECT NVL(sum(c.recd_amount_hdr_curr), 0)")
                //sb.Append(" FROM collections_trn_tbl c ")
                //sb.Append(" WHERE c.invoice_ref_nr = vjc.INVOICE_REF_NO), 0.00)),0)")
                //sb.Append(" <=(select w.air_bal_amt from writeoff_mst_tbl")
                //sb.Append(" w where (w.apply_to_users = '0' or cmt.customer_mst_pk")
                //sb.Append(" in (" & ApplyUsersPK & "))  and w.write_off_user_fk = " & HttpContext.Current.Session("USER_PK") & "")
                //If usrLocFK > 0 Then
                //    sb.Append(" and w.write_off_loc_fk = " & usrLocFK & " ")
                //End If
                //sb.Append(" ) AND VJC.BUSINESS_TYPE=1 ")
                sb.Append(" )) AND VJC.BUSINESS_TYPE=1 ");
                if (!string.IsNullOrEmpty(strJobRefNo))
                {
                    sb.Append(" AND VJC.REF_NO='" + strJobRefNo + "'");
                }

                if (!string.IsNullOrEmpty(strCustomer))
                {
                    sb.Append(" AND CMT.CUSTOMER_MST_PK IN (" + strCustomer + ")");
                }

                if (!string.IsNullOrEmpty(strInvRefNo))
                {
                    sb.Append(" AND VJC.INVOICE_REF_NO = '" + strInvRefNo + "' ");
                }

                if (!string.IsNullOrEmpty(strVessel))
                {
                    sb.Append(" AND VJC.VOYAGE_NO LIKE '%" + strVessel.Trim() + "%'");
                }

                if (!string.IsNullOrEmpty(strVesselName))
                {
                    sb.Append(" AND VJC.VSL LIKE '%" + strVesselName.Trim() + "%'");
                }

                sb.Append(" GROUP BY");
                sb.Append(" VJC.INVOICE_REF_NO, VJC.process_type, VJC.business_type,VJC.INVOICE_DATE");
            }
            else if (BizType == 2)
            {
                sb.Append(" SELECT MAX(VJC.PARTY_NAME) CUSTOMER_NAME,");
                sb.Append(" DECODE(VJC.BUSINESS_TYPE, 1, 'Air', 2, 'Sea') BIZTYPE,");
                sb.Append(" DECODE(VJC.PROCESS_TYPE, 1, 'Export', 2, 'Import') PROCESS,");
                sb.Append(" '' LOCATION,");
                sb.Append(" MAX(VJC.CONSOL_INVOICE_PK) PK,");
                sb.Append(" VJC.INVOICE_REF_NO,");
                sb.Append(" MAX(VJC.INVOICE_DATE) INVOICE_DATE,");
                sb.Append(" MAX(VJC.NET_INV_AMT) NET_RECEIVABLE,");
                sb.Append(" (SELECT NVL(sum(c.recd_amount_hdr_curr), 0) ");
                sb.Append("                       FROM ");
                sb.Append("                             collections_trn_tbl c ");
                sb.Append("                       WHERE c.invoice_ref_nr=VJC.INVOICE_REF_NO)");
                sb.Append("                       ");
                sb.Append("                       Recieved,");
                sb.Append(" NVL((max(VJC.NET_INV_AMT) - ");
                sb.Append("   (SELECT NVL(sum(c.recd_amount_hdr_curr), 0) ");
                sb.Append("   FROM collections_trn_tbl c ");
                sb.Append("   WHERE c.invoice_ref_nr=VJC.INVOICE_REF_NO)");

                sb.Append(" -NVL(max(VJC.WRITEOFF_AMOUNT), '0.00')),'0.00') Balance,");
                sb.Append("  (TO_DATE(SYSDATE,'DD/MM/YYYY')-TO_DATE(VJC.INVOICE_DATE))OutStandingDays,");

                //'Pending
                if (Status == 1)
                {
                    sb.Append(" 0 amount,'' Idate,");
                    //'History
                }
                else
                {
                    sb.Append(" NVL(max(VJC.WRITEOFF_AMOUNT),'0.00') amount,NVL(max(TO_DATE(VJC.WRITEOFF_DATE,'DD/MM/YYYY')), '') Idate,");
                }
                sb.Append(" max(VJC.REMARKS) REMARKS,0 PARTY_TYPE,'' Sel");
                sb.Append(" FROM");
                sb.Append(" VIEW_WRITE_OFF_JOBCARD_POPUP VJC, CUSTOMER_MST_TBL CMT ");
                if (Apply == 2)
                {
                    sb.Append(" , WRITEOFF_MST_TBL WMT");
                }

                sb.Append("     WHERE VJC.PARTY_PK= CMT.CUSTOMER_MST_PK  /* AND VJC.PK IS NOT NULL */  AND VJC.PARTY_TYPE=0  AND VJC.CREATED_BY_FK IN");
                sb.Append("     (SELECT UMT.USER_MST_PK");
                sb.Append("     FROM USER_MST_TBL UMT");
                sb.Append("     WHERE UMT.DEFAULT_LOCATION_FK =");
                sb.Append("     (SELECT U.DEFAULT_LOCATION_FK");
                sb.Append("     FROM USER_MST_TBL U");
                sb.Append("     WHERE U.USER_MST_PK =" + HttpContext.Current.Session["USER_PK"] + "))");
                if (Apply == 2)
                {
                    sb.Append(" And instr(','||wmt.apply_to_users||',',','||CMT.customer_mst_pk||',') > 0");
                }
                if (!(fromDate == null | string.IsNullOrEmpty(fromDate)))
                {
                    sb.Append(" AND VJC.invoice_date >= TO_DATE('" + fromDate + "',dateformat) ");
                }
                else if (!(toDate == null | string.IsNullOrEmpty(toDate)))
                {
                    sb.Append(" AND VJC.invoice_date <= TO_DATE('" + toDate + "',dateformat) ");
                }
                //sb.Append(" and NVL((vjc.NET_INV_AMT -NVL((SELECT NVL(sum(c.recd_amount_hdr_curr), 0)")
                //sb.Append(" FROM collections_trn_tbl c ")
                //sb.Append(" WHERE c.invoice_ref_nr = vjc.INVOICE_REF_NO), 0.00)),0)")
                //sb.Append(" <=(select w.sea_bal_amt from writeoff_mst_tbl w where")
                //sb.Append(" (w.apply_to_users = '0' or cmt.customer_mst_pk")
                //sb.Append(" in (" & ApplyUsersPK & "))  and w.write_off_user_fk = " & HttpContext.Current.Session("USER_PK") & "")

                //If usrLocFK > 0 Then
                //    sb.Append(" and w.write_off_loc_fk = " & usrLocFK & " ")
                //End If
                //sb.Append(" ) and ((select floor(sysdate - vjc.INVOICE_DATE) from dual) >=")
                sb.Append(" and ((select floor(sysdate - vjc.INVOICE_DATE) from dual) >=");
                sb.Append(" (select w.write_off_sea_cr_days");
                sb.Append(" from writeoff_mst_tbl w");
                sb.Append(" where (w.apply_to_users = '0' or");
                sb.Append(" cmt.customer_mst_pk in (" + ApplyUsersPK + "))");
                sb.Append(" and w.write_off_user_fk = " + HttpContext.Current.Session["USER_PK"] + "");
                if (usrLocFK > 0)
                {
                    sb.Append(" and w.write_off_loc_fk = " + usrLocFK + " ");
                }
                sb.Append(" )) AND VJC.BUSINESS_TYPE=2 ");
                if (!string.IsNullOrEmpty(strJobRefNo))
                {
                    sb.Append(" AND VJC.REF_NO='" + strJobRefNo + "'");
                }

                if (!string.IsNullOrEmpty(strCustomer))
                {
                    sb.Append(" AND CMT.CUSTOMER_MST_PK IN (" + strCustomer + ")");
                }

                if (!string.IsNullOrEmpty(strInvRefNo))
                {
                    sb.Append(" AND VJC.INVOICE_REF_NO = '" + strInvRefNo + "' ");
                }
                if (!string.IsNullOrEmpty(strVessel))
                {
                    sb.Append(" AND VJC.VOYAGE_NO LIKE '%" + strVessel.Trim() + "%'");
                }

                if (!string.IsNullOrEmpty(strVesselName))
                {
                    sb.Append(" AND VJC.VSL LIKE '%" + strVesselName.Trim() + "%'");
                }

                sb.Append(" GROUP BY");
                sb.Append(" VJC.INVOICE_REF_NO, VJC.process_type, VJC.business_type,VJC.INVOICE_DATE");
            }
            else
            {
                //Air
                sb.Append(" SELECT MAX(VJC.PARTY_NAME) CUSTOMER_NAME,");
                sb.Append(" DECODE(VJC.BUSINESS_TYPE, 1, 'Air', 2, 'Sea') BIZTYPE,");
                sb.Append(" DECODE(VJC.PROCESS_TYPE, 1, 'Export', 2, 'Import') PROCESS,");
                sb.Append(" '' LOCATION,");
                sb.Append(" MAX(VJC.CONSOL_INVOICE_PK) PK,");
                sb.Append(" VJC.INVOICE_REF_NO,");
                sb.Append(" MAX(VJC.INVOICE_DATE) INVOICE_DATE,");
                sb.Append(" MAX(VJC.NET_INV_AMT) NET_RECEIVABLE,");
                sb.Append(" (SELECT NVL(sum(c.recd_amount_hdr_curr), 0) ");
                sb.Append("                       FROM ");
                sb.Append("                             collections_trn_tbl c ");
                sb.Append("                       WHERE c.invoice_ref_nr=VJC.INVOICE_REF_NO)");
                sb.Append("                       ");
                sb.Append("                       Recieved,");
                sb.Append(" NVL((max(VJC.NET_INV_AMT) - ");
                sb.Append("   (SELECT NVL(sum(c.recd_amount_hdr_curr), 0) ");
                sb.Append("   FROM collections_trn_tbl c ");
                sb.Append("   WHERE c.invoice_ref_nr=VJC.INVOICE_REF_NO)");

                sb.Append(" -NVL(max(VJC.WRITEOFF_AMOUNT), '0.00')),'0.00') Balance,");
                sb.Append("  (TO_DATE(SYSDATE,'DD/MM/YYYY')-TO_DATE(VJC.INVOICE_DATE))OutStandingDays,");

                //'Pending
                if (Status == 1)
                {
                    sb.Append(" 0 amount,'' Idate,");
                    //'History
                }
                else
                {
                    sb.Append(" NVL(max(VJC.WRITEOFF_AMOUNT),'0.00') amount,NVL(max(TO_DATE(VJC.WRITEOFF_DATE,'DD/MM/YYYY')), '') Idate,");
                }
                sb.Append(" max(VJC.REMARKS) REMARKS,0 PARTY_TYPE,'' Sel");
                sb.Append(" FROM");
                sb.Append(" VIEW_WRITE_OFF_JOBCARD_POPUP VJC, CUSTOMER_MST_TBL CMT ");
                if (Apply == 2)
                {
                    sb.Append(" , WRITEOFF_MST_TBL WMT");
                }

                sb.Append("     WHERE VJC.PARTY_PK= CMT.CUSTOMER_MST_PK  /*  AND VJC.PK IS NOT NULL */  AND VJC.PARTY_TYPE=0  AND VJC.CREATED_BY_FK IN");
                sb.Append("     (SELECT UMT.USER_MST_PK");
                sb.Append("     FROM USER_MST_TBL UMT");
                sb.Append("     WHERE UMT.DEFAULT_LOCATION_FK =");
                sb.Append("     (SELECT U.DEFAULT_LOCATION_FK");
                sb.Append("     FROM USER_MST_TBL U");
                sb.Append("     WHERE U.USER_MST_PK =" + HttpContext.Current.Session["USER_PK"] + "))");
                if (Apply == 2)
                {
                    sb.Append(" And instr(','||wmt.apply_to_users||',',','||CMT.customer_mst_pk||',') > 0");
                }
                if (!(fromDate == null | string.IsNullOrEmpty(fromDate)))
                {
                    sb.Append(" AND VJC.invoice_date >= TO_DATE('" + fromDate + "',dateformat) ");
                }
                else if (!(toDate == null | string.IsNullOrEmpty(toDate)))
                {
                    sb.Append(" AND VJC.invoice_date <= TO_DATE('" + toDate + "',dateformat) ");
                }
                sb.Append(" and ((select floor(sysdate-vjc.INVOICE_DATE) from dual) >= ");
                sb.Append("(select w.write_off_air_cr_days from writeoff_mst_tbl");
                sb.Append(" w where (w.apply_to_users = '0' or cmt.customer_mst_pk");
                sb.Append(" in (" + ApplyUsersPK + "))  ");
                sb.Append("   and w.write_off_user_fk = " + HttpContext.Current.Session["USER_PK"] + "");
                if (usrLocFK > 0)
                {
                    sb.Append("  and w.write_off_loc_fk = " + usrLocFK + " ");
                }
                //sb.Append(" )) and NVL((vjc.NET_INV_AMT -NVL((SELECT NVL(sum(c.recd_amount_hdr_curr), 0)")
                //sb.Append(" FROM collections_trn_tbl c ")
                //sb.Append(" WHERE c.invoice_ref_nr = vjc.INVOICE_REF_NO), 0.00)),0)")
                //sb.Append(" <=(select w.air_bal_amt from writeoff_mst_tbl")
                //sb.Append(" w where (w.apply_to_users = '0' or cmt.customer_mst_pk")
                //sb.Append(" in (" & ApplyUsersPK & "))  and w.write_off_user_fk = " & HttpContext.Current.Session("USER_PK") & "")
                //If usrLocFK > 0 Then
                //    sb.Append(" and w.write_off_loc_fk = " & usrLocFK & " ")
                //End If
                //sb.Append(" ) AND VJC.BUSINESS_TYPE=1 ")
                sb.Append(" )) AND VJC.BUSINESS_TYPE=1 ");
                if (!string.IsNullOrEmpty(strJobRefNo))
                {
                    sb.Append(" AND VJC.REF_NO='" + strJobRefNo + "'");
                }

                if (!string.IsNullOrEmpty(strCustomer))
                {
                    sb.Append(" AND CMT.CUSTOMER_MST_PK IN (" + strCustomer + ")");
                }

                if (!string.IsNullOrEmpty(strInvRefNo))
                {
                    sb.Append(" AND VJC.INVOICE_REF_NO = '" + strInvRefNo + "' ");
                }
                if (!string.IsNullOrEmpty(strVessel))
                {
                    sb.Append(" AND VJC.VOYAGE_NO LIKE '%" + strVessel.Trim() + "%'");
                }

                if (!string.IsNullOrEmpty(strVesselName))
                {
                    sb.Append(" AND VJC.VSL LIKE '%" + strVesselName.Trim() + "%'");
                }

                sb.Append(" GROUP BY");
                sb.Append(" VJC.INVOICE_REF_NO, VJC.process_type, VJC.business_type,VJC.INVOICE_DATE ");
                //Sea
                sb.Append(" UNION SELECT MAX(VJC.PARTY_NAME) CUSTOMER_NAME,");
                sb.Append(" DECODE(VJC.BUSINESS_TYPE, 1, 'Air', 2, 'Sea') BIZTYPE,");
                sb.Append(" DECODE(VJC.PROCESS_TYPE, 1, 'Export', 2, 'Import') PROCESS,");
                sb.Append(" '' LOCATION,");
                sb.Append(" MAX(VJC.CONSOL_INVOICE_PK) PK,");
                sb.Append(" VJC.INVOICE_REF_NO,");
                sb.Append(" MAX(VJC.INVOICE_DATE) INVOICE_DATE,");
                sb.Append(" MAX(VJC.NET_INV_AMT) NET_RECEIVABLE,");
                sb.Append(" (SELECT NVL(sum(c.recd_amount_hdr_curr), 0) ");
                sb.Append("                       FROM ");
                sb.Append("                             collections_trn_tbl c ");
                sb.Append("                       WHERE c.invoice_ref_nr=VJC.INVOICE_REF_NO)");
                sb.Append("                       ");
                sb.Append("                       Recieved,");
                sb.Append(" NVL((max(VJC.NET_INV_AMT) - ");
                sb.Append("   (SELECT NVL(sum(c.recd_amount_hdr_curr), 0) ");
                sb.Append("   FROM collections_trn_tbl c ");
                sb.Append("   WHERE c.invoice_ref_nr=VJC.INVOICE_REF_NO)");

                sb.Append(" -NVL(max(VJC.WRITEOFF_AMOUNT), '0.00')),'0.00') Balance,");
                sb.Append("  (TO_DATE(SYSDATE,'DD/MM/YYYY')-TO_DATE(VJC.INVOICE_DATE))OutStandingDays,");

                //'Pending
                if (Status == 1)
                {
                    sb.Append(" 0 amount,'' Idate,");
                    //'History
                }
                else
                {
                    sb.Append(" NVL(max(VJC.WRITEOFF_AMOUNT),'0.00') amount,NVL(max(TO_DATE(VJC.WRITEOFF_DATE,'DD/MM/YYYY')), '') Idate,");
                }
                sb.Append(" max(VJC.REMARKS) REMARKS,0 PARTY_TYPE,'' Sel");
                sb.Append(" FROM");
                sb.Append(" VIEW_WRITE_OFF_JOBCARD_POPUP VJC, CUSTOMER_MST_TBL CMT ");
                if (Apply == 2)
                {
                    sb.Append(" , WRITEOFF_MST_TBL WMT");
                }

                sb.Append("     WHERE VJC.PARTY_PK= CMT.CUSTOMER_MST_PK   /* AND VJC.PK IS NOT NULL */  AND VJC.PARTY_TYPE=0  AND VJC.CREATED_BY_FK IN");
                sb.Append("     (SELECT UMT.USER_MST_PK");
                sb.Append("     FROM USER_MST_TBL UMT");
                sb.Append("     WHERE UMT.DEFAULT_LOCATION_FK =");
                sb.Append("     (SELECT U.DEFAULT_LOCATION_FK");
                sb.Append("     FROM USER_MST_TBL U");
                sb.Append("     WHERE U.USER_MST_PK =" + HttpContext.Current.Session["USER_PK"] + "))");
                if (Apply == 2)
                {
                    sb.Append(" And instr(','||wmt.apply_to_users||',',','||CMT.customer_mst_pk||',') > 0");
                }
                if (!(fromDate == null | string.IsNullOrEmpty(fromDate)))
                {
                    sb.Append(" AND VJC.invoice_date >= TO_DATE('" + fromDate + "',dateformat) ");
                }
                else if (!(toDate == null | string.IsNullOrEmpty(toDate)))
                {
                    sb.Append(" AND VJC.invoice_date <= TO_DATE('" + toDate + "',dateformat) ");
                }
                //sb.Append(" and NVL((vjc.NET_INV_AMT -NVL((SELECT NVL(sum(c.recd_amount_hdr_curr), 0)")
                //sb.Append(" FROM collections_trn_tbl c ")
                //sb.Append(" WHERE c.invoice_ref_nr = vjc.INVOICE_REF_NO), 0.00)),0)")
                //sb.Append(" <=(select w.sea_bal_amt from writeoff_mst_tbl w where")
                //sb.Append(" (w.apply_to_users = '0' or cmt.customer_mst_pk")
                //sb.Append(" in (" & ApplyUsersPK & "))  and w.write_off_user_fk = " & HttpContext.Current.Session("USER_PK") & "")
                //If usrLocFK > 0 Then
                //    sb.Append(" and w.write_off_loc_fk = " & usrLocFK & " ")
                //End If
                //sb.Append(" ) and ((select floor(sysdate - vjc.INVOICE_DATE) from dual) >=")
                sb.Append(" and ((select floor(sysdate - vjc.INVOICE_DATE) from dual) >=");
                sb.Append(" (select w.write_off_sea_cr_days");
                sb.Append(" from writeoff_mst_tbl w");
                sb.Append(" where (w.apply_to_users = '0' or");
                sb.Append(" cmt.customer_mst_pk in (" + ApplyUsersPK + "))");
                sb.Append(" and w.write_off_user_fk = " + HttpContext.Current.Session["USER_PK"] + "");
                if (usrLocFK > 0)
                {
                    sb.Append(" and w.write_off_loc_fk = " + usrLocFK + " ");
                }
                sb.Append(" )) AND VJC.BUSINESS_TYPE=2 ");
                if (!string.IsNullOrEmpty(strJobRefNo))
                {
                    sb.Append(" AND VJC.REF_NO='" + strJobRefNo + "'");
                }

                if (!string.IsNullOrEmpty(strCustomer))
                {
                    sb.Append(" AND CMT.CUSTOMER_MST_PK IN (" + strCustomer + ")");
                }

                if (!string.IsNullOrEmpty(strInvRefNo))
                {
                    sb.Append(" AND VJC.INVOICE_REF_NO = '" + strInvRefNo + "' ");
                }
                if (!string.IsNullOrEmpty(strVessel))
                {
                    sb.Append(" AND VJC.VOYAGE_NO LIKE '%" + strVessel.Trim() + "%'");
                }

                if (!string.IsNullOrEmpty(strVesselName))
                {
                    sb.Append(" AND VJC.VSL LIKE '%" + strVesselName.Trim() + "%'");
                }

                sb.Append(" GROUP BY");
                sb.Append(" VJC.INVOICE_REF_NO, VJC.process_type, VJC.business_type,VJC.INVOICE_DATE");
            }
            //*************AGENT*****************
            if (BizType == 1)
            {
                sb.Append(" UNION SELECT MAX(VJC.PARTY_NAME) CUSTOMER_NAME,");
                sb.Append(" DECODE(VJC.BUSINESS_TYPE, 1, 'Air', 2, 'Sea') BIZTYPE,");
                sb.Append(" DECODE(VJC.PROCESS_TYPE, 1, 'Export', 2, 'Import') PROCESS,");
                sb.Append(" '' LOCATION,");
                sb.Append(" MAX(VJC.CONSOL_INVOICE_PK) PK,");
                sb.Append(" VJC.INVOICE_REF_NO,");
                sb.Append(" MAX(VJC.INVOICE_DATE) INVOICE_DATE,");
                sb.Append(" MAX(VJC.NET_INV_AMT) NET_RECEIVABLE,");
                sb.Append(" (SELECT NVL(sum(c.recd_amount_hdr_curr), 0) ");
                sb.Append("                       FROM ");
                sb.Append("                             collections_trn_tbl c ");
                sb.Append("                       WHERE c.invoice_ref_nr=VJC.INVOICE_REF_NO)");
                sb.Append("                       ");
                sb.Append("                       Recieved,");
                sb.Append(" NVL((max(VJC.NET_INV_AMT) - ");
                sb.Append("   (SELECT NVL(sum(c.recd_amount_hdr_curr), 0) ");
                sb.Append("   FROM collections_trn_tbl c ");
                sb.Append("   WHERE c.invoice_ref_nr=VJC.INVOICE_REF_NO)");
                sb.Append(" -NVL(max(VJC.WRITEOFF_AMOUNT), '0.00')),'0.00') Balance,");
                sb.Append("  (TO_DATE(SYSDATE,'DD/MM/YYYY')-TO_DATE(VJC.INVOICE_DATE))OutStandingDays,");

                //'Pending
                if (Status == 1)
                {
                    sb.Append(" 0 amount,'' Idate,");
                    //'History
                }
                else
                {
                    sb.Append(" NVL(max(VJC.WRITEOFF_AMOUNT),'0.00') amount,NVL(max(TO_DATE(VJC.WRITEOFF_DATE,'DD/MM/YYYY')), '') Idate,");
                }
                sb.Append(" max(VJC.REMARKS) REMARKS,1 PARTY_TYPE,'' Sel");
                sb.Append(" FROM");

                sb.Append(" VIEW_WRITE_OFF_JOBCARD_POPUP VJC, AGENT_MST_TBL AMT ");
                if (Apply == 2)
                {
                    sb.Append(" , WRITEOFF_MST_TBL WMT");
                }

                sb.Append("     WHERE VJC.PARTY_PK= AMT.AGENT_MST_PK   /* AND VJC.PK IS NOT NULL*/ AND VJC.PARTY_TYPE=1  AND VJC.CREATED_BY_FK IN");
                sb.Append("     (SELECT UMT.USER_MST_PK");
                sb.Append("     FROM USER_MST_TBL UMT");
                sb.Append("     WHERE UMT.DEFAULT_LOCATION_FK =");
                sb.Append("     (SELECT U.DEFAULT_LOCATION_FK");
                sb.Append("     FROM USER_MST_TBL U");
                sb.Append("     WHERE U.USER_MST_PK =" + HttpContext.Current.Session["USER_PK"] + "))");
                if (Apply == 2)
                {
                    sb.Append(" And instr(','||WMT.APPLY_TO_AGENTS||',',','||AMT.AGENT_MST_PK||',') > 0");
                }
                if (!(fromDate == null | string.IsNullOrEmpty(fromDate)))
                {
                    sb.Append(" AND VJC.invoice_date >= TO_DATE('" + fromDate + "',dateformat) ");
                }
                else if (!(toDate == null | string.IsNullOrEmpty(toDate)))
                {
                    sb.Append(" AND VJC.invoice_date <= TO_DATE('" + toDate + "',dateformat) ");
                }
                sb.Append(" and ((select floor(sysdate-vjc.INVOICE_DATE) from dual) >= ");
                sb.Append("(select w.write_off_air_cr_days from writeoff_mst_tbl");
                sb.Append(" w where (w.APPLY_TO_AGENTS = '0' or AMT.AGENT_MST_PK");
                sb.Append(" in (" + ApplyUsersPK + "))  ");
                sb.Append(" and w.write_off_user_fk = " + HttpContext.Current.Session["USER_PK"] + "");
                if (usrLocFK > 0)
                {
                    sb.Append(" and w.write_off_loc_fk = " + usrLocFK + " ");
                }
                //sb.Append(" )) and NVL((vjc.NET_INV_AMT -NVL((SELECT NVL(sum(c.recd_amount_hdr_curr), 0)")
                //sb.Append(" FROM collections_trn_tbl c ")
                //sb.Append(" WHERE c.invoice_ref_nr = vjc.INVOICE_REF_NO), 0.00)),0)")
                //sb.Append(" <=(select w.air_bal_amt from writeoff_mst_tbl")
                //sb.Append(" w where (w.APPLY_TO_AGENTS = '0' or AMT.AGENT_MST_PK")
                //sb.Append(" in (" & ApplyUsersPK & "))  and w.write_off_user_fk = " & HttpContext.Current.Session("USER_PK") & "")
                //If usrLocFK > 0 Then
                //    sb.Append(" and w.write_off_loc_fk = " & usrLocFK & " ")
                //End If
                //sb.Append(" ) AND VJC.BUSINESS_TYPE=1 ")
                sb.Append(" )) AND VJC.BUSINESS_TYPE=1 ");
                if (!string.IsNullOrEmpty(strVessel))
                {
                    sb.Append(" AND VJC.VOYAGE_NO LIKE '%" + strVessel.Trim() + "%'");
                }
                if (!string.IsNullOrEmpty(strVesselName))
                {
                    sb.Append(" AND VJC.VSL LIKE '%" + strVesselName.Trim() + "%'");
                }
                sb.Append(" GROUP BY");
                sb.Append(" VJC.INVOICE_REF_NO, VJC.process_type, VJC.business_type,VJC.INVOICE_DATE");
            }
            else if (BizType == 2)
            {
                sb.Append(" UNION SELECT MAX(VJC.PARTY_NAME) CUSTOMER_NAME,");
                sb.Append(" DECODE(VJC.BUSINESS_TYPE, 1, 'Air', 2, 'Sea') BIZTYPE,");
                sb.Append(" DECODE(VJC.PROCESS_TYPE, 1, 'Export', 2, 'Import') PROCESS,");
                sb.Append(" '' LOCATION,");
                sb.Append(" MAX(VJC.CONSOL_INVOICE_PK) PK,");
                sb.Append(" VJC.INVOICE_REF_NO,");
                sb.Append(" MAX(VJC.INVOICE_DATE) INVOICE_DATE,");
                sb.Append(" MAX(VJC.NET_INV_AMT) NET_RECEIVABLE,");
                sb.Append(" (SELECT NVL(sum(c.recd_amount_hdr_curr), 0) ");
                sb.Append("                       FROM ");
                sb.Append("                             collections_trn_tbl c ");
                sb.Append("                       WHERE c.invoice_ref_nr=VJC.INVOICE_REF_NO)");
                sb.Append("                       ");
                sb.Append("                       Recieved,");
                sb.Append(" NVL((max(VJC.NET_INV_AMT) - ");
                sb.Append("   (SELECT NVL(sum(c.recd_amount_hdr_curr), 0) ");
                sb.Append("   FROM collections_trn_tbl c ");
                sb.Append("   WHERE c.invoice_ref_nr=VJC.INVOICE_REF_NO)");
                sb.Append(" -NVL(max(VJC.WRITEOFF_AMOUNT), '0.00')),'0.00') Balance,");
                sb.Append("  (TO_DATE(SYSDATE,'DD/MM/YYYY')-TO_DATE(VJC.INVOICE_DATE))OutStandingDays,");

                //'Pending
                if (Status == 1)
                {
                    sb.Append(" 0 amount,'' Idate,");
                    //'History
                }
                else
                {
                    sb.Append(" NVL(max(VJC.WRITEOFF_AMOUNT),'0.00') amount,NVL(max(TO_DATE(VJC.WRITEOFF_DATE,'DD/MM/YYYY')), '') Idate,");
                }
                sb.Append(" max(VJC.REMARKS) REMARKS,1 PARTY_TYPE,'' Sel");
                sb.Append(" FROM");
                sb.Append(" VIEW_WRITE_OFF_JOBCARD_POPUP VJC, AGENT_MST_TBL AMT ");
                if (Apply == 2)
                {
                    sb.Append(" , WRITEOFF_MST_TBL WMT");
                }
                sb.Append("     WHERE VJC.PARTY_PK= AMT.AGENT_MST_PK   /* AND VJC.PK IS NOT NULL*/ AND VJC.PARTY_TYPE=1  AND VJC.CREATED_BY_FK IN");
                sb.Append("     (SELECT UMT.USER_MST_PK");
                sb.Append("     FROM USER_MST_TBL UMT");
                sb.Append("     WHERE UMT.DEFAULT_LOCATION_FK =");
                sb.Append("     (SELECT U.DEFAULT_LOCATION_FK");
                sb.Append("     FROM USER_MST_TBL U");
                sb.Append("     WHERE U.USER_MST_PK =" + HttpContext.Current.Session["USER_PK"] + "))");
                if (Apply == 2)
                {
                    sb.Append(" And instr(','||WMT.APPLY_TO_AGENTS||',',','||AMT.AGENT_MST_PK||',') > 0");
                }
                if (!(fromDate == null | string.IsNullOrEmpty(fromDate)))
                {
                    sb.Append(" AND VJC.invoice_date >= TO_DATE('" + fromDate + "',dateformat) ");
                }
                else if (!(toDate == null | string.IsNullOrEmpty(toDate)))
                {
                    sb.Append(" AND VJC.invoice_date <= TO_DATE('" + toDate + "',dateformat) ");
                }
                //sb.Append(" and NVL((vjc.NET_INV_AMT -NVL((SELECT NVL(sum(c.recd_amount_hdr_curr), 0)")
                //sb.Append(" FROM collections_trn_tbl c ")
                //sb.Append(" WHERE c.invoice_ref_nr = vjc.INVOICE_REF_NO), 0.00)),0)")
                //sb.Append(" <=(select w.sea_bal_amt from writeoff_mst_tbl w where")
                //sb.Append(" (w.APPLY_TO_AGENTS = '0' or AMT.AGENT_MST_PK")
                //sb.Append(" in (" & ApplyUsersPK & "))  and w.write_off_user_fk = " & HttpContext.Current.Session("USER_PK") & "")
                //If usrLocFK > 0 Then
                //    sb.Append(" and w.write_off_loc_fk = " & usrLocFK & " ")
                //End If
                //sb.Append(" ) and ((select floor(sysdate - vjc.INVOICE_DATE) from dual) >=")
                sb.Append(" and ((select floor(sysdate - vjc.INVOICE_DATE) from dual) >=");
                sb.Append(" (select w.write_off_sea_cr_days");
                sb.Append(" from writeoff_mst_tbl w");
                sb.Append(" where (w.APPLY_TO_AGENTS = '0' or");
                sb.Append(" AMT.AGENT_MST_PK in (" + ApplyUsersPK + "))");
                sb.Append(" and w.write_off_user_fk = " + HttpContext.Current.Session["USER_PK"] + "");
                if (usrLocFK > 0)
                {
                    sb.Append(" and w.write_off_loc_fk = " + usrLocFK + " ");
                }
                sb.Append(" )) AND VJC.BUSINESS_TYPE=2 ");
                if (!string.IsNullOrEmpty(strVessel))
                {
                    sb.Append(" AND VJC.VOYAGE_NO LIKE '%" + strVessel.Trim() + "%'");
                }
                if (!string.IsNullOrEmpty(strVesselName))
                {
                    sb.Append(" AND VJC.VSL LIKE '%" + strVesselName.Trim() + "%'");
                }
                sb.Append(" GROUP BY");
                sb.Append(" VJC.INVOICE_REF_NO, VJC.process_type, VJC.business_type,VJC.INVOICE_DATE");
            }
            else
            {
                //Air
                sb.Append(" UNION SELECT MAX(VJC.PARTY_NAME) CUSTOMER_NAME,");
                sb.Append(" DECODE(VJC.BUSINESS_TYPE, 1, 'Air', 2, 'Sea') BIZTYPE,");
                sb.Append(" DECODE(VJC.PROCESS_TYPE, 1, 'Export', 2, 'Import') PROCESS,");
                sb.Append(" '' LOCATION,");
                sb.Append(" MAX(VJC.CONSOL_INVOICE_PK) PK,");
                sb.Append(" VJC.INVOICE_REF_NO,");
                sb.Append(" MAX(VJC.INVOICE_DATE) INVOICE_DATE,");
                sb.Append(" MAX(VJC.NET_INV_AMT) NET_RECEIVABLE,");
                sb.Append(" (SELECT NVL(sum(c.recd_amount_hdr_curr), 0) ");
                sb.Append("                       FROM ");
                sb.Append("                             collections_trn_tbl c ");
                sb.Append("                       WHERE c.invoice_ref_nr=VJC.INVOICE_REF_NO)");
                sb.Append("                       ");
                sb.Append("                       Recieved,");
                sb.Append(" NVL((max(VJC.NET_INV_AMT) - ");
                sb.Append("   (SELECT NVL(sum(c.recd_amount_hdr_curr), 0) ");
                sb.Append("   FROM collections_trn_tbl c ");
                sb.Append("   WHERE c.invoice_ref_nr=VJC.INVOICE_REF_NO)");
                sb.Append(" -NVL(max(VJC.WRITEOFF_AMOUNT), '0.00')),'0.00') Balance,");
                sb.Append("  (TO_DATE(SYSDATE,'DD/MM/YYYY')-TO_DATE(VJC.INVOICE_DATE))OutStandingDays,");
                //'Pending
                if (Status == 1)
                {
                    sb.Append(" 0 amount,'' Idate,");
                    //'History
                }
                else
                {
                    sb.Append(" NVL(max(VJC.WRITEOFF_AMOUNT),'0.00') amount,NVL(max(TO_DATE(VJC.WRITEOFF_DATE,'DD/MM/YYYY')), '') Idate,");
                }
                sb.Append(" max(VJC.REMARKS) REMARKS,1 PARTY_TYPE,'' Sel");
                sb.Append(" FROM");
                sb.Append(" VIEW_WRITE_OFF_JOBCARD_POPUP VJC, AGENT_MST_TBL AMT ");
                if (Apply == 2)
                {
                    sb.Append(" , WRITEOFF_MST_TBL WMT");
                }
                sb.Append("     WHERE VJC.PARTY_PK= AMT.AGENT_MST_PK  /*  AND VJC.PK IS NOT NULL*/ AND VJC.PARTY_TYPE=1  AND VJC.CREATED_BY_FK IN");
                sb.Append("     (SELECT UMT.USER_MST_PK");
                sb.Append("     FROM USER_MST_TBL UMT");
                sb.Append("     WHERE UMT.DEFAULT_LOCATION_FK =");
                sb.Append("     (SELECT U.DEFAULT_LOCATION_FK");
                sb.Append("     FROM USER_MST_TBL U");
                sb.Append("     WHERE U.USER_MST_PK =" + HttpContext.Current.Session["USER_PK"] + "))");
                if (Apply == 2)
                {
                    sb.Append(" And instr(','||WMT.APPLY_TO_AGENTS||',',','||AMT.AGENT_MST_PK||',') > 0");
                }
                if (!(fromDate == null | string.IsNullOrEmpty(fromDate)))
                {
                    sb.Append(" AND VJC.invoice_date >= TO_DATE('" + fromDate + "',dateformat) ");
                }
                else if (!(toDate == null | string.IsNullOrEmpty(toDate)))
                {
                    sb.Append(" AND VJC.invoice_date <= TO_DATE('" + toDate + "',dateformat) ");
                }
                sb.Append(" and ((select floor(sysdate-vjc.INVOICE_DATE) from dual) >= ");
                sb.Append("(select w.write_off_air_cr_days from writeoff_mst_tbl");
                sb.Append(" w where (w.APPLY_TO_AGENTS = '0' or AMT.AGENT_MST_PK");
                sb.Append(" in (" + ApplyUsersPK + "))  ");
                sb.Append(" and w.write_off_user_fk = " + HttpContext.Current.Session["USER_PK"] + "");
                if (usrLocFK > 0)
                {
                    sb.Append(" and w.write_off_loc_fk = " + usrLocFK + " ");
                }
                //sb.Append(" )) and NVL((vjc.NET_INV_AMT -NVL((SELECT NVL(sum(c.recd_amount_hdr_curr), 0)")
                //sb.Append(" FROM collections_trn_tbl c ")
                //sb.Append(" WHERE c.invoice_ref_nr = vjc.INVOICE_REF_NO), 0.00)),0)")
                //sb.Append(" <=(select w.air_bal_amt from writeoff_mst_tbl")
                //sb.Append(" w where (w.APPLY_TO_AGENTS = '0' or AMT.AGENT_MST_PK")
                //sb.Append(" in (" & ApplyUsersPK & "))  and w.write_off_user_fk = " & HttpContext.Current.Session("USER_PK") & "")
                //If usrLocFK > 0 Then
                //    sb.Append(" and w.write_off_loc_fk = " & usrLocFK & " ")
                //End If
                //sb.Append(" ) AND VJC.BUSINESS_TYPE=1 ")
                sb.Append(" )) AND VJC.BUSINESS_TYPE=1 ");
                if (!string.IsNullOrEmpty(strJobRefNo))
                {
                    sb.Append(" AND VJC.REF_NO='" + strJobRefNo + "'");
                }

                if (!string.IsNullOrEmpty(strCustomer))
                {
                    sb.Append(" AND AMT.AGENT_MST_PK IN (" + strCustomer + ")");
                }

                if (!string.IsNullOrEmpty(strInvRefNo))
                {
                    sb.Append(" AND VJC.INVOICE_REF_NO = '" + strInvRefNo + "' ");
                }
                if (!string.IsNullOrEmpty(strVessel))
                {
                    sb.Append(" AND VJC.VOYAGE_NO LIKE '%" + strVessel.Trim() + "%'");
                }
                if (!string.IsNullOrEmpty(strVesselName))
                {
                    sb.Append(" AND VJC.VSL LIKE '%" + strVesselName.Trim() + "%'");
                }
                sb.Append(" GROUP BY");
                sb.Append(" VJC.INVOICE_REF_NO, VJC.process_type, VJC.business_type,VJC.INVOICE_DATE");
                //Sea
                sb.Append(" UNION SELECT MAX(VJC.PARTY_NAME) CUSTOMER_NAME,");
                sb.Append(" DECODE(VJC.BUSINESS_TYPE, 1, 'Air', 2, 'Sea') BIZTYPE,");
                sb.Append(" DECODE(VJC.PROCESS_TYPE, 1, 'Export', 2, 'Import') PROCESS,");
                sb.Append(" '' LOCATION,");
                sb.Append(" MAX(VJC.CONSOL_INVOICE_PK) PK,");
                sb.Append(" VJC.INVOICE_REF_NO,");
                sb.Append(" MAX(VJC.INVOICE_DATE) INVOICE_DATE,");
                sb.Append(" MAX(VJC.NET_INV_AMT) NET_RECEIVABLE,");
                sb.Append(" (SELECT NVL(sum(c.recd_amount_hdr_curr), 0) ");
                sb.Append("                       FROM ");
                sb.Append("                             collections_trn_tbl c ");
                sb.Append("                       WHERE c.invoice_ref_nr=VJC.INVOICE_REF_NO)");
                sb.Append("                       ");
                sb.Append("                       Recieved,");
                sb.Append(" NVL((max(VJC.NET_INV_AMT) - ");
                sb.Append("   (SELECT NVL(sum(c.recd_amount_hdr_curr), 0) ");
                sb.Append("   FROM collections_trn_tbl c ");
                sb.Append("   WHERE c.invoice_ref_nr=VJC.INVOICE_REF_NO)");
                sb.Append(" -NVL(max(VJC.WRITEOFF_AMOUNT), '0.00')),'0.00') Balance,");
                sb.Append("  (TO_DATE(SYSDATE,'DD/MM/YYYY')-TO_DATE(VJC.INVOICE_DATE))OutStandingDays,");
                //'Pending
                if (Status == 1)
                {
                    sb.Append(" 0 amount,'' Idate,");
                    //'History
                }
                else
                {
                    sb.Append(" NVL(max(VJC.WRITEOFF_AMOUNT),'0.00') amount,NVL(max(TO_DATE(VJC.WRITEOFF_DATE,'DD/MM/YYYY')), '') Idate,");
                }
                sb.Append(" max(VJC.REMARKS) REMARKS,1 PARTY_TYPE,'' Sel");
                sb.Append(" FROM");
                sb.Append(" VIEW_WRITE_OFF_JOBCARD_POPUP VJC, AGENT_MST_TBL AMT ");
                if (Apply == 2)
                {
                    sb.Append(" , WRITEOFF_MST_TBL WMT");
                }
                sb.Append("     WHERE VJC.PARTY_PK= AMT.AGENT_MST_PK   /* AND VJC.PK IS NOT NULL */ AND VJC.PARTY_TYPE=1  AND VJC.CREATED_BY_FK IN");
                sb.Append("     (SELECT UMT.USER_MST_PK");
                sb.Append("     FROM USER_MST_TBL UMT");
                sb.Append("     WHERE UMT.DEFAULT_LOCATION_FK =");
                sb.Append("     (SELECT U.DEFAULT_LOCATION_FK");
                sb.Append("     FROM USER_MST_TBL U");
                sb.Append("     WHERE U.USER_MST_PK =" + HttpContext.Current.Session["USER_PK"] + "))");
                if (Apply == 2)
                {
                    sb.Append(" And instr(','||WMT.APPLY_TO_AGENTS||',',','||AMT.AGENT_MST_PK||',') > 0");
                }
                if (!(fromDate == null | string.IsNullOrEmpty(fromDate)))
                {
                    sb.Append(" AND VJC.invoice_date >= TO_DATE('" + fromDate + "',dateformat) ");
                }
                else if (!(toDate == null | string.IsNullOrEmpty(toDate)))
                {
                    sb.Append(" AND VJC.invoice_date <= TO_DATE('" + toDate + "',dateformat) ");
                }
                //sb.Append(" and NVL((vjc.NET_INV_AMT -NVL((SELECT NVL(sum(c.recd_amount_hdr_curr), 0)")
                //sb.Append(" FROM collections_trn_tbl c ")
                //sb.Append(" WHERE c.invoice_ref_nr = vjc.INVOICE_REF_NO), 0.00)),0)")
                //sb.Append(" <=(select w.sea_bal_amt from writeoff_mst_tbl w where")
                //sb.Append(" (w.APPLY_TO_AGENTS = '0' or AMT.AGENT_MST_PK")
                //sb.Append(" in (" & ApplyUsersPK & "))  and w.write_off_user_fk = " & HttpContext.Current.Session("USER_PK") & "")
                //If usrLocFK > 0 Then
                //    sb.Append(" and w.write_off_loc_fk = " & usrLocFK & " ")
                //End If
                //sb.Append(" ) and ((select floor(sysdate - vjc.INVOICE_DATE) from dual) >=")
                sb.Append(" and ((select floor(sysdate - vjc.INVOICE_DATE) from dual) >=");
                sb.Append(" (select w.write_off_sea_cr_days");
                sb.Append(" from writeoff_mst_tbl w");
                sb.Append(" where (w.APPLY_TO_AGENTS = '0' or");
                sb.Append(" AMT.AGENT_MST_PK in (" + ApplyUsersPK + "))");
                sb.Append(" and w.write_off_user_fk = " + HttpContext.Current.Session["USER_PK"] + "");
                if (usrLocFK > 0)
                {
                    sb.Append(" and w.write_off_loc_fk = " + usrLocFK + " ");
                }
                sb.Append(" )) AND VJC.BUSINESS_TYPE=2 ");
                if (!string.IsNullOrEmpty(strJobRefNo))
                {
                    sb.Append(" AND VJC.REF_NO='" + strJobRefNo + "'");
                }
                if (!string.IsNullOrEmpty(strCustomer))
                {
                    sb.Append(" AND AMT.AGENT_MST_PK IN (" + strCustomer + ")");
                }

                if (!string.IsNullOrEmpty(strInvRefNo))
                {
                    sb.Append(" AND VJC.INVOICE_REF_NO = '" + strInvRefNo + "' ");
                }
                if (!string.IsNullOrEmpty(strVessel))
                {
                    sb.Append(" AND VJC.VOYAGE_NO LIKE '%" + strVessel.Trim() + "%'");
                }
                if (!string.IsNullOrEmpty(strVesselName))
                {
                    sb.Append(" AND VJC.VSL LIKE '%" + strVesselName.Trim() + "%'");
                }
                sb.Append(" GROUP BY");
                sb.Append(" VJC.INVOICE_REF_NO, VJC.process_type, VJC.business_type,VJC.INVOICE_DATE");
            }
            //Air
            if (BizType == 1)
            {
                //***********VENDOR*AIR******************
                sb.Append(" UNION SELECT MAX(VJC.PARTY_NAME) CUSTOMER_NAME,");
                sb.Append(" 'Air' BIZTYPE,");
                sb.Append(" DECODE(VJC.PROCESS_TYPE, 1, 'Export', 2, 'Import') PROCESS,");
                sb.Append(" '' LOCATION,");
                sb.Append(" MAX(VJC.CONSOL_INVOICE_PK) PK,");
                sb.Append(" VJC.INVOICE_REF_NO,");
                sb.Append(" MAX(VJC.INVOICE_DATE) INVOICE_DATE,");
                sb.Append(" MAX(VJC.NET_INV_AMT) NET_RECEIVABLE,");
                sb.Append(" (SELECT NVL(sum(c.recd_amount_hdr_curr), 0) ");
                sb.Append("                       FROM ");
                sb.Append("                             collections_trn_tbl c ");
                sb.Append("                       WHERE c.invoice_ref_nr=VJC.INVOICE_REF_NO)");
                sb.Append("                       ");
                sb.Append("                       Recieved,");
                sb.Append(" NVL((max(VJC.NET_INV_AMT) - ");
                sb.Append("   (SELECT NVL(sum(c.recd_amount_hdr_curr), 0) ");
                sb.Append("   FROM collections_trn_tbl c ");
                sb.Append("   WHERE c.invoice_ref_nr=VJC.INVOICE_REF_NO)");
                sb.Append(" -NVL(max(VJC.WRITEOFF_AMOUNT), '0.00')),'0.00') Balance,");
                sb.Append("  (TO_DATE(SYSDATE,'DD/MM/YYYY')-TO_DATE(VJC.INVOICE_DATE))OutStandingDays,");
                //'Pending
                if (Status == 1)
                {
                    sb.Append(" 0 amount,'' Idate,");
                    //'History
                }
                else
                {
                    sb.Append(" NVL(max(VJC.WRITEOFF_AMOUNT),'0.00') amount,NVL(max(TO_DATE(VJC.WRITEOFF_DATE,'DD/MM/YYYY')), '') Idate,");
                }
                sb.Append(" max(VJC.REMARKS) REMARKS,2 PARTY_TYPE,'' Sel");
                sb.Append(" FROM");
                sb.Append(" VIEW_WRITE_OFF_JOBCARD_POPUP VJC, AIRLINE_MST_TBL AMT ");
                if (Apply == 2)
                {
                    sb.Append(" , WRITEOFF_MST_TBL WMT");
                }
                sb.Append("     WHERE VJC.PARTY_PK= AMT.AIRLINE_MST_PK   /* AND VJC.PK IS NOT NULL*/ AND VJC.PARTY_TYPE=2  AND VJC.CREATED_BY_FK IN");
                sb.Append("     (SELECT UMT.USER_MST_PK");
                sb.Append("     FROM USER_MST_TBL UMT");
                sb.Append("     WHERE UMT.DEFAULT_LOCATION_FK =");
                sb.Append("     (SELECT U.DEFAULT_LOCATION_FK");
                sb.Append("     FROM USER_MST_TBL U");
                sb.Append("     WHERE U.USER_MST_PK =" + HttpContext.Current.Session["USER_PK"] + "))");
                if (Apply == 2)
                {
                    sb.Append(" And instr(','||wmt.apply_to_vendors||',',','||AMT.AIRLINE_MST_PK||',') > 0");
                }
                if (!(fromDate == null | string.IsNullOrEmpty(fromDate)))
                {
                    sb.Append(" AND VJC.invoice_date >= TO_DATE('" + fromDate + "',dateformat) ");
                }
                else if (!(toDate == null | string.IsNullOrEmpty(toDate)))
                {
                    sb.Append(" AND VJC.invoice_date <= TO_DATE('" + toDate + "',dateformat) ");
                }
                sb.Append(" and ((select floor(sysdate-vjc.INVOICE_DATE) from dual) >= ");
                sb.Append("(select w.write_off_air_cr_days from writeoff_mst_tbl");
                sb.Append(" w where (w.apply_to_vendors = '0' or AMT.AIRLINE_MST_PK");
                sb.Append(" in (" + ApplyUsersPK + ")) ");
                sb.Append(" and w.write_off_user_fk = " + HttpContext.Current.Session["USER_PK"] + "");
                if (usrLocFK <= 0)
                {
                    sb.Append(" ))  ");
                }
                if (usrLocFK > 0)
                {
                    sb.Append("  and w.write_off_loc_fk = " + usrLocFK + " )) ");
                }
                //sb.Append(" and NVL((vjc.NET_INV_AMT -NVL((SELECT NVL(sum(c.recd_amount_hdr_curr), 0)")
                //sb.Append(" FROM collections_trn_tbl c ")
                //sb.Append(" WHERE c.invoice_ref_nr = vjc.INVOICE_REF_NO), 0.00)),0)")
                //sb.Append(" <=(select w.air_bal_amt from writeoff_mst_tbl")
                //sb.Append(" w where (w.apply_to_vendors = '0' or AMT.AIRLINE_MST_PK")
                //sb.Append(" in (" & ApplyUsersPK & "))  and w.write_off_user_fk = " & HttpContext.Current.Session("USER_PK") & "")
                //If usrLocFK > 0 Then
                //    sb.Append(" and w.write_off_loc_fk = " & usrLocFK & " ")
                //End If
                //sb.Append(" ) AND VJC.BUSINESS_TYPE=1 ")
                sb.Append(" AND VJC.BUSINESS_TYPE=1 ");
                if (!string.IsNullOrEmpty(strJobRefNo))
                {
                    sb.Append(" AND VJC.REF_NO='" + strJobRefNo + "'");
                }

                if (!string.IsNullOrEmpty(strCustomer))
                {
                    sb.Append(" AND AMT.AIRLINE_MST_PK IN (" + strCustomer + ")");
                }

                if (!string.IsNullOrEmpty(strInvRefNo))
                {
                    sb.Append(" AND VJC.INVOICE_REF_NO = '" + strInvRefNo + "' ");
                }
                if (!string.IsNullOrEmpty(strVessel))
                {
                    sb.Append(" AND VJC.VOYAGE_NO LIKE '%" + strVessel.Trim() + "%'");
                }
                if (!string.IsNullOrEmpty(strVesselName))
                {
                    sb.Append(" AND VJC.VSL LIKE '%" + strVesselName.Trim() + "%'");
                }
                sb.Append(" GROUP BY");
                sb.Append(" VJC.INVOICE_REF_NO, VJC.process_type, VJC.business_type,VJC.INVOICE_DATE");
                //Sea
            }
            else if (BizType == 2)
            {
                //*************VENDOR*SEA****************
                sb.Append(" UNION SELECT MAX(VJC.PARTY_NAME) CUSTOMER_NAME,");
                sb.Append(" 'Sea' BIZTYPE,");
                sb.Append(" DECODE(VJC.PROCESS_TYPE, 1, 'Export', 2, 'Import') PROCESS,");
                sb.Append(" '' LOCATION,");
                sb.Append(" MAX(VJC.CONSOL_INVOICE_PK) PK,");
                sb.Append(" VJC.INVOICE_REF_NO,");
                sb.Append(" MAX(VJC.INVOICE_DATE) INVOICE_DATE,");
                sb.Append(" MAX(VJC.NET_INV_AMT) NET_RECEIVABLE,");
                sb.Append(" (SELECT NVL(sum(c.recd_amount_hdr_curr), 0) ");
                sb.Append("                       FROM ");
                sb.Append("                             collections_trn_tbl c ");
                sb.Append("                       WHERE c.invoice_ref_nr=VJC.INVOICE_REF_NO)");
                sb.Append("                       ");
                sb.Append("                       Recieved,");
                sb.Append(" NVL((max(VJC.NET_INV_AMT) - ");
                sb.Append("   (SELECT NVL(sum(c.recd_amount_hdr_curr), 0) ");
                sb.Append("   FROM collections_trn_tbl c ");
                sb.Append("   WHERE c.invoice_ref_nr=VJC.INVOICE_REF_NO)");
                sb.Append(" -NVL(max(VJC.WRITEOFF_AMOUNT), '0.00')),'0.00') Balance,");
                sb.Append("  (TO_DATE(SYSDATE,'DD/MM/YYYY')-TO_DATE(VJC.INVOICE_DATE))OutStandingDays,");

                //'Pending
                if (Status == 1)
                {
                    sb.Append(" 0 amount,'' Idate,");
                    //'History
                }
                else
                {
                    sb.Append(" NVL(max(VJC.WRITEOFF_AMOUNT),'0.00') amount,NVL(max(TO_DATE(VJC.WRITEOFF_DATE,'DD/MM/YYYY')), '') Idate,");
                }
                sb.Append(" max(VJC.REMARKS) REMARKS,2 PARTY_TYPE,'' Sel");
                sb.Append(" FROM");
                sb.Append(" VIEW_WRITE_OFF_JOBCARD_POPUP VJC, OPERATOR_MST_TBL OMT ");
                if (Apply == 2)
                {
                    sb.Append(" , WRITEOFF_MST_TBL WMT");
                }
                sb.Append("     WHERE VJC.PARTY_PK= OMT.OPERATOR_MST_PK   /* AND VJC.PK IS NOT NULL*/  AND VJC.PARTY_TYPE=2  AND VJC.CREATED_BY_FK IN");
                sb.Append("     (SELECT UMT.USER_MST_PK");
                sb.Append("     FROM USER_MST_TBL UMT");
                sb.Append("     WHERE UMT.DEFAULT_LOCATION_FK =");
                sb.Append("     (SELECT U.DEFAULT_LOCATION_FK");
                sb.Append("     FROM USER_MST_TBL U");
                sb.Append("     WHERE U.USER_MST_PK =" + HttpContext.Current.Session["USER_PK"] + "))");
                if (Apply == 2)
                {
                    sb.Append(" And instr(','||wmt.apply_to_vendors||',',','||OMT.OPERATOR_MST_PK||',') > 0");
                }
                if (!(fromDate == null | string.IsNullOrEmpty(fromDate)))
                {
                    sb.Append(" AND VJC.invoice_date >= TO_DATE('" + fromDate + "',dateformat) ");
                }
                else if (!(toDate == null | string.IsNullOrEmpty(toDate)))
                {
                    sb.Append(" AND VJC.invoice_date <= TO_DATE('" + toDate + "',dateformat) ");
                }
                //sb.Append(" and NVL((vjc.NET_INV_AMT -NVL((SELECT NVL(sum(c.recd_amount_hdr_curr), 0)")
                //sb.Append(" FROM collections_trn_tbl c ")
                //sb.Append(" WHERE c.invoice_ref_nr = vjc.INVOICE_REF_NO), 0.00)),0)")
                //sb.Append(" <=(select w.sea_bal_amt from writeoff_mst_tbl w where")
                //sb.Append(" (w.apply_to_vendors = '0' or OMT.OPERATOR_MST_PK")
                //sb.Append(" in (" & ApplyUsersPK & ")) and w.write_off_user_fk = " & HttpContext.Current.Session("USER_PK") & "")
                //If usrLocFK > 0 Then
                //    sb.Append(" and w.write_off_loc_fk = " & usrLocFK & " ")
                //End If
                //sb.Append(" ) and ((select floor(sysdate - vjc.INVOICE_DATE) from dual) >=")
                sb.Append(" and ((select floor(sysdate - vjc.INVOICE_DATE) from dual) >=");
                sb.Append(" (select w.write_off_sea_cr_days");
                sb.Append(" from writeoff_mst_tbl w");
                sb.Append(" where (w.apply_to_vendors = '0' or");
                sb.Append(" OMT.OPERATOR_MST_PK in (" + ApplyUsersPK + "))");
                sb.Append(" and w.write_off_user_fk = " + HttpContext.Current.Session["USER_PK"] + "");
                if (usrLocFK > 0)
                {
                    sb.Append(" and w.write_off_loc_fk = " + usrLocFK + " ");
                }
                sb.Append(" )) AND VJC.BUSINESS_TYPE=2 ");
                if (!string.IsNullOrEmpty(strJobRefNo))
                {
                    sb.Append(" AND VJC.REF_NO='" + strJobRefNo + "'");
                }

                if (!string.IsNullOrEmpty(strCustomer))
                {
                    sb.Append(" AND OMT.OPERATOR_MST_PK IN (" + strCustomer + ")");
                }

                if (!string.IsNullOrEmpty(strInvRefNo))
                {
                    sb.Append(" AND VJC.INVOICE_REF_NO = '" + strInvRefNo + "' ");
                }

                if (!string.IsNullOrEmpty(strVessel))
                {
                    sb.Append(" AND VJC.VOYAGE_NO LIKE '%" + strVessel.Trim() + "%'");
                }
                if (!string.IsNullOrEmpty(strVesselName))
                {
                    sb.Append(" AND VJC.VSL LIKE '%" + strVesselName.Trim() + "%'");
                }

                sb.Append(" GROUP BY");
                sb.Append(" VJC.INVOICE_REF_NO, VJC.process_type, VJC.business_type,VJC.INVOICE_DATE");
            }
            else
            {
                //Air
                //*************VENDOR*AIR****************
                sb.Append(" UNION SELECT MAX(VJC.PARTY_NAME) CUSTOMER_NAME,");
                sb.Append(" 'Air' BIZTYPE,");
                sb.Append(" DECODE(VJC.PROCESS_TYPE, 1, 'Export', 2, 'Import') PROCESS,");
                sb.Append(" '' LOCATION,");
                sb.Append(" MAX(VJC.CONSOL_INVOICE_PK) PK,");
                sb.Append(" VJC.INVOICE_REF_NO,");
                sb.Append(" MAX(VJC.INVOICE_DATE) INVOICE_DATE,");
                sb.Append(" MAX(VJC.NET_INV_AMT) NET_RECEIVABLE,");
                sb.Append(" (SELECT NVL(sum(c.recd_amount_hdr_curr), 0) ");
                sb.Append("                       FROM ");
                sb.Append("                             collections_trn_tbl c ");
                sb.Append("                       WHERE c.invoice_ref_nr=VJC.INVOICE_REF_NO)");
                sb.Append("                       ");
                sb.Append("                       Recieved,");
                sb.Append(" NVL((max(VJC.NET_INV_AMT) - ");
                sb.Append("   (SELECT NVL(sum(c.recd_amount_hdr_curr), 0) ");
                sb.Append("   FROM collections_trn_tbl c ");
                sb.Append("   WHERE c.invoice_ref_nr=VJC.INVOICE_REF_NO)");
                sb.Append(" -NVL(max(VJC.WRITEOFF_AMOUNT), '0.00')),'0.00') Balance,");
                sb.Append("  (TO_DATE(SYSDATE,'DD/MM/YYYY')-TO_DATE(VJC.INVOICE_DATE))OutStandingDays,");

                //'Pending
                if (Status == 1)
                {
                    sb.Append(" 0 amount,'' Idate,");
                    //'History
                }
                else
                {
                    sb.Append(" NVL(max(VJC.WRITEOFF_AMOUNT),'0.00') amount,NVL(max(TO_DATE(VJC.WRITEOFF_DATE,'DD/MM/YYYY')), '') Idate,");
                }
                sb.Append(" max(VJC.REMARKS) REMARKS,2 PARTY_TYPE,'' Sel");
                sb.Append(" FROM");
                sb.Append(" VIEW_WRITE_OFF_JOBCARD_POPUP VJC, AIRLINE_MST_TBL AMT ");
                if (Apply == 2)
                {
                    sb.Append(" , WRITEOFF_MST_TBL WMT");
                }
                sb.Append("     WHERE VJC.PARTY_PK= AMT.AIRLINE_MST_PK   /* AND VJC.PK IS NOT NULL*/  AND VJC.PARTY_TYPE=2  AND VJC.CREATED_BY_FK IN");
                sb.Append("     (SELECT UMT.USER_MST_PK");
                sb.Append("     FROM USER_MST_TBL UMT");
                sb.Append("     WHERE UMT.DEFAULT_LOCATION_FK =");
                sb.Append("     (SELECT U.DEFAULT_LOCATION_FK");
                sb.Append("     FROM USER_MST_TBL U");
                sb.Append("     WHERE U.USER_MST_PK =" + HttpContext.Current.Session["USER_PK"] + "))");
                if (Apply == 2)
                {
                    sb.Append(" And instr(','||wmt.apply_to_vendors||',',','||AMT.AIRLINE_MST_PK||',') > 0");
                }
                if (!(fromDate == null | string.IsNullOrEmpty(fromDate)))
                {
                    sb.Append(" AND VJC.invoice_date >= TO_DATE('" + fromDate + "',dateformat) ");
                }
                else if (!(toDate == null | string.IsNullOrEmpty(toDate)))
                {
                    sb.Append(" AND VJC.invoice_date <= TO_DATE('" + toDate + "',dateformat) ");
                }
                sb.Append(" and ((select floor(sysdate-vjc.INVOICE_DATE) from dual) >= ");
                sb.Append("(select w.write_off_air_cr_days from writeoff_mst_tbl");
                sb.Append(" w where (w.apply_to_vendors = '0' or AMT.AIRLINE_MST_PK");
                sb.Append(" in (" + ApplyUsersPK + "))  ");
                sb.Append(" and w.write_off_user_fk = " + HttpContext.Current.Session["USER_PK"] + "");
                if (usrLocFK > 0)
                {
                    sb.Append(" and w.write_off_loc_fk = " + usrLocFK + " ");
                }
                //sb.Append(" )) and NVL((vjc.NET_INV_AMT -NVL((SELECT NVL(sum(c.recd_amount_hdr_curr), 0)")
                //sb.Append(" FROM collections_trn_tbl c ")
                //sb.Append(" WHERE c.invoice_ref_nr = vjc.INVOICE_REF_NO), 0.00)),0)")
                //sb.Append(" <=(select w.air_bal_amt from writeoff_mst_tbl")
                //sb.Append(" w where (w.apply_to_vendors = '0' or AMT.AIRLINE_MST_PK")
                //sb.Append(" in (" & ApplyUsersPK & "))  and w.write_off_user_fk = " & HttpContext.Current.Session("USER_PK") & "")
                //If usrLocFK > 0 Then
                //    sb.Append(" and w.write_off_loc_fk = " & usrLocFK & " ")
                //End If
                //sb.Append(" )  AND VJC.BUSINESS_TYPE=1 ")
                sb.Append(")) AND VJC.BUSINESS_TYPE=1 ");
                if (!string.IsNullOrEmpty(strJobRefNo))
                {
                    sb.Append(" AND VJC.REF_NO='" + strJobRefNo + "'");
                }

                if (!string.IsNullOrEmpty(strCustomer))
                {
                    sb.Append(" AND AMT.AIRLINE_MST_PK IN (" + strCustomer + ")");
                }

                if (!string.IsNullOrEmpty(strInvRefNo))
                {
                    sb.Append(" AND VJC.INVOICE_REF_NO = '" + strInvRefNo + "' ");
                }

                if (!string.IsNullOrEmpty(strVessel))
                {
                    sb.Append(" AND VJC.VOYAGE_NO LIKE '%" + strVessel.Trim() + "%'");
                }

                if (!string.IsNullOrEmpty(strVesselName))
                {
                    sb.Append(" AND VJC.VSL LIKE '%" + strVesselName.Trim() + "%'");
                }
                sb.Append(" GROUP BY");
                sb.Append(" VJC.INVOICE_REF_NO, VJC.process_type, VJC.business_type,VJC.INVOICE_DATE");
                //Sea
                //*************VENDOR*SEA****************
                sb.Append(" UNION SELECT MAX(VJC.PARTY_NAME) CUSTOMER_NAME,");
                sb.Append(" 'Sea' BIZTYPE,");
                sb.Append(" DECODE(VJC.PROCESS_TYPE, 1, 'Export', 2, 'Import') PROCESS,");
                sb.Append(" '' LOCATION,");
                sb.Append(" MAX(VJC.CONSOL_INVOICE_PK) PK,");
                sb.Append(" VJC.INVOICE_REF_NO,");
                sb.Append(" MAX(VJC.INVOICE_DATE) INVOICE_DATE,");
                sb.Append(" MAX(VJC.NET_INV_AMT) NET_RECEIVABLE,");
                sb.Append(" (SELECT NVL(sum(c.recd_amount_hdr_curr), 0) ");
                sb.Append("                       FROM ");
                sb.Append("                             collections_trn_tbl c ");
                sb.Append("                       WHERE c.invoice_ref_nr=VJC.INVOICE_REF_NO)");
                sb.Append("                       ");
                sb.Append("                       Recieved,");
                sb.Append(" NVL((max(VJC.NET_INV_AMT) - ");
                sb.Append("   (SELECT NVL(sum(c.recd_amount_hdr_curr), 0) ");
                sb.Append("   FROM collections_trn_tbl c ");
                sb.Append("   WHERE c.invoice_ref_nr=VJC.INVOICE_REF_NO)");
                sb.Append(" -NVL(max(VJC.WRITEOFF_AMOUNT), '0.00')),'0.00') Balance,");
                sb.Append("  (TO_DATE(SYSDATE,'DD/MM/YYYY')-TO_DATE(VJC.INVOICE_DATE))OutStandingDays,");
                //'Pending
                if (Status == 1)
                {
                    sb.Append(" 0 amount,'' Idate,");
                    //'History
                }
                else
                {
                    sb.Append(" NVL(max(VJC.WRITEOFF_AMOUNT),'0.00') amount,NVL(max(TO_DATE(VJC.WRITEOFF_DATE,'DD/MM/YYYY')), '') Idate,");
                }
                sb.Append(" max(VJC.REMARKS) REMARKS,2 PARTY_TYPE,'' Sel");
                sb.Append(" FROM");
                sb.Append(" VIEW_WRITE_OFF_JOBCARD_POPUP VJC, OPERATOR_MST_TBL OMT ");
                if (Apply == 2)
                {
                    sb.Append(" , WRITEOFF_MST_TBL WMT");
                }
                sb.Append("     WHERE VJC.PARTY_PK= OMT.OPERATOR_MST_PK   /* AND VJC.PK IS NOT NULL*/ AND VJC.PARTY_TYPE=2  AND VJC.CREATED_BY_FK IN");
                sb.Append("     (SELECT UMT.USER_MST_PK");
                sb.Append("     FROM USER_MST_TBL UMT");
                sb.Append("     WHERE UMT.DEFAULT_LOCATION_FK =");
                sb.Append("     (SELECT U.DEFAULT_LOCATION_FK");
                sb.Append("     FROM USER_MST_TBL U");
                sb.Append("     WHERE U.USER_MST_PK =" + HttpContext.Current.Session["USER_PK"] + "))");
                if (Apply == 2)
                {
                    sb.Append(" And instr(','||wmt.apply_to_vendors||',',','||OMT.OPERATOR_MST_PK||',') > 0");
                }
                if (!(fromDate == null | string.IsNullOrEmpty(fromDate)))
                {
                    sb.Append(" AND VJC.invoice_date >= TO_DATE('" + fromDate + "',dateformat) ");
                }
                else if (!(toDate == null | string.IsNullOrEmpty(toDate)))
                {
                    sb.Append(" AND VJC.invoice_date <= TO_DATE('" + toDate + "',dateformat) ");
                }
                //sb.Append(" and NVL((vjc.NET_INV_AMT -NVL((SELECT NVL(sum(c.recd_amount_hdr_curr), 0)")
                //sb.Append(" FROM collections_trn_tbl c ")
                //sb.Append(" WHERE c.invoice_ref_nr = vjc.INVOICE_REF_NO), 0.00)),0)")
                //sb.Append(" <=(select w.sea_bal_amt from writeoff_mst_tbl w where")
                //sb.Append(" (w.apply_to_vendors = '0' or OMT.OPERATOR_MST_PK")
                //sb.Append(" in (" & ApplyUsersPK & "))  and w.write_off_user_fk = " & HttpContext.Current.Session("USER_PK") & "")
                //If usrLocFK > 0 Then
                //    sb.Append(" and w.write_off_loc_fk = " & usrLocFK & " ")
                //End If
                //sb.Append(" ) and ((select floor(sysdate - vjc.INVOICE_DATE) from dual) >=")
                sb.Append(" and ((select floor(sysdate - vjc.INVOICE_DATE) from dual) >=");
                sb.Append(" (select w.write_off_sea_cr_days");
                sb.Append(" from writeoff_mst_tbl w");
                sb.Append(" where (w.apply_to_vendors = '0' or");
                sb.Append(" OMT.OPERATOR_MST_PK in (" + ApplyUsersPK + "))");
                sb.Append(" and w.write_off_user_fk = " + HttpContext.Current.Session["USER_PK"] + "");
                if (usrLocFK > 0)
                {
                    sb.Append(" and w.write_off_loc_fk = " + usrLocFK + " ");
                }
                sb.Append(" )) AND VJC.BUSINESS_TYPE=2 ");
                if (!string.IsNullOrEmpty(strJobRefNo))
                {
                    sb.Append(" AND VJC.REF_NO='" + strJobRefNo + "'");
                }
                if (!string.IsNullOrEmpty(strCustomer))
                {
                    sb.Append(" AND OMT.OPERATOR_MST_PK IN (" + strCustomer + ")");
                }

                if (!string.IsNullOrEmpty(strInvRefNo))
                {
                    sb.Append(" AND VJC.INVOICE_REF_NO = '" + strInvRefNo + "' ");
                }
                if (!string.IsNullOrEmpty(strVessel))
                {
                    sb.Append(" AND VJC.VOYAGE_NO LIKE '%" + strVessel.Trim() + "%'");
                }

                if (!string.IsNullOrEmpty(strVesselName))
                {
                    sb.Append(" AND VJC.VSL LIKE '%" + strVesselName.Trim() + "%'");
                }
                sb.Append(" GROUP BY");
                sb.Append(" VJC.INVOICE_REF_NO, VJC.process_type, VJC.business_type,VJC.INVOICE_DATE");
            }
            // Get the Count of Records
            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            OracleDataReader dr = null;
            strCount.Append(" SELECT COUNT(*) cnt from  ");
            strCount.Append((" (" + sb.ToString() + ") Q"));
            strCount.Append("   WHERE ");
            //'Pending
            if (Status == 1)
            {
                strCount.Append(" Q.BALANCE > 0 ");
                //'History
            }
            else if (Status == 2)
            {
                strCount.Append("   Q.AMOUNT <> 0");
            }
            if (BizType != 0)
            {
                strCount.Append(" AND Q.BIZTYPE='" + BizText + "'");
            }
            if (process != 0)
            {
                strCount.Append(" AND Q.PROCESS='" + ProcessText + "'");
            }
            if (Party_Type != 3)
            {
                strCount.Append(" AND Q.PARTY_TYPE='" + Party_Type + "'");
            }
            dr = objWF.GetDataReader(strCount.ToString());
            while (dr.Read())
            {
                TotalRecords += Convert.ToInt32(dr[0]) + 0;
            }
            dr.Close();

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
            sqlstr.Append("SELECT ROWNUM \"SL_NR\", QRY2.CUSTOMER_NAME, ");
            sqlstr.Append("BIZTYPE,PROCESS,LOCATION,PK, ");
            sqlstr.Append("INVOICE_REF_NO,INVOICE_DATE, ");
            sqlstr.Append("NET_RECEIVABLE,Recieved, ");
            sqlstr.Append("Balance,OutStandingDays, ");
            sqlstr.Append("amount,Idate,");
            //'Pending
            if (Status == 1)
            {
                sqlstr.Append(" '' REMARKS,");
            }
            else
            {
                sqlstr.Append(" REMARKS,");
            }
            sqlstr.Append("PARTY_TYPE,Sel  FROM ( ");
            sqlstr.Append("SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL_NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T  WHERE 1=1 ");
            sqlstr.Append("  ) QRY ");
            sqlstr.Append("   WHERE 1 = 1 ");
            if (BizType != 0)
            {
                sqlstr.Append(" AND QRY.BIZTYPE='" + BizText + "'");
            }
            if (process != 0)
            {
                sqlstr.Append(" AND QRY.PROCESS='" + ProcessText + "'");
            }
            if (Party_Type != 3)
            {
                sqlstr.Append(" AND QRY.PARTY_TYPE='" + Party_Type + "'");
            }

            //'Pending
            if (Status == 1)
            {
                sqlstr.Append(" AND QRY.BALANCE > 0 ");
                //'History
            }
            else if (Status == 2)
            {
                sqlstr.Append(" AND  QRY.AMOUNT <> 0");
            }
            sqlstr.Append("  AND \"SL_NR\"  BETWEEN " + start + " AND " + last);
            sqlstr.Append("  )QRY2 ");
            string sql = null;
            sql = sqlstr.ToString();
            try
            {
                return objWF.GetDataSet(sql);
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
        /// Saves the writeoff.
        /// </summary>
        /// <param name="ds">The ds.</param>
        /// <returns></returns>
        public ArrayList SaveWriteoff(DataSet ds)
        {
            // for saving write off amount manually
            WorkFlow objWK = new WorkFlow();
            OracleTransaction INSERT = null;
            OracleCommand Ins = new OracleCommand();
            Int32 RecAfct = default(Int32);
            Int32 nRowCnt = default(Int32);
            string strPKs = "";
            objWK.OpenConnection();
            INSERT = objWK.MyConnection.BeginTransaction();

            try
            {
                var _with5 = Ins;
                _with5.Transaction = INSERT;
                _with5.Connection = objWK.MyConnection;
                _with5.CommandType = CommandType.StoredProcedure;
                _with5.CommandText = objWK.MyUserName + ".WRITE_OFF_PKG.INS_WRITEOFF_MANUAL";

                for (nRowCnt = 0; nRowCnt <= ds.Tables[0].Rows.Count - 1; nRowCnt++)
                {
                    _with5.Parameters.Clear();
                    Ins.Parameters.Add("CONSOL_INVOICE_PK_IN", ds.Tables[0].Rows[nRowCnt]["CONSOL_INVOICE_PK"]).Direction = ParameterDirection.Input;
                    Ins.Parameters.Add("WRITEOFF_AMOUNT_IN", ds.Tables[0].Rows[nRowCnt]["WRITEOFF_AMOUNT"]).Direction = ParameterDirection.Input;
                    Ins.Parameters.Add("WRITEOFF_DATE_IN", ds.Tables[0].Rows[nRowCnt]["WRITEOFF_DATE"]).Direction = ParameterDirection.Input;
                    Ins.Parameters.Add("REMARKS_IN", ds.Tables[0].Rows[nRowCnt]["REMARKS"]).Direction = ParameterDirection.Input;
                    Ins.Parameters.Add("USER_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                    //Snigdharani - 04/12/2008
                    Ins.Parameters.Add("BIZTYPE_IN", ds.Tables[0].Rows[nRowCnt]["BIZTYPE"]).Direction = ParameterDirection.Input;
                    Ins.Parameters.Add("PROCESS_IN", ds.Tables[0].Rows[nRowCnt]["PROCESS"]).Direction = ParameterDirection.Input;
                    Ins.Parameters.Add("PARTY_TYPE_IN", ds.Tables[0].Rows[nRowCnt]["PARTY_TYPE"]).Direction = ParameterDirection.Input;
                    Ins.ExecuteNonQuery();
                    if (string.IsNullOrEmpty(strPKs))
                    {
                        strPKs = Convert.ToString(ds.Tables[0].Rows[nRowCnt]["CONSOL_INVOICE_PK"]);
                    }
                    else
                    {
                        strPKs = strPKs + "," + ds.Tables[0].Rows[nRowCnt]["CONSOL_INVOICE_PK"];
                    }
                }
                if (arrMessage.Count > 0)
                {
                    INSERT.Rollback();
                    return arrMessage;
                }
                else
                {
                    arrMessage.Add("All Data Saved Successfully");
                    INSERT.Commit();
                    //Push to financial system if realtime is selected
                    if (!string.IsNullOrEmpty(strPKs))
                    {
                        Quantum_QFOR.cls_Scheduler objSch = new Quantum_QFOR.cls_Scheduler();
                        ArrayList schDtls = null;
                        bool errGen = false;
                        if (objSch.GetSchedulerPushType() == true)
                        {
                            //QFSIService.serFinApp objPush = new QFSIService.serFinApp();
                            //try {
                            //	schDtls = objSch.FetchSchDtls();
                            //	//'Used to Fetch the Sch Dtls
                            //	objPush.UpdateTaxElement(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen);
                            //	objPush.UpdatebankAcc(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen);
                            //	objPush.UpdateCurrency(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen);
                            //	objPush.UpdateTransaction(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen, strPKs);
                            //	if (ConfigurationSettings.AppSettings["EVENTVIEWER"]) {
                            //		objPush.EventViewer(1, 1, HttpContext.Current.Session["USER_PK"]);
                            //	}
                            //} catch (Exception ex) {
                            //	if (ConfigurationSettings.AppSettings["EVENTVIEWER"]) {
                            //		objPush.EventViewer(1, 2, HttpContext.Current.Session["USER_PK"]);
                            //	}
                            //}
                        }
                    }
                    //*****************************************************************
                    return arrMessage;
                }
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
                objWK.MyCommand.Connection.Close();
            }
        }

        #endregion "FOR WRITEOFF MANUAL"

        #region "Automatic Writoff"

        /// <summary>
        /// Automatics the write off.
        /// </summary>
        /// <returns></returns>
        public ArrayList AutomaticWriteOff()
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction INSERT = null;
            OracleCommand Ins = new OracleCommand();
            objWK.OpenConnection();
            INSERT = objWK.MyConnection.BeginTransaction();
            try
            {
                var _with6 = Ins;
                _with6.Transaction = INSERT;
                _with6.Connection = objWK.MyConnection;
                _with6.CommandType = CommandType.StoredProcedure;
                _with6.CommandText = objWK.MyUserName + ".WRITE_OFF_PKG.INS_WRITEOFF_AUTOMATIC";
                var _with7 = _with6.Parameters;
                _with7.Clear();
                _with7.Add("WRITE_OFF_LOC_FK_IN", HttpContext.Current.Session["LOGED_IN_LOC_FK"]).Direction = ParameterDirection.Input;
                _with7.Add("WRITE_OFF_USER_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                _with6.ExecuteNonQuery();
                arrMessage.Add("All Data Saved Successfully");
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return new ArrayList();
        }

        #endregion "Automatic Writoff"

        #region "Fetch Grid Details"

        /// <summary>
        /// Fetches the grid detais.
        /// </summary>
        /// <param name="txtLocPK">The text loc pk.</param>
        /// <param name="UserPK">The user pk.</param>
        /// <param name="LocFK">The loc fk.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <returns></returns>
        public DataSet FetchGridDetais(int txtLocPK = 0, int UserPK = 0, int LocFK = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {
            WorkFlow objWF = new WorkFlow();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            Int32 TotalRecords = default(Int32);
            string strSQL = null;
            try
            {
                sb.Append("SELECT DISTINCT '1' ACTIVE,");
                sb.Append("       WMT.WRITE_OFF_USER_FK,");
                sb.Append("       UMT.USER_ID,");
                sb.Append("       UMT.USER_NAME, ");
                sb.Append("       DEPT.DEPARTMENT_NAME,");
                sb.Append("       DESG.DESIGNATION_NAME,");
                sb.Append("       WMT.WRITE_OFF_LOC_FK,");
                sb.Append("       LMT.LOCATION_ID,");
                sb.Append("       ' ' CURRENCY_ID,");
                sb.Append("       WMT.VALID_FROM,");
                sb.Append("       WMT.VALID_TO,UMT.BUSINESS_TYPE");
                sb.Append("  FROM USER_MST_TBL          UMT,");
                sb.Append("       WRITEOFF_MST_TBL      WMT,");
                sb.Append("       LOCATION_MST_TBL      LMT,");
                sb.Append("       EMPLOYEE_MST_TBL      EMP,");
                sb.Append("       DEPARTMENT_MST_TBL    DEPT,");
                sb.Append("       DESIGNATION_MST_TBL   DESG");
                sb.Append(" WHERE UMT.USER_MST_PK = WMT.WRITE_OFF_USER_FK");
                sb.Append("   AND WMT.WRITE_OFF_LOC_FK = LMT.LOCATION_MST_PK");
                sb.Append("     AND UMT.EMPLOYEE_MST_FK = EMP.EMPLOYEE_MST_PK");
                sb.Append("   AND DEPT.DEPARTMENT_MST_PK = EMP.DEPARTMENT_MST_FK");
                sb.Append("   AND DESG.DESIGNATION_MST_PK = EMP.DESIGNATION_MST_FK");
                sb.Append("   AND LMT.LOCATION_MST_PK IN");
                sb.Append("       (SELECT DISTINCT LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL");
                sb.Append("         START WITH LOCATION_MST_PK = " + LocFK + "");
                sb.Append("        CONNECT BY PRIOR LOCATION_MST_PK = REPORTING_TO_FK)");
                if (txtLocPK != 0)
                {
                    sb.Append("   AND WMT.WRITE_OFF_LOC_FK=" + txtLocPK + "");
                }
                if (UserPK != 0)
                {
                    sb.Append("  AND  UMT.USER_MST_PK=" + UserPK + "");
                }
                strSQL = " select count(*) from (";
                strSQL += sb.ToString() + ")";
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
                strSQL = " SELECT * FROM (SELECT ROWNUM SR_NO, q.* FROM(";
                strSQL += sb.ToString();
                strSQL += " )q ) WHERE SR_NO Between " + start + " and " + last;
                return objWF.GetDataSet(strSQL);
            }
            catch (OracleException SqlExp)
            {
                throw SqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion "Fetch Grid Details"

        #region "Fecth WriteOff Details"

        /// <summary>
        /// Fetches the sea write off details.
        /// </summary>
        /// <param name="UserPK">The user pk.</param>
        /// <returns></returns>
        public object FetchSeaWriteOffDetails(int UserPK = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                sb.Append("SELECT WMT.WRITE_OFF_PK,");
                sb.Append("       EMP.EMPLOYEE_NAME,");
                sb.Append("       WMT.VALID_FROM,");
                sb.Append("       WMT.VALID_TO,");
                sb.Append("       WMT.WRITE_OFF_SEA_LIMIT,");
                sb.Append("       WMT.WRITE_OFF_SEA_CURR_FK,");
                sb.Append("       CMT.CURRENCY_ID");
                sb.Append("  FROM WRITEOFF_MST_TBL      WMT,");
                sb.Append("       USER_MST_TBL          UMT,");
                sb.Append("       EMPLOYEE_MST_TBL      EMP,");
                sb.Append("       CURRENCY_TYPE_MST_TBL CMT");
                sb.Append(" WHERE WMT.WRITE_OFF_USER_FK = UMT.USER_MST_PK");
                sb.Append("   AND UMT.EMPLOYEE_MST_FK = EMP.EMPLOYEE_MST_PK");
                sb.Append("   AND CMT.CURRENCY_MST_PK = WMT.WRITE_OFF_SEA_CURR_FK");
                sb.Append("   AND WMT.WRITE_OFF_USER_FK=" + UserPK + "");
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException SqlExp)
            {
                throw SqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        /// <summary>
        /// Fetches the air write off details.
        /// </summary>
        /// <param name="UserPK">The user pk.</param>
        /// <returns></returns>
        public object FetchAirWriteOffDetails(int UserPK = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                sb.Append("SELECT WMT.WRITE_OFF_PK,");
                sb.Append("       EMP.EMPLOYEE_NAME,");
                sb.Append("       WMT.VALID_FROM,");
                sb.Append("       WMT.VALID_TO,");
                sb.Append("       WMT.WRITE_OFF_AIR_LIMIT,");
                sb.Append("       WMT.WRITE_OFF_AIR_CURR_FK,");
                sb.Append("       CMT.CURRENCY_ID");
                sb.Append("  FROM WRITEOFF_MST_TBL      WMT,");
                sb.Append("       USER_MST_TBL          UMT,");
                sb.Append("       EMPLOYEE_MST_TBL      EMP,");
                sb.Append("       CURRENCY_TYPE_MST_TBL CMT");
                sb.Append(" WHERE WMT.WRITE_OFF_USER_FK = UMT.USER_MST_PK");
                sb.Append("   AND UMT.EMPLOYEE_MST_FK = EMP.EMPLOYEE_MST_PK");
                sb.Append("   AND CMT.CURRENCY_MST_PK = WMT.WRITE_OFF_AIR_CURR_FK");
                sb.Append("   AND WMT.WRITE_OFF_USER_FK=" + UserPK + "");
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException SqlExp)
            {
                throw SqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion "Fecth WriteOff Details"

        #region "FetchWriteOffAmt"

        /// <summary>
        /// Fetches the write off amt.
        /// </summary>
        /// <param name="usrLocFK">The usr loc fk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="process">The process.</param>
        /// <returns></returns>
        public DataSet FetchWriteOffAmt(long usrLocFK = 0, short BizType = 1, short process = 1)
        {
            System.Text.StringBuilder strCondition = new System.Text.StringBuilder();
            System.Text.StringBuilder str = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            System.Text.StringBuilder sb1 = new System.Text.StringBuilder();
            string condition1 = null;
            string Columns = null;
            string sqlA = null;
            string ApplyStr = null;
            Int32 Apply = default(Int32);
            string ApplyUsersPK = null;
            ApplyStr = "select nvl(j.apply_to_users,0) from writeoff_mst_tbl j where j.write_off_loc_fk = " + usrLocFK + "and j.write_off_user_fk = " + HttpContext.Current.Session["USER_PK"];
            ApplyUsersPK = Convert.ToString(objWF.ExecuteScaler(ApplyStr.ToString()));
            if (ApplyUsersPK == null)
            {
                ApplyUsersPK = " 0";
            }
            sqlA = "select WMT.APPLY_TO from writeoff_mst_tbl WMT WHERE  WMT.WRITE_OFF_USER_FK =" + HttpContext.Current.Session["USER_PK"] + "";
            Apply = Convert.ToInt32(objWF.ExecuteScaler(sqlA.ToString()));

            sb.Append(" SELECT  max(INV.CONSOL_INVOICE_PK) PK,");
            sb.Append(" INV.INVOICE_REF_NO,");
            sb.Append(" max(CMT.CUSTOMER_NAME), ");
            sb.Append(" max(INV.INVOICE_DATE),");
            sb.Append(" DECODE(INV.BUSINESS_TYPE, 1, 'Air', 2, 'Sea') BIZTYPE,");
            sb.Append(" DECODE(INV.PROCESS_TYPE, 1, 'Export', 2, 'Import') PROCESS,");
            sb.Append(" max(INV.NET_RECEIVABLE),");
            sb.Append(" (SELECT NVL(sum(c.recd_amount_hdr_curr), 0) ");
            sb.Append("                       FROM ");
            sb.Append("                             collections_trn_tbl c ");
            sb.Append("                       WHERE c.invoice_ref_nr=INV.INVOICE_REF_NO)");
            sb.Append("                       ");
            sb.Append("                       Recieved,");
            sb.Append(" NVL((max(inv.net_receivable) - ");
            sb.Append("   (SELECT NVL(sum(c.recd_amount_hdr_curr), 0) ");
            sb.Append("   FROM collections_trn_tbl c ");
            sb.Append("   WHERE c.invoice_ref_nr=INV.INVOICE_REF_NO)");
            sb.Append(" -NVL(max(WRM.WRITEOFF_AMOUNT), '0.00')),'0.00') Balance,");
            sb.Append(" NVL(max(WRM.WRITEOFF_AMOUNT),'0.00') amount,NVL(max(TO_CHAR(WRM.WRITEOFF_DATE,'DD/MM/YYYY')), '') Idate,");
            sb.Append(" decode(max(WRM.REMARKS),'0','','1','Excess Charge'),'' Sel, inv.process_type, inv.business_type");
            sb.Append(" FROM");
            sb.Append(" CONSOL_INVOICE_TBL INV,CONSOL_INVOICE_TRN_TBL INVTRN, ");
            sb.Append("writeoff_manual_tbl WRM,");
            if (Apply == 2)
            {
                sb.Append(" WRITEOFF_MST_TBL WMT,");
            }
            Columns = sb.ToString();

            sb1.Append(" AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK(+)");
            sb1.Append(" and cmt.customer_mst_pk = inv.customer_mst_fk");
            sb1.Append(" AND wrm.consol_invoice_fk(+)=INV.CONSOL_INVOICE_PK");
            sb1.Append("     AND INV.CREATED_BY_FK IN");
            sb1.Append("     (SELECT UMT.USER_MST_PK");
            sb1.Append("     FROM USER_MST_TBL UMT");
            sb1.Append("     WHERE UMT.DEFAULT_LOCATION_FK =");
            sb1.Append("     (SELECT U.DEFAULT_LOCATION_FK");
            sb1.Append("     FROM USER_MST_TBL U");
            sb1.Append("     WHERE U.USER_MST_PK =" + HttpContext.Current.Session["USER_PK"] + "))");
            if (Apply == 2)
            {
                sb1.Append(" And instr(','||wmt.apply_to_users||',',','||CMT.customer_mst_pk||',') > 0");
            }
            sb1.Append(" GROUP BY");
            sb1.Append(" INV.INVOICE_REF_NO, inv.process_type, inv.business_type");
            condition1 = sb1.ToString();

            if (BizType == 2 & process == 1)
            {
                strCondition.Append("   " + Columns);
                strCondition.Append(" JOB_CARD_TRN   JOB,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT ");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+) ");
                strCondition.Append(" and NVL((INV.NET_RECEIVABLE -NVL((SELECT NVL(sum(c.recd_amount_hdr_curr), 0)");
                strCondition.Append(" FROM collections_trn_tbl c ");
                strCondition.Append(" WHERE c.invoice_ref_nr = INV.INVOICE_REF_NO), 0.00)),0)");
                strCondition.Append(" <=(select w.sea_bal_amt from writeoff_mst_tbl w where");
                strCondition.Append(" (w.apply_to_users = '0' or cmt.customer_mst_pk");
                strCondition.Append(" in (" + ApplyUsersPK + "))and w.write_off_loc_fk =" + usrLocFK + " and w.write_off_user_fk = " + HttpContext.Current.Session["USER_PK"] + ")");
                strCondition.Append(" and ((select floor(sysdate - INV.INVOICE_DATE) from dual) >=");
                strCondition.Append(" (select w.write_off_sea_cr_days");
                strCondition.Append(" from writeoff_mst_tbl w");
                strCondition.Append(" where (w.apply_to_users = '0' or");
                strCondition.Append(" cmt.customer_mst_pk in (" + ApplyUsersPK + "))");
                strCondition.Append(" and w.write_off_loc_fk =" + usrLocFK + " and w.write_off_user_fk = " + HttpContext.Current.Session["USER_PK"] + "))");
                strCondition.Append(" AND INV.PROCESS_TYPE =" + process);
                strCondition.Append(" AND INV.BUSINESS_TYPE =" + BizType);
                strCondition.Append(" " + condition1);
            }
            else if (BizType == 1 & process == 1)
            {
                strCondition.Append("   " + Columns);
                strCondition.Append(" JOB_CARD_TRN   JOB,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                //If Apply <> 2 Then
                strCondition.Append(" and ((select floor(sysdate-INV.INVOICE_DATE) from dual) >= ");
                strCondition.Append("(select w.write_off_sea_cr_days from writeoff_mst_tbl");
                strCondition.Append(" w where (w.apply_to_users = '0' or cmt.customer_mst_pk");
                strCondition.Append(" in (" + ApplyUsersPK + ")) and");
                strCondition.Append(" w.write_off_loc_fk = " + usrLocFK + " and w.write_off_user_fk = " + HttpContext.Current.Session["USER_PK"] + "))");
                strCondition.Append(" and NVL((INV.NET_RECEIVABLE -NVL((SELECT NVL(sum(c.recd_amount_hdr_curr), 0)");
                strCondition.Append(" FROM collections_trn_tbl c ");
                strCondition.Append(" WHERE c.invoice_ref_nr = INV.INVOICE_REF_NO), 0.00)),0)");
                strCondition.Append(" <=(select w.air_bal_amt from writeoff_mst_tbl");
                strCondition.Append(" w where (w.apply_to_users = '0' or cmt.customer_mst_pk");
                strCondition.Append(" in (" + ApplyUsersPK + "))and w.write_off_loc_fk = " + usrLocFK + " and w.write_off_user_fk = " + HttpContext.Current.Session["USER_PK"] + ")");
                strCondition.Append(" AND INV.PROCESS_TYPE =" + process);
                strCondition.Append(" AND INV.BUSINESS_TYPE =" + BizType);
                strCondition.Append(" " + condition1);

                //Sea Import
            }
            else if (BizType == 2 & process == 2)
            {
                strCondition.Append("   " + Columns);
                strCondition.Append(" JOB_CARD_TRN   JOB,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition.Append(" VESSEL_VOYAGE_TRN      VTRN");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                strCondition.Append(" AND JOB.VOYAGE_TRN_FK=VTRN.VOYAGE_TRN_PK(+)");
                //If Apply <> 2 Then
                strCondition.Append(" and ((select floor(sysdate-INV.INVOICE_DATE) from dual) >= ");
                strCondition.Append("(select w.write_off_sea_cr_days from writeoff_mst_tbl");
                strCondition.Append(" w where (w.apply_to_users = 0 or cmt.customer_mst_pk");
                strCondition.Append(" in (" + ApplyUsersPK + ")) and ");
                strCondition.Append(" w.write_off_loc_fk = " + usrLocFK + " and w.write_off_user_fk = " + HttpContext.Current.Session["USER_PK"] + "))");
                strCondition.Append(" and NVL((INV.NET_RECEIVABLE -NVL((SELECT NVL(sum(c.recd_amount_hdr_curr), 0)");
                strCondition.Append(" FROM collections_trn_tbl c ");
                strCondition.Append(" WHERE c.invoice_ref_nr = INV.INVOICE_REF_NO), 0.00)),0)");
                strCondition.Append("  <=(select w.sea_bal_amt from writeoff_mst_tbl");
                strCondition.Append(" w where (w.apply_to_users = 0 or cmt.customer_mst_pk");
                strCondition.Append(" in (" + ApplyUsersPK + "))and w.write_off_loc_fk = " + usrLocFK + " and w.write_off_user_fk = " + HttpContext.Current.Session["USER_PK"] + ")");
                strCondition.Append(" AND INV.PROCESS_TYPE =" + process);
                strCondition.Append(" AND INV.BUSINESS_TYPE =" + BizType);
                strCondition.Append(" " + condition1);

                //Air Import
            }
            else if (BizType == 1 & process == 2)
            {
                strCondition.Append("   " + Columns);
                strCondition.Append(" JOB_CARD_TRN   JOB,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                //If Apply <> 2 Then
                strCondition.Append(" and ((select floor(sysdate-INV.INVOICE_DATE) from dual) >= ");
                strCondition.Append("(select w.write_off_sea_cr_days from writeoff_mst_tbl");
                strCondition.Append(" w where (w.apply_to_users = 0 or cmt.customer_mst_pk");
                strCondition.Append(" in (" + ApplyUsersPK + ")) and ");
                strCondition.Append(" w.write_off_loc_fk = " + usrLocFK + " and w.write_off_user_fk = " + HttpContext.Current.Session["USER_PK"] + "))");
                strCondition.Append(" and NVL((INV.NET_RECEIVABLE -NVL((SELECT NVL(sum(c.recd_amount_hdr_curr), 0)");
                strCondition.Append(" FROM collections_trn_tbl c ");
                strCondition.Append(" WHERE c.invoice_ref_nr = INV.INVOICE_REF_NO), 0.00)),0)");
                strCondition.Append("  <=(select w.air_bal_amt ");
                strCondition.Append(" from writeoff_mst_tbl w ");
                strCondition.Append(" where (w.apply_to_users = 0 or cmt.customer_mst_pk ");
                strCondition.Append(" in (" + ApplyUsersPK + "))and w.write_off_loc_fk = " + usrLocFK + " and w.write_off_user_fk = " + HttpContext.Current.Session["USER_PK"] + ")");
                strCondition.Append(" AND INV.PROCESS_TYPE =" + process);
                strCondition.Append(" AND INV.BUSINESS_TYPE =" + BizType);
                strCondition.Append(" " + condition1);
                // End If
            }
            else
            {
                strCondition.Append("   " + Columns);
                strCondition.Append(" JOB_CARD_TRN   JOB,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT ");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+) ");
                strCondition.Append(" and NVL((INV.NET_RECEIVABLE -NVL((SELECT NVL(sum(c.recd_amount_hdr_curr), 0)");
                strCondition.Append(" FROM collections_trn_tbl c ");
                strCondition.Append(" WHERE c.invoice_ref_nr = INV.INVOICE_REF_NO), 0.00)),0)");
                strCondition.Append(" <=(select w.sea_bal_amt from writeoff_mst_tbl w where");
                strCondition.Append(" (w.apply_to_users = '0' or cmt.customer_mst_pk");
                strCondition.Append(" in (" + ApplyUsersPK + "))and w.write_off_loc_fk =" + usrLocFK + " and w.write_off_user_fk = " + HttpContext.Current.Session["USER_PK"] + ")");
                strCondition.Append(" and ((select floor(sysdate - INV.INVOICE_DATE) from dual) >=");
                strCondition.Append(" (select w.write_off_sea_cr_days");
                strCondition.Append(" from writeoff_mst_tbl w");
                strCondition.Append(" where (w.apply_to_users = '0' or");
                strCondition.Append(" cmt.customer_mst_pk in (" + ApplyUsersPK + "))");
                strCondition.Append(" and w.write_off_loc_fk =" + usrLocFK + " and w.write_off_user_fk = " + HttpContext.Current.Session["USER_PK"] + "))");
                strCondition.Append(" AND INV.PROCESS_TYPE =1");
                strCondition.Append(" AND INV.BUSINESS_TYPE =2");
                strCondition.Append(" " + condition1);

                strCondition.Append(" union");

                strCondition.Append("   " + Columns);
                strCondition.Append(" JOB_CARD_TRN   JOB,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                //If Apply <> 2 Then
                strCondition.Append(" and ((select floor(sysdate-INV.INVOICE_DATE) from dual) >= ");
                strCondition.Append("(select w.write_off_sea_cr_days from writeoff_mst_tbl");
                strCondition.Append(" w where (w.apply_to_users = '0' or cmt.customer_mst_pk");
                strCondition.Append(" in (" + ApplyUsersPK + ")) and");
                strCondition.Append(" w.write_off_loc_fk = " + usrLocFK + " and w.write_off_user_fk = " + HttpContext.Current.Session["USER_PK"] + "))");
                strCondition.Append(" and NVL((INV.NET_RECEIVABLE -NVL((SELECT NVL(sum(c.recd_amount_hdr_curr), 0)");
                strCondition.Append(" FROM collections_trn_tbl c ");
                strCondition.Append(" WHERE c.invoice_ref_nr = INV.INVOICE_REF_NO), 0.00)),0)");
                strCondition.Append(" <=(select w.air_bal_amt from writeoff_mst_tbl");
                strCondition.Append(" w where (w.apply_to_users = '0' or cmt.customer_mst_pk");
                strCondition.Append(" in (" + ApplyUsersPK + "))and w.write_off_loc_fk = " + usrLocFK + " and w.write_off_user_fk = " + HttpContext.Current.Session["USER_PK"] + ")");
                strCondition.Append(" AND INV.PROCESS_TYPE =1");
                strCondition.Append(" AND INV.BUSINESS_TYPE =1");
                strCondition.Append(" " + condition1);

                strCondition.Append(" union");

                strCondition.Append("   " + Columns);
                strCondition.Append(" JOB_CARD_TRN   JOB,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition.Append(" VESSEL_VOYAGE_TRN      VTRN");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                strCondition.Append(" AND JOB.VOYAGE_TRN_FK=VTRN.VOYAGE_TRN_PK(+)");
                //If Apply <> 2 Then
                strCondition.Append(" and ((select floor(sysdate-INV.INVOICE_DATE) from dual) >= ");
                strCondition.Append("(select w.write_off_sea_cr_days from writeoff_mst_tbl");
                strCondition.Append(" w where (w.apply_to_users = 0 or cmt.customer_mst_pk");
                strCondition.Append(" in (" + ApplyUsersPK + ")) and ");
                strCondition.Append(" w.write_off_loc_fk = " + usrLocFK + " and w.write_off_user_fk = " + HttpContext.Current.Session["USER_PK"] + "))");
                strCondition.Append(" and NVL((INV.NET_RECEIVABLE -NVL((SELECT NVL(sum(c.recd_amount_hdr_curr), 0)");
                strCondition.Append(" FROM collections_trn_tbl c ");
                strCondition.Append(" WHERE c.invoice_ref_nr = INV.INVOICE_REF_NO), 0.00)),0)");
                strCondition.Append("  <=(select w.sea_bal_amt from writeoff_mst_tbl");
                strCondition.Append(" w where (w.apply_to_users = 0 or cmt.customer_mst_pk");
                strCondition.Append(" in (" + ApplyUsersPK + "))and w.write_off_loc_fk = " + usrLocFK + " and w.write_off_user_fk = " + HttpContext.Current.Session["USER_PK"] + ")");
                strCondition.Append(" AND INV.PROCESS_TYPE =2");
                strCondition.Append(" AND INV.BUSINESS_TYPE =2");
                strCondition.Append(" " + condition1);

                strCondition.Append(" union");

                strCondition.Append("   " + Columns);
                strCondition.Append(" JOB_CARD_TRN   JOB,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                //If Apply <> 2 Then
                strCondition.Append(" and ((select floor(sysdate-INV.INVOICE_DATE) from dual) >= ");
                strCondition.Append("(select w.write_off_sea_cr_days from writeoff_mst_tbl");
                strCondition.Append(" w where (w.apply_to_users = 0 or cmt.customer_mst_pk");
                strCondition.Append(" in (" + ApplyUsersPK + ")) and ");
                strCondition.Append(" w.write_off_loc_fk = " + usrLocFK + " and w.write_off_user_fk = " + HttpContext.Current.Session["USER_PK"] + "))");
                strCondition.Append(" and NVL((INV.NET_RECEIVABLE -NVL((SELECT NVL(sum(c.recd_amount_hdr_curr), 0)");
                strCondition.Append(" FROM collections_trn_tbl c ");
                strCondition.Append(" WHERE c.invoice_ref_nr = INV.INVOICE_REF_NO), 0.00)),0)");
                strCondition.Append("  <=(select w.air_bal_amt ");
                strCondition.Append(" from writeoff_mst_tbl w ");
                strCondition.Append(" where (w.apply_to_users = 0 or cmt.customer_mst_pk ");
                strCondition.Append(" in (" + ApplyUsersPK + "))and w.write_off_loc_fk = " + usrLocFK + " and w.write_off_user_fk = " + HttpContext.Current.Session["USER_PK"] + ")");
                strCondition.Append(" AND INV.PROCESS_TYPE =2");
                strCondition.Append(" AND INV.BUSINESS_TYPE =1");
                strCondition.Append(" " + condition1);
            }

            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append("SELECT SUM(QRY.AMOUNT) WRITEOFFAMT FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + strCondition.ToString() + " ");
            sqlstr.Append("  ) T");
            if (BizType != 0)
            {
                sqlstr.Append(" WHERE BUSINESS_TYPE='" + BizType + "'");
            }
            if (BizType != 0 & process != 0)
            {
                sqlstr.Append(" AND PROCESS_TYPE='" + process + "'");
            }
            else if (process != 0)
            {
                sqlstr.Append(" WHERE PROCESS_TYPE='" + process + "'");
            }
            sqlstr.Append(" ) QRY  ");
            sqlstr.Append("    WHERE ");
            sqlstr.Append("   QRY.AMOUNT <> 0");

            string sql = null;
            sql = sqlstr.ToString();
            DataSet DS = null;
            try
            {
                return objWF.GetDataSet(sql);
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

        #endregion "FetchWriteOffAmt"

        #region "Save Invoice Amount"

        /// <summary>
        /// Saves the invoice amt.
        /// </summary>
        /// <param name="InvoicePK">The invoice pk.</param>
        /// <param name="PartyType">Type of the party.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <returns></returns>
        public ArrayList SaveInvoiceAmt(int InvoicePK, int PartyType = 0, int BizType = 0, int ProcessType = 0)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            string str = null;
            string str1 = null;
            Int16 intIns = default(Int16);
            Int16 cnt = default(Int16);
            Int32 RowsAft = default(Int32);
            OracleCommand updCmdUser = new OracleCommand();
            OracleCommand UpdCommandUser = new OracleCommand();
            OracleCommand UP = new OracleCommand();
            TRAN = objWK.MyConnection.BeginTransaction();
            try
            {
                updCmdUser.Transaction = TRAN;
                UpdCommandUser.Transaction = TRAN;
                //0-Customers Invoice,2-Vendors Invoice
                if (PartyType == 0 | PartyType == 2)
                {
                    str = "UPDATE  CONSOL_INVOICE_TBL INV SET ";
                    str += " INV.CHK_INVOICE = 1 ";
                    str += " WHERE INV.CONSOL_INVOICE_PK =" + InvoicePK;
                    //1-Agents (Sea Exp Invoice)
                }
                else if (PartyType == 1 & BizType == 2 & ProcessType == 1)
                {
                    str = "UPDATE  INV_AGENT_TBL INV SET ";
                    str += " INV.CHK_INVOICE = 1 ";
                    str += " WHERE INV.INV_AGENT_PK =" + InvoicePK;
                    //1-Agents (Sea Imp Invoice)
                }
                else if (PartyType == 1 & BizType == 2 & ProcessType == 2)
                {
                    str = "UPDATE  INV_AGENT_TBL INV SET ";
                    str += " INV.CHK_INVOICE = 1 ";
                    str += " WHERE INV.INV_AGENT_PK =" + InvoicePK;
                    //1-Agents (Air Exp Invoice)
                }
                else if (PartyType == 1 & BizType == 1 & ProcessType == 1)
                {
                    str = "UPDATE  INV_AGENT_TBL INV SET ";
                    str += " INV.CHK_INVOICE = 1 ";
                    str += " WHERE INV.INV_AGENT_PK =" + InvoicePK;
                    //1-Agents (Air Imp Invoice)
                }
                else if (PartyType == 1 & BizType == 1 & ProcessType == 2)
                {
                    str = "UPDATE  INV_AGENT_TBL INV SET ";
                    str += " INV.CHK_INVOICE = 1 ";
                    str += " WHERE INV.INV_AGENT_PK =" + InvoicePK;
                }

                var _with8 = updCmdUser;
                _with8.Connection = objWK.MyConnection;
                _with8.Transaction = TRAN;
                _with8.CommandType = CommandType.Text;
                _with8.CommandText = str;
                intIns = Convert.ToInt16(_with8.ExecuteNonQuery());

                var _with9 = UP;
                _with9.Transaction = TRAN;
                _with9.Connection = objWK.MyConnection;
                _with9.CommandType = CommandType.StoredProcedure;
                _with9.CommandText = objWK.MyUserName + ".FIN_APP_DML_PKG.FIN_APP_TXNS_INS";

                var _with10 = _with9.Parameters;
                _with10.Clear();
                _with10.Add("FIN_APP_TXNS_FK_IN", InvoicePK).Direction = ParameterDirection.Input;
                _with10.Add("FIN_APP_TXN_TYPE_IN", 6).Direction = ParameterDirection.Input;
                _with10.Add("FIN_APP_TXN_CATEGORY_IN", (PartyType == 0 ? 16 : (PartyType == 1 ? 17 : 18))).Direction = ParameterDirection.Input;
                _with10.Add("FIN_APP_CUST_TYPE_IN", (PartyType == 0 ? 1 : (PartyType == 1 ? 3 : 2))).Direction = ParameterDirection.Input;
                _with10.Add("FIN_APP_PRODUCT_IN", "QFOR").Direction = ParameterDirection.Input;
                _with10.Add("FIN_APP_SOURCE_IN", "WRITEOFF_MANUAL_TBL").Direction = ParameterDirection.Input;
                _with10.Add("FIN_APP_BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with10.Add("FIN_APP_PROCESS_TYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                cnt = Convert.ToInt16(UP.ExecuteNonQuery());

                str1 = "DELETE  writeoff_manual_tbl wmt where wmt.consol_invoice_fk =" + InvoicePK;
                var _with11 = updCmdUser;
                _with11.Connection = objWK.MyConnection;
                _with11.Transaction = TRAN;
                _with11.CommandType = CommandType.Text;
                _with11.CommandText = str1;
                RowsAft = _with11.ExecuteNonQuery();
                if (intIns > 0 & RowsAft > 0 & cnt > 0)
                {
                    TRAN.Commit();
                    arrMessage.Add("All data Saved Successfully");
                    //Push to financial system if realtime is selected
                    if (InvoicePK > 0)
                    {
                        Quantum_QFOR.cls_Scheduler objSch = new Quantum_QFOR.cls_Scheduler();
                        ArrayList schDtls = null;
                        bool errGen = false;
                        if (objSch.GetSchedulerPushType() == true)
                        {
                            //QFSIService.serFinApp objPush = new QFSIService.serFinApp();
                            //try {
                            //	schDtls = objSch.FetchSchDtls();
                            //	//'Used to Fetch the Sch Dtls
                            //	objPush.UpdateTaxElement(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen);
                            //	objPush.UpdatebankAcc(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen);
                            //	objPush.UpdateCurrency(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen);
                            //	objPush.UpdateTransaction(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen, Convert.ToString(InvoicePK));
                            //	if (ConfigurationSettings.AppSettings["EVENTVIEWER"]) {
                            //		objPush.EventViewer(1, 1, HttpContext.Current.Session["USER_PK"]);
                            //	}
                            //} catch (Exception ex) {
                            //	if (ConfigurationSettings.AppSettings["EVENTVIEWER"]) {
                            //		objPush.EventViewer(1, 2, HttpContext.Current.Session["USER_PK"]);
                            //	}
                            //}
                        }
                    }
                    //*****************************************************************
                    return arrMessage;
                }
            }
            catch (OracleException OraEx)
            {
                TRAN.Rollback();
                arrMessage.Add(OraEx.Message);
                return arrMessage;
            }
            finally
            {
                objWK.CloseConnection();
            }
            return new ArrayList();
        }

        #endregion "Save Invoice Amount"

        #region "Get Write Off Amount"

        /// <summary>
        /// Gets the write off amt.
        /// </summary>
        /// <param name="InvoicePK">The invoice pk.</param>
        /// <returns></returns>
        public double GetWriteOffAmt(int InvoicePK)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT WMT.WRITEOFF_AMOUNT");
            sb.Append("  FROM WRITEOFF_MANUAL_TBL WMT");
            sb.Append(" WHERE WMT.CONSOL_INVOICE_FK = " + InvoicePK + "");
            try
            {
                return Convert.ToDouble(objWF.ExecuteScaler(sb.ToString()));
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

        #endregion "Get Write Off Amount"
    }
}