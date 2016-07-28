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

using Oracle.ManagedDataAccess.Client;
using System;
using System.Data;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_Currency_Type_Mst_Tbl : CommonFeatures
    {
        #region "List of Members of the Class"

        /// <summary>
        /// The m_ currency_ MST_ pk
        /// </summary>
        private Int64 M_Currency_Mst_Pk;

        /// <summary>
        /// The m_ currency_ identifier
        /// </summary>
        private string M_Currency_Id;

        /// <summary>
        /// The m_ currency_ name
        /// </summary>
        private string M_Currency_Name;

        /// <summary>
        /// The m_ created_ date
        /// </summary>
        private string M_Created_Date;

        #endregion "List of Members of the Class"

        /// <summary>
        /// The m_ data set
        /// </summary>
        private DataSet M_DataSet;

        #region "List of Properties"

        /// <summary>
        /// Gets or sets my data set.
        /// </summary>
        /// <value>
        /// My data set.
        /// </value>
        public DataSet MyDataSet
        {
            get { return M_DataSet; }
            set { M_DataSet = value; }
        }

        /// <summary>
        /// Gets or sets the currency_ MST_ pk.
        /// </summary>
        /// <value>
        /// The currency_ MST_ pk.
        /// </value>
        public Int64 Currency_Mst_Pk
        {
            get { return M_Currency_Mst_Pk; }
            set { M_Currency_Mst_Pk = value; }
        }

        /// <summary>
        /// Gets or sets the currency_ identifier.
        /// </summary>
        /// <value>
        /// The currency_ identifier.
        /// </value>
        public string Currency_Id
        {
            get { return M_Currency_Id; }
            set { M_Currency_Id = value; }
        }

        /// <summary>
        /// Gets or sets the name of the currency_.
        /// </summary>
        /// <value>
        /// The name of the currency_.
        /// </value>
        public string Currency_Name
        {
            get { return M_Currency_Name; }
            set { M_Currency_Name = value; }
        }

        /// <summary>
        /// Gets or sets the created_ date.
        /// </summary>
        /// <value>
        /// The created_ date.
        /// </value>
        public string Created_Date
        {
            get { return M_Created_Date; }
            set { M_Created_Date = value; }
        }

        #endregion "List of Properties"

        #region "Constructors"

        /// <summary>
        /// Initializes a new instance of the <see cref="cls_Currency_Type_Mst_Tbl"/> class.
        /// </summary>
        public cls_Currency_Type_Mst_Tbl()
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            strSQL = "SELECT Currency_Mst_Pk,";
            strSQL += "Currency_Id,";
            strSQL += "Currency_Name,";
            strSQL += "CREATED_BY_FK,";
            strSQL += "CREATED_DT,";
            strSQL += "LAST_MODIFIED_BY_FK,";
            strSQL += "LAST_MODIFIED_DT,";
            strSQL += "VERSION_NO";
            strSQL += " FROM CURRENCY_TYPE_MST_TBL";
            strSQL += " WHERE ACTIVE_FLAG =1";
            //Condition added by Akhilesh as only active currencies to be listed.
            strSQL += " ORDER BY Currency_Id";
            // Added by soman.
            try
            {
                M_DataSet = objWF.GetDataSet(strSQL);
                //Manjunath  PTS ID:Sep-02  14/09/2011
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

        /// <summary>
        /// Initializes a new instance of the <see cref="cls_Currency_Type_Mst_Tbl"/> class.
        /// </summary>
        /// <param name="CurrencyID">The currency identifier.</param>
        /// <param name="CurrencyName">Name of the currency.</param>
        /// <param name="Currency_Pk">The currency_ pk.</param>
        public cls_Currency_Type_Mst_Tbl(string CurrencyID, string CurrencyName = "", Int64 Currency_Pk = 0)
        {
            string strSQL = null;
            strSQL = "SELECT Currency_Mst_Pk,";
            strSQL += "Currency_Id,";
            strSQL += "Currency_Name,";
            strSQL += "CREATED_BY_FK,";
            strSQL += "CREATED_DT,";
            strSQL += "LAST_MODIFIED_BY_FK,";
            strSQL += "LAST_MODIFIED_DT,";
            strSQL += "VERSION_NO";
            strSQL += " FROM CURRENCY_TYPE_MST_TBL WHERE 1= 1 ";
            if (Currency_Pk > 0)
            {
                strSQL += "and Currency_Mst_Pk = " + Currency_Pk + " ";
            }
            if ((Currency_Id != null))
            {
                if (Currency_Id.Trim().Length > 0)
                    strSQL += "  and CURRENCY_ID = '" + Currency_Id + "'";
            }
            if ((CurrencyName != null))
            {
                if (CurrencyName.Trim().Length > 0)
                {
                    strSQL += " AND UPPER(CURRENCY_NAME) LIKE '" + CurrencyName.Trim().ToUpper() + "%'";
                }
            }
            try
            {
                M_DataSet = (new WorkFlow()).GetDataSet(strSQL);
                //Manjunath  PTS ID:Sep-02  14/09/2011
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

        #endregion "Constructors"

        #region "Fetch All"

        /// <summary>
        /// Automatics the text.
        /// </summary>
        /// <returns></returns>
        public string AutoText()
        {
            WorkFlow objWF = new WorkFlow();
            DataSet dss = objWF.GetDataSet("select currency_id from currency_type_mst_tbl");
            string Str = "";
            if (dss.Tables[0].Rows.Count > 0)
            {
                for (int i = 0; i <= dss.Tables[0].Rows.Count - 1; i++)
                {
                    if (i == 0)
                    {
                        Str = "\"" + dss.Tables[0].Rows[i][0] + "\"";
                    }
                    else
                    {
                        Str = Str + "," + "\"" + dss.Tables[0].Rows[i][0] + "\"";
                    }
                }
            }
            return Str;
        }

        /// <summary>
        /// Automatics the text_ desc.
        /// </summary>
        /// <returns></returns>
        public string AutoText_Desc()
        {
            WorkFlow objWF = new WorkFlow();
            DataSet dss = objWF.GetDataSet("select currency_name from currency_type_mst_tbl order by currency_name asc");
            string Str = "";
            if (dss.Tables[0].Rows.Count > 0)
            {
                for (int i = 0; i <= dss.Tables[0].Rows.Count - 1; i++)
                {
                    if (i == 0)
                    {
                        Str = "\"" + dss.Tables[0].Rows[i][0] + "\"";
                    }
                    else
                    {
                        Str = Str + "," + "\"" + dss.Tables[0].Rows[i][0] + "\"";
                    }
                }
            }
            return Str;
        }

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="CurrencyPK">The currency pk.</param>
        /// <param name="CurrencyID">The currency identifier.</param>
        /// <param name="CurrencyName">Name of the currency.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="strColumnName">Name of the string column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="ActiveFlag">The active flag.</param>
        /// <param name="blnSortAscending">if set to <c>true</c> [BLN sort ascending].</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public DataSet FetchAll(Int16 CurrencyPK = 0, string CurrencyID = "", string CurrencyName = "", string SearchType = "", string strColumnName = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, int ActiveFlag = 1, bool blnSortAscending = false, Int32 flag = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            if (flag == 0)
            {
                strCondition += " AND 1=2";
            }

            if (CurrencyPK > 0)
            {
                strCondition += " AND CURRENCY_MST_PK=" + CurrencyPK;
            }

            if (CurrencyID.Trim().Length > 0)
            {
                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition += " AND UPPER(CURRENCY_ID) LIKE '" + CurrencyID.ToUpper().Replace("'", "''") + "%'";
                    }
                    else
                    {
                        strCondition += " AND UPPER(CURRENCY_ID) LIKE '%" + CurrencyID.ToUpper().Replace("'", "''") + "%'";
                    }
                }
                else
                {
                    strCondition += " AND UPPER(CURRENCY_ID) LIKE '%" + CurrencyID.ToUpper().Replace("'", "''") + "%'";
                }
            }

            if (CurrencyName.Trim().Length > 0)
            {
                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition += " AND UPPER(CURRENCY_NAME) LIKE '" + CurrencyName.ToUpper().Replace("'", "''") + "%'";
                    }
                    else
                    {
                        strCondition += " AND UPPER(CURRENCY_NAME) LIKE '%" + CurrencyName.ToUpper().Replace("'", "''") + "%'";
                    }
                }
                else
                {
                    strCondition += " AND UPPER(CURRENCY_NAME) LIKE '%" + CurrencyName.ToUpper().Replace("'", "''") + "%'";
                }
            }

            if (ActiveFlag == 1)
            {
                strCondition += " AND ACTIVE_FLAG = 1 ";
            }
            else
            {
                strCondition += "";
            }

            strSQL = "SELECT Count(*) from CURRENCY_TYPE_MST_TBL where 1=1";
            strSQL += strCondition;
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

            strSQL = " select * from (";
            strSQL += "SELECT ROWNUM SR_NO,q.* FROM ";
            strSQL += "(SELECT  ";
            strSQL += "CURRENCY_MST_PK, ";
            strSQL += "NVL(ACTIVE_FLAG,0) ACTIVE_FLAG , ";
            strSQL += "CURRENCY_ID, ";
            //strSQL &= vbCrLf & "Initcap(CURRENCY_NAME) CURRENCY_NAME, "
            strSQL += "CURRENCY_NAME CURRENCY_NAME, ";
            strSQL += "Version_No  ";
            strSQL += "FROM CURRENCY_TYPE_MST_TBL ";
            strSQL += "WHERE 1=1";

            strSQL += strCondition;

            if (!strColumnName.Equals("SR_NO"))
            {
                strSQL += "order by " + strColumnName;
            }

            if (!blnSortAscending & !strColumnName.Equals("SR_NO"))
            {
                strSQL += " DESC";
            }

            strSQL += ") q  ) WHERE SR_NO  Between " + start + " and " + last;

            try
            {
                return objWF.GetDataSet(strSQL);
                //Catch sqlExp As OracleException
                //Modified by Manjunath  PTS ID:Sep-02  14/09/2011
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

        #endregion "Fetch All"

        #region "Fetch Currency"

        /// <summary>
        /// Fetches the currency.
        /// </summary>
        /// <param name="CurrencyPK">The currency pk.</param>
        /// <param name="CurrencyID">The currency identifier.</param>
        /// <param name="CurrencyName">Name of the currency.</param>
        /// <param name="ActiveOnly">if set to <c>true</c> [active only].</param>
        /// <returns></returns>
        public DataSet FetchCurrency(Int16 CurrencyPK = 0, string CurrencyID = "", string CurrencyName = "", bool ActiveOnly = true)
        {
            string strSQL = null;
            strSQL = "select ' ' CURRENCY_ID,";
            strSQL = strSQL + "' ' CURRENCY_NAME, ";
            strSQL = strSQL + "0 CURRENCY_MST_PK ";
            strSQL = strSQL + "from CURRENCY_TYPE_MST_TBL ";
            strSQL = strSQL + "UNION ";
            strSQL = strSQL + "Select CURRENCY_ID, ";
            strSQL = strSQL + "CURRENCY_NAME,";
            strSQL = strSQL + "CURRENCY_MST_PK ";
            strSQL = strSQL + "from CURRENCY_TYPE_MST_TBL Where 1=1 ";
            if (CurrencyPK > 0)
            {
                strSQL = strSQL + " And CURRENCY_MST_PK =" + Convert.ToString(CurrencyPK);
            }
            if (ActiveOnly)
            {
                strSQL = strSQL + " And Active_Flag = 1  ";
            }
            //strSQL = strSQL & "order by CURRENCY_ID"
            strSQL = strSQL + "order by CURRENCY_NAME";
            WorkFlow objWF = new WorkFlow();
            try
            {
                return objWF.GetDataSet(strSQL);
                //Catch sqlExp As OracleException
                //Modified by Manjunath  PTS ID:Sep-02  14/09/2011
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

        #endregion "Fetch Currency"

        #region "GetBaseCurrency"

        /// <summary>
        /// Gets the base currency.
        /// </summary>
        /// <param name="LogLocation">The log location.</param>
        /// <param name="CurrencyID">The currency identifier.</param>
        /// <param name="CurrencyName">Name of the currency.</param>
        /// <returns></returns>
        public Int32 GetBaseCurrency(long LogLocation, string CurrencyID = "", string CurrencyName = "")
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();

            strSQL = "Select curr.currency_mst_pk,";
            strSQL += "curr.currency_ID,";
            strSQL += "curr.currency_Name";
            strSQL += " from location_mst_tbl loc,";
            strSQL += " country_mst_tbl c,currency_type_mst_tbl curr ";
            strSQL += " where ";
            strSQL += " loc.country_mst_fk=c.country_mst_pk";
            strSQL += " and c.currency_mst_fk=curr.currency_mst_pk";
            strSQL += " and loc.location_mst_pk = " + LogLocation;
            try
            {
                OracleDataReader dr = objWF.GetDataReader(strSQL);
                if (dr.Read())
                {
                    CurrencyID = Convert.ToString(dr["currency_id"]);
                    CurrencyName = Convert.ToString(dr["currency_Name"]);
                    return Convert.ToInt32(dr["Currency_Mst_Pk"]);
                }

                //Catch sqlExp As OracleException
                //Modified by Manjunath  PTS ID:Sep-02  14/09/2011
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
            return 0;
        }

        /// <summary>
        /// Gets the corp curr.
        /// </summary>
        /// <param name="strCurId">The string current identifier.</param>
        /// <param name="strCurName">Name of the string current.</param>
        /// <returns></returns>
        public Int32 GetCorpCurr(string strCurId, string strCurName)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            strBuilder.Append(" select  cmt.currency_mst_fk,cm.currency_id,cm.currency_name from corporate_mst_tbl cmt ,currency_type_mst_tbl cm ");
            strBuilder.Append(" where cm.currency_mst_pk = cmt.currency_mst_fk");
            try
            {
                OracleDataReader dr = objWF.GetDataReader(strBuilder.ToString());
                if (dr.Read())
                {
                    strCurId = Convert.ToString(dr["currency_id"]);
                    strCurName = Convert.ToString(dr["currency_name"]);
                    return Convert.ToInt32(dr["currency_mst_fk"]);
                }
                //Catch sqlExp As OracleException
                //Modified by Manjunath  PTS ID:Sep-02  14/09/2011
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
            return 0;
        }

        #endregion "GetBaseCurrency"

        #region "Get Active Base Currency"

        /// <summary>
        /// Gets the active base currency.
        /// </summary>
        /// <param name="ActiveCurr">The active curr.</param>
        /// <returns></returns>
        public DataSet GetActiveBaseCurrency(Int64 ActiveCurr = 0)
        {
            WorkFlow Objwk = new WorkFlow();
            string Strsql = null;

            try
            {
                Strsql = "select curr.currency_mst_pk,";
                Strsql += " curr.currency_id,curr.currency_name";
                Strsql += " from currency_type_mst_tbl curr";
                Strsql += " where curr.ACTIVE_FLAG=1";
                if (ActiveCurr > 0)
                {
                    Strsql += " AND CURRENCY_MST_PK <> " + ActiveCurr;
                }
                Strsql += " ORDER BY  curr.currency_id ";
                return Objwk.GetDataSet(Strsql);
                //Manjunath  PTS ID:Sep-02  14/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
        }

        #endregion "Get Active Base Currency"
    }
}