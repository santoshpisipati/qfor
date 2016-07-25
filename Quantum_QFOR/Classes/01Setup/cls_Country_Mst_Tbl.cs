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

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class clsCountry_Mst_Tbl : CommonFeatures
    {
        #region "List of Members of the Class"

        /// <summary>
        /// The m_ country_ MST_ pk
        /// </summary>
        private Int64 M_Country_Mst_Pk;

        /// <summary>
        /// The m_ country_ identifier
        /// </summary>
        private string M_Country_Id;

        #endregion "List of Members of the Class"

        /// <summary>
        /// The m_ country_ name
        /// </summary>
        private string M_Country_Name;

        #region "List of Properties"

        /// <summary>
        /// Gets or sets the country_ MST_ pk.
        /// </summary>
        /// <value>
        /// The country_ MST_ pk.
        /// </value>
        public Int64 Country_Mst_Pk
        {
            get { return M_Country_Mst_Pk; }
            set { M_Country_Mst_Pk = value; }
        }

        /// <summary>
        /// Gets or sets the country_ identifier.
        /// </summary>
        /// <value>
        /// The country_ identifier.
        /// </value>
        public string Country_Id
        {
            get { return M_Country_Id; }
            set { M_Country_Id = value; }
        }

        /// <summary>
        /// Gets or sets the name of the country_.
        /// </summary>
        /// <value>
        /// The name of the country_.
        /// </value>
        public string Country_Name
        {
            get { return M_Country_Name; }
            set { M_Country_Name = value; }
        }

        #endregion "List of Properties"

        #region "FetchAll Function"

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="CountryID">The country identifier.</param>
        /// <param name="CountryName">Name of the country.</param>
        /// <param name="AreaName">Name of the area.</param>
        /// <param name="CurrencyID">The currency identifier.</param>
        /// <param name="CurrencyName">Name of the currency.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="SortColumn">The sort column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="ActiveFlag">if set to <c>true</c> [active flag].</param>
        /// <param name="SortType">Type of the sort.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="Export">The export.</param>
        /// <returns></returns>
        public DataSet FetchAll(string CountryID = "", string CountryName = "", string AreaName = "", string CurrencyID = "", string CurrencyName = "", string SearchType = "", string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, bool ActiveFlag = true,
        string SortType = " ASC ", Int32 flag = 0, Int32 Export = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = "";
            string strCondition = "";
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            if (flag == 0)
            {
                strCondition += " AND 1=2";
            }
            if (SearchType == "C")
            {
                if (CountryID.Trim().Length > 0)
                {
                    strCondition = strCondition + " AND UPPER(COUNTRY_ID) LIKE '%" + CountryID.ToUpper().Replace("'", "''") + "%'";
                }
                if (CountryName.Trim().Length > 0)
                {
                    strCondition = strCondition + " AND UPPER(COUNTRY_NAME) LIKE '%" + CountryName.ToUpper().Replace("'", "''") + "%'";
                }
                if (AreaName.Trim().Length > 0)
                {
                    strCondition = strCondition + " AND UPPER(AREA_NAME) LIKE '%" + AreaName.ToUpper().Replace("'", "''") + "%'";
                }
                if (CurrencyID.Trim().Length > 0)
                {
                    strCondition = strCondition + " AND UPPER(CURRENCY_ID) LIKE '%" + CurrencyID.ToUpper().Replace("'", "''") + "%'";
                }
                if (CurrencyName.Trim().Length > 0)
                {
                    strCondition = strCondition + " AND UPPER(CURRENCY_NAME) LIKE '%" + CurrencyName.ToUpper().Replace("'", "''") + "%'";
                }
            }
            else
            {
                if (CountryID.Trim().Length > 0)
                {
                    strCondition = strCondition + " AND UPPER(COUNTRY_ID) LIKE '" + CountryID.ToUpper().Replace("'", "''") + "%'";
                }
                if (CountryName.Trim().Length > 0)
                {
                    strCondition = strCondition + " AND UPPER(COUNTRY_NAME) LIKE '" + CountryName.ToUpper().Replace("'", "''") + "%'";
                }
                if (AreaName.Trim().Length > 0)
                {
                    strCondition = strCondition + " AND UPPER(AREA_NAME) LIKE '" + AreaName.ToUpper().Replace("'", "''") + "%'";
                }
                if (CurrencyID.Trim().Length > 0)
                {
                    strCondition = strCondition + " AND UPPER(CURRENCY_ID) LIKE '" + CurrencyID.ToUpper().Replace("'", "''") + "%'";
                }
                if (CurrencyName.Trim().Length > 0)
                {
                    strCondition = strCondition + " AND UPPER(CURRENCY_NAME) LIKE '" + CurrencyName.ToUpper().Replace("'", "''") + "%'";
                }
            }

            if (ActiveFlag == true)
            {
                strCondition += " AND CN.ACTIVE_FLAG = 1 ";
            }
            else
            {
                strCondition += " ";
            }
            //If Sel = True Then
            //    strCondition &= vbCrLf & " AND CN = 1 "
            //End If
            strSQL = "SELECT Count(*) from COUNTRY_MST_TBL CN,CURRENCY_TYPE_MST_TBL CR, AREA_MST_TBL ";
            strSQL = strSQL + " WHERE CURRENCY_MST_PK(+) = CURRENCY_MST_FK AND AREA_MST_PK(+) = AREA_MST_FK ";
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

            strSQL = " select * from ( ";
            strSQL += "SELECT ROWNUM SR_NO,q.* FROM ";
            strSQL += "(SELECT ";
            strSQL += "COUNTRY_MST_PK, ";
            strSQL += "NVL(CN.ACTIVE_FLAG,0) ACTIVE_FLAG, ";
            strSQL += "COUNTRY_ID, ";
            strSQL += "COUNTRY_NAME, ";
            strSQL += "CN.AREA_MST_FK, ";
            strSQL += "UPPER(AMT.AREA_ID) AREA_ID, ";
            strSQL += "(AMT.AREA_NAME) AREA_NAME, ";
            strSQL += "CURRENCY_MST_FK, ";
            strSQL += "CURRENCY_ID, ";
            strSQL += "CURRENCY_NAME, ";
            strSQL += "CN.VERSION_NO, ";
            strSQL += "'' VATSetting, ";
            //strSQL &= vbCrLf & " NVL(CN.EU_NEU,0) European, " 'Sivachandran To apply Vat - Region wise
            strSQL += " DECODE(CN.EU_NEU,1,'TRUE',0,'FALSE') European, ";
            strSQL += "'' Sel,";
            strSQL += "'' TDSSETTING";
            strSQL += "FROM COUNTRY_MST_TBL CN, CURRENCY_TYPE_MST_TBL CR, AREA_MST_TBL AMT ";
            strSQL += "WHERE CURRENCY_MST_PK(+) = CURRENCY_MST_FK ";
            strSQL += " AND CN.AREA_MST_FK = AMT.AREA_MST_PK(+)";
            strSQL += strCondition;
            //sorting definition
            strSQL += " order by " + SortColumn + SortType + "  ";
            if (Export == 0)
            {
                strSQL += " ) q ) WHERE SR_NO  Between " + start + " and " + last;
            }
            else
            {
                strSQL += " ) q )";
            }
            try
            {
                return objWF.GetDataSet(strSQL);
                //Modified by Manjunath  PTS ID:Sep-02  13/09/2011
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

        #endregion "FetchAll Function"

        #region "Fetch Country Popup Function "

        // By Vimlesh QFOR
        // Added for Country Popup Udsed in Custom Status Code
        // this is using by country transaction
        /// <summary>
        /// Fetches the name of the country identifier.
        /// </summary>
        /// <param name="CountryPk">The country pk.</param>
        /// <param name="CountryID">The country identifier.</param>
        /// <param name="CountryName">Name of the country.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="ActiveFlag">if set to <c>true</c> [active flag].</param>
        /// <param name="SortType">Type of the sort.</param>
        /// <returns></returns>
        public DataSet FetchCountryIdName(string CountryPk = "", string CountryID = "", string CountryName = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, bool ActiveFlag = true, string SortType = " ASC ")
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = "";
            string strCondition = "";
            string strCondition1 = "";
            string strCondition2 = "";
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            if (CountryID == "Country ID")
            {
                if (CountryName.Trim().Length > 0)
                {
                    strCondition = strCondition + " AND UPPER(COUNTRY_ID) LIKE '%" + CountryName.ToUpper().Replace("'", "''") + "%'";
                }
            }
            else
            {
                if (CountryName.Trim().Length > 0)
                {
                    strCondition = strCondition + " AND UPPER(COUNTRY_NAME) LIKE '%" + CountryName.ToUpper().Replace("'", "''") + "%'";
                }
            }
            if (CountryPk != "null")
            {
                if (CountryPk.Length > 0)
                {
                    strCondition1 += "and COUNTRY_MST_PK not in (" + CountryPk + " )";
                    strCondition2 += "and COUNTRY_MST_PK in (" + CountryPk + " )";
                }
            }
            strCondition += " AND CN.ACTIVE_FLAG = 1 ";
            strSQL = "SELECT Count(*) from COUNTRY_MST_TBL CN ";
            strSQL = strSQL + " WHERE 1=1 ";
            strSQL += strCondition;
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
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
            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;

            strSQL = " select * from (";
            strSQL += "SELECT ROWNUM SR_NO,q.* FROM (SELECT * FROM (";
            //If chkall = "0" Then    'Manoharan 08Mar2007:
            if (!string.IsNullOrEmpty(strCondition2))
            {
                strSQL += " SELECT ";
                strSQL += "COUNTRY_MST_PK, ";
                strSQL += "COUNTRY_ID, ";
                strSQL += "(COUNTRY_NAME) COUNTRY_NAME, ";
                strSQL += " 'true' active";
                strSQL += "FROM COUNTRY_MST_TBL CN ";
                strSQL += "WHERE  1=1";
                strSQL += strCondition2;
                strSQL += strCondition;
                strSQL += " union ";
            }
            strSQL += " SELECT ";
            strSQL += "COUNTRY_MST_PK, ";
            strSQL += "COUNTRY_ID, ";
            strSQL += "(COUNTRY_NAME) COUNTRY_NAME, ";
            strSQL += " 'false' active";
            strSQL += "FROM COUNTRY_MST_TBL CN ";
            strSQL += "WHERE  1=1";
            strSQL += strCondition1;
            strSQL += strCondition;
            //ElseIf chkall = "1" Then    'Manoharan 08Mar2007: to select all countries in PopUp: from FlexiReport
            //strSQL &= vbCrLf & " SELECT COUNTRY_MST_PK, COUNTRY_ID, Initcap(COUNTRY_NAME) COUNTRY_NAME, "
            //strSQL &= vbCrLf & " 'true' active FROM COUNTRY_MST_TBL CN WHERE CN.ACTIVE_FLAG = 1 "
            //End If
            //sorting definition
            strSQL += " order by COUNTRY_ID ) ORDER BY  ACTIVE DESC ,COUNTRY_ID)Q) ";
            strSQL += " WHERE SR_NO  Between " + start + " and " + last;

            try
            {
                return objWF.GetDataSet(strSQL);
                //Modified by Manjunath  PTS ID:Sep-02  13/09/2011
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

        #endregion "Fetch Country Popup Function "

        #region "Fetch Country Function"

        /// <summary>
        /// Fetches the country.
        /// </summary>
        /// <param name="CountryPK">The country pk.</param>
        /// <param name="CountryID">The country identifier.</param>
        /// <param name="CountryName">Name of the country.</param>
        /// <param name="ActiveOnly">if set to <c>true</c> [active only].</param>
        /// <param name="RemoveBlankRecord">if set to <c>true</c> [remove blank record].</param>
        /// <returns></returns>
        public DataSet FetchCountry(Int16 CountryPK = 0, string CountryID = "", string CountryName = "", bool ActiveOnly = true, bool RemoveBlankRecord = false)
        {
            string strSQL = null;
            string strCondition = " 1 = 1 ";
            if (ActiveOnly == true)
            {
                strCondition += " and ACTIVE_FLAG = 1 ";
            }
            strSQL = strSQL + " SELECT * FROM ( ";
            strSQL = strSQL + " select ";
            strSQL = strSQL + " ' ' COUNTRY_ID,";
            strSQL = strSQL + " ' ' COUNTRY_NAME,";
            strSQL = strSQL + " 0 COUNTRY_MST_PK ";
            strSQL = strSQL + " FROM ";
            strSQL = strSQL + " DUAL ";
            strSQL = strSQL + " UNION";
            strSQL = strSQL + " SELECT ";
            strSQL = strSQL + " COUNTRY_ID,";
            strSQL = strSQL + " COUNTRY_NAME,";
            strSQL = strSQL + " COUNTRY_MST_PK ";
            strSQL = strSQL + " FROM COUNTRY_MST_TBL";
            strSQL = strSQL + " WHERE " + strCondition;
            strSQL = strSQL + " )";
            strSQL = strSQL + " ORDER BY COUNTRY_NAME";
            WorkFlow objWF = new WorkFlow();
            try
            {
                // Coded Added for removing the First blank record which is generally used
                // in Dropdown population
                // Edited By Rajesh Raushan 11-Nov-05
                DataSet ds = null;
                ds = objWF.GetDataSet(strSQL);
                if (RemoveBlankRecord == true)
                {
                    ds.Tables[0].Rows.RemoveAt(0);
                }
                return ds;
                //Code End: Rahesh
                //Modified by Manjunath  PTS ID:Sep-02  13/09/2011
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

        #endregion "Fetch Country Function"

        #region " Fetch Valid Countries for a Regions "

        // Added By Rajesh Raushan. 12-Nov-2005
        // This will return List of all Countries For given condition
        // Used in Region Transaction in QFOR Project. ( frmRegionDetails )
        /// <summary>
        /// Fetches the countries.
        /// </summary>
        /// <param name="forRegionPKs">For region p ks.</param>
        /// <param name="activeOnly">if set to <c>true</c> [active only].</param>
        /// <param name="businessType">Type of the business.</param>
        /// <returns></returns>
        public DataTable FetchCountries(string forRegionPKs = "", bool activeOnly = true, string businessType = "3")
        {
            string strSQL = "";
            string strCondition = "";
            // REGION Condition
            string strRegionCondition = " and  1=2 ";
            if (!string.IsNullOrEmpty(forRegionPKs))
            {
                strRegionCondition = " and REGION_MST_FK in (" + forRegionPKs + ")";
            }

            // Active Flag Condition
            if (activeOnly == true)
            {
                strCondition = " and ACTIVE_FLAG = 1 ";
            }
            // Business Type Condition
            //'Commented To Hide business type
            //'If businessType = "1" Then
            //'    strCondition &= vbCrLf & " and BUSINESS_TYPE in(1,3) "
            //'ElseIf businessType = "2" Then
            //'    strCondition &= vbCrLf & " and BUSINESS_TYPE in(2,3) "
            //'End If

            strSQL = "            Select ";
            strSQL += "       COUNTRY_MST_PK, ";
            strSQL += "       1 SELECTED, ";
            strSQL += "       COUNTRY_ID, ";
            strSQL += "       UPPER(COUNTRY_NAME) As Country ";
            strSQL += "    from ";
            strSQL += "       COUNTRY_MST_TBL ";
            strSQL += "    where ";
            strSQL += "       1 = 1 " + strCondition;
            strSQL += "    and ";
            strSQL += "    COUNTRY_MST_PK in  ";
            strSQL += "    ( Select COUNTRY_MST_FK from  ";
            strSQL += "            REGION_MST_TRN ";
            strSQL += "        where 1=1 " + strRegionCondition;
            strSQL += "     ) ";
            strSQL += "     union ";
            strSQL += "  Select ";
            strSQL += "       COUNTRY_MST_PK, ";
            strSQL += "       0 SELECTED, ";
            strSQL += "       COUNTRY_ID, ";
            strSQL += "       UPPER(COUNTRY_NAME) As Country ";
            strSQL += "    from ";
            strSQL += "       COUNTRY_MST_TBL ";
            strSQL += "    where ";
            strSQL += "       1 = 1 " + strCondition;
            strSQL += "    and ";
            strSQL += "    COUNTRY_MST_PK not in  ";
            strSQL += "    ( Select COUNTRY_MST_FK from  ";
            strSQL += "            REGION_MST_TRN ";
            strSQL += "        where 1=1 " + strRegionCondition;
            strSQL += "     ) ";
            strSQL += "  ORDER BY COUNTRY";

            WorkFlow objWF = new WorkFlow();
            try
            {
                return objWF.GetDataSet(strSQL).Tables[0];
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

        #endregion " Fetch Valid Countries for a Regions "

        #region "Enhance Search Function"

        /// <summary>
        /// Fetches the country.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchCountry(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strLOC_MST_IN = null;
            string strSERACH_IN = null;
            string strBizType = null;
            string strReq = null;
            var strNull = "";
            string SEARCH_FLAG_IN = "";
            arr = strCond.Split('~');

            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                SEARCH_FLAG_IN = Convert.ToString(arr.GetValue(2));

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_COUNTRY_PKG.GETCOUNTRY_COMMON";

                var _with6 = selectCommand.Parameters;
                _with6.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with6.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with6.Add("SEARCH_FLAG_IN", (string.IsNullOrEmpty(SEARCH_FLAG_IN) ? strNull : SEARCH_FLAG_IN)).Direction = ParameterDirection.Input;
                _with6.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
                //Manjunath  PTS ID:Sep-02  13/09/2011
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
                selectCommand.Connection.Close();
            }
        }

        /// <summary>
        /// Fetches the name of the country by.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchCountryByName(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strLOC_MST_IN = null;
            string strSERACH_IN = null;
            string strBizType = null;
            string strReq = null;
            var strNull = "";
            arr = strCond.Split('~');

            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_COUNTRY_PKG.GETCOUNTRY_BY_NAME";

                var _with7 = selectCommand.Parameters;
                _with7.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with7.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with7.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
                //Manjunath  PTS ID:Sep-02  13/09/2011
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
                selectCommand.Connection.Close();
            }
        }

        #endregion "Enhance Search Function"

        #region "Fetch CountryID Function"

        /// <summary>
        /// Fetches the country identifier.
        /// </summary>
        /// <param name="CountryPK">The country pk.</param>
        /// <param name="CountryID">The country identifier.</param>
        /// <param name="CountryName">Name of the country.</param>
        /// <param name="ActiveOnly">if set to <c>true</c> [active only].</param>
        /// <param name="RemoveBlankRecord">if set to <c>true</c> [remove blank record].</param>
        /// <returns></returns>
        public DataSet FetchCountryID(Int16 CountryPK = 0, string CountryID = "", string CountryName = "", bool ActiveOnly = true, bool RemoveBlankRecord = false)
        {
            string strSQL = null;
            string strCondition = " 1 = 1 ";
            if (ActiveOnly == true)
            {
                strCondition += " and ACTIVE_FLAG = 1 ";
            }
            strSQL = strSQL + " SELECT * FROM ( ";
            strSQL = strSQL + " select ";
            strSQL = strSQL + " ' ' COUNTRY_ID,";
            strSQL = strSQL + " ' ' COUNTRY_NAME,";
            strSQL = strSQL + " 0 COUNTRY_MST_PK ";
            strSQL = strSQL + " FROM ";
            strSQL = strSQL + " DUAL ";
            strSQL = strSQL + " UNION";
            strSQL = strSQL + " SELECT ";
            strSQL = strSQL + " COUNTRY_ID,";
            strSQL = strSQL + " COUNTRY_NAME COUNTRY_NAME,";
            strSQL = strSQL + " COUNTRY_MST_PK ";
            strSQL = strSQL + " FROM COUNTRY_MST_TBL";
            strSQL = strSQL + " WHERE " + strCondition;
            strSQL = strSQL + " )";
            strSQL = strSQL + " ORDER BY COUNTRY_ID";
            WorkFlow objWF = new WorkFlow();
            try
            {
                // Coded Added for removing the First blank record which is generally used
                // in Dropdown population
                // Edited By Rajesh Raushan 11-Nov-05
                DataSet ds = null;
                ds = objWF.GetDataSet(strSQL);
                if (RemoveBlankRecord == true)
                {
                    ds.Tables[0].Rows.RemoveAt(0);
                }
                return ds;
                //Code End: Rahesh
                //Modified by Manjunath  PTS ID:Sep-02  13/09/2011
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

        #endregion "Fetch CountryID Function"

        #region "Get Area PK"

        /// <summary>
        /// Gets the area pk.
        /// </summary>
        /// <param name="AreaName">Name of the area.</param>
        /// <returns></returns>
        public int GetAreaPK(string AreaName)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder();
                WorkFlow objWF = new WorkFlow();
                sb.Append(" SELECT A.AREA_MST_PK FROM AREA_MST_TBL A WHERE A.AREA_NAME ='" + AreaName + "'");
                return Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Get Area PK"
    }
}