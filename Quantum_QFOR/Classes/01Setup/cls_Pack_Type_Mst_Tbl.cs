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
using System.Data;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_Pack_Type_Mst_Tbl : CommonFeatures
    {
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

        #endregion "List of Properties"

        #region "Constructors"

        /// <summary>
        /// Initializes a new instance of the <see cref="cls_Pack_Type_Mst_Tbl"/> class.
        /// </summary>
        /// <param name="BlankRow">if set to <c>true</c> [blank row].</param>
        public cls_Pack_Type_Mst_Tbl(bool BlankRow)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            if (BlankRow)
            {
                strQuery.Append("SELECT 0 PACK_TYPE_MST_PK, ");
                strQuery.Append("' ' PACK_TYPE_ID, ");
                strQuery.Append("' ' PACK_TYPE_DESC  ");
                strQuery.Append("FROM DUAL ");
                strQuery.Append("UNION ");
            }
            strQuery.Append("SELECT P.PACK_TYPE_MST_PK, ");
            strQuery.Append("P.PACK_TYPE_ID, ");
            strQuery.Append("P.PACK_TYPE_DESC  ");
            strQuery.Append("FROM PACK_TYPE_MST_TBL P ");
            strQuery.Append("WHERE P.ACTIVE_FLAG =1 ");
            strQuery.Append("ORDER BY PACK_TYPE_MST_PK ");
            try
            {
                M_DataSet = objWF.GetDataSet(strQuery.ToString());
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

        #region "Fetch Function"

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="P_Pack_Type_Mst_Pk">The p_ pack_ type_ MST_ pk.</param>
        /// <param name="P_Pack_Type_Id">The p_ pack_ type_ identifier.</param>
        /// <param name="P_Pack_Type_Desc">The p_ pack_ type_ desc.</param>
        /// <param name="P_Active">The p_ active.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="strColumnName">Name of the string column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="ActiveFlag">The active flag.</param>
        /// <param name="SortCol">The sort col.</param>
        /// <param name="blnSortAscending">if set to <c>true</c> [BLN sort ascending].</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public DataSet FetchAll(Int64 P_Pack_Type_Mst_Pk, string P_Pack_Type_Id, string P_Pack_Type_Desc, Int64 P_Active, string SearchType = "", string strColumnName = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, int ActiveFlag = 1, Int16 SortCol = 2,
        bool blnSortAscending = false, Int32 flag = 0)
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
            if (P_Pack_Type_Id.Trim().Length > 0)
            {
                if (SearchType == "S")
                {
                    strCondition = " AND UPPER(Pack_Type_Id) LIKE '" + P_Pack_Type_Id.ToUpper().Replace("'", "''") + "%'";
                }
                else
                {
                    strCondition = " AND UPPER(Pack_Type_Id) LIKE '%" + P_Pack_Type_Id.ToUpper().Replace("'", "''") + "%'";
                }
            }

            if (P_Pack_Type_Desc.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strCondition = strCondition + " And Upper(Pack_Type_Desc) like '%" + P_Pack_Type_Desc.ToUpper().Replace("'", "''") + "%' ";
                }
                else
                {
                    strCondition = strCondition + " And Upper(Pack_Type_Desc) like '" + P_Pack_Type_Desc.ToUpper().Replace("'", "''") + "%' ";
                }
            }
            else
            {
            }

            if (ActiveFlag > 0)
            {
                // strCondition = strCondition & " AND ACTIVE_FLAG = 1"
                //NEW  ADDED BY MINAKSHI ON 1-JAN-2009
                strCondition = strCondition + " AND P.ACTIVE_FLAG  = 1";
            }
            else
            {
                strCondition = strCondition + "";
            }

            strSQL = " SELECT Count(*) from Pack_Type_Mst_Tbl P where 1=1 ";
            strSQL = strSQL + strCondition;
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

            //COMMENTED BY MINAKSHI ON 1-JAN-2009 FOR NEW REQUIREMENT FOR VEK CLIENT(PACK MATERIALS DETAILS)
            //strSQL = " select * from ("
            //strSQL = strSQL & " select ROWNUM SR_NO,q.* from ( "
            //strSQL = strSQL & " SELECT "
            //strSQL = strSQL & " Pack_Type_Mst_Pk,"
            //strSQL = strSQL & " ACTIVE_FLAG,"
            //strSQL = strSQL & " Pack_Type_Id,"
            //strSQL = strSQL & " Pack_Type_Desc,"
            //strSQL = strSQL & " Version_No "
            //strSQL = strSQL & " FROM PACK_TYPE_MST_TBL "
            //strSQL = strSQL & " WHERE ( 1 = 1)  "
            //ENDED BY MINAKSHI ON 1-JAN-2009
            //NEW  ADDED BY MINAKSHI ON 1-JAN-2009
            strSQL = " select * from (";
            strSQL = strSQL + " select ROWNUM SR_NO,q.* from ( ";
            strSQL = strSQL + " SELECT ";
            strSQL = strSQL + " Pack_Type_Mst_Pk,";
            strSQL = strSQL + " P.ACTIVE_FLAG ACTIVE_FLAG,";
            strSQL = strSQL + " Pack_Type_Id,";
            strSQL = strSQL + " Pack_Type_Desc,";
            strSQL = strSQL + "  LENGTH,";
            strSQL = strSQL + "  WIDTH,";
            strSQL = strSQL + "  HEIGHT,";
            strSQL = strSQL + "  DIMENTION_UNIT_MST_FK,";
            strSQL = strSQL + "  UOM.DIMENTION_ID DIMENTION_ID,";
            strSQL = strSQL + "  CURRENCY_MST_FK,";
            strSQL = strSQL + "  CURR.CURRENCY_ID CURRENCY_ID,";
            strSQL = strSQL + "  BUY_RATE,";
            strSQL = strSQL + "  SELL_RATE,";
            strSQL = strSQL + "  P.Version_No Version_No";
            strSQL = strSQL + " FROM PACK_TYPE_MST_TBL P,DIMENTION_UNIT_MST_TBL UOM,CURRENCY_TYPE_MST_TBL CURR";
            strSQL = strSQL + " WHERE ( 1 = 1)  ";
            strSQL = strSQL + strCondition;
            strSQL = strSQL + " AND P.DIMENTION_UNIT_MST_FK = UOM.DIMENTION_UNIT_MST_PK(+)";
            strSQL = strSQL + " AND P.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK(+)";
            //ENDED BY MINAKSHI ON 1-JAN-2009
            if (!strColumnName.Equals("SR_NO"))
            {
                strSQL += "order by " + strColumnName;
            }

            if (!blnSortAscending & !strColumnName.Equals("SR_NO"))
            {
                strSQL += " DESC";
            }

            strSQL = strSQL + " )q)  WHERE SR_NO  Between " + start + " and " + last;

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

        #endregion "Fetch Function"

        //ADDED BY MINAKSHI ON 1-JAN-2009 FOR NEW REQUIREMENT FOR VEK CLIENT(PACK MATERIALS DETAILS)

        #region "Fetch UOM"

        /// <summary>
        /// Fetches the uom.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchUOM()
        {
            string strSql = null;
            try
            {
                strSql = strSql + "SELECT ";
                strSql = strSql + "0 DIMENTION_UNIT_MST_PK,";
                strSql = strSql + "' ' DIMENTION_ID";
                strSql = strSql + "FROM dual";
                strSql = strSql + "Union ";
                strSql = strSql + "SELECT ";
                strSql = strSql + "DIMENTION_UNIT_MST_PK,";
                strSql = strSql + " DIMENTION_ID";
                strSql = strSql + " FROM DIMENTION_UNIT_MST_TBL";
                strSql = strSql + " WHERE ACTIVE = 1";
                strSql = strSql + " order by DIMENTION_ID";

                WorkFlow objWF = new WorkFlow();

                return objWF.GetDataSet(strSql);
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

        #endregion "Fetch UOM"

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
            strSQL = strSQL + "order by CURRENCY_ID";
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

        #region "GET UOM FOR IMPORT"

        /// <summary>
        /// Getuoms the specified uomid.
        /// </summary>
        /// <param name="uomid">The uomid.</param>
        /// <returns></returns>
        public Int32 GETUOM(string uomid = "")
        {
            string strSql = null;
            try
            {
                strSql = strSql + "SELECT ";
                strSql = strSql + "D.DIMENTION_UNIT_MST_PK";
                strSql = strSql + " FROM DIMENTION_UNIT_MST_TBL D";
                strSql = strSql + " WHERE D.DIMENTION_ID = '" + uomid + "'";
                WorkFlow objWF = new WorkFlow();
                return Convert.ToInt32(objWF.ExecuteScaler(strSql));
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

        #endregion "GET UOM FOR IMPORT"

        #region "GET CURRENCY FOR IMPORT"

        /// <summary>
        /// Getcurrencies the specified currencyid.
        /// </summary>
        /// <param name="currencyid">The currencyid.</param>
        /// <returns></returns>
        public Int32 GETCURRENCY(string currencyid = "")
        {
            string strSql = null;
            try
            {
                strSql = strSql + "SELECT ";
                strSql = strSql + "CURR.CURRENCY_MST_PK";
                strSql = strSql + " FROM CURRENCY_TYPE_MST_TBL CURR";
                strSql = strSql + " WHERE CURR.CURRENCY_ID = '" + currencyid + "'";
                WorkFlow objWF = new WorkFlow();
                return Convert.ToInt32(objWF.ExecuteScaler(strSql));
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

        #endregion "GET CURRENCY FOR IMPORT"

        //ENDED BY MINAKSHI ON 1-JAN-2009
    }
}