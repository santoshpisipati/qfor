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
using System.Collections;
using System.Data;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_Region_Mst_Tbl : CommonFeatures
    {
        #region " Fetch Function "

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="RegionMSTPK">The region MSTPK.</param>
        /// <param name="Region_Code">The region_ code.</param>
        /// <param name="Region_Name">Name of the region_.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="SortExpression">The sort expression.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="ActiveFlag">The active flag.</param>
        /// <param name="SortCol">The sort col.</param>
        /// <param name="intBusType">Type of the int bus.</param>
        /// <param name="intUser">The int user.</param>
        /// <param name="SortType">Type of the sort.</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public DataSet FetchAll(Int64 RegionMSTPK = 0, string Region_Code = "", string Region_Name = "", string SearchType = "", string SortExpression = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, int ActiveFlag = 1, Int32 SortCol = 3, int intBusType = 0,
        int intUser = 0, string SortType = " ASC ", Int32 flag = 0)
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
            if (RegionMSTPK > 0)
            {
                if (SearchType == "C")
                {
                    strCondition = strCondition + " and RMT.REGION_MST_PK like '%" + RegionMSTPK + "%'";
                }
                else if (SearchType == "S")
                {
                    strCondition = strCondition + " and RMT.REGION_MST_PK like '" + RegionMSTPK + "%'";
                }
            }

            if (Region_Code.Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strCondition = strCondition + " and UPPER(RMT.REGION_CODE) like '%" + Region_Code.ToUpper().Replace("'", "''") + "%'";
                }
                else if (SearchType == "S")
                {
                    strCondition = strCondition + " and UPPER(RMT.REGION_CODE) like '" + Region_Code.ToUpper().Replace("'", "''") + "%'";
                }
            }
            if (Region_Name.Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strCondition = strCondition + " and UPPER(RMT.REGION_NAME) like '%" + Region_Name.ToUpper().Replace("'", "''") + "%'";
                }
                else if (SearchType == "S")
                {
                    strCondition = strCondition + " and UPPER(RMT.REGION_NAME) like '" + Region_Name.ToUpper().Replace("'", "''") + "%'";
                }
            }
            if (intBusType == 3 & intUser == 3)
            {
                strCondition += " AND BUSINESS_TYPE IN (1,2,3) ";
            }
            else if (intBusType == 3 & intUser == 2)
            {
                strCondition += " AND BUSINESS_TYPE IN (2,3) ";
            }
            else if (intBusType == 3 & intUser == 1)
            {
                strCondition += " AND BUSINESS_TYPE IN (1,3) ";
            }
            else if (intBusType > 0)
            {
                strCondition += " AND BUSINESS_TYPE = " + intBusType + " ";
            }
            if (ActiveFlag > 0)
            {
                strCondition = strCondition + " and RMT.ACTIVE_FLAG = 1";
            }
            strSQL = "SELECT Count(*) from REGION_MST_TBL RMT";
            strSQL = strSQL + "WHERE ( 1 = 1) ";
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
            strSQL = " SELECT * from (";
            strSQL = strSQL + "SELECT ROWNUM SR_NO,q.* FROM ";
            strSQL = strSQL + " (SELECT RMT.REGION_MST_PK,";
            strSQL = strSQL + "RMT.ACTIVE_FLAG,";
            strSQL = strSQL + "RMT.REGION_CODE,";
            strSQL = strSQL + "RMT.REGION_NAME,";
            strSQL = strSQL + " DECODE(BUSINESS_TYPE,'0','','1','Air','2','Sea','3','Both') BUSINESS_TYPE, ";
            strSQL = strSQL + "RMT.VERSION_NO";
            strSQL = strSQL + " FROM REGION_MST_TBL RMT";
            strSQL = strSQL + "WHERE ( 1 = 1) ";
            strSQL = strSQL + strCondition;
            strSQL = strSQL + "ORDER BY " + SortCol + " " + SortType + " ) ";
            strSQL = strSQL + " q  )WHERE SR_NO  Between " + start + " and " + last;
            try
            {
                return objWF.GetDataSet(strSQL);
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

        #endregion " Fetch Function "

        #region " Fetch Valid Regions "

        /// <summary>
        /// Fetches the regions.
        /// </summary>
        /// <param name="tradePK">The trade pk.</param>
        /// <param name="activeOnly">if set to <c>true</c> [active only].</param>
        /// <param name="businessType">Type of the business.</param>
        /// <param name="countryPK">The country pk.</param>
        /// <returns></returns>
        public DataTable FetchRegions(string tradePK = "", bool activeOnly = true, string businessType = "3", string countryPK = "")
        {
            string strSQL = "";
            string strCondition = "";
            string strCountryCondition = "";
            string strTradeCondition = " and  1=2 ";
            if (!string.IsNullOrEmpty(tradePK))
            {
                strTradeCondition = " and TRADE_MST_FK = " + tradePK;
            }

            if (activeOnly == true)
            {
                strCondition = " and ACTIVE_FLAG = 1 ";
            }
            if (!string.IsNullOrEmpty(countryPK))
            {
                strCountryCondition += " and ";
                strCountryCondition += " REGION_MST_PK in  ";
                strCountryCondition += " ( Select REGION_MST_FK from REGION_MST_TRN  ";
                strCountryCondition += "    where ";
                strCountryCondition += "    COUNTRY_MST_FK = " + countryPK + " )";
            }
            strSQL = "            Select ";
            strSQL += "       REGION_MST_PK, ";
            strSQL += "       1 SELECTED, ";
            strSQL += "       REGION_CODE, ";
            strSQL += "       REGION_NAME ";
            strSQL += "    from ";
            strSQL += "       REGION_MST_TBL ";
            strSQL += "    where ";
            strSQL += "       1 = 1 " + strCondition + strCountryCondition;
            strSQL += "    and ";
            strSQL += "    REGION_MST_PK in  ";
            strSQL += "    ( Select REGION_MST_FK from  ";
            strSQL += "            REGION_MST_TRN ";
            strSQL += "        where PORT_MST_FK in ";
            strSQL += "         ( Select PORT_MST_FK from TRADE_MST_TRN ";
            strSQL += "            where 1=1 " + strTradeCondition;
            strSQL += "          ) ";
            strSQL += "     ) ";
            strSQL += "     union ";
            strSQL += "  Select ";
            strSQL += "       REGION_MST_PK, ";
            strSQL += "       0 SELECTED, ";
            strSQL += "       REGION_CODE, ";
            strSQL += "       REGION_NAME ";
            strSQL += "    from ";
            strSQL += "       REGION_MST_TBL ";
            strSQL += "    where ";
            strSQL += "       1 = 1 " + strCondition + strCountryCondition;
            strSQL += "    and ";
            strSQL += "    REGION_MST_PK not in  ";
            strSQL += "    ( Select REGION_MST_FK from  ";
            strSQL += "            REGION_MST_TRN ";
            strSQL += "        where PORT_MST_FK in ";
            strSQL += "         ( Select PORT_MST_FK from TRADE_MST_TRN ";
            strSQL += "            where 1=1 " + strTradeCondition;
            strSQL += "          ) ";
            strSQL += "     ) ";
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

        #endregion " Fetch Valid Regions "

        #region "Get columns nullable or not nullable"

        /// <summary>
        /// Gets the nullable columns.
        /// </summary>
        /// <param name="TableName">Name of the table.</param>
        /// <param name="Nullable">if set to <c>true</c> [nullable].</param>
        /// <returns></returns>
        public DataTable GetNullableColumns(string TableName, bool Nullable = false)
        {
            WorkFlow objWF = new WorkFlow();
            string Query = "";
            Query = "SELECT DISTINCT COLUMN_NAME FROM USER_TAB_COLUMNS WHERE TABLE_NAME = '" + TableName + "'";
            if (Nullable)
            {
                Query += " AND NULLABLE = 'Y'";
            }
            else
            {
                Query += " AND NULLABLE = 'N'";
            }
            return objWF.GetDataTable(Query);
        }

        #endregion "Get columns nullable or not nullable"

        #region "Import Function"

        /// <summary>
        /// Imports the specified imp data.
        /// </summary>
        /// <param name="ImpData">The imp data.</param>
        /// <param name="Mode">The mode.</param>
        /// <param name="BlnImport">if set to <c>true</c> [BLN import].</param>
        /// <returns></returns>
        public ArrayList Import(DataTable ImpData, string Mode = "I", bool BlnImport = false)
        {
            WorkFlow objWF = new WorkFlow();
            string Query = "";
            int Count = 0;
            string MSG = "";
            string MSG_Failed = "";
            if (Mode == "I")
            {
                foreach (DataRow dr in ImpData.Rows)
                {
                    Count += 1;
                    try
                    {
                        Query = "insert into REGION_MST_TBL(";
                        Query += "REGION_MST_PK,";
                        Query += "REGION_CODE,";
                        Query += "REGION_NAME,";
                        Query += "LOCATION_MST_FK,";
                        Query += "BUSINESS_TYPE,";
                        Query += "ACTIVE_FLAG,";
                        Query += "CREATED_BY_FK,";
                        Query += "CREATED_DT,";
                        Query += "LAST_MODIFIED_DT,";
                        Query += "VERSION_NO) ";
                        Query += "VALUES(";

                        Query += "SEQ_REGION_MST_TBL.NEXTVAL,";
                        Query += "'" + dr["REGION_CODE"] + "',";
                        Query += "TRIM('" + dr["REGION_NAME"] + "'),";
                        Query += HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",";
                        //'Commented because Business Type Column is not belongs to DataTable
                        //If dr("BUSINESS_TYPE") = "Air" Or dr("BUSINESS_TYPE") = "1" Then
                        //    Query &= "1,"
                        //Else
                        //    Query &= "2,"
                        //End If
                        Query += "NULL" + ",";
                        //Query &= dr("ACTIVE_FLAG") & ","
                        Query += dr["Active"] + ",";
                        Query += CREATED_BY + ",";
                        Query += "SYSDATE,";
                        Query += "SYSDATE,";
                        Query += "DEFAULT)";

                        if (objWF.ExecuteCommands(Query))
                        {
                            if (string.IsNullOrEmpty(MSG))
                            {
                                MSG = Count.ToString();
                            }
                            else
                            {
                                MSG += "," + Count.ToString();
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        if (string.IsNullOrEmpty(MSG_Failed))
                        {
                            MSG_Failed = Count.ToString();
                        }
                        else
                        {
                            MSG_Failed += "," + Count.ToString();
                        }
                    }
                }
            }
            else if (Mode == "U")
            {
            }
            else if (Mode == "D")
            {
            }
            if (BlnImport == false)
            {
                arrMessage.Add("All Data Saved Successfully");
            }
            else
            {
                arrMessage.Add("Data Imported Successfully");
                return arrMessage;
            }
            return new ArrayList();
        }

        #endregion "Import Function"
    }
}