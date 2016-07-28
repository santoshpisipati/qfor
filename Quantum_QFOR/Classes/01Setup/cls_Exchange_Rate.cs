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

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class clsExchangeRate : CommonFeatures
    {
        #region "Fetch All"

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="CurrencyID">The currency identifier.</param>
        /// <param name="CurrencyName">Name of the currency.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="strColumnName">Name of the string column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="blnSortAscending">if set to <c>true</c> [BLN sort ascending].</param>
        /// <param name="fromDate">From date.</param>
        /// <param name="toDate">To date.</param>
        /// <param name="VoyName">Name of the voy.</param>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="ExType">Type of the ex.</param>
        /// <param name="Basecurrencyfk">The basecurrencyfk.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="CurrStatus">The curr status.</param>
        /// <param name="voyagepk">The voyagepk.</param>
        /// <returns></returns>
        public DataSet FetchAll(string CurrencyID = "", string CurrencyName = "", string SearchType = "", string strColumnName = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, bool blnSortAscending = false, string fromDate = "", string toDate = "", string VoyName = "",
        string VslName = "", short ExType = 1, Int32 Basecurrencyfk = 0, Int32 flag = 0, string CurrStatus = "", int voyagepk = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            //holds the first record slo of the page
            string strSQL = null;
            //Holds the query string .
            string newStrSql = null;
            //Holds the query for status
            string strCondition = null;
            //it holds the where class of the query.
            Int32 TotalRecords = default(Int32);
            //total number of records
            WorkFlow objWF = new WorkFlow();
            //common class object.
            if (flag == 0)
            {
                strCondition += " AND 1=2";
            }
            //Added by Ajay on 07/06/2011
            if (ExType != 2)
            {
                if (Convert.ToInt32(CurrStatus) == 0)
                {
                    //newStrSql &= vbCrLf & " and EXCH.FROM_DATE <= SYSDATE " & vbCrLf
                    newStrSql += " and TO_DATE(EXCH.TO_DATE,'DD/MM/YYYY') >=TO_DATE(SYSDATE,'DD/MM/YYYY') ";
                }
                if (Convert.ToInt32(CurrStatus) == 1)
                {
                    newStrSql += " and TO_DATE(EXCH.TO_DATE,'DD/MM/YYYY') < TO_DATE(SYSDATE,'DD/MM/YYYY') ";
                }
            }
            //Snigdharani - 29/11/2008
            if (ExType != 2)
            {
                //modifying  by thiyagarajan on 18/8/08 to avoid duplicates while fetching as prabhu suggestion
                //If Not fromDate Is Nothing And Not fromDate = " " And toDate Is Nothing And Not toDate = " " Then
                if ((fromDate != null) & !(fromDate == " ") & (toDate == null | toDate == " "))
                {
                    //strCondition &= vbCrLf & " AND exch.to_date >= TO_DATE('" & fromDate & "','" & dateFormat & "') " & vbCrLf
                    strCondition += " AND exch.from_date  >= TO_DATE('" + fromDate + "','" + dateFormat + "') ";
                }

                if ((toDate != null) & !(toDate == " ") & (fromDate == null | fromDate == " "))
                {
                    //If Not toDate Is Nothing And Not toDate = " " And fromDate Is Nothing And Not fromDate = " " Then
                    //strCondition &= vbCrLf & " AND exch.from_date <= TO_DATE('" & toDate & "','" & dateFormat & "') " & vbCrLf
                    strCondition += " AND exch.to_date  <= TO_DATE('" + toDate + "','" + dateFormat + "') ";
                }

                if ((fromDate != null) & !(fromDate == " ") & (toDate != null) & !(toDate == " "))
                {
                    //strCondition &= vbCrLf & "AND (TO_DATE('" & fromDate & "','" & dateFormat & "') BETWEEN FROM_DATE AND TO_DATE OR TO_DATE('" & toDate & "','" & dateFormat & "') BETWEEN FROM_DATE AND TO_DATE ) " & vbCrLf
                    strCondition += "AND ((EXCH.FROM_DATE <= TO_DATE('" + toDate + "','" + dateFormat + "')) AND (EXCH.TO_DATE >=TO_DATE('" + fromDate + "','" + dateFormat + "'))) ";
                    //strCondition &= vbCrLf & "AND ((EXCH.FROM_DATE = TO_DATE('" & fromDate & "','" & dateFormat & "')) AND (EXCH.TO_DATE =TO_DATE('" & toDate & "','" & dateFormat & "'))) " 'Sivachandran 3-8-08
                }
                //end by thiyagarajan
            }
            if (CurrencyID.Trim().Length > 0)
            {
                strCondition += " AND UPPER(CURR.CURRENCY_ID) LIKE '" + CurrencyID.ToUpper().Replace("'", "''") + "'";
            }

            if (CurrencyName.Trim().Length > 0)
            {
                strCondition += " AND UPPER(CURR.CURRENCY_NAME) LIKE '" + CurrencyName.ToUpper().Replace("'", "''") + "'";
            }

            //If VslName.Trim.Length > 0 And ExType = 2 Then
            //    ' strCondition &= vbCrLf & " AND UPPER(V.VESSEL_NAME) LIKE '%" & VslName.ToUpper.Replace("'", "''") & "%'" & vbCrLf
            //    strCondition &= vbCrLf & " AND UPPER(EXCH.VOYAGE_TRN_FK) LIKE '%" & VslName.ToUpper.Replace("'", "''") & "%'" & vbCrLf
            //End If

            if (voyagepk > 0 & ExType == 2)
            {
                //strCondition &= vbCrLf & " AND UPPER(V.VESSEL_ID) LIKE '%" & VoyName.ToUpper.Replace("'", "''") & "%'" & vbCrLf
                //strCondition &= vbCrLf & " AND UPPER(EXCH.VOYAGE_TRN_FK) LIKE '%" & voyagepk.ToUpper.Replace("'", "''") & "%'" & vbCrLf
                strCondition += " AND EXCH.VOYAGE_TRN_FK = " + voyagepk + " ";
            }

            //adding by thiyagarajan on 4/2/09:GAP-VEK-QFOR-007c:Customs exchange rate
            if (ExType == 1 | ExType == 3 | ExType == 4)
            {
                if (fromDate == null & toDate == null)
                {
                    if (!string.IsNullOrEmpty(fromDate) & !(toDate == " "))
                    {
                        strCondition += " AND(TO_DATE(TO_CHAR(sysdate,'" + dateFormat + "'),'" + dateFormat + "')  between exch.from_date and exch.to_date OR exch.to_date  >= TO_DATE(TO_CHAR(sysdate,'" + dateFormat + "'),'" + dateFormat + "'))  ";
                    }
                }
                //Selected date else nothing, so no check on date
                //If Not fromDate Is Nothing Then
                //    If Not fromDate = " " Then
                //        strCondition &= vbCrLf & " AND(TO_DATE('" & fromDate & "','" & dateFormat & "')  between exch.from_date and exch.to_date OR exch.from_date  >= TO_DATE('" & fromDate & "','" & dateFormat & "'))  " & vbCrLf
                //    End If
                //End If
                //If BlankGrid = 0 Then
                //    strCondition &= vbCrLf & " AND 1=2" & vbCrLf
                //End If

                if (((fromDate == null | string.IsNullOrEmpty(fromDate)) & (toDate == null | string.IsNullOrEmpty(toDate))))
                {
                    strSQL += " SELECT COUNT(*) FROM ";
                    strSQL += " EXCHANGE_RATE_TRN EXCH,";
                    strSQL += " CURRENCY_TYPE_MST_TBL CURR";
                    strSQL += " WHERE 1 = 1 ";
                    strSQL += " AND CURR.CURRENCY_MST_PK = EXCH.CURRENCY_MST_FK ";
                    strSQL += " AND EXCH.VOYAGE_TRN_FK IS NULL ";

                    //strSQL &= " AND SYSDATE BETWEEN EXCH.FROM_DATE AND EXCH.TO_DATE"
                    strSQL += " AND EXCH.EXCH_RATE_TYPE_FK = " + ExType;
                }
                else
                {
                    strSQL += " SELECT COUNT(*) FROM ";
                    strSQL += " EXCHANGE_RATE_TRN EXCH,";
                    strSQL += " CURRENCY_TYPE_MST_TBL CURR";
                    strSQL += " WHERE 1 = 1 ";
                    strSQL += " AND CURR.CURRENCY_MST_PK = EXCH.CURRENCY_MST_FK ";
                    strSQL += " AND EXCH.VOYAGE_TRN_FK IS NULL ";
                    strSQL += " AND EXCH.EXCH_RATE_TYPE_FK = " + ExType;
                }
                strSQL += "  and EXCH.CURRENCY_MST_BASE_FK=" + Basecurrencyfk;
                //HttpContext.Current.Session("CURRENCY_MST_PK") & vbCrLf
                strSQL += strCondition;
                strSQL += newStrSql;
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
                strSQL += " SELECT Q1.* FROM (SELECT ROWNUM SR_NO,  ";
                strSQL += " Q.* FROM (select DISTINCT EXCH.EXCHANGE_RATE_PK,  ";
                strSQL += " ' ' AS VSLNAME,";
                strSQL += " ' ' AS VOYNO,";
                strSQL += " CURR.CURRENCY_ID ,CURR.CURRENCY_NAME,  ";
                strSQL += " EXCH.EXCHANGE_RATE ,EXCH.FROM_DATE , EXCH.TO_DATE, 0 DELFLAG ";
                //strSQL &= vbCrLf & " from EXCHANGE_RATE_TRN EXCH,CURRENCY_TYPE_MST_TBL CURR,VESSEL_VOYAGE_TBL V,VESSEL_VOYAGE_TRN VVT  "
                strSQL += " from EXCHANGE_RATE_TRN EXCH,CURRENCY_TYPE_MST_TBL CURR ";
                strSQL += " WHERE EXCH.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK  ";
                //strSQL &= vbCrLf & " AND EXCH.VOYAGE_TRN_FK= V.VESSEL_VOYAGE_TBL_PK"
                strSQL += " AND EXCH.VOYAGE_TRN_FK IS NULL";
                strSQL += " AND EXCH.EXCH_RATE_TYPE_FK = " + ExType;
                //If ((fromDate Is Nothing Or fromDate = "") And (toDate Is Nothing Or toDate = "")) Then
                //    strSQL &= " AND SYSDATE BETWEEN EXCH.FROM_DATE AND EXCH.TO_DATE"
                //End If
                strSQL += "  and EXCH.CURRENCY_MST_BASE_FK=" + Basecurrencyfk;
                //HttpContext.Current.Session("CURRENCY_MST_PK") & vbCrLf
                strSQL += strCondition;
                strSQL += newStrSql;
            }
            else
            {
                strSQL = "SELECT COUNT(*) FROM EXCHANGE_RATE_TRN EXCH,CURRENCY_TYPE_MST_TBL CURR,VESSEL_VOYAGE_TBL V,VESSEL_VOYAGE_TRN VVT WHERE 1=1 AND V.VESSEL_VOYAGE_TBL_PK = vvt.vessel_voyage_tbl_fk ";
                strSQL += "  and EXCH.CURRENCY_MST_BASE_FK=" + Basecurrencyfk;
                //HttpContext.Current.Session("CURRENCY_MST_PK") & vbCrLf
                strSQL += " AND CURR.CURRENCY_MST_PK = EXCH.CURRENCY_MST_FK  AND EXCH.VOYAGE_TRN_FK IS NOT NULL AND EXCH.VOYAGE_TRN_FK= VVT.VOYAGE_TRN_PK AND EXCH.EXCH_RATE_TYPE_FK = " + ExType;
                //Added by sivachandran after discussion with prabhu

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
                strSQL += " SELECT Q1.* FROM (SELECT ROWNUM SR_NO,  ";
                strSQL += " Q.* FROM (select DISTINCT EXCH.EXCHANGE_RATE_PK,  ";
                strSQL += " V.VESSEL_NAME AS VSLNAME,";
                strSQL += " VVT.VOYAGE AS VOYNO,";
                strSQL += " CURR.CURRENCY_ID ,CURR.CURRENCY_NAME,  ";
                strSQL += " EXCH.EXCHANGE_RATE ,EXCH.FROM_DATE , EXCH.TO_DATE, 0 DELFLAG ";
                strSQL += " from EXCHANGE_RATE_TRN EXCH,CURRENCY_TYPE_MST_TBL CURR,VESSEL_VOYAGE_TBL V,VESSEL_VOYAGE_TRN VVT  ";
                //strSQL &= vbCrLf & " from EXCHANGE_RATE_TRN EXCH,CURRENCY_TYPE_MST_TBL CURR "
                strSQL += " WHERE EXCH.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK  ";
                strSQL += " AND V.VESSEL_VOYAGE_TBL_PK = vvt.vessel_voyage_tbl_fk";
                strSQL += " AND EXCH.VOYAGE_TRN_FK= VVT.VOYAGE_TRN_PK";
                strSQL += " AND EXCH.VOYAGE_TRN_FK IS NOT NULL";
                strSQL += " AND EXCH.EXCH_RATE_TYPE_FK = " + ExType;
                //adding by thiygarajan on 18/3/09
                strSQL += "  and EXCH.CURRENCY_MST_BASE_FK=" + Basecurrencyfk;
                //strSQL &= vbCrLf & strCondition
                strSQL += newStrSql;
            }
            if (voyagepk > 0 & ExType == 2)
            {
                strSQL += "  AND EXCH.VOYAGE_TRN_FK = " + voyagepk + " ";
                if (CurrencyID.Trim().Length > 0)
                {
                    strSQL += " AND UPPER(CURR.CURRENCY_ID) LIKE '" + CurrencyID.ToUpper().Replace("'", "''") + "'";
                }

                if (CurrencyName.Trim().Length > 0)
                {
                    strSQL += " AND UPPER(CURR.CURRENCY_NAME) LIKE '" + CurrencyName.ToUpper().Replace("'", "''") + "'";
                }
            }

            if (!strColumnName.Equals("SR_NO"))
            {
                //strSQL &= vbCrLf & "order by " & strColumnName
                strSQL += "order by from_date DESC,CURRENCY_ID ASC ";
                //ORDER BY from_date DESC
            }

            if (!blnSortAscending & !strColumnName.Equals("SR_NO"))
            {
                strSQL += " DESC ";
            }

            strSQL += ")Q)Q1)  WHERE SR_NO  Between " + start + " and " + last;

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

        #endregion "Fetch All"

        #region "Save Function"

        /// <summary>
        /// Saves the specified m_ data set.
        /// </summary>
        /// <param name="M_DataSet">The m_ data set.</param>
        /// <param name="strFromDate">The string from date.</param>
        /// <param name="strToDate">The string to date.</param>
        /// <param name="strBaseCurrency">The string base currency.</param>
        /// <param name="VoyagePK">The voyage pk.</param>
        /// <param name="ExchTypePK">The exch type pk.</param>
        /// <returns></returns>
        public ArrayList Save(DataSet M_DataSet, string strFromDate = "", string strToDate = "", string strBaseCurrency = " ", Int32 VoyagePK = 0, Int32 ExchTypePK = 0)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();

            int intPKVal = 0;
            long lngI = 0;
            Int32 RecAfct = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            OracleCommand delCommand = new OracleCommand();
            try
            {
                DataTable DtTbl = new DataTable();
                DataRow DtRw = null;
                int i = 0;
                var _with1 = insCommand;
                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".EXCH_RATE_TRN_PKG.EXCHANGE_RATE_TRN_INS";
                var _with2 = _with1.Parameters;
                insCommand.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Varchar2, 3, "CURRENCY_ID").Direction = ParameterDirection.Input;
                insCommand.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CURRENCY_MST_BASE_FK_IN", strBaseCurrency).Direction = ParameterDirection.Input;

                //Changed by Sivachandran to validate From Date
                if (!string.IsNullOrEmpty(strFromDate))
                {
                    insCommand.Parameters.Add("FROM_DATE_IN", Convert.ToDateTime(strFromDate)).Direction = ParameterDirection.Input;
                }
                else
                {
                    insCommand.Parameters.Add("FROM_DATE_IN", "").Direction = ParameterDirection.Input;
                }

                //Changed by Sivachandran to validate To Date
                if (!string.IsNullOrEmpty(strToDate))
                {
                    insCommand.Parameters.Add("TO_DATE_IN", Convert.ToDateTime(strToDate)).Direction = ParameterDirection.Input;
                }
                else
                {
                    insCommand.Parameters.Add("TO_DATE_IN", "").Direction = ParameterDirection.Input;
                }
                insCommand.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "EXCHANGE_RATE").Direction = ParameterDirection.Input;
                insCommand.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;
                //sivachandran 14May2008 RTO-QFOR-001 - For Vsl/Voy Exchange Rate
                insCommand.Parameters.Add("ROE_BUY_IN", OracleDbType.Int32, 10, "ROE_BUY").Direction = ParameterDirection.Input;
                insCommand.Parameters["ROE_BUY_IN"].SourceVersion = DataRowVersion.Current;
                //End
                insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("CONFIG_MST_PK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;

                if (VoyagePK > 0)
                {
                    insCommand.Parameters.Add("VOYAGE_TRN_FK_IN", VoyagePK).Direction = ParameterDirection.Input;
                    insCommand.Parameters["VOYAGE_TRN_FK_IN"].SourceVersion = DataRowVersion.Current;
                }
                else
                {
                    insCommand.Parameters.Add("VOYAGE_TRN_FK_IN", "").Direction = ParameterDirection.Input;
                    insCommand.Parameters["VOYAGE_TRN_FK_IN"].SourceVersion = DataRowVersion.Current;
                }
                if (ExchTypePK > 0)
                {
                    insCommand.Parameters.Add("EXCH_RATE_TYPE_FK_IN", ExchTypePK).Direction = ParameterDirection.Input;
                    insCommand.Parameters["EXCH_RATE_TYPE_FK_IN"].SourceVersion = DataRowVersion.Current;
                }
                else
                {
                    insCommand.Parameters.Add("EXCH_RATE_TYPE_FK_IN", "").Direction = ParameterDirection.Input;
                    insCommand.Parameters["EXCH_RATE_TYPE_FK_IN"].SourceVersion = DataRowVersion.Current;
                }
                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "EXCHANGE_RATE_PK").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with3 = updCommand;
                _with3.Connection = objWK.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".EXCH_RATE_TRN_PKG.EXCHANGE_RATE_TRN_UPD";
                var _with4 = _with3.Parameters;

                updCommand.Parameters.Add("EXCHANGE_RATE_PK_IN", OracleDbType.Int32, 10, "EXCHANGE_RATE_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["EXCHANGE_RATE_PK_IN"].SourceVersion = DataRowVersion.Current;
                //Snigdharani - 29/11/2008
                updCommand.Parameters.Add("CURRENCY_MST_BASE_FK_IN", strBaseCurrency).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "EXCHANGE_RATE").Direction = ParameterDirection.Input;
                updCommand.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;
                //sivachandran 14May2008 RTO-QFOR-001 - For Vsl/Voy Exchange Rate
                updCommand.Parameters.Add("ROE_BUY_IN", OracleDbType.Int32, 10, "ROE_BUY").Direction = ParameterDirection.Input;
                updCommand.Parameters["ROE_BUY_IN"].SourceVersion = DataRowVersion.Current;
                //End
                updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", LAST_MODIFIED_BY).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("CONFIG_MST_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "EXCHANGE_RATE_PK").Direction = ParameterDirection.Output;
                updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with5 = objWK.MyDataAdapter;

                _with5.InsertCommand = insCommand;
                _with5.InsertCommand.Transaction = TRAN;
                _with5.UpdateCommand = updCommand;
                _with5.UpdateCommand.Transaction = TRAN;
                RecAfct = _with5.Update(M_DataSet);

                if (arrMessage.Count > 0)
                {
                    TRAN.Commit();
                    return arrMessage;
                }
                else
                {
                    arrMessage.Add("All Data Saved Successfully");
                    TRAN.Commit();
                    return arrMessage;
                }
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
                //Manjunath  PTS ID:Sep-02   15/09/2011
            }
            finally
            {
                objWK.MyCommand.Connection.Close();
            }
        }

        #endregion "Save Function"

        #region "Function Get LOGGEDIN LOCATION Currency"

        //by Thiyagarajan on 25/3/08 for location based currency : PTS TASK GEN-FEB-003

        /// <summary>
        /// Gets the currency.
        /// </summary>
        /// <param name="LocPk">The loc pk.</param>
        /// <returns></returns>
        public string GetCurrency(int LocPk)
        {
            try
            {
                WorkFlow objWF = new WorkFlow();
                //common class object.
                string strSQL = "select currency.currency_id from currency_type_mst_tbl currency where currency.currency_mst_pk in (";
                strSQL += " select country.currency_mst_fk from country_mst_tbl country where country.country_mst_pk in (";
                strSQL += "select loc.country_mst_fk from location_mst_tbl loc where loc.location_mst_pk= " + LocPk + " ))";
                return objWF.ExecuteScaler(strSQL);
                //Manjunath  PTS ID:Sep-02  15/09/2011
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

        #endregion "Function Get LOGGEDIN LOCATION Currency"

        #region "Function Get Base Currency"

        /// <summary>
        /// Gets the base currency.
        /// </summary>
        /// <returns></returns>
        public string GetBaseCurrency()
        {
            try
            {
                WorkFlow objWF = new WorkFlow();
                //common class object.
                string strSQL = "SELECT CURR.CURRENCY_ID FROM CURRENCY_TYPE_MST_TBL CURR";
                strSQL += ",CORPORATE_MST_TBL CORP WHERE CORP.CURRENCY_MST_FK = ";
                strSQL += " curr.currency_mst_pk ";
                return objWF.ExecuteScaler(strSQL);
                //Manjunath  PTS ID:Sep-02  15/09/2011
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

        //adding by thiyagarajan on 19/2/09
        /// <summary>
        /// Gets all currency.
        /// </summary>
        /// <returns></returns>
        public DataSet GetAllCurrency()
        {
            try
            {
                WorkFlow objWF = new WorkFlow();
                string strSQL = "SELECT CURR.CURRENCY_ID , CURR.CURRENCY_MST_PK  FROM CURRENCY_TYPE_MST_TBL CURR ORDER BY CURRENCY_ID ASC";
                return objWF.GetDataSet(strSQL);
                //Manjunath  PTS ID:Sep-02  15/09/2011
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

        #endregion "Function Get Base Currency"

        #region "Function GetExchangeRateBase"

        /// <summary>
        /// Gets the exchange rate base.
        /// </summary>
        /// <returns></returns>
        public string GetExchangeRateBase()
        {
            try
            {
                WorkFlow objWF = new WorkFlow();
                //common class object.
                string strSQL = "select c.exch_rate_basis from corporate_mst_tbl c";

                return objWF.ExecuteScaler(strSQL);
                //Manjunath  PTS ID:Sep-02  15/09/2011
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

        #endregion "Function GetExchangeRateBase"

        #region "Function GetActiveCurrency"

        //this block modified by thiyagarajan on 28/11/08 for location based currency task
        /// <summary>
        /// Gets the active currency.
        /// </summary>
        /// <param name="strFromDate">The string from date.</param>
        /// <param name="strToDate">The string to date.</param>
        /// <param name="ExType">Type of the ex.</param>
        /// <param name="VoyPk">The voy pk.</param>
        /// <param name="Basecurrpk">The basecurrpk.</param>
        /// <returns></returns>
        public DataSet GetActiveCurrency(string strFromDate = "", string strToDate = "", short ExType = 1, Int64 VoyPk = 0, Int32 Basecurrpk = 0)
        {
            WorkFlow objWF = new WorkFlow();
            //common class object.

            string strSQL = null;
            //adding by thiyagarajan on 4/2/09:GAP-VEK-QFOR-007c:Customs exchange rate

            if (ExType == 1 | ExType == 3 | ExType == 4)
            {
                strSQL = "SELECT to_number(null) exchange_rate_pk,";
                strSQL += "       'true' sel,";
                strSQL += "       c.currency_id,";
                strSQL += "       c.currency_name,";
                //added by Sivachandran 14May2008 - For Exchange Rate As per Vessel/Voyage PTS
                strSQL += "       e.roe_buy,";
                strSQL += "       e.exchange_rate,";
                strSQL += "       '' version_no";
                strSQL += "FROM exchange_rate_trn e, currency_type_mst_tbl c   ";
                strSQL += "WHERE e.currency_mst_fk = c.currency_mst_pk(+)  ";
                strSQL += "AND E.EXCH_RATE_TYPE_FK = " + ExType;
                strSQL += " and E.CURRENCY_MST_BASE_FK=" + Basecurrpk;
                //HttpContext.Current.Session("CURRENCY_MST_PK") & vbCrLf
                //strSQL &= "                    -- AND to_date('" & strFromDate & "','" & dateFormat & "') BETWEEN E.FROM_DATE AND E.TO_DATE " & vbCrLf
                if (!string.IsNullOrEmpty(strFromDate))
                {
                    //modifying  by thiyagarajan on 14/8/08 to avoid duplicates on currency while page load itself
                    //strSQL &= "AND (TO_DATE('" & strFromDate & "','" & dateFormat & "') BETWEEN E.FROM_DATE AND E.TO_DATE" & vbCrLf
                    //strSQL &= "OR TO_DATE('" & strToDate & "','" & dateFormat & "') BETWEEN E.FROM_DATE AND E.TO_DATE)" & vbCrLf
                    //strSQL &= "AND (TO_DATE('" & strFromDate & "','" & dateFormat & "') BETWEEN E.FROM_DATE AND E.TO_DATE" & vbCrLf
                    //strSQL &= "AND TO_DATE('" & strToDate & "','" & dateFormat & "') BETWEEN E.FROM_DATE AND E.TO_DATE)" & vbCrLf
                    strSQL += " and (e.from_date=TO_DATE('" + strFromDate + "','" + dateFormat + "') and e.to_date =TO_DATE('" + strToDate + "','" + dateFormat + "'))";
                }
                strSQL += "Union all";
                strSQL += "SELECT to_number(null) exchange_rate_pk,";
                strSQL += "       'false' sel,";
                strSQL += "       curr.currency_id,";
                strSQL += "       curr.currency_name,";
                //added by Sivachandran 14May2008 - For Exchange Rate As per Vessel/Voyage PTS
                strSQL += "       null roe_buy,";
                strSQL += "       null exchange_rate,";
                strSQL += "       '' version_no";
                strSQL += "from currency_type_mst_tbl curr";
                strSQL += "where curr.active_flag=1";
                //modified by thiyagarajan on 18/11/08 for location based currency task
                //strSQL &= "and curr.currency_mst_pk not in ( select corp.currency_mst_fk from corporate_mst_tbl corp)" & vbCrLf
                strSQL += " and curr.currency_mst_pk not in (" + Basecurrpk + ")";
                //end
                strSQL += "and curr.currency_mst_pk not in ";
                strSQL += "(select e.currency_mst_fk ";
                strSQL += "from exchange_rate_trn e  ";
                strSQL += " where(1=1) ";
                strSQL += " AND E.EXCH_RATE_TYPE_FK = " + ExType;
                strSQL += " and E.CURRENCY_MST_BASE_FK=" + Basecurrpk;
                //HttpContext.Current.Session("CURRENCY_MST_PK") & vbCrLf
                if (!string.IsNullOrEmpty(strFromDate))
                {
                    //modifying by thiyagarajan on 14/8/08 to avoid duplicates on currency while page load itself
                    //strSQL &= " and e.from_date >= to_date('" & strFromDate & "','" & dateFormat & "')" & vbCrLf
                    //strSQL &= " and e.to_date <= NVL(to_date('" & strToDate & "','" & dateFormat & "'),NULL_DATE_FORMAT)" & vbCrLf
                    //strSQL &= "AND (TO_DATE('" & strFromDate & "','" & dateFormat & "') BETWEEN E.FROM_DATE AND E.TO_DATE" & vbCrLf
                    //strSQL &= "AND TO_DATE('" & strToDate & "','" & dateFormat & "') BETWEEN E.FROM_DATE AND E.TO_DATE)" & vbCrLf
                    strSQL += " and (e.from_date=TO_DATE('" + strFromDate + "','" + dateFormat + "') and e.to_date =TO_DATE('" + strToDate + "','" + dateFormat + "'))";
                }
                //strSQL &= " ) order by exchange_rate asc, currency_id asc"
                strSQL += " ) order by SEL DESC, currency_id asc";
                //strSQL = "SELECT" & vbCrLf
                //strSQL &= "     to_number(null) exchange_rate_pk," & vbCrLf
                //strSQL &= "     'false' sel," & vbCrLf
                //strSQL &= "     c.currency_id," & vbCrLf
                //strSQL &= "     c.currency_name," & vbCrLf
                //strSQL &= "     exchange_rate," & vbCrLf
                //strSQL &= "     '' version_no" & vbCrLf
                //strSQL &= "FROM" & vbCrLf
                //strSQL &= "     currency_type_mst_tbl c,corporate_mst_tbl corp,exchange_rate_trn e " & vbCrLf
                //strSQL &= "WHERE" & vbCrLf
                //strSQL &= " c.active_flag = 1" & vbCrLf
                //strSQL &= " AND corp.currency_mst_fk <> currency_mst_pk " & vbCrLf
                //strSQL &= " AND e.currency_mst_fk(+) = c.currency_mst_pk  " & vbCrLf
                //If strToDate = "" And strFromDate <> "" Then
                //    strSQL &= " AND to_date('" & strFromDate & "','" & dateFormat & "') BETWEEN E.FROM_DATE AND E.TO_DATE " & vbCrLf
                //ElseIf strFromDate <> "" And strToDate <> "" Then
                //    strSQL &= "  and (e.from_date >= to_date('" & strFromDate & "','" & dateFormat & "')" & vbCrLf
                //    strSQL &= "  and  e.to_date <= NVL(to_date('" & strToDate & "','" & dateFormat & "'),NULL_DATE_FORMAT))" & vbCrLf
                //End If
                //strSQL &= " order by currency_id   "

                //commented by gopi
                //strSQL &= " SELECT  distinct" & vbCrLf
                //strSQL &= " to_number(null) exchange_rate_pk," & vbCrLf
                //strSQL &= " 'false' sel," & vbCrLf
                //strSQL &= " curr.currency_id," & vbCrLf
                //strSQL &= " curr.currency_name," & vbCrLf
                //strSQL &= " exch.exchange_rate," & vbCrLf
                //strSQL &= " '' version_no" & vbCrLf
                //strSQL &= " FROM" & vbCrLf
                //strSQL &= " currency_type_mst_tbl curr, corporate_mst_tbl corp," & vbCrLf
                //strSQL &= " exchange_rate_trn exch" & vbCrLf
                //strSQL &= " WHERE" & vbCrLf
                //strSQL &= "     curr.active_flag = 1" & vbCrLf
                //strSQL &= "     AND corp.currency_mst_fk <> currency_mst_pk " & vbCrLf
                //strSQL &= " AND exch.currency_mst_fk = curr.currency_mst_pk" & vbCrLf
                //strSQL &= " order by currency_id"
            }
            else
            {
                //strSQL = " SELECT" & vbCrLf
                //strSQL &= " exchange_rate_pk," & vbCrLf
                //strSQL &= " 'false' sel," & vbCrLf
                //strSQL &= " c.currency_id," & vbCrLf
                //strSQL &= " c.currency_name," & vbCrLf
                //strSQL &= " exchange_rate," & vbCrLf
                //strSQL &= " e.version_no" & vbCrLf
                //strSQL &= " FROM" & vbCrLf
                //strSQL &= " currency_type_mst_tbl c, corporate_mst_tbl corp,exchange_rate_trn e " & vbCrLf
                //strSQL &= " WHERE" & vbCrLf
                //strSQL &= " c.active_flag = 1" & vbCrLf
                //strSQL &= " AND corp.currency_mst_fk <> currency_mst_pk" & vbCrLf
                //strSQL &= " and e.currency_mst_fk(+) = c.currency_mst_pk" & vbCrLf
                //strSQL &= " and e.voyage_trn_fk(+)=" & VoyPk & vbCrLf
                //If strToDate = "" Then
                //    strSQL &= " AND to_date('" & strFromDate & "','" & dateFormat & "') BETWEEN E.FROM_DATE AND E.TO_DATE " & vbCrLf
                //Else
                //    strSQL &= "  and (e.from_date >= to_date('" & strFromDate & "','" & dateFormat & "')" & vbCrLf
                //    strSQL &= "  and  e.to_date <= NVL(to_date('" & strToDate & "','" & dateFormat & "'),NULL_DATE_FORMAT))" & vbCrLf
                //End If
                //strSQL &= " order by currency_id"
                // added by gopi for vessel voyage on 7/5/2005
                strSQL = "SELECT to_number(null) exchange_rate_pk,";
                strSQL += "       'true' sel,";
                strSQL += "       c.currency_id,";
                strSQL += "       c.currency_name,";
                //added by Sivachandran 14May2008 - For Exchange Rate As per Vessel/Voyage PTS
                strSQL += "       e.roe_buy,";
                strSQL += "       e.exchange_rate,";
                strSQL += "       '' version_no";
                strSQL += "FROM exchange_rate_trn e, currency_type_mst_tbl c   ";
                strSQL += "WHERE e.currency_mst_fk = c.currency_mst_pk(+)  ";
                strSQL += "  and E.CURRENCY_MST_BASE_FK=" + Basecurrpk;
                //HttpContext.Current.Session("CURRENCY_MST_PK") & vbCrLf
                //commenting  by thiyagarajan on 14/8/08 to avoid duplicates on currency while page load itself
                //If VoyPk <> 0 Then

                strSQL += " and e.voyage_trn_fk(+)=" + VoyPk;
                //End If
                strSQL += "Union all";
                strSQL += "SELECT to_number(null) exchange_rate_pk,";
                strSQL += "       'false' sel,";
                strSQL += "       curr.currency_id,";
                strSQL += "       curr.currency_name,";
                //added by Sivachandran 14May2008 - For Exchange Rate As per Vessel/Voyage PTS
                strSQL += "       null roe_buy,";
                strSQL += "       null exchange_rate,";
                strSQL += "       '' version_no";
                strSQL += "from currency_type_mst_tbl curr";
                strSQL += "where curr.active_flag=1";
                //modified by thiyagarajan on 18/11/08 for location based currency task
                //strSQL &= "and curr.currency_mst_pk not in ( select corp.currency_mst_fk from corporate_mst_tbl corp)" & vbCrLf
                strSQL += " and curr.currency_mst_pk not in (" + Basecurrpk + ")";
                //end
                strSQL += "and curr.currency_mst_pk not in ";
                strSQL += "(select e.currency_mst_fk ";
                strSQL += "from exchange_rate_trn e ";
                strSQL += " where E.CURRENCY_MST_BASE_FK=" + Basecurrpk;
                //HttpContext.Current.Session("CURRENCY_MST_PK") & vbCrLf
                //commenting  by thiyagarajan on 14/8/08 to avoid duplicates on currency while page load itself
                //If VoyPk <> 0 Then
                strSQL += " and  e.voyage_trn_fk(+)=" + VoyPk;
                //End If
                //strSQL &= " ) order by exchange_rate asc, currency_id asc"
                strSQL += " ) order by SEL DESC, currency_id asc";
            }
            try
            {
                return objWF.GetDataSet(strSQL);
                //Manjunath  PTS ID:Sep-02  15/09/2011
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

        #endregion "Function GetActiveCurrency"

        #region "Function GetCurrencyForEdit"

        /// <summary>
        /// Gets the currency for edit.
        /// </summary>
        /// <param name="pk">The pk.</param>
        /// <param name="ExType">Type of the ex.</param>
        /// <returns></returns>
        public DataSet GetCurrencyForEdit(string pk = "0", string ExType = "1")
        {
            WorkFlow objWF = new WorkFlow();
            //common class object.

            string strSQL = null;

            strSQL += "SELECT ";
            strSQL += "  exch.exchange_rate_pk,";
            strSQL += "  'true' sel,";
            strSQL += "  curr.currency_id,";
            strSQL += "  curr.currency_name,";
            //added by Sivachandran 14May2008 - For Exchange Rate As per Vessel/Voyage PTS
            strSQL += "  exch.roe_buy,";
            strSQL += "  exch.exchange_rate,";
            strSQL += "  exch.version_no";
            strSQL += "FROM";
            strSQL += "  currency_type_mst_tbl curr,";
            strSQL += "  exchange_rate_trn exch";
            strSQL += "WHERE";
            strSQL += "  exch.currency_mst_fk = curr.currency_mst_pk ";

            if ((ExType != null))
            {
                if ((!string.IsNullOrEmpty(ExType) | Convert.ToInt32(ExType) != 0))
                {
                    strSQL += " AND EXCH.EXCH_RATE_TYPE_FK = " + ExType;
                }
            }
            if ((pk != null))
            {
                if ((!string.IsNullOrEmpty(pk) | Convert.ToInt32(pk) != 0))
                {
                    strSQL += "  AND exch.exchange_rate_pk = " + pk;
                }
            }
            // strSQL &= "   exch.currency_mst_fk = curr.currency_mst_pk" & vbCrLf
            try
            {
                return objWF.GetDataSet(strSQL);
                //Manjunath  PTS ID:Sep-02  15/09/2011
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

        #endregion "Function GetCurrencyForEdit"

        #region "Function GetCurrencyForEdit"

        //Added By Sivachandran 13May2008 - For Exchange Rate As per Vessel Voyage
        /// <summary>
        /// Gets the exchange rate.
        /// </summary>
        /// <param name="VoyPk">The voy pk.</param>
        /// <returns></returns>
        public DataSet GetExchangeRate(Int64 VoyPk = 0)
        {
            WorkFlow objWF = new WorkFlow();
            //common class object.
            string strSQL = null;

            strSQL = "SELECT  e.exchange_rate_pk,";
            strSQL += "       c.currency_id,";
            strSQL += "       c.currency_name,";
            strSQL += "       e.roe_buy,";
            strSQL += "       e.exchange_rate,";
            strSQL += "       'true' sel,";
            strSQL += "       e.version_no";
            strSQL += "FROM exchange_rate_trn e, currency_type_mst_tbl c   ";
            strSQL += "WHERE e.currency_mst_fk = c.currency_mst_pk(+)  ";
            if (VoyPk != 0)
            {
                strSQL += " and e.voyage_trn_fk(+)=" + VoyPk;
            }
            strSQL += "Union all";
            strSQL += "SELECT to_number(null) exchange_rate_pk,";
            strSQL += "       curr.currency_id,";
            strSQL += "       curr.currency_name,";
            strSQL += "       null roe_buy,";
            strSQL += "       null exchange_rate,";
            strSQL += "       'false' sel,";
            strSQL += "       null version_no";
            strSQL += "from currency_type_mst_tbl curr";
            strSQL += "where curr.active_flag=1";
            strSQL += "and curr.currency_mst_pk not in ( select corp.currency_mst_fk from corporate_mst_tbl corp)";
            strSQL += "and curr.currency_mst_pk not in ";
            strSQL += "(select e.currency_mst_fk ";
            strSQL += "from exchange_rate_trn e ";
            if (VoyPk != 0)
            {
                strSQL += " where e.voyage_trn_fk(+)=" + VoyPk;
            }
            strSQL += " ) order by exchange_rate asc, currency_id asc";
            try
            {
                return objWF.GetDataSet(strSQL);
                //Manjunath  PTS ID:Sep-02  15/09/2011
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

        #endregion "Function GetCurrencyForEdit"

        #region "Function GetHeaderDetailsForEdit"

        /// <summary>
        /// Gets the header details for edit.
        /// </summary>
        /// <param name="pk">The pk.</param>
        /// <param name="ExType">Type of the ex.</param>
        /// <returns></returns>
        public DataSet GetHeaderDetailsForEdit(string pk = "0", string ExType = "1")
        {
            WorkFlow objWF = new WorkFlow();
            //common class object.

            string strSQL = null;

            strSQL += "SELECT ";
            strSQL += "     exch.from_date, exch.to_date";
            strSQL += "FROM";
            strSQL += "     exchange_rate_trn exch";
            strSQL += "WHERE";
            strSQL += "     exch.exchange_rate_pk = " + pk;
            if ((ExType != null))
            {
                if ((!string.IsNullOrEmpty(ExType) | Convert.ToInt32(ExType) != 0))
                {
                    strSQL += " AND EXCH.EXCH_RATE_TYPE_FK = " + ExType;
                }
            }
            try
            {
                return objWF.GetDataSet(strSQL);
                //Manjunath  PTS ID:Sep-02  15/09/2011
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

        #endregion "Function GetHeaderDetailsForEdit"

        #region "Function GetVesselDetails"

        /// <summary>
        /// Gets the vessel details.
        /// </summary>
        /// <param name="pk">The pk.</param>
        /// <returns></returns>
        public DataSet GetVesselDetails(string pk = "0")
        {
            WorkFlow objWF = new WorkFlow();
            //common class object.

            string strSQL = null;

            strSQL += "SELECT ";
            strSQL += "vvt.voyage_trn_pk, v.vessel_name, vvt.voyage";
            strSQL += "FROM";
            strSQL += "exchange_rate_trn exch, vessel_voyage_tbl v, vessel_voyage_trn vvt";
            strSQL += "WHERE";
            strSQL += "exch.voyage_trn_fk = vvt.voyage_trn_pk";
            strSQL += "and v.vessel_voyage_tbl_pk = vvt.vessel_voyage_tbl_fk";
            //strSQL &= "and exch.exchange_rate_pk=" & pk
            if ((pk != null))
            {
                if ((!string.IsNullOrEmpty(pk) | Convert.ToInt32(pk) != 0))
                {
                    strSQL += "  AND exch.exchange_rate_pk = " + pk;
                }
            }
            try
            {
                return objWF.GetDataSet(strSQL);
                //Manjunath  PTS ID:Sep-02  15/09/2011
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

        #endregion "Function GetVesselDetails"

        #region "Function GetCurrencyAfterSave"

        //this block modified by thiyagarajan on 28/11/08 for location based currency task
        /// <summary>
        /// Gets the currency after save.
        /// </summary>
        /// <param name="fromDate">From date.</param>
        /// <param name="toDate">To date.</param>
        /// <param name="ExType">Type of the ex.</param>
        /// <param name="VoyPk">The voy pk.</param>
        /// <param name="basecurrpk">The basecurrpk.</param>
        /// <returns></returns>
        public DataSet GetCurrencyAfterSave(string fromDate, string toDate, short ExType, Int64 VoyPk = 0, Int32 basecurrpk = 0)
        {
            WorkFlow objWF = new WorkFlow();
            //common class object.

            string strSQL = null;

            //strSQL &= "SELECT " & vbCrLf
            //strSQL &= "exch.exchange_rate_pk," & vbCrLf
            //strSQL &= "'true' sel," & vbCrLf
            //strSQL &= "curr.currency_id," & vbCrLf
            //strSQL &= "curr.currency_name," & vbCrLf
            //strSQL &= "exch.exchange_rate," & vbCrLf
            //strSQL &= "exch.version_no" & vbCrLf
            //strSQL &= "FROM" & vbCrLf
            //strSQL &= "currency_type_mst_tbl curr," & vbCrLf
            //strSQL &= "exchange_rate_trn exch" & vbCrLf
            //strSQL &= "WHERE" & vbCrLf
            //strSQL &= "exch.currency_mst_fk = curr.currency_mst_pk" & vbCrLf
            //'strSQL &= "AND (TO_DATE('" & fromDate & "','" & dateFormat & "') BETWEEN EXCH.FROM_DATE AND EXCH.TO_DATE" & vbCrLf
            //'strSQL &= "OR TO_DATE('" & toDate & "','" & dateFormat & "') BETWEEN EXCH.FROM_DATE AND EXCH.TO_DATE) order by currency_id " & vbCrLf
            //If ExType = 1 Then
            //    strSQL &= " AND EXCH.VOYAGE_TRN_FK= V.VESSEL_VOYAGE_TBL_PK" & vbCrLf
            //    strSQL &= " AND EXCH.VOYAGE_TRN_FK IS NULL" & vbCrLf
            //    strSQL &= " AND (TO_DATE('" & fromDate & "','" & dateFormat & "') BETWEEN EXCH.FROM_DATE AND EXCH.TO_DATE" & vbCrLf
            //    strSQL &= " OR TO_DATE('" & toDate & "','" & dateFormat & "') BETWEEN EXCH.FROM_DATE AND EXCH.TO_DATE) order by currency_id " & vbCrLf

            //Else
            //    strSQL &= " AND EXCH.VOYAGE_TRN_FK= V.VESSEL_VOYAGE_TBL_PK" & vbCrLf
            //    strSQL &= " AND EXCH.VOYAGE_TRN_FK IS NOT NULL" & vbCrLf
            //End If

            //adding by thiyagarajan on 4/2/09:GAP-VEK-QFOR-007c:Customs exchange rate
            if (ExType == 1 | ExType == 3 | ExType == 4)
            {
                strSQL += " SELECT ";
                strSQL += " exch.exchange_rate_pk,";
                strSQL += " 'true' sel,";
                strSQL += " curr.currency_id,";
                strSQL += " curr.currency_name,";
                //added by Sivachandran 13 MAy2008 - For Exchange Rate As per Vessel/Voyage PTS
                strSQL += " exch.roe_buy,";
                strSQL += " exch.exchange_rate,";
                strSQL += " exch.version_no";
                strSQL += " FROM";
                strSQL += " currency_type_mst_tbl curr,";
                strSQL += " exchange_rate_trn exch";
                strSQL += " WHERE";
                strSQL += " exch.currency_mst_fk = curr.currency_mst_pk";

                strSQL += " and exch.CURRENCY_MST_BASE_FK=" + basecurrpk;
                //HttpContext.Current.Session("CURRENCY_MST_PK") & vbCrLf
                //modified by thiyagarajan on 18/11/08 for location based currency task
                strSQL += " and curr.currency_mst_pk not in (" + basecurrpk + ")";
                //end
                //strSQL &= " AND EXCH.VOYAGE_TRN_FK= V.VESSEL_VOYAGE_TBL_PK" & vbCrLf
                strSQL += " AND EXCH.VOYAGE_TRN_FK IS NULL";
                strSQL += "AND EXCH.EXCH_RATE_TYPE_FK = " + ExType;
                //modifying by thiyagarajan on 19/8/08 to avoid get unnecessary rec. after saving
                //strSQL &= " AND (TO_DATE('" & fromDate & "','" & dateFormat & "') BETWEEN EXCH.FROM_DATE AND EXCH.TO_DATE" & vbCrLf
                //strSQL &= " and TO_DATE('" & toDate & "','" & dateFormat & "') BETWEEN EXCH.FROM_DATE AND EXCH.TO_DATE) order by currency_id " & vbCrLf
                strSQL += " and (exch.from_date=TO_DATE('" + fromDate + "','" + dateFormat + "') and exch.to_date =TO_DATE('" + toDate + "','" + dateFormat + "'))";
            }
            else if (ExType == 2)
            {
                strSQL += " SELECT ";
                strSQL += " exch.exchange_rate_pk,";
                strSQL += " 'true' sel,";
                strSQL += " curr.currency_id,";
                strSQL += " curr.currency_name,";
                //added by Sivachandran 13May2008 - For Exchange Rate As per Vessel/Voyage PTS
                strSQL += " exch.roe_buy,";
                strSQL += " exch.exchange_rate,";
                strSQL += " exch.version_no";
                strSQL += " FROM";
                strSQL += " currency_type_mst_tbl curr,";
                strSQL += " exchange_rate_trn exch, VESSEL_VOYAGE_TRN VVT";
                strSQL += " WHERE";
                strSQL += " exch.currency_mst_fk = curr.currency_mst_pk ";
                strSQL += " and exch.CURRENCY_MST_BASE_FK=" + basecurrpk;
                //HttpContext.Current.Session("CURRENCY_MST_PK") & vbCrLf
                //modified by thiyagarajan on 18/11/08 for location based currency task
                strSQL += " and curr.currency_mst_pk not in (" + basecurrpk + ")";
                //end
                strSQL += " AND EXCH.VOYAGE_TRN_FK= VVT.VOYAGE_TRN_PK ";
                strSQL += " AND EXCH.VOYAGE_TRN_FK IS NOT NULL ";
                strSQL += " AND EXCH.VOYAGE_TRN_FK=" + VoyPk;
            }
            try
            {
                return objWF.GetDataSet(strSQL);
                //Manjunath  PTS ID:Sep-02  15/09/2011
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

        #endregion "Function GetCurrencyAfterSave"

        #region "Fetch Voyage and Vessel PK's"

        /// <summary>
        /// Fetches the voy pk for VSL voy.
        /// </summary>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="Voyage">The voyage.</param>
        /// <returns></returns>
        public Int64 FetchVoyPkForVslVoy(string VslName, string Voyage)
        {
            WorkFlow objWF = new WorkFlow();
            //common class object.
            System.Text.StringBuilder strSql = new System.Text.StringBuilder();
            try
            {
                strSql.Append(" select");
                strSql.Append(" vt.voyage_trn_pk");
                strSql.Append(" from");
                strSql.Append(" vessel_voyage_tbl v,");
                strSql.Append(" vessel_voyage_trn vt");
                strSql.Append(" where");
                strSql.Append(" v.vessel_voyage_tbl_pk = vt.vessel_voyage_tbl_fk");
                strSql.Append(" and v.vessel_name ='" + VslName + "'");
                strSql.Append(" and vt.voyage ='" + Voyage + "'");
                return Convert.ToInt64(objWF.ExecuteScaler(strSql.ToString()));
                //Manjunath  PTS ID:Sep-02  15/09/2011
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

        #endregion "Fetch Voyage and Vessel PK's"

        #region "Get exchange_rate_pk for the particular Voyage, if present"

        /// <summary>
        /// Gets the ex rate pk.
        /// </summary>
        /// <param name="Voyage">The voyage.</param>
        /// <returns></returns>
        public Int64 GetExRatePK(string Voyage)
        {
            try
            {
                WorkFlow objWF = new WorkFlow();
                DataSet VoyDS = null;
                System.Text.StringBuilder sb = new System.Text.StringBuilder();
                System.Text.StringBuilder strSql = new System.Text.StringBuilder();
                strSql.Append(" select * from exchange_rate_trn exch where exch.voyage_trn_fk = " + Voyage);
                VoyDS = objWF.GetDataSet(strSql.ToString());
                if (VoyDS.Tables[0].Rows.Count > 0)
                {
                    sb.Append(" select exch.exchange_rate_pk from exchange_rate_trn exch where exch.voyage_trn_fk = " + Voyage);
                    return Convert.ToInt64(objWF.ExecuteScaler(sb.ToString()));
                }
                else
                {
                    return 0;
                }
                //Manjunath  PTS ID:Sep-02  15/09/2011
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

        #endregion "Get exchange_rate_pk for the particular Voyage, if present"

        #region "Exchange Rate Status"

        /// <summary>
        /// Gets the exch rate status.
        /// </summary>
        /// <param name="ExRatePk">The ex rate pk.</param>
        /// <returns></returns>
        public short GetExchRateStatus(int ExRatePk)
        {
            WorkFlow objwf = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT COUNT(*)");
            sb.Append("  FROM EXCHANGE_RATE_TRN ER");
            sb.Append(" WHERE ER.EXCHANGE_RATE_PK = " + ExRatePk);
            sb.Append("   AND TO_DATE(ER.TO_DATE, 'DD.MM/YYYY') >= TO_DATE(SYSDATE, 'DD/MM/YYYY')");
            //IF Count is 1 then ACTIVE else HISTORY
            return Convert.ToInt16(objwf.ExecuteScaler(sb.ToString()));
        }

        #endregion "Exchange Rate Status"
    }
}