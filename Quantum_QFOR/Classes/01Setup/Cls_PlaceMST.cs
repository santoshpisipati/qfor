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

using Oracle.DataAccess.Client;
using System;
using System.Collections;
using System.Data;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class ClsPlace_MST_TBL : CommonFeatures
    {
        #region "List of Members of the Class"

        /// <summary>
        /// The m_ place_ pk
        /// </summary>
        private Int64 M_Place_Pk;

        /// <summary>
        /// The m_ place_ code
        /// </summary>
        private string M_Place_Code;

        /// <summary>
        /// The m_ place_ name
        /// </summary>
        private string M_Place_Name;

        /// <summary>
        /// The m_ dist_ from_ ware house
        /// </summary>
        private string M_Dist_From_WareHouse;

        #endregion "List of Members of the Class"

        #region "List of Properties"

        /// <summary>
        /// Gets or sets the place_ pk.
        /// </summary>
        /// <value>
        /// The place_ pk.
        /// </value>
        public Int64 Place_Pk
        {
            get { return M_Place_Pk; }
            set { M_Place_Pk = value; }
        }

        /// <summary>
        /// Gets or sets the place_ code.
        /// </summary>
        /// <value>
        /// The place_ code.
        /// </value>
        public string Place_Code
        {
            get { return M_Place_Code; }
            set { M_Place_Code = value; }
        }

        /// <summary>
        /// Gets or sets the name of the place_.
        /// </summary>
        /// <value>
        /// The name of the place_.
        /// </value>
        public string Place_Name
        {
            get { return M_Place_Name; }
            set { M_Place_Name = value; }
        }

        /// <summary>
        /// Gets or sets the dist_ from_ ware house.
        /// </summary>
        /// <value>
        /// The dist_ from_ ware house.
        /// </value>
        public string Dist_From_WareHouse
        {
            get { return M_Dist_From_WareHouse; }
            set { M_Dist_From_WareHouse = value; }
        }

        #endregion "List of Properties"

        #region "Fetch All"

        //objCountry.FetchAll(CountryID, CountryName, CurrencyID, CurrencyName, SearchType, _
        //            SortColumn, CurrentPage, TotalPage, ActiveFlag, SortType)

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="PlaceCode">The place code.</param>
        /// <param name="PlaceName">Name of the place.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="SortColumn">The sort column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="ActiveFlag">if set to <c>true</c> [active flag].</param>
        /// <param name="SortType">Type of the sort.</param>
        /// <param name="Location">The location.</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public DataSet FetchAll(string PlaceCode = "", string PlaceName = "", string SearchType = "", string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, bool ActiveFlag = true, string SortType = " ASC ", string Location = "", Int32 flag = 0)
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
            if (Location.Length > 0)
            {
                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition += " AND UPPER(LOCATION_ID) LIKE '" + Location.ToUpper().Replace("'", "''") + "%'";
                    }
                    else
                    {
                        strCondition += " AND UPPER(LOCATION_ID) LIKE '%" + Location.ToUpper().Replace("'", "''") + "%'";
                    }
                }
                else
                {
                    strCondition += " AND UPPER(LOCATION_ID) LIKE '%" + Location.ToUpper().Replace("'", "''") + "%'";
                }
                //    strCondition &= vbCrLf & " AND EXISTS "
                //    strCondition &= vbCrLf & " (SELECT LOCATION_ID FROM LOCATION_MST_TBL L "
                //    strCondition &= vbCrLf & "  WHERE UPPER(LOCATION_ID) LIKE '%" & Location.ToUpper.Replace("'", "''") & "%' "
                //    strCondition &= vbCrLf & "  AND LOCATION_MST_PK IN "
                //    strCondition &= vbCrLf & "    ( SELECT LOCATION_MST_FK FROM PLACE_MST_TBL PL "
                //    strCondition &= vbCrLf & "      WHERE LOCATION_MST_PK = PL.LOCATION_MST_FK "
                //    strCondition &= vbCrLf & "     ) "
                //    strCondition &= vbCrLf & "  ) "
            }

            if (PlaceCode.Trim().Length > 0)
            {
                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition += " AND UPPER(PLACE_CODE) LIKE '" + PlaceCode.ToUpper().Replace("'", "''") + "%'";
                    }
                    else
                    {
                        strCondition += " AND UPPER(PLACE_CODE) LIKE '%" + PlaceCode.ToUpper().Replace("'", "''") + "%'";
                    }
                }
                else
                {
                    strCondition += " AND UPPER(PLACE_CODE) LIKE '%" + PlaceCode.ToUpper().Replace("'", "''") + "%'";
                }
            }

            if (PlaceName.Trim().Length > 0)
            {
                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition += " AND UPPER(PLACE_NAME) LIKE '" + PlaceName.ToUpper().Replace("'", "''") + "%'";
                    }
                    else
                    {
                        strCondition += " AND UPPER(PLACE_NAME) LIKE '%" + PlaceName.ToUpper().Replace("'", "''") + "%'";
                    }
                }
                else
                {
                    strCondition += " AND UPPER(PLACE_NAME) LIKE '%" + PlaceName.ToUpper().Replace("'", "''") + "%'";
                }
            }

            // strCondition &= vbCrLf & " AND L.ACTIVE_FLAG = 1 "

            if (ActiveFlag == true)
            {
                strCondition += " AND PL.ACTIVE_FLAG = 1 ";
            }
            else
            {
                strCondition += " ";
            }
            strCondition += " AND PL.Location_Mst_Fk = L.LOCATION_MST_PK(+) ";
            strSQL = "SELECT Count(*) from PLACE_MST_TBL PL,LOCATION_MST_TBL L where 1=1";
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
            // SR_NO, Place_Pk, ACTIVE_FLAG, Place_Code, Place_Name
            // Dist_From_WareHouse, Version_No

            strSQL = " select * from (";
            strSQL += "SELECT ROWNUM SR_NO,q.* FROM ";
            strSQL += "(SELECT  ";
            strSQL += "PL.PLACE_PK, ";
            strSQL += "NVL(PL.ACTIVE_FLAG,0) ACTIVE_FLAG , ";
            strSQL += "PL.PLACE_CODE, ";
            strSQL += "PL.Place_Name PLACE_NAME, ";
            strSQL += "PL.DIST_FROM_WAREHOUSE, ";
            strSQL += "PL.VERSION_NO,  ";
            strSQL += "L.LOCATION_NAME,  ";
            strSQL += "PL.LOCATION_MST_FK  ";
            strSQL += "FROM PLACE_MST_TBL PL, ";
            strSQL += "LOCATION_MST_TBL L";
            strSQL += "WHERE 1=1 ";
            strSQL += strCondition;
            strSQL += " order by " + SortColumn + SortType + " ) q  ) ";
            strSQL += " WHERE SR_NO  Between " + start + " and " + last;
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

        //fatch places for Zone trn form
        /// <summary>
        /// Fetches the places.
        /// </summary>
        /// <param name="strColumnName">Name of the string column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="ActiveFlag">if set to <c>true</c> [active flag].</param>
        /// <param name="blnSortAscending">if set to <c>true</c> [BLN sort ascending].</param>
        /// <param name="placePkgroup">The place pkgroup.</param>
        /// <param name="placePknongroup">The place pknongroup.</param>
        /// <returns></returns>
        public DataSet FetchPlaces(string strColumnName = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, bool ActiveFlag = true, bool blnSortAscending = true, string placePkgroup = "", string placePknongroup = "")
        {
            int i = 0;
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strQuery = null;
            string strCondition = "";
            string strCondition1 = "";
            string strCondition2 = "";
            Int32 TotalRecords = default(Int32);
            placePkgroup = placePkgroup.TrimEnd(',');

            WorkFlow objWF = new WorkFlow();

            if (placePkgroup.Length > 0)
            {
                string[] placepk = null;
                placepk = placePkgroup.Split(',');
                strCondition += "and P.PLACE_PK not in (" + placePkgroup + " )";
                strCondition1 += "and P.PLACE_PK in (" + placePkgroup + " )";
            }
            if (placePknongroup.Length > 0)
            {
                string[] placePknone = null;
                placePknone = placePknongroup.Split(',');
                strCondition2 = "and P.PLACE_PK not in (" + placePknongroup + " )";
            }

            if (ActiveFlag == true)
            {
                strCondition += " AND P.ACTIVE_FLAG = 1 ";
            }
            else
            {
                strCondition += " ";
            }
            strSQL = "select * from  ";
            strSQL += "(SELECT ROWNUM SR_NO,q.* FROM (";
            if (!string.IsNullOrEmpty(strCondition) & !string.IsNullOrEmpty(strCondition1))
            {
                strSQL += " SELECT  ";
                strSQL += " P.PLACE_PK, ";
                strSQL += " P.PLACE_NAME ,";
                strSQL += " L.LOCATION_NAME,";
                strSQL += " 1 active";
                strSQL += " FROM PLACE_MST_TBL P,Location_Mst_Tbl L ";
                strSQL += " WHERE(1 = 1) AND P.location_mst_fk=L.location_mst_pk ";
                strSQL += strCondition1;
                strSQL += " union ";
            }
            strSQL += "SELECT ";
            strSQL += " P.PLACE_PK, ";
            strSQL += " P.PLACE_NAME ,";
            strSQL += " L.LOCATION_NAME,";
            strSQL += " 0 active";
            strSQL += " FROM PLACE_MST_TBL P,Location_Mst_Tbl L ";
            strSQL += " WHERE(1 = 1) AND P.location_mst_fk=L.location_mst_pk ";
            strSQL += strCondition;
            strSQL += strCondition2;
            if (!strColumnName.Equals("SR_NO"))
            {
                strSQL += "order by  Active desc,  " + strColumnName;
            }
            else
            {
                strSQL += "order by PLACE_NAME";
            }

            if (!blnSortAscending & !strColumnName.Equals("SR_NO"))
            {
                strSQL += " DESC";
            }
            strSQL += ")q )";

            strQuery = "SELECT Count(*) from (";
            strQuery += strSQL + ")";
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strQuery));
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

            strSQL += " WHERE SR_NO  Between " + start + " and " + last;

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

        #region "Fetch Place Master "

        /// <summary>
        /// Fetches the place master.
        /// </summary>
        /// <param name="PlacePK">The place pk.</param>
        /// <param name="PlaceCode">The place code.</param>
        /// <param name="PlaceName">Name of the place.</param>
        /// <param name="ActiveOnly">if set to <c>true</c> [active only].</param>
        /// <returns></returns>
        public DataSet FetchPlaceMaster(Int64 PlacePK = 0, string PlaceCode = "", string PlaceName = "", bool ActiveOnly = true)
        {
            string strSQL = null;
            strSQL = "select ' ' Place_Code,";
            strSQL = strSQL + "' ' Place_Name, ";
            strSQL = strSQL + "0 Place_PK,";
            strSQL = strSQL + "0 Dist_From_WareHouse";
            strSQL = strSQL + "from Place_MST_TBL ";
            strSQL = strSQL + "UNION ";
            strSQL = strSQL + "Select Place_Code, ";
            strSQL = strSQL + "Place_Name,";
            strSQL = strSQL + "Place_PK,";
            strSQL = strSQL + "Dist_From_WareHouse";
            strSQL = strSQL + "from Place_MST_TBL Where 1=1 ";
            if (ActiveOnly)
            {
                strSQL = strSQL + " And Active_Flag = 1  ";
            }
            strSQL = strSQL + "order by Product_Code";
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

        #endregion "Fetch Place Master "

        #region "Save Function"

        /// <summary>
        /// Saves the specified m_ data set.
        /// </summary>
        /// <param name="M_DataSet">The m_ data set.</param>
        /// <param name="Import">if set to <c>true</c> [import].</param>
        /// <returns></returns>
        public ArrayList Save(DataSet M_DataSet, bool Import = false)
        {
            //sivachandran 05Jun08 Imp-Exp-Wiz 16May08
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
                DtTbl = M_DataSet.Tables[0];
                string INS_Proc = null;
                string DEL_Proc = null;
                string UPD_Proc = null;
                INS_Proc = objWK.MyUserName + ".Place_MST_TBL_PKG.Place_MST_TBL_INS";
                DEL_Proc = objWK.MyUserName + ".Place_MST_TBL_PKG.Place_MST_TBL_DEL";
                UPD_Proc = objWK.MyUserName + ".Place_MST_TBL_PKG.Place_MST_TBL_UPD";

                var _with1 = insCommand;
                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = INS_Proc;

                _with1.Parameters.Add("PLACE_CODE_IN", OracleDbType.Varchar2, 5, "Place_Code").Direction = ParameterDirection.Input;
                _with1.Parameters["PLACE_CODE_IN"].SourceVersion = DataRowVersion.Current;

                _with1.Parameters.Add("PLACE_NAME_IN", OracleDbType.Varchar2, 50, "Place_Name").Direction = ParameterDirection.Input;
                _with1.Parameters["PLACE_NAME_IN"].SourceVersion = DataRowVersion.Current;

                _with1.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "LOCATION_MST_FK").Direction = ParameterDirection.Input;
                _with1.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with1.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;

                _with1.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                //.Parameters["CONFIG_PK_IN"].SourceVersion = DataRowVersion.Current

                _with1.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
                _with1.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;

                //insCommand.Parameters.Add("VERSION_NO_IN", 10).Direction = ParameterDirection.Input
                //insCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current

                _with1.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "Place_PK").Direction = ParameterDirection.Output;
                _with1.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with2 = delCommand;
                _with2.Connection = objWK.MyConnection;
                _with2.CommandType = CommandType.StoredProcedure;
                _with2.CommandText = DEL_Proc;

                _with2.Parameters.Add("Place_PK_IN", OracleDbType.Int32, 10, "Place_PK").Direction = ParameterDirection.Input;
                _with2.Parameters["Place_PK_IN"].SourceVersion = DataRowVersion.Current;

                _with2.Parameters.Add("DELETED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

                _with2.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                _with2.Parameters["CONFIG_PK_IN"].SourceVersion = DataRowVersion.Current;

                //Concurrency Control added on 12/9/2005 by Mikky

                _with2.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                _with2.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                _with2.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with2.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with3 = updCommand;
                _with3.Connection = objWK.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = UPD_Proc;

                _with3.Parameters.Add("Place_PK_IN", OracleDbType.Int32, 10, "Place_PK").Direction = ParameterDirection.Input;
                _with3.Parameters["Place_PK_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("Place_Code_IN", OracleDbType.Varchar2, 5, "Place_Code").Direction = ParameterDirection.Input;
                _with3.Parameters["Place_Code_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("Place_NAME_IN", OracleDbType.Varchar2, 50, "Place_Name").Direction = ParameterDirection.Input;
                _with3.Parameters["Place_Name_IN"].SourceVersion = DataRowVersion.Current;

                //.Parameters.Add("Dist_From_WareHouse_IN", _
                //                 OracleClient.OracleDbType.Int32, 5, _
                //                 "Dist_From_WareHouse").Direction = ParameterDirection.Input
                //.Parameters["Dist_From_WareHouse_IN"].SourceVersion = DataRowVersion.Current

                _with3.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "LOCATION_MST_FK").Direction = ParameterDirection.Input;
                _with3.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
                _with3.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

                _with3.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                _with3.Parameters["CONFIG_PK_IN"].SourceVersion = DataRowVersion.Current;

                //Concurrency Control added on 12/9/2005 by Mikky

                _with3.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                _with3.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with3.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with4 = objWK.MyDataAdapter;

                _with4.InsertCommand = insCommand;
                _with4.InsertCommand.Transaction = TRAN;
                _with4.UpdateCommand = updCommand;
                _with4.UpdateCommand.Transaction = TRAN;
                _with4.DeleteCommand = delCommand;
                _with4.DeleteCommand.Transaction = TRAN;
                RecAfct = _with4.Update(M_DataSet);

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    TRAN.Commit();
                    //arrMessage.Add("All Data Saved Successfully") 'sivachandran 05Jun08 Imp-Exp-Wiz 16May08
                    //Return arrMessage
                    if (Import == true)
                    {
                        arrMessage.Add("Data Imported Successfully");
                    }
                    else
                    {
                        arrMessage.Add("All Data Saved Successfully");
                    }
                    return arrMessage;
                    //End
                }
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWK.CloseConnection();
            }
        }

        #endregion "Save Function"
    }
}