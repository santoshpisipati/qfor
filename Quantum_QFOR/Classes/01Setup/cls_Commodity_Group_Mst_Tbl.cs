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

using Newtonsoft.Json;
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
    public class cls_Commodity_Group_Mst_Tbl : CommonFeatures
    {
        //Private arrMessage As New ArrayList

        #region "List of Members of Class"

        /// <summary>
        /// The m_ commodit y_ grou p_ pk
        /// </summary>
        private Int16 M_COMMODITY_GROUP_PK;

        /// <summary>
        /// The m_ commodit y_ grou p_ code
        /// </summary>
        private string M_COMMODITY_GROUP_CODE;

        /// <summary>
        /// The m_ commodit y_ grou p_ desc
        /// </summary>
        private string M_COMMODITY_GROUP_DESC;

        /// <summary>
        /// The m_ data set
        /// </summary>
        private static DataSet M_DataSet = new DataSet();

        #endregion "List of Members of Class"

        #region "List of Properties"

        /// <summary>
        /// Gets or sets the comm GRP_ pk.
        /// </summary>
        /// <value>
        /// The comm GRP_ pk.
        /// </value>
        public Int16 CommGrp_Pk
        {
            get { return M_COMMODITY_GROUP_PK; }
            set { M_COMMODITY_GROUP_PK = value; }
        }

        /// <summary>
        /// Gets or sets the comm GRP_ code.
        /// </summary>
        /// <value>
        /// The comm GRP_ code.
        /// </value>
        public string CommGrp_Code
        {
            get { return M_COMMODITY_GROUP_CODE; }
            set { M_COMMODITY_GROUP_CODE = value; }
        }

        /// <summary>
        /// Gets or sets the comm GRP_ desc.
        /// </summary>
        /// <value>
        /// The comm GRP_ desc.
        /// </value>
        public string CommGrp_Desc
        {
            get { return M_COMMODITY_GROUP_DESC; }
            set { M_COMMODITY_GROUP_DESC = value; }
        }

        /// <summary>
        /// Gets my data set.
        /// </summary>
        /// <value>
        /// My data set.
        /// </value>
        public static DataSet MyDataSet
        {
            get { return M_DataSet; }
        }

        #endregion "List of Properties"

        #region "Constructor"

        //Modified by Mani
        /// <summary>
        /// Initializes a new instance of the <see cref="cls_Commodity_Group_Mst_Tbl"/> class.
        /// </summary>
        /// <param name="SelectAll">if set to <c>true</c> [select all].</param>
        public cls_Commodity_Group_Mst_Tbl(bool SelectAll)
        {
            WorkFlow objWF = new WorkFlow();
            string Sql = null;
                      //Gopi  ****************************** For "<ALL>" ****************
            if (SelectAll)
            {
                Sql += " SELECT COMMODITY_GROUP_PK,COMMODITY_GROUP_CODE,COMMODITY_GROUP_DESC,VERSION_NO";
                Sql += " FROM (select 0 COMMODITY_GROUP_PK,";
                Sql += " '<ALL>' COMMODITY_GROUP_CODE, ";
                Sql += " ' ' COMMODITY_GROUP_DESC, ";
                Sql += " 0 VERSION_NO,0 PREFERENCE from dual UNION ";
            }
            else
            {
                Sql += " SELECT COMMODITY_GROUP_PK,COMMODITY_GROUP_CODE,COMMODITY_GROUP_DESC,VERSION_NO";
                Sql += " FROM (select 0 COMMODITY_GROUP_PK,";
                Sql += " ' ' COMMODITY_GROUP_CODE, ";
                Sql += " ' ' COMMODITY_GROUP_DESC, ";
                Sql += " 0 VERSION_NO,0 PREFERENCE from dual UNION ";
            }

            Sql += " SELECT CG.COMMODITY_GROUP_PK,CG.COMMODITY_GROUP_CODE, ";
            Sql += " CG.COMMODITY_GROUP_DESC,CG.VERSION_NO,CG.PREFERENCE ";
            Sql += " FROM COMMODITY_GROUP_MST_TBL CG ";
            Sql += " WHERE CG.ACTIVE_FLAG=1) ";
            Sql += " ORDER BY PREFERENCE ";
            try
            {
                M_DataSet = objWF.GetDataSet(Sql);
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
        public cls_Commodity_Group_Mst_Tbl()
        {
                
        }

        #endregion "Constructor"

        #region "Fetch Function"

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="P_Commodity_Group_Pk">The p_ commodity_ group_ pk.</param>
        /// <param name="P_Commodity_Group_Code">The p_ commodity_ group_ code.</param>
        /// <param name="P_Commodity_Group_Desc">The p_ commodity_ group_ desc.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="strColumnName">Name of the string column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="SortCol">The sort col.</param>
        /// <param name="isActive">The is active.</param>
        /// <param name="blnSortAscending">if set to <c>true</c> [BLN sort ascending].</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public string FetchAll(Int16 P_Commodity_Group_Pk = 0, string P_Commodity_Group_Code = "", string P_Commodity_Group_Desc = "", string SearchType = "", string strColumnName = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int16 SortCol = 2, Int16 isActive = -1, bool blnSortAscending = false,
        Int32 flag = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string SQLQuery = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            if (flag == 0)
            {
                strCondition += " AND 1=2";
            }

            if (P_Commodity_Group_Pk > 0)
            {
                strCondition = strCondition + "AND COMMODITY_GROUP_PK= " + P_Commodity_Group_Pk;
            }

            if (!string.IsNullOrEmpty(P_Commodity_Group_Desc) && P_Commodity_Group_Code.Trim().Length > 0)
            {
                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition += " AND UPPER(COMMODITY_GROUP_CODE) LIKE '" + P_Commodity_Group_Code.ToUpper().Replace("'", "''") + "%'";
                    }
                    else
                    {
                        strCondition += " AND UPPER(COMMODITY_GROUP_CODE) LIKE '%" + P_Commodity_Group_Code.ToUpper().Replace("'", "''") + "%'";
                    }
                }
                else
                {
                    strCondition += " AND UPPER(COMMODITY_GROUP_CODE) LIKE '%" + P_Commodity_Group_Code.ToUpper().Replace("'", "''") + "%'";
                }
            }

            if (!string.IsNullOrEmpty(P_Commodity_Group_Desc) && P_Commodity_Group_Desc.Trim().Length > 0)
            {
                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition += " AND UPPER(COMMODITY_GROUP_DESC) LIKE '" + P_Commodity_Group_Desc.ToUpper().Replace("'", "''") + "%'";
                    }
                    else
                    {
                        strCondition += " AND UPPER(COMMODITY_GROUP_DESC) LIKE '%" + P_Commodity_Group_Desc.ToUpper().Replace("'", "''") + "%'";
                    }
                }
                else
                {
                    strCondition += " AND UPPER(COMMODITY_GROUP_DESC) LIKE '%" + P_Commodity_Group_Desc.ToUpper().Replace("'", "''") + "%'";
                }
            }

            //Added by soman, on 15th Nov-2005
            if (isActive == 1)
            {
                strCondition += " AND ACTIVE_FLAG = 1 ";
            }

            SQLQuery = "SELECT Count(*) from commodity_group_mst_tbl where 1=1";
            SQLQuery += strCondition;
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(SQLQuery));
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

            SQLQuery = "select * from (";
            SQLQuery += "SELECT ROWNUM SR_NO,q.* FROM ";
            SQLQuery += "(SELECT  ";
            SQLQuery = SQLQuery + " Commodity_Group_Pk,";
            SQLQuery = SQLQuery + " Active_Flag,";
            SQLQuery = SQLQuery + " Commodity_Group_Code,";
            SQLQuery = SQLQuery + " Commodity_Group_Desc,";
            SQLQuery = SQLQuery + " Version_No ";
            SQLQuery = SQLQuery + " FROM COMMODITY_GROUP_MST_TBL ";
            SQLQuery = SQLQuery + " WHERE  1 = 1 ";

            SQLQuery += strCondition;

            if (!strColumnName.Equals("SR_NO"))
            {
                SQLQuery += "order by " + strColumnName;
            }

            if (!blnSortAscending & !strColumnName.Equals("SR_NO"))
            {
                SQLQuery += " DESC";
            }

            SQLQuery = SQLQuery + " )q ) WHERE SR_NO  Between " + 1 + " and " + 30;

            try
            {
                DataSet getDs = objWF.GetDataSet(SQLQuery);
                return JsonConvert.SerializeObject(getDs, Formatting.Indented);
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

        #endregion "Fetch Function"

        #region "Save Function"

        /// <summary>
        /// Saves the specified m_ data set.
        /// </summary>
        /// <param name="M_DataSet">The m_ data set.</param>
        /// <param name="Import">if set to <c>true</c> [import].</param>
        /// <returns></returns>
        public ArrayList save(DataSet M_DataSet, bool Import = false)
        {
            //Sivachandran 17jun08 Imp-Exp-Wiz
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = default(OracleTransaction);
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
                var _with1 = insCommand;
                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".COMMODITY_GROUP_MST_TBL_PKG.COMMODITY_GROUP_MST_TBL_INS";
                var _with2 = _with1.Parameters;

                insCommand.Parameters.Add("COMMODITY_GROUP_CODE_IN", OracleDbType.Varchar2, 20, "COMMODITY_GROUP_CODE").Direction = ParameterDirection.Input;
                insCommand.Parameters["COMMODITY_GROUP_CODE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
                insCommand.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("COMMODITY_GROUP_DESC_IN", OracleDbType.Varchar2, 50, "COMMODITY_GROUP_DESC").Direction = ParameterDirection.Input;
                insCommand.Parameters["COMMODITY_GROUP_DESC_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "COMMODITY_GROUP_MST_TBL_PK").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with3 = updCommand;
                _with3.Connection = objWK.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".COMMODITY_GROUP_MST_TBL_PKG.COMMODITY_GROUP_MST_TBL_UPD";
                var _with4 = _with3.Parameters;

                updCommand.Parameters.Add("COMMODITY_GROUP_PK_IN", OracleDbType.Int32, 10, "COMMODITY_GROUP_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["COMMODITY_GROUP_PK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("COMMODITY_GROUP_CODE_IN", OracleDbType.Varchar2, 20, "COMMODITY_GROUP_CODE").Direction = ParameterDirection.Input;
                updCommand.Parameters["COMMODITY_GROUP_CODE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
                updCommand.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("COMMODITY_GROUP_DESC_IN", OracleDbType.Varchar2, 50, "COMMODITY_GROUP_DESC").Direction = ParameterDirection.Input;
                updCommand.Parameters["COMMODITY_GROUP_DESC_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with5 = objWK.MyDataAdapter;

                _with5.InsertCommand = insCommand;
                _with5.InsertCommand.Transaction = TRAN;
                _with5.UpdateCommand = updCommand;
                _with5.UpdateCommand.Transaction = TRAN;

                RecAfct = _with5.Update(M_DataSet);

                if (arrMessage.Count > 0)
                {
                    //This is if any error occurs then rollback..
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    //This part executes only when no error in the execution..
                    TRAN.Commit();
                    //Sivachandran 17Jun08 Imp-Exp-Wiz
                    if (Import == true)
                    {
                        arrMessage.Add("Data Imported Successfully");
                    }
                    else
                    {
                        arrMessage.Add("All Data Saved Successfully");
                    }
                    //End
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
            }
            finally
            {
                objWK.CloseConnection();
            }
        }

        #endregion "Save Function"

        #region "Fetch Function"

        /// <summary>
        /// Fetches the allcomodity.
        /// </summary>
        /// <returns></returns>
        public string FetchAllcomodity()
        {
            WorkFlow objWF = new WorkFlow();
            string Sql = null;
            //Gopi  ****************************** For "<ALL>" ****************
            Sql += " SELECT COMMODITY_GROUP_PK,COMMODITY_GROUP_CODE,COMMODITY_GROUP_DESC,VERSION_NO";
            Sql += " FROM (select 0 COMMODITY_GROUP_PK,";
            Sql += " '<ALL>' COMMODITY_GROUP_CODE, ";
            Sql += " ' ' COMMODITY_GROUP_DESC, ";
            Sql += " 0 VERSION_NO,0 PREFERENCE from dual UNION ";
            Sql += " SELECT CG.COMMODITY_GROUP_PK,CG.COMMODITY_GROUP_CODE, ";
            Sql += " CG.COMMODITY_GROUP_DESC,CG.VERSION_NO,CG.PREFERENCE ";
            Sql += " FROM COMMODITY_GROUP_MST_TBL CG ";
            Sql += " WHERE CG.ACTIVE_FLAG=1) ";
            Sql += " ORDER BY PREFERENCE ";
            try
            {

                DataSet getDs = objWF.GetDataSet(Sql);
                return JsonConvert.SerializeObject(getDs, Formatting.Indented);
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
    }
}