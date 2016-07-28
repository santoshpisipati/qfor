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
    public class clsDESIGNATION_MST_TBL : CommonFeatures
    {
        #region "List of Members of the Class"

        /// <summary>
        /// The m_ designatio n_ MST_ pk
        /// </summary>
        private Int64 M_DESIGNATION_Mst_Pk;

        /// <summary>
        /// The m_ designatio n_ identifier
        /// </summary>
        private string M_DESIGNATION_Id;

        /// <summary>
        /// The m_ designatio n_ name
        /// </summary>
        private string M_DESIGNATION_Name;

        #endregion "List of Members of the Class"

        /// <summary>
        /// The m_ created_ date
        /// </summary>
        private string M_Created_Date;

        #region "List of Properties"

        /// <summary>
        /// Gets or sets the designatio n_ MST_ pk.
        /// </summary>
        /// <value>
        /// The designatio n_ MST_ pk.
        /// </value>
        public Int64 DESIGNATION_Mst_Pk
        {
            get { return M_DESIGNATION_Mst_Pk; }
            set { M_DESIGNATION_Mst_Pk = value; }
        }

        /// <summary>
        /// Gets or sets the designatio n_ identifier.
        /// </summary>
        /// <value>
        /// The designatio n_ identifier.
        /// </value>
        public string DESIGNATION_Id
        {
            get { return M_DESIGNATION_Id; }
            set { M_DESIGNATION_Id = value; }
        }

        /// <summary>
        /// Gets or sets the name of the designatio n_.
        /// </summary>
        /// <value>
        /// The name of the designatio n_.
        /// </value>
        public string DESIGNATION_Name
        {
            get { return M_DESIGNATION_Name; }
            set { M_DESIGNATION_Name = value; }
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

        #region "Insert Function"

        /// <summary>
        /// Inserts this instance.
        /// </summary>
        /// <returns></returns>
        public int Insert()
        {
            WorkFlow objWS = new WorkFlow();
            Int64 intPkVal = default(Int64);
            try
            {
                objWS.MyCommand.CommandType = CommandType.StoredProcedure;
                var _with1 = objWS.MyCommand.Parameters;
                _with1.Add("DESIGNATION_Id_IN", M_DESIGNATION_Id).Direction = ParameterDirection.Input;
                _with1.Add("DESIGNATION_Name_IN", M_DESIGNATION_Name).Direction = ParameterDirection.Input;
                _with1.Add("Created_By_Fk_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                _with1.Add("Created_Date_IN", M_Created_Date).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", intPkVal).Direction = ParameterDirection.Output;
                objWS.MyCommand.CommandText = "DESIGNATION_MST_TBL_Ins";
                if (objWS.ExecuteCommands() == true)
                {
                    return Convert.ToInt32(intPkVal);
                }
                else
                {
                    return -1;
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
        }

        #endregion "Insert Function"

        #region "Update Function"

        /// <summary>
        /// Updates this instance.
        /// </summary>
        /// <returns></returns>
        public int Update()
        {
            WorkFlow objWS = new WorkFlow();
            Int32 intPkVal = default(Int32);
            try
            {
                objWS.MyCommand.CommandType = CommandType.StoredProcedure;
                var _with2 = objWS.MyCommand.Parameters;
                _with2.Add("DESIGNATION_Mst_Pk_IN", M_DESIGNATION_Mst_Pk).Direction = ParameterDirection.Input;
                _with2.Add("DESIGNATION_Id_IN", M_DESIGNATION_Id).Direction = ParameterDirection.Input;
                _with2.Add("DESIGNATION_Name_IN", M_DESIGNATION_Name).Direction = ParameterDirection.Input;
                _with2.Add("Created_Date_IN", M_Created_Date).Direction = ParameterDirection.Input;
                _with2.Add("Last_Modified_By_Fk_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                _with2.Add("Version_No_IN", M_VERSION_NO).Direction = ParameterDirection.Input;
                objWS.MyCommand.CommandText = "DESIGNATION_MST_TBL_UPD";
                if (objWS.ExecuteCommands() == true)
                {
                    return 1;
                }
                else
                {
                    return -1;
                }
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

        #endregion "Update Function"

        #region "Delete Function"

        /// <summary>
        /// Deletes this instance.
        /// </summary>
        /// <returns></returns>
        public int Delete()
        {
            WorkFlow objWS = new WorkFlow();
            Int32 intPkVal = default(Int32);
            try
            {
                objWS.MyCommand.CommandType = CommandType.StoredProcedure;
                var _with3 = objWS.MyCommand.Parameters;
                _with3.Add("DESIGNATION_Mst_Pk_IN", M_DESIGNATION_Mst_Pk).Direction = ParameterDirection.Input;
                _with3.Add("Version_No_IN", M_VERSION_NO).Direction = ParameterDirection.Input;
                objWS.MyCommand.CommandText = "DESIGNATION_MST_TBL_DEL";
                if (objWS.ExecuteCommands() == true)
                {
                    return 1;
                }
                else
                {
                    return -1;
                }
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

        #endregion "Delete Function"

        #region "Fetch All"

        /// <summary>
        /// Fetches the desig.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchDesig()
        {
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            strSQL = " SELECT  ";
            strSQL += "0 DESIGNATION_MST_PK, ";
            strSQL += "' ' DESIGNATION_ID, ";
            strSQL += "' ' DESIGNATION_NAME ";
            strSQL += "FROM dual ";
            strSQL += " union ";
            strSQL += " SELECT  ";
            strSQL += "DESIGNATION_MST_PK, ";
            strSQL += "DESIGNATION_ID, ";
            strSQL += "DESIGNATION_NAME ";
            strSQL += "FROM DESIGNATION_MST_TBL ";
            strSQL += "Order by DESIGNATION_ID";

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

        /// <summary>
        /// Fetches the desig on department.
        /// </summary>
        /// <param name="DepartmentFK">The department fk.</param>
        /// <returns></returns>
        public DataSet FetchDesigOnDepartment(int DepartmentFK = 0)
        {
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            strSQL = " SELECT  ";
            strSQL += "' ' DESIGNATION_ID, ";
            strSQL += "0 DEPART_MST_FK ";
            strSQL += "FROM dual ";
            strSQL += " union ";
            strSQL += "SELECT DISTINCT DMT.DESIGNATION_ID,DDT.DEPART_MST_FK FROM ";
            strSQL += "DEPT_DESIG_TRN DDT,";
            strSQL += "DESIGNATION_MST_TBL DMT,";
            strSQL += "DEPARTMENT_MST_TBL DEMT";
            strSQL += "WHERE";
            strSQL += "DDT.DESIGNATION_MST_FK = DMT.DESIGNATION_MST_PK";
            strSQL += "AND DDT.DEPART_MST_FK=DEMT.DEPARTMENT_MST_PK";
            strSQL += "AND ACTIVE=1";

            if (DepartmentFK > 0)
            {
                strSQL += "AND DDT.DEPART_MST_FK=" + DepartmentFK;
            }

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

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="DESIGNATIONPK">The designationpk.</param>
        /// <param name="DESIGNATIONID">The designationid.</param>
        /// <param name="DESIGNATIONName">Name of the designation.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="strColumnName">Name of the string column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="SortCol">The sort col.</param>
        /// <param name="IsActive">The is active.</param>
        /// <param name="blnSortAscending">if set to <c>true</c> [BLN sort ascending].</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public DataSet FetchAll(Int16 DESIGNATIONPK = 0, string DESIGNATIONID = "", string DESIGNATIONName = "", string SearchType = "", string strColumnName = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int16 SortCol = 0, Int16 IsActive = 0, bool blnSortAscending = false,
        Int32 flag = 0)
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
            if (DESIGNATIONPK > 0)
            {
                strCondition += " AND DESIGNATION_MST_PK=" + DESIGNATIONPK;
            }
            if (DESIGNATIONID.Trim().Length > 0)
            {
                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition += " AND UPPER(DESIGNATION_ID) LIKE '" + DESIGNATIONID.ToUpper().ToUpper().Replace("'", "''") + "%'";
                    }
                    else
                    {
                        strCondition += " AND UPPER(DESIGNATION_ID) LIKE '%" + DESIGNATIONID.ToUpper().Replace("'", "''") + "%'";
                    }
                }
                else
                {
                    strCondition += " AND UPPER(DESIGNATION_ID) LIKE '%" + DESIGNATIONID.ToUpper().Replace("'", "''") + "%'";
                }
            }
            if (DESIGNATIONName.Trim().Length > 0)
            {
                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition += " AND UPPER(DESIGNATION_NAME) LIKE '" + DESIGNATIONName.ToUpper().Replace("'", "''") + "%'";
                    }
                    else
                    {
                        strCondition += " AND UPPER(DESIGNATION_NAME) LIKE '%" + DESIGNATIONName.ToUpper().Replace("'", "''") + "%'";
                    }
                }
                else
                {
                    strCondition += " AND UPPER(DESIGNATION_NAME) LIKE '%" + DESIGNATIONName.ToUpper().Replace("'", "''") + "%'";
                }
            }
            if (IsActive == 1)
            {
                strCondition += " AND ACTIVE_FLAG=1";
            }
            strSQL = "SELECT Count(*) from DESIGNATION_MST_TBL where 1=1";
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
            strSQL += "SELECT ROWNUM SR_NO, q.* FROM ";
            strSQL += "(SELECT ";
            strSQL += "DESIGNATION_MST_PK,ACTIVE_FLAG, ";
            strSQL += "DESIGNATION_ID, ";
            strSQL += "DESIGNATION_NAME, ";
            strSQL += "VERSION_No  ";
            strSQL += "FROM DESIGNATION_MST_TBL ";
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

            strSQL = strSQL + " ) q ) WHERE SR_NO  Between " + start + " and " + last;

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

        #region "Save Function"

        /// <summary>
        /// Saves the specified m_ data set.
        /// </summary>
        /// <param name="M_DataSet">The m_ data set.</param>
        /// <returns></returns>
        public ArrayList Save(DataSet M_DataSet)
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
                var _with4 = insCommand;
                _with4.Connection = objWK.MyConnection;
                _with4.CommandType = CommandType.StoredProcedure;
                _with4.CommandText = objWK.MyUserName + ".DESIGNATION_MST_TBL_PKG.DESIGNATION_MST_TBL_INS";
                var _with5 = _with4.Parameters;

                insCommand.Parameters.Add("DESIGNATION_ID_IN", OracleDbType.Varchar2, 20, "DESIGNATION_ID").Direction = ParameterDirection.Input;
                insCommand.Parameters["DESIGNATION_ID_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("DESIGNATION_NAME_IN", OracleDbType.Varchar2, 50, "DESIGNATION_NAME").Direction = ParameterDirection.Input;
                insCommand.Parameters["DESIGNATION_NAME_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
                insCommand.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "DESIGNATION_MST_TBL_PK").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with6 = updCommand;
                _with6.Connection = objWK.MyConnection;
                _with6.CommandType = CommandType.StoredProcedure;
                _with6.CommandText = objWK.MyUserName + ".DESIGNATION_MST_TBL_PKG.DESIGNATION_MST_TBL_UPD";
                var _with7 = _with6.Parameters;

                updCommand.Parameters.Add("DESIGNATION_MST_PK_IN", OracleDbType.Int32, 10, "DESIGNATION_MST_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["DESIGNATION_MST_PK_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("DESIGNATION_ID_IN", OracleDbType.Varchar2, 20, "DESIGNATION_ID").Direction = ParameterDirection.Input;
                updCommand.Parameters["DESIGNATION_ID_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("DESIGNATION_NAME_IN", OracleDbType.Varchar2, 50, "DESIGNATION_NAME").Direction = ParameterDirection.Input;
                updCommand.Parameters["DESIGNATION_NAME_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
                updCommand.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with8 = objWK.MyDataAdapter;
                _with8.InsertCommand = insCommand;
                _with8.InsertCommand.Transaction = TRAN;
                _with8.UpdateCommand = updCommand;
                _with8.UpdateCommand.Transaction = TRAN;
                RecAfct = _with8.Update(M_DataSet);

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
                    arrMessage.Add("All Data Saved Successfully");
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
    }
}