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
    public class clsDEPARTMENT_MST_TBL : CommonFeatures
    {
        #region "List of Members of the Class"

        /// <summary>
        /// The m_ department_ MST_ pk
        /// </summary>
        private Int16 M_Department_Mst_Pk;

        /// <summary>
        /// The m_ department_ identifier
        /// </summary>
        private string M_Department_Id;

        /// <summary>
        /// The m_ department_ name
        /// </summary>
        private string M_Department_Name;

        #endregion "List of Members of the Class"

        #region "List of Properties"

        /// <summary>
        /// Gets or sets the department_ MST_ pk.
        /// </summary>
        /// <value>
        /// The department_ MST_ pk.
        /// </value>
        public Int16 Department_Mst_Pk
        {
            get { return M_Department_Mst_Pk; }
            set { M_Department_Mst_Pk = value; }
        }

        /// <summary>
        /// Gets or sets the department_ identifier.
        /// </summary>
        /// <value>
        /// The department_ identifier.
        /// </value>
        public string Department_Id
        {
            get { return M_Department_Id; }
            set { M_Department_Id = value; }
        }

        /// <summary>
        /// Gets or sets the name of the department_.
        /// </summary>
        /// <value>
        /// The name of the department_.
        /// </value>
        public string Department_Name
        {
            get { return M_Department_Name; }
            set { M_Department_Name = value; }
        }

        #endregion "List of Properties"

        #region "Fetch All"

        /// <summary>
        /// Fetches the department.
        /// </summary>
        /// <param name="DesigTrnPK">The desig TRN pk.</param>
        /// <returns></returns>
        public DataSet FetchDepartment(long DesigTrnPK = 0)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();

            strSQL = " SELECT";
            strSQL = strSQL + " ' '  Department_ID,";
            strSQL = strSQL + " ' ' Department_NAME,";
            strSQL = strSQL + " 0 Department_MST_PK ";
            strSQL = strSQL + " from ";
            strSQL = strSQL + " DUAL ";
            strSQL = strSQL + " UNION";
            strSQL = strSQL + " SELECT";
            strSQL += " Department_ID, ";
            strSQL += " Department_NAME,";
            strSQL += " Department_MST_PK ";
            strSQL += " FROM Department_MST_TBL ";
            strSQL += " WHERE 1=1 ";
            if (DesigTrnPK > 0)
            {
                strSQL += "AND Department_MST_PK=" + DesigTrnPK;
            }
            strSQL += "AND ACTIVE_FLAG=1 ";

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
        /// <param name="DepartmentPK">The department pk.</param>
        /// <param name="DepartmentID">The department identifier.</param>
        /// <param name="DepartmentName">Name of the department.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="strColumnName">Name of the string column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="SortCol">The sort col.</param>
        /// <param name="IsActive">The is active.</param>
        /// <param name="blnSortAscending">if set to <c>true</c> [BLN sort ascending].</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public DataSet FetchAll(Int16 DepartmentPK, string DepartmentID, string DepartmentName, string SearchType, string strColumnName, ref Int32 CurrentPage, ref Int32 TotalPage, Int16 SortCol, Int16 IsActive, bool blnSortAscending,
        Int32 flag)
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
            if (DepartmentPK > 0)
            {
                strCondition = strCondition + " AND Department_MST_PK=" + DepartmentPK;
            }
            if (DepartmentID.Trim().Length > 0)
            {
                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition += " AND UPPER(Department_ID) LIKE '" + DepartmentID.ToUpper().Replace("'", "''") + "%'";
                    }
                    else
                    {
                        strCondition += " AND UPPER(Department_ID) LIKE '%" + DepartmentID.ToUpper().Replace("'", "''") + "%'";
                    }
                }
                else
                {
                    strCondition += " AND UPPER(Department_ID) LIKE '%" + DepartmentID.ToUpper().Replace("'", "''") + "%'";
                }
            }
            if (DepartmentName.Trim().Length > 0)
            {
                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition += " AND UPPER(Department_NAME) LIKE '" + DepartmentName.ToUpper().Replace("'", "''") + "%'";
                    }
                    else
                    {
                        strCondition += " AND UPPER(Department_NAME) LIKE '%" + DepartmentName.ToUpper().Replace("'", "''") + "%'";
                    }
                }
                else
                {
                    strCondition += " AND UPPER(Department_NAME) LIKE '%" + DepartmentName.ToUpper().Replace("'", "''") + "%'";
                }
            }
            if (IsActive == 1)
            {
                strCondition += " AND ACTIVE_FLAG=1";
            }

            strSQL = "SELECT Count(*) from Department_MST_TBL where 1=1";
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
            strSQL += "SELECT  ROWNUM SR_NO,q.* FROM ";
            strSQL += "(SELECT ";
            strSQL += "Department_MST_PK,ACTIVE_FLAG, ";
            strSQL += "Department_ID, ";
            strSQL += "Department_NAME, ";
            strSQL += "Version_No  ";
            strSQL += "FROM Department_MST_TBL ";
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

            strSQL = strSQL + " )q)  WHERE sr_no  Between " + start + " and " + last;

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

        #region "Fetch Vendor Type"

        /// <summary>
        /// Fetches the type of the vendor.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchVendorType()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();

            strSQL = " SELECT ROWNUM SL,";
            strSQL = strSQL + " V.VENDOR_TYPE_PK,";
            strSQL = strSQL + " V.VENDOR_TYPE_ID";
            strSQL = strSQL + " V.VENDOR_TYPE";
            strSQL = strSQL + " FROM";
            strSQL = strSQL + " VENDOR_TYPE_MST_TBL V";
            strSQL = strSQL + " ORDER BY V.VENDOR_TYPE_ID";

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

        #endregion "Fetch Vendor Type"

        #region "Insert Function"

        /// <summary>
        /// Inserts this instance.
        /// </summary>
        /// <returns></returns>
        public int Insert()
        {
            WorkFlow objWS = new WorkFlow();
            Int32 intPkVal = default(Int32);
            try
            {
                objWS.MyCommand.CommandType = CommandType.StoredProcedure;
                var _with1 = objWS.MyCommand.Parameters;
                //.Add("DEPARTMENT_Mst_Pk_IN", M_Department_Mst_Pk).Direction = ParameterDirection.Input
                _with1.Add("DEPARTMENT_ID_IN", M_Department_Id).Direction = ParameterDirection.Input;
                _with1.Add("DEPARTMENT_NAME_IN", M_Department_Name).Direction = ParameterDirection.Input;
                _with1.Add("Created_By_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", intPkVal).Direction = ParameterDirection.Output;
                objWS.MyCommand.CommandText = "FEEDERUSER.DEPARTMENT_MST_TBL_PKG.DEPARTMENT_MST_TBL_Ins";
                if (objWS.ExecuteCommands() == true)
                {
                    return intPkVal;
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

        #endregion "Insert Function"

        #region "Update Function"

        /// <summary>
        /// Updates this instance.
        /// </summary>
        /// <returns></returns>
        public int Update()
        {
            WorkFlow objWS = new WorkFlow();
            try
            {
                objWS.MyCommand.CommandType = CommandType.StoredProcedure;
                var _with2 = objWS.MyCommand.Parameters;
                _with2.Add("Department_Mst_Pk_IN", M_Department_Mst_Pk).Direction = ParameterDirection.Input;
                _with2.Add("Department_Id_IN", M_Department_Id).Direction = ParameterDirection.Input;
                _with2.Add("Department_Name_IN", M_Department_Name).Direction = ParameterDirection.Input;
                _with2.Add("Last_Modified_By_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                _with2.Add("VERSION_NO_IN", M_VERSION_NO).Direction = ParameterDirection.Input;
                _with2.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;
                objWS.MyCommand.CommandText = "FEEDERUSER.DEPARTMENT_MST_TBL_PKG.DEPARTMENT_MST_TBL_UPD";
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
                _with3.Add("Department_Mst_Pk_IN", M_Department_Mst_Pk).Direction = ParameterDirection.Input;
                _with3.Add("VERSION_NO", M_VERSION_NO).Direction = ParameterDirection.Input;
                _with3.Add("RETURN_VALUE", intPkVal).Direction = ParameterDirection.Output;
                objWS.MyCommand.CommandText = "FEEDERUSER.DEPARTMENT_MST_TBL_PKG.DEPARTMENT_MST_TBL_DEL";
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

        #region "Save Function"

        /// <summary>
        /// Saves the specified m_ data set.
        /// </summary>
        /// <param name="M_DataSet">The m_ data set.</param>
        /// <returns></returns>
        public ArrayList Save(ref DataSet M_DataSet)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = default(OracleTransaction);
            TRAN = objWK.MyConnection.BeginTransaction();

            Int32 RecAfct = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            OracleCommand delCommand = new OracleCommand();

            try
            {
                var _with4 = insCommand;
                _with4.Connection = objWK.MyConnection;
                _with4.CommandType = CommandType.StoredProcedure;
                _with4.CommandText = objWK.MyUserName + ".DEPARTMENT_MST_TBL_PKG.DEPARTMENT_MST_TBL_INS";
                var _with5 = _with4.Parameters;

                insCommand.Parameters.Add("DEPARTMENT_ID_IN", OracleDbType.Varchar2, 20, "Department_ID").Direction = ParameterDirection.Input;
                insCommand.Parameters["DEPARTMENT_ID_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("DEPARTMENT_NAME_IN", OracleDbType.Varchar2, 50, "Department_NAME").Direction = ParameterDirection.Input;
                insCommand.Parameters["DEPARTMENT_NAME_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
                insCommand.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                insCommand.Parameters["CONFIG_PK_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "Department_MST_TBL_PK").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with6 = updCommand;
                _with6.Connection = objWK.MyConnection;
                _with6.CommandType = CommandType.StoredProcedure;
                _with6.CommandText = objWK.MyUserName + ".DEPARTMENT_MST_TBL_PKG.DEPARTMENT_MST_TBL_UPD";
                var _with7 = _with6.Parameters;

                updCommand.Parameters.Add("DEPARTMENT_MST_PK_IN", OracleDbType.Int32, 10, "Department_MST_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["DEPARTMENT_MST_PK_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("DEPARTMENT_ID_IN", OracleDbType.Varchar2, 20, "Department_ID").Direction = ParameterDirection.Input;
                updCommand.Parameters["DEPARTMENT_ID_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("DEPARTMENT_NAME_IN", OracleDbType.Varchar2, 50, "Department_NAME").Direction = ParameterDirection.Input;
                updCommand.Parameters["DEPARTMENT_NAME_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "Version_No").Direction = ParameterDirection.Input;
                updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
                updCommand.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                updCommand.Parameters["CONFIG_PK_IN"].SourceVersion = DataRowVersion.Current;
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

        #region "Fetch Designation"

        /// <summary>
        /// Fetches the design TRN.
        /// </summary>
        /// <param name="DesignTRNPK">The design TRNPK.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <returns></returns>
        public DataSet FetchDesignTrn(long DesignTRNPK, ref Int32 CurrentPage, ref Int32 TotalPage)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            strSQL = "SELECT Count(*) from DEPT_DESIG_TRN DD,";
            strSQL += "  DESIGNATION_MST_TBL DG";
            strSQL += "WHERE ";
            strSQL += "DD.DESIGNATION_MST_FK(+)=DG.DESIGNATION_MST_PK\t";
            strSQL += "AND  DD.DEPART_MST_FK (+)=" + DesignTRNPK;

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

            strSQL = "SELECT * FROM ";
            strSQL += "(SELECT ROWNUM SR_NO, ";
            strSQL += "DD.DEPT_DESIG_TRN_PK,";
            strSQL += "DD.DEPART_MST_FK,";
            strSQL += " DG.DESIGNATION_MST_PK,";
            strSQL += " NVL(DD.ACTIVE,0) ACTIVE,";
            strSQL += "DG.DESIGNATION_ID,";
            strSQL += " DG.DESIGNATION_NAME,";
            strSQL += "DD.VERSION_NO";
            strSQL += "FROM";
            strSQL += "DEPT_DESIG_TRN DD,DESIGNATION_MST_TBL DG";
            strSQL += "WHERE ";
            strSQL += "DD.DESIGNATION_MST_FK(+)=DG.DESIGNATION_MST_PK\t";
            strSQL += "AND DD.DEPART_MST_FK (+)=" + DesignTRNPK;

            //If ToCountryFK > 0 Then
            //    strSQL &= vbCrLf & "AND TOPORT.COUNTRY_MST_FK=" & ToCountryFK
            //End If
            //If ToPortFK > 0 Then
            //    strSQL &= vbCrLf & "AND TOPORT.PORT_MST_PK=" & ToPortFK
            //End If
            //strSQL &= vbCrLf & "order by toport.country_mst_fk"
            strSQL += " ) WHERE SR_NO  Between " + start + " and " + last;
            strSQL += " order by SR_NO ";

            DataSet objDS = null;
            try
            {
                objDS = objWF.GetDataSet(strSQL);
                if (objDS.Tables[0].Rows.Count <= 0)
                {
                    return null;
                }
                else
                {
                    return objDS;
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
        }

        #endregion "Fetch Designation"
    }
}