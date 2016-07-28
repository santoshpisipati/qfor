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
    public class cls_Customer_Category : CommonFeatures
    {
        #region "List of Members of the Class"

        /// <summary>
        /// The m_ customer_ category_ MST_ pk
        /// </summary>
        private Int64 M_Customer_Category_Mst_Pk;

        /// <summary>
        /// The m_ customer_ category_ identifier
        /// </summary>
        private string M_Customer_Category_Id;

        /// <summary>
        /// The m_ customer_ category_ desc
        /// </summary>
        private string M_Customer_Category_Desc;

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
        /// Gets or sets the customer_ category_ pk.
        /// </summary>
        /// <value>
        /// The customer_ category_ pk.
        /// </value>
        public Int64 Customer_Category_PK
        {
            get { return M_Customer_Category_Mst_Pk; }
            set { M_Customer_Category_Mst_Pk = value; }
        }

        /// <summary>
        /// Gets or sets the customer_ category_ identifier.
        /// </summary>
        /// <value>
        /// The customer_ category_ identifier.
        /// </value>
        public string Customer_Category_Id
        {
            get { return M_Customer_Category_Id; }
            set { M_Customer_Category_Id = value; }
        }

        /// <summary>
        /// Gets or sets the name of the customer_ category_.
        /// </summary>
        /// <value>
        /// The name of the customer_ category_.
        /// </value>
        public string Customer_Category_Name
        {
            get { return M_Customer_Category_Desc; }
            set { M_Customer_Category_Desc = value; }
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

        #region " Fetch All Function "

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="CustomerCategoryID">The customer category identifier.</param>
        /// <param name="CustomerCategoryDesc">The customer category desc.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="SortColumn">The sort column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="ActiveFlag">if set to <c>true</c> [active flag].</param>
        /// <param name="SortType">Type of the sort.</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public DataSet FetchAll(string CustomerCategoryID = "", string CustomerCategoryDesc = "", string SearchType = "", string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, bool ActiveFlag = true, string SortType = " ASC ", Int32 flag = 0)
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
            if (CustomerCategoryID.Trim().Length > 0)
            {
                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition += " AND UPPER(CUSTOMER_CATEGORY_ID) LIKE '" + CustomerCategoryID.ToUpper().Replace("'", "''") + "%'";
                    }
                    else
                    {
                        strCondition += " AND UPPER(CUSTOMER_CATEGORY_ID) LIKE '%" + CustomerCategoryID.ToUpper().Replace("'", "''") + "%'";
                    }
                }
                else
                {
                    strCondition += " AND UPPER(CUSTOMER_CATEGORY_ID) LIKE '%" + CustomerCategoryID.ToUpper().Replace("'", "''") + "%'";
                }
            }

            if (CustomerCategoryDesc.Trim().Length > 0)
            {
                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition += " AND UPPER(CUSTOMER_CATEGORY_DESC) LIKE '" + CustomerCategoryDesc.ToUpper().Replace("'", "''") + "%'";
                    }
                    else
                    {
                        strCondition += " AND UPPER(CUSTOMER_CATEGORY_DESC) LIKE '%" + CustomerCategoryDesc.ToUpper().Replace("'", "''") + "%'";
                    }
                }
                else
                {
                    strCondition += " AND UPPER(CUSTOMER_CATEGORY_DESC) LIKE '%" + CustomerCategoryDesc.ToUpper().Replace("'", "''") + "%'";
                }
            }

            if (ActiveFlag == true)
            {
                strCondition += " AND ACTIVE_FLAG = 1 ";
            }
            else
            {
                strCondition += " ";
            }

            strSQL = "SELECT Count(*) from CUSTOMER_CATEGORY_MST_TBL where 1=1 ";
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
            strSQL += "CUSTOMER_CATEGORY_MST_PK, ";
            strSQL += "NVL(ACTIVE_FLAG,0) ACTIVE_FLAG , ";
            strSQL += "CUSTOMER_CATEGORY_ID, ";
            strSQL += "CUSTOMER_CATEGORY_DESC, ";
            strSQL += "VERSION_NO  ";
            strSQL += "FROM CUSTOMER_CATEGORY_MST_TBL ";
            strSQL += "WHERE 1=1 ";
            strSQL += strCondition;
            strSQL += " order by " + SortColumn + SortType + " ) q  ) ";
            strSQL += " WHERE SR_NO  Between " + start + " and " + last;
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

        #endregion " Fetch All Function "

        #region "Save Function"

        /// <summary>
        /// Saves the specified m_ data set.
        /// </summary>
        /// <param name="M_DataSet">The m_ data set.</param>
        /// <param name="Import">if set to <c>true</c> [import].</param>
        /// <returns></returns>
        public ArrayList Save(DataSet M_DataSet, bool Import = false)
        {
            //Sivachandran 05Jun08 Imp-Exp-Wiz16May08
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
                string UserName = objWK.MyUserName;
                INS_Proc = UserName + ".CUSTOMER_CATEGORY_MST_TBL_PKG.CUSTOMER_CATEGORY_MST_TBL_INS";
                DEL_Proc = UserName + ".CUSTOMER_CATEGORY_MST_TBL_PKG.CUSTOMER_CATEGORY_MST_TBL_DEL";
                UPD_Proc = UserName + ".CUSTOMER_CATEGORY_MST_TBL_PKG.CUSTOMER_CATEGORY_MST_TBL_UPD";

                var _with1 = insCommand;
                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = INS_Proc;

                _with1.Parameters.Add("CUSTOMER_CATEGORY_ID_IN", OracleDbType.Varchar2, 20, "CUSTOMER_CATEGORY_ID").Direction = ParameterDirection.Input;
                _with1.Parameters["CUSTOMER_CATEGORY_ID_IN"].SourceVersion = DataRowVersion.Current;

                _with1.Parameters.Add("CUSTOMER_CATEGORY_DESC_IN", OracleDbType.Varchar2, 50, "CUSTOMER_CATEGORY_DESC").Direction = ParameterDirection.Input;
                _with1.Parameters["CUSTOMER_CATEGORY_DESC_IN"].SourceVersion = DataRowVersion.Current;

                _with1.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
                _with1.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;

                _with1.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;

                _with1.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

                _with1.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "Place_PK").Direction = ParameterDirection.Output;
                _with1.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with2 = delCommand;
                _with2.Connection = objWK.MyConnection;
                _with2.CommandType = CommandType.StoredProcedure;
                _with2.CommandText = DEL_Proc;

                _with2.Parameters.Add("CUSTOMER_CATEGORY_MST_PK_IN", OracleDbType.Int32, 10, "CUSTOMER_CATEGORY_MST_PK").Direction = ParameterDirection.Input;
                _with2.Parameters["CUSTOMER_CATEGORY_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

                _with2.Parameters.Add("DELETED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

                _with2.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                _with2.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                _with2.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                _with2.Parameters["CONFIG_PK_IN"].SourceVersion = DataRowVersion.Current;

                _with2.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with2.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with3 = updCommand;
                _with3.Connection = objWK.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = UPD_Proc;

                _with3.Parameters.Add("CUSTOMER_CATEGORY_MST_PK_IN", OracleDbType.Int32, 10, "CUSTOMER_CATEGORY_MST_PK").Direction = ParameterDirection.Input;
                _with3.Parameters["CUSTOMER_CATEGORY_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("CUSTOMER_CATEGORY_ID_IN", OracleDbType.Varchar2, 20, "CUSTOMER_CATEGORY_ID").Direction = ParameterDirection.Input;
                _with3.Parameters["CUSTOMER_CATEGORY_ID_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("CUSTOMER_CATEGORY_DESC_IN", OracleDbType.Varchar2, 50, "CUSTOMER_CATEGORY_DESC").Direction = ParameterDirection.Input;
                _with3.Parameters["CUSTOMER_CATEGORY_DESC_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
                _with3.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

                _with3.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                _with3.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                _with3.Parameters["CONFIG_PK_IN"].SourceVersion = DataRowVersion.Current;

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
                TRAN.Commit();
                if (arrMessage.Count > 0)
                {
                    return arrMessage;
                }
                else if (Import == false)
                {
                    arrMessage.Add("All Data Saved Successfully");
                    return arrMessage;
                }
                else
                {
                    arrMessage.Add("Data Imported Successfully");
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