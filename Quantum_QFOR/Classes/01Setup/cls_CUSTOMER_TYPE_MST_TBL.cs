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
    public class clsCUSTOMER_TYPE_MST_TBL : CommonFeatures
    {
        #region "List of Members of the Class"

        /// <summary>
        /// The m_ customer_ type_ MST_ pk
        /// </summary>
        private Int64 M_Customer_Type_Mst_Pk;

        /// <summary>
        /// The m_ customer_ type_ identifier
        /// </summary>
        private string M_Customer_Type_Id;

        /// <summary>
        /// The m_ customer_ type_ name
        /// </summary>
        private string M_Customer_Type_Name;

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
        /// Gets or sets the customer_ type_ pk.
        /// </summary>
        /// <value>
        /// The customer_ type_ pk.
        /// </value>
        public Int64 Customer_Type_PK
        {
            get { return M_Customer_Type_Mst_Pk; }
            set { M_Customer_Type_Mst_Pk = value; }
        }

        /// <summary>
        /// Gets or sets the customer_ type_ identifier.
        /// </summary>
        /// <value>
        /// The customer_ type_ identifier.
        /// </value>
        public string Customer_Type_Id
        {
            get { return M_Customer_Type_Id; }
            set { M_Customer_Type_Id = value; }
        }

        /// <summary>
        /// Gets or sets the name of the customer_ type_.
        /// </summary>
        /// <value>
        /// The name of the customer_ type_.
        /// </value>
        public string Customer_Type_Name
        {
            get { return M_Customer_Type_Name; }
            set { M_Customer_Type_Name = value; }
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
        /// <param name="CustomerTypeID">The customer type identifier.</param>
        /// <param name="CustomerTypeName">Name of the customer type.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="SortColumn">The sort column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="ActiveFlag">if set to <c>true</c> [active flag].</param>
        /// <param name="SortType">Type of the sort.</param>
        /// <returns></returns>
        public DataSet FetchAll(string CustomerTypeID = "", string CustomerTypeName = "", string SearchType = "", string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, bool ActiveFlag = true, string SortType = " ASC ")
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            if (CustomerTypeID.Trim().Length > 0)
            {
                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition += " AND UPPER(CUSTOMER_TYPE_ID) LIKE '" + CustomerTypeID.ToUpper().Replace("'", "''") + "%'";
                    }
                    else
                    {
                        strCondition += " AND UPPER(CUSTOMER_TYPE_ID) LIKE '%" + CustomerTypeID.ToUpper().Replace("'", "''") + "%'";
                    }
                }
                else
                {
                    strCondition += " AND UPPER(CUSTOMER_TYPE_ID) LIKE '%" + CustomerTypeID.ToUpper().Replace("'", "''") + "%'";
                }
            }

            if (CustomerTypeName.Trim().Length > 0)
            {
                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition += " AND UPPER(CUSTOMER_TYPE_NAME) LIKE '" + CustomerTypeName.ToUpper().Replace("'", "''") + "%'";
                    }
                    else
                    {
                        strCondition += " AND UPPER(CUSTOMER_TYPE_NAME) LIKE '%" + CustomerTypeName.ToUpper().Replace("'", "''") + "%'";
                    }
                }
                else
                {
                    strCondition += " AND UPPER(CUSTOMER_TYPE_NAME) LIKE '%" + CustomerTypeName.ToUpper().Replace("'", "''") + "%'";
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

            strSQL = "SELECT Count(*) from CUSTOMER_TYPE_MST_TBL where 1=1 ";
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
            strSQL += "CUSTOMER_TYPE_PK, ";
            strSQL += "NVL(ACTIVE_FLAG,0) ACTIVE_FLAG , ";
            strSQL += "CUSTOMER_TYPE_ID, ";
            strSQL += "CUSTOMER_TYPE_NAME, ";
            strSQL += "VERSION_NO  ";
            strSQL += "FROM CUSTOMER_TYPE_MST_TBL ";
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

        #endregion " Fetch All Function "

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
                DataTable DtTbl = new DataTable();
                DataRow DtRw = null;
                int i = 0;
                DtTbl = M_DataSet.Tables[0];
                string INS_Proc = null;
                string DEL_Proc = null;
                string UPD_Proc = null;
                string UserName = objWK.MyUserName;
                INS_Proc = UserName + ".CUSTOMER_TYPE_MST_TBL_PKG.CUSTOMER_TYPE_MST_TBL_INS";
                DEL_Proc = UserName + ".CUSTOMER_TYPE_MST_TBL_PKG.CUSTOMER_TYPE_MST_TBL_DEL";
                UPD_Proc = UserName + ".CUSTOMER_TYPE_MST_TBL_PKG.CUSTOMER_TYPE_MST_TBL_UPD";

                var _with1 = insCommand;
                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = INS_Proc;

                _with1.Parameters.Add("CUSTOMER_TYPE_ID_IN", OracleDbType.Varchar2, 20, "CUSTOMER_TYPE_ID").Direction = ParameterDirection.Input;
                _with1.Parameters["CUSTOMER_TYPE_ID_IN"].SourceVersion = DataRowVersion.Current;

                _with1.Parameters.Add("CUSTOMER_TYPE_NAME_IN", OracleDbType.Varchar2, 50, "CUSTOMER_TYPE_NAME").Direction = ParameterDirection.Input;
                _with1.Parameters["CUSTOMER_TYPE_NAME_IN"].SourceVersion = DataRowVersion.Current;

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

                _with2.Parameters.Add("CUSTOMER_TYPE_PK_IN", OracleDbType.Int32, 10, "CUSTOMER_TYPE_PK").Direction = ParameterDirection.Input;

                _with2.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                _with2.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                _with2.Parameters["CUSTOMER_TYPE_PK_IN"].SourceVersion = DataRowVersion.Current;

                _with2.Parameters.Add("DELETED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

                _with2.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                _with2.Parameters["CONFIG_PK_IN"].SourceVersion = DataRowVersion.Current;

                _with2.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with2.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with3 = updCommand;
                _with3.Connection = objWK.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = UPD_Proc;

                _with3.Parameters.Add("CUSTOMER_TYPE_PK_IN", OracleDbType.Int32, 10, "CUSTOMER_TYPE_PK").Direction = ParameterDirection.Input;
                _with3.Parameters["CUSTOMER_TYPE_PK_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("CUSTOMER_TYPE_ID_IN", OracleDbType.Varchar2, 20, "CUSTOMER_TYPE_ID").Direction = ParameterDirection.Input;
                _with3.Parameters["CUSTOMER_TYPE_ID_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("CUSTOMER_TYPE_NAME_IN", OracleDbType.Varchar2, 50, "CUSTOMER_TYPE_NAME").Direction = ParameterDirection.Input;
                _with3.Parameters["CUSTOMER_TYPE_NAME_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
                _with3.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                _with3.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

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
                else
                {
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

        #region "Constructor"

        /// <summary>
        /// Initializes a new instance of the <see cref="clsCUSTOMER_TYPE_MST_TBL"/> class.
        /// </summary>
        public clsCUSTOMER_TYPE_MST_TBL()
        {
            string Sql = null;
            Sql = "SELECT 0 CUSTOMER_CATEGORY_MST_PK,' Select' CUSTOMER_CATEGORY_ID,'' CUSTOMER_CATEGORY_DESC,0 VERSION_NO";
            Sql += "FROM CUSTOMER_CATEGORY_MST_TBL CG";
            Sql += "UNION";
            Sql += "SELECT CG.CUSTOMER_CATEGORY_MST_PK,CG.CUSTOMER_CATEGORY_ID,";
            Sql += "CG.CUSTOMER_CATEGORY_DESC,CG.VERSION_NO";
            Sql += "FROM CUSTOMER_CATEGORY_MST_TBL CG";
            Sql += "WHERE CG.ACTIVE_FLAG=1";
            Sql += "ORDER BY CUSTOMER_CATEGORY_ID";
            try
            {
                M_DataSet = (new WorkFlow()).GetDataSet(Sql);
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

        #endregion "Constructor"

        #region "Fetch Category for Customer"

        /// <summary>
        /// Gets the role for customer.
        /// </summary>
        /// <param name="nCustPK">The n customer pk.</param>
        /// <returns></returns>
        public DataSet GetRoleForCustomer(long nCustPK)
        {
            string Sql = null;
            DataSet DS = null;
            Sql = "SELECT 0 CUSTOMER_CATEGORY_MST_PK,' Select' CUSTOMER_CATEGORY_ID,'' CUSTOMER_CATEGORY_DESC,0 VERSION_NO";
            Sql += "FROM CUSTOMER_CATEGORY_MST_TBL CG";
            Sql += "UNION";
            Sql += "SELECT CG.CUSTOMER_CATEGORY_MST_PK,CG.CUSTOMER_CATEGORY_ID,";
            Sql += "CG.CUSTOMER_CATEGORY_DESC,CG.VERSION_NO";
            Sql += "FROM CUSTOMER_CATEGORY_MST_TBL CG,CUSTOMER_CATEGORY_TRN CGT";
            Sql += "WHERE";
            Sql += "CG.CUSTOMER_CATEGORY_MST_PK=CGT.CUSTOMER_CATEGORY_MST_FK";
            Sql += "AND CGT.CUSTOMER_MST_FK = " + Convert.ToString(nCustPK);
            Sql += "AND CG.ACTIVE_FLAG = 1";
            Sql += "ORDER BY CUSTOMER_CATEGORY_ID";
            try
            {
                DS = (new WorkFlow()).GetDataSet(Sql);
                return DS;
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

        #endregion "Fetch Category for Customer"

        #region "Fetch Category for Customer"

        /// <summary>
        /// Gets the customer categ.
        /// </summary>
        /// <returns></returns>
        public DataSet GetCustCateg()
        {
            string Sql = null;
            DataSet DS = null;
            Sql = "SELECT 0 CUSTOMER_CATEGORY_MST_PK,' Shipper' CUSTOMER_CATEGORY_ID FROM DUAL";
            Sql += " UNION SELECT 1 CUSTOMER_CATEGORY_MST_PK,' Consignee' CUSTOMER_CATEGORY_ID FROM DUAL";
            try
            {
                DS = (new WorkFlow()).GetDataSet(Sql);
                return DS;
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

        #endregion "Fetch Category for Customer"
    }
}

#region " Previous Code "

//#Region "Insert Function"
//Public Function Insert() As Int64
//    Dim objWS As New WorkFlow
//    Dim intPkVal As Int64
//    objWS.MyCommand.CommandType = CommandType.StoredProcedure
//    With objWS.MyCommand.Parameters
//        .Add("Customer_Type_Id_IN", M_Customer_Type_Id).Direction = ParameterDirection.Input
//        .Add("Customer_Type_Name_IN", M_Customer_Type_Name).Direction = ParameterDirection.Input
//        .Add("Created_By_Fk_IN", M_Created_By_Fk).Direction = ParameterDirection.Input
//        .Add("Created_Date_IN", M_Created_Date).Direction = ParameterDirection.Input
//        .Add("RETURN_VALUE", intPkVal).Direction = ParameterDirection.Output
//    End With
//    objWS.MyCommand.CommandText = "Customer_Type_TYPE_MST_TBL_Ins"
//    If objWS.ExecuteCommands() = True Then
//        Return intPkVal
//    Else
//        Return -1
//    End If
//End Function
//#End Region

//#Region "Update Function"
//Public Function Update() As Integer
//    Dim objWS As New WorkFlow
//    Dim intPkVal As Int32
//    objWS.MyCommand.CommandType = CommandType.StoredProcedure
//    With objWS.MyCommand.Parameters
//        .Add("Customer_Type_Mst_Pk_IN", M_Customer_Type_Mst_Pk).Direction = ParameterDirection.Input
//        .Add("Customer_Type_Id_IN", M_Customer_Type_Id).Direction = ParameterDirection.Input
//        .Add("Customer_Type_Name_IN", M_Customer_Type_Name).Direction = ParameterDirection.Input
//        .Add("Created_Date_IN", M_Created_Date).Direction = ParameterDirection.Input
//        .Add("Last_Modified_By_Fk_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input
//        .Add("Version_No_IN", M_Version_No).Direction = ParameterDirection.Input
//    End With
//    objWS.MyCommand.CommandText = "Customer_Type_TYPE_MST_TBL_UPD"
//    If objWS.ExecuteCommands() = True Then
//        Return 1
//    Else
//        Return -1
//    End If
//End Function
//#End Region

//#Region "Delete Function"
//Public Function Delete() As Integer
//    Dim objWS As New WorkFlow
//    Dim intPkVal As Int32
//    objWS.MyCommand.CommandType = CommandType.StoredProcedure
//    With objWS.MyCommand.Parameters
//        .Add("Customer_Type_Mst_Pk_IN", M_Customer_Type_Mst_Pk).Direction = ParameterDirection.Input
//        .Add("Version_No_IN", M_Version_No).Direction = ParameterDirection.Input
//    End With
//    objWS.MyCommand.CommandText = "Customer_Type_TYPE_MST_TBL_DEL"
//    If objWS.ExecuteCommands() = True Then
//        Return 1
//    Else
//        Return -1
//    End If
//End Function
//#End Region

//#Region "Fetch All"
//Public Function FetchAll(Optional ByVal Customer_TypeID As String = "", _
//            Optional ByVal Customer_TypeName As String = "", _
//            Optional ByVal SearchType As String = "", _
//            Optional ByRef CurrentPage As Int32 = 0, _
//            Optional ByRef TotalPage As Int32 = 0, _
//            Optional ByVal SortCol As Int16 = 2 _
//            ) As DataSet
//    Dim last As Int32
//    Dim start As Int32
//    Dim strSQL As String
//    Dim strCondition As String
//    Dim TotalRecords As Int32
//    Dim objWF As New WorkFlow

//    If Customer_TypeID.Trim.Length > 0 Then
//        If SearchType = "S" Then
//            strCondition &= vbCrLf & " AND UPPER(Customer_Type_ID) LIKE '" & Customer_TypeID.ToUpper.Replace("'", "''") & "%'" & vbCrLf
//        Else
//            strCondition &= vbCrLf & " AND UPPER(Customer_Type_ID) LIKE '%" & Customer_TypeID.ToUpper.Replace("'", "''") & "%'" & vbCrLf
//        End If
//    End If
//    If Customer_TypeName.Trim.Length > 0 Then
//        If SearchType = "S" Then
//            strCondition &= vbCrLf & " AND UPPER(Customer_Type_NAME) LIKE '" & Customer_TypeName.ToUpper.Replace("'", "''") & "%'" & vbCrLf
//        Else
//            strCondition &= vbCrLf & " AND UPPER(Customer_Type_NAME) LIKE '%" & Customer_TypeName.ToUpper.Replace("'", "''") & "%'" & vbCrLf
//        End If
//    End If

//    strSQL = "SELECT Count(*) from Customer_TYPE_MST_TBL where 1=1"
//    strSQL &= vbCrLf & strCondition
//    TotalRecords = CType(objWF.ExecuteScaler(strSQL), Int32)
//    TotalPage = TotalRecords \ M_MasterPageSize
//    If TotalRecords Mod M_MasterPageSize <> 0 Then
//        TotalPage += 1
//    End If
//    If CurrentPage > TotalPage Then
//        CurrentPage = 1
//    End If
//    If TotalRecords = 0 Then
//        CurrentPage = 0
//    End If
//    last = CurrentPage * M_MasterPageSize
//    start = (CurrentPage - 1) * M_MasterPageSize + 1

//    If CInt(SortCol) > 0 Then
//        strCondition = strCondition & " order by " & CInt(SortCol)
//    End If

//    strSQL = " select * from ("
//    strSQL &= "SELECT  ROWNUM SR_NO,q.* FROM "
//    strSQL &= vbCrLf & "(SELECT "
//    strSQL &= vbCrLf & "Customer_Type_PK Customer_Type_PK, "
//    strSQL &= vbCrLf & "Customer_Type_ID, "
//    strSQL &= vbCrLf & "Initcap(Customer_Type_NAME) Customer_Type_NAME, "
//    strSQL &= vbCrLf & "Version_No  "
//    strSQL &= vbCrLf & "FROM Customer_TYPE_MST_TBL "
//    strSQL &= vbCrLf & "WHERE 1=1"
//    strSQL &= vbCrLf & strCondition
//    'strSQL = strSQL & " order by Customer_Type_ID"
//    strSQL = strSQL & vbCrLf & " )q)  WHERE sr_no  Between " & start & " and " & last
//    'strSQL &= vbCrLf & " Order By Customer_Type_ID"
//    Try
//        Return objWF.GetDataSet(strSQL)
//    Catch sqlExp As OracleException
//        ErrorMessage = sqlExp.Message
//        Throw sqlExp
//    Catch exp As Exception
//        ErrorMessage = exp.Message
//        Throw exp
//    End Try
//End Function
//#End Region

//#Region "Fetch Customer_Type"
//Public Function FetchCustomer_Type(Optional ByVal Customer_TypePK As Int16 = 0, _
//            Optional ByVal Customer_TypeID As String = "", _
//            Optional ByVal Customer_TypeName As String = "" _
//            ) As DataSet
//    Dim strSQL As String
//    strSQL = "SELECT ' ' CUSTOMER_TYPE_ID,"
//    strSQL = strSQL & "' ' CUSTOMER_TYPE_NAME, "
//    strSQL = strSQL & "0 CUSTOMER_TYPE_PK "
//    strSQL = strSQL & "FROM CUSTOMER_TYPE_MST_TBL "
//    strSQL = strSQL & "UNION "
//    strSQL = strSQL & "SELECT CUSTOMER_TYPE_ID, "
//    strSQL = strSQL & "CUSTOMER_TYPE_NAME,"
//    strSQL = strSQL & "CUSTOMER_TYPE_PK "
//    strSQL = strSQL & "FROM CUSTOMER_TYPE_MST_TBL"
//    strSQL &= " ORDER BY CUSTOMER_TYPE_ID"
//    'strSQL = strSQL & "order by Customer_Type_ID"
//    Dim objWF As New WorkFlow
//    Try
//        Return objWF.GetDataSet(strSQL)
//    Catch sqlExp As OracleException
//        ErrorMessage = sqlExp.Message
//        Throw sqlExp
//    Catch exp As Exception
//        ErrorMessage = exp.Message
//        Throw exp
//    End Try
//End Function
//Public Function FetchCustomer_Type_List(Optional ByVal Customer_TypePK As Int16 = 0, _
//           Optional ByVal Customer_TypeID As String = "", _
//           Optional ByVal Customer_TypeName As String = "" _
//           ) As OracleClient.OracleDataReader
//    Dim strSQL As String
//    'strSQL = "SELECT ' ' CUSTOMER_TYPE_ID,"
//    'strSQL = strSQL & "' ' CUSTOMER_TYPE_NAME, "
//    'strSQL = strSQL & "0 CUSTOMER_TYPE_PK "
//    'strSQL = strSQL & "FROM CUSTOMER_TYPE_MST_TBL "
//    'strSQL = strSQL & "UNION "
//    strSQL = strSQL & "SELECT CUSTOMER_TYPE_ID, "
//    strSQL = strSQL & "CUSTOMER_TYPE_NAME,"
//    strSQL = strSQL & "CUSTOMER_TYPE_PK "
//    strSQL = strSQL & "FROM CUSTOMER_TYPE_MST_TBL"
//    strSQL &= " ORDER BY CUSTOMER_TYPE_ID"
//    'strSQL = strSQL & "order by Customer_Type_ID"
//    Dim objWF As New WorkFlow
//    Try
//        Return objWF.GetDataReader(strSQL)
//    Catch sqlExp As OracleException
//        ErrorMessage = sqlExp.Message
//        Throw sqlExp
//    Catch exp As Exception
//        ErrorMessage = exp.Message
//        Throw exp
//    End Try
//End Function
//Public Function FetchCustomerType() As DataSet
//    Dim strSQL As String
//    strSQL = strSQL & "Select Customer_Type_ID, "
//    strSQL = strSQL & "Customer_Type_NAME,"
//    strSQL = strSQL & "Customer_Type_PK "
//    strSQL = strSQL & "from Customer_Type_MST_TBL"
//    strSQL &= " order by Customer_Type_ID"

//    Dim objWF As New WorkFlow
//    Try
//        Return objWF.GetDataSet(strSQL)
//    Catch sqlExp As OracleException
//        ErrorMessage = sqlExp.Message
//        Throw sqlExp
//    Catch exp As Exception
//        ErrorMessage = exp.Message
//        Throw exp
//    End Try

//End Function
//#End Region

//#Region "Save Function"

//Public Function Save(ByRef M_DataSet As DataSet) As ArrayList
//    Dim objWK As New WorkFlow
//    objWK.OpenConnection()
//    Dim TRAN As OracleTransaction
//    TRAN = objWK.MyConnection.BeginTransaction()

//    Dim ColPara As New OracleClient.OracleParameterCollection
//    Dim intPKVal As Integer
//    Dim lngI As Long
//    Dim RecAfct As Int32
//    Dim insCommand As New OracleClient.OracleCommand
//    Dim updCommand As New OracleClient.OracleCommand
//    Dim delCommand As New OracleClient.OracleCommand

//    Try

//        Dim DtTbl As New DataTable
//        Dim DtRw As DataRow
//        Dim i As Integer
//        DtTbl = M_DataSet.Tables(0)
//        With insCommand
//            .Connection = objWK.MyConnection
//            .CommandType = CommandType.StoredProcedure
//            .CommandText = objWK.MyUserName & ".Customer_TYPE_MST_TBL_PKG.Customer_TYPE_MST_TBL_INS"
//            With .Parameters

//                insCommand.Parameters.Add("CUSTOMER_TYPE_ID_IN", OracleClient.OracleDbType.Varchar2, 20, "Customer_Type_ID").Direction = ParameterDirection.Input
//                insCommand.Parameters["CUSTOMER_TYPE_ID_IN"].SourceVersion = DataRowVersion.Current

//                insCommand.Parameters.Add("CUSTOMER_TYPE_NAME_IN", OracleClient.OracleDbType.Varchar2, 50, "Customer_Type_NAME").Direction = ParameterDirection.Input
//                insCommand.Parameters["CUSTOMER_TYPE_NAME_IN"].SourceVersion = DataRowVersion.Current

//                insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input

//                insCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input

//                insCommand.Parameters.Add("RETURN_VALUE", OracleClient.OracleDbType.Int32, 10, "Customer_Type_TYPE_MST_TBL_PK").Direction = ParameterDirection.Output
//                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current
//            End With
//        End With

//        With delCommand
//            .Connection = objWK.MyConnection
//            .CommandType = CommandType.StoredProcedure
//            .CommandText = objWK.MyUserName & ".Customer_TYPE_MST_TBL_PKG.Customer_TYPE_MST_TBL_DEL"
//            With .Parameters
//                delCommand.Parameters.Add("CUSTOMER_TYPE_PK_IN", OracleClient.OracleDbType.Int32, 10, "Customer_Type_PK").Direction = ParameterDirection.Input
//                delCommand.Parameters["CUSTOMER_TYPE_PK_IN"].SourceVersion = DataRowVersion.Current

//                delCommand.Parameters.Add("DELETED_BY_FK_IN", M_Last_Modified_By_FK).Direction = ParameterDirection.Input

//                delCommand.Parameters.Add("VERSION_NO_IN", OracleClient.OracleDbType.Int32, 4, "Version_No").Direction = ParameterDirection.Input
//                delCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current

//                delCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input

//                delCommand.Parameters.Add("RETURN_VALUE", OracleClient.OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output
//                delCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current
//            End With
//        End With

//        With updCommand
//            .Connection = objWK.MyConnection
//            .CommandType = CommandType.StoredProcedure
//            .CommandText = objWK.MyUserName & ".Customer_TYPE_MST_TBL_PKG.Customer_TYPE_MST_TBL_UPD"
//            With .Parameters

//                updCommand.Parameters.Add("CUSTOMER_TYPE_PK_IN", OracleClient.OracleDbType.Int32, 10, "Customer_Type_PK").Direction = ParameterDirection.Input
//                updCommand.Parameters["CUSTOMER_TYPE_PK_IN"].SourceVersion = DataRowVersion.Current

//                updCommand.Parameters.Add("CUSTOMER_TYPE_ID_IN", OracleClient.OracleDbType.Varchar2, 20, "Customer_Type_ID").Direction = ParameterDirection.Input
//                updCommand.Parameters["CUSTOMER_TYPE_ID_IN"].SourceVersion = DataRowVersion.Current

//                updCommand.Parameters.Add("CUSTOMER_TYPE_NAME_IN", OracleClient.OracleDbType.Varchar2, 50, "Customer_Type_NAME").Direction = ParameterDirection.Input
//                updCommand.Parameters["CUSTOMER_TYPE_NAME_IN"].SourceVersion = DataRowVersion.Current

//                updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_Last_Modified_By_FK).Direction = ParameterDirection.Input

//                updCommand.Parameters.Add("VERSION_NO_IN", OracleClient.OracleDbType.Int32, 4, "Version_No").Direction = ParameterDirection.Input
//                updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current

//                updCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input

//                updCommand.Parameters.Add("RETURN_VALUE", OracleClient.OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output
//                updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current
//            End With
//        End With

//        AddHandler objWK.MyDataAdapter.RowUpdated, New OracleRowUpdatedEventHandler(AddressOf OnRowUpdated)

//        With objWK.MyDataAdapter

//            .InsertCommand = insCommand
//            .InsertCommand.Transaction = TRAN
//            .UpdateCommand = updCommand
//            .UpdateCommand.Transaction = TRAN
//            .DeleteCommand = delCommand
//            .DeleteCommand.Transaction = TRAN
//            RecAfct = .Update(M_DataSet)
//            TRAN.Commit()
//            If arrMessage.Count > 0 Then
//                Return arrMessage
//            Else
//                arrMessage.Add("All Data Saved Successfully")
//                Return arrMessage
//            End If

//        End With
//    Catch oraexp As OracleException
//        Throw oraexp
//    Catch ex As Exception
//        Throw ex
//    End Try
//End Function
//#End Region

#endregion " Previous Code "