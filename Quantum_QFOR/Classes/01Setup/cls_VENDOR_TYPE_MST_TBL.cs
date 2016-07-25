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
    public class clsVENDOR_TYPE_MST_TBL : CommonFeatures
    {
        #region "List of Members of the Class"

        /// <summary>
        /// The m_ vendor_ type_ pk
        /// </summary>
        private Int64 M_Vendor_Type_Pk;
        /// <summary>
        /// The m_ vendor_ type_ identifier
        /// </summary>
        private string M_Vendor_Type_Id;
        /// <summary>
        /// The m_ vendor_ type_ name
        /// </summary>
        private string M_Vendor_Type_Name;

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
        /// Gets or sets the vendor_ type_ pk.
        /// </summary>
        /// <value>
        /// The vendor_ type_ pk.
        /// </value>
        public Int64 Vendor_Type_PK
        {
            get { return M_Vendor_Type_Pk; }
            set { M_Vendor_Type_Pk = value; }
        }

        /// <summary>
        /// Gets or sets the vendor_ type_ identifier.
        /// </summary>
        /// <value>
        /// The vendor_ type_ identifier.
        /// </value>
        public string Vendor_Type_Id
        {
            get { return M_Vendor_Type_Id; }
            set { M_Vendor_Type_Id = value; }
        }

        /// <summary>
        /// Gets or sets the name of the vendor_ type_.
        /// </summary>
        /// <value>
        /// The name of the vendor_ type_.
        /// </value>
        public string Vendor_Type_Name
        {
            get { return M_Vendor_Type_Name; }
            set { M_Vendor_Type_Name = value; }
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
        /// <param name="VendorTypeID">The vendor type identifier.</param>
        /// <param name="VendorTypeName">Name of the vendor type.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="SortColumn">The sort column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="ActiveFlag">if set to <c>true</c> [active flag].</param>
        /// <param name="SortType">Type of the sort.</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public DataSet FetchAll(string VendorTypeID = "", string VendorTypeName = "", string SearchType = "", string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, bool ActiveFlag = true, string SortType = " ASC ", Int32 flag = 0)
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
            if (VendorTypeID.Trim().Length > 0)
            {
                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition += " AND UPPER(VENDOR_TYPE_ID) LIKE '" + VendorTypeID.ToUpper().Replace("'", "''") + "%'";
                    }
                    else
                    {
                        strCondition += " AND UPPER(VENDOR_TYPE_ID) LIKE '%" + VendorTypeID.ToUpper().Replace("'", "''") + "%'";
                    }
                }
                else
                {
                    strCondition += " AND UPPER(VENDOR_TYPE_ID) LIKE '%" + VendorTypeID.ToUpper().Replace("'", "''") + "%'";
                }
            }

            if (VendorTypeName.Trim().Length > 0)
            {
                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition += " AND UPPER(VENDOR_TYPE_NAME) LIKE '" + VendorTypeName.ToUpper().Replace("'", "''") + "%'";
                    }
                    else
                    {
                        strCondition += " AND UPPER(VENDOR_TYPE_NAME) LIKE '%" + VendorTypeName.ToUpper().Replace("'", "''") + "%'";
                    }
                }
                else
                {
                    strCondition += " AND UPPER(VENDOR_TYPE_NAME) LIKE '%" + VendorTypeName.ToUpper().Replace("'", "''") + "%'";
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

            strSQL = "SELECT Count(*) from VENDOR_TYPE_MST_TBL where 1=1 ";
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
            strSQL += "VENDOR_TYPE_PK, ";
            strSQL += "NVL(ACTIVE_FLAG,0) ACTIVE_FLAG , ";
            strSQL += "VENDOR_TYPE_ID, ";
            strSQL += "VENDOR_TYPE_NAME, ";
            strSQL += "VERSION_NO  ";
            strSQL += "FROM VENDOR_TYPE_MST_TBL ";
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
                string UserName = objWK.MyUserName;
                INS_Proc = UserName + ".VENDOR_TYPE_MST_TBL_PKG.VENDOR_TYPE_MST_TBL_INS";
                DEL_Proc = UserName + ".VENDOR_TYPE_MST_TBL_PKG.VENDOR_TYPE_MST_TBL_DEL";
                UPD_Proc = UserName + ".VENDOR_TYPE_MST_TBL_PKG.VENDOR_TYPE_MST_TBL_UPD";

                var _with1 = insCommand;
                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = INS_Proc;

                _with1.Parameters.Add("VENDOR_TYPE_ID_IN", OracleDbType.Varchar2, 20, "VENDOR_TYPE_ID").Direction = ParameterDirection.Input;
                _with1.Parameters["VENDOR_TYPE_ID_IN"].SourceVersion = DataRowVersion.Current;

                _with1.Parameters.Add("VENDOR_TYPE_NAME_IN", OracleDbType.Varchar2, 50, "VENDOR_TYPE_NAME").Direction = ParameterDirection.Input;
                _with1.Parameters["VENDOR_TYPE_NAME_IN"].SourceVersion = DataRowVersion.Current;

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

                _with2.Parameters.Add("VENDOR_TYPE_PK_IN", OracleDbType.Int32, 10, "VENDOR_TYPE_PK").Direction = ParameterDirection.Input;
                _with2.Parameters["VENDOR_TYPE_PK_IN"].SourceVersion = DataRowVersion.Current;

                _with2.Parameters.Add("DELETED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

                //Added by Mikky on 12/10/2005 for Concurrency Check
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

                _with3.Parameters.Add("VENDOR_TYPE_PK_IN", OracleDbType.Int32, 10, "VENDOR_TYPE_PK").Direction = ParameterDirection.Input;
                _with3.Parameters["VENDOR_TYPE_PK_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("VENDOR_TYPE_ID_IN", OracleDbType.Varchar2, 20, "VENDOR_TYPE_ID").Direction = ParameterDirection.Input;
                _with3.Parameters["VENDOR_TYPE_ID_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("VENDOR_TYPE_NAME_IN", OracleDbType.Varchar2, 50, "VENDOR_TYPE_NAME").Direction = ParameterDirection.Input;
                _with3.Parameters["VENDOR_TYPE_NAME_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
                _with3.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

                //Added by Mikky on 12/10/2005 for Concurrency Check
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
                if (arrMessage.Count > 0)
                {
                    return arrMessage;
                }
                else
                {
                    //arrMessage.Add("All Data Saved Successfully") ''sivachandran 05Jun08 Imp-Exp-Wiz 16May08
                    if (Import == false)
                    {
                        arrMessage.Add("All Data Saved Successfully");
                    }
                    else
                    {
                        arrMessage.Add("Data Imported Successfully");
                    }
                    //End
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
            }
            finally
            {
                objWK.CloseConnection();
            }
        }

        #endregion "Save Function"

        #region " Fetch for Import"

        //Sivachandran 07Jun08 Imp_Exp_Wiz16May08
        /// <summary>
        /// Fetches this instance.
        /// </summary>
        /// <returns></returns>
        public DataSet Fetch()
        {
            WorkFlow objWF = new WorkFlow();
            string SQL = null;
            SQL = "SELECT * FROM vendor_type_mst_tbl";
            try
            {
                return objWF.GetDataSet(SQL);
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

        //End

        #endregion " Fetch for Import"
    }
}

#region " Previous Code "

//#Region "Fetch Function"

//        Public Function FetchAll(Optional ByVal P_Vendor_Type_Pk As Int64 = 0, _
//            Optional ByVal P_Vendor_Type_Id As String = "", _
//            Optional ByVal P_Vendor_Type As String = "", _
//            Optional ByVal SearchType As String = "", _
//            Optional ByVal SortExpression As String = "", _
//            Optional ByRef CurrentPage As Int32 = 0, _
//            Optional ByRef TotalPage As Int32 = 0, _
//            Optional ByVal SortCol As Int16 = 2 _
//                ) As DataSet

//            Dim last As Int32
//            Dim start As Int32
//            Dim strSQL As String
//            Dim strCondition As String
//            Dim TotalRecords As Int32
//            Dim objWF As New Business.WorkFlow

//            If P_Vendor_Type_Pk > 0 Then
//                strCondition &= vbCrLf & " AND Vendor_Type_Pk=" & P_Vendor_Type_Pk
//            End If

//            If P_Vendor_Type_Id.Trim.Length > 0 Then
//                If SearchType.ToString.Trim.Length > 0 Then
//                    If SearchType = "S" Then
//                        strCondition &= vbCrLf & " AND UPPER(Vendor_Type_Id) LIKE '" & P_Vendor_Type_Id.ToUpper.Replace("'", "''") & "%'" & vbCrLf
//                    Else
//                        strCondition &= vbCrLf & " AND UPPER(Vendor_Type_Id) LIKE '%" & P_Vendor_Type_Id.ToUpper.Replace("'", "''") & "%'" & vbCrLf
//                    End If
//                Else
//                    strCondition &= vbCrLf & " AND UPPER(Vendor_Type_Id) LIKE '%" & P_Vendor_Type_Id.ToUpper.Replace("'", "''") & "%'" & vbCrLf
//                End If
//            End If

//            If P_Vendor_Type.Trim.Length > 0 Then
//                If SearchType.ToString.Trim.Length > 0 Then
//                    If SearchType = "S" Then
//                        strCondition &= vbCrLf & " AND UPPER(Vendor_Type) LIKE '" & P_Vendor_Type.ToUpper.Replace("'", "''") & "%'" & vbCrLf
//                    Else
//                        strCondition &= vbCrLf & " AND UPPER(Vendor_Type) LIKE '%" & P_Vendor_Type.ToUpper.Replace("'", "''") & "%'" & vbCrLf
//                    End If
//                Else
//                    strCondition &= vbCrLf & " AND UPPER(Vendor_Type) LIKE '%" & P_Vendor_Type.ToUpper.Replace("'", "''") & "%'" & vbCrLf
//                End If
//            End If

//            strSQL = "SELECT Count(*) from VENDOR_TYPE_MST_TBL where 1=1"
//            strSQL &= vbCrLf & strCondition
//            TotalRecords = CType(objWF.ExecuteScaler(strSQL), Int32)
//            TotalPage = TotalRecords \ M_MasterPageSize
//            If TotalRecords Mod M_MasterPageSize <> 0 Then
//                TotalPage += 1
//            End If
//            If CurrentPage > TotalPage Then
//                CurrentPage = 1
//            End If
//            If TotalRecords = 0 Then
//                CurrentPage = 0
//            End If
//            last = CurrentPage * M_MasterPageSize
//            start = (CurrentPage - 1) * M_MasterPageSize + 1

//            If CInt(SortCol) > 0 Then
//                strCondition = strCondition & " order by " & CInt(SortCol)
//            End If

//            strSQL = "select * from ( "
//            strSQL = strSQL & vbCrLf & "SELECT ROWNUM SR_NO,q.* FROM "
//            strSQL = strSQL & vbCrLf & "(SELECT  "
//            strSQL = strSQL & vbCrLf & " Vendor_Type_Pk,"
//            strSQL = strSQL & vbCrLf & " Vendor_Type_Id,"
//            strSQL = strSQL & vbCrLf & " Vendor_Type,"
//            strSQL = strSQL & vbCrLf & " Version_No "
//            strSQL = strSQL & vbCrLf & " FROM VENDOR_TYPE_MST_TBL "
//            strSQL = strSQL & vbCrLf & " WHERE 1=1 "
//            strSQL &= vbCrLf & strCondition

//            'If SortExpression.Trim.Length > 0 Then
//            '    strSQL &= vbCrLf & " " & SortExpression
//            'Else
//            '    strSQL &= vbCrLf & " order by Vendor_Type_Id"
//            'End If
//            strSQL &= vbCrLf & " )q) WHERE SR_NO  Between " & start & " and " & last
//            'strSQL &= vbCrLf & " Order By SR_NO "

//            'If P_Vendor_Type_Pk > 0 Then
//            '    strSQL = strSQL & " And Vendor_Type_Pk = " & P_Vendor_Type_Pk & " "
//            'End If
//            'If P_Vendor_Type_Id.ToString.Trim.Length > 0 Then
//            '    If SearchType = "C" Then
//            '        strSQL = strSQL & " And Vendor_Type_Id like '%" & P_Vendor_Type_Id & "%' "
//            '    Else
//            '        strSQL = strSQL & " And Vendor_Type_Id like '" & P_Vendor_Type_Id & "%' "
//            '    End If
//            'Else
//            'End If
//            'If P_Vendor_Type.ToString.Trim.Length > 0 Then
//            '    If SearchType = "C" Then
//            '        strSQL = strSQL & " And Vendor_Type like '%" & P_Vendor_Type & "%' "
//            '    Else
//            '        strSQL = strSQL & " And Vendor_Type like '" & P_Vendor_Type & "%' "
//            '    End If
//            'Else
//            'End If

//            Try
//                Return objWF.GetDataSet(strSQL)
//            Catch sqlExp As OracleException
//                ErrorMessage = sqlExp.Message
//                Throw sqlExp
//            Catch exp As Exception
//                ErrorMessage = exp.Message
//                Throw exp
//            End Try
//        End Function
//#End Region

//#Region "Save Function"

//        Public Function Save(ByRef M_DataSet As DataSet) As ArrayList
//            Dim objWK As New QFORBusinessDev.Business.WorkFlow
//            objWK.OpenConnection()
//            Dim TRAN As OracleTransaction
//            TRAN = objWK.MyConnection.BeginTransaction()

//            Dim ColPara As New OracleClient.OracleParameterCollection
//            Dim intPKVal As Integer
//            Dim lngI As Long
//            Dim RecAfct As Int32
//            Dim insCommand As New OracleClient.OracleCommand
//            Dim updCommand As New OracleClient.OracleCommand
//            Dim delCommand As New OracleClient.OracleCommand

//                Try

//                    Dim DtTbl As New DataTable
//                    Dim DtRw As DataRow
//                    Dim i As Integer
//                    DtTbl = M_DataSet.Tables(0)

//                With insCommand
//                    .Connection = objWK.MyConnection
//                    .CommandType = CommandType.StoredProcedure
//                    .CommandText = objWK.MyUserName & ".VENDOR_TYPE_MST_TBL_PKG.VENDOR_TYPE_MST_TBL_INS"

//                    With .Parameters

//                        insCommand.Parameters.Add("VENDOR_TYPE_ID_IN", OracleClient.OracleDbType.Varchar2, 0, "VENDOR_TYPE_ID").Direction = ParameterDirection.Input
//                        insCommand.Parameters["VENDOR_TYPE_ID_IN"].SourceVersion = DataRowVersion.Current

//                        insCommand.Parameters.Add("VENDOR_TYPE_IN", OracleClient.OracleDbType.Varchar2, 20, "VENDOR_TYPE").Direction = ParameterDirection.Input
//                        insCommand.Parameters["VENDOR_TYPE_IN"].SourceVersion = DataRowVersion.Current

//                        insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input
//                        insCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input

//                        insCommand.Parameters.Add("RETURN_VALUE", OracleClient.OracleDbType.Int32, 10, "VENDOR_TYPE_MST_TBL_PK").Direction = ParameterDirection.Output
//                        insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current
//                    End With
//                End With

//                With delCommand
//                    .Connection = objWK.MyConnection
//                    .CommandType = CommandType.StoredProcedure
//                    .CommandText = objWK.MyUserName & ".VENDOR_TYPE_MST_TBL_PKG.VENDOR_TYPE_MST_TBL_DEL"
//                    With .Parameters

//                        delCommand.Parameters.Add("VENDOR_TYPE_PK_IN", OracleClient.OracleDbType.Int32, 10, "VENDOR_TYPE_PK").Direction = ParameterDirection.Input
//                        delCommand.Parameters["VENDOR_TYPE_PK_IN"].SourceVersion = DataRowVersion.Current

//                        delCommand.Parameters.Add("DELETED_BY_FK_IN", M_Last_Modified_By_FK).Direction = ParameterDirection.Input

//                        delCommand.Parameters.Add("VERSION_NO_IN", OracleClient.OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input
//                        delCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current
//                        delCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input
//                        delCommand.Parameters.Add("RETURN_VALUE", OracleClient.OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output
//                        delCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current
//                    End With
//                End With

//                With updCommand
//                    .Connection = objWK.MyConnection
//                    .CommandType = CommandType.StoredProcedure
//                    .CommandText = objWK.MyUserName & ".VENDOR_TYPE_MST_TBL_PKG.VENDOR_TYPE_MST_TBL_UPD"
//                    With .Parameters

//                        updCommand.Parameters.Add("VENDOR_TYPE_PK_IN", OracleClient.OracleDbType.Int32, 10, "VENDOR_TYPE_PK").Direction = ParameterDirection.Input
//                        updCommand.Parameters["VENDOR_TYPE_PK_IN"].SourceVersion = DataRowVersion.Current

//                        updCommand.Parameters.Add("VENDOR_TYPE_ID_IN", OracleClient.OracleDbType.Varchar2, 0, "VENDOR_TYPE_ID").Direction = ParameterDirection.Input
//                        updCommand.Parameters["VENDOR_TYPE_ID_IN"].SourceVersion = DataRowVersion.Current

//                        updCommand.Parameters.Add("VENDOR_TYPE_IN", OracleClient.OracleDbType.Varchar2, 0, "VENDOR_TYPE").Direction = ParameterDirection.Input
//                        updCommand.Parameters["VENDOR_TYPE_IN"].SourceVersion = DataRowVersion.Current

//                        updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_Last_Modified_By_FK).Direction = ParameterDirection.Input

//                        updCommand.Parameters.Add("VERSION_NO_IN", OracleClient.OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input
//                        updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current
//                        updCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input
//                        updCommand.Parameters.Add("RETURN_VALUE", OracleClient.OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output
//                        updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current
//                    End With
//                End With

//                AddHandler objWK.MyDataAdapter.RowUpdated, New OracleRowUpdatedEventHandler(AddressOf OnRowUpdated)

//                'objWK.MyDataAdapter.ContinueUpdateOnError = True
//                With objWK.MyDataAdapter

//                    .InsertCommand = insCommand
//                    .InsertCommand.Transaction = TRAN
//                    .UpdateCommand = updCommand
//                    .UpdateCommand.Transaction = TRAN
//                    .DeleteCommand = delCommand
//                    .DeleteCommand.Transaction = TRAN
//                    RecAfct = .Update(M_DataSet)
//                    TRAN.Commit()
//                    If arrMessage.Count > 0 Then
//                        Return arrMessage
//                    Else
//                        arrMessage.Add("All Data Saved Successfully")
//                        Return arrMessage
//                    End If

//                End With
//            Catch oraexp As OracleException
//                    Throw oraexp
//                Catch ex As Exception
//                    Throw ex
//                End Try
//        End Function
//#End Region

#endregion " Previous Code "