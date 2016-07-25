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
    public class Cls_SRRSeaApproval : CommonFeatures
    {
        #region "Private Variables"

        private long _PkValue;

        #endregion "Private Variables"

        private string _CustomerContractNo;

        #region "Property"

        public long PkValue
        {
            get { return _PkValue; }
        }

        public string CustomerContractNo
        {
            get { return _CustomerContractNo; }
        }

        #endregion "Property"

        //
        // This region is called while editng and veiwing the SRR.
        // Region takes care about cargo type (FCL/LCL) and status of SRR and
        // displays data accordingly

        #region "Fetch Queries"

        // This procedure is called while editing or viewing the record.
        // Param name="Srrpk"      :- Primary Key of SRR sent ByValue
        // Param name="dsGrid"     :- Relational dataset for grid sent ByRef
        // Param name="dtMain"     :- Dataset containing header information
        // Param name="IsLCL"      :- Boolean variable for checking cargo type
        // Param name="Status"     :- Boolean variable for checking status of SRR
        // Exception cref="sqlExp" :- Catch SQL Exception
        public void Fetch_SRR(long SrrPk, DataSet dsGrid, DataTable dtMain, bool IsLCL, Int16 Status, bool save, int oGroup)
        {
            WorkFlow objWF = new WorkFlow();
            try
            {
                //Fetching SRR Header Information
                objWF.MyCommand.Parameters.Clear();
                var _with1 = objWF.MyCommand.Parameters;
                _with1.Add("SRRPK_IN", SrrPk).Direction = ParameterDirection.Input;
                _with1.Add("SRR_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dtMain = objWF.GetDataTable("FETCH_SRR_PKG", "FETCH_SRR");

                //Fetching SRR Transaction Information
                dsGrid.Tables.Add(Transaction(objWF, IsLCL, Status, SrrPk, oGroup));
                dsGrid.Tables[0].TableName = "Main";

                //Fetching SRR Surcharge Information
                dsGrid.Tables.Add(Surcharge(objWF, IsLCL, Status, SrrPk, save, oGroup));
                dsGrid.Tables[1].TableName = "Frt";

                //Making relation  between Main and Frt table of dsGrid
                //Relation between:
                //                 Main Table            Frt Table
                //                 ---------             ---------
                //                 1. POLPK              1. POLPK
                //                 2. PODPK              2. PODPK
                //                 3. CONT_BASIS         3. CONT_BASIS

                DataColumn[] dcParent = null;
                DataColumn[] dcChild = null;
                DataRelation re = null;

                dcParent = new DataColumn[] {
                    dsGrid.Tables["Main"].Columns["POLPK"],
                    dsGrid.Tables["Main"].Columns["PODPK"],
                    dsGrid.Tables["Main"].Columns["CONT_BASIS"]
                };

                dcChild = new DataColumn[] {
                    dsGrid.Tables["Frt"].Columns["POLPK"],
                    dsGrid.Tables["Frt"].Columns["PODPK"],
                    dsGrid.Tables["Frt"].Columns["CONT_BASIS"]
                };

                re = new DataRelation("rl_Port", dcParent, dcChild);
                //Adding relation to the grid.
                dsGrid.Relations.Add(re);
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        //Fetching SRR Transaction Information
        //If NOT IsLCL then
        //   if Status > 0 (Internal Approved or Customer Approved)
        //       Fetch data form Customer Contract
        //   else
        //       Fetch data from SRR
        //   end if
        //else
        //   if Status > 0 (Internal Approved or Customer Approved)
        //       Fetch data form Customer Contract
        //   else
        //       Fetch data from SRR
        //   end if
        //end if
        //Manoharan 15July2008: while coming from Message link the Cargotype is not assigned properly
        public Int16 getCargotype(long srrPk)
        {
            WorkFlow objWf = new WorkFlow();
            OracleCommand objCmd = new OracleCommand();
            OracleTransaction TRAN = null;
            Int16 retValue = default(Int16);
            string strSql = null;

            strSql = "select s.cargo_type from SRR_SEA_TBL s where s.srr_sea_pk = " + srrPk;

            objWf.OpenConnection();
            TRAN = objWf.MyConnection.BeginTransaction();
            objCmd.Connection = objWf.MyConnection;
            objCmd.CommandType = CommandType.Text;
            objCmd.CommandText = strSql;
            objCmd.Transaction = TRAN;

            retValue = Convert.ToInt16(objCmd.ExecuteScalar());
            return retValue;
        }

        private DataTable Transaction(WorkFlow objWF, bool IsLCL, Int16 Status, long SrrPk, int oGroup)
        {
            try
            {
                objWF.MyCommand.Parameters.Clear();
                if (!IsLCL)
                {
                    if (Status > 0)
                    {
                        var _with2 = objWF.MyCommand.Parameters;
                        _with2.Add("SRRPK_IN", SrrPk).Direction = ParameterDirection.Input;
                        _with2.Add("CUST_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                        if (oGroup == 0)
                        {
                            return objWF.GetDataTable("FETCH_SRR_PKG", "FETCH_FCL_CUSTOMER");
                        }
                        else
                        {
                            return objWF.GetDataTable("FETCH_SRR_PKG", "FETCH_FCL_CUSTOMER_GROUP");
                        }
                    }
                    else
                    {
                        var _with3 = objWF.MyCommand.Parameters;
                        _with3.Add("SRRPK_IN", SrrPk).Direction = ParameterDirection.Input;
                        _with3.Add("SRR_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                        if (oGroup == 0)
                        {
                            return objWF.GetDataTable("FETCH_SRR_PKG", "FETCH_FCL_SRR");
                        }
                        else
                        {
                            return objWF.GetDataTable("FETCH_SRR_PKG", "FETCH_FCL_SRR_GROUP");
                        }
                    }
                }
                else
                {
                    if (Status > 0)
                    {
                        var _with4 = objWF.MyCommand.Parameters;
                        _with4.Add("SRRPK_IN", SrrPk).Direction = ParameterDirection.Input;
                        _with4.Add("CUST_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                        if (oGroup == 0)
                        {
                            return objWF.GetDataTable("FETCH_SRR_PKG", "FETCH_LCL_CUSTOMER");
                        }
                        else
                        {
                            return objWF.GetDataTable("FETCH_SRR_PKG", "FETCH_LCL_CUSTOMER_GROUP");
                        }
                    }
                    else
                    {
                        var _with5 = objWF.MyCommand.Parameters;
                        _with5.Add("SRRPK_IN", SrrPk).Direction = ParameterDirection.Input;
                        _with5.Add("SRR_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                        if (oGroup == 0)
                        {
                            return objWF.GetDataTable("FETCH_SRR_PKG", "FETCH_LCL_SRR");
                        }
                        else
                        {
                            return objWF.GetDataTable("FETCH_SRR_PKG", "FETCH_LCL_SRR_GROUP");
                        }
                    }
                }
            }
            catch (OracleException ex)
            {
                throw ex;
                //Manjunath
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        //Fetching SRR Surcharge Information
        //If NOT IsLCL then
        //   if Status > 0 (Internal Approved or Customer Approved)
        //       Fetch data form Customer Contract
        //   else
        //       Fetch data from SRR
        //   end if
        //else
        //   if Status > 0 (Internal Approved or Customer Approved)
        //       Fetch data form Customer Contract
        //   else
        //       Fetch data from SRR
        //   end if
        //end if
        private DataTable Surcharge(WorkFlow objWF, bool IsLCL, Int16 Status, long SrrPk, bool save, int oGroup)
        {
            try
            {
                objWF.MyCommand.Parameters.Clear();
                if (save == true)
                {
                    if (!IsLCL)
                    {
                        if (Status > 0)
                        {
                            var _with6 = objWF.MyCommand.Parameters;
                            _with6.Add("SRRPK_IN", SrrPk).Direction = ParameterDirection.Input;
                            _with6.Add("CUST_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                            if (oGroup == 0)
                            {
                                return objWF.GetDataTable("FETCH_SRR_PKG", "FETCH_FCL_CUSTOMER_SUR");
                            }
                            else
                            {
                                return objWF.GetDataTable("FETCH_SRR_PKG", "FETCH_FCL_CUSTOMER_SUR_GROUP");
                            }
                        }
                        else
                        {
                            var _with7 = objWF.MyCommand.Parameters;
                            _with7.Add("SRRPK_IN", SrrPk).Direction = ParameterDirection.Input;
                            _with7.Add("SRR_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                            if (oGroup == 0)
                            {
                                return objWF.GetDataTable("FETCH_SRR_PKG", "FETCH_FCL_SRR_SUR");
                            }
                            else
                            {
                                return objWF.GetDataTable("FETCH_SRR_PKG", "FETCH_FCL_SRR_SUR_GROUP");
                            }
                        }
                    }
                    else
                    {
                        if (Status > 0)
                        {
                            var _with8 = objWF.MyCommand.Parameters;
                            _with8.Add("SRRPK_IN", SrrPk).Direction = ParameterDirection.Input;
                            _with8.Add("CUST_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                            if (oGroup == 0)
                            {
                                return objWF.GetDataTable("FETCH_SRR_PKG", "FETCH_LCL_CUSTOMER_SUR");
                            }
                            else
                            {
                                return objWF.GetDataTable("FETCH_SRR_PKG", "FETCH_LCL_CUSTOMER_SUR_GROUP");
                            }
                        }
                        else
                        {
                            var _with9 = objWF.MyCommand.Parameters;
                            _with9.Add("SRRPK_IN", SrrPk).Direction = ParameterDirection.Input;
                            _with9.Add("SRR_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                            if (oGroup == 0)
                            {
                                return objWF.GetDataTable("FETCH_SRR_PKG", "FETCH_LCL_SRR_SUR");
                            }
                            else
                            {
                                return objWF.GetDataTable("FETCH_SRR_PKG", "FETCH_LCL_SRR_SUR_GROUP");
                            }
                        }
                    }
                }
                else if (save == false)
                {
                    if (!IsLCL)
                    {
                        if (Status > 0)
                        {
                            var _with10 = objWF.MyCommand.Parameters;
                            _with10.Add("SRRPK_IN", SrrPk).Direction = ParameterDirection.Input;
                            _with10.Add("CUST_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                            if (oGroup == 0)
                            {
                                return objWF.GetDataTable("FETCH_SRR_PKG", "FETCH_FCL_CUSTOMER_SUR");
                            }
                            else
                            {
                                return objWF.GetDataTable("FETCH_SRR_PKG", "FETCH_FCL_CUSTOMER_SUR_GROUP");
                            }
                        }
                        else
                        {
                            var _with11 = objWF.MyCommand.Parameters;
                            _with11.Add("SRRPK_IN", SrrPk).Direction = ParameterDirection.Input;
                            _with11.Add("SRR_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                            if (oGroup == 0)
                            {
                                return objWF.GetDataTable("FETCH_SRR_PKG", "FETCH_FCL_SRR_SUR_FETCH");
                            }
                            else
                            {
                                return objWF.GetDataTable("FETCH_SRR_PKG", "FETCH_FCL_SRR_SUR_FETCH_GROUP");
                            }
                        }
                    }
                    else
                    {
                        if (Status > 0)
                        {
                            var _with12 = objWF.MyCommand.Parameters;
                            _with12.Add("SRRPK_IN", SrrPk).Direction = ParameterDirection.Input;
                            _with12.Add("CUST_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                            if (oGroup == 0)
                            {
                                return objWF.GetDataTable("FETCH_SRR_PKG", "FETCH_LCL_CUSTOMER_SUR");
                            }
                            else
                            {
                                return objWF.GetDataTable("FETCH_SRR_PKG", "FETCH_LCL_CUSTOMER_SUR_GROUP");
                            }
                        }
                        else
                        {
                            var _with13 = objWF.MyCommand.Parameters;
                            _with13.Add("SRRPK_IN", SrrPk).Direction = ParameterDirection.Input;
                            _with13.Add("SRR_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                            if (oGroup == 0)
                            {
                                return objWF.GetDataTable("FETCH_SRR_PKG", "FETCH_LCL_SRR_SUR_FETCH");
                            }
                            else
                            {
                                return objWF.GetDataTable("FETCH_SRR_PKG", "FETCH_LCL_SRR_SUR_FETCH_GROUP");
                            }
                        }
                    }
                }
            }
            catch (OracleException ex)
            {
                throw ex;
                //Manjunath
            }
            catch (Exception exp)
            {
                throw exp;
            }
            return new DataTable();
        }

        #endregion "Fetch Queries"

        //
        // This function saves data in Customer Contract with the status internal approval
        // also the status is updated in SRR so as to restricting user from further modifications.
        // In return this function will give the ContractRefNo. back to the user which he
        // can refer in Customer Contract

        #region "Save"

        //Public Function SaveHDR(ByRef dsMain As DataSet, ByVal nLocationId As Long, ByVal nEmpId As Long, _
        //                        ByVal IsLcl As Boolean) As ArrayList
        //    Dim ContractRefNo As String
        //    Dim objWK As New WorkFlow
        //    Dim TRAN As OracleClient.OracleTransaction
        //    objWK.OpenConnection()
        //    TRAN = objWK.MyConnection.BeginTransaction()
        //    arrMessage.Clear()
        //    objWK.MyCommand.Transaction = TRAN
        //    Try

        //        ContractRefNo = GenerateContractNo(nLocationId, nEmpId, M_CREATED_BY_FK, objWK)
        //        If InStr(ContractRefNo, "Protocol") > 0 Then
        //            arrMessage.Add("Protocol Not Defined.")
        //            Return arrMessage
        //        End If
        //        objWK.MyCommand.Parameters.Clear()
        //        'SRR_SEA_PK_IN           IN SRR_SEA_TBL.SRR_SEA_PK%TYPE,
        //        '            COMMODITY_MST_FK_IN     IN SRR_SEA_TBL.COMMODITY_MST_FK%TYPE,
        //        '            SRR_CLAUSE_IN           IN SRR_SEA_TBL.SRR_CLAUSE%TYPE,
        //        '            PYMT_LOCATION_MST_FK_IN IN SRR_SEA_TBL.PYMT_LOCATION_MST_FK%TYPE,
        //        '            CREDIT_PERIOD_IN        IN SRR_SEA_TBL.CREDIT_PERIOD%TYPE,
        //        '            LAST_MODIFIED_BY_FK_IN  IN SRR_SEA_TBL.LAST_MODIFIED_BY_FK%TYPE,
        //        '            VERSION_NO_IN           IN SRR_SEA_TBL.VERSION_NO%TYPE,
        //        '            CONFIG_PK_IN            IN CONFIG_MST_TBL.CONFIG_MST_PK%TYPE,
        //        '            OPERATOR_MST_FK_IN      IN SRR_SEA_TBL.OPERATOR_MST_FK%TYPE,
        //        '            CUSTOMER_MST_FK_IN      IN SRR_SEA_TBL.CUSTOMER_MST_FK%TYPE,
        //        '            CARGO_TYPE_IN           IN SRR_SEA_TBL.CARGO_TYPE%TYPE,
        //        '            VALID_FROM_IN           IN VARCHAR2,
        //        '            VALID_TO_IN             IN VARCHAR2,
        //        '            STRCONDITION            IN VARCHAR2,
        //        '            COMMODITY_GROUP_FK_IN   IN SRR_SEA_TBL.COMMODITY_GROUP_MST_FK%TYPE,
        //        With objWK.MyCommand
        //            .CommandType = CommandType.StoredProcedure
        //            .CommandText = objWK.MyUserName & ".CONT_CUST_SEA_TBL_PKG.CONT_CUST_SEA_TBL_INS"

        //            .Parameters.Add("OPERATOR_MST_FK_IN", _
        //            CLng(dsMain.Tables("Master").Rows(0).Item("OPERATOR_MST_FK"))).Direction = _
        //            ParameterDirection.Input

        //            .Parameters.Add("CUSTOMER_MST_FK_IN", _
        //            CLng(dsMain.Tables("Master").Rows(0).Item("CUSTOMER_MST_FK"))).Direction = _
        //            ParameterDirection.Input
        //            'SRR_SEA_PK_IN           IN SRR_SEA_TBL.SRR_SEA_PK%TYPE,
        //            .Parameters.Add("SRR_SEA_FK_IN", _
        //            CLng(dsMain.Tables("Master").Rows(0).Item("SRR_SEA_FK"))).Direction = _
        //            ParameterDirection.Input

        //            .Parameters.Add("TARIFF_MAIN_SEA_FK_IN", "").Direction = ParameterDirection.Input

        //            .Parameters.Add("CONT_REF_NO_IN", ContractRefNo).Direction = ParameterDirection.Input

        //            .Parameters.Add("CARGO_TYPE_IN", _
        //            CLng(dsMain.Tables("Master").Rows(0).Item("CARGO_TYPE"))).Direction = _
        //            ParameterDirection.Input

        //            .Parameters.Add("VALID_FROM_IN", _
        //            CStr(dsMain.Tables("Master").Rows(0).Item("VALID_FROM"))).Direction = _
        //            ParameterDirection.Input

        //            .Parameters.Add("VALID_TO_IN", _
        //            CStr(dsMain.Tables("Master").Rows(0).Item("VALID_TO"))).Direction = _
        //            ParameterDirection.Input

        //            .Parameters.Add("STRCONDITION", _
        //            CStr(dsMain.Tables("Master").Rows(0).Item("STRCONDITION"))).Direction = _
        //            ParameterDirection.Input

        //            .Parameters.Add("COMMODITY_GROUP_MST_FK_IN", _
        //            CLng(dsMain.Tables("Master").Rows(0).Item("COMMODITY_GROUP_MST_FK"))).Direction = _
        //            ParameterDirection.Input

        //            If IsDBNull(dsMain.Tables("Master").Rows(0).Item("COMMODITY_MST_FK")) Then
        //                .Parameters.Add("COMMODITY_MST_FK_IN", "").Direction = ParameterDirection.Input
        //            Else
        //                .Parameters.Add("COMMODITY_MST_FK_IN", _
        //                CLng(dsMain.Tables("Master").Rows(0).Item("COMMODITY_MST_FK"))).Direction = _
        //                ParameterDirection.Input
        //            End If

        //            If IsDBNull(dsMain.Tables("Master").Rows(0).Item("CONT_CLAUSE")) Then
        //                .Parameters.Add("CONT_CLAUSE_IN", "").Direction = ParameterDirection.Input
        //            Else
        //                .Parameters.Add("CONT_CLAUSE_IN", _
        //                CStr(dsMain.Tables("Master").Rows(0).Item("CONT_CLAUSE"))).Direction = _
        //                ParameterDirection.Input
        //            End If

        //            If IsDBNull(dsMain.Tables("Master").Rows(0).Item("PYMT_LOCATION_MST_FK")) Then
        //                .Parameters.Add("PYMT_LOCATION_MST_FK_IN", "").Direction = ParameterDirection.Input
        //            Else
        //                .Parameters.Add("PYMT_LOCATION_MST_FK_IN", _
        //                CLng(dsMain.Tables("Master").Rows(0).Item("PYMT_LOCATION_MST_FK"))).Direction = _
        //                ParameterDirection.Input
        //            End If

        //            If IsDBNull(dsMain.Tables("Master").Rows(0).Item("CREDIT_PERIOD")) Then
        //                .Parameters.Add("CREDIT_PERIOD_IN", "").Direction = ParameterDirection.Input
        //            Else
        //                .Parameters.Add("CREDIT_PERIOD_IN", _
        //                CLng(dsMain.Tables("Master").Rows(0).Item("CREDIT_PERIOD"))).Direction = _
        //                ParameterDirection.Input
        //            End If

        //            .Parameters.Add("CONFIG_PK_IN", M_Configuration_PK).Direction = ParameterDirection.Input

        //            .Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input

        //            .Parameters.Add("STATUS_IN", 1).Direction = ParameterDirection.Input

        //            .Parameters.Add("RETURN_VALUE", _
        //            OracleClient.OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output

        //            .ExecuteNonQuery()
        //        End With

        //        If _
        //            InStr(objWK.MyCommand.Parameters["RETURN_VALUE"].Value, "Contract") Or _
        //            InStr(objWK.MyCommand.Parameters["RETURN_VALUE"].Value, "MODIFIED") _
        //        Then
        //            arrMessage.Add(CStr(objWK.MyCommand.Parameters["RETURN_VALUE"].Value))
        //            TRAN.Rollback()
        //            Return arrMessage
        //        Else
        //            _PkValue = CLng(objWK.MyCommand.Parameters["RETURN_VALUE"].Value)
        //        End If

        //        arrMessage = SaveSrrTRN(dsMain, objWK, IsLcl)

        //        If arrMessage.Count > 0 Then
        //            If InStr(arrMessage(0), "saved") Then
        //                arrMessage.Add("All data saved successfully")
        //                TRAN.Commit()
        //                _CustomerContractNo = ContractRefNo
        //                Return arrMessage
        //            Else
        //                TRAN.Rollback()
        //                Return arrMessage
        //            End If
        //        End If
        //    Catch ex As Exception
        //        TRAN.Rollback()
        //        Throw ex
        //    Finally
        //        objWK.MyConnection.Close()
        //    End Try
        //End Function

        //Private Function SaveSrrTRN( _
        //                             ByRef dsMain As DataSet, _
        //                             ByRef objWK As WorkFlow, _
        //                             ByVal IsLCL As Boolean _
        //                             ) As ArrayList
        //    Dim nTransactionRowCnt As Int32
        //    Dim dtTransaction As New DataTable
        //    Dim nTransactionPk As Long = 0
        //    Dim Cont_BasisPk As String

        //    If IsLCL Then
        //        Cont_BasisPk = "LCL_BASIS"
        //    Else
        //        Cont_BasisPk = "CONTAINER_TYPE_MST_FK"
        //    End If

        //    dtTransaction = dsMain.Tables("Transaction")
        //    Try
        //        For nTransactionRowCnt = 0 To dsMain.Tables("Transaction").Rows.Count - 1

        //            objWK.MyCommand.Parameters.Clear()
        //            arrMessage.Clear()
        //            nTransactionPk = 0

        //            With objWK.MyCommand
        //                .CommandType = CommandType.StoredProcedure
        //                .CommandText = objWK.MyUserName & ".CONT_CUST_SEA_TBL_PKG.CONT_CUST_TRN_SEA_TBL_INS"

        //                .Parameters.Add("CONT_CUST_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input

        //                .Parameters.Add("PORT_MST_POL_FK_IN", _
        //                CLng(dtTransaction.Rows(nTransactionRowCnt).Item("PORT_MST_POL_FK"))).Direction = _
        //                ParameterDirection.Input

        //                .Parameters.Add("PORT_MST_POD_FK_IN", _
        //                CLng(dtTransaction.Rows(nTransactionRowCnt).Item("PORT_MST_POD_FK"))).Direction = _
        //                ParameterDirection.Input

        //                If IsDBNull(dtTransaction.Rows(nTransactionRowCnt).Item("CONTAINER_TYPE_MST_FK")) Then
        //                    .Parameters.Add("CONTAINER_TYPE_MST_FK_IN", "").Direction = _
        //                    ParameterDirection.Input
        //                Else
        //                    .Parameters.Add("CONTAINER_TYPE_MST_FK_IN", _
        //                    CLng( _
        //                    dtTransaction.Rows(nTransactionRowCnt).Item("CONTAINER_TYPE_MST_FK")) _
        //                    ).Direction = _
        //                    ParameterDirection.Input
        //                End If

        //                If IsDBNull(dtTransaction.Rows(nTransactionRowCnt).Item("LCL_BASIS")) Then
        //                    .Parameters.Add("LCL_BASIS_IN", "").Direction = ParameterDirection.Input
        //                Else
        //                    .Parameters.Add("LCL_BASIS_IN", _
        //                    CLng(dtTransaction.Rows(nTransactionRowCnt).Item("LCL_BASIS"))).Direction = _
        //                    ParameterDirection.Input
        //                End If

        //                .Parameters.Add("CURRENCY_MST_FK_IN", _
        //                CLng(dtTransaction.Rows(nTransactionRowCnt).Item("CURRENCY_MST_FK"))).Direction = _
        //                ParameterDirection.Input

        //                If IsDBNull(dtTransaction.Rows(nTransactionRowCnt).Item("CURRENT_BOF_RATE")) Then
        //                    .Parameters.Add("CURRENT_BOF_RATE_IN", "").Direction = ParameterDirection.Input
        //                Else

        //                    .Parameters.Add("CURRENT_BOF_RATE_IN", _
        //                    CDbl(dtTransaction.Rows(nTransactionRowCnt).Item("CURRENT_BOF_RATE"))).Direction = _
        //                    ParameterDirection.Input
        //                End If

        //                If IsDBNull(dtTransaction.Rows(nTransactionRowCnt).Item("CURRENT_ALL_IN_RATE")) Then
        //                    .Parameters.Add("CURRENT_ALL_IN_RATE_IN", "").Direction = ParameterDirection.Input
        //                Else
        //                    .Parameters.Add("CURRENT_ALL_IN_RATE_IN", _
        //                    CDbl(dtTransaction.Rows(nTransactionRowCnt).Item("CURRENT_ALL_IN_RATE"))).Direction = _
        //                    ParameterDirection.Input
        //                End If

        //                .Parameters.Add("VALID_FROM_IN", _
        //                CStr(dtTransaction.Rows(nTransactionRowCnt).Item("VALID_FROM"))).Direction = _
        //                ParameterDirection.Input

        //                If IsDBNull(dtTransaction.Rows(nTransactionRowCnt).Item("VALID_TO")) Then
        //                    .Parameters.Add("VALID_TO_IN", "").Direction = _
        //                    ParameterDirection.Input
        //                Else
        //                    .Parameters.Add("VALID_TO_IN", _
        //                    CStr(dtTransaction.Rows(nTransactionRowCnt).Item("VALID_TO"))).Direction = _
        //                    ParameterDirection.Input
        //                End If

        //                If IsDBNull(dtTransaction.Rows(nTransactionRowCnt).Item("REQUESTED_BOF_RATE")) Then
        //                    .Parameters.Add("REQUESTED_BOF_RATE_IN", "").Direction = _
        //                    ParameterDirection.Input
        //                Else
        //                    .Parameters.Add("REQUESTED_BOF_RATE_IN", _
        //                    CDbl( _
        //                          dtTransaction.Rows(nTransactionRowCnt).Item("REQUESTED_BOF_RATE") _
        //                        )).Direction = _
        //                    ParameterDirection.Input
        //                End If

        //                If IsDBNull(dtTransaction.Rows(nTransactionRowCnt).Item("REQUESTED_ALL_IN_RATE")) Then
        //                    .Parameters.Add("REQUESTED_ALL_IN_RATE_IN", "").Direction = _
        //                    ParameterDirection.Input
        //                Else
        //                    .Parameters.Add("REQUESTED_ALL_IN_RATE_IN", _
        //                    CDbl( _
        //                          dtTransaction.Rows(nTransactionRowCnt).Item("REQUESTED_ALL_IN_RATE") _
        //                        )).Direction = _
        //                    ParameterDirection.Input
        //                End If

        //                If IsDBNull(dtTransaction.Rows(nTransactionRowCnt).Item("APPROVED_ALL_IN_RATE")) Then
        //                    .Parameters.Add("APPROVED_ALL_IN_RATE_IN", "").Direction = _
        //                    ParameterDirection.Input
        //                Else
        //                    .Parameters.Add("APPROVED_ALL_IN_RATE_IN", _
        //                    CDbl( _
        //                          dtTransaction.Rows(nTransactionRowCnt).Item("APPROVED_ALL_IN_RATE") _
        //                        )).Direction = _
        //                    ParameterDirection.Input
        //                End If

        //                If IsDBNull(dtTransaction.Rows(nTransactionRowCnt).Item("APPROVED_BOF_RATE")) Then
        //                    .Parameters.Add("APPROVED_BOF_RATE_IN", "").Direction = _
        //                    ParameterDirection.Input
        //                Else
        //                    .Parameters.Add("APPROVED_BOF_RATE_IN", _
        //                    CDbl( _
        //                          dtTransaction.Rows(nTransactionRowCnt).Item("APPROVED_BOF_RATE") _
        //                        )).Direction = _
        //                    ParameterDirection.Input
        //                End If

        //                .Parameters.Add("ON_THL_OR_THD_IN", _
        //                CInt(dtTransaction.Rows(nTransactionRowCnt).Item("ON_THL_OR_THD"))).Direction = _
        //                ParameterDirection.Input

        //                If IsDBNull(dtTransaction.Rows(nTransactionRowCnt).Item("EXPECTED_VOLUME")) Then
        //                    .Parameters.Add("EXPECTED_VOLUME_IN", "").Direction = ParameterDirection.Input
        //                Else
        //                    .Parameters.Add("EXPECTED_VOLUME_IN", _
        //                    CDbl(dtTransaction.Rows(nTransactionRowCnt).Item("EXPECTED_VOLUME"))).Direction = _
        //                    ParameterDirection.Input
        //                End If

        //                If IsDBNull(dtTransaction.Rows(nTransactionRowCnt).Item("EXPECTED_BOXES")) Then
        //                    .Parameters.Add("EXPECTED_BOXES_IN", "").Direction = ParameterDirection.Input
        //                Else
        //                    .Parameters.Add("EXPECTED_BOXES_IN", _
        //                    CLng(dtTransaction.Rows(nTransactionRowCnt).Item("EXPECTED_BOXES"))).Direction = _
        //                    ParameterDirection.Input
        //                End If

        //                .Parameters.Add("SUBJECT_TO_SURCHRG_CHG_IN", _
        //                CInt(dtTransaction.Rows(nTransactionRowCnt).Item("SUBJECT_TO_SURCHG_CHG"))).Direction = _
        //                ParameterDirection.Input

        //                .Parameters.Add("OPERATOR_SPEC_SURCHRG_IN", _
        //                CInt(dtTransaction.Rows(nTransactionRowCnt).Item("OPERATOR_SPEC_SURCRG"))).Direction = _
        //                ParameterDirection.Input

        //                .Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = _
        //                ParameterDirection.Output

        //                .ExecuteNonQuery()

        //                nTransactionPk = CLng(.Parameters["RETURN_VALUE"].Value)
        //                arrMessage = SaveSrrSurcharge( _
        //                            dsMain, _
        //                            objWK, _
        //                            nTransactionPk, _
        //                            CLng(dtTransaction.Rows(nTransactionRowCnt).Item("PORT_MST_POL_FK")), _
        //                            CLng(dtTransaction.Rows(nTransactionRowCnt).Item("PORT_MST_POD_FK")), _
        //                            CLng(dtTransaction.Rows(nTransactionRowCnt).Item(Cont_BasisPk)) _
        //                            )
        //                If Not (InStr(arrMessage(0), "saved") > 0) Then
        //                    Return arrMessage
        //                End If
        //            End With
        //        Next

        //        arrMessage.Add("All data saved successfully")
        //        Return arrMessage

        //    Catch oraexp As OracleException
        //        arrMessage.Add(oraexp.Message)
        //        Return arrMessage
        //    Catch ex As Exception
        //        arrMessage.Add(ex.Message)
        //        Return arrMessage
        //    End Try
        //End Function

        //Private Function SaveSrrSurcharge( _
        //                             ByRef dsMain As DataSet, _
        //                             ByRef objWK As WorkFlow, _
        //                             ByVal TransactionPkValue As Long, _
        //                             ByVal PolPk As Long, _
        //                             ByVal PodPk As Long, _
        //                             ByVal CONT_BASIS As Long _
        //                             ) As ArrayList
        //    Dim nSurchargeRowCnt As Int32
        //    Dim dv_Surcharge As New DataView

        //    dv_Surcharge = getDataView(dsMain.Tables("Surcharge"), _
        //                               PolPk, _
        //                               PodPk, _
        //                               CONT_BASIS _
        //                               )

        //    arrMessage.Clear()

        //    Try
        //        For nSurchargeRowCnt = 0 To dv_Surcharge.Table.Rows.Count - 1
        //            objWK.MyCommand.Parameters.Clear()
        //            With objWK.MyCommand
        //                .CommandType = CommandType.StoredProcedure
        //                .CommandText = objWK.MyUserName & ".CONT_CUST_SEA_TBL_PKG.CONT_SUR_CHRG_SEA_TBL_INS"

        //                .Parameters.Add("CONT_CUST_TRN_SEA_FK_IN", TransactionPkValue).Direction = _
        //                ParameterDirection.Input

        //                .Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", _
        //                CLng( _
        //                dv_Surcharge.Table.Rows(nSurchargeRowCnt).Item("FREIGHT_ELEMENT_MST_FK"))).Direction = _
        //                ParameterDirection.Input

        //                If IsDBNull(dv_Surcharge.Table.Rows(nSurchargeRowCnt).Item("CURR_SURCHARGE_AMT")) Then
        //                    .Parameters.Add("CURR_SURCHARGE_AMT_IN", "").Direction = ParameterDirection.Input
        //                Else
        //                    .Parameters.Add("CURR_SURCHARGE_AMT_IN", _
        //                    CDbl(dv_Surcharge.Table.Rows(nSurchargeRowCnt).Item("CURR_SURCHARGE_AMT"))).Direction = _
        //                    ParameterDirection.Input
        //                End If

        //                If IsDBNull(dv_Surcharge.Table.Rows(nSurchargeRowCnt).Item("REQ_SURCHARGE_AMT")) Then
        //                    .Parameters.Add("REQ_SURCHARGE_AMT_IN", "").Direction = ParameterDirection.Input
        //                Else
        //                    .Parameters.Add("REQ_SURCHARGE_AMT_IN", _
        //                    CDbl(dv_Surcharge.Table.Rows(nSurchargeRowCnt).Item("REQ_SURCHARGE_AMT"))).Direction = _
        //                    ParameterDirection.Input
        //                End If

        //                .Parameters.Add("APP_SURCHARGE_AMT_IN", _
        //                CDbl(dv_Surcharge.Table.Rows(nSurchargeRowCnt).Item("APP_SURCHARGE_AMT"))).Direction = _
        //                ParameterDirection.Input

        //                .Parameters.Add("CHECK_FOR_ALL_IN_RT_IN", _
        //                CLng(dv_Surcharge.Table.Rows(nSurchargeRowCnt).Item("CHECK_FOR_ALL_IN_RT"))).Direction = _
        //                ParameterDirection.Input

        //                .Parameters.Add("CURRENCY_MST_FK_IN", _
        //                CLng(dv_Surcharge.Table.Rows(nSurchargeRowCnt).Item("CURRENCY_MST_FK"))).Direction = _
        //                ParameterDirection.Input

        //                .Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, _
        //                "RETURN_VALUE").Direction = _
        //                ParameterDirection.Output

        //                .ExecuteNonQuery()
        //            End With
        //        Next

        //        arrMessage.Add("All data saved successfully")
        //        Return arrMessage

        //    Catch oraexp As OracleException
        //        arrMessage.Add(oraexp.Message)
        //        Return arrMessage
        //    Catch ex As Exception
        //        arrMessage.Add(ex.Message)
        //        Return arrMessage
        //    End Try
        //End Function
        //FUNCTIONS FOR UPDATING THE SRR AND APPROVING THE SRR
        public ArrayList SaveHDR(DataSet dsMain, long nLocationId, long nEmpId, bool IsLcl, int status, string Remarks = "", Int16 Restricted = 0)
        {
            string ContractRefNo = null;
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            arrMessage.Clear();
            objWK.MyCommand.Transaction = TRAN;

            try
            {
                //ContractRefNo = GenerateContractNo(nLocationId, nEmpId, M_CREATED_BY_FK, objWK)
                //If InStr(ContractRefNo, "Protocol") > 0 Then
                //    arrMessage.Add("Protocol Not Defined.")
                //    Return arrMessage
                //End If
                objWK.MyCommand.Parameters.Clear();
                var _with14 = objWK.MyCommand;
                _with14.CommandType = CommandType.StoredProcedure;
                _with14.CommandText = objWK.MyUserName + ".SRR_SEA_TBL_PKG.SRR_SEA_TBL_UPD";
                //SRR_SEA_PK_IN           IN SRR_SEA_TBL.SRR_SEA_PK%TYPE,
                _with14.Parameters.Add("SRR_SEA_PK_IN", Convert.ToInt64(dsMain.Tables["Master"].Rows[0]["SRR_SEA_FK"])).Direction = ParameterDirection.Input;
                //COMMODITY_MST_FK_IN     IN SRR_SEA_TBL.COMMODITY_MST_FK%TYPE,
                if (string.IsNullOrEmpty(dsMain.Tables["Master"].Rows[0]["COMMODITY_MST_FK"].ToString()))
                {
                    _with14.Parameters.Add("COMMODITY_MST_FK_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with14.Parameters.Add("COMMODITY_MST_FK_IN", Convert.ToInt64(dsMain.Tables["Master"].Rows[0]["COMMODITY_MST_FK"])).Direction = ParameterDirection.Input;
                }
                //COMMODITY_GROUP_FK_IN   IN SRR_SEA_TBL.COMMODITY_GROUP_MST_FK%TYPE,
                _with14.Parameters.Add("COMMODITY_GROUP_FK_IN", Convert.ToInt64(dsMain.Tables["Master"].Rows[0]["COMMODITY_GROUP_MST_FK"])).Direction = ParameterDirection.Input;
                //SRR_CLAUSE_IN           IN SRR_SEA_TBL.SRR_CLAUSE%TYPE,
                if (string.IsNullOrEmpty(dsMain.Tables["Master"].Rows[0]["CONT_CLAUSE"].ToString()))
                {
                    _with14.Parameters.Add("SRR_CLAUSE_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with14.Parameters.Add("SRR_CLAUSE_IN", Convert.ToString(dsMain.Tables["Master"].Rows[0]["CONT_CLAUSE"])).Direction = ParameterDirection.Input;
                }
                if (string.IsNullOrEmpty(dsMain.Tables["Master"].Rows[0]["SRR_REMARKS"].ToString()))
                {
                    _with14.Parameters.Add("SRR_REMARKS_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with14.Parameters.Add("SRR_REMARKS_IN", Convert.ToString(dsMain.Tables["Master"].Rows[0]["SRR_REMARKS"])).Direction = ParameterDirection.Input;
                }

                //collection address
                if (string.IsNullOrEmpty(dsMain.Tables["Master"].Rows[0]["COLLECTION_ADDRESS"].ToString()))
                {
                    _with14.Parameters.Add("SRR_COLLECTION_ADDRESS_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with14.Parameters.Add("SRR_COLLECTION_ADDRESS_IN", Convert.ToString(dsMain.Tables["Master"].Rows[0]["COLLECTION_ADDRESS"])).Direction = ParameterDirection.Input;
                }

                //PYMT_LOCATION_MST_FK_IN IN SRR_SEA_TBL.PYMT_LOCATION_MST_FK%TYPE,
                if (string.IsNullOrEmpty(dsMain.Tables["Master"].Rows[0]["PYMT_LOCATION_MST_FK"].ToString()))
                {
                    _with14.Parameters.Add("PYMT_LOCATION_MST_FK_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with14.Parameters.Add("PYMT_LOCATION_MST_FK_IN", Convert.ToInt64(dsMain.Tables["Master"].Rows[0]["PYMT_LOCATION_MST_FK"])).Direction = ParameterDirection.Input;
                }
                //CREDIT_PERIOD_IN        IN SRR_SEA_TBL.CREDIT_PERIOD%TYPE,
                if (string.IsNullOrEmpty(dsMain.Tables["Master"].Rows[0]["CREDIT_PERIOD"].ToString()))
                {
                    _with14.Parameters.Add("CREDIT_PERIOD_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with14.Parameters.Add("CREDIT_PERIOD_IN", Convert.ToInt64(dsMain.Tables["Master"].Rows[0]["CREDIT_PERIOD"])).Direction = ParameterDirection.Input;
                }
                //OPERATOR_MST_FK_IN      IN SRR_SEA_TBL.OPERATOR_MST_FK%TYPE,
                if (string.IsNullOrEmpty(dsMain.Tables["Master"].Rows[0]["OPERATOR_MST_FK"].ToString()) | Convert.ToInt32(dsMain.Tables["Master"].Rows[0]["OPERATOR_MST_FK"]) == 0)
                {
                    _with14.Parameters.Add("OPERATOR_MST_FK_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with14.Parameters.Add("OPERATOR_MST_FK_IN", Convert.ToInt64(dsMain.Tables["Master"].Rows[0]["OPERATOR_MST_FK"])).Direction = ParameterDirection.Input;
                }
                //CUSTOMER_MST_FK_IN      IN SRR_SEA_TBL.CUSTOMER_MST_FK%TYPE,
                _with14.Parameters.Add("CUSTOMER_MST_FK_IN", Convert.ToInt64(dsMain.Tables["Master"].Rows[0]["CUSTOMER_MST_FK"])).Direction = ParameterDirection.Input;

                //VERSION_NO_IN           IN SRR_SEA_TBL.VERSION_NO%TYPE,
                _with14.Parameters.Add("VERSION_NO_IN", Convert.ToInt64(dsMain.Tables["Master"].Rows[0]["VERSION_NO"])).Direction = ParameterDirection.Input;
                // .Parameters.Add("CONT_REF_NO_IN", ContractRefNo).Direction = ParameterDirection.Input
                //CARGO_TYPE_IN           IN SRR_SEA_TBL.CARGO_TYPE%TYPE,
                _with14.Parameters.Add("CARGO_TYPE_IN", Convert.ToInt64(dsMain.Tables["Master"].Rows[0]["CARGO_TYPE"])).Direction = ParameterDirection.Input;
                //VALID_FROM_IN           IN VARCHAR2,
                _with14.Parameters.Add("VALID_FROM_IN", Convert.ToString(dsMain.Tables["Master"].Rows[0]["VALID_FROM"])).Direction = ParameterDirection.Input;
                //VALID_TO_IN             IN VARCHAR2,
                _with14.Parameters.Add("VALID_TO_IN", Convert.ToString(dsMain.Tables["Master"].Rows[0]["VALID_TO"])).Direction = ParameterDirection.Input;
                //STRCONDITION            IN VARCHAR2,
                _with14.Parameters.Add("STRCONDITION", Convert.ToString(dsMain.Tables["Master"].Rows[0]["STRCONDITION"])).Direction = ParameterDirection.Input;

                //CONFIG_PK_IN            IN CONFIG_MST_TBL.CONFIG_MST_PK%TYPE,
                _with14.Parameters.Add("CONFIG_PK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                //LAST_MODIFIED_BY_FK_IN  IN SRR_SEA_TBL.LAST_MODIFIED_BY_FK%TYPE,
                _with14.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                //STATUS_IN               IN SRR_SEA_TBL.STATUS%TYPE,
                //.Parameters.Add("STATUS_IN", 1).Direction = ParameterDirection.Input
                _with14.Parameters.Add("STATUS_IN", Convert.ToInt64(dsMain.Tables["Master"].Rows[0]["STATUS"])).Direction = ParameterDirection.Input;
                //Added by Manoj K Sethi for Saving Active checkbox
                _with14.Parameters.Add("ACTIVE_IN", Convert.ToInt32(dsMain.Tables["Master"].Rows[0]["ACTIVE"])).Direction = ParameterDirection.Input;
                //end
                _with14.Parameters.Add("RESTRICTED_IN", Restricted).Direction = ParameterDirection.Input;

                _with14.Parameters.Add("PORT_GROUP_IN", Convert.ToInt32(dsMain.Tables["Master"].Rows[0]["PORT_GROUP"])).Direction = ParameterDirection.Input;

                _with14.Parameters.Add("SRR_TYPE_IN", Convert.ToInt32(dsMain.Tables["Master"].Rows[0]["SRR_TYPE"])).Direction = ParameterDirection.Input;
                _with14.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;

                _with14.ExecuteNonQuery();

                if (string.Compare(objWK.MyCommand.Parameters["RETURN_VALUE"].Value.ToString(), "Contract") > 0 | string.Compare(objWK.MyCommand.Parameters["RETURN_VALUE"].Value.ToString(), "MODIFIED") > 0)
                {
                    arrMessage.Add(Convert.ToString(objWK.MyCommand.Parameters["RETURN_VALUE"].Value));
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    _PkValue = Convert.ToInt64(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);
                }

                arrMessage = SaveSrrTRN(dsMain, objWK, IsLcl);

                if (arrMessage.Count > 0)
                {
                    if (string.Compare(arrMessage[0].ToString(), "saved") > 0)
                    {
                        arrMessage.Add("All data saved successfully");
                        TRAN.Commit();
                        // _CustomerContractNo = ContractRefNo
                        return arrMessage;
                    }
                    else
                    {
                        TRAN.Rollback();
                        return arrMessage;
                    }
                }
                //Manjunath  PTS ID:Sep-02  17/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                TRAN.Rollback();
                throw ex;
            }
            finally
            {
                objWK.MyConnection.Close();
            }
            return new ArrayList();
        }

        private ArrayList SaveSrrTRN(DataSet dsMain, WorkFlow objWK, bool IsLCL)
        {
            Int32 nTransactionRowCnt = default(Int32);
            DataTable dtTransaction = new DataTable();
            long nTransactionPk = 0;
            dtTransaction = dsMain.Tables["Transaction"];
            bool IsUpdate = true;
            string Cont_BasisPk = null;
            if (IsLCL)
            {
                Cont_BasisPk = "LCL_BASIS";
            }
            else
            {
                Cont_BasisPk = "CONTAINER_TYPE_MST_FK";
            }
            try
            {
                for (nTransactionRowCnt = 0; nTransactionRowCnt <= dsMain.Tables["Transaction"].Rows.Count - 1; nTransactionRowCnt++)
                {
                    objWK.MyCommand.Parameters.Clear();
                    arrMessage.Clear();
                    nTransactionPk = 0;

                    var _with15 = objWK.MyCommand;

                    //**********************************UPDATE********************************************
                    //************************************************************************************
                    IsUpdate = true;

                    _with15.CommandType = CommandType.StoredProcedure;
                    _with15.CommandText = objWK.MyUserName + ".SRR_SEA_TBL_PKG.SRR_TRN_SEA_TBL_UPD";

                    _with15.Parameters.Add("SRR_TRN_SEA_PK_IN", Convert.ToInt64(dtTransaction.Rows[nTransactionRowCnt]["SRR_TRN_PK"])).Direction = ParameterDirection.Input;

                    //********************************COMMON TO INSERT AND UPDATE************************************
                    //***********************************************************************************************
                    _with15.Parameters.Add("VALID_FROM_IN", Convert.ToString(dtTransaction.Rows[nTransactionRowCnt]["VALID_FROM"])).Direction = ParameterDirection.Input;

                    if (string.IsNullOrEmpty(dtTransaction.Rows[nTransactionRowCnt]["VALID_TO"].ToString()))
                    {
                        _with15.Parameters.Add("VALID_TO_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with15.Parameters.Add("VALID_TO_IN", Convert.ToString(dtTransaction.Rows[nTransactionRowCnt]["VALID_TO"])).Direction = ParameterDirection.Input;
                    }

                    if (string.IsNullOrEmpty(dtTransaction.Rows[nTransactionRowCnt]["REQUESTED_BOF_RATE"].ToString()))
                    {
                        _with15.Parameters.Add("REQUESTED_BOF_RATE_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with15.Parameters.Add("REQUESTED_BOF_RATE_IN", Convert.ToDouble(dtTransaction.Rows[nTransactionRowCnt]["REQUESTED_BOF_RATE"])).Direction = ParameterDirection.Input;
                    }

                    if (string.IsNullOrEmpty(dtTransaction.Rows[nTransactionRowCnt]["REQUESTED_ALL_IN_RATE"].ToString()))
                    {
                        _with15.Parameters.Add("REQUESTED_ALL_IN_RATE_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with15.Parameters.Add("REQUESTED_ALL_IN_RATE_IN", Convert.ToDouble(dtTransaction.Rows[nTransactionRowCnt]["REQUESTED_ALL_IN_RATE"])).Direction = ParameterDirection.Input;
                    }

                    _with15.Parameters.Add("ON_THL_OR_THD_IN", Convert.ToInt64(dtTransaction.Rows[nTransactionRowCnt]["ON_THL_OR_THD"])).Direction = ParameterDirection.Input;
                    if (string.IsNullOrEmpty(dtTransaction.Rows[nTransactionRowCnt]["APPROVED_ALL_IN_RATE"].ToString()))
                    {
                        _with15.Parameters.Add("APPROVED_ALL_IN_RATE_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with15.Parameters.Add("APPROVED_ALL_IN_RATE_IN", Convert.ToDouble(dtTransaction.Rows[nTransactionRowCnt]["APPROVED_ALL_IN_RATE"])).Direction = ParameterDirection.Input;
                    }

                    if (string.IsNullOrEmpty(dtTransaction.Rows[nTransactionRowCnt]["APPROVED_BOF_RATE"].ToString()))
                    {
                        _with15.Parameters.Add("APPROVED_BOF_RATE_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with15.Parameters.Add("APPROVED_BOF_RATE_IN", Convert.ToDouble(dtTransaction.Rows[nTransactionRowCnt]["APPROVED_BOF_RATE"])).Direction = ParameterDirection.Input;
                    }

                    if (string.IsNullOrEmpty(dtTransaction.Rows[nTransactionRowCnt]["EXPECTED_VOLUME"].ToString()))
                    {
                        _with15.Parameters.Add("EXPECTED_VOLUME_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with15.Parameters.Add("EXPECTED_VOLUME_IN", Convert.ToDouble(dtTransaction.Rows[nTransactionRowCnt]["EXPECTED_VOLUME"])).Direction = ParameterDirection.Input;
                    }

                    if (string.IsNullOrEmpty(dtTransaction.Rows[nTransactionRowCnt]["EXPECTED_BOXES"].ToString()))
                    {
                        _with15.Parameters.Add("EXPECTED_BOXES_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with15.Parameters.Add("EXPECTED_BOXES_IN", Convert.ToInt64(dtTransaction.Rows[nTransactionRowCnt]["EXPECTED_BOXES"])).Direction = ParameterDirection.Input;
                    }

                    _with15.Parameters.Add("SUBJECT_TO_SURCHG_CHG_IN", Convert.ToInt64(dtTransaction.Rows[nTransactionRowCnt]["SUBJECT_TO_SURCHG_CHG"])).Direction = ParameterDirection.Input;

                    _with15.Parameters.Add("OPERATOR_SPEC_SURCRG_IN", Convert.ToInt64(dtTransaction.Rows[nTransactionRowCnt]["OPERATOR_SPEC_SURCRG"])).Direction = ParameterDirection.Input;

                    _with15.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;

                    _with15.ExecuteNonQuery();

                    nTransactionPk = Convert.ToInt64(_with15.Parameters["RETURN_VALUE"].Value);
                    arrMessage = SaveSrrSurcharge(dsMain, objWK, nTransactionPk, IsUpdate, Convert.ToInt64(dtTransaction.Rows[nTransactionRowCnt]["PORT_MST_POL_FK"]), Convert.ToInt64(dtTransaction.Rows[nTransactionRowCnt]["PORT_MST_POD_FK"]), Convert.ToInt64(dtTransaction.Rows[nTransactionRowCnt][Cont_BasisPk]));

                    if (!(string.Compare(arrMessage[0].ToString(), "saved") > 0))
                    {
                        return arrMessage;
                    }
                }

                arrMessage.Add("All data saved successfully");
                return arrMessage;
            }
            catch (OracleException oraexp)
            {
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
        }

        private ArrayList SaveSrrSurcharge(DataSet dsMain, WorkFlow objWK, long TransactionPkValue, bool IsUpdate, long PolPk, long PodPk, long CONT_BASIS)
        {
            Int32 nSurchargeRowCnt = default(Int32);
            DataView dv_Surcharge = new DataView();

            dv_Surcharge = getDataView(dsMain.Tables["Surcharge"], PolPk, PodPk, CONT_BASIS);

            arrMessage.Clear();

            try
            {
                for (nSurchargeRowCnt = 0; nSurchargeRowCnt <= dv_Surcharge.Table.Rows.Count - 1; nSurchargeRowCnt++)
                {
                    objWK.MyCommand.Parameters.Clear();
                    var _with16 = objWK.MyCommand;

                    //**********************************UPDATE********************************************
                    _with16.CommandType = CommandType.StoredProcedure;
                    _with16.CommandText = objWK.MyUserName + ".SRR_SEA_TBL_PKG.SRR_SUR_CHRG_SEA_TBL_UPD";

                    _with16.Parameters.Add("SRR_SUR_CHRG_SEA_PK_IN", Convert.ToDouble(dv_Surcharge.Table.Rows[nSurchargeRowCnt]["SRR_SUR_CHRG_SEA_PK"])).Direction = ParameterDirection.Input;
                    if (string.IsNullOrEmpty(dv_Surcharge.Table.Rows[nSurchargeRowCnt]["REQ_SURCHARGE_AMT"].ToString()))
                    {
                        _with16.Parameters.Add("REQ_SURCHARGE_AMT_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with16.Parameters.Add("REQ_SURCHARGE_AMT_IN", Convert.ToDouble(dv_Surcharge.Table.Rows[nSurchargeRowCnt]["REQ_SURCHARGE_AMT"])).Direction = ParameterDirection.Input;
                    }

                    _with16.Parameters.Add("CHECK_FOR_ALL_IN_RT_IN", Convert.ToInt64(dv_Surcharge.Table.Rows[nSurchargeRowCnt]["CHECK_FOR_ALL_IN_RT"])).Direction = ParameterDirection.Input;
                    if (string.IsNullOrEmpty(dv_Surcharge.Table.Rows[nSurchargeRowCnt]["CURRENCY_MST_FK"].ToString()))
                    {
                        _with16.Parameters.Add("CURRENCY_MST_FK_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with16.Parameters.Add("CURRENCY_MST_FK_IN", Convert.ToInt64(dv_Surcharge.Table.Rows[nSurchargeRowCnt]["CURRENCY_MST_FK"])).Direction = ParameterDirection.Input;
                    }

                    if (string.IsNullOrEmpty(dv_Surcharge.Table.Rows[nSurchargeRowCnt]["APP_SURCHARGE_AMT"].ToString()))
                    {
                        _with16.Parameters.Add("APP_SURCHARGE_AMT_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with16.Parameters.Add("APP_SURCHARGE_AMT_IN", Convert.ToDouble(dv_Surcharge.Table.Rows[nSurchargeRowCnt]["APP_SURCHARGE_AMT"])).Direction = ParameterDirection.Input;
                    }
                    if (!string.IsNullOrEmpty(dv_Surcharge.Table.Rows[nSurchargeRowCnt]["SURCHARGE"].ToString()))
                    {
                        _with16.Parameters.Add("SURCHARGE_IN", dv_Surcharge.Table.Rows[nSurchargeRowCnt]["SURCHARGE"]).Direction = ParameterDirection.Input;
                        //'added by subhransu for surcharge
                    }
                    else
                    {
                        _with16.Parameters.Add("SURCHARGE_IN", "").Direction = ParameterDirection.Input;
                        //'added by subhransu for surcharge
                    }

                    if (!string.IsNullOrEmpty(dv_Surcharge.Table.Rows[nSurchargeRowCnt]["CONTARCTRATE"].ToString()))
                    {
                        _with16.Parameters.Add("SL_CONTARCT_RATE_IN", Convert.ToInt32(dv_Surcharge.Table.Rows[nSurchargeRowCnt]["CONTARCTRATE"])).Direction = ParameterDirection.Input;
                        //'
                    }
                    else
                    {
                        _with16.Parameters.Add("SL_CONTARCT_RATE_IN", "").Direction = ParameterDirection.Input;
                        //'
                    }

                    _with16.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;

                    _with16.ExecuteNonQuery();
                }

                arrMessage.Add("All data saved successfully");
                return arrMessage;
            }
            catch (OracleException oraexp)
            {
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
        }

        //This function generates the Customer Referrence no. as per the protocol saved by the user.
        public string GenerateContractNo(long nLocationId, long nEmployeeId, long nCreatedBy, WorkFlow objWK)
        {
            string functionReturnValue = null;
            try
            {
                functionReturnValue = GenerateProtocolKey("CUSTOMER CONTRACT OPERATOR", nLocationId, nEmployeeId, DateTime.Now, "","" ,"" , nCreatedBy, objWK);
                return functionReturnValue;
                //Manjunath  PTS ID:Sep-02  17/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return functionReturnValue;
        }

        private DataView getDataView(DataTable dtSurcharge, long POLPK, long PODPK, long CONT_BASIS)
        {
            DataTable dstemp = new DataTable();
            DataRow dr = null;
            Int32 nRowCnt = default(Int32);
            Int32 nColCnt = default(Int32);
            string Cont_BasisPk = null;
            try
            {
                dstemp = dtSurcharge.Clone();
                for (nRowCnt = 0; nRowCnt <= dtSurcharge.Rows.Count - 1; nRowCnt++)
                {
                    if (POLPK == Convert.ToInt64(dtSurcharge.Rows[nRowCnt]["PORT_MST_POL_FK"]) & PODPK == Convert.ToInt64(dtSurcharge.Rows[nRowCnt]["PORT_MST_POD_FK"]) & CONT_BASIS == Convert.ToInt64(dtSurcharge.Rows[nRowCnt]["CONT_BASIS"]))
                    {
                        dr = dstemp.NewRow();
                        for (nColCnt = 0; nColCnt <= dtSurcharge.Columns.Count - 1; nColCnt++)
                        {
                            dr[nColCnt] = dtSurcharge.Rows[nRowCnt][nColCnt];
                        }
                        dstemp.Rows.Add(dr);
                    }
                }
                return dstemp.DefaultView;
                //Manjunath  PTS ID:Sep-02  17/09/2011
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

        #endregion "Save"

        #region "Fetch Header Details"

        public DataSet FetchHeader(int SRRSeaPk)
        {
            WorkFlow ObjWk = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT CMT.CUSTOMER_NAME,");
            sb.Append("       CCD.ADM_ADDRESS_1,");
            sb.Append("       CCD.ADM_ADDRESS_2,");
            sb.Append("       CCD.ADM_ADDRESS_3,");
            sb.Append("       CCD.ADM_ZIP_CODE,");
            sb.Append("       CMTS.COUNTRY_NAME,");
            sb.Append("       CCST.SRR_REF_NO,");
            sb.Append("       CASE WHEN CCST.SRR_TYPE=0 THEN CCS.CONT_REF_NO ELSE TARIFF.Tariff_Ref_No END AS CONT_REF_NO,");
            sb.Append("       CCST.SRR_DATE,");
            sb.Append("       'SEA' BIZ_TYPE,");
            sb.Append("       OMT.OPERATOR_NAME,");
            sb.Append("       CGMT.COMMODITY_GROUP_DESC,");
            sb.Append("       DECODE(CCST.CARGO_TYPE,1,'FCL',2,'LCL') CARGO_TYPE,");
            sb.Append("       CCST.VALID_FROM,");
            sb.Append("       CCST.VALID_TO,");
            sb.Append("       CCST.CREDIT_PERIOD,");
            sb.Append("       DECODE(CCST.STATUS,1,'Internal Approval',2,'Customer Approval')STATUS,");
            sb.Append("       DECODE(CCST.STATUS,0,CUMT.USER_ID,1,LUMT.USER_ID,2,LUMT.USER_ID) USER_ID, ");
            sb.Append("        CCST.SRR_CLAUSE,");
            sb.Append("       CCST.COL_ADDRESS,");
            sb.Append("        LMT.LOCATION_NAME,    ");
            sb.Append("        LUMT.USER_NAME APPD_BY,     ");
            sb.Append("       CCST.LAST_MODIFIED_DT  APP_DT               ");
            sb.Append("  FROM SRR_SEA_TBL     CCST,");
            sb.Append("       CONT_CUST_SEA_TBL CCS,TARIFF_MAIN_SEA_TBL TARIFF,");
            sb.Append("       CUSTOMER_MST_TBL      CMT,");
            sb.Append("       CUSTOMER_CONTACT_DTLS CCD,");
            sb.Append("       LOCATION_MST_TBL      LMT,");
            sb.Append("       COUNTRY_MST_TBL       CMTS,");
            sb.Append("       OPERATOR_MST_TBL      OMT,");
            sb.Append("       COMMODITY_MST_TBL     CT,");
            sb.Append("       COMMODITY_GROUP_MST_TBL CGMT,");
            sb.Append("       USER_MST_TBL            CUMT,");
            sb.Append("       USER_MST_TBL            LUMT");
            sb.Append(" WHERE CCST.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
            sb.Append("   AND CCST.OPERATOR_MST_FK = OMT.OPERATOR_MST_PK(+)");
            sb.Append("   AND CCST.TARIFF_MAIN_SEA_FK = CCS.CONT_CUST_SEA_PK(+) AND CCST.TARIFF_MAIN_SEA_FK = TARIFF.TARIFF_MAIN_SEA_PK(+)");
            sb.Append("   AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
            sb.Append("   AND CCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
            sb.Append("   AND LMT.COUNTRY_MST_FK = CMTS.COUNTRY_MST_PK");
            sb.Append("   AND CCST.COMMODITY_MST_FK = CT.COMMODITY_MST_PK(+)");
            sb.Append("   AND CCST.COMMODITY_GROUP_MST_FK = CGMT.COMMODITY_GROUP_PK");
            sb.Append("   AND CCST.CREATED_BY_FK = CUMT.USER_MST_PK");
            sb.Append("   AND CCST.LAST_MODIFIED_BY_FK = LUMT.USER_MST_PK(+)");
            sb.Append("   AND CCST.SRR_SEA_PK = " + SRRSeaPk);
            try
            {
                return ObjWk.GetDataSet(sb.ToString());
                //Manjunath  PTS ID:Sep-02  17/09/2011
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

        #endregion "Fetch Header Details"

        #region "Fetch FreightDetails"

        public DataSet FetchFreightDetails(long SRRSeaContractPk)
        {
            WorkFlow objWK = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT POL.PORT_ID POL,");
            sb.Append("       POD.PORT_ID POD,");
            sb.Append("       CASE");
            sb.Append("         WHEN SST.CARGO_TYPE=1 THEN");
            sb.Append("          CTMT.CONTAINER_TYPE_MST_ID");
            sb.Append("         ELSE");
            sb.Append("          DUMT.DIMENTION_ID");
            sb.Append("       END CONTAINER_TYPE_MST_ID,");
            sb.Append("       CTM.CURRENCY_ID,");
            sb.Append("       STST.CURRENT_BOF_RATE,");
            sb.Append("       STST.CURRENT_ALL_IN_RATE,");
            sb.Append("       STST.REQUESTED_BOF_RATE,");
            sb.Append("       STST.REQUESTED_ALL_IN_RATE,");
            sb.Append("       CASE");
            sb.Append("         WHEN STST.EXPECTED_VOLUME IS NULL THEN");
            sb.Append("          STST.EXPECTED_BOXES");
            sb.Append("         ELSE");
            sb.Append("          STST.EXPECTED_VOLUME");
            sb.Append("       END EXPECTED_VOLUME,");
            sb.Append("       STST.VALID_FROM,");
            sb.Append("       STST.VALID_TO,");
            sb.Append("       STST.APPROVED_BOF_RATE,");
            sb.Append("       STST.APPROVED_ALL_IN_RATE,");
            sb.Append("       CTM1.CURRENCY_ID,");
            sb.Append("       SSCS.CURR_SURCHARGE_AMT,");
            sb.Append("       SSCS.REQ_SURCHARGE_AMT,");
            sb.Append("       SSCS.APP_SURCHARGE_AMT,");
            sb.Append("       FEMT.FREIGHT_ELEMENT_ID,");
            sb.Append("       FEMT.FREIGHT_ELEMENT_NAME");
            sb.Append("  FROM SRR_SEA_TBL             SST,");
            sb.Append("       SRR_TRN_SEA_TBL         STST,");
            sb.Append("       SRR_SUR_CHRG_SEA_TBL    SSCS,");
            sb.Append("       CONTAINER_TYPE_MST_TBL  CTMT,");
            sb.Append("       DIMENTION_UNIT_MST_TBL  DUMT,");
            sb.Append("       PORT_MST_TBL            POL,");
            sb.Append("       PORT_MST_TBL            POD,");
            sb.Append("       CURRENCY_TYPE_MST_TBL   CTM,");
            sb.Append("       CURRENCY_TYPE_MST_TBL   CTM1,");
            sb.Append("       FREIGHT_ELEMENT_MST_TBL FEMT");
            sb.Append(" WHERE STST.SRR_SEA_FK = SST.SRR_SEA_PK");
            sb.Append("   AND SSCS.SRR_TRN_SEA_FK = STST.SRR_TRN_SEA_PK");
            sb.Append("   AND STST.PORT_MST_POL_FK = POL.PORT_MST_PK");
            sb.Append("   AND STST.PORT_MST_POD_FK = POD.PORT_MST_PK");
            sb.Append("   AND STST.CURRENCY_MST_FK = CTM.CURRENCY_MST_PK");
            sb.Append("   AND STST.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK(+)");
            sb.Append("   AND STST.LCL_BASIS = DUMT.DIMENTION_UNIT_MST_PK(+)");
            sb.Append("   AND SSCS.FREIGHT_ELEMENT_MST_FK = FEMT.FREIGHT_ELEMENT_MST_PK");
            sb.Append("   AND SSCS.CURRENCY_MST_FK = CTM1.CURRENCY_MST_PK");
            sb.Append("        AND SST.SRR_SEA_PK =" + SRRSeaContractPk);
            try
            {
                return objWK.GetDataSet(sb.ToString());
                //Manjunath  PTS ID:Sep-02  17/09/2011
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

        #endregion "Fetch FreightDetails"

        #region " Port Group "

        public string FetchPrtGroup(int QuotPK)
        {
            try
            {
                string strSQL = null;
                WorkFlow objWF = new WorkFlow();
                strSQL = " SELECT NVL(Q.PORT_GROUP,0) PORT_GROUP FROM SRR_SEA_TBL Q WHERE Q.SRR_SEA_PK = " + QuotPK;
                return objWF.ExecuteScaler(strSQL);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public DataSet FetchFromPortGroup(int QuotPK = 0, int PortGrpPK = 0, string POLPK = "0")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (QuotPK != 0)
                {
                    sb.Append(" SELECT P.PORT_MST_PK, P.PORT_ID, T.POL_GRP_FK FROM SRR_TRN_SEA_TBL T, PORT_MST_TBL P WHERE T.PORT_MST_POL_FK = P.PORT_MST_PK AND T.POL_GRP_FK = " + PortGrpPK);
                    sb.Append(" AND T.SRR_SEA_FK = " + QuotPK);
                }
                else
                {
                    sb.Append(" SELECT P.PORT_MST_PK, P.PORT_ID, PGT.PORT_GRP_MST_FK POL_GRP_FK FROM PORT_MST_TBL P, PORT_GRP_TRN_TBL PGT WHERE P.PORT_MST_PK = PGT.PORT_MST_FK AND PGT.PORT_GRP_MST_FK =" + PortGrpPK);
                    sb.Append(" AND P.BUSINESS_TYPE = 2 AND P.ACTIVE_FLAG = 1 ");
                    if (POLPK != "0")
                    {
                        sb.Append(" AND PGT.PORT_MST_FK IN (" + POLPK + ") ");
                    }
                }
                //sb.Append("SELECT P.PORT_MST_PK, P.PORT_ID FROM PORT_MST_TBL P WHERE P.PORT_GRP_MST_FK=" & Quotationpk)
                //sb.Append("AND P.BUSINESS_TYPE=2 AND P.ACTIVE_FLAG=1")

                return (objWF.GetDataSet(sb.ToString()));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public DataSet FetchToPortGroup(int QuotPK = 0, int PortGrpPK = 0, string PODPK = "0")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (QuotPK != 0)
                {
                    sb.Append(" SELECT P.PORT_MST_PK, P.PORT_ID, T.POD_GRP_FK FROM SRR_TRN_SEA_TBL T, PORT_MST_TBL P WHERE T.PORT_MST_POD_FK = P.PORT_MST_PK AND T.POD_GRP_FK = " + PortGrpPK);
                    sb.Append(" AND T.SRR_SEA_FK = " + QuotPK);
                }
                else
                {
                    sb.Append(" SELECT P.PORT_MST_PK, P.PORT_ID, PGT.PORT_GRP_MST_FK POD_GRP_FK FROM PORT_MST_TBL P, PORT_GRP_TRN_TBL PGT WHERE P.PORT_MST_PK = PGT.PORT_MST_FK AND PGT.PORT_GRP_MST_FK =" + PortGrpPK);
                    sb.Append(" AND P.BUSINESS_TYPE = 2 AND P.ACTIVE_FLAG = 1");
                    if (PODPK != "0")
                    {
                        sb.Append(" AND PGT.PORT_MST_FK IN (" + PODPK + ") ");
                    }
                }
                //sb.Append("SELECT P.PORT_MST_PK, P.PORT_ID FROM PORT_MST_TBL P WHERE P.PORT_GRP_MST_FK=" & Quotationpk)
                //sb.Append("AND P.BUSINESS_TYPE=2 AND P.ACTIVE_FLAG=1")

                return (objWF.GetDataSet(sb.ToString()));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public DataSet FetchTariffGrp(int QuotPK = 0, int PortGrpPK = 0, string TariffPK = "0")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                sb.Append(" SELECT DISTINCT * FROM (");
                sb.Append(" SELECT POL.PORT_MST_PK   POL_PK,");
                sb.Append("       POL.PORT_ID       POL_ID,");
                sb.Append("       POD.PORT_MST_PK   POD_PK,");
                sb.Append("       POD.PORT_ID       POD_ID,");
                sb.Append("       T.POL_GRP_FK      POL_GRP_FK,");
                sb.Append("       T.PORT_MST_POD_FK POD_GRP_FK,");
                sb.Append("       T.TARIFF_GRP_FK   TARIFF_GRP_MST_PK");
                sb.Append("  FROM PORT_MST_TBL POL, PORT_MST_TBL POD, SRR_TRN_SEA_TBL T");
                sb.Append(" WHERE T.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND T.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND T.SRR_SEA_FK =" + QuotPK);
                sb.Append("   UNION");
                sb.Append(" SELECT POL.PORT_MST_PK       POL_PK,");
                sb.Append("       POL.PORT_ID           POL_ID,");
                sb.Append("       POD.PORT_MST_PK       POD_PK,");
                sb.Append("       POD.PORT_ID           POD_ID,");
                sb.Append("       TGM.POL_GRP_MST_FK    POL_GRP_FK,");
                sb.Append("       TGM.POD_GRP_MST_FK    POD_GRP_FK,");
                sb.Append("       TGM.TARIFF_GRP_MST_PK");
                sb.Append("  FROM PORT_MST_TBL       POL,");
                sb.Append("       PORT_MST_TBL       POD,");
                sb.Append("       TARIFF_GRP_TRN_TBL TGT,");
                sb.Append("       TARIFF_GRP_MST_TBL TGM");
                sb.Append(" WHERE TGM.TARIFF_GRP_MST_PK = TGT.TARIFF_GRP_MST_FK");
                sb.Append("   AND POL.PORT_MST_PK = TGT.POL_MST_FK");
                sb.Append("   AND POD.PORT_MST_PK = TGT.POD_MST_FK");
                sb.Append("   AND TGM.TARIFF_GRP_MST_PK =" + TariffPK);
                sb.Append("   AND POL.BUSINESS_TYPE = 2");
                sb.Append("   AND POL.ACTIVE_FLAG = 1");
                sb.Append("   )");

                //'Comeented if we are fetching from tariff screen records not displaying
                //If QuotPK <> 0 Then
                //    sb.Append(" SELECT POL.PORT_MST_PK   POL_PK,")
                //    sb.Append("       POL.PORT_ID       POL_ID,")
                //    sb.Append("       POD.PORT_MST_PK   POD_PK,")
                //    sb.Append("       POD.PORT_ID       POD_ID,")
                //    sb.Append("       T.POL_GRP_FK      POL_GRP_FK,")
                //    sb.Append("       T.PORT_MST_POD_FK POD_GRP_FK,")
                //    sb.Append("       T.TARIFF_GRP_FK   TARIFF_GRP_MST_PK")
                //    sb.Append("  FROM PORT_MST_TBL POL, PORT_MST_TBL POD, QUOTATION_TRN_SEA_FCL_LCL T")
                //    sb.Append(" WHERE T.PORT_MST_POD_FK = POL.PORT_MST_PK")
                //    sb.Append("   AND T.PORT_MST_POD_FK = POD.PORT_MST_PK")
                //    sb.Append("   AND T.QUOTATION_SEA_FK =" & QuotPK)
                //Else
                //    sb.Append(" SELECT POL.PORT_MST_PK       POL_PK,")
                //    sb.Append("       POL.PORT_ID           POL_ID,")
                //    sb.Append("       POD.PORT_MST_PK       POD_PK,")
                //    sb.Append("       POD.PORT_ID           POD_ID,")
                //    sb.Append("       TGM.POL_GRP_MST_FK    POL_GRP_FK,")
                //    sb.Append("       TGM.POD_GRP_MST_FK    POD_GRP_FK,")
                //    sb.Append("       TGM.TARIFF_GRP_MST_PK")
                //    sb.Append("  FROM PORT_MST_TBL       POL,")
                //    sb.Append("       PORT_MST_TBL       POD,")
                //    sb.Append("       TARIFF_GRP_TRN_TBL TGT,")
                //    sb.Append("       TARIFF_GRP_MST_TBL TGM")
                //    sb.Append(" WHERE TGM.TARIFF_GRP_MST_PK = TGT.TARIFF_GRP_MST_FK")
                //    sb.Append("   AND POL.PORT_MST_PK = TGT.POL_MST_FK")
                //    sb.Append("   AND POD.PORT_MST_PK = TGT.POD_MST_FK")
                //    sb.Append("   AND TGM.TARIFF_GRP_MST_PK =" & TariffPK)
                //    sb.Append("   AND POL.BUSINESS_TYPE = 2")
                //    sb.Append("   AND POL.ACTIVE_FLAG = 1")
                //End If

                return (objWF.GetDataSet(sb.ToString()));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public DataSet FetchTariffPODGrp(int QuotPK = 0, int PortGrpPK = 0, string TariffPK = "0")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (QuotPK != 0)
                {
                    sb.Append(" SELECT P.PORT_MST_PK, P.PORT_ID, T.POD_GRP_FK, T.TARIFF_GRP_FK FROM CONT_CUST_TRN_SEA_TBL T, PORT_MST_TBL P WHERE T.PORT_MST_POD_FK = P.PORT_MST_PK AND T.TARIFF_GRP_FK = " + TariffPK);
                    sb.Append(" AND T.CONT_CUST_SEA_FK =" + QuotPK);
                }
                else
                {
                    sb.Append(" SELECT P.PORT_MST_PK,");
                    sb.Append("        P.PORT_ID,");
                    sb.Append("        TGM.POD_GRP_MST_FK POD_GRP_FK,");
                    sb.Append("        TGM.TARIFF_GRP_MST_PK");
                    sb.Append("  FROM PORT_MST_TBL P, TARIFF_GRP_TRN_TBL TGT, TARIFF_GRP_MST_TBL TGM");
                    sb.Append(" WHERE TGM.TARIFF_GRP_MST_PK = TGT.TARIFF_GRP_MST_FK");
                    sb.Append("   AND P.PORT_MST_PK = TGT.POD_MST_FK");
                    sb.Append("   AND TGM.TARIFF_GRP_MST_PK =" + TariffPK);
                    sb.Append("   AND P.BUSINESS_TYPE = 2");
                    sb.Append("   AND P.ACTIVE_FLAG = 1");
                }
                //sb.Append("SELECT P.PORT_MST_PK, P.PORT_ID FROM PORT_MST_TBL P WHERE P.PORT_GRP_MST_FK=" & Quotationpk)
                //sb.Append("AND P.BUSINESS_TYPE=2 AND P.ACTIVE_FLAG=1")

                return (objWF.GetDataSet(sb.ToString()));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public long FetchFreightGridPK(string CCPK, int CCTrnFK, int CCFreightFK)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();

            try
            {
                sb.Append("SELECT CFT.SRR_SUR_CHRG_SEA_PK ");
                sb.Append("  FROM SRR_SUR_CHRG_SEA_TBL CFT,");
                sb.Append("       SRR_SEA_TBL     CMT,");
                sb.Append("       SRR_TRN_SEA_TBL CTT");
                sb.Append(" WHERE CMT.SRR_SEA_PK = CTT.SRR_SEA_FK");
                sb.Append("   AND CTT.SRR_TRN_SEA_PK = CFT.SRR_TRN_SEA_FK");
                sb.Append("   AND CMT.SRR_SEA_PK = " + CCPK);
                sb.Append("   AND CTT.SRR_TRN_SEA_PK = " + CCTrnFK);
                sb.Append("   AND CFT.FREIGHT_ELEMENT_MST_FK = " + CCFreightFK);

                return Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public long FetchTrnGridPK(string CCPK, int CCPOLFK, int CCPODFK)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();

            try
            {
                sb.Append("SELECT CTT.SRR_TRN_SEA_PK");
                sb.Append("  FROM SRR_SEA_TBL CMT, SRR_TRN_SEA_TBL CTT");
                sb.Append(" WHERE CMT.SRR_SEA_PK = CTT.SRR_SEA_FK");
                sb.Append("   AND CMT.SRR_SEA_PK = " + CCPK);
                sb.Append("   AND CTT.PORT_MST_POL_FK = " + CCPOLFK);
                sb.Append("   AND CTT.PORT_MST_POD_FK = " + CCPODFK);

                 return Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion " Port Group "
    }
}