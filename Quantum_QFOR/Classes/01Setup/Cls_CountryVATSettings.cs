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

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class Cls_CountryVATSettings : CommonFeatures
    {
        //#region "For fetching Freight Deatils"

        //public DataSet FetchFreight(Int32 CurrentPage = 0, Int32 TotalPage = 0, string CountryPk = "0")
        //{
        //    Int32 last = default(Int32);
        //    Int32 start = default(Int32);
        //    Int32 TotalRecords = default(Int32);
        //    StringBuilder strQuery = new StringBuilder();
        //    WorkFlow objWF = new WorkFlow();
        //    DataSet DS = new DataSet();

        //    string strPsql = null;
        //    StringBuilder strCondition = new StringBuilder();

        //    strCondition.Append("SELECT Count(*) from freight_element_mst_tbl fr");
        //    strCondition.Append("          WHERE ");
        //    strCondition.Append(" fr.active_flag = 1");

        //    TotalRecords = (Int32)objWF.ExecuteScaler(strCondition.ToString());
        //    TotalPage = TotalRecords / RecordsPerPage;
        //    if (TotalRecords % RecordsPerPage != 0)
        //    {
        //        TotalPage += 1;
        //    }
        //    if (CurrentPage > TotalPage)
        //    {
        //        CurrentPage = 1;
        //    }
        //    if (TotalRecords == 0)
        //    {
        //        CurrentPage = 0;
        //    }
        //    last = CurrentPage * RecordsPerPage;
        //    start = (CurrentPage - 1) * RecordsPerPage + 1;

        //    try
        //    {
        //        strQuery.Append(" select * from (");
        //        strQuery.Append("SELECT ROWNUM SR_NO,q.* FROM ");
        //        strQuery.Append("(select frv.vat_mst_pk,");
        //        strQuery.Append("       frv.freight_element_mst_fk,");
        //        strQuery.Append("       fr.freight_element_id,");
        //        strQuery.Append("       fr.freight_element_name,");
        //        strQuery.Append("       frv.vat_code,");
        //        strQuery.Append("       frv.vat_percentage,");
        //        strQuery.Append("       frv.version_no");
        //        strQuery.Append("  from freight_element_mst_tbl fr, frt_vat_country_tbl frv");
        //        strQuery.Append(" where frv.country_mst_fk in (" + CountryPk + ")");
        //        strQuery.Append("   and fr.freight_element_mst_pk = frv.freight_element_mst_fk");
        //        strQuery.Append("   and fr.active_flag = 1");
        //        strQuery.Append("");
        //        strQuery.Append("Union");
        //        strQuery.Append("");
        //        strQuery.Append("select 0 vat_mst_pk,");
        //        strQuery.Append("       fr.freight_element_mst_pk,");
        //        strQuery.Append("       fr.freight_element_id,");
        //        strQuery.Append("       fr.freight_element_name,");
        //        strQuery.Append("       '' vat_code,");
        //        strQuery.Append("       0 vat_percentage,");
        //        strQuery.Append("       fr.version_no");
        //        strQuery.Append("  from freight_element_mst_tbl fr");
        //        strQuery.Append(" where fr.freight_element_mst_pk Not in");
        //        strQuery.Append("       (Select FVC.Freight_Element_Mst_Fk");
        //        strQuery.Append("          From Frt_Vat_Country_Tbl FVC");
        //        strQuery.Append("         where FVC.COUNTRY_MST_FK in(" + CountryPk + "))");
        //        strQuery.Append("   and  fr.active_flag = 1");
        //        strQuery.Append(" order by freight_element_id )q ) ");
        //        strQuery.Append("  WHERE SR_NO  Between " + start + " and " + last + "");
        //        strPsql = strQuery.ToString();
        //        return objWF.GetDataSet(strPsql);
        //    }
        //    catch (OracleException sqlExp)
        //    {
        //        ErrorMessage = sqlExp.Message;
        //        throw sqlExp;
        //    }
        //    catch (Exception exp)
        //    {
        //        ErrorMessage = exp.Message;
        //        throw exp;
        //    }
        //}

        //#endregion "For fetching Freight Deatils"

        //#region "For fetching Freight Deatils"

        //public DataSet FetchTDSFreight(Int32 CurrentPage = 0, Int32 TotalPage = 0, string CountryPk = "0")
        //{
        //    Int32 last = default(Int32);
        //    Int32 start = default(Int32);
        //    Int32 TotalRecords = default(Int32);
        //    StringBuilder strQuery = new StringBuilder();
        //    WorkFlow objWF = new WorkFlow();
        //    DataSet DS = new DataSet();

        //    string strPsql = null;
        //    StringBuilder strCondition = new StringBuilder();

        //    strCondition.Append("SELECT Count(*) from freight_element_mst_tbl fr");
        //    strCondition.Append("          WHERE ");
        //    strCondition.Append(" fr.active_flag = 1");

        //    TotalRecords = (Int32)objWF.ExecuteScaler(strCondition.ToString());
        //    TotalPage = TotalRecords / RecordsPerPage;
        //    if (TotalRecords % RecordsPerPage != 0)
        //    {
        //        TotalPage += 1;
        //    }
        //    if (CurrentPage > TotalPage)
        //    {
        //        CurrentPage = 1;
        //    }
        //    if (TotalRecords == 0)
        //    {
        //        CurrentPage = 0;
        //    }
        //    last = CurrentPage * RecordsPerPage;
        //    start = (CurrentPage - 1) * RecordsPerPage + 1;

        //    try
        //    {
        //        strQuery.Append(" select * from (");
        //        strQuery.Append("SELECT ROWNUM SR_NO,q.* FROM ");
        //        strQuery.Append("(select frv.tds_mst_pk  vat_mst_pk,");
        //        strQuery.Append("       frv.freight_element_mst_fk,");
        //        // strQuery.Append("       frv.country_mst_fk," & vbCrLf)
        //        strQuery.Append("       fr.freight_element_id,");
        //        strQuery.Append("       fr.freight_element_name,");
        //        strQuery.Append("       frv.tds_code vat_code,");
        //        strQuery.Append("       frv.tds_percentage vat_percentage,");
        //        strQuery.Append("       frv.version_no");
        //        strQuery.Append("  from freight_element_mst_tbl fr, frt_tds_country_tbl frv");
        //        strQuery.Append(" where frv.country_mst_fk in (" + CountryPk + ")");
        //        strQuery.Append("   and fr.freight_element_mst_pk = frv.freight_element_mst_fk");
        //        strQuery.Append("   and fr.active_flag = 1");
        //        strQuery.Append("");
        //        strQuery.Append("Union");
        //        strQuery.Append("");
        //        strQuery.Append("select 0 vat_mst_pk,");
        //        strQuery.Append("       fr.freight_element_mst_pk,");
        //        //strQuery.Append("       frv.country_mst_fk," & vbCrLf)
        //        strQuery.Append("       fr.freight_element_id,");
        //        strQuery.Append("       fr.freight_element_name,");
        //        strQuery.Append("       '' vat_code,");
        //        strQuery.Append("       0  vat_percentage,");
        //        strQuery.Append("       fr.version_no");
        //        strQuery.Append("  from freight_element_mst_tbl fr");
        //        strQuery.Append(" where fr.freight_element_mst_pk Not in");
        //        strQuery.Append("       (Select FVC.Freight_Element_Mst_Fk");
        //        strQuery.Append("          From Frt_tds_Country_Tbl FVC");
        //        strQuery.Append("         where FVC.COUNTRY_MST_FK in(" + CountryPk + "))");
        //        //" & CountryPk & "
        //        // strQuery.Append("   and fr.freight_element_mst_pk = frv.freight_element_mst_fk(+)" & vbCrLf)
        //        strQuery.Append("   and  fr.active_flag = 1");
        //        strQuery.Append(" order by freight_element_id )q ) ");
        //        strQuery.Append("  WHERE SR_NO  Between " + start + " and " + last + "");

        //        //select FVC.COUNTRY_MST_FK  From Frt_Vat_Country_Tbl FVC where FVC.COUNTRY_MST_FK not in
        //        strPsql = strQuery.ToString();
        //        return objWF.GetDataSet(strPsql);
        //        //Modified by Manjunath  PTS ID:Sep-02  13/09/2011
        //    }
        //    catch (OracleException sqlExp)
        //    {
        //        ErrorMessage = sqlExp.Message;
        //        throw sqlExp;
        //    }
        //    catch (Exception exp)
        //    {
        //        ErrorMessage = exp.Message;
        //        throw exp;
        //    }
        //}

        //#endregion "For fetching Freight Deatils"

        //#region "Fetch Country"

        //public DataSet FetchCountry(string CountryPk = "")
        //{
        //    string strSQL = null;
        //    WorkFlow objWF = new WorkFlow();
        //    try
        //    {
        //        strSQL = "select c.country_name from Country_Mst_Tbl c where c.country_mst_pk=" + CountryPk;
        //        return objWF.GetDataSet(strSQL);
        //    }
        //    catch (OracleException sqlExp)
        //    {
        //        ErrorMessage = sqlExp.Message;
        //        throw sqlExp;
        //    }
        //    catch (Exception exp)
        //    {
        //        ErrorMessage = exp.Message;
        //        throw exp;
        //    }
        //}

        //#endregion "Fetch Country"

        //#region "For Save"

        //// Public Function Save(ByRef M_DataSet As DataSet, Optional ByVal CountryPks As String = "0") As ArrayList
        ////add by latha
        //public ArrayList Save(DataSet M_DataSet, string CountryPks = "0", string FrieghtPks = "0")
        //{
        //    //Optional ByVal CountryPk As String = "0"
        //    WorkFlow objWK = new WorkFlow();
        //    OracleTransaction insertTrans = null;
        //    OracleCommand insCommand = new OracleCommand();
        //    OracleCommand updCommand = new OracleCommand();
        //    int intPkValue = 0;
        //    bool isUpdate = false;
        //    int Afct = 0;
        //    DataTable DtTbl = new DataTable();
        //    DataRow DtRw = null;
        //    int nRowCnt = 0;
        //    int i = 0;
        //    int len = 0;
        //    int CPK = 0;
        //    string[] temp = null;
        //    if (!string.IsNullOrEmpty(CountryPks))
        //    {
        //        temp = CountryPks.Split(',');
        //        len = temp.Length();
        //    }

        //    objWK.OpenConnection();

        //    insertTrans = objWK.MyConnection.BeginTransaction();
        //    try
        //    {
        //        for (i = 0; i <= len - 1; i++)
        //        {
        //            CPK = temp[i];
        //            for (nRowCnt = 0; nRowCnt <= M_DataSet.Tables[0].Rows.Count - 1; nRowCnt++)
        //            {
        //                var _with1 = objWK.MyCommand;
        //                _with1.Transaction = insertTrans;
        //                _with1.Connection = objWK.MyConnection;
        //                _with1.CommandType = CommandType.StoredProcedure;
        //                _with1.CommandText = objWK.MyUserName + ".FRT_VAT_COUNTRY_TBL_PKG.FRT_VAT_COUNTRY_TBL_UPD";
        //                _with1.Parameters.Clear();

        //                var _with2 = _with1.Parameters;
        //                _with2.Add("VAT_MST_PK_IN", M_DataSet.Tables[0].Rows[nRowCnt]["VAT_MST_PK"]).Direction = ParameterDirection.Input;
        //                _with2.Add("COUNTRY_MST_FK_IN", CPK).Direction = ParameterDirection.Input;
        //                _with2.Add("FREIGHT_ELEMENT_MST_FK_IN", M_DataSet.Tables[0].Rows[nRowCnt]["FREIGHT_ELEMENT_MST_FK"]).Direction = ParameterDirection.Input;
        //                _with2.Add("VAT_CODE_IN", getDefault(M_DataSet.Tables[0].Rows[nRowCnt]["VAT_CODE"], "")).Direction = ParameterDirection.Input;
        //                _with2.Add("VAT_PERCENTAGE_IN", getDefault(M_DataSet.Tables[0].Rows[nRowCnt]["VAT_PERCENTAGE"], "")).Direction = ParameterDirection.Input;
        //                _with2.Add("LAST_MODIFIED_FK_IN", LAST_MODIFIED_BY).Direction = ParameterDirection.Input;
        //                _with2.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
        //                _with2.Add("RETURN_VALUE", OracleDbType.Varchar2, 100).Direction = ParameterDirection.Output;
        //                objWK.MyCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

        //                Afct = _with1.ExecuteNonQuery();

        //                if (Afct > 0)
        //                {
        //                    intPkValue = Convert.ToInt32(getDefault(objWK.MyCommand.Parameters["RETURN_VALUE"].Value, 0));
        //                }
        //                else
        //                {
        //                    insertTrans.Rollback();
        //                }
        //            }
        //        }

        //        if (arrMessage.Count > 0)
        //        {
        //            insertTrans.Rollback();
        //            return arrMessage;
        //        }
        //        else if (Afct > 0)
        //        {
        //            insertTrans.Commit();
        //            arrMessage.Add("All Data Saved Successfully");
        //            return arrMessage;
        //        }
        //    }
        //    catch (OracleException oraexp)
        //    {
        //        insertTrans.Rollback();
        //        throw oraexp;
        //        arrMessage.Add(oraexp.Message);
        //    }
        //    catch (Exception ex)
        //    {
        //        insertTrans.Rollback();
        //        arrMessage.Add(ex.Message);
        //        throw ex;
        //    }
        //    finally
        //    {
        //        objWK.CloseConnection();
        //    }
        //}

        //#endregion "For Save"

        //#region "For save Vat settings to one country"

        //public ArrayList SaveVatForOneCountry(DataSet M_DataSet, string CountryPk = "0")
        //{
        //    WorkFlow objWK = new WorkFlow();
        //    OracleTransaction insertTrans = null;
        //    OracleCommand insCommand = new OracleCommand();
        //    int intPkValue = 0;
        //    bool isUpdate = false;
        //    int Afct = 0;
        //    DataTable DtTbl = new DataTable();
        //    DataRow DtRw = null;
        //    int nRowCnt = 0;
        //    objWK.OpenConnection();
        //    insertTrans = objWK.MyConnection.BeginTransaction();

        //    try
        //    {
        //        for (nRowCnt = 0; nRowCnt <= M_DataSet.Tables[0].Rows.Count - 1; nRowCnt++)
        //        {
        //            var _with3 = objWK.MyCommand;
        //            _with3.Transaction = insertTrans;
        //            _with3.Connection = objWK.MyConnection;
        //            _with3.CommandType = CommandType.StoredProcedure;
        //            _with3.CommandText = objWK.MyUserName + ".FRT_VAT_COUNTRY_TBL_PKG.FRT_VAT_COUNTRY_TBL_UPD";
        //            _with3.Parameters.Clear();

        //            var _with4 = _with3.Parameters;

        //            _with4.Add("VAT_MST_PK_IN", M_DataSet.Tables[0].Rows[nRowCnt]["VAT_MST_PK"]).Direction = ParameterDirection.Input;
        //            _with4.Add("COUNTRY_MST_FK_IN", CountryPk).Direction = ParameterDirection.Input;

        //            _with4.Add("FREIGHT_ELEMENT_MST_FK_IN", M_DataSet.Tables[0].Rows[nRowCnt]["FREIGHT_ELEMENT_MST_FK"]).Direction = ParameterDirection.Input;

        //            _with4.Add("VAT_CODE_IN", getDefault(M_DataSet.Tables[0].Rows[nRowCnt]["VAT_CODE"], "")).Direction = ParameterDirection.Input;

        //            _with4.Add("VAT_PERCENTAGE_IN", getDefault(M_DataSet.Tables[0].Rows[nRowCnt]["VAT_PERCENTAGE"], "")).Direction = ParameterDirection.Input;

        //            _with4.Add("LAST_MODIFIED_FK_IN", LAST_MODIFIED_BY).Direction = ParameterDirection.Input;

        //            _with4.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
        //            //.Add("VERSION_NO_IN", M_DataSet.Tables(0).Rows(nRowCnt).Item("VERSION_NO")).Direction = ParameterDirection.Input

        //            _with4.Add("RETURN_VALUE", OracleDbType.Varchar2, 100).Direction = ParameterDirection.Output;
        //            objWK.MyCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

        //            Afct = _with3.ExecuteNonQuery();

        //            if (Afct > 0)
        //            {
        //                intPkValue = Convert.ToInt32(getDefault(objWK.MyCommand.Parameters["RETURN_VALUE"].Value, 0));
        //            }
        //            else
        //            {
        //                insertTrans.Rollback();
        //            }
        //        }

        //        if (arrMessage.Count > 0)
        //        {
        //            insertTrans.Rollback();
        //            return arrMessage;
        //        }
        //        else
        //        {
        //            insertTrans.Commit();
        //            arrMessage.Add("All Data Saved Successfully");
        //            return arrMessage;
        //        }
        //    }
        //    catch (OracleException oraexp)
        //    {
        //        insertTrans.Rollback();
        //        throw oraexp;
        //        arrMessage.Add(oraexp.Message);
        //    }
        //    catch (Exception ex)
        //    {
        //        insertTrans.Rollback();
        //        arrMessage.Add(ex.Message);
        //        throw ex;
        //    }
        //    finally
        //    {
        //        objWK.CloseConnection();
        //    }
        //}

        ////        Public Function delTrnVatDetails(ByRef M_DataSet As DataSet, Optional ByVal CountryPk As String = "0")
        ////            Dim objWK As New WorkFlow()
        ////            Dim delTrans As OracleTransaction
        ////            Dim delCommand As New OracleClient.OracleCommand
        ////            Dim intPkValue As String
        ////            Dim isUpdate As Boolean
        ////            Dim Afct As Integer
        ////            Dim DtTbl As New DataTable
        ////            Dim DtRw As DataRow
        ////            Dim nRowCnt As Integer
        ////            objWK.OpenConnection()

        ////            delTrans = objWK.MyConnection.BeginTransaction()
        ////            Try
        ////                arrmessage.Clear()
        ////                With objWK.MyCommand
        ////                    .Transaction = delTrans
        ////                    .Connection = objWK.MyConnection
        ////                    .CommandType = CommandType.StoredProcedure
        ////                    .CommandText = objWK.MyUserName & ".FRT_VAT_COUNTRY_TBL_PKG.FRT_VAT_COUNTRY_TBL_DEL"
        ////                    .Parameters.Clear()

        ////                    For nRowCnt = 0 To M_DataSet.Tables(0).Rows.Count - 1
        ////                        If M_DataSet.Tables(0).Rows(nRowCnt).Item("VAT_PERCENTAGE") = 0 And Not IsDBNull(M_DataSet.Tables(0).Rows(nRowCnt).Item("VAT_MST_PK")) Then
        ////                            With .Parameters

        ////                                .Add("VAT_MST_PK_IN", M_DataSet.Tables(0).Rows(nRowCnt).Item("VAT_MST_PK")).Direction = ParameterDirection.Input
        ////                                '.Add("COUNTRY_MST_FK_IN", CountryPk).Direction = ParameterDirection.Input
        ////                                '.Add("RETURN_VALUE", _
        ////                                'OracleClient.OracleDbType.Varchar2, 100).Direction = ParameterDirection.Output
        ////                                'objWK.MyCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current
        ////                                ' .Add("RETURN_VALUE", "VAT_MST_PK").Direction = ParameterDirection.Output

        ////                            End With
        ////                            Afct = .ExecuteNonQuery()
        ////                        End If
        ////                        'If Afct > 0 Then
        ////                        '    intPkValue = CType(objWK.MyCommand.Parameters["RETURN_VALUE"].Value, String)
        ////                        'Else
        ////                        '    delTrans.Rollback()
        ////                        'End If
        ////                    Next
        ////                End With
        ////                If arrMessage.Count > 0 Then
        ////                    delTrans.Rollback()
        ////                    Return arrMessage
        ////                Else
        ////                    delTrans.Commit()
        ////                    arrMessage.Add("All Data Saved Successfully")
        ////                    Return arrMessage
        ////                End If
        ////            Catch ex As Exception
        ////                arrMessage.Add(ex.Message)
        ////                Return arrMessage
        ////            End Try
        ////        End Function

        ////        Public Function delTrnVatDetailsForCountries(ByRef M_DataSet As DataSet, Optional ByVal CountryPks As String = "0")
        ////            Dim objWK As New WorkFlow()
        ////            Dim delTrans As OracleTransaction
        ////            Dim delCommand As New OracleClient.OracleCommand
        ////            Dim intPkValue As Integer
        ////            Dim isUpdate As Boolean
        ////            Dim Afct As Integer
        ////            Dim DtTbl As New DataTable
        ////            Dim DtRw As DataRow
        ////            Dim nRowCnt As Integer
        ////            Dim i As Integer
        ////            Dim len As Integer
        ////            Dim CPK As Integer 'Country pk
        ////            Dim temp As String()

        ////            If CountryPks <> "" Then
        ////                temp = CountryPks.Split(',')
        ////                len = temp.Length()
        ////            End If

        ////            objWK.OpenConnection()

        ////            delTrans = objWK.MyConnection.BeginTransaction()
        ////            Try
        ////                arrmessage.Clear()
        ////                With objWK.MyCommand
        ////                    .Transaction = delTrans
        ////                    .Connection = objWK.MyConnection
        ////                    .CommandType = CommandType.StoredProcedure
        ////                    .CommandText = objWK.MyUserName & ".FRT_VAT_COUNTRY_TBL_PKG.FRT_VAT_COUNTRY_TBL_DEL"
        ////                    .Parameters.Clear()

        ////                    For i = 0 To len - 1
        ////                        CPK = temp(i)
        ////                        For nRowCnt = 0 To M_DataSet.Tables(0).Rows.Count - 1
        ////                            If M_DataSet.Tables(0).Rows(nRowCnt).Item("VAT_PERCENTAGE") = 0 Then
        ////                                With .Parameters

        ////                                    .Add("VAT_MST_PK_IN", M_DataSet.Tables(0).Rows(nRowCnt).Item("VAT_MST_PK")).Direction = ParameterDirection.Input
        ////                                    '.Add("COUNTRY_MST_FK_IN", CountryPk).Direction = ParameterDirection.Input
        ////                                    '.Add("RETURN_VALUE", _
        ////                                    '                     OracleClient.OracleDbType.Varchar2, 100).Direction = ParameterDirection.Output
        ////                                    'objWK.MyCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current
        ////                                    '.Add("RETURN_VALUE", "VAT_MST_PK").Direction = ParameterDirection.Output

        ////                                End With
        ////                                Afct = .ExecuteNonQuery()
        ////                            End If
        ////                            'If Afct > 0 Then
        ////                            '    intPkValue = CType(objWK.MyCommand.Parameters["RETURN_VALUE"].Value, Integer)
        ////                            'Else
        ////                            '    delTrans.Rollback()
        ////                            'End If
        ////                        Next
        ////                    Next
        ////                End With
        ////                If arrMessage.Count > 0 Then
        ////                    delTrans.Rollback()
        ////                    Return arrMessage
        ////                ElseIf Afct > 0 Then
        ////                    delTrans.Commit()
        ////                    arrMessage.Add("All Data Saved Successfully")
        ////                    Return arrMessage
        ////                End If
        ////            Catch ex As Exception
        ////                arrMessage.Add(ex.Message)
        ////                Return arrMessage
        ////            End Try
        ////        End Function

        //#endregion "For save Vat settings to one country"

        ////'Vasva for TDS Settings

        //#region "For save Vat settings to one country"

        //public ArrayList SaveTDSForOneCountry(DataSet M_DataSet, string CountryPk = "0")
        //{
        //    WorkFlow objWK = new WorkFlow();
        //    OracleTransaction insertTrans = null;
        //    OracleCommand insCommand = new OracleCommand();
        //    int intPkValue = 0;
        //    bool isUpdate = false;
        //    int Afct = 0;
        //    DataTable DtTbl = new DataTable();
        //    DataRow DtRw = null;
        //    int nRowCnt = 0;
        //    objWK.OpenConnection();

        //    insertTrans = objWK.MyConnection.BeginTransaction();
        //    try
        //    {
        //        for (nRowCnt = 0; nRowCnt <= M_DataSet.Tables[0].Rows.Count - 1; nRowCnt++)
        //        {
        //            var _with5 = objWK.MyCommand;
        //            _with5.Transaction = insertTrans;
        //            _with5.Connection = objWK.MyConnection;
        //            _with5.CommandType = CommandType.StoredProcedure;
        //            _with5.CommandText = objWK.MyUserName + ".FRT_TDS_COUNTRY_TBL_PKG.FRT_TDS_COUNTRY_TBL_UPD";
        //            _with5.Parameters.Clear();

        //            var _with6 = _with5.Parameters;
        //            _with6.Add("TDS_MST_PK_IN", M_DataSet.Tables[0].Rows[nRowCnt]["VAT_MST_PK"]).Direction = ParameterDirection.Input;
        //            _with6.Add("COUNTRY_MST_FK_IN", CountryPk).Direction = ParameterDirection.Input;

        //            _with6.Add("FREIGHT_ELEMENT_MST_FK_IN", M_DataSet.Tables[0].Rows[nRowCnt]["FREIGHT_ELEMENT_MST_FK"]).Direction = ParameterDirection.Input;

        //            _with6.Add("TDS_CODE_IN", getDefault(M_DataSet.Tables[0].Rows[nRowCnt]["VAT_CODE"], "")).Direction = ParameterDirection.Input;

        //            _with6.Add("TDS_PERCENTAGE_IN", getDefault(M_DataSet.Tables[0].Rows[nRowCnt]["VAT_PERCENTAGE"], "")).Direction = ParameterDirection.Input;

        //            _with6.Add("LAST_MODIFIED_FK_IN", LAST_MODIFIED_BY).Direction = ParameterDirection.Input;

        //            _with6.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

        //            _with6.Add("RETURN_VALUE", OracleDbType.Varchar2, 100).Direction = ParameterDirection.Output;
        //            objWK.MyCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

        //            Afct = _with5.ExecuteNonQuery();

        //            if (Afct > 0)
        //            {
        //                intPkValue = Convert.ToInt32(getDefault(objWK.MyCommand.Parameters["RETURN_VALUE"].Value, 0));
        //            }
        //            else
        //            {
        //                insertTrans.Rollback();
        //            }
        //        }
        //        if (arrMessage.Count > 0)
        //        {
        //            insertTrans.Rollback();
        //            return arrMessage;
        //        }
        //        else
        //        {
        //            insertTrans.Commit();
        //            arrMessage.Add("All Data Saved Successfully");
        //            return arrMessage;
        //        }
        //    }
        //    catch (OracleException oraexp)
        //    {
        //        insertTrans.Rollback();
        //        throw oraexp;
        //        arrMessage.Add(oraexp.Message);
        //    }
        //    catch (Exception ex)
        //    {
        //        insertTrans.Rollback();
        //        arrMessage.Add(ex.Message);
        //        throw ex;
        //    }
        //    finally
        //    {
        //        objWK.CloseConnection();
        //    }
        //}

        //#endregion "For save Vat settings to one country"

        //#region "For Save"

        //public ArrayList SaveTDS(DataSet M_DataSet, string CountryPks = "0", string FrieghtPks = "0")
        //{
        //    WorkFlow objWK = new WorkFlow();
        //    OracleTransaction insertTrans = null;
        //    OracleCommand insCommand = new OracleCommand();
        //    OracleCommand updCommand = new OracleCommand();
        //    int intPkValue = 0;
        //    int Afct = 0;
        //    DataTable DtTbl = new DataTable();
        //    int nRowCnt = 0;
        //    int i = 0;
        //    int len = 0;
        //    int CPK = 0;
        //    //Country pk
        //    //add by latha
        //    string[] temp = null;
        //    if (!string.IsNullOrEmpty(CountryPks))
        //    {
        //        temp = CountryPks.Split(',');
        //        len = temp.Length();
        //    }

        //    objWK.OpenConnection();

        //    insertTrans = objWK.MyConnection.BeginTransaction();
        //    try
        //    {
        //        for (i = 0; i <= len - 1; i++)
        //        {
        //            CPK = temp[i];
        //            for (nRowCnt = 0; nRowCnt <= M_DataSet.Tables[0].Rows.Count - 1; nRowCnt++)
        //            {
        //                var _with7 = objWK.MyCommand;
        //                _with7.Transaction = insertTrans;
        //                _with7.Connection = objWK.MyConnection;
        //                _with7.CommandType = CommandType.StoredProcedure;
        //                _with7.CommandText = objWK.MyUserName + ".FRT_TDS_COUNTRY_TBL_PKG.FRT_TDS_COUNTRY_TBL_UPD";
        //                _with7.Parameters.Clear();

        //                var _with8 = _with7.Parameters;
        //                _with8.Add("TDS_MST_PK_IN", M_DataSet.Tables[0].Rows[nRowCnt]["VAT_MST_PK"]).Direction = ParameterDirection.Input;
        //                _with8.Add("COUNTRY_MST_FK_IN", CPK).Direction = ParameterDirection.Input;
        //                _with8.Add("FREIGHT_ELEMENT_MST_FK_IN", M_DataSet.Tables[0].Rows[nRowCnt]["FREIGHT_ELEMENT_MST_FK"]).Direction = ParameterDirection.Input;
        //                _with8.Add("TDS_CODE_IN", getDefault(M_DataSet.Tables[0].Rows[nRowCnt]["VAT_CODE"], "")).Direction = ParameterDirection.Input;
        //                _with8.Add("TDS_PERCENTAGE_IN", getDefault(M_DataSet.Tables[0].Rows[nRowCnt]["VAT_PERCENTAGE"], "")).Direction = ParameterDirection.Input;
        //                _with8.Add("LAST_MODIFIED_FK_IN", LAST_MODIFIED_BY).Direction = ParameterDirection.Input;
        //                _with8.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

        //                _with8.Add("RETURN_VALUE", OracleDbType.Varchar2, 100).Direction = ParameterDirection.Output;
        //                objWK.MyCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

        //                Afct = _with7.ExecuteNonQuery();

        //                if (Afct > 0)
        //                {
        //                    intPkValue = Convert.ToInt32(getDefault(objWK.MyCommand.Parameters["RETURN_VALUE"].Value, 0));
        //                }
        //                else
        //                {
        //                    insertTrans.Rollback();
        //                }
        //            }
        //        }
        //        if (arrMessage.Count > 0)
        //        {
        //            insertTrans.Rollback();
        //            return arrMessage;
        //        }
        //        else if (Afct > 0)
        //        {
        //            insertTrans.Commit();
        //            arrMessage.Add("All Data Saved Successfully");
        //            return arrMessage;
        //        }
        //    }
        //    catch (OracleException oraexp)
        //    {
        //        insertTrans.Rollback();
        //        throw oraexp;
        //        arrMessage.Add(oraexp.Message);
        //    }
        //    catch (Exception ex)
        //    {
        //        insertTrans.Rollback();
        //        arrMessage.Add(ex.Message);
        //        throw ex;
        //    }
        //    finally
        //    {
        //        objWK.CloseConnection();
        //    }
        //}

        //#endregion "For Save"
    }
}