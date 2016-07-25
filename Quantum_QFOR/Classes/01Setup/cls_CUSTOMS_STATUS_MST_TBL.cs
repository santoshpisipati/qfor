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

//Option Strict On

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class clsCUSTOMS_STATUS_MST_TBL : CommonFeatures
    {
        //#region "List of Members of the Class"

        //private Int64 M_Customs_Code_Mst_Pk;
        //private string M_Customs_Status_Code;
        //private string M_Customs_Status_desc;
        //private Int16 M_Commisionable;
        //private Int16 M_Local_Charges;
        //private Int16 M_Income_To_Principle;
        //private Int16 M_Income_To_Agency;
        //private string M_Surcharge_Type;
        //private Int16 M_ITEM_TYPE;
        //private Int16 M_CHARGE_BASIS;

        //private Int16 M_PRINTING_PRIORITY;

        //#endregion "List of Members of the Class"

        //#region "List of Properties"

        //public Int64 Customs_Code_Mst_Pk
        //{
        //    get { return M_Customs_Code_Mst_Pk; }
        //    set { M_Customs_Code_Mst_Pk = value; }
        //}

        //public string Customs_Status_Code
        //{
        //    get { return M_Customs_Status_Code; }
        //    set { M_Customs_Status_Code = value; }
        //}

        //public string Customs_Status_desc
        //{
        //    get { return M_Customs_Status_desc; }
        //    set { M_Customs_Status_desc = value; }
        //}

        //public Int16 Commisionable
        //{
        //    get { return M_Commisionable; }
        //    set { M_Commisionable = value; }
        //}

        //public Int16 Local_Charges
        //{
        //    get { return M_Local_Charges; }
        //    set { M_Local_Charges = value; }
        //}

        //public Int16 Income_To_Principle
        //{
        //    get { return M_Income_To_Principle; }
        //    set { M_Income_To_Principle = value; }
        //}

        //public Int16 Income_To_Agency
        //{
        //    get { return M_Income_To_Agency; }
        //    set { M_Income_To_Agency = value; }
        //}

        //public string Surcharge_Type
        //{
        //    get { return M_Surcharge_Type; }
        //    set { M_Surcharge_Type = value; }
        //}

        //public Int16 ITEM_TYPE
        //{
        //    get { return M_ITEM_TYPE; }
        //    set { M_ITEM_TYPE = value; }
        //}

        //public Int16 CHARGE_BASIS
        //{
        //    get { return M_CHARGE_BASIS; }
        //    set { M_CHARGE_BASIS = value; }
        //}

        //public Int16 PRINTING_PRIORITY
        //{
        //    get { return M_PRINTING_PRIORITY; }
        //    set { M_PRINTING_PRIORITY = value; }
        //}

        //#endregion "List of Properties"

        //#region "Fetch All"

        //public DataSet FetchAll(Int16 Customs_Code_Mst_Pk = 0, string Customs_Status_Code = "", string Customs_Status_desc = "", string Country = "", string SearchType = "", string strColumnName = "", int ActiveFlag = 1, Int32 CurrentPage = 0, Int32 TotalPage = 0, bool blnSortAscending = false,
        //Int32 flag = 0)
        //{
        //    Int32 last = default(Int32);
        //    Int32 start = default(Int32);
        //    string strSQL = null;
        //    string strCondition = null;
        //    Int32 TotalRecords = default(Int32);
        //    WorkFlow objWF = new WorkFlow();
        //    if (flag == 0)
        //    {
        //        strCondition += " AND 1=2";
        //    }

        //    if (Customs_Code_Mst_Pk > 0)
        //    {
        //        strCondition = strCondition + " AND Customs_Code_Mst_Pk=" + Customs_Code_Mst_Pk;
        //    }
        //    if (Customs_Status_Code.Trim().Length > 0)
        //    {
        //        if (SearchType.ToString().Trim().Length > 0)
        //        {
        //            if (SearchType == "S")
        //            {
        //                strCondition = strCondition + " AND UPPER(Customs_Status_Code) LIKE '" + Customs_Status_Code.ToUpper().Replace("'", "''") + "%'";
        //            }
        //            else
        //            {
        //                strCondition = strCondition + " AND UPPER(Customs_Status_Code) LIKE '%" + Customs_Status_Code.ToUpper().Replace("'", "''") + "%'";
        //            }
        //        }
        //        else
        //        {
        //            strCondition = strCondition + " AND UPPER(Customs_Status_Code) LIKE '%" + Customs_Status_Code.ToUpper().Replace("'", "''") + "%'";
        //        }
        //    }
        //    if (Customs_Status_desc.Trim().Length > 0)
        //    {
        //        if (SearchType.ToString().Trim().Length > 0)
        //        {
        //            if (SearchType == "S")
        //            {
        //                strCondition = strCondition + " AND UPPER(Customs_Status_desc) LIKE '" + Customs_Status_desc.ToUpper().Replace("'", "''") + "%'";
        //            }
        //            else
        //            {
        //                strCondition = strCondition + " AND UPPER(Customs_Status_desc) LIKE '%" + Customs_Status_desc.ToUpper().Replace("'", "''") + "%'";
        //            }
        //        }
        //        else
        //        {
        //            strCondition = strCondition + " AND UPPER(Customs_Status_desc) LIKE '%" + Customs_Status_desc.ToUpper().Replace("'", "''") + "%'";
        //        }
        //    }
        //    if (Country.Trim().Length > 0)
        //    {
        //        //strCondition &= " and exist(" & _
        //        //     "select country_name from country_mst_tbl where " & _
        //        //     "country_mst_pk in(select country_mst_fk from customs_status_code_trn where " & _
        //        //     "customs_code_mst_fk = fr.costoms_status_mst_pk) and " & _
        //        //     "upper(country_name) like '%" & Country.ToUpper.Replace("'", "''") & "%' ) "
        //        strCondition += " and fr.CUSTOMS_CODE_MST_PK in ";
        //        strCondition += " ( select CUSTOMS_CODE_MST_FK from customs_status_code_trn , country_mst_tbl, Customs_Status_Mst_Tbl ";
        //        strCondition += " where country_mst_tbl.COUNTRY_MST_PK= customs_status_code_trn.COUNTRY_MST_FK and  ";
        //        strCondition += " Customs_Status_Mst_Tbl.CUSTOMS_CODE_MST_PK = customs_status_code_trn.CUSTOMS_CODE_MST_FK ";
        //        if (SearchType == "S")
        //        {
        //            strCondition = strCondition + " AND UPPER(Country_name) LIKE '" + Country.ToUpper().Replace("'", "''") + "%' )";
        //        }
        //        else
        //        {
        //            strCondition = strCondition + " AND UPPER(Country_name) LIKE '%" + Country.ToUpper().Replace("'", "''") + "%' )";
        //        }
        //    }
        //    //If Country.Trim.Length > 0 Then
        //    //    If SearchType.ToString.Trim.Length > 0 Then
        //    //        If SearchType = "S" Then
        //    //            strCondition = strCondition & " AND UPPER(Country_name) LIKE '" & Country.ToUpper.Replace("'", "''") & "%'" & vbCrLf
        //    //        Else
        //    //            strCondition = strCondition & " AND UPPER(Country_name) LIKE '%" & Country.ToUpper.Replace("'", "''") & "%'" & vbCrLf
        //    //        End If
        //    //    Else
        //    //        strCondition = strCondition & " AND UPPER(Country_name) LIKE '%" & Country.ToUpper.Replace("'", "''") & "%'" & vbCrLf
        //    //    End If
        //    //End If
        //    if (ActiveFlag == 1)
        //    {
        //        strCondition = strCondition + " AND fr.ACTIVE_FLAG= 1";
        //    }

        //    strSQL = "SELECT Count(*)";
        //    strSQL = strSQL + " FROM ";
        //    strSQL = strSQL + " Customs_Status_Mst_Tbl fr ";
        //    //strSQL = strSQL & vbCrLf & " COUNTRY_MST_TBL crr"
        //    strSQL = strSQL + "WHERE 1=1";
        //    //strSQL = strSQL & vbCrLf & " and"
        //    //strSQL = strSQL & vbCrLf & " fr.COUNTRY_MST_FK= CRR.COUNTRY_MST_PK(+)"
        //    strSQL += strCondition;
        //    TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
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

        //    //If CInt(SortCol) > 0 Then
        //    //    strCondition = strCondition & " order by " & CInt(SortCol)
        //    //End If

        //    strSQL = "SELECT * FROM (";
        //    strSQL += "SELECT ROWNUM SR_NO, q.* FROM ";
        //    strSQL += "(SELECT ";
        //    strSQL = strSQL + " fr.Customs_Code_Mst_Pk,";
        //    strSQL = strSQL + " NVL(fr.ACTIVE_FLAG,0) ACTIVE_FLAG , ";
        //    strSQL = strSQL + " fr.Customs_Status_Code,";
        //    strSQL = strSQL + " fr.Customs_Status_desc Customs_Status_desc,";
        //    //strSQL = strSQL & vbCrLf & " crr.COUNTRY_NAME,"
        //    //strSQL = strSQL & vbCrLf & " fr.COUNTRY_MST_FK,"
        //    strSQL = strSQL + " fr.VERSION_NO";
        //    strSQL = strSQL + " FROM ";
        //    strSQL = strSQL + " Customs_Status_Mst_Tbl fr ";
        //    //strSQL = strSQL & vbCrLf & " CHART_OF_ACCOUNT_TRN COA,"
        //    //strSQL = strSQL & vbCrLf & " COUNTRY_MST_TBL crr"
        //    strSQL = strSQL + "WHERE 1=1";
        //    //strSQL = strSQL & vbCrLf & " and"
        //    // strSQL = strSQL & vbCrLf & " fr.COUNTRY_MST_FK= CRR.COUNTRY_MST_PK(+)"
        //    //strSQL = strSQL & vbCrLf & " and"
        //    //strSQL = strSQL & vbCrLf & " fr.CURRENCY_MST_FK= crr.CURRENCY_MST_PK(+)"
        //    strSQL += strCondition;

        //    if (!strColumnName.Equals("SR_NO"))
        //    {
        //        strSQL += "order by " + strColumnName;
        //    }
        //    else
        //    {
        //        strSQL += "order by fr.Customs_Status_Code";
        //    }

        //    if (!blnSortAscending & !strColumnName.Equals("SR_NO"))
        //    {
        //        strSQL += " DESC";
        //    }

        //    strSQL += ")q ) WHERE SR_NO  Between " + start + " and " + last;

        //    //strSQL &= vbCrLf & " order by Customs_Status_Code "
        //    try
        //    {
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

        //#endregion "Fetch All"

        //#region "currency "

        //public DataSet currency()
        //{
        //    string strSQL = null;
        //    strSQL = "SELECT ";
        //    strSQL = strSQL + " currency_mst_pk,";
        //    strSQL = strSQL + " currency_id";
        //    strSQL = strSQL + " from";
        //    strSQL = strSQL + " currency_type_mst_tbl";
        //    strSQL = strSQL + " where";
        //    strSQL = strSQL + " active_flag = 1";
        //    WorkFlow objWF = new WorkFlow();
        //    try
        //    {
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

        //#endregion "currency "

        //#region "Fetch KIND"

        //public DataSet FetchKind(Int16 ContainerKindPK = 0, string ContainerKind = "")
        //{
        //    string strSQL = null;
        //    strSQL = "select c.surcharge_type_mst_pk, c.surcharge_type from surcharge_type_mst_tbl c";
        //    strSQL = strSQL + " order by c.surcharge_type";

        //    WorkFlow objWF = new WorkFlow();
        //    try
        //    {
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

        //#endregion "Fetch KIND"

        //#region "fetch only Country PK"

        //public string fetchOnlyCountry(int customFk)
        //{
        //    string strSQL = null;
        //    //strSQL = strSQL & "Select "
        //    //strSQL = strSQL & "COUNTRY_MST_FK "
        //    //strSQL = strSQL & "from CUSTOMS_STATUS_CODE_TRN Where CUSTOMS_CODE_MST_FK=" & customFk & ""
        //    strSQL = "SELECT colasrow(" + customFk + ") FROM dual";
        //    WorkFlow objWF = new WorkFlow();
        //    try
        //    {
        //        return objWF.ExecuteScaler(strSQL);
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

        //#endregion "fetch only Country PK"

        //#region "Save Function"

        //public ArrayList Save(DataSet M_DataSet)
        //{
        //    //, 'ByRef C_DataTable As DataTable) As ArrayList
        //    WorkFlow objWK = new WorkFlow();
        //    objWK.OpenConnection();
        //    OracleTransaction TRAN = null;

        //    TRAN = objWK.MyConnection.BeginTransaction();
        //    OracleParameterCollection ColPara = new OracleParameterCollection();
        //    int intPKVal = 0;
        //    long lngI = 0;
        //    Int32 RecAfct = default(Int32);
        //    OracleCommand insCommand = new OracleCommand();
        //    OracleCommand updCommand = new OracleCommand();
        //    OracleCommand delCommand = new OracleCommand();

        //    try
        //    {
        //        DataTable DtTbl = new DataTable();
        //        DataRow DtRw = null;
        //        int i = 0;
        //        DtTbl = M_DataSet.Tables[0];
        //        var _with1 = insCommand;
        //        _with1.Connection = objWK.MyConnection;
        //        _with1.CommandType = CommandType.StoredProcedure;
        //        _with1.CommandText = objWK.MyUserName + ".CUSTOMS_STATUS_TBL_PKG.CUSTOMS_CODE_MST_TBL_INS";

        //        _with1.Parameters.Add("Customs_Status_Code_IN", OracleDbType.Varchar2, 20, "Customs_Status_Code").Direction = ParameterDirection.Input;
        //        _with1.Parameters["Customs_Status_Code_IN"].SourceVersion = DataRowVersion.Current;

        //        _with1.Parameters.Add("Customs_Status_desc_IN", OracleDbType.Varchar2, 100, "Customs_Status_desc").Direction = ParameterDirection.Input;
        //        _with1.Parameters["Customs_Status_desc_IN"].SourceVersion = DataRowVersion.Current;

        //        _with1.Parameters.Add("COUNTRY_MST_FK_IN", OracleDbType.Varchar2, 2000, "countryPk").Direction = ParameterDirection.Input;
        //        _with1.Parameters["COUNTRY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

        //        _with1.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 10, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
        //        _with1.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;
        //        _with1.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

        //        _with1.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;

        //        _with1.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

        //        _with1.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "Customs_Code_Mst_Pk").Direction = ParameterDirection.Output;
        //        _with1.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

        //        var _with2 = updCommand;
        //        _with2.Connection = objWK.MyConnection;

        //        _with2.CommandType = CommandType.StoredProcedure;
        //        _with2.CommandText = objWK.MyUserName + ".CUSTOMS_STATUS_TBL_PKG.CUSTOMS_CODE_MST_TBL_UPD";
        //        var _with3 = _with2.Parameters;
        //        updCommand.Parameters.Add("Customs_Code_Mst_Pk_IN", OracleDbType.Int32, 10, "Customs_Code_Mst_Pk").Direction = ParameterDirection.Input;
        //        updCommand.Parameters["Customs_Code_Mst_Pk_IN"].SourceVersion = DataRowVersion.Current;
        //        updCommand.Parameters.Add("Customs_Status_Code_IN", OracleDbType.Varchar2, 20, "Customs_Status_Code").Direction = ParameterDirection.Input;
        //        updCommand.Parameters["Customs_Status_Code_IN"].SourceVersion = DataRowVersion.Current;
        //        updCommand.Parameters.Add("Customs_Status_desc_IN", OracleDbType.Varchar2, 100, "Customs_Status_desc").Direction = ParameterDirection.Input;
        //        updCommand.Parameters["Customs_Status_desc_IN"].SourceVersion = DataRowVersion.Current;
        //        updCommand.Parameters.Add("COUNTRY_MST_FK_IN", OracleDbType.Varchar2, 2000, "countryPk").Direction = ParameterDirection.Input;
        //        updCommand.Parameters["COUNTRY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
        //        updCommand.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 10, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
        //        updCommand.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;
        //        updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 10, "VERSION_NO").Direction = ParameterDirection.Input;
        //        updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
        //        updCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
        //        updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
        //        updCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
        //        updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.NVarchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
        //        updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

        //        objWK.MyDataAdapter.RowUpdated += new OracleRowUpdatedEventHandler(OnRowUpdated);

        //        var _with4 = objWK.MyDataAdapter;
        //        _with4.InsertCommand = insCommand;
        //        _with4.InsertCommand.Transaction = TRAN;
        //        _with4.UpdateCommand = updCommand;
        //        _with4.UpdateCommand.Transaction = TRAN;
        //        RecAfct = _with4.Update(M_DataSet);
        //        if (arrMessage.Count > 0)
        //        {
        //            return arrMessage;
        //        }
        //        else
        //        {
        //            TRAN.Commit();
        //            arrMessage.Add("All Data Saved Successfully");
        //            return arrMessage;
        //        }
        //    }
        //    catch (OracleException oraexp)
        //    {
        //        throw oraexp;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //    finally
        //    {
        //        objWK.CloseConnection();
        //    }
        //}

        //public ArrayList SaveTrnInsert(DataTable C_DataTable)
        //{
        //    WorkFlow objWK = new WorkFlow();
        //    objWK.OpenConnection();
        //    OracleTransaction TRAN = null;
        //    OracleParameterCollection ColPara = new OracleParameterCollection();
        //    int intPKVal = 0;
        //    int i = 0;
        //    DataTable dt = new DataTable();
        //    dt = C_DataTable;

        //    long lngI = 0;
        //    Int32 RecAfct = default(Int32);
        //    OracleCommand insCommand = new OracleCommand();
        //    try
        //    {
        //        var _with5 = insCommand;
        //        _with5.Connection = objWK.MyConnection;
        //        _with5.CommandType = CommandType.StoredProcedure;
        //        _with5.CommandText = objWK.MyUserName + ".CUSTOMS_CODE_MST_TBL_PKG.CUSTOMS_STATUS_CODE_CHECK";
        //        for (i = 0; i <= dt.Rows.Count - 1; i++)
        //        {
        //            _with5.Parameters.Add("CUSTOMS_STATUS_CODE_IN", dt.Rows[i][0]);
        //            _with5.Parameters.Add("COUNTRY_ID_IN", dt.Rows[i][1]);
        //            _with5.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK);
        //            _with5.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK);
        //            _with5.Parameters.Add("ACTIVE_FLAG_IN", dt.Rows[i][3]);
        //            insCommand.ExecuteNonQuery();
        //            _with5.Parameters.Clear();
        //        }
        //        objWK.MyDataAdapter.RowUpdated += new OracleRowUpdatedEventHandler(OnRowUpdated);
        //        if (arrMessage.Count > 0)
        //        {
        //            return arrMessage;
        //        }
        //        else
        //        {
        //            return arrMessage;
        //        }
        //    }
        //    catch (OracleException oraexp)
        //    {
        //        throw oraexp;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //#endregion "Save Function"

        //#region "Fetch Currency"

        //public DataSet FetchCurrency(Int16 CurrencyPK = 0, string CurrencyID = "", string CurrencyName = "", bool ActiveOnly = true)
        //{
        //    string strSQL = null;
        //    strSQL = "select ' ' CURRENCY_ID,";
        //    strSQL = strSQL + "' ' CURRENCY_NAME, ";
        //    strSQL = strSQL + "0 CURRENCY_MST_PK ";
        //    strSQL = strSQL + "from CURRENCY_TYPE_MST_TBL ";
        //    strSQL = strSQL + "UNION ";
        //    strSQL = strSQL + "Select CURRENCY_ID, ";
        //    strSQL = strSQL + "CURRENCY_NAME,";
        //    strSQL = strSQL + "CURRENCY_MST_PK ";
        //    strSQL = strSQL + "from CURRENCY_TYPE_MST_TBL Where 1=1 ";
        //    if (ActiveOnly)
        //    {
        //        strSQL = strSQL + " And Active_Flag = 1  ";
        //    }
        //    strSQL = strSQL + "order by CURRENCY_ID";
        //    WorkFlow objWF = new WorkFlow();
        //    try
        //    {
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

        //#endregion "Fetch Currency"
    }
}