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
    public class clsDEPOT_MST_TBL : CommonFeatures
    {
        //    #region "Save Function"

        //    public ArrayList Save(DataSet M_DataSet)
        //    {
        //        WorkFlow objWK = new WorkFlow();
        //        objWK.OpenConnection();
        //        OracleTransaction TRAN = null;
        //        TRAN = objWK.MyConnection.BeginTransaction();

        //        OracleParameterCollection ColPara = new OracleParameterCollection();
        //        int intPKVal = 0;
        //        long lngI = 0;
        //        Int32 RecAfct = default(Int32);
        //        OracleCommand insCommand = new OracleCommand();
        //        OracleCommand updCommand = new OracleCommand();
        //        OracleCommand delCommand = new OracleCommand();

        //        try
        //        {
        //            var _with1 = insCommand;
        //            _with1.Connection = objWK.MyConnection;
        //            _with1.CommandType = CommandType.StoredProcedure;
        //            _with1.CommandText = objWK.MyUserName + ".DEPOT_MST_TBL_PKG.DEPOT_MST_TBL_INS";
        //            var _with2 = _with1.Parameters;

        //            insCommand.Parameters.Add("DEPOT_ID_IN", OracleDbType.Varchar2, 20, "DEPOT_ID").Direction = ParameterDirection.Input;
        //            insCommand.Parameters["DEPOT_ID_IN"].SourceVersion = DataRowVersion.Current;
        //            insCommand.Parameters.Add("DEPOT_NAME_IN", OracleDbType.Varchar2, 50, "DEPOT_NAME").Direction = ParameterDirection.Input;
        //            insCommand.Parameters["DEPOT_NAME_IN"].SourceVersion = DataRowVersion.Current;
        //            insCommand.Parameters.Add("ADDRESS_1_IN", OracleDbType.Varchar2, 50, "ADDRESS_1").Direction = ParameterDirection.Input;
        //            insCommand.Parameters["ADDRESS_1_IN"].SourceVersion = DataRowVersion.Current;
        //            insCommand.Parameters.Add("ADDRESS_2_IN", OracleDbType.Varchar2, 50, "ADDRESS_2").Direction = ParameterDirection.Input;
        //            insCommand.Parameters["ADDRESS_2_IN"].SourceVersion = DataRowVersion.Current;
        //            insCommand.Parameters.Add("ADDRESS_3_IN", OracleDbType.Varchar2, 50, "ADDRESS_3").Direction = ParameterDirection.Input;
        //            insCommand.Parameters["ADDRESS_3_IN"].SourceVersion = DataRowVersion.Current;
        //            insCommand.Parameters.Add("CITY_IN", OracleDbType.Varchar2, 100, "CITY").Direction = ParameterDirection.Input;
        //            insCommand.Parameters["CITY_IN"].SourceVersion = DataRowVersion.Current;
        //            insCommand.Parameters.Add("ZIP_CODE_IN", OracleDbType.Varchar2, 25, "ZIP_CODE").Direction = ParameterDirection.Input;
        //            insCommand.Parameters["ZIP_CODE_IN"].SourceVersion = DataRowVersion.Current;
        //            insCommand.Parameters.Add("PHONE_1_IN", OracleDbType.Varchar2, 25, "PHONE_1").Direction = ParameterDirection.Input;
        //            insCommand.Parameters["PHONE_1_IN"].SourceVersion = DataRowVersion.Current;
        //            insCommand.Parameters.Add("PHONE_2_IN", OracleDbType.Varchar2, 25, "PHONE_2").Direction = ParameterDirection.Input;
        //            insCommand.Parameters["PHONE_2_IN"].SourceVersion = DataRowVersion.Current;
        //            insCommand.Parameters.Add("FAX_IN", OracleDbType.Varchar2, 25, "FAX").Direction = ParameterDirection.Input;
        //            insCommand.Parameters["FAX_IN"].SourceVersion = DataRowVersion.Current;
        //            insCommand.Parameters.Add("EMAIL_IN", OracleDbType.Varchar2, 50, "EMAIL").Direction = ParameterDirection.Input;
        //            insCommand.Parameters["EMAIL_IN"].SourceVersion = DataRowVersion.Current;
        //            //**************************************************************************
        //            insCommand.Parameters.Add("FIRST_DAY_IN", OracleDbType.Int32, 1, "FIRST_DAY").Direction = ParameterDirection.Input;
        //            insCommand.Parameters["FIRST_DAY_IN"].SourceVersion = DataRowVersion.Current;
        //            insCommand.Parameters.Add("LAST_DAY_IN", OracleDbType.Int32, 1, "LAST_DAY").Direction = ParameterDirection.Input;
        //            insCommand.Parameters["LAST_DAY_IN"].SourceVersion = DataRowVersion.Current;

        //            //****************************************************************************
        //            insCommand.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "LOCATION_MST_FK").Direction = ParameterDirection.Input;
        //            insCommand.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
        //            insCommand.Parameters.Add("LIFT_ON_CHG_20_IN", OracleDbType.Int32, 10, "LIFT_ON_CHG_20").Direction = ParameterDirection.Input;
        //            insCommand.Parameters["LIFT_ON_CHG_20_IN"].SourceVersion = DataRowVersion.Current;
        //            insCommand.Parameters.Add("LIFT_ON_CHG_40_IN", OracleDbType.Int32, 10, "LIFT_ON_CHG_40").Direction = ParameterDirection.Input;
        //            insCommand.Parameters["LIFT_ON_CHG_40_IN"].SourceVersion = DataRowVersion.Current;
        //            insCommand.Parameters.Add("LIFT_OFF_CHG_20_IN", OracleDbType.Int32, 10, "LIFT_OFF_CHG_20").Direction = ParameterDirection.Input;
        //            insCommand.Parameters["LIFT_OFF_CHG_20_IN"].SourceVersion = DataRowVersion.Current;
        //            insCommand.Parameters.Add("LIFT_OFF_CHG_40_IN", OracleDbType.Int32, 10, "LIFT_OFF_CHG_40").Direction = ParameterDirection.Input;
        //            insCommand.Parameters["LIFT_OFF_CHG_40_IN"].SourceVersion = DataRowVersion.Current;
        //            insCommand.Parameters.Add("CURRNCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRNCY_MST_FK").Direction = ParameterDirection.Input;
        //            insCommand.Parameters["CURRNCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
        //            insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
        //            insCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
        //            insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 10, "DEPOT_MST_PK").Direction = ParameterDirection.Output;
        //            insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

        //            var _with3 = delCommand;
        //            _with3.Connection = objWK.MyConnection;
        //            _with3.CommandType = CommandType.StoredProcedure;
        //            _with3.CommandText = objWK.MyUserName + ".DEPOT_MST_TBL_PKG.DEPOT_MST_TBL_DEL";
        //            var _with4 = _with3.Parameters;
        //            delCommand.Parameters.Add("DEPOT_MST_PK_IN", OracleDbType.Int32, 10, "DEPOT_MST_PK").Direction = ParameterDirection.Input;
        //            delCommand.Parameters["DEPOT_MST_PK_IN"].SourceVersion = DataRowVersion.Current;
        //            delCommand.Parameters.Add("DELETED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
        //            delCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
        //            delCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
        //            delCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
        //            delCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "DEPOT_MST_PK").Direction = ParameterDirection.Output;
        //            delCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

        //            var _with5 = updCommand;
        //            _with5.Connection = objWK.MyConnection;
        //            _with5.CommandType = CommandType.StoredProcedure;
        //            _with5.CommandText = objWK.MyUserName + ".DEPOT_MST_TBL_PKG.DEPOT_MST_TBL_UPD";
        //            var _with6 = _with5.Parameters;

        //            updCommand.Parameters.Add("DEPOT_MST_PK_IN", OracleDbType.Int32, 10, "DEPOT_MST_PK").Direction = ParameterDirection.Input;
        //            updCommand.Parameters["DEPOT_MST_PK_IN"].SourceVersion = DataRowVersion.Current;
        //            updCommand.Parameters.Add("DEPOT_ID_IN", OracleDbType.Varchar2, 20, "DEPOT_ID").Direction = ParameterDirection.Input;
        //            updCommand.Parameters["DEPOT_ID_IN"].SourceVersion = DataRowVersion.Current;
        //            updCommand.Parameters.Add("DEPOT_NAME_IN", OracleDbType.Varchar2, 50, "DEPOT_NAME").Direction = ParameterDirection.Input;
        //            updCommand.Parameters["DEPOT_NAME_IN"].SourceVersion = DataRowVersion.Current;
        //            updCommand.Parameters.Add("ADDRESS_1_IN", OracleDbType.Varchar2, 50, "ADDRESS_1").Direction = ParameterDirection.Input;
        //            updCommand.Parameters["ADDRESS_1_IN"].SourceVersion = DataRowVersion.Current;
        //            updCommand.Parameters.Add("ADDRESS_2_IN", OracleDbType.Varchar2, 50, "ADDRESS_2").Direction = ParameterDirection.Input;
        //            updCommand.Parameters["ADDRESS_2_IN"].SourceVersion = DataRowVersion.Current;
        //            updCommand.Parameters.Add("ADDRESS_3_IN", OracleDbType.Varchar2, 50, "ADDRESS_3").Direction = ParameterDirection.Input;
        //            updCommand.Parameters["ADDRESS_3_IN"].SourceVersion = DataRowVersion.Current;
        //            updCommand.Parameters.Add("CITY_IN", OracleDbType.Varchar2, 50, "CITY").Direction = ParameterDirection.Input;
        //            updCommand.Parameters["CITY_IN"].SourceVersion = DataRowVersion.Current;
        //            updCommand.Parameters.Add("ZIP_CODE_IN", OracleDbType.Varchar2, 25, "ZIP_CODE").Direction = ParameterDirection.Input;
        //            updCommand.Parameters["ZIP_CODE_IN"].SourceVersion = DataRowVersion.Current;
        //            updCommand.Parameters.Add("PHONE_1_IN", OracleDbType.Varchar2, 25, "PHONE_1").Direction = ParameterDirection.Input;
        //            updCommand.Parameters["PHONE_1_IN"].SourceVersion = DataRowVersion.Current;
        //            updCommand.Parameters.Add("PHONE_2_IN", OracleDbType.Varchar2, 25, "PHONE_2").Direction = ParameterDirection.Input;
        //            updCommand.Parameters["PHONE_2_IN"].SourceVersion = DataRowVersion.Current;
        //            updCommand.Parameters.Add("FAX_IN", OracleDbType.Varchar2, 25, "FAX").Direction = ParameterDirection.Input;
        //            updCommand.Parameters["FAX_IN"].SourceVersion = DataRowVersion.Current;
        //            updCommand.Parameters.Add("EMAIL_IN", OracleDbType.Varchar2, 25, "EMAIL").Direction = ParameterDirection.Input;
        //            updCommand.Parameters["EMAIL_IN"].SourceVersion = DataRowVersion.Current;

        //            updCommand.Parameters.Add("FIRST_DAY_IN", OracleDbType.Int32, 1, "FIRST_DAY").Direction = ParameterDirection.Input;
        //            updCommand.Parameters["FIRST_DAY_IN"].SourceVersion = DataRowVersion.Current;
        //            updCommand.Parameters.Add("LAST_DAY_IN", OracleDbType.Int32, 1, "LAST_DAY").Direction = ParameterDirection.Input;
        //            updCommand.Parameters["LAST_DAY_IN"].SourceVersion = DataRowVersion.Current;

        //            updCommand.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "LOCATION_MST_FK").Direction = ParameterDirection.Input;
        //            updCommand.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
        //            updCommand.Parameters.Add("LIFT_ON_CHG_20_IN", OracleDbType.Int32, 10, "LIFT_ON_CHG_20").Direction = ParameterDirection.Input;
        //            updCommand.Parameters["LIFT_ON_CHG_20_IN"].SourceVersion = DataRowVersion.Current;
        //            updCommand.Parameters.Add("LIFT_ON_CHG_40_IN", OracleDbType.Int32, 10, "LIFT_ON_CHG_40").Direction = ParameterDirection.Input;
        //            updCommand.Parameters["LIFT_ON_CHG_40_IN"].SourceVersion = DataRowVersion.Current;
        //            updCommand.Parameters.Add("LIFT_OFF_CHG_20_IN", OracleDbType.Int32, 10, "LIFT_OFF_CHG_20").Direction = ParameterDirection.Input;
        //            updCommand.Parameters["LIFT_OFF_CHG_20_IN"].SourceVersion = DataRowVersion.Current;
        //            updCommand.Parameters.Add("LIFT_OFF_CHG_40_IN", OracleDbType.Int32, 10, "LIFT_OFF_CHG_40").Direction = ParameterDirection.Input;
        //            updCommand.Parameters["LIFT_OFF_CHG_40_IN"].SourceVersion = DataRowVersion.Current;
        //            updCommand.Parameters.Add("CURRNCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRNCY_MST_FK").Direction = ParameterDirection.Input;
        //            updCommand.Parameters["CURRNCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
        //            updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
        //            updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
        //            updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
        //            updCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
        //            updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "DEPOT_MST_PK").Direction = ParameterDirection.Output;
        //            updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

        //            objWK.MyDataAdapter.RowUpdated += new OracleRowUpdatedEventHandler(OnRowUpdated);

        //            //objWK.MyDataAdapter.ContinueUpdateOnError = True
        //            var _with7 = objWK.MyDataAdapter;

        //            _with7.InsertCommand = insCommand;
        //            _with7.InsertCommand.Transaction = TRAN;
        //            _with7.UpdateCommand = updCommand;
        //            _with7.UpdateCommand.Transaction = TRAN;
        //            _with7.DeleteCommand = delCommand;
        //            _with7.DeleteCommand.Transaction = TRAN;
        //            RecAfct = _with7.Update(M_DataSet);
        //            TRAN.Commit();
        //            if (arrMessage.Count > 0)
        //            {
        //                return arrMessage;
        //            }
        //            else
        //            {
        //                arrMessage.Add("All Data Saved Successfully");
        //                return arrMessage;
        //            }
        //        }
        //        catch (OracleException oraexp)
        //        {
        //            throw oraexp;
        //        }
        //        catch (Exception ex)
        //        {
        //            throw ex;
        //        }
        //        finally
        //        {
        //            objWK.CloseConnection();
        //        }
        //    }

        //    #endregion "Save Function"

        //    #region "Class Level variable"

        //    private enum Header
        //    {
        //        DEPOT_STORAGE_SLAB_PK = 0,
        //        SLNO = 1,
        //        From_days = 2,
        //        To_Days = 3,
        //        RATE_20 = 4,
        //        RATE_40 = 5,
        //        Del_Flag = 6,
        //        CHEFLAG = 7
        //    }

        //    #endregion "Class Level variable"

        //    #region "Fetch PK"

        //    public DataSet FetchPK(long pk)
        //    {
        //        WorkFlow objWF = new WorkFlow();
        //        string strSQL = null;
        //        strSQL = "Select Depot_Mst_Pk,";
        //        strSQL = strSQL + " Depot_Id,";
        //        strSQL = strSQL + " Depot_Name,";
        //        strSQL = strSQL + " Address_1,";
        //        strSQL = strSQL + " Address_2,";
        //        strSQL = strSQL + " Address_3,";
        //        strSQL = strSQL + " City,";
        //        strSQL = strSQL + " Zip_Code,";
        //        strSQL = strSQL + " Phone_1,";
        //        strSQL = strSQL + " Phone_2,";
        //        strSQL = strSQL + " Fax,";
        //        strSQL = strSQL + " Email,";
        //        strSQL = strSQL + " Location_Mst_Fk,";
        //        strSQL = strSQL + " Lift_On_Chg_20,";
        //        strSQL = strSQL + " Lift_On_Chg_40,";
        //        strSQL = strSQL + " Lift_Off_Chg_20,";
        //        strSQL = strSQL + " Lift_Off_Chg_40,";
        //        strSQL = strSQL + " Currncy_Mst_Fk,";
        //        strSQL = strSQL + " Version_No ";
        //        strSQL = strSQL + " FROM DEPOT_MST_TBL ";
        //        strSQL = strSQL + " Where Depot_MSt_Pk = " + pk;
        //        //strSQL = "Select * From Depot_Mst_Tbl Where Depot_MSt_Pk=" & pk
        //        try
        //        {
        //            return objWF.GetDataSet(strSQL);
        //            //Catch sqlExp As OracleException
        //            //Modified by Manjunath  PTS ID:Sep-02  14/09/2011
        //        }
        //        catch (OracleException sqlExp)
        //        {
        //            ErrorMessage = sqlExp.Message;
        //            throw sqlExp;
        //        }
        //        catch (Exception exp)
        //        {
        //            ErrorMessage = exp.Message;
        //            throw exp;
        //        }
        //    }

        //    #endregion "Fetch PK"

        //    #region "Fetch Function"

        //    public DataSet FetchAll(Int16 P_Depot_Mst_Pk = 0, string P_Depot_Id = "", string P_Depot_Name = "", string SearchType = "", string SortExpression = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int16 SortCol = 2)
        //    {
        //        Int32 last = default(Int32);
        //        Int32 start = default(Int32);
        //        string strCondition = null;
        //        Int32 TotalRecords = default(Int32);
        //        WorkFlow objWF = new WorkFlow();
        //        string strSQL = null;

        //        //If P_Depot_Mst_Pk > 0 Then
        //        //    strCondition = strCondition & " And Depot_Mst_Pk = " & P_Depot_Mst_Pk & " "
        //        //End If

        //        if (P_Depot_Id.ToString().Trim().Length > 0)
        //        {
        //            if (SearchType == "C")
        //            {
        //                strCondition = strCondition + " And upper(Depot_Id) like '%" + P_Depot_Id.ToUpper().Replace("'", "''") + "%' ";
        //            }
        //            else
        //            {
        //                strCondition = strCondition + " And upper(Depot_Id) like '" + P_Depot_Id.ToUpper().Replace("'", "''") + "%' ";
        //            }
        //        }

        //        //If P_Depot_Name.ToString.Trim.Length > 0 Then
        //        //    If SearchType = "C" Then
        //        //        strCondition = strCondition & " And Depot_Name like '%" & P_Depot_Name & "%' "
        //        //    Else
        //        //        strCondition = strCondition & " And Depot_Name like '" & P_Depot_Name & "%' "
        //        //    End If
        //        //Else
        //        //End If

        //        if (P_Depot_Name.Trim().Length > 0)
        //        {
        //            if (SearchType == "S")
        //            {
        //                strCondition += " AND UPPER(Depot_Name) LIKE '" + P_Depot_Name.ToUpper().Replace("'", "''") + "%'";
        //            }
        //            else
        //            {
        //                strCondition += " AND UPPER(Depot_Name) LIKE '%" + P_Depot_Name.ToUpper().Replace("'", "''") + "%'";
        //            }
        //        }

        //        strSQL = "SELECT Count(*) from Depot_Mst_Tbl";
        //        strSQL = strSQL + "WHERE ( 1 = 1) ";
        //        strSQL += strCondition;
        //        //TotalRecords = CType(objWF.ExecuteScaler(strSQL), Int32)
        //        TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));

        //        TotalPage = TotalRecords / RecordsPerPage;
        //        if (TotalRecords % RecordsPerPage != 0)
        //        {
        //            TotalPage += 1;
        //        }
        //        if (CurrentPage > TotalPage)
        //        {
        //            CurrentPage = 1;
        //        }
        //        if (TotalRecords == 0)
        //        {
        //            CurrentPage = 0;
        //        }
        //        last = CurrentPage * RecordsPerPage;
        //        start = (CurrentPage - 1) * RecordsPerPage + 1;

        //        if (Convert.ToInt32(SortCol) > 0)
        //        {
        //            strCondition = strCondition + " order by " + Convert.ToInt32(SortCol);
        //        }

        //        strSQL = "  Select * From ( ";
        //        strSQL = strSQL + "SELECT ROWNUM SR_NO,q.* From ( Select";
        //        strSQL = strSQL + " dep.depot_mst_pk,";
        //        strSQL = strSQL + "dep.depot_id, ";
        //        strSQL = strSQL + "dep.depot_name,";
        //        strSQL = strSQL + " dep.version_no ";
        //        strSQL = strSQL + " FROM Depot_Mst_Tbl Dep";
        //        strSQL = strSQL + " WHERE ( 1 = 1) ";

        //        strSQL += strCondition;

        //        strSQL = strSQL + " )q ) WHERE SR_NO  Between " + start + " and " + last;

        //        try
        //        {
        //            return objWF.GetDataSet(strSQL);
        //            //Catch sqlExp As OracleException
        //            //Modified by Manjunath  PTS ID:Sep-02  14/09/2011
        //        }
        //        catch (OracleException sqlExp)
        //        {
        //            ErrorMessage = sqlExp.Message;
        //            throw sqlExp;
        //        }
        //        catch (Exception exp)
        //        {
        //            ErrorMessage = exp.Message;
        //            throw exp;
        //        }
        //    }

        //    #endregion "Fetch Function"

        //    #region "FetchSelected"

        //    public DataSet FetchDepotDtls(long DepotPK)
        //    {
        //        string strSQL = null;
        //        strSQL = string.Empty;
        //        strSQL += "select ";
        //        strSQL += "depot.DEPOT_MST_PK,  ";
        //        strSQL += "depot.DEPOT_ID,  ";
        //        strSQL += "depot.DEPOT_NAME,";
        //        strSQL += "depot.ADDRESS_1,  ";
        //        strSQL += "depot.ADDRESS_2,  ";
        //        strSQL += "depot.ADDRESS_3,  ";
        //        strSQL += "depot.CITY,  ";
        //        strSQL += "depot.ZIP_CODE,  ";
        //        strSQL += "depot.PHONE_1,  ";
        //        strSQL += "depot.PHONE_2,  ";
        //        strSQL += "depot.FAX,  ";
        //        strSQL += "depot.EMAIL,  ";
        //        strSQL += "depot.LIFT_ON_CHG_20,  ";
        //        strSQL += "depot.LIFT_ON_CHG_40,  ";
        //        strSQL += "depot.LIFT_OFF_CHG_20,  ";
        //        strSQL += "depot.LIFT_OFF_CHG_40,  ";
        //        strSQL += "depot.FIRST_DAY,  ";
        //        strSQL += "depot.LAST_DAY,   ";
        //        strSQL += "depot.CURRNCY_MST_FK,  ";
        //        strSQL += "depot.location_mst_fk,depot.Version_No,  ";
        //        strSQL += "loc.location_id,";
        //        strSQL += "loc.location_name,";
        //        strSQL += "Currency.CURRENCY_ID";
        //        strSQL += " FROM DEPOT_MST_TBL depot,";
        //        strSQL += " LOCATION_MST_TBL loc,";
        //        strSQL += "CURRENCY_TYPE_MST_TBL Currency";
        //        strSQL += " WHERE depot.LOCATION_MST_FK=loc.LOCATION_MST_PK";
        //        strSQL += " AND depot.CURRNCY_MST_FK=Currency.CURRENCY_MST_PK";
        //        strSQL += " and depot.DEPOT_MST_PK = " + DepotPK;

        //        WorkFlow objWF = new WorkFlow();
        //        try
        //        {
        //            return objWF.GetDataSet(strSQL);
        //            //Catch sqlExp As OracleException
        //            //Modified by Manjunath  PTS ID:Sep-02  14/09/2011
        //        }
        //        catch (OracleException sqlExp)
        //        {
        //            ErrorMessage = sqlExp.Message;
        //            throw sqlExp;
        //        }
        //        catch (Exception exp)
        //        {
        //            ErrorMessage = exp.Message;
        //            throw exp;
        //        }
        //    }

        //    #endregion "FetchSelected"

        //    #region "Fetchstoragelist"

        //    public DataSet Fetchstoragelist(long depotstorage)
        //    {
        //        string strSQl = null;

        //        strSQl = string.Empty;
        //        strSQl += "SELECT rownum SR_NO,DEPOT_STORAGE_SLAB_PK,";
        //        strSQl += " FROM_DAYS, ";
        //        strSQl += " TO_DAYS, ";
        //        strSQl += " RATE_20, ";
        //        strSQl += " RATE_40, VERSION_NO ";
        //        strSQl += "  from depot_storage_slab_trn depotstore";
        //        strSQl += " where depotstore.Depot_Mst_Fk = " + depotstorage;
        //        strSQl += " order by from_days ";
        //        WorkFlow objWF = new WorkFlow();
        //        try
        //        {
        //            return objWF.GetDataSet(strSQl);
        //            //Catch sqlExp As OracleException
        //            //Modified by Manjunath  PTS ID:Sep-02  14/09/2011
        //        }
        //        catch (OracleException sqlExp)
        //        {
        //            ErrorMessage = sqlExp.Message;
        //            throw sqlExp;
        //        }
        //        catch (Exception exp)
        //        {
        //            ErrorMessage = exp.Message;
        //            throw exp;
        //        }
        //    }

        //    #endregion "Fetchstoragelist"

        //    #region "Insert Function"

        //    public ArrayList InsertDepots(string DepotID, string strDepotName, string straddress1, string straddress2, string strcity, string strzipcode, string stremail, Int16 ifirstday, Int16 ilastday, long lnglocationfk,
        //    long lnglifton20, long lnglifton40, long lngliftoff20, long lngliftoff40, long lngcurrencyfk, DataSet M_Dataset, string strphone1 = "", string strphone2 = "", string strfax = "", string address3 = "")
        //    {
        //        OracleParameterCollection ColPara = new OracleParameterCollection();
        //        int intPKVal = 0;
        //        long lngI = 0;
        //        DateTime EnteredBudgetStartDate = default(DateTime);
        //        Int32 RecAfct = default(Int32);
        //        System.DBNull StrNull = null;
        //        long lngBudHdrPK = 0;
        //        OracleCommand insCommand = new OracleCommand();
        //        System.DateTime EnteredDate = default(System.DateTime);
        //        Int32 inti = default(Int32);
        //        long lngDepotPK = 0;

        //        WorkFlow objWK = new WorkFlow();
        //        objWK.OpenConnection();
        //        OracleTransaction TRAN = null;
        //        TRAN = objWK.MyConnection.BeginTransaction();

        //        try
        //        {
        //            DataTable DtTbl = new DataTable();
        //            DataRow DtRw = null;
        //            int i = 0;
        //            DtTbl = M_Dataset.Tables[0];
        //            var _with8 = insCommand;
        //            _with8.Connection = objWK.MyConnection;
        //            _with8.CommandType = CommandType.StoredProcedure;
        //            _with8.CommandText = objWK.MyUserName + ".DEPOT_MST_TBL_PKG.DEPOT_MST_TBL_INS";

        //            var _with9 = _with8.Parameters;
        //            _with9.Add("DEPOT_ID_IN", DepotID).Direction = ParameterDirection.Input;

        //            _with9.Add("DEPOT_NAME_IN", strDepotName).Direction = ParameterDirection.Input;

        //            _with9.Add("ADDRESS_1_IN", straddress1).Direction = ParameterDirection.Input;

        //            _with9.Add("ADDRESS_2_IN", straddress2).Direction = ParameterDirection.Input;

        //            _with9.Add("ADDRESS_3_IN", address3).Direction = ParameterDirection.Input;

        //            _with9.Add("CITY_IN", strcity).Direction = ParameterDirection.Input;

        //            _with9.Add("ZIP_CODE_IN", strzipcode).Direction = ParameterDirection.Input;

        //            _with9.Add("PHONE_1_IN", strphone1).Direction = ParameterDirection.Input;

        //            _with9.Add("PHONE_2_IN", strphone2).Direction = ParameterDirection.Input;

        //            _with9.Add("FAX_IN", strfax).Direction = ParameterDirection.Input;

        //            _with9.Add("EMAIL_IN", stremail).Direction = ParameterDirection.Input;
        //            _with9.Add("FIRST_DAY_IN", ifirstday).Direction = ParameterDirection.Input;
        //            _with9.Add("LAST_DAY_IN", ilastday).Direction = ParameterDirection.Input;

        //            _with9.Add("LOCATION_MST_FK_IN", lnglocationfk).Direction = ParameterDirection.Input;

        //            _with9.Add("LIFT_ON_CHG_20_IN", lnglifton20).Direction = ParameterDirection.Input;

        //            _with9.Add("LIFT_ON_CHG_40_IN", lnglifton40).Direction = ParameterDirection.Input;

        //            _with9.Add("LIFT_OFF_CHG_20_IN", lngliftoff20).Direction = ParameterDirection.Input;

        //            _with9.Add("LIFT_OFF_CHG_40_IN", lngliftoff40).Direction = ParameterDirection.Input;

        //            _with9.Add("CURRNCY_MST_FK_IN", lngcurrencyfk).Direction = ParameterDirection.Input;

        //            _with9.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;

        //            _with9.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

        //            _with9.Add("RETURN_VALUE", OracleDbType.Varchar2, 10, "DEPOT_MST_PK").Direction = ParameterDirection.Output;
        //            insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

        //            objWK.MyDataAdapter.RowUpdated += new OracleRowUpdatedEventHandler(OnRowUpdated);

        //            var _with10 = objWK.MyDataAdapter;
        //            _with10.InsertCommand = insCommand;
        //            _with10.InsertCommand.Transaction = TRAN;
        //            _with10.InsertCommand.ExecuteNonQuery();
        //        }
        //        catch (Exception ex)
        //        {
        //            arrMessage.Add(ex.Message);
        //            TRAN.Rollback();
        //            return arrMessage;
        //            //Return 1
        //        }

        //        lngDepotPK = insCommand.Parameters["RETURN_VALUE"].Value;
        //        //If lngDepotPK = 0 Then
        //        //    lngDepotPK = CType(insCommand.Parameters["RETURN_VALUE"].Value, Long)
        //        //End If

        //        try
        //        {
        //            var _with11 = insCommand;
        //            _with11.CommandType = CommandType.StoredProcedure;
        //            _with11.CommandText = objWK.MyUserName + ".DEPOT_STORAGE_SLAB_TRN_PKG.DEPOT_STORAGE_SLAB_TRN_INS";

        //            for (inti = 0; inti <= M_Dataset.Tables[0].Rows.Count - 1; inti++)
        //            {
        //                var _with12 = _with11.Parameters;

        //                _with12.Clear();

        //                //If Not (IsDBNull(M_Dataset.Tables(0).Rows(inti).Item("SLNO"))) Then
        //                //    insCommand.Parameters.Add("SLNO_IN", M_Dataset.Tables(0).Rows(inti).Item("SLNO")).Direction = ParameterDirection.Input
        //                //Else
        //                //insCommand.Parameters.Add("SLNO_IN", "").Direction = ParameterDirection.Input
        //                //End If

        //                insCommand.Parameters.Add("DEPOT_MST_FK_IN", lngDepotPK).Direction = ParameterDirection.Input;

        //                if (!(string.IsNullOrEmpty(M_Dataset.Tables[0].Rows[inti]["From_days"])))
        //                {
        //                    insCommand.Parameters.Add("FROM_DAYS_IN", M_Dataset.Tables[0].Rows[inti]["From_days"]).Direction = ParameterDirection.Input;
        //                }
        //                else
        //                {
        //                    insCommand.Parameters.Add("FROM_DAYS_IN", "").Direction = ParameterDirection.Input;
        //                }

        //                if (!(string.IsNullOrEmpty(M_Dataset.Tables[0].Rows[inti]["To_Days"])))
        //                {
        //                    insCommand.Parameters.Add("TO_DAYS_IN", M_Dataset.Tables[0].Rows[inti]["To_Days"]).Direction = ParameterDirection.Input;
        //                }
        //                else
        //                {
        //                    insCommand.Parameters.Add("TO_DAYS_IN", "").Direction = ParameterDirection.Input;
        //                }

        //                if (!(string.IsNullOrEmpty(M_Dataset.Tables[0].Rows[inti]["RATE_20"])))
        //                {
        //                    insCommand.Parameters.Add("RATE_20_IN", M_Dataset.Tables[0].Rows[inti]["RATE_20"]).Direction = ParameterDirection.Input;
        //                }
        //                else
        //                {
        //                    insCommand.Parameters.Add("RATE_20_IN", "").Direction = ParameterDirection.Input;
        //                }

        //                if (!(string.IsNullOrEmpty(M_Dataset.Tables[0].Rows[inti]["RATE_40"])))
        //                {
        //                    insCommand.Parameters.Add("RATE_40_IN", M_Dataset.Tables[0].Rows[inti]["RATE_40"]).Direction = ParameterDirection.Input;
        //                }
        //                else
        //                {
        //                    insCommand.Parameters.Add("RATE_40_IN", "").Direction = ParameterDirection.Input;
        //                }

        //                _with12.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
        //                _with12.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

        //                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "DEPOT_STORAGE_SLAB_TRN_PK").Direction = ParameterDirection.Output;
        //                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

        //                //.Add("DEPOT_MST_FK_IN", lngDepotPK).Direction = ParameterDirection.Input

        //                //.Add("SLNO_IN", M_Dataset.Tables(0).Rows(inti).Item(Header.SLNO)).Direction = ParameterDirection.Input
        //                //'.Add("PERIOD_ID_IN", M_Dataset.Tables(0).Rows(inti).Item(Header.PERIODID)).Direction = ParameterDirection.Input

        //                //.Add("FROM_DAYS_IN", M_Dataset.Tables(0).Rows(inti).Item(Header.From_days)).Direction = ParameterDirection.Input

        //                //.Add("TO_DAYS_IN", M_Dataset.Tables(0).Rows(inti).Item(Header.To_Days)).Direction = ParameterDirection.Input

        //                //.Add("RATE_20_IN", M_Dataset.Tables(0).Rows(inti).Item(Header.Storage20)).Direction = ParameterDirection.Input

        //                //.Add("RATE_40_IN", M_Dataset.Tables(0).Rows(inti).Item(Header.storage40)).Direction = ParameterDirection.Input

        //                //.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input
        //                //.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input

        //                //insCommand.Parameters.Add("RETURN_VALUE", OracleClient.OracleDbType.Int32, 10, "DEPOT_STORAGE_SLAB_TRN_PK").Direction = ParameterDirection.Output
        //                //insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current

        //                objWK.MyDataAdapter.RowUpdated += new OracleRowUpdatedEventHandler(OnRowUpdated);

        //                try
        //                {
        //                    var _with13 = objWK.MyDataAdapter;
        //                    _with13.InsertCommand = insCommand;
        //                    _with13.InsertCommand.Transaction = TRAN;
        //                    _with13.InsertCommand.ExecuteNonQuery();
        //                    //Manjunath  PTS ID:Sep-02  14/09/2011
        //                }
        //                catch (OracleException OraExp)
        //                {
        //                    throw OraExp;
        //                }
        //                catch (Exception ex)
        //                {
        //                    //Message1.GeneralError(90000002)
        //                    arrMessage.Add(ex.Message);
        //                    TRAN.Rollback();
        //                    return arrMessage;
        //                    //Return 1
        //                }
        //            }
        //            TRAN.Commit();
        //            arrMessage.Add("All Data Saved Successfully");
        //            arrMessage.Add(lngDepotPK);
        //            return arrMessage;
        //            //Manjunath  PTS ID:Sep-02  14/09/2011
        //        }
        //        catch (OracleException OraExp)
        //        {
        //            throw OraExp;
        //        }
        //        catch (Exception ex)
        //        {
        //            arrMessage.Add(ex.Message);
        //            TRAN.Rollback();
        //            return arrMessage;
        //            //Return 1
        //            //Manjunath  PTS ID:Sep-02  14/09/2011
        //        }
        //        finally
        //        {
        //            objWK.CloseConnection();
        //        }
        //    }

        //    #endregion "Insert Function"

        //    #region "Update Function"

        //    public ArrayList Updatedepot(long DeptPk, string DepotID, string strDepotName, string straddress1, string straddress2, string strcity, string strzipcode, string stremail, Int16 ifirstday, Int16 ilastday,
        //    long lnglocationfk, double lnglifton20, double lnglifton40, double lngliftoff20, double lngliftoff40, long lngcurrencyfk, long iVersion, DataSet M_Dataset, string strphone1 = "", string strphone2 = "",
        //    string strfax = "", string address3 = "")
        //    {
        //        long lngDepotPK = 0;
        //        OracleParameterCollection ColPara = new OracleParameterCollection();
        //        int intPKVal = 0;
        //        long lngI = 0;
        //        DateTime EnteredBudgetStartDate = default(DateTime);
        //        Int32 RecAfct = default(Int32);
        //        System.DBNull StrNull = null;
        //        long lngBudHdrPK = 0;
        //        OracleCommand insCommand = new OracleCommand();
        //        System.DateTime EnteredDate = default(System.DateTime);
        //        Int32 inti = default(Int32);

        //        WorkFlow objWK = new WorkFlow();
        //        objWK.OpenConnection();
        //        OracleTransaction TRAN = null;
        //        OracleCommand updCommand = new OracleCommand();
        //        TRAN = objWK.MyConnection.BeginTransaction();

        //        try
        //        {
        //            DataTable DtTbl = new DataTable();
        //            DataRow DtRw = null;
        //            int i = 0;

        //            DtTbl = M_Dataset.Tables[0];

        //            var _with14 = updCommand;
        //            _with14.Connection = objWK.MyConnection;
        //            _with14.CommandType = CommandType.StoredProcedure;
        //            _with14.CommandText = objWK.MyUserName + ".DEPOT_MST_TBL_PKG.DEPOT_MST_TBL_UPD";

        //            var _with15 = _with14.Parameters;
        //            _with15.Add("DEPOT_MST_PK_IN", DeptPk).Direction = ParameterDirection.Input;
        //            _with15.Add("DEPOT_ID_IN", DepotID).Direction = ParameterDirection.Input;

        //            _with15.Add("DEPOT_NAME_IN", strDepotName).Direction = ParameterDirection.Input;

        //            _with15.Add("ADDRESS_1_IN", straddress1).Direction = ParameterDirection.Input;

        //            _with15.Add("ADDRESS_2_IN", straddress2).Direction = ParameterDirection.Input;

        //            _with15.Add("ADDRESS_3_IN", address3).Direction = ParameterDirection.Input;

        //            _with15.Add("CITY_IN", strcity).Direction = ParameterDirection.Input;

        //            _with15.Add("ZIP_CODE_IN", strzipcode).Direction = ParameterDirection.Input;

        //            _with15.Add("PHONE_1_IN", strphone1).Direction = ParameterDirection.Input;

        //            _with15.Add("PHONE_2_IN", strphone2).Direction = ParameterDirection.Input;

        //            _with15.Add("FAX_IN", strfax).Direction = ParameterDirection.Input;

        //            _with15.Add("EMAIL_IN", stremail).Direction = ParameterDirection.Input;
        //            _with15.Add("FIRST_DAY_IN", ifirstday).Direction = ParameterDirection.Input;
        //            _with15.Add("LAST_DAY_IN", ilastday).Direction = ParameterDirection.Input;

        //            _with15.Add("LOCATION_MST_FK_IN", lnglocationfk).Direction = ParameterDirection.Input;

        //            _with15.Add("LIFT_ON_CHG_20_IN", lnglifton20).Direction = ParameterDirection.Input;

        //            _with15.Add("LIFT_ON_CHG_40_IN", lnglifton40).Direction = ParameterDirection.Input;

        //            _with15.Add("LIFT_OFF_CHG_20_IN", lngliftoff20).Direction = ParameterDirection.Input;

        //            _with15.Add("LIFT_OFF_CHG_40_IN", lngliftoff40).Direction = ParameterDirection.Input;

        //            _with15.Add("CURRNCY_MST_FK_IN", lngcurrencyfk).Direction = ParameterDirection.Input;

        //            _with15.Add("LAST_MODIFIED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;

        //            _with15.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
        //            _with15.Add("VERSION_NO_IN", iVersion).Direction = ParameterDirection.Input;
        //            _with15.Add("RETURN_VALUE", OracleDbType.Varchar2, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
        //            updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

        //            objWK.MyDataAdapter.RowUpdated += new OracleRowUpdatedEventHandler(OnRowUpdated);

        //            var _with16 = objWK.MyDataAdapter;
        //            _with16.UpdateCommand = updCommand;
        //            _with16.UpdateCommand = updCommand;
        //            _with16.UpdateCommand.Transaction = TRAN;
        //            _with16.UpdateCommand.ExecuteNonQuery();
        //        }
        //        catch (Exception ex)
        //        {
        //            arrMessage.Add(ex.Message);
        //            TRAN.Rollback();
        //            return arrMessage;
        //            //Return 1
        //        }

        //        lngDepotPK = updCommand.Parameters["RETURN_VALUE"].Value;
        //        return SAve_C(TRAN, lngDepotPK, M_Dataset);
        //    }

        //    public ArrayList SAve_C(OracleTransaction Tran, long lngDepotPK, DataSet M_Dataset)
        //    {
        //        WorkFlow objWK = new WorkFlow();
        //        objWK.MyConnection = Tran.Connection;
        //        long recf = 0;
        //        try
        //        {
        //            OracleCommand updCommand1 = new OracleCommand();
        //            OracleCommand delCommand1 = new OracleCommand();
        //            OracleCommand insCommand1 = new OracleCommand();

        //            var _with17 = insCommand1;
        //            _with17.Connection = objWK.MyConnection;
        //            _with17.CommandType = CommandType.StoredProcedure;
        //            _with17.CommandText = objWK.MyUserName + ".DEPOT_STORAGE_SLAB_TRN_PKG.DEPOT_STORAGE_SLAB_TRN_INS";
        //            var _with18 = _with17.Parameters;

        //            insCommand1.Parameters.Add("DEPOT_MST_FK_IN", lngDepotPK).Direction = ParameterDirection.Input;
        //            //insCommand1.Parameters["DEPOT_MST_FK_IN"].SourceVersion = DataRowVersion.Current
        //            insCommand1.Parameters.Add("FROM_DAYS_IN", OracleDbType.Int32, 10, "FROM_DAYS").Direction = ParameterDirection.Input;
        //            insCommand1.Parameters["FROM_DAYS_IN"].SourceVersion = DataRowVersion.Current;
        //            insCommand1.Parameters.Add("TO_DAYS_IN", OracleDbType.Int32, 10, "TO_DAYS").Direction = ParameterDirection.Input;
        //            insCommand1.Parameters["TO_DAYS_IN"].SourceVersion = DataRowVersion.Current;
        //            insCommand1.Parameters.Add("RATE_20_IN", OracleDbType.Int32, 10, "RATE_20").Direction = ParameterDirection.Input;
        //            insCommand1.Parameters["RATE_20_IN"].SourceVersion = DataRowVersion.Current;
        //            insCommand1.Parameters.Add("RATE_40_IN", OracleDbType.Int32, 10, "RATE_40").Direction = ParameterDirection.Input;
        //            insCommand1.Parameters["RATE_40_IN"].SourceVersion = DataRowVersion.Current;
        //            insCommand1.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
        //            insCommand1.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
        //            //insCommand1.Parameters["CONFIG_MST_FK_IN"].SourceVersion = DataRowVersion.Current
        //            insCommand1.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
        //            insCommand1.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

        //            var _with19 = delCommand1;
        //            _with19.Connection = objWK.MyConnection;
        //            _with19.CommandType = CommandType.StoredProcedure;
        //            _with19.CommandText = objWK.MyUserName + ".DEPOT_STORAGE_SLAB_TRN_PKG.DEPOT_STORAGE_SLAB_TRN_DEL";
        //            var _with20 = _with19.Parameters;
        //            delCommand1.Parameters.Add("DEPOT_STORAGE_SLAB_PK_IN", OracleDbType.Int32, 10, "DEPOT_STORAGE_SLAB_PK").Direction = ParameterDirection.Input;
        //            delCommand1.Parameters["DEPOT_STORAGE_SLAB_PK_IN"].SourceVersion = DataRowVersion.Current;
        //            delCommand1.Parameters.Add("DELETED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
        //            delCommand1.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
        //            delCommand1.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
        //            delCommand1.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
        //            delCommand1.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
        //            delCommand1.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

        //            var _with21 = updCommand1;
        //            _with21.Connection = objWK.MyConnection;
        //            _with21.CommandType = CommandType.StoredProcedure;
        //            _with21.CommandText = objWK.MyUserName + ".DEPOT_STORAGE_SLAB_TRN_PKG.DEPOT_STORAGE_SLAB_TRN_UPD";
        //            var _with22 = _with21.Parameters;

        //            updCommand1.Parameters.Add("DEPOT_STORAGE_SLAB_PK_IN", OracleDbType.Int32, 10, "DEPOT_STORAGE_SLAB_PK").Direction = ParameterDirection.Input;
        //            updCommand1.Parameters["DEPOT_STORAGE_SLAB_PK_IN"].SourceVersion = DataRowVersion.Current;
        //            updCommand1.Parameters.Add("DEPOT_MST_FK_IN", lngDepotPK).Direction = ParameterDirection.Input;
        //            //updCommand1.Parameters["DEPOT_MST_FK_IN"].SourceVersion = DataRowVersion.Current
        //            updCommand1.Parameters.Add("FROM_DAYS_IN", OracleDbType.Int32, 10, "FROM_DAYS").Direction = ParameterDirection.Input;
        //            updCommand1.Parameters["FROM_DAYS_IN"].SourceVersion = DataRowVersion.Current;
        //            updCommand1.Parameters.Add("TO_DAYS_IN", OracleDbType.Int32, 10, "TO_DAYS").Direction = ParameterDirection.Input;
        //            updCommand1.Parameters["TO_DAYS_IN"].SourceVersion = DataRowVersion.Current;
        //            updCommand1.Parameters.Add("RATE_20_IN", OracleDbType.Int32, 10, "RATE_20").Direction = ParameterDirection.Input;
        //            updCommand1.Parameters["RATE_20_IN"].SourceVersion = DataRowVersion.Current;
        //            updCommand1.Parameters.Add("RATE_40_IN", OracleDbType.Int32, 10, "RATE_40").Direction = ParameterDirection.Input;
        //            updCommand1.Parameters["RATE_40_IN"].SourceVersion = DataRowVersion.Current;
        //            updCommand1.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
        //            updCommand1.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
        //            updCommand1.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
        //            updCommand1.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
        //            updCommand1.Parameters["CONFIG_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
        //            updCommand1.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
        //            updCommand1.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

        //            objWK.MyDataAdapter.RowUpdated += new OracleRowUpdatedEventHandler(OnRowUpdated);

        //            //objWK.MyDataAdapter.ContinueUpdateOnError = True
        //            var _with23 = objWK.MyDataAdapter;
        //            _with23.InsertCommand = insCommand1;
        //            _with23.InsertCommand.Transaction = Tran;
        //            _with23.UpdateCommand = updCommand1;
        //            _with23.UpdateCommand.Transaction = Tran;
        //            _with23.DeleteCommand = delCommand1;
        //            _with23.DeleteCommand.Transaction = Tran;
        //            recf = _with23.Update(M_Dataset);

        //            if (arrMessage.Count > 0)
        //            {
        //                Tran.Rollback();
        //                return arrMessage;
        //            }
        //            else
        //            {
        //                Tran.Commit();
        //                arrMessage.Add("All Data Saved Successfully");
        //                arrMessage.Add(lngDepotPK);
        //                return arrMessage;
        //            }
        //        }
        //        catch (OracleException oraexp)
        //        {
        //            throw oraexp;
        //        }
        //        catch (Exception ex)
        //        {
        //            throw ex;
        //            //Manjunath  PTS ID:Sep-02  14/09/2011
        //        }
        //        finally
        //        {
        //            objWK.CloseConnection();
        //        }
        //    }

        //    #endregion "Update Function"

        //    #region "Delete Depot"

        //    public ArrayList DeleteDepo(DataSet M_DataSet)
        //    {
        //        //Dim objWF As New WorkFlow
        //        //objWF.OpenConnection()
        //        WorkFlow objWK = new WorkFlow();
        //        objWK.OpenConnection();
        //        //Dim DelCommand As OracleCommand
        //        OracleCommand DelCommand = new OracleCommand();
        //        OracleTransaction Tran = null;
        //        Int32 RecAfct = default(Int32);
        //        try
        //        {
        //            Tran = objWK.MyConnection.BeginTransaction();

        //            var _with24 = DelCommand;
        //            _with24.Connection = objWK.MyConnection;
        //            _with24.CommandType = CommandType.StoredProcedure;
        //            _with24.CommandText = objWK.MyUserName + ".DEPOT_MST_TBL_PKG.DEPOT_MST_TBL_DEL";
        //            var _with25 = _with24.Parameters;
        //            DelCommand.Parameters.Add("DEPOT_MST_PK_IN", OracleDbType.Int32, 10, "DEPOT_MST_PK").Direction = ParameterDirection.Input;
        //            DelCommand.Parameters["DEPOT_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

        //            DelCommand.Parameters.Add("DELETED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

        //            DelCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
        //            DelCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

        //            DelCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
        //            DelCommand.Parameters["CONFIG_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

        //            DelCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 200, "RETURN_VALUE").Direction = ParameterDirection.Output;
        //            DelCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

        //            objWK.MyDataAdapter.RowUpdated += new OracleRowUpdatedEventHandler(OnRowUpdated);

        //            var _with26 = objWK.MyDataAdapter;
        //            _with26.DeleteCommand = DelCommand;
        //            _with26.DeleteCommand.Transaction = Tran;
        //            RecAfct = _with26.Update(M_DataSet);

        //            if (arrMessage.Count > 0)
        //            {
        //                Tran.Rollback();
        //                return arrMessage;
        //            }
        //            else
        //            {
        //                Tran.Commit();
        //                arrMessage.Add("All Data Saved Successfully");
        //                return arrMessage;
        //            }
        //        }
        //        catch (OracleException oraexp)
        //        {
        //            throw oraexp;
        //        }
        //        catch (Exception ex)
        //        {
        //            throw ex;
        //            //Manjunath  PTS ID:Sep-02  14/09/2011
        //        }
        //        finally
        //        {
        //            objWK.CloseConnection();
        //        }
        //    }

        //    #endregion "Delete Depot"

        //    #region "Fetch Currency"

        //    public DataSet fetchcurrency(long Currencypk = 0, string CurrencyID = "")
        //    {
        //        string strSQL = null;
        //        strSQL = " SELECT ";
        //        strSQL += " ' 'CURRENCY_ID,";
        //        strSQL += " from currency_type_mst_tbl,";
        //        strSQL += "where ACTIVE_FLAG = 1 ";

        //        if (Currencypk > 0)
        //        {
        //            strSQL = strSQL + " where currency_type_mst_tbl.CURRENCY_MST_PK=" + Currencypk;
        //        }
        //        strSQL += " ORDER BY currency_type_mst_tbl ";

        //        WorkFlow objWF = new WorkFlow();
        //        OracleDataReader objDR = null;
        //        try
        //        {
        //            return objWF.GetDataSet(strSQL);
        //            //If objDR.HasRows = False Then
        //            //    Return Nothing
        //            //End If
        //            //Return objDR
        //            //Catch sqlExp As OracleException
        //            //Modified by Manjunath  PTS ID:Sep-02  14/09/2011
        //        }
        //        catch (OracleException sqlExp)
        //        {
        //            ErrorMessage = sqlExp.Message;
        //            throw sqlExp;
        //        }
        //        catch (Exception exp)
        //        {
        //            ErrorMessage = exp.Message;
        //            throw exp;
        //        }
        //    }

        //    #endregion "Fetch Currency"

        //    #region "Enhance Search & Lookup Search Block "

        //    //Pls do the impact the analysis before changing as this function
        //    //as might be accesed by other forms also.
        //    public string FetchDepot(string strCond)
        //    {
        //        WorkFlow objWF = new WorkFlow();
        //        OracleCommand selectCommand = new OracleCommand();
        //        string strReturn = null;
        //        Array arr = null;
        //        string strLOC_MST_IN = null;
        //        string strSERACH_IN = null;
        //        string strReq = null;
        //        int strBizType = 0;
        //        var strNull = "";
        //        arr = strCond.Split("~");
        //        strReq = arr(0);
        //        strSERACH_IN = arr(1);
        //        strLOC_MST_IN = arr(2);
        //        strBizType = arr(3);
        //        try
        //        {
        //            objWF.OpenConnection();
        //            selectCommand.Connection = objWF.MyConnection;
        //            selectCommand.CommandType = CommandType.StoredProcedure;
        //            selectCommand.CommandText = objWF.MyUserName + ".EN_DEPOT_PKG.GETDEPOT_COMMON";

        //            var _with27 = selectCommand.Parameters;
        //            _with27.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
        //            _with27.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
        //            _with27.Add("LOCATION_MST_FK_IN", strLOC_MST_IN).Direction = ParameterDirection.Input;
        //            _with27.Add("BIZTYPE_IN", strBizType).Direction = ParameterDirection.Input;
        //            _with27.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
        //            selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
        //            selectCommand.ExecuteNonQuery();
        //            strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
        //            return strReturn;
        //            //Manjunath  PTS ID:Sep-02  14/09/2011
        //        }
        //        catch (OracleException OraExp)
        //        {
        //            throw OraExp;
        //        }
        //        catch (Exception ex)
        //        {
        //            throw ex;
        //        }
        //        finally
        //        {
        //            selectCommand.Connection.Close();
        //        }
        //    }

        //    #endregion "Enhance Search & Lookup Search Block "
    }
}