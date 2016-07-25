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
    public class Cls_CreditCntlMngr : CommonFeatures
    {
        //#region " Fetch Data "

        //public DataSet FetchData(string strLocPk)
        //{
        //    WorkFlow objWF = new WorkFlow();
        //    StringBuilder strQuery = new StringBuilder();
        //    strQuery.Append(" SELECT CRT.CR_MANAGER_TBL_PK,CRT.LOCATION_MST_FK,CRT.CM_USER_MST_FK,");
        //    strQuery.Append(" CRT.SEA_CREDIT_LIMIT,CRT.SEA_CURRENCY_MST_FK,CRT.SEA_CREDIT_DAYS,CRT.SEA_RATE_OF_INTEREST,");
        //    strQuery.Append(" CRT.SEA_INT_PER_PERIOD,CRT.SEA_FLAT_PENALTY,CRT.AIR_CREDIT_LIMIT,CRT.AIR_CURRENCY_MST_FK,");
        //    strQuery.Append(" CRT.AIR_CREDIT_DAYS,CRT.AIR_RATE_OF_INTEREST,CRT.AIR_INT_PER_PERIOD,CRT.AIR_FLAT_PENALTY,");
        //    strQuery.Append(" CRT.ALERT_CM,CRT.CR_LIMIT_EDITABLE,CRT.CR_DAYS_EDITABLE,CRT.CR_CURR_EDITABLE,CRT.INTEREST_EDITABLE,");
        //    strQuery.Append(" CRT.PENALTY_EDITABLE,CRT.APP_BOOKING,CRT.APP_BL_RELEASE,CRT.APP_RELEASE_ORDER,CRT.EMAIL_ON_POLICY_CHG,CRT.APP_BOOKING,CRT.APP_BL_RELEASE,CRT.APP_RELEASE_ORDER,");
        //    strQuery.Append(" CRT.EMAIL_ON_CREDIT,CRT.TOTAL_OUTS_AMT,CRT.VERSION_NO,CRNSEA.CURRENCY_ID,LOC.LOCATION_ID,LOC.LOCATION_MST_PK,");
        //    strQuery.Append(" LOC.LOCATION_NAME,EMP.USER_ID EMPLOYEE_ID,EMP.USER_MST_PK EMPLOYEE_MST_PK,EMP.USER_NAME EMPLOYEE_NAME,");
        //    strQuery.Append(" NVL(CRT.ACTIVE_FLAG,0) ACTIVE_FLAG,CRT.EXPIRY_DATE,CRT.APP_CONSOLIDATION,CRT.EXPIRED_ON_EDITABLE");
        //    strQuery.Append(" FROM CREDIT_MANAGER_TBL CRT,CURRENCY_TYPE_MST_TBL CRNSEA,CURRENCY_TYPE_MST_TBL CRNAIR,LOCATION_MST_TBL LOC,USER_MST_TBL EMP");
        //    //EMPLOYEE_MST_TBL EMP")
        //    strQuery.Append(" WHERE CRT.SEA_CURRENCY_MST_FK = CRNSEA.CURRENCY_MST_PK AND CRT.AIR_CURRENCY_MST_FK = CRNAIR.CURRENCY_MST_PK(+)");
        //    strQuery.Append(" AND CRT.LOCATION_MST_FK = LOC.LOCATION_MST_PK AND CRT.CM_USER_MST_FK = EMP.USER_MST_PK");
        //    strQuery.Append(" AND CRT.LOCATION_MST_FK = " + strLocPk + " ");
        //    try
        //    {
        //        return objWF.GetDataSet(strQuery.ToString());
        //    }
        //    catch (OracleException OraExp)
        //    {
        //        throw OraExp;
        //    }
        //    catch (Exception EX)
        //    {
        //        throw EX;
        //    }
        //}

        //#endregion " Fetch Data "

        //#region "Fetch outstanding Sea "

        //public DataSet FetchOsSeaData(string LocationFk = "0", string CustomerPK = "0")
        //{
        //    WorkFlow objWF = new WorkFlow();

        //    try
        //    {
        //        objWF.MyCommand.Parameters.Clear();
        //        var _with1 = objWF.MyCommand.Parameters;
        //        _with1.Add("LOC_PK", LocationFk).Direction = ParameterDirection.Input;
        //        _with1.Add("CUST_PK", CustomerPK).Direction = ParameterDirection.Input;
        //        _with1.Add("LOC_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
        //        return objWF.GetDataSet("FETCH_JOB_CARD_REVENUE_DATA", "FETCH_FREIGHT_OUTSTANDING_SEA");
        //    }
        //    catch (OracleException OraExp)
        //    {
        //        throw OraExp;
        //    }
        //    catch (Exception EX)
        //    {
        //        throw EX;
        //    }
        //}

        //#endregion "Fetch outstanding Sea "

        //#region "Fetch outstanding Air "

        //public DataSet FetchOsAirData(string LocationFk = "0", string CustomerPK = "0")
        //{
        //    WorkFlow objWF = new WorkFlow();

        //    try
        //    {
        //        objWF.MyCommand.Parameters.Clear();
        //        var _with2 = objWF.MyCommand.Parameters;
        //        _with2.Add("LOC_PK", LocationFk).Direction = ParameterDirection.Input;
        //        _with2.Add("CUST_PK", CustomerPK).Direction = ParameterDirection.Input;
        //        _with2.Add("LOC_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
        //        return objWF.GetDataSet("FETCH_JOB_CARD_REVENUE_DATA", "FETCH_FREIGHT_OUTSTANDING_AIR");
        //    }
        //    catch (OracleException OraExp)
        //    {
        //        throw OraExp;
        //    }
        //    catch (Exception EX)
        //    {
        //        throw EX;
        //    }
        //}

        //#endregion "Fetch outstanding Air "

        //#region " Fetch Location "

        //public string FetchLoc(string strLocPk)
        //{
        //    WorkFlow objWF = new WorkFlow();
        //    StringBuilder strQuery = new StringBuilder();
        //    strQuery.Append(" SELECT CR.CR_MANAGER_TBL_PK FROM CREDIT_MANAGER_TBL CR");
        //    strQuery.Append(" WHERE CR.LOCATION_MST_FK  = " + strLocPk + " ");
        //    try
        //    {
        //        return objWF.ExecuteScaler(strQuery.ToString());
        //    }
        //    catch (OracleException OraExp)
        //    {
        //        throw OraExp;
        //    }
        //    catch (Exception EX)
        //    {
        //        throw EX;
        //    }
        //}

        //#endregion " Fetch Location "

        //#region " Fetch Location Data "

        //public DataSet FetchLocData(string strLocPk, int intUsrPk)
        //{
        //    WorkFlow objWF = new WorkFlow();
        //    StringBuilder strQuery = new StringBuilder();
        //    strQuery.Append(" SELECT LOC.LOCATION_MST_PK,LOC.LOCATION_ID, ");
        //    strQuery.Append(" LOC.LOCATION_NAME,USR.USER_NAME employee_name,usr.user_mst_pk employee_mst_pk");
        //    strQuery.Append(" FROM LOCATION_MST_TBL LOC, USER_MST_TBL USR, employee_mst_tbl emp");
        //    strLocPk = (string.IsNullOrEmpty(strLocPk) ? 0 : strLocPk);
        //    strQuery.Append(" WHERE LOC.LOCATION_MST_PK = " + strLocPk + " ");
        //    strQuery.Append(" and emp.employee_mst_pk = usr.employee_mst_fk");
        //    strQuery.Append(" AND USR.DEFAULT_LOCATION_FK = LOC.LOCATION_MST_PK ");
        //    strQuery.Append(" AND USR.USER_MST_PK =  " + intUsrPk + " ");
        //    try
        //    {
        //        return objWF.GetDataSet(strQuery.ToString());
        //    }
        //    catch (OracleException OraExp)
        //    {
        //        throw OraExp;
        //    }
        //    catch (Exception EX)
        //    {
        //        throw EX;
        //    }
        //}

        //#endregion " Fetch Location Data "

        //#region " Fetch Permissible Data "

        //public DataSet FetchData1(string strLocPk)
        //{
        //    WorkFlow objWF = new WorkFlow();
        //    StringBuilder strQuery = new StringBuilder();
        //    strQuery.Append("   SELECT CRT.LOCATION_MST_FK,");
        //    strQuery.Append("   CRT.CR_DAYS_EDITABLE,");
        //    strQuery.Append("   CRT.CR_LIMIT_EDITABLE,");
        //    strQuery.Append("   CRT.EXPIRED_ON_EDITABLE,");
        //    strQuery.Append("   CRT.PENALTY_EDITABLE,");
        //    strQuery.Append("   CRT.INTEREST_EDITABLE");
        //    strQuery.Append("    FROM CREDIT_MANAGER_TBL CRT");
        //    strQuery.Append("    WHERE CRT.LOCATION_MST_FK (+) = " + strLocPk + " ");
        //    try
        //    {
        //        return objWF.GetDataSet(strQuery.ToString());
        //    }
        //    catch (OracleException OraExp)
        //    {
        //        throw OraExp;
        //    }
        //    catch (Exception EX)
        //    {
        //        throw EX;
        //    }
        //}

        //#endregion " Fetch Permissible Data "

        //#region " Fetch Action Data "

        //public DataSet FetchActionData(string strLocPk)
        //{
        //    WorkFlow objWF = new WorkFlow();
        //    StringBuilder strQuery = new StringBuilder();
        //    strQuery.Append("   SELECT CRT.LOCATION_MST_FK,");
        //    strQuery.Append("   CRT.APP_BOOKING,");
        //    strQuery.Append("   CRT.APP_BL_RELEASE,");
        //    strQuery.Append("   CRT.APP_CONSOLIDATION,");
        //    strQuery.Append("   CRT.APP_RELEASE_ORDER");
        //    strQuery.Append("    FROM CREDIT_MANAGER_TBL CRT");
        //    strQuery.Append("    WHERE CRT.LOCATION_MST_FK (+) = " + strLocPk + " ");
        //    try
        //    {
        //        return objWF.GetDataSet(strQuery.ToString());
        //    }
        //    catch (OracleException OraExp)
        //    {
        //        throw OraExp;
        //    }
        //    catch (Exception EX)
        //    {
        //        throw EX;
        //    }
        //}

        //#endregion " Fetch Action Data "

        //#region " Fetch Action Data "

        //public DataSet FetchCustData(string strCusPk, int biztype = 0)
        //{
        //    WorkFlow objWF = new WorkFlow();
        //    StringBuilder strQuery = new StringBuilder();
        //    strQuery.Append("   SELECT CUST.CUSTOMER_MST_PK,CUST.SEA_APP_BOOKING,CUST.SEA_APP_BL_RELEASE,CUST.SEA_APP_RELEASE_ODR,CUST.SEA_APP_CONSOLIDATION");
        //    if (biztype == 0)
        //    {
        //        strQuery.Append(", CUST.AIR_APP_BOOKING,CUST.AIR_APP_BL_RELEASE,CUST.AIR_APP_RELEASE_ODR ");
        //    }
        //    strQuery.Append("    FROM  CUSTOMER_MST_TBL CUST");
        //    strQuery.Append("    WHERE CUST.CUSTOMER_MST_PK = " + strCusPk + " ");
        //    try
        //    {
        //        return objWF.GetDataSet(strQuery.ToString());
        //    }
        //    catch (OracleException OraExp)
        //    {
        //        throw OraExp;
        //    }
        //    catch (Exception EX)
        //    {
        //        throw EX;
        //    }
        //}

        //#endregion " Fetch Action Data "

        //#region " Save And Update "

        //public ArrayList SaveData(long nLocationfk, string CrMPk, int CreditMngrFk, string CreditLimitS, string CurrencyFkS, string CreditDaysS, string IntRateS, int IntPerPeriodS, string FlatPenaltyS, string ExpiryDt,
        //string CreditLimitA, string CurrencyFkA, string CreditDaysA, string IntRateA, int IntPerPeriodA, string FlatPenaltyA, int PenaltyEdit, int CR_LIMIT, int CR_DAYS, int CR_CURR,
        //int INTEREST, int EXPIREDON, int AlertCM, int EmailPolicyChange, int APP_BOOKING, int APP_BL_RELEASE, int APP_RELEASE_ORDER, int APP_CONSOLIDATION, int EmailOnCredit, int ActiveFlag,
        //int Version, string CustPk, string TotOSAmt)
        //{
        //    WorkFlow objWK = new WorkFlow();
        //    objWK.OpenConnection();

        //    string CurMgrPk = null;
        //    Int16 Execute = default(Int16);
        //    OracleTransaction TRAN = null;

        //    try
        //    {
        //        TRAN = objWK.MyConnection.BeginTransaction();
        //        objWK.MyCommand.Transaction = TRAN;
        //        OracleParameterCollection ColPara = new OracleParameterCollection();

        //        var _with3 = objWK.MyCommand;

        //        if (CrMPk == "0")
        //        {
        //            _with3.CommandText = objWK.MyUserName + ".CREDIT_MANAGER_TBL_PKG.CREDIT_MANAGER_TBL_INS";
        //        }
        //        else
        //        {
        //            _with3.CommandText = objWK.MyUserName + ".CREDIT_MANAGER_TBL_PKG.CREDIT_MANAGER_TBL_UPD";
        //        }
        //        _with3.Connection = objWK.MyConnection;
        //        _with3.CommandType = CommandType.StoredProcedure;
        //        _with3.Transaction = TRAN;
        //        var _with4 = _with3.Parameters;
        //        if (CrMPk != "0")
        //        {
        //            _with4.Add("CR_MANAGER_TBL_PK_IN", CrMPk);
        //            _with4.Add("LAST_MODIFIED_BY_FK_IN", LAST_MODIFIED_BY);
        //            _with4.Add("VERSION_NO_IN", Version);
        //        }
        //        else
        //        {
        //            _with4.Add("CREATED_BY_FK_IN", CREATED_BY);
        //        }
        //        _with4.Add("LOCATION_MST_FK_IN", getDefault(nLocationfk, "")).Direction = ParameterDirection.Input;
        //        //.Add("CM_USER_MST_FK_IN", getDefault(CreditMngrFk, "")).Direction = ParameterDirection.Input
        //        //Snigdharani - 12/01/2010 - This field need the user pk and not the employee pk
        //        _with4.Add("CM_USER_MST_FK_IN", getDefault(CreditMngrFk, "")).Direction = ParameterDirection.Input;
        //        _with4.Add("S_CREDIT_LIMIT_IN", getDefault(Convert.ToDouble(CreditLimitS), "")).Direction = ParameterDirection.Input;
        //        _with4.Add("S_CURRENCY_MST_FK_IN", getDefault(CurrencyFkS, "")).Direction = ParameterDirection.Input;
        //        _with4.Add("S_CREDIT_DAYS_IN", getDefault(CreditDaysS, "")).Direction = ParameterDirection.Input;
        //        _with4.Add("S_RATE_OF_INTEREST_IN", getDefault(IntRateS, "")).Direction = ParameterDirection.Input;
        //        _with4.Add("S_INT_PER_PERIOD_IN", IntPerPeriodS).Direction = ParameterDirection.Input;
        //        _with4.Add("S_FLAT_PENALTY_IN", getDefault(FlatPenaltyS, "")).Direction = ParameterDirection.Input;
        //        _with4.Add("S_EXPIRYDT_IN", getDefault(ExpiryDt, "")).Direction = ParameterDirection.Input;
        //        _with4.Add("A_CREDIT_LIMIT_IN", getDefault(Convert.ToDouble(CreditLimitS), "")).Direction = ParameterDirection.Input;
        //        _with4.Add("A_CURRENCY_MST_FK_IN", getDefault(CurrencyFkS, "")).Direction = ParameterDirection.Input;
        //        _with4.Add("A_CREDIT_DAYS_IN", getDefault(CreditDaysS, "")).Direction = ParameterDirection.Input;
        //        _with4.Add("A_RATE_OF_INTEREST_IN", getDefault(IntRateS, "")).Direction = ParameterDirection.Input;
        //        _with4.Add("A_INT_PER_PERIOD_IN", IntPerPeriodS).Direction = ParameterDirection.Input;
        //        _with4.Add("A_FLAT_PENALTY_IN", getDefault(FlatPenaltyS, "")).Direction = ParameterDirection.Input;
        //        _with4.Add("PENALTY_EDITABLE_IN", PenaltyEdit).Direction = ParameterDirection.Input;
        //        _with4.Add("ALERT_CM_IN", AlertCM).Direction = ParameterDirection.Input;
        //        _with4.Add("CR_LIMIT_EDITABLE_IN", CR_LIMIT).Direction = ParameterDirection.Input;
        //        _with4.Add("CR_DAYS_EDITABLE_IN", CR_DAYS).Direction = ParameterDirection.Input;
        //        _with4.Add("CR_CURR_EDITABLE_IN", CR_CURR).Direction = ParameterDirection.Input;
        //        _with4.Add("INTEREST_EDITABLE_IN", INTEREST).Direction = ParameterDirection.Input;
        //        _with4.Add("EXPIREDON_EDITABLE_IN", EXPIREDON).Direction = ParameterDirection.Input;
        //        _with4.Add("EMAIL_ON_POLICY_CHG_IN", EmailPolicyChange).Direction = ParameterDirection.Input;
        //        _with4.Add("APP_BOOKING_IN", APP_BOOKING).Direction = ParameterDirection.Input;
        //        _with4.Add("APP_BL_RELEASE_IN", APP_BL_RELEASE).Direction = ParameterDirection.Input;
        //        _with4.Add("APP_RELEASE_ORDER_IN", APP_RELEASE_ORDER).Direction = ParameterDirection.Input;
        //        _with4.Add("APP_CONSOLIDATION_IN", APP_CONSOLIDATION).Direction = ParameterDirection.Input;
        //        _with4.Add("EMAIL_ON_CREDIT_IN", EmailOnCredit).Direction = ParameterDirection.Input;
        //        _with4.Add("ACTIVE_IN", ActiveFlag).Direction = ParameterDirection.Input;
        //        _with4.Add("TOTAL_OUTS_AMT_IN", getDefault(Convert.ToDouble(Conversion.Val(TotOSAmt)), "")).Direction = ParameterDirection.Input;
        //        _with4.Add("CUSTPK_IN", (!string.IsNullOrEmpty(CustPk) ? CustPk : "")).Direction = ParameterDirection.Input;
        //        _with4.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
        //        _with4.Add("RETURN_VALUE", OracleDbType.Int32, 10, "CR_MANAGER_TBL_PK").Direction = ParameterDirection.Output;
        //        try
        //        {
        //            Execute = _with3.ExecuteNonQuery();
        //        }
        //        catch (OracleException oraexp)
        //        {
        //            TRAN.Rollback();
        //            throw oraexp;
        //        }
        //        CrMPk = _with3.Parameters["RETURN_VALUE"].Value;
        //        if (Execute >= 1)
        //        {
        //            TRAN.Commit();
        //            arrMessage.Add("All Data Saved Successfully");
        //            return arrMessage;
        //        }
        //    }
        //    catch (OracleException OraExp)
        //    {
        //        throw OraExp;
        //    }
        //    catch (Exception ex)
        //    {
        //        arrMessage.Add(ex.Message);
        //        return arrMessage;
        //    }
        //    finally
        //    {
        //        objWK.CloseConnection();
        //    }
        //}

        //#endregion " Save And Update "

        //#region "Fetch Credit Policy Details For the Selected Customer"

        //public DataSet FetchCreditDet(int CustPk)
        //{
        //    try
        //    {
        //        DataSet CreditDS = new DataSet();
        //        WorkFlow objWF = new WorkFlow();
        //        objWF.MyCommand.Parameters.Clear();
        //        objWF.MyCommand.Parameters.Add("CUSTOMER_PK_IN", CustPk).Direction = ParameterDirection.Input;
        //        objWF.MyCommand.Parameters.Add("CURSOR_CUST", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
        //        CreditDS = objWF.GetDataSet("CREDIT_MANAGER_TBL_PKG", "FETCH_CREDIT_POLICY_DET_CUST");
        //        return CreditDS;
        //    }
        //    catch (OracleException OraExp)
        //    {
        //        throw OraExp;
        //    }
        //    catch (Exception EX)
        //    {
        //        throw EX;
        //    }
        //}

        //#endregion "Fetch Credit Policy Details For the Selected Customer"

        //#region "Fetch Customer details From Customer Profile"

        //public object FecthCustProfile(string CustProfPk = "0")
        //{
        //    System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
        //    WorkFlow objWF = new WorkFlow();
        //    strSQL.Append(" SELECT CMT.CUSTOMER_MST_PK, CMT.CUSTOMER_ID, CMT.CUSTOMER_NAME ");
        //    strSQL.Append(" FROM CUSTOMER_MST_TBL CMT");
        //    strSQL.Append(" WHERE CMT.CUSTOMER_MST_PK =" + CustProfPk);

        //    try
        //    {
        //        return (objWF.GetDataSet(strSQL.ToString()));
        //    }
        //    catch (OracleException Oraexp)
        //    {
        //        throw Oraexp;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //#endregion "Fetch Customer details From Customer Profile"

        //#region "Credit Cust Grid Details"

        //public DataSet FetchGridDetails(int LocationPk = 0, int CustomerPk = 0, int ActiveFlag = 1, int LocPk = 0, int TotalPage = 0, int CurrentPage = 0, Int32 Onload = 0)
        //{
        //    Int32 Last = default(Int32);
        //    Int32 Start = default(Int32);
        //    Int32 TotalRecords = default(Int32);
        //    WorkFlow objWF = new WorkFlow();
        //    System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
        //    sb.Append("SELECT CMT.CR_MANAGER_TBL_PK,");
        //    sb.Append("       NVL(CMT.ACTIVE_FLAG, 0) ACTIVE_FLAG,");
        //    sb.Append("       LMT.LOCATION_MST_PK,");
        //    sb.Append("       LMT.LOCATION_ID,");
        //    sb.Append("       LMT.LOCATION_NAME,");
        //    sb.Append("       UMT.USER_MST_PK,");
        //    sb.Append("       UMT.USER_NAME,");
        //    sb.Append("       CASE WHEN CMT.ALERT_CM=1 THEN 'a' ELSE 'r' END ALERT_CM,");
        //    sb.Append("       'VIEW' DETAILSVIEW");
        //    sb.Append("  FROM CREDIT_MANAGER_TBL CMT, LOCATION_MST_TBL LMT, USER_MST_TBL UMT");
        //    sb.Append(" WHERE CMT.LOCATION_MST_FK = LMT.LOCATION_MST_PK");
        //    sb.Append("   AND CMT.CM_USER_MST_FK = UMT.USER_MST_PK");
        //    sb.Append("   AND LMT.LOCATION_MST_PK IN");
        //    sb.Append("       (SELECT L.LOCATION_MST_PK");
        //    sb.Append("          FROM LOCATION_MST_TBL L");
        //    sb.Append("         START WITH L.LOCATION_MST_PK = " + LocPk);
        //    sb.Append("        CONNECT BY PRIOR L.LOCATION_MST_PK = L.REPORTING_TO_FK)");
        //    if (Onload == 1)
        //    {
        //        sb.Append("  AND 1=1");
        //    }
        //    if (LocationPk != 0)
        //    {
        //        sb.Append(" AND LMT.LOCATION_MST_PK= " + LocationPk);
        //    }
        //    if (CustomerPk != 0)
        //    {
        //        sb.Append(" AND UMT.USER_MST_PK= " + CustomerPk);
        //    }
        //    if (ActiveFlag == 1)
        //    {
        //        sb.Append(" AND CMT.ACTIVE_FLAG=1 ");
        //    }
        //    DataTable tbl = new DataTable();
        //    tbl = objWF.GetDataTable(sb.ToString());
        //    TotalRecords = (Int32)tbl.Rows.Count;
        //    TotalPage = TotalRecords / RecordsPerPage;
        //    if (TotalRecords % RecordsPerPage != 0)
        //    {
        //        TotalPage += 1;
        //    }
        //    if (CurrentPage > TotalPage)
        //        CurrentPage = 1;
        //    if (TotalRecords == 0)
        //        CurrentPage = 0;
        //    Last = CurrentPage * RecordsPerPage;
        //    Start = (CurrentPage - 1) * RecordsPerPage + 1;
        //    System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
        //    sqlstr.Append(" SELECT Q.* FROM (SELECT ROWNUM AS \"SLNO\", QRY.* FROM ");
        //    sqlstr.Append("  (" + sb.ToString() + " ");
        //    sqlstr.Append("   ORDER BY LMT.LOCATION_ID ASC) QRY )Q ");

        //    sqlstr.Append("  WHERE Q.SLNO  BETWEEN " + Start + " AND " + Last + "");

        //    try
        //    {
        //        return objWF.GetDataSet(sqlstr.ToString());
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

        //public DataSet FetchCustGridDetails(int LocPk = 0, int TotalPage = 0, int CurrentPage = 0, int Onload = 0, int Customer = 0)
        //{
        //    Int32 Last = default(Int32);
        //    Int32 Start = default(Int32);
        //    Int32 TotalRecords = default(Int32);
        //    WorkFlow objWF = new WorkFlow();
        //    System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
        //    sb.Append("SELECT CMT.CUSTOMER_MST_PK,");
        //    sb.Append("       CMT.CUSTOMER_ID,");
        //    sb.Append("       CMT.CUSTOMER_NAME,");
        //    sb.Append("       CMT.SEA_CREDIT_LIMIT,");
        //    sb.Append("       CMT.SEA_CREDIT_DAYS,");
        //    sb.Append("       INV.INVOICE_AMT,");
        //    sb.Append("       '' OUTSTANDING_AMT,");
        //    sb.Append("       (NVL(CMT.SEA_CREDIT_LIMIT, 0) - NVL(ABS(CMT.CREDIT_LIMIT_USED), 0)) CREDIT_AVAILABLE");
        //    sb.Append("");
        //    sb.Append("  FROM CUSTOMER_MST_TBL CMT,CUSTOMER_CONTACT_DTLS CCD,");
        //    sb.Append("       (SELECT CIT.CUSTOMER_MST_FK,");
        //    sb.Append("               SUM(NVL(CIT.NET_RECEIVABLE, 0) *");
        //    sb.Append("                   GET_EX_RATE(CIT.CURRENCY_MST_FK,");
        //    sb.Append("                               C.CURRENCY_MST_FK,");
        //    sb.Append("                               CIT.INVOICE_DATE)) INVOICE_AMT");
        //    sb.Append("          FROM CONSOL_INVOICE_TBL    CIT,");
        //    sb.Append("               CUSTOMER_MST_TBL      CTM,");
        //    sb.Append("               CUSTOMER_CONTACT_DTLS CCD,");
        //    sb.Append("               COUNTRY_MST_TBL       C");
        //    sb.Append("         WHERE CTM.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
        //    sb.Append("           AND CCD.ADM_COUNTRY_MST_FK = C.COUNTRY_MST_PK");
        //    sb.Append("           AND CTM.CUSTOMER_MST_PK = CIT.CUSTOMER_MST_FK");
        //    sb.Append("         GROUP BY CIT.CUSTOMER_MST_FK) INV");
        //    sb.Append(" WHERE CMT.CREDIT_CUSTOMER = 1");
        //    sb.Append("   AND INV.CUSTOMER_MST_FK(+) = CMT.CUSTOMER_MST_PK");
        //    sb.Append("   AND CMT.CUSTOMER_MST_PK=CCD.CUSTOMER_MST_FK");
        //    sb.Append(" AND CCD.ADM_LOCATION_MST_FK=" + LocPk);
        //    if (Onload == 1)
        //    {
        //        sb.Append(" AND 1=1");
        //    }
        //    if (Customer != 0)
        //    {
        //        sb.Append(" AND cmt.customer_mst_pk=" + Customer);
        //    }

        //    DataTable tbl = new DataTable();
        //    tbl = objWF.GetDataTable(sb.ToString());
        //    TotalRecords = (Int32)tbl.Rows.Count;
        //    TotalPage = TotalRecords / RecordsPerPage;
        //    if (TotalRecords % RecordsPerPage != 0)
        //    {
        //        TotalPage += 1;
        //    }
        //    if (CurrentPage > TotalPage)
        //        CurrentPage = 1;
        //    if (TotalRecords == 0)
        //        CurrentPage = 0;
        //    Last = CurrentPage * RecordsPerPage;
        //    Start = (CurrentPage - 1) * RecordsPerPage + 1;
        //    System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
        //    sqlstr.Append(" SELECT Q.* FROM (SELECT ROWNUM AS \"SLNO\", QRY.* FROM ");
        //    sqlstr.Append("  (" + sb.ToString() + " ");
        //    sqlstr.Append("   ORDER BY CMT.CUSTOMER_ID ASC) QRY )Q ");

        //    sqlstr.Append("  WHERE Q.SLNO  BETWEEN " + Start + " AND " + Last + "");

        //    try
        //    {
        //        return objWF.GetDataSet(sqlstr.ToString());
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

        //#endregion "Credit Cust Grid Details"

        //#region "Credit Cust Currency"

        //public DataSet GetCorpCurrency(Int32 LocationPK)
        //{
        //    WorkFlow objWK = new WorkFlow();
        //    System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
        //    sb.Append("SELECT CTMT.CURRENCY_MST_PK,CTMT.CURRENCY_ID");
        //    sb.Append("  FROM LOCATION_MST_TBL      LMT,");
        //    sb.Append("       COUNTRY_MST_TBL       CMT,");
        //    sb.Append("       CURRENCY_TYPE_MST_TBL CTMT");
        //    sb.Append(" WHERE LMT.COUNTRY_MST_FK = CMT.COUNTRY_MST_PK");
        //    sb.Append("   AND CMT.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK");
        //    sb.Append("   AND LMT.LOCATION_MST_PK = " + LocationPK);
        //    sb.Append("");
        //    try
        //    {
        //        return objWK.GetDataSet(sb.ToString());
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

        //#endregion "Credit Cust Currency"
    }
}