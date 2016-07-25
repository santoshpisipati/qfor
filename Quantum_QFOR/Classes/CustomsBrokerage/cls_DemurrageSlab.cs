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

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_DemurrageSlab : CommonFeatures
    {
        //#region "Fetch Currency"

        //public DataSet FetchCurrency()
        //{
        //    string StrSQL = null;
        //    try
        //    {
        //        StrSQL += "SELECT 0 CURRENCY_MST_PK, ' ' CURRENCY_ID ";
        //        StrSQL += " FROM DUAL ";
        //        StrSQL += " UNION ";
        //        StrSQL += " SELECT C.CURRENCY_MST_PK, C.CURRENCY_ID FROM CURRENCY_TYPE_MST_TBL C";
        //        StrSQL += " where c.active_flag=1  ORDER BY currency_id";
        //        WorkFlow objWF = new WorkFlow();
        //        DataSet objDS = null;
        //        objDS = objWF.GetDataSet(StrSQL);
        //        return objDS;
        //    }
        //    catch (OracleException sqlExp)
        //    {
        //        throw sqlExp;
        //    }
        //    catch (Exception exp)
        //    {
        //        throw exp;
        //    }
        //}

        //public string fetchcurrency(string locpk)
        //{
        //    WorkFlow objWF = new WorkFlow();
        //    string strSql = null;

        //    strSql = string.Empty;
        //    strSql += "select currency_mst_pk from currency_type_mst_tbl curr,country_mst_tbl cont,location_mst_tbl loc";
        //    strSql += " where cont.currency_mst_fk=curr.currency_mst_pk  ";
        //    strSql += " and loc.country_mst_fk=cont.country_mst_pk and loc.location_mst_pk= " + locpk;

        //    try
        //    {
        //        if (string.IsNullOrEmpty(objWF.ExecuteScaler(strSql)))
        //            return 0;
        //        return objWF.ExecuteScaler(strSql);
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

        //#region "FetchContainers"

        //public DataSet FetchContainers(string containerPk = "")
        //{
        //    System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
        //    WorkFlow objWF = new WorkFlow();
        //    strSQL.Append(" select ");
        //    strSQL.Append(" CTMT.CONTAINER_TYPE_MST_PK,  CTMT.CONTAINER_TYPE_MST_ID, CTMT.CONTAINER_TYPE_NAME ");
        //    strSQL.Append(" FROM CONTAINER_TYPE_MST_TBL CTMT ");
        //    strSQL.Append(" WHERE CTMT.ACTIVE_FLAG = 1");
        //    if (!string.IsNullOrEmpty(containerPk))
        //    {
        //        strSQL.Append(" AND CTMT.CONTAINER_TYPE_MST_PK IN (" + containerPk + ")");
        //    }
        //    strSQL.Append(" ORDER BY CTMT.PREFERENCES");
        //    try
        //    {
        //        return objWF.GetDataSet(strSQL.ToString());
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

        //#endregion "FetchContainers"

        //#region "FetchHeader"

        //public DataSet FetchHeader(DataSet dsActive)
        //{
        //    string strSql = null;
        //    string strDDlchargeCode = null;
        //    long lngCharge = 0;
        //    string strddlCurrency = null;
        //    long lngCurrency = 0;
        //    string strContainerStatus = null;
        //    long lngStatus = 0;
        //    string strToDate = null;
        //    System.DBNull strNull = null;
        //    int I = 0;

        //    strSql = string.Empty;
        //    strSql = "SELECT SR_NO, '' Port, PERIOD, DAYS, ";

        //    if ((dsActive != null))
        //    {
        //        for (I = 0; I <= dsActive.Tables[0].Rows.Count - 1; I++)
        //        {
        //            strSql += "SUM(CTPK" + Strings.Trim(I) + ") " + "CTPK" + Strings.Trim(I) + ", ";
        //            strSql += "SUM(PK" + Strings.Trim(I) + ") " + "PK" + Strings.Trim(I) + ", ";
        //            strSql += "SUM(RATE" + Strings.Trim(I) + ") " + "RATE" + Strings.Trim(I) + ", ";
        //        }
        //        strSql = Strings.Mid(strSql, 1, Strings.Len(strSql) - 2) + " ";
        //    }
        //    strSql += ",DEL";
        //    strSql += ", '' ID,'A' STATUS,'' CHEFLAG, dem_slab_hdr_fk";
        //    strSql += "  FROM (SELECT ROWNUM SR_NO, T.dem_slab_hdr_fk, '' PERIOD, '' DAYS, ";
        //    if ((dsActive != null))
        //    {
        //        for (I = 0; I <= dsActive.Tables[0].Rows.Count - 1; I++)
        //        {
        //            strSql += " DECODE(T.CONTAINER_TYPE_MST_FK, " + dsActive.Tables[0].Rows[I]["CONTAINER_TYPE_MST_PK"] + ",T.CONTAINER_TYPE_MST_FK, NULL) CTPK" + Strings.Trim(I) + ", ";
        //            strSql += " DECODE(T.CONTAINER_TYPE_MST_FK, " + dsActive.Tables[0].Rows[I]["CONTAINER_TYPE_MST_PK"] + ",T.dem_slab_trn_pk, NULL) PK" + Strings.Trim(I) + ", ";
        //            strSql += " DECODE(T.CONTAINER_TYPE_MST_FK, " + dsActive.Tables[0].Rows[I]["CONTAINER_TYPE_MST_PK"] + ",T.slab_rate, NULL) RATE" + Strings.Trim(I) + ", ";
        //        }
        //        strSql = Strings.Mid(strSql, 1, Strings.Len(strSql) - 2) + " ";
        //    }
        //    strSql += ",'' DEL";
        //    strSql += " FROM   dem_slab_trn T WHERE T.dem_slab_hdr_fk=-1 ";
        //    strSql += " ) GROUP BY SR_NO, dem_slab_hdr_fk, PERIOD, DAYS,DEL";

        //    WorkFlow objWF = new WorkFlow();
        //    return objWF.GetDataSet(strSql);
        //}

        //#endregion "FetchHeader"

        //#region "FetchPortPlaces"

        //public DataSet FetchPortPlaces()
        //{
        //    string strSql = null;
        //    int I = 0;

        //    strSql = "SELECT ROWNUM AS SL_NR, PMT.PORT_MST_PK, PMT.PORT_ID";
        //    strSql += "FROM PORT_MST_TBL PMT";
        //    strSql += "WHERE(Pmt.PORT_TYPE = 1)";
        //    strSql += "AND PMT.PORT_MST_PK IN (0)";

        //    WorkFlow objWF = new WorkFlow();
        //    return objWF.GetDataSet(strSql);
        //}

        //#endregion "FetchPortPlaces"

        //#region "FetchData"

        //public DataSet FetchHolidays(string hdrpk, System.DateTime @from, System.DateTime todt, Int32 CurrentPage = 0, Int32 TotalPage = 0)
        //{
        //    string strSql = null;
        //    string str = null;
        //    int TotalRecords = 0;
        //    int start = 0;
        //    int last = 0;
        //    WorkFlow objWF = new WorkFlow();
        //    DataSet ds = null;
        //    strSql = string.Empty;
        //    if (!string.IsNullOrEmpty(hdrpk))
        //    {
        //        strSql = "SELECT ROWNUM SLNR,dsc.dem_slab_calender_pk,dsc.holiday_date,dsc.holiday_desc,'' Sel FROM dem_slab_calender dsc WHERE dsc.dem_slab_hdr_fk=" + hdrpk;
        //        str = "select count(*) from (" + strSql + ")";
        //        TotalRecords = (Int32)objWF.ExecuteScaler(str);
        //        if (TotalRecords > 0)
        //        {
        //            TotalPage = TotalRecords / M_MasterPageSize;
        //            if (TotalRecords % M_MasterPageSize != 0)
        //            {
        //                TotalPage += 1;
        //            }
        //            if (CurrentPage > TotalPage)
        //            {
        //                CurrentPage = 1;
        //            }
        //            if (TotalRecords == 0)
        //            {
        //                CurrentPage = 0;
        //            }
        //            last = CurrentPage * M_MasterPageSize;
        //            start = (CurrentPage - 1) * M_MasterPageSize + 1;

        //            str = " SELECT * FROM (" + strSql;
        //            str += " ) WHERE SLNR  Between " + start + " and " + last;
        //            return objWF.GetDataSet(strSql);
        //        }
        //    }
        //    strSql = string.Empty;
        //    System.DateTime dt = default(System.DateTime);
        //    if (todt == dt)
        //    {
        //        strSql = "SELECT ROWNUM SLNR,0 dem_slab_calender_pk, dhc.holiday_date,dhc.holiday_desc,'' Sel FROM dem_holiday_calender dhc where dhc.holiday_date>=to_date('" + @from + "') ORDER BY dhc.holiday_calender_pk";
        //    }
        //    else
        //    {
        //        strSql = "SELECT ROWNUM SLNR,0 dem_slab_calender_pk, dhc.holiday_date,dhc.holiday_desc,'' Sel FROM dem_holiday_calender dhc where dhc.holiday_date between to_date('" + @from + "') and to_date('" + todt + "') ORDER BY dhc.holiday_calender_pk";
        //    }
        //    str = "select count(*) from (" + strSql + ")";
        //    TotalRecords = (Int32)objWF.ExecuteScaler(str);
        //    TotalPage = TotalRecords / M_MasterPageSize;
        //    if (TotalRecords % M_MasterPageSize != 0)
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
        //    last = CurrentPage * M_MasterPageSize;
        //    start = (CurrentPage - 1) * M_MasterPageSize + 1;
        //    str = " SELECT * FROM (" + strSql;
        //    str += " ) WHERE SLNR  Between " + start + " and " + last;
        //    return objWF.GetDataSet(str);
        //}

        //public DataSet FetchHoliday(string hdrpk, string @from = "", string todt = "")
        //{
        //    string strSql = null;
        //    string str = null;
        //    int TotalRecords = 0;
        //    int start = 0;
        //    int last = 0;
        //    WorkFlow objWF = new WorkFlow();
        //    DataSet ds = null;
        //    strSql = string.Empty;
        //    if (!string.IsNullOrEmpty(hdrpk))
        //    {
        //        strSql = "SELECT ROWNUM SLNR,q.* FROM (SELECT DSC.DEM_SLAB_CALENDER_PK, DSC.HOLIDAY_DATE, DSC.HOLIDAY_DESC, '' SEL FROM DEM_SLAB_CALENDER DSC WHERE DSC.DEM_SLAB_HDR_FK=" + hdrpk + " ORDER BY DSC.HOLIDAY_DATE)q";
        //    }
        //    else
        //    {
        //        System.DateTime dt = default(System.DateTime);
        //        if (!string.IsNullOrEmpty(@from) & !string.IsNullOrEmpty(todt))
        //        {
        //            strSql = "SELECT ROWNUM SLNR,q.* FROM (SELECT '' DEM_SLAB_CALENDER_PK, DHC.HOLIDAY_DATE, DHC.HOLIDAY_DESC,'' SEL FROM DEM_HOLIDAY_CALENDER DHC WHERE DHC.HOLIDAY_DATE BETWEEN TO_DATE('" + @from + "', dateformat) AND TO_DATE('" + todt + "', dateformat) ORDER BY DHC.HOLIDAY_DATE)q";
        //        }
        //        else if (!string.IsNullOrEmpty(@from))
        //        {
        //            strSql = "SELECT ROWNUM SLNR,q.* FROM (SELECT '' DEM_SLAB_CALENDER_PK, DHC.HOLIDAY_DATE, DHC.HOLIDAY_DESC,'' SEL FROM DEM_HOLIDAY_CALENDER DHC WHERE TO_DATE(DHC.HOLIDAY_DATE, dateformat) >= TO_DATE('" + @from + "', dateformat) ORDER BY DHC.HOLIDAY_DATE)q";
        //        }
        //        else
        //        {
        //            strSql = "SELECT ROWNUM SLNR,q.* FROM (SELECT '' DEM_SLAB_CALENDER_PK, DHC.HOLIDAY_DATE, DHC.HOLIDAY_DESC,'' SEL FROM DEM_HOLIDAY_CALENDER DHC WHERE DHC.HOLIDAY_DATE BETWEEN TO_DATE('01/09/2000', dateformat) AND TO_DATE('01/10/2000', dateformat) ORDER BY DHC.HOLIDAY_DATE)q";
        //        }
        //    }
        //    return objWF.GetDataSet(strSql);
        //}

        //#endregion "FetchData"

        //#region "Check For Existing Records"

        //public long HDRAlreadyExistsClashing_withoutToDt(int PortPK, int ContainerPK, System.DateTime ValidFrom, string ValidTo, string form_flag, int pk, string process, int terminal = 0)
        //{
        //    string strSql = null;
        //    DataSet TempDs = null;
        //    DataSet TempDs1 = null;
        //    long HDRPK = 0;
        //    if (ValidTo.Trim().Trim().Length > 0)
        //    {
        //        strSql = string.Empty;
        //        strSql += " select DISTINCT t.dem_slab_hdr_pk ";
        //        strSql += " from   dem_slab_hdr t, dem_slab_trn dt ";
        //        strSql += " where dt.dem_slab_hdr_fk=t.dem_slab_hdr_pk AND t.port_mst_fk = " + PortPK + " And dt.container_type_mst_fk = " + ContainerPK + "";

        //        strSql += " and (( to_date('" + ValidFrom + "') >= t.valid_from ";
        //        strSql += " and t.valid_to is null ) or ";
        //        strSql += " (to_date('" + ValidTo + "') between t.valid_from and t.valid_to  ";
        //        strSql += " ))  AND t.form_flag=" + form_flag;
        //        strSql += "  AND t.process=" + process;
        //    }
        //    else
        //    {
        //        strSql = string.Empty;
        //        strSql += " select DISTINCT t.dem_slab_hdr_pk ";
        //        strSql += " from   dem_slab_hdr t, dem_slab_trn dt ";
        //        strSql += " where dt.dem_slab_hdr_fk=t.dem_slab_hdr_pk AND t.port_mst_fk = " + PortPK + " And dt.container_type_mst_fk = " + ContainerPK + "";

        //        strSql += " and ( to_date('" + ValidFrom + "') >= t.valid_from ";
        //        strSql += " and t.valid_to is null )  AND t.form_flag=" + form_flag;
        //        strSql += "  AND t.process=" + process;
        //    }
        //    if (pk != 0)
        //    {
        //        strSql += "   AND t.dem_slab_hdr_pk<>" + pk;
        //    }
        //    if (form_flag == 3)
        //    {
        //        if (terminal == 0)
        //        {
        //            strSql += "  AND t.terminal_mst_fk is NULL ";
        //        }
        //        else
        //        {
        //            strSql += "  AND t.terminal_mst_fk=" + terminal;
        //        }
        //    }

        //    WorkFlow objWF = new WorkFlow();
        //    TempDs = objWF.GetDataSet(strSql);
        //    if (TempDs.Tables[0].Rows.Count > 0)
        //    {
        //        if (TempDs.Tables[0].Rows.Count > 0)
        //        {
        //            return -1;
        //        }
        //        else
        //        {
        //            return 0;
        //        }
        //    }
        //}

        //#endregion "Check For Existing Records"

        //#region "Region ContainerId"

        //public object FetchContainerId(int containerPk)
        //{
        //    WorkFlow objWF = new WorkFlow();
        //    System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
        //    sb.Append("");
        //    sb.Append(" SELECT CT.CONTAINER_TYPE_MST_ID CONTAINER_ID ");
        //    sb.Append("  FROM CONTAINER_TYPE_MST_TBL CT ");
        //    sb.Append(" WHERE CT.CONTAINER_TYPE_MST_PK =");
        //    sb.Append(containerPk);

        //    try
        //    {
        //        return objWF.GetDataSet(sb.ToString());
        //    }
        //    catch (Exception exp)
        //    {
        //        ErrorMessage = exp.Message;
        //        throw exp;
        //    }
        //}

        //#endregion "Region ContainerId"

        //#region "Fetch Function"

        //public DataSet FetchAll(string P_TariffRefNo = "", string P_ValidAsOn = "", string P_Port = "", string fromdate = "", string todate = "", string SearchType = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string LogInLocFK = "0", bool IsAdmin = false,
        //Int32 ChkONLD = 0, string form_flag = "", int process = 0, string Terminal = "")
        //{
        //    Int32 last = default(Int32);
        //    Int32 start = default(Int32);
        //    string strSQL = null;
        //    string strSQL1 = null;
        //    string strCondition = null;
        //    Int32 TotalRecords = default(Int32);
        //    WorkFlow objWF = new WorkFlow();
        //    strCondition = strCondition + " AND T.FORM_FLAG=" + form_flag;
        //    if (P_Port.ToString().Trim().Length > 0)
        //    {
        //        strCondition = strCondition + " AND T.PORT_MST_FK = '" + P_Port + "' ";
        //    }
        //    if (process != 0)
        //    {
        //        strCondition = strCondition + "AND T.PROCESS=" + process;
        //    }
        //    if (!string.IsNullOrEmpty(Terminal))
        //    {
        //        strCondition = strCondition + "AND T.TERMINAL_MST_FK=" + Terminal;
        //    }
        //    if (fromdate.Trim().Length > 0)
        //    {
        //        strCondition = strCondition + "AND TO_DATE(TO_DATE(T.VALID_FROM,'" + HttpContext.Current.Session["DATE_FORMAT"] + "'),'" + HttpContext.Current.Session["DATE_FORMAT"] + "') >= TO_DATE(' " + fromdate + " ','" + HttpContext.Current.Session["DATE_FORMAT"] + "')  ";
        //    }
        //    if (todate.Trim().Length > 0)
        //    {
        //        strCondition = strCondition + " AND TO_DATE(TO_DATE(T.VALID_TO,'" + HttpContext.Current.Session["DATE_FORMAT"] + "'),'" + HttpContext.Current.Session["DATE_FORMAT"] + "') <= TO_DATE(' " + todate + " ','" + HttpContext.Current.Session["DATE_FORMAT"] + "')  ";
        //    }
        //    if (P_ValidAsOn.Trim().Length > 0)
        //    {
        //        strCondition = strCondition + " AND TO_DATE(TO_DATE(T.SLAB_DATE,'" + HttpContext.Current.Session["DATE_FORMAT"] + "'),'" + HttpContext.Current.Session["DATE_FORMAT"] + "') = TO_DATE(' " + P_ValidAsOn + " ','" + HttpContext.Current.Session["DATE_FORMAT"] + "')  ";
        //    }
        //    if (P_TariffRefNo.ToString().Trim().Length > 0)
        //    {
        //        if (SearchType == "C")
        //        {
        //            strCondition = strCondition + " AND UPPER(T.REFERENCE_NR) LIKE  '%" + P_TariffRefNo.ToUpper() + "%' ";
        //        }
        //        else
        //        {
        //            strCondition = strCondition + " AND UPPER(T.REFERENCE_NR)  LIKE  '" + P_TariffRefNo.ToUpper() + "%' ";
        //        }
        //    }

        //    if (ChkONLD == 0)
        //    {
        //        strCondition = strCondition + " AND 1 = 2 ";
        //    }

        //    strSQL += " SELECT DISTINCT T.REFERENCE_NR,TO_DATE(T.SLAB_DATE,'" + HttpContext.Current.Session["DATE_FORMAT"] + "') TARIFF_DATE, ";

        //    strSQL += " DECODE(T.PROCESS,1,'EXPORT','IMPORT') PROCESS,";
        //    strSQL += " T.OPERATOR_ID,";
        //    strSQL += " (ROWTOCOL('SELECT DISTINCT DT.ICD_PORT FROM DEM_SLAB_TRN DT ";
        //    strSQL += " WHERE DT.DEM_SLAB_HDR_FK=' || T.DEM_SLAB_HDR_PK || '')) TERMINAL, ";

        //    strSQL += "   T.CURRENCY_ID,TO_DATE(T.VALID_FROM,'" + HttpContext.Current.Session["DATE_FORMAT"] + "') VALID_FROM, TO_DATE(T.VALID_TO,'" + HttpContext.Current.Session["DATE_FORMAT"] + "') VALID_TO, T.DEM_SLAB_HDR_PK";
        //    strSQL += " FROM  VIEW_DEMMURRAGE_LIST  T WHERE 1= 1";
        //    strSQL += strCondition;

        //    strSQL1 = "SELECT COUNT(*) from (";
        //    strSQL1 += strSQL.ToString() + ")";
        //    TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL1));
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
        //    strSQL1 = "";
        //    strSQL1 = " SELECT * FROM (SELECT ROWNUM SR_NO, Q.* FROM(";
        //    strSQL1 += strSQL.ToString();
        //    strSQL1 += "ORDER BY TO_DATE(TARIFF_DATE,DATEFORMAT) DESC,T.REFERENCE_NR DESC";
        //    strSQL1 += " ) Q) WHERE SR_NO  BETWEEN " + start + " AND " + last;

        //    try
        //    {
        //        return objWF.GetDataSet(strSQL1.ToString());
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

        //#endregion "Fetch Function"

        //#region "Function to check whether a user is an administrator or not"

        //public int IsAdministrator(string strUserID)
        //{
        //    string strSQL = null;
        //    Int16 Admin = default(Int16);
        //    WorkFlow objWF = new WorkFlow();
        //    strSQL = "SELECT COUNT(*) FROM User_Mst_Tbl U WHERE U.ROLE_MST_FK = ";
        //    strSQL = strSQL + "(SELECT R.ROLE_MST_TBL_PK FROM ROLE_MST_TBL R WHERE R.ROLE_ID = 'ADMIN')";
        //    strSQL = strSQL + "AND U.USER_MST_PK = " + HttpContext.Current.Session["USER_PK"];
        //    try
        //    {
        //        Admin = objWF.ExecuteScaler(strSQL.ToString());
        //        if (Admin == 1)
        //        {
        //            return true;
        //        }
        //        else
        //        {
        //            return false;
        //        }
        //    }
        //    catch (OracleException sqlExp)
        //    {
        //        throw sqlExp;
        //    }
        //    catch (Exception exp)
        //    {
        //        throw exp;
        //    }
        //}

        //#endregion "Function to check whether a user is an administrator or not"

        //#region "DeleteSlabTRN.."

        //public ArrayList DeleteHoliday(string DEM_SLAB_CALENDER_PK_IN, long DELETED_BY_FK_IN, long CONFIG_MST_FK_IN)
        //{
        //    WorkFlow objWK = new WorkFlow();
        //    objWK.OpenConnection();
        //    OracleTransaction TRAN = null;
        //    TRAN = objWK.MyConnection.BeginTransaction();

        //    OracleCommand delCommand = new OracleCommand();
        //    Int16 RETURN_VALUE = default(Int16);
        //    Array a = null;
        //    if (!string.IsNullOrEmpty(DEM_SLAB_CALENDER_PK_IN))
        //    {
        //        a = DEM_SLAB_CALENDER_PK_IN.Split(',');
        //    }
        //    Int16 i = default(Int16);
        //    for (i = 0; i <= a.Length - 1; i++)
        //    {
        //        try
        //        {
        //            var _with1 = delCommand;
        //            _with1.Connection = objWK.MyConnection;
        //            _with1.CommandType = CommandType.StoredProcedure;
        //            _with1.CommandText = objWK.MyUserName + ".dem_slab_calender_pkg.dem_slab_calender_del";
        //            var _with2 = _with1.Parameters;
        //            _with2.Clear();
        //            delCommand.Parameters.Add("DEM_SLAB_CALENDER_PK_IN", a(i)).Direction = ParameterDirection.Input;
        //            delCommand.Parameters.Add("DELETED_BY_FK_IN", DELETED_BY_FK_IN).Direction = ParameterDirection.Input;
        //            delCommand.Parameters.Add("CONFIG_MST_FK_IN", CONFIG_MST_FK_IN).Direction = ParameterDirection.Input;
        //            delCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
        //            delCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

        //            var _with3 = objWK.MyDataAdapter;
        //            _with3.DeleteCommand = delCommand;
        //            _with3.DeleteCommand.Transaction = TRAN;
        //            _with3.DeleteCommand.ExecuteNonQuery();
        //        }
        //        catch (OracleException oraexp)
        //        {
        //            arrMessage.Add(oraexp.Message);
        //        }
        //        catch (Exception ex)
        //        {
        //            arrMessage.Add(ex.Message);
        //        }
        //    }
        //    if (arrMessage.Count > 0)
        //    {
        //        TRAN.Rollback();
        //    }
        //    else
        //    {
        //        TRAN.Commit();
        //    }
        //    return arrMessage;
        //}

        //#endregion "DeleteSlabTRN.."

        //#region "Fetch Existing Containers"

        //public object FetchExistingContainers(int detentionHdrPk)
        //{
        //    WorkFlow objWF = new WorkFlow();
        //    System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
        //    sb.Append("SELECT DISTINCT C.CONTAINER_TYPE_MST_FK CONTAINER_TYPE_MST_PK,");
        //    sb.Append("  CT.CONTAINER_TYPE_MST_ID, CT.CONTAINER_TYPE_NAME, CT.PREFERENCES ");
        //    sb.Append("  FROM dem_slab_trn C, CONTAINER_TYPE_MST_TBL CT");
        //    sb.Append(" WHERE C.CONTAINER_TYPE_MST_FK = CT.CONTAINER_TYPE_MST_PK");
        //    sb.Append("  AND C.dem_slab_hdr_fk =");
        //    sb.Append(detentionHdrPk);
        //    sb.Append(" Order By CT.PREFERENCES ");
        //    try
        //    {
        //        return objWF.GetDataSet(sb.ToString());
        //    }
        //    catch (Exception exp)
        //    {
        //        ErrorMessage = exp.Message;
        //        throw exp;
        //    }
        //}

        //#endregion "Fetch Existing Containers"

        //#region "Generate Protocol"

        //public string GenerateProtocol(long LocPk, long Emp_Pk, long userid, string STR, string SID = "")
        //{
        //    System.Web.UI.Page objPage = new System.Web.UI.Page();
        //    return GenerateProtocolKey(STR, LocPk, Emp_Pk, DateTime.Now, , , , userid, , SID) + "";
        //}

        //#endregion "Generate Protocol"

        //#region "Fetch HeaderDetail"

        //public DataSet FetchHeaderDetail(string Pk_Value)
        //{
        //    string StrSQL = null;
        //    try
        //    {
        //        StrSQL = string.Empty;
        //        StrSQL += " SELECT  Port.operator_Mst_Pk, Port.operator_Id, Port.operator_Name,  Cu.Currency_Mst_Pk, t.valid_from, t.valid_to,t.slab_date,t.reference_nr, t.apply_freedays,t.freedays,t.weekends";
        //        StrSQL += "   ,t.process,T.TARIFF_TYPE,PMT.PORT_MST_PK,PMT.PORT_ID,PMT.PORT_NAME FROM  dem_slab_hdr t, operator_Mst_Tbl Port, Currency_Type_Mst_Tbl Cu,PORT_MST_TBL PMT ";
        //        StrSQL += "   WHERE t.port_mst_fk = PMT.PORT_MST_PK(+) ";
        //        StrSQL += "         AND t.currency_mst_fk = Cu.Currency_Mst_Pk ";
        //        StrSQL += "         AND t.dem_slab_hdr_pk = " + Pk_Value;
        //        StrSQL += "        AND t.terminal_mst_fk=Port.operator_Mst_Pk(+) ";
        //        WorkFlow objWf = new WorkFlow();
        //        return objWf.GetDataSet(StrSQL);
        //    }
        //    catch (OracleException sqlExp)
        //    {
        //        throw sqlExp;
        //    }
        //    catch (Exception exp)
        //    {
        //        throw exp;
        //    }
        //}

        //#endregion "Fetch HeaderDetail"

        //#region "FetchTRN"

        //public DataSet FetchTRN(string PKValue, DataSet dsactive)
        //{
        //    string strSql = null;
        //    int I = 0;
        //    strSql = string.Empty;
        //    strSql = "SELECT ROWNUM SR_NO,Q.* FROM (SELECT  ICD_PORT, DECODE(Period,1,'First',2,'Next',3,'Thereafter') Period, DAYS, ";

        //    if ((dsactive != null))
        //    {
        //        for (I = 0; I <= dsactive.Tables[0].Rows.Count - 1; I++)
        //        {
        //            strSql += "SUM(CTPK" + Strings.Trim(I) + ") " + "CTPK" + Strings.Trim(I) + ", ";
        //            strSql += "SUM(PK" + Strings.Trim(I) + ") " + "PK" + Strings.Trim(I) + ", ";
        //            strSql += "SUM(RATE" + Strings.Trim(I) + ") " + "RATE" + Strings.Trim(I) + ", ";
        //        }
        //        strSql = Strings.Mid(strSql, 1, Strings.Len(strSql) - 2) + " ";
        //    }
        //    strSql += ", DEL ,'' ID, STATUS,CHEFLAG,dem_slab_hdr_fk ";

        //    if ((dsactive != null))
        //    {
        //        strSql += " FROM (SELECT ceil(ROWNUM/" + dsactive.Tables[0].Rows.Count + ") sr_no,T.ICD_PORT,t.dem_slab_hdr_fk,t.Period,t.Days, ";
        //        for (I = 0; I <= dsactive.Tables[0].Rows.Count - 1; I++)
        //        {
        //            strSql += " DECODE(T.CONTAINER_TYPE_MST_FK, " + dsactive.Tables[0].Rows[I]["CONTAINER_TYPE_MST_PK"] + ",T.CONTAINER_TYPE_MST_FK, NULL) CTPK" + Strings.Trim(I) + ", ";
        //            strSql += " DECODE(T.CONTAINER_TYPE_MST_FK, " + dsactive.Tables[0].Rows[I]["CONTAINER_TYPE_MST_PK"] + ",T.dem_slab_trn_pk, NULL) PK" + Strings.Trim(I) + ", ";
        //            strSql += " DECODE(T.CONTAINER_TYPE_MST_FK, " + dsactive.Tables[0].Rows[I]["CONTAINER_TYPE_MST_PK"] + ",nvl(T.slab_rate,0), NULL) RATE" + Strings.Trim(I) + ", ";
        //        }
        //    }
        //    else
        //    {
        //        strSql += " FROM (SELECT ROWNUM sr_no,t.dem_slab_hdr_fk,t.Period,t.Days, ";
        //    }
        //    strSql += " '' Del, 'A' Status, '' CheFlag ";
        //    strSql += " FROM  dem_slab_trn t ";
        //    strSql += " WHERE   t.dem_slab_hdr_fk = " + PKValue;
        //    strSql += " ) GROUP BY ICD_PORT,dem_slab_hdr_fk, PERIOD, DAYS,DEL, ";
        //    strSql += "  STATUS,CHEFLAG ";
        //    strSql += " ORDER BY ICD_PORT, PERIOD, DAYS)Q";
        //    WorkFlow objWF = new WorkFlow();
        //    return objWF.GetDataSet(strSql);
        //}

        //#endregion "FetchTRN"

        //#region "SaveHDRINS"

        //public ArrayList SaveHDRINS(string TARIFF_REF_NO_IN, bool chkExisting, System.DateTime TARIFF_DATE_IN, System.DateTime VALID_FROM_DT_IN, System.DateTime VALID_TO_DT_IN, int PORT_MST_FK_IN, int CURRENCY_MST_FK_IN, int TType, string WEEKENDS_IN, int PROCESS_IN,
        //int FORM_FLAG_IN, int TERMINAL_MST_FK_IN, int CREATED_BY_FK_IN, string ConfigurationPK, DataSet GridDS, DataSet DsHoliday, string DELHolPK)
        //{
        //    Int16 RETURN_VALUE = default(Int16);
        //    WorkFlow objWK = new WorkFlow();
        //    objWK.OpenConnection();
        //    OracleTransaction TRAN = null;
        //    TRAN = objWK.MyConnection.BeginTransaction();

        //    OracleCommand insCommand = new OracleCommand();
        //    string g_PKValue = null;

        //    try
        //    {
        //        var _with4 = insCommand;
        //        _with4.Connection = objWK.MyConnection;
        //        _with4.CommandType = CommandType.StoredProcedure;
        //        _with4.CommandText = objWK.MyUserName + ".dem_slab_hdr_pkg.dem_slab_hdr_ins";
        //        var _with5 = _with4.Parameters;
        //        insCommand.Parameters.Add("REFERENCE_NR_IN", OracleDbType.Varchar2, 20, "TARIFF_REF_NO").Direction = ParameterDirection.Input;
        //        insCommand.Parameters["REFERENCE_NR_IN"].Value = TARIFF_REF_NO_IN;
        //        insCommand.Parameters.Add("SLAB_DATE_IN", OracleDbType.Date, 0, "TARIFF_DATE").Direction = ParameterDirection.Input;
        //        insCommand.Parameters["SLAB_DATE_IN"].Value = TARIFF_DATE_IN.DateTime;
        //        insCommand.Parameters.Add("VALID_FROM_IN", OracleDbType.Date, 0, "VALID_FROM_DT").Direction = ParameterDirection.Input;
        //        insCommand.Parameters["VALID_FROM_IN"].Value = VALID_FROM_DT_IN.DateTime;
        //        insCommand.Parameters.Add("PROCESS_IN", OracleDbType.Int32, 0, "PROCESS_IN").Direction = ParameterDirection.Input;
        //        insCommand.Parameters["PROCESS_IN"].Value = PROCESS_IN;
        //        insCommand.Parameters.Add("FORM_FLAG_IN", OracleDbType.Int32, 0, "FORM_FLAG_IN").Direction = ParameterDirection.Input;
        //        insCommand.Parameters["FORM_FLAG_IN"].Value = FORM_FLAG_IN;
        //        insCommand.Parameters.Add("TERMINAL_MST_FK_IN", OracleDbType.Int32, 0, "TERMINAL_MST_FK_IN").Direction = ParameterDirection.Input;
        //        if (TERMINAL_MST_FK_IN == 0)
        //        {
        //            insCommand.Parameters["TERMINAL_MST_FK_IN"].Value = Convert.DBNull;
        //        }
        //        else
        //        {
        //            insCommand.Parameters["TERMINAL_MST_FK_IN"].Value = TERMINAL_MST_FK_IN;
        //        }
        //        insCommand.Parameters.Add("VALID_TO_IN", OracleDbType.Date, 0, "VALID_TO_DT").Direction = ParameterDirection.Input;
        //        System.DateTime dte = default(System.DateTime);
        //        if (VALID_TO_DT_IN == dte)
        //        {
        //            insCommand.Parameters["VALID_TO_IN"].Value = Convert.DBNull;
        //        }
        //        else
        //        {
        //            insCommand.Parameters["VALID_TO_IN"].Value = VALID_TO_DT_IN.DateTime;
        //        }
        //        insCommand.Parameters.Add("PORT_MST_FK_IN", OracleDbType.Int32, 10, "PORT_MST_FK").Direction = ParameterDirection.Input;
        //        insCommand.Parameters["PORT_MST_FK_IN"].Value = PORT_MST_FK_IN;

        //        insCommand.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
        //        insCommand.Parameters["CURRENCY_MST_FK_IN"].Value = CURRENCY_MST_FK_IN;

        //        insCommand.Parameters.Add("TTYPE_IN", OracleDbType.Int32, 10, "TTYPE").Direction = ParameterDirection.Input;
        //        insCommand.Parameters["TTYPE_IN"].Value = TType;

        //        insCommand.Parameters.Add("WEEKENDS_IN", OracleDbType.Varchar2, 20, "WEEKENDS_IN").Direction = ParameterDirection.Input;
        //        if (string.IsNullOrEmpty(WEEKENDS_IN))
        //        {
        //            insCommand.Parameters["WEEKENDS_IN"].Value = Convert.DBNull;
        //        }
        //        else
        //        {
        //            insCommand.Parameters["WEEKENDS_IN"].Value = WEEKENDS_IN;
        //        }

        //        insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
        //        insCommand.Parameters["CREATED_BY_FK_IN"].Value = CREATED_BY_FK_IN;
        //        insCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
        //        insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "dem_slab_hdr_PK").Direction = ParameterDirection.Output;
        //        insCommand.Parameters["RETURN_VALUE"].Value = RETURN_VALUE;
        //        _with4.Transaction = TRAN;
        //        _with4.ExecuteNonQuery();
        //        g_PKValue = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
        //    }
        //    catch (OracleException oraexp)
        //    {
        //        arrMessage.Add(oraexp.Message);
        //    }
        //    catch (Exception ex)
        //    {
        //        arrMessage.Add(ex.Message);
        //    }
        //    if (arrMessage.Count > 0)
        //    {
        //        return arrMessage;
        //    }
        //    else
        //    {
        //        arrMessage = SaveTRN(GridDS, g_PKValue, chkExisting, CREATED_BY_FK_IN, ConfigurationPK, TRAN);
        //        if (arrMessage.Count > 0)
        //        {
        //            TRAN.Rollback();
        //            return arrMessage;
        //        }
        //        else
        //        {
        //            arrMessage = SaveHoliday(DsHoliday, g_PKValue, CREATED_BY_FK_IN, ConfigurationPK, DELHolPK, TRAN);
        //            if (arrMessage.Count > 0)
        //            {
        //                TRAN.Rollback();
        //                return arrMessage;
        //            }
        //            else
        //            {
        //                TRAN.Commit();
        //                arrMessage.Add("All Data Saved Successfully");
        //                arrMessage.Add(g_PKValue);
        //                return arrMessage;
        //            }
        //        }
        //    }
        //}

        //#endregion "SaveHDRINS"

        //#region "SaveHDRUPD"

        //public ArrayList SaveHDRUPD(string dem_slab_hdr_PK_IN, bool chkExisting, System.DateTime TARIFF_DATE_IN, System.DateTime VALID_FROM_DT_IN, string VALID_TO_DT_IN, int PORT_MST_FK_IN, int CURRENCY_MST_FK_IN, int TType, string WEEKENDS_IN, int PROCESS_IN,
        //int TERMINAL_MST_FK_IN, int LAST_MODIFIED_BY_FK_IN, string ConfigurationPK, DataSet GridDS, DataSet DsHoliday, string DELHolPK)
        //{
        //    Int16 RETURN_VALUE = default(Int16);
        //    WorkFlow objWK = new WorkFlow();
        //    objWK.OpenConnection();
        //    OracleTransaction TRAN = null;
        //    TRAN = objWK.MyConnection.BeginTransaction();

        //    OracleCommand updCommand = new OracleCommand();

        //    try
        //    {
        //        var _with6 = updCommand;
        //        _with6.Connection = objWK.MyConnection;
        //        _with6.CommandType = CommandType.StoredProcedure;
        //        _with6.CommandText = objWK.MyUserName + ".dem_slab_hdr_pkg.dem_slab_hdr_upd";
        //        var _with7 = _with6.Parameters;

        //        updCommand.Parameters.Add("dem_slab_hdr_PK_IN", OracleDbType.Int32, 10, "CONTAINER_DETENTION_HDR_PK").Direction = ParameterDirection.Input;
        //        updCommand.Parameters["dem_slab_hdr_PK_IN"].Value = dem_slab_hdr_PK_IN;

        //        updCommand.Parameters.Add("SLAB_DATE_IN", OracleDbType.Date, 0, "TARIFF_DATE").Direction = ParameterDirection.Input;
        //        updCommand.Parameters["SLAB_DATE_IN"].Value = TARIFF_DATE_IN;
        //        updCommand.Parameters.Add("VALID_FROM_IN", OracleDbType.Date, 0, "VALID_FROM_DT").Direction = ParameterDirection.Input;
        //        updCommand.Parameters["VALID_FROM_IN"].Value = VALID_FROM_DT_IN;
        //        updCommand.Parameters.Add("VALID_TO_IN", OracleDbType.Date, 0, "VALID_TO_DT").Direction = ParameterDirection.Input;
        //        if (string.IsNullOrEmpty(VALID_TO_DT_IN))
        //        {
        //            updCommand.Parameters["VALID_TO_IN"].Value = Convert.DBNull;
        //        }
        //        else
        //        {
        //            updCommand.Parameters["VALID_TO_IN"].Value = VALID_TO_DT_IN;
        //        }
        //        updCommand.Parameters.Add("PORT_MST_FK_IN", OracleDbType.Int32, 10, "PORT_MST_FK").Direction = ParameterDirection.Input;
        //        updCommand.Parameters["PORT_MST_FK_IN"].Value = PORT_MST_FK_IN;

        //        updCommand.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
        //        updCommand.Parameters["CURRENCY_MST_FK_IN"].Value = CURRENCY_MST_FK_IN;

        //        updCommand.Parameters.Add("TTYPE_IN", OracleDbType.Int32, 10, "TTYPE").Direction = ParameterDirection.Input;
        //        updCommand.Parameters["TTYPE_IN"].Value = TType;

        //        updCommand.Parameters.Add("WEEKENDS_IN", OracleDbType.Varchar2, 20, "WEEKENDS_IN").Direction = ParameterDirection.Input;
        //        if (string.IsNullOrEmpty(WEEKENDS_IN))
        //        {
        //            updCommand.Parameters["WEEKENDS_IN"].Value = Convert.DBNull;
        //        }
        //        else
        //        {
        //            updCommand.Parameters["WEEKENDS_IN"].Value = WEEKENDS_IN;
        //        }
        //        updCommand.Parameters.Add("PROCESS_IN", OracleDbType.Int32, 0, "PROCESS_IN").Direction = ParameterDirection.Input;
        //        updCommand.Parameters["PROCESS_IN"].Value = PROCESS_IN;
        //        updCommand.Parameters.Add("TERMINAL_MST_FK_IN", OracleDbType.Int32, 0, "TERMINAL_MST_FK_IN").Direction = ParameterDirection.Input;
        //        if (TERMINAL_MST_FK_IN == 0)
        //        {
        //            updCommand.Parameters["TERMINAL_MST_FK_IN"].Value = Convert.DBNull;
        //        }
        //        else
        //        {
        //            updCommand.Parameters["TERMINAL_MST_FK_IN"].Value = TERMINAL_MST_FK_IN;
        //        }
        //        updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", LAST_MODIFIED_BY_FK_IN).Direction = ParameterDirection.Input;
        //        updCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

        //        updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
        //        updCommand.Parameters["RETURN_VALUE"].Value = RETURN_VALUE;
        //        _with6.Transaction = TRAN;
        //        _with6.ExecuteNonQuery();
        //    }
        //    catch (OracleException oraexp)
        //    {
        //        arrMessage.Add(oraexp.Message);
        //    }
        //    catch (Exception ex)
        //    {
        //        arrMessage.Add(ex.Message);
        //    }
        //    if (arrMessage.Count > 0)
        //    {
        //        return arrMessage;
        //    }
        //    else
        //    {
        //        arrMessage = SaveTRN(GridDS, dem_slab_hdr_PK_IN, chkExisting, LAST_MODIFIED_BY_FK_IN, ConfigurationPK, TRAN);
        //        if (arrMessage.Count > 0)
        //        {
        //            TRAN.Rollback();
        //            return arrMessage;
        //        }
        //        else
        //        {
        //            arrMessage = SaveHoliday(DsHoliday, dem_slab_hdr_PK_IN, LAST_MODIFIED_BY_FK_IN, ConfigurationPK, DELHolPK, TRAN);
        //            if (arrMessage.Count > 0)
        //            {
        //                TRAN.Rollback();
        //                return arrMessage;
        //            }
        //            else
        //            {
        //                TRAN.Commit();
        //                arrMessage.Add("All Data Saved Successfully");
        //                arrMessage.Add(dem_slab_hdr_PK_IN);
        //                return arrMessage;
        //            }
        //        }
        //    }
        //}

        //#endregion "SaveHDRUPD"

        //#region "SaveTRN"

        //public ArrayList SaveTRN(DataSet M_DataSet, int Detention_HDR_FK_IN, bool chkExisting, string created, string config, OracleTransaction TRAN)
        //{
        //    WorkFlow objWK = new WorkFlow();
        //    objWK.OpenConnection();
        //    objWK.MyConnection = TRAN.Connection;
        //    OracleCommand insCommand = new OracleCommand();
        //    Int16 RETURN_VALUE = default(Int16);

        //    try
        //    {
        //        int ROWCNT = 0;
        //        int standardFrtPk = 0;
        //        Int32 RowCntContainer = 0;
        //        var _with8 = insCommand;

        //        for (ROWCNT = 0; ROWCNT <= M_DataSet.Tables[0].Rows.Count - 1; ROWCNT++)
        //        {
        //            if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[ROWCNT]["DEM_SLAB_TRN_PK"]))
        //            {
        //                _with8.Connection = objWK.MyConnection;
        //                _with8.CommandType = CommandType.StoredProcedure;
        //                _with8.CommandText = objWK.MyUserName + ".dem_slab_trn_pkg.dem_slab_trn_ins";
        //                var _with9 = _with8.Parameters;
        //                _with9.Clear();
        //                _with9.Add("DEM_SLAB_HDR_FK_IN", Detention_HDR_FK_IN).Direction = ParameterDirection.Input;
        //                _with9.Add("CONTAINER_TYPE_MST_FK_IN", M_DataSet.Tables[0].Rows[ROWCNT]["CONTAINER_TYPE_MST_FK"]).Direction = ParameterDirection.Input;
        //                _with9.Add("PERIOD_IN", M_DataSet.Tables[0].Rows[ROWCNT]["PERIOD"]).Direction = ParameterDirection.Input;
        //                _with9.Add("DAYS_IN", M_DataSet.Tables[0].Rows[ROWCNT]["DAYS"]).Direction = ParameterDirection.Input;
        //                _with9.Add("SLAB_RATE_IN", M_DataSet.Tables[0].Rows[ROWCNT]["SLAB_RATE"]).Direction = ParameterDirection.Input;
        //                _with9.Add("ICD_PORT_IN", M_DataSet.Tables[0].Rows[ROWCNT]["PORT"]).Direction = ParameterDirection.Input;
        //                _with9.Add("CREATED_BY_FK_IN", created).Direction = ParameterDirection.Input;
        //                _with9.Add("CONFIG_MST_FK_IN", config).Direction = ParameterDirection.Input;
        //                _with9.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
        //                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

        //                try
        //                {
        //                    var _with10 = objWK.MyDataAdapter;
        //                    _with10.InsertCommand = insCommand;
        //                    _with10.InsertCommand.Transaction = TRAN;
        //                    _with10.InsertCommand.ExecuteNonQuery();
        //                }
        //                catch (OracleException oraexp)
        //                {
        //                    arrMessage.Add(oraexp.Message);
        //                }
        //                catch (Exception ex)
        //                {
        //                    arrMessage.Add(ex.Message);
        //                }
        //            }
        //            else
        //            {
        //                _with8.Connection = objWK.MyConnection;
        //                _with8.CommandType = CommandType.StoredProcedure;
        //                _with8.CommandText = objWK.MyUserName + ".dem_slab_trn_pkg.dem_slab_trn_upd";
        //                var _with11 = _with8.Parameters;
        //                _with11.Clear();
        //                _with11.Add("dem_slab_trn_pk_in", M_DataSet.Tables[0].Rows[ROWCNT]["DEM_SLAB_TRN_PK"]).Direction = ParameterDirection.Input;
        //                _with11.Add("dem_slab_hdr_fk_in", Detention_HDR_FK_IN).Direction = ParameterDirection.Input;
        //                _with11.Add("container_type_mst_fk_in", M_DataSet.Tables[0].Rows[ROWCNT]["CONTAINER_TYPE_MST_FK"]).Direction = ParameterDirection.Input;
        //                _with11.Add("period_in", M_DataSet.Tables[0].Rows[ROWCNT]["PERIOD"]).Direction = ParameterDirection.Input;
        //                _with11.Add("days_in", M_DataSet.Tables[0].Rows[ROWCNT]["DAYS"]).Direction = ParameterDirection.Input;
        //                _with11.Add("slab_rate_in", M_DataSet.Tables[0].Rows[ROWCNT]["SLAB_RATE"]).Direction = ParameterDirection.Input;
        //                _with11.Add("ICD_PORT_IN", M_DataSet.Tables[0].Rows[ROWCNT]["PORT"]).Direction = ParameterDirection.Input;
        //                _with11.Add("last_modified_by_fk_in", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
        //                _with11.Add("config_mst_fk_in", config).Direction = ParameterDirection.Input;
        //                _with11.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "DEM_SLAB_TRN_PK").Direction = ParameterDirection.Output;
        //                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
        //                try
        //                {
        //                    var _with12 = objWK.MyDataAdapter;
        //                    _with12.InsertCommand = insCommand;
        //                    _with12.InsertCommand.Transaction = TRAN;
        //                    _with12.InsertCommand.ExecuteNonQuery();
        //                }
        //                catch (OracleException oraexp)
        //                {
        //                    arrMessage.Add(oraexp.Message);
        //                }
        //                catch (Exception ex)
        //                {
        //                    arrMessage.Add(ex.Message);
        //                }
        //            }
        //        }

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

        //#endregion "SaveTRN"

        //#region "SaveHoliday"

        //public ArrayList SaveHoliday(DataSet M_DataSet, int Detention_HDR_FK_IN, string create, string config, string DELHolPK, OracleTransaction TRAN)
        //{
        //    WorkFlow objWK = new WorkFlow();
        //    objWK.OpenConnection();
        //    objWK.MyConnection = TRAN.Connection;
        //    OracleCommand insCommand = new OracleCommand();

        //    try
        //    {
        //        int ROWCNT = 0;
        //        int standardFrtPk = 0;
        //        Int32 RowCntContainer = 0;
        //        var _with13 = insCommand;

        //        for (ROWCNT = 0; ROWCNT <= M_DataSet.Tables[0].Rows.Count - 1; ROWCNT++)
        //        {
        //            if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[ROWCNT]["HDRPK"]))
        //            {
        //                _with13.Connection = objWK.MyConnection;
        //                _with13.CommandType = CommandType.StoredProcedure;
        //                _with13.CommandText = objWK.MyUserName + ".dem_slab_calender_pkg.DEM_SLAB_CALENDER_INS";
        //                var _with14 = _with13.Parameters;
        //                _with14.Clear();
        //                _with14.Add("DEM_SLAB_HDR_FK_IN", Detention_HDR_FK_IN).Direction = ParameterDirection.Input;
        //                System.DateTime dt = M_DataSet.Tables[0].Rows[ROWCNT]["HOLYDATE"];
        //                _with14.Add("HOLIDAY_DATE_IN", dt.DateTime).Direction = ParameterDirection.Input;
        //                _with14.Add("HOLIDAY_DESC_IN", M_DataSet.Tables[0].Rows[ROWCNT]["HOLYDESC"]).Direction = ParameterDirection.Input;
        //                _with14.Add("CREATED_BY_FK_IN", create).Direction = ParameterDirection.Input;
        //                _with14.Add("CONFIG_MST_FK_IN", config).Direction = ParameterDirection.Input;
        //                _with14.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
        //                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

        //                try
        //                {
        //                    var _with15 = objWK.MyDataAdapter;
        //                    _with15.InsertCommand = insCommand;
        //                    _with15.InsertCommand.Transaction = TRAN;
        //                    _with15.InsertCommand.ExecuteNonQuery();
        //                }
        //                catch (OracleException oraexp)
        //                {
        //                    arrMessage.Add(oraexp.Message);
        //                }
        //                catch (Exception ex)
        //                {
        //                    arrMessage.Add(ex.Message);
        //                }
        //            }
        //            else
        //            {
        //                _with13.Connection = objWK.MyConnection;
        //                _with13.CommandType = CommandType.StoredProcedure;
        //                _with13.CommandText = objWK.MyUserName + ".dem_slab_calender_pkg.DEM_SLAB_CALENDER_UPD";
        //                var _with16 = _with13.Parameters;
        //                _with16.Clear();
        //                _with16.Add("DEM_SLAB_CALENDER_PK_IN", M_DataSet.Tables[0].Rows[ROWCNT]["HDRPK"]).Direction = ParameterDirection.Input;
        //                _with16.Add("DEM_SLAB_HDR_FK_IN", Detention_HDR_FK_IN).Direction = ParameterDirection.Input;
        //                System.DateTime dt = M_DataSet.Tables[0].Rows[ROWCNT]["HOLYDATE"];
        //                _with16.Add("HOLIDAY_DATE_IN", dt.DateTime).Direction = ParameterDirection.Input;
        //                _with16.Add("HOLIDAY_DESC_IN", M_DataSet.Tables[0].Rows[ROWCNT]["HOLYDESC"]).Direction = ParameterDirection.Input;
        //                _with16.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
        //                _with16.Add("CONFIG_MST_FK_IN", config).Direction = ParameterDirection.Input;
        //                _with16.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "DEM_SLAB_TRN_PK").Direction = ParameterDirection.Output;
        //                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
        //                try
        //                {
        //                    var _with17 = objWK.MyDataAdapter;
        //                    _with17.InsertCommand = insCommand;
        //                    _with17.InsertCommand.Transaction = TRAN;
        //                    _with17.InsertCommand.ExecuteNonQuery();
        //                }
        //                catch (OracleException oraexp)
        //                {
        //                    arrMessage.Add(oraexp.Message);
        //                }
        //                catch (Exception ex)
        //                {
        //                    arrMessage.Add(ex.Message);
        //                }
        //            }
        //        }
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
        //        TRAN.Rollback();
        //        throw oraexp;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //#endregion "SaveHoliday"

        //#region "Fetch DropDown"

        //public DataSet FetchDropDown(string config, string flag)
        //{
        //    string StrSQL = null;
        //    try
        //    {
        //        StrSQL += "select dd.dd_value,dd.dd_id from qfor_drop_down_tbl dd";
        //        StrSQL += " where dd.config_id='" + config + "' AND dd.dd_flag='" + flag + "'  ORDER BY dd.dropdown_pk ";

        //        WorkFlow objWF = new WorkFlow();
        //        DataSet objDS = null;
        //        objDS = objWF.GetDataSet(StrSQL);
        //        return objDS;
        //    }
        //    catch (OracleException sqlExp)
        //    {
        //        throw sqlExp;
        //    }
        //    catch (Exception exp)
        //    {
        //        throw exp;
        //    }
        //}

        //#endregion "Fetch DropDown"
    }
}