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
    public class clsDemurrageCalculation : CommonFeatures
    {
        //private string strReturnPK;

        //public string pkstrReturn
        //{
        //    get { return strReturnPK; }
        //}

        //#region " Demurrage Calculation Listing Screen "

        //private WorkFlow objWF = new WorkFlow();

        //#region "Fetch Demurrage List "

        //public DataSet FetchDemList(string JOB_CARD_REF_NO = "", string DEMURRAGE_REF_NO = "", string DEPOT_ID = "", string SearchType = "", string SortExpression = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " ASC ", string SortCol = "", Int64 JOB_CARD_AIR_IMP_FK = 0,
        //Int64 DEMURRAGE_CALC_TBL_PK = 0, Int64 DEPOT_MST_FK = 0, long lngUsrLocFk = 0, Int32 flag = 0)
        //{
        //    Int32 last = default(Int32);
        //    Int32 start = default(Int32);
        //    string strSQL = null;
        //    string strCondition = null;
        //    Int32 TotalRecords = default(Int32);
        //    WorkFlow objWF = new WorkFlow();

        //    if (JOB_CARD_AIR_IMP_FK > 0)
        //    {
        //        strCondition += " AND DCT.JOB_CARD_AIR_IMP_FK=" + JOB_CARD_AIR_IMP_FK;
        //    }
        //    if (DEMURRAGE_CALC_TBL_PK > 0)
        //    {
        //        strCondition += " AND DCT.DEMURRAGE_CALC_TBL_PK=" + DEMURRAGE_CALC_TBL_PK;
        //    }

        //    if (DEPOT_MST_FK > 0)
        //    {
        //        strCondition += " AND DCT.DEPOT_MST_FK=" + DEPOT_MST_FK;
        //    }

        //    if (JOB_CARD_REF_NO.Trim().Length > 0)
        //    {
        //        if (SearchType.ToString().Trim().Length > 0)
        //        {
        //            if (SearchType == "S")
        //            {
        //                strCondition += " AND UPPER(JCSIT.JOBCARD_REF_NO) LIKE '" + JOB_CARD_REF_NO.ToUpper().Replace("'", "''") + "%'";
        //            }
        //            else
        //            {
        //                strCondition += " AND UPPER(JCSIT.JOBCARD_REF_NO) LIKE '%" + JOB_CARD_REF_NO.ToUpper().Replace("'", "''") + "%'";
        //            }
        //        }
        //        else
        //        {
        //            strCondition += " AND UPPER(JCSIT.JOBCARD_REF_NO) LIKE '%" + JOB_CARD_REF_NO.ToUpper().Replace("'", "''") + "%'";
        //        }
        //    }
        //    if (DEMURRAGE_REF_NO.Trim().Length > 0)
        //    {
        //        if (SearchType.ToString().Trim().Length > 0)
        //        {
        //            if (SearchType == "S")
        //            {
        //                strCondition += " AND UPPER(DCT.DEMURRAGE_REF_NO) LIKE '" + DEMURRAGE_REF_NO.ToUpper().Replace("'", "''") + "%'";
        //            }
        //            else
        //            {
        //                strCondition += " AND UPPER(DCT.DEMURRAGE_REF_NO) LIKE '%" + DEMURRAGE_REF_NO.ToUpper().Replace("'", "''") + "%'";
        //            }
        //        }
        //        else
        //        {
        //            strCondition += " AND UPPER(DCT.DEMURRAGE_REF_NO) LIKE '%" + DEMURRAGE_REF_NO.ToUpper().Replace("'", "''") + "%'";
        //        }
        //    }

        //    if (DEPOT_ID.Trim().Length > 0)
        //    {
        //        if (SearchType.ToString().Trim().Length > 0)
        //        {
        //            if (SearchType == "S")
        //            {
        //                strCondition += " AND UPPER(DMT.VENDOR_ID) LIKE '" + DEPOT_ID.ToUpper().Replace("'", "''") + "%'";
        //            }
        //            else
        //            {
        //                strCondition += " AND UPPER(DMT.VENDOR_ID) LIKE '%" + DEPOT_ID.ToUpper().Replace("'", "''") + "%'";
        //            }
        //        }
        //        else
        //        {
        //            strCondition += " AND UPPER(DMT.VENDOR_ID) LIKE '%" + DEPOT_ID.ToUpper().Replace("'", "''") + "%'";
        //        }
        //    }
        //    strCondition += "  AND VST.VENDOR_MST_FK=DMT.VENDOR_MST_PK AND VST.VENDOR_TYPE_FK=VT.VENDOR_TYPE_PK AND DMT.VENDOR_MST_PK=VCD.VENDOR_MST_FK ";
        //    strCondition += "   And vt.vendor_type_id= 'WAREHOUSE' And  VCD.ADM_LOCATION_MST_FK = " + lngUsrLocFk;
        //    // ------------------------------------------
        //    if (flag == 0)
        //    {
        //        strCondition += " AND 1=2";
        //    }
        //    strCondition += " AND UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk + "";
        //    strCondition += " AND DCT.CREATED_BY_FK = UMT.USER_MST_PK ";
        //    strSQL = "SELECT Count(*) from DEMURRAGE_CALC_TBL DCT,USER_MST_TBL UMT,VENDOR_MST_TBL DMT,JOB_CARD_TRN JCSIT,vendor_contact_dtls VCD,vendor_type_mst_tbl vt,vendor_services_trn VST where DMT.VENDOR_MST_PK=DCT.DEPOT_MST_FK AND JCSIT.JOB_CARD_TRN_PK=DCT.JOB_CARD_AIR_IMP_FK ";
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

        //    strSQL = " SELECT * FROM (";
        //    strSQL += "SELECT ROWNUM SR_NO,q.* FROM ";
        //    strSQL += "(SELECT  ";
        //    strSQL += " DCT.JOB_CARD_AIR_IMP_FK, ";
        //    strSQL += " JCSIT.JOBCARD_REF_NO,";
        //    strSQL += " DCT.DEMURRAGE_CALC_TBL_PK,";
        //    strSQL += " DCT.DEMURRAGE_REF_NO,";
        //    strSQL += " TO_CHAR(DCT.DEMURRAGE_DATE, '" + dateFormat + "'), ";
        //    strSQL += " DCT.DEPOT_MST_FK, ";
        //    strSQL += " DMT.VENDOR_ID, ";
        //    strSQL += " DCT.CURRENCY_MST_FK, ";
        //    strSQL += " CUR.CURRENCY_ID, ";
        //    strSQL += " NET_DEMURRAGE_AMT";
        //    strSQL += " FROM JOB_CARD_TRN JCSIT,vendor_contact_dtls VCD,vendor_type_mst_tbl vt,vendor_services_trn VST, ";
        //    strSQL += " VENDOR_MST_TBL DMT,USER_MST_TBL UMT, ";
        //    strSQL += " DEMURRAGE_CALC_TBL DCT, ";
        //    strSQL += " CURRENCY_TYPE_MST_TBL CUR ";
        //    strSQL += " WHERE JCSIT.JOB_CARD_TRN_PK=DCT.JOB_CARD_AIR_IMP_FK ";
        //    strSQL += " AND DMT.VENDOR_MST_PK=DCT.DEPOT_MST_FK ";
        //    strSQL += " AND DCT.CURRENCY_MST_FK=CUR.CURRENCY_MST_PK ";
        //    strSQL += strCondition;
        //    strSQL += "order by DEMURRAGE_DATE " + SortType + ",";
        //    strSQL += SortCol + SortType + ") q  ) WHERE SR_NO  Between " + start + " and " + last;
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

        //#endregion "Fetch Demurrage List "

        //#endregion " Demurrage Calculation Listing Screen "

        //#region " Demurrage Entry Screen "

        ////
        ///// <summary>
        ///// This function is called while viewing records in grid for selected job card.
        ///// </summary>
        ///// <param name="Job_Card_Pk">Primary Key of Job card import table, sent ByValue</param>
        ///// <example>Grid_Details(-1)</example>
        ///// <exception cref="sqlExp">Catch SQL Exception</exception>
        ///// <remarks>This function retrun datatable</remarks>
        //public DataTable Grid_Details(long Job_Card_Pk)
        //{
        //    try
        //    {
        //        objWF.MyCommand.Parameters.Clear();

        //        var _with1 = objWF.MyCommand.Parameters;
        //        _with1.Add("JOB_CARD_PK_IN", Job_Card_Pk).Direction = ParameterDirection.Input;
        //        _with1.Add("DEM_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
        //        return objWF.GetDataTable("DEMURRAGE_CALCULATION_PKG", "GRID_DETAILS");
        //    }
        //    catch (OracleException Oraexp)
        //    {
        //        throw Oraexp;
        //        //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public string Calculate_Demurrage(string JobContPKList, string DepotList, string Container_Ty, string GLD, string Coll_DT, string Free_Days)
        //{
        //    try
        //    {
        //        objWF.OpenConnection();
        //        objWF.MyCommand.Connection = objWF.MyConnection;
        //        objWF.MyCommand.CommandType = CommandType.StoredProcedure;

        //        objWF.MyCommand.CommandText = objWF.MyUserName + ".DEMURRAGE_LCL";
        //        var _with2 = objWF.MyCommand.Parameters;
        //        _with2.Clear();
        //        _with2.Add("JOB_CARD_AIR_IMP_PK_LIST1", JobContPKList.TrimEnd(Convert.ToChar(","))).Direction = ParameterDirection.Input;
        //        _with2.Add("DEPOT_PK_LIST1", DepotList.TrimEnd(Convert.ToChar(","))).Direction = ParameterDirection.Input;
        //        _with2.Add("VOL_WT_LIST1", Container_Ty.TrimEnd(Convert.ToChar(","))).Direction = ParameterDirection.Input;
        //        _with2.Add("GL_DT_LIST1", GLD.TrimEnd(Convert.ToChar(","))).Direction = ParameterDirection.Input;
        //        _with2.Add("COLL_DT_LIST1", Coll_DT.TrimEnd(Convert.ToChar(","))).Direction = ParameterDirection.Input;
        //        _with2.Add("FREE_DAYS_LIST1", Free_Days.TrimEnd(Convert.ToChar(","))).Direction = ParameterDirection.Input;
        //        _with2.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000).Direction = ParameterDirection.ReturnValue;

        //        objWF.MyCommand.ExecuteScalar();
        //        return objWF.MyCommand.Parameters["RETURN_VALUE"].Value.ToString();
        //    }
        //    catch (OracleException sqlEx)
        //    {
        //        throw sqlEx;
        //    }
        //    catch (Exception Ex)
        //    {
        //        throw Ex;
        //    }
        //    finally
        //    {
        //        objWF.MyCommand.Connection.Close();
        //    }
        //}

        //public DataSet FetchHDR(long nDetPK)
        //{
        //    System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
        //    DataSet DS = null;
        //    try
        //    {
        //        strBuilder.Append(" SELECT ");
        //        strBuilder.Append(" DEM.JOB_CARD_AIR_IMP_FK, ");
        //        strBuilder.Append(" JOB.JOBCARD_REF_NO, ");
        //        strBuilder.Append(" DEM.DEMURRAGE_REF_NO, ");
        //        strBuilder.Append(" DEM.DEMURRAGE_DATE, ");
        //        strBuilder.Append(" DEM.GROSS_DEMURRAGE_AMT, ");
        //        strBuilder.Append(" DEM.WAIVER_TYPE, ");
        //        strBuilder.Append(" DEM.WAIVER_PERCENTAGE, ");
        //        strBuilder.Append(" DEM.WAIVER_AMT, ");
        //        strBuilder.Append(" DEM.NET_DEMURRAGE_AMT, ");
        //        strBuilder.Append(" DEM.APPROVED_BY, ");
        //        strBuilder.Append(" EMP.EMPLOYEE_NAME, ");
        //        strBuilder.Append(" DEM.REFERENCE_NO, ");
        //        strBuilder.Append(" DEM.CURRENCY_MST_FK, ");
        //        strBuilder.Append(" CMT.CURRENCY_ID, ");
        //        strBuilder.Append(" DEM.DEPOT_MST_FK, ");
        //        strBuilder.Append(" DEM.HANDLING_CHARGES, ");
        //        strBuilder.Append(" VMT.VENDOR_ID ");
        //        strBuilder.Append(" FROM ");
        //        strBuilder.Append(" DEMURRAGE_CALC_TBL DEM, ");
        //        strBuilder.Append(" JOB_CARD_TRN JOB, ");
        //        strBuilder.Append(" CURRENCY_TYPE_MST_TBL CMT, ");
        //        strBuilder.Append(" VENDOR_MST_TBL VMT, ");
        //        strBuilder.Append(" EMPLOYEE_MST_TBL EMP ");
        //        strBuilder.Append(" WHERE ");
        //        strBuilder.Append(" DEM.CURRENCY_MST_FK=CMT.CURRENCY_MST_PK (+) ");
        //        strBuilder.Append(" AND DEM.APPROVED_BY=EMP.EMPLOYEE_MST_PK (+) ");
        //        strBuilder.Append(" AND DEM.DEPOT_MST_FK=VMT.VENDOR_MST_PK (+) ");
        //        strBuilder.Append(" AND DEM.JOB_CARD_AIR_IMP_FK=JOB.JOB_CARD_TRN_PK ");
        //        strBuilder.Append(" AND DEM.DEMURRAGE_CALC_TBL_PK=" + Convert.ToString(nDetPK));
        //        objWF.OpenConnection();
        //        DS = (new WorkFlow()).GetDataSet(strBuilder.ToString());
        //        return DS;
        //    }
        //    catch (OracleException Oraexp)
        //    {
        //        throw Oraexp;
        //        //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //    finally
        //    {
        //        //objWF.MyConnection.Close()
        //    }
        //}

        //public DataSet FetchTRN(long nDetPK)
        //{
        //    string strSQL = null;
        //    DataSet DS = null;

        //    try
        //    {
        //        strSQL = "SELECT ";
        //        strSQL += "TRN.PALETTE_SIZE,";
        //        strSQL += "TRN.PACK_TYPE_MST_FK,";
        //        strSQL += "PMT.PACK_TYPE_ID,";
        //        strSQL += "TRN.NO_OF_PACKAGES,";
        //        strSQL += "TRN.GROSS_WEIGHT,";
        //        strSQL += "TRN.VOLUME_IN_CBM,";
        //        strSQL += "TO_CHAR(TRN.GEN_LAND_DATE, DATEFORMAT) GLD,";
        //        strSQL += "TO_CHAR(TRN.DEMURRAGE_CALC_DATE, DATEFORMAT) DEM_CAL_DT,";
        //        strSQL += "TO_CHAR(TRN.COLLECTING_DATE_TIME, DATEFORMAT) COLLECTING_DT,";
        //        strSQL += "TRN.FREE_DAYS,";
        //        strSQL += "TRN.DEMURRAGE_AMT,";
        //        strSQL += "'1' SEL, ";
        //        strSQL += "0 JOB_CARD_AIR_IMP_CONT_PK ";
        //        strSQL += "FROM";
        //        strSQL += "DEMURRAGE_CALC_TRN TRN,";
        //        strSQL += "PACK_TYPE_MST_TBL PMT";
        //        strSQL += "WHERE";
        //        strSQL += "TRN.PACK_TYPE_MST_FK=PMT.PACK_TYPE_MST_PK (+)";
        //        strSQL += "AND TRN.DEMURRAGE_CALC_TBL_FK=" + Convert.ToString(nDetPK);
        //        strSQL += "ORDER BY TRN.PACK_TYPE_MST_FK,PMT.PACK_TYPE_ID,TRN.GEN_LAND_DATE";

        //        objWF.OpenConnection();
        //        DS = (new WorkFlow()).GetDataSet(strSQL);
        //        return DS;
        //    }
        //    catch (OracleException Oraexp)
        //    {
        //        throw Oraexp;
        //        //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //    finally
        //    {
        //        //objWF.MyConnection.Close()
        //    }
        //}

        //public DataSet FetchDemurrageDetails(long nDetPK, long nJobCardPK)
        //{
        //    string strSQL = null;
        //    WorkFlow objWK = new WorkFlow();
        //    DataSet DS = null;

        //    try
        //    {
        //        strSQL = "SELECT ";
        //        strSQL += "TRN.PALETTE_SIZE,";
        //        strSQL += "TRN.PACK_TYPE_MST_FK,";
        //        strSQL += "PMT.PACK_TYPE_ID,";
        //        strSQL += "TRN.NO_OF_PACKAGES,";
        //        strSQL += "TRN.GROSS_WEIGHT,";
        //        strSQL += "TRN.VOLUME_IN_CBM,";
        //        strSQL += "TO_CHAR(TRN.GEN_LAND_DATE, DATEFORMAT) GLD,";
        //        strSQL += "TO_CHAR(TRN.DEMURRAGE_CALC_DATE, DATEFORMAT) DEM_CAL_DT,";
        //        strSQL += "TO_CHAR(TRN.COLLECTING_DATE_TIME, DATEFORMAT) COLLECTING_DT,";
        //        strSQL += "TRN.FREE_DAYS,";
        //        strSQL += "TRN.DEMURRAGE_AMT,";
        //        strSQL += "'true' AS SEL, ";
        //        strSQL += "0 JOB_CARD_AIR_IMP_CONT_PK ";
        //        strSQL += "FROM";
        //        strSQL += "DEMURRAGE_CALC_TRN TRN,";
        //        strSQL += "DEMURRAGE_CALC_TBL HDR,";
        //        strSQL += "PACK_TYPE_MST_TBL PMT";
        //        strSQL += "WHERE";
        //        strSQL += "TRN.DEMURRAGE_CALC_TBL_FK=HDR.DEMURRAGE_CALC_TBL_PK AND";
        //        strSQL += "TRN.PACK_TYPE_MST_FK=PMT.PACK_TYPE_MST_PK (+)";
        //        //strSQL &= vbCrLf & "AND TRN.DEMURRAGE_CALC_TBL_FK<>" & CStr(nDetPK)
        //        strSQL += "AND TRN.DEMURRAGE_CALC_TBL_FK = " + Convert.ToString(nDetPK);
        //        strSQL += "AND HDR.JOB_CARD_AIR_IMP_FK=" + Convert.ToString(nJobCardPK);
        //        strSQL += "ORDER BY TRN.PACK_TYPE_MST_FK, PMT.PACK_TYPE_ID,TRN.GEN_LAND_DATE";

        //        DS = objWK.GetDataSet(strSQL);
        //        return DS;
        //    }
        //    catch (OracleException Oraexp)
        //    {
        //        throw Oraexp;
        //        //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //    finally
        //    {
        //        //objWF.MyConnection.Close()
        //    }
        //}

        //public DataSet FetchSlabDetails(long nJobContPK)
        //{
        //    System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
        //    WorkFlow objWK = new WorkFlow();
        //    DataSet DS = null;
        //    try
        //    {
        //        strBuilder.Append("SELECT * FROM DEMURRAGE_AIR_CALC_REPORT WHERE job_card_air_imp_count_pk=" + Convert.ToString(nJobContPK) + " ");
        //        strBuilder.Append("ORDER BY FROM_DAY");
        //        DS = objWK.GetDataSet(strBuilder.ToString());
        //        return DS;
        //    }
        //    catch (OracleException Oraexp)
        //    {
        //        throw Oraexp;
        //        //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //#endregion " Demurrage Entry Screen "

        //#region "Save"

        ////This function is called to save the Demurrage Calculation details
        //public ArrayList Save(DataSet hdrDS, DataSet trnDS, long nConfigPK, long nDetPK, object DetRefNo, long nLocationId, long nEmpId, long nUserId, System.DateTime arrivaldate)
        //{
        //    WorkFlow objWK = new WorkFlow();

        //    OracleTransaction TRAN = null;
        //    Int32 RecAfct = default(Int32);
        //    long nJobCardPK = 0;
        //    long nDetCalcPK = 0;
        //    OracleCommand insCommand = new OracleCommand();
        //    OracleCommand updCommand = new OracleCommand();
        //    string DetRef = null;
        //    string InvRefNo = null;

        //    objWK.OpenConnection();
        //    TRAN = objWK.MyConnection.BeginTransaction();
        //    objWK.MyCommand.Connection = objWK.MyConnection;
        //    objWK.MyCommand.Transaction = TRAN;
        //    try
        //    {
        //        if (string.IsNullOrEmpty(DetRefNo.Text))
        //        {
        //            DetRef = GenerateProtocolKey("DEMURRAGE CALCULATION", nLocationId, nEmpId, DateTime.Today, "", "", "", nUserId, objWK);
        //            if (DetRef == "Protocol Not Defined.")
        //            {
        //                arrMessage.Add("Protocol Not Defined.");
        //                return arrMessage;
        //            }
        //        }
        //        else
        //        {
        //            DetRef = DetRefNo.Text;
        //        }
        //        objWK.MyCommand.Parameters.Clear();
        //        InvRefNo = GenerateProtocolKey("CONSOLIDATED INVOICE", nLocationId, nEmpId, DateTime.Now, "", "", "", nUserId, objWK);
        //        if (Convert.ToString(InvRefNo) == "Protocol Not Defined.")
        //        {
        //            InvRefNo = "";
        //        }
        //        //Snigdharani - 11/02/2009 - Unique Invoice Reference Int32(VEK)
        //        string uniqueRefNr = null;
        //        if (string.IsNullOrEmpty(uniqueRefNr))
        //        {
        //            //    uniqueRefNr = GetVEKInvoiceRef(10000000, 99999999)
        //            //    While IsUniqueRefNr(uniqueRefNr, objWK.MyCommand) > 0
        //            //        uniqueRefNr = GetVEKInvoiceRef(10000000, 99999999)
        //            //    End While
        //            //End If
        //            //MOdified by manivannan
        //            System.DateTime dt = default(System.DateTime);
        //            dt = System.DateTime.Now;
        //            string st = null;
        //            st = dt.Day + dt.Month + dt.Year + dt.Hour + dt.Minute + dt.Second + dt.Millisecond;
        //            uniqueRefNr = GetVEKInvoiceRef(0, 0, st);
        //        }
        //        //End Snigdharani
        //        objWK.MyCommand.Parameters.Clear();

        //        nJobCardPK = hdrDS.Tables[0].Rows[0]["JOB_CARD_AIR_IMP_FK"];
        //        hdrDS.Tables[0].Rows[0]["DEMURRAGE_REF_NO"] = DetRef;
        //        var _with3 = insCommand;
        //        _with3.Connection = objWK.MyConnection;
        //        _with3.CommandType = CommandType.StoredProcedure;
        //        _with3.CommandText = objWK.MyUserName + ".DEMURRAGE_CALC_TBL_PKG.DEMURRAGE_CALC_TBL_INS";

        //        //JOB_CARD_AIR_IMP_FK_IN,
        //        _with3.Parameters.Add("JOB_CARD_AIR_IMP_FK_IN", OracleDbType.Int32, 10, "JOB_CARD_AIR_IMP_FK").Direction = ParameterDirection.Input;
        //        _with3.Parameters["JOB_CARD_AIR_IMP_FK_IN"].SourceVersion = DataRowVersion.Current;
        //        //DEMURRAGE_REF_NO_IN
        //        _with3.Parameters.Add("DEMURRAGE_REF_NO_IN", OracleDbType.Varchar2, 50, "DEMURRAGE_REF_NO").Direction = ParameterDirection.Input;
        //        _with3.Parameters["DEMURRAGE_REF_NO_IN"].SourceVersion = DataRowVersion.Current;
        //        //INVOICE_REF_NO_IN
        //        _with3.Parameters.Add("INVOICE_REF_NO_IN", InvRefNo).Direction = ParameterDirection.Input;
        //        //.Parameters.Add("INVOICE_REF_NO_IN", OracleClient.OracleDbType.Varchar2, 50, InvRefNo).Direction = ParameterDirection.Input
        //        //.Parameters["INVOICE_REF_NO_IN"].SourceVersion = DataRowVersion.Current
        //        //DEMURRAGE_DATE_IN,
        //        _with3.Parameters.Add("DEMURRAGE_DATE_IN", OracleDbType.Varchar2, 12, "DEMURRAGE_DATE").Direction = ParameterDirection.Input;
        //        _with3.Parameters["DEMURRAGE_DATE_IN"].SourceVersion = DataRowVersion.Current;
        //        //DEPOT_MST_FK_IN,
        //        _with3.Parameters.Add("DEPOT_MST_FK_IN", OracleDbType.Int32, 10, "VENDOR_MST_FK").Direction = ParameterDirection.Input;
        //        _with3.Parameters["DEPOT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
        //        //GROSS_DEMURRAGE_AMT_IN,
        //        _with3.Parameters.Add("GROSS_DEMURRAGE_AMT_IN", OracleDbType.Int32, 10, "GROSS_DEMURRAGE_AMT").Direction = ParameterDirection.Input;
        //        _with3.Parameters["GROSS_DEMURRAGE_AMT_IN"].SourceVersion = DataRowVersion.Current;
        //        //WAIVER_TYPE_IN,
        //        _with3.Parameters.Add("WAIVER_TYPE_IN", OracleDbType.Int32, 1, "WAIVER_TYPE").Direction = ParameterDirection.Input;
        //        _with3.Parameters["WAIVER_TYPE_IN"].SourceVersion = DataRowVersion.Current;
        //        //WAIVER_PERCENTAGE_IN,
        //        _with3.Parameters.Add("WAIVER_PERCENTAGE_IN", OracleDbType.Int32, 5, "WAIVER_PERCENTAGE").Direction = ParameterDirection.Input;
        //        _with3.Parameters["WAIVER_PERCENTAGE_IN"].SourceVersion = DataRowVersion.Current;
        //        //WAIVER_AMT_IN,
        //        _with3.Parameters.Add("WAIVER_AMT_IN", OracleDbType.Int32, 10, "WAIVER_AMT").Direction = ParameterDirection.Input;
        //        _with3.Parameters["WAIVER_AMT_IN"].SourceVersion = DataRowVersion.Current;
        //        //NET_DEMURRAGE_AMT_IN,
        //        _with3.Parameters.Add("NET_DEMURRAGE_AMT_IN", OracleDbType.Int32, 10, "NET_DEMURRAGE_AMT").Direction = ParameterDirection.Input;
        //        _with3.Parameters["NET_DEMURRAGE_AMT_IN"].SourceVersion = DataRowVersion.Current;
        //        //APPROVED_BY_IN,
        //        _with3.Parameters.Add("APPROVED_BY_IN", OracleDbType.Int32, 10, "APPROVED_BY").Direction = ParameterDirection.Input;
        //        _with3.Parameters["APPROVED_BY_IN"].SourceVersion = DataRowVersion.Current;
        //        //REFERENCE_NO_IN,
        //        _with3.Parameters.Add("REFERENCE_NO_IN", OracleDbType.Varchar2, 30, "REFERENCE_NO").Direction = ParameterDirection.Input;
        //        _with3.Parameters["REFERENCE_NO_IN"].SourceVersion = DataRowVersion.Current;
        //        //CURRENCY_MST_FK_IN,
        //        _with3.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
        //        _with3.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
        //        //HANDLING CHARGES
        //        _with3.Parameters.Add("HANDLING_CHARGES_IN", OracleDbType.Int32, 10, "HANDLING_CHARGES").Direction = ParameterDirection.Input;
        //        _with3.Parameters["HANDLING_CHARGES_IN"].SourceVersion = DataRowVersion.Current;
        //        //CREATED_BY_FK_IN,
        //        _with3.Parameters.Add("CREATED_BY_FK_IN", OracleDbType.Int32, 10, "CREATED_BY_FK").Direction = ParameterDirection.Input;
        //        _with3.Parameters["CREATED_BY_FK_IN"].SourceVersion = DataRowVersion.Current;
        //        //CONFIG_PK_IN
        //        _with3.Parameters.Add("CONFIG_PK_IN", nConfigPK).Direction = ParameterDirection.Input;
        //        //INV_UNIQUE_REF_NR_IN
        //        _with3.Parameters.Add("INV_UNIQUE_REF_NR_IN", uniqueRefNr).Direction = ParameterDirection.Input;
        //        //RETURN_VALUE
        //        _with3.Parameters.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
        //        _with3.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

        //        var _with4 = updCommand;
        //        _with4.Connection = objWK.MyConnection;
        //        _with4.CommandType = CommandType.StoredProcedure;
        //        _with4.CommandText = objWK.MyUserName + ".DEMURRAGE_CALC_TBL_PKG.DEMURRAGE_CALC_TBL_UPD";

        //        //DEMURRAGE_CALC_TBL_PK_IN
        //        _with4.Parameters.Add("DEMURRAGE_CALC_TBL_PK_IN", OracleDbType.Int32, 10, "DEMURRAGE_CALC_TBL_PK").Direction = ParameterDirection.Input;
        //        _with4.Parameters["DEMURRAGE_CALC_TBL_PK_IN"].SourceVersion = DataRowVersion.Current;
        //        //JOB_CARD_AIR_IMP_FK_IN,
        //        _with4.Parameters.Add("JOB_CARD_AIR_IMP_FK_IN", OracleDbType.Int32, 10, "JOB_CARD_AIR_IMP_FK").Direction = ParameterDirection.Input;
        //        _with4.Parameters["JOB_CARD_AIR_IMP_FK_IN"].SourceVersion = DataRowVersion.Current;
        //        //DEMURRAGE_REF_NO_IN
        //        _with4.Parameters.Add("DEMURRAGE_REF_NO_IN", OracleDbType.Varchar2, 50, "DEMURRAGE_REF_NO").Direction = ParameterDirection.Input;
        //        _with4.Parameters["DEMURRAGE_REF_NO_IN"].SourceVersion = DataRowVersion.Current;
        //        //DEMURRAGE_DATE_IN,
        //        _with4.Parameters.Add("DEMURRAGE_DATE_IN", OracleDbType.Varchar2, 12, "DEMURRAGE_DATE").Direction = ParameterDirection.Input;
        //        _with4.Parameters["DEMURRAGE_DATE_IN"].SourceVersion = DataRowVersion.Current;
        //        //DEPOT_MST_FK_IN,
        //        _with4.Parameters.Add("DEPOT_MST_FK_IN", OracleDbType.Int32, 10, "VENDOR_MST_FK").Direction = ParameterDirection.Input;
        //        _with4.Parameters["DEPOT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
        //        //GROSS_DEMURRAGE_AMT_IN,
        //        _with4.Parameters.Add("GROSS_DEMURRAGE_AMT_IN", OracleDbType.Int32, 10, "GROSS_DEMURRAGE_AMT").Direction = ParameterDirection.Input;
        //        _with4.Parameters["GROSS_DEMURRAGE_AMT_IN"].SourceVersion = DataRowVersion.Current;
        //        //WAIVER_TYPE_IN,
        //        _with4.Parameters.Add("WAIVER_TYPE_IN", OracleDbType.Int32, 1, "WAIVER_TYPE").Direction = ParameterDirection.Input;
        //        _with4.Parameters["WAIVER_TYPE_IN"].SourceVersion = DataRowVersion.Current;
        //        //WAIVER_PERCENTAGE_IN,
        //        _with4.Parameters.Add("WAIVER_PERCENTAGE_IN", OracleDbType.Int32, 5, "WAIVER_PERCENTAGE").Direction = ParameterDirection.Input;
        //        _with4.Parameters["WAIVER_PERCENTAGE_IN"].SourceVersion = DataRowVersion.Current;
        //        //WAIVER_AMT_IN,
        //        _with4.Parameters.Add("WAIVER_AMT_IN", OracleDbType.Int32, 10, "WAIVER_AMT").Direction = ParameterDirection.Input;
        //        _with4.Parameters["WAIVER_AMT_IN"].SourceVersion = DataRowVersion.Current;
        //        //NET_DEMURRAGE_AMT_IN,
        //        _with4.Parameters.Add("NET_DEMURRAGE_AMT_IN", OracleDbType.Int32, 10, "NET_DEMURRAGE_AMT").Direction = ParameterDirection.Input;
        //        _with4.Parameters["NET_DEMURRAGE_AMT_IN"].SourceVersion = DataRowVersion.Current;
        //        //APPROVED_BY_IN,
        //        _with4.Parameters.Add("APPROVED_BY_IN", OracleDbType.Int32, 10, "APPROVED_BY").Direction = ParameterDirection.Input;
        //        _with4.Parameters["APPROVED_BY_IN"].SourceVersion = DataRowVersion.Current;
        //        //REFERENCE_NO_IN,
        //        _with4.Parameters.Add("REFERENCE_NO_IN", OracleDbType.Varchar2, 30, "REFERENCE_NO").Direction = ParameterDirection.Input;
        //        _with4.Parameters["REFERENCE_NO_IN"].SourceVersion = DataRowVersion.Current;
        //        //CURRENCY_MST_FK_IN,
        //        _with4.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
        //        _with4.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
        //        //LAST_MODIFIED_BY_FK_IN,
        //        _with4.Parameters.Add("LAST_MODIFIED_BY_FK_IN", OracleDbType.Int32, 10, "LAST_MODIFIED_BY_FK").Direction = ParameterDirection.Input;
        //        _with4.Parameters["LAST_MODIFIED_BY_FK_IN"].SourceVersion = DataRowVersion.Current;
        //        //VERSION_NO_IN
        //        _with4.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 10, "VERSION_NO").Direction = ParameterDirection.Input;
        //        _with4.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
        //        //HANDLING CHARGES
        //        _with4.Parameters.Add("HANDLING_CHARGES_IN", OracleDbType.Int32, 10, "HANDLING_CHARGES").Direction = ParameterDirection.Input;
        //        _with4.Parameters["HANDLING_CHARGES_IN"].SourceVersion = DataRowVersion.Current;
        //        //CONFIG_PK_IN
        //        _with4.Parameters.Add("CONFIG_PK_IN", nConfigPK).Direction = ParameterDirection.Input;
        //        _with4.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;
        //        _with4.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

        //        objWK.MyDataAdapter.RowUpdated += new OracleRowUpdatedEventHandler(OnRowUpdated);
        //        arrMessage.Clear();

        //        var _with5 = objWK.MyDataAdapter;

        //        _with5.InsertCommand = insCommand;
        //        _with5.InsertCommand.Transaction = TRAN;
        //        _with5.UpdateCommand = updCommand;
        //        _with5.UpdateCommand.Transaction = TRAN;
        //        RecAfct = _with5.Update(hdrDS.Tables[0]);
        //        long ConsInvPK = 0;
        //        if (RecAfct > 0)
        //        {
        //            //nDetCalcPK = insCommand.Parameters["RETURN_VALUE"].Value
        //            strReturnPK = Convert.ToString(insCommand.Parameters["RETURN_VALUE"].Value).Trim();
        //            string[] arr = null;
        //            arr = strReturnPK.Split(Convert.ToChar("~"));
        //            nDetCalcPK = arr[0];
        //            ConsInvPK = arr[1];
        //            updatearrivaldate(arrivaldate, nDetCalcPK, nJobCardPK);
        //            SaveTrn(trnDS, TRAN, nDetCalcPK, nJobCardPK);
        //            SaveJCFDAndInvoiceDet(trnDS, TRAN, nDetCalcPK, nJobCardPK, ConsInvPK, hdrDS.Tables[0].Rows[0]["CURRENCY_MST_FK"]);
        //        }
        //        if (arrMessage.Count > 0)
        //        {
        //            if (string.IsNullOrEmpty(DetRefNo.Text))
        //            {
        //                RollbackProtocolKey("DEMURRAGE CALCULATION", HttpContext.Current.Session["LOGED_IN_LOC_FK"], HttpContext.Current.Session["EMP_PK"], DetRef, System.DateTime.Now);
        //                RollbackProtocolKey("CONSOLIDATED INVOICE", HttpContext.Current.Session["LOGED_IN_LOC_FK"], HttpContext.Current.Session["EMP_PK"], InvRefNo, System.DateTime.Now);
        //            }
        //            TRAN.Rollback();
        //            return arrMessage;
        //        }
        //        else
        //        {
        //            TRAN.Commit();
        //            arrMessage.Add("All Data Saved Successfully");
        //            DetRefNo.Text = DetRef;
        //            return arrMessage;
        //        }
        //    }
        //    catch (OracleException oraexp)
        //    {
        //        if ((TRAN != null))
        //        {
        //            if (TRAN.Connection.State == ConnectionState.Open)
        //            {
        //                if (string.IsNullOrEmpty(DetRefNo.Text))
        //                {
        //                    RollbackProtocolKey("DEMURRAGE CALCULATION", HttpContext.Current.Session["LOGED_IN_LOC_FK"], HttpContext.Current.Session["EMP_PK"], DetRef, System.DateTime.Now);
        //                    RollbackProtocolKey("CONSOLIDATED INVOICE", HttpContext.Current.Session["LOGED_IN_LOC_FK"], HttpContext.Current.Session["EMP_PK"], InvRefNo, System.DateTime.Now);
        //                }
        //                TRAN.Rollback();
        //                TRAN = null;
        //            }
        //        }
        //        throw oraexp;
        //    }
        //    catch (Exception ex)
        //    {
        //        if ((TRAN != null))
        //        {
        //            if (TRAN.Connection.State == ConnectionState.Open)
        //            {
        //                if (string.IsNullOrEmpty(DetRefNo.Text))
        //                {
        //                    RollbackProtocolKey("DEMURRAGE CALCULATION", HttpContext.Current.Session["LOGED_IN_LOC_FK"], HttpContext.Current.Session["EMP_PK"], DetRef, System.DateTime.Now);
        //                    RollbackProtocolKey("CONSOLIDATED INVOICE", HttpContext.Current.Session["LOGED_IN_LOC_FK"], HttpContext.Current.Session["EMP_PK"], InvRefNo, System.DateTime.Now);
        //                }
        //                TRAN.Rollback();
        //                TRAN = null;
        //            }
        //        }
        //        throw ex;
        //    }
        //}

        //private void SaveJCFDAndInvoiceDet(DataSet trnDS, OracleTransaction TRAN, long nDemCalcPK, long nJobCardPK, long ConsInvPK, long CurrPk)
        //{
        //    Int32 RecAfct = default(Int32);
        //    OracleCommand insCommand = new OracleCommand();
        //    OracleCommand updCommand = new OracleCommand();
        //    OracleCommand delCommand = new OracleCommand();
        //    WorkFlow objWK = new WorkFlow();
        //    int i = 0;
        //    try
        //    {
        //        RecAfct = 0;
        //        objWK.MyConnection = TRAN.Connection;
        //        for (i = 0; i <= trnDS.Tables[0].Rows.Count - 1; i++)
        //        {
        //            var _with6 = insCommand;
        //            _with6.Transaction = TRAN;
        //            _with6.Connection = objWK.MyConnection;
        //            _with6.CommandType = CommandType.StoredProcedure;
        //            _with6.CommandText = objWK.MyUserName + ".DEMURRAGE_CALC_TBL_PKG.DEMURRAGE_TO_JC_INS";
        //            _with6.Parameters.Clear();
        //            _with6.Parameters.Add("CONSOL_INVOICE_PK_IN", ConsInvPK).Direction = ParameterDirection.Input;
        //            _with6.Parameters.Add("JOB_CARD_SEA_IMP_FK_IN", nJobCardPK).Direction = ParameterDirection.Input;
        //            _with6.Parameters.Add("DEMURRAGE_CALC_TBL_FK_IN", nDemCalcPK).Direction = ParameterDirection.Input;
        //            _with6.Parameters.Add("DEMURRAGE_AMT_IN", trnDS.Tables[0].Rows[i]["DEMURRAGE_AMT"]).Direction = ParameterDirection.Input;
        //            _with6.Parameters.Add("CURRENCY_MST_FK_IN", CurrPk).Direction = ParameterDirection.Input;
        //            _with6.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
        //            RecAfct = _with6.ExecuteNonQuery();
        //        }
        //        if (RecAfct == 0)
        //        {
        //            arrMessage.Add("Save not successful");
        //        }
        //        else
        //        {
        //            int OthChgPk = 0;
        //            OthChgPk = insCommand.Parameters["RETURN_VALUE"].Value;
        //        }
        //    }
        //    catch (OracleException ex1)
        //    {
        //        arrMessage.Add(ex1.Message);
        //    }
        //    catch (Exception ex)
        //    {
        //        arrMessage.Add(ex.Message);
        //    }
        //}

        //public object updatearrivaldate(System.DateTime arrdt, long nDetCalcPK, long nJobCardPK)
        //{
        //    try
        //    {
        //        WorkFlow objWK = new WorkFlow();
        //        string strSQL = null;
        //        strSQL = "UPDATE JOB_CARD_TRN JSI set ";
        //        strSQL = strSQL + "jsi.arrival_date = '" + Strings.Trim(arrdt) + "'";
        //        strSQL = strSQL + " WHERE ";
        //        strSQL = strSQL + " jsi.JOB_CARD_TRN_PK = " + nJobCardPK;
        //        //strSQL = strSQL & vbCrLf & " det.detention_calc_tbl_pk = " & nDetCalcPK
        //        objWK.ExecuteCommands(strSQL);
        //    }
        //    catch (OracleException Oraexp)
        //    {
        //        throw Oraexp;
        //        //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        ////This function is called to save the Demurrage Calculation TRN details
        //private void SaveTrn(DataSet trnDS, OracleTransaction TRAN, long nDetCalcPK, long nJobCardPK)
        //{
        //    Int32 RecAfct = default(Int32);
        //    OracleCommand insCommand = new OracleCommand();
        //    OracleCommand updCommand = new OracleCommand();
        //    OracleCommand delCommand = new OracleCommand();
        //    WorkFlow objWK = new WorkFlow();
        //    try
        //    {
        //        objWK.MyConnection = TRAN.Connection;
        //        var _with7 = insCommand;
        //        _with7.Connection = objWK.MyConnection;
        //        _with7.CommandType = CommandType.StoredProcedure;
        //        _with7.CommandText = objWK.MyUserName + ".DEMURRAGE_CALC_TBL_PKG.DEMURRAGE_CALC_TRN_INS";
        //        // DEMURRAGE_CALC_TBL_FK_IN
        //        _with7.Parameters.Add("DEMURRAGE_CALC_TBL_FK_IN", nDetCalcPK).Direction = ParameterDirection.Input;
        //        _with7.Parameters["DEMURRAGE_CALC_TBL_FK_IN"].SourceVersion = DataRowVersion.Current;
        //        // CALCULATION_BASIS_IN,
        //        _with7.Parameters.Add("CALCULATION_BASIS_IN", OracleDbType.Int32, 1, "CALCULATION_BASIS").Direction = ParameterDirection.Input;
        //        _with7.Parameters["CALCULATION_BASIS_IN"].SourceVersion = DataRowVersion.Current;
        //        // CALCULATION_BASIS_IN,
        //        _with7.Parameters.Add("PALETTE_SIZE_IN", OracleDbType.Varchar2, 20, "PALETTE_SIZE").Direction = ParameterDirection.Input;
        //        _with7.Parameters["PALETTE_SIZE_IN"].SourceVersion = DataRowVersion.Current;
        //        // NO_OF_PACKAGES_IN,
        //        _with7.Parameters.Add("NO_OF_PACKAGES_IN", OracleDbType.Int32, 6, "NO_OF_PACKAGES").Direction = ParameterDirection.Input;
        //        _with7.Parameters["NO_OF_PACKAGES_IN"].SourceVersion = DataRowVersion.Current;
        //        // GROSS_WEIGHT_IN,
        //        _with7.Parameters.Add("GROSS_WEIGHT_IN", OracleDbType.Int32, 10, "GROSS_WEIGHT").Direction = ParameterDirection.Input;
        //        _with7.Parameters["GROSS_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;
        //        // VOLUME_IN_CBM_IN,
        //        _with7.Parameters.Add("VOLUME_IN_CBM_IN", OracleDbType.Int32, 10, "VOLUME_IN_CBM").Direction = ParameterDirection.Input;
        //        _with7.Parameters["VOLUME_IN_CBM_IN"].SourceVersion = DataRowVersion.Current;
        //        //GEN_LAND_DATE_IN,
        //        _with7.Parameters.Add("GEN_LAND_DATE_IN", OracleDbType.Date, 10, "GEN_LAND_DATE").Direction = ParameterDirection.Input;
        //        _with7.Parameters["GEN_LAND_DATE_IN"].SourceVersion = DataRowVersion.Current;
        //        //COLLECTING_DATE_TIME_IN,
        //        _with7.Parameters.Add("COLLECTING_DATE_TIME_IN", OracleDbType.Date, 10, "COLLECTING_DATE_TIME").Direction = ParameterDirection.Input;
        //        _with7.Parameters["COLLECTING_DATE_TIME_IN"].SourceVersion = DataRowVersion.Current;
        //        //FREE_DAYS_IN,
        //        _with7.Parameters.Add("FREE_DAYS_IN", OracleDbType.Int32, 2, "FREE_DAYS").Direction = ParameterDirection.Input;
        //        _with7.Parameters["FREE_DAYS_IN"].SourceVersion = DataRowVersion.Current;
        //        //DEMURRAGE_AMT_IN,
        //        _with7.Parameters.Add("DEMURRAGE_AMT_IN", OracleDbType.Int32, 10, "DEMURRAGE_AMT").Direction = ParameterDirection.Input;
        //        _with7.Parameters["DEMURRAGE_AMT_IN"].SourceVersion = DataRowVersion.Current;
        //        //PACK_TYPE_MST_FK_IN,
        //        _with7.Parameters.Add("PACK_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "PACK_TYPE_MST_FK").Direction = ParameterDirection.Input;
        //        _with7.Parameters["PACK_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
        //        //DEMURRAGE_CALC_DATE_IN,
        //        _with7.Parameters.Add("DEMURRAGE_CALC_DATE_IN", OracleDbType.Date, 10, "DEMURRAGE_CALC_DATE").Direction = ParameterDirection.Input;
        //        _with7.Parameters["DEMURRAGE_CALC_DATE_IN"].SourceVersion = DataRowVersion.Current;

        //        _with7.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "DEMURRAGE_CALC_TRN_PK").Direction = ParameterDirection.Output;
        //        _with7.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

        //        objWK.MyDataAdapter.RowUpdated += new OracleRowUpdatedEventHandler(OnRowUpdated);
        //        var _with8 = objWK.MyDataAdapter;
        //        _with8.InsertCommand = insCommand;
        //        _with8.InsertCommand.Transaction = TRAN;
        //        RecAfct = _with8.Update(trnDS.Tables[0]);
        //        if (RecAfct == 0)
        //        {
        //            arrMessage.Add("Save not successful");
        //        }
        //        var _with9 = insCommand;

        //        _with9.Parameters.Clear();
        //        _with9.CommandText = objWK.MyUserName + ".DEMURRAGE_CALC_TBL_PKG.DEMURRAGE_CALC_SLAB_DTLS_INS";
        //        // DETENTION_CALC_TRN_FK_IN
        //        _with9.Parameters.Add("DEMURRAGE_CALC_TRN_FK_IN", OracleDbType.Int32, 10, "DEMURRAGE_CALC_TRN_FK").Direction = ParameterDirection.Input;
        //        _with9.Parameters["DEMURRAGE_CALC_TRN_FK_IN"].SourceVersion = DataRowVersion.Current;
        //        // FROM_DAY_IN,
        //        _with9.Parameters.Add("FROM_DAY_IN", OracleDbType.Int32, 3, "FROM_DAY").Direction = ParameterDirection.Input;
        //        _with9.Parameters["FROM_DAY_IN"].SourceVersion = DataRowVersion.Current;
        //        // TO_DAY_IN,
        //        _with9.Parameters.Add("TO_DAY_IN", OracleDbType.Int32, 3, "TO_DAY").Direction = ParameterDirection.Input;
        //        _with9.Parameters["TO_DAY_IN"].SourceVersion = DataRowVersion.Current;
        //        // NO_OF_DAYS_IN,
        //        _with9.Parameters.Add("NO_OF_DAYS_IN", OracleDbType.Int32, 3, "NO_OF_DAYS").Direction = ParameterDirection.Input;
        //        _with9.Parameters["NO_OF_DAYS_IN"].SourceVersion = DataRowVersion.Current;
        //        // TARIFF_RATE_IN,
        //        _with9.Parameters.Add("TARIFF_RATE_IN", OracleDbType.Int32, 10, "TARIFF_RATE").Direction = ParameterDirection.Input;
        //        _with9.Parameters["TARIFF_RATE_IN"].SourceVersion = DataRowVersion.Current;
        //        // DETENTION_AMT_IN,
        //        _with9.Parameters.Add("DEMURRAGE_AMOUNT_IN", OracleDbType.Int32, 10, "DEMURRAGE_AMT").Direction = ParameterDirection.Input;
        //        _with9.Parameters["DEMURRAGE_AMOUNT_IN"].SourceVersion = DataRowVersion.Current;

        //        _with9.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "DEMURRAGE_CALC_SLAB_DTLS_PK").Direction = ParameterDirection.Output;
        //        _with9.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

        //        objWK.MyDataAdapter.RowUpdated += new OracleRowUpdatedEventHandler(OnRowUpdated);
        //        var _with10 = objWK.MyDataAdapter;
        //        _with10.InsertCommand = insCommand;
        //        _with10.InsertCommand.Transaction = TRAN;
        //        RecAfct = _with10.Update(trnDS.Tables[1]);
        //        if (RecAfct == 0)
        //        {
        //            arrMessage.Add("Save not successful");
        //        }
        //    }
        //    catch (OracleException Oraexp)
        //    {
        //        throw Oraexp;
        //        //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
        //    }
        //    catch (Exception ex)
        //    {
        //        arrMessage.Add(ex.Message);
        //    }
        //}

        ////This function is called to generate the invoice . no.
        //public string GenerateKey(string strName, long nLocPK, long nEmpPK, System.DateTime dtDate, long nUserID)
        //{
        //    return GenerateProtocolKey(strName, nLocPK, nEmpPK, dtDate, "", "", "", nUserID);
        //}

        //#endregion "Save"

        //#region "Fetch for Print"

        //public DataSet funFetchHdrPrint(string strJobPk, long lngUserPk)
        //{
        //    try
        //    {
        //        System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
        //        WorkFlow objWF = new WorkFlow();

        //        strBuilder.Append("SELECT ");
        //        strBuilder.Append("CMT.CUSTOMER_NAME AS CONSIGNEE_NAME, ");
        //        strBuilder.Append("DECODE(CCD.ADM_SALUTATION,'0',' ','1','Mr ','2','Mrs ') || CCD.ADM_CONTACT_PERSON AS CONTACT_PERSON, ");
        //        strBuilder.Append("CCD.ADM_FAX_NO AS FAX_NO, ");
        //        strBuilder.Append("UMT.USER_NAME AS CUSTOMER_NAME, ");
        //        strBuilder.Append("JCHDR.JOB_CARD_TRN_PK AS JOB_CARD_AIR_EXP_FK ");
        //        strBuilder.Append("FROM ");
        //        strBuilder.Append("JOB_CARD_TRN JCHDR, ");
        //        strBuilder.Append("CUSTOMER_MST_TBL CMT, ");
        //        strBuilder.Append("CUSTOMER_CONTACT_DTLS CCD, ");
        //        strBuilder.Append("USER_MST_TBL UMT ");
        //        strBuilder.Append("WHERE ");
        //        strBuilder.Append("JCHDR.CONSIGNEE_CUST_MST_FK = CMT.CUSTOMER_MST_PK ");
        //        strBuilder.Append("AND CCD.CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK(+) ");
        //        strBuilder.Append("AND JCHDR.JOB_CARD_TRN_PK= " + strJobPk);
        //        strBuilder.Append("AND UMT.USER_MST_PK=" + lngUserPk);

        //        return objWF.GetDataSet(strBuilder.ToString());
        //    }
        //    catch (OracleException Oraexp)
        //    {
        //        throw Oraexp;
        //        //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public object funFetchTrnPrint(string strJobPk, string strDemRefNo = "")
        //{
        //    System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
        //    string strCondition = null;
        //    WorkFlow objWF = new WorkFlow();

        //    if (!string.IsNullOrEmpty(strDemRefNo))
        //    {
        //        strCondition = " AND DCHDR.DEMURRAGE_REF_NO='" + strDemRefNo + "' ";
        //    }
        //    else
        //    {
        //        strCondition = " ";
        //    }
        //    strBuilder.Append("SELECT DISTINCT ");
        //    strBuilder.Append("DCHDR.DEMURRAGE_CALC_TBL_PK AS DEMURRAGE_CALC_TBL_PK, ");
        //    strBuilder.Append("DCHDR.JOB_CARD_AIR_IMP_FK AS JOB_CARD_AIR_IMP_FK, ");
        //    strBuilder.Append("DCTRN.DEMURRAGE_CALC_TRN_PK AS DEMURRAGE_CALC_TRN_PK, ");
        //    strBuilder.Append("VMT.VENDOR_ID VENDOR_ID, ");
        //    strBuilder.Append("TO_CHAR(DCTRN.DEMURRAGE_CALC_DATE,dateformat) AS DEMURRAGE_CALC_DATE, ");
        //    strBuilder.Append("DCTRN.PALETTE_SIZE AS PALETTE_SIZE, ");
        //    strBuilder.Append("PTMT.PACK_TYPE_ID AS PACK_TYPE_ID, ");
        //    strBuilder.Append("TO_CHAR(DCTRN.GEN_LAND_DATE,DATEFORMAT) AS GEN_LAND_DATE, ");
        //    strBuilder.Append("DCTRN.NO_OF_PACKAGES AS NO_OF_PACKAGES, ");
        //    strBuilder.Append("DCTRN.GROSS_WEIGHT AS GROSS_WEIGHT, ");
        //    strBuilder.Append("DCTRN.VOLUME_IN_CBM AS VOLUME_IN_CBM, ");
        //    strBuilder.Append("TO_CHAR(DCTRN.COLLECTING_DATE_TIME,dateformat) AS COLLECTING_DATE_TIME, ");
        //    strBuilder.Append("DECODE(DCTRN.CALCULATION_BASIS,'1','Weight','Volume') AS CALCULATION_BASIS, ");
        //    strBuilder.Append("DCTRN.FREE_DAYS AS FREE_DAYS, ");
        //    strBuilder.Append("DCSD.FROM_DAY AS FROM_DAY, ");
        //    strBuilder.Append("DCSD.TO_DAY AS TO_DAY, ");
        //    strBuilder.Append("DCSD.TARIFF_RATE AS TARIFF_RATE, ");
        //    strBuilder.Append("DCSD.NO_OF_DAYS AS NO_OF_DAYS, ");
        //    strBuilder.Append("DCSD.DEMURRAGE_AMOUNT AS DEMURRAGE_AMOUNT, ");
        //    strBuilder.Append("CTMT.CURRENCY_ID AS CURRENCY_ID, ");
        //    strBuilder.Append("(DCTRN.DEMURRAGE_CALC_DATE - DCTRN.COLLECTING_DATE_TIME) - DCTRN.FREE_DAYS AS DEMURRAGE_DAYS ");
        //    strBuilder.Append("FROM ");
        //    strBuilder.Append("DEMURRAGE_CALC_TBL DCHDR, ");
        //    strBuilder.Append("DEMURRAGE_CALC_TRN DCTRN, ");
        //    strBuilder.Append("DEMURRAGE_CALC_SLAB_DTLS DCSD, ");
        //    strBuilder.Append("VENDOR_MST_TBL VMT, ");
        //    strBuilder.Append("PACK_TYPE_MST_TBL PTMT, ");
        //    strBuilder.Append("DEMURRAGE_SLAB_MAIN_TBL DSMT, ");
        //    strBuilder.Append("CURRENCY_TYPE_MST_TBL CTMT ");
        //    strBuilder.Append("WHERE ");
        //    strBuilder.Append("DCTRN.DEMURRAGE_CALC_TBL_FK = DCHDR.DEMURRAGE_CALC_TBL_PK ");
        //    strBuilder.Append("AND DCSD.DEMURRAGE_CALC_TRN_FK=DCTRN.DEMURRAGE_CALC_TRN_PK(+) ");
        //    strBuilder.Append("AND DCHDR.DEPOT_MST_FK=VMT.VENDOR_MST_PK(+) ");
        //    strBuilder.Append("AND DCTRN.PACK_TYPE_MST_FK=PTMT.PACK_TYPE_MST_PK(+) ");
        //    strBuilder.Append("AND VMT.VENDOR_MST_PK=DSMT.DEPOT_MST_FK ");
        //    strBuilder.Append("AND DSMT.CURRENCY_MST_FK=CTMT.CURRENCY_MST_PK ");
        //    strBuilder.Append("AND DCHDR.CURRENCY_MST_FK=CTMT.CURRENCY_MST_PK ");
        //    strBuilder.Append("AND DCHDR.JOB_CARD_AIR_IMP_FK= " + strJobPk + strCondition + " ");
        //    strBuilder.Append("ORDER BY DCTRN.DEMURRAGE_CALC_TRN_PK, DCSD.FROM_DAY ");
        //    try
        //    {
        //        return objWF.GetDataSet(strBuilder.ToString());
        //    }
        //    catch (OracleException Oraexp)
        //    {
        //        throw Oraexp;
        //        //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //#endregion "Fetch for Print"

        //#region "Check for Concurrency"

        //public DataTable funResetDataSet(string strJobPk)
        //{
        //    System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
        //    WorkFlow objWF = new WorkFlow();
        //    DataTable dsTemp = null;
        //    strBuilder.Append("SELECT ");
        //    strBuilder.Append("JOB_CONT.PALETTE_SIZE, ");
        //    strBuilder.Append("JCDI.PACK_TYPE_FK PACK_TYPE_MST_FK, ");
        //    strBuilder.Append("NVL(SUM(JOB_CONT.PACK_COUNT), 0) - ");
        //    strBuilder.Append("(SELECT NVL(SUM(TRN.NO_OF_PACKAGES), 0) ");
        //    strBuilder.Append("FROM DEMURRAGE_CALC_TRN TRN, DEMURRAGE_CALC_TBL HDR ");
        //    strBuilder.Append("WHERE(TRN.DEMURRAGE_CALC_TBL_FK = HDR.DEMURRAGE_CALC_TBL_PK) ");
        //    strBuilder.Append("AND HDR.JOB_CARD_AIR_IMP_FK = " + strJobPk + " ");
        //    strBuilder.Append("AND TRN.PACK_TYPE_MST_FK = JCDI.PACK_TYPE_FK ");
        //    strBuilder.Append("AND TRN.PALETTE_SIZE = JOB_CONT.PALETTE_SIZE) AS NO_OF_PACKAGES ");
        //    strBuilder.Append("FROM JOB_TRN_CONT JOB_CONT, JOB_TRN_COMMODITY JCDI ");
        //    strBuilder.Append("WHERE JCDI.PACK_TYPE_FK IS NOT NULL AND ");
        //    strBuilder.Append("JOB_CONT.JOB_CARD_TRN_FK = " + strJobPk + " ");
        //    strBuilder.Append(" AND JOB_CONT.JOB_TRN_CONT_PK = JCDI.JOB_TRN_CONT_FK ");
        //    strBuilder.Append("GROUP BY ");
        //    strBuilder.Append("JOB_CONT.PALETTE_SIZE, ");
        //    strBuilder.Append("JCDI.PACK_TYPE_FK ");
        //    strBuilder.Append("ORDER BY JCDI.PACK_TYPE_FK ");
        //    try
        //    {
        //        return objWF.GetDataTable(strBuilder.ToString());
        //    }
        //    catch (OracleException Oraexp)
        //    {
        //        throw Oraexp;
        //        //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //#endregion "Check for Concurrency"

        //#region "Check for Warehouse Validity"

        //public string FetchValidityWareHouse(string strCond)
        //{
        //    WorkFlow objWF = new WorkFlow();
        //    OracleCommand SCM = new OracleCommand();
        //    string strReturn = null;
        //    Array arr = null;
        //    string strDEPOT_PK_IN = "";
        //    string strGL_DT_LIST1_IN = "";
        //    string strCOLL_DT_LIST1_IN = "";
        //    string strFREE_DAYS_LIST1_IN = "";
        //    string strBUSINESSTYPE_IN = "";
        //    string strCARGO_TYPE_IN = "";

        //    string strReq = null;
        //    arr = strCond.Split("~");
        //    strDEPOT_PK_IN = arr(0);
        //    strGL_DT_LIST1_IN = arr(1);
        //    strCOLL_DT_LIST1_IN = arr(2);
        //    strFREE_DAYS_LIST1_IN = arr(3);
        //    strBUSINESSTYPE_IN = arr(4);
        //    if (arr.Length > 5)
        //        strCARGO_TYPE_IN = arr(5);
        //    else
        //        strCARGO_TYPE_IN = 0;

        //    try
        //    {
        //        objWF.OpenConnection();
        //        SCM.Connection = objWF.MyConnection;
        //        SCM.CommandType = CommandType.StoredProcedure;
        //        SCM.CommandText = objWF.MyUserName + ".DEMDET_WAREHOUSE_VALIDITY";
        //        var _with11 = SCM.Parameters;
        //        _with11.Add("DEPOT_PK_IN", strDEPOT_PK_IN).Direction = ParameterDirection.Input;
        //        _with11.Add("GL_DT_LIST1_IN", strGL_DT_LIST1_IN.TrimEnd(Convert.ToChar(","))).Direction = ParameterDirection.Input;
        //        _with11.Add("COLL_DT_LIST1_IN", strCOLL_DT_LIST1_IN.TrimEnd(Convert.ToChar(","))).Direction = ParameterDirection.Input;
        //        _with11.Add("FREE_DAYS_LIST1_IN", strFREE_DAYS_LIST1_IN.TrimEnd(Convert.ToChar(","))).Direction = ParameterDirection.Input;
        //        _with11.Add("BUSINESSTYPE_IN", strBUSINESSTYPE_IN).Direction = ParameterDirection.Input;
        //        _with11.Add("CARGO_TYPE_IN", strCARGO_TYPE_IN).Direction = ParameterDirection.Input;
        //        _with11.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.ReturnValue;
        //        SCM.ExecuteScalar();
        //        strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
        //        return strReturn;
        //    }
        //    catch (OracleException Oraexp)
        //    {
        //        throw Oraexp;
        //        //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //        SCM.Connection.Close();
        //    }
        //}

        //#endregion "Check for Warehouse Validity"

        //#region "For Handling Charges"

        //public int HandlingChargesFCL(string CalculationBasis = "1", string Weight = "0.0", string Volume = "0.0")
        //{
        //    try
        //    {
        //        StringBuilder strQuery = new StringBuilder();
        //        WorkFlow objWF = new WorkFlow();
        //        DataSet dsHandlingCharges = new DataSet();
        //        double HandlingCharges = 0.0;
        //        string demurragepk = null;

        //        demurragepk = Fetch_Demurrage_Pk();
        //        if ((demurragepk != null))
        //        {
        //            strQuery.Append("  SELECT SUM(DECODE(" + CalculationBasis + ",");
        //            strQuery.Append("                1,");
        //            strQuery.Append("                TSD.LCL_RATE_PER_100KG * (" + Weight + " / 100),");
        //            strQuery.Append("                2,");
        //            strQuery.Append("                TSD.LCL_RATE_PER_CBM * " + Volume + ")) OTH_COST");
        //            strQuery.Append("    FROM TARIFF_AIR_DEPOT_OTH_CHG TSD, DEMURRAGE_SLAB_MAIN_TBL DSM");
        //            strQuery.Append("   WHERE TSD.DEMURAGE_SLAB_MAIN_FK = DSM.DEMURAGE_SLAB_MAIN_PK");
        //            strQuery.Append("      AND DSM.DEMURAGE_SLAB_MAIN_PK  = " + demurragepk);
        //            strQuery.Append("");

        //            dsHandlingCharges = objWF.GetDataSet(strQuery.ToString());

        //            if (dsHandlingCharges.Tables[0].Rows.Count > 0)
        //            {
        //                HandlingCharges = getDefault(dsHandlingCharges.Tables[0].Rows[0][0], 0.0);
        //            }
        //        }
        //        return HandlingCharges;
        //    }
        //    catch (OracleException Oraexp)
        //    {
        //        throw Oraexp;
        //        //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //#endregion "For Handling Charges"

        //#region "Fetch Demurrage Pk "

        //public string Fetch_Demurrage_Pk()
        //{
        //    try
        //    {
        //        StringBuilder strQuery = new StringBuilder();
        //        WorkFlow objWF = new WorkFlow();
        //        DataSet dsDetentionCharges = new DataSet();
        //        string DemurragePk = null;
        //        strQuery.Append("select distinct t.demurrage_slab_main_fk from demurrage_air_calc_report t ");
        //        dsDetentionCharges = objWF.GetDataSet(strQuery.ToString());
        //        if (dsDetentionCharges.Tables[0].Rows.Count > 0)
        //        {
        //            DemurragePk = dsDetentionCharges.Tables[0].Rows[0]["demurrage_slab_main_fk"];
        //        }
        //        return DemurragePk;
        //    }
        //    catch (OracleException Oraexp)
        //    {
        //        throw Oraexp;
        //        //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //#endregion "Fetch Demurrage Pk "

        //#region "Save Cost"

        //public ArrayList SaveCost(long JobCardPK, DataSet dsCostDetails)
        //{
        //    WorkFlow objWK = new WorkFlow();
        //    objWK.OpenConnection();
        //    OracleTransaction TRAN = null;
        //    TRAN = objWK.MyConnection.BeginTransaction();
        //    Int32 RecAfct = default(Int32);
        //    int i = 0;
        //    OracleCommand insCostDetails = new OracleCommand();
        //    //'Added By Koteshwari
        //    OracleCommand updCostDetails = new OracleCommand();
        //    try
        //    {
        //        for (i = 0; i <= dsCostDetails.Tables[0].Rows.Count - 1; i++)
        //        {
        //            if (string.IsNullOrEmpty(dsCostDetails.Tables[0].Rows[i]["JOB_TRN_COST_PK"]))
        //            {
        //                var _with12 = objWK.MyCommand;
        //                _with12.Transaction = TRAN;
        //                _with12.Connection = objWK.MyConnection;
        //                _with12.CommandType = CommandType.StoredProcedure;
        //                _with12.CommandText = objWK.MyUserName + ".JOB_CARD_TRN_PKG.JOB_TRN_COST_INS";
        //                _with12.Parameters.Clear();
        //                var _with13 = _with12.Parameters;
        //                _with13.Add("JOB_CARD_TRN_FK_IN", JobCardPK).Direction = ParameterDirection.Input;
        //                _with13.Add("VENDOR_MST_FK_IN", dsCostDetails.Tables[0].Rows[i]["VENDOR_MST_PK"]).Direction = ParameterDirection.Input;
        //                _with13.Add("COST_ELEMENT_FK_IN", dsCostDetails.Tables[0].Rows[i]["COST_ELEMENT_MST_PK"]).Direction = ParameterDirection.Input;
        //                _with13.Add("LOCATION_FK_IN", dsCostDetails.Tables[0].Rows[i]["LOCATION_MST_FK"]).Direction = ParameterDirection.Input;
        //                _with13.Add("VENDOR_KEY_IN", dsCostDetails.Tables[0].Rows[i]["VENDOR_KEY"]).Direction = ParameterDirection.Input;
        //                _with13.Add("PTMT_TYPE_IN", Convert.ToInt32(dsCostDetails.Tables[0].Rows[0]["PTMT_TYPE"])).Direction = ParameterDirection.Input;
        //                _with13.Add("CURRENCY_MST_FK_IN", dsCostDetails.Tables[0].Rows[i]["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
        //                _with13.Add("ESTIMATED_COST_IN", dsCostDetails.Tables[0].Rows[i]["ESTIMATED_COST"]).Direction = ParameterDirection.Input;
        //                _with13.Add("TOTAL_COST_IN", dsCostDetails.Tables[0].Rows[i]["TOTAL_COST"]).Direction = ParameterDirection.Input;
        //                _with13.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
        //                RecAfct = _with12.ExecuteNonQuery();
        //            }
        //            else
        //            {
        //                var _with14 = objWK.MyCommand;
        //                _with14.Transaction = TRAN;
        //                _with14.Connection = objWK.MyConnection;
        //                _with14.CommandType = CommandType.StoredProcedure;
        //                _with14.CommandText = objWK.MyUserName + ".JOB_CARD_TRN_PKG.JOB_TRN_COST_UPD";
        //                _with14.Parameters.Clear();
        //                var _with15 = _with14.Parameters;

        //                _with15.Add("JOB_TRN_COST_PK_IN", dsCostDetails.Tables[0].Rows[i]["JOB_TRN_AIR_IMP_COST_PK"]).Direction = ParameterDirection.Input;
        //                _with15.Add("JOB_CARD_TRN_FK_IN", JobCardPK).Direction = ParameterDirection.Input;
        //                _with15.Add("VENDOR_MST_FK_IN", dsCostDetails.Tables[0].Rows[i]["VENDOR_MST_PK"]).Direction = ParameterDirection.Input;
        //                _with15.Add("COST_ELEMENT_FK_IN", dsCostDetails.Tables[0].Rows[i]["COST_ELEMENT_MST_PK"]).Direction = ParameterDirection.Input;
        //                _with15.Add("LOCATION_FK_IN", dsCostDetails.Tables[0].Rows[i]["LOCATION_MST_FK"]).Direction = ParameterDirection.Input;
        //                _with15.Add("VENDOR_KEY_IN", dsCostDetails.Tables[0].Rows[i]["VENDOR_KEY"]).Direction = ParameterDirection.Input;
        //                _with15.Add("PTMT_TYPE_IN", Convert.ToInt32(dsCostDetails.Tables[0].Rows[i]["PTMT_TYPE"])).Direction = ParameterDirection.Input;
        //                _with15.Add("CURRENCY_MST_FK_IN", dsCostDetails.Tables[0].Rows[i]["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
        //                _with15.Add("ESTIMATED_COST_IN", dsCostDetails.Tables[0].Rows[i]["ESTIMATED_COST"]).Direction = ParameterDirection.Input;
        //                _with15.Add("TOTAL_COST_IN", dsCostDetails.Tables[0].Rows[i]["TOTAL_COST"]).Direction = ParameterDirection.Input;
        //                _with15.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
        //                RecAfct = _with14.ExecuteNonQuery();
        //            }
        //        }

        //        if (RecAfct > 0)
        //        {
        //            TRAN.Commit();
        //            arrMessage.Add("All Data Saved Successfully");
        //            return arrMessage;
        //        }
        //        else
        //        {
        //            TRAN.Rollback();
        //            return arrMessage;
        //        }
        //    }
        //    catch (OracleException Oraexp)
        //    {
        //        throw Oraexp;
        //        //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
        //    }
        //    catch (Exception ex)
        //    {
        //        TRAN.Rollback();
        //        throw ex;
        //    }
        //    finally
        //    {
        //        objWF.MyConnection.Close();
        //    }
        //}

        //#endregion "Save Cost"
    }
}