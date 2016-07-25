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
    public class clsAirLineTariffUpload : CommonFeatures
    {
        //#region "Variables"

        //private DataSet dsMain;
        //private string airlinePk;
        //private string aooPk;
        //private string todt;
        //private string fromdt;
        //private string Slab;
        //private int rw;
        //private int Clm;
        //private string cur;
        //private long emp;
        //private long locid;
        //private bool isLogi;
        //private System.DateTime tarrifdat;
        //private bool match = false;
        //private string AODID = "";

        //#endregion "Variables"

        //private int RowAod;

        //#region "property"

        //public DataSet Mainds
        //{
        //    get { return dsMain; }
        //    set { dsMain = value; }
        //}

        //private System.DateTime tariffdate
        //{
        //    get { return tarrifdat; }
        //    set { tarrifdat = value; }
        //}

        //public bool islog
        //{
        //    get { return isLogi; }
        //    set { isLogi = value; }
        //}

        //public long EmpId
        //{
        //    get { return emp; }
        //    set { emp = value; }
        //}

        //public long Loc
        //{
        //    get { return locid; }
        //    set { locid = value; }
        //}

        //public string AirPk
        //{
        //    get { return airlinePk; }
        //    set { airlinePk = value; }
        //}

        //public string AodPk
        //{
        //    get { return aooPk; }
        //    set { aooPk = value; }
        //}

        //public string FromDate
        //{
        //    get { return fromdt; }
        //    set { fromdt = value; }
        //}

        //public string ToDate
        //{
        //    get { return todt; }
        //    set { todt = value; }
        //}

        //public string Currenc
        //{
        //    get { return cur; }
        //    set { cur = value; }
        //}

        //public int ROWS
        //{
        //    //for max row
        //    get
        //    {
        //        rw = 1;
        //        while ((xlsdata.GetValue(rw, 1) != null))
        //        {
        //            rw = rw + 1;
        //        }
        //        return rw;
        //    }
        //}

        //public int Columns
        //{
        //    //for maximum columns
        //    get
        //    {
        //        Clm = 1;
        //        while ((xlsdata.GetValue(4, Clm) != null))
        //        {
        //            Clm = Clm + 1;
        //        }
        //        return Clm;
        //    }
        //}

        //public string Slabs
        //{
        //    //for getting all the slabs
        //    get
        //    {
        //        int i = 0;
        //        int C = Columns[xlsdata];
        //        if (C > 2)
        //        {
        //            for (i = 3; i <= C - 2; i++)
        //            {
        //                if (string.IsNullOrEmpty(Slab))
        //                {
        //                    Slab = xlsdata(4, i);
        //                }
        //                else
        //                {
        //                    Slab += "," + xlsdata(4, i);
        //                }
        //            }
        //        }
        //        return Slab;
        //    }
        //}

        //public string Surcharge
        //{
        //    //for getting all the surcharge element
        //    get
        //    {
        //        string functionReturnValue = null;
        //        int i = 0;
        //        int C = Columns[xlsdata];
        //        if (C > 2)
        //        {
        //            for (i = C - 2; i <= C; i++)
        //            {
        //                if (string.IsNullOrEmpty(functionReturnValue))
        //                {
        //                    functionReturnValue = xlsdata(4, i);
        //                }
        //                else
        //                {
        //                    functionReturnValue += "," + xlsdata(4, i);
        //                }
        //            }
        //        }
        //        return functionReturnValue;
        //        return functionReturnValue;
        //    }
        //}

        //#endregion "property"

        //#region " Reading Excel"

        //#endregion " Reading Excel"

        //#region "Dataset"

        //public object MakingDataset(Array xlsData)
        //{
        //    try
        //    {
        //        dsMain = new DataSet();
        //        dsMain.Tables.Add("tblMaster");
        //        dsMain.Tables["tblMaster"].Columns.Add("TARIFF_MAIN_AIR_PK");
        //        dsMain.Tables["tblMaster"].Columns.Add("AIRLINE_MST_FK");
        //        dsMain.Tables["tblMaster"].Columns.Add("AGENT_MST_FK");
        //        dsMain.Tables["tblMaster"].Columns.Add("TARIFF_DATE");
        //        dsMain.Tables["tblMaster"].Columns.Add("VALID_FROM");
        //        dsMain.Tables["tblMaster"].Columns.Add("VALID_TO");
        //        dsMain.Tables["tblMaster"].Columns.Add("ACTIVE");
        //        dsMain.Tables["tblMaster"].Columns.Add("COMMODITY_GROUP_FK");
        //        dsMain.Tables["tblMaster"].Columns.Add("VERSION_NO");
        //        dsMain.Tables["tblMaster"].Columns.Add("POLPK");
        //        dsMain.Tables["tblMaster"].Columns.Add("PODPK");
        //        dsMain.Tables["tblMaster"].Columns.Add("TARIFF_TYPE");
        //        dsMain.Tables["tblMaster"].Columns.Add("RATE_REFERENCE_NO");
        //        dsMain.Tables["tblMaster"].Columns.Add("REMARKS");
        //        //Creating a new datatable with name tblTransaction for transaction table
        //        dsMain.Tables.Add("tblTransaction");
        //        dsMain.Tables["tbltransaction"].Columns.Add("TARIFF_TRN_AIR_PK");
        //        dsMain.Tables["tbltransaction"].Columns.Add("CONT_MAIN_AIR_PK");
        //        dsMain.Tables["tblTransaction"].Columns.Add("PORT_MST_POD_FK");
        //        dsMain.Tables["tblTransaction"].Columns.Add("PORT_MST_POL_FK");
        //        dsMain.Tables["tblTransaction"].Columns.Add("VALID_FROM");
        //        dsMain.Tables["tblTransaction"].Columns.Add("VALID_TO");
        //        //Format of string: FrtFk~CurrentRate~TariffRate$
        //        dsMain.Tables["tblTransaction"].Columns.Add("AIR_OTH_CHRG");
        //        //Format of string: SlabFk~CurrentRate~TariffRate$
        //        dsMain.Tables["tblTransaction"].Columns.Add("AIR_BRK_PNTS");
        //        //Format of string: FrtFk~ChargeBasis~CurrentRate~TariffRate$
        //        dsMain.Tables["tblTransaction"].Columns.Add("AIR_SURCHRG");
        //        //Format of string: FrtFk~MinAmt$
        //        dsMain.Tables["tblTransaction"].Columns.Add("AIR_FREIGHT");
        //        dsMain.Tables["tblTransaction"].Columns.Add("CURRENCY_MST_FK");
        //        //Filling the Header Table
        //        FillHDRTable(dsMain, xlsData);
        //        //Filling the Transaction Table
        //        FillTransactionTable(dsMain, xlsData);
        //        //for using saving data first put in dataset
        //        Mainds = dsMain;
        //        //for making log file
        //        CleanLog();
        //        InsertLog(dsMain);
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //private void FillHDRTable(DataSet dsMain, Array xlsData)
        //{
        //    try
        //    {
        //        DataRow drRow = null;
        //        //xlsData.GetValue(row,columns)
        //        //Creating the new row object of the table Master
        //        drRow = dsMain.Tables["tblMaster"].NewRow();
        //        //Contract Generation DateTime formatted into MM-DD-YYYY
        //        drRow["TARIFF_DATE"] = Strings.Format("{0:" + dateFormat + "}", tariffdate);

        //        //drRow.Item("TARIFF_DATE") = xlsData.GetValue(1, 2)
        //        //for date from xl sheet
        //        //Master starting validity date formatted into MM-DD-YYYY.
        //        //Port wise validity date should be greater than or equal to this date
        //        drRow["VALID_FROM"] = Strings.Format("{0:" + dateFormat + "}", FromDate);
        //        //End validity date formatted into MM-DD-YYYY
        //        //Port wise validity date should be less than or equal to this date
        //        if (string.IsNullOrEmpty(ToDate) | (ToDate == null))
        //        {
        //            //If the validity is nothing then null
        //            drRow["VALID_TO"] = "";
        //        }
        //        else
        //        {
        //            drRow["VALID_TO"] = Strings.Format("{0:" + dateFormat + "}", ToDate);
        //        }
        //        //Active Check
        //        drRow["ACTIVE"] = 1;
        //        //CommodityFk
        //        drRow["COMMODITY_GROUP_FK"] = "";
        //        //Concurrency Check
        //        drRow["VERSION_NO"] = 0;
        //        drRow["TARIFF_MAIN_AIR_PK"] = "";
        //        drRow["TARIFF_TYPE"] = 1;
        //        //Airline Tariff
        //        drRow["AIRLINE_MST_FK"] = AirPk;
        //        drRow["AGENT_MST_FK"] = "";
        //        drRow["RATE_REFERENCE_NO"] = "";
        //        drRow["REMARKS"] = "";
        //        //Adding the row object to the table.
        //        drRow["POLPK"] = AodPk;
        //        dsMain.Tables["tblMaster"].Rows.Add(drRow);
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //private void FillTransactionTable(DataSet dsMain, Array xlsData)
        //{
        //    //data filling in transation table.
        //    try
        //    {
        //        string strBrpnt = null;
        //        string strSur = null;
        //        string strFrt = null;
        //        int rw = 0;
        //        int cl = 1;
        //        DataRow drRow = null;
        //        int Allclm = Columns[xlsData];
        //        //new row for transation table

        //        //after selection of aod
        //        if (match == true)
        //        {
        //            drRow = dsMain.Tables["tblTransaction"].NewRow();
        //            cl = 1;
        //            strFrt = "";
        //            strSur = "";
        //            strBrpnt = "";
        //            var _with1 = drRow;
        //            _with1.Item("TARIFF_TRN_AIR_PK") = "";
        //            _with1.Item("CONT_MAIN_AIR_PK") = "";
        //            _with1.Item("PORT_MST_POD_FK") = xlsData(RowAod, 1);
        //            _with1.Item("PORT_MST_POL_FK") = AodPk;
        //            _with1.Item("VALID_FROM") = FromDate;
        //            _with1.Item("VALID_TO") = getDefault(ToDate, "");
        //            _with1.Item("AIR_OTH_CHRG") = "";
        //            for (cl = 3; cl <= Allclm - 3; cl++)
        //            {
        //                strBrpnt += xlsData(4, cl) + "~~" + xlsData(RowAod, cl) + "$";
        //            }
        //            _with1.Item("AIR_BRK_PNTS") = strBrpnt;
        //            //slabs ~ tariff
        //            for (cl = Allclm - 2; cl <= Allclm - 1; cl++)
        //            {
        //                strSur += xlsData(4, cl) + "~~" + xlsData(RowAod, cl) + "$";
        //            }
        //            _with1.Item("AIR_SURCHRG") = strSur;
        //            //freight element ~ tariff
        //            for (cl = 2; cl <= 2; cl++)
        //            {
        //                strFrt += "AFC" + "~" + xlsData(RowAod, cl) + "$";
        //            }
        //            _with1.Item("AIR_FREIGHT") = strFrt;
        //            //freight element ~ tariff
        //            _with1.Item("CURRENCY_MST_FK") = Currenc;
        //            dsMain.Tables["tblTransaction"].Rows.Add(drRow);
        //            //without aod selection
        //        }
        //        else
        //        {
        //            for (rw = 5; rw <= ROWS[xlsData] - 1; rw++)
        //            {
        //                drRow = dsMain.Tables["tblTransaction"].NewRow();
        //                cl = 1;
        //                strFrt = "";
        //                strSur = "";
        //                strBrpnt = "";
        //                var _with2 = drRow;
        //                _with2.Item("TARIFF_TRN_AIR_PK") = "";
        //                _with2.Item("CONT_MAIN_AIR_PK") = "";
        //                _with2.Item("PORT_MST_POD_FK") = xlsData(rw, 1);
        //                _with2.Item("PORT_MST_POL_FK") = AodPk;
        //                _with2.Item("VALID_FROM") = FromDate;
        //                _with2.Item("VALID_TO") = getDefault(ToDate, "");
        //                _with2.Item("AIR_OTH_CHRG") = "";
        //                for (cl = 3; cl <= Allclm - 3; cl++)
        //                {
        //                    strBrpnt += xlsData(4, cl) + "~~" + xlsData(rw, cl) + "$";
        //                }
        //                _with2.Item("AIR_BRK_PNTS") = strBrpnt;
        //                //slabs ~ tariff
        //                for (cl = Allclm - 2; cl <= Allclm - 1; cl++)
        //                {
        //                    strSur += xlsData(4, cl) + "~~" + xlsData(rw, cl) + "$";
        //                }
        //                _with2.Item("AIR_SURCHRG") = strSur;
        //                //freight element ~ tariff
        //                for (cl = 2; cl <= 2; cl++)
        //                {
        //                    strFrt += "AFC" + "~" + xlsData(rw, cl) + "$";
        //                }
        //                _with2.Item("AIR_FREIGHT") = strFrt;
        //                //freight element ~ tariff
        //                _with2.Item("CURRENCY_MST_FK") = Currenc;
        //                dsMain.Tables["tblTransaction"].Rows.Add(drRow);
        //            }
        //        }
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

        //public DataSet DatasetForfetch(Array xlsData)
        //{
        //    try
        //    {
        //        DataSet dsGrid = new DataSet();
        //        DataRow drRow = null;
        //        int i = 0;
        //        dsGrid.Tables.Add("Tariff");
        //        Clm = Columns[xlsData];
        //        for (i = 1; i <= Clm - 1; i++)
        //        {
        //            var _with3 = dsGrid.Tables["Tariff"].Columns;
        //            if (i != 1)
        //            {
        //                _with3.Add(xlsData.GetValue(4, i), typeof(double));
        //            }
        //            else
        //            {
        //                _with3.Add(xlsData.GetValue(4, i));
        //            }
        //        }
        //        if (match == false)
        //        {
        //            for (rw = 5; rw <= ROWS[xlsData] - 1; rw++)
        //            {
        //                drRow = dsGrid.Tables["Tariff"].NewRow();
        //                for (i = 0; i <= Clm - 2; i++)
        //                {
        //                    var _with4 = drRow;
        //                    drRow[i] = xlsData.GetValue(rw, i + 1);
        //                }
        //                dsGrid.Tables["Tariff"].Rows.Add(drRow);
        //            }
        //        }
        //        else
        //        {
        //            //RowAod
        //            drRow = dsGrid.Tables["Tariff"].NewRow();
        //            for (i = 0; i <= Clm - 2; i++)
        //            {
        //                var _with5 = drRow;
        //                drRow[i] = xlsData.GetValue(RowAod, i + 1);
        //            }
        //            dsGrid.Tables["Tariff"].Rows.Add(drRow);
        //        }
        //        return dsGrid;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //#endregion "Dataset"

        //#region "Database And log file"

        //public object CleanLog()
        //{
        //    try
        //    {
        //        WorkFlow objWK = new WorkFlow();
        //        objWK.ExecuteCommands("DELETE FROM AIR_TARIFF_UPLOAD_LOG");
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

        //public object ClearTemp(int PK)
        //{
        //    try
        //    {
        //        WorkFlow objWK = new WorkFlow();
        //        if (islog)
        //        {
        //            objWK.ExecuteCommands("DELETE FROM TARIFF_MAIN_AIR_TBL WHERE TARIFF_MAIN_AIR_PK=" + PK);
        //        }
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

        //public object InsertLog(DataSet dsMain)
        //{
        //    WorkFlow objWK = new WorkFlow();
        //    Int16 exe = default(Int16);
        //    string TariffRefNo = null;
        //    Exception exp = new Exception("This file cannot be uploaded");
        //    int tariffPK = 0;
        //    cls_AirlineTariffEntry objAirtrf = new cls_AirlineTariffEntry(1, 2, false);
        //    OracleTransaction TRAN = null;
        //    try
        //    {
        //        objWK.OpenConnection();
        //        TRAN = objWK.MyConnection.BeginTransaction();
        //        objWK.MyCommand.Transaction = TRAN;
        //        if (islog)
        //        {
        //            TariffRefNo = "xyz123TEMP";
        //        }
        //        else
        //        {
        //            TariffRefNo = objAirtrf.GenerateTariffNo(Loc, EmpId, M_CREATED_BY_FK, objWK);
        //            if (TariffRefNo == "Protocol Not Defined.")
        //            {
        //                throw (new Exception(TariffRefNo));
        //            }
        //        }
        //        objWK.MyCommand.Parameters.Clear();
        //        var _with6 = objWK.MyCommand;
        //        _with6.CommandText = objWK.MyUserName + ".AIR_TARIFF_UPLOAD_PKG.TARIFF_MAIN_AIR_TBL_INS";
        //        _with6.Connection = objWK.MyConnection;
        //        _with6.CommandType = CommandType.StoredProcedure;
        //        var _with7 = objWK.MyCommand.Parameters;
        //        //AIRLINE_MST_FK_IN()
        //        _with7.Add("AIRLINE_MST_FK_IN", dsMain.Tables["tblMaster"].Rows[0]["AIRLINE_MST_FK"]);
        //        //TARIFF_REF_NO_IN()
        //        _with7.Add("TARIFF_REF_NO_IN", TariffRefNo);
        //        //TARIFF_DATE_IN()
        //        _with7.Add("TARIFF_DATE_IN", dsMain.Tables["tblMaster"].Rows[0]["TARIFF_DATE"]);
        //        //VALID_FROM_IN()
        //        _with7.Add("VALID_FROM_IN", Convert.ToString(Convert.ToDateTime(FromDate)));
        //        //VALID_TO_IN()

        //        if (string.IsNullOrEmpty(ToDate) | (ToDate == null))
        //        {
        //            _with7.Add("VALID_TO_IN", "");
        //        }
        //        else
        //        {
        //            _with7.Add("VALID_TO_IN", Convert.ToString(Convert.ToDateTime(ToDate)));
        //        }

        //        //ACTIVE_IN()
        //        _with7.Add("ACTIVE_IN", dsMain.Tables["tblMaster"].Rows[0]["ACTIVE"]);
        //        //COMMODITY_GROUP_FK_IN()
        //        _with7.Add("COMMODITY_GROUP_FK_IN", dsMain.Tables["tblMaster"].Rows[0]["COMMODITY_GROUP_FK"]);
        //        //CREATED_BY_FK_IN()
        //        _with7.Add("CREATED_BY_FK_IN", CREATED_BY);
        //        //TARIFF_TYPE_IN()
        //        _with7.Add("TARIFF_TYPE_IN", dsMain.Tables["tblMaster"].Rows[0]["TARIFF_TYPE"]);
        //        //RATE_REFERENCE_NO_IN()
        //        _with7.Add("RATE_REFERENCE_NO_IN", dsMain.Tables["tblMaster"].Rows[0]["RATE_REFERENCE_NO"]);
        //        //POLPK_IN()
        //        _with7.Add("POLPK_IN", dsMain.Tables["tblMaster"].Rows[0]["POLPK"]);
        //        //PODPK_IN()
        //        //.Add("PODPK_IN", dsMain.Tables("tblMaster").Rows(0).Item("PODPK"))
        //        //CONFIG_PK_IN()
        //        _with7.Add("CONFIG_PK_IN", ConfigurationPK);
        //        //AGENT_MST_FK_IN()
        //        _with7.Add("AGENT_MST_FK_IN", dsMain.Tables["tblMaster"].Rows[0]["AGENT_MST_FK"]);
        //        //REMARKS_IN()
        //        _with7.Add("REMARKS_IN", dsMain.Tables["tblMaster"].Rows[0]["REMARKS"]);
        //        //RETURN_VALUE()
        //        _with7.Add("RETURN_VALUE", OracleDbType.Int32, 10, tariffPK).Direction = ParameterDirection.Output;
        //        exe = _with6.ExecuteNonQuery();
        //        tariffPK = _with6.Parameters["RETURN_VALUE"].Value;
        //        if (exe > 0 & tariffPK > 0)
        //        {
        //            InsertLogChild(dsMain, objWK, tariffPK);
        //        }
        //        TRAN.Commit();
        //        if (Inserted(tariffPK) == 0)
        //        {
        //            ClearTemp(tariffPK);
        //            throw exp;
        //        }
        //        if (islog)
        //        {
        //            ClearTemp(tariffPK);
        //            //clearing the temparary records from all tables
        //        }
        //        ///'''''''''''''''
        //        //Dim dsError As New DataSet
        //        //dsError = objWK.GetDataSet("SELECT * FROM AIR_TARIFF_UPLOAD_LOG")
        //    }
        //    catch (OracleException Oraexp)
        //    {
        //        throw Oraexp;
        //        //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
        //    }
        //    catch (Exception ex)
        //    {
        //        TRAN.Rollback();
        //        throw exp;
        //    }
        //}

        //public object InsertLogChild(DataSet dsMain, WorkFlow objFw, int tariffPK)
        //{
        //    int nRowCnt = 0;
        //    try
        //    {
        //        var _with8 = objFw.MyCommand;
        //        _with8.CommandType = CommandType.StoredProcedure;
        //        _with8.CommandText = objFw.MyUserName + ".AIR_TARIFF_UPLOAD_PKG.AIR_TARIFF_TRN_INS";
        //        for (nRowCnt = 0; nRowCnt <= dsMain.Tables["tblTransaction"].Rows.Count - 1; nRowCnt++)
        //        {
        //            _with8.Parameters.Clear();
        //            //TARIFF_MAIN_AIR_FK_IN()
        //            _with8.Parameters.Add("TARIFF_MAIN_AIR_FK_IN", Convert.ToInt64(tariffPK)).Direction = ParameterDirection.Input;
        //            _with8.Parameters.Add("AIRLINE_MST_FK_IN", dsMain.Tables["tblMaster"].Rows[0]["AIRLINE_MST_FK"]);
        //            //PORT_MST_POL_FK_IN
        //            _with8.Parameters.Add("PORT_MST_POL_FK_IN", Convert.ToInt64(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["PORT_MST_POL_FK"])).Direction = ParameterDirection.Input;

        //            //PORT_MST_POD_FK_IN
        //            _with8.Parameters.Add("PORT_MST_POD_FK_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["PORT_MST_POD_FK"]).Direction = ParameterDirection.Input;
        //            //CURRENCY_MST_FK_IN
        //            _with8.Parameters.Add("CURRENCY_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CURRENCY_MST_FK"])).Direction = ParameterDirection.Input;

        //            //Valid From date
        //            _with8.Parameters.Add("VALID_FROM_IN", Convert.ToString(Convert.ToDateTime(FromDate))).Direction = ParameterDirection.Input;
        //            //VALID_TO_IN
        //            if (string.IsNullOrEmpty(ToDate) | (ToDate == null))
        //            {
        //                _with8.Parameters.Add("VALID_TO_IN", "").Direction = ParameterDirection.Input;
        //            }
        //            else
        //            {
        //                _with8.Parameters.Add("VALID_TO_IN", Convert.ToString(Convert.ToDateTime(ToDate))).Direction = ParameterDirection.Input;
        //            }
        //            //If Not IsDBNull(dsMain.Tables("tblTransaction").Rows(nRowCnt).Item("VALID_TO")) Then
        //            //    .Parameters.Add("VALID_TO_IN", _
        //            //     CStr(CDate( _
        //            //                    ToDate))).Direction = _
        //            //     ParameterDirection.Input
        //            //Else
        //            //    .Parameters.Add("VALID_TO_IN", "").Direction = ParameterDirection.Input
        //            //End If
        //            //AIR_BRK_PNTS_IN
        //            if (!string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["AIR_BRK_PNTS"]))
        //            {
        //                _with8.Parameters.Add("AIR_BRK_PNTS_IN", Convert.ToString(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["AIR_BRK_PNTS"])).Direction = ParameterDirection.Input;
        //            }
        //            else
        //            {
        //                _with8.Parameters.Add("AIR_BRK_PNTS_IN", "").Direction = ParameterDirection.Input;
        //            }
        //            //AIR_SURCHRG_IN
        //            if (!string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["AIR_SURCHRG"]))
        //            {
        //                _with8.Parameters.Add("AIR_SURCHRG_IN", Convert.ToString(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["AIR_SURCHRG"])).Direction = ParameterDirection.Input;
        //            }
        //            else
        //            {
        //                _with8.Parameters.Add("AIR_SURCHRG_IN", "").Direction = ParameterDirection.Input;
        //            }

        //            //AIR_FREIGHT_IN
        //            if (!string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["AIR_FREIGHT"]))
        //            {
        //                _with8.Parameters.Add("AIR_FREIGHT_IN", Convert.ToString(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["AIR_FREIGHT"])).Direction = ParameterDirection.Input;
        //            }
        //            else
        //            {
        //                _with8.Parameters.Add("AIR_FREIGHT_IN", "").Direction = ParameterDirection.Input;
        //            }
        //            //Return value of the proc.
        //            _with8.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
        //            //Execute the command
        //            _with8.ExecuteNonQuery();
        //        }
        //    }
        //    catch (OracleException Oraexp)
        //    {
        //        throw Oraexp;
        //        //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
        //    }
        //    catch (Exception ex)
        //    {
        //        throw new Exception("Data is not in correct format.");
        //    }
        //}

        ////this function is used for checking data is properly inserted or not
        //public int Inserted(int pk)
        //{
        //    try
        //    {
        //        string strSql = null;
        //        strSql = "SELECT COUNT(*)  FROM TARIFF_MAIN_AIR_TBL tm, TARIFF_TRN_AIR_FREIGHT_TBL tf, " + " TARIFF_TRN_AIR_SURCHARGE ts, TARIFF_TRN_AIR_TBL tt, tariff_air_breakpoints tb " + " WHERE  tm.tariff_main_air_pk=tt.tariff_main_air_fk AND " + " tt.tariff_trn_air_pk=tf.tariff_trn_air_fk AND " + " tt.tariff_trn_air_pk= ts.tariff_trn_air_fk(+) AND " + " tb.tariff_trn_freight_fk(+)=tf.tariff_trn_freight_pk AND tm.tariff_main_air_pk=" + pk;
        //        return Convert.ToInt32((new WorkFlow()).ExecuteScaler(strSql));
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

        //public DataSet ErrorDs()
        //{
        //    try
        //    {
        //        return (new WorkFlow()).GetDataSet("Select distinct * from AIR_TARIFF_UPLOAD_LOG");
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

        //#endregion "Database And log file"
    }
}