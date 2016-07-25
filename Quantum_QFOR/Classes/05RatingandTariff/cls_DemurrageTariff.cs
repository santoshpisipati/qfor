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
    public class clsDemurrageTariff : CommonFeatures
    {
        //#region "This function returns the Depot contract tarrif for perticular DepotID and the cargoType"

        //public DataSet FetchDepotData(string depotID = "", string fromdate = "", string Todate = "")
        //{
        //    WorkFlow objWF = new WorkFlow();

        //    DataSet dsMain = new DataSet();
        //    string strCondition = string.Empty;
        //    string str = null;

        //    if ((depotID != null))
        //    {
        //        strCondition += " AND dmt.vendor_mst_pk = " + depotID;
        //    }

        //    str = str.Empty + "SELECT";
        //    str += "        trn.cont_trn_depot_pk,";
        //    str += "        trn.from_day,";
        //    str += "        trn.to_day,";
        //    str += "        trn.lcl_volume,";
        //    str += "        trn.lcl_weight,";
        //    str += "        trn.lcl_amount";
        //    str += "        FROM    cont_trn_depot_fcl_lcl trn,";
        //    str += "        cont_main_depot_tbl depot,";
        //    str += "        vendor_mst_tbl dmt,";
        //    str += "        currency_type_mst_tbl curr";
        //    str += "        WHERE 1 = 1";
        //    str += "        AND dmt.vendor_mst_pk = depot.depot_mst_fk";
        //    str += "        AND depot.cont_main_depot_pk = trn.cont_main_depot_fk ";
        //    str += "        AND depot.active=1 ";
        //    str += "        AND depot.cont_approved = 1";
        //    str += "        AND depot.business_type = 1";
        //    str += "        AND depot.currency_mst_fk = curr.currency_mst_pk";
        //    str += "        AND depot.cargo_type = 2 AND TO_DATE('" + fromdate + "','" + dateFormat + "') BETWEEN depot.valid_from  AND  NVL(depot.valid_to, NULL_DATE_FORMAT) ";
        //    //for LCL
        //    if (!string.IsNullOrEmpty(Todate))
        //    {
        //        str += "    AND TO_DATE('" + Todate + "','" + dateFormat + "') BETWEEN depot.valid_from  AND  NVL(depot.valid_to, NULL_DATE_FORMAT) ";
        //    }
        //    str += strCondition;

        //    str += " ORDER BY from_day ";

        //    try
        //    {
        //        dsMain = objWF.GetDataSet(str);
        //        return dsMain;
        //    }
        //    catch (OracleException OraExp)
        //    {
        //        throw OraExp;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public DataSet FetchDepotDataOth(string depotID = "", string fromdate = "", string Todate = "")
        //{
        //    WorkFlow objWF = new WorkFlow();

        //    DataSet dsMain = new DataSet();
        //    string strCondition = string.Empty;
        //    string str = null;

        //    if ((depotID != null))
        //    {
        //        strCondition += " AND dmt.vendor_mst_pk = " + depotID;
        //    }

        //    str = str.Empty + "SELECT";
        //    str += "        trf_trn.cont_depot_oth_chg_pk, ";
        //    str += "        trf_trn.cost_element_mst_fk, ";
        //    str += "        'true' as Sel, ";
        //    str += "        c.COST_ELEMENT_NAME, ";
        //    str += "        trf_trn.lcl_rate_per_cbm, ";
        //    str += "        trf_trn.lcl_rate_per_ton ";
        //    str += "        FROM   cont_trn_depot_oth_chg trf_trn, ";
        //    str += "        cont_main_depot_tbl depot, ";
        //    str += "        vendor_mst_tbl dmt, ";
        //    str += "        cost_element_mst_tbl c ";
        //    str += "        WHERE 1 = 1";
        //    str += "        AND dmt.vendor_mst_pk = depot.depot_mst_fk";
        //    str += "        AND depot.cont_main_depot_pk = trf_trn.cont_main_depot_fk ";
        //    str += "        and trf_trn.cost_element_mst_fk = c.cost_element_mst_pk  ";
        //    str += "        AND depot.active=1 ";
        //    str += "        AND depot.cont_approved = 1";
        //    str += "        AND depot.business_type = 1";
        //    str += "        AND depot.cargo_type = 2 AND TO_DATE('" + fromdate + "','" + dateFormat + "') BETWEEN depot.valid_from  AND  NVL(depot.valid_to, NULL_DATE_FORMAT) ";
        //    //for LCL
        //    if (!string.IsNullOrEmpty(Todate))
        //    {
        //        str += "    AND TO_DATE('" + Todate + "','" + dateFormat + "') BETWEEN depot.valid_from  AND  NVL(depot.valid_to, NULL_DATE_FORMAT) ";
        //    }
        //    str += strCondition;
        //    str += "  UNION";
        //    str += "  SELECT null CONT_DEPOT_OTH_CHG_PK, ";
        //    str += "  C.COST_ELEMENT_MST_PK, ";
        //    str += "  'false' as \"Sel\", ";
        //    str += "  c.COST_ELEMENT_NAME AS \"Other Charges\",  ";
        //    str += "  null lcl_rate_per_cbm,";
        //    str += "  null lcl_rate_per_ton";
        //    str += "  FROM COST_ELEMENT_MST_TBL   C,";
        //    str += "  CONT_TRN_DEPOT_OTH_CHG CON,";
        //    str += "  VENDOR_TYPE_MST_TBL    V";
        //    str += "  WHERE C.VENDOR_TYPE_MST_FK = V.VENDOR_TYPE_PK";
        //    str += "  AND C.BUSINESS_TYPE in (3,1)";
        //    str += "  AND V.VENDOR_TYPE_ID = 'WAREHOUSE'";
        //    str += "  AND C.COST_ELEMENT_MST_PK NOT IN (SELECT trn.COST_ELEMENT_MST_FK";
        //    str += " FROM CONT_MAIN_DEPOT_TBL mn, cont_trn_depot_oth_chg trn,";
        //    str += " vendor_mst_tbl         dmt, cost_element_mst_tbl   c";
        //    str += " WHERE mn.cont_main_depot_pk = trn.cont_main_depot_fk";
        //    str += " AND mn.depot_mst_fk = dmt.vendor_mst_pk";
        //    str += " and c.cost_element_mst_pk = trn.cost_element_mst_fk";
        //    str += " AND dmt.vendor_mst_pk = " + depotID;
        //    str += " AND TO_DATE('" + fromdate + "','" + dateFormat + "') BETWEEN mn.valid_from AND NVL(mn.valid_to, NULL_DATE_FORMAT) ";
        //    if (!string.IsNullOrEmpty(Todate))
        //    {
        //        str += "    AND TO_DATE('" + Todate + "','" + dateFormat + "') BETWEEN mn.valid_from  AND  NVL(mn.valid_to, NULL_DATE_FORMAT) ";
        //    }
        //    str += " )";
        //    try
        //    {
        //        dsMain = objWF.GetDataSet(str);
        //        return dsMain;
        //    }
        //    catch (OracleException OraExp)
        //    {
        //        throw OraExp;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //#endregion "This function returns the Depot contract tarrif for perticular DepotID and the cargoType"

        //#region "This function returns the Depot contract tarrif for perticular DetentionID and the cargoType"

        //public DataSet FetchDetentionData(string demurrageID = "")
        //{
        //    string str = null;
        //    WorkFlow objWF = new WorkFlow();
        //    DataSet dsMain = new DataSet();

        //    str = str.Empty + "SELECT";
        //    str += "       dem_lcl_fcl.demurage_slab_trn_pk,";
        //    str += "       dem_lcl_fcl.from_day, ";
        //    str += "       dem_lcl_fcl.to_day,";
        //    str += "       dem_lcl_fcl.lcl_volume,";
        //    str += "       dem_lcl_fcl.lcl_weight,";
        //    str += "       dem_lcl_fcl.lcl_amount";
        //    str += "       from   demurrage_slab_main_tbl dem_main,";
        //    str += "       demurrage_slab_trn dem_lcl_fcl";
        //    str += "       where 1 = 1 ";
        //    str += "       and dem_main.demurage_slab_main_pk = " + demurrageID;
        //    str += "       and dem_main.demurage_slab_main_pk= dem_lcl_fcl.demurage_slab_main_fk";
        //    str += " ORDER BY from_day ";

        //    try
        //    {
        //        dsMain = objWF.GetDataSet(str);
        //        return dsMain;
        //    }
        //    catch (OracleException OraExp)
        //    {
        //        throw OraExp;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public DataSet FetchDetentionDataOth(string demurrageID = "")
        //{
        //    string str = null;
        //    WorkFlow objWF = new WorkFlow();
        //    DataSet dsMain = new DataSet();

        //    str = str.Empty + "SELECT";
        //    str += "       trf_trn.trf_air_depot_oth_chg_pk,";
        //    str += "       trf_trn.cost_element_mst_fk, ";
        //    str += "        'true' as \"Sel\", c.COST_ELEMENT_NAME AS \"Other Charges\",   ";
        //    str += "       trf_trn.lcl_rate_per_cbm, ";
        //    str += "       trf_trn.lcl_rate_per_100kg ";
        //    str += "       from   demurrage_slab_main_tbl dem_main,";
        //    str += "       TARIFF_AIR_DEPOT_OTH_CHG trf_trn ,cost_element_mst_tbl c ";
        //    str += "       where 1 = 1 ";
        //    str += "   and trf_trn.cost_element_mst_fk = c.cost_element_mst_pk ";
        //    str += "       and dem_main.demurage_slab_main_pk = " + demurrageID;
        //    str += "       and dem_main.demurage_slab_main_pk = trf_trn.demurage_slab_main_fk ";
        //    str += "  GROUP BY cost_element_mst_fk,COST_ELEMENT_NAME,trf_air_depot_oth_chg_pk,lcl_rate_per_cbm,lcl_rate_per_100kg ";
        //    str += "  UNION";
        //    str += "  SELECT NULL trf_air_depot_oth_chg_pk,";
        //    str += "   c.cost_element_mst_pk,";
        //    str += "  'false' as Sel,";
        //    str += "  c.COST_ELEMENT_NAME AS \"Other Charges\",";
        //    str += "  NULL lcl_rate_per_cbm,";
        //    str += "  NULL lcl_rate_per_100kg";
        //    str += "  FROM VENDOR_TYPE_MST_TBL  V,";
        //    str += "  cost_element_mst_tbl   c";
        //    str += "  WHERE 1 = 1 ";
        //    str += "  AND C.VENDOR_TYPE_MST_FK = V.VENDOR_TYPE_PK ";
        //    str += "  AND V.VENDOR_TYPE_ID = 'WAREHOUSE' ";
        //    str += "  and c.business_type in (3,1) ";
        //    str += "  AND C.COST_ELEMENT_MST_PK NOT IN (SELECT TAN.COST_ELEMENT_MST_FK";
        //    str += "  FROM CONT_MAIN_DEPOT_TBL CMDT,";
        //    str += "  TARIFF_AIR_DEPOT_OTH_CHG TAN,";
        //    str += "  demurrage_slab_main_tbl DEM1";
        //    str += "  WHERE CMDT.CONT_MAIN_DEPOT_PK = DEM1.CONT_MAIN_DEPOT_FK";
        //    str += "  AND DEM1.DEMURAGE_SLAB_MAIN_PK = TAN.DEMURAGE_SLAB_MAIN_FK";
        //    str += "  AND DEM1.DEMURAGE_SLAB_MAIN_PK = " + demurrageID + " ) ";
        //    try
        //    {
        //        dsMain = objWF.GetDataSet(str);
        //        return dsMain;
        //    }
        //    catch (OracleException OraExp)
        //    {
        //        throw OraExp;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //#endregion "This function returns the Depot contract tarrif for perticular DetentionID and the cargoType"

        //#region "Save Function"

        //public ArrayList Save(DataSet M_DataSet, DataSet dsChildData, DataSet dsChildDataOth, bool isEdting, string demurragePK, string userLocation, string tariffRefNumber, int employeeID)
        //{
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

        //    OracleCommand insCommandChlid = new OracleCommand();
        //    OracleCommand updCommandChild = new OracleCommand();

        //    OracleCommand insCommandChlidOth = new OracleCommand();
        //    OracleCommand updCommandChildOth = new OracleCommand();

        //    if (isEdting == false)
        //    {
        //        tariffRefNumber = GenerateProtocolKey("DEMURRAGE SLAB", userLocation, employeeID, System.DateTime.Now, , , , M_LAST_MODIFIED_BY_FK);
        //    }

        //    try
        //    {
        //        DataTable DtTbl = new DataTable();
        //        DataRow DtRw = null;
        //        int i = 0;

        //        var _with1 = insCommand;
        //        _with1.Connection = objWK.MyConnection;
        //        _with1.CommandType = CommandType.StoredProcedure;
        //        _with1.CommandText = objWK.MyUserName + ".DEMURRAGE_TARIFF_PKG.DEMURRAGE_SLAB_MAIN_TBL_INS";
        //        var _with2 = _with1.Parameters;
        //        insCommand.Parameters.Add("DEPOT_MST_FK_IN", OracleDbType.Int32, 10, "DEPOT_MST_FK").Direction = ParameterDirection.Input;
        //        insCommand.Parameters["DEPOT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
        //        insCommand.Parameters.Add("TARIFF_REF_NO_IN", tariffRefNumber).Direction = ParameterDirection.Input;
        //        insCommand.Parameters.Add("TARIFF_DATE_IN", OracleDbType.Date, 20, "TARIFF_DATE").Direction = ParameterDirection.Input;
        //        insCommand.Parameters["TARIFF_DATE_IN"].SourceVersion = DataRowVersion.Current;
        //        insCommand.Parameters.Add("VALID_FROM_IN", OracleDbType.Date, 20, "VALID_FROM").Direction = ParameterDirection.Input;
        //        insCommand.Parameters["VALID_FROM_IN"].SourceVersion = DataRowVersion.Current;
        //        insCommand.Parameters.Add("VALID_TO_IN", OracleDbType.Date, 20, "VALID_TO").Direction = ParameterDirection.Input;
        //        insCommand.Parameters["VALID_TO_IN"].SourceVersion = DataRowVersion.Current;
        //        insCommand.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
        //        insCommand.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
        //        insCommand.Parameters.Add("ACTIVE_IN", OracleDbType.Int32, 1, "ACTIVE").Direction = ParameterDirection.Input;
        //        insCommand.Parameters["ACTIVE_IN"].SourceVersion = DataRowVersion.Current;
        //        insCommand.Parameters.Add("cont_main_depot_fk_in", OracleDbType.Int32, 10, "cont_main_depot_fk").Direction = ParameterDirection.Input;
        //        insCommand.Parameters["cont_main_depot_fk_in"].SourceVersion = DataRowVersion.Current;
        //        insCommand.Parameters.Add("CREATED_BY_FK_IN", CREATED_BY).Direction = ParameterDirection.Input;
        //        insCommand.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
        //        insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "DETENTION_SLAB_MAIN_PK").Direction = ParameterDirection.Output;
        //        insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

        //        var _with3 = updCommand;
        //        _with3.Connection = objWK.MyConnection;
        //        _with3.CommandType = CommandType.StoredProcedure;
        //        _with3.CommandText = objWK.MyUserName + ".DEMURRAGE_TARIFF_PKG.DEMURRAGE_SLAB_MAIN_TBL_UPD";
        //        var _with4 = _with3.Parameters;

        //        updCommand.Parameters.Add("DEMURAGE_SLAB_MAIN_PK_IN", OracleDbType.Int32, 10, "DEMURAGE_SLAB_MAIN_PK").Direction = ParameterDirection.Input;
        //        updCommand.Parameters["DEMURAGE_SLAB_MAIN_PK_IN"].SourceVersion = DataRowVersion.Current;
        //        updCommand.Parameters.Add("DEPOT_MST_FK_IN", OracleDbType.Int32, 10, "DEPOT_MST_FK").Direction = ParameterDirection.Input;
        //        updCommand.Parameters["DEPOT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
        //        updCommand.Parameters.Add("TARIFF_REF_NO_IN", OracleDbType.Varchar2, 20, "TARIFF_REF_NO").Direction = ParameterDirection.Input;
        //        updCommand.Parameters["TARIFF_REF_NO_IN"].SourceVersion = DataRowVersion.Current;
        //        updCommand.Parameters.Add("TARIFF_DATE_IN", OracleDbType.Date, 20, "TARIFF_DATE").Direction = ParameterDirection.Input;
        //        updCommand.Parameters["TARIFF_DATE_IN"].SourceVersion = DataRowVersion.Current;
        //        updCommand.Parameters.Add("VALID_FROM_IN", OracleDbType.Date, 20, "VALID_FROM").Direction = ParameterDirection.Input;
        //        updCommand.Parameters["VALID_FROM_IN"].SourceVersion = DataRowVersion.Current;
        //        updCommand.Parameters.Add("VALID_TO_IN", OracleDbType.Date, 20, "VALID_TO").Direction = ParameterDirection.Input;
        //        updCommand.Parameters["VALID_TO_IN"].SourceVersion = DataRowVersion.Current;
        //        updCommand.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
        //        updCommand.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
        //        updCommand.Parameters.Add("ACTIVE_IN", OracleDbType.Int32, 1, "ACTIVE").Direction = ParameterDirection.Input;
        //        updCommand.Parameters["ACTIVE_IN"].SourceVersion = DataRowVersion.Current;
        //        updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 1, "VERSION_NO").Direction = ParameterDirection.Input;
        //        updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

        //        updCommand.Parameters.Add("cont_main_depot_fk_in", OracleDbType.Int32, 10, "cont_main_depot_fk").Direction = ParameterDirection.Input;
        //        updCommand.Parameters["cont_main_depot_fk_in"].SourceVersion = DataRowVersion.Current;

        //        updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", LAST_MODIFIED_BY).Direction = ParameterDirection.Input;

        //        updCommand.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

        //        updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
        //        updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

        //        objWK.MyDataAdapter.RowUpdated += new OracleRowUpdatedEventHandler(OnRowUpdated);

        //        var _with5 = objWK.MyDataAdapter;

        //        _with5.InsertCommand = insCommand;
        //        _with5.InsertCommand.Transaction = TRAN;

        //        _with5.UpdateCommand = updCommand;
        //        _with5.UpdateCommand.Transaction = TRAN;

        //        RecAfct = _with5.Update(M_DataSet);

        //        if (arrMessage.Count > 0)
        //        {
        //            if (isEdting == false)
        //            {
        //                RollbackProtocolKey("DEMURRAGE SLAB", HttpContext.Current.Session["LOGED_IN_LOC_FK"], HttpContext.Current.Session["EMP_PK"], tariffRefNumber, System.DateTime.Now);
        //                //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
        //            }
        //            TRAN.Rollback();
        //            return arrMessage;
        //        }
        //        else
        //        {
        //            if (isEdting == false)
        //            {
        //                demurragePK = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
        //            }
        //            else
        //            {
        //                demurragePK = Convert.ToInt32(updCommand.Parameters["RETURN_VALUE"].Value);
        //                DeleteOthChrg(demurragePK);
        //            }
        //        }

        //        var _with6 = insCommandChlid;
        //        _with6.Connection = objWK.MyConnection;
        //        _with6.CommandType = CommandType.StoredProcedure;
        //        _with6.CommandText = objWK.MyUserName + ".DEMURRAGE_TARIFF_PKG.DEMURRAGE_SLAB_TRN_INS";
        //        var _with7 = _with6.Parameters;
        //        insCommandChlid.Parameters.Add("DEMURAGE_SLAB_MAIN_FK_IN", demurragePK).Direction = ParameterDirection.Input;
        //        insCommandChlid.Parameters.Add("FROM_DAY_IN", OracleDbType.Int32, 3, "FROM_DAY").Direction = ParameterDirection.Input;
        //        insCommandChlid.Parameters["FROM_DAY_IN"].SourceVersion = DataRowVersion.Current;
        //        insCommandChlid.Parameters.Add("TO_DAY_IN", OracleDbType.Int32, 3, "TO_DAY").Direction = ParameterDirection.Input;
        //        insCommandChlid.Parameters["TO_DAY_IN"].SourceVersion = DataRowVersion.Current;
        //        insCommandChlid.Parameters.Add("LCL_VOLUME_IN", OracleDbType.Int32, 10, "LCL_VOLUME").Direction = ParameterDirection.Input;
        //        insCommandChlid.Parameters["LCL_VOLUME_IN"].SourceVersion = DataRowVersion.Current;
        //        insCommandChlid.Parameters.Add("LCL_WEIGHT_IN", OracleDbType.Int32, 10, "LCL_WEIGHT").Direction = ParameterDirection.Input;
        //        insCommandChlid.Parameters["LCL_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;
        //        insCommandChlid.Parameters.Add("LCL_AMOUNT_IN", OracleDbType.Int32, 10, "LCL_AMOUNT").Direction = ParameterDirection.Input;
        //        insCommandChlid.Parameters["LCL_AMOUNT_IN"].SourceVersion = DataRowVersion.Current;
        //        insCommandChlid.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "DEMURAGE_SLAB_TRN_PK").Direction = ParameterDirection.Output;
        //        insCommandChlid.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

        //        var _with8 = updCommandChild;
        //        _with8.Connection = objWK.MyConnection;
        //        _with8.CommandType = CommandType.StoredProcedure;
        //        _with8.CommandText = objWK.MyUserName + ".DEMURRAGE_TARIFF_PKG.DEMURRAGE_SLAB_TRN_UPD";
        //        var _with9 = _with8.Parameters;
        //        updCommandChild.Parameters.Add("DEMURRAGE_SLAB_TRN_PK_IN", OracleDbType.Int32, 10, "DEMURAGE_SLAB_TRN_PK").Direction = ParameterDirection.Input;
        //        updCommandChild.Parameters["DEMURRAGE_SLAB_TRN_PK_IN"].SourceVersion = DataRowVersion.Current;
        //        updCommandChild.Parameters.Add("DEMURRAGE_SLAB_MAIN_FK_IN", demurragePK).Direction = ParameterDirection.Input;
        //        updCommandChild.Parameters.Add("FROM_DAY_IN", OracleDbType.Int32, 10, "FROM_DAY").Direction = ParameterDirection.Input;
        //        updCommandChild.Parameters["FROM_DAY_IN"].SourceVersion = DataRowVersion.Current;
        //        updCommandChild.Parameters.Add("TO_DAY_IN", OracleDbType.Int32, 10, "TO_DAY").Direction = ParameterDirection.Input;
        //        updCommandChild.Parameters["TO_DAY_IN"].SourceVersion = DataRowVersion.Current;
        //        updCommandChild.Parameters.Add("LCL_VOLUME_IN", OracleDbType.Int32, 10, "LCL_VOLUME").Direction = ParameterDirection.Input;
        //        updCommandChild.Parameters["LCL_VOLUME_IN"].SourceVersion = DataRowVersion.Current;
        //        updCommandChild.Parameters.Add("LCL_WEIGHT_IN", OracleDbType.Int32, 10, "LCL_WEIGHT").Direction = ParameterDirection.Input;
        //        updCommandChild.Parameters["LCL_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;
        //        updCommandChild.Parameters.Add("LCL_AMOUNT_IN", OracleDbType.Int32, 10, "LCL_AMOUNT").Direction = ParameterDirection.Input;
        //        updCommandChild.Parameters["LCL_AMOUNT_IN"].SourceVersion = DataRowVersion.Current;
        //        updCommandChild.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "DEMURAGE_SLAB_TRN_PK").Direction = ParameterDirection.Output;
        //        updCommandChild.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

        //        var _with10 = objWK.MyDataAdapter;

        //        _with10.InsertCommand = insCommandChlid;
        //        _with10.InsertCommand.Transaction = TRAN;

        //        _with10.UpdateCommand = updCommandChild;
        //        _with10.UpdateCommand.Transaction = TRAN;

        //        RecAfct = _with10.Update(dsChildData);

        //        if (arrMessage.Count > 0)
        //        {
        //            if (isEdting == false)
        //            {
        //                RollbackProtocolKey("DEMURRAGE SLAB", HttpContext.Current.Session["LOGED_IN_LOC_FK"], HttpContext.Current.Session["EMP_PK"], tariffRefNumber, System.DateTime.Now);
        //                //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
        //            }
        //            TRAN.Rollback();
        //            return arrMessage;
        //        }

        //        var _with11 = insCommandChlidOth;
        //        _with11.Connection = objWK.MyConnection;
        //        _with11.CommandType = CommandType.StoredProcedure;
        //        _with11.CommandText = objWK.MyUserName + ".DEMURRAGE_TARIFF_PKG.TARIFF_AIR_DEPOT_OTH_CHG_INS";
        //        var _with12 = _with11.Parameters;

        //        insCommandChlidOth.Parameters.Add("DEMURAGE_SLAB_MAIN_FK_IN", Convert.ToInt64(demurragePK)).Direction = ParameterDirection.Input;

        //        insCommandChlidOth.Parameters.Add("COST_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "COST_ELEMENT_MST_FK").Direction = ParameterDirection.Input;
        //        insCommandChlidOth.Parameters["COST_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

        //        insCommandChlidOth.Parameters.Add("LCL_VOLUME_IN", OracleDbType.Int32, 10, "LCL_VOLUME").Direction = ParameterDirection.Input;
        //        insCommandChlidOth.Parameters["LCL_VOLUME_IN"].SourceVersion = DataRowVersion.Current;

        //        insCommandChlidOth.Parameters.Add("LCL_WEIGHT_IN", OracleDbType.Int32, 10, "LCL_WEIGHT").Direction = ParameterDirection.Input;
        //        insCommandChlidOth.Parameters["LCL_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

        //        insCommandChlidOth.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
        //        insCommandChlidOth.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

        //        objWK.MyDataAdapter.RowUpdated += new OracleRowUpdatedEventHandler(OnRowUpdated);

        //        var _with13 = objWK.MyDataAdapter;
        //        _with13.InsertCommand = insCommandChlidOth;
        //        _with13.InsertCommand.Transaction = TRAN;

        //        if (dsChildDataOth.Tables.Count > 0)
        //        {
        //            RecAfct = _with13.Update(dsChildDataOth.Tables[0]);
        //        }

        //        if (arrMessage.Count > 1)
        //        {
        //            if (isEdting == false)
        //            {
        //                RollbackProtocolKey("DEMURRAGE SLAB", HttpContext.Current.Session["LOGED_IN_LOC_FK"], HttpContext.Current.Session["EMP_PK"], tariffRefNumber, System.DateTime.Now);
        //                //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
        //            }
        //            TRAN.Rollback();
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
        //        if (isEdting == false)
        //        {
        //            RollbackProtocolKey("DEMURRAGE SLAB", HttpContext.Current.Session["LOGED_IN_LOC_FK"], HttpContext.Current.Session["EMP_PK"], tariffRefNumber, System.DateTime.Now);
        //            //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
        //        }
        //        throw oraexp;
        //    }
        //    catch (Exception ex)
        //    {
        //        if (isEdting == false)
        //        {
        //            RollbackProtocolKey("DEMURRAGE SLAB", HttpContext.Current.Session["LOGED_IN_LOC_FK"], HttpContext.Current.Session["EMP_PK"], tariffRefNumber, System.DateTime.Now);
        //            //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
        //        }
        //        throw ex;
        //    }
        //    finally
        //    {
        //        objWK.CloseConnection();
        //    }
        //}

        //#endregion "Save Function"

        //#region "Delete Other Chrge"

        //public string DeleteOthChrg(long demurragePK)
        //{
        //    string Strsql = null;
        //    WorkFlow Objwk = new WorkFlow();
        //    Strsql = "DELETE FROM TARIFF_AIR_DEPOT_OTH_CHG  TAR WHERE TAR.DEMURAGE_SLAB_MAIN_FK = " + demurragePK;
        //    try
        //    {
        //        Objwk.ExecuteCommands(Strsql);
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

        //#endregion "Delete Other Chrge"

        //#region "Fetch main Data"

        //public DataSet FetchMainData(string demurrageID = "")
        //{
        //    string str = null;
        //    WorkFlow objWF = new WorkFlow();
        //    DataSet dsMain = null;

        //    str = str.Empty + "SELECT ";
        //    str += "       d.demurage_slab_main_pk,";
        //    str += "       d.depot_mst_fk,";
        //    str += "       d.tariff_ref_no,";
        //    str += "       to_date(d.tariff_date,'dd/MM/RRRR') \"tariff_date\" ,";
        //    str += "       to_date(d.valid_from,'dd/MM/RRRR') \"valid_from\" ,";
        //    str += "       to_date(d.valid_to,'dd/MM/RRRR') \"valid_to\" ,";
        //    str += "       d.currency_mst_fk,";
        //    str += "       d.active,";
        //    str += "       d.version_no,";
        //    str += "       d.cont_main_depot_fk";
        //    str += "FROM";
        //    str += "       demurrage_slab_main_tbl d";
        //    str += "WHERE";
        //    str += "       d.demurage_slab_main_pk = " + demurrageID;

        //    try
        //    {
        //        dsMain = objWF.GetDataSet(str);
        //        return dsMain;
        //    }
        //    catch (OracleException OraExp)
        //    {
        //        throw OraExp;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //#endregion "Fetch main Data"

        //#region "Fetch Currency"

        //public DataTable FetchCurrencyData(string currencyPK = "")
        //{
        //    string str = null;
        //    WorkFlow objWF = new WorkFlow();
        //    DataTable dtMain = null;

        //    str = str.Empty + "select ";
        //    str += "       c.currency_mst_pk,";
        //    str += "       c.currency_id,";
        //    str += "       c.currency_name";
        //    str += "       from";
        //    str += "       currency_type_mst_tbl c";
        //    str += "       where";
        //    str += "       c.currency_mst_pk=" + currencyPK;

        //    try
        //    {
        //        dtMain = objWF.GetDataTable(str);
        //        return dtMain;
        //    }
        //    catch (OracleException OraExp)
        //    {
        //        throw OraExp;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //#endregion "Fetch Currency"

        //#region "Depot description"

        //public DataTable FetchDepotDesc(string DepotPK = "")
        //{
        //    string str = null;
        //    WorkFlow objWF = new WorkFlow();
        //    DataTable dtMain = null;

        //    str = str.Empty + "select";
        //    str += "       c.vendor_mst_pk,";
        //    str += "       c.vendor_id,";
        //    str += "       c.vendor_name";
        //    str += "       from";
        //    str += "       vendor_mst_tbl c";
        //    str += "       where";
        //    str += "       c.vendor_mst_pk=" + DepotPK;

        //    try
        //    {
        //        dtMain = objWF.GetDataTable(str);
        //        return dtMain;
        //    }
        //    catch (OracleException OraExp)
        //    {
        //        throw OraExp;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //#endregion "Depot description"

        //#region "Fetch Contract Description"

        //public DataTable FetchContractDescEdit(string contractID = "")
        //{
        //    string str = null;
        //    WorkFlow objWF = new WorkFlow();
        //    DataTable dtContainerData = new DataTable();

        //    Array arrPolPk = null;
        //    Array arrPodPk = null;
        //    string strCondition = string.Empty;

        //    str = str.Empty + "SELECT";
        //    str += "        depot.cont_main_depot_pk, ";
        //    str += "        depot.contract_no,";
        //    str += "        depot.valid_from,";
        //    str += "        depot.valid_to,";
        //    str += "        depot.currency_mst_fk";
        //    str += " from   cont_main_depot_tbl depot,";
        //    str += "        vendor_mst_tbl dmt";
        //    str += " where  1=1 ";
        //    str += "        AND depot.depot_mst_fk =dmt.vendor_mst_pk";
        //    str += "        AND depot.cont_main_depot_pk=" + contractID;

        //    try
        //    {
        //        dtContainerData = objWF.GetDataTable(str);
        //        return dtContainerData;
        //    }
        //    catch (OracleException OraExp)
        //    {
        //        throw OraExp;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //#endregion "Fetch Contract Description"

        //#region "Fetch Contract Description"

        //public DataTable FetchContractDesc(string depotID = "", string fromdate = "", string Todate = "")
        //{
        //    string str = null;
        //    WorkFlow objWF = new WorkFlow();
        //    DataTable dtContainerData = new DataTable();

        //    Array arrPolPk = null;
        //    Array arrPodPk = null;
        //    string strCondition = string.Empty;

        //    str = str.Empty + "SELECT";
        //    str += "        depot.cont_main_depot_pk, ";
        //    str += "        depot.contract_no,";
        //    str += "        depot.valid_from,";
        //    str += "        depot.valid_to,";
        //    str += "        depot.currency_mst_fk";
        //    str += " from   cont_main_depot_tbl depot,";
        //    str += "        vendor_mst_tbl dmt";
        //    str += " where  1=1 ";
        //    str += "        AND depot.depot_mst_fk =dmt.vendor_mst_pk";
        //    str += "        AND depot.active=1 ";
        //    str += "        AND depot.cont_approved = 1";
        //    str += "        AND depot.business_type = 1 AND TO_DATE('" + fromdate + "','" + dateFormat + "') BETWEEN depot.valid_from AND NVL(depot.valid_to, NULL_DATE_FORMAT) ";
        //    //only for sea...
        //    if (!string.IsNullOrEmpty(Todate))
        //    {
        //        str += "    AND TO_DATE('" + Todate + "','" + dateFormat + "') BETWEEN depot.valid_from  AND  NVL(depot.valid_to, NULL_DATE_FORMAT)";
        //    }
        //    str += "        AND dmt.vendor_mst_pk = " + depotID;
        //    str += "        AND depot.cargo_type = 2";
        //    try
        //    {
        //        dtContainerData = objWF.GetDataTable(str);
        //        return dtContainerData;
        //    }
        //    catch (OracleException OraExp)
        //    {
        //        throw OraExp;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //#endregion "Fetch Contract Description"
    }
}