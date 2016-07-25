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
    public class cls_Depot_Contract_Entry : CommonFeatures
    {
        /// <summary>
        /// The object wk
        /// </summary>
        private WorkFlow objWK = new WorkFlow();

        /// <summary>
        /// The _ pk value
        /// </summary>
        private long _PkValue;

        //#region " Property "

        //public long PkValue
        //{
        //    get { return _PkValue; }
        //}

        //#endregion " Property "

        //#region " Grid Heading "

        //public DataTable FetchDTL(string strContId = "", string depotMainPK = "", bool NewRecord = false)
        //{
        //    string str = null;
        //    WorkFlow objWF = new WorkFlow();
        //    DataTable dtMain = new DataTable();
        //    DataTable dtContainerType = new DataTable();
        //    DataColumn dcCol = null;
        //    Int16 rowCnt = default(Int16);
        //    string strCondition = string.Empty;
        //    strCondition += " AND  1 <> 1 ";
        //    str = str.Empty + "SELECT CT.CONT_TRN_DEPOT_PK,";
        //    str += " CT.FROM_DAY,";
        //    str += " CT.TO_DAY ";
        //    str += " FROM CONT_MAIN_DEPOT_TBL CMDT,";
        //    str += " CONT_TRN_DEPOT_FCL_LCL CT";
        //    str += " WHERE CMDT.CONT_MAIN_DEPOT_PK = CT.CONT_MAIN_DEPOT_FK ";
        //    str += strCondition;
        //    try
        //    {
        //        dtMain = objWF.GetDataTable(str);
        //        dtContainerType = FetchActiveCont(strContId, depotMainPK, NewRecord);
        //        for (rowCnt = 0; rowCnt <= dtContainerType.Rows.Count - 1; rowCnt++)
        //        {
        //            dcCol = new DataColumn(Convert.ToString(dtContainerType.Rows[rowCnt][1]));
        //            dtMain.Columns.Add(dcCol);
        //        }
        //        return dtMain;
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

        //#endregion " Grid Heading "

        //#region " Grid Heading "

        //public DataTable FetchDTLOthChrg(string strContId = "", bool NewRecord = false)
        //{
        //    string str = null;
        //    WorkFlow objWF = new WorkFlow();
        //    DataTable dtMain = new DataTable();
        //    DataTable dtContainerType = new DataTable();
        //    DataColumn dcCol = null;
        //    Int16 rowCnt = default(Int16);
        //    string strCondition = string.Empty;
        //    str = str.Empty + " SELECT '' CONT_DEPOT_OTH_CHG_PK,";
        //    str += "  C.COST_ELEMENT_MST_PK,";
        //    str += "  'false' as Sel,";
        //    str += "  C.COST_ELEMENT_NAME AS \"Other Charges\"";
        //    str += "  FROM COST_ELEMENT_MST_TBL   C,";
        //    str += "  VENDOR_TYPE_MST_TBL    V";
        //    str += "  WHERE C.VENDOR_TYPE_MST_FK = V.VENDOR_TYPE_PK";
        //    str += "  AND C.BUSINESS_TYPE in (3,2)";
        //    str += "  AND V.VENDOR_TYPE_ID = 'WAREHOUSE'";
        //    str += strCondition;
        //    try
        //    {
        //        dtMain = objWF.GetDataTable(str);
        //        dtContainerType = FetchActiveCont(strContId, , NewRecord);
        //        for (rowCnt = 0; rowCnt <= dtContainerType.Rows.Count - 1; rowCnt++)
        //        {
        //            dcCol = new DataColumn(Convert.ToString(dtContainerType.Rows[rowCnt][1]));
        //            dtMain.Columns.Add(dcCol);
        //        }
        //        return dtMain;
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

        //#endregion " Grid Heading "

        //#region "Container Main Header"

        //public object fetchContMainHDR(string depot_Pk)
        //{
        //    string strSQL = null;
        //    DataSet dsHeader = new DataSet();
        //    WorkFlow objWF = new WorkFlow();
        //    strSQL = "SELECT  CMDT.CONT_MAIN_DEPOT_PK,CMDT.CONTRACT_NO,";
        //    strSQL = strSQL + " CMDT.DEPOT_MST_FK,DMT.VENDOR_ID,DMT.VENDOR_NAME,";
        //    strSQL = strSQL + " TO_CHAR(CMDT.CONTRACT_DATE,'" + dateFormat + "') CONTRACT_DATE,";
        //    strSQL = strSQL + " CMDT.CONT_APPROVED,";
        //    strSQL = strSQL + " TO_CHAR(CMDT.VALID_FROM,'" + dateFormat + "') VALID_FROM,";
        //    strSQL = strSQL + " TO_CHAR(CMDT.VALID_TO,'" + dateFormat + "') VALID_TO,";
        //    strSQL = strSQL + " CTM.CURRENCY_ID,";
        //    strSQL = strSQL + " CMDT.BILLING_CYCLE_DAYS,";
        //    strSQL = strSQL + " CMDT.CARGO_TYPE,";
        //    strSQL = strSQL + " CMDT.ACTIVE,";
        //    strSQL = strSQL + " CMDT.BUSINESS_TYPE,CMDT.VERSION_NO,CTM.CURRENCY_MST_PK,CMDT.WORKFLOW_STATUS, ";
        //    strSQL = strSQL + " TO_CHAR(CMDT.LAST_MODIFIED_DT,'" + dateFormat + "')LAST_MODIFIED_DT,";
        //    strSQL = strSQL + " UMT.USER_NAME USER_ID ";
        //    strSQL = strSQL + " FROM CONT_MAIN_DEPOT_TBL CMDT,";
        //    strSQL = strSQL + " CURRENCY_TYPE_MST_TBL CTM, VENDOR_MST_TBL DMT,USER_MST_TBL UMT";
        //    strSQL = strSQL + " WHERE CMDT.CURRENCY_MST_FK = CTM.CURRENCY_MST_PK";
        //    strSQL = strSQL + " AND CMDT.DEPOT_MST_FK=DMT.VENDOR_MST_PK AND CMDT.LAST_MODIFIED_BY_FK = UMT.USER_MST_PK(+)";
        //    if (depot_Pk == "0")
        //    {
        //        strSQL = strSQL + " AND 1=2 ";
        //    }
        //    else
        //    {
        //        strSQL = strSQL + " AND CMDT.CONT_MAIN_DEPOT_PK = " + depot_Pk;
        //    }
        //    try
        //    {
        //        dsHeader = objWF.GetDataSet(strSQL);
        //        return dsHeader;
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

        //#endregion "Container Main Header"

        //#region "FetchLclDetail"

        //public DataSet FetchLclDetail(string depot_PK = "0")
        //{
        //    string str = null;
        //    bool NewRecord = false;
        //    if (depot_PK == "0")
        //        NewRecord = true;
        //    WorkFlow objWF = new WorkFlow();
        //    DataSet dsLCL = new DataSet();
        //    if (NewRecord)
        //    {
        //        str = "SELECT CT.CONT_TRN_DEPOT_PK,";
        //        str += " CT.FROM_DAY,";
        //        str += " CT.TO_DAY, ";
        //        str += " '' LCL_VOLUME,";
        //        str += " '' LCL_WEIGHT,";
        //        str += " CT.LCL_AMOUNT,'' LCL_RATE_PALETTE,CM.VERSION_NO";
        //        str += " FROM CONT_TRN_DEPOT_FCL_LCL CT,";
        //        str += " CONT_MAIN_DEPOT_TBL CM";
        //        str += " WHERE CT.CONT_MAIN_DEPOT_FK = CM.CONT_MAIN_DEPOT_PK";
        //        str += " AND CM.CARGO_TYPE=2";
        //        str += " AND 1 = 2 ";
        //        str += " order by from_day ";
        //    }
        //    else
        //    {
        //        str = "SELECT CT.CONT_TRN_DEPOT_PK,";
        //        str += " CT.FROM_DAY,";
        //        str += " CT.TO_DAY, ";
        //        str += " CT.LCL_VOLUME,";
        //        str += " CT.LCL_WEIGHT,";
        //        str += " CT.LCL_AMOUNT,CT.LCL_RATE_PALETTE,CM.VERSION_NO";
        //        str += " FROM CONT_TRN_DEPOT_FCL_LCL CT,";
        //        str += " CONT_MAIN_DEPOT_TBL CM";
        //        str += " WHERE CT.CONT_MAIN_DEPOT_FK = CM.CONT_MAIN_DEPOT_PK";
        //        str += " AND CM.CARGO_TYPE=2";
        //        str += " AND CM.CONT_MAIN_DEPOT_PK = " + depot_PK;
        //        str += " order by from_day ";
        //    }
        //    try
        //    {
        //        dsLCL = objWF.GetDataSet(str);
        //        return dsLCL;
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

        //#endregion "FetchLclDetail"

        //#region "FetchLclDetailOthChrg"

        //public DataSet FetchLclDetailOthChrg(string depot_PK = "0", int BizType = 2)
        //{
        //    string str = null;
        //    bool NewRecord = false;
        //    if (depot_PK == "0")
        //        NewRecord = true;
        //    WorkFlow objWF = new WorkFlow();
        //    DataSet dsLCL = new DataSet();

        //    if (NewRecord)
        //    {
        //        str = " SELECT '' CONT_DEPOT_OTH_CHG_PK, CC.COST_ELEMENT_MST_PK,";
        //        str += " 'false' AS SEL, ";
        //        str += " CC.COST_ELEMENT_NAME AS \"Other Charges\",";
        //        str += " '' AS LCL_RATE_PER_CBM, ";
        //        str += " '' AS LCL_RATE_PER_TON, ";
        //        str += " '' AS AIR_RATE_PER_100KG, '' AS LCL_RATE_PALETTE,'' AS VERSION_NO";
        //        str += " FROM COST_ELEMENT_MST_TBL CC, VENDOR_TYPE_MST_TBL V ";
        //        str += " WHERE CC.VENDOR_TYPE_MST_FK = V.VENDOR_TYPE_PK  ";
        //        str += " AND CC.BUSINESS_TYPE in (3," + BizType + " ) ";
        //        str += " AND V.VENDOR_TYPE_ID = 'WAREHOUSE' ";
        //    }
        //    else
        //    {
        //        str = "  SELECT CT.CONT_DEPOT_OTH_CHG_PK, CT.COST_ELEMENT_MST_FK, ";
        //        str += " 'TRUE' AS SEL, ";
        //        str += " CC.COST_ELEMENT_NAME AS \"Other Charges\",";
        //        str += " CT.LCL_RATE_PER_CBM, ";
        //        str += " CT.LCL_RATE_PER_TON, ";
        //        str += " CT.AIR_RATE_PER_100KG,  CT.LCL_RATE_PALETTE,";
        //        str += " CM.VERSION_NO ";
        //        str += " FROM CONT_TRN_DEPOT_OTH_CHG CT,CONT_MAIN_DEPOT_TBL  CM,COST_ELEMENT_MST_TBL CC ";
        //        str += " WHERE CT.CONT_MAIN_DEPOT_FK = CM.CONT_MAIN_DEPOT_PK ";
        //        str += " AND CT.COST_ELEMENT_MST_FK(+) = CC.COST_ELEMENT_MST_PK ";
        //        str += " AND CM.CARGO_TYPE = 2";
        //        str += " AND CM.CONT_MAIN_DEPOT_PK = " + depot_PK;
        //        str += " union";
        //        str += " SELECT DISTINCT null CONT_DEPOT_OTH_CHG_PK,";
        //        str += " CC.COST_ELEMENT_MST_PK, ";
        //        str += " 'false' as \"SEL\",";
        //        str += " CC.COST_ELEMENT_NAME AS \"Other Charges\",";
        //        str += " CT.LCL_RATE_PER_CBM,";
        //        str += " CT.LCL_RATE_PER_TON,";
        //        str += " CT.AIR_RATE_PER_100KG,CT.LCL_RATE_PALETTE,";
        //        str += " NULL VERSION_NO";
        //        str += " FROM CONT_TRN_DEPOT_OTH_CHG CT,";
        //        str += " CONT_MAIN_DEPOT_TBL CMDT,";
        //        str += " DEPOT_TRN_OTH_CHG_CONT_DET CONT,";
        //        str += " CONTAINER_TYPE_MST_TBL CMT,";
        //        str += " COST_ELEMENT_MST_TBL CC,";
        //        str += " VENDOR_TYPE_MST_TBL V";
        //        str += " WHERE Cc.VENDOR_TYPE_MST_FK = V.VENDOR_TYPE_PK(+)";
        //        str += " AND Ct.CONT_MAIN_DEPOT_FK = CMDT.CONT_MAIN_DEPOT_PK";
        //        str += " AND Ct.cont_depot_oth_chg_pk = CONT.cont_depot_oth_chg_fk";
        //        str += " AND CONT.CONTAINER_TYPE_MST_FK = CMT.CONTAINER_TYPE_MST_PK";
        //        str += " AND CMDT.CONT_MAIN_DEPOT_PK = CT.CONT_MAIN_DEPOT_FK";
        //        str += " AND CC.COST_ELEMENT_MST_PK NOT IN";
        //        str += " (SELECT CON.COST_ELEMENT_MST_FK";
        //        str += "  FROM CONT_MAIN_DEPOT_TBL CMDT, CONT_TRN_DEPOT_OTH_CHG CON";
        //        str += " WHERE CMDT.CONT_MAIN_DEPOT_PK = CON.CONT_MAIN_DEPOT_FK";
        //        str += " AND CMDT.CONT_MAIN_DEPOT_PK = " + depot_PK + " ) ";
        //        str += " AND CC.BUSINESS_TYPE in (3," + BizType + " ) ";
        //        str += " AND V.VENDOR_TYPE_ID = 'WAREHOUSE'";
        //    }
        //    try
        //    {
        //        dsLCL = objWF.GetDataSet(str);
        //        return dsLCL;
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

        //#endregion "FetchLclDetailOthChrg"

        //#region "FetchActiveCont"

        //public DataTable FetchActiveCont(string strContId = "", string depotMainPK = "", bool NewRecord = false)
        //{
        //    string str = null;
        //    WorkFlow objWF = new WorkFlow();
        //    if (string.IsNullOrEmpty(strContId))
        //    {
        //        strContId = " AND ROWNUM <=10 ";
        //    }
        //    else
        //    {
        //        strContId = " AND CTMT.CONTAINER_TYPE_MST_ID IN (" + strContId + ") ";
        //    }
        //    if (!string.IsNullOrEmpty(depotMainPK) & depotMainPK != "0")
        //    {
        //        strContId += " AND CMDT.CONT_MAIN_DEPOT_PK = " + depotMainPK;
        //    }
        //    str = " SELECT CONTAINER_TYPE_MST_PK,CTMT.CONTAINER_TYPE_MST_ID,CTMT.CONTAINER_TYPE_MST_ID" + " FROM CONTAINER_TYPE_MST_TBL CTMT WHERE CTMT.ACTIVE_FLAG=1" + strContId + " ORDER BY CTMT.PREFERENCES";
        //    try
        //    {
        //        return objWF.GetDataTable(str);
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

        //#endregion "FetchActiveCont"

        //#region "FetchContainerData"

        //public DataTable FetchContainerData(string depotMainPK)
        //{
        //    string str = null;
        //    WorkFlow objWF = new WorkFlow();
        //    DataSet dSMain = new DataSet();
        //    DataTable dtMain = new DataTable();
        //    DataTable dtContainerType = new DataTable();

        //    str = " SELECT";
        //    str += " CT.CONT_TRN_DEPOT_PK,";
        //    str += " CT.FROM_DAY,";
        //    str += " CT.TO_DAY,";
        //    str += " CONT.CONTAINER_TYPE_MST_FK AS \"ContainerPK\",";
        //    str += " CMT.CONTAINER_TYPE_MST_ID AS \"Container Type\",";
        //    str += " CONT.FCL_CURRENT_RATE";
        //    str += " FROM ";
        //    str += " CONT_TRN_DEPOT_FCL_LCL CT ,";
        //    str += " CONT_MAIN_DEPOT_TBL CMDT,";
        //    str += " CONT_DEPOT_FCL_RATE_TRN CONT,";
        //    str += " CONTAINER_TYPE_MST_TBL CMT";
        //    str += " WHERE ";
        //    str += " CONT.CONTAINER_TYPE_MST_FK = cmt.container_type_mst_pk";
        //    str += " AND CMDT.CONT_MAIN_DEPOT_PK=CT.CONT_MAIN_DEPOT_FK";
        //    str += " AND CONT.CONT_TRN_DEPOT_FK=CT.CONT_TRN_DEPOT_PK";
        //    str += " AND CMDT.CONT_MAIN_DEPOT_PK = " + depotMainPK;
        //    str += " ORDER BY CT.CONT_TRN_DEPOT_PK, CMT.PREFERENCES";
        //    try
        //    {
        //        dSMain = objWF.GetDataSet(str);
        //        return TransposeColumns(dSMain);
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

        //#endregion "FetchContainerData"

        //#region "FetchContainerDataOthChrg"

        //public DataTable FetchContainerDataOthChrg(string depotMainPK, string strContId = "")
        //{
        //    DataSet dSMain = new DataSet();
        //    WorkFlow objWF = new WorkFlow();
        //    OracleCommand selectCommand = new OracleCommand();
        //    string strReturn = null;
        //    try
        //    {
        //        objWF.OpenConnection();
        //        selectCommand.Connection = objWF.MyConnection;
        //        selectCommand.CommandType = CommandType.StoredProcedure;
        //        selectCommand.CommandText = objWF.MyUserName + ".CONT_DEPOT_PKG.GET_CONTAINER_DETAILS";

        //        var _with1 = selectCommand.Parameters;
        //        //modified by thiyagarajan on 10/12/08
        //        _with1.Add("DEPOTMAINPK_IN", depotMainPK).Direction = ParameterDirection.Input;
        //        _with1.Add("STRCONTID_IN", getDefault(strContId, "0")).Direction = ParameterDirection.Input;
        //        _with1.Add("RETURN_VALUE", OracleDbType.Clob, 5000, "RETURN_VALUE").Direction = ParameterDirection.Output;
        //        selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
        //        selectCommand.ExecuteNonQuery();
        //        OracleClob clob = null;
        //        clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
        //        System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
        //        strReturn = strReader.ReadToEnd();
        //        dSMain = objWF.GetDataSet(strReturn);
        //        return TransposeColumnsOthchrg(dSMain);
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
        //        objWF.MyCommand.Connection.Close();
        //    }
        //}

        //#endregion "FetchContainerDataOthChrg"

        //#region "TransposeColumns"

        //private DataTable TransposeColumns(DataSet DS)
        //{
        //    try
        //    {
        //        DataTable dt = new DataTable();
        //        var _with2 = dt.Columns;
        //        _with2.Add("CONT_TRN_DEPOT_PK", DS.Tables[0].Columns[0].DataType);
        //        _with2.Add("FROM_DAY", DS.Tables[0].Columns[1].DataType);
        //        _with2.Add("TO_DAY", DS.Tables[0].Columns[2].DataType);
        //        if (DS.Tables[0].Rows.Count > 0)
        //        {
        //            string strFirstColName = DS.Tables[0].Rows[0][4];
        //            Int16 RowCnt = default(Int16);
        //            for (RowCnt = 0; RowCnt <= DS.Tables[0].Rows.Count - 1; RowCnt++)
        //            {
        //                string strCurrentCol = DS.Tables[0].Rows[RowCnt][4];
        //                if (strCurrentCol != strFirstColName | RowCnt == 0)
        //                {
        //                    var _with3 = dt.Columns;
        //                    _with3.Add(DS.Tables[0].Rows[RowCnt][4], typeof(decimal));
        //                }
        //                else
        //                {
        //                    break; // TODO: might not be correct. Was : Exit For
        //                }
        //            }
        //            int iRow = 0;
        //            int iCol = 0;
        //            int iDsRow = 0;
        //            DataRow dr = null;
        //            int NextVal = 0;
        //            int CurrVal = -1;
        //            iDsRow = 0;
        //            iCol = 0;
        //            for (iRow = 0; iRow <= DS.Tables[0].Rows.Count - 1; iRow++)
        //            {
        //                NextVal = DS.Tables[0].Rows[iRow][0];
        //                if (CurrVal != NextVal)
        //                {
        //                    CurrVal = NextVal;
        //                    dr = dt.NewRow();
        //                    for (iCol = 0; iCol <= 2; iCol++)
        //                    {
        //                        dr[iCol] = DS.Tables[0].Rows[iRow][iCol];
        //                    }
        //                    for (iDsRow = iRow; iDsRow <= DS.Tables[0].Rows.Count - 1; iDsRow++)
        //                    {
        //                        NextVal = DS.Tables[0].Rows[iDsRow][0];
        //                        if (CurrVal != NextVal)
        //                            break; // TODO: might not be correct. Was : Exit For
        //                        string colName = DS.Tables[0].Rows[iDsRow][4];
        //                        for (iCol = 3; iCol <= dt.Columns.Count - 1; iCol++)
        //                        {
        //                            string gridColumn = dt.Columns[iCol].ColumnName;
        //                            if (gridColumn == colName)
        //                            {
        //                                dr[iCol] = DS.Tables[0].Rows[iDsRow][5];
        //                                break; // TODO: might not be correct. Was : Exit For
        //                            }
        //                        }
        //                    }
        //                    iRow = iDsRow - 1;
        //                    dt.Rows.Add(dr);
        //                }
        //            }
        //        }
        //        return dt;
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

        //#endregion "TransposeColumns"

        //#region "TransposeColumnsOthchrg"

        //private DataTable TransposeColumnsOthchrg(DataSet DS)
        //{
        //    DataTable dt = new DataTable();
        //    try
        //    {
        //        var _with4 = dt.Columns;
        //        _with4.Add("CONT_DEPOT_OTH_CHG_PK", DS.Tables[0].Columns[0].DataType);
        //        _with4.Add("COST_ELEMENT_MST_FK", DS.Tables[0].Columns[1].DataType);
        //        _with4.Add("SEL", DS.Tables[0].Columns[2].DataType);
        //        _with4.Add("COST_ELEMENT_ID", DS.Tables[0].Columns[3].DataType);

        //        if (DS.Tables[0].Rows.Count > 0)
        //        {
        //            string strFirstColName = DS.Tables[0].Rows[0][5];
        //            Int16 RowCnt = default(Int16);
        //            for (RowCnt = 0; RowCnt <= DS.Tables[0].Rows.Count - 1; RowCnt++)
        //            {
        //                string strCurrentCol = DS.Tables[0].Rows[RowCnt][5];
        //                if (strCurrentCol != strFirstColName | RowCnt == 0)
        //                {
        //                    var _with5 = dt.Columns;
        //                    _with5.Add(DS.Tables[0].Rows[RowCnt][5], typeof(decimal));
        //                }
        //                else
        //                {
        //                    break; // TODO: might not be correct. Was : Exit For
        //                }
        //            }
        //            int iRow = 0;
        //            int iCol = 0;
        //            int iDsRow = 0;
        //            DataRow dr = null;
        //            int NextVal = 0;
        //            int CurrVal = -1;
        //            iDsRow = 0;
        //            iCol = 0;
        //            for (iRow = 0; iRow <= DS.Tables[0].Rows.Count - 1; iRow++)
        //            {
        //                NextVal = DS.Tables[0].Rows[iRow][1];
        //                if (CurrVal != NextVal)
        //                {
        //                    CurrVal = NextVal;
        //                    dr = dt.NewRow();
        //                    for (iCol = 0; iCol <= 3; iCol++)
        //                    {
        //                        dr[iCol] = DS.Tables[0].Rows[iRow][iCol];
        //                    }
        //                    for (iDsRow = iRow; iDsRow <= DS.Tables[0].Rows.Count - 1; iDsRow++)
        //                    {
        //                        NextVal = DS.Tables[0].Rows[iDsRow][1];
        //                        if (CurrVal != NextVal)
        //                            break; // TODO: might not be correct. Was : Exit For
        //                        string colName = DS.Tables[0].Rows[iDsRow][5];
        //                        for (iCol = 4; iCol <= dt.Columns.Count - 1; iCol++)
        //                        {
        //                            string gridColumn = dt.Columns[iCol].ColumnName;
        //                            if (gridColumn == colName)
        //                            {
        //                                dr[iCol] = getDefault(DS.Tables[0].Rows[iDsRow][6], "");
        //                                break; // TODO: might not be correct. Was : Exit For
        //                            }
        //                        }
        //                    }
        //                    iRow = iDsRow - 1;
        //                    dt.Rows.Add(dr);
        //                }
        //            }
        //        }
        //        return dt;
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

        //#endregion "TransposeColumnsOthchrg"

        //#region " Contract Ref No "

        //public string GenerateContractNo(long nLocationId, int bizType, long nEmployeeId, long nCreatedBy, WorkFlow objWK)
        //{
        //    string functionReturnValue = null;
        //    try
        //    {
        //        string sbizType = "";
        //        if (bizType == 1)
        //        {
        //            sbizType = "AIR DEPOT CONTRACT";
        //        }
        //        else if (bizType == 2)
        //        {
        //            sbizType = "SEA DEPOT CONTRACT";
        //        }
        //        functionReturnValue = GenerateProtocolKey(sbizType, nLocationId, nEmployeeId, DateTime.Now, , , , nCreatedBy, objWK);
        //        return functionReturnValue;
        //    }
        //    catch (OracleException oraexp)
        //    {
        //        throw oraexp;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //    return functionReturnValue;
        //}

        //#endregion " Contract Ref No "

        //#region "Save Function"

        //public ArrayList Save(DataSet dsMain, DataSet dsChildData, DataSet dsChildDataOth, bool isEdting, object contMainDepotPK, string userLocation, object txtContractNo, int employeeID, Int16 biztype, string strContainerId)
        //{
        //    WorkFlow objWK = new WorkFlow();
        //    objWK.OpenConnection();
        //    OracleTransaction TRAN = null;
        //    TRAN = objWK.MyConnection.BeginTransaction();
        //    objWK.MyCommand.Transaction = TRAN;
        //    string ContractRefNo = null;

        //    OracleParameterCollection ColPara = new OracleParameterCollection();

        //    Int32 RecAfct = default(Int32);
        //    OracleCommand insCommand = new OracleCommand();
        //    OracleCommand updCommand = new OracleCommand();

        //    OracleCommand insCommandChlid = new OracleCommand();
        //    OracleCommand updCommandChild = new OracleCommand();

        //    OracleCommand insCommandChlidOth = new OracleCommand();
        //    OracleCommand updCommandChildOth = new OracleCommand();

        //    if (isEdting == false)
        //    {
        //        if (string.IsNullOrEmpty(txtContractNo.text))
        //        {
        //            ContractRefNo = GenerateContractNo(userLocation, biztype, employeeID, M_CREATED_BY_FK, objWK);
        //            if (ContractRefNo == "Protocol Not Defined.")
        //            {
        //                arrMessage.Add("Protocol Not Defined.");
        //                return arrMessage;
        //            }
        //        }
        //        else
        //        {
        //            ContractRefNo = txtContractNo.text;
        //        }
        //    }
        //    else
        //    {
        //        ContractRefNo = txtContractNo.text;
        //    }

        //    arrMessage.Clear();

        //    try
        //    {
        //        DataTable DtTbl = new DataTable();

        //        var _with6 = insCommand;
        //        _with6.Connection = objWK.MyConnection;
        //        _with6.CommandType = CommandType.StoredProcedure;
        //        _with6.CommandText = objWK.MyUserName + ".CONT_DEPOT_PKG.CONT_MAIN_DEPOT_TBL_INS";
        //        var _with7 = _with6.Parameters;
        //        insCommand.Parameters.Add("DEPOT_MST_FK_IN", OracleDbType.Int32, 10, "DEPOT_MST_FK").Direction = ParameterDirection.Input;
        //        insCommand.Parameters["DEPOT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

        //        insCommand.Parameters.Add("CONTRACT_NO_IN", ContractRefNo).Direction = ParameterDirection.Input;

        //        insCommand.Parameters.Add("CONTAINER_ID_IN", (string.IsNullOrEmpty(strContainerId) ? "LCL" : strContainerId)).Direction = ParameterDirection.Input;
        //        insCommand.Parameters.Add("CONTRACT_DATE_IN", OracleDbType.Varchar2, 20, "CONTRACT_DATE").Direction = ParameterDirection.Input;
        //        insCommand.Parameters["CONTRACT_DATE_IN"].SourceVersion = DataRowVersion.Current;

        //        insCommand.Parameters.Add("CONT_APPROVED_IN", OracleDbType.Int32, 1, "CONT_APPROVED").Direction = ParameterDirection.Input;
        //        insCommand.Parameters["CONT_APPROVED_IN"].SourceVersion = DataRowVersion.Current;

        //        insCommand.Parameters.Add("VALID_FROM_IN", OracleDbType.Varchar2, 20, "VALID_FROM").Direction = ParameterDirection.Input;
        //        insCommand.Parameters["VALID_FROM_IN"].SourceVersion = DataRowVersion.Current;

        //        insCommand.Parameters.Add("VALID_TO_IN", OracleDbType.Varchar2, 20, "VALID_TO").Direction = ParameterDirection.Input;
        //        insCommand.Parameters["VALID_TO_IN"].SourceVersion = DataRowVersion.Current;

        //        insCommand.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_PK").Direction = ParameterDirection.Input;
        //        insCommand.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

        //        insCommand.Parameters.Add("BILLING_CYCLE_DAYS_IN", OracleDbType.Int32, 2, "BILLING_CYCLE_DAYS").Direction = ParameterDirection.Input;
        //        insCommand.Parameters["BILLING_CYCLE_DAYS_IN"].SourceVersion = DataRowVersion.Current;

        //        insCommand.Parameters.Add("CARGO_TYPE_IN", OracleDbType.Int32, 1, "CARGO_TYPE").Direction = ParameterDirection.Input;
        //        insCommand.Parameters["CARGO_TYPE_IN"].SourceVersion = DataRowVersion.Current;

        //        insCommand.Parameters.Add("BUSINESS_TYPE_IN", OracleDbType.Int32, 1, "BUSINESS_TYPE").Direction = ParameterDirection.Input;
        //        insCommand.Parameters["BUSINESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

        //        insCommand.Parameters.Add("ACTIVE_IN", OracleDbType.Int32, 1, "ACTIVE").Direction = ParameterDirection.Input;
        //        insCommand.Parameters["ACTIVE_IN"].SourceVersion = DataRowVersion.Current;
        //        insCommand.Parameters.Add("WORKFLOW_STATUS_IN", OracleDbType.Varchar2, 1, "WORKFLOW_STATUS").Direction = ParameterDirection.Input;
        //        insCommand.Parameters["WORKFLOW_STATUS_IN"].SourceVersion = DataRowVersion.Current;

        //        insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
        //        insCommand.Parameters.Add("CONFIG_PK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;

        //        insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.NVarchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;

        //        //  Else
        //        var _with8 = updCommand;
        //        _with8.Connection = objWK.MyConnection;
        //        _with8.CommandType = CommandType.StoredProcedure;
        //        _with8.CommandText = objWK.MyUserName + ".CONT_DEPOT_PKG.CONT_MAIN_DEPOT_TBL_UPD";
        //        var _with9 = _with8.Parameters;

        //        updCommand.Parameters.Add("CONT_MAIN_DEPOT_PK_IN", OracleDbType.Int32, 10, "CONT_MAIN_DEPOT_PK").Direction = ParameterDirection.Input;
        //        updCommand.Parameters["CONT_MAIN_DEPOT_PK_IN"].SourceVersion = DataRowVersion.Current;

        //        updCommand.Parameters.Add("DEPOT_MST_FK_IN", OracleDbType.Int32, 10, "DEPOT_MST_FK").Direction = ParameterDirection.Input;
        //        updCommand.Parameters["DEPOT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

        //        updCommand.Parameters.Add("CONTRACT_NO_IN", Convert.ToString(txtContractNo.text)).Direction = ParameterDirection.Input;
        //        updCommand.Parameters.Add("CONTAINER_ID_IN", (string.IsNullOrEmpty(strContainerId) ? "LCL" : strContainerId)).Direction = ParameterDirection.Input;

        //        updCommand.Parameters.Add("CONTRACT_DATE_IN", OracleDbType.Varchar2, 20, "CONTRACT_DATE").Direction = ParameterDirection.Input;
        //        updCommand.Parameters["CONTRACT_DATE_IN"].SourceVersion = DataRowVersion.Current;

        //        updCommand.Parameters.Add("CONT_APPROVED_IN", OracleDbType.Int32, 1, "CONT_APPROVED").Direction = ParameterDirection.Input;
        //        updCommand.Parameters["CONT_APPROVED_IN"].SourceVersion = DataRowVersion.Current;

        //        updCommand.Parameters.Add("VALID_FROM_IN", OracleDbType.Varchar2, 20, "VALID_FROM").Direction = ParameterDirection.Input;
        //        updCommand.Parameters["VALID_FROM_IN"].SourceVersion = DataRowVersion.Current;

        //        updCommand.Parameters.Add("VALID_TO_IN", OracleDbType.Varchar2, 20, "VALID_TO").Direction = ParameterDirection.Input;
        //        updCommand.Parameters["VALID_TO_IN"].SourceVersion = DataRowVersion.Current;

        //        updCommand.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_PK").Direction = ParameterDirection.Input;
        //        updCommand.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

        //        updCommand.Parameters.Add("WORKFLOW_STATUS_IN", OracleDbType.Varchar2, 1, "WORKFLOW_STATUS").Direction = ParameterDirection.Input;
        //        updCommand.Parameters["WORKFLOW_STATUS_IN"].SourceVersion = DataRowVersion.Current;

        //        updCommand.Parameters.Add("BILLING_CYCLE_DAYS_IN", OracleDbType.Int32, 2, "BILLING_CYCLE_DAYS").Direction = ParameterDirection.Input;
        //        updCommand.Parameters["BILLING_CYCLE_DAYS_IN"].SourceVersion = DataRowVersion.Current;

        //        updCommand.Parameters.Add("CARGO_TYPE_IN", OracleDbType.Int32, 1, "CARGO_TYPE").Direction = ParameterDirection.Input;
        //        updCommand.Parameters["CARGO_TYPE_IN"].SourceVersion = DataRowVersion.Current;

        //        updCommand.Parameters.Add("BUSINESS_TYPE_IN", OracleDbType.Int32, 1, "BUSINESS_TYPE").Direction = ParameterDirection.Input;
        //        updCommand.Parameters["BUSINESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

        //        updCommand.Parameters.Add("ACTIVE_IN", OracleDbType.Int32, 1, "ACTIVE").Direction = ParameterDirection.Input;
        //        updCommand.Parameters["ACTIVE_IN"].SourceVersion = DataRowVersion.Current;

        //        updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
        //        updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

        //        updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

        //        updCommand.Parameters.Add("CONFIG_PK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;

        //        updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.NVarchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;

        //        //  End If

        //        objWK.MyDataAdapter.RowUpdated += new OracleRowUpdatedEventHandler(OnRowUpdated);

        //        var _with10 = objWK.MyDataAdapter;

        //        _with10.InsertCommand = insCommand;
        //        _with10.InsertCommand.Transaction = TRAN;

        //        _with10.UpdateCommand = updCommand;
        //        _with10.UpdateCommand.Transaction = TRAN;

        //        RecAfct = _with10.Update(dsMain);

        //        if (arrMessage.Count > 0)
        //        {
        //            if (isEdting == false)
        //            {
        //                if (biztype == 1)
        //                {
        //                    RollbackProtocolKey("AIR DEPOT CONTRACT", HttpContext.Current.Session["LOGED_IN_LOC_FK"], HttpContext.Current.Session["EMP_PK"], ContractRefNo, System.DateTime.Now);
        //                    //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
        //                }
        //                else
        //                {
        //                    RollbackProtocolKey("SEA DEPOT CONTRACT", HttpContext.Current.Session["LOGED_IN_LOC_FK"], HttpContext.Current.Session["EMP_PK"], ContractRefNo, System.DateTime.Now);
        //                }
        //            }
        //            TRAN.Rollback();
        //            return arrMessage;
        //        }
        //        else
        //        {
        //            if (isEdting == false)
        //            {
        //                contMainDepotPK = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
        //            }
        //            else
        //            {
        //                contMainDepotPK = Convert.ToInt32(updCommand.Parameters["RETURN_VALUE"].Value);
        //                DeleteOthChrg(contMainDepotPK);
        //            }
        //        }

        //        var _with11 = insCommandChlid;
        //        _with11.Connection = objWK.MyConnection;
        //        _with11.CommandType = CommandType.StoredProcedure;
        //        _with11.CommandText = objWK.MyUserName + ".CONT_DEPOT_PKG.CONT_TRN_DEPOT_FCL_LCL_INS";
        //        var _with12 = _with11.Parameters;

        //        insCommandChlid.Parameters.Add("CONT_MAIN_DEPOT_FK_IN", Convert.ToInt64(contMainDepotPK)).Direction = ParameterDirection.Input;
        //        insCommandChlid.Parameters.Add("FROM_DAY_IN", OracleDbType.Int32, 3, "FROM_DAY").Direction = ParameterDirection.Input;
        //        insCommandChlid.Parameters["FROM_DAY_IN"].SourceVersion = DataRowVersion.Current;

        //        insCommandChlid.Parameters.Add("TO_DAY_IN", OracleDbType.Int32, 3, "TO_DAY").Direction = ParameterDirection.Input;
        //        insCommandChlid.Parameters["TO_DAY_IN"].SourceVersion = DataRowVersion.Current;
        //        //
        //        insCommandChlid.Parameters.Add("CONTAINER_DTL_FCL_IN", OracleDbType.Varchar2, 300, "CONTAINER_DTL_FCL").Direction = ParameterDirection.Input;
        //        insCommandChlid.Parameters["CONTAINER_DTL_FCL_IN"].SourceVersion = DataRowVersion.Current;

        //        insCommandChlid.Parameters.Add("LCL_VOLUME_IN", OracleDbType.Int32, 10, "LCL_VOLUME").Direction = ParameterDirection.Input;
        //        insCommandChlid.Parameters["LCL_VOLUME_IN"].SourceVersion = DataRowVersion.Current;

        //        insCommandChlid.Parameters.Add("LCL_WEIGHT_IN", OracleDbType.Int32, 10, "LCL_WEIGHT").Direction = ParameterDirection.Input;
        //        insCommandChlid.Parameters["LCL_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

        //        insCommandChlid.Parameters.Add("LCL_AMOUNT_IN", OracleDbType.Int32, 10, "LCL_AMOUNT").Direction = ParameterDirection.Input;
        //        insCommandChlid.Parameters["LCL_AMOUNT_IN"].SourceVersion = DataRowVersion.Current;

        //        insCommandChlid.Parameters.Add("LCL_RATE_PALETTE_IN", OracleDbType.Int32, 10, "LCL_RATE_PALETTE").Direction = ParameterDirection.Input;
        //        insCommandChlid.Parameters["LCL_RATE_PALETTE_IN"].SourceVersion = DataRowVersion.Current;

        //        insCommandChlid.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
        //        insCommandChlid.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
        //        //************************************************************************

        //        var _with13 = updCommandChild;
        //        _with13.Connection = objWK.MyConnection;
        //        _with13.CommandType = CommandType.StoredProcedure;
        //        _with13.CommandText = objWK.MyUserName + ".CONT_DEPOT_PKG.CONT_TRN_DEPOT_FCL_LCL_UPD";

        //        var _with14 = _with13.Parameters;

        //        updCommandChild.Parameters.Add("CONT_TRN_DEPOT_PK_IN", OracleDbType.Int32, 10, "CONT_TRN_DEPOT_PK").Direction = ParameterDirection.Input;
        //        updCommandChild.Parameters["CONT_TRN_DEPOT_PK_IN"].SourceVersion = DataRowVersion.Current;

        //        updCommandChild.Parameters.Add("FROM_DAY_IN", OracleDbType.Int32, 3, "FROM_DAY").Direction = ParameterDirection.Input;
        //        updCommandChild.Parameters["FROM_DAY_IN"].SourceVersion = DataRowVersion.Current;

        //        updCommandChild.Parameters.Add("TO_DAY_IN", OracleDbType.Int32, 3, "TO_DAY").Direction = ParameterDirection.Input;
        //        updCommandChild.Parameters["TO_DAY_IN"].SourceVersion = DataRowVersion.Current;
        //        //
        //        updCommandChild.Parameters.Add("CONTAINER_DTL_FCL_IN", OracleDbType.Varchar2, 300, "CONTAINER_DTL_FCL").Direction = ParameterDirection.Input;
        //        updCommandChild.Parameters["CONTAINER_DTL_FCL_IN"].SourceVersion = DataRowVersion.Current;

        //        updCommandChild.Parameters.Add("LCL_VOLUME_IN", OracleDbType.Int32, 10, "LCL_VOLUME").Direction = ParameterDirection.Input;
        //        updCommandChild.Parameters["LCL_VOLUME_IN"].SourceVersion = DataRowVersion.Current;

        //        updCommandChild.Parameters.Add("LCL_WEIGHT_IN", OracleDbType.Int32, 10, "LCL_WEIGHT").Direction = ParameterDirection.Input;
        //        updCommandChild.Parameters["LCL_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

        //        updCommandChild.Parameters.Add("LCL_AMOUNT_IN", OracleDbType.Int32, 10, "LCL_AMOUNT").Direction = ParameterDirection.Input;
        //        updCommandChild.Parameters["LCL_AMOUNT_IN"].SourceVersion = DataRowVersion.Current;

        //        updCommandChild.Parameters.Add("LCL_RATE_PALETTE_IN", OracleDbType.Int32, 10, "LCL_RATE_PALETTE").Direction = ParameterDirection.Input;
        //        updCommandChild.Parameters["LCL_RATE_PALETTE_IN"].SourceVersion = DataRowVersion.Current;

        //        updCommandChild.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
        //        updCommandChild.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

        //        var _with15 = objWK.MyDataAdapter;
        //        _with15.InsertCommand = insCommandChlid;
        //        _with15.InsertCommand.Transaction = TRAN;

        //        _with15.UpdateCommand = updCommandChild;
        //        _with15.UpdateCommand.Transaction = TRAN;

        //        RecAfct = _with15.Update(dsChildData.Tables[0]);

        //        if (arrMessage.Count > 0)
        //        {
        //            if (isEdting == false)
        //            {
        //                if (biztype == 1)
        //                {
        //                    RollbackProtocolKey("AIR DEPOT CONTRACT", HttpContext.Current.Session["LOGED_IN_LOC_FK"], HttpContext.Current.Session["EMP_PK"], ContractRefNo, System.DateTime.Now);
        //                    //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
        //                }
        //                else
        //                {
        //                    RollbackProtocolKey("SEA DEPOT CONTRACT", HttpContext.Current.Session["LOGED_IN_LOC_FK"], HttpContext.Current.Session["EMP_PK"], ContractRefNo, System.DateTime.Now);
        //                }
        //            }
        //            TRAN.Rollback();
        //            return arrMessage;
        //        }
        //        var _with16 = insCommandChlidOth;
        //        _with16.Connection = objWK.MyConnection;
        //        _with16.CommandType = CommandType.StoredProcedure;
        //        _with16.CommandText = objWK.MyUserName + ".CONT_DEPOT_PKG.CONT_TRN_DEPOT_OTH_CHG_INS";
        //        var _with17 = _with16.Parameters;

        //        insCommandChlidOth.Parameters.Add("CONT_MAIN_DEPOT_FK_IN", Convert.ToInt64(contMainDepotPK)).Direction = ParameterDirection.Input;

        //        insCommandChlidOth.Parameters.Add("COST_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "COST_ELEMENT_MST_FK").Direction = ParameterDirection.Input;
        //        insCommandChlidOth.Parameters["COST_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

        //        insCommandChlidOth.Parameters.Add("CONTAINER_DTL_FCL_IN", OracleDbType.Varchar2, 300, "CONTAINER_DTL_FCL").Direction = ParameterDirection.Input;
        //        insCommandChlidOth.Parameters["CONTAINER_DTL_FCL_IN"].SourceVersion = DataRowVersion.Current;

        //        insCommandChlidOth.Parameters.Add("LCL_VOLUME_IN", OracleDbType.Int32, 10, "LCL_VOLUME").Direction = ParameterDirection.Input;
        //        insCommandChlidOth.Parameters["LCL_VOLUME_IN"].SourceVersion = DataRowVersion.Current;

        //        insCommandChlidOth.Parameters.Add("LCL_WEIGHT_IN", OracleDbType.Int32, 10, "LCL_WEIGHT").Direction = ParameterDirection.Input;
        //        insCommandChlidOth.Parameters["LCL_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

        //        insCommandChlidOth.Parameters.Add("LCL_AMOUNT_IN", OracleDbType.Int32, 10, "LCL_AMOUNT").Direction = ParameterDirection.Input;
        //        insCommandChlidOth.Parameters["LCL_AMOUNT_IN"].SourceVersion = DataRowVersion.Current;

        //        insCommandChlidOth.Parameters.Add("LCL_RATE_PALETTE_IN", OracleDbType.Int32, 10, "LCL_RATE_PALETTE").Direction = ParameterDirection.Input;
        //        insCommandChlidOth.Parameters["LCL_RATE_PALETTE_IN"].SourceVersion = DataRowVersion.Current;

        //        insCommandChlidOth.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
        //        insCommandChlidOth.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
        //        //************************************************************************

        //        objWK.MyDataAdapter.RowUpdated += new OracleRowUpdatedEventHandler(OnRowUpdated);

        //        var _with18 = objWK.MyDataAdapter;
        //        _with18.InsertCommand = insCommandChlidOth;
        //        _with18.InsertCommand.Transaction = TRAN;

        //        if (dsChildDataOth.Tables.Count > 0)
        //        {
        //            RecAfct = _with18.Update(dsChildDataOth.Tables[0]);
        //        }

        //        if (arrMessage.Count > 0)
        //        {
        //            if (isEdting == false)
        //            {
        //                if (biztype == 1)
        //                {
        //                    RollbackProtocolKey("AIR DEPOT CONTRACT", HttpContext.Current.Session["LOGED_IN_LOC_FK"], HttpContext.Current.Session["EMP_PK"], ContractRefNo, System.DateTime.Now);
        //                    //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
        //                }
        //                else
        //                {
        //                    RollbackProtocolKey("SEA DEPOT CONTRACT", HttpContext.Current.Session["LOGED_IN_LOC_FK"], HttpContext.Current.Session["EMP_PK"], ContractRefNo, System.DateTime.Now);
        //                }
        //            }
        //            TRAN.Rollback();
        //            return arrMessage;
        //        }
        //        else
        //        {
        //            TRAN.Commit();
        //            txtContractNo.text = ContractRefNo;
        //            arrMessage.Add("All Data Saved Successfully");
        //            return arrMessage;
        //        }
        //    }
        //    catch (OracleException oraexp)
        //    {
        //        if (isEdting == false)
        //        {
        //            if (biztype == 1)
        //            {
        //                RollbackProtocolKey("AIR DEPOT CONTRACT", HttpContext.Current.Session["LOGED_IN_LOC_FK"], HttpContext.Current.Session["EMP_PK"], ContractRefNo, System.DateTime.Now);
        //                //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
        //            }
        //            else
        //            {
        //                RollbackProtocolKey("SEA DEPOT CONTRACT", HttpContext.Current.Session["LOGED_IN_LOC_FK"], HttpContext.Current.Session["EMP_PK"], ContractRefNo, System.DateTime.Now);
        //            }
        //        }
        //        throw oraexp;
        //    }
        //    catch (Exception ex)
        //    {
        //        if (isEdting == false)
        //        {
        //            if (biztype == 1)
        //            {
        //                RollbackProtocolKey("AIR DEPOT CONTRACT", HttpContext.Current.Session["LOGED_IN_LOC_FK"], HttpContext.Current.Session["EMP_PK"], ContractRefNo, System.DateTime.Now);
        //                //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
        //            }
        //            else
        //            {
        //                RollbackProtocolKey("SEA DEPOT CONTRACT", HttpContext.Current.Session["LOGED_IN_LOC_FK"], HttpContext.Current.Session["EMP_PK"], ContractRefNo, System.DateTime.Now);
        //            }
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

        //public ArrayList DeleteOthChrg(long contMainDepotPK)
        //{
        //    try
        //    {
        //        string Strsql = null;
        //        WorkFlow Objwk = new WorkFlow();
        //        System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
        //        sb.Append("DELETE FROM DEPOT_TRN_OTH_CHG_CONT_DET DET");
        //        sb.Append(" WHERE DET.CONT_DEPOT_OTH_CHG_FK IN");
        //        sb.Append("       (SELECT TRN.CONT_DEPOT_OTH_CHG_PK");
        //        sb.Append("          FROM CONT_TRN_DEPOT_OTH_CHG TRN");
        //        sb.Append("         WHERE TRN.CONT_MAIN_DEPOT_FK =" + contMainDepotPK + ")");

        //        Objwk.ExecuteCommands(sb.ToString());
        //        Strsql = "DELETE FROM CONT_TRN_DEPOT_OTH_CHG CHRG WHERE CHRG.CONT_MAIN_DEPOT_FK = " + contMainDepotPK;
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

        //    objWK.OpenConnection();
        //    OracleTransaction TRAN = null;
        //    Int32 RecAfct = default(Int32);
        //    OracleCommand DeleteOther = new OracleCommand();
        //    Int32 inti = default(Int32);
        //    TRAN = objWK.MyConnection.BeginTransaction();
        //    arrMessage.Clear();

        //    try
        //    {
        //        var _with19 = DeleteOther;
        //        _with19.Parameters.Clear();
        //        _with19.Connection = objWK.MyConnection;
        //        _with19.CommandType = CommandType.StoredProcedure;
        //        _with19.CommandText = objWK.MyUserName + ".CONT_DEPOT_PKG.CONT_TRN_DEPOT_OTH_CHG_DEL";
        //        var _with20 = _with19.Parameters;
        //        _with20.Add("CONT_DEPOT_OTH_CHG_PK_IN", contMainDepotPK).Direction = ParameterDirection.Input;
        //        _with20.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
        //        var _with21 = objWK.MyDataAdapter;

        //        _with21.DeleteCommand = DeleteOther;
        //        _with21.DeleteCommand.Transaction = TRAN;
        //        _with21.DeleteCommand.ExecuteNonQuery();

        //        if (arrMessage.Count > 0)
        //        {
        //            TRAN.Rollback();
        //            return arrMessage;
        //        }
        //        if (arrMessage.Count == 0)
        //        {
        //            TRAN.Commit();
        //            //arrMessage.Add("All Data Deleted Successfully")
        //            //Return arrMessage
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

        //#endregion "Delete Other Chrge"

        //#region "File If Exist"

        //public bool CheckFileExistence(long DepotPk, string strFileName)
        //{
        //    WorkFlow objwk = new WorkFlow();
        //    bool Chk = false;
        //    string RemQuery = "";
        //    RemQuery = " SELECT COUNT(*) FROM CONT_MAIN_DEPOT_TBL CMDT ";
        //    RemQuery += " WHERE CMDT.CONT_MAIN_DEPOT_PK = " + DepotPk + " AND CMDT.ATTACHED_FILE_NAME='" + strFileName + "'";
        //    try
        //    {
        //        objwk.OpenConnection();
        //        if (Convert.ToInt32(objwk.ExecuteScaler(RemQuery)) > 0)
        //        {
        //            Chk = true;
        //        }
        //    }
        //    catch (OracleException OraExp)
        //    {
        //        throw OraExp;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //        return false;
        //    }
        //    finally
        //    {
        //        objwk.MyCommand.Connection.Close();
        //    }
        //    return Chk;
        //}

        //#endregion "File If Exist"

        //#region "Update File Name"

        //public bool UpdateFileName(long DepotPk, string strFileName, Int16 Flag)
        //{
        //    if (strFileName.Trim().Length > 0)
        //    {
        //        string RemQuery = "";
        //        WorkFlow objwk = new WorkFlow();
        //        if (Flag == 1)
        //        {
        //            RemQuery = " UPDATE CONT_MAIN_DEPOT_TBL CMDT SET CMDT.ATTACHED_FILE_NAME='" + strFileName + "'";
        //            RemQuery += " WHERE CMDT.CONT_MAIN_DEPOT_PK = " + DepotPk;
        //        }
        //        else if (Flag == 2)
        //        {
        //            RemQuery = " UPDATE CONT_MAIN_DEPOT_TBL CMDT SET CMDT.ATTACHED_FILE_NAME='" + "" + "'";
        //            RemQuery += " WHERE CMDT.CONT_MAIN_DEPOT_PK = " + DepotPk;
        //        }
        //        try
        //        {
        //            objwk.OpenConnection();
        //            objwk.ExecuteCommands(RemQuery);
        //            return true;
        //            return true;
        //        }
        //        catch (OracleException OraExp)
        //        {
        //            throw OraExp;
        //        }
        //        catch (Exception ex)
        //        {
        //            throw ex;
        //            return false;
        //        }
        //        finally
        //        {
        //            objwk.MyCommand.Connection.Close();
        //        }
        //    }
        //}

        //#endregion "Update File Name"

        //#region "FetchFileName"

        //public string FetchFileName(long DepotPk)
        //{
        //    string strSQL = null;
        //    string strUpdFileName = null;
        //    WorkFlow objWF = new WorkFlow();
        //    strSQL = " SELECT ";
        //    strSQL += " CMDT.ATTACHED_FILE_NAME FROM CONT_MAIN_DEPOT_TBL CMDT WHERE CMDT.CONT_MAIN_DEPOT_PK = " + DepotPk;
        //    try
        //    {
        //        DataSet ds = new DataSet();
        //        strUpdFileName = objWF.ExecuteScaler(strSQL);
        //        return strUpdFileName;
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

        //#endregion "FetchFileName"

        //#region "getBasecurrency"

        //public string getBasecurrency(object ID)
        //{
        //    string strSql = null;
        //    DataSet ds = null;
        //    try
        //    {
        //        strSql = "SELECT C.CURRENCY_MST_PK, C.CURRENCY_ID, C.CURRENCY_NAME " + " FROM CURRENCY_TYPE_MST_TBL C, CORPORATE_MST_TBL CO " + " WHERE CO.CURRENCY_MST_FK = C.CURRENCY_MST_PK ";
        //        ds = (new WorkFlow()).GetDataSet(strSql);
        //        if (ds.Tables[0].Rows.Count > 0)
        //        {
        //            ID.text = ds.Tables[0].Rows[0][1];
        //            return ds.Tables[0].Rows[0][0];
        //        }
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

        //#endregion "getBasecurrency"

        //#region "Fetch Report Detail"

        //public DataSet FetchReportDetail(int DepotContMainPK)
        //{
        //    try
        //    {
        //        System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
        //        WorkFlow objWK = new WorkFlow();
        //        sb.Append("SELECT VMT.VENDOR_NAME,");
        //        sb.Append("       VTM.VENDOR_TYPE_ID,");
        //        sb.Append("       VCD.ADM_ADDRESS_1,");
        //        sb.Append("       VCD.ADM_ADDRESS_2,");
        //        sb.Append("       VCD.ADM_ADDRESS_3,");
        //        sb.Append("       VCD.ADM_ZIP_CODE,");
        //        sb.Append("       VCD.ADM_PHONE,");
        //        sb.Append("       VCD.ADM_CITY,");
        //        sb.Append("       DECODE(CMDT.BUSINESS_TYPE, 1, 'AIR',2,'SEA') BIZ_TYPE,");
        //        sb.Append("        DECODE(CMDT.CARGO_TYPE, 1, 'FCL', 2, 'LCL') CARGO_TYPE,");
        //        sb.Append("       DECODE(CMDT.CONT_APPROVED, 0, 'REQUESTED', 1, 'APPROVED',2,'REJECTED') CONT_APPROVED,");
        //        sb.Append("       CASE WHEN LUMT.USER_NAME IS NULL THEN");
        //        sb.Append("         CUMT.USER_NAME");
        //        sb.Append("       ELSE");
        //        sb.Append("         LUMT.USER_NAME");
        //        sb.Append("       END USER_ID,");
        //        sb.Append("       CMDT.CONTRACT_NO,");
        //        sb.Append("       CMDT.CONTRACT_DATE,");
        //        sb.Append("       CMDT.VALID_FROM,");
        //        sb.Append("       CMDT.VALID_TO,");
        //        sb.Append("       CMDT.BILLING_CYCLE_DAYS   ");
        //        sb.Append("                   ");
        //        sb.Append("   FROM CONT_MAIN_DEPOT_TBL CMDT,");
        //        sb.Append("        VENDOR_MST_TBL       VMT,");
        //        sb.Append("        VENDOR_TYPE_MST_TBL   VTM,");
        //        sb.Append("        VENDOR_CONTACT_DTLS    VCD,");
        //        sb.Append("        VENDOR_SERVICES_TRN     VST,");
        //        sb.Append("        USER_MST_TBL            CUMT,");
        //        sb.Append("        USER_MST_TBL            LUMT");
        //        sb.Append("                ");
        //        sb.Append(" WHERE CMDT.CONT_MAIN_DEPOT_PK =" + DepotContMainPK);
        //        sb.Append("   AND CMDT.DEPOT_MST_FK= VMT.VENDOR_MST_PK");
        //        sb.Append("   and VST.VENDOR_MST_FK = VMT.VENDOR_MST_PK ");
        //        sb.Append("   AND VST.VENDOR_TYPE_FK = VTM.VENDOR_TYPE_PK ");
        //        sb.Append("   AND VCD.VENDOR_MST_FK = VMT.VENDOR_MST_PK");
        //        sb.Append("   AND CMDT.CREATED_BY_FK = CUMT.USER_MST_PK(+)");
        //        sb.Append("   AND CMDT.LAST_MODIFIED_BY_FK = LUMT.USER_MST_PK(+)");
        //        sb.Append("   AND VTM.VENDOR_TYPE_ID='WAREHOUSE'");
        //        sb.Append("  ");
        //        return objWK.GetDataSet(sb.ToString());
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

        //#endregion "Fetch Report Detail"

        //#region "Fetch Freight Details"

        //public DataSet FetchFreightDetail(int DepotContMainPK)
        //{
        //    try
        //    {
        //        System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
        //        WorkFlow objWK = new WorkFlow();
        //        sb.Append("SELECT CTDF.FROM_DAY,");
        //        sb.Append("       CTDF.TO_DAY,");
        //        sb.Append("       CTMT.CONTAINER_TYPE_MST_ID,");
        //        sb.Append("       CDFR.FCL_CURRENT_RATE,");
        //        sb.Append("       CTMT.PREFERENCES");
        //        sb.Append("   FROM CONT_MAIN_DEPOT_TBL CMDT,");
        //        sb.Append("       CONT_TRN_DEPOT_FCL_LCL CTDF,");
        //        sb.Append("       CONT_DEPOT_FCL_RATE_TRN CDFR,");
        //        sb.Append("       CONTAINER_TYPE_MST_TBL CTMT");
        //        sb.Append(" WHERE CMDT.CONT_MAIN_DEPOT_PK =" + DepotContMainPK);
        //        sb.Append("   AND CMDT.CONT_MAIN_DEPOT_PK = CTDF.CONT_MAIN_DEPOT_FK");
        //        sb.Append("   AND CTDF.CONT_TRN_DEPOT_PK = CDFR.CONT_TRN_DEPOT_FK");
        //        sb.Append("   AND CTMT.CONTAINER_TYPE_MST_PK = CDFR.CONTAINER_TYPE_MST_FK");
        //        sb.Append("   ORDER BY CTDF.CONT_TRN_DEPOT_PK,CTMT.PREFERENCES");
        //        return objWK.GetDataSet(sb.ToString());
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

        //#endregion "Fetch Freight Details"

        //#region "Fetch Othercharges detail"

        //public DataSet FetchOtherchargeDetails(int DepotContMainPK)
        //{
        //    try
        //    {
        //        System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
        //        WorkFlow objWK = new WorkFlow();
        //        sb.Append("SELECT CEMT.COST_ELEMENT_ID,");
        //        sb.Append("       CEMT.COST_ELEMENT_NAME,");
        //        sb.Append("       CTMT.CONTAINER_TYPE_MST_ID,");
        //        sb.Append("       DTOC.OTH_CHG_PER_CONTAINER,");
        //        sb.Append("       CTMT.PREFERENCES");
        //        sb.Append("  FROM CONT_MAIN_DEPOT_TBL        CMDT,");
        //        sb.Append("       CONT_TRN_DEPOT_OTH_CHG     CTDO,");
        //        sb.Append("       DEPOT_TRN_OTH_CHG_CONT_DET DTOC,");
        //        sb.Append("       COST_ELEMENT_MST_TBL       CEMT,");
        //        sb.Append("       CONTAINER_TYPE_MST_TBL     CTMT");
        //        sb.Append("       ");
        //        sb.Append(" WHERE CMDT.CONT_MAIN_DEPOT_PK = CTDO.CONT_MAIN_DEPOT_FK");
        //        sb.Append("       AND CTDO.CONT_DEPOT_OTH_CHG_PK = DTOC.CONT_DEPOT_OTH_CHG_FK");
        //        sb.Append("       AND CEMT.COST_ELEMENT_MST_PK = CTDO.COST_ELEMENT_MST_FK");
        //        sb.Append("       AND CTMT.CONTAINER_TYPE_MST_PK = DTOC.CONTAINER_TYPE_MST_FK");
        //        sb.Append("        AND  CMDT.CONT_MAIN_DEPOT_PK=" + DepotContMainPK);
        //        sb.Append(" ORDER BY CEMT.COST_ELEMENT_ID,CTMT.PREFERENCES");

        //        return objWK.GetDataSet(sb.ToString());
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

        //#endregion "Fetch Othercharges detail"

        //#region "Fetch LCL freight Details "

        //public DataSet FetchLCLFreightDetail(int DepotContMainPK)
        //{
        //    try
        //    {
        //        System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
        //        WorkFlow objWK = new WorkFlow();
        //        sb.Append("SELECT CTDF.FROM_DAY,");
        //        sb.Append("       CTDF.TO_DAY,");
        //        sb.Append("       CTDF.LCL_VOLUME,");
        //        sb.Append("       CTDF.LCL_WEIGHT,");
        //        sb.Append("       CTDF.LCL_RATE_PALETTE");
        //        sb.Append("       ");
        //        sb.Append("   FROM CONT_MAIN_DEPOT_TBL CMDT,");
        //        sb.Append("        CONT_TRN_DEPOT_FCL_LCL CTDF        ");
        //        sb.Append("    ");
        //        sb.Append(" WHERE CMDT.CONT_MAIN_DEPOT_PK = CTDF.CONT_MAIN_DEPOT_FK");
        //        sb.Append("        AND  CMDT.CONT_MAIN_DEPOT_PK=" + DepotContMainPK);
        //        return objWK.GetDataSet(sb.ToString());
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

        //#endregion "Fetch LCL freight Details "

        //#region "Fetch LCL Othercharge Details"

        //public DataSet FetchLCLOtherchargeDetails(int DepotContMainPK)
        //{
        //    try
        //    {
        //        System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
        //        WorkFlow objWK = new WorkFlow();
        //        sb.Append("SELECT CEMT.COST_ELEMENT_ID COST_ELEMENT_MST_FK,");
        //        sb.Append("       CTDO.LCL_RATE_PER_CBM,");
        //        sb.Append("       CTDO.LCL_RATE_PER_TON,");
        //        sb.Append("       CTDO.LCL_RATE_PALETTE");
        //        sb.Append("       ");
        //        sb.Append("  FROM CONT_MAIN_DEPOT_TBL CMDT, ");
        //        sb.Append("       COST_ELEMENT_MST_TBL       CEMT,");
        //        sb.Append("       CONT_TRN_DEPOT_OTH_CHG CTDO");
        //        sb.Append("  ");
        //        sb.Append(" WHERE CMDT.CONT_MAIN_DEPOT_PK = CTDO.CONT_MAIN_DEPOT_FK");
        //        sb.Append("       AND CEMT.COST_ELEMENT_MST_PK = CTDO.COST_ELEMENT_MST_FK");
        //        sb.Append("        AND  CMDT.CONT_MAIN_DEPOT_PK=" + DepotContMainPK);
        //        sb.Append("   ");

        //        return objWK.GetDataSet(sb.ToString());
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

        //#endregion "Fetch LCL Othercharge Details"

        //#region "Fetch Max Contract No."

        //public string FetchContractNo(string strContractNo)
        //{
        //    try
        //    {
        //        string strSQL = null;
        //        strSQL = "SELECT NVL(MAX(T.CONTRACT_NO),0) FROM CONT_MAIN_DEPOT_TBL T " + "WHERE T.CONTRACT_NO LIKE '" + strContractNo + "/%' " + "ORDER BY T.CONTRACT_NO";
        //        return (new WorkFlow()).ExecuteScaler(strSQL);
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

        //#endregion "Fetch Max Contract No."

        //#region "Fetch Existing Data While Amending"

        //public DataTable FetchNewEditData(string depotMainPK)
        //{
        //    string str = null;
        //    WorkFlow objWF = new WorkFlow();
        //    DataSet dSMain = new DataSet();
        //    DataTable dtMain = new DataTable();
        //    DataTable dtContainerType = new DataTable();
        //    str = " SELECT";
        //    str += " CT.CONT_TRN_DEPOT_PK,";
        //    str += " CT.FROM_DAY,";
        //    str += " CT.TO_DAY,";
        //    str += " CONT.CONTAINER_TYPE_MST_FK AS \"ContainerPK\",";
        //    str += " CMT.CONTAINER_TYPE_MST_ID AS \"Container Type\",";
        //    str += " CONT.FCL_CURRENT_RATE";
        //    str += " FROM ";
        //    str += " CONT_TRN_DEPOT_FCL_LCL CT ,";
        //    str += " CONT_MAIN_DEPOT_TBL CMDT,";
        //    str += " CONT_DEPOT_FCL_RATE_TRN CONT,";
        //    str += " CONTAINER_TYPE_MST_TBL CMT";
        //    str += " WHERE ";
        //    str += " CONT.CONTAINER_TYPE_MST_FK = cmt.container_type_mst_pk";
        //    str += " AND CMDT.CONT_MAIN_DEPOT_PK=CT.CONT_MAIN_DEPOT_FK";
        //    str += " AND CONT.CONT_TRN_DEPOT_FK=CT.CONT_TRN_DEPOT_PK";
        //    str += " AND CMDT.CONT_MAIN_DEPOT_PK = " + depotMainPK;
        //    str += " ORDER BY CT.CONT_TRN_DEPOT_PK, CMT.PREFERENCES";
        //    try
        //    {
        //        dSMain = objWF.GetDataSet(str);
        //        return TransposeColumns(dSMain);
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

        //#endregion "Fetch Existing Data While Amending"

        //#region "For All Data while Amending (EXisting and New)"

        //public DataTable FetchNewEditData1(string strContId = "", string depotMainPK = "")
        //{
        //    string str = "";
        //    WorkFlow objWF = new WorkFlow();
        //    DataSet dSMain = new DataSet();
        //    DataTable dtMain = new DataTable();
        //    DataTable dtContainerType = new DataTable();
        //    string strCondition = string.Empty;
        //    Int16 i = default(Int16);
        //    strContId = Strings.Replace(strContId, "'", "");
        //    string[] arr = Strings.Split(strContId, ",");
        //    try
        //    {
        //        str += " SELECT ";
        //        str += " CONT_TRN_DEPOT_PK,";
        //        str += " FROM_DAY,";
        //        str += " TO_DAY";

        //        for (i = 0; i <= arr.Length - 1; i++)
        //        {
        //            str += ",MAX(\"" + arr[i] + "\" ) \"" + arr[i] + "\" ";
        //        }

        //        str += " FROM ( SELECT ";
        //        str += " CT.CONT_TRN_DEPOT_PK,";
        //        str += " CT.FROM_DAY,";
        //        str += " CT.TO_DAY";

        //        for (i = 0; i <= arr.Length - 1; i++)
        //        {
        //            str += ", CASE WHEN CMT.CONTAINER_TYPE_MST_ID = '" + arr[i] + "' THEN CONT.FCL_CURRENT_RATE END \"" + arr[i] + "\"";
        //        }

        //        str += " FROM ";
        //        str += " CONT_TRN_DEPOT_FCL_LCL CT ,";
        //        str += " CONT_MAIN_DEPOT_TBL CMDT,";
        //        str += " CONT_DEPOT_FCL_RATE_TRN CONT,";
        //        str += " CONTAINER_TYPE_MST_TBL CMT";
        //        str += " WHERE ";
        //        str += " CONT.CONTAINER_TYPE_MST_FK = cmt.container_type_mst_pk";
        //        str += " AND CMDT.CONT_MAIN_DEPOT_PK=CT.CONT_MAIN_DEPOT_FK";
        //        str += " AND CONT.CONT_TRN_DEPOT_FK=CT.CONT_TRN_DEPOT_PK";
        //        str += " AND CMDT.CONT_MAIN_DEPOT_PK = " + depotMainPK;
        //        str += ") GROUP BY ";
        //        str += " CONT_TRN_DEPOT_PK,";
        //        str += " FROM_DAY,";
        //        str += " TO_DAY  order by from_day ";
        //        dtMain = objWF.GetDataTable(str);
        //        return dtMain;
        //    }
        //    catch (OracleException ex)
        //    {
        //        throw ex;
        //    }
        //}

        //#endregion "For All Data while Amending (EXisting and New)"

        //#region "FetchContainerData1"

        //public DataTable FetchContainerData1(string strContId = "", string depotMainPK = "")
        //{
        //    string str = "";
        //    WorkFlow objWF = new WorkFlow();
        //    DataSet dSMain = new DataSet();
        //    DataTable dtMain = new DataTable();
        //    DataTable dtContainerType = new DataTable();
        //    string strCondition = string.Empty;
        //    Int16 i = default(Int16);
        //    strContId = Strings.Replace(strContId, "'", "");
        //    string[] arr = Strings.Split(strContId, ",");

        //    try
        //    {
        //        str += " SELECT ";
        //        str += " CONT_TRN_DEPOT_PK,";
        //        str += " FROM_DAY,";
        //        str += " TO_DAY";

        //        for (i = 0; i <= arr.Length - 1; i++)
        //        {
        //            str += ",MAX(\"" + arr[i] + "\" ) \"" + arr[i] + "\" ";
        //        }

        //        str += " FROM ( SELECT ";
        //        str += " CT.CONT_TRN_DEPOT_PK,";
        //        str += " CT.FROM_DAY,";
        //        str += " CT.TO_DAY";

        //        for (i = 0; i <= arr.Length - 1; i++)
        //        {
        //            str += ", CASE WHEN CMT.CONTAINER_TYPE_MST_ID = '" + arr[i] + "' THEN CONT.FCL_CURRENT_RATE END \"" + arr[i] + "\"";
        //        }

        //        str += " FROM ";
        //        str += " CONT_TRN_DEPOT_FCL_LCL CT ,";
        //        str += " CONT_MAIN_DEPOT_TBL CMDT,";
        //        str += " CONT_DEPOT_FCL_RATE_TRN CONT,";
        //        str += " CONTAINER_TYPE_MST_TBL CMT";
        //        str += " WHERE ";
        //        str += " CONT.CONTAINER_TYPE_MST_FK = cmt.container_type_mst_pk";
        //        str += " AND CMDT.CONT_MAIN_DEPOT_PK=CT.CONT_MAIN_DEPOT_FK";
        //        str += " AND CONT.CONT_TRN_DEPOT_FK=CT.CONT_TRN_DEPOT_PK";
        //        str += " AND CMDT.CONT_MAIN_DEPOT_PK = " + depotMainPK;
        //        str += ") GROUP BY ";
        //        str += " CONT_TRN_DEPOT_PK,";
        //        str += " FROM_DAY,";
        //        str += " TO_DAY order by from_day ";
        //        dtMain = objWF.GetDataTable(str);
        //        return dtMain;
        //    }
        //    catch (OracleException ex)
        //    {
        //        throw ex;
        //    }
        //}

        //#endregion "FetchContainerData1"

        //#region "Edit Existing Other Charges"

        //public DataTable FetchContainerDataOthChrg1(string depotMainPK, string strContId = "")
        //{
        //    WorkFlow objWF = new WorkFlow();
        //    DataSet dSMain = new DataSet();
        //    string str = null;
        //    DataTable dtMain = new DataTable();
        //    string strCondition = string.Empty;
        //    Int16 i = default(Int16);
        //    strContId = Strings.Replace(strContId, "'", "");
        //    string[] arr = Strings.Split(strContId, ",");

        //    try
        //    {
        //        str += " SELECT ";
        //        str += " CONT_DEPOT_OTH_CHG_PK, ";
        //        str += " COST_ELEMENT_MST_FK,";
        //        str += " SEL,";
        //        str += "  \"Other Charges\" ";

        //        for (i = 0; i <= arr.Length - 1; i++)
        //        {
        //            str += ",MAX(\"" + arr[i] + "\" ) \"" + arr[i] + "\" ";
        //        }

        //        str += " FROM ( SELECT ";
        //        str += " CT.CONT_DEPOT_OTH_CHG_PK, ";
        //        str += " CT.COST_ELEMENT_MST_FK,";
        //        str += " 'true' AS \"SEL\",";
        //        str += " CC.COST_ELEMENT_NAME AS \"Other Charges\" ";

        //        for (i = 0; i <= arr.Length - 1; i++)
        //        {
        //            str += ", CASE WHEN CMT.CONTAINER_TYPE_MST_ID = '" + arr[i] + "' THEN CONT.OTH_CHG_PER_CONTAINER END \"" + arr[i] + "\"";
        //        }

        //        str += " FROM CONT_TRN_DEPOT_OTH_CHG     CT,";
        //        str += " CONT_MAIN_DEPOT_TBL        CMDT,";
        //        str += "  DEPOT_TRN_OTH_CHG_CONT_DET CONT,";
        //        str += " CONTAINER_TYPE_MST_TBL     CMT,";
        //        str += " COST_ELEMENT_MST_TBL       CC ";

        //        str += " WHERE CONT.CONTAINER_TYPE_MST_FK = CMT.CONTAINER_TYPE_MST_PK";
        //        str += " AND CC.COST_ELEMENT_MST_PK = CT.COST_ELEMENT_MST_FK ";
        //        str += " AND CMDT.CONT_MAIN_DEPOT_PK = CT.CONT_MAIN_DEPOT_FK ";
        //        str += " AND CMDT.CONT_MAIN_DEPOT_PK = " + depotMainPK;
        //        str += " AND CONT.CONT_DEPOT_OTH_CHG_FK = CT.CONT_DEPOT_OTH_CHG_PK ";

        //        str += "  UNION ";
        //        str += " SELECT NULL CONT_DEPOT_OTH_CHG_PK,";
        //        str += " CC.COST_ELEMENT_MST_PK, ";
        //        str += " 'false' AS \"SEL\", ";
        //        str += " CC.COST_ELEMENT_NAME AS \"Other Charges\" ";

        //        for (i = 0; i <= arr.Length - 1; i++)
        //        {
        //            str += " , NULL AS \"Oth_Chg_Per_Container\" ";
        //        }
        //        str += "  FROM CONT_TRN_DEPOT_OTH_CHG     CT, ";
        //        str += " CONT_MAIN_DEPOT_TBL        CMDT, ";
        //        str += " DEPOT_TRN_OTH_CHG_CONT_DET CONT, ";
        //        str += " CONTAINER_TYPE_MST_TBL  CMT,";
        //        str += " COST_ELEMENT_MST_TBL    CC, ";
        //        str += "  VENDOR_TYPE_MST_TBL  V ";

        //        str += " WHERE CC.VENDOR_TYPE_MST_FK = V.VENDOR_TYPE_PK(+) ";
        //        str += " AND CT.CONT_MAIN_DEPOT_FK = CMDT.CONT_MAIN_DEPOT_PK ";
        //        str += " AND CONT.CONTAINER_TYPE_MST_FK = CMT.CONTAINER_TYPE_MST_PK ";
        //        str += " AND CMDT.CONT_MAIN_DEPOT_PK = CT.CONT_MAIN_DEPOT_FK ";
        //        str += " AND CONT.CONT_DEPOT_OTH_CHG_FK = CT.CONT_DEPOT_OTH_CHG_PK ";

        //        str += " AND CMT.CONTAINER_TYPE_MST_PK IN ";
        //        str += " (SELECT CONT.CONTAINER_TYPE_MST_FK ";
        //        str += "  FROM CONT_TRN_DEPOT_FCL_LCL  CTN, ";
        //        str += "  CONT_MAIN_DEPOT_TBL     CMDT, ";
        //        str += " CONT_DEPOT_FCL_RATE_TRN CONT,";
        //        str += "  CONTAINER_TYPE_MST_TBL  CMT ";

        //        str += "  WHERE CMDT.CONT_MAIN_DEPOT_PK = CTN.CONT_MAIN_DEPOT_FK ";
        //        str += "  AND CONT.CONTAINER_TYPE_MST_FK = CMT.CONTAINER_TYPE_MST_PK ";
        //        str += "  AND CONT.CONT_TRN_DEPOT_FK = CTN.CONT_TRN_DEPOT_PK ";
        //        str += " AND CMDT.CONT_MAIN_DEPOT_PK = " + depotMainPK;
        //        str += "  ) ";
        //        str += " AND CC.COST_ELEMENT_MST_PK NOT IN ( ";
        //        str += " Select CON.COST_ELEMENT_MST_FK ";
        //        str += " FROM CONT_MAIN_DEPOT_TBL CMDT, CONT_TRN_DEPOT_OTH_CHG CON ";
        //        str += " WHERE CMDT.CONT_MAIN_DEPOT_PK = CON.CONT_MAIN_DEPOT_FK ";
        //        str += "  AND CMDT.CONT_MAIN_DEPOT_PK = " + depotMainPK;
        //        str += " ) ";
        //        str += " AND CC.BUSINESS_TYPE IN (3, 2) ";
        //        str += " AND V.VENDOR_TYPE_ID = 'WAREHOUSE' ";
        //        str += " ORDER BY CONT_DEPOT_OTH_CHG_PK, SEL)";
        //        str += " GROUP BY ";
        //        str += " CONT_DEPOT_OTH_CHG_PK,";
        //        str += " COST_ELEMENT_MST_FK,";
        //        str += " SEL,\"Other Charges\" ";

        //        dtMain = objWF.GetDataTable(str);
        //        return dtMain;
        //    }
        //    catch (OracleException ex)
        //    {
        //        throw ex;
        //    }
        //}

        //#endregion "Edit Existing Other Charges"

        //#region "Deactive Existing Cntract"

        //public ArrayList Deactivate(long ContractPk)
        //{
        //    WorkFlow objWK = new WorkFlow();
        //    string strSQL = null;
        //    OracleTransaction TRAN = null;
        //    objWK.OpenConnection();
        //    TRAN = objWK.MyConnection.BeginTransaction();
        //    arrMessage.Clear();
        //    objWK.MyCommand.Transaction = TRAN;

        //    strSQL = "UPDATE CONT_MAIN_DEPOT_TBL T " + "SET T.ACTIVE = 0,T.VALID_TO = TO_DATE(SYSDATE,'dd/mm/yyyy')," + "T.LAST_MODIFIED_BY_FK =" + M_CREATED_BY_FK + "," + "T.LAST_MODIFIED_DT = SYSDATE," + "T.VERSION_NO = T.VERSION_NO + 1" + "WHERE T.CONT_MAIN_DEPOT_PK =" + ContractPk;
        //    objWK.MyCommand.CommandType = CommandType.Text;
        //    objWK.MyCommand.CommandText = strSQL;
        //    try
        //    {
        //        objWK.MyCommand.ExecuteScalar();
        //        _PkValue = ContractPk;
        //        arrMessage.Add("All data saved successfully");
        //        TRAN.Commit();
        //        return arrMessage;
        //    }
        //    catch (Exception ex)
        //    {
        //        arrMessage.Add(ex.Message);
        //        TRAN.Rollback();
        //        return arrMessage;
        //    }
        //    finally
        //    {
        //        objWK.MyConnection.Close();
        //    }
        //}

        //#endregion "Deactive Existing Cntract"
    }
}