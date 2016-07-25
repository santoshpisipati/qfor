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
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_DeliveryOrder : CommonFeatures
    {
        public long doHeadperPk;

        #region "FetchDO"

        public DataSet FetchDO(Int32 JobPk, Int32 BizType, Int32 FclLcl, Int32 DOPk, Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {
            WorkFlow objWK = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            DataSet dsData = new DataSet();

            try
            {
                objWK.OpenConnection();
                objWK.MyCommand.Connection = objWK.MyConnection;

                var _with1 = objWK.MyCommand;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".DO_TBL_PKG.DO_MST_TBL_FETCH";

                objWK.MyCommand.Parameters.Clear();
                var _with2 = objWK.MyCommand.Parameters;

                _with2.Add("JOB_CARD_FK_IN", JobPk).Direction = ParameterDirection.Input;
                _with2.Add("Biz_Type_IN", BizType).Direction = ParameterDirection.Input;
                _with2.Add("Fcl_Lcl_IN", FclLcl).Direction = ParameterDirection.Input;
                _with2.Add("DO_PK_IN", DOPk).Direction = ParameterDirection.Input;
                _with2.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
                _with2.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with2.Add("MASTER_PAGE_SIZE_IN", RecordsPerPage).Direction = ParameterDirection.InputOutput;
                _with2.Add("DO_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                objWK.MyDataAdapter.SelectCommand = objWK.MyCommand;
                objWK.MyDataAdapter.Fill(dsData);

                TotalPage = Convert.ToInt32(objWK.MyCommand.Parameters["TOTALPAGE_IN"].Value);
                CurrentPage = Convert.ToInt32(objWK.MyCommand.Parameters["CURRENTPAGE_IN"].Value);

                //If TotalPage = 1 Then
                //    CurrentPage = 1
                //Else
                //    CurrentPage = 0
                //End If

                return dsData;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "FetchDO"

        #region "FetchHeaderInfo"

        public DataSet FetchHeaderInfo(Int32 DoPk, Int32 BizType)
        {
            WorkFlow objWK = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            DataSet dsData = new DataSet();

            try
            {
                objWK.OpenConnection();
                objWK.MyCommand.Connection = objWK.MyConnection;

                var _with3 = objWK.MyCommand;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".DO_TBL_PKG.DO_HDR_INFO";

                objWK.MyCommand.Parameters.Clear();
                var _with4 = objWK.MyCommand.Parameters;

                _with4.Add("DO_PK_IN", DoPk).Direction = ParameterDirection.Input;
                _with4.Add("Biz_Type_IN", BizType).Direction = ParameterDirection.Input;
                _with4.Add("DO_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                objWK.MyDataAdapter.SelectCommand = objWK.MyCommand;
                objWK.MyDataAdapter.Fill(dsData);
                return dsData;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "FetchHeaderInfo"

        #region "FetchDONr"

        public DataSet FetchDONr(Int32 JobPk, Int32 BizType)
        {
            WorkFlow objWK = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            DataSet dsData = new DataSet();

            try
            {
                objWK.OpenConnection();
                objWK.MyCommand.Connection = objWK.MyConnection;

                var _with5 = objWK.MyCommand;
                _with5.CommandType = CommandType.StoredProcedure;
                _with5.CommandText = objWK.MyUserName + ".DO_TBL_PKG.DO_MST_TBL_FETCH_NR";

                objWK.MyCommand.Parameters.Clear();
                var _with6 = objWK.MyCommand.Parameters;

                _with6.Add("JOB_CARD_FK_IN", JobPk).Direction = ParameterDirection.Input;
                _with6.Add("Biz_Type_IN", BizType).Direction = ParameterDirection.Input;
                _with6.Add("DO_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                objWK.MyDataAdapter.SelectCommand = objWK.MyCommand;
                objWK.MyDataAdapter.Fill(dsData);

                return dsData;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "FetchDONr"

        #region "FetchPrintDS"

        public DataSet FetchPrintDS(Int32 JobPk, Int32 BizType)
        {
            WorkFlow objWK = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            DataSet dsData = new DataSet();

            try
            {
                objWK.OpenConnection();
                objWK.MyCommand.Connection = objWK.MyConnection;

                var _with7 = objWK.MyCommand;
                _with7.CommandType = CommandType.StoredProcedure;
                _with7.CommandText = objWK.MyUserName + ".DO_TBL_PKG.DO_MST_TBL_FETCH_PRINT";

                objWK.MyCommand.Parameters.Clear();
                var _with8 = objWK.MyCommand.Parameters;

                _with8.Add("JOB_CARD_FK_IN", JobPk).Direction = ParameterDirection.Input;
                _with8.Add("Biz_Type_IN", BizType).Direction = ParameterDirection.Input;
                _with8.Add("DO_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                objWK.MyDataAdapter.SelectCommand = objWK.MyCommand;
                objWK.MyDataAdapter.Fill(dsData);

                return dsData;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "FetchPrintDS"

        #region "SaveDO"

        public ArrayList SaveDO(Int32 JobPk, string BLRefNo, string BLRefDate, Int32 BizType, DataSet gridDS, DataSet DSHistoryDO, System.DateTime doValidDt, string MDO_Nr, string MDO_Date = "", string SplIns = "",
        string GLDDate = "", Int32 DepotPK = 0, int ReturnDepotPK = 0, string PickupDate = "", string ReturnDate = "", Int32 FclLcl = 0, Int32 DOPK = 0, Int32 DOStatus = 0)
        {
            ArrayList functionReturnValue = null;
            WorkFlow objWK = new WorkFlow();
            //Dim objCommand As New OracleClient.OracleCommand
            cls_TrackAndTrace objTrackNTrace = new cls_TrackAndTrace();
            DataSet dsData = new DataSet();
            string MSTDORefNum = null;
            //Dim doHeadperPk As Long
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            OracleTransaction insertTrans = null;
            OracleTransaction updateTrans = null;

            try
            {
                if (DOPK != 0)
                {
                    objWK.OpenConnection();
                    objWK.MyCommand.Connection = objWK.MyConnection;
                    updateTrans = objWK.MyConnection.BeginTransaction();
                    var _with9 = updCommand;
                    _with9.Connection = objWK.MyConnection;
                    _with9.CommandType = CommandType.StoredProcedure;
                    _with9.CommandText = objWK.MyUserName + ".DO_TBL_PKG.DO_MST_TBL_SAVE_UPD";
                    updCommand.Parameters.Clear();

                    var _with10 = updCommand.Parameters;
                    _with10.Add("DO_PK_IN", DOPK).Direction = ParameterDirection.Input;
                    _with10.Add("DO_STATUS_IN", DOStatus).Direction = ParameterDirection.Input;

                    var _with11 = objWK.MyDataAdapter;
                    _with11.UpdateCommand = updCommand;
                    _with11.UpdateCommand.Transaction = updateTrans;
                    _with11.UpdateCommand.ExecuteNonQuery();
                    updateTrans.Commit();
                    arrMessage.Add("All Data Saved Successfully");
                    arrMessage.Add(DOPK);
                    arrMessage.Add("");
                    return arrMessage;
                }
                else
                {
                    objWK.OpenConnection();
                    //objWK.MyCommand.Connection = objWK.MyConnection
                    insertTrans = objWK.MyConnection.BeginTransaction();

                    if (BizType == 2)
                    {
                        MSTDORefNum = GenerateProtocolKey("DO (SEA)", Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt64(HttpContext.Current.Session["EMP_PK"]), DateTime.Now, "","" ,"" , CREATED_BY);
                    }
                    else
                    {
                        MSTDORefNum = GenerateProtocolKey("DO (AIR)", Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt64(HttpContext.Current.Session["EMP_PK"]), DateTime.Now, "", "", "", CREATED_BY);
                    }
                    if (MSTDORefNum == "Protocol Not Defined.")
                    {
                        arrMessage.Add("Protocol Not Defined");
                        return functionReturnValue;
                    }

                    var _with12 = insCommand;
                    _with12.Connection = objWK.MyConnection;
                    _with12.CommandType = CommandType.StoredProcedure;
                    _with12.CommandText = objWK.MyUserName + ".DO_TBL_PKG.DO_MST_TBL_SAVE";
                    insCommand.Parameters.Clear();

                    if (BizType == 2)
                    {
                        var _with13 = insCommand.Parameters;
                        _with13.Add("job_card_fk_in", JobPk).Direction = ParameterDirection.Input;
                        _with13.Add("bl_ref_nr_in", getDefault(BLRefNo, "")).Direction = ParameterDirection.Input;
                        _with13.Add("mst_do_refnum_in", MSTDORefNum).Direction = ParameterDirection.Input;
                        _with13.Add("blrefdate_in", getDefault(BLRefDate, "")).Direction = ParameterDirection.Input;
                        _with13.Add("biz_type_in", BizType).Direction = ParameterDirection.Input;
                        if (doValidDt == null)
                        {
                            _with13.Add("do_valid_til_in", "").Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with13.Add("do_valid_til_in", doValidDt).Direction = ParameterDirection.Input;
                        }
                        if (string.IsNullOrEmpty(MDO_Nr))
                        {
                            _with13.Add("mdo_nr_in", "").Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with13.Add("mdo_nr_in", MDO_Nr).Direction = ParameterDirection.Input;
                        }
                        _with13.Add("MDO_DATE_IN", Convert.ToDateTime(MDO_Date)).Direction = ParameterDirection.Input;
                        _with13.Add("fcl_lcl_in", FclLcl).Direction = ParameterDirection.Input;
                        _with13.Add("DO_STATUS_IN", DOStatus).Direction = ParameterDirection.Input;
                        _with13.Add("JOB_CARD_AIR_FK_IN", "").Direction = ParameterDirection.Input;
                        if ((DSHistoryDO != null))
                        {
                            if (DSHistoryDO.Tables[0].Rows.Count > 0)
                            {
                                _with13.Add("DO_PK_IN", DSHistoryDO.Tables[0].Rows[DSHistoryDO.Tables[0].Rows.Count - 1]["DOPK"]).Direction = ParameterDirection.Input;
                                _with13.Add("REMARKS_IN", DSHistoryDO.Tables[0].Rows[DSHistoryDO.Tables[0].Rows.Count - 1]["REMARKS"]).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with13.Add("DO_PK_IN", "").Direction = ParameterDirection.Input;
                                _with13.Add("REMARKS_IN", "").Direction = ParameterDirection.Input;
                            }
                        }
                        else
                        {
                            _with13.Add("DO_PK_IN", "").Direction = ParameterDirection.Input;
                            _with13.Add("REMARKS_IN", "").Direction = ParameterDirection.Input;
                        }
                        _with13.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    }
                    else
                    {
                        var _with14 = insCommand.Parameters;
                        _with14.Add("job_card_fk_in", "").Direction = ParameterDirection.Input;
                        _with14.Add("bl_ref_nr_in", getDefault(BLRefNo, "")).Direction = ParameterDirection.Input;
                        _with14.Add("mst_do_refnum_in", MSTDORefNum).Direction = ParameterDirection.Input;
                        _with14.Add("blrefdate_in", getDefault(BLRefDate, "")).Direction = ParameterDirection.Input;
                        _with14.Add("biz_type_in", BizType).Direction = ParameterDirection.Input;
                        if (doValidDt == null)
                        {
                            _with14.Add("do_valid_til_in", "").Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with14.Add("do_valid_til_in", doValidDt).Direction = ParameterDirection.Input;
                        }
                        if (string.IsNullOrEmpty(MDO_Nr))
                        {
                            _with14.Add("mdo_nr_in", "").Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with14.Add("mdo_nr_in", MDO_Nr).Direction = ParameterDirection.Input;
                        }
                        _with14.Add("MDO_DATE_IN", Convert.ToDateTime(MDO_Date)).Direction = ParameterDirection.Input;
                        _with14.Add("fcl_lcl_in", FclLcl).Direction = ParameterDirection.Input;
                        _with14.Add("DO_STATUS_IN", DOStatus).Direction = ParameterDirection.Input;
                        _with14.Add("JOB_CARD_AIR_FK_IN", JobPk).Direction = ParameterDirection.Input;
                        if ((DSHistoryDO != null))
                        {
                            if (DSHistoryDO.Tables[0].Rows.Count > 0)
                            {
                                _with14.Add("DO_PK_IN", DSHistoryDO.Tables[0].Rows[DSHistoryDO.Tables[0].Rows.Count - 1]["DOPK"]).Direction = ParameterDirection.Input;
                                _with14.Add("REMARKS_IN", DSHistoryDO.Tables[0].Rows[DSHistoryDO.Tables[0].Rows.Count - 1]["REMARKS"]).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with14.Add("DO_PK_IN", "").Direction = ParameterDirection.Input;
                                _with14.Add("REMARKS_IN", "").Direction = ParameterDirection.Input;
                            }
                        }
                        else
                        {
                            _with14.Add("DO_PK_IN", "").Direction = ParameterDirection.Input;
                            _with14.Add("REMARKS_IN", "").Direction = ParameterDirection.Input;
                        }
                        _with14.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    }

                    var _with15 = objWK.MyDataAdapter;
                    _with15.InsertCommand = insCommand;
                    _with15.InsertCommand.Transaction = insertTrans;
                    _with15.InsertCommand.ExecuteNonQuery();
                    doHeadperPk = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
                    //If Not IsNothing(DSHistoryDO) Then
                    //    If DSHistoryDO.Tables(0).Rows.Count > 0 Then
                    //        objWK.OpenConnection()
                    //        objWK.MyCommand.Connection = objWK.MyConnection
                    //        updateTrans = objWK.MyConnection.BeginTransaction()
                    //        With updCommand
                    //            .Connection = objWK.MyConnection
                    //            .CommandType = CommandType.StoredProcedure
                    //            .CommandText = objWK.MyUserName & ".DO_TBL_PKG.DO_MST_TBL_SAVE_UPD_REMARKS"
                    //            updCommand.Parameters.Clear()
                    //        End With

                    //        With updCommand.Parameters
                    //            .Add("DO_PK_IN", DSHistoryDO.Tables(0).Rows(DSHistoryDO.Tables(0).Rows.Count - 1).Item("DOPK")).Direction = ParameterDirection.Input
                    //            .Add("REMARKS_IN", DSHistoryDO.Tables(0).Rows(DSHistoryDO.Tables(0).Rows.Count - 1).Item("REMARKS")).Direction = ParameterDirection.Input
                    //        End With

                    //        With objWK.MyDataAdapter
                    //            .UpdateCommand = updCommand
                    //            .UpdateCommand.Transaction = updateTrans
                    //            .UpdateCommand.ExecuteNonQuery()
                    //        End With
                    //    End If
                    //End If

                    if ((SaveDOGrid(BizType, gridDS, doHeadperPk, JobPk, insertTrans, SplIns, GLDDate, DepotPK, ReturnDepotPK, PickupDate,
                    ReturnDate, FclLcl)) == 0)
                    {
                        SaveTrackAndTrace(insertTrans, JobPk, BizType, 2, "DO", "DO-INS", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), objWK, "INS", Convert.ToInt64(HttpContext.Current.Session["USER_PK"]),
                        "O");
                        UpdateDOExport(JobPk, BizType, MSTDORefNum);
                        arrMessage.Add("All Data Saved Successfully");
                        arrMessage.Add(doHeadperPk);
                        arrMessage.Add(MSTDORefNum);
                        return arrMessage;
                    }
                    else
                    {
                        if (BizType == 2)
                        {
                            RollbackProtocolKey("DO (SEA)", Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt64(HttpContext.Current.Session["EMP_PK"]), MSTDORefNum, System.DateTime.Now);
                            //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                        }
                        else
                        {
                            RollbackProtocolKey("DO (AIR)", Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt64(HttpContext.Current.Session["EMP_PK"]), MSTDORefNum, System.DateTime.Now);
                            //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                        }
                        insertTrans.Rollback();
                    }
                }
                //arrMessage = objTrackNTrace.SaveTrackAndTrace(JobPk, BizType, 2, "DO", "DO-INS", CLng(HttpContext.Current.Session("LOGED_IN_LOC_FK")), objWK, "INS", HttpContext.Current.Session("USER_PK"), "O")
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                if (BizType == 2)
                {
                    RollbackProtocolKey("DO (SEA)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), MSTDORefNum, System.DateTime.Now);
                    //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                }
                else
                {
                    RollbackProtocolKey("DO (AIR)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), MSTDORefNum, System.DateTime.Now);
                    //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                }
                insertTrans.Rollback();
                throw ex;
            }
            finally
            {
                objWK.CloseConnection();
                //Added by sivachandran - To close the connection after Transaction
            }
            return functionReturnValue;
        }

        #endregion "SaveDO"

        #region "SaveDOGrid"

        public Int16 SaveDOGrid(Int32 BizType, DataSet gridDS, long DOHeaderPk, long JobPk, OracleTransaction insertTrans, string SplInst, string GLDDate, Int32 DepotPK, int ReturnDepotPK, string PickupDate,
        string ReturnDate, Int32 FclLcl = 0)
        {
            WorkFlow objWK = new WorkFlow();
            OracleCommand insCommand = new OracleCommand();
            cls_TrackAndTrace objTrackNTrace = new cls_TrackAndTrace();
            DataSet dsData = new DataSet();
            string MSTDORefNum = null;
            Int32 i = default(Int32);
            try
            {
                objWK.OpenConnection();
                objWK.MyConnection = insertTrans.Connection;

                var _with16 = insCommand;
                _with16.Connection = objWK.MyConnection;
                _with16.CommandType = CommandType.StoredProcedure;
                _with16.CommandText = objWK.MyUserName + ".DO_TBL_PKG.DO_DTL_TBL_SAVE";
                if (BizType == 2)
                {
                    for (i = 0; i <= gridDS.Tables[0].Rows.Count - 1; i++)
                    {
                        insCommand.Parameters.Clear();
                        if (Convert.ToInt32(gridDS.Tables[0].Rows[i]["delivery_order_dtl_pk"]) == 0)
                        {
                            //If Not IsDBNull(gridDS.Tables(0).Rows(i).Item("SELFLAG")) Then
                            //If gridDS.Tables(0).Rows(i).Item("SELFLAG") = "True" Then
                            var _with17 = insCommand.Parameters;
                            _with17.Add("DELIVERY_ORDER_FK_IN", DOHeaderPk).Direction = ParameterDirection.Input;
                            //.Add("DISCHARGE_DT_IN", IIf(IsDBNull(gridDS.Tables(0).Rows(i).Item("DISCHARGE_DATE")), "", gridDS.Tables(0).Rows(i).Item("DISCHARGE_DATE"))).Direction = ParameterDirection.Input
                            //.Add("PICK_UP_DT_IN", IIf(IsDBNull(gridDS.Tables(0).Rows(i).Item("PICK_UP_DATE")), "", gridDS.Tables(0).Rows(i).Item("PICK_UP_DATE"))).Direction = ParameterDirection.Input
                            //.Add("PICK_UP_DPT_FK_IN", IIf(IsDBNull(gridDS.Tables(0).Rows(i).Item("PICK_UP_DEPOT_FK")), "", gridDS.Tables(0).Rows(i).Item("PICK_UP_DEPOT_FK"))).Direction = ParameterDirection.Input
                            //.Add("RETURN_DT_IN", IIf(IsDBNull(gridDS.Tables(0).Rows(i).Item("RETURN_DATE")), "", gridDS.Tables(0).Rows(i).Item("RETURN_DATE"))).Direction = ParameterDirection.Input
                            //.Add("RETURN_DPT_FK_IN", IIf(IsDBNull(gridDS.Tables(0).Rows(i).Item("RETURN_DEPOT_FK")), "", gridDS.Tables(0).Rows(i).Item("RETURN_DEPOT_FK"))).Direction = ParameterDirection.Input

                            _with17.Add("DISCHARGE_DT_IN", ((GLDDate == null) ? DateTime.MinValue : Convert.ToDateTime(GLDDate))).Direction = ParameterDirection.Input;
                            _with17.Add("PICK_UP_DT_IN", ((PickupDate == null) ? DateTime.MinValue : Convert.ToDateTime(PickupDate))).Direction = ParameterDirection.Input;
                            _with17.Add("PICK_UP_DPT_FK_IN", ((DepotPK == null) ? 0 : DepotPK)).Direction = ParameterDirection.Input;
                            if (FclLcl == 4)
                            {
                                _with17.Add("RETURN_DT_IN", "").Direction = ParameterDirection.Input;
                                _with17.Add("RETURN_DPT_FK_IN", "").Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with17.Add("RETURN_DT_IN", ((ReturnDate == null) ? DateTime.MinValue : Convert.ToDateTime(ReturnDate))).Direction = ParameterDirection.Input;
                                _with17.Add("RETURN_DPT_FK_IN", ((ReturnDepotPK == null) ? 0 : ReturnDepotPK)).Direction = ParameterDirection.Input;
                            }
                            //If FclLcl = 2 Then
                            //    .Add("CONTAINER_IN", "").Direction = ParameterDirection.Input
                            //    '''Added By Kotehswari on 23/2/2011
                            //Else--------COMMENTED BY ASHISH
                            if (FclLcl == 4)
                            {
                                _with17.Add("CONTAINER_IN", "").Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with17.Add("CONTAINER_IN", (string.IsNullOrEmpty(gridDS.Tables[0].Rows[i]["CONT_NR"].ToString()) ? "" : gridDS.Tables[0].Rows[i]["CONT_NR"])).Direction = ParameterDirection.Input;
                            }

                            _with17.Add("SPL_INST_IN", (string.IsNullOrEmpty(SplInst) ? "" : SplInst)).Direction = ParameterDirection.Input;
                            _with17.Add("Biz_Type_IN", BizType).Direction = ParameterDirection.Input;
                            _with17.Add("JOB_CARD_SEA_IMP_CONT_FK_IN", (string.IsNullOrEmpty(gridDS.Tables[0].Rows[i]["JOB_CARD_SEA_IMP_CONT_FK"].ToString()) ? "" : gridDS.Tables[0].Rows[i]["JOB_CARD_SEA_IMP_CONT_FK"])).Direction = ParameterDirection.Input;
                            _with17.Add("JOB_CARD_AIR_IMP_CONT_FK_IN", "").Direction = ParameterDirection.Input;
                            _with17.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                            //.ExecuteNonQuery()
                            var _with18 = objWK.MyDataAdapter;
                            _with18.InsertCommand = insCommand;
                            _with18.InsertCommand.Transaction = insertTrans;
                            _with18.InsertCommand.ExecuteNonQuery();
                            //End If
                            //End If
                        }
                    }
                }
                else if (BizType == 1)
                {
                    for (i = 0; i <= gridDS.Tables[0].Rows.Count - 1; i++)
                    {
                        insCommand.Parameters.Clear();
                        if (Convert.ToInt32(gridDS.Tables[0].Rows[i]["delivery_order_dtl_pk"]) == 0)
                        {
                            //If Not IsDBNull(gridDS.Tables(0).Rows(i).Item("SELFLAG")) Then
                            //If gridDS.Tables(0).Rows(i).Item("SELFLAG") = "True" Then
                            var _with19 = insCommand.Parameters;
                            _with19.Add("DELIVERY_ORDER_FK_IN", DOHeaderPk).Direction = ParameterDirection.Input;
                            //.Add("DISCHARGE_DT_IN", IIf(IsDBNull(gridDS.Tables(0).Rows(i).Item("DISCHARGE_DATE")), "", gridDS.Tables(0).Rows(i).Item("DISCHARGE_DATE"))).Direction = ParameterDirection.Input
                            //.Add("PICK_UP_DT_IN", IIf(IsDBNull(gridDS.Tables(0).Rows(i).Item("PICK_UP_DATE")), "", gridDS.Tables(0).Rows(i).Item("PICK_UP_DATE"))).Direction = ParameterDirection.Input
                            _with19.Add("DISCHARGE_DT_IN", ((GLDDate == null) ? DateTime.MinValue : Convert.ToDateTime(GLDDate))).Direction = ParameterDirection.Input;
                            _with19.Add("PICK_UP_DT_IN", (string.IsNullOrEmpty(PickupDate) ? DateTime.MinValue : Convert.ToDateTime(PickupDate))).Direction = ParameterDirection.Input;
                            if (FclLcl == 1 | FclLcl == 3)
                            {
                                //.Add("PICK_UP_DPT_FK_IN", IIf(IsDBNull(gridDS.Tables(0).Rows(i).Item("PICK_UP_DEPOT_FK")), "", gridDS.Tables(0).Rows(i).Item("PICK_UP_DEPOT_FK"))).Direction = ParameterDirection.Input
                                _with19.Add("PICK_UP_DPT_FK_IN", ((DepotPK == null) ? 0 : DepotPK)).Direction = ParameterDirection.Input;
                                _with19.Add("RETURN_DT_IN", "").Direction = ParameterDirection.Input;
                                _with19.Add("RETURN_DPT_FK_IN", "").Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                //.Add("PICK_UP_DPT_FK_IN", IIf(IsDBNull(gridDS.Tables(0).Rows(i).Item("PICK_UP_DEPOT_FK")), "", gridDS.Tables(0).Rows(i).Item("PICK_UP_DEPOT_FK"))).Direction = ParameterDirection.Input
                                //.Add("RETURN_DT_IN", IIf(IsDBNull(gridDS.Tables(0).Rows(i).Item("RETURN_DATE")), "", gridDS.Tables(0).Rows(i).Item("RETURN_DATE"))).Direction = ParameterDirection.Input
                                //.Add("RETURN_DPT_FK_IN", IIf(IsDBNull(gridDS.Tables(0).Rows(i).Item("RETURN_DEPOT_FK")), "", gridDS.Tables(0).Rows(i).Item("RETURN_DEPOT_FK"))).Direction = ParameterDirection.Input
                                _with19.Add("PICK_UP_DPT_FK_IN", ((DepotPK == null) ? 0 : DepotPK)).Direction = ParameterDirection.Input;
                                _with19.Add("RETURN_DT_IN", ((ReturnDate == null) ? DateTime.MinValue : Convert.ToDateTime(ReturnDate))).Direction = ParameterDirection.Input;
                                _with19.Add("RETURN_DPT_FK_IN", ((ReturnDepotPK == null) ? 0: ReturnDepotPK)).Direction = ParameterDirection.Input;
                            }
                            _with19.Add("CONTAINER_IN", "").Direction = ParameterDirection.Input;
                            //.Add("SPL_INST_IN", IIf(IsDBNull(gridDS.Tables(0).Rows(i).Item("SPLC_INST")), "", gridDS.Tables(0).Rows(i).Item("SPLC_INST"))).Direction = ParameterDirection.Input
                            _with19.Add("SPL_INST_IN", (string.IsNullOrEmpty(SplInst) ? "" : SplInst)).Direction = ParameterDirection.Input;
                            _with19.Add("Biz_Type_IN", BizType).Direction = ParameterDirection.Input;
                            _with19.Add("JOB_CARD_SEA_IMP_CONT_FK_IN", "").Direction = ParameterDirection.Input;
                            _with19.Add("JOB_CARD_AIR_IMP_CONT_FK_IN", (string.IsNullOrEmpty(gridDS.Tables[0].Rows[i]["JOB_CARD_AIR_IMP_CONT_FK"].ToString()) ? "" : gridDS.Tables[0].Rows[i]["JOB_CARD_AIR_IMP_CONT_FK"])).Direction = ParameterDirection.Input;
                            _with19.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                            //.ExecuteNonQuery()
                            var _with20 = objWK.MyDataAdapter;
                            _with20.InsertCommand = insCommand;
                            _with20.InsertCommand.Transaction = insertTrans;
                            _with20.InsertCommand.ExecuteNonQuery();
                            //End If
                            //End If
                        }
                    }
                }
                //arrMessage = objTrackNTrace.SaveTrackAndTrace(JobPk, BizType, 2, "DO", "DO-INS", CLng(HttpContext.Current.Session("LOGED_IN_LOC_FK")), objWK, "INS", HttpContext.Current.Session("USER_PK"), "O")
                insertTrans.Commit();
                return 0;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                insertTrans.Rollback();
                throw ex;
            }
            finally
            {
                objWK.MyCommand.Connection.Close();
            }
        }

        #endregion "SaveDOGrid"

        #region "save TrackAndTrace"

        public ArrayList SaveTrackAndTrace(OracleTransaction TRAN, int PkValue, int BizType, int Process, string Status, string OnStatus, int Locationfk, WorkFlow objWF, string flagInsUpd, long lngCreatedby,
        string PkStatus)
        {
            Int32 retVal = default(Int32);
            Int32 RecAfct = default(Int32);
            WorkFlow objWk = new WorkFlow();
            objWk.OpenConnection();
            OracleTransaction TRAN1 = null;
            TRAN1 = objWk.MyConnection.BeginTransaction();
            objWk.MyCommand.Transaction = TRAN1;
            try
            {
                //arrMessage.Clear()
                var _with21 = objWk.MyCommand;
                _with21.CommandType = CommandType.StoredProcedure;
                _with21.CommandText = objWk.MyUserName + ".TRACK_N_TRACE_PKG.TRACK_N_TRACE_INS";
                _with21.Transaction = TRAN1;
                _with21.Parameters.Clear();
                _with21.Parameters.Add("Key_fk_in", PkValue).Direction = ParameterDirection.Input;
                _with21.Parameters.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with21.Parameters.Add("PROCESS_IN", Process).Direction = ParameterDirection.Input;
                _with21.Parameters.Add("status_in", Status).Direction = ParameterDirection.Input;
                _with21.Parameters.Add("locationfk_in", Locationfk).Direction = ParameterDirection.Input;
                _with21.Parameters.Add("OnStatus_in", OnStatus).Direction = ParameterDirection.Input;
                _with21.Parameters.Add("pkStatus_in", PkStatus).Direction = ParameterDirection.Input;
                _with21.Parameters.Add("flagInsUpd_in", flagInsUpd).Direction = ParameterDirection.Input;
                _with21.Parameters.Add("Container_Data_in", "").Direction = ParameterDirection.Input;
                _with21.Parameters.Add("CreatedUser_in", lngCreatedby).Direction = ParameterDirection.Input;
                _with21.Parameters.Add("Return_value", OracleDbType.NVarchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with21.ExecuteNonQuery();
                TRAN1.Commit();
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWk.MyCommand.Connection.Close();
            }
            return new ArrayList();
        }

        #endregion "save TrackAndTrace"

        #region "fetch WareHouses"

        public DataSet fetchPickUpDepot(int BizType = 0)
        {
            WorkFlow objWK = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();
            try
            {
                strQuery.Append("   SELECT 0 VENDOR_MST_PK, ' ' VENDOR_NAME FROM DUAL ");
                strQuery.Append("  UNION ");
                strQuery.Append("   SELECT VMT.VENDOR_MST_PK, VMT.VENDOR_NAME ");
                strQuery.Append("   FROM VENDOR_MST_TBL      VMT, ");
                strQuery.Append("       VENDOR_SERVICES_TRN VST, ");
                strQuery.Append("       VENDOR_TYPE_MST_TBL VTYP, ");
                strQuery.Append("   VENDOR_CONTACT_DTLS VCTN ");
                strQuery.Append("   WHERE VMT.VENDOR_MST_PK = VCTN.VENDOR_MST_FK ");
                strQuery.Append("   AND VMT.ACTIVE=1 ");
                strQuery.Append("   AND VST.VENDOR_MST_FK = VMT.VENDOR_MST_PK ");
                strQuery.Append("   AND VST.VENDOR_TYPE_FK = VTYP.VENDOR_TYPE_PK ");
                strQuery.Append("   AND UPPER(VTYP.VENDOR_TYPE_ID) = 'WAREHOUSE' ");
                strQuery.Append("   AND VCTN.VENDOR_MST_FK = VST.VENDOR_MST_FK ");
                strQuery.Append("   AND VCTN.ADM_LOCATION_MST_FK=" + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
                if (BizType > 0)
                {
                    strQuery.Append("   AND VMT.BUSINESS_TYPE IN(" + BizType + ",3) ");
                }
                return (objWK.GetDataSet(strQuery.ToString()));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "fetch WareHouses"

        #region "fetch Report"

        public DataSet fetchReport(string deldtlPk, long BizType, long cargo_type = 0)
        {
            WorkFlow objWK = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();
            try
            {
                if (BizType == 2)
                {
                    strQuery.Append("      Select  distinct ");
                    strQuery.Append("      jc.jobcard_ref_no JOBREFNO, ");
                    //strQuery.Append("       hbl.hbl_ref_no HBLREFNO,  ")
                    //strQuery.Append("       mbl.mbl_ref_no MBLREFNO, ")
                    strQuery.Append("      JC.HBL_HAWB_REF_NO HBLREFNO, ");
                    strQuery.Append("       JC.MBL_MAWB_REF_NO MBLREFNO, ");
                    strQuery.Append("       JC.GOODS_DESCRIPTION, ");
                    strQuery.Append("       JC.MARKS_NUMBERS, ");
                    strQuery.Append("       vmst_pickup.vendor_name PICK_UP_DEPOT, ");
                    strQuery.Append("       vctn_pickup.adm_address_1 pickup_add1, ");
                    strQuery.Append("       vctn_pickup.adm_address_2 pickup_add2, ");
                    strQuery.Append("       vctn_pickup.adm_address_3 pickup_add3, ");
                    strQuery.Append("       vctn_pickup.adm_zip_code  pickup_zip, ");
                    strQuery.Append("       cty_pickup.country_name pickup_country, ");
                    strQuery.Append("       vctn_pickup.adm_phone     pickup_phone, ");
                    strQuery.Append("       vctn_pickup.adm_city      pickup_city, ");
                    strQuery.Append("       vctn_pickup.adm_fax_no    pickup_faxno, ");
                    strQuery.Append("       vctn_pickup.adm_email_id  pickup_email, ");
                    strQuery.Append("       vctn_pickup.adm_url       pickup_url, ");
                    if (cargo_type == 2)
                    {
                        strQuery.Append("       '' RETURN_DEPOT, ");
                        strQuery.Append("       '' return_add1, ");
                        strQuery.Append("       '' return_add2, ");
                        strQuery.Append("       '' return_add3, ");
                        strQuery.Append("       '' return_zip, ");
                        strQuery.Append("       '' return_phone, ");
                        strQuery.Append("       '' return_city, ");
                        strQuery.Append("       '' return_country, ");
                        strQuery.Append("       '' return_email, ");
                        strQuery.Append("       '' return_faxno, ");
                        strQuery.Append("       '' return_url, ");
                    }
                    else
                    {
                        strQuery.Append("       vmst_return.vendor_name RETURN_DEPOT, ");
                        strQuery.Append("       vctn_return.adm_address_1 return_add1, ");
                        strQuery.Append("       vctn_return.adm_address_2 return_add2, ");
                        strQuery.Append("       vctn_return.adm_address_3 return_add3, ");
                        strQuery.Append("       vctn_return.adm_zip_code  return_zip, ");
                        strQuery.Append("       vctn_return.adm_phone     return_phone, ");
                        strQuery.Append("       vctn_return.adm_city      return_city, ");
                        strQuery.Append("       cty_return.country_name return_country, ");
                        strQuery.Append("       vctn_return.adm_email_id  return_email, ");
                        strQuery.Append("       vctn_return.adm_fax_no    return_faxno, ");
                        strQuery.Append("       vctn_return.adm_url       return_url, ");
                    }

                    strQuery.Append("       jc.vessel_name            vessel_name, ");
                    strQuery.Append("       jc.voyage_flight_no                 voyage, ");
                    strQuery.Append("       pol.port_name             pol, ");
                    strQuery.Append("       pod.port_name             pod, ");
                    strQuery.Append("       domst.delivery_order_ref_no, ");
                    strQuery.Append("       domst.delivery_order_pk, ");
                    strQuery.Append("       to_char(domst.delivery_order_date, dateFormat ), ");
                    strQuery.Append("       to_char(domst.delivery_ord_valid_dt, dateFormat ), ");
                    strQuery.Append("       cust.customer_name cons_name, ");
                    strQuery.Append("       cdtls.adm_address_1 con_add1, ");
                    strQuery.Append("       cdtls.adm_address_2 con_add2, ");
                    strQuery.Append("       cdtls.adm_address_3 con_add3, ");
                    strQuery.Append("       cdtls.adm_city con_city,");
                    strQuery.Append("       cdtls.adm_zip_code  con_zip,");
                    strQuery.Append("       cdtls.adm_phone_no_1 con_phone, ");
                    strQuery.Append("       cdtls.adm_email_id   con_mailid, ");
                    strQuery.Append("       cty_con.country_name, ");
                    strQuery.Append("       dodtl.spl_inst, ");
                    //********
                    strQuery.Append("       to_char(jc.HBL_HAWB_DATE, dateFormat) hbl_date, ");
                    strQuery.Append("       to_char(jc.MBL_MAWB_DATE, dateFormat) mbl_date ");
                    //********
                    strQuery.Append(" from JOB_CARD_TRN    jc, ");
                    strQuery.Append("       hbl_exp_tbl             hbl, ");
                    strQuery.Append("       mbl_exp_tbl             mbl, ");
                    strQuery.Append("       commodity_group_mst_tbl cgmt, ");
                    strQuery.Append("       commodity_mst_tbl       cmt, ");
                    strQuery.Append("       job_trn_cont    jcnt, ");
                    //strQuery.Append("       job_trn_fd      jcfd, ")
                    strQuery.Append("       container_type_mst_tbl  cont, ");
                    strQuery.Append("       delivery_order_mst_tbl  domst, ");
                    strQuery.Append("       DELIVERY_ORDER_DTL_TBL  dodtl, ");
                    if (cargo_type != 2)
                    {
                        strQuery.Append("       vendor_mst_tbl          vmst_return, ");
                        strQuery.Append("       vendor_contact_dtls     vctn_return, ");
                        strQuery.Append("       country_mst_tbl         cty_return, ");
                    }
                    strQuery.Append("       vendor_mst_tbl          vmst_pickup, ");
                    strQuery.Append("       vendor_contact_dtls     vctn_pickup, ");
                    strQuery.Append("       country_mst_tbl         cty_pickup, ");
                    strQuery.Append("       port_mst_tbl            pol, ");
                    strQuery.Append("       port_mst_tbl            pod, ");
                    strQuery.Append("       CUSTOMER_CONTACT_DTLS CDTLS, ");
                    strQuery.Append("       customer_mst_tbl     cust, ");
                    strQuery.Append("       country_mst_tbl     cty_con ");
                    strQuery.Append("   where hbl.hbl_ref_no(+) = jc.HBL_HAWB_REF_NO ");
                    strQuery.Append("   and mbl.mbl_ref_no(+) = jc.MBL_MAWB_REF_NO ");
                    strQuery.Append("   and jc.commodity_group_fk = cgmt.commodity_group_pk(+) ");
                    strQuery.Append("   and jcnt.commodity_mst_fk = cmt.commodity_mst_pk(+) ");
                    strQuery.Append("   and jcnt.JOB_CARD_TRN_FK = jc.JOB_CARD_TRN_PK ");
                    strQuery.Append("   and jcnt.container_type_mst_fk=cont.container_type_mst_pk(+) ");
                    //strQuery.Append("   and jcfd.container_type_mst_fk = cont.container_type_mst_pk ")
                    //strQuery.Append("   and jcfd.JOB_CARD_TRN_FK = jc.JOB_CARD_TRN_PK ")
                    strQuery.Append("   and vmst_pickup.vendor_mst_pk = dodtl.pick_up_depot_fk ");
                    strQuery.Append("   and jc.JOB_CARD_TRN_PK = domst.job_card_mst_fk ");
                    strQuery.Append("   and domst.delivery_order_pk = dodtl.delivery_order_fk ");
                    strQuery.Append("   and DOMST.DELIVERY_ORDER_PK in ( " + deldtlPk + " ) ");
                    if (cargo_type != 2)
                    {
                        strQuery.Append("   and vmst_return.vendor_mst_pk(+) = dodtl.return_depot_fk ");
                        strQuery.Append("   and jcnt.container_number = dodtl.container_number ");
                        strQuery.Append("   and vmst_return.vendor_mst_pk = vctn_return.vendor_mst_fk(+) ");
                        strQuery.Append("   and vctn_return.adm_country_mst_fk = cty_return.country_mst_pk(+) ");
                        strQuery.Append("   and jc.cargo_type=1 ");
                        //strQuery.Append("   and jc.cargo_type=" & cargo_type & " ")
                    }
                    else
                    {
                        strQuery.Append("   and jc.cargo_type=2 ");
                        // strQuery.Append("   and jc.cargo_type=" & cargo_type & " ")
                    }
                    strQuery.Append("   and vmst_pickup.vendor_mst_pk = vctn_pickup.vendor_mst_fk ");
                    strQuery.Append("   and vctn_pickup.adm_country_mst_fk = cty_pickup.country_mst_pk ");
                    strQuery.Append("   and pol.port_mst_pk = jc.port_mst_pol_fk");
                    strQuery.Append("   and pod.port_mst_pk = jc.port_mst_pod_fk ");
                    strQuery.Append("   and cdtls.customer_mst_fk = jc.consignee_cust_mst_fk ");
                    strQuery.Append("   and cust.customer_mst_pk = jc.consignee_cust_mst_fk ");
                    strQuery.Append("   and cty_con.country_mst_pk = cdtls.adm_country_mst_fk ");
                }
                else
                {
                    strQuery.Append("       Select  distinct ");
                    strQuery.Append("       jc.jobcard_ref_no JOBREFNO, ");
                    strQuery.Append("       jc.HBL_HAWB_REF_NO HBLREFNO, ");
                    strQuery.Append("       jc.MBL_MAWB_REF_NO MBLREFNO, ");
                    strQuery.Append("       JC.GOODS_DESCRIPTION, ");
                    strQuery.Append("       JC.MARKS_NUMBERS, ");
                    strQuery.Append("       vmst_pickup.vendor_name PICK_UP_DEPOT, ");
                    strQuery.Append("       vctn_pickup.adm_address_1 pickup_add1, ");
                    strQuery.Append("       vctn_pickup.adm_address_2 pickup_add2, ");
                    strQuery.Append("       vctn_pickup.adm_address_3 pickup_add3, ");
                    strQuery.Append("       vctn_pickup.adm_zip_code pickup_zip, ");
                    strQuery.Append("       cty_pickup.country_name pickup_country, ");
                    strQuery.Append("       vctn_pickup.adm_phone pickup_phone, ");
                    strQuery.Append("       vctn_pickup.adm_city pickup_city, ");
                    strQuery.Append("       vctn_pickup.adm_fax_no pickup_faxno, ");
                    strQuery.Append("       vctn_pickup.adm_email_id pickup_email, ");
                    strQuery.Append("       vctn_pickup.adm_url pickup_url, ");
                    strQuery.Append("       vmst_return.vendor_name RETURN_DEPOT, ");
                    strQuery.Append("       vctn_return.adm_address_1 return_add1, ");
                    strQuery.Append("       vctn_return.adm_address_2 return_add2, ");
                    strQuery.Append("       vctn_return.adm_address_3 return_add3, ");
                    strQuery.Append("       vctn_return.adm_zip_code return_zip, ");
                    strQuery.Append("       vctn_return.adm_phone return_phone, ");
                    strQuery.Append("       vctn_return.adm_city return_city, ");
                    strQuery.Append("       cty_return.country_name return_country, ");
                    strQuery.Append("       vctn_return.adm_email_id return_email, ");
                    strQuery.Append("       vctn_return.adm_fax_no return_faxno, ");
                    strQuery.Append("       vctn_return.adm_url return_url, ");
                    strQuery.Append("       AMT.AIRLINE_NAME VESSEL_NAME, ");
                    strQuery.Append("       JC.VOYAGE_FLIGHT_NO VOYAGE, ");
                    strQuery.Append("       pol.port_name pol, ");
                    strQuery.Append("       pod.port_name pod, ");
                    strQuery.Append("       domst.delivery_order_ref_no, ");
                    strQuery.Append("       domst.delivery_order_pk, ");
                    strQuery.Append("       to_char(domst.delivery_order_date, dateformat), ");
                    strQuery.Append("       to_char(domst.delivery_ord_valid_dt, dateformat), ");
                    strQuery.Append("       cust.customer_name cons_name, ");
                    strQuery.Append("       cdtls.adm_address_1 con_add1, ");
                    strQuery.Append("       cdtls.adm_address_2 con_add2, ");
                    strQuery.Append("       cdtls.adm_address_3 con_add3, ");
                    strQuery.Append("       cdtls.adm_city con_city,");
                    strQuery.Append("       cdtls.adm_zip_code  con_zip,");
                    strQuery.Append("       cdtls.adm_phone_no_1 con_phone, ");
                    strQuery.Append("       cdtls.adm_email_id   con_mailid, ");
                    strQuery.Append("       cty_con.country_name, ");
                    strQuery.Append("       dodtl.spl_inst, ");
                    //********
                    strQuery.Append("       to_char(jc.HBL_HAWB_DATE, dateFormat) hbl_date, ");
                    strQuery.Append("       to_char(jc.MBL_MAWB_DATE, dateFormat) mbl_date ");
                    //********
                    strQuery.Append("  from  JOB_CARD_TRN    jc, ");
                    //strQuery.Append("       hawb_exp_tbl             hawb, ")
                    //strQuery.Append("       mawb_exp_tbl             mawb,  ")
                    strQuery.Append("       commodity_group_mst_tbl cgmt, ");
                    strQuery.Append("       commodity_mst_tbl       cmt, ");
                    strQuery.Append("       job_trn_cont    jcnt, ");
                    strQuery.Append("       PACK_TYPE_MST_TBL       PCT , ");
                    strQuery.Append("       delivery_order_mst_tbl  domst, ");
                    strQuery.Append("       DELIVERY_ORDER_DTL_TBL  dodtl, ");
                    strQuery.Append("       vendor_mst_tbl          vmst_pickup, ");
                    strQuery.Append("       vendor_mst_tbl          vmst_return, ");
                    strQuery.Append("       vendor_contact_dtls     vctn_pickup, ");
                    strQuery.Append("       vendor_contact_dtls     vctn_return, ");
                    strQuery.Append("       country_mst_tbl         cty_pickup, ");
                    strQuery.Append("       country_mst_tbl         cty_return, ");
                    strQuery.Append("       port_mst_tbl            pol, ");
                    strQuery.Append("       port_mst_tbl            pod, ");
                    strQuery.Append("       CUSTOMER_CONTACT_DTLS CDTLS, ");
                    strQuery.Append("       customer_mst_tbl     cust, ");
                    strQuery.Append("        AIRLINE_MST_TBL         AMT, ");
                    strQuery.Append("       country_mst_tbl     cty_con ");
                    //strQuery.Append("   where  hawb.hawb_ref_no = jc.hawb_ref_no ")
                    //strQuery.Append("   and mawb.mawb_ref_no(+) = jc.mawb_ref_no ")
                    strQuery.Append("   where jc.commodity_group_fk = cgmt.commodity_group_pk(+) ");
                    strQuery.Append("   and jcnt.commodity_mst_fk = cmt.commodity_mst_pk(+) ");
                    strQuery.Append("   and jcnt.JOB_CARD_TRN_FK = jc.JOB_CARD_TRN_PK(+) ");
                    strQuery.Append("   and vmst_pickup.vendor_mst_pk(+) = dodtl.pick_up_depot_fk ");
                    strQuery.Append("   and vmst_return.vendor_mst_pk(+) = dodtl.return_depot_fk ");
                    strQuery.Append("   AND JCNT.PACK_TYPE_MST_FK = PCT.PACK_TYPE_MST_PK(+) ");
                    //strQuery.Append("   AND JCNT.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK(+) ")
                    strQuery.Append("   and jc.JOB_CARD_TRN_PK = domst.job_card_air_mst_fk ");
                    strQuery.Append("   and domst.delivery_order_pk(+) = dodtl.delivery_order_fk ");
                    strQuery.Append("   and jcnt.job_trn_cont_pk = dodtl.job_card_air_imp_cont_fk(+) ");
                    strQuery.Append("   and DOMST.DELIVERY_ORDER_PK in ( " + deldtlPk + ") ");
                    strQuery.Append("   and vmst_pickup.vendor_mst_pk = vctn_pickup.vendor_mst_fk(+) ");
                    strQuery.Append("   and vmst_return.vendor_mst_pk = vctn_return.vendor_mst_fk(+) ");
                    strQuery.Append("   and vctn_pickup.adm_country_mst_fk = cty_pickup.country_mst_pk(+) ");
                    strQuery.Append("   and vctn_return.adm_country_mst_fk = cty_return.country_mst_pk(+) ");
                    strQuery.Append("   and pol.port_mst_pk(+) = jc.port_mst_pol_fk");
                    strQuery.Append("   and pod.port_mst_pk(+) = jc.port_mst_pod_fk ");
                    strQuery.Append("   and cdtls.customer_mst_fk(+) = jc.consignee_cust_mst_fk ");
                    strQuery.Append("   and cust.customer_mst_pk(+) = jc.consignee_cust_mst_fk ");
                    strQuery.Append("   and cty_con.country_mst_pk(+) = cdtls.adm_country_mst_fk ");
                    strQuery.Append("   AND JC.CARRIER_MST_FK = AMT.AIRLINE_MST_PK ");
                }

                return (objWK.GetDataSet(strQuery.ToString()));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public DataSet fetchContainers(string deldtlPk, long BizType, long Fcl_Lcl)
        {
            WorkFlow objWK = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();
            try
            {
                if (BizType == 2 & Fcl_Lcl == 2)
                {
                    strQuery.Append("   Select distinct ");
                    strQuery.Append("         rownum srno, ");
                    strQuery.Append("         jcnt.pack_count No_of_Pcs, ");
                    strQuery.Append("         jcnt.volume_in_cbm Volume, ");
                    strQuery.Append("         jcnt.gross_weight GWT,    ");
                    strQuery.Append("         to_char(dodtl.pick_up_date,dateformat) PICK_UP_DATE, ");
                    strQuery.Append("         to_char(dodtl.return_date,dateformat) RETURN_DATE, ");
                    strQuery.Append("         vmst_return.vendor_name RETURN_DEPOT, ");
                    strQuery.Append("         dodtl.delivery_order_dtl_pk, ");
                    strQuery.Append("         domst.delivery_order_pk     ");
                    strQuery.Append("   from  JOB_CARD_TRN    jc, ");
                    strQuery.Append("         hbl_exp_tbl             hbl, ");
                    strQuery.Append("         mbl_exp_tbl             mbl, ");
                    strQuery.Append("         commodity_group_mst_tbl cgmt, ");
                    strQuery.Append("         commodity_mst_tbl       cmt, ");
                    strQuery.Append("         job_trn_cont    jcnt, ");
                    strQuery.Append("         PACK_TYPE_MST_TBL       PCT ,  ");
                    strQuery.Append("         container_type_mst_tbl  cont, ");
                    strQuery.Append("         delivery_order_mst_tbl  domst, ");
                    strQuery.Append("         DELIVERY_ORDER_DTL_TBL  dodtl, ");
                    strQuery.Append("         vendor_mst_tbl          vmst_pickup, ");
                    strQuery.Append("         vendor_mst_tbl          vmst_return   ");
                    strQuery.Append("     where   hbl.hbl_ref_no(+) = jc.HBL_HAWB_REF_NO ");
                    strQuery.Append("     and mbl.mbl_ref_no(+) = jc.MBL_MAWB_REF_NO ");
                    strQuery.Append("     and jc.commodity_group_fk = cgmt.commodity_group_pk(+) ");
                    strQuery.Append("     and jcnt.commodity_mst_fk = cmt.commodity_mst_pk(+) ");
                    strQuery.Append("     and jcnt.JOB_CARD_TRN_FK = jc.JOB_CARD_TRN_PK ");
                    strQuery.Append("     and jcnt.container_type_mst_fk=cont.container_type_mst_pk(+) ");
                    strQuery.Append("     and vmst_pickup.vendor_mst_pk = dodtl.pick_up_depot_fk ");
                    strQuery.Append("     and vmst_return.vendor_mst_pk(+) = dodtl.return_depot_fk ");
                    strQuery.Append("     and jc.cargo_type=2 ");
                    strQuery.Append("     AND JCNT.PACK_TYPE_MST_FK = PCT.PACK_TYPE_MST_PK(+) ");
                    strQuery.Append("     AND JCNT.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK(+) ");
                    strQuery.Append("     and jc.JOB_CARD_TRN_PK = domst.job_card_mst_fk ");
                    strQuery.Append("     and domst.delivery_order_pk = dodtl.delivery_order_fk ");
                    strQuery.Append("     and jcnt.job_trn_cont_pk = dodtl.job_card_sea_imp_cont_fk ");
                    strQuery.Append("     and DOMST.DELIVERY_ORDER_PK in (" + deldtlPk + ")   ");
                }
                else if (BizType == 2 & Fcl_Lcl == 1)
                {
                    strQuery.Append("      SELECT ROWNUM SRNO,Q.* FROM ( Select  distinct ");
                    //strQuery.Append("       rownum srno, ")
                    strQuery.Append("       domst.delivery_order_pk, ");
                    strQuery.Append("       dodtl.delivery_order_dtl_pk, ");
                    strQuery.Append("       to_char(dodtl.pick_up_date,dateformat) PICK_UP_DATE, ");
                    strQuery.Append("       to_char(dodtl.return_date,dateformat) RETURN_DATE, ");
                    strQuery.Append("       vmst_return.vendor_name, ");
                    strQuery.Append("       JCNT.CONTAINER_NUMBER, ");
                    strQuery.Append("       cont.container_type_mst_id ");
                    strQuery.Append("   from JOB_CARD_TRN    jc, ");
                    strQuery.Append("       job_trn_cont    jcnt, ");
                    strQuery.Append("       container_type_mst_tbl  cont, ");
                    strQuery.Append("       delivery_order_mst_tbl  domst, ");
                    strQuery.Append("       DELIVERY_ORDER_DTL_TBL  dodtl, ");
                    strQuery.Append("       vendor_mst_tbl          vmst_return   ");
                    strQuery.Append("   where  jcnt.JOB_CARD_TRN_FK = jc.JOB_CARD_TRN_PK ");
                    strQuery.Append("   and jcnt.container_type_mst_fk=cont.container_type_mst_pk ");
                    strQuery.Append("   and vmst_return.vendor_mst_pk(+) = dodtl.return_depot_fk ");
                    strQuery.Append("   and jc.cargo_type=1 ");
                    strQuery.Append("   and jcnt.container_number = dodtl.container_number ");
                    strQuery.Append("   and jc.JOB_CARD_TRN_PK = domst.job_card_mst_fk ");
                    strQuery.Append("   and domst.delivery_order_pk = dodtl.delivery_order_fk ");
                    strQuery.Append("   and DOMST.DELIVERY_ORDER_PK in ( " + deldtlPk + ")  )Q ");
                }
                else if (BizType == 1 & Fcl_Lcl == 2)
                {
                    strQuery.Append("     Select  distinct ");
                    strQuery.Append("        rownum srno, ");
                    strQuery.Append("        stbl.breakpoint_id ULD, ");
                    strQuery.Append("        jcnt_exp.uld_number ULD_NR, ");
                    strQuery.Append("        to_char(dodtl.pick_up_date,dateformat) PICK_UP_DATE, ");
                    strQuery.Append("        vmst_pickup.vendor_name Pick_UP_Depot, ");
                    strQuery.Append("        to_char(dodtl.return_date,dateformat) RETURN_DATE, ");
                    strQuery.Append("        vmst_return.vendor_name RETURN_DEPOT, ");
                    strQuery.Append("        dodtl.delivery_order_dtl_pk, ");
                    strQuery.Append("        domst.delivery_order_pk ");
                    strQuery.Append("    from  ");
                    strQuery.Append("        JOB_CARD_TRN    jc, ");
                    strQuery.Append("        hawb_exp_tbl             hawb, ");
                    strQuery.Append("        commodity_group_mst_tbl cgmt, ");
                    strQuery.Append("        commodity_mst_tbl       cmt, ");
                    strQuery.Append("        job_trn_cont    jcnt, ");
                    strQuery.Append("        PACK_TYPE_MST_TBL       PCT ,  ");
                    strQuery.Append("        delivery_order_mst_tbl  domst, ");
                    strQuery.Append("        DELIVERY_ORDER_DTL_TBL  dodtl, ");
                    strQuery.Append("        vendor_mst_tbl          vmst_return, ");
                    strQuery.Append("        vendor_mst_tbl          vmst_pickup, ");
                    strQuery.Append("        booking_MST_tbl         bkg, ");
                    strQuery.Append("        JOB_CARD_TRN    jc_exp, ");
                    strQuery.Append("        job_trn_cont    jcnt_exp, ");
                    strQuery.Append("        airfreight_slabs_tbl    stbl  ");
                    strQuery.Append("    where  hawb.hawb_ref_no = jc.Hbl_Hawb_Ref_No ");
                    strQuery.Append("    and jc.commodity_group_fk = cgmt.commodity_group_pk(+) ");
                    strQuery.Append("    and jcnt.commodity_mst_fk = cmt.commodity_mst_pk(+) ");
                    strQuery.Append("    and jcnt.JOB_CARD_TRN_FK = jc.JOB_CARD_TRN_PK ");
                    strQuery.Append("    and jc_exp.hawb_exp_tbl_fk = hawb.hawb_exp_tbl_pk ");
                    strQuery.Append("    and hawb.hawb_ref_no = jc.hawb_ref_no ");
                    strQuery.Append("    and jcnt_exp.JOB_CARD_TRN_FK = jc_exp.JOB_CARD_TRN_PK ");
                    strQuery.Append("    and jc_exp.commodity_group_fk = cgmt.commodity_group_pk ");
                    strQuery.Append("    and jcnt_exp.airfreight_slabs_tbl_fk = stbl.airfreight_slabs_tbl_pk ");
                    strQuery.Append("    and stbl.breakpoint_type = '2' ");
                    strQuery.Append("    and bkg.booking_MST_PK = jc_exp.booking_MST_FK ");
                    strQuery.Append("    and bkg.cargo_type = 2 ");
                    strQuery.Append("    and vmst_return.vendor_mst_pk = dodtl.return_depot_fk ");
                    strQuery.Append("    and vmst_pickup.vendor_mst_pk = dodtl.pick_up_depot_fk ");
                    strQuery.Append("    AND JCNT.PACK_TYPE_MST_FK = PCT.PACK_TYPE_MST_PK(+) ");
                    strQuery.Append("    AND JCNT.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK(+) ");
                    strQuery.Append("    and jc.JOB_CARD_TRN_PK = domst.job_card_air_mst_fk ");
                    strQuery.Append("    and domst.delivery_order_pk = dodtl.delivery_order_fk ");
                    strQuery.Append("    and jcnt.job_trn_cont_pk = dodtl.job_card_air_imp_cont_fk ");
                    strQuery.Append("    and DOMST.DELIVERY_ORDER_PK in (" + deldtlPk + ") ");
                }
                else if (BizType == 1 & Fcl_Lcl == 1)
                {
                    strQuery.Append("   Select  distinct ");
                    strQuery.Append("        rownum srno, ");
                    strQuery.Append("        domst.delivery_order_pk, ");
                    strQuery.Append("        dodtl.delivery_order_dtl_pk, ");
                    strQuery.Append("        to_char(dodtl.pick_up_date,dateformat) PICK_UP_DATE, ");
                    strQuery.Append("        to_char(dodtl.return_date,dateformat) RETURN_DATE, ");
                    strQuery.Append("        vmst_return.vendor_name, ");
                    strQuery.Append("        jcnt.chargeable_weight,jcnt.volume_in_cbm, ");
                    strQuery.Append("        jcnt.pack_count, ");
                    strQuery.Append("        jcnt.gross_weight ");
                    strQuery.Append("  from  JOB_CARD_TRN    jc, ");
                    strQuery.Append("        job_trn_cont    jcnt, ");
                    strQuery.Append("        delivery_order_mst_tbl  domst, ");
                    strQuery.Append("        DELIVERY_ORDER_DTL_TBL  dodtl, ");
                    strQuery.Append("        vendor_mst_tbl          vmst_return  ");
                    strQuery.Append("   where jcnt.JOB_CARD_TRN_FK = jc.JOB_CARD_TRN_PK ");
                    strQuery.Append("   and jc.JOB_CARD_TRN_PK = domst.job_card_air_mst_fk ");
                    strQuery.Append("   and vmst_return.vendor_mst_pk(+) = dodtl.return_depot_fk ");
                    strQuery.Append("   and domst.delivery_order_pk = dodtl.delivery_order_fk ");
                    strQuery.Append("   and jcnt.job_trn_cont_pk = dodtl.job_card_air_imp_cont_fk ");
                    strQuery.Append("   and DOMST.DELIVERY_ORDER_PK in ( " + deldtlPk + " ) ");
                }
                return (objWK.GetDataSet(strQuery.ToString()));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //Public Function fetchQRY() As DataSet
        //    Dim objWK As New Business.WorkFlow
        //    Dim strQuery As String

        //    strQuery = "   select rownum srno,"
        //    strQuery = strQuery & "    Q.JOBREFNO ,"
        //    strQuery = strQuery & "    Q.BLREFNO,"
        //    strQuery = strQuery & "    Q.BLDATE,"
        //    strQuery = strQuery & "    Q.CONT_TYPE,"
        //    strQuery = strQuery & "    Q.CONT_NR,"
        //    strQuery = strQuery & "    Q.DISCHARGE_DATE,"
        //    strQuery = strQuery & "    Q.PICK_UP_DATE,"
        //    strQuery = strQuery & "    Q.PICK_UP_DATE,"
        //    strQuery = strQuery & "    Q.PICK_UP_DEPOT_FK,"
        //    strQuery = strQuery & "    Q.PICK_UP_DEPOT,"
        //    strQuery = strQuery & "    Q.RETURN_DATE,"
        //    strQuery = strQuery & "    Q.RETURN_DEPOT_FK,"
        //    strQuery = strQuery & "    Q.RETURN_DEPOT,"
        //    strQuery = strQuery & "    Q.SPLC_INST,"
        //    strQuery = strQuery & "    Q.FETCH_SPL_INST"
        //    strQuery = strQuery & "    FROM (select distinct  jc.jobcard_ref_no JOBREFNO,"
        //    strQuery = strQuery & "    (case when hbl.hbl_ref_no='' then"
        //    strQuery = strQuery & "       mbl.mbl_ref_no"
        //    strQuery = strQuery & "       else"
        //    strQuery = strQuery & "       hbl.hbl_ref_no"
        //    strQuery = strQuery & "        end)BLREFNO,"
        //    strQuery = strQuery & "      to_char((case when hbl.hbl_ref_no='' then"
        //    strQuery = strQuery & "       mbl.mbl_date"
        //    strQuery = strQuery & "       else"
        //    strQuery = strQuery & "       hbl.hbl_date"
        //    strQuery = strQuery & "        end), dateformat) BLDATE ,"
        //    strQuery = strQuery & "       cont.container_type_mst_id CONT_TYPE,"
        //    strQuery = strQuery & "       jcnt.container_number CONT_NR,"
        //    strQuery = strQuery & "       to_date(dodtl.discharge_date,dateformat) DISCHARGE_DATE,"
        //    strQuery = strQuery & "       to_date(dodtl.pick_up_date,dateformat) PICK_UP_DATE,"
        //    strQuery = strQuery & "       0 PICK_UP_DEPOT_FK,"
        //    strQuery = strQuery & "      '' PICK_UP_DEPOT,"
        //    strQuery = strQuery & "       to_date(dodtl.return_date,dateformat) RETURN_DATE,"
        //    strQuery = strQuery & "       0 RETURN_DEPOT_FK,"
        //    strQuery = strQuery & "      '' RETURN_DEPOT,"
        //    strQuery = strQuery & "      '' SPLC_INST,"
        //    strQuery = strQuery & "      '' FETCH_SPL_INST           "
        //    strQuery = strQuery & " from JOB_CARD_TRN    jc,"
        //    strQuery = strQuery & "       hbl_exp_tbl             hbl,"
        //    strQuery = strQuery & "       mbl_exp_tbl             mbl,"
        //    strQuery = strQuery & "       commodity_group_mst_tbl cgmt,"
        //    strQuery = strQuery & "       commodity_mst_tbl       cmt,"
        //    strQuery = strQuery & "       job_trn_cont    jcnt,"
        //    strQuery = strQuery & "       job_trn_fd      jcfd,"
        //    strQuery = strQuery & "       container_type_mst_tbl  cont,"
        //    strQuery = strQuery & "       delivery_order_mst_tbl  domst,"
        //    strQuery = strQuery & "       delivery_order_dtl_tbl  dodtl"
        //    strQuery = strQuery & "   where 1 = 1"
        //    strQuery = strQuery & "   and hbl.hbl_ref_no(+) = jc.hbl_ref_no"
        //    strQuery = strQuery & "   and mbl.mbl_ref_no(+) = jc.mbl_ref_no"
        //    strQuery = strQuery & "   and jc.commodity_group_fk = cgmt.commodity_group_pk"
        //    strQuery = strQuery & "   and jcnt.commodity_mst_fk = cmt.commodity_mst_pk(+)"
        //    strQuery = strQuery & "   and jcnt.JOB_CARD_TRN_FK = jc.JOB_CARD_TRN_PK"
        //    strQuery = strQuery & "   and jcnt.container_type_mst_fk=cont.container_type_mst_pk"
        //    strQuery = strQuery & "   and jcfd.container_type_mst_fk = cont.container_type_mst_pk"
        //    strQuery = strQuery & "   and jcfd.JOB_CARD_TRN_FK = jc.JOB_CARD_TRN_PK"
        //    strQuery = strQuery & "   and jc.cargo_type=1"
        //    strQuery = strQuery & "   and jc.JOB_CARD_TRN_PK = 627"
        //    strQuery = strQuery & "   and domst.delivery_order_pk = dodtl.delivery_order_fk(+)"
        //    strQuery = strQuery & "   and domst.job_card_mst_fk(+) = jc.JOB_CARD_TRN_PK"
        //    strQuery = strQuery & "   AND jcnt.container_number not in "
        //    strQuery = strQuery & "   ( select dodtl.container_number from"
        //    strQuery = strQuery & "     delivery_order_mst_tbl domst,"
        //    strQuery = strQuery & "     DELIVERY_ORDER_DTL_TBL dodtl"
        //    strQuery = strQuery & "     where domst.delivery_order_pk = dodtl.delivery_order_fk"
        //    strQuery = strQuery & "     and domst.job_card_mst_fk = 627"
        //    strQuery = strQuery & "   ))Q"
        //    Try
        //        Return (objWK.GetDataSet(strQuery.ToString()))
        //    Catch ex As Exception

        //    End Try
        //End Function

        #endregion "fetch Report"

        #region "fetch Report for Break Bulk"

        public DataSet fetchBBReport(string deldtlPk, long BizType, long cargo_type = 0)
        {
            WorkFlow objWK = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();
            try
            {
                if (BizType == 2)
                {
                    strQuery.Append("      Select  distinct ");
                    strQuery.Append("      jc.jobcard_ref_no JOBREFNO, ");
                    //strQuery.Append("       hbl.hbl_ref_no HBLREFNO,  ")
                    //strQuery.Append("       mbl.mbl_ref_no MBLREFNO, ")
                    strQuery.Append("      JC.HBL_HAWB_REF_NO HBLREFNO, ");
                    strQuery.Append("       JC.MBL_MAWB_REF_NO MBLREFNO, ");
                    strQuery.Append("       JC.GOODS_DESCRIPTION, ");
                    strQuery.Append("       JC.MARKS_NUMBERS, ");
                    strQuery.Append("       vmst_pickup.vendor_name PICK_UP_DEPOT, ");
                    strQuery.Append("       vctn_pickup.adm_address_1 pickup_add1, ");
                    strQuery.Append("       vctn_pickup.adm_address_2 pickup_add2, ");
                    strQuery.Append("       vctn_pickup.adm_address_3 pickup_add3, ");
                    strQuery.Append("       vctn_pickup.adm_zip_code  pickup_zip, ");
                    strQuery.Append("       cty_pickup.country_name pickup_country, ");
                    strQuery.Append("       vctn_pickup.adm_phone     pickup_phone, ");
                    strQuery.Append("       vctn_pickup.adm_city      pickup_city, ");
                    strQuery.Append("       vctn_pickup.adm_fax_no    pickup_faxno, ");
                    strQuery.Append("       vctn_pickup.adm_email_id  pickup_email, ");
                    strQuery.Append("       vctn_pickup.adm_url       pickup_url, ");
                    if (cargo_type == 4)
                    {
                        strQuery.Append("       '' RETURN_DEPOT, ");
                        strQuery.Append("       '' return_add1, ");
                        strQuery.Append("       '' return_add2, ");
                        strQuery.Append("       '' return_add3, ");
                        strQuery.Append("       '' return_zip, ");
                        strQuery.Append("       '' return_phone, ");
                        strQuery.Append("       '' return_city, ");
                        strQuery.Append("       '' return_country, ");
                        strQuery.Append("       '' return_email, ");
                        strQuery.Append("       '' return_faxno, ");
                        strQuery.Append("       '' return_url, ");
                    }
                    else
                    {
                        strQuery.Append("       vmst_return.vendor_name RETURN_DEPOT, ");
                        strQuery.Append("       vctn_return.adm_address_1 return_add1, ");
                        strQuery.Append("       vctn_return.adm_address_2 return_add2, ");
                        strQuery.Append("       vctn_return.adm_address_3 return_add3, ");
                        strQuery.Append("       vctn_return.adm_zip_code  return_zip, ");
                        strQuery.Append("       vctn_return.adm_phone     return_phone, ");
                        strQuery.Append("       vctn_return.adm_city      return_city, ");
                        strQuery.Append("       cty_return.country_name return_country, ");
                        strQuery.Append("       vctn_return.adm_email_id  return_email, ");
                        strQuery.Append("       vctn_return.adm_fax_no    return_faxno, ");
                        strQuery.Append("       vctn_return.adm_url       return_url, ");
                    }

                    strQuery.Append("       jc.vessel_name            vessel_name, ");
                    strQuery.Append("       jc.voyage_flight_no                 voyage, ");
                    strQuery.Append("       pol.port_name             pol, ");
                    strQuery.Append("       pod.port_name             pod, ");
                    strQuery.Append("       domst.delivery_order_ref_no, ");
                    strQuery.Append("       domst.delivery_order_pk, ");
                    strQuery.Append("       to_char(domst.delivery_order_date, dateFormat ), ");
                    strQuery.Append("       to_char(domst.delivery_ord_valid_dt, dateFormat ), ");
                    strQuery.Append("       cust.customer_name cons_name, ");
                    strQuery.Append("       cdtls.adm_address_1 con_add1, ");
                    strQuery.Append("       cdtls.adm_address_2 con_add2, ");
                    strQuery.Append("       cdtls.adm_address_3 con_add3, ");
                    strQuery.Append("       cdtls.adm_city con_city,");
                    strQuery.Append("       cdtls.adm_zip_code  con_zip,");
                    strQuery.Append("       cdtls.adm_phone_no_1 con_phone, ");
                    strQuery.Append("       cdtls.adm_email_id   con_mailid, ");
                    strQuery.Append("       cty_con.country_name, ");
                    strQuery.Append("       dodtl.spl_inst, ");
                    //********
                    strQuery.Append("       to_char(jc.HBL_HAWB_DATE, dateFormat) hbl_date, ");
                    strQuery.Append("       to_char(jc.MBL_MAWB_DATE, dateFormat) mbl_date ");
                    //********
                    strQuery.Append(" from JOB_CARD_TRN    jc, ");
                    strQuery.Append("       hbl_exp_tbl             hbl, ");
                    strQuery.Append("       mbl_exp_tbl             mbl, ");
                    strQuery.Append("       commodity_group_mst_tbl cgmt, ");
                    strQuery.Append("       commodity_mst_tbl       cmt, ");
                    strQuery.Append("       job_trn_cont    jcnt, ");
                    //strQuery.Append("       job_trn_fd      jcfd, ")
                    //strQuery.Append("       container_type_mst_tbl  cont, ")
                    strQuery.Append("       delivery_order_mst_tbl  domst, ");
                    strQuery.Append("       DELIVERY_ORDER_DTL_TBL  dodtl, ");
                    if (cargo_type != 4)
                    {
                        strQuery.Append("       vendor_mst_tbl          vmst_return, ");
                        strQuery.Append("       vendor_contact_dtls     vctn_return, ");
                        strQuery.Append("       country_mst_tbl         cty_return, ");
                    }
                    strQuery.Append("       vendor_mst_tbl          vmst_pickup, ");
                    strQuery.Append("       vendor_contact_dtls     vctn_pickup, ");
                    strQuery.Append("       country_mst_tbl         cty_pickup, ");
                    strQuery.Append("       port_mst_tbl            pol, ");
                    strQuery.Append("       port_mst_tbl            pod, ");
                    strQuery.Append("       CUSTOMER_CONTACT_DTLS CDTLS, ");
                    strQuery.Append("       customer_mst_tbl     cust, ");
                    strQuery.Append("       country_mst_tbl     cty_con ");
                    strQuery.Append("   where hbl.hbl_ref_no(+) = jc.HBL_HAWB_REF_NO ");
                    strQuery.Append("   and mbl.mbl_ref_no(+) = jc.MBL_MAWB_REF_NO ");
                    //strQuery.Append("   and jc.commodity_group_fk = cgmt.commodity_group_pk ")
                    strQuery.Append("   and cmt.commodity_group_fk = cgmt.commodity_group_pk ");
                    strQuery.Append("   and jcnt.commodity_mst_fk = cmt.commodity_mst_pk ");
                    strQuery.Append("   and jcnt.JOB_CARD_TRN_FK = jc.JOB_CARD_TRN_PK ");
                    //strQuery.Append("   and jcnt.container_type_mst_fk=cont.container_type_mst_pk ")
                    //strQuery.Append("   and jcfd.container_type_mst_fk = cont.container_type_mst_pk ")
                    //strQuery.Append("   and jcfd.JOB_CARD_TRN_FK = jc.JOB_CARD_TRN_PK ")
                    strQuery.Append("   and vmst_pickup.vendor_mst_pk(+) = dodtl.pick_up_depot_fk ");
                    strQuery.Append("   and jc.JOB_CARD_TRN_PK = domst.job_card_mst_fk ");
                    strQuery.Append("   and domst.delivery_order_pk = dodtl.delivery_order_fk ");
                    strQuery.Append("   and domst.DELIVERY_ORDER_PK in ( " + deldtlPk + " ) ");
                    if (cargo_type != 4)
                    {
                        strQuery.Append("   and vmst_return.vendor_mst_pk(+) = dodtl.return_depot_fk ");
                        strQuery.Append("   and jcnt.container_number = dodtl.container_number ");
                        strQuery.Append("   and vmst_return.vendor_mst_pk = vctn_return.vendor_mst_fk(+) ");
                        strQuery.Append("   and vctn_return.adm_country_mst_fk = cty_return.country_mst_pk(+) ");
                        strQuery.Append("   and jc.cargo_type=1 ");
                        //strQuery.Append("   and jc.cargo_type=" & cargo_type & " ")
                    }
                    else
                    {
                        strQuery.Append("   and jc.cargo_type=4 ");
                        //strQuery.Append("   and jc.cargo_type=" & cargo_type & " ")
                    }
                    strQuery.Append("   and vmst_pickup.vendor_mst_pk = vctn_pickup.vendor_mst_fk (+)");
                    strQuery.Append("   and vctn_pickup.adm_country_mst_fk = cty_pickup.country_mst_pk(+) ");
                    strQuery.Append("   and pol.port_mst_pk = jc.port_mst_pol_fk");
                    strQuery.Append("   and pod.port_mst_pk = jc.port_mst_pod_fk ");
                    strQuery.Append("   and cdtls.customer_mst_fk = jc.consignee_cust_mst_fk ");
                    strQuery.Append("   and cust.customer_mst_pk = jc.consignee_cust_mst_fk ");
                    strQuery.Append("   and cty_con.country_mst_pk = cdtls.adm_country_mst_fk ");
                }
                else
                {
                    strQuery.Append("       Select  distinct ");
                    strQuery.Append("       jc.jobcard_ref_no JOBREFNO, ");
                    strQuery.Append("       jc.Hbl_Hawb_Ref_No HBLREFNO, ");
                    strQuery.Append("       jc.Mbl_Mawb_Ref_No MBLREFNO, ");
                    strQuery.Append("       JC.GOODS_DESCRIPTION, ");
                    strQuery.Append("       JC.MARKS_NUMBERS, ");
                    strQuery.Append("       vmst_pickup.vendor_name PICK_UP_DEPOT, ");
                    strQuery.Append("       vctn_pickup.adm_address_1 pickup_add1, ");
                    strQuery.Append("       vctn_pickup.adm_address_2 pickup_add2, ");
                    strQuery.Append("       vctn_pickup.adm_address_3 pickup_add3, ");
                    strQuery.Append("       vctn_pickup.adm_zip_code pickup_zip, ");
                    strQuery.Append("       cty_pickup.country_name pickup_country, ");
                    strQuery.Append("       vctn_pickup.adm_phone pickup_phone, ");
                    strQuery.Append("       vctn_pickup.adm_city pickup_city, ");
                    strQuery.Append("       vctn_pickup.adm_fax_no pickup_faxno, ");
                    strQuery.Append("       vctn_pickup.adm_email_id pickup_email, ");
                    strQuery.Append("       vctn_pickup.adm_url pickup_url, ");
                    strQuery.Append("       vmst_return.vendor_name RETURN_DEPOT, ");
                    strQuery.Append("       vctn_return.adm_address_1 return_add1, ");
                    strQuery.Append("       vctn_return.adm_address_2 return_add2, ");
                    strQuery.Append("       vctn_return.adm_address_3 return_add3, ");
                    strQuery.Append("       vctn_return.adm_zip_code return_zip, ");
                    strQuery.Append("       vctn_return.adm_phone return_phone, ");
                    strQuery.Append("       vctn_return.adm_city return_city, ");
                    strQuery.Append("       cty_return.country_name return_country, ");
                    strQuery.Append("       vctn_return.adm_email_id return_email, ");
                    strQuery.Append("       vctn_return.adm_fax_no return_faxno, ");
                    strQuery.Append("       vctn_return.adm_url return_url, ");
                    strQuery.Append("       jc.VOYAGE_FLIGHT_NO vessel_name, ");
                    strQuery.Append("              ''  voyage, ");
                    strQuery.Append("       pol.port_name pol, ");
                    strQuery.Append("       pod.port_name pod, ");
                    strQuery.Append("       domst.delivery_order_ref_no, ");
                    strQuery.Append("       domst.delivery_order_pk, ");
                    strQuery.Append("       to_char(domst.delivery_order_date, dateformat), ");
                    strQuery.Append("       to_char(domst.delivery_ord_valid_dt, dateformat), ");
                    strQuery.Append("       cust.customer_name cons_name, ");
                    strQuery.Append("       cdtls.adm_address_1 con_add1, ");
                    strQuery.Append("       cdtls.adm_address_2 con_add2, ");
                    strQuery.Append("       cdtls.adm_address_3 con_add3, ");
                    strQuery.Append("       cdtls.adm_city con_city,");
                    strQuery.Append("       cdtls.adm_zip_code  con_zip,");
                    strQuery.Append("       cdtls.adm_phone_no_1 con_phone, ");
                    strQuery.Append("       cdtls.adm_email_id   con_mailid, ");
                    strQuery.Append("       cty_con.country_name, ");
                    strQuery.Append("       dodtl.spl_inst, ");
                    //********
                    strQuery.Append("       to_char(jc.HBL_HAWB_DATE, dateFormat) hbl_date, ");
                    strQuery.Append("       to_char(jc.MBL_MAWB_DATE, dateFormat) mbl_date ");
                    //********
                    strQuery.Append("  from  JOB_CARD_TRN    jc, ");
                    //strQuery.Append("       hawb_exp_tbl             hawb, ")
                    //strQuery.Append("       mawb_exp_tbl             mawb,  ")
                    strQuery.Append("       commodity_group_mst_tbl cgmt, ");
                    strQuery.Append("       commodity_mst_tbl       cmt, ");
                    strQuery.Append("       job_trn_cont    jcnt, ");
                    strQuery.Append("       PACK_TYPE_MST_TBL       PCT , ");
                    strQuery.Append("       delivery_order_mst_tbl  domst, ");
                    strQuery.Append("       DELIVERY_ORDER_DTL_TBL  dodtl, ");
                    strQuery.Append("       vendor_mst_tbl          vmst_pickup, ");
                    strQuery.Append("       vendor_mst_tbl          vmst_return, ");
                    strQuery.Append("       vendor_contact_dtls     vctn_pickup, ");
                    strQuery.Append("       vendor_contact_dtls     vctn_return, ");
                    strQuery.Append("       country_mst_tbl         cty_pickup, ");
                    strQuery.Append("       country_mst_tbl         cty_return, ");
                    strQuery.Append("       port_mst_tbl            pol, ");
                    strQuery.Append("       port_mst_tbl            pod, ");
                    strQuery.Append("       CUSTOMER_CONTACT_DTLS CDTLS, ");
                    strQuery.Append("       customer_mst_tbl     cust, ");
                    strQuery.Append("       country_mst_tbl     cty_con ");
                    //strQuery.Append("   where  hawb.hawb_ref_no = jc.hawb_ref_no ")
                    //strQuery.Append("   and mawb.mawb_ref_no(+) = jc.mawb_ref_no ")
                    strQuery.Append("   where jc.commodity_group_fk = cgmt.commodity_group_pk ");
                    strQuery.Append("   and jcnt.commodity_mst_fk = cmt.commodity_mst_pk(+) ");
                    strQuery.Append("   and jcnt.JOB_CARD_TRN_FK = jc.JOB_CARD_TRN_PK ");
                    strQuery.Append("   and vmst_pickup.vendor_mst_pk = dodtl.pick_up_depot_fk ");
                    strQuery.Append("   and vmst_return.vendor_mst_pk(+) = dodtl.return_depot_fk ");
                    strQuery.Append("   AND JCNT.PACK_TYPE_MST_FK = PCT.PACK_TYPE_MST_PK(+) ");
                    strQuery.Append("   AND JCNT.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK(+) ");
                    strQuery.Append("   and jc.JOB_CARD_TRN_PK = domst.job_card_air_mst_fk ");
                    strQuery.Append("   and domst.delivery_order_pk = dodtl.delivery_order_fk ");
                    strQuery.Append("   and jcnt.job_trn_cont_pk = dodtl.job_card_air_imp_cont_fk ");
                    strQuery.Append("   and dodtl.delivery_order_dtl_pk in ( " + deldtlPk + ") ");
                    strQuery.Append("   and vmst_pickup.vendor_mst_pk = vctn_pickup.vendor_mst_fk ");
                    strQuery.Append("   and vmst_return.vendor_mst_pk = vctn_return.vendor_mst_fk(+) ");
                    strQuery.Append("   and vctn_pickup.adm_country_mst_fk = cty_pickup.country_mst_pk ");
                    strQuery.Append("   and vctn_return.adm_country_mst_fk = cty_return.country_mst_pk(+) ");
                    strQuery.Append("   and pol.port_mst_pk = jc.port_mst_pol_fk");
                    strQuery.Append("   and pod.port_mst_pk = jc.port_mst_pod_fk ");
                    strQuery.Append("   and cdtls.customer_mst_fk = jc.consignee_cust_mst_fk ");
                    strQuery.Append("   and cust.customer_mst_pk = jc.consignee_cust_mst_fk ");
                    strQuery.Append("   and cty_con.country_mst_pk = cdtls.adm_country_mst_fk ");
                }

                return (objWK.GetDataSet(strQuery.ToString()));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "fetch Report for Break Bulk"

        #region "Fetch Commodity Details"

        public DataSet fetchCommDetails(string deldtlPk, long BizType, long Fcl_Lcl)
        {
            WorkFlow objWK = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                sb.Append("Select distinct rownum srno,");
                sb.Append("                cgmt.commodity_group_code,");
                sb.Append("                cmt.commodity_name,");
                sb.Append("                PCT.Pack_Type_Id,");
                sb.Append("                jcnt.pack_count quantity,");
                sb.Append("                jcnt.chargeable_weight Weight,");
                sb.Append("                jcnt.volume_in_cbm Volume,");
                //sb.Append("                to_char(dodtl.pick_up_date, dateformat) PICK_UP_DATE,")
                //sb.Append("                to_char(dodtl.return_date, dateformat) RETURN_DATE,")
                //sb.Append("                vmst_return.vendor_name RETURN_DEPOT,")
                sb.Append("                case when dodtl.pick_up_date is null then");
                sb.Append("                  ' '");
                sb.Append("                 else");
                sb.Append("                    to_char(dodtl.pick_up_date, dateformat)");
                sb.Append("                 end PICK_UP_DATE,");
                sb.Append("                 case when dodtl.return_date is null then");
                sb.Append("                   ' '");
                sb.Append("                   else");
                sb.Append("                      to_char(dodtl.return_date, dateformat) ");
                sb.Append("                 end RETURN_DATE,");
                sb.Append("                 case when vmst_return.vendor_name is null then");
                sb.Append("                   ' '");
                sb.Append("                   else ");
                sb.Append("                     vmst_return.vendor_name ");
                sb.Append("                end RETURN_DEPOT,");
                sb.Append("                dodtl.delivery_order_dtl_pk,");
                sb.Append("                domst.delivery_order_pk");
                sb.Append("  from JOB_CARD_TRN    jc,");
                sb.Append("       hbl_exp_tbl             hbl,");
                sb.Append("       mbl_exp_tbl             mbl,");
                sb.Append("       commodity_group_mst_tbl cgmt,");
                sb.Append("       commodity_mst_tbl       cmt,");
                sb.Append("       job_trn_cont    jcnt,");
                sb.Append("       PACK_TYPE_MST_TBL       PCT,");
                sb.Append("       delivery_order_mst_tbl  domst,");
                sb.Append("       DELIVERY_ORDER_DTL_TBL  dodtl,");
                sb.Append("       vendor_mst_tbl          vmst_pickup,");
                sb.Append("       vendor_mst_tbl          vmst_return");
                sb.Append(" where hbl.hbl_ref_no(+) = jc.HBL_HAWB_REF_NO");
                sb.Append("   and mbl.mbl_ref_no(+) = jc.MBL_MAWB_REF_NO");
                sb.Append("   and cmt.commodity_group_fk = cgmt.commodity_group_pk");
                sb.Append("   and jcnt.commodity_mst_fk = cmt.commodity_mst_pk");
                sb.Append("   and jcnt.JOB_CARD_TRN_FK = jc.JOB_CARD_TRN_PK");
                sb.Append("   and vmst_pickup.vendor_mst_pk(+) = dodtl.pick_up_depot_fk");
                sb.Append("   and vmst_return.vendor_mst_pk(+) = dodtl.return_depot_fk");
                sb.Append("   and jc.cargo_type = " + Fcl_Lcl + "");
                sb.Append("   AND JCNT.PACK_TYPE_MST_FK = PCT.PACK_TYPE_MST_PK");
                sb.Append("   and jc.JOB_CARD_TRN_PK = domst.job_card_mst_fk");
                sb.Append("   and domst.delivery_order_pk = dodtl.delivery_order_fk");
                sb.Append("   and jcnt.job_trn_cont_pk = dodtl.job_card_sea_imp_cont_fk");
                sb.Append("   and domst.delivery_order_pk in (" + deldtlPk + ")");
                return (objWK.GetDataSet(sb.ToString()));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Commodity Details"

        #region "DO LISTING FETCH QUERY"

        public DataSet FetchListQuery(Int16 BizPk, Int32 DelStatus, string del_nr = "", string jcrefnr = "", string mdonr = "", string DofromDt = "", string DovalidToDt = "", Int32 CustomerPk = 0, Int16 cargoType = 0, Int16 vesvoypk = 0,
        string flightNr = "", string ContainerNr = "", string CommGrp = "", string SearchType = "", int loc = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {
            WorkFlow objWK = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();
            Int32 last = 0;
            Int32 start = 0;
            Int32 TotalRecords = default(Int32);
            string strConditionSea = null;
            string strConditionAir = null;
            try
            {
                if (!string.IsNullOrEmpty(jcrefnr) & SearchType == "C")
                {
                    strConditionSea = strConditionSea + "  and UPPER(JCT.jobcard_ref_no) like '%" + jcrefnr.ToUpper().Replace("'", "''") + "%' ";
                }
                else if (!string.IsNullOrEmpty(jcrefnr) & SearchType == "S")
                {
                    strConditionSea = strConditionSea + "  and UPPER(JCT.jobcard_ref_no) like '" + jcrefnr.ToUpper().Replace("'", "''") + "%' ";
                }
                if (!string.IsNullOrEmpty(mdonr) & SearchType == "C")
                {
                    strConditionSea = strConditionSea + " and UPPER(domst.mdo_nr)like '%" + mdonr.ToUpper().Replace("'", "''") + "%'";
                }
                else if (!string.IsNullOrEmpty(mdonr) & SearchType == "S")
                {
                    strConditionSea = strConditionSea + " and UPPER(domst.mdo_nr)like '" + mdonr.ToUpper().Replace("'", "''") + "%'";
                }
                if (!string.IsNullOrEmpty(del_nr) & SearchType == "C")
                {
                    strConditionSea = strConditionSea + " and UPPER(domst.delivery_order_ref_no) like '%" + del_nr.ToUpper().Replace("'", "''") + "%'";
                }
                else if (!string.IsNullOrEmpty(del_nr) & SearchType == "S")
                {
                    strConditionSea = strConditionSea + " and UPPER(domst.delivery_order_ref_no) like '" + del_nr.ToUpper().Replace("'", "''") + "%' ";
                }
                if (CustomerPk != 0)
                {
                    strConditionSea = strConditionSea + " and JCT.consignee_cust_mst_fk =  " + CustomerPk + " ";
                }
                if (vesvoypk != 0)
                {
                    strConditionSea = strConditionSea + "  and JCT.voyage_trn_fk= " + vesvoypk + "  ";
                }
                if (cargoType != 0)
                {
                    strConditionSea = strConditionSea + " and domst.cargo_type =  " + cargoType + " ";
                }
                if (!string.IsNullOrEmpty(ContainerNr) & SearchType == "C")
                {
                    strConditionSea = strConditionSea + " and UPPER(dodtl.container_number) like '%" + ContainerNr.ToUpper().Replace("'", "''") + "%' ";
                }
                else if (!string.IsNullOrEmpty(ContainerNr) & SearchType == "S")
                {
                    strConditionSea = strConditionSea + " and UPPER(dodtl.container_number) like '" + ContainerNr.ToUpper().Replace("'", "''") + "%' ";
                }
                if (Convert.ToInt32(CommGrp) != 0)
                {
                    strConditionSea = strConditionSea + " AND CGMT.COMMODITY_GROUP_PK =  " + CommGrp + " ";
                }

                if (!string.IsNullOrEmpty(DofromDt) & !string.IsNullOrEmpty(DovalidToDt))
                {
                    strConditionSea = strConditionSea + " and to_date(domst.delivery_order_date,'" + dateFormat + "') ";
                    strConditionSea = strConditionSea + "  between  to_date( '" + DofromDt + "' ,'" + dateFormat + "') ";
                    strConditionSea = strConditionSea + " and  to_date('" + DovalidToDt + "' ,'" + dateFormat + "') ";
                }
                else if (!string.IsNullOrEmpty(DofromDt))
                {
                    strConditionSea = strConditionSea + " and to_date(domst.delivery_order_date,'" + dateFormat + "') >= to_date( '" + DofromDt + "' ,'" + dateFormat + "')";
                }
                else if (!string.IsNullOrEmpty(DovalidToDt))
                {
                    strConditionSea = strConditionSea + " and to_date(domst.delivery_order_date,'" + dateFormat + "') <= to_date( '" + DovalidToDt + "' , '" + dateFormat + "')";
                }

                //'Air
                if (!string.IsNullOrEmpty(jcrefnr) & SearchType == "C")
                {
                    strConditionAir = strConditionAir + "   and UPPER(JCT.jobcard_ref_no) like '%" + jcrefnr.ToUpper().Replace("'", "''") + "%'";
                }
                else if (!string.IsNullOrEmpty(jcrefnr) & SearchType == "S")
                {
                    strConditionAir = strConditionAir + "   and UPPER(JCT.jobcard_ref_no) like '" + jcrefnr.ToUpper().Replace("'", "''") + "%'";
                }
                if (!string.IsNullOrEmpty(mdonr) & SearchType == "C")
                {
                    strConditionAir = strConditionAir + " and UPPER(domst.mdo_nr) like '%" + mdonr.ToUpper().Replace("'", "''") + "%'";
                }
                else if (!string.IsNullOrEmpty(mdonr) & SearchType == "S")
                {
                    strConditionAir = strConditionAir + " and UPPER(domst.mdo_nr) like '" + mdonr.ToUpper().Replace("'", "''") + "%'";
                }
                if (!string.IsNullOrEmpty(del_nr) & SearchType == "C")
                {
                    strConditionAir = strConditionAir + " and UPPER(domst.delivery_order_ref_no) like '%" + del_nr.ToUpper().Replace("'", "''") + "%'";
                }
                else if (!string.IsNullOrEmpty(del_nr) & SearchType == "S")
                {
                    strConditionAir = strConditionAir + " and UPPER(domst.delivery_order_ref_no) like '" + del_nr.ToUpper().Replace("'", "''") + "%'";
                }
                if (CustomerPk != 0)
                {
                    strConditionAir = strConditionAir + " and JCT.consignee_cust_mst_fk =  " + CustomerPk + " ";
                }
                if (!string.IsNullOrEmpty(flightNr) & SearchType == "C")
                {
                    strConditionAir = strConditionAir + "  and UPPER(JCT.VOYAGE_FLIGHT_NO) like '%" + flightNr.ToUpper().Replace("'", "''") + "%'";
                }
                else if (!string.IsNullOrEmpty(flightNr) & SearchType == "S")
                {
                    strConditionAir = strConditionAir + "  and UPPER(JCT.VOYAGE_FLIGHT_NO) like '" + flightNr.ToUpper().Replace("'", "''") + "%'";
                }
                if (Convert.ToInt32(CommGrp) != 0)
                {
                    strConditionAir = strConditionAir + " AND CGMT.COMMODITY_GROUP_PK =  " + CommGrp + " ";
                }
                if (!string.IsNullOrEmpty(DofromDt) & !string.IsNullOrEmpty(DovalidToDt))
                {
                    strConditionAir = strConditionAir + " and to_date(domst.delivery_order_date,'" + dateFormat + "')";
                    strConditionAir = strConditionAir + "  between  to_date( '" + DofromDt + "' ,'" + dateFormat + "') ";
                    strConditionAir = strConditionAir + " and  to_date('" + DovalidToDt + "' ,'" + dateFormat + "') ";
                }
                else if (!string.IsNullOrEmpty(DofromDt))
                {
                    strConditionAir = strConditionAir + " and to_date(domst.delivery_order_date,'" + dateFormat + "') >= to_date( '" + DofromDt + "' ,'" + dateFormat + "') ";
                }
                else if (!string.IsNullOrEmpty(DovalidToDt))
                {
                    strConditionAir = strConditionAir + " and to_date(domst.delivery_order_date,'" + dateFormat + "') <= to_date( '" + DovalidToDt + "' , '" + dateFormat + "')";
                }
                if (!string.IsNullOrEmpty(ContainerNr) & SearchType == "C")
                {
                    strConditionAir = strConditionAir + " and UPPER(dodtl.container_number) like '%" + ContainerNr.ToUpper().Replace("'", "''") + "%' ";
                }
                else if (!string.IsNullOrEmpty(ContainerNr) & SearchType == "S")
                {
                    strConditionAir = strConditionAir + " and UPPER(dodtl.container_number) like '" + ContainerNr.ToUpper().Replace("'", "''") + "%' ";
                }
                //******
                strQuery.Append("  select rownum as Slnr,");
                strQuery.Append("    QRY.delivery_order_pk,");
                strQuery.Append("    QRY.delivery_order_ref_no,");
                strQuery.Append("    QRY.dodate,");
                strQuery.Append("    QRY.BIZTYPE,");
                strQuery.Append("    QRY.CARGO_TYPE,");
                strQuery.Append("    QRY.COMMGRP,");
                strQuery.Append("    QRY.customer_mst_pk,");
                strQuery.Append("    QRY.customer_name,");
                strQuery.Append("    QRY.jobcard_ref_no,");
                strQuery.Append("    QRY.mdo_nr,");
                strQuery.Append("    QRY.dovalidtill,");
                strQuery.Append("    QRY.JC_REF_PK,");
                strQuery.Append("    QRY.DO_STATUS ");
                strQuery.Append("  FROM (select distinct  domst.delivery_order_pk,");
                strQuery.Append("       domst.delivery_order_ref_no,");
                strQuery.Append("       to_char(domst.delivery_order_date ,'" + dateFormat + "') dodate,");
                strQuery.Append("       CASE WHEN JCT.BUSINESS_TYPE=2 THEN 'Sea' ELSE 'Air' END BIZTYPE ,");
                strQuery.Append("       cust.customer_mst_pk,");
                strQuery.Append("       cust.customer_name ,");
                strQuery.Append("       JCT.jobcard_ref_no,");
                strQuery.Append("       domst.mdo_nr,");
                strQuery.Append("       to_char(domst.delivery_ord_valid_dt ,'" + dateFormat + "') dovalidtill,");
                strQuery.Append("       JCT.JOB_CARD_TRN_PK JC_REF_PK,");
                strQuery.Append("       CASE WHEN JCT.BUSINESS_TYPE=1 THEN DECODE(DOMST.CARGO_TYPE, 1,'KGS',2,'ULD') ELSE  DECODE(DOMST.CARGO_TYPE,1,'FCL',2,'LCL', 4, 'BBC') END CARGO_TYPE,");
                strQuery.Append("       CGMT.COMMODITY_GROUP_CODE COMMGRP, ");
                strQuery.Append("       DECODE(DOMST.DO_STATUS,1,'Pending',2,'Delivered') DO_STATUS");
                strQuery.Append("  from delivery_order_mst_tbl domst,");
                strQuery.Append("  delivery_order_dtl_tbl dodtl,");
                strQuery.Append("  JOB_CARD_TRN JCT,");
                strQuery.Append("  customer_mst_tbl   cust,");
                strQuery.Append("  port_mst_tbl       pod,");
                strQuery.Append("  USER_MST_TBL UMT,");
                strQuery.Append("  COMMODITY_GROUP_MST_TBL CGMT");
                strQuery.Append("  where domst.delivery_order_pk = dodtl.delivery_order_fk");
                if (BizPk == 3)
                {
                    strQuery.Append("  AND (DOMST.JOB_CARD_MST_FK = JCT.JOB_CARD_TRN_PK OR DOMST.JOB_CARD_AIR_MST_FK = JCT.JOB_CARD_TRN_PK)  ");
                }
                else if (BizPk == 2)
                {
                    strQuery.Append("  and domst.job_card_mst_fk = JCT.JOB_CARD_TRN_PK");
                }
                else
                {
                    strQuery.Append("  and domst.JOB_CARD_AIR_MST_FK = JCT.JOB_CARD_TRN_PK");
                }
                strQuery.Append("  and JCT.consignee_cust_mst_fk = cust.customer_mst_pk");
                strQuery.Append("  AND JCT.CREATED_BY_FK=UMT.USER_MST_PK");
                strQuery.Append("  and JCT.port_mst_pod_fk=pod.port_mst_pk");

                if (BizPk != 3)
                {
                    strQuery.Append("  AND JCT.BUSINESS_TYPE = " + BizPk);
                }

                strQuery.Append("  AND CGMT.COMMODITY_GROUP_PK = JCT.COMMODITY_GROUP_FK ");

                strQuery.Append(" AND UMT.DEFAULT_LOCATION_FK IN ");
                strQuery.Append(" (SELECT LMT.LOCATION_MST_PK FROM LOCATION_MST_TBL LMT START WITH LMT.LOCATION_MST_PK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " CONNECT BY PRIOR LMT.LOCATION_MST_PK = LMT.REPORTING_TO_FK) ");

                strQuery.Append("  AND domst.DO_STATUS = " + DelStatus);
                strQuery.Append("  AND domst.ACTIVE = 1");
                strQuery.Append(strConditionSea);
                strQuery.Append(" ORDER BY TO_DATE(DODATE) DESC, delivery_order_ref_no desc  )QRY  ");

                string strSQL = null;
                string strSQL1 = null;
                strSQL = "select count(*) from (";
                strSQL += strQuery.ToString() + ")";
                TotalRecords = Convert.ToInt32(objWK.ExecuteScaler(strSQL));
                TotalPage = TotalRecords / RecordsPerPage;
                if (TotalRecords % RecordsPerPage != 0)
                {
                    TotalPage += 1;
                }
                if (CurrentPage > TotalPage)
                {
                    CurrentPage = 1;
                }
                if (TotalRecords == 0)
                {
                    CurrentPage = 0;
                }
                last = CurrentPage * RecordsPerPage;
                start = (CurrentPage - 1) * RecordsPerPage + 1;
                strSQL1 = " SELECT * FROM (";
                strSQL1 += strQuery.ToString();
                strSQL1 += " ) WHERE Slnr Between " + start + " and " + last;
                return objWK.GetDataSet(strSQL1);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "DO LISTING FETCH QUERY"

        #region "For Fetching DropDown Values From DataBase"

        public static DataSet FetchDropDownValues(string Flag = "", string ConfigID = "")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string ErrorMessage = null;
            sb.Append("SELECT T.DD_VALUE, T.DD_ID");
            sb.Append("  FROM QFOR_DROP_DOWN_TBL T");
            sb.Append(" WHERE T.DD_FLAG = '" + Flag + "'");
            sb.Append(" AND T.CONFIG_ID  = '" + ConfigID + "'");
            sb.Append("    ORDER BY T.DD_VALUE");
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        #endregion "For Fetching DropDown Values From DataBase"

        #region "DOValidationQry"

        public DataTable fetchCollectInfo(string jobpk, long BizType, long locpk)
        {
            WorkFlow objWK = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();
            try
            {
                if (BizType == 2)
                {
                    strQuery.Append("           SELECT QRY.* ");
                    strQuery.Append("           FROM (SELECT  T.* ");
                    strQuery.Append("           FROM (SELECT INV.CONSOL_INVOICE_PK PK, ");
                    strQuery.Append("           INV.INVOICE_REF_NO, ");
                    strQuery.Append("           CMT.CUSTOMER_NAME, ");
                    strQuery.Append("           INV.INVOICE_DATE, ");
                    strQuery.Append("           INV.NET_RECEIVABLE, ");
                    strQuery.Append("           NVL((select sum(ctrn.recd_amount_hdr_curr) ");
                    strQuery.Append("                 from collections_trn_tbl ctrn ");
                    strQuery.Append("                where ctrn.invoice_ref_nr like inv.invoice_ref_no), ");
                    strQuery.Append("               0) Recieved, ");
                    strQuery.Append("           NVL((INV.NET_RECEIVABLE - ");
                    strQuery.Append("               NVL((select sum(WMT.CREDIT_NOTE_AMT) ");
                    strQuery.Append("                      from cr_cust_sea_imp_tbl WMT ");
                    strQuery.Append("                     where WMT.INV_CUST_SEA_IMP_FK = ");
                    strQuery.Append("                           INV.CONSOL_INVOICE_PK), ");
                    strQuery.Append("                    0.00) - NVL((select sum(ctrn.recd_amount_hdr_curr) ");
                    strQuery.Append("                                   from collections_trn_tbl ctrn ");
                    strQuery.Append("                                  where ctrn.invoice_ref_nr like ");
                    strQuery.Append("                                        inv.invoice_ref_no), ");
                    strQuery.Append("                                 0.00)), ");
                    strQuery.Append("               0) Balance, ");
                    strQuery.Append("           CUMT.CURRENCY_ID, ");
                    strQuery.Append("           INV.INV_UNIQUE_REF_NR ");
                    strQuery.Append("      FROM CONSOL_INVOICE_TBL     INV, ");
                    strQuery.Append("           CONSOL_INVOICE_TRN_TBL INVTRN, ");
                    strQuery.Append("           JOB_CARD_TRN   JOB, ");
                    //strQuery.Append("           HBL_EXP_TBL            HBL, ")
                    strQuery.Append("           CUSTOMER_MST_TBL       CMT, ");
                    strQuery.Append("           CURRENCY_TYPE_MST_TBL  CUMT, ");
                    strQuery.Append("           USER_MST_TBL           UMT, ");
                    strQuery.Append("           VESSEL_VOYAGE_TRN      VTRN ");
                    strQuery.Append("       WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK ");
                    //strQuery.Append("       AND JOB.HBL_EXP_TBL_FK = HBL.HBL_EXP_TBL_PK(+) ")
                    strQuery.Append("       AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+) ");
                    strQuery.Append("       AND JOB.VOYAGE_TRN_FK = VTRN.VOYAGE_TRN_PK(+) ");
                    strQuery.Append("       AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+) ");
                    strQuery.Append("       AND UMT.DEFAULT_LOCATION_FK =" + locpk + "  ");
                    strQuery.Append("       AND INV.CREATED_BY_FK = UMT.USER_MST_PK ");
                    strQuery.Append("       AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK ");
                    strQuery.Append("       AND INV.PROCESS_TYPE = '2' ");
                    strQuery.Append("       AND INV.BUSINESS_TYPE = '2' ");
                    strQuery.Append("       AND INVTRN.JOB_CARD_FK in (" + jobpk + ")  ");
                    strQuery.Append("       GROUP BY INV.CONSOL_INVOICE_PK, ");
                    strQuery.Append("              INV.INVOICE_REF_NO, ");
                    strQuery.Append("              INV.INVOICE_DATE, ");
                    strQuery.Append("              CUMT.CURRENCY_ID, ");
                    strQuery.Append("              CMT.CUSTOMER_NAME, ");
                    strQuery.Append("              INV.NET_RECEIVABLE, ");
                    strQuery.Append("              INV.CREATED_DT, ");
                    strQuery.Append("              INV.INV_UNIQUE_REF_NR ");
                    strQuery.Append("       ORDER BY INV.CREATED_DT DESC) T) QRY ");
                }
                else
                {
                    strQuery.Append("     SELECT QRY.* ");
                    strQuery.Append("       FROM (SELECT T.*");
                    strQuery.Append("               FROM (SELECT INV.CONSOL_INVOICE_PK PK,");
                    strQuery.Append("                            INV.INVOICE_REF_NO,");
                    strQuery.Append("                            CMT.CUSTOMER_NAME,");
                    strQuery.Append("                            INV.INVOICE_DATE,");
                    strQuery.Append("                            INV.NET_RECEIVABLE,");
                    strQuery.Append("                            NVL((select sum(ctrn.recd_amount_hdr_curr)");
                    strQuery.Append("                                  from collections_trn_tbl ctrn,");
                    strQuery.Append("                                  collections_tbl cmst");
                    strQuery.Append("                                  where ctrn.collections_tbl_fk = cmst.collections_tbl_pk ");
                    strQuery.Append("                                  and cmst.business_type = 1");
                    strQuery.Append("                                  and cmst.process_type =2");
                    strQuery.Append("                                 and ctrn.invoice_ref_nr like inv.invoice_ref_no),");
                    strQuery.Append("                                0) Recieved,");
                    strQuery.Append("                             NVL((INV.NET_RECEIVABLE  -");
                    strQuery.Append("                             NVL((select sum(WMT.CREDIT_NOTE_AMT)");
                    strQuery.Append("                                       from cr_cust_air_imp_tbl WMT");
                    strQuery.Append("                                      where WMT.INV_CUST_AIR_IMP_FK = ");
                    strQuery.Append("                                      INV.CONSOL_INVOICE_PK");
                    strQuery.Append("                                     ), 0.00) - ");
                    strQuery.Append("                             NVL((select sum(ctrn.recd_amount_hdr_curr)");
                    strQuery.Append("                                      from collections_trn_tbl ctrn");
                    strQuery.Append("                                      where ctrn.invoice_ref_nr like");
                    strQuery.Append("                                      inv.invoice_ref_no),");
                    strQuery.Append("                                      0.00)), 0) Balance,");
                    strQuery.Append("                            CUMT.CURRENCY_ID,");
                    strQuery.Append("                            INV.INV_UNIQUE_REF_NR");
                    strQuery.Append("                       FROM CONSOL_INVOICE_TBL     INV,");
                    strQuery.Append("                            CONSOL_INVOICE_TRN_TBL INVTRN,");
                    strQuery.Append("                            JOB_CARD_TRN   JOB,");
                    strQuery.Append("                            CUSTOMER_MST_TBL       CMT,");
                    strQuery.Append("                            CURRENCY_TYPE_MST_TBL  CUMT,");
                    strQuery.Append("                            USER_MST_TBL           UMT");
                    strQuery.Append("                     WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK");
                    strQuery.Append("                        AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
                    strQuery.Append("                        AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                    strQuery.Append("                        AND UMT.DEFAULT_LOCATION_FK =" + locpk + "  ");
                    strQuery.Append("                        AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
                    strQuery.Append("                        AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
                    strQuery.Append("                        AND INV.PROCESS_TYPE = '2'");
                    strQuery.Append("                        AND INV.BUSINESS_TYPE = '1'");
                    strQuery.Append("                        AND INVTRN.JOB_CARD_FK in (" + jobpk + ")  ");
                    strQuery.Append("                    GROUP BY INV.CONSOL_INVOICE_PK,");
                    strQuery.Append("                               INV.INVOICE_REF_NO,");
                    strQuery.Append("                               INV.INVOICE_DATE,");
                    strQuery.Append("                               CUMT.CURRENCY_ID,");
                    strQuery.Append("                               CMT.CUSTOMER_NAME,");
                    strQuery.Append("                               INV.NET_RECEIVABLE,");
                    strQuery.Append("                               INV.CREATED_DT,");
                    strQuery.Append("                               INV.INV_UNIQUE_REF_NR");
                    strQuery.Append("                      ORDER BY INV.CREATED_DT DESC) T) QRY");
                }
                return (objWK.GetDataTable(strQuery.ToString()));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public DataTable fetchInvoiceInfo(string jobpk, long BizType, long locpk)
        {
            WorkFlow objWK = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();
            try
            {
                if (BizType == 2)
                {
                    strQuery.Append("      select distinct jcf.freight_element_mst_fk, ");
                    strQuery.Append("                      ft.freight_element_id, ");
                    strQuery.Append("                      jc.jobcard_ref_no, jcf.freight_amt ");
                    strQuery.Append("      from  JOB_CARD_TRN jc, ");
                    strQuery.Append("             job_trn_fd jcf, ");
                    strQuery.Append("             freight_element_mst_tbl ft ");
                    strQuery.Append("      where jc.JOB_CARD_TRN_PK = jcf.JOB_CARD_TRN_FK ");
                    strQuery.Append("      and ft.freight_element_mst_pk = jcf.freight_element_mst_fk ");
                    strQuery.Append("      and jcf.freight_element_mst_fk not in   ");
                    strQuery.Append("      (select jfrt.freight_element_mst_fk ");
                    strQuery.Append("      from JOB_CARD_TRN    jcimp, ");
                    strQuery.Append("           job_trn_fd      jfrt, ");
                    strQuery.Append("           consol_invoice_tbl      cinv, ");
                    strQuery.Append("           consol_invoice_trn_tbl  cintrn, ");
                    strQuery.Append("           freight_element_mst_tbl frt ");
                    strQuery.Append("     where jcimp.JOB_CARD_TRN_PK = jfrt.JOB_CARD_TRN_FK ");
                    strQuery.Append("       and cinv.consol_invoice_pk = cintrn.consol_invoice_fk ");
                    strQuery.Append("       and cintrn.job_card_fk = jcimp.JOB_CARD_TRN_PK ");
                    strQuery.Append("       and cintrn.frt_oth_element_fk = jfrt.freight_element_mst_fk ");
                    strQuery.Append("       and cinv.process_type = 2  ");
                    strQuery.Append("       and cinv.business_type  = 2 ");
                    strQuery.Append("       and frt.freight_element_mst_pk = ");
                    strQuery.Append("           jfrt.freight_element_mst_fk ");
                    strQuery.Append("       and jcimp.JOB_CARD_TRN_PK = jc.JOB_CARD_TRN_PK) ");
                    strQuery.Append("       and jc.JOB_CARD_TRN_PK in (" + jobpk + ")  ");
                }
                else
                {
                    strQuery.Append("      select distinct jcf.freight_element_mst_fk, ");
                    strQuery.Append("                      ft.freight_element_id, ");
                    strQuery.Append("                      jc.jobcard_ref_no ");
                    strQuery.Append("      from  JOB_CARD_TRN jc, ");
                    strQuery.Append("             job_trn_fd jcf, ");
                    strQuery.Append("             freight_element_mst_tbl ft ");
                    strQuery.Append("      where jc.JOB_CARD_TRN_PK = jcf.JOB_CARD_TRN_FK ");
                    strQuery.Append("      and ft.freight_element_mst_pk = jcf.freight_element_mst_fk ");
                    strQuery.Append("      and jcf.freight_element_mst_fk not in   ");

                    strQuery.Append("      (select jfrt.freight_element_mst_fk ");
                    strQuery.Append("      from JOB_CARD_TRN    jcimp, ");
                    strQuery.Append("           job_trn_fd      jfrt, ");
                    strQuery.Append("           consol_invoice_tbl      cinv, ");
                    strQuery.Append("           consol_invoice_trn_tbl  cintrn, ");
                    strQuery.Append("           freight_element_mst_tbl frt ");
                    strQuery.Append("      where jcimp.JOB_CARD_TRN_PK = jfrt.JOB_CARD_TRN_FK ");
                    strQuery.Append("       and cinv.consol_invoice_pk = cintrn.consol_invoice_fk ");
                    strQuery.Append("       and cintrn.job_card_fk = jcimp.JOB_CARD_TRN_PK ");
                    strQuery.Append("       and cintrn.frt_oth_element_fk = jfrt.freight_element_mst_fk ");
                    strQuery.Append("       and frt.freight_element_mst_pk = jfrt.freight_element_mst_fk ");
                    strQuery.Append("       and cinv.process_type = 2 ");
                    strQuery.Append("       and cinv.business_type  = 1 ");
                    strQuery.Append("       and jcimp.JOB_CARD_TRN_PK = jc.JOB_CARD_TRN_PK) ");
                    strQuery.Append("       and jc.JOB_CARD_TRN_PK in (" + jobpk + ")  ");
                }
                return (objWK.GetDataTable(strQuery.ToString()));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public DataTable displayOutstanding(string JobPK, int BizType)
        {
            WorkFlow objWK = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();

            try
            {
                if (BizType == 2)
                {
                    strQuery.Append(" SELECT DISTINCT '', CIT.INVOICE_REF_NO, TO_CHAR(CIT.INVOICE_DATE, dateformat), ");
                    //Modified by Faheem
                    //strQuery.Append(" NVL(CIT.NET_RECEIVABLE, 0) - NVL(CTT.RECD_AMOUNT_HDR_CURR, 0) OUTSTANDING, ")
                    strQuery.Append(" DECODE(NVL(CIT.NET_RECEIVABLE, 0) -");
                    strQuery.Append("                NVL((SELECT SUM(CL.RECD_AMOUNT_HDR_CURR)");
                    strQuery.Append("                      FROM COLLECTIONS_TRN_TBL CL");
                    strQuery.Append("                     WHERE CL.INVOICE_REF_NR = CIT.INVOICE_REF_NO),");
                    strQuery.Append("                    0),0,'0.00') OUTSTANDING,");
                    //End
                    strQuery.Append(" CUR.CURRENCY_ID FROM JOB_CARD_TRN JCS INNER JOIN ");
                    strQuery.Append(" CONSOL_INVOICE_TRN_TBL CITT ON CITT.JOB_CARD_FK = JCS.JOB_CARD_TRN_PK ");
                    strQuery.Append(" INNER JOIN CONSOL_INVOICE_TBL CIT ON CIT.CONSOL_INVOICE_PK = CITT.CONSOL_INVOICE_FK ");
                    strQuery.Append(" INNER JOIN CURRENCY_TYPE_MST_TBL CUR ON CUR.CURRENCY_MST_PK = CIT.CURRENCY_MST_FK ");
                    strQuery.Append(" LEFT OUTER JOIN COLLECTIONS_TRN_TBL CTT ON CTT.INVOICE_REF_NR = CIT.INVOICE_REF_NO ");
                    strQuery.Append(" WHERE JCS.JOBCARD_REF_NO = '" + JobPK + "' AND CIT.BUSINESS_TYPE = " + BizType);
                }
                else
                {
                    strQuery.Append(" SELECT DISTINCT '', CIT.INVOICE_REF_NO, TO_CHAR(CIT.INVOICE_DATE, dateformat), ");
                    //Modified by Faheem
                    //strQuery.Append(" NVL(CIT.NET_RECEIVABLE, 0) - NVL(CTT.RECD_AMOUNT_HDR_CURR, 0) OUTSTANDING, ")
                    strQuery.Append(" DECODE(NVL(CIT.NET_RECEIVABLE, 0) -");
                    strQuery.Append("                NVL((SELECT SUM(CL.RECD_AMOUNT_HDR_CURR)");
                    strQuery.Append("                      FROM COLLECTIONS_TRN_TBL CL");
                    strQuery.Append("                     WHERE CL.INVOICE_REF_NR = CIT.INVOICE_REF_NO),");
                    strQuery.Append("                    0),0,'0.00') OUTSTANDING,");
                    //End
                    strQuery.Append(" CUR.CURRENCY_ID FROM JOB_CARD_TRN JCA INNER JOIN ");
                    strQuery.Append(" CONSOL_INVOICE_TRN_TBL CITT ON CITT.JOB_CARD_FK = JCA.JOB_CARD_TRN_PK ");
                    strQuery.Append(" INNER JOIN CONSOL_INVOICE_TBL CIT ON CIT.CONSOL_INVOICE_PK = CITT.CONSOL_INVOICE_FK ");
                    strQuery.Append(" INNER JOIN CURRENCY_TYPE_MST_TBL CUR ON CUR.CURRENCY_MST_PK = CIT.CURRENCY_MST_FK ");
                    strQuery.Append(" LEFT OUTER JOIN COLLECTIONS_TRN_TBL CTT ON CTT.INVOICE_REF_NR = CIT.INVOICE_REF_NO ");
                    strQuery.Append(" WHERE JCA.JOBCARD_REF_NO = '" + JobPK + "' AND CIT.BUSINESS_TYPE = " + BizType);
                }

                return (objWK.GetDataTable(strQuery.ToString()));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public DataTable fetchHblInfo(string jobpk, long BizType, long locpk)
        {
            WorkFlow objWK = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();
            try
            {
                if (BizType == 2)
                {
                    strQuery.Append("    select jc.HBL_HAWB_SURRENDERED hbl_surrendered, jc.MBL_MAWB_SURRENDERED mbl_surrendered ");
                    strQuery.Append("   from JOB_CARD_TRN jc ");
                    strQuery.Append("    where jc.JOB_CARD_TRN_PK in (" + jobpk + ")  ");
                }
                else
                {
                    strQuery.Append(" select jc.HBL_HAWB_SURRDT hawb_surrdt, jc.MBL_MAWB_SURRDT mawb_surrdt  ");
                    strQuery.Append(" from JOB_CARD_TRN jc");
                    strQuery.Append(" where jc.JOB_CARD_TRN_PK in (" + jobpk + ")  ");
                }
                return (objWK.GetDataTable(strQuery.ToString()));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public string fetchCargoType(string jobpk)
        {
            WorkFlow objWK = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();
            try
            {
                strQuery.Append(" SELECT  distinct BKG.CARGO_TYPE  FROM ");
                strQuery.Append(" JOB_CARD_TRN JCIMP,");
                strQuery.Append(" JOB_CARD_TRN JCEXP,");
                strQuery.Append("  hawb_exp_tbl h,");
                strQuery.Append(" booking_MST_tbl BKG");
                strQuery.Append(" WHERE JCIMP.HBL_HAWB_REF_NO = h.hawb_ref_no");
                strQuery.Append(" AND h.hawb_exp_tbl_pk = JCEXP.HBL_HAWB_FK");
                strQuery.Append(" AND JCEXP.booking_MST_FK = BKG.booking_MST_PK");
                strQuery.Append("  and jcimp.JOB_CARD_TRN_PK = " + jobpk + " ");
                return (objWK.ExecuteScaler(strQuery.ToString()));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "DOValidationQry"

        #region "Fetch Outstanding Amount"

        public DataSet Fetch_OutStand(int CustPk, string JobNo)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();
            string strSQL = null;
            string strSQL1 = null;
            Int16 BizType = default(Int16);

            strSQL1 = "SELECT CMST.BUSINESS_TYPE FROM CUSTOMER_MST_TBL CMST WHERE CMST.CUSTOMER_MST_PK = " + CustPk;
            BizType = Convert.ToInt16(objWF.ExecuteScaler(strSQL1.ToString()));

            try
            {
                if (BizType == 2 | BizType == 0 | BizType == 1)
                {
                    strSQL = "SELECT ROWNUM SR_NO , A.JOBCARD_REF_NO,A.JOBCARD_DATE,A.INVOICE_REF_NO,A.INVOICE_DATE,A.DUEDATE,A.CURRENCY_ID,A.INVOICE,A.INVOICE_AMT - A.OUT_AMOUNT Outstanding,";
                    strSQL = strSQL + " case when (A.INVOICE_AMT-A.OUT_AMOUNT) <= 0 then 0 else A.OUTSTD_DAYS end OUTSTD_DAYS from  (";
                    strSQL = strSQL + " select JOBCARD_REF_NO,JOBCARD_DATE,INVOICE_REF_NO,INVOICE_DATE,DUEDATE,CURRENCY_ID,INVOICE,INVOICE_AMT,sum(OUT_AMOUNT) OUT_AMOUNT,OUTSTD_DAYS from (";

                    strSQL = strSQL + " SELECT JOB.JOBCARD_REF_NO,";
                    strSQL = strSQL + " TO_CHAR(TO_DATE(JOB.JOBCARD_DATE),'DD/MM/YYYY') JOBCARD_DATE,";
                    strSQL = strSQL + " CIT.INVOICE_REF_NO,";
                    strSQL = strSQL + " TO_CHAR(TO_DATE(CIT.INVOICE_DATE),'DD/MM/YYYY') INVOICE_DATE,";
                    strSQL = strSQL + " TO_CHAR(TO_DATE(CIT.INVOICE_DATE) + nvl(cust.credit_days, 0),'DD/MM/YYYY') DUEDATE,";
                    strSQL = strSQL + " CURR.CURRENCY_ID,";
                    strSQL = strSQL + " CIT.INVOICE_AMT * GET_EX_RATE(CIT.CURRENCY_MST_FK, " + HttpContext.Current.Session["currency_mst_pk"] + ",CIT.INVOICE_DATE) INVOICE,";
                    strSQL = strSQL + " ((CIT.INVOICE_AMT * GET_EX_RATE(CIT.CURRENCY_MST_FK, " + HttpContext.Current.Session["currency_mst_pk"] + ", CIT.INVOICE_DATE)) - ";
                    strSQL = strSQL + " (nvl(CIT.TOTAL_CREDIT_NOTE_AMT, 0) * GET_EX_RATE(CIT.CURRENCY_MST_FK, " + HttpContext.Current.Session["currency_mst_pk"] + ", CIT.INVOICE_DATE)))INVOICE_AMT,";
                    strSQL = strSQL + " (nvl(COLT.RECD_AMOUNT_HDR_CURR, 0) * GET_EX_RATE(Col.CURRENCY_MST_FK," + HttpContext.Current.Session["currency_mst_pk"] + ",Col.Collections_Date)) OUT_AMOUNT,";

                    strSQL = strSQL + " ceil(NVL(TO_DATE(sysdate,'DD/MM/YYYY') - (cit.invoice_date + nvl(cust.credit_days,0)),0)) OUTSTD_DAYS";
                    strSQL = strSQL + "  FROM";
                    strSQL = strSQL + " consol_invoice_tbl CIT,";
                    strSQL = strSQL + " consol_invoice_trn_tbl CITT,";
                    if (BizType == 2 | BizType == 0)
                    {
                        strSQL = strSQL + " JOB_CARD_TRN job,";
                        strSQL = strSQL + " booking_MST_tbl book,";
                    }
                    else if (BizType == 1)
                    {
                        strSQL = strSQL + " JOB_CARD_TRN job,";
                        strSQL = strSQL + " booking_MST_tbl book,";
                    }
                    strSQL = strSQL + " customer_mst_tbl cust,";
                    strSQL = strSQL + " CURRENCY_TYPE_MST_TBL CURR,";
                    strSQL = strSQL + " collections_tbl        col,";
                    strSQL = strSQL + " collections_trn_tbl    colt";
                    strSQL = strSQL + " WHERE";
                    strSQL = strSQL + " cit.consol_invoice_pk = citt.consol_invoice_fk";
                    if (BizType == 2 | BizType == 0)
                    {
                        strSQL = strSQL + " AND citt.job_card_fk = job.JOB_CARD_TRN_PK";
                        strSQL = strSQL + " AND job.booking_MST_FK = book.booking_MST_PK";
                    }
                    else if (BizType == 1)
                    {
                        strSQL = strSQL + " AND citt.job_card_fk = job.JOB_CARD_TRN_PK";
                        strSQL = strSQL + " AND job.booking_MST_FK = book.booking_MST_PK";
                    }
                    strSQL = strSQL + " AND book.cust_customer_mst_fk = cust.customer_mst_pk(+)";
                    strSQL = strSQL + " AND CURR.CURRENCY_MST_PK = CIT.CURRENCY_MST_FK";
                    strSQL = strSQL + " AND book.cust_customer_mst_fk = " + CustPk;
                    strSQL = strSQL + " AND job.JOBCARD_REF_NO = '" + JobNo + "' ";
                    strSQL = strSQL + " AND cit.invoice_date <= SYSDATE";
                    strSQL = strSQL + " AND COL.COLLECTIONS_TBL_PK = COLT.COLLECTIONS_TBL_FK";
                    strSQL = strSQL + " AND COLT.INVOICE_REF_NR = CIT.INVOICE_REF_NO ";
                    strSQL = strSQL + " GROUP BY";
                    strSQL = strSQL + " JOBCARD_REF_NO,JOBCARD_DATE,INVOICE_REF_NO,CIT.INVOICE_DATE,nvl(cust.credit_days, 0),CURRENCY_ID,CIT.CURRENCY_MST_FK,";
                    strSQL = strSQL + " CIT.INVOICE_AMT,CIT.TOTAL_CREDIT_NOTE_AMT,Col.CURRENCY_MST_FK,Col.Collections_Date,nvl(COLT.RECD_AMOUNT_HDR_CURR, 0),ceil(NVL(TO_DATE(sysdate,'DD/MM/YYYY') - ((cit.invoice_date) + nvl(cust.credit_days,0)),0)))";
                    strSQL = strSQL + " group by JOBCARD_REF_NO,JOBCARD_DATE,INVOICE_REF_NO,INVOICE_DATE,DUEDATE,CURRENCY_ID,INVOICE,INVOICE_AMT,OUTSTD_DAYS)A";
                }

                if (BizType == 3)
                {
                    strSQL = " SELECT ROWNUM SR_NO, Q.* FROM(";
                    strSQL = strSQL + "(SELECT A.JOBCARD_REF_NO,A.JOBCARD_DATE,A.INVOICE_REF_NO,A.INVOICE_DATE,A.DUEDATE,A.CURRENCY_ID,A.INVOICE,A.INVOICE_AMT-A.OUT_AMOUNT Outstanding,";
                    strSQL = strSQL + " case when (A.INVOICE_AMT-A.OUT_AMOUNT) <= 0 then 0 else A.OUTSTD_DAYS end OUTSTD_DAYS from  (";
                    strSQL = strSQL + " select JOBCARD_REF_NO,JOBCARD_DATE,INVOICE_REF_NO,INVOICE_DATE,DUEDATE,CURRENCY_ID,INVOICE,INVOICE_AMT,sum(OUT_AMOUNT) OUT_AMOUNT,OUTSTD_DAYS from (";

                    strSQL = strSQL + " SELECT JOB.JOBCARD_REF_NO,";
                    strSQL = strSQL + " TO_CHAR(TO_DATE(JOB.JOBCARD_DATE),'DD/MM/YYYY') JOBCARD_DATE,";
                    strSQL = strSQL + " CIT.INVOICE_REF_NO,";
                    strSQL = strSQL + " TO_CHAR(TO_DATE(CIT.INVOICE_DATE),'DD/MM/YYYY')INVOICE_DATE,";
                    strSQL = strSQL + " TO_CHAR(TO_DATE(CIT.INVOICE_DATE) + nvl(cust.credit_days, 0),'DD/MM/YYYY') DUEDATE,";
                    strSQL = strSQL + " CURR.CURRENCY_ID,";
                    strSQL = strSQL + " CIT.INVOICE_AMT * GET_EX_RATE(CIT.CURRENCY_MST_FK, " + HttpContext.Current.Session["currency_mst_pk"] + ",CIT.INVOICE_DATE) INVOICE,";
                    strSQL = strSQL + " ((CIT.INVOICE_AMT * GET_EX_RATE(CIT.CURRENCY_MST_FK, " + HttpContext.Current.Session["currency_mst_pk"] + ", CIT.INVOICE_DATE)) - ";
                    strSQL = strSQL + " (nvl(CIT.TOTAL_CREDIT_NOTE_AMT, 0) * GET_EX_RATE(CIT.CURRENCY_MST_FK, " + HttpContext.Current.Session["currency_mst_pk"] + ", CIT.INVOICE_DATE)))INVOICE_AMT,";
                    strSQL = strSQL + " (nvl(COLT.RECD_AMOUNT_HDR_CURR, 0) * GET_EX_RATE(Col.CURRENCY_MST_FK," + HttpContext.Current.Session["currency_mst_pk"] + ",Col.Collections_Date)) OUT_AMOUNT,";

                    strSQL = strSQL + " ceil(NVL(TO_DATE(sysdate,'DD/MM/YYYY') - (cit.invoice_date + nvl(cust.credit_days,0)),0)) OUTSTD_DAYS";
                    strSQL = strSQL + "  FROM";
                    strSQL = strSQL + " consol_invoice_tbl CIT,";
                    strSQL = strSQL + " consol_invoice_trn_tbl CITT,";

                    strSQL = strSQL + " JOB_CARD_TRN job,";
                    strSQL = strSQL + " booking_MST_tbl book,";

                    strSQL = strSQL + " customer_mst_tbl cust,";
                    strSQL = strSQL + " CURRENCY_TYPE_MST_TBL CURR,";
                    strSQL = strSQL + " collections_tbl        col,";
                    strSQL = strSQL + " collections_trn_tbl    colt";
                    strSQL = strSQL + " WHERE";
                    strSQL = strSQL + " cit.consol_invoice_pk = citt.consol_invoice_fk";

                    strSQL = strSQL + " AND citt.job_card_fk = job.JOB_CARD_TRN_PK";
                    strSQL = strSQL + " AND job.booking_MST_FK = book.booking_MST_PK";

                    strSQL = strSQL + " AND book.cust_customer_mst_fk = cust.customer_mst_pk(+)";
                    strSQL = strSQL + " AND CURR.CURRENCY_MST_PK = CIT.CURRENCY_MST_FK";
                    strSQL = strSQL + " AND book.cust_customer_mst_fk = " + CustPk;
                    strSQL = strSQL + " AND job.JOBCARD_REF_NO = '" + JobNo + "' ";
                    strSQL = strSQL + " AND cit.invoice_date <= SYSDATE";
                    strSQL = strSQL + " AND COL.COLLECTIONS_TBL_PK = COLT.COLLECTIONS_TBL_FK";
                    strSQL = strSQL + " AND COLT.INVOICE_REF_NR = CIT.INVOICE_REF_NO ";
                    strSQL = strSQL + " GROUP BY";
                    strSQL = strSQL + " JOBCARD_REF_NO,JOBCARD_DATE,INVOICE_REF_NO,CIT.INVOICE_DATE,nvl(cust.credit_days, 0),CURRENCY_ID,CIT.CURRENCY_MST_FK,";
                    strSQL = strSQL + " CIT.INVOICE_AMT,CIT.TOTAL_CREDIT_NOTE_AMT,Col.CURRENCY_MST_FK,Col.Collections_Date,nvl(COLT.RECD_AMOUNT_HDR_CURR, 0),ceil(NVL(TO_DATE(sysdate,'DD/MM/YYYY') - ((cit.invoice_date) + nvl(cust.credit_days,0)),0)))";
                    strSQL = strSQL + " group by JOBCARD_REF_NO,JOBCARD_DATE,INVOICE_REF_NO,INVOICE_DATE,DUEDATE,CURRENCY_ID,INVOICE,INVOICE_AMT,OUTSTD_DAYS)A)";

                    strSQL = strSQL + " UNION ";

                    strSQL = strSQL + "(SELECT  A.JOBCARD_REF_NO,A.JOBCARD_DATE,A.INVOICE_REF_NO,A.INVOICE_DATE,A.DUEDATE,A.CURRENCY_ID,A.INVOICE,A.INVOICE_AMT-A.OUT_AMOUNT Outstanding,";
                    strSQL = strSQL + " case when (A.INVOICE_AMT-A.OUT_AMOUNT) <= 0 then 0 else A.OUTSTD_DAYS end OUTSTD_DAYS from  (";
                    strSQL = strSQL + " select JOBCARD_REF_NO,JOBCARD_DATE,INVOICE_REF_NO,INVOICE_DATE,DUEDATE,CURRENCY_ID,INVOICE,INVOICE_AMT,sum(OUT_AMOUNT) OUT_AMOUNT,OUTSTD_DAYS from (";

                    strSQL = strSQL + " SELECT JOB.JOBCARD_REF_NO,";
                    strSQL = strSQL + " TO_CHAR(TO_DATE(JOB.JOBCARD_DATE),'DD/MM/YYYY') JOBCARD_DATE,";
                    strSQL = strSQL + " CIT.INVOICE_REF_NO,";
                    strSQL = strSQL + " TO_CHAR(TO_DATE(CIT.INVOICE_DATE),'DD/MM/YYYY') INVOICE_DATE,";
                    strSQL = strSQL + " TO_CHAR(TO_DATE(CIT.INVOICE_DATE) + nvl(cust.credit_days, 0),'DD/MM/YYYY') DUEDATE,";
                    strSQL = strSQL + " CURR.CURRENCY_ID,";
                    strSQL = strSQL + " CIT.INVOICE_AMT * GET_EX_RATE(CIT.CURRENCY_MST_FK, " + HttpContext.Current.Session["currency_mst_pk"] + ",CIT.INVOICE_DATE) INVOICE,";
                    strSQL = strSQL + " ((CIT.INVOICE_AMT * GET_EX_RATE(CIT.CURRENCY_MST_FK, " + HttpContext.Current.Session["currency_mst_pk"] + ", CIT.INVOICE_DATE)) - ";
                    strSQL = strSQL + " (nvl(CIT.TOTAL_CREDIT_NOTE_AMT, 0) * GET_EX_RATE(CIT.CURRENCY_MST_FK, " + HttpContext.Current.Session["currency_mst_pk"] + ", CIT.INVOICE_DATE)))INVOICE_AMT,";
                    strSQL = strSQL + " (nvl(COLT.RECD_AMOUNT_HDR_CURR, 0) * GET_EX_RATE(Col.CURRENCY_MST_FK," + HttpContext.Current.Session["currency_mst_pk"] + ",Col.Collections_Date)) OUT_AMOUNT,";

                    strSQL = strSQL + " ceil(NVL(TO_DATE(sysdate,'DD/MM/YYYY') - (cit.invoice_date + nvl(cust.credit_days,0)),0)) OUTSTD_DAYS";
                    strSQL = strSQL + "  FROM";
                    strSQL = strSQL + " consol_invoice_tbl CIT,";
                    strSQL = strSQL + " consol_invoice_trn_tbl CITT,";

                    strSQL = strSQL + " JOB_CARD_TRN job,";
                    strSQL = strSQL + " booking_MST_tbl book,";

                    strSQL = strSQL + " customer_mst_tbl cust,";
                    strSQL = strSQL + " CURRENCY_TYPE_MST_TBL CURR,";
                    strSQL = strSQL + " collections_tbl        col,";
                    strSQL = strSQL + " collections_trn_tbl    colt";
                    strSQL = strSQL + " WHERE";
                    strSQL = strSQL + " cit.consol_invoice_pk = citt.consol_invoice_fk";

                    strSQL = strSQL + " AND citt.job_card_fk = job.JOB_CARD_TRN_PK";
                    strSQL = strSQL + " AND job.booking_MST_FK = book.booking_MST_PK";

                    strSQL = strSQL + " AND book.cust_customer_mst_fk = cust.customer_mst_pk(+)";
                    strSQL = strSQL + " AND CURR.CURRENCY_MST_PK = CIT.CURRENCY_MST_FK";
                    strSQL = strSQL + " AND book.cust_customer_mst_fk = " + CustPk;
                    strSQL = strSQL + " AND job.JOBCARD_REF_NO = '" + JobNo + "' ";
                    strSQL = strSQL + " AND cit.invoice_date <= SYSDATE";
                    strSQL = strSQL + " AND COL.COLLECTIONS_TBL_PK = COLT.COLLECTIONS_TBL_FK";
                    strSQL = strSQL + " AND COLT.INVOICE_REF_NR = CIT.INVOICE_REF_NO ";
                    strSQL = strSQL + " GROUP BY";
                    strSQL = strSQL + " JOBCARD_REF_NO,JOBCARD_DATE,INVOICE_REF_NO,CIT.INVOICE_DATE,nvl(cust.credit_days, 0),CURRENCY_ID,CIT.CURRENCY_MST_FK,";
                    strSQL = strSQL + " CIT.INVOICE_AMT,CIT.TOTAL_CREDIT_NOTE_AMT,Col.CURRENCY_MST_FK,Col.Collections_Date,nvl(COLT.RECD_AMOUNT_HDR_CURR, 0),ceil(NVL(TO_DATE(sysdate,'DD/MM/YYYY') - ((cit.invoice_date) + nvl(cust.credit_days,0)),0)))";
                    strSQL = strSQL + " group by JOBCARD_REF_NO,JOBCARD_DATE,INVOICE_REF_NO,INVOICE_DATE,DUEDATE,CURRENCY_ID,INVOICE,INVOICE_AMT,OUTSTD_DAYS)A))Q";
                }
                ds = objWF.GetDataSet(strSQL);
                return ds;
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
        }

        #endregion "Fetch Outstanding Amount"

        #region "GetPortDetailforClause"

        public DataSet GetPortDetailforClause(string jobpk, long DOPK, long BizType)
        {
            WorkFlow objWK = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                if (BizType == 2)
                {
                    sb.Append(" SELECT DISTINCT JI.PORT_MST_POL_FK, JI.PORT_MST_POD_FK");
                    sb.Append("  FROM DELIVERY_ORDER_DTL_TBL T,");
                    sb.Append("       job_trn_cont   J,");
                    sb.Append("       JOB_CARD_TRN   JI");
                    sb.Append(" WHERE T.JOB_CARD_SEA_IMP_CONT_FK = J.job_trn_cont_pk");
                    sb.Append("   AND JI.JOB_CARD_TRN_PK = J.JOB_CARD_TRN_FK");
                    sb.Append("   AND JI.JOB_CARD_TRN_PK = " + jobpk);
                }
                else
                {
                    sb.Append(" SELECT DISTINCT JI.PORT_MST_POL_FK, JI.PORT_MST_POD_FK");
                    sb.Append("  FROM DELIVERY_ORDER_DTL_TBL T,");
                    sb.Append("       job_trn_cont   J,");
                    sb.Append("       JOB_CARD_TRN   JI");
                    sb.Append(" WHERE T.JOB_CARD_AIR_IMP_CONT_FK = J.job_trn_cont_pk");
                    sb.Append("   AND JI.JOB_CARD_TRN_PK = J.JOB_CARD_TRN_FK");
                    sb.Append("   AND JI.JOB_CARD_TRN_PK = " + jobpk);
                }
                return (objWK.GetDataSet(sb.ToString()));
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

        #endregion "GetPortDetailforClause"

        #region "Credit Details"

        public DataSet FetchCreditDetails(string jobPK = "0", string BizType = "0")
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder(5000);
            WorkFlow objWK = new WorkFlow();
            try
            {
                if (BizType == "2")
                {
                    strSQL.Append(" SELECT JOB.JOB_CARD_TRN_PK,");
                    strSQL.Append(" JOB.CONSIGNEE_CUST_MST_FK,");
                    strSQL.Append(" CMT.SEA_CREDIT_LIMIT,");
                    strSQL.Append(" JOB.JOBCARD_REF_NO,");
                    strSQL.Append(" SUM(NVL(JFD.EXCHANGE_RATE * JFD.FREIGHT_AMT, 0)) AS TOTAL,");
                    strSQL.Append(" CMT.CREDIT_CUSTOMER ");
                    strSQL.Append(" FROM JOB_CARD_TRN JOB,");
                    strSQL.Append(" job_trn_fd   JFD,");
                    strSQL.Append(" CUSTOMER_MST_TBL     CMT");

                    strSQL.Append(" WHERE JOB.JOB_CARD_TRN_PK = JFD.JOB_CARD_TRN_FK(+)");
                    strSQL.Append(" AND JOB.CONSIGNEE_CUST_MST_FK = CMT.CUSTOMER_MST_PK ");
                    strSQL.Append(" AND JOB.JOB_CARD_TRN_PK= " + jobPK);
                    strSQL.Append(" GROUP BY JOB.JOB_CARD_TRN_PK,");
                    strSQL.Append("  JOB.CONSIGNEE_CUST_MST_FK,");
                    strSQL.Append("  CMT.SEA_CREDIT_LIMIT,");
                    strSQL.Append(" JOB.JOBCARD_REF_NO,CMT.CREDIT_CUSTOMER");
                }
                else
                {
                    strSQL.Append(" SELECT JOB.JOB_CARD_TRN_PK,");
                    strSQL.Append(" JOB.CONSIGNEE_CUST_MST_FK,");
                    strSQL.Append(" CMT.SEA_CREDIT_LIMIT,");
                    strSQL.Append(" JOB.JOBCARD_REF_NO,");
                    strSQL.Append(" SUM(NVL(JFD.EXCHANGE_RATE * JFD.FREIGHT_AMT, 0)) AS TOTAL,");
                    strSQL.Append(" CMT.CREDIT_CUSTOMER ");
                    strSQL.Append(" FROM JOB_CARD_TRN JOB,");
                    strSQL.Append(" job_trn_fd   JFD,");
                    strSQL.Append(" CUSTOMER_MST_TBL     CMT");

                    strSQL.Append(" WHERE JOB.JOB_CARD_TRN_PK = JFD.JOB_CARD_TRN_FK(+)");
                    strSQL.Append(" AND JOB.CONSIGNEE_CUST_MST_FK = CMT.CUSTOMER_MST_PK ");
                    strSQL.Append(" AND JOB.JOB_CARD_TRN_PK = " + jobPK);
                    strSQL.Append(" GROUP BY JOB.JOB_CARD_TRN_PK,");
                    strSQL.Append("  JOB.CONSIGNEE_CUST_MST_FK,");
                    strSQL.Append("  CMT.SEA_CREDIT_LIMIT,");
                    strSQL.Append(" JOB.JOBCARD_REF_NO,CMT.CREDIT_CUSTOMER");
                }

                return (objWK.GetDataSet(strSQL.ToString()));
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

        #endregion "Credit Details"

        #region "Fetching CreditPolicy Details based on Consignee"

        public object FetchCreditPolicy(string ConsigneePK = "0")
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            strSQL.Append(" SELECT CMT.CREDIT_LIMIT,");
            strSQL.Append(" cmt.customer_name,");
            strSQL.Append(" CMT.CREDIT_DAYS,");
            strSQL.Append(" CMT.SEA_APP_BOOKING,");
            strSQL.Append(" CMT.SEA_APP_BL_RELEASE,");
            strSQL.Append(" CMT.SEA_APP_RELEASE_ODR,");
            strSQL.Append(" CMT.AIR_APP_BOOKING,");
            strSQL.Append(" CMT.AIR_APP_BL_RELEASE,");
            strSQL.Append(" CMT.AIR_APP_RELEASE_ODR");
            strSQL.Append(" FROM CUSTOMER_MST_TBL CMT");
            strSQL.Append(" WHERE CMT.CUSTOMER_MST_PK = " + ConsigneePK);

            try
            {
                return (objWF.GetDataSet(strSQL.ToString()));
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

        #endregion "Fetching CreditPolicy Details based on Consignee"

        #region "Update DO Jobcard"

        public ArrayList UpdateDOJobcard(int JobPk = 0, short BizType = 0)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            OracleCommand updCmdUser = new OracleCommand();
            string str = null;
            Int16 intIns = default(Int16);
            try
            {
                updCmdUser.Transaction = TRAN;
                if (BizType == 2)
                {
                    str = "UPDATE JOB_CARD_TRN  JA SET ";
                    str += "   JA.DO_STATUS = 1";
                    str += " WHERE JA.JOB_CARD_TRN_PK=" + JobPk;
                }
                else if (BizType == 1)
                {
                    str = "UPDATE JOB_CARD_TRN JA SET ";
                    str += "   JA.DO_STATUS = 1";
                    str += " WHERE  JA.JOB_CARD_TRN_PK=" + JobPk;
                }

                var _with22 = updCmdUser;
                _with22.Connection = objWK.MyConnection;
                _with22.Transaction = TRAN;
                _with22.CommandType = CommandType.Text;
                _with22.CommandText = str;
                intIns = Convert.ToInt16(_with22.ExecuteNonQuery());
                if (intIns > 0)
                {
                    TRAN.Commit();
                    arrMessage.Add("Protocol Generated Succesfully");
                    return arrMessage;
                }
            }
            catch (OracleException OraEx)
            {
                TRAN.Rollback();
                arrMessage.Add(OraEx.Message);
                return arrMessage;
            }
            finally
            {
                objWK.CloseConnection();
            }
            return new ArrayList();
        }

        public object fetchAgtcollection(string JOBPK = "", short BizType = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                sb.Append("SELECT JOB.JOB_CARD_TRN_PK,");
                sb.Append("       INVAGT.INV_AGENT_PK,");
                sb.Append("       INVAGT.CB_DP_LOAD_AGENT AGENTTYPE");
                sb.Append("  FROM JOB_CARD_TRN JOB, INV_AGENT_TBL INVAGT ");
                sb.Append("  WHERE JOB.JOB_CARD_TRN_PK = " + JOBPK);
                sb.Append("   AND INVAGT.JOB_CARD_FK(+) = JOB.JOB_CARD_TRN_PK");
                //If BizType = 1 Then
                //    sb.Replace("SEA", "AIR")
                //End If
                return objWF.GetDataSet(sb.ToString());
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

        public object checkstatus(DataSet dsAgtCollection, int JobPk = 0, short BizType = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            Int32 i = default(Int32);
            bool IsDPAgent = false;
            bool IsCBAgent = false;
            try
            {
                if (dsAgtCollection.Tables.Count > 0)
                {
                    if (dsAgtCollection.Tables[0].Rows.Count > 0)
                    {
                        for (i = 0; i <= dsAgtCollection.Tables[0].Rows.Count - 1; i++)
                        {
                            if (!string.IsNullOrEmpty(dsAgtCollection.Tables[0].Rows[i]["AGENTTYPE"].ToString()))
                            {
                                if (Convert.ToInt32(dsAgtCollection.Tables[0].Rows[i]["AGENTTYPE"]) == 2)
                                {
                                    IsDPAgent = true;
                                }
                                else if (Convert.ToInt32(dsAgtCollection.Tables[0].Rows[i]["AGENTTYPE"]) == 1)
                                {
                                    IsCBAgent = true;
                                }
                            }
                        }
                    }
                }

                if (IsDPAgent == true & IsCBAgent == true)
                {
                    sb.Append("select * FROM  JOB_CARD_TRN JOB");
                    sb.Append("  where JOB.JOB_CARD_TRN_PK=" + JobPk);
                    sb.Append("  AND JOB.COLLECTION_STATUS=1");
                    sb.Append("  AND JOB.PAYEMENT_STATUS=1");
                    sb.Append("  AND JOB.LOADAGENT_STATUS=1");
                    sb.Append("  AND JOB.CBAGENT_STATUS=1");
                    sb.Append("  AND JOB.DO_STATUS=1");
                }
                else if (IsDPAgent == true)
                {
                    sb.Append("select * FROM  JOB_CARD_TRN JOB");
                    sb.Append("  where JOB.JOB_CARD_TRN_PK=" + JobPk);
                    sb.Append("  AND JOB.COLLECTION_STATUS=1");
                    sb.Append("  AND JOB.PAYEMENT_STATUS=1");
                    sb.Append("  AND JOB.LOADAGENT_STATUS=1");
                    sb.Append("  AND JOB.DO_STATUS=1");
                }
                else if (IsCBAgent == true)
                {
                    sb.Append("select * FROM  JOB_CARD_TRN JOB");
                    sb.Append("  where JOB.JOB_CARD_TRN_PK=" + JobPk);
                    sb.Append("  AND JOB.COLLECTION_STATUS=1");
                    sb.Append("  AND JOB.PAYEMENT_STATUS=1");
                    sb.Append("  AND JOB.CBAGENT_STATUS=1");
                    sb.Append("  AND JOB.DO_STATUS=1");
                }
                else
                {
                    sb.Append("select * FROM  JOB_CARD_TRN JOB");
                    sb.Append(" where JOB.JOB_CARD_TRN_PK=" + JobPk);
                    sb.Append(" AND JOB.COLLECTION_STATUS=1");
                    sb.Append(" AND JOB.PAYEMENT_STATUS=1");
                    sb.Append(" AND JOB.DO_STATUS=1");
                }

                if (BizType == 1)
                {
                    sb.Replace("SEA", "AIR");
                }
                return objWF.GetDataSet(sb.ToString());
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

        public ArrayList updatejobcarddate(int JobPk = 0, short BizType = 0)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            OracleCommand updCmdUser = new OracleCommand();
            string str = null;
            Int16 intIns = default(Int16);
            try
            {
                updCmdUser.Transaction = TRAN;
                if (BizType == 2)
                {
                    str = "UPDATE JOB_CARD_TRN  JA SET ";
                    str += "   JA.JOB_CARD_STATUS = 2, JA.JOB_CARD_CLOSED_ON =SYSDATE ";
                    str += " WHERE JA.JOB_CARD_TRN_PK=" + JobPk;
                }
                else if (BizType == 1)
                {
                    str = "UPDATE JOB_CARD_TRN JAI SET ";
                    str += "   JAI.JOB_CARD_STATUS = 2, JAI.JOB_CARD_CLOSED_ON =SYSDATE";
                    str += " WHERE  JAI.JOB_CARD_TRN_PK=" + JobPk;
                }

                var _with23 = updCmdUser;
                _with23.Connection = objWK.MyConnection;
                _with23.Transaction = TRAN;
                _with23.CommandType = CommandType.Text;
                _with23.CommandText = str;
                intIns = Convert.ToInt16(_with23.ExecuteNonQuery());
                if (intIns > 0)
                {
                    TRAN.Commit();
                    arrMessage.Add("Protocol Generated Succesfully");
                    return arrMessage;
                }
            }
            catch (OracleException OraEx)
            {
                TRAN.Rollback();
                arrMessage.Add(OraEx.Message);
                return arrMessage;
            }
            finally
            {
                objWK.CloseConnection();
            }
            return new ArrayList();
        }

        #endregion "Update DO Jobcard"

        #region "Print flag update"

        public ArrayList PrintFlagUpdate(int DOPK, int PrintFlag)
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            OracleCommand updCommand = new OracleCommand();
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            var _with24 = updCommand;
            updCommand.Parameters.Clear();
            _with24.Connection = objWK.MyConnection;
            _with24.CommandType = CommandType.StoredProcedure;
            _with24.CommandText = objWK.MyUserName + ".DO_TBL_PKG.DO_PRINT_FLAG_UPD";
            var _with25 = _with24.Parameters;
            _with25.Add("DO_PK_IN", DOPK).Direction = ParameterDirection.Input;
            _with25.Add("PRINT_FLAG_IN", PrintFlag).Direction = ParameterDirection.Input;
            _with25.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
            
            var _with26 = objWK.MyDataAdapter;
            _with26.UpdateCommand = updCommand;
            _with26.UpdateCommand.Transaction = TRAN;
            _with26.UpdateCommand.ExecuteNonQuery();
            if (arrMessage.Count > 0)
            {
                TRAN.Rollback();
            }
            else
            {
                TRAN.Commit();
            }
            return new ArrayList();
        }

        #endregion "Print flag update"

        #region "Fetch DO History"

        public DataSet FetchDOHistory(int JobPk, int BizType)
        {
            try
            {
                WorkFlow objWK = new WorkFlow();
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("SELECT ROWNUM AS SLNO, Q.*");
                sb.Append("  FROM (SELECT DISTINCT DOMST.DELIVERY_ORDER_PK     DOPK,");
                sb.Append("                        DOMST.DELIVERY_ORDER_REF_NO DONR,");
                sb.Append("                        TO_CHAR(DOMST.DELIVERY_ORDER_DATE,DATEFORMAT) DODATE,");
                sb.Append("                        TO_CHAR(DOMST.DELIVERY_ORD_VALID_DT,DATEFORMAT) DOVALID_DATE,");
                sb.Append("                        DOMST.REMARKS");
                sb.Append("          FROM DELIVERY_ORDER_MST_TBL DOMST");
                sb.Append("         WHERE ");
                if (BizType == 1)
                {
                    sb.Append("          DOMST.JOB_CARD_AIR_MST_FK = " + JobPk);
                }
                else
                {
                    sb.Append("          DOMST.JOB_CARD_MST_FK = " + JobPk);
                }
                sb.Append("         ORDER BY DOPK ASC ) Q ");
                return objWK.GetDataSet(sb.ToString());
            }
            catch (OracleException Oraex)
            {
                throw Oraex;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch DO History"

        public void UpdateDOExport(int JOB_CARD_PK, int Biz_Type, string RefNr)
        {
            WorkFlow objWK = new WorkFlow();
            int JCCount = 0;
            int JCPK = 0;
            JCCount = Convert.ToInt32(objWK.ExecuteScaler("select count (*) from job_card_trn jct where jct.jobcard_ref_no in (select jc.jobcard_ref_no from job_card_trn jc where jc.job_card_trn_pk=" + JOB_CARD_PK + ") and jct.process_type=1"));
            if (JCCount > 0)
            {
                JCPK = Convert.ToInt32(objWK.ExecuteScaler("select jct.job_card_trn_pk from job_card_trn jct where jct.jobcard_ref_no in (select jc.jobcard_ref_no from job_card_trn jc where jc.job_card_trn_pk=" + JOB_CARD_PK + ") and jct.process_type=1 and rownum=1"));
            }
            else
            {
                return;
            }
            objWK.OpenConnection();
            OracleTransaction Tran = null;
            Tran = objWK.MyConnection.BeginTransaction();
            try
            {
                var _with27 = objWK.MyCommand;
                _with27.CommandType = CommandType.StoredProcedure;
                _with27.CommandText = objWK.MyUserName + ".TRACK_N_TRACE_PKG.TRACK_N_TRACE_INS_EXT";
                _with27.Transaction = Tran;
                _with27.Parameters.Clear();
                _with27.Parameters.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with27.Parameters.Add("PROCESS_IN", 1).Direction = ParameterDirection.Input;
                _with27.Parameters.Add("KEY_FK_IN", JCPK).Direction = ParameterDirection.Input;
                _with27.Parameters.Add("LOCATION_FK_IN", HttpContext.Current.Session["LOGED_IN_LOC_FK"]).Direction = ParameterDirection.Input;
                _with27.Parameters.Add("STATUS_IN", "DO Generated").Direction = ParameterDirection.Input;
                _with27.Parameters.Add("CREATEDUSER_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                _with27.Parameters.Add("DOC_REF_IN", RefNr).Direction = ParameterDirection.Input;
                _with27.Parameters.Add("CREATED_DT_IN", "").Direction = ParameterDirection.Input;
                _with27.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
                _with27.ExecuteNonQuery();
                Tran.Commit();
            }
            catch (OracleException oraexp)
            {
                Tran.Rollback();
                throw oraexp;
            }
            catch (Exception ex)
            {
                Tran.Rollback();
                throw ex;
            }
            finally
            {
                objWK.CloseConnection();
            }
        }
    }
}