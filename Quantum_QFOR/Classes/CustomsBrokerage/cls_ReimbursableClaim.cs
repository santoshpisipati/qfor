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

using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections;
using System.Data;
using System.Web;

namespace Quantum_QFOR
{
    public class cls_ReimbursableClaim : CommonFeatures
    {
        #region "Get Shipment Details"

        public DataSet FetchShipmentDts(int JobRefPk = 0, string JobType = "0", string BizType = "0", string Process = "0")
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                var _with1 = objWF.MyCommand.Parameters;
                _with1.Add("REF_JOBPK_IN", JobRefPk).Direction = ParameterDirection.Input;
                _with1.Add("JCTYPE_IN", JobType).Direction = ParameterDirection.Input;
                _with1.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with1.Add("PROCESS_TYPE_IN", Process).Direction = ParameterDirection.Input;
                _with1.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("REIMBURSABLE_TBL_PKG", "FETCH_JOB_DETAILS");
                return DS;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        #endregion "Get Shipment Details"

        #region "Get Main Details"

        public DataSet FetchMainDt(int Rem_Mst_Pk = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                var _with2 = objWF.MyCommand.Parameters;
                _with2.Add("REIMBURSABLE_MST_PK_IN", Rem_Mst_Pk).Direction = ParameterDirection.Input;
                _with2.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("REIMBURSABLE_TBL_PKG", "FETCH_MAIN_DATA");
                return DS;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        #endregion "Get Main Details"

        #region "Get Grid Details"

        public DataSet FetchGridDt(int Rem_Mst_Pk = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                var _with3 = objWF.MyCommand.Parameters;
                _with3.Add("REIMBURSABLE_MST_PK_IN", Rem_Mst_Pk).Direction = ParameterDirection.Input;
                _with3.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("REIMBURSABLE_TBL_PKG", "FETCH_GRID_DETAILS");
                return DS;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        #endregion "Get Grid Details"

        #region "SAVE"

        public ArrayList Save(DataSet M_DataSet, long Reim_PK, DataSet dsGridDetails = null, bool IsEdit = false, string ReimRefNo = "", string DelPks = "", string RefJobFk = "", string JobType = "", int ShipperPK = 0, int Currfk = 0,
        int ConsigneePK = 0, string BizType = "", string Process = "", bool Approved = true, string polid = "", string podid = "")
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            DataRow dr = null;
            DataRow dr1 = null;
            int RecAfct = 0;
            string Reimbursable_Ref_Nr = null;
            try
            {
                if (IsEdit == false)
                {
                    Reimbursable_Ref_Nr = GenerateProtocolKey("REIMBURSABLE CLAIM", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), DateTime.Today, "", "", polid, M_LAST_MODIFIED_BY_FK, new WorkFlow(), "",
                    podid);
                }
                else
                {
                    Reimbursable_Ref_Nr = ReimRefNo;
                }

                var _with4 = objWK.MyCommand;
                _with4.Transaction = TRAN;
                _with4.Connection = objWK.MyConnection;
                _with4.CommandType = CommandType.StoredProcedure;
                if (M_DataSet.Tables[0].Rows.Count > 0)
                {
                    foreach (DataRow dr_loopVariable in M_DataSet.Tables[0].Rows)
                    {
                        dr = dr_loopVariable;
                        if (string.IsNullOrEmpty(dr["REIMBURSABLE_MST_PK"].ToString()))
                        {
                            _with4.CommandText = objWK.MyUserName + ".REIMBURSABLE_TBL_PKG.REIMBURSABLE_INS";
                            var _with5 = _with4.Parameters;
                            _with5.Clear();
                            _with5.Add("REIMBURSABLE_REF_NO_IN", Reimbursable_Ref_Nr).Direction = ParameterDirection.Input;
                            _with5.Add("REIMBURSABLE_DATE_IN", dr["REIMBURSABLE_DATE"]).Direction = ParameterDirection.Input;
                            _with5.Add("JOB_TYPE_IN", dr["JOB_TYPE"]).Direction = ParameterDirection.Input;
                            _with5.Add("JOB_REF_FK_IN", dr["JOB_REF_FK"]).Direction = ParameterDirection.Input;
                            _with5.Add("CURRENCY_MST_FK_IN", dr["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                            _with5.Add("STATUS_IN", dr["STATUS"]).Direction = ParameterDirection.Input;
                            _with5.Add("BIZ_TYPE_IN", dr["BIZ_TYPE"]).Direction = ParameterDirection.Input;
                            _with5.Add("PROCESS_TYPE_IN", dr["PROCESS_TYPE"]).Direction = ParameterDirection.Input;
                            _with5.Add("REMARKS_IN", dr["REMARKS"]).Direction = ParameterDirection.Input;
                            _with5.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                            _with5.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;
                            _with4.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "REIMBURSABLE_MST_PK").Direction = ParameterDirection.Output;
                            _with4.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                            RecAfct = _with4.ExecuteNonQuery();
                            if (RecAfct > 0)
                            {
                                Reim_PK = Convert.ToInt32(_with4.Parameters["RETURN_VALUE"].Value);
                            }
                        }
                        else
                        {
                            _with4.CommandText = objWK.MyUserName + ".REIMBURSABLE_TBL_PKG.REIMBURSABLE_UPD";
                            var _with6 = _with4.Parameters;
                            _with6.Clear();
                            _with6.Add("REIMBURSABLE_MST_PK_IN", Reim_PK).Direction = ParameterDirection.Input;
                            _with6.Add("REIMBURSABLE_REF_NO_IN", Reimbursable_Ref_Nr).Direction = ParameterDirection.Input;
                            _with6.Add("REIMBURSABLE_DATE_IN", dr["REIMBURSABLE_DATE"]).Direction = ParameterDirection.Input;
                            _with6.Add("JOB_TYPE_IN", dr["JOB_TYPE"]).Direction = ParameterDirection.Input;
                            _with6.Add("JOB_REF_FK_IN", dr["JOB_REF_FK"]).Direction = ParameterDirection.Input;
                            _with6.Add("CURRENCY_MST_FK_IN", dr["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                            _with6.Add("STATUS_IN", dr["STATUS"]).Direction = ParameterDirection.Input;
                            _with6.Add("BIZ_TYPE_IN", dr["BIZ_TYPE"]).Direction = ParameterDirection.Input;
                            _with6.Add("PROCESS_TYPE_IN", dr["PROCESS_TYPE"]).Direction = ParameterDirection.Input;
                            _with6.Add("REMARKS_IN", dr["REMARKS"]).Direction = ParameterDirection.Input;
                            _with6.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                            _with6.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;
                            _with6.Add("VERSION_NO_IN", dr["VERSION_NO"]).Direction = ParameterDirection.Input;
                            _with4.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                            _with4.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                            RecAfct = _with4.ExecuteNonQuery();
                            if (RecAfct > 0)
                            {
                                if (!string.IsNullOrEmpty(_with4.Parameters["RETURN_VALUE"].Value.ToString()))
                                {
                                    Reim_PK = Convert.ToInt32(_with4.Parameters["RETURN_VALUE"].Value);
                                }
                                else
                                {
                                    Reim_PK = 0;
                                }
                            }
                        }
                    }
                }

                var _with7 = objWK.MyCommand;
                _with7.Transaction = TRAN;
                _with7.Connection = objWK.MyConnection;
                _with7.CommandType = CommandType.StoredProcedure;
                if (dsGridDetails.Tables[0].Rows.Count > 0)
                {
                    foreach (DataRow dr1_loopVariable in dsGridDetails.Tables[0].Rows)
                    {
                        dr1 = dr1_loopVariable;
                        if (string.IsNullOrEmpty(dr1["REIMBURSABLE_TRN_PK"].ToString()))
                        {
                            _with7.CommandText = objWK.MyUserName + ".REIMBURSABLE_TBL_PKG.REIMBURSABLE_TRN_INS";
                            var _with8 = _with7.Parameters;
                            _with8.Clear();
                            _with8.Add("REIMBURSABLE_MST_FK_IN", Reim_PK).Direction = ParameterDirection.Input;
                            _with8.Add("EXPENSE_DESC_IN", dr1["EXPENSE_DESC"]).Direction = ParameterDirection.Input;
                            _with8.Add("VENDOR_NAME_IN", dr1["VENDOR_NAME"]).Direction = ParameterDirection.Input;
                            _with8.Add("VENDOR_INV_REF_NR_IN", dr1["VENDOR_INV_REF_NR"]).Direction = ParameterDirection.Input;
                            if (string.IsNullOrEmpty(dr1["REM_DATE"].ToString()))
                            {
                                _with8.Add("REM_DATE_IN", "").Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with8.Add("REM_DATE_IN", Convert.ToDateTime(dr1["REM_DATE"])).Direction = ParameterDirection.Input;
                            }
                            _with8.Add("AMOUNT_IN", dr1["AMOUNT"]).Direction = ParameterDirection.Input;
                            _with8.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                            _with8.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;
                            _with7.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "REIMBURSABLE_TRN_PK").Direction = ParameterDirection.Output;
                            _with7.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                            RecAfct = _with7.ExecuteNonQuery();
                        }
                        else
                        {
                            _with7.CommandText = objWK.MyUserName + ".REIMBURSABLE_TBL_PKG.REIMBURSABLE_TRN_UPD";
                            var _with9 = _with7.Parameters;
                            _with9.Clear();
                            _with9.Add("REIMBURSABLE_TRN_PK_IN", dr1["REIMBURSABLE_TRN_PK"]).Direction = ParameterDirection.Input;
                            _with9.Add("REIMBURSABLE_MST_FK_IN", Reim_PK).Direction = ParameterDirection.Input;
                            _with9.Add("EXPENSE_DESC_IN", dr1["EXPENSE_DESC"]).Direction = ParameterDirection.Input;
                            _with9.Add("VENDOR_NAME_IN", dr1["VENDOR_NAME"]).Direction = ParameterDirection.Input;
                            _with9.Add("VENDOR_INV_REF_NR_IN", dr1["VENDOR_INV_REF_NR"]).Direction = ParameterDirection.Input;
                            if (string.IsNullOrEmpty(dr1["REM_DATE"].ToString()))
                            {
                                _with9.Add("REM_DATE_IN", "").Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with9.Add("REM_DATE_IN", Convert.ToDateTime(dr1["REM_DATE"])).Direction = ParameterDirection.Input;
                            }
                            _with9.Add("AMOUNT_IN", dr1["AMOUNT"]).Direction = ParameterDirection.Input;
                            _with9.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                            _with9.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;
                            _with9.Add("VERSION_NO_IN", dr1["VERSION_NO"]).Direction = ParameterDirection.Input;
                            _with7.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                            _with7.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                            RecAfct = _with7.ExecuteNonQuery();
                        }
                    }
                }

                if (!string.IsNullOrEmpty(DelPks))
                {
                    var _with10 = objWK.MyCommand;
                    _with10.Transaction = TRAN;
                    _with10.CommandType = CommandType.StoredProcedure;
                    _with10.CommandText = objWK.MyUserName + ".REIMBURSABLE_TBL_PKG.REIMBURSABLE_TRN_DEL";
                    _with10.Parameters.Clear();
                    _with10.Parameters.Add("REIMBURSABLE_PK_IN", Reim_PK).Direction = ParameterDirection.Input;
                    _with10.Parameters.Add("REIMBURSABLE_TRN_PKS_IN", (string.IsNullOrEmpty(DelPks) ? "" : DelPks)).Direction = ParameterDirection.Input;
                    _with10.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
                    RecAfct = _with10.ExecuteNonQuery();
                }

                if (Approved == true)
                {
                    int Freigh_Ele_Fk = 0;
                    double FrtAmt = 0.0;
                    int PayType = 0;
                    double Freight_Amt = 0;
                    bool IsUpdate = false;
                    DataSet DsFreight = null;
                    DataSet DsCost = null;
                    int JOB_TRN_FD_PK = 0;
                    int JOB_TRN_COST_PK = 0;
                    int Version = 0;
                    int Version1 = 0;

                    Freigh_Ele_Fk = GrtFrtPk();
                    if (dsGridDetails.Tables[0].Rows.Count > 0)
                    {
                        foreach (DataRow dr1_loopVariable in dsGridDetails.Tables[0].Rows)
                        {
                            dr1 = dr1_loopVariable;
                            FrtAmt += Convert.ToDouble(dr1["AMOUNT"].ToString());
                        }

                        if (Convert.ToInt32(Process) == 2)
                        {
                            PayType = 2;
                        }
                        else
                        {
                            PayType = 1;
                        }

                        DsFreight = GetFrtAmount(JobType, RefJobFk, Freigh_Ele_Fk, BizType, Process);
                        if (DsFreight.Tables[0].Rows.Count > 0)
                        {
                            Version = Convert.ToInt32(DsFreight.Tables[0].Rows[0]["VERSION_NO"]);
                            JOB_TRN_FD_PK = Convert.ToInt32(DsFreight.Tables[0].Rows[0]["JOB_TRN_FD_PK"]);
                            Freight_Amt = Convert.ToDouble(DsFreight.Tables[0].Rows[0]["FREIGHT_AMT"]);
                            FrtAmt = FrtAmt + Freight_Amt;
                            IsUpdate = true;
                        }

                        DsCost = GetCostAmount(JobType, RefJobFk, Freigh_Ele_Fk, BizType, Process);
                        if (DsCost.Tables[0].Rows.Count > 0)
                        {
                            if (DsCost.Tables[0].Rows.Count > 0)
                            {
                                Version1 = Convert.ToInt32(DsCost.Tables[0].Rows[0]["VERSION_NO"]);
                                JOB_TRN_COST_PK = Convert.ToInt32(DsCost.Tables[0].Rows[0]["JOB_TRN_COST_PK"]);
                            }
                        }

                        if (Convert.ToInt32(JobType) == 1)
                        {
                            var _with11 = objWK.MyCommand;
                            _with11.Parameters.Clear();
                            _with11.Transaction = TRAN;
                            _with11.CommandType = CommandType.StoredProcedure;
                            if (IsUpdate == true)
                            {
                                _with11.CommandText = objWK.MyUserName + ".JOB_CARD_TRN_PKG.JOB_TRN_FD_UPD";
                                _with11.Parameters.Add("JOB_TRN_FD_PK_IN", JOB_TRN_FD_PK).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with11.CommandText = objWK.MyUserName + ".JOB_CARD_TRN_PKG.JOB_TRN_FD_INS";
                                _with11.Parameters.Add("JOB_TRN_CONT_FK_IN", JOB_TRN_COST_PK).Direction = ParameterDirection.Input;
                                _with11.Parameters.Add("SERVICE_TYPE_FLAG_IN", 1).Direction = ParameterDirection.Input;
                                _with11.Parameters.Add("MIS_FLAG_IN", 1).Direction = ParameterDirection.Input;
                            }
                            _with11.Parameters.Add("JOB_CARD_TRN_FK_IN", RefJobFk).Direction = ParameterDirection.Input;
                            _with11.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", "").Direction = ParameterDirection.Input;
                            _with11.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", Freigh_Ele_Fk).Direction = ParameterDirection.Input;
                            _with11.Parameters.Add("FREIGHT_TYPE_IN", PayType).Direction = ParameterDirection.Input;
                            _with11.Parameters.Add("LOCATION_MST_FK_IN", Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"])).Direction = ParameterDirection.Input;
                            _with11.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", ConsigneePK).Direction = ParameterDirection.Input;
                            _with11.Parameters.Add("FREIGHT_AMT_IN", FrtAmt).Direction = ParameterDirection.Input;
                            _with11.Parameters.Add("CURRENCY_MST_FK_IN", Currfk).Direction = ParameterDirection.Input;
                            _with11.Parameters.Add("BASIS_IN", "").Direction = ParameterDirection.Input;
                            _with11.Parameters.Add("PRINT_ON_MBL_IN", 1).Direction = ParameterDirection.Input;
                            _with11.Parameters.Add("BASIS_FK_IN", 1).Direction = ParameterDirection.Input;
                            _with11.Parameters.Add("EXCHANGE_RATE_IN", 1).Direction = ParameterDirection.Input;
                            _with11.Parameters.Add("RATE_PERBASIS_IN", FrtAmt).Direction = ParameterDirection.Input;
                            _with11.Parameters.Add("SURCHARGE_IN", "").Direction = ParameterDirection.Input;
                            _with11.Parameters.Add("QUANTITY_IN", 1).Direction = ParameterDirection.Input;
                            _with11.Parameters.Add("SERVICE_MST_FK_IN", 5).Direction = ParameterDirection.Input;
                            _with11.Parameters.Add("AGENT_CURRENCY_MST_FK_IN", "").Direction = ParameterDirection.Input;
                            _with11.Parameters.Add("AGENT_RATEPERBASIS_IN", "").Direction = ParameterDirection.Input;
                            _with11.Parameters.Add("AGENT_FREIGHT_AMT_IN", "").Direction = ParameterDirection.Input;
                            _with11.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;
                            RecAfct = _with11.ExecuteNonQuery();

                            var _with12 = objWK.MyCommand;
                            _with12.Parameters.Clear();
                            _with12.Transaction = TRAN;
                            _with12.CommandType = CommandType.StoredProcedure;
                            if (Convert.ToInt32(BizType) == 2)
                            {
                                if (Convert.ToInt32(Process) == 2)
                                {
                                    if (IsUpdate == true)
                                    {
                                        _with12.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_COST_UPD";
                                        _with12.Parameters.Add("JOB_TRN_IMP_EST_PK_IN", JOB_TRN_COST_PK).Direction = ParameterDirection.Input;
                                    }
                                    else
                                    {
                                        _with12.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_COST_INS";
                                        _with12.Parameters.Add("SERVICE_TYPE_FLAG_IN", 1).Direction = ParameterDirection.Input;
                                        _with12.Parameters.Add("MIS_FLAG_IN", 1).Direction = ParameterDirection.Input;
                                    }
                                    _with12.Parameters.Add("JOB_CARD_SEA_IMP_FK_IN", RefJobFk).Direction = ParameterDirection.Input;
                                }
                                else
                                {
                                    if (IsUpdate == true)
                                    {
                                        _with12.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_COST_UPD";
                                        _with12.Parameters.Add("JOB_TRN_EST_PK_IN", JOB_TRN_COST_PK).Direction = ParameterDirection.Input;
                                    }
                                    else
                                    {
                                        _with12.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_COST_INS";
                                        _with12.Parameters.Add("SERVICE_TYPE_FLAG_IN", 1).Direction = ParameterDirection.Input;
                                        _with12.Parameters.Add("MIS_FLAG_IN", 1).Direction = ParameterDirection.Input;
                                    }
                                    _with12.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", RefJobFk).Direction = ParameterDirection.Input;
                                }
                            }
                            else
                            {
                                if (Convert.ToInt32(Process) == 2)
                                {
                                    if (IsUpdate == true)
                                    {
                                        _with12.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_COST_UPD";
                                        _with12.Parameters.Add("JOB_TRN_AIR_IMP_EST_PK_IN", JOB_TRN_COST_PK).Direction = ParameterDirection.Input;
                                    }
                                    else
                                    {
                                        _with12.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_COST_INS";
                                        _with12.Parameters.Add("SERVICE_TYPE_FLAG_IN", 1).Direction = ParameterDirection.Input;
                                        _with12.Parameters.Add("MIS_FLAG_IN", 1).Direction = ParameterDirection.Input;
                                    }
                                    _with12.Parameters.Add("JOB_CARD_AIR_IMP_FK_IN", RefJobFk).Direction = ParameterDirection.Input;
                                    //'EXPORT
                                }
                                else
                                {
                                    if (IsUpdate == true)
                                    {
                                        _with12.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_TRN_AIR_EXP_COST_UPD";
                                        _with12.Parameters.Add("JOB_TRN_AIR_EST_PK_IN", JOB_TRN_COST_PK).Direction = ParameterDirection.Input;
                                    }
                                    else
                                    {
                                        _with12.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_TRN_AIR_EXP_COST_INS";
                                        _with12.Parameters.Add("SERVICE_TYPE_FLAG_IN", 1).Direction = ParameterDirection.Input;
                                        _with12.Parameters.Add("MIS_FLAG_IN", 1).Direction = ParameterDirection.Input;
                                    }
                                    _with12.Parameters.Add("JOB_CARD_AIR_EXP_FK_IN", RefJobFk).Direction = ParameterDirection.Input;
                                }
                            }

                            _with12.Parameters.Add("VENDOR_MST_FK_IN", "").Direction = ParameterDirection.Input;
                            _with12.Parameters.Add("COST_ELEMENT_FK_IN", Freigh_Ele_Fk).Direction = ParameterDirection.Input;
                            _with12.Parameters.Add("LOCATION_FK_IN", Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"])).Direction = ParameterDirection.Input;
                            _with12.Parameters.Add("VENDOR_KEY_IN", "").Direction = ParameterDirection.Input;
                            _with12.Parameters.Add("PTMT_TYPE_IN", PayType).Direction = ParameterDirection.Input;
                            _with12.Parameters.Add("CURRENCY_MST_FK_IN", Currfk).Direction = ParameterDirection.Input;
                            _with12.Parameters.Add("ESTIMATED_COST_IN", FrtAmt).Direction = ParameterDirection.Input;
                            _with12.Parameters.Add("TOTAL_COST_IN", FrtAmt).Direction = ParameterDirection.Input;
                            _with12.Parameters.Add("BASIS_FK_IN", 1).Direction = ParameterDirection.Input;
                            _with12.Parameters.Add("RATEPERBASIS_IN", FrtAmt).Direction = ParameterDirection.Input;
                            _with12.Parameters.Add("QUANTITY_IN", 1).Direction = ParameterDirection.Input;
                            _with12.Parameters.Add("EXCHANGE_RATE_IN", 1).Direction = ParameterDirection.Input;
                            _with12.Parameters.Add("EXT_INT_FLAG_IN", 1).Direction = ParameterDirection.Input;
                            _with12.Parameters.Add("SERVICE_MST_FK_IN", 5).Direction = ParameterDirection.Input;
                            _with12.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;
                            RecAfct = _with12.ExecuteNonQuery();
                        }
                        else if (Convert.ToInt32(JobType) == 2)
                        {
                            var _with15 = objWK.MyCommand;
                            _with15.Parameters.Clear();
                            _with15.Transaction = TRAN;
                            _with15.CommandType = CommandType.StoredProcedure;
                            if (IsUpdate == true)
                            {
                                _with15.CommandText = objWK.MyUserName + ".CBJC_TRN_FD_PKG.CBJC_TRN_FD_UPD";
                                _with15.Parameters.Add("CBJC_TRN_FD_PK_IN", JOB_TRN_FD_PK).Direction = ParameterDirection.Input;
                                _with15.Parameters.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                                _with15.Parameters.Add("VERSION_NO_IN", Version).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with15.CommandText = objWK.MyUserName + ".CBJC_TRN_FD_PKG.CBJC_TRN_FD_INS";
                                _with15.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                                _with15.Parameters.Add("MIS_FLAG_IN", 1).Direction = ParameterDirection.Input;
                            }

                            _with15.Parameters.Add("CBJC_FK_IN", RefJobFk).Direction = ParameterDirection.Input;
                            _with15.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", Freigh_Ele_Fk).Direction = ParameterDirection.Input;
                            _with15.Parameters.Add("FREIGHT_TYPE_IN", PayType).Direction = ParameterDirection.Input;
                            _with15.Parameters.Add("LOCATION_MST_FK_IN", Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"])).Direction = ParameterDirection.Input;
                            if (Convert.ToInt32(Process) == 2)
                            {
                                _with15.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", ConsigneePK).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with15.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", ShipperPK).Direction = ParameterDirection.Input;
                            }
                            _with15.Parameters.Add("FREIGHT_AMT_IN", FrtAmt).Direction = ParameterDirection.Input;
                            _with15.Parameters.Add("CURRENCY_MST_FK_IN", Currfk).Direction = ParameterDirection.Input;
                            _with15.Parameters.Add("BASIS_FK_IN", 1).Direction = ParameterDirection.Input;
                            _with15.Parameters.Add("EXCHANGE_RATE_IN", 1).Direction = ParameterDirection.Input;
                            _with15.Parameters.Add("RATEPERBASIS_IN", FrtAmt).Direction = ParameterDirection.Input;
                            _with15.Parameters.Add("QUANTITY_IN", 1).Direction = ParameterDirection.Input;
                            _with15.Parameters.Add("SERVICE_MST_FK_IN", 5).Direction = ParameterDirection.Input;
                            _with15.Parameters.Add("INV_AGENT_TRN_FK_IN", "").Direction = ParameterDirection.Input;
                            _with15.Parameters.Add("CONSOL_INVOICE_TRN_FK_IN", "").Direction = ParameterDirection.Input;
                            _with15.Parameters.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;
                            _with15.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;
                            RecAfct = _with15.ExecuteNonQuery();

                            var _with16 = objWK.MyCommand;
                            _with16.Parameters.Clear();
                            _with16.Transaction = TRAN;
                            _with16.CommandType = CommandType.StoredProcedure;
                            if (IsUpdate == true)
                            {
                                _with16.CommandText = objWK.MyUserName + ".CBJC_TRN_COST_PKG.CBJC_TRN_COST_UPD";
                                _with16.Parameters.Add("CBJC_TRN_COST_PK_IN", JOB_TRN_COST_PK).Direction = ParameterDirection.Input;
                                _with16.Parameters.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                                _with16.Parameters.Add("VERSION_NO_IN", Version1).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with16.CommandText = objWK.MyUserName + ".CBJC_TRN_COST_PKG.CBJC_TRN_COST_INS";
                                _with16.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                                _with16.Parameters.Add("MIS_FLAG_IN", 1).Direction = ParameterDirection.Input;
                            }

                            _with16.Parameters.Add("CBJC_FK_IN", RefJobFk).Direction = ParameterDirection.Input;
                            _with16.Parameters.Add("VENDOR_MST_FK_IN", "").Direction = ParameterDirection.Input;
                            _with16.Parameters.Add("COST_ELEMENT_MST_FK_IN", Freigh_Ele_Fk).Direction = ParameterDirection.Input;
                            _with16.Parameters.Add("LOCATION_MST_FK_IN", Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"])).Direction = ParameterDirection.Input;
                            _with16.Parameters.Add("FREIGHT_TYPE_IN", PayType).Direction = ParameterDirection.Input;
                            _with16.Parameters.Add("CURRENCY_MST_FK_IN", Currfk).Direction = ParameterDirection.Input;
                            _with16.Parameters.Add("ESTIMATED_COST_IN", FrtAmt).Direction = ParameterDirection.Input;
                            _with16.Parameters.Add("TOTAL_COST_IN", FrtAmt).Direction = ParameterDirection.Input;
                            _with16.Parameters.Add("BASIS_FK_IN", 1).Direction = ParameterDirection.Input;
                            _with16.Parameters.Add("RATEPERBASIS_IN", FrtAmt).Direction = ParameterDirection.Input;
                            _with16.Parameters.Add("QUANTITY_IN", 1).Direction = ParameterDirection.Input;
                            _with16.Parameters.Add("EXCHANGE_RATE_IN", 1).Direction = ParameterDirection.Input;
                            _with16.Parameters.Add("EXT_INT_FLAG_IN", 1).Direction = ParameterDirection.Input;
                            _with16.Parameters.Add("SERVICE_MST_FK_IN", 5).Direction = ParameterDirection.Input;
                            _with16.Parameters.Add("INV_SUPPLIER_FK_IN", "").Direction = ParameterDirection.Input;
                            _with16.Parameters.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;
                            _with16.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;
                            RecAfct = _with16.ExecuteNonQuery();
                        }
                        else
                        {
                            var _with13 = objWK.MyCommand;
                            _with13.Parameters.Clear();
                            _with13.Transaction = TRAN;
                            _with13.CommandType = CommandType.StoredProcedure;
                            if (IsUpdate == true)
                            {
                                _with13.CommandText = objWK.MyUserName + ".TRANSPORT_TRN_FD_PKG.TRANSPORT_TRN_FD_UPD";
                                _with13.Parameters.Add("TRANSPORT_TRN_FD_PK_IN", JOB_TRN_FD_PK).Direction = ParameterDirection.Input;
                                _with13.Parameters.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                                _with13.Parameters.Add("VERSION_NO_IN", Version).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with13.CommandText = objWK.MyUserName + ".TRANSPORT_TRN_FD_PKG.TRANSPORT_TRN_FD_INS";
                                _with13.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                                _with13.Parameters.Add("MIS_FLAG_IN", 1).Direction = ParameterDirection.Input;
                            }

                            _with13.Parameters.Add("TRANSPORT_INST_FK_IN", RefJobFk).Direction = ParameterDirection.Input;
                            _with13.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", Freigh_Ele_Fk).Direction = ParameterDirection.Input;
                            _with13.Parameters.Add("FREIGHT_TYPE_IN", PayType).Direction = ParameterDirection.Input;
                            _with13.Parameters.Add("LOCATION_MST_FK_IN", Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"])).Direction = ParameterDirection.Input;
                            if (Convert.ToInt32(Process) == 2)
                            {
                                _with13.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", ConsigneePK).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with13.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", ShipperPK).Direction = ParameterDirection.Input;
                            }
                            _with13.Parameters.Add("FREIGHT_AMT_IN", FrtAmt).Direction = ParameterDirection.Input;
                            _with13.Parameters.Add("CURRENCY_MST_FK_IN", Currfk).Direction = ParameterDirection.Input;
                            _with13.Parameters.Add("BASIS_FK_IN", 1).Direction = ParameterDirection.Input;
                            _with13.Parameters.Add("EXCHANGE_RATE_IN", 1).Direction = ParameterDirection.Input;
                            _with13.Parameters.Add("RATEPERBASIS_IN", FrtAmt).Direction = ParameterDirection.Input;
                            _with13.Parameters.Add("QUANTITY_IN", 1).Direction = ParameterDirection.Input;
                            _with13.Parameters.Add("SERVICE_MST_FK_IN", 5).Direction = ParameterDirection.Input;
                            _with13.Parameters.Add("INV_AGENT_TRN_FK_IN", "").Direction = ParameterDirection.Input;
                            _with13.Parameters.Add("CONSOL_INVOICE_TRN_FK_IN", "").Direction = ParameterDirection.Input;
                            _with13.Parameters.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;
                            _with13.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;
                            RecAfct = _with13.ExecuteNonQuery();

                            var _with14 = objWK.MyCommand;
                            _with14.Parameters.Clear();
                            _with14.Transaction = TRAN;
                            _with14.CommandType = CommandType.StoredProcedure;
                            if (IsUpdate == true)
                            {
                                _with14.CommandText = objWK.MyUserName + ".TRANSPORT_TRN_COST_PKG.TRANSPORT_TRN_COST_UPD";
                                _with14.Parameters.Add("TRANSPORT_TRN_COST_PK_IN", JOB_TRN_COST_PK).Direction = ParameterDirection.Input;
                                _with14.Parameters.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                                _with14.Parameters.Add("VERSION_NO_IN", Version1).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with14.CommandText = objWK.MyUserName + ".TRANSPORT_TRN_COST_PKG.TRANSPORT_TRN_COST_INS";
                                _with14.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                                _with14.Parameters.Add("MIS_FLAG_IN", 1).Direction = ParameterDirection.Input;
                            }

                            _with14.Parameters.Add("TRANSPORT_INST_FK_IN", RefJobFk).Direction = ParameterDirection.Input;
                            _with14.Parameters.Add("VENDOR_MST_FK_IN", "").Direction = ParameterDirection.Input;
                            _with14.Parameters.Add("COST_ELEMENT_MST_FK_IN", Freigh_Ele_Fk).Direction = ParameterDirection.Input;
                            _with14.Parameters.Add("LOCATION_MST_FK_IN", Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"])).Direction = ParameterDirection.Input;
                            _with14.Parameters.Add("FREIGHT_TYPE_IN", PayType).Direction = ParameterDirection.Input;
                            _with14.Parameters.Add("CURRENCY_MST_FK_IN", Currfk).Direction = ParameterDirection.Input;
                            _with14.Parameters.Add("ESTIMATED_COST_IN", FrtAmt).Direction = ParameterDirection.Input;
                            _with14.Parameters.Add("TOTAL_COST_IN", FrtAmt).Direction = ParameterDirection.Input;
                            _with14.Parameters.Add("BASIS_FK_IN", 1).Direction = ParameterDirection.Input;
                            _with14.Parameters.Add("RATEPERBASIS_IN", FrtAmt).Direction = ParameterDirection.Input;
                            _with14.Parameters.Add("QUANTITY_IN", 1).Direction = ParameterDirection.Input;
                            _with14.Parameters.Add("EXCHANGE_RATE_IN", 1).Direction = ParameterDirection.Input;
                            _with14.Parameters.Add("EXT_INT_FLAG_IN", 1).Direction = ParameterDirection.Input;
                            _with14.Parameters.Add("SERVICE_MST_FK_IN", 5).Direction = ParameterDirection.Input;
                            _with14.Parameters.Add("INV_SUPPLIER_FK_IN", "").Direction = ParameterDirection.Input;
                            _with14.Parameters.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;
                            _with14.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;
                            RecAfct = _with14.ExecuteNonQuery();
                        }
                    }
                    //'
                }
                if (RecAfct > 0)
                {
                    TRAN.Commit();
                    arrMessage.Add("All Data Saved Successfully");
                    return arrMessage;
                }
                else
                {
                    TRAN.Rollback();
                }
            }
            catch (OracleException oraexp)
            {
                TRAN.Rollback();
                throw oraexp;
            }
            catch (Exception ex)
            {
                TRAN.Rollback();
                throw ex;
            }
            finally
            {
                objWK.CloseConnection();
            }
            return new ArrayList();
        }

        #endregion "SAVE"

        #region "Save Default"

        public ArrayList SaveDefault(DataSet dsGridDetails = null)
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            DataRow dr = null;
            Int32 RecAfct = default(Int32);
            string strSQL = null;
            try
            {
                strSQL = "DELETE FROM REIM_SET_DEFAULT_TBL";
                var _with17 = objWK.MyCommand;
                _with17.Connection = objWK.MyConnection;
                _with17.CommandType = CommandType.Text;
                _with17.CommandText = strSQL;
                _with17.Transaction = TRAN;
                _with17.ExecuteNonQuery();

                var _with18 = objWK.MyCommand;
                _with18.Transaction = TRAN;
                _with18.Connection = objWK.MyConnection;
                _with18.CommandType = CommandType.StoredProcedure;
                if (dsGridDetails.Tables[0].Rows.Count > 0)
                {
                    foreach (DataRow dr_loopVariable in dsGridDetails.Tables[0].Rows)
                    {
                        dr = dr_loopVariable;
                        _with18.CommandText = objWK.MyUserName + ".REIMBURSABLE_TBL_PKG.REIM_SET_DEFAULT_INS";
                        var _with19 = _with18.Parameters;
                        _with19.Clear();
                        _with19.Add("EXPENSE_DESC_IN", dr["EXPENSE_DESC"]).Direction = ParameterDirection.Input;
                        _with19.Add("VENDOR_NAME_IN", "").Direction = ParameterDirection.Input;
                        _with19.Add("VENDOR_INV_REF_NR_IN", "").Direction = ParameterDirection.Input;
                        _with19.Add("REM_DATE_IN", "").Direction = ParameterDirection.Input;
                        _with19.Add("AMOUNT_IN", "").Direction = ParameterDirection.Input;
                        _with19.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                        _with19.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;
                        _with18.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "REIMBURSABLE_TRN_PK").Direction = ParameterDirection.Output;
                        _with18.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                        RecAfct = _with18.ExecuteNonQuery();
                    }
                }
                if (RecAfct > 0)
                {
                    TRAN.Commit();
                    arrMessage.Add("Data Set as Default");
                    return arrMessage;
                }
                else
                {
                    TRAN.Rollback();
                }
            }
            catch (OracleException oraexp)
            {
                TRAN.Rollback();
                throw oraexp;
            }
            catch (Exception ex)
            {
                TRAN.Rollback();
                throw ex;
            }
            finally
            {
                objWK.CloseConnection();
            }
            return new ArrayList();
        }

        #endregion "Save Default"

        #region "Get FreightElemet FK"

        public int GrtFrtPk()
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            strSQL.Append("SELECT P.FRT_MIS_FK FROM PARAMETERS_TBL P ");
            try
            {
                return Convert.ToInt32(objWF.ExecuteScaler(strSQL.ToString()));
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

        #endregion "Get FreightElemet FK"

        #region "Freight Amount"

        public DataSet GetFrtAmount(string JobType, string JobPK, int FrtEleFK, string BizType, string Process)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            if (Convert.ToInt32(JobType) == 1)
            {
                sb.Append("SELECT JOB_FD.FREIGHT_AMT,JOB_FD.JOB_TRN_FD_PK JOB_TRN_FD_PK, 0 VERSION_NO ");
                sb.Append("  FROM JOB_CARD_TRN JOB, JOB_TRN_FD JOB_FD");
                sb.Append(" WHERE JOB.JOB_CARD_TRN_PK = JOB_FD.JOB_CARD_TRN_FK");
                sb.Append("   AND JOB_FD.FREIGHT_ELEMENT_MST_FK = " + FrtEleFK);
                sb.Append("   AND JOB.JOB_CARD_TRN_PK = " + JobPK);
                sb.Append("   AND JOB.BUSINESS_TYPE = " + BizType);
                sb.Append("   AND JOB.PROCESS_TYPE = " + Process);
            }
            else if (Convert.ToInt32(JobType) == 2)
            {
                sb.Append("SELECT CBJC_FD.FREIGHT_AMT, CBJC_FD.CBJC_TRN_FD_PK JOB_TRN_FD_PK,CBJC_FD.VERSION_NO ");
                sb.Append("  FROM CBJC_TBL CBJC, CBJC_TRN_FD CBJC_FD");
                sb.Append(" WHERE CBJC.CBJC_PK = CBJC_FD.CBJC_FK");
                sb.Append("   AND CBJC_FD.FREIGHT_ELEMENT_MST_FK = " + FrtEleFK);
                sb.Append("   AND CBJC.CBJC_PK = " + JobPK);
                sb.Append("   AND CBJC.BIZ_TYPE = " + BizType);
                sb.Append("   AND CBJC.PROCESS_TYPE = " + Process);
            }
            else
            {
                sb.Append("SELECT TP_FD.FREIGHT_AMT, TP_FD.TRANSPORT_TRN_FD_PK JOB_TRN_FD_PK, TP_FD.VERSION_NO ");
                sb.Append("  FROM TRANSPORT_INST_SEA_TBL TP, TRANSPORT_TRN_FD TP_FD");
                sb.Append(" WHERE TP.TRANSPORT_INST_SEA_PK = TP_FD.TRANSPORT_INST_FK");
                sb.Append("   AND TP_FD.FREIGHT_ELEMENT_MST_FK =" + FrtEleFK);
                sb.Append("   AND TP.BUSINESS_TYPE =" + BizType);
                sb.Append("   AND TP.PROCESS_TYPE =" + Process);
                sb.Append("   AND TP.TRANSPORT_INST_SEA_PK =" + JobPK);
            }
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

        #endregion "Freight Amount"

        #region "Cost Amount"

        public DataSet GetCostAmount(string JobType, string JobPK, int FrtEleFK, string BizType, string Process)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            if (Convert.ToInt32(JobType) == 1)
            {
                sb.Append("SELECT JOB_COST.TOTAL_COST, JOB_COST.JOB_TRN_COST_PK JOB_TRN_COST_PK, 0 VERSION_NO ");
                sb.Append("  FROM JOB_CARD_TRN JOB, JOB_TRN_COST JOB_COST");
                sb.Append(" WHERE JOB.JOB_CARD_TRN_PK = JOB_COST.JOB_CARD_TRN_FK");
                sb.Append("   AND JOB_COST.COST_ELEMENT_MST_FK =" + FrtEleFK);
                sb.Append("   AND JOB.JOB_CARD_TRN_PK =" + JobPK);
                sb.Append("   AND JOB.BUSINESS_TYPE = " + BizType);
                sb.Append("   AND JOB.PROCESS_TYPE = " + Process);
            }
            else if (Convert.ToInt32(JobType) == 2)
            {
                sb.Append("SELECT CBJC_COST.TOTAL_COST, CBJC_COST.CBJC_TRN_COST_PK JOB_TRN_COST_PK,CBJC_COST.VERSION_NO ");
                sb.Append("  FROM CBJC_TBL CBJC, CBJC_TRN_COST CBJC_COST");
                sb.Append(" WHERE CBJC.CBJC_PK = CBJC_COST.CBJC_FK");
                sb.Append("   AND CBJC_COST.COST_ELEMENT_MST_FK = " + FrtEleFK);
                sb.Append("   AND CBJC.CBJC_PK = " + JobPK);
                sb.Append("   AND CBJC.BIZ_TYPE = " + BizType);
                sb.Append("   AND CBJC.PROCESS_TYPE = " + Process);
            }
            else
            {
                sb.Append("SELECT TP_COST.TOTAL_COST,");
                sb.Append("       TP_COST.TRANSPORT_TRN_COST_PK JOB_TRN_COST_PK,");
                sb.Append("       TP_COST.VERSION_NO ");
                sb.Append("  FROM TRANSPORT_INST_SEA_TBL TP, TRANSPORT_TRN_COST TP_COST");
                sb.Append(" WHERE TP.TRANSPORT_INST_SEA_PK = TP_COST.TRANSPORT_INST_FK");
                sb.Append("   AND TP_COST.COST_ELEMENT_MST_FK=" + FrtEleFK);
                sb.Append("   AND TP.BUSINESS_TYPE =" + BizType);
                sb.Append("   AND TP.PROCESS_TYPE =" + Process);
                sb.Append("   AND TP.TRANSPORT_INST_SEA_PK =" + JobPK);
            }
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

        #endregion "Cost Amount"

        #region "Fetch Reim Details"

        public DataSet FetchRemData(Int32 ChkONLD, string FROM_DATE = "", string TO_DATE = "", int Reim_Pk = 0, int Ref_Jobpk = 0, string JobType = "", string BizType = "", string Process = "", string Status = "", Int32 CurrentPage = 1,
        Int32 TotalPage = 0, Int32 IsInvoice = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                var _with20 = objWF.MyCommand.Parameters;
                _with20.Add("REIMBURSABLE_PK_IN", Reim_Pk).Direction = ParameterDirection.Input;
                _with20.Add("JOB_REF_FK_IN", Ref_Jobpk).Direction = ParameterDirection.Input;
                _with20.Add("JOB_TYPE_IN", JobType).Direction = ParameterDirection.Input;
                _with20.Add("STATUS_IN", Status).Direction = ParameterDirection.Input;
                _with20.Add("FROM_DATE_IN", (string.IsNullOrEmpty(FROM_DATE) ? "" : FROM_DATE)).Direction = ParameterDirection.Input;
                _with20.Add("TODATE_IN", (string.IsNullOrEmpty(TO_DATE) ? "" : TO_DATE)).Direction = ParameterDirection.Input;
                _with20.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with20.Add("PROCESS_IN", Process).Direction = ParameterDirection.Input;
                _with20.Add("POST_BACK_IN", ChkONLD).Direction = ParameterDirection.Input;
                _with20.Add("ISINVOICE_IN", IsInvoice).Direction = ParameterDirection.Input;
                _with20.Add("M_MASTERPAGESIZE_IN", RecordsPerPage).Direction = ParameterDirection.Input;
                _with20.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
                _with20.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with20.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("REIMBURSABLE_TBL_PKG", "FETCH_REIM_DETAILS");
                TotalPage = Convert.ToInt32(objWF.MyCommand.Parameters["TOTALPAGE_IN"].Value);
                CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
                if (TotalPage == 0)
                {
                    CurrentPage = 0;
                }
                else
                {
                    CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
                }
                return DS;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        #endregion "Fetch Reim Details"

        #region "Fetch Report Transport Note"

        public DataSet FetchReimClaim(int ReimPK, int BizType, int Process, int JobType)
        {
            WorkFlow objWK = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            DataSet dsData = new DataSet();

            try
            {
                objWK.OpenConnection();
                objWK.MyCommand.Connection = objWK.MyConnection;

                var _with21 = objWK.MyCommand;
                _with21.CommandType = CommandType.StoredProcedure;
                _with21.CommandText = objWK.MyUserName + ".REIMBURSABLE_TBL_PKG.REPORT_PARAMETERS";

                objWK.MyCommand.Parameters.Clear();
                var _with22 = objWK.MyCommand.Parameters;

                _with22.Add("REIMPK_IN", ReimPK).Direction = ParameterDirection.Input;
                _with22.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with22.Add("PROCESS_TYPE_IN", Process).Direction = ParameterDirection.Input;
                _with22.Add("JOB_TYPE_IN", JobType).Direction = ParameterDirection.Input;
                _with22.Add("REIM_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                objWK.MyDataAdapter.SelectCommand = objWK.MyCommand;
                objWK.MyDataAdapter.Fill(dsData);

                return dsData;
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

        public string GetFrt(string Pk = "0")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append(" SELECT NVL(P.FRT_MIS_FK, 0) FROM PARAMETERS_TBL P ");
            try
            {
                return objWF.ExecuteScaler(sb.ToString());
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                return "0";
            }
        }

        #endregion "Fetch Report Transport Note"
    }
}