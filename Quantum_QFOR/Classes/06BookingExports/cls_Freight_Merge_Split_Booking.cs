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

using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections;
using System.Data;

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_Freight_Merge_Split_Booking : CommonFeatures
    {
        #region "Private Variables"

        /// <summary>
        /// The _ pk value main
        /// </summary>
        public long _PkValueMain;
        /// <summary>
        /// The _ pk value trans
        /// </summary>
        private long _PkValueTrans;
        /// <summary>
        /// The object track n trace
        /// </summary>
        private cls_TrackAndTrace objTrackNTrace = new cls_TrackAndTrace();
        /// <summary>
        /// The string ret
        /// </summary>
        public string strRet;

        #endregion "Private Variables"

        /// <summary>
        /// The CHK_ ebk
        /// </summary>
        private Int16 Chk_EBK = 0;

        #region "fetch MaWB Nr"

        /// <summary>
        /// Fetch_s the m awb nr.
        /// </summary>
        /// <param name="BkgNr">The BKG nr.</param>
        /// <param name="usrLocFK">The usr loc fk.</param>
        /// <returns></returns>
        public DataSet Fetch_MAwbNr(string BkgNr, long usrLocFK = 0)
        {
            WorkFlow ObjWk = new WorkFlow();
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();

            try
            {
                strQuery.Append("   select abn.airway_bill_no");
                strQuery.Append("   from airway_bill_trn abn, airway_bill_mst_tbl am");
                strQuery.Append("   where abn.reference_no = '" + BkgNr + "'");

                strQuery.Append(" and am.location_mst_fk=" + usrLocFK);
                strQuery.Append("");

                return ObjWk.GetDataSet(strQuery.ToString());
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

        #endregion "fetch MaWB Nr"

        #region "Fetch Aiwaybill MST Fk "

        /// <summary>
        /// Fecth_s the airway_mst_ fk.
        /// </summary>
        /// <param name="ref_nr">The ref_nr.</param>
        /// <param name="Air_Pk">The air_ pk.</param>
        /// <param name="usrLocFK">The usr loc fk.</param>
        /// <returns></returns>
        public DataSet fecth_Airway_mst_Fk(string ref_nr, string Air_Pk, long usrLocFK = 0)
        {
            WorkFlow ObjWk = new WorkFlow();
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();

            try
            {
                strQuery.Append("select a.airway_bill_mst_fk");
                strQuery.Append("from airway_bill_trn a, airway_bill_mst_tbl am");
                strQuery.Append("where am.airway_bill_mst_pk = a.airway_bill_mst_fk");
                strQuery.Append("and am.airline_mst_fk=" + Air_Pk);
                strQuery.Append("and am.location_mst_fk=" + usrLocFK);
                strQuery.Append("and a.airway_bill_no=" + ref_nr);
                strQuery.Append("");
                return ObjWk.GetDataSet(strQuery.ToString());
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

        #endregion "Fetch Aiwaybill MST Fk "


        /// <summary>
        /// Fetches the eb FRT.
        /// </summary>
        /// <param name="BkgPk">The BKG pk.</param>
        /// <returns></returns>
        public Int16 FetchEBFrt(long BkgPk)
        {
            string sql = "";
            string res = "";
            Int16 check = 0;
            WorkFlow objWK = new WorkFlow();

            sql = "select BOOKING_TRN_AIR_FK from BOOKING_TRN_AIR_FRT_DTLS where BOOKING_TRN_AIR_FK='" + BkgPk + "'";
            res = objWK.ExecuteScaler(sql);
            if (Convert.ToInt32(res) > 0)
            {
                check = 1;
            }
            else
            {
                check = 0;
            }
            return check;
        }

        /// <summary>
        /// Updates up stream.
        /// </summary>
        /// <param name="dsMain">The ds main.</param>
        /// <param name="SelectCommand">The select command.</param>
        /// <param name="IsUpdate">if set to <c>true</c> [is update].</param>
        /// <param name="strTranType">Type of the string tran.</param>
        /// <param name="strContractRefNo">The string contract reference no.</param>
        /// <param name="PkValue">The pk value.</param>
        /// <returns></returns>
        public object UpdateUpStream(DataSet dsMain, OracleCommand SelectCommand, bool IsUpdate, string strTranType, string strContractRefNo, long PkValue)
        {
            WorkFlow objWK = new WorkFlow();
            string strValueArgument = null;
            arrMessage.Clear();
            try
            {
                var _with13 = SelectCommand;
                _with13.CommandType = CommandType.StoredProcedure;
                _with13.CommandText = objWK.MyUserName + ".BOOKING_AIR_PKG.BOOKING_AIR_UPDATE_UPSTREAM";
                SelectCommand.Parameters.Clear();

                _with13.Parameters.Add("BOOKING_AIR_PK_IN", Convert.ToInt64(PkValue)).Direction = ParameterDirection.Input;
                _with13.Parameters.Add("TRANS_REFERED_FROM_IN", Convert.ToInt64(strTranType)).Direction = ParameterDirection.Input;
                _with13.Parameters.Add("TRANS_REF_NO_IN", Convert.ToString(strContractRefNo)).Direction = ParameterDirection.Input;

                _with13.Parameters.Add("ISUPDATE", IsUpdate.ToString()).Direction = ParameterDirection.Input;

                _with13.Parameters.Add("BOOKING_STATUS_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["STATUS"])).Direction = ParameterDirection.Input;

                _with13.Parameters.Add("CHARGEABLE_WEIGHT_IN", Convert.ToDouble(dsMain.Tables["tblMaster"].Rows[0]["CHARGEABLE_WEIGHT"])).Direction = ParameterDirection.Input;

                _with13.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;

                _with13.ExecuteNonQuery();
                arrMessage.Add("All data saved successfully");
                return arrMessage;
            }
            catch (OracleException oraexp)
            {
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
        }

        /// <summary>
        /// Gets the data view.
        /// </summary>
        /// <param name="dtFreight">The dt freight.</param>
        /// <param name="strContractRefNo">The string contract reference no.</param>
        /// <param name="strValueArgument">The string value argument.</param>
        /// <returns></returns>
        private DataView getDataView(DataTable dtFreight, string strContractRefNo, string strValueArgument)
        {
            try
            {
                DataTable dstemp = new DataTable();
                DataRow dr = null;
                Int32 nRowCnt = default(Int32);
                Int32 nColCnt = default(Int32);
                dstemp = dtFreight.Clone();
                for (nRowCnt = 0; nRowCnt <= dtFreight.Rows.Count - 1; nRowCnt++)
                {
                    if (strContractRefNo == getDefault(dtFreight.Rows[nRowCnt]["TRANS_REF_NO"], ""))
                    {
                        dr = dstemp.NewRow();
                        for (nColCnt = 0; nColCnt <= dtFreight.Columns.Count - 1; nColCnt++)
                        {
                            dr[nColCnt] = dtFreight.Rows[nRowCnt][nColCnt];
                        }
                        dstemp.Rows.Add(dr);
                    }
                }
                return dstemp.DefaultView;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Generates the booking no.
        /// </summary>
        /// <param name="nLocationId">The n location identifier.</param>
        /// <param name="nEmployeeId">The n employee identifier.</param>
        /// <param name="nCreatedBy">The n created by.</param>
        /// <param name="objWK">The object wk.</param>
        /// <returns></returns>
        public string GenerateBookingNo(long nLocationId, long nEmployeeId, long nCreatedBy, WorkFlow objWK)
        {
            string functionReturnValue = null;
            functionReturnValue = GenerateProtocolKey("BOOKING (AIR)", nLocationId, nEmployeeId, DateTime.Now, "","" ,"" , nCreatedBy, objWK);
            return functionReturnValue;
            return functionReturnValue;
        }

        /// <summary>
        /// Checks the active job card.
        /// </summary>
        /// <param name="strABEPk">The string abe pk.</param>
        /// <returns></returns>
        public bool CheckActiveJobCard(string strABEPk)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            short intCnt = 0;
            WorkFlow objWF = new WorkFlow();
            string strReturn = null;

            strBuilder.Append(" UPDATE JOB_CARD_AIR_EXP_TBL J ");
            strBuilder.Append(" SET J.JOB_CARD_STATUS = 2, J.JOB_CARD_CLOSED_ON = SYSDATE ");
            strBuilder.Append(" WHERE J.BOOKING_AIR_FK = " + strABEPk);

            try
            {
                intCnt = Convert.ToInt16(objWF.ExecuteScaler(strBuilder.ToString()));
                if (intCnt == 0)
                {
                    return true;
                }
                else
                {
                    return false;
                }
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

        #region "Update Airway Bill Trn"

        /// <summary>
        /// Update_s the airway_ bill_ TRN.
        /// </summary>
        /// <param name="BkgNo">The BKG no.</param>
        /// <param name="AirwayBillNo">The airway bill no.</param>
        /// <param name="AirwayPk">The airway pk.</param>
        /// <param name="cmd">The command.</param>
        /// <param name="AirRefNr">The air reference nr.</param>
        /// <returns></returns>
        public ArrayList Update_Airway_Bill_Trn(string BkgNo, string AirwayBillNo, string AirwayPk, OracleCommand cmd, string AirRefNr = "")
        {
            WorkFlow objWK = new WorkFlow();
            Int16 exe = default(Int16);
            System.Text.StringBuilder strQuery = null;
            arrMessage.Clear();

            try
            {
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();

                strQuery = new System.Text.StringBuilder();
                strQuery.Append(" update airway_bill_mst_tbl AMT ");
                strQuery.Append("   set AMT.total_nos_used = (AMT.total_nos_used - 1),  ");
                strQuery.Append("   AMT.total_nos_cancelled = (AMT.total_nos_cancelled + 1)  ");
                strQuery.Append(" Where AMT.Airway_Bill_Mst_Pk in (select abt.airway_bill_mst_fk  from airway_bill_trn ABT Where ABT.REFERENCE_NO = '" + BkgNo + "') ");
                strQuery.Append("");
                cmd.CommandText = strQuery.ToString();

                exe = Convert.ToInt16(cmd.ExecuteNonQuery());

                cmd.Parameters.Clear();

                strQuery = new System.Text.StringBuilder();
                strQuery.Append(" update airway_bill_mst_tbl AMT ");
                strQuery.Append("   set AMT.total_nos_used = (AMT.total_nos_used - 1),  ");
                strQuery.Append("   AMT.total_nos_cancelled = (AMT.total_nos_cancelled + 1)  ");
                strQuery.Append(" Where AMT.Airway_Bill_Mst_Pk in (select abt.airway_bill_mst_fk  from airway_bill_trn ABT Where ABT.REFERENCE_NO = '" + AirRefNr + "') ");
                strQuery.Append("");
                cmd.CommandText = strQuery.ToString();

                exe = Convert.ToInt16(cmd.ExecuteNonQuery());

                cmd.Parameters.Clear();
                strQuery = new System.Text.StringBuilder();
                strQuery.Append(" update airway_bill_trn ABT ");
                strQuery.Append(" set ABT.Status = 0, ABT.Used_At = 0, ABT.Reference_No = Null ");
                strQuery.Append(" Where ABT.REFERENCE_NO = '" + BkgNo + "'");
                strQuery.Append("");
                cmd.CommandText = strQuery.ToString();

                exe = Convert.ToInt16(cmd.ExecuteNonQuery());

                cmd.Parameters.Clear();
                strQuery = new System.Text.StringBuilder();
                strQuery.Append(" update airway_bill_trn ABT ");
                strQuery.Append(" set ABT.Status = 0, ABT.Used_At = 0, ABT.Reference_No = Null ");
                strQuery.Append(" Where ABT.REFERENCE_NO = '" + AirRefNr + "'");
                strQuery.Append("");
                cmd.CommandText = strQuery.ToString();

                exe = Convert.ToInt16(cmd.ExecuteNonQuery());

                if (AirwayPk != null)
                {
                    cmd.Parameters.Clear();

                    strQuery = new System.Text.StringBuilder();
                    strQuery.Append(" update airway_bill_trn ABT ");
                    strQuery.Append("   set ABT.Status       = 3, ");
                    strQuery.Append("       ABT.Used_At      = 3, ");
                    strQuery.Append("       ABT.Reference_No = '" + BkgNo + "'");
                    strQuery.Append(" Where ABT.Airway_Bill_Mst_Fk = " + AirwayPk);
                    strQuery.Append("   And ABT.AIRWAY_BILL_NO = " + AirwayBillNo);
                    strQuery.Append("");
                    cmd.CommandText = strQuery.ToString();

                    exe = Convert.ToInt16(cmd.ExecuteNonQuery());
                    cmd.Parameters.Clear();

                    strQuery = new System.Text.StringBuilder();
                    strQuery.Append(" update airway_bill_mst_tbl AMT ");
                    strQuery.Append(" set AMT.total_nos_used = ( select count(*) + 1 from airway_bill_trn trn ");
                    strQuery.Append(" where(trn.reference_no Is Not null) and trn.airway_bill_mst_fk= " + AirwayPk);
                    strQuery.Append(" ) Where AMT.Airway_Bill_Mst_Pk = " + AirwayPk);
                    strQuery.Append("");
                    cmd.CommandText = strQuery.ToString();

                    exe = Convert.ToInt16(cmd.ExecuteNonQuery());
                }

                arrMessage.Add("All data saved successfully");

                return arrMessage;
            }
            catch (OracleException oraexp)
            {
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
        }

        #endregion "Update Airway Bill Trn"

        #region "Spetial requirment"

        #region "Spacial Request"

        /// <summary>
        /// Saves the transaction hz SPCL.
        /// </summary>
        /// <param name="SCM">The SCM.</param>
        /// <param name="UserName">Name of the user.</param>
        /// <param name="strSpclRequest">The string SPCL request.</param>
        /// <param name="PkValue">The pk value.</param>
        /// <returns></returns>
        private ArrayList SaveTransactionHZSpcl(OracleCommand SCM, string UserName, string strSpclRequest, long PkValue)
        {
            if (!string.IsNullOrEmpty(strSpclRequest))
            {
                arrMessage.Clear();
                string[] strParam = null;
                strParam = strSpclRequest.Split('~');
                try
                {
                    var _with14 = SCM;
                    _with14.CommandType = CommandType.StoredProcedure;
                    _with14.CommandText = UserName + ".BKG_TRN_AIR_HAZ_SPL_REQ_PKG.BKG_TRN_AIR_HAZ_SPL_REQ_INS";
                    var _with15 = _with14.Parameters;
                    _with15.Clear();
                    //BKG_TRN_AIR_FK_IN()
                    _with15.Add("BOOKING_AIR_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    //OUTER_PACK_TYPE_MST_FK_IN()
                    _with15.Add("OUTER_PACK_TYPE_MST_FK_IN", getDefault(strParam[0], "")).Direction = ParameterDirection.Input;
                    //INNER_PACK_TYPE_MST_FK_IN()
                    _with15.Add("INNER_PACK_TYPE_MST_FK_IN", getDefault(strParam[1], "")).Direction = ParameterDirection.Input;
                    //MIN_TEMP_IN()
                    _with15.Add("MIN_TEMP_IN", getDefault(strParam[2], "")).Direction = ParameterDirection.Input;
                    //MIN_TEMP_UOM_IN()
                    _with15.Add("MIN_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[2]) ? "" : strParam[3]), 0)).Direction = ParameterDirection.Input;
                    //MAX_TEMP_IN()
                    _with15.Add("MAX_TEMP_IN", getDefault(strParam[4], "")).Direction = ParameterDirection.Input;
                    //MAX_TEMP_UOM_IN()
                    _with15.Add("MAX_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[4]) ? "" : strParam[5]), 0)).Direction = ParameterDirection.Input;
                    //FLASH_PNT_TEMP_IN()
                    _with15.Add("FLASH_PNT_TEMP_IN", getDefault(strParam[6], "")).Direction = ParameterDirection.Input;
                    //FLASH_PNT_TEMP_UOM_IN()
                    _with15.Add("FLASH_PNT_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[6]) ? "" : strParam[7]), 0)).Direction = ParameterDirection.Input;
                    //IMDG_CLASS_CODE_IN()
                    _with15.Add("IMDG_CLASS_CODE_IN", getDefault(strParam[8], "")).Direction = ParameterDirection.Input;
                    //UN_NO_IN()
                    _with15.Add("UN_NO_IN", getDefault(strParam[9], "")).Direction = ParameterDirection.Input;
                    //IMO_SURCHARGE_IN()
                    _with15.Add("IMO_SURCHARGE_IN", getDefault(strParam[10], 0)).Direction = ParameterDirection.Input;
                    //SURCHARGE_AMT_IN()
                    _with15.Add("SURCHARGE_AMT_IN", getDefault(strParam[11], 0)).Direction = ParameterDirection.Input;
                    //IS_MARINE_POLLUTANT_IN()
                    _with15.Add("IS_MARINE_POLLUTANT_IN", getDefault(strParam[12], 0)).Direction = ParameterDirection.Input;
                    //EMS_NUMBER_IN()
                    _with15.Add("EMS_NUMBER_IN", getDefault(strParam[13], "")).Direction = ParameterDirection.Input;
                    //RETURN_VALUE()
                    _with15.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with14.ExecuteNonQuery();
                    arrMessage.Add("All data saved successfully");
                    return arrMessage;
                }
                catch (OracleException oraexp)
                {
                    arrMessage.Add(oraexp.Message);
                    return arrMessage;
                }
                catch (Exception ex)
                {
                    arrMessage.Add(ex.Message);
                    return arrMessage;
                }
            }
            arrMessage.Add("All data saved successfully");
            return arrMessage;
        }

        /// <summary>
        /// Fetches the SPCL req.
        /// </summary>
        /// <param name="strPK">The string pk.</param>
        /// <returns></returns>
        public DataTable fetchSpclReq(string strPK)
        {
            try
            {
                if (!string.IsNullOrEmpty(strPK))
                {
                    System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
                    strQuery.Append("SELECT BKG_TRN_AIR_HAZ_SPL_PK,");
                    strQuery.Append("BOOKING_AIR_FK,");
                    strQuery.Append("OUTER_PACK_TYPE_MST_FK,");
                    strQuery.Append("INNER_PACK_TYPE_MST_FK,");
                    strQuery.Append("MIN_TEMP,");
                    strQuery.Append("MIN_TEMP_UOM,");
                    strQuery.Append("MAX_TEMP,");
                    strQuery.Append("MAX_TEMP_UOM,");
                    strQuery.Append("FLASH_PNT_TEMP,");
                    strQuery.Append("FLASH_PNT_TEMP_UOM,");
                    strQuery.Append("IMDG_CLASS_CODE,");
                    strQuery.Append("UN_NO,");
                    strQuery.Append("IMO_SURCHARGE,");
                    strQuery.Append("SURCHARGE_AMT,");
                    strQuery.Append("IS_MARINE_POLLUTANT,");
                    strQuery.Append("EMS_NUMBER FROM BKG_TRN_AIR_HAZ_SPL_REQ Q");
                    //,BKG_TRN_AIR_FCL_LCL QT,BKG_AIR_TBL QM" & vbCrLf)
                    strQuery.Append("WHERE ");
                    strQuery.Append("Q.BOOKING_AIR_FK=" + strPK);
                    //strQuery.Append("QT.BKG_AIR_FK=QM.BKG_AIR_PK" & vbCrLf)
                    //strQuery.Append("AND QT.TRANS_REF_NO='" & strRef & "'" & vbCrLf)
                    //strQuery.Append("AND QM.BKG_AIR_PK=" & strPK)
                    return (new WorkFlow()).GetDataTable(strQuery.ToString());
                }
                else
                {
                    return null;
                }
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Saves the transaction reefer.
        /// </summary>
        /// <param name="SCM">The SCM.</param>
        /// <param name="UserName">Name of the user.</param>
        /// <param name="strSpclRequest">The string SPCL request.</param>
        /// <param name="PkValue">The pk value.</param>
        /// <returns></returns>
        private ArrayList SaveTransactionReefer(OracleCommand SCM, string UserName, string strSpclRequest, long PkValue)
        {
            if (!string.IsNullOrEmpty(strSpclRequest))
            {
                arrMessage.Clear();
                string[] strParam = null;
                strParam = strSpclRequest.Split('~');
                try
                {
                    var _with16 = SCM;
                    _with16.CommandType = CommandType.StoredProcedure;
                    _with16.CommandText = UserName + ".BKG_TRN_AIR_REF_SPL_REQ_PKG.BKG_TRN_AIR_REF_SPL_REQ_INS";
                    var _with17 = _with16.Parameters;
                    _with17.Clear();
                    //BOOKING_TRN_AIR_FK_IN()
                    _with17.Add("BOOKING_AIR_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    //VENTILATION_IN()
                    _with17.Add("VENTILATION_IN", strParam[0]).Direction = ParameterDirection.Input;
                    //AIR_COOL_METHOD_IN()
                    _with17.Add("AIR_COOL_METHOD_IN", strParam[1]).Direction = ParameterDirection.Input;
                    //HUMIDITY_FACTOR_IN()
                    _with17.Add("HUMIDITY_FACTOR_IN", getDefault(strParam[2], "")).Direction = ParameterDirection.Input;
                    //IS_PERSHIABLE_GOODS_IN()
                    _with17.Add("IS_PERISHABLE_GOODS_IN", strParam[3]).Direction = ParameterDirection.Input;
                    //Sivachandran 26Jun2008 CR Date 04/06/2008 For Reefer special Requirment
                    //MIN_TEMP_IN()
                    _with17.Add("MIN_TEMP_IN", getDefault(strParam[4], "")).Direction = ParameterDirection.Input;
                    //MIN_TEMP_UOM_IN()
                    _with17.Add("MIN_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[4]) ? "" : strParam[5]), 0)).Direction = ParameterDirection.Input;
                    //MAX_TEMP_IN()
                    _with17.Add("MAX_TEMP_IN", getDefault(strParam[6], "")).Direction = ParameterDirection.Input;
                    //MAX_TEMP_UOM_IN()
                    _with17.Add("MAX_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[6]) ? "" : strParam[7]), 0)).Direction = ParameterDirection.Input;
                    //PACK_TYPE_MST_FK_IN()
                    _with17.Add("PACK_TYPE_MST_FK_IN", getDefault(strParam[8], "")).Direction = ParameterDirection.Input;
                    //PACK_COUNT_IN()
                    _with17.Add("PACK_COUNT_IN", getDefault(strParam[9], 0)).Direction = ParameterDirection.Input;
                    //sivachandran 26Jun2008 CR Date 04/06/2008 For Reefer special Requirment
                    _with17.Add("HAULAGE_IN", "").Direction = ParameterDirection.Input;
                    _with17.Add("GENSET_IN", "").Direction = ParameterDirection.Input;
                    _with17.Add("CO2_IN", "").Direction = ParameterDirection.Input;
                    _with17.Add("O2_IN", "").Direction = ParameterDirection.Input;
                    _with17.Add("REQ_SET_TEMP_IN", "").Direction = ParameterDirection.Input;
                    _with17.Add("REQ_SET_TEMP_UOM_IN", "").Direction = ParameterDirection.Input;
                    _with17.Add("AIR_VENTILATION_IN", "").Direction = ParameterDirection.Input;
                    _with17.Add("AIR_VENTILATION_UOM_IN", "").Direction = ParameterDirection.Input;
                    _with17.Add("DEHUMIDIFIER_IN", "").Direction = ParameterDirection.Input;
                    _with17.Add("FLOORDRAINS_IN", "").Direction = ParameterDirection.Input;
                    _with17.Add("DEFROSTING_INTERVAL_IN", "").Direction = ParameterDirection.Input;
                    //End
                    //RETURN_VALUE()
                    _with17.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with16.ExecuteNonQuery();
                    arrMessage.Add("All data saved successfully");
                    return arrMessage;
                }
                catch (OracleException oraexp)
                {
                    arrMessage.Add(oraexp.Message);
                    return arrMessage;
                }
                catch (Exception ex)
                {
                    arrMessage.Add(ex.Message);
                    return arrMessage;
                }
            }
            arrMessage.Add("All data saved successfully");
            return arrMessage;
        }

        /// <summary>
        /// Fetches the SPCL req reefer.
        /// </summary>
        /// <param name="strPK">The string pk.</param>
        /// <returns></returns>
        public DataTable fetchSpclReqReefer(string strPK)
        {
            try
            {
                if (!string.IsNullOrEmpty(strPK))
                {
                    System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
                    strQuery.Append("SELECT BKG_TRN_AIR_REF_SPL_PK,");
                    strQuery.Append("BOOKING_AIR_FK,");
                    strQuery.Append("VENTILATION,");
                    strQuery.Append("AIR_COOL_METHOD,");
                    strQuery.Append("HUMIDITY_FACTOR,");
                    strQuery.Append("IS_PERISHABLE_GOODS,");
                    //'sivachandran 26Jun2008 CR Date 04/06/2008 For Reefer special Requirment
                    strQuery.Append("MIN_TEMP,");
                    strQuery.Append("MIN_TEMP_UOM,");
                    strQuery.Append("MAX_TEMP,");
                    strQuery.Append("MAX_TEMP_UOM,");
                    strQuery.Append("PACK_TYPE_MST_FK,");
                    strQuery.Append("Q.PACK_COUNT ");
                    strQuery.Append("FROM BKG_TRN_AIR_REF_SPL_REQ Q");
                    //,BKG_TRN_AIR_FCL_LCL QT,BKG_AIR_TBL QM" & vbCrLf)
                    strQuery.Append("WHERE ");
                    strQuery.Append("Q.BOOKING_AIR_FK=" + strPK);
                    //strQuery.Append("BKG_AIR_TBL QM," & vbCrLf)
                    //strQuery.Append("BKG_TRN_AIR_FCL_LCL QT" & vbCrLf)
                    //strQuery.Append("WHERE " & vbCrLf)
                    //strQuery.Append("Q.BKG_TRN_AIR_FK=QT.BKG_TRN_AIR_PK" & vbCrLf)
                    //strQuery.Append("AND QT.BKG_AIR_FK=QM.BKG_AIR_PK" & vbCrLf)
                    //strQuery.Append("AND QT.TRANS_REF_NO='" & strRef & "'" & vbCrLf)
                    //strQuery.Append("AND QM.BKG_AIR_PK=" & strPK)
                    return (new WorkFlow()).GetDataTable(strQuery.ToString());
                }
                else
                {
                    return null;
                }
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the SPCL req odc.
        /// </summary>
        /// <param name="strPK">The string pk.</param>
        /// <returns></returns>
        public DataTable fetchSpclReqODC(string strPK)
        {
            try
            {
                if (!string.IsNullOrEmpty(strPK))
                {
                    System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
                    strQuery.Append("SELECT ");
                    strQuery.Append("BKG_TRN_AIR_ODC_SPL_PK,");
                    strQuery.Append("BOOKING_AIR_FK,");
                    strQuery.Append("LENGTH,");
                    strQuery.Append("LENGTH_UOM_MST_FK,");
                    strQuery.Append("HEIGHT,");
                    strQuery.Append("HEIGHT_UOM_MST_FK,");
                    strQuery.Append("WIDTH,");
                    strQuery.Append("WIDTH_UOM_MST_FK,");
                    strQuery.Append("WEIGHT,");
                    strQuery.Append("WEIGHT_UOM_MST_FK,");
                    strQuery.Append("VOLUME,");
                    strQuery.Append("VOLUME_UOM_MST_FK,");
                    strQuery.Append("SLOT_LOSS,");
                    strQuery.Append("LOSS_QUANTITY,");
                    strQuery.Append("APPR_REQ ");
                    //Snigdharani - 03/03/2009 - EBooking Integration
                    //strQuery.Append("NO_OF_SLOTS " & vbCrLf)
                    strQuery.Append("FROM BKG_TRN_AIR_ODC_SPL_REQ Q");
                    //,BKG_TRN_AIR_FCL_LCL QT,BKG_AIR_TBL QM" & vbCrLf)
                    strQuery.Append("WHERE ");
                    strQuery.Append("Q.BOOKING_AIR_FK=" + strPK);
                    //strQuery.Append("BKG_AIR_TBL QM," & vbCrLf)
                    //strQuery.Append("BKG_TRN_AIR_FCL_LCL QT" & vbCrLf)
                    //strQuery.Append("WHERE " & vbCrLf)
                    //strQuery.Append("Q.BKG_TRN_AIR_FK=QT.BKG_TRN_AIR_PK" & vbCrLf)
                    //strQuery.Append("AND QT.BKG_AIR_FK=QM.BKG_AIR_PK" & vbCrLf)
                    //strQuery.Append("AND QT.TRANS_REF_NO='" & strRef & "'" & vbCrLf)
                    //strQuery.Append("AND QM.BKG_AIR_PK=" & strPK)
                    return (new WorkFlow()).GetDataTable(strQuery.ToString());
                }
                else
                {
                    return null;
                }
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Saves the transaction odc.
        /// </summary>
        /// <param name="SCM">The SCM.</param>
        /// <param name="UserName">Name of the user.</param>
        /// <param name="strSpclRequest">The string SPCL request.</param>
        /// <param name="PkValue">The pk value.</param>
        /// <returns></returns>
        private ArrayList SaveTransactionODC(OracleCommand SCM, string UserName, string strSpclRequest, long PkValue)
        {
            if (!string.IsNullOrEmpty(strSpclRequest))
            {
                arrMessage.Clear();
                string[] strParam = null;
                strParam = strSpclRequest.Split('~');
                try
                {
                    var _with18 = SCM;
                    _with18.CommandType = CommandType.StoredProcedure;
                    _with18.CommandText = UserName + ".BKG_TRN_AIR_ODC_SPL_REQ_PKG.BKG_TRN_AIR_ODC_SPL_REQ_INS";
                    var _with19 = _with18.Parameters;
                    _with19.Clear();
                    //BKG_TRN_AIR_FK_IN()
                    _with19.Add("BOOKING_AIR_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    //LENGTH_IN()
                    _with19.Add("LENGTH_IN", getDefault(strParam[0], "")).Direction = ParameterDirection.Input;
                    //LENGTH_UOM_MST_FK_IN()
                    _with19.Add("LENGTH_UOM_MST_FK_IN", getDefault((string.IsNullOrEmpty(strParam[0]) ? "" : strParam[1]), 0)).Direction = ParameterDirection.Input;
                    //HEIGHT_IN()
                    _with19.Add("HEIGHT_IN", getDefault(strParam[2], "")).Direction = ParameterDirection.Input;
                    //HEIGHT_UOM_MST_FK_IN()
                    _with19.Add("HEIGHT_UOM_MST_FK_IN", getDefault((string.IsNullOrEmpty(strParam[2]) ? "" : strParam[3]), 0)).Direction = ParameterDirection.Input;
                    //WIDTH_IN()
                    _with19.Add("WIDTH_IN", getDefault(strParam[4], 0)).Direction = ParameterDirection.Input;
                    //WIDTH_UOM_MST_FK_IN()
                    _with19.Add("WIDTH_UOM_MST_FK_IN", getDefault((string.IsNullOrEmpty(strParam[4]) ? "" : strParam[5]), 0)).Direction = ParameterDirection.Input;
                    //WEIGHT_IN()
                    _with19.Add("WEIGHT_IN", getDefault(strParam[6], "")).Direction = ParameterDirection.Input;
                    //WEIGHT_UOM_MST_FK_IN()
                    _with19.Add("WEIGHT_UOM_MST_FK_IN", getDefault(strParam[7], 0)).Direction = ParameterDirection.Input;
                    //VOLUME_IN()
                    _with19.Add("VOLUME_IN", getDefault(strParam[8], "")).Direction = ParameterDirection.Input;
                    //VOLUME_UOM_MST_FK_IN()
                    _with19.Add("VOLUME_UOM_MST_FK_IN", getDefault(strParam[9], 0)).Direction = ParameterDirection.Input;
                    //SLOT_LOSS_IN()
                    _with19.Add("SLOT_LOSS_IN", getDefault(strParam[10], 0)).Direction = ParameterDirection.Input;
                    //LOSS_QUANTITY_IN()
                    _with19.Add("LOSS_QUANTITY_IN", getDefault(strParam[11], "")).Direction = ParameterDirection.Input;
                    //APPR_REQ_IN()
                    _with19.Add("APPR_REQ_IN", getDefault(strParam[12], 0)).Direction = ParameterDirection.Input;
                    //.Add("NO_OF_SLOTS", getDefault(strParam(13), 0)).Direction = ParameterDirection.Input
                    //RETURN_VALUE()
                    _with19.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with18.ExecuteNonQuery();
                    arrMessage.Add("All data saved successfully");
                    return arrMessage;
                }
                catch (OracleException oraexp)
                {
                    arrMessage.Add(oraexp.Message);
                    return arrMessage;
                }
                catch (Exception ex)
                {
                    arrMessage.Add(ex.Message);
                    return arrMessage;
                }
            }
            arrMessage.Add("All data saved successfully");
            return arrMessage;
        }

        #endregion "Spacial Request"

        #endregion "Spetial requirment"

        #region "Save Fright and Other charge"

        /// <summary>
        /// Save_s the freight_ other charge.
        /// </summary>
        /// <param name="DsFreight">The ds freight.</param>
        /// <param name="DsOthercharge">The ds othercharge.</param>
        /// <param name="Int_Bookingpk">The int_ bookingpk.</param>
        /// <returns></returns>
        public ArrayList Save_Freight_OtherCharge(DataSet DsFreight, DataSet DsOthercharge, int Int_Bookingpk)
        {
            try
            {
                int Rowcnt = 0;
                int Int_Booking_pk = 0;
                WorkFlow objWK = new WorkFlow();
                OracleTransaction TRAN = null;
                objWK.OpenConnection();
                TRAN = objWK.MyConnection.BeginTransaction();
                arrMessage.Clear();
                OracleCommand insCommand = new OracleCommand();
                OracleCommand updCommand = new OracleCommand();
                objWK.MyCommand.Transaction = TRAN;

                if (Int_Bookingpk > 0)
                {
                    if (DsFreight.Tables[0].Rows.Count > 0)
                    {
                        for (Rowcnt = 0; Rowcnt <= DsFreight.Tables[0].Rows.Count - 1; Rowcnt++)
                        {
                            var _with20 = updCommand;
                            updCommand.Parameters.Clear();
                            _with20.Connection = objWK.MyConnection;
                            _with20.CommandType = CommandType.StoredProcedure;
                            _with20.CommandText = objWK.MyUserName + ".MERGE_BOOKING_SEA_PKG.MERGE_AIRBOOKING_FREIGHT_UPD";
                            var _with21 = _with20.Parameters;
                            _with21.Add("BOOKING_AIR_PK_IN", DsFreight.Tables[0].Rows[Rowcnt]["BOOKING_SEA_FK"]).Direction = ParameterDirection.Input;
                            _with21.Add("BOOKING_TRN_AIR_FRT_PK_IN", DsFreight.Tables[0].Rows[Rowcnt]["FREIGHT_ELEMENT_MST_PK"]).Direction = ParameterDirection.Input;
                            _with21.Add("CURRENCY_MST_FK_IN", DsFreight.Tables[0].Rows[Rowcnt]["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                            _with21.Add("EXCHANGE_RATE_IN", DsFreight.Tables[0].Rows[Rowcnt]["EXCHANGE_RATE"]).Direction = ParameterDirection.Input;
                            _with21.Add("TARIFF_RATE_IN", DsFreight.Tables[0].Rows[Rowcnt]["AMOUNT"]).Direction = ParameterDirection.Input;
                            _with21.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                            var _with22 = objWK.MyDataAdapter;
                            _with22.UpdateCommand = updCommand;
                            _with22.UpdateCommand.Transaction = TRAN;
                            _with22.UpdateCommand.ExecuteNonQuery();
                            Int_Booking_pk = Convert.ToInt32(updCommand.Parameters["RETURN_VALUE"].Value);
                        }
                    }
                }
                if (Int_Bookingpk > 0)
                {
                    arrMessage = SaveBooking_Other_Freight(DsOthercharge, Int_Bookingpk, TRAN);
                }
                if (Int_Booking_pk > 0)
                {
                    arrMessage.Add("All Data Save Sucessfully");
                    return arrMessage;
                }
                else
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        /// <summary>
        /// Saves the booking_ other_ freight.
        /// </summary>
        /// <param name="dsMain">The ds main.</param>
        /// <param name="PkValue">The pk value.</param>
        /// <param name="TRAN">The tran.</param>
        /// <returns></returns>
        public ArrayList SaveBooking_Other_Freight(DataSet dsMain, long PkValue, OracleTransaction TRAN = null)
        {
            Int32 Rowcnt = default(Int32);
            Int32 RecAfct = default(Int32);
            WorkFlow objWK = new WorkFlow();
            arrMessage.Clear();
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            objWK.MyConnection = TRAN.Connection;
            try
            {
                if (PkValue > 0)
                {
                    for (Rowcnt = 0; Rowcnt <= dsMain.Tables[0].Rows.Count - 1; Rowcnt++)
                    {
                        var _with23 = insCommand;
                        insCommand.Parameters.Clear();
                        _with23.Connection = objWK.MyConnection;
                        _with23.CommandType = CommandType.StoredProcedure;
                        _with23.CommandText = objWK.MyUserName + ".MERGE_BOOKING_SEA_PKG.MERGE_AIRBKG_OTHERCHARGE_INS";
                        var _with24 = _with23.Parameters;
                        _with24.Add("BOOKING_TRN_AIR_FK", PkValue).Direction = ParameterDirection.Input;
                        _with24.Add("FREIGHT_ELEMENT_MST_FK_IN", dsMain.Tables[0].Rows[Rowcnt]["FREIGHT_ELEMENT_MST_FK"]).Direction = ParameterDirection.Input;
                        _with24.Add("CURRENCY_MST_FK_IN", dsMain.Tables[0].Rows[Rowcnt]["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                        _with24.Add("AMOUNT_IN", dsMain.Tables[0].Rows[Rowcnt]["AMOUNT"]).Direction = ParameterDirection.Input;
                        _with24.Add("FREIGHT_TYPE_IN", dsMain.Tables[0].Rows[Rowcnt]["FREIGHT_TYPE"]).Direction = ParameterDirection.Input;

                        _with24.Add("LOCATION_MST_FK_IN", dsMain.Tables[0].Rows[Rowcnt]["LOCATION_MST_FK"]).Direction = ParameterDirection.Input;
                        _with24.Add("FRTPAYER_CUST_MST_FK_IN", dsMain.Tables[0].Rows[Rowcnt]["FRTPAYER_CUST_MST_FK"]).Direction = ParameterDirection.Input;
                        _with24.Add("EXCHANGE_RATE_IN", dsMain.Tables[0].Rows[Rowcnt]["EXCHANGE_RATE"]).Direction = ParameterDirection.Input;
                        _with24.Add("PAYMENT_TYPE_IN", dsMain.Tables[0].Rows[Rowcnt]["PAYMENT_TYPE"]).Direction = ParameterDirection.Input;
                        _with24.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;

                        var _with25 = updCommand;
                        updCommand.Parameters.Clear();
                        _with25.Connection = objWK.MyConnection;
                        _with25.CommandType = CommandType.StoredProcedure;
                        _with25.CommandText = objWK.MyUserName + ".MERGE_BOOKING_SEA_PKG.MERGE_AIRBKG_OTHERCHARGE_UPD";
                        var _with26 = _with25.Parameters;
                        _with26.Add("BOOKING_TRN_AIR_FK", PkValue).Direction = ParameterDirection.Input;
                        _with26.Add("FREIGHT_ELEMENT_MST_FK_IN", dsMain.Tables[0].Rows[Rowcnt]["FREIGHT_ELEMENT_MST_FK"]).Direction = ParameterDirection.Input;
                        _with26.Add("CURRENCY_MST_FK_IN", dsMain.Tables[0].Rows[Rowcnt]["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                        _with26.Add("AMOUNT_IN", dsMain.Tables[0].Rows[Rowcnt]["AMOUNT"]).Direction = ParameterDirection.Input;
                        _with26.Add("FREIGHT_TYPE_IN", dsMain.Tables[0].Rows[Rowcnt]["FREIGHT_TYPE"]).Direction = ParameterDirection.Input;

                        _with26.Add("LOCATION_MST_FK_IN", dsMain.Tables[0].Rows[Rowcnt]["LOCATION_MST_FK"]).Direction = ParameterDirection.Input;
                        _with26.Add("FRTPAYER_CUST_MST_FK_IN", dsMain.Tables[0].Rows[Rowcnt]["FRTPAYER_CUST_MST_FK"]).Direction = ParameterDirection.Input;
                        _with26.Add("EXCHANGE_RATE_IN", dsMain.Tables[0].Rows[Rowcnt]["EXCHANGE_RATE"]).Direction = ParameterDirection.Input;
                        _with26.Add("PAYMENT_TYPE_IN", dsMain.Tables[0].Rows[Rowcnt]["PAYMENT_TYPE"]).Direction = ParameterDirection.Input;
                        _with26.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        var _with27 = objWK.MyDataAdapter;
                        if (!string.IsNullOrEmpty(dsMain.Tables[0].Rows[Rowcnt]["JOB_TRN_SEA_EXP_OTH_PK"].ToString()))
                        {
                            _with27.InsertCommand = insCommand;
                            _with27.InsertCommand.Transaction = TRAN;
                            RecAfct = _with27.InsertCommand.ExecuteNonQuery();
                        }
                        else
                        {
                            _with27.UpdateCommand = updCommand;
                            _with27.UpdateCommand.Transaction = TRAN;
                            RecAfct = _with27.UpdateCommand.ExecuteNonQuery();
                        }
                    }
                }

                if (RecAfct > 0 | PkValue > 0)
                {
                    arrMessage.Add("All Data Save Sucessfully");
                    return arrMessage;
                }
                else
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion "Save Fright and Other charge"
    }
}