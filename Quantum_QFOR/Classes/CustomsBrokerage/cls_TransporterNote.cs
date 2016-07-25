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
using Oracle.DataAccess.Types;
using System;
using System.Collections;
using System.Data;
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    public class cls_TransporterNote : CommonFeatures
    {

        cls_SeaBookingEntry objVesselVoyage = new cls_SeaBookingEntry();
        #region "Get Main Details"
        public DataSet FetchMainData(int TPNOTEPK = 0, string JOBPK = "0", string JcType = "", int BizType = 0, int ProcessType = 0, int CBJC = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                var _with1 = objWF.MyCommand.Parameters;
                _with1.Add("TRANSPORT_INST_SEA_PK_IN", TPNOTEPK).Direction = ParameterDirection.Input;
                _with1.Add("JOBPK_IN", (string.IsNullOrEmpty(JOBPK) ? "0" : JOBPK)).Direction = ParameterDirection.Input;
                _with1.Add("JCTYPE_IN", JcType).Direction = ParameterDirection.Input;
                _with1.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with1.Add("PROCESS_TYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with1.Add("CBJC_IN", CBJC).Direction = ParameterDirection.Input;
                _with1.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("TRANSPORT_INST_SEA_TBL_PKG", "FETCH_TRNNOTE_DETAILS");
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
        #endregion

        #region "Get Container Details"
        public DataSet FetchContainerDetails(int TPNOTEPK = 0, string JOBPK = "0", string JcType = "", string CargoType = "0", int BizType = 0, int ProcessType = 0, int CBJC = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                var _with2 = objWF.MyCommand.Parameters;
                _with2.Add("TPNOTE_FK_IN", TPNOTEPK).Direction = ParameterDirection.Input;
                _with2.Add("JOBPK_IN", (string.IsNullOrEmpty(JOBPK) ? "0" : JOBPK)).Direction = ParameterDirection.Input;
                _with2.Add("JCTYPE_IN", JcType).Direction = ParameterDirection.Input;
                _with2.Add("CARGO_TYPE_IN", CargoType).Direction = ParameterDirection.Input;
                _with2.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with2.Add("PROCESS_TYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with2.Add("CBJC_IN", CBJC).Direction = ParameterDirection.Input;
                _with2.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("TRANSPORT_TRN_CONT_PKG", "FETCH_CONT_DETAILS");
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
        #endregion

        #region "Jobcard Enhance Search"
        public string FetchAllJobcard(string strcond, string loc = "0")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strReq = null;
            string strSERACH_IN = null;
            string BizType = null;
            string ProcessType = null;
            string SearchFlag = null;
            int JobType = 0;
            var strNull = DBNull.Value;
            arr = strcond.Split('~');
            if (arr.Length > 0)
                strReq = Convert.ToString(arr.GetValue(0));
            if (arr.Length > 1)
                strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                SearchFlag = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                BizType = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                ProcessType = Convert.ToString(arr.GetValue(4));
            if (arr.Length > 5)
                loc = Convert.ToString(arr.GetValue(5));
            if (arr.Length > 6)
                JobType = Convert.ToInt32(arr.GetValue(6));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_JOBCARD_PKG.GET_JOBCARDS";

                var _with3 = selectCommand.Parameters;
                _with3.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with3.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with3.Add("SEARCH_FLAG_IN", SearchFlag).Direction = ParameterDirection.Input;
                _with3.Add("BUSINESS_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with3.Add("PROCESS_IN", ProcessType).Direction = ParameterDirection.Input;
                _with3.Add("LOC_IN", loc).Direction = ParameterDirection.Input;
                _with3.Add("JC_TYPE_IN", JobType).Direction = ParameterDirection.Input;
                _with3.Add("RETURN_VALUE", OracleDbType.Clob, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                OracleClob clob = null;
                clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        #endregion

        #region "SAVE"
        public ArrayList Save(DataSet M_DataSet, DataSet dsContainerData, long TPNotePK, bool isEdting, DataSet dsIncomeChargeDetails = null, DataSet dsExpenseChargeDetails = null, int Biztype = 0, int Preocess = 0, string CargoType = "0", string TpNoteRefNr = "",
        DataSet dsTruckMainData = null, DataSet dsDocDetails = null, DataSet dsTrackDetails = null, string sid = "", string polid = "")
        {
            objVesselVoyage.ConfigurationPK = M_Configuration_PK;
            objVesselVoyage.CREATED_BY = M_CREATED_BY_FK;
            string TpNoteRefNNumber = null;
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            Int32 RecAfct = default(Int32);

            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();

            OracleCommand insContainerDetails = new OracleCommand();
            OracleCommand updContainerDetails = new OracleCommand();
            OracleCommand delContainerDetails = new OracleCommand();

            OracleCommand updTruckMainDetails = new OracleCommand();
            OracleCommand insTruckMainDetails = new OracleCommand();
            OracleCommand delTruckMainDetails = new OracleCommand();

            if (isEdting == false)
            {
                if (Biztype == 2 & Preocess == 1)
                {
                    TpNoteRefNNumber = GenerateProtocolKey("TRANSPORT INSTRUCTION SEA EXPORT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), DateTime.Today, "", "", polid, M_LAST_MODIFIED_BY_FK,new WorkFlow() , sid);
                }
                else if (Biztype == 2 & Preocess == 2)
                {
                    TpNoteRefNNumber = GenerateProtocolKey("TRANSPORT INSTRUCTION SEA IMPORT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), DateTime.Today, "", "", polid, M_LAST_MODIFIED_BY_FK, new WorkFlow(), sid);
                }
                else if (Biztype == 1 & Preocess == 1)
                {
                    TpNoteRefNNumber = GenerateProtocolKey("TRANSPORT INSTRUCTION AIR EXPORT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), DateTime.Today, "", "", polid, M_LAST_MODIFIED_BY_FK, new WorkFlow(), sid);
                }
                else
                {
                    TpNoteRefNNumber = GenerateProtocolKey("TRANSPORT INSTRUCTION AIR IMPORT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), DateTime.Today, "", "", polid, M_LAST_MODIFIED_BY_FK, new WorkFlow(), sid);
                }

            }
            else
            {
                TpNoteRefNNumber = TpNoteRefNr;
            }
            try
            {
                var _with4 = insCommand;
                _with4.Connection = objWK.MyConnection;
                _with4.CommandType = CommandType.StoredProcedure;
                _with4.CommandText = objWK.MyUserName + ".TRANSPORT_INST_SEA_TBL_PKG.TPNOTE_INST_SEA_TBL_INS";
                var _with5 = _with4.Parameters;

                insCommand.Parameters.Add("TRANS_INST_REF_NO_IN", TpNoteRefNNumber).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("TRANS_INST_DATE_IN", OracleDbType.Date, 20, "TRANS_INST_DATE").Direction = ParameterDirection.Input;
                insCommand.Parameters["TRANS_INST_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("BUSINESS_TYPE_IN", OracleDbType.Int32, 1, "BUSINESS_TYPE").Direction = ParameterDirection.Input;
                insCommand.Parameters["BUSINESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("PROCESS_TYPE_IN", OracleDbType.Int32, 1, "PROCESS_TYPE").Direction = ParameterDirection.Input;
                insCommand.Parameters["PROCESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CUST_CAT_IN", OracleDbType.Int32, 1, "CUSTOMER_CATEGORY").Direction = ParameterDirection.Input;
                insCommand.Parameters["CUST_CAT_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("JC_TYPE_N", OracleDbType.Int32, 1, "JC_TYPE").Direction = ParameterDirection.Input;
                insCommand.Parameters["JC_TYPE_N"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("TP_CBJC_JC_IN", OracleDbType.Int32, 1, "TP_CBJC_JC").Direction = ParameterDirection.Input;
                insCommand.Parameters["TP_CBJC_JC_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("JOB_CARD_FK_IN", OracleDbType.Varchar2, 200, "JOB_CARD_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["JOB_CARD_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CARGO_TYPE_IN", OracleDbType.Int32, 1, "CARGO_TYPE").Direction = ParameterDirection.Input;
                insCommand.Parameters["CARGO_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("COMMODITY_GROUP_MST_FK_IN", OracleDbType.Int32, 10, "COMMODITY_GROUP_MST_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["COMMODITY_GROUP_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("TP_CUSTOMER_MST_FK_IN", OracleDbType.Int32, 10, "TP_CUSTOMER_MST_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["TP_CUSTOMER_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("TP_SHIPPER_MST_FK_IN", OracleDbType.Int32, 10, "TP_SHIPPER_MST_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["TP_SHIPPER_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("TP_CONSIGNEE_MST_FK_IN", OracleDbType.Int32, 10, "TP_CONSIGNEE_MST_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["TP_CONSIGNEE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("TP_NOTIFY_MST_FK_IN", OracleDbType.Int32, 10, "TP_NOTIFY_MST_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["TP_NOTIFY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("TP_CLOSE_DATE_IN", OracleDbType.Date, 20, "TP_CLOSE_DATE").Direction = ParameterDirection.Input;
                insCommand.Parameters["TP_CLOSE_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("TP_CLOSE_STATUS_IN", OracleDbType.Int32, 1, "TP_CLOSE_STATUS").Direction = ParameterDirection.Input;
                insCommand.Parameters["TP_CLOSE_STATUS_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("TP_STATUS_IN", OracleDbType.Int32, 1, "TP_STATUS").Direction = ParameterDirection.Input;
                insCommand.Parameters["TP_STATUS_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("OPERATOR_FK_IN", OracleDbType.Int32, 10, "OPERATOR_MST_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["OPERATOR_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("VSL_FK_IN", OracleDbType.Int32, 10, "VOYAGEPK").Direction = ParameterDirection.Input;
                insCommand.Parameters["VSL_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("FLIGHT_NO_IN", OracleDbType.Varchar2, 20, "FLIGHT_NO").Direction = ParameterDirection.Input;
                insCommand.Parameters["FLIGHT_NO_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("POL_FK_IN", OracleDbType.Int32, 10, "POL_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["POL_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("POD_FK_IN", OracleDbType.Int32, 10, "POD_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["POD_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("PFD_FK_IN", OracleDbType.Int32, 10, "PFD_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["PFD_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("WORK_ORDER_NR_IN", OracleDbType.Varchar2, 20, "WORK_ORDER_NR").Direction = ParameterDirection.Input;
                insCommand.Parameters["WORK_ORDER_NR_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "CBJC_PK").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with6 = updCommand;
                _with6.Connection = objWK.MyConnection;
                _with6.CommandType = CommandType.StoredProcedure;
                _with6.CommandText = objWK.MyUserName + ".TRANSPORT_INST_SEA_TBL_PKG.TPNOTE_INST_SEA_TBL_UPD";
                var _with7 = _with6.Parameters;
                updCommand.Parameters.Add("TRANSPORT_INST_SEA_PK_IN", OracleDbType.Int32, 10, "TRANPORT_NOTE_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["TRANSPORT_INST_SEA_PK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("TRANS_INST_REF_NO_IN", TpNoteRefNNumber).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("TRANS_INST_DATE_IN", OracleDbType.Date, 20, "TRANS_INST_DATE").Direction = ParameterDirection.Input;
                updCommand.Parameters["TRANS_INST_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("BUSINESS_TYPE_IN", OracleDbType.Int32, 1, "BUSINESS_TYPE").Direction = ParameterDirection.Input;
                updCommand.Parameters["BUSINESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("PROCESS_TYPE_IN", OracleDbType.Int32, 1, "PROCESS_TYPE").Direction = ParameterDirection.Input;
                updCommand.Parameters["PROCESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CUST_CAT_IN", OracleDbType.Int32, 1, "CUSTOMER_CATEGORY").Direction = ParameterDirection.Input;
                updCommand.Parameters["CUST_CAT_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("JC_TYPE_N", OracleDbType.Int32, 1, "JC_TYPE").Direction = ParameterDirection.Input;
                updCommand.Parameters["JC_TYPE_N"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("TP_CBJC_JC_IN", OracleDbType.Int32, 1, "TP_CBJC_JC").Direction = ParameterDirection.Input;
                updCommand.Parameters["TP_CBJC_JC_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("JOB_CARD_FK_IN", OracleDbType.Varchar2, 200, "JOB_CARD_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["JOB_CARD_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CARGO_TYPE_IN", OracleDbType.Int32, 1, "CARGO_TYPE").Direction = ParameterDirection.Input;
                updCommand.Parameters["CARGO_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("COMMODITY_GROUP_MST_FK_IN", OracleDbType.Int32, 10, "COMMODITY_GROUP_MST_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["COMMODITY_GROUP_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("TP_CUSTOMER_MST_FK_IN", OracleDbType.Int32, 10, "TP_CUSTOMER_MST_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["TP_CUSTOMER_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("TP_SHIPPER_MST_FK_IN", OracleDbType.Int32, 10, "TP_SHIPPER_MST_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["TP_SHIPPER_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("TP_CONSIGNEE_MST_FK_IN", OracleDbType.Int32, 10, "TP_CONSIGNEE_MST_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["TP_CONSIGNEE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("TP_NOTIFY_MST_FK_IN", OracleDbType.Int32, 10, "TP_NOTIFY_MST_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["TP_NOTIFY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("TP_CLOSE_DATE_IN", OracleDbType.Date, 20, "TP_CLOSE_DATE").Direction = ParameterDirection.Input;
                updCommand.Parameters["TP_CLOSE_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("TP_CLOSE_STATUS_IN", OracleDbType.Int32, 1, "TP_CLOSE_STATUS").Direction = ParameterDirection.Input;
                updCommand.Parameters["TP_CLOSE_STATUS_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("TP_STATUS_IN", OracleDbType.Int32, 1, "TP_STATUS").Direction = ParameterDirection.Input;
                updCommand.Parameters["TP_STATUS_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("OPERATOR_FK_IN", OracleDbType.Int32, 10, "OPERATOR_MST_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["OPERATOR_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("VSL_FK_IN", OracleDbType.Int32, 10, "VOYAGEPK").Direction = ParameterDirection.Input;
                updCommand.Parameters["VSL_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("FLIGHT_NO_IN", OracleDbType.Varchar2, 20, "FLIGHT_NO").Direction = ParameterDirection.Input;
                updCommand.Parameters["FLIGHT_NO_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("POL_FK_IN", OracleDbType.Int32, 10, "POL_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["POL_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("POD_FK_IN", OracleDbType.Int32, 10, "POD_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["POD_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("PFD_FK_IN", OracleDbType.Int32, 10, "PFD_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["PFD_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("WORK_ORDER_NR_IN", OracleDbType.Varchar2, 20, "WORK_ORDER_NR").Direction = ParameterDirection.Input;
                updCommand.Parameters["WORK_ORDER_NR_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                var _with8 = objWK.MyDataAdapter;
                _with8.InsertCommand = insCommand;
                _with8.InsertCommand.Transaction = TRAN;

                _with8.UpdateCommand = updCommand;
                _with8.UpdateCommand.Transaction = TRAN;

                RecAfct = _with8.Update(M_DataSet);

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    if (isEdting == false)
                    {
                        TPNotePK = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
                    }
                }
                ///'
                DataRow dr = null;
                DataRow dr1 = null;
                DataRow dr2 = null;
                DataRow dr3 = null;
                int Tp_ContFk = 0;
                int Tp_TruckFk = 0;
                var _with9 = objWK.MyCommand;
                _with9.Transaction = TRAN;
                _with9.Connection = objWK.MyConnection;
                _with9.CommandType = CommandType.StoredProcedure;
                if (dsContainerData.Tables[0].Rows.Count > 0)
                {
                    foreach (DataRow dr_loopVariable in dsContainerData.Tables[0].Rows)
                    {
                        dr = dr_loopVariable;
                        if (string.IsNullOrEmpty(dr["TRANSPORT_TRN_CONT_PK"].ToString()))
                        {
                            _with9.CommandText = objWK.MyUserName + ".TRANSPORT_TRN_CONT_PKG.TRANSPORT_TRN_CONT_INS";
                            var _with10 = _with9.Parameters;
                            _with10.Clear();
                            _with10.Add("TRANSPORT_INST_FK_IN", TPNotePK).Direction = ParameterDirection.Input;
                            if (Biztype == 2)
                            {
                                _with10.Add("CONTAINER_TYPE_MST_FK_IN", dr["CONTAINER_TYPE_MST_FK"]).Direction = ParameterDirection.Input;
                                _with10.Add("CONTAINER_NO_IN", dr["CONTAINER_NO"]).Direction = ParameterDirection.Input;
                                _with10.Add("AIRFREIGHT_SLABS_TBL_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                                _with10.Add("PALETTE_SIZE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                                _with10.Add("ULD_NUMBER_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with10.Add("CONTAINER_TYPE_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                                _with10.Add("CONTAINER_NO_IN", DBNull.Value).Direction = ParameterDirection.Input;
                                _with10.Add("ULD_NUMBER_IN", dr["CONTAINER_NO"]).Direction = ParameterDirection.Input;
                                _with10.Add("PALETTE_SIZE_IN", dr["CONTAINER_TYPE_MST_FK"]).Direction = ParameterDirection.Input;
                                _with10.Add("AIRFREIGHT_SLABS_TBL_FK_IN", dr["AIRFREIGHT_SLABS_TBL_FK"]).Direction = ParameterDirection.Input;
                            }
                            if (dsContainerData.Tables[0].Columns.Contains("CONTAINER_OWNER_TYPE_FK"))
                            {
                                if (!string.IsNullOrEmpty(dr["CONTAINER_OWNER_TYPE_FK"].ToString()))
                                {
                                    if (dr["CONTAINER_OWNER_TYPE_FK"] == "SOC" | dr["CONTAINER_OWNER_TYPE_FK"] == "2")
                                    {
                                        dr["CONTAINER_OWNER_TYPE_FK"] = 2;
                                    }
                                    else if (dr["CONTAINER_OWNER_TYPE_FK"] == "COC" | dr["CONTAINER_OWNER_TYPE_FK"] == "1")
                                    {
                                        dr["CONTAINER_OWNER_TYPE_FK"] = 1;
                                    }
                                }
                                _with10.Add("CONTAINER_OWNER_TYPE_FK_IN", dr["CONTAINER_OWNER_TYPE_FK"]).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with10.Add("CONTAINER_OWNER_TYPE_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }
                            _with10.Add("BL_NUMBER_IN", dr["BL_NUMBER"]).Direction = ParameterDirection.Input;
                            _with10.Add("SEAL_NUMBER_IN", dr["SEAL_NUMBER"]).Direction = ParameterDirection.Input;
                            _with10.Add("VOLUME_IN_CBM_IN", dr["VOLUME_IN_CBM"]).Direction = ParameterDirection.Input;
                            _with10.Add("GROSS_WEIGHT_IN", dr["GROSS_WEIGHT"]).Direction = ParameterDirection.Input;
                            _with10.Add("NET_CHARGEABLE_WT_IN", dr["NET_CHARGEABLE_WT"]).Direction = ParameterDirection.Input;
                            _with10.Add("PACK_TYPE_MST_FK_IN", dr["PACK_TYPE_MST_FK"]).Direction = ParameterDirection.Input;
                            _with10.Add("PACK_COUNT_IN", dr["PACK_COUNT"]).Direction = ParameterDirection.Input;
                            _with10.Add("COMMODITY_MST_FKS_IN", dr["COMMODITY_MST_FKS"]).Direction = ParameterDirection.Input;
                            _with10.Add("BIZ_TYPE_IN", dr["BIZ_TYPE"]).Direction = ParameterDirection.Input;
                            _with10.Add("PROCESS_TYPE_IN", dr["PROCESS_TYPE"]).Direction = ParameterDirection.Input;
                            _with10.Add("COMMODITY_MST_FK_IN", dr["COMMODITY_MST_FK"]).Direction = ParameterDirection.Input;
                            _with10.Add("BASIS_FK_IN", dr["BASIS_FK"]).Direction = ParameterDirection.Input;
                            _with10.Add("HIDDEN_CONT_PK_IN", dr["HIDDEN_CONT_PK"]).Direction = ParameterDirection.Input;
                            _with10.Add("LANDING_DATE_IN", dr["LANDING_DATE"]).Direction = ParameterDirection.Input;
                            _with10.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                            _with10.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;
                            _with9.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "CBJC_TRN_CONT_PK").Direction = ParameterDirection.Output;
                            _with9.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                            RecAfct = _with9.ExecuteNonQuery();
                            if (RecAfct > 0)
                            {
                                Tp_ContFk = Convert.ToInt32(_with9.Parameters["RETURN_VALUE"].Value);
                            }
                        }
                        else
                        {
                            _with9.CommandText = objWK.MyUserName + ".TRANSPORT_TRN_CONT_PKG.TRANSPORT_TRN_CONT_UPD";
                            var _with11 = _with9.Parameters;
                            _with11.Clear();
                            _with11.Add("TRANSPORT_TRN_CONT_PK_IN", dr["TRANSPORT_TRN_CONT_PK"]).Direction = ParameterDirection.Input;
                            _with11.Add("TRANSPORT_INST_FK_IN", TPNotePK).Direction = ParameterDirection.Input;
                            if (Biztype == 2)
                            {
                                _with11.Add("CONTAINER_TYPE_MST_FK_IN", dr["CONTAINER_TYPE_MST_FK"]).Direction = ParameterDirection.Input;
                                _with11.Add("CONTAINER_NO_IN", dr["CONTAINER_NO"]).Direction = ParameterDirection.Input;
                                _with11.Add("AIRFREIGHT_SLABS_TBL_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                                _with11.Add("PALETTE_SIZE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                                _with11.Add("ULD_NUMBER_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with11.Add("CONTAINER_TYPE_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                                _with11.Add("CONTAINER_NO_IN", DBNull.Value).Direction = ParameterDirection.Input;
                                _with11.Add("ULD_NUMBER_IN", dr["CONTAINER_NO"]).Direction = ParameterDirection.Input;
                                if (string.IsNullOrEmpty(dr["CONTAINER_TYPE_MST_FK"].ToString()))
                                {
                                    _with11.Add("PALETTE_SIZE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                                }
                                else
                                {
                                    _with11.Add("PALETTE_SIZE_IN", Convert.ToString(dr["CONTAINER_TYPE_MST_FK"])).Direction = ParameterDirection.Input;
                                }
                                _with11.Add("AIRFREIGHT_SLABS_TBL_FK_IN", dr["AIRFREIGHT_SLABS_TBL_FK"]).Direction = ParameterDirection.Input;
                            }
                            if (dsContainerData.Tables[0].Columns.Contains("CONTAINER_OWNER_TYPE_FK"))
                            {
                                if (!string.IsNullOrEmpty(dr["CONTAINER_OWNER_TYPE_FK"].ToString()))
                                {
                                    if (dr["CONTAINER_OWNER_TYPE_FK"] == "SOC" | dr["CONTAINER_OWNER_TYPE_FK"] == "2")
                                    {
                                        dr["CONTAINER_OWNER_TYPE_FK"] = 2;
                                    }
                                    else if (dr["CONTAINER_OWNER_TYPE_FK"] == "COC" | dr["CONTAINER_OWNER_TYPE_FK"] == "1")
                                    {
                                        dr["CONTAINER_OWNER_TYPE_FK"] = 1;
                                    }
                                }
                                _with11.Add("CONTAINER_OWNER_TYPE_FK_IN", dr["CONTAINER_OWNER_TYPE_FK"]).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with11.Add("CONTAINER_OWNER_TYPE_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }
                            _with11.Add("BL_NUMBER_IN", dr["BL_NUMBER"]).Direction = ParameterDirection.Input;
                            _with11.Add("SEAL_NUMBER_IN", dr["SEAL_NUMBER"]).Direction = ParameterDirection.Input;
                            _with11.Add("VOLUME_IN_CBM_IN", dr["VOLUME_IN_CBM"]).Direction = ParameterDirection.Input;
                            _with11.Add("GROSS_WEIGHT_IN", dr["GROSS_WEIGHT"]).Direction = ParameterDirection.Input;
                            _with11.Add("NET_CHARGEABLE_WT_IN", dr["NET_CHARGEABLE_WT"]).Direction = ParameterDirection.Input;
                            _with11.Add("PACK_TYPE_MST_FK_IN", dr["PACK_TYPE_MST_FK"]).Direction = ParameterDirection.Input;
                            _with11.Add("PACK_COUNT_IN", dr["PACK_COUNT"]).Direction = ParameterDirection.Input;
                            _with11.Add("COMMODITY_MST_FKS_IN", dr["COMMODITY_MST_FKS"]).Direction = ParameterDirection.Input;
                            _with11.Add("BIZ_TYPE_IN", dr["BIZ_TYPE"]).Direction = ParameterDirection.Input;
                            _with11.Add("PROCESS_TYPE_IN", dr["PROCESS_TYPE"]).Direction = ParameterDirection.Input;
                            _with11.Add("COMMODITY_MST_FK_IN", dr["COMMODITY_MST_FK"]).Direction = ParameterDirection.Input;
                            _with11.Add("BASIS_FK_IN", dr["BASIS_FK"]).Direction = ParameterDirection.Input;
                            _with11.Add("HIDDEN_CONT_PK_IN", dr["HIDDEN_CONT_PK"]).Direction = ParameterDirection.Input;
                            _with11.Add("LANDING_DATE_IN", dr["LANDING_DATE"]).Direction = ParameterDirection.Input;
                            _with11.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                            _with11.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;
                            _with11.Add("VERSION_NO_IN", dr["VERSION_NO"]).Direction = ParameterDirection.Input;
                            _with9.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                            _with9.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                            RecAfct = _with9.ExecuteNonQuery();
                            if (RecAfct > 0)
                            {
                                if (!string.IsNullOrEmpty(_with9.Parameters["RETURN_VALUE"].Value.ToString()))
                                {
                                    Tp_ContFk = Convert.ToInt32(_with9.Parameters["RETURN_VALUE"].Value);
                                }
                                else
                                {
                                    Tp_ContFk = 0;
                                }
                            }
                        }
                        //'Truck Details
                        var _with12 = objWK.MyCommand;
                        _with12.Transaction = TRAN;
                        _with12.Connection = objWK.MyConnection;
                        _with12.CommandType = CommandType.StoredProcedure;
                        if (dsTruckMainData.Tables[0].Rows.Count > 0)
                        {
                            foreach (DataRow dr1_loopVariable in dsTruckMainData.Tables[0].Rows)
                            {
                                dr1 = dr1_loopVariable;
                                //'Sea
                                if (Biztype == 2 & Convert.ToInt32(CargoType) != 4)
                                {
                                    if (dr["CONTAINER_TYPE_MST_FK"] == dr1["CONTAINER_TYPE"] & dr["CONTAINER_NO"] == dr1["CONTAINER_NR"] & dr["BL_NUMBER"] == dr1["BL_NUMBER"])
                                    {
                                        if (string.IsNullOrEmpty(dr1["TRANSPORT_TRN_TRUCK_PK"].ToString()))
                                        {
                                            _with12.CommandText = objWK.MyUserName + ".TRANSPORT_TRN_TRUCK_PKG.TRANSPORT_TRN_TRUCK_INS";
                                            var _with13 = _with12.Parameters;
                                            _with13.Clear();
                                            _with13.Add("TRANSPORT_TRN_CONT_FK_IN", Tp_ContFk).Direction = ParameterDirection.Input;
                                            _with13.Add("BIZ_TYPE_IN", Convert.ToInt32(dr1["BIZ_TYPE"])).Direction = ParameterDirection.Input;
                                            _with13.Add("PROCESS_TYPE_IN", Convert.ToInt32(dr1["PROCESS_TYPE"])).Direction = ParameterDirection.Input;
                                            _with13.Add("CONTAINER_NR_IN", dr1["CONTAINER_NR"]).Direction = ParameterDirection.Input;
                                            _with13.Add("CONTAINER_TYPE_IN", dr1["CONTAINER_TYPE"]).Direction = ParameterDirection.Input;
                                            _with13.Add("SEAL_NUMBER_IN", dr1["SEAL_NUMBER"]).Direction = ParameterDirection.Input;
                                            _with13.Add("TRANSPORTER_MST_FK_IN", Convert.ToInt32(dr1["TRANSPORTER_MST_FK"])).Direction = ParameterDirection.Input;
                                            _with13.Add("COMMODITY_DESC_IN", dr1["COMMODITY_DESC"]).Direction = ParameterDirection.Input;
                                            _with13.Add("BL_NUMBER_IN", dr1["BL_NUMBER"]).Direction = ParameterDirection.Input;
                                            _with13.Add("QTY_IN", dr1["QTY"]).Direction = ParameterDirection.Input;
                                            _with13.Add("NET_WT_IN", dr1["NET_WT"]).Direction = ParameterDirection.Input;
                                            _with13.Add("VOLUME_IN", dr1["VOLUME"]).Direction = ParameterDirection.Input;
                                            _with13.Add("TD_TRUCK_NUMBER_IN", dr1["TD_TRUCK_NUMBER"]).Direction = ParameterDirection.Input;
                                            _with13.Add("TD_CHASIS_NUMBER_IN", dr1["TD_CHASIS_NUMBER"]).Direction = ParameterDirection.Input;
                                            _with13.Add("TD_ODMRS_NUMBER_IN", dr1["TD_ODMRS_NUMBER"]).Direction = ParameterDirection.Input;
                                            _with13.Add("TD_ODMRE_NUMBER_IN", dr1["TD_ODMRE_NUMBER"]).Direction = ParameterDirection.Input;
                                            _with13.Add("TD_TRUCK_REMARKS_IN", dr1["TD_TRUCK_REMARKS"]).Direction = ParameterDirection.Input;
                                            _with13.Add("DD_DRIVER1_NAME_IN", dr1["DD_DRIVER1_NAME"]).Direction = ParameterDirection.Input;
                                            _with13.Add("DD_DRIVER2_NAME_IN", dr1["DD_DRIVER2_NAME"]).Direction = ParameterDirection.Input;
                                            _with13.Add("DD_LICENSE_NUMBER_IN", dr1["DD_LICENSE_NUMBER"]).Direction = ParameterDirection.Input;
                                            _with13.Add("DD_LICENSE_EXP_ON_IN", dr1["DD_LICENSE_EXP_ON"]).Direction = ParameterDirection.Input;
                                            _with13.Add("DD_PASSPORT_NUMBER_IN", dr1["DD_PASSPORT_NUMBER"]).Direction = ParameterDirection.Input;
                                            _with13.Add("DD_CONTACT_NUMBER1_IN", dr1["DD_CONTACT_NUMBER1"]).Direction = ParameterDirection.Input;
                                            _with13.Add("DD_CONTACT_NUMBER2_IN", dr1["DD_CONTACT_NUMBER2"]).Direction = ParameterDirection.Input;
                                            _with13.Add("DD_CONTACT_NUMBER3_IN", dr1["DD_CONTACT_NUMBER3"]).Direction = ParameterDirection.Input;
                                            _with13.Add("DD_OFFICE_NUMBER_IN", dr1["DD_OFFICE_NUMBER"]).Direction = ParameterDirection.Input;
                                            _with13.Add("DD_C65_FORM_NUMBER_IN", dr1["DD_C65_FORM_NUMBER"]).Direction = ParameterDirection.Input;
                                            _with13.Add("DD_PASSPORT_EXP_ON_IN", dr1["DD_PASSPORT_EXP_ON"]).Direction = ParameterDirection.Input;
                                            _with13.Add("DD_WORK_PERMIT_NO_IN", dr1["DD_WORK_PERMIT_NO"]).Direction = ParameterDirection.Input;
                                            _with13.Add("DD_WORK_PERMIT_EXP_ON_IN", dr1["DD_WORK_PERMIT_EXP_ON"]).Direction = ParameterDirection.Input;
                                            _with13.Add("CWD_CARGO_WT_IN", dr1["CWD_CARGO_WT"]).Direction = ParameterDirection.Input;

                                            _with13.Add("CWD_WT_LIMIT_ROAD_IN", dr1["CWD_WT_LIMIT_ROAD"]).Direction = ParameterDirection.Input;
                                            _with13.Add("CWD_EXCESS_WT_IN", dr1["CWD_EXCESS_WT"]).Direction = ParameterDirection.Input;
                                            _with13.Add("CWD_PENALTY_AMOUNT_IN", dr1["CWD_PENALTY_AMOUNT"]).Direction = ParameterDirection.Input;
                                            _with13.Add("CWD_PENALTY_APPLICABLE_IN", dr1["CWD_PENALTY_APPLICABLE"]).Direction = ParameterDirection.Input;
                                            _with13.Add("CTR_INSP_STATUS_IN", dr1["CTR_INSP_STATUS"]).Direction = ParameterDirection.Input;
                                            _with13.Add("CTR_INSP_DAMAGE_DESC_FK_IN", dr1["CTR_INSP_DAMAGE_DESC_FK"]).Direction = ParameterDirection.Input;
                                            _with13.Add("CTR_INSP_REF_NR_IN", dr1["CTR_INSP_REF_NR"]).Direction = ParameterDirection.Input;
                                            _with13.Add("CTR_INSP_DATE_IN", dr1["CTR_INSP_DATE"]).Direction = ParameterDirection.Input;
                                            _with13.Add("CTR_INSP_BY_IN", dr1["CTR_INSP_BY"]).Direction = ParameterDirection.Input;
                                            _with13.Add("CTR_INSP_REMARKS_IN", dr1["CTR_INSP_REMARKS"]).Direction = ParameterDirection.Input;
                                            _with13.Add("CTR_MOV_T1_REF_NR_IN", dr1["CTR_MOV_T1_REF_NR"]).Direction = ParameterDirection.Input;
                                            _with13.Add("CTR_MOV_T1_DT_IN", dr1["CTR_MOV_T1_DT"]).Direction = ParameterDirection.Input;
                                            _with13.Add("CTR_MOV_LOAD_DT_IN", dr1["CTR_MOV_LOAD_DT"]).Direction = ParameterDirection.Input;
                                            _with13.Add("CTR_MOV_UNLOAD_DT_IN", dr1["CTR_MOV_UNLOAD_DT"]).Direction = ParameterDirection.Input;
                                            _with13.Add("CTR_NR_OF_BORDERS_IN", dr1["CTR_NR_OF_BORDERS"]).Direction = ParameterDirection.Input;
                                            _with13.Add("PD_MTY_CTR_PICKUP_ADD_IN", dr1["PD_MTY_CTR_PICKUP_ADD"]).Direction = ParameterDirection.Input;
                                            _with13.Add("PD_MTY_CTR_DROP_ADD_IN", dr1["PD_MTY_CTR_DROP_ADD"]).Direction = ParameterDirection.Input;
                                            _with13.Add("PD_MTY_CTR_PICK_DT_IN", dr1["PD_MTY_CTR_PICK_DT"]).Direction = ParameterDirection.Input;
                                            _with13.Add("PD_MTY_CTR_DROP_DT_IN", dr1["PD_MTY_CTR_DROP_DT"]).Direction = ParameterDirection.Input;
                                            _with13.Add("PD_CARGO_PICKUP_ADD_IN", dr1["PD_CARGO_PICKUP_ADD"]).Direction = ParameterDirection.Input;
                                            _with13.Add("PD_CARGO_DROP_ADD_IN", dr1["PD_CARGO_DROP_ADD"]).Direction = ParameterDirection.Input;
                                            _with13.Add("PD_CARGO_PICKUP_DT_IN", dr1["PD_CARGO_PICKUP_DT"]).Direction = ParameterDirection.Input;
                                            _with13.Add("PD_CARGO_DROP_DT_IN", dr1["PD_CARGO_DROP_DT"]).Direction = ParameterDirection.Input;
                                            _with13.Add("PD_CTR_RETURN_ADD_IN", dr1["PD_CTR_RETURN_ADD"]).Direction = ParameterDirection.Input;
                                            _with13.Add("PD_CTR_RETURN_DT_IN", dr1["PD_CTR_RETURN_DT"]).Direction = ParameterDirection.Input;
                                            _with13.Add("PD_CTR_RETURN_DT_BYCUST_IN", dr1["PD_CTR_RETURN_DT_BYCUST"]).Direction = ParameterDirection.Input;
                                            _with13.Add("EST_DT_DEL_IN", dr1["EST_DT_DEL"]).Direction = ParameterDirection.Input;
                                            _with13.Add("EST_DT_MT_DEL_IN", dr1["EST_DT_MT_DEL"]).Direction = ParameterDirection.Input;
                                            _with13.Add("PD_SPECIAL_INST_IN", dr1["PD_SPECIAL_INST"]).Direction = ParameterDirection.Input;
                                            _with13.Add("PD_REMARKS_IN", dr1["PD_REMARKS"]).Direction = ParameterDirection.Input;
                                            _with13.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                                            _with13.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;
                                            _with13.Add("RETURN_VALUE", OracleDbType.Int32, 10, "TRANSPORT_TRN_TRUCK_PK").Direction = ParameterDirection.Output;
                                            RecAfct = _with12.ExecuteNonQuery();
                                            if (RecAfct > 0)
                                            {
                                                Tp_TruckFk = Convert.ToInt32(_with12.Parameters["RETURN_VALUE"].Value);
                                            }
                                        }
                                        else
                                        {
                                            _with12.CommandText = objWK.MyUserName + ".TRANSPORT_TRN_TRUCK_PKG.TRANSPORT_TRN_TRUCK_UPD";
                                            var _with14 = _with12.Parameters;
                                            _with14.Clear();
                                            _with14.Add("TRANSPORT_TRN_CONT_FK_IN", Tp_ContFk).Direction = ParameterDirection.Input;
                                            _with14.Add("TRANSPORT_TRN_TRUCK_PK_IN", dr1["TRANSPORT_TRN_TRUCK_PK"]).Direction = ParameterDirection.Input;
                                            _with14.Add("BIZ_TYPE_IN", dr1["BIZ_TYPE"]).Direction = ParameterDirection.Input;
                                            _with14.Add("PROCESS_TYPE_IN", dr1["PROCESS_TYPE"]).Direction = ParameterDirection.Input;
                                            _with14.Add("CONTAINER_NR_IN", dr1["CONTAINER_NR"]).Direction = ParameterDirection.Input;
                                            _with14.Add("CONTAINER_TYPE_IN", dr1["CONTAINER_TYPE"]).Direction = ParameterDirection.Input;
                                            _with14.Add("SEAL_NUMBER_IN", dr1["SEAL_NUMBER"]).Direction = ParameterDirection.Input;
                                            _with14.Add("TRANSPORTER_MST_FK_IN", dr1["TRANSPORTER_MST_FK"]).Direction = ParameterDirection.Input;
                                            _with14.Add("COMMODITY_DESC_IN", dr1["COMMODITY_DESC"]).Direction = ParameterDirection.Input;
                                            _with14.Add("BL_NUMBER_IN", dr1["BL_NUMBER"]).Direction = ParameterDirection.Input;
                                            _with14.Add("QTY_IN", dr1["QTY"]).Direction = ParameterDirection.Input;
                                            _with14.Add("NET_WT_IN", dr1["NET_WT"]).Direction = ParameterDirection.Input;
                                            _with14.Add("VOLUME_IN", dr1["VOLUME"]).Direction = ParameterDirection.Input;
                                            _with14.Add("TD_TRUCK_NUMBER_IN", dr1["TD_TRUCK_NUMBER"]).Direction = ParameterDirection.Input;
                                            _with14.Add("TD_CHASIS_NUMBER_IN", dr1["TD_CHASIS_NUMBER"]).Direction = ParameterDirection.Input;
                                            _with14.Add("TD_ODMRS_NUMBER_IN", dr1["TD_ODMRS_NUMBER"]).Direction = ParameterDirection.Input;
                                            _with14.Add("TD_ODMRE_NUMBER_IN", dr1["TD_ODMRE_NUMBER"]).Direction = ParameterDirection.Input;
                                            _with14.Add("TD_TRUCK_REMARKS_IN", dr1["TD_TRUCK_REMARKS"]).Direction = ParameterDirection.Input;
                                            _with14.Add("DD_DRIVER1_NAME_IN", dr1["DD_DRIVER1_NAME"]).Direction = ParameterDirection.Input;
                                            _with14.Add("DD_DRIVER2_NAME_IN", dr1["DD_DRIVER2_NAME"]).Direction = ParameterDirection.Input;
                                            _with14.Add("DD_LICENSE_NUMBER_IN", dr1["DD_LICENSE_NUMBER"]).Direction = ParameterDirection.Input;
                                            _with14.Add("DD_LICENSE_EXP_ON_IN", dr1["DD_LICENSE_EXP_ON"]).Direction = ParameterDirection.Input;
                                            _with14.Add("DD_PASSPORT_NUMBER_IN", dr1["DD_PASSPORT_NUMBER"]).Direction = ParameterDirection.Input;
                                            _with14.Add("DD_CONTACT_NUMBER1_IN", dr1["DD_CONTACT_NUMBER1"]).Direction = ParameterDirection.Input;
                                            _with14.Add("DD_CONTACT_NUMBER2_IN", dr1["DD_CONTACT_NUMBER2"]).Direction = ParameterDirection.Input;
                                            _with14.Add("DD_CONTACT_NUMBER3_IN", dr1["DD_CONTACT_NUMBER3"]).Direction = ParameterDirection.Input;
                                            _with14.Add("DD_OFFICE_NUMBER_IN", dr1["DD_OFFICE_NUMBER"]).Direction = ParameterDirection.Input;
                                            _with14.Add("DD_C65_FORM_NUMBER_IN", dr1["DD_C65_FORM_NUMBER"]).Direction = ParameterDirection.Input;
                                            _with14.Add("DD_PASSPORT_EXP_ON_IN", dr1["DD_PASSPORT_EXP_ON"]).Direction = ParameterDirection.Input;
                                            _with14.Add("DD_WORK_PERMIT_NO_IN", dr1["DD_WORK_PERMIT_NO"]).Direction = ParameterDirection.Input;
                                            _with14.Add("DD_WORK_PERMIT_EXP_ON_IN", dr1["DD_WORK_PERMIT_EXP_ON"]).Direction = ParameterDirection.Input;
                                            _with14.Add("CWD_CARGO_WT_IN", dr1["CWD_CARGO_WT"]).Direction = ParameterDirection.Input;
                                            _with14.Add("CWD_WT_LIMIT_ROAD_IN", dr1["CWD_WT_LIMIT_ROAD"]).Direction = ParameterDirection.Input;
                                            _with14.Add("CWD_EXCESS_WT_IN", dr1["CWD_EXCESS_WT"]).Direction = ParameterDirection.Input;
                                            _with14.Add("CWD_PENALTY_AMOUNT_IN", dr1["CWD_PENALTY_AMOUNT"]).Direction = ParameterDirection.Input;
                                            _with14.Add("CWD_PENALTY_APPLICABLE_IN", dr1["CWD_PENALTY_APPLICABLE"]).Direction = ParameterDirection.Input;
                                            _with14.Add("CTR_INSP_STATUS_IN", dr1["CTR_INSP_STATUS"]).Direction = ParameterDirection.Input;
                                            _with14.Add("CTR_INSP_DAMAGE_DESC_FK_IN", dr1["CTR_INSP_DAMAGE_DESC_FK"]).Direction = ParameterDirection.Input;
                                            _with14.Add("CTR_INSP_REF_NR_IN", dr1["CTR_INSP_REF_NR"]).Direction = ParameterDirection.Input;
                                            _with14.Add("CTR_INSP_DATE_IN", dr1["CTR_INSP_DATE"]).Direction = ParameterDirection.Input;
                                            _with14.Add("CTR_INSP_BY_IN", dr1["CTR_INSP_BY"]).Direction = ParameterDirection.Input;
                                            _with14.Add("CTR_INSP_REMARKS_IN", dr1["CTR_INSP_REMARKS"]).Direction = ParameterDirection.Input;
                                            _with14.Add("CTR_MOV_T1_REF_NR_IN", dr1["CTR_MOV_T1_REF_NR"]).Direction = ParameterDirection.Input;
                                            _with14.Add("CTR_MOV_T1_DT_IN", dr1["CTR_MOV_T1_DT"]).Direction = ParameterDirection.Input;
                                            _with14.Add("CTR_MOV_LOAD_DT_IN", dr1["CTR_MOV_LOAD_DT"]).Direction = ParameterDirection.Input;
                                            _with14.Add("CTR_MOV_UNLOAD_DT_IN", dr1["CTR_MOV_UNLOAD_DT"]).Direction = ParameterDirection.Input;
                                            _with14.Add("CTR_NR_OF_BORDERS_IN", dr1["CTR_NR_OF_BORDERS"]).Direction = ParameterDirection.Input;
                                            _with14.Add("PD_MTY_CTR_PICKUP_ADD_IN", dr1["PD_MTY_CTR_PICKUP_ADD"]).Direction = ParameterDirection.Input;
                                            _with14.Add("PD_MTY_CTR_DROP_ADD_IN", dr1["PD_MTY_CTR_DROP_ADD"]).Direction = ParameterDirection.Input;
                                            _with14.Add("PD_MTY_CTR_PICK_DT_IN", dr1["PD_MTY_CTR_PICK_DT"]).Direction = ParameterDirection.Input;
                                            _with14.Add("PD_MTY_CTR_DROP_DT_IN", dr1["PD_MTY_CTR_DROP_DT"]).Direction = ParameterDirection.Input;
                                            _with14.Add("PD_CARGO_PICKUP_ADD_IN", dr1["PD_CARGO_PICKUP_ADD"]).Direction = ParameterDirection.Input;
                                            _with14.Add("PD_CARGO_DROP_ADD_IN", dr1["PD_CARGO_DROP_ADD"]).Direction = ParameterDirection.Input;
                                            _with14.Add("PD_CARGO_PICKUP_DT_IN", dr1["PD_CARGO_PICKUP_DT"]).Direction = ParameterDirection.Input;
                                            _with14.Add("PD_CARGO_DROP_DT_IN", dr1["PD_CARGO_DROP_DT"]).Direction = ParameterDirection.Input;
                                            _with14.Add("PD_CTR_RETURN_ADD_IN", dr1["PD_CTR_RETURN_ADD"]).Direction = ParameterDirection.Input;
                                            _with14.Add("PD_CTR_RETURN_DT_IN", dr1["PD_CTR_RETURN_DT"]).Direction = ParameterDirection.Input;
                                            _with14.Add("PD_CTR_RETURN_DT_BYCUST_IN", dr1["PD_CTR_RETURN_DT_BYCUST"]).Direction = ParameterDirection.Input;
                                            _with14.Add("EST_DT_DEL_IN", dr1["EST_DT_DEL"]).Direction = ParameterDirection.Input;
                                            _with14.Add("EST_DT_MT_DEL_IN", dr1["EST_DT_MT_DEL"]).Direction = ParameterDirection.Input;
                                            _with14.Add("PD_SPECIAL_INST_IN", dr1["PD_SPECIAL_INST"]).Direction = ParameterDirection.Input;
                                            _with14.Add("PD_REMARKS_IN", dr1["PD_REMARKS"]).Direction = ParameterDirection.Input;
                                            _with14.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                                            _with14.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;
                                            _with14.Add("VERSION_NO_IN", dr1["VERSION_NO"]).Direction = ParameterDirection.Input;
                                            _with14.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                                            RecAfct = _with12.ExecuteNonQuery();
                                            if (RecAfct > 0)
                                            {
                                                if (!string.IsNullOrEmpty(_with12.Parameters["RETURN_VALUE"].Value.ToString()))
                                                {
                                                    Tp_TruckFk = Convert.ToInt32(_with12.Parameters["RETURN_VALUE"].Value);
                                                }
                                                else
                                                {
                                                    Tp_TruckFk = 0;
                                                }
                                            }
                                        }
                                        //'Doc Details
                                        if ((dsDocDetails != null))
                                        {
                                            var _with15 = objWK.MyCommand;
                                            _with15.Transaction = TRAN;
                                            _with15.Connection = objWK.MyConnection;
                                            _with15.CommandType = CommandType.StoredProcedure;
                                            if (dsDocDetails.Tables[0].Rows.Count > 0)
                                            {
                                                foreach (DataRow dr2_loopVariable in dsDocDetails.Tables[0].Rows)
                                                {
                                                    dr2 = dr2_loopVariable;
                                                    if (dr1["BIZ_TYPE"] == dr2["BIZ_TYPE"] & dr1["PROCESS_TYPE"] == dr2["PROCESS_TYPE"] & dr1["TRANSPORTER_MST_FK"] == dr2["TRANSPORTER_MST_FK"] & dr1["TD_TRUCK_NUMBER"] == dr2["TD_TRUCK_NUMBER"])
                                                    {
                                                        _with15.CommandText = objWK.MyUserName + ".TRANSPORT_TRN_DOC_PKG.TRANSPORT_TRN_DOC_INS";
                                                        if (string.IsNullOrEmpty(dr2["TRANSPORT_TRN_DOC_PK"].ToString()))
                                                        {
                                                            var _with16 = _with15.Parameters;
                                                            _with16.Clear();
                                                            _with16.Add("TRANSPORT_TRN_TRUCK_FK_IN", Tp_TruckFk).Direction = ParameterDirection.Input;
                                                            _with16.Add("DOCUMENT_REF_NR_IN", dr2["DOCUMENT_REF_NR"]).Direction = ParameterDirection.Input;
                                                            _with16.Add("DOCUMENT_NAME_IN", dr2["DOC_NAME"]).Direction = ParameterDirection.Input;
                                                            _with16.Add("DOCUMENT_TYPE_IN", dr2["DOC_TYPE"]).Direction = ParameterDirection.Input;
                                                            if (!string.IsNullOrEmpty(dr2["DOCUMENT_RECEIVED_ON"].ToString()))
                                                            {
                                                                _with16.Add("DOCUMENT_RECEIVED_ON_IN", getDefault(Convert.ToDateTime(dr2["DOCUMENT_RECEIVED_ON"]), DBNull.Value)).Direction = ParameterDirection.Input;
                                                            }
                                                            else
                                                            {
                                                                _with16.Add("DOCUMENT_RECEIVED_ON_IN", DBNull.Value).Direction = ParameterDirection.Input;
                                                            }
                                                            _with16.Add("DOCUMENT_REMARKS_IN", dr2["DOCUMENT_REMARKS"]).Direction = ParameterDirection.Input;
                                                            _with16.Add("BIZ_TYPE_IN", dr2["BIZ_TYPE"]).Direction = ParameterDirection.Input;
                                                            _with16.Add("PROCESS_TYPE_IN", dr2["PROCESS_TYPE"]).Direction = ParameterDirection.Input;
                                                            _with16.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                                                            _with16.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;
                                                            _with16.Add("RETURN_VALUE", OracleDbType.Int32, 10, "TRANSPORT_TRN_DOC_PK").Direction = ParameterDirection.Output;
                                                            RecAfct = _with15.ExecuteNonQuery();
                                                        }
                                                        else
                                                        {
                                                            if (Convert.ToInt32(dr2["TRANSPORT_TRN_TRUCK_FK"]) == Tp_TruckFk)
                                                            {
                                                                _with15.CommandText = objWK.MyUserName + ".TRANSPORT_TRN_DOC_PKG.TRANSPORT_TRN_DOC_UPD";
                                                                var _with17 = _with15.Parameters;
                                                                _with17.Clear();
                                                                _with17.Add("TRANSPORT_TRN_DOC_PK_IN", dr2["TRANSPORT_TRN_DOC_PK"]).Direction = ParameterDirection.Input;
                                                                _with17.Add("TRANSPORT_TRN_TRUCK_FK_IN", Tp_TruckFk).Direction = ParameterDirection.Input;
                                                                _with17.Add("DOCUMENT_REF_NR_IN", dr2["DOCUMENT_REF_NR"]).Direction = ParameterDirection.Input;
                                                                _with17.Add("DOCUMENT_NAME_IN", dr2["DOC_NAME"]).Direction = ParameterDirection.Input;
                                                                _with17.Add("DOCUMENT_TYPE_IN", dr2["DOC_TYPE"]).Direction = ParameterDirection.Input;
                                                                if (!string.IsNullOrEmpty(dr2["DOCUMENT_RECEIVED_ON"].ToString()))
                                                                {
                                                                    _with17.Add("DOCUMENT_RECEIVED_ON_IN", getDefault(Convert.ToDateTime(dr2["DOCUMENT_RECEIVED_ON"]), DBNull.Value)).Direction = ParameterDirection.Input;
                                                                }
                                                                else
                                                                {
                                                                    _with17.Add("DOCUMENT_RECEIVED_ON_IN", DBNull.Value).Direction = ParameterDirection.Input;
                                                                }
                                                                _with17.Add("DOCUMENT_REMARKS_IN", dr2["DOCUMENT_REMARKS"]).Direction = ParameterDirection.Input;
                                                                _with17.Add("BIZ_TYPE_IN", dr2["BIZ_TYPE"]).Direction = ParameterDirection.Input;
                                                                _with17.Add("PROCESS_TYPE_IN", dr2["PROCESS_TYPE"]).Direction = ParameterDirection.Input;
                                                                _with17.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                                                                _with17.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;
                                                                _with17.Add("VERSION_NO_IN", (string.IsNullOrEmpty(dr2["VERSION_NO"].ToString()) ? 0 : dr2["VERSION_NO"])).Direction = ParameterDirection.Input;
                                                                _with17.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                                                                RecAfct = _with15.ExecuteNonQuery();
                                                            }
                                                        }
                                                    }
                                                }
                                                //'Doc Details
                                            }
                                        }
                                        //'Track Details
                                        if ((dsTrackDetails != null))
                                        {
                                            var _with18 = objWK.MyCommand;
                                            _with18.Transaction = TRAN;
                                            _with18.Connection = objWK.MyConnection;
                                            _with18.CommandType = CommandType.StoredProcedure;
                                            if (dsTrackDetails.Tables[0].Rows.Count > 0)
                                            {
                                                foreach (DataRow dr3_loopVariable in dsTrackDetails.Tables[0].Rows)
                                                {
                                                    dr3 = dr3_loopVariable;
                                                    if (dr1["BIZ_TYPE"] == dr3["BIZ_TYPE"] & dr1["PROCESS_TYPE"] == dr3["PROCESS_TYPE"] & dr1["TRANSPORTER_MST_FK"] == dr3["TRANSPORTER_MST_FK"] & dr1["TD_TRUCK_NUMBER"] == dr3["TD_TRUCK_NUMBER"])
                                                    {
                                                        _with18.CommandText = objWK.MyUserName + ".TRANSPORT_TRN_TRACK_PKG.TRANSPORT_TRN_TRACK_INS";
                                                        if (string.IsNullOrEmpty(dr3["TRANSPORT_TRN_TRACK_PK"].ToString()))
                                                        {
                                                            var _with19 = _with18.Parameters;
                                                            _with19.Clear();
                                                            _with19.Add("TRANSPORT_TRN_TRUCK_FK_IN", Tp_TruckFk).Direction = ParameterDirection.Input;
                                                            _with19.Add("ONWARD_RETURN_IN", dr3["ONWARD_RETURN"]).Direction = ParameterDirection.Input;
                                                            _with19.Add("BORDER_NAME_IN", dr3["BORDER_NAME"]).Direction = ParameterDirection.Input;
                                                            if (!string.IsNullOrEmpty(dr3["ETA"].ToString()))
                                                            {
                                                                _with19.Add("ETA_IN", Convert.ToDateTime(dr3["ETA"])).Direction = ParameterDirection.Input;
                                                            }
                                                            else
                                                            {
                                                                _with19.Add("ETA_IN", DBNull.Value).Direction = ParameterDirection.Input;
                                                            }
                                                            if (!string.IsNullOrEmpty(dr3["ETD"].ToString()))
                                                            {
                                                                _with19.Add("ETD_IN", Convert.ToDateTime(dr3["ETD"])).Direction = ParameterDirection.Input;
                                                            }
                                                            else
                                                            {
                                                                _with19.Add("ETD_IN", DBNull.Value).Direction = ParameterDirection.Input;
                                                            }
                                                            if (!string.IsNullOrEmpty(dr3["ATA"].ToString()))
                                                            {
                                                                _with19.Add("ATA_IN", Convert.ToDateTime(dr3["ATA"])).Direction = ParameterDirection.Input;
                                                            }
                                                            else
                                                            {
                                                                _with19.Add("ATA_IN", DBNull.Value).Direction = ParameterDirection.Input;
                                                            }
                                                            if (!string.IsNullOrEmpty(dr3["ATD"].ToString()))
                                                            {
                                                                _with19.Add("ATD_IN", Convert.ToDateTime(dr3["ATD"])).Direction = ParameterDirection.Input;
                                                            }
                                                            else
                                                            {
                                                                _with19.Add("ATD_IN", DBNull.Value).Direction = ParameterDirection.Input;
                                                            }
                                                            _with19.Add("ROTATION_NUMBER_IN", dr3["ROTATION_NUMBER"]).Direction = ParameterDirection.Input;
                                                            _with19.Add("BIZ_TYPE_IN", dr3["BIZ_TYPE"]).Direction = ParameterDirection.Input;
                                                            _with19.Add("PROCESS_TYPE_IN", dr3["PROCESS_TYPE"]).Direction = ParameterDirection.Input;
                                                            _with19.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                                                            _with19.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;
                                                            _with19.Add("RETURN_VALUE", OracleDbType.Int32, 10, "TRANSPORT_TRN_TRACK_PK").Direction = ParameterDirection.Output;
                                                            RecAfct = _with18.ExecuteNonQuery();
                                                        }
                                                        else
                                                        {
                                                            if (Convert.ToInt32(dr3["TRANSPORT_TRN_TRUCK_FK"]) == Tp_TruckFk)
                                                            {
                                                                _with18.CommandText = objWK.MyUserName + ".TRANSPORT_TRN_TRACK_PKG.TRANSPORT_TRN_TRACK_UPD";
                                                                var _with20 = _with18.Parameters;
                                                                _with20.Clear();
                                                                _with20.Add("TRANSPORT_TRN_TRACK_PK_IN", dr3["TRANSPORT_TRN_TRACK_PK"]).Direction = ParameterDirection.Input;
                                                                _with20.Add("TRANSPORT_TRN_TRUCK_FK_IN", Tp_TruckFk).Direction = ParameterDirection.Input;
                                                                _with20.Add("ONWARD_RETURN_IN", dr3["ONWARD_RETURN"]).Direction = ParameterDirection.Input;
                                                                _with20.Add("BORDER_NAME_IN", dr3["BORDER_NAME"]).Direction = ParameterDirection.Input;
                                                                if (!string.IsNullOrEmpty(dr3["ETA"].ToString()))
                                                                {
                                                                    _with20.Add("ETA_IN", Convert.ToDateTime(dr3["ETA"])).Direction = ParameterDirection.Input;
                                                                }
                                                                else
                                                                {
                                                                    _with20.Add("ETA_IN", DBNull.Value).Direction = ParameterDirection.Input;
                                                                }
                                                                if (!string.IsNullOrEmpty(dr3["ETD"].ToString()))
                                                                {
                                                                    _with20.Add("ETD_IN", Convert.ToDateTime(dr3["ETD"])).Direction = ParameterDirection.Input;
                                                                }
                                                                else
                                                                {
                                                                    _with20.Add("ETD_IN", DBNull.Value).Direction = ParameterDirection.Input;
                                                                }
                                                                if (!string.IsNullOrEmpty(dr3["ATA"].ToString()))
                                                                {
                                                                    _with20.Add("ATA_IN", Convert.ToDateTime(dr3["ATA"])).Direction = ParameterDirection.Input;
                                                                }
                                                                else
                                                                {
                                                                    _with20.Add("ATA_IN", DBNull.Value).Direction = ParameterDirection.Input;
                                                                }
                                                                if (!string.IsNullOrEmpty(dr3["ATD"].ToString()))
                                                                {
                                                                    _with20.Add("ATD_IN", Convert.ToDateTime(dr3["ATD"])).Direction = ParameterDirection.Input;
                                                                }
                                                                else
                                                                {
                                                                    _with20.Add("ATD_IN", DBNull.Value).Direction = ParameterDirection.Input;
                                                                }
                                                                _with20.Add("ROTATION_NUMBER_IN", dr3["ROTATION_NUMBER"]).Direction = ParameterDirection.Input;
                                                                _with20.Add("BIZ_TYPE_IN", dr3["BIZ_TYPE"]).Direction = ParameterDirection.Input;
                                                                _with20.Add("PROCESS_TYPE_IN", dr3["PROCESS_TYPE"]).Direction = ParameterDirection.Input;
                                                                _with20.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                                                                _with20.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;
                                                                _with20.Add("VERSION_NO_IN", dr2["VERSION_NO"]).Direction = ParameterDirection.Input;
                                                                _with20.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                                                                RecAfct = _with18.ExecuteNonQuery();
                                                            }
                                                        }
                                                    }
                                                }
                                                //'Track Details
                                            }
                                        }
                                    }
                                    //'
                                    //'Air and BBC
                                }
                                else
                                {
                                    if (!string.IsNullOrEmpty(dr["BL_NUMBER"].ToString()))
                                    {
                                        if (dr["BL_NUMBER"] == dr1["BL_NUMBER"])
                                        {
                                            if (string.IsNullOrEmpty(dr1["TRANSPORT_TRN_TRUCK_PK"].ToString()))
                                            {
                                                _with12.CommandText = objWK.MyUserName + ".TRANSPORT_TRN_TRUCK_PKG.TRANSPORT_TRN_TRUCK_INS";
                                                var _with21 = _with12.Parameters;
                                                _with21.Clear();
                                                _with21.Add("TRANSPORT_TRN_CONT_FK_IN", Tp_ContFk).Direction = ParameterDirection.Input;
                                                _with21.Add("BIZ_TYPE_IN", Convert.ToInt32(dr1["BIZ_TYPE"])).Direction = ParameterDirection.Input;
                                                _with21.Add("PROCESS_TYPE_IN", Convert.ToInt32(dr1["PROCESS_TYPE"])).Direction = ParameterDirection.Input;
                                                _with21.Add("CONTAINER_NR_IN", dr1["CONTAINER_NR"]).Direction = ParameterDirection.Input;
                                                _with21.Add("CONTAINER_TYPE_IN", dr1["CONTAINER_TYPE"]).Direction = ParameterDirection.Input;
                                                _with21.Add("SEAL_NUMBER_IN", dr1["SEAL_NUMBER"]).Direction = ParameterDirection.Input;
                                                _with21.Add("TRANSPORTER_MST_FK_IN", Convert.ToInt32(dr1["TRANSPORTER_MST_FK"])).Direction = ParameterDirection.Input;
                                                _with21.Add("COMMODITY_DESC_IN", dr1["COMMODITY_DESC"]).Direction = ParameterDirection.Input;
                                                _with21.Add("BL_NUMBER_IN", dr1["BL_NUMBER"]).Direction = ParameterDirection.Input;
                                                _with21.Add("QTY_IN", dr1["QTY"]).Direction = ParameterDirection.Input;
                                                _with21.Add("NET_WT_IN", dr1["NET_WT"]).Direction = ParameterDirection.Input;
                                                _with21.Add("VOLUME_IN", dr1["VOLUME"]).Direction = ParameterDirection.Input;
                                                _with21.Add("TD_TRUCK_NUMBER_IN", dr1["TD_TRUCK_NUMBER"]).Direction = ParameterDirection.Input;
                                                _with21.Add("TD_CHASIS_NUMBER_IN", dr1["TD_CHASIS_NUMBER"]).Direction = ParameterDirection.Input;
                                                _with21.Add("TD_ODMRS_NUMBER_IN", dr1["TD_ODMRS_NUMBER"]).Direction = ParameterDirection.Input;
                                                _with21.Add("TD_ODMRE_NUMBER_IN", dr1["TD_ODMRE_NUMBER"]).Direction = ParameterDirection.Input;
                                                _with21.Add("TD_TRUCK_REMARKS_IN", dr1["TD_TRUCK_REMARKS"]).Direction = ParameterDirection.Input;
                                                _with21.Add("DD_DRIVER1_NAME_IN", dr1["DD_DRIVER1_NAME"]).Direction = ParameterDirection.Input;
                                                _with21.Add("DD_DRIVER2_NAME_IN", dr1["DD_DRIVER2_NAME"]).Direction = ParameterDirection.Input;
                                                _with21.Add("DD_LICENSE_NUMBER_IN", dr1["DD_LICENSE_NUMBER"]).Direction = ParameterDirection.Input;
                                                _with21.Add("DD_LICENSE_EXP_ON_IN", dr1["DD_LICENSE_EXP_ON"]).Direction = ParameterDirection.Input;
                                                _with21.Add("DD_PASSPORT_NUMBER_IN", dr1["DD_PASSPORT_NUMBER"]).Direction = ParameterDirection.Input;
                                                _with21.Add("DD_CONTACT_NUMBER1_IN", dr1["DD_CONTACT_NUMBER1"]).Direction = ParameterDirection.Input;
                                                _with21.Add("DD_CONTACT_NUMBER2_IN", dr1["DD_CONTACT_NUMBER2"]).Direction = ParameterDirection.Input;
                                                _with21.Add("DD_CONTACT_NUMBER3_IN", dr1["DD_CONTACT_NUMBER3"]).Direction = ParameterDirection.Input;
                                                _with21.Add("DD_OFFICE_NUMBER_IN", dr1["DD_OFFICE_NUMBER"]).Direction = ParameterDirection.Input;
                                                _with21.Add("DD_C65_FORM_NUMBER_IN", dr1["DD_C65_FORM_NUMBER"]).Direction = ParameterDirection.Input;
                                                _with21.Add("DD_PASSPORT_EXP_ON_IN", dr1["DD_PASSPORT_EXP_ON"]).Direction = ParameterDirection.Input;
                                                _with21.Add("DD_WORK_PERMIT_NO_IN", dr1["DD_WORK_PERMIT_NO"]).Direction = ParameterDirection.Input;
                                                _with21.Add("DD_WORK_PERMIT_EXP_ON_IN", dr1["DD_WORK_PERMIT_EXP_ON"]).Direction = ParameterDirection.Input;

                                                _with21.Add("CWD_CARGO_WT_IN", dr1["CWD_CARGO_WT"]).Direction = ParameterDirection.Input;
                                                _with21.Add("CWD_WT_LIMIT_ROAD_IN", dr1["CWD_WT_LIMIT_ROAD"]).Direction = ParameterDirection.Input;
                                                _with21.Add("CWD_EXCESS_WT_IN", dr1["CWD_EXCESS_WT"]).Direction = ParameterDirection.Input;
                                                _with21.Add("CWD_PENALTY_AMOUNT_IN", dr1["CWD_PENALTY_AMOUNT"]).Direction = ParameterDirection.Input;
                                                _with21.Add("CWD_PENALTY_APPLICABLE_IN", dr1["CWD_PENALTY_APPLICABLE"]).Direction = ParameterDirection.Input;
                                                _with21.Add("CTR_INSP_STATUS_IN", dr1["CTR_INSP_STATUS"]).Direction = ParameterDirection.Input;
                                                _with21.Add("CTR_INSP_DAMAGE_DESC_FK_IN", dr1["CTR_INSP_DAMAGE_DESC_FK"]).Direction = ParameterDirection.Input;
                                                _with21.Add("CTR_INSP_REF_NR_IN", dr1["CTR_INSP_REF_NR"]).Direction = ParameterDirection.Input;
                                                _with21.Add("CTR_INSP_DATE_IN", dr1["CTR_INSP_DATE"]).Direction = ParameterDirection.Input;
                                                _with21.Add("CTR_INSP_BY_IN", dr1["CTR_INSP_BY"]).Direction = ParameterDirection.Input;
                                                _with21.Add("CTR_INSP_REMARKS_IN", dr1["CTR_INSP_REMARKS"]).Direction = ParameterDirection.Input;
                                                _with21.Add("CTR_MOV_T1_REF_NR_IN", dr1["CTR_MOV_T1_REF_NR"]).Direction = ParameterDirection.Input;
                                                _with21.Add("CTR_MOV_T1_DT_IN", dr1["CTR_MOV_T1_DT"]).Direction = ParameterDirection.Input;
                                                _with21.Add("CTR_MOV_LOAD_DT_IN", dr1["CTR_MOV_LOAD_DT"]).Direction = ParameterDirection.Input;
                                                _with21.Add("CTR_MOV_UNLOAD_DT_IN", dr1["CTR_MOV_UNLOAD_DT"]).Direction = ParameterDirection.Input;
                                                _with21.Add("CTR_NR_OF_BORDERS_IN", dr1["CTR_NR_OF_BORDERS"]).Direction = ParameterDirection.Input;
                                                _with21.Add("PD_MTY_CTR_PICKUP_ADD_IN", dr1["PD_MTY_CTR_PICKUP_ADD"]).Direction = ParameterDirection.Input;
                                                _with21.Add("PD_MTY_CTR_DROP_ADD_IN", dr1["PD_MTY_CTR_DROP_ADD"]).Direction = ParameterDirection.Input;
                                                _with21.Add("PD_MTY_CTR_PICK_DT_IN", dr1["PD_MTY_CTR_PICK_DT"]).Direction = ParameterDirection.Input;
                                                _with21.Add("PD_MTY_CTR_DROP_DT_IN", dr1["PD_MTY_CTR_DROP_DT"]).Direction = ParameterDirection.Input;
                                                _with21.Add("PD_CARGO_PICKUP_ADD_IN", dr1["PD_CARGO_PICKUP_ADD"]).Direction = ParameterDirection.Input;
                                                _with21.Add("PD_CARGO_DROP_ADD_IN", dr1["PD_CARGO_DROP_ADD"]).Direction = ParameterDirection.Input;
                                                _with21.Add("PD_CARGO_PICKUP_DT_IN", dr1["PD_CARGO_PICKUP_DT"]).Direction = ParameterDirection.Input;
                                                _with21.Add("PD_CARGO_DROP_DT_IN", dr1["PD_CARGO_DROP_DT"]).Direction = ParameterDirection.Input;
                                                _with21.Add("PD_CTR_RETURN_ADD_IN", dr1["PD_CTR_RETURN_ADD"]).Direction = ParameterDirection.Input;
                                                _with21.Add("PD_CTR_RETURN_DT_IN", dr1["PD_CTR_RETURN_DT"]).Direction = ParameterDirection.Input;
                                                _with21.Add("PD_CTR_RETURN_DT_BYCUST_IN", DBNull.Value).Direction = ParameterDirection.Input;
                                                _with21.Add("PD_SPECIAL_INST_IN", dr1["PD_SPECIAL_INST"]).Direction = ParameterDirection.Input;
                                                _with21.Add("EST_DT_DEL_IN", dr1["EST_DT_DEL"]).Direction = ParameterDirection.Input;
                                                _with21.Add("EST_DT_MT_DEL_IN", dr1["EST_DT_MT_DEL"]).Direction = ParameterDirection.Input;
                                                _with21.Add("PD_REMARKS_IN", dr1["PD_REMARKS"]).Direction = ParameterDirection.Input;
                                                _with21.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                                                _with21.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;
                                                _with21.Add("RETURN_VALUE", OracleDbType.Int32, 10, "TRANSPORT_TRN_TRUCK_PK").Direction = ParameterDirection.Output;
                                                RecAfct = _with12.ExecuteNonQuery();
                                                if (RecAfct > 0)
                                                {
                                                    Tp_TruckFk = Convert.ToInt32(_with12.Parameters["RETURN_VALUE"].Value);
                                                }
                                            }
                                            else
                                            {
                                                _with12.CommandText = objWK.MyUserName + ".TRANSPORT_TRN_TRUCK_PKG.TRANSPORT_TRN_TRUCK_UPD";
                                                var _with22 = _with12.Parameters;
                                                _with22.Clear();
                                                _with22.Add("TRANSPORT_TRN_CONT_FK_IN", Tp_ContFk).Direction = ParameterDirection.Input;
                                                _with22.Add("TRANSPORT_TRN_TRUCK_PK_IN", dr1["TRANSPORT_TRN_TRUCK_PK"]).Direction = ParameterDirection.Input;
                                                _with22.Add("BIZ_TYPE_IN", dr1["BIZ_TYPE"]).Direction = ParameterDirection.Input;
                                                _with22.Add("PROCESS_TYPE_IN", dr1["PROCESS_TYPE"]).Direction = ParameterDirection.Input;
                                                _with22.Add("CONTAINER_NR_IN", dr1["CONTAINER_NR"]).Direction = ParameterDirection.Input;
                                                _with22.Add("CONTAINER_TYPE_IN", dr1["CONTAINER_TYPE"]).Direction = ParameterDirection.Input;
                                                _with22.Add("SEAL_NUMBER_IN", dr1["SEAL_NUMBER"]).Direction = ParameterDirection.Input;
                                                _with22.Add("TRANSPORTER_MST_FK_IN", dr1["TRANSPORTER_MST_FK"]).Direction = ParameterDirection.Input;
                                                _with22.Add("COMMODITY_DESC_IN", dr1["COMMODITY_DESC"]).Direction = ParameterDirection.Input;
                                                _with22.Add("BL_NUMBER_IN", dr1["BL_NUMBER"]).Direction = ParameterDirection.Input;
                                                _with22.Add("QTY_IN", dr1["QTY"]).Direction = ParameterDirection.Input;
                                                _with22.Add("NET_WT_IN", dr1["NET_WT"]).Direction = ParameterDirection.Input;
                                                _with22.Add("VOLUME_IN", dr1["VOLUME"]).Direction = ParameterDirection.Input;
                                                _with22.Add("TD_TRUCK_NUMBER_IN", dr1["TD_TRUCK_NUMBER"]).Direction = ParameterDirection.Input;
                                                _with22.Add("TD_CHASIS_NUMBER_IN", dr1["TD_CHASIS_NUMBER"]).Direction = ParameterDirection.Input;
                                                _with22.Add("TD_ODMRS_NUMBER_IN", dr1["TD_ODMRS_NUMBER"]).Direction = ParameterDirection.Input;
                                                _with22.Add("TD_ODMRE_NUMBER_IN", dr1["TD_ODMRE_NUMBER"]).Direction = ParameterDirection.Input;
                                                _with22.Add("TD_TRUCK_REMARKS_IN", dr1["TD_TRUCK_REMARKS"]).Direction = ParameterDirection.Input;
                                                _with22.Add("DD_DRIVER1_NAME_IN", dr1["DD_DRIVER1_NAME"]).Direction = ParameterDirection.Input;
                                                _with22.Add("DD_DRIVER2_NAME_IN", dr1["DD_DRIVER2_NAME"]).Direction = ParameterDirection.Input;
                                                _with22.Add("DD_LICENSE_NUMBER_IN", dr1["DD_LICENSE_NUMBER"]).Direction = ParameterDirection.Input;
                                                _with22.Add("DD_LICENSE_EXP_ON_IN", dr1["DD_LICENSE_EXP_ON"]).Direction = ParameterDirection.Input;
                                                _with22.Add("DD_PASSPORT_NUMBER_IN", dr1["DD_PASSPORT_NUMBER"]).Direction = ParameterDirection.Input;
                                                _with22.Add("DD_CONTACT_NUMBER1_IN", dr1["DD_CONTACT_NUMBER1"]).Direction = ParameterDirection.Input;
                                                _with22.Add("DD_CONTACT_NUMBER2_IN", dr1["DD_CONTACT_NUMBER2"]).Direction = ParameterDirection.Input;
                                                _with22.Add("DD_CONTACT_NUMBER3_IN", dr1["DD_CONTACT_NUMBER3"]).Direction = ParameterDirection.Input;
                                                _with22.Add("DD_OFFICE_NUMBER_IN", dr1["DD_OFFICE_NUMBER"]).Direction = ParameterDirection.Input;
                                                _with22.Add("DD_C65_FORM_NUMBER_IN", dr1["DD_C65_FORM_NUMBER"]).Direction = ParameterDirection.Input;
                                                _with22.Add("DD_PASSPORT_EXP_ON_IN", dr1["DD_PASSPORT_EXP_ON"]).Direction = ParameterDirection.Input;
                                                _with22.Add("DD_WORK_PERMIT_NO_IN", dr1["DD_WORK_PERMIT_NO"]).Direction = ParameterDirection.Input;
                                                _with22.Add("DD_WORK_PERMIT_EXP_ON_IN", dr1["DD_WORK_PERMIT_EXP_ON"]).Direction = ParameterDirection.Input;

                                                _with22.Add("CWD_CARGO_WT_IN", dr1["CWD_CARGO_WT"]).Direction = ParameterDirection.Input;
                                                _with22.Add("CWD_WT_LIMIT_ROAD_IN", dr1["CWD_WT_LIMIT_ROAD"]).Direction = ParameterDirection.Input;
                                                _with22.Add("CWD_EXCESS_WT_IN", dr1["CWD_EXCESS_WT"]).Direction = ParameterDirection.Input;
                                                _with22.Add("CWD_PENALTY_AMOUNT_IN", dr1["CWD_PENALTY_AMOUNT"]).Direction = ParameterDirection.Input;
                                                _with22.Add("CWD_PENALTY_APPLICABLE_IN", dr1["CWD_PENALTY_APPLICABLE"]).Direction = ParameterDirection.Input;
                                                _with22.Add("CTR_INSP_STATUS_IN", dr1["CTR_INSP_STATUS"]).Direction = ParameterDirection.Input;
                                                _with22.Add("CTR_INSP_DAMAGE_DESC_FK_IN", dr1["CTR_INSP_DAMAGE_DESC_FK"]).Direction = ParameterDirection.Input;
                                                _with22.Add("CTR_INSP_REF_NR_IN", dr1["CTR_INSP_REF_NR"]).Direction = ParameterDirection.Input;
                                                _with22.Add("CTR_INSP_DATE_IN", dr1["CTR_INSP_DATE"]).Direction = ParameterDirection.Input;
                                                _with22.Add("CTR_INSP_BY_IN", dr1["CTR_INSP_BY"]).Direction = ParameterDirection.Input;
                                                _with22.Add("CTR_INSP_REMARKS_IN", dr1["CTR_INSP_REMARKS"]).Direction = ParameterDirection.Input;
                                                _with22.Add("CTR_MOV_T1_REF_NR_IN", dr1["CTR_MOV_T1_REF_NR"]).Direction = ParameterDirection.Input;
                                                _with22.Add("CTR_MOV_T1_DT_IN", dr1["CTR_MOV_T1_DT"]).Direction = ParameterDirection.Input;
                                                _with22.Add("CTR_MOV_LOAD_DT_IN", dr1["CTR_MOV_LOAD_DT"]).Direction = ParameterDirection.Input;
                                                _with22.Add("CTR_MOV_UNLOAD_DT_IN", dr1["CTR_MOV_UNLOAD_DT"]).Direction = ParameterDirection.Input;
                                                _with22.Add("CTR_NR_OF_BORDERS_IN", dr1["CTR_NR_OF_BORDERS"]).Direction = ParameterDirection.Input;
                                                _with22.Add("PD_MTY_CTR_PICKUP_ADD_IN", dr1["PD_MTY_CTR_PICKUP_ADD"]).Direction = ParameterDirection.Input;
                                                _with22.Add("PD_MTY_CTR_DROP_ADD_IN", dr1["PD_MTY_CTR_DROP_ADD"]).Direction = ParameterDirection.Input;
                                                _with22.Add("PD_MTY_CTR_PICK_DT_IN", dr1["PD_MTY_CTR_PICK_DT"]).Direction = ParameterDirection.Input;
                                                _with22.Add("PD_MTY_CTR_DROP_DT_IN", dr1["PD_MTY_CTR_DROP_DT"]).Direction = ParameterDirection.Input;
                                                _with22.Add("PD_CARGO_PICKUP_ADD_IN", dr1["PD_CARGO_PICKUP_ADD"]).Direction = ParameterDirection.Input;
                                                _with22.Add("PD_CARGO_DROP_ADD_IN", dr1["PD_CARGO_DROP_ADD"]).Direction = ParameterDirection.Input;
                                                _with22.Add("PD_CARGO_PICKUP_DT_IN", dr1["PD_CARGO_PICKUP_DT"]).Direction = ParameterDirection.Input;
                                                _with22.Add("PD_CARGO_DROP_DT_IN", dr1["PD_CARGO_DROP_DT"]).Direction = ParameterDirection.Input;
                                                _with22.Add("PD_CTR_RETURN_ADD_IN", dr1["PD_CTR_RETURN_ADD"]).Direction = ParameterDirection.Input;
                                                _with22.Add("PD_CTR_RETURN_DT_IN", dr1["PD_CTR_RETURN_DT"]).Direction = ParameterDirection.Input;
                                                _with22.Add("PD_CTR_RETURN_DT_BYCUST_IN", DBNull.Value).Direction = ParameterDirection.Input;
                                                _with22.Add("EST_DT_DEL_IN", dr1["EST_DT_DEL"]).Direction = ParameterDirection.Input;
                                                _with22.Add("EST_DT_MT_DEL_IN", dr1["EST_DT_MT_DEL"]).Direction = ParameterDirection.Input;
                                                _with22.Add("PD_SPECIAL_INST_IN", dr1["PD_SPECIAL_INST"]).Direction = ParameterDirection.Input;
                                                _with22.Add("PD_REMARKS_IN", dr1["PD_REMARKS"]).Direction = ParameterDirection.Input;
                                                _with22.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                                                _with22.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;
                                                _with22.Add("VERSION_NO_IN", dr1["VERSION_NO"]).Direction = ParameterDirection.Input;
                                                _with22.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                                                RecAfct = _with12.ExecuteNonQuery();
                                                if (RecAfct > 0)
                                                {
                                                    if (!string.IsNullOrEmpty(_with12.Parameters["RETURN_VALUE"].Value.ToString()))
                                                    {
                                                        Tp_TruckFk = Convert.ToInt32(_with12.Parameters["RETURN_VALUE"].Value);
                                                    }
                                                    else
                                                    {
                                                        Tp_TruckFk = 0;
                                                    }
                                                }
                                            }
                                            //'Doc Details
                                            if ((dsDocDetails != null))
                                            {
                                                var _with23 = objWK.MyCommand;
                                                _with23.Transaction = TRAN;
                                                _with23.Connection = objWK.MyConnection;
                                                _with23.CommandType = CommandType.StoredProcedure;
                                                if (dsDocDetails.Tables[0].Rows.Count > 0)
                                                {
                                                    foreach (DataRow dr2_loopVariable in dsDocDetails.Tables[0].Rows)
                                                    {
                                                        dr2 = dr2_loopVariable;
                                                        if (dr1["BIZ_TYPE"] == dr2["BIZ_TYPE"] & dr1["PROCESS_TYPE"] == dr2["PROCESS_TYPE"] & dr1["TRANSPORTER_MST_FK"] == dr2["TRANSPORTER_MST_FK"] & dr1["TD_TRUCK_NUMBER"] == dr2["TD_TRUCK_NUMBER"])
                                                        {
                                                            _with23.CommandText = objWK.MyUserName + ".TRANSPORT_TRN_DOC_PKG.TRANSPORT_TRN_DOC_INS";
                                                            if (string.IsNullOrEmpty(dr2["TRANSPORT_TRN_DOC_PK"].ToString()))
                                                            {
                                                                var _with24 = _with23.Parameters;
                                                                _with24.Clear();
                                                                _with24.Add("TRANSPORT_TRN_TRUCK_FK_IN", Tp_TruckFk).Direction = ParameterDirection.Input;
                                                                _with24.Add("DOCUMENT_REF_NR_IN", dr2["DOCUMENT_REF_NR"]).Direction = ParameterDirection.Input;
                                                                _with24.Add("DOCUMENT_NAME_IN", dr2["DOC_NAME"]).Direction = ParameterDirection.Input;
                                                                _with24.Add("DOCUMENT_TYPE_IN", dr2["DOC_TYPE"]).Direction = ParameterDirection.Input;
                                                                if (!string.IsNullOrEmpty(dr2["DOCUMENT_RECEIVED_ON"].ToString()))
                                                                {
                                                                    _with24.Add("DOCUMENT_RECEIVED_ON_IN", getDefault(Convert.ToDateTime(dr2["DOCUMENT_RECEIVED_ON"]), DBNull.Value)).Direction = ParameterDirection.Input;
                                                                }
                                                                else
                                                                {
                                                                    _with24.Add("DOCUMENT_RECEIVED_ON_IN", DBNull.Value).Direction = ParameterDirection.Input;
                                                                }
                                                                _with24.Add("DOCUMENT_REMARKS_IN", dr2["DOCUMENT_REMARKS"]).Direction = ParameterDirection.Input;
                                                                _with24.Add("BIZ_TYPE_IN", dr2["BIZ_TYPE"]).Direction = ParameterDirection.Input;
                                                                _with24.Add("PROCESS_TYPE_IN", dr2["PROCESS_TYPE"]).Direction = ParameterDirection.Input;
                                                                _with24.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                                                                _with24.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;
                                                                _with24.Add("RETURN_VALUE", OracleDbType.Int32, 10, "TRANSPORT_TRN_DOC_PK").Direction = ParameterDirection.Output;
                                                                RecAfct = _with23.ExecuteNonQuery();
                                                            }
                                                            else
                                                            {
                                                                if (Convert.ToInt32(dr2["TRANSPORT_TRN_TRUCK_FK"]) == Tp_TruckFk)
                                                                {
                                                                    _with23.CommandText = objWK.MyUserName + ".TRANSPORT_TRN_DOC_PKG.TRANSPORT_TRN_DOC_UPD";
                                                                    var _with25 = _with23.Parameters;
                                                                    _with25.Clear();
                                                                    _with25.Add("TRANSPORT_TRN_DOC_PK_IN", dr2["TRANSPORT_TRN_DOC_PK"]).Direction = ParameterDirection.Input;
                                                                    _with25.Add("TRANSPORT_TRN_TRUCK_FK_IN", Tp_TruckFk).Direction = ParameterDirection.Input;
                                                                    _with25.Add("DOCUMENT_REF_NR_IN", dr2["DOCUMENT_REF_NR"]).Direction = ParameterDirection.Input;
                                                                    _with25.Add("DOCUMENT_NAME_IN", dr2["DOC_NAME"]).Direction = ParameterDirection.Input;
                                                                    _with25.Add("DOCUMENT_TYPE_IN", dr2["DOC_TYPE"]).Direction = ParameterDirection.Input;
                                                                    if (!string.IsNullOrEmpty(dr2["DOCUMENT_RECEIVED_ON"].ToString()))
                                                                    {
                                                                        _with25.Add("DOCUMENT_RECEIVED_ON_IN", getDefault(Convert.ToDateTime(dr2["DOCUMENT_RECEIVED_ON"]), DBNull.Value)).Direction = ParameterDirection.Input;
                                                                    }
                                                                    else
                                                                    {
                                                                        _with25.Add("DOCUMENT_RECEIVED_ON_IN", DBNull.Value).Direction = ParameterDirection.Input;
                                                                    }
                                                                    _with25.Add("DOCUMENT_REMARKS_IN", dr2["DOCUMENT_REMARKS"]).Direction = ParameterDirection.Input;
                                                                    _with25.Add("BIZ_TYPE_IN", dr2["BIZ_TYPE"]).Direction = ParameterDirection.Input;
                                                                    _with25.Add("PROCESS_TYPE_IN", dr2["PROCESS_TYPE"]).Direction = ParameterDirection.Input;
                                                                    _with25.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                                                                    _with25.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;
                                                                    _with25.Add("VERSION_NO_IN", dr2["VERSION_NO"]).Direction = ParameterDirection.Input;
                                                                    _with25.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                                                                    RecAfct = _with23.ExecuteNonQuery();
                                                                }
                                                            }
                                                        }
                                                    }
                                                    //'Doc Details
                                                }
                                            }
                                            //'Track Details
                                            if ((dsTrackDetails != null))
                                            {
                                                var _with26 = objWK.MyCommand;
                                                _with26.Transaction = TRAN;
                                                _with26.Connection = objWK.MyConnection;
                                                _with26.CommandType = CommandType.StoredProcedure;
                                                if (dsTrackDetails.Tables[0].Rows.Count > 0)
                                                {
                                                    foreach (DataRow dr3_loopVariable in dsTrackDetails.Tables[0].Rows)
                                                    {
                                                        dr3 = dr3_loopVariable;
                                                        if (dr1["BIZ_TYPE"] == dr3["BIZ_TYPE"] & dr1["PROCESS_TYPE"] == dr3["PROCESS_TYPE"] & dr1["TRANSPORTER_MST_FK"] == dr3["TRANSPORTER_MST_FK"] & dr1["TD_TRUCK_NUMBER"] == dr3["TD_TRUCK_NUMBER"])
                                                        {
                                                            _with26.CommandText = objWK.MyUserName + ".TRANSPORT_TRN_TRACK_PKG.TRANSPORT_TRN_TRACK_INS";
                                                            if (string.IsNullOrEmpty(dr3["TRANSPORT_TRN_TRACK_PK"].ToString()))
                                                            {
                                                                var _with27 = _with26.Parameters;
                                                                _with27.Clear();
                                                                _with27.Add("TRANSPORT_TRN_TRUCK_FK_IN", Tp_TruckFk).Direction = ParameterDirection.Input;
                                                                _with27.Add("ONWARD_RETURN_IN", dr3["ONWARD_RETURN"]).Direction = ParameterDirection.Input;
                                                                _with27.Add("BORDER_NAME_IN", dr3["BORDER_NAME"]).Direction = ParameterDirection.Input;
                                                                if (!string.IsNullOrEmpty(dr3["ETA"].ToString()))
                                                                {
                                                                    _with27.Add("ETA_IN", Convert.ToDateTime(dr3["ETA"])).Direction = ParameterDirection.Input;
                                                                }
                                                                else
                                                                {
                                                                    _with27.Add("ETA_IN", DBNull.Value).Direction = ParameterDirection.Input;
                                                                }
                                                                if (!string.IsNullOrEmpty(dr3["ETD"].ToString()))
                                                                {
                                                                    _with27.Add("ETD_IN", Convert.ToDateTime(dr3["ETD"])).Direction = ParameterDirection.Input;
                                                                }
                                                                else
                                                                {
                                                                    _with27.Add("ETD_IN", DBNull.Value).Direction = ParameterDirection.Input;
                                                                }
                                                                if (!string.IsNullOrEmpty(dr3["ATA"].ToString()))
                                                                {
                                                                    _with27.Add("ATA_IN", Convert.ToDateTime(dr3["ATA"])).Direction = ParameterDirection.Input;
                                                                }
                                                                else
                                                                {
                                                                    _with27.Add("ATA_IN", DBNull.Value).Direction = ParameterDirection.Input;
                                                                }
                                                                if (!string.IsNullOrEmpty(dr3["ATD"].ToString()))
                                                                {
                                                                    _with27.Add("ATD_IN", Convert.ToDateTime(dr3["ATD"])).Direction = ParameterDirection.Input;
                                                                }
                                                                else
                                                                {
                                                                    _with27.Add("ATD_IN", DBNull.Value).Direction = ParameterDirection.Input;
                                                                }
                                                                _with27.Add("ROTATION_NUMBER_IN", dr3["ROTATION_NUMBER"]).Direction = ParameterDirection.Input;
                                                                _with27.Add("BIZ_TYPE_IN", dr3["BIZ_TYPE"]).Direction = ParameterDirection.Input;
                                                                _with27.Add("PROCESS_TYPE_IN", dr3["PROCESS_TYPE"]).Direction = ParameterDirection.Input;
                                                                _with27.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                                                                _with27.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;
                                                                _with27.Add("RETURN_VALUE", OracleDbType.Int32, 10, "TRANSPORT_TRN_TRACK_PK").Direction = ParameterDirection.Output;
                                                                RecAfct = _with26.ExecuteNonQuery();
                                                            }
                                                            else
                                                            {
                                                                if (Convert.ToInt32(dr3["TRANSPORT_TRN_TRUCK_FK"]) == Tp_TruckFk)
                                                                {
                                                                    _with26.CommandText = objWK.MyUserName + ".TRANSPORT_TRN_TRACK_PKG.TRANSPORT_TRN_TRACK_UPD";
                                                                    var _with28 = _with26.Parameters;
                                                                    _with28.Clear();
                                                                    _with28.Add("TRANSPORT_TRN_TRACK_PK_IN", dr3["TRANSPORT_TRN_TRACK_PK"]).Direction = ParameterDirection.Input;
                                                                    _with28.Add("TRANSPORT_TRN_TRUCK_FK_IN", Tp_TruckFk).Direction = ParameterDirection.Input;
                                                                    _with28.Add("ONWARD_RETURN_IN", dr3["ONWARD_RETURN"]).Direction = ParameterDirection.Input;
                                                                    _with28.Add("BORDER_NAME_IN", dr3["BORDER_NAME"]).Direction = ParameterDirection.Input;
                                                                    if (!string.IsNullOrEmpty(dr3["ETA"].ToString()))
                                                                    {
                                                                        _with28.Add("ETA_IN", Convert.ToDateTime(dr3["ETA"])).Direction = ParameterDirection.Input;
                                                                    }
                                                                    else
                                                                    {
                                                                        _with28.Add("ETA_IN", DBNull.Value).Direction = ParameterDirection.Input;
                                                                    }
                                                                    if (!string.IsNullOrEmpty(dr3["ETD"].ToString()))
                                                                    {
                                                                        _with28.Add("ETD_IN", Convert.ToDateTime(dr3["ETD"])).Direction = ParameterDirection.Input;
                                                                    }
                                                                    else
                                                                    {
                                                                        _with28.Add("ETD_IN", DBNull.Value).Direction = ParameterDirection.Input;
                                                                    }
                                                                    if (!string.IsNullOrEmpty(dr3["ATA"].ToString()))
                                                                    {
                                                                        _with28.Add("ATA_IN", Convert.ToDateTime(dr3["ATA"])).Direction = ParameterDirection.Input;
                                                                    }
                                                                    else
                                                                    {
                                                                        _with28.Add("ATA_IN", DBNull.Value).Direction = ParameterDirection.Input;
                                                                    }
                                                                    if (!string.IsNullOrEmpty(dr3["ATD"].ToString()))
                                                                    {
                                                                        _with28.Add("ATD_IN", Convert.ToDateTime(dr3["ATD"])).Direction = ParameterDirection.Input;
                                                                    }
                                                                    else
                                                                    {
                                                                        _with28.Add("ATD_IN", DBNull.Value).Direction = ParameterDirection.Input;
                                                                    }
                                                                    _with28.Add("ROTATION_NUMBER_IN", dr3["ROTATION_NUMBER"]).Direction = ParameterDirection.Input;
                                                                    _with28.Add("BIZ_TYPE_IN", dr3["BIZ_TYPE"]).Direction = ParameterDirection.Input;
                                                                    _with28.Add("PROCESS_TYPE_IN", dr3["PROCESS_TYPE"]).Direction = ParameterDirection.Input;
                                                                    _with28.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                                                                    _with28.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;
                                                                    _with28.Add("VERSION_NO_IN", dr2["VERSION_NO"]).Direction = ParameterDirection.Input;
                                                                    _with28.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                                                                    RecAfct = _with26.ExecuteNonQuery();
                                                                }
                                                            }
                                                        }
                                                    }
                                                    //'Track Details
                                                }
                                            }
                                        }
                                        //'
                                    }
                                }
                            }
                            //'Truck details

                        }
                        //'
                    }
                    //' Cont Details
                }
                //'Delete Selected Records
                if (isEdting == true)
                {
                    DeleteRecords(Convert.ToInt32(TPNotePK), objWK, TRAN, dsContainerData, dsTruckMainData, dsDocDetails, dsTrackDetails);
                }
                //'
                if (!SaveSecondaryServices(objWK, TRAN, Convert.ToInt32(TPNotePK), dsIncomeChargeDetails, dsExpenseChargeDetails))
                {
                    arrMessage.Add("Error while saving secondary service details");
                    return arrMessage;
                }
                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    TRAN.Commit();
                    //Push to financial system if realtime is selected
                    if (TPNotePK > 0)
                    {
                        cls_Scheduler objSch = new cls_Scheduler();
                        ArrayList schDtls = null;
                        bool errGen = false;
                        if (objSch.GetSchedulerPushType() == true)
                        {
                            //QFSIService.serFinApp objPush = new QFSIService.serFinApp();
                            //try
                            //{
                            //    schDtls = objSch.FetchSchDtls();
                            //    //'Used to Fetch the Sch Dtls
                            //    objPush.UpdateCostCentre(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen, schDtls[5].ToString().ToUpper(), schDtls[0].ToString().ToUpper(), , , TPNotePK);
                            //    if (ConfigurationSettings.AppSettings["EVENTVIEWER"])
                            //    {
                            //        objPush.EventViewer(1, 1, HttpContext.Current.Session["USER_PK"]);
                            //    }
                            //}
                            //catch (Exception ex)
                            //{
                            //    if (ConfigurationSettings.AppSettings["EVENTVIEWER"])
                            //    {
                            //        objPush.EventViewer(1, 2, HttpContext.Current.Session["USER_PK"]);
                            //    }
                            //}
                        }
                    }
                    //*****************************************************************
                    arrMessage.Add("All Data Saved Successfully");
                    return arrMessage;

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
        }
        #endregion

        #region "Delete Records"
        public ArrayList DeleteRecords(int TPNotePK, WorkFlow objWK, OracleTransaction TRAN, DataSet dsContainerData = null, DataSet dsTruckMainData = null, DataSet dsDocDetails = null, DataSet dsTrackDetails = null)
        {
            Int32 RecAfct = default(Int32);
            arrMessage.Clear();
            try
            {
                if (TPNotePK > 0)
                {
                    string SelectedContPks = "0";
                    string SelectedTruckPks = "0";
                    string SelectedDocPks = "0";
                    string SelectedTrackPks = "0";
                    if (dsContainerData.Tables[0].Rows.Count > 0)
                    {
                        foreach (DataRow ri in dsContainerData.Tables[0].Rows)
                        {
                            if (!string.IsNullOrEmpty(ri["SEL"].ToString()))
                            {
                                if (Convert.ToInt32(getDefault(ri["TRANSPORT_TRN_CONT_PK"], 0)) > 0 & (ri["SEL"] == "1" | ri["SEL"] == "true"))
                                {
                                    SelectedContPks += "," + getDefault(ri["TRANSPORT_TRN_CONT_PK"], 0);
                                }
                            }
                        }
                    }
                    if ((dsTruckMainData != null))
                    {
                        if (dsTruckMainData.Tables[0].Rows.Count > 0)
                        {
                            foreach (DataRow re in dsTruckMainData.Tables[0].Rows)
                            {
                                if (!string.IsNullOrEmpty(re["SEL"].ToString()))
                                {
                                    if (Convert.ToInt32(getDefault(re["TRANSPORT_TRN_TRUCK_PK"], 0)) > 0 & (re["SEL"] == "1" | re["SEL"] == "true"))
                                    {
                                        SelectedTruckPks += "," + getDefault(re["TRANSPORT_TRN_TRUCK_PK"], 0);
                                    }
                                }
                            }
                        }
                    }
                    if ((dsDocDetails != null))
                    {
                        if (dsDocDetails.Tables[0].Rows.Count > 0)
                        {
                            foreach (DataRow re in dsDocDetails.Tables[0].Rows)
                            {
                                if (!string.IsNullOrEmpty(re["SEL"].ToString()))
                                {
                                    if (Convert.ToInt32(getDefault(re["TRANSPORT_TRN_DOC_PK"], 0)) > 0 & (re["SEL"] == "1" | re["SEL"] == "true"))
                                    {
                                        SelectedDocPks += "," + getDefault(re["TRANSPORT_TRN_DOC_PK"], 0);
                                    }
                                }
                            }
                        }
                    }
                    if ((dsTrackDetails != null))
                    {
                        if (dsTrackDetails.Tables[0].Rows.Count > 0)
                        {
                            foreach (DataRow re in dsTrackDetails.Tables[0].Rows)
                            {
                                if (!string.IsNullOrEmpty(re["SEL"].ToString()))
                                {
                                    if (Convert.ToInt32(getDefault(re["TRANSPORT_TRN_TRACK_PK"], 0)) > 0 & (re["SEL"] == "1" | re["SEL"] == "true"))
                                    {
                                        SelectedTrackPks += "," + getDefault(re["TRANSPORT_TRN_TRACK_PK"], 0);
                                    }
                                }
                            }
                        }
                    }
                    var _with29 = objWK.MyCommand;
                    _with29.Transaction = TRAN;
                    _with29.Connection = objWK.MyConnection;
                    _with29.CommandType = CommandType.StoredProcedure;
                    _with29.CommandText = objWK.MyUserName + ".TRANSPORT_TRN_CONT_PKG.TRANSPORT_TRN_CONT_DEL";
                    var _with30 = _with29.Parameters;
                    _with30.Clear();
                    _with30.Add("TRANSPORT_INST_FK_IN", TPNotePK).Direction = ParameterDirection.Input;
                    _with30.Add("TRANSPORT_TRN_CONT_FKS_IN", (string.IsNullOrEmpty(SelectedContPks) ? "" : SelectedContPks)).Direction = ParameterDirection.Input;
                    _with30.Add("TRANSPORT_TRN_TRUCK_FKS_IN", (string.IsNullOrEmpty(SelectedTruckPks) ? "" : SelectedTruckPks)).Direction = ParameterDirection.Input;
                    _with30.Add("TRANSPORT_TRN_DOC_FKS_IN", (string.IsNullOrEmpty(SelectedDocPks) ? "" : SelectedDocPks)).Direction = ParameterDirection.Input;
                    _with30.Add("TRANSPORT_TRN_TRACK_FKS_IN", (string.IsNullOrEmpty(SelectedTrackPks) ? "": SelectedTrackPks)).Direction = ParameterDirection.Input;
                    _with30.Add("DELETED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                    _with30.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;
                    _with30.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
                    RecAfct = _with29.ExecuteNonQuery();
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
                //objWK.CloseConnection()
            }
            return new ArrayList();
        }
        #endregion

        #region "Secondary Services"
        public bool SaveSecondaryServices(WorkFlow objWK, OracleTransaction TRAN, int TpNote_Pk, DataSet dsIncomeChargeDetails, DataSet dsExpenseChargeDetails)
        {
            if ((dsIncomeChargeDetails != null))
            {
                //----------------------------------Income Charge Details----------------------------------
                foreach (DataRow ri in dsIncomeChargeDetails.Tables[1].Rows)
                {
                    int Frt_Pk = 0;
                    try
                    {
                        Frt_Pk = Convert.ToInt32(ri["TRANSPORT_TRN_FD_PK"]);
                    }
                    catch (Exception ex)
                    {
                        Frt_Pk = 0;
                    }
                    var _with31 = objWK.MyCommand;
                    _with31.Parameters.Clear();
                    _with31.Transaction = TRAN;
                    _with31.CommandType = CommandType.StoredProcedure;
                    if (Frt_Pk > 0)
                    {
                        _with31.CommandText = objWK.MyUserName + ".TRANSPORT_TRN_FD_PKG.TRANSPORT_TRN_FD_UPD";
                        _with31.Parameters.Add("TRANSPORT_TRN_FD_PK_IN", ri["TRANSPORT_TRN_FD_PK"]).Direction = ParameterDirection.Input;
                        _with31.Parameters.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                        _with31.Parameters.Add("VERSION_NO_IN", ri["VERSION_NO"]).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with31.CommandText = objWK.MyUserName + ".TRANSPORT_TRN_FD_PKG.TRANSPORT_TRN_FD_INS";
                        _with31.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                    }
                    _with31.Parameters.Add("TRANSPORT_INST_FK_IN", TpNote_Pk).Direction = ParameterDirection.Input;
                    _with31.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", ri["CHARGE_PK"]).Direction = ParameterDirection.Input;
                    _with31.Parameters.Add("FREIGHT_TYPE_IN", ri["FREIGHT_TYPE"]).Direction = ParameterDirection.Input;
                    _with31.Parameters.Add("LOCATION_MST_FK_IN", ri["LOCATION_MST_FK"]).Direction = ParameterDirection.Input;
                    _with31.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", ri["FRTPAYER_CUST_MST_FK"]).Direction = ParameterDirection.Input;
                    _with31.Parameters.Add("FREIGHT_AMT_IN", ri["FREIGHT_AMT"]).Direction = ParameterDirection.Input;
                    _with31.Parameters.Add("CURRENCY_MST_FK_IN", ri["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                    _with31.Parameters.Add("BASIS_FK_IN", getDefault(ri["BASIS_PK"], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with31.Parameters.Add("EXCHANGE_RATE_IN", getDefault(ri["ROE"], 1)).Direction = ParameterDirection.Input;
                    _with31.Parameters.Add("RATEPERBASIS_IN", getDefault(ri["RATEPERBASIS"], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with31.Parameters.Add("QUANTITY_IN", getDefault(ri["VOLUME"], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with31.Parameters.Add("SERVICE_MST_FK_IN", ri["SERVICE_MST_PK"]).Direction = ParameterDirection.Input;
                    _with31.Parameters.Add("INV_AGENT_TRN_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with31.Parameters.Add("CONSOL_INVOICE_TRN_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with31.Parameters.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;
                    _with31.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;

                    try
                    {
                        _with31.ExecuteNonQuery();
                        if (Frt_Pk == 0)
                        {
                            Frt_Pk = Convert.ToInt32(_with31.Parameters["RETURN_VALUE"].Value);
                            ri["TRANSPORT_TRN_FD_PK"] = Frt_Pk;
                        }
                    }
                    catch (Exception ex)
                    {
                        return false;
                    }
                }
            }
            //----------------------------------Expense Charge Details----------------------------------
            if ((dsExpenseChargeDetails != null))
            {
                foreach (DataRow re in dsExpenseChargeDetails.Tables[1].Rows)
                {
                    int Cost_Pk = 0;
                    try
                    {
                        Cost_Pk = Convert.ToInt32(re["TRANSPORT_TRN_COST_PK"]);
                    }
                    catch (Exception ex)
                    {
                        Cost_Pk = 0;
                    }
                    var _with32 = objWK.MyCommand;
                    _with32.Parameters.Clear();
                    _with32.Transaction = TRAN;
                    _with32.CommandType = CommandType.StoredProcedure;
                    if (Cost_Pk > 0)
                    {
                        _with32.CommandText = objWK.MyUserName + ".TRANSPORT_TRN_COST_PKG.TRANSPORT_TRN_COST_UPD";
                        _with32.Parameters.Add("TRANSPORT_TRN_COST_PK_IN", re["TRANSPORT_TRN_COST_PK"]).Direction = ParameterDirection.Input;
                        _with32.Parameters.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                        _with32.Parameters.Add("VERSION_NO_IN", re["VERSION_NO"]).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with32.CommandText = objWK.MyUserName + ".TRANSPORT_TRN_COST_PKG.TRANSPORT_TRN_COST_INS";
                        _with32.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                    }

                    _with32.Parameters.Add("TRANSPORT_INST_FK_IN", TpNote_Pk).Direction = ParameterDirection.Input;
                    _with32.Parameters.Add("VENDOR_MST_FK_IN", re["SUPPLIER_MST_PK"]).Direction = ParameterDirection.Input;
                    _with32.Parameters.Add("COST_ELEMENT_MST_FK_IN", re["COST_ELEMENT_MST_PK"]).Direction = ParameterDirection.Input;
                    _with32.Parameters.Add("LOCATION_MST_FK_IN", re["LOCATION_MST_FK"]).Direction = ParameterDirection.Input;
                    _with32.Parameters.Add("FREIGHT_TYPE_IN", re["PTMT_TYPE"]).Direction = ParameterDirection.Input;
                    _with32.Parameters.Add("CURRENCY_MST_FK_IN", re["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                    _with32.Parameters.Add("ESTIMATED_COST_IN", re["ESTIMATED_COST"]).Direction = ParameterDirection.Input;
                    _with32.Parameters.Add("TOTAL_COST_IN", re["TOTAL_COST"]).Direction = ParameterDirection.Input;
                    _with32.Parameters.Add("BASIS_FK_IN", re["DD_VALUE"]).Direction = ParameterDirection.Input;
                    _with32.Parameters.Add("RATEPERBASIS_IN", re["RATEPERBASIS"]).Direction = ParameterDirection.Input;
                    _with32.Parameters.Add("QUANTITY_IN", getDefault(re["VOLUME"], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with32.Parameters.Add("EXCHANGE_RATE_IN", getDefault(re["ROE"], 1)).Direction = ParameterDirection.Input;
                    _with32.Parameters.Add("EXT_INT_FLAG_IN", getDefault(re["EXT_INT_FLAG"], 2)).Direction = ParameterDirection.Input;
                    _with32.Parameters.Add("SERVICE_MST_FK_IN", re["SERVICE_MST_FK"]).Direction = ParameterDirection.Input;
                    _with32.Parameters.Add("INV_SUPPLIER_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with32.Parameters.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;
                    _with32.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;
                    try
                    {
                        _with32.ExecuteNonQuery();
                        if (Cost_Pk == 0)
                        {
                            Cost_Pk = Convert.ToInt32(_with32.Parameters["RETURN_VALUE"].Value);
                            re["TRANSPORT_TRN_COST_PK"] = Cost_Pk;
                        }
                    }
                    catch (Exception ex)
                    {
                        return false;
                    }
                }
            }
            ClearRemovedServices(objWK, TRAN, TpNote_Pk, dsIncomeChargeDetails, dsExpenseChargeDetails);
            return true;
        }
        public bool ClearRemovedServices(WorkFlow objWK, OracleTransaction TRAN, int TpNote_Pk, DataSet dsIncomeChargeDetails, DataSet dsExpenseChargeDetails)
        {
            string SelectedFrtPks = "";
            string SelectedCostPks = "";
            if (TpNote_Pk > 0)
            {
                try
                {
                    foreach (DataRow ri in dsIncomeChargeDetails.Tables[1].Rows)
                    {
                        if (string.IsNullOrEmpty(SelectedFrtPks))
                        {
                            SelectedFrtPks = Convert.ToString(getDefault(ri["TRANSPORT_TRN_FD_PK"].ToString(), 0));
                        }
                        else
                        {
                            SelectedFrtPks += "," + getDefault(ri["TRANSPORT_TRN_FD_PK"], 0);
                        }
                    }
                    foreach (DataRow re in dsExpenseChargeDetails.Tables[1].Rows)
                    {
                        if (string.IsNullOrEmpty(SelectedCostPks))
                        {
                            SelectedCostPks = Convert.ToString(getDefault(re["TRANSPORT_TRN_COST_PK"].ToString(), 0));
                        }
                        else
                        {
                            SelectedCostPks += "," + getDefault(re["TRANSPORT_TRN_COST_PK"], 0);
                        }
                    }

                    var _with33 = objWK.MyCommand;
                    _with33.Transaction = TRAN;
                    _with33.CommandType = CommandType.StoredProcedure;
                    _with33.CommandText = objWK.MyUserName + ".TRANSPORT_TRN_FD_PKG.DELETE_SEC_CHG_EXCEPT";
                    _with33.Parameters.Clear();
                    _with33.Parameters.Add("TRANSPORT_INST_SEA_PK_IN", TpNote_Pk).Direction = ParameterDirection.Input;
                    _with33.Parameters.Add("TRANSPORT_TRN_FD_FKS_IN", (string.IsNullOrEmpty(SelectedFrtPks) ? "" : SelectedFrtPks)).Direction = ParameterDirection.Input;
                    _with33.Parameters.Add("TRANSPORT_TRN_COST_FKS_IN", (string.IsNullOrEmpty(SelectedCostPks) ? "" : SelectedCostPks)).Direction = ParameterDirection.Input;
                    _with33.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
                    _with33.ExecuteNonQuery();
                }
                catch (OracleException oraexp)
                {
                    TRAN.Rollback();
                }
                catch (Exception ex)
                {
                    TRAN.Rollback();

                }
                finally
                {
                }
            }
            return false;
        }
        #endregion

        #region "Fetch JC Details"
        public DataSet FetchJC(int TPNOTEPK = 0)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT TP.JOB_CARD_FK,");
            sb.Append("       TP.JC_TYPE,");
            sb.Append("       TP.TP_CBJC_JC,");
            sb.Append("       (CASE");
            sb.Append("         WHEN TP.TP_CBJC_JC = 1 THEN");
            sb.Append("          (SELECT ROWTOCOL('SELECT C.CBJC_NO FROM CBJC_TBL C WHERE C.CBJC_PK IN (' ||");
            sb.Append("                           NVL(TP.JOB_CARD_FK, -1) || ')')");
            sb.Append("             FROM DUAL)");
            sb.Append("         ELSE");
            sb.Append("          (SELECT ROWTOCOL('SELECT C.JOBCARD_REF_NO FROM JOB_CARD_TRN C WHERE C.JOB_CARD_TRN_PK IN (' ||");
            sb.Append("                           NVL(TP.JOB_CARD_FK, -1) || ')')");
            sb.Append("             FROM DUAL)");
            sb.Append("       END) REF_NR");
            sb.Append("  FROM TRANSPORT_INST_SEA_TBL TP");
            sb.Append(" WHERE TP.TRANSPORT_INST_SEA_PK =" + TPNOTEPK);
            try
            {
                return ObjWk.GetDataSet(sb.ToString());
                //Manjunath  PTS ID:Sep-02  26/09/2011
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
        #endregion

        #region "Fetch Doc Details"
        public DataSet FetchDocDetails(Int16 Tp_Trn_Truck_Fk = 0, int IsEdit = 0, Int16 BizType = 0, int ProcessType = 0, Int16 TransporterPK = 0, string TruckNr = "", int TpNote_PK = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                var _with34 = objWF.MyCommand.Parameters;
                _with34.Add("TRANSPORT_TRN_TRUCK_FK_IN", Tp_Trn_Truck_Fk).Direction = ParameterDirection.Input;
                _with34.Add("IS_EDIT_IN", IsEdit).Direction = ParameterDirection.Input;
                _with34.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with34.Add("PROCESS_TYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with34.Add("TRUCKNR_IN", TruckNr).Direction = ParameterDirection.Input;
                _with34.Add("TRANSPORTER_PK_IN", TransporterPK).Direction = ParameterDirection.Input;
                _with34.Add("TPNOTE_PK_IN", getDefault(TpNote_PK, DBNull.Value)).Direction = ParameterDirection.Input;
                _with34.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("TRANSPORT_TRN_DOC_PKG", "FETCH_TRANSPORT_DOC_DT");
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
        #endregion

        #region "Fetch Cont Details"
        public DataSet FetchContDetails(Int16 Tp_Trn_Truck_Fk = 0, string CtrNr = "", Int16 BizType = 0, Int16 ProcessType = 0, Int16 CargoType = 0, Int16 TransporterPK = 0, string TruckNr = "", int TpNote_PK = 0)
        {

            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                var _with35 = objWF.MyCommand.Parameters;
                _with35.Add("TRANSPORT_TRN_TRUCK_FK_IN", Tp_Trn_Truck_Fk).Direction = ParameterDirection.Input;
                _with35.Add("CTR_NR_IN", getDefault(CtrNr, DBNull.Value)).Direction = ParameterDirection.Input;
                _with35.Add("BIZ_TYPE_IN", getDefault(BizType, DBNull.Value)).Direction = ParameterDirection.Input;
                _with35.Add("PROCESS_TYPE_IN", getDefault(ProcessType, DBNull.Value)).Direction = ParameterDirection.Input;
                _with35.Add("CARGO_TYPE_IN", getDefault(CargoType, DBNull.Value)).Direction = ParameterDirection.Input;
                _with35.Add("TRANSPORTER_PK_IN", getDefault(TransporterPK, DBNull.Value)).Direction = ParameterDirection.Input;
                _with35.Add("TRUCKNR_IN", getDefault(TruckNr, DBNull.Value)).Direction = ParameterDirection.Input;
                _with35.Add("TPNOTE_PK_IN", getDefault(TpNote_PK, DBNull.Value)).Direction = ParameterDirection.Input;
                _with35.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("TRANSPORT_TRN_DOC_PKG", "FETCH_TRANSPORT_CONT_DT");
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
        #endregion

        #region "Fetch Income and Expense Details"
        public DataSet FetchSecSerIncomeDetails(long TPNOTEPK, int CurFK = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataTable dtTotalAmt = new DataTable();
            DataTable dtChargeDet = new DataTable();
            DataSet dsIncomeDet = new DataSet();
            int CurrencyPK = 0;
            if (CurFK > 0)
            {
                CurrencyPK = CurFK;
            }
            else
            {
                CurrencyPK = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
            }
            try
            {
                var _with36 = objWF.MyCommand.Parameters;
                _with36.Clear();
                _with36.Add("TRANSPORT_INST_SEA_PK_IN", TPNOTEPK).Direction = ParameterDirection.Input;
                _with36.Add("BASE_CURRENCY_FK_IN", CurrencyPK).Direction = ParameterDirection.Input;
                _with36.Add("SEC_SRVC_CURSOR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dtTotalAmt = objWF.GetDataTable("TRANSPORT_TRN_FD_PKG", "INCOME_MAIN_SEA_EXP");
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }

            //Child Details
            try
            {
                var _with37 = objWF.MyCommand.Parameters;
                _with37.Clear();
                _with37.Add("TRANSPORT_INST_SEA_PK_IN", TPNOTEPK).Direction = ParameterDirection.Input;
                _with37.Add("BASE_CURRENCY_FK_IN", CurrencyPK).Direction = ParameterDirection.Input;
                _with37.Add("SEC_SRVC_CURSOR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dtChargeDet = objWF.GetDataTable("TRANSPORT_TRN_FD_PKG", "INCOME_CHILD_SEA_EXP");
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            try
            {
                dsIncomeDet.Tables.Add(dtTotalAmt);
                dsIncomeDet.Tables.Add(dtChargeDet);
                dsIncomeDet.Tables[0].TableName = "TOTAL_AMOUNT";
                dsIncomeDet.Tables[1].TableName = "CHARGE_DETAILS";
                var rel_TotAmtAndCharge = new DataRelation("rel1", dsIncomeDet.Tables["TOTAL_AMOUNT"].Columns["FLAG"], dsIncomeDet.Tables["CHARGE_DETAILS"].Columns["FLAG"]);
                dsIncomeDet.Relations.Add(rel_TotAmtAndCharge);
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
            return dsIncomeDet;
        }
        public DataSet FetchSecSerExpenseDetails(long TPNOTEPK, int CurFK = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataTable dtTotalAmt = new DataTable();
            DataTable dtChargeDet = new DataTable();
            DataSet dsExpenseDet = new DataSet();
            int CurrencyPK = 0;
            if (CurFK > 0)
            {
                CurrencyPK = CurFK;
            }
            else
            {
                CurrencyPK = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
            }
            try
            {
                var _with38 = objWF.MyCommand.Parameters;
                _with38.Add("TRANSPORT_INST_SEA_PK_IN", TPNOTEPK).Direction = ParameterDirection.Input;
                _with38.Add("BASE_CURRENCY_FK_IN", CurrencyPK).Direction = ParameterDirection.Input;
                _with38.Add("SEC_SRVC_CURSOR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dtTotalAmt = objWF.GetDataTable("TRANSPORT_TRN_FD_PKG", "EXPENSE_MAIN_SEA_EXP");
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }

            //Child Details
            try
            {
                var _with39 = objWF.MyCommand.Parameters;
                _with39.Clear();
                _with39.Add("TRANSPORT_INST_SEA_PK_IN", TPNOTEPK).Direction = ParameterDirection.Input;
                _with39.Add("BASE_CURRENCY_FK_IN", CurrencyPK).Direction = ParameterDirection.Input;
                _with39.Add("SEC_SRVC_CURSOR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dtChargeDet = objWF.GetDataTable("TRANSPORT_TRN_FD_PKG", "EXPENSE_CHILD_SEA_EXP");

            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }

            try
            {
                dsExpenseDet.Tables.Add(dtTotalAmt);
                dsExpenseDet.Tables.Add(dtChargeDet);
                dsExpenseDet.Tables[0].TableName = "TOTAL_AMOUNT";
                dsExpenseDet.Tables[1].TableName = "CHARGE_DETAILS";
                var rel_TotAmtAndCharge = new DataRelation("rel1", dsExpenseDet.Tables["TOTAL_AMOUNT"].Columns["FLAG"], dsExpenseDet.Tables["CHARGE_DETAILS"].Columns["FLAG"]);
                dsExpenseDet.Relations.Add(rel_TotAmtAndCharge);
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return dsExpenseDet;
        }
        #endregion

        #region "Fetch DocName"
        public DataSet FetchDocName()
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder StrSqlBuilder = new StringBuilder();
            StrSqlBuilder.Append("  SELECT Q.DROPDOWN_PK, Q.DD_ID ");
            StrSqlBuilder.Append("  FROM QFOR_DROP_DOWN_TBL Q ");
            StrSqlBuilder.Append("  WHERE Q.DD_FLAG = 'DOC_NAME'");
            StrSqlBuilder.Append("  AND Q.CONFIG_ID = 'QFOR4398'");
            try
            {
                return ObjWk.GetDataSet(StrSqlBuilder.ToString());
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
        #endregion

        #region "Fetch Damage Desc"
        public DataSet FetchDamage()
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder StrSqlBuilder = new StringBuilder();
            StrSqlBuilder.Append("  SELECT Q.DROPDOWN_PK, Q.DD_ID ");
            StrSqlBuilder.Append("  FROM QFOR_DROP_DOWN_TBL Q ");
            StrSqlBuilder.Append("  WHERE Q.DD_FLAG = 'DAMAGE_DESC'");
            StrSqlBuilder.Append("  AND Q.CONFIG_ID = 'QFOR4398'");
            try
            {
                return ObjWk.GetDataSet(StrSqlBuilder.ToString());
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
        #endregion

        #region "Truck Count"
        public int FetchTruckCount(int TpNote_PK = 0, Int16 TransporterPK = 0, string TruckNr = "")
        {
            WorkFlow ObjWk = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT DISTINCT COUNT(DISTINCT TP_TRUCK.TRANSPORT_TRN_TRUCK_PK) TP_COUNT");
            sb.Append("  FROM TRANSPORT_TRN_DOC   TP_DOC,");
            sb.Append("       TRANSPORT_TRN_CONT  TP_CONT,");
            sb.Append("       TRANSPORT_TRN_TRUCK TP_TRUCK");
            sb.Append(" WHERE TP_CONT.TRANSPORT_TRN_CONT_PK = TP_TRUCK.TRANSPORT_TRN_CONT_FK");
            sb.Append("   AND TP_DOC.TRANSPORT_TRN_TRUCK_FK = TP_TRUCK.TRANSPORT_TRN_TRUCK_PK");
            sb.Append("   AND TP_TRUCK.TRANSPORTER_MST_FK= " + TransporterPK);
            if (!string.IsNullOrEmpty(TruckNr))
            {
                sb.Append("   AND TP_TRUCK.TD_TRUCK_NUMBER = '" + TruckNr + "' ");
            }
            if (TpNote_PK > 0)
            {
                sb.Append("   AND TP_CONT.TRANSPORT_INST_FK = " + TpNote_PK);
            }
            try
            {
                return Convert.ToInt32(ObjWk.ExecuteScaler(sb.ToString()));
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
        #endregion

        #region "GetRevenueDetails"

        public DataSet GetRevenueDetails(decimal actualCost, decimal actualRevenue, decimal estimatedCost, decimal estimatedRevenue, int TpNotePK, int LocationPK = 0)
        {

            WorkFlow objWF = new WorkFlow();
            try
            {
                DataSet DS = new DataSet();
                var _with40 = objWF.MyCommand.Parameters;
                _with40.Add("TRANSPORT_INST_SEA_PK_IN", TpNotePK).Direction = ParameterDirection.Input;
                _with40.Add("CURRPK", HttpContext.Current.Session["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
                _with40.Add("CBJC_REV", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("TRANSPORT_INST_SEA_TBL_PKG", "FETCH_TRANSPORT_REVENUE");
                return DS;
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
        #endregion

        #region "Get Collection Address"
        public string FetchCollAdd(string ShipperPk)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder StrSqlBuilder = new StringBuilder();
            StrSqlBuilder.Append("  SELECT CMT.COL_ADDRESS");
            StrSqlBuilder.Append("   FROM CUSTOMER_MST_TBL CMT ");
            if (!string.IsNullOrEmpty(ShipperPk))
            {
                StrSqlBuilder.Append("  WHERE CMT.CUSTOMER_MST_PK =" + ShipperPk);
            }
            try
            {
                return Convert.ToString(ObjWk.ExecuteScaler(StrSqlBuilder.ToString()));
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
        public DataSet FetchCollAddforJobCard(string ShipperPk)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder StrSqlBuilder = new StringBuilder();
            StrSqlBuilder.Append("  SELECT CMT.CUSTOMER_NAME, CMT.COL_ADDRESS");
            StrSqlBuilder.Append("   FROM CUSTOMER_MST_TBL CMT ");
            if (!string.IsNullOrEmpty(ShipperPk))
            {
                StrSqlBuilder.Append("  WHERE CMT.CUSTOMER_MST_PK =" + ShipperPk);
            }
            try
            {
                return ObjWk.GetDataSet(StrSqlBuilder.ToString());
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
        #endregion

        #region "Get Collection Address"
        public string FetchDelAdd(string ConsigneePk)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder StrSqlBuilder = new StringBuilder();
            StrSqlBuilder.Append("  SELECT CMT.DEL_ADDRESS");
            StrSqlBuilder.Append("   FROM CUSTOMER_MST_TBL CMT ");
            if (!string.IsNullOrEmpty(ConsigneePk))
            {
                StrSqlBuilder.Append("  WHERE CMT.CUSTOMER_MST_PK =" + ConsigneePk);
            }
            try
            {
                return Convert.ToString(ObjWk.ExecuteScaler(StrSqlBuilder.ToString()));
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
        public DataSet FetchDelAddforJobCard(string ConsigneePk)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder StrSqlBuilder = new StringBuilder();
            StrSqlBuilder.Append("  SELECT CMT.CUSTOMER_NAME, CMT.DEL_ADDRESS");
            StrSqlBuilder.Append("   FROM CUSTOMER_MST_TBL CMT ");
            if (!string.IsNullOrEmpty(ConsigneePk))
            {
                StrSqlBuilder.Append("  WHERE CMT.CUSTOMER_MST_PK =" + ConsigneePk);
            }
            try
            {
                return ObjWk.GetDataSet(StrSqlBuilder.ToString());
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
        #endregion

        #region "Get Collection Address"
        public DataSet FetchPickupDropAddressDtl(string JobPK, int BizType, int ProcessType, int AddrType)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            try
            {
                var _with41 = objWF.MyCommand.Parameters;
                _with41.Add("JOB_PK_IN", JobPK).Direction = ParameterDirection.Input;
                _with41.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with41.Add("PROCESS_TYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with41.Add("ADDRESS_TYPE_IN", AddrType).Direction = ParameterDirection.Input;
                _with41.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                return objWF.GetDataSet("TRANSPORT_TRN_CONT_PKG", "FETCH_PICKUPDROP_DETAILS");
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
        #endregion

        #region "Get Truck Main Details"
        public DataSet FetchTruckMainDetails(int TPNOTEPK = 0, string CargoType = "0", int BizType = 0, int ProcessType = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                var _with42 = objWF.MyCommand.Parameters;
                _with42.Add("TPNOTE_FK_IN", TPNOTEPK).Direction = ParameterDirection.Input;
                _with42.Add("CARGO_TYPE_IN", CargoType).Direction = ParameterDirection.Input;
                _with42.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with42.Add("PROCESS_TYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with42.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("TRANSPORT_TRN_CONT_PKG", "FETCH_TRUCK_MAIN_DETAILS");
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
        public string FetchJobcardPK(string CBJCPK)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder StrSqlBuilder = new StringBuilder();
            StrSqlBuilder.Append("  SELECT C.JC_FK FROM CBJC_TBL C ");
            StrSqlBuilder.Append("  WHERE C.CBJC_PK =" + CBJCPK);
            try
            {
                return ObjWk.ExecuteScaler(StrSqlBuilder.ToString());
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
        #endregion

        #region "Fetch Onward Return"
        public DataSet FetchOnwardReturn()
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder StrSqlBuilder = new StringBuilder();
            StrSqlBuilder.Append("  SELECT Q.DD_VALUE, Q.DD_ID ");
            StrSqlBuilder.Append("  FROM QFOR_DROP_DOWN_TBL Q ");
            StrSqlBuilder.Append("  WHERE Q.DD_FLAG = 'ONWARD_RETURN'");
            StrSqlBuilder.Append("  AND Q.CONFIG_ID = 'QFOR4398'");
            try
            {
                return ObjWk.GetDataSet(StrSqlBuilder.ToString());
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
        public DataSet FetchBorder()
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder StrSqlBuilder = new StringBuilder();
            StrSqlBuilder.Append("  SELECT Q.DD_VALUE, Q.DD_ID ");
            StrSqlBuilder.Append("  FROM QFOR_DROP_DOWN_TBL Q ");
            StrSqlBuilder.Append("  WHERE Q.DD_FLAG = 'BORDER'");
            StrSqlBuilder.Append("  AND Q.CONFIG_ID = 'QFOR4398'");
            try
            {
                return ObjWk.GetDataSet(StrSqlBuilder.ToString());
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
        #endregion

        #region "Get Truck POPUP Details"
        public DataSet FetchTruckPopUpDetails(int TPNOTEPK = 0, string CargoType = "0", int BizType = 0, int ProcessType = 0, int TransporterPK = 0, string TruckNr = "")
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                var _with43 = objWF.MyCommand.Parameters;
                _with43.Add("TPNOTE_FK_IN", TPNOTEPK).Direction = ParameterDirection.Input;
                _with43.Add("CARGO_TYPE_IN", CargoType).Direction = ParameterDirection.Input;
                _with43.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with43.Add("PROCESS_TYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with43.Add("TRUCKNR_IN", TruckNr).Direction = ParameterDirection.Input;
                _with43.Add("TRANSPORTER_PK_IN", TransporterPK).Direction = ParameterDirection.Input;
                _with43.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("TRANSPORT_TRN_CONT_PKG", "FETCH_TRUCK_POPUP_DETAILS");
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
        #endregion

        #region "Get Truck Document Details"
        public DataTable FetchTruckDocumentDetails(int TPNOTEPK = 0, string CargoType = "0", int BizType = 0, int ProcessType = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                var _with44 = objWF.MyCommand.Parameters;
                _with44.Add("DOC_PK_IN", TPNOTEPK).Direction = ParameterDirection.Input;
                _with44.Add("TRUCK_FK_IN", TPNOTEPK).Direction = ParameterDirection.Input;
                _with44.Add("TPNOTE_FK_IN", TPNOTEPK).Direction = ParameterDirection.Input;
                _with44.Add("CARGO_TYPE_IN", CargoType).Direction = ParameterDirection.Input;
                _with44.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with44.Add("PROCESS_TYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with44.Add("TRUCKNR_IN", CargoType).Direction = ParameterDirection.Input;
                _with44.Add("TRANSPORTER_PK_IN", CargoType).Direction = ParameterDirection.Input;
                _with44.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("TRANSPORT_TRN_CONT_PKG", "FETCH_TRUCK_DOC_DETAILS");
                return DS.Tables[0];
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
        #endregion

        #region "Get Truck Track Details"
        public DataSet FetchTruckTrackDetails(int TPNOTEPK = 0, string CargoType = "0", int BizType = 0, int ProcessType = 0, Int16 TransporterPK = 0, string TruckNr = "")
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                var _with45 = objWF.MyCommand.Parameters;
                _with45.Add("TRACK_PK_IN", TPNOTEPK).Direction = ParameterDirection.Input;
                _with45.Add("TRUCK_FK_IN", TPNOTEPK).Direction = ParameterDirection.Input;
                _with45.Add("TPNOTE_FK_IN", TPNOTEPK).Direction = ParameterDirection.Input;
                _with45.Add("CARGO_TYPE_IN", CargoType).Direction = ParameterDirection.Input;
                _with45.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with45.Add("PROCESS_TYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with45.Add("TRUCKNR_IN", TruckNr).Direction = ParameterDirection.Input;
                _with45.Add("TRANSPORTER_PK_IN", TransporterPK).Direction = ParameterDirection.Input;
                _with45.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("TRANSPORT_TRN_CONT_PKG", "FETCH_TRUCK_TRACK_DETAILS");
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
        #endregion

        #region "For Fetching DropDown Values From DataBase"
        public static DataSet FetchOwnerDropDown(string Flag = "", string ConfigID = "")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string ErrorMessage = null;
            sb.Append("SELECT T.DD_VALUE CONTAINER_OWNER_TYPE_FK, T.DD_ID");
            sb.Append("  FROM QFOR_DROP_DOWN_TBL T");
            sb.Append(" WHERE T.DD_FLAG = '" + Flag + "'");
            sb.Append(" AND T.CONFIG_ID  = '" + ConfigID + "'");
            sb.Append("    ORDER BY T.DD_VALUE ");
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
        #endregion
    }
}