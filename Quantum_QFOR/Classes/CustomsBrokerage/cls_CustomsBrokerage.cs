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
    /// <summary>
    ///
    /// </summary>
    public class cls_CustomsBrokerage : CommonFeatures
    {

        cls_SeaBookingEntry objVesselVoyage = new cls_SeaBookingEntry();
        #region "Get Container Details"
        public DataSet FetchMainCBJCDetails(int CBJCPK = 0, int JOBPK = 0, string JcType = "", int BizType = 0, int ProcessType = 0, bool IsEdit = false)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                //If IsEdit = True Then
                //    JOBPK = 0
                //    JcType = 1
                //End If
                var _with1 = objWF.MyCommand.Parameters;
                _with1.Add("CBJC_PK_IN", CBJCPK).Direction = ParameterDirection.Input;
                _with1.Add("JOBPK_IN", JOBPK).Direction = ParameterDirection.Input;
                _with1.Add("JCTYPE_IN", JcType).Direction = ParameterDirection.Input;
                _with1.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with1.Add("PROCESS_TYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with1.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("CBJC_TBL_PKG", "FETCH_CBJC_DETAILS");
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
        public DataSet FetchContainerDetails(int CBJCPK = 0, int JOBPK = 0, string JcType = "", string CargoType = "0", int BizType = 0, int ProcessType = 0, bool IsEdit = false)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                //If IsEdit = True Then
                //    JOBPK = 0
                //    JcType = 1
                //End If
                var _with2 = objWF.MyCommand.Parameters;
                _with2.Add("CBJC_FK_IN", CBJCPK).Direction = ParameterDirection.Input;
                _with2.Add("JOBPK_IN", JOBPK).Direction = ParameterDirection.Input;
                _with2.Add("JCTYPE_IN", JcType).Direction = ParameterDirection.Input;
                _with2.Add("CARGO_TYPE_IN", CargoType).Direction = ParameterDirection.Input;
                _with2.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with2.Add("PROCESS_TYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with2.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("CBJC_TRN_CONT_PKG", "FETCH_CBJC_CONT_DT");
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
        public DataSet FetchDocDetails(int CBJCPK = 0, int JOBPK = 0, int BizType = 0, int ProcessType = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                var _with3 = objWF.MyCommand.Parameters;
                _with3.Add("CBJC_FK_IN", CBJCPK).Direction = ParameterDirection.Input;
                _with3.Add("JOBPK_IN", JOBPK).Direction = ParameterDirection.Input;
                _with3.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with3.Add("PROCESS_TYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with3.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("CBJC_TRN_DOC_PKG", "FETCH_CBJC_DOC_DT");
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

        #region "Save Function"
        public ArrayList Save(ref DataSet M_DataSet, ref DataSet dsContainerData, ref long CBJCPK, ref bool isEdting, DataSet dsIncomeChargeDetails = null, DataSet dsExpenseChargeDetails = null, DataSet dsDocDetails = null, int Biztype = 0, DataSet dsCC = null, string CbjcRefNr = "",
        string sid = "", string polid = "", string podid = "")
        {

            objVesselVoyage.ConfigurationPK = M_Configuration_PK;
            objVesselVoyage.CREATED_BY = M_CREATED_BY_FK;
            int strVoyagepk = Convert.ToInt32(getDefault(M_DataSet.Tables[0].Rows[0]["VOYAGEPK"], 0));
            string CBJCRefNNumber = null;
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

            OracleCommand insDocDetails = new OracleCommand();
            OracleCommand updDocDetails = new OracleCommand();
            OracleCommand delDocDetails = new OracleCommand();

            //'ADDED BY MAYUR FOR CUSTOMS CLEARANCE
            OracleCommand insCustomsClearanceDetails = new OracleCommand();
            OracleCommand updCustomsClearanceDetails = new OracleCommand();
            OracleCommand delCustomsClearanceDetails = new OracleCommand();
            //'ENDED BY MAYUR FOR CUSTOMS CLEARANCE

            OracleCommand insIncomeChargeDetails = new OracleCommand();
            OracleCommand updIncomeChargeDetails = new OracleCommand();
            OracleCommand delIncomeChargeDetails = new OracleCommand();

            OracleCommand insExpenseChargeDetails = new OracleCommand();
            OracleCommand updExpenseChargeDetails = new OracleCommand();
            OracleCommand delExpenseChargeDetails = new OracleCommand();
            if (isEdting == false)
            {
                CBJCRefNNumber = GenerateProtocolKey("CUSTOM BROKERAGE JC", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), DateTime.Now, "", "", polid, M_LAST_MODIFIED_BY_FK, new WorkFlow() , sid, podid);
            }
            else
            {
                CBJCRefNNumber = CbjcRefNr;
            }
            try
            {
                var _with4 = insCommand;
                _with4.Connection = objWK.MyConnection;
                _with4.CommandType = CommandType.StoredProcedure;
                _with4.CommandText = objWK.MyUserName + ".CBJC_TBL_PKG.CBJC_TBL_INS";
                var _with5 = _with4.Parameters;

                insCommand.Parameters.Add("CBJC_NO_IN", CBJCRefNNumber).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("CBJC_DATE_IN", OracleDbType.Date, 20, "CBJC_DATE").Direction = ParameterDirection.Input;
                insCommand.Parameters["CBJC_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("BIZ_TYPE_IN", OracleDbType.Int32, 1, "BIZ_TYPE").Direction = ParameterDirection.Input;
                insCommand.Parameters["BIZ_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("PROCESS_TYPE_IN", OracleDbType.Int32, 1, "PROCESS_TYPE").Direction = ParameterDirection.Input;
                insCommand.Parameters["PROCESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CUST_CAT_IN", OracleDbType.Int32, 1, "CUSTOMER_CATEGORY").Direction = ParameterDirection.Input;
                insCommand.Parameters["CUST_CAT_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("IE_IN", OracleDbType.Int32, 1, "IE").Direction = ParameterDirection.Input;
                insCommand.Parameters["IE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("JC_FK_IN", OracleDbType.Int32, 10, "JC_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["JC_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("MBL_NO_IN", OracleDbType.Varchar2, 50, "MBL_NO").Direction = ParameterDirection.Input;
                insCommand.Parameters["MBL_NO_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("MBL_DATE_IN", OracleDbType.Date, 20, "MBL_DATE").Direction = ParameterDirection.Input;
                insCommand.Parameters["MBL_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("HBL_NO_IN", OracleDbType.Varchar2, 50, "HBL_NO").Direction = ParameterDirection.Input;
                insCommand.Parameters["HBL_NO_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("HBL_DATE_IN", OracleDbType.Date, 20, "HBL_DATE").Direction = ParameterDirection.Input;
                insCommand.Parameters["HBL_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CARGO_TYPE_IN", OracleDbType.Int32, 1, "CARGO_TYPE").Direction = ParameterDirection.Input;
                insCommand.Parameters["CARGO_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("COMM_GRP_FK_IN", OracleDbType.Int32, 10, "COMM_GRP_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["COMM_GRP_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("OPERATOR_MST_FK_IN", OracleDbType.Int32, 10, "CARRIER_MST_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["OPERATOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("VOYAGE_TRN_FK_IN", getDefault(strVoyagepk, DBNull.Value)).Direction = ParameterDirection.Input;
                insCommand.Parameters["VOYAGE_TRN_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("FLIGHT_NO_IN", OracleDbType.Varchar2, 50, "FLIGHT_NO").Direction = ParameterDirection.Input;
                insCommand.Parameters["FLIGHT_NO_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("PLR_MST_FK_IN", OracleDbType.Int32, 10, "COL_PLACE_MST_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["PLR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("POL_MST_FK_IN", OracleDbType.Int32, 10, "POL_MST_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["POL_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("POD_MST_FK_IN", OracleDbType.Int32, 10, "POD_MST_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["POD_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("PFD_MST_FK_IN", OracleDbType.Int32, 10, "DEL_PLACE_MST_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["PFD_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["ETD_DATE"].ToString()))
                {
                    insCommand.Parameters.Add("ETD_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                else
                {
                    insCommand.Parameters.Add("ETD_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["ETD_DATE"])).Direction = ParameterDirection.Input;
                }
                insCommand.Parameters["ETD_IN"].SourceVersion = DataRowVersion.Current;
                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["departure_date"].ToString()))
                {
                    insCommand.Parameters.Add("ATD_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                else
                {
                    insCommand.Parameters.Add("ATD_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["departure_date"])).Direction = ParameterDirection.Input;
                }
                insCommand.Parameters["ATD_IN"].SourceVersion = DataRowVersion.Current;

                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["ETA_DATE"].ToString()))
                {
                    insCommand.Parameters.Add("ETA_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                else
                {
                    insCommand.Parameters.Add("ETA_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["ETA_DATE"])).Direction = ParameterDirection.Input;
                }
                insCommand.Parameters["ETA_IN"].SourceVersion = DataRowVersion.Current;

                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["arrival_date"].ToString()))
                {
                    insCommand.Parameters.Add("ATA_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                else
                {
                    insCommand.Parameters.Add("ATA_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["arrival_date"])).Direction = ParameterDirection.Input;
                }
                insCommand.Parameters["ATA_IN"].SourceVersion = DataRowVersion.Current;


                insCommand.Parameters.Add("CUSTOMER_MST_FK_IN", OracleDbType.Int32, 10, "CUSTOMER_MST_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["CUSTOMER_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("SHIPPER_MST_FK_IN", OracleDbType.Int32, 10, "SHIPPER_MST_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["SHIPPER_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CONSIGNEE_MST_FK_IN", OracleDbType.Int32, 10, "CONSIGNEE_MST_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["CONSIGNEE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("NOTIFY1_MST_FK_IN", OracleDbType.Int32, 10, "NOTIFY1_MST_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["NOTIFY1_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("NOTIFY2_MST_FK_IN", OracleDbType.Int32, 10, "NOTIFY2_MST_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["NOTIFY2_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("DP_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "DP_AGENT_MST_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["DP_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CL_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "CL_AGENT_MST_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["CL_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CARGO_MOVE_FK_IN", OracleDbType.Int32, 10, "CARGO_MOVE_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["CARGO_MOVE_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("PYMT_TYPE_IN", OracleDbType.Int32, 1, "PYMT_TYPE").Direction = ParameterDirection.Input;
                insCommand.Parameters["PYMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("SHIPPING_TERMS_MST_FK_IN", OracleDbType.Int32, 10, "SHIPPING_TERMS_MST_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["SHIPPING_TERMS_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("LC_SHIPMENT_IN", OracleDbType.Int32, 1, "LC_SHIPMENT").Direction = ParameterDirection.Input;
                insCommand.Parameters["LC_SHIPMENT_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("TRANSPORT_REQ_IN", OracleDbType.Int32, 1, "TRANSPORT_REQ").Direction = ParameterDirection.Input;
                insCommand.Parameters["TRANSPORT_REQ_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("LOCAL_TRANSIT_IN", OracleDbType.Int32, 1, "LOCAL_TRANSIT").Direction = ParameterDirection.Input;
                insCommand.Parameters["LOCAL_TRANSIT_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("JOB_CARD_STATUS_IN", OracleDbType.Int32, 1, "JOB_CARD_STATUS").Direction = ParameterDirection.Input;
                insCommand.Parameters["JOB_CARD_STATUS_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("JOB_CARD_CLOSED_ON_IN", OracleDbType.Date, 20, "JOB_CARD_CLOSED_ON").Direction = ParameterDirection.Input;
                insCommand.Parameters["JOB_CARD_CLOSED_ON_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("REMARKS_IN", OracleDbType.Varchar2, 500, "REMARKS").Direction = ParameterDirection.Input;
                insCommand.Parameters["REMARKS_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CBJC_STATUS_IN", OracleDbType.Int32, 1, "CBJC_STATUS").Direction = ParameterDirection.Input;
                insCommand.Parameters["CBJC_STATUS_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CHA_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "cha_agent_mst_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["CHA_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;


                insCommand.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "CBJC_PK").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;



                var _with6 = updCommand;
                _with6.Connection = objWK.MyConnection;
                _with6.CommandType = CommandType.StoredProcedure;
                _with6.CommandText = objWK.MyUserName + ".CBJC_TBL_PKG.CBJC_TBL_UPD";
                var _with7 = _with6.Parameters;

                updCommand.Parameters.Add("CBJC_PK_IN", OracleDbType.Int32, 10, "CBJC_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["CBJC_PK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CBJC_NO_IN", CBJCRefNNumber).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("CBJC_DATE_IN", OracleDbType.Date, 20, "CBJC_DATE").Direction = ParameterDirection.Input;
                updCommand.Parameters["CBJC_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("BIZ_TYPE_IN", OracleDbType.Int32, 1, "BIZ_TYPE").Direction = ParameterDirection.Input;
                updCommand.Parameters["BIZ_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("PROCESS_TYPE_IN", OracleDbType.Int32, 1, "PROCESS_TYPE").Direction = ParameterDirection.Input;
                updCommand.Parameters["PROCESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CUST_CAT_IN", OracleDbType.Int32, 1, "CUSTOMER_CATEGORY").Direction = ParameterDirection.Input;
                updCommand.Parameters["CUST_CAT_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("IE_IN", OracleDbType.Int32, 1, "IE").Direction = ParameterDirection.Input;
                updCommand.Parameters["IE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("JC_FK_IN", OracleDbType.Int32, 10, "JC_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["JC_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("MBL_NO_IN", OracleDbType.Varchar2, 50, "MBL_NO").Direction = ParameterDirection.Input;
                updCommand.Parameters["MBL_NO_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("MBL_DATE_IN", OracleDbType.Date, 20, "MBL_DATE").Direction = ParameterDirection.Input;
                updCommand.Parameters["MBL_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("HBL_NO_IN", OracleDbType.Varchar2, 50, "HBL_NO").Direction = ParameterDirection.Input;
                updCommand.Parameters["HBL_NO_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("HBL_DATE_IN", OracleDbType.Date, 20, "HBL_DATE").Direction = ParameterDirection.Input;
                updCommand.Parameters["HBL_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CARGO_TYPE_IN", OracleDbType.Int32, 1, "CARGO_TYPE").Direction = ParameterDirection.Input;
                updCommand.Parameters["CARGO_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("COMM_GRP_FK_IN", OracleDbType.Int32, 10, "COMM_GRP_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["COMM_GRP_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("OPERATOR_MST_FK_IN", OracleDbType.Int32, 10, "CARRIER_MST_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["OPERATOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("VOYAGE_TRN_FK_IN", getDefault(strVoyagepk, DBNull.Value)).Direction = ParameterDirection.Input;
                updCommand.Parameters["VOYAGE_TRN_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("FLIGHT_NO_IN", OracleDbType.Varchar2, 50, "FLIGHT_NO").Direction = ParameterDirection.Input;
                updCommand.Parameters["FLIGHT_NO_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("PLR_MST_FK_IN", OracleDbType.Int32, 10, "COL_PLACE_MST_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["PLR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("POL_MST_FK_IN", OracleDbType.Int32, 10, "POL_MST_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["POL_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("POD_MST_FK_IN", OracleDbType.Int32, 10, "POD_MST_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["POD_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("PFD_MST_FK_IN", OracleDbType.Int32, 10, "DEL_PLACE_MST_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["PFD_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["ETD_DATE"].ToString()))
                {
                    updCommand.Parameters.Add("ETD_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                else
                {
                    updCommand.Parameters.Add("ETD_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["ETD_DATE"])).Direction = ParameterDirection.Input;
                }
                updCommand.Parameters["ETD_IN"].SourceVersion = DataRowVersion.Current;
                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["departure_date"].ToString()))
                {
                    updCommand.Parameters.Add("ATD_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                else
                {
                    updCommand.Parameters.Add("ATD_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["departure_date"])).Direction = ParameterDirection.Input;
                }
                updCommand.Parameters["ATD_IN"].SourceVersion = DataRowVersion.Current;

                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["ETA_DATE"].ToString()))
                {
                    updCommand.Parameters.Add("ETA_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                else
                {
                    updCommand.Parameters.Add("ETA_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["ETA_DATE"])).Direction = ParameterDirection.Input;
                }
                updCommand.Parameters["ETA_IN"].SourceVersion = DataRowVersion.Current;

                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["arrival_date"].ToString()))
                {
                    updCommand.Parameters.Add("ATA_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                else
                {
                    updCommand.Parameters.Add("ATA_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["arrival_date"])).Direction = ParameterDirection.Input;
                }
                updCommand.Parameters["ATA_IN"].SourceVersion = DataRowVersion.Current;



                updCommand.Parameters.Add("CUSTOMER_MST_FK_IN", OracleDbType.Int32, 10, "CUSTOMER_MST_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["CUSTOMER_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("SHIPPER_MST_FK_IN", OracleDbType.Int32, 10, "SHIPPER_MST_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["SHIPPER_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CONSIGNEE_MST_FK_IN", OracleDbType.Int32, 10, "CONSIGNEE_MST_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["CONSIGNEE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("NOTIFY1_MST_FK_IN", OracleDbType.Int32, 10, "NOTIFY1_MST_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["NOTIFY1_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("NOTIFY2_MST_FK_IN", OracleDbType.Int32, 10, "NOTIFY2_MST_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["NOTIFY2_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("DP_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "DP_AGENT_MST_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["DP_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CL_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "CL_AGENT_MST_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["CL_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CARGO_MOVE_FK_IN", OracleDbType.Int32, 10, "CARGO_MOVE_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["CARGO_MOVE_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("PYMT_TYPE_IN", OracleDbType.Int32, 1, "PYMT_TYPE").Direction = ParameterDirection.Input;
                updCommand.Parameters["PYMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("SHIPPING_TERMS_MST_FK_IN", OracleDbType.Int32, 10, "SHIPPING_TERMS_MST_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["SHIPPING_TERMS_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("LC_SHIPMENT_IN", OracleDbType.Int32, 1, "LC_SHIPMENT").Direction = ParameterDirection.Input;
                updCommand.Parameters["LC_SHIPMENT_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("TRANSPORT_REQ_IN", OracleDbType.Int32, 1, "TRANSPORT_REQ").Direction = ParameterDirection.Input;
                updCommand.Parameters["TRANSPORT_REQ_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("LOCAL_TRANSIT_IN", OracleDbType.Int32, 1, "LOCAL_TRANSIT").Direction = ParameterDirection.Input;
                updCommand.Parameters["LOCAL_TRANSIT_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("JOB_CARD_STATUS_IN", OracleDbType.Int32, 1, "JOB_CARD_STATUS").Direction = ParameterDirection.Input;
                updCommand.Parameters["JOB_CARD_STATUS_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("JOB_CARD_CLOSED_ON_IN", OracleDbType.Date, 20, "JOB_CARD_CLOSED_ON").Direction = ParameterDirection.Input;
                updCommand.Parameters["JOB_CARD_CLOSED_ON_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("REMARKS_IN", OracleDbType.Varchar2, 500, "REMARKS").Direction = ParameterDirection.Input;
                updCommand.Parameters["REMARKS_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CBJC_STATUS_IN", OracleDbType.Int32, 1, "CBJC_STATUS").Direction = ParameterDirection.Input;
                updCommand.Parameters["CBJC_STATUS_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CHA_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "cha_agent_mst_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["CHA_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

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
                        CBJCPK = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
                    }
                }

                var _with9 = insContainerDetails;
                _with9.Connection = objWK.MyConnection;
                _with9.CommandType = CommandType.StoredProcedure;
                _with9.CommandText = objWK.MyUserName + ".CBJC_TRN_CONT_PKG.CBJC_TRN_CONT_INS";
                var _with10 = _with9.Parameters;

                insContainerDetails.Parameters.Add("CBJC_FK_IN", CBJCPK).Direction = ParameterDirection.Input;

                insContainerDetails.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "CONTAINER_TYPE_MST_FK").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                if (Biztype == 2)
                {
                    insContainerDetails.Parameters.Add("CONTAINER_NO_IN", OracleDbType.Varchar2, 16, "CONTAINER_NO").Direction = ParameterDirection.Input;
                    insContainerDetails.Parameters["CONTAINER_NO_IN"].SourceVersion = DataRowVersion.Current;

                    insContainerDetails.Parameters.Add("SEAL_NUMBER_IN", OracleDbType.Varchar2, 20, "SEAL_NUMBER").Direction = ParameterDirection.Input;
                    insContainerDetails.Parameters["SEAL_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                    insContainerDetails.Parameters.Add("PALETTE_SIZE_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    insContainerDetails.Parameters.Add("ULD_NUMBER_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    insContainerDetails.Parameters.Add("AIRFREIGHT_SLABS_TBL_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                else
                {
                    insContainerDetails.Parameters.Add("CONTAINER_NO_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    insContainerDetails.Parameters.Add("SEAL_NUMBER_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    insContainerDetails.Parameters.Add("PALETTE_SIZE_IN", OracleDbType.Varchar2, 20, "CONTAINER_NO").Direction = ParameterDirection.Input;
                    insContainerDetails.Parameters["PALETTE_SIZE_IN"].SourceVersion = DataRowVersion.Current;

                    insContainerDetails.Parameters.Add("ULD_NUMBER_IN", OracleDbType.Varchar2, 20, "CONTAINER_TYPE_MST_ID").Direction = ParameterDirection.Input;
                    insContainerDetails.Parameters["ULD_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                    insContainerDetails.Parameters.Add("AIRFREIGHT_SLABS_TBL_FK_IN", OracleDbType.Int32, 10, "SEAL_NUMBER").Direction = ParameterDirection.Input;
                    insContainerDetails.Parameters["AIRFREIGHT_SLABS_TBL_FK_IN"].SourceVersion = DataRowVersion.Current;
                }
                insContainerDetails.Parameters.Add("CONTAINER_OWNER_TYPE_FK_IN", OracleDbType.Int32, 10, "CONTAINER_OWNER_TYPE_FK").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["CONTAINER_OWNER_TYPE_FK_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("VOLUME_IN_CBM_IN", OracleDbType.Int32, 10, "VOLUME_IN_CBM").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["VOLUME_IN_CBM_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("GROSS_WEIGHT_IN", OracleDbType.Int32, 10, "GROSS_WEIGHT").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["GROSS_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("NET_CHARGEABLE_WT_IN", OracleDbType.Int32, 10, "NET_WEIGHT").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["NET_CHARGEABLE_WT_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("PACK_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "PACK_TYPE_MST_FK").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["PACK_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("PACK_COUNT_IN", OracleDbType.Int32, 6, "PACK_COUNT").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["PACK_COUNT_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("COMMODITY_MST_FKS_IN", OracleDbType.Varchar2, 200, "COMMODITY_MST_FKS").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["COMMODITY_MST_FKS_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("LOAD_DISCHARGE_DT_IN", OracleDbType.Date, 20, "LOAD_DATE").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["LOAD_DISCHARGE_DT_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("TRUCK_LOAD_DT_IN", OracleDbType.Date, 20, "TRUCK_LOAD_DT").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["TRUCK_LOAD_DT_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("CONTAINER_PK_IN", OracleDbType.Int32, 20, "CONTAINER_PK").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["CONTAINER_PK_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("BASIS_FK_IN", OracleDbType.Int32, 20, "BASIS_FK").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["BASIS_FK_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("COMMODITY_MST_FK_IN", OracleDbType.Int32, 20, "COMMODITY_MST_FK").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["COMMODITY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

                insContainerDetails.Parameters.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;

                insContainerDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "CBJC_TRN_CONT_PK").Direction = ParameterDirection.Output;
                insContainerDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


                var _with11 = updContainerDetails;
                _with11.Connection = objWK.MyConnection;
                _with11.CommandType = CommandType.StoredProcedure;
                _with11.CommandText = objWK.MyUserName + ".CBJC_TRN_CONT_PKG.CBJC_TRN_CONT_UPD";
                var _with12 = _with11.Parameters;

                updContainerDetails.Parameters.Add("CBJC_TRN_CONT_PK_IN", OracleDbType.Int32, 10, "CBJC_TRN_CONT_PK").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["CBJC_TRN_CONT_PK_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("CBJC_FK_IN", CBJCPK).Direction = ParameterDirection.Input;

                updContainerDetails.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "CONTAINER_TYPE_MST_FK").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                if (Biztype == 2)
                {
                    updContainerDetails.Parameters.Add("CONTAINER_NO_IN", OracleDbType.Varchar2, 16, "CONTAINER_NO").Direction = ParameterDirection.Input;
                    updContainerDetails.Parameters["CONTAINER_NO_IN"].SourceVersion = DataRowVersion.Current;

                    updContainerDetails.Parameters.Add("SEAL_NUMBER_IN", OracleDbType.Varchar2, 20, "SEAL_NUMBER").Direction = ParameterDirection.Input;
                    updContainerDetails.Parameters["SEAL_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                    updContainerDetails.Parameters.Add("PALETTE_SIZE_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    updContainerDetails.Parameters.Add("ULD_NUMBER_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    updContainerDetails.Parameters.Add("AIRFREIGHT_SLABS_TBL_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                else
                {
                    updContainerDetails.Parameters.Add("CONTAINER_NO_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    updContainerDetails.Parameters.Add("SEAL_NUMBER_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    updContainerDetails.Parameters.Add("PALETTE_SIZE_IN", OracleDbType.Varchar2, 20, "CONTAINER_NO").Direction = ParameterDirection.Input;
                    updContainerDetails.Parameters["PALETTE_SIZE_IN"].SourceVersion = DataRowVersion.Current;

                    updContainerDetails.Parameters.Add("ULD_NUMBER_IN", OracleDbType.Varchar2, 20, "CONTAINER_TYPE_MST_ID").Direction = ParameterDirection.Input;
                    updContainerDetails.Parameters["ULD_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                    updContainerDetails.Parameters.Add("AIRFREIGHT_SLABS_TBL_FK_IN", OracleDbType.Int32, 10, "SEAL_NUMBER").Direction = ParameterDirection.Input;
                    updContainerDetails.Parameters["AIRFREIGHT_SLABS_TBL_FK_IN"].SourceVersion = DataRowVersion.Current;
                }

                updContainerDetails.Parameters.Add("CONTAINER_OWNER_TYPE_FK_IN", OracleDbType.Int32, 10, "CONTAINER_OWNER_TYPE_FK").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["CONTAINER_OWNER_TYPE_FK_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("VOLUME_IN_CBM_IN", OracleDbType.Int32, 10, "VOLUME_IN_CBM").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["VOLUME_IN_CBM_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("GROSS_WEIGHT_IN", OracleDbType.Int32, 10, "GROSS_WEIGHT").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["GROSS_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("NET_CHARGEABLE_WT_IN", OracleDbType.Int32, 10, "NET_WEIGHT").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["NET_CHARGEABLE_WT_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("PACK_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "PACK_TYPE_MST_FK").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["PACK_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("PACK_COUNT_IN", OracleDbType.Int32, 6, "PACK_COUNT").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["PACK_COUNT_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("COMMODITY_MST_FKS_IN", OracleDbType.Varchar2, 200, "COMMODITY_MST_FKS").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["COMMODITY_MST_FKS_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("LOAD_DISCHARGE_DT_IN", OracleDbType.Date, 20, "LOAD_DATE").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["LOAD_DISCHARGE_DT_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("TRUCK_LOAD_DT_IN", OracleDbType.Date, 20, "TRUCK_LOAD_DT").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["TRUCK_LOAD_DT_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("CONTAINER_PK_IN", OracleDbType.Int32, 20, "CONTAINER_PK").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["CONTAINER_PK_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("BASIS_FK_IN", OracleDbType.Int32, 20, "BASIS_FK").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["BASIS_FK_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("COMMODITY_MST_FK_IN", OracleDbType.Int32, 20, "COMMODITY_MST_FK").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["COMMODITY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

                updContainerDetails.Parameters.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;

                updContainerDetails.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 20, "VERSION_NO").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updContainerDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                var _with13 = delContainerDetails;
                _with13.Connection = objWK.MyConnection;
                _with13.CommandType = CommandType.StoredProcedure;
                _with13.CommandText = objWK.MyUserName + ".CBJC_TRN_CONT_PKG.CBJC_TRN_CONT_DEL";

                delContainerDetails.Parameters.Add("CBJC_TRN_CONT_PK_IN", OracleDbType.Int32, 10, "CBJC_TRN_CONT_PK").Direction = ParameterDirection.Input;
                delContainerDetails.Parameters["CBJC_TRN_CONT_PK_IN"].SourceVersion = DataRowVersion.Current;

                delContainerDetails.Parameters.Add("DELETED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

                delContainerDetails.Parameters.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;

                delContainerDetails.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 20, "VERSION_NO").Direction = ParameterDirection.Input;
                delContainerDetails.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                delContainerDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                delContainerDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with14 = objWK.MyDataAdapter;
                _with14.InsertCommand = insContainerDetails;
                _with14.InsertCommand.Transaction = TRAN;

                _with14.UpdateCommand = updContainerDetails;
                _with14.UpdateCommand.Transaction = TRAN;

                _with14.DeleteCommand = delContainerDetails;
                _with14.DeleteCommand.Transaction = TRAN;

                RecAfct = _with14.Update(dsContainerData.Tables[0]);
                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }

                var _with15 = insDocDetails;
                _with15.Connection = objWK.MyConnection;
                _with15.CommandType = CommandType.StoredProcedure;
                _with15.CommandText = objWK.MyUserName + ".CBJC_TRN_DOC_PKG.CBJC_TRN_DOC_INS";
                var _with16 = _with15.Parameters;

                insDocDetails.Parameters.Add("CBJC_FK_IN", CBJCPK).Direction = ParameterDirection.Input;

                insDocDetails.Parameters.Add("DOC_REF_NO_IN", OracleDbType.Varchar2, 16, "DOC_REF_NO").Direction = ParameterDirection.Input;
                insDocDetails.Parameters["DOC_REF_NO_IN"].SourceVersion = DataRowVersion.Current;

                insDocDetails.Parameters.Add("CB_DOC_MST_FK_IN", OracleDbType.Int32, 10, "DOC_TYPE_FK").Direction = ParameterDirection.Input;
                insDocDetails.Parameters["CB_DOC_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insDocDetails.Parameters.Add("DOC_TYPE_IN", OracleDbType.Int32, 1, "DOC_IDENTITY_FK").Direction = ParameterDirection.Input;
                insDocDetails.Parameters["DOC_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                insDocDetails.Parameters.Add("DOC_REC_FROM_IN", OracleDbType.Varchar2, 100, "RECEIVED_FROM").Direction = ParameterDirection.Input;
                insDocDetails.Parameters["DOC_REC_FROM_IN"].SourceVersion = DataRowVersion.Current;

                insDocDetails.Parameters.Add("DOC_REC_THROUGH_IN", OracleDbType.Int32, 1, "RECEIVED_THROUGH_FK").Direction = ParameterDirection.Input;
                insDocDetails.Parameters["DOC_REC_THROUGH_IN"].SourceVersion = DataRowVersion.Current;

                insDocDetails.Parameters.Add("DOC_REC_BY_IN", DBNull.Value).Direction = ParameterDirection.Input;

                insDocDetails.Parameters.Add("DOC_REC_ON_IN", OracleDbType.Date, 20, "RECEIVED_ON").Direction = ParameterDirection.Input;
                insDocDetails.Parameters["DOC_REC_ON_IN"].SourceVersion = DataRowVersion.Current;

                insDocDetails.Parameters.Add("DOC_REC_REMARKS_IN", OracleDbType.Varchar2, 250, "REMARKS").Direction = ParameterDirection.Input;
                insDocDetails.Parameters["DOC_REC_REMARKS_IN"].SourceVersion = DataRowVersion.Current;

                insDocDetails.Parameters.Add("DOC_DATE_IN", OracleDbType.Date, 20, "DOC_DATE").Direction = ParameterDirection.Input;
                insDocDetails.Parameters["DOC_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insDocDetails.Parameters.Add("DOC_CURRENCY_FK_IN", OracleDbType.Int32, 10, "DOC_CUR_FK").Direction = ParameterDirection.Input;
                insDocDetails.Parameters["DOC_CURRENCY_FK_IN"].SourceVersion = DataRowVersion.Current;

                insDocDetails.Parameters.Add("DOC_AMOUNT_IN", OracleDbType.Double, 20, "DOC_AMOUNT").Direction = ParameterDirection.Input;
                insDocDetails.Parameters["DOC_AMOUNT_IN"].SourceVersion = DataRowVersion.Current;

                insDocDetails.Parameters.Add("DOC_REC_BY_FK_IN", OracleDbType.Int32, 10, "RECEIVED_BY_FK").Direction = ParameterDirection.Input;
                insDocDetails.Parameters["DOC_REC_BY_FK_IN"].SourceVersion = DataRowVersion.Current;

                insDocDetails.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

                insDocDetails.Parameters.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;

                insDocDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "CBJC_TRN_DOC_PK").Direction = ParameterDirection.Output;
                insDocDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with17 = updDocDetails;
                _with17.Connection = objWK.MyConnection;
                _with17.CommandType = CommandType.StoredProcedure;
                _with17.CommandText = objWK.MyUserName + ".CBJC_TRN_DOC_PKG.CBJC_TRN_DOC_UPD";
                var _with18 = _with17.Parameters;
                updDocDetails.Parameters.Add("CBJC_TRN_DOC_PK_IN", OracleDbType.Int32, 10, "DOC_PK").Direction = ParameterDirection.Input;
                updDocDetails.Parameters["CBJC_TRN_DOC_PK_IN"].SourceVersion = DataRowVersion.Current;

                updDocDetails.Parameters.Add("CBJC_FK_IN", CBJCPK).Direction = ParameterDirection.Input;

                updDocDetails.Parameters.Add("DOC_REF_NO_IN", OracleDbType.Varchar2, 16, "DOC_REF_NO").Direction = ParameterDirection.Input;
                updDocDetails.Parameters["DOC_REF_NO_IN"].SourceVersion = DataRowVersion.Current;

                updDocDetails.Parameters.Add("CB_DOC_MST_FK_IN", OracleDbType.Int32, 10, "DOC_TYPE_FK").Direction = ParameterDirection.Input;
                updDocDetails.Parameters["CB_DOC_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updDocDetails.Parameters.Add("DOC_TYPE_IN", OracleDbType.Int32, 1, "DOC_IDENTITY_FK").Direction = ParameterDirection.Input;
                updDocDetails.Parameters["DOC_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                updDocDetails.Parameters.Add("DOC_REC_FROM_IN", OracleDbType.Varchar2, 100, "RECEIVED_FROM").Direction = ParameterDirection.Input;
                updDocDetails.Parameters["DOC_REC_FROM_IN"].SourceVersion = DataRowVersion.Current;

                updDocDetails.Parameters.Add("DOC_REC_THROUGH_IN", OracleDbType.Int32, 1, "RECEIVED_THROUGH_FK").Direction = ParameterDirection.Input;
                updDocDetails.Parameters["DOC_REC_THROUGH_IN"].SourceVersion = DataRowVersion.Current;

                updDocDetails.Parameters.Add("DOC_REC_BY_IN", DBNull.Value).Direction = ParameterDirection.Input;

                updDocDetails.Parameters.Add("DOC_REC_ON_IN", OracleDbType.Date, 20, "RECEIVED_ON").Direction = ParameterDirection.Input;
                updDocDetails.Parameters["DOC_REC_ON_IN"].SourceVersion = DataRowVersion.Current;

                updDocDetails.Parameters.Add("DOC_REC_REMARKS_IN", OracleDbType.Varchar2, 250, "REMARKS").Direction = ParameterDirection.Input;
                updDocDetails.Parameters["DOC_REC_REMARKS_IN"].SourceVersion = DataRowVersion.Current;

                updDocDetails.Parameters.Add("DOC_DATE_IN", OracleDbType.Date, 20, "DOC_DATE").Direction = ParameterDirection.Input;
                updDocDetails.Parameters["DOC_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updDocDetails.Parameters.Add("DOC_CURRENCY_FK_IN", OracleDbType.Int32, 10, "DOC_CUR_FK").Direction = ParameterDirection.Input;
                updDocDetails.Parameters["DOC_CURRENCY_FK_IN"].SourceVersion = DataRowVersion.Current;

                updDocDetails.Parameters.Add("DOC_AMOUNT_IN", OracleDbType.Double, 20, "DOC_AMOUNT").Direction = ParameterDirection.Input;
                updDocDetails.Parameters["DOC_AMOUNT_IN"].SourceVersion = DataRowVersion.Current;

                updDocDetails.Parameters.Add("DOC_REC_BY_FK_IN", OracleDbType.Int32, 10, "RECEIVED_BY_FK").Direction = ParameterDirection.Input;
                updDocDetails.Parameters["DOC_REC_BY_FK_IN"].SourceVersion = DataRowVersion.Current;

                updDocDetails.Parameters.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

                updDocDetails.Parameters.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;

                updDocDetails.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 20, "VERSION_NO").Direction = ParameterDirection.Input;
                updDocDetails.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                updDocDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "CBJC_TRN_DOC_PK").Direction = ParameterDirection.Output;
                updDocDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with19 = delDocDetails;
                _with19.Connection = objWK.MyConnection;
                _with19.CommandType = CommandType.StoredProcedure;
                _with19.CommandText = objWK.MyUserName + ".CBJC_TRN_DOC_PKG.CBJC_TRN_DOC_DEL";

                delDocDetails.Parameters.Add("CBJC_TRN_DOC_PK_IN", OracleDbType.Int32, 10, "DOC_PK").Direction = ParameterDirection.Input;
                delDocDetails.Parameters["CBJC_TRN_DOC_PK_IN"].SourceVersion = DataRowVersion.Current;

                delDocDetails.Parameters.Add("DELETED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

                delDocDetails.Parameters.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;

                delDocDetails.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 20, "VERSION_NO").Direction = ParameterDirection.Input;
                delDocDetails.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                delDocDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                delDocDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with20 = objWK.MyDataAdapter;
                _with20.InsertCommand = insDocDetails;
                _with20.InsertCommand.Transaction = TRAN;

                _with20.UpdateCommand = updDocDetails;
                _with20.UpdateCommand.Transaction = TRAN;

                _with20.DeleteCommand = delDocDetails;
                _with20.DeleteCommand.Transaction = TRAN;

                RecAfct = _with20.Update(dsDocDetails.Tables[0]);
                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }

                //'ADDED BY MAYUR FOR CUSTOMS CLEARANCE
                var _with21 = insCustomsClearanceDetails;
                _with21.Connection = objWK.MyConnection;
                _with21.CommandType = CommandType.StoredProcedure;
                _with21.CommandText = objWK.MyUserName + ".CBJC_TRN_CC_PKG.CBJC_TRN_CC_INS";
                var _with22 = _with21.Parameters;

                insCustomsClearanceDetails.Parameters.Add("CBJC_FK_IN", CBJCPK).Direction = ParameterDirection.Input;

                insCustomsClearanceDetails.Parameters.Add("ASSESS_STATUS_IN", OracleDbType.Int32, 1, "ASSESS_STATUS").Direction = ParameterDirection.Input;
                insCustomsClearanceDetails.Parameters["ASSESS_STATUS_IN"].SourceVersion = DataRowVersion.Current;

                insCustomsClearanceDetails.Parameters.Add("ASSESS_STATUS_DATE_IN", OracleDbType.Date, 20, "ASSESS_STATUS_DATE").Direction = ParameterDirection.Input;
                insCustomsClearanceDetails.Parameters["ASSESS_STATUS_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insCustomsClearanceDetails.Parameters.Add("CUSTOMS_REF_NO_IN", OracleDbType.Varchar2, 100, "CUSTOMS_REF_NO").Direction = ParameterDirection.Input;
                insCustomsClearanceDetails.Parameters["CUSTOMS_REF_NO_IN"].SourceVersion = DataRowVersion.Current;

                insCustomsClearanceDetails.Parameters.Add("ASSESS_REMARKS_IN", OracleDbType.Varchar2, 250, "ASSESS_REMARKS").Direction = ParameterDirection.Input;
                insCustomsClearanceDetails.Parameters["ASSESS_REMARKS_IN"].SourceVersion = DataRowVersion.Current;

                insCustomsClearanceDetails.Parameters.Add("UPLOAD_STATUS_IN", OracleDbType.Int32, 1, "UPLOAD_STATUS").Direction = ParameterDirection.Input;
                insCustomsClearanceDetails.Parameters["UPLOAD_STATUS_IN"].SourceVersion = DataRowVersion.Current;

                insCustomsClearanceDetails.Parameters.Add("UPLOAD_DATE_IN", OracleDbType.Date, 20, "UPLOAD_DATE").Direction = ParameterDirection.Input;
                insCustomsClearanceDetails.Parameters["UPLOAD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insCustomsClearanceDetails.Parameters.Add("UPLOAD_BY_IN", OracleDbType.Varchar2, 100, "UPLOAD_BY").Direction = ParameterDirection.Input;
                insCustomsClearanceDetails.Parameters["UPLOAD_BY_IN"].SourceVersion = DataRowVersion.Current;

                insCustomsClearanceDetails.Parameters.Add("UPLOAD_OBL_IN", OracleDbType.Int32, 1, "UPLOAD_OBL").Direction = ParameterDirection.Input;
                insCustomsClearanceDetails.Parameters["UPLOAD_OBL_IN"].SourceVersion = DataRowVersion.Current;

                insCustomsClearanceDetails.Parameters.Add("UPLOAD_COMM_INV_IN", OracleDbType.Int32, 1, "UPLOAD_COMM_INV").Direction = ParameterDirection.Input;
                insCustomsClearanceDetails.Parameters["UPLOAD_COMM_INV_IN"].SourceVersion = DataRowVersion.Current;

                insCustomsClearanceDetails.Parameters.Add("UPLOAD_PACK_LIST_IN", OracleDbType.Int32, 1, "UPLOAD_PACK_LIST").Direction = ParameterDirection.Input;
                insCustomsClearanceDetails.Parameters["UPLOAD_PACK_LIST_IN"].SourceVersion = DataRowVersion.Current;

                insCustomsClearanceDetails.Parameters.Add("UPLOAD_OTHERS_IN", OracleDbType.Int32, 1, "UPLOAD_OTHERS").Direction = ParameterDirection.Input;
                insCustomsClearanceDetails.Parameters["UPLOAD_OTHERS_IN"].SourceVersion = DataRowVersion.Current;

                insCustomsClearanceDetails.Parameters.Add("UPLOAD_REMARKS_IN", OracleDbType.Varchar2, 250, "UPLOAD_REMARKS").Direction = ParameterDirection.Input;
                insCustomsClearanceDetails.Parameters["UPLOAD_REMARKS_IN"].SourceVersion = DataRowVersion.Current;

                insCustomsClearanceDetails.Parameters.Add("VERIFIED_CUSTOMS_IN", OracleDbType.Int32, 1, "VERIFIED_CUSTOMS").Direction = ParameterDirection.Input;
                insCustomsClearanceDetails.Parameters["VERIFIED_CUSTOMS_IN"].SourceVersion = DataRowVersion.Current;

                insCustomsClearanceDetails.Parameters.Add("VERIFIED_DATE_IN", OracleDbType.Date, 20, "VERIFIED_DATE").Direction = ParameterDirection.Input;
                insCustomsClearanceDetails.Parameters["VERIFIED_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insCustomsClearanceDetails.Parameters.Add("RO_REF_NO_IN", OracleDbType.Varchar2, 50, "RO_REF_NO").Direction = ParameterDirection.Input;
                insCustomsClearanceDetails.Parameters["RO_REF_NO_IN"].SourceVersion = DataRowVersion.Current;

                insCustomsClearanceDetails.Parameters.Add("VERIFIED_REMARKS_IN", OracleDbType.Varchar2, 250, "VERIFIED_REMARKS").Direction = ParameterDirection.Input;
                insCustomsClearanceDetails.Parameters["VERIFIED_REMARKS_IN"].SourceVersion = DataRowVersion.Current;

                insCustomsClearanceDetails.Parameters.Add("CC_STATUS_IN", OracleDbType.Int32, 1, "CC_STATUS").Direction = ParameterDirection.Input;
                insCustomsClearanceDetails.Parameters["CC_STATUS_IN"].SourceVersion = DataRowVersion.Current;

                insCustomsClearanceDetails.Parameters.Add("CC_DATE_IN", OracleDbType.Int32, 20, "CC_DATE").Direction = ParameterDirection.Input;
                insCustomsClearanceDetails.Parameters["CC_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insCustomsClearanceDetails.Parameters.Add("PORT_CHARGE_IN", OracleDbType.Int32, 1, "PORT_CHARGE").Direction = ParameterDirection.Input;
                insCustomsClearanceDetails.Parameters["PORT_CHARGE_IN"].SourceVersion = DataRowVersion.Current;

                insCustomsClearanceDetails.Parameters.Add("CC_REMARKS_IN", OracleDbType.Varchar2, 250, "CC_REMARKS").Direction = ParameterDirection.Input;
                insCustomsClearanceDetails.Parameters["CC_REMARKS_IN"].SourceVersion = DataRowVersion.Current;

                insCustomsClearanceDetails.Parameters.Add("CARGO_DESCRIPTION_IN", OracleDbType.Varchar2, 250, "CARGO_DESCRIPTION").Direction = ParameterDirection.Input;
                insCustomsClearanceDetails.Parameters["CARGO_DESCRIPTION_IN"].SourceVersion = DataRowVersion.Current;

                insCustomsClearanceDetails.Parameters.Add("MARKS_NUMBERS_IN", OracleDbType.Varchar2, 250, "MARKS_NUMBERS").Direction = ParameterDirection.Input;
                insCustomsClearanceDetails.Parameters["MARKS_NUMBERS_IN"].SourceVersion = DataRowVersion.Current;

                insCustomsClearanceDetails.Parameters.Add("PO_DATE_IN", OracleDbType.Date, 20, "PO_DATE").Direction = ParameterDirection.Input;
                insCustomsClearanceDetails.Parameters["PO_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insCustomsClearanceDetails.Parameters.Add("DO_RECIVED_IN", OracleDbType.Int32, 1, "DO_RECIVED").Direction = ParameterDirection.Input;
                insCustomsClearanceDetails.Parameters["DO_RECIVED_IN"].SourceVersion = DataRowVersion.Current;

                insCustomsClearanceDetails.Parameters.Add("DO_NUMNER_IN", OracleDbType.Varchar2, 50, "DO_NUMNER").Direction = ParameterDirection.Input;
                insCustomsClearanceDetails.Parameters["DO_NUMNER_IN"].SourceVersion = DataRowVersion.Current;

                insCustomsClearanceDetails.Parameters.Add("DO_RECIVEDON_IN", OracleDbType.Date, 20, "DO_RECEIVED_ON").Direction = ParameterDirection.Input;
                insCustomsClearanceDetails.Parameters["DO_RECIVEDON_IN"].SourceVersion = DataRowVersion.Current;

                insCustomsClearanceDetails.Parameters.Add("P_PAD_IN", OracleDbType.Varchar2, 100, "P_PAD").Direction = ParameterDirection.Input;
                insCustomsClearanceDetails.Parameters["P_PAD_IN"].SourceVersion = DataRowVersion.Current;

                insCustomsClearanceDetails.Parameters.Add("P_PAD_DT_IN", OracleDbType.Date, 20, "P_PAD_DT").Direction = ParameterDirection.Input;
                insCustomsClearanceDetails.Parameters["P_PAD_DT_IN"].SourceVersion = DataRowVersion.Current;

                insCustomsClearanceDetails.Parameters.Add("A_PAD_IN", OracleDbType.Varchar2, 100, "A_PAD").Direction = ParameterDirection.Input;
                insCustomsClearanceDetails.Parameters["A_PAD_IN"].SourceVersion = DataRowVersion.Current;

                insCustomsClearanceDetails.Parameters.Add("A_PAD_DT_IN", OracleDbType.Date, 20, "A_PAD_DT").Direction = ParameterDirection.Input;
                insCustomsClearanceDetails.Parameters["A_PAD_DT_IN"].SourceVersion = DataRowVersion.Current;

                insCustomsClearanceDetails.Parameters.Add("SCAN_PROCESS_IN", OracleDbType.Int32, 1, "SCAN_PROCESS").Direction = ParameterDirection.Input;
                insCustomsClearanceDetails.Parameters["SCAN_PROCESS_IN"].SourceVersion = DataRowVersion.Current;

                insCustomsClearanceDetails.Parameters.Add("SCAN_DT_IN", OracleDbType.Date, 20, "SCAN_DT").Direction = ParameterDirection.Input;
                insCustomsClearanceDetails.Parameters["SCAN_DT_IN"].SourceVersion = DataRowVersion.Current;

                insCustomsClearanceDetails.Parameters.Add("CUSTOMS_REMARKS_IN", OracleDbType.Varchar2, 250, "CUSTOMS_REMARKS").Direction = ParameterDirection.Input;
                insCustomsClearanceDetails.Parameters["CUSTOMS_REMARKS_IN"].SourceVersion = DataRowVersion.Current;

                insCustomsClearanceDetails.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

                insCustomsClearanceDetails.Parameters.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;

                insCustomsClearanceDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "CBJC_TRN_CC_PK").Direction = ParameterDirection.Output;
                insCustomsClearanceDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;



                var _with23 = updCustomsClearanceDetails;
                _with23.Connection = objWK.MyConnection;
                _with23.CommandType = CommandType.StoredProcedure;
                _with23.CommandText = objWK.MyUserName + ".CBJC_TRN_CC_PKG.CBJC_TRN_CC_UPD";
                var _with24 = _with23.Parameters;

                updCustomsClearanceDetails.Parameters.Add("CBJC_TRN_CC_PK_IN", OracleDbType.Int32, 10, "CBJC_TRN_CC_PK").Direction = ParameterDirection.Input;
                updCustomsClearanceDetails.Parameters["CBJC_TRN_CC_PK_IN"].SourceVersion = DataRowVersion.Current;

                updCustomsClearanceDetails.Parameters.Add("CBJC_FK_IN", CBJCPK).Direction = ParameterDirection.Input;

                updCustomsClearanceDetails.Parameters.Add("ASSESS_STATUS_IN", OracleDbType.Int32, 1, "ASSESS_STATUS").Direction = ParameterDirection.Input;
                updCustomsClearanceDetails.Parameters["ASSESS_STATUS_IN"].SourceVersion = DataRowVersion.Current;

                updCustomsClearanceDetails.Parameters.Add("ASSESS_STATUS_DATE_IN", OracleDbType.Date, 20, "ASSESS_STATUS_DATE").Direction = ParameterDirection.Input;
                updCustomsClearanceDetails.Parameters["ASSESS_STATUS_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCustomsClearanceDetails.Parameters.Add("CUSTOMS_REF_NO_IN", OracleDbType.Varchar2, 100, "CUSTOMS_REF_NO").Direction = ParameterDirection.Input;
                updCustomsClearanceDetails.Parameters["CUSTOMS_REF_NO_IN"].SourceVersion = DataRowVersion.Current;

                updCustomsClearanceDetails.Parameters.Add("ASSESS_REMARKS_IN", OracleDbType.Varchar2, 250, "ASSESS_REMARKS").Direction = ParameterDirection.Input;
                updCustomsClearanceDetails.Parameters["ASSESS_REMARKS_IN"].SourceVersion = DataRowVersion.Current;

                updCustomsClearanceDetails.Parameters.Add("UPLOAD_STATUS_IN", OracleDbType.Int32, 1, "UPLOAD_STATUS").Direction = ParameterDirection.Input;
                updCustomsClearanceDetails.Parameters["UPLOAD_STATUS_IN"].SourceVersion = DataRowVersion.Current;

                updCustomsClearanceDetails.Parameters.Add("UPLOAD_DATE_IN", OracleDbType.Date, 20, "UPLOAD_DATE").Direction = ParameterDirection.Input;
                updCustomsClearanceDetails.Parameters["UPLOAD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCustomsClearanceDetails.Parameters.Add("UPLOAD_BY_IN", OracleDbType.Varchar2, 100, "UPLOAD_BY").Direction = ParameterDirection.Input;
                updCustomsClearanceDetails.Parameters["UPLOAD_BY_IN"].SourceVersion = DataRowVersion.Current;

                updCustomsClearanceDetails.Parameters.Add("UPLOAD_OBL_IN", OracleDbType.Int32, 1, "UPLOAD_OBL").Direction = ParameterDirection.Input;
                updCustomsClearanceDetails.Parameters["UPLOAD_OBL_IN"].SourceVersion = DataRowVersion.Current;

                updCustomsClearanceDetails.Parameters.Add("UPLOAD_COMM_INV_IN", OracleDbType.Int32, 1, "UPLOAD_COMM_INV").Direction = ParameterDirection.Input;
                updCustomsClearanceDetails.Parameters["UPLOAD_COMM_INV_IN"].SourceVersion = DataRowVersion.Current;

                updCustomsClearanceDetails.Parameters.Add("UPLOAD_PACK_LIST_IN", OracleDbType.Int32, 1, "UPLOAD_PACK_LIST").Direction = ParameterDirection.Input;
                updCustomsClearanceDetails.Parameters["UPLOAD_PACK_LIST_IN"].SourceVersion = DataRowVersion.Current;

                updCustomsClearanceDetails.Parameters.Add("UPLOAD_OTHERS_IN", OracleDbType.Int32, 1, "UPLOAD_OTHERS").Direction = ParameterDirection.Input;
                updCustomsClearanceDetails.Parameters["UPLOAD_OTHERS_IN"].SourceVersion = DataRowVersion.Current;

                updCustomsClearanceDetails.Parameters.Add("UPLOAD_REMARKS_IN", OracleDbType.Varchar2, 250, "UPLOAD_REMARKS").Direction = ParameterDirection.Input;
                updCustomsClearanceDetails.Parameters["UPLOAD_REMARKS_IN"].SourceVersion = DataRowVersion.Current;

                updCustomsClearanceDetails.Parameters.Add("VERIFIED_CUSTOMS_IN", OracleDbType.Int32, 1, "VERIFIED_CUSTOMS").Direction = ParameterDirection.Input;
                updCustomsClearanceDetails.Parameters["VERIFIED_CUSTOMS_IN"].SourceVersion = DataRowVersion.Current;

                updCustomsClearanceDetails.Parameters.Add("VERIFIED_DATE_IN", OracleDbType.Date, 20, "VERIFIED_DATE").Direction = ParameterDirection.Input;
                updCustomsClearanceDetails.Parameters["VERIFIED_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCustomsClearanceDetails.Parameters.Add("RO_REF_NO_IN", OracleDbType.Varchar2, 50, "RO_REF_NO").Direction = ParameterDirection.Input;
                updCustomsClearanceDetails.Parameters["RO_REF_NO_IN"].SourceVersion = DataRowVersion.Current;

                updCustomsClearanceDetails.Parameters.Add("VERIFIED_REMARKS_IN", OracleDbType.Varchar2, 250, "VERIFIED_REMARKS").Direction = ParameterDirection.Input;
                updCustomsClearanceDetails.Parameters["VERIFIED_REMARKS_IN"].SourceVersion = DataRowVersion.Current;

                updCustomsClearanceDetails.Parameters.Add("CC_STATUS_IN", OracleDbType.Int32, 1, "CC_STATUS").Direction = ParameterDirection.Input;
                updCustomsClearanceDetails.Parameters["CC_STATUS_IN"].SourceVersion = DataRowVersion.Current;

                updCustomsClearanceDetails.Parameters.Add("CC_DATE_IN", OracleDbType.Date, 20, "CC_DATE").Direction = ParameterDirection.Input;
                updCustomsClearanceDetails.Parameters["CC_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCustomsClearanceDetails.Parameters.Add("PORT_CHARGE_IN", OracleDbType.Int32, 1, "PORT_CHARGE").Direction = ParameterDirection.Input;
                updCustomsClearanceDetails.Parameters["PORT_CHARGE_IN"].SourceVersion = DataRowVersion.Current;

                updCustomsClearanceDetails.Parameters.Add("CC_REMARKS_IN", OracleDbType.Varchar2, 250, "CC_REMARKS").Direction = ParameterDirection.Input;
                updCustomsClearanceDetails.Parameters["CC_REMARKS_IN"].SourceVersion = DataRowVersion.Current;

                updCustomsClearanceDetails.Parameters.Add("CARGO_DESCRIPTION_IN", OracleDbType.Varchar2, 250, "CARGO_DESCRIPTION").Direction = ParameterDirection.Input;
                updCustomsClearanceDetails.Parameters["CARGO_DESCRIPTION_IN"].SourceVersion = DataRowVersion.Current;

                updCustomsClearanceDetails.Parameters.Add("MARKS_NUMBERS_IN", OracleDbType.Varchar2, 250, "MARKS_NUMBERS").Direction = ParameterDirection.Input;
                updCustomsClearanceDetails.Parameters["MARKS_NUMBERS_IN"].SourceVersion = DataRowVersion.Current;

                updCustomsClearanceDetails.Parameters.Add("PO_DATE_IN", OracleDbType.Date, 20, "PO_DATE").Direction = ParameterDirection.Input;
                updCustomsClearanceDetails.Parameters["PO_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCustomsClearanceDetails.Parameters.Add("DO_RECIVED_IN", OracleDbType.Int32, 1, "DO_RECIVED").Direction = ParameterDirection.Input;
                updCustomsClearanceDetails.Parameters["DO_RECIVED_IN"].SourceVersion = DataRowVersion.Current;

                updCustomsClearanceDetails.Parameters.Add("DO_NUMNER_IN", OracleDbType.Varchar2, 50, "DO_NUMNER").Direction = ParameterDirection.Input;
                updCustomsClearanceDetails.Parameters["DO_NUMNER_IN"].SourceVersion = DataRowVersion.Current;

                updCustomsClearanceDetails.Parameters.Add("DO_RECIVEDON_IN", OracleDbType.Date, 20, "DO_RECEIVED_ON").Direction = ParameterDirection.Input;
                updCustomsClearanceDetails.Parameters["DO_RECIVEDON_IN"].SourceVersion = DataRowVersion.Current;

                updCustomsClearanceDetails.Parameters.Add("P_PAD_IN", OracleDbType.Varchar2, 100, "P_PAD").Direction = ParameterDirection.Input;
                updCustomsClearanceDetails.Parameters["P_PAD_IN"].SourceVersion = DataRowVersion.Current;

                updCustomsClearanceDetails.Parameters.Add("P_PAD_DT_IN", OracleDbType.Date, 20, "P_PAD_DT").Direction = ParameterDirection.Input;
                updCustomsClearanceDetails.Parameters["P_PAD_DT_IN"].SourceVersion = DataRowVersion.Current;

                updCustomsClearanceDetails.Parameters.Add("A_PAD_IN", OracleDbType.Varchar2, 100, "A_PAD").Direction = ParameterDirection.Input;
                updCustomsClearanceDetails.Parameters["A_PAD_IN"].SourceVersion = DataRowVersion.Current;

                updCustomsClearanceDetails.Parameters.Add("A_PAD_DT_IN", OracleDbType.Date, 20, "A_PAD_DT").Direction = ParameterDirection.Input;
                updCustomsClearanceDetails.Parameters["A_PAD_DT_IN"].SourceVersion = DataRowVersion.Current;

                updCustomsClearanceDetails.Parameters.Add("SCAN_PROCESS_IN", OracleDbType.Int32, 1, "SCAN_PROCESS").Direction = ParameterDirection.Input;
                updCustomsClearanceDetails.Parameters["SCAN_PROCESS_IN"].SourceVersion = DataRowVersion.Current;

                updCustomsClearanceDetails.Parameters.Add("SCAN_DT_IN", OracleDbType.Date, 20, "SCAN_DT").Direction = ParameterDirection.Input;
                updCustomsClearanceDetails.Parameters["SCAN_DT_IN"].SourceVersion = DataRowVersion.Current;

                updCustomsClearanceDetails.Parameters.Add("CUSTOMS_REMARKS_IN", OracleDbType.Varchar2, 250, "CUSTOMS_REMARKS").Direction = ParameterDirection.Input;
                updCustomsClearanceDetails.Parameters["CUSTOMS_REMARKS_IN"].SourceVersion = DataRowVersion.Current;

                updCustomsClearanceDetails.Parameters.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

                updCustomsClearanceDetails.Parameters.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;

                updCustomsClearanceDetails.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 20, "VERSION_NO").Direction = ParameterDirection.Input;
                updCustomsClearanceDetails.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                updCustomsClearanceDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updCustomsClearanceDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                
                var _with25 = objWK.MyDataAdapter;
                _with25.InsertCommand = insCustomsClearanceDetails;
                _with25.InsertCommand.Transaction = TRAN;

                _with25.UpdateCommand = updCustomsClearanceDetails;
                _with25.UpdateCommand.Transaction = TRAN;

                _with25.DeleteCommand = delCustomsClearanceDetails;
                _with25.DeleteCommand.Transaction = TRAN;

                RecAfct = _with25.Update(dsCC.Tables[0]);
                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                //'ENDED BY MAYUR FOR CUSTOMS CLEARANCE

                if (!SaveSecondaryServices(ref objWK, ref TRAN, Convert.ToInt32(CBJCPK), ref dsIncomeChargeDetails, ref dsExpenseChargeDetails))
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
                    if (CBJCPK > 0)
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
                            //    objPush.UpdateCostCentre(schDtls[10], schDtls[2], schDtls[6], schDtls[4], ref errGen, schDtls[5].ToString().ToUpper(), schDtls[0].ToString().ToUpper(), , , CBJCPK);
                            //    if (ConfigurationSettings.AppSettings["EVENTVIEWER"])
                            //    {
                            //        objPush.EventViewer(1, 1, Session["USER_PK"]);
                            //    }
                            //}
                            //catch (Exception ex)
                            //{
                            //    if (ConfigurationSettings.AppSettings["EVENTVIEWER"])
                            //    {
                            //        objPush.EventViewer(1, 2, Session["USER_PK"]);
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

        #region "Secondary Services"
        public bool SaveSecondaryServices(ref WorkFlow objWK, ref OracleTransaction TRAN, int CBJCPK, ref DataSet dsIncomeChargeDetails, ref DataSet dsExpenseChargeDetails)
        {
            if ((dsIncomeChargeDetails != null))
            {
                //----------------------------------Income Charge Details----------------------------------
                foreach (DataRow ri in dsIncomeChargeDetails.Tables[1].Rows)
                {
                    int Frt_Pk = 0;
                    try
                    {
                        Frt_Pk = Convert.ToInt32(ri["CBJC_TRN_FD_PK"]);
                    }
                    catch (Exception ex)
                    {
                        Frt_Pk = 0;
                    }
                    var _with26 = objWK.MyCommand;
                    _with26.Parameters.Clear();
                    _with26.Transaction = TRAN;
                    _with26.CommandType = CommandType.StoredProcedure;
                    if (Frt_Pk > 0)
                    {
                        _with26.CommandText = objWK.MyUserName + ".CBJC_TRN_FD_PKG.CBJC_TRN_FD_UPD";
                        _with26.Parameters.Add("CBJC_TRN_FD_PK_IN", ri["CBJC_TRN_FD_PK"]).Direction = ParameterDirection.Input;
                        _with26.Parameters.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                        _with26.Parameters.Add("VERSION_NO_IN", ri["VERSION_NO"]).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with26.CommandText = objWK.MyUserName + ".CBJC_TRN_FD_PKG.CBJC_TRN_FD_INS";
                        _with26.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                    }
                    _with26.Parameters.Add("CBJC_FK_IN", CBJCPK).Direction = ParameterDirection.Input;
                    _with26.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", ri["CHARGE_PK"]).Direction = ParameterDirection.Input;
                    _with26.Parameters.Add("FREIGHT_TYPE_IN", ri["FREIGHT_TYPE"]).Direction = ParameterDirection.Input;
                    _with26.Parameters.Add("LOCATION_MST_FK_IN", ri["LOCATION_MST_FK"]).Direction = ParameterDirection.Input;
                    _with26.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", ri["FRTPAYER_CUST_MST_FK"]).Direction = ParameterDirection.Input;
                    _with26.Parameters.Add("FREIGHT_AMT_IN", ri["FREIGHT_AMT"]).Direction = ParameterDirection.Input;
                    _with26.Parameters.Add("CURRENCY_MST_FK_IN", ri["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                    _with26.Parameters.Add("BASIS_FK_IN", getDefault(ri["BASIS_PK"], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with26.Parameters.Add("EXCHANGE_RATE_IN", getDefault(ri["ROE"], 1)).Direction = ParameterDirection.Input;
                    _with26.Parameters.Add("RATEPERBASIS_IN", getDefault(ri["RATEPERBASIS"], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with26.Parameters.Add("QUANTITY_IN", getDefault(ri["VOLUME"], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with26.Parameters.Add("SERVICE_MST_FK_IN", ri["SERVICE_MST_PK"]).Direction = ParameterDirection.Input;
                    _with26.Parameters.Add("INV_AGENT_TRN_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with26.Parameters.Add("CONSOL_INVOICE_TRN_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with26.Parameters.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;
                    _with26.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;

                    try
                    {
                        _with26.ExecuteNonQuery();
                        if (Frt_Pk == 0)
                        {
                            Frt_Pk = Convert.ToInt32(_with26.Parameters["RETURN_VALUE"].Value);
                            ri["CBJC_TRN_FD_PK"] = Frt_Pk;
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
                        Cost_Pk = Convert.ToInt32(re["CBJC_TRN_COST_PK"]);
                    }
                    catch (Exception ex)
                    {
                        Cost_Pk = 0;
                    }
                    var _with27 = objWK.MyCommand;
                    _with27.Parameters.Clear();
                    _with27.Transaction = TRAN;
                    _with27.CommandType = CommandType.StoredProcedure;
                    if (Cost_Pk > 0)
                    {
                        _with27.CommandText = objWK.MyUserName + ".CBJC_TRN_COST_PKG.CBJC_TRN_COST_UPD";
                        _with27.Parameters.Add("CBJC_TRN_COST_PK_IN", re["CBJC_TRN_COST_PK"]).Direction = ParameterDirection.Input;
                        _with27.Parameters.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                        _with27.Parameters.Add("VERSION_NO_IN", re["VERSION_NO"]).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with27.CommandText = objWK.MyUserName + ".CBJC_TRN_COST_PKG.CBJC_TRN_COST_INS";
                        _with27.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                    }

                    _with27.Parameters.Add("CBJC_FK_IN", CBJCPK).Direction = ParameterDirection.Input;
                    _with27.Parameters.Add("VENDOR_MST_FK_IN", re["SUPPLIER_MST_PK"]).Direction = ParameterDirection.Input;
                    _with27.Parameters.Add("COST_ELEMENT_MST_FK_IN", re["COST_ELEMENT_MST_PK"]).Direction = ParameterDirection.Input;
                    _with27.Parameters.Add("LOCATION_MST_FK_IN", re["LOCATION_MST_FK"]).Direction = ParameterDirection.Input;
                    _with27.Parameters.Add("FREIGHT_TYPE_IN", re["PTMT_TYPE"]).Direction = ParameterDirection.Input;
                    _with27.Parameters.Add("CURRENCY_MST_FK_IN", re["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                    _with27.Parameters.Add("ESTIMATED_COST_IN", re["ESTIMATED_COST"]).Direction = ParameterDirection.Input;
                    _with27.Parameters.Add("TOTAL_COST_IN", re["TOTAL_COST"]).Direction = ParameterDirection.Input;
                    _with27.Parameters.Add("BASIS_FK_IN", re["DD_VALUE"]).Direction = ParameterDirection.Input;
                    _with27.Parameters.Add("RATEPERBASIS_IN", re["RATEPERBASIS"]).Direction = ParameterDirection.Input;
                    _with27.Parameters.Add("QUANTITY_IN", getDefault(re["VOLUME"], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with27.Parameters.Add("EXCHANGE_RATE_IN", getDefault(re["ROE"], 1)).Direction = ParameterDirection.Input;
                    _with27.Parameters.Add("EXT_INT_FLAG_IN", getDefault(re["EXT_INT_FLAG"], 2)).Direction = ParameterDirection.Input;
                    _with27.Parameters.Add("SERVICE_MST_FK_IN", re["SERVICE_MST_FK"]).Direction = ParameterDirection.Input;
                    _with27.Parameters.Add("INV_SUPPLIER_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with27.Parameters.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;
                    _with27.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;
                    try
                    {
                        _with27.ExecuteNonQuery();
                        if (Cost_Pk == 0)
                        {
                            Cost_Pk = Convert.ToInt32(_with27.Parameters["RETURN_VALUE"].Value);
                            re["CBJC_TRN_COST_PK"] = Cost_Pk;
                        }
                    }
                    catch (Exception ex)
                    {
                        return false;
                    }
                }
            }
            ClearRemovedServices(ref objWK, ref TRAN, CBJCPK, ref dsIncomeChargeDetails, ref dsExpenseChargeDetails);
            return true;
        }
        public bool ClearRemovedServices(ref WorkFlow objWK, ref OracleTransaction TRAN, int CBJCPK, ref DataSet dsIncomeChargeDetails, ref DataSet dsExpenseChargeDetails)
        {
            string SelectedFrtPks = "";
            string SelectedCostPks = "";
            if (CBJCPK > 0)
            {
                try
                {
                    foreach (DataRow ri in dsIncomeChargeDetails.Tables[1].Rows)
                    {
                        if (string.IsNullOrEmpty(SelectedFrtPks))
                        {
                            SelectedFrtPks = Convert.ToString(getDefault(ri["CBJC_TRN_FD_PK"], 0));
                        }
                        else
                        {
                            SelectedFrtPks += "," + getDefault(ri["CBJC_TRN_FD_PK"], 0);
                        }
                    }
                    foreach (DataRow re in dsExpenseChargeDetails.Tables[1].Rows)
                    {
                        if (string.IsNullOrEmpty(SelectedCostPks))
                        {
                            SelectedCostPks = Convert.ToString(getDefault(re["CBJC_TRN_COST_PK"], 0));
                        }
                        else
                        {
                            SelectedCostPks += "," + getDefault(re["CBJC_TRN_COST_PK"], 0);
                        }
                    }

                    var _with28 = objWK.MyCommand;
                    _with28.Transaction = TRAN;
                    _with28.CommandType = CommandType.StoredProcedure;
                    _with28.CommandText = objWK.MyUserName + ".CBJC_TRN_FD_PKG.DELETE_SEC_CHG_EXCEPT";
                    _with28.Parameters.Clear();
                    _with28.Parameters.Add("CBJC_PK_IN", CBJCPK).Direction = ParameterDirection.Input;
                    _with28.Parameters.Add("CBJC_TRN_FD_FKS_IN", (string.IsNullOrEmpty(SelectedFrtPks) ? "" : SelectedFrtPks)).Direction = ParameterDirection.Input;
                    _with28.Parameters.Add("CBJC_TRN_COST_FKS_IN", (string.IsNullOrEmpty(SelectedCostPks) ? "" : SelectedCostPks)).Direction = ParameterDirection.Input;
                    _with28.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
                    _with28.ExecuteNonQuery();
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

        #region "Fetch Income and Expense Details"
        public DataSet FetchSecSerIncomeDetails(long CBJCPK, int CurFK = 0)
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
            //Parent Details
            try
            {
                var _with29 = objWF.MyCommand.Parameters;
                _with29.Clear();
                _with29.Add("CBJC_PK_IN", CBJCPK).Direction = ParameterDirection.Input;
                _with29.Add("BASE_CURRENCY_FK_IN", CurrencyPK).Direction = ParameterDirection.Input;
                _with29.Add("SEC_SRVC_CURSOR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dtTotalAmt = objWF.GetDataTable("CBJC_TRN_FD_PKG", "INCOME_MAIN_SEA_EXP");
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }

            //Child Details
            try
            {
                var _with30 = objWF.MyCommand.Parameters;
                _with30.Clear();
                _with30.Add("CBJC_PK_IN", CBJCPK).Direction = ParameterDirection.Input;
                _with30.Add("BASE_CURRENCY_FK_IN", Convert.ToInt64(HttpContext.Current.Session["CURRENCY_MST_PK"])).Direction = ParameterDirection.Input;
                _with30.Add("SEC_SRVC_CURSOR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dtChargeDet = objWF.GetDataTable("CBJC_TRN_FD_PKG", "INCOME_CHILD_SEA_EXP");
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
        public DataSet FetchSecSerExpenseDetails(long CBJCPK, int CurFK = 0)
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
            //Parent Details
            try
            {
                var _with31 = objWF.MyCommand.Parameters;
                _with31.Add("CBJC_PK_IN", CBJCPK).Direction = ParameterDirection.Input;
                _with31.Add("BASE_CURRENCY_FK_IN", CurrencyPK).Direction = ParameterDirection.Input;
                _with31.Add("SEC_SRVC_CURSOR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dtTotalAmt = objWF.GetDataTable("CBJC_TRN_FD_PKG", "EXPENSE_MAIN_SEA_EXP");
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
                var _with32 = objWF.MyCommand.Parameters;
                _with32.Clear();
                _with32.Add("CBJC_PK_IN", CBJCPK).Direction = ParameterDirection.Input;
                _with32.Add("BASE_CURRENCY_FK_IN", CurrencyPK).Direction = ParameterDirection.Input;
                _with32.Add("SEC_SRVC_CURSOR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dtChargeDet = objWF.GetDataTable("CBJC_TRN_FD_PKG", "EXPENSE_CHILD_SEA_EXP");

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
            StrSqlBuilder.Append("  SELECT DOC.CB_DOC_MST_PK, DOC.DOC_NAME FROM CB_DOC_MST_TBL DOC ");
            try
            {
                return ObjWk.GetDataSet(StrSqlBuilder.ToString());
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

        #region "Fetch ULD Type"
        public DataSet FetchUldType()
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder StrSqlBuilder = new StringBuilder();
            StrSqlBuilder.Append("  SELECT T.AIRFREIGHT_SLABS_TBL_PK, T.BREAKPOINT_ID ");
            StrSqlBuilder.Append(" FROM AIRFREIGHT_SLABS_TBL T ");
            StrSqlBuilder.Append("  WHERE T.BREAKPOINT_TYPE = 2");
            StrSqlBuilder.Append("   ORDER BY T.SEQUENCE_NO");
            try
            {
                return ObjWk.GetDataSet(StrSqlBuilder.ToString());
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

        #region "Fetch JC Details"
        public DataSet FetchJC(int CBJCPK = 0)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder sb = new StringBuilder();
            sb.Append("  SELECT CBJC.JC_FK, CBJC.IE FROM CBJC_TBL CBJC ");
            if (CBJCPK != 0)
            {
                sb.Append("  WHERE CBJC.CBJC_PK =" + CBJCPK);
            }
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

        #region "Get Customs Clearance Details"         ''ADDED BY MAYUR FOR CUSTOMS CLEARANCE
        public DataSet FetchCCCBJCDetails(int CBJCPK = 0, int BizType = 0, int ProcessType = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                var _with33 = objWF.MyCommand.Parameters;
                _with33.Add("CBJC_PK_IN", CBJCPK).Direction = ParameterDirection.Input;
                _with33.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with33.Add("PROCESS_TYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with33.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("CBJC_TBL_PKG", "FETCH_CBJC_CC_DETAILS");
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

        #region "Executive Enhance Search"
        public string FetchExecutive_All(string strcond, string loc = "0")
        {

            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strReq = null;
            string strSERACH_IN = null;
            var strNull = DBNull.Value;
            arr = strcond.Split('~');
            if (arr.Length > 0)
                strReq = Convert.ToString(arr.GetValue(0));
            if (arr.Length > 1)
                strSERACH_IN = Convert.ToString(arr.GetValue(1));

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_EXECUTIVE_PKG.GET_EXECUTIVE_ALL";

                var _with34 = selectCommand.Parameters;
                _with34.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with34.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with34.Add("LOCATION_MST_FK_IN", loc).Direction = ParameterDirection.Input;
                _with34.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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

        public string FetchJobcard(string strcond, string loc = "0")
        {

            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strReq = null;
            string strSERACH_IN = null;
            string BizType = null;
            string ProcessType = null;

            var strNull = DBNull.Value;
            arr = strcond.Split('~');
            if (arr.Length > 0)
                strReq = Convert.ToString(arr.GetValue(0));
            if (arr.Length > 1)
                strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                BizType = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                ProcessType = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 3)
                loc = Convert.ToString(arr.GetValue(4));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_JOBCARD_PKG.GET_ALL_JOBCARDS";

                var _with35 = selectCommand.Parameters;
                _with35.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with35.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with35.Add("BUSINESS_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with35.Add("PROCESS_IN", ProcessType).Direction = ParameterDirection.Input;
                _with35.Add("LOC_IN", loc).Direction = ParameterDirection.Input;
                _with35.Add("RETURN_VALUE", OracleDbType.Clob, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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

        #region "GetRevenueDetails"

        public DataSet GetRevenueDetails(ref decimal actualCost, ref decimal actualRevenue, ref decimal estimatedCost, ref decimal estimatedRevenue, int CBJCPK, int LocationPK = 0)
        {

            WorkFlow objWF = new WorkFlow();
            try
            {
                DataSet DS = new DataSet();
                var _with36 = objWF.MyCommand.Parameters;
                _with36.Add("CBJC_FK_IN", CBJCPK).Direction = ParameterDirection.Input;
                _with36.Add("CURRPK", HttpContext.Current.Session["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
                _with36.Add("CBJC_REV", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("CBJC_TBL_PKG", "FETCH_CBJC_REVENUE");
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

        #region "Purchase Invoice"
        public DataSet FetchPurchaseInvDataExp(string CBJCNR)
        {
            WorkFlow objWK = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            DataSet dsData = new DataSet();

            try
            {
                objWK.OpenConnection();
                objWK.MyCommand.Connection = objWK.MyConnection;

                var _with37 = objWK.MyCommand;
                _with37.CommandType = CommandType.StoredProcedure;
                _with37.CommandText = objWK.MyUserName + ".INV_SUPPLIER_TBL_PKG.PURCHASEINV_FETCH";

                objWK.MyCommand.Parameters.Clear();
                var _with38 = objWK.MyCommand.Parameters;
                _with38.Add("CBJCNR_IN", (string.IsNullOrEmpty(CBJCNR) ? "" : CBJCNR)).Direction = ParameterDirection.Input;
                _with38.Add("DETAIL0_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
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
        #endregion

        #region "Fetch Revenue data export"
        public DataSet FetchRevenueData(string jobCardPK = "0")
        {
            WorkFlow objWK = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            DataSet dsData = new DataSet();

            try
            {
                objWK.OpenConnection();
                objWK.MyCommand.Connection = objWK.MyConnection;

                var _with39 = objWK.MyCommand;
                _with39.CommandType = CommandType.StoredProcedure;
                _with39.CommandText = objWK.MyUserName + ".INV_SUPPLIER_TBL_PKG.REVENUE_FETCH";

                objWK.MyCommand.Parameters.Clear();
                var _with40 = objWK.MyCommand.Parameters;
                _with40.Add("CBJCNR_IN", (jobCardPK == "0" ? "" : jobCardPK)).Direction = ParameterDirection.Input;
                _with40.Add("DETAIL0_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
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
        #endregion

        #region "Fetch Consol Inv Pk"
        public DataSet CheckInv(Int64 JobPK)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder();
                WorkFlow objWF = new WorkFlow();
                sb.Append(" SELECT COUNT(*) ");
                sb.Append(" FROM (SELECT JF.FREIGHT_ELEMENT_MST_FK");
                sb.Append(" FROM CBJC_TBL J, CBJC_TRN_FD JF");
                sb.Append(" WHERE J.CBJC_PK =" + JobPK);
                sb.Append(" AND JF.CBJC_FK = J.CBJC_PK");
                sb.Append(" AND JF.CONSOL_INVOICE_TRN_FK IS NULL)");
                return objWF.GetDataSet(sb.ToString());
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
        public DataSet CheckInvTPN(Int64 JobPK)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder();
                WorkFlow objWF = new WorkFlow();
                sb.Append(" SELECT COUNT(*) ");
                sb.Append(" FROM (SELECT JF.FREIGHT_ELEMENT_MST_FK");
                sb.Append(" FROM TRANSPORT_INST_SEA_TBL J, TRANSPORT_TRN_FD JF");
                sb.Append(" WHERE J.TRANSPORT_INST_SEA_PK =" + JobPK);
                sb.Append(" AND JF.TRANSPORT_INST_FK = J.TRANSPORT_INST_SEA_PK");
                sb.Append(" AND JF.CONSOL_INVOICE_TRN_FK IS NULL)");
                return objWF.GetDataSet(sb.ToString());
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
        #endregion

        #region "Fetch TPN Count for CBJC"
        public int FetchTPNCount(Int64 CBJCPK)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder();
                WorkFlow objWF = new WorkFlow();
                sb.Append("SELECT NVL(TIST.TRANSPORT_INST_SEA_PK,0)");
                sb.Append("  FROM TRANSPORT_INST_SEA_TBL TIST");
                sb.Append(" WHERE TIST.TP_CBJC_JC = 1");
                sb.Append("   AND TIST.JOB_CARD_FK IS NOT NULL");
                sb.Append("   AND TIST.JC_TYPE = 2");
                sb.Append("   AND INSTR(',' || TIST.JOB_CARD_FK || ',','," + CBJCPK + ",') > 0 ");
                return Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
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
        #endregion
    }
}