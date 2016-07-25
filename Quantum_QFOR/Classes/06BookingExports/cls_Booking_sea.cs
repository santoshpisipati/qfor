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

using Oracle.DataAccess.Client;
using System;
using System.Collections;
using System.Data;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_Booking_sea : CommonFeatures
    {

        #region "Class Variables"
        private static DataSet M_CommDataSet = new DataSet();
        private static DataSet M_MoveCodeDataset = new DataSet();
        public int bookingQBSOPK;
        public int bookingQBSOTRNPk;

        public string bookingRefNr;
        #endregion
        WorkFlow objWF = new WorkFlow();

        #region "Properties"
        public static DataSet CommDataSet
        {
            get { return M_CommDataSet; }
        }
        public static DataSet MoveCodeDataSet
        {
            get { return M_MoveCodeDataset; }
        }
        #endregion

        #region "Fetch All Values"
        public DataSet FetchAllValues(int bookPK, int bizType)
        {
            DataSet bookDS = new DataSet();
            try
            {
                objWF.MyCommand.Parameters.Clear();
                var _with1 = objWF.MyCommand.Parameters;
                _with1.Add("BIZ_TYPE_IN", bizType).Direction = ParameterDirection.Input;
                _with1.Add("BKG_PK_IN", bookPK).Direction = ParameterDirection.Input;
                _with1.Add("BOOKING_ENTRY_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                bookDS = objWF.GetDataSet("PKG_EBK_BOOKING", "QBSO_EBK_M_FETCH_BKG_ENTRY");
                return bookDS;
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

        #region "Save New Booking"
        public ArrayList SaveNew(string E_BKG_ORDER_DATE_IN = "", int BIZ_TYPE_IN = 0, string CUST_REG_NR_IN = "", int SHIPPER_FK_IN = 0, string SHIPPER_ADD_IN = "", int CONSIGNEE_FK_IN = 0, string CONSIGNEE_ADD_IN = "", int POL_FK_IN = 0, int POD_FK_IN = 0, int PLR_FK_IN = 0,
        int PFD_FK_IN = 0, string SHIP_DATE_IN = "", int SHIPPING_LINE_FK_IN = 0, int AIR_LINE_FK_IN = 0, int VOYAGE_TRN_FK_IN = 0, string FLIGHT_NR_IN = "", int CARGO_MOVE_FK_IN = 0, int CARGO_TYPE_IN = 0, int COMM_GRP_FK_IN = 0, string GROSS_WT_IN = "",
        string NET_WT_IN = "", string VOLUME_IN = "", int PACK_TYPE_IN = 0, string TEU_FACTOR_IN = "", int PACK_COUNT_IN = 0, int PYMT_TYPE_IN = 0, string NOTES_IN = "", string ULD_COUNT_IN = "", string DENSITY_IN = "", int bookPK = 0,
        string ContString = "", string CommString = "")
        {
            OracleCommand insCommand = new OracleCommand();
            OracleCommand insContCommand = new OracleCommand();
            OracleCommand insCommodityCommand = new OracleCommand();
            objWF.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWF.MyConnection.BeginTransaction();
            try
            {
                insCommand.Connection = objWF.MyConnection;
                insCommand.CommandType = CommandType.StoredProcedure;
                insCommand.CommandText = objWF.MyUserName + ".PKG_EBK_BOOKING.QBSO_EBK_M_BOOKING_INS";
                insCommand.Transaction = TRAN;
                var _with2 = insCommand.Parameters;
                _with2.Clear();
                _with2.Add("E_BKG_ORDER_DATE_IN", System.String.Format("{0:dd-MMM-yyyy}", E_BKG_ORDER_DATE_IN));
                _with2.Add("BIZ_TYPE_IN", getDefault(BIZ_TYPE_IN, DBNull.Value));
                _with2.Add("CUST_REG_NR_IN", getDefault(CUST_REG_NR_IN, DBNull.Value));
                _with2.Add("SHIPPER_FK_IN", getDefault(SHIPPER_FK_IN, DBNull.Value));
                _with2.Add("SHIPPER_ADD_IN", getDefault(SHIPPER_ADD_IN, DBNull.Value));
                _with2.Add("CONSIGNEE_FK_IN", getDefault(CONSIGNEE_FK_IN, DBNull.Value));
                _with2.Add("CONSIGNEE_ADD_IN", getDefault(CONSIGNEE_ADD_IN, DBNull.Value));
                _with2.Add("POL_FK_IN", getDefault(POL_FK_IN, DBNull.Value));
                _with2.Add("POD_FK_IN", getDefault(POD_FK_IN, DBNull.Value));
                _with2.Add("PLR_FK_IN", getDefault(PLR_FK_IN, DBNull.Value));
                _with2.Add("PFD_FK_IN", getDefault(PFD_FK_IN, DBNull.Value));
                _with2.Add("SHIP_DATE_IN", System.String.Format("{0:dd-MMM-yyyy}", SHIP_DATE_IN));
                _with2.Add("SHIPPING_LINE_FK_IN", getDefault(SHIPPING_LINE_FK_IN, DBNull.Value));
                _with2.Add("AIR_LINE_FK_IN", getDefault(AIR_LINE_FK_IN, DBNull.Value));
                _with2.Add("VOYAGE_TRN_FK_IN", getDefault(VOYAGE_TRN_FK_IN, DBNull.Value));
                _with2.Add("FLIGHT_NR_IN", getDefault(FLIGHT_NR_IN, DBNull.Value));
                _with2.Add("CARGO_MOVE_FK_IN", getDefault(CARGO_MOVE_FK_IN, DBNull.Value));
                _with2.Add("CARGO_TYPE_IN", getDefault(CARGO_TYPE_IN, DBNull.Value));
                _with2.Add("COMM_GRP_FK_IN", getDefault(COMM_GRP_FK_IN, DBNull.Value));
                _with2.Add("GROSS_WT_IN", getDefault(GROSS_WT_IN, DBNull.Value));
                _with2.Add("NET_WT_IN", getDefault(NET_WT_IN, DBNull.Value));
                _with2.Add("VOLUME_IN", getDefault(VOLUME_IN, DBNull.Value));
                _with2.Add("PACK_TYPE_IN", getDefault(PACK_TYPE_IN, DBNull.Value));
                _with2.Add("TEU_FACTOR_IN", getDefault(TEU_FACTOR_IN, DBNull.Value));
                _with2.Add("PACK_COUNT_IN", getDefault(PACK_COUNT_IN, DBNull.Value));
                _with2.Add("PYMT_TYPE_IN", getDefault(PYMT_TYPE_IN, DBNull.Value));
                _with2.Add("NOTES_IN", getDefault(NOTES_IN, DBNull.Value));
                _with2.Add("ULD_COUNT_IN", getDefault(ULD_COUNT_IN, DBNull.Value));
                _with2.Add("DENSITY_IN", getDefault(DENSITY_IN, DBNull.Value));
                _with2.Add("E_BKG_ORDER_NR_PK", bookPK).Direction = ParameterDirection.Output;
                _with2.Add("E_BKG_ORDER_REF_NR", bookingRefNr).Direction = ParameterDirection.Output;
                insCommand.Parameters["E_BKG_ORDER_NR_PK"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters["E_BKG_ORDER_REF_NR"].SourceVersion = DataRowVersion.Current;
                insCommand.ExecuteNonQuery();
                bookPK = Convert.ToInt32(insCommand.Parameters["E_BKG_ORDER_NR_PK"].Value);
                bookingRefNr = Convert.ToString(insCommand.Parameters["E_BKG_ORDER_REF_NR"].Value);
                if (ContString.Trim().Length > 0)
                {
                    Array mainArray = null;
                    int i = 0;
                    mainArray = ContString.Split('$');
                    Array childArray = null;
                    for (i = 0; i <= mainArray.Length - 1; i++)
                    {
                        string str = null;
                        childArray = mainArray.GetValue(i).ToString().Split('~');
                        var _with3 = insContCommand;
                        _with3.Connection = objWF.MyConnection;
                        _with3.CommandType = CommandType.StoredProcedure;
                        _with3.Transaction = TRAN;
                        _with3.CommandText = objWF.MyUserName + ".PKG_EBK_BOOKING.QBSO_EBK_T_CONTAINER_INS";
                        var _with4 = _with3.Parameters;
                        _with4.Clear();
                        _with4.Add("CONTAINER_TYPE_FK", getDefault(childArray.GetValue(2), DBNull.Value));
                        _with4.Add("NO_OF_CONTAINER", getDefault(childArray.GetValue(1), DBNull.Value));
                        _with4.Add("E_BKG_ORDER_NR_FK", getDefault(bookPK, DBNull.Value));
                        _with4.Add("RETURN_VALUE", bookPK).Direction = ParameterDirection.Output;
                        insContCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                        insContCommand.ExecuteNonQuery();
                    }
                }
                if (CommString.Trim().Length > 0)
                {
                    Array mainArray = null;
                    int i = 0;
                    int j = 0;
                    mainArray = CommString.Split('$');
                    Array childArray = null;
                    for (i = 0; i <= mainArray.Length - 1; i++)
                    {
                        string str = null;
                        childArray = mainArray.GetValue(i).ToString().Split(',');
                        for (j = 0; j <= childArray.Length - 1; j++)
                        {
                            var _with5 = insCommodityCommand;
                            _with5.Connection = objWF.MyConnection;
                            _with5.CommandType = CommandType.StoredProcedure;
                            _with5.Transaction = TRAN;
                            _with5.CommandText = objWF.MyUserName + ".PKG_EBK_BOOKING.QBSO_EBK_T_COMMODITY_INS";
                            var _with6 = _with5.Parameters;
                            _with6.Clear();
                            _with6.Add("E_BKG_ORDER_NR_FK", getDefault(bookPK, DBNull.Value));
                            _with6.Add("COMMODITY_FK", getDefault(childArray.GetValue(1), DBNull.Value));
                            _with6.Add("RETURN_VALUE", bookPK).Direction = ParameterDirection.Output;
                            insCommodityCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                            insCommodityCommand.ExecuteNonQuery();
                        }
                    }
                }
                arrMessage = SaveQBSOAirMain(insCommand, "", "", 0, 0, 0, 0, "", 0, 0, 0, "", 0, "", "", 0, 0, 0, "", 0, "", "", "");

                if (arrMessage.Count > 0)
                {
                    if (string.Compare((Convert.ToString(arrMessage[0]).ToUpper()), "SAVED")>0)
                    {
                        arrMessage.Clear();
                        arrMessage.Add("All data saved successfully");
                        TRAN.Commit();
                        return arrMessage;
                    }
                    else
                    {
                        TRAN.Rollback();
                        return arrMessage;
                    }
                }
            }
            catch (OracleException oraexp)
            {
                TRAN.Rollback();
                arrMessage.Add(oraexp.Message);
                return arrMessage;
                throw oraexp;
            }
            catch (Exception ex)
            {
                TRAN.Rollback();
                throw ex;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
            return new ArrayList();
        }
        #endregion

        #region "Update data"
        public ArrayList UpdateData(int bookPK = 0, int process = 0, int pol_fk = 0, int pod_fk = 0, int commGrp = 0, int commodity = 0, int packType = 0, int volume = 0, int grossWt = 0, int netWt = 0,
        string shipDate = "", int moveCode = 0, int shipTerms = 0, int payTerms = 0, int payLocation = 0, string consignee = "", string consigneeAdd = "", int status = 0, int mode = 0, int contType = 0,
        int teus = 0, string shipper = "", string shipper_Add = "", string notify1 = "", string notify1_Add = "", string notify2 = "", string notify2_Add = "", int PLR = 0, int PFD = 0, string bkgDT = "",
        string marksNumbers = "", string goodDesc = "", int version = 0)
        {
            OracleCommand updCommand = new OracleCommand();
            objWF.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWF.MyConnection.BeginTransaction();
            try
            {
                updCommand.Connection = objWF.MyConnection;
                updCommand.CommandType = CommandType.StoredProcedure;
                updCommand.CommandText = objWF.MyUserName + ".BOOKING_PKG.BOOKING_SEA_UPD";
                updCommand.Transaction = TRAN;
                var _with7 = updCommand.Parameters;
                _with7.Add("BOOKING_PK_IN", getDefault(bookPK, DBNull.Value));
                _with7.Add("PROCESS_IN", getDefault(process, DBNull.Value));
                _with7.Add("POL_FK_IN", getDefault(pol_fk, DBNull.Value));
                _with7.Add("POD_FK_IN", getDefault(pod_fk, DBNull.Value));
                _with7.Add("COMM_GRP_FK_IN", getDefault(commGrp, DBNull.Value));
                _with7.Add("COMMODITY_FK_IN", getDefault(commodity, DBNull.Value));
                _with7.Add("PACK_TYPE_FK_IN", getDefault(packType, DBNull.Value));
                _with7.Add("VOLUME_IN", getDefault(volume, DBNull.Value));
                _with7.Add("GROSS_WT_IN", getDefault(grossWt, DBNull.Value));
                _with7.Add("NET_WT_IN", getDefault(netWt, DBNull.Value));
                _with7.Add("SHIP_DATE_IN", getDefault(shipDate, DBNull.Value));
                _with7.Add("MOVE_CODE_IN", getDefault(moveCode, DBNull.Value));
                _with7.Add("SHIP_TERMS_FK_IN", getDefault(shipTerms, DBNull.Value));
                _with7.Add("PAY_TERMS_FK_IN", getDefault(payTerms, DBNull.Value));
                _with7.Add("PAY_LOC_FK_IN", getDefault(payLocation, DBNull.Value));
                _with7.Add("CONSIGNEE_IN", getDefault(consignee, DBNull.Value));
                _with7.Add("CONSIGNEE_ADD_IN", getDefault(consigneeAdd, DBNull.Value));
                _with7.Add("LAST_MODIFIED_BY_FK_IN", getDefault(M_LAST_MODIFIED_BY_FK, DBNull.Value));
                _with7.Add("STATUS_IN", getDefault(status, DBNull.Value));
                _with7.Add("MODE_IN", getDefault(mode, DBNull.Value));
                _with7.Add("CONT_TYPE_IN", getDefault(contType, DBNull.Value));
                _with7.Add("TEUS_IN", getDefault(teus, DBNull.Value));

                _with7.Add("SHIPPER_IN", getDefault(shipper, DBNull.Value));
                _with7.Add("SHIPPER_ADD_IN", getDefault(shipper_Add, DBNull.Value));
                _with7.Add("NOTIFY1_IN", getDefault(notify1, DBNull.Value));
                _with7.Add("NOTIFY1_ADD_IN", getDefault(notify1_Add, DBNull.Value));
                _with7.Add("NOTIFY2_IN", getDefault(notify2, DBNull.Value));
                _with7.Add("NOTIFY2_ADD_IN", getDefault(notify2_Add, DBNull.Value));
                _with7.Add("PLR_PK_IN", getDefault(PLR, DBNull.Value));
                _with7.Add("PFD_PK_IN", getDefault(PFD, DBNull.Value));
                _with7.Add("BOOKING_DATE_IN", getDefault(bkgDT, DBNull.Value));
                _with7.Add("MARKS_IN", getDefault(marksNumbers, DBNull.Value));
                _with7.Add("GOOD_DESC_IN", getDefault(goodDesc, DBNull.Value));

                _with7.Add("VERSION_IN", version);
                _with7.Add("RETURN_VALUE", bookPK).Direction = ParameterDirection.Output;
                updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                updCommand.ExecuteNonQuery();
                TRAN.Commit();

                if (arrMessage.Count > 0)
                {
                    return arrMessage;
                }
                else
                {
                    arrMessage.Add("All Data Saved Successfully");
                    return arrMessage;
                }
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                TRAN.Rollback();
                return arrMessage;
            }
            finally
            {
                updCommand.Connection.Close();
            }
        }
        #endregion

        #region "Update Data SPCL REQ"
        public string UpdateTransactionODC(long PkValue, string UserName, string strSpclRequest, string Mode)
        {

            OracleCommand SCD = null;
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            if (!string.IsNullOrEmpty(strSpclRequest))
            {
                arrMessage.Clear();
                string[] strParam = null;
                strParam = strSpclRequest.Split('~');
                try
                {
                    var _with8 = SCD;
                    _with8.CommandType = CommandType.StoredProcedure;
                    _with8.CommandText = UserName + ".QUOTE_SEA_ODC_SPL_REQ_PKG.QUOTE_SEA_ODC_SPL_REQ_UPD";
                    var _with9 = _with8.Parameters;
                    _with9.Clear();
                    //QUOTE_TRN_SEA_FK_IN()
                    _with9.Add("QUOTE_TRN_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    //LENGTH_IN()
                    _with9.Add("LENGTH_IN", getDefault(strParam[0], DBNull.Value)).Direction = ParameterDirection.Input;
                    //LENGTH_UOM_MST_FK_IN()
                    _with9.Add("LENGTH_UOM_MST_FK_IN", getDefault((string.IsNullOrEmpty(strParam[0].ToString()) ? 0 : Convert.ToInt32(strParam[1])), 0)).Direction = ParameterDirection.Input;
                    //HEIGHT_IN()
                    _with9.Add("HEIGHT_IN", getDefault(strParam[2], DBNull.Value)).Direction = ParameterDirection.Input;
                    //HEIGHT_UOM_MST_FK_IN()
                    _with9.Add("HEIGHT_UOM_MST_FK_IN", getDefault((string.IsNullOrEmpty(strParam[2]) ? 0 : Convert.ToInt32(strParam[3])), 0)).Direction = ParameterDirection.Input;
                    //WIDTH_IN()
                    _with9.Add("WIDTH_IN", getDefault(strParam[4], 0)).Direction = ParameterDirection.Input;
                    //WIDTH_UOM_MST_FK_IN()
                    _with9.Add("WIDTH_UOM_MST_FK_IN", getDefault((string.IsNullOrEmpty(strParam[4]) ? 0 : Convert.ToInt32(strParam[5])), 0)).Direction = ParameterDirection.Input;
                    //WEIGHT_IN()
                    _with9.Add("WEIGHT_IN", getDefault(strParam[6], DBNull.Value)).Direction = ParameterDirection.Input;
                    //WEIGHT_UOM_MST_FK_IN()
                    _with9.Add("WEIGHT_UOM_MST_FK_IN", getDefault(strParam[7], 0)).Direction = ParameterDirection.Input;
                    //VOLUME_IN()
                    _with9.Add("VOLUME_IN", getDefault(strParam[8], DBNull.Value)).Direction = ParameterDirection.Input;
                    //VOLUME_UOM_MST_FK_IN()
                    _with9.Add("VOLUME_UOM_MST_FK_IN", getDefault(strParam[9], 0)).Direction = ParameterDirection.Input;
                    //SLOT_LOSS_IN()
                    _with9.Add("SLOT_LOSS_IN", getDefault(strParam[10], 0)).Direction = ParameterDirection.Input;
                    //LOSS_QUANTITY_IN()
                    _with9.Add("LOSS_QUANTITY_IN", getDefault(strParam[11], DBNull.Value)).Direction = ParameterDirection.Input;
                    //APPR_REQ_IN()
                    _with9.Add("APPR_REQ_IN", getDefault(strParam[12], 0)).Direction = ParameterDirection.Input;
                    //RETURN_VALUE()
                    _with9.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with8.ExecuteNonQuery();

                }
                catch (OracleException oraexp)
                {

                }
                catch (Exception ex)
                {

                }
            }

            return strReturn;
        }
        public string SaveTransactionODC(long PkValue, string UserName, string strSpclRequest, string Mode)
        {

            OracleCommand SCD = null;
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;

            if (!string.IsNullOrEmpty(strSpclRequest))
            {
                arrMessage.Clear();
                string[] strParam = null;
                strParam = strSpclRequest.Split('~');
                try
                {
                    var _with10 = SCD;
                    _with10.CommandType = CommandType.StoredProcedure;
                    _with10.CommandText = UserName + ".BKG_TRN_SEA_ODC_SPL_REQ_PKG.BKG_TRN_SEA_ODC_SPL_REQ_INS";
                    var _with11 = _with10.Parameters;
                    _with11.Clear();
                    //BKG_TRN_SEA_FK_IN()
                    _with11.Add("BOOKING_TRN_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    //LENGTH_IN()
                    _with11.Add("LENGTH_IN", getDefault(strParam[0], DBNull.Value)).Direction = ParameterDirection.Input;
                    //LENGTH_UOM_MST_FK_IN()
                    _with11.Add("LENGTH_UOM_MST_FK_IN", getDefault((string.IsNullOrEmpty(strParam[0]) ? 0 : Convert.ToInt32(strParam[1])), 0)).Direction = ParameterDirection.Input;
                    //HEIGHT_IN()
                    _with11.Add("HEIGHT_IN", getDefault(strParam[2], DBNull.Value)).Direction = ParameterDirection.Input;
                    //HEIGHT_UOM_MST_FK_IN()
                    _with11.Add("HEIGHT_UOM_MST_FK_IN", getDefault((string.IsNullOrEmpty(strParam[2]) ? 0 : Convert.ToInt32(strParam[3])), 0)).Direction = ParameterDirection.Input;
                    //WIDTH_IN()
                    _with11.Add("WIDTH_IN", getDefault(strParam[4], 0)).Direction = ParameterDirection.Input;
                    //WIDTH_UOM_MST_FK_IN()
                    _with11.Add("WIDTH_UOM_MST_FK_IN", getDefault((string.IsNullOrEmpty(strParam[4]) ? 0 : Convert.ToInt32(strParam[5])), 0)).Direction = ParameterDirection.Input;
                    //WEIGHT_IN()
                    _with11.Add("WEIGHT_IN", getDefault(strParam[6], DBNull.Value)).Direction = ParameterDirection.Input;
                    //WEIGHT_UOM_MST_FK_IN()
                    _with11.Add("WEIGHT_UOM_MST_FK_IN", getDefault(strParam[7], 0)).Direction = ParameterDirection.Input;
                    //VOLUME_IN()
                    _with11.Add("VOLUME_IN", getDefault(strParam[8], DBNull.Value)).Direction = ParameterDirection.Input;
                    //VOLUME_UOM_MST_FK_IN()
                    _with11.Add("VOLUME_UOM_MST_FK_IN", getDefault(strParam[9], 0)).Direction = ParameterDirection.Input;
                    //SLOT_LOSS_IN()
                    _with11.Add("SLOT_LOSS_IN", getDefault(strParam[10], 0)).Direction = ParameterDirection.Input;
                    //LOSS_QUANTITY_IN()
                    _with11.Add("LOSS_QUANTITY_IN", getDefault(strParam[11], DBNull.Value)).Direction = ParameterDirection.Input;
                    //APPR_REQ_IN()
                    _with11.Add("APPR_REQ_IN", getDefault(strParam[12], 0)).Direction = ParameterDirection.Input;
                    //RETURN_VALUE()
                    _with11.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with10.ExecuteNonQuery();


                }
                catch (OracleException oraexp)
                {
                }
                catch (Exception ex)
                {

                }
            }

            return strReturn;
        }

        public DataTable fetchSpclReqODC(string strPK, string Mode)
        {
            try
            {

                if (!string.IsNullOrEmpty(strPK))
                {
                    System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
                    strQuery.Append("SELECT " );
                    strQuery.Append("QUOTE_SEA_ODC_SPL_REQ_PK," );
                    strQuery.Append("QUOTE_TRN_SEA_FK," );
                    strQuery.Append("LENGTH," );
                    strQuery.Append("LENGTH_UOM_MST_FK," );
                    strQuery.Append("HEIGHT," );
                    strQuery.Append("HEIGHT_UOM_MST_FK," );
                    strQuery.Append("WIDTH," );
                    strQuery.Append("WIDTH_UOM_MST_FK," );
                    strQuery.Append("WEIGHT," );
                    strQuery.Append("WEIGHT_UOM_MST_FK," );
                    strQuery.Append("VOLUME," );
                    strQuery.Append("VOLUME_UOM_MST_FK," );
                    strQuery.Append("SLOT_LOSS," );
                    strQuery.Append("LOSS_QUANTITY," );
                    strQuery.Append("APPR_REQ, " );
                    strQuery.Append("NO_OF_SLOTS " );

                    if (Convert.ToInt32(Mode) == 0)
                    {
                        strQuery.Append("FROM BKG_TRN_SEA_ODC_SPL_REQ Q" );
                    }
                    else
                    {
                        strQuery.Append("FROM BKG_TRN_AIR_ODC_SPL_REQ Q" );
                    }

                    strQuery.Append("WHERE " );

                    if (Convert.ToInt32(Mode) == 0)
                    {
                        strQuery.Append("Q.BOOKING_TRN_SEA_FK=" + strPK);
                    }
                    else
                    {
                        strQuery.Append("Q.BOOKING_AIR_FK=" + strPK);
                    }

                    return (new WorkFlow()).GetDataTable(strQuery.ToString());
                }
                else
                {
                    return null;
                }
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

        public string UpdateTransactionHZSpcl(DataSet DS, string UserName)
        {
            OracleCommand SCD = null;
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;

            arrMessage.Clear();

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".PKG_BKG_TRN_HAZ_SPL_REQ.BKG_TRN_HAZ_SPL_REQ_UPD";

                var _with12 = selectCommand.Parameters;
                _with12.Clear();

                _with12.Add("BOOKING_TRNS_FK_IN", DS.Tables[0].Rows[0]["BOOKING_TRN_FK"]).Direction = ParameterDirection.Input;
                _with12.Add("HC_REQ_REF_NO_IN", getDefault(DS.Tables[0].Rows[0]["HC_REQ_REF_NO"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with12.Add("HC_REQUEST_DT_IN", System.DateTime.Today).Direction = ParameterDirection.Input;
                _with12.Add("BIZ_TYPE_IN", getDefault(DS.Tables[0].Rows[0]["BIZ_TYPE"], 0)).Direction = ParameterDirection.Input;
                _with12.Add("OUTER_PKG_TYPE_IN", getDefault(DS.Tables[0].Rows[0]["OUTER_PKG_TYPE"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with12.Add("INNER_PKG_TYPE_IN", getDefault(DS.Tables[0].Rows[0]["INNER_PKG_TYPE"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with12.Add("MIN_TEMP_IN", getDefault(DS.Tables[0].Rows[0]["MIN_TEMP"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with12.Add("MIN_TEMP_UOM_IN", getDefault(DS.Tables[0].Rows[0]["MIN_TEMP_UOM"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with12.Add("MAX_TEMP_IN", getDefault(DS.Tables[0].Rows[0]["MAX_TEMP"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with12.Add("MAX_TEMP_UOM_IN", getDefault(DS.Tables[0].Rows[0]["MAX_TEMP_UOM"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with12.Add("FLASH_PNT_TEMP_IN", getDefault(DS.Tables[0].Rows[0]["FLASH_PNT_TEMP"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with12.Add("FLASH_PNT_TEMP_UOM_IN", getDefault(DS.Tables[0].Rows[0]["FLASH_PNT_TEMP_UOM"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with12.Add("HC_DESCRIPTION_IN", getDefault(DS.Tables[0].Rows[0]["HC_DESCRIPTION"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with12.Add("CHEM_GIVING_RISE_TO_IN", getDefault(DS.Tables[0].Rows[0]["CHEM_GIVING_RISE_TO"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with12.Add("GROSS_WT_IN_KG_IN", getDefault(DS.Tables[0].Rows[0]["GROSS_WT_IN_KG"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with12.Add("NET_WT_IN_KG_IN", getDefault(DS.Tables[0].Rows[0]["NET_WT_IN_KG"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with12.Add("IMCO_CLASS_IN", getDefault(DS.Tables[0].Rows[0]["IMCO_CLASS"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with12.Add("UN_NO_IN", getDefault(DS.Tables[0].Rows[0]["UN_NO"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with12.Add("IMO_SURCHARGE_IN", getDefault(DS.Tables[0].Rows[0]["IMO_SURCHARGE"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with12.Add("SURCHARGE_AMT_IN", getDefault(DS.Tables[0].Rows[0]["SURCHARGE_AMT"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with12.Add("SECONDARY_CLASS_IN", getDefault(DS.Tables[0].Rows[0]["SECONDARY_CLASS"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with12.Add("EMS_NO_IN", getDefault(DS.Tables[0].Rows[0]["EMS_NO"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with12.Add("MFAG_NO_IN", getDefault(DS.Tables[0].Rows[0]["MFAG_NO"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with12.Add("FLASH_POINT_IN_CG_IN", getDefault(DS.Tables[0].Rows[0]["FLASH_POINT_IN_CG"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with12.Add("IS_CHEMICAL_IN", getDefault(DS.Tables[0].Rows[0]["IS_CHEMICAL"], 0)).Direction = ParameterDirection.Input;
                _with12.Add("IS_MARINE_POLLUTTANT_IN", getDefault(DS.Tables[0].Rows[0]["IS_MARINE_POLLUTTANT"], 0)).Direction = ParameterDirection.Input;
                _with12.Add("IS_OUTER_PKG_UNTEST_IN", getDefault(DS.Tables[0].Rows[0]["IS_OUTER_PKG_UNTEST"], 0)).Direction = ParameterDirection.Input;
                _with12.Add("PACKING_GROUP_IN", getDefault(DS.Tables[0].Rows[0]["PACKING_GROUP"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with12.Add("REQUESTED_BY_FK_IN", getDefault(DS.Tables[0].Rows[0]["REQUESTED_BY_FK"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with12.Add("COMMENTS_IN", getDefault(DS.Tables[0].Rows[0]["COMMENTS"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with12.Add("STATUS_IN", getDefault(DS.Tables[0].Rows[0]["STATUS"], 0)).Direction = ParameterDirection.Input;
                _with12.Add("APPROVED_BY_FK_IN", getDefault(DS.Tables[0].Rows[0]["APPROVED_BY_FK"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with12.Add("APPROVED_DT_IN", System.DateTime.Today).Direction = ParameterDirection.Input;
                _with12.Add("CREATED_BY_FK_IN", getDefault(DS.Tables[0].Rows[0]["CREATED_BY_FK"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with12.Add("CREATED_DT_IN", System.DateTime.Today).Direction = ParameterDirection.Input;
                _with12.Add("LAST_MODIFIED_BY_FK_IN", getDefault(DS.Tables[0].Rows[0]["LAST_MODIFIED_BY_FK"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with12.Add("LAST_MODIFIED_DT_IN", System.DateTime.Today).Direction = ParameterDirection.Input;
                _with12.Add("VERSION_NO_IN", getDefault(DS.Tables[0].Rows[0]["VERSION_NO"], 0)).Direction = ParameterDirection.Input;
                _with12.Add("APPROVING_LOC_FK_IN", getDefault(DS.Tables[0].Rows[0]["APPROVING_LOC_FK"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with12.Add("PSA_CODE_IN", getDefault(DS.Tables[0].Rows[0]["PSA_CODE"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with12.Add("FILE_CONTENTS_IN", getDefault(DS.Tables[0].Rows[0]["FILE_CONTENTS"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with12.Add("FILE_NAME_IN", getDefault(DS.Tables[0].Rows[0]["FILE_NAME"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with12.Add("SCRATCH_BKG_TRN_FK_IN", getDefault(DS.Tables[0].Rows[0]["SCRATCH_BKG_TRN_FK"], DBNull.Value)).Direction = ParameterDirection.Input;

                _with12.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.ExecuteNonQuery();

                return "0";
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


        public Int16 CheckUpdate(Int16 PK, Int16 Type)
        {
            try
            {
                string strSql = null;
                string Check = null;
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();

                if (Type == 1)
                {
                    strBuilder.Append(" SELECT booking_hc_req_pk");
                    strBuilder.Append(" FROM ");
                    strBuilder.Append(" syn_ebk_t_bkg_haz ");
                    strBuilder.Append(" WHERE ");
                    strBuilder.Append(" booking_trn_fk= " + PK);
                }
                if (Type == 2)
                {
                    strBuilder.Append(" SELECT reefer_req_pk");
                    strBuilder.Append(" FROM ");
                    strBuilder.Append(" syn_ebk_t_bkg_reff   ");
                    strBuilder.Append(" WHERE ");
                    strBuilder.Append(" booking_trn_fk= " + PK);
                }
                if (Type == 3)
                {
                    strBuilder.Append(" SELECT booking_ood_req_pk");
                    strBuilder.Append(" FROM ");
                    strBuilder.Append(" syn_ebk_t_bkg_odc ");
                    strBuilder.Append(" WHERE ");
                    strBuilder.Append(" booking_trn_fk= " + PK);
                }


                Check = objWF.ExecuteScaler(strBuilder.ToString());
                if (!string.IsNullOrEmpty(Check))
                {
                    return 1;
                }
                else
                {
                    return 0;
                }
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

        public Int16 CheckUpdate_Qbso(Int16 PK, Int16 Type)
        {
            try
            {
                string strSql = null;
                string Check = null;
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();

                if (Type == 1)
                {
                    strBuilder.Append(" SELECT BKG_TRN_SEA_HAZ_SPL_PK");
                    strBuilder.Append(" FROM ");
                    strBuilder.Append(" bkg_trn_sea_haz_spl_req ");
                    strBuilder.Append(" WHERE ");
                    strBuilder.Append(" BOOKING_TRN_SEA_FK= " + PK);
                }
                if (Type == 2)
                {
                    strBuilder.Append(" SELECT bkg_trn_sea_ref_spl_pk");
                    strBuilder.Append(" FROM ");
                    strBuilder.Append(" bkg_trn_sea_ref_spl_req   ");
                    strBuilder.Append(" WHERE ");
                    strBuilder.Append(" booking_trn_sea_fk= " + PK);
                }
                if (Type == 3)
                {
                    strBuilder.Append(" SELECT bkg_trn_sea_odc_spl_pk");
                    strBuilder.Append(" FROM ");
                    strBuilder.Append(" bkg_trn_sea_odc_spl_req ");
                    strBuilder.Append(" WHERE ");
                    strBuilder.Append(" booking_trn_sea_fk= " + PK);
                }


                Check = objWF.ExecuteScaler(strBuilder.ToString());
                if (!string.IsNullOrEmpty(Check))
                {
                    return 1;
                }
                else
                {
                    return 0;
                }
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

        public DataSet GetDatasetHAZ(Int16 PK)
        {
            try
            {
                string strSql = null;
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();

                strBuilder.Append(" SELECT *");
                strBuilder.Append(" FROM ");
                strBuilder.Append(" syn_ebk_t_bkg_haz ");
                strBuilder.Append(" WHERE ");
                strBuilder.Append(" booking_trn_fk= " + PK);

                return objWF.GetDataSet(strBuilder.ToString());
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

        public DataSet GetDatasetREF(Int16 PK)
        {
            try
            {
                string strSql = null;
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();

                strBuilder.Append(" SELECT *");
                strBuilder.Append(" FROM ");
                strBuilder.Append(" syn_ebk_t_bkg_reff ");
                strBuilder.Append(" WHERE ");
                strBuilder.Append(" booking_trn_fk= " + PK);

                return objWF.GetDataSet(strBuilder.ToString());
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

        public DataSet GetDatasetODC(Int16 PK)
        {
            try
            {
                string strSql = null;
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();

                strBuilder.Append(" SELECT *");
                strBuilder.Append(" FROM ");
                strBuilder.Append(" syn_ebk_t_bkg_odc ");
                strBuilder.Append(" WHERE ");
                strBuilder.Append(" booking_trn_fk= " + PK);

                return objWF.GetDataSet(strBuilder.ToString());
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


        public string UpdateTransactionHZSpclNew(DataSet DS, string UserName)
        {
            OracleCommand SCD = null;
            OracleCommand selectCommand = new OracleCommand();
            string surChrg = null;
            string strReturn = null;
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".PKG_BKG_TRN_HAZ_SPL_REQ.BKG_TRN_HAZ_SPL_REQ_UPD_QBSO";
                var _with13 = selectCommand.Parameters;
                _with13.Clear();
                _with13.Add("BOOKING_TRNS_FK_IN", DS.Tables[0].Rows[0]["BOOKING_TRN_FK"]).Direction = ParameterDirection.Input;
                _with13.Add("BIZ_TYPE_IN", getDefault(DS.Tables[0].Rows[0]["BIZ_TYPE"], 0)).Direction = ParameterDirection.Input;
                _with13.Add("OUTER_PKG_TYPE_IN", getDefault(DS.Tables[0].Rows[0]["OUTER_PKG_TYPE"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with13.Add("INNER_PKG_TYPE_IN", getDefault(DS.Tables[0].Rows[0]["INNER_PKG_TYPE"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with13.Add("MIN_TEMP_IN", getDefault(DS.Tables[0].Rows[0]["MIN_TEMP"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with13.Add("MIN_TEMP_UOM_IN", getDefault(DS.Tables[0].Rows[0]["MIN_TEMP_UOM"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with13.Add("MAX_TEMP_IN", getDefault(DS.Tables[0].Rows[0]["MAX_TEMP"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with13.Add("MAX_TEMP_UOM_IN", getDefault(DS.Tables[0].Rows[0]["MAX_TEMP_UOM"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with13.Add("FLASH_PNT_TEMP_IN", getDefault(DS.Tables[0].Rows[0]["FLASH_PNT_TEMP"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with13.Add("FLASH_PNT_TEMP_UOM_IN", getDefault(DS.Tables[0].Rows[0]["FLASH_PNT_TEMP_UOM"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with13.Add("IMCO_CLASS_IN", getDefault(DS.Tables[0].Rows[0]["IMCO_CLASS"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with13.Add("UN_NO_IN", getDefault(DS.Tables[0].Rows[0]["UN_NO"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with13.Add("IMO_SURCHARGE_IN", getDefault(DS.Tables[0].Rows[0]["IMO_SURCHARGE"], DBNull.Value)).Direction = ParameterDirection.Input;

                if (string.IsNullOrEmpty(DS.Tables[0].Rows[0]["SURCHARGE_AMT"].ToString()))
                {
                    surChrg = "0";
                }
                else
                {
                    surChrg = DS.Tables[0].Rows[0]["SURCHARGE_AMT"].ToString();
                }

                _with13.Add("SURCHARGE_AMT_IN", getDefault(surChrg, 0)).Direction = ParameterDirection.Input;
                _with13.Add("EMS_NO_IN", getDefault(DS.Tables[0].Rows[0]["EMS_NO"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with13.Add("IS_MARINE_POLLUTTANT_IN", getDefault(DS.Tables[0].Rows[0]["IS_MARINE_POLLUTTANT"], 0)).Direction = ParameterDirection.Input;
                _with13.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.ExecuteNonQuery();

                return "0";
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
        public string SaveTransactionHZSpclNew(DataSet DS, string UserName)
        {
            OracleCommand SCD = null;
            OracleCommand selectCommand = new OracleCommand();
            string surChrg = null;
            string strReturn = null;
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".PKG_BKG_TRN_HAZ_SPL_REQ.BKG_TRN_HAZ_SPL_REQ_INS";
                var _with14 = selectCommand.Parameters;
                _with14.Clear();
                _with14.Add("BOOKING_TRNS_FK", DS.Tables[0].Rows[0]["BOOKING_TRN_FK"]).Direction = ParameterDirection.Input;
                _with14.Add("BIZ_TYPE", getDefault(DS.Tables[0].Rows[0]["BIZ_TYPE"], 0)).Direction = ParameterDirection.Input;
                _with14.Add("OUTER_PKG_TYPE", getDefault(DS.Tables[0].Rows[0]["OUTER_PKG_TYPE"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with14.Add("INNER_PKG_TYPE", getDefault(DS.Tables[0].Rows[0]["INNER_PKG_TYPE"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with14.Add("MIN_TEMP", getDefault(DS.Tables[0].Rows[0]["MIN_TEMP"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with14.Add("MIN_TEMP_UOM", getDefault(DS.Tables[0].Rows[0]["MIN_TEMP_UOM"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with14.Add("MAX_TEMP", getDefault(DS.Tables[0].Rows[0]["MAX_TEMP"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with14.Add("MAX_TEMP_UOM", getDefault(DS.Tables[0].Rows[0]["MAX_TEMP_UOM"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with14.Add("FLASH_PNT_TEMP", getDefault(DS.Tables[0].Rows[0]["FLASH_PNT_TEMP"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with14.Add("FLASH_PNT_TEMP_UOM", getDefault(DS.Tables[0].Rows[0]["FLASH_PNT_TEMP_UOM"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with14.Add("IMCO_CLASS", getDefault(DS.Tables[0].Rows[0]["IMCO_CLASS"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with14.Add("UN_NO", getDefault(DS.Tables[0].Rows[0]["UN_NO"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with14.Add("IMO_SURCHARGE", getDefault(DS.Tables[0].Rows[0]["IMO_SURCHARGE"], DBNull.Value)).Direction = ParameterDirection.Input;

                if (string.IsNullOrEmpty(DS.Tables[0].Rows[0]["SURCHARGE_AMT"].ToString()))
                {
                    surChrg = "0";
                }
                else
                {
                    surChrg = DS.Tables[0].Rows[0]["SURCHARGE_AMT"].ToString();
                }

                _with14.Add("SURCHARGE_AMT", getDefault(surChrg, 0)).Direction = ParameterDirection.Input;
                _with14.Add("EMS_NO", getDefault(DS.Tables[0].Rows[0]["EMS_NO"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with14.Add("IS_MARINE_POLLUTTANT", getDefault(DS.Tables[0].Rows[0]["IS_MARINE_POLLUTTANT"], 0)).Direction = ParameterDirection.Input;
                _with14.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
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
        public string UpdateTransactionODCSpcl(DataSet DS, string UserName)
        {
            OracleCommand SCD = null;
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".PKG_BKG_TRN_ODC_SPL_REQ.BKG_TRN_ODC_SPL_REQ_UPD";
                var _with15 = selectCommand.Parameters;
                _with15.Clear();
                _with15.Add("BOOKING_OOD_REQ_PK_IN", DS.Tables[0].Rows[0]["BOOKING_OOD_REQ_PK"]).Direction = ParameterDirection.Input;
                _with15.Add("OOD_REQ_REF_NO_IN", getDefault(DS.Tables[0].Rows[0]["OOD_REQ_REF_NO"], 0)).Direction = ParameterDirection.Input;
                _with15.Add("OOD_REQ_DT_IN", System.DateTime.Today).Direction = ParameterDirection.Input;
                _with15.Add("PROV_BOOKING_FK_IN", getDefault(DS.Tables[0].Rows[0]["PROV_BOOKING_FK"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with15.Add("BOOKING_TRN_FK_IN", getDefault(DS.Tables[0].Rows[0]["BOOKING_TRN_FK"], 0)).Direction = ParameterDirection.Input;
                _with15.Add("LENGTH_IN", getDefault(DS.Tables[0].Rows[0]["LENGTH"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with15.Add("LENGTH_UOM_MST_FK_IN", getDefault(DS.Tables[0].Rows[0]["LENGTH_UOM_MST_FK"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with15.Add("HEIGHT_IN", getDefault(DS.Tables[0].Rows[0]["HEIGHT"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with15.Add("HEIGHT_UOM_MST_FK_IN", getDefault(DS.Tables[0].Rows[0]["HEIGHT_UOM_MST_FK"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with15.Add("WIDTH_IN", getDefault(DS.Tables[0].Rows[0]["WIDTH"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with15.Add("WIDTH_UOM_MST_FK_IN", getDefault(DS.Tables[0].Rows[0]["WIDTH_UOM_MST_FK"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with15.Add("CONTAINER_NO_IN", getDefault(DS.Tables[0].Rows[0]["CONTAINER_NO"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with15.Add("CONTAINER_TYPE_MST_FK_IN", getDefault(DS.Tables[0].Rows[0]["CONTAINER_TYPE_MST_FK"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with15.Add("OH_IN", getDefault(DS.Tables[0].Rows[0]["OH"], 0)).Direction = ParameterDirection.Input;
                _with15.Add("OWL_IN", getDefault(DS.Tables[0].Rows[0]["OWL"], 0)).Direction = ParameterDirection.Input;
                _with15.Add("OWR_IN", getDefault(DS.Tables[0].Rows[0]["OWR"], 0)).Direction = ParameterDirection.Input;
                _with15.Add("OLF_IN", getDefault(DS.Tables[0].Rows[0]["OLF"], 0)).Direction = ParameterDirection.Input;
                _with15.Add("OLB_IN", getDefault(DS.Tables[0].Rows[0]["OLB"], 0)).Direction = ParameterDirection.Input;
                _with15.Add("DIMENSION_UNIT_FK_IN", getDefault(DS.Tables[0].Rows[0]["DIMENSION_UNIT_FK"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with15.Add("WEIGHT_UNIT_FK_IN", getDefault(DS.Tables[0].Rows[0]["WEIGHT_UNIT_FK"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with15.Add("VOLUME_IN", getDefault(DS.Tables[0].Rows[0]["VOLUME"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with15.Add("VOLUME_UOM_MST_FK_IN", getDefault(DS.Tables[0].Rows[0]["VOLUME_UOM_MST_FK"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with15.Add("SLINGS_PROVIDED_IN", getDefault(DS.Tables[0].Rows[0]["SLINGS_PROVIDED"], 0)).Direction = ParameterDirection.Input;
                _with15.Add("CHAINS_PROVIDED_IN", getDefault(DS.Tables[0].Rows[0]["CHAINS_PROVIDED"], 0)).Direction = ParameterDirection.Input;
                _with15.Add("SLOTS_USED_IN", getDefault(DS.Tables[0].Rows[0]["SLOTS_USED"], 0)).Direction = ParameterDirection.Input;
                _with15.Add("TOT_SLOT_LOSS_IN", getDefault(DS.Tables[0].Rows[0]["TOT_SLOT_LOSS"], 0)).Direction = ParameterDirection.Input;
                _with15.Add("LOSS_QUANTITY_IN", getDefault(DS.Tables[0].Rows[0]["LOSS_QUANTITY"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with15.Add("SLOT_RATE_IN", getDefault(DS.Tables[0].Rows[0]["SLOT_RATE"], 0)).Direction = ParameterDirection.Input;
                _with15.Add("CURRENCY_MST_FK_IN", getDefault(DS.Tables[0].Rows[0]["CURRENCY_MST_FK"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with15.Add("REQUESTED_BY_IN", getDefault(DS.Tables[0].Rows[0]["REQUESTED_BY"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with15.Add("OOD_REQ_STATUS_IN", getDefault(DS.Tables[0].Rows[0]["OOD_REQ_STATUS"], 1)).Direction = ParameterDirection.Input;
                _with15.Add("STOWAGE_LOCATION_IN", getDefault(DS.Tables[0].Rows[0]["STOWAGE_LOCATION"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with15.Add("APPROVED_BY_FK_IN", getDefault(DS.Tables[0].Rows[0]["APPROVED_BY_FK"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with15.Add("APPROVED_DATE_IN", System.DateTime.Today).Direction = ParameterDirection.Input;
                _with15.Add("COMMENTS_IN", getDefault(DS.Tables[0].Rows[0]["COMMENTS"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with15.Add("FILE_NAME_IN", getDefault(DS.Tables[0].Rows[0]["FILE_NAME"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with15.Add("FILE_CONTENTS_IN", getDefault(DS.Tables[0].Rows[0]["FILE_CONTENTS"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with15.Add("CREATED_BY_FK_IN", getDefault(DS.Tables[0].Rows[0]["CREATED_BY_FK"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with15.Add("CREATED_DT_IN", System.DateTime.Today).Direction = ParameterDirection.Input;
                _with15.Add("LAST_MODIFIED_BY_FK_IN", getDefault(DS.Tables[0].Rows[0]["LAST_MODIFIED_BY_FK"], 0)).Direction = ParameterDirection.Input;
                _with15.Add("LAST_MODIFIED_DT_IN", System.DateTime.Today).Direction = ParameterDirection.Input;
                _with15.Add("VERSION_NO_IN", getDefault(DS.Tables[0].Rows[0]["VERSION_NO"], 0)).Direction = ParameterDirection.Input;
                _with15.Add("APPROVING_LOC_FK_IN", getDefault(DS.Tables[0].Rows[0]["APPROVING_LOC_FK"], 0)).Direction = ParameterDirection.Input;
                _with15.Add("WEIGHT_IN_TON_IN", getDefault(DS.Tables[0].Rows[0]["WEIGHT_IN_TON"], 0)).Direction = ParameterDirection.Input;
                _with15.Add("SCRATCH_BKG_TRN_FK_IN", getDefault(DS.Tables[0].Rows[0]["SCRATCH_BKG_TRN_FK"], 0)).Direction = ParameterDirection.Input;
                _with15.Add("BIZTYPE_IN", getDefault(DS.Tables[0].Rows[0]["BIZTYPE"], 0)).Direction = ParameterDirection.Input;
                _with15.Add("NO_OF_SLOTS", getDefault(DS.Tables[0].Rows[0]["BIZTYPE"], 0)).Direction = ParameterDirection.Input;
                _with15.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.ExecuteNonQuery();

                return "0";
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

        public string UpdateTransactionREFSpcl(DataSet DS, string UserName)
        {
            OracleCommand SCD = null;
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".PKG_BKG_TRN_REF_SPL_REQ.BKG_TRN_REF_SPL_REQ_UPD";
                var _with16 = selectCommand.Parameters;
                _with16.Clear();
                _with16.Add("REEFER_REQ_PK_IN", DS.Tables[0].Rows[0]["REEFER_REQ_PK"]).Direction = ParameterDirection.Input;
                _with16.Add("REEFER_REF_NO_IN", getDefault(DS.Tables[0].Rows[0]["REEFER_REF_NO"], 0)).Direction = ParameterDirection.Input;
                _with16.Add("REEFER_REQ_DT_IN", System.DateTime.Today).Direction = ParameterDirection.Input;
                _with16.Add("BOOKING_TRN_FK_IN", getDefault(DS.Tables[0].Rows[0]["BOOKING_TRN_FK"], 0)).Direction = ParameterDirection.Input;
                _with16.Add("BIZ_TYPE_IN", getDefault(DS.Tables[0].Rows[0]["BIZ_TYPE"], 0)).Direction = ParameterDirection.Input;
                _with16.Add("PACK_TYPE_MST_FK_IN", getDefault(DS.Tables[0].Rows[0]["PACK_TYPE_MST_FK"], 0)).Direction = ParameterDirection.Input;
                _with16.Add("PACK_COUNT_IN", getDefault(DS.Tables[0].Rows[0]["PACK_COUNT"], 0)).Direction = ParameterDirection.Input;
                _with16.Add("HAULAGE_IN", getDefault(DS.Tables[0].Rows[0]["HAULAGE"], 0)).Direction = ParameterDirection.Input;
                _with16.Add("GENSET_IN", getDefault(DS.Tables[0].Rows[0]["GENSET"], 0)).Direction = ParameterDirection.Input;
                _with16.Add("CO2_IN", getDefault(DS.Tables[0].Rows[0]["CO2"], 0)).Direction = ParameterDirection.Input;
                _with16.Add("O2_IN", getDefault(DS.Tables[0].Rows[0]["O2"], 0)).Direction = ParameterDirection.Input;
                _with16.Add("REQ_SET_TEMP_IN", getDefault(DS.Tables[0].Rows[0]["REQ_SET_TEMP"], 0)).Direction = ParameterDirection.Input;
                _with16.Add("REQ_SET_TEMP_UOM_IN", getDefault(DS.Tables[0].Rows[0]["REQ_SET_TEMP_UOM"], 0)).Direction = ParameterDirection.Input;
                _with16.Add("AIR_VENTILATION_IN", getDefault(DS.Tables[0].Rows[0]["AIR_VENTILATION"], 0)).Direction = ParameterDirection.Input;
                _with16.Add("AIR_VENTILATION_UOM_IN", getDefault(DS.Tables[0].Rows[0]["AIR_VENTILATION_UOM"], 0)).Direction = ParameterDirection.Input;
                _with16.Add("DEHUMIDIFIER_IN", getDefault(DS.Tables[0].Rows[0]["DEHUMIDIFIER"], 0)).Direction = ParameterDirection.Input;
                _with16.Add("HUMIDITY_FACTOR_IN", getDefault(DS.Tables[0].Rows[0]["HUMIDITY_FACTOR"], 0)).Direction = ParameterDirection.Input;
                _with16.Add("FLOORDRAINS_IN", getDefault(DS.Tables[0].Rows[0]["FLOORDRAINS"], 0)).Direction = ParameterDirection.Input;
                _with16.Add("DEFROSTING_INTERVAL_IN", getDefault(DS.Tables[0].Rows[0]["DEFROSTING_INTERVAL"], 0)).Direction = ParameterDirection.Input;
                _with16.Add("REQUESTED_BY_FK_IN", getDefault(DS.Tables[0].Rows[0]["REQUESTED_BY_FK"], 0)).Direction = ParameterDirection.Input;
                _with16.Add("COMMENTS_IN", getDefault(DS.Tables[0].Rows[0]["COMMENTS"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with16.Add("RF_STATUS_IN", getDefault(DS.Tables[0].Rows[0]["RF_STATUS"], 0)).Direction = ParameterDirection.Input;
                _with16.Add("APPROVED_BY_FK_IN", getDefault(DS.Tables[0].Rows[0]["APPROVED_BY_FK"], 0)).Direction = ParameterDirection.Input;
                _with16.Add("APPROVED_DT_IN", System.DateTime.Today).Direction = ParameterDirection.Input;
                _with16.Add("CREATED_BY_FK_IN", getDefault(DS.Tables[0].Rows[0]["CREATED_BY_FK"], 0)).Direction = ParameterDirection.Input;
                _with16.Add("CREATED_DT_IN", System.DateTime.Today).Direction = ParameterDirection.Input;
                _with16.Add("LAST_MODIFIED_BY_FK_IN", getDefault(DS.Tables[0].Rows[0]["LAST_MODIFIED_BY_FK"], 0)).Direction = ParameterDirection.Input;
                _with16.Add("LAST_MODIFIED_DT_IN", System.DateTime.Today).Direction = ParameterDirection.Input;
                _with16.Add("Version_No_IN", getDefault(DS.Tables[0].Rows[0]["Version_No"], 0)).Direction = ParameterDirection.Input;
                _with16.Add("APPROVING_LOC_FK_IN", getDefault(DS.Tables[0].Rows[0]["APPROVING_LOC_FK"], 0)).Direction = ParameterDirection.Input;
                _with16.Add("FILE_NAME_IN", getDefault(DS.Tables[0].Rows[0]["FILE_NAME"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with16.Add("FILE_CONTENTS_IN", getDefault(DS.Tables[0].Rows[0]["FILE_CONTENTS"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with16.Add("SCRATCH_BKG_TRN_FK_IN", getDefault(DS.Tables[0].Rows[0]["SCRATCH_BKG_TRN_FK"], 0)).Direction = ParameterDirection.Input;

                _with16.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.ExecuteNonQuery();

                return "0";
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


        public string UpdateTransactionODCSpclNewQBSO(DataSet DS, string UserName)
        {
            OracleCommand SCD = null;
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".PKG_BKG_TRN_ODC_SPL_REQ.BKG_TRN_ODC_SPL_REQ_INS";
                var _with17 = selectCommand.Parameters;
                _with17.Clear();
                _with17.Add("BOOKING_OOD_REQ_PK", DS.Tables[0].Rows[0]["BOOKING_OOD_REQ_PK"]).Direction = ParameterDirection.Input;
                _with17.Add("BOOKING_TRN_FK", getDefault(DS.Tables[0].Rows[0]["BOOKING_TRN_FK"], 0)).Direction = ParameterDirection.Input;
                _with17.Add("LENGTH", getDefault(DS.Tables[0].Rows[0]["LENGTH"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with17.Add("LENGTH_UOM_MST_FK", getDefault(DS.Tables[0].Rows[0]["LENGTH_UOM_MST_FK"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with17.Add("HEIGHT", getDefault(DS.Tables[0].Rows[0]["HEIGHT"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with17.Add("HEIGHT_UOM_MST_FK", getDefault(DS.Tables[0].Rows[0]["HEIGHT_UOM_MST_FK"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with17.Add("WIDTH", getDefault(DS.Tables[0].Rows[0]["WIDTH"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with17.Add("WIDTH_UOM_MST_FK", getDefault(DS.Tables[0].Rows[0]["WIDTH_UOM_MST_FK"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with17.Add("WEIGHT_IN_TON", getDefault(DS.Tables[0].Rows[0]["WEIGHT_IN_TON"], 0)).Direction = ParameterDirection.Input;
                _with17.Add("WEIGHT_UNIT_FK", getDefault(DS.Tables[0].Rows[0]["WEIGHT_UNIT_FK"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with17.Add("VOLUME", getDefault(DS.Tables[0].Rows[0]["VOLUME"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with17.Add("VOLUME_UOM_MST_FK", getDefault(DS.Tables[0].Rows[0]["VOLUME_UOM_MST_FK"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with17.Add("TOT_SLOT_LOSS", getDefault(DS.Tables[0].Rows[0]["TOT_SLOT_LOSS"], 0)).Direction = ParameterDirection.Input;
                _with17.Add("LOSS_QUANTITY", getDefault(DS.Tables[0].Rows[0]["LOSS_QUANTITY"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with17.Add("OOD_REQ_STATUS", getDefault(DS.Tables[0].Rows[0]["OOD_REQ_STATUS"], 1)).Direction = ParameterDirection.Input;
                _with17.Add("NO_OF_SLOTS", getDefault(DS.Tables[0].Rows[0]["NO_OF_SLOTS"], 1)).Direction = ParameterDirection.Input;
                _with17.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
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
        public string SaveTransactionODCSpclNew(DataSet DS, string UserName)
        {
            OracleCommand SCD = null;
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".PKG_BKG_TRN_ODC_SPL_REQ.BKG_TRN_ODC_SPL_REQ_INS";
                var _with18 = selectCommand.Parameters;
                _with18.Clear();
                _with18.Add("BOOKING_OOD_REQ_PK", DS.Tables[0].Rows[0]["BOOKING_OOD_REQ_PK"]).Direction = ParameterDirection.Input;
                _with18.Add("BOOKING_TRN_FK", getDefault(DS.Tables[0].Rows[0]["BOOKING_TRN_FK"], 0)).Direction = ParameterDirection.Input;
                _with18.Add("LENGTH", getDefault(DS.Tables[0].Rows[0]["LENGTH"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with18.Add("LENGTH_UOM_MST_FK", getDefault(DS.Tables[0].Rows[0]["LENGTH_UOM_MST_FK"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with18.Add("HEIGHT", getDefault(DS.Tables[0].Rows[0]["HEIGHT"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with18.Add("HEIGHT_UOM_MST_FK", getDefault(DS.Tables[0].Rows[0]["HEIGHT_UOM_MST_FK"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with18.Add("WIDTH", getDefault(DS.Tables[0].Rows[0]["WIDTH"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with18.Add("WIDTH_UOM_MST_FK", getDefault(DS.Tables[0].Rows[0]["WIDTH_UOM_MST_FK"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with18.Add("WEIGHT_IN_TON", getDefault(DS.Tables[0].Rows[0]["WEIGHT_IN_TON"], 0)).Direction = ParameterDirection.Input;
                _with18.Add("WEIGHT_UNIT_FK", getDefault(DS.Tables[0].Rows[0]["WEIGHT_UNIT_FK"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with18.Add("VOLUME", getDefault(DS.Tables[0].Rows[0]["VOLUME"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with18.Add("VOLUME_UOM_MST_FK", getDefault(DS.Tables[0].Rows[0]["VOLUME_UOM_MST_FK"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with18.Add("TOT_SLOT_LOSS", getDefault(DS.Tables[0].Rows[0]["TOT_SLOT_LOSS"], 0)).Direction = ParameterDirection.Input;
                _with18.Add("LOSS_QUANTITY", getDefault(DS.Tables[0].Rows[0]["LOSS_QUANTITY"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with18.Add("OOD_REQ_STATUS", getDefault(DS.Tables[0].Rows[0]["OOD_REQ_STATUS"], 1)).Direction = ParameterDirection.Input;
                _with18.Add("NO_OF_SLOTS", getDefault(DS.Tables[0].Rows[0]["NO_OF_SLOTS"], 1)).Direction = ParameterDirection.Input;
                _with18.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
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

        public string UpdateTransactionHZSpclNewQBSO(DataSet DS, string UserName)
        {
            OracleCommand SCD = null;
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".PKG_BKG_TRN_REF_SPL_REQ.BKG_TRN_REF_SPL_REQ_UPD_QBSO";
                var _with19 = selectCommand.Parameters;
                _with19.Clear();
                _with19.Add("REEFER_REQ_PK_IN", DS.Tables[0].Rows[0]["REEFER_REQ_PK"]).Direction = ParameterDirection.Input;
                _with19.Add("BOOKING_TRN_FK_IN", getDefault(DS.Tables[0].Rows[0]["BOOKING_TRN_FK"], 0)).Direction = ParameterDirection.Input;
                _with19.Add("VENTILATION_IN", getDefault(DS.Tables[0].Rows[0]["VENTILATION"], 0)).Direction = ParameterDirection.Input;
                _with19.Add("AIR_COOL_METHOD_IN", getDefault(DS.Tables[0].Rows[0]["AIR_COOL_METHOD"], 0)).Direction = ParameterDirection.Input;
                _with19.Add("HUMIDITY_FACTOR_IN", getDefault(DS.Tables[0].Rows[0]["HUMIDITY_FACTOR"], 0)).Direction = ParameterDirection.Input;
                _with19.Add("IS_PERISHABLE_GOODS_IN", getDefault(DS.Tables[0].Rows[0]["IS_PERISHABLE_GOODS"], 0)).Direction = ParameterDirection.Input;
                _with19.Add("MIN_TEMP_IN_CG_IN", getDefault(DS.Tables[0].Rows[0]["MIN_TEMP_IN_CG"], 0)).Direction = ParameterDirection.Input;
                _with19.Add("MIN_TEMP_UOM_IN", getDefault(DS.Tables[0].Rows[0]["MIN_TEMP_UOM"], 0)).Direction = ParameterDirection.Input;
                _with19.Add("MAX_TEMP_IN_CG_IN", getDefault(DS.Tables[0].Rows[0]["MAX_TEMP_IN_CG"], 0)).Direction = ParameterDirection.Input;
                _with19.Add("MAX_TEMP_UOM_IN", getDefault(DS.Tables[0].Rows[0]["MAX_TEMP_UOM"], 0)).Direction = ParameterDirection.Input;
                _with19.Add("PACK_TYPE_MST_FK_IN", getDefault(DS.Tables[0].Rows[0]["PACK_TYPE_MST_FK"], 0)).Direction = ParameterDirection.Input;
                _with19.Add("PACK_COUNT_IN", getDefault(DS.Tables[0].Rows[0]["PACK_COUNT"], 0)).Direction = ParameterDirection.Input;
                _with19.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
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
        public string SaveTransactionREFSpclNew(DataSet DS, string UserName)
        {
            OracleCommand SCD = null;
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".PKG_BKG_TRN_REF_SPL_REQ.BKG_TRN_REF_SPL_REQ_INS_QBSO";
                var _with20 = selectCommand.Parameters;
                _with20.Clear();
                _with20.Add("REEFER_REQ_PK", DS.Tables[0].Rows[0]["REEFER_REQ_PK"]).Direction = ParameterDirection.Input;
                _with20.Add("BOOKING_TRN_FK", getDefault(DS.Tables[0].Rows[0]["BOOKING_TRN_FK"], 0)).Direction = ParameterDirection.Input;
                _with20.Add("VENTILATION", getDefault(DS.Tables[0].Rows[0]["VENTILATION"], 0)).Direction = ParameterDirection.Input;
                _with20.Add("AIR_COOL_METHOD", getDefault(DS.Tables[0].Rows[0]["AIR_COOL_METHOD"], 0)).Direction = ParameterDirection.Input;
                _with20.Add("HUMIDITY_FACTOR", getDefault(DS.Tables[0].Rows[0]["HUMIDITY_FACTOR"], 0)).Direction = ParameterDirection.Input;
                _with20.Add("IS_PERISHABLE_GOODS", getDefault(DS.Tables[0].Rows[0]["IS_PERISHABLE_GOODS"], 0)).Direction = ParameterDirection.Input;
                _with20.Add("MIN_TEMP_IN_CG", getDefault(DS.Tables[0].Rows[0]["MIN_TEMP_IN_CG"], 0)).Direction = ParameterDirection.Input;
                _with20.Add("MIN_TEMP_UOM", getDefault(DS.Tables[0].Rows[0]["MIN_TEMP_UOM"], 0)).Direction = ParameterDirection.Input;
                _with20.Add("MAX_TEMP_IN_CG", getDefault(DS.Tables[0].Rows[0]["MAX_TEMP_IN_CG"], 0)).Direction = ParameterDirection.Input;
                _with20.Add("MAX_TEMP_UOM", getDefault(DS.Tables[0].Rows[0]["MAX_TEMP_UOM"], 0)).Direction = ParameterDirection.Input;
                _with20.Add("PACK_TYPE_MST_FK", getDefault(DS.Tables[0].Rows[0]["PACK_TYPE_MST_FK"], 0)).Direction = ParameterDirection.Input;
                _with20.Add("PACK_COUNT", getDefault(DS.Tables[0].Rows[0]["PACK_COUNT"], 0)).Direction = ParameterDirection.Input;
                _with20.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
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
        public int FetchIMOUno(string COM, string IMO, string Unr)
        {
            OracleDataReader dr = null;
            string UNnr = null;
            string strQuery = "SELECT CO.IMDG_CLASS_CODE,CO.UN_NO,CO.COMMODITY_NAME FROM commodity_mst_tbl co WHERE co.commodity_id='" + COM + "'";
            try
            {
                dr = (new WorkFlow()).GetDataReader(strQuery);
                while (dr.Read())
                {
                    IMO = getDefault(dr["IMDG_CLASS_CODE"], "").ToString();
                    Unr = getDefault(dr["UN_NO"], "").ToString();
                    COM = dr["COMMODITY_NAME"].ToString();
                }
                return 1;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                return 0;
            }
            finally
            {
                dr.Close();
            }
        }
        #endregion

        #region "Update Data in QBSO system"
        public ArrayList SaveQBSOAirMain(OracleCommand insCommand, string BOOKING_REF_NO = "", string BOOKING_DATE = "", int CUST_CUSTOMER_MST_FK = 0, int CONS_CUSTOMER_MST_FK = 0, int PORT_MST_POD_FK = 0, int PORT_MST_POL_FK = 0, string SHIPMENT_DATE = "", int PACK_TYPE_MST_FK = 0, int CARGO_TYPE = 0,
        int PACK_COUNT = 0, string GROSS_WEIGHT = "", int NO_OF_BOXES = 0, string VOLUME_IN_CBM = "", string FLIGHT_NO = "", int AIRLINE_MST_FK = 0, int CARGO_MOVE_FK = 0, int PYMT_TYPE = 0, string CHARGEABLE_WEIGHT = "", int COMMODITY_GROUP_FK = 0,
        string DENSITY = "", string ContString = "", string CommString = "")
        {
            try
            {
                insCommand.CommandType = CommandType.StoredProcedure;
                insCommand.CommandText = objWF.MyUserName + ".PKG_EBK_BOOKING.QBSO_EBK_M_BOOKING_INS";
                var _with21 = insCommand.Parameters;
                _with21.Add("BOOKING_REF_NO", getDefault(BOOKING_REF_NO, DBNull.Value));
                _with21.Add("BOOKING_DATE", getDefault(BOOKING_DATE, DBNull.Value));
                _with21.Add("CUST_CUSTOMER_MST_FK", getDefault(CUST_CUSTOMER_MST_FK, DBNull.Value));
                _with21.Add("CONS_CUSTOMER_MST_FK", getDefault(CONS_CUSTOMER_MST_FK, DBNull.Value));
                _with21.Add("SHIPMENT_DATE", getDefault(SHIPMENT_DATE, DBNull.Value));
                _with21.Add("CARGO_TYPE", getDefault(CARGO_TYPE, DBNull.Value));
                _with21.Add("PACK_TYPE_MST_FK", getDefault(PACK_TYPE_MST_FK, DBNull.Value));
                _with21.Add("PACK_COUNT", getDefault(PACK_COUNT, DBNull.Value));
                _with21.Add("GROSS_WEIGHT", getDefault(GROSS_WEIGHT, DBNull.Value));
                _with21.Add("NO_OF_BOXES", getDefault(NO_OF_BOXES, DBNull.Value));
                _with21.Add("VOLUME_IN_CBM", getDefault(VOLUME_IN_CBM, DBNull.Value));
                _with21.Add("FLIGHT_NO", getDefault(FLIGHT_NO, DBNull.Value));
                _with21.Add("PORT_MST_POD_FK", getDefault(PORT_MST_POD_FK, DBNull.Value));
                _with21.Add("PORT_MST_POL_FK", getDefault(PORT_MST_POL_FK, DBNull.Value));
                _with21.Add("AIRLINE_MST_FK", getDefault(AIRLINE_MST_FK, DBNull.Value));
                _with21.Add("CARGO_MOVE_FK", getDefault(CARGO_MOVE_FK, DBNull.Value));
                _with21.Add("PYMT_TYPE", getDefault(PYMT_TYPE, DBNull.Value));
                _with21.Add("CHARGEABLE_WEIGHT", getDefault(CHARGEABLE_WEIGHT, DBNull.Value));
                _with21.Add("COMMODITY_GROUP_FK", getDefault(COMMODITY_GROUP_FK, DBNull.Value));
                _with21.Add("DENSITY", getDefault(DENSITY, DBNull.Value));
                _with21.Add("RETURN_VALUE", bookingQBSOPK).Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                insCommand.ExecuteNonQuery();
                bookingQBSOPK = Convert.ToInt32(insCommand.Parameters["E_BKG_ORDER_NR_PK"].Value);
                arrMessage = (ArrayList)SaveQBSOAirTrn(insCommand, bookingQBSOPK, 7, BOOKING_REF_NO, COMMODITY_GROUP_FK, CommString, "0.00", ContString, 0);
                arrMessage.Add("All data saved successfully");
                return arrMessage;
            }
            catch (OracleException oraexp)
            {
                arrMessage.Add(oraexp.Message);
                return arrMessage;
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public object SaveQBSOAirTrn(OracleCommand insCommand, int BOOKING_AIR_FK = 0, int TRANS_REFERED_FROM = 0, string TRANS_REF_NO = "", int COMMODITY_GROUP_FK = 0, string COMMODITY_MST_FK = "", string ALL_IN_TARIFF = "", string BASIS = "", int QUANTITY = 0)
        {
            try
            {
                Array strBasis = null;
                strBasis = BASIS.Split('~');
                Array strComm = null;
                strComm = COMMODITY_MST_FK.Split('$');
                int i = 0;
                insCommand.CommandType = CommandType.StoredProcedure;
                insCommand.CommandText = objWF.MyUserName + ".PKG_EBK_BOOKING.QBSO_EBK_M_BOOKING_INS";
                for (i = 0; i <= strComm.Length - 1; i++)
                {
                    var _with22 = insCommand.Parameters;
                    _with22.Add("BOOKING_AIR_FK", getDefault(BOOKING_AIR_FK, DBNull.Value));
                    _with22.Add("TRANS_REFERED_FROM", getDefault(TRANS_REFERED_FROM, DBNull.Value));
                    _with22.Add("TRANS_REF_NO", getDefault(TRANS_REF_NO, DBNull.Value));
                    _with22.Add("COMMODITY_GROUP_FK", getDefault(COMMODITY_GROUP_FK, DBNull.Value));
                    _with22.Add("COMMODITY_MST_FK", getDefault(strBasis.GetValue(1), DBNull.Value));
                    _with22.Add("ALL_IN_TARIFF", getDefault(ALL_IN_TARIFF, DBNull.Value));
                    _with22.Add("BASIS", getDefault(BASIS, DBNull.Value));
                    _with22.Add("QUANTITY", getDefault(QUANTITY, DBNull.Value));
                    _with22.Add("RETURN_VALUE", bookingQBSOTRNPk).Direction = ParameterDirection.Output;
                    insCommand.Parameters["E_BKG_ORDER_NR_PK"].SourceVersion = DataRowVersion.Current;
                    insCommand.ExecuteNonQuery();
                    bookingQBSOTRNPk = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
                }
                arrMessage.Add("All data saved successfully");
                return arrMessage;
            }
            catch (OracleException oraexp)
            {
                arrMessage.Add(oraexp.Message);
                return arrMessage;
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }
        #endregion

        #region "Get Booking Details"
        public DataSet GetBookingDetails(int BkgPk, string FromFlag = "BOOKING")
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with23 = objWK.MyCommand.Parameters;
                _with23.Add("BOOKING_MST_FK_IN", BkgPk).Direction = ParameterDirection.Input;
                _with23.Add("FROM_FLG_IN", FromFlag).Direction = ParameterDirection.Input;
                _with23.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("BOOKING_COMMINV_PACK_PKG", "FETCH_BKG_DETAILS");
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
        public DataSet GetCommInvDetails(int BkgPk, int CurrecnyFK)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with24 = objWK.MyCommand.Parameters;
                _with24.Add("BOOKING_MST_FK_IN", BkgPk).Direction = ParameterDirection.Input;
                _with24.Add("CURRENCY_FK_IN", CurrecnyFK).Direction = ParameterDirection.Input;
                _with24.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("BOOKING_COMMINV_PACK_PKG", "FETCH_COMMINV_DETAILS");
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
        public DataSet GetInvSumDetails(int BkgPk)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with25 = objWK.MyCommand.Parameters;
                _with25.Add("BOOKING_MST_FK_IN", BkgPk).Direction = ParameterDirection.Input;
                _with25.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("BOOKING_COMMINV_PACK_PKG", "FETCH_INVSUM_DETAILS");
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
        public DataSet GetPackDetails(int BkgPk)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with26 = objWK.MyCommand.Parameters;
                _with26.Add("BOOKING_MST_FK_IN", BkgPk).Direction = ParameterDirection.Input;
                _with26.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("BOOKING_COMMINV_PACK_PKG", "FETCH_PACK_DETAILS");
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
        #region "Get Operator EmailID"
        public DataSet GetCountryPK()
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append(" SELECT CMT.COUNTRY_MST_PK, CMT.COUNTRY_ID");
            sb.Append("  FROM COUNTRY_MST_TBL CMT, LOCATION_MST_TBL LMT");
            sb.Append(" WHERE LMT.COUNTRY_MST_FK = CMT.COUNTRY_MST_PK");
            sb.Append("  AND LMT.LOCATION_MST_PK =" + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
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

        public long GetBKGPK(long JOBPK)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append(" SELECT NVL(JC.BOOKING_MST_FK,0) FROM JOB_CARD_TRN JC WHERE JC.JOB_CARD_TRN_PK =  " + JOBPK);
            try
            {
                return Convert.ToInt64(objWF.ExecuteScaler(sb.ToString()));
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
        public ArrayList SaveCommInv(DataSet dsCommInv, DataSet dsPackList, long Booking_Mst_Fk, Int32 BizType, System.DateTime CommInvDt, string Remarks = "", string Pack_Remarks = "", int NoOfCartons = 0, int TotGrossWt = 0, int TotVol = 0,
        int TotNetWt = 0, int CurrencyFK = 0)
        {


            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            objWK.MyCommand.Transaction = TRAN;
            arrMessage.Clear();
            try
            {
                if (dsCommInv.Tables[0].Rows.Count > 0)
                {
                    for (int Rcnt = 0; Rcnt <= dsCommInv.Tables[0].Rows.Count - 1; Rcnt++)
                    {
                        if (!string.IsNullOrEmpty(dsCommInv.Tables[0].Rows[Rcnt]["BOOKING_TRN_COMMINV_PK"].ToString()) & (dsCommInv.Tables[0].Rows[Rcnt]["BOOKING_TRN_COMMINV_PK"] != null))
                        {
                            for (int Rcnt1 = 0; Rcnt1 <= dsPackList.Tables[0].Rows.Count - 1; Rcnt1++)
                            {
                                if (!string.IsNullOrEmpty(dsPackList.Tables[0].Rows[Rcnt1]["BOOKING_TRN_COMMINV_PK"].ToString()) & (dsPackList.Tables[0].Rows[Rcnt1]["BOOKING_TRN_COMMINV_PK"] != null))
                                {
                                    if (dsCommInv.Tables[0].Rows[Rcnt]["BOOKING_TRN_COMMINV_PK"] == dsPackList.Tables[0].Rows[Rcnt1]["BOOKING_TRN_COMMINV_PK"])
                                    {
                                        var _with27 = objWK.MyCommand;
                                        _with27.CommandType = CommandType.StoredProcedure;
                                        _with27.CommandText = objWK.MyUserName + ".BOOKING_COMMINV_PACK_PKG.BOOKING_TRN_COMMINV_DTL_UPD";
                                        var _with28 = _with27.Parameters;
                                        _with28.Clear();
                                        _with28.Add("BOOKING_TRN_COMMINV_PK_IN", dsCommInv.Tables[0].Rows[Rcnt]["BOOKING_TRN_COMMINV_PK"]);
                                        _with28.Add("BOOKING_MST_FK_IN", Booking_Mst_Fk);
                                        _with28.Add("GOODS_DECRIPTION_IN", dsCommInv.Tables[0].Rows[Rcnt]["GOODS_DECRIPTION"]);
                                        _with28.Add("COUNTRY_MST_FK_IN", dsCommInv.Tables[0].Rows[Rcnt]["COUNTRY_MST_FK"]);
                                        _with28.Add("QUANTITTY_IN", dsCommInv.Tables[0].Rows[Rcnt]["QUANTITTY"]);
                                        _with28.Add("DIMENTION_UNIT_MST_FK_IN", Convert.ToInt32(dsCommInv.Tables[0].Rows[Rcnt]["UNIT_FK"])).Direction = ParameterDirection.Input;
                                        _with28.Add("UNIT_PRICE_IN", dsCommInv.Tables[0].Rows[Rcnt]["UNIT_PRICE"]);
                                        _with28.Add("CURRENCY_MST_FK_IN", CurrencyFK);
                                        _with28.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                                        _with28.Add("COMM_INV_REMARKS_IN", getDefault(Remarks, DBNull.Value));
                                        _with28.Add("TOT_CARTONS_IN", getDefault(NoOfCartons, DBNull.Value));
                                        _with28.Add("TOT_NET_WEIGHT_IN", getDefault(TotNetWt, DBNull.Value));
                                        _with28.Add("TOT_GROSS_WT_IN", getDefault(TotGrossWt, DBNull.Value));
                                        _with28.Add("TOT_VOLUME_IN", getDefault(TotVol, DBNull.Value));
                                        //'
                                        _with28.Add("MARKS_IN", dsPackList.Tables[0].Rows[Rcnt1]["MARKS"]);
                                        _with28.Add("CARTTON_NR_IN", dsPackList.Tables[0].Rows[Rcnt1]["CARTTON_NR"]);
                                        if (!string.IsNullOrEmpty(dsPackList.Tables[0].Rows[Rcnt1]["NCARTONS"].ToString()))
                                        {
                                            _with28.Add("NO_OF_CARTONS_IN", Convert.ToInt32(dsPackList.Tables[0].Rows[Rcnt1]["NCARTONS"]));
                                        }
                                        else
                                        {
                                            _with28.Add("NO_OF_CARTONS_IN", dsPackList.Tables[0].Rows[Rcnt1]["NCARTONS"]);
                                        }
                                        _with28.Add("NET_WEIGHT_IN", dsPackList.Tables[0].Rows[Rcnt1]["NET_WEIGHT"]);
                                        _with28.Add("GROSS_WEIGHT_IN", dsPackList.Tables[0].Rows[Rcnt1]["GROSS_WEIGHT"]);
                                        _with28.Add("LENGTH_IN", dsPackList.Tables[0].Rows[Rcnt1]["LENGTH"]);
                                        _with28.Add("WIDTH_IN", dsPackList.Tables[0].Rows[Rcnt1]["WIDTH"]);
                                        _with28.Add("HEIGHT_IN", dsPackList.Tables[0].Rows[Rcnt1]["HEIGHT"]);
                                        _with28.Add("VOLUME_IN", dsPackList.Tables[0].Rows[Rcnt1]["VOLUME"]);
                                        _with28.Add("PACK_LIST_REMARKS_IN", getDefault(Pack_Remarks, DBNull.Value));
                                        _with28.Add("LAST_MODIFIED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                                        _with28.Add("CONFIG_MST_FK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                                        _with28.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                                        _with27.ExecuteNonQuery();
                                    }
                                }
                            }
                        }
                        else
                        {
                            var _with29 = objWK.MyCommand;
                            _with29.CommandType = CommandType.StoredProcedure;
                            _with29.CommandText = objWK.MyUserName + ".BOOKING_COMMINV_PACK_PKG.BOOKING_TRN_COMMINV_DTL_INS";
                            var _with30 = _with29.Parameters;
                            _with30.Clear();
                            _with30.Add("BOOKING_MST_FK_IN", Booking_Mst_Fk);
                            _with30.Add("COMM_INV_DT_IN", CommInvDt);
                            _with30.Add("GOODS_DECRIPTION_IN", dsCommInv.Tables[0].Rows[Rcnt]["GOODS_DECRIPTION"]);
                            _with30.Add("COUNTRY_MST_FK_IN", dsCommInv.Tables[0].Rows[Rcnt]["COUNTRY_MST_FK"]);
                            _with30.Add("QUANTITTY_IN", dsCommInv.Tables[0].Rows[Rcnt]["QUANTITTY"]);
                            _with30.Add("DIMENTION_UNIT_MST_FK_IN", Convert.ToInt32(dsCommInv.Tables[0].Rows[Rcnt]["UNIT_FK"])).Direction = ParameterDirection.Input;
                            _with30.Add("UNIT_PRICE_IN", dsCommInv.Tables[0].Rows[Rcnt]["UNIT_PRICE"]);
                            _with30.Add("CURRENCY_MST_FK_IN", CurrencyFK);
                            _with30.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                            _with30.Add("COMM_INV_REMARKS_IN", getDefault(Remarks, DBNull.Value));
                            _with30.Add("TOT_CARTONS_IN", getDefault(NoOfCartons, DBNull.Value));
                            _with30.Add("TOT_NET_WEIGHT_IN", getDefault(TotNetWt, DBNull.Value));
                            //'
                            _with30.Add("TOT_GROSS_WT_IN", getDefault(TotGrossWt, DBNull.Value));
                            _with30.Add("TOT_VOLUME_IN", getDefault(TotVol, DBNull.Value));
                            //'
                            _with30.Add("MARKS_IN", dsPackList.Tables[0].Rows[Rcnt]["MARKS"]);
                            _with30.Add("CARTTON_NR_IN", dsPackList.Tables[0].Rows[Rcnt]["CARTTON_NR"]);
                            if (!string.IsNullOrEmpty(dsPackList.Tables[0].Rows[Rcnt]["NCARTONS"].ToString()))
                            {
                                _with30.Add("NO_OF_CARTONS_IN", Convert.ToInt32(dsPackList.Tables[0].Rows[Rcnt]["NCARTONS"]));
                            }
                            else
                            {
                                _with30.Add("NO_OF_CARTONS_IN", dsPackList.Tables[0].Rows[Rcnt]["NCARTONS"]);
                            }
                            _with30.Add("NET_WEIGHT_IN", dsPackList.Tables[0].Rows[Rcnt]["NET_WEIGHT"]);
                            _with30.Add("GROSS_WEIGHT_IN", dsPackList.Tables[0].Rows[Rcnt]["GROSS_WEIGHT"]);
                            _with30.Add("LENGTH_IN", dsPackList.Tables[0].Rows[Rcnt]["LENGTH"]);
                            _with30.Add("WIDTH_IN", dsPackList.Tables[0].Rows[Rcnt]["WIDTH"]);
                            _with30.Add("HEIGHT_IN", dsPackList.Tables[0].Rows[Rcnt]["HEIGHT"]);
                            _with30.Add("VOLUME_IN", dsPackList.Tables[0].Rows[Rcnt]["VOLUME"]);
                            _with30.Add("PACK_LIST_REMARKS_IN", getDefault(Pack_Remarks, DBNull.Value));
                            _with30.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                            _with30.Add("CONFIG_MST_FK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                            _with30.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
                            _with29.ExecuteNonQuery();
                        }
                    }
                    TRAN.Commit();
                    arrMessage.Add("All data saved successfully");
                    return arrMessage;
                }
            }
            catch (OracleException oraexp)
            {
                arrMessage.Add(oraexp.Message);
                TRAN.Rollback();
                return arrMessage;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
            return new ArrayList();
        }

        public ArrayList DeleteCommPackList(string PkList, long BookingFk)
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            objWK.MyCommand.Transaction = TRAN;
            arrMessage.Clear();
            try
            {
                var _with31 = objWK.MyCommand;
                _with31.CommandType = CommandType.StoredProcedure;
                _with31.CommandText = objWK.MyUserName + ".BOOKING_COMMINV_PACK_PKG.BOOKING_TRN_COMMINV_DTL_DEL";
                var _with32 = _with31.Parameters;
                _with32.Add("BOOKING_TRN_COMMINV_FKS_IN", PkList).Direction = ParameterDirection.Input;
                _with32.Add("BOOKING_MST_FK_IN", BookingFk).Direction = ParameterDirection.Input;
                _with32.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with31.ExecuteNonQuery();
                TRAN.Commit();
                arrMessage.Add("All data Deleted Successfully");
                return arrMessage;
            }
            catch (OracleException oraexp)
            {
                arrMessage.Add(oraexp.Message);
                TRAN.Rollback();
                return arrMessage;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
        }
        #endregion

        #region "GettingCountryPk"
        public long GettingCountryPk(string CPK)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append(" SELECT CMT.COUNTRY_MST_PK FROM COUNTRY_MST_TBL CMT WHERE UPPER(CMT.COUNTRY_ID) = '" + CPK.ToUpper() + "'");
            try
            {
                return Convert.ToInt64(objWF.ExecuteScaler(sb.ToString()));
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

        #region "GettingCountryPk"
        public long GettingDDPk(string CPK)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append(" SELECT QDDT.DD_VALUE FROM QFOR_DROP_DOWN_TBL QDDT WHERE QDDT.CONFIG_ID='QFOR4459'AND QDDT.DD_ID   =  '" + CPK + "'");
            try
            {
                return Convert.ToInt64(objWF.ExecuteScaler(sb.ToString()));
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