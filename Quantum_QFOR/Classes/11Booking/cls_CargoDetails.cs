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
//'*  Modified Date(DD-MON-YYYY)              Modified By                             Remarks (Bugs Related)
//'*
//'*
//'***************************************************************************************************************

#endregion "Comments"

using Oracle.DataAccess.Client;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_CargoDetails : CommonFeatures
    {
        #region "properties"

        private int _CargoDetIndex = 0;

        public int CargoDetIndex
        {
            get { return _CargoDetIndex; }
            set { _CargoDetIndex = value; }
        }

        private int _BookingTrnPK = 0;

        public int BookingTrnPK
        {
            get { return _BookingTrnPK; }
            set { _BookingTrnPK = value; }
        }

        #endregion "properties"

        #region "Save Function"

        public ArrayList Save(DataSet dsContainerData, DataSet dsFreightDetails, long JobCardPK, DataSet dsOtherCharges, string Mark = "", string Desc = "")
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            try
            {
                Int32 RecAfct = default(Int32);

                OracleCommand insContainerDetails = new OracleCommand();
                OracleCommand updContainerDetails = new OracleCommand();
                OracleCommand insFreightDetails = new OracleCommand();
                OracleCommand updFreightDetails = new OracleCommand();
                OracleCommand delFreightDetails = new OracleCommand();
                OracleCommand insOtherChargesDetails = new OracleCommand();
                OracleCommand updOtherChargesDetails = new OracleCommand();
                OracleCommand delOtherChargesDetails = new OracleCommand();

                var _with1 = insContainerDetails;
                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_CONT_INS";

                var _with2 = _with1.Parameters;
                _with2.Clear();
                insContainerDetails.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                insContainerDetails.Parameters.Add("CONTAINER_NUMBER_IN", OracleDbType.Varchar2, 16, "container_number").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["CONTAINER_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "container_type_mst_fk").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("SEAL_NUMBER_IN", OracleDbType.Varchar2, 20, "seal_number").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["SEAL_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("VOLUME_IN_CBM_IN", OracleDbType.Int32, 10, "volume_in_cbm").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["VOLUME_IN_CBM_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("GROSS_WEIGHT_IN", OracleDbType.Int32, 10, "gross_weight").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["GROSS_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("NET_WEIGHT_IN", OracleDbType.Int32, 10, "net_weight").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["NET_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("CHARGEABLE_WEIGHT_IN", OracleDbType.Int32, 10, "chargeable_weight").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["CHARGEABLE_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("PACK_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "pack_type_mst_fk").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["PACK_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("PACK_COUNT_IN", OracleDbType.Int32, 6, "pack_count").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["PACK_COUNT_IN"].SourceVersion = DataRowVersion.Current;

                //insContainerDetails.Parameters.Add("COMMODITY_MST_FK_IN", OracleDbType.Int32, 10, "commodity_mst_fk").Direction = ParameterDirection.Input
                //insContainerDetails.Parameters["COMMODITY_MST_FK_IN"].SourceVersion = DataRowVersion.Current

                insContainerDetails.Parameters.Add("COMMODITY_MST_FKS_IN", OracleDbType.Varchar2, 200, "COMMODITY_MST_FKS").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["COMMODITY_MST_FKS_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("LOAD_DATE_IN", OracleDbType.Date, 20, "load_date").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["LOAD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("CONTAINER_PK_IN", OracleDbType.Int32, 20, "CONTAINER_PK").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["CONTAINER_PK_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_SEA_EXP_CONT_PK").Direction = ParameterDirection.Output;
                insContainerDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with3 = updContainerDetails;
                _with3.Connection = objWK.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_CONT_UPD";

                var _with4 = _with3.Parameters;
                _with4.Clear();
                updContainerDetails.Parameters.Add("JOB_TRN_SEA_EXP_CONT_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_exp_cont_pk").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["JOB_TRN_SEA_EXP_CONT_PK_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                updContainerDetails.Parameters.Add("CONTAINER_NUMBER_IN", OracleDbType.Varchar2, 16, "container_number").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["CONTAINER_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "container_type_mst_fk").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("SEAL_NUMBER_IN", OracleDbType.Varchar2, 20, "seal_number").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["SEAL_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("VOLUME_IN_CBM_IN", OracleDbType.Int32, 10, "volume_in_cbm").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["VOLUME_IN_CBM_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("GROSS_WEIGHT_IN", OracleDbType.Int32, 10, "gross_weight").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["GROSS_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("NET_WEIGHT_IN", OracleDbType.Int32, 10, "net_weight").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["NET_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("CHARGEABLE_WEIGHT_IN", OracleDbType.Int32, 10, "chargeable_weight").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["CHARGEABLE_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("PACK_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "pack_type_mst_fk").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["PACK_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("PACK_COUNT_IN", OracleDbType.Int32, 6, "pack_count").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["PACK_COUNT_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("COMMODITY_MST_FK_IN", OracleDbType.Int32, 10, "commodity_mst_fk").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["COMMODITY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("COMMODITY_MST_FKS_IN", OracleDbType.Varchar2, 200, "COMMODITY_MST_FKS").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["COMMODITY_MST_FKS_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("LOAD_DATE_IN", OracleDbType.Date, 20, "load_date").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["LOAD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("CONTAINER_PK_IN", OracleDbType.Int32, 20, "CONTAINER_PK").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["CONTAINER_PK_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updContainerDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with5 = objWK.MyDataAdapter;

                _with5.InsertCommand = insContainerDetails;
                _with5.InsertCommand.Transaction = TRAN;

                _with5.UpdateCommand = updContainerDetails;
                _with5.UpdateCommand.Transaction = TRAN;

                RecAfct = _with5.Update(dsContainerData.Tables[0]);

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }

                var _with6 = insFreightDetails;
                _with6.Connection = objWK.MyConnection;
                _with6.CommandType = CommandType.StoredProcedure;
                _with6.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_FD_INS";

                var _with7 = _with6.Parameters;
                _with7.Clear();
                insFreightDetails.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                insFreightDetails.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "container_type_mst_fk").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("FREIGHT_TYPE_IN", OracleDbType.Int32, 1, "freight_type").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["FREIGHT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_mst_fk").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "frtpayer_cust_mst_fk").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("FREIGHT_AMT_IN", OracleDbType.Int32, 10, "freight_amt").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["FREIGHT_AMT_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_fk").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("BASIS_IN", OracleDbType.Int32, 10, "basis").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["BASIS_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "roe").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("QUANTITY_IN", OracleDbType.Int32, 10, "quantity").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["QUANTITY_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("PRINT_ON_MBL_IN", OracleDbType.Int32, 1, "Print").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["PRINT_ON_MBL_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_SEA_EXP_FD_PK").Direction = ParameterDirection.Output;
                insFreightDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with8 = updFreightDetails;
                _with8.Connection = objWK.MyConnection;
                _with8.CommandType = CommandType.StoredProcedure;
                _with8.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_FD_UPD";

                var _with9 = _with8.Parameters;
                _with9.Clear();
                updFreightDetails.Parameters.Add("JOB_TRN_SEA_EXP_FD_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_exp_fd_pk").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["JOB_TRN_SEA_EXP_FD_PK_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                updFreightDetails.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "container_type_mst_fk").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("FREIGHT_TYPE_IN", OracleDbType.Int32, 1, "freight_type").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["FREIGHT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_mst_fk").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "frtpayer_cust_mst_fk").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("FREIGHT_AMT_IN", OracleDbType.Int32, 10, "freight_amt").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["FREIGHT_AMT_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_fk").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("BASIS_IN", OracleDbType.Int32, 10, "basis").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["BASIS_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "roe").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("QUANTITY_IN", OracleDbType.Int32, 10, "quantity").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["QUANTITY_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("PRINT_ON_MBL_IN", OracleDbType.Int32, 1, "Print").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["PRINT_ON_MBL_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updFreightDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with10 = delFreightDetails;
                _with10.Connection = objWK.MyConnection;
                _with10.CommandType = CommandType.StoredProcedure;
                _with10.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_FD_DEL";
                objWK.MyCommand.Parameters.Clear();

                delFreightDetails.Parameters.Add("JOB_TRN_SEA_EXP_FD_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_exp_fd_pk").Direction = ParameterDirection.Input;
                delFreightDetails.Parameters["JOB_TRN_SEA_EXP_FD_PK_IN"].SourceVersion = DataRowVersion.Current;

                delFreightDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                delFreightDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with11 = objWK.MyDataAdapter;

                _with11.InsertCommand = insFreightDetails;
                _with11.InsertCommand.Transaction = TRAN;

                _with11.UpdateCommand = updFreightDetails;
                _with11.UpdateCommand.Transaction = TRAN;

                _with11.DeleteCommand = delFreightDetails;
                _with11.DeleteCommand.Transaction = TRAN;

                RecAfct = _with11.Update(dsFreightDetails);

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }

                var _with12 = insOtherChargesDetails;
                _with12.Connection = objWK.MyConnection;
                _with12.CommandType = CommandType.StoredProcedure;
                _with12.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_OTH_CHRG_INS";

                var _with13 = _with12.Parameters;
                _with13.Clear();
                insOtherChargesDetails.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                insOtherChargesDetails.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                insOtherChargesDetails.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insOtherChargesDetails.Parameters.Add("PAYMENT_TYPE_IN", OracleDbType.Int32, 1, "Payment_Type").Direction = ParameterDirection.Input;
                insOtherChargesDetails.Parameters["PAYMENT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                insOtherChargesDetails.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_mst_fk").Direction = ParameterDirection.Input;
                insOtherChargesDetails.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insOtherChargesDetails.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "frtpayer_cust_mst_fk").Direction = ParameterDirection.Input;
                insOtherChargesDetails.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insOtherChargesDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_pk").Direction = ParameterDirection.Input;
                insOtherChargesDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insOtherChargesDetails.Parameters.Add("AMOUNT_IN", OracleDbType.Int32, 10, "amount").Direction = ParameterDirection.Input;
                insOtherChargesDetails.Parameters["AMOUNT_IN"].SourceVersion = DataRowVersion.Current;

                insOtherChargesDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "ROE").Direction = ParameterDirection.Input;
                insOtherChargesDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                insOtherChargesDetails.Parameters.Add("PRINT_ON_MBL_IN", OracleDbType.Int32, 1, "Print").Direction = ParameterDirection.Input;
                insOtherChargesDetails.Parameters["PRINT_ON_MBL_IN"].SourceVersion = DataRowVersion.Current;

                insOtherChargesDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_SEA_EXP_OTH_PK").Direction = ParameterDirection.Output;
                insOtherChargesDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with14 = updOtherChargesDetails;
                _with14.Connection = objWK.MyConnection;
                _with14.CommandType = CommandType.StoredProcedure;
                _with14.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_OTH_CHRG_UPD";

                var _with15 = _with14.Parameters;
                _with15.Clear();
                updOtherChargesDetails.Parameters.Add("JOB_TRN_SEA_EXP_OTH_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_exp_oth_pk").Direction = ParameterDirection.Input;
                updOtherChargesDetails.Parameters["JOB_TRN_SEA_EXP_OTH_PK_IN"].SourceVersion = DataRowVersion.Current;

                updOtherChargesDetails.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                updOtherChargesDetails.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                updOtherChargesDetails.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updOtherChargesDetails.Parameters.Add("PAYMENT_TYPE_IN", OracleDbType.Int32, 1, "Payment_Type").Direction = ParameterDirection.Input;
                updOtherChargesDetails.Parameters["PAYMENT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                updOtherChargesDetails.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_mst_fk").Direction = ParameterDirection.Input;
                updOtherChargesDetails.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updOtherChargesDetails.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "frtpayer_cust_mst_fk").Direction = ParameterDirection.Input;
                updOtherChargesDetails.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updOtherChargesDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_pk").Direction = ParameterDirection.Input;
                updOtherChargesDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updOtherChargesDetails.Parameters.Add("AMOUNT_IN", OracleDbType.Int32, 10, "amount").Direction = ParameterDirection.Input;
                updOtherChargesDetails.Parameters["AMOUNT_IN"].SourceVersion = DataRowVersion.Current;

                updOtherChargesDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "ROE").Direction = ParameterDirection.Input;
                updOtherChargesDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                updOtherChargesDetails.Parameters.Add("PRINT_ON_MBL_IN", OracleDbType.Int32, 1, "Print").Direction = ParameterDirection.Input;
                updOtherChargesDetails.Parameters["PRINT_ON_MBL_IN"].SourceVersion = DataRowVersion.Current;

                updOtherChargesDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updOtherChargesDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with16 = delOtherChargesDetails;
                _with16.Connection = objWK.MyConnection;
                _with16.CommandType = CommandType.StoredProcedure;
                _with16.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_OTH_CHRG_DEL";
                objWK.MyCommand.Parameters.Clear();

                delOtherChargesDetails.Parameters.Add("JOB_TRN_SEA_EXP_OTH_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_exp_oth_pk").Direction = ParameterDirection.Input;
                delOtherChargesDetails.Parameters["JOB_TRN_SEA_EXP_OTH_PK_IN"].SourceVersion = DataRowVersion.Current;

                delOtherChargesDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                delOtherChargesDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with17 = objWK.MyDataAdapter;

                _with17.InsertCommand = insOtherChargesDetails;
                _with17.InsertCommand.Transaction = TRAN;

                _with17.UpdateCommand = updOtherChargesDetails;
                _with17.UpdateCommand.Transaction = TRAN;

                _with17.DeleteCommand = delOtherChargesDetails;
                _with17.DeleteCommand.Transaction = TRAN;

                RecAfct = _with17.Update(dsOtherCharges);

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    SaveOtherDet(JobCardPK, Mark, Desc, 2, TRAN);
                    TRAN.Commit();
                    arrMessage.Add("All Data Saved Successfully");
                    return arrMessage;
                }
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
                objWK.MyCommand.Connection.Close();
            }
        }

        public ArrayList SaveAir(DataSet dsContainerData, DataSet dsFreightDetails, long JobCardPK, DataSet dsOtherCharges, string Mark = "", string Desc = "")
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            try
            {
                Int32 RecAfct = default(Int32);

                OracleCommand insContainerDetails = new OracleCommand();
                OracleCommand updContainerDetails = new OracleCommand();
                OracleCommand insFreightDetails = new OracleCommand();
                OracleCommand updFreightDetails = new OracleCommand();
                OracleCommand delFreightDetails = new OracleCommand();
                OracleCommand insOtherChargesDetails = new OracleCommand();
                OracleCommand updOtherChargesDetails = new OracleCommand();
                OracleCommand delOtherChargesDetails = new OracleCommand();

                var _with18 = insContainerDetails;
                _with18.Connection = objWK.MyConnection;
                _with18.CommandType = CommandType.StoredProcedure;
                _with18.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_TRN_AIR_EXP_CONT_INS";

                var _with19 = _with18.Parameters;
                _with19.Clear();
                insContainerDetails.Parameters.Add("JOB_CARD_AIR_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                insContainerDetails.Parameters.Add("PALETTE_SIZE_IN", OracleDbType.Varchar2, 20, "palette_size").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["PALETTE_SIZE_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("AIRFREIGHT_SLABS_TBL_FK_IN", OracleDbType.Int32, 10, "airfreight_slabs_tbl_fk").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["AIRFREIGHT_SLABS_TBL_FK_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("ULD_NUMBER_IN", OracleDbType.Varchar2, 20, "uld_number").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["ULD_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("VOLUME_IN_CBM_IN", OracleDbType.Int32, 10, "volume_in_cbm").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["VOLUME_IN_CBM_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("GROSS_WEIGHT_IN", OracleDbType.Int32, 10, "gross_weight").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["GROSS_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("CHARGEABLE_WEIGHT_IN", OracleDbType.Int32, 10, "chargeable_weight").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["CHARGEABLE_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("PACK_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "pack_type_mst_fk").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["PACK_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("PACK_COUNT_IN", OracleDbType.Int32, 6, "pack_count").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["PACK_COUNT_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("COMMODITY_MST_FK_IN", OracleDbType.Varchar2, 50, "COMMODITY_MST_FKS").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["COMMODITY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("LOAD_DATE_IN", OracleDbType.Date, 20, "load_date").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["LOAD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_AIR_EXP_CONT_PK").Direction = ParameterDirection.Output;
                insContainerDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with20 = updContainerDetails;
                _with20.Connection = objWK.MyConnection;
                _with20.CommandType = CommandType.StoredProcedure;
                _with20.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_TRN_AIR_EXP_CONT_UPD";

                var _with21 = _with20.Parameters;
                _with21.Clear();
                updContainerDetails.Parameters.Add("JOB_TRN_AIR_EXP_CONT_PK_IN", OracleDbType.Int32, 10, "job_trn_air_exp_cont_pk").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["JOB_TRN_AIR_EXP_CONT_PK_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("JOB_CARD_AIR_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                updContainerDetails.Parameters.Add("PALETTE_SIZE_IN", OracleDbType.Varchar2, 20, "palette_size").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["PALETTE_SIZE_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("AIRFREIGHT_SLABS_TBL_FK_IN", OracleDbType.Int32, 10, "airfreight_slabs_tbl_fk").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["AIRFREIGHT_SLABS_TBL_FK_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("ULD_NUMBER_IN", OracleDbType.Varchar2, 20, "uld_number").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["ULD_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("VOLUME_IN_CBM_IN", OracleDbType.Int32, 10, "volume_in_cbm").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["VOLUME_IN_CBM_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("GROSS_WEIGHT_IN", OracleDbType.Int32, 10, "gross_weight").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["GROSS_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("CHARGEABLE_WEIGHT_IN", OracleDbType.Int32, 10, "chargeable_weight").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["CHARGEABLE_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("PACK_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "pack_type_mst_fk").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["PACK_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("PACK_COUNT_IN", OracleDbType.Int32, 6, "pack_count").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["PACK_COUNT_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("COMMODITY_MST_FK_IN", OracleDbType.Varchar2, 50, "COMMODITY_MST_FKS").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["COMMODITY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("LOAD_DATE_IN", OracleDbType.Date, 20, "load_date").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["LOAD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updContainerDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with22 = objWK.MyDataAdapter;

                _with22.InsertCommand = insContainerDetails;
                _with22.InsertCommand.Transaction = TRAN;

                _with22.UpdateCommand = updContainerDetails;
                _with22.UpdateCommand.Transaction = TRAN;

                RecAfct = _with22.Update(dsContainerData.Tables[0]);

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }

                var _with23 = insFreightDetails;
                _with23.Connection = objWK.MyConnection;
                _with23.CommandType = CommandType.StoredProcedure;
                _with23.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_TRN_AIR_EXP_FD_INS";

                var _with24 = _with23.Parameters;
                _with24.Clear();
                insFreightDetails.Parameters.Add("JOB_CARD_AIR_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                insFreightDetails.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("FREIGHT_TYPE_IN", OracleDbType.Int32, 1, "freight_type").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["FREIGHT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("FREIGHT_AMT_IN", OracleDbType.Int32, 10, "freight_amt").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["FREIGHT_AMT_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_fk").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("BASIS_IN", OracleDbType.Int32, 10, "basis").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["BASIS_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("QUANTITY_IN", OracleDbType.Int32, 10, "quantity").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["QUANTITY_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "roe").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("PRINT_ON_MAWB_IN", OracleDbType.Int32, 1, "Print").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["PRINT_ON_MAWB_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_AIR_EXP_FD_PK").Direction = ParameterDirection.Output;
                insFreightDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with25 = updFreightDetails;
                _with25.Connection = objWK.MyConnection;
                _with25.CommandType = CommandType.StoredProcedure;
                _with25.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_TRN_AIR_EXP_FD_UPD";
                var _with26 = _with25.Parameters;
                _with26.Clear();
                updFreightDetails.Parameters.Add("JOB_TRN_AIR_EXP_FD_PK_IN", OracleDbType.Int32, 10, "job_trn_air_exp_fd_pk").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["JOB_TRN_AIR_EXP_FD_PK_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("JOB_CARD_AIR_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                updFreightDetails.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("FREIGHT_TYPE_IN", OracleDbType.Int32, 1, "freight_type").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["FREIGHT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("FREIGHT_AMT_IN", OracleDbType.Int32, 10, "freight_amt").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["FREIGHT_AMT_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_fk").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("BASIS_IN", OracleDbType.Int32, 10, "basis").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["BASIS_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("QUANTITY_IN", OracleDbType.Int32, 10, "quantity").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["QUANTITY_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "roe").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("PRINT_ON_MAWB_IN", OracleDbType.Int32, 1, "Print").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["PRINT_ON_MAWB_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updFreightDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with27 = delFreightDetails;
                _with27.Connection = objWK.MyConnection;
                _with27.CommandType = CommandType.StoredProcedure;
                _with27.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_TRN_AIR_EXP_FD_DEL";
                objWK.MyCommand.Parameters.Clear();
                delFreightDetails.Parameters.Add("JOB_TRN_AIR_EXP_FD_PK_IN", OracleDbType.Int32, 10, "job_trn_air_exp_fd_pk").Direction = ParameterDirection.Input;
                delFreightDetails.Parameters["JOB_TRN_AIR_EXP_FD_PK_IN"].SourceVersion = DataRowVersion.Current;

                delFreightDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                delFreightDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with28 = objWK.MyDataAdapter;

                _with28.InsertCommand = insFreightDetails;
                _with28.InsertCommand.Transaction = TRAN;

                _with28.UpdateCommand = updFreightDetails;
                _with28.UpdateCommand.Transaction = TRAN;

                _with28.DeleteCommand = delFreightDetails;
                _with28.DeleteCommand.Transaction = TRAN;

                RecAfct = _with28.Update(dsFreightDetails);

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }

                var _with29 = insOtherChargesDetails;
                _with29.Connection = objWK.MyConnection;
                _with29.CommandType = CommandType.StoredProcedure;
                _with29.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_TRN_AIR_EXP_OTH_CHRG_INS";

                var _with30 = _with29.Parameters;
                _with30.Clear();
                insOtherChargesDetails.Parameters.Add("JOB_CARD_AIR_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                insOtherChargesDetails.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                insOtherChargesDetails.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insOtherChargesDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_pk").Direction = ParameterDirection.Input;
                insOtherChargesDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insOtherChargesDetails.Parameters.Add("AMOUNT_IN", OracleDbType.Int32, 10, "amount").Direction = ParameterDirection.Input;
                insOtherChargesDetails.Parameters["AMOUNT_IN"].SourceVersion = DataRowVersion.Current;

                insOtherChargesDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "ROE").Direction = ParameterDirection.Input;
                insOtherChargesDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                insOtherChargesDetails.Parameters.Add("PRINT_ON_MAWB_IN", OracleDbType.Int32, 1, "Print").Direction = ParameterDirection.Input;
                insOtherChargesDetails.Parameters["PRINT_ON_MAWB_IN"].SourceVersion = DataRowVersion.Current;

                insOtherChargesDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "job_trn_air_exp_oth_pk").Direction = ParameterDirection.Output;
                insOtherChargesDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with31 = updOtherChargesDetails;
                _with31.Connection = objWK.MyConnection;
                _with31.CommandType = CommandType.StoredProcedure;
                _with31.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_TRN_AIR_EXP_OTH_CHRG_UPD";

                var _with32 = _with31.Parameters;
                _with32.Clear();
                updOtherChargesDetails.Parameters.Add("JOB_TRN_AIR_EXP_OTH_PK_IN", OracleDbType.Int32, 10, "job_trn_air_exp_oth_pk").Direction = ParameterDirection.Input;
                updOtherChargesDetails.Parameters["JOB_TRN_AIR_EXP_OTH_PK_IN"].SourceVersion = DataRowVersion.Current;

                updOtherChargesDetails.Parameters.Add("JOB_CARD_AIR_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                updOtherChargesDetails.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                updOtherChargesDetails.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updOtherChargesDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_pk").Direction = ParameterDirection.Input;
                updOtherChargesDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updOtherChargesDetails.Parameters.Add("AMOUNT_IN", OracleDbType.Int32, 10, "amount").Direction = ParameterDirection.Input;
                updOtherChargesDetails.Parameters["AMOUNT_IN"].SourceVersion = DataRowVersion.Current;

                updOtherChargesDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "ROE").Direction = ParameterDirection.Input;
                updOtherChargesDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                updOtherChargesDetails.Parameters.Add("PRINT_ON_MAWB_IN", OracleDbType.Int32, 1, "Print").Direction = ParameterDirection.Input;
                updOtherChargesDetails.Parameters["PRINT_ON_MAWB_IN"].SourceVersion = DataRowVersion.Current;

                updOtherChargesDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updOtherChargesDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with33 = delOtherChargesDetails;
                _with33.Connection = objWK.MyConnection;
                _with33.CommandType = CommandType.StoredProcedure;

                _with33.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_TRN_AIR_EXP_OTH_CHRG_DEL";
                objWK.MyCommand.Parameters.Clear();
                delOtherChargesDetails.Parameters.Add("JOB_TRN_AIR_EXP_OTH_PK_IN", OracleDbType.Int32, 10, "job_trn_air_exp_oth_pk").Direction = ParameterDirection.Input;
                delOtherChargesDetails.Parameters["JOB_TRN_AIR_EXP_OTH_PK_IN"].SourceVersion = DataRowVersion.Current;

                delOtherChargesDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                delOtherChargesDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with34 = objWK.MyDataAdapter;

                _with34.InsertCommand = insOtherChargesDetails;
                _with34.InsertCommand.Transaction = TRAN;

                _with34.UpdateCommand = updOtherChargesDetails;
                _with34.UpdateCommand.Transaction = TRAN;

                _with34.DeleteCommand = delOtherChargesDetails;
                _with34.DeleteCommand.Transaction = TRAN;

                RecAfct = _with34.Update(dsOtherCharges);

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    SaveOtherDet(JobCardPK, Mark, Desc, 1, TRAN);
                    TRAN.Commit();
                    arrMessage.Add("All Data Saved Successfully");
                    return arrMessage;
                }
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
                objWK.MyCommand.Connection.Close();
            }
        }

        public DataTable ActiveCommodities(string Pk, Int32 CommPk = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0, string SearchOn = "COMMODITY_ID", string SearchValue = "", string formtype = "")
        {
            StringBuilder Strsql = new StringBuilder();
            StringBuilder strcount = new StringBuilder();
            string strcondition = "";
            string strcond = "";
            Int32 TotalRecords = default(Int32);
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            WorkFlow objWF = new WorkFlow();
            string DefaultContainers = null;

            try
            {
                if (string.IsNullOrEmpty(Pk))
                {
                    Pk = "0";
                }
                try
                {
                    Pk = Pk.Replace(",,", ",");
                }
                catch (Exception ex)
                {
                }
                if (Pk.Substring(1, 1) == ",")
                {
                    Pk = Pk.Substring(2, Pk.Length - 1);
                }
                if (CommPk > 0)
                {
                    strcondition = " where comdty.commodity_group_fk= " + CommPk;
                }
                if (!string.IsNullOrEmpty(SearchValue))
                {
                    if (Pk != "0" | Pk != "-1")
                    {
                        strcond = "AND UPPER(" + SearchOn + ") LIKE '%" + SearchValue.ToUpper() + "%'";
                    }
                    else
                    {
                        strcond = "WHERE UPPER(" + SearchOn + ") LIKE '%" + SearchValue.ToUpper() + "%'";
                    }
                }
                strcount.Append("SELECT COUNT(*) FROM COMMODITY_MST_TBL comdty " + strcondition);

                if (formtype == "BLClause")
                {
                    strcondition += " where comdty.COMMODITY_MST_PK not in ( " + Pk + " )";
                }
                else
                {
                    if (Pk != "0" | Pk != "-1")
                    {
                        //strcondition &= " and comdty.COMMODITY_MST_PK not in ( " & Pk & " )"
                        if (CommPk > 0)
                        {
                            strcondition += " and comdty.COMMODITY_MST_PK not in ( " + Pk + " )";
                        }
                        else
                        {
                            strcondition += " where comdty.COMMODITY_MST_PK not in ( " + Pk + " )";
                        }
                    }
                }

                Strsql.Append(" select q.* from (select ROWNUM SR_NO, MainQuery.* from ( SELECT QRY.* FROM ( select comdty.commodity_mst_pk,comdty.commodity_id,comdty.commodity_name,'false' CHK ");
                Strsql.Append("  from COMMODITY_MST_TBL comdty " + strcondition + strcond + " ");

                if (Pk != "0" | Pk != "-1")
                {
                    Strsql.Append(" union ");
                    Strsql.Append(" select comdty.commodity_mst_pk,comdty.commodity_id,comdty.commodity_name,'1' CHK from COMMODITY_MST_TBL comdty ");
                    if (CommPk > 0)
                    {
                        Strsql.Append("  where comdty.commodity_group_fk= " + CommPk);
                        Strsql.Append(" " + strcond + " and comdty.COMMODITY_MST_PK in ( " + Pk + " )");
                    }
                    else
                    {
                        Strsql.Append("  where comdty.COMMODITY_MST_PK in ( " + Pk + " )");
                        Strsql.Append("  " + strcond + "");
                    }
                }

                Strsql.Append(" )QRY order by chk asc, COMMODITY_ID) MainQuery) q where SR_NO between ");
                TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strcount.ToString()));

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
                //last = CurrentPage * 25
                start = (CurrentPage - 1) * RecordsPerPage + 1;
                //start = (CurrentPage - 1) * 25 + 1

                DataTable dt = objWF.GetDataTable(Strsql.ToString() + start + " and " + last);
                return dt;
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

        #endregion "Save Function"

        #region "Fetch Cargo Details"

        public DataSet CargoDetail(Int32 nCount = 1, Int32 BkgTrnPK = 0, string CtrType = "", Int32 RowIndex = 0, string CommFks = "")
        {
            WorkFlow objWF = new WorkFlow();
            DataTable dtTemp = new DataTable();
            CargoDetIndex = 0;
            if (nCount == 0)
                nCount = 1;
            dtTemp = GetDefaultBlankCrgoTbl(CtrType);
            DataSet ds = new DataSet();
            DataSet ds1 = new DataSet();
            try
            {
                if (BkgTrnPK > 0)
                {
                    ds = GetBkgTrnDS(Convert.ToString(BkgTrnPK));
                    if (ds.Tables[0].Rows.Count < nCount)
                    {
                        for (int count = 0; count <= nCount - ds.Tables[0].Rows.Count - 1; count++)
                        {
                            DataRow dr = null;
                            dr = ds.Tables[0].NewRow();
                            foreach (DataColumn col in ds.Tables[0].Columns)
                            {
                                dr[col.ColumnName] = dtTemp.Rows[0][col.ColumnName];
                            }
                            ds.Tables[0].Rows.Add(dr);
                        }
                    }
                    for (int rowCount = 0; rowCount <= ds.Tables[0].Rows.Count - 1; rowCount++)
                    {
                        ds.Tables[0].Rows[rowCount]["SL_NO"] = rowCount + 1;
                    }
                    ds.AcceptChanges();
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
            return ds;
        }

        public DataSet GetBkgTrnDS(string BkgTrnPks, bool Temporary = false)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            DataSet ds = new DataSet();
            try
            {
                objWF.MyCommand.Parameters.Clear();
                var _with35 = objWF.MyCommand.Parameters;
                _with35.Add("BOOKING_TRN_FK_IN", BkgTrnPks).Direction = ParameterDirection.Input;
                _with35.Add("CARGO_CURSOR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                ds = objWF.GetDataSet("BOOKING_TRN_CARGO_DTL_PKG", "FETCH_CARGO_DTL");
                return ds;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            return ds;
        }

        private DataTable GetDefaultBlankCrgoTbl(string ContTyp = "", string CommFks = "")
        {
            WorkFlow objWF = new WorkFlow();
            DataTable dt = new DataTable();
            string query = "SELECT C.CONTAINER_TYPE_MST_PK FROM CONTAINER_TYPE_MST_TBL C WHERE UPPER(C.CONTAINER_TYPE_MST_ID)=UPPER('" + ContTyp + "')";
            //Dim ContTypeFk As String = objWF.ExecuteScaler("SELECT C.CONTAINER_TYPE_MST_PK FROM CONTAINER_TYPE_MST_TBL C WHERE UPPER(C.CONTAINER_TYPE_MST_ID)=UPPER('" & ContTyp & "')")
            string ContTypeFk = objWF.ExecuteScaler(query);
            try
            {
                objWF.MyCommand.Parameters.Clear();
                var _with36 = objWF.MyCommand.Parameters;
                _with36.Add("BOOKING_TRN_FK_IN", BookingTrnPK).Direction = ParameterDirection.Input;
                _with36.Add("CONTAINER_TYPE_FK_IN", ContTypeFk).Direction = ParameterDirection.Input;
                _with36.Add("CARGO_CURSOR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                ContTyp = ContTyp.Replace("'", "").Trim();

                dt = objWF.GetDataTable("BOOKING_TRN_CARGO_DTL_PKG", "GET_BLANK_CARGO_DTL");
                return dt;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            return dt;
        }

        #endregion "Fetch Cargo Details"

        #region "Temp Cargo Details"

        public void CreateTempcargoCommTables()
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            objWF.MyConnection.Open();
            Int32 getCount = 0;
            try
            {
                objWF.MyCommand.Parameters.Clear();
                //creating tem booking_trn_cargo_tbl
                sb.Append("SELECT COUNT(*)");
                sb.Append("      FROM ALL_TABLES A");
                //sb.Append("     WHERE A.TEMPORARY = 'Y' AND UPPER(A.TABLE_NAME) = UPPER('TEMP_BKG_CARGO_TBL')")
                sb.Append("     WHERE A.OWNER = USER AND UPPER(A.TABLE_NAME) = UPPER('TEMP_BKG_CARGO_TBL')");

                getCount = Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
                sb = new StringBuilder();
                if (getCount == 0)
                {
                    //sb.Append(" CREATE GLOBAL TEMPORARY TABLE TEMP_BKG_CARGO_TBL ON COMMIT PRESERVE ROWS AS")
                    //sb.Append(" SELECT * FROM BOOKING_TRN_CARGO_DTL")
                    sb.Append(" CREATE TABLE TEMP_BKG_CARGO_TBL AS SELECT * FROM BOOKING_TRN_CARGO_DTL");
                    objWF.ExecuteCommands(sb.ToString());
                    sb = new StringBuilder();
                    sb.Append(" DELETE FROM TEMP_BKG_COMM_TBL ");
                    objWF.ExecuteCommands(sb.ToString());
                }
                //creating temp booking_commmofdity tbl
                sb = new StringBuilder();
                sb.Append("SELECT COUNT(*)");
                sb.Append("      FROM ALL_TABLES A");
                //sb.Append("     WHERE A.TEMPORARY = 'Y' AND UPPER(A.TABLE_NAME) = UPPER('TEMP_BKG_COMM_TBL') ")
                sb.Append("     WHERE A.OWNER = USER AND UPPER(A.TABLE_NAME) = UPPER('TEMP_BKG_COMM_TBL')");

                getCount = Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
                sb = new StringBuilder();
                if (getCount == 0)
                {
                    //sb.Append(" CREATE GLOBAL TEMPORARY TABLE TEMP_BKG_COMM_TBL ON COMMIT PRESERVE ROWS AS ")
                    //sb.Append(" SELECT * FROM BOOKING_COMMODITY_DTL")
                    sb.Append(" CREATE TABLE TEMP_BKG_COMM_TBL AS SELECT * FROM BOOKING_COMMODITY_DTL");
                    objWF.ExecuteCommands(sb.ToString());
                    sb = new StringBuilder();
                    sb.Append(" DELETE FROM TEMP_BKG_COMM_TBL ");
                    objWF.ExecuteCommands(sb.ToString());
                }
                //--------------------------------------------
                objWF.MyCommand.Connection.Open();
                var _with37 = objWF.MyCommand;
                _with37.Parameters.Clear();
                _with37.CommandType = CommandType.StoredProcedure;
                _with37.CommandText = objWF.MyUserName + ".BOOKING_TRN_CARGO_DTL_PKG.CREATE_TMP_CARGO_TBL";
                _with37.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        public object FetchTempBkgCargoDtl(string BkgTrnPks, int ContainerTypeFk, int BoxCount = 1)
        {
            WorkFlow objWF = new WorkFlow();
            DataTable dtTemp = new DataTable();
            DataSet ds = new DataSet();

            try
            {
                objWF.MyCommand.Parameters.Clear();
                objWF.OpenConnection();
                var _with38 = objWF.MyCommand.Parameters;
                _with38.Add("USER_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                _with38.Add("CONTAINER_TYPE_FK_IN", ContainerTypeFk).Direction = ParameterDirection.Input;
                _with38.Add("CARGO_CURSOR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                ds = objWF.GetDataSet("BOOKING_TRN_CARGO_DTL_PKG", "FETCH_CARGO_DTL_TEMP");
                if ((ds.Tables[0].Rows.Count == 0 | ds.Tables[0].Rows.Count < BoxCount) & ContainerTypeFk > 0)
                {
                    string ContTypeId = objWF.ExecuteScaler("SELECT C.CONTAINER_TYPE_MST_ID FROM CONTAINER_TYPE_MST_TBL C WHERE C.CONTAINER_TYPE_MST_PK=" + ContainerTypeFk);
                    //AddDefaultCargoDtl(ContTypeId, True)
                    dtTemp = GetDefaultBlankCrgoTbl(ContTypeId);
                    for (int count = 0; count <= BoxCount - ds.Tables[0].Rows.Count - 1; count++)
                    {
                        DataRow dr = null;
                        dr = ds.Tables[0].NewRow();
                        foreach (DataColumn col in dtTemp.Columns)
                        {
                            try
                            {
                                dr[col.ColumnName] = dtTemp.Rows[0][col.ColumnName];
                            }
                            catch (Exception ex)
                            {
                            }
                        }
                        ds.Tables[0].Rows.Add(dr);
                    }
                }
                for (int rowCount = 0; rowCount <= ds.Tables[0].Rows.Count - 1; rowCount++)
                {
                    ds.Tables[0].Rows[rowCount]["SL_NO"] = rowCount + 1;
                }
                ds.AcceptChanges();
                return ds;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
        }

        public object AddDefaultCargoDtl(string ContainerTypeId, bool Temporary = false)
        {
            DataTable dtTemp = new DataTable();
            int RETURN_VALUE = 0;
            dtTemp = GetDefaultBlankCrgoTbl(ContainerTypeId);
            var _with39 = dtTemp.Rows[0];
            _with39["PACK_COUNT"] = 0;
            _with39["NET_WEIGHT"] = 0;
            _with39["GROSS_WEIGHT"] = 0;
            _with39["VOLUME_IN_CBM"] = 0;
            dtTemp.AcceptChanges();
            SaveCargo(dtTemp, Temporary, false);
            WorkFlow objWF = new WorkFlow();
            try
            {
                objWF.OpenConnection();
                var _with40 = objWF.MyCommand;
                _with40.Parameters.Clear();
                _with40.CommandType = CommandType.StoredProcedure;
                if (Temporary)
                {
                    _with40.CommandText = objWF.MyUserName + ".BOOKING_TRN_CARGO_DTL_PKG.CURRENT_CARGOTRNPK_TEMP";
                }
                else
                {
                    _with40.CommandText = objWF.MyUserName + ".BOOKING_TRN_CARGO_DTL_PKG.CURRENT_CARGOTRNPK";
                }
                _with40.Parameters.Add("PK", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
                _with40.ExecuteNonQuery();
                RETURN_VALUE = Convert.ToInt32(string.IsNullOrEmpty(_with40.Parameters["PK"].Value.ToString()) ? "0" : _with40.Parameters["PK"].Value.ToString());
            }
            catch (Exception ex)
            {
            }
            finally
            {
                if (objWF.MyConnection.State == ConnectionState.Open)
                {
                    objWF.MyConnection.Close();
                }
            }
            return RETURN_VALUE;
        }

        #endregion "Temp Cargo Details"

        #region "Delete Cargo and Commodity details"

        public void CLEAR_TEMP_DATA()
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            objWK.MyCommand.Transaction = TRAN;
            try
            {
                var _with41 = objWK.MyCommand;
                _with41.Parameters.Clear();
                _with41.Parameters.Add("USER_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                _with41.CommandType = CommandType.StoredProcedure;
                _with41.CommandText = objWK.MyUserName + ".BOOKING_TRN_CARGO_DTL_PKG.CLEAR_TEMP_DATA";
                _with41.ExecuteNonQuery();
                TRAN.Commit();
            }
            catch (OracleException oraEx)
            {
                TRAN.Rollback();
                throw oraEx;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                if (objWK.MyConnection.State == ConnectionState.Open)
                {
                    objWK.MyConnection.Close();
                }
            }
        }

        public bool DeleteCargoDtl(string CargoPks, bool DeleteExceptThisCargoPks, bool Temporary)
        {
            if (string.IsNullOrEmpty(CargoPks.Trim()))
                return true;
            bool Deleted = false;
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            objWK.MyCommand.Transaction = TRAN;
            try
            {
                DeleteCargoDtl(CargoPks, DeleteExceptThisCargoPks, Temporary, objWK);
                TRAN.Commit();
            }
            catch (OracleException oraEx)
            {
                Deleted = false;
                TRAN.Rollback();
                throw oraEx;
            }
            catch (Exception ex)
            {
                Deleted = false;
                throw ex;
            }
            finally
            {
                if (objWK.MyConnection.State == ConnectionState.Open)
                {
                    objWK.MyConnection.Close();
                }
            }
            return Deleted;
        }

        public bool DeleteCargoDtl(string CargoPks, bool DeleteExceptThisCargoPks, bool Temporary, WorkFlow objWk)
        {
            if (string.IsNullOrEmpty(CargoPks))
            {
                return true;
            }
            try
            {
                short paramDelExcept = 0;
                if (DeleteExceptThisCargoPks)
                {
                    paramDelExcept = 1;
                }
                var _with42 = objWk.MyCommand;
                _with42.Parameters.Clear();
                var _with43 = _with42.Parameters;
                _with43.Add("BOOKING_TRN_CARGO_PK_IN", CargoPks).Direction = ParameterDirection.Input;
                _with43.Add("DELETE_EXCEPT_THIS_CARGOPKS", paramDelExcept).Direction = ParameterDirection.Input;
                _with43.Add("TEMPORARY", (Temporary ? 1 : 0)).Direction = ParameterDirection.Input;
                _with43.Add("RETURN_VALUE", OracleDbType.Varchar2, 200).Direction = ParameterDirection.Output;
                _with42.CommandType = CommandType.StoredProcedure;
                _with42.CommandText = objWk.MyUserName + ".BOOKING_TRN_CARGO_DTL_PKG.DELETE_CARGO_DTL";
                _with42.ExecuteNonQuery();
                return true;
            }
            catch (OracleException oraEx)
            {
                throw oraEx;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return false;
        }

        public bool DeleteCommodityDtl(string CommPks, bool DeleteExceptThisCommPks, bool Temporary)
        {
            if (string.IsNullOrEmpty(CommPks.Trim()))
                return true;
            bool Deleted = false;
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            objWK.MyCommand.Transaction = TRAN;
            try
            {
                DeleteCommodityDtl(CommPks, DeleteExceptThisCommPks, Temporary, objWK);
                TRAN.Commit();
            }
            catch (OracleException oraEx)
            {
                Deleted = false;
                TRAN.Rollback();
                throw oraEx;
            }
            catch (Exception ex)
            {
                Deleted = false;
                throw ex;
            }
            finally
            {
                if (objWK.MyConnection.State == ConnectionState.Open)
                {
                    objWK.MyConnection.Close();
                }
            }
            return Deleted;
        }

        public bool DeleteCommodityDtl(string CommPks, bool DeleteExceptThisCommPks, bool Temporary, WorkFlow objWk, string From = "")
        {
            if (string.IsNullOrEmpty(CommPks))
            {
                return true;
            }
            bool Deleted = true;
            try
            {
                short paramDelExcept = 0;
                if (DeleteExceptThisCommPks)
                {
                    paramDelExcept = 1;
                }
                var _with44 = objWk.MyCommand;
                _with44.Parameters.Clear();
                var _with45 = _with44.Parameters;
                _with45.Add("BOOKING_COMM_DTL_PK_IN", CommPks).Direction = ParameterDirection.Input;
                _with45.Add("DELETE_EXCEPT_THIS_COMMPKS", paramDelExcept).Direction = ParameterDirection.Input;
                _with45.Add("TEMPORARY", (Temporary ? 1 : 0)).Direction = ParameterDirection.Input;
                _with45.Add("FROM_FLAG", (string.IsNullOrEmpty(From) ? "" : From)).Direction = ParameterDirection.Input;
                _with45.Add("RETURN_VALUE", OracleDbType.Varchar2, 200).Direction = ParameterDirection.Output;
                _with44.CommandType = CommandType.StoredProcedure;
                _with44.CommandText = objWk.MyUserName + ".BOOKING_COMMODITY_DTL_PKG.DELETE_COMM_DTL";
                _with44.ExecuteNonQuery();
            }
            catch (OracleException oraEx)
            {
                Deleted = false;
                throw oraEx;
            }
            catch (Exception ex)
            {
                Deleted = false;
                throw ex;
            }
            return Deleted;
        }

        #endregion "Delete Cargo and Commodity details"

        #region "GetJobPkRegion"

        public string GetJobPk(string bPK, object refno)
        {
            string strSql = "";
            WorkFlow objwf = new WorkFlow();
            DataTable dt = null;
            string strJobpk = "";
            Int32 j = default(Int32);
            strSql = "SELECT J.JOB_CARD_AIR_EXP_PK,J.JOBCARD_REF_NO FROM JOB_CARD_AIR_EXP_TBL J WHERE J.BOOKING_AIR_FK=" + bPK;
            try
            {
                dt = objwf.GetDataTable(strSql);
                if (dt.Rows.Count > 0)
                {
                    strJobpk = Convert.ToString(dt.Rows[0][0]);
                    refno = dt.Rows[0][1];
                }
                return strJobpk;
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

        #endregion "GetJobPkRegion"

        #region "Fetch Marks n Numbers and Goods Description"

        public DataSet FetchMarksNumber(long BkgPk, int Status, int BizType)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                if (Status != 2)
                {
                    sb.Append(" SELECT BAT.MARKS_NUMBERS MARKS, BAT.GOODS_DESCRIPTION GOODS_DESC");
                    sb.Append(" FROM BOOKING_MST_TBL BAT");
                    sb.Append(" WHERE BAT.BOOKING_MST_PK = " + BkgPk);
                    //If BizType = 1 Then
                    //    sb.Append(" SELECT BAT.MARKS_NUMBERS MARKS, BAT.GOODS_DESCRIPTION GOODS_DESC")
                    //    sb.Append(" FROM BOOKING_MST_TBL BAT")
                    //    sb.Append(" WHERE BAT.BOOKING_AIR_PK = " & BkgPk)
                    //Else
                    //    sb.Append(" SELECT BST.MARKS_NUMBERS MARKS, BST.GOODS_DESCRIPTION GOODS_DESC")
                    //    sb.Append(" FROM BOOKING_SEA_TBL BST")
                    //    sb.Append(" WHERE BST.BOOKING_SEA_PK = " & BkgPk)
                    //End If
                }
                else
                {
                    sb.Append(" SELECT JC.MARKS_NUMBERS     MARKS,");
                    sb.Append(" JC.GOODS_DESCRIPTION GOODS_DESC");
                    sb.Append(" FROM JOB_CARD_TRN JC");
                    sb.Append(" WHERE JC.BOOKING_MST_FK = " + BkgPk);
                    //If BizType = 1 Then
                    //    sb.Append(" SELECT JCAE.MARKS_NUMBERS     MARKS,")
                    //    sb.Append(" JCAE.GOODS_DESCRIPTION GOODS_DESC")
                    //    sb.Append(" FROM JOB_CARD_AIR_EXP_TBL JCAE")
                    //    sb.Append(" WHERE JCAE.BOOKING_AIR_FK = " & BkgPk)
                    //Else
                    //    sb.Append(" SELECT JCSE.MARKS_NUMBERS     MARKS,")
                    //    sb.Append(" JCSE.GOODS_DESCRIPTION GOODS_DESC")
                    //    sb.Append(" FROM JOB_CARD_SEA_EXP_TBL JCSE")
                    //    sb.Append(" WHERE JCSE.BOOKING_SEA_FK = " & BkgPk)
                    //End If
                }
                WorkFlow objWF = new WorkFlow();
                return objWF.GetDataSet(sb.ToString());
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
        }

        #endregion "Fetch Marks n Numbers and Goods Description"

        #region "Save Other Details"

        public void SaveOtherDet(long JobPk, string Marks, string Desc, Int16 biz, OracleTransaction TRAN)
        {
            try
            {
                WorkFlow objWF = new WorkFlow();
                OracleCommand updCommand = new OracleCommand();
                objWF.OpenConnection();
                objWF.MyConnection = TRAN.Connection;
                updCommand.Connection = objWF.MyConnection;
                updCommand.Transaction = TRAN;
                updCommand.CommandType = CommandType.StoredProcedure;
                if (biz == 1)
                {
                    updCommand.CommandText = objWF.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.UPDATE_MARK_DESC";
                }
                else
                {
                    updCommand.CommandText = objWF.MyUserName + ".JOB_CARD_SEA_PKG.UPDATE_MARK_DESC";
                }
                var _with46 = updCommand.Parameters;
                _with46.Clear();
                _with46.Add("JOB_PK_IN", JobPk).Direction = ParameterDirection.Input;
                _with46.Add("MARKS_IN", getDefault(Marks, DBNull.Value)).Direction = ParameterDirection.Input;
                _with46.Add("DESC_IN", getDefault(Desc, DBNull.Value)).Direction = ParameterDirection.Input;
                updCommand.ExecuteNonQuery();
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

        #endregion "Save Other Details"

        #region "Save Cargo Details"

        public ArrayList TransferToMainTbl(WorkFlow objwf, int BOOKING_MST_FK)
        {
            try
            {
                var _with47 = objwf.MyCommand;
                _with47.Parameters.Clear();
                _with47.CommandType = CommandType.StoredProcedure;
                _with47.CommandText = objwf.MyUserName + ".BOOKING_TRN_CARGO_DTL_PKG.TRANSFER_TEMP_DATA";
                var _with48 = _with47.Parameters;
                _with48.Add("BOOKING_MST_FK_IN", BOOKING_MST_FK).Direction = ParameterDirection.Input;
                _with48.Add("USER_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                _with48.Add("RETURN_VALUE", OracleDbType.Varchar2, 150).Direction = ParameterDirection.Output;
                _with47.ExecuteNonQuery();
                arrMessage.Add((string.IsNullOrEmpty(_with47.Parameters["RETURN_VALUE"].Value.ToString()) ? "" : _with47.Parameters["RETURN_VALUE"].Value.ToString()));
                return arrMessage;
            }
            catch (OracleException oraEx)
            {
                throw oraEx;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            arrMessage.Add("");
            return arrMessage;
        }

        public ArrayList TransferToMainTbl(int BOOKING_MST_FK)
        {
            WorkFlow objwf = new WorkFlow();
            OracleTransaction TRAN = null;
            objwf.OpenConnection();
            TRAN = objwf.MyConnection.BeginTransaction();
            objwf.MyCommand.Transaction = TRAN;
            try
            {
                TransferToMainTbl(objwf, BOOKING_MST_FK);
                var _with49 = objwf.MyCommand;
                _with49.Parameters.Clear();
                _with49.CommandType = CommandType.StoredProcedure;
                _with49.CommandText = objwf.MyUserName + ".BOOKING_TRN_CARGO_DTL_PKG.TRANSFER_TEMP_DATA";
                var _with50 = _with49.Parameters;
                _with50.Add("BOOKING_MST_FK_IN", BOOKING_MST_FK).Direction = ParameterDirection.Input;
                _with50.Add("USER_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                _with50.Add("RETURN_VALUE", OracleDbType.Varchar2, 150).Direction = ParameterDirection.Output;
                arrMessage.Add((string.IsNullOrEmpty(_with49.Parameters["RETURN_VALUE"].Value.ToString()) ? "" : _with49.Parameters["RETURN_VALUE"].Value.ToString()));
                _with49.ExecuteNonQuery();
                TRAN.Commit();
            }
            catch (OracleException oraEx)
            {
                TRAN.Rollback();
                arrMessage.Add(oraEx.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                if (objwf.MyConnection.State == ConnectionState.Open)
                {
                    objwf.MyConnection.Close();
                }
            }
            return arrMessage;
        }

        public ArrayList SaveCargo(DataSet M_DataSet, WorkFlow objwf)
        {
            Int32 i = default(Int32);
            Int32 intPKValue = default(Int32);
            Int32 RecAfct = default(Int32);
            int BookingCargoPk = 0;
            OracleCommand insCommand = new OracleCommand();
            RecAfct = 0;
            string SelectedCargoPks = "";
            string SelectedTrnPks = "";
            try
            {
                foreach (DataTable _tbl in M_DataSet.Tables)
                {
                    if (_tbl.Rows.Count > 0)
                    {
                        for (i = 0; i <= _tbl.Rows.Count - 1; i++)
                        {
                            BookingCargoPk = 0;
                            try
                            {
                                BookingCargoPk = Convert.ToInt32(_tbl.Rows[i]["BOOKING_TRN_CARGO_PK"]);
                            }
                            catch (Exception ex)
                            {
                            }
                            if (BookingCargoPk == 0)
                            {
                                var _with51 = objwf.MyCommand;
                                _with51.Parameters.Clear();
                                _with51.CommandType = CommandType.StoredProcedure;
                                _with51.CommandText = objwf.MyUserName + ".BOOKING_TRN_CARGO_DTL_PKG.BOOKING_TRN_CARGO_DTL_INS";
                                var _with52 = _with51.Parameters;
                                _with52.Add("BOOKING_TRN_FK_IN", _tbl.Rows[i]["BOOKING_TRN_PK"]).Direction = ParameterDirection.Input;
                                _with52.Add("PACK_COUNT_IN", (string.IsNullOrEmpty(_tbl.Rows[i]["PACK_COUNT"].ToString()) ? 0 : _tbl.Rows[i]["PACK_COUNT"])).Direction = ParameterDirection.Input;
                                _with52.Add("GROSS_WEIGHT_IN", (string.IsNullOrEmpty(_tbl.Rows[i]["GROSS_WEIGHT"].ToString()) ? 0 : _tbl.Rows[i]["GROSS_WEIGHT"])).Direction = ParameterDirection.Input;
                                _with52.Add("NET_WEIGHT_IN", (string.IsNullOrEmpty(_tbl.Rows[i]["NET_WEIGHT"].ToString()) ? 0 : _tbl.Rows[i]["NET_WEIGHT"])).Direction = ParameterDirection.Input;
                                _with52.Add("VOLUME_IN_CBM_IN", (string.IsNullOrEmpty(_tbl.Rows[i]["VOLUME_IN_CBM"].ToString()) ? 0 : _tbl.Rows[i]["VOLUME_IN_CBM"])).Direction = ParameterDirection.Input;
                                _with52.Add("REMARK_IN", (string.IsNullOrEmpty(_tbl.Rows[i]["REMARK"].ToString()) ? DBNull.Value : _tbl.Rows[i]["REMARK"])).Direction = ParameterDirection.Input;
                                _with52.Add("CONTAINER_NUMBER_IN", (string.IsNullOrEmpty(_tbl.Rows[i]["CONTAINER_NUMBER"].ToString()) ? DBNull.Value : _tbl.Rows[i]["CONTAINER_NUMBER"])).Direction = ParameterDirection.Input;
                                _with52.Add("CONTAINER_TYPE_FK_IN", (string.IsNullOrEmpty(_tbl.Rows[i]["CONTAINER_TYPE_MST_FK"].ToString()) ? DBNull.Value : _tbl.Rows[i]["CONTAINER_TYPE_MST_FK"])).Direction = ParameterDirection.Input;
                                _with52.Add("COMMODITY_FKS_IN", (string.IsNullOrEmpty(_tbl.Rows[i]["COMMODITY_FKS"].ToString()) ? DBNull.Value : _tbl.Rows[i]["COMMODITY_FKS"])).Direction = ParameterDirection.Input;
                                _with52.Add("CREATED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                                _with52.Add("CONFIG_MST_FK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                                _with52.Add("RETURN_VALUE", OracleDbType.Int32, 10, "LOCATION_DWORKFLOW_PK").Direction = ParameterDirection.Output;
                                _with51.ExecuteNonQuery();
                                try
                                {
                                    BookingCargoPk = Convert.ToInt32(string.IsNullOrEmpty(_with51.Parameters["RETURN_VALUE"].Value.ToString()) ? "" : _with51.Parameters["RETURN_VALUE"].Value.ToString());
                                }
                                catch (Exception ex)
                                {
                                }
                            }
                            else if (BookingCargoPk > 0)
                            {
                                var _with53 = objwf.MyCommand;
                                _with53.Parameters.Clear();
                                _with53.CommandType = CommandType.StoredProcedure;
                                _with53.CommandText = objwf.MyUserName + ".BOOKING_TRN_CARGO_DTL_PKG.BOOKING_TRN_CARGO_DTL_UPD";
                                var _with54 = _with53.Parameters;
                                _with54.Add("BOOKING_TRN_CARGO_PK_IN", _tbl.Rows[i]["BOOKING_TRN_CARGO_PK"]).Direction = ParameterDirection.Input;
                                _with54.Add("BOOKING_TRN_FK_IN", _tbl.Rows[i]["BOOKING_TRN_PK"]).Direction = ParameterDirection.Input;
                                _with54.Add("PACK_COUNT_IN", (string.IsNullOrEmpty(_tbl.Rows[i]["PACK_COUNT"].ToString()) ? 0 : _tbl.Rows[i]["PACK_COUNT"])).Direction = ParameterDirection.Input;
                                _with54.Add("GROSS_WEIGHT_IN", (string.IsNullOrEmpty(_tbl.Rows[i]["GROSS_WEIGHT"].ToString()) ? 0 : _tbl.Rows[i]["GROSS_WEIGHT"])).Direction = ParameterDirection.Input;
                                _with54.Add("NET_WEIGHT_IN", (string.IsNullOrEmpty(_tbl.Rows[i]["NET_WEIGHT"].ToString()) ? 0 : _tbl.Rows[i]["NET_WEIGHT"])).Direction = ParameterDirection.Input;
                                _with54.Add("VOLUME_IN_CBM_IN", (string.IsNullOrEmpty(_tbl.Rows[i]["VOLUME_IN_CBM"].ToString()) ? 0 : _tbl.Rows[i]["VOLUME_IN_CBM"])).Direction = ParameterDirection.Input;
                                _with54.Add("REMARK_IN", (string.IsNullOrEmpty(_tbl.Rows[i]["REMARK"].ToString()) ? DBNull.Value : _tbl.Rows[i]["REMARK"])).Direction = ParameterDirection.Input;
                                _with54.Add("COMMODITY_FKS_IN", (string.IsNullOrEmpty(_tbl.Rows[i]["COMMODITY_FKS"].ToString()) ? DBNull.Value : _tbl.Rows[i]["COMMODITY_FKS"])).Direction = ParameterDirection.Input;
                                _with54.Add("LAST_MODIFIED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                                //.Add("VERSION_NO_IN", M_DataSet.Tables(0).Rows(i).Item("VERSION_NO")).Direction = ParameterDirection.Input
                                _with54.Add("CONFIG_MST_FK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                                _with54.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                                _with53.ExecuteNonQuery();
                            }

                            _tbl.Rows[i]["BOOKING_TRN_CARGO_PK"] = BookingCargoPk;
                            RecAfct += 1;
                            try
                            {
                                if (Convert.ToInt32(_tbl.Rows[i]["BOOKING_TRN_PK"]) > 0)
                                {
                                    if (string.IsNullOrEmpty(SelectedTrnPks))
                                    {
                                        SelectedTrnPks = Convert.ToString(_tbl.Rows[i]["BOOKING_TRN_PK"]);
                                    }
                                    else
                                    {
                                        SelectedTrnPks += "," + _tbl.Rows[i]["BOOKING_TRN_PK"];
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                            }
                            if (BookingCargoPk > 0)
                            {
                                if (string.IsNullOrEmpty(SelectedCargoPks))
                                {
                                    SelectedCargoPks = Convert.ToString(BookingCargoPk);
                                }
                                else
                                {
                                    SelectedCargoPks += "," + BookingCargoPk;
                                }
                            }
                        }
                    }
                }
                if (RecAfct > 0)
                {
                    arrMessage.Add("All data saved successfully");
                    if (!string.IsNullOrEmpty(SelectedCargoPks) & !string.IsNullOrEmpty(SelectedTrnPks))
                    {
                        DeleteCargoDtl(SelectedCargoPks, true, false, objwf);
                    }
                    return arrMessage;
                }
            }
            catch (OracleException oraEx)
            {
                throw oraEx;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return new ArrayList();
        }

        public ArrayList SaveCargo(DataSet M_DataSet)
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            objWK.MyCommand.Transaction = TRAN;
            SaveCargo(M_DataSet, objWK);
            try
            {
                if (Convert.ToString(arrMessage[arrMessage.Count - 1]).ToUpper().IndexOf("SAVED") >= 0)
                {
                    TRAN.Commit();
                    return arrMessage;
                }
                else
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
            }
            catch (OracleException oraEx)
            {
                TRAN.Rollback();
                arrMessage.Add(oraEx.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                if (objWK.MyConnection.State == ConnectionState.Open)
                {
                    objWK.MyConnection.Close();
                }
            }
            return arrMessage;
        }

        public ArrayList SaveCargo(DataTable _tbl, bool Temporary = false, bool DeleteRemovedCargos = false)
        {
            Int32 i = default(Int32);
            Int32 intPKValue = default(Int32);
            Int32 RecAfct = default(Int32);
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            int BookingCargoPk = 0;
            RecAfct = 0;
            string SelectedCargoPks = "";
            string SelectedTrnPks = "";
            try
            {
                if (_tbl.Rows.Count > 0)
                {
                    for (i = 0; i <= _tbl.Rows.Count - 1; i++)
                    {
                        BookingCargoPk = 0;
                        try
                        {
                            BookingCargoPk = Convert.ToInt32(_tbl.Rows[i]["BOOKING_TRN_CARGO_PK"]);
                        }
                        catch (Exception ex)
                        {
                        }
                        var _with55 = insCommand;
                        insCommand.Parameters.Clear();
                        _with55.Connection = objWK.MyConnection;
                        _with55.CommandType = CommandType.StoredProcedure;
                        var _with56 = _with55.Parameters;
                        _with56.Add("BOOKING_TRN_FK_IN", _tbl.Rows[i]["BOOKING_TRN_PK"]).Direction = ParameterDirection.Input;
                        _with56.Add("PACK_COUNT_IN", (string.IsNullOrEmpty(_tbl.Rows[i]["PACK_COUNT"].ToString()) ? 0 : _tbl.Rows[i]["PACK_COUNT"])).Direction = ParameterDirection.Input;
                        _with56.Add("GROSS_WEIGHT_IN", (string.IsNullOrEmpty(_tbl.Rows[i]["GROSS_WEIGHT"].ToString()) ? 0 : _tbl.Rows[i]["GROSS_WEIGHT"])).Direction = ParameterDirection.Input;
                        _with56.Add("NET_WEIGHT_IN", (string.IsNullOrEmpty(_tbl.Rows[i]["NET_WEIGHT"].ToString()) ? 0 : _tbl.Rows[i]["NET_WEIGHT"])).Direction = ParameterDirection.Input;
                        _with56.Add("VOLUME_IN_CBM_IN", (string.IsNullOrEmpty(_tbl.Rows[i]["VOLUME_IN_CBM"].ToString()) ? 0 : _tbl.Rows[i]["VOLUME_IN_CBM"])).Direction = ParameterDirection.Input;
                        _with56.Add("REMARK_IN", (string.IsNullOrEmpty(_tbl.Rows[i]["REMARK"].ToString()) ? DBNull.Value : _tbl.Rows[i]["REMARK"])).Direction = ParameterDirection.Input;
                        _with56.Add("CONTAINER_NUMBER_IN", (string.IsNullOrEmpty(_tbl.Rows[i]["CONTAINER_NUMBER"].ToString()) ? DBNull.Value : _tbl.Rows[i]["CONTAINER_NUMBER"])).Direction = ParameterDirection.Input;
                        _with56.Add("CONTAINER_TYPE_FK_IN", (string.IsNullOrEmpty(_tbl.Rows[i]["CONTAINER_TYPE_MST_FK"].ToString()) ? DBNull.Value : _tbl.Rows[i]["CONTAINER_TYPE_MST_FK"])).Direction = ParameterDirection.Input;
                        _with56.Add("COMMODITY_FKS_IN", (string.IsNullOrEmpty(_tbl.Rows[i]["COMMODITY_FKS"].ToString()) ? DBNull.Value : _tbl.Rows[i]["COMMODITY_FKS"])).Direction = ParameterDirection.Input;
                        _with56.Add("CREATED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                        _with56.Add("CONFIG_MST_FK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                        _with56.Add("RETURN_VALUE", OracleDbType.Int32, 10, "LOCATION_DWORKFLOW_PK").Direction = ParameterDirection.Output;

                        var _with57 = updCommand;
                        updCommand.Parameters.Clear();
                        _with57.Connection = objWK.MyConnection;
                        _with57.CommandType = CommandType.StoredProcedure;
                        var _with58 = _with57.Parameters;
                        _with58.Add("BOOKING_TRN_CARGO_PK_IN", _tbl.Rows[i]["BOOKING_TRN_CARGO_PK"]).Direction = ParameterDirection.Input;
                        _with58.Add("BOOKING_TRN_FK_IN", _tbl.Rows[i]["BOOKING_TRN_PK"]).Direction = ParameterDirection.Input;
                        _with58.Add("PACK_COUNT_IN", (string.IsNullOrEmpty(_tbl.Rows[i]["PACK_COUNT"].ToString()) ? 0 : _tbl.Rows[i]["PACK_COUNT"])).Direction = ParameterDirection.Input;
                        _with58.Add("GROSS_WEIGHT_IN", (string.IsNullOrEmpty(_tbl.Rows[i]["GROSS_WEIGHT"].ToString()) ? 0 : _tbl.Rows[i]["GROSS_WEIGHT"])).Direction = ParameterDirection.Input;
                        _with58.Add("NET_WEIGHT_IN", (string.IsNullOrEmpty(_tbl.Rows[i]["NET_WEIGHT"].ToString()) ? 0 : _tbl.Rows[i]["NET_WEIGHT"])).Direction = ParameterDirection.Input;
                        _with58.Add("VOLUME_IN_CBM_IN", (string.IsNullOrEmpty(_tbl.Rows[i]["VOLUME_IN_CBM"].ToString()) ? 0 : _tbl.Rows[i]["VOLUME_IN_CBM"])).Direction = ParameterDirection.Input;
                        _with58.Add("REMARK_IN", (string.IsNullOrEmpty(_tbl.Rows[i]["REMARK"].ToString()) ? DBNull.Value : _tbl.Rows[i]["REMARK"])).Direction = ParameterDirection.Input;
                        _with58.Add("COMMODITY_FKS_IN", (string.IsNullOrEmpty(_tbl.Rows[i]["COMMODITY_FKS"].ToString()) ? DBNull.Value : _tbl.Rows[i]["COMMODITY_FKS"])).Direction = ParameterDirection.Input;
                        _with58.Add("LAST_MODIFIED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                        //.Add("VERSION_NO_IN", M_DataSet.Tables(0).Rows(i).Item("VERSION_NO")).Direction = ParameterDirection.Input
                        _with58.Add("CONFIG_MST_FK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                        _with58.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        if (Temporary)
                        {
                            insCommand.CommandText = objWK.MyUserName + ".BOOKING_TRN_CARGO_DTL_PKG.INSERT_CARGO_TEMP";
                            updCommand.CommandText = objWK.MyUserName + ".BOOKING_TRN_CARGO_DTL_PKG.UPDATE_CARGO_TEMP";
                        }
                        else
                        {
                            insCommand.CommandText = objWK.MyUserName + ".BOOKING_TRN_CARGO_DTL_PKG.BOOKING_TRN_CARGO_DTL_INS";
                            updCommand.CommandText = objWK.MyUserName + ".BOOKING_TRN_CARGO_DTL_PKG.BOOKING_TRN_CARGO_DTL_UPD";
                        }

                        var _with59 = objWK.MyDataAdapter;
                        if (BookingCargoPk == 0)
                        {
                            _with59.InsertCommand = insCommand;
                            _with59.InsertCommand.Transaction = TRAN;
                            _with59.InsertCommand.ExecuteNonQuery();
                            try
                            {
                                BookingCargoPk = Convert.ToInt32(string.IsNullOrEmpty(_with59.InsertCommand.Parameters["RETURN_VALUE"].Value.ToString()) ? "" : _with59.InsertCommand.Parameters["RETURN_VALUE"].Value.ToString());
                            }
                            catch (Exception ex)
                            {
                            }
                        }
                        else
                        {
                            _with59.UpdateCommand = updCommand;
                            _with59.UpdateCommand.Transaction = TRAN;
                            _with59.UpdateCommand.ExecuteNonQuery();
                        }
                        _tbl.Rows[i]["BOOKING_TRN_CARGO_PK"] = BookingCargoPk;
                        RecAfct += 1;
                        try
                        {
                            if (Convert.ToInt32(_tbl.Rows[i]["BOOKING_TRN_PK"]) > 0)
                            {
                                if (string.IsNullOrEmpty(SelectedTrnPks))
                                {
                                    SelectedTrnPks = Convert.ToString(_tbl.Rows[i]["BOOKING_TRN_PK"]);
                                }
                                else
                                {
                                    SelectedTrnPks += "," + _tbl.Rows[i]["BOOKING_TRN_PK"];
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                        }
                        if (BookingCargoPk > 0)
                        {
                            if (string.IsNullOrEmpty(SelectedCargoPks))
                            {
                                SelectedCargoPks = Convert.ToString(BookingCargoPk);
                            }
                            else
                            {
                                SelectedCargoPks += "," + BookingCargoPk;
                            }
                        }
                    }
                }

                if (RecAfct > 0)
                {
                    if (arrMessage.Count == 0)
                    {
                        TRAN.Commit();
                        if (DeleteRemovedCargos)
                            DeleteCargoDtl(SelectedCargoPks, DeleteRemovedCargos, Temporary);
                        arrMessage.Add("All Data Saved Successfully");
                        arrMessage.Add(intPKValue);
                        return arrMessage;
                    }
                    else
                    {
                        TRAN.Rollback();
                        return arrMessage;
                    }
                }
                else
                {
                    TRAN.Rollback();
                }
            }
            catch (OracleException oraEx)
            {
                TRAN.Rollback();
                arrMessage.Add(oraEx.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                if (objWK.MyConnection.State == ConnectionState.Open)
                {
                    objWK.MyConnection.Close();
                }
            }
            return new ArrayList();
        }

        #endregion "Save Cargo Details"

        #region "Fetch Commodity Detail PopUp"

        public DataSet FetchCommodityDetails(int CargoTrnPK, string Commodity_Fks, string From_Flag = "", int BizType = 2, int CommDetIndex = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataTable dtComm = new DataTable();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            int RcdCnt = 0;
            DataSet dsMain = new DataSet();
            DataTable dtTemp = new DataTable();
            CommDetIndex = 0;
            try
            {
                objWF.MyCommand.Parameters.Clear();
                var _with60 = objWF.MyCommand.Parameters;
                _with60.Add("BKG_TRN_CARGO_FK_IN", CargoTrnPK).Direction = ParameterDirection.Input;
                _with60.Add("FROM_FLAG_IN", From_Flag).Direction = ParameterDirection.Input;
                _with60.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with60.Add("COMMDITY_PKS_IN", Commodity_Fks).Direction = ParameterDirection.Input;
                _with60.Add("COMM_CURSOR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dsMain = objWF.GetDataSet("BOOKING_COMMODITY_DTL_PKG", "FETCH_COMMODITY_DTL");
                if (dsMain.Tables[0].Rows.Count == 0)
                {
                    dtTemp = GetBlankCommDetails(CargoTrnPK);
                    foreach (DataRow row in dtTemp.Rows)
                    {
                        DataRow dr = null;
                        dr = dsMain.Tables[CargoDetIndex].NewRow();
                        foreach (DataColumn col in dtTemp.Columns)
                        {
                            try
                            {
                                dr[col.ColumnName] = row[col.ColumnName];
                            }
                            catch (Exception ex)
                            {
                            }
                        }
                        dsMain.Tables[0].Rows.Add(dr);
                    }
                }
                return dsMain;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
        }

        public DataTable GetBlankCommDetails(int CargoTrnPK = 0, string Commodity_Fks = "", string From_Flag = "")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            DataSet ds = new DataSet();
            string commodityPks = "";
            List<int> commPkList = new List<int>();
            foreach (string _pk in Commodity_Fks.Split(','))
            {
                try
                {
                    commPkList.Add(Convert.ToInt32(_pk));
                }
                catch (Exception ex)
                {
                }
            }
            foreach (int _pk in commPkList)
            {
                if (string.IsNullOrEmpty(commodityPks))
                    commodityPks = _pk.ToString();
                else
                    commodityPks += "," + _pk;
            }
            try
            {
                objWF.MyCommand.Parameters.Clear();
                var _with61 = objWF.MyCommand.Parameters;
                _with61.Add("BKG_TRN_CARGO_FK_IN", CargoTrnPK).Direction = ParameterDirection.Input;
                _with61.Add("COMMODITY_MST_FKS_IN", (string.IsNullOrEmpty(commodityPks) | commodityPks.ToUpper() == "NULL" ? "" : commodityPks)).Direction = ParameterDirection.Input;
                _with61.Add("COMM_CURSOR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                return objWF.GetDataTable("BOOKING_COMMODITY_DTL_PKG", "GET_BLANK_COMM_DTL");
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
        }

        public object FetchTempCommDtl(int CargoTrnPK, string Commodity_Fks, string From_Flag = "")
        {
            WorkFlow objWF = new WorkFlow();
            DataTable dtTemp = new DataTable();
            DataSet ds = new DataSet();
            try
            {
                objWF.MyCommand.Parameters.Clear();
                objWF.OpenConnection();
                var _with62 = objWF.MyCommand.Parameters;
                _with62.Add("BKG_TRN_CARGO_FK_IN", CargoTrnPK).Direction = ParameterDirection.Input;
                _with62.Add("FROM_FLAG_IN", From_Flag).Direction = ParameterDirection.Input;
                _with62.Add("COMM_CURSOR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                ds = objWF.GetDataSet("BOOKING_COMMODITY_DTL_PKG", "FETCH_COMMODITY_DTL_TEMP");
                if (ds.Tables[0].Rows.Count == 0)
                {
                    dtTemp = GetBlankCommDetails(CargoTrnPK, Commodity_Fks);
                    foreach (DataRow row in dtTemp.Rows)
                    {
                        DataRow dr = null;
                        dr = ds.Tables[0].NewRow();
                        foreach (DataColumn col in dtTemp.Columns)
                        {
                            dr[col.ColumnName] = row[col.ColumnName];
                        }
                        ds.Tables[0].Rows.Add(dr);
                    }
                }
                return ds;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
        }

        #endregion "Fetch Commodity Detail PopUp"

        #region "Fetch Total Text Boxes"

        public DataSet FetchTotal(int CargoTrnPK, string Commodity_Fks, string From = "")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            int RcdCnt = 0;
            try
            {
                if (From == "BOOKING")
                {
                    sb.Append("SELECT NVL(SUM(BC.PACK_COUNT),0) PACK_COUNT,");
                    sb.Append("       NVL(SUM(BC.NET_WEIGHT),0) NET_WEIGHT,");
                    sb.Append("       NVL(SUM(BC.GROSS_WEIGHT),0) GROSS_WEIGHT,");
                    sb.Append("       NVL(SUM(BC.VOLUME_IN_CBM), 0) VOLUME_IN_CBM");
                    sb.Append("  FROM BOOKING_COMMODITY_DTL BC");
                    sb.Append(" WHERE BC.BOOKING_CARGO_DTL_FK = " + CargoTrnPK);
                }
                else if (From == "JOBCARD" | From == "HBL")
                {
                    sb.Append("SELECT NVL(SUM(BC.PACK_COUNT),0) PACK_COUNT,");
                    sb.Append("       NVL(SUM(BC.NET_WEIGHT),0) NET_WEIGHT,");
                    sb.Append("       NVL(SUM(BC.GROSS_WEIGHT),0) GROSS_WEIGHT,");
                    sb.Append("       NVL(SUM(BC.VOLUME_IN_CBM), 0) VOLUME_IN_CBM");
                    sb.Append("  FROM JOBCARD_COMMODITY_DTL BC");
                    sb.Append(" WHERE BC.JOB_TRN_CONT_FK = " + CargoTrnPK);
                }
                else if (From == "IMPORTJOBCARD" | From == "MANUALJOBCARD")
                {
                    sb.Append("SELECT NVL(SUM(BC.PACK_COUNT),0) PACK_COUNT,");
                    sb.Append("       NVL(SUM(BC.NET_WEIGHT),0) NET_WEIGHT,");
                    sb.Append("       NVL(SUM(BC.GROSS_WEIGHT),0) GROSS_WEIGHT,");
                    sb.Append("       NVL(SUM(BC.VOLUME_IN_CBM), 0) VOLUME_IN_CBM");
                    sb.Append("  FROM JOBCARD_COMMODITY_DTL_IMP BC");
                    sb.Append(" WHERE BC.JOB_TRN_CONT_IMP_FK = " + CargoTrnPK);
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

        #endregion "Fetch Total Text Boxes"

        #region "Fetch Container Detail"

        public DataSet FetchCtrDtls(string CntrID)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            int RcdCnt = 0;
            try
            {
                CntrID = CntrID.Trim(Convert.ToChar("'")).Trim(' ');

                sb.Append("SELECT CTM.CONTAINER_TYPE_MST_PK,");
                sb.Append("       CTM.CONTAINER_TYPE_MST_ID,");
                sb.Append("       CTM.CONTAINER_TYPE_NAME,");
                sb.Append("       CTM.CONTAINER_KIND,");
                sb.Append("       CTM.CONTAINER_TAREWEIGHT_TONE,");
                sb.Append("       CTM.CONTAINER_MAX_CAPACITY_TONE,");
                sb.Append("       CTM.TEU_FACTOR,");
                sb.Append("       CTM.VOLUME,");
                sb.Append("       CTM.CONTAINER_LENGTH_FT,");
                sb.Append("       CTM.CONTAINER_WIDTH_FT,");
                sb.Append("       CTM.CONTAINER_HEIGHT_FT");
                sb.Append("  FROM CONTAINER_TYPE_MST_TBL CTM");
                sb.Append(" WHERE CTM.CONTAINER_TYPE_MST_ID = '" + CntrID + "'");

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

        #endregion "Fetch Container Detail"

        #region "Save Commodity Details"

        public ArrayList SaveCommodityDtl(DataSet dsMain, string From = "", bool Temporary = false, string BizType = "")
        {
            arrMessage.Clear();
            try
            {

                foreach (DataTable _tbl in dsMain.Tables)
                {
                    DataSet ds = new DataSet();
                    ds.Tables.Add(_tbl);
                    SaveCommodityDtl(ds, From, Temporary, BizType);
                }
                arrMessage.Clear();
                arrMessage.Add("All data Saved successfully");
                return arrMessage;
            }
            catch (OracleException oraexp)
            {
                arrMessage.Clear();
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                arrMessage.Clear();
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
            finally
            {
            }
        }

        public ArrayList SaveCommodityDtl(DataTable dtMain, string From = "", bool Temporary = false, bool DeleteRemovedComms = false, string BizType = "")
        {
            WorkFlow objWK = new WorkFlow();
            int RwCnt = 0;
            string dimFormat = "#0.00";
            var strNull = DBNull.Value;
            OracleTransaction TRAN = null;
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            arrMessage.Clear();
            objWK.MyCommand.Transaction = TRAN;
            string SelectedCargoPks = "";
            string SelectedCommPks = "";
            int CommPk = 0;
            try
            {
                foreach (DataRow _row in dtMain.Rows)
                {
                    CommPk = 0;
                    try
                    {
                        CommPk = Convert.ToInt32(_row["BOOKING_COMM_DTL_PK"]);
                    }
                    catch (Exception ex)
                    {
                    }
                    if (CommPk == 0)
                    {
                        var _with63 = objWK.MyCommand;
                        _with63.Parameters.Clear();
                        _with63.CommandType = CommandType.StoredProcedure;
                        if (Temporary)
                        {
                            _with63.CommandText = objWK.MyUserName + ".BOOKING_COMMODITY_DTL_PKG.INSERT_COMM_TEMP";
                        }
                        else
                        {
                            _with63.CommandText = objWK.MyUserName + ".BOOKING_COMMODITY_DTL_PKG.BOOKING_COMMODITY_DTL_INS";
                        }

                        _with63.Parameters.Add("BOOKING_CARGO_DTL_FK_IN", _row["BOOKING_CARGO_DTL_FK"]).Direction = ParameterDirection.Input;
                        _with63.Parameters["BOOKING_CARGO_DTL_FK_IN"].SourceVersion = DataRowVersion.Current;

                        _with63.Parameters.Add("COMMODITY_MST_FK_IN", _row["COMMODITY_MST_FK"]).Direction = ParameterDirection.Input;
                        _with63.Parameters["COMMODITY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        _with63.Parameters.Add("PACK_TYPE_FK_IN", _row["PACK_TYPE_FK"]).Direction = ParameterDirection.Input;
                        _with63.Parameters["PACK_TYPE_FK_IN"].SourceVersion = DataRowVersion.Current;

                        _with63.Parameters.Add("PACK_COUNT_IN", _row["PACK_COUNT"]).Direction = ParameterDirection.Input;
                        _with63.Parameters["PACK_COUNT_IN"].SourceVersion = DataRowVersion.Current;

                        _with63.Parameters.Add("GROSS_WEIGHT_IN", _row["GROSS_WEIGHT"]).Direction = ParameterDirection.Input;
                        _with63.Parameters["GROSS_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                        _with63.Parameters.Add("NET_WEIGHT_IN", _row["NET_WEIGHT"]).Direction = ParameterDirection.Input;
                        _with63.Parameters["NET_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                        _with63.Parameters.Add("VOLUME_IN_CBM_IN", _row["VOLUME_IN_CBM"]).Direction = ParameterDirection.Input;
                        _with63.Parameters["VOLUME_IN_CBM_IN"].SourceVersion = DataRowVersion.Current;

                        //------------------------------------------------------------------------------
                        if (string.IsNullOrEmpty(_row["LENGTH"].ToString()))
                        {
                            _with63.Parameters.Add("LENGTH_IN", strNull).Direction = ParameterDirection.Input;
                            _with63.Parameters["LENGTH_IN"].SourceVersion = DataRowVersion.Current;
                        }
                        else
                        {
                            _with63.Parameters.Add("LENGTH_IN", Convert.ToDouble(_row["LENGTH"])).Direction = ParameterDirection.Input;
                            _with63.Parameters["LENGTH_IN"].SourceVersion = DataRowVersion.Current;
                        }
                        if (string.IsNullOrEmpty(_row["WIDTH"].ToString()))
                        {
                            _with63.Parameters.Add("WIDTH_IN", strNull).Direction = ParameterDirection.Input;
                            _with63.Parameters["WIDTH_IN"].SourceVersion = DataRowVersion.Current;
                        }
                        else
                        {
                            _with63.Parameters.Add("WIDTH_IN", Convert.ToDouble(_row["WIDTH"])).Direction = ParameterDirection.Input;
                            _with63.Parameters["WIDTH_IN"].SourceVersion = DataRowVersion.Current;
                        }
                        if (string.IsNullOrEmpty(_row["HEIGHT"].ToString()))
                        {
                            _with63.Parameters.Add("HEIGHT_IN", strNull).Direction = ParameterDirection.Input;
                            _with63.Parameters["HEIGHT_IN"].SourceVersion = DataRowVersion.Current;
                        }
                        else
                        {
                            _with63.Parameters.Add("HEIGHT_IN", Convert.ToDouble(_row["HEIGHT"])).Direction = ParameterDirection.Input;
                            _with63.Parameters["HEIGHT_IN"].SourceVersion = DataRowVersion.Current;
                        }
                        if (string.IsNullOrEmpty(_row["UOM"].ToString()))
                        {
                            _with63.Parameters.Add("UOM_IN", strNull).Direction = ParameterDirection.Input;
                            _with63.Parameters["UOM_IN"].SourceVersion = DataRowVersion.Current;
                        }
                        else
                        {
                            _with63.Parameters.Add("UOM_IN", Convert.ToInt32(_row["UOM"])).Direction = ParameterDirection.Input;
                            _with63.Parameters["UOM_IN"].SourceVersion = DataRowVersion.Current;
                        }
                        if (string.IsNullOrEmpty(_row["WEIGHT_PER_PKG"].ToString()))
                        {
                            _with63.Parameters.Add("WEIGHT_PER_PKG_IN", strNull).Direction = ParameterDirection.Input;
                            _with63.Parameters["WEIGHT_PER_PKG_IN"].SourceVersion = DataRowVersion.Current;
                        }
                        else
                        {
                            _with63.Parameters.Add("WEIGHT_PER_PKG_IN", Convert.ToDouble(_row["WEIGHT_PER_PKG"])).Direction = ParameterDirection.Input;
                            _with63.Parameters["WEIGHT_PER_PKG_IN"].SourceVersion = DataRowVersion.Current;
                        }
                        if (string.IsNullOrEmpty(_row["CALCULATED_WEIGHT"].ToString()))
                        {
                            _with63.Parameters.Add("CALCULATED_WEIGHT_IN", strNull).Direction = ParameterDirection.Input;
                            _with63.Parameters["CALCULATED_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;
                        }
                        else
                        {
                            _with63.Parameters.Add("CALCULATED_WEIGHT_IN", Convert.ToDouble(_row["CALCULATED_WEIGHT"])).Direction = ParameterDirection.Input;
                            _with63.Parameters["CALCULATED_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;
                        }
                        if (string.IsNullOrEmpty(_row["CHARGEABLE_WEIGHT"].ToString()))
                        {
                            _with63.Parameters.Add("CHARGEABLE_WEIGHT_IN", strNull).Direction = ParameterDirection.Input;
                            _with63.Parameters["CHARGEABLE_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;
                        }
                        else
                        {
                            _with63.Parameters.Add("CHARGEABLE_WEIGHT_IN", Convert.ToDouble(_row["CHARGEABLE_WEIGHT"])).Direction = ParameterDirection.Input;
                            _with63.Parameters["CHARGEABLE_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;
                        }
                        //------------------------------------------------------------------------------

                        _with63.Parameters.Add("CREATED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                        _with63.Parameters.Add("FROM_FLAG_IN", From).Direction = ParameterDirection.Input;
                        _with63.Parameters.Add("BIZ_TYPE_IN", Convert.ToInt32(BizType)).Direction = ParameterDirection.Input;

                        _with63.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;

                        _with63.ExecuteNonQuery();
                        CommPk = Convert.ToInt32(string.IsNullOrEmpty(_with63.Parameters["RETURN_VALUE"].Value.ToString()) ? "" : _with63.Parameters["RETURN_VALUE"].Value.ToString());
                        _row["BOOKING_COMM_DTL_PK"] = CommPk;
                    }
                    else
                    {
                        var _with64 = objWK.MyCommand;
                        _with64.CommandType = CommandType.StoredProcedure;
                        _with64.Parameters.Clear();
                        if (Temporary)
                        {
                            _with64.CommandText = objWK.MyUserName + ".BOOKING_COMMODITY_DTL_PKG.UPDATE_COMM_TEMP";
                        }
                        else
                        {
                            _with64.CommandText = objWK.MyUserName + ".BOOKING_COMMODITY_DTL_PKG.BOOKING_COMMODITY_DTL_UPD";
                        }

                        _with64.Parameters.Add("BOOKING_COMM_DTL_PK_IN", _row["BOOKING_COMM_DTL_PK"]).Direction = ParameterDirection.Input;
                        _with64.Parameters["BOOKING_COMM_DTL_PK_IN"].SourceVersion = DataRowVersion.Current;

                        _with64.Parameters.Add("BOOKING_CARGO_DTL_FK_IN", _row["BOOKING_CARGO_DTL_FK"]).Direction = ParameterDirection.Input;
                        _with64.Parameters["BOOKING_CARGO_DTL_FK_IN"].SourceVersion = DataRowVersion.Current;

                        _with64.Parameters.Add("COMMODITY_MST_FK_IN", _row["COMMODITY_MST_FK"]).Direction = ParameterDirection.Input;
                        _with64.Parameters["COMMODITY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        _with64.Parameters.Add("PACK_TYPE_FK_IN", _row["PACK_TYPE_FK"]).Direction = ParameterDirection.Input;
                        _with64.Parameters["PACK_TYPE_FK_IN"].SourceVersion = DataRowVersion.Current;

                        _with64.Parameters.Add("PACK_COUNT_IN", _row["PACK_COUNT"]).Direction = ParameterDirection.Input;
                        _with64.Parameters["PACK_COUNT_IN"].SourceVersion = DataRowVersion.Current;

                        _with64.Parameters.Add("GROSS_WEIGHT_IN", _row["GROSS_WEIGHT"]).Direction = ParameterDirection.Input;
                        _with64.Parameters["GROSS_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                        _with64.Parameters.Add("NET_WEIGHT_IN", _row["NET_WEIGHT"]).Direction = ParameterDirection.Input;
                        _with64.Parameters["NET_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                        _with64.Parameters.Add("VOLUME_IN_CBM_IN", _row["VOLUME_IN_CBM"]).Direction = ParameterDirection.Input;
                        _with64.Parameters["VOLUME_IN_CBM_IN"].SourceVersion = DataRowVersion.Current;

                        //------------------------------------------------------------------------------
                        if (string.IsNullOrEmpty(_row["LENGTH"].ToString()))
                        {
                            _with64.Parameters.Add("LENGTH_IN", strNull).Direction = ParameterDirection.Input;
                            _with64.Parameters["LENGTH_IN"].SourceVersion = DataRowVersion.Current;
                        }
                        else
                        {
                            _with64.Parameters.Add("LENGTH_IN", Convert.ToDouble(_row["LENGTH"])).Direction = ParameterDirection.Input;
                            _with64.Parameters["LENGTH_IN"].SourceVersion = DataRowVersion.Current;
                        }
                        if (string.IsNullOrEmpty(_row["WIDTH"].ToString()))
                        {
                            _with64.Parameters.Add("WIDTH_IN", strNull).Direction = ParameterDirection.Input;
                            _with64.Parameters["WIDTH_IN"].SourceVersion = DataRowVersion.Current;
                        }
                        else
                        {
                            _with64.Parameters.Add("WIDTH_IN", Convert.ToDouble(_row["WIDTH"])).Direction = ParameterDirection.Input;
                            _with64.Parameters["WIDTH_IN"].SourceVersion = DataRowVersion.Current;
                        }
                        if (string.IsNullOrEmpty(_row["HEIGHT"].ToString()))
                        {
                            _with64.Parameters.Add("HEIGHT_IN", strNull).Direction = ParameterDirection.Input;
                            _with64.Parameters["HEIGHT_IN"].SourceVersion = DataRowVersion.Current;
                        }
                        else
                        {
                            _with64.Parameters.Add("HEIGHT_IN", Convert.ToDouble(_row["HEIGHT"])).Direction = ParameterDirection.Input;
                            _with64.Parameters["HEIGHT_IN"].SourceVersion = DataRowVersion.Current;
                        }
                        if (string.IsNullOrEmpty(_row["UOM"].ToString()))
                        {
                            _with64.Parameters.Add("UOM_IN", strNull).Direction = ParameterDirection.Input;
                            _with64.Parameters["UOM_IN"].SourceVersion = DataRowVersion.Current;
                        }
                        else
                        {
                            _with64.Parameters.Add("UOM_IN", Convert.ToInt32(_row["UOM"])).Direction = ParameterDirection.Input;
                            _with64.Parameters["UOM_IN"].SourceVersion = DataRowVersion.Current;
                        }
                        if (string.IsNullOrEmpty(_row["WEIGHT_PER_PKG"].ToString()))
                        {
                            _with64.Parameters.Add("WEIGHT_PER_PKG_IN", strNull).Direction = ParameterDirection.Input;
                            _with64.Parameters["WEIGHT_PER_PKG_IN"].SourceVersion = DataRowVersion.Current;
                        }
                        else
                        {
                            _with64.Parameters.Add("WEIGHT_PER_PKG_IN", Convert.ToDouble(_row["WEIGHT_PER_PKG"])).Direction = ParameterDirection.Input;
                            _with64.Parameters["WEIGHT_PER_PKG_IN"].SourceVersion = DataRowVersion.Current;
                        }
                        if (string.IsNullOrEmpty(_row["CALCULATED_WEIGHT"].ToString()))
                        {
                            _with64.Parameters.Add("CALCULATED_WEIGHT_IN", strNull).Direction = ParameterDirection.Input;
                            _with64.Parameters["CALCULATED_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;
                        }
                        else
                        {
                            _with64.Parameters.Add("CALCULATED_WEIGHT_IN", Convert.ToDouble(_row["CALCULATED_WEIGHT"])).Direction = ParameterDirection.Input;
                            _with64.Parameters["CALCULATED_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;
                        }
                        if (string.IsNullOrEmpty(_row["CHARGEABLE_WEIGHT"].ToString()))
                        {
                            _with64.Parameters.Add("CHARGEABLE_WEIGHT_IN", strNull).Direction = ParameterDirection.Input;
                            _with64.Parameters["CHARGEABLE_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;
                        }
                        else
                        {
                            _with64.Parameters.Add("CHARGEABLE_WEIGHT_IN", Convert.ToDouble(_row["CHARGEABLE_WEIGHT"])).Direction = ParameterDirection.Input;
                            _with64.Parameters["CHARGEABLE_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;
                        }
                        //------------------------------------------------------------------------------

                        _with64.Parameters.Add("LAST_MODIFIED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                        _with64.Parameters.Add("FROM_FLAG_IN", From).Direction = ParameterDirection.Input;
                        _with64.Parameters.Add("BIZ_TYPE_IN", Convert.ToInt32(BizType)).Direction = ParameterDirection.Input;

                        _with64.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;

                        _with64.ExecuteNonQuery();
                        CommPk = Convert.ToInt32(string.IsNullOrEmpty(_with64.Parameters["RETURN_VALUE"].Value.ToString()) ? "" : _with64.Parameters["RETURN_VALUE"].Value.ToString());
                    }
                    try
                    {
                        if (Convert.ToInt32(_row["BOOKING_CARGO_DTL_FK"]) > 0)
                        {
                            if (string.IsNullOrEmpty(SelectedCargoPks))
                            {
                                SelectedCargoPks = _row["BOOKING_CARGO_DTL_FK"].ToString();
                            }
                            else
                            {
                                SelectedCargoPks += "," + _row["BOOKING_CARGO_DTL_FK"];
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                    }
                    if (CommPk > 0)
                    {
                        if (string.IsNullOrEmpty(SelectedCommPks))
                        {
                            SelectedCommPks = Convert.ToString(CommPk);
                        }
                        else
                        {
                            SelectedCommPks += "," + CommPk;
                        }
                    }
                }

                if (arrMessage.Count == 0)
                {
                    arrMessage.Clear();
                    arrMessage.Add("All data Saved successfully");
                    if (DeleteRemovedComms & (From == "HBL" | From == "BOOKING" | From == "JOBCARD" | From == "IMPORTJOBCARD" | From == "MANUALJOBCARD" | From == "CBJC" | From == "TRANSPORTER" | From == "IMPORTTRANSPORTER"))
                        DeleteCommodityDtl(SelectedCommPks, DeleteRemovedComms, Temporary, objWK, From);
                    TRAN.Commit();
                }
                else
                {
                    TRAN.Rollback();
                }
                return arrMessage;
            }
            catch (OracleException oraexp)
            {
                TRAN.Rollback();
                arrMessage.Clear();
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                TRAN.Rollback();
                arrMessage.Clear();
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
            finally
            {
                objWK.MyCommand.Connection.Close();
            }
        }

        #endregion "Save Commodity Details"
    }
}