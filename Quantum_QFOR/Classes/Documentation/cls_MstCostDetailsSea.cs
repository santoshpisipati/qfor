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
using System.Data;
using System.Text;

namespace Quantum_QFOR
{
    public class clsMstCostDetailsSea : CommonFeatures
    {
        #region "Save"

        public int Save(DataSet dsCostDetails, string MJCfk, int CloaseFlag = 0)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            Int32 RecAfct = default(Int32);

            OracleCommand insPurchaseInvDetails = new OracleCommand();
            OracleCommand updPurchaseInvDetails = new OracleCommand();
            OracleCommand delPurchaseInvDetails = new OracleCommand();

            OracleCommand insCostDetails = new OracleCommand();
            OracleCommand updCostDetails = new OracleCommand();
            OracleCommand delCostDetails = new OracleCommand();

            try
            {
                //'Added By Koteshwari on 2/5/2011
                var _with1 = insCostDetails;
                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".MJC_TRN_SEA_PKG.MJC_TRN_SEA_EXP_COST_INS";
                var _with2 = _with1.Parameters;
                insCostDetails.Parameters.Add("MASTER_JC_SEA_EXP_FK_IN", Convert.ToInt32(MJCfk)).Direction = ParameterDirection.Input;

                insCostDetails.Parameters.Add("VENDOR_MST_FK_IN", OracleDbType.Int32, 10, "VENDOR_MST_PK").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["VENDOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("COST_ELEMENT_FK_IN", OracleDbType.Int32, 10, "COST_ELEMENT_MST_PK").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["COST_ELEMENT_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("LOCATION_FK_IN", OracleDbType.Int32, 10, "LOCATION_MST_FK").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["LOCATION_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("VENDOR_KEY_IN", OracleDbType.Varchar2, 50, "VENDOR_KEY").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["VENDOR_KEY_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("PTMT_TYPE_IN", OracleDbType.Int32, 1, "PTMT_TYPE").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["PTMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("ESTIMATED_COST_IN", OracleDbType.Int32, 20, "ESTIMATED_COST").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["ESTIMATED_COST_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("TOTAL_COST_IN", OracleDbType.Int32, 20, "TOTAL_COST").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["TOTAL_COST_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("CHARGE_BASIS_IN", OracleDbType.Int32, 1, "CHARGE_BASIS").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["CHARGE_BASIS_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 20, "ROE").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "MJC_TRN_SEA_EXP_COST_PK").Direction = ParameterDirection.Output;
                insCostDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with3 = updCostDetails;
                _with3.Connection = objWK.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".MJC_TRN_SEA_PKG.MJC_TRN_SEA_EXP_COST_UPD";
                var _with4 = _with3.Parameters;

                updCostDetails.Parameters.Add("MJC_TRN_SEA_EXP_COST_PK_IN", OracleDbType.Int32, 10, "MJC_TRN_SEA_EXP_COST_PK").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["MJC_TRN_SEA_EXP_COST_PK_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("MASTER_JC_SEA_EXP_FK_IN", MJCfk).Direction = ParameterDirection.Input;

                updCostDetails.Parameters.Add("VENDOR_MST_FK_IN", OracleDbType.Int32, 10, "VENDOR_MST_PK").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["VENDOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("COST_ELEMENT_FK_IN", OracleDbType.Int32, 50, "COST_ELEMENT_MST_PK").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["COST_ELEMENT_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("LOCATION_FK_IN", OracleDbType.Int32, 10, "LOCATION_MST_FK").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["LOCATION_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("VENDOR_KEY_IN", OracleDbType.Varchar2, 50, "VENDOR_KEY").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["VENDOR_KEY_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("PTMT_TYPE_IN", OracleDbType.Int32, 1, "PTMT_TYPE").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["PTMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("ESTIMATED_COST_IN", OracleDbType.Int32, 20, "ESTIMATED_COST").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["ESTIMATED_COST_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("TOTAL_COST_IN", OracleDbType.Int32, 20, "TOTAL_COST").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["TOTAL_COST_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("CHARGE_BASIS_IN", OracleDbType.Int32, 1, "CHARGE_BASIS").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["CHARGE_BASIS_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 20, "ROE").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updCostDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with5 = delCostDetails;
                _with5.Connection = objWK.MyConnection;
                _with5.CommandType = CommandType.StoredProcedure;
                _with5.CommandText = objWK.MyUserName + ".MJC_TRN_SEA_PKG.MJC_TRN_SEA_EXP_COST_DEL";

                delCostDetails.Parameters.Add("MJC_TRN_SEA_EXP_COST_PK_IN", OracleDbType.Int32, 10, "MJC_TRN_SEA_EXP_COST_PK").Direction = ParameterDirection.Input;
                delCostDetails.Parameters["MJC_TRN_SEA_EXP_COST_PK_IN"].SourceVersion = DataRowVersion.Current;

                delCostDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                delCostDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with6 = objWK.MyDataAdapter;

                _with6.InsertCommand = insCostDetails;
                _with6.InsertCommand.Transaction = TRAN;

                _with6.UpdateCommand = updCostDetails;
                _with6.UpdateCommand.Transaction = TRAN;

                _with6.DeleteCommand = delCostDetails;
                _with6.DeleteCommand.Transaction = TRAN;

                RecAfct = _with6.Update(dsCostDetails.Tables[0]);

                if (RecAfct > 0)
                {
                    TRAN.Commit();
                    if (CloaseFlag == 1)
                    {
                        OracleCommand ObjCommand = new OracleCommand();
                        var _with7 = ObjCommand;
                        _with7.Connection = objWK.MyConnection;
                        _with7.CommandType = CommandType.StoredProcedure;
                        _with7.CommandText = objWK.MyUserName + ".JC_COST_CALCULATION_PKG.JC_SEA_EXP_COST_CALC";
                        var _with8 = _with7.Parameters;
                        _with8.Add("MASTER_JC_SEA_EXP_PK_IN", MJCfk).Direction = ParameterDirection.Input;
                        _with8.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        _with7.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                        _with7.ExecuteNonQuery();
                    }
                    return RecAfct;
                }
                else
                {
                    TRAN.Rollback();
                    return 0;
                }
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
            finally
            {
                objWK.MyConnection.Close();
            }
        }

        #endregion "Save"

        #region "Save Imp"

        public int SaveImp(DataSet dsCostDetails, string MJCfk, int CloaseFlag = 0)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            Int32 RecAfct = default(Int32);

            OracleCommand insPurchaseInvDetails = new OracleCommand();
            OracleCommand updPurchaseInvDetails = new OracleCommand();
            OracleCommand delPurchaseInvDetails = new OracleCommand();

            OracleCommand insCostDetails = new OracleCommand();
            OracleCommand updCostDetails = new OracleCommand();
            OracleCommand delCostDetails = new OracleCommand();

            try
            {
                var _with9 = insCostDetails;
                _with9.Connection = objWK.MyConnection;
                _with9.CommandType = CommandType.StoredProcedure;
                _with9.CommandText = objWK.MyUserName + ".MJC_TRN_SEA_PKG.MJC_TRN_SEA_IMP_COST_INS";
                var _with10 = _with9.Parameters;
                insCostDetails.Parameters.Add("MASTER_JC_SEA_IMP_FK_IN", Convert.ToInt32(MJCfk)).Direction = ParameterDirection.Input;

                insCostDetails.Parameters.Add("VENDOR_MST_FK_IN", OracleDbType.Int32, 10, "VENDOR_MST_PK").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["VENDOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("COST_ELEMENT_FK_IN", OracleDbType.Int32, 10, "COST_ELEMENT_MST_PK").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["COST_ELEMENT_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("LOCATION_FK_IN", OracleDbType.Int32, 10, "LOCATION_MST_FK").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["LOCATION_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("VENDOR_KEY_IN", OracleDbType.Varchar2, 50, "VENDOR_KEY").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["VENDOR_KEY_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("PTMT_TYPE_IN", OracleDbType.Int32, 1, "PTMT_TYPE").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["PTMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("ESTIMATED_COST_IN", OracleDbType.Int32, 20, "ESTIMATED_COST").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["ESTIMATED_COST_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("TOTAL_COST_IN", OracleDbType.Int32, 20, "TOTAL_COST").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["TOTAL_COST_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("CHARGE_BASIS_IN", OracleDbType.Int32, 1, "CHARGE_BASIS").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["CHARGE_BASIS_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 20, "ROE").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "MJC_TRN_SEA_IMP_COST_PK").Direction = ParameterDirection.Output;
                insCostDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with11 = updCostDetails;
                _with11.Connection = objWK.MyConnection;
                _with11.CommandType = CommandType.StoredProcedure;
                _with11.CommandText = objWK.MyUserName + ".MJC_TRN_SEA_PKG.MJC_TRN_SEA_IMP_COST_UPD";
                var _with12 = _with11.Parameters;

                updCostDetails.Parameters.Add("MJC_TRN_SEA_IMP_COST_PK_IN", OracleDbType.Int32, 10, "MJC_TRN_SEA_IMP_COST_PK").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["MJC_TRN_SEA_IMP_COST_PK_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("MASTER_JC_SEA_IMP_FK_IN", MJCfk).Direction = ParameterDirection.Input;

                updCostDetails.Parameters.Add("VENDOR_MST_FK_IN", OracleDbType.Int32, 10, "VENDOR_MST_PK").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["VENDOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("COST_ELEMENT_FK_IN", OracleDbType.Int32, 50, "COST_ELEMENT_MST_PK").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["COST_ELEMENT_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("LOCATION_FK_IN", OracleDbType.Int32, 10, "LOCATION_MST_FK").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["LOCATION_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("VENDOR_KEY_IN", OracleDbType.Varchar2, 50, "VENDOR_KEY").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["VENDOR_KEY_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("PTMT_TYPE_IN", OracleDbType.Int32, 1, "PTMT_TYPE").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["PTMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("ESTIMATED_COST_IN", OracleDbType.Int32, 20, "ESTIMATED_COST").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["ESTIMATED_COST_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("TOTAL_COST_IN", OracleDbType.Int32, 20, "TOTAL_COST").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["TOTAL_COST_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("CHARGE_BASIS_IN", OracleDbType.Int32, 1, "CHARGE_BASIS").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["CHARGE_BASIS_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 20, "ROE").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updCostDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with13 = delCostDetails;
                _with13.Connection = objWK.MyConnection;
                _with13.CommandType = CommandType.StoredProcedure;
                _with13.CommandText = objWK.MyUserName + ".MJC_TRN_SEA_PKG.MJC_TRN_SEA_IMP_COST_DEL";

                delCostDetails.Parameters.Add("MJC_TRN_SEA_IMP_COST_PK_IN", OracleDbType.Int32, 10, "MJC_TRN_SEA_IMP_COST_PK").Direction = ParameterDirection.Input;
                delCostDetails.Parameters["MJC_TRN_SEA_IMP_COST_PK_IN"].SourceVersion = DataRowVersion.Current;

                delCostDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                delCostDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with14 = objWK.MyDataAdapter;

                _with14.InsertCommand = insCostDetails;
                _with14.InsertCommand.Transaction = TRAN;

                _with14.UpdateCommand = updCostDetails;
                _with14.UpdateCommand.Transaction = TRAN;

                _with14.DeleteCommand = delCostDetails;
                _with14.DeleteCommand.Transaction = TRAN;

                RecAfct = _with14.Update(dsCostDetails.Tables[0]);

                if (RecAfct > 0)
                {
                    TRAN.Commit();
                    if (CloaseFlag == 1)
                    {
                        OracleCommand ObjCommand = new OracleCommand();
                        var _with15 = ObjCommand;
                        _with15.Connection = objWK.MyConnection;
                        _with15.CommandType = CommandType.StoredProcedure;
                        _with15.CommandText = objWK.MyUserName + ".JC_COST_CALCULATION_PKG.JC_SEA_IMP_COST_CALC";
                        var _with16 = _with15.Parameters;
                        _with16.Add("MASTER_JC_SEA_IMP_PK_IN", MJCfk).Direction = ParameterDirection.Input;
                        _with16.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        _with15.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                        _with15.ExecuteNonQuery();
                    }
                    return RecAfct;
                }
                else
                {
                    TRAN.Rollback();
                    return 0;
                }
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
            finally
            {
                objWK.MyConnection.Close();
            }
        }

        #endregion "Save Imp"

        #region "Get Data"

        public DataSet getData(string Pk, string Pro = "")
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                // Import
                if (Convert.ToInt32(Pro) == 2)
                {
                    strSQL.Append(" select mjc_trn_sea_imp_pia_pk, v.vendor_id vendor_key, cost_element_id, invoice_number, ");
                    strSQL.Append(" to_char(invoice_date,dateformat) invoice_date, currency_mst_fk, actual_amt ACTUAL_AMT, tax_percentage, ");
                    strSQL.Append(" tax_amt, estimated_amt, actual_amt-estimated_amt diff_amt, decode(j.charge_basis,1,'Volume',2,'Gr. Wt.') CHARGE_BASIS1, vendor_mst_fk, ");
                    strSQL.Append(" cost_element_mst_fk, attached_file_name, 'false' Del, j.charge_basis from mjc_trn_sea_imp_pia j, ");
                    strSQL.Append(" currency_type_mst_tbl curr, cost_element_mst_tbl c, vendor_mst_tbl v ");
                    strSQL.Append(" where j.cost_element_mst_fk = c.cost_element_mst_pk and j.currency_mst_fk = curr.currency_mst_pk ");
                    strSQL.Append(" and j.vendor_mst_fk = v.vendor_mst_pk and master_jc_sea_imp_fk = " + Pk);
                    strSQL.Append(" order by mjc_trn_sea_imp_pia_pk ");
                    // Export
                }
                else
                {
                    strSQL.Append(" select mjc_trn_sea_exp_pia_pk, v.vendor_id vendor_key, cost_element_id, invoice_number, ");
                    strSQL.Append(" to_char(invoice_date,dateformat) invoice_date, currency_mst_fk, actual_amt ACTUAL_AMT, tax_percentage, ");
                    strSQL.Append(" tax_amt, estimated_amt, actual_amt-estimated_amt diff_amt, decode(j.charge_basis,1,'Volume',2,'Gr. Wt.') CHARGE_BASIS1, vendor_mst_fk, ");
                    strSQL.Append(" cost_element_mst_fk, attached_file_name, 'false' Del, j.charge_basis from mjc_trn_sea_exp_pia j, ");
                    strSQL.Append(" currency_type_mst_tbl curr, cost_element_mst_tbl c, vendor_mst_tbl v ");
                    strSQL.Append(" where j.cost_element_mst_fk = c.cost_element_mst_pk and j.currency_mst_fk = curr.currency_mst_pk ");
                    strSQL.Append(" and j.vendor_mst_fk = v.vendor_mst_pk and master_jc_sea_exp_fk = " + Pk);
                    strSQL.Append(" order by mjc_trn_sea_exp_pia_pk ");
                }
                return objWF.GetDataSet(strSQL.ToString());
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

        #endregion "Get Data"

        #region " Fetch Cost details data"

        public DataSet FetchCostDetailDataExp(string MstJcPK = "0", string pro = "", int baseCurrency = 0)
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (Convert.ToInt32(pro) == 2)
                {
                    strSQL.Append("SELECT MJC.MJC_TRN_SEA_IMP_COST_PK,");
                    strSQL.Append("       MJC.MASTER_JC_SEA_IMP_FK,");
                    strSQL.Append("       VMT.VENDOR_MST_PK,");
                    strSQL.Append("       CMT.COST_ELEMENT_MST_PK,");
                    strSQL.Append("       MJC.VENDOR_KEY,");
                    strSQL.Append("       CMT.COST_ELEMENT_ID,");
                    strSQL.Append("       CMT.COST_ELEMENT_NAME,");
                    strSQL.Append("       DECODE(MJC.PTMT_TYPE,1,'Prepaid',2,'Collect')PTMT_TYPE,");
                    strSQL.Append("       LMT.LOCATION_ID,");
                    strSQL.Append("       CURR.CURRENCY_ID,");
                    strSQL.Append("       MJC.ESTIMATED_COST,");
                    //strSQL.Append("       ROUND(GET_EX_RATE_BUY(MJC.CURRENCY_MST_FK," & baseCurrency & ", round(TO_DATE(MJOB.MASTER_JC_DATE,DATEFORMAT) - .5)), 4) AS ROE,")
                    strSQL.Append("       MJC.EXCHANGE_RATE AS ROE,");
                    strSQL.Append("       MJC.TOTAL_COST,");
                    strSQL.Append("       DECODE(MJC.CHARGE_BASIS,1,'Volume',2,'Gr. Wt.')CHARGE_BASIS,");
                    strSQL.Append("       '' DEL_FLAG,");
                    strSQL.Append("       'true' SEL_FLAG,");
                    strSQL.Append("       MJC.LOCATION_MST_FK,");
                    strSQL.Append("       MJC.CURRENCY_MST_FK");
                    strSQL.Append("  FROM MJC_TRN_SEA_IMP_COST  MJC,");
                    strSQL.Append("       MASTER_JC_SEA_IMP_TBL  MJOB,");
                    strSQL.Append("       VENDOR_MST_TBL        VMT,");
                    strSQL.Append("       COST_ELEMENT_MST_TBL  CMT,");
                    strSQL.Append("       CURRENCY_TYPE_MST_TBL CURR,");
                    strSQL.Append("       LOCATION_MST_TBL      LMT");
                    strSQL.Append(" WHERE MJC.MASTER_JC_SEA_IMP_FK = MJOB.MASTER_JC_SEA_IMP_PK");
                    strSQL.Append("   AND MJC.COST_ELEMENT_MST_FK = CMT.COST_ELEMENT_MST_PK");
                    strSQL.Append("   AND MJC.VENDOR_MST_FK = VMT.VENDOR_MST_PK");
                    strSQL.Append("   AND MJC.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
                    strSQL.Append("   AND MJC.LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                    strSQL.Append("   AND MJOB.MASTER_JC_SEA_IMP_PK = " + MstJcPK);
                    //Export
                }
                else
                {
                    strSQL.Append("SELECT MJC.MJC_TRN_SEA_EXP_COST_PK,");
                    strSQL.Append("       MJC.MASTER_JC_SEA_EXP_FK,");
                    strSQL.Append("       VMT.VENDOR_MST_PK,");
                    strSQL.Append("       CMT.COST_ELEMENT_MST_PK,");
                    strSQL.Append("       MJC.VENDOR_KEY,");
                    strSQL.Append("       CMT.COST_ELEMENT_ID,");
                    strSQL.Append("       CMT.COST_ELEMENT_NAME,");
                    strSQL.Append("       DECODE(MJC.PTMT_TYPE,1,'Prepaid',2,'Collect')PTMT_TYPE,");
                    strSQL.Append("       LMT.LOCATION_ID,");
                    strSQL.Append("       CURR.CURRENCY_ID,");
                    strSQL.Append("       MJC.ESTIMATED_COST,");
                    //strSQL.Append("       ROUND(GET_EX_RATE_BUY(MJC.CURRENCY_MST_FK," & baseCurrency & ", round(TO_DATE(MJOB.MASTER_JC_DATE,DATEFORMAT) - .5)), 4) AS ROE,")
                    strSQL.Append("       MJC.EXCHANGE_RATE AS ROE,");
                    strSQL.Append("       MJC.TOTAL_COST,");
                    strSQL.Append("       DECODE(MJC.CHARGE_BASIS,1,'Volume',2,'Gr. Wt.')CHARGE_BASIS,");
                    strSQL.Append("       '' DEL_FLAG,");
                    strSQL.Append("       '' SEL_FLAG,");
                    strSQL.Append("       MJC.LOCATION_MST_FK,");
                    strSQL.Append("       MJC.CURRENCY_MST_FK");
                    strSQL.Append("  FROM MJC_TRN_SEA_EXP_COST  MJC,");
                    strSQL.Append("       MASTER_JC_SEA_EXP_TBL  MJOB,");
                    strSQL.Append("       VENDOR_MST_TBL        VMT,");
                    strSQL.Append("       COST_ELEMENT_MST_TBL  CMT,");
                    strSQL.Append("       CURRENCY_TYPE_MST_TBL CURR,");
                    strSQL.Append("       LOCATION_MST_TBL      LMT");
                    strSQL.Append(" WHERE MJC.MASTER_JC_SEA_EXP_FK = MJOB.MASTER_JC_SEA_EXP_PK");
                    strSQL.Append("   AND MJC.COST_ELEMENT_MST_FK = CMT.COST_ELEMENT_MST_PK");
                    strSQL.Append("   AND MJC.VENDOR_MST_FK = VMT.VENDOR_MST_PK");
                    strSQL.Append("   AND MJC.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
                    strSQL.Append("   AND MJC.LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                    strSQL.Append("   AND MJOB.MASTER_JC_SEA_EXP_PK = " + MstJcPK);
                }

                return objWF.GetDataSet(strSQL.ToString());
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

        #endregion " Fetch Cost details data"

        #region "GetBaseCurrency"

        public Int64 GetBaseCurrency()
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder SQL = new StringBuilder();

            SQL.Append(" SELECT c.currency_mst_fk FROM corporate_mst_tbl c ");

            try
            {
                return Convert.ToInt32(objWF.ExecuteScaler(SQL.ToString()));
            }
            catch (OracleException sqlExp)
            {
                return 0;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        #endregion "GetBaseCurrency"
    }
}