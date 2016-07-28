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
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_EDI : CommonFeatures
    {
        /// <summary>
        /// Saves the new upload.
        /// </summary>
        /// <param name="intBizType">Type of the int biz.</param>
        /// <param name="strModule">The string module.</param>
        /// <param name="strFileName">Name of the string file.</param>
        /// <param name="dtUploadedDateTime">The dt uploaded date time.</param>
        /// <param name="intTotalRecords">The int total records.</param>
        /// <param name="intProcessed">The int processed.</param>
        /// <param name="intError">The int error.</param>
        /// <returns></returns>
        public string SaveNewUpload(int intBizType, string strModule, string strFileName, DateTime dtUploadedDateTime, int intTotalRecords, int intProcessed, int intError)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;

            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EDI_MST_TBL_PKG.EDI_MST_TBL_INS";
                var _with1 = SCM.Parameters;
                _with1.Add("BUSINESS_TYPE_IN", intBizType).Direction = ParameterDirection.Input;
                _with1.Add("MODULE_IN", strModule).Direction = ParameterDirection.Input;
                _with1.Add("FILE_NAME_IN", strFileName).Direction = ParameterDirection.Input;
                _with1.Add("UPLOADED_DATE_TIME_IN", dtUploadedDateTime).Direction = ParameterDirection.Input;
                _with1.Add("TOTAL_RECORDS_IN", intTotalRecords).Direction = ParameterDirection.Input;
                _with1.Add("PROCESSED_IN", intProcessed).Direction = ParameterDirection.Input;
                _with1.Add("ERROR_IN", intError).Direction = ParameterDirection.Input;
                _with1.Add("CREATED_BY_FK_IN", Convert.ToInt64(CREATED_BY));
                _with1.Add("CONFIG_MST_FK_IN", ConfigurationPK);
                _with1.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                SCM.Connection.Close();
            }
        }

        #region "Update Function (Quick Entry)"

        /// <summary>
        /// Updates the common details sea.
        /// </summary>
        /// <param name="intQEPK">The int qepk.</param>
        /// <param name="intEdiMstPk">The int edi MST pk.</param>
        /// <param name="strMblNr">The string MBL nr.</param>
        /// <param name="strMblDt">The string MBL dt.</param>
        /// <param name="strHblNr">The string HBL nr.</param>
        /// <param name="strHblDt">The string HBL dt.</param>
        /// <param name="strLine">The string line.</param>
        /// <param name="strVslVoy">The string VSL voy.</param>
        /// <param name="strFstVoy">The string FST voy.</param>
        /// <param name="strPodVslVoy">The string pod VSL voy.</param>
        /// <param name="strPodVoy">The string pod voy.</param>
        /// <param name="strPolAgent">The string pol agent.</param>
        /// <param name="strPOO">The string poo.</param>
        /// <param name="strPOL">The string pol.</param>
        /// <param name="strPOD">The string pod.</param>
        /// <param name="strPFD">The string PFD.</param>
        /// <param name="strPolEtd">The string pol etd.</param>
        /// <param name="strPolAtd">The string pol atd.</param>
        /// <param name="strPodEta">The string pod eta.</param>
        /// <param name="strPodAta">The string pod ata.</param>
        /// <param name="strCustomer">The string customer.</param>
        /// <param name="strShipper">The string shipper.</param>
        /// <param name="strConsignee">The string consignee.</param>
        /// <param name="strNotify">The string notify.</param>
        /// <param name="strTerms">The string terms.</param>
        /// <param name="strPayType">Type of the string pay.</param>
        /// <param name="strMoveCode">The string move code.</param>
        /// <param name="strCargoType">Type of the string cargo.</param>
        /// <param name="strMarksNumbers">The string marks numbers.</param>
        /// <param name="strGoodsDesc">The string goods desc.</param>
        /// <param name="strRemarks">The string remarks.</param>
        /// <param name="strVersionNr">The string version nr.</param>
        /// <returns></returns>
        public string UpdateCommonDetailsSea(int intQEPK, int intEdiMstPk, string strMblNr, string strMblDt, string strHblNr, string strHblDt, string strLine, string strVslVoy, string strFstVoy, string strPodVslVoy,
        string strPodVoy, string strPolAgent, string strPOO, string strPOL, string strPOD, string strPFD, string strPolEtd, string strPolAtd, string strPodEta, string strPodAta,
        string strCustomer, string strShipper, string strConsignee, string strNotify, string strTerms, string strPayType, string strMoveCode, string strCargoType, string strMarksNumbers, string strGoodsDesc,
        string strRemarks, string strVersionNr)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;

            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EDI_MST_TBL_PKG.EDI_QUICK_ENTRY_CMN_TBL_UPD";
                var _with20 = SCM.Parameters;
                _with20.Add("QUICK_ENTRY_PK_IN", intQEPK).Direction = ParameterDirection.Input;
                _with20.Add("EDI_MST_FK_IN", intEdiMstPk).Direction = ParameterDirection.Input;
                _with20.Add("MBL_REF_NO_IN", (string.IsNullOrEmpty(strMblNr) ? "" : strMblNr)).Direction = ParameterDirection.Input;
                _with20.Add("MBL_DATE_IN", (string.IsNullOrEmpty(strMblDt) ? "" : strMblDt)).Direction = ParameterDirection.Input;
                _with20.Add("HBL_REF_NO_IN", (string.IsNullOrEmpty(strHblNr) ? "" : strHblNr)).Direction = ParameterDirection.Input;
                _with20.Add("HBL_DATE_IN", (string.IsNullOrEmpty(strHblDt) ? "" : strHblDt)).Direction = ParameterDirection.Input;
                _with20.Add("LINE_IN", (string.IsNullOrEmpty(strLine) ? "" : strLine)).Direction = ParameterDirection.Input;
                _with20.Add("FIRST_VESSEL_NAME_IN", (string.IsNullOrEmpty(strVslVoy) ? "" : strVslVoy)).Direction = ParameterDirection.Input;
                _with20.Add("FIRST_VOYAGE_IN", (string.IsNullOrEmpty(strFstVoy) ? "" : strFstVoy)).Direction = ParameterDirection.Input;
                _with20.Add("POD_VESSEL_NAME_IN", (string.IsNullOrEmpty(strPodVslVoy) ? "" : strPodVslVoy)).Direction = ParameterDirection.Input;
                _with20.Add("POD_VOYAGE_IN", (string.IsNullOrEmpty(strPodVoy) ? "" : strPodVoy)).Direction = ParameterDirection.Input;
                _with20.Add("POL_AGENT_IN", (string.IsNullOrEmpty(strPolAgent) ? "" : strPolAgent)).Direction = ParameterDirection.Input;
                _with20.Add("POO_IN", (string.IsNullOrEmpty(strPOO) ? "" : strPOO)).Direction = ParameterDirection.Input;
                _with20.Add("POL_IN", (string.IsNullOrEmpty(strPOL) ? "" : strPOL)).Direction = ParameterDirection.Input;
                _with20.Add("POD_IN", (string.IsNullOrEmpty(strPOD) ? "" : strPOD)).Direction = ParameterDirection.Input;
                _with20.Add("PFD_IN", (string.IsNullOrEmpty(strPFD) ? "" : strPFD)).Direction = ParameterDirection.Input;
                _with20.Add("POL_ETD_IN", (string.IsNullOrEmpty(strPolEtd) ? "" : strPolEtd)).Direction = ParameterDirection.Input;
                _with20.Add("POL_ATD_IN", (string.IsNullOrEmpty(strPolAtd) ? "" : strPolAtd)).Direction = ParameterDirection.Input;
                _with20.Add("POD_ETA_IN", (string.IsNullOrEmpty(strPodEta) ? "" : strPodEta)).Direction = ParameterDirection.Input;
                _with20.Add("POD_ATA_IN", (string.IsNullOrEmpty(strPodAta) ? "" : strPodAta)).Direction = ParameterDirection.Input;
                _with20.Add("CUSTOMER_IN", (string.IsNullOrEmpty(strCustomer) ? "" : strCustomer)).Direction = ParameterDirection.Input;
                _with20.Add("SHIPPER_IN", (string.IsNullOrEmpty(strShipper) ? "" : strShipper)).Direction = ParameterDirection.Input;
                _with20.Add("CONSIGNEE_IN", (string.IsNullOrEmpty(strConsignee) ? "" : strConsignee)).Direction = ParameterDirection.Input;
                _with20.Add("NOTIFY_IN", (string.IsNullOrEmpty(strNotify) ? "" : strNotify)).Direction = ParameterDirection.Input;
                _with20.Add("TERMS_IN", (string.IsNullOrEmpty(strTerms) ? "" : strTerms)).Direction = ParameterDirection.Input;
                _with20.Add("PAY_TYPE_IN", (string.IsNullOrEmpty(strPayType) ? "" : strPayType)).Direction = ParameterDirection.Input;
                _with20.Add("MOVE_CODE_IN", (string.IsNullOrEmpty(strMoveCode) ? "" : strMoveCode)).Direction = ParameterDirection.Input;
                _with20.Add("CARGO_TYPE_IN", (string.IsNullOrEmpty(strCargoType) ? "" : strCargoType)).Direction = ParameterDirection.Input;
                _with20.Add("MARKS_NUMBERS_IN", (string.IsNullOrEmpty(strMarksNumbers) ? "" : strMarksNumbers)).Direction = ParameterDirection.Input;
                _with20.Add("GOODS_DESC_IN", (string.IsNullOrEmpty(strGoodsDesc) ? "" : strGoodsDesc)).Direction = ParameterDirection.Input;
                _with20.Add("REMARKS_IN", (string.IsNullOrEmpty(strRemarks) ? "" : strRemarks)).Direction = ParameterDirection.Input;
                _with20.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK);
                _with20.Add("VERSION_NO_IN", CREATED_BY);
                _with20.Add("CONFIG_MST_FK_IN", ConfigurationPK);
                _with20.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                SCM.Connection.Close();
            }
        }

        /// <summary>
        /// Updates the container details sea.
        /// </summary>
        /// <param name="intQEPK">The int qepk.</param>
        /// <param name="intEdiMstPk">The int edi MST pk.</param>
        /// <param name="M_DataSet">The m_ data set.</param>
        /// <returns></returns>
        public ArrayList updateContainerDetailsSea(int intQEPK, int intEdiMstPk, DataSet M_DataSet)
        {
            //Sivachandran 17jun08 Imp-Exp-Wiz
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();

            int intPKVal = 0;
            long lngI = 0;
            Int32 RecAfct = default(Int32);
            OracleCommand updCommand = new OracleCommand();

            try
            {
                var _with21 = updCommand;
                _with21.Connection = objWK.MyConnection;
                _with21.CommandType = CommandType.StoredProcedure;
                _with21.CommandText = objWK.MyUserName + ".EDI_MST_TBL_PKG.EDI_QUICK_ENTRY_CNTR_TBL_UPD";
                var _with22 = _with21.Parameters;

                updCommand.Parameters.Add("QUICK_ENTRY_PK_IN", OracleDbType.Int32, 10, "PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["QUICK_ENTRY_PK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("EDI_MST_FK_IN", intEdiMstPk).Direction = ParameterDirection.Input;
                updCommand.Parameters["EDI_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CONTAINER_TYPE_IN", OracleDbType.Varchar2, 50, "CONTAINER_TYPE").Direction = ParameterDirection.Input;
                updCommand.Parameters["CONTAINER_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CONTAINER_NR_IN", OracleDbType.Varchar2, 50, "CONTAINER_NR").Direction = ParameterDirection.Input;
                updCommand.Parameters["CONTAINER_NR_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("SEAL_NR_IN", OracleDbType.Varchar2, 50, "SEAL_NR").Direction = ParameterDirection.Input;
                updCommand.Parameters["SEAL_NR_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("COMMODITY_GRP_IN", OracleDbType.Varchar2, 50, "COMMODITY_GRP").Direction = ParameterDirection.Input;
                updCommand.Parameters["COMMODITY_GRP_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("COMMODITY_IN", OracleDbType.Varchar2, 150, "COMMODITY").Direction = ParameterDirection.Input;
                updCommand.Parameters["COMMODITY_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("PACK_TYPE_IN", OracleDbType.Varchar2, 50, "PACK_TYPE").Direction = ParameterDirection.Input;
                updCommand.Parameters["PACK_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("NOS_IN", OracleDbType.Varchar2, 50, "NOS").Direction = ParameterDirection.Input;
                updCommand.Parameters["NOS_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("VOL_IN", OracleDbType.Varchar2, 50, "VOL").Direction = ParameterDirection.Input;
                updCommand.Parameters["VOL_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("GROSS_WT_IN", OracleDbType.Varchar2, 50, "GROSS_WT").Direction = ParameterDirection.Input;
                updCommand.Parameters["GROSS_WT_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("NET_WT_IN", OracleDbType.Varchar2, 50, "NET_WT").Direction = ParameterDirection.Input;
                updCommand.Parameters["NET_WT_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("QUICK_ENTRY_CMN_FK_IN", intQEPK).Direction = ParameterDirection.Input;
                updCommand.Parameters["QUICK_ENTRY_CMN_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with23 = objWK.MyDataAdapter;
                _with23.UpdateCommand = updCommand;
                _with23.UpdateCommand.Transaction = TRAN;

                RecAfct = _with23.Update(M_DataSet);

                if (arrMessage.Count > 0)
                {
                    //This is if any error occurs then rollback..
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    //This part executes only when no error in the execution..
                    TRAN.Commit();
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
                objWK.CloseConnection();
            }
        }

        /// <summary>
        /// Updates the freight charges sea.
        /// </summary>
        /// <param name="intQEPK">The int qepk.</param>
        /// <param name="intEdiMstPk">The int edi MST pk.</param>
        /// <param name="M_DataSet">The m_ data set.</param>
        /// <returns></returns>
        public ArrayList updateFreightChargesSea(int intQEPK, int intEdiMstPk, DataSet M_DataSet)
        {
            //Sivachandran 17jun08 Imp-Exp-Wiz
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();

            int intPKVal = 0;
            long lngI = 0;
            Int32 RecAfct = default(Int32);
            OracleCommand updCommand = new OracleCommand();

            try
            {
                var _with24 = updCommand;
                _with24.Connection = objWK.MyConnection;
                _with24.CommandType = CommandType.StoredProcedure;
                _with24.CommandText = objWK.MyUserName + ".EDI_MST_TBL_PKG.EDI_QUICK_ENTRY_FRT_TBL_UPD";
                var _with25 = _with24.Parameters;

                updCommand.Parameters.Add("QUICK_ENTRY_PK_IN", OracleDbType.Int32, 10, "PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["QUICK_ENTRY_PK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("EDI_MST_FK_IN", intEdiMstPk).Direction = ParameterDirection.Input;
                updCommand.Parameters["EDI_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CONTAINER_TYPE_IN", OracleDbType.Varchar2, 50, "CONTAINER_TYPE").Direction = ParameterDirection.Input;
                updCommand.Parameters["CONTAINER_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("FRT_ELEMENT_IN", OracleDbType.Varchar2, 50, "FRT_ELEMENT").Direction = ParameterDirection.Input;
                updCommand.Parameters["FRT_ELEMENT_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("BASIS_IN", OracleDbType.Varchar2, 50, "BASIS").Direction = ParameterDirection.Input;
                updCommand.Parameters["BASIS_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("QTY_IN", OracleDbType.Varchar2, 50, "QTY").Direction = ParameterDirection.Input;
                updCommand.Parameters["QTY_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("PMT_TYPE_IN", OracleDbType.Varchar2, 150, "PMT_TYPE").Direction = ParameterDirection.Input;
                updCommand.Parameters["PMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("LOCATION_IN", OracleDbType.Varchar2, 50, "LOCATION").Direction = ParameterDirection.Input;
                updCommand.Parameters["LOCATION_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("FRT_PAYER_IN", OracleDbType.Varchar2, 50, "FRT_PAYER").Direction = ParameterDirection.Input;
                updCommand.Parameters["FRT_PAYER_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("AMOUNT_IN", OracleDbType.Varchar2, 50, "AMOUNT").Direction = ParameterDirection.Input;
                updCommand.Parameters["AMOUNT_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CUR_IN", OracleDbType.Varchar2, 50, "CUR").Direction = ParameterDirection.Input;
                updCommand.Parameters["CUR_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("ROE_IN", OracleDbType.Varchar2, 50, "ROE").Direction = ParameterDirection.Input;
                updCommand.Parameters["ROE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("TOTAL_IN", OracleDbType.Varchar2, 50, "TOTAL").Direction = ParameterDirection.Input;
                updCommand.Parameters["TOTAL_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("QUICK_ENTRY_CMN_FK_IN", intQEPK).Direction = ParameterDirection.Input;
                updCommand.Parameters["QUICK_ENTRY_CMN_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with26 = objWK.MyDataAdapter;
                _with26.UpdateCommand = updCommand;
                _with26.UpdateCommand.Transaction = TRAN;

                RecAfct = _with26.Update(M_DataSet);

                if (arrMessage.Count > 0)
                {
                    //This is if any error occurs then rollback..
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    //This part executes only when no error in the execution..
                    TRAN.Commit();
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
                objWK.CloseConnection();
            }
        }

        /// <summary>
        /// Updates the other charges sea.
        /// </summary>
        /// <param name="intQEPK">The int qepk.</param>
        /// <param name="intEdiMstPk">The int edi MST pk.</param>
        /// <param name="M_DataSet">The m_ data set.</param>
        /// <returns></returns>
        public ArrayList updateOtherChargesSea(int intQEPK, int intEdiMstPk, DataSet M_DataSet)
        {
            //Sivachandran 17jun08 Imp-Exp-Wiz
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();

            int intPKVal = 0;
            long lngI = 0;
            Int32 RecAfct = default(Int32);
            OracleCommand updCommand = new OracleCommand();

            try
            {
                var _with27 = updCommand;
                _with27.Connection = objWK.MyConnection;
                _with27.CommandType = CommandType.StoredProcedure;
                _with27.CommandText = objWK.MyUserName + ".EDI_MST_TBL_PKG.EDI_QUICK_ENTRY_OTH_TBL_UPD";
                var _with28 = _with27.Parameters;

                updCommand.Parameters.Add("QUICK_ENTRY_PK_IN", OracleDbType.Int32, 10, "PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["QUICK_ENTRY_PK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("EDI_MST_FK_IN", intEdiMstPk).Direction = ParameterDirection.Input;
                updCommand.Parameters["EDI_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CONTAINER_TYPE_IN", OracleDbType.Varchar2, 50, "CONTAINER_TYPE").Direction = ParameterDirection.Input;
                updCommand.Parameters["CONTAINER_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CHARGE_ID_IN", OracleDbType.Varchar2, 50, "CHARGE_ID").Direction = ParameterDirection.Input;
                updCommand.Parameters["CHARGE_ID_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("BASIS_IN", OracleDbType.Varchar2, 50, "BASIS").Direction = ParameterDirection.Input;
                updCommand.Parameters["BASIS_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("QTY_IN", OracleDbType.Varchar2, 50, "QTY").Direction = ParameterDirection.Input;
                updCommand.Parameters["QTY_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("PMT_TYPE_IN", OracleDbType.Varchar2, 150, "PMT_TYPE").Direction = ParameterDirection.Input;
                updCommand.Parameters["PMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("LOCATION_IN", OracleDbType.Varchar2, 50, "LOCATION").Direction = ParameterDirection.Input;
                updCommand.Parameters["LOCATION_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("FRT_PAYER_IN", OracleDbType.Varchar2, 50, "FRT_PAYER").Direction = ParameterDirection.Input;
                updCommand.Parameters["FRT_PAYER_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("AMOUNT_IN", OracleDbType.Varchar2, 50, "AMOUNT").Direction = ParameterDirection.Input;
                updCommand.Parameters["AMOUNT_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CUR_IN", OracleDbType.Varchar2, 50, "CUR").Direction = ParameterDirection.Input;
                updCommand.Parameters["CUR_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("ROE_IN", OracleDbType.Varchar2, 50, "ROE").Direction = ParameterDirection.Input;
                updCommand.Parameters["ROE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("TOTAL_IN", OracleDbType.Varchar2, 50, "TOTAL").Direction = ParameterDirection.Input;
                updCommand.Parameters["TOTAL_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("QUICK_ENTRY_CMN_FK_IN", intQEPK).Direction = ParameterDirection.Input;
                updCommand.Parameters["QUICK_ENTRY_CMN_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with29 = objWK.MyDataAdapter;
                _with29.UpdateCommand = updCommand;
                _with29.UpdateCommand.Transaction = TRAN;

                RecAfct = _with29.Update(M_DataSet);

                if (arrMessage.Count > 0)
                {
                    //This is if any error occurs then rollback..
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    //This part executes only when no error in the execution..
                    TRAN.Commit();
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
                objWK.CloseConnection();
            }
        }

        /// <summary>
        /// Updates the common details air.
        /// </summary>
        /// <param name="intQEPK">The int qepk.</param>
        /// <param name="intEdiMstPk">The int edi MST pk.</param>
        /// <param name="strMawbNr">The string mawb nr.</param>
        /// <param name="strMawbDt">The string mawb dt.</param>
        /// <param name="strHawbNr">The string hawb nr.</param>
        /// <param name="strHawbDt">The string hawb dt.</param>
        /// <param name="strAirLine">The string air line.</param>
        /// <param name="strFlightNr">The string flight nr.</param>
        /// <param name="strAodFlightNr">The string aod flight nr.</param>
        /// <param name="strAolAgent">The string aol agent.</param>
        /// <param name="strPLR">The string PLR.</param>
        /// <param name="strAOO">The string aoo.</param>
        /// <param name="strAOD">The string aod.</param>
        /// <param name="strPFD">The string PFD.</param>
        /// <param name="strAooEtd">The string aoo etd.</param>
        /// <param name="strAooAtd">The string aoo atd.</param>
        /// <param name="strAodEta">The string aod eta.</param>
        /// <param name="strAodAta">The string aod ata.</param>
        /// <param name="strCustomer">The string customer.</param>
        /// <param name="strShipper">The string shipper.</param>
        /// <param name="strConsignee">The string consignee.</param>
        /// <param name="strNotify">The string notify.</param>
        /// <param name="strTerms">The string terms.</param>
        /// <param name="strPayType">Type of the string pay.</param>
        /// <param name="strMoveCode">The string move code.</param>
        /// <param name="strMarksNumbers">The string marks numbers.</param>
        /// <param name="strGoodsDesc">The string goods desc.</param>
        /// <param name="strRemarks">The string remarks.</param>
        /// <param name="strVersionNr">The string version nr.</param>
        /// <returns></returns>
        public string UpdateCommonDetailsAir(int intQEPK, int intEdiMstPk, string strMawbNr, string strMawbDt, string strHawbNr, string strHawbDt, string strAirLine, string strFlightNr, string strAodFlightNr, string strAolAgent,
        string strPLR, string strAOO, string strAOD, string strPFD, string strAooEtd, string strAooAtd, string strAodEta, string strAodAta, string strCustomer, string strShipper,
        string strConsignee, string strNotify, string strTerms, string strPayType, string strMoveCode, string strMarksNumbers, string strGoodsDesc, string strRemarks, string strVersionNr)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;

            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EDI_MST_TBL_PKG.EDI_QE_CMN_AIR_TBL_UPD";
                var _with30 = SCM.Parameters;
                _with30.Add("QUICK_ENTRY_PK_IN", intQEPK).Direction = ParameterDirection.Input;
                _with30.Add("EDI_MST_FK_IN", intEdiMstPk).Direction = ParameterDirection.Input;
                _with30.Add("MAWB_REF_NO_IN", (string.IsNullOrEmpty(strMawbNr) ? "" : strMawbNr)).Direction = ParameterDirection.Input;
                _with30.Add("MAWB_DATE_IN", (string.IsNullOrEmpty(strMawbDt) ? "" : strMawbDt)).Direction = ParameterDirection.Input;
                _with30.Add("HAWB_REF_NO_IN", (string.IsNullOrEmpty(strHawbNr) ? "" : strHawbNr)).Direction = ParameterDirection.Input;
                _with30.Add("HAWB_DATE_IN", (string.IsNullOrEmpty(strHawbDt) ? "" : strHawbDt)).Direction = ParameterDirection.Input;
                _with30.Add("AIRLINE_IN", (string.IsNullOrEmpty(strAirLine) ? "" : strAirLine)).Direction = ParameterDirection.Input;
                _with30.Add("FLIGHT_NR_IN", (string.IsNullOrEmpty(strFlightNr) ? "" : strFlightNr)).Direction = ParameterDirection.Input;
                _with30.Add("AOD_FLIGHT_NR_IN", (string.IsNullOrEmpty(strAodFlightNr) ? "" : strAodFlightNr)).Direction = ParameterDirection.Input;
                _with30.Add("AOL_AGENT_IN", (string.IsNullOrEmpty(strAolAgent) ? "" : strAolAgent)).Direction = ParameterDirection.Input;
                _with30.Add("PLR_IN", (string.IsNullOrEmpty(strPLR) ? "" : strPLR)).Direction = ParameterDirection.Input;
                _with30.Add("AOO_IN", (string.IsNullOrEmpty(strAOO) ? "" : strAOO)).Direction = ParameterDirection.Input;
                _with30.Add("AOD_IN", (string.IsNullOrEmpty(strAOD) ? "" : strAOD)).Direction = ParameterDirection.Input;
                _with30.Add("PFD_IN", (string.IsNullOrEmpty(strPFD) ? "" : strPFD)).Direction = ParameterDirection.Input;
                _with30.Add("AOO_ETD_IN", (string.IsNullOrEmpty(strAooEtd) ? "" : strAooEtd)).Direction = ParameterDirection.Input;
                _with30.Add("AOO_ATD_IN", (string.IsNullOrEmpty(strAooAtd) ? "" : strAooAtd)).Direction = ParameterDirection.Input;
                _with30.Add("AOD_ETA_IN", (string.IsNullOrEmpty(strAodEta) ? "" : strAodEta)).Direction = ParameterDirection.Input;
                _with30.Add("AOD_ATA_IN", (string.IsNullOrEmpty(strAodAta) ? "" : strAodAta)).Direction = ParameterDirection.Input;
                _with30.Add("CUSTOMER_IN", (string.IsNullOrEmpty(strCustomer) ? "" : strCustomer)).Direction = ParameterDirection.Input;
                _with30.Add("SHIPPER_IN", (string.IsNullOrEmpty(strShipper) ? "" : strShipper)).Direction = ParameterDirection.Input;
                _with30.Add("CONSIGNEE_IN", (string.IsNullOrEmpty(strConsignee) ? "" : strConsignee)).Direction = ParameterDirection.Input;
                _with30.Add("NOTIFY_IN", (string.IsNullOrEmpty(strNotify) ? "" : strNotify)).Direction = ParameterDirection.Input;
                _with30.Add("TERMS_IN", (string.IsNullOrEmpty(strTerms) ? "" : strTerms)).Direction = ParameterDirection.Input;
                _with30.Add("PAY_TYPE_IN", (string.IsNullOrEmpty(strPayType) ? "" : strPayType)).Direction = ParameterDirection.Input;
                _with30.Add("MOVE_CODE_IN", (string.IsNullOrEmpty(strMoveCode) ? "" : strMoveCode)).Direction = ParameterDirection.Input;
                _with30.Add("MARKS_NUMBERS_IN", (string.IsNullOrEmpty(strMarksNumbers) ? "" : strMarksNumbers)).Direction = ParameterDirection.Input;
                _with30.Add("GOODS_DESC_IN", (string.IsNullOrEmpty(strGoodsDesc) ? "" : strGoodsDesc)).Direction = ParameterDirection.Input;
                _with30.Add("REMARKS_IN", (string.IsNullOrEmpty(strRemarks) ? "" : strRemarks)).Direction = ParameterDirection.Input;
                _with30.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK);
                _with30.Add("VERSION_NO_IN", CREATED_BY);
                _with30.Add("CONFIG_MST_FK_IN", ConfigurationPK);
                _with30.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                SCM.Connection.Close();
            }
        }

        /// <summary>
        /// Updates the cargo details air.
        /// </summary>
        /// <param name="intQEPK">The int qepk.</param>
        /// <param name="intEdiMstPk">The int edi MST pk.</param>
        /// <param name="M_DataSet">The m_ data set.</param>
        /// <returns></returns>
        public ArrayList updateCargoDetailsAir(int intQEPK, int intEdiMstPk, DataSet M_DataSet)
        {
            //Sivachandran 17jun08 Imp-Exp-Wiz
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();

            int intPKVal = 0;
            long lngI = 0;
            Int32 RecAfct = default(Int32);
            OracleCommand updCommand = new OracleCommand();

            try
            {
                var _with31 = updCommand;
                _with31.Connection = objWK.MyConnection;
                _with31.CommandType = CommandType.StoredProcedure;
                _with31.CommandText = objWK.MyUserName + ".EDI_MST_TBL_PKG.EDI_QE_CARGO_AIR_TBL_UPD";
                var _with32 = _with31.Parameters;

                updCommand.Parameters.Add("QUICK_ENTRY_PK_IN", OracleDbType.Int32, 10, "PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["QUICK_ENTRY_PK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("EDI_MST_FK_IN", intEdiMstPk).Direction = ParameterDirection.Input;
                updCommand.Parameters["EDI_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("DIMENSIONS_IN", OracleDbType.Varchar2, 50, "DIMENSIONS").Direction = ParameterDirection.Input;
                updCommand.Parameters["DIMENSIONS_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("COMMODITY_GRP_IN", OracleDbType.Varchar2, 50, "COMMODITY_GRP").Direction = ParameterDirection.Input;
                updCommand.Parameters["COMMODITY_GRP_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("COMMODITY_IN", OracleDbType.Varchar2, 150, "COMMODITY").Direction = ParameterDirection.Input;
                updCommand.Parameters["COMMODITY_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("PACK_TYPE_IN", OracleDbType.Varchar2, 50, "PACK_TYPE").Direction = ParameterDirection.Input;
                updCommand.Parameters["PACK_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("NOS_IN", OracleDbType.Varchar2, 50, "NOS").Direction = ParameterDirection.Input;
                updCommand.Parameters["NOS_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("VOL_IN", OracleDbType.Varchar2, 50, "VOL").Direction = ParameterDirection.Input;
                updCommand.Parameters["VOL_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("GROSS_WT_IN", OracleDbType.Varchar2, 50, "GROSS_WT").Direction = ParameterDirection.Input;
                updCommand.Parameters["GROSS_WT_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CHARGEABLE_WT_IN", OracleDbType.Varchar2, 50, "CHARGEABLE_WT").Direction = ParameterDirection.Input;
                updCommand.Parameters["CHARGEABLE_WT_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("QUICK_ENTRY_CMN_FK_IN", intQEPK).Direction = ParameterDirection.Input;
                updCommand.Parameters["QUICK_ENTRY_CMN_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with33 = objWK.MyDataAdapter;
                _with33.UpdateCommand = updCommand;
                _with33.UpdateCommand.Transaction = TRAN;

                RecAfct = _with33.Update(M_DataSet);

                if (arrMessage.Count > 0)
                {
                    //This is if any error occurs then rollback..
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    //This part executes only when no error in the execution..
                    TRAN.Commit();
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
                objWK.CloseConnection();
            }
        }

        /// <summary>
        /// Updates the freight charges air.
        /// </summary>
        /// <param name="intQEPK">The int qepk.</param>
        /// <param name="intEdiMstPk">The int edi MST pk.</param>
        /// <param name="M_DataSet">The m_ data set.</param>
        /// <returns></returns>
        public ArrayList updateFreightChargesAir(int intQEPK, int intEdiMstPk, DataSet M_DataSet)
        {
            //Sivachandran 17jun08 Imp-Exp-Wiz
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();

            int intPKVal = 0;
            long lngI = 0;
            Int32 RecAfct = default(Int32);
            OracleCommand updCommand = new OracleCommand();

            try
            {
                var _with34 = updCommand;
                _with34.Connection = objWK.MyConnection;
                _with34.CommandType = CommandType.StoredProcedure;
                _with34.CommandText = objWK.MyUserName + ".EDI_MST_TBL_PKG.EDI_QE_FRT_AIR_TBL_UPD";
                var _with35 = _with34.Parameters;

                updCommand.Parameters.Add("QUICK_ENTRY_PK_IN", OracleDbType.Int32, 10, "PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["QUICK_ENTRY_PK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("EDI_MST_FK_IN", intEdiMstPk).Direction = ParameterDirection.Input;
                updCommand.Parameters["EDI_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("FRT_ELEMENT_IN", OracleDbType.Varchar2, 50, "FRT_ELEMENT").Direction = ParameterDirection.Input;
                updCommand.Parameters["FRT_ELEMENT_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("BASIS_IN", OracleDbType.Varchar2, 50, "BASIS").Direction = ParameterDirection.Input;
                updCommand.Parameters["BASIS_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("QTY_IN", OracleDbType.Varchar2, 50, "QTY").Direction = ParameterDirection.Input;
                updCommand.Parameters["QTY_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("PMT_TYPE_IN", OracleDbType.Varchar2, 150, "PMT_TYPE").Direction = ParameterDirection.Input;
                updCommand.Parameters["PMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("LOCATION_IN", OracleDbType.Varchar2, 50, "LOCATION").Direction = ParameterDirection.Input;
                updCommand.Parameters["LOCATION_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("FRT_PAYER_IN", OracleDbType.Varchar2, 50, "FRT_PAYER").Direction = ParameterDirection.Input;
                updCommand.Parameters["FRT_PAYER_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("RATE_IN", OracleDbType.Varchar2, 50, "RATE").Direction = ParameterDirection.Input;
                updCommand.Parameters["RATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("AMOUNT1_IN", OracleDbType.Varchar2, 50, "AMOUNT1").Direction = ParameterDirection.Input;
                updCommand.Parameters["AMOUNT1_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CUR_IN", OracleDbType.Varchar2, 50, "CUR").Direction = ParameterDirection.Input;
                updCommand.Parameters["CUR_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("ROE_IN", OracleDbType.Varchar2, 50, "ROE").Direction = ParameterDirection.Input;
                updCommand.Parameters["ROE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("AMOUNT2_IN", OracleDbType.Varchar2, 50, "AMOUNT2").Direction = ParameterDirection.Input;
                updCommand.Parameters["AMOUNT2_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("QUICK_ENTRY_CMN_FK_IN", intQEPK).Direction = ParameterDirection.Input;
                updCommand.Parameters["QUICK_ENTRY_CMN_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with36 = objWK.MyDataAdapter;
                _with36.UpdateCommand = updCommand;
                _with36.UpdateCommand.Transaction = TRAN;

                RecAfct = _with36.Update(M_DataSet);

                if (arrMessage.Count > 0)
                {
                    //This is if any error occurs then rollback..
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    //This part executes only when no error in the execution..
                    TRAN.Commit();
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
                objWK.CloseConnection();
            }
        }

        /// <summary>
        /// Updates the other charges air.
        /// </summary>
        /// <param name="intQEPK">The int qepk.</param>
        /// <param name="intEdiMstPk">The int edi MST pk.</param>
        /// <param name="M_DataSet">The m_ data set.</param>
        /// <returns></returns>
        public ArrayList updateOtherChargesAir(int intQEPK, int intEdiMstPk, DataSet M_DataSet)
        {
            //Sivachandran 17jun08 Imp-Exp-Wiz
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();

            int intPKVal = 0;
            long lngI = 0;
            Int32 RecAfct = default(Int32);
            OracleCommand updCommand = new OracleCommand();

            try
            {
                var _with37 = updCommand;
                _with37.Connection = objWK.MyConnection;
                _with37.CommandType = CommandType.StoredProcedure;
                _with37.CommandText = objWK.MyUserName + ".EDI_MST_TBL_PKG.EDI_QE_OTH_AIR_TBL_UPD";
                var _with38 = _with37.Parameters;

                updCommand.Parameters.Add("QUICK_ENTRY_PK_IN", OracleDbType.Int32, 10, "PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["QUICK_ENTRY_PK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("EDI_MST_FK_IN", intEdiMstPk).Direction = ParameterDirection.Input;
                updCommand.Parameters["EDI_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("FRT_ELEMENT_IN", OracleDbType.Varchar2, 50, "FRT_ELEMENT").Direction = ParameterDirection.Input;
                updCommand.Parameters["FRT_ELEMENT_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("PMT_TYPE_IN", OracleDbType.Varchar2, 150, "PMT_TYPE").Direction = ParameterDirection.Input;
                updCommand.Parameters["PMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("LOCATION_IN", OracleDbType.Varchar2, 50, "LOCATION").Direction = ParameterDirection.Input;
                updCommand.Parameters["LOCATION_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("FRT_PAYER_IN", OracleDbType.Varchar2, 50, "FRT_PAYER").Direction = ParameterDirection.Input;
                updCommand.Parameters["FRT_PAYER_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("RATE_IN", OracleDbType.Varchar2, 50, "RATE").Direction = ParameterDirection.Input;
                updCommand.Parameters["RATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("AMOUNT_IN", OracleDbType.Varchar2, 50, "AMOUNT").Direction = ParameterDirection.Input;
                updCommand.Parameters["AMOUNT_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CUR_IN", OracleDbType.Varchar2, 50, "CUR").Direction = ParameterDirection.Input;
                updCommand.Parameters["CUR_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("ROE_IN", OracleDbType.Varchar2, 50, "ROE").Direction = ParameterDirection.Input;
                updCommand.Parameters["ROE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("TOTAL_IN", OracleDbType.Varchar2, 50, "TOTAL").Direction = ParameterDirection.Input;
                updCommand.Parameters["TOTAL_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("QUICK_ENTRY_CMN_FK_IN", intQEPK).Direction = ParameterDirection.Input;
                updCommand.Parameters["QUICK_ENTRY_CMN_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with39 = objWK.MyDataAdapter;
                _with39.UpdateCommand = updCommand;
                _with39.UpdateCommand.Transaction = TRAN;

                RecAfct = _with39.Update(M_DataSet);

                if (arrMessage.Count > 0)
                {
                    //This is if any error occurs then rollback..
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    //This part executes only when no error in the execution..
                    TRAN.Commit();
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
                objWK.CloseConnection();
            }
        }

        #endregion "Update Function (Quick Entry)"

        #region "Update Function (Booking)"

        /// <summary>
        /// Updates the common details BKG sea.
        /// </summary>
        /// <param name="intBkgPK">The int BKG pk.</param>
        /// <param name="intEdiMstPk">The int edi MST pk.</param>
        /// <param name="strQuotNr">The string quot nr.</param>
        /// <param name="strShipDt">The string ship dt.</param>
        /// <param name="cargoType">Type of the cargo.</param>
        /// <param name="strPol">The string pol.</param>
        /// <param name="strPod">The string pod.</param>
        /// <param name="strShipper">The string shipper.</param>
        /// <param name="strConsignee">The string consignee.</param>
        /// <param name="strCustRefNr">The string customer reference nr.</param>
        /// <param name="strCreditDays">The string credit days.</param>
        /// <param name="strCreditLimit">The string credit limit.</param>
        /// <param name="strDpAgent">The string dp agent.</param>
        /// <param name="strXbkgAgent">The string XBKG agent.</param>
        /// <param name="strClAgent">The string cl agent.</param>
        /// <param name="strTerms">The string terms.</param>
        /// <param name="strPayType">Type of the string pay.</param>
        /// <param name="strMoveCode">The string move code.</param>
        /// <param name="strCmdtyGrp">The string cmdty GRP.</param>
        /// <param name="strLine">The string line.</param>
        /// <param name="strVessel">The string vessel.</param>
        /// <param name="strVoyage">The string voyage.</param>
        /// <param name="strEtdPol">The string etd pol.</param>
        /// <param name="strCutOff">The string cut off.</param>
        /// <param name="strEtaPod">The string eta pod.</param>
        /// <param name="strPackType">Type of the string pack.</param>
        /// <param name="strPackCnt">The string pack count.</param>
        /// <param name="strGWt">The string g wt.</param>
        /// <param name="strNetWt">The string net wt.</param>
        /// <param name="strVol">The string vol.</param>
        /// <param name="strRefType">Type of the string reference.</param>
        /// <param name="strVersionNr">The string version nr.</param>
        /// <returns></returns>
        public string UpdateCommonDetailsBkgSea(int intBkgPK, int intEdiMstPk, string strQuotNr, string strShipDt, string cargoType, string strPol, string strPod, string strShipper, string strConsignee, string strCustRefNr,
        string strCreditDays, string strCreditLimit, string strDpAgent, string strXbkgAgent, string strClAgent, string strTerms, string strPayType, string strMoveCode, string strCmdtyGrp, string strLine,
        string strVessel, string strVoyage, string strEtdPol, string strCutOff, string strEtaPod, string strPackType, string strPackCnt, string strGWt, string strNetWt, string strVol,
        string strRefType, string strVersionNr)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;

            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EDI_MST_TBL_PKG.EDI_BOOKING_SEA_TBL_UPD";
                var _with40 = SCM.Parameters;
                _with40.Add("BOOKING_SEA_PK_IN", intBkgPK).Direction = ParameterDirection.Input;
                _with40.Add("EDI_MST_FK_IN", intEdiMstPk).Direction = ParameterDirection.Input;
                _with40.Add("QUOTATION_REF_NO_IN", (string.IsNullOrEmpty(strQuotNr) ? "" : strQuotNr)).Direction = ParameterDirection.Input;
                _with40.Add("CUST_CUSTOMER_IN", (string.IsNullOrEmpty(strShipper) ? "" : strShipper)).Direction = ParameterDirection.Input;
                _with40.Add("CONS_CUSTOMER_IN", (string.IsNullOrEmpty(strConsignee) ? "" : strConsignee)).Direction = ParameterDirection.Input;
                if (string.IsNullOrEmpty(strShipDt))
                {
                    _with40.Add("SHIPMENT_DATE_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with40.Add("SHIPMENT_DATE_IN", Convert.ToDateTime(strShipDt)).Direction = ParameterDirection.Input;
                }
                _with40.Add("CB_AGENT_IN", (string.IsNullOrEmpty(strXbkgAgent) ? "" : strXbkgAgent)).Direction = ParameterDirection.Input;
                _with40.Add("PACK_TYP_IN", (string.IsNullOrEmpty(strPackType) ? "" : strPackType)).Direction = ParameterDirection.Input;
                _with40.Add("PACK_COUNT_IN", (string.IsNullOrEmpty(strPackCnt) ? "" : strPackCnt)).Direction = ParameterDirection.Input;
                _with40.Add("GROSS_WEIGHT_IN", (string.IsNullOrEmpty(strGWt) ? "" : strGWt)).Direction = ParameterDirection.Input;
                if (cargoType == "FCL")
                {
                    _with40.Add("NET_WEIGHT_IN", (string.IsNullOrEmpty(strNetWt) ? "" : strNetWt)).Direction = ParameterDirection.Input;
                    _with40.Add("CHARGEABLE_WEIGHT_IN", "").Direction = ParameterDirection.Input;
                }
                else if (cargoType == "LCL")
                {
                    _with40.Add("CHARGEABLE_WEIGHT_IN", (string.IsNullOrEmpty(strNetWt) ? "" : strNetWt)).Direction = ParameterDirection.Input;
                    _with40.Add("NET_WEIGHT_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with40.Add("NET_WEIGHT_IN", "").Direction = ParameterDirection.Input;
                    _with40.Add("CHARGEABLE_WEIGHT_IN", "").Direction = ParameterDirection.Input;
                }
                _with40.Add("VOLUME_IN_CBM_IN", (string.IsNullOrEmpty(strVol) ? "" : strVol)).Direction = ParameterDirection.Input;
                _with40.Add("VESSEL_NAME_IN", (string.IsNullOrEmpty(strVessel) ? "" : strVessel)).Direction = ParameterDirection.Input;
                _with40.Add("VOYAGE_IN", (string.IsNullOrEmpty(strVoyage) ? "" : strVoyage)).Direction = ParameterDirection.Input;
                if (string.IsNullOrEmpty(strEtaPod))
                {
                    _with40.Add("ETA_DATE_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with40.Add("ETA_DATE_IN", Convert.ToDateTime(strEtaPod)).Direction = ParameterDirection.Input;
                }
                if (string.IsNullOrEmpty(strEtdPol))
                {
                    _with40.Add("ETD_DATE_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with40.Add("ETD_DATE_IN", Convert.ToDateTime(strEtdPol)).Direction = ParameterDirection.Input;
                }
                if (string.IsNullOrEmpty(strCutOff))
                {
                    _with40.Add("CUT_OFF_DATE_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with40.Add("CUT_OFF_DATE_IN", Convert.ToDateTime(strCutOff)).Direction = ParameterDirection.Input;
                }
                _with40.Add("POD_IN", (string.IsNullOrEmpty(strPod) ? "" : strPod)).Direction = ParameterDirection.Input;
                _with40.Add("POL_IN", (string.IsNullOrEmpty(strPol) ? "" : strPol)).Direction = ParameterDirection.Input;
                _with40.Add("OPERATOR_IN", (string.IsNullOrEmpty(strLine) ? "" : strLine)).Direction = ParameterDirection.Input;
                _with40.Add("CL_AGENT_IN", (string.IsNullOrEmpty(strClAgent) ? "" : strClAgent)).Direction = ParameterDirection.Input;
                _with40.Add("CARGO_MOVE_IN", (string.IsNullOrEmpty(strMoveCode) ? "" : strMoveCode)).Direction = ParameterDirection.Input;
                _with40.Add("PYMT_TYPE_IN", (string.IsNullOrEmpty(strPayType) ? "" : strPayType)).Direction = ParameterDirection.Input;
                _with40.Add("COMMODITY_GROUP_IN", (string.IsNullOrEmpty(strCmdtyGrp) ? "" : strCmdtyGrp)).Direction = ParameterDirection.Input;
                _with40.Add("SHIPPING_TERMS_IN", (string.IsNullOrEmpty(strTerms) ? "" : strTerms)).Direction = ParameterDirection.Input;
                _with40.Add("CUSTOMER_REF_NO_IN", (string.IsNullOrEmpty(strCustRefNr) ? "" : strCustRefNr)).Direction = ParameterDirection.Input;
                _with40.Add("DP_AGENT_IN", (string.IsNullOrEmpty(strDpAgent) ? "" : strDpAgent)).Direction = ParameterDirection.Input;
                _with40.Add("CREDIT_LIMIT_IN", (string.IsNullOrEmpty(strCreditLimit) ? "" : strCreditLimit)).Direction = ParameterDirection.Input;
                _with40.Add("CREDIT_DAYS_IN", (string.IsNullOrEmpty(strCreditDays) ? "" : strCreditDays)).Direction = ParameterDirection.Input;
                _with40.Add("REFERENCE_TYPE_IN", (string.IsNullOrEmpty(strRefType) ? "" : strRefType)).Direction = ParameterDirection.Input;
                _with40.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK);
                _with40.Add("VERSION_NO_IN", CREATED_BY);
                _with40.Add("CONFIG_MST_FK_IN", ConfigurationPK);
                _with40.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                SCM.Connection.Close();
            }
        }

        /// <summary>
        /// Updates the container details BKG sea.
        /// </summary>
        /// <param name="intBkgPK">The int BKG pk.</param>
        /// <param name="intEdiMstPk">The int edi MST pk.</param>
        /// <param name="strCntrType">Type of the string CNTR.</param>
        /// <returns></returns>
        public string UpdateContainerDetailsBkgSea(int intBkgPK, int intEdiMstPk, string strCntrType)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;

            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EDI_MST_TBL_PKG.EDI_BOOKING_CNTR_BASIS_TBL_INS";
                var _with41 = SCM.Parameters;
                _with41.Clear();
                _with41.Add("EDI_MST_FK_IN", intEdiMstPk).Direction = ParameterDirection.Input;
                _with41.Add("BOOKING_SEA_FK_IN", intBkgPK).Direction = ParameterDirection.Input;
                _with41.Add("CONTAINER_TYPE_BASIS_IN", (string.IsNullOrEmpty(strCntrType) ? "" : strCntrType)).Direction = ParameterDirection.Input;
                _with41.Add("CREATED_BY_FK_IN", Convert.ToInt64(CREATED_BY)).Direction = ParameterDirection.Input;
                _with41.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                _with41.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                SCM.Connection.Close();
            }
        }

        /// <summary>
        /// Updates the common details BKG air.
        /// </summary>
        /// <param name="intBkgPK">The int BKG pk.</param>
        /// <param name="intEdiMstPk">The int edi MST pk.</param>
        /// <param name="strQuotNr">The string quot nr.</param>
        /// <param name="strShipDt">The string ship dt.</param>
        /// <param name="strAoo">The string aoo.</param>
        /// <param name="strAod">The string aod.</param>
        /// <param name="strShipper">The string shipper.</param>
        /// <param name="strConsignee">The string consignee.</param>
        /// <param name="strCustRefNr">The string customer reference nr.</param>
        /// <param name="strCreditDays">The string credit days.</param>
        /// <param name="strCreditLimit">The string credit limit.</param>
        /// <param name="strDpAgent">The string dp agent.</param>
        /// <param name="strXbkgAgent">The string XBKG agent.</param>
        /// <param name="strClAgent">The string cl agent.</param>
        /// <param name="strCustomStatus">The string custom status.</param>
        /// <param name="strMoveCode">The string move code.</param>
        /// <param name="strCmdtyGrp">The string cmdty GRP.</param>
        /// <param name="strTerms">The string terms.</param>
        /// <param name="strPayType">Type of the string pay.</param>
        /// <param name="strAirLine">The string air line.</param>
        /// <param name="strFlightNr">The string flight nr.</param>
        /// <param name="strEtdAoo">The string etd aoo.</param>
        /// <param name="strCutOff">The string cut off.</param>
        /// <param name="strEtaAod">The string eta aod.</param>
        /// <param name="strPackType">Type of the string pack.</param>
        /// <param name="strPackCnt">The string pack count.</param>
        /// <param name="strActWt">The string act wt.</param>
        /// <param name="strVolWt">The string vol wt.</param>
        /// <param name="strChrgWt">The string CHRG wt.</param>
        /// <param name="strUldCnt">The string uld count.</param>
        /// <param name="strVol">The string vol.</param>
        /// <param name="strDensity">The string density.</param>
        /// <param name="strVersionNr">The string version nr.</param>
        /// <returns></returns>
        public string UpdateCommonDetailsBkgAir(int intBkgPK, int intEdiMstPk, string strQuotNr, string strShipDt, string strAoo, string strAod, string strShipper, string strConsignee, string strCustRefNr, string strCreditDays,
        string strCreditLimit, string strDpAgent, string strXbkgAgent, string strClAgent, string strCustomStatus, string strMoveCode, string strCmdtyGrp, string strTerms, string strPayType, string strAirLine,
        string strFlightNr, string strEtdAoo, string strCutOff, string strEtaAod, string strPackType, string strPackCnt, string strActWt, string strVolWt, string strChrgWt, string strUldCnt,
        string strVol, string strDensity, string strVersionNr)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;

            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EDI_MST_TBL_PKG.EDI_BOOKING_AIR_TBL_UPD";
                var _with42 = SCM.Parameters;
                _with42.Add("BOOKING_AIR_PK_IN", intBkgPK).Direction = ParameterDirection.Input;
                _with42.Add("EDI_MST_FK_IN", intEdiMstPk).Direction = ParameterDirection.Input;
                _with42.Add("QUOTATION_REF_NO_IN", (string.IsNullOrEmpty(strQuotNr) ? "" : strQuotNr)).Direction = ParameterDirection.Input;
                _with42.Add("CUST_CUSTOMER_IN", (string.IsNullOrEmpty(strShipper) ? "" : strShipper)).Direction = ParameterDirection.Input;
                _with42.Add("CONS_CUSTOMER_IN", (string.IsNullOrEmpty(strConsignee) ? "" : strConsignee)).Direction = ParameterDirection.Input;
                _with42.Add("POD_IN", (string.IsNullOrEmpty(strAod) ? "" : strAod)).Direction = ParameterDirection.Input;
                _with42.Add("POL_IN", (string.IsNullOrEmpty(strAoo) ? "" : strAoo)).Direction = ParameterDirection.Input;
                _with42.Add("AIRLINE_IN", (string.IsNullOrEmpty(strAirLine) ? "" : strAirLine)).Direction = ParameterDirection.Input;
                _with42.Add("CARGO_MOVE_IN", (string.IsNullOrEmpty(strMoveCode) ? "" : strMoveCode)).Direction = ParameterDirection.Input;
                _with42.Add("PYMT_TYPE_IN", (string.IsNullOrEmpty(strPayType) ? "" : strPayType)).Direction = ParameterDirection.Input;
                _with42.Add("SHIPMENT_DATE_IN", (string.IsNullOrEmpty(strShipDt) ? "" : strShipDt)).Direction = ParameterDirection.Input;
                _with42.Add("CB_AGENT_IN", (string.IsNullOrEmpty(strXbkgAgent) ? "" : strXbkgAgent)).Direction = ParameterDirection.Input;
                _with42.Add("CL_AGENT_IN", (string.IsNullOrEmpty(strClAgent) ? "" : strClAgent)).Direction = ParameterDirection.Input;
                _with42.Add("PACK_TYPE_IN", (string.IsNullOrEmpty(strPackType) ? "" : strPackType)).Direction = ParameterDirection.Input;
                _with42.Add("PACK_COUNT_IN", (string.IsNullOrEmpty(strPackCnt) ? "" : strPackCnt)).Direction = ParameterDirection.Input;
                _with42.Add("GROSS_WEIGHT_IN", (string.IsNullOrEmpty(strActWt) ? "" : strActWt)).Direction = ParameterDirection.Input;
                _with42.Add("CHARGEABLE_WEIGHT_IN", (string.IsNullOrEmpty(strChrgWt) ? "" : strChrgWt)).Direction = ParameterDirection.Input;
                _with42.Add("VOLUME_IN_CBM_IN", (string.IsNullOrEmpty(strVol) ? "" : strVol)).Direction = ParameterDirection.Input;
                _with42.Add("FLIGHT_NO_IN", (string.IsNullOrEmpty(strFlightNr) ? "" : strFlightNr)).Direction = ParameterDirection.Input;
                _with42.Add("ETA_DATE_IN", (string.IsNullOrEmpty(strEtaAod) ? "" : strEtaAod)).Direction = ParameterDirection.Input;
                _with42.Add("ETD_DATE_IN", (string.IsNullOrEmpty(strEtdAoo) ? "" : strEtdAoo)).Direction = ParameterDirection.Input;
                _with42.Add("CUT_OFF_DATE_IN", (string.IsNullOrEmpty(strCutOff) ? "" : strCutOff)).Direction = ParameterDirection.Input;
                _with42.Add("COMMODITY_GROUP_IN", (string.IsNullOrEmpty(strCmdtyGrp) ? "" : strCmdtyGrp)).Direction = ParameterDirection.Input;
                _with42.Add("CUSTOMER_REF_NO_IN", (string.IsNullOrEmpty(strCustRefNr) ? "" : strCustRefNr)).Direction = ParameterDirection.Input;
                _with42.Add("CUSTOMS_CODE_IN", (string.IsNullOrEmpty(strCustomStatus) ? "" : strCustomStatus)).Direction = ParameterDirection.Input;
                _with42.Add("DP_AGENT_IN", (string.IsNullOrEmpty(strDpAgent) ? "" : strDpAgent)).Direction = ParameterDirection.Input;
                _with42.Add("VOLUME_WEIGHT_IN", (string.IsNullOrEmpty(strVolWt) ? "" : strVolWt)).Direction = ParameterDirection.Input;
                _with42.Add("DENSITY_IN", (string.IsNullOrEmpty(strDensity) ? "" : strDensity)).Direction = ParameterDirection.Input;
                _with42.Add("NO_OF_BOXES_IN", (string.IsNullOrEmpty(strUldCnt) ? "" : strUldCnt)).Direction = ParameterDirection.Input;
                _with42.Add("SHIPPING_TERMS_IN", (string.IsNullOrEmpty(strTerms) ? "" : strTerms)).Direction = ParameterDirection.Input;
                _with42.Add("CREDIT_LIMIT_IN", (string.IsNullOrEmpty(strCreditLimit) ? "" : strCreditLimit)).Direction = ParameterDirection.Input;
                _with42.Add("CREDIT_DAYS_IN", (string.IsNullOrEmpty(strCreditDays) ? "" : strCreditDays)).Direction = ParameterDirection.Input;
                _with42.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK);
                _with42.Add("VERSION_NO_IN", CREATED_BY);
                _with42.Add("CONFIG_MST_FK_IN", ConfigurationPK);
                _with42.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                SCM.Connection.Close();
            }
        }

        #endregion "Update Function (Booking)"

        #region "Load"

        /// <summary>
        /// Loads the list.
        /// </summary>
        /// <param name="intBizType">Type of the int biz.</param>
        /// <param name="strModule">The string module.</param>
        /// <param name="strFileName">Name of the string file.</param>
        /// <param name="strUploadedBy">The string uploaded by.</param>
        /// <param name="strUploadedOn">The string uploaded on.</param>
        /// <param name="strStatus">The string status.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <returns></returns>
        public DataSet LoadList(int intBizType, string strModule, string strFileName, string strUploadedBy, string strUploadedOn, string strStatus, Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strSql = "";
            WorkFlow objWF = new WorkFlow();
            Int32 TotalRecords = default(Int32);
            Int32 last = default(Int32);
            Int32 start = default(Int32);

            sb.Append(" SELECT ROWNUM SLNR,T.* FROM(SELECT EMT.EDI_MST_PK PK,CASE EMT.BUSINESS_TYPE WHEN 1 THEN 'Air' WHEN 2 THEN 'Sea' END AS BIZTYPE,");
            sb.Append(" EMT.MODULE MODULENAME,EMT.FILE_NAME FILENAME,UMT.USER_NAME UPLOADEDBY,TO_CHAR(EMT.UPLOADED_DATE_TIME, DATEFORMAT || ' HH24:MI') UPLOADEDDATETIME,CASE EMT.ERROR WHEN 0 THEN 'Processed' ELSE 'Error' END AS STATUS,");
            sb.Append(" EMT.TOTAL_RECORDS TOTALRECORDS,EMT.PROCESSED PROCESSED,EMT.ERROR ERRORCOUNT FROM EDI_MST_TBL EMT,user_mst_tbl UMT ");
            sb.Append(" WHERE UMT.USER_MST_PK(+)=EMT.CREATED_BY_FK AND EMT.BUSINESS_TYPE=" + intBizType + " AND EMT.MODULE='" + strModule + "'  ");
            if (!string.IsNullOrEmpty(strFileName))
            {
                sb.Append(" AND EMT.FILE_NAME='" + strFileName + "' ");
            }
            if (!string.IsNullOrEmpty(strUploadedBy))
            {
                sb.Append(" AND EMT.CREATED_BY_FK=" + strUploadedBy + " ");
            }
            if (!string.IsNullOrEmpty(strUploadedOn))
            {
                sb.Append(" AND to_date(EMT.CREATED_DT,'" + dateFormat + "') = to_date('" + strUploadedOn + "','" + dateFormat + "') ");
            }
            if (strStatus == "1")
            {
                sb.Append(" AND NVL(EMT.PROCESSED,0) = NVL(EMT.TOTAL_RECORDS,0) ");
            }
            else if (strStatus == "2")
            {
                sb.Append(" AND NVL(EMT.ERROR,0) > 0 ");
            }
            sb.Append(" AND UMT.DEFAULT_LOCATION_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
            sb.Append(" ORDER BY EMT.UPLOADED_DATE_TIME DESC) T ");
            strSql = sb.ToString();

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*)  from ");
            strCount.Append(" (" + sb.ToString() + ")");
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));
            TotalPage = TotalRecords / RecordsPerPage;
            if (TotalRecords % RecordsPerPage != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;
            last = CurrentPage * RecordsPerPage;
            start = (CurrentPage - 1) * RecordsPerPage + 1;

            return objWF.GetDataSet(strSql);
        }

        #endregion "Load"
    }
}