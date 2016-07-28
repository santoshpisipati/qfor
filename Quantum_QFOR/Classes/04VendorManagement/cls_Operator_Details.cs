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
    public class cls_Operator_Details : CommonFeatures
    {
        #region "Fetch Function"

        public DataSet FetchAll(string OPERATORID = "")
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            strCondition = "AND ACD.OPERATOR_MST_FK = '" + OPERATORID + "' AND ALM.OPERATOR_MST_PK = '" + OPERATORID + "'";
            strCondition += " AND ADMCNR.COUNTRY_MST_PK(+) = ACD.ADM_COUNTRY_MST_FK ";
            strCondition += " AND CORCNR.COUNTRY_MST_PK(+) = ACD.COR_COUNTRY_MST_FK";
            strCondition += " AND BILLCNR.COUNTRY_MST_PK(+) = ACD.BILL_COUNTRY_MST_FK";

            strSQL = " SELECT ALM.OPERATOR_ID,";
            strSQL = strSQL + "ALM.OPERATOR_MST_PK, ";
            strSQL = strSQL + "ALM.OPERATOR_NAME,";
            strSQL = strSQL + "ALM.ACTIVE_FLAG,";
            strSQL = strSQL + "ALM.CARGO_DEL_ADDRESS,";
            strSQL = strSQL + "ALM.BL_PREFIX,";
            strSQL = strSQL + "ALM.ACCOUNT_NO,";
            strSQL = strSQL + "ALM.LOCATION_MST_FK,";
            strSQL = strSQL + "ALM.VAT_NO,";
            strSQL = strSQL + "ALM.FAC_PERCENTAGE,";
            strSQL = strSQL + "ALM.FAC_APPLICABLE_ON,";
            strSQL = strSQL + "ALM.FAC_INVOICE,";

            strSQL = strSQL + "ALM.nrofdays,";

            strSQL = strSQL + "ALM.inwkend,";

            strSQL = strSQL + "ALM.incalday,";

            strSQL = strSQL + "ALM.weekends,";

            strSQL = strSQL + "ACD.ADM_ADDRESS_1,";
            strSQL = strSQL + "ACD.ADM_ADDRESS_2,";
            strSQL = strSQL + "ACD.ADM_ADDRESS_3,";
            strSQL = strSQL + "ACD.ADM_CITY,";
            strSQL = strSQL + "ACD.ADM_ZIP_CODE,";
            strSQL = strSQL + "ACD.ADM_COUNTRY_MST_FK,";
            strSQL = strSQL + "ACD.ADM_SALUTATION,";
            strSQL = strSQL + "ACD.ADM_CONTACT_PERSON,";
            strSQL = strSQL + "ACD.ADM_PHONE_NO_1,";
            strSQL = strSQL + "ACD.ADM_PHONE_NO_2,";
            strSQL = strSQL + "ACD.ADM_FAX_NO,";
            strSQL = strSQL + "ACD.ADM_EMAIL_ID,";
            strSQL = strSQL + "ACD.ADM_URL,";
            strSQL = strSQL + "ACD.ADM_SHORT_NAME,";

            strSQL = strSQL + "ACD.COR_ADDRESS_1,";
            strSQL = strSQL + "ACD.COR_ADDRESS_2,";
            strSQL = strSQL + "ACD.COR_ADDRESS_3,";
            strSQL = strSQL + "ACD.COR_CITY,";
            strSQL = strSQL + "ACD.COR_ZIP_CODE,";
            strSQL = strSQL + "ACD.COR_COUNTRY_MST_FK,";
            strSQL = strSQL + "ACD.COR_SALUTATION,";
            strSQL = strSQL + "ACD.COR_CONTACT_PERSON,";
            strSQL = strSQL + "ACD.COR_PHONE_NO_1,";
            strSQL = strSQL + "ACD.COR_PHONE_NO_2,";
            strSQL = strSQL + "ACD.COR_FAX_NO,";
            strSQL = strSQL + "ACD.COR_EMAIL_ID,";
            strSQL = strSQL + "ACD.COR_URL,";
            strSQL = strSQL + "ACD.COR_SHORT_NAME,";

            strSQL = strSQL + "ACD.BILL_ADDRESS_1,";
            strSQL = strSQL + "ACD.BILL_ADDRESS_2,";
            strSQL = strSQL + "ACD.BILL_ADDRESS_3,";
            strSQL = strSQL + "ACD.BILL_CITY,";
            strSQL = strSQL + "ACD.BILL_ZIP_CODE,";
            strSQL = strSQL + "ACD.BILL_COUNTRY_MST_FK,";
            strSQL = strSQL + "ACD.BILL_SALUTATION,";
            strSQL = strSQL + "ACD.BILL_CONTACT_PERSON,";
            strSQL = strSQL + "ACD.BILL_PHONE_NO_1,";
            strSQL = strSQL + "ACD.BILL_PHONE_NO_2,";
            strSQL = strSQL + "ACD.BILL_FAX_NO,";
            strSQL = strSQL + "ACD.BILL_EMAIL_ID,";
            strSQL = strSQL + "ACD.BILL_URL,";
            strSQL = strSQL + "ACD.BILL_SHORT_NAME,";
            strSQL = strSQL + "ALM.VERSION_NO,";
            strSQL = strSQL + "ADMCNR.COUNTRY_ID AS ADMCOUNTRYID,";
            strSQL = strSQL + "ADMCNR.COUNTRY_NAME AS ADMCOUNTRYNAME,";
            strSQL = strSQL + "BILLCNR.COUNTRY_ID BILLCOUNTRYID,";
            strSQL = strSQL + "BILLCNR.COUNTRY_NAME BILLCOUNTRYNAME,";
            strSQL = strSQL + "CORCNR.COUNTRY_ID CORCOUNTRYID,";
            strSQL = strSQL + "CORCNR.COUNTRY_NAME CORCOUNTRYNAME,";
            strSQL = strSQL + " ALM.REMARKS,";
            strSQL = strSQL + " ALM.CUSTOMS_CODE";
            strSQL = strSQL + " FROM OPERATOR_MST_TBL ALM,OPERATOR_CONTACT_DTLS ACD, ";
            strSQL = strSQL + " COUNTRY_MST_TBL ADMCNR,";
            strSQL = strSQL + " COUNTRY_MST_TBL CORCNR,";
            strSQL = strSQL + " COUNTRY_MST_TBL BILLCNR";
            strSQL = strSQL + "WHERE ( 1 = 1) ";
            strSQL = strSQL + strCondition;

            try
            {
                return objWF.GetDataSet(strSQL);
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

        #endregion "Fetch Function"

        #region "Save New"

        public ArrayList SaveNew(DataSet Holidaycan, string OPERATORID, string OPERATORName, string activeflag, string CargoDelAddress, string BillPrefix, string LOCATIONKEY, string VATCODE, string ACCOUNTNO, string ADMADDRESS1,
        string ADMADDRESS2, string ADMADDRESS3, string ADMCITY, string ADMZIPCODE, Int32 ADMCOUNTRY, short AdmSalutation, string ADMCONTACTPERSON, string ADMPHONE1, string ADMPHONE2, string ADMFAXNO,
        string ADMEMAIL, string ADMURL, string ADMSHORTNAME, string CORADDRESS1, string CORADDRESS2, string CORADDRESS3, string CORCITY, string CORZIPCODE, Int32 CORCOUNTRY, short CorSalutation,
        string CORCONTACTPERSON, string CORPHONE1, string CORPHONE2, string CORFAXNO, string COREMAIL, string CORURL, string CORSHORTNAME, string BILLADDRESS1, string BILLADDRESS2, string BILLADDRESS3,
        string BILLCITY, string BILLZIPCODE, Int32 BILLCOUNTRY, short BillSalutation, string BILLCONTACTPERSON, string BILLPHONE1, string BILLPHONE2, string BILLFAXNO, string BILLEMAIL, string BILLURL,
        string BILLSHORTNAME, string AIRREMARKS, long lngDepotPK = 0, string Customs_Code = "", int FACInv = 0, int vasseldalday = 0, int includecalanderholiday = 0, int includeweekendholiday = 0, string week_in = "")
        {
            int intPKVal = 0;
            long lngI = 0;
            DateTime EnteredBudgetStartDate = default(DateTime);
            Int32 RecAfct = default(Int32);
            System.DBNull StrNull = null;
            long lngBudHdrPK = 0;
            OracleCommand insCommand = new OracleCommand();
            System.DateTime EnteredDate = default(System.DateTime);
            Int32 inti = default(Int32);
            WorkFlow objWK = new WorkFlow();

            objWK.OpenConnection();
            M_Configuration_PK = 1003;
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();

            try
            {
                DataTable DtTbl = new DataTable();
                DataRow DtRw = null;
                int i = 0;
                var _with1 = insCommand;
                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".OPERATOR_MST_TBL_PKG.OPERATOR_MST_TBL_INS";

                var _with2 = _with1.Parameters;
                _with2.Add("OPERATOR_ID_IN", (string.IsNullOrEmpty(OPERATORID) ? "" : OPERATORID)).Direction = ParameterDirection.Input;
                _with2.Add("OPERATOR_NAME_IN", (string.IsNullOrEmpty(OPERATORName) ? "" : OPERATORName)).Direction = ParameterDirection.Input;
                _with2.Add("ACTIVE_FLAG_IN", activeflag).Direction = ParameterDirection.Input;
                _with2.Add("CARGO_DEL_ADDRESS_IN", (string.IsNullOrEmpty(CargoDelAddress) ? "" : CargoDelAddress)).Direction = ParameterDirection.Input;
                _with2.Add("BL_PREFIX_IN", (string.IsNullOrEmpty(BillPrefix) ? "" : BillPrefix)).Direction = ParameterDirection.Input;
                _with2.Add("LOCATION_MST_FK_IN", "").Direction = ParameterDirection.Input;
                _with2.Add("VAT_NO_IN", (string.IsNullOrEmpty(VATCODE) ? "" : VATCODE)).Direction = ParameterDirection.Input;
                _with2.Add("ACCOUNT_NO_IN", (string.IsNullOrEmpty(ACCOUNTNO) ? "" : ACCOUNTNO)).Direction = ParameterDirection.Input;

                _with2.Add("REMARKS_IN", (string.IsNullOrEmpty(AIRREMARKS) ? "" : AIRREMARKS)).Direction = ParameterDirection.Input;

                _with2.Add("ADM_ADDRESS_1_IN", (string.IsNullOrEmpty(ADMADDRESS1) ? "" : ADMADDRESS1)).Direction = ParameterDirection.Input;
                _with2.Add("ADM_ADDRESS_2_IN", (string.IsNullOrEmpty(ADMADDRESS2) ? "" : ADMADDRESS2)).Direction = ParameterDirection.Input;
                _with2.Add("ADM_ADDRESS_3_IN", (string.IsNullOrEmpty(ADMADDRESS3) ? "" : ADMADDRESS3)).Direction = ParameterDirection.Input;
                _with2.Add("ADM_CITY_IN", (string.IsNullOrEmpty(ADMCITY) ? "" : ADMCITY)).Direction = ParameterDirection.Input;
                _with2.Add("ADM_ZIP_CODE_IN", (string.IsNullOrEmpty(ADMZIPCODE) ? "" : ADMZIPCODE)).Direction = ParameterDirection.Input;
                _with2.Add("ADM_COUNTRY_MST_FK_IN", (ADMCOUNTRY == 0 ? 0 : ADMCOUNTRY)).Direction = ParameterDirection.Input;
                _with2.Add("ADM_SALUTATION_IN", (AdmSalutation == 0 ? 0 : AdmSalutation)).Direction = ParameterDirection.Input;
                _with2.Add("ADM_CONTACT_PERSON_IN", (string.IsNullOrEmpty(ADMCONTACTPERSON) ? "" : ADMCONTACTPERSON)).Direction = ParameterDirection.Input;
                _with2.Add("ADM_PHONE_NO_1_IN", (string.IsNullOrEmpty(ADMPHONE1) ? "" : ADMPHONE1)).Direction = ParameterDirection.Input;
                _with2.Add("ADM_PHONE_NO_2_IN", (string.IsNullOrEmpty(ADMPHONE2) ? "" : ADMPHONE2)).Direction = ParameterDirection.Input;
                _with2.Add("ADM_FAX_NO_IN", (string.IsNullOrEmpty(ADMFAXNO) ? "" : ADMFAXNO)).Direction = ParameterDirection.Input;
                _with2.Add("ADM_EMAIL_ID_IN", (string.IsNullOrEmpty(ADMEMAIL) ? "" : ADMEMAIL)).Direction = ParameterDirection.Input;
                _with2.Add("ADM_URL_IN", (string.IsNullOrEmpty(ADMURL) ? "" : ADMURL)).Direction = ParameterDirection.Input;
                _with2.Add("ADM_SHORT_NAME_IN", (string.IsNullOrEmpty(ADMSHORTNAME) ? "" : ADMSHORTNAME)).Direction = ParameterDirection.Input;
                _with2.Add("COR_ADDRESS_1_IN", (string.IsNullOrEmpty(CORADDRESS1) ? "" : CORADDRESS1)).Direction = ParameterDirection.Input;
                _with2.Add("COR_ADDRESS_2_IN", (string.IsNullOrEmpty(CORADDRESS2) ? "" : CORADDRESS2)).Direction = ParameterDirection.Input;
                _with2.Add("COR_ADDRESS_3_IN", (string.IsNullOrEmpty(CORADDRESS3) ? "" : CORADDRESS3)).Direction = ParameterDirection.Input;
                _with2.Add("COR_CITY_IN", (string.IsNullOrEmpty(CORCITY) ? "" : CORCITY)).Direction = ParameterDirection.Input;
                _with2.Add("COR_ZIP_CODE_IN", (string.IsNullOrEmpty(CORZIPCODE) ? "" : CORZIPCODE)).Direction = ParameterDirection.Input;
                _with2.Add("COR_COUNTRY_MST_FK_IN", (CORCOUNTRY == 0 ? 0 : CORCOUNTRY)).Direction = ParameterDirection.Input;
                _with2.Add("COR_SALUTATION_IN", (CorSalutation == 0 ? 0 : CorSalutation)).Direction = ParameterDirection.Input;
                _with2.Add("COR_CONTACT_PERSON_IN", (string.IsNullOrEmpty(CORCONTACTPERSON) ? "" : CORCONTACTPERSON)).Direction = ParameterDirection.Input;
                _with2.Add("COR_PHONE_NO_1_IN", (string.IsNullOrEmpty(CORPHONE1) ? "" : CORPHONE1)).Direction = ParameterDirection.Input;
                _with2.Add("COR_PHONE_NO_2_IN", (string.IsNullOrEmpty(CORPHONE2) ? "" : CORPHONE2)).Direction = ParameterDirection.Input;
                _with2.Add("COR_FAX_NO_IN", (string.IsNullOrEmpty(CORFAXNO) ? "" : CORFAXNO)).Direction = ParameterDirection.Input;
                _with2.Add("COR_EMAIL_ID_IN", (string.IsNullOrEmpty(COREMAIL) ? "" : COREMAIL)).Direction = ParameterDirection.Input;
                _with2.Add("COR_URL_IN", (string.IsNullOrEmpty(CORURL) ? "" : CORURL)).Direction = ParameterDirection.Input;
                _with2.Add("COR_SHORT_NAME_IN", (string.IsNullOrEmpty(CORSHORTNAME) ? "" : CORSHORTNAME)).Direction = ParameterDirection.Input;
                _with2.Add("BILL_ADDRESS_1_IN", (string.IsNullOrEmpty(BILLADDRESS1) ? "" : BILLADDRESS1)).Direction = ParameterDirection.Input;
                _with2.Add("BILL_ADDRESS_2_IN", (string.IsNullOrEmpty(BILLADDRESS2) ? "" : BILLADDRESS2)).Direction = ParameterDirection.Input;
                _with2.Add("BILL_ADDRESS_3_IN", (string.IsNullOrEmpty(BILLADDRESS3) ? "" : BILLADDRESS3)).Direction = ParameterDirection.Input;
                _with2.Add("BILL_CITY_IN", (string.IsNullOrEmpty(BILLCITY) ? "" : BILLCITY)).Direction = ParameterDirection.Input;
                _with2.Add("BILL_ZIP_CODE_IN", (string.IsNullOrEmpty(BILLZIPCODE) ? "" : BILLZIPCODE)).Direction = ParameterDirection.Input;
                _with2.Add("BILL_COUNTRY_MST_FK_IN", (BILLCOUNTRY == 0 ? 0 : BILLCOUNTRY)).Direction = ParameterDirection.Input;
                _with2.Add("BILL_SALUTATION_IN", (BillSalutation == 0 ? 0 : BillSalutation)).Direction = ParameterDirection.Input;
                _with2.Add("BILL_CONTACT_PERSON_IN", (string.IsNullOrEmpty(BILLCONTACTPERSON) ? "" : BILLCONTACTPERSON)).Direction = ParameterDirection.Input;
                _with2.Add("BILL_PHONE_NO_1_IN", (string.IsNullOrEmpty(BILLPHONE1) ? "" : BILLPHONE1)).Direction = ParameterDirection.Input;
                _with2.Add("BILL_PHONE_NO_2_IN", (string.IsNullOrEmpty(BILLPHONE2) ? "" : BILLPHONE2)).Direction = ParameterDirection.Input;
                _with2.Add("BILL_FAX_NO_IN", (string.IsNullOrEmpty(BILLFAXNO) ? "" : BILLFAXNO)).Direction = ParameterDirection.Input;
                _with2.Add("BILL_EMAIL_ID_IN", (string.IsNullOrEmpty(BILLEMAIL) ? "" : BILLEMAIL)).Direction = ParameterDirection.Input;
                _with2.Add("BILL_URL_IN", (string.IsNullOrEmpty(BILLURL) ? "" : BILLURL)).Direction = ParameterDirection.Input;
                _with2.Add("BILL_SHORT_NAME_IN", (string.IsNullOrEmpty(BILLSHORTNAME) ? "" : BILLSHORTNAME)).Direction = ParameterDirection.Input;

                _with2.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                _with2.Add("CREATEDD_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                _with2.Add("CONFIG_PK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                _with2.Add("CUSTOMS_CODE_IN", (string.IsNullOrEmpty(Customs_Code) ? "" : Customs_Code)).Direction = ParameterDirection.Input;
                _with2.Add("FAC_INVOICE_IN", FACInv).Direction = ParameterDirection.Input;
                _with2.Add("VESSEL_SAL_DAY", vasseldalday).Direction = ParameterDirection.Input;
                _with2.Add("INCLUDE_CALENDER_HOLIDAY", includecalanderholiday).Direction = ParameterDirection.Input;
                _with2.Add("INCLUDE_WEEK_DAY", includeweekendholiday).Direction = ParameterDirection.Input;
                _with2.Add("WEEKDAY", week_in).Direction = ParameterDirection.Input;

                _with2.Add("RETURN_VALUE", lngDepotPK).Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with3 = objWK.MyDataAdapter;
                _with3.InsertCommand = insCommand;
                _with3.InsertCommand.Transaction = TRAN;
                _with3.InsertCommand.ExecuteNonQuery();
                lngDepotPK = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
                TRAN.Commit();
                return arrMessage;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                TRAN.Rollback();
                return arrMessage;
            }
            finally
            {
                objWK.CloseConnection();
            }
            if (arrMessage.Count > 0)
            {
                return arrMessage;
            }
            else
            {
                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    arrMessage = SaveHoliday(Holidaycan, (Int32)lngDepotPK, M_CREATED_BY_FK.ToString(), ConfigurationPK.ToString());
                    if (arrMessage.Count > 0)
                    {
                        TRAN.Rollback();
                        return arrMessage;
                    }
                    else
                    {
                        TRAN.Commit();
                        arrMessage.Add("All Data Saved Successfully");
                        // arrMessage.Add(g_PKValue)
                        return arrMessage;
                    }
                }
            }
        }

        #endregion "Save New"

        #region "Update"

        public ArrayList UpdateData(DataSet Holidaycan, string OPERATORID, Int32 OPERATORPkValue, string OPERATORName, string activeflag, string CargoDelAddress, string AirwayBillPrefix, string LocationKey, string VATCODE, string ACCOUNTNO,
        string ADMADDRESS1, string ADMADDRESS2, string ADMADDRESS3, string ADMCITY, string ADMZIPCODE, Int32 ADMCOUNTRY, short AdmSalutation, string ADMCONTACTPERSON, string ADMPHONE1, string ADMPHONE2,
        string ADMFAXNO, string ADMEMAIL, string ADMURL, string ADMSHORTNAME, string CORADDRESS1, string CORADDRESS2, string CORADDRESS3, string CORCITY, string CORZIPCODE, Int32 CORCOUNTRY,
        string CorSalutation, string CORCONTACTPERSON, string CORPHONE1, string CORPHONE2, string CORFAXNO, string COREMAIL, string CORURL, string CORSHORTNAME, string BILLADDRESS1, string BILLADDRESS2,
        string BILLADDRESS3, string BILLCITY, string BILLZIPCODE, Int32 BILLCOUNTRY, string BillSalutation, string BILLCONTACTPERSON, string BILLPHONE1, string BILLPHONE2, string BILLFAXNO, string BILLEMAIL,
        string BILLURL, string BILLSHORTNAME, int VersionNo, string AIRREMARKS, double FAC = 0.0, string FACApplOn = "0", string Customs_Code = "", int FACInv = 0, int vasseldalday = 0, int includecalanderholiday = 0,
        int includeweekendholiday = 0, string week_in = "", Int16 modifyfk = 0)
        {
            int intPKVal = 0;
            long lngI = 0;
            DateTime EnteredBudgetStartDate = default(DateTime);
            Int32 RecAfct = default(Int32);
            System.DBNull StrNull = null;
            long lngBudHdrPK = 0;
            OracleCommand insCommand = new OracleCommand();
            System.DateTime EnteredDate = default(System.DateTime);
            Int32 inti = default(Int32);
            long lngDepotPK = 0;
            OracleCommand updCommand = new OracleCommand();
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            M_Configuration_PK = 1003;
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            int configKey = 0;
            configKey = 1;

            try
            {
                DataTable DtTbl = new DataTable();
                DataRow DtRw = null;
                int i = 0;
                //DtTbl = M_Dataset.Tables(0)
                var _with4 = updCommand;
                _with4.Connection = objWK.MyConnection;
                _with4.CommandType = CommandType.StoredProcedure;
                _with4.CommandText = objWK.MyUserName + ".OPERATOR_MST_TBL_PKG.OPERATOR_MST_TBL_UPD";

                var _with5 = _with4.Parameters;
                _with5.Add("OPERATOR_MST_PK_IN", OPERATORPkValue).Direction = ParameterDirection.Input;
                _with5.Add("OPERATOR_ID_IN", (string.IsNullOrEmpty(OPERATORID) ? "" : OPERATORID)).Direction = ParameterDirection.Input;
                _with5.Add("OPERATOR_NAME_IN", (string.IsNullOrEmpty(OPERATORName) ? "" : OPERATORName)).Direction = ParameterDirection.Input;
                _with5.Add("ACTIVE_FLAG_IN", activeflag).Direction = ParameterDirection.Input;
                _with5.Add("CARGO_DEL_ADDRESS_IN", (string.IsNullOrEmpty(CargoDelAddress) ? "" : CargoDelAddress)).Direction = ParameterDirection.Input;
                _with5.Add("BL_PREFIX_IN", (string.IsNullOrEmpty(AirwayBillPrefix) ? "" : AirwayBillPrefix)).Direction = ParameterDirection.Input;
                _with5.Add("LOCATION_MST_FK_IN", "").Direction = ParameterDirection.Input;
                _with5.Add("VAT_NO_IN", (string.IsNullOrEmpty(VATCODE) ? "" : VATCODE)).Direction = ParameterDirection.Input;
                _with5.Add("ACCOUNT_NO_IN", (string.IsNullOrEmpty(ACCOUNTNO) ? "" : ACCOUNTNO)).Direction = ParameterDirection.Input;
                //CODE ADDED BY SUMI TO INCLUDE REMARKS FIELD
                _with5.Add("REMARKS_IN", (string.IsNullOrEmpty(AIRREMARKS) ? "" : AIRREMARKS)).Direction = ParameterDirection.Input;
                //END CODE
                _with5.Add("ADM_ADDRESS_1_IN", (string.IsNullOrEmpty(ADMADDRESS1) ? "" : ADMADDRESS1)).Direction = ParameterDirection.Input;
                _with5.Add("ADM_ADDRESS_2_IN", (string.IsNullOrEmpty(ADMADDRESS2) ? "" : ADMADDRESS2)).Direction = ParameterDirection.Input;
                _with5.Add("ADM_ADDRESS_3_IN", (string.IsNullOrEmpty(ADMADDRESS3) ? "" : ADMADDRESS3)).Direction = ParameterDirection.Input;
                _with5.Add("ADM_CITY_IN", (string.IsNullOrEmpty(ADMCITY) ? "" : ADMCITY)).Direction = ParameterDirection.Input;
                _with5.Add("ADM_ZIP_CODE_IN", (string.IsNullOrEmpty(ADMZIPCODE) ? "" : ADMZIPCODE)).Direction = ParameterDirection.Input;
                _with5.Add("ADM_COUNTRY_MST_FK_IN", (ADMCOUNTRY == 0 ? 0 : ADMCOUNTRY)).Direction = ParameterDirection.Input;

                _with5.Add("ADM_SALUTATION_IN", (AdmSalutation == 0 ? 0 : AdmSalutation)).Direction = ParameterDirection.Input;
                _with5.Add("ADM_CONTACT_PERSON_IN", (string.IsNullOrEmpty(ADMCONTACTPERSON) ? "" : ADMCONTACTPERSON)).Direction = ParameterDirection.Input;
                _with5.Add("ADM_PHONE_NO_1_IN", (string.IsNullOrEmpty(ADMPHONE1) ? "" : ADMPHONE1)).Direction = ParameterDirection.Input;
                _with5.Add("ADM_PHONE_NO_2_IN", (string.IsNullOrEmpty(ADMPHONE2) ? "" : ADMPHONE2)).Direction = ParameterDirection.Input;
                _with5.Add("ADM_FAX_NO_IN", (string.IsNullOrEmpty(ADMFAXNO) ? "" : ADMFAXNO)).Direction = ParameterDirection.Input;
                _with5.Add("ADM_EMAIL_ID_IN", (string.IsNullOrEmpty(ADMEMAIL) ? "" : ADMEMAIL)).Direction = ParameterDirection.Input;
                _with5.Add("ADM_URL_IN", (string.IsNullOrEmpty(ADMURL) ? "" : ADMURL)).Direction = ParameterDirection.Input;
                _with5.Add("ADM_SHORT_NAME_IN", (string.IsNullOrEmpty(ADMSHORTNAME) ? "" : ADMSHORTNAME)).Direction = ParameterDirection.Input;
                _with5.Add("COR_ADDRESS_1_IN", (string.IsNullOrEmpty(CORADDRESS1) ? "" : CORADDRESS1)).Direction = ParameterDirection.Input;
                _with5.Add("COR_ADDRESS_2_IN", (string.IsNullOrEmpty(CORADDRESS2) ? "" : CORADDRESS2)).Direction = ParameterDirection.Input;
                _with5.Add("COR_ADDRESS_3_IN", (string.IsNullOrEmpty(CORADDRESS3) ? "" : CORADDRESS3)).Direction = ParameterDirection.Input;
                _with5.Add("COR_CITY_IN", (string.IsNullOrEmpty(CORCITY) ? "" : CORCITY)).Direction = ParameterDirection.Input;
                _with5.Add("COR_ZIP_CODE_IN", (string.IsNullOrEmpty(CORZIPCODE) ? "" : CORZIPCODE)).Direction = ParameterDirection.Input;
                _with5.Add("COR_COUNTRY_MST_FK_IN", (CORCOUNTRY == 0 ? 0 : CORCOUNTRY)).Direction = ParameterDirection.Input;
                _with5.Add("COR_SALUTATION_IN", (CorSalutation == "" ? "" : CorSalutation)).Direction = ParameterDirection.Input;
                _with5.Add("COR_CONTACT_PERSON_IN", (string.IsNullOrEmpty(CORCONTACTPERSON) ? "" : CORCONTACTPERSON)).Direction = ParameterDirection.Input;
                _with5.Add("COR_PHONE_NO_1_IN", (string.IsNullOrEmpty(CORPHONE1) ? "" : CORPHONE1)).Direction = ParameterDirection.Input;
                _with5.Add("COR_PHONE_NO_2_IN", (string.IsNullOrEmpty(CORPHONE2) ? "" : CORPHONE2)).Direction = ParameterDirection.Input;
                _with5.Add("COR_FAX_NO_IN", (string.IsNullOrEmpty(CORFAXNO) ? "" : CORFAXNO)).Direction = ParameterDirection.Input;
                _with5.Add("COR_EMAIL_ID_IN", (string.IsNullOrEmpty(COREMAIL) ? "" : COREMAIL)).Direction = ParameterDirection.Input;
                _with5.Add("COR_URL_IN", (string.IsNullOrEmpty(CORURL) ? "" : CORURL)).Direction = ParameterDirection.Input;
                _with5.Add("COR_SHORT_NAME_IN", (string.IsNullOrEmpty(CORSHORTNAME) ? "" : CORSHORTNAME)).Direction = ParameterDirection.Input;
                _with5.Add("BILL_ADDRESS_1_IN", (string.IsNullOrEmpty(BILLADDRESS1) ? "" : BILLADDRESS1)).Direction = ParameterDirection.Input;
                _with5.Add("BILL_ADDRESS_2_IN", (string.IsNullOrEmpty(BILLADDRESS2) ? "" : BILLADDRESS2)).Direction = ParameterDirection.Input;
                _with5.Add("BILL_ADDRESS_3_IN", (string.IsNullOrEmpty(BILLADDRESS3) ? "" : BILLADDRESS3)).Direction = ParameterDirection.Input;
                _with5.Add("BILL_CITY_IN", (string.IsNullOrEmpty(BILLCITY) ? "" : BILLCITY)).Direction = ParameterDirection.Input;
                _with5.Add("BILL_ZIP_CODE_IN", (string.IsNullOrEmpty(BILLZIPCODE) ? "" : BILLZIPCODE)).Direction = ParameterDirection.Input;
                _with5.Add("BILL_COUNTRY_MST_FK_IN", (BILLCOUNTRY == 0 ? 0 : BILLCOUNTRY)).Direction = ParameterDirection.Input;

                _with5.Add("BILL_SALUTATION_IN", (BillSalutation == "" ? "" : BillSalutation)).Direction = ParameterDirection.Input;
                _with5.Add("BILL_CONTACT_PERSON_IN", (string.IsNullOrEmpty(BILLCONTACTPERSON) ? "" : BILLCONTACTPERSON)).Direction = ParameterDirection.Input;
                _with5.Add("BILL_PHONE_NO_1_IN", (string.IsNullOrEmpty(BILLPHONE1) ? "" : BILLPHONE1)).Direction = ParameterDirection.Input;
                _with5.Add("BILL_PHONE_NO_2_IN", (string.IsNullOrEmpty(BILLPHONE2) ? "" : BILLPHONE2)).Direction = ParameterDirection.Input;
                _with5.Add("BILL_FAX_NO_IN", (string.IsNullOrEmpty(BILLFAXNO) ? "" : BILLFAXNO)).Direction = ParameterDirection.Input;
                _with5.Add("BILL_EMAIL_ID_IN", (string.IsNullOrEmpty(BILLEMAIL) ? "" : BILLEMAIL)).Direction = ParameterDirection.Input;
                _with5.Add("BILL_URL_IN", (string.IsNullOrEmpty(BILLURL) ? "" : BILLURL)).Direction = ParameterDirection.Input;
                _with5.Add("BILL_SHORT_NAME_IN", (string.IsNullOrEmpty(BILLSHORTNAME) ? "" : BILLSHORTNAME)).Direction = ParameterDirection.Input;

                _with5.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                _with5.Add("LAST_MODIFIEDD_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                _with5.Add("CONFIG_PK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                _with5.Add("VERSION_NO_IN", VersionNo);
                _with5.Add("FAC_PERCENTAGE_IN", (FAC == 0.0 ? 0 : FAC)).Direction = ParameterDirection.Input;
                _with5.Add("FAC_APPLICABLE_ON_IN", (FACApplOn == "0" ? "" : FACApplOn)).Direction = ParameterDirection.Input;
                _with5.Add("CUSTOMS_CODE_IN", (string.IsNullOrEmpty(Customs_Code) ? "" : Customs_Code)).Direction = ParameterDirection.Input;
                _with5.Add("FAC_INVOICE_IN", FACInv).Direction = ParameterDirection.Input;
                _with5.Add("VESSEL_SAL_DAY", vasseldalday).Direction = ParameterDirection.Input;
                _with5.Add("INCLUDE_CALENDER_HOLIDAY", includecalanderholiday).Direction = ParameterDirection.Input;
                _with5.Add("INCLUDE_WEEK_DAY", includeweekendholiday).Direction = ParameterDirection.Input;
                _with5.Add("WEEKDAY", week_in).Direction = ParameterDirection.Input;

                _with5.Add("RETURN_VALUE", OracleDbType.Varchar2, 500, "RETURN_VALUE").Direction = ParameterDirection.Output;
                var _with6 = objWK.MyDataAdapter;
                _with6.UpdateCommand = updCommand;
                _with6.UpdateCommand.Transaction = TRAN;
                _with6.UpdateCommand.ExecuteNonQuery();
                TRAN.Commit();
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                TRAN.Rollback();
            }

            if (arrMessage.Count > 0)
            {
            }
            else
            {
                arrMessage = SaveHoliday(Holidaycan, OPERATORPkValue, Convert.ToString(M_CREATED_BY_FK), Convert.ToString(ConfigurationPK), modifyfk);
                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    arrMessage.Add("All Data Saved Successfully");
                    // arrMessage.Add(g_PKValue)
                    return arrMessage;
                }
            }
            return new ArrayList();
        }

        #endregion "Update"

        #region "Select city country locatiom"

        public string SelectCity(int fkAdmincity)
        {
            try
            {
                string strSQL = null;
                WorkFlow objWF = new WorkFlow();
                strSQL = "select CITY_NAME from CITY_MST_TBL where CITY_PK = " + fkAdmincity + " ";
                string cityAdmin = null;
                cityAdmin = objWF.ExecuteScaler(strSQL);
                return cityAdmin;
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public string SelectCountry(int fkAdmincountry)
        {
            try
            {
                string strSQL = null;
                WorkFlow objWF = new WorkFlow();
                strSQL = "select COUNTRY_NAME from COUNTRY_MST_TBL where COUNTRY_MST_PK = " + fkAdmincountry + " ";
                string cityAdmin = null;
                cityAdmin = objWF.ExecuteScaler(strSQL);
                return cityAdmin;
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public string SelectLocation(int fkLocation)
        {
            try
            {
                string strSQL = null;
                WorkFlow objWF = new WorkFlow();
                strSQL = "select LOCATION_NAME from LOCATION_MST_TBL where LOCATION_MST_PK = " + fkLocation + " ";
                string LocName = null;
                LocName = objWF.ExecuteScaler(strSQL);
                return LocName;
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Select city country locatiom"

        #region "Fetch FAC Setup"

        public DataSet FetchFACSetup(int OperatorPK, int BizType, string CostID = "", string CostName = "")
        {
            try
            {
                WorkFlow objWF = new WorkFlow();
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                if (BizType == 2)
                {
                    sb.Append(" SELECT ROWNUM SLNR, Q.* FROM ( ");
                    sb.Append(" SELECT * FROM ( ");
                    sb.Append(" SELECT FAC.FAC_SETUP_PK ,");
                    sb.Append("       FAC.OPERATOR_MST_FK ,");
                    sb.Append("       CMT.COST_ELEMENT_MST_PK ,");
                    sb.Append("       CMT.COST_ELEMENT_ID ,");
                    sb.Append("       CMT.COST_ELEMENT_NAME,");
                    sb.Append("       FAC.CURRENCY_MST_FK ,");
                    sb.Append("       CTMT.CURRENCY_ID ,");
                    sb.Append("       FAC.CHARGE_BASIS CHARGE_BASIS_FK ,");
                    //sb.Append("       FAC.CHARGE_BASIS ,")
                    sb.Append("       DECODE(FAC.CHARGE_BASIS, '0', ' ', '1', '%', '2', 'CBM', '3', 'Kgs', '4', 'Lumpsum') CHARGE_BASIS,");
                    sb.Append("       FAC.COMMISSION ,");
                    sb.Append("       '' COPY ,");
                    sb.Append("       'true' SEL ");

                    sb.Append("  FROM FAC_SETUP_TBL FAC, COST_ELEMENT_MST_TBL CMT,CURRENCY_TYPE_MST_TBL CTMT ");
                    sb.Append(" WHERE CMT.COST_ELEMENT_MST_PK = FAC.COST_ELEMENT_FK AND CTMT.CURRENCY_MST_PK(+) = FAC.CURRENCY_MST_FK ");
                    sb.Append("   AND CMT.ACTIVE_FLAG = 1  AND CMT.BUSINESS_TYPE IN(3,2) ");
                    sb.Append("   AND FAC.OPERATOR_MST_FK = " + OperatorPK);
                    sb.Append(" UNION ");
                    sb.Append(" SELECT 0 FAC_SETUP_PK ,");
                    sb.Append("       0 OPERATOR_MST_FK ,");
                    sb.Append("       CMT.COST_ELEMENT_MST_PK ,");
                    sb.Append("       CMT.COST_ELEMENT_ID,");
                    sb.Append("       CMT.COST_ELEMENT_NAME,");
                    sb.Append("       0 CURRENCY_MST_FK ,");
                    sb.Append("       NULL CURRENCY_ID ,");
                    sb.Append("       NULL CHARGE_BASIS_FK ,");
                    sb.Append("       NULL CHARGE_BASIS ,");
                    sb.Append("       NULL COMMISSION,");
                    sb.Append("       '' COPY ,");
                    sb.Append("       'false' SEL ");
                    sb.Append("  FROM COST_ELEMENT_MST_TBL CMT ");
                    sb.Append(" WHERE CMT.ACTIVE_FLAG = 1");
                    sb.Append("   AND CMT.BUSINESS_TYPE IN(3,2) ");
                    sb.Append("   AND CMT.COST_ELEMENT_MST_PK NOT IN");
                    sb.Append("       (SELECT F.COST_ELEMENT_FK");
                    sb.Append("          FROM FAC_SETUP_TBL F");
                    sb.Append("         WHERE F.OPERATOR_MST_FK = " + OperatorPK + ")) ");
                    sb.Append(" WHERE 1 = 1");
                    if (!string.IsNullOrEmpty(CostID))
                    {
                        sb.Append(" AND UPPER(COST_ELEMENT_ID) LIKE '" + CostID.ToUpper().Replace("'", "''") + "%' ");
                    }
                    if (!string.IsNullOrEmpty(CostName))
                    {
                        sb.Append(" AND UPPER(COST_ELEMENT_NAME) LIKE '" + CostName.ToUpper().Replace("'", "''") + "%' ");
                    }
                    sb.Append("  ORDER BY COST_ELEMENT_ID ) Q ");
                }
                else
                {
                    sb.Append(" SELECT ROWNUM SLNR, Q.* FROM ( ");
                    sb.Append(" SELECT * FROM ( ");
                    sb.Append(" SELECT FAC.FAC_SETUP_PK ,");
                    sb.Append("       FAC.AIRLINE_MST_FK OPERATOR_MST_FK ,");
                    sb.Append("       CMT.COST_ELEMENT_MST_PK ,");
                    sb.Append("       CMT.COST_ELEMENT_ID ,");
                    sb.Append("       CMT.COST_ELEMENT_NAME,");
                    sb.Append("       FAC.CURRENCY_MST_FK ,");
                    sb.Append("       CTMT.CURRENCY_ID ,");
                    sb.Append("       FAC.CHARGE_BASIS CHARGE_BASIS_FK ,");
                    // sb.Append("       FAC.CHARGE_BASIS ,")
                    sb.Append("       DECODE(FAC.CHARGE_BASIS, '0', ' ', '1', '%', '2', 'CBM', '3', 'Kgs','4', 'Lumpsum') CHARGE_BASIS,");
                    sb.Append("       FAC.COMMISSION,");
                    sb.Append("       '' COPY ,");
                    sb.Append("       'true' SEL ");
                    sb.Append("  FROM FAC_SETUP_TBL FAC, COST_ELEMENT_MST_TBL CMT,CURRENCY_TYPE_MST_TBL CTMT ");
                    sb.Append(" WHERE CMT.COST_ELEMENT_MST_PK = FAC.COST_ELEMENT_FK AND CTMT.CURRENCY_MST_PK(+) = FAC.CURRENCY_MST_FK  ");
                    sb.Append("   AND CMT.ACTIVE_FLAG = 1  AND CMT.BUSINESS_TYPE IN(3,1) ");
                    sb.Append("   AND FAC.AIRLINE_MST_FK = " + OperatorPK);
                    sb.Append(" UNION ");
                    sb.Append(" SELECT 0 FAC_SETUP_PK ,");
                    sb.Append("       0 OPERATOR_MST_FK ,");
                    sb.Append("       CMT.COST_ELEMENT_MST_PK ,");
                    sb.Append("       CMT.COST_ELEMENT_ID,");
                    sb.Append("       CMT.COST_ELEMENT_NAME,");
                    sb.Append("       0 CURRENCY_MST_FK ,");
                    sb.Append("       NULL CURRENCY_ID ,");
                    sb.Append("       0 CHARGE_BASIS_FK ,");
                    sb.Append("       NULL CHARGE_BASIS ,");
                    sb.Append("       NULL COMMISSION,");
                    sb.Append("       '' COPY,");
                    sb.Append("       'false' SEL ");
                    sb.Append("  FROM COST_ELEMENT_MST_TBL CMT ");
                    sb.Append(" WHERE CMT.ACTIVE_FLAG = 1");
                    sb.Append("   AND CMT.BUSINESS_TYPE IN(3,1) ");
                    sb.Append("   AND CMT.COST_ELEMENT_MST_PK NOT IN");
                    sb.Append("       (SELECT F.COST_ELEMENT_FK");
                    sb.Append("          FROM FAC_SETUP_TBL F");
                    sb.Append("         WHERE F.AIRLINE_MST_FK = " + OperatorPK + ")) ");
                    sb.Append(" WHERE 1 = 1");
                    if (!string.IsNullOrEmpty(CostID))
                    {
                        sb.Append(" AND UPPER(COST_ELEMENT_ID) LIKE '" + CostID.ToUpper().Replace("'", "''") + "%' ");
                    }
                    if (!string.IsNullOrEmpty(CostName))
                    {
                        sb.Append(" AND UPPER(COST_ELEMENT_NAME) LIKE '" + CostName.ToUpper().Replace("'", "''") + "%' ");
                    }
                    sb.Append("  ORDER BY COST_ELEMENT_ID ) Q ");
                }

                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch FAC Setup"

        #region "Fetch FAC Setup"

        public DataSet CopyFAC(string CostPK, int BizType)
        {
            try
            {
                WorkFlow objWF = new WorkFlow();
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                string strSQL = null;
                sb.Append("SELECT ");
                sb.Append("       C.COST_ELEMENT_MST_PK,");
                sb.Append("       C.COST_ELEMENT_ID,");
                sb.Append("       C.COST_ELEMENT_NAME,");
                sb.Append("       '' SEL ");
                sb.Append("  FROM COST_ELEMENT_MST_TBL C");
                sb.Append(" WHERE C.BUSINESS_TYPE IN (3, " + BizType + ")");
                sb.Append("   AND C.ACTIVE_FLAG = 1");
                sb.Append("   AND C.COST_ELEMENT_MST_PK NOT IN (" + CostPK + ") ");
                sb.Append(" ORDER BY C.COST_ELEMENT_ID");
                strSQL = " SELECT * FROM (SELECT ROWNUM SR_NO, q.* FROM(";
                strSQL += sb.ToString();
                strSQL += " )q ) ";
                return objWF.GetDataSet(strSQL);
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch FAC Setup"

        #region "Save Surcharge PopUp"

        public ArrayList SaveFACPopUp(DataSet M_DataSet, int BizType, int OPRPK = 0)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            Int32 RecAfct = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            OracleCommand delCommand = new OracleCommand();
            arrMessage.Clear();
            try
            {
                if (BizType == 2)
                {
                    if (M_DataSet.Tables[0].Rows.Count > 0)
                    {
                        DeleteFAC(Convert.ToInt64(M_DataSet.Tables[0].Rows[0]["OPERATOR_MST_FK"]), BizType);
                    }
                    else
                    {
                        DeleteFAC(OPRPK, BizType);
                    }
                    var _with7 = insCommand;
                    _with7.Connection = objWK.MyConnection;
                    _with7.CommandType = CommandType.StoredProcedure;
                    _with7.CommandText = objWK.MyUserName + ".FAC_SETUP_TBL_PKG.FAC_SETUP_TBL_INS";
                    var _with8 = _with7.Parameters;

                    insCommand.CommandText = objWK.MyUserName + ".FAC_SETUP_TBL_PKG.FAC_SETUP_TBL_INS";

                    insCommand.Parameters.Add("COST_ELEMENT_FK_IN", OracleDbType.Int32, 10, "COST_ELEMENT_MST_PK").Direction = ParameterDirection.Input;
                    insCommand.Parameters["COST_ELEMENT_FK_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("OPERATOR_MST_FK_IN", OracleDbType.Int32, 2, "OPERATOR_MST_FK").Direction = ParameterDirection.Input;
                    insCommand.Parameters["OPERATOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("COMMISSION_IN", OracleDbType.Int32, 10, "COMMISSION").Direction = ParameterDirection.Input;
                    insCommand.Parameters["COMMISSION_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;

                    //insCommand.Parameters.Add("CHARGE_BASIS_FK_IN", OracleClient.OracleDbType.Int32, 10, "CHARGE_BASIS_FK").Direction = ParameterDirection.Input
                    //insCommand.Parameters["CHARGE_BASIS_FK_IN"].SourceVersion = DataRowVersion.Current

                    insCommand.Parameters.Add("CHARGE_BASIS_IN", OracleDbType.Int32, 10, "CHARGE_BASIS_FK").Direction = ParameterDirection.Input;
                    insCommand.Parameters["CHARGE_BASIS_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
                    insCommand.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    //   insCommand.Parameters.Add("CURRENCY_ID_IN", OracleClient.OracleDbType.NVarchar2, 10, "CURRENCY_ID").Direction = ParameterDirection.Input
                    //insCommand.Parameters["CURRENCY_ID_IN"].SourceVersion = DataRowVersion.Current

                    insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "FAC_SETUP_PK").Direction = ParameterDirection.Output;
                    insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with9 = objWK.MyDataAdapter;
                    _with9.InsertCommand = insCommand;
                    _with9.InsertCommand.Transaction = TRAN;
                    RecAfct = _with9.Update(M_DataSet);
                    if (arrMessage.Count > 0)
                    {
                        TRAN.Rollback();
                        return arrMessage;
                    }
                    else
                    {
                        TRAN.Commit();
                        arrMessage.Add("All Data Saved Successfully");
                        return arrMessage;
                    }
                }
                else
                {
                    if (M_DataSet.Tables[0].Rows.Count > 0)
                    {
                        DeleteFAC(Convert.ToInt64(M_DataSet.Tables[0].Rows[0]["OPERATOR_MST_FK"]), BizType);
                    }
                    var _with10 = insCommand;
                    _with10.Connection = objWK.MyConnection;
                    _with10.CommandType = CommandType.StoredProcedure;
                    _with10.CommandText = objWK.MyUserName + ".FAC_SETUP_TBL_PKG.FAC_SETUP_TBL_INS";
                    var _with11 = _with10.Parameters;

                    insCommand.CommandText = objWK.MyUserName + ".FAC_SETUP_TBL_PKG.FAC_SETUP_TBL_INS";

                    insCommand.Parameters.Add("COST_ELEMENT_FK_IN", OracleDbType.Int32, 10, "COST_ELEMENT_MST_PK").Direction = ParameterDirection.Input;
                    insCommand.Parameters["COST_ELEMENT_FK_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("OPERATOR_MST_FK_IN", OracleDbType.Int32, 2, "OPERATOR_MST_FK").Direction = ParameterDirection.Input;
                    insCommand.Parameters["OPERATOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("COMMISSION_IN", OracleDbType.Int32, 10, "COMMISSION").Direction = ParameterDirection.Input;
                    insCommand.Parameters["COMMISSION_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;

                    //insCommand.Parameters.Add("CHARGE_BASIS_FK_IN", OracleClient.OracleDbType.Int32, 10, "CHARGE_BASIS_FK").Direction = ParameterDirection.Input
                    //insCommand.Parameters["CHARGE_BASIS_FK_IN"].SourceVersion = DataRowVersion.Current

                    insCommand.Parameters.Add("CHARGE_BASIS_IN", OracleDbType.Int32, 10, "CHARGE_BASIS_FK").Direction = ParameterDirection.Input;
                    insCommand.Parameters["CHARGE_BASIS_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
                    insCommand.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    //insCommand.Parameters.Add("CURRENCY_ID_IN", OracleClient.OracleDbType.NVarchar2, 10, "CURRENCY_ID").Direction = ParameterDirection.Input
                    //insCommand.Parameters["CURRENCY_ID_IN"].SourceVersion = DataRowVersion.Current

                    insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "FAC_SETUP_PK").Direction = ParameterDirection.Output;
                    insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with12 = objWK.MyDataAdapter;
                    _with12.InsertCommand = insCommand;
                    _with12.InsertCommand.Transaction = TRAN;
                    RecAfct = _with12.Update(M_DataSet);
                    if (arrMessage.Count > 0)
                    {
                        TRAN.Rollback();
                        return arrMessage;
                    }
                    else
                    {
                        TRAN.Commit();
                        arrMessage.Add("All Data Saved Successfully");
                        return arrMessage;
                    }
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

        public string DeleteFAC(long OperatorPK, long BizType)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            System.Text.StringBuilder sb1 = new System.Text.StringBuilder(5000);
            int RcdCnt = 0;
            try
            {
                if (BizType == 2)
                {
                    sb.Append("SELECT COUNT(*)");
                    sb.Append("      FROM FAC_SETUP_TBL F");
                    sb.Append("     WHERE F.OPERATOR_MST_FK = " + OperatorPK);

                    RcdCnt = Convert.ToInt32(Objwk.ExecuteScaler(sb.ToString()));

                    if (RcdCnt > 0)
                    {
                        sb1.Append(" DELETE FROM FAC_SETUP_TBL F ");
                        sb1.Append(" WHERE F.OPERATOR_MST_FK =" + OperatorPK);
                        Objwk.ExecuteCommands(sb1.ToString());
                    }
                }
                else
                {
                    sb.Append("SELECT COUNT(*)");
                    sb.Append("      FROM FAC_SETUP_TBL F");
                    sb.Append("     WHERE F.AIRLINE_MST_FK = " + OperatorPK);

                    RcdCnt = Convert.ToInt32(Objwk.ExecuteScaler(sb.ToString()));

                    if (RcdCnt > 0)
                    {
                        sb1.Append(" DELETE FROM FAC_SETUP_TBL F ");
                        sb1.Append(" WHERE F.AIRLINE_MST_FK =" + OperatorPK);
                        Objwk.ExecuteCommands(sb1.ToString());
                    }
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
            return "";
        }

        #endregion "Save Surcharge PopUp"

        #region "SaveHoliday"

        public ArrayList SaveHoliday(DataSet M_DataSet, int Detention_HDR_FK_IN, string create, string config, int modifyfk = 0)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            OracleCommand insCommand = new OracleCommand();

            try
            {
                int ROWCNT = 0;
                int standardFrtPk = 0;
                Int32 RowCntContainer = 0;
                var _with13 = insCommand;

                for (ROWCNT = 0; ROWCNT <= M_DataSet.Tables[0].Rows.Count - 1; ROWCNT++)
                {
                    if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[ROWCNT]["Holipk"].ToString()))
                    {
                        _with13.Connection = objWK.MyConnection;
                        _with13.CommandType = CommandType.StoredProcedure;
                        _with13.CommandText = objWK.MyUserName + ".OPERATOR_CALENDER_PKG.OPERATOR_CALENDER_INS";
                        var _with14 = _with13.Parameters;
                        _with14.Clear();
                        _with14.Add("OPERATOR_MST_FK_IN", Detention_HDR_FK_IN).Direction = ParameterDirection.Input;
                        System.DateTime dt = Convert.ToDateTime(M_DataSet.Tables[0].Rows[ROWCNT]["Holiday_date"]);
                        _with14.Add("HOLIDAY_DATE_IN", dt.Date).Direction = ParameterDirection.Input;
                        _with14.Add("HOLIDAY_DESC_IN", M_DataSet.Tables[0].Rows[ROWCNT]["Holiday_desc"]).Direction = ParameterDirection.Input;
                        _with14.Add("CREATED_BY_FK_IN", create).Direction = ParameterDirection.Input;
                        _with14.Add("CONFIG_MST_FK_IN", config).Direction = ParameterDirection.Input;
                        _with14.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                        try
                        {
                            var _with15 = objWK.MyDataAdapter;
                            _with15.InsertCommand = insCommand;
                            _with15.InsertCommand.Transaction = TRAN;
                            _with15.InsertCommand.ExecuteNonQuery();
                        }
                        catch (OracleException oraexp)
                        {
                            arrMessage.Add(oraexp.Message);
                        }
                        catch (Exception ex)
                        {
                            arrMessage.Add(ex.Message);
                        }
                    }
                    else
                    {
                        _with13.Connection = objWK.MyConnection;
                        _with13.CommandType = CommandType.StoredProcedure;
                        _with13.CommandText = objWK.MyUserName + ".OPERATOR_CALENDER_PKG.OPERATOR_CALENDER_UPD";
                        var _with16 = _with13.Parameters;
                        _with16.Clear();

                        _with16.Add("OPERATOR_CALENDER_PK_IN", M_DataSet.Tables[0].Rows[ROWCNT]["Holipk"]).Direction = ParameterDirection.Input;
                        _with16.Add("OPERATOR_MST_FK_IN", Detention_HDR_FK_IN).Direction = ParameterDirection.Input;
                        System.DateTime dt = Convert.ToDateTime(M_DataSet.Tables[0].Rows[ROWCNT]["Holiday_date"]);
                        _with16.Add("HOLIDAY_DATE_IN", dt.Date).Direction = ParameterDirection.Input;
                        _with16.Add("HOLIDAY_DESC_IN", M_DataSet.Tables[0].Rows[ROWCNT]["Holiday_desc"]).Direction = ParameterDirection.Input;
                        _with16.Add("LAST_MODIFIED_BY_FK_IN", modifyfk).Direction = ParameterDirection.Input;
                        _with16.Add("CONFIG_MST_FK_IN", config).Direction = ParameterDirection.Input;
                        _with16.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "DET_SLAB_TRN_PK").Direction = ParameterDirection.Output;
                        insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                        try
                        {
                            var _with17 = objWK.MyDataAdapter;
                            _with17.InsertCommand = insCommand;
                            _with17.InsertCommand.Transaction = TRAN;
                            _with17.InsertCommand.ExecuteNonQuery();
                        }
                        catch (OracleException oraexp)
                        {
                            arrMessage.Add(oraexp.Message);
                        }
                        catch (Exception ex)
                        {
                            arrMessage.Add(ex.Message);
                        }
                    }
                }
                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    TRAN.Commit();
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
                throw ex;
            }
        }

        #endregion "SaveHoliday"

        #region "Fetch holiday"

        public DataSet Fetchholiday(string OPERATORID = "", Int32 CurrentPage = 1, Int32 TotalPage = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);

            WorkFlow objWF = new WorkFlow();
            strCondition = "AND tt.operator_mst_fk= '" + OPERATORID + "'";
            strCondition += ")";
            strSQL = "select";
            strSQL = strSQL + "tt.holiday_date as date_holiday, ";
            strSQL = strSQL + "tt.holiday_desc as holiday_des,";
            strSQL = strSQL + "'' selection,";
            strSQL = strSQL + "tt.operator_calender_pk";
            strSQL = strSQL + " FROM OPERATOR_CALENDER_TBL TT ";
            strSQL = strSQL + "WHERE ( 1 = 1) ";
            strSQL = strSQL + strCondition;

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*)  from  ");
            strCount.Append((" (" + strSQL.ToString() + ""));
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
            strCount.Remove(0, strCount.Length);

            System.Text.StringBuilder sqlstr2 = new System.Text.StringBuilder();
            sqlstr2.Append(" Select * from ");
            sqlstr2.Append("  ( Select ROWNUM SR_NO, q.* from ");
            sqlstr2.Append("  (" + strSQL.ToString() + " ");
            sqlstr2.Append("  q )    WHERE \"SR_NO\"  BETWEEN " + start + " AND " + last + "");
            DataSet DS = null;

            DS = objWF.GetDataSet(sqlstr2.ToString());

            try
            {
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

        #endregion "Fetch holiday"
    }
}