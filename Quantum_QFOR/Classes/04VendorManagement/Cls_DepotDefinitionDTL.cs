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
    public class ClsDepotDefinitionDTL : CommonFeatures
    {
        #region " Fetch One "

        /// <summary>
        /// Fetches the one.
        /// </summary>
        /// <param name="depotPK">The depot pk.</param>
        /// <returns></returns>
        public DataSet FetchOne(string depotPK = "")
        {
            bool NewRecord = true;
            if (depotPK.Trim().Length > 0)
                NewRecord = false;

            string strSQL = null;
            string strCondition = null;
            WorkFlow objWF = new WorkFlow();

            strSQL = "Select ";
            strSQL += " DEPOT_MST_PK,";
            strSQL += " DEPOT_ID,";
            strSQL += " DEPOT_NAME,";
            strSQL += " dm.VERSION_NO VERSION_NO,";
            strSQL += " BUSINESS_TYPE,";
            strSQL += " ACCOUNT_NO,";
            strSQL += " VAT_NO,";
            strSQL += " NVL(dm.ACTIVE_FLAG,0) ACTIVE_FLAG,";
            strSQL += " LOCATION_MST_FK LOC_FK, ";
            strSQL += " L.LOCATION_NAME LOC_NAME, ";
            strSQL += " L.LOCATION_ID LOC_ID, ";
            strSQL += " ADM_ADDRESS_1,         ADM_ADDRESS_2,      ADM_ADDRESS_3,";
            strSQL += " ADM_CITY,              ADM_ZIP_CODE,       ADM_COUNTRY_MST_FK,";
            strSQL += " ADM_CN.COUNTRY_ID     ADM_COUNTRY_ID,";
            strSQL += " ADM_CN.COUNTRY_NAME   ADM_COUNTRY_NAME,";
            strSQL += " ADM_SALUTATION,        ADM_CONTACT_PERSON,";
            strSQL += " ADM_PHONE_NO_1,        ADM_PHONE_NO_2,";
            strSQL += " ADM_FAX_NO,            ADM_EMAIL_ID,       ADM_URL,";
            strSQL += " COR_ADDRESS_1,         COR_ADDRESS_2,      COR_ADDRESS_3,";
            strSQL += " COR_CITY,              COR_ZIP_CODE,       COR_COUNTRY_MST_FK,";
            strSQL += " COR_CN.COUNTRY_ID     COR_COUNTRY_ID,";
            strSQL += " COR_CN.COUNTRY_NAME   COR_COUNTRY_NAME,";
            strSQL += " COR_SALUTATION,        COR_CONTACT_PERSON,";
            strSQL += " COR_PHONE_NO_1,        COR_PHONE_NO_2,";
            strSQL += " COR_FAX_NO,            COR_EMAIL_ID,       COR_URL,";
            strSQL += " BILL_ADDRESS_1,        BILL_ADDRESS_2,     BILL_ADDRESS_3,";
            strSQL += " BILL_CITY,             BILL_ZIP_CODE,      BILL_COUNTRY_MST_FK,";
            strSQL += " BILL_CN.COUNTRY_ID    BILL_COUNTRY_ID,";
            strSQL += " BILL_CN.COUNTRY_NAME  BILL_COUNTRY_NAME,";
            strSQL += " BILL_SALUTATION,       BILL_CONTACT_PERSON,";
            strSQL += " BILL_PHONE_NO_1,       BILL_PHONE_NO_2,";
            strSQL += " BILL_FAX_NO,           BILL_EMAIL_ID,      BILL_URL,";
            strSQL += " dm.remarks ";
            strSQL += " from ";
            strSQL += " DEPOT_MST_TBL dm,";
            strSQL += " DEPOT_CONTACT_DTLS dc,";
            strSQL += " COUNTRY_MST_TBL ADM_CN,";
            strSQL += " COUNTRY_MST_TBL COR_CN,";
            strSQL += " COUNTRY_MST_TBL BILL_CN, ";
            strSQL += " LOCATION_MST_TBL L ";
            strCondition = " Where ";
            strCondition += " dm.DEPOT_MST_PK = dc.DEPOT_MST_FK and ";
            strCondition += " dc.ADM_COUNTRY_MST_FK = ADM_CN.COUNTRY_MST_PK and ";
            strCondition += " dc.COR_COUNTRY_MST_FK = COR_CN.COUNTRY_MST_PK(+) and ";
            strCondition += " dc.BILL_COUNTRY_MST_FK = BILL_CN.COUNTRY_MST_PK(+) and ";
            strCondition += " L.LOCATION_MST_PK = DM.LOCATION_MST_FK AND ";
            if (NewRecord == false)
            {
                strCondition += " dm.DEPOT_MST_PK = " + depotPK + " ";
            }
            else
            {
                strCondition += " 1 = 2 ";
            }
            strSQL += strCondition;
            DataSet ds = null;
            try
            {
                ds = objWF.GetDataSet(strSQL);
                if (NewRecord == true)
                {
                    ds.Tables[0].Rows.Add(ds.Tables[0].NewRow());
                    ds.Tables[0].Rows[0]["DEPOT_MST_PK"] = 0;
                    ds.Tables[0].Rows[0]["ACTIVE_FLAG"] = 1;
                }
                return ds;
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

        #endregion " Fetch One "

        #region " Save "

        /// <summary>
        /// Saves the specified m_ data set.
        /// </summary>
        /// <param name="M_DataSet">The m_ data set.</param>
        /// <returns></returns>
        public ArrayList Save(DataSet M_DataSet)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();

            string retVal = null;

            Int32 RecAfct = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            OracleCommand delCommand = new OracleCommand();

            string INS_Proc = null;
            string DEL_Proc = null;
            string UPD_Proc = null;
            string UserName = objWK.MyUserName;
            INS_Proc = UserName + ".DEPOT_MST_TBL_PKG.DEPOT_MST_TBL_INS";
            DEL_Proc = UserName + ".DEPOT_MST_TBL_PKG.DEPOT_MST_TBL_DEL";
            UPD_Proc = UserName + ".DEPOT_MST_TBL_PKG.DEPOT_MST_TBL_UPD";
            try
            {
                var _with1 = insCommand;
                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = INS_Proc;
                _with1.Parameters.Add("DEPOT_ID_IN", OracleDbType.Varchar2, 20, "DEPOT_ID").Direction = ParameterDirection.Input;
                _with1.Parameters["DEPOT_ID_IN"].SourceVersion = DataRowVersion.Current;
                _with1.Parameters.Add("DEPOT_NAME_IN", OracleDbType.Varchar2, 50, "DEPOT_NAME").Direction = ParameterDirection.Input;
                _with1.Parameters["DEPOT_NAME_IN"].SourceVersion = DataRowVersion.Current;
                _with1.Parameters.Add("BUSINESS_TYPE_IN", OracleDbType.Int32, 1, "BUSINESS_TYPE").Direction = ParameterDirection.Input;
                _with1.Parameters["BUSINESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;
                _with1.Parameters.Add("ACCOUNT_NO_IN", OracleDbType.Varchar2, 15, "ACCOUNT_NO").Direction = ParameterDirection.Input;
                _with1.Parameters["ACCOUNT_NO_IN"].SourceVersion = DataRowVersion.Current;
                _with1.Parameters.Add("VAT_NO_IN", OracleDbType.Varchar2, 11, "VAT_NO").Direction = ParameterDirection.Input;
                _with1.Parameters["VAT_NO_IN"].SourceVersion = DataRowVersion.Current;
                _with1.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
                _with1.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;

                _with1.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "LOC_FK").Direction = ParameterDirection.Input;
                _with1.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with1.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;

                _with1.Parameters.Add("ADM_ADDRESS_1_IN", OracleDbType.Varchar2, 50, "ADM_ADDRESS_1").Direction = ParameterDirection.Input;
                _with1.Parameters["ADM_ADDRESS_1_IN"].SourceVersion = DataRowVersion.Current;
                _with1.Parameters.Add("ADM_ADDRESS_2_IN", OracleDbType.Varchar2, 50, "ADM_ADDRESS_2").Direction = ParameterDirection.Input;
                _with1.Parameters["ADM_ADDRESS_2_IN"].SourceVersion = DataRowVersion.Current;
                _with1.Parameters.Add("ADM_ADDRESS_3_IN", OracleDbType.Varchar2, 50, "ADM_ADDRESS_3").Direction = ParameterDirection.Input;
                _with1.Parameters["ADM_ADDRESS_3_IN"].SourceVersion = DataRowVersion.Current;
                _with1.Parameters.Add("ADM_CITY_IN", OracleDbType.Varchar2, 50, "ADM_CITY").Direction = ParameterDirection.Input;
                _with1.Parameters["ADM_CITY_IN"].SourceVersion = DataRowVersion.Current;
                _with1.Parameters.Add("ADM_ZIP_CODE_IN", OracleDbType.Varchar2, 10, "ADM_ZIP_CODE").Direction = ParameterDirection.Input;
                _with1.Parameters["ADM_ZIP_CODE_IN"].SourceVersion = DataRowVersion.Current;
                _with1.Parameters.Add("ADM_COUNTRY_MST_FK_IN", OracleDbType.Int32, 10, "ADM_COUNTRY_MST_FK").Direction = ParameterDirection.Input;
                _with1.Parameters["ADM_COUNTRY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with1.Parameters.Add("ADM_SALUTATION_IN", OracleDbType.Int32, 1, "ADM_SALUTATION").Direction = ParameterDirection.Input;
                _with1.Parameters["ADM_SALUTATION_IN"].SourceVersion = DataRowVersion.Current;
                _with1.Parameters.Add("ADM_CONTACT_PERSON_IN", OracleDbType.Varchar2, 50, "ADM_CONTACT_PERSON").Direction = ParameterDirection.Input;
                _with1.Parameters["ADM_CONTACT_PERSON_IN"].SourceVersion = DataRowVersion.Current;
                // ADM_PHONE_NO_1
                _with1.Parameters.Add("ADM_PHONE_NO_1_IN", OracleDbType.Varchar2, 25, "ADM_PHONE_NO_1").Direction = ParameterDirection.Input;
                _with1.Parameters["ADM_PHONE_NO_1_IN"].SourceVersion = DataRowVersion.Current;
                // ADM_PHONE_NO_2
                _with1.Parameters.Add("ADM_PHONE_NO_2_IN", OracleDbType.Varchar2, 25, "ADM_PHONE_NO_2").Direction = ParameterDirection.Input;
                _with1.Parameters["ADM_PHONE_NO_2_IN"].SourceVersion = DataRowVersion.Current;
                // ADM_FAX_NO
                _with1.Parameters.Add("ADM_FAX_NO_IN", OracleDbType.Varchar2, 25, "ADM_FAX_NO").Direction = ParameterDirection.Input;
                _with1.Parameters["ADM_FAX_NO_IN"].SourceVersion = DataRowVersion.Current;
                // ADM_EMAIL_ID
                _with1.Parameters.Add("ADM_EMAIL_ID_IN", OracleDbType.Varchar2, 50, "ADM_EMAIL_ID").Direction = ParameterDirection.Input;
                _with1.Parameters["ADM_EMAIL_ID_IN"].SourceVersion = DataRowVersion.Current;
                // ADM_URL
                _with1.Parameters.Add("ADM_URL_IN", OracleDbType.Varchar2, 100, "ADM_URL").Direction = ParameterDirection.Input;
                _with1.Parameters["ADM_URL_IN"].SourceVersion = DataRowVersion.Current;
                //

                // REmarks ----added by sumi
                _with1.Parameters.Add("REMARKS_IN", OracleDbType.Varchar2, 500, "REMARKS").Direction = ParameterDirection.Input;
                _with1.Parameters["REMARKS_IN"].SourceVersion = DataRowVersion.Current;
                // end sumi

                // Correspondence Address
                //
                // COR_ADDRESS_1
                _with1.Parameters.Add("COR_ADDRESS_1_IN", OracleDbType.Varchar2, 50, "COR_ADDRESS_1").Direction = ParameterDirection.Input;
                _with1.Parameters["COR_ADDRESS_1_IN"].SourceVersion = DataRowVersion.Current;
                // COR_ADDRESS_2
                _with1.Parameters.Add("COR_ADDRESS_2_IN", OracleDbType.Varchar2, 50, "COR_ADDRESS_2").Direction = ParameterDirection.Input;
                _with1.Parameters["COR_ADDRESS_2_IN"].SourceVersion = DataRowVersion.Current;
                // COR_ADDRESS_3
                _with1.Parameters.Add("COR_ADDRESS_3_IN", OracleDbType.Varchar2, 50, "COR_ADDRESS_3").Direction = ParameterDirection.Input;
                _with1.Parameters["COR_ADDRESS_3_IN"].SourceVersion = DataRowVersion.Current;
                // COR_CITY
                _with1.Parameters.Add("COR_CITY_IN", OracleDbType.Varchar2, 50, "COR_CITY").Direction = ParameterDirection.Input;
                _with1.Parameters["COR_CITY_IN"].SourceVersion = DataRowVersion.Current;
                // COR_ZIP_CODE
                _with1.Parameters.Add("COR_ZIP_CODE_IN", OracleDbType.Varchar2, 10, "COR_ZIP_CODE").Direction = ParameterDirection.Input;
                _with1.Parameters["COR_ZIP_CODE_IN"].SourceVersion = DataRowVersion.Current;
                // COR_COUNTRY_MST_FK
                _with1.Parameters.Add("COR_COUNTRY_MST_FK_IN", OracleDbType.Int32, 10, "COR_COUNTRY_MST_FK").Direction = ParameterDirection.Input;
                _with1.Parameters["COR_COUNTRY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                // COR_SALUTATION
                _with1.Parameters.Add("COR_SALUTATION_IN", OracleDbType.Int32, 1, "COR_SALUTATION").Direction = ParameterDirection.Input;
                _with1.Parameters["COR_SALUTATION_IN"].SourceVersion = DataRowVersion.Current;
                // COR_CONTACT_PERSON
                _with1.Parameters.Add("COR_CONTACT_PERSON_IN", OracleDbType.Varchar2, 50, "COR_CONTACT_PERSON").Direction = ParameterDirection.Input;
                _with1.Parameters["COR_CONTACT_PERSON_IN"].SourceVersion = DataRowVersion.Current;
                // COR_PHONE_NO_1
                _with1.Parameters.Add("COR_PHONE_NO_1_IN", OracleDbType.Varchar2, 25, "COR_PHONE_NO_1").Direction = ParameterDirection.Input;
                _with1.Parameters["COR_PHONE_NO_1_IN"].SourceVersion = DataRowVersion.Current;
                // COR_PHONE_NO_2
                _with1.Parameters.Add("COR_PHONE_NO_2_IN", OracleDbType.Varchar2, 25, "COR_PHONE_NO_2").Direction = ParameterDirection.Input;
                _with1.Parameters["COR_PHONE_NO_2_IN"].SourceVersion = DataRowVersion.Current;
                // COR_FAX_NO
                _with1.Parameters.Add("COR_FAX_NO_IN", OracleDbType.Varchar2, 25, "COR_FAX_NO").Direction = ParameterDirection.Input;
                _with1.Parameters["COR_FAX_NO_IN"].SourceVersion = DataRowVersion.Current;
                // COR_EMAIL_ID
                _with1.Parameters.Add("COR_EMAIL_ID_IN", OracleDbType.Varchar2, 50, "COR_EMAIL_ID").Direction = ParameterDirection.Input;
                _with1.Parameters["COR_EMAIL_ID_IN"].SourceVersion = DataRowVersion.Current;
                // COR_URL
                _with1.Parameters.Add("COR_URL_IN", OracleDbType.Varchar2, 100, "COR_URL").Direction = ParameterDirection.Input;
                _with1.Parameters["COR_URL_IN"].SourceVersion = DataRowVersion.Current;
                //
                // Billing Address
                //
                // BILL_ADDRESS_1
                _with1.Parameters.Add("BILL_ADDRESS_1_IN", OracleDbType.Varchar2, 50, "BILL_ADDRESS_1").Direction = ParameterDirection.Input;
                _with1.Parameters["BILL_ADDRESS_1_IN"].SourceVersion = DataRowVersion.Current;
                // BILL_ADDRESS_2
                _with1.Parameters.Add("BILL_ADDRESS_2_IN", OracleDbType.Varchar2, 50, "BILL_ADDRESS_2").Direction = ParameterDirection.Input;
                _with1.Parameters["BILL_ADDRESS_2_IN"].SourceVersion = DataRowVersion.Current;
                // BILL_ADDRESS_3
                _with1.Parameters.Add("BILL_ADDRESS_3_IN", OracleDbType.Varchar2, 50, "BILL_ADDRESS_3").Direction = ParameterDirection.Input;
                _with1.Parameters["BILL_ADDRESS_3_IN"].SourceVersion = DataRowVersion.Current;
                // BILL_CITY
                _with1.Parameters.Add("BILL_CITY_IN", OracleDbType.Varchar2, 50, "BILL_CITY").Direction = ParameterDirection.Input;
                _with1.Parameters["BILL_CITY_IN"].SourceVersion = DataRowVersion.Current;
                // BILL_ZIP_CODE
                _with1.Parameters.Add("BILL_ZIP_CODE_IN", OracleDbType.Varchar2, 10, "BILL_ZIP_CODE").Direction = ParameterDirection.Input;
                _with1.Parameters["BILL_ZIP_CODE_IN"].SourceVersion = DataRowVersion.Current;
                // BILL_COUNTRY_MST_FK
                _with1.Parameters.Add("BILL_COUNTRY_MST_FK_IN", OracleDbType.Int32, 10, "BILL_COUNTRY_MST_FK").Direction = ParameterDirection.Input;
                _with1.Parameters["BILL_COUNTRY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                // BILL_SALUTATION
                _with1.Parameters.Add("BILL_SALUTATION_IN", OracleDbType.Int32, 1, "BILL_SALUTATION").Direction = ParameterDirection.Input;
                _with1.Parameters["BILL_SALUTATION_IN"].SourceVersion = DataRowVersion.Current;
                // BILL_CONTACT_PERSON
                _with1.Parameters.Add("BILL_CONTACT_PERSON_IN", OracleDbType.Varchar2, 50, "BILL_CONTACT_PERSON").Direction = ParameterDirection.Input;
                _with1.Parameters["BILL_CONTACT_PERSON_IN"].SourceVersion = DataRowVersion.Current;
                // BILL_PHONE_NO_1
                _with1.Parameters.Add("BILL_PHONE_NO_1_IN", OracleDbType.Varchar2, 25, "BILL_PHONE_NO_1").Direction = ParameterDirection.Input;
                _with1.Parameters["BILL_PHONE_NO_1_IN"].SourceVersion = DataRowVersion.Current;
                // BILL_PHONE_NO_2
                _with1.Parameters.Add("BILL_PHONE_NO_2_IN", OracleDbType.Varchar2, 25, "BILL_PHONE_NO_2").Direction = ParameterDirection.Input;
                _with1.Parameters["BILL_PHONE_NO_2_IN"].SourceVersion = DataRowVersion.Current;
                // BILL_FAX_NO
                _with1.Parameters.Add("BILL_FAX_NO_IN", OracleDbType.Varchar2, 25, "BILL_FAX_NO").Direction = ParameterDirection.Input;
                _with1.Parameters["BILL_FAX_NO_IN"].SourceVersion = DataRowVersion.Current;
                // BILL_EMAIL_ID
                _with1.Parameters.Add("BILL_EMAIL_ID_IN", OracleDbType.Varchar2, 50, "BILL_EMAIL_ID").Direction = ParameterDirection.Input;
                _with1.Parameters["BILL_EMAIL_ID_IN"].SourceVersion = DataRowVersion.Current;
                // BILL_URL
                _with1.Parameters.Add("BILL_URL_IN", OracleDbType.Varchar2, 100, "BILL_URL").Direction = ParameterDirection.Input;
                _with1.Parameters["BILL_URL_IN"].SourceVersion = DataRowVersion.Current;
                //
                // CONFIG_MST_TBL COLUMN
                //
                // CONFIG_PK_IN
                _with1.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                //
                // DEPOT_MST_TBL COLUMN
                //
                // RETURN_VALUE ( OUTPUT PARAMETER )
                _with1.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "DEPOT_MST_PK").Direction = ParameterDirection.Output;
                _with1.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                // DELETE COMMAND *******************************************************************
                var _with2 = delCommand;
                _with2.Connection = objWK.MyConnection;
                _with2.CommandType = CommandType.StoredProcedure;
                _with2.CommandText = DEL_Proc;
                // DEPOT_MST_PK
                _with2.Parameters.Add("DEPOT_MST_PK_IN", OracleDbType.Int32, 10, "DEPOT_MST_PK").Direction = ParameterDirection.Input;
                _with2.Parameters["DEPOT_MST_PK_IN"].SourceVersion = DataRowVersion.Current;
                // DELETED_BY_FK
                _with2.Parameters.Add("DELETED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                // CONFIG_PK
                _with2.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                // VERSON_NO
                _with2.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                _with2.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                // RETURN VALUE
                _with2.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with2.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                // UPDATE COMMAND **********************************************************************
                var _with3 = updCommand;
                _with3.Connection = objWK.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = UPD_Proc;
                //
                // DEPOT_MST_TBL COLUMNS
                //
                // DEPOT_MST_PK
                _with3.Parameters.Add("DEPOT_MST_PK_IN", OracleDbType.Int32, 10, "DEPOT_MST_PK").Direction = ParameterDirection.Input;
                _with3.Parameters["DEPOT_MST_PK_IN"].SourceVersion = DataRowVersion.Current;
                // DEPOT_ID
                _with3.Parameters.Add("DEPOT_ID_IN", OracleDbType.Varchar2, 20, "DEPOT_ID").Direction = ParameterDirection.Input;
                _with3.Parameters["DEPOT_ID_IN"].SourceVersion = DataRowVersion.Current;
                // DEPOT_NAME
                _with3.Parameters.Add("DEPOT_NAME_IN", OracleDbType.Varchar2, 50, "DEPOT_NAME").Direction = ParameterDirection.Input;
                _with3.Parameters["DEPOT_NAME_IN"].SourceVersion = DataRowVersion.Current;
                // BUSINESS_TYPE
                _with3.Parameters.Add("BUSINESS_TYPE_IN", OracleDbType.Int32, 1, "BUSINESS_TYPE").Direction = ParameterDirection.Input;
                _with3.Parameters["BUSINESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;
                // ACCOUNT_NO
                _with3.Parameters.Add("ACCOUNT_NO_IN", OracleDbType.Varchar2, 15, "ACCOUNT_NO").Direction = ParameterDirection.Input;
                _with3.Parameters["ACCOUNT_NO_IN"].SourceVersion = DataRowVersion.Current;
                // VAT_NO
                _with3.Parameters.Add("VAT_NO_IN", OracleDbType.Varchar2, 11, "VAT_NO").Direction = ParameterDirection.Input;
                _with3.Parameters["VAT_NO_IN"].SourceVersion = DataRowVersion.Current;
                // ACTIVE_FLAG
                _with3.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
                _with3.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "LOC_FK").Direction = ParameterDirection.Input;
                _with3.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                // LAST_MODIFIED_BY_FK
                _with3.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                //
                // DEPOT_CONTACT_DTLS TABLE COLUMNS
                //
                // Administrative Address
                //
                // ADM_ADDRESS_1
                _with3.Parameters.Add("ADM_ADDRESS_1_IN", OracleDbType.Varchar2, 50, "ADM_ADDRESS_1").Direction = ParameterDirection.Input;
                _with3.Parameters["ADM_ADDRESS_1_IN"].SourceVersion = DataRowVersion.Current;
                // ADM_ADDRESS_2
                _with3.Parameters.Add("ADM_ADDRESS_2_IN", OracleDbType.Varchar2, 50, "ADM_ADDRESS_2").Direction = ParameterDirection.Input;
                _with3.Parameters["ADM_ADDRESS_2_IN"].SourceVersion = DataRowVersion.Current;
                // ADM_ADDRESS_3
                _with3.Parameters.Add("ADM_ADDRESS_3_IN", OracleDbType.Varchar2, 50, "ADM_ADDRESS_3").Direction = ParameterDirection.Input;
                _with3.Parameters["ADM_ADDRESS_3_IN"].SourceVersion = DataRowVersion.Current;
                // ADM_CITY
                _with3.Parameters.Add("ADM_CITY_IN", OracleDbType.Varchar2, 50, "ADM_CITY").Direction = ParameterDirection.Input;
                _with3.Parameters["ADM_CITY_IN"].SourceVersion = DataRowVersion.Current;
                // ADM_ZIP_CODE
                _with3.Parameters.Add("ADM_ZIP_CODE_IN", OracleDbType.Varchar2, 10, "ADM_ZIP_CODE").Direction = ParameterDirection.Input;
                _with3.Parameters["ADM_ZIP_CODE_IN"].SourceVersion = DataRowVersion.Current;
                // ADM_COUNTRY_MST_FK
                _with3.Parameters.Add("ADM_COUNTRY_MST_FK_IN", OracleDbType.Int32, 10, "ADM_COUNTRY_MST_FK").Direction = ParameterDirection.Input;
                _with3.Parameters["ADM_COUNTRY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                // ADM_SALUTATION
                _with3.Parameters.Add("ADM_SALUTATION_IN", OracleDbType.Int32, 1, "ADM_SALUTATION").Direction = ParameterDirection.Input;
                _with3.Parameters["ADM_SALUTATION_IN"].SourceVersion = DataRowVersion.Current;
                // ADM_CONTACT_PERSON
                _with3.Parameters.Add("ADM_CONTACT_PERSON_IN", OracleDbType.Varchar2, 50, "ADM_CONTACT_PERSON").Direction = ParameterDirection.Input;
                _with3.Parameters["ADM_CONTACT_PERSON_IN"].SourceVersion = DataRowVersion.Current;
                // ADM_PHONE_NO_1
                _with3.Parameters.Add("ADM_PHONE_NO_1_IN", OracleDbType.Varchar2, 25, "ADM_PHONE_NO_1").Direction = ParameterDirection.Input;
                _with3.Parameters["ADM_PHONE_NO_1_IN"].SourceVersion = DataRowVersion.Current;
                // ADM_PHONE_NO_2
                _with3.Parameters.Add("ADM_PHONE_NO_2_IN", OracleDbType.Varchar2, 25, "ADM_PHONE_NO_2").Direction = ParameterDirection.Input;
                _with3.Parameters["ADM_PHONE_NO_2_IN"].SourceVersion = DataRowVersion.Current;
                // ADM_FAX_NO
                _with3.Parameters.Add("ADM_FAX_NO_IN", OracleDbType.Varchar2, 25, "ADM_FAX_NO").Direction = ParameterDirection.Input;
                _with3.Parameters["ADM_FAX_NO_IN"].SourceVersion = DataRowVersion.Current;
                // ADM_EMAIL_ID
                _with3.Parameters.Add("ADM_EMAIL_ID_IN", OracleDbType.Varchar2, 50, "ADM_EMAIL_ID").Direction = ParameterDirection.Input;
                _with3.Parameters["ADM_EMAIL_ID_IN"].SourceVersion = DataRowVersion.Current;
                // ADM_URL
                _with3.Parameters.Add("ADM_URL_IN", OracleDbType.Varchar2, 100, "ADM_URL").Direction = ParameterDirection.Input;
                _with3.Parameters["ADM_URL_IN"].SourceVersion = DataRowVersion.Current;

                // REmarks ----added by sumi
                _with3.Parameters.Add("REMARKS_IN", OracleDbType.Varchar2, 500, "REMARKS").Direction = ParameterDirection.Input;
                _with3.Parameters["REMARKS_IN"].SourceVersion = DataRowVersion.Current;
                // end sumi

                //
                // Correspondence Address
                //
                // COR_ADDRESS_1
                _with3.Parameters.Add("COR_ADDRESS_1_IN", OracleDbType.Varchar2, 50, "COR_ADDRESS_1").Direction = ParameterDirection.Input;
                _with3.Parameters["COR_ADDRESS_1_IN"].SourceVersion = DataRowVersion.Current;
                // COR_ADDRESS_2
                _with3.Parameters.Add("COR_ADDRESS_2_IN", OracleDbType.Varchar2, 50, "COR_ADDRESS_2").Direction = ParameterDirection.Input;
                _with3.Parameters["COR_ADDRESS_2_IN"].SourceVersion = DataRowVersion.Current;
                // COR_ADDRESS_3
                _with3.Parameters.Add("COR_ADDRESS_3_IN", OracleDbType.Varchar2, 50, "COR_ADDRESS_3").Direction = ParameterDirection.Input;
                _with3.Parameters["COR_ADDRESS_3_IN"].SourceVersion = DataRowVersion.Current;
                // COR_CITY
                _with3.Parameters.Add("COR_CITY_IN", OracleDbType.Varchar2, 50, "COR_CITY").Direction = ParameterDirection.Input;
                _with3.Parameters["COR_CITY_IN"].SourceVersion = DataRowVersion.Current;
                // COR_ZIP_CODE
                _with3.Parameters.Add("COR_ZIP_CODE_IN", OracleDbType.Varchar2, 10, "COR_ZIP_CODE").Direction = ParameterDirection.Input;
                _with3.Parameters["COR_ZIP_CODE_IN"].SourceVersion = DataRowVersion.Current;
                // COR_COUNTRY_MST_FK
                _with3.Parameters.Add("COR_COUNTRY_MST_FK_IN", OracleDbType.Int32, 10, "COR_COUNTRY_MST_FK").Direction = ParameterDirection.Input;
                _with3.Parameters["COR_COUNTRY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                // COR_SALUTATION
                _with3.Parameters.Add("COR_SALUTATION_IN", OracleDbType.Int32, 1, "COR_SALUTATION").Direction = ParameterDirection.Input;
                _with3.Parameters["COR_SALUTATION_IN"].SourceVersion = DataRowVersion.Current;
                // COR_CONTACT_PERSON
                _with3.Parameters.Add("COR_CONTACT_PERSON_IN", OracleDbType.Varchar2, 50, "COR_CONTACT_PERSON").Direction = ParameterDirection.Input;
                _with3.Parameters["COR_CONTACT_PERSON_IN"].SourceVersion = DataRowVersion.Current;
                // COR_PHONE_NO_1
                _with3.Parameters.Add("COR_PHONE_NO_1_IN", OracleDbType.Varchar2, 25, "COR_PHONE_NO_1").Direction = ParameterDirection.Input;
                _with3.Parameters["COR_PHONE_NO_1_IN"].SourceVersion = DataRowVersion.Current;
                // COR_PHONE_NO_2
                _with3.Parameters.Add("COR_PHONE_NO_2_IN", OracleDbType.Varchar2, 25, "COR_PHONE_NO_2").Direction = ParameterDirection.Input;
                _with3.Parameters["COR_PHONE_NO_2_IN"].SourceVersion = DataRowVersion.Current;
                // COR_FAX_NO
                _with3.Parameters.Add("COR_FAX_NO_IN", OracleDbType.Varchar2, 25, "COR_FAX_NO").Direction = ParameterDirection.Input;
                _with3.Parameters["COR_FAX_NO_IN"].SourceVersion = DataRowVersion.Current;
                // COR_EMAIL_ID
                _with3.Parameters.Add("COR_EMAIL_ID_IN", OracleDbType.Varchar2, 50, "COR_EMAIL_ID").Direction = ParameterDirection.Input;
                _with3.Parameters["COR_EMAIL_ID_IN"].SourceVersion = DataRowVersion.Current;
                // COR_URL
                _with3.Parameters.Add("COR_URL_IN", OracleDbType.Varchar2, 100, "COR_URL").Direction = ParameterDirection.Input;
                _with3.Parameters["COR_URL_IN"].SourceVersion = DataRowVersion.Current;
                //
                // Billing Address
                //
                // BILL_ADDRESS_1
                _with3.Parameters.Add("BILL_ADDRESS_1_IN", OracleDbType.Varchar2, 50, "BILL_ADDRESS_1").Direction = ParameterDirection.Input;
                _with3.Parameters["BILL_ADDRESS_1_IN"].SourceVersion = DataRowVersion.Current;
                // BILL_ADDRESS_2
                _with3.Parameters.Add("BILL_ADDRESS_2_IN", OracleDbType.Varchar2, 50, "BILL_ADDRESS_2").Direction = ParameterDirection.Input;
                _with3.Parameters["BILL_ADDRESS_2_IN"].SourceVersion = DataRowVersion.Current;
                // BILL_ADDRESS_3
                _with3.Parameters.Add("BILL_ADDRESS_3_IN", OracleDbType.Varchar2, 50, "BILL_ADDRESS_3").Direction = ParameterDirection.Input;
                _with3.Parameters["BILL_ADDRESS_3_IN"].SourceVersion = DataRowVersion.Current;
                // BILL_CITY
                _with3.Parameters.Add("BILL_CITY_IN", OracleDbType.Varchar2, 50, "BILL_CITY").Direction = ParameterDirection.Input;
                _with3.Parameters["BILL_CITY_IN"].SourceVersion = DataRowVersion.Current;
                // BILL_ZIP_CODE
                _with3.Parameters.Add("BILL_ZIP_CODE_IN", OracleDbType.Varchar2, 10, "BILL_ZIP_CODE").Direction = ParameterDirection.Input;
                _with3.Parameters["BILL_ZIP_CODE_IN"].SourceVersion = DataRowVersion.Current;
                // BILL_COUNTRY_MST_FK
                _with3.Parameters.Add("BILL_COUNTRY_MST_FK_IN", OracleDbType.Int32, 10, "BILL_COUNTRY_MST_FK").Direction = ParameterDirection.Input;
                _with3.Parameters["BILL_COUNTRY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                // BILL_SALUTATION
                _with3.Parameters.Add("BILL_SALUTATION_IN", OracleDbType.Int32, 1, "BILL_SALUTATION").Direction = ParameterDirection.Input;
                _with3.Parameters["BILL_SALUTATION_IN"].SourceVersion = DataRowVersion.Current;
                // BILL_CONTACT_PERSON
                _with3.Parameters.Add("BILL_CONTACT_PERSON_IN", OracleDbType.Varchar2, 50, "BILL_CONTACT_PERSON").Direction = ParameterDirection.Input;
                _with3.Parameters["BILL_CONTACT_PERSON_IN"].SourceVersion = DataRowVersion.Current;
                // BILL_PHONE_NO_1
                _with3.Parameters.Add("BILL_PHONE_NO_1_IN", OracleDbType.Varchar2, 25, "BILL_PHONE_NO_1").Direction = ParameterDirection.Input;
                _with3.Parameters["BILL_PHONE_NO_1_IN"].SourceVersion = DataRowVersion.Current;
                // BILL_PHONE_NO_2
                _with3.Parameters.Add("BILL_PHONE_NO_2_IN", OracleDbType.Varchar2, 25, "BILL_PHONE_NO_2").Direction = ParameterDirection.Input;
                _with3.Parameters["BILL_PHONE_NO_2_IN"].SourceVersion = DataRowVersion.Current;
                // BILL_FAX_NO
                _with3.Parameters.Add("BILL_FAX_NO_IN", OracleDbType.Varchar2, 25, "BILL_FAX_NO").Direction = ParameterDirection.Input;
                _with3.Parameters["BILL_FAX_NO_IN"].SourceVersion = DataRowVersion.Current;
                // BILL_EMAIL_ID
                _with3.Parameters.Add("BILL_EMAIL_ID_IN", OracleDbType.Varchar2, 50, "BILL_EMAIL_ID").Direction = ParameterDirection.Input;
                _with3.Parameters["BILL_EMAIL_ID_IN"].SourceVersion = DataRowVersion.Current;
                // BILL_URL
                _with3.Parameters.Add("BILL_URL_IN", OracleDbType.Varchar2, 100, "BILL_URL").Direction = ParameterDirection.Input;
                _with3.Parameters["BILL_URL_IN"].SourceVersion = DataRowVersion.Current;
                //
                // CONFIG_MST_TBL COLUMN
                //
                // CONFIG_PK_IN
                _with3.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                //
                // DEPOT_MST_TBL COLUMN
                //
                // VERSON_NO
                _with3.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                _with3.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
                // RETURN_VALUE ( OUTPUT PARAMETER )
                _with3.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with3.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with4 = objWK.MyDataAdapter;

                _with4.InsertCommand = insCommand;
                _with4.InsertCommand.Transaction = TRAN;
                _with4.UpdateCommand = updCommand;
                _with4.UpdateCommand.Transaction = TRAN;
                _with4.DeleteCommand = delCommand;
                _with4.DeleteCommand.Transaction = TRAN;
                RecAfct = _with4.Update(M_DataSet);
                retVal = Convert.ToString(insCommand.Parameters["RETURN_VALUE"].Value);
                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    TRAN.Commit();
                    if ((retVal != null))
                    {
                        M_DataSet.Tables[0].Rows[0]["DEPOT_MST_PK"] = retVal;
                        M_DataSet.Tables[0].Rows[0]["VERSION_NO"] = 0;
                    }
                    else
                    {
                        M_DataSet.Tables[0].Rows[0]["VERSION_NO"] = Convert.ToInt32(M_DataSet.Tables[0].Rows[0]["VERSION_NO"]) + 1;
                    }

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
                objWK.CloseConnection();
            }
        }

        #endregion " Save "

        #region " Supporting Function "

        /// <summary>
        /// Ifs the database null.
        /// </summary>
        /// <param name="col">The col.</param>
        /// <returns></returns>
        private object ifDBNull(object col)
        {
            if (Convert.ToString(col).Length == 0)
            {
                return "";
            }
            else
            {
                return col;
            }
        }

        /// <summary>
        /// Removes the database null.
        /// </summary>
        /// <param name="col">The col.</param>
        /// <returns></returns>
        private object removeDBNull(object col)
        {
            if (object.ReferenceEquals(col, ""))
            {
                return "";
            }
            return col;
        }

        #endregion " Supporting Function "
    }
}