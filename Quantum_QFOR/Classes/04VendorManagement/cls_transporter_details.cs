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
    public class Cls_Transporter_Details : CommonFeatures
	{

		#region "Fetch Function"
		public DataSet FetchAll(string TransporterID = "", Int32 businesstype = 0)
		{

			string strSQL = null;
			string strCondition = null;
			WorkFlow objWF = new WorkFlow();
			strCondition = "AND ACD.TRANSPORTER_MST_FK(+) = ALM.TRANSPORTER_MST_PK AND ALM.TRANSPORTER_MST_PK = '" + TransporterID + "'";
			strCondition += " AND ALM.LOCATION_MST_FK = LOC.LOCATION_MST_PK(+)";
			strCondition += " AND ADMCNR.COUNTRY_MST_PK(+) = ACD.ADM_COUNTRY_MST_FK ";
			strCondition += " AND CORCNR.COUNTRY_MST_PK(+) = ACD.COR_COUNTRY_MST_FK ";
			strCondition += " AND BILLCNR.COUNTRY_MST_PK(+) = ACD.BILL_COUNTRY_MST_FK";

			strSQL = " SELECT ALM.TRANSPORTER_ID,";
			strSQL = strSQL + "ALM.TRANSPORTER_MST_PK, ";
			strSQL = strSQL + "ALM.TRANSPORTER_NAME,";
			strSQL = strSQL + "ALM.ACTIVE_FLAG,";
			strSQL = strSQL + "DECODE(ALM.BUSINESS_TYPE,'0','','1','Air','2','Sea','3','Both') BUSINESS_TYPE,";
			strSQL = strSQL + "ALM.VAT_NO,";
			strSQL = strSQL + "ALM.ACCOUNT_NO,";
			strSQL = strSQL + "ACD.ADM_ADDRESS_1,";
			strSQL = strSQL + "ACD.ADM_ADDRESS_2,";
			strSQL = strSQL + "ACD.ADM_ADDRESS_3,";
			strSQL = strSQL + "ACD.ADM_CITY,";
			strSQL = strSQL + "ACD.ADM_ZIP_CODE,";
			strSQL = strSQL + "ACD.ADM_COUNTRY_MST_FK,";
			strSQL = strSQL + "ACD.ADM_CONTACT_PERSON,";
			strSQL = strSQL + "ACD.ADM_PHONE_NO_1,";
			strSQL = strSQL + "ACD.ADM_PHONE_NO_2,";
			strSQL = strSQL + "ACD.ADM_FAX_NO,";
			strSQL = strSQL + "ACD.ADM_EMAIL_ID,";
			strSQL = strSQL + "ACD.ADM_URL,";

			strSQL = strSQL + "ACD.COR_ADDRESS_1,";
			strSQL = strSQL + "ACD.COR_ADDRESS_2,";
			strSQL = strSQL + "ACD.COR_ADDRESS_3,";
			strSQL = strSQL + "ACD.COR_CITY,";
			strSQL = strSQL + "ACD.COR_ZIP_CODE,";
			strSQL = strSQL + "ACD.COR_COUNTRY_MST_FK,";
			strSQL = strSQL + "ACD.COR_CONTACT_PERSON,";
			strSQL = strSQL + "ACD.COR_PHONE_NO_1,";
			strSQL = strSQL + "ACD.COR_PHONE_NO_2,";
			strSQL = strSQL + "ACD.COR_FAX_NO,";
			strSQL = strSQL + "ACD.COR_EMAIL_ID,";
			strSQL = strSQL + "ACD.COR_URL,";

			strSQL = strSQL + "ACD.BILL_ADDRESS_1,";
			strSQL = strSQL + "ACD.BILL_ADDRESS_2,";
			strSQL = strSQL + "ACD.BILL_ADDRESS_3,";
			strSQL = strSQL + "ACD.BILL_CITY,";
			strSQL = strSQL + "ACD.BILL_ZIP_CODE,";
			strSQL = strSQL + "ACD.BILL_COUNTRY_MST_FK,";
			strSQL = strSQL + "ACD.BILL_CONTACT_PERSON,";
			strSQL = strSQL + "ACD.BILL_PHONE_NO_1,";
			strSQL = strSQL + "ACD.BILL_PHONE_NO_2,";
			strSQL = strSQL + "ACD.BILL_FAX_NO,";
			strSQL = strSQL + "ACD.BILL_EMAIL_ID,";
			strSQL = strSQL + "ACD.BILL_URL,";
			strSQL = strSQL + "ACD.ADM_SALUTATION,";
			strSQL = strSQL + "ACD.COR_SALUTATION,";
			strSQL = strSQL + "ACD.BILL_SALUTATION,";
			strSQL = strSQL + " ALM.VERSION_NO,";
			strSQL = strSQL + "ALM.REMARKS,";
			strSQL = strSQL + "ADMCNR.COUNTRY_ID AS ADMCOUNTRYID,";
			strSQL = strSQL + "ADMCNR.COUNTRY_NAME AS ADMCOUNTRYNAME,";
			strSQL = strSQL + "BILLCNR.COUNTRY_ID BILLCOUNTRYID,";
			strSQL = strSQL + "BILLCNR.COUNTRY_NAME BILLCOUNTRYNAME,";
			strSQL = strSQL + "CORCNR.COUNTRY_ID CORCOUNTRYID,";
			strSQL = strSQL + "CORCNR.COUNTRY_NAME CORCOUNTRYNAME, ";
			strSQL = strSQL + " ALM.LOCATION_MST_FK,";
			strSQL = strSQL + " LOC.LOCATION_ID,";
			strSQL = strSQL + " LOC.LOCATION_NAME";

			strSQL = strSQL + " FROM TRANSPORTER_MST_TBL ALM,TRANSPORTER_CONTACT_DTLS ACD,LOCATION_MST_TBL LOC, ";
			strSQL = strSQL + " COUNTRY_MST_TBL ADMCNR,";
			strSQL = strSQL + " COUNTRY_MST_TBL CORCNR,";
			strSQL = strSQL + " COUNTRY_MST_TBL BILLCNR";
			strSQL = strSQL + "WHERE ( 1 = 1) ";

			strSQL = strSQL + strCondition;
			try {
				return objWF.GetDataSet(strSQL);
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		#endregion

		#region "SaveNew"
		public ArrayList SaveNew(string TRANSPORTERID = "", string TRANSPORTERName = "", string activeflag = "", string VATCODE = "", int BType = 0, string ACCOUNTNO = "", string ADMADDRESS1 = "", string ADMADDRESS2 = "", string ADMADDRESS3 = "", string ADMCITY = "",
		string ADMZIPCODE = "", Int32 ADMCOUNTRY = 0, Int32 ADMSALUTATION = 0, string ADMCONTACTPERSON = "", string ADMPHONE1 = "", string ADMPHONE2 = "", string ADMFAXNO = "", string ADMEMAIL = "", string ADMURL = "", string CORADDRESS1 = "",
		string CORADDRESS2 = "", string CORADDRESS3 = "", string CORCITY = "", string CORZIPCODE = "", string CORCOUNTRY = "", Int32 CORSALUTATION = 0, string CORCONTACTPERSON = "", string CORPHONE1 = "", string CORPHONE2 = "", string CORFAXNO = "",
		string COREMAIL = "", string CORURL = "", string BILLADDRESS1 = "", string BILLADDRESS2 = "", string BILLADDRESS3 = "", string BILLCITY = "", string BILLZIPCODE = "", string BILLCOUNTRY = "", Int32 BILLSALUTATION = 0, string BILLCONTACTPERSON = "",
		string BILLPHONE1 = "", string BILLPHONE2 = "", string BILLFAXNO = "", string BILLEMAIL = "", string BILLURL = "", int transporterPk = 0, string location = "", string admremark = "")
		{


			OracleCommand insCommand = new OracleCommand();
			WorkFlow objWK = new WorkFlow();
			objWK.OpenConnection();
			OracleTransaction TRAN = null;
			TRAN = objWK.MyConnection.BeginTransaction();
			try {
				DataTable DtTbl = new DataTable();
				var _with1 = insCommand;
				_with1.Connection = objWK.MyConnection;
				_with1.CommandType = CommandType.StoredProcedure;
				_with1.CommandText = objWK.MyUserName + ".TRANSPORTER_MST_TBL_PKG.TRANSPORTER_MST_TBL_INS";

				var _with2 = _with1.Parameters;
				_with2.Add("TRANSPORTER_ID_IN", TRANSPORTERID).Direction = ParameterDirection.Input;
				_with2.Add("TRANSPORTER_NAME_IN", TRANSPORTERName).Direction = ParameterDirection.Input;
				_with2.Add("ACTIVE_FLAG_IN", activeflag).Direction = ParameterDirection.Input;
				_with2.Add("ACCOUNT_NO_IN", ACCOUNTNO).Direction = ParameterDirection.Input;
				_with2.Add("VAT_NO_IN", VATCODE).Direction = ParameterDirection.Input;
				_with2.Add("BUSINESS_TYPE_IN", BType).Direction = ParameterDirection.Input;
				_with2.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
				_with2.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
				_with2.Add("ADM_ADDRESS_1_IN", ADMADDRESS1).Direction = ParameterDirection.Input;
				_with2.Add("ADM_ADDRESS_2_IN", ADMADDRESS2).Direction = ParameterDirection.Input;
				_with2.Add("ADM_ADDRESS_3_IN", ADMADDRESS3).Direction = ParameterDirection.Input;
				_with2.Add("ADM_CITY_IN", ADMCITY).Direction = ParameterDirection.Input;
				_with2.Add("ADM_ZIP_CODE_IN", ADMZIPCODE).Direction = ParameterDirection.Input;
				_with2.Add("ADM_COUNTRY_MST_FK_IN", ADMCOUNTRY).Direction = ParameterDirection.Input;
				_with2.Add("ADM_SALUTATION_IN", ADMSALUTATION).Direction = ParameterDirection.Input;
				_with2.Add("ADM_CONTACT_PERSON_IN", ADMCONTACTPERSON).Direction = ParameterDirection.Input;
				_with2.Add("ADM_PHONE_NO_1_IN", ADMPHONE1).Direction = ParameterDirection.Input;
				_with2.Add("ADM_PHONE_NO_2_IN", ADMPHONE2).Direction = ParameterDirection.Input;
				_with2.Add("ADM_FAX_NO_IN", ADMFAXNO).Direction = ParameterDirection.Input;
				_with2.Add("ADM_EMAIL_ID_IN", ADMEMAIL).Direction = ParameterDirection.Input;
				_with2.Add("ADM_URL_IN", ADMURL).Direction = ParameterDirection.Input;
				_with2.Add("REMARKS_IN", admremark).Direction = ParameterDirection.Input;

				_with2.Add("COR_ADDRESS_1_IN", BILLADDRESS1).Direction = ParameterDirection.Input;
				_with2.Add("COR_ADDRESS_2_IN", BILLADDRESS2).Direction = ParameterDirection.Input;
				_with2.Add("COR_ADDRESS_3_IN", BILLADDRESS3).Direction = ParameterDirection.Input;
				_with2.Add("COR_CITY_IN", BILLCITY).Direction = ParameterDirection.Input;
				_with2.Add("COR_ZIP_CODE_IN", BILLZIPCODE).Direction = ParameterDirection.Input;
				_with2.Add("COR_COUNTRY_MST_FK_IN", (BILLCOUNTRY != "" ? BILLCOUNTRY : "")).Direction = ParameterDirection.Input;
				_with2.Add("COR_SALUTATION_IN", CORSALUTATION).Direction = ParameterDirection.Input;
				_with2.Add("COR_CONTACT_PERSON_IN", BILLCONTACTPERSON).Direction = ParameterDirection.Input;
				_with2.Add("COR_PHONE_NO_1_IN", BILLPHONE1).Direction = ParameterDirection.Input;
				_with2.Add("COR_PHONE_NO_2_IN", BILLPHONE2).Direction = ParameterDirection.Input;
				_with2.Add("COR_FAX_NO_IN", BILLFAXNO).Direction = ParameterDirection.Input;
				_with2.Add("COR_EMAIL_ID_IN", BILLEMAIL).Direction = ParameterDirection.Input;
				_with2.Add("COR_URL_IN", BILLURL).Direction = ParameterDirection.Input;

				_with2.Add("BILL_ADDRESS_1_IN", CORADDRESS1).Direction = ParameterDirection.Input;
				_with2.Add("BILL_ADDRESS_2_IN", CORADDRESS2).Direction = ParameterDirection.Input;
				_with2.Add("BILL_ADDRESS_3_IN", CORADDRESS3).Direction = ParameterDirection.Input;
				_with2.Add("BILL_CITY_IN", CORCITY).Direction = ParameterDirection.Input;
				_with2.Add("BILL_ZIP_CODE_IN", CORZIPCODE).Direction = ParameterDirection.Input;
				_with2.Add("BILL_COUNTRY_MST_FK_IN", (CORCOUNTRY != "" ? CORCOUNTRY : "")).Direction = ParameterDirection.Input;
				_with2.Add("BILL_SALUTATION_IN", CORSALUTATION).Direction = ParameterDirection.Input;
				_with2.Add("BILL_CONTACT_PERSON_IN", CORCONTACTPERSON).Direction = ParameterDirection.Input;
				_with2.Add("BILL_PHONE_NO_1_IN", CORPHONE1).Direction = ParameterDirection.Input;
				_with2.Add("BILL_PHONE_NO_2_IN", CORPHONE2).Direction = ParameterDirection.Input;
				_with2.Add("BILL_FAX_NO_IN", CORFAXNO).Direction = ParameterDirection.Input;
				_with2.Add("BILL_EMAIL_ID_IN", COREMAIL).Direction = ParameterDirection.Input;
				_with2.Add("BILL_URL_IN", CORURL).Direction = ParameterDirection.Input;
				_with2.Add("CREATEDD_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
				_with2.Add("LASTT_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
				_with2.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
				_with2.Add("LOCATION_MST_FK_IN", (string.IsNullOrEmpty(location) ? "" : location)).Direction = ParameterDirection.Input;
				_with2.Add("RETURN_VALUE", transporterPk).Direction = ParameterDirection.Output;
				insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				var _with3 = objWK.MyDataAdapter;
				_with3.InsertCommand = insCommand;
				_with3.InsertCommand.Transaction = TRAN;
				_with3.InsertCommand.ExecuteNonQuery();
				TRAN.Commit();
				transporterPk = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
				return arrMessage;
			} catch (Exception ex) {
				arrMessage.Add(ex.Message);
				TRAN.Rollback();
				return arrMessage;
			} finally {
				objWK.CloseConnection();
			}
		}
		#endregion

		#region "Update"
		public ArrayList UpdateData(string TRANSPORTERID = "", Int32 TRANSPORTERPkValue = 0, string TRANSPORTERName = "", string activeflag = "", string Vatcode = "", int BType = 0, string ACCOUNTNO = "", string ADMADDRESS1 = "", string ADMADDRESS2 = "", string ADMADDRESS3 = "",
		string ADMCITY = "", string ADMZIPCODE = "", Int32 ADMCOUNTRY = 0, Int32 ADMSALUTATION = 0, string ADMCONTACTPERSON = "", string ADMPHONE1 = "", string ADMPHONE2 = "", string ADMFAXNO = "", string ADMEMAIL = "", string ADMURL = "",
		string CORADDRESS1 = "", string CORADDRESS2 = "", string CORADDRESS3 = "", string CORCITY = "", string CORZIPCODE = "", Int32 CORCOUNTRY = 0, Int32 CORSALUTATION = 0, string CORCONTACTPERSON = "", string CORPHONE1 = "", string CORPHONE2 = "",
		string CORFAXNO = "", string COREMAIL = "", string CORURL = "", string BILLADDRESS1 = "", string BILLADDRESS2 = "", string BILLADDRESS3 = "", string BILLCITY = "", string BILLZIPCODE = "", Int32 BILLCOUNTRY = 0, Int32 BILLSALUTATION = 0,
		string BILLCONTACTPERSON = "", string BILLPHONE1 = "", string BILLPHONE2 = "", string BILLFAXNO = "", string BILLEMAIL = "", string BILLURL = "", string versionNo = "", string location = "", string ADMREMARK = "")
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
			OracleTransaction TRAN = null;
			TRAN = objWK.MyConnection.BeginTransaction();
			int configKey = 0;
			M_Configuration_PK = 24;

			try {
				DataTable DtTbl = new DataTable();
				DataRow DtRw = null;
				int i = 0;
				var _with4 = updCommand;
				_with4.Connection = objWK.MyConnection;
				_with4.CommandType = CommandType.StoredProcedure;
				_with4.CommandText = objWK.MyUserName + ".TRANSPORTER_MST_TBL_PKG.TRANSPORTER_MST_TBL_UPD";

				var _with5 = _with4.Parameters;
				_with5.Add("TRANSPORTER_MST_PK_IN", TRANSPORTERPkValue).Direction = ParameterDirection.Input;
				_with5.Add("TRANSPORTER_ID_IN", TRANSPORTERID).Direction = ParameterDirection.Input;

				_with5.Add("TRANSPORTER_NAME_IN", TRANSPORTERName).Direction = ParameterDirection.Input;

				_with5.Add("ACTIVE_FLAG_IN", activeflag).Direction = ParameterDirection.Input;

				_with5.Add("ACCOUNT_NO_IN", ACCOUNTNO).Direction = ParameterDirection.Input;


				_with5.Add("VAT_NO_IN", Vatcode).Direction = ParameterDirection.Input;
				_with5.Add("BUSINESS_TYPE_IN", BType).Direction = ParameterDirection.Input;
				_with5.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;


				_with5.Add("ADM_ADDRESS_1_IN", ADMADDRESS1).Direction = ParameterDirection.Input;
				_with5.Add("ADM_ADDRESS_2_IN", ADMADDRESS2).Direction = ParameterDirection.Input;
				_with5.Add("ADM_ADDRESS_3_IN", ADMADDRESS3).Direction = ParameterDirection.Input;
				_with5.Add("ADM_CITY_IN", ADMCITY).Direction = ParameterDirection.Input;
				_with5.Add("ADM_ZIP_CODE_IN", ADMZIPCODE).Direction = ParameterDirection.Input;
				_with5.Add("ADM_COUNTRY_MST_FK_IN", ADMCOUNTRY).Direction = ParameterDirection.Input;
				_with5.Add("ADM_SALUTATION_IN", ADMSALUTATION).Direction = ParameterDirection.Input;
				_with5.Add("ADM_CONTACT_PERSON_IN", ADMCONTACTPERSON).Direction = ParameterDirection.Input;
				_with5.Add("ADM_PHONE_NO_1_IN", ADMPHONE1).Direction = ParameterDirection.Input;
				_with5.Add("ADM_PHONE_NO_2_IN", ADMPHONE2).Direction = ParameterDirection.Input;
				_with5.Add("ADM_FAX_NO_IN", ADMFAXNO).Direction = ParameterDirection.Input;
				_with5.Add("ADM_EMAIL_ID_IN", ADMEMAIL).Direction = ParameterDirection.Input;
				_with5.Add("ADM_URL_IN", ADMURL).Direction = ParameterDirection.Input;
				_with5.Add("REMARKS_IN", ADMREMARK).Direction = ParameterDirection.Input;

				_with5.Add("COR_ADDRESS_1_IN", CORADDRESS1).Direction = ParameterDirection.Input;
				_with5.Add("COR_ADDRESS_2_IN", CORADDRESS2).Direction = ParameterDirection.Input;
				_with5.Add("COR_ADDRESS_3_IN", CORADDRESS3).Direction = ParameterDirection.Input;
				_with5.Add("COR_CITY_IN", CORCITY).Direction = ParameterDirection.Input;
				_with5.Add("COR_ZIP_CODE_IN", CORZIPCODE).Direction = ParameterDirection.Input;

				_with5.Add("COR_COUNTRY_MST_FK_IN", (CORCOUNTRY != 0 ? CORCOUNTRY : 0)).Direction = ParameterDirection.Input;
				_with5.Add("COR_SALUTATION_IN", CORSALUTATION).Direction = ParameterDirection.Input;
				_with5.Add("COR_CONTACT_PERSON_IN", CORCONTACTPERSON).Direction = ParameterDirection.Input;
				_with5.Add("COR_PHONE_NO_1_IN", CORPHONE1).Direction = ParameterDirection.Input;
				_with5.Add("COR_PHONE_NO_2_IN", CORPHONE2).Direction = ParameterDirection.Input;
				_with5.Add("COR_FAX_NO_IN", CORFAXNO).Direction = ParameterDirection.Input;
				_with5.Add("COR_EMAIL_ID_IN", COREMAIL).Direction = ParameterDirection.Input;
				_with5.Add("COR_URL_IN", CORURL).Direction = ParameterDirection.Input;

				_with5.Add("BILL_ADDRESS_1_IN", BILLADDRESS1).Direction = ParameterDirection.Input;
				_with5.Add("BILL_ADDRESS_2_IN", BILLADDRESS2).Direction = ParameterDirection.Input;
				_with5.Add("BILL_ADDRESS_3_IN", BILLADDRESS3).Direction = ParameterDirection.Input;
				_with5.Add("BILL_CITY_IN", BILLCITY).Direction = ParameterDirection.Input;
				_with5.Add("BILL_ZIP_CODE_IN", BILLZIPCODE).Direction = ParameterDirection.Input;
				_with5.Add("BILL_COUNTRY_MST_FK_IN", (BILLCOUNTRY != 0 ? BILLCOUNTRY : 0)).Direction = ParameterDirection.Input;
				_with5.Add("BILL_SALUTATION_IN", BILLSALUTATION).Direction = ParameterDirection.Input;
				_with5.Add("BILL_CONTACT_PERSON_IN", BILLCONTACTPERSON).Direction = ParameterDirection.Input;
				_with5.Add("BILL_PHONE_NO_1_IN", BILLPHONE1).Direction = ParameterDirection.Input;
				_with5.Add("BILL_PHONE_NO_2_IN", BILLPHONE2).Direction = ParameterDirection.Input;
				_with5.Add("BILL_FAX_NO_IN", BILLFAXNO).Direction = ParameterDirection.Input;
				_with5.Add("BILL_EMAIL_ID_IN", BILLEMAIL).Direction = ParameterDirection.Input;
				_with5.Add("BILL_URL_IN", BILLURL).Direction = ParameterDirection.Input;
				_with5.Add("LASTT_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
				_with5.Add("CONFIG_PK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
				_with5.Add("VERSION_NO_IN", versionNo).Direction = ParameterDirection.Input;
				_with5.Add("LOCATION_MST_FK_IN", (string.IsNullOrEmpty(location) ? "" : location)).Direction = ParameterDirection.Input;
				_with5.Add("RETURN_VALUE", ADMCOUNTRY).Direction = ParameterDirection.Output;
                

				var _with6 = objWK.MyDataAdapter;
				_with6.UpdateCommand = updCommand;
				_with6.UpdateCommand.Transaction = TRAN;
				_with6.UpdateCommand.ExecuteNonQuery();
				TRAN.Commit();
				return arrMessage;
			} catch (Exception ex) {
				arrMessage.Add(ex.Message);
				TRAN.Rollback();
				return arrMessage;
			} finally {
				objWK.CloseConnection();
			}
		}
		#endregion

		#region "fetching zone"
		public DataSet fetchZone(int transporterpk, int indexZero = 0)
		{
			string strSQL = null;
			if (indexZero == 1) {
				strSQL = strSQL + "Select ";
				strSQL = strSQL + " ''ZONE_NAME,";
				strSQL = strSQL + " 0 TRANSPORTER_ZONES_PK,";
				strSQL = strSQL + " 0 TRANSPORTER_MST_FK, ";
				strSQL = strSQL + " 0 VERSION_NO ";
				strSQL = strSQL + " from DUAL";
				strSQL = strSQL + " UNION";
			}
			strSQL = strSQL + " Select ";
			strSQL = strSQL + " ZONE_NAME,";
			strSQL = strSQL + " TRANSPORTER_ZONES_PK,";
			strSQL = strSQL + " TRANSPORTER_MST_FK, ";
			strSQL = strSQL + " VERSION_NO ";
			strSQL = strSQL + " from TRANSPORTER_ZONES_TBL Where TRANSPORTER_MST_FK=" + transporterpk + "";
			strSQL = strSQL + " order by ZONE_NAME";
			WorkFlow objWF = new WorkFlow();
			try {
				return objWF.GetDataSet(strSQL);
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		#endregion

		#region "fetch only places "
		public OracleDataReader fetchOnlyPlaces(int transporterFk)
		{
			string strSQL = null;
			strSQL = strSQL + "Select ";
			strSQL = strSQL + "PLACE_MST_TBL_FK ";
			strSQL = strSQL + "from transporter_zones_trn Where TRANSPORTER_ZONES_FK=" + transporterFk + "";
			WorkFlow objWF = new WorkFlow();
			try {
				return objWF.GetDataReader(strSQL);
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		#endregion

		#region "save Zone"
		public ArrayList trnSave(DataSet M_DataSet)
		{
			WorkFlow objWK = new WorkFlow();
			objWK.OpenConnection();
			OracleTransaction TRAN = null;
			TRAN = objWK.MyConnection.BeginTransaction();
			int intPKVal = 0;
			long lngI = 0;
			Int32 RecAfct = default(Int32);
			OracleCommand insCommand = new OracleCommand();
			OracleCommand updCommand = new OracleCommand();
			string INS_Proc = null;
			string UPD_Proc = null;
			INS_Proc = objWK.MyUserName + ".TRANSPORTER_ZONES_TBL_PKG.TRANSPORTER_ZONES_TBL_INS";
			UPD_Proc = objWK.MyUserName + ".TRANSPORTER_ZONES_TBL_PKG.TRANSPORTER_ZONES_TBL_UPD";
			insCommand.Transaction = TRAN;
			updCommand.Transaction = TRAN;

			try {
				DataTable DtTbl = new DataTable();
				DataRow DtRw = null;
				int i = 0;
				DtTbl = M_DataSet.Tables[0];

				var _with7 = insCommand;
				_with7.Connection = objWK.MyConnection;
				_with7.CommandType = CommandType.StoredProcedure;
				_with7.CommandText = INS_Proc;
				foreach (DataRow DtRw_loopVariable in M_DataSet.Tables[0].Rows) {
					DtRw = DtRw_loopVariable;
					if (string.IsNullOrEmpty(DtRw["TRANSPORTER_ZONES_PK"].ToString())) {
						_with7.Parameters.Add("TRANSPORTER_MST_FK_IN", DtRw[2]);
						_with7.Parameters.Add("ZONE_NAME_IN", DtRw[0]);
						_with7.Parameters.Add("PLACE_MST_TBL_FK_IN", DtRw[4]);
						_with7.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
						_with7.Parameters.Add("CONFIG_MST_PK_IN", ConfigurationPK);
						_with7.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "TRANSPORTER_ZONES_PK").Direction = ParameterDirection.Output;
						_with7.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
						insCommand.ExecuteNonQuery();
						_with7.Parameters.Clear();
					}

				}

				var _with8 = updCommand;
				_with8.Connection = objWK.MyConnection;
				_with8.CommandType = CommandType.StoredProcedure;
				_with8.CommandText = UPD_Proc;
				foreach (DataRow DtRw_loopVariable in M_DataSet.Tables[0].Rows) {
					DtRw = DtRw_loopVariable;
					if (!string.IsNullOrEmpty(DtRw["TRANSPORTER_ZONES_PK"].ToString())) {
						_with8.Parameters.Add("TRANSPORTER_ZONES_PK_IN", DtRw[1]);
						_with8.Parameters.Add("TRANSPORTER_MST_FK_IN", DtRw[2]);
						_with8.Parameters.Add("ZONE_NAME_IN", DtRw[0]);
						_with8.Parameters.Add("PLACE_MST_TBL_FK_IN", (string.IsNullOrEmpty(DtRw[4].ToString()) ? "" : DtRw[4]));
						_with8.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK);
						_with8.Parameters.Add("VERSION_NO_IN", DtRw[3]);
						_with8.Parameters.Add("CONFIG_MST_PK_IN", ConfigurationPK);
						_with8.Parameters.Add("RETURN_VALUE", OracleDbType.NVarchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
						_with8.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
						updCommand.ExecuteNonQuery();
						_with8.Parameters.Clear();
					}
				}
                

				var _with9 = objWK.MyDataAdapter;
				if (arrMessage.Count > 0) {
					return arrMessage;
				} else {
					TRAN.Commit();
					arrMessage.Add("All Data Saved Successfully");
					return arrMessage;
				}

			} catch (OracleException oraexp) {
				arrMessage.Add(oraexp.Message.ToString());
				return arrMessage;
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				objWK.CloseConnection();
			}
		}
		#endregion

		#region "SelectCity"
		public string SelectCity(int fkAdmincity)
		{
			try {
				string strSQL = null;
				WorkFlow objWF = new WorkFlow();
				strSQL = "select CITY_NAME from CITY_MST_TBL where CITY_PK = " + fkAdmincity + " ";
				string cityAdmin = null;
				cityAdmin = objWF.ExecuteScaler(strSQL);
				return cityAdmin;
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "SelectCountry"
		public string SelectCountry(int fkAdmincountry)
		{
			try {
				string strSQL = null;
				WorkFlow objWF = new WorkFlow();
				strSQL = "select COUNTRY_NAME from COUNTRY_MST_TBL where COUNTRY_MST_PK = " + fkAdmincountry + " ";
				string cityAdmin = null;
				cityAdmin = objWF.ExecuteScaler(strSQL);
				return cityAdmin;
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "SelectLocation"
		public string SelectLocation(int fkLocation)
		{
			try {
				string strSQL = null;
				WorkFlow objWF = new WorkFlow();
				strSQL = "select LOCATION_NAME from LOCATION_MST_TBL where LOCATION_MST_PK = " + fkLocation + " ";
				string LocName = null;
				LocName = objWF.ExecuteScaler(strSQL);
				return LocName;
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "SelectLocation"
		public DataSet FetchTransporter(int TransPk)
		{
			try {
				string strSQL = null;
				DataSet ds = new DataSet();
				WorkFlow objWF = new WorkFlow();
				strSQL = "select VENDOR_ID,VENDOR_NAME from VENDOR_MST_TBL where VENDOR_MST_PK = " + TransPk + " ";
				ds = objWF.GetDataSet(strSQL);
				return ds;
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

	}
}
