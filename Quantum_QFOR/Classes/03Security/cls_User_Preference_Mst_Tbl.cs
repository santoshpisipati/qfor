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

using Newtonsoft.Json;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections;
using System.Data;

namespace Quantum_QFOR
{
    public class cls_User_Preference_Mst_Tbl : CommonFeatures
	{
		#region "Private Memebers of the class"
		private int M_USER_PREFERENCE_PK;
		private int M_USER_MST_FK;
			#endregion
		private string M_STYLESHEETPATH;
		#region "Public Properties of the class"
		public int User_Preference_PK {
			get { return M_USER_PREFERENCE_PK; }
			set { M_USER_PREFERENCE_PK = value; }
		}
		public string StyleSHeetPath {
			get { return M_STYLESHEETPATH; }
			set { M_STYLESHEETPATH = value; }
		}
		public int User_Mst_FK {
			get { return M_USER_MST_FK; }
			set { M_USER_MST_FK = value; }
		}
		#endregion
		#region "Public functions"
		public string GetStyleSheetPath(int UserFK)
		{
			string strSql = null;
			try {
				WorkFlow objWK = new WorkFlow();

				strSql = strSql + " SELECT";
				strSql = strSql + " USRPREF.USER_PREFERENCE_VALUE PREFVALUE";
				strSql = strSql + " FROM";
				strSql = strSql + " USER_PREFERENCE_TBL USRPREF,";
				strSql = strSql + " PREFERENCES_MST_TBL PREFMST";
				strSql = strSql + " WHERE";
				strSql = strSql + " USRPREF.PREFERENCE_MST_FK = PREFMST.PAREFERENCES_MST_PK";
				strSql = strSql + " AND USRPREF.USER_MST_FK = " + UserFK;
				strSql = strSql + " AND PREFMST.PREFENECES_ID = 'STYLESHEET'";

				return objWK.GetDataSet(strSql).Tables[0].Rows[0]["PREFVALUE"].ToString();
			} catch (Exception ex) {
				return "";
			}
		}
		public void UpdateStylePath(string FileName, int UserFK)
		{
			try {
				string strSQL = "UPDATE USER_PREFERENCE_MST_TBL set STYLESHEETPATH='" + FileName + "' WHERE USER_MST_FK=" + UserFK;
				WorkFlow objWK = new WorkFlow();
				objWK.ExecuteCommands(strSQL);
				objWK = null;

			} catch (Exception ex) {
			}
		}

		public double GetLanguageOptionold(int UserFK)
		{
			string strSql = null;
			try {
				WorkFlow objWK = new WorkFlow();
				double EnvPk = 0;
				strSql = strSql + " SELECT";
				strSql = strSql + " USRPREF.USER_PREFERENCE_VALUE PREFVALUE";
				strSql = strSql + " FROM";
				strSql = strSql + " USER_PREFERENCE_TBL USRPREF,";
				strSql = strSql + " PREFERENCES_MST_TBL PREFMST";
				strSql = strSql + " WHERE";
				strSql = strSql + " USRPREF.PREFERENCE_MST_FK = PREFMST.PAREFERENCES_MST_PK";
				strSql = strSql + " AND USRPREF.USER_MST_FK = " + UserFK;
				strSql = strSql + " AND PREFMST.PREFENECES_ID = 'LANG'";
				return Convert.ToDouble(objWK.GetDataSet(strSql).Tables[0].Rows[0]["PREFVALUE"].ToString());

			} catch (Exception ex) {
				return 0;
			}
		}
		public double GetLanguageOption(int UserFK)
		{
			//'Added by kanakaraj for getting enviornmetpk
			string strSql = null;
			try {
				WorkFlow objWK = new WorkFlow();
				double EnvPk = 0;
				strSql = strSql + " SELECT";
				strSql = strSql + " USRPREF.ENVIRONMENT_FK ENVIRONMENT_FK";
				strSql = strSql + " FROM";
				strSql = strSql + " USER_PREFERENCE_TBL USRPREF,";
				strSql = strSql + " PREFERENCES_MST_TBL PREFMST";
				strSql = strSql + " WHERE";
				// strSql = strSql & vbCrLf & " USRPREF.PREFERENCE_MST_FK = PREFMST.PAREFERENCES_MST_PK"
				strSql = strSql + " USRPREF.USER_MST_FK = " + UserFK;
				strSql = strSql + " AND PREFMST.PREFENECES_ID = 'LANG'";
				return Convert.ToDouble(objWK.GetDataSet(strSql).Tables[0].Rows[0]["ENVIRONMENT_FK"].ToString());

			} catch (Exception ex) {
				return 0;
			}
		}
		public double GetPasswordExpiryDate(int UserFK)
		{
			string strSql = null;
			try {
				WorkFlow objWK = new WorkFlow();
				double EnvPk = 0;
				strSql = strSql + " SELECT";
				strSql = strSql + " USRPREF.PASSWORD_EXPIRY_DT PASSWORD_EXPIRY_DT";
				strSql = strSql + " FROM";
				strSql = strSql + " USER_PREFERENCE_TBL USRPREF,";
				strSql = strSql + " PREFERENCES_MST_TBL PREFMST";
				strSql = strSql + " WHERE";
				// strSql = strSql & vbCrLf & " USRPREF.PREFERENCE_MST_FK = PREFMST.PAREFERENCES_MST_PK"
				strSql = strSql + " USRPREF.USER_MST_FK = " + UserFK;
				strSql = strSql + " AND PREFMST.PREFENECES_ID = 'LANG'";
				return Convert.ToDouble(objWK.GetDataSet(strSql).Tables[0].Rows[0]["PASSWORD_EXPIRY_DT"].ToString());

			} catch (Exception ex) {
				return 0;
			}
		}
		public double GetPasswordAlertDate(int UserFK)
		{
			string strSql = null;
			try {
				WorkFlow objWK = new WorkFlow();
				double EnvPk = 0;
				strSql = strSql + " SELECT";
				strSql = strSql + " usrpref.exp_notification_dt exp_notification_dt";
				strSql = strSql + " FROM";
				strSql = strSql + " USER_PREFERENCE_TBL USRPREF,";
				strSql = strSql + " PREFERENCES_MST_TBL PREFMST";
				strSql = strSql + " WHERE";
				// strSql = strSql & vbCrLf & " USRPREF.PREFERENCE_MST_FK = PREFMST.PAREFERENCES_MST_PK"
				strSql = strSql + " USRPREF.USER_MST_FK = " + UserFK;
				strSql = strSql + " AND PREFMST.PREFENECES_ID = 'LANG'";
				return Convert.ToDouble(objWK.GetDataSet(strSql).Tables[0].Rows[0]["exp_notification_dt"].ToString());

			} catch (Exception ex) {
				return 0;
			}
		}

		public string GetDateFormat(int UserFK)
		{
			string strSql = null;
			try {
				//Dim objWK As New WorkFlow
				//Dim EnvPk As Double
				//strSql = strSql & vbCrLf & " SELECT"
				//strSql = strSql & vbCrLf & " USRPREF.USER_PREFERENCE_VALUE PREFVALUE"
				//strSql = strSql & vbCrLf & " FROM"
				//strSql = strSql & vbCrLf & " USER_PREFERENCE_TBL USRPREF,"
				//strSql = strSql & vbCrLf & " PREFERENCES_MST_TBL PREFMST"
				//strSql = strSql & vbCrLf & " WHERE"
				//strSql = strSql & vbCrLf & " USRPREF.PREFERENCE_MST_FK = PREFMST.PAREFERENCES_MST_PK"
				//strSql = strSql & vbCrLf & " AND USRPREF.USER_MST_FK = " & UserFK
				//strSql = strSql & vbCrLf & " AND PREFMST.PREFENECES_ID = 'DATE FORMAT'"
				//If objWK.GetDataSet(strSql).Tables(0).Rows.Count > 0 Then
				//    Return objWK.GetDataSet(strSql).Tables(0).Rows(0).Item("PREFVALUE").ToString()
				//Else
				//    Return "dd/MM/yyyy"
				//End If
				return "dd/MM/yyyy";
			} catch (Exception ex) {
				return "dd/MM/yyyy";
			}
		}
		#endregion

		#region "Fetch All"
		public string FetchAll(Int32 User_Pk, Int32 Loc_Fk)
		{
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			sb.Append("SELECT U.USER_PREFERENCES_PK,");
			sb.Append("       U.USER_MST_FK,");
			sb.Append("       U.USER_STYLE_SHEET,");
			sb.Append("       UMT.DEFAULT_LOCATION_FK,");
			sb.Append("       U.USER_DATE,");
			sb.Append("       U.VERSION_NO,");
			sb.Append("       U.USER_DATE,");
			sb.Append("       U.LIST_ONLOAD, ");
			sb.Append("       U.USER_GRIDRECORDS,");
			sb.Append("       Nvl(u.password_expiry_dt,0) password_expiry_dt,");
			sb.Append("       Nvl(u.exp_notification_dt,0 ) exp_notification_dt,");
			sb.Append("       Nvl(u.CONTR_EXPIRY_ALERT_DAYS,0 ) CONTR_EXPIRY_ALERT_DAYS,");
			//sb.Append("       U.DATE_RANGE")
			sb.Append("       U.PREFERENCE_DAYS,");
			//Added by anand for fetching preference days 12-05-08
			sb.Append("       U.DATE_ONLOAD, U.ENVIRONMENT_FK");
			//Added by anand for fetching preference days 12-05-08
			sb.Append("  FROM USER_PREFERENCE_TBL U,USER_MST_TBL UMT");
			sb.Append(" WHERE U.USER_MST_FK=UMT.USER_MST_PK");
			sb.Append(" And U.USER_MST_FK=" + User_Pk + " ");
			sb.Append(" AND  UMT.DEFAULT_LOCATION_FK=" + Loc_Fk + " ");

			try {
                return JsonConvert.SerializeObject(objWF.GetDataSet(sb.ToString()), Formatting.Indented);
            } catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		#endregion
		#region "Fetch UserID"
		public string FetchUserID(string UserPK)
		{
			string StrSql = null;
			StrSql = StrSql + "select t.user_id from user_mst_tbl t Where t.user_mst_pk = '" + UserPK + "'";
			WorkFlow objWF = new WorkFlow();
			try {
				return objWF.ExecuteScaler(StrSql);
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}

		#endregion
		#region "Fetch UserName"
		public string FetchUserName(string UserPK)
		{
			string StrSql = null;
			StrSql = StrSql + "select t.user_name from user_mst_tbl t Where t.user_mst_pk = '" + UserPK + "'";
			WorkFlow objWF = new WorkFlow();
			try {
				return objWF.ExecuteScaler(StrSql);
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}

		#endregion
		#region "Fetch Location"
		public string FetchLocationName(string LocationId)
		{
			string StrSql = null;
			StrSql = StrSql + "Select Location_Name From Location_Mst_Tbl t where t.location_mst_pk = '" + LocationId + "'";
			WorkFlow objWF = new WorkFlow();
			try {
				return objWF.ExecuteScaler(StrSql);
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}

		public DataSet FetchEnvironmentDetails()
		{
			string strSQL = null;

			strSQL = strSQL + " select ";
			strSQL = strSQL + " environment_mst_pk, ";
			strSQL = strSQL + " environment_descr ";
			strSQL = strSQL + " from environment_mst_tbl ";
			strSQL = strSQL + " order by environment_descr";

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

		public string FetchLanguage(string LanguagePK)
		{
			string StrSql = null;

			StrSql = StrSql + "select environment_fk from user_preference_tbl where USER_MST_FK = " + LanguagePK;

			WorkFlow objWF = new WorkFlow();

			try {
				return objWF.ExecuteScaler(StrSql);
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		#endregion
		#region "FETCH STYLE SHEET"
		public string FetchStyleSheet(Int32 UserPK)
		{
			string StrSql = null;
			StrSql = StrSql + " SELECT UPT.USER_STYLE_SHEET FROM  USER_PREFERENCE_TBL UPT  WHERE UPT.USER_MST_FK=" + UserPK;
			WorkFlow objWF = new WorkFlow();
			try {
				return objWF.ExecuteScaler(StrSql);
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		#endregion
		//Public Function Update(ByVal User_PrefenceVal As Long, _
		//   ByVal User_PreFMstFk As String, _
		//   ByVal User_MstFk As String) As Boolean

		//   Dim strSql as String
		//    strsql=""
		//    strsql = strsql & vbCrLf &  " Update user_preference_tbl t set t.user_preference_value = 2"
		//    strsql = strsql & vbCrLf &  " Where t.preference_mst_fk=1"
		//    strsql = strsql & vbCrLf &  " And t.user_mst_fk =3"


		//    Try
		//        TRAN = objWK.MyConnection.BeginTransaction()
		//'        
		//        With objWK.MyDataAdapter
		//            .UpdateCommand = updCommand
		//            .UpdateCommand.Transaction = TRAN
		//            .UpdateCommand.ExecuteNonQuery()

		//            TRAN.Commit()
		//            Return True
		//        End With
		//    Catch oraexp As OracleException
		//        Throw oraexp
		//    Catch ex As Exception
		//        Throw ex
		//    End Try
		//    Return False
		//End Function
		public int IsUserAvailable(Int32 UserPK)
		{
			string StrSql = null;
			StrSql = StrSql + " SELECT COUNT(*) FROM  User_Preference_Tbl Upt WHERE Upt.USER_MST_FK=" + UserPK;
			WorkFlow objWF = new WorkFlow();
			try {
				return Convert.ToInt32(objWF.ExecuteScaler(StrSql));
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		public string FetchNumberOfRecords(Int32 UserPK)
		{
			string StrSql = null;
			StrSql = StrSql + " SELECT Upt.USER_GRIDRECORDS FROM  User_Preference_Tbl Upt WHERE Upt.USER_MST_FK=" + UserPK;
			WorkFlow objWF = new WorkFlow();
			try {
				return objWF.ExecuteScaler(StrSql);
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		//Function Added By Anand G  Reason:To Fetch preference days  for the logged in user 13-05-08
		public string FetchDateLoad(Int32 UserPK)
		{
			string StrSql = null;
			string strDateLoad = null;
			StrSql = StrSql + " SELECT Upt.DATE_ONLOAD FROM  User_Preference_Tbl Upt WHERE Upt.USER_MST_FK=" + UserPK;
			WorkFlow objWF = new WorkFlow();
			try {
				strDateLoad = objWF.ExecuteScaler(StrSql);
				return strDateLoad;
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		//Function Added By Anand G  Reason:To Fetch preference days for the logged in user 13-05-08
		//modifying by thiyagarajan on 10/2/09 : implementing logged loc. time from server's time using GMT
		public string FetchPrefDate(Int32 UserPK)
		{
			string StrSql = null;
			string strDate = null;
			string strNoOfDays = null;
			string strNoOfDays1 = null;
			StrSql = StrSql + " SELECT Upt.PREFERENCE_DAYS FROM  User_Preference_Tbl Upt WHERE Upt.USER_MST_FK=" + UserPK;
			WorkFlow objWF = new WorkFlow();
			try {
				strNoOfDays = "-";
				strNoOfDays1 = objWF.ExecuteScaler(StrSql);
				//strNoOfDays = strNoOfDays.Concat(strNoOfDays, strNoOfDays1);
				//strDate = System.DateTime.Today.ToString();
				////modifying by thiyagarajan on 10/2/09 
				//strDate = DateTime.Now.AddDays(strNoOfDays);
				//dt.AddDays(strNoOfDays)-parameter 
				//end
				strDate = Convert.ToDateTime(strDate).Day + "/" + Convert.ToDateTime(strDate).Month + "/" + Convert.ToDateTime(strDate).Year;
				return strDate;
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}

		public string FetchListOnLoad(Int32 UserPK)
		{
			string StrSql = null;
			StrSql = StrSql + " SELECT UPT.LIST_ONLOAD FROM  USER_PREFERENCE_TBL UPT WHERE UPT.USER_MST_FK=" + UserPK;
			WorkFlow objWF = new WorkFlow();
			try {
				return objWF.ExecuteScaler(StrSql);
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		public string FetchDateRangeOnLoad(Int32 UserPK)
		{
			string StrSql = null;
			StrSql = StrSql + " SELECT UPT.DATE_RANGE FROM  USER_PREFERENCE_TBL UPT WHERE UPT.USER_MST_FK=" + UserPK;
			WorkFlow objWF = new WorkFlow();
			try {
				return objWF.ExecuteScaler(StrSql);
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		public string FetchUserStyleSheet(Int32 UserPK)
		{
			string StrSql = null;
			StrSql = StrSql + " SELECT upt.User_style_sheet FROM  User_Preference_Tbl Upt WHERE Upt.USER_MST_FK=" + UserPK;
			WorkFlow objWF = new WorkFlow();
			try {
				return objWF.ExecuteScaler(StrSql);
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		public string FetchUserPrefPk(string LocationId)
		{
			string StrSql = null;
			StrSql = StrSql + " SELECT Upt.USER_PREFERENCES_PK FROM  User_Preference_Tbl Upt WHERE Upt.USER_MST_FK=" + LocationId;
			WorkFlow objWF = new WorkFlow();
			try {
				return objWF.ExecuteScaler(StrSql);
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		public ArrayList UpdateUserPrefData(DataSet M_DataSet)
		{
			WorkFlow objWK = new WorkFlow();
			objWK.OpenConnection();
			OracleTransaction TRAN = null;
			TRAN = objWK.MyConnection.BeginTransaction();
			string strPKVal = null;
			long lngI = 0;
			Int32 RecAfct = default(Int32);
			OracleCommand insCommand = new OracleCommand();
			OracleCommand updCommand = new OracleCommand();
			OracleCommand delCommand = new OracleCommand();

			try {
				int i = 0;
				var _with1 = updCommand;
				_with1.Connection = objWK.MyConnection;
				_with1.CommandType = CommandType.StoredProcedure;
				_with1.CommandText = objWK.MyUserName + ".USER_PREFERENCES_TBL_PKG.USER_PREFERENCES_TBL_UPD";
				DataRow drPref = M_DataSet.Tables[0].Rows[0];
				var _with2 = _with1.Parameters;
				_with2.Add("USER_PREFERENCES_PK_IN", drPref["USER_PREFERENCES_PK"]).Direction = ParameterDirection.Input;
				// updCommand.Parameters["USER_PREFERENCES_PK_IN"].SourceVersion = DataRowVersion.Current
				_with2.Add("USER_MST_FK_IN", drPref["USER_MST_FK"]).Direction = ParameterDirection.Input;
				//updCommand.Parameters["USER_MST_FK_IN"].SourceVersion = DataRowVersion.Current
				_with2.Add("USER_STYLE_SHEET_IN", drPref["USER_STYLE_SHEET"]).Direction = ParameterDirection.Input;
				//updCommand.Parameters["USER_STYLE_SHEET_IN"].SourceVersion = DataRowVersion.Current
				_with2.Add("PREFERENCE_DAYS_IN", drPref["PREFERENCE_DAYS"]).Direction = ParameterDirection.Input;
				// Added By Anand To Update Preference Days 12-05-08

				_with2.Add("PASSWORD_EXPIRY_DT_IN", drPref["PASSWORD_EXPIRY_DT"]).Direction = ParameterDirection.Input;
				// Added By Anand To Update Preference Days 12-05-08
				_with2.Add("EXP_NOTIFICATION_DT_IN", drPref["EXP_NOTIFICATION_DT"]).Direction = ParameterDirection.Input;
				// Added By Anand To Update Preference Days 12-05-08

				_with2.Add("LOCATION_MST_FK_IN", drPref["DEFAULT_LOCATION_FK"]).Direction = ParameterDirection.Input;
				//updCommand.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current
				_with2.Add("USER_DATE_IN", drPref["USER_DATE"]).Direction = ParameterDirection.Input;
				// updCommand.Parameters.Add("USER_STYLE_SHEET_IN", drPref.Item("USER_STYLE_SHEET")).Direction = ParameterDirection.Input
				//updCommand.Parameters["USER_DATE_IN"].SourceVersion = DataRowVersion.Current
				_with2.Add("USER_GRIDRECORDS_IN", drPref["USER_GRIDRECORDS"]).Direction = ParameterDirection.Input;
				//updCommand.Parameters["USER_GRIDRECORDS_IN"].SourceVersion = DataRowVersion.Current
				_with2.Add("LAST_MODIFIED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
				_with2.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
				_with2.Add("VERSION_NO_IN", drPref["VERSION_NO"]).Direction = ParameterDirection.Input;
				//Added by Shankar as on 09-01-07 for adding Active CheckBox
				_with2.Add("list_onload_in", drPref["LIST_ONLOAD"]).Direction = ParameterDirection.Input;
				_with2.Add("DATE_ONLOAD_IN", drPref["DATE_ONLOAD"]).Direction = ParameterDirection.Input;
				//updCommand.Parameters.Add("DATE_RANGE_IN", drPref.Item("DATE_RANGE")).Direction = ParameterDirection.Input
				// updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current
				updCommand.Parameters.Add("CONTR_EXPIRY_ALERT_DAYS_IN", drPref["CONTR_EXPIRY_ALERT_DAYS"]).Direction = ParameterDirection.Input;
				///' Added by Rajesh, 31/10/2009
				updCommand.Parameters.Add("ENVIRONMENT_FK_IN", drPref["ENVIRONMENT_FK"]).Direction = ParameterDirection.Input;

				_with2.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
				//updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current


				var _with3 = objWK.MyDataAdapter;
				_with3.UpdateCommand = updCommand;
				_with3.UpdateCommand.Transaction = TRAN;
				RecAfct = _with3.Update(M_DataSet);
				if (RecAfct > 0) {
					TRAN.Commit();
					arrMessage.Add("All Data Saved Successfully");
					return arrMessage;
				} else {
					TRAN.Rollback();
					return arrMessage;
				}

			} catch (OracleException oraexp) {
				TRAN.Rollback();
				throw oraexp;
			} catch (Exception ex) {
				TRAN.Rollback();
				throw ex;
			} finally {
				objWK.CloseConnection();
				//added by surya prasad on 16-02-2009
			}
		}
		public ArrayList InsertUserPrefData(int userMstFk, string StyleSheet, int DefLocation, System.DateTime UserDate, int GridRecords, int NoOfDays, int PassExp, int daysAlert, int DateOnLoad, int ListOnLoad,
		int RateExpAlertDays = 0)
		{
			WorkFlow objWK = new WorkFlow();
			objWK.OpenConnection();
			OracleTransaction TRAN = null;
			TRAN = objWK.MyConnection.BeginTransaction();
			string strPKVal = null;
			long lngI = 0;
			int userPreferencePk = 0;
			Int32 RecAfct = default(Int32);
			OracleCommand insCommand = new OracleCommand();

			try {
				int i = 0;
				var _with4 = insCommand;
				_with4.Connection = objWK.MyConnection;
				_with4.CommandType = CommandType.StoredProcedure;
				_with4.CommandText = objWK.MyUserName + ".USER_PREFERENCES_TBL_PKG.USER_PREFERENCES_TBL_INS";

				var _with5 = _with4.Parameters;
				_with5.Add("USER_MST_FK_IN", userMstFk).Direction = ParameterDirection.Input;
				_with5.Add("USER_STYLE_SHEET_IN", StyleSheet).Direction = ParameterDirection.Input;
				_with5.Add("LOCATION_MST_FK_IN", DefLocation).Direction = ParameterDirection.Input;
				_with5.Add("USER_DATE_IN", UserDate).Direction = ParameterDirection.Input;
				_with5.Add("USER_GRIDRECORDS_IN", GridRecords).Direction = ParameterDirection.Input;
				_with5.Add("PREFERENCE_DAYS_IN", NoOfDays).Direction = ParameterDirection.Input;
				_with5.Add("PASSWORD_EXPIRY_DT_IN", PassExp).Direction = ParameterDirection.Input;
				_with5.Add("EXP_NOTIFICATION_DT_IN", daysAlert).Direction = ParameterDirection.Input;
				_with5.Add("CONTR_EXPIRY_ALERT_DAYS_IN", RateExpAlertDays).Direction = ParameterDirection.Input;
				_with5.Add("LAST_MODIFIED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
				_with5.Add("list_onload_in", ListOnLoad).Direction = ParameterDirection.Input;
				_with5.Add("DATE_ONLOAD_IN", DateOnLoad).Direction = ParameterDirection.Input;
				_with5.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
				_with5.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
				_with5.Add("ENVIRONMENT_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
				_with5.Add("RETURN_VALUE", userPreferencePk).Direction = ParameterDirection.Output;


				var _with6 = objWK.MyDataAdapter;
				_with6.InsertCommand = insCommand;
				_with6.InsertCommand.Transaction = TRAN;
				_with6.InsertCommand.ExecuteNonQuery();
				TRAN.Commit();
				if (arrMessage.Count > 0) {
					return arrMessage;
				} else {
					arrMessage.Add("All Data Saved Successfully");
					return arrMessage;
				}
			} catch (OracleException oraexp) {
				TRAN.Rollback();
				throw oraexp;
			} catch (Exception ex) {
				TRAN.Rollback();
				throw ex;
			} finally {
				objWK.CloseConnection();
				//added by surya prasad on 16-02-2009
			}
		}
		//Public Sub UpDateUserPref(ByVal Usr_Mst_Fk_Val As Integer, ByVal Pref_Mst_Pk As Integer, ByVal Loc_Mst_Fk As Int32, ByVal Number_of_Records As Int32, ByVal User_StyleSheet As String)

		//    Dim objWf As New WorkFlow

		//    Dim TRAN As OracleTransaction

		//    Dim updCommand As New OracleClient.OracleCommand
		//    Dim ColPara As New OracleClient.OracleParameterCollection

		//    Dim RowCnt As Int32

		//    Dim RecAfct As Integer
		//    Dim Ret_Value As Integer

		//    Try
		//        With updCommand
		//            objWf.OpenConnection()
		//            .Connection = objWf.MyConnection
		//            .CommandType = CommandType.StoredProcedure
		//            .CommandText = objWf.MyUserName & ".USER_PREFERENCES_PKG.USER_PREFERENCES_UPD"
		//            TRAN = objWf.MyConnection.BeginTransaction()

		//            updCommand.Parameters.Add("USER_PREFERENCES_PK_IN", Usr_Mst_Fk_Val).Direction = ParameterDirection.Input
		//            updCommand.Parameters.Add("USER_MST_FK_IN", Pref_Mst_Pk).Direction = ParameterDirection.Input
		//            updCommand.Parameters.Add("USER_STYLE_SHEET_IN", Loc_Mst_Fk).Direction = ParameterDirection.Input
		//            updCommand.Parameters.Add("USER_DATE_IN", Usr_Mst_Fk_Val).Direction = ParameterDirection.Input
		//            updCommand.Parameters.Add("USER_GRIDRECORDS_IN", Number_of_Records).Direction = ParameterDirection.Input
		//            updCommand.Parameters.Add("USER_GRIDRECORDS_IN", Number_of_Records).Direction = ParameterDirection.Input
		//            updCommand.Parameters.Add("VERSION_NO_IN", User_StyleSheet).Direction = ParameterDirection.Input



		//            With objWf.MyDataAdapter
		//                .UpdateCommand = updCommand
		//                .UpdateCommand.Transaction = TRAN
		//                .UpdateCommand.ExecuteNonQuery()
		//                TRAN.Commit()
		//            End With
		//        End With
		//    Catch ex As Exception

		//    End Try

		//End Sub

	}
}

