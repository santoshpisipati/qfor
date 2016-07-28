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

    #region "Enum"
    public enum NavigationLocation
	{
		FirstRecord = 0,
		NextRecord = 1,
		PreviousRecord = 2,
		LastRecord = 3,
		None = 4
	}
	#endregion

	public class clsLocation_Type_Mst_Tbl : CommonFeatures
	{

		#region "List of Members of the Class"
		private Int16 M_Location_Type_Mst_Pk;
		private string M_Location_Type_Id;
			#endregion
		private string M_Location_Type_Desc;

		#region "List of Properties"
		public Int16 Location_Type_Mst_Pk {
			get { return M_Location_Type_Mst_Pk; }
			set { M_Location_Type_Mst_Pk = value; }
		}

		public string Location_Type_Id {
			get { return M_Location_Type_Id; }
			set { M_Location_Type_Id = value; }
		}

		public string Location_Type_Desc {
			get { return M_Location_Type_Desc; }
			set { M_Location_Type_Desc = value; }
		}
		#endregion

		#region "Insert Function"
		public int Insert()
		{
			WorkFlow objWS = new WorkFlow();
			Int32 intPkVal = default(Int32);
			try {
				objWS.MyCommand.CommandType = CommandType.StoredProcedure;
				var _with1 = objWS.MyCommand.Parameters;
				_with1.Add("Location_Type_Id_IN", M_Location_Type_Id).Direction = ParameterDirection.Input;
				_with1.Add("Location_Type_Desc_IN", M_Location_Type_Desc).Direction = ParameterDirection.Input;
				_with1.Add("RETURN_VALUE", intPkVal).Direction = ParameterDirection.Output;
				objWS.MyCommand.CommandText = objWS.MyUserName.Trim() + ".Location_Type_MST_TBL_PKG.Location_Type_Mst_Tbl_Ins";
				if (objWS.ExecuteCommands() == true) {
					return intPkVal;
				} else {
					return -1;
				}
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Update Function"
		public int Update()
		{
			WorkFlow objWS = new WorkFlow();
			Int32 intPkVal = default(Int32);
			try {
				objWS.MyCommand.CommandType = CommandType.StoredProcedure;
				var _with2 = objWS.MyCommand.Parameters;
				_with2.Add("Location_Type_Mst_Pk_IN", M_Location_Type_Mst_Pk).Direction = ParameterDirection.Input;
				_with2.Add("Location_Type_Id_IN", M_Location_Type_Id).Direction = ParameterDirection.Input;
				_with2.Add("Location_Type_Desc_IN", M_Location_Type_Desc).Direction = ParameterDirection.Input;
				objWS.MyCommand.CommandText = "FEEDERUSER.LOCATION_TYPE_MST_TBL_PKG.Location_Type_Mst_Tbl_UPD";
				if (objWS.ExecuteCommands() == true) {
					return 1;
				} else {
					return -1;
				}
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Delete Function"
		public int Delete()
		{
			WorkFlow objWS = new WorkFlow();
			Int32 intPkVal = default(Int32);
			try {
				objWS.MyCommand.CommandType = CommandType.StoredProcedure;
				var _with3 = objWS.MyCommand.Parameters;
				objWS.MyCommand.CommandText = "Location_Type_Mst_Tbl_DEL";
				if (objWS.ExecuteCommands() == true) {
					return 1;
				} else {
					return -1;
				}
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion


		#region "Fetch Location Type"
		public DataSet FetchUserWiseLocation()
		{
			string strSQL = null;
			WorkFlow objWF = new WorkFlow();
			DataSet objDS = null;

			strSQL = string.Empty ;
			strSQL += "select   " ;
			strSQL += " l.location_mst_pk,  " ;
			strSQL += " l.location_name  " ;
			strSQL += " from   " ;
			strSQL += " location_mst_tbl l  " ;

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


		public bool Fetch(NavigationLocation Navigation = NavigationLocation.None, Int16 CurrentPKValue = 0, Int16 LocationTypeMSTPK = 0, string LocationTypeID = "", string LocationTypeDesc = "")
		{
			string strSQL = null;


			strSQL = string.Empty ;
			strSQL += "select  " ;
			strSQL += "Location_Type_mst_pk, " ;
			strSQL += "location_type_id, " ;
			strSQL += "location_type_desc " ;
			strSQL += "from  " ;
			strSQL += "location_type_mst_tbl " ;
			strSQL = strSQL + "WHERE 1=1" ;
			strSQL += " " ;

			if (LocationTypeMSTPK > 0) {
				strSQL = strSQL + " AND Location_Type_mst_pk=" + LocationTypeMSTPK;
			}
			if (LocationTypeID.Length > 0) {
				strSQL = strSQL + " AND location_type_id=" + LocationTypeID;
			}
			if (LocationTypeDesc.Length > 0) {
				strSQL = strSQL + " AND location_type_desc=" + LocationTypeDesc;
			}


			//if (Navigation != NavigationType.None) {
			//	switch (Navigation) {
			//		case NavigationLocation.FirstRecord:
			//			strSQL = strSQL + " AND Location_Type_mst_pk=(SELECT MIN(Location_Type_mst_pk) FROM location_type_mst_tbl)";
			//			break;
			//		case NavigationLocation.PreviousRecord:
			//			strSQL = strSQL + " AND Location_Type_mst_pk < " + CurrentPKValue + " ORDER BY Location_Type_mst_pk DESC";
			//			break;
			//		case NavigationLocation.NextRecord:
			//			strSQL = strSQL + " AND Location_Type_mst_pk > " + CurrentPKValue + " ORDER BY Location_Type_mst_pk";
			//			break;
			//		case NavigationLocation.LastRecord:
			//			strSQL = strSQL + " AND Location_Type_mst_pk=(SELECT MAX(Location_Type_mst_pk) FROM location_type_mst_tbl)";
			//			break;
			//	}
			//}

			WorkFlow objWF = new WorkFlow();
			DataSet objDS = new DataSet();

			try {
				objDS = objWF.GetDataSet(strSQL);
				if (objDS.Tables[0].Rows.Count > 0) {
					M_Location_Type_Mst_Pk = (Int16)objDS.Tables[0].Rows[0]["Location_Type_mst_pk"];
					M_Location_Type_Id = objDS.Tables[0].Rows[0]["Location_Type_ID"].ToString();
					M_Location_Type_Desc = objDS.Tables[0].Rows[0]["location_type_desc"].ToString();
					return true;
				} else {
					return false;
				}


				//Catch sqlExp As OracleException
			//Modified by Manjunath  PTS ID:Sep-02  14/09/2011
			} catch (OracleException sqlExp) {
				throw sqlExp;
			} catch (Exception exp) {
				throw exp;
			}
		}

		#endregion

		#region "FetchAll"
		public OracleDataReader FetchAll(Int16 LocationTypeMSTPK = 0, string LocationTypeID = "", string LocationTypeDesc = "")
		{

			string strSQL = null;
			strSQL = string.Empty ;
			strSQL += "select  " ;
			strSQL += "Location_Type_mst_pk, " ;
			strSQL = strSQL + "ROWNUM SR_NO," ;
			strSQL += "location_type_id, " ;
			strSQL += "location_type_desc " ;
			strSQL += "from  " ;
			strSQL += "location_type_mst_tbl " ;
			strSQL = strSQL + "WHERE 1=1" ;
			strSQL += " " ;

			if (LocationTypeMSTPK > 0) {
				strSQL = strSQL + " AND LOCATION_TYPE_MST_PK=" + LocationTypeMSTPK;
			}
			if (LocationTypeID.Length > 0) {
				strSQL = strSQL + " AND UPPER(LOCATION_TYPE_ID) LIKE '%" + LocationTypeID.ToUpper().Replace("'", "''") + "%'" ;
			}
			if (LocationTypeDesc.Trim().Length > 0) {
				strSQL = strSQL + " AND UPPER(LOCATION_TYPE_DESC) LIKE '%" + LocationTypeDesc.ToUpper().Replace("'", "''") + "%'" ;
			}
			WorkFlow objWF = new WorkFlow();
			OracleDataReader objDR = null;
			try {
				objDR = objWF.GetDataReader(strSQL);
				if (objDR.HasRows == false) {
					return null;
				}
				return objDR;
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		#endregion

		#region "FetchLocation_type Function"
		public DataSet FetchLocation_type(Int64 P_Location_Type_Mst_Pk = 0, string P_Location_Type_Id = "", string P_Location_Type_Desc = "", string SearchType = "", string strColumnName = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int16 SortCol = 0, Int16 IsActive = 0, bool blnSortAscending = false,
		Int32 flag = 0)
		{
			Int32 last = default(Int32);
			Int32 start = default(Int32);
			string strSQL = null;
			string strCondition = null;
			Int32 TotalRecords = default(Int32);
			WorkFlow objWF = new WorkFlow();
			if (flag == 0) {
				strCondition += " AND 1=2";
			}
			if (P_Location_Type_Mst_Pk > 0) {
				strCondition += " AND location_type_mst_pk =" + P_Location_Type_Mst_Pk;
			}

			if (P_Location_Type_Id.Trim().Length > 0) {
				if (SearchType.ToString().Trim().Length > 0) {
					if (SearchType == "S") {
						strCondition += " AND UPPER(Location_Type_Id) LIKE '" + P_Location_Type_Id.ToUpper().Replace("'", "''") + "%'" ;
					} else {
						strCondition += " AND UPPER(Location_Type_Id) LIKE '%" + P_Location_Type_Id.ToUpper().Replace("'", "''") + "%'" ;
					}
				} else {
					strCondition += " AND UPPER(Location_Type_Id) LIKE '%" + P_Location_Type_Id.ToUpper().Replace("'", "''") + "%'" ;
				}
			}

			if (P_Location_Type_Desc.Trim().Length > 0) {
				if (SearchType.ToString().Trim().Length > 0) {
					if (SearchType == "S") {
						strCondition += " AND UPPER(Location_Type_Desc) LIKE '" + P_Location_Type_Desc.ToUpper().Replace("'", "''") + "%'" ;
					} else {
						strCondition += " AND UPPER(Location_Type_Desc) LIKE '%" + P_Location_Type_Desc.ToUpper().Replace("'", "''") + "%'" ;
					}
				} else {
					strCondition += " AND UPPER(Location_Type_Desc) LIKE '%" + P_Location_Type_Desc.ToUpper().Replace("'", "''") + "%'" ;
				}
			}


			if (IsActive == 1) {
				strCondition += " AND ACTIVE_FLAG =1 ";
			}

			strSQL = "SELECT Count(*) from LOCATION_TYPE_MST_TBL where 1=1";
			strSQL += strCondition;
			TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
			TotalPage = TotalRecords / RecordsPerPage;
			if (TotalRecords % RecordsPerPage != 0) {
				TotalPage += 1;
			}
			if (CurrentPage > TotalPage) {
				CurrentPage = 1;
			}
			if (TotalRecords == 0) {
				CurrentPage = 0;
			}
			last = CurrentPage * RecordsPerPage;
			start = (CurrentPage - 1) * RecordsPerPage + 1;

			strSQL = " select * from (";
			strSQL += "SELECT ROWNUM SR_NO,q.* FROM ";
			strSQL = strSQL + "(SELECT  ";
			strSQL = strSQL + " Location_Type_Mst_Pk,ACTIVE_FLAG,";
			strSQL = strSQL + " Location_Type_Id,";
			strSQL = strSQL + " Location_Type_Desc,";
			strSQL = strSQL + " Version_No ";
			strSQL = strSQL + " FROM LOCATION_TYPE_MST_TBL ";
			strSQL = strSQL + " WHERE 1 = 1 ";

			strSQL += strCondition;

			if (!strColumnName.Equals("SR_NO")) {
				strSQL += "order by " + strColumnName;
			}

			if (!blnSortAscending & !strColumnName.Equals("SR_NO")) {
				strSQL += " DESC";
			}

			strSQL += " )q) WHERE SR_NO  Between " + start + " and " + last;

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

		#region "Save Function"
		public ArrayList Save(DataSet M_DataSet, bool Import = false)
		{
			//sivachandran 05Jun08 Imp-Exp-Wiz 16May08
			WorkFlow objWK = new WorkFlow();
			objWK.OpenConnection();
			OracleTransaction TRAN = null;
			TRAN = objWK.MyConnection.BeginTransaction();

			int intPKVal = 0;
			long lngI = 0;
			Int32 RecAfct = default(Int32);
			OracleCommand insCommand = new OracleCommand();
			OracleCommand updCommand = new OracleCommand();
			OracleCommand delCommand = new OracleCommand();
			try {
				DataTable DtTbl = new DataTable();
				DataRow DtRw = null;
				int i = 0;
				DtTbl = M_DataSet.Tables[0];
				var _with4 = insCommand;
				_with4.Connection = objWK.MyConnection;
				_with4.CommandType = CommandType.StoredProcedure;
				_with4.CommandText = objWK.MyUserName + ".LOCATION_TYPE_MST_TBL_PKG.LOCATION_TYPE_MST_TBL_INS";
				var _with5 = _with4.Parameters;

				insCommand.Parameters.Add("LOCATION_TYPE_ID_IN", OracleDbType.Varchar2, 0, "LOCATION_TYPE_ID").Direction = ParameterDirection.Input;
				insCommand.Parameters["LOCATION_TYPE_ID_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("LOCATION_TYPE_DESC_IN", OracleDbType.Varchar2, 0, "LOCATION_TYPE_DESC").Direction = ParameterDirection.Input;
				insCommand.Parameters["LOCATION_TYPE_DESC_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
				insCommand.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
				insCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

				insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "LOCATION_TYPE_MST_TBL_PK").Direction = ParameterDirection.Output;
				insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

				var _with6 = updCommand;
				_with6.Connection = objWK.MyConnection;
				_with6.CommandType = CommandType.StoredProcedure;
				_with6.CommandText = objWK.MyUserName + ".LOCATION_TYPE_MST_TBL_PKG.LOCATION_TYPE_MST_TBL_UPD";
				var _with7 = _with6.Parameters;

				updCommand.Parameters.Add("LOCATION_TYPE_MST_PK_IN", OracleDbType.Int32, 10, "LOCATION_TYPE_MST_PK").Direction = ParameterDirection.Input;
				updCommand.Parameters["LOCATION_TYPE_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("LOCATION_TYPE_ID_IN", OracleDbType.Varchar2, 0, "LOCATION_TYPE_ID").Direction = ParameterDirection.Input;
				updCommand.Parameters["LOCATION_TYPE_ID_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("LOCATION_TYPE_DESC_IN", OracleDbType.Varchar2, 0, "LOCATION_TYPE_DESC").Direction = ParameterDirection.Input;
				updCommand.Parameters["LOCATION_TYPE_DESC_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

				updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
				updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
				updCommand.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
				updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
				updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				var _with8 = objWK.MyDataAdapter;

				_with8.InsertCommand = insCommand;
				_with8.InsertCommand.Transaction = TRAN;
				_with8.UpdateCommand = updCommand;
				_with8.UpdateCommand.Transaction = TRAN;

				RecAfct = _with8.Update(M_DataSet);
				if (arrMessage.Count > 0) {
					TRAN.Rollback();
					return arrMessage;
				} else {
					TRAN.Commit();
					if (Import == false) {
						arrMessage.Add("All Data Saved Successfully");
					} else {
						arrMessage.Add("Data Imported Successfully");
					}
					return arrMessage;
				}
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				objWK.CloseConnection();
			}
		}
		#endregion

		#region "Fetch LocationType"
		public DataSet Fetch_LocationType(Int16 LocationTypeMSTPK = 0, string LocationTypeID = "", string LocationTypeDesc = "")
		{

			string strSQL = null;
			strSQL = " SELECT ' ' LOCATION_TYPE_ID,";
			strSQL += " ' ' LOCATION_TYPE_DESC,";
			strSQL += " 0 LOCATION_TYPE_MST_PK";
			strSQL += " FROM LOCATION_TYPE_MST_TBL ";
			strSQL += " WHERE ACTIVE_FLAG = 1";
			strSQL += " UNION ";
			strSQL += "SELECT  ";
			strSQL += " LOCATION_TYPE_ID, ";
			strSQL += " LOCATION_TYPE_DESC, ";
			strSQL += " LOCATION_TYPE_MST_PK ";
			strSQL += " FROM LOCATION_TYPE_MST_TBL ";
			strSQL += " WHERE ACTIVE_FLAG = 1";
			if (LocationTypeMSTPK > 0) {
				strSQL = strSQL + " AND LOCATION_TYPE_MST_PK=" + LocationTypeMSTPK;
			}
			if (LocationTypeID.Length > 0) {
				strSQL = strSQL + " AND UPPER(LOCATION_TYPE_ID) LIKE '%" + LocationTypeID.ToUpper() + "%'";
			}
			if (LocationTypeDesc.Trim().Length > 0) {
				strSQL = strSQL + " AND UPPER(LOCATION_TYPE_DESC) LIKE '%" + LocationTypeDesc.ToUpper() + "%'";
			}
			strSQL = strSQL + " order by LOCATION_TYPE_DESC";

			WorkFlow objWF = new WorkFlow();
			DataSet objDS = null;
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

		#region "Fetch Location Popup Function "
		public DataSet FetchLocationIdName(string LocationPk = "0", string LocationID = "", string LocationName = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, bool ActiveFlag = true, string SortType = " ASC ", string chkall = "0", bool fetchAll = false, string Loggedinpk = "")
		{

			Int32 last = default(Int32);
			Int32 start = default(Int32);
			string strSQL = "";
			string strCondition = "";
			string strCondition1 = "";
			string strCondition2 = "";
			string strCondition3 = "";
			string strCondition4 = "";
			Int32 TotalRecords = default(Int32);
			WorkFlow objWF = new WorkFlow();

			if (LocationID == "Country ID") {
				if (LocationName.Trim().Length > 0) {
					strCondition = strCondition + " AND UPPER(LOCATION_ID) LIKE '%" + LocationName.ToUpper().Replace("'", "''") + "%'";
				}
			} else {
				if (LocationName.Trim().Length > 0) {
					strCondition = strCondition + " AND UPPER(LOCATION_NAME) LIKE '%" + LocationName.ToUpper().Replace("'", "''") + "%'";
				}
			}
			if (LocationPk != "null" & LocationPk != "0") {
				if (LocationPk.Length > 0) {
					strCondition1 += "and LOCATION_MST_PK not in (" + LocationPk + " )";
					strCondition2 += "and LOCATION_MST_PK in (" + LocationPk + " )";
				}
			}
			if (chkall != "1" & chkall != "0") {

				strCondition3 += " and LMT.country_mst_fk in (" + chkall + ")";
			}
			if (Loggedinpk != "null" & Loggedinpk != "0") {
				strCondition4 += " and LMT.LOCATION_MST_PK in (select L.LOCATION_MST_PK FROM LOCATION_MST_TBL L START WITH L.LOCATION_MST_PK in(" + Loggedinpk + " ) CONNECT BY PRIOR L.LOCATION_MST_PK=L.REPORTING_TO_FK)";

			}

			strCondition += " AND LMT.ACTIVE_FLAG = 1 ";
			strSQL = "SELECT Count(*) from LOCATION_MST_TBL LMT ";
			strSQL = strSQL + " WHERE 1=1 ";
			strSQL += strCondition + strCondition3;
			TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
			TotalPage = TotalRecords / M_MasterPageSize;
			if (TotalRecords % M_MasterPageSize != 0) {
				TotalPage += 1;
			}
			if (CurrentPage > TotalPage) {
				CurrentPage = 1;
			}
			if (TotalRecords == 0) {
				CurrentPage = 0;
			}
			last = CurrentPage * M_MasterPageSize;
			start = (CurrentPage - 1) * M_MasterPageSize + 1;

			strSQL = " select * from (";
			strSQL += "SELECT ROWNUM SR_NO,q.* FROM (SELECT * FROM (";
			if (!string.IsNullOrEmpty(strCondition2)) {
				strSQL += " SELECT ";
				strSQL += "LOCATION_MST_PK, ";
				strSQL += "LOCATION_ID, ";
				strSQL += "UPPER(LOCATION_NAME) LOCATION_NAME, ";
				strSQL += " 'true' active";
				strSQL += "FROM LOCATION_MST_TBL LMT ";
				strSQL += "WHERE  1=1";
				strSQL += strCondition2;
				strSQL += strCondition + strCondition3 + strCondition4;
				strSQL += " union ";
			}

			strSQL += " SELECT ";
			strSQL += "LOCATION_MST_PK, ";
			strSQL += "LOCATION_ID, ";
			strSQL += "UPPER(LOCATION_NAME) LOCATION_NAME, ";
			strSQL += " 'false' active";
			strSQL += "FROM LOCATION_MST_TBL LMT ";
			strSQL += "WHERE  1=1";
			strSQL += strCondition1;
			strSQL += strCondition + strCondition3 + strCondition4;
			strSQL += " order by LOCATION_ID ) ORDER BY  ACTIVE DESC ,LOCATION_ID)Q) ";
			if (fetchAll == false) {
				strSQL += " WHERE SR_NO  Between " + start + " and " + last;
			}
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

	}

}
