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

using Newtonsoft.Json;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections;
using System.Data;
using System.Web;

namespace Quantum_QFOR
{
    public class cls_ROLE_MST_TBL : CommonFeatures
	{
		DataSet M_GridDS;
		public DataSet GRIDDS {
			get { return M_GridDS; }
			set { M_GridDS = value; }
		}

		#region "Fetch Function"
		public DataSet FetchAll(Int64 P_Role_Mst_Tbl_Pk, string P_Role_Id, string P_Role_Description, Int64 P_Active_Flag, string SearchType = "", string SortExpression = "")
		{
			string strSQL = null;
			strSQL = "SELECT ROWNUM SR_NO, ";
			strSQL = strSQL + " Role_Mst_Tbl_Pk,";
			strSQL = strSQL + " Role_Id,";
			strSQL = strSQL + " Role_Description,";
			strSQL = strSQL + " Active_Flag,";
			strSQL = strSQL + " Version_No ";
			strSQL = strSQL + " FROM ROLE_MST_TBL ";
			strSQL = strSQL + " WHERE ( 1 = 1) ";
			if (P_Role_Mst_Tbl_Pk > 0) {
				strSQL = strSQL + " And Role_Mst_Tbl_Pk = " + P_Role_Mst_Tbl_Pk + " ";
			}
			if (P_Role_Id.ToString().Trim().Length > 0) {
				if (SearchType == "C") {
					strSQL = strSQL + " And Role_Id like '%" + P_Role_Id + "%' ";
				} else {
					strSQL = strSQL + " And Role_Id like '" + P_Role_Id + "%' ";
				}
			} else {
			}
			if (P_Role_Description.ToString().Trim().Length > 0) {
				if (SearchType == "C") {
					strSQL = strSQL + " And Role_Description like '%" + P_Role_Description + "%' ";
				} else {
					strSQL = strSQL + " And Role_Description like '" + P_Role_Description + "%' ";
				}
			} else {
			}
			if (P_Active_Flag > 0) {
				strSQL = strSQL + " And Active_Flag = " + P_Active_Flag + " ";
			}
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

		#region "Fetch Menu"
		public DataSet FetchMenu(Int32 Val)
		{
			string strSQL = null;
			strSQL = "SELECT ROWNUM SLNO,QRY.* FROM ( ";
			strSQL = strSQL + " SELECT NVL(USRROLE.ROLE_ACCESS_TRN_PK,0) PK,";
			strSQL = strSQL + " MST.MENU_MST_PK MENU_PK,  ";
			strSQL = strSQL + " NVL(MTXT.MENU_TEXT,'')  MENU_TEXT ,  ";
			strSQL = strSQL + " NVL(MST1.MENU_ID,'')  MENU_PARENT ,";
			strSQL = strSQL + " MST.DISPLAY_ORDER,";
			strSQL = strSQL + " MST.MENU_LEVEL ,";
			strSQL = strSQL + " NVL(MST.CONFIG_ID_FK,0) CONFIG_ID_FK,   ";
			strSQL = strSQL + " NVL(CONFMST.APPLICABLE_OPERATIONS_VALUE,0) ACCESSRIGHT,";
			strSQL = strSQL + " NVL(USRROLE.ALLOWED_OPERATIONS_VALUE,0) ALLOWEDRIGHT,";
			strSQL = strSQL + " nvl(USRROLE.VERSION_NO,0) VERSION_NO ,";
			strSQL = strSQL + " (case when ( select count(*) from menu_mst_tbl menu where menu.active_flag=1 and menu.config_id_fk=MST.CONFIG_ID_FK) > 1 then  (case when (instr(','||USRROLE.menu_mst_fk||',' , ','||MST.MENU_MST_PK||',')) > 0 then 1 else 0 end ) else (";
			strSQL = strSQL + " (CASE WHEN USRROLE.ALLOWED_OPERATIONS_VALUE  > 0 then 1 else 0 end ) ) end )  SELECTED,USRROLE.MENU_MST_FK,USRROLE.ALLOWED_OPERATIONS ";
			strSQL = strSQL + " FROM MENU_MST_TBL MST, MENU_MST_TBL MST1,  ";
			strSQL = strSQL + " MENU_TEXT_MST_TBL MTXT,   ";
			strSQL = strSQL + " CONFIG_MST_TBL CONFMST ,";
			strSQL = strSQL + " ROLE_ACCESS_TRN USRROLE";
			strSQL = strSQL + " WHERE CONFMST.CONFIG_MST_PK= USRROLE.CONFIG_MST_FK(+)";
			strSQL = strSQL + " AND MTXT.MENU_MST_FK(+) = MST.MENU_MST_PK ";
			strSQL = strSQL + " AND MST.MENU_MST_FK=MST1.MENU_MST_PK ";
			strSQL = strSQL + " AND MTXT.MENU_TEXT IS NOT NULL  ";
			strSQL = strSQL + " AND USRROLE.ROLE_MST_FK(+)=" + Val + "";
			strSQL = strSQL + " AND MTXT.ENVIRONMENT_MST_FK(+) = 1  ";
			strSQL = strSQL + " AND CONFMST.CONFIG_MST_PK(+) =  MST.CONFIG_ID_FK  ";
			strSQL = strSQL + " AND  MST.ACTIVE_FLAG=1 ";
			if (Convert.ToInt32(HttpContext.Current.Session["BIZ_TYPE"]) == 1) {
				strSQL = strSQL + "  AND MST.BIZ_TYPE IN (1,3)";
			} else if (Convert.ToInt32(HttpContext.Current.Session["BIZ_TYPE"]) == 2) {
				strSQL = strSQL + "  AND MST.BIZ_TYPE IN (2,3)";
			}
			strSQL = strSQL + " ORDER BY MST.DISPLAY_ORDER) QRY";
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

		#region "Fetch Header"
		public DataSet FetchHeader(Int32 Val)
		{
			string StrSQl = null;
			StrSQl = "select t.role_mst_tbl_pk,t.role_id,t.role_description,t. Active_Flag,t.Version_No ";
			StrSQl = StrSQl + "from Role_Mst_Tbl t where ";
			if (Val > 0) {
				StrSQl = StrSQl + " Role_Mst_Tbl_Pk=" + Val + "";
			} else {
				StrSQl = StrSQl + " Role_Mst_Tbl_Pk=" + -1 + "";
			}
			WorkFlow objWF = new WorkFlow();
			try {
				return objWF.GetDataSet(StrSQl);
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		#endregion

		#region "Fetch Function"
		public DataSet Fetch1()
		{
			string strSQL = null;
			strSQL = string.Empty ;
			strSQL += "Select MST.MENU_MST_PK, " ;
			strSQL += " MST.MENU_MST_FK, " ;
			strSQL += " NVL(Mtxt.MENU_Text,'')  MENU_Text , " ;
			strSQL += " nvl(MST.CONFIG_ID_FK,0) CONFIG_ID_FK, " ;
			strSQL += " NVL(CONFMST.APPLICABLE_OPERATIONS_VALUE,0) APPLICABLE_OPERATIONS_VALUE, " ;
			strSQL += " NVL(UAT.ALLOWED_OPERATIONS_VALUE,15) ALLOWED_OPERATIONS_VALUE, " ;
			strSQL += " NVL(UAT.USER_ACCESS_PK,0) USER_ACCESS_PK, " ;
			strSQL += " NVL(UAT.VERSION_NO,0) VERSION_NO,  " ;
			strSQL += " MST.Menu_Level  " ;
			strSQL += " FROM Menu_MST_TBL MST,  " ;
			strSQL += " Menu_Text_Mst_Tbl MTxt,  " ;
			strSQL += " CONFIG_MST_TBL CONFMST  " ;
			strSQL = strSQL + " WHERE MTxt.Menu_Mst_FK(+) = Mst.Menu_Mst_Pk ";
			strSQL += "AND MTXT.MENU_TEXT IS NOT NULL " ;
			strSQL = strSQL + " And CONFMST.CONFIG_MST_PK(+) =  Mst.CONFIG_ID_Fk ";
			strSQL = strSQL + " And CONFMST.CONFIG_MST_PK = UAT.Config_Mst_FK(+) ";
			strSQL = strSQL + "ORDER BY Mst.DISPLAY_ORDER";
			WorkFlow objWF = new WorkFlow();
			DataSet objDS = null;
			try {
				objDS = objWF.GetDataSet(strSQL);
				return objDS;
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		#endregion

		#region "Save Function Header"
		public ArrayList Save(DataSet M_DataSet, DataTable DtInsert)
		{
			WorkFlow objWK = new WorkFlow();
			objWK.OpenConnection();
			OracleTransaction TRAN = null;
			TRAN = objWK.MyConnection.BeginTransaction();
			string strSQl = null;
			int intPKVal = 0;
			long lngRoleId = 0;
			Int32 RecAfct = default(Int32);
			OracleCommand insCommand = new OracleCommand();
			OracleCommand updCommand = new OracleCommand();
			OracleCommand delCommand = new OracleCommand();
			OracleCommand delRoleAccess = new OracleCommand();
			try {
				var _with1 = insCommand;
				_with1.Connection = objWK.MyConnection;
				_with1.CommandType = CommandType.StoredProcedure;
				_with1.CommandText = objWK.MyUserName + ".ROLE_MST_TBL_PKG.ROLE_MST_TBL_INS";
				var _with2 = _with1.Parameters;
				insCommand.Parameters.Add("ROLE_ID_IN", OracleDbType.Varchar2, 20, "ROLE_ID").Direction = ParameterDirection.Input;
				insCommand.Parameters["ROLE_ID_IN"].SourceVersion = DataRowVersion.Current;
				insCommand.Parameters.Add("ROLE_DESCRIPTION_IN", OracleDbType.Varchar2, 50, "ROLE_DESCRIPTION").Direction = ParameterDirection.Input;
				insCommand.Parameters["ROLE_DESCRIPTION_IN"].SourceVersion = DataRowVersion.Current;
				insCommand.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
				insCommand.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;
				insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
				insCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
				insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "ROLE_MST_TBL_PK").Direction = ParameterDirection.Output;
				insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				var _with3 = delCommand;
				_with3.Connection = objWK.MyConnection;
				_with3.CommandType = CommandType.StoredProcedure;
				_with3.CommandText = objWK.MyUserName + ".ROLE_MST_TBL_PKG.ROLE_MST_TBL_DEL";
				var _with4 = _with3.Parameters;
				delCommand.Parameters.Add("ROLE_MST_TBL_PK_IN", OracleDbType.Int32, 10, "ROLE_MST_TBL_PK").Direction = ParameterDirection.Input;
				delCommand.Parameters["ROLE_MST_TBL_PK_IN"].SourceVersion = DataRowVersion.Current;
				delCommand.Parameters.Add("DELETED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
				delCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
				delCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
				delCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
				delCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
				delCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				var _with5 = updCommand;
				_with5.Connection = objWK.MyConnection;
				_with5.CommandType = CommandType.StoredProcedure;
				_with5.CommandText = objWK.MyUserName + ".ROLE_MST_TBL_PKG.ROLE_MST_TBL_UPD";
				var _with6 = _with5.Parameters;
				updCommand.Parameters.Add("ROLE_MST_TBL_PK_IN", OracleDbType.Int32, 10, "ROLE_MST_TBL_PK").Direction = ParameterDirection.Input;
				updCommand.Parameters["ROLE_MST_TBL_PK_IN"].SourceVersion = DataRowVersion.Current;
				updCommand.Parameters.Add("ROLE_ID_IN", OracleDbType.Varchar2, 20, "ROLE_ID").Direction = ParameterDirection.Input;
				updCommand.Parameters["ROLE_ID_IN"].SourceVersion = DataRowVersion.Current;
				updCommand.Parameters.Add("ROLE_DESCRIPTION_IN", OracleDbType.Varchar2, 50, "ROLE_DESCRIPTION").Direction = ParameterDirection.Input;
				updCommand.Parameters["ROLE_DESCRIPTION_IN"].SourceVersion = DataRowVersion.Current;
				updCommand.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
				updCommand.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;
				updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
				updCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
				updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
				updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
				updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
				updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				var _with7 = objWK.MyDataAdapter;
				_with7.InsertCommand = insCommand;
				_with7.InsertCommand.Transaction = TRAN;
				_with7.UpdateCommand = updCommand;
				_with7.UpdateCommand.Transaction = TRAN;
				_with7.DeleteCommand = delCommand;
				_with7.DeleteCommand.Transaction = TRAN;
				RecAfct = _with7.Update(M_DataSet);
				if (RecAfct > 0) {
					lngRoleId = Convert.ToInt64(M_DataSet.Tables[0].Rows[0]["ROLE_MST_TBL_PK"]);
					SaveGrid(lngRoleId, TRAN, DtInsert);
				}
				if (arrMessage.Count > 0) {
					TRAN.Rollback();
					return arrMessage;
				} else {
					arrMessage.Add("All Data Saved Successfully");
					TRAN.Commit();
					arrMessage.Add(lngRoleId);
					return arrMessage;
				}
			} catch (OracleException oraexp) {
				arrMessage.Add(oraexp.Message);
				return arrMessage;
			} catch (Exception ex) {
				arrMessage.Add(ex.Message);
				return arrMessage;
			} finally {
				objWK.CloseConnection();
			}
		}
		#endregion

		#region "Save Function Grid"
		public ArrayList SaveGrid(Int64 ROLE_MST_FK, OracleTransaction TRAN, DataTable DtInsert)
		{
			WorkFlow objWK = new WorkFlow();
			objWK.MyConnection = TRAN.Connection;
			int intPKVal = 0;
			long lngI = 0;
			Int32 RecAfct = default(Int32);
			Int32 RetVal = default(Int32);
			OracleCommand insCommand = new OracleCommand();
			OracleCommand updCommand = new OracleCommand();
			OracleCommand delCommand = new OracleCommand();
			try {
				var _with8 = insCommand;
				_with8.Transaction = TRAN;
				_with8.Connection = objWK.MyConnection;
				_with8.CommandType = CommandType.StoredProcedure;
				_with8.CommandText = objWK.MyUserName + ".ROLE_ACCESS_TRN_PKG.ROLE_ACCESS_TRN_INS";
				Int32 i = default(Int32);
				for (i = 0; i <= DtInsert.Rows.Count - 1; i++) {
					var _with9 = _with8.Parameters;
					_with9.Clear();
					insCommand.Parameters.Add("ROLE_MST_FK_IN", ROLE_MST_FK).Direction = ParameterDirection.Input;
					insCommand.Parameters.Add("CONFIG_MST_FK_IN", DtInsert.Rows[i][0]).Direction = ParameterDirection.Input;
					insCommand.Parameters.Add("ALLOWED_OPERATIONS_VALUE_IN", DtInsert.Rows[i][1]).Direction = ParameterDirection.Input;
					insCommand.Parameters.Add("ALLOWED_OPERATIONS_IN", DtInsert.Rows[i][2]).Direction = ParameterDirection.Input;
					insCommand.Parameters.Add("MENU_MST_FK_IN", DtInsert.Rows[i][3]).Direction = ParameterDirection.Input;
					insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
					insCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
					insCommand.Parameters.Add("RETURN_VALUE", RetVal).Direction = ParameterDirection.Output;
					RecAfct = insCommand.ExecuteNonQuery();
					if (RecAfct <= 0) {
						arrMessage.Add("ERROR!!!!1111");
						TRAN.Rollback();
						break; // TODO: might not be correct. Was : Exit For
					}
				}
			} catch (OracleException oraexp) {
				TRAN.Rollback();
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
            return new ArrayList();
		}
		#endregion

		public DataSet DumDataset(Int32 Str)
		{
			string strsql = null;
			strsql = "SELECT ROWNUM SR_NO, ";
			strsql = strsql + " Role_Mst_Tbl_Pk,";
			strsql = strsql + " Active_Flag,";
			strsql = strsql + " Role_Id,";
			strsql = strsql + " Role_Description,";
			strsql = strsql + " Version_No ";
			strsql = strsql + " FROM ROLE_MST_TBL ";
			strsql = strsql + " WHERE ";
			if (Str > 0) {
				strsql = strsql + " Role_Mst_Tbl_Pk=" + Str + "";
			} else {
				strsql = strsql + " Role_Mst_Tbl_Pk=" + Str + "";
			}

			WorkFlow objWF = new WorkFlow();
			try {
				return objWF.GetDataSet(strsql);
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}

		public DataSet RoleListing()
		{
			string strSQL = null;
			strSQL = "select ROWNUM SLNo,role_mst_tbl.role_mst_tbl_pk,";
			strSQL = strSQL + " role_mst_tbl.active_flag,";
			strSQL = strSQL + " role_mst_tbl.role_id,";
			strSQL = strSQL + " role_mst_tbl.role_description";

			strSQL = strSQL + " from role_mst_tbl order by role_mst_tbl.role_mst_tbl_pk";

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

        public string RoleListingSearch(string Str, string strDesc, string Searchtype, string strColumnName = "", bool blnSortAscending = false, Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 flag = 0, Int32 ActFlag = 0)
        {

            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            if (flag == 0)
            {
                strCondition += " AND 1=2";
            }
            if (Searchtype == "S")
            {
                if (Str.Length > 0)
                {
                    strCondition = strCondition + "  and upper(role_mst_tbl.role_id) like '" + Str.ToUpper().Replace("'", "''") + "%'";
                }
            }
            else
            {
                if (Str.Length > 0)
                {
                    strCondition = strCondition + " and upper(role_mst_tbl.role_id) like '%" + Str.ToUpper().Replace("'", "''") + "%'";
                }
            }

            if (Searchtype == "S")
            {
                if (strDesc.Length > 0)
                {
                    strCondition = strCondition + " and upper(role_mst_tbl.role_description) like '" + strDesc.ToUpper().Replace("'", "''") + "%'";
                }
            }
            else
            {
                if (strDesc.Length > 0)
                {
                    strCondition = strCondition + " and upper(role_mst_tbl.role_description) like '%" + strDesc.ToUpper().Replace("'", "''") + "%'";
                }
            }
            if (ActFlag == 1)
            {
                strCondition = strCondition + " AND ROLE_MST_TBL.ACTIVE_FLAG = " + ActFlag;
            }
            strSQL = "select Count(*) from role_mst_tbl where 1=1 ";
            strSQL += strCondition;
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
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
            start = (CurrentPage - 1) * RecordsPerPage + 1;
            strSQL = "SELECT * from (";
            strSQL = strSQL + " SELECT ROWNUM SR_NO, q.* FROM ";
            strSQL = strSQL + " (select role_mst_tbl.role_mst_tbl_pk,";
            strSQL = strSQL + " role_mst_tbl.active_flag,";
            strSQL = strSQL + " role_mst_tbl.role_id,";
            strSQL = strSQL + " role_mst_tbl.role_description";
            strSQL = strSQL + " from role_mst_tbl where 1=1  ";
            strSQL = strSQL + strCondition;
            if (!strColumnName.Equals("SR_NO"))
            {
                strSQL += "order by " + strColumnName;
            }
            if (!blnSortAscending & !strColumnName.Equals("SR_NO"))
            {
                strSQL += " DESC";
            }
            strSQL = strSQL + " )q)";
                //WHERE SR_NO  Between " + start + " and " + last;
            strSQL += " ORDER BY  SR_NO";
            try
            {
                DataSet DS = objWF.GetDataSet(strSQL);
                return JsonConvert.SerializeObject(DS, Formatting.Indented);
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

		public DataSet GetData()
		{
			string StrSQL = null;
			StrSQL = "Select t.user_operation_mst_pk,t.OPERATION_Name,t.OPERATION_Value from user_operation_mst_tbl t order by t.user_operation_mst_pk";

			WorkFlow objWF = new WorkFlow();
			try {
				return objWF.GetDataSet(StrSQL);
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}

		}

		public ArrayList DeleteGrid(DataSet DS)
		{
			string GateInQuery = null;
			Int32 i = default(Int32);
			WorkFlow objWK = new WorkFlow();
			string DeptDate = null;
			try {
				objWK.OpenConnection();

				for (i = 0; i <= DS.Tables[0].Rows.Count - 1; i++) {
					GateInQuery = " Delete from Role_Access_Trn t where t.role_mst_fk= ";
					GateInQuery += DS.Tables[0].Rows[i][0];
					GateInQuery += "AND t.CONFIG_MST_FK=" + DS.Tables[0].Rows[i][1];
					objWK.ExecuteCommands(GateInQuery);
				}
				arrMessage.Add("All Data Saved Successfully");
				return arrMessage;
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				objWK.CloseConnection();
			}
		}

		public ArrayList DeleteHead(Int32 PK)
		{
			string GateInQuery = null;
			Int32 i = default(Int32);
			WorkFlow objWK = new WorkFlow();
			string DeptDate = null;
			try {
				objWK.OpenConnection();
				GateInQuery = " Delete from Role_Mst_Tbl  where role_mst_tbl_pk= " + PK;
				if (objWK.ExecuteCommands(GateInQuery) == true) {
					arrMessage.Add("All Data Saved Successfully");
					return arrMessage;
				} else {
					return arrMessage;
				}
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				objWK.CloseConnection();
			}
		}

		#region "Fetch Role"
		public DataTable FetchRoles()
		{
			string strSQL = null;
			WorkFlow objWF = new WorkFlow();
			try {
				strSQL = "SELECT R.ROLE_ID,R.ROLE_MST_TBL_PK FROM ROLE_MST_TBL R ORDER BY R.ROLE_ID";
				return objWF.GetDataTable(strSQL);
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
