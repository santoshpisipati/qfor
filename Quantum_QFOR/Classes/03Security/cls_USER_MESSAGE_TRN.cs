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
namespace Quantum_QFOR
{
    public class clsUSER_MESSAGE_TRN : CommonFeatures
	{
		System.Web.UI.Page objPage = new System.Web.UI.Page();
			#region "Get Message Information"
		public int M_Count = 0;
		public DataSet GetMessageInfo(Int64 DOCPk, Int64 LOCPk)
		{
			string strSql = null;
			DataSet MainDS = new DataSet();
			OracleDataAdapter DA = new OracleDataAdapter();

			strSql = string.Empty ;
			strSql += " SELECT ROWNUM SR_NO,  " ;
			strSql += " 0 User_Message_Pk, " ;
			strSql += " UsrMsgTrn.Sender_Fk, " ;
			strSql += " UsrMsgTrn.Receiver_Fk, " ;
			strSql += " UsrMsgTrn.Msg_Read, " ;
			strSql += " UsrMsgTrn.Followup_Flag, " ;
			strSql += " UsrMsgTrn.Have_Attachment, " ;
			strSql += " doc.document_subject  Msg_Subject, " ;
			strSql += " doc.document_header ||'~'|| doc.document_body || '~' ||  doc.document_footer || ' ' Msg_Body," ;
			strSql += " UsrMsgTrn.Read_Receipt, " ;
			strSql += " UsrMsgTrn.Document_Mst_Fk, " ;
			strSql += " doc.message_folder_mst_fk User_Message_Folders_Fk, " ;
			strSql += " UsrMsgTrn.Msg_Received_Dt, " ;
			strSql += " UsrMsgTrn.Version_No  " ;
			strSql += " FROM USER_MESSAGE_TRN UsrMsgTrn, " ;
			strSql += " document_mst_tbl doc " ;
			strSql += " WHERE usrmsgtrn.document_mst_fk(+) = doc.document_mst_pk " ;
			strSql += " AND UsrMsgTrn.DELETE_FLAG is null  AND doc.document_mst_pk =  " + DOCPk ;
			strSql += " AND User_Message_Pk(+) =  -1 " ;
			WorkFlow objWF = new WorkFlow();

			try {
				DA = objWF.GetDataAdapter(strSql.Trim());
				DA.Fill(MainDS, "MsgTrn");
				strSql = string.Empty ;
				strSql += " SELECT ROWNUM SR_NO, " ;
				strSql += " 0 User_Message_Det_Pk, " ;
				strSql += " 0 User_Message_Fk, " ;
				strSql += " '' Attachment_Caption, " ;
				strSql += " '' Attachment_Data, " ;
				strSql += " doc.attachment_url Url_Page, " ;
				strSql += " 0 Version_No " ;
				strSql += " FROM document_mst_tbl doc " ;
				strSql += " WHERE doc.Active=1 And doc.document_mst_pk = " + DOCPk ;

				DA = objWF.GetDataAdapter(strSql);
				DA.Fill(MainDS, "MsgDet");
				DataRelation DSWFMSg = new DataRelation("WFMsg", new DataColumn[] { MainDS.Tables["MsgTrn"].Columns["User_Message_Pk"] }, new DataColumn[] { MainDS.Tables["MsgDet"].Columns["User_Message_Fk"] });
				MainDS.Relations.Add(DSWFMSg);
				return MainDS;

			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		#endregion

		#region "Check for worlflow"
		public Int32 iSWorkFlowDefine(Int64 DocPk, Int64 LOCPk)
		{
			string strSql = null;

			strSql = "SELECT t.user_mst_fk FROM Workflow_Rules_Trn t ";
			strSql = strSql + " WHERE t.document_mst_fk = " + DocPk;
			strSql = strSql + " And t.from_loc_mst_fk = " + LOCPk;
			strSql = strSql + " AND SYSDATE BETWEEN t.valid_from AND t.validto AND t.active = 1";
			WorkFlow objWK = new WorkFlow();
			try {
				return Convert.ToInt32(objWK.ExecuteScaler(strSql));
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				return 0;
			}

		}
		#endregion

		#region "Fetch Function"
		public DataSet FetchAll(Int64 P_User_Message_Pk = -1)
		{
			string strSQL = null;
			strSQL = "SELECT ROWNUM SR_NO, ";
			strSQL = strSQL + " User_Message_Pk,";
			strSQL = strSQL + " Sender_Fk,";
			strSQL = strSQL + " Receiver_Fk,";
			strSQL = strSQL + " Msg_Read,";
			strSQL = strSQL + " Followup_Flag,";
			strSQL = strSQL + " Have_Attachment,";
			strSQL = strSQL + " Msg_Subject,";
			strSQL = strSQL + " Msg_Body,";
			strSQL = strSQL + " Read_Receipt,";
			strSQL = strSQL + " Document_Mst_Fk,";
			strSQL = strSQL + " User_Message_Folders_Fk,";
			strSQL = strSQL + " Msg_Received_Dt,";
			strSQL = strSQL + " Version_No ";
			strSQL = strSQL + " FROM USER_MESSAGE_TRN ";
			strSQL = strSQL + " Where DELETE_FLAG is null And User_Message_Pk = " + P_User_Message_Pk + " ";

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
		public DataSet FetchMessageByFolder(Int32 FolderPK, Int32 UserPK, Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 sCol = 9, string SortOrder = "DESC", string FilterBy = "", string SearchValue = "", string SearchType = "")
		{
			Int32 last = default(Int32);
			Int32 start = default(Int32);
			string strSQL = null;
			string strCondition = null;
			Int32 TotalRecords = default(Int32);
			WorkFlow objWF = new WorkFlow();
			string searchQuery = null;
			string searchQuery1 = null;
			int ParentFolderFk = GetparentFolderPK(FolderPK);

			if (ParentFolderFk == 1) {
				searchQuery = " AND ((UsrMsg.DELETE_FLAG is Null ";
				searchQuery += " And usrmsg.receiver_fk = " + UserPK;
				searchQuery1 = " and ((EMT.DELETE_FLAG is Null   and EMT.user_message_folders_fk =1 And EMT.SENDER_FK!=" + UserPK;
			} else if (ParentFolderFk == 4) {
				searchQuery = " AND ((Usrmsg.del_sender_flag is null And Usrmsg.del_sentitem_flag IS NULL and Usrmsg.delete_flag is null and usrmsg.sender_fk= " + UserPK;
				searchQuery1 = "  and ((EMT.del_sender_flag is null And EMT.del_sentitem_flag IS NULL and EMT.delete_flag is null and EMT.sender_fk= " + UserPK;
			} else if (ParentFolderFk == 7) {
				searchQuery = "  AND ((UsrMsg.DELETE_FLAG is Null  AND USRMSG.MSG_READ = 1 AND usrmsg.receiver_fk = " + UserPK;
				searchQuery1 = "  AND ((EMT.DELETE_FLAG is Null AND EMT.MSG_READ = 1 ";
			} else if (ParentFolderFk == 8) {
				searchQuery = "  AND ((UsrMsg.DELETE_FLAG is Null AND USRMSG.MSG_READ = 0 AND usrmsg.receiver_fk = " + UserPK;
				searchQuery1 = "  AND ((EMT.DELETE_FLAG is Null AND EMT.MSG_READ = 0 ";
			} else if (ParentFolderFk != 0) {
				searchQuery = " AND ((UsrMsg.DELETE_FLAG is Null AND usrmsg.receiver_fk = " + UserPK;
				searchQuery1 = " and ((EMT.DELETE_FLAG is Null ";
			}
			searchQuery += ") OR UF.USER_MESSAGE_FOLDERS_PK=" + FolderPK + ")";
			searchQuery1 += ") OR UF.USER_MESSAGE_FOLDERS_PK=" + FolderPK + ")";

			searchQuery += " AND UF.USER_MESSAGE_FOLDERS_PK=" + FolderPK;
			searchQuery1 += " AND UF.USER_MESSAGE_FOLDERS_PK=" + FolderPK;

			//strSQL = "SELECT Count(*) "
			//strSQL &= vbCrLf & " FROM ("
			//strSQL &= vbCrLf & " SELECT USRMSG.USER_MESSAGE_PK FROM "
			//If FolderPK = 6 Then
			//    strSQL &= vbCrLf & " USER_MESSAGE_TRN_ARCHIVE UsrMsg,"
			//Else
			//    strSQL &= vbCrLf & " user_message_trn UsrMsg,"
			//End If
			//strSQL &= vbCrLf & " user_mst_tbl Usr, "
			//strSQL &= vbCrLf & " user_mst_tbl Usr1 "
			//strSQL &= vbCrLf & " WHERE   "
			//strSQL &= vbCrLf & " usrmsg.receiver_fk = usr1.user_mst_pk(+) "
			//strSQL &= vbCrLf & " and usrmsg.Sender_Fk = usr.user_mst_pk(+) "
			//strSQL &= searchQuery

			//strSQL &= " union  " & vbCrLf
			//strSQL &= " SELECT EMT.EXTERNAL_MSG_TRN_PK "
			//strSQL &= vbCrLf & " FROM EXTERNAL_MESSAGE_TRN EMT,user_mst_tbl umt"
			//strSQL &= vbCrLf & " WHERE  EMT.SENDER_FK=UMT.USER_MST_PK(+) "
			//strSQL &= searchQuery1
			//strSQL &= vbCrLf & " )   "


			if (sCol == 0)
				sCol = 2;

			strSQL = " Select * From (";
			strSQL = strSQL + " SELECT ROWNUM SR_NO,qry.* FROM ";
			strSQL += "( Select ";
			strSQL += " usrmsg.user_message_pk, ";
			strSQL += " '' Del, ";
			strSQL += " usrmsg.have_attachment HvAttch, ";
			strSQL += " usrmsg.followup_flag FollowFlg, ";
			strSQL += " usrmsg.sender_fk SenderFk,";
			strSQL += " usr.user_name Sender,";
			strSQL += " usr1.user_name Receiver,";
			strSQL += " usrmsg.msg_subject MsgSub, ";
			strSQL += " UF.FOLDER_NAME FOLDER,";
			strSQL += " usrmsg.msg_received_dt ReceiveDt,";
			strSQL += " usrmsg.msg_read,";
			strSQL += " usrmsg.read_receipt,1 Flag";
			strSQL += " FROM USER_MESSAGE_FOLDERS_TRN UF, ";
			if (FolderPK == 6) {
				strSQL += " USER_MESSAGE_TRN_ARCHIVE UsrMsg,";
			} else {
				strSQL += " user_message_trn UsrMsg,";
			}
			strSQL += " user_mst_tbl Usr,user_mst_tbl Usr1 ";
			strSQL += " WHERE usrmsg.sender_fk = usr.user_mst_pk(+) ";
			strSQL += " AND usrmsg.Receiver_Fk = usr1.user_mst_pk(+) ";
			strSQL += " AND USRMSG.USER_MESSAGE_FOLDERS_FK=UF.USER_MESSAGE_FOLDERS_PK";
			strSQL += searchQuery;

			strSQL += " union  " ;

			strSQL += " Select ";
			strSQL += " EMT.EXTERNAL_MSG_TRN_PK, ";
			strSQL += " '' Del, ";
			strSQL += " 0 HvAttch, ";
			strSQL += " 0 FollowFlg, ";
			strSQL += " EMT.SENDER_FK SenderFk,";
			strSQL += " UMT.USER_NAME Sender,";
			strSQL += " TO_CHAR(EMT.EX_EMAIL_ID) Receiver,";
			strSQL += " EMT.MSG_SUBJECT MsgSub, ";
			strSQL += " UF.FOLDER_NAME FOLDER,";
			strSQL += " EMT.MSG_RECEIVED_DT ReceiveDt,";
			strSQL += " EMT.msg_read,";
			strSQL += " 0 read_receipt,2 Flag ";
			strSQL += " FROM USER_MESSAGE_FOLDERS_TRN UF,EXTERNAL_MESSAGE_TRN EMT,user_mst_tbl umt ";
			strSQL += " WHERE EMT.SENDER_FK=UMT.USER_MST_PK(+) ";
			strSQL += searchQuery1;
			strSQL += " Order By " + sCol + "  " + SortOrder;
			strSQL += " ) qry ) ";

			objWF.MyDataReader = objWF.GetDataReader("SELECT COUNT(*) FROM (" + strSQL + ")");
			while (objWF.MyDataReader.Read()) {
				TotalRecords = objWF.MyDataReader.GetInt32(0);
			}
			objWF.MyDataReader.Close();
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

			strSQL += " WHERE SR_NO  Between " + start + " and " + last;

			try {
				return objWF.GetDataSet(strSQL);
			} catch (OracleException sqlExp) {
				throw sqlExp;
			} catch (Exception exp) {
				throw exp;
			}
		}
		public int GetparentFolderPK(int FolderFk)
		{
			WorkFlow objWF = new WorkFlow();
			int ParentFolderFk = 0;
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			sb.Append("SELECT Q.USER_MESSAGE_FOLDERS_PK");
			sb.Append("  FROM (SELECT UF.USER_MESSAGE_FOLDERS_PK, UF.PARENT_FK");
			sb.Append("          FROM USER_MESSAGE_FOLDERS_TRN UF");
			sb.Append("         START WITH UF.USER_MESSAGE_FOLDERS_PK = " + FolderFk);
			sb.Append("        CONNECT BY PRIOR UF.PARENT_FK = UF.USER_MESSAGE_FOLDERS_PK) Q");
			sb.Append(" WHERE PARENT_FK IS NULL ");
			try {
				objWF.MyDataReader = objWF.GetDataReader(sb.ToString());
				while (objWF.MyDataReader.Read()) {
					ParentFolderFk = objWF.MyDataReader.GetInt32(0);
				}
				objWF.MyDataReader.Close();
			} catch (Exception ex) {
				throw ex;
			}
			return ParentFolderFk;
		}

		#region "Fetch UserName & Location Details"
		public DataSet FetchUserDetails(Int64 P_User_Message_Pk)
		{
			string strSQL = null;
			strSQL = "select UMT.USER_NAME, LMT.LOCATION_NAME";
			strSQL = strSQL + " from user_mst_tbl UMT, Location_Mst_Tbl LMT";
			strSQL = strSQL + " where UMT.USER_MST_PK=" + P_User_Message_Pk;
			strSQL = strSQL + " and LMT.LOCATION_MST_PK = UMT.DEFAULT_LOCATION_FK";

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
		#region "Get user PK"
		public long get_User_Pk(long Employee_FK)
		{
			WorkFlow objWK = new WorkFlow();
			string strSQL = null;
			long userpk = 0;
			try {
				strSQL = " SELECT U.USER_MST_PK  FROM USER_MST_TBL U WHERE U.EMPLOYEE_MST_FK = " + Employee_FK;
				if (!string.IsNullOrEmpty(objWK.ExecuteScaler(strSQL))) {
					userpk = Convert.ToInt64(objWK.ExecuteScaler(strSQL));
					return userpk;
				}
			//Manjunath  PTS ID:Sep-02  16/09/2011
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
            return 0;
		}

		#endregion
		#region "Fetch EmployeeName & Location Details"
		public DataSet FetchEmpDetails(Int64 P_User_Message_Pk)
		{
			string strSQL = null;

			strSQL = " select EMT.employee_name, LMT.LOCATION_NAME ";
			strSQL = strSQL + " from employee_mst_tbl EMT, Location_Mst_Tbl LMT ";
			strSQL = strSQL + " where EMT.EMPLOYEE_MST_PK= " + P_User_Message_Pk;
			strSQL = strSQL + " and LMT.LOCATION_MST_PK = EMT.LOCATION_MST_FK ";

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

		#endregion

		#region "Update Read status"
		public int UpdateMsgReadStatus(Int32 MsgPK)
		{
			string strSQL = null;
			string strSQL1 = null;
			string strSQL2 = null;
			WorkFlow objWK = new WorkFlow();

			try {
				objWK.OpenConnection();
				strSQL = string.Empty ;
				strSQL += "UPDATE User_Message_Trn " ;
				strSQL += "SET msg_read = 1" ;
				strSQL += "WHERE  " ;
				strSQL += "user_message_pk = " + MsgPK;
				strSQL += " " ;
				//'
				strSQL1 = string.Empty ;
				strSQL1 += "UPDATE EXTERNAL_MESSAGE_TRN " ;
				strSQL1 += "SET msg_read = 1" ;
				strSQL1 += "WHERE  " ;
				strSQL1 += "EXTERNAL_MSG_TRN_PK = " + MsgPK;

				strSQL2 = string.Empty ;
				strSQL2 += "UPDATE User_Message_Trn_Archive " ;
				strSQL2 += "SET msg_read = 1" ;
				strSQL2 += "WHERE  " ;
				strSQL2 += "user_message_pk = " + MsgPK;
				strSQL2 += " " ;

				if (objWK.ExecuteCommands(strSQL) == true | objWK.ExecuteCommands(strSQL1) == true | objWK.ExecuteCommands(strSQL2) == true) {
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

		#region "Update UnRead status"
		public int UpdateMsgUnReadStatus(Int32 MsgPK)
		{
			string strSQL = null;
			WorkFlow objWK = new WorkFlow();

			try {
				objWK.OpenConnection();
				strSQL = string.Empty ;
				strSQL += "UPDATE User_Message_Trn " ;
				strSQL += "SET msg_read = 0" ;
				strSQL += "WHERE  " ;
				strSQL += "user_message_pk = " + MsgPK;
				strSQL += " " ;
				if (objWK.ExecuteCommands(strSQL) == true) {
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

		#region "Update Set Flag status"
		public int UpdateSetFlag(Int32 MsgPK)
		{
			string strSQL = null;
			Int32 i = default(Int32);
			WorkFlow objWK = new WorkFlow();
			string ArrDate = null;
			string ADate = null;
			DateTime ArDate = default(DateTime);

			try {
				objWK.OpenConnection();
				strSQL = string.Empty ;
				strSQL += "UPDATE User_Message_Trn " ;
				strSQL += "SET followup_flag = 1" ;
				strSQL += "WHERE  " ;
				strSQL += "user_message_pk = " + MsgPK;
				strSQL += " " ;
				if (objWK.ExecuteCommands(strSQL) == true) {
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

		#region "Update Clear Flag status"
		public int UpdateClearFlag(Int32 MsgPK)
		{
			string strSQL = null;
			WorkFlow objWK = new WorkFlow();

			try {
				objWK.OpenConnection();
				strSQL = string.Empty ;
				strSQL += "UPDATE User_Message_Trn " ;
				strSQL += "SET followup_flag = 0" ;
				strSQL += "WHERE  " ;
				strSQL += "user_message_pk = " + MsgPK;
				strSQL += " " ;
				if (objWK.ExecuteCommands(strSQL) == true) {
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

		#region "Delete MSg"
		public int UpdateMsgStatus(Int32 DocPK, Int32 PKVal)
		{
			string strSQL = null;
			WorkFlow objWK = new WorkFlow();
			try {
				objWK.OpenConnection();
				strSQL = string.Empty ;
				strSQL += "DELETE " ;
				strSQL += "FROM  " ;
				strSQL += "User_Message_Trn MsgTrn, " ;
				strSQL += "user_message_det_trn MsgDet " ;
				strSQL += "WHERE  " ;
				strSQL += "MsgTrn.document_mst_fk = " + DocPK;
				strSQL += "AND msgtrn.user_message_pk = msgdet.user_message_fk " ;
				strSQL += "AND trim(substr(msgdet.url_page,instr(msgdet.url_page,'=')+1)) = " + PKVal;
				if (objWK.ExecuteCommands(strSQL) == true) {
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

		#region "Delete Mails"
		public int DeleteMails(Int32 MailPK)
		{
			string strSQL = null;
			WorkFlow objWK = new WorkFlow();

			try {
				objWK.OpenConnection();
				strSQL = "Update USER_MESSAGE_TRN ";
				strSQL += " Set DELETE_FLAG=nvl(DELETE_FLAG,0)+1 Where ";
				strSQL += " USER_MESSAGE_PK = " + MailPK;

				if (objWK.ExecuteCommands(strSQL)) {
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

		#region "MoveToFolders"
		public int MoveToFolder(string strPKVal, Int32 FolderFK)
		{
			System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
			System.Text.StringBuilder strBuilder1 = new System.Text.StringBuilder();
			WorkFlow objWK = new WorkFlow();

			try {
				objWK.OpenConnection();

				strBuilder.Append(" UPDATE User_Message_Trn SET ");
				strBuilder.Append(" USER_MESSAGE_FOLDERS_FK = " + FolderFK + "");
				strBuilder1.Append(" UPDATE EXTERNAL_MESSAGE_TRN SET ");
				strBuilder1.Append(" USER_MESSAGE_FOLDERS_FK = " + FolderFK + "");
				if (FolderFK == 4) {
					strBuilder.Append(" AND  del_sentitem_flag = 1");
				}
				strBuilder.Append(" WHERE user_message_pk IN (" + strPKVal + ") ");
				strBuilder1.Append(" WHERE EXTERNAL_MSG_TRN_PK IN (" + strPKVal + ") ");
				if (objWK.ExecuteCommands(strBuilder.ToString()) == true | objWK.ExecuteCommands(strBuilder1.ToString()) == true) {
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

		#region "GetFolders"
		public DataSet GetFolders(Int32 UserPK)
		{
			System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
			strBuilder.Append(" SELECT");
			strBuilder.Append(" UF.USER_MESSAGE_FOLDERS_PK,");
			strBuilder.Append(" INITCAP(UF.FOLDER_NAME) FOLDER_NAME,");
			strBuilder.Append(" INITCAP(UF.FOLDER_DESCRIPTION) FOLDER_DESCRIPTION,");
			strBuilder.Append(" NVL(UF.PARENT_FK,0) PARENT_FK");
			strBuilder.Append(" FROM USER_MESSAGE_FOLDERS_TRN UF");
			strBuilder.Append(" ORDER BY UF.USER_MESSAGE_FOLDERS_PK");
			WorkFlow objWF = new WorkFlow();
			try {
				return objWF.GetDataSet(strBuilder.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		#endregion
		#region "Save"
		public int Save(DataSet M_DataSet, OracleTransaction TRAN, Int32 SRRPK)
		{
			WorkFlow objWK = new WorkFlow();
			objWK.MyConnection = TRAN.Connection;
			objWK.MyCommand.Transaction = TRAN;
			objWK.MyCommand.Connection = TRAN.Connection;
			try {
				int intPKVal = 0;
				long lngI = 0;
				Int32 RecAfct = default(Int32);
				var _with1 = objWK.MyCommand.Parameters;
				_with1.Clear();
				_with1.Add("SENDER_FK_IN", M_DataSet.Tables[0].Rows[0]["Sender_Fk"]).Direction = ParameterDirection.Input;
				_with1.Add("RECEIVER_FK_IN", M_DataSet.Tables[0].Rows[0]["Receiver_Fk"]).Direction = ParameterDirection.Input;
				_with1.Add("MSG_READ_IN", M_DataSet.Tables[0].Rows[0]["Msg_Read"]).Direction = ParameterDirection.Input;
				_with1.Add("FOLLOWUP_FLAG_IN", M_DataSet.Tables[0].Rows[0]["Followup_Flag"]).Direction = ParameterDirection.Input;
				_with1.Add("HAVE_ATTACHMENT_IN", M_DataSet.Tables[0].Rows[0]["Have_Attachment"]).Direction = ParameterDirection.Input;
				_with1.Add("MSG_SUBJECT_IN", M_DataSet.Tables[0].Rows[0]["Msg_Subject"]).Direction = ParameterDirection.Input;
				_with1.Add("MSG_BODY_IN", M_DataSet.Tables[0].Rows[0]["Msg_Body"]).Direction = ParameterDirection.Input;
				_with1.Add("READ_RECEIPT_IN", M_DataSet.Tables[0].Rows[0]["Read_Receipt"]).Direction = ParameterDirection.Input;
				_with1.Add("DOCUMENT_MST_FK_IN", M_DataSet.Tables[0].Rows[0]["Document_Mst_Fk"]).Direction = ParameterDirection.Input;
				_with1.Add("USER_MESSAGE_FOLDERS_FK_IN", M_DataSet.Tables[0].Rows[0]["User_Message_Folders_Fk"]).Direction = ParameterDirection.Input;
				_with1.Add("MSG_RECEIVED_DT_IN", M_DataSet.Tables[0].Rows[0]["Msg_Received_Dt"]).Direction = ParameterDirection.Input;
				_with1.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
				_with1.Add("RETURN_VALUE", intPKVal).Direction = ParameterDirection.Output;
				objWK.MyCommand.CommandText = objWK.MyUserName + ".USER_MESSAGE_TRN_PKG.USER_MESSAGE_TRN_INS";
				objWK.MyCommand.CommandType = CommandType.StoredProcedure;
				if (objWK.MyCommand.ExecuteNonQuery() == 1) {
					M_DataSet.Tables[0].Rows[0]["USER_MESSAGE_PK"] = objWK.MyCommand.Parameters["Return_value"].Value;
					string strURL = null;
					strURL = Convert.ToString(M_DataSet.Tables[1].Rows[0]["Url_Page"]) + SRRPK;
					var _with2 = objWK.MyCommand.Parameters;
					_with2.Clear();
					_with2.Add("USER_MESSAGE_FK_IN", M_DataSet.Tables[0].Rows[0]["USER_MESSAGE_PK"]).Direction = ParameterDirection.Input;
					_with2.Add("ATTACHMENT_CAPTION_IN", M_DataSet.Tables[0].Rows[0]["Msg_Subject"]).Direction = ParameterDirection.Input;
					_with2.Add("ATTACHMENT_DATA_IN", M_DataSet.Tables[1].Rows[0]["Attachment_Data"]).Direction = ParameterDirection.Input;
					_with2.Add("URL_PAGE_IN", strURL).Direction = ParameterDirection.Input;
					_with2.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
					_with2.Add("RETURN_VALUE", intPKVal).Direction = ParameterDirection.Output;
					objWK.MyCommand.CommandText = objWK.MyUserName + ".USER_MESSAGE_DET_TRN_PKG.USER_MESSAGE_DET_TRN_INS";
					objWK.MyCommand.CommandType = CommandType.StoredProcedure;

					if (objWK.MyCommand.ExecuteNonQuery() == 1) {
					}
				} else {
					arrMessage.Add("Record Not Saved");
				}
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
            return 0;
		}
		public bool CreateFolder(int ParentFolderFk, string FolderName, string FolderDesc, Int32 UserPK)
		{
			WorkFlow objWF = new WorkFlow();
			objWF.MyConnection.Open();
			OracleTransaction tran = null;
			try {
				objWF.MyCommand = new OracleCommand();
				objWF.MyCommand.Connection = objWF.MyConnection;
				tran = objWF.MyConnection.BeginTransaction();
				var _with3 = objWF.MyCommand;
				_with3.Connection = objWF.MyConnection;
				_with3.CommandText = objWF.MyUserName + ".USER_MESSAGE_FOLDERS_TRN_PKG.USER_MESSAGE_FOLDERS_TRN_INS";
				_with3.CommandType = CommandType.StoredProcedure;
				_with3.Transaction = tran;
				_with3.Parameters.Clear();
				_with3.Parameters.Add("USER_MST_FK_IN", UserPK).Direction = ParameterDirection.Input;
				_with3.Parameters.Add("FOLDER_NAME_IN", FolderName).Direction = ParameterDirection.Input;
				_with3.Parameters.Add("FOLDER_DESCRIPTION_IN", FolderDesc).Direction = ParameterDirection.Input;
				_with3.Parameters.Add("PARENT_FK_IN", (ParentFolderFk > 0 ? ParentFolderFk : 0)).Direction = ParameterDirection.Input;
				_with3.Parameters.Add("CREATED_BY_FK_IN", UserPK).Direction = ParameterDirection.Input;
				_with3.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
				_with3.Parameters.Add("RETURN_VALUE", OracleDbType.Int32).Direction = ParameterDirection.Output;
				_with3.ExecuteNonQuery();
				tran.Commit();
				//Dim sqlstr As String = "INSERT INTO USER_MESSAGE_FOLDERS_TRN(USER_MESSAGE_FOLDERS_PK,USER_MST_FK,FOLDER_NAME,CREATED_BY_FK,CREATED_DT,VERSION_NO,FOLDER_DESCRIPTION,PARENT_FK)"
				//sqlstr &= "VALUES(SEQ_USER_MESSAGE_FOLDERS_TRN.NEXTVAL," & UserPK & ",'" & FolderName & "'," & UserPK & ",SYSDATE,0,'" & FolderDesc & "'," & ParentFolderFk & ")"

				//objWF.ExecuteCommands(sqlstr)
			} catch (Exception ex) {
				tran.Rollback();
				return false;
			} finally {
				objWF.CloseConnection();
				tran.Dispose();
			}
			return true;
		}
		public bool DeleteFolder(int FolderPK, bool DeleteMessages = false)
		{
			WorkFlow objWF = new WorkFlow();
			objWF.MyConnection.Open();
			OracleTransaction tran = null;
			try {
				objWF.MyCommand = new OracleCommand();
				objWF.MyCommand.Connection = objWF.MyConnection;
				tran = objWF.MyConnection.BeginTransaction();
				var _with4 = objWF.MyCommand;
				_with4.Connection = objWF.MyConnection;
				_with4.CommandText = objWF.MyUserName + ".USER_MESSAGE_FOLDERS_TRN_PKG.DELETE_FOLDER";
				_with4.CommandType = CommandType.StoredProcedure;
				_with4.Transaction = tran;
				_with4.Parameters.Clear();
				_with4.Parameters.Add("USER_MESSAGE_FOLDERS_PK_IN", FolderPK).Direction = ParameterDirection.Input;
				_with4.Parameters.Add("DELETED_BY_FK_IN", CREATED_BY).Direction = ParameterDirection.Input;
				_with4.Parameters.Add("DELETE_MSG_FLAG_IN", (DeleteMessages ? 1 : 0)).Direction = ParameterDirection.Input;
				_with4.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
				_with4.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 200).Direction = ParameterDirection.Output;
				_with4.ExecuteNonQuery();
				tran.Commit();
			} catch (Exception ex) {
				tran.Rollback();
				return false;
			} finally {
				objWF.CloseConnection();
				tran.Dispose();
			}
			return true;
		}
		#endregion

		#region "Save Direct"
		public int SaveDirect(DataSet M_DataSet)
		{
			WorkFlow objWK = new WorkFlow();
			objWK.OpenConnection();
			OracleTransaction TRAN = null;
			TRAN = objWK.MyConnection.BeginTransaction();
			objWK.MyCommand.Transaction = TRAN;
			objWK.MyCommand.Connection = objWK.MyConnection;
			try {
				int intPKVal = 0;
				long lngI = 0;
				Int32 RecAfct = default(Int32);
				var _with5 = objWK.MyCommand.Parameters;
				_with5.Clear();
				_with5.Add("SENDER_FK_IN", M_DataSet.Tables[0].Rows[0]["Sender_Fk"]).Direction = ParameterDirection.Input;
				_with5.Add("RECEIVER_FK_IN", M_DataSet.Tables[0].Rows[0]["Receiver_Fk"]).Direction = ParameterDirection.Input;
				_with5.Add("MSG_READ_IN", M_DataSet.Tables[0].Rows[0]["Msg_Read"]).Direction = ParameterDirection.Input;
				_with5.Add("FOLLOWUP_FLAG_IN", M_DataSet.Tables[0].Rows[0]["Followup_Flag"]).Direction = ParameterDirection.Input;
				_with5.Add("HAVE_ATTACHMENT_IN", M_DataSet.Tables[0].Rows[0]["Have_Attachment"]).Direction = ParameterDirection.Input;
				_with5.Add("MSG_SUBJECT_IN", M_DataSet.Tables[0].Rows[0]["Msg_Subject"]).Direction = ParameterDirection.Input;
				_with5.Add("MSG_BODY_IN", M_DataSet.Tables[0].Rows[0]["Msg_Body"]).Direction = ParameterDirection.Input;
				_with5.Add("READ_RECEIPT_IN", M_DataSet.Tables[0].Rows[0]["Read_Receipt"]).Direction = ParameterDirection.Input;
				_with5.Add("DOCUMENT_MST_FK_IN", M_DataSet.Tables[0].Rows[0]["Document_Mst_Fk"]).Direction = ParameterDirection.Input;
				_with5.Add("USER_MESSAGE_FOLDERS_FK_IN", M_DataSet.Tables[0].Rows[0]["User_Message_Folders_Fk"]).Direction = ParameterDirection.Input;
				_with5.Add("MSG_RECEIVED_DT_IN", M_DataSet.Tables[0].Rows[0]["Msg_Received_Dt"]).Direction = ParameterDirection.Input;
				_with5.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
				_with5.Add("RETURN_VALUE", intPKVal).Direction = ParameterDirection.Output;
				objWK.MyCommand.CommandText = objWK.MyUserName + ".USER_MESSAGE_TRN_PKG.USER_MESSAGE_TRN_INS";
				objWK.MyCommand.CommandType = CommandType.StoredProcedure;
				if (objWK.MyCommand.ExecuteNonQuery() == 1) {
					M_DataSet.Tables[0].Rows[0]["USER_MESSAGE_PK"] = objWK.MyCommand.Parameters["Return_value"].Value;
					//Dim strURL As String
					//strURL = CType(M_DataSet.Tables(1).Rows(0).Item("Url_Page"), String) & SRRPK
					var _with6 = objWK.MyCommand.Parameters;
					_with6.Clear();
					_with6.Add("USER_MESSAGE_FK_IN", M_DataSet.Tables[0].Rows[0]["USER_MESSAGE_PK"]).Direction = ParameterDirection.Input;
					_with6.Add("ATTACHMENT_CAPTION_IN", M_DataSet.Tables[0].Rows[0]["Msg_Subject"]).Direction = ParameterDirection.Input;
					_with6.Add("ATTACHMENT_DATA_IN", M_DataSet.Tables[1].Rows[0]["Attachment_Data"]).Direction = ParameterDirection.Input;
					objWK.MyCommand.Parameters["ATTACHMENT_DATA_IN"].Size = 200;
					_with6.Add("URL_PAGE_IN", "").Direction = ParameterDirection.Input;
					objWK.MyCommand.Parameters["URL_PAGE_IN"].Size = 100;
					_with6.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
					_with6.Add("RETURN_VALUE", intPKVal).Direction = ParameterDirection.Output;
					objWK.MyCommand.Parameters["RETURN_VALUE"].Size = 10;
					objWK.MyCommand.CommandText = objWK.MyUserName + ".USER_MESSAGE_DET_TRN_PKG.USER_MESSAGE_DET_TRN_INS";
					objWK.MyCommand.CommandType = CommandType.StoredProcedure;
					if (objWK.MyCommand.ExecuteNonQuery() == 1) {
						TRAN.Commit();
						return 1;
					} else {
						TRAN.Rollback();
						return -1;
					}
				} else {
					arrMessage.Add("Record Not Saved");
				}
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				objWK.CloseConnection();
			}
            return 0;
		}
		#endregion

		#region "Save UnApprovedTDR"
		public int SaveUnApprovedTDR(DataSet M_DataSet, int i = 1, string DocRefNr = "")
		{
			WorkFlow objWK = new WorkFlow();
			objWK.OpenConnection();
			OracleTransaction TRAN = null;
			TRAN = objWK.MyConnection.BeginTransaction();
			objWK.MyCommand.Transaction = TRAN;
			objWK.MyCommand.Connection = objWK.MyConnection;
			try {
				int intPKVal = 0;
				long lngI = 0;
				Int32 RecAfct = default(Int32);
				var _with7 = objWK.MyCommand.Parameters;
				_with7.Clear();
				_with7.Add("SENDER_FK_IN", M_DataSet.Tables[0].Rows[0]["Sender_Fk"]).Direction = ParameterDirection.Input;
				_with7.Add("RECEIVER_FK_IN", M_DataSet.Tables[0].Rows[0]["Receiver_Fk"]).Direction = ParameterDirection.Input;
				_with7.Add("MSG_READ_IN", M_DataSet.Tables[0].Rows[0]["Msg_Read"]).Direction = ParameterDirection.Input;
				_with7.Add("FOLLOWUP_FLAG_IN", M_DataSet.Tables[0].Rows[0]["Followup_Flag"]).Direction = ParameterDirection.Input;
				_with7.Add("HAVE_ATTACHMENT_IN", M_DataSet.Tables[0].Rows[0]["Have_Attachment"]).Direction = ParameterDirection.Input;
				_with7.Add("MSG_SUBJECT_IN", M_DataSet.Tables[0].Rows[0]["Msg_Subject"]).Direction = ParameterDirection.Input;
				_with7.Add("MSG_BODY_IN", M_DataSet.Tables[0].Rows[0]["Msg_Body"]).Direction = ParameterDirection.Input;
				_with7.Add("READ_RECEIPT_IN", M_DataSet.Tables[0].Rows[0]["Read_Receipt"]).Direction = ParameterDirection.Input;
				_with7.Add("DOCUMENT_MST_FK_IN", M_DataSet.Tables[0].Rows[0]["Document_Mst_Fk"]).Direction = ParameterDirection.Input;
				_with7.Add("USER_MESSAGE_FOLDERS_FK_IN", M_DataSet.Tables[0].Rows[0]["User_Message_Folders_Fk"]).Direction = ParameterDirection.Input;
				_with7.Add("MSG_RECEIVED_DT_IN", M_DataSet.Tables[0].Rows[0]["Msg_Received_Dt"]).Direction = ParameterDirection.Input;
				_with7.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
				_with7.Add("RETURN_VALUE", intPKVal).Direction = ParameterDirection.Output;
				objWK.MyCommand.CommandText = objWK.MyUserName + ".USER_MESSAGE_TRN_PKG.USER_MESSAGE_TRN_INS";
				objWK.MyCommand.CommandType = CommandType.StoredProcedure;
				if (objWK.MyCommand.ExecuteNonQuery() == 1) {
					M_DataSet.Tables[0].Rows[0]["USER_MESSAGE_PK"] = objWK.MyCommand.Parameters["Return_value"].Value;

					var _with8 = objWK.MyCommand.Parameters;
					_with8.Clear();
					_with8.Add("USER_MESSAGE_FK_IN", M_DataSet.Tables[0].Rows[0]["USER_MESSAGE_PK"]).Direction = ParameterDirection.Input;
					_with8.Add("ATTACHMENT_CAPTION_IN", M_DataSet.Tables[0].Rows[0]["Msg_Subject"]).Direction = ParameterDirection.Input;
					_with8.Add("ATTACHMENT_DATA_IN", M_DataSet.Tables[1].Rows[0]["Attachment_Data"]).Direction = ParameterDirection.Input;
					objWK.MyCommand.Parameters["ATTACHMENT_DATA_IN"].Size = 200;
					_with8.Add("URL_PAGE_IN", M_DataSet.Tables[1].Rows[0]["URL_PAGE"]).Direction = ParameterDirection.Input;

					_with8.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
					_with8.Add("RETURN_VALUE", intPKVal).Direction = ParameterDirection.Output;
					objWK.MyCommand.Parameters["RETURN_VALUE"].Size = 10;
					objWK.MyCommand.CommandText = objWK.MyUserName + ".USER_MESSAGE_DET_TRN_PKG.USER_MESSAGE_DET_TRN_INS";
					objWK.MyCommand.CommandType = CommandType.StoredProcedure;
					if (objWK.MyCommand.ExecuteNonQuery() == 1) {
						TRAN.Commit();
						return 1;
					} else {
						TRAN.Rollback();
						return -1;
					}
				} else {
					arrMessage.Add("Record Not Saved");
				}
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				objWK.CloseConnection();
			}
            return 0;
		}
		#endregion

		#region "Mail Count"
		public int MailCount(int RefType, string RefNr, int RefPK = 0)
		{
			WorkFlow objWF = new WorkFlow();
			string Str = null;
			Str = "select nvl(max(mail_count)+ 1,1) from mail_send_status_tbl mst where mst.mail_type=" + RefType + " and trim(mst.doc_ref_nr)='" + RefNr + "'";
			if (RefPK != 0) {
				Str = Str + " and mst.doc_type_fk=" + RefPK;
			}
			return Convert.ToInt32(objWF.ExecuteScaler(Str));
		}
		#endregion
	}
}
