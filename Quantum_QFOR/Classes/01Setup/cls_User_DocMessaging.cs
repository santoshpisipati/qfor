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
    public class cls_User_DocMessaging : CommonFeatures
	{

		#region "Messaging_Grid"
		public DataTable fn_Messaging_Grid(string MODE = "NEW", long TransPK = 0, long CustPK = 0, bool FilterCriteria = false, int ParyType = 1)
		{

			WorkFlow objWF = new WorkFlow();
			DataTable dt = new DataTable();
			Int32 RecordsFound = default(Int32);

			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);


			///If FilterCriteria = True Then
			///    sb.Append(" select count(*) from (SELECT ")
			///    sb.Append(" V.TRANFK,")
			///    sb.Append(" V.TRANDTLPK,")
			///    sb.Append(" V.DOCUMENTS,")
			///    sb.Append(" V.SMS,")
			///    sb.Append(" V.EMAIL,")
			///    sb.Append(" V.PRINT,")
			///    sb.Append(" V.FAX,")
			///    sb.Append(" V.ALL_MESSAGING,")
			///    sb.Append(" V.CONFIG_FLAG,")
			///    sb.Append(" V.CONFIG_BTN,")
			///    sb.Append(" V.CONFIG_MSG_DS,")
			///    sb.Append(" V.LASTUPD")

			///    sb.Append(" FROM VIEW_DOC_MESSAGING_EDIT_DOC V where 1=1")
			///    If TransPK > 0 Then
			///        sb.Append("  and v.TRANFK = " & TransPK)
			///    End If
			///    If CustPK > 0 Then
			///        sb.Append(" and v.TRANFK in  ")
			///        sb.Append(" ( select c.message_setup_mst_fk from qcor_mc_m_message_setup_cust c where c.cust_mst_fk=" & CustPK & ")")
			///    End If
			///End If



			///sb.Length = 0
			if (FilterCriteria == false) {
				//Fetching for new record
				if (MODE == "NEW") {
					sb.Append(" select * from (SELECT ");
					sb.Append(" V.TRANFK,");
					sb.Append(" V.TRANDTLPK,");
					sb.Append(" V.DOCUMENTS,");
					sb.Append(" V.SMS,");
					sb.Append(" V.EMAIL,");
					sb.Append(" V.PRINT,");
					sb.Append(" V.FAX,");
					sb.Append(" V.ALL_MESSAGING,");
					sb.Append(" V.CONFIG_FLAG,");
					sb.Append(" V.CONFIG_BTN,");
					sb.Append(" V.CONFIG_MSG_DS,");
					sb.Append(" V.LASTMODIFIED_PK,");
					sb.Append(" V.LASTMODIFIED,");
					sb.Append(" V.LASTUPD,");
					sb.Append(" V.SMS H_SMS,");
					sb.Append(" V.EMAIL H_EMAIL,");
					sb.Append(" V.PRINT H_PRINT,");
					sb.Append(" V.FAX H_FAX,");
					sb.Append(" V.ALL_MESSAGING H_ALL_MESSAGING,");
					sb.Append(" V.LASTMODIFIED_PK H_LASTMODIFIED_PK,");
					sb.Append(" V.LASTMODIFIED H_LASTMODIFIED,");
					sb.Append(" V.LASTUPD H_LASTUPD,");
					sb.Append(" V.DISPLAY_ORDER");
					sb.Append(" FROM VIEW_DOC_MESSAGING V ");
					sb.Append(" WHERE V.PARTY_TYPE=" + ParyType);
					sb.Append(" )q order by q.DISPLAY_ORDER");
					// sb.Append(" )q order by q.TRANDTLPK")
				}
				if (MODE == "COPYFROM") {
					sb.Append(" select * from (SELECT ");
					sb.Append(" V.TRANFK,");
					sb.Append(" V.TRANDTLPK,");
					sb.Append(" V.DOCUMENTS,");
					sb.Append(" V.SMS,");
					sb.Append(" V.EMAIL,");
					sb.Append(" V.PRINT,");
					sb.Append(" V.FAX,");
					sb.Append(" V.ALL_MESSAGING,");
					sb.Append(" V.CONFIG_FLAG,");
					sb.Append(" V.CONFIG_BTN,");
					sb.Append(" V.CONFIG_MSG_DS,");
					sb.Append(" V.LASTMODIFIED_PK,");
					sb.Append(" V.LASTMODIFIED,");
					sb.Append(" V.LASTUPD,");
					sb.Append(" V.SMS H_SMS,");
					sb.Append(" V.EMAIL H_EMAIL,");
					sb.Append(" V.PRINT H_PRINT,");
					sb.Append(" V.FAX H_FAX,");
					sb.Append(" V.ALL_MESSAGING H_ALL_MESSAGING,");
					sb.Append(" V.LASTMODIFIED_PK H_LASTMODIFIED_PK,");
					sb.Append(" V.LASTMODIFIED H_LASTMODIFIED,");
					sb.Append(" V.LASTUPD H_LASTUPD,");
					sb.Append(" V.DISPLAY_ORDER ");
					//fetching for copy from 
					if (CustPK > 0) {
						sb.Append(" FROM VIEW_DOC_MESSAGING_EDIT_DOC V ");
						sb.Append(" , qcor_mc_m_message_setup_cust c ");
						sb.Append(" where c.message_setup_mst_fk = v.TRANFK ");
						sb.Append(" and c.cust_mst_fk=" + CustPK);
						sb.Append(" and V.PARTY_TYPE=" + ParyType);
						sb.Append(" UNION ");
						sb.Append("  SELECT 0   TRANFK, ");
						sb.Append(" DOC.DOCUMENT_MST_PK TRANDTLPK, ");
						sb.Append(" DOC.DOCUMENT_NAME   DOCUMENTS, ");
						sb.Append(" '0' SMS, ");
						sb.Append(" '0' EMAIL,");
						sb.Append(" '0'   PRINT,");
						sb.Append("  '0'  FAX, ");
						sb.Append("  '0'     ALL_MESSAGING, ");
						sb.Append("  NULL    CONFIG_FLAG, ");
						sb.Append("  NULL    CONFIG_BTN, ");
						sb.Append("  NULL   CONFIG_MSG_DS,");
						sb.Append(" NULL    LASTMODIFIED,");
						sb.Append(" NULL    LASTMODIFIED_PK,");
						sb.Append("   NULL LASTUPD,");
						sb.Append(" '0' H_SMS, ");
						sb.Append(" '0' H_EMAIL,");
						sb.Append(" '0' H_PRINT,");
						sb.Append("  '0' H_FAX, ");
						sb.Append("  '0'   H_ALL_MESSAGING, ");
						sb.Append(" NULL H_LASTMODIFIED_PK,");
						sb.Append(" NULL H_LASTMODIFIED,");
						sb.Append(" NULL H_LASTUPD,");
						sb.Append(" DOC.DISPLAY_ORDER ");
						sb.Append(" FROM QCOR_MC_M_DOCUMENTS DOC ");
						sb.Append(" WHERE DOC.DOCUMENT_MST_PK NOT IN ");
						sb.Append("  (SELECT D.DOCUMENT_MST_FK ");
						sb.Append(" FROM QCOR_MC_M_MESSAGE_SETUP_DOCS D , qcor_mc_m_message_setup_cust c");
						sb.Append(" where c.message_setup_mst_fk = d.message_setup_mst_fk");
						sb.Append(" and c.cust_mst_fk = " + CustPK + ") ");
						sb.Append(" and DOC.PARTY_TYPE=" + ParyType);
						sb.Append(" )q order by q.DISPLAY_ORDER ");
						// sb.Append(" )q order by q.TRANDTLPK ")

					}
				}
				if (MODE == "EDITAFTERSAVE") {
					sb.Append(" select * from (SELECT ");
					sb.Append(" V.TRANFK,");
					sb.Append(" V.TRANDTLPK,");
					sb.Append(" V.DOCUMENTS,");
					sb.Append(" V.SMS,");
					sb.Append(" V.EMAIL,");
					sb.Append(" V.PRINT,");
					sb.Append(" V.FAX,");
					sb.Append(" V.ALL_MESSAGING,");
					sb.Append(" V.CONFIG_FLAG,");
					sb.Append(" V.CONFIG_BTN,");
					sb.Append(" V.CONFIG_MSG_DS,");
					sb.Append(" V.LASTMODIFIED_PK,");
					sb.Append(" V.LASTMODIFIED,");
					sb.Append(" V.LASTUPD,");
					sb.Append(" V.SMS H_SMS,");
					sb.Append(" V.EMAIL H_EMAIL,");
					sb.Append(" V.PRINT H_PRINT,");
					sb.Append(" V.FAX H_FAX,");
					sb.Append(" V.ALL_MESSAGING H_ALL_MESSAGING,");
					sb.Append(" V.LASTMODIFIED_PK H_LASTMODIFIED_PK,");
					sb.Append(" V.LASTMODIFIED H_LASTMODIFIED,");
					sb.Append(" V.LASTUPD H_LASTUPD,");
					sb.Append(" V.DISPLAY_ORDER ");
					sb.Append(" FROM VIEW_DOC_MESSAGING_EDIT_DOC V");
					sb.Append(" WHERE V.TRANFK in (" + TransPK + ",0) ");
					sb.Append(" UNION ");
					sb.Append(" SELECT 0                   TRANFK, ");
					sb.Append(" DOC.DOCUMENT_MST_PK TRANDTLPK, ");
					sb.Append(" DOC.DOCUMENT_NAME   DOCUMENTS, ");
					sb.Append(" '0' SMS, ");
					sb.Append(" '0'                    EMAIL,");
					sb.Append(" '0'                    PRINT,");
					sb.Append(" '0'                   FAX, ");
					sb.Append(" '0'                   ALL_MESSAGING, ");
					sb.Append(" NULL                CONFIG_FLAG, ");
					sb.Append(" NULL                CONFIG_BTN, ");
					sb.Append(" NULL                CONFIG_MSG_DS,");
					sb.Append(" NULL                LASTMODIFIED_PK,");
					sb.Append(" NULL                LASTMODIFIED,");
					sb.Append(" SYSDATE             LASTUPD, ");
					sb.Append(" '0' H_SMS, ");
					sb.Append(" '0' H_EMAIL,");
					sb.Append(" '0' H_PRINT,");
					sb.Append("  '0' H_FAX, ");
					sb.Append("  '0'   H_ALL_MESSAGING, ");
					sb.Append(" NULL H_LASTMODIFIED_PK,");
					sb.Append(" NULL H_LASTMODIFIED,");
					sb.Append(" NULL H_LASTUPD,");
					sb.Append(" DOC.DISPLAY_ORDER ");
					sb.Append(" FROM QCOR_MC_M_DOCUMENTS DOC ");
					sb.Append(" WHERE DOC.DOCUMENT_MST_PK NOT IN ");
					sb.Append("     (SELECT D.DOCUMENT_MST_FK ");
					sb.Append(" FROM QCOR_MC_M_MESSAGE_SETUP_DOCS D ");
					sb.Append(" WHERE D.MESSAGE_SETUP_MST_FK = " + TransPK + ")");
					sb.Append(" and DOC.PARTY_TYPE=" + ParyType);
					sb.Append(")q order by q.DISPLAY_ORDER");
					//sb.Append(")q order by q.TRANDTLPK")
				}

			}
			if (FilterCriteria == true) {

				if (MODE == "EDIT") {
					sb.Append(" select COUNT(*) from (SELECT ");
					sb.Append(" V.TRANFK,");
					sb.Append(" V.TRANDTLPK,");
					sb.Append(" V.DOCUMENTS,");
					sb.Append(" V.SMS,");
					sb.Append(" V.EMAIL,");
					sb.Append(" V.PRINT,");
					sb.Append(" V.FAX,");
					sb.Append(" V.ALL_MESSAGING,");
					sb.Append(" V.CONFIG_FLAG,");
					sb.Append(" V.CONFIG_BTN,");
					sb.Append(" V.CONFIG_MSG_DS,");
					sb.Append(" V.LASTMODIFIED_PK,");
					sb.Append(" V.LASTMODIFIED,");
					sb.Append(" V.LASTUPD,");
					sb.Append(" V.SMS H_SMS,");
					sb.Append(" V.EMAIL H_EMAIL,");
					sb.Append(" V.PRINT H_PRINT,");
					sb.Append(" V.FAX H_FAX,");
					sb.Append(" V.ALL_MESSAGING H_ALL_MESSAGING,");
					sb.Append(" V.LASTMODIFIED_PK H_LASTMODIFIED_PK,");
					sb.Append(" V.LASTMODIFIED H_LASTMODIFIED,");
					sb.Append(" V.LASTUPD H_LASTUPD,");
					sb.Append(" V.DISPLAY_ORDER ");
					sb.Append(" FROM VIEW_DOC_MESSAGING_EDIT_DOC V where 1=1");
					sb.Append(" and V.PARTY_TYPE=" + ParyType);
					if (TransPK > 0) {
						sb.Append("  and v.TRANFK = " + TransPK);
					}
					if (CustPK > 0) {
						sb.Append(" and v.TRANFK in  ");
						sb.Append(" ( select c.message_setup_mst_fk from qcor_mc_m_message_setup_cust c where c.cust_mst_fk=" + CustPK + ")");
					}
					sb.Append(" )  ");
					if ((objWF.ExecuteScaler(sb.ToString()) != null)) {
						RecordsFound = Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
					} else {
						RecordsFound = 0;
					}
					sb.Length = 0;
					sb.Append(" select * from (SELECT ");
					sb.Append(" V.TRANFK,");
					sb.Append(" V.TRANDTLPK,");
					sb.Append(" V.DOCUMENTS,");
					sb.Append(" V.SMS,");
					sb.Append(" V.EMAIL,");
					sb.Append(" V.PRINT,");
					sb.Append(" V.FAX,");
					sb.Append(" V.ALL_MESSAGING,");
					sb.Append(" V.CONFIG_FLAG,");
					sb.Append(" V.CONFIG_BTN,");
					sb.Append(" V.CONFIG_MSG_DS,");
					sb.Append(" V.LASTMODIFIED_PK,");
					sb.Append(" V.LASTMODIFIED,");
					sb.Append(" V.LASTUPD,");
					sb.Append(" V.SMS H_SMS,");
					sb.Append(" V.EMAIL H_EMAIL,");
					sb.Append(" V.PRINT H_PRINT,");
					sb.Append(" V.FAX H_FAX,");
					sb.Append(" V.ALL_MESSAGING H_ALL_MESSAGING,");
					sb.Append(" V.LASTMODIFIED_PK H_LASTMODIFIED_PK,");
					sb.Append(" V.LASTMODIFIED H_LASTMODIFIED,");
					sb.Append(" V.LASTUPD H_LASTUPD,");
					sb.Append(" V.DISPLAY_ORDER ");
					sb.Append(" FROM VIEW_DOC_MESSAGING_EDIT_DOC V where 1=1");
					sb.Append(" and V.PARTY_TYPE=" + ParyType);
					if (TransPK > 0) {
						sb.Append("  and v.TRANFK = " + TransPK);
					}
					if (CustPK > 0) {
						sb.Append(" and v.TRANFK in  ");
						sb.Append(" ( select c.message_setup_mst_fk from qcor_mc_m_message_setup_cust c where c.cust_mst_fk=" + CustPK + ")");
					}
					sb.Append("    UNION ");
					sb.Append("       SELECT 0   TRANFK,");
					sb.Append("    DOC.DOCUMENT_MST_PK TRANDTLPK,  ");
					sb.Append("       DOC.DOCUMENT_NAME   DOCUMENTS, ");
					sb.Append("        '0'   SMS, ");
					sb.Append("   '0' EMAIL,");
					sb.Append("    '0'   PRINT,");
					sb.Append("        '0'  FAX, ");
					sb.Append("       '0'    ALL_MESSAGING, ");
					sb.Append("          NULL    CONFIG_FLAG, ");
					sb.Append("        NULL    CONFIG_BTN, ");
					sb.Append("        NULL   CONFIG_MSG_DS,");
					sb.Append("       NULL            LASTMODIFIED_PK,");
					sb.Append("        NULL  LASTMODIFIED,");
					sb.Append("    SYSDATE LASTUPD,");
					sb.Append(" '0' H_SMS, ");
					sb.Append(" '0' H_EMAIL,");
					sb.Append(" '0' H_PRINT,");
					sb.Append("  '0' H_FAX, ");
					sb.Append("  '0'   H_ALL_MESSAGING, ");
					sb.Append(" NULL H_LASTMODIFIED_PK,");
					sb.Append(" NULL H_LASTMODIFIED,");
					sb.Append(" NULL H_LASTUPD,");
					sb.Append("    DOC.DISPLAY_ORDER");
					sb.Append("      FROM QCOR_MC_M_DOCUMENTS DOC, qcor_mc_m_message_setup cc, user_mst_tbl umt ");
					sb.Append("       WHERE DOC.DOCUMENT_MST_PK NOT IN ");
					sb.Append("      (SELECT D.DOCUMENT_MST_FK ");
					sb.Append("       FROM QCOR_MC_M_MESSAGE_SETUP_DOCS D , qcor_mc_m_message_setup_cust c");
					sb.Append("        where c.message_setup_mst_fk = d.message_setup_mst_fk");
					if (CustPK > 0) {
						sb.Append("     and c.cust_mst_fk = " + CustPK);
					}
					if (TransPK > 0) {
						sb.Append("      and D.MESSAGE_SETUP_MST_FK = " + TransPK);
					}
					sb.Append(" ) ");
					sb.Append(" and DOC.PARTY_TYPE=" + ParyType);
					sb.Append(")q ");
					if (RecordsFound == 0) {
						sb.Append(" where 1=2 ");
					}
					sb.Append(" order by q.DISPLAY_ORDER ");
					//sb.Append(" order by q.TRANDTLPK ")
				}
			}
			///''sb.Append(" select * from (SELECT ")
			///''sb.Append(" V.TRANFK,")
			///''sb.Append(" V.TRANDTLPK,")
			///''sb.Append(" V.DOCUMENTS,")
			///''sb.Append(" V.SMS,")
			///''sb.Append(" V.EMAIL,")
			///''sb.Append(" V.PRINT,")
			///''sb.Append(" V.FAX,")
			///''sb.Append(" V.ALL_MESSAGING,")
			///''sb.Append(" V.CONFIG_FLAG,")
			///''sb.Append(" V.CONFIG_BTN,")
			///''sb.Append(" V.CONFIG_MSG_DS,")
			///''sb.Append(" V.LASTUPD")
			///''If FilterCriteria = True Then
			///''    sb.Append(" FROM VIEW_DOC_MESSAGING_EDIT_DOC V where 1=1")
			///''    If TransPK > 0 Then
			///''        sb.Append("  and v.TRANFK = " & TransPK)
			///''    End If
			///''    If CustPK > 0 Then
			///''        sb.Append(" and v.TRANFK in  ")
			///''        sb.Append(" ( select c.message_setup_mst_fk from qcor_mc_m_message_setup_cust c where c.cust_mst_fk=" & CustPK & ")")
			///''    End If
			///''    sb.Append("    UNION ")
			///''    sb.Append("       SELECT 0   TRANFK,")
			///''    sb.Append("    DOC.DOCUMENT_MST_PK TRANDTLPK,  ")
			///''    sb.Append("       DOC.DOCUMENT_NAME   DOCUMENTS, ")
			///''    sb.Append("       0  SMS, ")
			///''    sb.Append("   0 EMAIL,")
			///''    sb.Append("    0   PRINT,")
			///''    sb.Append("        0  FAX, ")
			///''    sb.Append("       0     ALL_MESSAGING, ")
			///''    sb.Append("          NULL    CONFIG_FLAG, ")
			///''    sb.Append("        NULL    CONFIG_BTN, ")
			///''    sb.Append("        NULL   CONFIG_MSG_DS,")
			///''    sb.Append("    SYSDATE LASTUPD")
			///''    sb.Append("      FROM QCOR_MC_M_DOCUMENTS DOC ")
			///''    sb.Append("       WHERE DOC.DOCUMENT_MST_PK NOT IN ")
			///''    sb.Append("      (SELECT D.DOCUMENT_MST_FK ")
			///''    sb.Append("       FROM QCOR_MC_M_MESSAGE_SETUP_DOCS D , qcor_mc_m_message_setup_cust c")
			///''    sb.Append("        where c.message_setup_mst_fk = d.message_setup_mst_fk")
			///''    If CustPK > 0 Then
			///''        sb.Append("     and c.cust_mst_fk = " & CustPK)
			///''    End If
			///''    If TransPK > 0 Then
			///''        sb.Append("      and D.MESSAGE_SETUP_MST_FK = " & TransPK)
			///''    End If
			///''    sb.Append(" ) ")
			///''Else
			///''    If CustPK > 0 Then 'fetching for copy from 
			///''        sb.Append(" FROM VIEW_DOC_MESSAGING_EDIT_DOC V ")
			///''        sb.Append(" , qcor_mc_m_message_setup_cust c ")
			///''        sb.Append(" where c.message_setup_mst_fk = v.TRANFK ")
			///''        sb.Append(" and c.cust_mst_fk=" & CustPK)
			///''        sb.Append(" UNION ")
			///''        sb.Append("  SELECT 0   TRANFK, ")
			///''        sb.Append(" DOC.DOCUMENT_MST_PK TRANDTLPK, ")
			///''        sb.Append(" DOC.DOCUMENT_NAME   DOCUMENTS, ")
			///''        sb.Append(" 0  SMS, ")
			///''        sb.Append(" 0 EMAIL,")
			///''        sb.Append(" 0   PRINT,")
			///''        sb.Append("  0  FAX, ")
			///''        sb.Append("  0     ALL_MESSAGING, ")
			///''        sb.Append("  NULL    CONFIG_FLAG, ")
			///''        sb.Append("  NULL    CONFIG_BTN, ")
			///''        sb.Append("  NULL   CONFIG_MSG_DS,")
			///''        sb.Append("   SYSDATE LASTUPD")
			///''        sb.Append(" FROM QCOR_MC_M_DOCUMENTS DOC ")
			///''        sb.Append(" WHERE DOC.DOCUMENT_MST_PK NOT IN ")
			///''        sb.Append("  (SELECT D.DOCUMENT_MST_FK ")
			///''        sb.Append(" FROM QCOR_MC_M_MESSAGE_SETUP_DOCS D , qcor_mc_m_message_setup_cust c")
			///''        sb.Append(" where c.message_setup_mst_fk = d.message_setup_mst_fk")
			///''        sb.Append(" and c.cust_mst_fk = " & CustPK & ") ")
			///''    ElseIf MODE = "NEW" Then 'Fetching for new record
			///''        sb.Append(" FROM VIEW_DOC_MESSAGING V ")
			///''    ElseIf MODE = "EDIT" Then 'fetching for edit after selection of existing protocol
			///''        sb.Append(" FROM VIEW_DOC_MESSAGING_EDIT_DOC V")
			///''        sb.Append(" WHERE V.TRANFK in (" & TransPK & ",0) ")
			///''        sb.Append(" UNION ")
			///''        sb.Append(" SELECT 0                   TRANFK, ")
			///''        sb.Append(" DOC.DOCUMENT_MST_PK TRANDTLPK, ")
			///''        sb.Append(" DOC.DOCUMENT_NAME   DOCUMENTS, ")
			///''        sb.Append(" 0                   SMS, ")
			///''        sb.Append(" 0                    EMAIL,")
			///''        sb.Append(" 0                    PRINT,")
			///''        sb.Append(" 0                   FAX, ")
			///''        sb.Append(" 0                   ALL_MESSAGING, ")
			///''        sb.Append(" NULL                CONFIG_FLAG, ")
			///''        sb.Append(" NULL                CONFIG_BTN, ")
			///''        sb.Append(" NULL                CONFIG_MSG_DS,")
			///''        sb.Append(" SYSDATE             LASTUPD ")
			///''        sb.Append(" FROM QCOR_MC_M_DOCUMENTS DOC ")
			///''        sb.Append(" WHERE DOC.DOCUMENT_MST_PK NOT IN ")
			///''        sb.Append("     (SELECT D.DOCUMENT_MST_FK ")
			///''        sb.Append(" FROM QCOR_MC_M_MESSAGE_SETUP_DOCS D ")
			///''        sb.Append(" WHERE D.MESSAGE_SETUP_MST_FK = " & TransPK & ")")
			///''    End If
			///''End If
			///''sb.Append(")q order by q.DOCUMENTS")

			try {
				dt = objWF.GetDataTable(sb.ToString());
				return dt;
			//Manjunath  PTS ID:Sep-02   12/09/2011
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Messaging_Grid_MailMsg"
		public DataTable fn_Messaging_Grid_MailMsg(string TransPK, string TransDtlPK)
		{
			WorkFlow objWF = new WorkFlow();
			DataTable dt = new DataTable();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

			if (Convert.ToInt32(TransPK) > 0) {
				sb.Append("  select ");
				sb.Append(" msg.TRANFK,msg.TRANDTLPK, nvl(msg.EMAIL_SUB,docs.default_email_subject) EMAIL_SUB , ");
				sb.Append(" nvl(msg.EMAIL_MSG,docs.default_email_msg) EMAIL_MSG , ");
				sb.Append(" nvl(msg.SMS_MSG,docs.default_sms_msg) SMS_MSG, msg.ATTACH_FLAG, ");
				sb.Append("   msg.CONSIGNEE_FLAG,");
				sb.Append("   msg.DP_AGENT_FLAG,");
				sb.Append("   msg.HBL_ATTACH_FLAG,");
				sb.Append("   msg.CARGO_MANIFEST_FLAG,");
				sb.Append("   msg.FREIGHT_DETAILS_FLAG,");
				sb.Append("   msg.NOTIFY_PARTY_FLAG,");
				sb.Append("   msg.NOTIFY_ACTIVE,");
				sb.Append("   msg.NOTIFY_BEFORE,");
				sb.Append("   msg.NOTIFY_FREQUENCY ");
				sb.Append(" from ");
				sb.Append(" VIEW_DOC_MESSAGING_EDIT_DOCMSG msg, ");
				sb.Append(" qcor_mc_m_documents docs ");
				sb.Append(" where docs.document_mst_pk=msg.TRANDTLPK ");
				sb.Append(" and msg.TRANFK=" + TransPK + " ");
				sb.Append(" and msg.TRANDTLPK= " + TransDtlPK + " ");
			} else {
				sb.Append("  select ");
				sb.Append("   DOCS.DEFAULT_EMAIL_SUBJECT  EMAIL_SUB, ");
				sb.Append("   DOCS.DEFAULT_EMAIL_MSG  EMAIL_MSG ,");
				sb.Append("   DOCS.DEFAULT_SMS_MSG SMS_MSG,");
				sb.Append("   0 ATTACH_FLAG ,");
				sb.Append("   1 CONSIGNEE_FLAG,");
				sb.Append("   1 DP_AGENT_FLAG,");
				sb.Append("   1 HBL_ATTACH_FLAG,");
				sb.Append("   1 CARGO_MANIFEST_FLAG,");
				sb.Append("   1 FREIGHT_DETAILS_FLAG,");
				sb.Append("   0 NOTIFY_PARTY_FLAG,");
				sb.Append("   1 NOTIFY_ACTIVE,");
				sb.Append("   0 NOTIFY_BEFORE,");
				sb.Append("   1 NOTIFY_FREQUENCY ");
				sb.Append("  FROM QCOR_MC_M_DOCUMENTS DOCS ");
				sb.Append("    WHERE DOCS.DOCUMENT_MST_PK = " + TransDtlPK + " ");
			}

			try {
				dt = objWF.GetDataTable(sb.ToString());
				return dt;
			//Manjunath  PTS ID:Sep-02   12/09/2011
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion
		#region "Get Single Customer"
		public object fn_GetSingleCustomer(string TransPK, string CustomerPk)
		{
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			sb.Append("SELECT T.CUSTOMER_NAME, T.CUSTOMER_TYPE, S.PROTOCOL_NAME");
			sb.Append("");
			sb.Append("  FROM CUSTOMER_MST_TBL             T,");
			sb.Append("       QCOR_MC_M_MESSAGE_SETUP_CUST C,");
			sb.Append("       QCOR_MC_M_MESSAGE_SETUP      S");
			sb.Append(" WHERE T.CUSTOMER_MST_PK = C.CUST_MST_FK");
			sb.Append("   AND S.MESSAGE_SETUP_MST_PK = C.MESSAGE_SETUP_MST_FK");
			sb.Append("   AND S.MESSAGE_SETUP_MST_PK = " + TransPK);
			sb.Append("   AND T.CUSTOMER_MST_PK = " + CustomerPk);
			try {
				return objWF.GetDataSet(sb.ToString());
			} catch (Exception ex) {
				throw ex;
			}

		}
		#endregion

		#region "Get Trans Customer"
		public DataSet fn_GetTransCustomer(string TransPK)
		{
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

			sb.Append("  SELECT ");
			sb.Append("  M.PROTOCOL_NAME, M.SETUP_DATE ,");
			sb.Append("   PKG_QCOR_MC_M_MESSAGE_SETUP.FN_CONCAT_CUST(" + TransPK + ") CUSTNAME");
			sb.Append("   FROM ");
			sb.Append("   QCOR_MC_M_MESSAGE_SETUP M ");
			sb.Append("   WHERE ");
			sb.Append("   M.MESSAGE_SETUP_MST_PK = " + TransPK);

			try {
				return objWF.GetDataSet(sb.ToString());
			//Manjunath  PTS ID:Sep-02   12/09/2011
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Messaging Grid Documentaion Fields"
		public DataTable fn_Messaging_Grid_DocFields(string DocPK)
		{
			WorkFlow objWF = new WorkFlow();
			DataTable dt = new DataTable();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

			sb.Append(" SELECT ");
			sb.Append(" rownum slnr, v.DB_FIELD_NAME,  v.DB_FIELD_DESC ");
			sb.Append(" from ");
			sb.Append(" view_doc_messaging_doc_fields v");
			sb.Append(" where v.ACTIVE_FLAG=1");
			sb.Append(" and v.DOCUMENT_MST_FK=" + DocPK + " ");
			try {
				dt = objWF.GetDataTable(sb.ToString());
				return dt;
			//Manjunath  PTS ID:Sep-02   12/09/2011
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Check if protocol id exists"
		public bool fn_Check_Protocol_ID_Exists(string Protocol)
		{
			WorkFlow objWF = new WorkFlow();
			Int32 RowCount = default(Int32);
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

			sb.Append("select count(*) from qcor_mc_m_message_setup m where m.protocol_name='" + Protocol + "'");
			try {
				RowCount = Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
				if (RowCount > 0) {
					return true;
					//entered Protocol ID Exists
				} else {
					return false;
					// entered Protocol ID Does not Exists
				}
			//Manjunath  PTS ID:Sep-02   12/09/2011
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion


		#region "Saving the Message"
		public ArrayList fn_Save_DocMessaging(string Protocol, Int32 LOGED_IN_BRANCH, Int32 userPk, System.DateTime SetUpDate, string CustomersPk, long MsgSetUpPk, DataSet dsGridData, DataSet EmailDet_Ds, long CopyFromCustPK = 0, string PartyFlag = "",
		string From = "", string EmailSetup_PKs = "", int Party_Type = 1)
		{
			ArrayList functionReturnValue = null;

			OracleTransaction TRAN = null;
			WorkFlow objWK = new WorkFlow();
			objWK.OpenConnection();
			string ProtocolName = "";
			string StdProtocol = "";
			DataSet DSProtocol = null;
			int i = 0;

			try {
				//If MsgSetUpPk = 0 Then 'new record
				//    '''PROTOCOL
				//    Protocol = fn_GenerateProtocol("QCOR-M-119", LOGED_IN_BRANCH, userPk, "", "", "")
				//    If InStr(Protocol, "Protocol is not defined") > 0 Then
				//        arrMessage.Add("Protocol is not defined.")
				//        Return arrMessage
				//        Exit Function
				//    End If
				//End If
				if (MsgSetUpPk > 0) {
					StdProtocol = GetStdProtocol(Convert.ToString(MsgSetUpPk));
					if ((StdProtocol != null)) {
						StdProtocol = StdProtocol.ToUpper();
						Protocol = Protocol.ToUpper();
						//"STDPROTOCOL" is standard protocol stored in database
						if (StdProtocol == "STDPROTOCOL" & StdProtocol != Protocol) {
							MsgSetUpPk = 0;
						}
					}
				}
				DSProtocol = (DataSet)GetProtocol(Convert.ToString(CustomersPk));
				if (DSProtocol.Tables[0].Rows.Count > 0) {
					for (i = 0; i <= DSProtocol.Tables[0].Rows.Count - 1; i++) {
						if (DSProtocol.Tables[0].Rows[i]["PROTOCOL_NAME"].ToString() != "STDPROTOCOL") {
							ProtocolName = Convert.ToString(DSProtocol.Tables[0].Rows[i]["PROTOCOL_NAME"]);
						}
					}
					if (!string.IsNullOrEmpty(ProtocolName) & Protocol != ProtocolName) {
						arrMessage.Add("Selected Customer(s) already belongs to " + ProtocolName + " Protocol");
						return arrMessage;
						return functionReturnValue;
					}
				}
				//'
				//new record
				if (MsgSetUpPk == 0 | (MsgSetUpPk == 0 & CopyFromCustPK > 0)) {

					//entered protocol id exists
					if (fn_Check_Protocol_ID_Exists(Protocol) == true) {
						arrMessage.Add("Protocol ID Already Exists. Enter a different ID");
						return arrMessage;
						return functionReturnValue;
					}
				}


				TRAN = objWK.MyConnection.BeginTransaction();

				OracleCommand insCommand = new OracleCommand();
				OracleCommand updCommand = new OracleCommand();

				//new record
				if (MsgSetUpPk == 0) {

					var _with1 = insCommand;
					_with1.Connection = objWK.MyConnection;
					_with1.CommandType = CommandType.StoredProcedure;
					_with1.CommandText = objWK.MyUserName + ".PKG_QCOR_MC_M_MESSAGE_SETUP.QCOR_MC_M_MESSAGE_SETUP_INS";
					var _with2 = _with1.Parameters;

					insCommand.Parameters.Add("PROTOCOL_NAME_IN", Protocol).Direction = ParameterDirection.Input;
					insCommand.Parameters["PROTOCOL_NAME_IN"].SourceVersion = DataRowVersion.Current;

					insCommand.Parameters.Add("SETUP_DATE_IN", SetUpDate).Direction = ParameterDirection.Input;
					insCommand.Parameters["SETUP_DATE_IN"].SourceVersion = DataRowVersion.Current;

					insCommand.Parameters.Add("ACTIVE_FLAG_IN", 1).Direction = ParameterDirection.Input;
					insCommand.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;

					insCommand.Parameters.Add("PARTY_TYPE_IN", Party_Type).Direction = ParameterDirection.Input;
					insCommand.Parameters["PARTY_TYPE_IN"].SourceVersion = DataRowVersion.Current;

					insCommand.Parameters.Add("CREATED_BY_FK_IN", CREATED_BY).Direction = ParameterDirection.Input;
					insCommand.Parameters.Add("RETURN_VALUE", MsgSetUpPk).Direction = ParameterDirection.Output;
                    
					var _with3 = objWK.MyDataAdapter;
					_with3.InsertCommand = insCommand;
					_with3.InsertCommand.Transaction = TRAN;
					_with3.InsertCommand.ExecuteNonQuery();
					MsgSetUpPk = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
					if (MsgSetUpPk > 0) {
						fn_SaveCustomer(TRAN, Convert.ToInt32(MsgSetUpPk), CustomersPk, "NEW", PartyFlag, From);
						fn_SaveDocGrid(TRAN, Convert.ToInt32(MsgSetUpPk), dsGridData, EmailDet_Ds, EmailSetup_PKs);
						TRAN.Commit();
						arrMessage.Add("All Data Saved Successfully");

						return arrMessage;
					}
				//update record
				} else {
					var _with4 = updCommand;
					_with4.Connection = objWK.MyConnection;
					_with4.CommandType = CommandType.StoredProcedure;
					_with4.CommandText = objWK.MyUserName + ".PKG_QCOR_MC_M_MESSAGE_SETUP.QCOR_MC_M_MESSAGE_SETUP_UPD";
					var _with5 = _with4.Parameters;
					updCommand.Parameters.Add("MESSAGE_SETUP_MST_PK_IN", MsgSetUpPk).Direction = ParameterDirection.Input;
					updCommand.Parameters["MESSAGE_SETUP_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

					updCommand.Parameters.Add("PROTOCOL_NAME_IN", Protocol).Direction = ParameterDirection.Input;
					updCommand.Parameters["PROTOCOL_NAME_IN"].SourceVersion = DataRowVersion.Current;

					updCommand.Parameters.Add("SETUP_DATE_IN", SetUpDate).Direction = ParameterDirection.Input;
					updCommand.Parameters["SETUP_DATE_IN"].SourceVersion = DataRowVersion.Current;

					updCommand.Parameters.Add("ACTIVE_FLAG_IN", 1).Direction = ParameterDirection.Input;
					updCommand.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;

					updCommand.Parameters.Add("PARTY_TYPE_IN", Party_Type).Direction = ParameterDirection.Input;
					updCommand.Parameters["PARTY_TYPE_IN"].SourceVersion = DataRowVersion.Current;

					updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
					updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
					//updCommand.Parameters.Add("RETURN_VALUE", MsgSetUpPk).Direction = ParameterDirection.Output
                    
					var _with6 = objWK.MyDataAdapter;
					_with6.UpdateCommand = updCommand;
					_with6.UpdateCommand.Transaction = TRAN;
					_with6.UpdateCommand.ExecuteNonQuery();
					MsgSetUpPk = Convert.ToInt32(updCommand.Parameters["RETURN_VALUE"].Value);
					if (MsgSetUpPk > 0) {
						fn_SaveCustomer(TRAN, Convert.ToInt32(MsgSetUpPk), CustomersPk, "EDIT", PartyFlag, From);
						fn_SaveDocGrid(TRAN, Convert.ToInt32(MsgSetUpPk), dsGridData, EmailDet_Ds, EmailSetup_PKs, "EDIT");
						TRAN.Commit();
						arrMessage.Add("All Data Saved Successfully");

						return arrMessage;
					}

				}
				// end of insert/update

			} catch (OracleException oraexp) {
				TRAN.Rollback();
				arrMessage.Add(oraexp.Message);
				return arrMessage;
			} catch (Exception ex) {
				TRAN.Rollback();
				arrMessage.Add(ex.Message);
				return arrMessage;
				//Manjunath  PTS ID:Sep-02   12/09/2011
			} finally {
				objWK.MyCommand.Connection.Close();
			}
			return functionReturnValue;
		}
		public int fn_SaveCustomer(OracleTransaction TRAN, Int32 MsgSetUpPk, string customerpk, string Mode = "NEW", string PartyFlag = "", string From = "")
		{
			try {
				Int32 i = default(Int32);			
				string strsql = null;
				Array flags = ("1,1").Split(',');
				Array pks = customerpk.Split(',');
				WorkFlow objWK = new WorkFlow();
				OracleCommand insCommand = new OracleCommand();
				objWK.MyConnection = TRAN.Connection;
				objWK.OpenConnection();

				if (Mode == "EDIT") {
					var _with7 = insCommand;
					_with7.Connection = objWK.MyConnection;
					_with7.CommandType = CommandType.Text;
					_with7.Transaction = TRAN;
					if (From == "CustomerProfile") {
						strsql = "delete from qcor_mc_m_message_setup_cust c  where c.message_setup_mst_fk = " + MsgSetUpPk + " and c.cust_mst_fk = " + customerpk;
					} else {
						strsql = "delete from qcor_mc_m_message_setup_cust c  where c.message_setup_mst_fk = " + MsgSetUpPk;
					}
					_with7.CommandText = strsql;
					_with7.ExecuteNonQuery();
				}

				for (int count = 0; count <= pks.Length - 1; count++) {
					var _with8 = insCommand;
					_with8.Connection = objWK.MyConnection;
					_with8.CommandType = CommandType.StoredProcedure;
					_with8.CommandText = objWK.MyUserName + ".Pkg_Qcor_Mc_m_Message_Setup.QCOR_MC_M_CUSTOMER_INS ";
					_with8.Parameters.Clear();
					_with8.Parameters.Add("MESSAGE_SETUP_MST_FK_IN", MsgSetUpPk).Direction = ParameterDirection.Input;
					_with8.Parameters.Add("CUST_MST_FK_IN", pks.GetValue(count)).Direction = ParameterDirection.Input;
					_with8.Parameters.Add("PARTY_FLAG_IN", (string.IsNullOrEmpty(flags.GetValue(0).ToString()) ? 0 : Convert.ToInt16(flags.GetValue(0).ToString()))).Direction = ParameterDirection.Input;
					var _with9 = objWK.MyDataAdapter;
					_with9.InsertCommand = insCommand;
					_with9.InsertCommand.Transaction = TRAN;
					_with9.InsertCommand.ExecuteNonQuery();
				}

			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
            return 0;
		}


		public int fn_SaveCustomerNew(Int32 MsgSetUpPk, string customerpk, string Mode = "NEW", string PartyFlag = "")
		{
			try {
				Int32 i = default(Int32);
				WorkFlow objWK1 = new WorkFlow();
				//If Mode = "EDIT" Then
				//    objWK1.ExecuteCommands("delete from qcor_mc_m_message_setup_cust c  where c.message_setup_mst_fk= " & MsgSetUpPk)
				//End If

			
				string strsql = null;
				Array flags = ("1,1").Split(',');
				Array pks = customerpk.Split(',');
				WorkFlow objWK = new WorkFlow();
				OracleCommand insCommand = new OracleCommand();
				//objWK.MyConnection = TRAN.Connection
				objWK.OpenConnection();

				for (int count = 0; count <= pks.Length - 1; count++) {
					var _with10 = insCommand;
					_with10.Connection = objWK.MyConnection;
					_with10.CommandType = CommandType.StoredProcedure;
					_with10.CommandText = objWK.MyUserName + ".Pkg_Qcor_Mc_m_Message_Setup.QCOR_MC_M_CUSTOMER_INS ";
					_with10.Parameters.Clear();
					_with10.Parameters.Add("MESSAGE_SETUP_MST_FK_IN", MsgSetUpPk).Direction = ParameterDirection.Input;
                    _with10.Parameters.Add("CUST_MST_FK_IN", pks.GetValue(count)).Direction = ParameterDirection.Input;
                    _with10.Parameters.Add("PARTY_FLAG_IN", (string.IsNullOrEmpty(flags.GetValue(0).ToString()) ? 0 : Convert.ToInt16(flags.GetValue(0).ToString()))).Direction = ParameterDirection.Input;
                    var _with11 = objWK.MyDataAdapter;
					_with11.InsertCommand = insCommand;
					//.InsertCommand.Transaction = TRAN
					_with11.InsertCommand.ExecuteNonQuery();
				}


			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
            return 0;
		}
		public object fn_SaveDocGrid(OracleTransaction TRAN, Int32 MsgSetUpPk, DataSet dsGridData, DataSet DS_EmailDet, string EmailSetup_PKs, string Mode = "NEW")
		{

			Int32 i = default(Int32);
			//Dim objWK1 As New WorkFlow
			//If Mode = "EDIT" Then
			//    objWK1.ExecuteCommands("DELETE FROM QCOR_MC_M_MESSAGE_SETUP_DOCS MD WHERE MD.MESSAGE_SETUP_MST_FK = " & MsgSetUpPk)
			//End If

			WorkFlow objWK = new WorkFlow();
			objWK.MyConnection = TRAN.Connection;
			objWK.OpenConnection();
			OracleCommand insCommand = new OracleCommand();

			try {

				for (i = 0; i <= dsGridData.Tables[0].Rows.Count - 1; i++) {
					var _with12 = insCommand;
					_with12.Connection = objWK.MyConnection;
					_with12.CommandType = CommandType.StoredProcedure;
					_with12.CommandText = objWK.MyUserName + ".Pkg_Qcor_Mc_m_Message_Setup.QCOR_MC_M_SETUP_DOCS_INS";
					_with12.Parameters.Clear();

					_with12.Parameters.Add("MESSAGE_SETUP_MST_FK_IN", MsgSetUpPk).Direction = ParameterDirection.Input;
					_with12.Parameters.Add("DOCUMENT_MST_FK_IN", dsGridData.Tables[0].Rows[i]["TRANDTLPK"]).Direction = ParameterDirection.Input;

					if (!string.IsNullOrEmpty(dsGridData.Tables[0].Rows[i]["EMAIL"].ToString())) {
						_with12.Parameters.Add("EMAIL_FLAG_IN", dsGridData.Tables[0].Rows[i]["EMAIL"]).Direction = ParameterDirection.Input;
					} else {
						_with12.Parameters.Add("EMAIL_FLAG_IN", "").Direction = ParameterDirection.Input;
					}

					if (!string.IsNullOrEmpty(dsGridData.Tables[0].Rows[i]["SMS"].ToString())) {
						_with12.Parameters.Add("SMS_FLAG_IN", dsGridData.Tables[0].Rows[i]["SMS"]).Direction = ParameterDirection.Input;
					} else {
						_with12.Parameters.Add("SMS_FLAG_IN", "").Direction = ParameterDirection.Input;
					}
					if (!string.IsNullOrEmpty(dsGridData.Tables[0].Rows[i]["FAX"].ToString())) {
						_with12.Parameters.Add("FAX_FLAG_IN", dsGridData.Tables[0].Rows[i]["FAX"]).Direction = ParameterDirection.Input;
					} else {
						_with12.Parameters.Add("FAX_FLAG_IN", "").Direction = ParameterDirection.Input;
					}
					if (!string.IsNullOrEmpty(dsGridData.Tables[0].Rows[i]["PRINT"].ToString())) {
						_with12.Parameters.Add("PRINT_FLAG_IN", dsGridData.Tables[0].Rows[i]["PRINT"]).Direction = ParameterDirection.Input;
					} else {
						_with12.Parameters.Add("PRINT_FLAG_IN", "").Direction = ParameterDirection.Input;
					}

					///'getting data from Message PopUp
					if (!string.IsNullOrEmpty(dsGridData.Tables[0].Rows[i]["EMAIL_SUB"].ToString())) {
						_with12.Parameters.Add("DOC_EMAIL_SUBJECT_IN", dsGridData.Tables[0].Rows[i]["EMAIL_SUB"]).Direction = ParameterDirection.Input;
					} else {
						_with12.Parameters.Add("DOC_EMAIL_SUBJECT_IN", "").Direction = ParameterDirection.Input;
					}
					if (!string.IsNullOrEmpty(dsGridData.Tables[0].Rows[i]["EMAIL_MSG"].ToString())) {
						_with12.Parameters.Add("DOC_EMAIL_MSG_IN", dsGridData.Tables[0].Rows[i]["EMAIL_MSG"]).Direction = ParameterDirection.Input;
					} else {
						_with12.Parameters.Add("DOC_EMAIL_MSG_IN", "").Direction = ParameterDirection.Input;
					}
					if (!string.IsNullOrEmpty(dsGridData.Tables[0].Rows[i]["ATTACH_FLAG"].ToString())) {
						_with12.Parameters.Add("DOC_ATTACH_IN", dsGridData.Tables[0].Rows[i]["ATTACH_FLAG"]).Direction = ParameterDirection.Input;
					} else {
						_with12.Parameters.Add("DOC_ATTACH_IN", "").Direction = ParameterDirection.Input;
					}
					if (!string.IsNullOrEmpty(dsGridData.Tables[0].Rows[i]["CONSIGNEE_FLAG"].ToString())) {
						_with12.Parameters.Add("CONSIGNEE_FLAG_IN", dsGridData.Tables[0].Rows[i]["CONSIGNEE_FLAG"]).Direction = ParameterDirection.Input;
					} else {
						_with12.Parameters.Add("CONSIGNEE_FLAG_IN", "").Direction = ParameterDirection.Input;
					}
					if (!string.IsNullOrEmpty(dsGridData.Tables[0].Rows[i]["DP_AGENT_FLAG"].ToString())) {
						_with12.Parameters.Add("DP_AGENT_FLAG_IN", dsGridData.Tables[0].Rows[i]["DP_AGENT_FLAG"]).Direction = ParameterDirection.Input;
					} else {
						_with12.Parameters.Add("DP_AGENT_FLAG_IN", "").Direction = ParameterDirection.Input;
					}
					if (!string.IsNullOrEmpty(dsGridData.Tables[0].Rows[i]["CARGO_MANIFEST_FLAG"].ToString())) {
						_with12.Parameters.Add("CARGO_MANIFEST_FLAG_IN", dsGridData.Tables[0].Rows[i]["CARGO_MANIFEST_FLAG"]).Direction = ParameterDirection.Input;
					} else {
						_with12.Parameters.Add("CARGO_MANIFEST_FLAG_IN", "").Direction = ParameterDirection.Input;
					}
					if (!string.IsNullOrEmpty(dsGridData.Tables[0].Rows[i]["HBL_ATTACH_FLAG"].ToString())) {
						_with12.Parameters.Add("HBL_ATTACH_FLAG_IN", dsGridData.Tables[0].Rows[i]["HBL_ATTACH_FLAG"]).Direction = ParameterDirection.Input;
					} else {
						_with12.Parameters.Add("HBL_ATTACH_FLAG_IN", "").Direction = ParameterDirection.Input;
					}
					if (!string.IsNullOrEmpty(dsGridData.Tables[0].Rows[i]["FREIGHT_DETAILS_FLAG"].ToString())) {
						_with12.Parameters.Add("FREIGHT_DETAILS_FLAG_IN", dsGridData.Tables[0].Rows[i]["FREIGHT_DETAILS_FLAG"]).Direction = ParameterDirection.Input;
					} else {
						_with12.Parameters.Add("FREIGHT_DETAILS_FLAG_IN", "").Direction = ParameterDirection.Input;
					}
					if (!string.IsNullOrEmpty(dsGridData.Tables[0].Rows[i]["NOTIFY_PARTY_FLAG"].ToString())) {
						_with12.Parameters.Add("NOTIFY_PARTY_FLAG_IN", dsGridData.Tables[0].Rows[i]["NOTIFY_PARTY_FLAG"]).Direction = ParameterDirection.Input;
					} else {
						_with12.Parameters.Add("NOTIFY_PARTY_FLAG_IN", "").Direction = ParameterDirection.Input;
					}
					if (!string.IsNullOrEmpty(dsGridData.Tables[0].Rows[i]["SMS_MSG"].ToString())) {
						_with12.Parameters.Add("DOC_SMS_MSG_IN", dsGridData.Tables[0].Rows[i]["SMS_MSG"]).Direction = ParameterDirection.Input;
					} else {
						_with12.Parameters.Add("DOC_SMS_MSG_IN", "").Direction = ParameterDirection.Input;
					}

					if (!string.IsNullOrEmpty(dsGridData.Tables[0].Rows[i]["NOTIFY_ACTIVE"].ToString())) {
						_with12.Parameters.Add("NOTIFY_ACTIVE_IN", dsGridData.Tables[0].Rows[i]["NOTIFY_ACTIVE"]).Direction = ParameterDirection.Input;
					} else {
						_with12.Parameters.Add("NOTIFY_ACTIVE_IN", "").Direction = ParameterDirection.Input;
					}

					if (!string.IsNullOrEmpty(dsGridData.Tables[0].Rows[i]["NOTIFY_BEFORE"].ToString())) {
						_with12.Parameters.Add("NOTIFY_BEFORE_IN", dsGridData.Tables[0].Rows[i]["NOTIFY_BEFORE"]).Direction = ParameterDirection.Input;
					} else {
						_with12.Parameters.Add("NOTIFY_BEFORE_IN", "").Direction = ParameterDirection.Input;
					}

					if (!string.IsNullOrEmpty(dsGridData.Tables[0].Rows[i]["NOTIFY_FREQUENCY"].ToString())) {
						_with12.Parameters.Add("NOTIFY_FREQUENCY_IN", dsGridData.Tables[0].Rows[i]["NOTIFY_FREQUENCY"]).Direction = ParameterDirection.Input;
					} else {
						_with12.Parameters.Add("NOTIFY_FREQUENCY_IN", "").Direction = ParameterDirection.Input;
					}
					if (Mode == "EDIT") {
						_with12.Parameters.Add("LASTMODIFIED_PK_IN", dsGridData.Tables[0].Rows[i]["LASTMODIFIED_PK"]).Direction = ParameterDirection.Input;
						_with12.Parameters.Add("LASTUPD_IN", dsGridData.Tables[0].Rows[i]["LASTUPD"]).Direction = ParameterDirection.Input;
						_with12.Parameters.Add("CREATED_BY_FK_IN", CREATED_BY).Direction = ParameterDirection.Input;
						_with12.Parameters.Add("CREATED_DATE_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with12.Parameters.Add("CREATED_BY_FK_IN", CREATED_BY).Direction = ParameterDirection.Input;
						_with12.Parameters.Add("CREATED_DATE_IN", System.DateTime.Now).Direction = ParameterDirection.Input;
						_with12.Parameters.Add("LASTMODIFIED_PK_IN", dsGridData.Tables[0].Rows[i]["LASTMODIFIED_PK"]).Direction = ParameterDirection.Input;
						_with12.Parameters.Add("LASTUPD_IN", "").Direction = ParameterDirection.Input;
					}
					var _with13 = objWK.MyDataAdapter;
					_with13.InsertCommand = insCommand;
					_with13.InsertCommand.Transaction = TRAN;
					_with13.InsertCommand.ExecuteNonQuery();
					fn_SaveEmailDetails(TRAN, MsgSetUpPk, DS_EmailDet, Convert.ToInt64(dsGridData.Tables[0].Rows[i]["TRANDTLPK"].ToString()), EmailSetup_PKs);
				}
				return 1;
			//Manjunath  PTS ID:Sep-02   12/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}

		}
		public void fn_SaveEmailDetails(OracleTransaction TRAN, Int32 MsgSetUpPk, DataSet dsGridData, Int64 docPk, string EmailSetupPKs)
		{
			try {
				DataView dv = null;
				Int16 i = default(Int16);
				Int16 j = default(Int16);
				if ((dsGridData != null)) {
					for (j = 0; j <= dsGridData.Tables.Count - 1; j++) {
						dsGridData.Tables[j].DefaultView.RowFilter = "DOC_MST_FK='" + docPk + "'";
						dv = dsGridData.Tables[j].DefaultView;
						if (dv.Count != 0) {
							break; // TODO: might not be correct. Was : Exit For
						}
					}


					WorkFlow objWK = new WorkFlow();
					objWK.MyConnection = TRAN.Connection;
					objWK.OpenConnection();
					OracleCommand insCommand = new OracleCommand();


					for (i = 0; i <= dv.Count - 1; i++) {
						var _with14 = insCommand;
						_with14.Connection = objWK.MyConnection;
						_with14.CommandType = CommandType.StoredProcedure;
						_with14.CommandText = objWK.MyUserName + ".Pkg_Qcor_Mc_m_Message_Setup.QCOR_MC_DOCS_EMAIL_SET_INS";
						_with14.Parameters.Clear();
						_with14.Parameters.Add("QCOR_MC_M_MSG_EMAIL_PK_IN", dv[i]["QCOR_MC_M_MSG_SETUP_EMAIL_PK"]).Direction = ParameterDirection.Input;
						_with14.Parameters.Add("MESSAGE_SETUP_MST_FK_IN", MsgSetUpPk).Direction = ParameterDirection.Input;
						_with14.Parameters.Add("DOC_MST_FK_IN", dv[i]["DOC_MST_FK"]).Direction = ParameterDirection.Input;
						_with14.Parameters.Add("CUST_CTG_FLAG_IN", "1").Direction = ParameterDirection.Input;
						_with14.Parameters.Add("CUST_MST_FK_IN", dv[i]["PARTY_MST_PK"]).Direction = ParameterDirection.Input;
						_with14.Parameters.Add("CONTACT_PERSON_IN", dv[i]["CONTACT_PERSON"]).Direction = ParameterDirection.Input;
						_with14.Parameters.Add("EMAIL_ID_IN", dv[i]["EMAIL_ID"]).Direction = ParameterDirection.Input;
						_with14.Parameters.Add("MOBILE_NO_IN", dv[i]["MOBILE_NO"]).Direction = ParameterDirection.Input;
						_with14.Parameters.Add("FAX_NO_IN", dv[i]["FAX_NO"]).Direction = ParameterDirection.Input;
						_with14.Parameters.Add("CUST_CNT_FK_IN", (string.IsNullOrEmpty(dv[i]["PARTY_TYPE"].ToString()) ? 0 : dv[i]["PARTY_TYPE"])).Direction = ParameterDirection.Input;
						var _with15 = objWK.MyDataAdapter;
						_with15.InsertCommand = insCommand;
						_with15.InsertCommand.Transaction = TRAN;
						_with15.InsertCommand.ExecuteNonQuery();
					}
					if (EmailSetupPKs != "0") {
						fn_DelEmailDetails(TRAN, EmailSetupPKs);
					}
				}
			//Manjunath  PTS ID:Sep-02   12/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion
		#region "Get Protocol Name"
		public string GetStdProtocol(string MsgSeupPK = "")
		{
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			sb.Append("SELECT QMSG.PROTOCOL_NAME");
			sb.Append("  FROM QCOR_MC_M_MESSAGE_SETUP QMSG");
			if (!string.IsNullOrEmpty(MsgSeupPK)) {
				sb.Append("  WHERE QMSG.MESSAGE_SETUP_MST_PK =" + MsgSeupPK);
			}
			try {
				return objWF.ExecuteScaler(sb.ToString());
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		public object GetProtocol(string Cust_Pk = "")
		{
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			sb.Append("select S.PROTOCOL_NAME,M.MESSAGE_SETUP_MST_FK, ");
			sb.Append(" M.CUST_MST_FK, ");
			sb.Append(" C.CUSTOMER_ID, ");
			sb.Append(" C.CUSTOMER_NAME ");
			sb.Append(" From QCOR_MC_M_MESSAGE_SETUP_CUST M, ");
			sb.Append(" QCOR_MC_M_MESSAGE_SETUP S, ");
			sb.Append(" CUSTOMER_MST_TBL  C ");
			sb.Append(" WHERE S.MESSAGE_SETUP_MST_PK = M.MESSAGE_SETUP_MST_FK ");
			sb.Append(" AND C.CUSTOMER_MST_PK = M.CUST_MST_FK ");
			sb.Append(" AND M.CUST_TYPE in (0, 1) ");
			if (!string.IsNullOrEmpty(Cust_Pk)) {
				sb.Append(" AND C.CUSTOMER_MST_PK IN ( " + Cust_Pk + ")");
			}
			return objWF.GetDataSet(sb.ToString());
			try {
				return objWF.ExecuteScaler(sb.ToString());
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		//Public Function fn_TextCustomer()
		//    Try
		//        Dim objWF As New WorkFlow
		//        Dim sb As New System.Text.StringBuilder(5000)
		//        sb.Append("SELECT CUST.CUSTOMER_NAME")
		//        sb.Append(" FROM CUSTOMER_MST_TBL CUST, ")
		//        sb.Append(" QCOR_MC_M_MESSAGE_SETUP_CUST QCUST ")
		//        sb.Append(" WHERE QCUST.CUST_MST_FK = CUST.CUSTOMER_MST_PK ")
		//        sb.Append(" AND AND QCUST.MESSAGE_SETUP_MST_FK = 421 ")
		//        Return objWF.GetDataSet(sb.ToString)
		//    Catch ex As Exception
		//        Throw ex
		//    End Try
		//End Function
		public DataSet GetCustomer(int Cust_Pk, int StdPro)
		{


			try {
				WorkFlow objWF = new WorkFlow();
				System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
				sb.Append("select DISTINCT S.PROTOCOL_NAME, ");
				sb.Append(" M.CUST_MST_FK, ");
				sb.Append(" C.CUSTOMER_ID, ");
				sb.Append(" C.CUSTOMER_NAME ");
				sb.Append(" From QCOR_MC_M_MESSAGE_SETUP_CUST M, ");
				sb.Append(" QCOR_MC_M_MESSAGE_SETUP S, ");
				sb.Append(" CUSTOMER_MST_TBL  C ");
				sb.Append(" Where M.MESSAGE_SETUP_MST_FK =" + StdPro);
				sb.Append(" AND S.MESSAGE_SETUP_MST_PK(+) = M.MESSAGE_SETUP_MST_FK ");
				sb.Append(" AND C.CUSTOMER_MST_PK = M.CUST_MST_FK ");
				sb.Append(" AND M.CUST_TYPE = 1 ");
				sb.Append(" AND C.CUSTOMER_MST_PK = " + Cust_Pk);
				return objWF.GetDataSet(sb.ToString());
			} catch (Exception ex) {
				throw ex;
			}
		}
		public int GetStdPro()
		{
			try {
				WorkFlow objWF = new WorkFlow();
				long PKValue = 0;
				return Convert.ToInt32(objWF.ExecuteScaler("select p.message_setup_mst_pk from qcor_mc_m_message_setup p where p.protocol_name='STDPROTOCOL'"));

			} catch (Exception ex) {
				throw ex;
			}
		}
		#region "Fetch for messeage details"

		public DataTable fn_fetchMsgDetails(string configid = "", long Transaction_pk = 0, long Party_fk = 0, long Doc_pk = 0, Hashtable AdnlQry = null)
		{
			try {
				System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
				//'sb.Append("select MSD.EMAIL_FLAG, ")
				//'sb.Append("       MSD.SMS_FLAG,")
				//'sb.Append("       MSD.FAX_FLAG,")
				//'sb.Append("       MSD.PRINT_FLAG,")
				//'sb.Append("       MSD.DOC_EMAIL_SUBJECT,")
				//'sb.Append("       MSD.DOC_EMAIL_MSG,")
				//'sb.Append("       MSD.DOC_SMS_MSG,")
				//'sb.Append("       MSD.DOC_ATTACH,")
				//'sb.Append("       PARTY_VIEW.email_id,")
				//'sb.Append("       PARTY_VIEW.PHONE_NO,")
				//'sb.Append("       PARTY_VIEW.FAX_NO")
				//'sb.Append("  from qcor_mc_m_message_setup_docs msd,")
				//'sb.Append("       qcor_mc_m_message_setup_cust msc,")
				//'sb.Append("       VIEW_DOC_MESSAGING_PARTY       party_view")
				//'sb.Append(" where msc.message_setup_mst_fk = msd.message_setup_mst_fk")
				//'sb.Append("   and msc.cust_mst_fk = " & Party_fk)
				//'sb.Append("   and msc.cust_mst_fk = party_view.party_mst_pk")
				sb.Append(" SELECT DISTINCT MSD.EMAIL_FLAG,");
				sb.Append("        MSD.SMS_FLAG,");
				sb.Append("        MSD.FAX_FLAG,");
				sb.Append("        MSD.PRINT_FLAG,");
				sb.Append("        MSD.DOC_EMAIL_SUBJECT,");
				sb.Append("        MSD.DOC_EMAIL_MSG,");
				sb.Append("        MSD.DOC_SMS_MSG,");
				sb.Append("        MSD.DOC_ATTACH,");
				sb.Append("        ME.EMAIL_ID,");
				sb.Append("        ME.MOBILE_NO PHONE_NO,");
				sb.Append("        PARTY_VIEW.FAX_NO, nvl(MSD.CONSIGNEE_FLAG,0) CONSIGNEE_FLAG,  nvl(MSD.DP_AGENT_FLAG,0) DP_AGENT_FLAG,  nvl(MSD.NOTIFY_PARTY_FLAG,0) NOTIFY_PARTY_FLAG, ");
				sb.Append("        nvl(MSD.FREIGHT_DETAILS_FLAG,0) FREIGHT_DETAILS_FLAG, nvl(MSD.CARGO_MANIFEST_FLAG,0) CARGO_MANIFEST_FLAG, nvl(MSD.HBL_ATTACH_FLAG,0) HBL_ATTACH_FLAG ");
				sb.Append("   FROM QCOR_MC_M_MESSAGE_SETUP_DOCS   MSD,");
				sb.Append("        QCOR_MC_M_MESSAGE_SETUP_CUST   MSC,");
				sb.Append("        QCOR_MC_M_MESSAGE_SETUP_EMAILS ME,");
				sb.Append("        VIEW_DOC_MESSAGING_PARTY       PARTY_VIEW");
				sb.Append("  WHERE MSC.MESSAGE_SETUP_MST_FK = MSD.MESSAGE_SETUP_MST_FK");
				sb.Append("    AND MSC.MESSAGE_SETUP_MST_FK = ME.MESSAGE_SETUP_MST_FK");
				sb.Append("    AND MSD.DOCUMENT_MST_FK = ME.DOC_MST_FK");
				sb.Append("    AND PARTY_VIEW.PARTY_MST_PK = ME.CUST_MST_FK");
				sb.Append("    AND MSC.CUST_MST_FK = ME.CUST_MST_FK");
				sb.Append("    AND MSC.CUST_MST_FK = " + Party_fk);
				if ((AdnlQry != null)) {
					IDictionaryEnumerator AdnlQryEnum = AdnlQry.GetEnumerator();
					while (AdnlQryEnum.MoveNext()) {
						sb.Append(Convert.ToChar(" and " + AdnlQryEnum.Key + " = "), Convert.ToInt32(AdnlQryEnum.Value.ToString()));
					}
				}
				sb.Append("   and msd.document_mst_fk = " + Doc_pk);
				WorkFlow objWF = new WorkFlow();
				return objWF.GetDataTable(sb.ToString());
			//Manjunath  PTS ID:Sep-02   12/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}


		public DataTable fn_getEmailDetails(string customerPks, long TRANDTLPK, string PartyFlag, long docPk, int Party_Flag = 1)
		{

			try {
				System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
				WorkFlow objWF = new WorkFlow();
				string strSql = "";
				DataSet ds = new DataSet();
				bool blnDataExists = false;

				strSql = " SELECT * FROM QCOR_MC_M_MESSAGE_SETUP_EMAILS E WHERE E.CUST_MST_FK IN (" + customerPks + ") AND E.DOC_MST_FK=" + docPk + "   ";
				ds = objWF.GetDataSet(strSql);
				if (ds.Tables[0].Rows.Count > 0) {
					blnDataExists = true;
				}
				if (Party_Flag == 1) {
					sb.Append("SELECT ROWNUM SLNR, QRY.* ");
					sb.Append("  FROM ( ");
					sb.Append("        /*CCD */ ");
					sb.Append("        SELECT DISTINCT CMT.CUSTOMER_MST_PK PARTY_MST_PK, ");
					sb.Append("                         E.DOC_MST_FK DOC_MST_FK, ");
					sb.Append("                         '' PARTY_ID, ");
					sb.Append("                         CMT.CUSTOMER_NAME PARTY_NAME, ");
					sb.Append("                         CCD.ADM_CONTACT_PERSON CONTACT_PERSON, ");
					sb.Append("                         CCD.ADM_EMAIL_ID EMAIL_ID, ");
					sb.Append("                         CCD.ADM_PHONE_NO_1 MOBILE_NO, ");
					sb.Append("                         CCD.ADM_FAX_NO FAX_NO, ");
					sb.Append("                         1 PARTY_TYPE, ");
					sb.Append("                         '0' SELFLAG, ");
					sb.Append("                         QCOR_MC_M_MSG_SETUP_EMAIL_PK, ");
					sb.Append("                         '1' DISABLE_FLAG ");
					sb.Append("          FROM CUSTOMER_MST_TBL               CMT, ");
					sb.Append("                CUSTOMER_CONTACT_DTLS          CCD, ");
					sb.Append("                QCOR_MC_M_MESSAGE_SETUP_EMAILS E ");
					sb.Append("         WHERE CCD.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK ");

					sb.Append("           AND CMT.CUSTOMER_MST_PK IN (" + customerPks + ") ");
					sb.Append("           AND E.CUST_CNT_FK(+) = 0 ");

					if (blnDataExists) {
						sb.Append("           AND E.CUST_MST_FK = CMT.CUSTOMER_MST_PK ");
						sb.Append("            AND E.CUST_CTG_FLAG<>2 ");
					} else {
						sb.Append("           AND E.CUST_MST_FK(+) = CMT.CUSTOMER_MST_PK ");
					}
					sb.Append("        UNION ");
					sb.Append("         ");
					sb.Append("        /* CCT */ ");
					sb.Append("        SELECT CMT.CUSTOMER_MST_PK PARTY_MST_PK, ");
					sb.Append("               E.DOC_MST_FK DOC_MST_FK, ");
					sb.Append("               '' PARTY_ID, ");
					sb.Append("               CMT.CUSTOMER_NAME PARTY_NAME, ");
					sb.Append("               CASE ");
					sb.Append("                 WHEN ((SELECT COUNT(EM.CUST_CNT_FK) ");
					sb.Append("                          FROM QCOR_MC_M_MESSAGE_SETUP_EMAILS EM ");
					sb.Append("                         WHERE EM.CUST_CNT_FK = CCT.CUST_CONTACT_PK ");
					sb.Append("                           AND EM.CUST_MST_FK IN (" + customerPks + ")) > 0) THEN ");
					sb.Append("                  CCT.NAME ");
					sb.Append("                 ELSE ");
					sb.Append("                  CCT.NAME ");
					sb.Append("               END CUST_CNT_FK, ");
					sb.Append("               CASE ");
					sb.Append("                 WHEN E.EMAIL_ID IS NOT NULL THEN ");
					sb.Append("                  E.EMAIL_ID ");
					sb.Append("                 ELSE ");
					sb.Append("                  CCT.EMAIL ");
					sb.Append("               END EMAIL_ID, ");
					sb.Append("               CASE ");
					sb.Append("                 WHEN E.MOBILE_NO IS NOT NULL THEN ");
					sb.Append("                  E.MOBILE_NO ");
					sb.Append("                 ELSE ");
					sb.Append("                  CCT.MOBILE ");
					sb.Append("               END MOBILE_NO, ");
					sb.Append("               CASE ");
					sb.Append("                 WHEN CCT.FAX IS NOT NULL THEN ");
					sb.Append("                  CCT.FAX ");
					sb.Append("                 ELSE ");
					sb.Append("                  E.FAX_NO ");
					sb.Append("               END FAX, ");
					sb.Append("               CCT.CUST_CONTACT_PK PARTY_TYPE, ");
					sb.Append("               '0' SELFLAG, ");
					sb.Append("               QCOR_MC_M_MSG_SETUP_EMAIL_PK, ");
					sb.Append("               '1' DISABLE_FLAG ");
					sb.Append("          FROM CUSTOMER_MST_TBL               CMT, ");
					sb.Append("               CUSTOMER_CONTACT_TRN           CCT, ");
					sb.Append("               QCOR_MC_M_MESSAGE_SETUP_EMAILS E ");
					sb.Append("         WHERE CMT.CUSTOMER_MST_PK IN (" + customerPks + ") ");
					sb.Append("           AND CCT.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK ");

					sb.Append("           AND E.CUST_CNT_FK(+) <> 0 ");
					if (blnDataExists) {
						sb.Append("   AND E.CUST_CNT_FK(+) = CCT.CUST_CONTACT_PK");
						sb.Append("            AND E.CUST_CTG_FLAG<>2 ");
						sb.Append("           AND E.CUST_MST_FK = CCT.CUSTOMER_MST_FK ");
					} else {
						sb.Append("           AND E.CUST_MST_FK(+) = CCT.CUSTOMER_MST_FK ");
					}
					//sb.Append("           AND E.CUST_CNT_FK(+) = CCT.CUST_CONTACT_PK ")

					sb.Append("        UNION ");
					sb.Append("        /*SETUP EMAIL */");
					sb.Append("        SELECT CMT.CUSTOMER_MST_PK PARTY_MST_PK, ");
					sb.Append("               E.DOC_MST_FK DOC_MST_FK, ");
					sb.Append("               '' PARTY_ID, ");
					sb.Append("                CMT.CUSTOMER_NAME  PARTY_NAME, ");
					sb.Append("               E.CONTACT_PERSON CONTACT_PERSON, ");
					sb.Append("               E.EMAIL_ID, ");
					sb.Append("               E.MOBILE_NO MOBILE_NO, ");
					sb.Append("               E.FAX_NO, ");
					sb.Append("               1 PARTY_TYPE, ");
					sb.Append("               '0' SELFLAG, ");
					sb.Append("               QCOR_MC_M_MSG_SETUP_EMAIL_PK, ");
					sb.Append("               '0' DISABLE_FLAG ");
					sb.Append("          FROM QCOR_MC_M_MESSAGE_SETUP_EMAILS E,CUSTOMER_MST_TBL CMT ");
					sb.Append("         WHERE CMT.CUSTOMER_MST_PK IN (" + customerPks + ") ");
					sb.Append("         AND E.CUST_MST_FK=CMT.CUSTOMER_MST_PK ");
					sb.Append("           ) QRY");
					//sb.Append("           AND E.CUST_CTG_FLAG = 2) QRY")

					if (docPk > 0 & blnDataExists) {
						sb.Append(" WHERE DOC_MST_FK = " + docPk);
					}
					sb.Append(" ORDER BY PARTY_NAME ,CONTACT_PERSON ");
				} else if (Party_Flag == 2) {
					sb.Append("SELECT ROWNUM SLNR, QRY.*");
					sb.Append("  FROM ( ");
					sb.Append("        SELECT DISTINCT AMT.AGENT_MST_PK PARTY_MST_PK,");
					sb.Append("                         E.DOC_MST_FK DOC_MST_FK,");
					sb.Append("                         '' PARTY_ID,");
					sb.Append("                         AMT.AGENT_NAME PARTY_NAME,");
					sb.Append("                         ACD.ADM_CONTACT_PERSON CONTACT_PERSON,");
					sb.Append("                         ACD.ADM_EMAIL_ID EMAIL_ID,");
					sb.Append("                         ACD.ADM_PHONE_NO_1 MOBILE_NO,");
					sb.Append("                         ACD.ADM_FAX_NO FAX_NO,");
					sb.Append("                         2 PARTY_TYPE,");
					sb.Append("                         '0' SELFLAG,");
					sb.Append("                         QCOR_MC_M_MSG_SETUP_EMAIL_PK,");
					sb.Append("                         '1' DISABLE_FLAG");
					sb.Append("          FROM AGENT_MST_TBL                  AMT,");
					sb.Append("                AGENT_CONTACT_DTLS             ACD,");
					sb.Append("                QCOR_MC_M_MESSAGE_SETUP_EMAILS E");
					sb.Append("         WHERE AMT.AGENT_MST_PK = ACD.AGENT_MST_FK");
					sb.Append("           AND AMT.AGENT_MST_PK IN (" + customerPks + ")");
					sb.Append("           AND E.CUST_CNT_FK(+) = 0");
					if (blnDataExists) {
						sb.Append("           AND E.CUST_MST_FK = AMT.AGENT_MST_PK ");
						sb.Append("            AND E.CUST_CTG_FLAG<>2 ");
					} else {
						sb.Append("           AND E.CUST_MST_FK(+) = AMT.AGENT_MST_PK");
					}
					sb.Append("        UNION /* CCT */");
					sb.Append("        SELECT AMT.AGENT_MST_PK PARTY_MST_PK,");
					sb.Append("               E.DOC_MST_FK DOC_MST_FK,");
					sb.Append("               '' PARTY_ID,");
					sb.Append("               AMT.AGENT_NAME PARTY_NAME,");
					sb.Append("               ACD.ADM_CONTACT_PERSON CUST_CNT_FK,");
					sb.Append("               CASE");
					sb.Append("                 WHEN E.EMAIL_ID IS NOT NULL THEN");
					sb.Append("                  E.EMAIL_ID");
					sb.Append("                 ELSE");
					sb.Append("                  ACD.ADM_EMAIL_ID");
					sb.Append("               END EMAIL_ID,");
					sb.Append("               CASE");
					sb.Append("                 WHEN E.MOBILE_NO IS NOT NULL THEN");
					sb.Append("                  E.MOBILE_NO");
					sb.Append("                 ELSE");
					sb.Append("                  ACD.ADM_PHONE_NO_1");
					sb.Append("               END MOBILE_NO,");
					sb.Append("               CASE");
					sb.Append("                 WHEN ACD.ADM_FAX_NO IS NOT NULL THEN");
					sb.Append("                  ACD.ADM_FAX_NO");
					sb.Append("                 ELSE");
					sb.Append("                  E.FAX_NO");
					sb.Append("               END FAX,");
					sb.Append("               2 PARTY_TYPE,");
					sb.Append("               '0' SELFLAG,");
					sb.Append("               QCOR_MC_M_MSG_SETUP_EMAIL_PK,");
					sb.Append("               '1' DISABLE_FLAG");
					sb.Append("          FROM AGENT_MST_TBL                  AMT,");
					sb.Append("               AGENT_CONTACT_DTLS             ACD,");
					sb.Append("               QCOR_MC_M_MESSAGE_SETUP_EMAILS E");
					sb.Append("         WHERE AMT.AGENT_MST_PK IN (" + customerPks + ")");
					sb.Append("           AND AMT.AGENT_MST_PK = ACD.AGENT_MST_FK");
					sb.Append("           AND E.CUST_CNT_FK(+) <> 0");
					if (blnDataExists) {
						sb.Append("           AND E.CUST_MST_FK = AMT.AGENT_MST_PK ");
						sb.Append("            AND E.CUST_CTG_FLAG<>2 ");
					} else {
						sb.Append("           AND E.CUST_MST_FK(+) = ACD.AGENT_MST_FK");
					}

					sb.Append("        UNION /*SETUP EMAIL */");
					sb.Append("        SELECT AMT.AGENT_MST_PK PARTY_MST_PK,");
					sb.Append("               E.DOC_MST_FK DOC_MST_FK,");
					sb.Append("               '' PARTY_ID,");
					sb.Append("               AMT.AGENT_NAME PARTY_NAME,");
					sb.Append("               E.CONTACT_PERSON CONTACT_PERSON,");
					sb.Append("               E.EMAIL_ID,");
					sb.Append("               E.MOBILE_NO MOBILE_NO,");
					sb.Append("               E.FAX_NO,");
					sb.Append("               2 PARTY_TYPE,");
					sb.Append("               '0' SELFLAG,");
					sb.Append("               QCOR_MC_M_MSG_SETUP_EMAIL_PK,");
					sb.Append("               '0' DISABLE_FLAG");
					sb.Append("          FROM QCOR_MC_M_MESSAGE_SETUP_EMAILS E, AGENT_MST_TBL AMT");
					sb.Append("         WHERE AMT.AGENT_MST_PK IN (" + customerPks + ")");
					sb.Append("           AND E.CUST_MST_FK = AMT.AGENT_MST_PK");
					sb.Append("           ) QRY");
					//sb.Append("           AND E.CUST_CTG_FLAG = 2) QRY")
					if (docPk > 0 & blnDataExists) {
						sb.Append(" WHERE DOC_MST_FK = " + docPk);
					}
					sb.Append(" ORDER BY PARTY_NAME, CONTACT_PERSON");
				} else {
					sb.Append("SELECT ROWNUM SLNR, QRY.*");
					sb.Append("  FROM ( /*CCD */");
					sb.Append("        SELECT DISTINCT VMT.VENDOR_MST_PK PARTY_MST_PK,");
					sb.Append("                         E.DOC_MST_FK DOC_MST_FK,");
					sb.Append("                         '' PARTY_ID,");
					sb.Append("                         VMT.VENDOR_NAME PARTY_NAME,");
					sb.Append("                         VCD.ADM_CONTACT_PERSON CONTACT_PERSON,");
					sb.Append("                         VCD.ADM_EMAIL_ID EMAIL_ID,");
					sb.Append("                         VCD.ADM_MOBILE MOBILE_NO,");
					sb.Append("                         VCD.ADM_FAX_NO FAX_NO,");
					sb.Append("                         3 PARTY_TYPE,");
					sb.Append("                         '0' SELFLAG,");
					sb.Append("                         QCOR_MC_M_MSG_SETUP_EMAIL_PK,");
					sb.Append("                         '1' DISABLE_FLAG");
					sb.Append("          FROM VENDOR_MST_TBL                 VMT,");
					sb.Append("                VENDOR_CONTACT_DTLS            VCD,");
					sb.Append("                QCOR_MC_M_MESSAGE_SETUP_EMAILS E");
					sb.Append("         WHERE VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
					sb.Append("           AND VMT.VENDOR_MST_PK IN (" + customerPks + ")");
					sb.Append("           AND E.CUST_CNT_FK(+) = 0");
					if (blnDataExists) {
						sb.Append("           AND E.CUST_MST_FK = VMT.VENDOR_MST_PK ");
						sb.Append("            AND E.CUST_CTG_FLAG<>2 ");
					} else {
						sb.Append("           AND E.CUST_MST_FK(+) = VMT.VENDOR_MST_PK");
					}

					sb.Append("        UNION /* CCT */");
					sb.Append("        SELECT VMT.VENDOR_MST_PK PARTY_MST_PK,");
					sb.Append("               E.DOC_MST_FK DOC_MST_FK,");
					sb.Append("               '' PARTY_ID,");
					sb.Append("               VMT.VENDOR_NAME PARTY_NAME,");
					sb.Append("               VCD.ADM_CONTACT_PERSON CUST_CNT_FK,");
					sb.Append("               CASE");
					sb.Append("                 WHEN E.EMAIL_ID IS NOT NULL THEN");
					sb.Append("                  E.EMAIL_ID");
					sb.Append("                 ELSE");
					sb.Append("                  VCD.ADM_EMAIL_ID");
					sb.Append("               END EMAIL_ID,");
					sb.Append("               CASE");
					sb.Append("                 WHEN E.MOBILE_NO IS NOT NULL THEN");
					sb.Append("                  E.MOBILE_NO");
					sb.Append("                 ELSE");
					sb.Append("                  VCD.ADM_MOBILE");
					sb.Append("               END MOBILE_NO,");
					sb.Append("               CASE");
					sb.Append("                 WHEN VCD.ADM_FAX_NO IS NOT NULL THEN");
					sb.Append("                  VCD.ADM_FAX_NO");
					sb.Append("                 ELSE");
					sb.Append("                  E.FAX_NO");
					sb.Append("               END FAX,");
					sb.Append("               3 PARTY_TYPE,");
					sb.Append("               '0' SELFLAG,");
					sb.Append("               QCOR_MC_M_MSG_SETUP_EMAIL_PK,");
					sb.Append("               '1' DISABLE_FLAG");
					sb.Append("          FROM VENDOR_MST_TBL                 VMT,");
					sb.Append("               VENDOR_CONTACT_DTLS            VCD,");
					sb.Append("               QCOR_MC_M_MESSAGE_SETUP_EMAILS E");
					sb.Append("         WHERE VMT.VENDOR_MST_PK IN (" + customerPks + ")");
					sb.Append("           AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
					sb.Append("           AND E.CUST_CNT_FK(+) <> 0");
					if (blnDataExists) {
						sb.Append("           AND E.CUST_MST_FK = VMT.VENDOR_MST_PK ");
						sb.Append("            AND E.CUST_CTG_FLAG<>2 ");
					} else {
						sb.Append("           AND E.CUST_MST_FK(+) = VCD.VENDOR_MST_FK");
					}
					sb.Append("        UNION /*SETUP EMAIL */");
					sb.Append("        SELECT VMT.VENDOR_MST_PK PARTY_MST_PK,");
					sb.Append("               E.DOC_MST_FK DOC_MST_FK,");
					sb.Append("               '' PARTY_ID,");
					sb.Append("               VMT.VENDOR_NAME PARTY_NAME,");
					sb.Append("               E.CONTACT_PERSON CONTACT_PERSON,");
					sb.Append("               E.EMAIL_ID,");
					sb.Append("               E.MOBILE_NO MOBILE_NO,");
					sb.Append("               E.FAX_NO,");
					sb.Append("               3 PARTY_TYPE,");
					sb.Append("               '0' SELFLAG,");
					sb.Append("               QCOR_MC_M_MSG_SETUP_EMAIL_PK,");
					sb.Append("               '0' DISABLE_FLAG");
					sb.Append("          FROM QCOR_MC_M_MESSAGE_SETUP_EMAILS E, VENDOR_MST_TBL VMT");
					sb.Append("         WHERE VMT.VENDOR_MST_PK IN (" + customerPks + ")");
					sb.Append("           AND E.CUST_MST_FK = VMT.VENDOR_MST_PK");
					sb.Append("           ) QRY");
					//sb.Append("           AND E.CUST_CTG_FLAG = 2) QRY")
					if (docPk > 0 & blnDataExists) {
						sb.Append(" WHERE DOC_MST_FK = " + docPk);
					}
					sb.Append(" ORDER BY PARTY_NAME, CONTACT_PERSON");

				}

				//sb.Append("select rownum SlNr, qry.* from (select  cmt.customer_mst_pk PARTY_MST_PK, ")
				//sb.Append(" E.DOC_MST_FK DOC_MST_FK, ")
				//sb.Append(" '' PARTY_ID, ")
				//sb.Append(" cmt.customer_name PARTY_NAME, ")
				//sb.Append(" E.CONTACT_PERSON  CONTACT_PERSON, ")
				//sb.Append("  case when e.email_id is not null then ")
				//sb.Append(" e.email_id ")
				//sb.Append(" else ")
				//sb.Append(" ccd.adm_email_id  ")
				//sb.Append(" end email_id, ")
				//sb.Append(" case when e.mobile_no is not null then ")
				//sb.Append(" e.mobile_no ")
				//sb.Append(" else ")
				//sb.Append(" ccd.adm_phone_no_1  ")
				//sb.Append(" end mobile_no,")
				//sb.Append(" case when e.fax_no is not null then ")
				//sb.Append(" e.fax_no ")
				//sb.Append(" else ")
				//sb.Append(" ccd.adm_fax_no ")
				//sb.Append(" end fax_no, ")
				//sb.Append(" 0 PARTY_TYPE, '0' SELFLAG, QCOR_MC_M_MSG_SETUP_EMAIL_PK, '0' DISABLE_FLAG  ")
				//sb.Append(" from customer_mst_tbl cmt, ")
				//sb.Append(" customer_contact_dtls ccd, qcor_mc_m_message_setup_emails e ")
				//sb.Append(" where ccd.customer_mst_fk = cmt.customer_mst_pk ")
				//sb.Append(" and e.cust_mst_fk(+) = cmt.customer_mst_pk")
				//sb.Append(" and cmt.customer_mst_pk in(" & customerPks & ")")
				//sb.Append(" and e.cust_cnt_fk(+) = 0 ")
				//sb.Append(" union ")



				//sb.Append("SELECT  CMT.CUSTOMER_MST_PK PARTY_MST_PK,")
				//sb.Append("               E.DOC_MST_FK DOC_MST_FK,")
				//sb.Append("               '' PARTY_ID,")
				//sb.Append("               CMT.CUSTOMER_NAME PARTY_NAME,")
				//sb.Append("               CASE WHEN ((SELECT COUNT(EM.CUST_CNT_FK) FROM QCOR_MC_M_MESSAGE_SETUP_EMAILS EM")
				//sb.Append("               WHERE EM.CUST_CNT_FK=CCT.CUST_CONTACT_PK")
				//sb.Append("                       AND EM.CUST_MST_FK IN (" & customerPks & " ))>0) THEN")
				//sb.Append("                       CCT.NAME")
				//sb.Append("               ELSE")
				//sb.Append("                 CCT.NAME")
				//sb.Append("                END CUST_CNT_FK,")
				//sb.Append("              ")
				//sb.Append("               CASE")
				//sb.Append("                 WHEN E.EMAIL_ID IS NOT NULL THEN")
				//sb.Append("                  E.EMAIL_ID")
				//sb.Append("                 ELSE")
				//sb.Append("                  CCT.EMAIL")
				//sb.Append("               END EMAIL_ID,")
				//sb.Append("               CASE")
				//sb.Append("                 WHEN E.MOBILE_NO IS NOT NULL THEN")
				//sb.Append("                  E.MOBILE_NO")
				//sb.Append("                 ELSE")
				//sb.Append("                  CCT.MOBILE")
				//sb.Append("               END MOBILE_NO,")
				//sb.Append("               CASE")
				//sb.Append("                 WHEN CCT.FAX IS NOT NULL THEN")
				//sb.Append("                  CCT.FAX")
				//sb.Append("                 ELSE")
				//sb.Append("                  E.FAX_NO")
				//sb.Append("               END FAX,")
				//sb.Append("               CCT.CUST_CONTACT_PK PARTY_TYPE, '0' SELFLAG, QCOR_MC_M_MSG_SETUP_EMAIL_PK, '1' DISABLE_FLAG   ")
				//sb.Append("  FROM CUSTOMER_MST_TBL               CMT,")
				//sb.Append("       CUSTOMER_CONTACT_TRN           CCT,")
				//sb.Append("       QCOR_MC_M_MESSAGE_SETUP_EMAILS E")
				//sb.Append(" WHERE CMT.CUSTOMER_MST_PK IN (" & customerPks & " )")
				//sb.Append("   AND CCT.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK")
				//sb.Append("   AND E.CUST_MST_FK(+) = CCT.CUSTOMER_MST_FK")
				//sb.Append("   AND E.CUST_CNT_FK(+) <> 0")
				//If blnDataExists Then
				//    sb.Append("   AND E.CUST_CNT_FK(+) = CCT.CUST_CONTACT_PK")
				//End If
				//sb.Append(" )qry ")
				//If docPk > 0 And blnDataExists Then
				//    sb.Append(" WHERE DOC_MST_FK = " & docPk)
				//End If
				//sb.Append(" ORDER BY PARTY_NAME ")
				return objWF.GetDataTable(sb.ToString());
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		public DataSet FetchUserName(string selectId)
		{
			try {
				string strSQL = null;
				WorkFlow objWF = new WorkFlow();
				strSQL = "select u.user_name from user_mst_tbl u where u.user_mst_pk=" + selectId;
				return objWF.GetDataSet(strSQL.ToString());
			} catch (Exception ex) {
				throw ex;
			}
		}

		#region "Getting common table name,column Name"

		public DataSet fn_Get_MsgCenter_ColumnRelations(Int64 MasterPK)
		{

			WorkFlow objWF = new WorkFlow();
			DataSet ds = new DataSet();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			sb.Append(" select dfm.db_table_name || ' ' || dfm.db_table_alias,dfm.rel_db_table_name || ' ' || dfm.db_rel_table_alias,dfm.db_table_alias || '.'|| dfm.db_field_name ||'='||dfm.db_rel_table_alias||'.'||dfm.rel_db_field_name");
			sb.Append("          from qcor_mc_m_document_fields dfm");
			sb.Append("         where dfm.document_mst_fk = " + MasterPK + "  ");
			sb.Append("         and dfm.rel_db_field_name is not null");

			try {
				ds = objWF.GetDataSet(sb.ToString());
				return ds;
			//Manjunath  PTS ID:Sep-02   12/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		public DataSet fn_Get_MsgCenter_Columns(Int64 MasterPK)
		{

			WorkFlow objWF = new WorkFlow();
			DataSet ds = new DataSet();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			sb.Append(" select dfm.db_table_alias || '.' ||dfm.db_field_name ");
			sb.Append("          from qcor_mc_m_document_fields dfm");
			sb.Append("         where dfm.document_mst_fk = " + MasterPK + "  ");
			sb.Append("         and dfm.active_flag = 1");
			try {
				ds = objWF.GetDataSet(sb.ToString());
				return ds;
			//Manjunath  PTS ID:Sep-02   12/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}

		public DataSet fn_Get_MsgCenter_Tablesnames(Int64 MasterPK)
		{

			WorkFlow objWF = new WorkFlow();
			DataSet ds = new DataSet();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

			sb.Append(" select distinct(dfm.db_table_name || ' ' || dfm.db_table_alias)");
			sb.Append("          from qcor_mc_m_document_fields dfm");
			sb.Append("         where dfm.document_mst_fk = " + MasterPK + " ");

			try {
				ds = objWF.GetDataSet(sb.ToString());
				return ds;
			//Manjunath  PTS ID:Sep-02   12/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		public DataSet fn_Get_MsgCenter_Data(string sbQry)
		{

			WorkFlow objWF = new WorkFlow();
			DataSet ds = new DataSet();
			//     Dim sb As New System.Text.StringBuilder(5000)
			//     sb.Append(sbQry)

			try {
				//      ds = objWF.GetDataSet(sb.ToString)
				ds = objWF.GetDataSet(sbQry.ToString());
				return ds;
			} catch (Exception ex) {
				throw ex;
			}
		}
		public string fn_Get_MsgCenter_PK_ColumnName(Int64 docPk)
		{

			WorkFlow objWF = new WorkFlow();
			DataSet ds = new DataSet();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

			sb.Append(" select dfm.db_field_name ");
			sb.Append("          from qcor_mc_m_document_fields dfm");
			sb.Append("         where dfm.document_mst_fk = " + docPk + " ");
			sb.Append("         and  dfm.db_field_pk = 1");


			try {
				return objWF.ExecuteScaler(sb.ToString());
			//Manjunath  PTS ID:Sep-02   12/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}

		#endregion

		#region "Curtomer Look Up"
		public DataSet fn_FetchDetails(Int32 CurrentPage = 0, Int32 TotalPage = 0, string strWhereCondition = "", string CustPks = "", long ProtocolPk = 0, Int32 Party_Type = 0, string CustName = "")
		{
			try {
				WorkFlow objWF = new WorkFlow();

				System.Text.StringBuilder sb = new System.Text.StringBuilder();
				//If CustPks.Length > 0 Then
				//    sb.Append("SELECT ROWNUM SLNR, Q.*")
				//    sb.Append("  FROM (SELECT P.PARTY_MST_PK, P.PARTY_ID, P.PARTY_NAME,'1' CheckBox,P.PARTY_TYPE ")
				//    sb.Append("          FROM VIEW_DOC_MESSAGING_PARTY P WHERE P.PARTY_MST_PK IN(" & CustPks & ") AND P.PARTY_MST_PK NOT IN (SELECT CUST_MST_FK FROM QCOR_MC_M_MESSAGE_SETUP_CUST")

				//    If ProtocolPk > 0 Then
				//        sb.Append(" WHERE MESSAGE_SETUP_MST_FK <> " & ProtocolPk)
				//    End If
				//    sb.Append("   ) ")
				//    sb.Append(" " & strWhereCondition & " ")
				//    If Not Party_Type = 0 Then
				//        sb.Append(" AND P.PARTY_TYPE = " & Party_Type & " ")
				//    End If
				//    sb.Append(" Union All ")
				//    sb.Append(" SELECT P.PARTY_MST_PK, P.PARTY_ID, P.PARTY_NAME,'0' CheckBox,P.PARTY_TYPE ")
				//    sb.Append(" FROM VIEW_DOC_MESSAGING_PARTY P WHERE P.PARTY_MST_PK not IN (" & CustPks & ") AND P.PARTY_MST_PK NOT IN (SELECT CUST_MST_FK FROM QCOR_MC_M_MESSAGE_SETUP_CUST ")
				//    If ProtocolPk > 0 Then
				//        sb.Append(" WHERE MESSAGE_SETUP_MST_FK <> " & ProtocolPk)
				//    End If
				//    sb.Append("   ) ")
				//    sb.Append(" " & strWhereCondition & " ")
				//    If Not Party_Type = 0 Then
				//        sb.Append(" AND P.PARTY_TYPE = " & Party_Type & " ")
				//    End If
				//    sb.Append(" ) Q order by CheckBox DESC ")
				//Else
				//    sb.Append("SELECT ROWNUM SLNR, Q.*")
				//    sb.Append("  FROM (SELECT P.PARTY_MST_PK, P.PARTY_ID, P.PARTY_NAME,'0' CheckBox,P.PARTY_TYPE ")
				//    sb.Append("   FROM VIEW_DOC_MESSAGING_PARTY P  WHERE P.PARTY_MST_PK NOT IN (SELECT CUST_MST_FK FROM QCOR_MC_M_MESSAGE_SETUP_CUST ")
				//    If ProtocolPk > 0 Then
				//        sb.Append(" WHERE MESSAGE_SETUP_MST_FK <> " & ProtocolPk)
				//    End If
				//    sb.Append(" ) ")
				//    sb.Append(" " & strWhereCondition & " ")
				//    If Not Party_Type = 0 Then
				//        sb.Append(" AND P.PARTY_TYPE = " & Party_Type & " ")
				//    End If
				//    sb.Append(" ORDER BY P.PARTY_ID) Q ")
				//End If

				//Dim sb As New System.Text.StringBuilder(5000)
				if (!string.IsNullOrEmpty(strWhereCondition)) {
					strWhereCondition = strWhereCondition.Replace("AND", "WHERE");
					if (strWhereCondition.IndexOf("PARTY_ID") > 0) {
						strWhereCondition = strWhereCondition.Replace("PARTY_ID", "UPPER(PARTY_ID)");
					} else if (strWhereCondition.IndexOf("PARTY_NAME") > 0) {
						strWhereCondition = strWhereCondition.Replace("PARTY_NAME", "UPPER(PARTY_NAME)");
					}
				}
				sb.Append("SELECT ROWNUM SLNR, Q.*");
				sb.Append("          FROM (");
				//'For DTS:12474
				//sb.Append("     SELECT P.PARTY_MST_PK,")
				//sb.Append("                       P.PARTY_ID,")
				//sb.Append("                       P.PARTY_NAME,")
				//sb.Append("                       '1' CheckBox,")
				//sb.Append("                       P.PARTY_TYPE")
				//sb.Append("                  FROM VIEW_DOC_MESSAGING_PARTY     P,")
				//sb.Append("                       QCOR_MC_M_MESSAGE_SETUP_CUST QMST")
				//sb.Append("                 WHERE QMST.CUST_MST_FK = P.PARTY_MST_PK")
				//sb.Append("                   AND QMST.MESSAGE_SETUP_MST_FK =" & ProtocolPk)
				//sb.Append("                UNION")
				//'End
				sb.Append("                SELECT P.PARTY_MST_PK,");
				sb.Append("                       P.PARTY_ID,");
				sb.Append("                       P.PARTY_NAME,");
				sb.Append("                       '0' CheckBox,");
				sb.Append("                       P.PARTY_TYPE");
				sb.Append("                  FROM VIEW_DOC_MESSAGING_PARTY P");
				sb.Append("                 WHERE P.PARTY_MST_PK NOT IN");
				sb.Append("                       (SELECT CUST_MST_FK FROM QCOR_MC_M_MESSAGE_SETUP_CUST )");
				sb.Append("                AND P.PARTY_TYPE=" + Party_Type);
				if (!string.IsNullOrEmpty(CustName)) {
					sb.Append("                AND P.PARTY_ID LIKE '%" + CustName + "%'");
				}
				sb.Append("       ORDER BY CheckBox DESC,PARTY_ID ASC ) Q ");
				sb.Append(" " + strWhereCondition.ToUpper() + " ");

				///'''''''''''''''common''''''''''''''''''''
				//Get the Total Pages
				Int32 last = default(Int32);
				Int32 start = default(Int32);
				Int32 TotalRecords = default(Int32);
				string StrSqlCount = null;

				StrSqlCount = "SELECT COUNT(*) FROM ( ";
				StrSqlCount = StrSqlCount + sb.ToString();
				StrSqlCount = StrSqlCount + " ) ";

				TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(StrSqlCount.ToString()));
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

				///'''''''''''''''''''''''''''''''''''''''''''''''''''
				///'''''''''''''''''''''''''''common''''''''''''''''''''''''''''''''''
				string StrSqlRecords = null;
				StrSqlRecords = "SELECT * FROM ( ";
				StrSqlRecords = StrSqlRecords + sb.ToString();
				StrSqlRecords = StrSqlRecords + " ) WHERE SLNR BETWEEN " + start + " AND " + last;
				return objWF.GetDataSet(StrSqlRecords);
			//Manjunath  PTS ID:Sep-02   12/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		public void fn_DelEmailDetails(OracleTransaction TRAN, string EmailSetupPKs)
		{
			try {
				Int16 i = default(Int16);
				string[] emailPKs = EmailSetupPKs.Split(',');
				WorkFlow objWK = new WorkFlow();
				objWK.MyConnection = TRAN.Connection;
				objWK.OpenConnection();
				OracleCommand insCommand = new OracleCommand();
				for (i = 0; i <= emailPKs.Length - 1; i++) {
					if (emailPKs[i] == "null") {
						emailPKs[i] = "0";
					}
					var _with16 = insCommand;
					_with16.Connection = objWK.MyConnection;
					_with16.CommandType = CommandType.StoredProcedure;
					_with16.CommandText = objWK.MyUserName + ".Pkg_Qcor_Mc_m_Message_Setup.QCOR_MC_DOCS_EMAIL_SET_DEL";
					_with16.Parameters.Clear();
					_with16.Parameters.Add("EMAIL_SETUP_PK_IN", Convert.ToInt32(emailPKs[i])).Direction = ParameterDirection.Input;
					var _with17 = objWK.MyDataAdapter;
					_with17.InsertCommand = insCommand;
					_with17.InsertCommand.Transaction = TRAN;
					_with17.InsertCommand.ExecuteNonQuery();
				}

			//Manjunath  PTS ID:Sep-02   12/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
	}
}
