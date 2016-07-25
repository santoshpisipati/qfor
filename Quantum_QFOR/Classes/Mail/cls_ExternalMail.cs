using Oracle.DataAccess.Client;
using System;
using System.Collections;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Net;
using System.Net.Mail;
using System.Text;
using System.Web;
using System.Xml;

namespace Quantum_QFOR
{
    public class cls_ExternalMail : CommonFeatures
    {
        #region "WORKFLOW Properties"
        public string DocRefNr = "";
        public string TaskDocRefNr = "";
        public string CurrentActivityWF = "";
        public string NextActivityWF = "";
        public string TaskDocRefDt = "";
        public string TaskDocRefStatus = "";
        public string TaskDeadLineDt = "";
        public string TaskOverDueDt = "";
        #endregion
        public int TaskDocCreatedBy = 0;

        public int DocTypePK;
        public string DocNr;
        public string Create_by;

        public string M_MAIL_BODY;
        public string M_MAIL_SUBJECT;
        public string M_MAIL_TO;
        public string M_MAIL_CC;
        public string M_MAIL_BCC;
        public Int32 M_MAIL_ATTACHMENTS_COUNT;
        public string M_MailServer = ConfigurationManager.AppSettings["MailServer"];

        #region "Global Variable"
        public string M_SMTP_SERVER = ConfigurationManager.AppSettings["MailServer"];
        public string M_SEND_USERNAME = ConfigurationManager.AppSettings["SEND_USERNAME"];
        public string M_SEND_PASSWORD = ConfigurationManager.AppSettings["SEND_PASSWORD"];
        public string M_MAIL_SERVER = ConfigurationManager.AppSettings["MailServer"];
        public string M_SEND_NAME = ConfigurationManager.AppSettings["CustSupportName"];
        public static string M_AnnId = "";
        public static string Create_By = "";
        public static string M_Name = ConfigurationManager.AppSettings["SEND_USERNAME"];
        public static string M_Password = ConfigurationManager.AppSettings["SEND_PASSWORD"];
        public string M_MAIL_ATTACH_PATH;
        public string M_MAIL_ATTACH_DELIMTER = "^^^";
        public string MSendUserName = ConfigurationManager.AppSettings["SEND_USERNAME"];
        public string MSendPassword = ConfigurationManager.AppSettings["SEND_PASSWORD"];



        #endregion
        public static string FileName;

        public Int32 fn_send_ExternalMail()
        {

            try
            {
                SmtpClient smtpserver = new SmtpClient();
                MailMessage objMail = new MailMessage();
                WorkFlow objWF = new WorkFlow();
                string EAttach = null;
                string dsMail = null;
                Int32 intCnt = default(Int32);
                Int32 index = default(Int32);
                string strbody = null;
                bool Result = false;
                if (string.Compare(M_MAIL_TO, ";")>0)
                {
                    M_MAIL_TO = M_MAIL_TO.Replace(";", ",");

                }

                index = M_MAIL_TO.LastIndexOf(",");
                if (index != -1)
                {
                    if (M_MAIL_TO.EndsWith(","))
                    {
                        M_MAIL_TO = M_MAIL_TO.Remove(index);
                    }
                }
                objMail = new MailMessage();
                objMail.From = new MailAddress(MSendUserName, M_SEND_NAME);
                objMail.To.Add(M_MAIL_TO);
                objMail.Subject = M_MAIL_SUBJECT;
                objMail.IsBodyHtml = true;
                objMail.Body = M_MAIL_BODY;
                objMail.BodyEncoding = Encoding.UTF8;
                if (M_MAIL_ATTACHMENTS_COUNT > 0)
                {
                    Attachment ObjAttachment = default(Attachment);
                    foreach (string M_MAIL_ATTACH_PATH_loopVariable in M_MAIL_ATTACH_PATH.Split(Convert.ToChar("^^^")))
                    {
                        M_MAIL_ATTACH_PATH = M_MAIL_ATTACH_PATH_loopVariable;
                        ObjAttachment = new Attachment(M_MAIL_ATTACH_PATH);
                        objMail.Attachments.Add(ObjAttachment);
                    }
                }
                smtpserver = new SmtpClient(M_MailServer, 587);
                NetworkCredential basicCredentials = new NetworkCredential(MSendUserName, MSendPassword);
                if (string.Compare(MSendUserName, "gmail.com") > 0)
                {
                    smtpserver.EnableSsl = true;
                }
                smtpserver.UseDefaultCredentials = true;
                smtpserver.Credentials = basicCredentials;
                object userState = objMail;
                smtpserver.SendCompleted += SendCompletedCallback;
                smtpserver.SendAsync(objMail, userState);
                objMail = null;

            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return 0;
        }

        #region " Send Message "
        public string SendMessage(long lngCreatedBy, long lngPkValue, string strDocId, string strCustomer, bool blnApp, string strSpecificURL, long lngLocFk, string strSRRRefNo, string strSrrDate, long SenderFk = 0,
        string Status = "", long Biz_Type = 0)
        {
            string functionReturnValue = null;
            DataSet dsMsg = new DataSet();
            DataSet dsDoc = null;
            DataSet dsUser = new DataSet();
            DataSet dsSender = new DataSet();
            string strUsrName = null;
            string strLocName = null;
            string strMsgSub = null;
            string strMsgHeader = null;
            string strMsgFooter = null;
            string strMsgBody = null;
            DataRow DR = null;
            DataSet dsUserFks = null;
            long lngdocPk = 0;
            string strSenderLocName = null;
            string strSenderUsrName = null;
            string strPageURL = null;
            long lngUserFk = 0;
            int intSenderCnt = 0;
            int Cargo_Type = 0;
            WorkFlow objWF = new WorkFlow();

            try
            {
                dsDoc = FetchDocument(strDocId);
                if (dsDoc.Tables[0].Rows.Count > 0)
                {
                    lngdocPk = Convert.ToInt64(dsDoc.Tables[0].Rows[0]["DOCUMENT_MST_PK"]);
                    dsMsg = GetMessageInfo(lngdocPk, -1);
                    if (dsMsg.Tables[0].Rows.Count < 1)
                    {
                        DR = dsMsg.Tables[0].NewRow();
                        dsMsg.Tables[0].Rows.Add(DR);
                    }

                    dsUserFks = GetUserInfo(lngdocPk, lngLocFk, SenderFk);
                    if (dsUserFks.Tables[0].Rows.Count > 0)
                    {
                        for (intSenderCnt = 0; intSenderCnt <= dsUserFks.Tables[0].Columns.Count - 1; intSenderCnt++)
                        {
                            if (!(string.IsNullOrEmpty(dsUserFks.Tables[0].Rows[0][intSenderCnt].ToString())))
                            {
                                if ((Convert.ToInt32(dsUserFks.Tables[0].Rows[0][intSenderCnt])) > 0)
                                {
                                    dsUser = FetchUserDetails(Convert.ToInt64(dsUserFks.Tables[0].Rows[0][intSenderCnt]));
                                    strUsrName = Convert.ToString(dsUser.Tables[0].Rows[0]["USER_NAME"]);
                                    strLocName = Convert.ToString(dsUser.Tables[0].Rows[0]["LOCATION_NAME"]);

                                    if (dsMsg.Tables[0].Rows.Count > 0)
                                    {
                                        var _with1 = dsMsg.Tables[0].Rows[0];
                                        _with1["Sender_Fk"] = lngCreatedBy;
                                        dsSender = FetchUserDetails(lngCreatedBy);
                                        strSenderUsrName = Convert.ToString(dsSender.Tables[0].Rows[0]["USER_NAME"]);
                                        strSenderLocName = Convert.ToString(dsSender.Tables[0].Rows[0]["LOCATION_NAME"]);

                                        _with1["Receiver_Fk"] = Convert.ToInt64(dsUserFks.Tables[0].Rows[0][intSenderCnt]);
                                        _with1["Msg_Read"] = 0;
                                        _with1["Followup_Flag"] = 0;
                                        _with1["Have_Attachment"] = 1;

                                        strMsgBody = Convert.ToString(_with1["Msg_Body"]);
                                        strMsgBody = strMsgBody.Replace("<<", "<");
                                        strMsgBody = strMsgBody.Replace("&lt;&lt;", "<");
                                        strMsgBody = strMsgBody.Replace(">>", ">");
                                        strMsgBody = strMsgBody.Replace("&gt;&gt;", ">");
                                        strMsgBody = strMsgBody.Replace("<SENDER>", (strSenderUsrName + ", " + strSenderLocName));
                                        strMsgBody = strMsgBody.Replace("<RECEIVER>", strUsrName);
                                        strMsgBody = strMsgBody.Replace("<CURRENT DATE>", Convert.ToString(DateTime.Now.Date));
                                        strMsgBody = strMsgBody.Replace("<SRR Nr.>", strSRRRefNo);
                                        strMsgBody = strMsgBody.Replace("<SRR DATE>", strSrrDate);
                                        strMsgBody = strMsgBody.Replace("<CUSTOMER>", strCustomer);
                                        if (!string.IsNullOrEmpty(Status))
                                        {
                                            strMsgBody = strMsgBody.Replace("<STATUS>", Status);
                                        }
                                        _with1["Msg_Body"] = strMsgBody;

                                        strMsgSub = Convert.ToString(_with1["MSG_SUBJECT"]);
                                        strMsgSub = strMsgSub.Replace("<<", "<");
                                        strMsgSub = strMsgSub.Replace("&lt;&lt;", "<");
                                        strMsgSub = strMsgSub.Replace(">>", ">");
                                        strMsgSub = strMsgSub.Replace("&gt;&gt;", ">");
                                        strMsgSub = strMsgSub.Replace("<SENDER>", (strSenderUsrName + ", " + strSenderLocName));
                                        strMsgSub = strMsgSub.Replace("<RECEIVER>", strUsrName);
                                        strMsgSub = strMsgSub.Replace("<CURRENT DATE>", Convert.ToString(DateTime.Now.Date));
                                        strMsgSub = strMsgSub.Replace("<SRR Nr.>", strSRRRefNo);
                                        strMsgSub = strMsgSub.Replace("<SRR DATE>", strSrrDate);
                                        strMsgSub = strMsgSub.Replace("<CUSTOMER>", strCustomer);
                                        _with1["Msg_Subject"] = strMsgSub;

                                        _with1["Read_Receipt"] = 0;
                                        _with1["Document_Mst_Fk"] = lngdocPk;
                                        _with1["User_Message_Folders_Fk"] = 1;
                                        _with1["Msg_Received_Dt"] = DateTime.Now;
                                    }
                                    else
                                    {
                                        return "Please define WorkFlow for the particular document";
                                    }
                                    if (dsMsg.Tables[1].Rows.Count < 1)
                                    {
                                        DR = dsMsg.Tables[1].NewRow();
                                        dsMsg.Tables[1].Rows.Add(DR);
                                    }
                                    if (dsMsg.Tables[1].Rows.Count > 0)
                                    {
                                        var _with2 = dsMsg.Tables[1].Rows[0];
                                        if (!string.IsNullOrEmpty(_with2["URL_PAGE"].ToString()))
                                        {
                                            strPageURL = Convert.ToString(_with2["URL_PAGE"]);
                                        }
                                        else
                                        {
                                            strPageURL = "";
                                        }
                                        strPageURL = strPageURL.Replace("directory", strSpecificURL);
                                        strPageURL = strPageURL.Replace("pkvalue", Convert.ToString(lngPkValue));
                                        strPageURL = strPageURL.Replace("Biztype", Convert.ToString(Biz_Type));
                                        if (string.IsNullOrEmpty(strPageURL))
                                        {
                                            _with2["URL_PAGE"] = DBNull.Value;
                                            _with2["ATTACHMENT_DATA"] = DBNull.Value;
                                        }
                                        else
                                        {
                                            if (strDocId == "Quotation Sea Request" | strDocId == "Quotation Sea Approval" | strDocId == "Quotation Import Sea Request" | strDocId == "Quotation Import Sea Approval")
                                            {
                                                Cargo_Type = Convert.ToInt32(objWF.ExecuteScaler("SELECT Q.CARGO_TYPE FROM QUOTATION_MST_TBL Q WHERE Q.QUOTATION_MST_PK = " + lngPkValue));
                                                strPageURL = strPageURL + "&LableStatus=" + Cargo_Type;
                                            }
                                            _with2["URL_PAGE"] = strPageURL;
                                            _with2["ATTACHMENT_DATA"] = strPageURL;
                                        }
                                    }

                                    if ((SaveUnApprovedTDR(dsMsg, lngCreatedBy) == -1))
                                    {
                                        return "<br>Due to some problem Mail has not been sent</br>";
                                        return functionReturnValue;
                                    }
                                }
                            }
                        }
                        return "<br>All data saved Successfully and Mail has been sent</br>";
                    }
                    else
                    {
                        return "<br>Mail has not been sent.Document Id not created.</br>";
                    }
                }
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return functionReturnValue;
        }
        #endregion

        #region "SendMessageNew "
        public string SendMessageNew(long lngCreatedBy, long lngPkValue, string strDocId, long lngLocFk, System.DateTime salecaldate, long SenderFk = 0, string strSpecificURL = "", long strpk = 0, long Biz_Type = 0, string TypeFlag = "",
        string strReqDocId = "", int ApprovalFlg = 0, int ReqByUserFk = 0)
        {
            string functionReturnValue = null;
            DataSet dsMsg = new DataSet();
            DataSet dsDoc = null;
            DataSet dsUser = new DataSet();
            DataSet dsSender = new DataSet();
            string strUsrName = null;
            string strLocName = null;
            string strMsgSub = null;
            string strMsgHeader = null;
            string strMsgFooter = null;
            string strMsgBody = null;
            DataRow DR = null;
            DataSet dsUserFks = null;
            long lngdocPk = 0;
            string strSenderLocName = null;
            string strSenderUsrName = null;
            string strPageURL = null;
            long lngUserFk = 0;
            int intSenderCnt = 0;
            int Cargo_Type = 0;
            WorkFlow objWF = new WorkFlow();
            //DocRefNr = ""
            try
            {
                dsDoc = FetchDocument(strDocId);
                if (dsDoc.Tables[0].Rows.Count > 0)
                {
                    lngdocPk = Convert.ToInt64(dsDoc.Tables[0].Rows[0]["DOCUMENT_MST_PK"]);
                    dsMsg = GetMessageInfo(lngdocPk, -1);
                    if (dsMsg.Tables[0].Rows.Count < 1)
                    {
                        DR = dsMsg.Tables[0].NewRow();
                        dsMsg.Tables[0].Rows.Add(DR);
                    }
                    if (ApprovalFlg == 1)
                    {
                        dsDoc = FetchDocument(strReqDocId);
                        if (dsDoc.Tables[0].Rows.Count > 0)
                        {
                            lngdocPk = Convert.ToInt64(dsDoc.Tables[0].Rows[0]["DOCUMENT_MST_PK"]);
                        }
                        dsUserFks = GetApproverInfo(lngdocPk, ReqByUserFk);
                    }
                    else
                    {
                        dsUserFks = GetUserInfo(lngdocPk, lngLocFk, SenderFk, ReqByUserFk);
                    }
                    if (dsUserFks.Tables[0].Rows.Count == 0 & TaskDocCreatedBy > 0)
                    {
                        dsUserFks = GetUserInfo(lngdocPk, lngLocFk, SenderFk, TaskDocCreatedBy);
                    }
                    if (dsUserFks.Tables[0].Rows.Count > 0)
                    {
                        for (intSenderCnt = 0; intSenderCnt <= dsUserFks.Tables[0].Columns.Count - 1; intSenderCnt++)
                        {
                            if (!(string.IsNullOrEmpty(dsUserFks.Tables[0].Rows[0][intSenderCnt].ToString())))
                            {
                                if ((Convert.ToInt32(dsUserFks.Tables[0].Rows[0][intSenderCnt])) > 0)
                                {
                                    dsUser = FetchUserDetails(Convert.ToInt64(dsUserFks.Tables[0].Rows[0][intSenderCnt]));
                                    strUsrName = Convert.ToString(dsUser.Tables[0].Rows[0]["USER_NAME"]);
                                    strLocName = Convert.ToString(dsUser.Tables[0].Rows[0]["LOCATION_NAME"]);
                                    if (dsMsg.Tables[0].Rows.Count > 0)
                                    {
                                        var _with3 = dsMsg.Tables[0].Rows[0];
                                        _with3["Sender_Fk"] = lngCreatedBy;
                                        dsSender = FetchUserDetails(lngCreatedBy);
                                        strSenderUsrName = Convert.ToString(dsSender.Tables[0].Rows[0]["USER_NAME"]);
                                        strSenderLocName = Convert.ToString(dsSender.Tables[0].Rows[0]["LOCATION_NAME"]);

                                        _with3["Receiver_Fk"] = Convert.ToInt64(dsUserFks.Tables[0].Rows[0][intSenderCnt]);
                                        _with3["Msg_Read"] = 0;
                                        _with3["Followup_Flag"] = 0;
                                        _with3["Have_Attachment"] = 1;

                                        strMsgBody = Convert.ToString(_with3["Msg_Body"]);
                                        strMsgBody = strMsgBody.Replace("<<", "<");
                                        strMsgBody = strMsgBody.Replace("&lt;&lt;", "<");
                                        strMsgBody = strMsgBody.Replace(">>", ">");
                                        strMsgBody = strMsgBody.Replace("&gt;&gt;", ">");
                                        strMsgBody = strMsgBody.Replace("<SENDER>", (strSenderUsrName + ", " + strSenderLocName));
                                        strMsgBody = strMsgBody.Replace("<RECEIVER>", strUsrName);
                                        strMsgBody = strMsgBody.Replace("<CURRENT DATE>", Convert.ToString(DateTime.Now.Date));
                                        strMsgBody = GetMessageBody(lngPkValue, strDocId, strMsgBody, salecaldate, strpk, TypeFlag);
                                        //Added by Faheem
                                        _with3["Msg_Body"] = strMsgBody;
                                        strMsgSub = Convert.ToString(_with3["MSG_SUBJECT"]);
                                        strMsgSub = GetMessageBody(lngPkValue, strDocId, strMsgSub, salecaldate, strpk, TypeFlag);
                                        //Added by Faheem
                                        _with3["Msg_Subject"] = strMsgSub;

                                        _with3["Read_Receipt"] = 0;
                                        _with3["Document_Mst_Fk"] = lngdocPk;
                                        _with3["User_Message_Folders_Fk"] = 1;
                                        _with3["Msg_Received_Dt"] = DateTime.Now;
                                    }
                                    else
                                    {
                                        return "Please define WorkFlow for the particular document";
                                    }
                                    if (dsMsg.Tables[1].Rows.Count < 1)
                                    {
                                        DR = dsMsg.Tables[1].NewRow();
                                        dsMsg.Tables[1].Rows.Add(DR);
                                    }
                                    if (dsMsg.Tables[1].Rows.Count > 0)
                                    {
                                        var _with4 = dsMsg.Tables[1].Rows[0];
                                        if (!string.IsNullOrEmpty(_with4["URL_PAGE"].ToString()))
                                        {
                                            strPageURL = Convert.ToString(_with4["URL_PAGE"]);
                                        }
                                        else
                                        {
                                            strPageURL = "";
                                        }
                                        strPageURL = strPageURL.Replace("directory", strSpecificURL);
                                        strPageURL = strPageURL.Replace("FORM_PK", Convert.ToString(lngPkValue));
                                        strPageURL = strPageURL.Replace("Biztype", Convert.ToString(Biz_Type));
                                        if (string.IsNullOrEmpty(strPageURL))
                                        {
                                            _with4["URL_PAGE"] = DBNull.Value;
                                            _with4["ATTACHMENT_DATA"] = DBNull.Value;
                                        }
                                        else
                                        {
                                            if (strDocId == "Quotation Sea Request" | strDocId == "Quotation Sea Approval" | strDocId == "Quotation Import Sea Request" | strDocId == "Quotation Import Sea Approval")
                                            {
                                                Cargo_Type = Convert.ToInt32(objWF.ExecuteScaler("SELECT Q.CARGO_TYPE FROM QUOTATION_MST_TBL Q WHERE Q.QUOTATION_MST_PK = " + lngPkValue));
                                                strPageURL = strPageURL + "&LableStatus=" + Cargo_Type;
                                            }
                                            if (strDocId == "SL Contract Request" | strDocId == "SL Contract  Approval")
                                            {
                                                Cargo_Type = Convert.ToInt32(objWF.ExecuteScaler("SELECT DISTINCT C.CARGO_TYPE FROM CONT_MAIN_SEA_TBL C WHERE C.CONT_MAIN_SEA_PK = " + lngPkValue));
                                                if (intSenderCnt == 0)
                                                {
                                                    strPageURL = strPageURL + "&IsLCL=" + Cargo_Type;
                                                }
                                            }
                                            _with4["URL_PAGE"] = strPageURL;
                                            _with4["ATTACHMENT_DATA"] = strPageURL;
                                        }
                                    }

                                    if ((SaveUnApprovedTDR(dsMsg, lngCreatedBy) == -1))
                                    {
                                        return "<br>Due to some problem Mail has not been sent</br>";
                                        return functionReturnValue;
                                    }
                                }
                            }
                        }
                        return "<br>All data saved Successfully and Mail has been sent</br>";
                    }
                    else
                    {
                        return "<br>Mail has not been sent.Document Id not created or Workflow is Inactive.</br>";
                    }
                }
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return functionReturnValue;
        }
        #endregion

        #region "SendMessageNew "
        public string SendMessageJMF(long lngCreatedBy, DataSet dsUserFks, string strDocId, long lngLocFk, System.DateTime salecaldate, long SenderFk = 0, string strSpecificURL = "", string JobPk = "0", string Reminderpk = "0", long Biz_Type = 0, long Process_Type = 0, long strpk = 0, string TypeFlag = "", string strReqDocId = "", int ApprovalFlg = 0, int ReqByUserFk = 0)
        {
            string functionReturnValue = null;
            DataSet dsMsg = new DataSet();
            DataSet dsDoc = null;
            DataSet dsUser = new DataSet();
            DataSet dsSender = new DataSet();
            string strUsrName = null;
            string strLocName = null;
            string strMsgSub = null;
            string strMsgHeader = null;
            string strMsgFooter = null;
            string strMsgBody = null;
            DataRow DR = null;
            long lngdocPk = 0;
            string strSenderLocName = null;
            string strSenderUsrName = null;
            string strPageURL = null;
            long lngUserFk = 0;
            int intSenderCnt = 0;
            int Cargo_Type = 0;
            long lngPkValue = 0;
            WorkFlow objWF = new WorkFlow();
            DocRefNr = "";
            try
            {
                dsDoc = FetchDocument(strDocId);
                if (dsDoc.Tables[0].Rows.Count > 0)
                {
                    lngdocPk = Convert.ToInt64(dsDoc.Tables[0].Rows[0]["DOCUMENT_MST_PK"]);
                    dsMsg = GetMessageInfo(lngdocPk, -1);
                    if (dsMsg.Tables[0].Rows.Count < 1)
                    {
                        DR = dsMsg.Tables[0].NewRow();
                        dsMsg.Tables[0].Rows.Add(DR);
                    }
                    if (dsUserFks.Tables[0].Rows.Count > 0)
                    {
                        for (intSenderCnt = 0; intSenderCnt <= dsUserFks.Tables[0].Rows.Count - 1; intSenderCnt++)
                        {
                            if (!(string.IsNullOrEmpty(dsUserFks.Tables[0].Rows[intSenderCnt]["USER_MST_PK"].ToString())))
                            {
                                if ((Convert.ToInt32(dsUserFks.Tables[0].Rows[intSenderCnt]["USER_MST_PK"])) > 0)
                                {
                                    lngPkValue = Convert.ToInt64(dsUserFks.Tables[0].Rows[intSenderCnt]["USER_MST_PK"].ToString());
                                    dsUser = FetchUserDetails(Convert.ToInt64(dsUserFks.Tables[0].Rows[intSenderCnt]["USER_MST_PK"]));
                                    strUsrName = Convert.ToString(dsUser.Tables[0].Rows[0]["USER_NAME"]);
                                    strLocName = Convert.ToString(dsUser.Tables[0].Rows[0]["LOCATION_NAME"]);
                                    if (dsMsg.Tables[0].Rows.Count > 0)
                                    {
                                        var _with5 = dsMsg.Tables[0].Rows[0];
                                        _with5["Sender_Fk"] = lngCreatedBy;
                                        dsSender = FetchUserDetails(lngCreatedBy);
                                        strSenderUsrName = Convert.ToString(dsSender.Tables[0].Rows[0]["USER_NAME"]);
                                        strSenderLocName = Convert.ToString(dsSender.Tables[0].Rows[0]["LOCATION_NAME"]);

                                        _with5["Receiver_Fk"] = Convert.ToInt64(dsUserFks.Tables[0].Rows[intSenderCnt]["USER_MST_PK"]);
                                        _with5["Msg_Read"] = 0;
                                        _with5["Followup_Flag"]= 0;
                                        _with5["Have_Attachment"]= 1;

                                        strMsgBody = Convert.ToString(_with5["Msg_Body"]);
                                        strMsgBody = strMsgBody.Replace("<<", "<");
                                        strMsgBody = strMsgBody.Replace("&lt;&lt;", "<");
                                        strMsgBody = strMsgBody.Replace(">>", ">");
                                        strMsgBody = strMsgBody.Replace("&gt;&gt;", ">");
                                        strMsgBody = strMsgBody.Replace("<SENDER>", (strSenderUsrName + ", " + strSenderLocName));
                                        strMsgBody = strMsgBody.Replace("<RECEIVER>", strUsrName);
                                        strMsgBody = strMsgBody.Replace("<CURRENT DATE>", Convert.ToString(DateTime.Now.Date));
                                        strMsgBody = GetMessageBody(Convert.ToInt64(JobPk), strDocId, strMsgBody, salecaldate, Convert.ToInt64(Reminderpk), TypeFlag);
                                        //Added by Faheem
                                        _with5["Msg_Body"] = strMsgBody;
                                        strMsgSub = Convert.ToString(_with5["MSG_SUBJECT"]);
                                        strMsgSub = GetMessageBody(Convert.ToInt64(JobPk), strDocId, strMsgSub, salecaldate, Convert.ToInt64(Reminderpk), TypeFlag);
                                        //Added by Faheem
                                        _with5["Msg_Subject"] = strMsgSub;

                                        _with5["Read_Receipt"] = 0;
                                        _with5["Document_Mst_Fk"] = lngdocPk;
                                        _with5["User_Message_Folders_Fk"] = 1;
                                        _with5["Msg_Received_Dt"] = DateTime.Now;
                                    }
                                    else
                                    {
                                        return "Please define WorkFlow for the particular document";
                                    }
                                    if (dsMsg.Tables[1].Rows.Count < 1)
                                    {
                                        DR = dsMsg.Tables[1].NewRow();
                                        dsMsg.Tables[1].Rows.Add(DR);
                                    }
                                    if (dsMsg.Tables[1].Rows.Count > 0)
                                    {
                                        var _with6 = dsMsg.Tables[1].Rows[0];
                                        if (!string.IsNullOrEmpty(_with6["URL_PAGE"].ToString()))
                                        {
                                            strPageURL = Convert.ToString(_with6["URL_PAGE"]);
                                        }
                                        else
                                        {
                                            strPageURL = "";
                                        }
                                        strPageURL = strPageURL.Replace("directory", strSpecificURL);
                                        //strPageURL = strPageURL.Replace("FORM_PK", CStr(JobPk))
                                        //strPageURL = strPageURL.Replace("Biztype", CStr(Biz_Type))
                                        if (string.IsNullOrEmpty(strPageURL))
                                        {
                                            _with6["URL_PAGE"] = DBNull.Value;
                                            _with6["ATTACHMENT_DATA"] = DBNull.Value;
                                        }
                                        else
                                        {
                                            _with6["URL_PAGE"] = "../documentation/frmjobmanagement.aspx?FROM_FLAG=Message&DOCUMENT_FK=" + JobPk + "&BIZ_TYPE=" + Biz_Type + "&PROCESS_TYPE=" + Process_Type;
                                            _with6["ATTACHMENT_DATA"] = strSpecificURL;
                                        }
                                    }

                                    if ((SaveUnApprovedTDR(dsMsg, lngCreatedBy) == -1))
                                    {
                                        return "<br>Due to some problem Mail has not been sent</br>";
                                        return functionReturnValue;
                                    }
                                }
                            }
                        }
                        return "<br>All data saved Successfully and Mail has been sent</br>";
                    }
                    else
                    {
                        return "<br>Mail has not been sent.Document Id not created or Workflow is Inactive.</br>";
                    }
                }
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return functionReturnValue;
        }
        public string SendExternalMailJMF(string PKvalue, DataSet dsUserFks, string strDocId = "", string File_Paths = "", string ExtraPk = "")
        {
            string functionReturnValue = null;

            try
            {
                SmtpClient smtpserver = new SmtpClient();
                MailMessage objMail = new MailMessage();
                WorkFlow objWF = new WorkFlow();
                string EAttach = null;
                string dsMail = null;
                Int32 intCnt = default(Int32);
                string strbody = null;
                string M_MAIL_ATTACHMENTS_PATH = null;
                bool Result = false;
                DataSet dsMsg = new DataSet();
                long lngPkValue = 0;
                string strMsgSub = null;
                DataSet dsDoc = null;
                long lngdocPk = 0;
                Create_By = HttpContext.Current.Session["USER_PK"].ToString();
                dsDoc = FetchDocument(strDocId);
                if (dsDoc.Tables[0].Rows.Count > 0)
                {
                    lngdocPk = Convert.ToInt64(dsDoc.Tables[0].Rows[0]["DOCUMENT_MST_PK"]);
                }
                dsMsg = GetMessageInfo(lngdocPk, -1);
                string strMsgBody = Convert.ToString(dsMsg.Tables[0].Rows[0]["Msg_Body"]);
                strMsgSub = Convert.ToString(dsMsg.Tables[0].Rows[0]["MSG_SUBJECT"]);
                strMsgBody = GetMessageBody(Convert.ToInt64(PKvalue), strDocId, strMsgBody, System.DateTime.Now, Convert.ToInt64(ExtraPk));
                //Added by Faheem
                string EmailId = null;
                Int16 i = default(Int16);
                Int16 j = default(Int16);
                if (dsUserFks.Tables[0].Rows.Count > 0)
                {
                    for (int intSenderCnt = 0; intSenderCnt <= dsUserFks.Tables[0].Rows.Count - 1; intSenderCnt++)
                    {
                        if (!(string.IsNullOrEmpty(dsUserFks.Tables[0].Rows[intSenderCnt]["USER_MST_PK"].ToString())))
                        {
                            if ((Convert.ToInt32(dsUserFks.Tables[0].Rows[intSenderCnt]["USER_MST_PK"])) > 0)
                            {
                                lngPkValue = Convert.ToInt64(dsUserFks.Tables[0].Rows[intSenderCnt]["USER_MST_PK"]);
                                EmailId = (string.IsNullOrEmpty(dsUserFks.Tables[0].Rows[intSenderCnt]["EMAIL_ID"].ToString()) ? "" : dsUserFks.Tables[0].Rows[intSenderCnt]["EMAIL_ID"].ToString());
                                if (!string.IsNullOrEmpty(EmailId))
                                {
                                    objMail = new MailMessage();
                                    objMail.From = new MailAddress(M_SEND_USERNAME, M_SEND_NAME);
                                    objMail.To.Add(EmailId);

                                    objMail.Subject = strMsgSub;
                                    objMail.IsBodyHtml = true;
                                    //strbody = " Dear  " & dsUserFks.Tables(0).Rows(intSenderCnt)["USER_NAME") & vbCrLf & vbCrLf
                                    strbody = strMsgBody.Replace("<Receiver>", dsUserFks.Tables[0].Rows[intSenderCnt]["USER_NAME"].ToString());
                                    strbody += "   " ;
                                    // strbody &= " " & strMsgBody & " " & vbCrLf
                                    strbody += " This is an auto generated e-Mail. Please do not reply to this e-Mail-ID." ;
                                    strbody += " " ;
                                    strbody = ConvertHTML(strbody);
                                    objMail.Body = strbody;
                                    objMail.BodyEncoding = Encoding.UTF8;

                                    if (!string.IsNullOrEmpty(File_Paths))
                                    {
                                        M_MAIL_ATTACHMENTS_PATH = File_Paths;
                                        for (i = 0; i <= M_MAIL_ATTACHMENTS_PATH.Split(Convert.ToChar(",")).Length - 1; i++)
                                        {
                                            EAttach = M_MAIL_ATTACHMENTS_PATH.Split(Convert.ToChar(","))[i];
                                            objMail.Attachments.Add(new Attachment(EAttach));
                                        }
                                    }
                                    smtpserver = new SmtpClient(M_MAIL_SERVER, 587);
                                    NetworkCredential basicCredentials = new NetworkCredential(M_SEND_USERNAME, M_SEND_PASSWORD);
                                    if (string.Compare(M_SEND_USERNAME, "gmail.com") > 0)
                                    {
                                        smtpserver.EnableSsl = true;
                                    }
                                    smtpserver.UseDefaultCredentials = true;
                                    smtpserver.Credentials = basicCredentials;
                                    object userState = objMail;
                                    smtpserver.SendCompleted += SendCompletedCallback;
                                    smtpserver.SendAsync(objMail, userState);
                                }
                            }
                        }
                    }
                }
                objMail = null;
                if (!string.IsNullOrEmpty(EmailId))
                {
                    return "All Data Saved Successfully and Mail has been sent.";
                }
                else
                {
                    return "Posted Succesfully.";
                }

            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                return "All Data Saved Successfully.Due to some problem Mail has not been sent";
                return functionReturnValue;
            }
            return functionReturnValue;
        }

        public string SendExternalMailEcomm(string CustRegFK, string strDocId, string Company_Name = "", string Emailid = "", string Userid = "")
        {
            string functionReturnValue = null;

            try
            {
                SmtpClient smtpserver = new SmtpClient();
                MailMessage objMail = new MailMessage();
                WorkFlow objWF = new WorkFlow();
                string EAttach = null;
                string dsMail = null;
                Int32 intCnt = default(Int32);
                string strbody = null;
                string M_MAIL_ATTACHMENTS_PATH = null;
                bool Result = false;
                DataSet dsMsg = new DataSet();
                long lngPkValue = 0;
                string strMsgSub = null;
                DataSet dsDoc = null;
                long lngdocPk = 0;
                dsDoc = FetchDocument(strDocId);
                M_AnnId = strDocId;
                if (dsDoc.Tables[0].Rows.Count > 0)
                {
                    lngdocPk = Convert.ToInt64(dsDoc.Tables[0].Rows[0]["DOCUMENT_MST_PK"]);
                    dsMsg = GetMessageInfo(lngdocPk, -1);
                }
                string strMsgBody = Convert.ToString(dsMsg.Tables[0].Rows[0]["Msg_Body"]);
                strMsgSub = Convert.ToString(dsMsg.Tables[0].Rows[0]["MSG_SUBJECT"]);
                strMsgBody = GetMessageBody(1, strDocId, strMsgBody, System.DateTime.Now, 0,"" , CustRegFK, Userid);
                Int16 i = default(Int16);
                Int16 j = default(Int16);
                Create_By = HttpContext.Current.Session["USER_PK"].ToString();
                if (!string.IsNullOrEmpty(Emailid))
                {
                    objMail = new MailMessage();
                    objMail.From = new MailAddress(M_SEND_USERNAME, M_SEND_NAME);
                    objMail.To.Add(Emailid);

                    objMail.Subject = strMsgSub;
                    objMail.IsBodyHtml = true;
                    strbody = " Dear  " + Company_Name ;
                    strbody += "   " ;
                    strbody += " " + strMsgBody + " " ;
                    strbody += "   " ;
                    strbody += " This is an auto generated e-Mail. Please do not reply to this e-Mail-ID." ;
                    strbody += " " ;
                    strbody = strMsgBody.Replace("<Sender>", M_SEND_NAME);
                    strbody = ConvertHTML(strbody);
                    objMail.Body = strbody;
                    objMail.BodyEncoding = Encoding.UTF8;
                    smtpserver = new SmtpClient(M_MAIL_SERVER, 587);
                    NetworkCredential basicCredentials = new NetworkCredential(M_SEND_USERNAME, M_SEND_PASSWORD);
                    if (string.Compare(M_SEND_USERNAME, "gmail.com") > 0)
                    {
                        smtpserver.EnableSsl = true;
                    }
                    smtpserver.UseDefaultCredentials = true;
                    smtpserver.Credentials = basicCredentials;
                    object userState = objMail;
                    smtpserver.SendCompleted += SendCompletedCallback;
                    smtpserver.SendAsync(objMail, userState);
                }
                objMail = null;
                if (!string.IsNullOrEmpty(Emailid))
                {
                    return "All Data Saved Successfully and Mail has been sent.";
                }
                else
                {
                    return "Posted Succesfully.";
                }

            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                return "All Data Saved Successfully.Due to some problem Mail has not been sent";
                return functionReturnValue;
            }
            return functionReturnValue;
        }
        #endregion

        private static void SendCompletedCallback(object sender, AsyncCompletedEventArgs e)
        {
            // Get the unique identifier for this asynchronous operation. 

            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = default(OracleTransaction);
            int RetVal = 0;
            string MailDesc = null;
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            objWK.MyCommand.Transaction = TRAN;
            objWK.MyCommand.Parameters.Clear();
            try
            {
                MailMessage mail = (MailMessage)e.UserState;
                string str = null;
                int MailStatus = 0;
                if (e.Error != null)
                {
                    str = e.Error.ToString();
                    MailStatus = 2;
                }
                else
                {
                    str = "Mail Send Successfully";
                    MailStatus = 1;
                }
                if (string.Compare(M_Name, "gmail.com") > 0)
                {
                    System.Net.WebClient objClient = new System.Net.WebClient();
                    XmlNodeList nodelist = null;
                    XmlNode node = null;
                    string response = null;
                    XmlDocument xmlDoc = new XmlDocument();
                    int mailCount = 0;
                    int tempCounter = 0;
                    string emailFrom = null;
                    string emailMessages = null;
                    objClient.Credentials = new System.Net.NetworkCredential(M_Name, M_Password);
                    response = Encoding.UTF8.GetString(objClient.DownloadData("https://mail.google.com/mail/feed/atom"));
                    response = response.Replace("<feed version=\"0.3\" xmlns=\"http://purl.org/atom/ns#\">", "<feed>");

                    xmlDoc.LoadXml(response);
                    node = xmlDoc.SelectSingleNode("/feed/fullcount");
                    mailCount = Convert.ToInt32(node.InnerText);
                    //Get the number of unread emails

                    if (mailCount > 0)
                    {
                        nodelist = xmlDoc.SelectNodes("/feed/entry");
                        node = xmlDoc.SelectSingleNode("title");
                        tempCounter = 0;
                        foreach (XmlNode node_loopVariable in nodelist)
                        {
                            node = node_loopVariable;
                            emailMessages = node.ChildNodes[1].InnerText;
                            emailFrom = node.ChildNodes[6].ChildNodes[0].InnerText;
                            tempCounter = tempCounter + 1;
                            if (tempCounter <= 10)
                            {
                                if (string.Compare(emailMessages.ToUpper(), mail.To[0].Address.ToUpper()) > 0)
                                {
                                    str = emailMessages;
                                    MailStatus = 2;
                                    break; // TODO: might not be correct. Was : Exit For
                                }
                            }
                        }
                    }
                }

                if (str == "Mail Send Successfully")
                {
                    MailDesc = str;
                }
                else if (string.Compare(str.ToUpper(), "MAILBOX NOT FOUND") > 0 | string.Compare(str.ToUpper(), "INVALID MAILBOX") > 0 | string.Compare(str.ToUpper(), "USER UNKNOWN") > 0 | string.Compare(str.ToUpper(), "NOT OUR CUSTOMER") > 0 | string.Compare(str.ToUpper(), "MAILBOX UNAVAILABLE") > 0)
                {
                    MailDesc = "Invalid Email ID";
                }
                else if (string.Compare(str.ToUpper(), "MAILBOX FULL") > 0 | string.Compare(str.ToUpper(), "QUOTA EXCEEDED") > 0)
                {
                    MailDesc = "Mail Box Full";
                }
                else if (string.Compare(str.ToUpper(), "BLACKLIST FILTERS") > 0)
                {
                    MailDesc = "Email is Blocked by receiver";
                }
                else if (string.Compare(str.ToUpper(), "CONTENT FILTERS") > 0)
                {
                    MailDesc = "Email looks like a spam and is blocked by receiver";
                }
                else if (string.Compare(str.ToUpper(), "5.5.1 AUTHENTICATION REQUIRED") > 0)
                {
                    MailDesc = "Unable to Connect to SMTP Server";
                }
                else
                {
                    MailDesc = "Invalid Email ID";
                }

                var _with7 = objWK.MyCommand;
                _with7.CommandType = CommandType.StoredProcedure;
                _with7.CommandText = objWK.MyUserName + ".MAIL_SEND_STATUS_TBL_PKG.MAIL_SEND_STATUS_INS";
                _with7.Parameters.Add("MAIL_TYPE_IN", 1).Direction = ParameterDirection.Input;
                _with7.Parameters.Add("DOC_TYPE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                _with7.Parameters.Add("DOC_REF_NR_IN", M_AnnId).Direction = ParameterDirection.Input;
                _with7.Parameters.Add("MAIL_SUBJECT_IN", mail.Subject).Direction = ParameterDirection.Input;
                _with7.Parameters.Add("SEND_FROM_IN", mail.From.Address).Direction = ParameterDirection.Input;
                _with7.Parameters.Add("SEND_TO_IN", mail.To[0].Address).Direction = ParameterDirection.Input;
                _with7.Parameters.Add("MAIL_STATUS_IN", MailStatus).Direction = ParameterDirection.Input;
                _with7.Parameters.Add("MAIL_DESC_IN", MailDesc).Direction = ParameterDirection.Input;
                _with7.Parameters.Add("MAIL_DESC_DTL_IN", str).Direction = ParameterDirection.Input;
                _with7.Parameters.Add("CREATED_BY_FK_IN", Create_By).Direction = ParameterDirection.Input;
                _with7.Parameters.Add("MAIL_BODY_IN", mail.Body).Direction = ParameterDirection.Input;
                if ((FileName != null))
                {
                    FileName = FileName;
                }
                else
                {
                    FileName = "";
                }
                _with7.Parameters.Add("MAIL_FILE_IN", FileName).Direction = ParameterDirection.Input;
                _with7.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                RetVal = _with7.ExecuteNonQuery();
                TRAN.Commit();
                FileName = "";
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                TRAN.Rollback();
                throw ex;
            }
            finally
            {
                objWK.MyConnection.Close();
            }

        }
        #region " SendMessageProfitability "
        public string SendMessageProfitability(long lngCreatedBy, long lngPkValue, string strDocId, DataSet dsDetails, string IncTeaMoney, string IncTeaCur, string Status)
        {
            string functionReturnValue = null;
            DataSet dsMsg = new DataSet();
            DataSet dsDoc = null;
            DataSet dsUser = new DataSet();
            DataSet dsSender = new DataSet();
            string strUsrName = null;
            string strLocName = null;
            string strMsgSub = null;
            string strMsgHeader = null;
            string strMsgFooter = null;
            string strMsgBody = null;
            DataRow DR = null;
            DataSet dsUserFks = null;
            long lngdocPk = 0;
            string strSenderLocName = null;
            string strSenderUsrName = null;
            string strPageURL = null;
            long lngUserFk = 0;
            int intSenderCnt = 0;

            try
            {
                dsDoc = FetchDocument(strDocId);
                if (dsDoc.Tables[0].Rows.Count > 0)
                {
                    lngdocPk = Convert.ToInt64(dsDoc.Tables[0].Rows[0]["DOCUMENT_MST_PK"]);
                    dsMsg = GetMessageInfo(lngdocPk, -1);
                    if (dsMsg.Tables[0].Rows.Count < 1)
                    {
                        DR = dsMsg.Tables[0].NewRow();
                        dsMsg.Tables[0].Rows.Add(DR);
                    }

                    dsUserFks = GetUserInfo(lngdocPk, Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), 0);
                    if (dsUserFks.Tables[0].Rows.Count > 0)
                    {
                        for (intSenderCnt = 0; intSenderCnt <= dsUserFks.Tables[0].Columns.Count - 1; intSenderCnt++)
                        {
                            if (!(string.IsNullOrEmpty(dsUserFks.Tables[0].Rows[0][intSenderCnt].ToString())))
                            {
                                if ((Convert.ToInt32(dsUserFks.Tables[0].Rows[0][intSenderCnt])) > 0)
                                {
                                    dsUser = FetchUserDetails(Convert.ToInt64(dsUserFks.Tables[0].Rows[0][intSenderCnt]));
                                    strUsrName = Convert.ToString(dsUser.Tables[0].Rows[0]["USER_NAME"]);
                                    strLocName = Convert.ToString(dsUser.Tables[0].Rows[0]["LOCATION_NAME"]);

                                    if (dsMsg.Tables[0].Rows.Count > 0)
                                    {
                                        var _with8 = dsMsg.Tables[0].Rows[0];
                                        _with8["Sender_Fk"] = lngCreatedBy;
                                        dsSender = FetchUserDetails(lngCreatedBy);
                                        strSenderUsrName = Convert.ToString(dsSender.Tables[0].Rows[0]["USER_NAME"]);
                                        strSenderLocName = Convert.ToString(dsSender.Tables[0].Rows[0]["LOCATION_NAME"]);

                                        _with8["Receiver_Fk"] = Convert.ToInt64(dsUserFks.Tables[0].Rows[0][intSenderCnt]);
                                        _with8["Msg_Read"] = 0;
                                        _with8["Followup_Flag"] = 0;
                                        _with8["Have_Attachment"] = 1;

                                        strMsgBody = Convert.ToString(_with8["Msg_Body"]);
                                        strMsgBody = strMsgBody.Replace("<<", "<");
                                        strMsgBody = strMsgBody.Replace("&lt;&lt;", "<");
                                        strMsgBody = strMsgBody.Replace(">>", ">");
                                        strMsgBody = strMsgBody.Replace("&gt;&gt;", ">");
                                        strMsgBody = strMsgBody.Replace("<SENDER>", (strSenderUsrName + ", " + strSenderLocName));
                                        strMsgBody = strMsgBody.Replace("<RECEIVER>", strUsrName);
                                        strMsgBody = strMsgBody.Replace("<CURRENT DATE>", Convert.ToString(DateTime.Now.Date));
                                        strMsgBody = strMsgBody.Replace("<CUSTOMER>", dsDetails.Tables[0].Rows[0]["CUSTOMER_NAME"].ToString());
                                        strMsgBody = strMsgBody.Replace("<REFERENCE DOC TYPE>", dsDetails.Tables[0].Rows[0]["DOC_REF_TYPE"].ToString());
                                        strMsgBody = strMsgBody.Replace("<REFERENCE DOC NR>", dsDetails.Tables[0].Rows[0]["DOC_REF_NR"].ToString());
                                        strMsgBody = strMsgBody.Replace("<STATUS>", Status);
                                        strMsgBody = strMsgBody.Replace("<DOCUMENT DATE>", dsDetails.Tables[0].Rows[0]["DOC_REF_DATE"].ToString());
                                        strMsgBody = strMsgBody.Replace("<INCENTIVE TEA MONEY>", IncTeaMoney);
                                        strMsgBody = strMsgBody.Replace("<CURRENCY>", IncTeaCur);

                                        _with8["Msg_Body"] = strMsgBody;

                                        strMsgSub = Convert.ToString(_with8["MSG_SUBJECT"]);
                                        strMsgSub = strMsgSub.Replace("<<", "<");
                                        strMsgSub = strMsgSub.Replace("&lt;&lt;", "<");
                                        strMsgSub = strMsgSub.Replace(">>", ">");
                                        strMsgSub = strMsgSub.Replace("&gt;&gt;", ">");
                                        strMsgSub = strMsgSub.Replace("<SENDER>", (strSenderUsrName + ", " + strSenderLocName));
                                        strMsgSub = strMsgSub.Replace("<RECEIVER>", strUsrName);
                                        strMsgSub = strMsgSub.Replace("<CURRENT DATE>", Convert.ToString(DateTime.Now.Date));
                                        strMsgSub = strMsgSub.Replace("<CUSTOMER>", dsDetails.Tables[0].Rows[0]["CUSTOMER_NAME"].ToString());
                                        strMsgSub = strMsgSub.Replace("<REFERENCE DOC TYPE>", dsDetails.Tables[0].Rows[0]["DOC_REF_TYPE"].ToString());
                                        strMsgSub = strMsgSub.Replace("<REFERENCE DOC NR>", dsDetails.Tables[0].Rows[0]["DOC_REF_NR"].ToString());
                                        strMsgSub = strMsgSub.Replace("<STATUS>", Status);
                                        strMsgSub = strMsgSub.Replace("<DOCUMENT DATE>", dsDetails.Tables[0].Rows[0]["DOC_REF_DATE"].ToString());
                                        strMsgSub = strMsgSub.Replace("<INCENTIVE TEA MONEY>", IncTeaMoney);
                                        strMsgSub = strMsgSub.Replace("<CURRENCY>", IncTeaCur);
                                        _with8["Msg_Subject"] = strMsgSub;

                                        _with8["Read_Receipt"] = 0;
                                        _with8["Document_Mst_Fk"] = lngdocPk;
                                        _with8["User_Message_Folders_Fk"] = 1;
                                        _with8["Msg_Received_Dt"] = DateTime.Now;
                                    }
                                    else
                                    {
                                        return "Please define WorkFlow for the particular document";
                                    }
                                    if (dsMsg.Tables[1].Rows.Count < 1)
                                    {
                                        DR = dsMsg.Tables[1].NewRow();
                                        dsMsg.Tables[1].Rows.Add(DR);
                                    }
                                    if (dsMsg.Tables[1].Rows.Count > 0)
                                    {
                                        var _with9 = dsMsg.Tables[1].Rows[0];
                                        strPageURL = dsDetails.Tables[0].Rows[0]["URL"].ToString();

                                        if (string.IsNullOrEmpty(strPageURL))
                                        {
                                            _with9["URL_PAGE"] = DBNull.Value;
                                            _with9["ATTACHMENT_DATA"] = DBNull.Value;
                                        }
                                        else
                                        {
                                            _with9["URL_PAGE"] = strPageURL;
                                            _with9["ATTACHMENT_DATA"] = strPageURL;
                                        }
                                    }

                                    if ((SaveUnApprovedTDR(dsMsg, lngCreatedBy) == -1))
                                    {
                                        return "<br>Due to some problem Mail has not been sent</br>";
                                        return functionReturnValue;
                                    }
                                }
                            }
                        }
                        return "<br>All data saved Successfully and Mail has been sent</br>";
                    }
                    else
                    {
                        return "<br>Mail has not been sent.Document Id not created.</br>";
                    }
                }
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return functionReturnValue;
        }
        #endregion

        #region "GetMessageBody "
        public string GetMessageBody(long lngPkValue, string strDocId, string strMsg, System.DateTime saledate, long strpkvalue = 0, string TypeFlag = "", string CustRegFK = "", string Userid = "")
        {
            string MsgBody = null;
            DataSet objDS = null;
            try
            {
                while (strMsg.Contains("<<"))
                {
                    strMsg = strMsg.Replace("<<", "<");
                }
                while (strMsg.Contains("&lt;&lt;"))
                {
                    strMsg = strMsg.Replace("&lt;&lt;", "<");
                }
                while (strMsg.Contains(">>"))
                {
                    strMsg = strMsg.Replace(">>", ">");
                }
                while (strMsg.Contains("&gt;&gt;"))
                {
                    strMsg = strMsg.Replace("&gt;&gt;", ">");
                }
                if (strDocId == "ALERT SETUP" | strDocId == "ALERT SETUP")
                {
                    strMsg = strMsg.Replace("<ACTIVITY>", CurrentActivityWF);
                    strMsg = strMsg.Replace("<NEXT ACTIVITY>", NextActivityWF);
                    strMsg = strMsg.Replace("<REFERENCE NR.>", TaskDocRefNr);
                    strMsg = strMsg.Replace("<REFERENCE DT.>", TaskDocRefDt);
                    strMsg = strMsg.Replace("<STATUS>", TaskDocRefStatus);
                    strMsg = strMsg.Replace("<DEADLINE TIME>", TaskDeadLineDt);
                    strMsg = strMsg.Replace("<OVERDUE PERIOD>", TaskOverDueDt);
                    //strMsg = strMsg.Replace("<SENDER>", CStr(Session("USER_NAME")))
                    //strMsg = strMsg.Replace("<RECEIVER>", CStr(_dr["")))
                    strMsg = strMsg.Replace("<DATE & TIME>", DateTime.Now.ToString("dd/MM/yyyy HH:mm"));
                    DocRefNr = TaskDocRefNr;
                }
                else if (strDocId.ToUpper() == "PROFIT MARGIN APPROVAL REQUEST" | strDocId.ToUpper() == "PROFIT MARGIN APPROVAL")
                {
                    objDS = FetchDocDetailsForProfitMargin(lngPkValue);
                    if (objDS.Tables[0].Rows.Count > 0)
                    {
                        DataRow _dr = null;
                        _dr = objDS.Tables[0].Rows[0];
                        strMsg = strMsg.Replace("<CARRIER>", Convert.ToString(_dr["CARRIER_NAME"]));
                        strMsg = strMsg.Replace("<CUSTOMER>", Convert.ToString(_dr["CUSTOMER_NAME"]));
                        strMsg = strMsg.Replace("<DATE AND TIME>", Convert.ToString(_dr["DATE_TIME"]));
                        strMsg = strMsg.Replace("<DEFINED PROFIT>", Convert.ToString(_dr["DEFINED_PROFIT"]));
                        strMsg = strMsg.Replace("<POD/AOD>", Convert.ToString(_dr["POL_NAME"]));
                        strMsg = strMsg.Replace("<POL/AOO>", Convert.ToString(_dr["POD_NAME"]));
                        strMsg = strMsg.Replace("<REFERENCE NR>", Convert.ToString(_dr["REF_NO"]));
                        strMsg = strMsg.Replace("<REFERENCE TYPE>", Convert.ToString(_dr["REF_TYPE"]));
                        strMsg = strMsg.Replace("<SENDER>", Convert.ToString(HttpContext.Current.Session["USER_NAME"]));
                        strMsg = strMsg.Replace("<SHIPMENT DT>", Convert.ToString(_dr["SHIPMENT_DT"]));
                        if (!string.IsNullOrEmpty(_dr["SHIPMENT_PROFIT"].ToString()))
                        {
                            strMsg = strMsg.Replace("<SHIPMENT PROFIT>", Convert.ToString(_dr["SHIPMENT_PROFIT"]));
                        }
                        DocRefNr = Convert.ToString(objDS.Tables[0].Rows[0]["REF_NO"]);
                    }

                }
                else if (strDocId == "SL SPOT RATE REQUEST" | strDocId == "SL SPOT RATE APPROVAL")
                {
                    objDS = FetchRFQ(lngPkValue);
                    if (objDS.Tables[0].Rows.Count > 0)
                    {
                        strMsg = strMsg.Replace("<RFQ NR>", Convert.ToString(objDS.Tables[0].Rows[0]["RFQ_REF_NO"]));
                        strMsg = strMsg.Replace("<CUSTOMER>", Convert.ToString(objDS.Tables[0].Rows[0]["CUSTOMER_NAME"]));
                        strMsg = strMsg.Replace("<RFQ DATE>", Convert.ToString(objDS.Tables[0].Rows[0]["RFQ_DATE"]));
                        strMsg = strMsg.Replace("<CONTRACT NR>", Convert.ToString(objDS.Tables[0].Rows[0]["CONTRACT_NO"]));
                        strMsg = strMsg.Replace("<SL NAME>", Convert.ToString(objDS.Tables[0].Rows[0]["OPERATOR_NAME"]));
                        strMsg = strMsg.Replace("<CARGO TYPE>", Convert.ToString(objDS.Tables[0].Rows[0]["CARGO_TYPE"]));
                        strMsg = strMsg.Replace("<VALID FROM>", Convert.ToString(objDS.Tables[0].Rows[0]["VALID_FROM"]));
                        strMsg = strMsg.Replace("<VALID TO>", Convert.ToString(objDS.Tables[0].Rows[0]["VALID_TO"]));
                        strMsg = strMsg.Replace("<STATUS>", Convert.ToString(objDS.Tables[0].Rows[0]["STATUS"]));
                        DocRefNr = Convert.ToString(objDS.Tables[0].Rows[0]["RFQ_REF_NO"]);
                    }

                }
                else if (strDocId == "AIR Spot Rate Request" | strDocId == "AIR Spot Rate Approval")
                {
                    objDS = FetchAIRRFQ(lngPkValue);
                    if (objDS.Tables[0].Rows.Count > 0)
                    {
                        strMsg = strMsg.Replace("<RFQ NR>", Convert.ToString(objDS.Tables[0].Rows[0]["RFQ_REF_NO"]));
                        DocRefNr = Convert.ToString(objDS.Tables[0].Rows[0]["RFQ_REF_NO"]);
                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["CUSTOMER_NAME"].ToString()))
                        {
                            strMsg = strMsg.Replace("<CUSTOMER>", Convert.ToString(objDS.Tables[0].Rows[0]["CUSTOMER_NAME"]));
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["RFQ_DATE"].ToString()))
                        {
                            strMsg = strMsg.Replace("<RFQ DATE>", Convert.ToString(objDS.Tables[0].Rows[0]["RFQ_DATE"]));
                        }
                        strMsg = strMsg.Replace("<CONTRACT NR>", Convert.ToString(objDS.Tables[0].Rows[0]["CONTRACT_NO"]));

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["AIRLINE_NAME"].ToString()))
                        {
                            strMsg = strMsg.Replace("<AIRLINE NAME>", Convert.ToString(objDS.Tables[0].Rows[0]["AIRLINE_NAME"]));
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["VALID_FROM"].ToString()))
                        {
                            strMsg = strMsg.Replace("<VALID FROM>", Convert.ToString(objDS.Tables[0].Rows[0]["VALID_FROM"]));
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["VALID_TO"].ToString()))
                        {
                            strMsg = strMsg.Replace("<VALID TO>", Convert.ToString(objDS.Tables[0].Rows[0]["VALID_TO"]));
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["STATUS"].ToString()))
                        {
                            strMsg = strMsg.Replace("<STATUS>", Convert.ToString(objDS.Tables[0].Rows[0]["STATUS"]));
                        }

                    }

                }
                else if (strDocId == "SL Tariff Request" | strDocId == "SL Tariff Approval")
                {
                    objDS = FetchSeaTariff(lngPkValue);
                    if (objDS.Tables[0].Rows.Count > 0)
                    {
                        strMsg = strMsg.Replace("<TARIFF NR>", Convert.ToString(objDS.Tables[0].Rows[0]["TARIFF_REF_NO"]));
                        DocRefNr = Convert.ToString(objDS.Tables[0].Rows[0]["TARIFF_REF_NO"]);
                        //If Not IsDBNull(objDS.Tables(0).Rows(0)["CUSTOMER_NAME")) Then
                        //    strMsg = strMsg.Replace("<CUSTOMER>", CStr(objDS.Tables(0).Rows(0)["CUSTOMER_NAME")))
                        //End If
                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["TARIFF_DATE"].ToString()))
                        {
                            strMsg = strMsg.Replace("<TARIFF DATE>", Convert.ToString(objDS.Tables[0].Rows[0]["TARIFF_DATE"]));
                        }
                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["CONTRACT_NO"].ToString()))
                        {
                            strMsg = strMsg.Replace("<CONTRACT NR>", Convert.ToString(objDS.Tables[0].Rows[0]["CONTRACT_NO"]));
                        }
                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["OPERATOR_NAME"].ToString()))
                        {
                            strMsg = strMsg.Replace("<SL NAME>", Convert.ToString(objDS.Tables[0].Rows[0]["OPERATOR_NAME"]));
                        }
                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["CARGO_TYPE"].ToString()))
                        {
                            strMsg = strMsg.Replace("<CARGO TYPE>", Convert.ToString(objDS.Tables[0].Rows[0]["CARGO_TYPE"]));
                        }
                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["VALID_FROM"].ToString()))
                        {
                            strMsg = strMsg.Replace("<VALID FROM>", Convert.ToString(objDS.Tables[0].Rows[0]["VALID_FROM"]));
                        }
                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["VALID_TO"].ToString()))
                        {
                            strMsg = strMsg.Replace("<VALID TO>", Convert.ToString(objDS.Tables[0].Rows[0]["VALID_TO"]));
                        }
                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["STATUS"].ToString()))
                        {
                            strMsg = strMsg.Replace("<STATUS>", Convert.ToString(objDS.Tables[0].Rows[0]["STATUS"]));
                        }
                    }
                }
                else if (strDocId == "AIR Tariff Request" | strDocId == "AIR Tariff Approval")
                {
                    objDS = FetchAIRTariff(lngPkValue);
                    if (objDS.Tables[0].Rows.Count > 0)
                    {
                        strMsg = strMsg.Replace("<TARIFF NR>", Convert.ToString(objDS.Tables[0].Rows[0]["TARIFF_REF_NO"]));
                        DocRefNr = Convert.ToString(objDS.Tables[0].Rows[0]["TARIFF_REF_NO"]);
                        //If Not IsDBNull(objDS.Tables(0).Rows(0)["CUSTOMER_NAME")) Then
                        //    strMsg = strMsg.Replace("<CUSTOMER>", CStr(objDS.Tables(0).Rows(0)["CUSTOMER_NAME")))
                        //End If

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["TARIFF_DATE"].ToString()))
                        {
                            strMsg = strMsg.Replace("<TARIFF DATE>", Convert.ToString(objDS.Tables[0].Rows[0]["TARIFF_DATE"]));
                        }
                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["CONTRACT_NO"].ToString()))
                        {
                            strMsg = strMsg.Replace("<CONTRACT NR>", Convert.ToString(objDS.Tables[0].Rows[0]["CONTRACT_NO"]));
                        }
                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["AIRLINE_NAME"].ToString()))
                        {
                            strMsg = strMsg.Replace("<AIRLINE NAME>", Convert.ToString(objDS.Tables[0].Rows[0]["AIRLINE_NAME"]));
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["VALID_FROM"].ToString()))
                        {
                            strMsg = strMsg.Replace("<VALID FROM>", Convert.ToString(objDS.Tables[0].Rows[0]["VALID_FROM"]));
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["VALID_TO"].ToString()))
                        {
                            strMsg = strMsg.Replace("<VALID TO>", Convert.ToString(objDS.Tables[0].Rows[0]["VALID_TO"]));
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["STATUS"].ToString()))
                        {
                            strMsg = strMsg.Replace("<STATUS>", Convert.ToString(objDS.Tables[0].Rows[0]["STATUS"]));
                        }

                    }
                }
                else if (strDocId == "SL Contract Request" | strDocId == "SL Contract  Approval")
                {
                    objDS = FetchSLContract(lngPkValue);
                    if (objDS.Tables[0].Rows.Count > 0)
                    {
                        strMsg = strMsg.Replace("<CONTRACT NR>", Convert.ToString(objDS.Tables[0].Rows[0]["CONTRACT_NO"]));
                        strMsg = strMsg.Replace("<SHIPPING LINE NAME>", Convert.ToString(objDS.Tables[0].Rows[0]["OPERATOR_NAME"]));
                        strMsg = strMsg.Replace("<CONTRACT DATE>", Convert.ToString(objDS.Tables[0].Rows[0]["CONTRACT_DATE"]));
                        strMsg = strMsg.Replace("<CONTRACT NR>", Convert.ToString(objDS.Tables[0].Rows[0]["CONTRACT_NO"]));
                        strMsg = strMsg.Replace("<CARGO TYPE>", Convert.ToString(objDS.Tables[0].Rows[0]["CARGO_TYPE"]));
                        strMsg = strMsg.Replace("<VALID FROM>", Convert.ToString(objDS.Tables[0].Rows[0]["VALID_FROM"]));
                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["VALID_TO"].ToString()))
                        {
                            strMsg = strMsg.Replace("<VALID TO>", Convert.ToString(objDS.Tables[0].Rows[0]["VALID_TO"]));
                        }
                        else
                        {
                            strMsg = strMsg.Replace("<VALID TO>", "");
                        }
                        strMsg = strMsg.Replace("<POL>", Convert.ToString(objDS.Tables[0].Rows[0]["POL"]));
                        strMsg = strMsg.Replace("<POD>", Convert.ToString(objDS.Tables[0].Rows[0]["POD"]));
                        strMsg = strMsg.Replace("<STATUS>", Convert.ToString(objDS.Tables[0].Rows[0]["STATUS"]));
                        DocRefNr = Convert.ToString(objDS.Tables[0].Rows[0]["CONTRACT_NO"]);
                    }

                    ///added by subhransu for pts jun-29, on 03/07/2011
                }
                else if (strDocId == "Airline Contract Request" | strDocId == "Airline Contract Approval")
                {

                    objDS = FetchAirLineContract(lngPkValue);

                    if (objDS.Tables[0].Rows.Count > 0)
                    {
                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["CONTRACT_NO"].ToString()))
                        {
                            strMsg = strMsg.Replace("<CONTRACT NR>", Convert.ToString(objDS.Tables[0].Rows[0]["CONTRACT_NO"]));
                            DocRefNr = Convert.ToString(objDS.Tables[0].Rows[0]["CONTRACT_NO"]);
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["CONTRACT_DATE"].ToString()))
                        {
                            strMsg = strMsg.Replace("<CONTRACT DATE>", Convert.ToString(objDS.Tables[0].Rows[0]["CONTRACT_DATE"]));
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["AIRLINE_NAME"].ToString()))
                        {
                            strMsg = strMsg.Replace("<AIRLINE NAME>", Convert.ToString(objDS.Tables[0].Rows[0]["AIRLINE_NAME"]));
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["VALID_FROM"].ToString()))
                        {
                            strMsg = strMsg.Replace("<VALID FROM>", Convert.ToString(objDS.Tables[0].Rows[0]["VALID_FROM"]));
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["VALID_TO"].ToString()))
                        {
                            strMsg = strMsg.Replace("<VALID TO>", Convert.ToString(objDS.Tables[0].Rows[0]["VALID_TO"]));
                        }
                        else
                        {
                            strMsg = strMsg.Replace("<VALID TO>", "");
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["POL"].ToString()))
                        {
                            strMsg = strMsg.Replace("<POL>", Convert.ToString(objDS.Tables[0].Rows[0]["POL"]));
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["POD"].ToString()))
                        {
                            strMsg = strMsg.Replace("<POD>", Convert.ToString(objDS.Tables[0].Rows[0]["POD"]));
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["STATUS"].ToString()))
                        {
                            strMsg = strMsg.Replace("<STATUS>", Convert.ToString(objDS.Tables[0].Rows[0]["STATUS"]));
                        }

                    }

                    ///added by subhransu for pts jun-29, on 03/07/2011
                }
                else if (strDocId == "Warehouse Contract Request" | strDocId == "Warehouse Contract Approval")
                {
                    objDS = FetchWareHouseContract(lngPkValue);

                    if (objDS.Tables[0].Rows.Count > 0)
                    {
                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["CONTRACT_NO"].ToString()))
                        {
                            strMsg = strMsg.Replace("<CONTRACT NR>", Convert.ToString(objDS.Tables[0].Rows[0]["CONTRACT_NO"]));
                            DocRefNr = Convert.ToString(objDS.Tables[0].Rows[0]["CONTRACT_NO"]);
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["CONTRACT_DATE"].ToString()))
                        {
                            strMsg = strMsg.Replace("<CONTRACT DATE>", Convert.ToString(objDS.Tables[0].Rows[0]["CONTRACT_DATE"]));
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["VENDOR_NAME"].ToString()))
                        {
                            strMsg = strMsg.Replace("<WAREHOUSE NAME>", Convert.ToString(objDS.Tables[0].Rows[0]["VENDOR_NAME"]));
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["VALID_FROM"].ToString()))
                        {
                            strMsg = strMsg.Replace("<VALID FROM>", Convert.ToString(objDS.Tables[0].Rows[0]["VALID_FROM"]));
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["VALID_TO"].ToString()))
                        {
                            strMsg = strMsg.Replace("<VALID TO>", Convert.ToString(objDS.Tables[0].Rows[0]["VALID_TO"]));
                        }
                        else
                        {
                            strMsg = strMsg.Replace("<VALID TO>", "");
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["BIZ_TYPE"].ToString()))
                        {
                            strMsg = strMsg.Replace("<BIZ TYPE>", Convert.ToString(objDS.Tables[0].Rows[0]["BIZ_TYPE"]));
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["CARGO_TYPE"].ToString()))
                        {
                            strMsg = strMsg.Replace("<CARGO TYPE>", Convert.ToString(objDS.Tables[0].Rows[0]["CARGO_TYPE"]));
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["STATUS"].ToString()))
                        {
                            strMsg = strMsg.Replace("<STATUS>", Convert.ToString(objDS.Tables[0].Rows[0]["STATUS"]));
                        }
                    }


                    ///added by subhransu for pts jun-29, on 03/07/2011
                }
                else if (strDocId == "Transport Contract Air Request" | strDocId == "Transport Contract Air Approval")
                {

                    objDS = FetchTransportContractAir(lngPkValue);
                    if (objDS.Tables[0].Rows.Count > 0)
                    {
                        strMsg = strMsg.Replace("<CONTRACT NR>", Convert.ToString(objDS.Tables[0].Rows[0]["CONTRACT_NO"]));
                        DocRefNr = Convert.ToString(objDS.Tables[0].Rows[0]["CONTRACT_NO"]);
                        strMsg = strMsg.Replace("<CONTRACT DATE>", Convert.ToString(objDS.Tables[0].Rows[0]["CONTRACT_DATE"]));
                        strMsg = strMsg.Replace("<WAREHOUSE NAME>", Convert.ToString(objDS.Tables[0].Rows[0]["VENDOR_NAME"]));
                        strMsg = strMsg.Replace("<VALID FROM>", Convert.ToString(objDS.Tables[0].Rows[0]["VALID_FROM"]));
                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["VALID_TO"].ToString()))
                        {
                            strMsg = strMsg.Replace("<VALID TO>", Convert.ToString(objDS.Tables[0].Rows[0]["VALID_TO"]));
                        }
                        strMsg = strMsg.Replace("<BIZ TYPE>", Convert.ToString(objDS.Tables[0].Rows[0]["BIZ_TYPE"]));
                        strMsg = strMsg.Replace("<CARGO TYPE>", Convert.ToString(objDS.Tables[0].Rows[0]["CARGO_TYPE"]));
                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["STATUS"].ToString()))
                        {
                            strMsg = strMsg.Replace("<STATUS>", Convert.ToString(objDS.Tables[0].Rows[0]["STATUS"]));
                        }
                    }


                    ///added by subhransu for pts jun-29, on 03/07/2011
                }
                else if (strDocId == "Transport Contract Request" | strDocId == "Transport Contract Approval")
                {

                    objDS = FetchTransPortContract(lngPkValue);
                    if (objDS.Tables[0].Rows.Count > 0)
                    {
                        strMsg = strMsg.Replace("<CONTRACT NR>", Convert.ToString(objDS.Tables[0].Rows[0]["CONTRACT_NO"]));
                        DocRefNr = Convert.ToString(objDS.Tables[0].Rows[0]["CONTRACT_NO"]);
                        strMsg = strMsg.Replace("<CONTRACT DATE>", Convert.ToString(objDS.Tables[0].Rows[0]["CONTRACT_DATE"]));
                        strMsg = strMsg.Replace("<TRANSPORTER NAME>", Convert.ToString(objDS.Tables[0].Rows[0]["VENDOR_NAME"]));
                        strMsg = strMsg.Replace("<WAREHOUSE NAME>", Convert.ToString(objDS.Tables[0].Rows[0]["VENDOR_NAME"]));
                        strMsg = strMsg.Replace("<VALID FROM>", Convert.ToString(objDS.Tables[0].Rows[0]["VALID_FROM"]));
                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["VALID_TO"].ToString()))
                        {
                            strMsg = strMsg.Replace("<VALID TO>", Convert.ToString(objDS.Tables[0].Rows[0]["VALID_TO"]));
                        }
                        strMsg = strMsg.Replace("<BIZ TYPE>", Convert.ToString(objDS.Tables[0].Rows[0]["BIZ_TYPE"]));
                        strMsg = strMsg.Replace("<CARGO TYPE>", Convert.ToString(objDS.Tables[0].Rows[0]["CARGO_TYPE"]));
                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["STATUS"].ToString()))
                        {
                            strMsg = strMsg.Replace("<STATUS>", Convert.ToString(objDS.Tables[0].Rows[0]["STATUS"]));
                        }
                    }

                }
                else if (strDocId == "Customer Contract Sea Request" | strDocId == "Customer Contract Sea Approval")
                {
                    objDS = FetchSeaCustContract(lngPkValue);
                    if (objDS.Tables[0].Rows.Count > 0)
                    {
                        strMsg = strMsg.Replace("<CONTRACT NR>", Convert.ToString(objDS.Tables[0].Rows[0]["CONT_REF_NO"]));
                        DocRefNr = Convert.ToString(objDS.Tables[0].Rows[0]["CONT_REF_NO"]);
                        strMsg = strMsg.Replace("<CONTRACT DATE>", Convert.ToString(objDS.Tables[0].Rows[0]["CONTRACT_DATE"]));
                        strMsg = strMsg.Replace("<CARGO TYPE>", Convert.ToString(objDS.Tables[0].Rows[0]["CARGO_TYPE"]));
                        strMsg = strMsg.Replace("<VALID FROM>", Convert.ToString(objDS.Tables[0].Rows[0]["VALID_FROM"]));
                        strMsg = strMsg.Replace("<VALID TO>", Convert.ToString(objDS.Tables[0].Rows[0]["VALID_TO"]));
                        strMsg = strMsg.Replace("<STATUS>", Convert.ToString(objDS.Tables[0].Rows[0]["STATUS"]));
                        strMsg = strMsg.Replace("<TARIFF NR>", Convert.ToString(objDS.Tables[0].Rows[0]["TARIFF_REF_NO"]));
                        strMsg = strMsg.Replace("<CUSTOMER>", Convert.ToString(objDS.Tables[0].Rows[0]["CUSTOMER_NAME"]));
                    }

                }
                else if (strDocId == "Customer Contract Air Request" | strDocId == "Customer Contract Air Approval")
                {
                    objDS = FetchAirCustContract(lngPkValue);
                    if (objDS.Tables[0].Rows.Count > 0)
                    {
                        strMsg = strMsg.Replace("<CONTRACT NR>", Convert.ToString(objDS.Tables[0].Rows[0]["CONT_REF_NO"]));
                        DocRefNr = Convert.ToString(objDS.Tables[0].Rows[0]["CONT_REF_NO"]);
                        strMsg = strMsg.Replace("<CONTRACT DATE>", Convert.ToString(objDS.Tables[0].Rows[0]["CONTRACT_DATE"]));
                        strMsg = strMsg.Replace("<VALID FROM>", Convert.ToString(objDS.Tables[0].Rows[0]["VALID_FROM"]));
                        strMsg = strMsg.Replace("<VALID TO>", Convert.ToString(objDS.Tables[0].Rows[0]["VALID_TO"]));
                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["STATUS"].ToString()))
                        {
                            strMsg = strMsg.Replace("<STATUS>", Convert.ToString(objDS.Tables[0].Rows[0]["STATUS"]));
                        }
                        strMsg = strMsg.Replace("<TARIFF NR>", Convert.ToString(objDS.Tables[0].Rows[0]["TARIFF_REF_NO"]));
                        strMsg = strMsg.Replace("<CUSTOMER>", Convert.ToString(objDS.Tables[0].Rows[0]["CUSTOMER_NAME"]));
                    }

                }
                else if (strDocId == "Quotation Sea Request" | strDocId == "Quotation Sea Approval")
                {
                    objDS = FetchSeaQuotation(lngPkValue);
                    if (objDS.Tables[0].Rows.Count > 0)
                    {
                        strMsg = strMsg.Replace("<QUOTATION NR>", Convert.ToString(objDS.Tables[0].Rows[0]["QUOTATION_REF_NO"]));
                        DocRefNr = Convert.ToString(objDS.Tables[0].Rows[0]["QUOTATION_REF_NO"]);
                        strMsg = strMsg.Replace("<QUOTATION DATE>", Convert.ToString(objDS.Tables[0].Rows[0]["QUOT_DATE"]));
                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["SHIPMENT_DT"].ToString()))
                        {
                            strMsg = strMsg.Replace("<SHIPMENT DATE>", Convert.ToString(objDS.Tables[0].Rows[0]["SHIPMENT_DT"]));
                        }
                        else
                        {
                            strMsg = strMsg.Replace("<SHIPMENT DATE>", "");
                        }
                        strMsg = strMsg.Replace("<VALID FROM>", Convert.ToString(objDS.Tables[0].Rows[0]["QUOT_DATE"]));
                        strMsg = strMsg.Replace("<VALID TO>", Convert.ToString(objDS.Tables[0].Rows[0]["VALID_TILL"]));
                        strMsg = strMsg.Replace("<CUSTOMER>", Convert.ToString(objDS.Tables[0].Rows[0]["CUSTOMER_NAME"]));
                        strMsg = strMsg.Replace("<STATUS>", Convert.ToString(objDS.Tables[0].Rows[0]["STATUS"]));
                        strMsg = strMsg.Replace("<CARGO TYPE>", Convert.ToString(objDS.Tables[0].Rows[0]["CARGO_TYPE"]));
                    }
                }
                else if (strDocId == "Quotation Import Sea Request" | strDocId == "Quotation Import Sea Approval")
                {
                    objDS = FetchSeaQuotation(lngPkValue);
                    if (objDS.Tables[0].Rows.Count > 0)
                    {
                        strMsg = strMsg.Replace("<QUOTATION NR>", Convert.ToString(objDS.Tables[0].Rows[0]["QUOTATION_REF_NO"]));
                        DocRefNr = Convert.ToString(objDS.Tables[0].Rows[0]["QUOTATION_REF_NO"]);
                        strMsg = strMsg.Replace("<QUOTATION DATE>", Convert.ToString(objDS.Tables[0].Rows[0]["QUOT_DATE"]));
                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["SHIPMENT_DT"].ToString()))
                        {
                            strMsg = strMsg.Replace("<SHIPMENT DATE>", Convert.ToString(objDS.Tables[0].Rows[0]["SHIPMENT_DT"]));
                        }
                        else
                        {
                            strMsg = strMsg.Replace("<SHIPMENT DATE>", "");
                        }
                        //strMsg = strMsg.Replace("<SHIPMENT DATE>", objDS.Tables(0).Rows(0)["SHIPMENT_DT"))
                        strMsg = strMsg.Replace("<VALID FROM>", Convert.ToString(objDS.Tables[0].Rows[0]["QUOT_DATE"]));
                        strMsg = strMsg.Replace("<VALID TO>", Convert.ToString(objDS.Tables[0].Rows[0]["VALID_TILL"]));
                        strMsg = strMsg.Replace("<CUSTOMER>", Convert.ToString(objDS.Tables[0].Rows[0]["CUSTOMER_NAME"]));
                        strMsg = strMsg.Replace("<STATUS>", Convert.ToString(objDS.Tables[0].Rows[0]["STATUS"]));
                        strMsg = strMsg.Replace("<CARGO TYPE>", Convert.ToString(objDS.Tables[0].Rows[0]["CARGO_TYPE"]));
                    }

                }
                else if (strDocId == "Quotation Air Request" | strDocId == "Quotation Air Approval")
                {
                    objDS = FetchAirQuotation(lngPkValue);
                    if (objDS.Tables[0].Rows.Count > 0)
                    {
                        strMsg = strMsg.Replace("<QUOTATION NR>", Convert.ToString(objDS.Tables[0].Rows[0]["QUOTATION_REF_NO"]));
                        DocRefNr = Convert.ToString(objDS.Tables[0].Rows[0]["QUOTATION_REF_NO"]);
                        strMsg = strMsg.Replace("<QUOTATION DATE>", Convert.ToString(objDS.Tables[0].Rows[0]["QUOT_DATE"]));
                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["SHIPMENT_DT"].ToString()))
                        {
                            strMsg = strMsg.Replace("<SHIPMENT DATE>", Convert.ToString(objDS.Tables[0].Rows[0]["SHIPMENT_DT"]));
                        }
                        else
                        {
                            strMsg = strMsg.Replace("<SHIPMENT DATE>", "");
                        }
                        strMsg = strMsg.Replace("<CUSTOMER>", Convert.ToString(objDS.Tables[0].Rows[0]["CUSTOMER_NAME"]));
                        strMsg = strMsg.Replace("<STATUS>", Convert.ToString(objDS.Tables[0].Rows[0]["STATUS"]));
                    }
                }
                else if (strDocId == "Quotation Import Air Request" | strDocId == "Quotation Import Air Approval")
                {
                    objDS = FetchAirQuotation(lngPkValue);
                    if (objDS.Tables[0].Rows.Count > 0)
                    {
                        strMsg = strMsg.Replace("<QUOTATION NR>", Convert.ToString(objDS.Tables[0].Rows[0]["QUOTATION_REF_NO"]));
                        DocRefNr = Convert.ToString(objDS.Tables[0].Rows[0]["QUOTATION_REF_NO"]);
                        strMsg = strMsg.Replace("<QUOTATION DATE>", objDS.Tables[0].Rows[0]["QUOT_DATE"].ToString());
                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["SHIPMENT_DT"].ToString()))
                        {
                            strMsg = strMsg.Replace("<SHIPMENT DATE>", Convert.ToString(objDS.Tables[0].Rows[0]["SHIPMENT_DT"]));
                        }
                        else
                        {
                            strMsg = strMsg.Replace("<SHIPMENT DATE>", "");
                        }
                        strMsg = strMsg.Replace("<CUSTOMER>", Convert.ToString(objDS.Tables[0].Rows[0]["CUSTOMER_NAME"]));
                        strMsg = strMsg.Replace("<STATUS>", Convert.ToString(objDS.Tables[0].Rows[0]["STATUS"]));
                    }

                }
                else if (strDocId == "Sales Plan Request" | strDocId == "Sales Plan Approval")
                {
                    objDS = FetchSalesPlan(lngPkValue, saledate, strpkvalue);
                    if (objDS.Tables[0].Rows.Count > 0)
                    {
                        strMsg = strMsg.Replace("<EXECUTIVE NAME>", Convert.ToString(objDS.Tables[0].Rows[0]["employee_name"]));
                        strMsg = strMsg.Replace("<CUSTOMER NAME>", Convert.ToString(objDS.Tables[0].Rows[0]["CUSTOMER_NAME"]));
                        strMsg = strMsg.Replace("<FROM TIME>", Convert.ToString(objDS.Tables[0].Rows[0]["FR_TIME"]));
                        strMsg = strMsg.Replace("<TO TIME>", Convert.ToString(objDS.Tables[0].Rows[0]["TO_TIME"]));
                        strMsg = strMsg.Replace("<STATUS>", Convert.ToString(objDS.Tables[0].Rows[0]["STATUS"]));
                        DocRefNr = "";
                    }

                }
                else if (strDocId == "Voucher Request" | strDocId == "Voucher Approval")
                {
                    objDS = FetchVoucher(lngPkValue);
                    if (objDS.Tables[0].Rows.Count > 0)
                    {
                        if (strMsg.IndexOf("<VOUCHER NR>") >= 0)
                        {
                            strMsg = strMsg.Replace("<VOUCHER NR>", Convert.ToString(objDS.Tables[0].Rows[0]["VCHR_NR"]));
                        }
                        else
                        {
                            strMsg = strMsg.Replace("<VOUCHER NUMBER>", Convert.ToString(objDS.Tables[0].Rows[0]["VCHR_NR"]));
                        }
                        DocRefNr = Convert.ToString(objDS.Tables[0].Rows[0]["VCHR_NR"]);
                        strMsg = strMsg.Replace("<VOUCHER DATE>", Convert.ToString(objDS.Tables[0].Rows[0]["VCHR_DT"]));
                        strMsg = strMsg.Replace("<SUPPLIER DUEDATE>", Convert.ToString(objDS.Tables[0].Rows[0]["Due_DT"]));
                        strMsg = strMsg.Replace("<INVOICE DATE>", Convert.ToString(objDS.Tables[0].Rows[0]["INVOICE_DATE"]));
                        strMsg = strMsg.Replace("<SUPPLIER NAME>", Convert.ToString(objDS.Tables[0].Rows[0]["SUPP_NAME"]));
                        strMsg = strMsg.Replace("<BIZ TYPE>", Convert.ToString(objDS.Tables[0].Rows[0]["BIZ_TYPE"]));
                        strMsg = strMsg.Replace("<PROCESS TYPE>", Convert.ToString(objDS.Tables[0].Rows[0]["PROCESS_TYPE"]));
                        strMsg = strMsg.Replace("<STATUS>", Convert.ToString(objDS.Tables[0].Rows[0]["STATUS"]));
                        strMsg = strMsg.Replace("<INVOICE NUMBER>", Convert.ToString(objDS.Tables[0].Rows[0]["SUPPLIER_INV_NO"]));
                    }

                }
                else if (strDocId == "Payment Request" | strDocId == "Payment Approval")
                {
                    objDS = FetchPayment(lngPkValue);
                    if (objDS.Tables[0].Rows.Count > 0)
                    {
                        strMsg = strMsg.Replace("<PAYMENT REFNR>", Convert.ToString(objDS.Tables[0].Rows[0]["PAYMENT_REF_NO"]));
                        DocRefNr = Convert.ToString(objDS.Tables[0].Rows[0]["PAYMENT_REF_NO"]);
                        strMsg = strMsg.Replace("<PAYMENT DATE>", Convert.ToString(objDS.Tables[0].Rows[0]["PAYMENT_DATE"]));
                        strMsg = strMsg.Replace("<PAYMENT CURRENCY>", Convert.ToString(objDS.Tables[0].Rows[0]["CURRENCY_NAME"]));
                        strMsg = strMsg.Replace("<SUPPLIER NAME>", Convert.ToString(objDS.Tables[0].Rows[0]["VENDOR_NAME"]));
                        strMsg = strMsg.Replace("<BIZ TYPE>", Convert.ToString(objDS.Tables[0].Rows[0]["BIZ_TYPE"]));
                        strMsg = strMsg.Replace("<VOUCHER REFNR>", Convert.ToString(objDS.Tables[0].Rows[0]["INVOICE_REF_NO"]));
                        strMsg = strMsg.Replace("<PROCESS TYPE>", Convert.ToString(objDS.Tables[0].Rows[0]["PROCESS_TYPE"]));
                        strMsg = strMsg.Replace("<PAYMENT STATUS>", Convert.ToString(objDS.Tables[0].Rows[0]["STATUS"]));
                    }

                }
                else if (strDocId == "SRR Request Sea" | strDocId == "SRR Approval Sea")
                {
                    objDS = FetchSrrContractSea(lngPkValue);
                    if (objDS.Tables[0].Rows.Count > 0)
                    {
                        strMsg = strMsg.Replace("<SRR NR>", Convert.ToString(objDS.Tables[0].Rows[0]["SRR_REfNR"]));
                        DocRefNr = Convert.ToString(objDS.Tables[0].Rows[0]["SRR_REfNR"]);
                        strMsg = strMsg.Replace("<SRR DATE>", Convert.ToString(objDS.Tables[0].Rows[0]["SRR_DATE"]));
                        strMsg = strMsg.Replace("<CUSTOMER>", Convert.ToString(objDS.Tables[0].Rows[0]["CUST_NAME"]));
                        strMsg = strMsg.Replace("<VALID FROM>", Convert.ToString(objDS.Tables[0].Rows[0]["VALID_FROM"]));
                        strMsg = strMsg.Replace("<VALID TO>", Convert.ToString(objDS.Tables[0].Rows[0]["VALID_TO"]));
                        strMsg = strMsg.Replace("<CARGO TYPE>", Convert.ToString(objDS.Tables[0].Rows[0]["CARGO_TYPE"]));
                        strMsg = strMsg.Replace("<STATUS>", Convert.ToString(objDS.Tables[0].Rows[0]["STATUS"]));

                    }
                }
                else if (strDocId == "SRR Request Air" | strDocId == "SRR Approval Air")
                {
                    objDS = FetchSrrContractAir(lngPkValue);
                    if (objDS.Tables[0].Rows.Count > 0)
                    {
                        strMsg = strMsg.Replace("<SRR NR>", Convert.ToString(objDS.Tables[0].Rows[0]["SRR_REfNR"]));
                        DocRefNr = Convert.ToString(objDS.Tables[0].Rows[0]["SRR_REfNR"]);
                        strMsg = strMsg.Replace("<SRR DATE>", Convert.ToString(objDS.Tables[0].Rows[0]["SRR_DATE"]));
                        strMsg = strMsg.Replace("<CUSTOMER>", Convert.ToString(objDS.Tables[0].Rows[0]["CUST_NAME"]));
                        strMsg = strMsg.Replace("<VALID FROM>", Convert.ToString(objDS.Tables[0].Rows[0]["VALID_FROM"]));
                        strMsg = strMsg.Replace("<VALID TO>", Convert.ToString(objDS.Tables[0].Rows[0]["VALID_TO"]));
                        strMsg = strMsg.Replace("<STATUS>", Convert.ToString(objDS.Tables[0].Rows[0]["STATUS"]));

                    }
                }
                else if (strDocId == "Call Registration")
                {
                    objDS = FetchCallReg(lngPkValue);
                    if (objDS.Tables[0].Rows.Count > 0)
                    {
                        strMsg = strMsg.Replace("<CALL REG NR>", Convert.ToString(objDS.Tables[0].Rows[0]["CS_REF_NO"]));
                        DocRefNr = Convert.ToString(objDS.Tables[0].Rows[0]["CS_REF_NO"]);
                        strMsg = strMsg.Replace("<DATE>", Convert.ToString(objDS.Tables[0].Rows[0]["CS_DT"]));
                        strMsg = strMsg.Replace("<CALL TYPE>", Convert.ToString(objDS.Tables[0].Rows[0]["CALL_TYPE"]));
                        strMsg = strMsg.Replace("<STATUS>", Convert.ToString(objDS.Tables[0].Rows[0]["STATUS"]));
                        strMsg = strMsg.Replace("<CUSTOMER", Convert.ToString(objDS.Tables[0].Rows[0]["CUSTOMER_NAME"]));
                        strMsg = strMsg.Replace("<POL>", Convert.ToString(objDS.Tables[0].Rows[0]["POL"]));
                        strMsg = strMsg.Replace("<POD>", Convert.ToString(objDS.Tables[0].Rows[0]["POD"]));
                        strMsg = strMsg.Replace("<BIZ TYPE>", Convert.ToString(objDS.Tables[0].Rows[0]["BIZ_TYPE"]));
                        strMsg = strMsg.Replace("<PROCESS>", Convert.ToString(objDS.Tables[0].Rows[0]["PROCESS"]));
                    }
                }
                else if (strDocId == "Invoice Request" | strDocId == "Invoice Approval")
                {
                    objDS = FetchInvoice(lngPkValue, TypeFlag);
                    if (objDS.Tables[0].Rows.Count > 0)
                    {
                        strMsg = strMsg.Replace("<INVOICE REFNR>", Convert.ToString((string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["INVOICE_REF_NO"].ToString()) ? "" : objDS.Tables[0].Rows[0]["INVOICE_REF_NO"])));
                        DocRefNr = Convert.ToString(objDS.Tables[0].Rows[0]["INVOICE_REF_NO"]);
                        strMsg = strMsg.Replace("<INVOICE DATE>", Convert.ToString((string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["INVOICE_DATE"].ToString()) ? "" : objDS.Tables[0].Rows[0]["INVOICE_DATE"])));
                        strMsg = strMsg.Replace("<INVOICE DUE DATE>", Convert.ToString((string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["INVOICE_DUE_DATE"].ToString()) ? "" : objDS.Tables[0].Rows[0]["INVOICE_DUE_DATE"])));
                        strMsg = strMsg.Replace("<INVOICE CURRENCY>", Convert.ToString((string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["CURRENCY_ID"].ToString()) ? "" : objDS.Tables[0].Rows[0]["CURRENCY_ID"])));
                        strMsg = strMsg.Replace("<CUSTOMER NAME>", Convert.ToString((string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["CUSTOMER_NAME"].ToString()) ? "" : objDS.Tables[0].Rows[0]["CUSTOMER_NAME"])));
                        strMsg = strMsg.Replace("<JC REFNR>", Convert.ToString((string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["JC_REFNO"].ToString()) ? "" : objDS.Tables[0].Rows[0]["JC_REFNO"])));
                        strMsg = strMsg.Replace("<INVOICE STATUS>", Convert.ToString(objDS.Tables[0].Rows[0]["CHK_INVOICE"]));
                    }
                }
                else if (strDocId == "Customer Allocation Request" | strDocId == "Customer Allocation Approval")
                {
                    objDS = FetchCustomer(lngPkValue);
                    if (objDS.Tables[0].Rows.Count > 0)
                    {
                        DocRefNr = Convert.ToString(objDS.Tables[0].Rows[0]["CUSTOMER_ID"]);
                        strMsg = strMsg.Replace("<CUSTOMER ID>", Convert.ToString((string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["CUSTOMER_ID"].ToString()) ? "" : objDS.Tables[0].Rows[0]["CUSTOMER_ID"])));
                        strMsg = strMsg.Replace("<CUSTOMER NAME>", Convert.ToString((string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["CUSTOMER_NAME"].ToString()) ? "" : objDS.Tables[0].Rows[0]["CUSTOMER_NAME"])));
                        strMsg = strMsg.Replace("<SALES EXECUTIVE>", Convert.ToString((string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["EMPLOYEE_NAME"].ToString()) ? "" : objDS.Tables[0].Rows[0]["EMPLOYEE_NAME"])));
                        strMsg = strMsg.Replace("<STATUS>", Convert.ToString((string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["STATUS"].ToString()) ? "" : objDS.Tables[0].Rows[0]["STATUS"])));
                    }

                }
                else if (strDocId == "JMF Alert" | strDocId == "JMF Alert")
                {
                    objDS = FetchJMFDetails(lngPkValue, strpkvalue);
                    if (objDS.Tables[0].Rows.Count > 0)
                    {
                        DocRefNr = Convert.ToString(objDS.Tables[0].Rows[0]["BOOKING_REF_NO"]);
                        strMsg = strMsg.Replace("<Booking Nr.>", Convert.ToString(objDS.Tables[0].Rows[0]["BOOKING_REF_NO"]));
                        strMsg = strMsg.Replace("<Booking Date>", Convert.ToString(objDS.Tables[0].Rows[0]["BOOKING_DATE"]));
                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["CARGO_TYPE"].ToString()))
                        {
                            strMsg = strMsg.Replace("<Cargo Type>", Convert.ToString(objDS.Tables[0].Rows[0]["CARGO_TYPE"]));
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["CUSTOMER_NAME"].ToString()))
                        {
                            strMsg = strMsg.Replace("<Customer>", Convert.ToString(objDS.Tables[0].Rows[0]["CUSTOMER_NAME"]));
                        }
                        strMsg = strMsg.Replace("<Biz Type>", Convert.ToString(objDS.Tables[0].Rows[0]["BUSINESS_TYPE"]));

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["COMMODITY_GROUP_CODE"].ToString()))
                        {
                            strMsg = strMsg.Replace("<Commodity Type>", Convert.ToString(objDS.Tables[0].Rows[0]["COMMODITY_GROUP_CODE"]));
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["DOC_REF_NO"].ToString()))
                        {
                            strMsg = strMsg.Replace("<Process/Activity>", Convert.ToString(objDS.Tables[0].Rows[0]["DOC_REF_NO"]));
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["ACTIVITY_DESC"].ToString()))
                        {
                            strMsg = strMsg.Replace("<Alert Description>", Convert.ToString(objDS.Tables[0].Rows[0]["ACTIVITY_DESC"]));
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["USER_NAME"].ToString()))
                        {
                            strMsg = strMsg.Replace("<Sender>", Convert.ToString(objDS.Tables[0].Rows[0]["USER_NAME"]));
                        }
                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["REMINDER_ON_DT"].ToString()))
                        {
                            strMsg = strMsg.Replace("<Date and Time>", Convert.ToString(objDS.Tables[0].Rows[0]["REMINDER_ON_DT"]));
                        }

                    }
                }
                else if (strDocId == "Foreign Agent Alert")
                {
                    objDS = FetchForeignAgentDetails(lngPkValue, strpkvalue);
                    if (objDS.Tables[0].Rows.Count > 0)
                    {
                        DocRefNr = Convert.ToString(objDS.Tables[0].Rows[0]["THD_FRT_PAYER"]);
                        strMsg = strMsg.Replace("<POL>", Convert.ToString(objDS.Tables[0].Rows[0]["POL"]));
                        strMsg = strMsg.Replace("<POD>", Convert.ToString(objDS.Tables[0].Rows[0]["POD"]));
                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["THD_FRT_PAYER"].ToString()))
                        {
                            strMsg = strMsg.Replace("<Third Party Freight Payer Name>", Convert.ToString(objDS.Tables[0].Rows[0]["THD_FRT_PAYER"]));
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["ADDRESS"].ToString()))
                        {
                            strMsg = strMsg.Replace("<Addres>", Convert.ToString(objDS.Tables[0].Rows[0]["ADDRESS"]));
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["FREIGHT AMOUNT"].ToString()))
                        {
                            strMsg = strMsg.Replace("<Freight Amount>", Convert.ToString(objDS.Tables[0].Rows[0]["FREIGHT AMOUNT"]));
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["SHIPMENT DATE"].ToString()))
                        {
                            strMsg = strMsg.Replace("<Shipment Date>", Convert.ToString(objDS.Tables[0].Rows[0]["SHIPMENT DATE"]));
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["VOYAGE"].ToString()))
                        {
                            strMsg = strMsg.Replace("<Vessel Voyage>", Convert.ToString(objDS.Tables[0].Rows[0]["VOYAGE"]));
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["ETD_DATE"].ToString()))
                        {
                            strMsg = strMsg.Replace("<ETD>", Convert.ToString(objDS.Tables[0].Rows[0]["ETD_DATE"]));
                        }
                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["ETA_DATE"].ToString()))
                        {
                            strMsg = strMsg.Replace("<ETA>", Convert.ToString(objDS.Tables[0].Rows[0]["ETA_DATE"]));
                        }
                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["USER_ID"].ToString()))
                        {
                            strMsg = strMsg.Replace("<Sender>", Convert.ToString(objDS.Tables[0].Rows[0]["USER_ID"]));
                        }
                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["AGENT_NAME"].ToString()))
                        {
                            strMsg = strMsg.Replace("<Receiver>", Convert.ToString(objDS.Tables[0].Rows[0]["AGENT_NAME"]));
                        }

                    }
                }
                else if (strDocId == "E-Comm Gate Way Approve" | strDocId == "E-Comm Gate Way Reject")
                {
                    objDS = FetchECOMMDetails(CustRegFK, Userid);
                    if (objDS.Tables[0].Rows.Count > 0)
                    {
                        DocRefNr = Convert.ToString(objDS.Tables[0].Rows[0]["COMPANY_NAME"]);
                        strMsg = strMsg.Replace("<Company Name>", Convert.ToString(objDS.Tables[0].Rows[0]["COMPANY_NAME"]));
                        strMsg = strMsg.Replace("<Location>", Convert.ToString(objDS.Tables[0].Rows[0]["LOCATION_NAME"]));
                        strMsg = strMsg.Replace("<Req. Reg. Date>", Convert.ToString(objDS.Tables[0].Rows[0]["RECDATE"]));
                        strMsg = strMsg.Replace("<User Id>", Convert.ToString(objDS.Tables[0].Rows[0]["USER_ID"]));
                        strMsg = strMsg.Replace("<Password>", Convert.ToString(objDS.Tables[0].Rows[0]["CUST_PWD"]));
                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["STATUS"].ToString()))
                        {
                            strMsg = strMsg.Replace("<Status>", Convert.ToString(objDS.Tables[0].Rows[0]["STATUS"]));
                        }
                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["REJECT_REMARKS"].ToString()))
                        {
                            strMsg = strMsg.Replace("<Remarks>", Convert.ToString(objDS.Tables[0].Rows[0]["REJECT_REMARKS"]));
                        }
                        else
                        {
                            strMsg = strMsg.Replace("<Remarks>", "");
                        }
                        strMsg = strMsg.Replace("<Date and Time>", Convert.ToString(DateTime.Now.Date));
                    }

                }
                else if (strDocId == "CBJC Profit Percent Request" | strDocId == "CBJC Profit Percent Approval")
                {
                    objDS = FetchCBJCDetails(Convert.ToString(lngPkValue), TypeFlag);
                    if (objDS.Tables[0].Rows.Count > 0)
                    {
                        DocRefNr = Convert.ToString(objDS.Tables[0].Rows[0]["CBJC_NO"]);
                        strMsg = strMsg.Replace("<REFERENCE NR.>", Convert.ToString(objDS.Tables[0].Rows[0]["CBJC_NO"]));
                        strMsg = strMsg.Replace("<REFERENCE DT.>", Convert.ToString(objDS.Tables[0].Rows[0]["CBJC_DATE"]));
                        strMsg = strMsg.Replace("<REFERENCE TYPE>", Convert.ToString(objDS.Tables[0].Rows[0]["REFERENCE_TYPE"]));
                        strMsg = strMsg.Replace("<BIZ TYPE>", Convert.ToString(objDS.Tables[0].Rows[0]["BIZ_TYPE"]));
                        strMsg = strMsg.Replace("<CARGO TYPE>", Convert.ToString(objDS.Tables[0].Rows[0]["CARGO_TYPE"]));
                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["POL"].ToString()))
                        {
                            strMsg = strMsg.Replace("<POL/AOO>", Convert.ToString(objDS.Tables[0].Rows[0]["POL"]));
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["POD"].ToString()))
                        {
                            strMsg = strMsg.Replace("<POD/AOD>", Convert.ToString(objDS.Tables[0].Rows[0]["POD"]));
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["CUSTOMER_NAME"].ToString()))
                        {
                            strMsg = strMsg.Replace("<CUSTOMER NAME>", Convert.ToString(objDS.Tables[0].Rows[0]["CUSTOMER_NAME"]));
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["VESSEL_VOYAGE"].ToString()))
                        {
                            strMsg = strMsg.Replace("<VSL/VOY / FLIGHT NR.>", Convert.ToString(objDS.Tables[0].Rows[0]["VESSEL_VOYAGE"]));
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["STATUS"].ToString()))
                        {
                            strMsg = strMsg.Replace("<STATUS>", Convert.ToString(objDS.Tables[0].Rows[0]["STATUS"]));
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["LOCATION_PROFIT"].ToString()))
                        {
                            strMsg = strMsg.Replace("<LOCATION PROFIT>", Convert.ToString(objDS.Tables[0].Rows[0]["LOCATION_PROFIT"]));
                        }
                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["SHIPMENT_PROFIT"].ToString()))
                        {
                            strMsg = strMsg.Replace("<SHIPMENT PROFIT>", Convert.ToString(objDS.Tables[0].Rows[0]["SHIPMENT_PROFIT"]));
                        }
                    }
                }
                else if (strDocId == "Transporter Profit Percent Request" | strDocId == "Transporter Profit Percent  Approval")
                {
                    objDS = FetchTransporterNoteDetails(Convert.ToString(lngPkValue), TypeFlag);
                    if (objDS.Tables[0].Rows.Count > 0)
                    {
                        DocRefNr = Convert.ToString(objDS.Tables[0].Rows[0]["TRANS_INST_REF_NO"]);
                        strMsg = strMsg.Replace("<REFERENCE NR.>", Convert.ToString(objDS.Tables[0].Rows[0]["TRANS_INST_REF_NO"]));
                        strMsg = strMsg.Replace("<REFERENCE DT.>", Convert.ToString(objDS.Tables[0].Rows[0]["TRANS_INST_DATE"]));
                        strMsg = strMsg.Replace("<REFERENCE TYPE>", Convert.ToString(objDS.Tables[0].Rows[0]["REFERENCE_TYPE"]));
                        strMsg = strMsg.Replace("<BIZ TYPE>", Convert.ToString(objDS.Tables[0].Rows[0]["BIZ_TYPE"]));
                        strMsg = strMsg.Replace("<CARGO TYPE>", Convert.ToString(objDS.Tables[0].Rows[0]["CARGO_TYPE"]));
                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["POL"].ToString()))
                        {
                            strMsg = strMsg.Replace("<POL/AOO>", Convert.ToString(objDS.Tables[0].Rows[0]["POL"]));
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["POD"].ToString()))
                        {
                            strMsg = strMsg.Replace("<POD/AOD>", Convert.ToString(objDS.Tables[0].Rows[0]["POD"]));
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["CUSTOMER_NAME"].ToString()))
                        {
                            strMsg = strMsg.Replace("<CUSTOMER NAME>", Convert.ToString(objDS.Tables[0].Rows[0]["CUSTOMER_NAME"]));
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["VESSEL_VOYAGE"].ToString()))
                        {
                            strMsg = strMsg.Replace("<VSL/VOY / FLIGHT NR.>", Convert.ToString(objDS.Tables[0].Rows[0]["VESSEL_VOYAGE"]));
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["STATUS"].ToString()))
                        {
                            strMsg = strMsg.Replace("<STATUS>", Convert.ToString(objDS.Tables[0].Rows[0]["STATUS"]));
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["LOCATION_PROFIT"].ToString()))
                        {
                            strMsg = strMsg.Replace("<LOCATION PROFIT>", Convert.ToString(objDS.Tables[0].Rows[0]["LOCATION_PROFIT"]));
                        }
                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["SHIPMENT_PROFIT"].ToString()))
                        {
                            strMsg = strMsg.Replace("<SHIPMENT PROFIT>", Convert.ToString(objDS.Tables[0].Rows[0]["SHIPMENT_PROFIT"]));
                        }
                    }
                }
                else if (strDocId == "Job Card Profit Percent Request" | strDocId == "Job Card Profit Percent Approval")
                {
                    objDS = FetchJobcardDetails(Convert.ToString(lngPkValue), TypeFlag);
                    if (objDS.Tables[0].Rows.Count > 0)
                    {
                        DocRefNr = Convert.ToString(objDS.Tables[0].Rows[0]["JOBCARD_REF_NO"]);
                        strMsg = strMsg.Replace("<REFERENCE NR.>", Convert.ToString(objDS.Tables[0].Rows[0]["JOBCARD_REF_NO"]));
                        strMsg = strMsg.Replace("<REFERENCE DT.>", Convert.ToString(objDS.Tables[0].Rows[0]["JOBCARD_DATE"]));
                        strMsg = strMsg.Replace("<REFERENCE TYPE>", Convert.ToString(objDS.Tables[0].Rows[0]["REFERENCE_TYPE"]));
                        strMsg = strMsg.Replace("<BIZ TYPE>", Convert.ToString(objDS.Tables[0].Rows[0]["BIZ_TYPE"]));
                        strMsg = strMsg.Replace("<CARGO TYPE>", Convert.ToString(objDS.Tables[0].Rows[0]["CARGO_TYPE"]));
                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["POL"].ToString()))
                        {
                            strMsg = strMsg.Replace("<POL/AOO>", Convert.ToString(objDS.Tables[0].Rows[0]["POL"]));
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["POD"].ToString()))
                        {
                            strMsg = strMsg.Replace("<POD/AOD>", Convert.ToString(objDS.Tables[0].Rows[0]["POD"]));
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["CUSTOMER_NAME"].ToString()))
                        {
                            strMsg = strMsg.Replace("<CUSTOMER NAME>", Convert.ToString(objDS.Tables[0].Rows[0]["CUSTOMER_NAME"]));
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["VESSEL_VOYAGE"].ToString()))
                        {
                            strMsg = strMsg.Replace("<VSL/VOY / FLIGHT NR.>", Convert.ToString(objDS.Tables[0].Rows[0]["VESSEL_VOYAGE"]));
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["STATUS"].ToString()))
                        {
                            strMsg = strMsg.Replace("<STATUS>", Convert.ToString(objDS.Tables[0].Rows[0]["STATUS"]));
                        }

                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["LOCATION_PROFIT"].ToString()))
                        {
                            strMsg = strMsg.Replace("<LOCATION PROFIT>", Convert.ToString(objDS.Tables[0].Rows[0]["LOCATION_PROFIT"]));
                        }
                        if (!string.IsNullOrEmpty(objDS.Tables[0].Rows[0]["SHIPMENT_PROFIT"].ToString()))
                        {
                            strMsg = strMsg.Replace("<SHIPMENT PROFIT>", Convert.ToString(objDS.Tables[0].Rows[0]["SHIPMENT_PROFIT"]));
                        }
                    }
                }

                return strMsg;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion
        #region "FetchJMFDetails "
        public DataSet FetchJMFDetails(long lngPkValue, long strpkvalue)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append("SELECT B.BOOKING_REF_NO,");
            sb.Append("       B.BOOKING_DATE,");
            sb.Append("       C.CUSTOMER_NAME,");
            sb.Append("       DECODE(B.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGO_TYPE,");
            sb.Append("       DECODE(B.BUSINESS_TYPE, 1, 'AIR', 2, 'SEA') BUSINESS_TYPE,");
            sb.Append("       CG.COMMODITY_GROUP_CODE,");
            sb.Append("        CASE ");
            sb.Append("        WHEN  T.DOC_REF_NO IS NOT NULL THEN ");
            sb.Append("       (T.Status || ' (' || T.DOC_REF_NO ||')') ");
            sb.Append("        ELSE ");
            sb.Append("        T.STATUS ");
            sb.Append("       END DOC_REF_NO,");
            sb.Append("       T.ACTIVITY_DESC,");
            sb.Append("       U.USER_NAME,");
            sb.Append("       T.REMINDER_ON_DT,");
            sb.Append("       J.JOB_CARD_TRN_PK");
            sb.Append("  FROM BOOKING_MST_TBL         B,");
            sb.Append("       JOB_CARD_TRN            J,");
            sb.Append("       CUSTOMER_MST_TBL        C,");
            sb.Append("       COMMODITY_GROUP_MST_TBL CG,");
            sb.Append("       TRACK_N_TRACE_TBL       T,");
            sb.Append("       USER_MST_TBL            U");
            sb.Append(" WHERE J.BOOKING_MST_FK = B.BOOKING_MST_PK");
            sb.Append("   AND B.COMMODITY_GROUP_FK = CG.COMMODITY_GROUP_PK(+)");
            sb.Append("   AND B.CUST_CUSTOMER_MST_FK = C.CUSTOMER_MST_PK");
            sb.Append("   AND T.JOB_CARD_FK(+) = J.JOB_CARD_TRN_PK");
            sb.Append("   AND J.JOB_CARD_TRN_PK = " + lngPkValue);
            sb.Append("   AND T.REMINDER_PK = " + strpkvalue);
            sb.Append("   AND T.CREATED_BY = U.USER_MST_PK");
            sb.Append("   UNION ALL");
            sb.Append("   SELECT B.BOOKING_REF_NO,");
            sb.Append("       B.BOOKING_DATE,");
            sb.Append("       C.CUSTOMER_NAME,");
            sb.Append("       DECODE(B.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGO_TYPE,");
            sb.Append("       DECODE(B.BUSINESS_TYPE, 1, 'AIR', 2, 'SEA') BUSINESS_TYPE,");
            sb.Append("       CG.COMMODITY_GROUP_CODE,");
            sb.Append("        CASE ");
            sb.Append("        WHEN  TE.DOC_REF_NO IS NOT NULL THEN ");
            sb.Append("       (TE.Status || ' (' || TE.DOC_REF_NO ||')') ");
            sb.Append("        ELSE ");
            sb.Append("        TE.STATUS ");
            sb.Append("       END DOC_REF_NO,");
            sb.Append("       TE.ACTIVITY_DESC,");
            sb.Append("       U.USER_NAME,");
            sb.Append("       TE.REMINDER_ON_DT,");
            sb.Append("       J.JOB_CARD_TRN_PK");
            sb.Append("  FROM BOOKING_MST_TBL         B,");
            sb.Append("       JOB_CARD_TRN            J,");
            sb.Append("       CUSTOMER_MST_TBL        C,");
            sb.Append("       COMMODITY_GROUP_MST_TBL CG,");
            sb.Append("       TRACK_N_TRACE_TBL_EXT   TE,");
            sb.Append("       USER_MST_TBL            U");
            sb.Append(" WHERE J.BOOKING_MST_FK = B.BOOKING_MST_PK");
            sb.Append("   AND B.COMMODITY_GROUP_FK = CG.COMMODITY_GROUP_PK(+)");
            sb.Append("   AND B.CUST_CUSTOMER_MST_FK = C.CUSTOMER_MST_PK");
            sb.Append("   AND TE.JOB_CARD_FK = J.JOB_CARD_TRN_PK");
            sb.Append("   AND J.JOB_CARD_TRN_PK = " + lngPkValue);
            sb.Append("   AND TE.REMINDER_PK = " + strpkvalue);
            sb.Append("   AND TE.CREATED_BY = U.USER_MST_PK");
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion

        #region " Fetch CBJC Details"
        public DataSet FetchCBJCDetails(string lngPkValue, string Percentage = "0")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT CT.CBJC_PK,");
            sb.Append("       CT.CBJC_NO,");
            sb.Append("       TO_CHAR(CT.CBJC_DATE,'DD/MM/YYYY') CBJC_DATE,");
            sb.Append("       'CBJC' REFERENCE_TYPE,");
            sb.Append("       DECODE(CT.BIZ_TYPE, 1, 'Air', 2, 'Sea') BIZ_TYPE,");
            sb.Append("       CASE");
            sb.Append("         WHEN CT.BIZ_TYPE = 2 THEN");
            sb.Append("       DECODE(CT.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC')");
            sb.Append("         ELSE");
            sb.Append("       DECODE(CT.CARGO_TYPE, 1, 'KGS', 2, 'ULD') ");
            sb.Append("       END CARGO_TYPE,");
            sb.Append("       CASE");
            sb.Append("         WHEN CT.BIZ_TYPE = 2 THEN");
            sb.Append("          (SELECT (VT.VESSEL_ID || ' / ' || VT.VESSEL_NAME || ' / ' ||");
            sb.Append("                  VVT.VOYAGE)");
            sb.Append("             FROM VESSEL_VOYAGE_TBL VT, VESSEL_VOYAGE_TRN VVT");
            sb.Append("            WHERE VT.VESSEL_VOYAGE_TBL_PK = VVT.VESSEL_VOYAGE_TBL_FK");
            sb.Append("              AND VVT.VOYAGE_TRN_PK = CT.VOYAGE_TRN_FK)");
            sb.Append("         ELSE");
            sb.Append("          (SELECT (AMT.AIRLINE_ID || ' / ' || AMT.AIRLINE_NAME || ' / ' ||");
            sb.Append("                  CT.FLIGHT_NO)");
            sb.Append("             FROM AIRLINE_MST_TBL AMT");
            sb.Append("            WHERE CT.OPERATOR_MST_FK = AMT.AIRLINE_MST_PK)");
            sb.Append("       END VESSEL_VOYAGE,");
            sb.Append("       POL.PORT_ID POL,");
            sb.Append("       POD.PORT_ID POD,");
            sb.Append("       CMT.CUSTOMER_NAME,");
            sb.Append("       (SELECT Q.DD_ID");
            sb.Append("          FROM QFOR_DROP_DOWN_TBL Q");
            sb.Append("         WHERE Q.DD_VALUE = CT.CBJC_STATUS");
            sb.Append("           AND Q.DD_FLAG = 'JC_STATUS'");
            sb.Append("           AND Q.CONFIG_ID = 'QFOR4394') STATUS,");
            sb.Append("       (LOC.PROFIT_PERCENT || '%') LOCATION_PROFIT,");
            sb.Append("       (" + Percentage + " || '%')  SHIPMENT_PROFIT");
            sb.Append("  FROM CBJC_TBL         CT,");
            sb.Append("       CUSTOMER_MST_TBL CMT,");
            sb.Append("       PORT_MST_TBL     POL,");
            sb.Append("       PORT_MST_TBL     POD,");
            sb.Append("       USER_MST_TBL     UMT,");
            sb.Append("       lOCATION_MST_TBL LOC");
            sb.Append(" WHERE CT.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
            sb.Append("   AND CT.POL_MST_FK = POL.PORT_MST_PK(+)");
            sb.Append("   AND CT.POD_MST_FK = POD.PORT_MST_PK(+)");
            sb.Append("   AND CT.CREATED_BY_FK = UMT.USER_MST_PK");
            sb.Append("   AND UMT.DEFAULT_LOCATION_FK = LOC.LOCATION_MST_PK");
            if (Convert.ToInt64(lngPkValue) > 0)
            {
                sb.Append("   AND CT.CBJC_PK = " + lngPkValue);
            }
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion

        #region " Fetch TransporterNote Details"
        public DataSet FetchTransporterNoteDetails(string lngPkValue, string Percentage = "0")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT CT.TRANSPORT_INST_SEA_PK,");
            sb.Append("       CT.TRANS_INST_REF_NO,");
            sb.Append("       TO_CHAR(CT.TRANS_INST_DATE,'DD/MM/YYYY') TRANS_INST_DATE,");
            sb.Append("       'Transporter Note' REFERENCE_TYPE,");
            sb.Append("       DECODE(CT.BUSINESS_TYPE, 1, 'Air', 2, 'Sea') BIZ_TYPE,");
            sb.Append("       CASE");
            sb.Append("         WHEN CT.BUSINESS_TYPE = 2 THEN");
            sb.Append("       DECODE(CT.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC')");
            sb.Append("         ELSE");
            sb.Append("       DECODE(CT.CARGO_TYPE, 1, 'KGS', 2, 'ULD') ");
            sb.Append("       END CARGO_TYPE,");
            sb.Append("       CASE");
            sb.Append("         WHEN CT.BUSINESS_TYPE = 2 THEN");
            sb.Append("          (SELECT (VT.VESSEL_ID || ' / ' || VT.VESSEL_NAME || ' / ' ||");
            sb.Append("                  VVT.VOYAGE)");
            sb.Append("             FROM VESSEL_VOYAGE_TBL VT, VESSEL_VOYAGE_TRN VVT");
            sb.Append("            WHERE VT.VESSEL_VOYAGE_TBL_PK = VVT.VESSEL_VOYAGE_TBL_FK");
            sb.Append("              AND VVT.VOYAGE_TRN_PK = CT.VSL_VOY_FK)");
            sb.Append("         ELSE");
            sb.Append("          (SELECT (AMT.AIRLINE_ID || ' / ' || AMT.AIRLINE_NAME || ' / ' ||");
            sb.Append("                  CT.FLIGHT_NO)");
            sb.Append("             FROM AIRLINE_MST_TBL AMT");
            sb.Append("            WHERE CT.OPERATOR_MST_FK = AMT.AIRLINE_MST_PK)");
            sb.Append("       END VESSEL_VOYAGE,");
            sb.Append("     CASE  ");
            sb.Append("         WHEN CT.PROCESS_TYPE = 1 THEN ");
            sb.Append("      POL.PORT_ID  ");
            sb.Append("        ELSE  ");
            sb.Append("        ''  ");
            sb.Append("         END POL, ");
            sb.Append("     CASE  ");
            sb.Append("         WHEN CT.PROCESS_TYPE = 1 THEN ");
            sb.Append("      ''  ");
            sb.Append("        ELSE  ");
            sb.Append("        POD.PORT_ID  ");
            sb.Append("         END POD, ");
            sb.Append("       CMT.CUSTOMER_NAME,");
            sb.Append("       (SELECT Q.DD_ID");
            sb.Append("          FROM QFOR_DROP_DOWN_TBL Q");
            sb.Append("         WHERE Q.DD_VALUE = CT.TP_STATUS");
            sb.Append("           AND Q.DD_FLAG = 'JC_STATUS'");
            sb.Append("           AND Q.CONFIG_ID = 'QFOR4394') STATUS,");
            sb.Append("       (LOC.PROFIT_PERCENT || '%') LOCATION_PROFIT,");
            sb.Append("       (" + Percentage + " || '%')  SHIPMENT_PROFIT");
            sb.Append("  FROM TRANSPORT_INST_SEA_TBL         CT,");
            sb.Append("       CUSTOMER_MST_TBL CMT,");
            sb.Append("       PORT_MST_TBL     POL,");
            sb.Append("       PORT_MST_TBL     POD,");
            sb.Append("       USER_MST_TBL     UMT,");
            sb.Append("       lOCATION_MST_TBL LOC");
            sb.Append(" WHERE CT.TP_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
            sb.Append("   AND CT.POL_FK = POL.PORT_MST_PK(+)");
            sb.Append("   AND CT.POD_FK = POD.PORT_MST_PK(+)");
            sb.Append("   AND CT.CREATED_BY_FK = UMT.USER_MST_PK");
            sb.Append("   AND UMT.DEFAULT_LOCATION_FK = LOC.LOCATION_MST_PK");
            if (Convert.ToInt64(lngPkValue) > 0)
            {
                sb.Append("   AND CT.TRANSPORT_INST_SEA_PK = " + lngPkValue);
            }
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion

        #region " Fetch Jobcard Details"
        public DataSet FetchJobcardDetails(string lngPkValue, string Percentage = "0")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT CT.JOB_CARD_TRN_PK,");
            sb.Append("       CT.JOBCARD_REF_NO,");
            sb.Append("       TO_CHAR(CT.JOBCARD_DATE,'DD/MM/YYYY') JOBCARD_DATE,");
            sb.Append("       'JOBCARD' REFERENCE_TYPE,");
            sb.Append("       DECODE(CT.BUSINESS_TYPE, 1, 'AIR', 2, 'SEA') BIZ_TYPE,");
            sb.Append("       CASE");
            sb.Append("         WHEN CT.BUSINESS_TYPE = 2 THEN");
            sb.Append("       DECODE(CT.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC')");
            sb.Append("         ELSE");
            sb.Append("       DECODE(CT.CARGO_TYPE, 1, 'KGS', 2, 'ULD') ");
            sb.Append("       END CARGO_TYPE,");
            sb.Append("       CASE");
            sb.Append("         WHEN CT.BUSINESS_TYPE = 2 THEN");
            sb.Append("          (SELECT (VT.VESSEL_ID || ' / ' || VT.VESSEL_NAME || ' / ' ||");
            sb.Append("                  VVT.VOYAGE)");
            sb.Append("             FROM VESSEL_VOYAGE_TBL VT, VESSEL_VOYAGE_TRN VVT");
            sb.Append("            WHERE VT.VESSEL_VOYAGE_TBL_PK = VVT.VESSEL_VOYAGE_TBL_FK");
            sb.Append("              AND VVT.VOYAGE_TRN_PK = CT.VOYAGE_TRN_FK)");
            sb.Append("         ELSE");
            sb.Append("          (SELECT (AMT.AIRLINE_ID || ' / ' || AMT.AIRLINE_NAME || ' / ' ||");
            sb.Append("                  CT.VOYAGE_FLIGHT_NO)");
            sb.Append("             FROM AIRLINE_MST_TBL AMT");
            sb.Append("            WHERE CT.CARRIER_MST_FK = AMT.AIRLINE_MST_PK)");
            sb.Append("       END VESSEL_VOYAGE,");
            sb.Append("       POL.PORT_ID POL,");
            sb.Append("       POD.PORT_ID POD,");
            sb.Append("       CMT.CUSTOMER_NAME,");
            sb.Append("       CT.JOB_CARD_STATUS,");
            sb.Append("       (SELECT Q.DD_ID");
            sb.Append("          FROM QFOR_DROP_DOWN_TBL Q");
            sb.Append("         WHERE Q.DD_VALUE = CT.STATUS");
            sb.Append("           AND Q.DD_FLAG = 'JC_STATUS'");
            sb.Append("           AND Q.CONFIG_ID = 'QFOR4394') STATUS,");
            sb.Append("       (LOC.PROFIT_PERCENT || '%') LOCATION_PROFIT,");
            sb.Append("       (" + Percentage + " || '%')  SHIPMENT_PROFIT");
            sb.Append("  FROM JOB_CARD_TRN      CT,");
            sb.Append("       CUSTOMER_MST_TBL CMT,");
            sb.Append("       PORT_MST_TBL     POL,");
            sb.Append("       PORT_MST_TBL     POD,");
            sb.Append("       USER_MST_TBL     UMT,");
            sb.Append("       LOCATION_MST_TBL LOC");
            sb.Append(" WHERE CT.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
            sb.Append("   AND CT.PORT_MST_POL_FK = POL.PORT_MST_PK(+)");
            sb.Append("   AND CT.PORT_MST_POD_FK = POD.PORT_MST_PK(+)");
            sb.Append("   AND CT.CREATED_BY_FK = UMT.USER_MST_PK");
            sb.Append("   AND UMT.DEFAULT_LOCATION_FK = LOC.LOCATION_MST_PK");
            if (Convert.ToInt64(lngPkValue) > 0)
            {
                sb.Append("  AND CT.JOB_CARD_TRN_PK = " + lngPkValue);
            }
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion

        #region "FetchForeignAgentDetails "
        public DataSet FetchForeignAgentDetails(long lngPkValue = 0, long strpkvalue = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append("SELECT POL.PORT_ID POL,");
            sb.Append("       POD.PORT_ID POD,");
            sb.Append("       C.CUSTOMER_NAME THD_FRT_PAYER,");
            sb.Append("       (AGD.ADM_ADDRESS_1 || ',' || AGD.ADM_ADDRESS_2 || ',' || AGD.ADM_ADDRESS_3 || ',' || AGD.ADM_CITY || ','||AGD.ADM_PHONE_NO_1) ADDRESS,");
            sb.Append("       SUM(QF.QUOTED_RATE) \"FREIGHT AMOUNT\",");
            sb.Append("       Q.EXPECTED_SHIPMENT_DT \"SHIPMENT DATE\",");
            sb.Append("       (V.VESSEL_NAME || '/' || VT.VOYAGE) VOYAGE,");
            sb.Append("       B.ETD_DATE,");
            sb.Append("       B.ETA_DATE,");
            sb.Append("       AGT.AGENT_NAME,");
            sb.Append("       U.USER_ID");
            sb.Append("  FROM QUOTATION_MST_TBL     Q,");
            sb.Append("       BOOKING_MST_TBL       B,");
            sb.Append("       BOOKING_TRN           BT,");
            sb.Append("       QUOTATION_DTL_TBL     QT,");
            sb.Append("       QUOTATION_FREIGHT_TRN QF,");
            sb.Append("       PORT_MST_TBL          POL,");
            sb.Append("       PORT_MST_TBL          POD,");
            sb.Append("       VESSEL_VOYAGE_TBL     V,");
            sb.Append("       VESSEL_VOYAGE_TRN     VT,");
            sb.Append("       AGENT_MST_TBL         AGT,");
            sb.Append("       CUSTOMER_MST_TBL      C,");
            sb.Append("       AGENT_CONTACT_DTLS    AGD,");
            sb.Append("       USER_MST_TBL          U");
            sb.Append(" WHERE QT.QUOTATION_MST_FK = Q.QUOTATION_MST_PK");
            sb.Append("   AND QF.QUOTATION_DTL_FK = QT.QUOTE_DTL_PK");
            sb.Append("   AND QT.PORT_MST_POL_FK = POL.PORT_MST_PK");
            sb.Append("   AND QT.PORT_MST_POD_FK = POD.PORT_MST_PK");
            sb.Append("   AND B.VESSEL_VOYAGE_FK = VT.VOYAGE_TRN_PK(+)");
            sb.Append("   AND VT.VESSEL_VOYAGE_TBL_FK = V.VESSEL_VOYAGE_TBL_PK(+)");
            sb.Append("   AND BT.BOOKING_MST_FK = B.BOOKING_MST_PK(+)");
            sb.Append("   AND BT.TRANS_REF_NO(+) = Q.QUOTATION_REF_NO");
            sb.Append("   AND Q.TARIFF_AGENT_MST_FK = AGT.AGENT_MST_PK(+)");
            sb.Append("   AND AGD.AGENT_MST_FK = AGT.AGENT_MST_PK");
            sb.Append("   AND Q.THIRD_PARTY_FRTPAYER_FK = C.CUSTOMER_MST_PK");
            if (lngPkValue > 0)
            {
                sb.Append("   AND Q.QUOTATION_MST_PK = " + lngPkValue);
            }
            if (strpkvalue > 0)
            {
                sb.Append("   AND B.BOOKING_MST_PK = " + strpkvalue);
            }
            sb.Append("   AND QF.PYMT_TYPE = 3");
            sb.Append("   AND Q.CREATED_BY_FK = U.USER_MST_PK");
            sb.Append(" GROUP BY POL.PORT_ID,");
            sb.Append("          POD.PORT_ID,");
            sb.Append("          C.CUSTOMER_NAME,");
            sb.Append("          AGD.ADM_ADDRESS_1,");
            sb.Append("          AGD.ADM_ADDRESS_2,");
            sb.Append("          AGD.ADM_ADDRESS_3,");
            sb.Append("          AGD.ADM_CITY,");
            sb.Append("          AGD.ADM_PHONE_NO_1,");
            sb.Append("            AGD.ADM_EMAIL_ID,");
            sb.Append("          Q.EXPECTED_SHIPMENT_DT,");
            sb.Append("          V.VESSEL_NAME,");
            sb.Append("          VT.VOYAGE,");
            sb.Append("          B.ETD_DATE,");
            sb.Append("          B.ETA_DATE,");
            sb.Append("          AGT.AGENT_NAME,");
            sb.Append("          U.USER_ID");


            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion

        #region "FetchAirQuotation "
        public DataSet FetchAirQuotation(long lngPkValue)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append("SELECT QM.QUOTATION_REF_NO,");
            sb.Append("       TO_CHAR(QM.QUOTATION_DATE, DATEFORMAT) QUOT_DATE,");
            sb.Append("       TO_CHAR(QM.EXPECTED_SHIPMENT_DT, DATEFORMAT) SHIPMENT_DT,");
            sb.Append("       DECODE(QM.STATUS, 1, 'ACTIVE', 2, 'CONFIRM', 3, 'CANCELLED', 5, 'Req. Revision') STATUS,");
            sb.Append("       CM.CUSTOMER_NAME");
            sb.Append("  FROM QUOTATION_MST_TBL QM, CUSTOMER_MST_TBL CM");
            sb.Append(" WHERE CM.CUSTOMER_MST_PK = QM.CUSTOMER_MST_FK");
            sb.Append("   AND QM.QUOTATION_MST_PK = " + lngPkValue);
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion

        #region "FetchSeaQuotation "
        public DataSet FetchSeaQuotation(long lngPkValue)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append("SELECT QM.QUOTATION_REF_NO,");
            sb.Append("       TO_CHAR(QM.QUOTATION_DATE, DATEFORMAT) QUOT_DATE,");
            sb.Append("       TO_CHAR(QM.EXPECTED_SHIPMENT_DT, DATEFORMAT) SHIPMENT_DT,");
            sb.Append("       TO_CHAR(QM.QUOTATION_DATE + QM.VALID_FOR) VALID_TILL,");
            sb.Append("       DECODE(QM.STATUS, 1, 'ACTIVE', 2, 'CONFIRM', 3, 'CANCELLED', 4, 'USED' , 5, 'Req. for Revision') STATUS,");
            sb.Append("       CM.CUSTOMER_NAME,");
            sb.Append("       DECODE(QM.CARGO_TYPE, 1, 'FCL', 2, 'LCL',4,'BBC') CARGO_TYPE");
            sb.Append("  FROM QUOTATION_MST_TBL QM, CUSTOMER_MST_TBL CM");
            sb.Append(" WHERE CM.CUSTOMER_MST_PK = QM.CUSTOMER_MST_FK");
            sb.Append("   AND QM.QUOTATION_MST_PK = " + lngPkValue);
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion

        #region "FetchSeaCustContract "
        public DataSet FetchSeaCustContract(long lngPkValue)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append("SELECT HDR.CONT_REF_NO,");
            sb.Append("       TO_CHAR(HDR.CONT_DATE, 'DD/MM/YYYY') CONTRACT_DATE,");
            sb.Append("       DECODE(HDR.CARGO_TYPE, 1, 'FCL', 2, 'LCL') CARGO_TYPE,");
            sb.Append("       TO_CHAR(HDR.VALID_FROM, DATEFORMAT) VALID_FROM,");
            sb.Append("       TO_CHAR(HDR.VALID_TO, DATEFORMAT) VALID_TO,");
            sb.Append("       DECODE(HDR.APP_STATUS, 0, 'REQUESTED', 1, 'APPROVED', 2, 'REJECTED') STATUS,");
            sb.Append("       TARIFF.TARIFF_REF_NO,");
            sb.Append("       CUST.CUSTOMER_NAME");
            sb.Append("  FROM CONT_CUST_SEA_TBL   HDR,");
            sb.Append("       TARIFF_MAIN_SEA_TBL TARIFF,");
            sb.Append("       CUSTOMER_MST_TBL    CUST");
            sb.Append(" WHERE HDR.TARIFF_MAIN_SEA_FK = TARIFF.TARIFF_MAIN_SEA_PK(+)");
            sb.Append("   AND HDR.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK");
            sb.Append("   AND HDR.CONT_CUST_SEA_PK = " + lngPkValue);
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion

        #region "FetchAirCustContract "
        public DataSet FetchAirCustContract(long lngPkValue)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append("SELECT HDR.CONT_REF_NO,");
            sb.Append("       TO_CHAR(HDR.CONT_DATE, 'DD/MM/YYYY') CONTRACT_DATE,");
            sb.Append("       TO_CHAR(HDR.VALID_FROM, 'DD/MM/YYYY') VALID_FROM,");
            sb.Append("       TO_CHAR(HDR.VALID_TO, 'DD/MM/YYYY') VALID_TO,");
            sb.Append("       DECODE(HDR.CONT_APPROVED,");
            sb.Append("              0,");
            sb.Append("              'REQUESTED',");
            sb.Append("              1,");
            sb.Append("              'APPROVED',");
            sb.Append("              2,");
            sb.Append("              'REJECTED') STATUS,");
            sb.Append("       TARIFF.TARIFF_REF_NO,");
            sb.Append("       CUST.CUSTOMER_NAME");
            sb.Append("  FROM CONT_CUST_AIR_TBL   HDR,");
            sb.Append("       TARIFF_MAIN_AIR_TBL TARIFF,");
            sb.Append("       CUSTOMER_MST_TBL    CUST");
            sb.Append(" WHERE HDR.TARIFF_MAIN_AIR_FK = TARIFF.TARIFF_MAIN_AIR_PK(+)");
            sb.Append("   AND HDR.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK");
            sb.Append("   AND HDR.CONT_CUST_AIR_PK = " + lngPkValue);
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion

        #region "FetchSalesPlan "
        public DataSet FetchSalesPlan(long lngPkValue, System.DateTime saledate, long strpk)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append("SELECT  e.employee_name,");
            sb.Append("       CMT.CUSTOMER_NAME,");
            sb.Append("        SL.FR_TIME,");
            sb.Append("        SL.TO_TIME,");
            sb.Append("       DECODE(SL.PLAN_STATUS, 1, 'PENDING', 2, 'APPROVED', 3, 'REJECTED') STATUS,");
            sb.Append("       SL.CUSTOMER_MST_FK");
            sb.Append("  FROM sales_call_trn   SL,");
            sb.Append("          customer_mst_tbl CMT,");
            sb.Append("          user_mst_tbl     usr,");
            sb.Append("          employee_mst_tbl e");
            sb.Append(" WHERE  SL.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
            sb.Append("  AND e.employee_mst_pk = usr.employee_mst_fk");
            if ((strpk > 0))
            {
                sb.Append(" AND SL.SALES_CALL_PK=" + strpk);
            }
            else
            {
            }
            sb.Append("  AND usr.user_mst_pk = " + lngPkValue);
            sb.Append("  AND sl.sales_call_dt='" + saledate + "'");

            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion

        #region "FetchVoucher "
        public DataSet FetchVoucher(long lngPkValue)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("select INV.INVOICE_REF_NO VCHR_NR,");
            sb.Append("       TO_CHAR(INV.INVOICE_DATE, 'DD/MM/YYYY') VCHR_DT,");
            sb.Append("        TO_CHAR(INV.supplier_due_dt,'DD/MM/YYYY')Due_DT,");
            sb.Append("        INV.INVOICE_DATE,");
            sb.Append("       VENDOR_NAME SUPP_NAME,");
            sb.Append("       DECODE(INV.BUSINESS_TYPE, 1, 'AIR', 2, 'SEA') BIZ_TYPE,");
            sb.Append("       DECODE(INV.PROCESS_TYPE, 1, 'EXPORT', 2, 'IMPORT') PROCESS_TYPE,");
            sb.Append("       DECODE(INV.APPROVED, 0, 'PENDING', 1, 'APPROVED', 2, 'REJECTED') STATUS,");
            sb.Append("       INV.SUPPLIER_INV_NO");
            sb.Append("  from INV_SUPPLIER_TBL INV, VENDOR_MST_TBL VMT");
            sb.Append(" WHERE VMT.VENDOR_MST_PK = INV.VENDOR_MST_FK");
            sb.Append("   AND INV.INV_SUPPLIER_PK = " + lngPkValue);

            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion

        #region "FetchSLContract "
        public DataSet FetchSLContract(long lngPkValue)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();

            sb.Append("       SELECT CMS.CONTRACT_NO,");
            sb.Append("       OMT.OPERATOR_NAME,");
            sb.Append("       TO_CHAR(CMS.CONTRACT_DATE, 'DD/MM/YYYY') CONTRACT_DATE,");
            sb.Append("       ");
            sb.Append("       DECODE(CMS.CARGO_TYPE, 1, 'FCL', 2, 'LCL') CARGO_TYPE,");
            sb.Append("       TO_CHAR(CMS.VALID_FROM) VALID_FROM,");
            sb.Append("       TO_CHAR(CMS.VALID_TO) VALID_TO,");
            sb.Append("       POL.PORT_NAME POL,");
            sb.Append("       POD.PORT_NAME POD,");
            sb.Append("       DECODE(CMS.WORKFLOW_STATUS,");
            sb.Append("              0,");
            sb.Append("              'REQUESTED',");
            sb.Append("              1,");
            sb.Append("              'APPROVED',");
            sb.Append("              2,");
            sb.Append("              'REJECTED') STATUS");
            sb.Append("");
            sb.Append("       FROM CONT_MAIN_SEA_TBL    CMS,");
            sb.Append("       OPERATOR_MST_TBL     OMT,");
            sb.Append("       CONT_TRN_SEA_FCL_LCL CTS,");
            sb.Append("       PORT_MST_TBL         POL,");
            sb.Append("       PORT_MST_TBL         POD");
            sb.Append("");
            sb.Append("   WHERE CMS.OPERATOR_MST_FK = OMT.OPERATOR_MST_PK ");
            sb.Append("   AND CMS.CONT_MAIN_SEA_PK = CTS.CONT_MAIN_SEA_FK");
            sb.Append("   AND CTS.PORT_MST_POL_FK = POL.PORT_MST_PK");
            sb.Append("   AND CTS.PORT_MST_POD_FK = POD.PORT_MST_PK");
            sb.Append("   AND CMS.CONT_MAIN_SEA_PK = " + lngPkValue);
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion

        #region "FetchAirLineContract "
        public DataSet FetchAirLineContract(long lngPkValue)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();

            sb.Append("       SELECT CMT.CONTRACT_NO,");
            sb.Append("       TO_CHAR(CMT.CONTRACT_DATE, 'DD/MM/YYYY') CONTRACT_DATE,");
            sb.Append("       AMT.AIRLINE_NAME,");
            sb.Append("       TO_CHAR(CMT.VALID_FROM, 'DD/MM/YYYY') VALID_FROM,");
            sb.Append("       TO_CHAR(CMT.VALID_TO, 'DD/MM/YYYY') VALID_TO,");
            sb.Append("       POL.PORT_NAME POL,");
            sb.Append("       POD.PORT_NAME POD,");
            sb.Append("       DECODE(cmt.cont_approved,");
            sb.Append("              0,");
            sb.Append("              'REQUESTED',");
            sb.Append("              1,");
            sb.Append("              'APPROVED',");
            sb.Append("              2,");
            sb.Append("              'REJECTED') STATUS");
            sb.Append("");
            sb.Append("       FROM CONT_MAIN_AIR_TBL CMT,");
            sb.Append("       CONT_TRN_AIR_LCL  CTA,");
            sb.Append("       AIRLINE_MST_TBL   AMT,");
            sb.Append("       PORT_MST_TBL      POL,");
            sb.Append("       PORT_MST_TBL      POD");
            sb.Append("       WHERE CMT.AIRLINE_MST_FK = AMT.AIRLINE_MST_PK");
            sb.Append("       AND CMT.CONT_MAIN_AIR_PK = CTA.CONT_MAIN_AIR_FK");
            sb.Append("       AND CTA.PORT_MST_POL_FK = POL.PORT_MST_PK");
            sb.Append("       AND CTA.PORT_MST_POD_FK = POD.PORT_MST_PK");
            sb.Append("       AND CMT.CONT_MAIN_AIR_PK = " + lngPkValue);

            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion

        #region "FetchWareHouseContract "
        public DataSet FetchWareHouseContract(long lngPkValue)
        {

            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append("       SELECT CMD.CONTRACT_NO,");
            sb.Append("       TO_CHAR(CMD.CONTRACT_DATE, 'DD/MM/YYYY') CONTRACT_DATE,");
            sb.Append("       VM.VENDOR_NAME,");
            sb.Append("       CMD.VALID_FROM,");
            sb.Append("       CMD.VALID_TO,");
            sb.Append("       DECODE(CMD.BUSINESS_TYPE,1,'AIR',2,'SEA') BIZ_TYPE,");
            sb.Append("       DECODE(CMD.CARGO_TYPE,1,'FCL',2,'LCL') CARGO_TYPE,");
            sb.Append("       DECODE(CMD.WORKFLOW_STATUS,");
            sb.Append("              0,");
            sb.Append("              'REQUESTED',");
            sb.Append("              1,");
            sb.Append("              'APPROVED',");
            sb.Append("              2,");
            sb.Append("              'REJECTED') STATUS");
            sb.Append("");
            sb.Append("   FROM CONT_MAIN_DEPOT_TBL CMD, VENDOR_MST_TBL VM");
            sb.Append("   WHERE VM.VENDOR_MST_PK = CMD.DEPOT_MST_FK");
            sb.Append("   AND CMD.CONT_MAIN_DEPOT_PK = " + lngPkValue);

            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion

        #region "FetchTransPortContract "
        public DataSet FetchTransPortContract(long lngPkValue)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();

            sb.Append("       SELECT MAIN.CONTRACT_NO,");
            sb.Append("       TO_CHAR(MAIN.CONTRACT_DATE, 'DD/MM/YYYY') CONTRACT_DATE,");
            sb.Append("       VMT.VENDOR_NAME,");
            sb.Append("       TO_CHAR(MAIN.VALID_FROM, 'DD/MM/YYYY') VALID_FROM,");
            sb.Append("       TO_CHAR(MAIN.VALID_TO, 'DD/MM/YYYY') VALID_TO,");
            sb.Append("       'SEA' BIZ_TYPE,");
            sb.Append("       'FCL' CARGO_TYPE,");
            sb.Append("       DECODE(MAIN.CONT_STATUS,");
            sb.Append("              0,");
            sb.Append("              'REQUESTED',");
            sb.Append("              1,");
            sb.Append("              'APPROVED',");
            sb.Append("              2,");
            sb.Append("              'REJECTED') STATUS");
            sb.Append("");
            sb.Append("  FROM CONT_TRANS_FCL_TBL MAIN, VENDOR_MST_TBL VMT");
            sb.Append("");
            sb.Append(" WHERE MAIN.TRANSPORTER_MST_FK = VMT.VENDOR_MST_PK");
            sb.Append("   AND MAIN.CONT_TRANS_FCL_PK = " + lngPkValue);

            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion

        #region "FetchSrrContractSea "
        public DataSet FetchSrrContractSea(long lngPkValue)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append("select SRS.SRR_REF_NO SRR_REfNR,");
            sb.Append("Srs.Srr_Date SRR_DATE,");
            sb.Append("Srs.Customer_Mst_Fk SRR_CUST_FK,");
            sb.Append("srs.valid_from VALID_FROM ,");
            sb.Append("srs.valid_to VALID_TO,");
            sb.Append("DECODE(srs.CARGO_TYPE, 1, 'FCL', 2, 'LCL') CARGO_TYPE,");
            sb.Append("cust.customer_name CUST_NAME,");
            sb.Append("DECODE(srs.status,0, 'Requested', 1,'Approved', 2,'Rejected')STATUS");
            sb.Append("");
            sb.Append("   From Srr_Sea_Tbl srs,customer_mst_tbl cust");
            sb.Append("");
            sb.Append(" WHERE Srs.Customer_Mst_Fk=cust.customer_mst_pk");
            sb.Append(" AND Srs.Srr_Sea_Pk = " + lngPkValue);

            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }

        }
        #endregion

        #region "FetchSrrContractAir "
        public DataSet FetchSrrContractAir(long lngPkValue)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append("select SRS.SRR_REF_NO SRR_REfNR,");
            sb.Append("TO_CHAR(Srs.Srr_Date,'DD/MM/YYYY') SRR_DATE,");
            sb.Append("Srs.Customer_Mst_Fk SRR_CUST_FK,");
            sb.Append("srs.valid_from VALID_FROM ,");
            sb.Append("srs.valid_to VALID_TO,");
            sb.Append("cust.customer_name CUST_NAME,");
            sb.Append("DECODE(srs.SRR_APPROVED,0, 'Requested', 1,'Approved', 2,'Rejected')STATUS");
            sb.Append("");
            sb.Append("   From Srr_Air_Tbl srs,customer_mst_tbl cust");
            sb.Append("");
            sb.Append(" WHERE Srs.Customer_Mst_Fk=cust.customer_mst_pk");
            sb.Append(" AND Srs.Srr_Air_Pk = " + lngPkValue);

            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }

        }
        #endregion

        #region "FetchTransportContractAir "
        public DataSet FetchTransportContractAir(long lngPkValue)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append("        SELECT MAIN.CONTRACT_NO,");
            sb.Append("       TO_CHAR(MAIN.CONTRACT_DATE, 'DD/MM/YYYY') CONTRACT_DATE,");
            sb.Append("       VMT.VENDOR_NAME,");
            sb.Append("       TO_CHAR(MAIN.VALID_FROM, 'DD/MM/YYYY') VALID_FROM,");
            sb.Append("       TO_CHAR(MAIN.VALID_TO, 'DD/MM/YYYY') VALID_TO,");
            sb.Append("       DECODE(MAIN.BUSINESS_TYPE, 2, 'SEA', 1, 'AIR') BIZ_TYPE,");
            sb.Append("       CASE");
            sb.Append("         WHEN MAIN.BUSINESS_TYPE = 2 THEN");
            sb.Append("          'LCL'");
            sb.Append("         ELSE");
            sb.Append("          ''");
            sb.Append("       END CARGO_TYPE,");
            sb.Append("       DECODE(MAIN.WORKFLOW_STATUS,");
            sb.Append("              0,");
            sb.Append("              'REQUESTED',");
            sb.Append("              1,");
            sb.Append("              'APPROVED',");
            sb.Append("              2,");
            sb.Append("              'REJECTED') STATUS");
            sb.Append("");
            sb.Append("  FROM CONT_MAIN_TRANS_TBL MAIN, VENDOR_MST_TBL VMT");
            sb.Append("  WHERE MAIN.TRANSPORTER_MST_FK = VMT.VENDOR_MST_PK");
            sb.Append("  AND MAIN.CONT_MAIN_TRANS_PK = " + lngPkValue);
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion

        #region "Fetch Doc Details by Activity"
        private DataSet FetchDocDetails(string ActivityName, string DocRefNr)
        {
            DataSet dsDoc = new DataSet();
            WorkFlow objWf = new WorkFlow();
            objWf.OpenConnection();
            objWf.MyCommand = new OracleCommand();
            var _with10 = objWf.MyCommand;
            _with10.Connection = objWf.MyConnection;
            _with10.Parameters.Clear();
            _with10.Parameters.Add("ACTIVITY_NAME_IN", ActivityName).Direction = ParameterDirection.Input;
            _with10.Parameters.Add("DOC_REF_IN", DocRefNr).Direction = ParameterDirection.Input;
            _with10.Parameters.Add("FETCH_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
            try
            {
                dsDoc = objWf.GetDataSet("WORKFLOW_RULES_ENTRY_PKG", "FETCH_DOC_DETAILS");
            }
            catch (Exception ex)
            {
            }
            return dsDoc;
        }
        #endregion

        #region "FetchDocDetailsForProfitMargin "
        public DataSet FetchDocDetailsForProfitMargin(long lngPkValue)
        {
            DataSet dsPM = new DataSet();
            WorkFlow objWF = new WorkFlow();
            objWF.OpenConnection();

            try
            {
                objWF.MyCommand = new OracleCommand();
                var _with11 = objWF.MyCommand;
                _with11.Connection = objWF.MyConnection;
                _with11.Parameters.Clear();
                _with11.Parameters.Add("REF_FLAG_IN", HttpContext.Current.Session["REF_TYPE_FLAG"]).Direction = ParameterDirection.Input;
                _with11.Parameters.Add("REF_PK_IN", lngPkValue).Direction = ParameterDirection.Input;
                _with11.Parameters.Add("LOCATION_MST_FK_IN", HttpContext.Current.Session["LOGED_IN_LOC_FK"]).Direction = ParameterDirection.Input;
                _with11.Parameters.Add("FETCH_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dsPM = objWF.GetDataSet("QUOTATION_PROFITABILITY_PKG", "FETCH_DOC_DETAILS_WORKFLOW");
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
            finally
            {
                objWF.CloseConnection();
            }
            return dsPM;
        }
        #endregion

        #region "FetchCallReg "
        public DataSet FetchCallReg(long lngPkValue)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();

            sb.Append("SELECT T.CS_REF_NO,");
            sb.Append("       TO_CHAR(T.CS_DT,DATEFORMAT) CS_DT,");
            sb.Append("       T.CALL_TYPE,");
            sb.Append("       DECODE(T.STATUS, 0, 'Open', 1, 'Close') STATUS,");
            sb.Append("       CMT.CUSTOMER_NAME,");
            sb.Append("       POL.PORT_NAME POL,");
            sb.Append("       POD.PORT_NAME POD,");
            sb.Append("       DECODE(T.BIZ_TYPE, 1, 'Air', 2, 'Sea') BIZ_TYPE,");
            sb.Append("       DECODE(T.PROCESS, 1, 'Export', 2, 'Import') PROCESS");
            sb.Append("  FROM CUSTOMER_SERVICE_TRN T, CUSTOMER_MST_TBL CMT,");
            sb.Append("  JOB_CARD_SEA_EXP_TBL JSE,BOOKING_MST_TBL  BST,PORT_MST_TBL  POL,PORT_MST_TBL  POD");
            sb.Append(" WHERE T.CUSTOMER_SERVICE_PK = " + lngPkValue);
            sb.Append(" AND CMT.CUSTOMER_MST_PK = T.CUSTOMER_MST_FK");
            sb.Append(" AND T.JOB_CARD_FK = JSE.JOB_CARD_SEA_EXP_PK ");
            // sb.Append(" AND BST.BOOKING_MST_PK = JSE.BOOKING_MST_FK ") JSE.BOOKING_SEA_FK
            sb.Append(" AND BST.BOOKING_MST_PK = JSE.BOOKING_SEA_FK ");
            sb.Append(" AND BST.PORT_MST_POL_FK = POL.PORT_MST_PK ");
            sb.Append(" AND BST.PORT_MST_POD_FK = POD.PORT_MST_PK");

            sb.Append(" UNION SELECT T.CS_REF_NO,");
            sb.Append("       TO_CHAR(T.CS_DT,DATEFORMAT) CS_DT,");
            sb.Append("       T.CALL_TYPE,");
            sb.Append("       DECODE(T.STATUS, 0, 'Open', 1, 'Close') STATUS,");
            sb.Append("       CMT.CUSTOMER_NAME,");
            sb.Append("       POL.PORT_NAME POL,");
            sb.Append("       POD.PORT_NAME POD,");
            sb.Append("       DECODE(T.BIZ_TYPE, 1, 'Air', 2, 'Sea') BIZ_TYPE,");
            sb.Append("       DECODE(T.PROCESS, 1, 'Export', 2, 'Import') PROCESS");
            sb.Append("  FROM CUSTOMER_SERVICE_TRN T, CUSTOMER_MST_TBL CMT,");
            sb.Append("  JOB_CARD_SEA_IMP_TBL JSI,PORT_MST_TBL  POL,PORT_MST_TBL  POD");
            sb.Append(" WHERE T.CUSTOMER_SERVICE_PK = " + lngPkValue);
            sb.Append(" AND CMT.CUSTOMER_MST_PK = T.CUSTOMER_MST_FK");
            sb.Append(" AND T.JOB_CARD_FK = JSI.JOB_CARD_SEA_IMP_PK ");
            sb.Append(" AND JSI.PORT_MST_POL_FK = POL.PORT_MST_PK ");
            sb.Append(" AND JSI.PORT_MST_POD_FK = POD.PORT_MST_PK ");

            sb.Append(" UNION SELECT T.CS_REF_NO,");
            sb.Append("       TO_CHAR(T.CS_DT,DATEFORMAT) CS_DT,");
            sb.Append("       T.CALL_TYPE,");
            sb.Append("       DECODE(T.STATUS, 0, 'Open', 1, 'Close') STATUS,");
            sb.Append("       CMT.CUSTOMER_NAME,");
            sb.Append("       POL.PORT_NAME POL,");
            sb.Append("       POD.PORT_NAME POD,");
            sb.Append("       DECODE(T.BIZ_TYPE, 1, 'Air', 2, 'Sea') BIZ_TYPE,");
            sb.Append("       DECODE(T.PROCESS, 1, 'Export', 2, 'Import') PROCESS");
            sb.Append("  FROM CUSTOMER_SERVICE_TRN T, CUSTOMER_MST_TBL CMT,");
            sb.Append("   JOB_CARD_AIR_EXP_TBL JAE,BOOKING_MST_TBL  BAT,PORT_MST_TBL  POL,PORT_MST_TBL  POD");
            sb.Append(" WHERE T.CUSTOMER_SERVICE_PK = " + lngPkValue);
            sb.Append(" AND CMT.CUSTOMER_MST_PK = T.CUSTOMER_MST_FK");
            sb.Append(" AND T.JOB_CARD_FK = JAE.JOB_CARD_AIR_EXP_PK");
            //  sb.Append(" AND BAT.BOOKING_MST_PK = JAE.BOOKING_MST_FK ") BOOKING_AIR_FK
            sb.Append(" AND BAT.BOOKING_MST_PK = JAE.BOOKING_AIR_FK ");
            sb.Append(" AND BAT.PORT_MST_POL_FK = POL.PORT_MST_PK ");
            sb.Append(" AND BAT.PORT_MST_POD_FK = POD.PORT_MST_PK ");

            sb.Append(" UNION SELECT T.CS_REF_NO,");
            sb.Append("       TO_CHAR(T.CS_DT,DATEFORMAT) CS_DT,");
            sb.Append("       T.CALL_TYPE,");
            sb.Append("       DECODE(T.STATUS, 0, 'Open', 1, 'Close') STATUS,");
            sb.Append("       CMT.CUSTOMER_NAME,");
            sb.Append("       POL.PORT_NAME POL,");
            sb.Append("       POD.PORT_NAME POD,");
            sb.Append("       DECODE(T.BIZ_TYPE, 1, 'Air', 2, 'Sea') BIZ_TYPE,");
            sb.Append("       DECODE(T.PROCESS, 1, 'Export', 2, 'Import') PROCESS");
            sb.Append("  FROM CUSTOMER_SERVICE_TRN T, CUSTOMER_MST_TBL CMT,");
            sb.Append("   JOB_CARD_AIR_IMP_TBL JAI,PORT_MST_TBL  POL,PORT_MST_TBL  POD");
            sb.Append(" WHERE T.CUSTOMER_SERVICE_PK = " + lngPkValue);
            sb.Append(" AND CMT.CUSTOMER_MST_PK = T.CUSTOMER_MST_FK");
            sb.Append(" AND T.JOB_CARD_FK = JAI.JOB_CARD_AIR_IMP_PK ");
            sb.Append(" AND JAI.PORT_MST_POL_FK = POL.PORT_MST_PK ");
            sb.Append(" AND JAI.PORT_MST_POD_FK = POD.PORT_MST_PK ");
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion

        #region "FetchSeaTariff "
        public DataSet FetchSeaTariff(long lngPkValue)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append("SELECT RSR.TARIFF_REF_NO,");
            sb.Append("       TO_CHAR(RSR.TARIFF_DATE, 'DD/MM/YYYY') TARIFF_DATE,");
            sb.Append("       TO_CHAR(RSR.VALID_FROM) VALID_FROM,");
            sb.Append("       TO_CHAR(RSR.VALID_TO) VALID_TO,");
            sb.Append("       DECODE(RSR.STATUS, 0, 'REQUESTED', 1, 'APPROVED', 2, 'REJECTED') STATUS,");
            sb.Append("       DECODE(RSR.CARGO_TYPE, 1, 'FCL', 2, 'LCL') CARGO_TYPE,");
            sb.Append("       CNT.CONTRACT_NO,");
            sb.Append("       OPR.OPERATOR_NAME");
            sb.Append("  FROM TARIFF_MAIN_SEA_TBL RSR,");
            sb.Append("       CONT_MAIN_SEA_TBL     CNT,");
            sb.Append("       OPERATOR_MST_TBL      OPR");
            sb.Append(" WHERE CNT.CONT_MAIN_SEA_PK(+) = RSR.CONT_MAIN_SEA_FK");
            sb.Append("   AND OPR.OPERATOR_MST_PK(+) = RSR.OPERATOR_MST_FK");
            sb.Append("   AND RSR.TARIFF_MAIN_SEA_PK = " + lngPkValue);
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion

        #region "FetchSeaTariff "
        public DataSet FetchAIRTariff(long lngPkValue)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append("SELECT RSR.TARIFF_REF_NO,");
            sb.Append("       TO_CHAR(RSR.TARIFF_DATE, 'DD/MM/YYYY') TARIFF_DATE,");
            sb.Append("       TO_CHAR(RSR.VALID_FROM) VALID_FROM,");
            sb.Append("       TO_CHAR(RSR.VALID_TO) VALID_TO,");
            sb.Append("       DECODE(RSR.STATUS, 0, 'REQUESTED', 1, 'APPROVED', 2, 'REJECTED') STATUS,");
            sb.Append("       CNT.CONTRACT_NO,");
            sb.Append("       OPR.AIRLINE_NAME");
            sb.Append("  FROM TARIFF_MAIN_AIR_TBL RSR,");
            sb.Append("       TARIFF_TRN_AIR_TBL RST,");
            sb.Append("       CONT_MAIN_AIR_TBL     CNT,");
            sb.Append("       AIRLINE_MST_TBL      OPR");
            sb.Append(" WHERE RST.TARIFF_MAIN_AIR_FK=RSR.TARIFF_MAIN_AIR_PK");
            sb.Append("   AND CNT.CONT_MAIN_AIR_PK(+) = RST.CONT_MAIN_AIR_FK");
            sb.Append("   AND OPR.AIRLINE_MST_PK(+) = RSR.AIRLINE_MST_FK");
            sb.Append("   AND RSR.TARIFF_MAIN_AIR_PK = " + lngPkValue);
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion

        #region "FetchSEARFQ "
        public DataSet FetchRFQ(long lngPkValue)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append("SELECT RSR.RFQ_REF_NO,");
            sb.Append("       CMT.CUSTOMER_NAME,");
            sb.Append("       TO_CHAR(RSR.RFQ_DATE, 'DD/MM/YYYY') RFQ_DATE,");
            sb.Append("       TO_CHAR(RSR.VALID_FROM) VALID_FROM,");
            sb.Append("       TO_CHAR(RSR.VALID_TO) VALID_TO,");
            sb.Append("       DECODE(RSR.APPROVED, 0, 'REQUESTED', 1, 'APPROVED', 2, 'REJECTED') STATUS,");
            sb.Append("       DECODE(RSR.CARGO_TYPE, 1, 'FCL', 2, 'LCL') CARGO_TYPE,");
            sb.Append("       CNT.CONTRACT_NO,");
            sb.Append("       OPR.OPERATOR_NAME");
            sb.Append("  FROM RFQ_SPOT_RATE_SEA_TBL RSR,");
            sb.Append("       CUSTOMER_MST_TBL      CMT,");
            sb.Append("       CONT_MAIN_SEA_TBL     CNT,");
            sb.Append("       OPERATOR_MST_TBL      OPR");
            sb.Append(" WHERE CMT.CUSTOMER_MST_PK = RSR.CUSTOMER_MST_FK");
            sb.Append("   AND CNT.CONT_MAIN_SEA_PK = RSR.CONT_MAIN_SEA_FK");
            sb.Append("   AND OPR.OPERATOR_MST_PK = RSR.OPERATOR_MST_FK");
            sb.Append("   AND RSR.RFQ_SPOT_SEA_PK = " + lngPkValue);
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion

        #region "FetchAIRRFQ "
        public DataSet FetchAIRRFQ(long lngPkValue)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append("SELECT RSR.RFQ_REF_NO,");
            sb.Append("       CMT.CUSTOMER_NAME,");
            sb.Append("       TO_CHAR(RSR.RFQ_DATE, 'DD/MM/YYYY') RFQ_DATE,");
            sb.Append("       TO_CHAR(RSR.VALID_FROM) VALID_FROM,");
            sb.Append("       TO_CHAR(RSR.VALID_TO) VALID_TO,");
            sb.Append("       DECODE(RSR.APPROVED, 0, 'REQUESTED', 1, 'APPROVED', 2, 'REJECTED') STATUS,");
            sb.Append("       CNT.CONTRACT_NO,");
            sb.Append("       OPR.AIRLINE_NAME");
            sb.Append("  FROM RFQ_SPOT_RATE_AIR_TBL RSR,");
            sb.Append("       CUSTOMER_MST_TBL      CMT,");
            sb.Append("       CONT_MAIN_AIR_TBL     CNT,");
            sb.Append("       AIRLINE_MST_TBL      OPR");
            sb.Append(" WHERE CMT.CUSTOMER_MST_PK(+) = RSR.CUSTOMER_MST_FK");
            sb.Append("   AND CNT.CONT_MAIN_AIR_PK = RSR.CONT_MAIN_AIR_FK");
            sb.Append("   AND OPR.AIRLINE_MST_PK = RSR.AIRLINE_MST_FK");
            sb.Append("   AND RSR.RFQ_SPOT_AIR_PK = " + lngPkValue);
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion

        #region "Fetch Ecomm Details"
        public DataSet FetchECOMMDetails(string lngPkValue, string Userid)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append("SELECT CUST_REG.REGN_NR_PK,");
            sb.Append("       CUST_REG.COMPANY_NAME,");
            sb.Append("       LMT.LOCATION_NAME,");
            sb.Append("       CUST_PWD.USER_ID,");
            sb.Append("       CUST_PWD.CUST_PWD,");
            sb.Append("       CUST_PWD.EMAIL_ID,");
            sb.Append("       TO_CHAR(CUST_REG.CREATED_DATE, 'dd/MM/yyyy') RECDATE,");
            sb.Append("       CUST_PWD.REJECT_REMARKS,");
            sb.Append("       DECODE(CUST_PWD.STATUS,'1','Approved',2,'Rejected') STATUS");
            sb.Append("  FROM SYN_EBK_M_CUST_REGN     CUST_REG,");
            sb.Append("       SYN_EBK_T_CUST_ADDRESS  CUST_ADD,");
            sb.Append("       SYN_QBSO_EBK_M_CUST_PWD CUST_PWD,");
            sb.Append("       LOCATION_MST_TBL        LMT");

            sb.Append(" WHERE CUST_REG.REGN_NR_PK = CUST_ADD.REGN_NR_FK");
            sb.Append("   AND CUST_REG.REGN_NR_PK = CUST_PWD.REGN_NR_FK");
            sb.Append("   AND CUST_REG.LOCATION_FK = LMT.LOCATION_MST_PK");
            sb.Append("   AND CUST_PWD.USER_ID IS NOT NULL");
            sb.Append("   AND CUST_REG.REGN_NR_PK = '" + lngPkValue + "'");
            sb.Append("   AND CUST_PWD.USER_ID = '" + Userid + "'");
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion

        #region "FetchPayment "
        public DataSet FetchPayment(long lngPkValue)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append("select PAY.PAYMENT_REF_NO,PAY.PAYMENT_DATE,");
            sb.Append("       PAY.CURRENCY_MST_FK,PAY.VENDOR_MST_FK,");
            sb.Append("       VMT.VENDOR_NAME,CURR.CURRENCY_NAME,");
            sb.Append("      DECODE(PAY.BUSINESS_TYPE, 1, 'AIR', 2, 'SEA') BIZ_TYPE,");
            sb.Append("       INV.INVOICE_REF_NO,");
            sb.Append("      DECODE(PAY.PROCESS_TYPE, 1, 'EXPORT', 2, 'IMPORT') PROCESS_TYPE,");
            sb.Append("      DECODE(PAY.APPROVED, 0, 'PENDING', 1, 'APPROVED', 2, 'REJECTED') STATUS");
            sb.Append("  from PAYMENTS_TBL PAY,VENDOR_MST_TBL VMT,payment_trn_tbl prn,Inv_Supplier_Tbl INV,CURRENCY_TYPE_MST_TBL CURR");
            sb.Append("  Where VMT.VENDOR_MST_PK= PAY.Vendor_Mst_Fk");
            sb.Append("  AND  pay.payment_tbl_pk=prn.payments_tbl_fk");
            sb.Append("  AND  prn.inv_supplier_tbl_fk=INV.INV_SUPPLIER_PK");
            sb.Append("  AND  CURR.CURRENCY_MST_PK=PAY.Currency_Mst_Fk");
            sb.Append("  AND PAY.PAYMENT_TBL_PK=" + lngPkValue);

            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion

        #region "FetchInvoice "
        public DataSet FetchInvoice(long lngPkValue, string invoiceFlg)
        {
            WorkFlow objWF = new WorkFlow();
            try
            {
                var _with12 = objWF.MyCommand;
                _with12.Parameters.Add("INVOICE_PK_IN", lngPkValue).Direction = ParameterDirection.Input;
                _with12.Parameters.Add("INVOICE_FLG_IN", invoiceFlg).Direction = ParameterDirection.Input;
                _with12.Parameters.Add("HEADER_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                return objWF.GetDataSet("FETCH_MAILING_DETAIL_PKG", "FETCH_INVOICE_DETAILS");
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWF.MyConnection.Close();
                objWF = null;
            }
        }
        #endregion

        #region " Send Message "
        public string SendMessageCrd(long lngCreatedBy, string strDocId, string strCustomer, bool blnApp, string strSpecificURL, long lngLocFk, string strDocDate, long SenderFk = 0)
        {
            string functionReturnValue = null;
            DataSet dsMsg = new DataSet();
            DataSet dsDoc = null;
            DataSet dsUser = new DataSet();
            DataSet dsSender = new DataSet();
            string strUsrName = null;
            string strLocName = null;
            string strMsgSub = null;
            string strMsgHeader = null;
            string strMsgFooter = null;
            string strMsgBody = null;
            DataRow DR = null;
            DataSet dsUserFks = null;
            long lngdocPk = 0;
            string strSenderLocName = null;
            string strSenderUsrName = null;
            string strPageURL = null;
            long lngUserFk = 0;
            int intSenderCnt = 0;
            try
            {
                dsDoc = FetchDocument(strDocId);
                if (dsDoc.Tables[0].Rows.Count > 0)
                {
                    lngdocPk = Convert.ToInt64(dsDoc.Tables[0].Rows[0]["DOCUMENT_MST_PK"]);
                    dsMsg = GetMessageInfo(lngdocPk, -1);
                    if (dsMsg.Tables[0].Rows.Count < 1)
                    {
                        DR = dsMsg.Tables[0].NewRow();
                        dsMsg.Tables[0].Rows.Add(DR);
                    }

                    dsUserFks = GetUserInfo(lngdocPk, lngLocFk, SenderFk);
                    if (dsUserFks.Tables[0].Rows.Count > 0)
                    {
                        for (intSenderCnt = 0; intSenderCnt <= dsUserFks.Tables[0].Columns.Count - 1; intSenderCnt++)
                        {
                            if (!(string.IsNullOrEmpty(dsUserFks.Tables[0].Rows[0][intSenderCnt].ToString())))
                            {
                                if ((Convert.ToInt32(dsUserFks.Tables[0].Rows[0][intSenderCnt])) > 0)
                                {
                                    dsUser = FetchUserDetails(Convert.ToInt64(dsUserFks.Tables[0].Rows[0][intSenderCnt]));
                                    strUsrName = Convert.ToString(dsUser.Tables[0].Rows[0]["USER_NAME"]);
                                    strLocName = Convert.ToString(dsUser.Tables[0].Rows[0]["LOCATION_NAME"]);

                                    if (dsMsg.Tables[0].Rows.Count > 0)
                                    {
                                        var _with13 = dsMsg.Tables[0].Rows[0];
                                        _with13["Sender_Fk"] = lngCreatedBy;
                                        dsSender = FetchUserDetails(lngCreatedBy);
                                        strSenderUsrName = Convert.ToString(dsSender.Tables[0].Rows[0]["USER_NAME"]);
                                        strSenderLocName = Convert.ToString(dsSender.Tables[0].Rows[0]["LOCATION_NAME"]);

                                        _with13["Receiver_Fk"] = Convert.ToInt64(dsUserFks.Tables[0].Rows[0][intSenderCnt]);
                                        _with13["Msg_Read"] = 0;
                                        _with13["Followup_Flag"] = 0;
                                        _with13["Have_Attachment"] = 0;

                                        strMsgBody = Convert.ToString(_with13["Msg_Body"]);
                                        strMsgBody = strMsgBody.Replace("<<", "<");
                                        strMsgBody = strMsgBody.Replace("&lt;&lt;", "<");
                                        strMsgBody = strMsgBody.Replace(">>", ">");
                                        strMsgBody = strMsgBody.Replace("&gt;&gt;", ">");
                                        strMsgBody = strMsgBody.Replace("<SENDER>", (strSenderUsrName + ", " + strSenderLocName));
                                        strMsgBody = strMsgBody.Replace("<RECEIVER>", strUsrName);
                                        strMsgBody = strMsgBody.Replace("<CURRENT DATE>", Convert.ToString(DateTime.Now.Date));
                                        strMsgBody = strMsgBody.Replace("<HBL DATE>", strDocDate);
                                        strMsgBody = strMsgBody.Replace("<CUSTOMER>", strCustomer);
                                        _with13["Msg_Body"] = strMsgBody;

                                        strMsgSub = Convert.ToString(_with13["MSG_SUBJECT"]);
                                        strMsgSub = strMsgSub.Replace("<<", "<");
                                        strMsgSub = strMsgSub.Replace("&lt;&lt;", "<");
                                        strMsgSub = strMsgSub.Replace(">>", ">");
                                        strMsgSub = strMsgSub.Replace("&gt;&gt;", ">");
                                        strMsgSub = strMsgSub.Replace("<SENDER>", (strSenderUsrName + ", " + strSenderLocName));
                                        strMsgSub = strMsgSub.Replace("<RECEIVER>", strUsrName);
                                        strMsgSub = strMsgSub.Replace("<CURRENT DATE>", Convert.ToString(DateTime.Now.Date));
                                        strMsgSub = strMsgSub.Replace("<HBL DATE>", strDocDate);
                                        strMsgSub = strMsgSub.Replace("<CUSTOMER>", strCustomer);
                                        _with13["Msg_Subject"] = strMsgSub;

                                        _with13["Read_Receipt"] = 0;
                                        _with13["Document_Mst_Fk"] = lngdocPk;
                                        if (strDocId == "Booking Sea Approval")
                                        {
                                            _with13["User_Message_Folders_Fk"] = 5;
                                        }
                                        else if (strDocId == "Booking Air Approval")
                                        {
                                            _with13["User_Message_Folders_Fk"] = 5;
                                        }
                                        else if (strDocId == "HBL Approval")
                                        {
                                            _with13["User_Message_Folders_Fk"] = 6;
                                        }
                                        else if (strDocId == "HAWB Approval")
                                        {
                                            _with13["User_Message_Folders_Fk"] = 7;
                                        }
                                        else if (strDocId == "Release Note")
                                        {
                                            _with13["User_Message_Folders_Fk"] = 8;
                                        }
                                        _with13["Msg_Received_Dt"] = DateTime.Now;
                                    }
                                    else
                                    {
                                        return "Please define WorkFlow for the particular document";
                                    }

                                    if ((SaveUnApprovedCrd(dsMsg, lngCreatedBy) == -1))
                                    {
                                        return "<br>Due to some problem Mail has not been sent</br>";
                                        return functionReturnValue;
                                    }
                                }
                            }
                        }
                        return "<br>All data saved Successfully and Mail has been sent</br>";
                    }
                    else
                    {
                        return "<br>Mail has not been sent.Document Id not created.</br>";
                    }
                }
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return functionReturnValue;
        }
        #endregion

        #region " Fetch Document Details"
        public DataSet FetchDocument(string documentId)
        {

            StringBuilder strSqlBuilder = new StringBuilder();
            WorkFlow objWF = new WorkFlow();

            strSqlBuilder.Append(" SELECT DMT.DOCUMENT_MST_PK ");
            strSqlBuilder.Append(" FROM DOCUMENT_MST_TBL DMT, ");
            strSqlBuilder.Append(" DOCUMENT_NAME_MST_TBL DN ");
            strSqlBuilder.Append(" WHERE");
            strSqlBuilder.Append(" UPPER(DN.DOCUMENT_NAME)= UPPER('" + documentId.ToUpper() + "')");
            strSqlBuilder.Append(" AND DN.DOCUMENT_NAME_MST_PK = DMT.DOCUMENT_NAME_MST_FK ");
            try
            {
                return objWF.GetDataSet(strSqlBuilder.ToString());
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion

        #region " Fetch UserName & Location Details"
        public DataSet FetchUserDetails(long lngUserMessagePk)
        {
            StringBuilder strSqlBuilder = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSqlBuilder.Append(" SELECT UMT.USER_NAME, LMT.LOCATION_NAME");
                strSqlBuilder.Append(" FROM USER_MST_TBL UMT, Location_Mst_Tbl LMT");
                strSqlBuilder.Append(" WHERE UMT.USER_MST_PK=" + lngUserMessagePk + "");
                strSqlBuilder.Append(" and LMT.LOCATION_MST_PK = UMT.DEFAULT_LOCATION_FK");


                return objWF.GetDataSet(strSqlBuilder.ToString());
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion

        #region " Get User Information "
        public DataSet GetUserInfo(long lngDocumentPk, long lngLocFk, long SenderFk, int ReqByPK = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet dsUser = null;
            StringBuilder strSqlBuilder = new StringBuilder();
            //If lngLocFk <= 0 Then
            //    lngLocFk = Session("LOGED_IN_LOC_FK")
            //End If

            try
            {
                strSqlBuilder.Append(" SELECT  ");
                strSqlBuilder.Append(" WF.USER_MST_FK,WF.COPY_TO1_FK,COPY_TO2_FK,COPY_TO3_FK");
                if (ReqByPK > 0)
                {
                    strSqlBuilder.Append("," + ReqByPK + "");
                }
                strSqlBuilder.Append(" FROM WORKFLOW_RULES_TRN WF, DOCUMENT_MST_TBL DOC,WORKFLOW_LOC_TRN WFL");
                strSqlBuilder.Append(" WHERE  ");
                strSqlBuilder.Append(" WF.DOCUMENT_MST_FK = DOC.DOCUMENT_MST_PK ");
                strSqlBuilder.Append(" AND WFL.WORKFLOW_RULES_FK = WF.WORKFLOW_RULES_PK ");
                strSqlBuilder.Append(" AND WF.DOCUMENT_MST_FK = " + lngDocumentPk + "");
                strSqlBuilder.Append(" AND TO_DATE(SYSDATE,'DD/MM/YYYY') >= WF.VALID_FROM  AND TO_DATE(SYSDATE,'DD/MM/YYYY') <= NVL(WF.VALIDTO, '31/12/2999') ");
                strSqlBuilder.Append(" AND WFL.FROM_LOC_MST_FK =  " + lngLocFk + "");
                strSqlBuilder.Append(" AND WF.ACTIVE=1 ");
                dsUser = objWF.GetDataSet(strSqlBuilder.ToString());
                if (dsUser.Tables[0].Rows.Count == 0 & ReqByPK > 0)
                {
                    strSqlBuilder.Clear();
                    strSqlBuilder.Append(" SELECT " + ReqByPK + " FROM DUAL ");
                    dsUser = objWF.GetDataSet(strSqlBuilder.ToString());
                }
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
            return dsUser;
        }

        #endregion

        public DataSet GetMessageInfoNotify(long lngVslPk, long lngLocPk)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder strSqlBuilder = new StringBuilder();
            try
            {
                strSqlBuilder.Append(" SELECT VVT.PORT_MST_POL_FK,PMT.PORT_NAME PORT,'' TERMINALID ,  ");
                strSqlBuilder.Append(" VVT.VOYAGE_TRN_PK,VV.VESSEL_NAME ||'/'|| VVT.VOYAGE VESSELVOY, VVT.VESSEL_VOYAGE_TBL_FK,VVT.POL_ETA ETA,VVT.POL_ETD ETD,VVT.REVISED_ETA REVISEDETA,VVT.REVISED_ETD REVISEDETD ");
                strSqlBuilder.Append(" FROM VESSEL_VOYAGE_TRN VVT, PORT_MST_TBL PMT, VESSEL_VOYAGE_TBL VV  ");
                strSqlBuilder.Append(" WHERE PMT.PORT_MST_PK = VVT.PORT_MST_POL_FK ");
                strSqlBuilder.Append("AND VV.VESSEL_VOYAGE_TBL_PK = VVT.VESSEL_VOYAGE_TBL_FK ");
                strSqlBuilder.Append("  AND VVT.VOYAGE_TRN_PK = " + lngVslPk + "");
                return objWF.GetDataSet(strSqlBuilder.ToString());
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #region "Get Message Information"
        public DataSet GetMessageInfo(long lngDocPk, long lngLocPk)
        {
            StringBuilder strSqlBuilder = new StringBuilder();
            DataSet dsMsgInfo = new DataSet();
            OracleDataAdapter DA = new OracleDataAdapter();
            WorkFlow objWF = new WorkFlow();

            try
            {
                strSqlBuilder.Append(" SELECT ROWNUM SR_NO,  ");
                strSqlBuilder.Append(" 0 USER_MESSAGE_PK, ");
                strSqlBuilder.Append(" USRMSGTRN.SENDER_FK, ");
                strSqlBuilder.Append(" USRMSGTRN.RECEIVER_FK, ");
                strSqlBuilder.Append(" USRMSGTRN.MSG_READ, ");
                strSqlBuilder.Append(" USRMSGTRN.FOLLOWUP_FLAG, ");
                strSqlBuilder.Append(" USRMSGTRN.HAVE_ATTACHMENT, ");
                strSqlBuilder.Append(" DOC.DOCUMENT_SUBJECT  MSG_SUBJECT, ");
                strSqlBuilder.Append(" DOC.DOCUMENT_BODY  MSG_BODY1, ");
                strSqlBuilder.Append(" DOC.DOCUMENT_HEADER || chr(10) ");
                strSqlBuilder.Append(" || '     ' || DOC.DOCUMENT_BODY || chr(10) ");
                strSqlBuilder.Append(" ||  DOC.DOCUMENT_FOOTER ||");
                strSqlBuilder.Append(" chr(10) || ' ' Msg_Body,");

                strSqlBuilder.Append(" DOC.DOCUMENT_HEADER || chr(10) ");
                strSqlBuilder.Append(" || '     ' || DOC.DOCUMENT_BODY || ");
                strSqlBuilder.Append(" chr(10) || ' ' ExternalMsgBody,");
                strSqlBuilder.Append(" DOC.DOCUMENT_FOOTER as ExternalMsgFooter,");

                strSqlBuilder.Append(" USRMSGTRN.READ_RECEIPT, ");
                strSqlBuilder.Append(" USRMSGTRN.DOCUMENT_MST_FK, ");
                strSqlBuilder.Append(" DOC.MESSAGE_FOLDER_MST_FK USER_MESSAGE_FOLDERS_FK, ");
                strSqlBuilder.Append(" USRMSGTRN.MSG_RECEIVED_DT, ");
                strSqlBuilder.Append(" USRMSGTRN.VERSION_NO  ");
                strSqlBuilder.Append(" FROM USER_MESSAGE_TRN USRMSGTRN, ");
                strSqlBuilder.Append(" DOCUMENT_MST_TBL DOC ");
                strSqlBuilder.Append(" WHERE USRMSGTRN.DOCUMENT_MST_FK(+) = DOC.DOCUMENT_MST_PK ");
                strSqlBuilder.Append(" AND USRMSGTRN.DELETE_FLAG IS NULL  AND DOC.document_mst_pk =  " + lngDocPk + "");
                strSqlBuilder.Append(" AND USER_MESSAGE_PK(+) =  -1 ");

                DA = objWF.GetDataAdapter(strSqlBuilder.ToString().Trim());
                DA.Fill(dsMsgInfo, "MsgTrn");
                strSqlBuilder = new StringBuilder();
                strSqlBuilder.Append(" SELECT ROWNUM SR_NO, ");
                strSqlBuilder.Append(" 0 User_Message_Det_Pk, ");
                strSqlBuilder.Append(" 0 User_Message_Fk, ");
                strSqlBuilder.Append(" '' Attachment_Caption, ");
                strSqlBuilder.Append(" '' Attachment_Data, ");
                strSqlBuilder.Append(" doc.attachment_url Url_Page, ");
                strSqlBuilder.Append(" 0 Version_No ");
                strSqlBuilder.Append(" FROM document_mst_tbl doc ");
                strSqlBuilder.Append(" WHERE doc.Active=1 And doc.document_mst_pk = " + lngDocPk + "");

                DA = objWF.GetDataAdapter(strSqlBuilder.ToString());
                DA.Fill(dsMsgInfo, "MsgDet");
                DataRelation DSWFMSg = new DataRelation("WFMsg", new DataColumn[] { dsMsgInfo.Tables["MsgTrn"].Columns["User_Message_Pk"] }, new DataColumn[] { dsMsgInfo.Tables["MsgDet"].Columns["User_Message_Fk"] });
                dsMsgInfo.Relations.Add(DSWFMSg);
                return dsMsgInfo;
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion

        public DataTable FetchDocumentDetail(string documentId)
        {
            System.Text.StringBuilder strbldrSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            strbldrSQL.Append(" select doc.document_subject,doc.attachment_url, ");
            strbldrSQL.Append(" doc.document_header h1,doc.document_body h2,doc.document_footer h3,'PS: This is a system generated message.' msg_body ");
            strbldrSQL.Append(" from document_mst_tbl doc ");
            strbldrSQL.Append(" where doc.document_mst_pk=" + documentId + "");
            try
            {
                return objWF.GetDataTable(strbldrSQL.ToString());

            }
            catch (Exception exp)
            {
                throw exp;
            }
        }


        #region " Fetch Message"
        public DataSet FetchMessage(Int32 FolderPK, Int32 UserPK, Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 sCol = 9, string SortOrder = "DESC", string FilterBy = "", string SearchValue = "", string SearchType = "")
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            string searchQuery = null;
            string searchQuery1 = null;

            if (FolderPK == 4)
            {
                searchQuery = " AND Usrmsg.del_sender_flag is null And Usrmsg.del_sentitem_flag IS NULL and Usrmsg.delete_flag is null and usrmsg.sender_fk= " + UserPK;
                searchQuery1 = "  and EMT.del_sender_flag is null And EMT.del_sentitem_flag IS NULL and EMT.delete_flag is null and EMT.sender_fk= " + UserPK;
            }
            else if (FolderPK == 1)
            {
                searchQuery = " AND UsrMsg.DELETE_FLAG is Null And usrmsg.user_message_folders_fk =1 ";
                searchQuery += " And usrmsg.receiver_fk = " + UserPK;
                searchQuery1 = " and EMT.DELETE_FLAG is Null   and EMT.user_message_folders_fk =1 And EMT.SENDER_FK!=" + UserPK;
            }
            else if (FolderPK == 7)
            {
                searchQuery = "  AND UsrMsg.DELETE_FLAG is Null  AND USRMSG.MSG_READ = 1 AND usrmsg.receiver_fk = " + UserPK;
                searchQuery1 = "  AND EMT.DELETE_FLAG is Null AND EMT.MSG_READ = 1 ";
            }
            else if (FolderPK == 8)
            {
                searchQuery = "  AND UsrMsg.DELETE_FLAG is Null AND USRMSG.MSG_READ = 0 AND usrmsg.receiver_fk = " + UserPK;
                searchQuery1 = "  AND EMT.DELETE_FLAG is Null AND EMT.MSG_READ = 0 ";
            }
            else if (FolderPK != 0)
            {
                searchQuery = " AND UsrMsg.DELETE_FLAG is Null And usrmsg.user_message_folders_fk = " + FolderPK + " AND usrmsg.receiver_fk = " + UserPK;
                searchQuery1 = " and  EMT.DELETE_FLAG is Null  And EMT.user_message_folders_fk = " + FolderPK;
            }
            if (!string.IsNullOrEmpty(SearchValue))
            {
                if (Convert.ToInt32(FilterBy) == 0)
                {
                    if (SearchType.ToString().Trim().Length > 0)
                    {
                        if (SearchType == "S")
                        {
                            searchQuery += " AND UPPER(usr.user_name) LIKE '" + SearchValue.ToUpper().Replace("'", "''") + "%'" ;
                            searchQuery1 += " AND UPPER(UMT.USER_NAME) LIKE '" + SearchValue.ToUpper().Replace("'", "''") + "%'" ;
                        }
                        else
                        {
                            searchQuery += " AND UPPER(usr.user_name) LIKE '%" + SearchValue.ToUpper().Replace("'", "''") + "%'" ;
                            searchQuery1 += " AND UPPER(UMT.USER_NAME) LIKE '%" + SearchValue.ToUpper().Replace("'", "''") + "%'" ;
                        }
                    }
                }
                else if (Convert.ToInt32(FilterBy) == 1)
                {
                    if (SearchType.ToString().Trim().Length > 0)
                    {
                        if (SearchType == "S")
                        {
                            searchQuery += " AND UPPER(usr1.user_name) LIKE '" + SearchValue.ToUpper().Replace("'", "''") + "%'" ;
                            searchQuery1 += " AND UPPER(EMT.EX_EMAIL_ID) LIKE '" + SearchValue.ToUpper().Replace("'", "''") + "%'" ;
                        }
                        else
                        {
                            searchQuery += " AND UPPER(usr1.user_name) LIKE '%" + SearchValue.ToUpper().Replace("'", "''") + "%'" ;
                            searchQuery1 += " AND UPPER(EMT.EX_EMAIL_ID) LIKE '%" + SearchValue.ToUpper().Replace("'", "''") + "%'" ;
                        }
                    }
                }
                else if (Convert.ToInt32(FilterBy) == 2)
                {
                    if (SearchType.ToString().Trim().Length > 0)
                    {
                        if (SearchType == "S")
                        {
                            searchQuery += " AND UPPER(usrmsg.msg_subject) LIKE '" + SearchValue.ToUpper().Replace("'", "''") + "%'" ;
                            searchQuery1 += " AND UPPER(EMT.MSG_SUBJECT) LIKE '" + SearchValue.ToUpper().Replace("'", "''") + "%'" ;
                        }
                        else
                        {
                            searchQuery += " AND UPPER(usrmsg.msg_subject) LIKE '%" + SearchValue.ToUpper().Replace("'", "''") + "%'" ;
                            searchQuery1 += " AND UPPER(EMT.MSG_SUBJECT) LIKE '%" + SearchValue.ToUpper().Replace("'", "''") + "%'" ;
                        }
                    }
                }
                else if (Convert.ToInt32(FilterBy) == 3)
                {
                    searchQuery += "  And TO_DATE(usrmsg.msg_received_dt) = TO_DATE('" + SearchValue + "',dateformat)";
                    searchQuery1 += "  And TO_DATE(EMT.MSG_RECEIVED_DT) = TO_DATE('" + SearchValue + "',dateformat)";
                }
                else if (Convert.ToInt32(FilterBy) == 4)
                {
                    if (SearchType == "S")
                    {
                        searchQuery += " AND UPPER(usrmsg.msg_body) LIKE '" + SearchValue.ToUpper().Replace("'", "''") + "%'" ;
                        searchQuery1 += " AND UPPER(EMT.MSG_BODY) LIKE '" + SearchValue.ToUpper().Replace("'", "''") + "%'" ;
                    }
                    else
                    {
                        searchQuery += " AND UPPER(usrmsg.msg_body) LIKE '%" + SearchValue.ToUpper().Replace("'", "''") + "%'" ;
                        searchQuery1 += " AND UPPER(EMT.MSG_BODY) LIKE '%" + SearchValue.ToUpper().Replace("'", "''") + "%'" ;
                    }
                }
            }

            strSQL = "SELECT Count(*) ";
            strSQL += " FROM (";
            strSQL += " SELECT USRMSG.USER_MESSAGE_PK FROM ";
            if (FolderPK == 6)
            {
                strSQL += " USER_MESSAGE_TRN_ARCHIVE UsrMsg,";
            }
            else
            {
                strSQL += " user_message_trn UsrMsg,";
            }
            strSQL += " user_mst_tbl Usr, ";
            strSQL += " user_mst_tbl Usr1 ";
            strSQL += " WHERE   ";
            strSQL += " usrmsg.receiver_fk = usr1.user_mst_pk(+) ";
            strSQL += " and usrmsg.Sender_Fk = usr.user_mst_pk(+) ";
            strSQL += searchQuery;

            strSQL += " union  " ;
            strSQL += " SELECT EMT.EXTERNAL_MSG_TRN_PK ";
            strSQL += " FROM EXTERNAL_MESSAGE_TRN EMT,user_mst_tbl umt";
            strSQL += " WHERE  EMT.SENDER_FK=UMT.USER_MST_PK(+) ";
            strSQL += searchQuery1;
            strSQL += " )   ";
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
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
            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;

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
            strSQL += "(CASE";
            strSQL += "                        WHEN USRMSG.USER_MESSAGE_FOLDERS_FK = 1 THEN";
            strSQL += "                          'Inbox'";
            strSQL += "                         WHEN USRMSG.USER_MESSAGE_FOLDERS_FK = 2 THEN";
            strSQL += "                          'Archive'";
            strSQL += "                       WHEN USRMSG.USER_MESSAGE_FOLDERS_FK = 3 THEN";
            strSQL += "                        'Draft'";
            strSQL += "                         WHEN USRMSG.USER_MESSAGE_FOLDERS_FK = 4 THEN";
            strSQL += "                          'Sent'";
            strSQL += "                         WHEN USRMSG.USER_MESSAGE_FOLDERS_FK = 5 THEN";
            strSQL += "                          'Deleted'";
            strSQL += "                         WHEN USRMSG.USER_MESSAGE_FOLDERS_FK = 6 THEN";
            strSQL += "                         'History'";
            strSQL += "                     END) FOLDER,";
            strSQL += " usrmsg.msg_received_dt ReceiveDt,";
            strSQL += " usrmsg.msg_read,";
            strSQL += " usrmsg.read_receipt,1 Flag";
            strSQL += " FROM ";
            if (FolderPK == 6)
            {
                strSQL += " USER_MESSAGE_TRN_ARCHIVE UsrMsg,";
            }
            else
            {
                strSQL += " user_message_trn UsrMsg,";
            }

            strSQL += " user_mst_tbl Usr,";
            strSQL += " user_mst_tbl Usr1 Where";
            strSQL += " usrmsg.sender_fk = usr.user_mst_pk(+) ";
            strSQL += "and  usrmsg.Receiver_Fk = usr1.user_mst_pk(+) ";
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
            strSQL += "(CASE";
            strSQL += "                        WHEN EMT.USER_MESSAGE_FOLDERS_FK = 1 THEN";
            strSQL += "                          'Inbox'";
            strSQL += "                         WHEN EMT.USER_MESSAGE_FOLDERS_FK = 2 THEN";
            strSQL += "                          'Archive'";
            strSQL += "                       WHEN EMT.USER_MESSAGE_FOLDERS_FK = 3 THEN";
            strSQL += "                        'Draft'";
            strSQL += "                         WHEN EMT.USER_MESSAGE_FOLDERS_FK = 4 THEN";
            strSQL += "                          'Sent'";
            strSQL += "                         WHEN EMT.USER_MESSAGE_FOLDERS_FK = 5 THEN";
            strSQL += "                          'Deleted'";
            strSQL += "                         WHEN EMT.USER_MESSAGE_FOLDERS_FK = 6 THEN";
            strSQL += "                         'History'";
            strSQL += "                     END) FOLDER,";
            strSQL += " EMT.MSG_RECEIVED_DT ReceiveDt,";
            strSQL += " EMT.msg_read,";
            strSQL += " 0 read_receipt,2 Flag";
            strSQL += " FROM ";
            strSQL += "  EXTERNAL_MESSAGE_TRN EMT,";
            strSQL += " user_mst_tbl umt Where";
            strSQL += "  EMT.SENDER_FK=UMT.USER_MST_PK(+) ";
            strSQL += searchQuery1;
            strSQL += " Order By " + sCol + "  " + SortOrder;
            strSQL += " ) qry ) WHERE SR_NO  Between " + start + " and " + last;

            try
            {
                return objWF.GetDataSet(strSQL);
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion

        #region "Display Message"
        public DataSet DisplayMessage(Int32 MessagePK)
        {
            StringBuilder strSqlBuilder = new StringBuilder();
            strSqlBuilder.Append(" SELECT ");
            strSqlBuilder.Append(" usrmsg.sender_fk,");
            strSqlBuilder.Append(" usr.USER_NAME MsgFrom,");
            strSqlBuilder.Append(" usr1.USER_NAME MsgTo,");
            strSqlBuilder.Append(" usrmsg.msg_received_dt,");
            strSqlBuilder.Append(" usrmsg.msg_subject,");
            strSqlBuilder.Append(" usrmsg.msg_body");
            strSqlBuilder.Append(" FROM user_message_trn usrMsg, ");
            strSqlBuilder.Append(" User_Mst_Tbl usr,");
            strSqlBuilder.Append(" user_mst_tbl usr1");
            strSqlBuilder.Append(" WHERE  ");
            strSqlBuilder.Append(" usrMsg.User_Message_Pk = " + MessagePK + "");
            strSqlBuilder.Append(" AND usr.user_mst_pk = usrmsg.sender_fk");
            strSqlBuilder.Append(" AND usr1.user_mst_pk = usrmsg.receiver_fk");

            strSqlBuilder.Append(" Union All ");

            strSqlBuilder.Append(" SELECT ");
            strSqlBuilder.Append(" usrmsg.sender_fk,");
            strSqlBuilder.Append(" usr.USER_NAME MsgFrom,");
            strSqlBuilder.Append(" usr1.USER_NAME MsgTo,");
            strSqlBuilder.Append(" usrmsg.msg_received_dt,");
            strSqlBuilder.Append(" usrmsg.msg_subject,");
            strSqlBuilder.Append(" usrmsg.msg_body");
            strSqlBuilder.Append(" FROM user_message_trn_archive usrMsg, ");
            strSqlBuilder.Append(" User_Mst_Tbl usr,");
            strSqlBuilder.Append(" user_mst_tbl usr1");
            strSqlBuilder.Append(" WHERE  ");
            strSqlBuilder.Append(" usrMsg.User_Message_Pk = " + MessagePK + "");
            strSqlBuilder.Append(" AND usr.user_mst_pk = usrmsg.sender_fk");
            strSqlBuilder.Append(" AND usr1.user_mst_pk = usrmsg.receiver_fk");
            strSqlBuilder.Append(" Union All ");
            strSqlBuilder.Append(" SELECT ");
            strSqlBuilder.Append(" EMT.SENDER_FK SENDERFK,");
            strSqlBuilder.Append(" UMT.USER_NAME MSGFROM,");
            strSqlBuilder.Append(" TO_CHAR(EMT.EX_EMAIL_ID) MSGTO,");
            strSqlBuilder.Append(" EMT.MSG_RECEIVED_DT RECEIVEDT,");
            strSqlBuilder.Append(" EMT.MSG_SUBJECT,");
            strSqlBuilder.Append(" emt.msg_body");
            strSqlBuilder.Append(" FROM EXTERNAL_MESSAGE_TRN EMT, ");
            strSqlBuilder.Append(" USER_MST_TBL UMT");
            strSqlBuilder.Append(" WHERE  ");
            strSqlBuilder.Append(" emt.EXTERNAL_MSG_TRN_PK = " + MessagePK + "");
            strSqlBuilder.Append(" AND EMT.SENDER_FK = UMT.USER_MST_PK");
            WorkFlow objWF = new WorkFlow();
            try
            {
                return objWF.GetDataSet(strSqlBuilder.ToString());
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion

        #region "DisplayMessageDetail"
        public DataSet DisplayMessageDetail(Int32 MessagePK)
        {
            StringBuilder strSqlBuilder = new StringBuilder();
            WorkFlow objWF = new WorkFlow();

            strSqlBuilder.Append(" SELECT ");
            strSqlBuilder.Append(" msgdet.attachment_caption,");
            strSqlBuilder.Append(" msgdet.attachment_data,");
            strSqlBuilder.Append(" msgdet.url_page");
            strSqlBuilder.Append(" from user_message_det_trn msgDet");
            strSqlBuilder.Append(" where msgdet.user_message_fk = " + MessagePK + "");
            strSqlBuilder.Append(" Union All ");
            strSqlBuilder.Append(" SELECT ");
            strSqlBuilder.Append(" msgdet.attachment_caption,");
            strSqlBuilder.Append(" msgdet.attachment_data,");
            strSqlBuilder.Append(" msgdet.url_page");
            strSqlBuilder.Append(" from user_message_det_trn_archive msgDet");
            strSqlBuilder.Append(" where msgdet.user_message_fk = " + MessagePK + "");
            try
            {
                return objWF.GetDataSet(strSqlBuilder.ToString());
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion

        #region " Save Approval Messages"
        public long SaveApprovalMsg(DataSet dsMessage)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = default(OracleTransaction);
            TRAN = objWK.MyConnection.BeginTransaction();
            objWK.MyCommand.Transaction = TRAN;
            objWK.MyCommand.Connection = objWK.MyConnection;
            long lngPkVal = 0;
            try
            {
                var _with14 = objWK.MyCommand;
                _with14.CommandText = objWK.MyUserName + ".USER_MESSAGE_TRN_PKG.USER_MESSAGE_TRN_INS";
                _with14.CommandType = CommandType.StoredProcedure;
                var _with15 = _with14.Parameters;
                _with15.Clear();
                _with15.Add("SENDER_FK_IN", dsMessage.Tables[0].Rows[0]["Sender_Fk"]).Direction = ParameterDirection.Input;
                _with15.Add("RECEIVER_FK_IN", dsMessage.Tables[0].Rows[0]["Receiver_Fk"]).Direction = ParameterDirection.Input;
                _with15.Add("MSG_READ_IN", dsMessage.Tables[0].Rows[0]["Msg_Read"]).Direction = ParameterDirection.Input;
                _with15.Add("FOLLOWUP_FLAG_IN", dsMessage.Tables[0].Rows[0]["Followup_Flag"]).Direction = ParameterDirection.Input;
                _with15.Add("HAVE_ATTACHMENT_IN", dsMessage.Tables[0].Rows[0]["Have_Attachment"]).Direction = ParameterDirection.Input;
                _with15.Add("MSG_SUBJECT_IN", dsMessage.Tables[0].Rows[0]["Msg_Subject"]).Direction = ParameterDirection.Input;
                _with15.Add("MSG_BODY_IN", dsMessage.Tables[0].Rows[0]["Msg_Body"]).Direction = ParameterDirection.Input;
                _with15.Add("READ_RECEIPT_IN", dsMessage.Tables[0].Rows[0]["Read_Receipt"]).Direction = ParameterDirection.Input;
                _with15.Add("DOCUMENT_MST_FK_IN", dsMessage.Tables[0].Rows[0]["Document_Mst_Fk"]).Direction = ParameterDirection.Input;
                _with15.Add("USER_MESSAGE_FOLDERS_FK_IN", dsMessage.Tables[0].Rows[0]["User_Message_Folders_Fk"]).Direction = ParameterDirection.Input;
                _with15.Add("MSG_RECEIVED_DT_IN", dsMessage.Tables[0].Rows[0]["Msg_Received_Dt"]).Direction = ParameterDirection.Input;
                _with15.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                _with15.Add("RETURN_VALUE", lngPkVal).Direction = ParameterDirection.Output;
                if (_with14.ExecuteNonQuery() == 1)
                {
                    dsMessage.Tables[0].Rows[0]["USER_MESSAGE_PK"] = objWK.MyCommand.Parameters["Return_value"].Value;
                    _with14.CommandText = objWK.MyUserName + ".USER_MESSAGE_DET_TRN_PKG.USER_MESSAGE_DET_TRN_INS";
                    _with14.CommandType = CommandType.StoredProcedure;
                    var _with16 = _with14.Parameters;
                    _with16.Clear();
                    _with16.Add("USER_MESSAGE_FK_IN", dsMessage.Tables[0].Rows[0]["USER_MESSAGE_PK"]).Direction = ParameterDirection.Input;
                    _with16.Add("ATTACHMENT_CAPTION_IN", dsMessage.Tables[0].Rows[0]["Msg_Subject"]).Direction = ParameterDirection.Input;
                    _with16.Add("ATTACHMENT_DATA_IN", dsMessage.Tables[1].Rows[0]["Attachment_Data"]).Direction = ParameterDirection.Input;
                    _with16.Add("URL_PAGE_IN", dsMessage.Tables[1].Rows[0]["URL_PAGE"]).Direction = ParameterDirection.Input;
                    _with16.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                    _with16.Add("RETURN_VALUE", lngPkVal).Direction = ParameterDirection.Output;

                    if (_with14.ExecuteNonQuery() == 1)
                    {
                        TRAN.Commit();
                        return 1;
                    }
                    else
                    {
                        TRAN.Rollback();
                        return -1;
                    }
                }
                else
                {
                    arrMessage.Add("Record Not Saved");
                }
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                TRAN.Rollback();
                throw ex;
            }
            finally
            {
                objWK.CloseConnection();
            }
            return 0;
        }
        #endregion

        #region "SaveUnApprovedTDR"
        public int SaveUnApprovedTDR(DataSet dsMsg, long lngCreatedBy)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = default(OracleTransaction);
            TRAN = objWK.MyConnection.BeginTransaction();
            objWK.MyCommand.Transaction = TRAN;
            objWK.MyCommand.Connection = objWK.MyConnection;
            try
            {
                int intPKVal = 0;
                long lngI = 0;
                Int32 RecAfct = default(Int32);
                var _with17 = objWK.MyCommand.Parameters;
                _with17.Clear();

                _with17.Add("SENDER_FK_IN", dsMsg.Tables[0].Rows[0]["Sender_Fk"]).Direction = ParameterDirection.Input;
                _with17.Add("RECEIVER_FK_IN", dsMsg.Tables[0].Rows[0]["Receiver_Fk"]).Direction = ParameterDirection.Input;
                _with17.Add("MSG_READ_IN", dsMsg.Tables[0].Rows[0]["Msg_Read"]).Direction = ParameterDirection.Input;
                _with17.Add("FOLLOWUP_FLAG_IN", dsMsg.Tables[0].Rows[0]["Followup_Flag"]).Direction = ParameterDirection.Input;
                _with17.Add("HAVE_ATTACHMENT_IN", dsMsg.Tables[0].Rows[0]["Have_Attachment"]).Direction = ParameterDirection.Input;
                _with17.Add("MSG_SUBJECT_IN", dsMsg.Tables[0].Rows[0]["Msg_Subject"]).Direction = ParameterDirection.Input;
                _with17.Add("MSG_BODY_IN", dsMsg.Tables[0].Rows[0]["Msg_Body"]).Direction = ParameterDirection.Input;
                _with17.Add("READ_RECEIPT_IN", dsMsg.Tables[0].Rows[0]["Read_Receipt"]).Direction = ParameterDirection.Input;
                _with17.Add("DOCUMENT_MST_FK_IN", dsMsg.Tables[0].Rows[0]["Document_Mst_Fk"]).Direction = ParameterDirection.Input;
                _with17.Add("USER_MESSAGE_FOLDERS_FK_IN", dsMsg.Tables[0].Rows[0]["User_Message_Folders_Fk"]).Direction = ParameterDirection.Input;
                _with17.Add("MSG_RECEIVED_DT_IN", dsMsg.Tables[0].Rows[0]["Msg_Received_Dt"]).Direction = ParameterDirection.Input;
                _with17.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                _with17.Add("RETURN_VALUE", intPKVal).Direction = ParameterDirection.Output;

                objWK.MyCommand.CommandText = objWK.MyUserName + ".USER_MESSAGE_TRN_PKG.USER_MESSAGE_TRN_INS";
                objWK.MyCommand.CommandType = CommandType.StoredProcedure;

                if (objWK.MyCommand.ExecuteNonQuery() == 1)
                {
                    dsMsg.Tables[0].Rows[0]["USER_MESSAGE_PK"] = objWK.MyCommand.Parameters["Return_value"].Value;
                    var _with18 = objWK.MyCommand.Parameters;
                    _with18.Clear();
                    _with18.Add("USER_MESSAGE_FK_IN", dsMsg.Tables[0].Rows[0]["USER_MESSAGE_PK"]).Direction = ParameterDirection.Input;
                    _with18.Add("ATTACHMENT_CAPTION_IN", dsMsg.Tables[0].Rows[0]["Msg_Subject"]).Direction = ParameterDirection.Input;
                    string str = null;
                    _with18.Add("ATTACHMENT_DATA_IN", dsMsg.Tables[1].Rows[0]["Attachment_Data"]).Direction = ParameterDirection.Input;
                    _with18.Add("URL_PAGE_IN", dsMsg.Tables[1].Rows[0]["URL_PAGE"]).Direction = ParameterDirection.Input;
                    _with18.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                    _with18.Add("RETURN_VALUE", intPKVal).Direction = ParameterDirection.Output;
                    objWK.MyCommand.CommandText = objWK.MyUserName + ".USER_MESSAGE_DET_TRN_PKG.USER_MESSAGE_DET_TRN_INS";
                    objWK.MyCommand.CommandType = CommandType.StoredProcedure;
                    if (objWK.MyCommand.ExecuteNonQuery() == 1)
                    {
                        TRAN.Commit();
                        UpdateMailStatus(Convert.ToInt32(dsMsg.Tables[0].Rows[0]["Document_Mst_Fk"]), DocRefNr, Convert.ToString(dsMsg.Tables[0].Rows[0]["Msg_Subject"]), Convert.ToInt32(dsMsg.Tables[0].Rows[0]["Sender_Fk"]), Convert.ToInt32(dsMsg.Tables[0].Rows[0]["Receiver_Fk"]), Convert.ToString(dsMsg.Tables[0].Rows[0]["Msg_Body"]));
                        return 1;
                    }
                    else
                    {
                        TRAN.Rollback();
                        return -1;
                    }
                }
                else
                {
                    arrMessage.Add("Record Not Saved");
                }
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWK.CloseConnection();
            }
            return 0;
        }
        #endregion

        #region "SaveUnApprovedCrd"
        public int SaveUnApprovedCrd(DataSet dsMsg, long lngCreatedBy)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = default(OracleTransaction);
            TRAN = objWK.MyConnection.BeginTransaction();
            objWK.MyCommand.Transaction = TRAN;
            objWK.MyCommand.Connection = objWK.MyConnection;
            try
            {
                int intPKVal = 0;
                long lngI = 0;
                Int32 RecAfct = default(Int32);
                var _with19 = objWK.MyCommand.Parameters;
                _with19.Clear();
                _with19.Add("SENDER_FK_IN", dsMsg.Tables[0].Rows[0]["Sender_Fk"]).Direction = ParameterDirection.Input;
                _with19.Add("RECEIVER_FK_IN", dsMsg.Tables[0].Rows[0]["Receiver_Fk"]).Direction = ParameterDirection.Input;
                _with19.Add("MSG_READ_IN", dsMsg.Tables[0].Rows[0]["Msg_Read"]).Direction = ParameterDirection.Input;
                _with19.Add("FOLLOWUP_FLAG_IN", dsMsg.Tables[0].Rows[0]["Followup_Flag"]).Direction = ParameterDirection.Input;
                _with19.Add("HAVE_ATTACHMENT_IN", dsMsg.Tables[0].Rows[0]["Have_Attachment"]).Direction = ParameterDirection.Input;
                _with19.Add("MSG_SUBJECT_IN", dsMsg.Tables[0].Rows[0]["Msg_Subject"]).Direction = ParameterDirection.Input;
                _with19.Add("MSG_BODY_IN", dsMsg.Tables[0].Rows[0]["Msg_Body"]).Direction = ParameterDirection.Input;
                _with19.Add("READ_RECEIPT_IN", dsMsg.Tables[0].Rows[0]["Read_Receipt"]).Direction = ParameterDirection.Input;
                _with19.Add("DOCUMENT_MST_FK_IN", dsMsg.Tables[0].Rows[0]["Document_Mst_Fk"]).Direction = ParameterDirection.Input;
                _with19.Add("USER_MESSAGE_FOLDERS_FK_IN", dsMsg.Tables[0].Rows[0]["User_Message_Folders_Fk"]).Direction = ParameterDirection.Input;
                _with19.Add("MSG_RECEIVED_DT_IN", dsMsg.Tables[0].Rows[0]["Msg_Received_Dt"]).Direction = ParameterDirection.Input;
                _with19.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                _with19.Add("RETURN_VALUE", intPKVal).Direction = ParameterDirection.Output;
                objWK.MyCommand.CommandText = objWK.MyUserName + ".USER_MESSAGE_TRN_PKG.USER_MESSAGE_TRN_INS";
                objWK.MyCommand.CommandType = CommandType.StoredProcedure;
                if (objWK.MyCommand.ExecuteNonQuery() == 1)
                {
                    TRAN.Commit();
                    return 1;
                }
                else
                {
                    TRAN.Rollback();
                    return -1;
                }
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
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
        #endregion

        #region " Fetch Messages - On Login"
        public DataSet FetchMessageSummary(Int32 UserPK)
        {
            string strSQL = null;
            strSQL = string.Empty ;
            strSQL += "   SELECT FOLDERFK, FOLDERNAME, SUM(NEWMSG) || '/' || SUM(TOTAL) MSGS FROM(";
            strSQL += " SELECT 1 folderFk, 'Inbox' FolderName,COUNT(um.user_message_pk) NewMsg, 0 total " ;
            strSQL += " FROM User_Message_Trn UM   " ;
            strSQL += " WHERE  um.msg_read(+) = 1 " ;
            strSQL += " AND UM.DELETE_FLAG(+) is null  and UM.USER_MESSAGE_FOLDERS_FK =1   And um.receiver_fk(+) =" + UserPK;
            strSQL += "  And um.SENDER_FK! =" + UserPK;

            strSQL += " union  " ;
            strSQL += " SELECT 1 folderFk,'Inbox' FolderName, 0 NewMsg, COUNT(um.user_message_pk) Total " ;
            strSQL += " FROM User_Message_Trn UM    " ;
            strSQL += " WHERE UM.DELETE_FLAG(+) is null   and UM.USER_MESSAGE_FOLDERS_FK =1    AND um.receiver_fk(+) =" + UserPK;
            strSQL += "  And um.SENDER_FK !=" + UserPK;
            strSQL += " union " ;
            strSQL += "   SELECT 1 FOLDERFK, 'Inbox' FOLDERNAME,COUNT(EMT.EXTERNAL_MSG_TRN_PK) NEWMSG,0 TOTAL " ;
            strSQL += " FROM EXTERNAL_MESSAGE_TRN EMT " ;
            strSQL += " WHERE EMT.MSG_READ(+) = 1 and EMT.DELETE_FLAG(+) IS NULL " ;
            strSQL += "  AND EMT.USER_MESSAGE_FOLDERS_FK =1 " ;
            strSQL += "  And emt.sender_fk! =" + UserPK;

            strSQL += " union " ;
            strSQL += "    SELECT 1 FOLDERFK, 'Inbox' FOLDERNAME,0 NEWMSG,COUNT(EMT.EXTERNAL_MSG_TRN_PK) TOTAL" ;
            strSQL += " FROM EXTERNAL_MESSAGE_TRN EMT " ;
            strSQL += " WHERE EMT.DELETE_FLAG(+) IS NULL " ;
            strSQL += "  And emt.sender_fk! =" + UserPK;
            strSQL += "  AND EMT.USER_MESSAGE_FOLDERS_FK =1)" ;
            strSQL += "  GROUP BY FOLDERFK, FOLDERNAME";

            //===============For INBOX
            //===============For ARCHIVE
            strSQL += " union " ;
            strSQL += "   SELECT FOLDERFK, FOLDERNAME, SUM(NEWMSG) || '/' || SUM(TOTAL) MSGS FROM(";
            strSQL += "  SELECT 2 folderFk,'Archive' FolderName,count(um.user_message_folders_fk) NewMsg,0 Total" ;
            strSQL += "  FROM User_Message_Trn UM WHERE UM.USER_MESSAGE_FOLDERS_FK = 2 and UM.DELETE_FLAG(+) is null and um.msg_read = 1 AND um.receiver_fk(+) = " + UserPK;

            strSQL += "  union  " ;
            strSQL += "  SELECT 2 folderFk,'Archive' FolderName,0 NewMsg,count(um.user_message_folders_fk) Total" ;
            strSQL += "  FROM User_Message_Trn UM WHERE UM.USER_MESSAGE_FOLDERS_FK = 2 and UM.DELETE_FLAG(+) is null  AND um.receiver_fk(+) = " + UserPK;
            //'
            strSQL += "  union  " ;
            strSQL += "  SELECT DISTINCT 2 FOLDERFK,";
            strSQL += "   'Archive' FOLDERNAME, COUNT(EMT.USER_MESSAGE_FOLDERS_FK) NEWMSG,0 TOTAL  FROM EXTERNAL_MESSAGE_TRN EMT";
            strSQL += "   WHERE EMT.USER_MESSAGE_FOLDERS_FK = 2   AND EMT.DELETE_FLAG(+) IS NULL  AND EMT.MSG_READ(+) = 1";


            strSQL += "  union  " ;
            strSQL += "  SELECT DISTINCT 2 FOLDERFK,";
            strSQL += "   'Archive' FOLDERNAME, 0 NEWMSG,COUNT(EMT.USER_MESSAGE_FOLDERS_FK) TOTAL  FROM EXTERNAL_MESSAGE_TRN EMT";
            strSQL += "   WHERE EMT.USER_MESSAGE_FOLDERS_FK = 2   AND EMT.DELETE_FLAG(+) IS NULL )";
            strSQL += "   GROUP BY FOLDERFK, FOLDERNAME";

            strSQL += " union " ;
            strSQL += "   SELECT FOLDERFK, FOLDERNAME, SUM(NEWMSG) || '/' || SUM(TOTAL) MSGS FROM(";
            strSQL += " SELECT 3 folderFk,'Draft' FolderName,   count(um.user_message_folders_fk) NewMsg, 0 Total  " ;
            strSQL += " FROM User_Message_Trn UM    " ;
            strSQL += " WHERE UM.DELETE_FLAG(+) is null   and um.user_message_folders_fk=3   AND um.receiver_fk(+) =" + UserPK;
            strSQL += " and  um.msg_read(+) = 1 " ;

            strSQL += " union " ;
            strSQL += " SELECT 3 folderFk,'Draft' FolderName,   0 NewMsg, count(um.user_message_folders_fk) Total  " ;
            strSQL += " FROM User_Message_Trn UM    " ;
            strSQL += " WHERE UM.DELETE_FLAG(+) is null    and um.user_message_folders_fk=3  AND um.receiver_fk(+) =" + UserPK;

            strSQL += "  union  " ;
            strSQL += " SELECT 3 FOLDERFK,'Draft' FOLDERNAME, COUNT(EMT.USER_MESSAGE_FOLDERS_FK) NEWMSG,  0 TOTAL " ;
            strSQL += "  FROM EXTERNAL_MESSAGE_TRN EMT " ;
            strSQL += "  WHERE EMT.DELETE_FLAG(+) IS NULL " ;
            strSQL += "  AND EMT.USER_MESSAGE_FOLDERS_FK = 3 " ;
            strSQL += "  AND EMT.MSG_READ(+) = 1 " ;

            strSQL += "  union  " ;
            strSQL += " SELECT 3 FOLDERFK,'Draft' FOLDERNAME,  0 NEWMSG,  COUNT(EMT.USER_MESSAGE_FOLDERS_FK) TOTAL " ;
            strSQL += "  FROM EXTERNAL_MESSAGE_TRN EMT " ;
            strSQL += "  WHERE EMT.DELETE_FLAG(+) IS NULL " ;
            strSQL += "  AND EMT.USER_MESSAGE_FOLDERS_FK = 3)" ;
            strSQL += "   GROUP BY FOLDERFK, FOLDERNAME";

            strSQL += " union  " ;
            strSQL += "   SELECT FOLDERFK, FOLDERNAME, SUM(NEWMSG) || '/' || SUM(TOTAL) MSGS FROM(";
            strSQL += " SELECT 4 folderFk, 'Sent' FolderName,COUNT(um.user_message_pk) NewMsg, 0 total " ;
            strSQL += " FROM User_Message_Trn UM   " ;
            strSQL += " WHERE  um.msg_read(+) = 1 " ;
            strSQL += " AND UM.del_sender_flag is null And UM.del_sentitem_flag IS NULL and UM.DELETE_FLAG(+) is null  And um.sender_fk(+) =" + UserPK;

            strSQL += " union  " ;
            strSQL += " SELECT 4 folderFk,'Sent' FolderName, 0 NewMsg, COUNT(um.user_message_pk) Total " ;
            strSQL += " FROM User_Message_Trn UM    " ;
            strSQL += " WHERE UM.del_sender_flag is null And UM.del_sentitem_flag IS NULL and UM.DELETE_FLAG(+) is null  AND um.sender_fk(+) =" + UserPK;

            strSQL += " union  " ;
            strSQL += " SELECT 4 FOLDERFK,'Sent' FOLDERNAME,COUNT(EMT.EXTERNAL_MSG_TRN_PK) NEWMSG,0 TOTAL  " ;
            strSQL += "  FROM EXTERNAL_MESSAGE_TRN EMT  " ;
            strSQL += " WHERE  EMT.MSG_READ(+) = 1 AND EMT.DEL_SENDER_FLAG IS NULL  " ;
            strSQL += " AND EMT.DEL_SENTITEM_FLAG IS NULL AND EMT.DELETE_FLAG(+) IS NULL  " ;
            strSQL += "  And emt.sender_fk(+) =" + UserPK;

            strSQL += " union  " ;
            strSQL += " SELECT 4 FOLDERFK,'Sent' FOLDERNAME, 0 NEWMSG,COUNT(EMT.EXTERNAL_MSG_TRN_PK) TOTAL  " ;
            strSQL += "  FROM EXTERNAL_MESSAGE_TRN EMT  " ;
            strSQL += " WHERE EMT.DEL_SENDER_FLAG IS NULL  " ;
            strSQL += "  And emt.sender_fk(+) =" + UserPK;
            strSQL += " AND EMT.DEL_SENTITEM_FLAG IS NULL AND EMT.DELETE_FLAG(+) IS NULL)   " ;
            strSQL += "  GROUP BY FOLDERFK, FOLDERNAME";

            strSQL += "  UNION";
            strSQL += "  SELECT FOLDERFK, FOLDERNAME, SUM(NEWMSG) || '/' || SUM(TOTAL) MSGS";
            strSQL += "   FROM (SELECT 5 FOLDERFK,";
            strSQL += "               'Deleted' FOLDERNAME,";
            strSQL += "               COUNT(UM.USER_MESSAGE_FOLDERS_FK) NEWMSG,";
            strSQL += "                0 TOTAL";
            strSQL += "          FROM USER_MESSAGE_TRN UM";
            strSQL += "         WHERE UM.DELETE_FLAG(+) IS NULL";
            strSQL += "            AND UM.USER_MESSAGE_FOLDERS_FK = 5";
            strSQL += "            AND UM.RECEIVER_FK(+)  =" + UserPK;
            strSQL += "           and  um.msg_read(+) = 1 ";
            strSQL += "         UNION";
            strSQL += "         SELECT 5 FOLDERFK,";
            strSQL += "               'Deleted' FOLDERNAME,";
            strSQL += "                0 NEWMSG,";
            strSQL += "               COUNT(UM.USER_MESSAGE_FOLDERS_FK) TOTAL";
            strSQL += "          FROM USER_MESSAGE_TRN UM";
            strSQL += "          WHERE UM.DELETE_FLAG(+) IS NULL";
            strSQL += "           AND UM.USER_MESSAGE_FOLDERS_FK = 5";
            strSQL += "            AND UM.RECEIVER_FK(+)  =" + UserPK;
            strSQL += "         UNION";
            strSQL += "         SELECT 5 FOLDERFK,";
            strSQL += "                'Deleted' FOLDERNAME,";
            strSQL += "                COUNT(EMT.USER_MESSAGE_FOLDERS_FK) NEWMSG,";
            strSQL += "                0 TOTAL";
            strSQL += "           FROM EXTERNAL_MESSAGE_TRN EMT";
            strSQL += "          WHERE EMT.DELETE_FLAG(+) IS NULL";
            strSQL += "            AND EMT.USER_MESSAGE_FOLDERS_FK = 5";
            strSQL += "         UNION";
            strSQL += "        SELECT 5 FOLDERFK,";
            strSQL += "               'Deleted' FOLDERNAME,";
            strSQL += "               0 NEWMSG,";
            strSQL += "               COUNT(EMT.USER_MESSAGE_FOLDERS_FK) TOTAL";
            strSQL += "           FROM EXTERNAL_MESSAGE_TRN EMT";
            strSQL += "         WHERE EMT.DELETE_FLAG(+) IS NULL";
            strSQL += "           AND EMT.USER_MESSAGE_FOLDERS_FK = 5)";
            strSQL += "  GROUP BY FOLDERFK, FOLDERNAME";

            strSQL += "   UNION";
            strSQL += "  SELECT FOLDERFK, FOLDERNAME, SUM(NEWMSG) || '/' || SUM(TOTAL) MSGS";
            strSQL += " FROM (SELECT 6 FOLDERFK,";
            strSQL += "                'History' FOLDERNAME,";
            strSQL += "                COUNT(UM.USER_MESSAGE_FOLDERS_FK) NEWMSG,";
            strSQL += "                0 TOTAL";
            strSQL += "           FROM USER_MESSAGE_TRN_ARCHIVE UM";
            strSQL += "        WHERE UM.DELETE_FLAG(+) IS NULL";
            strSQL += "            AND UM.USER_MESSAGE_FOLDERS_FK = 6";
            strSQL += "           AND UM.RECEIVER_FK(+)  =" + UserPK;
            strSQL += "           and  um.msg_read(+) = 1 ";
            strSQL += "         UNION";
            strSQL += "         SELECT 6 FOLDERFK,";
            strSQL += "               'History' FOLDERNAME,";
            strSQL += "               0 NEWMSG,";
            strSQL += "                COUNT(UM.USER_MESSAGE_FOLDERS_FK) TOTAL";
            strSQL += "          FROM USER_MESSAGE_TRN_ARCHIVE UM";
            strSQL += "         WHERE UM.DELETE_FLAG(+) IS NULL";
            strSQL += "            AND UM.USER_MESSAGE_FOLDERS_FK = 6";
            strSQL += "            AND UM.RECEIVER_FK(+)  =" + UserPK;
            strSQL += "        UNION";
            strSQL += "        SELECT 6 FOLDERFK,";
            strSQL += "                'History' FOLDERNAME,";
            strSQL += "                COUNT(EMT.USER_MESSAGE_FOLDERS_FK) NEWMSG,";
            strSQL += "                0 TOTAL";
            strSQL += "           FROM EXTERNAL_MESSAGE_TRN EMT";
            strSQL += "         WHERE EMT.DELETE_FLAG(+) IS NULL";
            strSQL += "           AND EMT.USER_MESSAGE_FOLDERS_FK = 6";
            strSQL += "        UNION";
            strSQL += "         SELECT 6 FOLDERFK,";
            strSQL += "                'History' FOLDERNAME,";
            strSQL += "                0 NEWMSG,";
            strSQL += "               COUNT(EMT.USER_MESSAGE_FOLDERS_FK) TOTAL";
            strSQL += "           FROM EXTERNAL_MESSAGE_TRN EMT";
            strSQL += "         WHERE EMT.DELETE_FLAG(+) IS NULL";
            strSQL += "           AND EMT.USER_MESSAGE_FOLDERS_FK = 6)";
            strSQL += "   GROUP BY FOLDERFK, FOLDERNAME";
            strSQL += "  order by folderFk " ;
            WorkFlow objWF = new WorkFlow();
            OracleDataReader objDR = default(OracleDataReader);
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

        #endregion

        #region "Delete Mails"
        public int DeleteMails(Int32 MailPK, bool blnSentItem)
        {
            StringBuilder strSqlBuilder = new StringBuilder();
            StringBuilder strSqlBuilder1 = new StringBuilder();

            WorkFlow objWK = new WorkFlow();
            try
            {
                objWK.OpenConnection();

                if (blnSentItem == true)
                {
                    strSqlBuilder = new StringBuilder();
                    strSqlBuilder.Append(" Update USER_MESSAGE_TRN msg");
                    strSqlBuilder.Append(" Set msg.DEL_SENTITEM_FLAG=1 Where ");
                    strSqlBuilder.Append(" msg.USER_MESSAGE_PK = " + MailPK + "");
                    strSqlBuilder.Append(" AND msg.DELETE_FLAG is null ");
                    strSqlBuilder1 = new StringBuilder();
                    strSqlBuilder1.Append(" Update EXTERNAL_MESSAGE_TRN EMT");
                    strSqlBuilder1.Append(" Set EMT.DEL_SENTITEM_FLAG=1 Where ");
                    strSqlBuilder1.Append(" EMT.EXTERNAL_MSG_TRN_PK = " + MailPK + "");
                    strSqlBuilder1.Append(" AND EMT.DELETE_FLAG is null ");
                    //'
                }
                else
                {
                    strSqlBuilder.Append(" DELETE ");
                    strSqlBuilder.Append(" FROM user_message_trn MsgTrn ");
                    strSqlBuilder.Append(" WHERE msgtrn.user_message_pk IN (");
                    strSqlBuilder.Append(" SELECT MsgDet.User_Message_Fk ");
                    strSqlBuilder.Append(" FROM User_Message_Det_Trn MsgDet");
                    strSqlBuilder.Append(" WHERE ");
                    strSqlBuilder.Append(" MSGDET.USER_MESSAGE_FK = " + MailPK + ")");
                    strSqlBuilder.Append(" AND MsgTrn.DELETE_FLAG = 1");
                    strSqlBuilder1 = new StringBuilder();
                    strSqlBuilder1.Append(" DELETE EXTERNAL_MESSAGE_TRN EMT");
                    strSqlBuilder1.Append("  Where ");
                    strSqlBuilder1.Append(" EMT.EXTERNAL_MSG_TRN_PK = " + MailPK + "");
                    strSqlBuilder1.Append(" AND EMT.DELETE_FLAG = 1 ");
                    //'
                    if (objWK.ExecuteCommands(strSqlBuilder.ToString()) | objWK.ExecuteCommands(strSqlBuilder1.ToString()))
                    {
                        return 1;
                    }
                    strSqlBuilder = new StringBuilder();
                    strSqlBuilder.Append(" Update USER_MESSAGE_TRN msg");
                    strSqlBuilder.Append(" Set msg.DELETE_FLAG=nvl(msg.DELETE_FLAG,0)+1 Where ");
                    strSqlBuilder.Append(" msg.USER_MESSAGE_PK = " + MailPK + "");
                    strSqlBuilder.Append(" AND msg.DELETE_FLAG is null ");

                    strSqlBuilder1 = new StringBuilder();
                    strSqlBuilder1.Append(" Update EXTERNAL_MESSAGE_TRN EMT");
                    strSqlBuilder1.Append(" Set EMT.DELETE_FLAG=nvl(EMT.DELETE_FLAG,0)+1 Where ");
                    strSqlBuilder1.Append("  EMT.EXTERNAL_MSG_TRN_PK = " + MailPK + "");
                    strSqlBuilder1.Append(" and EMT.DELETE_FLAG is null ");
                    //'
                }
                if (objWK.ExecuteCommands(strSqlBuilder.ToString()) | objWK.ExecuteCommands(strSqlBuilder1.ToString()))
                {
                    return 1;
                }
                else
                {
                    return -1;
                }
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
            }
            return 0;
        }
        #endregion

        #region " Update Read status"
        public int UpdateMsgReadStatus(Int32 MsgPK)
        {
            StringBuilder strSqlBuilder = new StringBuilder();
            StringBuilder strSqlBuilder1 = new StringBuilder();
            WorkFlow objWK = new WorkFlow();
            try
            {
                objWK.OpenConnection();
                strSqlBuilder.Append(" UPDATE User_Message_Trn");
                strSqlBuilder.Append(" SET msg_read = 1");
                strSqlBuilder.Append(" WHERE");
                strSqlBuilder.Append(" user_message_pk = " + MsgPK + "");
                strSqlBuilder1.Append(" UPDATE EXTERNAL_MESSAGE_TRN");
                strSqlBuilder1.Append(" SET msg_read = 1");
                strSqlBuilder1.Append(" WHERE");
                strSqlBuilder1.Append(" EXTERNAL_MSG_TRN_PK = " + MsgPK + "");
                //'
                if (objWK.ExecuteCommands(strSqlBuilder.ToString()) == true | objWK.ExecuteCommands(strSqlBuilder1.ToString()) == true)
                {
                    return 1;
                }
                else
                {
                    return -1;
                }
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region " Save Direct"
        public int SaveDirect(DataSet M_DataSet,OracleDataReader drUsers, int lngUserFk)
        {

            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = default(OracleTransaction);
            TRAN = objWK.MyConnection.BeginTransaction();
            objWK.MyCommand.Transaction = TRAN;
            objWK.MyCommand.Connection = objWK.MyConnection;
            try
            {
                int intExe = 0;
                int intPKVal = 0;
                var _with20 = objWK.MyCommand;
                while (drUsers.Read())
                {
                    objWK.MyCommand.CommandText = objWK.MyUserName + ".USER_MESSAGE_TRN_PKG.USER_MESSAGE_TRN_INS";
                    objWK.MyCommand.CommandType = CommandType.StoredProcedure;
                    var _with21 = _with20.Parameters;
                    _with21.Clear();
                    _with21.Add("SENDER_FK_IN", M_DataSet.Tables[0].Rows[0]["Sender_Fk"]).Direction = ParameterDirection.Input;
                    _with21.Add("RECEIVER_FK_IN", drUsers.GetValue(0)).Direction = ParameterDirection.Input;
                    _with21.Add("MSG_READ_IN", M_DataSet.Tables[0].Rows[0]["Msg_Read"]).Direction = ParameterDirection.Input;
                    _with21.Add("FOLLOWUP_FLAG_IN", M_DataSet.Tables[0].Rows[0]["Followup_Flag"]).Direction = ParameterDirection.Input;
                    _with21.Add("HAVE_ATTACHMENT_IN", M_DataSet.Tables[0].Rows[0]["Have_Attachment"]).Direction = ParameterDirection.Input;
                    _with21.Add("MSG_SUBJECT_IN", M_DataSet.Tables[0].Rows[0]["Msg_Subject"]).Direction = ParameterDirection.Input;
                    _with21.Add("MSG_BODY_IN", M_DataSet.Tables[0].Rows[0]["Msg_Body"]).Direction = ParameterDirection.Input;
                    _with21.Add("READ_RECEIPT_IN", M_DataSet.Tables[0].Rows[0]["Read_Receipt"]).Direction = ParameterDirection.Input;
                    _with21.Add("DOCUMENT_MST_FK_IN", M_DataSet.Tables[0].Rows[0]["Document_Mst_Fk"]).Direction = ParameterDirection.Input;
                    _with21.Add("USER_MESSAGE_FOLDERS_FK_IN", M_DataSet.Tables[0].Rows[0]["User_Message_Folders_Fk"]).Direction = ParameterDirection.Input;
                    _with21.Add("MSG_RECEIVED_DT_IN", M_DataSet.Tables[0].Rows[0]["Msg_Received_Dt"]).Direction = ParameterDirection.Input;
                    _with21.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                    _with21.Add("RETURN_VALUE", intPKVal).Direction = ParameterDirection.Output;
                    if (_with20.ExecuteNonQuery() == 1)
                    {
                        M_DataSet.Tables[0].Rows[0]["USER_MESSAGE_PK"] = objWK.MyCommand.Parameters["Return_value"].Value;
                        _with20.CommandText = objWK.MyUserName + ".USER_MESSAGE_DET_TRN_PKG.USER_MESSAGE_DET_TRN_INS";
                        _with20.CommandType = CommandType.StoredProcedure;
                        var _with22 = _with20.Parameters;
                        _with22.Clear();
                        _with22.Add("USER_MESSAGE_FK_IN", M_DataSet.Tables[0].Rows[0]["USER_MESSAGE_PK"]).Direction = ParameterDirection.Input;
                        _with22.Add("ATTACHMENT_CAPTION_IN", M_DataSet.Tables[0].Rows[0]["Msg_Subject"]).Direction = ParameterDirection.Input;
                        _with22.Add("ATTACHMENT_DATA_IN", M_DataSet.Tables[1].Rows[0]["ATTACHMENT_DATA"]).Direction = ParameterDirection.Input;
                        _with22.Add("URL_PAGE_IN", M_DataSet.Tables[1].Rows[0]["URL_PAGE"]).Direction = ParameterDirection.Input;
                        objWK.MyCommand.Parameters["URL_PAGE_IN"].Size = 200;
                        _with22.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                        _with22.Add("RETURN_VALUE", intPKVal).Direction = ParameterDirection.Output;
                        objWK.MyCommand.Parameters["RETURN_VALUE"].Size = 10;
                        intExe = _with20.ExecuteNonQuery();
                    }
                    else
                    {
                        arrMessage.Add("Record Not Saved");
                    }
                }
                if (intExe == 1)
                {
                    TRAN.Commit();
                    return 1;
                }
                else
                {
                    TRAN.Rollback();
                    return -1;
                }
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                arrMessage.Add("Record Not Saved");
                return -1;
            }
            finally
            {
                objWK.CloseConnection();
            }
        }
        #endregion

        #region " ExternalSave Direct"
        public int ExternalSaveDirect(DataSet M_DataSet)
        {

            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = default(OracleTransaction);
            TRAN = objWK.MyConnection.BeginTransaction();
            objWK.MyCommand.Transaction = TRAN;
            objWK.MyCommand.Connection = objWK.MyConnection;
            try
            {
                int intExe = 0;
                int intPKVal = 0;
                var _with23 = objWK.MyCommand;
                objWK.MyCommand.CommandText = objWK.MyUserName + ".EXTERNAL_MESSAGE_TRN_PKG.EXTERNAL_MESSAGE_TRN_INS";
                objWK.MyCommand.CommandType = CommandType.StoredProcedure;
                var _with24 = _with23.Parameters;
                _with24.Clear();
                _with24.Add("SENDER_FK_IN", M_DataSet.Tables[0].Rows[0]["SENDER_FK"]).Direction = ParameterDirection.Input;
                _with24.Add("EX_USER_ID_IN", M_DataSet.Tables[0].Rows[0]["EX_USER_ID"]).Direction = ParameterDirection.Input;
                _with24.Add("EX_EMAIL_ID_IN", M_DataSet.Tables[0].Rows[0]["EX_EMAIL_ID"]).Direction = ParameterDirection.Input;
                _with24.Add("MSG_SUBJECT_IN", M_DataSet.Tables[0].Rows[0]["MSG_SUBJECT"]).Direction = ParameterDirection.Input;
                _with24.Add("MSG_RECEIVED_DT_IN", M_DataSet.Tables[0].Rows[0]["MSG_RECEIVED_DT"]).Direction = ParameterDirection.Input;
                _with24.Add("MSG_BODY_IN", M_DataSet.Tables[0].Rows[0]["MSG_BODY"]).Direction = ParameterDirection.Input;
                _with24.Add("USER_MESSAGE_FOLDERS_FK_IN", M_DataSet.Tables[0].Rows[0]["USER_MESSAGE_FOLDERS_FK"]).Direction = ParameterDirection.Input;
                _with24.Add("MSG_READ_IN", M_DataSet.Tables[0].Rows[0]["MSG_READ"]).Direction = ParameterDirection.Input;
                _with24.Add("CREATED_BY_FK_IN", M_DataSet.Tables[0].Rows[0]["CREATED_BY_FK"]).Direction = ParameterDirection.Input;
                _with24.Add("RETURN_VALUE", intPKVal).Direction = ParameterDirection.Output;
                intExe = _with23.ExecuteNonQuery();
                if (intExe == 1)
                {
                    TRAN.Commit();
                    return 1;
                }
                else
                {
                    TRAN.Rollback();
                    return -1;
                }
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                arrMessage.Add("Record Not Saved");
                return -1;
            }
            finally
            {
                objWK.CloseConnection();
            }
        }
        #endregion

        #region " Save Direct Single user"
        public int SaveSingleUser(DataSet M_DataSet, string drUsers, int lngUserFk)
        {

            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = default(OracleTransaction);
            TRAN = objWK.MyConnection.BeginTransaction();
            objWK.MyCommand.Transaction = TRAN;
            objWK.MyCommand.Connection = objWK.MyConnection;
            try
            {
                int intExe = 0;
                int intPKVal = 0;
                var _with25 = objWK.MyCommand;

                objWK.MyCommand.CommandText = objWK.MyUserName + ".USER_MESSAGE_TRN_PKG.USER_MESSAGE_TRN_INS";
                objWK.MyCommand.CommandType = CommandType.StoredProcedure;
                var _with26 = _with25.Parameters;
                _with26.Clear();
                _with26.Add("SENDER_FK_IN", M_DataSet.Tables[0].Rows[0]["Sender_Fk"]).Direction = ParameterDirection.Input;
                _with26.Add("RECEIVER_FK_IN", drUsers).Direction = ParameterDirection.Input;
                _with26.Add("MSG_READ_IN", M_DataSet.Tables[0].Rows[0]["Msg_Read"]).Direction = ParameterDirection.Input;
                _with26.Add("FOLLOWUP_FLAG_IN", M_DataSet.Tables[0].Rows[0]["Followup_Flag"]).Direction = ParameterDirection.Input;
                _with26.Add("HAVE_ATTACHMENT_IN", M_DataSet.Tables[0].Rows[0]["Have_Attachment"]).Direction = ParameterDirection.Input;
                _with26.Add("MSG_SUBJECT_IN", M_DataSet.Tables[0].Rows[0]["Msg_Subject"]).Direction = ParameterDirection.Input;
                _with26.Add("MSG_BODY_IN", M_DataSet.Tables[0].Rows[0]["Msg_Body"]).Direction = ParameterDirection.Input;
                _with26.Add("READ_RECEIPT_IN", M_DataSet.Tables[0].Rows[0]["Read_Receipt"]).Direction = ParameterDirection.Input;
                _with26.Add("DOCUMENT_MST_FK_IN", M_DataSet.Tables[0].Rows[0]["Document_Mst_Fk"]).Direction = ParameterDirection.Input;
                _with26.Add("USER_MESSAGE_FOLDERS_FK_IN", M_DataSet.Tables[0].Rows[0]["User_Message_Folders_Fk"]).Direction = ParameterDirection.Input;
                _with26.Add("MSG_RECEIVED_DT_IN", M_DataSet.Tables[0].Rows[0]["Msg_Received_Dt"]).Direction = ParameterDirection.Input;
                _with26.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                _with26.Add("RETURN_VALUE", intPKVal).Direction = ParameterDirection.Output;
                if (_with25.ExecuteNonQuery() == 1)
                {
                    M_DataSet.Tables[0].Rows[0]["USER_MESSAGE_PK"] = objWK.MyCommand.Parameters["Return_value"].Value;
                    _with25.CommandText = objWK.MyUserName + ".USER_MESSAGE_DET_TRN_PKG.USER_MESSAGE_DET_TRN_INS";
                    _with25.CommandType = CommandType.StoredProcedure;
                    var _with27 = _with25.Parameters;
                    _with27.Clear();
                    _with27.Add("USER_MESSAGE_FK_IN", M_DataSet.Tables[0].Rows[0]["USER_MESSAGE_PK"]).Direction = ParameterDirection.Input;
                    _with27.Add("ATTACHMENT_CAPTION_IN", M_DataSet.Tables[0].Rows[0]["Msg_Subject"]).Direction = ParameterDirection.Input;
                    _with27.Add("ATTACHMENT_DATA_IN", M_DataSet.Tables[1].Rows[0]["Attachment_Data"]).Direction = ParameterDirection.Input;
                    _with27.Add("URL_PAGE_IN", "").Direction = ParameterDirection.Input;
                    objWK.MyCommand.Parameters["URL_PAGE_IN"].Size = 200;
                    _with27.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                    _with27.Add("RETURN_VALUE", intPKVal).Direction = ParameterDirection.Output;
                    objWK.MyCommand.Parameters["RETURN_VALUE"].Size = 10;
                    intExe = _with25.ExecuteNonQuery();
                }
                else
                {
                    arrMessage.Add("Record Not Saved");
                }

                if (intExe == 1)
                {
                    TRAN.Commit();
                    return 1;
                }
                else
                {
                    TRAN.Rollback();
                    return -1;
                }
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                arrMessage.Add("Record Not Saved");
                return -1;
            }
            finally
            {
                objWK.CloseConnection();
            }
        }
        #endregion

        #region " Save Requests"
        public int SaveRequest(DataSet M_DataSet)
        {

            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = default(OracleTransaction);
            TRAN = objWK.MyConnection.BeginTransaction();
            objWK.MyCommand.Transaction = TRAN;
            objWK.MyCommand.Connection = objWK.MyConnection;
            try
            {
                int lngPkValue = 0;
                var _with28 = objWK.MyCommand;
                objWK.MyCommand.CommandText = objWK.MyUserName + ".USER_MESSAGE_TRN_PKG.USER_MESSAGE_TRN_INS";
                objWK.MyCommand.CommandType = CommandType.StoredProcedure;
                var _with29 = _with28.Parameters;
                _with29.Clear();
                _with29.Add("SENDER_FK_IN", M_DataSet.Tables[0].Rows[0]["Sender_Fk"]).Direction = ParameterDirection.Input;
                _with29.Add("RECEIVER_FK_IN", M_DataSet.Tables[0].Rows[0]["Receiver_Fk"]).Direction = ParameterDirection.Input;
                _with29.Add("MSG_READ_IN", M_DataSet.Tables[0].Rows[0]["Msg_Read"]).Direction = ParameterDirection.Input;
                _with29.Add("FOLLOWUP_FLAG_IN", M_DataSet.Tables[0].Rows[0]["Followup_Flag"]).Direction = ParameterDirection.Input;
                _with29.Add("HAVE_ATTACHMENT_IN", M_DataSet.Tables[0].Rows[0]["Have_Attachment"]).Direction = ParameterDirection.Input;
                _with29.Add("MSG_SUBJECT_IN", M_DataSet.Tables[0].Rows[0]["Msg_Subject"]).Direction = ParameterDirection.Input;
                _with29.Add("MSG_BODY_IN", M_DataSet.Tables[0].Rows[0]["Msg_Body"]).Direction = ParameterDirection.Input;
                _with29.Add("READ_RECEIPT_IN", M_DataSet.Tables[0].Rows[0]["Read_Receipt"]).Direction = ParameterDirection.Input;
                _with29.Add("DOCUMENT_MST_FK_IN", M_DataSet.Tables[0].Rows[0]["Document_Mst_Fk"]).Direction = ParameterDirection.Input;
                _with29.Add("USER_MESSAGE_FOLDERS_FK_IN", M_DataSet.Tables[0].Rows[0]["User_Message_Folders_Fk"]).Direction = ParameterDirection.Input;
                _with29.Add("MSG_RECEIVED_DT_IN", M_DataSet.Tables[0].Rows[0]["Msg_Received_Dt"]).Direction = ParameterDirection.Input;
                _with29.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                _with29.Add("RETURN_VALUE", lngPkValue).Direction = ParameterDirection.Output;

                if (_with28.ExecuteNonQuery() == 1)
                {
                    M_DataSet.Tables[0].Rows[0]["USER_MESSAGE_PK"] = objWK.MyCommand.Parameters["Return_value"].Value;
                    _with28.CommandText = objWK.MyUserName + ".USER_MESSAGE_DET_TRN_PKG.USER_MESSAGE_DET_TRN_INS";
                    _with28.CommandType = CommandType.StoredProcedure;
                    var _with30 = _with28.Parameters;
                    _with30.Clear();
                    _with30.Add("USER_MESSAGE_FK_IN", M_DataSet.Tables[0].Rows[0]["USER_MESSAGE_PK"]).Direction = ParameterDirection.Input;
                    _with30.Add("ATTACHMENT_CAPTION_IN", M_DataSet.Tables[0].Rows[0]["Msg_Subject"]).Direction = ParameterDirection.Input;
                    _with30.Add("ATTACHMENT_DATA_IN", M_DataSet.Tables[1].Rows[0]["Attachment_Data"]).Direction = ParameterDirection.Input;
                    _with30.Add("URL_PAGE_IN", M_DataSet.Tables[1].Rows[0]["URL_PAGE"]).Direction = ParameterDirection.Input;
                    _with30.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                    _with30.Add("RETURN_VALUE", lngPkValue).Direction = ParameterDirection.Output;
                    if (_with28.ExecuteNonQuery() == 1)
                    {
                        TRAN.Commit();
                        return 1;
                    }
                    else
                    {
                        TRAN.Rollback();
                        return -1;
                    }
                }
                else
                {
                    arrMessage.Add("Record Not Saved");
                }
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                arrMessage.Add("Record Not Saved");
            }
            finally
            {
                objWK.CloseConnection();
            }
            return 0;
        }
        #endregion

        #region " Save Messages"
        public int SaveMessages(DataSet M_DataSet, OracleTransaction TRAN, Int32 SRRPK)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.MyConnection = TRAN.Connection;
            objWK.MyCommand.Transaction = TRAN;
            objWK.MyCommand.Connection = TRAN.Connection;
            try
            {
                int intPKVal = 0;
                var _with31 = objWK.MyCommand.Parameters;
                _with31.Clear();
                _with31.Add("SENDER_FK_IN", M_DataSet.Tables[0].Rows[0]["Sender_Fk"]).Direction = ParameterDirection.Input;
                _with31.Add("RECEIVER_FK_IN", M_DataSet.Tables[0].Rows[0]["Receiver_Fk"]).Direction = ParameterDirection.Input;
                _with31.Add("MSG_READ_IN", M_DataSet.Tables[0].Rows[0]["Msg_Read"]).Direction = ParameterDirection.Input;
                _with31.Add("FOLLOWUP_FLAG_IN", M_DataSet.Tables[0].Rows[0]["Followup_Flag"]).Direction = ParameterDirection.Input;
                _with31.Add("HAVE_ATTACHMENT_IN", M_DataSet.Tables[0].Rows[0]["Have_Attachment"]).Direction = ParameterDirection.Input;
                _with31.Add("MSG_SUBJECT_IN", M_DataSet.Tables[0].Rows[0]["Msg_Subject"]).Direction = ParameterDirection.Input;
                _with31.Add("MSG_BODY_IN", M_DataSet.Tables[0].Rows[0]["Msg_Body"]).Direction = ParameterDirection.Input;
                _with31.Add("READ_RECEIPT_IN", M_DataSet.Tables[0].Rows[0]["Read_Receipt"]).Direction = ParameterDirection.Input;
                _with31.Add("DOCUMENT_MST_FK_IN", M_DataSet.Tables[0].Rows[0]["Document_Mst_Fk"]).Direction = ParameterDirection.Input;
                _with31.Add("USER_MESSAGE_FOLDERS_FK_IN", M_DataSet.Tables[0].Rows[0]["User_Message_Folders_Fk"]).Direction = ParameterDirection.Input;
                _with31.Add("MSG_RECEIVED_DT_IN", M_DataSet.Tables[0].Rows[0]["Msg_Received_Dt"]).Direction = ParameterDirection.Input;
                _with31.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                _with31.Add("RETURN_VALUE", intPKVal).Direction = ParameterDirection.Output;
                objWK.MyCommand.CommandText = objWK.MyUserName + ".USER_MESSAGE_TRN_PKG.USER_MESSAGE_TRN_INS";
                objWK.MyCommand.CommandType = CommandType.StoredProcedure;
                if (objWK.MyCommand.ExecuteNonQuery() == 1)
                {
                    M_DataSet.Tables[0].Rows[0]["USER_MESSAGE_PK"] = objWK.MyCommand.Parameters["Return_value"].Value;
                    string strURL = null;
                    strURL = Convert.ToString(M_DataSet.Tables[1].Rows[0]["Url_Page"]) + SRRPK;
                    var _with32 = objWK.MyCommand.Parameters;
                    _with32.Clear();
                    _with32.Add("USER_MESSAGE_FK_IN", M_DataSet.Tables[0].Rows[0]["USER_MESSAGE_PK"]).Direction = ParameterDirection.Input;
                    _with32.Add("ATTACHMENT_CAPTION_IN", M_DataSet.Tables[0].Rows[0]["Msg_Subject"]).Direction = ParameterDirection.Input;
                    _with32.Add("ATTACHMENT_DATA_IN", M_DataSet.Tables[1].Rows[0]["Attachment_Data"]).Direction = ParameterDirection.Input;
                    _with32.Add("URL_PAGE_IN", strURL).Direction = ParameterDirection.Input;
                    _with32.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                    _with32.Add("RETURN_VALUE", intPKVal).Direction = ParameterDirection.Output;
                    objWK.MyCommand.CommandText = objWK.MyUserName + ".USER_MESSAGE_DET_TRN_PKG.USER_MESSAGE_DET_TRN_INS";
                    objWK.MyCommand.CommandType = CommandType.StoredProcedure;
                }
                else
                {
                    arrMessage.Add("Record Not Saved");
                }
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWK.CloseConnection();
            }
            return 0;
        }

        #endregion
        #region "Check Approver"
        public bool CheckApprover(string strDocId, long RequesterFk, bool chkDocDefined)
        {
            DataSet dsDoc = null;
            DataSet dsUserFks = null;
            long lngdocPk = 0;
            long SenderLocFk = 0;
            long returnVal = 0;
            WorkFlow objWK = new WorkFlow();
            bool isValidApprover = false;
            try
            {
                dsDoc = FetchDocument(strDocId);
                if (dsDoc.Tables[0].Rows.Count > 0)
                {
                    chkDocDefined = true;
                    lngdocPk = Convert.ToInt64(dsDoc.Tables[0].Rows[0]["DOCUMENT_MST_PK"]);
                    objWK.OpenConnection();
                    var _with33 = objWK.MyCommand.Parameters;
                    _with33.Clear();
                    _with33.Add("APPROVER_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                    _with33.Add("REQUESTER_FK_IN", RequesterFk).Direction = ParameterDirection.Input;
                    _with33.Add("REQ_DOC_FK_IN", lngdocPk).Direction = ParameterDirection.Input;
                    _with33.Add("RETURN_VALUE", returnVal).Direction = ParameterDirection.Output;
                    objWK.MyCommand.CommandText = objWK.MyUserName + ".USER_MESSAGE_TRN_PKG.VALIDATE_APPROVER";
                    objWK.MyCommand.CommandType = CommandType.StoredProcedure;
                    if (objWK.MyCommand.ExecuteNonQuery() == 1)
                    {
                        returnVal = Convert.ToInt64(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);
                        if (returnVal == 0)
                        {
                            return false;
                        }
                        else if (returnVal == 1)
                        {
                            return true;
                        }
                        else if (returnVal == 2)
                        {
                            chkDocDefined = false;
                        }
                    }

                }
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWK.CloseConnection();
            }
            return false;
        }
        #endregion

        #region " Get User Information "
        public DataSet GetApproverInfo(long lngDocumentPk, long ReqByUserFk)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder strSqlBuilder = new StringBuilder();
            try
            {
                strSqlBuilder.Append(" SELECT  ");
                strSqlBuilder.Append(" WF.USER_MST_FK,WF.COPY_TO1_FK,COPY_TO2_FK,COPY_TO3_FK," + ReqByUserFk + "");
                strSqlBuilder.Append(" FROM WORKFLOW_RULES_TRN WF, DOCUMENT_MST_TBL DOC,WORKFLOW_LOC_TRN WFL");
                strSqlBuilder.Append(" WHERE  ");
                strSqlBuilder.Append(" WF.DOCUMENT_MST_FK = DOC.DOCUMENT_MST_PK ");
                strSqlBuilder.Append(" AND WFL.WORKFLOW_RULES_FK = WF.WORKFLOW_RULES_PK ");
                strSqlBuilder.Append(" AND WF.DOCUMENT_MST_FK = " + lngDocumentPk + "");
                strSqlBuilder.Append(" AND TO_DATE(SYSDATE,'DD/MM/YYYY') >= WF.VALID_FROM  AND TO_DATE(SYSDATE,'DD/MM/YYYY') <= NVL(WF.VALIDTO, '31/12/2999') ");
                strSqlBuilder.Append(" AND WFL.FROM_LOC_MST_FK = (SELECT UMT.DEFAULT_LOCATION_FK ");
                strSqlBuilder.Append(" FROM USER_MST_TBL UMT WHERE UMT.USER_MST_PK= " + ReqByUserFk + ")");
                strSqlBuilder.Append(" AND WF.ACTIVE=1 ");
                return objWF.GetDataSet(strSqlBuilder.ToString());
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion

        #region "Update Mail Status"
        private int UpdateMailStatus(int WorkFlowPK, string RefNr, string MailSubject, int SendPK, int ReceivePK, string MsgBody = "")
        {
            WorkFlow objWK = new WorkFlow();
            int RetVal = 0;
            string MailDesc = null;
            string SendFrom = null;
            string SendTo = null;

            OracleTransaction InsTrans = default(OracleTransaction);
            SendFrom = objWK.ExecuteScaler("select umt.user_id from user_mst_tbl umt where umt.user_mst_pk=" + SendPK);
            SendTo = objWK.ExecuteScaler("select umt.user_id from user_mst_tbl umt where umt.user_mst_pk=" + ReceivePK);
            objWK.OpenConnection();
            InsTrans = objWK.MyConnection.BeginTransaction();
            objWK.MyCommand.Transaction = InsTrans;
            objWK.MyCommand.Connection = objWK.MyConnection;
            objWK.MyCommand.Parameters.Clear();
            MailDesc = "Mail Send Successfully";

            try
            {
                var _with34 = objWK.MyCommand;
                _with34.CommandType = CommandType.StoredProcedure;
                _with34.CommandText = objWK.MyUserName + ".MAIL_SEND_STATUS_TBL_PKG.MAIL_SEND_STATUS_INS";
                _with34.Parameters.Add("MAIL_TYPE_IN", 3).Direction = ParameterDirection.Input;
                _with34.Parameters.Add("DOC_TYPE_IN", WorkFlowPK).Direction = ParameterDirection.Input;
                _with34.Parameters.Add("DOC_REF_NR_IN", RefNr).Direction = ParameterDirection.Input;
                _with34.Parameters.Add("MAIL_SUBJECT_IN", MailSubject).Direction = ParameterDirection.Input;
                _with34.Parameters.Add("SEND_FROM_IN", SendFrom).Direction = ParameterDirection.Input;
                _with34.Parameters.Add("SEND_TO_IN", SendTo).Direction = ParameterDirection.Input;
                _with34.Parameters.Add("MAIL_STATUS_IN", 1).Direction = ParameterDirection.Input;
                _with34.Parameters.Add("MAIL_DESC_IN", MailDesc).Direction = ParameterDirection.Input;
                _with34.Parameters.Add("MAIL_DESC_DTL_IN", MailDesc).Direction = ParameterDirection.Input;
                if (TaskDocCreatedBy > 0)
                {
                    _with34.Parameters.Add("CREATED_BY_FK_IN", TaskDocCreatedBy).Direction = ParameterDirection.Input;
                }
                else
                {
                    _with34.Parameters.Add("CREATED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                }
                _with34.Parameters.Add("MAIL_BODY_IN", MsgBody).Direction = ParameterDirection.Input;
                if ((FileName != null))
                {
                    FileName = FileName;
                }
                else
                {
                    FileName = "";
                }
                _with34.Parameters.Add("MAIL_FILE_IN", FileName).Direction = ParameterDirection.Input;
                if ((FileName != null))
                {
                    FileName = FileName;
                }
                else
                {
                    FileName = "";
                }

                _with34.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                RetVal = _with34.ExecuteNonQuery();
                InsTrans.Commit();
                FileName = "";
                return 1;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                InsTrans.Rollback();
                throw ex;
            }
            finally
            {
                objWK.MyConnection.Close();
            }
        }
        #endregion

        #region "FetchCustomer "
        public DataSet FetchCustomer(long lngPkValue)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();

            sb.Append("SELECT CMT.CUSTOMER_ID,CMT.CUSTOMER_NAME,NVL(EMP2.EMPLOYEE_NAME,EMP1.EMPLOYEE_NAME) AS EMPLOYEE_NAME,DECODE(CMT.SALES_EXE_MAP_STATUS,2,'Approved',3,'Rejected','Request') AS STATUS FROM CUSTOMER_MST_TBL CMT,EMPLOYEE_MST_TBL EMP1,EMPLOYEE_MST_TBL EMP2 WHERE");
            sb.Append(" CMT.REP_EMP_MST_FK=EMP1.EMPLOYEE_MST_PK(+) AND CMT.REQ_SALES_EXE=EMP2.EMPLOYEE_MST_PK(+)");
            sb.Append(" AND CMT.CUSTOMER_MST_PK=" + lngPkValue);
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion

        #region "Convert HTML Body"
        public string ConvertHTML(string strBody)
        {
            System.Text.StringBuilder strHtml = new System.Text.StringBuilder();
            Array ArrList = null;
            Array LineSplit = null;
            string MsgText = null;
            int j = 0;
            strHtml.Append("<Table border='0' width='100%' cellspacing='8' cellpading='0'>");
            if (!string.IsNullOrEmpty(strBody))
            {
                MsgText = strBody.Replace(Environment.NewLine, "~");
                MsgText = MsgText.Replace(Environment.NewLine, "~");
                MsgText = MsgText.Replace(MsgText[10], '~');
                ArrList = MsgText.ToString().Split('~');
                for (j = 0; j <= ArrList.Length - 1; j++)
                {
                    strHtml.Append("<tr>");
                    if (string.Compare(ArrList.GetValue(j).ToString(), ":") == 0)
                    {
                        strHtml.Append("<td class='MsgBody' colspan=\"3\">");
                        strHtml.Append(ArrList.GetValue(j).ToString());
                        strHtml.Append("</td>");
                    }
                    else
                    {
                        DateTime dt = DateTime.Parse(ArrList.GetValue(j).ToString());
                        if (!((dt.Month != System.DateTime.Now.Month) || (dt.Day < 1 && dt.Day > 31) || dt.Year != System.DateTime.Now.Year))
                        {
                            LineSplit = ArrList.GetValue(j).ToString().Split(':');
                            strHtml.Append("<td class='MsgBody' style=\"width:20%\">");
                            strHtml.Append(LineSplit.GetValue(0));
                            strHtml.Append("</td>");
                            strHtml.Append("<td class='MsgBody' style=\"width:5%\">:</td>");
                            strHtml.Append("<td class='MsgBody' style=\"width:75%\">");
                            strHtml.Append(LineSplit.GetValue(1));
                            strHtml.Append("</td>");
                        }
                        else
                        {
                            strHtml.Append("<td class='MsgBody' colspan=\"3\">");
                            strHtml.Append(ArrList.GetValue(j).ToString());
                            strHtml.Append("</td>");
                        }
                    }
                    strHtml.Append("</tr>");
                }
            }
            return strHtml.ToString();
        }
        #endregion

        #region "SendMessageNew "
        public string SendRestrictionMsg(long lngCreatedBy, long lngPkValue, string strDocId, long lngLocFk, string CustName, string ReferenceType, string ReferenceNr, string TranDate, string RestrictType, string RestrictMsg,
        string Status, long SenderFk, Int16 FormType, string strReqDocId = "", int ApprovalFlg = 0, int ReqByUserFk = 0, bool OperatorRestriction = false, string strURL = "")
        {
            string functionReturnValue = null;
            DataSet dsMsg = new DataSet();
            DataSet dsDoc = null;
            DataSet dsUser = new DataSet();
            DataSet dsSender = new DataSet();
            string strUsrName = null;
            string strLocName = null;
            string strMsgSub = null;
            string strMsgHeader = null;
            string strMsgFooter = null;
            string strMsgBody = null;
            DataRow DR = null;
            DataSet dsUserFks = null;
            long lngdocPk = 0;
            string strSenderLocName = null;
            string strSenderUsrName = null;
            string strPageURL = null;
            long lngUserFk = 0;
            int intSenderCnt = 0;
            int Cargo_Type = 0;
            WorkFlow objWF = new WorkFlow();
            DocRefNr = "";
            try
            {
                dsDoc = FetchDocument(strDocId);
                if (dsDoc.Tables[0].Rows.Count > 0)
                {
                    lngdocPk = Convert.ToInt64(dsDoc.Tables[0].Rows[0]["DOCUMENT_MST_PK"]);
                    dsMsg = GetMessageInfo(lngdocPk, -1);
                    if (dsMsg.Tables[0].Rows.Count < 1)
                    {
                        DR = dsMsg.Tables[0].NewRow();
                        dsMsg.Tables[0].Rows.Add(DR);
                    }
                    if (ApprovalFlg == 1)
                    {
                        dsDoc = FetchDocument(strReqDocId);
                        if (dsDoc.Tables[0].Rows.Count > 0)
                        {
                            lngdocPk = Convert.ToInt64(dsDoc.Tables[0].Rows[0]["DOCUMENT_MST_PK"]);
                        }
                        dsUserFks = GetApproverInfo(lngdocPk, ReqByUserFk);
                    }
                    else
                    {
                        dsUserFks = GetUserInfo(lngdocPk, lngLocFk, SenderFk, ReqByUserFk);
                    }
                    if (dsUserFks.Tables[0].Rows.Count > 0)
                    {
                        for (intSenderCnt = 0; intSenderCnt <= dsUserFks.Tables[0].Columns.Count - 1; intSenderCnt++)
                        {
                            if (!(string.IsNullOrEmpty(dsUserFks.Tables[0].Rows[0][intSenderCnt].ToString())))
                            {
                                if ((Convert.ToInt32(dsUserFks.Tables[0].Rows[0][intSenderCnt])) > 0)
                                {
                                    dsUser = FetchUserDetails(Convert.ToInt64(dsUserFks.Tables[0].Rows[0][intSenderCnt]));
                                    strUsrName = Convert.ToString(dsUser.Tables[0].Rows[0]["USER_NAME"]);
                                    strLocName = Convert.ToString(dsUser.Tables[0].Rows[0]["LOCATION_NAME"]);
                                    if (dsMsg.Tables[0].Rows.Count > 0)
                                    {
                                        var _with35 = dsMsg.Tables[0].Rows[0];
                                        _with35["Sender_Fk"] = lngCreatedBy;
                                        dsSender = FetchUserDetails(lngCreatedBy);
                                        strSenderUsrName = Convert.ToString(dsSender.Tables[0].Rows[0]["USER_NAME"]);
                                        strSenderLocName = Convert.ToString(dsSender.Tables[0].Rows[0]["LOCATION_NAME"]);

                                        _with35["Receiver_Fk"] = Convert.ToInt64(dsUserFks.Tables[0].Rows[0][intSenderCnt]);
                                        _with35["Msg_Read"] = 0;
                                        _with35["Followup_Flag"] = 0;
                                        _with35["Have_Attachment"] = 1;

                                        strMsgBody = Convert.ToString(_with35["Msg_Body"]);
                                        strMsgBody = strMsgBody.Replace("<<", "<");
                                        strMsgBody = strMsgBody.Replace("&lt;&lt;", "<");
                                        strMsgBody = strMsgBody.Replace(">>", ">");
                                        strMsgBody = strMsgBody.Replace("&gt;&gt;", ">");
                                        strMsgBody = strMsgBody.Replace("<SENDER>", (strSenderUsrName + ", " + strSenderLocName));
                                        strMsgBody = strMsgBody.Replace("<RECEIVER>", strUsrName);
                                        strMsgBody = strMsgBody.Replace("<CURRENT DATE>", Convert.ToString(DateTime.Now.Date));
                                        strMsgBody = strMsgBody.Replace("<CUSTOMER NAME>", CustName);
                                        strMsgBody = strMsgBody.Replace("<REFERENCE TYPE>", ReferenceType);
                                        strMsgBody = strMsgBody.Replace("<REFERENCE NR>", ReferenceNr);
                                        strMsgBody = strMsgBody.Replace("<DATE>", TranDate);
                                        strMsgBody = strMsgBody.Replace("<RESTRICTION TYPE>", RestrictType);
                                        strMsgBody = strMsgBody.Replace("<RESTRICTION MSG>", RestrictMsg);
                                        strMsgBody = strMsgBody.Replace("<STATUS>", Status);
                                        if (OperatorRestriction == true)
                                        {
                                            strMsgBody = strMsgBody.Replace("Customer Name", "Line");
                                        }
                                        _with35["Msg_Body"] = strMsgBody;

                                        strMsgSub = Convert.ToString(_with35["MSG_SUBJECT"]);
                                        strMsgSub = strMsgSub.Replace("<<", "<");
                                        strMsgSub = strMsgSub.Replace("&lt;&lt;", "<");
                                        strMsgSub = strMsgSub.Replace(">>", ">");
                                        strMsgSub = strMsgSub.Replace("&gt;&gt;", ">");
                                        strMsgSub = strMsgSub.Replace("<SENDER>", (strSenderUsrName + ", " + strSenderLocName));
                                        strMsgSub = strMsgSub.Replace("<RECEIVER>", strUsrName);
                                        strMsgSub = strMsgSub.Replace("<CURRENT DATE>", Convert.ToString(DateTime.Now.Date));
                                        strMsgSub = strMsgSub.Replace("<CUSTOMER NAME>", CustName);
                                        strMsgSub = strMsgSub.Replace("<REFERENCE TYPE>", ReferenceType);
                                        strMsgSub = strMsgSub.Replace("<REFERENCE NR>", ReferenceNr);
                                        strMsgSub = strMsgSub.Replace("<DATE>", TranDate);
                                        strMsgSub = strMsgSub.Replace("<RESTRICTION TYPE>", RestrictType);
                                        strMsgSub = strMsgSub.Replace("<RESTRICTION MSG>", RestrictMsg);
                                        strMsgSub = strMsgSub.Replace("<STATUS>", Status);
                                        _with35["Msg_Subject"] = strMsgSub;

                                        _with35["Read_Receipt"] = 0;
                                        _with35["Document_Mst_Fk"] = lngdocPk;
                                        _with35["User_Message_Folders_Fk"] = 1;
                                        _with35["Msg_Received_Dt"] = DateTime.Now;
                                    }
                                    else
                                    {
                                        return "Please define WorkFlow for the particular document";
                                    }
                                    if (dsMsg.Tables[1].Rows.Count < 1)
                                    {
                                        DR = dsMsg.Tables[1].NewRow();
                                        dsMsg.Tables[1].Rows.Add(DR);
                                    }
                                    if (dsMsg.Tables[1].Rows.Count > 0)
                                    {
                                        var _with36 = dsMsg.Tables[1].Rows[0];
                                        if (!string.IsNullOrEmpty(_with36["URL_PAGE"].ToString()))
                                        {
                                            strPageURL = Convert.ToString(_with36["URL_PAGE"]);
                                        }
                                        else
                                        {
                                            strPageURL = "";
                                        }
                                        //strPageURL = strPageURL.Replace("directory", strSpecificURL)
                                        strPageURL = strPageURL.Replace("FORM_PK", Convert.ToString(lngPkValue));
                                        //strPageURL = strPageURL.Replace("Biztype", CStr(Biz_Type))
                                        if (ApprovalFlg == 0)
                                        {
                                            strPageURL = "../01Setup/frmRestrictionApproval.aspx?frmForm=MSG&FORM_TYPE=" + FormType + "&REF_PK=" + lngPkValue + "&REF_NR=" + ReferenceNr;
                                        }
                                        else
                                        {
                                            strPageURL = strURL;
                                        }
                                        if (string.IsNullOrEmpty(strPageURL))
                                        {
                                            _with36["URL_PAGE"] = DBNull.Value;
                                            _with36["ATTACHMENT_DATA"] = DBNull.Value;
                                        }
                                        else
                                        {
                                            _with36["URL_PAGE"] = strPageURL;
                                            _with36["ATTACHMENT_DATA"]= strPageURL;
                                        }
                                    }

                                    if ((SaveUnApprovedTDR(dsMsg, lngCreatedBy) == -1))
                                    {
                                        return "<br>Due to some problem Mail has not been sent</br>";
                                        return functionReturnValue;
                                    }
                                }
                            }
                        }
                        return "<br>All data saved Successfully and Restriction Request Mail has been sent</br>";
                    }
                    else
                    {
                        return "<br>Mail has not been sent.Document Id not created or Workflow is Inactive.</br>";
                    }
                }
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return functionReturnValue;
        }
        #endregion


        #region "SaveAffiliates "
        public ArrayList SaveAffiliates(string ReferencePk, string CustomerPk, string ReferenceType)
        {

            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = default(OracleTransaction);
            int RetVal = 0;
            StringBuilder strSqlBuilder = new StringBuilder();
            try
            {
                if (ReferencePk != "0")
                {
                    strSqlBuilder.Append(" DELETE FROM AFFILIATE_CUSTOMER_DTLS A ");
                    strSqlBuilder.Append(" WHERE A.REFERENCE_MST_FK = " + ReferencePk + "");
                    strSqlBuilder.Append(" AND A.REFERENCE_TYPE = " + ReferenceType + "");
                    objWK.ExecuteCommands(strSqlBuilder.ToString());
                }
                objWK.OpenConnection();
                TRAN = objWK.MyConnection.BeginTransaction();
                objWK.MyCommand.Transaction = TRAN;
                objWK.MyCommand.Parameters.Clear();
                var _with37 = objWK.MyCommand;
                _with37.CommandType = CommandType.StoredProcedure;
                _with37.CommandText = objWK.MyUserName + ".CUSTOMER_MST_TBL_PKG.AFFILIATES_DTLS_INS";
                _with37.Parameters.Add("REFERENCE_MST_FK_IN", ReferencePk).Direction = ParameterDirection.Input;
                _with37.Parameters.Add("CUST_MST_FK_IN", CustomerPk).Direction = ParameterDirection.Input;
                _with37.Parameters.Add("REFERENCE_TYPE_IN", ReferenceType).Direction = ParameterDirection.Input;
                _with37.Parameters.Add("CREATED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                _with37.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                RetVal = _with37.ExecuteNonQuery();
                TRAN.Commit();
                arrMessage.Add("All data saved successfully");
                return arrMessage;
            }
            catch (OracleException OraExp)
            {
                TRAN.Rollback();
                arrMessage.Add(OraExp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                TRAN.Rollback();
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
            finally
            {
                objWK.MyConnection.Close();
            }
        }
        public string AffiliateCustomers(long lngPkValue = 0, string REFERENCE_TYPE = "1")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            DataSet DS = null;
            string ReturnVal = null;
            sb.Append("SELECT ROWTOCOL('SELECT A.CUST_MST_FK FROM AFFILIATE_CUSTOMER_DTLS A ");
            sb.Append(" WHERE A.REFERENCE_MST_FK=" + lngPkValue);
            sb.Append(" AND A.REFERENCE_TYPE=" + REFERENCE_TYPE);
            sb.Append(" ') FROM DUAL");
            try
            {
                DS = objWF.GetDataSet(sb.ToString());
                if (DS.Tables[0].Rows.Count > 0)
                {
                    ReturnVal = Convert.ToString((string.IsNullOrEmpty(DS.Tables[0].Rows[0][0].ToString()) ? "" : DS.Tables[0].Rows[0][0]));
                }
                return ReturnVal;
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion

        #region "Send External Mail "
        public string SendExternalMessage(long lngCreatedBy, long lngPkValue, string strDocId, long lngLocFk, System.DateTime salecaldate, long SenderFk = 0, string strSpecificURL = "",  long strpk = 0, long Biz_Type = 0, string TypeFlag = "",
        string strReqDocId = "", int ApprovalFlg = 0, int ReqByUserFk = 0)
        {
            DataSet dsMsg = new DataSet();
            cls_ExternalMail Mail_obj = new cls_ExternalMail();
            DataSet dsDoc = null;
            string NewstrPageURL = null;
            DataSet dsUser = new DataSet();
            DataSet dsSender = new DataSet();
            string strUsrName = null;
            string strLocName = null;
            string strMsgSub = null;
            string strMsgHeader = null;
            string strMsgFooter = null;
            string strMsgBody = null;
            string strFooter = null;
            DataRow DR = null;
            DataSet dsUserFks = null;
            long lngdocPk = 0;
            string strSenderLocName = null;
            string strSenderUsrName = null;
            string MailTo = null;
            string strPageURL = null;
            long lngUserFk = 0;
            int intSenderCnt = 0;
            int Cargo_Type = 0;
            WorkFlow objWF = new WorkFlow();

            DocRefNr = "";
            try
            {
                dsDoc = FetchDocument(strDocId);
                if (dsDoc.Tables[0].Rows.Count > 0)
                {
                    lngdocPk = Convert.ToInt64(dsDoc.Tables[0].Rows[0]["DOCUMENT_MST_PK"]);
                    dsMsg = GetMessageInfo(lngdocPk, -1);
                    if (dsMsg.Tables[0].Rows.Count < 1)
                    {
                        DR = dsMsg.Tables[0].NewRow();
                        dsMsg.Tables[0].Rows.Add(DR);
                    }
                    if (ApprovalFlg == 1)
                    {
                        dsDoc = FetchDocument(strReqDocId);
                        if (dsDoc.Tables[0].Rows.Count > 0)
                        {
                            lngdocPk = Convert.ToInt64(dsDoc.Tables[0].Rows[0]["DOCUMENT_MST_PK"]);
                        }
                        dsUserFks = GetApproverInfo(lngdocPk, ReqByUserFk);
                    }
                    else
                    {
                        dsUserFks = GetUserInfo(lngdocPk, lngLocFk, SenderFk, ReqByUserFk);
                    }
                    if (dsUserFks.Tables[0].Rows.Count == 0 & TaskDocCreatedBy > 0)
                    {
                        dsUserFks = GetUserInfo(lngdocPk, lngLocFk, SenderFk, TaskDocCreatedBy);
                    }
                    if (dsUserFks.Tables[0].Rows.Count > 0)
                    {
                        for (intSenderCnt = 0; intSenderCnt <= dsUserFks.Tables[0].Columns.Count - 1; intSenderCnt++)
                        {
                            if (!(string.IsNullOrEmpty(dsUserFks.Tables[0].Rows[0][intSenderCnt].ToString())))
                            {
                                if ((Convert.ToInt32(dsUserFks.Tables[0].Rows[0][intSenderCnt])) > 0)
                                {
                                    dsUser = FetchUserDetails(Convert.ToInt64(dsUserFks.Tables[0].Rows[0][intSenderCnt]));
                                    strUsrName = Convert.ToString(dsUser.Tables[0].Rows[0]["USER_NAME"]);
                                    strLocName = Convert.ToString(dsUser.Tables[0].Rows[0]["LOCATION_NAME"]);
                                    if (dsMsg.Tables[0].Rows.Count > 0)
                                    {
                                        var _with38 = dsMsg.Tables[0].Rows[0];
                                        _with38["Sender_Fk"] = lngCreatedBy;
                                        dsSender = FetchUserEmail(lngCreatedBy);
                                        strSenderUsrName = Convert.ToString(dsSender.Tables[0].Rows[0]["USER_NAME"]);
                                        strSenderLocName = Convert.ToString(dsSender.Tables[0].Rows[0]["LOCATION_NAME"]);
                                        MailTo = Convert.ToString(dsSender.Tables[0].Rows[0]["EMAIL_Id"]);
                                        strMsgBody = Convert.ToString(_with38["ExternalMsgBody"]);
                                        strMsgBody = strMsgBody.Replace("<<", "<");
                                        strMsgBody = strMsgBody.Replace("&lt;&lt;", "<");
                                        strMsgBody = strMsgBody.Replace(">>", ">");
                                        strMsgBody = strMsgBody.Replace("&gt;&gt;", ">");
                                        strMsgBody = strMsgBody.Replace("<SENDER>", (strSenderUsrName + ", " + strSenderLocName));
                                        strMsgBody = strMsgBody.Replace("<RECEIVER>", strUsrName);
                                        strMsgBody = strMsgBody.Replace("<CURRENT DATE>", Convert.ToString(DateTime.Now.Date));
                                        strMsgBody = GetMessageBody(lngPkValue, strDocId, strMsgBody, salecaldate, strpk, TypeFlag);
                                        //Added by Faheem
                                        strMsgSub = Convert.ToString(_with38["MSG_SUBJECT"]);
                                        strMsgSub = GetMessageBody(lngPkValue, strDocId, strMsgSub, salecaldate, strpk, TypeFlag);
                                        //Added by Faheem
                                        strFooter = Convert.ToString(_with38["ExternalMsgFooter"]);
                                        strFooter = strFooter.Replace("<<", "<");
                                        strFooter = strFooter.Replace("&lt;&lt;", "<");
                                        strFooter = strFooter.Replace(">>", ">");
                                        strFooter = strFooter.Replace("&gt;&gt;", ">");
                                        strFooter = strFooter.Replace("<SENDER>", (strSenderUsrName + ", " + strSenderLocName));
                                        strFooter = strFooter.Replace("<RECEIVER>", strUsrName);
                                        strFooter = strFooter.Replace("<CURRENT DATE>", Convert.ToString(DateTime.Now.Date));
                                        strFooter = GetMessageBody(lngPkValue, strDocId, strFooter, salecaldate, strpk, TypeFlag);
                                        //Added by Faheem
                                    }
                                    else
                                    {
                                        return "Please define WorkFlow for the particular document";
                                    }
                                    string path = HttpContext.Current.Request.Url.AbsolutePath;
                                    string host = HttpContext.Current.Request.Url.Host;
                                    if (path.Length > 2)
                                    {
                                        path = path.Substring(1, string.Compare(path, "/"));
                                    }
                                    NewstrPageURL = "http://" + host + path + "frmlogin.aspx";
                                    strPageURL = NewstrPageURL + GetURLInformation(strDocId, lngPkValue);
                                    strPageURL = "For Approval <a href=\"" + strPageURL + "\">Please Click Here For approval</a>";
                                    Mail_obj.M_MAIL_SUBJECT = strMsgSub;
                                    Mail_obj.M_MAIL_BODY = strMsgBody + Environment.NewLine + strPageURL + strFooter;
                                    Mail_obj.Create_by = Convert.ToString(HttpContext.Current.Session["USER_PK"]);
                                    Mail_obj.M_MAIL_TO = MailTo;
                                    Mail_obj.M_MAIL_ATTACHMENTS_COUNT = 0;
                                    Mail_obj.DocNr = DocRefNr;
                                    Mail_obj.DocTypePK = Convert.ToInt32(lngdocPk);
                                    Mail_obj.fn_send_ExternalMail();
                                }
                            }
                        }
                        return "<br>All data saved Successfully and Mail has been sent</br>";
                    }
                    else
                    {
                        return "<br>Mail has not been sent.Document Id not created or Workflow is Inactive.</br>";
                    }
                }
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return "";
        }
        #endregion
        #region " Fetch UserName & Location Details"
        public DataSet FetchUserEmail(long lngUserMessagePk)
        {
            StringBuilder strSqlBuilder = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSqlBuilder.Append(" SELECT UMT.USER_NAME, LMT.LOCATION_NAME,EMT.EMAIL_Id");
                strSqlBuilder.Append(" FROM USER_MST_TBL UMT, Location_Mst_Tbl LMT,EMPLOYEE_MST_TBL EMT");
                strSqlBuilder.Append(" WHERE UMT.USER_MST_PK=" + lngUserMessagePk + "");
                strSqlBuilder.Append(" and LMT.LOCATION_MST_PK = UMT.DEFAULT_LOCATION_FK AND UMT.EMPLOYEE_MST_FK=EMT.EMPLOYEE_MST_PK");


                return objWF.GetDataSet(strSqlBuilder.ToString());
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion

        #region " Get URL Information "
        public string GetURLInformation(string DocID, long HeaderPK)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder strSqlBuilder = new StringBuilder();
            DataSet ds = null;
            DataSet ds1 = null;
            string strVal = null;


            try
            {
                objWF.MyCommand.Parameters.Clear();

                var _with39 = objWF.MyCommand.Parameters;
                _with39.Add("DOCID_IN", DocID).Direction = ParameterDirection.Input;
                _with39.Add("HEADER_PK_IN", HeaderPK).Direction = ParameterDirection.Input;
                _with39.Add("LOAD_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                ds = objWF.GetDataSet("FETCH_LOAD_CONFIRM_PKG", "FETCH_FOR_EXTERNAL_MAIL");

                if (DocID == "SRR Request Sea")
                {
                    objWF.MyCommand.Parameters.Clear();
                    strSqlBuilder.Append("select srs.CARGO_TYPE,srs.status,srs.restricted from");
                    strSqlBuilder.Append("   Srr_Sea_Tbl srs");
                    strSqlBuilder.Append(" WHERE Srs.Srr_Sea_Pk = " + HeaderPK);
                    ds1 = objWF.GetDataSet(strSqlBuilder.ToString());
                    if (ds1.Tables[0].Rows.Count >= 1)
                    {
                        strVal = "?MasterForm=SRRSEA&frompage=Approval&SRRPk=" + HeaderPK + "&IsLCL=" + ds1.Tables[0].Rows[0]["CARGO_TYPE"] + "&status=" + ds1.Tables[0].Rows[0]["status"] + "&Restricted=" + ds1.Tables[0].Rows[0]["restricted"];
                    }
                }
                else if (DocID == "SRR Request Air")
                {
                    objWF.MyCommand.Parameters.Clear();
                    strSqlBuilder.Append("select sra.srr_approved,sra.restricted from");
                    strSqlBuilder.Append("    srr_air_tbl sra");
                    strSqlBuilder.Append("  WHERE sra.Srr_Air_Pk = " + HeaderPK);
                    ds1 = objWF.GetDataSet(strSqlBuilder.ToString());
                    if (ds1.Tables[0].Rows.Count >= 1)
                    {
                        strVal = "?MasterForm=SRRAIR&frompage=Approval&status=0&SRRPk=" + HeaderPK + "&Restricted=" + ds1.Tables[0].Rows[0]["restricted"];
                    }
                }
                else if (DocID == "Invoice Approval" | DocID == "Invoice Request")
                {
                    objWF.MyCommand.Parameters.Clear();

                    strSqlBuilder.Append("select cit.consol_invoice_pk,cit.business_type,cit.process_type,cit.customer_mst_fk,cit.chk_invoice,'1' Approval");
                    strSqlBuilder.Append("   from consol_invoice_tbl cit");
                    strSqlBuilder.Append("  where cit.consol_invoice_pk=" + HeaderPK);
                    ds1 = objWF.GetDataSet(strSqlBuilder.ToString());
                    if (ds1.Tables[0].Rows.Count >= 1)
                    {
                        strVal = "?MasterForm=INVOICE&nInvPk=" + HeaderPK + "&BizType=" + ds1.Tables[0].Rows[0]["business_type"] + "&Process=" + ds1.Tables[0].Rows[0]["process_type"] + "&custpk=" + ds1.Tables[0].Rows[0]["customer_mst_fk"] + "&status=" + ds1.Tables[0].Rows[0]["chk_invoice"] + "&Approval=" + ds1.Tables[0].Rows[0]["Approval"];
                    }
                }
                else if (DocID == "SL Tariff Approval" | DocID == "SL Tariff Request")
                {
                    objWF.MyCommand.Parameters.Clear();
                    strSqlBuilder.Append("select tmst.tariff_main_sea_pk,tmst.active,tmst.cargo_type,tmst.tariff_type  from");
                    strSqlBuilder.Append("   tariff_main_sea_tbl tmst");
                    strSqlBuilder.Append("  where tmst.tariff_main_sea_pk=" + HeaderPK);
                    ds1 = objWF.GetDataSet(strSqlBuilder.ToString());
                    if (ds1.Tables[0].Rows.Count >= 1)
                    {
                        strVal = "?MasterForm=SLTARIFF&BasePage=Approval&TariffPk=" + HeaderPK + "&Active=" + ds1.Tables[0].Rows[0]["active"] + "&IsLCL=" + ds1.Tables[0].Rows[0]["cargo_type"] + "&TrfType=" + ds1.Tables[0].Rows[0]["tariff_type"];
                    }
                }
                else if (DocID == "AIR Tariff Request")
                {
                    strVal = "?MasterForm=AIRTARIFF&BasePage=Approval&acess=15&ATEPk=" + HeaderPK;
                }
                else if (DocID == "Quotation Sea Request" | DocID == "Quotation Air Request")
                {
                    if (ds.Tables[0].Rows.Count >= 1)
                    {
                        strVal = "?MasterForm=QUOSEA&frompage=approval&pk=" + HeaderPK + "&Status=" + ds.Tables[0].Rows[0]["STATUS"] + "&CargoType=" + ds.Tables[0].Rows[0]["CARGO_TYPE"] + "&ProcessType=" + ds.Tables[0].Rows[0]["PROCESS_TYPE"] + "&BizType=" + ds.Tables[0].Rows[0]["BIZ_TYPE"] + "&Restricted=" + ds.Tables[0].Rows[0]["RESTRICTED"];
                    }
                }
                else if (DocID == "Quotation Import Sea Request" | DocID == "Quotation Import Air Request")
                {
                    if (ds.Tables[0].Rows.Count >= 1)
                    {
                        strVal = "?MasterForm=IMPQUOSEA&frompage=importapproval&pk=" + HeaderPK + "&Status=" + ds.Tables[0].Rows[0]["STATUS"] + "&CargoType=" + ds.Tables[0].Rows[0]["CARGO_TYPE"] + "&ProcessType=" + ds.Tables[0].Rows[0]["PROCESS_TYPE"] + "&BizType=" + ds.Tables[0].Rows[0]["BIZ_TYPE"] + "&Restricted=" + ds.Tables[0].Rows[0]["RESTRICTED"];
                    }
                }
                else if (DocID == "SL Spot Rate Request")
                {
                    if (ds.Tables[0].Rows.Count >= 1)
                    {
                        strVal = "?MasterForm=RFQSLSEA&frompage=approval&pkvalue=" + HeaderPK + "&Restricted=" + ds.Tables[0].Rows[0]["restricted"] + "&atv=" + ds.Tables[0].Rows[0]["active"] + "&approve=" + ds.Tables[0].Rows[0]["approved"];
                    }
                }
                else if (DocID == "AIR Spot Rate Request")
                {
                    if (ds.Tables[0].Rows.Count >= 1)
                    {
                        strVal = "?MasterForm=RFQSLAIR&frompage=approval&pkvalue=" + HeaderPK + "&Restricted=" + ds.Tables[0].Rows[0]["restricted"] + "&atv=" + ds.Tables[0].Rows[0]["active"] + "&approve=" + ds.Tables[0].Rows[0]["approved"];
                    }
                }
                else if (DocID == "Voucher Request")
                {
                    if (ds.Tables[0].Rows.Count >= 1)
                    {
                        strVal = "?MasterForm=VOUCHER&Invoice=UnApproved&lblInvSupplierPK=" + HeaderPK + "&Pay_Status=" + ds.Tables[0].Rows[0]["APPROVED"] + "&Business_Type=" + ds.Tables[0].Rows[0]["BUSINESS_TYPE"] + "&Job_Type=" + ds.Tables[0].Rows[0]["JOB_TYPE"] + "& Process_type" + ds.Tables[0].Rows[0]["PROCESS_TYPE"] + "& nAccessRights=15";
                    }
                }
                else if (DocID == "Transport Contract Request")
                {
                    if (ds.Tables[0].Rows.Count >= 1)
                    {
                        strVal = "?MasterForm=TRANSCONT&frompage=approval&ContPk=" + HeaderPK + "&Editflag=" + ds.Tables[0].Rows[0]["ACTIVE"] + "&Btype=" + ds.Tables[0].Rows[0]["TRANSPORT_MODE"];
                    }

                }
                else if (DocID == "SL Contract Request")
                {
                    if (ds.Tables[0].Rows.Count >= 1)
                    {
                        strVal = "?MasterForm=SLCONTRACT&ContractPk=" + HeaderPK + "&Approved=" + ds.Tables[0].Rows[0]["CONT_APPROVED"] + "&IsLCL=" + ds.Tables[0].Rows[0]["CARGO_TYPE"] + "&Restricted=" + ds.Tables[0].Rows[0]["RESTRICTED"] + "&ACTIVE=" + ds.Tables[0].Rows[0]["ACTIVE"];
                    }
                }
                else if (DocID == "Airline Contract Request")
                {
                    strVal = "?MasterForm=AIRCONTRACT&BasePage=approval&Frompage=frmAirContractListingApproval.aspx&ContPk=" + HeaderPK;
                }
                else if (DocID == "Warehouse Contract Request")
                {
                    strVal = "?MasterForm=WHCONTRACT&frompage=approval&PK=" + HeaderPK + "&CAN=" + ds.Tables[0].Rows[0]["contract_no"] + "&BUSINESS_TYPE=" + ds.Tables[0].Rows[0]["BUSINESS_TYPE"] + "&CARGO_TYPE=" + ds.Tables[0].Rows[0]["CARGO_TYPE"];
                }
                else if (DocID == "Payment Request")
                {
                    strVal = "?MasterForm=PAYMENT&Base=approval&PK=" + HeaderPK;
                }
                else if (DocID == "Customer Contract Air Request")
                {
                    strVal = "?MasterForm=CUSTOMERCONTRACT&frompage=approval&acess=15&Restricted=0&ContractPK=" + HeaderPK;
                }
                else if (DocID == "Customer Contract Sea Request")
                {
                    strVal = "?MasterForm=CUSTOMERCONTRACTSEA&frompage=approved&ContractPk=" + HeaderPK + "&CustPK=" + ds.Tables[0].Rows[0]["customer_mst_fk"] + "&IsLCL=" + ds.Tables[0].Rows[0]["cargo_type"];

                }
                else if (DocID == "Transport Contract Air Request")
                {
                    strVal = "?MasterForm=TRNSPORTCONTAIR&fromPage=approval&EditFlag=1&Btype=1&ContPk=" + HeaderPK + "&VAL=" + ds.Tables[0].Rows[0]["transporter_mst_fk"];

                }


                return strVal;
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion
        #region "SendMessageNew "
        public string SendRestrictionMsgExternal(long lngCreatedBy, long lngPkValue, string strDocId, long lngLocFk, string CustName, string ReferenceType, string ReferenceNr, string TranDate, string RestrictType, string RestrictMsg,
        string Status, long SenderFk, Int16 FormType, string strReqDocId = "", int ApprovalFlg = 0, int ReqByUserFk = 0, bool OperatorRestriction = false, string strURL = "")
        {
            string functionReturnValue = null;
            DataSet dsMsg = new DataSet();
            DataSet dsDoc = null;
            cls_ExternalMail Mail_obj = new cls_ExternalMail();
            DataSet dsUser = new DataSet();
            DataSet dsSender = new DataSet();
            string strUsrName = null;
            string strLocName = null;
            string strMsgSub = null;
            string strMsgHeader = null;
            string strMsgFooter = null;
            string strMsgBody = null;
            DataRow DR = null;
            string NewstrPageURL = null;
            DataSet dsUserFks = null;
            long lngdocPk = 0;
            string strSenderLocName = null;
            string strSenderUsrName = null;
            string strPageURL = null;
            long lngUserFk = 0;
            int intSenderCnt = 0;
            int Cargo_Type = 0;
            string strPageURLforexternal = null;
            WorkFlow objWF = new WorkFlow();
            DocRefNr = "";
            try
            {
                dsDoc = FetchDocument(strDocId);
                if (dsDoc.Tables[0].Rows.Count > 0)
                {
                    lngdocPk = Convert.ToInt64(dsDoc.Tables[0].Rows[0]["DOCUMENT_MST_PK"]);
                    dsMsg = GetMessageInfo(lngdocPk, -1);
                    if (dsMsg.Tables[0].Rows.Count < 1)
                    {
                        DR = dsMsg.Tables[0].NewRow();
                        dsMsg.Tables[0].Rows.Add(DR);
                    }
                    if (ApprovalFlg == 1)
                    {
                        dsDoc = FetchDocument(strReqDocId);
                        if (dsDoc.Tables[0].Rows.Count > 0)
                        {
                            lngdocPk = Convert.ToInt64(dsDoc.Tables[0].Rows[0]["DOCUMENT_MST_PK"]);
                        }
                        dsUserFks = GetApproverInfo(lngdocPk, ReqByUserFk);
                    }
                    else
                    {
                        dsUserFks = GetUserInfo(lngdocPk, lngLocFk, SenderFk, ReqByUserFk);
                    }
                    if (dsUserFks.Tables[0].Rows.Count > 0)
                    {
                        for (intSenderCnt = 0; intSenderCnt <= dsUserFks.Tables[0].Columns.Count - 1; intSenderCnt++)
                        {
                            if (!(string.IsNullOrEmpty(dsUserFks.Tables[0].Rows[0][intSenderCnt].ToString())))
                            {
                                if ((Convert.ToInt32(dsUserFks.Tables[0].Rows[0][intSenderCnt])) > 0)
                                {
                                    dsUser = FetchUserDetails(Convert.ToInt64(dsUserFks.Tables[0].Rows[0][intSenderCnt]));
                                    strUsrName = Convert.ToString(dsUser.Tables[0].Rows[0]["USER_NAME"]);
                                    strLocName = Convert.ToString(dsUser.Tables[0].Rows[0]["LOCATION_NAME"]);
                                    if (dsMsg.Tables[0].Rows.Count > 0)
                                    {
                                        var _with40 = dsMsg.Tables[0].Rows[0];
                                        _with40["Sender_Fk"] = lngCreatedBy;
                                        dsSender = FetchUserDetails(lngCreatedBy);
                                        strSenderUsrName = Convert.ToString(dsSender.Tables[0].Rows[0]["USER_NAME"]);
                                        strSenderLocName = Convert.ToString(dsSender.Tables[0].Rows[0]["LOCATION_NAME"]);

                                        _with40["Receiver_Fk"] = Convert.ToInt64(dsUserFks.Tables[0].Rows[0][intSenderCnt]);
                                        _with40["Msg_Read"] = 0;
                                        _with40["Followup_Flag"] = 0;
                                        _with40["Have_Attachment"] = 1;

                                        strMsgBody = Convert.ToString(_with40["Msg_Body"]);
                                        strMsgBody = strMsgBody.Replace("<<", "<");
                                        strMsgBody = strMsgBody.Replace("&lt;&lt;", "<");
                                        strMsgBody = strMsgBody.Replace(">>", ">");
                                        strMsgBody = strMsgBody.Replace("&gt;&gt;", ">");
                                        strMsgBody = strMsgBody.Replace("<SENDER>", (strSenderUsrName + ", " + strSenderLocName));
                                        strMsgBody = strMsgBody.Replace("<RECEIVER>", strUsrName);
                                        strMsgBody = strMsgBody.Replace("<CURRENT DATE>", Convert.ToString(DateTime.Now.Date));
                                        strMsgBody = strMsgBody.Replace("<CUSTOMER NAME>", CustName);
                                        strMsgBody = strMsgBody.Replace("<REFERENCE TYPE>", ReferenceType);
                                        strMsgBody = strMsgBody.Replace("<REFERENCE NR>", ReferenceNr);
                                        strMsgBody = strMsgBody.Replace("<DATE>", TranDate);
                                        strMsgBody = strMsgBody.Replace("<RESTRICTION TYPE>", RestrictType);
                                        strMsgBody = strMsgBody.Replace("<RESTRICTION MSG>", RestrictMsg);
                                        strMsgBody = strMsgBody.Replace("<STATUS>", Status);
                                        if (OperatorRestriction == true)
                                        {
                                            strMsgBody = strMsgBody.Replace("Customer Name", "Line");
                                        }
                                        _with40["Msg_Body"] = strMsgBody;

                                        strMsgSub = Convert.ToString(_with40["MSG_SUBJECT"]);
                                        strMsgSub = strMsgSub.Replace("<<", "<");
                                        strMsgSub = strMsgSub.Replace("&lt;&lt;", "<");
                                        strMsgSub = strMsgSub.Replace(">>", ">");
                                        strMsgSub = strMsgSub.Replace("&gt;&gt;", ">");
                                        strMsgSub = strMsgSub.Replace("<SENDER>", (strSenderUsrName + ", " + strSenderLocName));
                                        strMsgSub = strMsgSub.Replace("<RECEIVER>", strUsrName);
                                        strMsgSub = strMsgSub.Replace("<CURRENT DATE>", Convert.ToString(DateTime.Now.Date));
                                        strMsgSub = strMsgSub.Replace("<CUSTOMER NAME>", CustName);
                                        strMsgSub = strMsgSub.Replace("<REFERENCE TYPE>", ReferenceType);
                                        strMsgSub = strMsgSub.Replace("<REFERENCE NR>", ReferenceNr);
                                        strMsgSub = strMsgSub.Replace("<DATE>", TranDate);
                                        strMsgSub = strMsgSub.Replace("<RESTRICTION TYPE>", RestrictType);
                                        strMsgSub = strMsgSub.Replace("<RESTRICTION MSG>", RestrictMsg);
                                        strMsgSub = strMsgSub.Replace("<STATUS>", Status);
                                        _with40["Msg_Subject"]= strMsgSub;

                                        _with40["Read_Receipt"] = 0;
                                        _with40["Document_Mst_Fk"] = lngdocPk;
                                        _with40["User_Message_Folders_Fk"] = 1;
                                        _with40["Msg_Received_Dt"] = DateTime.Now;
                                    }
                                    else
                                    {
                                        return "Please define WorkFlow for the particular document";
                                    }
                                    if (dsMsg.Tables[1].Rows.Count < 1)
                                    {
                                        DR = dsMsg.Tables[1].NewRow();
                                        dsMsg.Tables[1].Rows.Add(DR);
                                    }
                                    if (dsMsg.Tables[1].Rows.Count > 0)
                                    {
                                        var _with41 = dsMsg.Tables[1].Rows[0];
                                        if (!string.IsNullOrEmpty(_with41["URL_PAGE"].ToString()))
                                        {
                                            strPageURL = Convert.ToString(_with41["URL_PAGE"]);
                                        }
                                        else
                                        {
                                            strPageURL = "";
                                        }
                                        //strPageURL = strPageURL.Replace("directory", strSpecificURL)
                                        strPageURL = strPageURL.Replace("FORM_PK", Convert.ToString(lngPkValue));
                                        //strPageURL = strPageURL.Replace("Biztype", CStr(Biz_Type))
                                        if (ApprovalFlg == 0)
                                        {
                                            string path = HttpContext.Current.Request.Url.AbsolutePath;
                                            string host = HttpContext.Current.Request.Url.Host;
                                            if (path.Length > 2)
                                            {
                                                path = path.Substring(1, string.Compare(path, "/"));
                                            }
                                            dsSender = FetchUserEmail(lngCreatedBy);
                                            strPageURL = "?MasterForm=RESTRICATION&frmForm=MSG&FORM_TYPE=" + FormType + "&REF_PK=" + lngPkValue + "&REF_NR=" + ReferenceNr;
                                            NewstrPageURL = "http://" + host + path + "frmlogin.aspx" + strPageURL;
                                            strPageURLforexternal = "For Approval <a href=\"" + NewstrPageURL + "\">Please Click Here For approval</a>";
                                            Mail_obj.M_MAIL_SUBJECT = strMsgSub;
                                            Mail_obj.M_MAIL_BODY = strMsgBody + Environment.NewLine + strPageURLforexternal;
                                            Mail_obj.Create_by = Convert.ToString(HttpContext.Current.Session["USER_PK"]);
                                            Mail_obj.M_MAIL_TO = Convert.ToString(dsSender.Tables[0].Rows[0]["EMAIL_Id"]);
                                            Mail_obj.M_MAIL_ATTACHMENTS_COUNT = 0;
                                            Mail_obj.fn_send_ExternalMail();
                                        }
                                        else
                                        {
                                            strPageURL = strURL;
                                        }
                                        if (string.IsNullOrEmpty(strPageURL))
                                        {
                                            _with41["URL_PAGE"] = DBNull.Value;
                                            _with41["ATTACHMENT_DATA"] = DBNull.Value;
                                        }
                                        else
                                        {
                                            _with41["URL_PAGE"] = strPageURL;
                                            _with41["ATTACHMENT_DATA"] = strPageURL;
                                        }

                                    }

                                    if ((SaveUnApprovedTDR(dsMsg, lngCreatedBy) == -1))
                                    {
                                        return "<br>Due to some problem Mail has not been sent</br>";
                                        return functionReturnValue;
                                    }
                                }
                            }
                        }
                        return "<br>All data saved Successfully and Restriction Request Mail has been sent</br>";
                    }
                    else
                    {
                        return "<br>Mail has not been sent.Document Id not created or Workflow is Inactive.</br>";
                    }
                }
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return functionReturnValue;
        }
        #endregion

        #region "Noti Customer"
        public DataSet FetchNotifyCust(long lngPkValue)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();

            sb.Append(" SELECT B.CUSTOMER_MST_FK CUST_PK,B.CUST_CUSTOMER_MST_FK SHIP_PK, B.CONS_CUSTOMER_MST_FK CONSIGN_PK,B.NOTIFY1_CUST_MST_FK NOTIFY_PK, 0 AGENT_PK ");
            sb.Append(" FROM BOOKING_MST_TBL B, VESSEL_VOYAGE_TBL VV, VESSEL_VOYAGE_TRN VVT  WHERE B.STATUS = 2 ");
            sb.Append(" AND VV.VESSEL_VOYAGE_TBL_PK = VVT.VESSEL_VOYAGE_TBL_FK AND VVT.VOYAGE_TRN_PK = B.VESSEL_VOYAGE_FK ");
            sb.Append(" AND B.VESSEL_VOYAGE_FK =" + lngPkValue);
            sb.Append(" UNION ");
            sb.Append(" SELECT J.CUST_CUSTOMER_MST_FK CUST_PK,J.SHIPPER_CUST_MST_FK SHIP_PK, J.CONSIGNEE_CUST_MST_FK CONSIGN_PK,J.NOTIFY1_CUST_MST_FK NOTIFY_PK, 0 AGENT_PK ");
            sb.Append(" FROM JOB_CARD_TRN J, VESSEL_VOYAGE_TBL VV, VESSEL_VOYAGE_TRN VVT  WHERE 1 = 1 ");
            sb.Append(" AND VV.VESSEL_VOYAGE_TBL_PK = VVT.VESSEL_VOYAGE_TBL_FK AND VVT.VOYAGE_TRN_PK = J.VOYAGE_TRN_FK ");
            sb.Append("  AND J.PROCESS_TYPE = 2 AND J.VOYAGE_TRN_FK =" + lngPkValue);
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion

        #region " Notiy Customer Email"
        public DataSet FetchNotifyCustEmail(long lngPkValue)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append(" SELECT CCT.EMAIL ");
            sb.Append(" FROM CUSTOMER_MST_TBL CMT, CUSTOMER_CONTACT_TRN CCT ");
            sb.Append(" WHERE CMT.CUSTOMER_MST_PK = CCT.CUSTOMER_MST_FK ");
            sb.Append(" AND CMT.CUSTOMER_MST_PK  =" + lngPkValue);
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion



    }
}