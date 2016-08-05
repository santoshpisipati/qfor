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
using Quantum_QFOR.Models;
using System;
using System.Collections;
using System.Configuration;
using System.Data;
using System.Net.Http;
using System.Text;
using System.Web;
using System.Web.Caching;
using System.Web.Http;
using System.Web.Mail;
using Microsoft.VisualBasic;

namespace Quantum_QFOR.Controllers
{
    //[Authorize]
    [RoutePrefix("api/Account")]
    public class AccountController : ApiController
    {
        private cls_Corporate_Mst_Tbl objCorp = new cls_Corporate_Mst_Tbl();
        private cls_ValidateLicense objLicenseCmn = new cls_ValidateLicense();
        private clsUser_Mst_Tbl objUser = new clsUser_Mst_Tbl();
        private CLS_UserAccountVariable userDetail = new CLS_UserAccountVariable();

        private bool IsUserIdExist(string userName)
        {
            //IDictionaryEnumerator CacheEnum = Cache.GetEnumerator();
            //string[] Key = null;
            //string LoggedInUser = null;
            //while (CacheEnum.MoveNext())
            //{
            //    Key = CacheEnum.Key.ToString().Split(',');
            //    LoggedInUser = Key[0];
            //    if (LoggedInUser == userName)
            //    {
            //        return true;
            //    }
            //}
            return false;
        }

        private void RemoveEntryFromCache(string Key, object Val, System.Web.Caching.CacheItemRemovedReason Reason)
        {
            //Session.Clear()
            //Session.Abandon()
        }

        private void CreateEntryInCache(string userName)
        {
            CacheItemRemovedCallback OnRemove = default(CacheItemRemovedCallback);
            OnRemove = new CacheItemRemovedCallback(this.RemoveEntryFromCache);
            //Cache.Insert(userName, Request., null, Cache.NoAbsoluteExpiration, TimeSpan.FromMinutes(HttpContext.Current.Session.Timeout), System.Web.Caching.CacheItemPriority.High, OnRemove);
        }

        #region "Check Password Expiry"

        public bool CheckPasswordExpiry(LoginModel loginModel)
        {
            bool functionReturnValue = false;
            Int32 PassExp = default(Int32);
            PassExp = Convert.ToInt32(loginModel.PASSWORD_EXPIRY_DT);

            Int32 daysAlert = default(Int32);
            daysAlert = Convert.ToInt32(loginModel.EXP_NOTIFICATION_DT);
            if (Convert.ToString(loginModel.EXP_NOTIFICATION_DT) == "0")
            {
                daysAlert = 0;
            }

            //Dim jscript As String

            functionReturnValue = true;
            try
            {
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return functionReturnValue;
        }

        #endregion "Check Password Expiry"

        public DateTime fun_getTimeZoneDateTime()
        {
            try
            {
                DateTime Final_Datetime = default(DateTime);
                Final_Datetime = System.DateTime.Now;
                return Final_Datetime;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #region "Get Mail for internal or external"

        private void GetMailforinterandexter(int LocPK)
        {
            try
            {
                WorkFlow objWF = new WorkFlow();
                Int16 getmailid = default(Int16);

                getmailid = Convert.ToInt16(objWF.ExecuteScaler(" SELECT LMT.MAIL_TYPE FROM LOCATION_MST_TBL LMT WHERE LMT.LOCATION_MST_PK=" + LocPK));
                //Session.Add("GETMAIL_INTERNAL_EXTERNAL", getmailid);
            }
            catch (Exception ex)
            {
            }
        }

        #endregion "Get Mail for internal or external"

        public string Location_image(long Locpk)
        {
            WorkFlow objWF = new WorkFlow();
            string strSql = null;
            string strLogoName = null;
            try
            {
                strSql = "Select LOGO_FILE_PATH from Location_Mst_Tbl lmst where lmst.location_mst_pk =" + Locpk;
                strLogoName = objWF.ExecuteScaler(strSql);
                if (string.IsNullOrEmpty(strLogoName))
                {
                    strLogoName = ConfigurationManager.AppSettings["LOGO"];
                }
                return strLogoName;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        [HttpPost]
        public int IsAdmin(HttpRequestMessage request)
        {
            var jsonString =  request.Content.ReadAsStringAsync();
            string strSQL = null;
            short Admin = 0;
            WorkFlow objWF = new WorkFlow();
            strSQL = "SELECT COUNT(*) FROM User_Mst_Tbl U WHERE U.ROLE_MST_FK = ";
            strSQL = strSQL + "(SELECT R.ROLE_MST_TBL_PK FROM ROLE_MST_TBL R WHERE R.ROLE_ID = 'ADMIN')";
            strSQL = strSQL + "AND U.USER_MST_PK = " + "";
            try
            {
                Admin = Convert.ToInt16(objWF.ExecuteScaler(strSQL.ToString()));
                if (Admin == 1)
                {
                    return 1;
                }
                else
                {
                    return 0;
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
        }
        [HttpGet]
        public int IsAdministrator(string strUserID)
        {
            string strSQL = null;
            short Admin = 0;
            WorkFlow objWF = new WorkFlow();
            strSQL = "SELECT COUNT(*) FROM User_Mst_Tbl U WHERE U.ROLE_MST_FK = ";
            strSQL = strSQL + "(SELECT R.ROLE_MST_TBL_PK FROM ROLE_MST_TBL R WHERE R.ROLE_ID = 'ADMIN')";
            strSQL = strSQL + "AND U.USER_MST_PK = " + strUserID;
            try
            {
                Admin = Convert.ToInt16(objWF.ExecuteScaler(strSQL.ToString()));
                if (Admin == 1)
                {
                    return 1;
                }
                else
                {
                    return 0;
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
        }

        public bool IsActivityLogEnabled = Convert.ToBoolean(ConfigurationManager.AppSettings["ISACTIVITYLOGENABLED"]);

        public Int32 LicenseExpiryDays = Convert.ToInt32(ConfigurationManager.AppSettings["LicenseExpiryDays"]);

        public string GetMailServer
        {
            get { return ConfigurationSettings.AppSettings["MailServer"]; }
        }

        public void LicenseExipryNotification()
        {
            
               DateTime ExpiryDate = default(DateTime);
            int ExipreDays = 0;
            clsMail objMail = new clsMail();
            MailMessage ml = new MailMessage();
            string strMsgHeader = null;
            string strMsgBody = null;
            StringBuilder sbMsgBody = new StringBuilder(5000);
            //****************************** External*********************************
            ml.Fields["http://schemas.microsoft.com/cdo/configuration/smtpserver"] = GetMailServer;
            ml.Fields["http://schemas.microsoft.com/cdo/configuration/smtpserverport"] = 25;
            //465
            ml.Fields["http://schemas.microsoft.com/cdo/configuration/sendusing"] = 2;
            ml.Fields["http://schemas.microsoft.com/cdo/configuration/smtpauthenticate"] = 1;
            ml.Fields["http://schemas.microsoft.com/cdo/configuration/sendusername"] = Convert.ToString(ConfigurationManager.AppSettings["SEND_USERNAME"]);
            ml.Fields["http://schemas.microsoft.com/cdo/configuration/sendpassword"] = Convert.ToString(ConfigurationManager.AppSettings["SEND_PASSWORD"]);

            strMsgHeader = "Password Changed";
            string _tab = "&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;";
            sbMsgBody.AppendLine("Dear Support team" + ",<br /><br />");
            sbMsgBody.AppendLine("The system license information are as follows<br />");
            sbMsgBody.AppendLine("License Expiry Date is : " + ExpiryDate + "<br />");
            sbMsgBody.AppendLine("License will expire in:  " + ExipreDays + "days<br /><br />");
            sbMsgBody.AppendLine("Thanks & Regards,<br />");
            sbMsgBody.AppendLine("Q-FOR<br /><br />");
            sbMsgBody.AppendLine("This is a system generated message, Please do not reply.");
            strMsgBody = sbMsgBody.ToString();

            ml.To = Convert.ToString(ConfigurationManager.AppSettings["SupportEmail"]);
            ml.From = Convert.ToString(ConfigurationManager.AppSettings["SEND_USERNAME"]);
            ml.Subject = "Q-FOR- License Expiry Notification";
            ml.Body = strMsgBody;
            ml.BodyFormat = MailFormat.Html;
            SmtpMail.SmtpServer = GetMailServer;
            SmtpMail.Send(ml);
        }

        [AcceptVerbs("GET","POST")]
        public object Login([FromBody]Credentials userName)
        {
            if (userName == null) return new object();
            string Username = userName.Username;
            string Password = userName.Password;
            Common cmnClass = new Common();
            LoginModel loginModel = new LoginModel();
            string json = string.Empty;
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();
            CLS_STATUS status = new CLS_STATUS();
            try
            {
                int intValidUser = 0;
                string strValidityCheckValue = null;
                string strUsrPk = "";
                string strUsrName = "";
                string strLocPk = "";
                string strDesig = "";
                string strLocName = "";
                string strLocTypeID = "";
                string strCurrencyID = "";
                string strCurrencyName = "";
                string[] strSplitvalue = null;
                string imagepath = null;
                bool isadminusr;
                long LOCPK = 0;
                int Customer = 0;
                ArrayList arrMessage = new ArrayList();
                bool IsMultiLogin = false;
                int WrongPwdCount = 0;

                cls_Employee_Mst_Table objemp = new cls_Employee_Mst_Table();
               
                string hdnRegistrationKeyValue = ""; // was retrieved from a Label on Form
                string lblStatus = ""; // was retrieved from a Label on Form
                long ExpireDays = 0;
                DateTime ExpiryDate = new DateTime();
                CLS_STATUS chkVal = (objLicenseCmn.fn_ValidateUsers(this, hdnRegistrationKeyValue, lblStatus, /*txtUSER_ID.Text*/ Username, ExpireDays, ExpiryDate));
                if (chkVal != null && Convert.ToInt32(chkVal.value) == 0)
                {
                    status.status = false;
                    status.message = "";
                    return JsonConvert.SerializeObject(status, Formatting.Indented);
                }

                if (Convert.ToInt32(0) > 0)
                {
                    if (Convert.ToInt32(objemp.fn_GetStoredUsers(Convert.ToInt32(HttpContext.Current.Session["SESSION_NO_OF_LICENSE_USERS"]))) > 0)
                    {
                        HttpContext.Current.Session.Abandon();
                        HttpContext.Current.Session.Clear();
                        status.message = cmnClass.GetErrorMessage(5004);
                        status.status = false;
                        return cmnClass.ReturnDeserializedJson(status);
                    }
                }

                if ((ConfigurationManager.AppSettings["IsMultiLogin"] != null))
                {
                    IsMultiLogin = Convert.ToBoolean(ConfigurationManager.AppSettings["IsMultiLogin"]);
                }
                else
                {
                    IsMultiLogin = false;
                }

                //WrongPwdCount = objUser.FetchWrongPwd(Username.Trim());
                if (WrongPwdCount >= 5)
                {
                    status.message = "";
                    status.status = false;
                    return cmnClass.ReturnDeserializedJson(status);
                }

                long PCC = 0;
                strValidityCheckValue = objUser.IsUserValid(Username.Trim(), Password, PCC);

                switch (strValidityCheckValue)
                {
                    case "0":
                        status.message = cmnClass.GetErrorMessage(10020,0);
                        status.status = false;
                        return cmnClass.ReturnDeserializedJson(status);

                    case "1":
                        status.message = cmnClass.GetErrorMessage(10021, 0);
                        status.status = false;
                        return cmnClass.ReturnDeserializedJson(status);

                    case "2":
                        status.message = cmnClass.GetErrorMessage(10022, 0);
                        status.status = false;
                        return cmnClass.ReturnDeserializedJson(status);

                    case "3":
                        status.message = cmnClass.GetErrorMessage(10023, 0);
                        status.status = false;
                        return cmnClass.ReturnDeserializedJson(status);

                    case "4":
                        status.message = cmnClass.GetErrorMessage(10026, 0);
                        status.status = false;
                        return cmnClass.ReturnDeserializedJson(status);

                    case "5":
                        status.message = cmnClass.GetErrorMessage(10025, 0);
                        status.status = false;
                        return cmnClass.ReturnDeserializedJson(status);

                    case "7":
                        status.message = "This Location is In-Active.";
                        status.status = false;
                        return cmnClass.ReturnDeserializedJson(status);

                    default:
                        break;
                }

                if (strValidityCheckValue.Length > 1)
                {
                    if (IsMultiLogin == false)
                    {
                        if (IsUserIdExist(Username))
                        {
                            //if (Application("NUM_LOGIN_USERS") > 0)
                            //{
                            //    Application("NUM_LOGIN_USERS") = Application("NUM_LOGIN_USERS") - 1;
                            //}
                            status.message = cmnClass.GetErrorMessage(5005);
                            status.status = false;
                            return cmnClass.ReturnDeserializedJson(status);
                        }
                        CreateEntryInCache(Username.Trim());
                    }
                    strSplitvalue = strValidityCheckValue.Split('|');
                    strUsrName = strSplitvalue[0];
                    strUsrPk = strSplitvalue[1];
                    strLocPk = strSplitvalue[2];
                    strDesig = strSplitvalue[3];
                    strLocName = strSplitvalue[4];
                    LOCPK = Convert.ToInt64(strLocPk);
                    CLS_UserAccountVariable uav = new CLS_UserAccountVariable();
                    strLocTypeID = strSplitvalue[6];
                    Customer = Convert.ToInt32(strSplitvalue.GetValue(7));
                    //CreateCookies("UserID", Username.Trim(), Response, Request);
                    cls_Currency_Type_Mst_Tbl objCurrency = new cls_Currency_Type_Mst_Tbl();
                    loginModel.USER_PK = strUsrPk;
                    loginModel.USER_NAME = strUsrName;
                    loginModel.LOGED_IN_LOC_FK = strLocPk;
                    loginModel.LOGED_IN_LOC_NAME = strLocName;
                    loginModel.LOGED_IN_LOC_TYPE_ID = strLocTypeID;
                    loginModel.DESIGNATION = strDesig;
                    loginModel.BIZ_TYPE = uav.getCurrentBusinessType(strSplitvalue[5].ToString());
                              loginModel.CURRENCY_MST_PK = objCurrency.GetBaseCurrency(Convert.ToInt32(strLocPk), strCurrencyID, strCurrencyName);
                    loginModel.CURRENCY_ID = strCurrencyID;
                    loginModel.CURRENCY_NAME = strCurrencyName;
                            loginModel.Cust_PK = objUser.FetchEmpForUser(Convert.ToInt16(strUsrPk));
                    loginModel.Role_PK = objUser.FetchROLEForUser(Convert.ToInt16(strUsrPk));
                    loginModel.ImageFile = imagepath;
                    loginModel.EMP_PK = objUser.FetchEmpForUser(Convert.ToInt16(strUsrPk));
                    loginModel.Cust_PK = objUser.FetchCustForUser(Convert.ToInt16(strUsrPk));
                    loginModel.Role_PK = objUser.FetchROLEForUser(Convert.ToInt16(strUsrPk));
                    GetMailforinterandexter(Convert.ToInt32(strLocPk));

                    imagepath = Location_image(LOCPK);

                    cls_User_Preference_Mst_Tbl objUserPref = new cls_User_Preference_Mst_Tbl();
                    int IsUserPresent = objUserPref.IsUserAvailable(Convert.ToInt16(strUsrPk));

                    if (IsUserPresent == 1)
                    {
                        loginModel.List_OnLoad = objUserPref.FetchListOnLoad(Convert.ToInt16(strUsrPk));
                        loginModel.GRID_RECORD_SIZE = objUserPref.FetchNumberOfRecords(Convert.ToInt16(strUsrPk));
                        loginModel.STYLESHEET = objUserPref.FetchListOnLoad(Convert.ToInt16(strUsrPk));
                        loginModel.Date_OnLoad = objUserPref.FetchDateLoad(Convert.ToInt16(strUsrPk));
                        if (Convert.ToInt32(loginModel.Date_OnLoad) == 1)
                        {
                            loginModel.User_Preference_Date = objUserPref.FetchPrefDate(Convert.ToInt16(strUsrPk));
                        }
                    }
                    else if (IsUserPresent == 0)
                    {
                        loginModel.GRID_RECORD_SIZE = "15";
                        loginModel.List_OnLoad = "0";
                        loginModel.STYLESHEET = "StyleWGrid_7.css";
                    }

                    loginModel.REMOVALS_AVAILABLE = objCorp.RemovalsAvailable();
                    loginModel.EBOOKING_AVAILABLE = objCorp.EBookingAvailable();
                    loginModel.CHECK_EXRATE = objCorp.ChkExRateAvailable();
                    cls_User_Preference_Mst_Tbl objPref = new cls_User_Preference_Mst_Tbl();

                    loginModel.DATE_FORMAT = objPref.GetDateFormat(Convert.ToInt16(strUsrPk));
                    loginModel.ENVIRONMENT_PK = objPref.GetLanguageOption(Convert.ToInt16(strUsrPk));
                    loginModel.PASSWORD_EXPIRY_DT = objPref.GetPasswordExpiryDate(Convert.ToInt16(strUsrPk));
                    loginModel.EXP_NOTIFICATION_DT = objPref.GetPasswordAlertDate(Convert.ToInt16(strUsrPk));

                    objPref = null;

                    if (IsAdministrator(strUsrPk.Trim()) > 1)
                        isadminusr = true;
                    else
                        isadminusr = false;
                    if (isadminusr == true)
                    {
                        cls_User_Preference_Mst_Tbl objPref1 = new cls_User_Preference_Mst_Tbl();
                        if ((ExpireDays == LicenseExpiryDays) | (ExpireDays < LicenseExpiryDays))
                        {
                            status.message = "License Expiry Date is " + ExpiryDate + ". License will expire in " + ExpireDays + " days.\\n Please contact support-erp@quantum-bso.com to extend the license.";
                            status.status = false;
                        }
                    }

                    try
                    {
                        if (isadminusr == true)
                        {
                            if ((ExpireDays <= LicenseExpiryDays))
                            {
                                LicenseExipryNotification();
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                    }
                    if (IsActivityLogEnabled)
                    {
                        loginModel.SESSION_ID = Guid.NewGuid().ToString();
                           cls_UserActivity objLoginDet = new cls_UserActivity();
                        objLoginDet.fn_SaveLoginDetails(Convert.ToInt64(strLocPk), Convert.ToInt64(loginModel.USER_PK), loginModel.SESSION_ID, System.DateTime.Now, 0, "");
                    }

                    if (intValidUser == 5)
                    {
                        status.message = "User is not valid.";
                        status.status = false;
                        return cmnClass.ReturnDeserializedJson(status);
                    }
                    else if (intValidUser == 6)
                    {
                        status.message = "User is not enabled.";
                        status.status = false;
                        return cmnClass.ReturnDeserializedJson(status);
                    }
                    Int32 NoOfDays = default(Int32);
                    Int32 PassExp = default(Int32);
                    System.DateTime passdate = default(System.DateTime);
                    PassExp = Convert.ToInt32(loginModel.PASSWORD_EXPIRY_DT);
                    Int32 daysAlert = default(Int32);
                    daysAlert = Convert.ToInt32(loginModel.EXP_NOTIFICATION_DT);
                    if (Convert.ToString(loginModel.EXP_NOTIFICATION_DT) == "0")
                    {
                        daysAlert = 0;
                    }
                    passdate = objUser.chkpasswordExpiry(Convert.ToInt32(strUsrPk));
                    NoOfDays = Convert.ToInt32((DateAndTime.DateDiff(DateInterval.Day, passdate, System.DateTime.Now)));
                    if (((PassExp - daysAlert) <= NoOfDays & NoOfDays < PassExp))
                    {
                    }
                    else
                    {
                        Password = "2";
                    }

                    if (CheckPasswordExpiry(loginModel) == true & Password == "2")
                    {
                        //if ((Request.QueryString["MasterForm"] != null))
                        //{
                        //    HttpContext.Current.Session.Add("External_Mail_FOR_LOGOUT", "Externalmail");
                        //    reURL = RedirectURL(Request.QueryString("MasterForm"));
                        //}
                        //else
                        //{
                        if (Customer == 0)
                        {
                            status.message = "Forms/03Security/frmMainMenu.aspx";
                            status.status = true;
                            return cmnClass.ReturnDeserializedJson(status);
                        }
                        else
                        {
                            status.message = "Forms/03Security/frmMainMenuTNT.aspx";
                            status.status = true;
                            return cmnClass.ReturnDeserializedJson(status);
                        }
                    }
                    else
                    {
                        // Dim jscript As String
                    }
                }
                else
                {
                }
            }
            catch (Exception ex)
            {
                status.message = ex.Message;
                status.status = false;
                return cmnClass.ReturnDeserializedJson(status);
            }
            return cmnClass.ReturnDeserializedJson(status);
        }

        [AcceptVerbs("GET", "POST")]
        [HttpGet]
        public object GetMenuData()
        {
            cls_MENU_MST_TBL cs = new cls_MENU_MST_TBL();
            string value = cs.GetMenuData(1, 1841, 3);
            return JsonConvert.DeserializeObject(value);
        }
    }
}