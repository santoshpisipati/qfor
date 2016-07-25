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

using Microsoft.VisualBasic;
using System;
using System.Data;
using System.Management;
using System.Web;
namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.Common" />
    public class cls_ValidateLicense : Common
    {
        /// <summary>
        /// The object request
        /// </summary>
        public HttpRequest objRequest;
        /// <summary>
        /// The object application
        /// </summary>
        public HttpApplicationState ObjApplication;
        /// <summary>
        /// The object cache
        /// </summary>
        public System.Web.Caching.Cache objCache;

        /// <summary>
        /// The object CLS license
        /// </summary>
        private cls_Qcore_ValidateLicense objClsLicense = new cls_Qcore_ValidateLicense();
        /// <summary>
        /// The arr_ license
        /// </summary>
        Array arr_License = null;
        #region "License Key Validation Process"

        #region "fn_ChkRegistrationCode  - Check if logging in for first time"
        /// <summary>
        /// FN_s the CHK registration code exists.
        /// </summary>
        /// <returns></returns>
        public Int32 fn_ChkRegistrationCodeExists()
        {
            try
            {
                if (objClsLicense.fn_chkRegistrationCode() == "0")
                {
                    return 0;
                }
                else
                {
                    return 1;
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "fn_ChkRegistrationCode  - Check if logging in for first time"

        #region "fn_ValidateUsers()- Validate License Key"

        /// <summary>
        /// FN_s the validate users.
        /// </summary>
        /// <param name="objPage">The object page.</param>
        /// <param name="hdnRegistrationKey">The HDN registration key.</param>
        /// <param name="lblStatus">The label status.</param>
        /// <param name="txtUserId">The text user identifier.</param>
        /// <param name="ExipreDays">The exipre days.</param>
        /// <param name="ExpiryDate">The expiry date.</param>
        /// <returns></returns>
        public CLS_STATUS fn_ValidateUsers(object objPage, string hdnRegistrationKey, String lblStatus, string txtUserId, Int64 ExipreDays, System.DateTime ExpiryDate)
        {
            // Change: Converted lblStatus from label to String
            Array arr_License = null;
            string str_result = null;
            CLS_STATUS userStatus = new CLS_STATUS();
            try
            {
                hdnRegistrationKey = objClsLicense.fn_GetRegistrationCode(Convert.ToInt64(objClsLicense.fn_GetCorporatePK()));

                if (string.IsNullOrEmpty(hdnRegistrationKey))
                {
                    userStatus.message = "No Valid License Key Found.Please Register New License Key.";
                    userStatus.status = false;
                    userStatus.value = 0;
                    return userStatus;
                }
                else
                {
                    hdnRegistrationKey = objClsLicense.DecryptString128Bit(hdnRegistrationKey, "32");
                    arr_License = hdnRegistrationKey.Split(new Char[] { '~' });
                    //Regex.Split(hdnRegistrationKey, "~~~");

                    if (Convert.ToString(arr_License.GetValue(6)) == "NAMEDUSER")
                    {
                        arr_License.SetValue("1", 6);
                    }
                    else
                    {
                        arr_License.SetValue("2", 6);
                    }

                    if (Convert.ToString(arr_License.GetValue(9)) == "PERP")
                    {
                        arr_License.SetValue("1", 9);
                    }
                    else
                    {
                        arr_License.SetValue("2", 9);
                    }

                    if (Convert.ToString(arr_License.GetValue(0)).Trim().Length > 0)
                    {
                        string[] TMP_NO_OF_LICENSE_USERS = null;
                        TMP_NO_OF_LICENSE_USERS = Convert.ToString(arr_License.GetValue(0)).Split(',');
                        if (TMP_NO_OF_LICENSE_USERS.Length == 2)
                        {
                            //HttpContext.Current.Session.Add("SESSION_NO_OF_LICENSE_USERS", TMP_NO_OF_LICENSE_USERS[1]);
                        }
                    }

                    if (fn_GetServerSerialNr(Convert.ToString(arr_License.GetValue(3)), Convert.ToString(arr_License.GetValue(18))) == 1)
                    {
                        //Periodic
                        if (Convert.ToInt32(arr_License.GetValue(9)) == 1)
                        {
                            DateTime expDate = new DateTime();
                            DateTime currDate = DateTime.Now;
                            Int64 NoOfDays = default(Int64);
                            expDate = Convert.ToDateTime(arr_License.GetValue(15));
                            // License Expiry date
                            currDate = objClsLicense.fn_GetTodayDate();
                            // Get today's date from oracle db format(date.Now,"dd/MM/yyyy")
                            NoOfDays = (DateAndTime.DateDiff(DateInterval.Day, currDate, expDate));
                            ExipreDays = NoOfDays;
                            ExpiryDate = expDate;
                            ///'Raghu Check
                            if (System.DateTime.Compare(Convert.ToDateTime(string.Format(expDate.ToString(), "dd/MM/yy")), Convert.ToDateTime(string.Format(currDate.ToString(), "dd/MM/yy"))) < 0)
                            {
                                userStatus.message = "License Key has Expired";
                                userStatus.value = 0;
                                userStatus.status = false;
                                return userStatus;
                            }
                        }

                        //fn_GetlogedUsercount(objRequest.ServerVariables["remote_addr"].ToString());
                        //if (ObjApplication[1]("NUM_LOGIN_USERS") > 1)
                        //if (Convert.ToInt32(ObjApplication[1]) > 1)
                        //{
                        //    //Check How many Users can login for this Product
                        //    WorkFlow objWF = new WorkFlow();
                        //    string str_Db_User_id = null;
                        //    string str_Output = null;
                        //    Array str_product = null;
                        //    Array str_product_usr = null;
                        //    Int32 int_Prodcnt = default(Int32);
                        //    bool Prodflg = false;

                        //    str_Db_User_id = objWF.MyUserName.ToUpper();
                        //    arr_License.GetValue(int_Prodcnt).ToString();
                        //    str_product = arr_License.GetValue(0).ToString().Split(new Char[] { '&' });
                        //    for (int_Prodcnt = 0; int_Prodcnt <= str_product.Length - 1; int_Prodcnt++)
                        //    {
                        //        str_product_usr = str_product.GetValue(int_Prodcnt).ToString().Split(',');
                        //        //Get The Product ID from DB based on Application DB-User-ID.
                        //        //If Matching then check No of Users for that Product with Application variable  Application("NUM_LOGIN_USERS")
                        //        if (str_product_usr.GetValue(0) == fn_GetProduct_by_DBUser(str_Db_User_id))
                        //        {
                        //            Prodflg = true;
                        //            //if (ObjApplication("NUM_LOGIN_USERS") > Convert.ToInt32(str_product_usr.GetValue(1)))
                        //            //{
                        //            //    ObjApplication("NUM_LOGIN_USERS") = ObjApplication("NUM_LOGIN_USERS") - 1;
                        //            //    lblStatus.Text = "Nr. of Users Exceeded the Max Limit - " + str_product_usr.GetValue(1) + " .Please wait for Sometime.";
                        //            //    return 0;
                        //            //}
                        //        }
                        //    }
                        //    if (Prodflg == false)
                        //    {
                        //        userStatus.message = "License Key not valid for - " + fn_GetProduct_by_DBUser(str_Db_User_id) + " Application.";
                        //        userStatus.value = 0;
                        //        userStatus.status = false;
                        //        return userStatus;
                        //    }
                        //}
                        //    End If
                        userStatus.message = "Success";
                        userStatus.value = 1;
                        userStatus.status = true;
                        return userStatus;
                    }
                    else
                    {
                       userStatus.message = "Application Server Changed. Please Register New License Key.";
                        userStatus.status = false;
                        userStatus.value = 0;
                        return userStatus;
                    }
                }
            }
            catch (Exception ex)
            {
                userStatus.message = ex.Message;
                userStatus.status = false;
                userStatus.value = 0;
            }
            return userStatus;
        }

        #endregion "fn_ValidateUsers()- Validate License Key"

        #region "fn_GetServerSerialNr - function to validate the server serial number"

        /// <summary>
        /// FN_s the get server serial nr.
        /// </summary>
        /// <param name="License_SerialNr">The license_ serial nr.</param>
        /// <param name="DemoFlag">The demo flag.</param>
        /// <returns></returns>
        public Int32 fn_GetServerSerialNr(string License_SerialNr, string DemoFlag)
        {
            ManagementObjectSearcher query = default(ManagementObjectSearcher);
            ManagementObjectCollection queryCollection = default(ManagementObjectCollection);
            System.Management.ManagementScope ms = default(System.Management.ManagementScope);
            System.Management.ObjectQuery oq = default(System.Management.ObjectQuery);
            string str_Result = null;
            string str_LicenseText = null;
            Array arr_License = null;
            string str_HostName = null;
            string str_SerialNr = null;
            ManagementObject mo = default(ManagementObject);
            string strScript = null;
            string str_ms = null;
            DataSet ds_serverdtls = new DataSet();
            ConnectionOptions co = new ConnectionOptions();
            System.Net.IPHostEntry host = null;
            string strlocalhostname = null;

            str_HostName = objRequest != null ? objRequest.ServerVariables["HTTP_HOST"] : "";
            try
            {
                //Purchase
                if (DemoFlag == "PURCH")
                {
                    str_ms = "\\\\" + str_HostName + "\\root\\cimv2";
                    if (str_HostName == System.Environment.MachineName | str_HostName == "localhost")
                    {
                        ms = new System.Management.ManagementScope(str_ms);
                    }
                    else
                    {
                        var _with1 = co;
                        _with1.Impersonation = System.Management.ImpersonationLevel.Impersonate;
                        _with1.Authentication = System.Management.AuthenticationLevel.Packet;
                        ms = new System.Management.ManagementScope(str_ms);
                    }

                    oq = new System.Management.ObjectQuery("SELECT * FROM Win32_BaseBoard");
                    query = new ManagementObjectSearcher(ms, oq);
                    queryCollection = query.Get();
                    foreach (ManagementObject mo_loopVariable in queryCollection)
                    {
                        mo = mo_loopVariable;
                        str_SerialNr = mo["SerialNumber"].ToString().Trim();
                    }

                    if (str_SerialNr == License_SerialNr)
                    {
                        return 1;
                    }
                    else
                    {
                        return 0;
                    }

                    // DEMO - Don't check Server serial number
                }
                else
                {
                    return 1;
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "fn_GetServerSerialNr - function to validate the server serial number"

        #region "Cache Management"

        #region "IsUserIdExist"



        #endregion "IsUserIdExist"

        #region "GetlogedUsercount"

        //public bool fn_GetlogedUsercount(string RemoteIp)
        //{
        //    if ((ObjApplication("NUM_LOGIN_USERS") == null))
        //    {
        //        ObjApplication("NUM_LOGIN_USERS") = 1;
        //    }
        //    else
        //    {
        //        ObjApplication("NUM_LOGIN_USERS") = ObjApplication("NUM_LOGIN_USERS") + 1;
        //    }

        //    return true;
        //}

        #endregion "GetlogedUsercount"

        #region "CreateEntryInCache"

        //public object fn_CreateEntryInCache(object objPage, string txtUSER_ID)
        //{
        //    objPage.Cache().Insert(txtUSER_ID, objRequest.ServerVariables("remote_addr"), null, objPage.Cache.NoAbsoluteExpiration, null, CacheItemPriority.High, null);
        //}

        #endregion "CreateEntryInCache"

        #region "RemoveEntryFromCache"

        /// <summary>
        /// FN_s the remove entry from cache.
        /// </summary>
        /// <param name="Key">The key.</param>
        /// <param name="Val">The value.</param>
        /// <param name="Reason">The reason.</param>
        public void fn_RemoveEntryFromCache(string Key, object Val, System.Web.Caching.CacheItemRemovedReason Reason)
        {
            //  HttpContext.Current.Session.Clear()
            //  HttpContext.Current.Session.Abandon()
        }

        #endregion "RemoveEntryFromCache"

        #endregion "Cache Management"

        #endregion "License Key Validation Process"
    }
}