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
using System.Text;
using System.Web;
using System.Web.Mail;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
   	public class Cls_CustomerReconciliation : CommonFeatures
    {
        #region "Fetch Temporary Customer"
        public string errorLog;
        public DataSet FetchTempCustomers(string T_Customer_Mst_Pk = "", string T_Customer_Name = "", int CurrentBType = 0, int BType = 0, string strLoc = "", string SearchType = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 flag = 0, bool ChkReconcile = false,
        string PhNr = "")
        {

            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            System.Web.UI.Page objPage = new System.Web.UI.Page();


            if (!string.IsNullOrEmpty(T_Customer_Mst_Pk))
            {
                strCondition += " And TCT.CUSTOMER_MST_PK  =" + T_Customer_Mst_Pk;
            }

            if (T_Customer_Name.ToString().Trim().Length > 0 & SearchType == "C")
            {
                strCondition += " And UPPER(TCT.CUSTOMER_NAME) like '%" + T_Customer_Name.ToUpper().Replace("'", "''") + "%' " ;
            }
            else if (T_Customer_Name.ToString().Trim().Length > 0 & SearchType == "S")
            {
                strCondition += " And UPPER(TCT.CUSTOMER_NAME) like '" + T_Customer_Name.ToUpper().Replace("'", "''") + "%' " ;
            }
            if (PhNr.ToString().Trim().Length > 0 & SearchType == "C")
            {
                strCondition += " And UPPER(TCCD.ADM_PHONE_NO_1) like '%" + PhNr.ToUpper().Replace("'", "''") + "%' " ;
            }
            else if (PhNr.ToString().Trim().Length > 0 & SearchType == "S")
            {
                strCondition += " And UPPER(TCCD.ADM_PHONE_NO_1) like '" + PhNr.ToUpper().Replace("'", "''") + "%' " ;
            }
            if (BType != 3)
            {
                strCondition += " and (TCT.BUSINESS_TYPE = " + BType + " OR TCT.BUSINESS_TYPE = 3 )";
            }
            if (CurrentBType == 3 & BType == 3)
            {
                strCondition += " AND TCT.BUSINESS_TYPE IN (1,2,3) ";
            }
            else if (CurrentBType == 3 & BType == 2)
            {
                strCondition += " AND TCT.BUSINESS_TYPE IN (2,3) ";
            }
            else if (CurrentBType == 3 & BType == 1)
            {
                strCondition += " AND TCT.BUSINESS_TYPE IN (1,3) ";
                //aded by surya prasad on 04-jan-2009 for removasls as any type of user can reconcile the removal customer
            }
            else if (BType == 4)
            {
                strCondition += " AND TCT.BUSINESS_TYPE IN (4) ";
            }
            else
            {
                strCondition += " AND TCT.BUSINESS_TYPE = " + CurrentBType + " ";
            }
            //If BlankGrid = 0 Then
            //    strCondition &= " AND 1=2 "
            //End If
            //Snigdharani - 05/02/2009 - EBooking Integration
            if (Convert.ToInt32(HttpContext.Current.Session["EBOOKING_AVAILABLE"]) == 0)
            {
                strCondition += " AND TCT.TRANSACTION_TYPE NOT IN (4)  ";
            }
            if (flag == 0)
            {
                strCondition += " AND 1=2 ";
            }
            strSQL = "SELECT Count(*) ";
            strSQL += " FROM";
            strSQL += " temp_customer_tbl TCT,";
            strSQL += " temp_customer_contact_dtls TCCD, ";
            strSQL += " user_mst_tbl UMT ";
            strSQL += " WHERE TCT.CUSTOMER_MST_PK = TCCD.CUSTOMER_MST_FK";
            strSQL += " AND  TCT.ACTIVE_FLAG = 1";
            if (ChkReconcile == true)
            {
                strSQL += " AND  TCT.RECONCILE_STATUS IN (1,2) ";
            }
            else
            {
                strSQL += " AND  TCT.RECONCILE_STATUS = 0";
            }
            strSQL += " AND TCT.CREATED_BY_FK = UMT.USER_MST_PK(+) ";
            //Modified By Arun on 20/10/2012 to show reporting location temporary customers
            //strSQL &= vbCrLf & " AND UMT.DEFAULT_LOCATION_FK IN (" & strLoc & ") "
            strSQL += " AND UMT.DEFAULT_LOCATION_FK IN ( SELECT L.LOCATION_MST_PK FROM LOCATION_MST_TBL L";
            strSQL += " START WITH L.LOCATION_MST_PK IN ( " + strLoc + ")CONNECT BY PRIOR L.LOCATION_MST_PK = L.REPORTING_TO_FK)";
            //*******************************
            strSQL += " AND TCT.CUSTOMER_ID NOT IN(SELECT  cmt.customer_id FROM customer_mst_tbl cmt WHERE cmt.ACTIVE_FLAG =1) ";
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

            strSQL = " SELECT  qry.* FROM ";
            strSQL += " (SELECT ROWNUM SR_NO,T.* FROM ";
            strSQL += " (SELECT TCT.TRANSACTION_TYPE TRANS_TYPE,";
            strSQL += "  TCT.CUSTOMER_MST_PK T_CUST_PK, ";
            strSQL += "  TCT.Business_Type BIZTYPE, ";
            strSQL += "  UPPER(TCT.CUSTOMER_ID) T_CUST_ID,";
            strSQL += "  TCT.CUSTOMER_NAME T_CUST_NAME,";
            strSQL += "  DECODE(TCCD.ADM_SALUTATION, '1', 'Mr', '2', 'Miss') SALUTATION,";
            strSQL += "  DECODE(TCCD.ADM_SALUTATION, '1', 'Mr. ', '2', 'Miss. ') || TCCD.ADM_CONTACT_PERSON CONTACT,";
            strSQL += "  TCCD.ADM_EMAIL_ID EMAIL,";
            strSQL += "  (TCCD.ADM_ADDRESS_1 || '  ' || TCCD.ADM_ADDRESS_2 || '  ' || TCCD.ADM_ADDRESS_3 || '  ' || TCCD.ADM_ZIP_CODE || ' ' || TCCD.adm_city || ' ' || (select cmt.country_name from country_mst_tbl cmt where cmt.country_mst_pk=tccd.adm_country_mst_fk) ) ADDRESS,";
            strSQL += "  TCCD.ADM_PHONE_NO_1 PHONE,";
            strSQL += "  TCCD.ADM_FAX_NO FAX,umt.user_name as Created_By,to_char(tct.created_dt,'dd/mm/yyyy') as Created_Date,tct.created_form,";
            if (ChkReconcile == true)
            {
                strSQL += "  DECODE(TCT.RECONCILE_STATUS, '1', 'MAP', '2', 'NEW') MAPTO,";
            }
            else
            {
                strSQL += "  ' ' MAPTO,";
            }
            strSQL += "  ' ' MAP_BUTTON, ";
            if (ChkReconcile == true)
            {
                strSQL += "  cmt.customer_name P_CUST_ID, ";
                strSQL += "  cmt.customer_mst_pk P_CUST_PK ";
            }
            else
            {
                strSQL += "  ' ' P_CUST_ID, ";
                strSQL += "  '0' P_CUST_PK ";
            }
            strSQL += " FROM ";
            strSQL += " temp_customer_tbl TCT, ";
            strSQL += " temp_customer_contact_dtls TCCD, ";
            if (ChkReconcile == true)
            {
                strSQL += " customer_mst_tbl  cmt, ";
            }
            strSQL += " user_mst_tbl UMT ";
            strSQL += " WHERE TCT.CUSTOMER_MST_PK = TCCD.CUSTOMER_MST_FK ";
            strSQL += " AND  TCT.ACTIVE_FLAG = 1 ";
            if (ChkReconcile == true)
            {
                strSQL += "  AND  TCT.RECONCILE_STATUS IN (1,2) ";
                strSQL += "  and tct.permanent_cust_mst_fk = cmt.customer_mst_pk";
            }
            else
            {
                strSQL += " AND  TCT.RECONCILE_STATUS = 0 ";
            }

            // Amit
            strSQL += " AND TCT.CREATED_BY_FK = UMT.USER_MST_PK(+) ";
            //Modified By Arun on 20/10/2012 to show reporting location temporary customers
            //strSQL &= vbCrLf & " AND UMT.DEFAULT_LOCATION_FK IN (" & strLoc & ") "
            strSQL += " AND UMT.DEFAULT_LOCATION_FK IN ( SELECT L.LOCATION_MST_PK FROM LOCATION_MST_TBL L";
            strSQL += " START WITH L.LOCATION_MST_PK IN ( " + strLoc + ")CONNECT BY PRIOR L.LOCATION_MST_PK = L.REPORTING_TO_FK)";
            //*******************************
            // Amit "IQA DTS-145"
            //strSQL &= vbCrLf & " AND TCT.CUSTOMER_ID NOT IN(SELECT  cmt.customer_id FROM customer_mst_tbl cmt WHERE cmt.ACTIVE_FLAG =1) "
            // End
            strSQL += strCondition + " ORDER BY T_CUST_ID) T )qry WHERE SR_NO  Between " + start + " and " + last;
            strSQL += " ORDER BY  SR_NO";
            try
            {
                return (new WorkFlow()).GetDataSet(strSQL);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }
        #endregion

        #region "Fetch For Enhanced Search"
        public string E_FetchTempCustomer(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strLOC_MST_IN = null;
            string strSERACH_IN = "";
            int intBUSINESS_TYPE_IN = 3;
            int Reconcile = 0;
            string strReq = null;
            var strNull = DBNull.Value;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                intBUSINESS_TYPE_IN = Convert.ToInt32(arr.GetValue(2));
            if (arr.Length > 3)
                Reconcile = Convert.ToInt32(arr.GetValue(3));
            //strLOC_MST_IN = arr(3)

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_CUSTOMER_PKG.GETTEMPCUSTOMER_COMMON";

                var _with1 = selectCommand.Parameters;
                _with1.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with1.Add("BUSINESS_TYPE_IN", intBUSINESS_TYPE_IN).Direction = ParameterDirection.Input;
                _with1.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with1.Add("LOCATION_MST_FK_IN", loc).Direction = ParameterDirection.Input;
                _with1.Add("RECONCILE_IN", Reconcile).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }
        #endregion

        #region "Reconcile Customer"
        public ArrayList ReconcileCustomer(DataSet CustDS)
        {
            OracleCommand updCommand = new OracleCommand();
            WorkFlow objWK = new WorkFlow();
            Int32 j = default(Int32);
            string strData = "";
            bool flagEbk = false;
            OracleTransaction TRAN = null;
            arrMessage.Clear();
            try
            {
                objWK.OpenConnection();
                TRAN = objWK.MyConnection.BeginTransaction();

                var _with2 = updCommand;
                _with2.Connection = objWK.MyConnection;
                _with2.CommandType = CommandType.StoredProcedure;
                _with2.Transaction = TRAN;
                //.CommandText = objWK.MyUserName & ".CUSTOMER_RECONCILIATION_PKG.RECONCIL_CUSTOMER"
                var _with3 = _with2.Parameters;
                for (j = 0; j <= CustDS.Tables[0].Rows.Count - 1; j++)
                {
                    if (Convert.ToInt32(CustDS.Tables[0].Rows[j]["T_CUST_TRANS_TYPE"]) == 4)
                    {
                        flagEbk = true;
                    }
                    else
                    {
                        flagEbk = false;
                    }
                    if (flagEbk == true)
                    {
                        updCommand.CommandText = objWK.MyUserName + ".PKG_EBK_CUST_REG.RECONCIL_CUSTOMER";
                    }
                    else
                    {
                        updCommand.CommandText = objWK.MyUserName + ".CUSTOMER_RECONCILIATION_PKG.RECONCIL_CUSTOMER";
                    }
                    //updCommand.CommandText = objWK.MyUserName & ".CUSTOMER_RECONCILIATION_PKG.RECONCIL_CUSTOMER"
                    _with3.Clear();
                    _with3.Add("T_CUST_PK_IN", CustDS.Tables[0].Rows[j]["T_CUST_PK"]).Direction = ParameterDirection.Input;
                    _with3.Add("T_CUST_TRANS_TYPE_IN", CustDS.Tables[0].Rows[j]["T_CUST_TRANS_TYPE"]).Direction = ParameterDirection.Input;
                    _with3.Add("T_BIZ_TYPE_IN", CustDS.Tables[0].Rows[j]["T_BIZ_TYPE"]).Direction = ParameterDirection.Input;
                    _with3.Add("P_CUST_PK_IN", CustDS.Tables[0].Rows[j]["P_CUST_PK"]).Direction = ParameterDirection.Input;
                    _with3.Add("MapOrNew_IN", CustDS.Tables[0].Rows[j]["MapOrNew"]).Direction = ParameterDirection.Input;
                    _with3.Add("LAST_MODIFIED_BY_FK_IN", LAST_MODIFIED_BY).Direction = ParameterDirection.Input;
                    _with3.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                    updCommand.ExecuteNonQuery();
                    if (flagEbk == true)
                    {
                        if (string.IsNullOrEmpty(strData))
                        {
                            strData = updCommand.Parameters["RETURN_VALUE"].Value.ToString();
                        }
                        else
                        {
                            strData = strData + "," + updCommand.Parameters["RETURN_VALUE"].Value;
                        }
                    }
                    if (string.Compare(updCommand.Parameters["RETURN_VALUE"].Value.ToString(), "Reconciled Successfully") > 0 | updCommand.Parameters["RETURN_VALUE"].Value == "1")
                    {
                        if (string.Compare(updCommand.Parameters["RETURN_VALUE"].Value.ToString(), "Reconciled Successfully")>0)
                        {
                            if (flagEbk == true)
                            {
                                errorLog = errorLog + "#" + CustDS.Tables[0].Rows[j]["T_CUST_ID"] + " : " + "Reconciled Successfully";
                            }
                            else
                            {
                                errorLog = errorLog + "#" + CustDS.Tables[0].Rows[j]["T_CUST_ID"] + " : " + updCommand.Parameters["RETURN_VALUE"].Value;
                            }
                        }
                        else
                        {
                            errorLog = errorLog + "#" + CustDS.Tables[0].Rows[j]["T_CUST_ID"] + " : All Data Saved Successfully";
                        }
                        //'added for the dts -6520
                    }
                    else if (string.Compare(updCommand.Parameters["RETURN_VALUE"].Value.ToString(), "This temporary customer is already being reconciled") > 0)
                    {
                        errorLog = errorLog + "#" + CustDS.Tables[0].Rows[j]["T_CUST_ID"] + " : " + updCommand.Parameters["RETURN_VALUE"].Value;
                    }
                    //updCommand.ExecuteNonQuery()
                    //If InStr(updCommand.Parameters("RETURN_VALUE").Value , "Reconciled Successfully") > 0 =  Or updCommand.Parameters("RETURN_VALUE").Value = "1" Then
                    //    Exit For
                    //End If
                }
                //updCommand.ExecuteNonQuery()
                TRAN.Commit();
                if (Convert.ToInt32(HttpContext.Current.Session["EBOOKING_AVAILABLE"])== 1)
                {
                    if (!string.IsNullOrEmpty(strData))
                    {
                        Array strArray = null;
                        strArray = strData.Split(',');
                        int k = 0;
                        string custID = null;
                        string EmailID = null;
                        Array mainDataArr = null;
                        string Maindata = null;
                        for (k = 0; k <= strArray.Length - 1; k++)
                        {
                            Maindata = Convert.ToString(strArray.GetValue(k));
                            mainDataArr = Maindata.Split('~');
                            SendMail(mainDataArr.GetValue(3).ToString(), mainDataArr.GetValue(1).ToString(), mainDataArr.GetValue(2).ToString());
                        }
                    }
                }
                arrMessage.Add("Please Click Error Log Button To View Error Log File");
                return arrMessage;
                //Snigdharani - 04/03/2009 - E-Booking Integration
                //If Session("EBOOKING_AVAILABLE") = 1 Then
                //    If InStr(updCommand.Parameters("RETURN_VALUE").Value, "Reconciled Successfully") > 0 Or updCommand.Parameters("RETURN_VALUE").Value = "1" Then
                //        If InStr(updCommand.Parameters("RETURN_VALUE").Value, "Reconciled Successfully") Then
                //            arrMessage.Add(updCommand.Parameters("RETURN_VALUE").Value)
                //            If strData <> "" Then
                //                Dim strArray As Array
                //                strArray = strData.Split(",")
                //                Dim k As Integer
                //                Dim custID, EmailID As String
                //                Dim mainDataArr As Array
                //                Dim Maindata As String
                //                For k = 0 To strArray.Length - 1
                //                    Maindata = strArray(k)
                //                    mainDataArr = Maindata.Split("~")
                //                    SendMail(mainDataArr(3), mainDataArr(1))
                //                Next
                //                arrMessage.Add("All Data Saved Successfully")
                //            End If
                //        Else
                //            arrMessage.Add("All Data Saved Successfully")
                //        End If
                //        TRAN.Commit()
                //        Return arrMessage
                //    Else
                //        arrMessage.Add(updCommand.Parameters("RETURN_VALUE").Value)
                //        TRAN.Rollback()
                //        Return arrMessage
                //    End If
                //Else
                //    If InStr(updCommand.Parameters("RETURN_VALUE").Value, "Reconciled Successfully") > 0 Or updCommand.Parameters("RETURN_VALUE").Value = "1" Then
                //        If InStr(updCommand.Parameters("RETURN_VALUE").Value, "Reconciled Successfully") Then
                //            arrMessage.Add(updCommand.Parameters("RETURN_VALUE").Value)
                //        Else
                //            arrMessage.Add("All Data Saved Successfully")
                //        End If

                //        TRAN.Commit()
                //        Return arrMessage
                //    Else
                //        arrMessage.Add(updCommand.Parameters("RETURN_VALUE").Value)
                //        TRAN.Rollback()
                //        Return arrMessage
                //    End If
                //End If
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                TRAN.Rollback();
                return arrMessage;
                //'added by minakshi on 17-feb-08 for connection close task 
            }
            finally
            {
                objWK.CloseConnection();
            }
        }
        //Snigdharani - 04/03/2009 - E-Booking Integration
        public ArrayList ReconcileECustomer(string CustID, string PWD, string TCusId)
        {
            OracleCommand updCommand = new OracleCommand();
            OracleDataReader dsRead = null;
            WorkFlow objWK = new WorkFlow();
            Int32 j = default(Int32);
            string sql = null;
            string sqlstr = null;
            string str = null;
            string SQPRO = null;
            string Password = null;
            int numSQ = 0;
            int strNum = 0;
            SQPRO = "EBKC";
            arrMessage.Clear();
            try
            {
                objWK.OpenConnection();
                sql = "select substr(t.regn_nr_pk,5,10) seq from syn_ebk_m_cust_regn t";
                dsRead = objWK.GetDataReader(sql);
                while (dsRead.Read())
                {
                    str = getDefault(dsRead.GetValue(0), "").ToString();
                    Int32 value = 0;
                    if (Int32.TryParse(str, out value))
                    {
                        if (value >= 0)
                        {
                            strNum = Convert.ToInt32(str);
                            //If numSQ > strNum Then
                            numSQ = strNum;
                        }
                        //End If
                    }
                }
                numSQ = numSQ + 1;
                str = Convert.ToString(numSQ);
                if (str.Length == 1)
                {
                    str = string.Concat("00000", str);
                }
                if (str.Length == 2)
                {
                    str = string.Concat("0000", str);
                }
                if (str.Length == 3)
                {
                    str = string.Concat("000", str);
                }
                if (str.Length == 4)
                {
                    str = string.Concat("00", str);
                }
                if (str.Length == 5)
                {
                    str = string.Concat("0", str);
                }
                SQPRO = string.Concat(SQPRO, str);

                Password = GenerateCode(2);
                PWD = Password;
                //sqlstr = "update qbso_ebk_m_cust_regn set regn_nr_pk='" & SQPRO & "',cust_pwd='" & Password & "' where regn_nr_pk='" & CustID & "'"
                sqlstr = "";
                sqlstr = "update syn_ebk_m_cust_regn set regn_nr_pk='" + CustID + "',cust_pwd='" + Password + "' where regn_nr_pk='" + TCusId + "'";
                objWK.ExecuteCommands(sqlstr);
                sqlstr = "";
                sqlstr = "update syn_ebk_t_cust_address set regn_nr_fk='" + CustID + "' where regn_nr_fk='" + TCusId + "'";
                objWK.ExecuteCommands(sqlstr);
                arrMessage.Add("All Data Saved Successfully");
                return arrMessage;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                //TRAN.Rollback()
                return arrMessage;
            }
            finally
            {
                dsRead.Close();
                objWK.MyConnection.Close();
            }
        }
        #endregion
        public object SendMail(string MailId, string CUSTOMERID, string Password)
        {
            object functionReturnValue = null;
            MailMessage objMail = new MailMessage();
            //Dim Mailsend As String = ConfigurationSettings.AppSettings("MailServer")
            string EAttach = null;
            string dsMail = null;
            Int32 intCnt = default(Int32);
            try
            {
                //****************************** External*********************************
                objMail.Fields["http://schemas.microsoft.com/cdo/configuration/smtpserver"] = "smtpout.secureserver.net";
                objMail.Fields["http://schemas.microsoft.com/cdo/configuration/smtpserverport"] = 25;
                objMail.Fields["http://schemas.microsoft.com/cdo/configuration/sendusing"] = 2;
                objMail.Fields["http://schemas.microsoft.com/cdo/configuration/smtpauthenticate"] = 1;
                objMail.Fields["http://schemas.microsoft.com/cdo/configuration/sendusername"] = "support_temp@quantum-bso.com";
                objMail.Fields["http://schemas.microsoft.com/cdo/configuration/sendpassword"] = "test123";
                objMail.BodyFormat = MailFormat.Html;
                //or MailFormat.Text 
                objMail.To = MailId;
                objMail.From = "support_temp@quantum-bso.com";
                objMail.Subject = "Registration Confirmation";

                StringBuilder strHTML = new StringBuilder();
                strHTML.Append("<html><body>");
                strHTML.Append("<p><b>Dear Customer,<br><br>");
                strHTML.Append("Thanks for registration with us. <br><br>");
                strHTML.Append("Please find below your account information to log-in to our system. <br><br>");
                strHTML.Append(" UserID : " + CUSTOMERID + " <br> Password : " + Password + " <br><br>");
                strHTML.Append("Best Regards, <br><br>");
                strHTML.Append("Customer Support Team <br></b></p>");
                strHTML.Append("</body></html>");
                objMail.Body = strHTML.ToString();
                SmtpMail.SmtpServer = "smtpout.secureserver.net";
                SmtpMail.Send(objMail);
                objMail = null;
                return "All Data Saved Successfully.";
            }
            catch (Exception ex)
            {
                return "All Data Saved Successfully.Due to some problem Mail has not been sent";
                return functionReturnValue;
            }
            return functionReturnValue;
        }
    }
}