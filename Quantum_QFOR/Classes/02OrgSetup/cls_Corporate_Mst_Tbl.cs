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
//'*  Modified DateTime(DD-MON-YYYY)              Modified By                             Remarks (Bugs Related)
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
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class clsCorporate_Mst_Tbl : CommonFeatures
    {
        #region "KeyContact"

        /// <summary>
        /// Fetches the key contacts.
        /// </summary>
        /// <param name="EmpId">The emp identifier.</param>
        /// <returns></returns>
        public DataSet FetchKeyContacts(long EmpId = 0)
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            strSQL.Append("SELECT E.EMPLOYEE_NAME,");
            strSQL.Append("       RLMST.ROLE_DESCRIPTION,");
            strSQL.Append("       E.PHONE_NO,");
            strSQL.Append("       E.EMAIL_ID");
            strSQL.Append("  FROM ROLE_MST_TBL RLMST, USER_MST_TBL UMT, EMPLOYEE_MST_TBL E");
            strSQL.Append(" WHERE RLMST.ROLE_MST_TBL_PK(+) = UMT.ROLE_MST_FK");
            strSQL.Append("   AND UMT.EMPLOYEE_MST_FK = E.EMPLOYEE_MST_PK");
            strSQL.Append("   AND E.KEY_CONTACT = 1");
            strSQL.Append("   ORDER BY E.EMPLOYEE_NAME");

            WorkFlow objWF = new WorkFlow();
            try
            {
                return objWF.GetDataSet(strSQL.ToString());
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

        #endregion "KeyContact"

        #region "List of Members of the Class"

        /// <summary>
        /// The m_ corporate_ MST_ pk
        /// </summary>
        private Int64 M_Corporate_Mst_Pk;

        /// <summary>
        /// The m_ corporate_ identifier
        /// </summary>
        private string M_Corporate_Id;

        /// <summary>
        /// The m_ corporate_ name
        /// </summary>
        private string M_Corporate_Name;

        /// <summary>
        /// The m_ address_ line1
        /// </summary>
        private string M_Address_Line1;

        /// <summary>
        /// The m_ address_ line2
        /// </summary>
        private string M_Address_Line2;

        /// <summary>
        /// The m_ address_ line3
        /// </summary>
        private string M_Address_Line3;

        /// <summary>
        /// The m_ country_ MST_ fk
        /// </summary>
        private Int64 M_Country_Mst_Fk;

        /// <summary>
        /// The m_ country_ identifier
        /// </summary>
        private string M_Country_ID;

        /// <summary>
        /// The m_ country_ name
        /// </summary>
        private string M_Country_Name;

        /// <summary>
        /// The m_ state_ MST_ fk
        /// </summary>
        private Int64 M_State_Mst_Fk;

        /// <summary>
        /// The m_ state_ identifier
        /// </summary>
        private string M_State_Id;

        /// <summary>
        /// The m_ state_ name
        /// </summary>
        private string M_State_Name;

        /// <summary>
        /// The m_ city
        /// </summary>
        private string M_City;

        /// <summary>
        /// The m_ position t_ code
        /// </summary>
        private string M_POST_CODE;

        /// <summary>
        /// The m_ hom e_ page
        /// </summary>
        private string M_HOME_PAGE;

        /// <summary>
        /// The m_ email
        /// </summary>
        private string M_EMAIL;

        /// <summary>
        /// The m_ phone
        /// </summary>
        private string M_PHONE;

        /// <summary>
        /// The m_ fax
        /// </summary>
        private string M_FAX;

        /// <summary>
        /// The m_ con
        /// </summary>
        private int M_Con;

        /// <summary>
        /// The m_ comp reg no
        /// </summary>
        private string M_CompRegNo;

        /// <summary>
        /// The m_ GST no
        /// </summary>
        private string M_GSTNo;

        //Credit Manager
        /// <summary>
        /// The m_ credit MGR pk
        /// </summary>
        private Int64 M_CreditMgrPk;

        /// <summary>
        /// The m_ credit MGR identifier
        /// </summary>
        private string M_CreditMgrID;

        /// <summary>
        /// The m_ credit MGR name
        /// </summary>
        private string M_CreditMgrName;

        //Credit Manager
        /// <summary>
        /// The m_ business type
        /// </summary>
        private int M_BusinessType;

        /// <summary>
        /// The m_ base currency
        /// </summary>
        private int M_BaseCurrency;

        /// <summary>
        /// The m_ active flag
        /// </summary>
        private int M_ActiveFlag;

        /// <summary>
        /// The m_ check ex rate
        /// </summary>
        private int M_CheckExRate;

        /// <summary>
        /// The m_ biz type
        /// </summary>
        private int M_BizType;

        /// <summary>
        /// The m_ ex rate
        /// </summary>
        private int M_ExRate;

        /// <summary>
        /// The m_ FMC no
        /// </summary>
        private string M_FmcNo;

        /// <summary>
        /// The m_ iata code
        /// </summary>
        private string M_IataCode;

        /// <summary>
        /// The m_ acc no
        /// </summary>
        private string M_AccNo;

        #endregion "List of Members of the Class"

        /// <summary>
        /// The m_ vat percent
        /// </summary>
        private double M_VATPercent;

        #region "List of Properties"

        /// <summary>
        /// Gets or sets the email.
        /// </summary>
        /// <value>
        /// The email.
        /// </value>
        public string Email
        {
            get { return M_EMAIL; }
            set { M_EMAIL = value; }
        }

        /// <summary>
        /// Gets or sets the phone.
        /// </summary>
        /// <value>
        /// The phone.
        /// </value>
        public string Phone
        {
            get { return M_PHONE; }
            set { M_PHONE = value; }
        }

        /// <summary>
        /// Gets or sets the fax.
        /// </summary>
        /// <value>
        /// The fax.
        /// </value>
        public string Fax
        {
            get { return M_FAX; }
            set { M_FAX = value; }
        }

        /// <summary>
        /// Gets or sets the concurrency check.
        /// </summary>
        /// <value>
        /// The concurrency check.
        /// </value>
        public int ConcurrencyCheck
        {
            get { return M_Con; }
            set { M_Con = value; }
        }

        /// <summary>
        /// Gets or sets the home_ page.
        /// </summary>
        /// <value>
        /// The home_ page.
        /// </value>
        public string Home_Page
        {
            get { return M_HOME_PAGE; }
            set { M_HOME_PAGE = value; }
        }

        /// <summary>
        /// Gets or sets the position t_ code.
        /// </summary>
        /// <value>
        /// The position t_ code.
        /// </value>
        public string POST_CODE
        {
            get { return M_POST_CODE; }
            set { M_POST_CODE = value; }
        }

        /// <summary>
        /// Gets or sets the corporate_ MST_ pk.
        /// </summary>
        /// <value>
        /// The corporate_ MST_ pk.
        /// </value>
        public Int64 Corporate_Mst_Pk
        {
            get { return M_Corporate_Mst_Pk; }
            set { M_Corporate_Mst_Pk = value; }
        }

        /// <summary>
        /// Gets or sets the country_ identifier.
        /// </summary>
        /// <value>
        /// The country_ identifier.
        /// </value>
        public string Country_ID
        {
            get { return M_Country_ID; }
            set { M_Country_ID = value; }
        }

        /// <summary>
        /// Gets or sets the name of the country_.
        /// </summary>
        /// <value>
        /// The name of the country_.
        /// </value>
        public string Country_Name
        {
            get { return M_Country_Name; }
            set { M_Country_Name = value; }
        }

        /// <summary>
        /// Gets or sets the country_ MST_ fk.
        /// </summary>
        /// <value>
        /// The country_ MST_ fk.
        /// </value>
        public Int64 Country_Mst_Fk
        {
            get { return M_Country_Mst_Fk; }
            set { M_Country_Mst_Fk = value; }
        }

        /// <summary>
        /// Gets or sets the state_ MST_ fk.
        /// </summary>
        /// <value>
        /// The state_ MST_ fk.
        /// </value>
        public Int64 State_Mst_Fk
        {
            get { return M_State_Mst_Fk; }
            set { M_State_Mst_Fk = value; }
        }

        /// <summary>
        /// Gets or sets the state_ identifier.
        /// </summary>
        /// <value>
        /// The state_ identifier.
        /// </value>
        public string State_ID
        {
            get { return M_State_Id; }
            set { M_State_Id = value; }
        }

        /// <summary>
        /// Gets or sets the name of the state_.
        /// </summary>
        /// <value>
        /// The name of the state_.
        /// </value>
        public string State_Name
        {
            get { return M_State_Name; }
            set { M_State_Name = value; }
        }

        /// <summary>
        /// Gets or sets the corporate_ identifier.
        /// </summary>
        /// <value>
        /// The corporate_ identifier.
        /// </value>
        public string Corporate_Id
        {
            get { return M_Corporate_Id; }
            set { M_Corporate_Id = value; }
        }

        /// <summary>
        /// Gets or sets the name of the corporate_.
        /// </summary>
        /// <value>
        /// The name of the corporate_.
        /// </value>
        public string Corporate_Name
        {
            get { return M_Corporate_Name; }
            set { M_Corporate_Name = value; }
        }

        /// <summary>
        /// Gets or sets the address_ line1.
        /// </summary>
        /// <value>
        /// The address_ line1.
        /// </value>
        public string Address_Line1
        {
            get { return M_Address_Line1; }
            set { M_Address_Line1 = value; }
        }

        /// <summary>
        /// Gets or sets the address_ line2.
        /// </summary>
        /// <value>
        /// The address_ line2.
        /// </value>
        public string Address_Line2
        {
            get { return M_Address_Line2; }
            set { M_Address_Line2 = value; }
        }

        /// <summary>
        /// Gets or sets the address_ line3.
        /// </summary>
        /// <value>
        /// The address_ line3.
        /// </value>
        public string Address_Line3
        {
            get { return M_Address_Line3; }
            set { M_Address_Line3 = value; }
        }

        /// <summary>
        /// Gets or sets the city.
        /// </summary>
        /// <value>
        /// The city.
        /// </value>
        public string City
        {
            get { return M_City; }
            set { M_City = value; }
        }

        /// <summary>
        /// Gets or sets the comp reg no.
        /// </summary>
        /// <value>
        /// The comp reg no.
        /// </value>
        public string CompRegNo
        {
            get { return M_CompRegNo; }
            set { M_CompRegNo = value; }
        }

        //Credit Manager
        /// <summary>
        /// Gets or sets the credit MGR identifier.
        /// </summary>
        /// <value>
        /// The credit MGR identifier.
        /// </value>
        public string CreditMgrID
        {
            get { return M_CreditMgrID; }
            set { M_CreditMgrID = value; }
        }

        /// <summary>
        /// Gets or sets the name of the credit MGR.
        /// </summary>
        /// <value>
        /// The name of the credit MGR.
        /// </value>
        public string CreditMgrName
        {
            get { return M_CreditMgrName; }
            set { M_CreditMgrName = value; }
        }

        /// <summary>
        /// Gets or sets the credit MGRPK.
        /// </summary>
        /// <value>
        /// The credit MGRPK.
        /// </value>
        public Int64 CreditMgrpk
        {
            get { return M_CreditMgrPk; }
            set { M_CreditMgrPk = value; }
        }

        /// <summary>
        /// Gets or sets the GST no.
        /// </summary>
        /// <value>
        /// The GST no.
        /// </value>
        public string GSTNo
        {
            get { return M_GSTNo; }
            set { M_GSTNo = value; }
        }

        /// <summary>
        /// Gets or sets the type of the business.
        /// </summary>
        /// <value>
        /// The type of the business.
        /// </value>
        public int BusinessType
        {
            get { return M_BusinessType; }
            set { M_BusinessType = value; }
        }

        /// <summary>
        /// Gets or sets the base currency.
        /// </summary>
        /// <value>
        /// The base currency.
        /// </value>
        public int BaseCurrency
        {
            get { return M_BaseCurrency; }
            set { M_BaseCurrency = value; }
        }

        /// <summary>
        /// Gets or sets the active flag.
        /// </summary>
        /// <value>
        /// The active flag.
        /// </value>
        public int ActiveFlag
        {
            get { return M_ActiveFlag; }
            set { M_ActiveFlag = value; }
        }

        /// <summary>
        /// Gets or sets the type of the biz.
        /// </summary>
        /// <value>
        /// The type of the biz.
        /// </value>
        public int BizType
        {
            get { return M_BizType; }
            set { M_BizType = value; }
        }

        /// <summary>
        /// Gets or sets the check ex rate.
        /// </summary>
        /// <value>
        /// The check ex rate.
        /// </value>
        public int CheckExRate
        {
            get { return M_CheckExRate; }
            set { M_CheckExRate = value; }
        }

        //Private M_ExRate As Integer
        //Private M_FmcNo As String
        //Private M_IataCode As String
        //Private M_AccNo As String
        /// <summary>
        /// Gets or sets the ex rate.
        /// </summary>
        /// <value>
        /// The ex rate.
        /// </value>
        public int ExRate
        {
            get { return M_ExRate; }
            set { M_ExRate = value; }
        }

        /// <summary>
        /// Gets or sets the FMC no.
        /// </summary>
        /// <value>
        /// The FMC no.
        /// </value>
        public string FmcNo
        {
            get { return M_FmcNo; }
            set { M_FmcNo = value; }
        }

        /// <summary>
        /// Gets or sets the iata code.
        /// </summary>
        /// <value>
        /// The iata code.
        /// </value>
        public string IataCode
        {
            get { return M_IataCode; }
            set { M_IataCode = value; }
        }

        /// <summary>
        /// Gets or sets the acc no.
        /// </summary>
        /// <value>
        /// The acc no.
        /// </value>
        public string AccNo
        {
            get { return M_AccNo; }
            set { M_AccNo = value; }
        }

        /// <summary>
        /// Gets or sets the vat percent.
        /// </summary>
        /// <value>
        /// The vat percent.
        /// </value>
        public double VATPercent
        {
            get { return M_VATPercent; }
            set { M_VATPercent = value; }
        }

        #endregion "List of Properties"

        #region "Fetch Function"

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="CorporatePK">The corporate pk.</param>
        /// <param name="CorporateID">The corporate identifier.</param>
        /// <param name="CorporateName">Name of the corporate.</param>
        /// <returns></returns>
        public bool FetchAll(Int64 CorporatePK = 0, string CorporateID = "", string CorporateName = "")
        {
            bool functionReturnValue = false;
            string strSQL = null;
            functionReturnValue = false;
            strSQL = string.Empty;
            strSQL += "SELECT  ";
            strSQL += "ROWNUM SR_NO, ";
            strSQL += "CORPORATE_MST_PK, ";
            strSQL += "CORPORATE_ID, ";
            strSQL += "CORPORATE_NAME, ";
            strSQL += "ADDRESS_LINE1, ";
            strSQL += "ADDRESS_LINE2, ";
            strSQL += "ADDRESS_LINE3, ";
            strSQL += "CORPORATE.COUNTRY_MST_FK, ";
            strSQL += "COUNTRY.COUNTRY_ID, ";
            strSQL += "COUNTRY.COUNTRY_NAME, ";
            strSQL += "CORPORATE.STATE_MST_FK, ";
            strSQL += "STATE.STATE_ID, ";
            strSQL += "STATE.STATE_NAME, ";
            strSQL += "CORPORATE.CITY, ";
            strSQL += "POST_CODE, ";
            strSQL += "HOME_PAGE, ";
            strSQL += "EMAIL, ";
            strSQL += "EMPLOYEE.EMPLOYEE_ID, ";
            strSQL += "EMPLOYEE.EMPLOYEE_NAME, ";
            strSQL += "PHONE, ";
            strSQL += "FAX, ";
            strSQL += "FMC_NO, ";
            strSQL += "ACCOUNT_NO, ";
            //strSQL &= "IATA_CODE, " & vbCrLf
            strSQL += "CORPORATE.EXCH_RATE_BASIS, ";
            strSQL += "CORPORATE.COMPANY_REG_NO, ";
            strSQL += "CORPORATE.GST_NO, ";
            strSQL += "CORPORATE.VERSION_NO,  ";
            //strSQL &= "CORPORATE.CURRENCY_MST_FK,  " & vbCrLf
            strSQL += "CORPORATE.ACTIVE_FLAG,  ";
            strSQL += "CORPORATE.CREDIT_MGR_HQ ,CORPORATE.CHK_EXRATE, ";
            strSQL += "CORPORATE.Biz_Type ";
            //strSQL &= "CORPORATE.VAT_Percentage  " & vbCrLf
            strSQL += "FROM CORPORATE_MST_TBL CORPORATE, ";
            strSQL += "COUNTRY_MST_TBL COUNTRY, ";
            strSQL += "EMPLOYEE_MST_TBL EMPLOYEE, ";
            strSQL += "STATE_MST_TBL STATE  ";
            strSQL += "WHERE 1=1 ";
            strSQL += "AND CORPORATE.COUNTRY_MST_FK=COUNTRY.COUNTRY_MST_PK(+) ";
            strSQL += "AND CORPORATE.CREDIT_MGR_HQ=EMPLOYEE.EMPLOYEE_MST_PK(+) ";
            strSQL += "AND CORPORATE.STATE_MST_FK=STATE.STATE_MST_PK(+) ";
            strSQL += " ";

            WorkFlow objWF = new WorkFlow();
            DataSet objDs = null;
            try
            {
                objDs = objWF.GetDataSet(strSQL);
                if (objDs.Tables[0].Rows.Count > 0)
                {
                    functionReturnValue = true;
                    Corporate_Id = Convert.ToString(objDs.Tables[0].Rows[0]["Corporate_ID"]);
                    Corporate_Mst_Pk = (Int64)objDs.Tables[0].Rows[0]["Corporate_MST_PK"];
                    Corporate_Name = Convert.ToString(objDs.Tables[0].Rows[0]["Corporate_NAME"]);
                    if (!string.IsNullOrEmpty(objDs.Tables[0].Rows[0]["Address_Line1"].ToString()))
                    {
                        Address_Line1 = Convert.ToString(objDs.Tables[0].Rows[0]["Address_Line1"]);
                    }
                    if (!string.IsNullOrEmpty(objDs.Tables[0].Rows[0]["Address_Line2"].ToString()))
                    {
                        Address_Line2 = "" + Convert.ToString(objDs.Tables[0].Rows[0]["Address_Line2"]);
                    }
                    if (!string.IsNullOrEmpty(objDs.Tables[0].Rows[0]["Address_Line3"].ToString()))
                    {
                        Address_Line3 = Convert.ToString(objDs.Tables[0].Rows[0]["Address_Line3"]);
                    }
                    ///'''''''''''''''''''
                    ExRate = Convert.ToInt32(objDs.Tables[0].Rows[0]["EXCH_RATE_BASIS"]);
                    if (!string.IsNullOrEmpty(objDs.Tables[0].Rows[0]["FMC_NO"].ToString()))
                    {
                        FmcNo = "" + Convert.ToString(objDs.Tables[0].Rows[0]["FMC_NO"]);
                    }
                    //If Not IsDBNull(objDs.Tables(0).Rows(0).Item("IATA_CODE")) Then
                    //    IataCode = "" & CType(objDs.Tables(0).Rows(0).Item("IATA_CODE"), String)
                    //End If
                    if (!string.IsNullOrEmpty(objDs.Tables[0].Rows[0]["ACCOUNT_NO"].ToString()))
                    {
                        AccNo = "" + Convert.ToString(objDs.Tables[0].Rows[0]["ACCOUNT_NO"]);
                    }
                    ///'''''''''''''''''''

                    if (!string.IsNullOrEmpty(objDs.Tables[0].Rows[0]["State_Mst_FK"].ToString()))
                    {
                        State_Mst_Fk = (Int64)objDs.Tables[0].Rows[0]["State_Mst_Fk"];
                    }

                    if (!string.IsNullOrEmpty(objDs.Tables[0].Rows[0]["CITY"].ToString()))
                    {
                        M_City = Convert.ToString(objDs.Tables[0].Rows[0]["CITY"]);
                    }

                    M_Country_Name = Convert.ToString(objDs.Tables[0].Rows[0]["Country_Name"]);
                    M_Country_ID = Convert.ToString(objDs.Tables[0].Rows[0]["Country_ID"]);
                    M_Country_Mst_Fk = (Int32)objDs.Tables[0].Rows[0]["Country_Mst_Fk"];
                    //Credit Manager
                    //M_CreditMgrPk = CType(objDs.Tables(0).Rows(0).Item("CREDIT_MGR_HQ"), Int32)
                    //M_CreditMgrID = CType(objDs.Tables(0).Rows(0).Item("EMPLOYEE_ID"), String)
                    //M_CreditMgrName = CType(objDs.Tables(0).Rows(0).Item("EMPLOYEE_NAME"), String)
                    //Credit Manager
                    if (!string.IsNullOrEmpty(objDs.Tables[0].Rows[0]["State_Name"].ToString()))
                    {
                        M_State_Name = Convert.ToString(objDs.Tables[0].Rows[0]["State_Name"]);
                    }
                    if (!string.IsNullOrEmpty(objDs.Tables[0].Rows[0]["State_Id"].ToString()))
                    {
                        M_State_Id = Convert.ToString(objDs.Tables[0].Rows[0]["State_Id"]);
                    }
                    //If Not IsDBNull(objDs.Tables(0).Rows(0).Item("VAT_Percentage")) Then
                    //    M_VATPercent = CType(objDs.Tables(0).Rows(0).Item("VAT_Percentage"), Double)
                    //Else
                    //    M_VATPercent = 0
                    //End If
                    //Credit Manager
                    if (!string.IsNullOrEmpty(objDs.Tables[0].Rows[0]["CREDIT_MGR_HQ"].ToString()))
                    {
                        M_CreditMgrPk = (Int32)objDs.Tables[0].Rows[0]["CREDIT_MGR_HQ"];
                    }
                    else
                    {
                        M_CreditMgrPk = 0;
                    }
                    if (!string.IsNullOrEmpty(objDs.Tables[0].Rows[0]["EMPLOYEE_ID"].ToString()))
                    {
                        M_CreditMgrID = Convert.ToString(objDs.Tables[0].Rows[0]["EMPLOYEE_ID"]);
                    }
                    if (!string.IsNullOrEmpty(objDs.Tables[0].Rows[0]["EMPLOYEE_NAME"].ToString()))
                    {
                        M_CreditMgrName = Convert.ToString(objDs.Tables[0].Rows[0]["EMPLOYEE_NAME"]);
                    }
                    //Credit Manager
                    if (!string.IsNullOrEmpty(objDs.Tables[0].Rows[0]["POST_CODE"].ToString()))
                    {
                        POST_CODE = Convert.ToString(objDs.Tables[0].Rows[0]["POST_CODE"]);
                    }
                    if (!string.IsNullOrEmpty(objDs.Tables[0].Rows[0]["HOME_PAGE"].ToString()))
                    {
                        Home_Page = Convert.ToString(objDs.Tables[0].Rows[0]["HOME_PAGE"]);
                    }
                    if (!string.IsNullOrEmpty(objDs.Tables[0].Rows[0]["EMAIL"].ToString()))
                    {
                        Email = Convert.ToString(objDs.Tables[0].Rows[0]["EMAIL"]);
                    }
                    if (!string.IsNullOrEmpty(objDs.Tables[0].Rows[0]["PHONE"].ToString()))
                    {
                        Phone = Convert.ToString(objDs.Tables[0].Rows[0]["PHONE"]);
                    }

                    if (!string.IsNullOrEmpty(objDs.Tables[0].Rows[0]["FAX"].ToString()))
                    {
                        Fax = Convert.ToString(objDs.Tables[0].Rows[0]["FAX"]);
                    }

                    if (!string.IsNullOrEmpty(objDs.Tables[0].Rows[0]["COMPANY_REG_NO"].ToString()))
                    {
                        CompRegNo = Convert.ToString(objDs.Tables[0].Rows[0]["COMPANY_REG_NO"]);
                    }

                    if (!string.IsNullOrEmpty(objDs.Tables[0].Rows[0]["GST_NO"].ToString()))
                    {
                        GSTNo = Convert.ToString(objDs.Tables[0].Rows[0]["GST_NO"]);
                    }

                    //If Not IsDBNull(objDs.Tables(0).Rows(0).Item("CURRENCY_MST_FK")) Then
                    //    BaseCurrency = CType(objDs.Tables(0).Rows(0).Item("CURRENCY_MST_FK"), Integer)
                    //End If

                    if (!string.IsNullOrEmpty(objDs.Tables[0].Rows[0]["VERSION_NO"].ToString()))
                    {
                        Version_No = (Int64)objDs.Tables[0].Rows[0]["VERSION_NO"];
                    }

                    if (!string.IsNullOrEmpty(objDs.Tables[0].Rows[0]["ACTIVE_FLAG"].ToString()))
                    {
                        ActiveFlag = Convert.ToInt32(objDs.Tables[0].Rows[0]["ACTIVE_FLAG"]);
                    }
                    if (!string.IsNullOrEmpty(objDs.Tables[0].Rows[0]["CHK_EXRATE"].ToString()))
                    {
                        M_CheckExRate = Convert.ToInt32(objDs.Tables[0].Rows[0]["CHK_EXRATE"]);
                    }
                    if (!string.IsNullOrEmpty(objDs.Tables[0].Rows[0]["Biz_Type"].ToString()))
                    {
                        M_BizType = Convert.ToInt32(objDs.Tables[0].Rows[0]["Biz_Type"]);
                    }
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
            return functionReturnValue;
        }

        /// <summary>
        /// Fetches the organogram.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchOrganogram()
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            ///Modified and Added By Koteshwari for active locations on 15/3/2011
            //strSQL = " Select * From Organogram_VW"
            strSQL = " Select o.SR_NO,o.location_id,o.location_mst_pk,o.reporting_to_fk,o.location_type_fk";
            strSQL += "  From Organogram_VW o,";
            strSQL += " location_mst_tbl l";
            strSQL += " where(o.location_mst_pk = l.location_mst_pk)";
            strSQL += " and l.active_flag = 1 ORDER BY O.SR_NO";
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

        /// <summary>
        /// Fetches the department details.
        /// </summary>
        /// <param name="P_Location_PK">The p_ location_ pk.</param>
        /// <returns></returns>
        public DataSet FetchDepartmentDetails(Int64 P_Location_PK)
        {
            string strSQL = null;
            DataSet MainDS = new DataSet();
            WorkFlow objWk = new WorkFlow();
            OracleDataAdapter DA = new OracleDataAdapter();

            strSQL = " Select DISTINCT Dept.Department_Mst_Pk, Dept.Department_Id, ";
            strSQL += " Dept.Department_Name, ";
            strSQL += " Loc.Location_Id ";
            strSQL += " FRom ";
            strSQL += " Location_Departments_Trn LD, ";
            strSQL += " Department_Mst_Tbl Dept, ";
            strSQL += " Location_Mst_Tbl Loc";
            strSQL += " Where ";
            strSQL += " LD.DEPARTMENT_MST_FK = Dept.Department_Mst_Pk And ";
            strSQL += " LD.LOCATION_MST_FK = Loc.Location_Mst_Pk And ";
            strSQL += " LD.Location_Mst_Fk = " + P_Location_PK;
            strSQL += " order by Dept.Department_Id";

            try
            {
                DA = objWk.GetDataAdapter(strSQL.Trim());
                DA.Fill(MainDS, "Department");

                strSQL = " Select ";
                strSQL += "     Emp.Employee_Id, ";
                strSQL += "     Emp.Employee_Name, ";
                //*********jmr**************
                strSQL += "     Desg.Designation_Name,";
                strSQL += "     Emp.EMail_ID, ";
                //strSQL &= vbCrLf & "     Emp.MOBILE_NO, "
                strSQL += "     Emp.PHONE_NO, ";
                //**************************
                strSQL += "     Emp.Department_Mst_Fk ";
                strSQL += " From ";
                strSQL += "     Employee_Mst_Tbl Emp, ";
                strSQL += "     Designation_Mst_Tbl Desg, ";
                strSQL += "     Location_Departments_Trn LDT ";
                strSQL += " Where ";
                strSQL += "     Emp.Designation_Mst_Fk = Desg.Designation_Mst_Pk ";
                strSQL += "     And Ldt.Location_Mst_Fk = Emp.Location_Mst_Fk ";
                strSQL += "     And Ldt.Department_Mst_Fk = emp.department_mst_fk ";
                strSQL += "     And Emp.Location_Mst_Fk = " + P_Location_PK;
                strSQL += "     order by Emp.Employee_Id";

                DA = objWk.GetDataAdapter(strSQL);
                DA.Fill(MainDS, "Employee");
                DataRelation relEmpLocation = new DataRelation("Department", new DataColumn[] { MainDS.Tables["Department"].Columns["Department_Mst_Pk"] }, new DataColumn[] { MainDS.Tables["Employee"].Columns["Department_Mst_Fk"] });

                MainDS.Relations.Add(relEmpLocation);
                return MainDS;
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

        /// <summary>
        /// Fetches the workin ports.
        /// </summary>
        /// <param name="P_Location_PK">The p_ location_ pk.</param>
        /// <returns></returns>
        public DataSet FetchWorkinPorts(Int64 P_Location_PK)
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;

            strSQL = "SELECT LW.BRANCH_WORKING_PORT ACTIVE,";
            strSQL += "P.PORT_ID,";
            strSQL += "P.PORT_NAME ";
            strSQL += "FROM LOC_PORT_MAPPING_TRN LW, ";
            strSQL += "PORT_MST_TBL P ";
            strSQL += "WHERE (LW.PORT_MST_FK = P.PORT_MST_PK) ";
            strSQL += "AND LW.LOCATION_MST_FK=" + P_Location_PK;
            strSQL += "ORDER BY P.PORT_ID";

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

        #endregion "Fetch Function"

        #region "Insert Function"

        /// <summary>
        /// Inserts the specified from dt.
        /// </summary>
        /// <param name="FromDt">From dt.</param>
        /// <param name="Todt">The todt.</param>
        /// <param name="Exrate">The exrate.</param>
        /// <param name="FmcNo">The FMC no.</param>
        /// <param name="AccNo">The acc no.</param>
        /// <param name="IataCode">The iata code.</param>
        /// <param name="VAT">The vat.</param>
        /// <param name="CurrFks">The curr FKS.</param>
        /// <param name="Automatic">The automatic.</param>
        /// <param name="SchdType">Type of the SCHD.</param>
        /// <param name="Server">The server.</param>
        /// <param name="Other">The other.</param>
        /// <param name="StartDay">The start day.</param>
        /// <param name="StartTime">The start time.</param>
        /// <param name="NextShd">The next SHD.</param>
        /// <param name="ExShdSetUpFK">The ex SHD set up fk.</param>
        /// <returns></returns>
        public int Insert(System.DateTime FromDt, System.DateTime Todt, int Exrate = 1, string FmcNo = "", string AccNo = "", string IataCode = "", double VAT = 0, string CurrFks = "", int Automatic = 0, int SchdType = 0,
        int Server = 0, string Other = "", int StartDay = 0, string StartTime = "", string NextShd = "", long ExShdSetUpFK = 0)
        {
            try
            {
                WorkFlow objWS = new WorkFlow();
                System.DBNull MyNull = null;
                Int32 intPkVal = default(Int32);
                objWS.MyCommand.CommandType = CommandType.StoredProcedure;
                var _with1 = objWS.MyCommand.Parameters;
                _with1.Add("Corporate_Id_IN", M_Corporate_Id).Direction = ParameterDirection.Input;
                _with1.Add("Corporate_Name_IN", M_Corporate_Name).Direction = ParameterDirection.Input;
                _with1.Add("Address_Line1_IN", (M_Address_Line1.Trim().Length > 0 ? M_Address_Line1 : " ")).Direction = ParameterDirection.Input;
                _with1.Add("Address_Line2_IN", (M_Address_Line2.Trim().Length > 0 ? M_Address_Line2 : " ")).Direction = ParameterDirection.Input;
                _with1.Add("Address_Line3_IN", (M_Address_Line3.Trim().Length > 0 ? M_Address_Line3 : " ")).Direction = ParameterDirection.Input;
                _with1.Add("Country_Mst_Fk_In", M_Country_Mst_Fk).Direction = ParameterDirection.Input;
                _with1.Add("State_Mst_Fk_IN", (M_State_Mst_Fk >= 0 ? M_State_Mst_Fk : 0)).Direction = ParameterDirection.Input;
                _with1.Add("City_IN", (M_City.Trim().Length > 0 ? M_City : " ")).Direction = ParameterDirection.Input;
                _with1.Add("Post_Code_IN", (M_POST_CODE.Trim().Length > 0 ? M_POST_CODE : " ")).Direction = ParameterDirection.Input;
                _with1.Add("Home_Page_IN", (M_HOME_PAGE.Trim().Length > 0 ? M_HOME_PAGE : " ")).Direction = ParameterDirection.Input;
                _with1.Add("Email_IN", (M_EMAIL.Trim().Length > 0 ? M_EMAIL : " ")).Direction = ParameterDirection.Input;
                _with1.Add("Phone_IN", (M_PHONE.Trim().Length > 0 ? M_PHONE : " ")).Direction = ParameterDirection.Input;
                _with1.Add("Fax_IN", (M_FAX.Trim().Length > 0 ? M_FAX : " ")).Direction = ParameterDirection.Input;
                _with1.Add("COMP_REG_NO_IN", (M_CompRegNo.Trim().Length > 0 ? M_CompRegNo : " ")).Direction = ParameterDirection.Input;
                _with1.Add("GST_NO_IN", (M_GSTNo.Trim().Length > 0 ? M_GSTNo : " ")).Direction = ParameterDirection.Input;
                _with1.Add("Created_By_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                _with1.Add("CONFIG_MST_FK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                //commented by thiyagarajan on 15/11/08 for location base currency task
                //.Add("CURRENCY_MST_FK_IN", M_BaseCurrency).Direction = ParameterDirection.Input

                _with1.Add("EXCH_RATE_BASIS_IN", Exrate).Direction = ParameterDirection.Input;
                _with1.Add("FMC_NO_IN", (FmcNo.Trim().Length > 0 ? FmcNo : "")).Direction = ParameterDirection.Input;
                //.Add("ACCOUNT_NO_IN", IIf(AccNo.Trim.Length > 0, AccNo, 0)).Direction = ParameterDirection.Input
                _with1.Add("ACCOUNT_NO_IN", DBNull.Value).Direction = ParameterDirection.Input;
                //.Add("IATA_CODE_IN", IIf(IataCode.Trim.Length > 0, IataCode, 0)).Direction = ParameterDirection.Input
                //.Add("VAT_PERCENTAGE_IN", VAT).Direction = ParameterDirection.Input
                //end
                _with1.Add("CHK_EXRATE_IN", M_CheckExRate).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", intPkVal).Direction = ParameterDirection.Output;
                _with1.Add("ACTIVE_FLAG_IN", ActiveFlag).Direction = ParameterDirection.Input;
                objWS.MyCommand.CommandText = objWS.MyUserName + ".CORPORATE_MST_TBL_PKG.CORPORATE_MST_TBL_INS";
                if (objWS.ExecuteCommands() == true)
                {
                    InsertService(FromDt, Todt, CurrFks, Automatic, SchdType, Server, Other, StartDay, StartTime, NextShd,
                    ExShdSetUpFK);
                    return intPkVal;
                }
                else
                {
                    return -1;
                }
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception e)
            {
                throw e;
            }
        }

        #endregion "Insert Function"

        #region "Update Function"

        /// <summary>
        /// Updates the specified from dt.
        /// </summary>
        /// <param name="FromDt">From dt.</param>
        /// <param name="Todt">The todt.</param>
        /// <param name="Exrate">The exrate.</param>
        /// <param name="FmcNo">The FMC no.</param>
        /// <param name="AccNo">The acc no.</param>
        /// <param name="IataCode">The iata code.</param>
        /// <param name="VAT">The vat.</param>
        /// <param name="CurrFks">The curr FKS.</param>
        /// <param name="Automatic">The automatic.</param>
        /// <param name="SchdType">Type of the SCHD.</param>
        /// <param name="Server">The server.</param>
        /// <param name="Other">The other.</param>
        /// <param name="StartDay">The start day.</param>
        /// <param name="StartTime">The start time.</param>
        /// <param name="NextShd">The next SHD.</param>
        /// <param name="ExShdSetUpFK">The ex SHD set up fk.</param>
        /// <returns></returns>
        public ArrayList Update(System.DateTime FromDt, System.DateTime Todt, int Exrate = 1, string FmcNo = "", string AccNo = "", string IataCode = "", double VAT = 0, string CurrFks = "", int Automatic = 0, int SchdType = 0,
        int Server = 0, string Other = "", int StartDay = 0, string StartTime = "", string NextShd = "", long ExShdSetUpFK = 0)
        {
            try
            {
                WorkFlow objWS = new WorkFlow();
                string intPkVal = "";
                System.DBNull MyNull = null;
                objWS.MyCommand.CommandType = CommandType.StoredProcedure;
                var _with2 = objWS.MyCommand.Parameters;
                _with2.Clear();
                _with2.Add("CORPORATE_MST_PK_IN", M_Corporate_Mst_Pk).Direction = ParameterDirection.Input;
                //ok

                _with2.Add("CORPORATE_ID_IN", M_Corporate_Id).Direction = ParameterDirection.Input;
                //ok

                _with2.Add("CORPORATE_NAME_IN", M_Corporate_Name).Direction = ParameterDirection.Input;
                //ok

                _with2.Add("ADDRESS_LINE1_IN", (M_Address_Line1.Trim().Length > 0 ? M_Address_Line1 : "")).Direction = ParameterDirection.Input;
                //ok
                _with2.Add("ADDRESS_LINE2_IN", (M_Address_Line2.Trim().Length > 0 ? M_Address_Line2 : "")).Direction = ParameterDirection.Input;
                //ok
                _with2.Add("ADDRESS_LINE3_IN", (M_Address_Line3.Trim().Length > 0 ? M_Address_Line3 : "")).Direction = ParameterDirection.Input;
                //ok

                _with2.Add("CITY_IN", (M_City.Trim().Length > 0 ? M_City.Trim() : "")).Direction = ParameterDirection.Input;
                //ok
                _with2.Add("COUNTRY_MST_FK_IN", M_Country_Mst_Fk).Direction = ParameterDirection.Input;
                //ok

                _with2.Add("HOME_PAGE_IN", (M_HOME_PAGE.Trim().Length > 0 ? M_HOME_PAGE : "")).Direction = ParameterDirection.Input;

                _with2.Add("EMAIL_IN", (M_EMAIL.Trim().Length > 0 ? M_EMAIL : "")).Direction = ParameterDirection.Input;
                _with2.Add("PHONE_IN", (M_PHONE.Trim().Length > 0 ? M_PHONE : "")).Direction = ParameterDirection.Input;
                _with2.Add("FAX_IN", (M_FAX.Trim().Length > 0 ? M_FAX : "")).Direction = ParameterDirection.Input;

                _with2.Add("COMP_REG_NO_IN", (M_CompRegNo.Trim().Length > 0 ? M_CompRegNo : "")).Direction = ParameterDirection.Input;
                _with2.Add("GST_NO_IN", (M_GSTNo.Trim().Length > 0 ? M_GSTNo : "")).Direction = ParameterDirection.Input;

                //commented by thiyagarajan on 15/11/08 for location base currency task
                // .Add("CURRENCY_MST_FK_IN", M_BaseCurrency).Direction = ParameterDirection.Input

                _with2.Add("STATE_MST_FK_IN", (M_State_Mst_Fk > 0 ? M_State_Mst_Fk : 0)).Direction = ParameterDirection.Input;

                _with2.Add("POST_CODE_IN", (M_POST_CODE.Trim().Length > 0 ? M_POST_CODE : "")).Direction = ParameterDirection.Input;
                //ExRateBasis, FmcNo, AccNo, IataCode
                _with2.Add("EXCH_RATE_BASIS_IN", Exrate).Direction = ParameterDirection.Input;
                _with2.Add("FMC_NO_IN", (FmcNo.Trim().Length > 0 ? FmcNo : "")).Direction = ParameterDirection.Input;
                //.Add("ACCOUNT_NO_IN", IIf(AccNo.Trim.Length > 0, AccNo, 0)).Direction = ParameterDirection.Input
                _with2.Add("ACCOUNT_NO_IN", DBNull.Value).Direction = ParameterDirection.Input;
                // .Add("IATA_CODE_IN", IIf(IataCode.Trim.Length > 0, IataCode, 0)).Direction = ParameterDirection.Input
                //Credit Manager
                _with2.Add("CREDIT_MGR_HQ_IN", (CreditMgrpk > 0 ? M_CreditMgrPk : 0)).Direction = ParameterDirection.Input;

                _with2.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                _with2.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

                _with2.Add("VERSION_NO_IN", M_VERSION_NO).Direction = ParameterDirection.Input;
                _with2.Add("ACTIVE_FLAG_IN", ActiveFlag).Direction = ParameterDirection.Input;
                //.Add("VAT_PERCENTAGE_IN", VAT).Direction = ParameterDirection.Input
                //end
                _with2.Add("CHK_EXRATE_IN", M_CheckExRate).Direction = ParameterDirection.Input;
                _with2.Add("RETURN_VALUE", intPkVal).Direction = ParameterDirection.Output;
                objWS.MyCommand.Parameters["RETURN_VALUE"].OracleDbType = OracleDbType.Varchar2;
                objWS.MyCommand.Parameters["RETURN_VALUE"].Size = 50;

                objWS.MyCommand.CommandText = objWS.MyUserName + ".CORPORATE_MST_TBL_PKG.CORPORATE_MST_TBL_UPD";

                if (objWS.ExecuteCommands() == true)
                {
                    InsertService(FromDt, Todt, CurrFks, Automatic, SchdType, Server, Other, StartDay, StartTime, NextShd,
                    ExShdSetUpFK);
                    arrMessage.Add("All Data Saved Successfully");
                    return arrMessage;
                    //If IsDBNull(objWS.MyCommand.Parameters.Item("RETURN_VALUE").Value) Then
                    //    Return 1
                    //Else
                    //    intPkVal = CType(objWS.MyCommand.Parameters.Item("RETURN_VALUE").Value, String)
                    //    Return -1
                    //End If
                }
                else
                {
                    arrMessage.Add("All Data Saved Successfully");
                    return arrMessage;
                }
            }
            catch (OracleException oraexp)
            {
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            catch (Exception e)
            {
                throw e;
            }
        }

        #endregion "Update Function"

        #region "InsertService Set Up"

        /// <summary>
        /// Inserts the service.
        /// </summary>
        /// <param name="FromDt">From dt.</param>
        /// <param name="Todt">The todt.</param>
        /// <param name="CurrFks">The curr FKS.</param>
        /// <param name="Automatic">The automatic.</param>
        /// <param name="SchdType">Type of the SCHD.</param>
        /// <param name="Server">The server.</param>
        /// <param name="Other">The other.</param>
        /// <param name="StartDay">The start day.</param>
        /// <param name="StartTime">The start time.</param>
        /// <param name="NextShd">The next SHD.</param>
        /// <param name="ExShdSetUpFK">The ex SHD set up fk.</param>
        /// <returns></returns>
        public int InsertService(System.DateTime FromDt, System.DateTime Todt, string CurrFks = "", int Automatic = 0, int SchdType = 0, int Server = 0, string Other = "", int StartDay = 0, string StartTime = "", string NextShd = "",
        long ExShdSetUpFK = 0)
        {
            WorkFlow objWS = new WorkFlow();
            string intPkVal = "";
            System.DBNull MyNull = null;
            objWS.MyCommand.CommandType = CommandType.StoredProcedure;
            try
            {
                //'Insert
                if (ExShdSetUpFK == 0)
                {
                    var _with3 = objWS.MyCommand.Parameters;
                    _with3.Clear();
                    _with3.Add("EXCHANGE_RATE_UPDATE_IN", Automatic).Direction = ParameterDirection.Input;
                    _with3.Add("EXCHANGE_RATE_SERVER_IN", Server).Direction = ParameterDirection.Input;
                    _with3.Add("OTHER_DTL_IN", (string.IsNullOrEmpty(Other) ? "" : Other)).Direction = ParameterDirection.Input;
                    _with3.Add("SCHEDULER_SETUP_IN", SchdType).Direction = ParameterDirection.Input;
                    _with3.Add("START_DAY_IN", StartDay).Direction = ParameterDirection.Input;
                    _with3.Add("START_TIME_IN", StartTime).Direction = ParameterDirection.Input;
                    _with3.Add("NEXT_SCHEDULE_AT_IN", NextShd).Direction = ParameterDirection.Input;
                    _with3.Add("LAST_RUN_ON_IN", System.DateTime.Now).Direction = ParameterDirection.Input;
                    _with3.Add("BASE_CURRENCY_FKS_IN", CurrFks).Direction = ParameterDirection.Input;
                    _with3.Add("FROM_DT_IN", FromDt.Date).Direction = ParameterDirection.Input;
                    _with3.Add("TO_DT_IN", Todt.Date).Direction = ParameterDirection.Input;
                    _with3.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                    _with3.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
                    objWS.MyCommand.CommandText = objWS.MyUserName + ".EXCHANGE_SCHEDULER_SETUP_PKG.EX_SCHEDULER_SETUP_INS";
                }
                else
                {
                    var _with4 = objWS.MyCommand.Parameters;
                    //'Update
                    _with4.Clear();
                    _with4.Add("EX_SCHEDULER_SETUP_FK_IN", ExShdSetUpFK).Direction = ParameterDirection.Input;
                    _with4.Add("EXCHANGE_RATE_UPDATE_IN", Automatic).Direction = ParameterDirection.Input;
                    _with4.Add("EXCHANGE_RATE_SERVER_IN", Server).Direction = ParameterDirection.Input;
                    _with4.Add("OTHER_DTL_IN", (string.IsNullOrEmpty(Other) ? "" : Other)).Direction = ParameterDirection.Input;
                    _with4.Add("SCHEDULER_SETUP_IN", SchdType).Direction = ParameterDirection.Input;
                    _with4.Add("START_DAY_IN", StartDay).Direction = ParameterDirection.Input;
                    _with4.Add("START_TIME_IN", StartTime).Direction = ParameterDirection.Input;
                    _with4.Add("NEXT_SCHEDULE_AT_IN", NextShd).Direction = ParameterDirection.Input;
                    _with4.Add("LAST_RUN_ON_IN", System.DateTime.Now).Direction = ParameterDirection.Input;
                    _with4.Add("BASE_CURRENCY_FKS_IN", CurrFks).Direction = ParameterDirection.Input;
                    _with4.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                    _with4.Add("FROM_DT_IN", FromDt.Date).Direction = ParameterDirection.Input;
                    _with4.Add("TO_DT_IN", Todt.Date).Direction = ParameterDirection.Input;
                    _with4.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;
                    objWS.MyCommand.CommandText = objWS.MyUserName + ".EXCHANGE_SCHEDULER_SETUP_PKG.EX_SCHEDULER_SETUP_UPD";
                }
                if (objWS.ExecuteCommands() == true)
                {
                    return 1;
                }
                else
                {
                    return -1;
                }
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception e)
            {
                throw e;
            }
        }

        #endregion "InsertService Set Up"

        #region "Delete Function"

        /// <summary>
        /// Deletes this instance.
        /// </summary>
        /// <returns></returns>
        public int Delete()
        {
            try
            {
                WorkFlow objWS = new WorkFlow();
                Int32 intPkVal = default(Int32);
                objWS.MyCommand.CommandType = CommandType.StoredProcedure;
                var _with5 = objWS.MyCommand.Parameters;
                _with5.Add("Corporate_Mst_Pk_IN", M_Corporate_Mst_Pk).Direction = ParameterDirection.Input;
                _with5.Add("DELETED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                _with5.Add("VERSION_NO", M_VERSION_NO).Direction = ParameterDirection.Input;
                _with5.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                _with5.Add("RETURN_VALUE", intPkVal).Direction = ParameterDirection.Output;
                objWS.MyCommand.CommandText = objWS.MyUserName + ".CORPORATE_MST_TBL_PKG.CORPORATE_MST_TBL_DEL";
                if (objWS.ExecuteCommands() == true)
                {
                    return 1;
                }
                else
                {
                    return -1;
                }
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception e)
            {
                throw e;
            }
        }

        #endregion "Delete Function"

        #region "Fill Currency"

        //Added by soman, to populate the base currency dropdown.
        /// <summary>
        /// Fills the currency.
        /// </summary>
        /// <returns></returns>
        public DataSet FillCurrency()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();

            strSQL = "SELECT C.CURRENCY_MST_PK,C.CURRENCY_ID,C.CURRENCY_NAME FROM CURRENCY_TYPE_MST_TBL C  WHERE C.ACTIVE_FLAG =1 ORDER BY C.CURRENCY_NAME";

            DataSet objDS = null;
            try
            {
                objDS = objWF.GetDataSet(strSQL);
                return objDS;
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

        #endregion "Fill Currency"

        #region "Availability Of Removals and EBooking"

        /// <summary>
        /// es the booking available.
        /// </summary>
        /// <returns></returns>
        public Int32 EBookingAvailable()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            strSQL = "SELECT CORP.EBOOKING_AVAILABLE FROM CORPORATE_MST_TBL CORP";
            try
            {
                return Convert.ToInt32(objWF.ExecuteScaler(strSQL));
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

        /// <summary>
        /// Removalses the available.
        /// </summary>
        /// <returns></returns>
        public Int32 RemovalsAvailable()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            strSQL = "SELECT CORP.REMOVALS_AVAILABLE FROM CORPORATE_MST_TBL CORP";
            try
            {
                return Convert.ToInt32(objWF.ExecuteScaler(strSQL));
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

        /// <summary>
        /// CHKs the ex rate available.
        /// </summary>
        /// <returns></returns>
        public Int32 ChkExRateAvailable()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            strSQL = "SELECT CORP.CHK_EXRATE FROM CORPORATE_MST_TBL CORP";
            try
            {
                return Convert.ToInt32(objWF.ExecuteScaler(strSQL));
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

        #endregion "Availability Of Removals and EBooking"

        #region "Exchange Rate Set Up"

        /// <summary>
        /// Fetches the currency.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchCurrency()
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                var _with6 = objWF.MyCommand.Parameters;
                _with6.Add("CURR_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("EXCHANGE_SCHEDULER_SETUP_PKG", "GET_CURRENCY");
                return DS;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        /// <summary>
        /// Fetches the setup DTLS.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchSetupDtls()
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                var _with7 = objWF.MyCommand.Parameters;
                _with7.Add("CURR_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("EXCHANGE_SCHEDULER_SETUP_PKG", "GET_SHD_SETUP_DTLS");
                return DS;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        /// <summary>
        /// Gets the ex scheduler set up.
        /// </summary>
        /// <returns></returns>
        public int GetExSchedulerSetUp()
        {
            WorkFlow objWF = new WorkFlow();
            string StrSql = null;
            StrSql = "SELECT E.EXCHANGE_RATE_UPDATE FROM EXCHANGE_SCHEDULER_SETUP_TBL E";
            try
            {
                return Convert.ToInt32(objWF.ExecuteScaler(StrSql));
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

        #endregion "Exchange Rate Set Up"
    }
}