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

using Oracle.DataAccess.Client;
using System;
using System.Collections;
using System.Data;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_VendorListing : CommonFeatures
    {
        #region "List of Members of the Class"

        /// <summary>
        /// The m_ vendor_ MST_ pk
        /// </summary>
        private long M_Vendor_Mst_Pk;
        /// <summary>
        /// The m_ vendor_ MST_ fk
        /// </summary>
        private long M_Vendor_Mst_Fk;
        /// <summary>
        /// The m_ vendor_ type_ fk
        /// </summary>
        private long M_Vendor_Type_Fk;
        /// <summary>
        /// The m_ vendor type name
        /// </summary>
        private string M_VendorTypeName;
        /// <summary>
        /// The m_ vendor_ identifier
        /// </summary>
        private string M_Vendor_Id;
        /// <summary>
        /// The m_ vendor_ name
        /// </summary>
        private string M_Vendor_Name;
        /// <summary>
        /// The m_ address1
        /// </summary>
        private string M_Address1;
        /// <summary>
        /// The m_ address2
        /// </summary>
        private string M_Address2;
        /// <summary>
        /// The m_ city
        /// </summary>
        private string M_City;
        /// <summary>
        /// The m_ phone1
        /// </summary>
        private string M_Phone1;
        /// <summary>
        /// The m_ phone2
        /// </summary>
        private string M_Phone2;
        /// <summary>
        /// The m_ fax
        /// </summary>
        private string M_Fax;
        /// <summary>
        /// The m_ email
        /// </summary>
        private string M_Email;
        /// <summary>
        /// The m_ URL
        /// </summary>
        private string M_Url;
        /// <summary>
        /// The m_ location_ MST_ fk
        /// </summary>
        private long M_Location_Mst_Fk;
        /// <summary>
        /// The m_ location name
        /// </summary>
        private string M_LocationName;
        /// <summary>
        /// The m_ zip
        /// </summary>
        private string M_Zip;
        /// <summary>
        /// The m_ data set
        /// </summary>
        private DataSet M_DataSet;

        #endregion "List of Members of the Class"

        /// <summary>
        /// The m_ contact_ data set
        /// </summary>
        private DataSet M_Contact_DataSet;

        #region "List of Properties"

        /// <summary>
        /// Gets or sets the vendor_ MST_ pk.
        /// </summary>
        /// <value>
        /// The vendor_ MST_ pk.
        /// </value>
        public long Vendor_Mst_Pk
        {
            get { return M_Vendor_Mst_Pk; }
            set { M_Vendor_Mst_Pk = value; }
        }

        /// <summary>
        /// Gets or sets the vendor_ MST_ fk.
        /// </summary>
        /// <value>
        /// The vendor_ MST_ fk.
        /// </value>
        public long Vendor_Mst_Fk
        {
            get { return M_Vendor_Mst_Fk; }
            set { M_Vendor_Mst_Fk = value; }
        }

        /// <summary>
        /// Gets or sets the vendor_ type_ fk.
        /// </summary>
        /// <value>
        /// The vendor_ type_ fk.
        /// </value>
        public long Vendor_Type_Fk
        {
            get { return M_Vendor_Type_Fk; }
            set { M_Vendor_Type_Fk = value; }
        }

        /// <summary>
        /// Gets or sets the vendor_ identifier.
        /// </summary>
        /// <value>
        /// The vendor_ identifier.
        /// </value>
        public string Vendor_Id
        {
            get { return M_Vendor_Id; }
            set { M_Vendor_Id = value; }
        }

        /// <summary>
        /// Gets or sets the name of the vendor_.
        /// </summary>
        /// <value>
        /// The name of the vendor_.
        /// </value>
        public string Vendor_Name
        {
            get { return M_Vendor_Name; }
            set { M_Vendor_Name = value; }
        }

        /// <summary>
        /// Gets or sets the address1.
        /// </summary>
        /// <value>
        /// The address1.
        /// </value>
        public string Address1
        {
            get { return M_Address1; }
            set { M_Address1 = value; }
        }

        /// <summary>
        /// Gets or sets the address2.
        /// </summary>
        /// <value>
        /// The address2.
        /// </value>
        public string Address2
        {
            get { return M_Address2; }
            set { M_Address2 = value; }
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
        /// Gets or sets the phone1.
        /// </summary>
        /// <value>
        /// The phone1.
        /// </value>
        public string Phone1
        {
            get { return M_Phone1; }
            set { M_Phone1 = value; }
        }

        /// <summary>
        /// Gets or sets the phone2.
        /// </summary>
        /// <value>
        /// The phone2.
        /// </value>
        public string Phone2
        {
            get { return M_Phone2; }
            set { M_Phone2 = value; }
        }

        /// <summary>
        /// Gets or sets the fax.
        /// </summary>
        /// <value>
        /// The fax.
        /// </value>
        public string Fax
        {
            get { return M_Fax; }
            set { M_Fax = value; }
        }

        /// <summary>
        /// Gets or sets the email.
        /// </summary>
        /// <value>
        /// The email.
        /// </value>
        public string Email
        {
            get { return M_Email; }
            set { M_Email = value; }
        }

        /// <summary>
        /// Gets or sets the URL.
        /// </summary>
        /// <value>
        /// The URL.
        /// </value>
        public string Url
        {
            get { return M_Url; }
            set { M_Url = value; }
        }

        /// <summary>
        /// Gets or sets the location_ MST_ fk.
        /// </summary>
        /// <value>
        /// The location_ MST_ fk.
        /// </value>
        public long Location_Mst_Fk
        {
            get { return M_Location_Mst_Fk; }
            set { M_Location_Mst_Fk = value; }
        }

        /// <summary>
        /// Gets or sets the zip.
        /// </summary>
        /// <value>
        /// The zip.
        /// </value>
        public string Zip
        {
            get { return M_Zip; }
            set { M_Zip = value; }
        }

        /// <summary>
        /// Gets or sets my data set.
        /// </summary>
        /// <value>
        /// My data set.
        /// </value>
        public DataSet MyDataSet
        {
            get { return M_DataSet; }
            set { M_DataSet = value; }
        }

        /// <summary>
        /// Gets or sets the contact data set.
        /// </summary>
        /// <value>
        /// The contact data set.
        /// </value>
        public DataSet ContactDataSet
        {
            get { return M_Contact_DataSet; }
            set { M_Contact_DataSet = value; }
        }

        /// <summary>
        /// Gets the name of the vendor type.
        /// </summary>
        /// <value>
        /// The name of the vendor type.
        /// </value>
        public string VendorTypeName
        {
            get { return M_VendorTypeName; }
        }

        /// <summary>
        /// Gets the name of the location.
        /// </summary>
        /// <value>
        /// The name of the location.
        /// </value>
        public string LocationName
        {
            get { return M_LocationName; }
        }

        #endregion "List of Properties"

        #region "Initalize dataset"

        /// <summary>
        /// Initalizes the ds.
        /// </summary>
        public void InitalizeDS()
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            strSQL = "SELECT A.Vendor_Mst_Pk,";
            strSQL += "A.Vendor_Mst_Fk,";
            strSQL += "A.Vendor_Type_Fk,";
            strSQL += "A.Vendor_Id,";
            strSQL += "A.Vendor_Name,";
            strSQL += "A.Address1,";
            strSQL += "A.Address2,";
            strSQL += "A.Address3,";
            strSQL += "A.City,";
            strSQL += "A.State_Mst_Fk,";
            strSQL += "A.Zip,";
            strSQL += "A.Phone1,";
            strSQL += "A.Phone2,";
            strSQL += "A.Fax,";
            strSQL += "A.Email,";
            strSQL += "A.Url,";
            strSQL += "A.Location_Mst_Fk,";
            strSQL += "A.Employee_Mst_Fk,";
            strSQL += "A.Created_By_Fk,";
            strSQL += "A.LAST_MODIFIED_BY_FK,";
            strSQL += "A.VERSION_NO";
            strSQL += " FROM Vendor_MST_TBL A WHERE 1=2";
            try
            {
                M_DataSet = objWF.GetDataSet(strSQL);
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 13/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Initalize dataset"

        #region "Constructors"

        /// <summary>
        /// Initializes a new instance of the <see cref="cls_VendorListing"/> class.
        /// </summary>
        public cls_VendorListing()
        {
        }

        #endregion "Constructors"

        #region "Enhance Search & Lookup Search Block"

        /// <summary>
        /// Fetches the vendor.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchVendor(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strLOC_MST_IN = null;
            string strSERACH_IN = "";
            string strVendorTypeIN = "1";
            string strArr5 = "";
            string strArr6 = "";
            string strArr7 = "";
            //Dim intBUSINESS_TYPE_IN As Integer = 3
            string strBUSINESS_TYPE_IN = null;
            int intLocationfk = 0;
            string strReq = null;
            string strNull = null;
            arr = strCond.Split('~');
            string Cond = "";
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                intLocationfk = Convert.ToInt32(arr.GetValue(2));
            if (arr.Length > 3)
                strBUSINESS_TYPE_IN = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                strVendorTypeIN = Convert.ToString(arr.GetValue(4));
            if (arr.Length > 5)
                strArr5 = Convert.ToString(arr.GetValue(5));
            if (arr.Length > 6)
                strArr6 = Convert.ToString(arr.GetValue(6));
            if (arr.Length > 7)
                strArr7 = Convert.ToString(arr.GetValue(7));

            try
            {
                //modified by thanga on 4/8/08
                if ((arr.Length > 5))
                {
                    if (strArr5 != "Payment" & strArr5 != "Voucher")
                    {
                        Cond = arr.Length.ToString();
                        objWF.OpenConnection();
                        selectCommand.Connection = objWF.MyConnection;
                        selectCommand.CommandType = CommandType.StoredProcedure;
                        selectCommand.CommandText = objWF.MyUserName + ".EN_VENDOR_PKG.GETVENDOR_COMMON_SUPTYPEDEF";
                        var _with1 = selectCommand.Parameters;
                        _with1.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                        _with1.Add("BUSINESS_TYPE_IN", strBUSINESS_TYPE_IN).Direction = ParameterDirection.Input;
                        _with1.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                        _with1.Add("VENDOR_TYPE_IN", strVendorTypeIN).Direction = ParameterDirection.Input;
                        _with1.Add("LOCATION_MST_FK_IN", intLocationfk).Direction = ParameterDirection.Input;
                        _with1.Add("Cond_IN", Cond).Direction = ParameterDirection.Input;
                        _with1.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                    }
                    else
                    {
                        objWF.OpenConnection();
                        selectCommand.Connection = objWF.MyConnection;
                        selectCommand.CommandType = CommandType.StoredProcedure;
                        selectCommand.CommandText = objWF.MyUserName + ".EN_VENDOR_PKG.GETVENDOR_COMMON";
                        var _with2 = selectCommand.Parameters;
                        _with2.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                        _with2.Add("BUSINESS_TYPE_IN", strBUSINESS_TYPE_IN).Direction = ParameterDirection.Input;
                        _with2.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                        _with2.Add("VENDOR_TYPE_IN", strVendorTypeIN).Direction = ParameterDirection.Input;
                        _with2.Add("LOCATION_MST_FK_IN", intLocationfk).Direction = ParameterDirection.Input;
                        _with2.Add("FLAG_IN", getDefault(strArr5, " ")).Direction = ParameterDirection.Input;
                        _with2.Add("PROCESS_IN", getDefault(strArr6, "3")).Direction = ParameterDirection.Input;
                        _with2.Add("JOBTYPE_IN", getDefault(strArr7, "0")).Direction = ParameterDirection.Input;
                        _with2.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                    }
                }
                else
                {
                    objWF.OpenConnection();
                    selectCommand.Connection = objWF.MyConnection;
                    selectCommand.CommandType = CommandType.StoredProcedure;
                    selectCommand.CommandText = objWF.MyUserName + ".EN_VENDOR_PKG.GETVENDOR_COMMON";
                    var _with3 = selectCommand.Parameters;
                    _with3.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                    _with3.Add("BUSINESS_TYPE_IN", strBUSINESS_TYPE_IN).Direction = ParameterDirection.Input;
                    _with3.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                    _with3.Add("VENDOR_TYPE_IN", strVendorTypeIN).Direction = ParameterDirection.Input;
                    _with3.Add("LOCATION_MST_FK_IN", intLocationfk).Direction = ParameterDirection.Input;
                    _with3.Add("FLAG_IN", getDefault(strArr5, " ")).Direction = ParameterDirection.Input;
                    _with3.Add("PROCESS_IN", getDefault(strArr6, "3")).Direction = ParameterDirection.Input;
                    _with3.Add("JOBTYPE_IN", getDefault(strArr7, "0")).Direction = ParameterDirection.Input;
                    _with3.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                }

                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 13/09/2011, PTS ID: SEP-01
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

        /// <summary>
        /// Fetches the rem vendor.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchRemVendor(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strLOC_MST_IN = null;
            string strSERACH_IN = "";
            string strVendorTypeIN = "1";
            string strBUSINESS_TYPE_IN = null;
            int intLocationfk = 0;
            string strReq = null;
            var strNull = DBNull.Value;
            arr = strCond.Split('~');
            string Cond = "";
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                intLocationfk = Convert.ToInt32(arr.GetValue(2));
            if (arr.Length > 3)
                strBUSINESS_TYPE_IN = Convert.ToString(arr.GetValue(3));
            if (arr.Length >= 4)
                strVendorTypeIN = Convert.ToString(arr.GetValue(4));

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_VENDOR_PKG.GETREMVENDOR_COMMON";
                var _with4 = selectCommand.Parameters;
                _with4.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with4.Add("BUSINESS_TYPE_IN", strBUSINESS_TYPE_IN).Direction = ParameterDirection.Input;
                _with4.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with4.Add("VENDOR_TYPE_IN", strVendorTypeIN).Direction = ParameterDirection.Input;
                _with4.Add("LOCATION_MST_FK_IN", intLocationfk).Direction = ParameterDirection.Input;
                _with4.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 13/09/2011, PTS ID: SEP-01
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

        /// <summary>
        /// Fetches the job vendors.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchJobVendors(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strLOC_MST_IN = null;
            string strSERACH_IN = "";
            string strVendorTypeIN = "1";
            int intBUSINESS_TYPE_IN = 3;
            int intLocationfk = 0;
            int intProcess = 0;
            string strReq = null;
            string strJobRefNr = null;
            var strNull = DBNull.Value;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                intLocationfk = Convert.ToInt32(arr.GetValue(2));
            if (arr.Length > 3)
                intBUSINESS_TYPE_IN = Convert.ToInt32(arr.GetValue(3));
            if (arr.Length > 4)
                strVendorTypeIN = Convert.ToString(arr.GetValue(4));
            if (arr.Length > 5)
                intProcess = Convert.ToInt32(arr.GetValue(5));
            if (arr.Length >= 6)
                strJobRefNr = Convert.ToString(arr.GetValue(6));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_VENDOR_PKG.GETJOBVENDOR_COMMON";
                var _with5 = selectCommand.Parameters;
                _with5.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with5.Add("BUSINESS_TYPE_IN", intBUSINESS_TYPE_IN).Direction = ParameterDirection.Input;
                _with5.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with5.Add("VENDOR_TYPE_IN", strVendorTypeIN).Direction = ParameterDirection.Input;
                _with5.Add("LOCATION_MST_FK_IN", intLocationfk).Direction = ParameterDirection.Input;
                _with5.Add("PROCESS_TYPE_IN", intProcess).Direction = ParameterDirection.Input;
                _with5.Add("JOB_REF_NR_IN", (!string.IsNullOrEmpty(strJobRefNr) ? strJobRefNr : "")).Direction = ParameterDirection.Input;
                _with5.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 13/09/2011, PTS ID: SEP-01
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

        /// <summary>
        /// Fetches the vendor category.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchVendorCategory(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strLOC_MST_IN = null;
            string strSERACH_IN = null;
            string strVendorCategory = null;
            string strReq = null;
            int iPortPk = 0;
            int iBUSINESS_MODEL_IN = 0;
            int iCorpPK = 0;
            var strNull = DBNull.Value;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strVendorCategory = Convert.ToString(arr.GetValue(2));
            iPortPk = Convert.ToInt32(arr.GetValue(3));
            iBUSINESS_MODEL_IN = Convert.ToInt32(arr.GetValue(4));
            strLOC_MST_IN = Convert.ToString(arr.GetValue(5));
            iCorpPK = Convert.ToInt32(arr.GetValue(6));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_Vendor_PKG.GETVendor_CATEGORY_COMMON";

                var _with6 = selectCommand.Parameters;
                _with6.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with6.Add("BUSINESS_MODEL_IN", iBUSINESS_MODEL_IN).Direction = ParameterDirection.Input;
                _with6.Add("CATEGORY_IN", strVendorCategory).Direction = ParameterDirection.Input;
                _with6.Add("CUST_CORP_PK", iCorpPK).Direction = ParameterDirection.Input;
                _with6.Add("PORT_MST_FK_IN", iPortPk).Direction = ParameterDirection.Input;
                _with6.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with6.Add("RETURN_VALUE", OracleDbType.Varchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 13/09/2011, PTS ID: SEP-01
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

        #endregion "Enhance Search & Lookup Search Block"

        #region "Function to check whether a user is an administrator or not"

        /// <summary>
        /// Determines whether the specified string user identifier is administrator.
        /// </summary>
        /// <param name="strUserID">The string user identifier.</param>
        /// <returns></returns>
        public int IsAdministrator(string strUserID)
        {
            string strSQL = null;
            Int16 Admin = default(Int16);
            WorkFlow objWF = new WorkFlow();
            strSQL = "SELECT COUNT(*) FROM User_Mst_Tbl U WHERE U.ROLE_MST_FK = ";
            strSQL = strSQL + "(SELECT R.ROLE_MST_TBL_PK FROM ROLE_MST_TBL R WHERE R.ROLE_ID = 'ADMIN')";
            strSQL = strSQL + "AND U.USER_MST_PK = " + HttpContext.Current.Session["USER_PK"];
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

        #endregion "Function to check whether a user is an administrator or not"

        #region "FetchListing"

        /// <summary>
        /// Fetches the listing.
        /// </summary>
        /// <param name="P_VendorId">The p_ vendor identifier.</param>
        /// <param name="P_VendorName">Name of the p_ vendor.</param>
        /// <param name="P_LocationId">The p_ location identifier.</param>
        /// <param name="CurrentBType">Type of the current b.</param>
        /// <param name="BType">Type of the b.</param>
        /// <param name="SupplierId">The supplier identifier.</param>
        /// <param name="SupplierType">Type of the supplier.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="strColumnName">Name of the string column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="blnSortAscending">if set to <c>true</c> [BLN sort ascending].</param>
        /// <param name="IsActive">The is active.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="locpk">The locpk.</param>
        /// <param name="IsAdmin">The is admin.</param>
        /// <param name="UserBizType">Type of the user biz.</param>
        /// <returns></returns>
        public DataSet FetchListing(string P_VendorId = "", string P_VendorName = "", string P_LocationId = "", int CurrentBType = 0, int BType = 0, string SupplierId = "", string SupplierType = "", string SearchType = "", string strColumnName = "", Int32 CurrentPage = 0,
       Int32 TotalPage = 0, bool blnSortAscending = false, Int16 IsActive = 0, Int32 flag = 0, string locpk = "", int IsAdmin = 0, int UserBizType = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            bool cond = false;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            System.Web.UI.Page objPage = new System.Web.UI.Page();
            //     ***************User Preference(Fetch On Load)*****************
            if (flag == 0)
            {
                strCondition += " AND 1=2";
            }
            //     ***************Search Vendor Id*****************
            if (P_VendorId.ToString().Trim().Length > 0 & SearchType == "C")
            {
                cond = true;
                strCondition += " And UPPER(VMT.VENDOR_ID) like '%" + P_VendorId.ToUpper().Replace("'", "''") + "%' ";
            }
            else if (P_VendorId.ToString().Trim().Length > 0 & SearchType == "S")
            {
                cond = true;
                strCondition += " And UPPER(VMT.VENDOR_ID) like '" + P_VendorId.ToUpper().Replace("'", "''") + "%' ";
            }

            //***********************Search Vendor Type ***************

            if (P_VendorName.ToString().Trim().Length > 0 & SearchType == "C")
            {
                cond = true;
                strCondition = strCondition + " And UPPER(VMT.VENDOR_NAME) like '%" + P_VendorName.ToUpper().Replace("'", "''") + "%' ";
            }
            else if (P_VendorName.ToString().Trim().Length > 0 & SearchType == "S")
            {
                cond = true;
                strCondition = strCondition + " And UPPER(VMT.VENDOR_NAME) like '" + P_VendorName.ToUpper().Replace("'", "''") + "%' ";
            }
            //     ***************Search Location Id*****************
            if (P_LocationId.Trim().Length > 0 & SearchType == "C")
            {
                strCondition = strCondition + " AND Upper(LMT.LOCATION_ID) LIKE '%" + P_LocationId.ToUpper().Replace("'", "''") + "%'";
            }
            else if (P_LocationId.Trim().Length > 0 & SearchType == "S")
            {
                strCondition = strCondition + " AND upper(LMT.LOCATION_ID) LIKE '" + P_LocationId.ToUpper().Replace("'", "''") + "%'";
            }
            //Added by Thangadurai to fetch location wise supplier
            if (P_LocationId.Trim().Length == 0 & SupplierId != "SHIPPINGLINE" & SupplierId != "AIRLINE")
            {
                //strCondition = strCondition & " AND lmt.location_mst_pk  =" & Session("LOGED_IN_LOC_FK")
                //strCondition &= vbCrLf & "  AND VMTDTL.ADM_LOCATION_MST_FK =" & Session("LOGED_IN_LOC_FK")
            }
            if (P_LocationId.Trim().Length > 0)
            {
                if ((HttpContext.Current.Session["loc_pk"] != null))
                {
                    strCondition += "  AND VMTDTL.ADM_LOCATION_MST_FK =" + HttpContext.Current.Session["loc_pk"];
                }
            }

            if (IsActive == 1)
            {
                strCondition = strCondition + " AND VMT.ACTIVE =1 ";
            }

            //     ***************Search Vendor of Sea or Air or Both Business*****************
            if (BType != 3)
            {
                strCondition += " and (VMT.BUSINESS_TYPE in (" + BType + ",3) )";
            }
            else if (UserBizType == 1)
            {
                strCondition += " and ( VMT.BUSINESS_TYPE in (1,3))";
            }
            else if (UserBizType == 2)
            {
                strCondition += " and ( VMT.BUSINESS_TYPE in (2,3))";
            }
            else
            {
                strCondition += " and ( VMT.BUSINESS_TYPE in (1,2,3))";
            }

            if (SupplierId != "ALL" & SupplierId != " ALL ")
            {
                strCondition += "   AND VTMST.VENDOR_TYPE_ID LIKE '" + SupplierId + "'";
            }
            else
            {
            }

            strCondition += " AND VSTRN.VENDOR_MST_FK = VMT.VENDOR_MST_PK  ";

            strCondition += "  AND VTMST.VENDOR_TYPE_PK = VSTRN.VENDOR_TYPE_FK ";

            strSQL = "  SELECT  Count(*) FROM ";
            strSQL += "(SELECT ROWNUM SR_NO,T.* FROM";
            strSQL += " (SELECT DISTINCT VMT.VENDOR_MST_PK ,";
            strSQL += " VMT.ACTIVE,";
            strSQL += " VMT.VENDOR_ID VENDOR_ID, ";
            strSQL += " VMT.VENDOR_NAME VENDOR_NAME, ";
            strSQL += " LMT.LOCATION_ID LOCATION_ID,";
            strSQL += " DECODE(VMT.BUSINESS_TYPE,'1','Air','2','Sea','3','Both') BUSINESS_TYPE,";
            strSQL += " VMT.VERSION_NO";
            strSQL += " FROM";
            strSQL += " VENDOR_MST_TBL VMT,VENDOR_SERVICES_TRN VSTRN,";
            strSQL += " VENDOR_TYPE_MST_TBL VTMST ,";
            strSQL += " VENDOR_CONTACT_DTLS VMTDTL,";
            strSQL += " LOCATION_MST_TBL LMT";
            strSQL += " WHERE 1=1";
            strSQL += " AND  VMTDTL.VENDOR_MST_FK(+) = VMT.VENDOR_MST_PK";
            strSQL += " AND  LMT.LOCATION_MST_PK(+) = VMTDTL.ADM_LOCATION_MST_FK ";

            strSQL += strCondition;
            strSQL += " )T)qry";

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
            strSQL += "(SELECT ROWNUM SR_NO,T.* FROM";
            strSQL += " (SELECT DISTINCT VMT.VENDOR_MST_PK ,";
            strSQL += " VMT.ACTIVE,";
            strSQL += " VMT.VENDOR_ID VENDOR_ID, ";
            strSQL += " VMT.VENDOR_NAME VENDOR_NAME, ";
            strSQL += " LMT.LOCATION_ID LOCATION_ID,";
            strSQL += " DECODE(VMT.BUSINESS_TYPE,'1','Air','2','Sea','3','Both') BUSINESS_TYPE,";
            strSQL += " VMT.VERSION_NO";
            strSQL += " FROM";
            strSQL += " VENDOR_MST_TBL VMT,VENDOR_SERVICES_TRN VSTRN,";
            strSQL += " VENDOR_TYPE_MST_TBL VTMST ,";
            strSQL += " VENDOR_CONTACT_DTLS VMTDTL,";
            // strSQL &= vbCrLf & " VENDOR_SERVICES_TRN VS,"
            strSQL += " LOCATION_MST_TBL LMT";
            strSQL += " WHERE 1=1";
            strSQL += " AND  VMTDTL.VENDOR_MST_FK(+) = VMT.VENDOR_MST_PK";
            strSQL += " AND  LMT.LOCATION_MST_PK(+) = VMTDTL.ADM_LOCATION_MST_FK ";
            if (Convert.ToInt32(locpk) > 0 & IsAdmin == 0)
            {
                strSQL += " AND  LMT.LOCATION_MST_PK = " + locpk;
            }

            //strSQL &= vbCrLf & " AND  VS.VENDOR_MST_FK(+) = VMT.VENDOR_MST_PK "
            strSQL += strCondition;

            if (!strColumnName.Equals("SR_NO"))
            {
                strSQL += "order by " + strColumnName;
            }
            if (!blnSortAscending & !strColumnName.Equals("SR_NO"))
            {
                strSQL += " DESC";
            }
            strSQL += " )T)qry WHERE SR_NO  Between " + start + " and " + last;
            strSQL += " ORDER BY  SR_NO";
            try
            {
                return (new WorkFlow()).GetDataSet(strSQL);
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 13/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "FetchListing"

        #region "FetchAll Vendor Function"

        /// <summary>
        /// Fetches the selected.
        /// </summary>
        /// <param name="pk">The pk.</param>
        /// <returns></returns>
        public DataSet fetchSelected(long pk)
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            strSQL = "DELETE FROM Vendor_CATEGORY_TRN CCT WHERE CCT.Vendor_MST_FK=" + pk;
            strSQL = objWF.ExecuteScaler(strSQL);
            strSQL = "select Vendor_category_PK,Vendor_Mst_Fk,Vendor_Category_Mst_FK,version_No from Vendor_category_trn Where Vendor_Mst_FK=" + pk;

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
        /// Fetches all.
        /// </summary>
        /// <param name="VendorPK">The vendor pk.</param>
        /// <returns></returns>
        public DataSet FetchAll(long VendorPK)
        {
            string strSQL = null;
            strSQL = string.Empty;
            strSQL += "SELECT ROWNUM SR_NO,  ";
            strSQL += " CMT.Vendor_Mst_Pk Vendor_Mst_Pk, ";
            strSQL += " CMT.Vendor_Id Vendor_Id, ";
            strSQL += " CMT.Vendor_Name Vendor_Name, ";
            strSQL += " CMT.cust_corporate_mst_fk cust_corporate_mst_fk, ";
            strSQL += " CMT.Vendor_Name Vendor_Corp_Name, ";
            strSQL += " CMT.Vendor_TYPE_FK, ";
            strSQL += " CTMT.Vendor_TYPE_id Vendor_TYPE, ";
            strSQL += " CMT.location_type_mst_fk location_type_mst_fk, ";
            strSQL += " LTMT.location_type_id location_type_id, ";
            strSQL += " CMT.Vendor_Type_Fk Vendor_Type_Fk, ";
            strSQL += " CMTp.Vendor_Mst_Fk Parent_Vendor_Fk, ";
            strSQL += " CMTp.Vendor_id Group_ID, ";
            strSQL += " CMT.Address1 Address1, ";
            strSQL += " CMT.Address2 Address2, ";
            strSQL += " CMT.Address3 Address3, ";
            strSQL += " CMT.City City, ";
            strSQL += " CMT.Zip Zip, ";
            strSQL += " CMT.country_mst_fk country_mst_fk , ";
            strSQL += " CoMT.Country_NAME Country_Name, ";
            strSQL += " CMT.Phone1 Phone1, ";
            strSQL += " CMT.Phone2 Phone2, ";
            strSQL += " CMT.Fax Fax, ";
            strSQL += " CMT.Email Email, ";
            strSQL += " CMT.Url URL, ";
            strSQL += " CMT.Location_Mst_Fk Location_mst_fk, ";
            strSQL += " LMT.LOCATION_NAME Location_Name, ";
            strSQL += " CMT.EMPLOYEE_MST_FK EMPLOYEE_MST_FK, ";
            strSQL += " Emp.EMPLOYEE_NAME EMPLOYEE_NAME, ";
            strSQL += " CMT.VERSION_NO Version_No ,CMT.ISACTIVE ";
            strSQL += " FROM Vendor_MST_TBL CMT, ";
            strSQL += "  Vendor_TYPE_MST_TBL CTMT, ";
            strSQL += "  location_type_mst_tbl LTMT, ";
            strSQL += "  LOCATION_MST_TBL LMT, ";
            strSQL += "  employee_mst_tbl Emp, ";
            strSQL += "  COUNTRY_MST_TBL CoMT, ";
            strSQL += "  Vendor_MST_TBL CMTp  ";
            strSQL += "  WHERE CMT.Vendor_TYPE_FK=CTMT.Vendor_TYPE_PK  ";
            strSQL += "   AND CMT.LOCATION_MST_FK=LMT.LOCATION_MST_PK ";
            strSQL += "   AND CMT.country_mst_fk = CoMT.country_mst_pk ";
            strSQL += "   AND CMT.LOCATION_TYPE_MST_FK = LTMT.LOCATION_TYPE_MST_PK ";
            strSQL += "   AND CMT.EMPLOYEE_MST_FK = Emp.EMPLOYEE_MST_PK ";
            strSQL += "   AND CMTp.Vendor_mst_pk(+) = cmt.Vendor_mst_fk";
            strSQL += "   AND CMT.Vendor_Mst_Pk = " + VendorPK;
            strSQL += "  Order by CMT.Vendor_Id ";
            WorkFlow objWF = new WorkFlow();
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
        /// Fetch_s the vendor.
        /// </summary>
        /// <param name="VendorPK">The vendor pk.</param>
        /// <param name="VendorID">The vendor identifier.</param>
        /// <param name="VendorName">Name of the vendor.</param>
        /// <param name="Condition">The condition.</param>
        /// <param name="ExecutiveId">The executive identifier.</param>
        /// <param name="LoggedLocPK">The logged loc pk.</param>
        /// <returns></returns>
        public DataSet Fetch_Vendor(Int16 VendorPK = 0, string VendorID = "", string VendorName = "", string Condition = "", Int64 ExecutiveId = 0, Int64 LoggedLocPK = 0)
        {
            string strSQL = null;
            strSQL = "select ' '  Vendor_ID,' ' Vendor_NAME,0 Vendor_MST_PK from dual ";
            strSQL = strSQL + " UNION ";
            strSQL = strSQL + "select  Vendor_ID,Vendor_NAME,Vendor_MST_PK from Vendor_mst_tbl";
            strSQL = strSQL + " Where 1=1 ";

            if (LoggedLocPK > 0)
            {
                strSQL = strSQL + " and Vendor_mst_tbl.LOCATION_MST_FK= " + LoggedLocPK;
            }

            if (ExecutiveId > 0)
            {
                strSQL = strSQL + " and  EMPLOYEE_MST_FK =" + ExecutiveId.ToString().Trim() + " ";
            }
            if (Condition.Trim().Length > 0)
            {
                strSQL = strSQL + " and  Vendor_MST_PK not in (" + Condition.Trim() + ")";
            }

            strSQL = strSQL + " order by Vendor_ID";

            WorkFlow objWF = new WorkFlow();
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

        /// <summary>
        /// Fetches for port vendor.
        /// </summary>
        /// <param name="VendorPK">The vendor pk.</param>
        /// <param name="VendorID">The vendor identifier.</param>
        /// <param name="VendorName">Name of the vendor.</param>
        /// <param name="Condition">The condition.</param>
        /// <param name="ExecutiveId">The executive identifier.</param>
        /// <param name="PortPK">The port pk.</param>
        /// <returns></returns>
        public DataSet FetchForPortVendor(Int16 VendorPK = 0, string VendorID = "", string VendorName = "", string Condition = "", Int64 ExecutiveId = 1, Int64 PortPK = 0)
        {
            string strSQL = null;
            strSQL = "select ' '  Vendor_ID,' ' Vendor_NAME,0 Vendor_MST_PK from dual ";
            strSQL = strSQL + " UNION ";
            strSQL = strSQL + "select  Vendor_ID,Vendor_NAME,Vendor_MST_PK from Vendor_mst_tbl ";
            strSQL = strSQL + " Where 1=1 and Vendor_mst_tbl.LOCATION_MST_FK IN ";
            strSQL = strSQL + " (select lwp.location_mst_fk from location_working_ports_trn lwp where";
            strSQL = strSQL + " lwp.port_mst_fk = " + PortPK + ")";

            if (ExecutiveId > 0)
            {
                strSQL = strSQL + " and  EMPLOYEE_MST_FK =" + ExecutiveId.ToString().Trim() + " ";
            }
            if (Condition.Trim().Length > 0)
            {
                strSQL = strSQL + " and  Vendor_MST_PK not in (" + Condition.Trim() + ")";
            }

            strSQL = strSQL + " order by Vendor_ID";

            WorkFlow objWF = new WorkFlow();
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

        #endregion "FetchAll Vendor Function"

        #region "Insert Function"

        /// <summary>
        /// Inserts the specified object ws.
        /// </summary>
        /// <param name="objWS">The object ws.</param>
        /// <returns></returns>
        public long Insert(ref WorkFlow objWS)
        {
            try
            {
                System.DBNull MYNULL = null;
                long intPkVal = 0;
                objWS.MyCommand.CommandType = CommandType.StoredProcedure;
                var _with7 = objWS.MyCommand.Parameters;
                _with7.Add("Vendor_Mst_Fk_IN", M_Vendor_Mst_Fk).Direction = ParameterDirection.Input;
                _with7.Add("Vendor_Type_Fk_IN", M_Vendor_Type_Fk).Direction = ParameterDirection.Input;
                _with7.Add("Vendor_Id_IN", M_Vendor_Id).Direction = ParameterDirection.Input;
                _with7.Add("Vendor_Name_IN", M_Vendor_Name).Direction = ParameterDirection.Input;
                _with7.Add("Address1_IN", M_Address1).Direction = ParameterDirection.Input;
                _with7.Add("Address2_IN", M_Address2).Direction = ParameterDirection.Input;
                _with7.Add("City_IN", M_City).Direction = ParameterDirection.Input;
                _with7.Add("Zip_IN", M_Zip).Direction = ParameterDirection.Input;
                _with7.Add("Phone1_IN", M_Phone1).Direction = ParameterDirection.Input;
                _with7.Add("Phone2_IN", M_Phone2).Direction = ParameterDirection.Input;
                _with7.Add("Fax_IN", M_Fax).Direction = ParameterDirection.Input;
                _with7.Add("Email_IN", M_Email).Direction = ParameterDirection.Input;
                _with7.Add("Url_IN", M_Url).Direction = ParameterDirection.Input;
                _with7.Add("Location_Mst_Fk_IN", (M_Location_Mst_Fk == 0 ? 0 : M_Location_Mst_Fk)).Direction = ParameterDirection.Input;
                _with7.Add("Created_By_Fk_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                _with7.Add("RETURN_VALUE", intPkVal).Direction = ParameterDirection.Output;
                objWS.MyCommand.CommandText = objWS.MyUserName + ".Vendor_MST_TBL_PKG.Vendor_MST_TBL_Ins";
                if (objWS.ExecuteTransactionCommands() == true)
                {
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
                //'Exception Handling Added by Gangadhar on 13/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Insert Function"

        #region "Update Function"

        /// <summary>
        /// Updates the specified object ws.
        /// </summary>
        /// <param name="objWS">The object ws.</param>
        /// <returns></returns>
        public int Update(WorkFlow objWS)
        {
            try
            {
                Int32 intPkVal = default(Int32);
                objWS.MyCommand.CommandType = CommandType.StoredProcedure;
                var _with8 = objWS.MyCommand.Parameters;
                _with8.Add("Vendor_Mst_Pk_IN", M_Vendor_Mst_Pk).Direction = ParameterDirection.Input;
                _with8.Add("Vendor_Mst_Fk_IN", M_Vendor_Mst_Fk).Direction = ParameterDirection.Input;
                _with8.Add("Vendor_Type_Fk_IN", M_Vendor_Type_Fk).Direction = ParameterDirection.Input;
                _with8.Add("Vendor_Id_IN", M_Vendor_Id).Direction = ParameterDirection.Input;
                _with8.Add("Vendor_Name_IN", M_Vendor_Name).Direction = ParameterDirection.Input;
                _with8.Add("Address1_IN", M_Address1).Direction = ParameterDirection.Input;
                _with8.Add("Address2_IN", M_Address2).Direction = ParameterDirection.Input;
                _with8.Add("City_IN", M_City).Direction = ParameterDirection.Input;
                _with8.Add("Zip_IN", M_Zip).Direction = ParameterDirection.Input;
                _with8.Add("Phone1_IN", M_Phone1).Direction = ParameterDirection.Input;
                _with8.Add("Phone2_IN", M_Phone2).Direction = ParameterDirection.Input;
                _with8.Add("Fax_IN", M_Fax).Direction = ParameterDirection.Input;
                _with8.Add("Email_IN", M_Email).Direction = ParameterDirection.Input;
                _with8.Add("Url_IN", M_Url).Direction = ParameterDirection.Input;
                _with8.Add("Location_Mst_Fk_IN", M_Location_Mst_Fk).Direction = ParameterDirection.Input;
                _with8.Add("Last_Modified_By_Fk_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                _with8.Add("Version_No_IN", M_VERSION_NO).Direction = ParameterDirection.Input;

                objWS.MyCommand.CommandText = objWS.MyUserName + ".Vendor_MST_TBL_PKG.Vendor_MST_TBL_UPD";
                if (objWS.ExecuteTransactionCommands() == true)
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
                //'Exception Handling Added by Gangadhar on 13/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Update Function"

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
                var _with9 = objWS.MyCommand.Parameters;
                _with9.Add("Vendor_Mst_Pk_IN", M_Vendor_Mst_Pk).Direction = ParameterDirection.Input;
                _with9.Add("Version_No_IN", M_VERSION_NO).Direction = ParameterDirection.Input;
                objWS.MyCommand.CommandText = objWS.MyUserName + ".Vendor_MST_TBL_PKG.Vendor_MST_TBL_DEL";
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
                //'Exception Handling Added by Gangadhar on 13/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Delete Function"

        #region "Delete"

        /// <summary>
        /// Deletes the specified deleted row.
        /// </summary>
        /// <param name="DeletedRow">The deleted row.</param>
        /// <returns></returns>
        public ArrayList Delete(ArrayList DeletedRow)
        {
            WorkFlow obJWk = new WorkFlow();
            OracleTransaction oraTran = null;
            System.Data.OracleClient.OracleCommand delCommand = new System.Data.OracleClient.OracleCommand();
            string strReturn = null;
            string[] arrRowDetail = null;
            Int32 i = default(Int32);
            try
            {
                obJWk.OpenConnection();
                oraTran = obJWk.MyConnection.BeginTransaction();

                for (i = 0; i <= DeletedRow.Count - 1; i++)
                {
                    var _with10 = obJWk.MyCommand;
                    _with10.Transaction = oraTran;
                    _with10.Connection = obJWk.MyConnection;
                    _with10.CommandType = CommandType.StoredProcedure;
                    _with10.CommandText = obJWk.MyUserName + ".Vendor_MST_TBL_PKG.Vendor_MST_TBL_DEL";
                    arrRowDetail = Convert.ToString(DeletedRow[i]).Split(',');
                    _with10.Parameters.Clear();
                    var _with11 = _with10.Parameters;
                    _with11.Add("Vendor_MST_PK_IN", arrRowDetail[0]).Direction = ParameterDirection.Input;
                    _with11.Add("DELETED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                    _with11.Add("VERSION_NO_IN", arrRowDetail[1]).Direction = ParameterDirection.Input;
                    _with11.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                    _with11.Add("RETURN_VALUE", strReturn).Direction = ParameterDirection.Output;
                    obJWk.MyCommand.Parameters["RETURN_VALUE"].OracleDbType = OracleDbType.Varchar2;
                    obJWk.MyCommand.Parameters["RETURN_VALUE"].Size = 50;
                    _with10.ExecuteNonQuery();
                    // Try
                    //If .ExecuteNonQuery() > 0 Then
                    //    oraTran.Commit()
                    //Else
                    //    arrMessage.Add(arrRowDetail(0) & " cannot be deleted")
                    //    oraTran.Rollback()
                    //End If
                    // Catch e As Exception
                    //     arrMessage.Add(arrRowDetail(0) & " cannot be deleted")
                    //    oraTran.Rollback()
                    // End Try
                }
                if (arrMessage.Count > 0)
                {
                    oraTran.Rollback();
                    return arrMessage;
                }
                else
                {
                    oraTran.Commit();
                    arrMessage.Add("Success");
                    return arrMessage;
                }
            }
            catch (OracleException oraexp)
            {
                //Throw oraexp
                arrMessage.Add(oraexp.Message);
                oraTran.Rollback();
                return arrMessage;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                obJWk.MyConnection.Close();
            }
        }

        #endregion "Delete"

        #region "Fetch Vendors-Organorgram"

        /// <summary>
        /// Fetches the og vendors.
        /// </summary>
        /// <param name="LocMstPk">The loc MST pk.</param>
        /// <param name="CustPK">The customer pk.</param>
        /// <param name="CustID">The customer identifier.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="BusiModel">The busi model.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="SortExpression">The sort expression.</param>
        /// <returns></returns>
        public DataSet FetchOGVendors(long LocMstPk = 0, long CustPK = 0, string CustID = "", string CustName = "", string BusiModel = "", string SearchType = "", string SortExpression = "")
        {
            string strSQL = null;
            string strCondition = null;
            System.Web.UI.Page objPage = new System.Web.UI.Page();

            //If LocMstPk > 0 Then
            //    strCondition &= vbCrLf & "loc.LOCATION_MST_PK=" & LocMstPk
            //End If

            if (CustPK > 0)
            {
                strCondition += " And cust.Vendor_mst_pk =" + CustPK;
            }

            if (CustID.ToString().Trim().Length > 0 & SearchType == "C")
            {
                strCondition += " And UPPER(cust.Vendor_id) like '%" + CustID.ToUpper().Trim().Replace("'", "''") + "%' ";
            }
            else if (CustID.ToString().Trim().Length > 0 & SearchType == "S")
            {
                strCondition += " And UPPER(cust.Vendor_id) like '" + CustID.ToUpper().Trim().Replace("'", "''") + "%' ";
            }

            if (CustName.ToString().Trim().Length > 0 & SearchType == "C")
            {
                strCondition = strCondition + " And UPPER(cust.Vendor_name) like '%" + CustName.ToUpper().Trim().Replace("'", "''") + "%' ";
            }
            else if (CustName.ToString().Trim().Length > 0 & SearchType == "S")
            {
                strCondition = strCondition + " And UPPER(cust.Vendor_name) like '" + CustName.ToUpper().Trim().Replace("'", "''") + "%' ";
            }
            if (BlankGrid == 0)
            {
                strCondition += " AND 1=2 ";
            }

            strSQL = "select ROWNUM SR_NO,q.* from (";
            strSQL += " SELECT  cust.Vendor_mst_pk, ";
            strSQL += " cust.cust_corporate_mst_fk, ";
            strSQL += " cust.Vendor_id,";
            strSQL += " cust.Vendor_name,";
            strSQL += " cust.Vendor_type_fk,";
            strSQL += " cust.location_mst_fk,";
            strSQL += " loc.location_name Location,";
            strSQL += " loctype.location_type_desc BranchType,";
            strSQL += " cutype.Vendor_type_id VendorType,";
            strSQL += " cust.version_no FROM ";
            strSQL += " Vendor_mst_tbl cust,";
            strSQL += " Vendor_type_mst_tbl cutype,";
            strSQL += " location_mst_tbl loc,";
            strSQL += " location_type_mst_tbl loctype WHERE";
            strSQL += " cust.Vendor_type_fk=cutype.Vendor_type_pk";
            strSQL += " AND cust.location_mst_fk=loc.location_mst_pk";
            strSQL += " AND loc.location_type_fk=loctype.location_type_mst_pk";
            // strSQL &= vbCrLf & " AND loc.location_mst_pk=" & CType(objPage.Session.Item("LOGED_IN_LOC_FK"), Int64) & " "
            strSQL += " AND loc.location_mst_pk=" + LocMstPk + " ";

            if (BusiModel != "All")
            {
                strSQL += "AND UPPER(cutype.Vendor_TYPE_id) LIKE '" + BusiModel + "%'";
            }
            strSQL += strCondition;
            strSQL += " order by cust.Vendor_id)q";
            //strSQL &= vbCrLf & " AND loc.location_mst_pk=1"
            //strSQL &= vbCrLf & strCondition

            //If SortExpression.Trim.Length > 0 Then
            //    strSQL = strSQL & " " & SortExpression
            //Else
            //    strSQL = strSQL & " " & SortExpression
            //End If
            //strSQL &= vbCrLf & " )qry WHERE SR_NO  Between " & start & " and " & last

            try
            {
                return (new WorkFlow()).GetDataSet(strSQL);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 13/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Vendors-Organorgram"

        #region "Fetch Other Agents"

        /// <summary>
        /// Fetches the other agents.
        /// </summary>
        /// <param name="CorpPK">The corp pk.</param>
        /// <returns></returns>
        public DataSet FetchOtherAgents(long CorpPK = 0)
        {
            string strSQL = null;
            string strCondition = null;
            System.Web.UI.Page objPage = new System.Web.UI.Page();

            if (BlankGrid == 0)
            {
                strCondition += " AND 1=2 ";
            }

            strSQL = "select ROWNUM SR_NO,q.* from (";
            strSQL += " SELECT  cust.Vendor_mst_pk, ";
            strSQL += " cust.cust_corporate_mst_fk, ";
            strSQL += " cust.Vendor_id,";
            strSQL += " cust.Vendor_name,";
            strSQL += " cust.Vendor_type_fk,";
            strSQL += " cust.location_mst_fk,";
            strSQL += " loc.location_name Location,";
            strSQL += " loctype.location_type_desc BranchType,";
            strSQL += " cutype.Vendor_type_id VendorType,";
            strSQL += " cust.version_no FROM ";
            strSQL += " Vendor_mst_tbl cust,";
            strSQL += " Vendor_type_mst_tbl cutype,";
            strSQL += " location_mst_tbl loc,";
            strSQL += " location_type_mst_tbl loctype WHERE";
            strSQL += " cust.Vendor_type_fk=cutype.Vendor_type_pk";
            strSQL += " AND cust.location_mst_fk=loc.location_mst_pk";
            strSQL += " AND loc.location_type_fk=loctype.location_type_mst_pk";
            strSQL += " AND cust.cust_corporate_mst_fk=" + CorpPK;
            strSQL += " order by cust.Vendor_id)q";
            //If SortExpression.Trim.Length > 0 Then
            //    strSQL = strSQL & " " & SortExpression
            //Else
            //    strSQL = strSQL & " " & SortExpression & ")qry"
            //End If
            //strSQL &= vbCrLf & " )qry WHERE SR_NO  Between " & start & " and " & last

            try
            {
                return (new WorkFlow()).GetDataSet(strSQL);
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 13/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Other Agents"

        #region "Fetch Location"

        /// <summary>
        /// Fetches the location.
        /// </summary>
        /// <returns></returns>
        public OracleDataReader FetchLocation()
        {
            OracleDataReader dR = null;
            string SqlStr = null;
            SqlStr = "select location_mst_pk,location_id,upper(location_name) as LOCATION_NAME";
            SqlStr += " from location_mst_tbl ORDER BY location_name ASC";
            WorkFlow objwf = new WorkFlow();
            try
            {
                return objwf.GetDataReader(SqlStr);
            }
            catch (OracleException sqlex)
            {
                ErrorMessage = sqlex.Message;
            }
            catch (Exception Exp)
            {
                ErrorMessage = Exp.Message;
                throw Exp;
            }
            return dR;
        }

        #endregion "Fetch Location"

        #region "Fetch Vendor Type"

        /// <summary>
        /// Fetches the type of the customer.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchCustType()
        {
            string strSQL = null;
            strSQL = "select ' ' Vendor_TYPE_ID,'ALL' Vendor_TYPE_NAME,Vendor_TYPE_PK from dual ";
            strSQL = strSQL + " UNION ";
            strSQL = strSQL + "select  Vendor_TYPE_ID,Vendor_TYPE_NAME,Vendor_TYPE_PK from Vendor_type_mst_tbl ctmt where ACTIVE_FLAG = 1";
            try
            {
                return (new WorkFlow()).GetDataSet(strSQL);
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 13/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Vendor Type"

        #region "Fetch Supplier Type"

        /// <summary>
        /// Fetches the type of the supplier.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchSupplierType()
        {
            string strSQL = null;
            //strSQL = "select 0 Vendor_TYPE_PK,'<ALL>' Vendor_TYPE_ID from dual UNION select  Vendor_TYPE_PK,Vendor_TYPE_ID from Vendor_type_mst_tbl ctmt where ACTIVE_FLAG = 1"
            strSQL = "select 0 Vendor_TYPE_PK,' ALL ' Vendor_TYPE_ID from dual UNION select  Vendor_TYPE_PK,Vendor_TYPE_ID from Vendor_type_mst_tbl ctmt where ACTIVE_FLAG = 1  order by Vendor_TYPE_ID";
            try
            {
                return (new WorkFlow()).GetDataSet(strSQL);
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Supplier Type"

        #region " Enhance Search "

        // This enhance search is for displaying Vendor list according to provided Vendor category PK
        // Rajesh 19-Jan-2006
        /// <summary>
        /// Fetches the vendor for category.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchVendorForCategory(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strSEARCH_CAT_PK_IN = "";
            string strLOCATION_IN = "";
            int intBUSINESS_TYPE_IN = 3;
            string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));

            //Modified by Mani: This enhance search is for displaying Vendor list
            if (arr.Length > 2)
                strSEARCH_CAT_PK_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strLOCATION_IN = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                intBUSINESS_TYPE_IN = Convert.ToInt32(arr.GetValue(4));

            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_Vendor_PKG.GET_Vendor_CAT_COMMON";
                var _with12 = SCM.Parameters;
                _with12.Add("SEARCH_CATPK_IN", ifDBNull(strSEARCH_CAT_PK_IN)).Direction = ParameterDirection.Input;
                _with12.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with12.Add("LOCATION_MST_FK_IN", strLOCATION_IN).Direction = ParameterDirection.Input;
                _with12.Add("BUSINESS_TYPE_IN", intBUSINESS_TYPE_IN).Direction = ParameterDirection.Input;
                _with12.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with12.Add("RETURN_VALUE", OracleDbType.Varchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 13/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                SCM.Connection.Close();
            }
        }

        #endregion " Enhance Search "

        #region " Supporting Function "

        /// <summary>
        /// Ifs the database null.
        /// </summary>
        /// <param name="col">The col.</param>
        /// <returns></returns>
        private object ifDBNull(object col)
        {
            if (Convert.ToString(col).Length == 0)
            {
                return DBNull.Value;
            }
            else
            {
                return col;
            }
        }

        /// <summary>
        /// Removes the database null.
        /// </summary>
        /// <param name="col">The col.</param>
        /// <returns></returns>
        private object removeDBNull(object col)
        {
            if (object.ReferenceEquals(col, DBNull.Value))
            {
                return "";
            }
            return col;
        }

        #endregion " Supporting Function "

        #region " Fetch Vendor Category Pk for ('SHIPPER','NOTIFY') "

        /// <summary>
        /// Fetches the vendor category.
        /// </summary>
        /// <returns></returns>
        public OracleDataReader FetchVendorCategory()
        {
            try
            {
                System.Text.StringBuilder Sql = new System.Text.StringBuilder();
                Sql.Append("SELECT C.Vendor_CATEGORY_MST_PK FROM ");
                Sql.Append("Vendor_CATEGORY_MST_TBL C ");
                Sql.Append("WHERE UPPER(C.Vendor_CATEGORY_ID) IN ('SHIPPER','NOTIFY') ");

                return (new WorkFlow()).GetDataReader(Sql.ToString());
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 13/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion " Fetch Vendor Category Pk for ('SHIPPER','NOTIFY') "

        #region "FetchSupplierSecondaryServ"

        /// <summary>
        /// Fetches the supplier secondary serv.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchSupplierSecondaryServ(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strLOC_MST_IN = null;
            string strSERACH_IN = "";
            string PaymentTypeIN = "";
            string ServiceTypeIN = "";
            int intBUSINESS_TYPE_IN = 3;
            int intLocationfk = 0;
            int intProcess = 0;
            int intPortLocPK = 0;
            int intVendorflg = 0;
            string strReq = null;
            string strJobRefNr = null;
            var strNull = DBNull.Value;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                PaymentTypeIN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                intLocationfk = Convert.ToInt32(arr.GetValue(3));
            if (arr.Length > 4)
                intPortLocPK = Convert.ToInt32(arr.GetValue(4));
            if (arr.Length > 5)
                intVendorflg = Convert.ToInt32(arr.GetValue(5));
            if (arr.Length >= 6)
                ServiceTypeIN = Convert.ToString(arr.GetValue(6));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_VENDOR_PKG.GETJOBSECONDSERV_SUPPLIER";
                var _with13 = selectCommand.Parameters;
                _with13.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with13.Add("BUSINESS_TYPE_IN", intBUSINESS_TYPE_IN).Direction = ParameterDirection.Input;
                _with13.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with13.Add("VENDOR_TYPE_IN", ServiceTypeIN).Direction = ParameterDirection.Input;
                _with13.Add("LOCATION_MST_FK_IN", intLocationfk).Direction = ParameterDirection.Input;
                _with13.Add("PROCESS_TYPE_IN", intProcess).Direction = ParameterDirection.Input;
                _with13.Add("PAYMENT_TYPE_IN", (!string.IsNullOrEmpty(PaymentTypeIN) ? PaymentTypeIN : "")).Direction = ParameterDirection.Input;
                _with13.Add("PORT_LOC_FK_IN", intPortLocPK).Direction = ParameterDirection.Input;
                _with13.Add("VENDOR_FLG_IN", intVendorflg).Direction = ParameterDirection.Input;
                _with13.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 13/09/2011, PTS ID: SEP-01
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

        #endregion "FetchSupplierSecondaryServ"
    }
}