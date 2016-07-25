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
using Oracle.DataAccess.Types;
using System;
using System.Collections;
using System.Data;
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    internal class cls_Customer_Mst_Tbl :CommonFeatures
    {
        #region "List of Members of the Class"
        private long M_Customer_Mst_Pk;
        private long M_Customer_Mst_Fk;
        private long M_Customer_Type_Fk;
        private string M_CustomerTypeName;
        private string M_Customer_Id;
        private string M_Customer_Name;
        private string M_Address1;
        private string M_Address2;
        private string M_City;
        private string M_Phone1;
        private string M_Phone2;
        private string M_Fax;
        private string M_Email;
        private string M_Url;
        private long M_Location_Mst_Fk;
        private string M_LocationName;
        private string M_Zip;
        private DataSet M_DataSet;

        private DataSet M_Contact_DataSet;
        #endregion

        #region "List of Properties"

        public long Customer_Mst_Pk
        {
            get { return M_Customer_Mst_Pk; }
            set { M_Customer_Mst_Pk = value; }
        }

        public long Customer_Mst_Fk
        {
            get { return M_Customer_Mst_Fk; }
            set { M_Customer_Mst_Fk = value; }
        }

        public long Customer_Type_Fk
        {
            get { return M_Customer_Type_Fk; }
            set { M_Customer_Type_Fk = value; }
        }

        public string Customer_Id
        {
            get { return M_Customer_Id; }
            set { M_Customer_Id = value; }
        }

        public string Customer_Name
        {
            get { return M_Customer_Name; }
            set { M_Customer_Name = value; }
        }

        public string Address1
        {
            get { return M_Address1; }
            set { M_Address1 = value; }
        }

        public string Address2
        {
            get { return M_Address2; }
            set { M_Address2 = value; }
        }

        public string City
        {
            get { return M_City; }
            set { M_City = value; }
        }

        public string Phone1
        {
            get { return M_Phone1; }
            set { M_Phone1 = value; }
        }

        public string Phone2
        {
            get { return M_Phone2; }
            set { M_Phone2 = value; }
        }

        public string Fax
        {
            get { return M_Fax; }
            set { M_Fax = value; }
        }

        public string Email
        {
            get { return M_Email; }
            set { M_Email = value; }
        }

        public string Url
        {
            get { return M_Url; }
            set { M_Url = value; }
        }

        public long Location_Mst_Fk
        {
            get { return M_Location_Mst_Fk; }
            set { M_Location_Mst_Fk = value; }
        }
        public string Zip
        {
            get { return M_Zip; }
            set { M_Zip = value; }
        }

        public DataSet MyDataSet
        {
            get { return M_DataSet; }
            set { M_DataSet = value; }
        }

        public DataSet ContactDataSet
        {
            get { return M_Contact_DataSet; }
            set { M_Contact_DataSet = value; }
        }


        public string CustomerTypeName
        {
            get { return M_CustomerTypeName; }
        }
        public string LocationName
        {
            get { return M_LocationName; }
        }

        #endregion

        #region "Initalize dataset"
        public void InitalizeDS()
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            strSQL = "SELECT A.Customer_Mst_Pk,";
            strSQL += "A.Customer_Mst_Fk,";
            strSQL += "A.Customer_Type_Fk,";
            strSQL += "A.Customer_Id,";
            strSQL += "A.Customer_Name,";
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
            strSQL += " FROM CUSTOMER_MST_TBL A WHERE 1=2";
            try
            {
                M_DataSet = objWF.GetDataSet(strSQL);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Constructors"
       
        #endregion

        #region "Enhance Search & Lookup Search Block"
        //Pls do the impact the analysis before changing as this function
        //as might be accesed by other forms also. 
        public string FetchCustomer_Temp(string strcond)
        {
            //Created by Manoharan 01Feb2007
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_CUSTOMER_PKG.GETCUSTOMER_TEMP";

                var _with1 = selectCommand.Parameters;
                _with1.Add("LOOKUP_VALUE_IN", strcond).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
                //Manjunath  PTS ID:Sep-02  17/09/2011
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
                selectCommand.Connection.Close();
            }
        }
        public string FetchCustomer(string strCond, string loc = "0")
        {

            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strLOC_MST_IN = null;
            string strSERACH_IN = "";
            int intBUSINESS_TYPE_IN = 3;
            string strCustomerType = "";
            string strReq = null;
            var strNull = DBNull.Value;
            arr = strCond.Split('~');;
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                intBUSINESS_TYPE_IN = Convert.ToInt32(arr.GetValue(2));
            if (arr.Length > 4)
                strCustomerType = Convert.ToString(arr.GetValue(4));
            //strLOC_MST_IN = Convert.ToString(arr.GetValue(3))

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_CUSTOMER_PKG.GETCUSTOMER_COMMON";

                var _with2 = selectCommand.Parameters;
                _with2.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with2.Add("CUSTOMER_TYPE_IN", getDefault(strCustomerType, DBNull.Value)).Direction = ParameterDirection.Input;
                _with2.Add("BUSINESS_TYPE_IN", intBUSINESS_TYPE_IN).Direction = ParameterDirection.Input;
                _with2.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with2.Add("LOCATION_MST_FK_IN", loc).Direction = ParameterDirection.Input;
                _with2.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
                //Manjunath  PTS ID:Sep-02  17/09/2011
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
                selectCommand.Connection.Close();
            }

        }
        public string FetchContainerType(string strCond, string loc = "0")
        {

            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strLOC_MST_IN = null;
            string strSERACH_IN = "";
            int intBUSINESS_TYPE_IN = 3;
            string strCustomerType = "";
            string strReq = null;
            var strNull = DBNull.Value;
            arr = strCond.Split('~');;
            strReq = Convert.ToString(arr.GetValue(0));
            // strSERACH_IN = Convert.ToString(arr.GetValue(1));
            //If arr.Length > 2 Then intBUSINESS_TYPE_IN = Convert.ToString(arr.GetValue(2))
            //If arr.Length > 4 Then strCustomerType = Convert.ToString(arr.GetValue(4))
            //strLOC_MST_IN = Convert.ToString(arr.GetValue(3))

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_CUSTOMER_PKG.GETCONTAINER_TYPE_COMMON";

                var _with3 = selectCommand.Parameters;
                _with3.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                //.Add("CUSTOMER_TYPE_IN", getDefault(strCustomerType, DBNull.Value)).Direction = ParameterDirection.Input
                //.Add("BUSINESS_TYPE_IN", intBUSINESS_TYPE_IN).Direction = ParameterDirection.Input
                _with3.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                //.Add("LOCATION_MST_FK_IN", loc).Direction = ParameterDirection.Input
                _with3.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
                //Manjunath  PTS ID:Sep-02  17/09/2011
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
                selectCommand.Connection.Close();
            }

        }

        public string FetchCreditCustomer(string strCond, string loc = "0")
        {

            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strLOC_MST_IN = null;
            string strSERACH_IN = "";
            int intBUSINESS_TYPE_IN = 3;
            string strCustomerType = "";
            string strReq = null;
            var strNull = DBNull.Value;
            arr = strCond.Split('~');;
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                intBUSINESS_TYPE_IN = Convert.ToInt32(arr.GetValue(2));
            if (arr.Length > 4)
                strCustomerType = Convert.ToString(arr.GetValue(4));
            //strLOC_MST_IN = Convert.ToString(arr.GetValue(3))

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_CUSTOMER_PKG.GETCREDITCUSTOMER_COMMON";

                var _with4 = selectCommand.Parameters;
                _with4.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with4.Add("CUSTOMER_TYPE_IN", getDefault(strCustomerType, DBNull.Value)).Direction = ParameterDirection.Input;
                _with4.Add("BUSINESS_TYPE_IN", intBUSINESS_TYPE_IN).Direction = ParameterDirection.Input;
                _with4.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with4.Add("LOCATION_MST_FK_IN", loc).Direction = ParameterDirection.Input;
                _with4.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
                //Manjunath  PTS ID:Sep-02  17/09/2011
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
                selectCommand.Connection.Close();
            }

        }
        public string FetchGroupCustomer(string strCond, string loc = "0")
        {

            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strLOC_MST_IN = null;
            string strSERACH_IN = "";
            int intLOC_MST_IN = 0;
            int intBUSINESS_TYPE_IN = 0;
            string strCustomerType = "";
            string strReq = null;
            var strNull = DBNull.Value;
            string SEARCH_FLAG_IN = "";
            arr = strCond.Split('~');;
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            //If arr.Length > 2 Then intBUSINESS_TYPE_IN = Convert.ToString(arr.GetValue(2))
            if (arr.Length > 2)
                loc = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 4)
                strCustomerType = Convert.ToString(arr.GetValue(4));
            if (arr.Length > 5)
                SEARCH_FLAG_IN = Convert.ToString(arr.GetValue(5));
            //strLOC_MST_IN = Convert.ToString(arr.GetValue(3))

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_CUSTOMER_PKG.GET_GROUP_CUSTOMER_LOCATION";

                var _with5 = selectCommand.Parameters;
                _with5.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with5.Add("category_in", getDefault(strCustomerType, DBNull.Value)).Direction = ParameterDirection.Input;
                //.Add("BUSINESS_TYPE_IN", intBUSINESS_TYPE_IN).Direction = ParameterDirection.Input
                _with5.Add("BUSINESS_TYPE_IN", (intBUSINESS_TYPE_IN == 0 ? 0 : intBUSINESS_TYPE_IN)).Direction = ParameterDirection.Input;
                _with5.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                //.Add("LOCATION_MST_FK_IN", loc).Direction = ParameterDirection.Input
                _with5.Add("LOCATION_MST_FK_IN", (string.IsNullOrEmpty(loc) ? "" : loc)).Direction = ParameterDirection.Input;
                _with5.Add("SEARCH_FLAG_IN", (string.IsNullOrEmpty(SEARCH_FLAG_IN) ? "" : SEARCH_FLAG_IN)).Direction = ParameterDirection.Input;
                _with5.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
                //Manjunath  PTS ID:Sep-02  17/09/2011
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
                selectCommand.Connection.Close();
            }

        }

        public string CustomerCategory(string strCond)
        {

            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strLOC_MST_IN = null;
            string strSERACH_IN = null;
            string strCustomerCategory = null;
            string strReq = null;
            int iPortPk = 0;
            int iBUSINESS_MODEL_IN = 0;
            int iCorpPK = 0;

            var strNull = DBNull.Value;
            arr = strCond.Split('~');;
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strCustomerCategory = Convert.ToString(arr.GetValue(2));
            iPortPk = Convert.ToInt32(arr.GetValue(3));
            iBUSINESS_MODEL_IN = Convert.ToInt32(arr.GetValue(4));
            strLOC_MST_IN = Convert.ToString(arr.GetValue(5));
            iCorpPK = Convert.ToInt32(arr.GetValue(6));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_CUSTOMER_PKG.GETCUSTOMER_CATEGORY_COMMON";

                var _with6 = selectCommand.Parameters;
                _with6.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with6.Add("BUSINESS_MODEL_IN", iBUSINESS_MODEL_IN).Direction = ParameterDirection.Input;
                _with6.Add("CATEGORY_IN", strCustomerCategory).Direction = ParameterDirection.Input;
                _with6.Add("CUST_CORP_PK", iCorpPK).Direction = ParameterDirection.Input;
                _with6.Add("PORT_MST_FK_IN", iPortPk).Direction = ParameterDirection.Input;
                _with6.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with6.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
                //Manjunath  PTS ID:Sep-02  17/09/2011
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
                selectCommand.Connection.Close();
            }

        }
        #endregion

        #region "FetchListing"
        public DataSet FetchListing(Int64 P_Customer_Mst_Pk = 0, string P_Customer_Id = "", string P_Customer_Name = "", string P_CUSTOMER_TYPE = "", string P_Location_ID = "", int BType = 0, int CustCategory = 0, int Tax = 0, int Credit = 0, string CustGroup = "",
        int CurrentBType = 0, string Client = "", string SearchType = "", string strColumnName = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, bool blnSortAscending = false, Int16 IsActive = 0, Int16 Status = -1, Int32 flag = 0,
        string IsAdmin = "N", string EcommUser = "")
        {
            //Manoharan 21Feb07: GAP-USS-QFOR-027: added status to fetch only permanent/temporary customer
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            System.Web.UI.Page objPage = new System.Web.UI.Page();

            if (P_Customer_Mst_Pk > 0 & SearchType == "C")
            {
                strCondition += " And cmt.Customer_Mst_Pk =" + P_Customer_Mst_Pk;
            }
            else if (P_Customer_Mst_Pk > 0 & SearchType == "S")
            {
                strCondition += " And cmt.Customer_Mst_Pk =" + P_Customer_Mst_Pk;
            }

            if (P_Customer_Id.ToString().Trim().Length > 0 & SearchType == "C")
            {
                strCondition += " And UPPER(cmt.Customer_Id) like '%" + P_Customer_Id.ToUpper().Replace("'", "''") + "%' ";
            }
            else if (P_Customer_Id.ToString().Trim().Length > 0 & SearchType == "S")
            {
                strCondition += " And UPPER(cmt.Customer_Id) like '" + P_Customer_Id.ToUpper().Replace("'", "''") + "%' ";
            }

            if (P_Customer_Name.ToString().Trim().Length > 0 & SearchType == "C")
            {
                strCondition = strCondition + " And UPPER(cmt.customer_name) like '%" + P_Customer_Name.ToUpper().Replace("'", "''") + "%' ";
            }
            else if (P_Customer_Name.ToString().Trim().Length > 0 & SearchType == "S")
            {
                strCondition = strCondition + " And UPPER(cmt.customer_name) like '" + P_Customer_Name.ToUpper().Replace("'", "''") + "%' ";
            }

            if (P_CUSTOMER_TYPE.Trim().Length > 0 & SearchType == "C")
            {
                strCondition = strCondition + " AND UPPER(ctmt.CUSTOMER_TYPE_id) LIKE '%" + P_CUSTOMER_TYPE.ToUpper().Replace("'", "''") + "%'";
            }
            else if (P_CUSTOMER_TYPE.Trim().Length > 0 & SearchType == "S")
            {
                strCondition = strCondition + " AND UPPER(ctmt.CUSTOMER_TYPE_id) LIKE '" + P_CUSTOMER_TYPE.ToUpper().Replace("'", "''") + "%'";
            }

            if (P_Location_ID.Trim().Length > 0 & SearchType == "C")
            {
                strCondition = strCondition + " AND Upper(lmt.location_ID) LIKE '%" + P_Location_ID.ToUpper().Replace("'", "''") + "%'";
            }
            else if (P_Location_ID.Trim().Length > 0 & SearchType == "S")
            {
                strCondition = strCondition + " AND upper(lmt.location_ID) LIKE '" + P_Location_ID.ToUpper().Replace("'", "''") + "%'";
            }

            //If P_Repo_To.Trim.Length > 0 And SearchType = "C" Then
            //    strCondition = strCondition & " AND upper(repcust.customer_id)  LIKE '%" & P_Repo_To.ToUpper.Replace("'", "''") & "%'" & vbCrLf
            //ElseIf P_Repo_To.Trim.Length > 0 And SearchType = "S" Then
            //    strCondition = strCondition & " AND upper(repcust.customer_id)  LIKE '" & P_Repo_To.ToUpper.Replace("'", "''") & "%'" & vbCrLf
            //End If

            //Manoharan 21Feb07: GAP-USS-QFOR-027
            if (Status != -1)
            {
                strCondition = strCondition + " AND CMT.TEMP_PARTY = " + Status;
            }
            if (Client == "BNM")
            {
                if (Tax != 0)
                {
                    strCondition = strCondition + " AND CMT.GROUP_CATEGORY = " + Tax;
                }
            }
            else
            {
                if (Tax != 0)
                {
                    strCondition = strCondition + " AND CMT.TAX_STATUS = " + Tax;
                }
            }

            if (CustCategory != 0)
            {
                strCondition = strCondition + " AND CMT.CUSTOMER_CATEGORY = " + CustCategory;
            }



            if (Client == "BNM")
            {
                if (!string.IsNullOrEmpty(CustGroup))
                {
                    strCondition = strCondition + " AND CMT.CUSTOMER_MST_PK = " + CustGroup + "AND CMT.GROUP_HEADER = 1";
                }
            }
            else
            {
                if (!string.IsNullOrEmpty(CustGroup))
                {
                    strCondition = strCondition + " AND CMT.REF_GROUP_CUST_PK = " + CustGroup;
                }
            }
            if (IsActive == 1)
            {
                strCondition = strCondition + " AND CMT.ACTIVE_FLAG =1 ";
            }
            if (BType != 3)
            {
                strCondition += " and (cmt.BUSINESS_TYPE = " + BType + " OR cmt.BUSINESS_TYPE = 3 )";
            }
            if (CurrentBType == 3 & BType == 3)
            {
                strCondition += " AND cmt.BUSINESS_TYPE IN (1,2,3) ";
            }
            else if (CurrentBType == 2 & BType == 3)
            {
                strCondition += " AND cmt.BUSINESS_TYPE IN (2,3) ";
            }
            else if (CurrentBType == 1 & BType == 3)
            {
                strCondition += " AND cmt.BUSINESS_TYPE IN (1,3) ";
                //added by surya prasad
            }
            else if (CurrentBType == 4 & BType == 4)
            {
                strCondition += " AND cmt.BUSINESS_TYPE IN (4) ";
            }
            else
            {
                strCondition += " AND cmt.BUSINESS_TYPE = " + CurrentBType + " ";
            }
            if (flag == 0)
            {
                strCondition += " AND 1=2";
            }
            if (!string.IsNullOrEmpty(EcommUser.ToString().Trim()))
            {
                strCondition += " AND CMT.CUSTOMER_ID IN";
                strCondition += "  (SELECT CUST_REG.REGN_NR_PK";
                strCondition += "   FROM SYN_EBK_M_CUST_REGN     CUST_REG,";
                strCondition += "   SYN_QBSO_EBK_M_CUST_PWD CUST_PWD";
                strCondition += "   WHERE CUST_REG.REGN_NR_PK = CUST_PWD.REGN_NR_FK";
                strCondition += "   AND CUST_REG.REGN_NR_PK = CMT.CUSTOMER_ID";
                strCondition += "   AND UPPER(CUST_PWD.USER_ID) = '" + EcommUser.ToString().Trim().ToUpper() + "')";
            }
            if (IsAdmin == "Y")
            {
                if (BType != 4)
                {
                    //Manoharan 06Feb08: it will show all customers of logged in location & logged in reporting loc
                    //strCondition &= vbCrLf & " and cmtdtl.ADM_LOCATION_MST_FK in (  select l.location_mst_pk from location_mst_tbl l"
                    //strCondition &= vbCrLf & " where l.location_mst_pk = " & CType(objPage.Session.Item("LOGED_IN_LOC_FK"), Int64)
                    //strCondition &= vbCrLf & " union select l.location_mst_pk from location_mst_tbl l where l.reporting_to_fk = " & CType(objPage.Session.Item("LOGED_IN_LOC_FK"), Int64)
                    //strCondition &= vbCrLf & " union select ll.location_mst_pk from location_mst_tbl ll where ll.reporting_to_fk in "
                    //strCondition &= vbCrLf & " (select l.location_mst_pk from location_mst_tbl l where l.reporting_to_fk = " & CType(objPage.Session.Item("LOGED_IN_LOC_FK"), Int64) & "))"
                    //End Manoharan 06Feb08:

                    strCondition += " AND CMTDTL.ADM_LOCATION_MST_FK IN ";
                    strCondition += " (SELECT L.LOCATION_MST_PK FROM LOCATION_MST_TBL L";
                    strCondition += " START WITH L.LOCATION_MST_PK=" + (Int64)objPage.Session["LOGED_IN_LOC_FK"];
                    strCondition += " CONNECT BY PRIOR L.LOCATION_MST_PK=L.REPORTING_TO_FK)";
                }
            }
            else
            {
                strCondition += " AND CMTDTL.ADM_LOCATION_MST_FK=" + (Int64)objPage.Session["LOGED_IN_LOC_FK"];
            }

            strCondition += " AND cmt.customer_type_fk NOT IN(SELECT customer_type_pk FROM customer_type_mst_tbl WHERE CTMT.ACTIVE_FLAG =0) ";
            strSQL = "SELECT Count(*) ";
            strSQL += " FROM";
            strSQL += " customer_mst_tbl cmt,";
            strSQL += " CUSTOMER_CONTACT_DTLS cmtdtl,";
            strSQL += " customer_type_mst_tbl ctmt,";
            strSQL += " location_mst_tbl lmt";
            strSQL += " WHERE cmt.customer_type_fk = ctmt.customer_type_pk(+)";
            strSQL += " AND  cmtdtl.CUSTOMER_MST_FK(+) = cmt.customer_mst_pk";
            strSQL += " AND  lmt.location_mst_pk(+) = cmtdtl.ADM_LOCATION_MST_FK ";
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
            strSQL += "(SELECT ROWNUM SR_NO,T.* FROM";
            strSQL += " (SELECT cmt.customer_mst_pk customer_mst_pk,CMT.ACTIVE_FLAG, ";
            strSQL += " cmt.customer_id customer_id, ";
            //strSQL &= vbCrLf & " DECODE(cmt.customer_type,'0','Group','1','Individual') CUSTOMER_GROUP,"
            strSQL += " cmt.customer_name customer_name, ";
            strSQL += " (select c.customer_id from customer_mst_tbl c where cmt.ref_group_cust_pk= c.customer_mst_pk) CUSTOMER_GROUP,";
            strSQL += " ctmt.customer_type_id customer_type, ";
            strSQL += " DECODE(cmt.BUSINESS_TYPE,'1','Air','2','Sea','3','Both','4','Removals') BUSINESS_TYPE,";
            strSQL += " (SELECT q.dd_id FROM qfor_drop_down_tbl q WHERE q.dd_value = cmt.customer_category AND q.config_id='QFOR2083'AND q.dd_flag='CUST_TYPE') CUSTOMER_CATEGORY,";
            if (Client == "BNM")
            {
                strSQL += " (SELECT Q.DD_ID FROM QFOR_DROP_DOWN_TBL Q WHERE Q.DD_VALUE = CMT.GROUP_CATEGORY AND Q.CONFIG_ID = 'QFOR2083' AND Q.DD_FLAG = 'GROUP_CAT') TAX_STATUS,";
            }
            else
            {
                strSQL += " (SELECT q.dd_id FROM qfor_drop_down_tbl q WHERE q.dd_value = cmt.tax_status AND q.config_id='QFOR2083'AND q.dd_flag='TAX_TYPE') TAX_STATUS,";
            }
            strSQL += " lmt.location_id location_id,";
            strSQL += "      CASE";
            strSQL += "         WHEN cmt.credit_customer = 1 THEN";
            strSQL += "          'a'";
            strSQL += "         ELSE";
            strSQL += "          'r'";
            strSQL += "    END CREDIT_CUSTOMER,";
            strSQL += " cmt.Version_No";
            strSQL += " FROM";
            strSQL += " customer_mst_tbl cmt,";
            strSQL += " CUSTOMER_CONTACT_DTLS cmtdtl,";
            strSQL += " customer_type_mst_tbl ctmt,";
            strSQL += " location_mst_tbl lmt";
            strSQL += " WHERE cmt.customer_type_fk = ctmt.customer_type_pk(+)";
            strSQL += " AND  cmtdtl.CUSTOMER_MST_FK(+) = cmt.customer_mst_pk";
            strSQL += " AND  lmt.location_mst_pk = cmtdtl.ADM_LOCATION_MST_FK ";
            // strSQL &= vbCrLf & " AND cmt.location_mst_fk = " & CType(objPage.Session.Item("LOGED_IN_LOC_FK"), Int64) & " "

            if (Credit > 0)
            {
                if (Credit == 1)
                {
                    strSQL += " and nvl(cmt.CREDIT_CUSTOMER, 0) = 1 ";
                }
                else
                {
                    strSQL += " and nvl(cmt.CREDIT_CUSTOMER, 0) = 0 ";
                }
            }

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
                //Manjunath  PTS ID:Sep-02  17/09/2011
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

        #region "FetchAll Customer Function"

        public DataSet fetchSelected(long pk)
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            strSQL = "DELETE FROM CUSTOMER_CATEGORY_TRN CCT WHERE CCT.CUSTOMER_MST_FK=" + pk;
            strSQL = objWF.ExecuteScaler(strSQL);
            strSQL = "";
            strSQL = "select customer_category_PK,Customer_Mst_Fk,Customer_Category_Mst_FK,version_No from customer_category_trn Where Customer_Mst_FK=" + pk;

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

        public DataSet FetchAll(long CustomerPK)
        {
            string strSQL = null;
            strSQL = string.Empty;
            strSQL += "SELECT ROWNUM SR_NO,  ";
            strSQL += " CMT.Customer_Mst_Pk Customer_Mst_Pk, ";
            strSQL += " CMT.Customer_Id Customer_Id, ";
            strSQL += " CMT.Customer_Name Customer_Name, ";
            strSQL += " CMT.cust_corporate_mst_fk cust_corporate_mst_fk, ";
            strSQL += " CMT.Customer_Name Customer_Corp_Name, ";
            strSQL += " CMT.CUSTOMER_TYPE_FK, ";
            strSQL += " CTMT.CUSTOMER_TYPE_id CUSTOMER_TYPE, ";
            strSQL += " CMT.location_type_mst_fk location_type_mst_fk, ";
            strSQL += " LTMT.location_type_id location_type_id, ";
            strSQL += " CMT.Customer_Type_Fk Customer_Type_Fk, ";
            strSQL += " CMTp.Customer_Mst_Fk Parent_Customer_Fk, ";
            strSQL += " CMTp.customer_id Group_ID, ";
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
            strSQL += " FROM CUSTOMER_MST_TBL CMT, ";
            strSQL += "  CUSTOMER_TYPE_MST_TBL CTMT, ";
            strSQL += "  location_type_mst_tbl LTMT, ";
            strSQL += "  LOCATION_MST_TBL LMT, ";
            strSQL += "  employee_mst_tbl Emp, ";
            strSQL += "  COUNTRY_MST_TBL CoMT, ";
            strSQL += "  CUSTOMER_MST_TBL CMTp  ";
            strSQL += "  WHERE CMT.CUSTOMER_TYPE_FK=CTMT.CUSTOMER_TYPE_PK  ";
            strSQL += "   AND CMT.LOCATION_MST_FK=LMT.LOCATION_MST_PK ";
            strSQL += "   AND CMT.country_mst_fk = CoMT.country_mst_pk ";
            strSQL += "   AND CMT.LOCATION_TYPE_MST_FK = LTMT.LOCATION_TYPE_MST_PK ";
            strSQL += "   AND CMT.EMPLOYEE_MST_FK = Emp.EMPLOYEE_MST_PK ";
            strSQL += "   AND CMTp.customer_mst_pk(+) = cmt.customer_mst_fk";
            strSQL += "   AND CMT.Customer_Mst_Pk = " + CustomerPK;
            strSQL += "  Order by CMT.Customer_Id ";
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

        public DataSet Fetch_Customer(Int16 CustomerPK = 0, string CustomerID = "", string CustomerName = "", string Condition = "", Int64 ExecutiveId = 0, Int64 LoggedLocPK = 0)
        {
            string strSQL = null;
            strSQL = "select ' '  CUSTOMER_ID,' ' CUSTOMER_NAME,0 Customer_MST_PK from dual ";
            strSQL = strSQL + " UNION ";
            strSQL = strSQL + "select  CUSTOMER_ID,CUSTOMER_NAME,Customer_MST_PK from Customer_mst_tbl";
            strSQL = strSQL + " Where 1=1 ";

            if (LoggedLocPK > 0)
            {
                strSQL = strSQL + " and Customer_mst_tbl.LOCATION_MST_FK= " + LoggedLocPK;
            }

            if (ExecutiveId > 0)
            {
                strSQL = strSQL + " and  EMPLOYEE_MST_FK =" + ExecutiveId.ToString().Trim() + " ";
            }
            if (Condition.Trim().Length > 0)
            {
                strSQL = strSQL + " and  Customer_MST_PK not in (" + Condition.Trim() + ")";
            }

            strSQL = strSQL + " order by CUSTOMER_ID";

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

        public DataSet FetchForPortCustomer(Int16 CustomerPK = 0, string CustomerID = "", string CustomerName = "", string Condition = "", Int64 ExecutiveId = 1, Int64 PortPK = 0)
        {
            string strSQL = null;
            strSQL = "select ' '  CUSTOMER_ID,' ' CUSTOMER_NAME,0 Customer_MST_PK from dual ";
            strSQL = strSQL + " UNION ";
            strSQL = strSQL + "select  CUSTOMER_ID,CUSTOMER_NAME,Customer_MST_PK from Customer_mst_tbl ";
            strSQL = strSQL + " Where 1=1 and Customer_mst_tbl.LOCATION_MST_FK IN ";
            strSQL = strSQL + " (select lwp.location_mst_fk from location_working_ports_trn lwp where";
            strSQL = strSQL + " lwp.port_mst_fk = " + PortPK + ")";

            if (ExecutiveId > 0)
            {
                strSQL = strSQL + " and  EMPLOYEE_MST_FK =" + ExecutiveId.ToString().Trim() + " ";
            }
            if (Condition.Trim().Length > 0)
            {
                strSQL = strSQL + " and  Customer_MST_PK not in (" + Condition.Trim() + ")";
            }

            strSQL = strSQL + " order by CUSTOMER_ID";

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


        #endregion

        #region "Save"

        public ArrayList Save(DataSet M_DataSet, bool DelFlg = false)
        {
            return new ArrayList();
        }

        #endregion

        #region "Save Category"
        public ArrayList SaveCategory(DataSet M_DataSet, string customer_mst_pk, bool DelFlg = false)
        {
            return new ArrayList();
        }
        #endregion

        #region "Insert Function"
        public long Insert(WorkFlow objWS)
        {
            System.DBNull MYNULL = null;
            long intPkVal = 0;
            objWS.MyCommand.CommandType = CommandType.StoredProcedure;
            var _with7 = objWS.MyCommand.Parameters;
            _with7.Add("Customer_Mst_Fk_IN", M_Customer_Mst_Fk).Direction = ParameterDirection.Input;
            _with7.Add("Customer_Type_Fk_IN", M_Customer_Type_Fk).Direction = ParameterDirection.Input;
            _with7.Add("Customer_Id_IN", M_Customer_Id).Direction = ParameterDirection.Input;
            _with7.Add("Customer_Name_IN", M_Customer_Name).Direction = ParameterDirection.Input;
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
            objWS.MyCommand.CommandText = objWS.MyUserName + ".CUSTOMER_MST_TBL_PKG.CUSTOMER_MST_TBL_Ins";
            if (objWS.ExecuteTransactionCommands() == true)
            {
                return intPkVal;
            }
            else
            {
                return -1;
            }
        }
        #endregion

        #region "Update Function"
        public int Update(WorkFlow objWS)
        {
            Int32 intPkVal = default(Int32);
            objWS.MyCommand.CommandType = CommandType.StoredProcedure;
            var _with8 = objWS.MyCommand.Parameters;
            _with8.Add("Customer_Mst_Pk_IN", M_Customer_Mst_Pk).Direction = ParameterDirection.Input;
            _with8.Add("Customer_Mst_Fk_IN", M_Customer_Mst_Fk).Direction = ParameterDirection.Input;
            _with8.Add("Customer_Type_Fk_IN", M_Customer_Type_Fk).Direction = ParameterDirection.Input;
            _with8.Add("Customer_Id_IN", M_Customer_Id).Direction = ParameterDirection.Input;
            _with8.Add("Customer_Name_IN", M_Customer_Name).Direction = ParameterDirection.Input;
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

            objWS.MyCommand.CommandText = objWS.MyUserName + ".CUSTOMER_MST_TBL_PKG.CUSTOMER_MST_TBL_UPD";
            if (objWS.ExecuteTransactionCommands() == true)
            {
                return 1;
            }
            else
            {
                return -1;
            }
        }
        #endregion

        #region "Delete Function"
        public int Delete()
        {
            WorkFlow objWS = new WorkFlow();
            Int32 intPkVal = default(Int32);
            try
            {
                objWS.MyCommand.CommandType = CommandType.StoredProcedure;
                var _with9 = objWS.MyCommand.Parameters;
                _with9.Add("Customer_Mst_Pk_IN", M_Customer_Mst_Pk).Direction = ParameterDirection.Input;
                _with9.Add("Version_No_IN", M_VERSION_NO).Direction = ParameterDirection.Input;
                objWS.MyCommand.CommandText = objWS.MyUserName + ".CUSTOMER_MST_TBL_PKG.CUSTOMER_MST_TBL_DEL";
                if (objWS.ExecuteCommands() == true)
                {
                    return 1;
                }
                else
                {
                    return -1;
                }
                //Manjunath  PTS ID:Sep-02  17/09/2011
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

        #region "Delete"
        public ArrayList Delete(ArrayList DeletedRow)
        {
            WorkFlow obJWk = new WorkFlow();
            OracleTransaction oraTran = null;
            OracleCommand delCommand = new OracleCommand();
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
                    _with10.CommandText = obJWk.MyUserName + ".CUSTOMER_MST_TBL_PKG.CUSTOMER_MST_TBL_DEL";
                    arrRowDetail = DeletedRow[i].ToString().Split(',');
                    _with10.Parameters.Clear();
                    var _with11 = _with10.Parameters;
                    _with11.Add("CUSTOMER_MST_PK_IN", arrRowDetail[0]).Direction = ParameterDirection.Input;
                    _with11.Add("DELETED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                    _with11.Add("VERSION_NO_IN", arrRowDetail[1]).Direction = ParameterDirection.Input;
                    _with11.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                    _with11.Add("RETURN_VALUE", strReturn).Direction = ParameterDirection.Output;
                    obJWk.MyCommand.Parameters["RETURN_VALUE"].OracleDbType = OracleDbType.Varchar2;
                    obJWk.MyCommand.Parameters["RETURN_VALUE"].Size = 50;
                    _with10.ExecuteNonQuery();
                   
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
                //Manjunath
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
        #endregion

        #region "Fetch Customers-Organorgram"
        public DataSet FetchOGCustomers(long LocMstPk = 0, long CustPK = 0, string CustID = "", string CustName = "", string BusiModel = "", string SearchType = "", string SortExpression = "")
        {

            string strSQL = null;
            string strCondition = null;
            System.Web.UI.Page objPage = new System.Web.UI.Page();

           
            if (CustPK > 0)
            {
                strCondition += " And cust.customer_mst_pk =" + CustPK;
            }

            if (CustID.ToString().Trim().Length > 0 & SearchType == "C")
            {
                strCondition += " And UPPER(cust.customer_id) like '%" + CustID.ToUpper().Trim().Replace("'", "''") + "%' ";
            }
            else if (CustID.ToString().Trim().Length > 0 & SearchType == "S")
            {
                strCondition += " And UPPER(cust.customer_id) like '" + CustID.ToUpper().Trim().Replace("'", "''") + "%' ";
            }

            if (CustName.ToString().Trim().Length > 0 & SearchType == "C")
            {
                strCondition = strCondition + " And UPPER(cust.customer_name) like '%" + CustName.ToUpper().Trim().Replace("'", "''") + "%' ";
            }
            else if (CustName.ToString().Trim().Length > 0 & SearchType == "S")
            {
                strCondition = strCondition + " And UPPER(cust.customer_name) like '" + CustName.ToUpper().Trim().Replace("'", "''") + "%' ";
            }

            strSQL = "select ROWNUM SR_NO,q.* from (";
            strSQL += " SELECT  cust.customer_mst_pk, ";
            strSQL += " cust.cust_corporate_mst_fk, ";
            strSQL += " cust.customer_id,";
            strSQL += " cust.customer_name,";
            strSQL += " cust.customer_type_fk,";
            strSQL += " cust.location_mst_fk,";
            strSQL += " loc.location_name Location,";
            strSQL += " loctype.location_type_desc BranchType,";
            strSQL += " cutype.customer_type_id CustomerType,";
            strSQL += " cust.version_no FROM ";
            strSQL += " customer_mst_tbl cust,";
            strSQL += " customer_type_mst_tbl cutype,";
            strSQL += " location_mst_tbl loc,";
            strSQL += " location_type_mst_tbl loctype WHERE";
            strSQL += " cust.customer_type_fk=cutype.customer_type_pk";
            strSQL += " AND cust.location_mst_fk=loc.location_mst_pk";
            strSQL += " AND loc.location_type_fk=loctype.location_type_mst_pk";
            // strSQL &= vbCrLf & " AND loc.location_mst_pk=" & CType(objPage.Session.Item("LOGED_IN_LOC_FK"), Int64) & " "
            strSQL += " AND loc.location_mst_pk=" + LocMstPk + " ";

            if (BusiModel != "All")
            {
                strSQL += "AND UPPER(cutype.CUSTOMER_TYPE_id) LIKE '" + BusiModel + "%'";
            }
            strSQL += strCondition;
            strSQL += " order by cust.customer_id)q";
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
                //Manjunath  PTS ID:Sep-02  17/09/2011
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

        #region "Fetch Other Agents"
        public DataSet FetchOtherAgents(long CorpPK = 0)
        {

            string strSQL = null;
            string strCondition = null;
            System.Web.UI.Page objPage = new System.Web.UI.Page();


            strSQL = "select ROWNUM SR_NO,q.* from (";
            strSQL += " SELECT  cust.customer_mst_pk, ";
            strSQL += " cust.cust_corporate_mst_fk, ";
            strSQL += " cust.customer_id,";
            strSQL += " cust.customer_name,";
            strSQL += " cust.customer_type_fk,";
            strSQL += " cust.location_mst_fk,";
            strSQL += " loc.location_name Location,";
            strSQL += " loctype.location_type_desc BranchType,";
            strSQL += " cutype.customer_type_id CustomerType,";
            strSQL += " cust.version_no FROM ";
            strSQL += " customer_mst_tbl cust,";
            strSQL += " customer_type_mst_tbl cutype,";
            strSQL += " location_mst_tbl loc,";
            strSQL += " location_type_mst_tbl loctype WHERE";
            strSQL += " cust.customer_type_fk=cutype.customer_type_pk";
            strSQL += " AND cust.location_mst_fk=loc.location_mst_pk";
            strSQL += " AND loc.location_type_fk=loctype.location_type_mst_pk";
            strSQL += " AND cust.cust_corporate_mst_fk=" + CorpPK;
            strSQL += " order by cust.customer_id)q";
            
            try
            {
                return (new WorkFlow()).GetDataSet(strSQL);
                //Manjunath  PTS ID:Sep-02  17/09/2011
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

        #region "Fetch Location"
        public OracleDataReader FetchLocation()
        {
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
            return null;
        }
        #endregion

        #region "Fetch Customer Type"
        public DataSet FetchCustType()
        {
            string strSQL = null;
            strSQL = "select ' '  CUSTOMER_TYPE_ID,' ' CUSTOMER_TYPE_NAME,0 CUSTOMER_TYPE_PK from dual ";
            strSQL = strSQL + " UNION ";
            strSQL = strSQL + "select  CUSTOMER_TYPE_ID,CUSTOMER_TYPE_NAME,CUSTOMER_TYPE_PK from customer_type_mst_tbl ctmt where ACTIVE_FLAG = 1";
            try
            {
                return (new WorkFlow()).GetDataSet(strSQL);
                //Manjunath  PTS ID:Sep-02  17/09/2011
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

        #region " Enhance Search "
        // This enhance search is for displaying Customer list according to provided customer category PK
        // Rajesh 19-Jan-2006
        public string FetchCustomerForCategory(string strCond)
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
            arr = strCond.Split('~');;
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
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
                SCM.CommandText = objWF.MyUserName + ".EN_CUSTOMER_PKG.GET_CUSTOMER_CAT_COMMON";
                var _with12 = SCM.Parameters;
                _with12.Add("SEARCH_CATPK_IN", ifDBNull(strSEARCH_CAT_PK_IN)).Direction = ParameterDirection.Input;
                _with12.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with12.Add("LOCATION_MST_FK_IN", strLOCATION_IN).Direction = ParameterDirection.Input;
                _with12.Add("BUSINESS_TYPE_IN", intBUSINESS_TYPE_IN).Direction = ParameterDirection.Input;
                _with12.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with12.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
                //Manjunath  PTS ID:Sep-02  17/09/2011
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
                SCM.Connection.Close();
            }

        }

        #endregion

        #region " Supporting Function "

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

        private object removeDBNull(object col)
        {
            if (object.ReferenceEquals(col, DBNull.Value))
            {
                return "";
            }
            return col;
        }

        #endregion

        //
        // This function returns primary key for customer category ('SHIPPER','NOTIFY')
        // Author : Akhilesh Kumar
        // Used in : Split BL
        #region " Fetch Customer Category Pk for ('SHIPPER','NOTIFY') "
        public OracleDataReader FetchCustomerCategory()
        {
            System.Text.StringBuilder Sql = new System.Text.StringBuilder();
            try
            {
                Sql.Append("SELECT C.CUSTOMER_CATEGORY_MST_PK FROM ");
                Sql.Append("CUSTOMER_CATEGORY_MST_TBL C ");
                Sql.Append("WHERE UPPER(C.CUSTOMER_CATEGORY_ID) IN ('SHIPPER','NOTIFY') ");

                return (new WorkFlow()).GetDataReader(Sql.ToString());
                //Manjunath  PTS ID:Sep-02  17/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return null;
        }
        #endregion
        //This  Function return The  Sr_no,Custmer_PK,Customer_ID,Customer_name From Customer Master Table Based On the Location
        //Author :Mani.SureshKumar
        //Used in : Customer Selection Look Up Screen

        #region "FetchCustomerName"
        public DataSet FetchCustomerName(string Process, string selectPK = "0", string Biztype = "3", string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, long LocFk = 0, bool CustGrp = false, string frmForm = null, string SrchCtra = null)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            StringBuilder strSQLBuilder = new StringBuilder();
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            StringBuilder strCount = new StringBuilder();
            Array arr = null;
            if ((SrchCtra != null))
            {
                arr = SrchCtra.Split('~');;
            }
            selectPK = selectPK.TrimEnd(',');
            //Grid Selection 
            // LocFk = 0  Its Display all the Customers
            // LocFk != 0 Based on the Location display the Customers
            if ((frmForm != null))
            {
                if (frmForm == "Ageing")
                {
                    strSQL.Append(" SELECT * FROM ");
                    strSQL.Append("( SELECT DISTINCT ");
                    strSQL.Append(" CUST.CUSTOMER_MST_PK,");
                    strSQL.Append(" CUST.CUSTOMER_ID,");
                    strSQL.Append(" CUST.CUSTOMER_NAME,");
                    strSQL.Append(" '0' CHK");
                    strSQL.Append(" FROM ");
                    strSQL.Append(" CUSTOMER_MST_TBL CUST where 1=1");
                    if (CustGrp == true)
                    {
                        strSQL.Append(" AND CUST.GROUP_HEADER = 1 ");
                    }
                    if (selectPK == null | string.IsNullOrEmpty(selectPK))
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK NOT IN (0) ");
                    }
                    else
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK NOT IN (" + selectPK + " ) ");
                    }
                    if (arr.Length > 0)
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK in(");
                        if (CustGrp == true)
                        {
                            strSQL.Append("select c.ref_group_cust_pk from customer_mst_tbl c where c.customer_mst_pk in( ");
                        }
                        strSQL.Append(" SELECT T.CUSTOMER_MST_FK FROM VIEW_AGEING_RPT T WHERE 1=1 AND T.OUTSTD_DAYS>0");
                        strSQL.Append(" AND (SELECT (GET_EX_RATE(CIT.CURRENCY_MST_FK, " + Convert.ToInt32(arr.GetValue(12)) + ", CIT.INVOICE_DATE)* (CIT.NET_RECEIVABLE - NVL(CIT.TOTAL_CREDIT_NOTE_AMT,0)))");
                        strSQL.Append("  - NVL((SELECT SUM(GET_EX_RATE(COLL.CURRENCY_MST_FK, " + Convert.ToInt32(arr.GetValue(12)) + ", COLL.COLLECTIONS_DATE)* NVL(COLT.RECD_AMOUNT_HDR_CURR,0))");
                        strSQL.Append("  FROM COLLECTIONS_TBL COLL, COLLECTIONS_TRN_TBL COLT");
                        strSQL.Append("  WHERE COLL.COLLECTIONS_TBL_PK = COLT.COLLECTIONS_TBL_FK");
                        strSQL.Append("  AND COLT.INVOICE_REF_NR = CIT.INVOICE_REF_NO");
                        strSQL.Append("  ) ,0)");
                        strSQL.Append("  FROM CONSOL_INVOICE_TBL CIT WHERE CIT.CONSOL_INVOICE_PK=T.CONSOL_INVOICE_PK)>0");
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(0))))
                        {
                            strSQL.Append("  AND T.DEFAULT_LOCATION_FK IN(" + Convert.ToString(arr.GetValue(0)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(1))))
                        {
                            strSQL.Append("  AND T.COUNTRY_MST_FK IN(" + Convert.ToString(arr.GetValue(1)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(2))))
                        {
                            strSQL.Append("  AND T.CUSTOMER_MST_FK IN(" + Convert.ToString(arr.GetValue(2)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(3))))
                        {
                            strSQL.Append("  AND T.POLFK IN(" + Convert.ToString(arr.GetValue(3)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(4))))
                        {
                            strSQL.Append("  AND T.PODFK IN(" + Convert.ToString(arr.GetValue(4)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5))))
                        {
                            strSQL.Append(" AND TO_DATE(T.INVOICE_DATE,'DD/MM/YYYY') <= TO_DATE('" + Convert.ToString(arr.GetValue(5)) + "','DD/MM/YYYY')");
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(6))) > 0)
                        {
                            strSQL.Append("  AND T.PROCESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(6))));
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(7))) > 0 & Convert.ToInt32(Convert.ToString(arr.GetValue(7))) != 3)
                        {
                            strSQL.Append("  AND T.BUSINESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(7))));
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(8))) > 0)
                        {
                            strSQL.Append("  AND T.CARGO_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(8))));
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(9))) > 0)
                        {
                            strSQL.Append("  AND T.JOB_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(9))));
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(10))) > 0)
                        {
                            strSQL.Append("  AND T.CONSOL_INVOICE_PK =" + Convert.ToInt32(Convert.ToString(arr.GetValue(10))));
                        }
                        strSQL.Append(")");
                        if (CustGrp == true)
                        {
                            strSQL.Append(")");
                        }
                    }
                    strSQL.Append(" UNION ");
                    strSQL.Append(" SELECT DISTINCT");
                    strSQL.Append(" CUST.CUSTOMER_MST_PK,");
                    strSQL.Append(" CUST.CUSTOMER_ID,");
                    strSQL.Append(" CUST.CUSTOMER_NAME,");
                    strSQL.Append(" '1' CHK");
                    strSQL.Append(" FROM ");
                    strSQL.Append(" CUSTOMER_MST_TBL CUST where 1=1");
                    if (arr.Length > 0)
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK in(");
                        if (CustGrp == true)
                        {
                            strSQL.Append("select c.ref_group_cust_pk from customer_mst_tbl c where c.customer_mst_pk in( ");
                        }
                        strSQL.Append(" SELECT T.CUSTOMER_MST_FK FROM VIEW_AGEING_RPT T WHERE 1=1 AND T.OUTSTD_DAYS>0");
                        strSQL.Append(" AND (SELECT (GET_EX_RATE(CIT.CURRENCY_MST_FK, " + Convert.ToInt32(Convert.ToString(arr.GetValue(12))) + ", CIT.INVOICE_DATE)* (CIT.NET_RECEIVABLE - NVL(CIT.TOTAL_CREDIT_NOTE_AMT,0)))");
                        strSQL.Append("  - NVL((SELECT SUM(GET_EX_RATE(COLL.CURRENCY_MST_FK, " + Convert.ToInt32(Convert.ToString(arr.GetValue(12))) + ", COLL.COLLECTIONS_DATE)* NVL(COLT.RECD_AMOUNT_HDR_CURR,0))");
                        strSQL.Append("  FROM COLLECTIONS_TBL COLL, COLLECTIONS_TRN_TBL COLT");
                        strSQL.Append("  WHERE COLL.COLLECTIONS_TBL_PK = COLT.COLLECTIONS_TBL_FK");
                        strSQL.Append("  AND COLT.INVOICE_REF_NR = CIT.INVOICE_REF_NO");
                        strSQL.Append("  ) ,0)");
                        strSQL.Append("  FROM CONSOL_INVOICE_TBL CIT WHERE CIT.CONSOL_INVOICE_PK=T.CONSOL_INVOICE_PK)>0");
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(0))))
                        {
                            strSQL.Append("  AND T.DEFAULT_LOCATION_FK IN(" + Convert.ToString(arr.GetValue(0)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(1))))
                        {
                            strSQL.Append("  AND T.COUNTRY_MST_FK IN(" + Convert.ToString(arr.GetValue(1))+ ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(2))))
                        {
                            strSQL.Append("  AND T.CUSTOMER_MST_FK IN(" + Convert.ToString(arr.GetValue(2)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(3))))
                        {
                            strSQL.Append("  AND T.POLFK IN(" + Convert.ToString(arr.GetValue(3)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(4))))
                        {
                            strSQL.Append("  AND T.PODFK IN(" + Convert.ToString(arr.GetValue(4)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5))))
                        {
                            strSQL.Append(" AND TO_DATE(T.INVOICE_DATE,'DD/MM/YYYY') <= TO_DATE('" + Convert.ToString(arr.GetValue(5)) + "','DD/MM/YYYY')");
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(6))) > 0)
                        {
                            strSQL.Append("  AND T.PROCESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(6))));
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(7))) > 0 & Convert.ToInt32(Convert.ToString(arr.GetValue(7))) != 3)
                        {
                            strSQL.Append("  AND T.BUSINESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(7))));
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(8))) > 0)
                        {
                            strSQL.Append("  AND T.CARGO_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(8))));
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(9))) > 0)
                        {
                            strSQL.Append("  AND T.JOB_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(9))));
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(10))) > 0)
                        {
                            strSQL.Append("  AND T.CONSOL_INVOICE_PK =" + Convert.ToInt32(Convert.ToString(arr.GetValue(10))));
                        }
                        strSQL.Append(")");
                        if (CustGrp == true)
                        {
                            strSQL.Append(")");
                        }
                    }
                    if (selectPK == null | string.IsNullOrEmpty(selectPK))
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK IN (0)");
                        if (CustGrp == true)
                        {
                            strSQL.Append(" AND CUST.GROUP_HEADER = 1 ");
                        }
                        strSQL.Append(" ) T ORDER BY CUSTOMER_ID,CHK DESC ");
                    }
                    else
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK IN (" + selectPK + ") ");
                        if (CustGrp == true)
                        {
                            strSQL.Append(" AND CUST.GROUP_HEADER = 1 ");
                        }
                        strSQL.Append(" ) T  ORDER BY CUSTOMER_ID,CHK DESC ");
                    }
                }
                else if (frmForm == "CustomerProfit")
                {
                    strSQL.Append(" SELECT * FROM ");
                    strSQL.Append("( SELECT DISTINCT ");
                    strSQL.Append(" CUST.CUSTOMER_MST_PK,");
                    strSQL.Append(" CUST.CUSTOMER_ID,");
                    strSQL.Append(" CUST.CUSTOMER_NAME,");
                    strSQL.Append(" '0' CHK");
                    strSQL.Append(" FROM ");
                    strSQL.Append(" CUSTOMER_MST_TBL CUST where 1=1");
                    if (CustGrp == true)
                    {
                        strSQL.Append(" AND CUST.GROUP_HEADER = 1 ");
                    }
                    if (selectPK == null | string.IsNullOrEmpty(selectPK))
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK NOT IN (0) ");
                    }
                    else
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK NOT IN (" + selectPK + ")");
                    }
                    if (arr.Length > 0)
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK IN(");
                        if (CustGrp == true)
                        {
                            strSQL.Append("SELECT C.REF_GROUP_CUST_PK FROM CUSTOMER_MST_TBL C WHERE C.CUSTOMER_MST_PK IN( ");
                        }
                        strSQL.Append(" SELECT T.CUSTFK FROM VIEW_CUSTPROFIT_RPT T WHERE 1=1");

                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(0))))
                        {
                            strSQL.Append("  AND T.DEFAULT_LOCATION_FK IN(" + Convert.ToString(arr.GetValue(0)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(1))))
                        {
                            strSQL.Append("  AND T.CUSTFK IN(" + Convert.ToString(arr.GetValue(1)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(2))))
                        {
                            strSQL.Append("  AND T.JCPK =" + Convert.ToString(arr.GetValue(2)));
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(3))))
                        {
                            strSQL.Append("  AND T.POLFK IN(" + Convert.ToString(arr.GetValue(3)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(4))))
                        {
                            strSQL.Append("  AND T.PODFK IN(" + Convert.ToString(arr.GetValue(4)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(6)).Trim()))
                        {
                            strSQL.Append(" AND TO_DATE(T.TRANSDATE,'DD/MM/YYYY') BETWEEN TO_DATE('" + Convert.ToString(arr.GetValue(5)) + "','DD/MM/YYYY') AND TO_DATE('" + Convert.ToString(arr.GetValue(6)) + "','DD/MM/YYYY')");
                        }
                        else if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()) & string.IsNullOrEmpty(Convert.ToString(arr.GetValue(6)).Trim()))
                        {
                            strSQL.Append(" AND TO_DATE(T.TRANSDATE,'DD/MM/YYYY') >= TO_DATE('" + Convert.ToString(arr.GetValue(5)) + "','DD/MM/YYYY')");
                        }
                        else if (string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(6)).Trim()))
                        {
                            strSQL.Append(" AND TO_DATE(T.TRANSDATE,'DD/MM/YYYY') <= TO_DATE('" + Convert.ToString(arr.GetValue(6)) + "','DD/MM/YYYY')");
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(7))) > 0)
                        {
                            strSQL.Append("  AND T.PROCESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(7))));
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(8))) > 0 & Convert.ToInt32(Convert.ToString(arr.GetValue(8))) != 3)
                        {
                            strSQL.Append("  AND T.BUSINESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(8))));
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(9))) > 0)
                        {
                            strSQL.Append("  AND T.CARGO_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(9))));
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(10))) > 0)
                        {
                            strSQL.Append("  AND T.JOB_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(10))));
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(11))) > 0)
                        {
                            strSQL.Append("  AND T.COMM_GRP_GK =" + Convert.ToInt32(Convert.ToString(arr.GetValue(11))));
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(12))))
                        {
                            strSQL.Append("  AND T.REF_GROUP_CUST_PK IN(" + Convert.ToString(arr.GetValue(12)) + ")");
                        }
                        strSQL.Append(")");
                        if (CustGrp == true)
                        {
                            strSQL.Append(")");
                        }
                    }
                    strSQL.Append(" UNION ");
                    strSQL.Append(" SELECT DISTINCT");
                    strSQL.Append(" CUST.CUSTOMER_MST_PK,");
                    strSQL.Append(" CUST.CUSTOMER_ID,");
                    strSQL.Append(" CUST.CUSTOMER_NAME,");
                    strSQL.Append(" '1' CHK");
                    strSQL.Append(" FROM ");
                    strSQL.Append(" CUSTOMER_MST_TBL CUST where 1=1");
                    if (arr.Length > 0)
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK IN(");
                        if (CustGrp == true)
                        {
                            strSQL.Append("SELECT C.REF_GROUP_CUST_PK FROM CUSTOMER_MST_TBL C WHERE C.CUSTOMER_MST_PK IN( ");
                        }
                        strSQL.Append(" SELECT T.CUSTFK FROM VIEW_CUSTPROFIT_RPT T WHERE 1=1");

                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(0))))
                        {
                            strSQL.Append("  AND T.DEFAULT_LOCATION_FK IN(" + Convert.ToString(arr.GetValue(0)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(1))))
                        {
                            strSQL.Append("  AND T.CUSTFK IN(" + Convert.ToString(arr.GetValue(1)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(2))))
                        {
                            strSQL.Append("  AND T.JCPK =" + Convert.ToString(arr.GetValue(2)));
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(3))))
                        {
                            strSQL.Append("  AND T.POLFK IN(" + Convert.ToString(arr.GetValue(3)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(4))))
                        {
                            strSQL.Append("  AND T.PODFK IN (" + Convert.ToString(arr.GetValue(4)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(6)).Trim()))
                        {
                            strSQL.Append(" AND TO_DATE(T.TRANSDATE,'DD/MM/YYYY') BETWEEN TO_DATE('" + Convert.ToString(arr.GetValue(5)) + "','DD/MM/YYYY') AND TO_DATE('" + Convert.ToString(arr.GetValue(6)) + "','DD/MM/YYYY')");
                        }
                        else if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()) & string.IsNullOrEmpty(Convert.ToString(arr.GetValue(6)).Trim()))
                        {
                            strSQL.Append(" AND TO_DATE(T.TRANSDATE,'DD/MM/YYYY') >= TO_DATE('" + Convert.ToString(arr.GetValue(5)) + "','DD/MM/YYYY')");
                        }
                        else if (string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(6)).Trim()))
                        {
                            strSQL.Append(" AND TO_DATE(T.TRANSDATE,'DD/MM/YYYY') <= TO_DATE('" + Convert.ToString(arr.GetValue(6)) + "','DD/MM/YYYY')");
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(7))) > 0)
                        {
                            strSQL.Append("  AND T.PROCESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(7))));
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(8))) > 0 & Convert.ToInt32(Convert.ToString(arr.GetValue(8))) != 3)
                        {
                            strSQL.Append("  AND T.BUSINESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(8))));
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(9))) > 0)
                        {
                            strSQL.Append("  AND T.CARGO_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(9))));
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(10))) > 0)
                        {
                            strSQL.Append("  AND T.JOB_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(10))));
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(11))) > 0)
                        {
                            strSQL.Append("  AND T.COMM_GRP_GK =" + Convert.ToInt32(Convert.ToString(arr.GetValue(11))));
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(12))))
                        {
                            strSQL.Append("  AND T.REF_GROUP_CUST_PK IN(" + Convert.ToString(arr.GetValue(12)) + ")");
                        }
                        strSQL.Append(")");
                        if (CustGrp == true)
                        {
                            strSQL.Append(")");
                        }
                    }
                    if (selectPK == null | string.IsNullOrEmpty(selectPK))
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK IN (0)");
                        if (CustGrp == true)
                        {
                            strSQL.Append(" AND CUST.GROUP_HEADER = 1 ");
                        }
                        strSQL.Append(" ) T ORDER BY CUSTOMER_ID,CHK DESC ");
                    }
                    else
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK IN (" + selectPK + ") ");
                        if (CustGrp == true)
                        {
                            strSQL.Append(" AND CUST.GROUP_HEADER = 1 ");
                        }
                        strSQL.Append(" ) T  ORDER BY CUSTOMER_ID,CHK DESC ");
                    }
                }
                else if (frmForm == "FRTOUT")
                {
                    strSQL.Append(" SELECT * FROM ");
                    strSQL.Append("( SELECT DISTINCT ");
                    strSQL.Append(" CUST.CUSTOMER_MST_PK,");
                    strSQL.Append(" CUST.CUSTOMER_ID,");
                    strSQL.Append(" CUST.CUSTOMER_NAME,");
                    strSQL.Append(" '0' CHK");
                    strSQL.Append(" FROM ");
                    strSQL.Append(" CUSTOMER_MST_TBL CUST where 1=1");
                    if (CustGrp == true)
                    {
                        strSQL.Append(" AND CUST.GROUP_HEADER = 1 ");
                    }
                    if (selectPK == null | string.IsNullOrEmpty(selectPK))
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK NOT IN (0) ");
                    }
                    else
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK NOT IN (" + selectPK + ")");
                    }
                    if (arr.Length > 0)
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK IN(");
                        if (CustGrp == true)
                        {
                            strSQL.Append("SELECT C.REF_GROUP_CUST_PK FROM CUSTOMER_MST_TBL C WHERE C.CUSTOMER_MST_PK IN( ");
                        }
                        strSQL.Append(" SELECT T.CUSTFK FROM VIEW_FRTOUT_RPT T WHERE 1=1");
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(0))))
                        {
                            strSQL.Append("  AND (FETCH_FRIEGHT_OUTSTANDING.GETINVOICES(T.JCPK,T.BUSINESS_TYPE," + Convert.ToInt32(Convert.ToString(arr.GetValue(0))) + ",T.JOB_TYPE) - (FETCH_FRIEGHT_OUTSTANDING.GETCOLLECTIONS(T.JCPK,T.BUSINESS_TYPE," + Convert.ToInt32(Convert.ToString(arr.GetValue(0))) + ")))>0");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(1))))
                        {
                            strSQL.Append("  AND T.DEFAULT_LOCATION_FK IN(" + Convert.ToString(arr.GetValue(1)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(2))))
                        {
                            strSQL.Append("  AND T.POLFK IN(" + Convert.ToString(arr.GetValue(2)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(3))))
                        {
                            strSQL.Append("  AND T.PODFK IN(" + Convert.ToString(arr.GetValue(3)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(4))))
                        {
                            strSQL.Append("  AND T.CUSTFK IN(" + Convert.ToString(arr.GetValue(4)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()))
                        {
                            strSQL.Append("  AND UPPER(T.VSL_ID)='" + Convert.ToString(arr.GetValue(5)).ToUpper() + "'");
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(6))) > 0)
                        {
                            strSQL.Append("  AND T.JOB_TYPE =" + Convert.ToString(arr.GetValue(6)));
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(7))) > 0 & Convert.ToInt32(Convert.ToString(arr.GetValue(7))) != 3)
                        {
                            strSQL.Append("  AND T.BUSINESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(7))));
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(8))) > 0)
                        {
                            strSQL.Append("  AND T.PROCESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(8))));
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(9)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(10)).Trim()))
                        {
                            strSQL.Append(" AND TO_DATE(T.TRANSDATE,'DD/MM/YYYY') BETWEEN TO_DATE('" + Convert.ToString(arr.GetValue(9)) + "','DD/MM/YYYY') AND TO_DATE('" + Convert.ToString(arr.GetValue(10)) + "','DD/MM/YYYY')");
                        }
                        else if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(9)).Trim()) & string.IsNullOrEmpty(Convert.ToString(arr.GetValue(10)).Trim()))
                        {
                            strSQL.Append(" AND TO_DATE(T.TRANSDATE,'DD/MM/YYYY') >= TO_DATE('" + Convert.ToString(arr.GetValue(9)) + "','DD/MM/YYYY')");
                        }
                        else if (string.IsNullOrEmpty(Convert.ToString(arr.GetValue(9)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(10)).Trim()))
                        {
                            strSQL.Append(" AND TO_DATE(T.TRANSDATE,'DD/MM/YYYY') <= TO_DATE('" + Convert.ToString(arr.GetValue(10)) + "','DD/MM/YYYY')");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(11))))
                        {
                            strSQL.Append("  AND T.REF_GROUP_CUST_PK IN(" + Convert.ToString(arr.GetValue(11)) + ")");
                        }
                        strSQL.Append(")");
                        if (CustGrp == true)
                        {
                            strSQL.Append(")");
                        }
                    }
                    strSQL.Append(" UNION ");
                    strSQL.Append(" SELECT DISTINCT");
                    strSQL.Append(" CUST.CUSTOMER_MST_PK,");
                    strSQL.Append(" CUST.CUSTOMER_ID,");
                    strSQL.Append(" CUST.CUSTOMER_NAME,");
                    strSQL.Append(" '1' CHK");
                    strSQL.Append(" FROM ");
                    strSQL.Append(" CUSTOMER_MST_TBL CUST where 1=1");
                    if (arr.Length > 0)
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK IN(");
                        if (CustGrp == true)
                        {
                            strSQL.Append("SELECT C.REF_GROUP_CUST_PK FROM CUSTOMER_MST_TBL C WHERE C.CUSTOMER_MST_PK IN( ");
                        }
                        strSQL.Append(" SELECT T.CUSTFK FROM VIEW_FRTOUT_RPT T WHERE 1=1");
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(0))))
                        {
                            strSQL.Append("  AND (FETCH_FRIEGHT_OUTSTANDING.GETINVOICES(T.JCPK,T.BUSINESS_TYPE," + Convert.ToInt32(Convert.ToString(arr.GetValue(0))) + ",T.JOB_TYPE) - (FETCH_FRIEGHT_OUTSTANDING.GETCOLLECTIONS(T.JCPK,T.BUSINESS_TYPE," + Convert.ToInt32(Convert.ToString(arr.GetValue(0))) + ")))>0");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(1))))
                        {
                            strSQL.Append("  AND T.DEFAULT_LOCATION_FK IN(" + Convert.ToString(arr.GetValue(1)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(2))))
                        {
                            strSQL.Append("  AND T.POLFK IN(" + Convert.ToString(arr.GetValue(2)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(3))))
                        {
                            strSQL.Append("  AND T.PODFK IN(" + Convert.ToString(arr.GetValue(3)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(4))))
                        {
                            strSQL.Append("  AND T.CUSTFK IN(" + Convert.ToString(arr.GetValue(4)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()))
                        {
                            strSQL.Append("  AND UPPER(T.VSL_ID)='" + Convert.ToString(arr.GetValue(5)).ToUpper() + "'");
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(6))) > 0)
                        {
                            strSQL.Append("  AND T.JOB_TYPE =" + Convert.ToString(arr.GetValue(6)));
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(7))) > 0 & Convert.ToInt32(Convert.ToString(arr.GetValue(7))) != 3)
                        {
                            strSQL.Append("  AND T.BUSINESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(7))));
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(8))) > 0)
                        {
                            strSQL.Append("  AND T.PROCESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(8))));
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(9)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(10)).Trim()))
                        {
                            strSQL.Append(" AND TO_DATE(T.TRANSDATE,'DD/MM/YYYY') BETWEEN TO_DATE('" + Convert.ToString(arr.GetValue(9)) + "','DD/MM/YYYY') AND TO_DATE('" + Convert.ToString(arr.GetValue(10)) + "','DD/MM/YYYY')");
                        }
                        else if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(9)).Trim()) & string.IsNullOrEmpty(Convert.ToString(arr.GetValue(10)).Trim()))
                        {
                            strSQL.Append(" AND TO_DATE(T.TRANSDATE,'DD/MM/YYYY') >= TO_DATE('" + Convert.ToString(arr.GetValue(9)) + "','DD/MM/YYYY')");
                        }
                        else if (string.IsNullOrEmpty(Convert.ToString(arr.GetValue(9)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(10)).Trim()))
                        {
                            strSQL.Append(" AND TO_DATE(T.TRANSDATE,'DD/MM/YYYY') <= TO_DATE('" + Convert.ToString(arr.GetValue(10)) + "','DD/MM/YYYY')");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(11))))
                        {
                            strSQL.Append("  AND T.REF_GROUP_CUST_PK IN(" + Convert.ToString(arr.GetValue(11)) + ")");
                        }
                        strSQL.Append(")");
                        if (CustGrp == true)
                        {
                            strSQL.Append(")");
                        }
                    }
                    if (selectPK == null | string.IsNullOrEmpty(selectPK))
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK IN (0)");
                        if (CustGrp == true)
                        {
                            strSQL.Append(" AND CUST.GROUP_HEADER = 1 ");
                        }
                        strSQL.Append(" ) T ORDER BY CUSTOMER_ID,CHK DESC ");
                    }
                    else
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK IN (" + selectPK + ") ");
                        if (CustGrp == true)
                        {
                            strSQL.Append(" AND CUST.GROUP_HEADER = 1 ");
                        }
                        strSQL.Append(" ) T  ORDER BY CUSTOMER_ID,CHK DESC ");
                    }
                }
                else if (frmForm == "JCProfit")
                {
                    strSQL.Append(" SELECT * FROM ");
                    strSQL.Append("( SELECT DISTINCT ");
                    strSQL.Append(" CUST.CUSTOMER_MST_PK,");
                    strSQL.Append(" CUST.CUSTOMER_ID,");
                    strSQL.Append(" CUST.CUSTOMER_NAME,");
                    strSQL.Append(" '0' CHK");
                    strSQL.Append(" FROM ");
                    strSQL.Append(" CUSTOMER_MST_TBL CUST where 1=1");
                    if (CustGrp == true)
                    {
                        strSQL.Append(" AND CUST.GROUP_HEADER = 1 ");
                    }
                    if (selectPK == null | string.IsNullOrEmpty(selectPK))
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK NOT IN (0) ");
                    }
                    else
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK NOT IN (" + selectPK + ")");
                    }
                    if (arr.Length > 0)
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK IN(");
                        if (CustGrp == true)
                        {
                            strSQL.Append("SELECT C.REF_GROUP_CUST_PK FROM CUSTOMER_MST_TBL C WHERE C.CUSTOMER_MST_PK IN( ");
                        }
                        strSQL.Append(" SELECT T.CUSTFK FROM VIEW_JCPROFIT_RPT T WHERE 1=1");

                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(0))))
                        {
                            strSQL.Append("  AND T.DEFAULT_LOCATION_FK IN(" + Convert.ToString(arr.GetValue(0)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(1))))
                        {
                            strSQL.Append("  AND T.CUSTFK IN(" + Convert.ToString(arr.GetValue(1)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(2))))
                        {
                            strSQL.Append("  AND T.JCPK =" + Convert.ToString(arr.GetValue(2)));
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(3))))
                        {
                            strSQL.Append("  AND T.POLFK IN(" + Convert.ToString(arr.GetValue(3)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(4))))
                        {
                            strSQL.Append("  AND T.PODFK IN(" + Convert.ToString(arr.GetValue(4)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(6)).Trim()))
                        {
                            strSQL.Append(" AND TO_DATE(T.JCDATE,'DD/MM/YYYY') BETWEEN TO_DATE('" + Convert.ToString(arr.GetValue(5)) + "','DD/MM/YYYY') AND TO_DATE('" + Convert.ToString(arr.GetValue(6)) + "','DD/MM/YYYY')");
                        }
                        else if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()) & string.IsNullOrEmpty(Convert.ToString(arr.GetValue(6)).Trim()))
                        {
                            strSQL.Append(" AND TO_DATE(T.JCDATE,'DD/MM/YYYY') >= TO_DATE('" + Convert.ToString(arr.GetValue(5)) + "','DD/MM/YYYY')");
                        }
                        else if (string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(6)).Trim()))
                        {
                            strSQL.Append(" AND TO_DATE(T.JCDATE,'DD/MM/YYYY') <= TO_DATE('" + Convert.ToString(arr.GetValue(6)) + "','DD/MM/YYYY')");
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(7))) > 0)
                        {
                            strSQL.Append("  AND T.PROCESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(7))));
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(8))) > 0 & Convert.ToInt32(Convert.ToString(arr.GetValue(8))) != 3)
                        {
                            strSQL.Append("  AND T.BUSINESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(8))));
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(9))) > 0)
                        {
                            strSQL.Append("  AND T.CARGO_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(9))));
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(10))) > 0)
                        {
                            strSQL.Append("  AND T.JOB_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(10))));
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(11))) > 0)
                        {
                            strSQL.Append("  AND T.COMMODITY_GROUP_FK =" + Convert.ToInt32(Convert.ToString(arr.GetValue(11))));
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(12))) != 1)
                        {
                            if (Convert.ToInt32(Convert.ToString(arr.GetValue(12))) == 2)
                            {
                                strSQL.Append("  AND NVL(T.FD_SERVICE_TYPE_FLAG,0)=0");
                                strSQL.Append("  AND NVL(T.CST_SERVICE_TYPE_FLAG,0)=0");
                            }
                            else if (Convert.ToInt32(Convert.ToString(arr.GetValue(12))) == 3)
                            {
                                strSQL.Append("  AND NVL(T.FD_SERVICE_TYPE_FLAG,1)=1");
                                strSQL.Append("  AND NVL(T.CST_SERVICE_TYPE_FLAG,1)=1");
                            }
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(13))))
                        {
                            strSQL.Append("  AND T.REF_GROUP_CUST_PK IN(" + Convert.ToString(arr.GetValue(13)) + ")");
                        }
                        strSQL.Append("  AND (T.FRTPK IS NOT NULL OR T.CSTPK IS NOT NULL)");
                        strSQL.Append(")");
                        if (CustGrp == true)
                        {
                            strSQL.Append(")");
                        }
                    }
                    strSQL.Append(" UNION ");
                    strSQL.Append(" SELECT DISTINCT");
                    strSQL.Append(" CUST.CUSTOMER_MST_PK,");
                    strSQL.Append(" CUST.CUSTOMER_ID,");
                    strSQL.Append(" CUST.CUSTOMER_NAME,");
                    strSQL.Append(" '1' CHK");
                    strSQL.Append(" FROM ");
                    strSQL.Append(" CUSTOMER_MST_TBL CUST where 1=1");
                    if (arr.Length > 0)
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK IN(");
                        if (CustGrp == true)
                        {
                            strSQL.Append("SELECT C.REF_GROUP_CUST_PK FROM CUSTOMER_MST_TBL C WHERE C.CUSTOMER_MST_PK IN( ");
                        }
                        strSQL.Append(" SELECT T.CUSTFK FROM VIEW_JCPROFIT_RPT T WHERE 1=1");

                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(0))))
                        {
                            strSQL.Append("  AND T.DEFAULT_LOCATION_FK IN(" + Convert.ToString(arr.GetValue(0)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(1))))
                        {
                            strSQL.Append("  AND T.CUSTFK IN(" + Convert.ToString(arr.GetValue(1)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(2))))
                        {
                            strSQL.Append("  AND T.JCPK =" + Convert.ToString(arr.GetValue(2)));
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(3))))
                        {
                            strSQL.Append("  AND T.POLFK IN(" + Convert.ToString(arr.GetValue(3)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(4))))
                        {
                            strSQL.Append("  AND T.PODFK IN(" + Convert.ToString(arr.GetValue(4)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(6)).Trim()))
                        {
                            strSQL.Append(" AND TO_DATE(T.JCDATE,'DD/MM/YYYY') BETWEEN TO_DATE('" + Convert.ToString(arr.GetValue(5)) + "','DD/MM/YYYY') AND TO_DATE('" + Convert.ToString(arr.GetValue(6)) + "','DD/MM/YYYY')");
                        }
                        else if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()) & string.IsNullOrEmpty(Convert.ToString(arr.GetValue(6)).Trim()))
                        {
                            strSQL.Append(" AND TO_DATE(T.JCDATE,'DD/MM/YYYY') >= TO_DATE('" + Convert.ToString(arr.GetValue(5)) + "','DD/MM/YYYY')");
                        }
                        else if (string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(6)).Trim()))
                        {
                            strSQL.Append(" AND TO_DATE(T.JCDATE,'DD/MM/YYYY') <= TO_DATE('" + Convert.ToString(arr.GetValue(6)) + "','DD/MM/YYYY')");
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(7))) > 0)
                        {
                            strSQL.Append("  AND T.PROCESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(7))));
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(8))) > 0 & Convert.ToInt32(Convert.ToString(arr.GetValue(8))) != 3)
                        {
                            strSQL.Append("  AND T.BUSINESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(8))));
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(9))) > 0)
                        {
                            strSQL.Append("  AND T.CARGO_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(9))));
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(10))) > 0)
                        {
                            strSQL.Append("  AND T.JOB_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(10))));
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(11))) > 0)
                        {
                            strSQL.Append("  AND T.COMMODITY_GROUP_FK =" + Convert.ToInt32(Convert.ToString(arr.GetValue(11))));
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(12))) != 1)
                        {
                            if (Convert.ToInt32(Convert.ToString(arr.GetValue(12))) == 2)
                            {
                                strSQL.Append("  AND NVL(T.FD_SERVICE_TYPE_FLAG,0)=0");
                                strSQL.Append("  AND NVL(T.CST_SERVICE_TYPE_FLAG,0)=0");
                            }
                            else if (Convert.ToInt32(Convert.ToString(arr.GetValue(12))) == 3)
                            {
                                strSQL.Append("  AND NVL(T.FD_SERVICE_TYPE_FLAG,1)=1");
                                strSQL.Append("  AND NVL(T.CST_SERVICE_TYPE_FLAG,1)=1");
                            }
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(13))))
                        {
                            strSQL.Append("  AND T.REF_GROUP_CUST_PK IN(" + Convert.ToString(arr.GetValue(13)) + ")");
                        }
                        strSQL.Append("  AND (T.FRTPK IS NOT NULL OR T.CSTPK IS NOT NULL)");
                        strSQL.Append(")");
                        if (CustGrp == true)
                        {
                            strSQL.Append(")");
                        }
                    }
                    if (selectPK == null | string.IsNullOrEmpty(selectPK))
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK IN (0)");
                        if (CustGrp == true)
                        {
                            strSQL.Append(" AND CUST.GROUP_HEADER = 1 ");
                        }
                        strSQL.Append(" ) T ORDER BY CUSTOMER_ID,CHK DESC ");
                    }
                    else
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK IN (" + selectPK + ") ");
                        if (CustGrp == true)
                        {
                            strSQL.Append(" AND CUST.GROUP_HEADER = 1 ");
                        }
                        strSQL.Append(" ) T  ORDER BY CUSTOMER_ID,CHK DESC ");
                    }
                }
                else if (frmForm == "OTC")
                {
                    strSQL.Append(" SELECT * FROM ");
                    strSQL.Append("( SELECT DISTINCT ");
                    strSQL.Append(" CUST.CUSTOMER_MST_PK,");
                    strSQL.Append(" CUST.CUSTOMER_ID,");
                    strSQL.Append(" CUST.CUSTOMER_NAME,");
                    strSQL.Append(" '0' CHK");
                    strSQL.Append(" FROM ");
                    strSQL.Append(" CUSTOMER_MST_TBL CUST where 1=1");
                    if (CustGrp == true)
                    {
                        strSQL.Append(" AND CUST.GROUP_HEADER = 1 ");
                    }
                    if (selectPK == null | string.IsNullOrEmpty(selectPK))
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK NOT IN (0) ");
                    }
                    else
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK NOT IN (" + selectPK + ")");
                    }
                    if (arr.Length > 0)
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK IN(");
                        if (CustGrp == true)
                        {
                            strSQL.Append("SELECT C.REF_GROUP_CUST_PK FROM CUSTOMER_MST_TBL C WHERE C.CUSTOMER_MST_PK IN( ");
                        }
                        strSQL.Append(" SELECT T.CUSTFK FROM VIEW_OTC T WHERE 1=1");

                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(0))))
                        {
                            strSQL.Append("  AND T.DEFAULT_LOCATION_FK IN(" + Convert.ToString(arr.GetValue(0)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(1))))
                        {
                            strSQL.Append("  AND T.CUSTFK IN(" + Convert.ToString(arr.GetValue(1)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(2))))
                        {
                            strSQL.Append("  AND T.POLFK IN(" + Convert.ToString(arr.GetValue(2)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(3))))
                        {
                            strSQL.Append("  AND T.PODFK IN(" + Convert.ToString(arr.GetValue(3)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(4)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()))
                        {
                            strSQL.Append(" AND TO_DATE(T.BOOKING_DATE,'DD/MM/YYYY') BETWEEN TO_DATE('" + Convert.ToString(arr.GetValue(4)) + "','DD/MM/YYYY') AND TO_DATE('" + Convert.ToString(arr.GetValue(5)) + "','DD/MM/YYYY')");
                        }
                        else if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(4)).Trim()) & string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()))
                        {
                            strSQL.Append(" AND TO_DATE(T.BOOKING_DATE,'DD/MM/YYYY') >= TO_DATE('" + Convert.ToString(arr.GetValue(4)) + "','DD/MM/YYYY')");
                        }
                        else if (string.IsNullOrEmpty(Convert.ToString(arr.GetValue(4)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()))
                        {
                            strSQL.Append(" AND TO_DATE(T.BOOKING_DATE,'DD/MM/YYYY') <= TO_DATE('" + Convert.ToString(arr.GetValue(5)) + "','DD/MM/YYYY')");
                        }

                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(6))) > 0 & Convert.ToInt32(Convert.ToString(arr.GetValue(6))) != 3)
                        {
                            strSQL.Append("  AND T.BUSINESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(6))));
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(7))) & Convert.ToString(arr.GetValue(7)) != "All")
                        {
                            strSQL.Append("  AND UPPER(T.SHIPMENT_STATUS) ='" + Convert.ToString(arr.GetValue(7)).ToUpper() + "'");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(8))))
                        {
                            strSQL.Append("  AND UPPER(T.VESSEL_ID) ='" + Convert.ToString(arr.GetValue(8)).ToUpper() + "'");
                        }
                        strSQL.Append(")");
                        if (CustGrp == true)
                        {
                            strSQL.Append(")");
                        }
                    }
                    strSQL.Append(" UNION ");
                    strSQL.Append(" SELECT DISTINCT");
                    strSQL.Append(" CUST.CUSTOMER_MST_PK,");
                    strSQL.Append(" CUST.CUSTOMER_ID,");
                    strSQL.Append(" CUST.CUSTOMER_NAME,");
                    strSQL.Append(" '1' CHK");
                    strSQL.Append(" FROM ");
                    strSQL.Append(" CUSTOMER_MST_TBL CUST where 1=1");
                    if (arr.Length > 0)
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK IN(");
                        if (CustGrp == true)
                        {
                            strSQL.Append("SELECT C.REF_GROUP_CUST_PK FROM CUSTOMER_MST_TBL C WHERE C.CUSTOMER_MST_PK IN( ");
                        }
                        strSQL.Append(" SELECT T.CUSTFK FROM VIEW_OTC T WHERE 1=1");

                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(0))))
                        {
                            strSQL.Append("  AND T.DEFAULT_LOCATION_FK IN(" + Convert.ToString(arr.GetValue(0)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(1))))
                        {
                            strSQL.Append("  AND T.CUSTFK IN(" + Convert.ToString(arr.GetValue(1)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(2))))
                        {
                            strSQL.Append("  AND T.POLFK IN(" + Convert.ToString(arr.GetValue(2)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(3))))
                        {
                            strSQL.Append("  AND T.PODFK IN(" + Convert.ToString(arr.GetValue(3)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(4)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()))
                        {
                            strSQL.Append(" AND TO_DATE(T.BOOKING_DATE,'DD/MM/YYYY') BETWEEN TO_DATE('" + Convert.ToString(arr.GetValue(4)) + "','DD/MM/YYYY') AND TO_DATE('" + Convert.ToString(arr.GetValue(5)) + "','DD/MM/YYYY')");
                        }
                        else if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(4)).Trim()) & string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()))
                        {
                            strSQL.Append(" AND TO_DATE(T.BOOKING_DATE,'DD/MM/YYYY') >= TO_DATE('" + Convert.ToString(arr.GetValue(4)) + "','DD/MM/YYYY')");
                        }
                        else if (string.IsNullOrEmpty(Convert.ToString(arr.GetValue(4)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()))
                        {
                            strSQL.Append(" AND TO_DATE(T.BOOKING_DATE,'DD/MM/YYYY') <= TO_DATE('" + Convert.ToString(arr.GetValue(5)) + "','DD/MM/YYYY')");
                        }

                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(6))) > 0 & Convert.ToInt32(Convert.ToString(arr.GetValue(6))) != 3)
                        {
                            strSQL.Append("  AND T.BUSINESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(6))));
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(7))) & Convert.ToString(arr.GetValue(7)) != "All")
                        {
                            strSQL.Append("  AND UPPER(T.SHIPMENT_STATUS) ='" + Convert.ToString(arr.GetValue(7)).ToUpper() + "'");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(8))))
                        {
                            strSQL.Append("  AND UPPER(T.VESSEL_ID) ='" + Convert.ToString(arr.GetValue(8)).ToUpper() + "'");
                        }
                        strSQL.Append(")");
                        if (CustGrp == true)
                        {
                            strSQL.Append(")");
                        }
                    }
                    if (selectPK == null | string.IsNullOrEmpty(selectPK))
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK IN (0)");
                        if (CustGrp == true)
                        {
                            strSQL.Append(" AND CUST.GROUP_HEADER = 1 ");
                        }
                        strSQL.Append(" ) T ORDER BY CUSTOMER_ID,CHK DESC ");
                    }
                    else
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK IN (" + selectPK + ") ");
                        if (CustGrp == true)
                        {
                            strSQL.Append(" AND CUST.GROUP_HEADER = 1 ");
                        }
                        strSQL.Append(" ) T  ORDER BY CUSTOMER_ID,CHK DESC ");
                    }
                }
                else if (frmForm == "RevenueRpt")
                {
                    strSQL.Append(" SELECT * FROM ");
                    strSQL.Append("( SELECT DISTINCT ");
                    strSQL.Append(" CUST.CUSTOMER_MST_PK,");
                    strSQL.Append(" CUST.CUSTOMER_ID,");
                    strSQL.Append(" CUST.CUSTOMER_NAME,");
                    strSQL.Append(" '0' CHK");
                    strSQL.Append(" FROM ");
                    strSQL.Append(" CUSTOMER_MST_TBL CUST where 1=1");
                    if (CustGrp == true)
                    {
                        strSQL.Append(" AND CUST.GROUP_HEADER = 1 ");
                    }
                    if (selectPK == null | string.IsNullOrEmpty(selectPK))
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK NOT IN (0) ");
                    }
                    else
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK NOT IN (" + selectPK + ")");
                    }
                    if (arr.Length > 0)
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK IN(");
                        if (CustGrp == true)
                        {
                            strSQL.Append("SELECT C.REF_GROUP_CUST_PK FROM CUSTOMER_MST_TBL C WHERE C.CUSTOMER_MST_PK IN( ");
                        }
                        strSQL.Append(" SELECT T.CUSTFK FROM VIEW_REVENUE_RPT T WHERE 1=1");

                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(0))))
                        {
                            strSQL.Append("  AND T.DEFAULT_LOCATION_FK IN(" + Convert.ToString(arr.GetValue(0)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(1))))
                        {
                            strSQL.Append("  AND T.CUSTFK IN(" + Convert.ToString(arr.GetValue(1)) + ")");
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(2))) > 0)
                        {
                            strSQL.Append("  AND T.POLFK =" + Convert.ToString(arr.GetValue(2)));
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(3))) > 0)
                        {
                            strSQL.Append("  AND T.PODFK =" + Convert.ToString(arr.GetValue(3)));
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(4)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()))
                        {
                            strSQL.Append(" AND TO_DATE(T.JCDATE,'DD/MM/YYYY') BETWEEN TO_DATE('" + Convert.ToString(arr.GetValue(4)) + "','DD/MM/YYYY') AND TO_DATE('" + Convert.ToString(arr.GetValue(5)) + "','DD/MM/YYYY')");
                        }
                        else if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(4)).Trim()) & string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()))
                        {
                            strSQL.Append(" AND TO_DATE(T.JCDATE,'DD/MM/YYYY') >= TO_DATE('" + Convert.ToString(arr.GetValue(4)) + "','DD/MM/YYYY')");
                        }
                        else if (string.IsNullOrEmpty(Convert.ToString(arr.GetValue(4)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()))
                        {
                            strSQL.Append(" AND TO_DATE(T.JCDATE,'DD/MM/YYYY') <= TO_DATE('" + Convert.ToString(arr.GetValue(5)) + "','DD/MM/YYYY')");
                        }

                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(6))) > 0 & Convert.ToInt32(Convert.ToString(arr.GetValue(6))) != 3)
                        {
                            strSQL.Append("  AND T.BUSINESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(6))));
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(7))) > 0)
                        {
                            strSQL.Append("  AND T.JOB_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(7))));
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(8))))
                        {
                            strSQL.Append("  AND UPPER(T.VOYAGE) ='" + Convert.ToString(arr.GetValue(8)).ToUpper() + "'");
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(9))) > 0)
                        {
                            strSQL.Append("  AND T.JCPK =" + Convert.ToInt32(Convert.ToString(arr.GetValue(9))));
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(10))) > 0)
                        {
                            strSQL.Append("  AND T.PROCESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(10))));
                        }
                        strSQL.Append(")");
                        if (CustGrp == true)
                        {
                            strSQL.Append(")");
                        }
                    }
                    strSQL.Append(" UNION ");
                    strSQL.Append(" SELECT DISTINCT");
                    strSQL.Append(" CUST.CUSTOMER_MST_PK,");
                    strSQL.Append(" CUST.CUSTOMER_ID,");
                    strSQL.Append(" CUST.CUSTOMER_NAME,");
                    strSQL.Append(" '1' CHK");
                    strSQL.Append(" FROM ");
                    strSQL.Append(" CUSTOMER_MST_TBL CUST where 1=1");
                    if (arr.Length > 0)
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK IN(");
                        if (CustGrp == true)
                        {
                            strSQL.Append("SELECT C.REF_GROUP_CUST_PK FROM CUSTOMER_MST_TBL C WHERE C.CUSTOMER_MST_PK IN( ");
                        }
                        strSQL.Append(" SELECT T.CUSTFK FROM VIEW_REVENUE_RPT T WHERE 1=1");

                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(0))))
                        {
                            strSQL.Append("  AND T.DEFAULT_LOCATION_FK IN(" + Convert.ToString(arr.GetValue(0)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(1))))
                        {
                            strSQL.Append("  AND T.CUSTFK IN(" + Convert.ToString(arr.GetValue(1)) + ")");
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(2))) > 0)
                        {
                            strSQL.Append("  AND T.POLFK =" + Convert.ToString(arr.GetValue(2)));
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(3))) > 0)
                        {
                            strSQL.Append("  AND T.PODFK =" + Convert.ToString(arr.GetValue(3)));
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(4)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()))
                        {
                            strSQL.Append(" AND TO_DATE(T.JCDATE,'DD/MM/YYYY') BETWEEN TO_DATE('" + Convert.ToString(arr.GetValue(4)) + "','DD/MM/YYYY') AND TO_DATE('" + Convert.ToString(arr.GetValue(5)) + "','DD/MM/YYYY')");
                        }
                        else if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(4)).Trim()) & string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()))
                        {
                            strSQL.Append(" AND TO_DATE(T.JCDATE,'DD/MM/YYYY') >= TO_DATE('" + Convert.ToString(arr.GetValue(4)) + "','DD/MM/YYYY')");
                        }
                        else if (string.IsNullOrEmpty(Convert.ToString(arr.GetValue(4)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()))
                        {
                            strSQL.Append(" AND TO_DATE(T.JCDATE,'DD/MM/YYYY') <= TO_DATE('" + Convert.ToString(arr.GetValue(5)) + "','DD/MM/YYYY')");
                        }

                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(6))) > 0 & Convert.ToInt32(Convert.ToString(arr.GetValue(6))) != 3)
                        {
                            strSQL.Append("  AND T.BUSINESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(6))));
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(7))) > 0)
                        {
                            strSQL.Append("  AND T.JOB_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(7))));
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(8))))
                        {
                            strSQL.Append("  AND UPPER(T.VOYAGE) ='" + Convert.ToString(arr.GetValue(8)).ToUpper() + "'");
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(9))) > 0)
                        {
                            strSQL.Append("  AND T.JCPK =" + Convert.ToInt32(Convert.ToString(arr.GetValue(9))));
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(10))) > 0)
                        {
                            strSQL.Append("  AND T.PROCESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(10))));
                        }
                        strSQL.Append(")");
                        if (CustGrp == true)
                        {
                            strSQL.Append(")");
                        }
                    }
                    if (selectPK == null | string.IsNullOrEmpty(selectPK))
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK IN (0)");
                        if (CustGrp == true)
                        {
                            strSQL.Append(" AND CUST.GROUP_HEADER = 1 ");
                        }
                        strSQL.Append(" ) T ORDER BY CUSTOMER_ID,CHK DESC ");
                    }
                    else
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK IN (" + selectPK + ") ");
                        if (CustGrp == true)
                        {
                            strSQL.Append(" AND CUST.GROUP_HEADER = 1 ");
                        }
                        strSQL.Append(" ) T  ORDER BY CUSTOMER_ID,CHK DESC ");
                    }
                }
                else if (frmForm == "TOPCUST")
                {
                    strSQL.Append(" SELECT * FROM ");
                    strSQL.Append("( SELECT DISTINCT ");
                    strSQL.Append(" CUST.CUSTOMER_MST_PK,");
                    strSQL.Append(" CUST.CUSTOMER_ID,");
                    strSQL.Append(" CUST.CUSTOMER_NAME,");
                    strSQL.Append(" '0' CHK");
                    strSQL.Append(" FROM ");
                    strSQL.Append(" CUSTOMER_MST_TBL CUST where 1=1");
                    if (CustGrp == true)
                    {
                        strSQL.Append(" AND CUST.GROUP_HEADER = 1 ");
                    }
                    if (selectPK == null | string.IsNullOrEmpty(selectPK))
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK NOT IN (0) ");
                    }
                    else
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK NOT IN (" + selectPK + ")");
                    }
                    if (arr.Length > 0)
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK IN(");
                        if (CustGrp == true)
                        {
                            strSQL.Append("SELECT C.REF_GROUP_CUST_PK FROM CUSTOMER_MST_TBL C WHERE C.CUSTOMER_MST_PK IN( ");
                        }
                        strSQL.Append(" SELECT T.CUSTFK FROM VIEW_TOPCUST_RPT T WHERE 1=1");
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(0))))
                        {
                            strSQL.Append("  AND T.DEFAULT_LOCATION_FK IN(" + Convert.ToString(arr.GetValue(0)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(1))))
                        {
                            strSQL.Append("  AND T.CUSTFK IN(" + Convert.ToString(arr.GetValue(1)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(2))))
                        {
                            strSQL.Append("  AND T.POLFK IN(" + Convert.ToString(arr.GetValue(2)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(3))))
                        {
                            strSQL.Append("  AND T.PODFK IN(" + Convert.ToString(arr.GetValue(3)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(4)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()))
                        {
                            strSQL.Append(" AND TO_DATE(T.JCDATE,'DD/MM/YYYY') BETWEEN TO_DATE('" + Convert.ToString(arr.GetValue(4)) + "','DD/MM/YYYY') AND TO_DATE('" + Convert.ToString(arr.GetValue(5)) + "','DD/MM/YYYY')");
                        }
                        else if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(4)).Trim()) & string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()))
                        {
                            strSQL.Append(" AND TO_DATE(T.JCDATE,'DD/MM/YYYY') >= TO_DATE('" + Convert.ToString(arr.GetValue(4)) + "','DD/MM/YYYY')");
                        }
                        else if (string.IsNullOrEmpty(Convert.ToString(arr.GetValue(4)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()))
                        {
                            strSQL.Append(" AND TO_DATE(T.JCDATE,'DD/MM/YYYY') <= TO_DATE('" + Convert.ToString(arr.GetValue(5)) + "','DD/MM/YYYY')");
                        }

                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(6))) > 0 & Convert.ToInt32(Convert.ToString(arr.GetValue(6))) != 3)
                        {
                            strSQL.Append("  AND T.BUSINESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(6))));
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(7))) > 0)
                        {
                            strSQL.Append("  AND T.COMMODITY_GROUP_FK =" + Convert.ToInt32(Convert.ToString(arr.GetValue(7))));
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(8))) > 0)
                        {
                            strSQL.Append("  AND T.PROCESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(8))));
                        }

                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(9))) > 0)
                        {
                            if (Convert.ToInt32(Convert.ToString(arr.GetValue(6))) > 0)
                            {
                                strSQL.Append("  AND T.CARGO_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(9))));
                            }
                        }
                        strSQL.Append(" AND ( NUMBEROF_TEUS(T.JCPK, T.PROCESS_TYPE, T.BUSINESS_TYPE," + Convert.ToInt32(Convert.ToString(arr.GetValue(9))) + ",T.JOB_TYPE)>0 OR");
                        strSQL.Append(" CASE WHEN T.JOB_TYPE=1 AND T.BUSINESS_TYPE=2 AND T.PROCESS_TYPE=1 THEN ");
                        strSQL.Append(" FETCH_JOB_CARD_SEA_EXP_ACTREV( T.JCPK," + Convert.ToInt32(Convert.ToString(arr.GetValue(10))) + ")");
                        strSQL.Append(" WHEN T.JOB_TYPE=1 AND T.BUSINESS_TYPE=2 AND T.PROCESS_TYPE=2 THEN ");
                        strSQL.Append(" FETCH_JOB_CARD_SEA_IMP_ACTREV( T.JCPK," + Convert.ToInt32(Convert.ToString(arr.GetValue(10))) + ")");
                        strSQL.Append(" WHEN T.JOB_TYPE=1 AND T.BUSINESS_TYPE=1 AND T.PROCESS_TYPE=1 THEN ");
                        strSQL.Append(" FETCH_JOB_CARD_AIR_EXP_ACTREV( T.JCPK," + Convert.ToInt32(Convert.ToString(arr.GetValue(10))) + ")");
                        strSQL.Append(" WHEN T.JOB_TYPE=1 AND T.BUSINESS_TYPE=1 AND T.PROCESS_TYPE=2 THEN ");
                        strSQL.Append(" FETCH_JOB_CARD_AIR_IMP_ACTREV( T.JCPK," + Convert.ToInt32(Convert.ToString(arr.GetValue(10))) + ")");
                        strSQL.Append(" WHEN T.JOB_TYPE=2 THEN ");
                        strSQL.Append(" FETCH_CBJC_ACTUAL_REVENUE( T.JCPK," + Convert.ToInt32(Convert.ToString(arr.GetValue(10))) + ",T.BUSINESS_TYPE,T.PROCESS_TYPE)");
                        strSQL.Append(" WHEN T.JOB_TYPE=3 THEN ");
                        strSQL.Append(" FETCH_TPT_ACTUAL_REVENUE( T.JCPK," + Convert.ToInt32(Convert.ToString(arr.GetValue(10))) + ",T.BUSINESS_TYPE,T.PROCESS_TYPE)");
                        strSQL.Append(" END >0)");
                        strSQL.Append(")");
                        if (CustGrp == true)
                        {
                            strSQL.Append(")");
                        }
                    }
                    strSQL.Append(" UNION ");
                    strSQL.Append(" SELECT DISTINCT");
                    strSQL.Append(" CUST.CUSTOMER_MST_PK,");
                    strSQL.Append(" CUST.CUSTOMER_ID,");
                    strSQL.Append(" CUST.CUSTOMER_NAME,");
                    strSQL.Append(" '1' CHK");
                    strSQL.Append(" FROM ");
                    strSQL.Append(" CUSTOMER_MST_TBL CUST where 1=1");
                    if (arr.Length > 0)
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK IN(");
                        if (CustGrp == true)
                        {
                            strSQL.Append("SELECT C.REF_GROUP_CUST_PK FROM CUSTOMER_MST_TBL C WHERE C.CUSTOMER_MST_PK IN( ");
                        }
                        strSQL.Append(" SELECT T.CUSTFK FROM VIEW_TOPCUST_RPT T WHERE 1=1");
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(0))))
                        {
                            strSQL.Append("  AND T.DEFAULT_LOCATION_FK IN(" + Convert.ToString(arr.GetValue(0)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(1))))
                        {
                            strSQL.Append("  AND T.CUSTFK IN(" + Convert.ToString(arr.GetValue(1)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(2))))
                        {
                            strSQL.Append("  AND T.POLFK IN(" + Convert.ToString(arr.GetValue(2)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(3))))
                        {
                            strSQL.Append("  AND T.PODFK IN(" + Convert.ToString(arr.GetValue(3)) + ")");
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(4)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()))
                        {
                            strSQL.Append(" AND TO_DATE(T.JCDATE,'DD/MM/YYYY') BETWEEN TO_DATE('" + Convert.ToString(arr.GetValue(4)) + "','DD/MM/YYYY') AND TO_DATE('" + Convert.ToString(arr.GetValue(5)) + "','DD/MM/YYYY')");
                        }
                        else if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(4)).Trim()) & string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()))
                        {
                            strSQL.Append(" AND TO_DATE(T.JCDATE,'DD/MM/YYYY') >= TO_DATE('" + Convert.ToString(arr.GetValue(4)) + "','DD/MM/YYYY')");
                        }
                        else if (string.IsNullOrEmpty(Convert.ToString(arr.GetValue(4)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()))
                        {
                            strSQL.Append(" AND TO_DATE(T.JCDATE,'DD/MM/YYYY') <= TO_DATE('" + Convert.ToString(arr.GetValue(5)) + "','DD/MM/YYYY')");
                        }

                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(6))) > 0 & Convert.ToInt32(Convert.ToString(arr.GetValue(6))) != 3)
                        {
                            strSQL.Append("  AND T.BUSINESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(6))));
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(7))) > 0)
                        {
                            strSQL.Append("  AND T.COMMODITY_GROUP_FK =" + Convert.ToInt32(Convert.ToString(arr.GetValue(7))));
                        }
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(8))) > 0)
                        {
                            strSQL.Append("  AND T.PROCESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(8))));
                        }

                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(9))) > 0)
                        {
                            if (Convert.ToInt32(Convert.ToString(arr.GetValue(6))) > 0)
                            {
                                strSQL.Append("  AND T.CARGO_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(9))));
                            }
                        }
                        strSQL.Append(" AND ( NUMBEROF_TEUS(T.JCPK, T.PROCESS_TYPE, T.BUSINESS_TYPE," + Convert.ToInt32(Convert.ToString(arr.GetValue(9))) + ",T.JOB_TYPE)>0 OR");
                        strSQL.Append(" CASE WHEN T.JOB_TYPE=1 AND T.BUSINESS_TYPE=2 AND T.PROCESS_TYPE=1 THEN ");
                        strSQL.Append(" FETCH_JOB_CARD_SEA_EXP_ACTREV( T.JCPK," + Convert.ToInt32(Convert.ToString(arr.GetValue(10))) + ")");
                        strSQL.Append(" WHEN T.JOB_TYPE=1 AND T.BUSINESS_TYPE=2 AND T.PROCESS_TYPE=2 THEN ");
                        strSQL.Append(" FETCH_JOB_CARD_SEA_IMP_ACTREV( T.JCPK," + Convert.ToInt32(Convert.ToString(arr.GetValue(10))) + ")");
                        strSQL.Append(" WHEN T.JOB_TYPE=1 AND T.BUSINESS_TYPE=1 AND T.PROCESS_TYPE=1 THEN ");
                        strSQL.Append(" FETCH_JOB_CARD_AIR_EXP_ACTREV( T.JCPK," + Convert.ToInt32(Convert.ToString(arr.GetValue(10))) + ")");
                        strSQL.Append(" WHEN T.JOB_TYPE=1 AND T.BUSINESS_TYPE=1 AND T.PROCESS_TYPE=2 THEN ");
                        strSQL.Append(" FETCH_JOB_CARD_AIR_IMP_ACTREV( T.JCPK," + Convert.ToInt32(Convert.ToString(arr.GetValue(10))) + ")");
                        strSQL.Append(" WHEN T.JOB_TYPE=2 THEN ");
                        strSQL.Append(" FETCH_CBJC_ACTUAL_REVENUE( T.JCPK," + Convert.ToInt32(Convert.ToString(arr.GetValue(10))) + ",T.BUSINESS_TYPE,T.PROCESS_TYPE)");
                        strSQL.Append(" WHEN T.JOB_TYPE=3 THEN ");
                        strSQL.Append(" FETCH_TPT_ACTUAL_REVENUE( T.JCPK," + Convert.ToInt32(Convert.ToString(arr.GetValue(10))) + ",T.BUSINESS_TYPE,T.PROCESS_TYPE)");
                        strSQL.Append(" END >0)");
                        strSQL.Append(")");
                        if (CustGrp == true)
                        {
                            strSQL.Append(")");
                        }
                    }
                    if (selectPK == null | string.IsNullOrEmpty(selectPK))
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK IN (0)");
                        if (CustGrp == true)
                        {
                            strSQL.Append(" AND CUST.GROUP_HEADER = 1 ");
                        }
                        strSQL.Append(" ) T ORDER BY CUSTOMER_ID,CHK DESC ");
                    }
                    else
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK IN (" + selectPK + ") ");
                        if (CustGrp == true)
                        {
                            strSQL.Append(" AND CUST.GROUP_HEADER = 1 ");
                        }
                        strSQL.Append(" ) T  ORDER BY CUSTOMER_ID,CHK DESC ");
                    }
                }

            }
            else
            {
                if (string.IsNullOrEmpty(selectPK))
                {
                    selectPK = "0";
                }
                if (LocFk == 0)
                {
                    strSQL.Append(" SELECT * FROM ");
                    strSQL.Append("( SELECT DISTINCT ");
                    strSQL.Append(" CUST.CUSTOMER_MST_PK,");
                    strSQL.Append(" CUST.CUSTOMER_ID,");
                    strSQL.Append(" CUST.CUSTOMER_NAME,");
                    strSQL.Append(" '0' CHK");
                    strSQL.Append(" FROM ");
                    strSQL.Append(" CUSTOMER_MST_TBL CUST ,");
                    strSQL.Append(" CUSTOMER_CATEGORY_TRN CR ");
                    strSQL.Append(" WHERE ");
                    strSQL.Append(" CUST.CUSTOMER_MST_PK = CR.CUSTOMER_MST_FK  AND ");
                    strSQL.Append(" CUST.BUSINESS_TYPE IN (3," + Biztype + ")");
                    if (selectPK == null | string.IsNullOrEmpty(selectPK))
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK NOT IN (0) ");
                    }
                    else
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK NOT IN (" + selectPK + " ) ");
                    }
                    strSQL.Append(" AND CUST.ACTIVE_FLAG =1 ");
                    strSQL.Append(" AND CUST.Temp_Party <> 1");
                    if (CustGrp == true)
                    {
                        strSQL.Append(" AND CUST.GROUP_HEADER = 1 ");
                    }
                    if (Process != "All" & Process != "0")
                    {
                        strSQL.Append(" AND CR.CUSTOMER_CATEGORY_MST_FK =(SELECT CUSTOMER_CATEGORY_MST_PK FROM CUSTOMER_CATEGORY_MST_TBL T  WHERE CUSTOMER_CATEGORY_ID LIKE'" + Process + "')");
                    }

                    strSQL.Append(" UNION ");
                    strSQL.Append(" SELECT DISTINCT");
                    strSQL.Append(" CUST.CUSTOMER_MST_PK,");
                    strSQL.Append(" CUST.CUSTOMER_ID,");
                    strSQL.Append(" CUST.CUSTOMER_NAME,");
                    strSQL.Append(" '1' CHK");
                    strSQL.Append(" FROM ");
                    strSQL.Append(" CUSTOMER_MST_TBL CUST,");
                    strSQL.Append(" CUSTOMER_CATEGORY_TRN CR ");
                    strSQL.Append(" WHERE");
                    strSQL.Append(" CUST.CUSTOMER_MST_PK = CR.CUSTOMER_MST_FK  AND ");
                    strSQL.Append(" CUST.BUSINESS_TYPE IN (3," + Biztype + ")");
                    if (selectPK == null)
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK IN (0)");
                        strSQL.Append(" AND CUST.ACTIVE_FLAG =1 ");
                        strSQL.Append(" AND CUST.Temp_Party <> 1");
                        if (CustGrp == true)
                        {
                            strSQL.Append(" AND CUST.GROUP_HEADER = 1 ");
                        }
                        if (Process != "All" & Process != "0")
                        {
                            strSQL.Append(" AND CR.CUSTOMER_CATEGORY_MST_FK =(SELECT CUSTOMER_CATEGORY_MST_PK FROM CUSTOMER_CATEGORY_MST_TBL T  WHERE CUSTOMER_CATEGORY_ID LIKE'" + Process + "')");
                        }
                        strSQL.Append(" ) T ORDER BY CUSTOMER_ID,CHK DESC ");
                    }
                    else
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK IN (" + selectPK + ") ");
                        strSQL.Append(" AND CUST.ACTIVE_FLAG =1");
                        strSQL.Append(" AND CUST.Temp_Party <> 1");
                        if (CustGrp == true)
                        {
                            strSQL.Append(" AND CUST.GROUP_HEADER = 1 ");
                        }
                        if (Process != "All" & Process != "0")
                        {
                            strSQL.Append(" AND CR.CUSTOMER_CATEGORY_MST_FK =(SELECT CUSTOMER_CATEGORY_MST_PK FROM CUSTOMER_CATEGORY_MST_TBL T  WHERE CUSTOMER_CATEGORY_ID LIKE'" + Process + "')");
                        }
                        strSQL.Append(" ) T  ORDER BY CUSTOMER_ID,CHK DESC ");
                    }

                }
                else
                {
                    strSQL.Append(" SELECT * FROM ( ");
                    strSQL.Append("SELECT DISTINCT ");
                    strSQL.Append(" CUST.CUSTOMER_MST_PK,");
                    strSQL.Append(" CUST.CUSTOMER_ID,");
                    strSQL.Append(" CUST.CUSTOMER_NAME,");
                    strSQL.Append(" '0' CHK");
                    //strSQL.Append(" CUST.SEA_CREDIT_LIMIT,")
                    //strSQL.Append(" CUST.AIR_CREDIT_LIMIT,")
                    //strSQL.Append(" (CUST.SEA_CREDIT_LIMIT + CUST.AIR_CREDIT_LIMIT) ""Total"" ")
                    strSQL.Append(" FROM ");
                    strSQL.Append(" CUSTOMER_MST_TBL      CUST,");
                    strSQL.Append(" CUSTOMER_CATEGORY_TRN CR ,");
                    strSQL.Append("  LOCATION_MST_TBL      L ,");
                    strSQL.Append(" CUSTOMER_CONTACT_DTLS CTDL ");
                    strSQL.Append(" WHERE ");
                    strSQL.Append(" CUST.CUSTOMER_MST_PK = CR.CUSTOMER_MST_FK  AND ");
                    strSQL.Append(" CUST.BUSINESS_TYPE IN (3," + Biztype + ") AND ");
                    strSQL.Append(" CUST.CUSTOMER_MST_PK NOT IN (0, " + selectPK + ") AND");
                    strSQL.Append(" CTDL.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK AND ");

                    // strSQL.Append(" CTDL.ADM_LOCATION_MST_FK in (SELECT PM.LOCATION_MST_FK FROM LOC_PORT_MAPPING_TRN LPM, PORT_MST_TBL PM WHERE LPM.PORT_MST_FK=PM.PORT_MST_PK AND LPM.LOCATION_MST_FK = (" & LocFk & ") UNION SELECT (" & LocFk & ") AS LOCATION_MST_FK from DUAL) ") '(" & LocFk & ")
                    strSQL.Append("  L.LOCATION_MST_PK IN (SELECT DISTINCT LOCATION_MST_FK FROM  (SELECT PM.LOCATION_MST_FK FROM LOC_PORT_MAPPING_TRN LPM, PORT_MST_TBL PM WHERE LPM.PORT_MST_FK=PM.PORT_MST_PK AND LPM.LOCATION_MST_FK = (" + LocFk + ") UNION SELECT (" + LocFk + ") AS LOCATION_MST_FK from DUAL)) ");
                    //(" & LocFk & ")
                    strSQL.Append(" AND CUST.ACTIVE_FLAG = 1");
                    strSQL.Append(" AND CUST.TEMP_PARTY <> 1");
                    if (CustGrp == true)
                    {
                        strSQL.Append(" AND CUST.GROUP_HEADER = 1 ");
                    }

                    //If Process <> "All" And Process <> "0" Then
                    //    strSQL.Append(" AND CR.CUSTOMER_CATEGORY_MST_FK =(SELECT CUSTOMER_CATEGORY_MST_PK FROM CUSTOMER_CATEGORY_MST_TBL T  WHERE CUSTOMER_CATEGORY_ID LIKE'" & Process & "')")
                    //End If
                    strSQL.Append(" UNION");
                    strSQL.Append(" SELECT DISTINCT");
                    strSQL.Append(" CUST.CUSTOMER_MST_PK,");
                    strSQL.Append(" CUST.CUSTOMER_ID,");
                    strSQL.Append(" CUST.CUSTOMER_NAME,");
                    strSQL.Append(" '1' CHK");

                    strSQL.Append(" FROM");
                    strSQL.Append(" CUSTOMER_MST_TBL CUST, ");
                    strSQL.Append(" CUSTOMER_CATEGORY_TRN CR ,");
                    strSQL.Append("  LOCATION_MST_TBL      L ,");
                    strSQL.Append(" CUSTOMER_CONTACT_DTLS CTDL");
                    strSQL.Append(" WHERE");
                    strSQL.Append(" CUST.CUSTOMER_MST_PK = CR.CUSTOMER_MST_FK  AND ");
                    strSQL.Append(" CUST.BUSINESS_TYPE IN ( 3," + Biztype + ")");
                    if (Convert.ToInt32(selectPK) == 0 | string.IsNullOrEmpty(selectPK))
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK  IN (0) ");
                    }
                    else
                    {
                        strSQL.Append(" AND CUST.CUSTOMER_MST_PK IN (" + selectPK + " ) ");
                    }
                    strSQL.Append(" AND CTDL.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK ");
                    //strSQL.Append(" AND CTDL.ADM_LOCATION_MST_FK in (SELECT PM.LOCATION_MST_FK FROM LOC_PORT_MAPPING_TRN LPM, PORT_MST_TBL PM WHERE LPM.PORT_MST_FK=PM.PORT_MST_PK AND LPM.LOCATION_MST_FK = (" & LocFk & ") UNION SELECT (" & LocFk & ") AS LOCATION_MST_FK from DUAL) ") '(" & LocFk & ")
                    strSQL.Append(" AND L.LOCATION_MST_PK IN (SELECT DISTINCT LOCATION_MST_FK FROM  (SELECT PM.LOCATION_MST_FK FROM LOC_PORT_MAPPING_TRN LPM, PORT_MST_TBL PM WHERE LPM.PORT_MST_FK=PM.PORT_MST_PK AND LPM.LOCATION_MST_FK = (" + LocFk + ") UNION SELECT (" + LocFk + ") AS LOCATION_MST_FK from DUAL)) ");
                    //(" & LocFk & ")
                    strSQL.Append(" AND CUST.ACTIVE_FLAG = 1");
                    strSQL.Append(" AND CUST.TEMP_PARTY <> 1");
                    strSQL.Append(" AND CTDL.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK");

                    if (CustGrp == true)
                    {
                        strSQL.Append(" AND CUST.GROUP_HEADER = 1 ");
                    }
                    //If Process <> "All" And Process <> "0" Then
                    //    strSQL.Append(" AND CR.CUSTOMER_CATEGORY_MST_FK =(SELECT CUSTOMER_CATEGORY_MST_PK FROM CUSTOMER_CATEGORY_MST_TBL T  WHERE CUSTOMER_CATEGORY_ID LIKE'" & Process & "')")
                    //End If
                    strSQL.Append(" ) T  ORDER BY CUSTOMER_ID,CHK DESC ");
                }
            }

            //Count the  Records
            strCount.Append(" SELECT COUNT(*)");
            strCount.Append(" FROM ");
            strCount.Append("(" + strSQL.ToString() + ")");
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));
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
            strSQLBuilder.Append(" SELECT  qry.* FROM ");
            strSQLBuilder.Append(" (SELECT ROWNUM SR_NO,T.* FROM ");
            strSQLBuilder.Append((" (" + strSQL.ToString() + ")"));
            strSQLBuilder.Append(" T) qry  WHERE SR_NO  Between " + start + " and " + last);
            strSQLBuilder.Append(" ORDER BY CHK DESC, CUSTOMER_ID ");
            try
            {
                return (new WorkFlow()).GetDataSet(strSQLBuilder.ToString());
                //Manjunath  PTS ID:Sep-02  17/09/2011
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
        #region "FetchCustomerName"
        public DataSet FetchCreditCustomerName(string Process, string selectPK = "0", string Biztype = "3", Int16 CrdCust = 0, string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, long LocFk = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            StringBuilder strSQLBuilder = new StringBuilder();
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            StringBuilder strCount = new StringBuilder();

            selectPK = selectPK.TrimEnd(',');
            //Grid Selection 
            // LocFk = 0  Its Display all the Customers
            // LocFk != 0 Based on the Location display the Customers
            if (string.IsNullOrEmpty(selectPK))
            {
                selectPK = "0";
            }
            //Where condition modified by Snigdharani for Business Type -= 15/01/2010
            if (LocFk == 0)
            {
                strSQL.Append(" SELECT * FROM ");
                strSQL.Append("( SELECT ");
                strSQL.Append(" CUST.CUSTOMER_MST_PK,");
                strSQL.Append(" CUST.CUSTOMER_ID,");
                strSQL.Append(" CUST.CUSTOMER_NAME,");
                strSQL.Append(" '0' CHK");
                strSQL.Append(" FROM ");
                strSQL.Append(" CUSTOMER_MST_TBL CUST ,");
                strSQL.Append(" CUSTOMER_CATEGORY_TRN CR ");
                strSQL.Append(" WHERE ");
                strSQL.Append(" CUST.CUSTOMER_MST_PK = CR.CUSTOMER_MST_FK ");
                if (Convert.ToInt32(Biztype) == 1 | Convert.ToInt32(Biztype) == 2)
                {
                    strSQL.Append("AND CUST.BUSINESS_TYPE IN (3," + Biztype + ")");
                }
                else if (Convert.ToInt32(Biztype) == 3)
                {
                    strSQL.Append("AND CUST.BUSINESS_TYPE IN (1,2,3)");
                }
                else if (Convert.ToInt32(Biztype) == 4)
                {
                    strSQL.Append("AND CUST.BUSINESS_TYPE IN (4)");
                }
                //If selectPK Is Nothing Or selectPK = "" Then
                //    strSQL.Append(" AND CUST.CUSTOMER_MST_PK NOT IN (0) ")
                //Else
                //    strSQL.Append(" AND CUST.CUSTOMER_MST_PK NOT IN (" & selectPK & " ) ")
                //End If
                strSQL.Append(" AND CUST.ACTIVE_FLAG =1 ");
                strSQL.Append(" AND CUST.Temp_Party <> 1");
                strSQL.Append(" AND CR.CUSTOMER_CATEGORY_MST_FK =(SELECT CUSTOMER_CATEGORY_MST_PK FROM CUSTOMER_CATEGORY_MST_TBL T  WHERE CUSTOMER_CATEGORY_ID LIKE'" + Process + "'))");
                strSQL.Append(" UNION ");
                strSQL.Append(" SELECT");
                strSQL.Append(" CUST.CUSTOMER_MST_PK,");
                strSQL.Append(" CUST.CUSTOMER_ID,");
                strSQL.Append(" CUST.CUSTOMER_NAME,");
                strSQL.Append(" '1' CHK");
                strSQL.Append(" FROM ");
                strSQL.Append(" CUSTOMER_MST_TBL CUST,");
                strSQL.Append(" CUSTOMER_CATEGORY_TRN CR ");
                strSQL.Append(" WHERE");
                strSQL.Append(" CUST.CUSTOMER_MST_PK = CR.CUSTOMER_MST_FK  ");
                if (Convert.ToInt32(Biztype) == 1 | Convert.ToInt32(Biztype) == 2)
                {
                    strSQL.Append("AND CUST.BUSINESS_TYPE IN (3," + Biztype + ")");
                }
                else if (Convert.ToInt32(Biztype) == 3)
                {
                    strSQL.Append("AND CUST.BUSINESS_TYPE IN (1,2,3)");
                }
                else if (Convert.ToInt32(Biztype) == 4)
                {
                    strSQL.Append("AND CUST.BUSINESS_TYPE IN (4)");
                }
                //If selectPK Is Nothing Then
                //    strSQL.Append(" AND CUST.CUSTOMER_MST_PK IN (0)")
                //    strSQL.Append(" AND CUST.ACTIVE_FLAG =1 ")
                //    strSQL.Append(" AND CUST.Temp_Party <> 1")
                //    strSQL.Append(" AND CR.CUSTOMER_CATEGORY_MST_FK =(SELECT CUSTOMER_CATEGORY_MST_PK FROM CUSTOMER_CATEGORY_MST_TBL T  WHERE CUSTOMER_CATEGORY_ID LIKE'" & Process & "')")
                //    strSQL.Append(" ) T ORDER BY CUSTOMER_ID,CHK DESC ")
                //Else
                //    strSQL.Append(" AND CUST.CUSTOMER_MST_PK IN (" & selectPK & ") ")
                //    strSQL.Append(" AND CUST.ACTIVE_FLAG =1")
                //    strSQL.Append(" AND CUST.Temp_Party <> 1")
                //    strSQL.Append(" AND CR.CUSTOMER_CATEGORY_MST_FK =(SELECT CUSTOMER_CATEGORY_MST_PK FROM CUSTOMER_CATEGORY_MST_TBL T  WHERE CUSTOMER_CATEGORY_ID LIKE'" & Process & "')")
                //    strSQL.Append(" ) T  ORDER BY CUSTOMER_ID,CHK DESC ")
                //End If

            }
            else
            {
                strSQL.Append(" SELECT * FROM (( ");
                strSQL.Append("  SELECT ");
                strSQL.Append(" CUST.CUSTOMER_MST_PK,");
                strSQL.Append(" CUST.CUSTOMER_ID,");
                strSQL.Append(" CUST.CUSTOMER_NAME,");
                strSQL.Append(" CUST.SEA_CREDIT_LIMIT,");
                strSQL.Append(" CUST.AIR_CREDIT_LIMIT,");
                strSQL.Append(" (CUST.SEA_CREDIT_LIMIT + CUST.AIR_CREDIT_LIMIT) \"Total\" ,");
                strSQL.Append(" '0' CHK");
                strSQL.Append(" FROM ");
                strSQL.Append(" CUSTOMER_MST_TBL      CUST,");
                strSQL.Append(" CUSTOMER_CATEGORY_TRN CR ,");
                strSQL.Append(" CUSTOMER_CONTACT_DTLS CTDL ");
                strSQL.Append(" WHERE ");
                strSQL.Append(" CUST.CUSTOMER_MST_PK = CR.CUSTOMER_MST_FK  AND ");
                if (CrdCust == 1)
                {
                    strSQL.Append(" CUST.CREDIT_CUSTOMER =1 AND ");
                }
                strSQL.Append(" CUST.BUSINESS_TYPE IN (3," + Biztype + ") AND ");
                strSQL.Append(" CUST.CUSTOMER_MST_PK NOT IN (0, " + selectPK + ") AND");
                strSQL.Append(" CTDL.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK AND ");
                strSQL.Append(" CTDL.ADM_LOCATION_MST_FK in (" + LocFk + ") AND");
                strSQL.Append(" CUST.ACTIVE_FLAG = 1");
                strSQL.Append(" AND CR.CUSTOMER_CATEGORY_MST_FK in (SELECT CUSTOMER_CATEGORY_MST_PK FROM CUSTOMER_CATEGORY_MST_TBL T )");
                // WHERE CUSTOMER_CATEGORY_ID LIKE'" & Process & "')")
                strSQL.Append(" UNION");
                strSQL.Append(" SELECT ");
                strSQL.Append(" CUST.CUSTOMER_MST_PK,");
                strSQL.Append(" CUST.CUSTOMER_ID,");
                strSQL.Append(" CUST.CUSTOMER_NAME,");
                strSQL.Append(" CUST.SEA_CREDIT_LIMIT,");
                strSQL.Append(" CUST.AIR_CREDIT_LIMIT,");
                strSQL.Append(" (CUST.SEA_CREDIT_LIMIT + CUST.AIR_CREDIT_LIMIT) \"Total\" ,");
                strSQL.Append(" '1' CHK");
                strSQL.Append(" FROM");
                strSQL.Append(" CUSTOMER_MST_TBL CUST, ");
                strSQL.Append(" CUSTOMER_CATEGORY_TRN CR ,");
                strSQL.Append(" CUSTOMER_CONTACT_DTLS CTDL");
                strSQL.Append(" WHERE");
                strSQL.Append(" CUST.CUSTOMER_MST_PK = CR.CUSTOMER_MST_FK  AND ");
                if (CrdCust == 1)
                {
                    strSQL.Append("   CUST.CREDIT_CUSTOMER =1 AND ");
                }
                strSQL.Append(" CUST.BUSINESS_TYPE IN ( 3," + Biztype + ")");

                if (Convert.ToInt32(selectPK) == 0 | string.IsNullOrEmpty(selectPK))
                {
                    strSQL.Append(" AND CUST.CUSTOMER_MST_PK  IN (0) ");
                }
                else
                {
                    strSQL.Append(" AND CUST.CUSTOMER_MST_PK IN (" + selectPK + " ) ");
                }
                strSQL.Append(" AND CTDL.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK ");
                strSQL.Append(" AND CTDL.ADM_LOCATION_MST_FK in(" + LocFk + " )");
                strSQL.Append(" AND CUST.ACTIVE_FLAG = 1");
                strSQL.Append(" AND CR.CUSTOMER_CATEGORY_MST_FK in (SELECT CUSTOMER_CATEGORY_MST_PK FROM CUSTOMER_CATEGORY_MST_TBL T )");
                //WHERE CUSTOMER_CATEGORY_ID LIKE'" & Process & "')")
                strSQL.Append(" )) T  ORDER BY CUSTOMER_ID,CHK DESC ");
            }
            //Count the  Records
            strCount.Append(" SELECT COUNT(*)");
            strCount.Append(" FROM ");
            strCount.Append("(" + strSQL.ToString() + ")");

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));
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
            strSQLBuilder.Append(" SELECT  qry.* FROM ");
            strSQLBuilder.Append(" (SELECT ROWNUM SR_NO,T.* FROM ");
            strSQLBuilder.Append((" (" + strSQL.ToString() + ")"));
            strSQLBuilder.Append(" T) qry  WHERE SR_NO  Between " + start + " and " + last);
            strSQLBuilder.Append(" ORDER BY CHK DESC, CUSTOMER_ID ");
            try
            {
                return (new WorkFlow()).GetDataSet(strSQLBuilder.ToString());
                //Manjunath  PTS ID:Sep-02  17/09/2011
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


        #region "FetchCustomerName"
        public DataSet FetchCustomerForWorkFlow(string Process, string selectPK = "0", string Biztype = "3", string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, long LocFk = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            StringBuilder strSQLBuilder = new StringBuilder();
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            StringBuilder strCount = new StringBuilder();
            string strLocation = null;

            strLocation = "SELECT DISTINCT LOCATION_MST_FK";
            strLocation += "FROM";
            strLocation += "(SELECT PM.LOCATION_MST_FK as LOCATION_MST_FK ";
            strLocation += "FROM LOC_PORT_MAPPING_TRN LPM, PORT_MST_TBL PM";
            strLocation += "WHERE LPM.PORT_MST_FK=PM.PORT_MST_PK";
            strLocation += "AND LPM.LOCATION_MST_FK = " + LocFk;
            strLocation += "UNION";
            strLocation += "SELECT " + LocFk + " AS LOCATION_MST_FK from DUAL)";

            selectPK = selectPK.TrimEnd(',');
            //Grid Selection 
            // LocFk = 0  Its Display all the Customers
            // LocFk != 0 Based on the Location display the Customers
            if (string.IsNullOrEmpty(selectPK))
            {
                selectPK = "0";
            }

            if (LocFk == 0)
            {
                strSQL.Append(" SELECT * FROM ");
                strSQL.Append("( SELECT DISTINCT");
                strSQL.Append(" CUST.CUSTOMER_MST_PK,");
                strSQL.Append(" CUST.CUSTOMER_ID,");
                strSQL.Append(" CUST.CUSTOMER_NAME,");
                strSQL.Append(" '0' CHK");
                strSQL.Append(" FROM ");
                strSQL.Append(" CUSTOMER_MST_TBL CUST ,");
                strSQL.Append(" CUSTOMER_CATEGORY_TRN CR, ");

                strSQL.Append(" CUSTOMER_CONTACT_DTLS CTDL ");
                strSQL.Append(" WHERE ");
                strSQL.Append(" CUST.CUSTOMER_MST_PK = CR.CUSTOMER_MST_FK  AND ");
                strSQL.Append(" CTDL.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK AND ");
                strSQL.Append(" CUST.BUSINESS_TYPE IN (3," + Biztype + ")");
                if (selectPK == null | string.IsNullOrEmpty(selectPK))
                {
                    strSQL.Append(" AND CUST.CUSTOMER_MST_PK NOT IN (0) ");
                }
                else
                {
                    strSQL.Append(" AND CUST.CUSTOMER_MST_PK NOT IN (" + selectPK + " ) ");
                }
                strSQL.Append(" AND CUST.ACTIVE_FLAG =1 ");
                strSQL.Append(" AND CUST.Temp_Party <> 1");
                if (Process != "All")
                {
                    strSQL.Append(" AND CR.CUSTOMER_CATEGORY_MST_FK =(SELECT CUSTOMER_CATEGORY_MST_PK FROM CUSTOMER_CATEGORY_MST_TBL T  WHERE CUSTOMER_CATEGORY_ID LIKE'" + Process + "')");
                }
                strSQL.Append(" UNION ");
                strSQL.Append(" SELECT DISTINCT ");
                strSQL.Append(" CUST.CUSTOMER_MST_PK,");
                strSQL.Append(" CUST.CUSTOMER_ID,");
                strSQL.Append(" CUST.CUSTOMER_NAME,");
                strSQL.Append(" '1' CHK");
                strSQL.Append(" FROM ");
                strSQL.Append(" CUSTOMER_MST_TBL CUST,");
                strSQL.Append(" CUSTOMER_CATEGORY_TRN CR, ");
                strSQL.Append(" LOCATION_MST_TBL  L, ");
                strSQL.Append(" CUSTOMER_CONTACT_DTLS CTDL ");
                strSQL.Append(" WHERE");
                strSQL.Append(" CUST.CUSTOMER_MST_PK = CR.CUSTOMER_MST_FK  AND ");
                strSQL.Append(" CTDL.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK AND ");
                strSQL.Append(" CUST.BUSINESS_TYPE IN (3," + Biztype + ")");
                if (selectPK == null)
                {
                    strSQL.Append(" AND CUST.CUSTOMER_MST_PK IN (0)");
                    strSQL.Append(" AND CUST.ACTIVE_FLAG =1 ");
                    strSQL.Append(" AND CUST.Temp_Party <> 1");
                    if (Process != "All")
                    {
                        strSQL.Append(" AND CR.CUSTOMER_CATEGORY_MST_FK =(SELECT CUSTOMER_CATEGORY_MST_PK FROM CUSTOMER_CATEGORY_MST_TBL T  WHERE CUSTOMER_CATEGORY_ID LIKE'" + Process + "')");
                    }
                    strSQL.Append(" ) T ORDER BY CUSTOMER_ID,CHK DESC ");
                }
                else
                {
                    strSQL.Append(" AND CUST.CUSTOMER_MST_PK IN (" + selectPK + ") ");
                    strSQL.Append(" AND CUST.ACTIVE_FLAG =1");
                    strSQL.Append(" AND CUST.Temp_Party <> 1");
                    if (Process != "All")
                    {
                        strSQL.Append(" AND CR.CUSTOMER_CATEGORY_MST_FK =(SELECT CUSTOMER_CATEGORY_MST_PK FROM CUSTOMER_CATEGORY_MST_TBL T  WHERE CUSTOMER_CATEGORY_ID LIKE'" + Process + "')");
                    }
                    strSQL.Append(" ) T  ORDER BY CUSTOMER_ID,CHK DESC ");
                }

            }
            else
            {
                strSQL.Append(" SELECT * FROM ( ");
                strSQL.Append(" SELECT DISTINCT ");
                strSQL.Append(" CUST.CUSTOMER_MST_PK,");
                strSQL.Append(" CUST.CUSTOMER_ID,");
                strSQL.Append(" CUST.CUSTOMER_NAME,");
                strSQL.Append(" '0' CHK");
                //strSQL.Append(" CUST.SEA_CREDIT_LIMIT,")
                //strSQL.Append(" CUST.AIR_CREDIT_LIMIT,")
                //strSQL.Append(" (CUST.SEA_CREDIT_LIMIT + CUST.AIR_CREDIT_LIMIT) ""Total"" ")
                strSQL.Append(" FROM ");
                strSQL.Append(" CUSTOMER_MST_TBL      CUST,");
                strSQL.Append(" CUSTOMER_CATEGORY_TRN CR ,");
                strSQL.Append(" LOCATION_MST_TBL  L, ");
                strSQL.Append(" CUSTOMER_CONTACT_DTLS CTDL ");
                strSQL.Append(" WHERE ");
                strSQL.Append(" CUST.CUSTOMER_MST_PK = CR.CUSTOMER_MST_FK  AND ");
                strSQL.Append(" CUST.BUSINESS_TYPE IN (3," + Biztype + ") AND ");
                strSQL.Append(" CUST.CUSTOMER_MST_PK NOT IN (0, " + selectPK + ") AND");
                strSQL.Append(" CTDL.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK ");
                // strSQL.Append(" CTDL.ADM_LOCATION_MST_FK in (" & LocFk & ") AND")
                //strSQL.Append(" CTDL.ADM_LOCATION_MST_FK in (SELECT PM.LOCATION_MST_FK FROM LOC_PORT_MAPPING_TRN LPM, PORT_MST_TBL PM WHERE LPM.PORT_MST_FK=PM.PORT_MST_PK AND LPM.LOCATION_MST_FK = (" & LocFk & ") UNION SELECT (" & LocFk & ") AS LOCATION_MST_FK from DUAL) ") '(" & LocFk & ")
                strSQL.Append("AND L.LOCATION_MST_PK = CTDL.ADM_LOCATION_MST_FK");
                strSQL.Append(" AND L.LOCATION_MST_PK IN (" + strLocation + ")");
                strSQL.Append(" AND CUST.ACTIVE_FLAG = 1");
                strSQL.Append(" AND CUST.TEMP_PARTY <> 1");
                if (Process != "All")
                {
                    strSQL.Append(" AND CR.CUSTOMER_CATEGORY_MST_FK =(SELECT CUSTOMER_CATEGORY_MST_PK FROM CUSTOMER_CATEGORY_MST_TBL T  WHERE CUSTOMER_CATEGORY_ID LIKE'" + Process + "')");
                }
                strSQL.Append(" UNION");
                strSQL.Append(" SELECT DISTINCT");
                strSQL.Append(" CUST.CUSTOMER_MST_PK,");
                strSQL.Append(" CUST.CUSTOMER_ID,");
                strSQL.Append(" CUST.CUSTOMER_NAME,");
                strSQL.Append(" '1' CHK");
                strSQL.Append(" FROM");
                strSQL.Append(" CUSTOMER_MST_TBL CUST, ");
                strSQL.Append(" CUSTOMER_CATEGORY_TRN CR ,");
                strSQL.Append(" LOCATION_MST_TBL  L, ");
                strSQL.Append(" CUSTOMER_CONTACT_DTLS CTDL");
                strSQL.Append(" WHERE");
                strSQL.Append(" CUST.CUSTOMER_MST_PK = CR.CUSTOMER_MST_FK  AND ");
                strSQL.Append(" CUST.BUSINESS_TYPE IN ( 3," + Biztype + ")");
                strSQL.Append("AND L.LOCATION_MST_PK = CTDL.ADM_LOCATION_MST_FK");
                strSQL.Append(" AND L.LOCATION_MST_PK IN (" + strLocation + ")");
                if (Convert.ToInt32(selectPK) == 0 | string.IsNullOrEmpty(selectPK))
                {
                    strSQL.Append(" AND CUST.CUSTOMER_MST_PK  IN (0) ");
                }
                else
                {
                    strSQL.Append(" AND CUST.CUSTOMER_MST_PK IN (" + selectPK + " ) ");
                }
                strSQL.Append(" AND CTDL.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK ");
                //strSQL.Append(" AND CTDL.ADM_LOCATION_MST_FK in (SELECT PM.LOCATION_MST_FK FROM LOC_PORT_MAPPING_TRN LPM, PORT_MST_TBL PM WHERE LPM.PORT_MST_FK=PM.PORT_MST_PK AND LPM.LOCATION_MST_FK = (" & LocFk & ") UNION SELECT (" & LocFk & ") AS LOCATION_MST_FK from DUAL) ") '(" & LocFk & ")
                strSQL.Append(" AND CUST.ACTIVE_FLAG = 1");
                strSQL.Append(" AND CUST.TEMP_PARTY <> 1");
                if (Process != "All")
                {
                    strSQL.Append(" AND CR.CUSTOMER_CATEGORY_MST_FK =(SELECT CUSTOMER_CATEGORY_MST_PK FROM CUSTOMER_CATEGORY_MST_TBL T  WHERE CUSTOMER_CATEGORY_ID LIKE'" + Process + "')");
                }
                strSQL.Append(" ) T  ORDER BY CUSTOMER_ID,CHK DESC ");
            }
            //Count the  Records
            strCount.Append(" SELECT COUNT(*)");
            strCount.Append(" FROM ");
            strCount.Append("(" + strSQL.ToString() + ")");
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));
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
            strSQLBuilder.Append(" SELECT  qry.* FROM ");
            strSQLBuilder.Append(" (SELECT ROWNUM SR_NO,T.* FROM ");
            strSQLBuilder.Append((" (" + strSQL.ToString() + ")"));
            strSQLBuilder.Append(" T) qry  WHERE SR_NO  Between " + start + " and " + last);
            strSQLBuilder.Append(" ORDER BY CHK DESC, CUSTOMER_ID ");
            try
            {
                return (new WorkFlow()).GetDataSet(strSQLBuilder.ToString());
                //Manjunath  PTS ID:Sep-02  17/09/2011
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

        //Snigdharani - 13/08/2008 - Location or POL/POD Based Customer
        #region "Enhance-Search POL/POD Based Customer"
        public string FetchPOLPODBasedCust(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strSEARCH_CAT_PK_IN = "";
            string strLOCATION_IN = "";
            string strPortIN = "";
            string strAddressIN = null;
            int intBUSINESS_TYPE_IN = 3;
            Int16 ProcessType = 0;
            string strReq = null;
            arr = strCond.Split('~');;
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strSEARCH_CAT_PK_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strLOCATION_IN = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                intBUSINESS_TYPE_IN = Convert.ToInt32(arr.GetValue(4));
            if (arr.Length > 5)
                strPortIN = Convert.ToString(arr.GetValue(5));
            if (arr.Length > 6)
                strAddressIN = Convert.ToString(arr.GetValue(6));
            if (arr.Length > 7)
                ProcessType = Convert.ToInt16(arr.GetValue(7));
            if ((strAddressIN == null))
            {
                strAddressIN = "";
            }
            if (string.IsNullOrEmpty(strAddressIN))
            {
                strAddressIN = "";
            }

            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_CUSTOMER_PKG.POLPOD_BASED_CUSTOMER";
                var _with13 = SCM.Parameters;
                _with13.Add("CATEGORY_IN", ifDBNull(strSEARCH_CAT_PK_IN)).Direction = ParameterDirection.Input;
                _with13.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with13.Add("LOCATION_MST_FK_IN", strLOCATION_IN).Direction = ParameterDirection.Input;
                _with13.Add("BIZ_TYPE_IN", intBUSINESS_TYPE_IN).Direction = ParameterDirection.Input;
                _with13.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with13.Add("PORT_IN", ifDBNull(strPortIN)).Direction = ParameterDirection.Input;
                _with13.Add("ADDRESS_TYPE_IN", getDefault(strAddressIN, DBNull.Value)).Direction = ParameterDirection.Input;
                //Manoharan 03July2009: for Export Shipper PopUp should show all the customers based on the logged in location
                //for Import it should show all the customers based on the POL selected.
                _with13.Add("PROCESS_TYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with13.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                OracleClob clob = null;
                clob = (OracleClob)SCM.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
                //Manjunath  PTS ID:Sep-02  17/09/2011
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
                SCM.Connection.Close();
            }
        }
        #endregion
        #region "Enhance-Search Location Based Customers"
        public string FetchLocationBasedCust(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            int VoyagePk = 0;
            string strSERACH_IN = "";
            string strSEARCH_CAT_PK_IN = "";
            string strLOCATION_IN = "";
            int intBUSINESS_TYPE_IN = 3;
            string SEARCH_FLAG_IN = "";
            var strNull = DBNull.Value;
            string strReq = null;
            arr = strCond.Split('~');;
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strSEARCH_CAT_PK_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strLOCATION_IN = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                intBUSINESS_TYPE_IN = Convert.ToInt32(arr.GetValue(4));
            if (arr.Length > 5)
                SEARCH_FLAG_IN = Convert.ToString(arr.GetValue(5));
            if (arr.Length > 6)
                VoyagePk = Convert.ToInt32(arr.GetValue(6));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_CUSTOMER_PKG.LOCATION_BASED_CUSTOMER_TEST";
                //SCM.CommandText = objWF.MyUserName & ".EN_CUSTOMER_PKG.LOCATION_BASED_CUSTOMER"
                var _with14 = SCM.Parameters;
                _with14.Add("CATEGORY_IN", ifDBNull(strSEARCH_CAT_PK_IN)).Direction = ParameterDirection.Input;
                _with14.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                // .Add("LOCATION_MST_FK_IN", strLOCATION_IN).Direction = ParameterDirection.Input
                _with14.Add("LOCATION_MST_FK_IN", (string.IsNullOrEmpty(strLOCATION_IN) ? "" : strLOCATION_IN)).Direction = ParameterDirection.Input;
                _with14.Add("BIZ_TYPE_IN", intBUSINESS_TYPE_IN).Direction = ParameterDirection.Input;
                _with14.Add("SEARCH_FLAG_IN", (string.IsNullOrEmpty(SEARCH_FLAG_IN) ? "" : SEARCH_FLAG_IN)).Direction = ParameterDirection.Input;
                _with14.Add("VoyagePk_IN", VoyagePk).Direction = ParameterDirection.Input;
                _with14.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with14.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                OracleClob clob = null;
                clob = (OracleClob)SCM.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
                //Manjunath  PTS ID:Sep-02  17/09/2011
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
                SCM.Connection.Close();
            }
        }
        #endregion
        //End Snigdharani

        //ADDED BY MINAKSHI FOR REMOVALS-7-JANUARY-2008(ONLY BUSINESS TYPE REMOVALS AND ACTIVE CUSTOMER)
        #region "Enhance-Search Removals Based Customer"
        public string FetchRemovalsBasedCustomer(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strSEARCH_CAT_PK_IN = "";
            string strLOCATION_IN = "";
            int strPortIN = 0;
            string strAddressIN = null;
            int intBUSINESS_TYPE_IN = 3;
            string strReq = null;
            arr = strCond.Split('~');;
            strReq = Convert.ToString(arr.GetValue(0));

            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                intBUSINESS_TYPE_IN = Convert.ToInt32(arr.GetValue(2));



            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_CUSTOMER_PKG.GET_REMOVAL_ENQUIRY_CUSTOMER";
                var _with15 = SCM.Parameters;
                _with15.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with15.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with15.Add("BUSINESS_TYPE_IN", intBUSINESS_TYPE_IN).Direction = ParameterDirection.Input;
                _with15.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
                //Manjunath  PTS ID:Sep-02  17/09/2011
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
                SCM.Connection.Close();
            }
        }
        #endregion
        //ENDED BY MINAKSHI

        #region "Fetch Location"
        public Int32 FetchCountryPK()
        {
            string SqlStr = null;
            WorkFlow objWF = new WorkFlow();
            SqlStr = " SELECT L.COUNTRY_MST_FK FROM LOCATION_MST_TBL L WHERE L.LOCATION_MST_PK= " + HttpContext.Current.Session["LOGED_IN_LOC_FK"];
            try
            {
                return Convert.ToInt32(objWF.ExecuteScaler(SqlStr));
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
            return 0;
        }
        #endregion

        #region "AffiliateCustomer"
        public DataSet FetchAfiiliateCustomer(string selectPK = "0", string Biztype = "3", string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, long LocFk = 0, bool CustGrp = false, string lblCustPK = "0")
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            StringBuilder strSQLBuilder = new StringBuilder();
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            StringBuilder strCount = new StringBuilder();
            selectPK = selectPK.TrimEnd(',');

            if (string.IsNullOrEmpty(selectPK))
            {
                selectPK = "0";
            }
            strSQL.Append(" SELECT * FROM ( ");
            strSQL.Append("  SELECT ");
            strSQL.Append(" CMT.CUSTOMER_MST_PK,");
            strSQL.Append(" CMT.CUSTOMER_ID,");
            strSQL.Append(" CMT.CUSTOMER_NAME,");
            strSQL.Append(" '0' CHK");
            strSQL.Append(" FROM ");
            strSQL.Append(" CUSTOMER_MST_TBL      CMT,");
            strSQL.Append(" CUSTOMER_CONTACT_DTLS CCD, ");
            strSQL.Append(" LOCATION_MST_TBL      LMT,");
            strSQL.Append(" PORT_MST_TBL          POL");
            strSQL.Append(" WHERE ");
            strSQL.Append("  CMT.BUSINESS_TYPE IN (3," + Biztype + ")  ");
            strSQL.Append("  AND CMT.CUSTOMER_MST_PK NOT IN (0, " + selectPK + ") ");
            strSQL.Append("  AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK ");
            strSQL.Append("  AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
            strSQL.Append("  AND LMT.LOCATION_MST_PK = POL.LOCATION_MST_FK(+) ");
            strSQL.Append("  AND CMT.ACTIVE_FLAG = 1");
            strSQL.Append("  AND CMT.TEMP_PARTY <> 1");
            if (Convert.ToInt32(lblCustPK) != 0)
            {
                strSQL.Append(" AND POL.LOCATION_MST_FK NOT IN (" + lblCustPK + ")");
            }
            strSQL.Append(" UNION");
            strSQL.Append(" SELECT ");
            strSQL.Append(" CMT.CUSTOMER_MST_PK,");
            strSQL.Append(" CMT.CUSTOMER_ID,");
            strSQL.Append(" CMT.CUSTOMER_NAME,");
            strSQL.Append(" '1' CHK");
            strSQL.Append(" FROM");
            strSQL.Append(" CUSTOMER_MST_TBL CMT, ");
            strSQL.Append(" CUSTOMER_CONTACT_DTLS CCD,");
            strSQL.Append(" LOCATION_MST_TBL      LMT,");
            strSQL.Append(" PORT_MST_TBL          POL");
            strSQL.Append(" WHERE");
            strSQL.Append(" CMT.BUSINESS_TYPE IN ( 3," + Biztype + ")");
            if (Convert.ToInt32(selectPK) == 0 | string.IsNullOrEmpty(selectPK))
            {
                strSQL.Append(" AND CMT.CUSTOMER_MST_PK  IN (0) ");
            }
            else
            {
                strSQL.Append(" AND CMT.CUSTOMER_MST_PK IN (" + selectPK + " ) ");
            }
            strSQL.Append("  AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
            strSQL.Append("  AND LMT.LOCATION_MST_PK = POL.LOCATION_MST_FK(+) ");
            strSQL.Append("  AND CMT.ACTIVE_FLAG = 1");
            strSQL.Append("  AND CMT.TEMP_PARTY <> 1");
            if (Convert.ToInt32(lblCustPK) != 0)
            {
                strSQL.Append(" AND POL.LOCATION_MST_FK NOT IN (" + lblCustPK + ")");
            }
            strSQL.Append(" ) T  ORDER BY CUSTOMER_ID,CHK DESC ");
            //Count the  Records
            strCount.Append(" SELECT COUNT(*)");
            strCount.Append(" FROM ");
            strCount.Append("(" + strSQL.ToString() + ")");
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));
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
            strSQLBuilder.Append(" SELECT  qry.* FROM ");
            strSQLBuilder.Append(" (SELECT ROWNUM SR_NO,T.* FROM ");
            strSQLBuilder.Append((" (" + strSQL.ToString() + ")"));
            strSQLBuilder.Append(" T) qry  WHERE SR_NO  Between " + start + " and " + last);
            strSQLBuilder.Append(" ORDER BY CHK DESC, CUSTOMER_ID ");
            try
            {
                return (new WorkFlow()).GetDataSet(strSQLBuilder.ToString());
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

        #region "Fetch IsAdmin"
        public string Fetch_IsAdmin(int UserPk)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append(" SELECT U.ADMINISTRATOR FROM USER_MST_TBL U WHERE U.USER_MST_PK=" + UserPk);
            return (new WorkFlow()).ExecuteScaler(sb.ToString());
        }
        #endregion
    }
}