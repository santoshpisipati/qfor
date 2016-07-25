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
using System.Data;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class clsVendorDetails : CommonFeatures
    {
        #region "Fetch Catagories"

        /// <summary>
        /// Fetches the customer catagories.
        /// </summary>
        /// <param name="VendorPk">The vendor pk.</param>
        /// <returns></returns>
        public DataSet fetchCustCatagories(string VendorPk = "")
        {
            string strSQL = null;
            strSQL = "select ROWNUM SR_NO,q.* from (";
            if (!string.IsNullOrEmpty(VendorPk))
            {
                strSQL += " SELECT VT.VENDOR_TYPE_PK ,";
                strSQL += " VT.VENDOR_TYPE_ID ,";
                strSQL += " VT.VENDOR_TYPE_NAME,";
                strSQL += " (SELECT QDT.DD_ID ";
                strSQL += " FROM QFOR_DROP_DOWN_TBL QDT ";
                strSQL += " WHERE QDT.DD_FLAG = 'SUPP_TYPE' ";
                strSQL += " AND QDT.CONFIG_ID = 'QFOR4047' ";
                strSQL += " AND QDT.DD_VALUE = VST.SUPP_TYPE) SUPP_TYPE,";
                strSQL += " 1 SELECTE";
                strSQL += " FROM VENDOR_TYPE_MST_TBL VT,VENDOR_MST_TBL VM,VENDOR_SERVICES_TRN  VST";
                strSQL += " WHERE VT.VENDOR_TYPE_PK = VST.VENDOR_TYPE_FK ";
                strSQL += " And VM.VENDOR_MST_PK = VST.VENDOR_MST_FK ";
                strSQL += " AND VST.VENDOR_MST_FK=" + VendorPk;
                strSQL += " AND VT.ACTIVE_FLAG=1";
                strSQL += " UNION";
            }
            strSQL += " SELECT VT.VENDOR_TYPE_PK ,";
            strSQL += " VT.VENDOR_TYPE_ID ,";
            strSQL += " VT.VENDOR_TYPE_NAME,";
            strSQL += " (SELECT QDT.DD_ID ";
            strSQL += " FROM QFOR_DROP_DOWN_TBL QDT ";
            strSQL += " WHERE QDT.DD_FLAG = 'SUPP_TYPE' ";
            strSQL += " AND QDT.CONFIG_ID = 'QFOR4047' ";
            strSQL += " AND QDT.DD_VALUE = 1) SUPP_TYPE,";
            strSQL += " 0 SELECTE";
            strSQL += " FROM VENDOR_TYPE_MST_TBL VT";
            strSQL += " WHERE 1=1";
            if (!string.IsNullOrEmpty(VendorPk))
            {
                strSQL += " AND VT.VENDOR_TYPE_PK NOT IN (SELECT VT.VENDOR_TYPE_PK";
                strSQL += " FROM VENDOR_TYPE_MST_TBL VT,VENDOR_MST_TBL VM,VENDOR_SERVICES_TRN  VST";
                strSQL += " WHERE(VT.VENDOR_TYPE_PK = VST.VENDOR_TYPE_FK )";
                strSQL += " And VM.VENDOR_MST_PK = VST.VENDOR_MST_FK";
                strSQL += " AND VST.VENDOR_MST_FK=" + VendorPk;
                strSQL += " AND VT.ACTIVE_FLAG=1 )";
            }
            strSQL += " AND VT.ACTIVE_FLAG=1 ";
            strSQL += " order by VENDOR_TYPE_ID)q";
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

        #endregion "Fetch Catagories"

        #region "Fetch Details"

        /// <summary>
        /// Fetches the by procedure.
        /// </summary>
        /// <param name="VendorPk">The vendor pk.</param>
        /// <returns></returns>
        public DataSet FetchByProcedure(int VendorPk = 0)
        {
            try
            {
                WorkFlow objWF = new WorkFlow();

                DataTable dt = null;
                DataSet ds = new DataSet();
                objWF.MyCommand.Parameters.Clear();
                var _with1 = objWF.MyCommand.Parameters;
                _with1.Add("VENDOR_MST_FK_IN", VendorPk).Direction = ParameterDirection.Input;
                _with1.Add("VENDOR_CUR", System.Data.OracleClient.OracleType.Cursor).Direction = ParameterDirection.Output;
                dt = objWF.GetDataTable("VENDOR_MST_TBL_PKG", "FETCH_DATA");
                ds.Tables.Add(dt);
                return ds;
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 13/09/2011, PTS ID: SEP-01
            }
            catch (Exception Exp)
            {
                throw Exp;
            }
        }

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="VendorPK">The vendor pk.</param>
        /// <param name="businesstype">The businesstype.</param>
        /// <returns></returns>
        public DataSet FetchAll(string VendorPK = "", Int32 businesstype = 0)
        {
            string strSQL = null;
            string strCondition = null;
            WorkFlow objWF = new WorkFlow();
            strCondition = "AND VCD.VENDOR_MST_FK = '" + VendorPK + "' AND VMT.VENDOR_MST_PK = '" + VendorPK + "'";
            strSQL = " SELECT VMT.VENDOR_ID,";
            strSQL = strSQL + "VMT.VENDOR_MST_PK, ";
            strSQL = strSQL + "VMT.VENDOR_NAME,";
            strSQL = strSQL + "VMT.ACTIVE,";
            strSQL = strSQL + "DECODE(VMT.BUSINESS_TYPE,'0','','1','Air','2','Sea','3','Both') BUSINESS_TYPE,";
            strSQL = strSQL + "VMT.VAT_NO,";
            strSQL = strSQL + "VMT.ACCOUNT_NO,";
            strSQL = strSQL + "VCD.ADM_ADDRESS_1,";
            strSQL = strSQL + "VCD.ADM_ADDRESS_2,";
            strSQL = strSQL + "VCD.ADM_ADDRESS_3,";
            strSQL = strSQL + "VCD.ADM_CITY,";
            strSQL = strSQL + "VCD.ADM_ZIP_CODE,";
            strSQL = strSQL + "VCD.ADM_LOCATION_MST_FK,";
            strSQL = strSQL + "VCD.ADM_CONTACT_PERSON,";
            strSQL = strSQL + "VCD.ADM_PHONE,";
            strSQL = strSQL + "VCD.ADM_MOBILE,";
            strSQL = strSQL + "VCD.ADM_FAX_NO,";
            strSQL = strSQL + "VCD.ADM_EMAIL_ID,";
            strSQL = strSQL + "VCD.ADM_URL,";
            strSQL = strSQL + "VCD.ADM_COUNTRY_MST_FK,";

            strSQL = strSQL + "VCD.COR_ADDRESS_1,";
            strSQL = strSQL + "VCD.COR_ADDRESS_2,";
            strSQL = strSQL + "VCD.COR_ADDRESS_3,";
            strSQL = strSQL + "VCD.COR_CITY,";
            strSQL = strSQL + "VCD.COR_ZIP_CODE,";
            strSQL = strSQL + "VCD.COR_LOCATION_MST_FK,";
            strSQL = strSQL + "VCD.COR_CONTACT_PERSON,";
            strSQL = strSQL + "VCD.COR_PHONE,";
            strSQL = strSQL + "VCD.COR_MOBILE,";
            strSQL = strSQL + "VCD.COR_FAX_NO,";
            strSQL = strSQL + "VCD.COR_EMAIL_ID,";
            strSQL = strSQL + "VCD.COR_URL,";
            strSQL = strSQL + "VCD.COR_COUNTRY_MST_FK,";

            strSQL = strSQL + "VCD.BILL_ADDRESS_1,";
            strSQL = strSQL + "VCD.BILL_ADDRESS_2,";
            strSQL = strSQL + "VCD.BILL_ADDRESS_3,";
            strSQL = strSQL + "VCD.BILL_CITY,";
            strSQL = strSQL + "VCD.BILL_ZIP_CODE,";
            strSQL = strSQL + "VCD.BILL_LOCATION_MST_FK,";
            strSQL = strSQL + "VCD.BILL_CONTACT_PERSON,";
            strSQL = strSQL + "VCD.BILL_PHONE,";
            strSQL = strSQL + "VCD.BILL_MOBILE,";
            strSQL = strSQL + "VCD.BILL_FAX_NO,";
            strSQL = strSQL + "VCD.BILL_EMAIL_ID,";
            strSQL = strSQL + "VCD.BILL_URL,";
            strSQL = strSQL + "VCD.ADM_SHORT_NAME,";
            strSQL = strSQL + "VCD.COR_SHORT_NAME,";
            strSQL = strSQL + "VCD.BILL_SHORT_NAME,";
            strSQL = strSQL + "VCD.BILL_COUNTRY_MST_FK,";

            strSQL = strSQL + " VMT.VERSION_NO,";
            strSQL = strSQL + " VCD.ADM_SALUTATION,";
            strSQL = strSQL + " VCD.COR_SALUTATION,";
            strSQL = strSQL + " VCD.BILL_SALUTATION,";
            strSQL = strSQL + " FROM VENDOR_MST_TBL VMT,VENDOR_CONTACT_DTLS VCD";
            strSQL = strSQL + "WHERE ( 1 = 1) ";
            strSQL = strSQL + strCondition;

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

        #endregion "Fetch Details"

        #region "location"

        /// <summary>
        /// Selects the location.
        /// </summary>
        /// <param name="fkLocation">The fk location.</param>
        /// <returns></returns>
        public string SelectLocation(int fkLocation)
        {
            try
            {
                string strSQL = null;
                WorkFlow objWF = new WorkFlow();
                strSQL = "select LOCATION_NAME from LOCATION_MST_TBL where LOCATION_MST_PK = " + fkLocation + " ";
                string LocName = null;
                LocName = objWF.ExecuteScaler(strSQL);
                return LocName;
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

        /// <summary>
        /// Gets the loc country.
        /// </summary>
        /// <returns></returns>
        public object GetLocCountry()
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string LocPK = null;
            WorkFlow objWF = new WorkFlow();
            LocPK = HttpContext.Current.Session["LOGED_IN_LOC_FK"].ToString();
            sb.Append("SELECT L.LOCATION_MST_PK,");
            sb.Append("       L.LOCATION_ID,");
            sb.Append("       L.LOCATION_NAME,");
            sb.Append("       CMT.COUNTRY_MST_PK,");
            sb.Append("       CMT.COUNTRY_ID,");
            sb.Append("       CMT.COUNTRY_NAME");
            sb.Append("  FROM LOCATION_MST_TBL L, COUNTRY_MST_TBL CMT");
            sb.Append("  WHERE L.COUNTRY_MST_FK = CMT.COUNTRY_MST_PK");
            sb.Append("  AND L.LOCATION_MST_PK=" + LocPK);
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "location"

        #region "For CHA Agent"

        /// <summary>
        /// Fetches all cha agent.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchAllCHAAgent(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string C = null;
            string C1 = null;
            string C2 = null;
            string C3 = null;
            string C4 = null;
            string C5 = null;
            string C6 = null;
            string C7 = null;
            string C8 = null;
            string C9 = null;
            string C10 = null;
            string User_pk = null;
            string strSearchFlag_IN = null;
            string strFromToType = "";
            var strNull = "";
            string SelectedLoc = null;
            string strLOOKUP_IN = null;
            arr = strCond.Split('~');
            //***********************
            // lOOK UP VALUE E OR L
            strLOOKUP_IN = Convert.ToString(arr.GetValue(0));

            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (strSERACH_IN.IndexOf("$") != -1)
            {
                strSERACH_IN = strSERACH_IN.Replace("$", "&");
            }
            strSERACH_IN = strSERACH_IN.TrimEnd(',');
            //***********************
            User_pk = Convert.ToString(HttpContext.Current.Session["USER_PK"]);
            if (arr.Length > 2)
            {
                C2 = Convert.ToString(arr.GetValue(2));
            }
            if (arr.Length > 3)
            {
                C3 = Convert.ToString(arr.GetValue(3));
            }
            if (arr.Length > 4)
            {
                C4 = Convert.ToString(arr.GetValue(4));
            }

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_VENDOR_PKG.COMMON_CHA_AGENT";

                var _with12 = selectCommand.Parameters;
                _with12.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with12.Add("LOOKUP_VALUE_IN", strLOOKUP_IN).Direction = ParameterDirection.Input;
                _with12.Add("LOCATION_MST_FK_IN", (string.IsNullOrEmpty(C2) ? strNull : C2)).Direction = ParameterDirection.Input;
                _with12.Add("BUSINESS_TYPE_IN", (string.IsNullOrEmpty(C3) ? strNull : C3)).Direction = ParameterDirection.Input;
                _with12.Add("SUPP_TYPE_IN", (string.IsNullOrEmpty(C4) ? strNull : C4)).Direction = ParameterDirection.Input;
                _with12.Add("RETURN_VALUE", OracleDbType.Varchar2, 32767, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString((!string.IsNullOrEmpty(selectCommand.Parameters["RETURN_VALUE"].Value.ToString()) ? "" : selectCommand.Parameters["RETURN_VALUE"].Value));
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

        #endregion "For CHA Agent"
    }
}