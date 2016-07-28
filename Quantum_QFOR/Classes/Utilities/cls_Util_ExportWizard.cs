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
using Oracle.ManagedDataAccess.Types;
using System;
using System.Collections;
using System.Data;
using System.Text;

namespace Quantum_QFOR
{
    public class clsUtil_ExportWizard : CommonFeatures
    {

        #region "Fetch Master Forms + Enhance Search + Lookup Search Block"
        public object FetchMasterForms(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSEARCH_IN = null;
            string strReq = null;
            string strBranchpk = null;
            string strSearchType = null;
            arr = strCond.Split('~');
            strReq =Convert.ToString(arr.GetValue(0));;
            strSEARCH_IN = Convert.ToString(arr.GetValue(1));

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_UTIL_EXPORT_WIZARD_PKG.SP_GET_MASTER_FORMS";

                var _with1 = selectCommand.Parameters;
                _with1.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSEARCH_IN) ? strSEARCH_IN : "")).Direction = ParameterDirection.Input;
                _with1.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", OracleDbType.Varchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
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

        #region "fn_Get_Export_Tablesnames"
        public DataSet fn_Get_Export_Tablesnames(Int64 MasterPK)
        {

            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

            sb.Append(" select distinct(mp.master_table_name || '  ' || mp.master_table_alias_name ) name");
            sb.Append("          from qcor_gen_master_tbl_map_fields mp");
            sb.Append("         where mp.master_fk = " + MasterPK + " ");

            try
            {
                ds = objWF.GetDataSet(sb.ToString());
                return ds;
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

        #region "Get Tables and Columns"
        public DataSet fn_Get_Export_Tablecolumns(Int64 MasterPK, Int32 IsActive = 1)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append(" SELECT * ");
            sb.Append("  FROM QCOR_GEN_MASTER_TBL_MAP_FIELDS MP");
            sb.Append("  WHERE MP.MASTER_FK = " + MasterPK + "");
            if (IsActive == 1)
            {
                sb.Append("          AND MP.IS_ACTIVE IN (1)");
            }
            else if (IsActive == 2)
            {
                sb.Append("          AND MP.IS_ACTIVE IN (1,2)");
            }
            else
            {
                sb.Append("          AND MP.IS_ACTIVE = 0");
            }
            sb.Append("  ORDER BY MP.PRIORITY");
            try
            {
                ds = objWF.GetDataSet(sb.ToString());
                return ds;
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

        #region "fn_Get_Export_Columnnames"
        public DataSet fn_Get_Export_Columnnames(Int64 MasterPK, Int32 IsActive = 1)
        {

            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append(" SELECT   MP.MASTER_FIELD_DESCRIPTION,  MP.MASTER_FIELD_NAME");
            sb.Append("          FROM QCOR_GEN_MASTER_TBL_MAP_FIELDS MP");
            sb.Append("         WHERE MP.MASTER_FK = " + MasterPK + " ");
            if (IsActive == 1)
            {
                sb.Append("          AND MP.IS_ACTIVE in (1)");
            }
            else if (IsActive == 2)
            {
                sb.Append("          AND MP.IS_ACTIVE in (1,2)");
            }
            sb.Append("          ORDER BY MP.PRIORITY ");
            try
            {
                ds = objWF.GetDataSet(sb.ToString());
                return ds;
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

        #region "fn_Get_Export_ColumnRelations"
        public DataSet fn_Get_Export_ColumnRelations(Int64 MasterPK, Int32 IsActive = 1)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append(" SELECT (MP.MASTER_TABLE_NAME || ' ' || MP.MASTER_TABLE_ALIAS_NAME) ALIAS_NAME, (MP.RELATED_TABLE_NAME || ' ' || MP.RELATED_TABLE_ALIAS_NAME) NAME, (MP.MASTER_TABLE_ALIAS_NAME || '.'|| MP.MASTER_FIELD_NAME ||'='||MP.RELATED_TABLE_ALIAS_NAME||'.'||MP.RELATED_FIELD_NAME) RELATED_FIELD_NAME");
            sb.Append("          FROM QCOR_GEN_MASTER_TBL_MAP_FIELDS MP");
            sb.Append("         WHERE MP.MASTER_FK = " + MasterPK + "  ");
            sb.Append("         AND MP.RELATED_FIELD_NAME  IS NOT NULL");
            if (IsActive == 1)
            {
                sb.Append("          AND MP.IS_ACTIVE = 1");
            }
            sb.Append("          ORDER BY MP.PRIORITY ");
            try
            {
                ds = objWF.GetDataSet(sb.ToString());
                return ds;
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

        #region "fn_Get_Export_Data"
        public DataSet fn_Get_Export_Data(string sbQry)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();
            try
            {
                ds = objWF.GetDataSet(sbQry.ToString());
                return ds;
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

        #region "fn_Get_Export_qry"
        public DataSet fn_Get_Export_qry(Int64 MasterPK)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT * FROM QCOR_GEN_MASTER_TBL_MAP_FIELDS MP WHERE MP.MASTER_FK " + MasterPK + "  ");
            try
            {
                ds = objWF.GetDataSet(sb.ToString());
                return ds;
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
        public string fn_Get_Import_UniqueCheck(string TableName, string FieldPK = "", string FieldName = "", string FieldValue = "SACHIN")
        {
            WorkFlow objWF = new WorkFlow();
            string RelatedPK = "0";
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

            sb.Append(" SELECT DISTINCT (" + TableName + "." + FieldPK + ") NAME");
            sb.Append("          FROM " + TableName + " ");
            sb.Append("         WHERE UPPER(" + FieldName + ") =  UPPER('" + Convert.ToString(FieldValue) + "') ");

            try
            {
                RelatedPK = objWF.ExecuteScaler(sb.ToString());
                return RelatedPK;
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

        #region " SaveUploadTemp "
        public ArrayList SaveUploadTemp(ref DataSet M_DataSet)
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction oraTran = null;
            OracleCommand insCommand = new OracleCommand();
            Int32 RecAfct = default(Int32);
            int i = 0;
            int delflg = 1;
            string strUploadMessage = null;

            try
            {
                objWK.OpenConnection();

                oraTran = objWK.MyConnection.BeginTransaction();
                objWK.MyCommand.Transaction = oraTran;

                var _with2 = insCommand;
                _with2.Connection = objWK.MyConnection;
                _with2.CommandType = CommandType.StoredProcedure;
                _with2.CommandText = objWK.MyUserName + ".CUSTOMER_UPLOAD_PKG.CUSTOMER_UPLOAD_TEMP_INS";

                for (i = 0; i <= M_DataSet.Tables[0].Rows.Count - 1; i++)
                {
                    _with2.Parameters.Clear();
                    _with2.Parameters.Add("CUSTOMER_ID_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["CUSTOMER_ID"].ToString()) ? DBNull.Value : M_DataSet.Tables[0].Rows[i]["CUSTOMER_ID"])).Direction = ParameterDirection.Input;
                    _with2.Parameters.Add("CUSTOMER_NAME_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["CUSTOMER_NAME"].ToString()) ? DBNull.Value : M_DataSet.Tables[0].Rows[i]["CUSTOMER_NAME"])).Direction = ParameterDirection.Input;
                    _with2.Parameters.Add("BUSINESS_TYPE_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["BUSINESS_TYPE"].ToString()) ? DBNull.Value : M_DataSet.Tables[0].Rows[i]["BUSINESS_TYPE"])).Direction = ParameterDirection.Input;
                    _with2.Parameters.Add("VAT_NO_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["VAT_NO"].ToString()) ? DBNull.Value : M_DataSet.Tables[0].Rows[i]["VAT_NO"])).Direction = ParameterDirection.Input;
                    _with2.Parameters.Add("ACCOUNT_NO_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["ACCOUNT_NO"].ToString()) ? DBNull.Value : M_DataSet.Tables[0].Rows[i]["ACCOUNT_NO"])).Direction = ParameterDirection.Input;
                    _with2.Parameters.Add("TAX_STATUS_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["TAX_STATUS"].ToString()) ? DBNull.Value : M_DataSet.Tables[0].Rows[i]["TAX_STATUS"])).Direction = ParameterDirection.Input;
                    _with2.Parameters.Add("PRIORITY_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["PRIORITY"].ToString()) ? DBNull.Value : M_DataSet.Tables[0].Rows[i]["PRIORITY"])).Direction = ParameterDirection.Input;

                    _with2.Parameters.Add("ADM_ADDRESS_1_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["ADM_ADDRESS_1"].ToString()) ? DBNull.Value : M_DataSet.Tables[0].Rows[i]["ADM_ADDRESS_1"])).Direction = ParameterDirection.Input;
                    _with2.Parameters.Add("ADM_CITY_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["ADM_CITY"].ToString()) ? DBNull.Value : M_DataSet.Tables[0].Rows[i]["ADM_CITY"])).Direction = ParameterDirection.Input;
                    _with2.Parameters.Add("ADM_ZIP_CODE_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["ADM_ZIP_CODE"].ToString()) ? DBNull.Value : M_DataSet.Tables[0].Rows[i]["ADM_ZIP_CODE"])).Direction = ParameterDirection.Input;
                    _with2.Parameters.Add("LOCATION_ID_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["LOCATION_ID"].ToString()) ? DBNull.Value : M_DataSet.Tables[0].Rows[i]["LOCATION_ID"])).Direction = ParameterDirection.Input;
                    _with2.Parameters.Add("ADM_PHONE_NO_1_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["ADM_PHONE_NO_1"].ToString()) ? DBNull.Value : M_DataSet.Tables[0].Rows[i]["ADM_PHONE_NO_1"])).Direction = ParameterDirection.Input;
                    _with2.Parameters.Add("ADM_PHONE_NO_2_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["ADM_PHONE_NO_2"].ToString()) ? DBNull.Value : M_DataSet.Tables[0].Rows[i]["ADM_PHONE_NO_2"])).Direction = ParameterDirection.Input;
                    _with2.Parameters.Add("ADM_FAX_NO_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["ADM_FAX_NO"].ToString()) ? DBNull.Value : M_DataSet.Tables[0].Rows[i]["ADM_FAX_NO"])).Direction = ParameterDirection.Input;
                    _with2.Parameters.Add("ADM_EMAIL_ID_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["ADM_EMAIL_ID"].ToString()) ? DBNull.Value : M_DataSet.Tables[0].Rows[i]["ADM_EMAIL_ID"])).Direction = ParameterDirection.Input;
                    _with2.Parameters.Add("ADM_URL_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["ADM_URL"].ToString()) ? DBNull.Value : M_DataSet.Tables[0].Rows[i]["ADM_URL"])).Direction = ParameterDirection.Input;

                    _with2.Parameters.Add("COUNTRY_ID_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["COUNTRY_ID"].ToString()) ? DBNull.Value : M_DataSet.Tables[0].Rows[i]["COUNTRY_ID"])).Direction = ParameterDirection.Input;
                    _with2.Parameters.Add("COR_ADDRESS_1_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["COR_ADDRESS_1"].ToString()) ? DBNull.Value : M_DataSet.Tables[0].Rows[i]["COR_ADDRESS_1"])).Direction = ParameterDirection.Input;
                    _with2.Parameters.Add("BILL_ADDRESS_1_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["BILL_ADDRESS_1"].ToString()) ? DBNull.Value : M_DataSet.Tables[0].Rows[i]["BILL_ADDRESS_1"])).Direction = ParameterDirection.Input;

                    _with2.Parameters.Add("NAME_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["NAME"].ToString()) ? DBNull.Value : M_DataSet.Tables[0].Rows[i]["NAME"])).Direction = ParameterDirection.Input;
                    _with2.Parameters.Add("ALIAS_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["ALIAS"].ToString()) ? DBNull.Value : M_DataSet.Tables[0].Rows[i]["ALIAS"])).Direction = ParameterDirection.Input;
                    _with2.Parameters.Add("DESIGNATION_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["DESIGNATION"].ToString()) ? DBNull.Value : M_DataSet.Tables[0].Rows[i]["DESIGNATION"])).Direction = ParameterDirection.Input;
                    _with2.Parameters.Add("RESPONSIBILITY_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["RESPONSIBILITY"].ToString()) ? DBNull.Value : M_DataSet.Tables[0].Rows[i]["RESPONSIBILITY"])).Direction = ParameterDirection.Input;
                    _with2.Parameters.Add("DIR_PHONE_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["DIR_PHONE"].ToString()) ? DBNull.Value : M_DataSet.Tables[0].Rows[i]["DIR_PHONE"])).Direction = ParameterDirection.Input;
                    _with2.Parameters.Add("MOBILE_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["MOBILE"].ToString()) ? DBNull.Value : M_DataSet.Tables[0].Rows[i]["MOBILE"])).Direction = ParameterDirection.Input;
                    _with2.Parameters.Add("FAX_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["FAX"].ToString()) ? DBNull.Value : M_DataSet.Tables[0].Rows[i]["FAX"])).Direction = ParameterDirection.Input;
                    _with2.Parameters.Add("EMAIL_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["EMAIL"].ToString()) ? DBNull.Value : M_DataSet.Tables[0].Rows[i]["EMAIL"])).Direction = ParameterDirection.Input;
                    _with2.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;

                    _with2.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;

                    var _with3 = objWK.MyDataAdapter;
                    _with3.InsertCommand = insCommand;
                    _with3.InsertCommand.Transaction = oraTran;
                    RecAfct = RecAfct + _with3.InsertCommand.ExecuteNonQuery();
                    delflg = 0;
                }

                if (arrMessage.Count <= 0 & RecAfct > 0)
                {
                    var _with4 = insCommand;
                    _with4.Connection = objWK.MyConnection;
                    _with4.CommandType = CommandType.StoredProcedure;
                    _with4.CommandText = objWK.MyUserName + ".CUSTOMER_UPLOAD_PKG.CUSTOMER_VALIDATE_AND_INS";

                    _with4.Parameters.Clear();
                    _with4.Parameters.Add("RETURN_VALUE", OracleDbType.Clob, 10000, "RETURN_VALUE").Direction = ParameterDirection.Output;

                    var _with5 = objWK.MyDataAdapter;
                    _with5.InsertCommand = insCommand;
                    _with5.InsertCommand.Transaction = oraTran;
                    RecAfct = _with5.InsertCommand.ExecuteNonQuery();

                    OracleClob clob = null;
                    clob = (OracleClob)insCommand.Parameters["RETURN_VALUE"].Value;
                    System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                    strUploadMessage = strReader.ReadToEnd();
                }

                if (arrMessage.Count > 0)
                {
                    oraTran.Rollback();
                    return arrMessage;
                }
                else
                {
                    if (RecAfct > 0 & strUploadMessage == "SUCCESS")
                    {
                        oraTran.Commit();
                        arrMessage.Add("All Data Saved Successfully");
                    }
                    else
                    {
                        arrMessage.Add(strUploadMessage);
                    }
                }
                return arrMessage;
            }
            catch (Exception ex)
            {
                oraTran.Rollback();
                throw ex;
            }
            finally
            {
                objWK.MyConnection.Close();
            }
        }
        public DataSet FetchUploadCustomerProcedure(int customerPK = 0)
        {
            try
            {
                WorkFlow objWF = new WorkFlow();
                DataTable dt = null;
                DataSet ds = new DataSet();

                objWF.MyCommand.Parameters.Clear();
                var _with6 = objWF.MyCommand.Parameters;
                _with6.Add("CUSTOMER_MST_FK_IN", customerPK).Direction = ParameterDirection.Input;
                _with6.Add("CUST_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dt = objWF.GetDataTable("CUSTOMER_MST_TBL_PKG", "FETCH_CUSTOMER_UPLOAD");

                ds.Tables.Add(dt);
                return ds;
                //Manjunath  PTS ID:Sep-02  17/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
        }
        #endregion
    }
}