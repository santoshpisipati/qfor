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
using System;
using System.Data;
using System.Text;

namespace Quantum_QFOR
{
    public class cls_Util_ExportWizard_Transaction
    {
        #region "Enhance Search + Lookup Search Block"
        public object Fetch_Export_AR(string strCond)
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
            strReq = Convert.ToString(arr.GetValue(0));
            strSEARCH_IN = Convert.ToString(arr.GetValue(1));

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_UTIL_EXPORT_WIZARD_PKG.SP_GET_EXPORT_AR_FORMS";

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
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
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





        public Int32 fn_Get_BusinessType(Int32 userpk)
        {
            StringBuilder strsql = new StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                strsql.Append("select users.business_type from user_mst_tbl users where users.user_mst_pk=" + userpk);
                return Convert.ToInt32(objWF.ExecuteScaler(strsql.ToString()));
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public DataSet fn_Get_Export_Tablesnames(Int64 MasterPK, Int32 TablePK)
        {

            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();
            StringBuilder sb = new StringBuilder(5000);

            sb.Append("  select distinct(mp.tran_table_name || '  ' || mp.tran_table_alias_name) ");
            sb.Append("          from QCOR_GEN_TRANS_FIELD_MAP mp ");
            sb.Append("        where mp.tran_fk = " + MasterPK + " ");
            sb.Append("        and mp.tran_table_map_fk = " + TablePK + " ");

            try
            {
                ds = objWF.GetDataSet(sb.ToString());
                return ds;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public DataSet fn_Get_Export_Columnnames(Int64 MasterPK, Int32 TablePK)
        {

            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();
            StringBuilder sb = new StringBuilder(5000);

            sb.Append("  select   mp.Tran_Field_Description, MP.TRAN_TABLE_ALIAS_NAME || '.' || MP.TRAN_FIELD_NAME ");
            sb.Append("           from QCOR_GEN_TRANS_FIELD_MAP mp  ");
            sb.Append("           where mp.Tran_Fk =  " + MasterPK + " ");
            sb.Append("            and mp.tran_table_map_fk= " + TablePK + " ");
            sb.Append("       and mp.related_field_name  is null AND MP.ACTIVE_FLAG = 1 ");

            try
            {
                ds = objWF.GetDataSet(sb.ToString());
                return ds;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public DataSet fn_Get_Export_ColumnRelations(Int64 MasterPK, Int32 TablePK)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();
            StringBuilder sb = new StringBuilder(5000);
            sb.Append("  select mp.tran_table_name || ' ' || mp.tran_table_alias_name, ");
            sb.Append("   mp.Related_Table_Name || ' ' || mp.Related_Table_Alias_Name,");
            sb.Append("   mp.Tran_Table_Alias_Name || '.' || mp.Tran_Field_Name || '=' ||");
            sb.Append("   mp.Related_Table_Alias_Name || '.' || mp.Related_Field_Name");
            sb.Append("   from QCOR_GEN_TRANS_FIELD_MAP mp");
            sb.Append("   where mp.tran_fk = " + MasterPK + "  ");
            sb.Append("   and mp.tran_table_map_fk = " + TablePK + "  ");
            sb.Append("   and mp.related_field_name is not null ");

            try
            {
                ds = objWF.GetDataSet(sb.ToString());
                return ds;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public DataSet fn_Get_Export_TableLevels(Int64 MasterPK)
        {

            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();
            StringBuilder sb = new StringBuilder(5000);

            sb.Append("  SELECT  T.TRAN_TABLE_MAP_PK TABLEPK, T.PARENT_FK, T.RELATION_FIELD, T.PARENT_RELATION_FIELD FROM QCOR_GEN_TRANS_TABLE_MAP T  ");
            sb.Append("      WHERE T.TRAN_FK=  " + MasterPK + " ");
            sb.Append("      AND T.ACTIVE_FLAG=1");

            try
            {
                ds = objWF.GetDataSet(sb.ToString());
                return ds;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public DataTable fn_Get_Export_Data(string sbQry)
        {

            WorkFlow objWF = new WorkFlow();
            DataTable dt = new DataTable();
            try
            {
                dt = objWF.GetDataTable(sbQry.ToString());
                return dt;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public DataSet fn_Get_Export_qry(Int64 MasterPK)
        {

            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();
            StringBuilder sb = new StringBuilder(5000);
            sb.Append("select * from qcor_gen_master_tbl_map_fields mp where mp.master_fk " + MasterPK + "  ");

            try
            {
                ds = objWF.GetDataSet(sb.ToString());
                return ds;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public string fn_Get_Loc_Reporting_Locs(Int64 loc_fk)
        {

            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();
            StringBuilder sb = new StringBuilder(5000);

            sb.Append("  select fn_get_loc_reporting_locs(" + loc_fk + " ) from dual  ");

            try
            {
                if (Convert.ToString(objWF.ExecuteScaler(sb.ToString())).Trim().Length > 0)
                {
                    return Convert.ToString(objWF.ExecuteScaler(sb.ToString()));
                }
                else
                {
                    return "";
                }
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
    }
}