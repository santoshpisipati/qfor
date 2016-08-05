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

using Newtonsoft.Json;
using Oracle.ManagedDataAccess.Client;
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
    public class cls_AuditTrailsSetup : CommonFeatures
    {
        /// <summary>
        /// The m_ data set
        /// </summary>
        private static DataSet M_DataSet = new DataSet();

        #region "List Of Properties"

        /// <summary>
        /// Gets my dataset.
        /// </summary>
        /// <value>
        /// My dataset.
        /// </value>
        public static DataSet MyDataset
        {
            get { return M_DataSet; }
        }

        #endregion "List Of Properties"

        #region "Audit Trials Grid"

        /// <summary>
        /// FN_s the audit trials_ grid.
        /// </summary>
        /// <param name="MENU_MST_PK">The men u_ ms t_ pk.</param>
        /// <returns></returns>
        public string Fn_AuditTrials_Grid(long MENU_MST_PK = 0, Int32 bizType = 1)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder(5000);
            try
            {
                strBuilder.Append(" SELECT ROWNUM SLNR,");
                strBuilder.Append(" QRY.* FROM (");
                strBuilder.Append(" SELECT V.ACTIVE_FLAG,");
                strBuilder.Append(" V.MENU_MST_PK,");
                strBuilder.Append(" V.MENU_TEXT,");
                strBuilder.Append(" V.INS_FLAG,");
                strBuilder.Append(" V.UPD_FLAG,");
                strBuilder.Append(" V.DEL_FLAG,");
                strBuilder.Append(" V.ALL_FLAG,");
                strBuilder.Append(" '' BTN,");
                strBuilder.Append(" V.AUDIT_SETUP_PK");
                strBuilder.Append(" FROM VIEW_QCOR_AUDIT_SETUP V");
                strBuilder.Append(" WHERE V.MODULE_FK=" + MENU_MST_PK);
                if (bizType == 1)
                {
                    strBuilder.Append("   AND V.BIZ_TYPE IN (1,3)");
                }
                else if (bizType == 2)
                {
                    strBuilder.Append("  AND V.BIZ_TYPE IN (2,3)");
                }

                strBuilder.Append(" ORDER BY V.display_order ");
                strBuilder.Append(" ) QRY ");
                return JsonConvert.SerializeObject(objWF.GetDataTable(strBuilder.ToString()), Formatting.Indented);
                //Manjunath  PTS ID:Sep-02   12/09/2011
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
                strBuilder = null;
                objWF = null;
            }
        }

        #endregion "Audit Trials Grid"

        #region "Constructor"

        //ToFill Module Name
        /// <summary>
        /// Initializes a new instance of the <see cref="cls_AuditTrailsSetup"/> class.
        /// </summary>
        public cls_AuditTrailsSetup()
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder(5000);

            strBuilder.Append("SELECT VMF.MENU_MST_PK,VMF.MENU_TEXT ");
            strBuilder.Append("FROM VIEW_MODULE_FORMS VMF ");
            strBuilder.Append("WHERE VMF.MENU_LEVEL=1 ");
            try
            {
                M_DataSet = objWF.GetDataSet(strBuilder.ToString());
                //Manjunath  PTS ID:Sep-02   12/09/2011
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
                objWF = null;
                strBuilder = null;
            }
        }

        #endregion "Constructor"

        #region "Table Field Name"

        /// <summary>
        /// Fetches the table fields.
        /// </summary>
        /// <param name="FormPk">The form pk.</param>
        /// <returns></returns>
        public DataSet FetchTableFields(Int64 FormPk)
        {
            System.Text.StringBuilder StrBuilder = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                StrBuilder.Append(" SELECT ROWNUM SLNO, ");
                StrBuilder.Append(" Q.AUDIT_SETUP_PK,");
                StrBuilder.Append(" Q.AUDIT_TABLE_PK,");
                StrBuilder.Append(" Q.TABLE_NAME,");
                StrBuilder.Append(" Q.TABLE_DESC,");
                StrBuilder.Append(" Q.AUDIT_FIELD_PK,");
                StrBuilder.Append(" Q.FIELD_NAME,");
                StrBuilder.Append(" Q.FIELD_DESC,");
                StrBuilder.Append(" Q.ACTIVE_FLAG ");
                StrBuilder.Append(" FROM (");
                StrBuilder.Append(" SELECT SAC.AUDIT_SETUP_PK,");
                StrBuilder.Append(" SAT.AUDIT_TABLE_PK, ");
                StrBuilder.Append(" SAT.TABLE_NAME, ");
                StrBuilder.Append(" SAT.TABLE_DESC,");
                StrBuilder.Append(" null AUDIT_FIELD_PK,");
                StrBuilder.Append(" '' FIELD_NAME,");
                StrBuilder.Append(" '' FIELD_DESC,");
                StrBuilder.Append(" SAT.ACTIVE_FLAG ");
                StrBuilder.Append(" FROM QCOR_SEC_AUDIT_CONFIG SAC, QCOR_SEC_AUDIT_TABLES SAT");
                StrBuilder.Append(" WHERE SAC.MENU_CONFIG_FK =" + FormPk);
                StrBuilder.Append(" AND SAC.AUDIT_SETUP_PK = SAT.AUDIT_SETUP_FK");
                StrBuilder.Append(" UNION ");
                StrBuilder.Append(" SELECT SAC.AUDIT_SETUP_PK,");
                StrBuilder.Append(" SATF.AUDIT_TABLE_FK AS AUDIT_TABLE_PK,");
                StrBuilder.Append(" '' TABLE_NAME,");
                StrBuilder.Append(" ''TABLE_DESC,");
                StrBuilder.Append(" SATF.AUDIT_FIELD_PK,");
                StrBuilder.Append(" SATF.FIELD_NAME,");
                StrBuilder.Append(" SATF.FIELD_DESC,");
                StrBuilder.Append(" SATF.ACTIVE_FLAG");
                StrBuilder.Append(" FROM QCOR_SEC_AUDIT_CONFIG        SAC,");
                StrBuilder.Append(" QCOR_SEC_AUDIT_TABLES        SAT,");
                StrBuilder.Append(" QCOR_SEC_AUDIT_TABLES_FIELDS SATF");
                StrBuilder.Append(" WHERE SAC.MENU_CONFIG_FK =" + FormPk);
                StrBuilder.Append(" AND SAC.AUDIT_SETUP_PK = SAT.AUDIT_SETUP_FK ");
                StrBuilder.Append(" AND SAT.AUDIT_TABLE_PK = SATF.AUDIT_TABLE_FK ");
                StrBuilder.Append(" ) Q");
                return objWF.GetDataSet(StrBuilder.ToString());
                //Manjunath  PTS ID:Sep-02   12/09/2011
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
                StrBuilder = null;
                objWF = null;
            }
        }

        /// <summary>
        /// Dts the fetch table fields.
        /// </summary>
        /// <param name="FormPk">The form pk.</param>
        /// <returns></returns>
        public DataTable dtFetchTableFields(Int64 FormPk)
        {
            System.Text.StringBuilder StrBuilder = new System.Text.StringBuilder();
            //Dim objWF As New QFORBusinessDev.Business.WorkFlow
            WorkFlow objWF = new WorkFlow();
            DataTable dt = new DataTable();
            try
            {
                StrBuilder.Append(" SELECT ROWNUM SLNO, ");
                StrBuilder.Append(" Q.AUDIT_SETUP_PK,");
                StrBuilder.Append(" Q.AUDIT_TABLE_PK,");
                StrBuilder.Append(" Q.TABLE_NAME,");
                StrBuilder.Append(" Q.TABLE_DESC,");
                StrBuilder.Append(" Q.AUDIT_FIELD_PK,");
                StrBuilder.Append(" Q.FIELD_NAME,");
                StrBuilder.Append(" Q.FIELD_DESC,");
                StrBuilder.Append(" Q.ACTIVE_FLAG ");
                StrBuilder.Append(" FROM (");
                StrBuilder.Append(" SELECT SAC.AUDIT_SETUP_PK,");
                StrBuilder.Append(" SAT.AUDIT_TABLE_PK, ");
                StrBuilder.Append(" SAT.TABLE_NAME, ");
                StrBuilder.Append(" SAT.TABLE_DESC,");
                StrBuilder.Append(" null AUDIT_FIELD_PK,");
                StrBuilder.Append(" '' FIELD_NAME,");
                StrBuilder.Append(" '' FIELD_DESC,");
                StrBuilder.Append(" SAT.ACTIVE_FLAG ");
                StrBuilder.Append(" FROM QCOR_SEC_AUDIT_CONFIG SAC, QCOR_SEC_AUDIT_TABLES SAT");
                StrBuilder.Append(" WHERE SAC.MENU_CONFIG_FK =" + FormPk);
                StrBuilder.Append(" AND SAC.AUDIT_SETUP_PK = SAT.AUDIT_SETUP_FK");
                StrBuilder.Append(" AND SAT.ACTIVE_FLAG=1 ");
                StrBuilder.Append(" UNION ");
                StrBuilder.Append(" SELECT SAC.AUDIT_SETUP_PK,");
                StrBuilder.Append(" SATF.AUDIT_TABLE_FK AS AUDIT_TABLE_PK,");
                StrBuilder.Append(" '' TABLE_NAME,");
                StrBuilder.Append(" ''TABLE_DESC,");
                StrBuilder.Append(" SATF.AUDIT_FIELD_PK,");
                StrBuilder.Append(" SATF.FIELD_NAME,");
                StrBuilder.Append(" SATF.FIELD_DESC,");
                StrBuilder.Append(" SATF.ACTIVE_FLAG");
                StrBuilder.Append(" FROM QCOR_SEC_AUDIT_CONFIG        SAC,");
                StrBuilder.Append(" QCOR_SEC_AUDIT_TABLES        SAT,");
                StrBuilder.Append(" QCOR_SEC_AUDIT_TABLES_FIELDS SATF");
                StrBuilder.Append(" WHERE SAC.MENU_CONFIG_FK =" + FormPk);
                StrBuilder.Append(" AND SAC.AUDIT_SETUP_PK = SAT.AUDIT_SETUP_FK ");
                StrBuilder.Append(" AND SAT.AUDIT_TABLE_PK = SATF.AUDIT_TABLE_FK ");
                StrBuilder.Append(" AND SAT.ACTIVE_FLAG=1 ");
                StrBuilder.Append(" AND SATF.ACTIVE_FLAG_FOR_SETUP=1 ");
                StrBuilder.Append(" ) Q");
                dt = objWF.GetDataTable(StrBuilder.ToString());
                return dt;
                //Manjunath  PTS ID:Sep-02   12/09/2011
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
                StrBuilder = null;
                objWF = null;
            }
        }

        /// <summary>
        /// Fetches the table fields1.
        /// </summary>
        /// <param name="FormPk">The form pk.</param>
        /// <returns></returns>
        public DataSet FetchTableFields1(Int64 FormPk)
        {
            System.Text.StringBuilder StrBuilder = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                StrBuilder.Append(" SELECT ROWNUM SLNO, ");
                StrBuilder.Append(" SAC.AUDIT_SETUP_PK, ");
                StrBuilder.Append(" SAT.AUDIT_TABLE_PK, ");
                StrBuilder.Append(" SAT.TABLE_NAME, ");
                StrBuilder.Append(" SATF.AUDIT_FIELD_PK,");
                StrBuilder.Append(" SATF.FIELD_NAME,");
                StrBuilder.Append(" SATF.FIELD_DESC,");
                StrBuilder.Append(" SATF.ACTIVE_FLAG ");
                StrBuilder.Append(" FROM QCOR_SEC_AUDIT_CONFIG        SAC,  ");
                StrBuilder.Append(" QCOR_SEC_AUDIT_TABLES        SAT,");
                StrBuilder.Append(" QCOR_SEC_AUDIT_TABLES_FIELDS SATF");
                StrBuilder.Append(" WHERE SAC.MENU_CONFIG_FK =" + FormPk);
                StrBuilder.Append(" AND SAC.AUDIT_SETUP_PK = SAT.AUDIT_SETUP_FK ");
                StrBuilder.Append(" AND SAT.AUDIT_TABLE_PK = SATF.AUDIT_TABLE_FK ");

                return objWF.GetDataSet(StrBuilder.ToString());
                //Manjunath  PTS ID:Sep-02   12/09/2011
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
                StrBuilder = null;
                objWF = null;
            }
        }

        #endregion "Table Field Name"

        #region "Calling Trigger Execution"

        /// <summary>
        /// FN_s the call_ trigger execution.
        /// </summary>
        /// <returns></returns>
        public ArrayList fn_Call_TriggerExecution()
        {
            OracleTransaction TRAN = default(OracleTransaction);
            WorkFlow objWK = new WorkFlow();
            OracleCommand updCommand = new OracleCommand();
            try
            {
                objWK.OpenConnection();
                TRAN = objWK.MyConnection.BeginTransaction();
                var _with7 = updCommand;
                _with7.Connection = objWK.MyConnection;
                _with7.CommandType = CommandType.StoredProcedure;
                _with7.CommandText = objWK.MyUserName + ".qcor_sec_audit_trail_pkg.sp_audit_trail";
                _with7.Parameters.Clear();
                var _with8 = _with7.Parameters;
                _with8.Add("FLAG", 1);
                var _with9 = objWK.MyDataAdapter;
                _with9.UpdateCommand = updCommand;
                _with9.UpdateCommand.Transaction = TRAN;
                _with9.UpdateCommand.ExecuteNonQuery();
                arrMessage.Add("All Data Saved Successfully");
                TRAN.Commit();
                return arrMessage;
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
                objWK.MyCommand.Connection.Close();
                objWK = null;
                TRAN = null;
            }
            //Catch ex As Exception
            //    Throw ex
            //Finally
            //    objWK = Nothing
            //    TRAN = Nothing
            //End Try
        }

        /// <summary>
        /// FN_s the call_ trigger execution by module.
        /// </summary>
        /// <param name="MODULE_FK">The modul e_ fk.</param>
        /// <returns></returns>
        public ArrayList fn_Call_TriggerExecutionByModule(int MODULE_FK)
        {
            OracleTransaction TRAN = default(OracleTransaction);
            WorkFlow objWK = new WorkFlow();
            OracleCommand updCommand = new OracleCommand();
            try
            {
                objWK.OpenConnection();
                TRAN = objWK.MyConnection.BeginTransaction();
                var _with10 = updCommand;
                _with10.Connection = objWK.MyConnection;
                _with10.CommandType = CommandType.StoredProcedure;
                _with10.CommandText = objWK.MyUserName + ".qcor_sec_audit_trail_pkg.SP_AUDIT_TRAIL_FOR_MODULE";
                _with10.Parameters.Clear();
                var _with11 = _with10.Parameters;
                _with11.Add("FLAG", 1);
                _with11.Add("MODULE_FK_IN", MODULE_FK);
                var _with12 = objWK.MyDataAdapter;
                _with12.UpdateCommand = updCommand;
                _with12.UpdateCommand.Transaction = TRAN;
                _with12.UpdateCommand.ExecuteNonQuery();
                arrMessage.Add("All Data Saved Successfully");
                TRAN.Commit();
                return arrMessage;
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
                objWK.MyCommand.Connection.Close();
                objWK = null;
                TRAN = null;
            }
        }

        #endregion "Calling Trigger Execution"
    }
}