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
    public class cls_AreaMaster : CommonFeatures
    {
        #region "List of Members of Class"
        private Int16 M_AREA_MST_PK;
        private string M_AREA_MST_ID;
        private string M_AREA_MAST_NAME;
        #endregion
        private static DataSet M_DataSet = new DataSet();

        #region "Fetch Records"
        public DataSet FetchAll(Int16 AreaPK = 0, string AreaID = "", string AreaName = "", Int16 RegionPK = 0, string SearchType = "", string strColumnName = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int16 SortCol = 2, Int16 ActiveFlag = -1,
        bool blnSortAscending = false, Int32 bListOnLoad = 0)
        {

            WorkFlow objWF = new WorkFlow();

            OracleDataAdapter daObj = null;
            DataSet dsObj = new DataSet();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strCondition = null;

            sb.Append("  SELECT   ");
            sb.Append("  ROWNUM SLNO, Q.* FROM(SELECT  ");
            sb.Append("  AR.ACTIVE_FLAG,");
            sb.Append("  AR.AREA_MST_PK,");
            sb.Append("  UPPER(AR.AREA_ID) AREA_ID,");
            sb.Append("  UPPER(AR.AREA_NAME) AREA_NAME,");
            sb.Append("  AR.REGION_MST_FK,");
            sb.Append("  RG.REGION_CODE,");
            sb.Append("  RG.REGION_NAME,");
            sb.Append("  AR.VERSION_NO,");
            sb.Append("  '0' DELFLAG,");
            sb.Append("  '0' CHEFLAG");
            sb.Append("  FROM ");
            sb.Append("  AREA_MST_TBL AR, REGION_MST_TBL RG");
            sb.Append("  WHERE ");
            sb.Append("  AR.REGION_MST_FK = RG.REGION_MST_PK");

            if (bListOnLoad == 0)
            {
                sb.Append(" AND 1=2 ");
            }
            if (AreaID.Trim().Length > 0)
            {
                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition +=" AND UPPER(AR.AREA_ID) LIKE '" + AreaID.ToUpper().Replace("'", "''") + "%'" ;
                    }
                    else
                    {
                        strCondition +=" AND UPPER(AR.AREA_ID) LIKE '%" + AreaID.ToUpper().Replace("'", "''") + "%'" ;
                    }
                }
                else
                {
                    strCondition +=" AND UPPER(AR.AREA_ID) LIKE '%" + AreaID.ToUpper().Replace("'", "''") + "%'" ;
                }
            }

            if (AreaName.Trim().Length > 0)
            {
                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition +=" AND UPPER(AR.AREA_NAME) LIKE '" + AreaName.ToUpper().Replace("'", "''") + "%'" ;
                    }
                    else
                    {
                        strCondition +=" AND UPPER(AR.AREA_NAME) LIKE '%" + AreaName.ToUpper().Replace("'", "''") + "%'" ;
                    }
                }
                else
                {
                    strCondition +=" AND UPPER(AR.AREA_NAME) LIKE '%" + AreaName.ToUpper().Replace("'", "''") + "%'" ;
                }
            }

            if (AreaPK > 0)
            {
                strCondition +=" AND  AR.AREA_MST_PK = " + AreaPK;
            }
            if (RegionPK > 0)
            {
                strCondition +=" AND AR.REGION_MST_FK = " + RegionPK ;
            }

            if (ActiveFlag == 1)
            {
                strCondition +=" AND AR.ACTIVE_FLAG = 1 ";
            }
            if (!blnSortAscending & !strColumnName.Equals("SLNO"))
            {
                strCondition +=" ORDER BY AR.AREA_NAME DESC";
            }
            else
            {
                strCondition +=" ORDER BY AR.AREA_NAME ASC";
            }

            sb.Append(strCondition);
            sb.Append(" ) Q ");

            try
            {
                //Get the Total Pages
                Int32 last = default(Int32);
                Int32 start = default(Int32);
                Int32 TotalRecords = default(Int32);
                string StrSqlCount = null;

                StrSqlCount = "SELECT COUNT(*) FROM ( ";
                StrSqlCount = StrSqlCount + sb.ToString();
                StrSqlCount = StrSqlCount + " ) ";

                TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(StrSqlCount.ToString()));
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

                string StrSqlRecords = null;
                StrSqlRecords = "SELECT * FROM ( ";
                StrSqlRecords = StrSqlRecords + sb.ToString();
                StrSqlRecords = StrSqlRecords + " ) WHERE SLNO BETWEEN " + start + " AND " + last;

                return objWF.GetDataSet(StrSqlRecords.ToString());
            }
            catch (OracleException oraExp)
            {
                ErrorMessage = oraExp.Message;
                throw oraExp;

            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
        }

        #endregion

        #region "Save Function"
        public ArrayList save(DataSet M_DataSet)
        {
            //Sivachandran 17jun08 Imp-Exp-Wiz
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            
            int intPKVal = 0;
            long lngI = 0;
            Int32 RecAfct = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            OracleCommand delCommand = new OracleCommand();


            try
            {
                DataTable DtTbl = new DataTable();
                DataRow DtRw = null;
                int i = 0;
                DtTbl = M_DataSet.Tables[0];
                var _with1 = insCommand;
                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".AREA_MST_TBL_PKG.AREA_MST_TBL_INS";
                var _with2 = _with1.Parameters;

                insCommand.Parameters.Add("AREA_ID_IN", OracleDbType.Varchar2, 20, "AREA_ID").Direction = ParameterDirection.Input;
                insCommand.Parameters["AREA_ID_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("AREA_NAME_IN", OracleDbType.Varchar2, 50, "AREA_NAME").Direction = ParameterDirection.Input;
                insCommand.Parameters["AREA_NAME_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("REGION_MST_FK_IN", OracleDbType.Int32, 10, "REGION_MST_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["REGION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
                insCommand.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CREATED_BY_FK_IN", CREATED_BY).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "AREA_MST_PK").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                var _with3 = delCommand;
                // Working Fine
                _with3.Connection = objWK.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".AREA_MST_TBL_PKG.AREA_MST_TBL_DEL";
                var _with4 = _with3.Parameters;
                delCommand.Parameters.Add("AREA_MST_PK_IN", OracleDbType.Int32, 10, "AREA_MST_PK").Direction = ParameterDirection.Input;
                delCommand.Parameters["AREA_MST_PK_IN"].SourceVersion = DataRowVersion.Current;
                delCommand.Parameters.Add("DELETED_BY_FK_IN", LAST_MODIFIED_BY).Direction = ParameterDirection.Input;
                delCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                delCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
                delCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                delCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                delCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                var _with5 = updCommand;
                _with5.Connection = objWK.MyConnection;
                _with5.CommandType = CommandType.StoredProcedure;
                _with5.CommandText = objWK.MyUserName + ".AREA_MST_TBL_PKG.AREA_MST_TBL_UPD";
                var _with6 = _with5.Parameters;
                updCommand.Parameters.Add("AREA_MST_PK_IN", OracleDbType.Int32, 10, "AREA_MST_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["AREA_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("AREA_ID_IN", OracleDbType.Varchar2, 20, "AREA_ID").Direction = ParameterDirection.Input;
                updCommand.Parameters["AREA_ID_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("AREA_NAME_IN", OracleDbType.Varchar2, 20, "AREA_NAME").Direction = ParameterDirection.Input;
                updCommand.Parameters["AREA_NAME_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("REGION_MST_FK_IN", OracleDbType.Int32, 10, "REGION_MST_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["REGION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
                updCommand.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", LAST_MODIFIED_BY).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                
                var _with7 = objWK.MyDataAdapter;
                _with7.InsertCommand = insCommand;
                _with7.InsertCommand.Transaction = TRAN;
                _with7.UpdateCommand = updCommand;
                _with7.UpdateCommand.Transaction = TRAN;
                _with7.DeleteCommand = delCommand;
                _with7.DeleteCommand.Transaction = TRAN;
                RecAfct = _with7.Update(M_DataSet);
                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    TRAN.Commit();
                    arrMessage.Add("All Data Saved Successfully");
                    return arrMessage;
                }

            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWK.CloseConnection();
            }
        }
        #endregion

    }
}