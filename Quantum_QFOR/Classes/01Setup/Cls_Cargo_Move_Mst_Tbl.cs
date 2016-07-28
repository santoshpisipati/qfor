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
    public class Cls_Cargo_Move_Mst_Tbl : CommonFeatures
    {

        #region "List of Members of the Class"
        private Int64 M_CARGO_MOVE_Pk;
        private string M_CARGO_MOVE_CODE;
        private string M_CARGO_MOVE_DESC;
        private string M_Created_Date;
        #endregion
        private DataSet M_DataSet;

        #region "List of Properties"
        public DataSet MyDataSet
        {
            get { return M_DataSet; }
            set { M_DataSet = value; }
        }

        public Int64 CARGO_MOVE_PK
        {
            get { return M_CARGO_MOVE_Pk; }
            set { M_CARGO_MOVE_Pk = value; }
        }

        public string CARGO_MOVE_CODE
        {
            get { return M_CARGO_MOVE_CODE; }
            set { M_CARGO_MOVE_CODE = value; }
        }

        public string CARGO_MOVE_DESC
        {
            get { return M_CARGO_MOVE_DESC; }
            set { M_CARGO_MOVE_DESC = value; }
        }
        public string Created_Date
        {
            get { return M_Created_Date; }
            set { M_Created_Date = value; }
        }
        #endregion
       
        #region "Constructor"
        public Cls_Cargo_Move_Mst_Tbl()
        {
            WorkFlow objWF = new WorkFlow();
            string Sql = null;
            Sql = "SELECT 0 CARGO_MOVE_PK,' Select' CARGO_MOVE_CODE,'' CARGO_MOVE_DESC,0 VERSION_NO";
            Sql += "FROM CARGO_MOVE_MST_TBL CM";
            Sql += "UNION";
            Sql += "SELECT CM.CARGO_MOVE_PK,CM.CARGO_MOVE_CODE,";
            Sql += "CM.CARGO_MOVE_DESC,CM.VERSION_NO";
            Sql += "FROM CARGO_MOVE_MST_TBL CM";
            Sql += "WHERE CM.ACTIVE_FLAG=1";
            Sql += "ORDER BY CARGO_MOVE_CODE";
            try
            {
                M_DataSet = objWF.GetDataSet(Sql);
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

        #region " Fetch All Function "
        public DataSet FetchAll(string CargoMoveCode = "", string CargoMoveDesc = "", string SearchType = "", string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, bool ActiveFlag = true, string SortType = " ASC ", Int32 flag = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            if (flag == 0)
            {
                strCondition += " AND 1=2";
            }
            if (CargoMoveCode.Trim().Length > 0)
            {
                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition += " AND UPPER(CARGO_MOVE_CODE) LIKE '" + CargoMoveCode.ToUpper().Replace("'", "''") + "%'" ;
                    }
                    else
                    {
                        strCondition += " AND UPPER(CARGO_MOVE_CODE) LIKE '%" + CargoMoveCode.ToUpper().Replace("'", "''") + "%'" ;
                    }
                }
                else
                {
                    strCondition += " AND UPPER(CARGO_MOVE_CODE) LIKE '%" + CargoMoveCode.ToUpper().Replace("'", "''") + "%'" ;
                }
            }
            if (CargoMoveDesc.Trim().Length > 0)
            {
                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition += " AND UPPER(CARGO_MOVE_DESC) LIKE '" + CargoMoveDesc.ToUpper().Replace("'", "''") + "%'" ;
                    }
                    else
                    {
                        strCondition += " AND UPPER(CARGO_MOVE_DESC) LIKE '%" + CargoMoveDesc.ToUpper().Replace("'", "''") + "%'" ;
                    }
                }
                else
                {
                    strCondition += " AND UPPER(CARGO_MOVE_DESC) LIKE '%" + CargoMoveDesc.ToUpper().Replace("'", "''") + "%'" ;
                }
            }
            if (ActiveFlag == true)
            {
                strCondition += " AND ACTIVE_FLAG = 1 ";
            }
            else
            {
                strCondition += " ";
            }
            strSQL = "SELECT Count(*) from CARGO_MOVE_MST_TBL where 1=1 ";
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
            strSQL = " select * from (";
            strSQL += "SELECT ROWNUM SR_NO,q.* FROM ";
            strSQL += "(SELECT  ";
            strSQL += "CARGO_MOVE_PK, ";
            strSQL += "NVL(ACTIVE_FLAG,0) ACTIVE_FLAG , ";
            strSQL += "CARGO_MOVE_CODE, ";
            strSQL += "CARGO_MOVE_DESC, ";
            strSQL += " (SELECT Q.DD_ID";
            strSQL += " FROM QFOR_DROP_DOWN_TBL Q";
            strSQL += " WHERE Q.CONFIG_ID = 'QFLX2008'";
            strSQL += " AND Q.DD_VALUE = WIN_CARGO_MOVE_FK) CARGO_MOVEMENT, ";
            strSQL += "VERSION_NO  ";
            strSQL += "FROM CARGO_MOVE_MST_TBL ";
            strSQL += "WHERE 1=1 ";
            strSQL += strCondition;
            strSQL += " order by " + SortColumn + SortType + " ) q  ) ";
            strSQL += " WHERE SR_NO  Between " + start + " and " + last;
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
        #endregion

        #region "Save Function"
        public ArrayList Save(DataSet M_DataSet, bool Import = false)
        {
            //Sivachandran 19Jun08 For Imp-Exp-wiz
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
                string INS_Proc = null;
                string DEL_Proc = null;
                string UPD_Proc = null;
                string UserName = objWK.MyUserName;
                INS_Proc = UserName + ".CARGO_MOVE_MST_TBL_PKG.CARGO_MOVE_MST_TBL_INS";
                DEL_Proc = UserName + ".CARGO_MOVE_MST_TBL_PKG.CARGO_MOVE_MST_TBL_DEL";
                UPD_Proc = UserName + ".CARGO_MOVE_MST_TBL_PKG.CARGO_MOVE_MST_TBL_UPD";
                var _with1 = insCommand;
                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = INS_Proc;
                _with1.Parameters.Add("CARGO_MOVE_CODE_IN", OracleDbType.Varchar2, 10, "CARGO_MOVE_CODE").Direction = ParameterDirection.Input;
                _with1.Parameters["CARGO_MOVE_CODE_IN"].SourceVersion = DataRowVersion.Current;
                _with1.Parameters.Add("CARGO_MOVE_DESC_IN", OracleDbType.Varchar2, 50, "CARGO_MOVE_DESC").Direction = ParameterDirection.Input;
                _with1.Parameters["CARGO_MOVE_DESC_IN"].SourceVersion = DataRowVersion.Current;
                _with1.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
                _with1.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;
                _with1.Parameters.Add("WIN_CARGO_MOVE_IN", OracleDbType.Int32, 1, "CARGO_MOVEMENT").Direction = ParameterDirection.Input;
                _with1.Parameters["WIN_CARGO_MOVE_IN"].SourceVersion = DataRowVersion.Current;
                _with1.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "Place_PK").Direction = ParameterDirection.Output;
                _with1.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                var _with2 = delCommand;
                _with2.Connection = objWK.MyConnection;
                _with2.CommandType = CommandType.StoredProcedure;
                _with2.CommandText = DEL_Proc;
                _with2.Parameters.Add("CARGO_MOVE_PK_IN", OracleDbType.Int32, 10, "CARGO_MOVE_PK").Direction = ParameterDirection.Input;
                _with2.Parameters["CARGO_MOVE_PK_IN"].SourceVersion = DataRowVersion.Current;

                _with2.Parameters.Add("DELETED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

                _with2.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                _with2.Parameters["CONFIG_PK_IN"].SourceVersion = DataRowVersion.Current;
                _with2.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                _with2.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                _with2.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with2.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                var _with3 = updCommand;
                _with3.Connection = objWK.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = UPD_Proc;

                _with3.Parameters.Add("CARGO_MOVE_PK_IN", OracleDbType.Int32, 10, "CARGO_MOVE_PK").Direction = ParameterDirection.Input;
                _with3.Parameters["CARGO_MOVE_PK_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("CARGO_MOVE_CODE_IN", OracleDbType.Varchar2, 10, "CARGO_MOVE_CODE").Direction = ParameterDirection.Input;
                _with3.Parameters["CARGO_MOVE_CODE_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("CARGO_MOVE_DESC_IN", OracleDbType.Varchar2, 50, "CARGO_MOVE_DESC").Direction = ParameterDirection.Input;
                _with3.Parameters["CARGO_MOVE_DESC_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
                _with3.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("WIN_CARGO_MOVE_IN", OracleDbType.Int32, 1, "CARGO_MOVEMENT").Direction = ParameterDirection.Input;
                _with3.Parameters["WIN_CARGO_MOVE_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

                _with3.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                _with3.Parameters["CONFIG_PK_IN"].SourceVersion = DataRowVersion.Current;
                _with3.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                _with3.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with3.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                var _with4 = objWK.MyDataAdapter;
                _with4.InsertCommand = insCommand;
                _with4.InsertCommand.Transaction = TRAN;
                _with4.UpdateCommand = updCommand;
                _with4.UpdateCommand.Transaction = TRAN;
                _with4.DeleteCommand = delCommand;
                _with4.DeleteCommand.Transaction = TRAN;
                RecAfct = _with4.Update(M_DataSet);
                TRAN.Commit();
                if (arrMessage.Count > 0)
                {
                    return arrMessage;
                }
                else if (Import == false)
                {
                    arrMessage.Add("All Data Saved Successfully");
                }
                else
                {
                    arrMessage.Add("Data Imported Successfully");
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
            return new ArrayList();
        }
        #endregion

    }
}