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

//Option Strict On

using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections;
using System.Data;
using System.Web;

namespace Quantum_QFOR
{
    public class clsUSER_ENTRY_TRN : CommonFeatures
	{


        #region "Fetch Function"
        public DataSet FetchAll(Int64 P_User_Access_Pk, Int64 P_User_Mst_Fk, Int64 P_Location_Mst_Fk, Int64 P_Config_Mst_Fk, Int64 P_Allowed_Operations_Value, string SearchType = "", string SortExpression = "")
        {
            string strSQL = null;
            strSQL = "SELECT ROWNUM SR_NO, ";
            strSQL = strSQL + " User_Access_Pk,";
            strSQL = strSQL + " User_Mst_Fk,";
            strSQL = strSQL + " Location_Mst_Fk,";
            strSQL = strSQL + " Config_Mst_Fk,";
            strSQL = strSQL + " Allowed_Operations_Value,";
            strSQL = strSQL + " Version_No ";
            strSQL = strSQL + " FROM USER_ACCESS_TRN ";
            strSQL = strSQL + " WHERE ( 1 = 1) ";
            if (P_User_Access_Pk > 0)
            {
                strSQL = strSQL + " And User_Access_Pk = " + P_User_Access_Pk + " ";
            }
            if (P_User_Mst_Fk > 0)
            {
                strSQL = strSQL + " And User_Mst_Fk = " + P_User_Mst_Fk + " ";
            }
            if (P_Location_Mst_Fk > 0)
            {
                strSQL = strSQL + " And Location_Mst_Fk = " + P_Location_Mst_Fk + " ";
            }
            if (P_Config_Mst_Fk > 0)
            {
                strSQL = strSQL + " And Config_Mst_Fk = " + P_Config_Mst_Fk + " ";
            }
            if (P_Allowed_Operations_Value > 0)
            {
                strSQL = strSQL + " And Allowed_Operations_Value = " + P_Allowed_Operations_Value + " ";
            }
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
        #endregion

        #region "Fetch Menu"
        public DataSet FetchMenu(Int32 ROLE, Int32 USER, Int32 chk = 0)
        {
            string strSQL = null;
            strSQL = "SELECT ROWNUM SLNO,QRY.PK,QRY.MENU_PK,QRY.MENU_TEXT,QRY.MENU_PARENT,";
            strSQL = strSQL + " QRY.DISPLAY_ORDER,QRY.MENU_LEVEL,QRY.CONFIG_ID_FK,QRY.ACCESSRIGHT,";
            if (chk == 0)
            {
                strSQL = strSQL + " (CASE WHEN QRY.USRALLOWEDRIGHT>0 THEN QRY.USRALLOWEDRIGHT ELSE QRY.ROLEALLOWEDRIGHT END ) ALLOWEDRIGHT,";
            }
            else
            {
                strSQL = strSQL + " (CASE WHEN QRY.ROLEALLOWEDRIGHT>0 THEN QRY.ROLEALLOWEDRIGHT ELSE QRY.USRALLOWEDRIGHT END ) ALLOWEDRIGHT,";
            }
            strSQL = strSQL + " QRY.MALLOWEDRIGHT, QRY.VERSION_NO,";
            strSQL = strSQL + " (case when ( select count(*) from menu_mst_tbl menu where menu.active_flag=1 and menu.config_id_fk=QRY.CONFIG_ID_FK) > 1 then ";
            if (chk == 1)
            {
                strSQL = strSQL + " (case when QRY.ROLEALLOWEDRIGHT > 0 then 1 else 0 end ) else ( ";
            }
            else
            {
                strSQL = strSQL + " (case when (instr(','||QRY.menu_mst_fk||',' , ','||QRY.MENU_PK||',')) > 0 AND qry.role_fk=" + ROLE + " then 1 else 0 end ) else (";
            }
            if (chk == 1)
            {
                strSQL = strSQL + " (CASE WHEN QRY.ROLEALLOWEDRIGHT  > 0 then 1 else 0 end ) ";
            }
            else
            {
                strSQL = strSQL + " (CASE WHEN QRY.USRALLOWEDRIGHT  > 0 AND qry.role_fk=" + ROLE + " then 1 else 0 end ) ";
            }
            strSQL = strSQL + "   ) end ) SELECTED, ";
            strSQL = strSQL + " (CASE WHEN NVL(QRY.USRALLOWEDRIGHT,0)=0 THEN 0 ELSE 1 END)   MODFLAG , ";
            strSQL = strSQL + " QRY.USER_ACCESS_PK, QRY.menu_mst_fk, QRY.ALLOWED_OPERATIONS ";
            strSQL = strSQL + " FROM ( SELECT NVL(USR.USER_ACCESS_PK,0) PK,";
            strSQL = strSQL + " NVL(MST.MENU_MST_PK,0) MENU_PK,    ";
            strSQL = strSQL + " NVL(MTXT.MENU_TEXT,'')  MENU_TEXT ,";
            strSQL = strSQL + " NVL(MST1.MENU_ID,'')  MENU_PARENT ,";
            strSQL = strSQL + " MST.DISPLAY_ORDER, MST.MENU_LEVEL ,";
            strSQL = strSQL + " MST.CONFIG_ID_FK CONFIG_ID_FK,";
            strSQL = strSQL + " NVL(CONFMST.APPLICABLE_OPERATIONS_VALUE,0) ACCESSRIGHT,         ";
            strSQL = strSQL + " NVL(USR.ALLOWED_OPERATIONS_VALUE,0)USRALLOWEDRIGHT,";
            strSQL = strSQL + " NVL(USR.ALLOWED_OPERATIONS_VALUE, 0)+(SELECT COUNT(*) FROM CONFIG_RIGHTS_TBL C WHERE ";
            strSQL = strSQL + "  (C.CONFIG_ID_FK IN (SELECT C.CONFIG_MST_PK FROM CONFIG_MST_TBL C WHERE C.PARENT_CONFIG_ID_FK = CONFMST.CONFIG_MST_PK)) AND C.ACTIVE_FLAG=1) MALLOWEDRIGHT ,";
            strSQL = strSQL + " NVL((SELECT USRROLE.ALLOWED_OPERATIONS_VALUE ";
            strSQL = strSQL + " FROM ROLE_ACCESS_TRN USRROLE ";
            strSQL = strSQL + " WHERE(USRROLE.ROLE_MST_FK = " + ROLE + ")";
            strSQL = strSQL + " AND USRROLE.CONFIG_MST_FK=CONFMST.CONFIG_MST_PK AND (instr(','||USRROLE.menu_mst_fk||',' , ','||MST.MENU_MST_PK||',')) > 0) ,0)  ROLEALLOWEDRIGHT,";
            strSQL = strSQL + " nvl(USR.VERSION_NO,0) VERSION_NO ,";
            strSQL = strSQL + " nvl(USR.USER_ACCESS_PK,0) USER_ACCESS_PK";
            strSQL = strSQL + " ,USR.MENU_MST_FK ,USR.ALLOWED_OPERATIONS, usr.role_fk  ";
            strSQL = strSQL + " FROM MENU_MST_TBL MST,";
            strSQL = strSQL + " MENU_MST_TBL MST1,";
            strSQL = strSQL + " MENU_TEXT_MST_TBL MTXT,";
            strSQL = strSQL + " CONFIG_MST_TBL CONFMST ,";
            strSQL = strSQL + " USER_ACCESS_TRN USR";
            strSQL = strSQL + " WHERE";
            strSQL = strSQL + " MTXT.MENU_MST_FK(+) = MST.MENU_MST_PK";
            strSQL = strSQL + " AND MST.MENU_MST_FK=MST1.MENU_MST_PK   ";
            strSQL = strSQL + " AND CONFMST.CONFIG_MST_PK(+)= MST.CONFIG_ID_FK    ";
            strSQL = strSQL + " AND CONFMST.CONFIG_MST_PK=USR.CONFIG_MST_FK(+)";
            strSQL = strSQL + " AND USR.USER_MST_FK(+)=" + USER + "";
            strSQL = strSQL + " AND MTXT.ENVIRONMENT_MST_FK = 1 ";
            strSQL = strSQL + " AND MST.ACTIVE_FLAG = 1 ";



            if ((Int16)HttpContext.Current.Session["BIZ_TYPE"] == 1)
            {
                strSQL = strSQL + "  AND MST.BIZ_TYPE IN (1,3)";
            }
            else if ((Int16)HttpContext.Current.Session["BIZ_TYPE"] == 2)
            {
                strSQL = strSQL + "  AND MST.BIZ_TYPE IN (2,3)";
            }



            strSQL = strSQL + " ORDER BY MST.DISPLAY_ORDER) QRY";
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
        #endregion

        #region "User Listing Search"
        public DataSet UserListingSearch(string Str, string Str1, string Str2, string Searchtype, string strColumnName = "", bool blnSortAscending = false, Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 flag = 0)
        {
            string strSQL = null;
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string sql = null;
            string strCondition = null;
            string SortExpression = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            SortExpression = "ORDER BY USER_ID";
            if (flag == 0)
            {
                strCondition += " AND 1=2";
            }
            if (Searchtype == "S")
            {
                if (Str.Length > 0)
                {
                    strCondition = strCondition + " and upper(USR.user_id) like '" + Str.ToUpper().Replace("'", "''") + "%'";
                }
                if (Str1.Length > 0)
                {
                    strCondition = strCondition + " and upper(USR.user_name) like '" + Str1.ToUpper().Replace("'", "''") + "%'";
                }
                if (Str2.Length > 0)
                {
                    strCondition = strCondition + " and upper(ROL.ROLE_DESCRIPTION) like '" + Str2.ToUpper().Replace("'", "''") + "%'";
                }
            }
            else
            {
                if (Str.Length > 0)
                {
                    strCondition = strCondition + " and upper(USR.user_id) like '%" + Str.ToUpper().Replace("'", "''") + "%'";
                }
                if (Str1.Length > 0)
                {
                    strCondition = strCondition + " and upper(USR.user_name) like '%" + Str1.ToUpper().Replace("'", "''") + "%'";
                }
                if (Str2.Length > 0)
                {
                    strCondition = strCondition + " and upper(ROL.ROLE_DESCRIPTION) like '%" + Str2.ToUpper().Replace("'", "''") + "%'";
                }
            }
            strSQL = "select Count(*) from user_mst_tbl USR,Role_Mst_Tbl ROL where ROL.ROLE_MST_TBL_PK(+)=USR.ROLE_MST_FK and USR.IS_ACTIVATED = 1 ";
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
            strSQL = "SELECT * from (";
            strSQL = strSQL + " SELECT ROWNUM SR_NO, q.* FROM ";
            strSQL = strSQL + "(SELECT USR.user_mst_pk,";
            strSQL = strSQL + " USR.is_activated,";
            strSQL = strSQL + " USR.user_id,";
            strSQL = strSQL + " USR.user_name,";
            strSQL = strSQL + " LOC.location_mst_pk,";
            strSQL = strSQL + " LOC.location_id,";
            strSQL = strSQL + " ROL.ROLE_DESCRIPTION";
            strSQL = strSQL + " FROM user_mst_tbl USR,";
            strSQL = strSQL + " location_mst_tbl LOC,";
            strSQL = strSQL + " Role_Mst_Tbl ROL";
            strSQL = strSQL + " WHERE (USR.default_location_fk = Loc.location_mst_pk)";
            strSQL = strSQL + " and ROL.ROLE_MST_TBL_PK(+)=USR.ROLE_MST_FK";
            strSQL = strSQL + " and USR.IS_ACTIVATED=1";
            //'Added By Koteshwari on 12/4/2011 to display only active users.
            strSQL = strSQL + strCondition;
            if (!strColumnName.Equals("SR_NO"))
            {
                strSQL += "order by " + strColumnName;
            }
            if (!blnSortAscending & !strColumnName.Equals("SR_NO"))
            {
                strSQL += " DESC";
            }
            strSQL = strSQL + " ) q) WHERE SR_NO  Between " + start + " and " + last;
            strSQL += " ORDER BY  SR_NO";
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

        #region "Fetch Header"
        public DataSet FetchHeader(Int32 Val)
        {
            string StrSQl = null;
            StrSQl = "select USR.user_mst_pk, ";
            StrSQl = StrSQl + "USR.user_id, ";
            StrSQl = StrSQl + "USR.user_name, ";
            StrSQl = StrSQl + "nvl(ROL.ROLE_MST_TBL_PK,0) ROLE_MST_TBL_PK,";
            StrSQl = StrSQl + "ROL.ROLE_DESCRIPTION, ";
            StrSQl = StrSQl + "USR.is_activated,USR.VERSION_NO ";
            StrSQl = StrSQl + "from ";
            StrSQl = StrSQl + "user_mst_tbl USR, ";
            StrSQl = StrSQl + "Role_Mst_Tbl ROL where ROL.ROLE_MST_TBL_PK(+)=USR.ROLE_MST_FK ";
            if (Val > 0)
            {
                StrSQl = StrSQl + " and USR.user_mst_pk=" + Val + "";
            }
            else
            {
                StrSQl = StrSQl + " and USR.user_mst_pk=" + -1 + "";
            }
            WorkFlow objWF = new WorkFlow();
            try
            {
                return objWF.GetDataSet(StrSQl);
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

        #region "Delete Grid"
        public int DeleteGrid(Int32 PK)
        {
            string GateInQuery = null;
            Int32 i = default(Int32);
            WorkFlow objWK = new WorkFlow();
            string DeptDate = null;
            try
            {
                objWK.OpenConnection();
                GateInQuery = " Delete from User_Access_Trn t where t.user_mst_fk= " + PK;
                if (objWK.ExecuteCommands(GateInQuery) == true)
                {
                    return 1;
                }
                else
                {
                    return -1;
                }
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

        #region "Delete Head"
        public int DeleteHead(Int32 PK)
        {
            string GateInQuery = null;
            Int32 i = default(Int32);
            WorkFlow objWK = new WorkFlow();
            string DeptDate = null;
            try
            {
                objWK.OpenConnection();
                GateInQuery = " Delete from user_mst_tbl F where f.user_mst_pk= " + PK;
                if (objWK.ExecuteCommands(GateInQuery) == true)
                {
                    return 1;
                }
                else
                {
                    return -1;
                }
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

        #region "Save Function Grid"
        public ArrayList SaveGrid(DataSet M_GRIDDS, Int64 lblUserpk, Int64 location_mst_fk)
        {
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
                var _with1 = insCommand;
                _with1.Transaction = TRAN;
                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".USER_ACCESS_TRN_PKG.USER_ACCESS_ENTRY_INS";
                Int32 i = default(Int32);
                for (i = 0; i <= M_GRIDDS.Tables[0].Rows.Count - 1; i++)
                {
                    if ((Int16)M_GRIDDS.Tables[0].Rows[i]["Selected"] == 1 & (Int64)M_GRIDDS.Tables[0].Rows[i]["ALLOWEDRIGHT"] != (Int64)M_GRIDDS.Tables[0].Rows[i]["MODFLAG"])
                    {
                        var _with2 = _with1.Parameters;
                        _with2.Clear();
                        insCommand.Parameters.Add("USER_ACCESS_PK_IN", M_GRIDDS.Tables[0].Rows[i]["USER_ACCESS_PK"]).Direction = ParameterDirection.Input;
                        insCommand.Parameters.Add("USER_MST_FK_IN", lblUserpk).Direction = ParameterDirection.Input;
                        insCommand.Parameters.Add("LOCATION_MST_FK_IN", location_mst_fk).Direction = ParameterDirection.Input;
                        insCommand.Parameters.Add("CONFIG_MST_FK_IN", M_GRIDDS.Tables[0].Rows[i]["CONFIG_ID_FK"]).Direction = ParameterDirection.Input;
                        insCommand.Parameters.Add("ALLOWED_OPERATIONS_VALUE_IN", M_GRIDDS.Tables[0].Rows[i]["ALLOWEDRIGHT"]).Direction = ParameterDirection.Input;
                        insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                        insCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                        insCommand.Parameters.Add("VERSION_NO_IN", M_GRIDDS.Tables[0].Rows[i]["VERSION_NO"]).Direction = ParameterDirection.Input;
                        insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "USER_ACCESS_TRN_PK").Direction = ParameterDirection.Output;
                        insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                        insCommand.ExecuteNonQuery();
                    }
                }
                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    arrMessage.Add("All Data Saved Successfully");
                    TRAN.Commit();
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

        #region "UpdateHead"
        public int UpdateHead(Int32 Role, Int16 iActive, Int32 PK, Int32 iVersion)
        {
            string GateInQuery = null;
            Int32 i = default(Int32);
            WorkFlow objWK = new WorkFlow();
            string DeptDate = null;
            try
            {
                objWK.OpenConnection();
                if (Role > 0)
                {
                    GateInQuery = "Update user_mst_tbl rr set rr.is_activated=" + iActive + ", rr.role_mst_fk=" + Role + " where rr.user_mst_pk= " + PK + " And rr.Version_No=" + iVersion;
                }
                else
                {
                    GateInQuery = "Update user_mst_tbl rr set rr.is_activated=" + iActive + ", rr.role_mst_fk=null where rr.user_mst_pk= " + PK + " And rr.Version_No=" + iVersion;
                }
                if (objWK.ExecuteCommands(GateInQuery) == true)
                {
                    return 1;
                }
                else
                {
                    return -1;
                }
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

        #region "Get Dummy"
        public DataSet GetDummy(long User_Pk)
        {
            string StrSQl = null;
            WorkFlow objWK = new WorkFlow();
            StrSQl = " Select User_Access_Pk,Config_Mst_Fk,Allowed_Operations_Value,Version_No,menu_mst_fk, 0 role_fk,ALLOWED_OPERATIONS  From User_Access_Trn  Where User_Mst_Fk=" + User_Pk;
            try
            {
                return objWK.GetDataSet(StrSQl);
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

        #region "Save Grid"
        public ArrayList SaveGrid(DataSet dsAccess, long Loc_Pk, long User_Pk, long Role_Pk, Int16 iActive)
        {
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
            OracleCommand updCommand1 = new OracleCommand();
            try
            {
                UPDROLE(User_Pk, Role_Pk);
                var _with3 = insCommand;
                _with3.Connection = objWK.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".USER_ACCESS_TRN_PKG.USER_ACCESS_TRN_INS";
                var _with4 = _with3.Parameters;
                insCommand.Parameters.Add("USER_MST_FK_IN", User_Pk).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("LOCATION_MST_FK_IN", Loc_Pk).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("CONFIG_MST_FK_IN", OracleDbType.Int32, 10, "CONFIG_MST_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["CONFIG_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("MENU_MST_FK_IN", OracleDbType.Varchar2, 50, "MENU_MST_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["MENU_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("ROLE_FK_IN", OracleDbType.Int32, 10, "ROLE_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["ROLE_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("ALLOWED_OPERATIONS_IN", OracleDbType.Varchar2, 20, "ALLOWED_OPERATIONS").Direction = ParameterDirection.Input;
                insCommand.Parameters["ALLOWED_OPERATIONS_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("ALLOWED_OPERATIONS_VALUE_IN", OracleDbType.Int32, 10, "ALLOWED_OPERATIONS_VALUE").Direction = ParameterDirection.Input;
                insCommand.Parameters["ALLOWED_OPERATIONS_VALUE_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "USER_ACCESS_TRN_PK").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with5 = delCommand;
                _with5.Connection = objWK.MyConnection;
                _with5.CommandType = CommandType.StoredProcedure;
                _with5.CommandText = objWK.MyUserName + ".USER_ACCESS_TRN_PKG.USER_ACCESS_TRN_DEL";
                var _with6 = _with5.Parameters;
                delCommand.Parameters.Add("USER_ACCESS_PK_IN", OracleDbType.Int32, 10, "USER_ACCESS_PK").Direction = ParameterDirection.Input;
                delCommand.Parameters["USER_ACCESS_PK_IN"].SourceVersion = DataRowVersion.Current;
                delCommand.Parameters.Add("DELETED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                delCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                delCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
                delCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                delCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                delCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with7 = updCommand;
                _with7.Connection = objWK.MyConnection;
                _with7.CommandType = CommandType.StoredProcedure;
                _with7.CommandText = objWK.MyUserName + ".USER_ACCESS_TRN_PKG.USER_ACCESS_TRN_UPD";
                var _with8 = _with7.Parameters;
                updCommand.Parameters.Add("USER_ACCESS_PK_IN", OracleDbType.Int32, 10, "USER_ACCESS_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["USER_ACCESS_PK_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("USER_MST_FK_IN", User_Pk).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("LOCATION_MST_FK_IN", Loc_Pk).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("CONFIG_MST_FK_IN", OracleDbType.Int32, 10, "CONFIG_MST_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["CONFIG_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("MENU_MST_FK_IN", OracleDbType.Varchar2, 50, "MENU_MST_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["MENU_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("ROLE_FK_IN", OracleDbType.Int32, 10, "ROLE_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["ROLE_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("ALLOWED_OPERATIONS_IN", OracleDbType.Varchar2, 20, "ALLOWED_OPERATIONS").Direction = ParameterDirection.Input;
                updCommand.Parameters["ALLOWED_OPERATIONS_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("ALLOWED_OPERATIONS_VALUE_IN", OracleDbType.Int32, 10, "ALLOWED_OPERATIONS_VALUE").Direction = ParameterDirection.Input;
                updCommand.Parameters["ALLOWED_OPERATIONS_VALUE_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 10, "VERSION_NO").Direction = ParameterDirection.Input;
                updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                var _with9 = objWK.MyDataAdapter;
                _with9.InsertCommand = insCommand;
                _with9.InsertCommand.Transaction = TRAN;
                _with9.UpdateCommand = updCommand;
                _with9.UpdateCommand.Transaction = TRAN;
                _with9.DeleteCommand = delCommand;
                _with9.DeleteCommand.Transaction = TRAN;
                RecAfct = _with9.Update(dsAccess);
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
                TRAN.Rollback();
                throw oraexp;
            }
            catch (Exception ex)
            {
                TRAN.Rollback();
                throw ex;
            }
            finally
            {
                objWK.CloseConnection();
            }
        }
        #endregion

        #region "Update Role"
        private void UPDROLE(long userpk, long rolepk, OracleTransaction TRANsac = null)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            try
            {
                if ((TRANsac != null))
                {
                    objWK.MyConnection = TRANsac.Connection;
                    OracleCommand updCommand = new OracleCommand();

                    var _with10 = updCommand;
                    _with10.Connection = objWK.MyConnection;
                    _with10.CommandType = CommandType.StoredProcedure;
                    _with10.Transaction = TRANsac;
                    _with10.CommandText = objWK.MyUserName + ".USER_ACCESS_TRN_PKG.USER_ACCESS_ROLE_UP";
                    var _with11 = _with10.Parameters;
                    _with11.Add("USER_PK_IN", userpk).Direction = ParameterDirection.Input;
                    _with11.Add("ROLE_PK_IN", rolepk).Direction = ParameterDirection.Input;
                    _with10.ExecuteNonQuery();
                }
                else
                {
                    TRAN = objWK.MyConnection.BeginTransaction();
                    OracleCommand updCommand = new OracleCommand();
                    var _with12 = updCommand;
                    _with12.Connection = objWK.MyConnection;
                    _with12.CommandType = CommandType.StoredProcedure;
                    _with12.Transaction = TRAN;
                    _with12.CommandText = objWK.MyUserName + ".USER_ACCESS_TRN_PKG.USER_ACCESS_ROLE_UP";
                    var _with13 = _with12.Parameters;
                    _with13.Add("USER_PK_IN", userpk).Direction = ParameterDirection.Input;
                    _with13.Add("ROLE_PK_IN", rolepk).Direction = ParameterDirection.Input;
                    _with12.ExecuteNonQuery();
                    TRAN.Commit();
                }
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
                objWK.CloseConnection();
            }
        }
        #endregion

        #region "fetch ExtraRights"
        public DataSet fn_GetExtraRights(long MenuPK, long UserAccessPK, long UserPK)
        {
            try
            {
                WorkFlow objWK = new WorkFlow();
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("SELECT ROWNUM SLNR, QRY.*");
                sb.Append("  FROM (SELECT CRT.CONFIG_RIGHTS_PK,");
                sb.Append("               CMT.CONFIG_MST_PK,");
                sb.Append("               CMT.CONFIG_NAME,");
                sb.Append("               CRT.CONTROL_DESC RIGHTS,");
                sb.Append("               NVL(URT.ACTIVE_FLAG, 0) ACTIVE_FLAG,");
                sb.Append("               URT.USER_ACCESS_RIGHTS_PK,");
                sb.Append("               UAT.USER_ACCESS_PK");
                sb.Append("          FROM CONFIG_RIGHTS_TBL      CRT,");
                sb.Append("               CONFIG_MST_TBL         CMT,");
                sb.Append("               USER_ACCESS_RIGHTS_TRN URT,");
                sb.Append("               USER_ACCESS_TRN        UAT");
                sb.Append("         WHERE CRT.CONFIG_ID_FK = CMT.CONFIG_MST_PK");
                sb.Append("           AND URT.CONFIG_RIGHTS_FK(+) = CRT.CONFIG_RIGHTS_PK");
                sb.Append("           AND UAT.USER_ACCESS_PK(+) = URT.USER_ACCESS_FK");
                sb.Append("            AND UAT.USER_MST_FK =" + UserPK + "");
                sb.Append("            AND URT.USER_ACCESS_FK=" + UserAccessPK + "");
                sb.Append("           AND CRT.CONFIG_ID_FK IN");
                sb.Append("               (SELECT C.CONFIG_MST_PK");
                sb.Append("                  FROM CONFIG_MST_TBL C");
                sb.Append("                 WHERE C.PARENT_CONFIG_ID_FK = " + MenuPK + ")");
                sb.Append("        union");
                sb.Append("        SELECT CRT.CONFIG_RIGHTS_PK,");
                sb.Append("               CMT.CONFIG_MST_PK,");
                sb.Append("               CMT.CONFIG_NAME,");
                sb.Append("               CRT.CONTROL_DESC RIGHTS,");
                sb.Append("               0 ACTIVE_FLAG,");
                sb.Append("               0 USER_ACCESS_RIGHTS_PK,");
                sb.Append("               0 USER_ACCESS_PK");
                sb.Append("          FROM CONFIG_RIGHTS_TBL CRT, CONFIG_MST_TBL CMT");
                sb.Append("         WHERE CRT.CONFIG_ID_FK = CMT.CONFIG_MST_PK");
                sb.Append("           AND CRT.CONFIG_ID_FK IN");
                sb.Append("               (SELECT C.CONFIG_MST_PK");
                sb.Append("                  FROM CONFIG_MST_TBL C");
                sb.Append("                 WHERE C.PARENT_CONFIG_ID_FK = " + MenuPK + ")");
                sb.Append("           AND CRT.CONFIG_RIGHTS_PK NOT IN");
                sb.Append("               (SELECT TTT.CONFIG_RIGHTS_FK");
                sb.Append("                  FROM USER_ACCESS_RIGHTS_TRN TTT, USER_ACCESS_TRN TT");
                sb.Append("                 WHERE TTT.USER_ACCESS_FK = TT.USER_ACCESS_PK");
                sb.Append("                   AND  TTT.USER_ACCESS_FK=" + UserAccessPK + "");
                sb.Append("                   AND TT.USER_MST_FK = " + UserPK + ")) QRY");
                return objWK.GetDataSet(sb.ToString());
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


        #region "user Access More Rights"
        public void fn_Remove_UserAccessMoreRights(Int32 UAPK)
        {
            try
            {
                WorkFlow objDAL = new WorkFlow();
                string strSQL1 = null;
                strSQL1 = " DELETE FROM USER_ACCESS_RIGHTS_TRN  URT WHERE URT.USER_ACCESS_FK =" + UAPK;
                objDAL.ExecuteCommands(strSQL1);
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

    }
}
