
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

namespace Quantum_QFOR
{
    public class cls_UserActivity : CommonFeatures
    {
        #region "fn_FetchData"

        public DataSet fn_FetchData(Int32 strGridColumns, Int32 strFilteringCond, string strFilteringText, string FromDate = "", string ToDate = "", bool ChkStatus = true, Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 flag = 0, int Excel = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            System.Text.StringBuilder sbCondition = new System.Text.StringBuilder(5000);
            string GridCol = null;
            string FilteringCond = null;
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string SQLQuery = null;
            Int32 TotalRecords = default(Int32);

            switch (strGridColumns)
            {
                case 1:
                    GridCol = "V.USER_ID";
                    break;

                case 2:
                    GridCol = "V.FIRST_NAME";
                    break;

                case 3:
                    GridCol = "V.LAST_NAME";
                    break;

                case 4:
                    GridCol = "V.LOCATION_ID";
                    break;
                    //Case 5
                    //    GridCol = "V.MENU_ID"
                    //Case 6
                    //    GridCol = "V.CURRENT_ACTIVITY"
            }

            switch (strFilteringCond)
            {
                case 1:
                    FilteringCond = " LIKE '" + strFilteringText.ToUpper() + "%'";
                    break;

                case 2:
                    FilteringCond = " LIKE '%" + strFilteringText.ToUpper() + "%'";
                    break;

                case 3:
                    FilteringCond = "  BETWEEN to_date('" + FromDate + " ','" + M_DateFormat + "') ";
                    FilteringCond += " AND    ";
                    FilteringCond += " to_date('" + ToDate + " ','" + M_DateFormat + "') ";
                    break;

                case 4:
                    FilteringCond = " <  to_date('" + FromDate + " ','" + M_DateFormat + "') ";
                    break;

                case 5:
                    FilteringCond = " >  to_date('" + FromDate + " ','" + M_DateFormat + "') ";
                    break;
            }

            sb.Append(" SELECT ROWNUM SLNR,Q.* FROM (SELECT * FROM VIEW_QCOR_UA_DASHBOARD V WHERE 1=1 ");

            if (flag == 0)
            {
                sb.Append(" AND 1=2 ");
            }

            if (strGridColumns != 0 & strFilteringCond != 0)
            {
                ///'''''''''
                sbCondition.Append(" AND  " + GridCol + FilteringCond + "");
                sb.Append(sbCondition);
                ///'''''''''
            }

            if (ChkStatus == true)
            {
                sb.Append(" AND V.LOGIN_STATUS ='Active'");
            }
            sb.Append("  ORDER BY APPLICATION_LOGIN DESC  ) Q  ");

            SQLQuery = "SELECT COUNT(*) FROM (" + sb.ToString() + ")";

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(SQLQuery));
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
            if (Excel > 0)
            {
                SQLQuery = " SELECT * FROM (" + sb.ToString() + ") ";
            }
            else
            {
                SQLQuery = " SELECT * FROM (" + sb.ToString() + ") WHERE SLNR Between " + start + " and " + last;
            }

            try
            {
                ds = objWF.GetDataSet(SQLQuery);
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

        #endregion "fn_FetchData"

        #region "fn_FetchProductsClients()- Function to fetch the Products & Clients for Tree View"

        public DataSet fn_FetchProductsClients()
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds_ProductClient = new DataSet();
            string str = null;
            try
            {
                //Query to get all the quantum products
                str = " SELECT DISTINCT PC.QPRODUCT_ID,PC.DISPLAY_ORDER ";
                str = str + " FROM QCOR_GEN_M_PRODUCT_CLIENTS PC ";
                str = str + " ORDER BY PC.DISPLAY_ORDER ";
                ds_ProductClient.Tables.Add(objWF.GetDataTable(str));
                ds_ProductClient.Tables[0].TableName = "QPRODUCTS";

                //Query to get all the Clients
                str = "       SELECT DISTINCT C.CLIENT_PK, ";
                str = str + " C.CLEINT_NAME CLIENT_NAME, ";
                str = str + " Q.QPRODUCT_ID, ";
                str = str + " Q.DISPLAY_ORDER ";
                str = str + " FROM QCOR_GEN_M_PRODUCT_CLIENTS Q, QCOR_LIC_M_CLIENT C ";
                str = str + " WHERE(Q.CLIENT_FK = C.CLIENT_PK) ";
                str = str + " ORDER BY Q.DISPLAY_ORDER ";
                //ds_ProductClient.Tables.Add(objWF.GetDataTable(str))  'commented by sherin and replaced with getTable(str) to fix the bug "DataTable already belongs to this Dataset"
                ds_ProductClient.Tables.Add(getTable(str));
                ds_ProductClient.Tables[1].TableName = "CLIENTS";

                //Set relationship between Products Table (table 0) and Client Table (Table 1)
                DataRelation objRel1 = new DataRelation("REL_PROD_CLIENT", ds_ProductClient.Tables[0].Columns["QPRODUCT_ID"], ds_ProductClient.Tables[1].Columns["QPRODUCT_ID"]);
                objRel1.Nested = true;
                ds_ProductClient.Relations.Add(objRel1);

                return ds_ProductClient;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public DataTable getTable(string str)
        {
            WorkFlow objWF = new WorkFlow();
            return objWF.GetDataTable(str);
        }

        #endregion "fn_FetchProductsClients()- Function to fetch the Products & Clients for Tree View"

        #region "fn_Fetch_DBConnectionStr()- Function to fetch the DataBase Connection String from QCOR_GEN_M_PRODUCT_CLIENTS Table"

        public DataSet fn_Fetch_DBConnectionStr(string strProductID, string strClientID)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

            sb.Append(" SELECT PC.DBCONNECTIONSTRING,");
            sb.Append(" PC.DBUSERID ");
            sb.Append(" FROM QCOR_GEN_M_PRODUCT_CLIENTS PC ");
            sb.Append(" WHERE PC.QPRODUCT_ID = '" + strProductID + "' ");
            sb.Append(" AND PC.CLIENT_FK = " + strClientID + " ");

            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion "fn_Fetch_DBConnectionStr()- Function to fetch the DataBase Connection String from QCOR_GEN_M_PRODUCT_CLIENTS Table"

        #region "fn_fetchUsrInformation"

        public DataTable fn_Fetch_UsrDetails(string SessionID)
        {
            try
            {
                WorkFlow objWF = new WorkFlow();
                System.Text.StringBuilder sb = new System.Text.StringBuilder(1000);
                sb.Append("SELECT V.USER_ID,");
                sb.Append("       V.FIRST_NAME,");
                sb.Append("       V.LAST_NAME,");
                sb.Append("       V.DEPARTMENT_ID,");
                sb.Append("       V.DESIGNATION_ID,");
                sb.Append("       V.LOCATION_ID");
                sb.Append("  FROM VIEW_QCOR_UA_HISTORY_HDR V");
                sb.Append(" WHERE UPPER(V.USER_ID) = UPPER('" + SessionID + "')");
                return objWF.GetDataTable(sb.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public DataTable fn_fetch_UsrGridDetails(string UsrID)
        {
            try
            {
                WorkFlow objWF = new WorkFlow();
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

                sb.Append(" SELECT ");
                sb.Append("                ROWNUM SLNR,");
                sb.Append("                QRY.* FROM (");
                sb.Append("                SELECT ");
                sb.Append("                       VH.CONFIG_MST_PK,");
                sb.Append("                       VH.LOGIN_DATETIME,");
                sb.Append("                       VH.LOGOUT_DATETIME,");
                sb.Append("                       VH.MODULENAME,");
                sb.Append("                       VH.MENU_ID,");
                sb.Append("                       VH.VISITED_TIME_IN,");
                sb.Append("                       VH.VISITED_TIME_OUT,");
                sb.Append("                       VH.DURATION");
                sb.Append("                  FROM VIEW_USER_ACTIVITY_HISTORY VH");
                sb.Append("                      WHERE UPPER(VH.USER_ID) = UPPER('" + UsrID + "') ORDER BY LOGIN_DATETIME DESC,VISITED_TIME_IN DESC)QRY");

                return objWF.GetDataTable(sb.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        ///'''''''''''''''''
        public DataTable fn_fetch_UsrVisitedDetails(string UsrID, Int32 CurrentPage, Int32 TotalPage)
        {
            try
            {
                WorkFlow objWF = new WorkFlow();
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

                sb.Append(" SELECT ");
                sb.Append(" ROWNUM SLNR,");
                sb.Append(" QRY.* FROM (");
                sb.Append(" SELECT VH.MENU_NAME,");
                sb.Append(" VH.FORM_NAME,");
                sb.Append(" VH.VISITED_TIME_IN,");
                sb.Append(" VH.VISITED_TIME_OUT");
                sb.Append(" FROM VIEW_QCOR_UA_HISTORY_DTL VH");
                sb.Append(" WHERE UPPER(VH.USER_ID) = UPPER('" + UsrID + "') ORDER BY VISITED_TIME_IN DESC)QRY");

                ///'''''''''''''''common''''''''''''''''''''
                //Get the Total Pages
                Int32 last = default(Int32);
                Int32 start = default(Int32);
                Int32 TotalRecords = default(Int32);
                string StrSqlCount = null;

                StrSqlCount = "SELECT COUNT(*) FROM ( ";
                StrSqlCount = StrSqlCount + sb.ToString();
                StrSqlCount = StrSqlCount + " ) ";

                TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(StrSqlCount.ToString()));
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

                ///'''''''''''''''''''''''''''''''''''''''''''''''''''
                ///'''''''''''''''''''''''''''common''''''''''''''''''''''''''''''''''
                string StrSqlRecords = null;
                StrSqlRecords = "SELECT SLNR, MENU_NAME, FORM_NAME, VISITED_TIME_IN, VISITED_TIME_OUT, FN_DURATION(VISITED_TIME_IN, VISITED_TIME_OUT) DURATION FROM ( ";
                StrSqlRecords = StrSqlRecords + sb.ToString();
                StrSqlRecords = StrSqlRecords + " ) WHERE SLNR BETWEEN " + start + " AND " + last;

                return objWF.GetDataTable(StrSqlRecords);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "fn_fetchUsrInformation"

        #region "For dash Board details"

        public DataTable fn_DashBoarDetails1()
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT FN_UA_DASHBOARD_DETAILS('BOOKING') TOT_BOOKING,");
            sb.Append("FN_UA_DASHBOARD_DETAILS('TOTALINVOICE') TOT_INVOICE,");
            sb.Append("FN_UA_DASHBOARD_DETAILS('BOOKINGDATE') LST_BKG_DT,");
            sb.Append("FN_UA_DASHBOARD_DETAILS('LSTUSRLOGIN') LST_USR_LOGIN,");
            sb.Append("FN_UA_DASHBOARD_DETAILS('LSTUSRLOGINDT') LST_USR_LOGINDT,");
            sb.Append("FN_UA_DASHBOARD_DETAILS('USRONLINE') USR_ONLINE,");
            sb.Append("FN_UA_DASHBOARD_DETAILS('USROFFLINE') USR_OFFLINE,");
            sb.Append("FN_UA_DASHBOARD_DETAILS('MOSTACTLOCATION') MOST_ACTIVELOCATION,");
            sb.Append("FN_UA_DASHBOARD_DETAILS('MOSTACTUSER') MOST_ACTUSR, ");
            sb.Append("FN_UA_DASHBOARD_DETAILS('MOSTVISITEDPAGE') MOST_VISITPAGE ");
            sb.Append("FROM DUAL");
            try
            {
                WorkFlow objWF = new WorkFlow();
                return objWF.GetDataTable(sb.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "For dash Board details"

        //'added by umasankar
        ///''''''********* saving the LogIn/LogOut users Information details and form Based ACTIVITY TRACKER

        #region "For Dash Board"

        #region "Save the Login details Information"

        public bool fn_SaveLoginDetails(Int64 BranchPk, Int64 userPk, string sessionId, DateTime loginTime, Int64 locationPk = 0, string ClientIP = "")
        {
            Int32 PKValue = default(Int32);
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            OracleCommand insCommand = new OracleCommand();
            try
            {
                objWK.OpenConnection();
                TRAN = objWK.MyConnection.BeginTransaction();
                var _with1 = insCommand;
                ///'''''''''''''''''''''''''''INSERT HDR
                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".PKG_QCOR_UA_TRACKER.QCOR_UA_LOGIN_ACTIVITY_INS";
                var _with2 = _with1.Parameters;
                _with2.Add("SESSION_ID_IN", sessionId);
                _with2.Add("BRANCH_MST_FK_IN", BranchPk);
                _with2.Add("USER_MST_FK_IN", userPk);
                _with2.Add("LOCATION_MST_IN", BranchPk);
                _with2.Add("LOGIN_DATETIME_IN", loginTime);
                _with2.Add("LOGIN_CLIENT_IP_IN", ClientIP);
                _with2.Add("RETURN_VALUE", PKValue).Direction = ParameterDirection.Output;
                ///'''''''''''  insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current
                //AddHandler objWK.MyDataAdapter.RowUpdated, New OracleRowUpdatedEventHandler(AddressOf OnRowUpdated)
                var _with3 = objWK.MyDataAdapter;
                _with3.InsertCommand = insCommand;
                _with3.InsertCommand.Transaction = TRAN;
                _with3.InsertCommand.ExecuteNonQuery();
                TRAN.Commit();
                /// PKValue = CInt(insCommand.Parameters["RETURN_VALUE"].Value)
                return true;
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

        #endregion "Save the Login details Information"

        #region "Update the Login Details"

        public object fn_UserActivityTracking_ProcCall1(string ConfigurationID, string SESSION_ID, DateTime VISITED_TIME_IN, Int32 UserFK)
        {
            Int32 PKValue = default(Int32);
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            OracleCommand insCommand = new OracleCommand();
            try
            {
                objWK.OpenConnection();
                TRAN = objWK.MyConnection.BeginTransaction();
                var _with4 = insCommand;
                ///'''''''''''''''''''''''''''INSERT HDR
                _with4.Connection = objWK.MyConnection;
                _with4.CommandType = CommandType.StoredProcedure;
                _with4.CommandText = objWK.MyUserName + ".PKG_QCOR_UA_TRACKER.QCOR_UA_FORM_ACTIVITY_INS";
                var _with5 = _with4.Parameters;
                _with5.Add("SESSION_ID_IN", SESSION_ID);
                if (string.IsNullOrEmpty(ConfigurationID))
                {
                    _with5.Add("CONFIG_ID_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with5.Add("CONFIG_ID_IN", ConfigurationID).Direction = ParameterDirection.Input;
                }
                _with5.Add("VISITED_TIME_IN_IN", VISITED_TIME_IN);
                _with5.Add("USER_MST_FK_IN", UserFK);
                // AddHandler objWK.MyDataAdapter.RowUpdated, New OracleRowUpdatedEventHandler(AddressOf OnRowUpdated)
                var _with6 = objWK.MyDataAdapter;
                _with6.InsertCommand = insCommand;
                _with6.InsertCommand.Transaction = TRAN;
                _with6.InsertCommand.ExecuteNonQuery();
                TRAN.Commit();
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
            return new object();
        }

        public void fn_UpdateUsrLoginDetails(string strSessionId, Int16 intLoginStatus, DateTime loginTime, Int32 UserFK)
        {
            WorkFlow objWF = new WorkFlow();

            try
            {
                objWF.OpenConnection();
                OracleTransaction TRAN = null;
                TRAN = objWF.MyConnection.BeginTransaction();
                OracleCommand UpdCommand = new OracleCommand();

                var _with7 = UpdCommand;
                _with7.Connection = objWF.MyConnection;
                _with7.CommandType = CommandType.StoredProcedure;
                _with7.CommandText = objWF.MyUserName + ".PKG_QCOR_UA_TRACKER.QCOR_UA_LOGIN_ACTIVITY_UPD";
                var _with8 = _with7.Parameters;
                UpdCommand.Parameters.Add("SESSION_ID_IN", strSessionId);
                UpdCommand.Parameters["SESSION_ID_IN"].SourceVersion = DataRowVersion.Current;

                UpdCommand.Parameters.Add("LOGOUT_DATETIME_IN", loginTime);
                UpdCommand.Parameters["LOGOUT_DATETIME_IN"].SourceVersion = DataRowVersion.Current;

                UpdCommand.Parameters.Add("LOGIN_STATUS_IN", intLoginStatus);
                UpdCommand.Parameters["LOGIN_STATUS_IN"].SourceVersion = DataRowVersion.Current;

                UpdCommand.Parameters.Add("USER_MST_FK_IN", UserFK);
                UpdCommand.Parameters["USER_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                var _with9 = objWF.MyDataAdapter;
                _with9.InsertCommand = UpdCommand;
                _with9.InsertCommand.Transaction = TRAN;
                _with9.InsertCommand.ExecuteNonQuery();
                TRAN.Commit();
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWF.CloseConnection();
            }
        }

        #endregion "Update the Login Details"

        #region "For User Activity Tracking"

        public object fn_UserActivityTracking_ProcCall(string ConfigurationID, string SESSION_ID, DateTime VISITED_TIME_IN, Int32 UserFK)
        {
            Int32 PKValue = default(Int32);
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            OracleCommand insCommand = new OracleCommand();
            try
            {
                objWK.OpenConnection();
                TRAN = objWK.MyConnection.BeginTransaction();
                var _with10 = insCommand;
                ///'''''''''''''''''''''''''''INSERT HDR
                _with10.Connection = objWK.MyConnection;
                _with10.CommandType = CommandType.StoredProcedure;
                _with10.CommandText = objWK.MyUserName + ".PKG_QCOR_UA_TRACKER.QCOR_UA_FORM_ACTIVITY_INS";
                var _with11 = _with10.Parameters;
                _with11.Add("SESSION_ID_IN", SESSION_ID);
                if (string.IsNullOrEmpty(ConfigurationID))
                {
                    _with11.Add("CONFIG_ID_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with11.Add("CONFIG_ID_IN", ConfigurationID).Direction = ParameterDirection.Input;
                }
                _with11.Add("VISITED_TIME_IN_IN", VISITED_TIME_IN);
                _with11.Add("USER_MST_FK_IN", UserFK);
                var _with12 = objWK.MyDataAdapter;
                _with12.InsertCommand = insCommand;
                _with12.InsertCommand.Transaction = TRAN;
                _with12.InsertCommand.ExecuteNonQuery();
                TRAN.Commit();
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
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
            return new object();
        }

        #endregion "For User Activity Tracking"

        #endregion "For Dash Board"

        ///'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

        #region "For dash Board details"

        public DataTable fn_DashBoarDetails()
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT FN_UA_DASHBOARD_DETAILS('BOOKING') TOT_BOOKING,");
            sb.Append("FN_UA_DASHBOARD_DETAILS('TOTALINVOICE') TOT_INVOICE,");
            sb.Append("FN_UA_DASHBOARD_DETAILS('BOOKINGDATE') LST_BKG_DT,");
            sb.Append("FN_UA_DASHBOARD_DETAILS('LSTUSRLOGIN') LST_USR_LOGIN,");
            sb.Append("FN_UA_DASHBOARD_DETAILS('LSTUSRLOGINDT') LST_USR_LOGINDT,");
            sb.Append("FN_UA_DASHBOARD_DETAILS('USRONLINE') USR_ONLINE,");
            sb.Append("FN_UA_DASHBOARD_DETAILS('USROFFLINE') USR_OFFLINE,");
            sb.Append("FN_UA_DASHBOARD_DETAILS('MOSTACTLOCATION') MOST_ACTIVELOCATION,");
            sb.Append("FN_UA_DASHBOARD_DETAILS('MOSTACTUSER') MOST_ACTUSR, ");
            sb.Append("FN_UA_DASHBOARD_DETAILS('MOSTVISITEDPAGE') MOST_VISITPAGE ");
            sb.Append("FROM DUAL");
            try
            {
                WorkFlow objWF = new WorkFlow();
                return objWF.GetDataTable(sb.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "For dash Board details"

        ///'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
    }
}