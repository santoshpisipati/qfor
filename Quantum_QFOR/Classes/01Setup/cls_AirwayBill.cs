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

namespace Quantum_QFOR
{
    public class clsAirwayBill : CommonFeatures
    {
        private int pageSize = 5;
        int _current;
        public int currentfrom
        {
            get { return _current; }
            set { _current = value; }
        }

        #region " Fetch All "
        public string GetCurrentValues(string strAirLineCode = "", string strAirLineDesc = "", short Status = 1)
        {

            Int32 last = default(Int32);
            // holds the last record slo of the page
            Int32 start = default(Int32);
            //holds the first record slo of the page
            string strSQL = null;
            //Holds the query string . 
            string strCondition = null;
            //it holds the where class of the query.
            Int32 TotalRecords = default(Int32);
            //total number of records
            WorkFlow objWF = new WorkFlow();
            //common class object.
            string strReturn = null;
            if (strAirLineCode.Trim().Length > 0)
            {
                strCondition +=" AND UPPER(AIRLINE.AIRLINE_ID) = '" + strAirLineCode.ToUpper().Replace("'", "''") + "'" ;
            }
            if (strAirLineDesc.Trim().Length > 0)
            {
                strCondition +=" AND UPPER(AIRLINE.AIRLINE_NAME) = '" + strAirLineDesc.ToUpper().Replace("'", "''") + "'" ;
            }
            //strSQL = "   SELECT DISTINCT AIR.START_NO,AIR.END_NO, "
            //strSQL &= "  AIR.TOTAL_NOS_USED, "
            //strSQL &= "  ((AIR.END_NO-AIR.START_NO)+1)-AIR.TOTAL_NOS_USED REMAINING, "
            //strSQL &= "  AIR.TOTAL_NOS_CANCELLED  CANCELLED"
            //strSQL &= "   FROM AIRWAY_BILL_MST_TBL AIR,AIRWAY_BILL_TRN AIRT "
            //'modified  by latha
            //strSQL &= "   WHERE (AIR.TOTAL_NOS_USED+AIR.TOTAL_NOS_CANCELLED) >= 0 AND"
            //strSQL &= "  (((AIR.END_NO-AIR.START_NO)+1)-(AIR.TOTAL_NOS_USED+AIR.TOTAL_NOS_CANCELLED))<>0 "
            //strSQL &= "  AND AIR.AIRWAY_BILL_MST_PK = AIRT.AIRWAY_BILL_MST_FK"
            //'ADD BY LATHA
            //strSQL &= "  AND AIR.LOCATION_MST_FK = " & LoggedIn_Loc_FK & " "
            //strSQL &= "  AND AIR.AIRLINE_MST_FK IN "
            //strSQL &= "  (SELECT AIRLINE.AIRLINE_MST_PK "
            //strSQL &= "  FROM AIRLINE_MST_TBL AIRLINE WHERE AIRLINE.ACTIVE_FLAG=1"
            //strSQL &= strCondition & ") "
            //Manoharan 05May07: query given by Surya for AWB no.
            strSQL += " SELECT AIR.START_NO, AIR.END_NO, AIR.TOTAL_NOS_USED, ";
            strSQL += " (AIR.END_NO - AIR.START_NO + 1 - AIR.TOTAL_NOS_USED) REMAINING,";
            strSQL += " AIR.TOTAL_NOS_CANCELLED CANCELLED";
            strSQL += " FROM AIRWAY_BILL_MST_TBL AIR";
            strSQL += " WHERE AIR.AIRWAY_BILL_MST_PK IN";
            strSQL += " (select a.airway_bill_mst_pk from (";
            strSQL += " Select awb.airway_bill_mst_pk";
            strSQL += " from airway_bill_mst_tbl awb";
            strSQL += " where";
            strSQL += " awb.airline_mst_fk = ";
            strSQL += "  (SELECT AIRLINE.AIRLINE_MST_PK ";
            strSQL += "  FROM AIRLINE_MST_TBL AIRLINE WHERE AIRLINE.ACTIVE_FLAG=1";
            strSQL += strCondition + ") ";
            strSQL += " and awb.location_mst_fk = " + LoggedIn_Loc_FK;
            strSQL += " and (awb.end_no - awb.start_no + 1 - awb.total_nos_used + awb.total_nos_cancelled) >0";
            //strSQL &= " order by  (awb.total_nos_used - awb.total_nos_cancelled) desc ) a where rownum=1)"
            strSQL += " order by awb.total_nos_used desc ) a where rownum=1)";
            //end
            objWF.MyDataReader = objWF.GetDataReader(strSQL);

            try
            {
                strReturn = "";
                //Dim index As Integer = 0

                while (objWF.MyDataReader.Read())
                {
                    strReturn = objWF.MyDataReader[0].ToString();
                    strReturn += "," + objWF.MyDataReader[1];
                    strReturn += "," + objWF.MyDataReader[2];
                    strReturn += "," + objWF.MyDataReader[3];
                    strReturn += "," + objWF.MyDataReader[4];
                    return strReturn;
                }
                //Manjunath  PTS ID:Sep-02  13/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                string s = ex.Message;
            }
            finally
            {
                objWF.MyDataReader.Close();
                objWF.MyConnection.Close();
            }
            return "";
        }
        #endregion

        #region " Save Function "

        public ArrayList Save(string strAirlineCode, DataSet M_DataSet)
        {
            WorkFlow objWK = new WorkFlow();
            int pk = 0;
            string s = "select AIRLINE_MST_PK from AIRLINE_MST_TBL WHERE AIRLINE_ID = '" + strAirlineCode + "' ";
            //Commented By: Akhilesh Kumar
            //Date        : 1/7/2006
            //Reason      : Should be converted to int64
            //pk = Convert.ToInt32(objWK.ExecuteScaler(s))
            pk = Convert.ToInt32(objWK.ExecuteScaler(s));
            //Commenting End
            OracleTransaction TRAN = null;
            objWK.OpenConnection();
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
                var _with1 = insCommand;
                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".AIRWAY_BILL_MST_TBL_PKG.AIRWAY_BILL_MST_TBL_INS";
                var _with2 = _with1.Parameters;
                insCommand.Parameters.Add("AIRLINE_MST_FK_IN", pk).Direction = ParameterDirection.Input;
                //Commented By: Akhilesh Kumar
                //Date        : 1/7/2006
                //Reason      : Datatype converted to Varchar2
                //insCommand.Parameters.Add("START_NO_IN", OracleDbType.Int32, 11, "START_NO").Direction = ParameterDirection.Input
                //insCommand.Parameters("START_NO_IN").SourceVersion = DataRowVersion.Current
                //insCommand.Parameters.Add("END_NO_IN", OracleDbType.Int32, 11, "END_NO").Direction = ParameterDirection.Input
                //insCommand.Parameters("END_NO_IN").SourceVersion = DataRowVersion.Current
                insCommand.Parameters.Add("START_NO_IN", OracleDbType.Varchar2, 11, "START_NO").Direction = ParameterDirection.Input;
                insCommand.Parameters["START_NO_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("END_NO_IN", OracleDbType.Varchar2, 11, "END_NO").Direction = ParameterDirection.Input;
                insCommand.Parameters["END_NO_IN"].SourceVersion = DataRowVersion.Current;
                //Commenting End
                insCommand.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                //ADD BY LATHA
                insCommand.Parameters.Add("LOCATION_MST_FK_IN", LoggedIn_Loc_FK).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "AIRWAY_BILL_MST_PK").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                var _with3 = updCommand;
                _with3.Connection = objWK.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".AIRWAY_BILL_MST_TBL_PKG.AIRWAY_BILL_MST_TBL_UPD";
                var _with4 = _with3.Parameters;
                updCommand.Parameters.Add("AIRWAY_BILL_MST_PK_IN", OracleDbType.Int32, 10, "AIRWAY_BILL_MST_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["AIRWAY_BILL_MST_PK_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("AIRLINE_MST_FK_IN", pk).Direction = ParameterDirection.Input;
                //Commented By: Akhilesh Kumar
                //Date        : 1/7/2006
                //Reason      : Datatype converted to Varchar2
                //updCommand.Parameters.Add("START_NO_IN", OracleDbType.Int32, 11, "START_NO").Direction = ParameterDirection.Input
                //updCommand.Parameters("START_NO_IN").SourceVersion = DataRowVersion.Current
                //updCommand.Parameters.Add("END_NO_IN", OracleDbType.Int32, 11, "END_NO").Direction = ParameterDirection.Input
                //updCommand.Parameters("END_NO_IN").SourceVersion = DataRowVersion.Current
                updCommand.Parameters.Add("START_NO_IN", OracleDbType.Varchar2, 11, "START_NO").Direction = ParameterDirection.Input;
                updCommand.Parameters["START_NO_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("END_NO_IN", OracleDbType.Varchar2, 11, "END_NO").Direction = ParameterDirection.Input;
                updCommand.Parameters["END_NO_IN"].SourceVersion = DataRowVersion.Current;
                //Commenting End                        
                updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", LAST_MODIFIED_BY).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                var _with5 = objWK.MyDataAdapter;
                _with5.InsertCommand = insCommand;
                _with5.InsertCommand.Transaction = TRAN;
                _with5.UpdateCommand = updCommand;
                _with5.UpdateCommand.Transaction = TRAN;
                RecAfct = _with5.Update(M_DataSet);
                //TRAN.Commit()
                if (arrMessage.Count > 0)
                {
                    return arrMessage;
                }
                else
                {
                    TRAN.Commit();
                    //Moved from Line 184 to here for commit TRAN when Insert or Update is completed.Moved by Sivachandran
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

        #region " Save Transaction "

        private void SaveTrn(WorkFlow objWK, DataSet dsTrn, int Airlinepk, OracleTransaction TRAN)
        {
            int intPKVal = 0;
            long lngI = 0;
            Int32 RecAfct = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            OracleCommand delCommand = new OracleCommand();
            Int32 RecAft = default(Int32);
            try
            {
                DataTable DtTbl = new DataTable();
                DataRow DtRw = null;
                int i = 0;
                var _with6 = insCommand;
                _with6.Connection = objWK.MyConnection;
                _with6.CommandType = CommandType.StoredProcedure;
                _with6.CommandText = objWK.MyUserName + ".AIRWAY_BILL_MST_TBL_PKG.AIRWAY_BILL_TRN_INS";
                var _with7 = _with6.Parameters;
                _with7.Clear();
                insCommand.Parameters.Add("AIRWAY_BILL_MST_FK_IN", OracleDbType.Varchar2, 10, "AIRWAY_BILL_MST_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["AIRWAY_BILL_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("START_NO_IN", OracleDbType.Varchar2, 11, "START_NO").Direction = ParameterDirection.Input;
                insCommand.Parameters["START_NO_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("END_NO_IN", OracleDbType.Varchar2, 11, "END_NO").Direction = ParameterDirection.Input;
                insCommand.Parameters["END_NO_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("AIRLINE_MST_FK_IN", Airlinepk).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                var _with8 = objWK.MyDataAdapter;
                _with8.InsertCommand = insCommand;
                _with8.InsertCommand.Transaction = TRAN;
                RecAfct = _with8.Update(dsTrn);
                TRAN.Commit();
                //Manjunath  PTS ID:Sep-02  13/09/2011
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

        #region " Fetch All "
        public DataSet FetchAll(string strAirlineCode = "", string strAirlineDesc = "", string strColumnName = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, bool blnSortAscending = false, long usrLocFK = 0)
        {

            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            if (strAirlineCode.Trim().Length > 0)
            {
                //Commented by Manoharan 21May07: Airline id equal to given string
                //strCondition &= vbCrLf & " AND UPPER(AIRLINE_ID) LIKE '" & strAirlineCode.ToUpper.Replace("'", "''") & "'" & vbCrLf
                strCondition +=" AND UPPER(AIRLINE_ID) = '" + strAirlineCode.ToUpper().Replace("'", "''") + "'" ;
            }
            //Commented by Manoharan 21May07: no need for Airline Name
            //If strAirlineDesc.Trim.Length > 0 Then
            //    strCondition &= vbCrLf & " AND UPPER(AIRLINE_NAME) LIKE '" & strAirlineDesc.ToUpper.Replace("'", "''") & "'" & vbCrLf
            //End If

            strSQL = "SELECT COUNT(*) FROM AIRWAY_BILL_MST_TBL C  WHERE C.TOTAL_NOS_USED = 0 AND C.LOCATION_MST_FK = " + usrLocFK + " ";
            if (BlankGrid == 0)
            {
                strSQL +=" AND 1=2 ";
            }
            strSQL += "AND  C.AIRLINE_MST_FK IN (SELECT AIRLINE_MST_PK FROM AIRLINE_MST_TBL WHERE ";
            //strSQL &= " ACTIVE_FLAG =1 "
            //ADD BY LATHA
            strSQL += " ACTIVE_FLAG =1  ";

            strSQL +=strCondition + ")";

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
            //If TotalRecords = 1 Or TotalRecords = 0 Then
            //    CurrentPage = 0
            //    TotalPage = 0
            //Else
            TotalPage = TotalRecords / pageSize;
            if (TotalRecords % pageSize != 0)
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
            //End If

            last = CurrentPage * pageSize;
            start = (CurrentPage - 1) * pageSize + 1;
            //strSQL = "   SELECT Q1.* FROM (SELECT ROWNUM"
            //strSQL &= vbCrLf & "  SR_NO,Q.* FROM (SELECT C.AIRWAY_BILL_MST_PK, C.START_NO,"
            //strSQL &= vbCrLf & "  MOD(C.START_NO,7),"
            //strSQL &= vbCrLf & "  C.END_NO,MOD(C.END_NO,7),C.VERSION_NO FROM"
            //strSQL &= vbCrLf & "  AIRWAY_BILL_MST_TBL C  "
            //strSQL &= vbCrLf & "  WHERE  START_NO <> " & currentfrom & " AND (C.TOTAL_NOS_USED+C.TOTAL_NOS_CANCELLED) = 0  AND "
            //'ADD BY LATHA
            //strSQL &= vbCrLf & " C.LOCATION_MST_FK = " & usrLocFK & " AND "
            //strSQL &= vbCrLf & "  ((C.END_NO-C.START_NO)+1)-C.TOTAL_NOS_USED <> 0 AND C.AIRLINE_MST_FK IN "
            //strSQL &= vbCrLf & "  (SELECT AIRLINE_MST_PK FROM AIRLINE_MST_TBL "
            //strSQL &= vbCrLf & "  WHERE ACTIVE_FLAG =1"
            //strSQL &= vbCrLf & strCondition & ")"

            //Manoharan 05May07: query given by Surya for AWB no.
            strSQL = "   SELECT Q1.* FROM (SELECT ROWNUM";
            strSQL += "  SR_NO,Q.* FROM (";
            strSQL += "  SELECT C.AIRWAY_BILL_MST_PK, C.START_NO,";
            strSQL += "  MOD(C.START_NO, 7),";
            strSQL += "  C.END_NO, MOD(C.END_NO, 7), C.VERSION_NO FROM AIRWAY_BILL_MST_TBL C";
            strSQL += "  WHERE C.AIRWAY_BILL_MST_PK NOT IN";
            strSQL += "  (select a.airway_bill_mst_pk from (";
            strSQL += "  Select awb.airway_bill_mst_pk";
            strSQL += "  from airway_bill_mst_tbl awb";
            strSQL += "  where ";
            strSQL += "  awb.airline_mst_fk IN ";
            strSQL += "  (Select amt.airline_mst_pk from airline_mst_tbl amt where 1=1 " + strCondition + ")";
            strSQL += "  and awb.location_mst_fk = " + usrLocFK + " and";
            strSQL += "  (awb.end_no - awb.start_no + 1 - awb.total_nos_used + awb.total_nos_cancelled) >0";
            strSQL += "  order by  (awb.total_nos_used - awb.total_nos_cancelled) desc ) a where rownum=1)";
            strSQL += "  AND C.TOTAL_NOS_USED = 0 ";
            strSQL += "  and c.airline_mst_fk IN ";
            strSQL += "  (Select amt.airline_mst_pk from airline_mst_tbl amt where 1=1 " + strCondition + ")";
            strSQL += "  and c.location_mst_fk = " + usrLocFK;
            //end

            //strSQL &= strCondition & ")"

            if (!strColumnName.Equals("SR_NO"))
            {
                strSQL +=" order by " + strColumnName;
            }

            if (!blnSortAscending & !strColumnName.Equals("SR_NO"))
            {
                strSQL += " DESC";
            }

            strSQL += " ) Q)Q1 ";
            if (TotalPage > 1)
            {
                strSQL += " WHERE SR_NO  BETWEEN " + start + " AND " + last;
            }
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
        //Added By Rijesh On MArch 12 2006 For Capturing  the Error Message   

        #region " Fetch All "
        public DataSet FetchOldData(string strAirlineCode = "", string strAirlineDesc = "", string strColumnName = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, bool blnSortAscending = false, long usrLocFK = 0)
        {

            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            if (strAirlineCode.Trim().Length > 0)
            {
                //Commented by Manoharan 21May07: Airline id equal to given string
                //strCondition &= vbCrLf & " AND UPPER(AIRLINE_ID) LIKE '" & strAirlineCode.ToUpper.Replace("'", "''") & "'" & vbCrLf
                strCondition +=" AND UPPER(AIRLINE_ID) = '" + strAirlineCode.ToUpper().Replace("'", "''") + "'" ;
            }
            //Commented by Manoharan 21May07: no need for Airline Name
            //If strAirlineDesc.Trim.Length > 0 Then
            //    strCondition &= vbCrLf & " AND UPPER(AIRLINE_NAME) LIKE '" & strAirlineDesc.ToUpper.Replace("'", "''") & "'" & vbCrLf
            //End If

            strSQL = " SELECT COUNT(*) FROM ";
            strSQL +=" AIRWAY_BILL_MST_TBL AIR ";
            strSQL +=" WHERE 1=1 AND ";
            //ADD BY LATHA
            strSQL += " AIR.LOCATION_MST_FK = " + LoggedIn_Loc_FK + " AND ";
            strSQL +=" ((AIR.END_NO-AIR.START_NO)+1)-AIR.TOTAL_NOS_USED = 0  AND ";
            strSQL +="AIR.AIRLINE_MST_FK IN ";
            strSQL +="(SELECT AIRLINE_MST_PK FROM ";
            strSQL +="AIRLINE_MST_TBL WHERE  1=1 AND  ";
            //strSQL &= vbCrLf & " AIRLINE_MST_TBL WHERE "
            strSQL +="  ACTIVE_FLAG =1 ";
            strSQL +=strCondition + ")";


            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));

            TotalPage = TotalRecords / pageSize;

            if (TotalRecords % pageSize != 0)
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

            last = CurrentPage * pageSize;
            start = (CurrentPage - 1) * pageSize + 1;

            //strSQL = "  SELECT B.* FROM (SELECT ROWNUM SR_NO,"
            //strSQL &= vbCrLf & "  A.* FROM ( SELECT AIR.START_NO,MOD(AIR.START_NO,7),"
            //strSQL &= vbCrLf & "  AIR.END_NO,MOD(AIR.END_NO,7),AIR.TOTAL_NOS_USED,"
            //strSQL &= vbCrLf & "  ((AIR.END_NO-AIR.START_NO)+1)-AIR.TOTAL_NOS_USED REMAINING ,"
            //strSQL &= vbCrLf & "  AIR.END_NO LAST_USED, "
            //strSQL &= vbCrLf & "  AIR.LAST_USED_DATE FROM  "
            //strSQL &= vbCrLf & "  AIRWAY_BILL_MST_TBL AIR WHERE  1=1 AND  "
            //'ADD BY LATHA
            //strSQL &= " AIR.LOCATION_MST_FK = " & LoggedIn_Loc_FK & " AND "
            //strSQL &= vbCrLf & "  ((AIR.END_NO-AIR.START_NO)+1)-(AIR.TOTAL_NOS_USED+AIR.TOTAL_NOS_CANCELLED)  = 0 AND "
            //strSQL &= vbCrLf & " AIR.AIRLINE_MST_FK IN  "
            //strSQL &= vbCrLf & " (SELECT AIRLINE_MST_PK FROM  "
            //strSQL &= vbCrLf & " AIRLINE_MST_TBL WHERE "
            //strSQL &= vbCrLf & " ACTIVE_FLAG =1"

            //Manoharan 05May07: query given by Surya for AWB no.
            strSQL = " SELECT B.* FROM (SELECT ROWNUM SR_NO, ";
            strSQL += " A.* FROM (SELECT AIR.START_NO, MOD(AIR.START_NO, 7),";
            strSQL += " AIR.END_NO, MOD(AIR.END_NO, 7), AIR.TOTAL_NOS_USED,";
            strSQL += " ((AIR.END_NO - AIR.START_NO) + 1) - AIR.TOTAL_NOS_USED REMAINING,";
            strSQL += " AIR.END_NO LAST_USED, AIR.LAST_USED_DATE";
            strSQL += " FROM AIRWAY_BILL_MST_TBL AIR WHERE 1 = 1";
            strSQL += " AND AIR.LOCATION_MST_FK = " + LoggedIn_Loc_FK;
            //strSQL &= " AND ((AIR.END_NO - AIR.START_NO) + 1) - (AIR.TOTAL_NOS_USED + AIR.TOTAL_NOS_CANCELLED) = 0"
            //strSQL &= " AND ((AIR.END_NO - AIR.START_NO) + 1) - (AIR.TOTAL_NOS_USED) = 0"
            strSQL += " AND AIR.TOTAL_NOS_USED >0";
            strSQL += " AND AIR.AIRLINE_MST_FK IN";
            strSQL += " (SELECT AIRLINE_MST_PK FROM AIRLINE_MST_TBL WHERE ACTIVE_FLAG = 1";
            //end 
            strSQL +=strCondition + ")";

            if (!strColumnName.Equals("SR_NO"))
            {
                strSQL +="order by " + strColumnName;
            }

            if (!blnSortAscending & !strColumnName.Equals("SR_NO"))
            {
                strSQL += " DESC";
            }

            strSQL += ") A )B ";
            if (TotalPage > 1)
            {
                strSQL += " WHERE SR_NO  BETWEEN " + start + " AND " + last;
            }

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

        #region " Fetch Available Bills "

        public DataSet FetchAvailableBills(int Loc, int AirlineFK)
        {

            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            //strSQL = " select awt.airway_bill_no,decode(awt.status,2,'Cancelled',1,'Used'), awt.reference_no "
            //strSQL = strSQL & "  from airway_bill_mst_tbl awb, airway_bill_trn awt "
            //strSQL = strSQL & "  where(awt.status = 2 Or awt.status = 0 Or awt.status Is null) "
            //strSQL = strSQL & "  and (awb.total_nos_used + awb.total_nos_cancelled)<=((awb.end_no - awb.start_no)+1)  "
            //strSQL = strSQL & "  and (awb.total_nos_used >=0) "
            //strSQL = strSQL & "  and awt.airway_bill_mst_fk in (select max(a.airway_bill_mst_pk) from airway_bill_mst_tbl a "
            //strSQL = strSQL & "  where  a.start_no = " & currentfrom '& " and ((a.end_no - a.start_no)+1) > a.total_nos_used "
            //strSQL = strSQL & "  and a.airline_mst_fk = " & AirlineFK & " ) "
            //strSQL = strSQL & "  and awb.airline_mst_fk = " & AirlineFK & ""
            //'add by latha
            //strSQL = strSQL & "  and awb.location_mst_fk = " & Loc & ""
            //strSQL = strSQL & "  and awt.airway_bill_mst_fk = awb.airway_bill_mst_pk "
            //strSQL = strSQL & " Order by awt.status Asc"

            //Manoharan 05May07: query given by Surya for AWB no.
            //strSQL = " select awt.airway_bill_no, decode(awt.status, 2, 'Cancelled', 1, 'Used'), awt.reference_no"
            strSQL = " select awt.airway_bill_no, decode(awt.status, 0, 'Cancelled'), awt.reference_no";
            //0-cancelled 3-used
            strSQL = strSQL + " from airway_bill_mst_tbl awb, airway_bill_trn awt ";
            strSQL = strSQL + " where awb.airway_bill_mst_pk = awt.airway_bill_mst_fk and ";
            strSQL = strSQL + " (awt.status = 2 Or awt.status = 0 Or awt.status Is null) and ";
            strSQL = strSQL + " (awb.total_nos_used + awb.total_nos_cancelled) <= ((awb.end_no - awb.start_no) + 1)  and ";
            strSQL = strSQL + " (awb.total_nos_used >= 0)    and ";
            strSQL = strSQL + " awb.airway_bill_mst_pk IN ";
            strSQL = strSQL + " (select a.airway_bill_mst_pk from (";
            strSQL = strSQL + " Select awb.airway_bill_mst_pk";
            strSQL = strSQL + " from airway_bill_mst_tbl awb";
            strSQL = strSQL + " where";
            strSQL = strSQL + " awb.airline_mst_fk = " + AirlineFK;
            strSQL = strSQL + " and awb.location_mst_fk = " + Loc;
            strSQL = strSQL + " and (awb.end_no - awb.start_no + 1 - awb.total_nos_used + awb.total_nos_cancelled) >0";
            strSQL = strSQL + " order by  (awb.total_nos_used - awb.total_nos_cancelled) desc ) a where rownum=1)";
            strSQL = strSQL + " Order by awt.status Asc";
            //end

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

        #region " Enhance Search Function for MAWB Int32 "
        // 16-Mar-2006 Rajesh [ Last Updated 31-Mar-2006 ]
        public string FetchMawbNr(string StrCond, int Loc)
        {
            string lookupVal = "";
            string strSearchIn = "";
            string strAirline = "";
            Array arr = null;
            arr = StrCond.Split('~');
            if (arr.Length > 0)
                lookupVal = Convert.ToString(arr.GetValue(0));
            if (arr.Length > 1)
                strSearchIn = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strAirline = Convert.ToString(arr.GetValue(2));
            WorkFlow objWK = new WorkFlow();
            try
            {
                objWK.OpenConnection();
                objWK.MyCommand.Parameters.Clear();
                var _with9 = objWK.MyCommand;
                _with9.CommandType = CommandType.StoredProcedure;
                _with9.CommandText = objWK.MyUserName + ".EN_MAWB_PKG.GET_MAWB_NO";

                var _with10 = _with9.Parameters;
                _with10.Add("SEARCH_IN", getDefault(strSearchIn, DBNull.Value)).Direction = ParameterDirection.Input;
                _with10.Add("LOOKUP_VALUE_IN", lookupVal).Direction = ParameterDirection.Input;
                //ADD BY LATHA
                _with10.Add("LOCATION_IN", Loc).Direction = ParameterDirection.Input;
                _with10.Add("AIRLINE_FK_IN", getDefault(strAirline, DBNull.Value)).Direction = ParameterDirection.Input;
                _with10.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with9.ExecuteNonQuery();
                string strReturn = null;
                strReturn = Convert.ToString(_with9.Parameters["RETURN_VALUE"].Value);
                return strReturn;
                //Manjunath  PTS ID:Sep-02  13/09/2011
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

        #region " Update & Cancel-Update Function for Airway Bill Master "
        // 17-Mar-2006 [ Rajesh ]
        // These are Shared Methods; So it is not not necessary to make instance of the class to Call these methods.
        //==========================================================================================================
        // This procedure will Update Airway bill Master 
        // Status [ 1: Used, 2:Cancel ] 
        // UsedAt [ 1: SpotRate ]
        // ReferenceNo   : This parameter is the reference no of other sources like SpotRate
        // AirwayBillNo  : This parameter is AirwayBill to update
        // UsedAt        : This refers the source, basically it will be a number; for SpotRate it is 1
        // this proc will change the status of AirwayBill to Used.
        //==========================================================================================================
        // There are two functions for same thing in overloaded form
        // the first can work under a transaction where as the second is independent command. 
        // With Transaction Support ( Safe )
        public static bool UpdateMAWB(string ReferenceNo, string AirwayBillNo, string UsedAt, WorkFlow ObjWk)
        {
            //Dim strSQL As String
            //strSQL = " Update AIRWAY_BILL_TRN ATN                   " & vbCrLf & _
            //        "   set REFERENCE_NO = '" & ReferenceNo & "',   " & vbCrLf & _
            //        "       STATUS       = 1,                       " & vbCrLf & _
            //        "       USED_AT      = " & UsedAt & "           " & vbCrLf & _
            //        "   where                                       " & vbCrLf & _
            //        "       AIRWAY_BILL_NO = '" & AirwayBillNo & "' " & vbCrLf & _
            //        "   AND nvl(STATUS,-1)  <> 1                    "

            //strSQL = strSQL.Replace("   ", " ")
            //strSQL = strSQL.Replace("  ", " ")

            //Try
            //    ObjWk.MyCommand.CommandType = CommandType.Text
            //    ObjWk.MyCommand.CommandText = strSQL
            //    ObjWk.MyCommand.Parameters.Clear()
            //    If ObjWk.MyCommand.ExecuteNonQuery() > 0 Then
            //        strSQL = " Update AIRWAY_BILL_MST_TBL am " & _
            //                 "     Set am.TOTAL_NOS_USED =  nvl(TOTAL_NOS_USED,0) + 1  " & _
            //                 " where " & _
            //                 "     am.AIRWAY_BILL_MST_PK =  " & _
            //                 "      ( Select AIRWAY_BILL_MST_FK from AIRWAY_BILL_TRN " & _
            //                 "        where AIRWAY_BILL_NO = '" & AirwayBillNo & "')"

            //        ObjWk.MyCommand.CommandText = strSQL
            //        ObjWk.MyCommand.Parameters.Clear()
            //        If ObjWk.MyCommand.ExecuteNonQuery() > 0 Then
            //            Return True
            //        End If
            //        Return False
            //    Else
            //        Return False
            //    End If
            try
            {
                ObjWk.MyCommand.CommandType = CommandType.StoredProcedure;
                ObjWk.MyCommand.CommandText = ObjWk.MyUserName + ".AIRWAY_BILL_MST_TBL_PKG.AIRWAY_BILL_RESERVE";
                ObjWk.MyCommand.Parameters.Clear();

                var _with11 = ObjWk.MyCommand.Parameters;

                _with11.Add("REFERENCE_NO_IN", ReferenceNo).Direction = ParameterDirection.Input;
                _with11.Add("USED_AT_IN", UsedAt).Direction = ParameterDirection.Input;
                _with11.Add("RESERVE_IN", 1).Direction = ParameterDirection.Input;
                _with11.Add("AIRWAY_BILL_NO_IN", AirwayBillNo).Direction = ParameterDirection.Input;
                _with11.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;

                if (ObjWk.MyCommand.ExecuteNonQuery() > 0)
                {
                    return true;
                }
                else
                {
                    return false;
                }
                //Manjunath  PTS ID:Sep-02  13/09/2011
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (System.Exception eX)
            {
                return false;
                throw eX;
            }

        }
        // No Transaction Support ( Unsafe )
        public static bool UpdateMAWB(string ReferenceNo, string AirwayBillNo, string UsedAt)
        {
            //Dim strSQL As String

            //strSQL = " Update AIRWAY_BILL_TRN ATN                   " & vbCrLf & _
            //        "   set REFERENCE_NO = '" & ReferenceNo & "',   " & vbCrLf & _
            //        "       STATUS       = 1,                       " & vbCrLf & _
            //        "       USED_AT      = " & UsedAt & "           " & vbCrLf & _
            //        "   where                                       " & vbCrLf & _
            //        "       AIRWAY_BILL_NO = '" & AirwayBillNo & "' " & vbCrLf & _
            //        "   AND nvl(STATUS,-1) <> 1                     "

            //strSQL = strSQL.Replace("   ", " ")
            //strSQL = strSQL.Replace("  ", " ")

            try
            {
                //    If (New WorkFlow).ExecuteCommands(strSQL) = True Then

                //        strSQL = " Update AIRWAY_BILL_MST_TBL am " & _
                //                 "     Set am.TOTAL_NOS_USED =  nvl(TOTAL_NOS_USED,0) + 1  " & _
                //                 " where " & _
                //                 "     am.AIRWAY_BILL_MST_PK =  " & _
                //                 "      ( Select AIRWAY_BILL_MST_FK from AIRWAY_BILL_TRN " & _
                //                 "        where AIRWAY_BILL_NO = '" & AirwayBillNo & "')"

                //        Return (New WorkFlow).ExecuteCommands(strSQL)

                //    Else
                //        Return False
                //    End If
                WorkFlow ObjWk = new WorkFlow();
                ObjWk.MyCommand.CommandType = CommandType.StoredProcedure;
                ObjWk.MyCommand.CommandText = ObjWk.MyUserName + ".AIRWAY_BILL_MST_TBL_PKG.AIRWAY_BILL_RESERVE";
                ObjWk.MyCommand.Parameters.Clear();

                var _with12 = ObjWk.MyCommand.Parameters;

                _with12.Add("REFERENCE_NO_IN", ReferenceNo).Direction = ParameterDirection.Input;
                _with12.Add("USED_AT_IN", UsedAt).Direction = ParameterDirection.Input;
                _with12.Add("RESERVE_IN", 1).Direction = ParameterDirection.Input;
                _with12.Add("AIRWAY_BILL_NO_IN", AirwayBillNo).Direction = ParameterDirection.Input;
                _with12.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;

                if (ObjWk.MyCommand.ExecuteNonQuery() > 0)
                {
                    return true;
                }
                else
                {
                    return false;
                }
                //Manjunath  PTS ID:Sep-02  13/09/2011
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception eX)
            {
                return false;
                throw eX;
            }

        }
        //
        // This procedure will Cancel the previous Update. It is reverse process to that of UpdateMAWB
        // It will go to a particular record in Airway Bill Master according to given referenceNo and AirwayBillNo
        // it will change the status to Cancelled and also set the reference no and used at to NULL
        // With Transaction Support ( Safe )
        public static bool CancelUpdateMAWB(string ReferenceNo, string AirwayBillNo, WorkFlow ObjWk)
        {
            //Dim strSQL As String
            //strSQL = " Update AIRWAY_BILL_TRN ATN                   " & vbCrLf & _
            //        "   set REFERENCE_NO = NULL,                    " & vbCrLf & _
            //        "       STATUS       = 2,                       " & vbCrLf & _
            //        "       USED_AT      = NULL                     " & vbCrLf & _
            //        "   where                                       " & vbCrLf & _
            //        "       AIRWAY_BILL_NO = '" & AirwayBillNo & "' " & vbCrLf & _
            //        "   AND STATUS         = 1                      " & vbCrLf & _
            //        "   AND REFERENCE_NO   = '" & ReferenceNo & "'  "

            //strSQL = strSQL.Replace("   ", " ")
            //strSQL = strSQL.Replace("  ", " ")


            try
            {
                ObjWk.MyCommand.CommandType = CommandType.StoredProcedure;
                ObjWk.MyCommand.CommandText = ObjWk.MyUserName + ".AIRWAY_BILL_MST_TBL_PKG.AIRWAY_BILL_RESERVE";
                ObjWk.MyCommand.Parameters.Clear();
                //If ObjWk.MyCommand.ExecuteNonQuery() > 0 Then

                //    strSQL = " Update AIRWAY_BILL_MST_TBL am " & _
                //             "     Set am.TOTAL_NOS_USED =  nvl(TOTAL_NOS_USED,1) -1 " & _
                //             " where " & _
                //             "     am.AIRWAY_BILL_MST_PK =  " & _
                //             "      ( Select AIRWAY_BILL_MST_FK from AIRWAY_BILL_TRN " & _
                //             "        where AIRWAY_BILL_NO = '" & AirwayBillNo & "'" & _
                //             "          and REFERENCE_NO   = '" & ReferenceNo & "')"

                //    ObjWk.MyCommand.CommandText = strSQL
                //    ObjWk.MyCommand.Parameters.Clear()
                //()
                //()
                //()
                //()
                //RETURN_VALUE()
                var _with13 = ObjWk.MyCommand.Parameters;

                _with13.Add("REFERENCE_NO_IN", ReferenceNo).Direction = ParameterDirection.Input;
                _with13.Add("USED_AT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                _with13.Add("RESERVE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                _with13.Add("AIRWAY_BILL_NO_IN", AirwayBillNo).Direction = ParameterDirection.Input;
                _with13.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;

                if (ObjWk.MyCommand.ExecuteNonQuery() > 0)
                {
                    return true;
                }
                else
                {
                    return false;
                }
                //Else
                //    Return False
                //End If
                //Manjunath  PTS ID:Sep-02  13/09/2011
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (System.Exception eX)
            {
                return false;
                throw eX;
            }

        }
        // No Transaction Support { Unsafe )
        public static bool CancelUpdateMAWB(string ReferenceNo, string AirwayBillNo)
        {
            //Dim strSQL As String
            //strSQL = " Update AIRWAY_BILL_TRN ATN                   " & vbCrLf & _
            //        "   set REFERENCE_NO = NULL,                    " & vbCrLf & _
            //        "       STATUS       = 2,                       " & vbCrLf & _
            //        "       USED_AT      = NULL                     " & vbCrLf & _
            //        "   where                                       " & vbCrLf & _
            //        "       AIRWAY_BILL_NO = '" & AirwayBillNo & "' " & vbCrLf & _
            //        "   AND STATUS         = 1                      " & vbCrLf & _
            //        "   AND REFERENCE_NO   = '" & ReferenceNo & "'  "

            //strSQL = strSQL.Replace("   ", " ")
            //strSQL = strSQL.Replace("  ", " ")
            //Try
            //    If (New WorkFlow).ExecuteCommands(strSQL) = True Then
            //        strSQL = " Update AIRWAY_BILL_MST_TBL am " & _
            //                 "     Set am.TOTAL_NOS_USED =  nvl(TOTAL_NOS_USED,1) - 1 " & _
            //                 " where " & _
            //                 "     am.AIRWAY_BILL_MST_PK =  " & _
            //                 "      ( Select AIRWAY_BILL_MST_FK from AIRWAY_BILL_TRN " & _
            //                 "        where AIRWAY_BILL_NO = '" & AirwayBillNo & "'" & _
            //                 "          and REFERENCE_NO   = '" & ReferenceNo & "')"
            //        Return (New WorkFlow).ExecuteCommands(strSQL)
            //    End If
            try
            {
                WorkFlow ObjWk = new WorkFlow();
                ObjWk.MyCommand.CommandType = CommandType.StoredProcedure;
                ObjWk.MyCommand.CommandText = ObjWk.MyUserName + ".AIRWAY_BILL_MST_TBL_PKG.AIRWAY_BILL_RESERVE";
                ObjWk.MyCommand.Parameters.Clear();
                var _with14 = ObjWk.MyCommand.Parameters;

                _with14.Add("REFERENCE_NO_IN", ReferenceNo).Direction = ParameterDirection.Input;
                _with14.Add("USED_AT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                _with14.Add("RESERVE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                _with14.Add("AIRWAY_BILL_NO_IN", AirwayBillNo).Direction = ParameterDirection.Input;
                _with14.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;

                if (ObjWk.MyCommand.ExecuteNonQuery() > 0)
                {
                    return true;
                }
                else
                {
                    return false;
                }
                //Manjunath  PTS ID:Sep-02  13/09/2011
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception eX)
            {
                return false;
                throw eX;
            }

        }

        #endregion

    }
}