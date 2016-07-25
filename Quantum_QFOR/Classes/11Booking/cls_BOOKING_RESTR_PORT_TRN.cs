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
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_BOOKING_RESTR_PORT_TRN : CommonFeatures
    {
        #region "Fetch Function"
        public DataSet FetchAllRestrPort(Int64 P_Booking_Restriction_Port_Pk = 0, Int64 P_Booking_Restriction_Fk = 0, Int64 P_Pol_Fk = 0, Int64 P_Pod_Fk = 0, string SearchType = "", string SortExpression = "", bool flag = false)
        {
            string strSQL = null;
            //strSQL = "SELECT ROWNUM SR_NO, "
            //strSQL = strSQL & " Booking_Restriction_Port_Pk,"
            //strSQL = strSQL & " Booking_Restriction_Fk,"
            //strSQL = strSQL & " Pol_Fk,"
            //strSQL = strSQL & " Pod_Fk,"
            //strSQL = strSQL & " Created_By_Fk,"
            //strSQL = strSQL & " Created_Dt,"
            //strSQL = strSQL & " Last_Modified_By_Fk,"
            //strSQL = strSQL & " Last_Modified_Dt,"
            //strSQL = strSQL & " Version_No "
            //strSQL = strSQL & " FROM BOOKING_RESTR_PORT_TRN "
            //strSQL = strSQL & " WHERE ( 1 = 1) "

            strSQL = "";
            strSQL += "SELECT ROWNUM SR_NO,  " ;
            strSQL += "              1 active, " ;
            strSQL += "             b.Pol_Fk POL_FK, " ;
            strSQL += "             pol.port_id POL_ID, " ;
            strSQL += "             pol.port_name POL_Name, " ;
            strSQL += "             b.Pod_Fk POD_FK, " ;
            strSQL += "             pod.port_id POD_ID, " ;
            strSQL += "             pod.port_name POD_Name, " ;
            strSQL += "              b.Booking_Restriction_Port_Pk Booking_Restriction_Port_Pk, " ;
            strSQL += "             b.Booking_Restriction_Fk Booking_Restriction_Fk, " ;
            strSQL += "             b.Created_By_Fk Created_By_Fk, " ;
            strSQL += "             b.Created_Dt Created_Dt, " ;
            strSQL += "             b.Last_Modified_By_Fk Last_Modified_By_Fk, " ;
            strSQL += "             b.Last_Modified_Dt Last_Modified_Dt, " ;
            strSQL += "             b.Version_No Version_No " ;
            strSQL += "              " ;
            strSQL += "             FROM BOOKING_RESTR_PORT_TRN b, " ;
            strSQL += "                  Port_Mst_Tbl POL, " ;
            strSQL += "                  Port_Mst_Tbl POD                   " ;
            strSQL += "             WHERE ( 1 = 1)  " ;
            strSQL += "                   AND b.pol_fk = pol.port_mst_pk " ;
            strSQL += "                   AND b.pod_fk = pod.port_mst_pk " ;

            if (flag == false)
            {
                strSQL = strSQL + " And Booking_Restriction_Port_Pk =0";
            }
            else
            {
                if (P_Booking_Restriction_Port_Pk != 0)
                {
                    if (SearchType == "C")
                    {
                        strSQL = strSQL + " And Booking_Restriction_Port_Pk =" + P_Booking_Restriction_Port_Pk;
                    }
                    else
                    {
                        strSQL = strSQL + " And Booking_Restriction_Port_Pk =" + P_Booking_Restriction_Port_Pk;
                    }
                }
                else
                {
                }
            }

            if (P_Booking_Restriction_Fk != 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Booking_Restriction_Fk =" + P_Booking_Restriction_Fk;
                }
                else
                {
                    strSQL = strSQL + " And Booking_Restriction_Fk =" + P_Booking_Restriction_Fk;
                }
            }
            else
            {
            }
            if (P_Pol_Fk != 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Pol_Fk =" + P_Pol_Fk;
                }
                else
                {
                    strSQL = strSQL + " And Pol_Fk =" + P_Pol_Fk;
                }
            }
            else
            {
            }
            if (P_Pod_Fk != 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Pod_Fk =" + P_Pod_Fk;
                }
                else
                {
                    strSQL = strSQL + " And Pod_Fk =" + P_Pod_Fk;
                }
            }
            else
            {
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

        public DataSet FetchRstrPort(Int64 P_Booking_Restriction_Port_Pk = 0, Int64 P_Booking_Restriction_Fk = 0, Int64 P_Pol_Fk = 0, Int64 P_Pod_Fk = 0, string SearchType = "", string SortExpression = "")
        {
            string strSQL = null;

            strSQL = string.Empty ;
            strSQL += " " ;
            strSQL += "SELECT  ROWNUM SR_NO, qry.* FROM( ( " ;
            strSQL += "SELECT brp.booking_restriction_port_pk active, " ;
            strSQL += "       s.from_port_fk Pol_Fk, " ;
            strSQL += "       POL.PORT_ID POL_ID, " ;
            strSQL += "       pol.port_name POL_Name, " ;
            strSQL += "       s.To_Port_Fk Pod_Fk, " ;
            strSQL += "       pod.port_id POD_ID, " ;
            strSQL += "       pod.port_name POD_name, " ;
            strSQL += "       brp.version_no, " ;
            strSQL += "       brp.booking_restriction_port_pk, " ;
            strSQL += "       brp.booking_restriction_fk, " ;
            strSQL += "       brp.created_by_fk, " ;
            strSQL += "       brp.created_dt, " ;
            strSQL += "       brp.last_modified_by_fk, " ;
            strSQL += "       brp.Last_Modified_Dt,s.tli_ref_no " ;
            strSQL += "FROM Booking_Restr_Port_Trn BRP, " ;
            strSQL += "     Sector_Mst_Tbl S, " ;
            strSQL += "     Port_Mst_Tbl POL, " ;
            strSQL += "     Port_Mst_Tbl POD " ;
            strSQL += "WHERE brp.pol_fk = pol.port_mst_pk " ;
            strSQL += "      AND brp.pod_fk = pod.port_mst_pk " ;
            strSQL += "      AND s.from_port_fk = pol.port_mst_pk " ;
            strSQL += "      AND s.to_port_fk = pod.port_mst_pk " ;
            strSQL += "      AND brp.booking_restriction_fk= " + P_Booking_Restriction_Fk ;
            strSQL += "UNION ALL " ;
            strSQL += "SELECT 0 active, " ;
            strSQL += "       s.from_port_fk Pol_Fk, " ;
            strSQL += "       POL.PORT_ID POL_ID, " ;
            strSQL += "       pol.port_name POL_Name, " ;
            strSQL += "       s.To_Port_Fk Pod_Fk, " ;
            strSQL += "       pod.port_id POD_ID, " ;
            strSQL += "       pod.port_name POD_name, " ;
            strSQL += "       0 version_no, " ;
            strSQL += "       0 booking_restriction_port_pk, " ;
            strSQL += "       0 booking_restriction_fk, " ;
            strSQL += "       0 created_by_fk, " ;
            strSQL += "       to_date(null,'dd-Mon-yyyy') created_dt, " ;
            strSQL += "       0 last_modified_by_fk, " ;
            strSQL += "      to_date(null,'dd-Mon-yyyy')  Last_Modified_Dt, " ;
            strSQL += "      s.tli_ref_no " ;
            strSQL += "FROM Sector_Mst_Tbl S, " ;
            strSQL += "     Port_Mst_Tbl POL, " ;
            strSQL += "     Port_Mst_Tbl POD " ;
            strSQL += "WHERE s.from_port_fk = pol.port_mst_pk " ;
            strSQL += "      AND s.to_port_fk = pod.port_mst_pk " ;
            strSQL += "      AND  (s.from_port_fk,s.to_port_fk) <> " ;
            strSQL += "            (SELECT  brp.pol_fk,brp.pod_fk  " ;
            strSQL += "               FROM Booking_Restr_Port_Trn BRP " ;
            strSQL += "               WHERE brp.booking_restriction_fk=" + P_Booking_Restriction_Fk + " ) " ;
            strSQL += "          " ;

            strSQL += " " ;
            if (P_Booking_Restriction_Port_Pk != 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And brp.Booking_Restriction_Port_Pk =" + P_Booking_Restriction_Port_Pk;
                }
                else
                {
                    strSQL = strSQL + " And brp.Booking_Restriction_Port_Pk =" + P_Booking_Restriction_Port_Pk;
                }
            }
            else
            {
            }
           
            if (P_Pol_Fk != 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Pol_Fk =" + P_Pol_Fk;
                }
                else
                {
                    strSQL = strSQL + " And Pol_Fk =" + P_Pol_Fk;
                }
            }
            else
            {
            }
            if (P_Pod_Fk != 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Pod_Fk =" + P_Pod_Fk;
                }
                else
                {
                    strSQL = strSQL + " And Pod_Fk =" + P_Pod_Fk;
                }
            }
            else
            {
            }
            strSQL += ")  ORDER BY tli_ref_no)qry " ;
            strSQL += "      " ;
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

        #region "Save Function"
        private bool DeleteRecords(int BR_FK)
        {
            string sqlStr = null;
            string DelSql = null;
            DataSet ds = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                sqlStr = " Select * from BOOKING_RESTR_PORT_TRN where BOOKING_RESTRICTION_FK = " + BR_FK;
                ds = objWF.GetDataSet(sqlStr);
                if (ds.Tables[0].Rows.Count > 0)
                {
                    DelSql = "Delete from BOOKING_RESTR_PORT_TRN where BOOKING_RESTRICTION_FK = " + BR_FK;
                    WorkFlow objWK = new WorkFlow();
                    try
                    {
                        objWK.OpenConnection();
                        if (objWK.ExecuteCommands(DelSql) == true)
                        {
                            return true;
                        }
                        else
                        {
                            return false;
                        }

                    }
                    catch (Exception ex)
                    {
                        throw ex;
                    }

                }
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
            return false;
        }
        public ArrayList Save(DataSet M_DataSet, OracleTransaction TRAN, Int32 BkPK, string RestNO)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.MyConnection = TRAN.Connection;
            //Dim TRAN As OracleTransaction
            //TRAN = objWK.MyConnection.BeginTransaction()
            
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

                //DeleteRecords(BkPK)



                var _with1 = insCommand;
                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".BOOKING_RESTR_PORT_TRN_PKG.BOOKING_RESTR_PORT_TRN_INS";
                var _with2 = _with1.Parameters;
                insCommand.Parameters.Add("BOOKING_RESTRICTION_FK_IN", BkPK).Direction = ParameterDirection.Input;
                //insCommand.Parameters.Add("BOOKING_RESTRICTION_FK_IN", OracleDbType.Int32, 10, "BOOKING_RESTRICTION_FK").Direction = ParameterDirection.Input
                //insCommand.Parameters("BOOKING_RESTRICTION_FK_IN").SourceVersion = DataRowVersion.Current
                insCommand.Parameters.Add("POL_FK_IN", OracleDbType.Int32, 10, "POL_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["POL_FK_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("POD_FK_IN", OracleDbType.Int32, 10, "POD_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["POD_FK_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                //insCommand.Parameters.Add("CONFIG_MST_FK_IN", OracleDbType.NUMBER, 10, "").Direction = ParameterDirection.INPUT
                //insCommand.Parameters("CONFIG_MST_FK_IN").SourceVersion = DataRowVersion.Current
                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "BOOKING_RESTR_PORT_TRN_PK").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


                var _with3 = delCommand;
                _with3.Connection = objWK.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".BOOKING_RESTR_PORT_TRN_PKG.BOOKING_RESTR_PORT_TRN_DEL";
                var _with4 = _with3.Parameters;
                delCommand.Parameters.Add("BOOKING_RESTRICTION_PORT_PK_IN", OracleDbType.Int32, 10, "BOOKING_RESTRICTION_PORT_PK").Direction = ParameterDirection.Input;
                delCommand.Parameters["BOOKING_RESTRICTION_PORT_PK_IN"].SourceVersion = DataRowVersion.Current;
                delCommand.Parameters.Add("DELETED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                delCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                delCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
                delCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                delCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                delCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


                var _with5 = updCommand;
                _with5.Connection = objWK.MyConnection;
                _with5.CommandType = CommandType.StoredProcedure;
                _with5.CommandText = objWK.MyUserName + ".BOOKING_RESTR_PORT_TRN_PKG.BOOKING_RESTR_PORT_TRN_UPD";
                var _with6 = _with5.Parameters;

                updCommand.Parameters.Add("BOOKING_RESTRICTION_PORT_PK_IN", OracleDbType.Int32, 10, "BOOKING_RESTRICTION_PORT_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["BOOKING_RESTRICTION_PORT_PK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("BOOKING_RESTRICTION_FK_IN", BkPK).Direction = ParameterDirection.Input;
                //updCommand.Parameters.Add("BOOKING_RESTRICTION_FK_IN", OracleDbType.Int32, 10, "BOOKING_RESTRICTION_FK").Direction = ParameterDirection.Input
                //updCommand.Parameters("BOOKING_RESTRICTION_FK_IN").SourceVersion = DataRowVersion.Current
                updCommand.Parameters.Add("POL_FK_IN", OracleDbType.Int32, 10, "POL_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["POL_FK_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("POD_FK_IN", OracleDbType.Int32, 10, "POD_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["POD_FK_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                //updCommand.Parameters.Add("CONFIG_MST_FK_IN", OracleDbType.NUMBER, 10, "").Direction = ParameterDirection.INPUT
                //updCommand.Parameters("CONFIG_MST_FK_IN").SourceVersion = DataRowVersion.Current
                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                
                //objWK.MyDataAdapter.ContinueUpdateOnError = True
                var _with7 = objWK.MyDataAdapter;

                _with7.InsertCommand = insCommand;
                _with7.InsertCommand.Transaction = TRAN;
                _with7.UpdateCommand = updCommand;
                _with7.UpdateCommand.Transaction = TRAN;
                _with7.DeleteCommand = delCommand;
                _with7.DeleteCommand.Transaction = TRAN;
                RecAfct = _with7.Update(M_DataSet);
                //TRAN.Commit()
                if (arrMessage.Count > 0)
                {
                    return arrMessage;
                }
                else
                {
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
        }
        #endregion
        #region "Each Row Updation"
        //Sub OnRowUpdated(ByVal objsender As Object, ByVal e As OracleRowUpdatedEventArgs)
        //    Try
        //        If e.RecordsAffected < 1 Then
        //            If e.Errors.Message <> "" Then
        //                arrMessage.Add(CType(e.Row.Item(2), String) & "~" & e.Errors.Message)
        //            Else
        //                arrMessage.Add(CType(e.Command.Parameters(0).Value, String) & "~" & e.Errors.Message)
        //            End If
        //            e.Status = UpdateStatus.SkipCurrentRow
        //        End If
        //    Catch ex As Exception
        //        Throw ex
        //    End Try
        //End Sub
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
                for (i = 0; i <= DeletedRow.Count - 1; i++)
                {
                    oraTran = obJWk.MyConnection.BeginTransaction();
                    var _with8 = obJWk.MyCommand;
                    _with8.Transaction = oraTran;
                    _with8.Connection = obJWk.MyConnection;
                    _with8.CommandType = CommandType.StoredProcedure;
                    _with8.CommandText = obJWk.MyUserName + ".BOOKING_RESTR_PORT_TRN_PKG.BOOKING_RESTR_PORT_TRN_DEL";
                    arrRowDetail = DeletedRow[i].ToString().Split(',');
                    _with8.Parameters.Clear();
                    var _with9 = _with8.Parameters;
                    _with9.Add("BOOKING_RESTRICTION_PORT_PK_IN", DeletedRow[0]).Direction = ParameterDirection.Input;
                    _with9.Add("VERSION_NO_IN", DeletedRow[1]).Direction = ParameterDirection.Input;
                    _with9.Add("RETURN_VALUE", strReturn).Direction = ParameterDirection.Output;
                    obJWk.MyCommand.Parameters["RETURN_VALUE"].OracleDbType = OracleDbType.Varchar2;
                    obJWk.MyCommand.Parameters["RETURN_VALUE"].Size = 50;
                    try
                    {
                        if (_with8.ExecuteNonQuery() > 0)
                        {
                            oraTran.Commit();
                        }
                        else
                        {
                            arrMessage.Add(DeletedRow[0].ToString() + " cannot be deleted");
                            oraTran.Rollback();
                        }
                    }
                    catch (Exception e)
                    {
                        arrMessage.Add(DeletedRow[0].ToString() + " cannot be deleted");
                        oraTran.Rollback();
                    }
                }
                if (arrMessage.Count > 0)
                {
                    return arrMessage;
                }
                else
                {
                    arrMessage.Add("Success");
                    return arrMessage;
                }

            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            finally
            {
                obJWk.MyConnection.Close();
            }
        }
        #endregion
    }
}