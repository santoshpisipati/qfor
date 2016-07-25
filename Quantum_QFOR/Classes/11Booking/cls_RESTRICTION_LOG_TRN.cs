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
    public class clsRESTRICTION_LOG_TRN : CommonFeatures
    {
        #region "Fetch Function"

        public DataSet FetchAll(Int64 P_Restriction_Log_Pk = 0, string P_Restriction_Dt = "", string P_Booking_Type = "", string P_Booking_Ref_No = "", Int64 P_Prov_Booking_Trn_Fk = 0, Int64 P_Booking_Trn_Fk = 0, Int64 P_Booking_Container_Fk = 0, Int64 P_Booking_Restriction_Fk = 0, Int64 P_Restriction_Type = 0, string P_Status = "",
        Int64 P_Approved_Fk = 0, string P_Approved_Dt = "", string SearchType = "", string SortExpression = "")
        {
            string strSQL = null;
            strSQL = "SELECT ROWNUM SR_NO, ";
            strSQL = strSQL + " Restriction_Log_Pk,";
            strSQL = strSQL + " Restriction_Dt,";
            strSQL = strSQL + " Booking_Type,";
            strSQL = strSQL + " Booking_Ref_No,";
            strSQL = strSQL + " Prov_Booking_Trn_Fk,";
            strSQL = strSQL + " Booking_Trn_Fk,";
            strSQL = strSQL + " Booking_Container_Fk,";
            strSQL = strSQL + " Booking_Restriction_Fk,";
            strSQL = strSQL + " Restriction_Type,";
            strSQL = strSQL + " Status,";
            strSQL = strSQL + " Approved_Fk,";
            strSQL = strSQL + " Approved_Dt,";
            strSQL = strSQL + " Version_No ";
            strSQL = strSQL + " FROM RESTRICTION_LOG_TRN ";
            strSQL = strSQL + " WHERE ( 1 = 1) ";

            if (P_Restriction_Log_Pk != 0)
            {
                strSQL = strSQL + " And Restriction_Log_Pk = " + P_Restriction_Log_Pk + " ";
            }

            if (P_Restriction_Dt.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Restriction_Dt like '%" + P_Restriction_Dt + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Restriction_Dt like '" + P_Restriction_Dt + "%' ";
                }
            }
            else
            {
            }
            if (P_Booking_Type.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Booking_Type like '%" + P_Booking_Type + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Booking_Type like '" + P_Booking_Type + "%' ";
                }
            }
            else
            {
            }
            if (P_Booking_Ref_No.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Booking_Ref_No like '%" + P_Booking_Ref_No + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Booking_Ref_No like '" + P_Booking_Ref_No + "%' ";
                }
            }
            else
            {
            }
            if (P_Prov_Booking_Trn_Fk > 0)
            {
                strSQL = strSQL + " And Prov_Booking_Trn_Fk = " + P_Prov_Booking_Trn_Fk + " ";
            }
            if (P_Booking_Trn_Fk > 0)
            {
                strSQL = strSQL + " And Booking_Trn_Fk = " + P_Booking_Trn_Fk + " ";
            }
            if (P_Booking_Container_Fk > 0)
            {
                strSQL = strSQL + " And Booking_Container_Fk = " + P_Booking_Container_Fk + " ";
            }
            if (P_Booking_Restriction_Fk > 0)
            {
                strSQL = strSQL + " And Booking_Restriction_Fk = " + P_Booking_Restriction_Fk + " ";
            }
            if (P_Restriction_Type > 0)
            {
                strSQL = strSQL + " And Restriction_Type = " + P_Restriction_Type + " ";
            }
            if (P_Status.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Status like '%" + P_Status + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Status like '" + P_Status + "%' ";
                }
            }
            else
            {
            }
            if (P_Approved_Fk > 0)
            {
                strSQL = strSQL + " And Approved_Fk = " + P_Approved_Fk + " ";
            }
            if (P_Approved_Dt.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Approved_Dt like '%" + P_Approved_Dt + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Approved_Dt like '" + P_Approved_Dt + "%' ";
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

        #endregion "Fetch Function"

        #region "Fetch Function"

        public DataSet FetchRestrictionDet(Int64 P_Restriction_Log_Pk = 0, Int64 P_Commercial_Schedule_Pk = 0, Int64 P_POL_Pk = 0, string P_Booking_Type = "", string P_Booking_Ref_No = "", Int64 P_Prov_Booking_Trn_Fk = 0, Int64 P_Booking_Trn_Fk = 0, Int64 P_Booking_Container_Fk = 0, Int64 P_Restriction_Type = 0, string P_Status = "",
        Int64 P_Approved_Fk = 0, string P_Approved_Dt = "", string SearchType = "", string SortExpression = "")
        {
            string strSQL = null;
            strSQL = "SELECT ROWNUM SR_NO, ";
            strSQL = strSQL + " Restriction_Log_Pk,";
            strSQL = strSQL + " RL.Restriction_Dt,";
            strSQL = strSQL + " Booking_Type,";
            strSQL = strSQL + " Booking_Ref_No,";
            strSQL = strSQL + " Prov_Booking_Trn_Fk,";
            strSQL = strSQL + " Booking_Summary_Trn_Fk,";
            strSQL = strSQL + " Booking_Container_Fk,";
            strSQL = strSQL + " Booking_Restriction_Fk,";
            strSQL = strSQL + " BR.RESTRICTION_MESSAGE,";
            strSQL = strSQL + " decode(RL.RESTRICTION_TYPE,0,'Blocked',1,'Need Approval') Restriction_Type , ";
            strSQL = strSQL + " decode(RL.Status,'B','Blocked','P','Need Approval','A','Approved')   Status,";
            strSQL = strSQL + " Approved_Fk,";
            strSQL = strSQL + " Approved_Dt,";
            strSQL = strSQL + " RL.Version_No ";
            strSQL = strSQL + " FROM RESTRICTION_LOG_TRN RL, BOOKING_RESTRICTION_TRN BR";
            strSQL = strSQL + " WHERE ( 1 = 1) ";
            strSQL = strSQL + " and  RL.BOOKING_RESTRICTION_FK = BR.BOOKING_RESTRICTION_PK";

            if (P_Booking_Type == "P")
            {
                strSQL = strSQL + " and RL.PROV_BOOKING_TRN_FK in (";
                strSQL = strSQL + " select PB.PROV_BOOKING_PK from prov_booking_trn PB ";
                strSQL = strSQL + " where pb.commercial_schedule_trn_fk = " + P_Commercial_Schedule_Pk;
                strSQL = strSQL + " and pb.pol_fk=" + P_POL_Pk + ")";
            }
            if (P_Booking_Type == "V")
            {
                strSQL = strSQL + " and RL.Booking_Summary_Trn_Fk in ";
                strSQL = strSQL + " (select BST.BOOKING_SUMMARY_PK from booking_summary_trn BST where BST.BOOKING_TRN_FK in ";
                strSQL = strSQL + " (select B.BOOKING_TRN_PK from booking_trn B ";
                strSQL = strSQL + " where B.commercial_schedule_trn_fk = " + P_Commercial_Schedule_Pk;
                strSQL = strSQL + " and B.POL_FK=" + P_POL_Pk + "))";
            }
            if (P_Booking_Type == "A")
            {
                strSQL = strSQL + " and RL.BOOKING_CONTAINER_FK in (";
                strSQL = strSQL + " select BCT.BOOKING_CONTAINERS_PK from booking_containers_trn BCT , ";
                strSQL = strSQL + " booking_trn BT , Booking_Summary_Trn BST where ";
                strSQL = strSQL + " BCT.BOOKING_SUMMARY_FK = BST.BOOKING_SUMMARY_PK";
                strSQL = strSQL + " and BST.BOOKING_TRN_FK = BT.BOOKING_TRN_PK";
                strSQL = strSQL + " and BT.COMMERCIAL_SCHEDULE_TRN_FK=" + P_Commercial_Schedule_Pk;
                strSQL = strSQL + " and BT.Pol_Fk=" + P_POL_Pk + ")";
            }

            if (string.IsNullOrEmpty(P_Booking_Type))
            {
                strSQL = strSQL + " and RL.PROV_BOOKING_TRN_FK in (";
                strSQL = strSQL + " select PB.PROV_BOOKING_PK from prov_booking_trn PB ";
                if (P_Commercial_Schedule_Pk > 0)
                {
                    strSQL = strSQL + " where pb.commercial_schedule_trn_fk = " + P_Commercial_Schedule_Pk;
                }
                if (P_POL_Pk > 0)
                {
                    strSQL = strSQL + " and pb.pol_fk=" + P_POL_Pk;
                }
                strSQL = strSQL + ")";
                //strSQL = strSQL & " and RL.Booking_summary_Trn_Fk in (" & vbCrLf
                //strSQL = strSQL & " select B.BOOKING_TRN_PK from booking_trn B " & vbCrLf
                //strSQL = strSQL & " where B.commercial_schedule_trn_fk = " & P_Commercial_Schedule_Pk & vbCrLf
                //strSQL = strSQL & " and B.POL_FK=" & P_POL_Pk & ")" & vbCrLf
                //strSQL = strSQL & " and RL.BOOKING_CONTAINER_FK in (" & vbCrLf
                //strSQL = strSQL & " select BCT.BOOKING_CONTAINERS_PK from booking_containers_trn BCT , " & vbCrLf
                //strSQL = strSQL & " booking_trn BT , Booking_Summary_Trn BST where " & vbCrLf
                //strSQL = strSQL & " BCT.BOOKING_SUMMARY_FK = BST.BOOKING_SUMMARY_PK" & vbCrLf
                //strSQL = strSQL & " and BST.BOOKING_TRN_FK = BT.BOOKING_TRN_PK" & vbCrLf
                //strSQL = strSQL & " and BT.COMMERCIAL_SCHEDULE_TRN_FK=" & P_Commercial_Schedule_Pk & vbCrLf
                //strSQL = strSQL & " and BT.Pol_Fk=" & P_POL_Pk & ")" & vbCrLf
            }
            //If P_Restriction_Log_Pk <> 0 Then
            //    strSQL = strSQL & " And Restriction_Log_Pk = " & P_Restriction_Log_Pk & " "
            //End If

            //If P_Booking_Type.ToString.Trim.Length > 0 Then
            //    If SearchType = "C" Then
            //        strSQL = strSQL & " And Booking_Type like '%" & P_Booking_Type & "%' "
            //    Else
            //        strSQL = strSQL & " And Booking_Type like '" & P_Booking_Type & "%' "
            //    End If
            //Else
            //End If
            //If P_Booking_Ref_No.ToString.Trim.Length > 0 Then
            //    If SearchType = "C" Then
            //        strSQL = strSQL & " And Booking_Ref_No like '%" & P_Booking_Ref_No & "%' "
            //    Else
            //        strSQL = strSQL & " And Booking_Ref_No like '" & P_Booking_Ref_No & "%' "
            //    End If
            //Else
            //End If
            //If P_Prov_Booking_Trn_Fk > 0 Then
            //    strSQL = strSQL & " And Prov_Booking_Trn_Fk = " & P_Prov_Booking_Trn_Fk & " "
            //End If
            //If P_Booking_Trn_Fk > 0 Then
            //    strSQL = strSQL & " And Booking_Trn_Fk = " & P_Booking_Trn_Fk & " "
            //End If
            //If P_Booking_Container_Fk > 0 Then
            //    strSQL = strSQL & " And Booking_Container_Fk = " & P_Booking_Container_Fk & " "
            //End If
            //If P_Booking_Restriction_Fk > 0 Then
            //    strSQL = strSQL & " And Booking_Restriction_Fk = " & P_Booking_Restriction_Fk & " "
            //End If
            //If P_Restriction_Type > 0 Then
            //    strSQL = strSQL & " And Restriction_Type = " & P_Restriction_Type & " "
            //End If
            //If P_Status.ToString.Trim.Length > 0 Then
            //    If SearchType = "C" Then
            //        strSQL = strSQL & " And Status like '%" & P_Status & "%' "
            //    Else
            //        strSQL = strSQL & " And Status like '" & P_Status & "%' "
            //    End If
            //Else
            //End If
            //If P_Approved_Fk > 0 Then
            //    strSQL = strSQL & " And Approved_Fk = " & P_Approved_Fk & " "
            //End If
            //If P_Approved_Dt.ToString.Trim.Length > 0 Then
            //    If SearchType = "C" Then
            //        strSQL = strSQL & " And Approved_Dt like '%" & P_Approved_Dt & "%' "
            //    Else
            //        strSQL = strSQL & " And Approved_Dt like '" & P_Approved_Dt & "%' "
            //    End If
            //Else
            //End If
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

        #endregion "Fetch Function"

        #region "Insert Function"

        public int Insert(string BOOKING_TYPE, string BOOKING_ID, long PROV_BOOKING_TRN_FK = 0, long BOOKING_TRN_FK = 0, long BOOKING_CONTAINER_FK = 0, long BOOKING_RESTRICTION_FK = 0, long RESTRICTION_TYPE = 0, string STATUS = "")
        {
            try
            {
                WorkFlow objWS = new WorkFlow();
                Int32 intPkVal = default(Int32);
                objWS.MyCommand.CommandType = CommandType.StoredProcedure;
                var _with1 = objWS.MyCommand.Parameters;
                _with1.Add("RESTRICTION_DT_IN", System.String.Format("{0:dd/MM/yyyy}", DateTime.Today)).Direction = ParameterDirection.Input;
                _with1.Add("BOOKING_TYPE_IN", BOOKING_TYPE).Direction = ParameterDirection.Input;
                _with1.Add("BOOKING_REF_NO_IN", BOOKING_ID).Direction = ParameterDirection.Input;
                _with1.Add("PROV_BOOKING_TRN_FK_IN", (PROV_BOOKING_TRN_FK == 0 ? 0 : PROV_BOOKING_TRN_FK)).Direction = ParameterDirection.Input;
                _with1.Add("BOOKING_SUMMARY_TRN_FK_IN", (BOOKING_TRN_FK == 0 ? 0 : BOOKING_TRN_FK)).Direction = ParameterDirection.Input;
                _with1.Add("BOOKING_CONTAINER_FK_IN", (BOOKING_CONTAINER_FK == 0 ? 0 : BOOKING_CONTAINER_FK)).Direction = ParameterDirection.Input;
                _with1.Add("BOOKING_RESTRICTION_FK_IN", BOOKING_RESTRICTION_FK).Direction = ParameterDirection.Input;
                _with1.Add("RESTRICTION_TYPE_IN", RESTRICTION_TYPE).Direction = ParameterDirection.Input;
                _with1.Add("STATUS_IN", STATUS).Direction = ParameterDirection.Input;
                _with1.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", intPkVal).Direction = ParameterDirection.Output;
                objWS.MyCommand.CommandText = objWS.MyUserName + ".RESTRICTION_LOG_TRN_PKG.RESTRICTION_LOG_TRN_INS";
                if (objWS.ExecuteCommands() == true)
                {
                    return intPkVal;
                }
                else
                {
                    return -1;
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

        #endregion "Insert Function"

        #region "Update Function"

        public int Update(long RESTRICTION_LOG_PK, string BOOKING_TYPE, string BOOKING_REF_NO, long PROV_BOOKING_TRN_FK = 0, long BOOKING_TRN_FK = 0, long BOOKING_CONTAINER_FK = 0, long BOOKING_RESTRICTION_FK = 0, string RESTRICTION_TYPE = "", string STATUS = "", long APPROVED_FK = 0)
        {
            try
            {
                WorkFlow objWS = new WorkFlow();
                string intPkVal = "";
                System.DBNull MyNull = null;
                objWS.MyCommand.CommandType = CommandType.StoredProcedure;
                var _with2 = objWS.MyCommand.Parameters;
                _with2.Add("RESTRICTION_LOG_PK_IN", RESTRICTION_LOG_PK).Direction = ParameterDirection.Input;
                _with2.Add("RESTRICTION_DT_IN", DateTime.Today).Direction = ParameterDirection.Input;
                _with2.Add("BOOKING_TYPE_IN", BOOKING_TYPE).Direction = ParameterDirection.Input;
                _with2.Add("BOOKING_REF_NO_IN", BOOKING_REF_NO).Direction = ParameterDirection.Input;
                _with2.Add("PROV_BOOKING_TRN_FK_IN", PROV_BOOKING_TRN_FK).Direction = ParameterDirection.Input;
                _with2.Add("BOOKING_TRN_FK_IN", BOOKING_TRN_FK).Direction = ParameterDirection.Input;
                _with2.Add("BOOKING_CONTAINER_FK_IN", BOOKING_CONTAINER_FK).Direction = ParameterDirection.Input;
                _with2.Add("BOOKING_RESTRICTION_FK_IN", BOOKING_RESTRICTION_FK).Direction = ParameterDirection.Input;
                _with2.Add("RESTRICTION_TYPE_IN", RESTRICTION_TYPE).Direction = ParameterDirection.Input;
                _with2.Add("STATUS_IN", STATUS).Direction = ParameterDirection.Input;
                _with2.Add("APPROVED_FK_IN", APPROVED_FK).Direction = ParameterDirection.Input;
                _with2.Add("APPROVED_DT_IN", DateTime.Today).Direction = ParameterDirection.Input;
                _with2.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                _with2.Add("VERSION_NO_IN", Version_No).Direction = ParameterDirection.Input;
                _with2.Add("RETURN_VALUE", intPkVal).Direction = ParameterDirection.Output;
                objWS.MyCommand.Parameters["RETURN_VALUE"].OracleDbType = OracleDbType.Varchar2;
                objWS.MyCommand.Parameters["RETURN_VALUE"].Size = 50;

                objWS.MyCommand.CommandText = objWS.MyUserName + ".CORPORATE_MST_TBL_PKG.CORPORATE_MST_TBL_UPD";
                if (objWS.ExecuteCommands() == true)
                {
                    if (string.IsNullOrEmpty(objWS.MyCommand.Parameters["RETURN_VALUE"].Value.ToString()))
                    {
                        return 1;
                    }
                    else
                    {
                        intPkVal = Convert.ToString(objWS.MyCommand.Parameters["RETURN_VALUE"].Value);
                        return -1;
                    }
                }
                else
                {
                    return -1;
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

        #endregion "Update Function"

        #region "Save Function"

        public ArrayList Save(DataSet M_DataSet)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            
            int intPKVal = 0;
            long lngI = 0;
            Int32 RecAfct = default(Int32);
            Int32 i = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            string strsql = null;
            //Dim updCommand As New OracleClient.OracleCommand
            //Dim delCommand As New OracleClient.OracleCommand
            insCommand.Connection = objWK.MyConnection;
            insCommand.CommandType = CommandType.Text;
            insCommand.Transaction = TRAN;
            try
            {
                for (i = 0; i <= M_DataSet.Tables[0].Rows.Count - 1; i++)
                {
                    if (M_DataSet.Tables[0].Rows[i]["Status"] == "A")
                    {
                        strsql = "";
                        strsql = "UPDATE RESTRICTION_LOG_TRN R";
                        strsql = strsql + "SET R.STATUS ='A',";
                        strsql = strsql + "R.APPROVED_FK = " + M_DataSet.Tables[0].Rows[i]["Approved_Fk"] + ",";
                        strsql = strsql + "R.APPROVED_DT = SYSDATE,";
                        strsql = strsql + "R.LAST_MODIFIED_BY_FK= " + M_DataSet.Tables[0].Rows[i]["Approved_Fk"] + ",";
                        strsql = strsql + "R.LAST_MODIFIED_DT = SYSDATE";
                        strsql = strsql + "WHERE R.RESTRICTION_LOG_PK = " + M_DataSet.Tables[0].Rows[i]["RESTRICTION_LOG_PK"];
                        insCommand.CommandText = strsql;
                        insCommand.ExecuteNonQuery();
                        if (arrMessage.Count > 0)
                        {
                            TRAN.Rollback();
                            return arrMessage;
                        }
                    }
                }

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    TRAN.Commit();
                    arrMessage.Add("all data saved successfully");
                    return arrMessage;
                }

                //With insCommand
                //    .Connection = objWK.MyConnection
                //    .CommandType = CommandType.StoredProcedure
                //    .CommandText = ""
                //    With .Parameters

                //        insCommand.Parameters.Add("RESTRICTION_DT_IN", OracleClient.OracleDbType.Date, 10, "RESTRICTION_DT").Direction = ParameterDirection.Input
                //        insCommand.Parameters["RESTRICTION_DT_IN"].SourceVersion = DataRowVersion.Current
                //        insCommand.Parameters.Add("BOOKING_TYPE_IN", OracleClient.OracleDbType.Varchar2, 1, "BOOKING_TYPE").Direction = ParameterDirection.Input
                //        insCommand.Parameters["BOOKING_TYPE_IN"].SourceVersion = DataRowVersion.Current
                //        insCommand.Parameters.Add("BOOKING_REF_NO_IN", OracleClient.OracleDbType.Varchar2, 20, "BOOKING_REF_NO").Direction = ParameterDirection.Input
                //        insCommand.Parameters["BOOKING_REF_NO_IN"].SourceVersion = DataRowVersion.Current
                //        insCommand.Parameters.Add("PROV_BOOKING_TRN_FK_IN", OracleClient.OracleDbType.Int32, 10, "PROV_BOOKING_TRN_FK").Direction = ParameterDirection.Input
                //        insCommand.Parameters["PROV_BOOKING_TRN_FK_IN"].SourceVersion = DataRowVersion.Current
                //        insCommand.Parameters.Add("BOOKING_TRN_FK_IN", OracleClient.OracleDbType.Int32, 10, "BOOKING_TRN_FK").Direction = ParameterDirection.Input
                //        insCommand.Parameters["BOOKING_TRN_FK_IN"].SourceVersion = DataRowVersion.Current
                //        insCommand.Parameters.Add("BOOKING_CONTAINER_FK_IN", OracleClient.OracleDbType.Int32, 10, "BOOKING_CONTAINER_FK").Direction = ParameterDirection.Input
                //        insCommand.Parameters["BOOKING_CONTAINER_FK_IN"].SourceVersion = DataRowVersion.Current
                //        insCommand.Parameters.Add("BOOKING_RESTRICTION_FK_IN", OracleClient.OracleDbType.Int32, 10, "BOOKING_RESTRICTION_FK").Direction = ParameterDirection.Input
                //        insCommand.Parameters["BOOKING_RESTRICTION_FK_IN"].SourceVersion = DataRowVersion.Current
                //        insCommand.Parameters.Add("RESTRICTION_TYPE_IN", OracleClient.OracleDbType.Int32, 1, "RESTRICTION_TYPE").Direction = ParameterDirection.Input
                //        insCommand.Parameters["RESTRICTION_TYPE_IN"].SourceVersion = DataRowVersion.Current
                //        insCommand.Parameters.Add("STATUS_IN", OracleClient.OracleDbType.Varchar2, 1, "STATUS").Direction = ParameterDirection.Input
                //        insCommand.Parameters["STATUS_IN"].SourceVersion = DataRowVersion.Current
                //        insCommand.Parameters.Add("APPROVED_FK_IN", OracleClient.OracleDbType.Int32, 10, "APPROVED_FK").Direction = ParameterDirection.Input
                //        insCommand.Parameters["APPROVED_FK_IN"].SourceVersion = DataRowVersion.Current
                //        insCommand.Parameters.Add("APPROVED_DT_IN", Date.Today).Direction = ParameterDirection.Input
                //        insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input
                //        insCommand.Parameters.Add("RETURN_VALUE", OracleClient.OracleDbType.Int32, 10, "RESTRICTION_LOG_TRN_PK").Direction = ParameterDirection.Output
                //        insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current
                //    End With
                //End With

                //With delCommand
                //    .Connection = objWK.MyConnection
                //    .CommandType = CommandType.StoredProcedure
                //    .CommandText = objWK.MyUserName & ".RESTRICTION_LOG_TRN_PKG.RESTRICTION_LOG_TRN_DEL"
                //    With .Parameters
                //        delCommand.Parameters.Add("RESTRICTION_LOG_PK_IN", OracleClient.OracleDbType.Int32, 10, "RESTRICTION_LOG_PK").Direction = ParameterDirection.Input
                //        delCommand.Parameters["RESTRICTION_LOG_PK_IN"].SourceVersion = DataRowVersion.Current
                //        delCommand.Parameters.Add("VERSION_NO_IN", OracleClient.OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input
                //        delCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current
                //        delCommand.Parameters.Add("RETURN_VALUE", OracleClient.OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output
                //        delCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current
                //    End With
                //End With

                //With updCommand
                //    .Connection = objWK.MyConnection
                //    .CommandType = CommandType.StoredProcedure
                //    .CommandText = objWK.MyUserName & ".RESTRICTION_LOG_TRN_PKG.RESTRICTION_LOG_TRN_UPD"
                //    With .Parameters

                //        updCommand.Parameters.Add("RESTRICTION_LOG_PK_IN", OracleClient.OracleDbType.Int32, 10, "RESTRICTION_LOG_PK").Direction = ParameterDirection.Input
                //        updCommand.Parameters["RESTRICTION_LOG_PK_IN"].SourceVersion = DataRowVersion.Current
                //        updCommand.Parameters.Add("RESTRICTION_DT_IN", OracleClient.OracleDbType.Date, 10, "RESTRICTION_DT").Direction = ParameterDirection.Input
                //        updCommand.Parameters["RESTRICTION_DT_IN"].SourceVersion = DataRowVersion.Current
                //        updCommand.Parameters.Add("BOOKING_TYPE_IN", OracleClient.OracleDbType.Varchar2, 1, "BOOKING_TYPE").Direction = ParameterDirection.Input
                //        updCommand.Parameters["BOOKING_TYPE_IN"].SourceVersion = DataRowVersion.Current
                //        updCommand.Parameters.Add("BOOKING_REF_NO_IN", OracleClient.OracleDbType.Varchar2, 20, "BOOKING_REF_NO").Direction = ParameterDirection.Input
                //        updCommand.Parameters["BOOKING_REF_NO_IN"].SourceVersion = DataRowVersion.Current
                //        updCommand.Parameters.Add("PROV_BOOKING_TRN_FK_IN", OracleClient.OracleDbType.Int32, 10, "PROV_BOOKING_TRN_FK").Direction = ParameterDirection.Input
                //        updCommand.Parameters["PROV_BOOKING_TRN_FK_IN"].SourceVersion = DataRowVersion.Current
                //        updCommand.Parameters.Add("BOOKING_TRN_FK_IN", OracleClient.OracleDbType.Int32, 10, "BOOKING_TRN_FK").Direction = ParameterDirection.Input
                //        updCommand.Parameters["BOOKING_TRN_FK_IN"].SourceVersion = DataRowVersion.Current
                //        updCommand.Parameters.Add("BOOKING_CONTAINER_FK_IN", OracleClient.OracleDbType.Int32, 10, "BOOKING_CONTAINER_FK").Direction = ParameterDirection.Input
                //        updCommand.Parameters["BOOKING_CONTAINER_FK_IN"].SourceVersion = DataRowVersion.Current
                //        updCommand.Parameters.Add("BOOKING_RESTRICTION_FK_IN", OracleClient.OracleDbType.Int32, 10, "BOOKING_RESTRICTION_FK").Direction = ParameterDirection.Input
                //        updCommand.Parameters["BOOKING_RESTRICTION_FK_IN"].SourceVersion = DataRowVersion.Current
                //        updCommand.Parameters.Add("RESTRICTION_TYPE_IN", OracleClient.OracleDbType.Int32, 1, "RESTRICTION_TYPE").Direction = ParameterDirection.Input
                //        updCommand.Parameters["RESTRICTION_TYPE_IN"].SourceVersion = DataRowVersion.Current
                //        updCommand.Parameters.Add("STATUS_IN", OracleClient.OracleDbType.Varchar2, 1, "STATUS").Direction = ParameterDirection.Input
                //        updCommand.Parameters["STATUS_IN"].SourceVersion = DataRowVersion.Current
                //        updCommand.Parameters.Add("APPROVED_FK_IN", OracleClient.OracleDbType.Int32, 10, "APPROVED_FK").Direction = ParameterDirection.Input
                //        updCommand.Parameters["APPROVED_FK_IN"].SourceVersion = DataRowVersion.Current
                //        updCommand.Parameters.Add("APPROVED_DT_IN", OracleClient.OracleDbType.Date, 10, "APPROVED_DT").Direction = ParameterDirection.Input
                //        updCommand.Parameters["APPROVED_DT_IN"].SourceVersion = DataRowVersion.Current
                //        updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_Last_Modified_By_FK).Direction = ParameterDirection.Input
                //        updCommand.Parameters.Add("VERSION_NO_IN", OracleClient.OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input
                //        updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current
                //        updCommand.Parameters.Add("RETURN_VALUE", OracleClient.OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output
                //        updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current
                //    End With
                //End With

                //AddHandler objWK.MyDataAdapter.RowUpdated, New OracleRowUpdatedEventHandler(AddressOf OnRowUpdated)

                //'objWK.MyDataAdapter.ContinueUpdateOnError = True
                //With objWK.MyDataAdapter

                //    .InsertCommand = insCommand
                //    .InsertCommand.Transaction = TRAN
                //    .UpdateCommand = updCommand
                //    .UpdateCommand.Transaction = TRAN
                //    .DeleteCommand = delCommand
                //    .DeleteCommand.Transaction = TRAN
                //    RecAfct = .Update(M_DataSet)

                //    If arrMessage.Count > 0 Then
                //        TRAN.Rollback()
                //        Return arrMessage
                //    Else
                //        TRAN.Commit()
                //        arrMessage.Add("All Data Saved Successfully")
                //        Return arrMessage
                //    End If

                //End With
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
                objWK.MyCommand.Connection.Close();
            }
        }

        #endregion "Save Function"
    }
}