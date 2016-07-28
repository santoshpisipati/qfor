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

//Option Strict On

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
    public class cls_BOOKING_RESTRICTION_TRN : CommonFeatures
    {
        #region "class level variables"
        #endregion
        private DataSet M_PortRestrictDs;
        #region "property Procedure"
        public DataSet PortRestrictDs
        {
            get { return M_PortRestrictDs; }
            set { M_PortRestrictDs = value; }
        }
        #endregion

        #region "Fetch Function"
        public DataSet FetchAll(DateTime P_Restriction_Dt, Int64 P_Booking_Restriction_Pk = 0, string P_Restriction_Ref_No = "", string P_Restricted_By = "", string P_Restriction_Message = "", string P_Commodity_Id = "", string P_Imdg_Class_Code = "", Int16 P_Hazardous = -1, string SearchType = "", string SortExpression = "")
        {

            string strSQL = null;
            strSQL = "SELECT ROWNUM SR_NO, ";
            strSQL += " R.Booking_Restriction_Pk,";
            strSQL += " R.Restriction_Ref_No,";
            //strSQL &= vbCrLf & " to_Char(R.Restriction_Dt,'dd-Mon-yyyy') Restriction_Dt,"
            strSQL += " to_Char(R.Restriction_Dt,'" + dateFormat + "') Restriction_Dt,";
            strSQL = strSQL + " R.Restricted_By_Fk,";
            strSQL += " decode(R.Restriction_Type,1,'Block','Approval') Restriction_Type,";
            strSQL += " initcap(R.Restriction_Message) Restriction_Message,";
            //strSQL &= vbCrLf & " to_char(R.Effective_From_Dt,'dd-Mon-yyyy') Effective_From_Dt,"
            //strSQL &= vbCrLf & " to_char(R.Effective_To_Dt,'dd-Mon-yyyy') Effective_To_Dt,"
            strSQL += " to_char(R.Effective_From_Dt,'" + dateFormat + "') Effective_From_Dt,";
            strSQL += " to_char(R.Effective_To_Dt,'" + dateFormat + "') Effective_To_Dt,";
            strSQL += " initcap(C.Commodity_Name) Commodity_Name,";
            strSQL += " R.Commodity_Mst_Fk,";
            strSQL += " R.Imdg_Class_Code,";
            strSQL += " R.Hazardous,";
            strSQL = strSQL + " R.Weight_Limit_In_Kg,";
            strSQL = strSQL + " R.Lead_Up_Dt,";
            strSQL += " R.LEAD_UP_MESSAGE LEAD_UP_MESSAGE,";
            strSQL = strSQL + "R.Pol_Fk,";
            strSQL = strSQL + " R.Pod_Fk,";
            strSQL = strSQL + " R.Version_No,E.employee_name  employee_name, ";
            strSQL += " R.Restriction_Type  RstType";
            strSQL += " FROM BOOKING_RESTRICTION_TRN  R,";
            strSQL = strSQL + " COMMODITY_MST_TBL C,";
            strSQL += " EMPLOYEE_MST_TBL E";
            strSQL += " Where ";
            strSQL += " R.Restricted_By_Fk = E.Employee_Mst_Pk ";
            strSQL = strSQL + " And R.Commodity_MSt_FK= C.Commodity_Mst_PK(+) ";

            if (P_Booking_Restriction_Pk != 0)
            {
                if (SearchType == "C")
                {
                    strSQL += " And R.Booking_Restriction_Pk like '%" + P_Booking_Restriction_Pk + "%' ";
                }
                else
                {
                    strSQL += " And R.Booking_Restriction_Pk like '" + P_Booking_Restriction_Pk + "%' ";
                }
            }

            if (P_Restriction_Dt != DateTime.MinValue) {
                //If SearchType = "C" Then
                //    strSQL &= vbCrLf & " And R.Restriction_Dt like '%" & P_Restriction_Dt & "%' "
                //Else
                //    strSQL &= vbCrLf & " And R.Restriction_Dt like '" & P_Restriction_Dt & "%' "
                //End If
                strSQL += " AND  To_char(R.Restriction_Dt,'dd-Mon-yyyy') = '" + System.String.Format("{0:dd-MMM-yyyy}", P_Restriction_Dt) + "'";
            }

            if (P_Restriction_Ref_No.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL += " And upper(R.Restriction_Ref_No) like '%" + P_Restriction_Ref_No.ToString().Trim().ToUpper() + "%' ";
                }
                else
                {
                    strSQL += " And upper(R.Restriction_Ref_No) like '" + P_Restriction_Ref_No.ToString().Trim().ToUpper() + "%' ";
                }
            }

            if (P_Restriction_Message.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL += " And upper(R.Restriction_Message) like '%" + P_Restriction_Message.ToUpper() + "%' ";
                }
                else
                {
                    strSQL += " And upper(R.Restriction_Message) like '" + P_Restriction_Message.ToUpper() + "%' ";
                }
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
        #region "function to fetch port"


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
            strSQL += "      AND brp.booking_restriction_fk=193 " ;
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
            strSQL += "               WHERE brp.booking_restriction_fk=193) " ;
            strSQL += "          " ;
            strSQL += ")  ORDER BY tli_ref_no)qry " ;
            strSQL += "      " ;
            strSQL += " " ;
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

        public DataSet FetchSpec_Port(Int64 P_Pol_mst_pk = 0, Int64 P_PoD_mst_pk = 0)
        {

            string strSQL = null;
            // Dim strCondition As String
            WorkFlow objWF = new WorkFlow();
            strSQL = "SELECT ROWNUM SR_NO, ";
            strSQL = strSQL + " 0 active,";
            strSQL = strSQL + " S.FROM_PORT_FK POL_FK,";
            strSQL = strSQL + " POL.PORT_ID POL_ID, ";
            strSQL = strSQL + " POL.Port_name POL_Name,";
            strSQL = strSQL + " S.TO_PORT_FK POD_FK,";
            strSQL = strSQL + " POD.PORT_ID POD_ID,";
            strSQL = strSQL + " Pod.Port_Name POD_Name,";
            strSQL = strSQL + " 0 Booking_Restriction_Port_Pk,";
            strSQL = strSQL + " 0 Booking_Restriction_Fk,";
            strSQL = strSQL + " 0 Created_By_Fk,";
            strSQL = strSQL + " to_date(null,'dd-Mon-yyyy') Created_Dt,";
            strSQL = strSQL + " 0 Last_Modified_By_Fk,";
            strSQL = strSQL + " to_date(null,'dd-Mon-yyyy') Last_Modified_Dt, ";
            strSQL = strSQL + " bRPtrn.Version_No Version_No";
            strSQL = strSQL + " from ";
            strSQL = strSQL + " Sector_Mst_Tbl S,";
            strSQL = strSQL + " Port_Mst_Tbl POL, ";
            strSQL = strSQL + " Port_Mst_Tbl POD,";
            strSQL = strSQL + "  booking_restr_port_trn bRPtrn";

            strSQL = strSQL + " where ";
            strSQL = strSQL + " S.FROM_PORT_FK=POL.PORT_MST_PK ";
            strSQL = strSQL + " and S.TO_PORT_FK=POD.PORT_MST_PK";
            strSQL = strSQL + " and brptrn.pol_fk(+)=pol.port_mst_pk";
            //strSQL &= vbCrLf & strCondition


            if (P_Pol_mst_pk > 0)
            {
                strSQL +=  " AND S.FROM_PORT_FK=" + P_Pol_mst_pk;
            }
            if (P_PoD_mst_pk > 0)
            {
                strSQL +=  " AND S.TO_PORT_FK=" + P_PoD_mst_pk;
            }
            if (P_Pol_mst_pk <= 0 & P_PoD_mst_pk <= 0)
            {
                strSQL +=  " AND S.FROM_PORT_FK=" + P_Pol_mst_pk;
                strSQL +=  " AND S.TO_PORT_FK=" + P_PoD_mst_pk;
            }

            //If P_Pol_mst_pk > 0 And P_PoD_mst_pk > 0 Then
            //    strSQL &= vbCrLf & " AND S.FROM_PORT_FK=" & P_Pol_mst_pk
            //    strSQL &= vbCrLf & " AND S.TO_PORT_FK=" & P_PoD_mst_pk
            //ElseIf P_Pol_mst_pk > 0 Then
            //    strSQL &= vbCrLf & " AND S.FROM_PORT_FK=" & P_Pol_mst_pk
            //ElseIf P_PoD_mst_pk > 0 Then
            //    strSQL &= vbCrLf & " AND S.TO_PORT_FK=" & P_PoD_mst_pk
            //End If

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


        public OracleDataReader FetchAllPort()
        {
            string strSQL = null;
            //strSQL = "select  ' ' Port_ID,"
            //strSQL = strSQL & vbCrLf & "' ' Port_NAME,"
            //strSQL = strSQL & vbCrLf & "0 Port_MST_PK "
            //strSQL = strSQL & vbCrLf & " from DUAL "
            //strSQL = strSQL & vbCrLf & " UNION "

            strSQL = "select ";
            strSQL +=  " 0  port_mst_pk,";
            strSQL +=  " ' '  port_id";
            strSQL +=  "  from Dual";
            strSQL +=  "  Union";
            strSQL +=  "  select ";
            strSQL +=  " port_mst_pk, ";
            strSQL +=  " port_id ";
            strSQL +=  " from ";
            strSQL +=  " Port_Mst_Tbl P";
            strSQL +=  " WHERE P.ACTIVE=1";
            WorkFlow objWF = new WorkFlow();
            try
            {
                return objWF.GetDataReader(strSQL);
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

        public OracleDataReader FetchIDGMCode()
        {
            string strSQL = null;
            strSQL = "select ";
            strSQL +=  " distinct imdg_class_code ";
            strSQL +=  " from ";
            strSQL +=  " commodity_mst_tbl ";
            strSQL +=  " where ";
            strSQL +=  " imdg_class_code is not null";
            strSQL +=  " union";
            strSQL +=  " select '' imdg_class_code ";
            strSQL +=  " from ";
            strSQL +=  " dual";
            strSQL +=  " ORDER BY imdg_class_code DESC";

            WorkFlow objWF = new WorkFlow();
            try
            {
                return objWF.GetDataReader(strSQL);
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

        #region "Save Function"
        public ArrayList Delete(ref DataSet M_DataSet)
        {
            WorkFlow objWK = new WorkFlow();
            cls_BOOKING_RESTR_PORT_TRN objBkgrst_port = new cls_BOOKING_RESTR_PORT_TRN();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            Int32 RecAfct = default(Int32);
            OracleCommand delCommand = new OracleCommand();
            try
            {
                DataTable DtTbl = new DataTable();

                DtTbl = M_DataSet.Tables[0];
                var _with1 = delCommand;
                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".BOOKING_RESTRICTION_TRN_PKG.BOOKING_RESTRICTION_TRN_DEL";
                var _with2 = _with1.Parameters;
                delCommand.Parameters.Add("BOOKING_RESTRICTION_PK_IN", OracleDbType.Int32, 10, "BOOKING_RESTRICTION_PK").Direction = ParameterDirection.Input;
                delCommand.Parameters["BOOKING_RESTRICTION_PK_IN"].SourceVersion = DataRowVersion.Current;
                delCommand.Parameters.Add("DELETED_BY_FK_IN", LAST_MODIFIED_BY).Direction = ParameterDirection.Input;
                delCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                delCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
                delCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                delCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                delCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                
                var _with3 = objWK.MyDataAdapter;

                _with3.DeleteCommand = delCommand;
                _with3.DeleteCommand.Transaction = TRAN;
                RecAfct = _with3.Update(M_DataSet);

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    arrMessage.Add("Record Deleted");
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
                objWK.MyCommand.Connection.Close();

            }

        }


        public ArrayList Save(ref DataSet M_DataSet, decimal Booking_Trn_Pk = 0, string RestNo = "")
        {
            WorkFlow objWK = new WorkFlow();
            cls_BOOKING_RESTR_PORT_TRN objBkgrst_port = new cls_BOOKING_RESTR_PORT_TRN();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            
            int intPKVal = 0;
            long lngI = 0;
            long lngPK = 0;
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
                var _with4 = insCommand;
                _with4.Connection = objWK.MyConnection;
                _with4.CommandType = CommandType.StoredProcedure;
                _with4.CommandText = objWK.MyUserName + ".BOOKING_RESTRICTION_TRN_PKG.BOOKING_RESTRICTION_TRN_INS";
                var _with5 = _with4.Parameters;

                insCommand.Parameters.Add("RESTRICTION_REF_NO_IN", M_DataSet.Tables[0].Rows[0]["RESTRICTION_REF_NO"]).Direction = ParameterDirection.Input;
                //insCommand.Parameters("RESTRICTION_REF_NO_IN").SourceVersion = DataRowVersion.Current
                insCommand.Parameters.Add("RESTRICTION_DT_IN", M_DataSet.Tables[0].Rows[0]["RESTRICTION_DT"]).Direction = ParameterDirection.Input;
                //insCommand.Parameters("RESTRICTION_DT_IN").SourceVersion = DataRowVersion.Current
                insCommand.Parameters.Add("RESTRICTED_BY_FK_IN", M_DataSet.Tables[0].Rows[0]["RESTRICTED_BY_FK"]).Direction = ParameterDirection.Input;
                //insCommand.Parameters("RESTRICTED_BY_FK_IN").SourceVersion = DataRowVersion.Current
                insCommand.Parameters.Add("RESTRICTION_TYPE_IN", M_DataSet.Tables[0].Rows[0]["RESTRICTION_TYPE"]).Direction = ParameterDirection.Input;
                //insCommand.Parameters("RESTRICTION_TYPE_IN").SourceVersion = DataRowVersion.Current
                insCommand.Parameters.Add("EFFECTIVE_FROM_DT_IN", M_DataSet.Tables[0].Rows[0]["EFFECTIVE_FROM_DT"]).Direction = ParameterDirection.Input;
                //insCommand.Parameters("EFFECTIVE_FROM_DT_IN").SourceVersion = DataRowVersion.Current

                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["EFFECTIVE_TO_DT"].ToString()))
                {
                }
                else if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["EFFECTIVE_TO_DT"].ToString()))
                {
                    M_DataSet.Tables[0].Rows[0]["EFFECTIVE_TO_DT"] = DBNull.Value;
                }
                insCommand.Parameters.Add("EFFECTIVE_TO_DT_IN", M_DataSet.Tables[0].Rows[0]["EFFECTIVE_TO_DT"]).Direction = ParameterDirection.Input;
                //insCommand.Parameters("EFFECTIVE_TO_DT_IN").SourceVersion = DataRowVersion.Current
                insCommand.Parameters.Add("COMMODITY_MST_FK_IN", M_DataSet.Tables[0].Rows[0]["COMMODITY_MST_FK"]).Direction = ParameterDirection.Input;
                //insCommand.Parameters("COMMODITY_MST_FK_IN").SourceVersion = DataRowVersion.Current
                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["IMDG_CLASS_CODE"].ToString()))
                    M_DataSet.Tables[0].Rows[0]["IMDG_CLASS_CODE"] = DBNull.Value;
                insCommand.Parameters.Add("IMDG_CLASS_CODE_IN", M_DataSet.Tables[0].Rows[0]["IMDG_CLASS_CODE"]).Direction = ParameterDirection.Input;
                //insCommand.Parameters("IMDG_CLASS_CODE_IN").SourceVersion = DataRowVersion.Current
                insCommand.Parameters.Add("HAZARDOUS_IN", M_DataSet.Tables[0].Rows[0]["HAZARDOUS"]).Direction = ParameterDirection.Input;
                //insCommand.Parameters("HAZARDOUS_IN").SourceVersion = DataRowVersion.Current
                insCommand.Parameters.Add("WEIGHT_LIMIT_IN_KG_IN", M_DataSet.Tables[0].Rows[0]["WEIGHT_LIMIT_IN_KG"]).Direction = ParameterDirection.Input;
                //insCommand.Parameters("WEIGHT_LIMIT_IN_KG_IN").SourceVersion = DataRowVersion.Current
                insCommand.Parameters.Add("LEAD_UP_DT_IN", M_DataSet.Tables[0].Rows[0]["LEAD_UP_DT"]).Direction = ParameterDirection.Input;
                //insCommand.Parameters("LEAD_UP_DT_IN").SourceVersion = DataRowVersion.Current
                insCommand.Parameters.Add("LEAD_UP_MESSAGE_IN", M_DataSet.Tables[0].Rows[0]["LEAD_UP_MESSAGE"]).Direction = ParameterDirection.Input;
                //insCommand.Parameters("LEAD_UP_MESSAGE_IN").SourceVersion = DataRowVersion.Current
                insCommand.Parameters.Add("RESTRICTION_MESSAGE_IN", M_DataSet.Tables[0].Rows[0]["RESTRICTION_MESSAGE"]).Direction = ParameterDirection.Input;
                //insCommand.Parameters("RESTRICTION_MESSAGE_IN").SourceVersion = DataRowVersion.Current
                insCommand.Parameters.Add("POL_FK_IN", M_DataSet.Tables[0].Rows[0]["POL_FK"]).Direction = ParameterDirection.Input;
                //insCommand.Parameters("POL_FK_IN").SourceVersion = DataRowVersion.Current
                insCommand.Parameters.Add("POD_FK_IN", M_DataSet.Tables[0].Rows[0]["POD_FK"]).Direction = ParameterDirection.Input;
                //insCommand.Parameters("POD_FK_IN").SourceVersion = DataRowVersion.Current
                insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                //insCommand.Parameters.Add("CONFIG_MST_FK_IN", OracleDbType.NUMBER, 10, "").Direction = ParameterDirection.INPUT
                //insCommand.Parameters("CONFIG_MST_FK_IN").SourceVersion = DataRowVersion.Current
                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "BOOKING_RESTRICTION_TRN_PK").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


                var _with6 = delCommand;
                _with6.Connection = objWK.MyConnection;
                _with6.CommandType = CommandType.StoredProcedure;
                _with6.CommandText = objWK.MyUserName + ".BOOKING_RESTRICTION_TRN_PKG.BOOKING_RESTRICTION_TRN_DEL";
                var _with7 = _with6.Parameters;
                delCommand.Parameters.Add("BOOKING_RESTRICTION_PK_IN", OracleDbType.Int32, 10, "BOOKING_RESTRICTION_PK").Direction = ParameterDirection.Input;
                delCommand.Parameters["BOOKING_RESTRICTION_PK_IN"].SourceVersion = DataRowVersion.Current;
                delCommand.Parameters.Add("DELETED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                delCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                delCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
                delCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                delCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                delCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


                var _with8 = updCommand;
                _with8.Connection = objWK.MyConnection;
                _with8.CommandType = CommandType.StoredProcedure;
                _with8.CommandText = objWK.MyUserName + ".BOOKING_RESTRICTION_TRN_PKG.BOOKING_RESTRICTION_TRN_UPD";
                var _with9 = _with8.Parameters;

                updCommand.Parameters.Add("BOOKING_RESTRICTION_PK_IN", OracleDbType.Int32, 10, "BOOKING_RESTRICTION_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["BOOKING_RESTRICTION_PK_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("RESTRICTION_REF_NO_IN", OracleDbType.Varchar2, 0, "RESTRICTION_REF_NO").Direction = ParameterDirection.Input;
                updCommand.Parameters["RESTRICTION_REF_NO_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("RESTRICTION_DT_IN", OracleDbType.Date, 0, "RESTRICTION_DT").Direction = ParameterDirection.Input;
                updCommand.Parameters["RESTRICTION_DT_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("RESTRICTED_BY_FK_IN", OracleDbType.Int32, 10, "RESTRICTED_BY_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["RESTRICTED_BY_FK_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("RESTRICTION_TYPE_IN", OracleDbType.Int32, 1, "RESTRICTION_TYPE").Direction = ParameterDirection.Input;
                updCommand.Parameters["RESTRICTION_TYPE_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("EFFECTIVE_FROM_DT_IN", OracleDbType.Date, 0, "EFFECTIVE_FROM_DT").Direction = ParameterDirection.Input;
                updCommand.Parameters["EFFECTIVE_FROM_DT_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("EFFECTIVE_TO_DT_IN", OracleDbType.Date, 0, "EFFECTIVE_TO_DT").Direction = ParameterDirection.Input;
                updCommand.Parameters["EFFECTIVE_TO_DT_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("COMMODITY_MST_FK_IN", OracleDbType.Int32, 10, "COMMODITY_MST_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["COMMODITY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("IMDG_CLASS_CODE_IN", OracleDbType.Varchar2, 0, "IMDG_CLASS_CODE").Direction = ParameterDirection.Input;
                updCommand.Parameters["IMDG_CLASS_CODE_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("HAZARDOUS_IN", OracleDbType.Int32, 1, "HAZARDOUS").Direction = ParameterDirection.Input;
                updCommand.Parameters["HAZARDOUS_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("WEIGHT_LIMIT_IN_KG_IN", OracleDbType.Int32, 14, "WEIGHT_LIMIT_IN_KG").Direction = ParameterDirection.Input;
                updCommand.Parameters["WEIGHT_LIMIT_IN_KG_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("LEAD_UP_DT_IN", OracleDbType.Date, 0, "LEAD_UP_DT").Direction = ParameterDirection.Input;
                updCommand.Parameters["LEAD_UP_DT_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("LEAD_UP_MESSAGE_IN", OracleDbType.Varchar2, 200, "LEAD_UP_MESSAGE").Direction = ParameterDirection.Input;
                updCommand.Parameters["LEAD_UP_MESSAGE_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("RESTRICTION_MESSAGE_IN", OracleDbType.Varchar2, 100, "RESTRICTION_MESSAGE").Direction = ParameterDirection.Input;
                updCommand.Parameters["RESTRICTION_MESSAGE_IN"].SourceVersion = DataRowVersion.Current;
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
                var _with10 = objWK.MyDataAdapter;

                _with10.InsertCommand = insCommand;
                _with10.InsertCommand.Transaction = TRAN;
                _with10.UpdateCommand = updCommand;
                _with10.UpdateCommand.Transaction = TRAN;
                _with10.DeleteCommand = delCommand;
                _with10.DeleteCommand.Transaction = TRAN;
                RecAfct = _with10.Update(M_DataSet);
                objBkgrst_port.ConfigurationPK = ConfigurationPK;
                objBkgrst_port.CREATED_BY = CREATED_BY;
                objBkgrst_port.LAST_MODIFIED_BY = LAST_MODIFIED_BY;

                if (PortRestrictDs.Tables[0].Rows.Count > 0)
                {
                    int lngPkVal = 0;
                    if (string.IsNullOrEmpty(RefNOCheck(RestNo)))
                    {
                        lngPkVal = 0;
                    }
                    else
                    {
                        lngPkVal = Convert.ToInt32(RefNOCheck(RestNo));
                    }

                    if (lngPkVal == 0)
                    {
                        lngPK = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
                    }
                    else
                    {
                        lngPK = lngPkVal;
                    }

                    if (arrMessage.Count > 0)
                    {
                        TRAN.Rollback();
                        return arrMessage;
                    }
                    else
                    {
                        arrMessage = objBkgrst_port.Save(PortRestrictDs, TRAN, Convert.ToInt32(lngPK), Convert.ToString(M_DataSet.Tables[0].Rows[0]["RESTRICTION_REF_NO"]));
                        if (arrMessage.Count > 0)
                        {
                            if (string.Compare(arrMessage[0].ToString(), "Saved")>0)
                            {
                                TRAN.Commit();
                                return arrMessage;
                            }
                            else
                            {
                                return arrMessage;
                            }
                        }
                        //arrMessage.Add("All Data Saved Successfully")

                    }
                }
                else
                {
                    if (arrMessage.Count > 0)
                    {
                        return arrMessage;
                    }
                    else
                    {
                        TRAN.Commit();
                        arrMessage.Add("All Data Saved Successfully");
                        return arrMessage;
                    }
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
                objWK.MyCommand.Connection.Close();

            }
            return new ArrayList();
        }
        #endregion

        private string RefNOCheck(string RESTRICTION_REF_NO)
        {
            string sqlstr = null;
            string lngPk = null;
            sqlstr = "SELECT * from BOOKING_RESTRICTION_TRN where RESTRICTION_REF_NO = '" + RESTRICTION_REF_NO + "'";
            WorkFlow objWK = new WorkFlow();
            try
            {
                objWK.OpenConnection();
                lngPk = objWK.ExecuteScaler(sqlstr);
                return lngPk;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #region "generate protocol"
        public string Generate_Ref_No(Int64 ILocationId, Int64 IEmployeeId, string sPOL)
        {
            string functionReturnValue = null;
            CREATED_BY = this.CREATED_BY;
            functionReturnValue = GenerateProtocolKey("BOOKING RESTRICTION", ILocationId, IEmployeeId, DateTime.Now,"" ,"" , sPOL);
            return functionReturnValue;
        }
        #endregion

        public DataSet FetchBKRSTandSpec_Port(Int64 fkval, Int64 Pol = 0, Int64 Pod = 0)
        {
            string strSQL = null;
            strSQL +=  " select ";
            strSQL +=  " rownum SRNO,active,POL_FK,POL_ID,POD_FK,POD_ID,POD_Name from ";
            strSQL +=  " (SELECT  1 active, ";
            strSQL +=  " S.FROM_PORT_FK POL_FK,";
            strSQL +=  " POL.PORT_ID POL_ID, ";
            strSQL +=  " POL.Port_name POL_Name, ";
            strSQL +=  " S.TO_PORT_FK POD_FK, ";
            strSQL +=  " POD.PORT_ID POD_ID,";
            strSQL +=  " Pod.Port_Name POD_Name,";
            strSQL +=  " Version";
            strSQL +=  " from  ";
            strSQL +=  " Sector_Mst_Tbl S,";
            strSQL +=  " Port_Mst_Tbl POL,";
            strSQL +=  " Port_Mst_Tbl POD,";

            strSQL +=  " where  ";
            strSQL +=  " S.FROM_PORT_FK=POL.PORT_MST_PK  ";
            strSQL +=  " and S.TO_PORT_FK=POD.PORT_MST_PK";
            strSQL +=  " AND S.FROM_PORT_FK=" + fkval;
            strSQL +=  " union";

            strSQL = "SELECT ";
            strSQL +=  " 0 active,";
            strSQL +=  " S.FROM_PORT_FK POL_FK,";
            strSQL +=  " POL.PORT_ID POL_ID, ";
            strSQL +=  " POL.Port_name POL_Name,";
            strSQL +=  " S.TO_PORT_FK POD_FK,";
            strSQL +=  " POD.PORT_ID POD_ID,";
            strSQL +=  " Pod.Port_Name POD_Name,";
            strSQL +=  " bRPtrn.Version_No";

            strSQL +=  " from ";
            strSQL +=  " Sector_Mst_Tbl S,";
            strSQL +=  " Port_Mst_Tbl POL, ";
            strSQL +=  " Port_Mst_Tbl POD,";
            strSQL +=  " booking_restr_port_trn bRPtrn";
            strSQL +=  " where ";
            strSQL +=  " S.FROM_PORT_FK=POL.PORT_MST_PK ";
            strSQL +=  " and S.TO_PORT_FK=POD.PORT_MST_PK";
            strSQL +=  " and brptrn.pol_fk(+)=pol.port_mst_pk";
            //strSQL &= vbCrLf &  strCondition

            if (Pol > 0)
            {
                strSQL +=  " AND S.FROM_PORT_FK=" + Pol;
            }
            if (Pod > 0)
            {
                strSQL +=  " AND S.TO_PORT_FK=" + Pod;
            }
            if (Pol <= 0 & Pod <= 0)
            {
                strSQL +=  " AND S.FROM_PORT_FK=" + Pol;
                strSQL +=  " AND S.TO_PORT_FK=" + Pod;
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


        #region "Check Booking Restriction"

        public bool CheckBkgRestriction(long P_Voyagepk = 0, long P_POL = 0, long P_POD = 0, long P_CommodityGrp = 0, long P_Commodity = 0, string P_Hazardous = "", long P_Wt_Limit_In_Kg = 0, double P_Imdg_class_code = 0, string P_provORactual = "", string P_Booking_ID = "",
        long P_Prov_Booking_Fk = 0, long P_Booking_Fk = 0, long P_Container_Fk = 0)
        {
            try
            {
                string strSQL = null;
                WorkFlow objWF = new WorkFlow();
                clsRESTRICTION_LOG_TRN objReslog = new clsRESTRICTION_LOG_TRN();
                System.DateTime ETD = default(System.DateTime);
                System.DateTime ETA = default(System.DateTime);
                string IMDGCODE = null;
                Int32 HAZARDOUS = 0;
                OracleDataReader dr = null;
                long P_Voyage_Hdr_pk = 0;
                string strFilter = null;
                objReslog.CREATED_BY = M_CREATED_BY_FK;
                strSQL = " select t.commercial_schedule_hdr_fk from commercial_schedule_trn t where t.commercial_schedule_trn_pk=" + P_Voyagepk;
                strSQL += " and t.port_mst_fk=" + P_POL;
                P_Voyage_Hdr_pk = Convert.ToInt64(objWF.ExecuteScaler(strSQL));

                strSQL = " select ETD from commercial_schedule_trn t where t.commercial_schedule_hdr_fk=" + P_Voyage_Hdr_pk;
                strSQL += " and t.port_mst_fk=" + P_POL;
                ETD = Convert.ToDateTime(objWF.ExecuteScaler(strSQL));

                strSQL = " select ETA from commercial_schedule_trn t where t.commercial_schedule_hdr_fk=" + P_Voyage_Hdr_pk;
                strSQL += " and t.port_mst_fk=" + P_POD;
                ETA = Convert.ToDateTime(objWF.ExecuteScaler(strSQL));

                if (P_Commodity > 0)
                {
                    strSQL = " select IMDG_CLASS_CODE from Commodity_MST_TBL commodity where ";
                    strSQL += " commodity.commodity_mst_pk = " + P_Commodity;
                    try
                    {
                        IMDGCODE = Convert.ToString(objWF.ExecuteScaler(strSQL));
                    }
                    catch (Exception ex)
                    {
                        IMDGCODE = "0";
                    }
                }
                if (P_Commodity > 0)
                {
                    strSQL = " select HAZARDOUS from Commodity_MST_TBL commodity where ";
                    strSQL += " commodity.commodity_mst_pk = " + P_Commodity;
                    HAZARDOUS = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
                }
                //common condition 
                if (P_Commodity > 0)
                {
                    strFilter += " and BRT.commodity_mst_fk= " + P_Commodity;
                    if (IMDGCODE.Trim().Length > 0)
                    {
                        strFilter += " and BRT.imdg_class_code= " + IMDGCODE;
                    }
                    else
                    {
                        strFilter += " and (BRT.imdg_class_code is null or BRT.imdg_class_code=0)";
                    }
                }
                else
                {
                    strFilter += " and BRT.commodity_mst_fk is null ";
                    strFilter += " and (BRT.imdg_class_code is null or BRT.imdg_class_code=0) ";
                }
                if (P_Wt_Limit_In_Kg > 0)
                {
                    strFilter += " and BRT.weight_limit_in_kg= " + P_Wt_Limit_In_Kg;
                }
                else
                {
                    strFilter += " and BRT.weight_limit_in_kg is null ";
                }
                strFilter += " and BRT.hazardous=" + HAZARDOUS;

                // General Restriction without pol, pod
                strSQL = " select distinct BRT.* from booking_restriction_trn BRT ";
                strSQL += "where ";
                strSQL += "BRT.pol_fk Is null and BRT.pod_fk is null ";
                strSQL += strFilter;
                strSQL += " and ( ( ( to_date(to_char(BRT.effective_from_dt,'dd-Mon-yyyy'),'dd-Mon-yyyy') <= '" + ETD.ToString("{dd/MMM/yyyy}") + "'";
                strSQL += "   AND  to_date(to_char(BRT.effective_to_dt,'dd-Mon-yyyy'),'dd-Mon-yyyy') >=  '" + ETD.ToString("{dd/MMM/yyyy}") + "')";
                strSQL += "   OR (to_date(to_char(BRT.effective_from_dt,'dd-Mon-yyyy'),'dd-Mon-yyyy') <= '" + ETD.ToString("{dd/MMM/yyyy}") + "'";
                strSQL += "   and BRT.effective_to_dt is null))";
                strSQL += " OR ((to_date(to_char(BRT.effective_from_dt,'dd-Mon-yyyy'),'dd-Mon-yyyy') <= '" + ETA.ToString("{dd/MMM/yyyy}") + "'";
                strSQL += "   AND  to_date(to_char(BRT.effective_to_dt,'dd-Mon-yyyy'),'dd-Mon-yyyy') >=  '" + ETA.ToString("{dd/MMM/yyyy}") + "')";
                strSQL += "   OR (to_date(to_char(BRT.effective_from_dt,'dd-Mon-yyyy'),'dd-Mon-yyyy') <= '" + ETA.ToString("{dd/MMM/yyyy}") + "'";
                strSQL += "   and BRT.effective_to_dt is null)) )";
                strSQL += " and BRT.BOOKING_RESTRICTION_PK not in (select RPT.BOOKING_RESTRICTION_FK from booking_restr_port_trn RPT )";
                dr = objWF.GetDataReader(strSQL);
                while (dr.Read())
                {
                    objReslog.Insert(P_provORactual, P_Booking_ID, P_Prov_Booking_Fk, 0, 0, Convert.ToInt64(dr["BOOKING_RESTRICTION_PK"]), Convert.ToInt64(dr["RESTRICTION_TYPE"]), Convert.ToString((Convert.ToInt64(dr["RESTRICTION_TYPE"]) == 0 ? "B" : "P")));
                }
                dr = null;

                // Restriction on pol
                strSQL = " select * from booking_restriction_trn BRT ";
                strSQL += " where ";
                strSQL += " BRT.pol_fk = " + P_POL + " and BRT.pod_fk is null ";
                strSQL += strFilter;
                strSQL += " and (to_char(BRT.effective_from_dt,'dd/Mon/YYYY') = '" + ETD.ToString("{dd/MMM/yyyy}") + "'";
                strSQL += " or (to_date(to_char(BRT.effective_from_dt,'dd/Mon/YYYY'),'dd/Mon/YYYY') < '" + ETD.ToString("{dd/MMM/yyyy}") + "'";
                strSQL += " and BRT.effective_from_dt is null))";
                strSQL += " and BRT.BOOKING_RESTRICTION_PK not in (select RPT.BOOKING_RESTRICTION_FK from booking_restr_port_trn RPT )";
                dr = objWF.GetDataReader(strSQL);
                while (dr.Read())
                {
                    objReslog.Insert(P_provORactual, P_Booking_ID, P_Prov_Booking_Fk, 0, 0, Convert.ToInt64(dr["BOOKING_RESTRICTION_PK"]), Convert.ToInt64(dr["RESTRICTION_TYPE"]), Convert.ToString((Convert.ToInt64(dr["RESTRICTION_TYPE"]) == 0 ? "B" : "P")));
                }
                dr = null;

                // Restriction on pod 
                strSQL = " select * from booking_restriction_trn BRT ";
                strSQL += " where ";
                strSQL += " BRT.pol_fk Is Null and BRT.pod_fk =" + P_POD;
                strSQL += strFilter;
                strSQL += " and (to_char(BRT.effective_from_dt,'dd/Mon/YYYY') = '" + ETA.ToString("{dd/MMM/yyyy}") + "'";
                strSQL += " or (to_date(to_char(BRT.effective_from_dt,'dd/Mon/YYYY'),'dd/Mon/YYYY') < '" + ETA.ToString("{dd/MMM/yyyy}") + "'";
                strSQL += " and BRT.effective_from_dt is null))";
                strSQL += " and BRT.BOOKING_RESTRICTION_PK not in (select RPT.BOOKING_RESTRICTION_FK from booking_restr_port_trn RPT )";
                dr = objWF.GetDataReader(strSQL);
                while (dr.Read())
                {
                    objReslog.Insert(P_provORactual, P_Booking_ID, P_Prov_Booking_Fk, 0, 0, Convert.ToInt64(dr["BOOKING_RESTRICTION_PK"]), Convert.ToInt64(dr["RESTRICTION_TYPE"]), Convert.ToString((Convert.ToInt64(dr["RESTRICTION_TYPE"]) == 0 ? "B" : "P")));
                }
                dr = null;

                // Restriction with pol , pod in master
                strSQL = " select * from booking_restriction_trn BRT ";
                strSQL += " where ";
                strSQL += " BRT.pol_fk =" + P_POL + " and BRT.pod_fk =" + P_POD;
                strSQL += strFilter;
                strSQL += " and ( ( ( to_date(to_char(BRT.effective_from_dt,'dd-Mon-yyyy'),'dd-Mon-yyyy') <= '" + ETD.ToString("{dd/MMM/yyyy}") + "'";
                strSQL += "   AND  to_date(to_char(BRT.effective_to_dt,'dd-Mon-yyyy'),'dd-Mon-yyyy') >=  '" + ETD.ToString("{dd/MMM/yyyy}") + "')";
                strSQL += "   OR (to_date(to_char(BRT.effective_from_dt,'dd-Mon-yyyy'),'dd-Mon-yyyy') <= '" + ETD.ToString("{dd/MMM/yyyy}") + "'";
                strSQL += "   and BRT.effective_to_dt is null))";
                strSQL += " OR ((to_date(to_char(BRT.effective_from_dt,'dd-Mon-yyyy'),'dd-Mon-yyyy') <= '" + ETA.ToString("{dd/MMM/yyyy}") + "'";
                strSQL += "   AND  to_date(to_char(BRT.effective_to_dt,'dd-Mon-yyyy'),'dd-Mon-yyyy') >=  '" + ETA.ToString("{dd/MMM/yyyy}") + "')";
                strSQL += "   OR (to_date(to_char(BRT.effective_from_dt,'dd-Mon-yyyy'),'dd-Mon-yyyy') <= '" + ETA.ToString("{dd/MMM/yyyy}") + "'";
                strSQL += "   and BRT.effective_to_dt is null)) )";
                dr = objWF.GetDataReader(strSQL);
                while (dr.Read())
                {
                    objReslog.Insert(P_provORactual, P_Booking_ID, P_Prov_Booking_Fk, 0, 0, Convert.ToInt64(dr["BOOKING_RESTRICTION_PK"]), Convert.ToInt64(dr["RESTRICTION_TYPE"]), Convert.ToString((Convert.ToInt64(dr["RESTRICTION_TYPE"]) == 0 ? "B" : "P")));
                }
                dr = null;
                // Restriction with pol , pod in child

                strSQL = " select * from booking_restriction_trn BRT, ";
                strSQL += " booking_restr_port_trn BRPT";
                strSQL += " where BRT.BOOKING_RESTRICTION_PK = BRPT.BOOKING_RESTRICTION_PORT_PK ";
                strSQL += " and BRPT.POL_FK = " + P_POL;
                strSQL += " and BRPT.Pod_Fk = " + P_POD;
                strSQL += " and BRT.pol_fk is null and BRT.pod_fk is null ";
                strSQL += strFilter;
                strSQL += " and ( ( ( to_date(to_char(BRT.effective_from_dt,'dd-Mon-yyyy'),'dd-Mon-yyyy') <= '" + ETD.ToString("{dd/MMM/yyyy}") + "'";
                strSQL += "   AND  to_date(to_char(BRT.effective_to_dt,'dd-Mon-yyyy'),'dd-Mon-yyyy') >=  '" + ETD.ToString("{dd/MMM/yyyy}") + "')";
                strSQL += "   OR (to_date(to_char(BRT.effective_from_dt,'dd-Mon-yyyy'),'dd-Mon-yyyy') <= '" + ETD.ToString("{dd/MMM/yyyy}") + "'";
                strSQL += "   and BRT.effective_to_dt is null))";
                strSQL += " OR ((to_date(to_char(BRT.effective_from_dt,'dd-Mon-yyyy'),'dd-Mon-yyyy') <= '" + ETA.ToString("{dd/MMM/yyyy}") + "'";
                strSQL += "   AND  to_date(to_char(BRT.effective_to_dt,'dd-Mon-yyyy'),'dd-Mon-yyyy') >=  '" + ETA.ToString("{dd/MMM/yyyy}") + "')";
                strSQL += "   OR (to_date(to_char(BRT.effective_from_dt,'dd-Mon-yyyy'),'dd-Mon-yyyy') <= '" + ETA.ToString("{dd/MMM/yyyy}") + "'";
                strSQL += "   and BRT.effective_to_dt is null)) )";
                dr = objWF.GetDataReader(strSQL);
                while (dr.Read())
                {
                    objReslog.Insert(P_provORactual, P_Booking_ID, P_Prov_Booking_Fk, 0, 0, Convert.ToInt64(dr["BOOKING_RESTRICTION_PK"]), Convert.ToInt64(dr["RESTRICTION_TYPE"]), Convert.ToString((Convert.ToInt64(dr["RESTRICTION_TYPE"]) == 0 ? "B" : "P")));
                }
                dr = null;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return false;
        }
        #endregion

    }
}