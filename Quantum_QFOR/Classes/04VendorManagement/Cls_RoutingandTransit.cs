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
using System.Text;

namespace Quantum_QFOR
{
    public class Cls_RoutingandTransit : CommonFeatures
    {
        #region "Fetch"

        public DataSet Fetch(int Mode, string Pk, string refno, int btype)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder strsql = new StringBuilder();
            try
            {
                if (string.IsNullOrEmpty(Pk) | Pk == null)
                {
                    Pk = "0";
                }
                strsql.Append(" SELECT Routing.Routing_Pk,Routing.Operator_Mst_Fk,");
                strsql.Append(" Routing.Service_Name,Routing.Routing,Routing.Transit_Time,Routing.Vessel_Flight_No,Routing.Sunday,");
                strsql.Append(" Routing.Monday,Routing.Tuesday,Routing.Wednesday,Routing.Thuresday,Routing.Friday,Routing.Saturday,0 as del,Routing.version_no ");
                if (string.IsNullOrEmpty(refno) | refno == null)
                {
                    strsql.Append(" FROM ROUTING_TRANSIT_TBL Routing where Routing.Routing_Del<>1 and Routing.Routing_Pk in(" + Pk.Replace("'", "") + ")");
                    strsql.Append(" and  Routing.BIZ_TYPE = " + btype);
                }
                else
                {
                    strsql.Append(" FROM ROUTING_TRANSIT_TBL Routing where Routing.Routing_Del<>1 and routing.operator_mst_fk in(" + refno.Replace("'", "") + ")");
                    strsql.Append(" and  Routing.BIZ_TYPE = " + btype);
                }
                return objWF.GetDataSet(strsql.ToString());
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

        #endregion "Fetch"

        #region "save"

        public ArrayList Save(DataSet M_DataSet, int btype)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            Int32 RecAfct = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            OracleCommand delCommand = new OracleCommand();
            try
            {
                var _with1 = insCommand;
                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".ROUTING_TRANSIT_TBL_PKG.ROUTING_TRANSIT_TBL_INS";
                var _with2 = _with1.Parameters;
                insCommand.Parameters.Add("SERVICE_IN", OracleDbType.Varchar2, 50, "Service_Name").Direction = ParameterDirection.Input;
                insCommand.Parameters["SERVICE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("Operator_pk", OracleDbType.Int32, 5, "Operator_Mst_Fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["Operator_pk"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("BIZ", btype).Direction = ParameterDirection.Input;
                insCommand.Parameters["BIZ"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("ROUTING_IN", OracleDbType.Varchar2, 50, "Routing").Direction = ParameterDirection.Input;
                insCommand.Parameters["ROUTING_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("TRANSIT_IN", OracleDbType.Varchar2, 15, "Transit_Time").Direction = ParameterDirection.Input;
                insCommand.Parameters["TRANSIT_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("VESSEL_IN", OracleDbType.Varchar2, 20, "Vessel_Flight_No").Direction = ParameterDirection.Input;
                insCommand.Parameters["VESSEL_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("SUN_IN", OracleDbType.Int32, 1, "Sunday").Direction = ParameterDirection.Input;
                insCommand.Parameters["SUN_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("MON_IN", OracleDbType.Int32, 1, "Monday").Direction = ParameterDirection.Input;
                insCommand.Parameters["MON_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("TUES_IN", OracleDbType.Int32, 1, "Tuesday").Direction = ParameterDirection.Input;
                insCommand.Parameters["TUES_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("WED_IN", OracleDbType.Int32, 1, "Wednesday").Direction = ParameterDirection.Input;
                insCommand.Parameters["WED_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("THUR_IN", OracleDbType.Int32, 1, "Thuresday").Direction = ParameterDirection.Input;
                insCommand.Parameters["THUR_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("FRI_IN", OracleDbType.Int32, 1, "Friday").Direction = ParameterDirection.Input;
                insCommand.Parameters["FRI_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("SAT_IN", OracleDbType.Int32, 1, "Saturday").Direction = ParameterDirection.Input;
                insCommand.Parameters["SAT_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CREATED_BY_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                insCommand.Parameters["CREATED_BY_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("MODIFIED_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                insCommand.Parameters["MODIFIED_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("DEL_IN", OracleDbType.Int32, 1, "del").Direction = ParameterDirection.Input;
                insCommand.Parameters["DEL_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "Routing_Pk").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                var _with3 = updCommand;
                _with3.Connection = objWK.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".ROUTING_TRANSIT_TBL_PKG.ROUTING_TRANSIT_TBL_UPD";
                var _with4 = _with3.Parameters;

                updCommand.Parameters.Add("PK", OracleDbType.Int32, 10, "Routing_Pk").Direction = ParameterDirection.Input;
                updCommand.Parameters["PK"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("SERVICE", OracleDbType.Varchar2, 50, "Service_Name").Direction = ParameterDirection.Input;
                updCommand.Parameters["SERVICE"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("ROUTINGS", OracleDbType.Varchar2, 50, "Routing").Direction = ParameterDirection.Input;
                updCommand.Parameters["ROUTINGS"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("TRANSIT", OracleDbType.Varchar2, 15, "Transit_Time").Direction = ParameterDirection.Input;
                updCommand.Parameters["TRANSIT"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("VESSEL", OracleDbType.Varchar2, 20, "Vessel_Flight_No").Direction = ParameterDirection.Input;
                updCommand.Parameters["VESSEL"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("SUN", OracleDbType.Int32, 1, "Sunday").Direction = ParameterDirection.Input;
                updCommand.Parameters["SUN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("MON", OracleDbType.Int32, 1, "Monday").Direction = ParameterDirection.Input;
                updCommand.Parameters["MON"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("TUES", OracleDbType.Int32, 1, "Tuesday").Direction = ParameterDirection.Input;
                updCommand.Parameters["TUES"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("WED", OracleDbType.Int32, 1, "Wednesday").Direction = ParameterDirection.Input;
                updCommand.Parameters["WED"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("THUR", OracleDbType.Int32, 1, "Thuresday").Direction = ParameterDirection.Input;
                updCommand.Parameters["THUR"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("FRI", OracleDbType.Int32, 1, "Friday").Direction = ParameterDirection.Input;
                updCommand.Parameters["FRI"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("SAT", OracleDbType.Int32, 1, "Saturday").Direction = ParameterDirection.Input;
                updCommand.Parameters["SAT"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("VERSION", OracleDbType.Int32, 1, "version_no").Direction = ParameterDirection.Input;
                updCommand.Parameters["VERSION"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("MODIFIED_FK", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                updCommand.Parameters["MODIFIED_FK"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("DEL", OracleDbType.Int32, 1, "del").Direction = ParameterDirection.Input;
                updCommand.Parameters["DEL"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "Service_Name").Direction = ParameterDirection.Output;
                updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


                var _with5 = objWK.MyDataAdapter;
                _with5.InsertCommand = insCommand;
                _with5.InsertCommand.Transaction = TRAN;
                _with5.UpdateCommand = updCommand;
                _with5.UpdateCommand.Transaction = TRAN;
                RecAfct = _with5.Update(M_DataSet);

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

        #endregion "save"

        #region "UpdatePk"

        public void UpdatePk(int OperatorPk, int Biz)
        {
            WorkFlow objWK = new WorkFlow();
            StringBuilder strsql = new StringBuilder();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            objWK.MyCommand.Transaction = TRAN;
            objWK.MyCommand.Parameters.Clear();
            try
            {
                var _with6 = objWK.MyCommand;
                _with6.CommandType = CommandType.StoredProcedure;
                _with6.CommandText = objWK.MyUserName + ".ROUTING_TRANSIT_TBL_PKG.ROUTING_TRANSIT_TBL_PK_UPD";
                _with6.Parameters.Add("OPERATOR_PK", OperatorPk).Direction = ParameterDirection.Input;
                _with6.Parameters.Add("Biz_types", Biz).Direction = ParameterDirection.Input;
                _with6.ExecuteNonQuery();
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
        }

        #endregion "UpdatePk"
    }
}