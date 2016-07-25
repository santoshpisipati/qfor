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
using System.Web;

namespace Quantum_QFOR
{
    public class Cls_BookingLoadConfirm : CommonFeatures
    {
        #region " Sea Print Function For Pending Bookings for Load Confirm "

        public DataSet FetchLoadSeaReport(Int32 LocFk = 0, string CustName = "", string VslName = "", string Voyage = "", string FromDt = "", string ToDt = "", string ETDDt = "")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strSQL = null;
            try
            {
                sb.Append("SELECT DISTINCT BKG.BOOKING_MST_PK,");
                sb.Append("       BKG.BOOKING_REF_NO,");
                sb.Append("       JCSET.JOB_CARD_TRN_PK,");
                sb.Append("       JCSET.JOBCARD_REF_NO, ");
                sb.Append("       TO_DATE(BKG.SHIPMENT_DATE, 'DD/MM/YYYY') SHIPMENT_DATE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       CASE WHEN VVT.VESSEL_NAME IS NULL THEN");
                sb.Append("         VVT.VESSEL_NAME");
                sb.Append("        ELSE");
                sb.Append("          (VVT.VESSEL_NAME || '/' || VT.VOYAGE)");
                sb.Append("        END AS \"VESSEL_FLIGHT\",");
                sb.Append("      TO_CHAR(JCSET.ETD_DATE, DATETIMEFORMAT24) ETD_DATE,");
                sb.Append("      TO_CHAR(JCSET.ETA_DATE, DATETIMEFORMAT24) ETA_DATE,");
                sb.Append("      DECODE(BKG.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                sb.Append("      (SELECT rowtocol('(SELECT CTMT.CONTAINER_TYPE_MST_ID FROM CONTAINER_TYPE_MST_TBL CTMT");
                sb.Append("      WHERE CTMT.CONTAINER_TYPE_MST_PK IN (SELECT JCONT.CONTAINER_TYPE_MST_FK FROM JOB_TRN_CONT    JCONT");
                sb.Append("      WHERE JCONT.JOB_CARD_TRN_FK =' ||  JCSET.JOB_CARD_TRN_PK || '))') FROM DUAL) CONTAINER_TYPE_MST_ID,");
                sb.Append("       CGMT.COMMODITY_GROUP_CODE,");
                sb.Append("       JCSET.JOBCARD_DATE ");
                sb.Append("  FROM BOOKING_MST_TBL         BKG,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT,");
                sb.Append("        VESSEL_VOYAGE_TBL       VVT,");
                sb.Append("       VESSEL_VOYAGE_TRN       VT,");
                sb.Append("       JOB_CARD_TRN    JCSET,");
                sb.Append("       JOB_TRN_CONT    JTSEC");
                sb.Append(" WHERE POL.PORT_MST_PK(+) = BKG.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK(+) = BKG.PORT_MST_POD_FK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK(+) = BKG.CUST_CUSTOMER_MST_FK");
                sb.Append("   AND CGMT.COMMODITY_GROUP_PK(+) = BKG.COMMODITY_GROUP_FK");
                sb.Append("   AND JCSET.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
                sb.Append("   AND JCSET.VOYAGE_TRN_FK = VT.VOYAGE_TRN_PK(+)");
                sb.Append("   AND VVT.VESSEL_VOYAGE_TBL_PK(+) = VT.VESSEL_VOYAGE_TBL_FK");
                sb.Append("   AND JCSET.JOB_CARD_TRN_PK=JTSEC.JOB_CARD_TRN_FK");
                sb.Append("   AND JTSEC.LOAD_DATE IS NULL");
                sb.Append("  AND POL.LOCATION_MST_FK = ");
                sb.Append(" (SELECT L.LOCATION_MST_PK");
                sb.Append("  FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");

                if (!string.IsNullOrEmpty(CustName))
                {
                    sb.Append(" And CMT.CUSTOMER_NAME = '" + CustName + "'");
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    sb.Append(" And VVT.VESSEL_NAME = '" + VslName + "'");
                }
                if (!string.IsNullOrEmpty(Voyage))
                {
                    sb.Append(" And VT.VOYAGE = '" + Voyage + "'");
                }
                if (!string.IsNullOrEmpty(FromDt))
                {
                    sb.Append(" And TO_DATE(JCSET.JOBCARD_DATE, DATEFORMAT) >= TO_DATE('" + FromDt + "',dateformat) ");
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    sb.Append(" And TO_DATE(JCSET.JOBCARD_DATE, DATEFORMAT) <= TO_DATE('" + ToDt + "',dateformat) ");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append(" And TO_DATE(JCSET.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat) ");
                }
                sb.Append("    ORDER BY  JCSET.JOBCARD_DATE DESC, JCSET.JOBCARD_REF_NO DESC");
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException SqlExp)
            {
                throw SqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion " Sea Print Function For Pending Bookings for Load Confirm "

        #region " Sea Grid Function For Pending Bookings for Load Confirm "

        public DataSet FetchLoadSeaGrid(Int32 LocFk = 0, Int32 OperatorFk = 0, string CustName = "", string VslName = "", string Voyage = "", string FromDt = "", string ToDt = "", string ETDDt = "", Int32 flag = 0, Int32 CurrentPage = 0,
        Int32 TotalPage = 0, Int32 ExportExcel = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();

            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with1 = objWF.MyDataAdapter;
                _with1.SelectCommand = new OracleCommand();
                _with1.SelectCommand.Connection = objWF.MyConnection;
                _with1.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_LOAD_CONFIRM_PKG.FETCH_LOAD_CONFIRM";
                _with1.SelectCommand.CommandType = CommandType.StoredProcedure;

                _with1.SelectCommand.Parameters.Add("LOCATION_MST_PK_IN", LocFk).Direction = ParameterDirection.Input;
                //_with1.SelectCommand.Parameters.Add("OPERATOR_MST_PK_IN", (OperatorFk <= 0 ? "" : OperatorFk)).Direction = ParameterDirection.Input;
                _with1.SelectCommand.Parameters.Add("CUSTOMER_NAME_IN", (string.IsNullOrEmpty(CustName) ? "" : CustName)).Direction = ParameterDirection.Input;
                _with1.SelectCommand.Parameters.Add("VESSEL_NAME_IN", (string.IsNullOrEmpty(VslName) ? "" : VslName)).Direction = ParameterDirection.Input;
                _with1.SelectCommand.Parameters.Add("VOYAGE_IN", (string.IsNullOrEmpty(Voyage) ? "" : Voyage)).Direction = ParameterDirection.Input;
                _with1.SelectCommand.Parameters.Add("FROM_DATE_IN", (string.IsNullOrEmpty(FromDt) ? "" : FromDt)).Direction = ParameterDirection.Input;
                _with1.SelectCommand.Parameters.Add("TO_DATE_IN", (string.IsNullOrEmpty(ToDt) ? "" : ToDt)).Direction = ParameterDirection.Input;
                _with1.SelectCommand.Parameters.Add("ETD_DATE_IN", (string.IsNullOrEmpty(ETDDt) ? "" : ETDDt)).Direction = ParameterDirection.Input;
                _with1.SelectCommand.Parameters.Add("FLAG_IN", flag).Direction = ParameterDirection.Input;
                _with1.SelectCommand.Parameters.Add("EXPORT_EXCEL_IN", ExportExcel).Direction = ParameterDirection.Input;

                _with1.SelectCommand.Parameters.Add("CURRENT_PAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with1.SelectCommand.Parameters.Add("MASTER_PAGE_SIZE_IN", RecordsPerPage).Direction = ParameterDirection.Input;
                _with1.SelectCommand.Parameters.Add("TOTAL_PAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
                _with1.SelectCommand.Parameters.Add("LOAD_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                _with1.Fill(ds);

                //TotalPage = _with1.SelectCommand.Parameters["TOTAL_PAGE_IN"].Value;
                //CurrentPage = _with1.SelectCommand.Parameters["CURRENT_PAGE_IN"].Value;

                return ds;
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

        #endregion " Sea Grid Function For Pending Bookings for Load Confirm "

        #region "Pending Act Search and Update"

        #region "Fetch Pending Activites"

        public DataSet FetchPendActGrid(Int32 LocFk = 0, Int32 OperatorFk = 0, string CustName = "", string VslName = "", string Voyage = "", string FromDt = "", string ToDt = "", string ETDDt = "", Int32 flag = 0, Int32 CurrentPage = 0,
        Int32 TotalPage = 0, Int32 ExportExcel = 0, Int32 Biz_Type = 0, Int32 Process_Type = 0, string Pending_Act = "", string POLPk = "", string PODPK = "", string JOB_PK = "", string BOOK_PK = "")
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();

            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with3 = objWF.MyDataAdapter;
                _with3.SelectCommand = new OracleCommand();
                _with3.SelectCommand.Connection = objWF.MyConnection;
                _with3.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_LOAD_CONFIRM_PKG.FETCH_PENDING_ACT";
                _with3.SelectCommand.CommandType = CommandType.StoredProcedure;

                _with3.SelectCommand.Parameters.Add("LOCATION_MST_PK_IN", LocFk).Direction = ParameterDirection.Input;
                _with3.SelectCommand.Parameters.Add("OPERATOR_MST_PK_IN", (OperatorFk <= 0 ? 0 : OperatorFk)).Direction = ParameterDirection.Input;
                _with3.SelectCommand.Parameters.Add("CUSTOMER_NAME_IN", (string.IsNullOrEmpty(CustName) ? "" : CustName)).Direction = ParameterDirection.Input;
                _with3.SelectCommand.Parameters.Add("VESSEL_NAME_IN", (string.IsNullOrEmpty(VslName) ? "" : VslName)).Direction = ParameterDirection.Input;
                _with3.SelectCommand.Parameters.Add("VOYAGE_IN", (string.IsNullOrEmpty(Voyage) ? "" : Voyage)).Direction = ParameterDirection.Input;
                _with3.SelectCommand.Parameters.Add("FROM_DATE_IN", (string.IsNullOrEmpty(FromDt) ? "" : FromDt)).Direction = ParameterDirection.Input;
                _with3.SelectCommand.Parameters.Add("TO_DATE_IN", (string.IsNullOrEmpty(ToDt) ? "" : ToDt)).Direction = ParameterDirection.Input;
                _with3.SelectCommand.Parameters.Add("ETD_DATE_IN", (string.IsNullOrEmpty(ETDDt) ? "" : ETDDt)).Direction = ParameterDirection.Input;
                _with3.SelectCommand.Parameters.Add("FLAG_IN", flag).Direction = ParameterDirection.Input;
                _with3.SelectCommand.Parameters.Add("EXPORT_EXCEL_IN", ExportExcel).Direction = ParameterDirection.Input;

                _with3.SelectCommand.Parameters.Add("BIZ_IN", (Biz_Type <= 0 ? 0 : Biz_Type)).Direction = ParameterDirection.Input;
                _with3.SelectCommand.Parameters.Add("PROCESS_IN", (Process_Type <= 0 ? 0 : Process_Type)).Direction = ParameterDirection.Input;
                _with3.SelectCommand.Parameters.Add("PENDING_ACT_IN", (string.IsNullOrEmpty(Pending_Act) ? "" : Pending_Act)).Direction = ParameterDirection.Input;
                _with3.SelectCommand.Parameters.Add("POLPK_IN", (string.IsNullOrEmpty(POLPk) ? "" : POLPk)).Direction = ParameterDirection.Input;
                _with3.SelectCommand.Parameters.Add("PODPK_IN", (string.IsNullOrEmpty(PODPK) ? "" : PODPK)).Direction = ParameterDirection.Input;
                _with3.SelectCommand.Parameters.Add("JOBPK_IN", (string.IsNullOrEmpty(JOB_PK) ? "" : JOB_PK)).Direction = ParameterDirection.Input;
                _with3.SelectCommand.Parameters.Add("BOOKPK_IN", (string.IsNullOrEmpty(BOOK_PK) ? "" : BOOK_PK)).Direction = ParameterDirection.Input;

                _with3.SelectCommand.Parameters.Add("CURRENT_PAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with3.SelectCommand.Parameters.Add("MASTER_PAGE_SIZE_IN", RecordsPerPage).Direction = ParameterDirection.Input;
                _with3.SelectCommand.Parameters.Add("TOTAL_PAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
                _with3.SelectCommand.Parameters.Add("LOAD_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                _with3.Fill(ds);

                TotalPage = Convert.ToInt32(_with3.SelectCommand.Parameters["TOTAL_PAGE_IN"].Value);
                CurrentPage = Convert.ToInt32(_with3.SelectCommand.Parameters["CURRENT_PAGE_IN"].Value);

                return ds;
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

        #endregion "Fetch Pending Activites"

        #region "Update Pending Activities"

        public ArrayList UpdatePendingActConfirm(DataSet ds, int BizType, int ProcessType, string Pending)
        {
            WorkFlow ObjWk = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            OracleTransaction TRAN = null;
            int nRecAfct = 0;
            string strSql = null;
            try
            {
                ObjWk.OpenConnection();
                TRAN = ObjWk.MyConnection.BeginTransaction();
                var _with4 = objCommand;
                _with4.Connection = ObjWk.MyConnection;
                _with4.CommandType = CommandType.Text;
                _with4.Transaction = TRAN;
                ///''''''''''''''''''Sea
                //Sea-Exp
                if (BizType == 2 & ProcessType == 1)
                {
                    if (!string.IsNullOrEmpty(Pending))
                    {
                        if (Convert.ToInt32(Pending) == 1)
                        {
                            for (int i = 0; i <= ds.Tables[0].Rows.Count - 1; i++)
                            {
                                if (!string.IsNullOrEmpty(ds.Tables[0].Rows[i]["PickUp_Date"].ToString()))
                                {
                                    strSql = " UPDATE JOB_TRN_CONT SET PICKUP_DATE = TO_DATE('" + ds.Tables[0].Rows[i]["PickUp_Date"] + "','DD/MM/YYYY HH24:MI:SS') ";
                                    strSql += " WHERE JOB_TRN_CONT_PK =" + ds.Tables[0].Rows[i]["JOB_TRN_CONT_PK"];

                                    _with4.CommandText = strSql;
                                    nRecAfct = nRecAfct + _with4.ExecuteNonQuery();
                                }
                            }
                        }
                    }
                    if (Convert.ToInt32(Pending) == 2)
                    {
                        for (int i = 0; i <= ds.Tables[0].Rows.Count - 1; i++)
                        {
                            if (!string.IsNullOrEmpty(ds.Tables[0].Rows[i]["GateIn_Date"].ToString()))
                            {
                                strSql = " UPDATE JOB_TRN_CONT SET GATEIN_DATE = TO_DATE('" + ds.Tables[0].Rows[i]["GateIn_Date"] + "','DD/MM/YYYY HH24:MI:SS') ";
                                strSql += " WHERE JOB_TRN_CONT_PK =" + ds.Tables[0].Rows[i]["JOB_TRN_CONT_PK"];

                                _with4.CommandText = strSql;
                                nRecAfct = nRecAfct + _with4.ExecuteNonQuery();
                            }
                        }
                    }
                    if (Convert.ToInt32(Pending) == 3)
                    {
                        for (int i = 0; i <= ds.Tables[0].Rows.Count - 1; i++)
                        {
                            if (!string.IsNullOrEmpty(ds.Tables[0].Rows[i]["Loading_Date"].ToString()))
                            {
                                strSql = " UPDATE JOB_TRN_CONT SET LOAD_DATE = TO_DATE('" + ds.Tables[0].Rows[i]["Loading_Date"] + "','DD/MM/YYYY HH24:MI:SS') ";
                                strSql += " WHERE JOB_TRN_CONT_PK =" + ds.Tables[0].Rows[i]["JOB_TRN_CONT_PK"];

                                _with4.CommandText = strSql;
                                nRecAfct = nRecAfct + _with4.ExecuteNonQuery();
                            }
                        }
                    }
                }

                // Sea Imp
                if (BizType == 2 & ProcessType == 2)
                {
                    if (!string.IsNullOrEmpty(Pending))
                    {
                        if (Convert.ToInt32(Pending) == 1)
                        {
                            for (int i = 0; i <= ds.Tables[0].Rows.Count - 1; i++)
                            {
                                if (!string.IsNullOrEmpty(ds.Tables[0].Rows[i]["PickUp_Date"].ToString()))
                                {
                                    strSql = " UPDATE JOB_TRN_CONT SET LOAD_DATE = TO_DATE('" + ds.Tables[0].Rows[i]["PickUp_Date"] + "','DD/MM/YYYY HH24:MI:SS') ";
                                    strSql += " WHERE JOB_TRN_CONT_PK =" + ds.Tables[0].Rows[i]["JOB_TRN_CONT_PK"];

                                    _with4.CommandText = strSql;
                                    nRecAfct = nRecAfct + _with4.ExecuteNonQuery();
                                }
                            }
                        }
                    }
                    if (Convert.ToInt32(Pending) == 2)
                    {
                        for (int i = 0; i <= ds.Tables[0].Rows.Count - 1; i++)
                        {
                            if (!string.IsNullOrEmpty(ds.Tables[0].Rows[i]["GateIn_Date"].ToString()))
                            {
                                strSql = " UPDATE JOB_TRN_CONT SET GATEOUT_DATE = TO_DATE('" + ds.Tables[0].Rows[i]["GateIn_Date"] + "','DD/MM/YYYY HH24:MI:SS') ";
                                strSql += " WHERE JOB_TRN_CONT_PK =" + ds.Tables[0].Rows[i]["JOB_TRN_CONT_PK"];

                                _with4.CommandText = strSql;
                                nRecAfct = nRecAfct + _with4.ExecuteNonQuery();
                            }
                        }
                    }
                }
                ///''''''''''''''''''''''''''''''''''''''''''''''''Air

                //Air Exp
                if (BizType == 1 & ProcessType == 1)
                {
                    if (!string.IsNullOrEmpty(Pending))
                    {
                        if (Convert.ToInt32(Pending) == 1)
                        {
                            for (int i = 0; i <= ds.Tables[0].Rows.Count - 1; i++)
                            {
                                if (!string.IsNullOrEmpty(ds.Tables[0].Rows[i]["PickUp_Date"].ToString()))
                                {
                                    strSql = " UPDATE JOB_TRN_CONT SET PICKUP_DATE = TO_DATE('" + ds.Tables[0].Rows[i]["PickUp_Date"] + "','DD/MM/YYYY HH24:MI:SS') ";
                                    strSql += " WHERE JOB_TRN_CONT_PK =" + ds.Tables[0].Rows[i]["JOB_TRN_CONT_PK"];

                                    _with4.CommandText = strSql;
                                    nRecAfct = nRecAfct + _with4.ExecuteNonQuery();
                                }
                            }
                        }
                    }
                    if (Convert.ToInt32(Pending) == 2)
                    {
                        for (int i = 0; i <= ds.Tables[0].Rows.Count - 1; i++)
                        {
                            if (!string.IsNullOrEmpty(ds.Tables[0].Rows[i]["GateIn_Date"].ToString()))
                            {
                                strSql = " UPDATE JOB_TRN_CONT SET GATEIN_DATE = TO_DATE('" + ds.Tables[0].Rows[i]["GateIn_Date"] + "','DD/MM/YYYY HH24:MI:SS') ";
                                strSql += " WHERE JOB_TRN_CONT_PK =" + ds.Tables[0].Rows[i]["JOB_TRN_CONT_PK"];

                                _with4.CommandText = strSql;
                                nRecAfct = nRecAfct + _with4.ExecuteNonQuery();
                            }
                        }
                    }
                    if (Convert.ToInt32(Pending) == 3)
                    {
                        for (int i = 0; i <= ds.Tables[0].Rows.Count - 1; i++)
                        {
                            if (!string.IsNullOrEmpty(ds.Tables[0].Rows[i]["Loading_Date"].ToString()))
                            {
                                strSql = " UPDATE JOB_TRN_CONT SET LOAD_DATE = TO_DATE('" + ds.Tables[0].Rows[i]["Loading_Date"] + "','DD/MM/YYYY HH24:MI:SS') ";
                                strSql += " WHERE JOB_TRN_CONT_PK =" + ds.Tables[0].Rows[i]["JOB_TRN_CONT_PK"];

                                _with4.CommandText = strSql;
                                nRecAfct = nRecAfct + _with4.ExecuteNonQuery();
                            }
                        }
                    }
                }

                //Air Imp
                if (BizType == 1 & ProcessType == 2)
                {
                    if (!string.IsNullOrEmpty(Pending))
                    {
                        if (Convert.ToInt32(Pending) == 1)
                        {
                            for (int i = 0; i <= ds.Tables[0].Rows.Count - 1; i++)
                            {
                                if (!string.IsNullOrEmpty(ds.Tables[0].Rows[i]["PickUp_Date"].ToString()))
                                {
                                    strSql = " UPDATE JOB_TRN_CONT SET LOAD_DATE = TO_DATE('" + ds.Tables[0].Rows[i]["PickUp_Date"] + "','DD/MM/YYYY HH24:MI:SS') ";
                                    strSql += " WHERE JOB_TRN_CONT_PK =" + ds.Tables[0].Rows[i]["JOB_TRN_CONT_PK"];

                                    _with4.CommandText = strSql;
                                    nRecAfct = nRecAfct + _with4.ExecuteNonQuery();
                                }
                            }
                        }
                    }
                    if (Convert.ToInt32(Pending) == 2)
                    {
                        for (int i = 0; i <= ds.Tables[0].Rows.Count - 1; i++)
                        {
                            if (!string.IsNullOrEmpty(ds.Tables[0].Rows[i]["GateIn_Date"].ToString()))
                            {
                                strSql = " UPDATE JOB_TRN_CONT SET GATEOUT_DATE = TO_DATE('" + ds.Tables[0].Rows[i]["GateIn_Date"] + "','DD/MM/YYYY HH24:MI:SS') ";
                                strSql += " WHERE JOB_TRN_CONT_PK =" + ds.Tables[0].Rows[i]["JOB_TRN_CONT_PK"];
                                //JOB_TRN_CONT_PK = JOB_TRN_CONT_PK

                                _with4.CommandText = strSql;
                                nRecAfct = nRecAfct + _with4.ExecuteNonQuery();
                            }
                        }
                    }
                }
                try
                {
                    if (BizType == 2)
                    {
                        //Conatiner Nr. Update
                        for (int i = 0; i <= ds.Tables[0].Rows.Count - 1; i++)
                        {
                            if (!string.IsNullOrEmpty(ds.Tables[0].Rows[i]["CONTAINER_NUM"].ToString()))
                            {
                                strSql = " UPDATE JOB_TRN_CONT SET CONTAINER_NUMBER = '" + ds.Tables[0].Rows[i]["CONTAINER_NUM"] + "' ";
                                strSql += " WHERE JOB_TRN_CONT_PK =" + ds.Tables[0].Rows[i]["JOB_TRN_CONT_PK"];

                                _with4.CommandText = strSql;
                                nRecAfct = nRecAfct + _with4.ExecuteNonQuery();
                            }
                        }
                    }
                    else
                    {
                        //Air Conatiner Nr. Update
                        for (int i = 0; i <= ds.Tables[0].Rows.Count - 1; i++)
                        {
                            if (!string.IsNullOrEmpty(ds.Tables[0].Rows[i]["CONTAINER_NUM"].ToString()))
                            {
                                strSql = " UPDATE JOB_TRN_CONT SET ULD_NUMBER = '" + ds.Tables[0].Rows[i]["CONTAINER_NUM"] + "' ";
                                strSql += " WHERE JOB_TRN_CONT_PK =" + ds.Tables[0].Rows[i]["JOB_TRN_CONT_PK"];

                                _with4.CommandText = strSql;
                                nRecAfct = nRecAfct + _with4.ExecuteNonQuery();
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                }
                if (nRecAfct > 0)
                {
                    TRAN.Commit();
                    arrMessage.Add("All Data Saved Successfully.");
                }
                else
                {
                    TRAN.Rollback();
                    arrMessage.Add("Error");
                }
                return arrMessage;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                ObjWk.MyConnection.Close();
            }
        }

        #endregion "Update Pending Activities"

        #endregion "Pending Act Search and Update"

        #region "Fetch Report Sch"

        public DataSet FetchReportSch(string SchPK = "", string CustPK = "", string SchType = "", string Frequence = "", string ReportType = "", Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();
            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with5 = objWF.MyDataAdapter;
                _with5.SelectCommand = new OracleCommand();
                _with5.SelectCommand.Connection = objWF.MyConnection;
                _with5.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_LOAD_CONFIRM_PKG.FETCH_REPORT_SCH";
                _with5.SelectCommand.CommandType = CommandType.StoredProcedure;
                _with5.SelectCommand.Parameters.Add("SCH_PK", (string.IsNullOrEmpty(SchPK) ? "" : SchPK)).Direction = ParameterDirection.Input;
                _with5.SelectCommand.Parameters.Add("CUST_PK", (string.IsNullOrEmpty(CustPK) ? "" : CustPK)).Direction = ParameterDirection.Input;
                _with5.SelectCommand.Parameters.Add("SCHTYPE", (string.IsNullOrEmpty(SchType) ? "" : SchType)).Direction = ParameterDirection.Input;
                _with5.SelectCommand.Parameters.Add("FREQUENCE", (string.IsNullOrEmpty(Frequence) ? "" : Frequence)).Direction = ParameterDirection.Input;
                _with5.SelectCommand.Parameters.Add("REPORTTYPE", (string.IsNullOrEmpty(ReportType) ? "" : ReportType)).Direction = ParameterDirection.Input;
                _with5.SelectCommand.Parameters.Add("CURRENT_PAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with5.SelectCommand.Parameters.Add("MASTER_PAGE_SIZE_IN", RecordsPerPage).Direction = ParameterDirection.Input;
                _with5.SelectCommand.Parameters.Add("TOTAL_PAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
                _with5.SelectCommand.Parameters.Add("LOAD_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with5.Fill(ds);
                TotalPage = Convert.ToInt32(_with5.SelectCommand.Parameters["TOTAL_PAGE_IN"].Value);
                CurrentPage = Convert.ToInt32(_with5.SelectCommand.Parameters["CURRENT_PAGE_IN"].Value);
                return ds;
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

        #endregion "Fetch Report Sch"

        #region "Fetch Tariff"

        public DataSet FetchTariff(string BIZ_TYPE = "", string CARGO_TYPE = "", string POLPK = "", string PODPK = "", string FromCountryPK = "", string ToCountryPK = "", string CustomerPK = "", string FrieghtPK = "", string CommGrpPK = "", string CarrierPK = "",
        string TariffTypePK = "", string Fromdate = "", string ToDate = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 Exportflg = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();
            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with6 = objWF.MyDataAdapter;
                _with6.SelectCommand = new OracleCommand();
                _with6.SelectCommand.Connection = objWF.MyConnection;
                _with6.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_LOAD_CONFIRM_PKG.FETCH_TARIFF";
                _with6.SelectCommand.CommandType = CommandType.StoredProcedure;
                _with6.SelectCommand.Parameters.Add("BIZ_TYPE_IN", (string.IsNullOrEmpty(BIZ_TYPE) ? "" : BIZ_TYPE)).Direction = ParameterDirection.Input;
                _with6.SelectCommand.Parameters.Add("CARGO_TYPE_IN", (string.IsNullOrEmpty(CARGO_TYPE) ? "" : CARGO_TYPE)).Direction = ParameterDirection.Input;
                _with6.SelectCommand.Parameters.Add("POLPK_IN", (string.IsNullOrEmpty(POLPK) ? "" : POLPK)).Direction = ParameterDirection.Input;
                _with6.SelectCommand.Parameters.Add("PODPK_IN", (string.IsNullOrEmpty(PODPK) ? "" : PODPK)).Direction = ParameterDirection.Input;
                _with6.SelectCommand.Parameters.Add("FROMCOUNTRYPK_IN", (string.IsNullOrEmpty(FromCountryPK) ? "" : FromCountryPK)).Direction = ParameterDirection.Input;
                _with6.SelectCommand.Parameters.Add("TOCOUNTRYPK_IN", (string.IsNullOrEmpty(ToCountryPK) ? "" : ToCountryPK)).Direction = ParameterDirection.Input;
                _with6.SelectCommand.Parameters.Add("CUSTOMERPK_IN", (string.IsNullOrEmpty(CustomerPK) ? "" : CustomerPK)).Direction = ParameterDirection.Input;
                _with6.SelectCommand.Parameters.Add("FRIEGHTPK_IN", (string.IsNullOrEmpty(FrieghtPK) ? "" : FrieghtPK)).Direction = ParameterDirection.Input;
                _with6.SelectCommand.Parameters.Add("COMMGRPPK_IN", (string.IsNullOrEmpty(CommGrpPK) ? "" : CommGrpPK)).Direction = ParameterDirection.Input;
                _with6.SelectCommand.Parameters.Add("CARRIERPK_IN", (string.IsNullOrEmpty(CarrierPK) ? "" : CarrierPK)).Direction = ParameterDirection.Input;
                _with6.SelectCommand.Parameters.Add("TARIFFTYPEPK_IN", (string.IsNullOrEmpty(TariffTypePK) ? "" : TariffTypePK)).Direction = ParameterDirection.Input;
                _with6.SelectCommand.Parameters.Add("FROMDATE_IN", (Fromdate == " " ? "" : Fromdate)).Direction = ParameterDirection.Input;
                _with6.SelectCommand.Parameters.Add("TODATE_IN", (ToDate == " " ? "" : ToDate)).Direction = ParameterDirection.Input;
                _with6.SelectCommand.Parameters.Add("EXPORT_FLG_IN", Exportflg).Direction = ParameterDirection.Input;
                _with6.SelectCommand.Parameters.Add("CURRENT_PAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with6.SelectCommand.Parameters.Add("MASTER_PAGE_SIZE_IN", RecordsPerPage).Direction = ParameterDirection.Input;
                _with6.SelectCommand.Parameters.Add("TOTAL_PAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
                _with6.SelectCommand.Parameters.Add("LOAD_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with6.Fill(ds);
                TotalPage = Convert.ToInt32(_with6.SelectCommand.Parameters["TOTAL_PAGE_IN"].Value);
                CurrentPage = Convert.ToInt32(_with6.SelectCommand.Parameters["CURRENT_PAGE_IN"].Value);
                return ds;
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

        #endregion "Fetch Tariff"

        #region "Fetch Report Sch"

        public DataSet FetchCustGroupHeader(string SchPK = "", string CustPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();
            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with7 = objWF.MyDataAdapter;
                _with7.SelectCommand = new OracleCommand();
                _with7.SelectCommand.Connection = objWF.MyConnection;
                _with7.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_LOAD_CONFIRM_PKG.FETCH_GROUP_HEADER";
                _with7.SelectCommand.CommandType = CommandType.StoredProcedure;
                _with7.SelectCommand.Parameters.Add("SCH_PK", (string.IsNullOrEmpty(SchPK) ? "" : SchPK)).Direction = ParameterDirection.Input;
                _with7.SelectCommand.Parameters.Add("CUST_PK", (string.IsNullOrEmpty(CustPK) ? "" : CustPK)).Direction = ParameterDirection.Input;
                _with7.SelectCommand.Parameters.Add("CURRENT_PAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with7.SelectCommand.Parameters.Add("MASTER_PAGE_SIZE_IN", RecordsPerPage).Direction = ParameterDirection.Input;
                _with7.SelectCommand.Parameters.Add("TOTAL_PAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
                _with7.SelectCommand.Parameters.Add("LOAD_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with7.Fill(ds);
                //TotalPage = .SelectCommand.Parameters.Item("TOTAL_PAGE_IN"].Value()
                CurrentPage = Convert.ToInt32(_with7.SelectCommand.Parameters["CURRENT_PAGE_IN"].Value);
                return ds;
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

        #endregion "Fetch Report Sch"

        #region "Fetch Report Sch"

        public DataSet FetchCustNotify(string SchPK = "", string CustPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();
            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with8 = objWF.MyDataAdapter;
                _with8.SelectCommand = new OracleCommand();
                _with8.SelectCommand.Connection = objWF.MyConnection;
                _with8.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_LOAD_CONFIRM_PKG.FETCH_NOTIFY";
                _with8.SelectCommand.CommandType = CommandType.StoredProcedure;
                _with8.SelectCommand.Parameters.Add("SCH_PK", (string.IsNullOrEmpty(SchPK) ? "" : SchPK)).Direction = ParameterDirection.Input;
                _with8.SelectCommand.Parameters.Add("CUST_PK", (string.IsNullOrEmpty(CustPK) ? "" : CustPK)).Direction = ParameterDirection.Input;
                _with8.SelectCommand.Parameters.Add("CURRENT_PAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with8.SelectCommand.Parameters.Add("MASTER_PAGE_SIZE_IN", RecordsPerPage).Direction = ParameterDirection.Input;
                _with8.SelectCommand.Parameters.Add("TOTAL_PAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
                _with8.SelectCommand.Parameters.Add("LOAD_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with8.Fill(ds);
                //TotalPage = .SelectCommand.Parameters.Item("TOTAL_PAGE_IN"].Value()
                CurrentPage = Convert.ToInt32(_with8.SelectCommand.Parameters["CURRENT_PAGE_IN"].Value);
                return ds;
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

        #endregion "Fetch Report Sch"

        #region "Save Report Sch"

        public ArrayList SaveReportSch(DataSet M_datasetGroup, DataSet M_datasetNotify, string txtSchID, int ddlReport, int ddlSchType, int txtCustPK, int ddlFrequency, int ddlMonth, int ddldate, int ddlDay,
        string txtTrigger, string hdnCountryPK, int ddlBizType, int ddlProcess, string hdnPOLPK, string hdnPODPK, string hdnLocationPK, string hdnCurrencyPK, int ddlReport1, long CREATED_BY = 0,
        long LAST_MODIFIED_BY = 0, long ConfigurationPK = 0, int SchPK1 = 0, int Active = 0)
        {
            ArrayList functionReturnValue = null;

            WorkFlow objWK = new WorkFlow();
            WorkFlow objWK1 = new WorkFlow();
            objWK.OpenConnection();
            objWK1.OpenConnection();
            string GenerateSCH = null;
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            Int32 RecAfct = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            OracleCommand delCommand = new OracleCommand();
            int schpk = 0;
            try
            {
                if (string.IsNullOrEmpty(txtSchID))
                {
                    GenerateSCH = GenerateProtocolKey("SCHEDULER", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), DateTime.Now, "", "", "", 0, objWK1, "", "");
                    if (GenerateSCH == "Protocol Not Defined.")
                    {
                        arrMessage.Add("Protocol Not Defined.");
                        return arrMessage;
                        return functionReturnValue;
                    }
                    var _with9 = insCommand;
                    _with9.Connection = objWK.MyConnection;
                    _with9.CommandType = CommandType.StoredProcedure;
                    _with9.CommandText = objWK.MyUserName + (".REPORT_HEADER_PKG_NEW.REPORT_HEADER_INS");
                    _with9.Parameters.Clear();
                    _with9.Parameters.Add("SCH_ID_IN", GenerateSCH).Direction = ParameterDirection.Input;
                    _with9.Parameters.Add("REPORT_TYPE_IN", ddlReport).Direction = ParameterDirection.Input;
                    _with9.Parameters.Add("SCH_TYPE_IN", ddlSchType).Direction = ParameterDirection.Input;
                    _with9.Parameters.Add("CUSTOMER_PK_IN", txtCustPK).Direction = ParameterDirection.Input;
                    _with9.Parameters.Add("FREQUENCY_TYPE_IN", ddlFrequency).Direction = ParameterDirection.Input;
                    _with9.Parameters.Add("MONTH_SCH_IN", ddlMonth).Direction = ParameterDirection.Input;
                    _with9.Parameters.Add("DATE_SCH_IN", ddldate).Direction = ParameterDirection.Input;
                    ///''
                    _with9.Parameters.Add("DAY_SCH_IN", ddlDay).Direction = ParameterDirection.Input;
                    ///''
                    _with9.Parameters.Add("TRIGGER_TIME_IN", txtTrigger).Direction = ParameterDirection.Input;
                    _with9.Parameters.Add("COUNTRY_PK_IN", hdnCountryPK).Direction = ParameterDirection.Input;
                    _with9.Parameters.Add("LOCATION_PK_IN", hdnLocationPK).Direction = ParameterDirection.Input;
                    ///''
                    _with9.Parameters.Add("BIZ_TYPE_IN", ddlBizType).Direction = ParameterDirection.Input;
                    _with9.Parameters.Add("CURENCY_IN", hdnCurrencyPK).Direction = ParameterDirection.Input;
                    _with9.Parameters.Add("PROCESS_TYPE_IN", ddlProcess).Direction = ParameterDirection.Input;
                    _with9.Parameters.Add("REPORT_BY_IN", ddlReport1).Direction = ParameterDirection.Input;
                    _with9.Parameters.Add("POLAOOPK_IN", hdnPOLPK).Direction = ParameterDirection.Input;
                    _with9.Parameters.Add("PODAODPK_IN", hdnPODPK).Direction = ParameterDirection.Input;
                    _with9.Parameters.Add("ACTIVE_IN", Active).Direction = ParameterDirection.Input;
                    //.Parameters.Add("CREATED_BY_FK_IN", CREATED_BY).Direction = ParameterDirection.Input
                    //.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input
                    _with9.Parameters.Add("return_value", OracleDbType.Varchar2, 200, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    var _with10 = objWK.MyDataAdapter;
                    _with10.InsertCommand = insCommand;
                    _with10.InsertCommand.Transaction = TRAN;
                    _with10.InsertCommand.ExecuteNonQuery();
                    schpk = Convert.ToInt32(Convert.ToString(insCommand.Parameters["RETURN_VALUE"].Value));
                    SchPK1 = Convert.ToInt32(Convert.ToString(insCommand.Parameters["RETURN_VALUE"].Value));
                    if (schpk > 0)
                    {
                        if ((M_datasetGroup != null))
                        {
                            arrMessage = GridSave(M_datasetGroup, M_datasetNotify, schpk, txtSchID, TRAN);
                        }
                        else
                        {
                            arrMessage = SAVEReportTRN(M_datasetGroup, M_datasetNotify, schpk, txtSchID, Convert.ToString(txtCustPK), TRAN);
                        }
                    }
                    return arrMessage;
                }
                else
                {
                    var _with11 = updCommand;
                    _with11.Connection = objWK.MyConnection;
                    _with11.CommandType = CommandType.StoredProcedure;
                    _with11.CommandText = objWK.MyUserName + (".REPORT_HEADER_PKG_NEW.REPORT_HEADER_UPD");
                    _with11.Parameters.Clear();
                    _with11.Parameters.Add("SCH_ID_IN", txtSchID).Direction = ParameterDirection.Input;
                    _with11.Parameters.Add("REPORT_TYPE_IN", ddlReport).Direction = ParameterDirection.Input;
                    _with11.Parameters.Add("SCH_TYPE_IN", ddlSchType).Direction = ParameterDirection.Input;
                    _with11.Parameters.Add("CUSTOMER_PK_IN", txtCustPK).Direction = ParameterDirection.Input;
                    _with11.Parameters.Add("FREQUENCY_TYPE_IN", ddlFrequency).Direction = ParameterDirection.Input;
                    _with11.Parameters.Add("MONTH_SCH_IN", ddlMonth).Direction = ParameterDirection.Input;
                    _with11.Parameters.Add("DATE_SCH_IN", ddldate).Direction = ParameterDirection.Input;
                    ///''
                    _with11.Parameters.Add("DAY_SCH_IN", ddlDay).Direction = ParameterDirection.Input;
                    ///''
                    _with11.Parameters.Add("TRIGGER_TIME_IN", txtTrigger).Direction = ParameterDirection.Input;
                    _with11.Parameters.Add("COUNTRY_PK_IN", hdnCountryPK).Direction = ParameterDirection.Input;
                    _with11.Parameters.Add("LOCATION_PK_IN", hdnLocationPK).Direction = ParameterDirection.Input;
                    ///''
                    _with11.Parameters.Add("BIZ_TYPE_IN", ddlBizType).Direction = ParameterDirection.Input;
                    _with11.Parameters.Add("CURENCY_IN", hdnCurrencyPK).Direction = ParameterDirection.Input;
                    _with11.Parameters.Add("PROCESS_TYPE_IN", ddlProcess).Direction = ParameterDirection.Input;
                    _with11.Parameters.Add("REPORT_BY_IN", ddlReport1).Direction = ParameterDirection.Input;
                    _with11.Parameters.Add("POLAOOPK_IN", hdnPOLPK).Direction = ParameterDirection.Input;
                    _with11.Parameters.Add("PODAODPK_IN", hdnPODPK).Direction = ParameterDirection.Input;
                    _with11.Parameters.Add("ACTIVE_IN", Active).Direction = ParameterDirection.Input;
                    //.Parameters.Add("CREATED_BY_FK_IN", CREATED_BY).Direction = ParameterDirection.Input
                    //.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input
                    _with11.Parameters.Add("return_value", OracleDbType.Varchar2, 200, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with11.Parameters["return_value"].SourceVersion = DataRowVersion.Current;

                    var _with12 = objWK.MyDataAdapter;
                    _with12.UpdateCommand = updCommand;
                    _with12.UpdateCommand.Transaction = TRAN;
                    _with12.UpdateCommand.ExecuteNonQuery();
                    schpk = Convert.ToInt32(updCommand.Parameters["RETURN_VALUE"].Value);
                    SchPK1 = Convert.ToInt32(updCommand.Parameters["RETURN_VALUE"].Value);
                    if (schpk > 0)
                    {
                        if ((M_datasetGroup != null))
                        {
                            arrMessage = GridSave(M_datasetGroup, M_datasetNotify, schpk, txtSchID, TRAN);
                        }
                        else
                        {
                            arrMessage = SAVEReportTRN(M_datasetGroup, M_datasetNotify, schpk, txtSchID, Convert.ToString(txtCustPK), TRAN);
                        }
                    }
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
                throw ex;
            }
            return functionReturnValue;
        }

        #endregion "Save Report Sch"

        #region "GridSaveReport"

        public ArrayList GridSave(DataSet M_DataSetGroupHeader, DataSet M_DataSetNotify, Int64 SchMstPk, string txtSchID, OracleTransaction Tran)
        {
            int intPKVal = 0;
            long lngI = 0;
            long VatPk = 0;
            Int32 RecAfct = 0;
            Int64 Countrypk = 0;

            ArrayList arraymsg = new ArrayList();
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            OracleCommand delCommand = new OracleCommand();
            WorkFlow objWK = new WorkFlow();
            objWK.MyConnection = Tran.Connection;
            try
            {
                Int32 RowCnt = default(Int32);
                DataTable DtTbl = new DataTable();
                DataRow DtRw = null;
                int i = 0;
                if (M_DataSetGroupHeader.Tables.Count > 0)
                {
                    if (string.IsNullOrEmpty(txtSchID))
                    {
                        var _with13 = insCommand;
                        _with13.Connection = objWK.MyConnection;
                        _with13.CommandType = CommandType.StoredProcedure;
                        _with13.CommandText = objWK.MyUserName + ".REPORT_HEADER_PKG_NEW.REPORT_HEADER_TRN_INS";
                        for (RowCnt = 0; RowCnt <= M_DataSetGroupHeader.Tables[0].Rows.Count - 1; RowCnt++)
                        {
                            _with13.Parameters.Clear();
                            if (!string.IsNullOrEmpty(M_DataSetGroupHeader.Tables[0].Rows[RowCnt]["SEL"].ToString()))
                            {
                                if (M_DataSetGroupHeader.Tables[0].Rows[RowCnt]["SEL"] == "true")
                                {
                                    _with13.Parameters.Add("SCH_FK_IN", SchMstPk).Direction = ParameterDirection.Input;
                                    _with13.Parameters.Add("GROUP_PK_IN", M_DataSetGroupHeader.Tables[0].Rows[RowCnt]["GROUPPK"]).Direction = ParameterDirection.Input;
                                    _with13.Parameters.Add("CUSTOMER_PK_IN", M_DataSetGroupHeader.Tables[0].Rows[RowCnt]["CUSTPK"]).Direction = ParameterDirection.Input;
                                    _with13.Parameters.Add("NOTIFY_TYPE_IN", M_DataSetNotify.Tables[0].Rows[RowCnt]["NOTIFYTYPE"]).Direction = ParameterDirection.Input;
                                    _with13.Parameters.Add("CUST_EMAILID_IN", M_DataSetNotify.Tables[0].Rows[RowCnt]["NOTIFYEMAIL"]).Direction = ParameterDirection.Input;
                                    _with13.Parameters.Add("return_value", OracleDbType.Int32, 10, "SCH_TRN_PK").Direction = ParameterDirection.Output;
                                    _with13.Parameters["return_value"].SourceVersion = DataRowVersion.Current;
                                    var _with14 = objWK.MyDataAdapter;
                                    _with14.InsertCommand = insCommand;
                                    _with14.InsertCommand.Transaction = Tran;
                                    RecAfct = _with14.InsertCommand.ExecuteNonQuery();
                                    //.Update(M_DataSet)
                                }
                            }
                        }
                    }
                    else
                    {
                        var _with15 = updCommand;
                        _with15.Connection = objWK.MyConnection;
                        _with15.CommandType = CommandType.StoredProcedure;
                        _with15.CommandText = objWK.MyUserName + ".REPORT_HEADER_PKG_NEW.REPORT_HEADER_TRN_UPD";
                        for (RowCnt = 0; RowCnt <= M_DataSetGroupHeader.Tables[0].Rows.Count - 1; RowCnt++)
                        {
                            _with15.Parameters.Clear();
                            // If Not IsDBNull(M_DataSetGroupHeader.Tables(0).Rows(RowCnt).Item("SEL")) Then
                            // If M_DataSetGroupHeader.Tables(0).Rows(RowCnt).Item("SEL") = "true" Then
                            _with15.Parameters.Add("SCH_TRN_PK_IN", 1).Direction = ParameterDirection.Input;
                            _with15.Parameters.Add("SCH_FK_IN", SchMstPk).Direction = ParameterDirection.Input;
                            _with15.Parameters.Add("GROUP_PK_IN", M_DataSetGroupHeader.Tables[0].Rows[RowCnt]["GROUPPK"]).Direction = ParameterDirection.Input;
                            _with15.Parameters.Add("CUSTOMER_PK_IN", M_DataSetGroupHeader.Tables[0].Rows[RowCnt]["CUSTPK"]).Direction = ParameterDirection.Input;
                            _with15.Parameters.Add("NOTIFY_TYPE_IN", M_DataSetNotify.Tables[0].Rows[RowCnt]["NOTIFYTYPE"]).Direction = ParameterDirection.Input;
                            _with15.Parameters.Add("CUST_EMAILID_IN", M_DataSetNotify.Tables[0].Rows[RowCnt]["NOTIFYEMAIL"]).Direction = ParameterDirection.Input;
                            _with15.Parameters.Add("return_value", OracleDbType.Int32, 10, "SCH_TRN_PK").Direction = ParameterDirection.Output;
                            _with15.Parameters["return_value"].SourceVersion = DataRowVersion.Current;
                            var _with16 = objWK.MyDataAdapter;
                            _with16.UpdateCommand = updCommand;
                            _with16.UpdateCommand.Transaction = Tran;
                            RecAfct = _with16.UpdateCommand.ExecuteNonQuery();
                            // End If
                            // End If
                        }
                    }
                }
                if (RecAfct > 0)
                {
                    Tran.Commit();
                    arrMessage.Add("All Data Saved Successfully");
                    return arrMessage;
                }
                else
                {
                    Tran.Rollback();
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

        public ArrayList SAVEReportTRN(DataSet M_DataSetGroupHeader, DataSet M_DataSetNotify, Int64 SchMstPk, string txtSchID, string txtCustPK, OracleTransaction Tran)
        {
            int intPKVal = 0;
            long lngI = 0;
            long VatPk = 0;
            Int32 RecAfct = 0;
            Int64 Countrypk = 0;

            ArrayList arraymsg = new ArrayList();
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            OracleCommand delCommand = new OracleCommand();
            WorkFlow objWK = new WorkFlow();
            string custEmail = null;
            custEmail = objWK.ExecuteScaler("SELECT CCT.ADM_EMAIL_ID FROM  CUSTOMER_MST_TBL CMT ,CUSTOMER_CONTACT_DTLS CCT  WHERE CMT.CUSTOMER_MST_PK = CCT.CUSTOMER_MST_FK AND CMT.CUSTOMER_MST_PK = " + txtCustPK + " ");
            objWK.MyConnection = Tran.Connection;
            try
            {
                Int32 RowCnt = default(Int32);
                DataTable DtTbl = new DataTable();
                DataRow DtRw = null;
                int i = 0;

                //   If M_DataSetGroupHeader.Tables.Count > 0 Then
                if (string.IsNullOrEmpty(txtSchID))
                {
                    var _with17 = insCommand;
                    _with17.Connection = objWK.MyConnection;
                    _with17.CommandType = CommandType.StoredProcedure;
                    _with17.CommandText = objWK.MyUserName + ".REPORT_HEADER_PKG_NEW.REPORT_HEADER_TRN_INS";
                    // For RowCnt = 0 To M_DataSetGroupHeader.Tables(0).Rows.Count - 1
                    _with17.Parameters.Clear();
                    //If Not IsDBNull(M_DataSetGroupHeader.Tables(0).Rows(RowCnt).Item("SEL")) Then
                    //If M_DataSetGroupHeader.Tables(0).Rows(RowCnt).Item("SEL") = "true" Then
                    _with17.Parameters.Add("SCH_FK_IN", SchMstPk).Direction = ParameterDirection.Input;
                    _with17.Parameters.Add("GROUP_PK_IN", 0).Direction = ParameterDirection.Input;
                    _with17.Parameters.Add("CUSTOMER_PK_IN", txtCustPK).Direction = ParameterDirection.Input;
                    _with17.Parameters.Add("NOTIFY_TYPE_IN", 0).Direction = ParameterDirection.Input;
                    _with17.Parameters.Add("CUST_EMAILID_IN", custEmail).Direction = ParameterDirection.Input;
                    _with17.Parameters.Add("return_value", OracleDbType.Int32, 10, "SCH_TRN_PK").Direction = ParameterDirection.Output;
                    _with17.Parameters["return_value"].SourceVersion = DataRowVersion.Current;
                    var _with18 = objWK.MyDataAdapter;
                    _with18.InsertCommand = insCommand;
                    _with18.InsertCommand.Transaction = Tran;
                    RecAfct = _with18.InsertCommand.ExecuteNonQuery();
                    //.Update(M_DataSet)
                    //End If
                    //End If
                    // Next
                }
                else
                {
                    var _with19 = updCommand;
                    _with19.Connection = objWK.MyConnection;
                    _with19.CommandType = CommandType.StoredProcedure;
                    _with19.CommandText = objWK.MyUserName + ".REPORT_HEADER_PKG_NEW.REPORT_HEADER_TRN_UPD";
                    // For RowCnt = 0 To M_DataSetGroupHeader.Tables(0).Rows.Count - 1
                    _with19.Parameters.Clear();
                    //If Not IsDBNull(M_DataSetGroupHeader.Tables(0).Rows(RowCnt).Item("SEL")) Then
                    //If M_DataSetGroupHeader.Tables(0).Rows(RowCnt).Item("SEL") = "true" Then
                    _with19.Parameters.Add("SCH_TRN_PK_IN", 1).Direction = ParameterDirection.Input;
                    _with19.Parameters.Add("SCH_FK_IN", SchMstPk).Direction = ParameterDirection.Input;
                    _with19.Parameters.Add("GROUP_PK_IN", 0).Direction = ParameterDirection.Input;
                    _with19.Parameters.Add("CUSTOMER_PK_IN", txtCustPK).Direction = ParameterDirection.Input;
                    _with19.Parameters.Add("NOTIFY_TYPE_IN", 0).Direction = ParameterDirection.Input;
                    _with19.Parameters.Add("CUST_EMAILID_IN", custEmail).Direction = ParameterDirection.Input;
                    _with19.Parameters.Add("return_value", OracleDbType.Int32, 10, "SCH_TRN_PK").Direction = ParameterDirection.Output;
                    _with19.Parameters["return_value"].SourceVersion = DataRowVersion.Current;
                    var _with20 = objWK.MyDataAdapter;
                    _with20.UpdateCommand = updCommand;
                    _with20.UpdateCommand.Transaction = Tran;
                    RecAfct = _with20.UpdateCommand.ExecuteNonQuery();
                    //End If
                    //End If
                    // Next
                }
                //  End If
                if (RecAfct > 0)
                {
                    Tran.Commit();
                    arrMessage.Add("All Data Saved Successfully");
                    return arrMessage;
                }
                else
                {
                    Tran.Rollback();
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

        #endregion "GridSaveReport"

        #region "Fetch Report Sch"

        public DataSet FetchReportHeader(string SchPK = "", string CustPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();
            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with21 = objWF.MyDataAdapter;
                _with21.SelectCommand = new OracleCommand();
                _with21.SelectCommand.Connection = objWF.MyConnection;
                _with21.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_LOAD_CONFIRM_PKG.FETCH_REPORT_HEADER";
                _with21.SelectCommand.CommandType = CommandType.StoredProcedure;
                _with21.SelectCommand.Parameters.Add("SCH_PK", (string.IsNullOrEmpty(SchPK) ? "" : SchPK)).Direction = ParameterDirection.Input;
                _with21.SelectCommand.Parameters.Add("CUST_PK", (string.IsNullOrEmpty(CustPK) ? "" : CustPK)).Direction = ParameterDirection.Input;
                _with21.SelectCommand.Parameters.Add("CURRENT_PAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with21.SelectCommand.Parameters.Add("MASTER_PAGE_SIZE_IN", RecordsPerPage).Direction = ParameterDirection.Input;
                _with21.SelectCommand.Parameters.Add("TOTAL_PAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
                _with21.SelectCommand.Parameters.Add("LOAD_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with21.Fill(ds);
                //TotalPage = .SelectCommand.Parameters.Item("TOTAL_PAGE_IN"].Value()
                CurrentPage = Convert.ToInt32(_with21.SelectCommand.Parameters["CURRENT_PAGE_IN"].Value);
                return ds;
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

        #endregion "Fetch Report Sch"

        #region "Fetch Record Others Tariff Listing.."

        public DataSet FetchHeaderOthersTariffListing(string POLPK = "", string PODPK = "", string FRT_PK = "", string CONT_PK = "", string Tariff = "", string ddlBizType = "")
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();
            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with22 = objWF.MyDataAdapter;
                _with22.SelectCommand = new OracleCommand();
                _with22.SelectCommand.Connection = objWF.MyConnection;
                if (Convert.ToInt32(ddlBizType) == 1)
                {
                    if (Convert.ToInt32(Tariff) == 1)
                    {
                        _with22.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_LOAD_CONFIRM_PKG.FETCH_OTHER_SLAB";
                    }
                    else
                    {
                        _with22.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_LOAD_CONFIRM_PKG.FETCH_OTHER_SLAB_CUST";
                    }
                }
                else
                {
                    if (Convert.ToInt32(Tariff) == 1)
                    {
                        _with22.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_LOAD_CONFIRM_PKG.FETCH_OTHER_CONT_TARIFF";
                    }
                    else
                    {
                        _with22.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_LOAD_CONFIRM_PKG.FETCH_OTHER_CONT";
                    }
                }

                _with22.SelectCommand.CommandType = CommandType.StoredProcedure;
                _with22.SelectCommand.Parameters.Add("POL_PK", (string.IsNullOrEmpty(POLPK) ? "" : POLPK)).Direction = ParameterDirection.Input;
                _with22.SelectCommand.Parameters.Add("POD_PK", (string.IsNullOrEmpty(PODPK) ? "" : PODPK)).Direction = ParameterDirection.Input;
                _with22.SelectCommand.Parameters.Add("FRT_PK", (string.IsNullOrEmpty(FRT_PK) ? "" : FRT_PK)).Direction = ParameterDirection.Input;
                _with22.SelectCommand.Parameters.Add("CONT_PK", (string.IsNullOrEmpty(CONT_PK) ? "" : CONT_PK)).Direction = ParameterDirection.Input;
                _with22.SelectCommand.Parameters.Add("LOAD_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with22.Fill(ds);
                return ds;
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

        #endregion "Fetch Record Others Tariff Listing.."
    }
}