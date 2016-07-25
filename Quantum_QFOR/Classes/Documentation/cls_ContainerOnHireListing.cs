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
    public class cls_ContainerOnHireListing : CommonFeatures
    {

        #region "FechOnHireListing"
        public DataSet FechOnHireListing(string WONumber = "", string WODate = "", string TransporterPK = "", string OnhireRef = "", string RefDate = "", string ShiPLinePK = "", string WHPk = "", Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {
            System.Text.StringBuilder strBuild = new System.Text.StringBuilder();
            WorkFlow objWK = new WorkFlow();
            DataSet DS = null;
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strCondition = "";
            string strCondition1 = "";
            Int32 TotalRecords = default(Int32);
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append(" SELECT ROWNUM SLNO, Q.*");
            sb.Append("  FROM (SELECT HDR.ON_HIRE_PK,");
            sb.Append("               HDR.WO_NUMBER,");
            sb.Append("               TO_DATE(HDR.WO_DATE, DATEFORMAT) WO_DATE,");
            sb.Append("               VST.VENDOR_NAME,");
            sb.Append("               SUM(DTL.ONHIRE_QTY),");
            sb.Append("               VST.VENDOR_NAME DEPOT_SUPP_WH_FK ");
            //sb.Append("               TSP.VENDOR_ID TRANSPORTER_ID,")
            //sb.Append("               TSP.VENDOR_NAME TRANSPORTER_NAME")
            sb.Append("          FROM WO_ONHIRE_HDR       HDR,");
            sb.Append("               VENDOR_MST_TBL      VST,");
            sb.Append("               VENDOR_MST_TBL      TSP,");
            sb.Append("               VENDOR_TYPE_MST_TBL VT,");
            sb.Append("               VENDOR_SERVICES_TRN VS,");
            sb.Append("               OPERATOR_MST_TBL    OPR,");
            sb.Append("               WO_ONHIRE_DTL       DTL");
            sb.Append("         WHERE HDR.TRANSPORTER_FK = TSP.VENDOR_MST_PK(+)");
            sb.Append("           AND HDR.DEPOT_SUPP_WH_FK = VST.VENDOR_MST_PK(+)");
            sb.Append("           AND HDR.SHIPPING_LINE_FK = OPR.OPERATOR_MST_PK(+)");
            sb.Append("           AND DTL.ON_HIRE_HDR_FK = HDR.ON_HIRE_PK");
            sb.Append("           AND TSP.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
            sb.Append("           AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK(+)");
            sb.Append("           AND UPPER(VT.VENDOR_TYPE_ID(+)) = 'TRANSPORTER'");

            if (!string.IsNullOrEmpty(WONumber))
            {
                sb.Append(" AND hdr.WO_NUMBER ='" + WONumber + "'");
            }
            if ((WODate != null))
            {
                sb.Append(" AND hdr.WO_DATE = '" + WODate + "'");
            }
            if (!string.IsNullOrEmpty(TransporterPK))
            {
                sb.Append(" AND hdr.TRANSPORTER_FK = " + TransporterPK);
            }
            if (!string.IsNullOrEmpty(OnhireRef))
            {
                sb.Append(" AND hdr.ON_HIRE_NUMBER = '" + OnhireRef + "'");
            }
            if ((RefDate != null))
            {
                sb.Append(" AND hdr.ON_HIRE_DATE = '" + RefDate + "'");
            }
            if (!string.IsNullOrEmpty(ShiPLinePK))
            {
                sb.Append(" AND hdr.SHIPPING_LINE_FK = " + ShiPLinePK);
            }
            if (!string.IsNullOrEmpty(WHPk))
            {
                sb.Append(" AND vst.VENDOR_MST_PK = " + WHPk);
            }

            sb.Append("         GROUP BY HDR.ON_HIRE_PK,");
            sb.Append("                  HDR.WO_NUMBER,");
            sb.Append("                  HDR.WO_DATE,");
            sb.Append("                  VST.VENDOR_NAME,");
            sb.Append("                  VST.VENDOR_NAME ");
            //sb.Append("                  VU.VENDOR_ID,")
            //sb.Append("                  VU.VENDOR_NAME")
            sb.Append("         ORDER BY HDR.WO_DATE DESC ");

            string StrSqlCount = null;
            StrSqlCount = "SELECT COUNT(*) FROM ( ";
            StrSqlCount = StrSqlCount + sb.ToString();
            StrSqlCount = StrSqlCount + " )q) ";

            TotalRecords = Convert.ToInt32(objWK.ExecuteScaler(StrSqlCount.ToString()));
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
            ///'''''''''''''''''''''''''''common''''''''''''''''''''''''''''''''''

            string StrSqlRecords = null;
            StrSqlRecords = "SELECT * FROM ( ";
            StrSqlRecords = StrSqlRecords + sb.ToString();
            StrSqlRecords = StrSqlRecords + " ) q) WHERE SLNO BETWEEN " + start + " AND " + last;

            try
            {
                DS = objWK.GetDataSet(StrSqlRecords);
                return DS;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "fetchWorkOrderCont"
        public DataTable fetchWorkOrderCont(Int32 PK)
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWK = new WorkFlow();
            DataTable Dt = null;
            try
            {
                strSQL.Append(" select rownum slnr,q.* ");
                strSQL.Append("     from(select dtl.on_hire_dtl_pk, ");
                strSQL.Append("       cty.container_type_mst_pk CTYPE_PK, ");
                strSQL.Append("       cty.container_type_mst_id CTYPE_ID, ");
                strSQL.Append("       cty.container_type_name CTYPE_DESC, ");
                strSQL.Append("       dtl.onhire_qty, ");
                strSQL.Append("       0 hidden_qty, ");
                strSQL.Append("       curr.currency_mst_pk, ");
                strSQL.Append("       curr.currency_id, ");
                strSQL.Append("       dtl.onhire_per_diem, ");
                if (PK > 0)
                {
                    strSQL.Append("       (nvl(dtl.onhire_qty,0) * nvl(onhire_cost,0)) onhire_cost, ");
                }
                else
                {
                    strSQL.Append("       0 as onhire_cost, ");
                }

                strSQL.Append("      ''  selflag  ");
                strSQL.Append("   from wo_onhire_hdr          hdr, ");
                strSQL.Append("        wo_onhire_dtl          dtl, ");
                strSQL.Append("        container_type_mst_tbl cty, ");
                strSQL.Append("        currency_type_mst_tbl  curr ");
                strSQL.Append("   where hdr.on_hire_pk = dtl.on_hire_hdr_fk ");
                strSQL.Append("   and dtl.cont_type_mst_fk = cty.container_type_mst_pk ");
                strSQL.Append("   and dtl.currency_fk = curr.currency_mst_pk ");
                strSQL.Append("    and dtl.on_hire_hdr_fk = hdr.on_hire_pk ");
                strSQL.Append("   and hdr.on_hire_pk = " + PK + ")q ");

                Dt = objWK.GetDataTable(strSQL.ToString());
                return Dt;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "fetchOnhireContDtl"
        public DataTable fetchOnhireContDtl(Int32 PK)
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWK = new WorkFlow();
            DataTable Dt = null;
            try
            {
                strSQL.Append("   select rownum slnr, q.* from ( ");
                strSQL.Append("   select wc_dtl.on_hire_cont_pk, ");
                strSQL.Append("          wc_dtl.on_hire_dtl_fk, ");
                strSQL.Append("          wc_dtl.cont_no,    ");
                strSQL.Append("          cty.container_type_mst_pk , ");
                strSQL.Append("          cty.container_type_mst_id cont_Type_Id, ");
                strSQL.Append("     '' selflag ");
                strSQL.Append("   from wo_onhire_hdr          hdr, ");
                strSQL.Append("        wo_onhire_dtl          dtl, ");
                strSQL.Append("        container_type_mst_tbl cty, ");
                strSQL.Append("        wo_onhire_cont_dtl     wc_dtl  ");
                strSQL.Append("   where hdr.on_hire_pk = dtl.on_hire_hdr_fk ");
                strSQL.Append("   and dtl.on_hire_dtl_pk = wc_dtl.on_hire_dtl_fk ");
                strSQL.Append("   and wc_dtl.cont_type_mst_fk = cty.container_type_mst_pk     ");
                strSQL.Append("   and hdr.on_hire_pk  = " + PK + ")q ");

                Dt = objWK.GetDataTable(strSQL.ToString());
                return Dt;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "fetchHeader"
        public DataSet fetchHeader(Int32 PK)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            WorkFlow objWK = new WorkFlow();
            DataSet Ds = null;
            try
            {
                sb.Append(" select hdr.on_hire_pk,");
                sb.Append("            hdr.wo_number,");
                sb.Append("            to_char(hdr.wo_date, 'dd/MM/yyyy') wo_date,");
                sb.Append("            hdr.on_hire_number,");
                sb.Append("            to_char(hdr.on_hire_date, 'dd/MM/yyyy') on_hire_date,");
                sb.Append("            hdr.pickup_ref_no,");
                sb.Append("            to_char(hdr.pickup_date, 'dd/MM/yyyy') pickup_date,");
                sb.Append("            hdr.vehicle_no,");
                sb.Append("            vst.vendor_name,");
                sb.Append("            vst.vendor_id,");
                sb.Append("            tsp.transporter_id,");
                sb.Append("            tsp.transporter_name,");
                sb.Append("            opr.operator_id,");
                sb.Append("            opr.operator_name,");
                sb.Append("            tsp.transporter_mst_pk,");
                sb.Append("           opr.operator_mst_pk,");
                sb.Append("            vst.vendor_mst_pk,hdr.version_no ");
                sb.Append("       from wo_onhire_hdr       hdr,");
                sb.Append("            VENDOR_MST_TBL      vst,");
                sb.Append("            transporter_mst_tbl tsp,");
                sb.Append("            operator_mst_tbl    opr");
                sb.Append("      where hdr.transporter_fk = tsp.transporter_mst_pk(+)");
                sb.Append("        and hdr.depot_supp_wh_fk = vst.vendor_mst_pk(+)");
                sb.Append("        and hdr.shipping_line_fk = opr.operator_mst_pk(+)");
                sb.Append("        and hdr.on_hire_pk=" + PK + "");
                Ds = objWK.GetDataSet(sb.ToString());

                sb = new System.Text.StringBuilder();
                sb.Append("select vst.vendor_id transporter_id,");
                sb.Append("       vst.vendor_name transporter_name,");
                sb.Append("       vst.vendor_mst_pk transporter_mst_pk");
                sb.Append("       from wo_onhire_hdr       hdr,");
                sb.Append("       VENDOR_MST_TBL      vst");
                sb.Append("       where hdr.transporter_fk = vst.vendor_mst_pk(+)");
                sb.Append("        and hdr.on_hire_pk=" + PK + "");
                Ds.Tables.Add(objWK.GetDataTable(sb.ToString()));
                return Ds;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "FETCH LOCATION WISE "
        public string FetchDepo(string strCond, string loc = "0")
        {

            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strLOC_MST_IN = null;
            string strSERACH_IN = null;
            int strBIZ_TYPE_IN = 0;
            string strReq = null;
            var strNull = DBNull.Value;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strBIZ_TYPE_IN = Convert.ToInt32(arr.GetValue(2));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_TRANSPORTER_PKG.Transporter_Depo_Wise";

                var _with1 = selectCommand.Parameters;
                _with1.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with1.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with1.Add("LOCATION_MST_FK_IN", loc).Direction = ParameterDirection.Input;
                _with1.Add("BIZ_TYPE_IN", strBIZ_TYPE_IN).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }

        }
        #endregion

        #region "SaveHeader"
        public ArrayList SaveHeader(DataSet dsHdr, DataTable dtParent, DataTable dtChild, Int32 onHirePk)
        {

            try
            {
                WorkFlow objWK = new WorkFlow();
                OracleCommand insCommand = new OracleCommand();
                OracleCommand updCommand = new OracleCommand();
                OracleTransaction insertTrans = null;
                Int32 RecAfct = default(Int32);

                objWK.OpenConnection();
                objWK.MyCommand.Connection = objWK.MyConnection;
                insertTrans = objWK.MyConnection.BeginTransaction();
                if (onHirePk == 0)
                {
                    var _with2 = insCommand;
                    _with2.Connection = objWK.MyConnection;
                    _with2.CommandType = CommandType.StoredProcedure;
                    _with2.CommandText = objWK.MyUserName + ".wo_onhire_hdr_pkg.wo_onhire_hdr_INS";
                    insCommand.Parameters.Clear();
                    var _with3 = _with2.Parameters;

                    insCommand.Parameters.Add("WO_NUMBER_in", OracleDbType.Varchar2, 20, "WO_NUMBER").Direction = ParameterDirection.Input;
                    insCommand.Parameters["WO_NUMBER_in"].SourceVersion = DataRowVersion.Current;

                    if (!string.IsNullOrEmpty(dsHdr.Tables[0].Rows[0]["WO_DATE"].ToString()))
                    {
                        insCommand.Parameters.Add("WO_DATE_in", Convert.ToDateTime(dsHdr.Tables[0].Rows[0]["WO_DATE"])).Direction = ParameterDirection.Input;
                        insCommand.Parameters["WO_DATE_in"].SourceVersion = DataRowVersion.Current;
                    }
                    else
                    {
                        insCommand.Parameters.Add("WO_DATE_in", DBNull.Value).Direction = ParameterDirection.Input;
                        insCommand.Parameters["WO_DATE_in"].SourceVersion = DataRowVersion.Current;
                    }

                    insCommand.Parameters.Add("ON_HIRE_NUMBER_in", OracleDbType.Varchar2, 25, "ON_HIRE_NUMBER").Direction = ParameterDirection.Input;
                    insCommand.Parameters["ON_HIRE_NUMBER_in"].SourceVersion = DataRowVersion.Current;


                    if (!string.IsNullOrEmpty(dsHdr.Tables[0].Rows[0]["ON_HIRE_DATE"].ToString()))
                    {
                        insCommand.Parameters.Add("ON_HIRE_DATE_in", Convert.ToDateTime(dsHdr.Tables[0].Rows[0]["ON_HIRE_DATE"])).Direction = ParameterDirection.Input;
                        insCommand.Parameters["ON_HIRE_DATE_in"].SourceVersion = DataRowVersion.Current;
                    }
                    else
                    {
                        insCommand.Parameters.Add("ON_HIRE_DATE_in", DBNull.Value).Direction = ParameterDirection.Input;
                        insCommand.Parameters["ON_HIRE_DATE_in"].SourceVersion = DataRowVersion.Current;
                    }
                    insCommand.Parameters.Add("PICKUP_REF_NO_in", OracleDbType.Varchar2, 20, "PICKUP_REF_NO").Direction = ParameterDirection.Input;
                    insCommand.Parameters["PICKUP_REF_NO_in"].SourceVersion = DataRowVersion.Current;

                    if (!string.IsNullOrEmpty(dsHdr.Tables[0].Rows[0]["PICKUP_DATE"].ToString()))
                    {
                        insCommand.Parameters.Add("PICKUP_DATE_in", Convert.ToDateTime(dsHdr.Tables[0].Rows[0]["PICKUP_DATE"])).Direction = ParameterDirection.Input;
                        insCommand.Parameters["PICKUP_DATE_in"].SourceVersion = DataRowVersion.Current;
                    }
                    else
                    {
                        insCommand.Parameters.Add("PICKUP_DATE_in", DBNull.Value).Direction = ParameterDirection.Input;
                        insCommand.Parameters["PICKUP_DATE_in"].SourceVersion = DataRowVersion.Current;
                    }

                    insCommand.Parameters.Add("TRANSPORTER_FK_in", OracleDbType.Varchar2, 10, "TRANSPORTER_MST_PK").Direction = ParameterDirection.Input;
                    insCommand.Parameters["TRANSPORTER_FK_in"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("VEHICLE_NO_in", OracleDbType.Varchar2, 20, "VEHICLE_NO").Direction = ParameterDirection.Input;
                    insCommand.Parameters["VEHICLE_NO_in"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("SHIPPING_LINE_FK_in", OracleDbType.Int32, 10, "OPERATOR_MST_PK").Direction = ParameterDirection.Input;
                    insCommand.Parameters["SHIPPING_LINE_FK_in"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("DEPOT_SUPP_WH_FK_in", OracleDbType.Int32, 10, "VENDOR_MST_PK").Direction = ParameterDirection.Input;
                    insCommand.Parameters["DEPOT_SUPP_WH_FK_in"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("CREATED_BY_FK_in", M_CREATED_BY_FK).Direction = ParameterDirection.Input;

                    insCommand.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                    insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "on_hire_pk").Direction = ParameterDirection.Output;
                    insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                    
                    var _with4 = objWK.MyDataAdapter;
                    _with4.InsertCommand = insCommand;
                    _with4.InsertCommand.Transaction = insertTrans;
                    RecAfct = _with4.Update(dsHdr);
                    if (RecAfct > 0)
                    {
                        onHirePk = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
                        if (SaveParent(dtParent, dtChild, onHirePk, insertTrans) == 0)
                        {
                            insertTrans.Commit();
                            arrMessage.Add("All Data Saved Successfully");
                            arrMessage.Add(onHirePk);
                            return arrMessage;
                        }
                    }
                    else
                    {
                        insertTrans.Rollback();
                        return arrMessage;
                    }

                }
                else
                {
                    var _with5 = insCommand;
                    _with5.Connection = objWK.MyConnection;
                    _with5.CommandType = CommandType.StoredProcedure;
                    _with5.CommandText = objWK.MyUserName + ".wo_onhire_hdr_pkg.wo_onhire_hdr_UPD";
                    insCommand.Parameters.Clear();
                    var _with6 = _with5.Parameters;
                    insCommand.Parameters.Add("on_hire_pk_in", OracleDbType.Int32, 10, "ON_HIRE_PK").Direction = ParameterDirection.Input;
                    insCommand.Parameters["on_hire_pk_in"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("WO_NUMBER_in", OracleDbType.Int32, 20, "WO_NUMBER").Direction = ParameterDirection.Input;
                    insCommand.Parameters["WO_NUMBER_in"].SourceVersion = DataRowVersion.Current;

                    if (!string.IsNullOrEmpty(dsHdr.Tables[0].Rows[0]["WO_DATE"].ToString()))
                    {
                        insCommand.Parameters.Add("WO_DATE_in", Convert.ToDateTime(dsHdr.Tables[0].Rows[0]["WO_DATE"])).Direction = ParameterDirection.Input;
                        insCommand.Parameters["WO_DATE_in"].SourceVersion = DataRowVersion.Current;
                    }
                    else
                    {
                        insCommand.Parameters.Add("WO_DATE_in", DBNull.Value).Direction = ParameterDirection.Input;
                        insCommand.Parameters["WO_DATE_in"].SourceVersion = DataRowVersion.Current;
                    }

                    insCommand.Parameters.Add("PICKUP_REF_NO_in", OracleDbType.Varchar2, 20, "PICKUP_REF_NO").Direction = ParameterDirection.Input;
                    insCommand.Parameters["PICKUP_REF_NO_in"].SourceVersion = DataRowVersion.Current;

                    if (!string.IsNullOrEmpty(dsHdr.Tables[0].Rows[0]["PICKUP_DATE"].ToString()))
                    {
                        insCommand.Parameters.Add("PICKUP_DATE_in", Convert.ToDateTime(dsHdr.Tables[0].Rows[0]["PICKUP_DATE"])).Direction = ParameterDirection.Input;
                        insCommand.Parameters["PICKUP_DATE_in"].SourceVersion = DataRowVersion.Current;
                    }
                    else
                    {
                        insCommand.Parameters.Add("PICKUP_DATE_in", DBNull.Value).Direction = ParameterDirection.Input;
                        insCommand.Parameters["PICKUP_DATE_in"].SourceVersion = DataRowVersion.Current;
                    }

                    insCommand.Parameters.Add("TRANSPORTER_FK_in", OracleDbType.Varchar2, 10, "TRANSPORTER_MST_PK").Direction = ParameterDirection.Input;
                    insCommand.Parameters["TRANSPORTER_FK_in"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("VEHICLE_NO_in", OracleDbType.Varchar2, 20, "VEHICLE_NO").Direction = ParameterDirection.Input;
                    insCommand.Parameters["VEHICLE_NO_in"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("SHIPPING_LINE_FK_in", OracleDbType.Int32, 10, "OPERATOR_MST_PK").Direction = ParameterDirection.Input;
                    insCommand.Parameters["SHIPPING_LINE_FK_in"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("DEPOT_SUPP_WH_FK_in", OracleDbType.Int32, 10, "VENDOR_MST_PK").Direction = ParameterDirection.Input;
                    insCommand.Parameters["DEPOT_SUPP_WH_FK_in"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;

                    insCommand.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

                    insCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "version_no").Direction = ParameterDirection.Input;
                    insCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                    
                    var _with7 = objWK.MyDataAdapter;
                    _with7.UpdateCommand = insCommand;
                    _with7.UpdateCommand.Transaction = insertTrans;
                    RecAfct = _with7.Update(dsHdr);
                    if (RecAfct > 0)
                    {
                        onHirePk = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
                        if (SaveParent(dtParent, dtChild, onHirePk, insertTrans) == 0)
                        {
                            insertTrans.Commit();
                            arrMessage.Add("All Data Saved Successfully");
                            arrMessage.Add(onHirePk);
                        }
                    }
                    else
                    {
                        insertTrans.Rollback();
                        return arrMessage;
                    }
                    return arrMessage;
                }
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return new ArrayList();
        }
        #endregion

        #region "SaveParent"
        public Int32 SaveParent(DataTable dtParent, DataTable dtChild, long onHirePk, OracleTransaction TRAN)
        {
            WorkFlow objWK = new WorkFlow();
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            Int32 RecAfct = default(Int32);
            Int32 onHireDtlPk = default(Int32);
            Int32 i = default(Int32);
            try
            {
                objWK.OpenConnection();
                objWK.MyConnection = TRAN.Connection;
                for (i = 0; i <= dtParent.Rows.Count - 1; i++)
                {
                    if (Convert.ToInt32(getDefault(dtParent.Rows[i]["on_hire_dtl_pk"], 0)) == 0)
                    {
                        var _with8 = insCommand;
                        _with8.Connection = objWK.MyConnection;
                        _with8.CommandType = CommandType.StoredProcedure;
                        _with8.CommandText = objWK.MyUserName + ".WO_ONHIRE_DTL_pkg.WO_ONHIRE_DTL_INS";

                        var _with9 = _with8.Parameters;
                        _with9.Clear();

                        insCommand.Parameters.Add("on_hire_hdr_fk_in", onHirePk).Direction = ParameterDirection.Input;
                        insCommand.Parameters["on_hire_hdr_fk_in"].SourceVersion = DataRowVersion.Current;


                        insCommand.Parameters.Add("CONT_TYPE_MST_FK_in", getDefault(dtParent.Rows[i]["CTYPE_PK"], DBNull.Value)).Direction = ParameterDirection.Input;
                        insCommand.Parameters["CONT_TYPE_MST_FK_in"].SourceVersion = DataRowVersion.Current;

                        insCommand.Parameters.Add("ONHIRE_QTY_in", getDefault(dtParent.Rows[i]["onhire_qty"], DBNull.Value)).Direction = ParameterDirection.Input;
                        insCommand.Parameters["ONHIRE_QTY_in"].SourceVersion = DataRowVersion.Current;

                        insCommand.Parameters.Add("ONHIRE_PER_DIEM_in", getDefault(dtParent.Rows[i]["onhire_per_diem"], DBNull.Value)).Direction = ParameterDirection.Input;
                        insCommand.Parameters["ONHIRE_PER_DIEM_in"].SourceVersion = DataRowVersion.Current;

                        insCommand.Parameters.Add("ONHIRE_COST_in", getDefault(dtParent.Rows[i]["onhire_cost"], DBNull.Value)).Direction = ParameterDirection.Input;
                        insCommand.Parameters["ONHIRE_COST_in"].SourceVersion = DataRowVersion.Current;

                        insCommand.Parameters.Add("CURRENCY_FK_in", getDefault(dtParent.Rows[i]["currency_mst_pk"], DBNull.Value)).Direction = ParameterDirection.Input;
                        insCommand.Parameters["CURRENCY_FK_in"].SourceVersion = DataRowVersion.Current;

                        insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "ON_HIRE_HDR_FK").Direction = ParameterDirection.Output;
                        insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                        var _with10 = objWK.MyDataAdapter;
                        _with10.InsertCommand = insCommand;
                        _with10.InsertCommand.Transaction = TRAN;
                        _with10.InsertCommand.ExecuteNonQuery();
                        onHireDtlPk = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
                        RecAfct = (SaveChild(dtChild, onHirePk, onHireDtlPk, TRAN));

                    }
                    else
                    {
                        RecAfct = (SaveChild(dtChild, onHirePk, onHireDtlPk, TRAN));
                    }
                }


                if (RecAfct == 0)
                {
                    return RecAfct;
                }
                else
                {
                    return RecAfct;
                    TRAN.Rollback();
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
        }
        #endregion

        #region "SaveChild"
        public Int32 SaveChild(DataTable dtChild, long onHirePk, long onHireDtlPk, OracleTransaction TRAN)
        {
            WorkFlow objWK = new WorkFlow();
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            OracleCommand delCommand = new OracleCommand();
            Int32 RecAfct = default(Int32);
            Int32 i = default(Int32);

            try
            {
                objWK.OpenConnection();
                objWK.MyConnection = TRAN.Connection;
                for (i = 0; i <= dtChild.Rows.Count - 1; i++)
                {
                    if (Convert.ToInt32(getDefault(dtChild.Rows[i]["on_hire_cont_pk"], 0)) == 0)
                    {
                        var _with11 = insCommand;
                        _with11.Connection = objWK.MyConnection;
                        _with11.CommandType = CommandType.StoredProcedure;
                        _with11.CommandText = objWK.MyUserName + ".WO_ONHIRE_CONT_DTL_pkg.WO_ONHIRE_CONT_DTL_INS";

                        var _with12 = _with11.Parameters;
                        _with12.Clear();
                        insCommand.Parameters.Add("ON_HIRE_DTL_FK_in", onHireDtlPk).Direction = ParameterDirection.Input;
                        insCommand.Parameters["ON_HIRE_DTL_FK_in"].SourceVersion = DataRowVersion.Current;

                        insCommand.Parameters.Add("ON_HIRE_HDR_FK_in", onHirePk).Direction = ParameterDirection.Input;
                        insCommand.Parameters["ON_HIRE_HDR_FK_in"].SourceVersion = DataRowVersion.Current;

                        insCommand.Parameters.Add("CONT_TYPE_MST_FK_in", getDefault(dtChild.Rows[i]["container_type_mst_pk"], DBNull.Value)).Direction = ParameterDirection.Input;
                        insCommand.Parameters["CONT_TYPE_MST_FK_in"].SourceVersion = DataRowVersion.Current;

                        insCommand.Parameters.Add("CONT_NO_in", getDefault(dtChild.Rows[i]["cont_no"], DBNull.Value)).Direction = ParameterDirection.Input;
                        insCommand.Parameters["CONT_NO_in"].SourceVersion = DataRowVersion.Current;

                        insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "ON_HIRE_CONT_PK").Direction = ParameterDirection.Output;
                        insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


                        var _with13 = objWK.MyDataAdapter;
                        _with13.InsertCommand = insCommand;
                        _with13.InsertCommand.Transaction = TRAN;
                        _with13.InsertCommand.ExecuteNonQuery();
                        RecAfct = 0;
                    }
                    else
                    {
                        var _with14 = updCommand;
                        _with14.Connection = objWK.MyConnection;
                        _with14.CommandType = CommandType.StoredProcedure;
                        _with14.CommandText = objWK.MyUserName + ".WO_ONHIRE_CONT_DTL_pkg.WO_ONHIRE_CONT_DTL_UPD";
                        var _with15 = _with14.Parameters;
                        _with15.Clear();

                        updCommand.Parameters.Add("ON_HIRE_CONT_PK_IN", getDefault(dtChild.Rows[i]["on_hire_cont_pk"], DBNull.Value)).Direction = ParameterDirection.Input;
                        updCommand.Parameters["ON_HIRE_CONT_PK_IN"].SourceVersion = DataRowVersion.Current;

                        updCommand.Parameters.Add("CONT_NO_in", getDefault(dtChild.Rows[i]["cont_no"], DBNull.Value)).Direction = ParameterDirection.Input;
                        updCommand.Parameters["CONT_NO_in"].SourceVersion = DataRowVersion.Current;


                        updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                        var _with16 = objWK.MyDataAdapter;
                        _with16.UpdateCommand = updCommand;
                        _with16.UpdateCommand.Transaction = TRAN;
                        _with16.UpdateCommand.ExecuteNonQuery();
                        RecAfct = 0;
                    }
                }
                return RecAfct;
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
        }
        #endregion

        #region "GenerateWorkorderNo"
        public string GenerateWorkorderNo(long nLocationId, long nEmployeeId)
        {
            string GenerateWorkNo = null;
            WorkFlow objWK = null;
            GenerateWorkNo = GenerateProtocolKey("ONHIRE", nLocationId, nEmployeeId, DateTime.Now, "", "", "", M_CREATED_BY_FK, objWK);
            return GenerateWorkNo;
        }
        #endregion

    }
}