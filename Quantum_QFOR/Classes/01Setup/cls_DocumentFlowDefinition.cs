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

using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections;
using System.Data;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_DocumentFlowDefinition : CommonFeatures
    {
        /// <summary>
        /// The dt attach
        /// </summary>
        private DataTable dtAttach = new DataTable();

        /// <summary>
        /// The ds attach
        /// </summary>
        private DataSet dsAttach = new DataSet();

        #region " GetDocumentFlowDescription "

        /// <summary>
        /// Gets the document flow description.
        /// </summary>
        /// <returns></returns>
        public DataSet GetDocumentFlowDescription()
        {
            try
            {
                WorkFlow objWF = new WorkFlow();
                string Sql = null;
                Sql = "SELECT * FROM DOCUMENT_FLOW_TBL ";
                return objWF.GetDataSet(Sql.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion " GetDocumentFlowDescription "

        #region " FileDetails "

        /// <summary>
        /// Gets the records for loc.
        /// </summary>
        /// <param name="LocPK">The loc pk.</param>
        /// <param name="DocPK">The document pk.</param>
        /// <returns></returns>
        public DataTable GetRecordsForLoc(int LocPK, int DocPK)
        {
            WorkFlow ObjWF = new WorkFlow();
            string Sql = null;
            try
            {
                Sql = " SELECT * FROM DOCUMENT_FLOW_DTL D WHERE D.LOGED_IN_LOC_FK=" + LocPK + " AND D.DOCUMENT_FLOW_FK=" + DocPK + "";
                DataTable dt = ObjWF.GetDataTable(Sql.ToString());
                return dt;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion " FileDetails "

        #region " GetDeatilsForLoc "

        /// <summary>
        /// Gets the deatils for loc.
        /// </summary>
        /// <param name="LocPK">The loc pk.</param>
        /// <returns></returns>
        public DataTable GetDeatilsForLoc(int LocPK)
        {
            WorkFlow ObjWF = new WorkFlow();
            string Sql = null;
            try
            {
                Sql = "SELECT * FROM  DOCUMENT_FLOW_DTL dtl WHERE dtl.loged_in_loc_fk=" + LocPK + "";
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return ObjWF.GetDataTable(Sql.ToString());
        }

        #endregion " GetDeatilsForLoc "

        #region " Fetch Data "

        /// <summary>
        /// Fetches the data.
        /// </summary>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <returns></returns>
        public object FetchData(Int32 CurrentPage, Int32 TotalPage)
        {
            DataSet DS = null;
            DataTable dt = null;
            WorkFlow ObjWF = new WorkFlow();
            try
            {
                DS = GetDocumentFlowDescription();
                dt = GetDeatilsForLoc((int)HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("SELECT Q.DOCUMENT_FLOW_PK   VIEW_DOCUMENT_PK,");
                sb.Append("       Q.DESCRIPTION        VIEW_DOCUMENT,");
                sb.Append("       D.FLAG1  \"ANMT\",");
                sb.Append("       D.FLAG2  \"SLC\",");
                sb.Append("       D.FLAG3  \"RFQ\",");
                sb.Append("       D.FLAG4  \"SAC\",");
                sb.Append("       D.FLAG5  \"WHC\",");
                sb.Append("       D.FLAG6  \"TRC\",");
                sb.Append("       D.FLAG7  \"ENQ.\",");
                sb.Append("       D.FLAG8  \"QTN.\",");
                sb.Append("       D.FLAG9  \"BKG.\",");
                sb.Append("       D.FLAG10 \"CRO\",");
                sb.Append("       D.FLAG11 \"EJC\",");
                sb.Append("       D.FLAG12 \"IJC\",");
                sb.Append("       D.FLAG13 \"BL/AWB\",");
                sb.Append("       D.FLAG14 \"CAN\",");
                sb.Append("       D.FLAG15 \"DO\",");
                sb.Append("       D.FLAG16 \"CBJC\",");
                sb.Append("       D.FLAG17 \"TPN\"");
                sb.Append("  FROM DOCUMENT_FLOW_TBL Q, DOCUMENT_FLOW_DTL D");
                sb.Append(" WHERE Q.DOCUMENT_FLOW_PK = D.DOCUMENT_FLOW_FK(+)");
                sb.Append(" AND D.LOGED_IN_LOC_FK(+) = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
                sb.Append(" ORDER BY Q.DOCUMENT_FLOW_PK");
                Int32 TotalRecords = default(Int32);
                Int32 last = default(Int32);
                Int32 start = default(Int32);
                DataSet DSCount = new DataSet();
                System.Text.StringBuilder strCount = new System.Text.StringBuilder();
                strCount.Append(" SELECT COUNT(*)  from  ");
                strCount.Append((" (" + sb.ToString() + ")"));

                DSCount = ObjWF.GetDataSet(strCount.ToString());
                TotalRecords = Convert.ToInt32(DSCount.Tables[0].Rows[0][0]);

                TotalPage = TotalRecords / 20;
                if (TotalRecords % 20 != 0)
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
                last = CurrentPage * 20;
                start = (CurrentPage - 1) * 20 + 1;

                System.Text.StringBuilder sbCount = new System.Text.StringBuilder(5000);
                sbCount.Append(" SELECT Q.* FROM ");
                sbCount.Append("(SELECT ROWNUM \"SLNO\", QRY.*  FROM ");
                sbCount.Append("  (" + sb.ToString() + " ");
                sbCount.Append("  ) QRY) Q  WHERE \"SLNO\"  BETWEEN " + start + " AND " + last + " ORDER BY \"SLNO\"");
                return ObjWF.GetDataSet(sbCount.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion " Fetch Data "

        #region " Save "

        /// <summary>
        /// Saves the specified dt.
        /// </summary>
        /// <param name="dt">The dt.</param>
        /// <returns></returns>
        public ArrayList Save(DataTable dt)
        {
            WorkFlow ObjWF = new WorkFlow();
            string Sql = null;
            DataSet DS = null;
            bool Existing = false;
            ArrayList arrMessage = new ArrayList();
            int DtlPK = 0;
            int ModPK = 0;
            Int32 RecAfct = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            OracleTransaction Tran = null;
            ObjWF.MyConnection.Open();
            Tran = ObjWF.MyConnection.BeginTransaction();
            DataTable dtTemp = null;
            try
            {
                if (dt.Rows.Count > 0)
                {
                    for (int i = 0; i <= dt.Rows.Count - 1; i++)
                    {
                        dtTemp = GetRecordsForLoc((int)dt.Rows[i]["LOGED_IN_LOC_FK"], (int)dt.Rows[i]["PK"]);
                        if (dtTemp.Rows.Count > 0)
                        {
                            //update
                            var _with1 = updCommand;
                            _with1.Parameters.Clear();
                            _with1.Connection = ObjWF.MyConnection;
                            _with1.CommandType = CommandType.StoredProcedure;
                            _with1.CommandText = ObjWF.MyUserName + ".DOCUMENT_FLOW_DTL_PKG.DOCUMENT_FLOW_DTL_UPD";
                            var _with2 = _with1.Parameters;
                            _with2.Add("DOCUMENT_FLOW_DTL_PK_IN", dtTemp.Rows[0]["DOCUMENT_FLOW_DTL_PK"]).Direction = ParameterDirection.Input;
                            _with2.Add("DOCUMENT_FLOW_FK_IN", dt.Rows[i]["PK"]).Direction = ParameterDirection.Input;
                            _with2.Add("FLAG1_IN", dt.Rows[i]["Flag1"]).Direction = ParameterDirection.Input;
                            _with2.Add("FLAG2_IN", dt.Rows[i]["Flag2"]).Direction = ParameterDirection.Input;
                            _with2.Add("FLAG3_IN", dt.Rows[i]["Flag3"]).Direction = ParameterDirection.Input;
                            _with2.Add("FLAG4_IN", dt.Rows[i]["Flag4"]).Direction = ParameterDirection.Input;
                            _with2.Add("FLAG5_IN", dt.Rows[i]["Flag5"]).Direction = ParameterDirection.Input;
                            _with2.Add("FLAG6_IN", dt.Rows[i]["Flag6"]).Direction = ParameterDirection.Input;
                            _with2.Add("FLAG7_IN", dt.Rows[i]["Flag7"]).Direction = ParameterDirection.Input;
                            _with2.Add("FLAG8_IN", dt.Rows[i]["Flag8"]).Direction = ParameterDirection.Input;
                            _with2.Add("FLAG9_IN", dt.Rows[i]["Flag9"]).Direction = ParameterDirection.Input;
                            _with2.Add("FLAG10_IN", dt.Rows[i]["Flag10"]).Direction = ParameterDirection.Input;
                            _with2.Add("FLAG11_IN", dt.Rows[i]["Flag11"]).Direction = ParameterDirection.Input;
                            _with2.Add("FLAG12_IN", dt.Rows[i]["Flag12"]).Direction = ParameterDirection.Input;
                            _with2.Add("FLAG13_IN", dt.Rows[i]["Flag13"]).Direction = ParameterDirection.Input;
                            _with2.Add("FLAG14_IN", dt.Rows[i]["Flag14"]).Direction = ParameterDirection.Input;
                            _with2.Add("FLAG15_IN", dt.Rows[i]["Flag15"]).Direction = ParameterDirection.Input;
                            _with2.Add("FLAG16_IN", dt.Rows[i]["Flag16"]).Direction = ParameterDirection.Input;
                            _with2.Add("FLAG17_IN", dt.Rows[i]["Flag17"]).Direction = ParameterDirection.Input;
                            _with2.Add("LAST_MODIFIED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                            _with2.Add("LOGED_IN_LOC_FK_IN", dt.Rows[i]["LOGED_IN_LOC_FK"]).Direction = ParameterDirection.Input;
                            _with2.Add("VERSION_NO_IN", dtTemp.Rows[0]["VERSION_NO"]).Direction = ParameterDirection.Input;
                            _with2.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                            _with2.Add("RETURN_VALUE", OracleDbType.Int32, 10, "DOCUMENT_FLOW_DTL_PK").Direction = ParameterDirection.Output;
                            var _with3 = ObjWF.MyDataAdapter;
                            _with3.UpdateCommand = updCommand;
                            _with3.UpdateCommand.Transaction = Tran;
                            _with3.UpdateCommand.ExecuteNonQuery();

                            int UpdPK = (int)updCommand.Parameters["RETURN_VALUE"].Value;
                        }
                        else
                        {
                            //insert
                            var _with4 = insCommand;
                            _with4.Parameters.Clear();
                            _with4.Connection = ObjWF.MyConnection;
                            _with4.CommandType = CommandType.StoredProcedure;
                            _with4.CommandText = ObjWF.MyUserName + ".DOCUMENT_FLOW_DTL_PKG.DOCUMENT_FLOW_DTL_INS";
                            var _with5 = _with4.Parameters;
                            _with5.Add("DOCUMENT_FLOW_FK_IN", dt.Rows[i]["PK"]).Direction = ParameterDirection.Input;
                            _with5.Add("FLAG1_IN", dt.Rows[i]["Flag1"]).Direction = ParameterDirection.Input;
                            _with5.Add("FLAG2_IN", dt.Rows[i]["Flag2"]).Direction = ParameterDirection.Input;
                            _with5.Add("FLAG3_IN", dt.Rows[i]["Flag3"]).Direction = ParameterDirection.Input;
                            _with5.Add("FLAG4_IN", dt.Rows[i]["Flag4"]).Direction = ParameterDirection.Input;
                            _with5.Add("FLAG5_IN", dt.Rows[i]["Flag5"]).Direction = ParameterDirection.Input;
                            _with5.Add("FLAG6_IN", dt.Rows[i]["Flag6"]).Direction = ParameterDirection.Input;
                            _with5.Add("FLAG7_IN", dt.Rows[i]["Flag7"]).Direction = ParameterDirection.Input;
                            _with5.Add("FLAG8_IN", dt.Rows[i]["Flag8"]).Direction = ParameterDirection.Input;
                            _with5.Add("FLAG9_IN", dt.Rows[i]["Flag9"]).Direction = ParameterDirection.Input;
                            _with5.Add("FLAG10_IN", dt.Rows[i]["Flag10"]).Direction = ParameterDirection.Input;
                            _with5.Add("FLAG11_IN", dt.Rows[i]["Flag11"]).Direction = ParameterDirection.Input;
                            _with5.Add("FLAG12_IN", dt.Rows[i]["Flag12"]).Direction = ParameterDirection.Input;
                            _with5.Add("FLAG13_IN", dt.Rows[i]["Flag13"]).Direction = ParameterDirection.Input;
                            _with5.Add("FLAG14_IN", dt.Rows[i]["Flag14"]).Direction = ParameterDirection.Input;
                            _with5.Add("FLAG15_IN", dt.Rows[i]["Flag15"]).Direction = ParameterDirection.Input;
                            _with5.Add("FLAG16_IN", dt.Rows[i]["Flag16"]).Direction = ParameterDirection.Input;
                            _with5.Add("FLAG17_IN", dt.Rows[i]["Flag17"]).Direction = ParameterDirection.Input;
                            _with5.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                            _with5.Add("LOGED_IN_LOC_FK_IN", dt.Rows[i]["LOGED_IN_LOC_FK"]).Direction = ParameterDirection.Input;
                            _with5.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                            _with5.Add("RETURN_VALUE", OracleDbType.Int32, 10, "DOCUMENT_FLOW_DTL_PK").Direction = ParameterDirection.Output;
                            var _with6 = ObjWF.MyDataAdapter;
                            _with6.InsertCommand = insCommand;
                            _with6.InsertCommand.Transaction = Tran;
                            RecAfct = _with6.InsertCommand.ExecuteNonQuery();
                            DtlPK = (int)insCommand.Parameters["RETURN_VALUE"].Value;
                        }
                    }
                }
                Tran.Commit();
                arrMessage.Add("Saved");
                return arrMessage;
            }
            catch (Exception ex)
            {
                Tran.Rollback();
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
        }

        #endregion " Save "
    }
}