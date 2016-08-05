using Newtonsoft.Json;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web;

namespace Quantum_QFOR
{
    public class cls_DOCUMENT_MST_TBL : CommonFeatures
    {

        #region "generate protocol"
        public string Generate_Document_Id()
        {
            System.Web.UI.Page objPage = new System.Web.UI.Page();
            return GenerateProtocolKey("DOCUMENT TYPE", (Int32)objPage.Session["LOGED_IN_LOC_FK"], (Int32)objPage.Session["EMP_PK"], DateTime.Now, "", "", "", (Int32)objPage.Session["USER_PK"], new WorkFlow() );
        }
        #endregion

        #region "Fetch Function"
        public DataSet FetchById(long lngDocPk)
        {
            StringBuilder strBuilder = new StringBuilder();
            strBuilder.Append(" SELECT DOCUMENT_ID,");
            strBuilder.Append(" DOCUMENT_NAME_MST_FK, ");
            strBuilder.Append(" DOCUMENT_TYPE_MST_FK, ");
            strBuilder.Append(" DOCUMENT_GROUP_MST_FK, ");
            strBuilder.Append(" DOCUMENT_SUBJECT, ");
            strBuilder.Append(" DOCUMENT_HEADER, ");
            strBuilder.Append(" DOCUMENT_BODY, ");
            strBuilder.Append(" DOCUMENT_FOOTER, ");
            strBuilder.Append(" MESSAGE_FOLDER_MST_FK, ");
            strBuilder.Append(" ATTACHMENT_URL, ");
            strBuilder.Append(" ACTIVE, ");
            strBuilder.Append(" VERSION_NO");
            strBuilder.Append(" FROM DOCUMENT_MST_TBL ");
            strBuilder.Append(" WHERE DOCUMENT_MST_PK = " + lngDocPk + "");
            WorkFlow objWF = new WorkFlow();
            try
            {
                return objWF.GetDataSet(strBuilder.ToString());
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
        public DataTable GetAllDocs(bool Active = true)
        {
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (Active)
                {
                    return objWF.GetDataTable("SELECT * FROM DOCUMENT_MST_TBL WHERE ACTIVE=1 ORDER BY DOCUMENT_ID");
                }
                else
                {
                    return objWF.GetDataTable("SELECT * FROM DOCUMENT_MST_TBL ORDER BY DOCUMENT_ID");
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
            return new DataTable();
        }
        public string FetchAll(string strDocId, int intDocName, Int32 biztype, int intDocTypeFk, int intDocGroupFk, short intActive, string SearchType = "", string strColumnName = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, bool blnSortAscending = false,
        Int32 flag = 0)
        {

            StringBuilder strBuilder = new StringBuilder();
            StringBuilder strCondition = new StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            if (flag == 0)
            {
                strCondition.Append(" AND 1=2");
            }
            if (strDocId.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strCondition.Append(" And upper(DOC.DOCUMENT_ID) like '%" + strDocId.ToUpper().Replace("'", "''") + "%' ");
                }
                else
                {
                    strCondition.Append("  And upper(DOC.DOCUMENT_ID) like '" + strDocId.ToUpper().Replace("'", "''") + "%' ");
                }
            }

            if (intDocName > 0)
            {
                strCondition.Append("  And DOC.DOCUMENT_NAME_MST_FK=  " + intDocName + "");
            }

            if (intDocTypeFk > 0)
            {
                strCondition.Append("  And DOC.DOCUMENT_TYPE_MST_FK=  " + intDocTypeFk + "");
            }

            if (intDocGroupFk > 0)
            {
                strCondition.Append("  And DOC.DOCUMENT_GROUP_MST_FK=" + intDocGroupFk + "");
            }

            if (intActive > 0)
            {
                strCondition.Append(" And Active =1 ");
            }

            strBuilder.Append(" SELECT COUNT(*) FROM DOCUMENT_MST_TBL DOC where 1=1 ");
            strBuilder.Append(strCondition.ToString());
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strBuilder.ToString()));
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
            strBuilder = new StringBuilder();

            strBuilder.Append(" SELECT * FROM (");
            strBuilder.Append(" SELECT ROWNUM SR_NO,q.* FROM ( SELECT");
            strBuilder.Append(" DOC.DOCUMENT_MST_PK,");
            strBuilder.Append(" DOC.ACTIVE,");
            strBuilder.Append(" DOC.DOCUMENT_ID,");
            strBuilder.Append(" DNAME.DOCUMENT_NAME,");
            strBuilder.Append(" DTYPE.DOCUMENT_TYPE,");
            strBuilder.Append(" DGRP.DOCUMENT_GROUP,");
            strBuilder.Append(" DOC.VERSION_NO ");
            strBuilder.Append(" FROM DOCUMENT_MST_TBL DOC, ");
            strBuilder.Append(" DOCUMENT_TYPE_MST_TBL DTYPE, ");
            strBuilder.Append(" DOCUMENT_GROUP_MST_TBL DGRP,");
            strBuilder.Append(" DOCUMENT_NAME_MST_TBL DNAME");
            strBuilder.Append(" WHERE DOC.DOCUMENT_TYPE_MST_FK = DTYPE.DOCUMENT_TYPE_MST_FK");
            strBuilder.Append("   AND DNAME.DOCUMENT_NAME_MST_PK = DOC.DOCUMENT_NAME_MST_FK");
            strBuilder.Append("   AND DOC.DOCUMENT_GROUP_MST_FK = DGRP.DOCUMENT_GROUP_MST_PK ");
            if (Convert.ToInt32(biztype) == 1)
            {
                strBuilder.Append("   AND DOC.BIZ_TYPE IN (1,3)");
            }
            else if (Convert.ToInt32(biztype) == 2)
            {
                strBuilder.Append("   AND DOC.BIZ_TYPE IN (2,3)");
            }
            strBuilder.Append(strCondition.ToString());

            if (!strColumnName.Equals("SR_NO"))
            {
                strBuilder.Append(" ORDER BY " + strColumnName + "");
            }

            if (!blnSortAscending & !strColumnName.Equals("SR_NO"))
            {
                strBuilder.Append(" DESC");
            }
            strBuilder.Append("  ) q) ");
            //strBuilder.Append(" WHERE SR_NO  BETWEEN " + start + " AND " + last + "");
            try
            {
                DataSet objDS = objWF.GetDataSet(strBuilder.ToString());
                return JsonConvert.SerializeObject(objDS, Formatting.Indented);
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

        #region "Fetcg workFlow"
        public DataSet fetchworkflow(Int64 DOCID)
        {
            string strSQL = null;
            strSQL = "SELECT ROWNUM";
            strSQL += " SLNO,";
            strSQL += " DPMT.DOCUMENT_PARAMETER_MST_PK,";
            strSQL += " DPMT.DOCUMENT_PARAMETER_FIELD,";
            strSQL += " DPMT.DOCUMENT_PARAMETER_FIELD_DESC";
            strSQL += " FROM  DOCUMENT_PARAMETER_MST_TBL DPMT, DOCUMENT_MST_TBL DMT ";
            strSQL += "WHERE " ;
            strSQL += " DPMT.DOCUMENT_MST_FK = DMT.DOCUMENT_MST_PK " ;
            strSQL += "AND DMT.DOCUMENT_MST_PK = " + DOCID + " " ;
            WorkFlow objWF = new WorkFlow();
            try
            {
                return objWF.GetDataSet(strSQL);
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion

        #region "Fetch Document & Group"
        public DataSet FetchDocType()
        {
            try
            {
                string biz_type = null;
                if (Convert.ToInt32(HttpContext.Current.Session["BIZ_TYPE"]) == 1)
                {
                    biz_type = "1,3";
                }
                else if (Convert.ToInt32(HttpContext.Current.Session["BIZ_TYPE"]) == 2)
                {
                    biz_type = "2,3";
                }
                else
                {
                    biz_type = "1,2,3";
                }
                string StrSql = null;
                WorkFlow objWF = new WorkFlow();
                StrSql = "Select  Document_Type_Mst_Fk,upper(Document_Type) Document_Type From Document_Type_Mst_Tbl DTMT WHERE DTMT.BIZ_TYPE IN (" + biz_type + ")  order by Document_Type";
                return objWF.GetDataSet(StrSql);
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

        public DataSet FetchDocGroup(string val)
        {
            try
            {
                string StrSql = null;
                WorkFlow objWF = new WorkFlow();
                StrSql = "Select g.Document_Group_Mst_pk,upper(g.Document_Group) Document_Group From Document_Group_Mst_Tbl g,Document_Type_Mst_Tbl t where g.document_group_mst_pk=t.DOCUMENT_GROUP_MST_FK and t.document_type_mst_fk=" + val + "";
                return objWF.GetDataSet(StrSql);
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

        public DataSet FetchDocName()
        {
            try
            {
                string biz_type = null;
                if (Convert.ToInt32(HttpContext.Current.Session["BIZ_TYPE"]) == 1)
                {
                    biz_type = "1,3";
                }
                else if (Convert.ToInt32(HttpContext.Current.Session["BIZ_TYPE"]) == 2)
                {
                    biz_type = "2,3";
                }
                else
                {
                    biz_type = "1,2,3";
                }
                StringBuilder StrBuilder = new StringBuilder();
                WorkFlow objWF = new WorkFlow();
                StrBuilder.Append(" select doc.document_name_mst_pk,doc.document_type_mst_fk,");
                StrBuilder.Append(" doc.document_name,doc.document_url from document_name_mst_tbl doc  where   doc.biz_type in (" + biz_type + ") ");
                StrBuilder.Append("  order by upper(doc.document_name) ");
                return objWF.GetDataSet(StrBuilder.ToString());
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
        public DataSet FillDocGroup()
        {
            try
            {
                string strGroup = null;
                WorkFlow objGR = new WorkFlow();
                strGroup = "Select g.Document_Group_Mst_pk,upper(g.Document_Group) Document_Group From Document_Group_Mst_Tbl g  order by Document_Group ";
                return objGR.GetDataSet(strGroup);
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

        #region "Save Function"
        public ArrayList SaveData(DataSet dsDoc)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                OracleTransaction TRAN = null;
                Int32 RecAfct = default(Int32);
                objWK.OpenConnection();
                TRAN = objWK.MyConnection.BeginTransaction();

                OracleCommand updCommand = new OracleCommand();
                OracleCommand delCommand = new OracleCommand();

                var _with1 = updCommand;
                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".DOCUMENT_MST_TBL_PKG.DOCUMENT_MST_TBL_UPD_ACTIVE";
                var _with2 = _with1.Parameters;

                updCommand.Parameters.Add("DOCUMENT_MST_PK_IN", OracleDbType.Int32, 10, "DOCUMENT_MST_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["DOCUMENT_MST_PK_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("ACTIVE_IN", OracleDbType.Int32, 1, "ACTIVE").Direction = ParameterDirection.Input;
                updCommand.Parameters["ACTIVE_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("CONFIG_MST_FK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with3 = delCommand;
                _with3.Connection = objWK.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".DOCUMENT_MST_TBL_PKG.DOCUMENT_MST_TBL_DEL";
                var _with4 = _with3.Parameters;
                delCommand.Parameters.Add("DOCUMENT_MST_PK_IN", OracleDbType.Int32, 10, "DOCUMENT_MST_PK").Direction = ParameterDirection.Input;
                delCommand.Parameters["DOCUMENT_MST_PK_IN"].SourceVersion = DataRowVersion.Current;
                delCommand.Parameters.Add("DELETED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                delCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                delCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
                delCommand.Parameters.Add("CONFIG_MST_FK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                delCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                delCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                
                var _with5 = objWK.MyDataAdapter;
                _with5.UpdateCommand = updCommand;
                _with5.UpdateCommand.Transaction = TRAN;
                _with5.DeleteCommand = delCommand;
                _with5.DeleteCommand.Transaction = TRAN;
                RecAfct = _with5.Update(dsDoc);
                TRAN.Commit();
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
            catch (Exception ex)
            {
                arrMessage.Add(ex.ToString());
            }
            finally
            {
                objWK.CloseConnection();
            }
            return new ArrayList();
        }

        public ArrayList Save(long intDocPk, string strDocId, int intDocNamePk, int intDocTypePk, int intDocGroupPk, string strDocSubject, string strDocHeader, string strDocBody, string strDocFooter, int intMessFolder,
        string strURL, short intActive, string Mode)
        {

            WorkFlow objWK = new WorkFlow();
            OracleCommand insCommand = new OracleCommand();
            OracleTransaction TRAN = null;
            int intPKVal = 0;
            long lngDocpk = 0;
            objWK.OpenConnection();
            try
            {
                DataTable DtTbl = new DataTable();
                DataRow DtRw = null;
                int i = 0;
                if (Mode == "EDIT")
                {
                    var _with6 = insCommand;
                    _with6.Connection = objWK.MyConnection;
                    _with6.CommandType = CommandType.StoredProcedure;
                    _with6.CommandText = objWK.MyUserName + ".DOCUMENT_MST_TBL_PKG.DOCUMENT_MST_TBL_UPD";
                    var _with7 = _with6.Parameters;
                    _with7.Add("DOCUMENT_MST_PK_IN", intDocPk).Direction = ParameterDirection.Input;
                    _with7.Add("DOCUMENT_ID_IN", strDocId).Direction = ParameterDirection.Input;
                    _with7.Add("DOCUMENT_NAME_IN", intDocNamePk).Direction = ParameterDirection.Input;
                    _with7.Add("DOCUMENT_TYPE_MST_FK_IN", intDocTypePk).Direction = ParameterDirection.Input;
                    _with7.Add("DOCUMENT_GROUP_MST_FK_IN", intDocGroupPk).Direction = ParameterDirection.Input;
                    _with7.Add("DOCUMENT_SUBJECT_IN", strDocSubject).Direction = ParameterDirection.Input;
                    _with7.Add("DOCUMENT_HEADER_IN", strDocHeader).Direction = ParameterDirection.Input;
                    _with7.Add("DOCUMENT_BODY_IN", strDocBody).Direction = ParameterDirection.Input;
                    _with7.Add("DOCUMENT_FOOTER_IN", strDocFooter).Direction = ParameterDirection.Input;
                    _with7.Add("MESSAGE_FOLDER_MST_FK_IN", intMessFolder).Direction = ParameterDirection.Input;
                    _with7.Add("ATTACHMENT_URL_IN", strURL).Direction = ParameterDirection.Input;
                    _with7["ATTACHMENT_URL_IN"].Size = 100;
                    _with7.Add("ACTIVE_IN", intActive).Direction = ParameterDirection.Input;
                    _with7.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                    _with7.Add("VERSION_NO_IN", M_VERSION_NO).Direction = ParameterDirection.Input;
                    _with7.Add("CONFIG_MST_FK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                    _with7.Add("RETURN_VALUE", OracleDbType.Int32).Direction = ParameterDirection.Output;
                    insCommand.ExecuteNonQuery();
                }
                else
                {
                    var _with8 = insCommand;
                    _with8.Connection = objWK.MyConnection;
                    _with8.CommandType = CommandType.StoredProcedure;
                    _with8.CommandText = objWK.MyUserName + ".DOCUMENT_MST_TBL_PKG.DOCUMENT_MST_TBL_INS";
                    var _with9 = _with8.Parameters;
                    _with9.Add("DOCUMENT_ID_IN", strDocId).Direction = ParameterDirection.Input;
                    _with9.Add("DOCUMENT_NAME_IN", intDocNamePk).Direction = ParameterDirection.Input;
                    _with9.Add("DOCUMENT_TYPE_MST_FK_IN", intDocTypePk).Direction = ParameterDirection.Input;
                    _with9.Add("DOCUMENT_GROUP_MST_FK_IN", intDocGroupPk).Direction = ParameterDirection.Input;
                    _with9.Add("DOCUMENT_SUBJECT_IN", strDocSubject).Direction = ParameterDirection.Input;
                    _with9.Add("DOCUMENT_HEADER_IN", strDocHeader).Direction = ParameterDirection.Input;
                    _with9.Add("DOCUMENT_BODY_IN", strDocBody).Direction = ParameterDirection.Input;
                    _with9.Add("DOCUMENT_FOOTER_IN", strDocFooter).Direction = ParameterDirection.Input;
                    _with9.Add("MESSAGE_FOLDER_MST_FK_IN", intMessFolder).Direction = ParameterDirection.Input;
                    _with9.Add("ATTACHMENT_URL_IN", strURL).Direction = ParameterDirection.Input;
                    _with9["ATTACHMENT_URL_IN"].Size = 100;
                    _with9.Add("ACTIVE_IN", intActive).Direction = ParameterDirection.Input;
                    _with9.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                    _with9.Add("CONFIG_MST_FK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                    _with9.Add("RETURN_VALUE", OracleDbType.Int32).Direction = ParameterDirection.Output;
                    insCommand.ExecuteNonQuery();
                }
                lngDocpk = Convert.ToInt64(insCommand.Parameters["RETURN_VALUE"].Value);
                if ((lngDocpk > 0))
                {
                    arrMessage.Add("All data saved successfully");
                    return arrMessage;
                }
            }
            catch (OracleException oraexp)
            {
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
            finally
            {
                objWK.CloseConnection();
            }
            return new ArrayList();
        }
        public DataTable FetchDDLGroup(Int32 DDLName)
        {
            StringBuilder str = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                str.Append(" select DG.DOCUMENT_GROUP_MST_PK,DT.DOCUMENT_TYPE_MST_FK from ");
                str.Append(" document_name_mst_tbl DN,");
                str.Append(" document_group_mst_tbl DG,");
                str.Append(" document_type_mst_tbl DT ");
                str.Append(" WHERE DN.DOCUMENT_TYPE_MST_FK = DT.DOCUMENT_TYPE_MST_FK ");
                str.Append(" AND DT.DOCUMENT_GROUP_MST_FK = DG.DOCUMENT_GROUP_MST_PK ");
                str.Append(" AND DN.DOCUMENT_NAME_MST_PK  = " + DDLName + " ");
                return objWF.GetDataTable(str.ToString());
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
