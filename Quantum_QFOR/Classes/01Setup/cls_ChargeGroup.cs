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
    public class cls_ChargeGroup : CommonFeatures
    {
        #region "Fetch Tree View"

        /// <summary>
        /// Fetches the TreeView.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchTreeView()
        {
            DataSet objds = new DataSet();
            string str = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                str = " SELECT CPMT.CHARGE_PARENT_PK CPK, CPMT.CHARGE_PARENT_ID, CPMT.CHARGE_PARENT_NAME FROM CHARGE_PARENT_MST_TBL CPMT WHERE CPMT.ACTIVE_FLAG=1  ";
                objds.Tables.Add(objWF.GetDataTable(str));
                objds.Tables[0].TableName = "PARENT";

                str = " SELECT CGMT.CHARGE_PARENT_FK CFK, CGMT.CHARGE_GRP_MST_PK CGPK, CGMT.CHARGE_GRP_ID, CGMT.CHARGE_GRP_NAME FROM CHARGE_GROUP_MST_TBL CGMT WHERE CGMT.ACTIVE_FLAG=1";
                objds.Tables.Add(objWF.GetDataTable(str));
                objds.Tables[1].TableName = "GROUP";

                str = " SELECT CEMT.CHARGE_GRP_MST_FK CGFK,";
                str += " CEMT.CHARGE_ELEMENT_MST_PK,";
                str += " CASE";
                str += " WHEN CEMT.FREIGHT_ELE_MST_FK IS NULL THEN";
                str += "      CEMT.COST_ELE_MST_FK";
                str += " ELSE";
                str += "       CEMT.FREIGHT_ELE_MST_FK";
                str += " END ELEMENT_PK,";
                str += " CASE";
                str += " WHEN CEMT.FREIGHT_ELE_MST_FK IS NULL THEN";
                str += "       CMT.COST_ELEMENT_NAME";
                str += " ELSE";
                str += "     FMT.FREIGHT_ELEMENT_NAME";
                str += " END ELEMENT_NAME, ";
                str += " (SELECT COUNT(*) FROM CHARGE_GRUOP_ELEMENT_MAP_TBL CGEMT ";
                str += " WHERE CGEMT.CHARGE_REV_ELE_MST_FK = CEMT.CHARGE_ELEMENT_MST_PK";
                str += "  OR CGEMT.CHARGE_COST_ELE_MST_FK = CEMT.CHARGE_ELEMENT_MST_PK) FLAG ";
                str += " FROM CHARGE_ELEMENT_MST_TBL  CEMT,";
                str += " CHARGE_GROUP_MST_TBL    CGMT,";
                str += " FREIGHT_ELEMENT_MST_TBL FMT,";
                str += " COST_ELEMENT_MST_TBL CMT ";
                str += " WHERE CGMT.CHARGE_GRP_MST_PK = CEMT.CHARGE_GRP_MST_FK";
                str += " AND FMT.FREIGHT_ELEMENT_MST_PK(+) = CEMT.FREIGHT_ELE_MST_FK ";
                str += " AND CMT.COST_ELEMENT_MST_PK(+) = CEMT.COST_ELE_MST_FK ";

                objds.Tables.Add(objWF.GetDataTable(str));
                objds.Tables[2].TableName = "ELEMENT";

                DataRelation objRel1 = new DataRelation("REL_PARENT_GROUP", objds.Tables[0].Columns["CPK"], objds.Tables[1].Columns["CFK"]);
                objRel1.Nested = true;
                objds.Relations.Add(objRel1);

                DataRelation objRel2 = new DataRelation("REL_GROUP_ELEMENT", objds.Tables[1].Columns["CGPK"], objds.Tables[2].Columns["CGFK"]);
                objRel1.Nested = true;
                objds.Relations.Add(objRel2);

                return objds;
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

        #endregion "Fetch Tree View"

        #region "Fetch Queries"

        /// <summary>
        /// Fetches the parent group.
        /// </summary>
        /// <param name="ChargePK">The charge pk.</param>
        /// <returns></returns>
        public DataSet FetchParentGroup(Int64 ChargePK)
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;

            strSQL = "SELECT ROWNUM SL_NO, CPMT.ACTIVE_FLAG, CPMT.CHARGE_PARENT_PK, CPMT.CHARGE_PARENT_ID, CPMT.CHARGE_PARENT_NAME FROM CHARGE_PARENT_MST_TBL CPMT";
            strSQL += " WHERE CPMT.CHARGE_PARENT_PK=" + ChargePK;

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

        /// <summary>
        /// Fetches the charge group.
        /// </summary>
        /// <param name="ChargePK">The charge pk.</param>
        /// <returns></returns>
        public DataSet FetchChargeGroup(Int64 ChargePK)
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;

            strSQL = " SELECT ROWNUM SL_NO, CGMT.ACTIVE_FLAG, CGMT.CHARGE_PARENT_FK ,  ";
            strSQL += " CGMT.CHARGE_GRP_MST_PK, CGMT.CHARGE_GRP_ID, CGMT.CHARGE_TYPE, CGMT.CHARGE_GRP_NAME, CGMT.VERSION, '' BTN FROM CHARGE_GROUP_MST_TBL CGMT ";
            strSQL += " WHERE CGMT.CHARGE_PARENT_FK=" + ChargePK;
            strSQL += " ORDER BY CGMT.CHARGE_GRP_ID";
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

        /// <summary>
        /// Fetches the name of the element identifier.
        /// </summary>
        /// <param name="GroupPK">The group pk.</param>
        /// <param name="ChargePK">The charge pk.</param>
        /// <returns></returns>
        public DataSet FetchElementIdName(int GroupPK, Int64 ChargePK)
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            if (ChargePK == 0)
            {
                strSQL = " SELECT ROWNUM SL_NO, Q.* FROM ( SELECT";
                strSQL += " DECODE(NVL(CEMT.ACTIVE_FLAG,0),0,'FALSE',1,'TRUE') ACTIVE_FLAG,";
                strSQL += " CEMT.CHARGE_ELEMENT_MST_PK,";
                strSQL += " CEMT.CHARGE_ELEMENT_ID,";
                strSQL += " CEMT.CHARGE_GRP_MST_FK,";
                strSQL += " CEMT.CHARGE_PARENT_FK,";
                strSQL += " FEMT.FREIGHT_ELEMENT_ID FETCH_ELEMENT,";
                strSQL += " CEMT.FREIGHT_ELE_MST_FK FRTCOST_ELE_MST_FK,";
                strSQL += " FEMT.FREIGHT_ELEMENT_NAME FRTCOST_ELE_NAME,";
                strSQL += " CEMT.VERSION, ";
                strSQL += " '' SEL, ";
                strSQL += " (SELECT COUNT(*) FROM CHARGE_GRUOP_ELEMENT_MAP_TBL CGEMT ";
                strSQL += " WHERE CGEMT.CHARGE_REV_ELE_MST_FK = CEMT.CHARGE_ELEMENT_MST_PK) FLAG ";
                strSQL += " FROM CHARGE_ELEMENT_MST_TBL  CEMT,";
                strSQL += " FREIGHT_ELEMENT_MST_TBL FEMT ";
                strSQL += " WHERE FEMT.FREIGHT_ELEMENT_MST_PK = CEMT.FREIGHT_ELE_MST_FK ";
                strSQL += " AND CEMT.CHARGE_PARENT_FK=" + ChargePK;
                strSQL += " AND CEMT.CHARGE_GRP_MST_FK=" + GroupPK;
                strSQL += " ORDER BY CEMT.CHARGE_ELEMENT_ID ) Q ORDER BY SL_NO";
            }
            else
            {
                strSQL = " SELECT ROWNUM SL_NO, Q.* FROM ( SELECT";
                strSQL += " DECODE(NVL(CEMT.ACTIVE_FLAG,0),0,'FALSE',1,'TRUE') ACTIVE_FLAG,";
                strSQL += " CEMT.CHARGE_ELEMENT_MST_PK,";
                strSQL += " CEMT.CHARGE_ELEMENT_ID,";
                strSQL += " CEMT.CHARGE_GRP_MST_FK,";
                strSQL += " CEMT.CHARGE_PARENT_FK,";
                strSQL += " CMT.COST_ELEMENT_ID FETCH_ELEMENT,";
                strSQL += " CEMT.COST_ELE_MST_FK FRTCOST_ELE_MST_FK,";
                strSQL += " CMT.COST_ELEMENT_NAME FRTCOST_ELE_NAME,";
                strSQL += " CEMT.VERSION, ";
                strSQL += " '' SEL, ";
                strSQL += " (SELECT COUNT(*) FROM CHARGE_GRUOP_ELEMENT_MAP_TBL CGEMT ";
                strSQL += " WHERE CGEMT.CHARGE_COST_ELE_MST_FK = CEMT.CHARGE_ELEMENT_MST_PK) FLAG ";
                strSQL += " FROM CHARGE_ELEMENT_MST_TBL  CEMT,";
                strSQL += " COST_ELEMENT_MST_TBL CMT ";
                strSQL += " WHERE CMT.COST_ELEMENT_MST_PK = CEMT.COST_ELE_MST_FK ";
                strSQL += " AND CEMT.CHARGE_PARENT_FK=" + ChargePK;
                strSQL += " AND CEMT.CHARGE_GRP_MST_FK=" + GroupPK;
                strSQL += " ORDER BY CEMT.CHARGE_ELEMENT_ID ) Q ORDER BY SL_NO";
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

        /// <summary>
        /// Fetches the charge mapping.
        /// </summary>
        /// <param name="PageLoad">The page load.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="GroupPK">The group pk.</param>
        /// <param name="ElementPK">The element pk.</param>
        /// <returns></returns>
        public DataSet FetchChargeMapping(Int32 PageLoad, Int32 CurrentPage, Int32 TotalPage, int GroupPK, int ElementPK)
        {
            try
            {
                WorkFlow objWF = new WorkFlow();
                Int32 TotalRecords = default(Int32);
                Int32 last = default(Int32);
                Int32 start = default(Int32);
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

                sb.Append("SELECT CHARGE_GRP_ELE_MAP_MST_PK, CHARGE_REV_PARENT_FK,");
                sb.Append("                       CHARGE_REV_ELE_MST_FK,");
                sb.Append("                       CHARGE_REV_ELE_ID,");
                sb.Append("                       FREIGHT_ELE_MST_FK,");
                sb.Append("                       FREIGHT_ELE_ID,");
                sb.Append("                       FREIGHT_ELE_NAME,");
                sb.Append("                       EXTRA_BLANK,");
                sb.Append("                       ROWNUM SL_NO_2,");
                sb.Append("                       CHARGE_COST_PARENT_FK,");
                sb.Append("                       CHARGE_COST_ELE_MST_FK,");
                sb.Append("                       CHARGE_COST_ELE_ID,");
                sb.Append("                       COST_ELE_MST_FK,");
                sb.Append("                       COST_ELE_ID,");
                sb.Append("                       COST_ELE_NAME");
                sb.Append(" FROM(SELECT CGEMT.CHARGE_GRP_ELE_MAP_MST_PK,");
                sb.Append("       CGEMT.CHARGE_REV_PARENT_FK,");
                sb.Append("       CGEMT.CHARGE_REV_ELE_MST_FK,");
                sb.Append("       CGEMT.CHARGE_REV_ELE_ID,");
                sb.Append("       CGEMT.FREIGHT_ELE_MST_FK,");
                sb.Append("       FEMT.FREIGHT_ELEMENT_ID FREIGHT_ELE_ID,");
                sb.Append("       FEMT.FREIGHT_ELEMENT_NAME FREIGHT_ELE_NAME,");
                sb.Append("       '' EXTRA_BLANK,");
                sb.Append("       0 SL_NO_2,");
                sb.Append("       CGEMT.CHARGE_COST_PARENT_FK,");
                sb.Append("       CGEMT.CHARGE_COST_ELE_MST_FK,");
                sb.Append("       CGEMT.CHARGE_COST_ELE_ID,");
                sb.Append("       CGEMT.COST_ELE_MST_FK, ");
                sb.Append("       CEMT.COST_ELEMENT_ID COST_ELE_ID,");
                sb.Append("       CEMT.COST_ELEMENT_NAME COST_ELE_NAME");
                sb.Append("  FROM CHARGE_GRUOP_ELEMENT_MAP_TBL CGEMT,");
                sb.Append("       FREIGHT_ELEMENT_MST_TBL      FEMT,");
                sb.Append("       CHARGE_GROUP_MST_TBL         CGMT,");
                sb.Append("       CHARGE_ELEMENT_MST_TBL       CMT,");
                sb.Append("       COST_ELEMENT_MST_TBL         CEMT");
                sb.Append(" WHERE FEMT.FREIGHT_ELEMENT_MST_PK = CGEMT.FREIGHT_ELE_MST_FK");
                sb.Append("   AND CEMT.COST_ELEMENT_MST_PK = CGEMT.COST_ELE_MST_FK");
                sb.Append("   AND CGMT.CHARGE_GRP_MST_PK = CMT.CHARGE_GRP_MST_FK ");
                sb.Append("   AND CGEMT.CHARGE_REV_ELE_MST_FK = CMT.CHARGE_ELEMENT_MST_PK ");

                if (GroupPK > 0)
                {
                    sb.Append("   AND CMT.CHARGE_GRP_MST_FK =" + GroupPK);
                }
                if (ElementPK > 0)
                {
                    sb.Append("   AND CGEMT.CHARGE_REV_ELE_MST_FK =" + ElementPK);
                }

                if (PageLoad == 0)
                {
                    sb.Append("       AND  1=2 ");
                }
                sb.Append(" ORDER BY CHARGE_REV_ELE_ID, SL_NO_2) ORDER BY CHARGE_REV_ELE_ID ");
                if (PageLoad == 0)
                {
                    TotalRecords = Convert.ToInt32(objWF.ExecuteScaler("SELECT count(*) FROM ( " + sb.ToString() + ") WHERE 1=2"));
                    // Getting No of satisfying records.
                }
                else
                {
                    TotalRecords = Convert.ToInt32(objWF.ExecuteScaler("SELECT count(*) FROM ( " + sb.ToString() + ")"));
                    // Getting No of satisfying records.
                }

                TotalPage = TotalRecords / RecordsPerPage;
                if (TotalRecords % RecordsPerPage != 0)
                    TotalPage += 1;
                if (CurrentPage > TotalPage)
                    CurrentPage = 1;
                if (TotalRecords == 0)
                    CurrentPage = 0;
                last = CurrentPage * RecordsPerPage;
                start = (CurrentPage - 1) * RecordsPerPage + 1;

                return objWF.GetDataSet(" SELECT MAINQ.* FROM( SELECT ROWNUM SL_NO_1 ,Q.* FROM  ( " + sb.ToString() + " ) Q ) MAINQ WHERE SL_NO_1 BETWEEN " + start + " AND " + last + " ");

                return objWF.GetDataSet(sb.ToString());
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

        #endregion "Fetch Queries"

        #region "Save Function"

        /// <summary>
        /// Saves the specified m_ data set.
        /// </summary>
        /// <param name="M_DataSet">The m_ data set.</param>
        /// <returns></returns>
        public ArrayList Save(DataSet M_DataSet)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = default(OracleTransaction);
            TRAN = objWK.MyConnection.BeginTransaction();

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
                var _with1 = insCommand;
                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".CHARGE_GROUP_MST_TBL_PKG.CHARGE_GROUP_MST_TBL_INS";
                var _with2 = _with1.Parameters;
                insCommand.Parameters.Add("CHARGE_PARENT_FK_IN", OracleDbType.Int32, 10, "CHARGE_PARENT_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["CHARGE_PARENT_FK_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("CHARGE_GRP_ID_IN", OracleDbType.Int32, 10, "CHARGE_GRP_ID").Direction = ParameterDirection.Input;
                insCommand.Parameters["CHARGE_GRP_ID_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("CHARGE_GRP_NAME_IN", OracleDbType.Varchar2, 50, "CHARGE_GRP_NAME").Direction = ParameterDirection.Input;
                insCommand.Parameters["CHARGE_GRP_NAME_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("CHARGE_TYPE_IN", OracleDbType.Int32, 1, "CHARGE_TYPE").Direction = ParameterDirection.Input;
                insCommand.Parameters["CHARGE_TYPE_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
                insCommand.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                var _with3 = updCommand;
                _with3.Connection = objWK.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".CHARGE_GROUP_MST_TBL_PKG.CHARGE_GROUP_MST_TBL_UPD";
                updCommand.Parameters.Add("CHARGE_GRP_MST_PK_IN", OracleDbType.Int32, 10, "CHARGE_GRP_MST_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["CHARGE_GRP_MST_PK_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("CHARGE_PARENT_FK_IN", OracleDbType.Int32, 10, "CHARGE_PARENT_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["CHARGE_PARENT_FK_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("CHARGE_GRP_ID_IN", OracleDbType.Int32, 10, "CHARGE_GRP_ID").Direction = ParameterDirection.Input;
                updCommand.Parameters["CHARGE_GRP_ID_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("CHARGE_GRP_NAME_IN", OracleDbType.Varchar2, 50, "CHARGE_GRP_NAME").Direction = ParameterDirection.Input;
                updCommand.Parameters["CHARGE_GRP_NAME_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("CHARGE_TYPE_IN", OracleDbType.Int32, 1, "CHARGE_TYPE").Direction = ParameterDirection.Input;
                updCommand.Parameters["CHARGE_TYPE_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
                updCommand.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION").Direction = ParameterDirection.Input;
                updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                var _with4 = objWK.MyDataAdapter;
                _with4.InsertCommand = insCommand;
                _with4.InsertCommand.Transaction = TRAN;
                _with4.UpdateCommand = updCommand;
                _with4.UpdateCommand.Transaction = TRAN;
                RecAfct = _with4.Update(M_DataSet);
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
                objWK.MyConnection.Close();
            }
        }

        /// <summary>
        /// Saves the element.
        /// </summary>
        /// <param name="T_DataSet">The t_ data set.</param>
        /// <param name="RevCost">The rev cost.</param>
        /// <returns></returns>
        public ArrayList SaveElement(DataSet T_DataSet, int RevCost)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = default(OracleTransaction);
            TRAN = objWK.MyConnection.BeginTransaction();

            Int32 RecAfct = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            OracleCommand delCommand = new OracleCommand();
            arrMessage.Clear();
            try
            {
                DataTable DtTbl = new DataTable();
                int i = 0;
                DtTbl = T_DataSet.Tables[0];
                var _with5 = insCommand;
                _with5.Connection = objWK.MyConnection;
                _with5.CommandType = CommandType.StoredProcedure;
                _with5.CommandText = objWK.MyUserName + ".CHARGE_ELEMENT_MST_TBL_PKG.CHARGE_ELEMENT_MST_TBL_INS";
                var _with6 = _with5.Parameters;
                insCommand.Parameters.Add("CHARGE_ELEMENT_ID_IN", OracleDbType.Int32, 10, "CHARGE_ELEMENT_ID").Direction = ParameterDirection.Input;
                insCommand.Parameters["CHARGE_ELEMENT_ID_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("CHARGE_GRP_MST_FK_IN", OracleDbType.Int32, 10, "CHARGE_GRP_MST_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["CHARGE_GRP_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("CHARGE_PARENT_FK_IN", OracleDbType.Int32, 10, "CHARGE_PARENT_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["CHARGE_PARENT_FK_IN"].SourceVersion = DataRowVersion.Current;
                if (RevCost == 0)
                {
                    insCommand.Parameters.Add("FREIGHT_ELE_MST_FK_IN", OracleDbType.Int32, 10, "FRTCOST_ELE_MST_FK").Direction = ParameterDirection.Input;
                    insCommand.Parameters["FREIGHT_ELE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                    insCommand.Parameters.Add("COST_ELE_MST_FK_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    insCommand.Parameters.Add("FREIGHT_ELE_MST_FK_IN", "").Direction = ParameterDirection.Input;
                    insCommand.Parameters.Add("COST_ELE_MST_FK_IN", OracleDbType.Int32, 10, "FRTCOST_ELE_MST_FK").Direction = ParameterDirection.Input;
                    insCommand.Parameters["COST_ELE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                }
                insCommand.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
                insCommand.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 500, "RETURN_VALUE").Direction = ParameterDirection.Output;
                var _with7 = updCommand;
                _with7.Connection = objWK.MyConnection;
                _with7.CommandType = CommandType.StoredProcedure;
                _with7.CommandText = objWK.MyUserName + ".CHARGE_ELEMENT_MST_TBL_PKG.CHARGE_ELEMENT_MST_TBL_UPD";
                updCommand.Parameters.Add("CHARGE_ELEMENT_MST_PK_IN", OracleDbType.Int32, 10, "CHARGE_ELEMENT_MST_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["CHARGE_ELEMENT_MST_PK_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("CHARGE_ELEMENT_ID_IN", OracleDbType.Int32, 10, "CHARGE_ELEMENT_ID").Direction = ParameterDirection.Input;
                updCommand.Parameters["CHARGE_ELEMENT_ID_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("CHARGE_GRP_MST_FK_IN", OracleDbType.Int32, 10, "CHARGE_GRP_MST_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["CHARGE_GRP_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("CHARGE_PARENT_FK_IN", OracleDbType.Int32, 10, "CHARGE_PARENT_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["CHARGE_PARENT_FK_IN"].SourceVersion = DataRowVersion.Current;
                if (RevCost == 0)
                {
                    updCommand.Parameters.Add("FREIGHT_ELE_MST_FK_IN", OracleDbType.Int32, 10, "FRTCOST_ELE_MST_FK").Direction = ParameterDirection.Input;
                    updCommand.Parameters["FREIGHT_ELE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                    updCommand.Parameters.Add("COST_ELE_MST_FK_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    updCommand.Parameters.Add("FREIGHT_ELE_MST_FK_IN", "").Direction = ParameterDirection.Input;
                    updCommand.Parameters.Add("COST_ELE_MST_FK_IN", OracleDbType.Int32, 10, "FRTCOST_ELE_MST_FK").Direction = ParameterDirection.Input;
                    updCommand.Parameters["COST_ELE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                }
                updCommand.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
                updCommand.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("VERSION_IN", OracleDbType.Int32, 4, "VERSION").Direction = ParameterDirection.Input;
                updCommand.Parameters["VERSION_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 500, "RETURN_VALUE").Direction = ParameterDirection.Output;
                var _with8 = objWK.MyDataAdapter;
                _with8.InsertCommand = insCommand;
                _with8.InsertCommand.Transaction = TRAN;
                _with8.UpdateCommand = updCommand;
                _with8.UpdateCommand.Transaction = TRAN;
                RecAfct = _with8.Update(T_DataSet);
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
                objWK.MyConnection.Close();
            }
        }

        /// <summary>
        ///
        /// </summary>
        protected enum ELEMENTHEADER
        {
            /// <summary>
            /// The s l_ no
            /// </summary>
            SL_NO = 0,

            /// <summary>
            /// The activ e_ flag
            /// </summary>
            ACTIVE_FLAG = 1,

            /// <summary>
            /// The charg e_ elemen t_ ms t_ pk
            /// </summary>
            CHARGE_ELEMENT_MST_PK = 2,

            /// <summary>
            /// The charg e_ elemen t_ identifier
            /// </summary>
            CHARGE_ELEMENT_ID = 3,

            /// <summary>
            /// The charg e_ gr p_ ms t_ fk
            /// </summary>
            CHARGE_GRP_MST_FK = 4,

            /// <summary>
            /// The charg e_ paren t_ fk
            /// </summary>
            CHARGE_PARENT_FK = 5,

            /// <summary>
            /// The fetc h_ element
            /// </summary>
            FETCH_ELEMENT = 6,

            /// <summary>
            /// The frtcos t_ el e_ ms t_ fk
            /// </summary>
            FRTCOST_ELE_MST_FK = 7,

            /// <summary>
            /// The frtcos t_ el e_ name
            /// </summary>
            FRTCOST_ELE_NAME = 8,

            /// <summary>
            /// The versio n_ no
            /// </summary>
            VERSION_NO = 9,

            /// <summary>
            /// The sel
            /// </summary>
            SEL = 10
        }

        #endregion "Save Function"

        #region " Fetch Function "

        /// <summary>
        /// Charges the mapping list.
        /// </summary>
        /// <param name="RevId">The rev identifier.</param>
        /// <param name="RevDesc">The rev desc.</param>
        /// <param name="CostId">The cost identifier.</param>
        /// <param name="CostDesc">The cost desc.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <returns></returns>
        public DataSet chargeMappingList(string RevId = "", string RevDesc = "", string CostId = "", string CostDesc = "", Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

            try
            {
                sb.Append("SELECT 1 SORTCLMN,");
                sb.Append("       'Revenue Element (11)' CHARGE_TYPE,");
                sb.Append("       'Revenue Element (11)' REV_PK,");
                sb.Append("       'Revenue Element (11)' REV_ELEM,");
                sb.Append("       'Revenue Element (11)' REV_ELEM,");
                sb.Append("       'Revenue Element (11)' REV_ELEM,");
                sb.Append("       'Revenue Element (11)' REV_ELEM,");
                sb.Append("       'Cost Element (12)' COST_PK,");
                sb.Append("       'Cost Element (12)' COST_ELEM,");
                sb.Append("       'Cost Element (12)' COST_ELEM,");
                sb.Append("       'Cost Element (12)' COST_ELEM,");
                sb.Append("       'Cost Element (12)' COST_ELEM,");
                sb.Append("       'Cost Element (12)' COST_ELEM,");
                sb.Append("       'Cost Element (12)' COST_ELEM");
                sb.Append("  FROM DUAL ");
                sb.Append(" UNION ");
                sb.Append(" SELECT 2 SORTCOLMN,");
                sb.Append("       CASE REV_ELEM");
                sb.Append("         WHEN 'Freight' THEN");
                sb.Append("          '1'");
                sb.Append("         WHEN 'Surcharge' THEN");
                sb.Append("          '2'");
                sb.Append("         WHEN 'Local Charges' THEN");
                sb.Append("          '3'");
                sb.Append("       END CHARGE_TYPE,");
                sb.Append("       TO_CHAR(REV_PK) REV_PK,");
                sb.Append("       CASE REV_CHARGE_TYPE");
                sb.Append("         WHEN 1 THEN");
                sb.Append("          'Freight' || ' (1101)'");
                sb.Append("         WHEN 2 THEN");
                sb.Append("          'Surcharge' || ' (1102)'");
                sb.Append("         WHEN 3 THEN");
                sb.Append("          'Local Charges' || ' (1103)'");
                sb.Append("       END REV_ELEM,");
                sb.Append("       CASE REV_CHARGE_TYPE");
                sb.Append("         WHEN 1 THEN");
                sb.Append("          'Freight' || ' (1101)'");
                sb.Append("         WHEN 2 THEN");
                sb.Append("          'Surcharge' || ' (1102)'");
                sb.Append("         WHEN 3 THEN");
                sb.Append("          'Local Charges' || ' (1103)'");
                sb.Append("       END REV_ELEM,");
                sb.Append("       CASE REV_CHARGE_TYPE");
                sb.Append("         WHEN 1 THEN");
                sb.Append("          'Freight' || ' (1101)'");
                sb.Append("         WHEN 2 THEN");
                sb.Append("          'Surcharge' || ' (1102)'");
                sb.Append("         WHEN 3 THEN");
                sb.Append("          'Local Charges' || ' (1103)'");
                sb.Append("       END REV_ELEM,");
                sb.Append("       CASE REV_CHARGE_TYPE");
                sb.Append("         WHEN 1 THEN");
                sb.Append("          'Freight' || ' (1101)'");
                sb.Append("         WHEN 2 THEN");
                sb.Append("          'Surcharge' || ' (1102)'");
                sb.Append("         WHEN 3 THEN");
                sb.Append("          'Local Charges' || ' (1103)'");
                sb.Append("       END REV_ELEM,");
                sb.Append("       TO_CHAR(COST_PK) COST_PK,");
                sb.Append("       CASE COST_CHARGE_TYPE");
                sb.Append("         WHEN 1 THEN");
                sb.Append("          'Freight' || ' (1201)'");
                sb.Append("         WHEN 2 THEN");
                sb.Append("          'Surcharge' || ' (1202)'");
                sb.Append("         WHEN 3 THEN");
                sb.Append("          'Local Charges' || ' (1203)'");
                sb.Append("       END COST_ELEM,");
                sb.Append("       CASE COST_CHARGE_TYPE");
                sb.Append("         WHEN 1 THEN");
                sb.Append("          'Freight' || ' (1201)'");
                sb.Append("         WHEN 2 THEN");
                sb.Append("          'Surcharge' || ' (1202)'");
                sb.Append("         WHEN 3 THEN");
                sb.Append("          'Local Charges' || ' (1203)'");
                sb.Append("       END COST_ELEM,");
                sb.Append("       CASE COST_CHARGE_TYPE");
                sb.Append("         WHEN 1 THEN");
                sb.Append("          'Freight' || ' (1201)'");
                sb.Append("         WHEN 2 THEN");
                sb.Append("          'Surcharge' || ' (1202)'");
                sb.Append("         WHEN 3 THEN");
                sb.Append("          'Local Charges' || ' (1203)'");
                sb.Append("       END COST_ELEM,");
                sb.Append("       CASE COST_CHARGE_TYPE");
                sb.Append("         WHEN 1 THEN");
                sb.Append("          'Freight' || ' (1201)'");
                sb.Append("         WHEN 2 THEN");
                sb.Append("          'Surcharge' || ' (1202)'");
                sb.Append("         WHEN 3 THEN");
                sb.Append("          'Local Charges' || ' (1203)'");
                sb.Append("       END COST_ELEM,");
                sb.Append("       CASE COST_CHARGE_TYPE");
                sb.Append("         WHEN 1 THEN");
                sb.Append("          'Freight' || ' (1201)'");
                sb.Append("         WHEN 2 THEN");
                sb.Append("          'Surcharge' || ' (1202)'");
                sb.Append("         WHEN 3 THEN");
                sb.Append("          'Local Charges' || ' (1203)'");
                sb.Append("       END COST_ELEM,");
                sb.Append("       CASE COST_CHARGE_TYPE");
                sb.Append("         WHEN 1 THEN");
                sb.Append("          'Freight' || ' (1201)'");
                sb.Append("         WHEN 2 THEN");
                sb.Append("          'Surcharge' || ' (1202)'");
                sb.Append("         WHEN 3 THEN");
                sb.Append("          'Local Charges' || ' (1203)'");
                sb.Append("       END COST_ELEM");
                sb.Append("  FROM (SELECT REV_PK, REV_ELEM, CHARGE_TYPE REV_CHARGE_TYPE ");
                sb.Append("          FROM (SELECT C.CHARGE_GRP_MST_PK REV_PK,  C.CHARGE_TYPE, ");
                sb.Append("                       (SELECT C1.CHARGE_GRP_NAME");
                sb.Append("                          FROM CHARGE_GROUP_MST_TBL C1");
                sb.Append("                         WHERE C1.CHARGE_PARENT_FK = 0");
                sb.Append("                           AND C1.CHARGE_GRP_MST_PK = C.CHARGE_GRP_MST_PK) REV_ELEM");
                sb.Append("                  FROM CHARGE_GROUP_MST_TBL C) T1");
                sb.Append("         WHERE T1.REV_ELEM IS NOT NULL) X,");
                sb.Append("       (SELECT COST_PK, COST_ELEM, CHARGE_TYPE COST_CHARGE_TYPE");
                sb.Append("          FROM (SELECT C.CHARGE_GRP_MST_PK COST_PK, C.CHARGE_TYPE, ");
                sb.Append("                       (SELECT C1.CHARGE_GRP_NAME");
                sb.Append("                          FROM CHARGE_GROUP_MST_TBL C1");
                sb.Append("                         WHERE C1.CHARGE_PARENT_FK = 1");
                sb.Append("                           AND C1.CHARGE_GRP_MST_PK = C.CHARGE_GRP_MST_PK) COST_ELEM");
                sb.Append("                  FROM CHARGE_GROUP_MST_TBL C) T2");
                sb.Append("         WHERE T2.COST_ELEM IS NOT NULL) Y");
                sb.Append(" WHERE X.REV_ELEM = Y.COST_ELEM");
                sb.Append("");

                DS.Tables.Add(objWF.GetDataTable(sb.ToString()));

                sb.Remove(0, sb.Length);

                sb.Append("SELECT REV_ID,");
                sb.Append("       REV_CODE,");
                sb.Append("       REV_DESC,");
                sb.Append("       REV_MAP,");
                sb.Append("       COST_ID,");
                sb.Append("       COST_CODE,");
                sb.Append("       COST_DESC,");
                sb.Append("       COST_MAP,");
                sb.Append("       CHARGE_ELEMENT_MST_PK,");
                sb.Append("       CHARGE_GRP_MST_FK,");
                sb.Append("       TO_CHAR(CHARGE_TYPE) CHARGE_TYPE/*,");
                sb.Append("       PREFERENCE*/");
                sb.Append("  FROM (SELECT TO_CHAR(CGE.CHARGE_REV_ELE_ID) REV_ID,");
                sb.Append("               FE.FREIGHT_ELEMENT_ID REV_CODE,");
                sb.Append("               FE.FREIGHT_ELEMENT_NAME REV_DESC,");
                sb.Append("               ' ' REV_MAP,");
                sb.Append("               TO_CHAR(CHE.CHARGE_ELEMENT_ID) COST_ID,");
                sb.Append("               CASE");
                sb.Append("                 WHEN CHE.CHARGE_ELEMENT_MST_PK IS NOT NULL THEN");
                sb.Append("                  CE.COST_ELEMENT_ID");
                sb.Append("               END COST_CODE,");
                sb.Append("               CASE");
                sb.Append("                 WHEN CHE.CHARGE_ELEMENT_MST_PK IS NOT NULL THEN");
                sb.Append("                  CE.COST_ELEMENT_NAME");
                sb.Append("               END COST_DESC,");
                sb.Append("               CASE");
                sb.Append("                 WHEN CHE.CHARGE_ELEMENT_MST_PK IS NOT NULL THEN");
                sb.Append("                  '1'");
                sb.Append("                  ELSE '0' END COST_MAP,");
                sb.Append("               TO_CHAR(CHE.CHARGE_ELEMENT_MST_PK) CHARGE_ELEMENT_MST_PK,");
                sb.Append("               CHE.CHARGE_GRP_MST_FK,");
                sb.Append("               FE.CHARGE_TYPE,");
                sb.Append("               FE.PREFERENCE");
                sb.Append("          FROM FREIGHT_ELEMENT_MST_TBL      FE,");
                sb.Append("               COST_ELEMENT_MST_TBL         CE,");
                sb.Append("               CHARGE_ELEMENT_MST_TBL       CHE,");
                sb.Append("               CHARGE_GRUOP_ELEMENT_MAP_TBL CGE");
                sb.Append("         WHERE FE.FREIGHT_ELEMENT_MST_PK = CE.COST_ELEMENT_MST_PK");
                sb.Append("           AND CE.COST_ELEMENT_MST_PK = CHE.COST_ELE_MST_FK");
                sb.Append("           AND CGE.COST_ELE_MST_FK = CHE.COST_ELE_MST_FK");
                sb.Append("           AND FE.ACTIVE_FLAG = 1");
                sb.Append("           AND FE.CHARGE_TYPE IN (1, 2, 3)");
                sb.Append("        UNION");
                sb.Append("        SELECT '' REV_ID,");
                sb.Append("               FE.FREIGHT_ELEMENT_ID REV_CODE,");
                sb.Append("               FE.FREIGHT_ELEMENT_NAME REV_DESC,");
                sb.Append("               ' ' REV_MAP,");
                sb.Append("               '' COST_ID,");
                sb.Append("               CASE");
                sb.Append("                 WHEN CHE.CHARGE_ELEMENT_MST_PK IS NOT NULL THEN");
                sb.Append("                  CE.COST_ELEMENT_ID");
                sb.Append("               END COST_CODE,");
                sb.Append("               CASE");
                sb.Append("                 WHEN CHE.CHARGE_ELEMENT_MST_PK IS NOT NULL THEN");
                sb.Append("                  CE.COST_ELEMENT_NAME");
                sb.Append("               END COST_DESC,");
                sb.Append("               CASE");
                sb.Append("                 WHEN CHE.CHARGE_ELEMENT_MST_PK IS NOT NULL THEN");
                sb.Append("                  '1'");
                sb.Append("                  ELSE '0' END COST_MAP,");
                sb.Append("               TO_CHAR(CHE.CHARGE_ELEMENT_MST_PK) CHARGE_ELEMENT_MST_PK,");
                sb.Append("               CHE.CHARGE_GRP_MST_FK,");
                sb.Append("               FE.CHARGE_TYPE,");
                sb.Append("               FE.PREFERENCE");
                sb.Append("          FROM FREIGHT_ELEMENT_MST_TBL FE,");
                sb.Append("               COST_ELEMENT_MST_TBL    CE,");
                sb.Append("               CHARGE_ELEMENT_MST_TBL  CHE");
                sb.Append("         WHERE FE.FREIGHT_ELEMENT_MST_PK = CE.COST_ELEMENT_MST_PK");
                sb.Append("           AND CE.COST_ELEMENT_MST_PK = CHE.COST_ELE_MST_FK(+)");
                sb.Append("           AND CHE.COST_ELE_MST_FK IS NULL");
                sb.Append("           AND FE.ACTIVE_FLAG = 1");
                sb.Append("           AND FE.CHARGE_TYPE IN (1, 2, 3))");
                sb.Append("");
                sb.Append(" WHERE 1 = 1 ");
                if (!string.IsNullOrEmpty(RevId))
                {
                    sb.Append(" AND REV_ID LIKE '%" + RevId + "%' ");
                }
                if (!string.IsNullOrEmpty(RevDesc))
                {
                    sb.Append(" AND REV_DESC LIKE '%" + RevDesc + "%' ");
                }
                if (!string.IsNullOrEmpty(CostId))
                {
                    sb.Append(" AND COST_ID LIKE '%" + CostId + "%' ");
                }
                if (!string.IsNullOrEmpty(CostDesc))
                {
                    sb.Append(" AND COST_DESC LIKE '%" + CostDesc + "%' ");
                }
                sb.Append(" ORDER BY REV_ID, REV_DESC ASC ");

                DS.Tables.Add(objWF.GetDataTable(sb.ToString()));

                //making relations
                DataRelation RelMap = null;
                RelMap = new DataRelation("Map_Relation", DS.Tables[0].Columns["CHARGE_TYPE"], DS.Tables[1].Columns["CHARGE_TYPE"]);
                RelMap.Nested = true;
                DS.Relations.Add(RelMap);

                return DS;
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

        #endregion " Fetch Function "

        #region " Charge Master "

        /// <summary>
        /// Saves the charge element.
        /// </summary>
        /// <param name="FrtElemId">The FRT elem identifier.</param>
        /// <param name="ChargeBasis">The charge basis.</param>
        /// <param name="CElemMstPk">The c elem MST pk.</param>
        /// <returns></returns>
        public string SaveChargeElement(string FrtElemId, string ChargeBasis, Int32 CElemMstPk)
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = default(OracleTransaction);
            OracleCommand insCommand = new OracleCommand();
            DataSet ds = new DataSet();
            string strSql = "";
            string FrtElemMstFk = null;
            string CostElemMstFk = null;
            Int32 i = 0;
            int RevCost = 0;
            string ChargeElemId = null;
            string ChargeGrpMstFk = "";
            string ChargeElemMstPk = "";

            arrMessage.Clear();
            try
            {
                strSql = " SELECT F.FREIGHT_ELEMENT_MST_PK, C.COST_ELEMENT_MST_PK, F.CHARGE_TYPE FROM FREIGHT_ELEMENT_MST_TBL F, COST_ELEMENT_MST_TBL C ";
                strSql += " WHERE F.FREIGHT_ELEMENT_MST_PK = C.COST_ELEMENT_MST_PK ";
                strSql += " AND F.FREIGHT_ELEMENT_ID = '" + FrtElemId + "' ";
                strSql += " AND F.CHARGE_TYPE IN (1, 2, 3) ";
                ds = objWK.GetDataSet(strSql);
                if (ds.Tables[0].Rows.Count > 0)
                {
                    FrtElemMstFk = Convert.ToString(ds.Tables[0].Rows[0]["FREIGHT_ELEMENT_MST_PK"]);
                    CostElemMstFk = Convert.ToString(ds.Tables[0].Rows[0]["COST_ELEMENT_MST_PK"]);
                }
                strSql = null;
                objWK.OpenConnection();
                TRAN = objWK.MyConnection.BeginTransaction();
                //For RevCost = 0 To 1
                ds = new DataSet();
                strSql = " SELECT NVL(MAX(CHARGE_ELEMENT_ID), 0) + 1 CHARGE_ELEMENT_ID, CG.CHARGE_GRP_MST_PK FROM CHARGE_ELEMENT_MST_TBL CE, CHARGE_GROUP_MST_TBL CG ";
                strSql += " WHERE CE.CHARGE_GRP_MST_FK = CG.CHARGE_GRP_MST_PK ";
                strSql += " AND CE.CHARGE_PARENT_FK = " + RevCost;
                strSql += " AND CG.CHARGE_GRP_NAME = '" + ChargeBasis + "'";
                strSql += " GROUP BY CG.CHARGE_GRP_MST_PK ";
                var _with12 = objWK.MyCommand;
                _with12.Parameters.Clear();
                _with12.Transaction = TRAN;
                _with12.CommandType = CommandType.Text;
                _with12.CommandText = strSql;
                OracleDataAdapter da = new OracleDataAdapter(objWK.MyCommand);
                da.Fill(ds);
                if (ds.Tables[0].Rows.Count > 0)
                {
                    ChargeElemId = Convert.ToString(ds.Tables[0].Rows[0]["CHARGE_ELEMENT_ID"]);
                    ChargeGrpMstFk = Convert.ToString(ds.Tables[0].Rows[0]["CHARGE_GRP_MST_PK"]);
                    if (ChargeElemId == "1")
                    {
                        if (RevCost == 0)
                        {
                            if (ChargeBasis == "Freight")
                            {
                                ChargeElemId = "110101";
                            }
                            else if (ChargeBasis == "Surcharge")
                            {
                                ChargeElemId = "110201";
                            }
                            else if (ChargeBasis == "Local Charges")
                            {
                                ChargeElemId = "110301";
                            }
                        }
                        else
                        {
                            if (ChargeBasis == "Freight")
                            {
                                ChargeElemId = "120101";
                            }
                            else if (ChargeBasis == "Surcharge")
                            {
                                ChargeElemId = "120201";
                            }
                            else if (ChargeBasis == "Local Charges")
                            {
                                ChargeElemId = "120301";
                            }
                        }
                    }
                }

                var _with13 = objWK.MyCommand;
                _with13.Transaction = TRAN;
                _with13.CommandType = CommandType.StoredProcedure;
                _with13.CommandText = objWK.MyUserName + ".CHARGE_ELEMENT_MST_TBL_PKG.CHARGE_ELEMENT_MST_TBL_INS";
                var _with14 = _with13.Parameters;
                _with14.Clear();
                _with14.Add("CHARGE_ELEMENT_ID_IN", ChargeElemId).Direction = ParameterDirection.Input;
                _with14.Add("CHARGE_GRP_MST_FK_IN", ChargeGrpMstFk).Direction = ParameterDirection.Input;
                _with14.Add("CHARGE_PARENT_FK_IN", RevCost).Direction = ParameterDirection.Input;
                if (RevCost == 0)
                {
                    _with14.Add("FREIGHT_ELE_MST_FK_IN", FrtElemMstFk).Direction = ParameterDirection.Input;
                    _with14.Add("COST_ELE_MST_FK_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with14.Add("FREIGHT_ELE_MST_FK_IN", "").Direction = ParameterDirection.Input;
                    _with14.Add("COST_ELE_MST_FK_IN", CostElemMstFk).Direction = ParameterDirection.Input;
                }
                _with14.Add("ACTIVE_FLAG_IN", "1").Direction = ParameterDirection.Input;
                _with14.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                _with14.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                _with14.Add("RETURN_VALUE", OracleDbType.Varchar2, 500).Direction = ParameterDirection.Output;
                _with13.ExecuteNonQuery();
                ChargeElemMstPk = Convert.ToString(_with13.Parameters["RETURN_VALUE"].Value);

                //' Map Table
                var _with15 = objWK.MyCommand;
                _with15.Transaction = TRAN;
                _with15.CommandType = CommandType.StoredProcedure;
                _with15.CommandText = objWK.MyUserName + ".CHARGE_GRP_ELE_MAP_TBL_PKG.CHARGE_GRP_ELE_MAP_TBL_INS";
                var _with16 = _with15.Parameters;
                _with16.Clear();
                _with16.Add("CHARGE_PARENT_FK_IN", RevCost).Direction = ParameterDirection.Input;
                _with16.Add("CHARGE_ELE_MST_FK_IN", Convert.ToInt32(ChargeElemMstPk) - 1).Direction = ParameterDirection.Input;
                _with16.Add("CHARGE_GRP_MST_FK_IN", ChargeGrpMstFk).Direction = ParameterDirection.Input;
                _with16.Add("CHARGE_ELE_ID_IN", ChargeElemId).Direction = ParameterDirection.Input;
                _with16.Add("FRT_COST_ELE_MST_FK_IN", FrtElemMstFk).Direction = ParameterDirection.Input;
                _with16.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                _with16.Add("CONFIG_MST_FK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                _with16.Add("RETURN_VALUE", OracleDbType.Varchar2, 500).Direction = ParameterDirection.Output;
                _with15.ExecuteNonQuery();
                //Next

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return "Error";
                }
                else
                {
                    TRAN.Commit();
                }
                CElemMstPk = Convert.ToInt32(ChargeElemMstPk) - 1;
                return "Saved";
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
                objWK.MyConnection.Close();
            }
        }

        #endregion " Charge Master "
    }
}