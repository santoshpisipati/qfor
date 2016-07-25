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
    public class cls_Commodity_Mst_Tbl : CommonFeatures
    {
        #region "List of Members of the Class"

        /// <summary>
        /// The m_ data set
        /// </summary>
        private DataSet M_DataSet;

        #endregion "List of Members of the Class"

        /// <summary>
        /// The m_ general
        /// </summary>
        private string M_General;

        #region "List of Properties"

        /// <summary>
        /// Gets or sets my data set.
        /// </summary>
        /// <value>
        /// My data set.
        /// </value>
        public DataSet MyDataSet
        {
            get { return M_DataSet; }
            set { M_DataSet = value; }
        }

        #endregion "List of Properties"

        #region "Constructors"

        /// <summary>
        /// Initializes a new instance of the <see cref="cls_Commodity_Mst_Tbl"/> class.
        /// </summary>
        /// <param name="nCommGrpPK">The n comm GRP pk.</param>
        public cls_Commodity_Mst_Tbl(long nCommGrpPK = 0)
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            strSQL = "SELECT c.commodity_mst_pk,";
            strSQL += "c.commodity_id,";
            strSQL += "c.commodity_name ";
            strSQL += " FROM commodity_mst_tbl c ";
            strSQL += " WHERE c.active_flag =1";
            if (nCommGrpPK != 0)
            {
                strSQL += " AND c.commodity_group_fk=" + Convert.ToString(nCommGrpPK);
            }
            strSQL += " ORDER BY c.commodity_name";
            try
            {
                M_DataSet = objWF.GetDataSet(strSQL);
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

        #endregion "Constructors"

        #region " FetchCommodity Function "

        /// <summary>
        /// Fetches the commodity.
        /// </summary>
        /// <param name="CommodityPK">The commodity pk.</param>
        /// <param name="CommodityID">The commodity identifier.</param>
        /// <param name="CommodityName">Name of the commodity.</param>
        /// <returns></returns>
        public DataSet FetchCommodity(Int16 CommodityPK = 0, string CommodityID = "", string CommodityName = "")
        {
            string SQLQuery = null;
            SQLQuery = SQLQuery + " select ";
            SQLQuery = SQLQuery + " '' Commodity_Id,";
            SQLQuery = SQLQuery + " '<ALL>' Commodity_Name,";
            SQLQuery = SQLQuery + " 0 Commodity_mst_PK ";
            SQLQuery = SQLQuery + " from ";
            SQLQuery = SQLQuery + " DUAL ";
            SQLQuery = SQLQuery + " UNION";
            SQLQuery = SQLQuery + " select ";
            SQLQuery = SQLQuery + " Commodity_Id,";
            SQLQuery = SQLQuery + " Commodity_Name,";
            SQLQuery = SQLQuery + " Commodity_mst_PK ";
            SQLQuery = SQLQuery + " from Commodity_mst_tbl";
            SQLQuery = SQLQuery + " order by Commodity_ID ";

            WorkFlow objWF = new WorkFlow();
            try
            {
                return objWF.GetDataSet(SQLQuery);
            }
            catch (OracleException dbExp)
            {
                ErrorMessage = dbExp.Message;
                throw dbExp;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        #endregion " FetchCommodity Function "

        #region " FetchAll Function "

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="P_Commodity_Id">The p_ commodity_ identifier.</param>
        /// <param name="P_Commodity_Name">Name of the p_ commodity_.</param>
        /// <param name="P_Imdg_Class_Code">The p_ imdg_ class_ code.</param>
        /// <param name="P_Imdg_Code_Page">The p_ imdg_ code_ page.</param>
        /// <param name="P_Un_No">The p_ un_ no.</param>
        /// <param name="P_Commodity_Group_Mst_Fk">The p_ commodity_ group_ MST_ fk.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="SortExpression">The sort expression.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="strColumnName">Name of the string column.</param>
        /// <param name="IsActive">The is active.</param>
        /// <param name="blnSortAscending">if set to <c>true</c> [BLN sort ascending].</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public DataSet FetchAll(string P_Commodity_Id, string P_Commodity_Name, string P_Imdg_Class_Code, string P_Imdg_Code_Page, string P_Un_No, Int32 P_Commodity_Group_Mst_Fk, string SearchType = "", string SortExpression = "", Int32 CurrentPage = 0, Int32 TotalPage = 0,
        string strColumnName = "", Int16 IsActive = 0, bool blnSortAscending = false, Int32 flag = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string SQLQuery = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            if (flag == 0)
            {
                strCondition += " AND 1=2";
            }

            if (P_Commodity_Id.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strCondition = strCondition + " And upper(Commodity_Id) like '%" + P_Commodity_Id.ToUpper().Replace("'", "''") + "%' ";
                }
                else
                {
                    strCondition = strCondition + " And upper(Commodity_Id) like '" + P_Commodity_Id.ToUpper().Replace("'", "''") + "%' ";
                }
            }
            else
            {
            }
            if (P_Commodity_Name.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strCondition = strCondition + " And Upper(Commodity_Name) like '%" + P_Commodity_Name.ToUpper().Replace("'", "''") + "%' ";
                }
                else
                {
                    strCondition = strCondition + " And upper(Commodity_Name) like '" + P_Commodity_Name.ToUpper().Replace("'", "''") + "%' ";
                }
            }

            if (P_Imdg_Class_Code.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strCondition = strCondition + " And upper(Imdg_Class_Code) like '%" + P_Imdg_Class_Code.Replace("'", "''").ToUpper() + "%' ";
                }
                else
                {
                    strCondition = strCondition + " And upper(Imdg_Class_Code) like '" + P_Imdg_Class_Code.Replace("'", "''").ToUpper() + "%' ";
                }
            }

            if (P_Imdg_Code_Page.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strCondition = strCondition + " And upper(Imdg_Code_Page) like '%" + P_Imdg_Code_Page.Replace("'", "''").ToUpper() + "%' ";
                }
                else
                {
                    strCondition = strCondition + " And upper(Imdg_Code_Page) like '" + P_Imdg_Code_Page.Replace("'", "''").ToUpper() + "%' ";
                }
            }

            if (P_Un_No.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strCondition = strCondition + " And upper(Un_No) like '%" + P_Un_No.Replace("'", "''").ToUpper() + "%' ";
                }
                else
                {
                    strCondition = strCondition + " And upper(Un_No) like '" + P_Un_No.Replace("'", "''").ToUpper() + "%' ";
                }
            }

            if (P_Commodity_Group_Mst_Fk > 0)
            {
                strCondition = strCondition + " And CM.COMMODITY_GROUP_FK = '" + P_Commodity_Group_Mst_Fk + "'";
            }

            if (IsActive == 1)
            {
                strCondition = strCondition + " And CM.ACTIVE_FLAG=1";
            }

            SQLQuery = "SELECT Count(*)";
            SQLQuery += " FROM  ";
            SQLQuery += " COMMODITY_MST_TBL CM,   ";
            SQLQuery += " commodity_group_mst_tbl cmgrp   ";
            SQLQuery += " WHERE CM.COMMODITY_GROUP_FK = CMGRP.COMMODITY_GROUP_PK ";
            SQLQuery += strCondition;

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(SQLQuery));
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
            SQLQuery = " select * from (";
            SQLQuery += "SELECT ROWNUM SR_NO, q.* from ";
            SQLQuery += " (SELECT ";
            SQLQuery += " CM.COMMODITY_MST_PK, ";
            SQLQuery += " CM.ACTIVE_FLAG,";
            SQLQuery += " CM.Commodity_Id,  ";
            SQLQuery += " CM.Commodity_Name,  ";
            SQLQuery += " CM.Imdg_Class_Code,  ";
            SQLQuery += " CM.Imdg_Code_Page,  ";
            SQLQuery += " CM.Un_No,  ";
            SQLQuery += " CM.commodity_group_fk,  ";
            SQLQuery += " cmgrp.commodity_group_code,  ";
            SQLQuery += " CM.HAZARDOUS, ";
            SQLQuery += " CM.Version_No   ";
            SQLQuery += " FROM  ";
            SQLQuery += " COMMODITY_MST_TBL CM,   ";
            SQLQuery += " commodity_group_mst_tbl cmgrp   ";
            SQLQuery += " WHERE CM.COMMODITY_GROUP_FK = CMGRP.COMMODITY_GROUP_PK (+)";
            SQLQuery += strCondition;
            if (!strColumnName.Equals("SR_NO"))
            {
                SQLQuery += "order by " + strColumnName;
            }
            if (!blnSortAscending & !strColumnName.Equals("SR_NO"))
            {
                SQLQuery += " DESC";
            }
            SQLQuery += " )q) WHERE SR_NO  Between " + start + " and " + last;
            try
            {
                return objWF.GetDataSet(SQLQuery);
            }
            catch (OracleException DBExp)
            {
                ErrorMessage = DBExp.Message;
                throw DBExp;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        #endregion " FetchAll Function "

        #region " Save Function "

        /// <summary>
        /// Saves the specified m_ data set.
        /// </summary>
        /// <param name="M_DataSet">The m_ data set.</param>
        /// <param name="Import">if set to <c>true</c> [import].</param>
        /// <returns></returns>
        public ArrayList Save(DataSet M_DataSet, bool Import = false)
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
                DataTable dttbl = new DataTable();
                dttbl = M_DataSet.Tables[0];
                var _with1 = insCommand;
                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".COMMODITY_MST_TBL_PKG.COMMODITY_MST_TBL_INS";
                var _with2 = _with1.Parameters;
                insCommand.Parameters.Add("COMMODITY_ID_IN", OracleDbType.Varchar2, 20, "COMMODITY_ID").Direction = ParameterDirection.Input;
                insCommand.Parameters["COMMODITY_ID_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("COMMODITY_NAME_IN", OracleDbType.Varchar2, 50, "COMMODITY_NAME").Direction = ParameterDirection.Input;
                insCommand.Parameters["COMMODITY_NAME_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("IMDG_CLASS_CODE_IN", OracleDbType.Varchar2, 20, "IMDG_CLASS_CODE").Direction = ParameterDirection.Input;
                insCommand.Parameters["IMDG_CLASS_CODE_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("IMDG_CODE_PAGE_IN", OracleDbType.Varchar2, 20, "IMDG_CODE_PAGE").Direction = ParameterDirection.Input;
                insCommand.Parameters["IMDG_CODE_PAGE_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("UN_NO_IN", OracleDbType.Varchar2, 20, "UN_NO").Direction = ParameterDirection.Input;
                insCommand.Parameters["UN_NO_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("commodity_group_fk_IN", OracleDbType.Int32, 10, "commodity_group_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["commodity_group_fk_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("HAZARDOUS_IN", OracleDbType.Int32, 1, "HAZARDOUS").Direction = ParameterDirection.Input;
                insCommand.Parameters["HAZARDOUS_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
                insCommand.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "COMMODITY_MST_PK").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                var _with3 = updCommand;
                _with3.Connection = objWK.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".COMMODITY_MST_TBL_PKG.COMMODITY_MST_TBL_UPD";
                var _with4 = _with3.Parameters;
                updCommand.Parameters.Add("COMMODITY_MST_PK_IN", OracleDbType.Int32, 10, "COMMODITY_MST_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["COMMODITY_MST_PK_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("commodity_group_fk_IN", OracleDbType.Int32, 10, "commodity_group_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["commodity_group_fk_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("COMMODITY_ID_IN", OracleDbType.Varchar2, 20, "COMMODITY_ID").Direction = ParameterDirection.Input;
                updCommand.Parameters["COMMODITY_ID_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("COMMODITY_NAME_IN", OracleDbType.Varchar2, 150, "COMMODITY_NAME").Direction = ParameterDirection.Input;
                updCommand.Parameters["COMMODITY_NAME_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("IMDG_CLASS_CODE_IN", OracleDbType.Varchar2, 20, "IMDG_CLASS_CODE").Direction = ParameterDirection.Input;
                updCommand.Parameters["IMDG_CLASS_CODE_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("IMDG_CODE_PAGE_IN", OracleDbType.Varchar2, 20, "IMDG_CODE_PAGE").Direction = ParameterDirection.Input;
                updCommand.Parameters["IMDG_CODE_PAGE_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("UN_NO_IN", OracleDbType.Varchar2, 50, "UN_NO").Direction = ParameterDirection.Input;
                updCommand.Parameters["UN_NO_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("HAZARDOUS_IN", OracleDbType.Int32, 1, "HAZARDOUS").Direction = ParameterDirection.Input;
                updCommand.Parameters["HAZARDOUS_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
                updCommand.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 200, "RETURN_VALUE").Direction = ParameterDirection.Output;
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
                    if (Import == false)
                    {
                        arrMessage.Add("All Data Saved Successfully");
                    }
                    else
                    {
                        arrMessage.Add("Data Imported Successfully");
                    }
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

        #endregion " Save Function "

        #region " Enhance Search Function for Commodity for Group "

        /// <summary>
        /// Fetches the commodity for group.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchCommodityForGroup(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            int iCommodity_Grp_PK = 0;
            string strReq = null;
            dynamic strNull = "";
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            iCommodity_Grp_PK = Convert.ToInt32(arr.GetValue(2));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_COMMODITY_PKG.GETCOMM_GRPY_COMMON";
                var _with6 = selectCommand.Parameters;
                _with6.Add("SEARCH_IN", getDefault(strSERACH_IN.ToUpper().Trim(), "")).Direction = ParameterDirection.Input;
                _with6.Add("COMMODITY_GRP_PK_IN", iCommodity_Grp_PK).Direction = ParameterDirection.Input;
                _with6.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with6.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1500, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
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
                selectCommand.Connection.Close();
            }
        }

        #endregion " Enhance Search Function for Commodity for Group "
    }
}